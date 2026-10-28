"""
Asynchronous TCP peer-to-peer networking primitives, connection lifecycle, and message exchange.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging
import time
from typing import Any
import uuid

from meshweaver.exceptions import (
    ConnectionClosedError,
    HandshakeError,
    InvalidPayloadError,
    MalformedMessageError,
    NetworkTimeoutError,
    PeerConnectionError,
    ValidationError,
)
from meshweaver.peers import PeerRegistry
from meshweaver.protocol import (
    Message,
    MessageType,
    read_frame,
    write_frame,
)

logger = logging.getLogger(__name__)

# Type alias for message handler callbacks
MessageHandler = Callable[[Message, "PeerConnection"], Awaitable[Message | None]]


class PeerConnection:
    """
    Represents an active bi-directional stream connection to a remote peer.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        remote_peer_id: str | None = None,
        is_inbound: bool = False,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.remote_peer_id = remote_peer_id
        self.is_inbound = is_inbound
        self.connected_at: float = time.time()
        self._closed: bool = False
        self._write_lock: asyncio.Lock = asyncio.Lock()

    @property
    def is_closed(self) -> bool:
        """Check if the connection has been closed."""
        return self._closed or self.writer.is_closing()

    @property
    def peer_address(self) -> tuple[str, int]:
        """Return the remote IP and port as reported by the socket."""
        extra = self.writer.get_extra_info("peername")
        if extra and isinstance(extra, tuple) and len(extra) >= 2:
            return str(extra[0]), int(extra[1])
        return "0.0.0.0", 0

    async def send(self, message: Message) -> None:
        """
        Send a framed message over the connection.

        Raises:
            ConnectionClosedError: If the connection is closed or breaks during transmission.
        """
        if self.is_closed:
            raise ConnectionClosedError("Cannot send on closed connection")

        async with self._write_lock:
            try:
                write_frame(self.writer, message)
                await self.writer.drain()
            except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError) as exc:
                self._closed = True
                raise ConnectionClosedError(f"Connection lost while sending: {exc}") from exc

    async def receive(self, timeout: float | None = None) -> Message | None:
        """
        Read a framed message from the connection.

        Returns:
            Message | None: The decoded Message, or None on clean EOF.

        Raises:
            NetworkTimeoutError: If the read operation exceeds timeout.
            ConnectionClosedError: If connection resets or breaks.
            MalformedMessageError: If the incoming frame violates protocol rules.
        """
        if self.is_closed:
            raise ConnectionClosedError("Cannot receive on closed connection")

        try:
            if timeout is not None:
                msg = await asyncio.wait_for(read_frame(self.reader), timeout=timeout)
            else:
                msg = await read_frame(self.reader)
        except TimeoutError as exc:
            raise NetworkTimeoutError(f"Timed out waiting for frame after {timeout}s") from exc
        except (ConnectionResetError, BrokenPipeError, ConnectionAbortedError, OSError) as exc:
            self._closed = True
            raise ConnectionClosedError(f"Connection closed by peer: {exc}") from exc

        if msg is None:
            self._closed = True
        return msg

    async def close(self) -> None:
        """Gracefully close the underlying stream connection."""
        if self._closed:
            return
        self._closed = True
        try:
            if not self.writer.is_closing():
                self.writer.close()
            try:
                await asyncio.shield(asyncio.wait_for(self.writer.wait_closed(), timeout=0.5))
            except Exception:
                pass
        except Exception:
            pass


class PeerNode:
    """
    Asynchronous TCP peer node capable of acting simultaneously as a server
    listening for incoming peer connections and a client connecting out to peers.
    """

    def __init__(
        self,
        peer_id: str | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        peer_registry: PeerRegistry | None = None,
        task_registry: Any | None = None,
    ) -> None:
        self.peer_id: str = peer_id if peer_id and peer_id.strip() else uuid.uuid4().hex[:12]
        self.host: str = host
        self.port: int = port
        self.peer_registry: PeerRegistry = (
            peer_registry if peer_registry is not None else PeerRegistry()
        )
        self._bound_port: int | None = None
        self._server: asyncio.Server | None = None
        self._is_running: bool = False

        # Active connections: internal mapping of id(conn) -> conn
        self._connections: dict[int, PeerConnection] = {}
        # Active peer directory: peer_id -> conn
        self._peer_connections: dict[str, PeerConnection] = {}
        # Tasks handling connection read loops
        self._listener_tasks: set[asyncio.Task[None]] = set()
        # Pending request-response futures: correlation_id/message_id -> Future[Message]
        self._pending_requests: dict[str, asyncio.Future[Message]] = {}
        # Registered message handlers
        self._handlers: dict[MessageType, MessageHandler] = {}

        # Task registry for local task execution
        from meshweaver.tasks import TaskRegistry
        from meshweaver.execution import TaskExecutionEngine

        self.task_registry: TaskRegistry = (
            task_registry if task_registry is not None else TaskRegistry.create_default()
        )
        self.execution_engine: TaskExecutionEngine = TaskExecutionEngine(
            registry=self.task_registry,
            auto_start=True,
        )
        from meshweaver.worker import TaskWorker

        self.worker: TaskWorker = TaskWorker(
            registry=self.task_registry,
            concurrency=self.execution_engine.max_concurrency,
            auto_start=True,
        )
        self._active_task_counter: int = 0

        # Load-aware task router
        from meshweaver.routing import TaskRouter

        self.task_router: TaskRouter = TaskRouter(self)

        # Register default protocol handlers
        self._register_default_handlers()

        # DHT routing table
        from meshweaver.dht import RoutingTable

        self.routing_table: RoutingTable = RoutingTable(self.node_id)

        # Asynchronous gossip engine for workload and health monitoring
        self.gossip_engine: Any | None = None


    @property
    def is_running(self) -> bool:
        """Return True if the peer server is actively running."""
        return self._is_running

    @property
    def bound_port(self) -> int:
        """Return the effective TCP port bound by the server."""
        if self._bound_port is not None:
            return self._bound_port
        return self.port

    @property
    def node_id(self) -> Any:
        """Return deterministic NodeID for DHT operations."""
        from meshweaver.dht import NodeID

        return NodeID.from_peer(self.peer_id)

    @property
    def active_connections(self) -> list[PeerConnection]:
        """Return a snapshot of active peer connections."""
        return list(self._connections.values())

    @property
    def connected_peers(self) -> dict[str, PeerConnection]:
        """Return a snapshot mapping of known remote peer IDs to connections."""
        return dict(self._peer_connections)

    @property
    def status_api(self) -> Any:
        """Internal node status and peer registry inspection API."""
        from meshweaver.status import NodeStatusAPI

        return NodeStatusAPI(self)

    def get_status(self, *, current_time: float | None = None) -> Any:
        """Return a NodeStatusSnapshot for this node and tracked peers."""
        return self.status_api.get_node_status(current_time=current_time)

    def get_peer_status(self, peer_id: str, *, current_time: float | None = None) -> Any:
        """Return PeerStatus for a specific peer, or None if not found."""
        return self.status_api.get_peer_status(peer_id, current_time=current_time)

    @property
    def active_task_count(self) -> int:
        """Return the count of currently executing tasks on this node."""
        engine_running = (
            self.execution_engine.running_count
            if hasattr(self, "execution_engine") and self.execution_engine is not None
            else 0
        )
        return max(self._active_task_counter, engine_running)

    def get_task(self, task_id: str) -> Any | None:
        """Retrieve a locally tracked TaskRecord by ID."""
        if hasattr(self, "execution_engine") and self.execution_engine is not None:
            return self.execution_engine.get_task(task_id)
        return None

    def get_task_status(self, task_id: str) -> Any | None:
        """Retrieve the TaskState of a locally tracked or executed task by ID."""
        rec = self.get_task(task_id)
        return rec.state if rec is not None else None

    def register_task(
        self,
        name_or_func: Any = None,
        handler: Any = None,
    ) -> Any:
        """Register a local task handler in this node's TaskRegistry."""
        return self.task_registry.register(name_or_func, handler)

    def register_handler(self, msg_type: MessageType, handler: MessageHandler) -> None:
        """Register a handler callback for incoming messages of a given MessageType."""
        self._handlers[msg_type] = handler

    def _register_default_handlers(self) -> None:
        """Install built-in protocol handlers (PING, HANDSHAKE, and TASK_REQUEST)."""

        async def _handle_ping(msg: Message, _conn: PeerConnection) -> Message:
            return Message.create(
                MessageType.PONG,
                sender_id=self.peer_id,
                recipient_id=msg.sender_id,
                correlation_id=msg.message_id,
                payload={"ack_time": time.time(), "echo": msg.payload},
            )

        async def _handle_handshake(msg: Message, conn: PeerConnection) -> Message:
            remote_node_id = str(msg.payload.get("node_id", msg.sender_id))
            conn.remote_peer_id = remote_node_id
            self._peer_connections[remote_node_id] = conn
            port_val = msg.payload.get("port")
            if port_val and isinstance(port_val, int) and not isinstance(port_val, bool):
                remote_host = conn.peer_address[0]
                rec = self.peer_registry.register(remote_node_id, remote_host, port_val)
                try:
                    self.routing_table.add_contact(rec.to_contact())
                except Exception:
                    pass
            return Message.create(
                MessageType.HANDSHAKE_ACK,
                sender_id=self.peer_id,
                recipient_id=remote_node_id,
                correlation_id=msg.message_id,
                payload={"node_id": self.peer_id, "status": "accepted"},
            )

        async def _handle_task_request(msg: Message, _conn: PeerConnection) -> Message:
            from meshweaver.exceptions import InvalidTaskError, ValidationError
            from meshweaver.tasks import TaskErrorInfo, TaskRequest, TaskResponse

            try:
                task_req = TaskRequest.from_message(msg)
            except (InvalidTaskError, ValidationError, Exception) as parse_exc:
                err_resp = TaskResponse.error(
                    task_id=str(msg.payload.get("task_id", uuid.uuid4().hex)),
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(parse_exc)),
                )
                return err_resp.to_message(
                    sender_id=self.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                )

            self._active_task_counter += 1
            try:
                task_resp = await self.execution_engine.execute_request(task_req)
            except Exception as exec_exc:
                task_resp = TaskResponse.error(
                    task_id=task_req.task_id,
                    error=TaskErrorInfo(code="EXECUTION_ERROR", message=str(exec_exc)),
                )
            finally:
                self._active_task_counter = max(0, self._active_task_counter - 1)

            return task_resp.to_message(
                sender_id=self.peer_id,
                recipient_id=msg.sender_id,
                correlation_id=msg.message_id,
            )

        async def _handle_find_node(msg: Message, _conn: PeerConnection) -> Message:
            from meshweaver.dht import NodeID

            target_val = msg.payload.get("target_id", "")
            count = int(msg.payload.get("count", 20))
            try:
                target_node = NodeID.from_peer(target_val)
            except Exception:
                target_node = NodeID.from_string(str(target_val))

            closest = self.peer_registry.find_closest_peers(
                target_node,
                count=count,
                exclude=[msg.sender_id, self.peer_id],
            )
            return Message.create(
                MessageType.NODES_FOUND,
                sender_id=self.peer_id,
                recipient_id=msg.sender_id,
                correlation_id=msg.message_id,
                payload={
                    "target_id": str(target_val),
                    "nodes": [p.to_dict() for p in closest],
                },
            )

        async def _handle_gossip(msg: Message, conn: PeerConnection) -> None:
            if self.gossip_engine is not None:
                await self.gossip_engine.handle_gossip_message(msg, conn)

        async def _handle_load_report(msg: Message, conn: PeerConnection) -> None:
            if self.gossip_engine is not None:
                await self.gossip_engine.handle_gossip_message(msg, conn)
            elif self.peer_registry.has_peer(msg.sender_id):
                meta: dict[str, Any] = {}
                if "cpu_percent" in msg.payload:
                    meta["cpu_percent"] = msg.payload["cpu_percent"]
                if "memory_percent" in msg.payload:
                    meta["memory_percent"] = msg.payload["memory_percent"]
                if meta:
                    self.peer_registry.update(msg.sender_id, metadata=meta)

        async def _handle_leave(msg: Message, _conn: PeerConnection) -> None:
            remote_node_id = str(msg.payload.get("node_id", msg.sender_id))
            logger.info("Received LEAVE notice from peer %s", remote_node_id)
            if remote_node_id in self._peer_connections:
                self._peer_connections.pop(remote_node_id, None)
            self.peer_registry.remove(remote_node_id)
            try:
                self.routing_table.remove_contact(remote_node_id)
            except Exception:
                pass
            if self.gossip_engine is not None:
                self.gossip_engine.store.remove(remote_node_id)

        self.register_handler(MessageType.PING, _handle_ping)
        self.register_handler(MessageType.HANDSHAKE, _handle_handshake)
        self.register_handler(MessageType.TASK_REQUEST, _handle_task_request)
        self.register_handler(MessageType.FIND_NODE, _handle_find_node)
        self.register_handler(MessageType.GOSSIP, _handle_gossip)
        self.register_handler(MessageType.LOAD_REPORT, _handle_load_report)
        self.register_handler(MessageType.LEAVE, _handle_leave)

    def enable_gossip(
        self,
        *,
        interval: float = 5.0,
        fanout: int = 3,
        stale_threshold: float = 30.0,
        ttl: float = 60.0,
        max_hops: int = 3,
        collector: Any | None = None,
        task_count_provider: Callable[[], int] | None = None,
    ) -> Any:
        """
        Enable and configure the asynchronous gossip engine on this node.
        """
        from meshweaver.gossip import GossipEngine

        if self.gossip_engine is not None:
            return self.gossip_engine

        resolved_task_provider = (
            task_count_provider if task_count_provider is not None else (lambda: self._active_task_counter)
        )

        self.gossip_engine = GossipEngine(
            node_id=self.peer_id,
            peer_node=self,
            peer_registry=self.peer_registry,
            gossip_interval=interval,
            fanout=fanout,
            stale_threshold=stale_threshold,
            ttl=ttl,
            max_hops=max_hops,
            collector=collector,
            task_count_provider=resolved_task_provider,
        )
        if self._is_running:
            self.gossip_engine.start()
        return self.gossip_engine


    async def start(self) -> None:
        """
        Start the asynchronous TCP peer server.
        """
        if self._is_running:
            return

        self._server = await asyncio.start_server(
            self._handle_inbound_stream,
            host=self.host,
            port=self.port,
        )

        sockets = self._server.sockets
        if sockets:
            self._bound_port = sockets[0].getsockname()[1]
        self._is_running = True
        logger.info("PeerNode %s listening on %s:%d", self.peer_id, self.host, self.bound_port)

        if self.gossip_engine is not None and not self.gossip_engine.is_running:
            self.gossip_engine.start()

        if hasattr(self, "execution_engine") and self.execution_engine is not None:
            if self.execution_engine.is_shutting_down:
                from meshweaver.execution import TaskExecutionEngine

                self.execution_engine = TaskExecutionEngine(
                    registry=self.task_registry,
                    auto_start=True,
                )
            elif not self.execution_engine.is_running:
                self.execution_engine.start()

    async def stop(self) -> None:
        """
        Gracefully stop the peer server, cancel background listeners, and close connections.
        """
        if not self._is_running and self._server is None:
            return

        self._is_running = False

        # Stop gossip engine first
        if self.gossip_engine is not None and self.gossip_engine.is_running:
            await self.gossip_engine.stop()

        # Close server first to stop accepting new connections
        if self._server is not None:

            self._server.close()
            try:
                await asyncio.shield(asyncio.wait_for(self._server.wait_closed(), timeout=1.0))
            except Exception:
                pass
            self._server = None

        # Close all active connections
        conns_to_close = list(self._connections.values())
        for c in conns_to_close:
            try:
                if not c.writer.is_closing():
                    c.writer.close()
            except Exception:
                pass

        # Cancel listener tasks
        for task in list(self._listener_tasks):
            task.cancel()
        if self._listener_tasks:
            await asyncio.gather(*self._listener_tasks, return_exceptions=True)
            self._listener_tasks.clear()

        # Await clean closing of writers
        await asyncio.gather(*(c.close() for c in conns_to_close), return_exceptions=True)
        self._connections.clear()
        self._peer_connections.clear()

        # Fail any remaining pending request futures
        for fut in list(self._pending_requests.values()):
            if not fut.done():
                fut.set_exception(ConnectionClosedError("Peer node stopped"))
        # Shut down execution engine
        if hasattr(self, "execution_engine") and self.execution_engine is not None:
            try:
                await self.execution_engine.shutdown(wait=False, cancel_pending=True)
            except Exception as exc:
                logger.debug("Error shutting down execution engine: %s", exc)

        # Yield a brief slice so event loop flushes socket cleanup
        await asyncio.sleep(0.01)

    async def __aenter__(self) -> PeerNode:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.stop()

    async def _handle_inbound_stream(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """Handle incoming TCP socket connection from another peer."""
        conn = PeerConnection(reader, writer, is_inbound=True)
        self._connections[id(conn)] = conn

        task = asyncio.create_task(self._connection_read_loop(conn))
        self._listener_tasks.add(task)
        task.add_done_callback(self._listener_tasks.discard)

    async def connect_to_peer(
        self,
        host: str,
        port: int,
        timeout: float = 5.0,
        *,
        do_handshake: bool = True,
    ) -> PeerConnection:
        """
        Connect to a remote peer over TCP, optionally performing the handshake.

        Returns:
            PeerConnection: The established connection.

        Raises:
            NetworkTimeoutError: If connection or handshake exceeds timeout.
            HandshakeError: If handshake response is invalid.
            PeerConnectionError: If network connection fails.
        """
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise NetworkTimeoutError(f"Connection to {host}:{port} timed out after {timeout}s") from exc
        except (OSError, ConnectionRefusedError) as exc:
            raise PeerConnectionError(f"Failed to connect to {host}:{port}: {exc}") from exc

        conn = PeerConnection(reader, writer, is_inbound=False)
        self._connections[id(conn)] = conn

        if do_handshake:
            try:
                handshake_msg = Message.create(
                    MessageType.HANDSHAKE,
                    sender_id=self.peer_id,
                    payload={"node_id": self.peer_id, "port": self.bound_port},
                )
                await conn.send(handshake_msg)
                resp = await conn.receive(timeout=timeout)
                if resp is None or resp.message_type != MessageType.HANDSHAKE_ACK:
                    await conn.close()
                    raise HandshakeError(f"Expected HANDSHAKE_ACK, received {resp}")

                remote_node_id = str(resp.payload.get("node_id", resp.sender_id))
                conn.remote_peer_id = remote_node_id
                self._peer_connections[remote_node_id] = conn
                rec = self.peer_registry.register(remote_node_id, host, port)
                try:
                    self.routing_table.add_contact(rec.to_contact())
                except Exception:
                    pass
            except Exception:
                await conn.close()
                self._connections.pop(id(conn), None)
                raise

        task = asyncio.create_task(self._connection_read_loop(conn))
        self._listener_tasks.add(task)
        task.add_done_callback(self._listener_tasks.discard)

        return conn

    async def _connection_read_loop(self, conn: PeerConnection) -> None:
        """
        Continuously read and dispatch incoming messages for a connection.
        """
        conn_id = id(conn)
        try:
            while self._is_running and not conn.is_closed:
                try:
                    msg = await conn.receive()
                except ConnectionClosedError:
                    break
                except (MalformedMessageError, InvalidPayloadError, ValidationError) as exc:
                    logger.warning("Received malformed/invalid message from %s: %s", conn.peer_address, exc)
                    try:
                        err_msg = Message.create(
                            MessageType.ERROR,
                            sender_id=self.peer_id,
                            payload={"code": "MALFORMED_MESSAGE", "message": str(exc)},
                        )
                        await conn.send(err_msg)
                    except Exception:
                        pass
                    continue

                if msg is None:
                    break

                # Correlated response resolution
                if msg.correlation_id and msg.correlation_id in self._pending_requests:
                    future = self._pending_requests[msg.correlation_id]
                    if not future.done():
                        future.set_result(msg)
                    continue

                # Normal message dispatch to registered handler
                handler = self._handlers.get(msg.message_type)
                if handler is not None:
                    try:
                        reply = await handler(msg, conn)
                        if reply is not None:
                            await conn.send(reply)
                    except Exception as handler_exc:
                        logger.exception("Error executing handler for %s", msg.message_type)
                        err_reply = Message.create(
                            MessageType.ERROR,
                            sender_id=self.peer_id,
                            recipient_id=msg.sender_id,
                            correlation_id=msg.message_id,
                            payload={"code": "HANDLER_ERROR", "message": str(handler_exc)},
                        )
                        await conn.send(err_reply)
                else:
                    logger.debug("No handler for message type %s", msg.message_type)

        except asyncio.CancelledError:
            pass
        finally:
            await conn.close()
            self._connections.pop(conn_id, None)
            if conn.remote_peer_id and conn.remote_peer_id in self._peer_connections:
                self._peer_connections.pop(conn.remote_peer_id, None)

    async def send_request(
        self,
        connection: PeerConnection,
        message: Message,
        timeout: float = 5.0,
    ) -> Message:
        """
        Send a request message and await a matching correlated response.

        Raises:
            NetworkTimeoutError: If no response arrives before timeout.
            ConnectionClosedError: If connection breaks while awaiting response.
        """
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Message] = loop.create_future()
        self._pending_requests[message.message_id] = future

        try:
            await connection.send(message)
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError as exc:
            raise NetworkTimeoutError(
                f"Request {message.message_id} ({message.message_type}) timed out after {timeout}s"
            ) from exc
        finally:
            self._pending_requests.pop(message.message_id, None)

    async def send_message(self, connection: PeerConnection, message: Message) -> None:
        """Send a one-way message over an existing connection without awaiting reply."""
        await connection.send(message)

    async def submit_task(
        self,
        task_name_or_request: str | Any,
        *args: Any,
        target_peer_id: str | None = None,
        timeout: float = 5.0,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        retry_on_failure: bool = True,
        max_candidates: int = 3,
        **kwargs: Any,
    ) -> Any:
        """
        Submit a task for remote execution across the mesh.

        Selects the best available candidate peer (or routes to target_peer_id),
        dispatches the task over an asynchronous TCP stream, and awaits the result.
        Gracefully handles invalid task schemas, unreachable peers (with optional
        failover retry), and timeouts, returning a structured TaskResponse.
        """
        from meshweaver.exceptions import (
            InvalidTaskError,
            NetworkTimeoutError,
            ValidationError,
        )
        from meshweaver.tasks import TaskErrorInfo, TaskRequest, TaskResponse

        # 1. Parse or create validated TaskRequest
        if isinstance(task_name_or_request, TaskRequest):
            req = task_name_or_request
        else:
            try:
                req = TaskRequest.create(
                    task_name=str(task_name_or_request),
                    args=args if args else None,
                    kwargs=kwargs if kwargs else None,
                    task_id=task_id,
                    metadata=metadata,
                )
            except (InvalidTaskError, ValidationError) as exc:
                return TaskResponse.error(
                    task_id=task_id if task_id else uuid.uuid4().hex,
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(exc)),
                )
            except Exception as exc:
                return TaskResponse.error(
                    task_id=task_id if task_id else uuid.uuid4().hex,
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(exc)),
                )

        # 2. Identify candidate peers
        candidates: list[tuple[str, str, int]] = []
        if target_peer_id is not None:
            if target_peer_id == self.peer_id:
                # Local execution via execution engine
                return await self.execution_engine.execute_request(req)

            rec = self.peer_registry.get(target_peer_id)
            if rec is not None:
                candidates.append((rec.peer_id, rec.host, rec.port))
            else:
                conn = self._peer_connections.get(target_peer_id)
                if conn is not None and not conn.is_closed:
                    addr = conn.peer_address
                    candidates.append((target_peer_id, addr[0], addr[1]))
                else:
                    return TaskResponse.error(
                        task_id=req.task_id,
                        error=TaskErrorInfo(
                            code="PEER_UNAVAILABLE",
                            message=f"Target peer '{target_peer_id}' is not found or reachable in mesh",
                        ),
                    )
        else:
            # Select candidate peers via task_router
            candidate_records = self.task_router.select_candidates(
                req,
                count=max_candidates,
                exclude=[self.peer_id],
            )
            if not candidate_records:
                return TaskResponse.error(
                    task_id=req.task_id,
                    error=TaskErrorInfo(
                        code="NO_PEERS_AVAILABLE",
                        message="No available or reachable worker peers found in mesh to route task",
                    ),
                )
            candidates = [(c.peer_id, c.host, c.port) for c in candidate_records]

        # 3. Dispatch task to candidate peer (with failover)
        last_error: TaskErrorInfo | None = None
        for candidate_id, host, port in candidates:
            conn = self._peer_connections.get(candidate_id)

            if conn is None or conn.is_closed:
                try:
                    conn = await self.connect_to_peer(host, port, timeout=timeout, do_handshake=True)
                except Exception as conn_exc:
                    logger.warning(
                        "Failed to connect to candidate worker %s (%s:%d): %s",
                        candidate_id,
                        host,
                        port,
                        conn_exc,
                    )
                    last_error = TaskErrorInfo(
                        code="PEER_UNREACHABLE",
                        message=f"Failed to connect to worker peer {candidate_id} at {host}:{port}: {conn_exc}",
                    )
                    if retry_on_failure:
                        continue
                    break

            msg = req.to_message(sender_id=self.peer_id, recipient_id=candidate_id)
            try:
                resp_msg = await self.send_request(conn, msg, timeout=timeout)
            except NetworkTimeoutError as timeout_exc:
                logger.warning("Task execution timed out on worker %s: %s", candidate_id, timeout_exc)
                last_error = TaskErrorInfo(
                    code="TASK_TIMEOUT",
                    message=f"Task execution on worker {candidate_id} timed out after {timeout}s",
                )
                if retry_on_failure:
                    continue
                break
            except Exception as net_exc:
                logger.warning("Communication error during task exchange with %s: %s", candidate_id, net_exc)
                last_error = TaskErrorInfo(
                    code="COMMUNICATION_ERROR",
                    message=f"Communication error with worker {candidate_id}: {net_exc}",
                )
                if retry_on_failure:
                    continue
                break

            if resp_msg.message_type == MessageType.TASK_RESPONSE:
                try:
                    return TaskResponse.from_message(resp_msg)
                except Exception as parse_exc:
                    return TaskResponse.error(
                        task_id=req.task_id,
                        error=TaskErrorInfo(
                            code="INVALID_RESPONSE",
                            message=f"Malformed task response from {candidate_id}: {parse_exc}",
                        ),
                    )
            elif resp_msg.message_type == MessageType.ERROR:
                return TaskResponse.error(
                    task_id=req.task_id,
                    error=TaskErrorInfo(
                        code=str(resp_msg.payload.get("code", "REMOTE_ERROR")),
                        message=str(resp_msg.payload.get("message", "Remote peer returned an error")),
                    ),
                )
            else:
                return TaskResponse.error(
                    task_id=req.task_id,
                    error=TaskErrorInfo(
                        code="UNEXPECTED_MESSAGE_TYPE",
                        message=f"Received unexpected message type {resp_msg.message_type} from {candidate_id}",
                    ),
                )

        return TaskResponse.error(
            task_id=req.task_id,
            error=last_error if last_error else TaskErrorInfo(
                code="ROUTING_FAILED",
                message="Failed to route task to any candidate peer",
            ),
        )

    async def find_node_remote(
        self,
        connection: PeerConnection,
        target: Any,
        count: int = 20,
        timeout: float = 3.0,
    ) -> list[Any]:
        """
        Send a FIND_NODE RPC over an established connection and return discovered peer records.
        """
        from meshweaver.dht import NodeID
        from meshweaver.peers import PeerRecord

        target_node = NodeID.from_peer(target)
        req = Message.create(
            MessageType.FIND_NODE,
            sender_id=self.peer_id,
            recipient_id=connection.remote_peer_id,
            payload={"target_id": target_node.hex, "count": count},
        )
        resp = await self.send_request(connection, req, timeout=timeout)
        if resp.message_type != MessageType.NODES_FOUND:
            return []

        raw_nodes = resp.payload.get("nodes", [])
        discovered: list[PeerRecord] = []
        for n in raw_nodes:
            try:
                rec = PeerRecord.from_dict(n)
                discovered.append(rec)
            except Exception:
                pass
        return discovered

    async def iterative_find_node(
        self,
        target: Any,
        *,
        alpha: int = 3,
        k: int = 20,
        query_timeout: float = 2.0,
        overall_timeout: float = 5.0,
        max_queries: int = 50,
        max_rounds: int = 20,
    ) -> Any:
        """
        Perform an asynchronous iterative DHT lookup across network peers for target ID.
        Discovered peers are automatically tracked in the local peer_registry.
        """
        from meshweaver.dht import IterativeLookup, NodeID, PeerContact

        target_node = NodeID.from_peer(target)
        initial = [
            PeerContact.from_peer_record(p)
            for p in self.peer_registry.list_active_peers()
        ]

        async def _query_contact(contact: PeerContact, tgt: NodeID) -> list[Any]:
            conn = self._peer_connections.get(contact.peer_id)
            should_close = False
            if conn is None or conn.is_closed:
                try:
                    conn = await self.connect_to_peer(
                        contact.host,
                        contact.port,
                        timeout=query_timeout,
                        do_handshake=True,
                    )
                    should_close = False
                except Exception:
                    return []

            try:
                records = await self.find_node_remote(conn, tgt, count=k, timeout=query_timeout)
                for rec in records:
                    try:
                        if rec.peer_id != self.peer_id:
                            self.peer_registry.register(rec.peer_id, rec.host, rec.port, metadata=rec.metadata)
                            self.routing_table.add_contact(rec.to_contact())
                    except Exception:
                        pass
                return records
            finally:
                if should_close and conn is not None:
                    await conn.close()

        lookup = IterativeLookup(
            target=target_node,
            initial_peers=initial,
            query_fn=_query_contact,
            alpha=alpha,
            k=k,
            query_timeout=query_timeout,
            overall_timeout=overall_timeout,
            max_queries=max_queries,
            max_rounds=max_rounds,
            local_node_id=self.node_id,
        )
        return await lookup.execute()

    async def join_mesh(
        self,
        seeds: list[str | tuple[str, int]],
        *,
        timeout: float = 5.0,
        discover_peers: bool = True,
    ) -> list[Any]:
        """
        Join a MeshWeaver network via one or more bootstrap seed nodes.

        Connects to each seed endpoint, negotiates handshake, registers peers,
        and optionally queries seed nodes for closest DHT neighbors.
        """
        from meshweaver.peers import PeerEndpoint, PeerRecord

        joined_records: list[PeerRecord] = []

        for seed in seeds:
            try:
                endpoint = PeerEndpoint.from_address(seed)
            except Exception as exc:
                logger.warning("Invalid seed address '%s': %s", seed, exc)
                continue

            try:
                conn = await self.connect_to_peer(
                    endpoint.host,
                    endpoint.port,
                    timeout=timeout,
                    do_handshake=True,
                )
                if conn.remote_peer_id:
                    rec = self.peer_registry.get(conn.remote_peer_id)
                    if rec is not None and rec not in joined_records:
                        joined_records.append(rec)

                # Optionally query seed for neighbors
                if discover_peers:
                    try:
                        neighbors = await self.find_node_remote(
                            conn,
                            self.node_id,
                            count=20,
                            timeout=timeout,
                        )
                        for neighbor in neighbors:
                            try:
                                if neighbor.peer_id != self.peer_id:
                                    reg_rec = self.peer_registry.register(
                                        neighbor.peer_id,
                                        neighbor.host,
                                        neighbor.port,
                                        metadata=neighbor.metadata,
                                    )
                                    self.routing_table.add_contact(reg_rec.to_contact())
                                    if reg_rec not in joined_records:
                                        joined_records.append(reg_rec)
                            except Exception:
                                pass
                    except Exception as disc_exc:
                        logger.debug("Discovery query failed on seed %s: %s", endpoint, disc_exc)

            except Exception as conn_exc:
                logger.warning("Failed to connect to seed peer %s: %s", endpoint, conn_exc)

        # Trigger immediate gossip round if enabled
        if self.gossip_engine is not None and self.gossip_engine.is_running:
            try:
                await self.gossip_engine.run_round()
            except Exception:
                pass

        logger.info(
            "Node %s joined mesh. Discovered/registered %d peer(s)",
            self.peer_id,
            len(joined_records),
        )
        return joined_records

    async def leave_mesh(
        self,
        *,
        graceful: bool = True,
        timeout: float = 2.0,
    ) -> None:
        """
        Leave the MeshWeaver network, announcing departure to active peers
        and performing an orderly shutdown.
        """
        if graceful and self._peer_connections:
            leave_msg = Message.create(
                MessageType.LEAVE,
                sender_id=self.peer_id,
                payload={"node_id": self.peer_id, "reason": "leaving"},
            )
            conns = list(self._peer_connections.values())
            for conn in conns:
                if not conn.is_closed:
                    try:
                        await asyncio.wait_for(conn.send(leave_msg), timeout=timeout)
                    except Exception:
                        pass

            # Brief pause for socket transmission before shutting down
            await asyncio.sleep(0.05)

        logger.info("Node %s leaving mesh", self.peer_id)
        await self.stop()

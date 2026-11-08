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
        security_manager: Any | None = None,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.remote_peer_id = remote_peer_id
        self.is_inbound = is_inbound
        self.security_manager = security_manager
        self.connected_at: float = time.time()
        self._closed: bool = False
        self._write_lock: asyncio.Lock = asyncio.Lock()

    @property
    def is_closed(self) -> bool:
        """Check if the connection has been closed."""
        return self._closed or self.writer.is_closing()

    @property
    def is_tls(self) -> bool:
        """Return True if the underlying connection is encrypted with TLS."""
        try:
            return self.writer.get_extra_info("ssl_object") is not None
        except Exception:
            return False

    @property
    def peer_certificate(self) -> dict[str, Any] | None:
        """Return the peer's decoded TLS certificate if available, or None."""
        try:
            ssl_obj = self.writer.get_extra_info("ssl_object")
            if ssl_obj is not None:
                return ssl_obj.getpeercert()
        except Exception:
            pass
        return None

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

        if self.security_manager is not None:
            message = self.security_manager.prepare_outbound(message)

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
        heartbeat_interval: float = 1.0,
        missed_heartbeat_threshold: int = 3,
        heartbeat_timeout: float = 1.0,
        enable_heartbeat: bool = False,
        security_config: Any | None = None,
        tls_config: Any | None = None,
        shared_secret: bytes | str | None = None,
        auth_token: str | None = None,
        allowed_peers: list[str] | set[str] | None = None,
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
        self.started_at: float | None = None

        # Peer security, TLS context, and authentication manager
        from meshweaver.security import SecurityConfig, SecurityManager, TLSConfig

        resolved_sec = security_config if security_config is not None else SecurityConfig()
        if tls_config is not None:
            resolved_sec.tls = tls_config
        if shared_secret is not None:
            resolved_sec.shared_secret = shared_secret
        if auth_token is not None:
            resolved_sec.auth_token = auth_token
        if allowed_peers is not None:
            resolved_sec.allowed_peers = set(allowed_peers)

        self.security_manager: SecurityManager = SecurityManager(resolved_sec)

        # Active connections: internal mapping of id(conn) -> conn
        self._connections: dict[int, PeerConnection] = {}
        # Active peer directory: peer_id -> conn
        self._peer_connections: dict[str, PeerConnection] = {}
        # Tasks handling connection read loops
        self._listener_tasks: set[asyncio.Task[None]] = set()
        # Pending request-response futures: correlation_id/message_id -> Future[Message]
        self._pending_requests: dict[str, asyncio.Future[Message]] = {}
        # Mapping of conn_id -> set of in-flight message_ids on that connection
        self._connection_requests: dict[int, set[str]] = {}
        # Registered message handlers
        self._handlers: dict[MessageType, MessageHandler] = {}

        # Heartbeat monitoring and failure detection
        from meshweaver.heartbeat import HeartbeatMonitor

        self.heartbeat_monitor: HeartbeatMonitor = HeartbeatMonitor(
            self,
            interval=heartbeat_interval,
            missed_threshold=missed_heartbeat_threshold,
            probe_timeout=heartbeat_timeout,
            auto_start=False,
        )
        self._enable_heartbeat: bool = enable_heartbeat

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

        # Load-aware task scheduler and router
        from meshweaver.scheduler import TaskScheduler
        from meshweaver.routing import TaskRouter

        self.scheduler: TaskScheduler = TaskScheduler(self)
        self.task_scheduler: TaskScheduler = self.scheduler
        self.task_router: TaskRouter = TaskRouter(self, scheduler=self.scheduler)

        # Deduplication and task state cache
        from meshweaver.tasks import TaskDeduplicationStore

        self._task_dedup: TaskDeduplicationStore = TaskDeduplicationStore()
        self._pending_task_responses: dict[str, Any] = {}

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
            from meshweaver.exceptions import AuthenticationError

            try:
                self.security_manager.validate_handshake(msg)
            except AuthenticationError as auth_exc:
                logger.warning("Handshake authentication failed from %s: %s", conn.peer_address, auth_exc)
                return Message.create(
                    MessageType.ERROR,
                    sender_id=self.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                    payload={"code": "AUTHENTICATION_FAILED", "message": str(auth_exc)},
                )

            remote_node_id = str(msg.payload.get("node_id", msg.sender_id))
            conn.remote_peer_id = remote_node_id
            self._peer_connections[remote_node_id] = conn
            port_val = msg.payload.get("port")
            if port_val and isinstance(port_val, int) and not isinstance(port_val, bool):
                remote_host = conn.peer_address[0]
                peer_meta = dict(msg.payload.get("metadata", {}))
                if "supported_tasks" in msg.payload:
                    peer_meta["supported_tasks"] = msg.payload["supported_tasks"]
                if "accepting_tasks" in msg.payload:
                    peer_meta["accepting_tasks"] = msg.payload["accepting_tasks"]
                rec = self.peer_registry.register(remote_node_id, remote_host, port_val, metadata=peer_meta)
                try:
                    self.routing_table.add_contact(rec.to_contact())
                except Exception:
                    pass
            handshake_ack_payload = {
                "node_id": self.peer_id,
                "status": "accepted",
                "supported_tasks": list(self.task_registry.list_tasks()),
            }
            if self.security_manager.config.auth_token:
                handshake_ack_payload["auth_token"] = self.security_manager.config.auth_token
            return Message.create(
                MessageType.HANDSHAKE_ACK,
                sender_id=self.peer_id,
                recipient_id=remote_node_id,
                correlation_id=msg.message_id,
                payload=handshake_ack_payload,
            )

        async def _handle_task_request(msg: Message, _conn: PeerConnection) -> Message:
            from meshweaver.exceptions import InvalidTaskError, ValidationError
            from meshweaver.tasks import TaskAck, TaskErrorInfo, TaskRequest, TaskResponse, TaskStatus

            try:
                task_req = TaskRequest.from_message(msg)
            except (InvalidTaskError, ValidationError, Exception) as parse_exc:
                err_resp = TaskResponse.error(
                    task_id=str(msg.payload.get("task_id", uuid.uuid4().hex)),
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(parse_exc)),
                    correlation_id=msg.correlation_id or msg.message_id,
                )
                return err_resp.to_message(
                    sender_id=self.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                )

            corr_id = msg.correlation_id or task_req.correlation_id or msg.message_id

            # 1. Deduplication check
            if self._task_dedup.is_duplicate(task_req.task_id):
                cached_resp = self._task_dedup.get_result(task_req.task_id)
                if cached_resp is not None:
                    logger.debug("Duplicate task %s already completed, returning cached response", task_req.task_id)
                    dup_resp = TaskResponse(
                        task_id=cached_resp.task_id,
                        status=cached_resp.status,
                        result=cached_resp.result,
                        error=cached_resp.error,
                        execution_time=cached_resp.execution_time,
                        metadata={**cached_resp.metadata, "duplicate": True},
                        correlation_id=corr_id,
                    )
                    return dup_resp.to_message(
                        sender_id=self.peer_id,
                        recipient_id=msg.sender_id,
                        correlation_id=msg.message_id,
                    )

            # 2. Check intermediary routing/relay request
            if task_req.metadata.get("relay") or task_req.metadata.get("route_through") == self.peer_id:
                if not self.task_registry.has_task(task_req.task_name) and self.peer_registry.peer_count > 0:
                    try:
                        # Forward/route to eligible worker in mesh
                        fwd_resp = await self.submit_task(task_req, correlation_id=corr_id, retry_on_failure=True)
                        if isinstance(fwd_resp, TaskResponse):
                            self._task_dedup.record_result(task_req.task_id, fwd_resp)
                            return fwd_resp.to_message(
                                sender_id=self.peer_id,
                                recipient_id=msg.sender_id,
                                correlation_id=msg.message_id,
                            )
                    except Exception as fwd_exc:
                        logger.warning("Failed to relay task %s: %s", task_req.task_id, fwd_exc)

            # 3. Register submission
            ack = TaskAck(
                task_id=task_req.task_id,
                correlation_id=corr_id,
                status="accepted",
                routed_to=self.peer_id,
                metadata={"worker": self.peer_id},
            )
            self._task_dedup.register_submission(task_req.task_id, corr_id, routed_to=self.peer_id, ack=ack)

            # 4. Send immediate ACK if requested
            if msg.payload.get("require_ack") or task_req.metadata.get("require_ack"):
                try:
                    ack_msg = ack.to_message(
                        sender_id=self.peer_id,
                        recipient_id=msg.sender_id,
                        correlation_id=msg.message_id,
                    )
                    await _conn.send(ack_msg)
                except Exception:
                    pass

            # 5. Local worker execution
            task_resp = await self._execute_local_task(task_req, corr_id)
            task_resp.metadata["ack"] = ack.to_dict()

            return task_resp.to_message(
                sender_id=self.peer_id,
                recipient_id=msg.sender_id,
                correlation_id=msg.message_id,
            )

        async def _handle_task_status_request(msg: Message, _conn: PeerConnection) -> Message:
            from meshweaver.tasks import TaskErrorInfo, TaskStatus, TaskStatusReport

            task_id = str(msg.payload.get("task_id", ""))
            corr_id = msg.correlation_id or msg.message_id

            # 1. Local execution engine / worker
            rec = self.get_task(task_id)
            if rec is not None:
                st = rec.state if hasattr(rec, "state") else getattr(rec, "status", TaskStatus.PENDING)
                try:
                    status_enum = TaskStatus(st)
                except (ValueError, TypeError):
                    try:
                        status_enum = TaskStatus(str(st).lower())
                    except (ValueError, TypeError):
                        status_enum = TaskStatus.PENDING
                err = getattr(rec, "error", None)
                if isinstance(err, str):
                    err = TaskErrorInfo(code="ERROR", message=err)

                report = TaskStatusReport(
                    task_id=task_id,
                    status=status_enum,
                    correlation_id=corr_id,
                    worker_peer_id=self.peer_id,
                    created_at=getattr(rec, "created_at", None),
                    started_at=getattr(rec, "started_at", None),
                    completed_at=getattr(rec, "completed_at", None),
                    execution_time=getattr(rec, "duration", None),
                    result=getattr(rec, "result", None),
                    error=err,
                    metadata={"worker": self.peer_id},
                )
                return report.to_message(
                    sender_id=self.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                )

            # 2. Deduplication store
            dedup = self._task_dedup.get_record(task_id)
            if dedup is not None:
                routed_to = dedup.get("routed_to")
                if routed_to and routed_to != self.peer_id:
                    try:
                        remote_rep = await self.query_task_status(task_id, target_peer_id=routed_to, timeout=2.0)
                        return remote_rep.to_message(
                            sender_id=self.peer_id,
                            recipient_id=msg.sender_id,
                            correlation_id=msg.message_id,
                        )
                    except Exception:
                        pass

                resp = dedup.get("response")
                if resp is not None:
                    report = TaskStatusReport(
                        task_id=task_id,
                        status=resp.status,
                        correlation_id=corr_id,
                        worker_peer_id=dedup.get("routed_to") or self.peer_id,
                        execution_time=resp.execution_time,
                        result=resp.result,
                        error=resp.error,
                        metadata=resp.metadata,
                    )
                    return report.to_message(
                        sender_id=self.peer_id,
                        recipient_id=msg.sender_id,
                        correlation_id=msg.message_id,
                    )
                report = TaskStatusReport(
                    task_id=task_id,
                    status=dedup.get("status", TaskStatus.RUNNING if dedup.get("future") else TaskStatus.PENDING),
                    correlation_id=corr_id,
                    worker_peer_id=dedup.get("routed_to") or self.peer_id,
                )
                return report.to_message(
                    sender_id=self.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                )

            # 3. Not found
            report = TaskStatusReport(
                task_id=task_id,
                status=TaskStatus.ERROR,
                correlation_id=corr_id,
                worker_peer_id=self.peer_id,
                error=TaskErrorInfo(code="TASK_NOT_FOUND", message=f"Task '{task_id}' not found on node {self.peer_id}"),
            )
            return report.to_message(
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

        async def _handle_heartbeat(msg: Message, _conn: PeerConnection) -> Message:
            if self.peer_registry.has_peer(msg.sender_id):
                try:
                    self.peer_registry.touch(msg.sender_id)
                except Exception:
                    pass
            return Message.create(
                MessageType.HEARTBEAT_ACK,
                sender_id=self.peer_id,
                recipient_id=msg.sender_id,
                correlation_id=msg.message_id,
                payload={
                    "node_id": self.peer_id,
                    "status": "ok",
                    "timestamp": time.time(),
                },
            )

        self.register_handler(MessageType.PING, _handle_ping)
        self.register_handler(MessageType.HANDSHAKE, _handle_handshake)
        self.register_handler(MessageType.HEARTBEAT, _handle_heartbeat)
        self.register_handler(MessageType.TASK_REQUEST, _handle_task_request)
        self.register_handler(MessageType.TASK_STATUS_REQUEST, _handle_task_status_request)
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

    def enable_heartbeats(
        self,
        interval: float | None = None,
        missed_threshold: int | None = None,
        probe_timeout: float | None = None,
    ) -> Any:
        """
        Enable and configure active heartbeat monitoring and failure detection.
        """
        if interval is not None:
            self.heartbeat_monitor.interval = interval
        if missed_threshold is not None:
            self.heartbeat_monitor.missed_threshold = missed_threshold
        if probe_timeout is not None:
            self.heartbeat_monitor.probe_timeout = probe_timeout
        self._enable_heartbeat = True
        if self._is_running and not self.heartbeat_monitor.is_running:
            self.heartbeat_monitor.start()
        return self.heartbeat_monitor

    async def start(self) -> None:
        """
        Start the asynchronous TCP peer server.
        """
        if self._is_running:
            return

        server_ssl = self.security_manager.get_server_ssl_context()
        self._server = await asyncio.start_server(
            self._handle_inbound_stream,
            host=self.host,
            port=self.port,
            ssl=server_ssl,
        )

        sockets = self._server.sockets
        if sockets:
            self._bound_port = sockets[0].getsockname()[1]
        self._is_running = True
        self.started_at = time.time()
        logger.info("PeerNode %s listening on %s:%d", self.peer_id, self.host, self.bound_port)

        if self.gossip_engine is not None and not self.gossip_engine.is_running:
            self.gossip_engine.start()

        if (self._enable_heartbeat or self.heartbeat_monitor.is_running) and not self.heartbeat_monitor.is_running:
            self.heartbeat_monitor.start()

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

        # Stop heartbeat monitor first
        if hasattr(self, "heartbeat_monitor") and self.heartbeat_monitor.is_running:
            await self.heartbeat_monitor.stop()

        # Stop gossip engine
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
        conn = PeerConnection(reader, writer, is_inbound=True, security_manager=self.security_manager)
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
        server_hostname: str | None = None,
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
        client_ssl = self.security_manager.get_client_ssl_context()
        effective_hostname = server_hostname or (
            host if (client_ssl and self.security_manager.config.tls.check_hostname) else None
        )
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, ssl=client_ssl, server_hostname=effective_hostname),
                timeout=timeout,
            )
        except TimeoutError as exc:
            raise NetworkTimeoutError(f"Connection to {host}:{port} timed out after {timeout}s") from exc
        except (OSError, ConnectionRefusedError) as exc:
            raise PeerConnectionError(f"Failed to connect to {host}:{port}: {exc}") from exc

        conn = PeerConnection(reader, writer, is_inbound=False, security_manager=self.security_manager)
        self._connections[id(conn)] = conn

        if do_handshake:
            try:
                handshake_payload = {
                    "node_id": self.peer_id,
                    "port": self.bound_port,
                    "supported_tasks": list(self.task_registry.list_tasks()),
                }
                if self.security_manager.config.auth_token:
                    handshake_payload["auth_token"] = self.security_manager.config.auth_token

                handshake_msg = Message.create(
                    MessageType.HANDSHAKE,
                    sender_id=self.peer_id,
                    payload=handshake_payload,
                )
                await conn.send(handshake_msg)
                resp = await conn.receive(timeout=timeout)
                if resp is None:
                    await conn.close()
                    raise HandshakeError("Connection closed before HANDSHAKE_ACK was received")

                if resp.message_type == MessageType.ERROR:
                    err_detail = resp.payload.get("message", "Handshake rejected by remote peer")
                    await conn.close()
                    raise HandshakeError(f"Handshake rejected: {err_detail}")

                if resp.message_type != MessageType.HANDSHAKE_ACK:
                    await conn.close()
                    raise HandshakeError(f"Expected HANDSHAKE_ACK, received {resp.message_type}")

                # Validate remote peer credentials
                self.security_manager.validate_handshake(resp)

                remote_node_id = str(resp.payload.get("node_id", resp.sender_id))
                conn.remote_peer_id = remote_node_id
                self._peer_connections[remote_node_id] = conn
                peer_meta = dict(resp.payload.get("metadata", {}))
                if "supported_tasks" in resp.payload:
                    peer_meta["supported_tasks"] = resp.payload["supported_tasks"]
                if "accepting_tasks" in resp.payload:
                    peer_meta["accepting_tasks"] = resp.payload["accepting_tasks"]
                rec = self.peer_registry.register(remote_node_id, host, port, metadata=peer_meta)
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

                # Comprehensive security validation (replay, integrity, authorization, size limits)
                from meshweaver.exceptions import (
                    AuthenticationError,
                    IntegrityError,
                    PayloadLimitExceededError,
                    ReplayAttackError,
                    SecurityError,
                )

                try:
                    self.security_manager.validate_inbound(msg, remote_peer_id=conn.remote_peer_id)
                except (SecurityError, AuthenticationError, IntegrityError, ReplayAttackError, PayloadLimitExceededError) as sec_exc:
                    logger.warning(
                        "Security validation failed for message %s from %s: %s",
                        msg.message_id,
                        conn.peer_address,
                        sec_exc,
                    )
                    try:
                        err_reply = Message.create(
                            MessageType.ERROR,
                            sender_id=self.peer_id,
                            recipient_id=msg.sender_id,
                            correlation_id=msg.message_id,
                            payload={"code": sec_exc.__class__.__name__, "message": str(sec_exc)},
                        )
                        await conn.send(err_reply)
                    except Exception:
                        pass
                    continue

                # Correlated response resolution
                if msg.correlation_id and msg.correlation_id in self._pending_requests:
                    future = self._pending_requests[msg.correlation_id]
                    if not future.done():
                        future.set_result(msg)
                    continue

                # Normal message dispatch to registered handler
                handler = self._handlers.get(msg.message_type)
                if handler is not None:
                    async def _dispatch_handler(h: MessageHandler, m: Message, c: PeerConnection) -> None:
                        try:
                            reply = await h(m, c)
                            if reply is not None and not c.is_closed:
                                await c.send(reply)
                        except Exception as handler_exc:
                            logger.exception("Error executing handler for %s", m.message_type)
                            try:
                                if not c.is_closed:
                                    err_reply = Message.create(
                                        MessageType.ERROR,
                                        sender_id=self.peer_id,
                                        recipient_id=m.sender_id,
                                        correlation_id=m.message_id,
                                        payload={"code": "HANDLER_ERROR", "message": str(handler_exc)},
                                    )
                                    await c.send(err_reply)
                            except Exception:
                                pass

                    handler_task = asyncio.create_task(_dispatch_handler(handler, msg, conn))
                    self._listener_tasks.add(handler_task)
                    handler_task.add_done_callback(self._listener_tasks.discard)
                else:
                    logger.debug("No handler for message type %s", msg.message_type)

        except asyncio.CancelledError:
            pass
        finally:
            await conn.close()
            self._connections.pop(conn_id, None)
            if conn.remote_peer_id and conn.remote_peer_id in self._peer_connections:
                self._peer_connections.pop(conn.remote_peer_id, None)

            # Immediately fail in-flight requests on this broken connection with ConnectionClosedError
            pending_ids = self._connection_requests.pop(conn_id, set())
            for msg_id in pending_ids:
                fut = self._pending_requests.get(msg_id)
                if fut is not None and not fut.done():
                    fut.set_exception(
                        ConnectionClosedError(
                            f"Connection to worker {conn.remote_peer_id or conn.peer_address} closed while request was in-flight"
                        )
                    )

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
        conn_id = id(connection)
        self._pending_requests[message.message_id] = future
        self._connection_requests.setdefault(conn_id, set()).add(message.message_id)

        try:
            await connection.send(message)
            return await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError as exc:
            raise NetworkTimeoutError(
                f"Request {message.message_id} ({message.message_type}) timed out after {timeout}s"
            ) from exc
        finally:
            self._pending_requests.pop(message.message_id, None)
            req_set = self._connection_requests.get(conn_id)
            if req_set is not None:
                req_set.discard(message.message_id)
                if not req_set:
                    self._connection_requests.pop(conn_id, None)

    async def send_message(self, connection: PeerConnection, message: Message) -> None:
        """Send a one-way message over an existing connection without awaiting reply."""
        await connection.send(message)

    async def _execute_local_task(self, req: Any, corr_id: str | None = None) -> Any:
        """Execute a validated TaskRequest locally using the execution engine."""
        from meshweaver.tasks import TaskErrorInfo, TaskResponse, TaskStatus

        self._active_task_counter += 1
        self._task_dedup.set_status(req.task_id, TaskStatus.RUNNING)
        try:
            task_resp = await self.execution_engine.execute_request(req)
        except Exception as exec_exc:
            task_resp = TaskResponse.error(
                task_id=req.task_id,
                error=TaskErrorInfo(code="EXECUTION_ERROR", message=str(exec_exc)),
            )
        finally:
            self._active_task_counter = max(0, self._active_task_counter - 1)

        task_resp.correlation_id = corr_id or req.correlation_id or req.task_id
        self._task_dedup.record_result(req.task_id, task_resp)
        return task_resp

    def select_candidate_workers(
        self,
        task_request: Any,
        *,
        count: int = 1,
        exclude: list[str] | set[str] | None = None,
    ) -> list[Any]:
        """
        Select candidate worker peers using the TaskScheduler with TaskRouter fallback.
        """
        exclude_list = list(exclude) if exclude else [self.peer_id]
        candidate_records = []
        if hasattr(self, "task_scheduler") and self.task_scheduler is not None:
            try:
                candidate_records = self.task_scheduler.select_candidate_workers(
                    task_request,
                    count=count,
                    exclude=exclude_list,
                )
            except Exception:
                pass

        if not candidate_records and hasattr(self, "task_router") and self.task_router is not None:
            try:
                candidate_records = self.task_router.select_candidates(
                    task_request,
                    count=count,
                    exclude=exclude_list,
                )
            except Exception:
                pass

        return candidate_records

    async def submit_task(
        self,
        task_name_or_request: str | Any,
        *args: Any,
        target_peer_id: str | None = None,
        timeout: float = 5.0,
        task_id: str | None = None,
        correlation_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        retry_on_failure: bool = True,
        max_retries: int = 2,
        max_candidates: int = 3,
        wait: bool = True,
        on_ack: Any | None = None,
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
            ConnectionClosedError,
            InvalidTaskError,
            NetworkTimeoutError,
            ValidationError,
        )
        from meshweaver.tasks import (
            TaskAck,
            TaskErrorInfo,
            TaskOwnership,
            TaskRequest,
            TaskResponse,
            TaskStatus,
        )

        corr_id = correlation_id or uuid.uuid4().hex

        # 1. Parse or create validated TaskRequest
        if isinstance(task_name_or_request, TaskRequest):
            req = task_name_or_request
            if not req.correlation_id:
                try:
                    object.__setattr__(req, "correlation_id", corr_id)
                except Exception:
                    pass
        else:
            try:
                req = TaskRequest.create(
                    task_name=str(task_name_or_request),
                    args=args if args else None,
                    kwargs=kwargs if kwargs else None,
                    task_id=task_id,
                    metadata=metadata,
                    correlation_id=corr_id,
                )
            except (InvalidTaskError, ValidationError) as exc:
                return TaskResponse.error(
                    task_id=task_id if task_id else uuid.uuid4().hex,
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(exc)),
                    correlation_id=corr_id,
                )
            except Exception as exc:
                return TaskResponse.error(
                    task_id=task_id if task_id else uuid.uuid4().hex,
                    error=TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(exc)),
                    correlation_id=corr_id,
                )

        # 2. Check duplicate submission
        if self._task_dedup.is_duplicate(req.task_id):
            if wait:
                cached = self._task_dedup.get_result(req.task_id)
                if cached is not None:
                    dup_resp = TaskResponse(
                        task_id=cached.task_id,
                        status=cached.status,
                        result=cached.result,
                        error=cached.error,
                        execution_time=cached.execution_time,
                        metadata={**cached.metadata, "duplicate": True},
                        correlation_id=corr_id,
                    )
                    return dup_resp
                fut = self._task_dedup.get_future(req.task_id)
                if fut is not None:
                    try:
                        return await asyncio.wait_for(fut, timeout=timeout)
                    except TimeoutError:
                        return TaskResponse.error(
                            task_id=req.task_id,
                            error=TaskErrorInfo(
                                code="TASK_TIMEOUT",
                                message=f"Duplicate submission for task {req.task_id} timed out awaiting existing execution",
                            ),
                            correlation_id=corr_id,
                        )
            else:
                cached_rec = self._task_dedup.get_record(req.task_id)
                return TaskAck(
                    task_id=req.task_id,
                    correlation_id=corr_id,
                    status="duplicate",
                    routed_to=cached_rec.get("routed_to") if cached_rec else None,
                    metadata={"duplicate": True, "already_submitted": True},
                )

        # Local execution short-circuit
        if target_peer_id is not None and target_peer_id == self.peer_id:
            ack = TaskAck(
                task_id=req.task_id,
                correlation_id=corr_id,
                status="accepted",
                routed_to=self.peer_id,
                metadata={"worker": self.peer_id, "local": True},
            )
            self._task_dedup.register_submission(
                req.task_id,
                corr_id,
                routed_to=self.peer_id,
                ack=ack,
                owner_id=self.peer_id,
                max_retries=max_retries,
            )
            if on_ack is not None:
                try:
                    on_ack(ack)
                except Exception:
                    pass
            if not wait:
                asyncio.create_task(self._execute_local_task(req, corr_id))
                return ack
            resp = await self._execute_local_task(req, corr_id)
            resp.metadata["ack"] = ack.to_dict()
            ownership = self._task_dedup.get_ownership(req.task_id)
            if ownership is not None:
                ownership.record_success()
                resp.metadata["ownership"] = ownership.to_dict()
            return resp

        # Register submission and track task ownership
        self._task_dedup.register_submission(
            req.task_id,
            corr_id,
            routed_to=target_peer_id,
            ack=None,
            owner_id=self.peer_id,
            max_retries=max_retries,
        )
        ownership = self._task_dedup.get_ownership(req.task_id)
        if ownership is None:
            ownership = TaskOwnership(
                task_id=req.task_id,
                owner_id=self.peer_id,
                assigned_worker_id=target_peer_id,
                max_retries=max_retries,
            )

        attempted_workers: list[str] = []
        last_error: TaskErrorInfo | None = None

        while True:
            # 3. Identify candidate peer for this attempt
            if target_peer_id is not None:
                if attempted_workers:
                    # Pinned target peer was already attempted
                    break
                candidate_id = target_peer_id
                rec = self.peer_registry.get(target_peer_id)
                if rec is not None:
                    host, port = rec.host, rec.port
                else:
                    conn = self._peer_connections.get(target_peer_id)
                    if conn is not None and not conn.is_closed:
                        addr = conn.peer_address
                        host, port = addr[0], addr[1]
                    else:
                        return TaskResponse.error(
                            task_id=req.task_id,
                            error=TaskErrorInfo(
                                code="PEER_UNAVAILABLE",
                                message=f"Target peer '{target_peer_id}' is not found or reachable in mesh",
                            ),
                            correlation_id=corr_id,
                        )
            else:
                # Select best available candidate peer via scheduler (excluding self & attempted workers)
                candidate_records = self.select_candidate_workers(
                    req,
                    count=1,
                    exclude=[self.peer_id] + attempted_workers,
                )

                if not candidate_records:
                    if not attempted_workers:
                        return TaskResponse.error(
                            task_id=req.task_id,
                            error=TaskErrorInfo(
                                code="NO_PEERS_AVAILABLE",
                                message="No available or reachable worker peers found in mesh to route task",
                            ),
                            correlation_id=corr_id,
                        )
                    # All available candidate peers exhausted
                    break

                selected = candidate_records[0]
                candidate_id, host, port = selected.peer_id, selected.host, selected.port

            attempted_workers.append(candidate_id)
            ownership.assign_worker(candidate_id)

            # 4. Connect to candidate worker
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
                    if hasattr(self, "heartbeat_monitor") and self.heartbeat_monitor is not None:
                        self.heartbeat_monitor.record_peer_failure(candidate_id, f"Connection failed: {conn_exc}")
                    self.peer_registry.record_heartbeat_failure(candidate_id, max_missed=1)

                    last_error = TaskErrorInfo(
                        code="PEER_UNREACHABLE",
                        message=f"Failed to connect to worker peer {candidate_id} at {host}:{port}: {conn_exc}",
                    )
                    can_retry = ownership.record_failure(f"Failed to connect to {candidate_id}: {conn_exc}")
                    if retry_on_failure and can_retry:
                        continue
                    break

            # Create acknowledgement
            ack = TaskAck(
                task_id=req.task_id,
                correlation_id=corr_id,
                status="accepted",
                routed_to=candidate_id,
                metadata={"worker": candidate_id, "host": host, "port": port},
            )
            dedup_rec = self._task_dedup.get_record(req.task_id)
            if dedup_rec is not None:
                dedup_rec["ack"] = ack
                dedup_rec["routed_to"] = candidate_id

            if on_ack is not None:
                try:
                    on_ack(ack)
                except Exception:
                    pass

            if not wait:
                # Asynchronous fire-and-forget submission
                msg = req.to_message(sender_id=self.peer_id, recipient_id=candidate_id, correlation_id=corr_id)
                await self.send_message(conn, msg)
                return ack

            msg = req.to_message(sender_id=self.peer_id, recipient_id=candidate_id, correlation_id=corr_id)
            try:
                resp_msg = await self.send_request(conn, msg, timeout=timeout)
            except (ConnectionClosedError, OSError) as conn_closed_exc:
                logger.warning(
                    "Worker %s disconnected during task %s execution: %s. "
                    "Note: Retrying task provides at-least-once execution semantics and may execute "
                    "the task more than once if %s performed computation before disconnecting; "
                    "exactly-once execution is not guaranteed.",
                    candidate_id,
                    req.task_id,
                    conn_closed_exc,
                    candidate_id,
                )
                if hasattr(self, "heartbeat_monitor") and self.heartbeat_monitor is not None:
                    self.heartbeat_monitor.record_peer_failure(candidate_id, f"Worker disconnected: {conn_closed_exc}")
                self.peer_registry.record_heartbeat_failure(candidate_id, max_missed=1)

                last_error = TaskErrorInfo(
                    code="WORKER_DISCONNECTED",
                    message=f"Worker {candidate_id} disconnected during task execution: {conn_closed_exc}",
                )
                can_retry = ownership.record_failure(f"Worker {candidate_id} disconnected: {conn_closed_exc}")
                if retry_on_failure and can_retry:
                    continue
                break
            except NetworkTimeoutError as timeout_exc:
                logger.warning(
                    "Task %s execution timed out on worker %s: %s. "
                    "Note: Retrying task provides at-least-once execution semantics and may execute "
                    "the task more than once; exactly-once execution is not guaranteed.",
                    req.task_id,
                    candidate_id,
                    timeout_exc,
                )
                if hasattr(self, "heartbeat_monitor") and self.heartbeat_monitor is not None:
                    self.heartbeat_monitor.record_peer_failure(candidate_id, f"Timeout: {timeout_exc}")
                self.peer_registry.record_heartbeat_failure(candidate_id, max_missed=1)

                last_error = TaskErrorInfo(
                    code="TASK_TIMEOUT",
                    message=f"Task execution on worker {candidate_id} timed out after {timeout}s",
                )
                can_retry = ownership.record_failure(f"Worker {candidate_id} timed out: {timeout_exc}")
                if retry_on_failure and can_retry:
                    continue
                break
            except Exception as net_exc:
                logger.warning("Communication error during task exchange with %s: %s", candidate_id, net_exc)
                if hasattr(self, "heartbeat_monitor") and self.heartbeat_monitor is not None:
                    self.heartbeat_monitor.record_peer_failure(candidate_id, f"Communication error: {net_exc}")
                self.peer_registry.record_heartbeat_failure(candidate_id, max_missed=1)

                last_error = TaskErrorInfo(
                    code="COMMUNICATION_ERROR",
                    message=f"Communication error with worker {candidate_id}: {net_exc}",
                )
                can_retry = ownership.record_failure(f"Communication error with {candidate_id}: {net_exc}")
                if retry_on_failure and can_retry:
                    continue
                break

            if resp_msg.message_type == MessageType.TASK_RESPONSE:
                try:
                    resp = TaskResponse.from_message(resp_msg)
                    resp.correlation_id = corr_id
                    resp.metadata["ack"] = ack.to_dict()
                    resp.metadata["ownership"] = ownership.to_dict()
                    ownership.record_success()
                    if ownership.retry_count > 0:
                        resp.metadata["retried"] = True
                        resp.metadata["retry_count"] = ownership.retry_count
                        resp.metadata["worker_history"] = list(ownership.worker_history)
                        resp.metadata["execution_semantics"] = "at-least-once"
                        resp.metadata["execution_warning"] = (
                            "Retrying a task provides at-least-once execution semantics; "
                            "the task may have executed more than once if the prior worker "
                            "disconnected after starting execution."
                        )
                    self._task_dedup.record_result(req.task_id, resp)
                    return resp
                except Exception as parse_exc:
                    return TaskResponse.error(
                        task_id=req.task_id,
                        error=TaskErrorInfo(
                            code="INVALID_RESPONSE",
                            message=f"Malformed task response from {candidate_id}: {parse_exc}",
                        ),
                        correlation_id=corr_id,
                    )
            elif resp_msg.message_type == MessageType.ERROR:
                err_resp = TaskResponse.error(
                    task_id=req.task_id,
                    error=TaskErrorInfo(
                        code=str(resp_msg.payload.get("code", "REMOTE_ERROR")),
                        message=str(resp_msg.payload.get("message", "Remote peer returned an error")),
                    ),
                    correlation_id=corr_id,
                )
                err_resp.metadata["ownership"] = ownership.to_dict()
                self._task_dedup.record_result(req.task_id, err_resp)
                return err_resp
            else:
                return TaskResponse.error(
                    task_id=req.task_id,
                    error=TaskErrorInfo(
                        code="UNEXPECTED_MESSAGE_TYPE",
                        message=f"Received unexpected message type {resp_msg.message_type} from {candidate_id}",
                    ),
                    correlation_id=corr_id,
                )

        # 5. Handle terminal failure when retry bounds are exhausted or no more peers
        if ownership.retry_count >= max_retries and last_error is not None:
            err_code = "MAX_RETRIES_EXCEEDED"
            err_msg = (
                f"Task {req.task_id} failed after {ownership.retry_count} retries: "
                f"retry limit ({max_retries}) reached. Last error: {last_error.message}"
            )
        elif last_error is not None:
            err_code = last_error.code
            err_msg = last_error.message
        else:
            err_code = "ROUTING_FAILED"
            err_msg = "Failed to route task to any candidate peer"

        final_err_resp = TaskResponse.error(
            task_id=req.task_id,
            error=TaskErrorInfo(code=err_code, message=err_msg),
            correlation_id=corr_id,
        )
        final_err_resp.metadata["ownership"] = ownership.to_dict()
        final_err_resp.metadata["execution_semantics"] = "at-least-once"
        self._task_dedup.record_result(req.task_id, final_err_resp)
        return final_err_resp

    async def submit_task_async(
        self,
        task_name_or_request: str | Any,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Submit a task asynchronously and return TaskAck immediately upon acknowledgement.
        """
        from meshweaver.tasks import TaskAck

        kwargs["wait"] = False
        res = await self.submit_task(task_name_or_request, *args, **kwargs)
        if isinstance(res, TaskAck):
            return res
        if hasattr(res, "ack") and res.ack is not None:
            if isinstance(res.ack, TaskAck):
                return res.ack
            if isinstance(res.ack, dict):
                return TaskAck.from_dict(res.ack)
        return TaskAck(
            task_id=getattr(res, "task_id", uuid.uuid4().hex),
            correlation_id=getattr(res, "correlation_id", uuid.uuid4().hex),
            status="accepted",
        )

    async def query_task_status(
        self,
        task_id: str,
        target_peer_id: str | None = None,
        timeout: float = 3.0,
    ) -> Any:
        """
        Query real-time execution status of a task across the mesh.

        Checks the local node if target_peer_id is None or matches self.peer_id.
        If the task was routed to another peer, queries that peer over TCP.
        """
        from meshweaver.tasks import TaskErrorInfo, TaskStatus, TaskStatusReport

        corr_id = uuid.uuid4().hex

        # 1. Target is self or not specified: check locally
        if target_peer_id is None or target_peer_id == self.peer_id:
            # Check local task record
            rec = self.get_task(task_id)
            if rec is not None:
                st = rec.state if hasattr(rec, "state") else getattr(rec, "status", TaskStatus.PENDING)
                try:
                    status_enum = TaskStatus(st)
                except (ValueError, TypeError):
                    try:
                        status_enum = TaskStatus(str(st).lower())
                    except (ValueError, TypeError):
                        status_enum = TaskStatus.PENDING
                err = getattr(rec, "error", None)
                if isinstance(err, str):
                    err = TaskErrorInfo(code="ERROR", message=err)

                return TaskStatusReport(
                    task_id=task_id,
                    status=status_enum,
                    correlation_id=corr_id,
                    worker_peer_id=self.peer_id,
                    created_at=getattr(rec, "created_at", None),
                    started_at=getattr(rec, "started_at", None),
                    completed_at=getattr(rec, "completed_at", None),
                    execution_time=getattr(rec, "duration", None),
                    result=getattr(rec, "result", None),
                    error=err,
                    metadata={"worker": self.peer_id},
                )

            # Check deduplication store
            dedup = self._task_dedup.get_record(task_id)
            if dedup is not None:
                routed_to = dedup.get("routed_to")
                if routed_to and routed_to != self.peer_id:
                    # Target is the routed peer
                    target_peer_id = routed_to
                else:
                    resp = dedup.get("response")
                    if resp is not None:
                        return TaskStatusReport(
                            task_id=task_id,
                            status=resp.status,
                            correlation_id=corr_id,
                            worker_peer_id=self.peer_id,
                            execution_time=resp.execution_time,
                            result=resp.result,
                            error=resp.error,
                            metadata=resp.metadata,
                        )
                    return TaskStatusReport(
                        task_id=task_id,
                        status=TaskStatus.RUNNING if dedup.get("future") else TaskStatus.PENDING,
                        correlation_id=corr_id,
                        worker_peer_id=self.peer_id,
                    )

        # 2. Remote target query
        if target_peer_id is not None and target_peer_id != self.peer_id:
            conn = self._peer_connections.get(target_peer_id)
            if conn is None or conn.is_closed:
                rec = self.peer_registry.get(target_peer_id)
                if rec is not None:
                    conn = await self.connect_to_peer(rec.host, rec.port, timeout=timeout)
                else:
                    return TaskStatusReport(
                        task_id=task_id,
                        status=TaskStatus.ERROR,
                        correlation_id=corr_id,
                        worker_peer_id=target_peer_id,
                        error=TaskErrorInfo(code="PEER_UNREACHABLE", message=f"Peer '{target_peer_id}' not found in registry"),
                    )

            msg = Message.create(
                MessageType.TASK_STATUS_REQUEST,
                sender_id=self.peer_id,
                recipient_id=target_peer_id,
                correlation_id=corr_id,
                payload={"task_id": task_id, "correlation_id": corr_id},
            )
            try:
                resp_msg = await self.send_request(conn, msg, timeout=timeout)
                if resp_msg.message_type == MessageType.TASK_STATUS_RESPONSE:
                    return TaskStatusReport.from_message(resp_msg)
            except Exception as exc:
                return TaskStatusReport(
                    task_id=task_id,
                    status=TaskStatus.ERROR,
                    correlation_id=corr_id,
                    worker_peer_id=target_peer_id,
                    error=TaskErrorInfo(code="STATUS_QUERY_ERROR", message=f"Failed to query status from {target_peer_id}: {exc}"),
                )

        # 3. Not found
        return TaskStatusReport(
            task_id=task_id,
            status=TaskStatus.ERROR,
            correlation_id=corr_id,
            error=TaskErrorInfo(code="TASK_NOT_FOUND", message=f"Task '{task_id}' not found"),
        )

    async def wait_for_task(
        self,
        task_id: str,
        target_peer_id: str | None = None,
        timeout: float = 10.0,
        poll_interval: float = 0.05,
    ) -> Any:
        """
        Wait for a previously submitted task to reach terminal state, returning TaskResponse.
        """
        from meshweaver.tasks import TaskErrorInfo, TaskResponse

        start_t = time.time()
        while time.time() - start_t < timeout:
            report = await self.query_task_status(task_id, target_peer_id=target_peer_id, timeout=min(3.0, timeout))
            if report.is_done:
                return TaskResponse(
                    task_id=task_id,
                    status=report.status,
                    result=report.result,
                    error=report.error,
                    execution_time=report.execution_time,
                    metadata=report.metadata,
                    correlation_id=report.correlation_id,
                )
            await asyncio.sleep(poll_interval)

        return TaskResponse.error(
            task_id=task_id,
            error=TaskErrorInfo(code="TASK_TIMEOUT", message=f"wait_for_task timed out after {timeout}s"),
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

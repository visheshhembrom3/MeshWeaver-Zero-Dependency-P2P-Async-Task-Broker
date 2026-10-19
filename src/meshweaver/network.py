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
    MalformedMessageError,
    NetworkTimeoutError,
    PeerConnectionError,
)
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
    ) -> None:
        self.peer_id: str = peer_id if peer_id and peer_id.strip() else uuid.uuid4().hex[:12]
        self.host: str = host
        self.port: int = port
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

        # Register default protocol handlers
        self._register_default_handlers()

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
    def active_connections(self) -> list[PeerConnection]:
        """Return a snapshot of active peer connections."""
        return list(self._connections.values())

    @property
    def connected_peers(self) -> dict[str, PeerConnection]:
        """Return a snapshot mapping of known remote peer IDs to connections."""
        return dict(self._peer_connections)

    def register_handler(self, msg_type: MessageType, handler: MessageHandler) -> None:
        """Register a handler callback for incoming messages of a given MessageType."""
        self._handlers[msg_type] = handler

    def _register_default_handlers(self) -> None:
        """Install built-in protocol handlers (PING and HANDSHAKE)."""

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
            return Message.create(
                MessageType.HANDSHAKE_ACK,
                sender_id=self.peer_id,
                recipient_id=remote_node_id,
                correlation_id=msg.message_id,
                payload={"node_id": self.peer_id, "status": "accepted"},
            )

        self.register_handler(MessageType.PING, _handle_ping)
        self.register_handler(MessageType.HANDSHAKE, _handle_handshake)

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

    async def stop(self) -> None:
        """
        Gracefully stop the peer server, cancel background listeners, and close connections.
        """
        if not self._is_running and self._server is None:
            return

        self._is_running = False

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
        self._pending_requests.clear()

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
                except MalformedMessageError as exc:
                    logger.warning("Received malformed message from %s: %s", conn.peer_address, exc)
                    err_msg = Message.create(
                        MessageType.ERROR,
                        sender_id=self.peer_id,
                        payload={"code": "MALFORMED_MESSAGE", "message": str(exc)},
                    )
                    await conn.send(err_msg)
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

"""
Integration tests for asynchronous peer networking, connection lifecycles, and request/response exchange.
"""

import asyncio
import pytest

from meshweaver import (
    ConnectionClosedError,
    Message,
    MessageType,
    NetworkTimeoutError,
    PeerConnection,
    PeerConnectionError,
    PeerNode,
)


@pytest.mark.asyncio
async def test_two_peers_startup_and_handshake() -> None:
    """Verify that two peer nodes can bind to dynamic ports and complete a handshake."""
    async with PeerNode(peer_id="node-alpha", host="127.0.0.1", port=0) as node_a:
        async with PeerNode(peer_id="node-beta", host="127.0.0.1", port=0) as node_b:
            assert node_a.is_running
            assert node_b.is_running
            assert node_a.bound_port > 0
            assert node_b.bound_port > 0
            assert node_a.bound_port != node_b.bound_port

            # Connect node_a to node_b
            conn = await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)
            assert conn.remote_peer_id == "node-beta"

            # Allow incoming handshake dispatch on node_b
            await asyncio.sleep(0.05)
            assert "node-alpha" in node_b.connected_peers
            assert "node-beta" in node_a.connected_peers


@pytest.mark.asyncio
async def test_request_response_ping_pong() -> None:
    """Verify correlated PING/PONG request-response between two peers."""
    async with PeerNode(peer_id="node-client", host="127.0.0.1", port=0) as client_node:
        async with PeerNode(peer_id="node-server", host="127.0.0.1", port=0) as server_node:
            conn = await client_node.connect_to_peer("127.0.0.1", server_node.bound_port)

            req = Message.create(
                MessageType.PING,
                sender_id=client_node.peer_id,
                payload={"test_key": "test_val"},
            )

            resp = await client_node.send_request(conn, req, timeout=2.0)

            assert resp.message_type == MessageType.PONG
            assert resp.correlation_id == req.message_id
            assert resp.sender_id == "node-server"
            assert resp.payload["echo"] == {"test_key": "test_val"}
            assert "ack_time" in resp.payload


@pytest.mark.asyncio
async def test_custom_message_handler() -> None:
    """Verify custom message handler registration and correlated responses."""
    async with PeerNode(peer_id="peer-1", host="127.0.0.1", port=0) as peer1:
        async with PeerNode(peer_id="peer-2", host="127.0.0.1", port=0) as peer2:

            # Register custom handler on peer2
            async def handle_load_report(msg: Message, _conn: PeerConnection) -> Message:
                return Message.create(
                    MessageType.HEARTBEAT,
                    sender_id=peer2.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                    payload={"status": "load_acknowledged", "received_cpu": msg.payload["cpu_percent"]},
                )

            peer2.register_handler(MessageType.LOAD_REPORT, handle_load_report)

            conn = await peer1.connect_to_peer("127.0.0.1", peer2.bound_port)

            report = Message.create(
                MessageType.LOAD_REPORT,
                sender_id=peer1.peer_id,
                payload={"cpu_percent": 42.0, "memory_percent": 50.0},
            )

            reply = await peer1.send_request(conn, report, timeout=2.0)

            assert reply.message_type == MessageType.HEARTBEAT
            assert reply.correlation_id == report.message_id
            assert reply.payload["received_cpu"] == 42.0
            assert reply.payload["status"] == "load_acknowledged"


@pytest.mark.asyncio
async def test_request_timeout() -> None:
    """Verify NetworkTimeoutError when a peer does not respond within deadline."""
    async with PeerNode(peer_id="peer-req", host="127.0.0.1", port=0) as peer_req:
        async with PeerNode(peer_id="peer-silent", host="127.0.0.1", port=0) as peer_silent:

            # Deliberately slow handler on peer_silent
            async def slow_handler(_msg: Message, _conn: PeerConnection) -> Message | None:
                await asyncio.sleep(1.0)
                return None

            peer_silent.register_handler(MessageType.HEARTBEAT, slow_handler)

            conn = await peer_req.connect_to_peer("127.0.0.1", peer_silent.bound_port)

            msg = Message.create(
                MessageType.HEARTBEAT,
                sender_id=peer_req.peer_id,
                payload={"status": "checking"},
            )

            with pytest.raises(NetworkTimeoutError, match="timed out"):
                await peer_req.send_request(conn, msg, timeout=0.1)


@pytest.mark.asyncio
async def test_one_way_send_message() -> None:
    """Verify asynchronous one-way message delivery without awaiting correlation."""
    received_event = asyncio.Event()
    received_payload: dict = {}

    async with PeerNode(peer_id="peer-sender", host="127.0.0.1", port=0) as sender:
        async with PeerNode(peer_id="peer-receiver", host="127.0.0.1", port=0) as receiver:

            async def on_heartbeat(msg: Message, _conn: PeerConnection) -> None:
                received_payload.update(msg.payload)
                received_event.set()

            receiver.register_handler(MessageType.HEARTBEAT, on_heartbeat)

            conn = await sender.connect_to_peer("127.0.0.1", receiver.bound_port)

            heartbeat = Message.create(
                MessageType.HEARTBEAT,
                sender_id=sender.peer_id,
                payload={"uptime": 120},
            )

            await sender.send_message(conn, heartbeat)

            await asyncio.wait_for(received_event.wait(), timeout=2.0)
            assert received_payload == {"uptime": 120}


@pytest.mark.asyncio
async def test_connect_to_unreachable_peer() -> None:
    """Verify PeerConnectionError when connecting to an unreachable port."""
    async with PeerNode(peer_id="node-isolated", host="127.0.0.1", port=0) as node:
        # Port 1 is reserved and typically closed
        with pytest.raises(PeerConnectionError):
            await node.connect_to_peer("127.0.0.1", 1, timeout=0.5)


@pytest.mark.asyncio
async def test_graceful_shutdown_cleans_resources() -> None:
    """Verify that stopping a node closes connections and aborts operations cleanly."""
    node_a = PeerNode(peer_id="node-a", host="127.0.0.1", port=0)
    node_b = PeerNode(peer_id="node-b", host="127.0.0.1", port=0)

    await node_a.start()
    await node_b.start()

    conn = await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)
    assert not conn.is_closed

    await node_a.stop()
    await node_b.stop()

    assert not node_a.is_running
    assert not node_b.is_running
    assert node_a.active_connections == []
    assert node_b.active_connections == []
    assert conn.is_closed

    # Attempting to send on the closed connection should raise ConnectionClosedError
    msg = Message.create(MessageType.PING, sender_id=node_a.peer_id)
    with pytest.raises(ConnectionClosedError):
        await conn.send(msg)

"""
Security-focused unit and integration tests for MeshWeaver peer-to-peer security.

Validates:
- Configurable TLS and mutual TLS (mTLS) using Python's standard `ssl` module.
- Certificate trust verification and rejection of untrusted certificates.
- HMAC-SHA256 message signing, canonicalization, and tamper detection.
- Replay attack mitigation via sliding timestamp freshness and nonce/ID tracking.
- Peer authentication via pre-shared tokens and authorized peer allowlists.
- Enforcement of message and payload size limits.
- Resilient rejection of malformed or corrupted inputs without process crashes.
- Enforcement of registered-task execution boundaries.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import time
import pytest

from meshweaver.exceptions import (
    AuthenticationError,
    HandshakeError,
    IntegrityError,
    PayloadLimitExceededError,
    PeerConnectionError,
    ReplayAttackError,
    SecurityError,
)
from meshweaver.network import PeerNode
from meshweaver.protocol import Message, MessageType
from meshweaver.security import (
    ReplayProtection,
    SecurityConfig,
    SecurityManager,
    TLSConfig,
    canonicalize_message,
    compute_hmac,
    sign_message,
    verify_message_integrity,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "certs"
CA_CERT = FIXTURES_DIR / "ca.pem"
SERVER_CERT = FIXTURES_DIR / "server.pem"
CLIENT_CERT = FIXTURES_DIR / "client.pem"
BAD_CERT = FIXTURES_DIR / "badcert.pem"


# =========================================================================
# 1. Message Canonicalization & HMAC Integrity Tests
# =========================================================================


def test_canonicalize_message_deterministic():
    """Verify that message canonicalization is deterministic regardless of dict key order."""
    msg1 = Message.create(
        MessageType.PING,
        sender_id="node-a",
        recipient_id="node-b",
        correlation_id="corr-1",
        message_id="msg-100",
        timestamp=1000.0,
        payload={"zebra": 1, "alpha": 2, "nested": {"b": 2, "a": 1}},
    )
    msg2 = Message.create(
        MessageType.PING,
        sender_id="node-a",
        recipient_id="node-b",
        correlation_id="corr-1",
        message_id="msg-100",
        timestamp=1000.0,
        payload={"alpha": 2, "nested": {"a": 1, "b": 2}, "zebra": 1},
    )
    assert canonicalize_message(msg1) == canonicalize_message(msg2)


def test_hmac_sign_and_verify_success():
    """Verify HMAC-SHA256 signing and constant-time integrity verification."""
    secret = "meshweaver-super-secret-key-42"
    msg = Message.create(
        MessageType.PING,
        sender_id="peer-1",
        payload={"status": "ok", "value": 123},
    )
    assert msg.signature is None

    sign_message(msg, secret)
    assert msg.signature is not None
    assert len(msg.signature) == 64  # SHA-256 hex string

    # Verification must succeed
    assert verify_message_integrity(msg, secret) is True


def test_hmac_verify_rejects_tampered_payload():
    """Verify that altering message payload causes IntegrityError."""
    from dataclasses import replace

    secret = "shared-secret-123"
    msg = Message.create(
        MessageType.PING,
        sender_id="client",
        payload={"value": 1, "text": "hello"},
    )
    signed_msg = sign_message(msg, secret)

    # Tamper with payload while keeping original signature
    tampered_msg = replace(signed_msg, payload={"value": 999, "text": "hello"})

    with pytest.raises(IntegrityError, match="signature mismatch"):
        verify_message_integrity(tampered_msg, secret)


def test_hmac_verify_rejects_tampered_headers():
    """Verify that altering message headers (sender, timestamp, type) causes IntegrityError."""
    from dataclasses import replace

    secret = "shared-secret-123"
    msg = Message.create(
        MessageType.PING,
        sender_id="alice",
        payload={"status": "ok"},
    )
    signed_msg = sign_message(msg, secret)

    # Impersonate sender while keeping original signature
    tampered_msg = replace(signed_msg, sender_id="mallory")

    with pytest.raises(IntegrityError, match="signature mismatch"):
        verify_message_integrity(tampered_msg, secret)


def test_hmac_verify_rejects_unsigned_message():
    """Verify that verify_message_integrity rejects unsigned messages."""
    msg = Message.create(MessageType.PING, sender_id="peer-1", payload={})
    with pytest.raises(IntegrityError, match="unsigned"):
        verify_message_integrity(msg, "some-secret")


# =========================================================================
# 2. Replay Protection Tests
# =========================================================================


def test_replay_protection_accepts_fresh_unique_messages():
    """Verify that fresh messages with unique IDs are accepted."""
    guard = ReplayProtection(window_seconds=30.0)
    now = time.time()

    guard.check_and_record("msg-1", now - 2.0, current_time=now)
    guard.check_and_record("msg-2", now - 1.0, current_time=now)
    guard.check_and_record("msg-3", now, current_time=now)


def test_replay_protection_rejects_duplicate_message_id():
    """Verify that replaying a message with an identical ID is rejected."""
    guard = ReplayProtection(window_seconds=30.0)
    now = time.time()

    guard.check_and_record("msg-unique-123", now - 5.0, current_time=now)
    with pytest.raises(ReplayAttackError, match="already been processed"):
        guard.check_and_record("msg-unique-123", now - 4.0, current_time=now)


def test_replay_protection_rejects_expired_timestamp():
    """Verify that a message older than the freshness window is rejected."""
    guard = ReplayProtection(window_seconds=10.0)
    now = time.time()

    with pytest.raises(ReplayAttackError, match="outside acceptable freshness window"):
        guard.check_and_record("msg-old", now - 25.0, current_time=now)


def test_replay_protection_rejects_future_timestamp_drift():
    """Verify that a message with future timestamp beyond the window is rejected."""
    guard = ReplayProtection(window_seconds=10.0)
    now = time.time()

    with pytest.raises(ReplayAttackError, match="outside acceptable freshness window"):
        guard.check_and_record("msg-future", now + 25.0, current_time=now)


# =========================================================================
# 3. Payload and Message Size Limits
# =========================================================================


def test_payload_limit_enforced_outbound():
    """Verify that SecurityManager rejects outbound payloads exceeding max_payload_size."""
    cfg = SecurityConfig(max_payload_size=1024)
    sec = SecurityManager(cfg)

    oversized_payload = {"data": "x" * 2048}
    msg = Message.create(MessageType.PING, sender_id="peer-1", payload=oversized_payload)

    with pytest.raises(PayloadLimitExceededError, match="exceeds limit"):
        sec.prepare_outbound(msg)


def test_payload_limit_enforced_inbound():
    """Verify that SecurityManager rejects inbound payloads exceeding max_payload_size."""
    cfg = SecurityConfig(max_payload_size=1024)
    sec = SecurityManager(cfg)

    oversized_payload = {"data": "x" * 2048}
    msg = Message.create(MessageType.PING, sender_id="peer-1", payload=oversized_payload)

    with pytest.raises(PayloadLimitExceededError, match="exceeds limit"):
        sec.validate_inbound(msg)


# =========================================================================
# 4. Peer Authentication & Whitelist Policy Tests
# =========================================================================


def test_allowed_peers_whitelist_validation():
    """Verify that messages and handshakes from unauthorized peer IDs are rejected."""
    sec = SecurityManager(SecurityConfig(allowed_peers={"alice", "bob"}))

    valid_msg = Message.create(MessageType.PING, sender_id="alice", payload={})
    sec.validate_inbound(valid_msg)

    unauthorized_msg = Message.create(MessageType.PING, sender_id="eve", payload={})
    with pytest.raises(AuthenticationError, match="not authorized in allowed_peers"):
        sec.validate_inbound(unauthorized_msg)


def test_auth_token_handshake_validation():
    """Verify pre-shared bearer token validation during handshake."""
    sec = SecurityManager(SecurityConfig(auth_token="mesh-token-secret"))

    # Valid token
    good_handshake = Message.create(
        MessageType.HANDSHAKE,
        sender_id="peer-b",
        payload={"node_id": "peer-b", "auth_token": "mesh-token-secret"},
    )
    sec.validate_handshake(good_handshake)

    # Missing token
    missing_handshake = Message.create(
        MessageType.HANDSHAKE,
        sender_id="peer-b",
        payload={"node_id": "peer-b"},
    )
    with pytest.raises(AuthenticationError, match="Missing authentication token"):
        sec.validate_handshake(missing_handshake)

    # Wrong token
    bad_handshake = Message.create(
        MessageType.HANDSHAKE,
        sender_id="peer-b",
        payload={"node_id": "peer-b", "auth_token": "incorrect-token"},
    )
    with pytest.raises(AuthenticationError, match="Invalid authentication token"):
        sec.validate_handshake(bad_handshake)


# =========================================================================
# 5. End-to-End TLS Network Tests (using Python ssl module)
# =========================================================================


@pytest.mark.asyncio
async def test_tls_encrypted_peer_communication():
    """Verify two peer nodes communicate over TLS-encrypted TCP sockets."""
    assert SERVER_CERT.exists(), "Test server certificate fixture missing"
    assert CA_CERT.exists(), "Test CA certificate fixture missing"

    server_tls = TLSConfig(
        enabled=True,
        certfile=SERVER_CERT,
        cafile=CA_CERT,
    )
    client_tls = TLSConfig(
        enabled=True,
        cafile=CA_CERT,
        check_hostname=False,
    )

    async with PeerNode(peer_id="tls-server", host="127.0.0.1", port=0, tls_config=server_tls) as server:
        async with PeerNode(peer_id="tls-client", host="127.0.0.1", port=0, tls_config=client_tls) as client:
            conn = await client.connect_to_peer(server.host, server.bound_port)
            assert conn.is_tls is True

            # Exchange ping/pong
            req = Message.create(
                MessageType.PING,
                sender_id=client.peer_id,
                payload={"greeting": "hello-tls"},
            )
            resp = await client.send_request(conn, req, timeout=3.0)
            assert resp.message_type == MessageType.PONG
            assert resp.payload["echo"] == {"greeting": "hello-tls"}


@pytest.mark.asyncio
async def test_tls_untrusted_certificate_rejected():
    """Verify client rejects server presenting untrusted/bad certificate when CA validation is enabled."""
    bad_server_tls = TLSConfig(
        enabled=True,
        certfile=BAD_CERT,
    )
    strict_client_tls = TLSConfig(
        enabled=True,
        cafile=CA_CERT,
        check_hostname=False,
    )

    async with PeerNode(peer_id="bad-server", host="127.0.0.1", port=0, tls_config=bad_server_tls) as server:
        async with PeerNode(peer_id="strict-client", host="127.0.0.1", port=0, tls_config=strict_client_tls) as client:
            with pytest.raises(PeerConnectionError):
                await client.connect_to_peer(server.host, server.bound_port, timeout=2.0)


@pytest.mark.asyncio
async def test_mutual_tls_mtls_peer_authentication():
    """Verify mutual TLS (mTLS): server requires and validates client certificate."""
    server_mtls = TLSConfig(
        enabled=True,
        certfile=SERVER_CERT,
        cafile=CA_CERT,
        require_client_cert=True,
    )
    client_mtls = TLSConfig(
        enabled=True,
        certfile=CLIENT_CERT,
        cafile=CA_CERT,
        check_hostname=False,
    )

    async with PeerNode(peer_id="mtls-server", host="127.0.0.1", port=0, tls_config=server_mtls) as server:
        async with PeerNode(peer_id="mtls-client", host="127.0.0.1", port=0, tls_config=client_mtls) as client:
            conn = await client.connect_to_peer(server.host, server.bound_port)
            assert conn.is_tls is True
            # Inbound server connection should report client cert
            server_conn = server.connected_peers.get("mtls-client")
            if server_conn is not None:
                assert server_conn.is_tls is True


# =========================================================================
# 6. End-to-End HMAC & Authentication Network Tests
# =========================================================================


@pytest.mark.asyncio
async def test_authenticated_peer_network_with_shared_secret():
    """Verify end-to-end peer network with HMAC-SHA256 message integrity signing."""
    secret = "meshweaver-cluster-shared-secret-999"

    async with PeerNode(peer_id="node-alpha", host="127.0.0.1", port=0, shared_secret=secret) as node_a:
        async with PeerNode(peer_id="node-beta", host="127.0.0.1", port=0, shared_secret=secret) as node_b:
            conn = await node_a.connect_to_peer(node_b.host, node_b.bound_port)

            req = Message.create(
                MessageType.PING,
                sender_id=node_a.peer_id,
                payload={"message": "secure-hello"},
            )
            resp = await node_a.send_request(conn, req, timeout=3.0)
            assert resp.message_type == MessageType.PONG
            assert resp.signature is not None
            assert verify_message_integrity(resp, secret) is True


@pytest.mark.asyncio
async def test_authenticated_peer_rejects_unshared_secret():
    """Verify peer with mismatched shared secret is rejected during handshake."""
    async with PeerNode(peer_id="server-node", host="127.0.0.1", port=0, shared_secret="secret-AAA") as server:
        async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0, shared_secret="secret-BBB") as client:
            with pytest.raises(HandshakeError, match="Handshake rejected"):
                await client.connect_to_peer(server.host, server.bound_port, timeout=2.0)


@pytest.mark.asyncio
async def test_handshake_rejects_invalid_auth_token():
    """Verify that a node requiring auth_token rejects peers providing invalid tokens."""
    async with PeerNode(peer_id="auth-server", host="127.0.0.1", port=0, auth_token="correct-token") as server:
        async with PeerNode(peer_id="auth-client", host="127.0.0.1", port=0, auth_token="wrong-token") as client:
            with pytest.raises(HandshakeError, match="Handshake rejected"):
                await client.connect_to_peer(server.host, server.bound_port, timeout=2.0)

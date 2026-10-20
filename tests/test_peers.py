"""
Deterministic unit and integration tests for PeerEndpoint, PeerRecord, PeerState,
and dynamic PeerRegistry membership and expiration.
"""

from __future__ import annotations

import pytest

from meshweaver import (
    DEFAULT_STALE_THRESHOLD,
    DEFAULT_TTL,
    InvalidPeerError,
    PeerEndpoint,
    PeerNode,
    PeerNotFoundError,
    PeerRecord,
    PeerRegistry,
    PeerRegistryError,
    PeerState,
    validate_host,
    validate_peer_id,
    validate_port,
)


# ============================================================================
# Validation Helper Tests
# ============================================================================


def test_validate_peer_id_valid() -> None:
    """Verify valid peer IDs are accepted."""
    assert validate_peer_id("node-alpha") == "node-alpha"
    assert validate_peer_id("node_123") == "node_123"
    assert validate_peer_id("0123456789abcdef") == "0123456789abcdef"


@pytest.mark.parametrize(
    "invalid_id",
    [
        "",
        "   ",
        1234,
        None,
        "node with spaces",
        "node:colon",
        "node/slash",
        "x" * 129,
    ],
)
def test_validate_peer_id_invalid(invalid_id: object) -> None:
    """Verify invalid peer IDs raise InvalidPeerError."""
    with pytest.raises(InvalidPeerError):
        validate_peer_id(invalid_id)


@pytest.mark.parametrize(
    "valid_host",
    [
        "127.0.0.1",
        "192.168.1.100",
        "10.0.0.1",
        "::1",
        "fe80::1",
        "localhost",
        "node1.mesh.local",
        "worker-alpha",
    ],
)
def test_validate_host_valid(valid_host: str) -> None:
    """Verify valid IPv4, IPv6, and hostnames are accepted."""
    assert validate_host(valid_host) == valid_host


@pytest.mark.parametrize(
    "invalid_host",
    [
        "",
        "   ",
        12345,
        None,
        "invalid host with spaces",
        "http://localhost",
        "256.256.256.256",
        "x" * 254,
    ],
)
def test_validate_host_invalid(invalid_host: object) -> None:
    """Verify malformed hosts raise InvalidPeerError."""
    with pytest.raises(InvalidPeerError):
        validate_host(invalid_host)


@pytest.mark.parametrize("valid_port", [1, 80, 443, 8000, 65535])
def test_validate_port_valid(valid_port: int) -> None:
    """Verify valid port numbers are accepted."""
    assert validate_port(valid_port) == valid_port


@pytest.mark.parametrize("invalid_port", [0, -1, 65536, 70000, True, False, "8000", None])
def test_validate_port_invalid(invalid_port: object) -> None:
    """Verify invalid or non-integer port numbers raise InvalidPeerError."""
    with pytest.raises(InvalidPeerError):
        validate_port(invalid_port)


# ============================================================================
# PeerEndpoint Tests
# ============================================================================


def test_peer_endpoint_creation_and_methods() -> None:
    """Verify PeerEndpoint creation, conversions, and string representations."""
    ep = PeerEndpoint("127.0.0.1", 8080)
    assert ep.host == "127.0.0.1"
    assert ep.port == 8080
    assert ep.to_tuple() == ("127.0.0.1", 8080)
    assert ep.to_dict() == {"host": "127.0.0.1", "port": 8080}
    assert str(ep) == "127.0.0.1:8080"

    # From dict
    reconstructed = PeerEndpoint.from_dict({"host": "127.0.0.1", "port": 8080})
    assert reconstructed == ep

    # From address string (IPv4)
    from_str = PeerEndpoint.from_address("192.168.1.1:9000")
    assert from_str == PeerEndpoint("192.168.1.1", 9000)

    # From address string (IPv6)
    from_ipv6 = PeerEndpoint.from_address("[::1]:9000")
    assert from_ipv6.host == "::1"
    assert from_ipv6.port == 9000
    assert str(from_ipv6) == "[::1]:9000"

    # From tuple
    from_tup = PeerEndpoint.from_address(("localhost", 5000))
    assert from_tup == PeerEndpoint("localhost", 5000)


def test_peer_endpoint_invalid_inputs() -> None:
    """Verify PeerEndpoint construction fails on bad inputs."""
    with pytest.raises(InvalidPeerError):
        PeerEndpoint.from_dict({"host": "127.0.0.1"})  # missing port

    with pytest.raises(InvalidPeerError):
        PeerEndpoint.from_address("no-colon-port")

    with pytest.raises(InvalidPeerError):
        PeerEndpoint.from_address(12345)  # type: ignore[arg-type]


# ============================================================================
# PeerRecord & Deterministic State Tests
# ============================================================================


def test_peer_record_creation_and_properties() -> None:
    """Verify PeerRecord fields, node_id alias, and serialization."""
    ep = PeerEndpoint("127.0.0.1", 9000)
    rec = PeerRecord(
        peer_id="node-1",
        endpoint=ep,
        last_seen=100.0,
        registered_at=100.0,
        metadata={"version": "0.1.0", "region": "local"},
    )

    assert rec.peer_id == "node-1"
    assert rec.node_id == "node-1"
    assert rec.endpoint == ep
    assert rec.host == "127.0.0.1"
    assert rec.port == 9000
    assert rec.last_seen == 100.0
    assert rec.registered_at == 100.0
    assert rec.metadata == {"version": "0.1.0", "region": "local"}

    # Dict roundtrip
    d = rec.to_dict()
    assert d["peer_id"] == "node-1"
    assert d["host"] == "127.0.0.1"
    assert d["port"] == 9000

    reconstructed = PeerRecord.from_dict(d)
    assert reconstructed == rec


def test_peer_record_deterministic_state_transitions() -> None:
    """
    Verify deterministic state transitions (ACTIVE -> STALE -> EXPIRED)
    based on elapsed time without sleeping.
    """
    rec = PeerRecord(
        peer_id="node-1",
        endpoint=PeerEndpoint("127.0.0.1", 8000),
        last_seen=1000.0,
        registered_at=1000.0,
    )
    stale_thresh = 60.0
    ttl = 300.0

    # 1. Active: elapsed = 30s <= 60s
    t_active = 1030.0
    assert rec.state(t_active, stale_thresh, ttl) == PeerState.ACTIVE
    assert rec.is_active(t_active, stale_thresh) is True
    assert rec.is_stale(t_active, stale_thresh, ttl) is False
    assert rec.is_expired(t_active, ttl) is False

    # 2. Exactly at threshold: elapsed = 60s <= 60s -> Active
    assert rec.state(1060.0, stale_thresh, ttl) == PeerState.ACTIVE

    # 3. Stale: elapsed = 61s (> 60s and <= 300s)
    t_stale = 1061.0
    assert rec.state(t_stale, stale_thresh, ttl) == PeerState.STALE
    assert rec.is_active(t_stale, stale_thresh) is False
    assert rec.is_stale(t_stale, stale_thresh, ttl) is True
    assert rec.is_expired(t_stale, ttl) is False

    # 4. Exactly at TTL: elapsed = 300s <= 300s -> Stale
    assert rec.state(1300.0, stale_thresh, ttl) == PeerState.STALE

    # 5. Expired: elapsed = 301s (> 300s)
    t_expired = 1301.0
    assert rec.state(t_expired, stale_thresh, ttl) == PeerState.EXPIRED
    assert rec.is_active(t_expired, stale_thresh) is False
    assert rec.is_stale(t_expired, stale_thresh, ttl) is False
    assert rec.is_expired(t_expired, ttl) is True


def test_peer_record_touch() -> None:
    """Verify touch updates last_seen and preserves or updates endpoint and metadata."""
    rec = PeerRecord(
        peer_id="node-1",
        endpoint=PeerEndpoint("127.0.0.1", 8000),
        last_seen=100.0,
        registered_at=100.0,
        metadata={"a": 1},
    )

    touched = rec.touch(current_time=200.0, endpoint=("127.0.0.1", 8001), metadata={"b": 2})
    assert touched.peer_id == "node-1"
    assert touched.last_seen == 200.0
    assert touched.registered_at == 100.0
    assert touched.port == 8001
    assert touched.metadata == {"b": 2}


# ============================================================================
# PeerRegistry Membership & Deduplication Tests
# ============================================================================


def test_peer_registry_register_and_get() -> None:
    """Verify registering peers, querying by ID, and querying by endpoint."""
    registry = PeerRegistry()

    record = registry.register("node-a", "127.0.0.1", 8001, metadata={"role": "worker"})

    assert record.peer_id == "node-a"
    assert record.host == "127.0.0.1"
    assert record.port == 8001

    assert registry.has_peer("node-a")
    assert "node-a" in registry
    assert registry.has_endpoint("127.0.0.1", 8001)

    assert registry.get("node-a") == record
    assert registry.get_by_endpoint("127.0.0.1", 8001) == record
    assert len(registry) == 1
    assert registry.size() == 1

    # Non-existent queries
    assert registry.get("unknown") is None
    assert registry.get_by_endpoint("127.0.0.1", 9999) is None
    assert not registry.has_peer("unknown")


def test_peer_registry_idempotent_duplicate_prevention() -> None:
    """
    Verify that re-registering an existing peer ID updates the existing record
    in-place instead of creating duplicate entries.
    """
    registry = PeerRegistry(stale_threshold=60.0, ttl=300.0)

    # Initial registration at t=100
    r1 = registry.register("node-dup", "127.0.0.1", 8000, metadata={"tag": "v1"}, current_time=100.0)
    assert len(registry) == 1
    assert r1.last_seen == 100.0

    # Re-registration with new endpoint and merged metadata at t=150
    r2 = registry.register("node-dup", "127.0.0.1", 8001, metadata={"region": "us"}, current_time=150.0)

    # Must NOT have created a duplicate record
    assert len(registry) == 1
    assert registry.size() == 1
    assert r2.peer_id == "node-dup"
    assert r2.port == 8001
    assert r2.last_seen == 150.0
    assert r2.registered_at == 100.0
    assert r2.metadata == {"tag": "v1", "region": "us"}

    # Old endpoint 8000 is no longer indexed to node-dup
    assert not registry.has_endpoint("127.0.0.1", 8000)
    assert registry.has_endpoint("127.0.0.1", 8001)


def test_peer_registry_endpoint_collision_handling() -> None:
    """
    Verify that when a new peer claims an endpoint previously occupied by another peer,
    the old peer's superseded entry is cleanly removed.
    """
    registry = PeerRegistry()

    # Node 1 at 127.0.0.1:8000
    registry.register("node-old", "127.0.0.1", 8000)
    assert registry.has_peer("node-old")
    assert registry.get_by_endpoint("127.0.0.1", 8000).peer_id == "node-old"  # type: ignore[union-attr]

    # Node 2 starts on the same socket (e.g. process restarted with new ID)
    registry.register("node-new", "127.0.0.1", 8000)

    # Old peer record is superseded
    assert not registry.has_peer("node-old")
    assert registry.has_peer("node-new")
    assert registry.get_by_endpoint("127.0.0.1", 8000).peer_id == "node-new"  # type: ignore[union-attr]
    assert len(registry) == 1


def test_peer_registry_update_and_touch() -> None:
    """Verify updating and touching peer records."""
    registry = PeerRegistry()
    registry.register("node-update", "127.0.0.1", 9000, current_time=100.0)

    # Touch refreshes timestamp
    touched = registry.touch("node-update", current_time=120.0)
    assert touched.last_seen == 120.0

    # Update metadata
    updated = registry.update("node-update", metadata={"load": 25.0}, current_time=130.0)
    assert updated.last_seen == 130.0
    assert updated.metadata["load"] == 25.0

    # Updating unknown peer raises PeerNotFoundError
    with pytest.raises(PeerNotFoundError):
        registry.update("non-existent-peer")


def test_peer_registry_remove_and_clear() -> None:
    """Verify removing peers by ID or endpoint, and clearing the registry."""
    registry = PeerRegistry()
    registry.register("peer-1", "127.0.0.1", 7001)
    registry.register("peer-2", "127.0.0.1", 7002)

    assert len(registry) == 2

    # Remove peer-1 by ID
    removed = registry.remove("peer-1")
    assert removed is not None and removed.peer_id == "peer-1"
    assert not registry.has_peer("peer-1")
    assert not registry.has_endpoint("127.0.0.1", 7001)
    assert registry.remove("peer-1") is None
    assert len(registry) == 1

    # Remove peer-2 by endpoint
    removed_ep = registry.remove_by_endpoint("127.0.0.1", 7002)
    assert removed_ep is not None and removed_ep.peer_id == "peer-2"
    assert len(registry) == 0

    # Clear
    registry.register("peer-3", "127.0.0.1", 7003)
    registry.clear()
    assert len(registry) == 0
    assert not registry.has_peer("peer-3")


# ============================================================================
# Deterministic Expiration & Stale Peer Filtering Tests
# ============================================================================


def test_peer_registry_filtering_active_stale_expired() -> None:
    """
    Verify listing peers filtered by active, stale, and expired states
    using deterministic timestamps.
    """
    registry = PeerRegistry(stale_threshold=50.0, ttl=200.0)

    # Populate peers with different last_seen times relative to base t=1000
    # Peer A: last_seen = 980 (elapsed = 20s <= 50s -> ACTIVE)
    registry.register("peer-a", "127.0.0.1", 8001, current_time=980.0)
    # Peer B: last_seen = 900 (elapsed = 100s > 50s, <= 200s -> STALE)
    registry.register("peer-b", "127.0.0.1", 8002, current_time=900.0)
    # Peer C: last_seen = 750 (elapsed = 250s > 200s -> EXPIRED)
    registry.register("peer-c", "127.0.0.1", 8003, current_time=750.0)

    now = 1000.0

    # Check peer states directly
    assert registry.get_peer_state("peer-a", current_time=now) == PeerState.ACTIVE
    assert registry.get_peer_state("peer-b", current_time=now) == PeerState.STALE
    assert registry.get_peer_state("peer-c", current_time=now) == PeerState.EXPIRED

    # Filtered lists
    active_peers = registry.list_active_peers(current_time=now)
    assert [p.peer_id for p in active_peers] == ["peer-a"]

    stale_peers = registry.list_stale_peers(current_time=now)
    assert [p.peer_id for p in stale_peers] == ["peer-b"]

    expired_peers = registry.list_expired_peers(current_time=now)
    assert [p.peer_id for p in expired_peers] == ["peer-c"]

    # list_peers omits expired by default unless include_expired=True
    default_list = registry.list_peers(current_time=now)
    assert {p.peer_id for p in default_list} == {"peer-a", "peer-b"}

    full_list = registry.list_peers(include_expired=True, current_time=now)
    assert {p.peer_id for p in full_list} == {"peer-a", "peer-b", "peer-c"}


def test_peer_registry_expiration_and_pruning() -> None:
    """
    Verify expire_stale_peers and prune remove all peers exceeding TTL deterministically.
    """
    registry = PeerRegistry(stale_threshold=30.0, ttl=100.0)

    registry.register("node-live-1", "127.0.0.1", 8001, current_time=950.0)
    registry.register("node-live-2", "127.0.0.1", 8002, current_time=910.0)
    registry.register("node-dead-1", "127.0.0.1", 8003, current_time=800.0)
    registry.register("node-dead-2", "127.0.0.1", 8004, current_time=750.0)

    now = 1000.0

    # Before expiration: 4 peers
    assert len(registry) == 4

    # Expire stale peers
    pruned = registry.expire_stale_peers(current_time=now)
    assert len(pruned) == 2
    assert {p.peer_id for p in pruned} == {"node-dead-1", "node-dead-2"}

    # After expiration: exactly the 2 live peers remain
    assert len(registry) == 2
    assert registry.has_peer("node-live-1")
    assert registry.has_peer("node-live-2")
    assert not registry.has_peer("node-dead-1")
    assert not registry.has_peer("node-dead-2")
    assert not registry.has_endpoint("127.0.0.1", 8003)
    assert not registry.has_endpoint("127.0.0.1", 8004)


def test_peer_registry_capacity_limit_and_auto_prune() -> None:
    """
    Verify max_peers capacity enforcement and auto-pruning of expired peers.
    """
    registry = PeerRegistry(max_peers=2, stale_threshold=20.0, ttl=50.0)

    # Register 2 peers at t=100
    registry.register("node-1", "127.0.0.1", 8001, current_time=100.0)
    registry.register("node-2", "127.0.0.1", 8002, current_time=100.0)
    assert len(registry) == 2

    # At t=200, both node-1 and node-2 are expired. Registering node-3 auto-prunes them
    registry.register("node-3", "127.0.0.1", 8003, current_time=200.0)
    assert registry.has_peer("node-3")
    assert not registry.has_peer("node-1")
    assert not registry.has_peer("node-2")

    # Fill capacity again at t=200
    registry.register("node-4", "127.0.0.1", 8004, current_time=200.0)
    assert len(registry) == 2

    # Trying to register node-5 at t=200 with no expired records raises PeerRegistryError
    with pytest.raises(PeerRegistryError, match="capacity limit"):
        registry.register("node-5", "127.0.0.1", 8005, current_time=200.0)


# ============================================================================
# PeerNode Network Integration Tests
# ============================================================================


@pytest.mark.asyncio
async def test_peer_node_registers_peer_on_handshake() -> None:
    """
    Integration test: verify PeerNode automatically registers remote peer
    endpoints in its PeerRegistry upon completing a handshake.
    """
    async with PeerNode(peer_id="node-alice", host="127.0.0.1", port=0) as alice:
        async with PeerNode(peer_id="node-bob", host="127.0.0.1", port=0) as bob:
            # Connect alice to bob
            conn = await alice.connect_to_peer("127.0.0.1", bob.bound_port)
            assert conn.remote_peer_id == "node-bob"

            # Check Alice's registry has Bob
            assert alice.peer_registry.has_peer("node-bob")
            bob_rec = alice.peer_registry.get("node-bob")
            assert bob_rec is not None
            assert bob_rec.host == "127.0.0.1"
            assert bob_rec.port == bob.bound_port
            assert bob_rec.is_active()

            # Check Bob's registry has Alice
            assert bob.peer_registry.has_peer("node-alice")
            alice_rec = bob.peer_registry.get("node-alice")
            assert alice_rec is not None
            assert alice_rec.host == "127.0.0.1"
            assert alice_rec.port == alice.bound_port
            assert alice_rec.is_active()

"""
Unit and integration tests for DHT-inspired peer discovery.

Covers:
- NodeID representation, validation, hashing, and conversions.
- Deterministic XOR distance metric and mathematical metric properties.
- Common prefix length (cpl) calculations.
- Lookup ordering against known peers (find_closest_peers and PeerRegistry).
- Iterative lookup convergence and multi-hop discovery.
- Timeout handling (per-query timeout and overall lookup timeout).
- Duplicate suppression (candidate deduplication, single-query guarantee, cycle protection).
- Bounded queries (max_queries, max_rounds, alpha concurrency).
- Wire protocol message validation (FIND_NODE and NODES_FOUND).
- Live network integration between PeerNodes over asynchronous TCP streams.
"""

from __future__ import annotations

import asyncio
import time
import pytest

from meshweaver import (
    DEFAULT_ALPHA,
    DEFAULT_K,
    DEFAULT_LOOKUP_TIMEOUT,
    DEFAULT_MAX_QUERIES,
    DEFAULT_MAX_ROUNDS,
    DEFAULT_QUERY_TIMEOUT,
    KEY_BITS,
    KEY_BYTES,
    MAX_KEY_INT,
    InvalidNodeIDError,
    InvalidPayloadError,
    IterativeLookup,
    KBucket,
    LookupResult,
    Message,
    MessageType,
    NodeID,
    PeerContact,
    PeerEndpoint,
    PeerNode,
    PeerRecord,
    PeerRegistry,
    RoutingTable,
    find_closest_peers,
    iterative_peer_lookup,
    xor_distance,
)


# ============================================================================
# 1. NodeID Creation, Conversion, and Validation Tests
# ============================================================================


def test_node_id_from_int_valid() -> None:
    """Verify NodeID creation from valid integers within the keyspace."""
    node0 = NodeID.from_int(0)
    assert node0.value == 0
    assert node0.bit_length == KEY_BITS
    assert int(node0) == 0

    max_node = NodeID.from_int(MAX_KEY_INT)
    assert max_node.value == MAX_KEY_INT
    assert len(max_node.raw_bytes) == KEY_BYTES
    assert max_node.hex == "f" * 40


@pytest.mark.parametrize(
    "invalid_val",
    [-1, MAX_KEY_INT + 1, True, False, None, 3.14, "not-hex"],
)
def test_node_id_from_int_invalid(invalid_val: object) -> None:
    """Verify out-of-bounds or non-integer values raise InvalidNodeIDError."""
    with pytest.raises(InvalidNodeIDError):
        NodeID.from_int(invalid_val)  # type: ignore[arg-type]


def test_node_id_from_bytes() -> None:
    """Verify NodeID creation from raw big-endian bytes."""
    raw = b"\x01" * 20
    node = NodeID.from_bytes(raw)
    assert node.raw_bytes == raw
    assert bytes(node) == raw

    # Invalid lengths
    with pytest.raises(InvalidNodeIDError):
        NodeID.from_bytes(b"\x01" * 19)
    with pytest.raises(InvalidNodeIDError):
        NodeID.from_bytes(b"\x01" * 21)


def test_node_id_from_hex() -> None:
    """Verify NodeID creation from 40-character hex string with optional 0x prefix."""
    hex_str = "0123456789abcdef0123456789abcdef01234567"
    node = NodeID.from_hex(hex_str)
    assert node.hex == hex_str

    node_prefixed = NodeID.from_hex("0x" + hex_str)
    assert node_prefixed == node

    # Invalid hex
    with pytest.raises(InvalidNodeIDError):
        NodeID.from_hex("zzzz" * 10)
    with pytest.raises(InvalidNodeIDError):
        NodeID.from_hex("1234")


def test_node_id_from_string_deterministic() -> None:
    """Verify string hashing produces deterministic, non-zero NodeIDs via SHA-1."""
    id1 = NodeID.from_string("peer-worker-alpha")
    id2 = NodeID.from_string("peer-worker-alpha")
    id3 = NodeID.from_string("peer-worker-beta")

    assert id1 == id2
    assert id1 != id3
    assert len(id1.raw_bytes) == 20
    assert 0 <= id1.value <= MAX_KEY_INT


def test_node_id_generate_randomness() -> None:
    """Verify random NodeID generation yields distinct uniform values."""
    id1 = NodeID.generate()
    id2 = NodeID.generate()
    assert id1 != id2
    assert len(id1.raw_bytes) == 20
    assert 0 <= id1.value <= MAX_KEY_INT


def test_node_id_from_peer_varieties() -> None:
    """Verify NodeID.from_peer correctly parses or derives from different peer representations."""
    # From NodeID
    base = NodeID.from_int(42)
    assert NodeID.from_peer(base) is base

    # From PeerContact
    contact = PeerContact(
        node_id=base,
        endpoint=PeerEndpoint("127.0.0.1", 8000),
        peer_id="node_42",
    )
    assert NodeID.from_peer(contact) == base

    # From PeerRecord with explicit dht_node_id
    rec_with_meta = PeerRecord(
        peer_id="node-x",
        endpoint=PeerEndpoint("127.0.0.1", 8001),
        last_seen=100.0,
        registered_at=100.0,
        metadata={"dht_node_id": base.hex},
    )
    assert NodeID.from_peer(rec_with_meta) == base

    # From PeerRecord without metadata (hashes peer_id)
    rec_plain = PeerRecord(
        peer_id="node-y",
        endpoint=PeerEndpoint("127.0.0.1", 8002),
        last_seen=100.0,
        registered_at=100.0,
    )
    assert NodeID.from_peer(rec_plain) == NodeID.from_string("node-y")

    # From hex string
    hex_str = "a" * 40
    assert NodeID.from_peer(hex_str) == NodeID.from_hex(hex_str)


def test_node_id_comparisons_and_hash() -> None:
    """Verify rich comparison operators, hashing, and set/dict membership."""
    id1 = NodeID.from_int(100)
    id2 = NodeID.from_int(200)
    id3 = NodeID.from_int(100)

    assert id1 == id3
    assert id1 != id2
    assert id1 < id2
    assert id1 <= id3
    assert id2 > id1
    assert id2 >= id3

    s = {id1, id2}
    assert id3 in s
    assert len(s) == 2

    d = {id1: "one", id2: "two"}
    assert d[id3] == "one"


# ============================================================================
# 2. XOR Distance Metric and Metric Properties
# ============================================================================


def test_xor_distance_identity() -> None:
    """Verify identity: d(x, x) == 0."""
    x = NodeID.generate()
    assert x.distance(x) == 0
    assert (x ^ x) == 0
    assert xor_distance(x, x) == 0


def test_xor_distance_symmetry() -> None:
    """Verify symmetry: d(x, y) == d(y, x)."""
    for _ in range(10):
        x = NodeID.generate()
        y = NodeID.generate()
        assert x.distance(y) == y.distance(x)
        assert xor_distance(x, y) == xor_distance(y, x)


def test_xor_distance_positivity() -> None:
    """Verify strict positivity: d(x, y) > 0 for all x != y."""
    for _ in range(10):
        x = NodeID.generate()
        y = NodeID.generate()
        if x != y:
            assert x.distance(y) > 0


def test_xor_distance_triangle_inequality() -> None:
    """Verify triangle inequality: d(x, z) <= d(x, y) ^ d(y, z)."""
    for _ in range(10):
        x = NodeID.generate()
        y = NodeID.generate()
        z = NodeID.generate()
        d_xz = x.distance(z)
        d_xy = x.distance(y)
        d_yz = y.distance(z)
        # In bitwise XOR metric, d(x, z) == d(x, y) ^ d(y, z) <= d(x, y) + d(y, z)
        assert d_xz == (d_xy ^ d_yz)
        assert d_xz <= (d_xy + d_yz)


def test_xor_distance_unidirectionality() -> None:
    """Verify unidirectionality: for given x and distance D, there is exactly one y."""
    x = NodeID.from_int(12345)
    delta = 54321
    y_val = x.value ^ delta
    y = NodeID.from_int(y_val)
    assert x.distance(y) == delta


def test_common_prefix_length() -> None:
    """Verify common prefix length calculation for k-bucket indexing."""
    x = NodeID.from_int(0)
    assert x.common_prefix_length(x) == KEY_BITS

    # Differ only in the lowest bit
    y = NodeID.from_int(1)
    assert x.common_prefix_length(y) == KEY_BITS - 1

    # Differ in the highest bit
    z = NodeID.from_int(1 << (KEY_BITS - 1))
    assert x.common_prefix_length(z) == 0


# ============================================================================
# 3. Lookup Ordering Against Known Peers
# ============================================================================


def test_find_closest_peers_strict_ordering() -> None:
    """Verify that find_closest_peers returns peers sorted strictly by XOR distance."""
    target = NodeID.from_int(100)

    # Create peers with known distances to target
    # target ^ id
    # 100 ^ 100 = 0 (exact match)
    # 100 ^ 101 = 1
    # 100 ^ 102 = 6
    # 100 ^ 96  = 4
    # 100 ^ 0   = 100
    peer_ids = [0, 102, 100, 96, 101]
    contacts = [
        PeerContact(
            node_id=NodeID.from_int(val),
            endpoint=PeerEndpoint("127.0.0.1", 8000 + val),
            peer_id=f"peer_{val}",
        )
        for val in peer_ids
    ]

    closest = find_closest_peers(contacts, target, count=5)
    assert len(closest) == 5

    # Expected order:
    # 1. 100 (dist 0: 100 ^ 100 = 0)
    # 2. 101 (dist 1: 100 ^ 101 = 1)
    # 3. 102 (dist 2: 100 ^ 102 = 2)
    # 4. 96  (dist 4: 100 ^ 96  = 4)
    # 5. 0   (dist 100: 100 ^ 0 = 100)
    expected_order = [100, 101, 102, 96, 0]
    actual_order = [c.node_id.value for c in closest]
    assert actual_order == expected_order

    # Verify distances are monotonically non-decreasing
    distances = [c.node_id.distance(target) for c in closest]
    assert distances == sorted(distances)


def test_find_closest_peers_count_and_exclude() -> None:
    """Verify count bounding and exclusion filters."""
    target = NodeID.from_int(50)
    contacts = [
        PeerContact(
            node_id=NodeID.from_int(i),
            endpoint=PeerEndpoint("127.0.0.1", 9000 + i),
            peer_id=f"peer_{i}",
        )
        for i in range(20)
    ]

    # Limit to count=3
    top3 = find_closest_peers(contacts, target, count=3)
    assert len(top3) == 3

    # Exclude peer_id or NodeID
    exclude_id = top3[0].node_id
    filtered = find_closest_peers(contacts, target, count=3, exclude=[exclude_id])
    assert len(filtered) == 3
    assert exclude_id not in [c.node_id for c in filtered]


def test_peer_registry_find_closest_peers() -> None:
    """Verify PeerRegistry.find_closest_peers orders and filters registered records."""
    registry = PeerRegistry()
    registry.register("node-a", "127.0.0.1", 8001)
    registry.register("node-b", "127.0.0.1", 8002)
    registry.register("node-c", "127.0.0.1", 8003)

    target = "node-a"  # target matching node-a
    closest = registry.find_closest_peers(target, count=10)
    assert len(closest) == 3
    assert closest[0].peer_id == "node-a"

    # With exclude
    without_a = registry.find_closest_peers(target, count=10, exclude=["node-a"])
    assert len(without_a) == 2
    assert "node-a" not in [p.peer_id for p in without_a]


# ============================================================================
# 4. Iterative Peer Lookup Tests (Multi-hop Convergence)
# ============================================================================


@pytest.mark.asyncio
async def test_iterative_lookup_multi_hop_discovery() -> None:
    """
    Test multi-hop iterative discovery:
    - Initiator knows Peer A.
    - Peer A knows Peer B.
    - Peer B knows Target Peer C.
    - Initiator finds Target Peer C through iterative rounds.
    """
    node_init = NodeID.from_int(1)
    node_a = NodeID.from_int(10)
    node_b = NodeID.from_int(20)
    node_target = NodeID.from_int(25)

    contact_a = PeerContact(node_a, PeerEndpoint("127.0.0.1", 8001), "peer_a")
    contact_b = PeerContact(node_b, PeerEndpoint("127.0.0.1", 8002), "peer_b")
    contact_target = PeerContact(node_target, PeerEndpoint("127.0.0.1", 8003), "peer_target")

    # Routing knowledge of each peer
    network_graph: dict[str, list[PeerContact]] = {
        "peer_a": [contact_a, contact_b],
        "peer_b": [contact_b, contact_target],
        "peer_target": [contact_target],
    }

    async def mock_query(contact: PeerContact, _target: NodeID) -> list[PeerContact]:
        await asyncio.sleep(0.01)
        return network_graph.get(contact.peer_id, [])

    result = await iterative_peer_lookup(
        target=node_target,
        initial_peers=[contact_a],
        query_fn=mock_query,
        alpha=2,
        k=5,
        local_node_id=node_init,
    )

    assert isinstance(result, LookupResult)
    assert result.converged is True
    assert result.timed_out is False
    assert contact_target in result
    assert result.closest_peers[0].node_id == node_target
    assert result.queries_sent >= 2  # Queried at least A and B


@pytest.mark.asyncio
async def test_iterative_lookup_empty_initial_peers() -> None:
    """Verify lookup with empty initial peers returns empty result cleanly."""
    async def mock_query(_c: PeerContact, _t: NodeID) -> list[PeerContact]:
        return []

    result = await iterative_peer_lookup(
        target=NodeID.from_int(999),
        initial_peers=[],
        query_fn=mock_query,
    )
    assert len(result) == 0
    assert result.queries_sent == 0
    assert result.converged is True


# ============================================================================
# 5. Timeout Handling and Unavailable Peer Tests
# ============================================================================


@pytest.mark.asyncio
async def test_iterative_lookup_per_query_timeout_resilience() -> None:
    """
    Verify that when one peer hangs and exceeds query_timeout:
    - The timeout is caught and logged.
    - The hanging peer is marked unresponsive.
    - The lookup continues with responsive peers without failing.
    """
    node_init = NodeID.from_int(0)
    target = NodeID.from_int(100)

    slow_peer = PeerContact(NodeID.from_int(10), PeerEndpoint("127.0.0.1", 8001), "slow_peer")
    fast_peer = PeerContact(NodeID.from_int(20), PeerEndpoint("127.0.0.1", 8002), "fast_peer")
    discovered_peer = PeerContact(NodeID.from_int(95), PeerEndpoint("127.0.0.1", 8003), "discovered")

    async def mock_query(contact: PeerContact, _target: NodeID) -> list[PeerContact]:
        if contact.peer_id == "slow_peer":
            # Hang longer than query_timeout (0.15s)
            await asyncio.sleep(0.6)
            return []
        # fast_peer and discovered respond promptly without delay
        await asyncio.sleep(0.0)
        return [discovered_peer]

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[slow_peer, fast_peer],
        query_fn=mock_query,
        query_timeout=0.15,
        overall_timeout=2.0,
        local_node_id=node_init,
    )

    assert slow_peer.node_id in result.unresponsive_peers
    assert fast_peer.node_id in result.queried_peers
    assert discovered_peer in result
    assert result.timed_out is False


@pytest.mark.asyncio
async def test_iterative_lookup_overall_timeout() -> None:
    """
    Verify that if the lookup exceeds overall_timeout:
    - It terminates gracefully without unhandled exception.
    - Returns LookupResult with timed_out=True.
    - Preserves candidates collected prior to the timeout.
    """
    node_init = NodeID.from_int(0)
    target = NodeID.from_int(100)
    peer_a = PeerContact(NodeID.from_int(10), PeerEndpoint("127.0.0.1", 8001), "peer_a")

    async def hanging_query(_c: PeerContact, _t: NodeID) -> list[PeerContact]:
        await asyncio.sleep(5.0)
        return []

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[peer_a],
        query_fn=hanging_query,
        query_timeout=2.0,
        overall_timeout=0.1,
        local_node_id=node_init,
    )

    assert result.timed_out is True
    assert result.converged is False
    assert peer_a in result.closest_peers


@pytest.mark.asyncio
async def test_iterative_lookup_peer_exception_resilience() -> None:
    """
    Verify that when peers throw network/connection exceptions:
    - The exceptions are trapped gracefully.
    - Faulty peers are marked in unresponsive_peers.
    - The lookup process does not terminate with an uncaught error.
    """
    node_init = NodeID.from_int(0)
    target = NodeID.from_int(50)

    failing_peer = PeerContact(NodeID.from_int(10), PeerEndpoint("127.0.0.1", 8001), "failing_peer")
    healthy_peer = PeerContact(NodeID.from_int(20), PeerEndpoint("127.0.0.1", 8002), "healthy_peer")
    discovered = PeerContact(NodeID.from_int(48), PeerEndpoint("127.0.0.1", 8003), "discovered")

    async def mock_query(contact: PeerContact, _t: NodeID) -> list[PeerContact]:
        if contact.peer_id == "failing_peer":
            raise ConnectionRefusedError("Simulated TCP connection reset")
        return [discovered]

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[failing_peer, healthy_peer],
        query_fn=mock_query,
        local_node_id=node_init,
    )

    assert failing_peer.node_id in result.unresponsive_peers
    assert healthy_peer.node_id in result.queried_peers
    assert discovered in result.closest_peers


@pytest.mark.asyncio
async def test_iterative_lookup_all_peers_unavailable() -> None:
    """
    Verify behavior when all contacted peers are unavailable:
    - All peers marked unresponsive.
    - Terminates cleanly without hanging or error.
    """
    target = NodeID.from_int(100)
    p1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "p1")
    p2 = PeerContact(NodeID.from_int(2), PeerEndpoint("127.0.0.1", 8002), "p2")

    async def failing_query(_c: PeerContact, _t: NodeID) -> list[PeerContact]:
        raise OSError("Host unreachable")

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[p1, p2],
        query_fn=failing_query,
    )

    assert p1.node_id in result.unresponsive_peers
    assert p2.node_id in result.unresponsive_peers
    assert len(result.queried_peers) == 0
    assert result.converged is True


# ============================================================================
# 6. Duplicate Suppression Tests
# ============================================================================


@pytest.mark.asyncio
async def test_iterative_lookup_duplicate_peer_suppression() -> None:
    """
    Verify duplicate suppression:
    - Candidate pool does not contain duplicate PeerContacts with same NodeID.
    - The querying node never queries the same peer twice.
    - Peers returning already-queried peers or cycles (A -> B -> A) do not cause loop.
    """
    node_init = NodeID.from_int(1)
    target = NodeID.from_int(50)

    contact_a = PeerContact(NodeID.from_int(10), PeerEndpoint("127.0.0.1", 8001), "peer_a")
    contact_b = PeerContact(NodeID.from_int(20), PeerEndpoint("127.0.0.1", 8002), "peer_b")
    contact_c = PeerContact(NodeID.from_int(30), PeerEndpoint("127.0.0.1", 8003), "peer_c")

    query_counts: dict[str, int] = {"peer_a": 0, "peer_b": 0, "peer_c": 0}

    async def mock_query(contact: PeerContact, _target: NodeID) -> list[PeerContact]:
        query_counts[contact.peer_id] += 1
        # Cyclic response: A returns B and C, B returns A and C, C returns B and A
        return [contact_a, contact_b, contact_c]

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[contact_a, contact_b],
        query_fn=mock_query,
        alpha=3,
        k=5,
        local_node_id=node_init,
    )

    # Every peer queried at most once!
    assert query_counts["peer_a"] == 1
    assert query_counts["peer_b"] == 1
    assert query_counts["peer_c"] == 1

    # No duplicate NodeIDs in closest_peers
    node_ids = [c.node_id for c in result.closest_peers]
    assert len(node_ids) == len(set(node_ids))


@pytest.mark.asyncio
async def test_iterative_lookup_self_exclusion() -> None:
    """Verify local node is excluded from being queried or added as candidate."""
    local_id = NodeID.from_int(10)
    target = NodeID.from_int(50)

    contact_self = PeerContact(local_id, PeerEndpoint("127.0.0.1", 8000), "self_node")
    contact_other = PeerContact(NodeID.from_int(20), PeerEndpoint("127.0.0.1", 8001), "other_node")

    async def mock_query(contact: PeerContact, _t: NodeID) -> list[PeerContact]:
        # Remote peer returns self_node
        return [contact_self]

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=[contact_self, contact_other],
        query_fn=mock_query,
        local_node_id=local_id,
    )

    assert local_id not in result.queried_peers
    assert not any(c.node_id == local_id for c in result.closest_peers)


# ============================================================================
# 7. Bounded Queries and Concurrency Control
# ============================================================================


@pytest.mark.asyncio
async def test_iterative_lookup_max_queries_bound() -> None:
    """Verify max_queries bound prevents unbounded query storms."""
    target = NodeID.from_int(1000)

    # Generate 50 candidate peers
    peers = [
        PeerContact(NodeID.from_int(i), PeerEndpoint("127.0.0.1", 9000 + i), f"peer_{i}")
        for i in range(1, 51)
    ]

    async def mock_query(contact: PeerContact, _t: NodeID) -> list[PeerContact]:
        # Returns all peers to keep fueling the candidate pool
        return peers

    # Hard cap at max_queries=4
    result = await iterative_peer_lookup(
        target=target,
        initial_peers=peers[:10],
        query_fn=mock_query,
        alpha=2,
        max_queries=4,
    )

    assert result.queries_sent <= 4


@pytest.mark.asyncio
async def test_iterative_lookup_max_rounds_bound() -> None:
    """Verify max_rounds bound stops iteration after the specified rounds."""
    target = NodeID.from_int(1000)
    peers = [
        PeerContact(NodeID.from_int(i), PeerEndpoint("127.0.0.1", 9000 + i), f"peer_{i}")
        for i in range(1, 20)
    ]

    async def mock_query(_c: PeerContact, _t: NodeID) -> list[PeerContact]:
        return peers

    result = await iterative_peer_lookup(
        target=target,
        initial_peers=peers[:2],
        query_fn=mock_query,
        alpha=1,
        max_rounds=2,
    )

    assert result.rounds <= 2


# ============================================================================
# 8. Protocol Message Validation Tests
# ============================================================================


def test_find_node_message_validation() -> None:
    """Verify FIND_NODE message payload validation."""
    valid_msg = Message.create(
        MessageType.FIND_NODE,
        sender_id="node_1",
        payload={"target_id": "target_peer", "count": 20},
    )
    assert valid_msg.message_type == MessageType.FIND_NODE

    # Missing target_id
    with pytest.raises(InvalidPayloadError):
        Message.create(
            MessageType.FIND_NODE,
            sender_id="node_1",
            payload={"count": 20},
        )

    # Invalid count
    with pytest.raises(InvalidPayloadError):
        Message.create(
            MessageType.FIND_NODE,
            sender_id="node_1",
            payload={"target_id": "target_peer", "count": -5},
        )


def test_nodes_found_message_validation() -> None:
    """Verify NODES_FOUND message payload validation."""
    valid_msg = Message.create(
        MessageType.NODES_FOUND,
        sender_id="node_1",
        payload={
            "target_id": "target_peer",
            "nodes": [{"peer_id": "peer_a", "host": "127.0.0.1", "port": 8000}],
        },
    )
    assert valid_msg.message_type == MessageType.NODES_FOUND

    # Missing nodes list
    with pytest.raises(InvalidPayloadError):
        Message.create(
            MessageType.NODES_FOUND,
            sender_id="node_1",
            payload={"target_id": "target_peer"},
        )

    # Malformed nodes element
    with pytest.raises(InvalidPayloadError):
        Message.create(
            MessageType.NODES_FOUND,
            sender_id="node_1",
            payload={"nodes": ["not-a-dict"]},
        )


# ============================================================================
# 9. Live TCP Network Integration Tests
# ============================================================================


@pytest.mark.asyncio
async def test_live_dht_find_node_exchange() -> None:
    """
    Verify live FIND_NODE / NODES_FOUND request-response exchange between
    two running PeerNode servers over TCP.
    """
    async with PeerNode(peer_id="node-alpha", host="127.0.0.1", port=0) as node_a:
        async with PeerNode(peer_id="node-beta", host="127.0.0.1", port=0) as node_b:
            # Register third peer in node_b's registry
            node_b.peer_registry.register("worker-charlie", "127.0.0.1", 9999)

            # Node A connects to Node B
            conn = await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)

            # Node A asks Node B for nodes closest to 'worker-charlie'
            discovered = await node_a.find_node_remote(conn, "worker-charlie", count=5)

            assert len(discovered) >= 1
            assert any(p.peer_id == "worker-charlie" for p in discovered)


@pytest.mark.asyncio
async def test_live_iterative_find_node_across_nodes() -> None:
    """
    Verify multi-node live iterative discovery across three nodes:
    - Node A connects to Node B.
    - Node B has Node C in its registry.
    - Node A invokes iterative_find_node targeting Node C's ID.
    - Node A discovers and registers Node C locally.
    """
    async with PeerNode(peer_id="node-a", host="127.0.0.1", port=0) as node_a:
        async with PeerNode(peer_id="node-b", host="127.0.0.1", port=0) as node_b:
            async with PeerNode(peer_id="node-c", host="127.0.0.1", port=0) as node_c:
                # Node B knows Node C
                node_b.peer_registry.register("node-c", "127.0.0.1", node_c.bound_port)

                # Node A only knows Node B initially
                await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)

                # Node A executes iterative discovery targeting Node C's ID
                result = await node_a.iterative_find_node(
                    node_c.node_id,
                    alpha=2,
                    k=5,
                    query_timeout=1.0,
                    overall_timeout=3.0,
                )

                # Verify Node C was discovered and registered in Node A's registry
                assert any(c.peer_id == "node-c" for c in result.closest_peers)
                assert node_a.peer_registry.has_peer("node-c")


# ============================================================================
# 10. Routing Bucket (KBucket) and RoutingTable Management Tests
# ============================================================================


def test_kbucket_initialization() -> None:
    """Verify KBucket initialization, defaults, and parameter validation."""
    b = KBucket(k=20)
    assert b.k == 20
    assert len(b) == 0
    assert b.is_full is False
    assert b.head is None
    assert b.tail is None
    assert b.contacts == []
    assert b.replacement_cache == []

    # Invalid k
    with pytest.raises(Exception):
        KBucket(k=0)
    with pytest.raises(Exception):
        KBucket(k=-5)
    with pytest.raises(Exception):
        KBucket(k=True)  # type: ignore[arg-type]


def test_kbucket_add_and_lru_ordering() -> None:
    """Verify contacts in KBucket are ordered from least-recently seen to most-recently seen."""
    b = KBucket(k=5)
    c1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "peer_1")
    c2 = PeerContact(NodeID.from_int(2), PeerEndpoint("127.0.0.1", 8002), "peer_2")
    c3 = PeerContact(NodeID.from_int(3), PeerEndpoint("127.0.0.1", 8003), "peer_3")

    assert b.add(c1) is True
    assert b.add(c2) is True
    assert b.add(c3) is True

    assert len(b) == 3
    assert b.head == c1  # Least recently seen
    assert b.tail == c3  # Most recently seen
    assert [c.peer_id for c in b] == ["peer_1", "peer_2", "peer_3"]


def test_kbucket_update_existing_moves_to_tail() -> None:
    """Verify that re-adding an existing contact refreshes it and moves it to the tail."""
    b = KBucket(k=5)
    c1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "peer_1")
    c2 = PeerContact(NodeID.from_int(2), PeerEndpoint("127.0.0.1", 8002), "peer_2")
    c3 = PeerContact(NodeID.from_int(3), PeerEndpoint("127.0.0.1", 8003), "peer_3")

    b.add(c1)
    b.add(c2)
    b.add(c3)

    # Re-add c1 with updated metadata
    c1_updated = PeerContact(
        NodeID.from_int(1),
        PeerEndpoint("127.0.0.1", 8001),
        "peer_1",
        metadata={"v": 2},
    )
    assert b.add(c1_updated) is True

    # Length unchanged, but c1 is now most-recently seen (tail), c2 is head
    assert len(b) == 3
    assert b.head == c2
    assert b.tail == c1_updated
    assert [c.peer_id for c in b] == ["peer_2", "peer_3", "peer_1"]


def test_kbucket_capacity_and_replacement_cache() -> None:
    """Verify that adding beyond capacity k redirects candidates into replacement_cache."""
    b = KBucket(k=2)
    c1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "peer_1")
    c2 = PeerContact(NodeID.from_int(2), PeerEndpoint("127.0.0.1", 8002), "peer_2")
    c3 = PeerContact(NodeID.from_int(3), PeerEndpoint("127.0.0.1", 8003), "peer_3")
    c4 = PeerContact(NodeID.from_int(4), PeerEndpoint("127.0.0.1", 8004), "peer_4")

    assert b.add(c1) is True
    assert b.add(c2) is True
    assert b.is_full is True

    # 3rd and 4th contacts overflow into replacement cache
    assert b.add(c3) is False
    assert b.add(c4) is False

    assert len(b) == 2
    assert len(b.replacement_cache) == 2
    assert b.replacement_cache[0] == c3
    assert b.replacement_cache[1] == c4


def test_kbucket_remove_and_promote_replacement() -> None:
    """Verify that removing a contact promotes the oldest candidate from replacement_cache."""
    b = KBucket(k=2)
    c1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "peer_1")
    c2 = PeerContact(NodeID.from_int(2), PeerEndpoint("127.0.0.1", 8002), "peer_2")
    c3 = PeerContact(NodeID.from_int(3), PeerEndpoint("127.0.0.1", 8003), "peer_3")

    b.add(c1)
    b.add(c2)
    b.add(c3)  # Goes to replacement cache

    assert len(b) == 2
    assert len(b.replacement_cache) == 1

    # Remove c1: c3 should be promoted
    assert b.remove(c1.node_id) is True
    assert len(b) == 2
    assert c1 not in b
    assert c3 in b
    assert len(b.replacement_cache) == 0
    assert b.head == c2
    assert b.tail == c3


def test_kbucket_get_and_contains() -> None:
    """Verify get and __contains__ operations with NodeID, PeerContact, and string peer_id."""
    b = KBucket(k=5)
    c1 = PeerContact(NodeID.from_int(42), PeerEndpoint("127.0.0.1", 8042), "peer_42")
    b.add(c1)

    assert b.get(c1.node_id) == c1
    assert b.get("peer_42") == c1
    assert b.get("nonexistent") is None

    assert c1 in b
    assert c1.node_id in b
    assert "peer_42" in b
    assert "unknown" not in b


def test_routing_table_init() -> None:
    """Verify RoutingTable initialization with 160 buckets and local node ID."""
    local_id = NodeID.from_int(0)
    rt = RoutingTable(local_id, k=20)

    assert rt.local_node_id == local_id
    assert rt.k == 20
    assert rt.bit_length == KEY_BITS
    assert len(rt.buckets) == KEY_BITS
    assert rt.total_contacts == 0
    assert len(rt) == 0


def test_routing_table_bucket_indexing_by_cpl() -> None:
    """Verify RoutingTable partitions contacts by common prefix length (cpl)."""
    local_id = NodeID.from_int(0)
    rt = RoutingTable(local_id)

    # Differ in MSB (bit 159): cpl = 0 -> bucket 0
    id_msb = NodeID.from_int(1 << (KEY_BITS - 1))
    assert rt.bucket_index(id_msb) == 0

    # Differ in second MSB (bit 158): cpl = 1 -> bucket 1
    id_bit158 = NodeID.from_int(1 << (KEY_BITS - 2))
    assert rt.bucket_index(id_bit158) == 1

    # Differ only in LSB (bit 0): cpl = 159 -> bucket 159
    id_lsb = NodeID.from_int(1)
    assert rt.bucket_index(id_lsb) == 159

    # Local node ID itself raises InvalidNodeIDError
    with pytest.raises(InvalidNodeIDError):
        rt.bucket_index(local_id)


def test_routing_table_self_exclusion() -> None:
    """Verify RoutingTable rejects attempts to add the local node ID to its own routing table."""
    local_id = NodeID.from_int(100)
    rt = RoutingTable(local_id)

    self_contact = PeerContact(local_id, PeerEndpoint("127.0.0.1", 8000), "self_node")
    assert rt.add_contact(self_contact) is False
    assert rt.total_contacts == 0


def test_routing_table_add_get_remove() -> None:
    """Verify adding, retrieving, and removing contacts across routing buckets."""
    local_id = NodeID.from_int(0)
    rt = RoutingTable(local_id, k=5)

    c1 = PeerContact(NodeID.from_int(1), PeerEndpoint("127.0.0.1", 8001), "peer_1")
    c2 = PeerContact(NodeID.from_int(1 << (KEY_BITS - 1)), PeerEndpoint("127.0.0.1", 8002), "peer_2")

    assert rt.add_contact(c1) is True
    assert rt.add_contact(c2) is True
    assert rt.total_contacts == 2

    # Verify they landed in the correct buckets
    assert c1 in rt.get_bucket_for_node(c1.node_id)
    assert c2 in rt.get_bucket_for_node(c2.node_id)
    assert rt.bucket_index(c1.node_id) == 159
    assert rt.bucket_index(c2.node_id) == 0

    # Retrieve
    assert rt.get_contact(c1.node_id) == c1
    assert rt.get_contact("peer_2") == c2
    assert rt.get_contact("nonexistent") is None

    # Remove
    assert rt.remove_contact(c1.node_id) is True
    assert rt.total_contacts == 1
    assert rt.get_contact(c1.node_id) is None


def test_routing_table_find_closest_peers() -> None:
    """Verify find_closest_peers selects and strictly sorts closest contacts by XOR distance."""
    local_id = NodeID.from_int(0)
    rt = RoutingTable(local_id, k=10)

    target = NodeID.from_int(100)
    peer_values = [100, 101, 102, 96, 50, 1 << (KEY_BITS - 1)]
    for val in peer_values:
        contact = PeerContact(NodeID.from_int(val), PeerEndpoint("127.0.0.1", 9000 + (val % 1000)), f"p_{val}")
        rt.add_contact(contact)

    closest = rt.find_closest_peers(target, count=4)
    assert len(closest) == 4

    # Distance to target 100:
    # 100 ^ 100 = 0
    # 100 ^ 101 = 1
    # 100 ^ 102 = 2
    # 100 ^ 96  = 4
    # Expected order: 100 (dist 0), 101 (dist 1), 102 (dist 2), 96 (dist 4)
    expected_order = [100, 101, 102, 96]
    assert [c.node_id.value for c in closest] == expected_order

    # With exclusion
    without_100 = rt.find_closest_peers(target, count=3, exclude=[NodeID.from_int(100)])
    assert len(without_100) == 3
    assert without_100[0].node_id.value == 101


def test_routing_table_iteration_and_membership() -> None:
    """Verify iterable interface, len, and membership check on RoutingTable."""
    local_id = NodeID.from_int(0)
    rt = RoutingTable(local_id, k=5)

    contacts = [
        PeerContact(NodeID.from_int(i + 1), PeerEndpoint("127.0.0.1", 8000 + i), f"node_{i}")
        for i in range(5)
    ]
    for c in contacts:
        rt.add_contact(c)

    assert len(rt) == 5
    all_listed = list(rt)
    assert len(all_listed) == 5
    for c in contacts:
        assert c in rt
        assert c.node_id in rt
        assert c.peer_id in rt


@pytest.mark.asyncio
async def test_peer_node_routing_table_integration() -> None:
    """Verify PeerNode initializes and updates its internal RoutingTable on handshake."""
    async with PeerNode(peer_id="node-alpha", host="127.0.0.1", port=0) as node_a:
        async with PeerNode(peer_id="node-beta", host="127.0.0.1", port=0) as node_b:
            assert isinstance(node_a.routing_table, RoutingTable)
            assert node_a.routing_table.local_node_id == node_a.node_id

            # Node A connects to Node B
            await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)

            # Node A's routing table should now contain Node B
            assert node_b.node_id in node_a.routing_table
            contact_b = node_a.routing_table.get_contact(node_b.node_id)
            assert contact_b is not None
            assert contact_b.peer_id == "node-beta"


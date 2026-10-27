"""
Automated integration tests for multi-node networking in MeshWeaver (Week 2, Milestone 5).

Verifies multi-node loopback networking across 3 or more nodes without Internet access:
1. Bootstrap discovery through seed nodes (join_mesh, neighbor discovery via FIND_NODE).
2. DHT peer lookup and XOR distance convergence across the mesh (iterative_find_node).
3. Gossip workload exchange (CPU, memory, active task count, health status, anti-amplification).
4. Node status reporting (NodeStatusAPI, formatted table, JSON snapshot).
5. Graceful shutdown and departure handling (leave_mesh, LEAVE propagation, socket cleanup).
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import pytest

from meshweaver import (
    HealthStatus,
    Message,
    MessageType,
    NodeID,
    NodeResourceReport,
    NodeStatusSnapshot,
    PeerEndpoint,
    PeerNode,
    PeerRecord,
    PeerRegistry,
    PeerState,
    PeerStatus,
    ResourceCollector,
    xor_distance,
)

logger = logging.getLogger("tests.multi_node")


@pytest.mark.asyncio
async def test_three_nodes_startup_and_dynamic_loopback_binding() -> None:
    """Verify three MeshWeaver nodes bind separate, dynamic ports strictly on 127.0.0.1."""
    async with PeerNode(peer_id="node-1", host="127.0.0.1", port=0) as n1:
        async with PeerNode(peer_id="node-2", host="127.0.0.1", port=0) as n2:
            async with PeerNode(peer_id="node-3", host="127.0.0.1", port=0) as n3:
                # All nodes are running
                assert n1.is_running is True
                assert n2.is_running is True
                assert n3.is_running is True

                # All bound to 127.0.0.1
                assert n1.host == "127.0.0.1"
                assert n2.host == "127.0.0.1"
                assert n3.host == "127.0.0.1"

                # Dynamic ports are strictly positive and unique
                ports = {n1.bound_port, n2.bound_port, n3.bound_port}
                assert len(ports) == 3
                assert all(p > 0 for p in ports)

                # DHT NodeIDs are 160-bit integers
                assert n1.node_id.bit_length == 160
                assert n2.node_id.bit_length == 160
                assert n3.node_id.bit_length == 160


@pytest.mark.asyncio
async def test_multi_node_bootstrap_discovery() -> None:
    """
    Verify bootstrap discovery through a seed node:
    - Node A is the seed node.
    - Node B joins Node A.
    - Node C joins Node A and discovers Node B via FIND_NODE.
    - Verifies registries and routing table population across all three nodes.
    """
    async with PeerNode(peer_id="bootstrap-seed", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="worker-alpha", host="127.0.0.1", port=0) as alpha:
            async with PeerNode(peer_id="worker-beta", host="127.0.0.1", port=0) as beta:
                seed_addr = f"127.0.0.1:{seed.bound_port}"

                # 1. Alpha joins Seed
                alpha_discovered = await alpha.join_mesh([seed_addr], timeout=3.0)
                assert len(alpha_discovered) >= 1
                assert alpha.peer_registry.has_peer("bootstrap-seed")

                # Allow incoming handshake dispatch on seed
                await asyncio.sleep(0.05)
                assert seed.peer_registry.has_peer("worker-alpha")

                # 2. Beta joins Seed and discovers Alpha via neighbor query
                beta_discovered = await beta.join_mesh([seed_addr], timeout=3.0)
                assert len(beta_discovered) >= 2
                assert beta.peer_registry.has_peer("bootstrap-seed")
                assert beta.peer_registry.has_peer("worker-alpha")

                # Allow incoming handshake dispatch on seed
                await asyncio.sleep(0.05)
                assert seed.peer_registry.has_peer("worker-beta")

                # Verify seed tracks both workers
                assert len(seed.peer_registry) == 2
                assert seed.peer_registry.has_peer("worker-alpha")
                assert seed.peer_registry.has_peer("worker-beta")

                # Verify routing table contacts on Beta
                assert beta.routing_table.has_contact("bootstrap-seed")
                assert beta.routing_table.has_contact("worker-alpha")


@pytest.mark.asyncio
async def test_multi_node_iterative_peer_lookup_and_xor_convergence() -> None:
    """
    Verify iterative DHT peer lookup across multiple network peers:
    - Node Alpha only knows Seed initially.
    - Beta joins Seed.
    - Alpha initiates iterative_find_node targeting Beta's DHT NodeID.
    - Alpha queries Seed, receives Beta, connects to Beta, and converges.
    """
    async with PeerNode(peer_id="node-seed", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="node-alpha", host="127.0.0.1", port=0) as alpha:
            async with PeerNode(peer_id="node-beta", host="127.0.0.1", port=0) as beta:
                # Alpha and Beta connect to Seed
                await alpha.join_mesh([f"127.0.0.1:{seed.bound_port}"], discover_peers=False)
                await beta.join_mesh([f"127.0.0.1:{seed.bound_port}"], discover_peers=False)
                await asyncio.sleep(0.05)

                # Alpha does NOT know Beta yet
                assert not alpha.peer_registry.has_peer("node-beta")

                # Alpha initiates iterative lookup for Beta's NodeID
                lookup_res = await alpha.iterative_find_node(
                    beta.node_id,
                    alpha=2,
                    k=5,
                    query_timeout=2.0,
                    overall_timeout=4.0,
                )

                # Lookup converged and located Beta
                assert lookup_res.converged is True
                assert any(c.peer_id == "node-beta" for c in lookup_res.closest_peers)

                # Alpha automatically registered Beta in peer_registry and routing table
                assert alpha.peer_registry.has_peer("node-beta")
                assert alpha.routing_table.has_contact("node-beta")

                # Verify XOR distance calculation
                expected_dist = int(xor_distance(alpha.node_id, beta.node_id))
                assert expected_dist > 0


@pytest.mark.asyncio
async def test_multi_node_workload_gossip_exchange() -> None:
    """
    Verify asynchronous workload and resource gossip dissemination across three nodes:
    - Each node enables GossipEngine with fast test intervals.
    - Nodes exchange CPU, memory, active task count, and health status.
    - Verifies reports are received, validated, and stored in gossip stores and peer registries.
    """
    async with PeerNode(peer_id="gossip-node-1", host="127.0.0.1", port=0) as n1:
        async with PeerNode(peer_id="gossip-node-2", host="127.0.0.1", port=0) as n2:
            async with PeerNode(peer_id="gossip-node-3", host="127.0.0.1", port=0) as n3:
                # Enable gossip engines
                g1 = n1.enable_gossip(interval=0.15, fanout=2, stale_threshold=10.0, ttl=30.0)
                g2 = n2.enable_gossip(interval=0.15, fanout=2, stale_threshold=10.0, ttl=30.0)
                g3 = n3.enable_gossip(interval=0.15, fanout=2, stale_threshold=10.0, ttl=30.0)

                # Connect mesh via bootstrap: N2 joins N1, N3 joins N1
                await n2.join_mesh([f"127.0.0.1:{n1.bound_port}"])
                await n3.join_mesh([f"127.0.0.1:{n1.bound_port}"])

                # Trigger gossip round on each node
                await g1.run_round()
                await g2.run_round()
                await g3.run_round()

                # Await gossip propagation over loopback sockets
                deadline = time.perf_counter() + 3.0
                gossip_ok = False
                while time.perf_counter() < deadline:
                    rep_2_on_1 = g1.store.get("gossip-node-2")
                    rep_3_on_1 = g1.store.get("gossip-node-3")
                    if rep_2_on_1 is not None and rep_3_on_1 is not None:
                        gossip_ok = True
                        break
                    await asyncio.sleep(0.05)

                assert gossip_ok is True, "Node 1 did not receive gossip reports from Node 2 and Node 3"

                # Validate report contents
                rep_2 = g1.store.get("gossip-node-2")
                assert rep_2 is not None
                assert rep_2.node_id == "gossip-node-2"
                assert 0.0 <= rep_2.cpu_percent <= 100.0
                assert 0.0 <= rep_2.memory_percent <= 100.0
                assert rep_2.health_status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY)

                # Verify peer registry metadata updated from gossip
                rec_2 = n1.peer_registry.get("gossip-node-2")
                assert rec_2 is not None
                assert "cpu_percent" in rec_2.metadata
                assert "memory_percent" in rec_2.metadata


@pytest.mark.asyncio
async def test_multi_node_status_api_inspection_and_reporting() -> None:
    """
    Verify NodeStatusAPI and NodeStatusSnapshot reporting in a multi-node mesh:
    - Inspects node status snapshots, active connections, and DHT proximity.
    - Verifies text table formatting and clean JSON export.
    """
    async with PeerNode(peer_id="status-seed", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="status-worker-1", host="127.0.0.1", port=0) as w1:
            async with PeerNode(peer_id="status-worker-2", host="127.0.0.1", port=0) as w2:
                seed.enable_gossip(interval=0.2)
                w1.enable_gossip(interval=0.2)
                w2.enable_gossip(interval=0.2)

                await w1.join_mesh([f"127.0.0.1:{seed.bound_port}"])
                await w2.join_mesh([f"127.0.0.1:{seed.bound_port}"])
                await asyncio.sleep(0.1)

                snapshot = seed.get_status()
                assert isinstance(snapshot, NodeStatusSnapshot)
                assert snapshot.node_id == "status-seed"
                assert snapshot.is_running is True
                assert snapshot.total_registered_peers == 2
                assert snapshot.active_peers_count == 2
                assert snapshot.stale_peers_count == 0
                assert snapshot.expired_peers_count == 0
                assert snapshot.active_connections_count >= 2

                # Verify peer status details
                p1_status = snapshot.get_peer("status-worker-1")
                assert p1_status is not None
                assert p1_status.is_active is True
                assert p1_status.is_connected is True
                assert p1_status.endpoint.host == "127.0.0.1"
                assert p1_status.endpoint.port == w1.bound_port
                assert p1_status.dht_distance is not None
                assert p1_status.dht_distance > 0

                # Verify text dashboard formatting
                formatted_text = snapshot.format_text()
                assert "MeshWeaver Node & Peer Status" in formatted_text
                assert "status-seed" in formatted_text
                assert "status-worker-1" in formatted_text
                assert "status-worker-2" in formatted_text
                assert "active" in formatted_text

                # Verify JSON serialization
                json_str = snapshot.to_json()
                parsed = json.loads(json_str)
                assert parsed["node_id"] == "status-seed"
                assert len(parsed["peers"]) == 2
                assert parsed["total_registered_peers"] == 2


@pytest.mark.asyncio
async def test_multi_node_graceful_departure_and_orderly_shutdown() -> None:
    """
    Verify graceful departure of a node from the mesh:
    - Node Gamma sends LEAVE notice to all active connections.
    - Seed and Node Alpha remove Gamma from their peer registries and routing tables.
    - All nodes shut down cleanly without unhandled exceptions or dangling tasks.
    """
    async with PeerNode(peer_id="mesh-anchor", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="mesh-worker-a", host="127.0.0.1", port=0) as worker_a:
            async with PeerNode(peer_id="mesh-worker-b", host="127.0.0.1", port=0) as worker_b:
                seed.enable_gossip(interval=0.2)
                worker_a.enable_gossip(interval=0.2)
                worker_b.enable_gossip(interval=0.2)

                await worker_a.join_mesh([f"127.0.0.1:{seed.bound_port}"])
                await worker_b.join_mesh([f"127.0.0.1:{seed.bound_port}"])

                # Connect worker_a directly to worker_b
                await worker_a.connect_to_peer("127.0.0.1", worker_b.bound_port)
                await asyncio.sleep(0.08)

                # Both seed and worker_a track worker_b
                assert seed.peer_registry.has_peer("mesh-worker-b")
                assert worker_a.peer_registry.has_peer("mesh-worker-b")

                # Worker B leaves the mesh gracefully
                await worker_b.leave_mesh(graceful=True)
                assert worker_b.is_running is False

                # Brief pause for LEAVE message handling
                await asyncio.sleep(0.1)

                # Seed and Worker A have removed Worker B
                assert not seed.peer_registry.has_peer("mesh-worker-b")
                assert not worker_a.peer_registry.has_peer("mesh-worker-b")

                # Routing table on seed no longer has Worker B
                assert not seed.routing_table.has_contact("mesh-worker-b")

                # Final shutdown of remaining nodes
                await worker_a.stop()
                await seed.stop()
                assert worker_a.is_running is False
                assert seed.is_running is False


@pytest.mark.asyncio
async def test_multi_node_deterministic_offline_isolation() -> None:
    """
    Verify that multi-node operations remain fully deterministic and offline:
    - Loopback addresses only (127.0.0.1).
    - Synthetic/mock resource collectors operate with deterministic metrics.
    """
    custom_collector_1 = ResourceCollector(
        node_id="isolated-node-1",
        custom_metrics_provider=lambda: {"zone": "us-east-1", "cluster": "alpha"},
    )
    custom_collector_2 = ResourceCollector(
        node_id="isolated-node-2",
        custom_metrics_provider=lambda: {"zone": "us-west-2", "cluster": "beta"},
    )

    async with PeerNode(peer_id="isolated-node-1", host="127.0.0.1", port=0) as n1:
        async with PeerNode(peer_id="isolated-node-2", host="127.0.0.1", port=0) as n2:
            g1 = n1.enable_gossip(interval=0.1, collector=custom_collector_1)
            g2 = n2.enable_gossip(interval=0.1, collector=custom_collector_2)

            # Sample local reports
            rep1 = g1.collect_local_status()
            rep2 = g2.collect_local_status()

            assert rep1.extra["zone"] == "us-east-1"
            assert rep1.extra["cluster"] == "alpha"
            assert rep2.extra["zone"] == "us-west-2"
            assert rep2.extra["cluster"] == "beta"
            assert 0.0 <= rep1.cpu_percent <= 100.0
            assert 0.0 <= rep2.cpu_percent <= 100.0

            # Connect and exchange gossip
            await n2.join_mesh([f"127.0.0.1:{n1.bound_port}"])
            await g1.run_round()
            await g2.run_round()

            await asyncio.sleep(0.15)
            # Node 1 received deterministic status of Node 2
            n2_rep = g1.store.get("isolated-node-2")
            assert n2_rep is not None
            assert n2_rep.extra["zone"] == "us-west-2"
            assert n2_rep.extra["cluster"] == "beta"


@pytest.mark.asyncio
async def test_four_node_mesh_topology_convergence() -> None:
    """
    Verify a 4-node mesh topology (1 seed + 3 workers):
    - All 3 workers join through the seed node.
    - Each worker discovers all previously registered workers.
    - Seed node tracks all 3 workers in active state.
    """
    async with PeerNode(peer_id="seed-central", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="worker-one", host="127.0.0.1", port=0) as w1:
            async with PeerNode(peer_id="worker-two", host="127.0.0.1", port=0) as w2:
                async with PeerNode(peer_id="worker-three", host="127.0.0.1", port=0) as w3:
                    seed_addr = f"127.0.0.1:{seed.bound_port}"

                    # Workers join sequentially
                    await w1.join_mesh([seed_addr])
                    await w2.join_mesh([seed_addr])
                    await w3.join_mesh([seed_addr])
                    await asyncio.sleep(0.08)

                    # Seed tracks all 3 workers
                    seed_status = seed.get_status()
                    assert seed_status.total_registered_peers == 3
                    assert seed_status.active_peers_count == 3
                    assert seed.peer_registry.has_peer("worker-one")
                    assert seed.peer_registry.has_peer("worker-two")
                    assert seed.peer_registry.has_peer("worker-three")

                    # Worker 3 (the latest to join) discovered Worker 1 and Worker 2
                    w3_peers = set(w3.peer_registry._peers.keys())
                    assert "seed-central" in w3_peers
                    assert "worker-one" in w3_peers
                    assert "worker-two" in w3_peers

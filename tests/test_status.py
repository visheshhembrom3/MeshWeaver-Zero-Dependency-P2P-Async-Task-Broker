"""
Unit and integration tests for MeshWeaver Node Status API, mesh joining/leaving,
stale peer lifecycle, resource reporting, structured logging, and CLI status command.
"""

from __future__ import annotations

import asyncio
import json
import logging
import subprocess
import sys
import time
import pytest

from meshweaver import (
    HealthStatus,
    Message,
    MessageType,
    NodeResourceReport,
    NodeStatusAPI,
    NodeStatusInfo,
    NodeStatusSnapshot,
    PeerEndpoint,
    PeerNode,
    PeerRecord,
    PeerRegistry,
    PeerState,
    PeerStatus,
    PeerStatusSnapshot,
    format_node_status,
    format_peer_status_table,
    get_node_status,
    log_node_status,
    setup_structured_logging,
)
from meshweaver.status import NodeStatusAPI


def test_peer_status_snapshot_properties() -> None:
    """Verify PeerStatus properties, state predicates, and serialization."""
    endpoint = PeerEndpoint("127.0.0.1", 8080)
    status = PeerStatus(
        peer_id="worker-node-1",
        endpoint=endpoint,
        state=PeerState.ACTIVE,
        last_seen=100.0,
        last_seen_seconds_ago=2.5,
        registered_at=90.0,
        is_connected=True,
        is_inbound=False,
        dht_distance=42,
        dht_bucket_index=5,
        cpu_percent=25.5,
        memory_percent=55.0,
        active_task_count=2,
        health_status=HealthStatus.HEALTHY,
        metadata={"rack": "us-east-1"},
    )

    assert status.peer_id == "worker-node-1"
    assert status.host == "127.0.0.1"
    assert status.port == 8080
    assert status.is_active is True
    assert status.is_stale is False
    assert status.is_expired is False
    assert status.is_healthy is True
    assert status.is_usable is True

    # Test summary string
    summary = status.summary()
    assert "[ACTIVE] worker-node-1" in summary
    assert "Health: healthy" in summary
    assert "CPU: 25.5%" in summary
    assert "Mem: 55.0%" in summary
    assert "Tasks: 2" in summary

    # Test dictionary serialization
    data = status.to_dict()
    assert data["peer_id"] == "worker-node-1"
    assert data["endpoint"] == {"host": "127.0.0.1", "port": 8080}
    assert data["state"] == "active"
    assert data["health_status"] == "healthy"
    assert data["cpu_percent"] == 25.5
    assert data["metadata"] == {"rack": "us-east-1"}


def test_node_status_snapshot_filtering_and_json() -> None:
    """Verify NodeStatusSnapshot peer filtering, JSON output, and formatting."""
    endpoint1 = PeerEndpoint("127.0.0.1", 8001)
    endpoint2 = PeerEndpoint("127.0.0.1", 8002)
    endpoint3 = PeerEndpoint("127.0.0.1", 8003)

    p1 = PeerStatus(
        peer_id="node-1",
        endpoint=endpoint1,
        state=PeerState.ACTIVE,
        last_seen=100.0,
        last_seen_seconds_ago=1.0,
        registered_at=50.0,
        is_connected=True,
        health_status=HealthStatus.HEALTHY,
    )
    p2 = PeerStatus(
        peer_id="node-2",
        endpoint=endpoint2,
        state=PeerState.STALE,
        last_seen=50.0,
        last_seen_seconds_ago=51.0,
        registered_at=10.0,
        is_connected=False,
        health_status=HealthStatus.DEGRADED,
    )
    p3 = PeerStatus(
        peer_id="node-3",
        endpoint=endpoint3,
        state=PeerState.EXPIRED,
        last_seen=10.0,
        last_seen_seconds_ago=91.0,
        registered_at=5.0,
        is_connected=False,
        health_status=HealthStatus.UNHEALTHY,
    )

    snapshot = NodeStatusSnapshot(
        node_id="coordinator-1",
        host="127.0.0.1",
        port=8000,
        bound_port=8000,
        dht_id_hex="abcdef0123456789abcdef0123456789abcdef01",
        is_running=True,
        health_status=HealthStatus.HEALTHY,
        cpu_percent=12.0,
        memory_percent=45.0,
        active_task_count=1,
        active_connections_count=1,
        total_registered_peers=3,
        active_peers_count=1,
        stale_peers_count=1,
        expired_peers_count=1,
        routing_table_contacts=3,
        gossip_reports_count=3,
        peers=[p1, p2, p3],
    )

    # Retrieval and filtering
    assert snapshot.get_peer("node-1") == p1
    assert snapshot.get_peer("unknown") is None

    assert snapshot.list_peers(state=PeerState.ACTIVE) == [p1]
    assert snapshot.list_peers(state="stale") == [p2]
    assert snapshot.list_peers(health=HealthStatus.UNHEALTHY) == [p3]
    assert snapshot.list_peers(connected_only=True) == [p1]

    # JSON export
    json_str = snapshot.to_json()
    parsed = json.loads(json_str)
    assert parsed["node_id"] == "coordinator-1"
    assert parsed["active_peers_count"] == 1
    assert len(parsed["peers"]) == 3

    # Human-readable format
    text = snapshot.format_text()
    assert "MeshWeaver Node & Peer Status" in text
    assert "coordinator-1" in text
    assert "node-1" in text
    assert "node-2" in text
    assert "node-3" in text


@pytest.mark.asyncio
async def test_node_status_api_basic() -> None:
    """Verify NodeStatusAPI properly inspects a running PeerNode."""
    async with PeerNode(host="127.0.0.1", port=0, peer_id="local-node") as node:
        node.enable_gossip()
        status_api = node.status_api
        assert isinstance(status_api, NodeStatusAPI)

        snapshot = node.get_status()
        assert snapshot.node_id == "local-node"
        assert snapshot.is_running is True
        assert snapshot.bound_port == node.bound_port
        assert snapshot.total_registered_peers == 0

        # Register a peer manually in peer_registry
        node.peer_registry.register("remote-peer", "127.0.0.1", 9999, metadata={"cpu_percent": 15.0})
        updated_snapshot = node.get_status()
        assert updated_snapshot.total_registered_peers == 1
        peer_stat = updated_snapshot.get_peer("remote-peer")
        assert peer_stat is not None
        assert peer_stat.host == "127.0.0.1"
        assert peer_stat.port == 9999
        assert peer_stat.cpu_percent == 15.0
        assert peer_stat.is_active is True


@pytest.mark.asyncio
async def test_joining_mesh_and_peer_discovery() -> None:
    """Verify joining the mesh via bootstrap seed nodes and peer status discovery."""
    # Start two seed nodes
    node_seed_1 = PeerNode(host="127.0.0.1", port=0, peer_id="seed-node-1")
    node_seed_2 = PeerNode(host="127.0.0.1", port=0, peer_id="seed-node-2")
    node_worker = PeerNode(host="127.0.0.1", port=0, peer_id="worker-node-1")

    await node_seed_1.start()
    await node_seed_2.start()
    await node_worker.start()

    try:
        # Worker joins mesh using both seeds
        seeds = [
            f"127.0.0.1:{node_seed_1.bound_port}",
            ("127.0.0.1", node_seed_2.bound_port),
        ]
        joined = await node_worker.join_mesh(seeds, timeout=3.0)
        assert len(joined) >= 2

        # Check worker status
        worker_status = node_worker.get_status()
        assert worker_status.total_registered_peers >= 2
        assert worker_status.active_peers_count >= 2

        seed1_status = worker_status.get_peer("seed-node-1")
        assert seed1_status is not None
        assert seed1_status.is_active is True
        assert seed1_status.is_connected is True
        assert seed1_status.port == node_seed_1.bound_port

        seed2_status = worker_status.get_peer("seed-node-2")
        assert seed2_status is not None
        assert seed2_status.is_active is True
        assert seed2_status.is_connected is True
        assert seed2_status.port == node_seed_2.bound_port

        # Check seed 1 status reflects the worker
        seed1_view = node_seed_1.get_status()
        worker_view = seed1_view.get_peer("worker-node-1")
        assert worker_view is not None
        assert worker_view.is_active is True
        assert worker_view.port == node_worker.bound_port

    finally:
        await node_worker.stop()
        await node_seed_2.stop()
        await node_seed_1.stop()


@pytest.mark.asyncio
async def test_leaving_mesh_gracefully() -> None:
    """Verify graceful departure using leave_mesh and LEAVE message handling."""
    node_a = PeerNode(host="127.0.0.1", port=0, peer_id="mesh-anchor")
    node_b = PeerNode(host="127.0.0.1", port=0, peer_id="mesh-departing")

    await node_a.start()
    await node_b.start()

    try:
        # B connects to A
        await node_b.connect_to_peer("127.0.0.1", node_a.bound_port, do_handshake=True)
        await asyncio.sleep(0.05)

        # Both nodes recognize each other
        assert node_a.get_status().get_peer("mesh-departing") is not None
        assert node_b.get_status().get_peer("mesh-anchor") is not None

        # B leaves the mesh
        await node_b.leave_mesh(graceful=True)
        await asyncio.sleep(0.1)

        # Node B is stopped
        assert node_b.is_running is False

        # Node A's registry has removed Node B or disconnected
        status_a = node_a.get_status()
        peer_b = status_a.get_peer("mesh-departing")
        assert peer_b is None

    finally:
        await node_a.stop()
        await node_b.stop()


def test_stale_peers_and_ttl_expiration() -> None:
    """Verify peer state transitions from ACTIVE to STALE and EXPIRED."""
    registry = PeerRegistry(stale_threshold=30.0, ttl=60.0)
    node = PeerNode(host="127.0.0.1", port=0, peer_id="status-eval-node", peer_registry=registry)

    base_time = 1000.0
    # Register peer at t=1000.0
    registry.register("peer-churn", "127.0.0.1", 5001, current_time=base_time)

    # At t=1010.0 (10s later): peer is ACTIVE
    snap_10 = node.get_status(current_time=base_time + 10.0)
    p_10 = snap_10.get_peer("peer-churn")
    assert p_10 is not None
    assert p_10.state == PeerState.ACTIVE
    assert p_10.is_active is True
    assert p_10.health_status == HealthStatus.HEALTHY
    assert snap_10.active_peers_count == 1
    assert snap_10.stale_peers_count == 0

    # At t=1035.0 (35s later, > 30s stale_threshold): peer is STALE
    snap_35 = node.get_status(current_time=base_time + 35.0)
    p_35 = snap_35.get_peer("peer-churn")
    assert p_35 is not None
    assert p_35.state == PeerState.STALE
    assert p_35.is_stale is True
    assert p_35.health_status == HealthStatus.DEGRADED
    assert snap_35.active_peers_count == 0
    assert snap_35.stale_peers_count == 1

    # At t=1070.0 (70s later, > 60s ttl): peer is EXPIRED
    snap_70 = node.get_status(current_time=base_time + 70.0)
    p_70 = snap_70.get_peer("peer-churn")
    assert p_70 is not None
    assert p_70.state == PeerState.EXPIRED
    assert p_70.is_expired is True
    assert p_70.health_status == HealthStatus.UNHEALTHY
    assert snap_70.expired_peers_count == 1

    # Purging expired peers removes it
    purged = registry.expire_stale_peers(current_time=base_time + 70.0)
    assert len(purged) == 1
    assert purged[0].peer_id == "peer-churn"
    assert node.get_status(current_time=base_time + 70.0).total_registered_peers == 0


@pytest.mark.asyncio
async def test_resource_report_updates_in_status_api() -> None:
    """Verify that resource reports update CPU, memory, and health in peer status."""
    node = PeerNode(host="127.0.0.1", port=0, peer_id="master-node")
    node.enable_gossip()

    # Pre-register remote peer in registry
    node.peer_registry.register("worker-x", "127.0.0.1", 6001)

    t0 = time.time()
    # 1. First gossip report: healthy load
    rep1 = NodeResourceReport(
        node_id="worker-x",
        timestamp=t0,
        cpu_percent=20.0,
        memory_percent=40.0,
        active_task_count=1,
        health_status=HealthStatus.HEALTHY,
    )
    node.gossip_engine.store.update(rep1, current_time=t0)

    snap1 = node.get_status(current_time=t0)
    peer_stat1 = snap1.get_peer("worker-x")
    assert peer_stat1 is not None
    assert peer_stat1.cpu_percent == 20.0
    assert peer_stat1.memory_percent == 40.0
    assert peer_stat1.active_task_count == 1
    assert peer_stat1.health_status == HealthStatus.HEALTHY

    # 2. Updated gossip report: high load and degraded health
    t1 = t0 + 5.0
    rep2 = NodeResourceReport(
        node_id="worker-x",
        timestamp=t1,
        cpu_percent=88.5,
        memory_percent=92.0,
        active_task_count=8,
        health_status=HealthStatus.DEGRADED,
    )
    node.gossip_engine.store.update(rep2, current_time=t1)

    snap2 = node.get_status(current_time=t1)
    peer_stat2 = snap2.get_peer("worker-x")
    assert peer_stat2 is not None
    assert peer_stat2.cpu_percent == 88.5
    assert peer_stat2.memory_percent == 92.0
    assert peer_stat2.active_task_count == 8
    assert peer_stat2.health_status == HealthStatus.DEGRADED

    # 3. Stale/out-of-order report rejected monotonically
    stale_rep = NodeResourceReport(
        node_id="worker-x",
        timestamp=t0 - 1.0,
        cpu_percent=5.0,
        memory_percent=10.0,
    )
    accepted = node.gossip_engine.store.update(stale_rep, current_time=t1)
    assert accepted is False
    # Store retains the latest values
    assert node.get_status(current_time=t1).get_peer("worker-x").cpu_percent == 88.5


def test_structured_logging(caplog: pytest.LogCaptureFixture) -> None:
    """Verify that log_status emits structured key-values and extra metadata."""
    caplog.set_level(logging.INFO)
    node = PeerNode(host="127.0.0.1", port=0, peer_id="logged-node")
    node.peer_registry.register("peer-foo", "127.0.0.1", 5005)

    data = node.status_api.log_status()
    assert data["node_id"] == "logged-node"
    assert data["total_registered_peers"] == 1

    # Check that caplog captured the structured message
    matching_records = [r for r in caplog.records if "Node status:" in r.message]
    assert len(matching_records) > 0
    rec = matching_records[0]
    assert "node_id=logged-node" in rec.message
    assert "total_peers=1" in rec.message
    assert hasattr(rec, "node_status")
    assert rec.node_status["node_id"] == "logged-node"


def test_cli_status_subcommand_in_process() -> None:
    """Verify that main(['status']) and main(['status', '--json']) execute properly."""
    from meshweaver.__main__ import main

    # 1. Formatted table output
    code = main(["status"])
    assert code == 0

    # 2. JSON output
    code = main(["status", "--json"])
    assert code == 0


def test_cli_status_subprocess_execution() -> None:
    """Verify executing python -m meshweaver status via subprocess."""
    # 1. Human-readable status
    res = subprocess.run(
        [sys.executable, "-m", "meshweaver", "status"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10.0,
    )
    assert res.returncode == 0
    assert "MeshWeaver Node & Peer Status" in res.stdout
    assert "Node ID:" in res.stdout
    assert "Listening Address:" in res.stdout

    # 2. JSON status
    res_json = subprocess.run(
        [sys.executable, "-m", "meshweaver", "status", "--json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10.0,
    )
    assert res_json.returncode == 0
    # stdout should contain valid JSON
    data = json.loads(res_json.stdout.strip())
    assert "node_id" in data
    assert "is_running" in data
    assert "peers" in data
    assert isinstance(data["peers"], list)


@pytest.mark.asyncio
async def test_cli_with_bootstrap_seeds() -> None:
    """Verify CLI status query against an active seed node."""
    seed = PeerNode(host="127.0.0.1", port=0, peer_id="cli-target-seed")
    await seed.start()
    try:
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "meshweaver",
            "status",
            "--seeds",
            f"127.0.0.1:{seed.bound_port}",
            "--json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        assert proc.returncode == 0, f"Process failed with stderr:\n{stderr_bytes.decode()}"
        data = json.loads(stdout_bytes.decode().strip())
        assert data["total_registered_peers"] >= 1
        peer_ids = [p["peer_id"] for p in data["peers"]]
        assert "cli-target-seed" in peer_ids
    finally:
        await seed.stop()

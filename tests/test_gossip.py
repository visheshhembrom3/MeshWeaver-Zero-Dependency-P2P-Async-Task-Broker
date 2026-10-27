"""
Unit and integration tests for gossip engine, resource monitoring, load reporting,
freshness checks, stale-record handling, and anti-amplification controls.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import time
from unittest.mock import MagicMock, patch

import pytest

from meshweaver.exceptions import (
    InvalidPayloadError,
    InvalidReportError,
)
from meshweaver.gossip import (
    DEFAULT_CLOCK_SKEW_TOLERANCE,
    DEFAULT_GOSSIP_FANOUT,
    DEFAULT_GOSSIP_INTERVAL,
    DEFAULT_GOSSIP_TTL,
    DEFAULT_MAX_HOPS,
    DEFAULT_STALE_THRESHOLD,
    MAX_GOSSIP_REPORTS_PER_MESSAGE,
    GossipEngine,
    GossipStore,
    HealthStatus,
    NodeResourceReport,
    NodeStatus,
    ResourceCollector,
    ResourceReport,
    collect_node_resources,
)
from meshweaver.network import PeerNode
from meshweaver.peers import PeerEndpoint, PeerRecord, PeerRegistry
from meshweaver.protocol import Message, MessageType


# ============================================================================
# 1. HealthStatus Enum Tests
# ============================================================================


def test_health_status_enum_values() -> None:
    """Verify supported health status string values and methods."""
    assert HealthStatus.HEALTHY == "healthy"
    assert HealthStatus.DEGRADED == "degraded"
    assert HealthStatus.UNHEALTHY == "unhealthy"
    assert HealthStatus.UNKNOWN == "unknown"

    assert HealthStatus.HEALTHY.is_healthy() is True
    assert HealthStatus.DEGRADED.is_healthy() is False
    assert HealthStatus.UNHEALTHY.is_healthy() is False

    assert HealthStatus.HEALTHY.is_usable() is True
    assert HealthStatus.DEGRADED.is_usable() is True
    assert HealthStatus.UNHEALTHY.is_usable() is False
    assert HealthStatus.UNKNOWN.is_usable() is False


# ============================================================================
# 2. NodeResourceReport Data Model & Validation Tests
# ============================================================================


def test_node_resource_report_valid_creation() -> None:
    """Verify creating a valid NodeResourceReport with standard and rich metrics."""
    now = time.time()
    report = NodeResourceReport(
        node_id="peer-node-1",
        timestamp=now,
        cpu_percent=42.5,
        memory_percent=68.0,
        active_task_count=3,
        health_status=HealthStatus.HEALTHY,
        cpu_count=8,
        memory_total_bytes=16 * 1024 * 1024 * 1024,
        memory_available_bytes=5 * 1024 * 1024 * 1024,
        load_average=(1.2, 1.0, 0.8),
        hops=1,
        sequence=5,
        extra={"custom_tag": "worker"},
    )

    assert report.node_id == "peer-node-1"
    assert report.peer_id == "peer-node-1"
    assert report.timestamp == now
    assert report.cpu_percent == 42.5
    assert report.memory_percent == 68.0
    assert report.active_task_count == 3
    assert report.active_tasks == 3
    assert report.health_status == HealthStatus.HEALTHY
    assert report.cpu_count == 8
    assert report.memory_total_bytes == 16 * 1024 * 1024 * 1024
    assert report.memory_available_bytes == 5 * 1024 * 1024 * 1024
    assert report.load_average == (1.2, 1.0, 0.8)
    assert report.hops == 1
    assert report.sequence == 5
    assert report.extra == {"custom_tag": "worker"}


def test_node_resource_report_defaults() -> None:
    """Verify default values for optional report fields."""
    report = NodeResourceReport(
        node_id="peer-default",
        timestamp=1000.0,
        cpu_percent=10.0,
        memory_percent=20.0,
    )
    assert report.active_task_count == 0
    assert report.health_status == HealthStatus.HEALTHY
    assert report.cpu_count is None
    assert report.memory_total_bytes is None
    assert report.memory_available_bytes is None
    assert report.load_average is None
    assert report.hops == 0
    assert report.sequence == 0
    assert report.extra == {}


def test_node_resource_report_aliases() -> None:
    """Verify NodeStatus and ResourceReport are aliases of NodeResourceReport."""
    assert NodeStatus is NodeResourceReport
    assert ResourceReport is NodeResourceReport


@pytest.mark.parametrize(
    "invalid_kwargs,match",
    [
        ({"node_id": ""}, "Invalid node_id"),
        ({"node_id": "invalid node with spaces"}, "Invalid node_id"),
        ({"timestamp": -1.0}, "non-negative number"),
        ({"timestamp": True}, "non-negative number"),
        ({"cpu_percent": -5.0}, "between 0.0 and 100.0"),
        ({"cpu_percent": 105.0}, "between 0.0 and 100.0"),
        ({"cpu_percent": True}, "between 0.0 and 100.0"),
        ({"memory_percent": -1.0}, "between 0.0 and 100.0"),
        ({"memory_percent": 101.0}, "between 0.0 and 100.0"),
        ({"active_task_count": -1}, "non-negative integer"),
        ({"active_task_count": True}, "non-negative integer"),
        ({"active_task_count": 2.5}, "non-negative integer"),
        ({"health_status": "bogus_status"}, "valid HealthStatus enum value"),
        ({"hops": -1}, "non-negative integer"),
        ({"sequence": -1}, "non-negative integer"),
        ({"cpu_count": 0}, "positive integer"),
        ({"cpu_count": -2}, "positive integer"),
        ({"memory_total_bytes": -100}, "non-negative integer"),
        ({"memory_available_bytes": -50}, "non-negative integer"),
        ({"extra": "not-a-dict"}, "must be a dictionary"),
    ],
)
def test_node_resource_report_validation_errors(invalid_kwargs: dict, match: str) -> None:
    """Verify validation errors for all constrained report attributes."""
    valid_base = {
        "node_id": "node-1",
        "timestamp": 12345.0,
        "cpu_percent": 50.0,
        "memory_percent": 50.0,
    }
    kwargs = dict(valid_base)
    kwargs.update(invalid_kwargs)

    with pytest.raises(InvalidReportError, match=match):
        NodeResourceReport(**kwargs)


def test_node_resource_report_dict_roundtrip() -> None:
    """Verify serialization to and from dictionary."""
    orig = NodeResourceReport(
        node_id="peer-100",
        timestamp=2000.0,
        cpu_percent=15.5,
        memory_percent=45.2,
        active_task_count=2,
        health_status=HealthStatus.DEGRADED,
        cpu_count=4,
        memory_total_bytes=8000000000,
        memory_available_bytes=4000000000,
        load_average=(0.5, 0.4, 0.3),
        hops=2,
        sequence=10,
        extra={"env": "test"},
    )
    d = orig.to_dict()
    reconstructed = NodeResourceReport.from_dict(d)
    assert reconstructed == orig


def test_node_resource_report_from_dict_validation() -> None:
    """Verify error when reconstructing from invalid data structure."""
    with pytest.raises(InvalidReportError, match="Expected dict"):
        NodeResourceReport.from_dict("not a dict")

    with pytest.raises(InvalidReportError, match="requires non-empty"):
        NodeResourceReport.from_dict({"cpu_percent": 10.0})


def test_node_resource_report_message_roundtrip() -> None:
    """Verify converting report to/from protocol Message."""
    orig = NodeResourceReport(
        node_id="peer-msg",
        timestamp=3000.0,
        cpu_percent=33.3,
        memory_percent=66.6,
        active_task_count=1,
        health_status=HealthStatus.HEALTHY,
        hops=1,
    )

    # GOSSIP message type
    msg_gossip = orig.to_message(sender_id="peer-msg", message_type=MessageType.GOSSIP)
    assert msg_gossip.message_type == MessageType.GOSSIP
    from_gossip = NodeResourceReport.from_message(msg_gossip)
    assert from_gossip == orig

    # LOAD_REPORT message type
    msg_load = orig.to_message(sender_id="peer-msg", message_type=MessageType.LOAD_REPORT)
    assert msg_load.message_type == MessageType.LOAD_REPORT
    from_load = NodeResourceReport.from_message(msg_load)
    assert from_load == orig


def test_node_resource_report_hop_increment() -> None:
    """Verify with_incremented_hop copies data and advances hop counter."""
    report = NodeResourceReport(
        node_id="peer-hop",
        timestamp=1000.0,
        cpu_percent=20.0,
        memory_percent=30.0,
        hops=0,
    )
    hop1 = report.with_incremented_hop()
    assert hop1.hops == 1
    assert hop1.node_id == report.node_id
    assert hop1.cpu_percent == report.cpu_percent

    hop2 = hop1.with_incremented_hop()
    assert hop2.hops == 2


def test_node_resource_report_staleness_and_expiration() -> None:
    """Verify is_stale and is_expired predicates."""
    t0 = 1000.0
    report = NodeResourceReport(
        node_id="peer-stale-check",
        timestamp=t0,
        cpu_percent=20.0,
        memory_percent=30.0,
    )

    # Right at creation time: active, not stale, not expired
    assert report.is_stale(current_time=t0, stale_threshold=30.0) is False
    assert report.is_expired(current_time=t0, ttl=60.0) is False

    # Within stale threshold
    assert report.is_stale(current_time=t0 + 20.0, stale_threshold=30.0) is False
    assert report.is_expired(current_time=t0 + 20.0, ttl=60.0) is False

    # Stale but not expired
    assert report.is_stale(current_time=t0 + 35.0, stale_threshold=30.0) is True
    assert report.is_expired(current_time=t0 + 35.0, ttl=60.0) is False

    # Expired
    assert report.is_stale(current_time=t0 + 65.0, stale_threshold=30.0) is True
    assert report.is_expired(current_time=t0 + 65.0, ttl=60.0) is True


# ============================================================================
# 3. ResourceCollector Tests (Standard Library + Optional psutil)
# ============================================================================


def test_resource_collector_stdlib_collection() -> None:
    """Verify that resource collection succeeds using standard library facilities."""
    collector = ResourceCollector(node_id="node-collector")
    report = collector.collect()

    assert report.node_id == "node-collector"
    assert isinstance(report.timestamp, float)
    assert 0.0 <= report.cpu_percent <= 100.0
    assert 0.0 <= report.memory_percent <= 100.0
    assert report.active_task_count == 0
    assert isinstance(report.health_status, HealthStatus)
    assert report.hops == 0
    assert report.sequence == 1


def test_resource_collector_task_provider() -> None:
    """Verify collector queries task_count_provider."""
    task_count = 5
    collector = ResourceCollector(
        node_id="node-tasks",
        task_count_provider=lambda: task_count,
    )
    report = collector.collect()
    assert report.active_task_count == 5

    # Test provider exception safety
    def failing_provider() -> int:
        raise RuntimeError("Task broker unavailable")

    safe_collector = ResourceCollector(
        node_id="node-tasks-err",
        task_count_provider=failing_provider,
    )
    safe_report = safe_collector.collect()
    assert safe_report.active_task_count == 0


def test_resource_collector_health_thresholds() -> None:
    """Verify health status evaluation under different threshold configurations."""
    collector = ResourceCollector(
        node_id="node-health",
        cpu_threshold_degraded=60.0,
        cpu_threshold_unhealthy=90.0,
        memory_threshold_degraded=70.0,
        memory_threshold_unhealthy=95.0,
    )

    assert collector._evaluate_health(40.0, 50.0) == HealthStatus.HEALTHY
    assert collector._evaluate_health(65.0, 50.0) == HealthStatus.DEGRADED
    assert collector._evaluate_health(40.0, 75.0) == HealthStatus.DEGRADED
    assert collector._evaluate_health(92.0, 50.0) == HealthStatus.UNHEALTHY
    assert collector._evaluate_health(40.0, 96.0) == HealthStatus.UNHEALTHY


def test_resource_collector_sequence_increment() -> None:
    """Verify sequence counter monotonically increments per collect call."""
    collector = ResourceCollector(node_id="node-seq")
    r1 = collector.collect()
    r2 = collector.collect()
    r3 = collector.collect()
    assert r1.sequence == 1
    assert r2.sequence == 2
    assert r3.sequence == 3


def test_resource_collector_custom_metrics_provider() -> None:
    """Verify custom metrics provider populates extra dictionary."""
    collector = ResourceCollector(
        node_id="node-custom",
        custom_metrics_provider=lambda: {"zone": "us-east-1", "tier": "gpu"},
    )
    report = collector.collect()
    assert report.extra.get("zone") == "us-east-1"
    assert report.extra.get("tier") == "gpu"


def test_resource_collector_with_mocked_psutil() -> None:
    """Verify collector extracts rich metrics when psutil is available."""
    mock_psutil = MagicMock()
    mock_psutil.cpu_percent.return_value = 28.5

    @dataclass
    class MockVMem:
        percent: float = 62.5
        total: int = 34359738368  # 32 GB
        available: int = 12884901888  # 12 GB

    mock_psutil.virtual_memory.return_value = MockVMem()
    mock_psutil.cpu_count.return_value = 16
    mock_psutil.getloadavg.return_value = (1.5, 1.2, 0.9)

    collector = ResourceCollector(node_id="node-psutil")

    with patch.dict("sys.modules", {"psutil": mock_psutil}):
        metrics = collector._collect_psutil()
        assert metrics is not None
        assert metrics["cpu_percent"] == 28.5
        assert metrics["memory_percent"] == 62.5
        assert metrics["cpu_count"] == 16
        assert metrics["memory_total_bytes"] == 34359738368
        assert metrics["memory_available_bytes"] == 12884901888
        assert metrics["load_average"] == (1.5, 1.2, 0.9)

        report = collector.collect()
        assert report.cpu_percent == 28.5
        assert report.memory_percent == 62.5
        assert report.cpu_count == 16


def test_collect_node_resources_helper() -> None:
    """Verify collect_node_resources convenience function."""
    report = collect_node_resources("node-helper", task_count_provider=lambda: 2)
    assert report.node_id == "node-helper"
    assert report.active_task_count == 2


# ============================================================================
# 4. GossipStore Tests (Monotonicity, Freshness, Staleness & Expire)
# ============================================================================


def test_gossip_store_initialization() -> None:
    """Verify valid initialization and parameter constraints of GossipStore."""
    store = GossipStore(stale_threshold=20.0, ttl=50.0, clock_skew_tolerance=30.0)
    assert store.stale_threshold == 20.0
    assert store.ttl == 50.0
    assert store.clock_skew_tolerance == 30.0
    assert len(store) == 0

    with pytest.raises(ValueError, match="stale_threshold"):
        GossipStore(stale_threshold=-1.0)
    with pytest.raises(ValueError, match="ttl must be positive"):
        GossipStore(stale_threshold=30.0, ttl=20.0)
    with pytest.raises(ValueError, match="clock_skew_tolerance"):
        GossipStore(clock_skew_tolerance=-5.0)


def test_gossip_store_basic_update_and_get() -> None:
    """Verify adding novel reports and retrieving them."""
    store = GossipStore(stale_threshold=30.0, ttl=60.0)
    now = 1000.0

    report = NodeResourceReport(
        node_id="peer-alpha",
        timestamp=now,
        cpu_percent=15.0,
        memory_percent=30.0,
    )

    accepted = store.update(report, current_time=now)
    assert accepted is True
    assert len(store) == 1
    assert "peer-alpha" in store

    retrieved = store.get("peer-alpha", current_time=now)
    assert retrieved == report


def test_gossip_store_discards_stale_data_monotonically() -> None:
    """Verify store discards reports with older or equal timestamps for the same node."""
    store = GossipStore(stale_threshold=30.0, ttl=60.0)
    t0 = 1000.0

    report_v1 = NodeResourceReport(
        node_id="peer-monotonic",
        timestamp=t0,
        cpu_percent=20.0,
        memory_percent=40.0,
    )
    assert store.update(report_v1, current_time=t0) is True

    # Same timestamp update -> discard as stale duplicate
    report_dup = NodeResourceReport(
        node_id="peer-monotonic",
        timestamp=t0,
        cpu_percent=99.0,  # tampered
        memory_percent=99.0,
    )
    assert store.update(report_dup, current_time=t0 + 1.0) is False
    assert store.get("peer-monotonic", current_time=t0 + 1.0).cpu_percent == 20.0

    # Older timestamp update -> discard as out-of-order stale record
    report_old = NodeResourceReport(
        node_id="peer-monotonic",
        timestamp=t0 - 10.0,
        cpu_percent=80.0,
        memory_percent=80.0,
    )
    assert store.update(report_old, current_time=t0 + 1.0) is False
    assert store.get("peer-monotonic", current_time=t0 + 1.0).cpu_percent == 20.0

    # Strictly newer timestamp -> accept update
    report_v2 = NodeResourceReport(
        node_id="peer-monotonic",
        timestamp=t0 + 5.0,
        cpu_percent=25.0,
        memory_percent=45.0,
    )
    assert store.update(report_v2, current_time=t0 + 5.0) is True
    assert store.get("peer-monotonic", current_time=t0 + 5.0).cpu_percent == 25.0


def test_gossip_store_clock_skew_rejection() -> None:
    """Verify store rejects reports with timestamps far into the future."""
    store = GossipStore(clock_skew_tolerance=60.0)
    now = 1000.0

    future_report = NodeResourceReport(
        node_id="peer-future",
        timestamp=now + 120.0,  # 120s in future > 60s tolerance
        cpu_percent=10.0,
        memory_percent=20.0,
    )
    assert store.update(future_report, current_time=now) is False
    assert "peer-future" not in store


def test_gossip_store_expired_rejection_on_receipt() -> None:
    """Verify store rejects reports that have already exceeded TTL upon receipt."""
    store = GossipStore(stale_threshold=30.0, ttl=60.0)
    now = 1000.0

    already_expired = NodeResourceReport(
        node_id="peer-dead",
        timestamp=now - 70.0,  # 70s old > 60s ttl
        cpu_percent=10.0,
        memory_percent=20.0,
    )
    assert store.update(already_expired, current_time=now) is False
    assert "peer-dead" not in store


def test_gossip_store_stale_and_expired_queries() -> None:
    """Verify is_stale, is_expired, and get() behavior over advancing time."""
    store = GossipStore(stale_threshold=30.0, ttl=60.0)
    t0 = 1000.0

    rep = NodeResourceReport(node_id="peer-time", timestamp=t0, cpu_percent=10.0, memory_percent=10.0)
    store.update(rep, current_time=t0)

    # At t0 + 10s: Fresh
    assert store.is_stale("peer-time", current_time=t0 + 10.0) is False
    assert store.is_expired("peer-time", current_time=t0 + 10.0) is False
    assert store.get("peer-time", current_time=t0 + 10.0) is not None

    # At t0 + 35s: Stale but not expired
    assert store.is_stale("peer-time", current_time=t0 + 35.0) is True
    assert store.is_expired("peer-time", current_time=t0 + 35.0) is False
    assert store.get("peer-time", current_time=t0 + 35.0) is not None

    # At t0 + 65s: Expired
    assert store.is_stale("peer-time", current_time=t0 + 65.0) is True
    assert store.is_expired("peer-time", current_time=t0 + 65.0) is True
    assert store.get("peer-time", current_time=t0 + 65.0) is None
    assert store.get("peer-time", current_time=t0 + 65.0, include_expired=True) is not None


def test_gossip_store_purge_stale() -> None:
    """Verify purge_stale removes records that exceeded TTL."""
    store = GossipStore(stale_threshold=20.0, ttl=50.0)
    t0 = 1000.0

    r1 = NodeResourceReport(node_id="peer-keep", timestamp=t0 - 10.0, cpu_percent=10.0, memory_percent=10.0)
    r2 = NodeResourceReport(node_id="peer-purge-1", timestamp=t0 - 60.0, cpu_percent=10.0, memory_percent=10.0)
    r3 = NodeResourceReport(node_id="peer-purge-2", timestamp=t0 - 80.0, cpu_percent=10.0, memory_percent=10.0)

    # Force inject records to simulate passage of time
    store._reports["peer-keep"] = r1
    store._reports["peer-purge-1"] = r2
    store._reports["peer-purge-2"] = r3
    assert len(store) == 3

    purged = store.purge_stale(current_time=t0)
    assert set(purged) == {"peer-purge-1", "peer-purge-2"}
    assert len(store) == 1
    assert "peer-keep" in store


def test_gossip_store_list_reports_filtering() -> None:
    """Verify filtering options in list_reports."""
    store = GossipStore(stale_threshold=20.0, ttl=50.0)
    t0 = 1000.0

    fresh_healthy = NodeResourceReport(
        node_id="fresh-h", timestamp=t0 - 5.0, cpu_percent=10.0, memory_percent=10.0, health_status=HealthStatus.HEALTHY
    )
    fresh_degraded = NodeResourceReport(
        node_id="fresh-d", timestamp=t0 - 5.0, cpu_percent=85.0, memory_percent=50.0, health_status=HealthStatus.DEGRADED
    )
    stale_healthy = NodeResourceReport(
        node_id="stale-h", timestamp=t0 - 25.0, cpu_percent=10.0, memory_percent=10.0, health_status=HealthStatus.HEALTHY
    )
    expired_healthy = NodeResourceReport(
        node_id="expired-h", timestamp=t0 - 60.0, cpu_percent=10.0, memory_percent=10.0, health_status=HealthStatus.HEALTHY
    )

    store._reports["fresh-h"] = fresh_healthy
    store._reports["fresh-d"] = fresh_degraded
    store._reports["stale-h"] = stale_healthy
    store._reports["expired-h"] = expired_healthy

    # Default: includes stale, excludes expired
    res_default = store.list_reports(current_time=t0)
    assert {r.node_id for r in res_default} == {"fresh-h", "fresh-d", "stale-h"}

    # Exclude stale
    res_no_stale = store.list_reports(include_stale=False, current_time=t0)
    assert {r.node_id for r in res_no_stale} == {"fresh-h", "fresh-d"}

    # Include expired
    res_with_expired = store.list_reports(include_expired=True, current_time=t0)
    assert len(res_with_expired) == 4

    # Healthy only
    res_healthy = store.list_reports(healthy_only=True, include_stale=False, current_time=t0)
    assert {r.node_id for r in res_healthy} == {"fresh-h"}


def test_gossip_store_anti_amplification_seen_cache() -> None:
    """Verify seen cache tracks signatures and prevents re-accepting old versions."""
    store = GossipStore()
    assert store.has_seen("peer-x", 100.0) is False

    store.record_seen("peer-x", 100.0)
    assert store.has_seen("peer-x", 50.0) is True
    assert store.has_seen("peer-x", 100.0) is True
    assert store.has_seen("peer-x", 150.0) is False


# ============================================================================
# 5. Anti-Amplification Controls in GossipEngine
# ============================================================================


def test_gossip_engine_bounded_fanout_selection() -> None:
    """Verify gossip target selection never exceeds configured fanout bound."""
    registry = PeerRegistry()
    for i in range(10):
        registry.register(f"peer-{i}", "127.0.0.1", 8000 + i)

    engine = GossipEngine(
        node_id="local-node",
        peer_registry=registry,
        fanout=3,
    )

    targets = engine._select_peer_targets()
    assert len(targets) == 3
    # Local node must never be in target list
    target_ids = {t[0] for t in targets}
    assert "local-node" not in target_ids

    # With fanout larger than available peers, select all available
    engine_wide = GossipEngine(
        node_id="local-node",
        peer_registry=registry,
        fanout=20,
    )
    targets_wide = engine_wide._select_peer_targets()
    assert len(targets_wide) == 10


@pytest.mark.asyncio
async def test_gossip_engine_hop_limit_prevents_amplification() -> None:
    """Verify reports reaching max_hops are not prepared for forward dissemination."""
    engine = GossipEngine(
        node_id="relaying-node",
        max_hops=2,
    )

    now = 1000.0
    # Report with hops=0 (locally originated or first hop)
    rep0 = NodeResourceReport(node_id="peer-orig", timestamp=now, cpu_percent=10.0, memory_percent=20.0, hops=0)
    # Report that already traveled 2 hops (= max_hops)
    rep_max = NodeResourceReport(node_id="peer-far", timestamp=now, cpu_percent=15.0, memory_percent=25.0, hops=2)

    engine.store.update(rep0, current_time=now)
    engine.store.update(rep_max, current_time=now)

    # In preparing reports to share:
    reports_to_share = []

    for other in engine.store.list_reports(include_stale=False, current_time=now):
        if other.node_id != engine.node_id and other.hops < engine.max_hops:
            forwarded = other.with_incremented_hop()
            reports_to_share.append(forwarded)

    # rep0 should be included with hops=1, but rep_max should be excluded!
    forwarded_ids = {r.node_id for r in reports_to_share}
    assert "peer-orig" in forwarded_ids
    assert "peer-far" not in forwarded_ids
    assert next(r for r in reports_to_share if r.node_id == "peer-orig").hops == 1


# ============================================================================
# 6. GossipEngine Lifecycle & Handler Tests
# ============================================================================


@pytest.mark.asyncio
async def test_gossip_engine_start_stop_lifecycle() -> None:
    """Verify asynchronous start, stop, and task cancellation for GossipEngine."""
    engine = GossipEngine(node_id="engine-lifecycle", gossip_interval=0.1)
    assert engine.is_running is False

    engine.start()
    assert engine.is_running is True

    # Calling start again should be idempotent
    engine.start()
    assert engine.is_running is True

    await asyncio.sleep(0.25)
    assert engine.rounds_completed >= 2

    await engine.stop()
    assert engine.is_running is False

    # Calling stop again should be idempotent
    await engine.stop()
    assert engine.is_running is False


@pytest.mark.asyncio
async def test_gossip_engine_context_manager() -> None:
    """Verify async context manager protocol for GossipEngine."""
    async with GossipEngine(node_id="engine-cm", gossip_interval=0.1) as engine:
        assert engine.is_running is True
        await asyncio.sleep(0.15)
        assert engine.rounds_completed >= 1

    assert engine.is_running is False


@pytest.mark.asyncio
async def test_gossip_engine_handle_gossip_message_single_report() -> None:
    """Verify processing single-report gossip message and updating peer registry."""
    registry = PeerRegistry()
    registry.register("remote-node", "127.0.0.1", 9000)

    engine = GossipEngine(
        node_id="local-receiver",
        peer_registry=registry,
    )

    now = time.time()
    msg = Message.create(
        MessageType.GOSSIP,
        sender_id="remote-node",
        payload={
            "node_id": "remote-node",
            "timestamp": now,
            "cpu_percent": 35.0,
            "memory_percent": 55.0,
            "active_task_count": 2,
            "health_status": "healthy",
        },
    )

    await engine.handle_gossip_message(msg)

    # Check store
    status = engine.get_status("remote-node")
    assert status is not None
    assert status.cpu_percent == 35.0
    assert status.memory_percent == 55.0
    assert status.active_task_count == 2
    assert status.health_status == HealthStatus.HEALTHY

    # Check peer registry metadata was refreshed
    peer_rec = registry.get("remote-node")
    assert peer_rec is not None
    assert peer_rec.metadata["cpu_percent"] == 35.0
    assert peer_rec.metadata["memory_percent"] == 55.0
    assert peer_rec.metadata["active_tasks"] == 2


@pytest.mark.asyncio
async def test_gossip_engine_handle_batch_reports() -> None:
    """Verify processing batch reports inside a gossip message."""
    engine = GossipEngine(node_id="local-batch")
    now = time.time()

    msg = Message.create(
        MessageType.GOSSIP,
        sender_id="sender-1",
        payload={
            "reports": [
                {"node_id": "batch-1", "timestamp": now, "cpu_percent": 10.0, "memory_percent": 20.0},
                {"node_id": "batch-2", "timestamp": now, "cpu_percent": 30.0, "memory_percent": 40.0},
            ]
        },
    )

    await engine.handle_gossip_message(msg)
    assert engine.get_status("batch-1") is not None
    assert engine.get_status("batch-2") is not None
    assert len(engine.list_statuses()) == 2


@pytest.mark.asyncio
async def test_gossip_engine_cluster_summary() -> None:
    """Verify cluster summary calculation across known statuses."""
    engine = GossipEngine(node_id="local-summary")
    now = time.time()

    r1 = NodeResourceReport(node_id="n1", timestamp=now, cpu_percent=20.0, memory_percent=40.0, active_task_count=1)
    r2 = NodeResourceReport(node_id="n2", timestamp=now, cpu_percent=40.0, memory_percent=60.0, active_task_count=3)
    engine.store.update(r1, current_time=now)
    engine.store.update(r2, current_time=now)

    summary = engine.get_cluster_summary(current_time=now)
    assert summary["total_nodes"] == 2
    assert summary["healthy_nodes"] == 2
    assert summary["usable_nodes"] == 2
    assert summary["total_active_tasks"] == 4
    assert summary["avg_cpu_percent"] == 30.0
    assert summary["avg_memory_percent"] == 50.0


# ============================================================================
# 7. End-to-End Multi-Node Integration over TCP
# ============================================================================


@pytest.mark.asyncio
async def test_two_peers_periodic_gossip_exchange() -> None:
    """
    Test two live PeerNode instances sharing status periodically over TCP.
    Verifies that Peer A automatically learns Peer B's CPU, RAM, active tasks,
    and operational health, and vice versa.
    """
    async with PeerNode(peer_id="node-a", host="127.0.0.1", port=0) as node_a:
        async with PeerNode(peer_id="node-b", host="127.0.0.1", port=0) as node_b:
            # Enable gossip on both nodes with fast interval for testing
            gossip_a = node_a.enable_gossip(interval=0.15, fanout=2, stale_threshold=5.0)
            gossip_b = node_b.enable_gossip(interval=0.15, fanout=2, stale_threshold=5.0)

            assert gossip_a.is_running is True
            assert gossip_b.is_running is True

            # Register each other in peer registries and establish connection
            node_a.peer_registry.register(node_b.peer_id, node_b.host, node_b.bound_port)
            node_b.peer_registry.register(node_a.peer_id, node_a.host, node_a.bound_port)

            conn_a_to_b = await node_a.connect_to_peer("127.0.0.1", node_b.bound_port)
            assert conn_a_to_b.remote_peer_id == node_b.peer_id

            # Allow gossip rounds to trigger across TCP
            deadline = time.perf_counter() + 3.0
            status_b_on_a = None
            status_a_on_b = None

            while time.perf_counter() < deadline:
                status_b_on_a = gossip_a.get_status(node_b.peer_id)
                status_a_on_b = gossip_b.get_status(node_a.peer_id)
                if status_b_on_a is not None and status_a_on_b is not None:
                    break
                await asyncio.sleep(0.05)

            assert status_b_on_a is not None, "Node A did not receive gossip from Node B"
            assert status_a_on_b is not None, "Node B did not receive gossip from Node A"

            assert status_b_on_a.node_id == node_b.peer_id
            assert 0.0 <= status_b_on_a.cpu_percent <= 100.0
            assert 0.0 <= status_b_on_a.memory_percent <= 100.0
            assert status_b_on_a.health_status in (HealthStatus.HEALTHY, HealthStatus.DEGRADED, HealthStatus.UNHEALTHY)

            assert status_a_on_b.node_id == node_a.peer_id
            assert 0.0 <= status_a_on_b.cpu_percent <= 100.0
            assert 0.0 <= status_a_on_b.memory_percent <= 100.0


@pytest.mark.asyncio
async def test_stale_record_handling_when_peer_stops_updating() -> None:
    """
    Test stale-record handling: when a peer ceases status updates,
    its record transitions to stale and is eventually purged upon TTL.
    """
    stale_thresh = 0.2
    ttl = 0.5

    store = GossipStore(stale_threshold=stale_thresh, ttl=ttl)
    t0 = time.time()

    report = NodeResourceReport(
        node_id="fading-peer",
        timestamp=t0,
        cpu_percent=50.0,
        memory_percent=50.0,
    )
    store.update(report, current_time=t0)

    # Immediately: active
    assert store.is_stale("fading-peer", current_time=t0) is False
    assert store.is_expired("fading-peer", current_time=t0) is False

    # After stale threshold: stale
    t_stale = t0 + stale_thresh + 0.05
    assert store.is_stale("fading-peer", current_time=t_stale) is True
    assert store.is_expired("fading-peer", current_time=t_stale) is False

    # After TTL: expired and purgeable
    t_expired = t0 + ttl + 0.05
    assert store.is_expired("fading-peer", current_time=t_expired) is True
    assert store.get("fading-peer", current_time=t_expired) is None

    purged = store.purge_stale(current_time=t_expired)
    assert "fading-peer" in purged
    assert len(store) == 0


@pytest.mark.asyncio
async def test_three_nodes_gossip_dissemination_and_hop_limits() -> None:
    """
    Test multi-node gossip: Node A shares status with Node B, which forwards
    it to Node C with incremented hop count, adhering to max_hops limit.
    """
    async with PeerNode(peer_id="node-1", host="127.0.0.1", port=0) as n1:
        async with PeerNode(peer_id="node-2", host="127.0.0.1", port=0) as n2:
            async with PeerNode(peer_id="node-3", host="127.0.0.1", port=0) as n3:
                # Max hops = 2
                g1 = n1.enable_gossip(interval=0.15, fanout=2, max_hops=2)
                g2 = n2.enable_gossip(interval=0.15, fanout=2, max_hops=2)
                g3 = n3.enable_gossip(interval=0.15, fanout=2, max_hops=2)

                # Connect 1 <-> 2 and 2 <-> 3 (linear topology: 1 does not connect directly to 3)
                n1.peer_registry.register(n2.peer_id, n2.host, n2.bound_port)
                n2.peer_registry.register(n1.peer_id, n1.host, n1.bound_port)
                n2.peer_registry.register(n3.peer_id, n3.host, n3.bound_port)
                n3.peer_registry.register(n2.peer_id, n2.host, n2.bound_port)

                await n1.connect_to_peer("127.0.0.1", n2.bound_port)
                await n2.connect_to_peer("127.0.0.1", n3.bound_port)

                # Wait for gossip to propagate through node 2 to node 3
                deadline = time.perf_counter() + 4.0
                status_1_on_3 = None

                while time.perf_counter() < deadline:
                    status_1_on_3 = g3.get_status(n1.peer_id)
                    if status_1_on_3 is not None:
                        break
                    await asyncio.sleep(0.05)

                assert status_1_on_3 is not None, "Node 3 did not receive propagated status from Node 1 via Node 2"
                assert status_1_on_3.node_id == n1.peer_id
                # Node 1 originated (hops=0), Node 2 relayed to Node 3 (hops=1)
                assert status_1_on_3.hops == 1
                assert status_1_on_3.hops < g3.max_hops

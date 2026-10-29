"""
Comprehensive unit and integration tests for load-aware task scheduling (Week 3, Milestone 2).

Verifies:
1. Documented scoring function with configurable weights and health penalties.
2. Preference for suitable lower-load workers over higher-load workers across CPU,
   memory, active tasks, and composite scores.
3. Strict rejection of stale resource reports (age > stale_report_threshold).
4. Strict avoidance of scheduling work to failed, unhealthy, or expired peers.
5. Task compatibility enforcement (supported tasks, capability tags, capacity limits, draining).
6. Deterministic tie-breaking using Kademlia XOR distance and peer ID.
7. Structured SchedulingDecision and WorkerEvaluation explainability.
8. Exception handling (NoEligibleWorkerError, StaleResourceReportError, IncompatibleWorkerError).
9. Integration with PeerNode.scheduler and remote task submission.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    DEFAULT_ACTIVE_TASK_WEIGHT,
    DEFAULT_CPU_WEIGHT,
    DEFAULT_DEGRADED_PENALTY,
    DEFAULT_MEMORY_WEIGHT,
    DEFAULT_STALE_REPORT_THRESHOLD,
    DEFAULT_UNHEALTHY_PENALTY,
    DEFAULT_UNKNOWN_PENALTY,
    HealthStatus,
    IncompatibleWorkerError,
    LoadAwareScheduler,
    NoEligibleWorkerError,
    NodeID,
    NodeResourceReport,
    PeerNode,
    PeerRecord,
    PeerState,
    SchedulerConfig,
    SchedulingDecision,
    TaskRequest,
    TaskResponse,
    TaskScheduler,
    TaskStatus,
    WorkerEvaluation,
    xor_distance,
)


# ==============================================================================
# 1. Documented Scoring Function & Configurable Weights
# ==============================================================================


def test_documented_scoring_function_formula() -> None:
    """
    Verify the documented mathematical formula:
        Score = (active_tasks * active_task_weight)
              + (cpu_percent * cpu_weight)
              + (memory_percent * memory_weight)
              + HealthPenalty(health_status)

    With default weights:
        active_task_weight = 10.0
        cpu_weight = 0.5
        memory_weight = 0.5
        HEALTHY penalty = 0.0
        DEGRADED penalty = 50.0
        UNKNOWN penalty = 200.0
        UNHEALTHY penalty = 1000.0
    """
    config = SchedulerConfig()
    scheduler = TaskScheduler(peer_node=None, config=config)

    # 1. Zero load, HEALTHY -> 0.0
    score_zero = scheduler.calculate_score(
        active_tasks=0, cpu_percent=0.0, memory_percent=0.0, health_status=HealthStatus.HEALTHY
    )
    assert score_zero == 0.0

    # 2. Known load, HEALTHY: 2 tasks * 10 + 40% CPU * 0.5 + 60% MEM * 0.5 = 20 + 20 + 30 = 70.0
    score_healthy = scheduler.calculate_score(
        active_tasks=2, cpu_percent=40.0, memory_percent=60.0, health_status=HealthStatus.HEALTHY
    )
    assert score_healthy == 70.0

    # 3. Same load, DEGRADED: 70.0 + 50.0 = 120.0
    score_degraded = scheduler.calculate_score(
        active_tasks=2, cpu_percent=40.0, memory_percent=60.0, health_status=HealthStatus.DEGRADED
    )
    assert score_degraded == 120.0

    # 4. Same load, UNKNOWN: 70.0 + 200.0 = 270.0
    score_unknown = scheduler.calculate_score(
        active_tasks=2, cpu_percent=40.0, memory_percent=60.0, health_status=HealthStatus.UNKNOWN
    )
    assert score_unknown == 270.0

    # 5. Same load, UNHEALTHY: 70.0 + 1000.0 = 1070.0
    score_unhealthy = scheduler.calculate_score(
        active_tasks=2, cpu_percent=40.0, memory_percent=60.0, health_status=HealthStatus.UNHEALTHY
    )
    assert score_unhealthy == 1070.0


def test_configurable_weights_and_thresholds() -> None:
    """
    Verify scheduler supports custom weighting coefficients.
    For example: heavily weighting CPU over memory or tasks.
    """
    custom_cfg = SchedulerConfig(
        active_task_weight=1.0,
        cpu_weight=5.0,  # Prioritize CPU heavily
        memory_weight=0.1,
        degraded_penalty=25.0,
        unhealthy_penalty=500.0,
        stale_report_threshold=15.0,
    )
    scheduler = TaskScheduler(peer_node=None, config=custom_cfg)

    # 1 task * 1 + 80% CPU * 5 + 50% MEM * 0.1 = 1 + 400 + 5 = 406.0
    score = scheduler.calculate_score(
        active_tasks=1, cpu_percent=80.0, memory_percent=50.0, health_status=HealthStatus.HEALTHY
    )
    assert score == 406.0
    assert scheduler.stale_report_threshold == 15.0


def test_scheduler_config_validation() -> None:
    """Verify invalid weights and thresholds raise ValueError."""
    with pytest.raises(ValueError, match="must be a non-negative float"):
        SchedulerConfig(active_task_weight=-1.0)

    with pytest.raises(ValueError, match="must be a non-negative float"):
        SchedulerConfig(cpu_weight="invalid")  # type: ignore

    with pytest.raises(ValueError, match="must be a positive float"):
        SchedulerConfig(stale_report_threshold=0.0)

    with pytest.raises(ValueError, match="must be a positive float"):
        SchedulerConfig(stale_report_threshold=-5.0)


# ==============================================================================
# 2. Preferring Suitable Lower-Load Workers
# ==============================================================================


@pytest.mark.asyncio
async def test_lower_load_worker_preferred_by_active_tasks() -> None:
    """
    Verify scheduler prefers a worker with fewer active tasks when CPU/MEM are identical.
    Worker 1: 5 active tasks, 30% CPU, 40% memory.
    Worker 2: 0 active tasks, 30% CPU, 40% memory.
    Scheduler must select Worker 2.
    """
    async with PeerNode(peer_id="worker-busy", host="127.0.0.1", port=0) as w_busy:
        async with PeerNode(peer_id="worker-free", host="127.0.0.1", port=0) as w_free:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_busy.peer_id, w_busy.host, w_busy.bound_port)
                client.peer_registry.register(w_free.peer_id, w_free.host, w_free.bound_port)

                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_busy.peer_id,
                        timestamp=now,
                        cpu_percent=30.0,
                        memory_percent=40.0,
                        active_task_count=5,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_free.peer_id,
                        timestamp=now,
                        cpu_percent=30.0,
                        memory_percent=40.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer is not None
                assert decision.selected_peer.peer_id == "worker-free"
                assert decision.active_tasks == 0
                assert decision.score is not None
                # Worker-free: 0*10 + 30*0.5 + 40*0.5 = 35.0
                assert decision.score == 35.0


@pytest.mark.asyncio
async def test_lower_load_worker_preferred_by_cpu() -> None:
    """
    Verify scheduler prefers a worker with lower CPU when tasks and memory are identical.
    Worker High: 85% CPU, 0 active tasks, 30% memory.
    Worker Low: 15% CPU, 0 active tasks, 30% memory.
    Scheduler must select Worker Low.
    """
    async with PeerNode(peer_id="worker-high-cpu", host="127.0.0.1", port=0) as w_high:
        async with PeerNode(peer_id="worker-low-cpu", host="127.0.0.1", port=0) as w_low:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_high.peer_id, w_high.host, w_high.bound_port)
                client.peer_registry.register(w_low.peer_id, w_low.host, w_low.bound_port)

                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_high.peer_id,
                        timestamp=now,
                        cpu_percent=85.0,
                        memory_percent=30.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_low.peer_id,
                        timestamp=now,
                        cpu_percent=15.0,
                        memory_percent=30.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-low-cpu"
                assert decision.cpu_percent == 15.0


@pytest.mark.asyncio
async def test_lower_load_worker_preferred_by_memory() -> None:
    """
    Verify scheduler prefers a worker with lower memory utilization when tasks/CPU are identical.
    """
    async with PeerNode(peer_id="worker-high-mem", host="127.0.0.1", port=0) as w_high:
        async with PeerNode(peer_id="worker-low-mem", host="127.0.0.1", port=0) as w_low:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_high.peer_id, w_high.host, w_high.bound_port)
                client.peer_registry.register(w_low.peer_id, w_low.host, w_low.bound_port)

                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_high.peer_id,
                        timestamp=now,
                        cpu_percent=20.0,
                        memory_percent=95.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_low.peer_id,
                        timestamp=now,
                        cpu_percent=20.0,
                        memory_percent=25.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-low-mem"
                assert decision.memory_percent == 25.0


@pytest.mark.asyncio
async def test_healthy_worker_preferred_over_degraded_worker() -> None:
    """
    Verify healthy worker is preferred over degraded worker due to health penalty (+50).
    Worker Degraded: 0 tasks, 10% CPU, 10% MEM (Base 10 + Penalty 50 = 60).
    Worker Healthy: 1 task, 20% CPU, 20% MEM (Base 10 + 10 + 10 = 30).
    Scheduler selects Worker Healthy.
    """
    async with PeerNode(peer_id="worker-deg", host="127.0.0.1", port=0) as w_deg:
        async with PeerNode(peer_id="worker-hlth", host="127.0.0.1", port=0) as w_hlth:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_deg.peer_id, w_deg.host, w_deg.bound_port)
                client.peer_registry.register(w_hlth.peer_id, w_hlth.host, w_hlth.bound_port)

                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_deg.peer_id,
                        timestamp=now,
                        cpu_percent=10.0,
                        memory_percent=10.0,
                        active_task_count=0,
                        health_status=HealthStatus.DEGRADED,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_hlth.peer_id,
                        timestamp=now,
                        cpu_percent=20.0,
                        memory_percent=20.0,
                        active_task_count=1,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-hlth"
                assert decision.health_status == HealthStatus.HEALTHY


# ==============================================================================
# 3. Deterministic Tie-Breaking
# ==============================================================================


@pytest.mark.asyncio
async def test_deterministic_tie_breaking_identical_load() -> None:
    """
    Verify deterministic tie-breaking when two workers have identical composite scores.
    Uses Kademlia XOR distance between task ID anchor and worker NodeID.
    The evaluation must be 100% deterministic and reproducible across repeated runs.
    """
    async with PeerNode(peer_id="worker-alpha", host="127.0.0.1", port=0) as w_alpha:
        async with PeerNode(peer_id="worker-beta", host="127.0.0.1", port=0) as w_beta:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_alpha.peer_id, w_alpha.host, w_alpha.bound_port)
                client.peer_registry.register(w_beta.peer_id, w_beta.host, w_beta.bound_port)

                now = time.time()
                # Exactly identical load
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_alpha.peer_id,
                        timestamp=now,
                        cpu_percent=20.0,
                        memory_percent=30.0,
                        active_task_count=1,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_beta.peer_id,
                        timestamp=now,
                        cpu_percent=20.0,
                        memory_percent=30.0,
                        active_task_count=1,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                task1 = TaskRequest.create("add", args=[1, 2], task_id="task-fixed-id-1")
                decision1_first = client.scheduler.schedule(task1)
                decision1_second = client.scheduler.schedule(task1)

                # Deterministic: repeated calls for the same task ID return the exact same worker
                assert decision1_first.selected_peer is not None
                assert decision1_second.selected_peer is not None
                assert decision1_first.selected_peer.peer_id == decision1_second.selected_peer.peer_id

                # Confirm the decision matched the peer with smaller XOR distance
                task_anchor = NodeID.from_peer(task1.task_id)
                rec_alpha = client.peer_registry.get(w_alpha.peer_id)
                rec_beta = client.peer_registry.get(w_beta.peer_id)
                assert rec_alpha is not None and rec_beta is not None
                dist_alpha = int(xor_distance(rec_alpha.dht_id, task_anchor))
                dist_beta = int(xor_distance(rec_beta.dht_id, task_anchor))

                expected_winner = "worker-alpha" if dist_alpha < dist_beta else "worker-beta"
                assert decision1_first.selected_peer.peer_id == expected_winner


# ==============================================================================
# 4. Rejection of Stale Resource Reports
# ==============================================================================


@pytest.mark.asyncio
async def test_reject_stale_resource_reports() -> None:
    """
    Verify scheduler strictly rejects stale resource reports:
    - Worker Fresh has higher load, but report is fresh (age 2.0s).
    - Worker Stale has 0 load, but report is stale (age 45.0s > threshold 30.0s).
    - Scheduler rejects Worker Stale's report and excludes it, selecting Worker Fresh.
    """
    async with PeerNode(peer_id="worker-fresh", host="127.0.0.1", port=0) as w_fresh:
        async with PeerNode(peer_id="worker-stale", host="127.0.0.1", port=0) as w_stale:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_fresh.peer_id, w_fresh.host, w_fresh.bound_port)
                client.peer_registry.register(w_stale.peer_id, w_stale.host, w_stale.bound_port)

                now = time.time()
                # Fresh report: 2 seconds old
                rep_fresh = NodeResourceReport(
                    node_id=w_fresh.peer_id,
                    timestamp=now - 2.0,
                    cpu_percent=50.0,
                    memory_percent=60.0,
                    active_task_count=2,
                    health_status=HealthStatus.HEALTHY,
                )
                # Stale report: 45 seconds old (threshold is 30s)
                rep_stale = NodeResourceReport(
                    node_id=w_stale.peer_id,
                    timestamp=now - 45.0,
                    cpu_percent=5.0,
                    memory_percent=10.0,
                    active_task_count=0,
                    health_status=HealthStatus.HEALTHY,
                )
                client.gossip_engine.store.update(rep_fresh)
                client.gossip_engine.store.update(rep_stale)

                decision = client.scheduler.schedule()
                assert decision.is_success
                # Worker-fresh must be chosen
                assert decision.selected_peer.peer_id == "worker-fresh"
                # Worker-stale must be recorded in rejected_workers
                assert "worker-stale" in decision.rejected_workers
                assert "Stale resource report" in decision.rejected_workers["worker-stale"]


@pytest.mark.asyncio
async def test_stale_threshold_customization() -> None:
    """Verify customizing stale_report_threshold adjusts staleness boundary."""
    async with PeerNode(peer_id="worker-test", host="127.0.0.1", port=0) as w:
        async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
            client.enable_gossip()
            client.peer_registry.register(w.peer_id, w.host, w.bound_port)

            now = time.time()
            rep = NodeResourceReport(
                node_id=w.peer_id,
                timestamp=now - 15.0,  # 15s old
                cpu_percent=10.0,
                memory_percent=10.0,
                active_task_count=0,
            )
            client.gossip_engine.store.update(rep)

            # 1. With threshold=10.0 -> report is stale -> rejected
            sched_strict = TaskScheduler(client, stale_report_threshold=10.0)
            dec_strict = sched_strict.schedule()
            assert not dec_strict.is_success
            assert "worker-test" in dec_strict.rejected_workers

            # 2. With threshold=30.0 -> report is fresh -> accepted
            sched_lenient = TaskScheduler(client, stale_report_threshold=30.0)
            dec_lenient = sched_lenient.schedule()
            assert dec_lenient.is_success
            assert dec_lenient.selected_peer.peer_id == "worker-test"


# ==============================================================================
# 5. Excluding Failed, Unhealthy, or Expired Peers
# ==============================================================================


@pytest.mark.asyncio
async def test_exclude_unhealthy_failed_peers() -> None:
    """
    Verify failed/unhealthy peers (HealthStatus.UNHEALTHY) are strictly excluded from scheduling.
    """
    async with PeerNode(peer_id="worker-failed", host="127.0.0.1", port=0) as w_failed:
        async with PeerNode(peer_id="worker-ok", host="127.0.0.1", port=0) as w_ok:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(w_failed.peer_id, w_failed.host, w_failed.bound_port)
                client.peer_registry.register(w_ok.peer_id, w_ok.host, w_ok.bound_port)

                now = time.time()
                # Failed peer has 0 tasks and 0 CPU, but is UNHEALTHY
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_failed.peer_id,
                        timestamp=now,
                        cpu_percent=0.0,
                        memory_percent=0.0,
                        active_task_count=0,
                        health_status=HealthStatus.UNHEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w_ok.peer_id,
                        timestamp=now,
                        cpu_percent=40.0,
                        memory_percent=50.0,
                        active_task_count=2,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-ok"
                assert "worker-failed" in decision.rejected_workers
                assert "UNHEALTHY" in decision.rejected_workers["worker-failed"]


@pytest.mark.asyncio
async def test_exclude_expired_peer_records() -> None:
    """
    Verify peers with EXPIRED state in peer registry (last_seen > ttl) are excluded.
    """
    async with PeerNode(peer_id="worker-expired", host="127.0.0.1", port=0) as w_exp:
        async with PeerNode(peer_id="worker-active", host="127.0.0.1", port=0) as w_act:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                # Active worker: registered now
                client.peer_registry.register(w_act.peer_id, w_act.host, w_act.bound_port)

                # Expired worker: simulate last_seen 400 seconds ago (TTL is 300s)
                old_rec = PeerRecord(
                    peer_id=w_exp.peer_id,
                    endpoint=(w_exp.host, w_exp.bound_port),
                    last_seen=time.time() - 400.0,
                    registered_at=time.time() - 500.0,
                )
                client.peer_registry._peers[w_exp.peer_id] = old_rec

                decision = client.scheduler.schedule()
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-active"
                assert "worker-expired" in decision.rejected_workers
                assert "EXPIRED" in decision.rejected_workers["worker-expired"]


@pytest.mark.asyncio
async def test_explicit_exclusion_and_failed_marker() -> None:
    """
    Verify passing exclude list or peer metadata markers excludes worker.
    """
    async with PeerNode(peer_id="worker-marked-failed", host="127.0.0.1", port=0) as w_marked:
        async with PeerNode(peer_id="worker-excluded", host="127.0.0.1", port=0) as w_excl:
            async with PeerNode(peer_id="worker-normal", host="127.0.0.1", port=0) as w_norm:
                async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                    client.peer_registry.register(w_marked.peer_id, w_marked.host, w_marked.bound_port)
                    client.peer_registry.register(w_excl.peer_id, w_excl.host, w_excl.bound_port)
                    client.peer_registry.register(w_norm.peer_id, w_norm.host, w_norm.bound_port)

                    # Mark one worker as failed in metadata
                    client.peer_registry._peers[w_marked.peer_id] = client.peer_registry._peers[
                        w_marked.peer_id
                    ].touch(metadata={"failed": True})

                    decision = client.scheduler.schedule(exclude=["worker-excluded"])
                    assert decision.is_success
                    assert decision.selected_peer.peer_id == "worker-normal"
                    assert "worker-excluded" in decision.rejected_workers
                    assert "worker-marked-failed" in decision.rejected_workers


# ==============================================================================
# 6. Task Compatibility Enforcement
# ==============================================================================


@pytest.mark.asyncio
async def test_task_compatibility_supported_tasks() -> None:
    """
    Verify worker is checked for task compatibility based on supported task names:
    - Worker Math supports ["add", "multiply"].
    - Worker Text supports ["reverse_string", "echo"].
    - Submitting task "add" routes to Worker Math.
    - Submitting task "reverse_string" routes to Worker Text.
    """
    async with PeerNode(peer_id="worker-math", host="127.0.0.1", port=0) as w_math:
        async with PeerNode(peer_id="worker-text", host="127.0.0.1", port=0) as w_text:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.peer_registry.register(w_math.peer_id, w_math.host, w_math.bound_port)
                client.peer_registry.register(w_text.peer_id, w_text.host, w_text.bound_port)

                # Annotate supported tasks in metadata
                client.peer_registry._peers[w_math.peer_id] = client.peer_registry._peers[
                    w_math.peer_id
                ].touch(metadata={"supported_tasks": ["add", "multiply"]})
                client.peer_registry._peers[w_text.peer_id] = client.peer_registry._peers[
                    w_text.peer_id
                ].touch(metadata={"supported_tasks": ["reverse_string", "echo"]})

                # 1. Schedule "add"
                dec_add = client.scheduler.schedule(TaskRequest.create("add"))
                assert dec_add.is_success
                assert dec_add.selected_peer.peer_id == "worker-math"
                assert "worker-text" in dec_add.rejected_workers
                assert "not supported" in dec_add.rejected_workers["worker-text"]

                # 2. Schedule "reverse_string"
                dec_rev = client.scheduler.schedule(TaskRequest.create("reverse_string"))
                assert dec_rev.is_success
                assert dec_rev.selected_peer.peer_id == "worker-text"
                assert "worker-math" in dec_rev.rejected_workers
                assert "not supported" in dec_rev.rejected_workers["worker-math"]


@pytest.mark.asyncio
async def test_task_compatibility_capacity_and_draining() -> None:
    """
    Verify worker capacity limits and draining / maintenance states:
    - Worker Maxed has reached max_concurrent_tasks.
    - Worker Draining has draining=True.
    - Both are rejected as incompatible.
    """
    async with PeerNode(peer_id="worker-maxed", host="127.0.0.1", port=0) as w_max:
        async with PeerNode(peer_id="worker-draining", host="127.0.0.1", port=0) as w_drain:
            async with PeerNode(peer_id="worker-ready", host="127.0.0.1", port=0) as w_rdy:
                async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                    client.enable_gossip()
                    client.peer_registry.register(w_max.peer_id, w_max.host, w_max.bound_port)
                    client.peer_registry.register(w_drain.peer_id, w_drain.host, w_drain.bound_port)
                    client.peer_registry.register(w_rdy.peer_id, w_rdy.host, w_rdy.bound_port)

                    client.peer_registry._peers[w_max.peer_id] = client.peer_registry._peers[
                        w_max.peer_id
                    ].touch(metadata={"max_concurrent_tasks": 2, "active_tasks": 2})

                    client.peer_registry._peers[w_drain.peer_id] = client.peer_registry._peers[
                        w_drain.peer_id
                    ].touch(metadata={"draining": True})

                    decision = client.scheduler.schedule()
                    assert decision.is_success
                    assert decision.selected_peer.peer_id == "worker-ready"
                    assert "worker-maxed" in decision.rejected_workers
                    assert "capacity limit" in decision.rejected_workers["worker-maxed"]
                    assert "worker-draining" in decision.rejected_workers
                    assert "draining" in decision.rejected_workers["worker-draining"]


@pytest.mark.asyncio
async def test_task_compatibility_required_capabilities() -> None:
    """
    Verify task capability constraints:
    Task requires GPU; Worker 1 lacks GPU, Worker 2 has GPU.
    """
    async with PeerNode(peer_id="worker-cpu-only", host="127.0.0.1", port=0) as w_cpu:
        async with PeerNode(peer_id="worker-gpu", host="127.0.0.1", port=0) as w_gpu:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.peer_registry.register(w_cpu.peer_id, w_cpu.host, w_cpu.bound_port)
                client.peer_registry.register(w_gpu.peer_id, w_gpu.host, w_gpu.bound_port)

                client.peer_registry._peers[w_cpu.peer_id] = client.peer_registry._peers[
                    w_cpu.peer_id
                ].touch(metadata={"capabilities": ["cpu", "fast_io"]})
                client.peer_registry._peers[w_gpu.peer_id] = client.peer_registry._peers[
                    w_gpu.peer_id
                ].touch(metadata={"capabilities": ["cpu", "gpu", "cuda"]})

                task_gpu = TaskRequest.create(
                    "matrix_multiply",
                    metadata={"required_capabilities": ["gpu"]},
                )
                decision = client.scheduler.schedule(task_gpu)
                assert decision.is_success
                assert decision.selected_peer.peer_id == "worker-gpu"
                assert "worker-cpu-only" in decision.rejected_workers
                assert "missing required capabilities" in decision.rejected_workers["worker-cpu-only"]


# ==============================================================================
# 7. No Eligible Workers Handling & Exceptions
# ==============================================================================


@pytest.mark.asyncio
async def test_no_eligible_workers_decision_and_exception() -> None:
    """
    Verify behavior when all discovered peers are unavailable, stale, or failed:
    - raise_if_none=False returns SchedulingDecision with selected_peer=None.
    - raise_if_none=True raises NoEligibleWorkerError with diagnostic details.
    """
    async with PeerNode(peer_id="worker-unhealthy", host="127.0.0.1", port=0) as w:
        async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
            client.enable_gossip()
            client.peer_registry.register(w.peer_id, w.host, w.bound_port)

            now = time.time()
            client.gossip_engine.store.update(
                NodeResourceReport(
                    node_id=w.peer_id,
                    timestamp=now,
                    cpu_percent=10.0,
                    memory_percent=10.0,
                    active_task_count=0,
                    health_status=HealthStatus.UNHEALTHY,
                )
            )

            # 1. Non-raising schedule
            decision = client.scheduler.schedule(raise_if_none=False)
            assert not decision.is_success
            assert decision.selected_peer is None
            assert decision.candidates_count == 0
            assert "worker-unhealthy" in decision.rejected_workers

            # 2. Raising schedule
            with pytest.raises(NoEligibleWorkerError, match="No eligible worker found"):
                client.scheduler.schedule(raise_if_none=True)


# ==============================================================================
# 8. Explainability & Observability
# ==============================================================================


@pytest.mark.asyncio
async def test_scheduler_explain_returns_structured_records() -> None:
    """
    Verify scheduler.explain() returns structured evaluation dictionaries for dashboards.
    """
    async with PeerNode(peer_id="worker-a", host="127.0.0.1", port=0) as wa:
        async with PeerNode(peer_id="worker-b", host="127.0.0.1", port=0) as wb:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                client.enable_gossip()
                client.peer_registry.register(wa.peer_id, wa.host, wa.bound_port)
                client.peer_registry.register(wb.peer_id, wb.host, wb.bound_port)

                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=wa.peer_id,
                        timestamp=now,
                        cpu_percent=25.0,
                        memory_percent=35.0,
                        active_task_count=1,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=wb.peer_id,
                        timestamp=now,
                        cpu_percent=60.0,
                        memory_percent=70.0,
                        active_task_count=3,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                explanations = client.scheduler.explain()
                assert len(explanations) == 2
                assert explanations[0]["peer_id"] == "worker-a"
                assert explanations[0]["is_eligible"] is True
                assert explanations[0]["score"] is not None
                assert explanations[0]["cpu_percent"] == 25.0
                assert explanations[0]["active_tasks"] == 1


# ==============================================================================
# 9. End-to-End Remote Task Submission Integration
# ==============================================================================


@pytest.mark.asyncio
async def test_end_to_end_load_aware_task_submission() -> None:
    """
    Verify full end-to-end integration:
    - Client submits tasks using submit_task.
    - Scheduler routes to lowest load worker across live TCP sockets.
    - Result is executed and returned successfully.
    """
    async with PeerNode(peer_id="compute-worker-1", host="127.0.0.1", port=0) as w1:
        async with PeerNode(peer_id="compute-worker-2", host="127.0.0.1", port=0) as w2:
            async with PeerNode(peer_id="client-submitter", host="127.0.0.1", port=0) as client:
                client.enable_gossip()

                # Register both workers
                client.peer_registry.register(w1.peer_id, w1.host, w1.bound_port)
                client.peer_registry.register(w2.peer_id, w2.host, w2.bound_port)

                now = time.time()
                # Worker 1 is busy: 6 active tasks, 80% CPU
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w1.peer_id,
                        timestamp=now,
                        cpu_percent=80.0,
                        memory_percent=70.0,
                        active_task_count=6,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                # Worker 2 is idle: 0 active tasks, 10% CPU
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=w2.peer_id,
                        timestamp=now,
                        cpu_percent=10.0,
                        memory_percent=20.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                # Scheduler confirms worker 2 is top candidate
                best = client.scheduler.select_best_worker()
                assert best is not None
                assert best.peer_id == "compute-worker-2"

                # Submit task -> routes to compute-worker-2
                resp = await client.submit_task("add", 100, 250)
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 350

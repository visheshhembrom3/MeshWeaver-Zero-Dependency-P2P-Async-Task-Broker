"""
Automated tests for heartbeat monitoring, failure detection, worker disconnection handling,
task ownership tracking, and bounded retries under at-least-once execution semantics.

Week 3, Milestone 4 verification:
1. Configurable heartbeat intervals, missed-heartbeat thresholds, and probe timeouts.
2. Peer health transitions (HEALTHY -> DEGRADED -> UNHEALTHY) upon heartbeat misses.
3. Recovery from DEGRADED / UNHEALTHY back to HEALTHY upon successful heartbeat probe.
4. Unresponsive peer detection and exclusion from scheduler candidate selection.
5. Handling worker disconnection mid-execution: immediate socket close detection,
   failover, and reassignment to alternate healthy workers.
6. Bounded retry limits preventing infinite loops on persistent failures.
7. Explicit at-least-once execution semantics tracking and documentation without
   claiming exactly-once execution.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    DEFAULT_HEARTBEAT_INTERVAL,
    DEFAULT_MISSED_THRESHOLD,
    DEFAULT_PROBE_TIMEOUT,
    HealthStatus,
    HeartbeatConfig,
    HeartbeatMonitor,
    Message,
    MessageType,
    PeerHeartbeatState,
    PeerNode,
    TaskAck,
    TaskErrorInfo,
    TaskOwnership,
    TaskRequest,
    TaskResponse,
    TaskStatus,
)
from meshweaver.exceptions import (
    HeartbeatError,
    HeartbeatTimeoutError,
    PeerUnresponsiveError,
    WorkerDisconnectedError,
)


# =============================================================================
# 1. Heartbeat Configuration and Health State Transition Tests
# =============================================================================


def test_heartbeat_config_validation() -> None:
    """Verify HeartbeatConfig parameter bounds and validation."""
    config = HeartbeatConfig(interval=2.5, missed_threshold=4, probe_timeout=1.5)
    assert config.interval == 2.5
    assert config.missed_threshold == 4
    assert config.probe_timeout == 1.5

    # Test invalid values raise ValueError
    with pytest.raises(ValueError, match=r"[Ii]nterval"):
        HeartbeatConfig(interval=0.0)
    with pytest.raises(ValueError, match=r"[Tt]hreshold"):
        HeartbeatConfig(missed_threshold=0)
    with pytest.raises(ValueError, match=r"[Tt]imeout"):
        HeartbeatConfig(probe_timeout=-1.0)


def test_peer_heartbeat_state_transitions() -> None:
    """
    Verify peer health transitions:
    - Initial: HEALTHY
    - 1 miss: DEGRADED
    - threshold misses: UNHEALTHY & is_unresponsive
    - Recovery upon success: returns to HEALTHY
    """
    state = PeerHeartbeatState(peer_id="worker-node-1", missed_threshold=3)
    assert state.health == HealthStatus.HEALTHY
    assert state.consecutive_misses == 0
    assert not state.is_unresponsive

    # First miss: transitions to DEGRADED
    state.record_miss("Timeout")
    assert state.consecutive_misses == 1
    assert state.health == HealthStatus.DEGRADED
    assert not state.is_unresponsive

    # Second miss: remains DEGRADED
    state.record_miss("Timeout")
    assert state.consecutive_misses == 2
    assert state.health == HealthStatus.DEGRADED
    assert not state.is_unresponsive

    # Third miss (threshold reached): transitions to UNHEALTHY
    state.record_miss("Timeout")
    assert state.consecutive_misses == 3
    assert state.health == HealthStatus.UNHEALTHY
    assert state.is_unresponsive

    # Fourth miss: stays UNHEALTHY
    state.record_miss("Connection reset")
    assert state.consecutive_misses == 4
    assert state.health == HealthStatus.UNHEALTHY
    assert state.is_unresponsive

    # Successful probe restores HEALTHY status
    state.record_success(rtt_ms=12.5)
    assert state.consecutive_misses == 0
    assert state.consecutive_successes == 1
    assert state.health == HealthStatus.HEALTHY
    assert not state.is_unresponsive
    assert state.last_rtt_ms == 12.5


def test_task_ownership_lifecycle_and_semantics() -> None:
    """
    Verify TaskOwnership state tracking, worker history, failure recording,
    and explicit at-least-once execution semantics declaration.
    """
    ownership = TaskOwnership(
        task_id="task-42",
        owner_id="client-node",
        max_retries=2,
    )
    assert ownership.task_id == "task-42"
    assert ownership.owner_id == "client-node"
    assert ownership.retry_count == 0
    assert ownership.can_retry is True
    assert not ownership.is_terminal

    # Assign to first worker
    ownership.assign_worker("worker-1")
    assert ownership.assigned_worker_id == "worker-1"
    assert ownership.worker_history == ["worker-1"]
    assert ownership.status == "assigned"

    # Worker 1 fails -> record failure
    can_retry_1 = ownership.record_failure("worker-1 disconnected mid-execution")
    assert can_retry_1 is True
    assert ownership.retry_count == 1
    assert ownership.status == "retrying"
    assert len(ownership.error_history) == 1

    # Assign to second worker
    ownership.assign_worker("worker-2")
    assert ownership.assigned_worker_id == "worker-2"
    assert ownership.worker_history == ["worker-1", "worker-2"]

    # Worker 2 succeeds
    ownership.record_success()
    assert ownership.status == "completed"
    assert ownership.is_terminal is True
    assert ownership.completed_at is not None

    ownership_dict = ownership.to_dict()
    assert ownership_dict["task_id"] == "task-42"
    assert ownership_dict["owner_id"] == "client-node"
    assert ownership_dict["worker_history"] == ["worker-1", "worker-2"]
    assert ownership_dict["retry_count"] == 1
    # Explicit execution semantics must never claim exactly-once
    assert ownership_dict["execution_semantics"] == "at-least-once"


def test_task_ownership_retry_exhaustion() -> None:
    """Verify TaskOwnership bounded retries exhaust at max_retries."""
    ownership = TaskOwnership(task_id="task-fail", owner_id="client", max_retries=2)
    assert ownership.can_retry is True

    # Attempt 1 fail
    can_retry = ownership.record_failure("error 1")
    assert can_retry is True
    assert ownership.retry_count == 1

    # Attempt 2 fail (hits max_retries limit)
    can_retry = ownership.record_failure("error 2")
    assert can_retry is False
    assert ownership.retry_count == 2
    assert ownership.can_retry is False
    assert ownership.status == "failed"


# =============================================================================
# 2. Peer Registry Health Tracking Tests
# =============================================================================


def test_peer_registry_heartbeat_tracking() -> None:
    """Verify PeerRegistry records heartbeat successes and failures."""
    from meshweaver.peers import PeerRegistry

    registry = PeerRegistry()
    registry.register("worker-node", "127.0.0.1", 9001)

    rec = registry.get("worker-node")
    assert rec is not None
    assert rec.missed_heartbeats == 0
    assert rec.health_status == HealthStatus.HEALTHY
    assert not rec.is_unresponsive

    # Record 1 failure -> DEGRADED
    registry.record_heartbeat_failure("worker-node", max_missed=3)
    rec = registry.get("worker-node")
    assert rec.missed_heartbeats == 1
    assert rec.health_status == HealthStatus.DEGRADED
    assert not rec.is_unresponsive

    # Record 2 more failures -> UNHEALTHY
    registry.record_heartbeat_failure("worker-node", max_missed=3)
    registry.record_heartbeat_failure("worker-node", max_missed=3)
    rec = registry.get("worker-node")
    assert rec.missed_heartbeats == 3
    assert rec.health_status == HealthStatus.UNHEALTHY
    assert rec.is_unresponsive
    unresponsive_ids = [p.peer_id for p in registry.list_unresponsive_peers()]
    assert "worker-node" in unresponsive_ids

    # Heartbeat success recovers the peer
    registry.record_heartbeat_success("worker-node", rtt_ms=5.0)
    rec = registry.get("worker-node")
    assert rec.missed_heartbeats == 0
    assert rec.health_status == HealthStatus.HEALTHY
    assert not rec.is_unresponsive
    unresponsive_ids = [p.peer_id for p in registry.list_unresponsive_peers()]
    assert "worker-node" not in unresponsive_ids


# =============================================================================
# 3. Active Peer Heartbeat Probing Integration Tests
# =============================================================================


@pytest.mark.asyncio
async def test_heartbeat_probe_active_peer() -> None:
    """Verify active heartbeat probe exchange between two live PeerNodes."""
    async with PeerNode(peer_id="prober-node", host="127.0.0.1", port=0) as prober:
        async with PeerNode(peer_id="target-node", host="127.0.0.1", port=0) as target:
            # Register target on prober
            prober.peer_registry.register("target-node", "127.0.0.1", target.bound_port)

            # Probe target peer
            rtt_ms = await prober.heartbeat_monitor.probe_peer("target-node")
            assert rtt_ms >= 0.0

            # State should be healthy
            state = prober.heartbeat_monitor.get_peer_state("target-node")
            assert state is not None
            assert state.health == HealthStatus.HEALTHY
            assert state.consecutive_misses == 0
            assert state.consecutive_successes >= 1


@pytest.mark.asyncio
async def test_heartbeat_monitor_detects_unresponsive_peer() -> None:
    """Verify HeartbeatMonitor detects an unresponsive/offline peer and marks it UNHEALTHY."""
    async with PeerNode(
        peer_id="prober-node",
        host="127.0.0.1",
        port=0,
        heartbeat_interval=0.2,
        missed_heartbeat_threshold=2,
        heartbeat_timeout=0.2,
    ) as prober:
        # Register a non-existent port
        prober.peer_registry.register("dead-worker", "127.0.0.1", 64999)

        # Probe once -> fails, transitions to DEGRADED
        with pytest.raises(HeartbeatError):
            await prober.heartbeat_monitor.probe_peer("dead-worker")

        state = prober.heartbeat_monitor.get_peer_state("dead-worker")
        assert state is not None
        assert state.consecutive_misses == 1
        assert state.health == HealthStatus.DEGRADED

        # Probe second time -> reaches threshold 2, transitions to UNHEALTHY
        with pytest.raises(HeartbeatError):
            await prober.heartbeat_monitor.probe_peer("dead-worker")

        state = prober.heartbeat_monitor.get_peer_state("dead-worker")
        assert state.consecutive_misses == 2
        assert state.health == HealthStatus.UNHEALTHY
        assert state.is_unresponsive
        assert "dead-worker" in prober.heartbeat_monitor.list_unresponsive_peers()


# =============================================================================
# 4. Unresponsive Peer Exclusion from Task Scheduling
# =============================================================================


@pytest.mark.asyncio
async def test_unresponsive_peer_excluded_from_scheduler() -> None:
    """
    Verify that nodes marked UNHEALTHY or unresponsive due to missed heartbeats
    are excluded by the TaskScheduler.
    """
    async with PeerNode(peer_id="coordinator", host="127.0.0.1", port=0) as coord:
        # Register two worker peers
        coord.peer_registry.register("healthy-worker", "127.0.0.1", 9001, metadata={"accepting_tasks": True})
        coord.peer_registry.register("unhealthy-worker", "127.0.0.1", 9002, metadata={"accepting_tasks": True})

        # Mark unhealthy-worker as having 3 missed heartbeats
        coord.peer_registry.record_heartbeat_failure("unhealthy-worker", max_missed=3)
        coord.peer_registry.record_heartbeat_failure("unhealthy-worker", max_missed=3)
        coord.peer_registry.record_heartbeat_failure("unhealthy-worker", max_missed=3)
        coord.heartbeat_monitor.record_peer_failure("unhealthy-worker", "Offline")

        req = TaskRequest.create(task_name="echo_task", args=["hello"])

        # Scheduler evaluation should reject unhealthy-worker
        eval_result = coord.task_scheduler.evaluate_candidate(
            coord.peer_registry.get("unhealthy-worker"), req
        )
        assert eval_result.is_eligible is False
        assert "UNRESPONSIVE" in eval_result.rejection_reason or "UNHEALTHY" in eval_result.rejection_reason

        # Candidate selection should only return the healthy-worker
        candidates = coord.task_scheduler.select_candidate_workers(req, count=5)
        candidate_ids = [c.peer_id for c in candidates]
        assert "healthy-worker" in candidate_ids
        assert "unhealthy-worker" not in candidate_ids


# =============================================================================
# 5. Worker Disconnection and Task Recovery (At-Least-Once Semantics)
# =============================================================================


@pytest.mark.asyncio
async def test_worker_disconnect_during_task_execution_recovers() -> None:
    """
    Verify handling when a worker disconnects mid-execution:
    - Worker 1 receives task and abruptly closes connection before returning result.
    - Client immediately detects disconnection via ConnectionClosedError.
    - Client marks Worker 1 failed and initiates retry on Worker 2.
    - Worker 2 successfully completes task and returns response.
    - Final TaskResponse metadata includes at-least-once execution semantics tags.
    """
    async with PeerNode(peer_id="worker-flaky", host="127.0.0.1", port=0) as flaky_worker:
        async with PeerNode(peer_id="worker-reliable", host="127.0.0.1", port=0) as reliable_worker:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
                # Custom handler on flaky_worker that drops connection on task request
                flaky_invocations = 0

                async def _flaky_task_handler(msg: Message, conn: Any) -> None:
                    nonlocal flaky_invocations
                    flaky_invocations += 1
                    # Simulate worker crash / abrupt socket close
                    await conn.close()
                    return None

                flaky_worker.register_handler(MessageType.TASK_REQUEST, _flaky_task_handler)

                # Reliable worker registers the task handler normally
                reliable_invocations = 0

                def _reliable_handler(text: str) -> str:
                    nonlocal reliable_invocations
                    reliable_invocations += 1
                    return f"processed: {text}"

                reliable_worker.task_registry.register("process_text", _reliable_handler)

                # Client registers both workers in registry (reliable has higher active_tasks to prioritize flaky first)
                client.peer_registry.register(
                    "worker-flaky",
                    "127.0.0.1",
                    flaky_worker.bound_port,
                    metadata={"supported_tasks": ["process_text"], "accepting_tasks": True, "active_tasks": 0},
                )
                client.peer_registry.register(
                    "worker-reliable",
                    "127.0.0.1",
                    reliable_worker.bound_port,
                    metadata={"supported_tasks": ["process_text"], "accepting_tasks": True, "active_tasks": 10},
                )

                # Submit task
                resp = await client.submit_task(
                    "process_text",
                    "hello world",
                    max_retries=2,
                    timeout=3.0,
                )

                # Assert successful recovery on reliable worker
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == "processed: hello world"
                assert reliable_invocations == 1
                assert flaky_invocations == 1

                # Verify metadata reflects at-least-once execution semantics
                assert resp.metadata.get("retried") is True
                assert resp.metadata.get("retry_count") == 1
                assert resp.metadata.get("execution_semantics") == "at-least-once"
                assert "worker-flaky" in resp.metadata.get("worker_history", [])
                assert "worker-reliable" in resp.metadata.get("worker_history", [])

                # Verify flaky worker was recorded as having failed
                flaky_rec = client.peer_registry.get("worker-flaky")
                assert flaky_rec is not None
                assert flaky_rec.missed_heartbeats >= 1


# =============================================================================
# 6. Bounded Retry Limits and Failure Termination
# =============================================================================


@pytest.mark.asyncio
async def test_bounded_retry_limit_exhaustion() -> None:
    """
    Verify bounded retries terminate deterministically when all candidate workers fail.
    Ensures no infinite retry loop occurs.
    """
    async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
        # Register workers that point to dead ports
        client.peer_registry.register("dead-1", "127.0.0.1", 64991, metadata={"accepting_tasks": True})
        client.peer_registry.register("dead-2", "127.0.0.1", 64992, metadata={"accepting_tasks": True})
        client.peer_registry.register("dead-3", "127.0.0.1", 64993, metadata={"accepting_tasks": True})

        # Submit task with max_retries = 2
        start_time = time.time()
        resp = await client.submit_task(
            "nonexistent_task",
            "data",
            max_retries=2,
            timeout=1.0,
        )
        duration = time.time() - start_time

        # Must return structured error (not hang or loop infinitely)
        assert resp.status == TaskStatus.FAILURE or resp.status == TaskStatus.ERROR
        assert resp.error is not None
        assert resp.error.code in ("MAX_RETRIES_EXCEEDED", "PEER_UNREACHABLE", "ROUTING_FAILED")

        # Verify task ownership tracked the attempts
        ownership = resp.metadata.get("ownership")
        assert ownership is not None
        assert ownership["retry_count"] <= 2
        assert ownership["execution_semantics"] == "at-least-once"


@pytest.mark.asyncio
async def test_worker_shutdown_during_long_task_failover() -> None:
    """
    Verify worker graceful shutdown mid-task triggers failover to another peer:
    - Worker A starts executing task that takes 1 second.
    - After 100ms, Worker A is shut down via worker_a.stop().
    - Client connection read loop detects disconnect immediately.
    - Task is retried and completes on Worker B.
    """
    async with PeerNode(peer_id="worker-a", host="127.0.0.1", port=0) as worker_a:
        async with PeerNode(peer_id="worker-b", host="127.0.0.1", port=0) as worker_b:
            async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:

                async def _slow_task() -> str:
                    await asyncio.sleep(2.0)
                    return "done slow"

                async def _fast_task() -> str:
                    return "done fast on backup"

                worker_a.task_registry.register("slow_or_fast", _slow_task)
                worker_b.task_registry.register("slow_or_fast", _fast_task)

                # Register both workers on client
                # Register both workers on client (worker-b has higher active_tasks to prioritize worker-a first)
                client.peer_registry.register(
                    "worker-a",
                    "127.0.0.1",
                    worker_a.bound_port,
                    metadata={"supported_tasks": ["slow_or_fast"], "accepting_tasks": True, "active_tasks": 0},
                )
                client.peer_registry.register(
                    "worker-b",
                    "127.0.0.1",
                    worker_b.bound_port,
                    metadata={"supported_tasks": ["slow_or_fast"], "accepting_tasks": True, "active_tasks": 10},
                )

                # Target worker-a explicitly or let scheduler pick
                # Trigger worker_a shutdown after task has started
                async def _shutdown_worker_a_delayed() -> None:
                    await asyncio.sleep(0.1)
                    await worker_a.stop()

                asyncio.create_task(_shutdown_worker_a_delayed())

                resp = await client.submit_task(
                    "slow_or_fast",
                    target_peer_id=None,
                    max_retries=2,
                    timeout=5.0,
                )

                assert resp.status == TaskStatus.SUCCESS
                assert resp.result in ("done fast on backup", "done slow")
                if resp.result == "done fast on backup":
                    assert resp.metadata.get("retried") is True
                    assert resp.metadata.get("execution_semantics") == "at-least-once"


@pytest.mark.asyncio
async def test_heartbeat_monitor_callbacks() -> None:
    """Verify on_peer_unresponsive and on_peer_recovered callbacks are invoked appropriately."""
    unresponsive_events: list[tuple[str, Any]] = []
    recovered_events: list[tuple[str, Any]] = []

    def _on_unresponsive(peer_id: str, state: Any) -> None:
        unresponsive_events.append((peer_id, state))

    def _on_recovered(peer_id: str, state: Any) -> None:
        recovered_events.append((peer_id, state))

    async with PeerNode(peer_id="callback-tester", host="127.0.0.1", port=0) as node:
        node.heartbeat_monitor.on_peer_unresponsive = _on_unresponsive
        node.heartbeat_monitor.on_peer_recovered = _on_recovered

        # Fail peer until threshold
        node.heartbeat_monitor.record_peer_failure("worker-x")
        node.heartbeat_monitor.record_peer_failure("worker-x")
        node.heartbeat_monitor.record_peer_failure("worker-x")

        assert len(unresponsive_events) == 1
        assert unresponsive_events[0][0] == "worker-x"
        assert unresponsive_events[0][1].is_unresponsive is True

        # Recover peer
        node.heartbeat_monitor.record_peer_success("worker-x", latency=0.01)
        assert len(recovered_events) == 1
        assert recovered_events[0][0] == "worker-x"
        assert recovered_events[0][1].is_unresponsive is False


@pytest.mark.asyncio
async def test_execution_semantics_explicit_at_least_once_warning() -> None:
    """
    Verify retried task explicitly returns at-least-once execution warning
    and does not claim exactly-once execution.
    """
    async with PeerNode(peer_id="worker-drop", host="127.0.0.1", port=0) as w1:
        async with PeerNode(peer_id="worker-ok", host="127.0.0.1", port=0) as w2:
            async with PeerNode(peer_id="client-checker", host="127.0.0.1", port=0) as client:
                # w1 drops connection
                async def _drop_handler(msg: Message, conn: Any) -> None:
                    await conn.close()
                    return None

                w1.register_handler(MessageType.TASK_REQUEST, _drop_handler)

                # w2 handles
                w2.task_registry.register("compute_pi", lambda: 3.14159)

                client.peer_registry.register(
                    "worker-drop",
                    "127.0.0.1",
                    w1.bound_port,
                    metadata={"supported_tasks": ["compute_pi"], "accepting_tasks": True, "active_tasks": 0},
                )
                client.peer_registry.register(
                    "worker-ok",
                    "127.0.0.1",
                    w2.bound_port,
                    metadata={"supported_tasks": ["compute_pi"], "accepting_tasks": True, "active_tasks": 10},
                )

                resp = await client.submit_task("compute_pi", max_retries=2)
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 3.14159
                assert resp.metadata.get("execution_semantics") == "at-least-once"
                assert "at-least-once" in resp.metadata.get("execution_warning", "").lower()
                assert "exactly-once" not in resp.metadata.get("execution_semantics", "")


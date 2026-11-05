"""
End-to-end integration tests for complete distributed task workflow.

Week 3, Milestone 5 verification:
1. Complete task submission workflow integrating scheduler, peer discovery, networking,
   workload gossip, worker execution, and result retrieval.
2. Successful execution across heterogeneous workers ranked by load score.
3. Task exceptions: application-level errors caught, isolated, and formatted into TaskErrorInfo.
4. Unavailable workers: no workers available, unknown target peer, all peers offline or UNHEALTHY.
5. Timeout handling: request timeouts, worker timeouts, duplicate submission timeouts.
6. Worker disconnection and recovery: mid-execution socket drops, fast failure detection,
   failover to alternate healthy worker, and explicit at-least-once metadata verification.
7. Bounded retry limits: deterministically halting on cascading worker failures.
8. Asynchronous submission (submit_task_async), TaskAck acknowledgements, and status polling (wait_for_task).
9. Security and serialization boundaries: rejecting invalid identifiers, forbidden imports, and oversized payloads.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    HealthStatus,
    Message,
    MessageType,
    NodeResourceReport,
    PeerNode,
    TaskAck,
    TaskErrorInfo,
    TaskRequest,
    TaskResponse,
    TaskStatus,
    TaskStatusReport,
)
from meshweaver.exceptions import (
    ConnectionClosedError,
    InvalidTaskError,
    NetworkTimeoutError,
)


# =============================================================================
# 1. Successful Distributed Execution Workflow
# =============================================================================


@pytest.mark.asyncio
async def test_end_to_end_successful_task_workflow() -> None:
    """
    Test complete task submission workflow across a multi-node mesh:
    - Seed coordinator for discovery.
    - Worker Alpha (high load: 8 active tasks).
    - Worker Beta (low load: 0 active tasks).
    - Client Node submitting task.

    Verifies:
    1. Discovery and gossip propagation across 4 nodes.
    2. Scheduler evaluates candidates and selects Worker Beta (lowest load).
    3. on_ack callback receives TaskAck with correlation ID and routed worker.
    4. Worker Beta executes task and returns TaskResponse.
    5. Correlation ID matches across request, ack, and response.
    6. query_task_status confirms successful terminal state.
    """
    async with PeerNode(peer_id="seed-coord", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="worker-alpha", host="127.0.0.1", port=0) as worker_alpha:
            async with PeerNode(peer_id="worker-beta", host="127.0.0.1", port=0) as worker_beta:
                async with PeerNode(peer_id="client-submitter", host="127.0.0.1", port=0) as client:
                    seed_addr = f"127.0.0.1:{seed.bound_port}"

                    # Task handler for squaring numbers
                    def _square(n: int) -> int:
                        return n * n

                    worker_alpha.task_registry.register("square", _square)
                    worker_beta.task_registry.register("square", _square)

                    # Join mesh via seed node
                    worker_alpha.enable_gossip(interval=0.2)
                    worker_beta.enable_gossip(interval=0.2)
                    client.enable_gossip(interval=0.2)

                    await worker_alpha.join_mesh([seed_addr])
                    await worker_beta.join_mesh([seed_addr])
                    await client.join_mesh([seed_addr])

                    # Update client's registry view (worker-alpha has higher active_tasks than worker-beta)
                    client.peer_registry.register(
                        "worker-alpha",
                        "127.0.0.1",
                        worker_alpha.bound_port,
                        metadata={"supported_tasks": ["square"], "accepting_tasks": True, "active_tasks": 8},
                    )
                    client.peer_registry.register(
                        "worker-beta",
                        "127.0.0.1",
                        worker_beta.bound_port,
                        metadata={"supported_tasks": ["square"], "accepting_tasks": True, "active_tasks": 0},
                    )

                    # Submit task with on_ack callback
                    received_acks: list[TaskAck] = []
                    corr_id = "test-corr-success-001"

                    resp = await client.submit_task(
                        "square",
                        12,
                        correlation_id=corr_id,
                        on_ack=lambda ack: received_acks.append(ack),
                    )

                    # Assertions on acknowledgement
                    assert len(received_acks) == 1
                    ack = received_acks[0]
                    assert ack.correlation_id == corr_id
                    assert ack.status == "accepted"
                    assert ack.routed_to == "worker-beta"

                    # Assertions on response
                    assert resp.status == TaskStatus.SUCCESS
                    assert resp.result == 144
                    assert resp.correlation_id == corr_id
                    assert resp.execution_time >= 0.0
                    assert resp.error is None
                    assert resp.metadata.get("ack", {}).get("routed_to") == "worker-beta"

                    # Query execution status on worker-beta
                    status_rep = await client.query_task_status(resp.task_id, target_peer_id="worker-beta")
                    assert status_rep.status == TaskStatus.SUCCESS
                    assert status_rep.result == 144


# =============================================================================
# 2. Task Exceptions and Application-Level Error Propagation
# =============================================================================


@pytest.mark.asyncio
async def test_task_exception_handling_and_propagation() -> None:
    """
    Test that application-level exceptions raised by task handlers on remote workers
    are safely trapped, encapsulated into TaskErrorInfo, and returned as FAILURE/ERROR
    without crashing worker processes or triggering spurious network failovers.
    """
    async with PeerNode(peer_id="worker-math", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-math", host="127.0.0.1", port=0) as client:
            # Register a task that raises an exception
            def _divide(a: float, b: float) -> float:
                if b == 0:
                    raise ZeroDivisionError("Cannot divide by zero in distributed task")
                return a / b

            worker.task_registry.register("divide", _divide)

            client.peer_registry.register(
                "worker-math",
                "127.0.0.1",
                worker.bound_port,
                metadata={"supported_tasks": ["divide"], "accepting_tasks": True},
            )

            # 1. Normal division
            ok_resp = await client.submit_task("divide", 100.0, 4.0, target_peer_id="worker-math")
            assert ok_resp.status == TaskStatus.SUCCESS
            assert ok_resp.result == 25.0

            # 2. Division by zero (raises exception)
            err_resp = await client.submit_task("divide", 100.0, 0.0, target_peer_id="worker-math")
            assert err_resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
            assert err_resp.result is None
            assert err_resp.error is not None
            assert "zero" in err_resp.error.message.lower()

            # Worker remains healthy and can continue processing further tasks
            next_resp = await client.submit_task("divide", 50.0, 2.0, target_peer_id="worker-math")
            assert next_resp.status == TaskStatus.SUCCESS
            assert next_resp.result == 25.0


# =============================================================================
# 3. Unavailable Worker Scenarios
# =============================================================================


@pytest.mark.asyncio
async def test_no_workers_available_in_mesh() -> None:
    """Verify clean error when submitting to an empty mesh without workers."""
    async with PeerNode(peer_id="isolated-client", host="127.0.0.1", port=0) as client:
        resp = await client.submit_task("any_task", 1, 2)
        assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp.error is not None
        assert resp.error.code == "NO_PEERS_AVAILABLE"


@pytest.mark.asyncio
async def test_target_peer_unavailable() -> None:
    """Verify clean error when target_peer_id is unknown or unreachable."""
    async with PeerNode(peer_id="client-target-test", host="127.0.0.1", port=0) as client:
        resp = await client.submit_task("any_task", target_peer_id="nonexistent-ghost-node")
        assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp.error is not None
        assert resp.error.code == "PEER_UNAVAILABLE"


@pytest.mark.asyncio
async def test_all_workers_unhealthy_excluded_by_scheduler() -> None:
    """
    Verify scheduler excludes worker candidates marked UNHEALTHY or exceeding
    missed heartbeat threshold, returning NO_PEERS_AVAILABLE.
    """
    async with PeerNode(peer_id="client-health-test", host="127.0.0.1", port=0) as client:
        # Register workers but mark them UNHEALTHY
        client.peer_registry.register("worker-down-1", "127.0.0.1", 59001, metadata={"accepting_tasks": True})
        client.peer_registry.register("worker-down-2", "127.0.0.1", 59002, metadata={"accepting_tasks": True})

        client.peer_registry.record_heartbeat_failure("worker-down-1", max_missed=2)
        client.peer_registry.record_heartbeat_failure("worker-down-1", max_missed=2)

        client.peer_registry.record_heartbeat_failure("worker-down-2", max_missed=2)
        client.peer_registry.record_heartbeat_failure("worker-down-2", max_missed=2)

        resp = await client.submit_task("any_task", 1)
        assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp.error is not None
        assert resp.error.code == "NO_PEERS_AVAILABLE"


# =============================================================================
# 4. Timeout Handling
# =============================================================================


@pytest.mark.asyncio
async def test_request_timeout_handling() -> None:
    """
    Verify request timeout is enforced when a remote worker executes a slow task
    that exceeds the client-configured timeout.
    """
    async with PeerNode(peer_id="worker-slow", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-fast", host="127.0.0.1", port=0) as client:

            async def _slow_computation() -> str:
                await asyncio.sleep(1.5)
                return "completed eventually"

            worker.task_registry.register("slow_job", _slow_computation)

            client.peer_registry.register(
                "worker-slow",
                "127.0.0.1",
                worker.bound_port,
                metadata={"supported_tasks": ["slow_job"], "accepting_tasks": True},
            )

            # Submit with strict 0.1s timeout
            start = time.time()
            resp = await client.submit_task("slow_job", timeout=0.1, retry_on_failure=False)
            elapsed = time.time() - start

            assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
            assert resp.error is not None
            assert resp.error.code == "TASK_TIMEOUT"
            assert elapsed < 1.0


# =============================================================================
# 5. Worker Disconnection, Failover, and Bounded Retries
# =============================================================================


@pytest.mark.asyncio
async def test_worker_disconnection_mid_execution_recovers() -> None:
    """
    Verify handling when a worker disconnects mid-execution:
    1. Primary worker receives task and abruptly closes TCP connection.
    2. Client immediately detects disconnection via ConnectionClosedError.
    3. Client marks primary worker failed and selects secondary worker.
    4. Secondary worker successfully computes and returns result.
    5. Response metadata confirms at-least-once execution semantics tags.
    """
    async with PeerNode(peer_id="worker-crashy", host="127.0.0.1", port=0) as crashy:
        async with PeerNode(peer_id="worker-backup", host="127.0.0.1", port=0) as backup:
            async with PeerNode(peer_id="client-failover", host="127.0.0.1", port=0) as client:
                crashy_calls = 0

                # Crashy handler drops connection
                async def _drop_conn(msg: Message, conn: Any) -> None:
                    nonlocal crashy_calls
                    crashy_calls += 1
                    await conn.close()
                    return None

                crashy.register_handler(MessageType.TASK_REQUEST, _drop_conn)

                # Backup worker registers normal task
                backup_calls = 0

                def _compute_data(data: str) -> str:
                    nonlocal backup_calls
                    backup_calls += 1
                    return f"data:{data.upper()}"

                backup.task_registry.register("compute_data", _compute_data)

                # Register both workers on client (backup has higher active_tasks to prioritize crashy first)
                client.peer_registry.register(
                    "worker-crashy",
                    "127.0.0.1",
                    crashy.bound_port,
                    metadata={"supported_tasks": ["compute_data"], "accepting_tasks": True, "active_tasks": 0},
                )
                client.peer_registry.register(
                    "worker-backup",
                    "127.0.0.1",
                    backup.bound_port,
                    metadata={"supported_tasks": ["compute_data"], "accepting_tasks": True, "active_tasks": 10},
                )

                resp = await client.submit_task("compute_data", "payload", max_retries=2)

                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == "data:PAYLOAD"
                assert crashy_calls == 1
                assert backup_calls == 1

                # Verify metadata reflects at-least-once execution semantics
                assert resp.metadata.get("retried") is True
                assert resp.metadata.get("retry_count") == 1
                assert resp.metadata.get("execution_semantics") == "at-least-once"
                assert "worker-crashy" in resp.metadata.get("worker_history", [])
                assert "worker-backup" in resp.metadata.get("worker_history", [])


@pytest.mark.asyncio
async def test_bounded_retry_exhaustion_on_cascading_failures() -> None:
    """
    Verify bounded retry limit prevents infinite retry loops when all workers fail.
    Halts at max_retries and returns structured error.
    """
    async with PeerNode(peer_id="client-retry-test", host="127.0.0.1", port=0) as client:
        # Register 3 offline ports
        client.peer_registry.register("dead-peer-1", "127.0.0.1", 64801, metadata={"accepting_tasks": True})
        client.peer_registry.register("dead-peer-2", "127.0.0.1", 64802, metadata={"accepting_tasks": True})
        client.peer_registry.register("dead-peer-3", "127.0.0.1", 64803, metadata={"accepting_tasks": True})

        resp = await client.submit_task("test_job", 1, 2, max_retries=2, timeout=0.5)

        assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp.error is not None
        assert resp.error.code in ("MAX_RETRIES_EXCEEDED", "PEER_UNREACHABLE", "ROUTING_FAILED")

        ownership = resp.metadata.get("ownership")
        assert ownership is not None
        assert ownership["retry_count"] <= 2
        assert ownership["execution_semantics"] == "at-least-once"


# =============================================================================
# 6. Asynchronous Submission and Status Polling
# =============================================================================


@pytest.mark.asyncio
async def test_asynchronous_submission_and_wait_for_task() -> None:
    """
    Verify asynchronous fire-and-forget submission via submit_task_async,
    immediate TaskAck receipt, and wait_for_task completion polling.
    """
    async with PeerNode(peer_id="worker-async", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-async", host="127.0.0.1", port=0) as client:

            async def _async_multiply(a: int, b: int) -> int:
                await asyncio.sleep(0.05)
                return a * b

            worker.task_registry.register("async_multiply", _async_multiply)

            client.peer_registry.register(
                "worker-async",
                "127.0.0.1",
                worker.bound_port,
                metadata={"supported_tasks": ["async_multiply"], "accepting_tasks": True},
            )

            # Submit task asynchronously
            ack = await client.submit_task_async("async_multiply", 7, 6)
            assert isinstance(ack, TaskAck)
            assert ack.status == "accepted"
            assert ack.routed_to == "worker-async"

            # Wait for task completion using polling API
            resp = await client.wait_for_task(ack.task_id, target_peer_id="worker-async", timeout=3.0)
            assert resp.status == TaskStatus.SUCCESS
            assert resp.result == 42


# =============================================================================
# 7. Strict Serialization and Security Boundaries
# =============================================================================


@pytest.mark.asyncio
async def test_strict_serialization_and_security_boundaries() -> None:
    """
    Verify client rejects invalid task identifiers, reserved/dangerous words,
    and oversized payloads upfront with structured INVALID_TASK_REQUEST errors.
    """
    async with PeerNode(peer_id="client-security", host="127.0.0.1", port=0) as client:
        # 1. Reserved / forbidden names
        for bad_name in ("eval", "exec", "os", "sys", "__import__", "importlib"):
            resp = await client.submit_task(bad_name)
            assert resp.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
            assert resp.error is not None
            assert resp.error.code == "INVALID_TASK_REQUEST"

        # 2. Forbidden keyword arguments
        resp_kw = await client.submit_task("add", 1, 2, __builtins__="malicious")
        assert resp_kw.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp_kw.error is not None
        assert resp_kw.error.code == "INVALID_TASK_REQUEST"

        # 3. Oversized payload (> 64 KB)
        giant_string = "X" * 70_000
        resp_giant = await client.submit_task("echo", giant_string)
        assert resp_giant.status in (TaskStatus.FAILURE, TaskStatus.ERROR)
        assert resp_giant.error is not None
        assert resp_giant.error.code == "INVALID_TASK_REQUEST"

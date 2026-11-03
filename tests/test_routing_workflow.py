"""
Automated integration tests for peer-to-peer task routing and submission workflow.

Week 3, Milestone 3 verification:
1. Complete task submission workflow integrating scheduler, peer discovery, networking, and worker execution.
2. Client submits task, receives structured TaskAck acknowledgement with correlation ID and routed peer.
3. Task is routed to an eligible peer selected by the load-aware scheduler.
4. Client obtains final TaskResponse result or structured error.
5. Correlation IDs match across submission, acknowledgement, and final response.
6. Request timeouts enforced on slow/hanging tasks with structured TASK_TIMEOUT errors.
7. Duplicate-submission handling: idempotent task submission, cached terminal results, and no duplicate execution.
8. Execution-status queries: querying real-time TaskStatusReport across peers.
9. Three-node end-to-end integration tests (Client -> Broker -> Worker, and Client -> Worker1/Worker2).
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


@pytest.mark.asyncio
async def test_three_node_end_to_end_routing_and_execution() -> None:
    """
    Verify complete task submission workflow across a three-node mesh:
    - Node 1: Client Node (submits task)
    - Node 2: Seed / Routing Node (bootstrap & discovery)
    - Node 3: Compute Worker Node (executes task via TaskWorker)

    Verifies:
    1. Client discovers Worker via Seed node.
    2. Client submits task and receives TaskAck acknowledgement.
    3. Task routed to eligible Worker peer using TaskScheduler.
    4. Worker executes task and returns TaskResponse.
    5. Correlation ID matches across submission and response.
    """
    async with PeerNode(peer_id="seed-coordinator", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="worker-compute", host="127.0.0.1", port=0) as worker:
            async with PeerNode(peer_id="client-submitter", host="127.0.0.1", port=0) as client:
                seed_addr = f"127.0.0.1:{seed.bound_port}"

                # Register custom compute task on worker
                execution_count = 0

                @worker.register_task("power")
                def compute_power(base: int, exp: int) -> int:
                    nonlocal execution_count
                    execution_count += 1
                    return base**exp

                # Join mesh via seed
                await worker.join_mesh([seed_addr], timeout=3.0)
                await client.join_mesh([seed_addr], timeout=3.0)
                await asyncio.sleep(0.1)

                assert client.peer_registry.has_peer("worker-compute")

                # Track acknowledgement via on_ack callback
                received_ack: list[TaskAck] = []

                def ack_callback(ack: TaskAck) -> None:
                    received_ack.append(ack)

                custom_corr_id = "corr-test-workflow-100"
                custom_task_id = "task-power-200"

                resp = await client.submit_task(
                    "power",
                    2,
                    10,
                    task_id=custom_task_id,
                    correlation_id=custom_corr_id,
                    on_ack=ack_callback,
                    timeout=5.0,
                )

                # 1. Verify acknowledgement
                assert len(received_ack) == 1
                ack = received_ack[0]
                assert ack.task_id == custom_task_id
                assert ack.correlation_id == custom_corr_id
                assert ack.is_accepted is True
                assert ack.routed_to == "worker-compute"
                assert ack.timestamp > 0

                # 2. Verify response
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 1024
                assert resp.task_id == custom_task_id
                assert resp.correlation_id == custom_corr_id
                assert resp.execution_time is not None
                assert resp.execution_time >= 0
                assert execution_count == 1

                # 3. Verify attached acknowledgement in response metadata
                assert resp.ack is not None
                assert resp.ack["task_id"] == custom_task_id


@pytest.mark.asyncio
async def test_scheduler_selects_lowest_load_peer_in_three_node_cluster() -> None:
    """
    Verify scheduler evaluates load across two worker nodes and routes to the lowest-load peer.
    - Worker A: 8 active tasks, 80% CPU
    - Worker B: 0 active tasks, 15% CPU
    Client scheduler should select Worker B.
    """
    async with PeerNode(peer_id="worker-heavy", host="127.0.0.1", port=0) as worker_heavy:
        async with PeerNode(peer_id="worker-light", host="127.0.0.1", port=0) as worker_light:
            async with PeerNode(peer_id="client-scheduler-test", host="127.0.0.1", port=0) as client:
                client.enable_gossip()

                # Register workers in client
                client.peer_registry.register(
                    worker_heavy.peer_id, worker_heavy.host, worker_heavy.bound_port
                )
                client.peer_registry.register(
                    worker_light.peer_id, worker_light.host, worker_light.bound_port
                )

                # Propagate gossip reports
                now = time.time()
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=worker_heavy.peer_id,
                        timestamp=now,
                        cpu_percent=80.0,
                        memory_percent=60.0,
                        active_task_count=8,
                        health_status=HealthStatus.HEALTHY,
                    )
                )
                client.gossip_engine.store.update(
                    NodeResourceReport(
                        node_id=worker_light.peer_id,
                        timestamp=now,
                        cpu_percent=15.0,
                        memory_percent=20.0,
                        active_task_count=0,
                        health_status=HealthStatus.HEALTHY,
                    )
                )

                # Submit task
                resp = await client.submit_task("add", 100, 200)
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 300
                assert resp.ack is not None
                assert resp.ack["routed_to"] == "worker-light"


@pytest.mark.asyncio
async def test_duplicate_submission_handling_idempotency() -> None:
    """
    Verify duplicate-submission handling:
    1. First submission routes and executes task.
    2. Second submission with identical task_id detects duplicate:
       - Returns cached result.
       - Does not re-execute the task handler (execution_count stays 1).
       - Response metadata includes duplicate=True.
    3. Asynchronous duplicate submission returns TaskAck with is_duplicate=True.
    """
    async with PeerNode(peer_id="worker-dedup", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-dedup", host="127.0.0.1", port=0) as client:
            execution_count = 0

            @worker.register_task("increment_counter")
            def increment_counter(amount: int) -> int:
                nonlocal execution_count
                execution_count += amount
                return execution_count

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            task_id = "idempotent-task-42"
            corr_id_1 = "corr-first-submission"

            # 1. First submission
            resp1 = await client.submit_task(
                "increment_counter",
                5,
                task_id=task_id,
                correlation_id=corr_id_1,
            )
            assert resp1.status == TaskStatus.SUCCESS
            assert resp1.result == 5
            assert execution_count == 5

            # 2. Duplicate submission with same task_id
            corr_id_2 = "corr-second-submission"
            resp2 = await client.submit_task(
                "increment_counter",
                5,
                task_id=task_id,
                correlation_id=corr_id_2,
            )
            assert resp2.status == TaskStatus.SUCCESS
            assert resp2.result == 5
            # Execution count was NOT incremented again!
            assert execution_count == 5
            # Metadata indicates duplicate
            assert resp2.metadata.get("duplicate") is True
            assert resp2.correlation_id == corr_id_2

            # 3. Asynchronous duplicate submission returns TaskAck with is_duplicate
            ack_dup = await client.submit_task_async(
                "increment_counter",
                5,
                task_id=task_id,
            )
            assert isinstance(ack_dup, TaskAck)
            assert ack_dup.is_duplicate is True
            assert ack_dup.task_id == task_id


@pytest.mark.asyncio
async def test_execution_status_queries_across_nodes() -> None:
    """
    Verify execution-status queries:
    1. Querying an uncompleted / running task returns PENDING or RUNNING status report.
    2. Querying a completed task returns SUCCESS report with result and execution time.
    3. Querying an unknown task returns TASK_NOT_FOUND error report.
    4. Status reports contain correlation ID and worker ID.
    """
    async with PeerNode(peer_id="worker-query-test", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-query-test", host="127.0.0.1", port=0) as client:
            @worker.register_task("slow_multiply")
            async def slow_multiply(a: int, b: int, delay: float) -> int:
                await asyncio.sleep(delay)
                return a * b

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            # 1. Submit task asynchronously
            task_id = "task-status-query-100"
            ack = await client.submit_task_async(
                "slow_multiply",
                6,
                7,
                0.2,
                task_id=task_id,
            )
            assert ack.is_accepted is True

            # 2. Query status while task is running
            report_running = await client.query_task_status(task_id, target_peer_id=worker.peer_id)
            assert isinstance(report_running, TaskStatusReport)
            assert report_running.task_id == task_id
            assert report_running.worker_peer_id == worker.peer_id

            # 3. Wait for completion and query terminal status
            final_resp = await client.wait_for_task(task_id, target_peer_id=worker.peer_id, timeout=3.0)
            assert final_resp.status == TaskStatus.SUCCESS
            assert final_resp.result == 42

            report_completed = await client.query_task_status(task_id, target_peer_id=worker.peer_id)
            assert report_completed.is_done is True
            assert report_completed.is_success is True
            assert report_completed.result == 42
            assert report_completed.execution_time is not None
            assert report_completed.execution_time >= 0.15

            # 4. Query unknown task ID
            report_unknown = await client.query_task_status("nonexistent-task-id", target_peer_id=worker.peer_id)
            assert report_unknown.status == TaskStatus.ERROR
            assert report_unknown.error is not None
            assert report_unknown.error.code == "TASK_NOT_FOUND"


@pytest.mark.asyncio
async def test_request_timeout_enforcement() -> None:
    """
    Verify request timeouts:
    - Task execution taking longer than the client's request timeout returns TASK_TIMEOUT.
    - System handles timeouts gracefully without crashing connection.
    """
    async with PeerNode(peer_id="worker-timeout-test", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-timeout-test", host="127.0.0.1", port=0) as client:
            @worker.register_task("sleeping_task")
            async def sleeping_task(sleep_sec: float) -> str:
                await asyncio.sleep(sleep_sec)
                return "woke_up"

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            # Submit task with 0.1s timeout while task takes 0.5s
            resp = await client.submit_task(
                "sleeping_task",
                0.5,
                timeout=0.1,
                retry_on_failure=False,
            )
            assert resp.status in (TaskStatus.ERROR, TaskStatus.FAILURE)
            assert resp.error is not None
            assert resp.error.code == "TASK_TIMEOUT"


@pytest.mark.asyncio
async def test_structured_error_propagation_for_unknown_task() -> None:
    """
    Verify structured error handling when submitting an unregistered task:
    - Returns TaskStatus.ERROR or FAILURE with code UNKNOWN_TASK.
    - Carries correlation ID and error message.
    """
    async with PeerNode(peer_id="worker-err-test", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-err-test", host="127.0.0.1", port=0) as client:
            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            corr_id = "corr-unknown-test"
            resp = await client.submit_task(
                "unregistered_function_call",
                1,
                2,
                correlation_id=corr_id,
            )
            assert resp.status in (TaskStatus.ERROR, TaskStatus.FAILURE)
            assert resp.error is not None
            assert resp.error.code == "UNKNOWN_TASK"
            assert resp.correlation_id == corr_id


@pytest.mark.asyncio
async def test_three_node_relay_broker_routing() -> None:
    """
    Verify broker relay routing:
    - Client submits task to Gateway/Broker node (Node B).
    - Gateway node acknowledges task, and routes task to Compute Worker (Node C).
    - Compute Worker executes task and returns result to Gateway.
    - Gateway returns result to Client.
    """
    async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
        async with PeerNode(peer_id="broker-gateway", host="127.0.0.1", port=0) as broker:
            async with PeerNode(peer_id="worker-target", host="127.0.0.1", port=0) as worker:
                # Custom task registered on Worker only (not on Broker or Client)
                @worker.register_task("cube")
                def compute_cube(x: int) -> int:
                    return x**3

                # Broker connects to Worker
                await broker.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)
                # Client connects to Broker
                await client.connect_to_peer(broker.host, broker.bound_port, timeout=3.0)

                # Client submits to Broker with relay metadata
                task_id = "relay-cube-100"
                corr_id = "corr-relay-100"
                resp = await client.submit_task(
                    "cube",
                    5,
                    target_peer_id=broker.peer_id,
                    task_id=task_id,
                    correlation_id=corr_id,
                    metadata={"relay": True},
                )

                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 125
                assert resp.task_id == task_id
                assert resp.correlation_id == corr_id

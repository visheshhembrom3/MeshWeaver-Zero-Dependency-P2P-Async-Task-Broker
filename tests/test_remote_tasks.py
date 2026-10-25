"""
Automated integration tests for end-to-end remote task execution and load-aware routing.

Verifies:
1. Client node submitting supported example tasks (add, reverse_string, echo, ping).
2. Routing tasks to discovered, reachable worker peers across a multi-node mesh.
3. Executing only supported, explicitly registered tasks (strict security boundary).
4. Returning task ID, execution status, and result or error details.
5. Handling unavailable peers, network disconnects, and invalid task requests gracefully.
6. Load-aware worker selection (prefers low-load worker over high-load worker).
7. Automatic failover to secondary candidate peers when the primary worker is unreachable.
8. Timeout enforcement on slow or hanging tasks.
9. Registration and remote invocation of custom task handlers.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    HealthStatus,
    LoadAwareRoutingStrategy,
    NoAvailablePeerError,
    NodeResourceReport,
    PeerNode,
    RouteDecision,
    TaskErrorInfo,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskRouter,
    TaskRoutingError,
    TaskStatus,
)


@pytest.mark.asyncio
async def test_end_to_end_remote_task_submission_success() -> None:
    """
    Verify client node submits supported example tasks to a discovered worker peer.
    Checks:
    - add(15, 27) -> 42
    - reverse_string("MeshWeaver") -> "revaeWhseM"
    - echo("arg1", key="value")
    - ping() -> "pong"
    """
    async with PeerNode(peer_id="worker-node-1", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-node-1", host="127.0.0.1", port=0) as client:
            # Client connects to and registers worker
            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)
            assert client.peer_registry.has_peer(worker.peer_id)

            # 1. Submit 'add'
            resp_add = await client.submit_task("add", 15, 27)
            assert resp_add.status == TaskStatus.SUCCESS
            assert resp_add.result == 42
            assert resp_add.task_id is not None
            assert resp_add.execution_time is not None
            assert resp_add.execution_time >= 0.0

            # 2. Submit 'reverse_string'
            resp_rev = await client.submit_task("reverse_string", "MeshWeaver")
            assert resp_rev.status == TaskStatus.SUCCESS
            assert resp_rev.result == "revaeWhseM"

            # 3. Submit 'echo'
            resp_echo = await client.submit_task("echo", "test_arg", mode="fast")
            assert resp_echo.status == TaskStatus.SUCCESS
            assert resp_echo.result == {
                "args": ["test_arg"],
                "kwargs": {"mode": "fast"},
            }

            # 4. Submit 'ping'
            resp_ping = await client.submit_task("ping")
            assert resp_ping.status == TaskStatus.SUCCESS
            assert resp_ping.result == "pong"


@pytest.mark.asyncio
async def test_multi_node_mesh_discovery_and_task_execution() -> None:
    """
    Verify task submission across a 3-node cluster:
    - Seed node acts as bootstrap
    - Worker joins seed
    - Client joins seed, discovers Worker, and routes task to Worker
    """
    async with PeerNode(peer_id="seed-node", host="127.0.0.1", port=0) as seed:
        async with PeerNode(peer_id="compute-worker", host="127.0.0.1", port=0) as worker:
            async with PeerNode(peer_id="submitter-client", host="127.0.0.1", port=0) as client:
                seed_addr = f"127.0.0.1:{seed.bound_port}"

                # Worker and client join mesh via seed
                await worker.join_mesh([seed_addr], timeout=3.0)
                await client.join_mesh([seed_addr], timeout=3.0)

                # Ensure client has discovered worker
                assert client.peer_registry.has_peer("compute-worker")

                # Submit task with custom task_id and metadata
                custom_id = "task-custom-999"
                resp = await client.submit_task(
                    "add",
                    500,
                    250,
                    task_id=custom_id,
                    metadata={"priority": "high"},
                )
                assert resp.task_id == custom_id
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 750


@pytest.mark.asyncio
async def test_load_aware_routing_selects_lowest_load_peer() -> None:
    """
    Verify TaskRouter selects the peer with the lowest composite load score.
    Worker 1 has high reported active tasks / CPU.
    Worker 2 has low reported active tasks / CPU.
    Client router should route the task to Worker 2.
    """
    async with PeerNode(peer_id="worker-busy", host="127.0.0.1", port=0) as worker_busy:
        async with PeerNode(peer_id="worker-idle", host="127.0.0.1", port=0) as worker_idle:
            async with PeerNode(peer_id="client-router-test", host="127.0.0.1", port=0) as client:
                client.enable_gossip()

                # Register both workers in client
                client.peer_registry.register(worker_busy.peer_id, worker_busy.host, worker_busy.bound_port)
                client.peer_registry.register(worker_idle.peer_id, worker_idle.host, worker_idle.bound_port)

                # Record gossip reports for workers: busy vs idle
                now = time.time()
                report_busy = NodeResourceReport(
                    node_id=worker_busy.peer_id,
                    timestamp=now,
                    cpu_percent=85.0,
                    memory_percent=70.0,
                    active_task_count=8,
                    health_status=HealthStatus.HEALTHY,
                )
                report_idle = NodeResourceReport(
                    node_id=worker_idle.peer_id,
                    timestamp=now,
                    cpu_percent=12.0,
                    memory_percent=25.0,
                    active_task_count=0,
                    health_status=HealthStatus.HEALTHY,
                )
                client.gossip_engine.store.update(report_busy)
                client.gossip_engine.store.update(report_idle)

                # Query task router
                decision = client.task_router.select_best_peer()
                assert decision is not None
                assert decision.selected_peer.peer_id == "worker-idle"
                assert decision.active_tasks == 0

                # Client submits task -> should execute on worker-idle
                resp = await client.submit_task("add", 10, 20)
                assert resp.status == TaskStatus.SUCCESS
                assert resp.result == 30


@pytest.mark.asyncio
async def test_execute_only_registered_tasks_security_boundary() -> None:
    """
    Verify security boundary:
    - Unregistered task name is rejected with UNKNOWN_TASK error code.
    - Dangerous or forbidden name (e.g. 'eval', 'exec', 'os.system') is rejected.
    """
    async with PeerNode(peer_id="worker-secure", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-tester", host="127.0.0.1", port=0) as client:
            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            # 1. Unregistered task name
            resp_unknown = await client.submit_task("unregistered_task_calc", 1, 2)
            assert resp_unknown.status in (TaskStatus.ERROR, TaskStatus.FAILURE)
            assert resp_unknown.error is not None
            assert resp_unknown.error.code == "UNKNOWN_TASK"

            # 2. Forbidden reserved task name (rejected during TaskRequest validation)
            resp_forbidden = await client.submit_task("eval", "1 + 1")
            assert resp_forbidden.status == TaskStatus.ERROR
            assert resp_forbidden.error is not None
            assert resp_forbidden.error.code == "INVALID_TASK_REQUEST"


@pytest.mark.asyncio
async def test_graceful_handling_when_no_peers_available() -> None:
    """
    Verify client submitting a task with an empty peer registry returns a clean
    TaskResponse error with code NO_PEERS_AVAILABLE without throwing uncaught exceptions.
    """
    async with PeerNode(peer_id="isolated-client", host="127.0.0.1", port=0) as client:
        resp = await client.submit_task("add", 1, 2)
        assert resp.status == TaskStatus.ERROR
        assert resp.error is not None
        assert resp.error.code == "NO_PEERS_AVAILABLE"
        assert "No available or reachable worker peers" in resp.error.message


@pytest.mark.asyncio
async def test_graceful_handling_and_failover_when_peer_unreachable() -> None:
    """
    Verify failover behavior:
    - Worker 1 is dead/closed.
    - Worker 2 is alive.
    - Client attempts to submit task: router tries Worker 1, catches connection error,
      and seamlessly fails over to Worker 2.
    """
    async with PeerNode(peer_id="worker-alive", host="127.0.0.1", port=0) as worker_alive:
        async with PeerNode(peer_id="client-failover", host="127.0.0.1", port=0) as client:
            client.enable_gossip()

            # Register Worker 1 on a closed port
            client.peer_registry.register("worker-dead", "127.0.0.1", 59999)
            client.peer_registry.register(worker_alive.peer_id, worker_alive.host, worker_alive.bound_port)

            # Client submits task with retry_on_failure=True
            resp = await client.submit_task("add", 100, 400, retry_on_failure=True)
            assert resp.status == TaskStatus.SUCCESS
            assert resp.result == 500


@pytest.mark.asyncio
async def test_target_specific_peer_id() -> None:
    """
    Verify submitting a task targeted explicitly to a specific peer ID.
    """
    async with PeerNode(peer_id="worker-target", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-sender", host="127.0.0.1", port=0) as client:
            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            # Explicit target_peer_id
            resp = await client.submit_task("reverse_string", "antigravity", target_peer_id="worker-target")
            assert resp.status == TaskStatus.SUCCESS
            assert resp.result == "ytivargitna"

            # Non-existent target_peer_id returns PEER_UNAVAILABLE
            resp_bad = await client.submit_task("reverse_string", "test", target_peer_id="non-existent-peer")
            assert resp_bad.status == TaskStatus.ERROR
            assert resp_bad.error is not None
            assert resp_bad.error.code == "PEER_UNAVAILABLE"


@pytest.mark.asyncio
async def test_custom_task_registration_and_execution() -> None:
    """
    Verify registering a custom application task on worker and executing it remotely.
    """
    async with PeerNode(peer_id="worker-custom", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-custom", host="127.0.0.1", port=0) as client:
            # Register custom handler on worker
            @worker.register_task("multiply")
            def multiply(a: int | float, b: int | float) -> int | float:
                return a * b

            # Register async custom handler on worker
            @worker.register_task("async_concat")
            async def async_concat(prefix: str, suffix: str) -> str:
                await asyncio.sleep(0.01)
                return f"{prefix}::{suffix}"

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            # Test multiply
            resp_mult = await client.submit_task("multiply", 7, 8)
            assert resp_mult.status == TaskStatus.SUCCESS
            assert resp_mult.result == 56

            # Test async_concat
            resp_concat = await client.submit_task("async_concat", "mesh", suffix="broker")
            assert resp_concat.status == TaskStatus.SUCCESS
            assert resp_concat.result == "mesh::broker"


@pytest.mark.asyncio
async def test_task_timeout_handling() -> None:
    """
    Verify that slow remote tasks exceeding the configured timeout return TASK_TIMEOUT.
    """
    async with PeerNode(peer_id="worker-slow", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-fast", host="127.0.0.1", port=0) as client:
            @worker.register_task("slow_task")
            async def slow_task() -> str:
                await asyncio.sleep(1.0)
                return "done"

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            resp = await client.submit_task("slow_task", timeout=0.15)
            assert resp.status == TaskStatus.ERROR
            assert resp.error is not None
            assert resp.error.code == "TASK_TIMEOUT"


@pytest.mark.asyncio
async def test_task_router_explain_routing() -> None:
    """
    Verify TaskRouter.explain_routing returns structured candidate evaluations.
    """
    async with PeerNode(peer_id="worker-exp-1", host="127.0.0.1", port=0) as w1:
        async with PeerNode(peer_id="worker-exp-2", host="127.0.0.1", port=0) as w2:
            async with PeerNode(peer_id="client-exp", host="127.0.0.1", port=0) as client:
                client.peer_registry.register(w1.peer_id, w1.host, w1.bound_port)
                client.peer_registry.register(w2.peer_id, w2.host, w2.bound_port)

                explanations = client.task_router.explain_routing()
                assert len(explanations) == 2
                assert all("peer_id" in item for item in explanations)
                assert all("score" in item for item in explanations)
                assert all("health_status" in item for item in explanations)


@pytest.mark.asyncio
async def test_degraded_health_peer_penalized_in_routing() -> None:
    """
    Verify degraded or unhealthy peers are penalized so healthy peers are selected first.
    """
    async with PeerNode(peer_id="worker-degraded", host="127.0.0.1", port=0) as w_deg:
        async with PeerNode(peer_id="worker-healthy", host="127.0.0.1", port=0) as w_hlth:
            async with PeerNode(peer_id="client-health-test", host="127.0.0.1", port=0) as client:
                client.enable_gossip()

                client.peer_registry.register(w_deg.peer_id, w_deg.host, w_deg.bound_port)
                client.peer_registry.register(w_hlth.peer_id, w_hlth.host, w_hlth.bound_port)

                now = time.time()
                # Degraded worker has lower active tasks (0 vs 1), but is DEGRADED
                rep_deg = NodeResourceReport(
                    node_id=w_deg.peer_id,
                    timestamp=now,
                    cpu_percent=10.0,
                    memory_percent=20.0,
                    active_task_count=0,
                    health_status=HealthStatus.DEGRADED,
                )
                rep_hlth = NodeResourceReport(
                    node_id=w_hlth.peer_id,
                    timestamp=now,
                    cpu_percent=15.0,
                    memory_percent=25.0,
                    active_task_count=1,
                    health_status=HealthStatus.HEALTHY,
                )
                client.gossip_engine.store.update(rep_deg)
                client.gossip_engine.store.update(rep_hlth)

                decision = client.task_router.select_best_peer()
                assert decision is not None
                # Healthy peer should be selected because degraded penalty is +50
                assert decision.selected_peer.peer_id == "worker-healthy"


@pytest.mark.asyncio
async def test_remote_task_handler_exception_returns_clean_error() -> None:
    """
    Verify when a registered remote task raises an exception (e.g. ZeroDivisionError),
    the worker returns a structured TaskResponse with status=ERROR and safe error info.
    """
    async with PeerNode(peer_id="worker-err", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-err", host="127.0.0.1", port=0) as client:
            @worker.register_task("faulty_div")
            def faulty_div(a: int, b: int) -> float:
                return a / b

            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            resp = await client.submit_task("faulty_div", 10, 0)
            assert resp.status in (TaskStatus.ERROR, TaskStatus.FAILURE)
            assert resp.error is not None
            assert resp.error.code == "ZeroDivisionError"
            assert "division by zero" in resp.error.message.lower()


@pytest.mark.asyncio
async def test_submit_task_with_task_request_object() -> None:
    """
    Verify submitting a task passing a pre-constructed TaskRequest instance.
    """
    async with PeerNode(peer_id="worker-pre", host="127.0.0.1", port=0) as worker:
        async with PeerNode(peer_id="client-pre", host="127.0.0.1", port=0) as client:
            await client.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)

            req = TaskRequest.create("add", args=[88, 12], task_id="task-req-obj-1")
            resp = await client.submit_task(req)
            assert resp.task_id == "task-req-obj-1"
            assert resp.status == TaskStatus.SUCCESS
            assert resp.result == 100


@pytest.mark.asyncio
async def test_local_task_execution_via_submit_task() -> None:
    """
    Verify submitting a task with target_peer_id == self.peer_id executes locally.
    """
    async with PeerNode(peer_id="self-exec-node", host="127.0.0.1", port=0) as node:
        resp = await node.submit_task("add", 33, 44, target_peer_id="self-exec-node")
        assert resp.status == TaskStatus.SUCCESS
        assert resp.result == 77


@pytest.mark.asyncio
async def test_remote_tasks_demo_execution() -> None:
    """
    Verify programmatic execution of the remote task demonstration.
    """
    from examples.remote_tasks import run_remote_tasks_demo

    results = await run_remote_tasks_demo(host="127.0.0.1", quiet=True)
    assert results["discovery_ok"] is True
    assert results["task_add"]["status"] == "success"
    assert results["task_add"]["result"] == 500
    assert results["task_reverse"]["status"] == "success"
    assert results["task_reverse"]["result"] == "P2P ycnednepeD-oreZ"
    assert results["task_fibonacci"]["status"] == "success"
    assert results["task_fibonacci"]["result"] == 55
    assert results["unregistered_rejected"] is True
    assert results["invalid_rejected"] is True
    assert results["failover_success"] is True
    assert results["teardown_ok"] is True


def test_remote_tasks_cli_script_execution() -> None:
    """
    Verify running examples/remote_tasks.py as a standalone subprocess.
    """
    import subprocess
    import sys
    from pathlib import Path

    workspace_root = Path(__file__).resolve().parent.parent
    script_path = workspace_root / "examples" / "remote_tasks.py"

    proc = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=15.0,
    )
    assert proc.returncode == 0, f"Script failed:\n{proc.stderr}"
    stdout = proc.stdout
    assert "MeshWeaver: End-to-End Remote Task Submission and Execution" in stdout
    assert "Client discovered" in stdout
    assert "Submitting validated example tasks from Client Node..." in stdout
    assert "Testing strict execution safety boundaries..." in stdout
    assert "Testing worker failover resilience..." in stdout
    assert "Remote Task Demonstration completed successfully!" in stdout

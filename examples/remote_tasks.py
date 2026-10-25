"""
MeshWeaver: End-to-End Remote Task Submission and Load-Aware Execution Demonstration.

Demonstrates distributed peer-to-peer task execution across multiple MeshWeaver nodes:
1. Starts a local cluster with Bootstrap Seed, Compute Worker 1, Compute Worker 2, and a Client Node.
2. Performs decentralized bootstrap discovery via seed nodes and neighbor exchange.
3. Disseminates resource and workload reports across the mesh via gossip.
4. Submits validated tasks (add, reverse_string, echo, ping) from the Client Node.
5. TaskRouter dynamically selects the lowest-load reachable worker node using composite scoring.
6. Executes tasks on workers within strict security boundaries (only registered tasks execute).
7. Demonstrates safe rejection of an unregistered task name (UNKNOWN_TASK error).
8. Demonstrates automatic failover: Worker 1 shuts down, tasks automatically route to Worker 2.
9. Executes an orderly, graceful shutdown of all connections and nodes.

Usage:
    python examples/remote_tasks.py
    python examples/remote_tasks.py --verbose
    python examples/remote_tasks.py --json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any

# Ensure meshweaver package in src/ is on sys.path
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = WORKSPACE_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from meshweaver import (  # noqa: E402
    HealthStatus,
    NodeResourceReport,
    PeerNode,
    TaskRequest,
    TaskResponse,
    TaskStatus,
    setup_structured_logging,
)

logger = logging.getLogger("meshweaver.demo.remote_tasks")


def setup_logging(verbose: bool = False, as_json: bool = False) -> None:
    """Configure structured logging for the demonstration."""
    setup_structured_logging(verbose=verbose, as_json=as_json)


async def run_remote_tasks_demo(
    host: str = "127.0.0.1",
    quiet: bool = False,
    as_json: bool = False,
) -> dict[str, Any]:
    """
    Run an end-to-end demonstration of remote task submission, load-aware routing,
    execution boundaries, failover, and graceful shutdown.

    Args:
        host: Host IP to bind nodes to (default: '127.0.0.1').
        quiet: If True, suppress console print statements (useful for automated tests).
        as_json: If True, emit output in structured JSON.

    Returns:
        dict[str, Any]: Verification metrics and results.
    """

    def log_print(text: str = "") -> None:
        if not quiet and not as_json:
            print(text)

    log_print("=" * 80)
    log_print("MeshWeaver: End-to-End Remote Task Submission and Execution")
    log_print("=" * 80)

    results: dict[str, Any] = {}

    # 1. Initialize nodes
    seed = PeerNode(peer_id="seed-broker-node", host=host, port=0)
    worker_1 = PeerNode(peer_id="worker-compute-alpha", host=host, port=0)
    worker_2 = PeerNode(peer_id="worker-compute-beta", host=host, port=0)
    client = PeerNode(peer_id="client-submission-node", host=host, port=0)

    # Register custom task on Worker 1
    @worker_1.register_task("fibonacci")
    def compute_fibonacci(n: int) -> int:
        if n < 0:
            raise ValueError("n must be non-negative")
        a, b = 0, 1
        for _ in range(n):
            a, b = b, a + b
        return a

    # Enable gossip on all nodes with fast intervals for demonstration
    for n in (seed, worker_1, worker_2, client):
        n.enable_gossip(interval=0.2, fanout=2, ttl=60.0)

    nodes = [seed, worker_1, worker_2, client]

    try:
        # Phase 1: Start nodes on dynamic loopback ports
        log_print("\n1. Starting local nodes on dynamic loopback ports...")
        for n in nodes:
            await n.start()
            log_print(f"   [{n.peer_id}] Bound to {n.host}:{n.bound_port}")

        results["ports"] = {n.peer_id: n.bound_port for n in nodes}

        # Phase 2: Bootstrap discovery
        log_print("\n2. Joining mesh via seed node and discovering peers...")
        seed_addr = f"{host}:{seed.bound_port}"
        await worker_1.join_mesh([seed_addr], timeout=3.0)
        await worker_2.join_mesh([seed_addr], timeout=3.0)
        await client.join_mesh([seed_addr], timeout=3.0)
        await asyncio.sleep(0.1)

        discovered = list(client.peer_registry._peers.keys())
        log_print(f"   Client discovered {len(discovered)} peer(s): {discovered}")
        results["client_discovered_peers"] = discovered
        results["discovery_ok"] = (
            worker_1.peer_id in discovered and worker_2.peer_id in discovered
        )

        # Phase 3: Gossip load propagation
        log_print("\n3. Exchanging workload reports via gossip...")
        for n in nodes:
            if n.gossip_engine is not None and n.gossip_engine.is_running:
                await n.gossip_engine.run_round()
        await asyncio.sleep(0.1)

        # Inspect routing decision explanation
        explanations = client.task_router.explain_routing()
        log_print("   Client task routing candidate ranking:")
        for exp in explanations:
            log_print(
                f"     - Peer: {exp['peer_id']} | Score: {exp['score']} | "
                f"Active Tasks: {exp['active_tasks']} | CPU: {exp['cpu_percent']}% | "
                f"Health: {exp['health_status']}"
            )
        results["routing_candidates"] = explanations

        # Phase 4: Submit supported example tasks
        log_print("\n4. Submitting validated example tasks from Client Node...")

        # Task 1: add
        resp_add = await client.submit_task("add", 125, 375)
        log_print(
            f"   [Task: add(125, 375)] -> Result: {resp_add.result} "
            f"(Status: {resp_add.status.value}, ID: {resp_add.task_id[:16]}..., "
            f"Time: {resp_add.execution_time * 1000:.2f}ms)"
        )
        results["task_add"] = {
            "status": resp_add.status.value,
            "result": resp_add.result,
        }

        # Task 2: reverse_string
        resp_rev = await client.submit_task("reverse_string", "Zero-Dependency P2P")
        log_print(
            f"   [Task: reverse_string] -> Result: '{resp_rev.result}' "
            f"(Status: {resp_rev.status.value})"
        )
        results["task_reverse"] = {
            "status": resp_rev.status.value,
            "result": resp_rev.result,
        }

        # Task 3: echo
        resp_echo = await client.submit_task("echo", "payload_sample", cluster="local")
        log_print(
            f"   [Task: echo] -> Result: {resp_echo.result} "
            f"(Status: {resp_echo.status.value})"
        )
        results["task_echo"] = {
            "status": resp_echo.status.value,
            "result": resp_echo.result,
        }

        # Task 4: Custom task 'fibonacci' on Worker 1
        resp_fib = await client.submit_task("fibonacci", 10, target_peer_id=worker_1.peer_id)
        log_print(
            f"   [Task: fibonacci(10) on Worker 1] -> Result: {resp_fib.result} "
            f"(Status: {resp_fib.status.value})"
        )
        results["task_fibonacci"] = {
            "status": resp_fib.status.value,
            "result": resp_fib.result,
        }

        # Phase 5: Rejection of unregistered and invalid tasks
        log_print("\n5. Testing strict execution safety boundaries...")
        resp_unreg = await client.submit_task("unregistered_remote_command", 1, 2)
        log_print(
            f"   Unregistered task safely rejected -> Status: {resp_unreg.status.value}, "
            f"Code: {resp_unreg.error.code if resp_unreg.error else 'None'}, "
            f"Message: {resp_unreg.error.message if resp_unreg.error else 'None'}"
        )
        results["unregistered_rejected"] = bool(
            resp_unreg.status in (TaskStatus.ERROR, TaskStatus.FAILURE)
            and resp_unreg.error is not None
            and resp_unreg.error.code == "UNKNOWN_TASK"
        )

        resp_invalid = await client.submit_task("eval", "print('malicious')")
        log_print(
            f"   Reserved task name rejected -> Status: {resp_invalid.status.value}, "
            f"Code: {resp_invalid.error.code if resp_invalid.error else 'None'}"
        )
        results["invalid_rejected"] = bool(
            resp_invalid.status == TaskStatus.ERROR
            and resp_invalid.error is not None
            and resp_invalid.error.code == "INVALID_TASK_REQUEST"
        )

        # Phase 6: Automatic failover testing
        log_print("\n6. Testing worker failover resilience...")
        # Gracefully stop Worker 1
        await worker_1.stop()
        log_print(f"   [Worker 1] {worker_1.peer_id} stopped (simulating worker crash/offline).")

        # Submit task with Worker 1 stopped: router fails over to Worker 2
        resp_failover = await client.submit_task("add", 999, 1, retry_on_failure=True)
        log_print(
            f"   Task dispatched during failover -> Result: {resp_failover.result} "
            f"(Status: {resp_failover.status.value})"
        )
        results["failover_success"] = bool(
            resp_failover.status == TaskStatus.SUCCESS and resp_failover.result == 1000
        )

        # Phase 7: Orderly teardown
        log_print("\n7. Executing graceful teardown of remaining nodes...")
        await client.stop()
        await worker_2.stop()
        await seed.stop()
        log_print("   All peer nodes cleanly stopped.")

        results["teardown_ok"] = bool(
            not client.is_running
            and not worker_1.is_running
            and not worker_2.is_running
            and not seed.is_running
        )

        log_print("\n" + "=" * 80)
        log_print("Remote Task Demonstration completed successfully!")
        log_print("=" * 80 + "\n")

        if as_json:
            print(json.dumps(results, indent=2))

    finally:
        for n in nodes:
            if n.is_running:
                try:
                    await n.stop()
                except Exception:
                    pass

    return results


def main() -> int:
    """Command-line entry point for the remote task demonstration."""
    parser = argparse.ArgumentParser(
        prog="remote_tasks.py",
        description="MeshWeaver End-to-End Remote Task Execution Demonstration",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Loopback host interface to bind nodes")
    parser.add_argument("--json", action="store_true", help="Output demonstration results as JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")

    args = parser.parse_args()
    setup_logging(verbose=args.verbose, as_json=args.json)

    try:
        results = asyncio.run(
            run_remote_tasks_demo(
                host=args.host,
                as_json=args.json,
            )
        )
        success = bool(
            results.get("discovery_ok")
            and results.get("unregistered_rejected")
            and results.get("invalid_rejected")
            and results.get("failover_success")
            and results.get("teardown_ok")
        )
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\nDemonstration interrupted by user.")
        return 130
    except Exception as exc:
        logger.exception("Demonstration failed: %s", exc)
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

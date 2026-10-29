"""
MeshWeaver: Load-Aware Task Scheduling Demonstration.

Week 3, Milestone 2 demonstration:
1. Evaluates candidate worker peers using CPU %, memory %, active tasks, and health status.
2. Calculates composite load scores using documented scoring function with configurable weights.
3. Prefers suitable lower-load workers over busy or degraded peers.
4. Enforces strict rejection of stale resource reports (age > stale_report_threshold).
5. Avoids scheduling work to failed or unhealthy peers (HealthStatus.UNHEALTHY).
6. Enforces task compatibility (supported tasks, capability tags, and capacity limits).
7. Demonstrates deterministic tie-breaking via Kademlia XOR distance.
8. Submits task to the selected optimal worker peer over live TCP connection.

Usage:
    python examples/load_aware_scheduling.py
"""

from __future__ import annotations

import asyncio
from pathlib import Path
import sys
import time

# Ensure meshweaver package in src/ is on sys.path
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = WORKSPACE_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from meshweaver import (  # noqa: E402
    HealthStatus,
    NodeResourceReport,
    PeerNode,
    SchedulerConfig,
    TaskRequest,
    TaskScheduler,
    TaskStatus,
)


async def run_scheduling_demo() -> None:
    """Run an interactive demonstration of load-aware task scheduling."""
    print("=" * 76)
    print("MeshWeaver: Load-Aware Task Scheduling (Week 3, Milestone 2)")
    print("=" * 76)

    # 1. Start candidate worker nodes
    async with PeerNode(peer_id="worker-heavy", host="127.0.0.1", port=0) as w_heavy:
        async with PeerNode(peer_id="worker-light", host="127.0.0.1", port=0) as w_light:
            async with PeerNode(peer_id="worker-stale", host="127.0.0.1", port=0) as w_stale:
                async with PeerNode(peer_id="worker-unhealthy", host="127.0.0.1", port=0) as w_unhealthy:
                    async with PeerNode(peer_id="client-scheduler", host="127.0.0.1", port=0) as client:
                        client.enable_gossip()

                        # Register all workers in client peer registry
                        client.peer_registry.register(w_heavy.peer_id, w_heavy.host, w_heavy.bound_port)
                        client.peer_registry.register(w_light.peer_id, w_light.host, w_light.bound_port)
                        client.peer_registry.register(w_stale.peer_id, w_stale.host, w_stale.bound_port)
                        client.peer_registry.register(w_unhealthy.peer_id, w_unhealthy.host, w_unhealthy.bound_port)

                        now = time.time()

                        # Worker Heavy: High active tasks, high CPU
                        client.gossip_engine.store.update(
                            NodeResourceReport(
                                node_id=w_heavy.peer_id,
                                timestamp=now - 2.0,
                                cpu_percent=85.0,
                                memory_percent=75.0,
                                active_task_count=6,
                                health_status=HealthStatus.HEALTHY,
                            )
                        )

                        # Worker Light: 0 active tasks, low CPU, low memory
                        client.gossip_engine.store.update(
                            NodeResourceReport(
                                node_id=w_light.peer_id,
                                timestamp=now - 1.0,
                                cpu_percent=12.0,
                                memory_percent=20.0,
                                active_task_count=0,
                                health_status=HealthStatus.HEALTHY,
                            )
                        )

                        # Worker Stale: Low load, but report is 50 seconds old (threshold: 30s)
                        client.gossip_engine.store.update(
                            NodeResourceReport(
                                node_id=w_stale.peer_id,
                                timestamp=now - 50.0,
                                cpu_percent=5.0,
                                memory_percent=10.0,
                                active_task_count=0,
                                health_status=HealthStatus.HEALTHY,
                            )
                        )

                        # Worker Unhealthy: UNHEALTHY health status
                        client.gossip_engine.store.update(
                            NodeResourceReport(
                                node_id=w_unhealthy.peer_id,
                                timestamp=now - 1.0,
                                cpu_percent=5.0,
                                memory_percent=10.0,
                                active_task_count=0,
                                health_status=HealthStatus.UNHEALTHY,
                            )
                        )

                        print("\nCandidate Workers Discovered in Mesh:")
                        print("-" * 76)
                        print(f"  - {w_heavy.peer_id:<20} | 6 active tasks | 85.0% CPU | 75.0% MEM | HEALTHY")
                        print(f"  - {w_light.peer_id:<20} | 0 active tasks | 12.0% CPU | 20.0% MEM | HEALTHY")
                        print(f"  - {w_stale.peer_id:<20} | 0 active tasks |  5.0% CPU | 10.0% MEM | STALE REPORT (50s old)")
                        print(f"  - {w_unhealthy.peer_id:<20} | 0 active tasks |  5.0% CPU | 10.0% MEM | UNHEALTHY")

                        # Perform scheduling decision
                        task_req = TaskRequest.create("add", args=[40, 2], task_id="task-demo-42")
                        decision = client.scheduler.schedule(task_req)

                        print("\nScheduling Evaluation Results:")
                        print("-" * 76)
                        for ev in decision.evaluations:
                            status_tag = "ELIGIBLE" if ev.is_eligible else f"REJECTED ({ev.rejection_reason})"
                            score_str = f"Score: {ev.score:.2f}" if ev.score is not None else "Score: N/A"
                            print(f"  [{ev.peer.peer_id}] -> {status_tag} | {score_str}")

                        print("\nSelection Outcome:")
                        print("-" * 76)
                        print(f"  Selected Worker : {decision.selected_peer.peer_id}")
                        print(f"  Endpoint        : {decision.selected_peer.host}:{decision.selected_peer.port}")
                        print(f"  Composite Score : {decision.score:.2f}")
                        print(f"  Reason          : {decision.reason}")

                        assert decision.selected_peer.peer_id == "worker-light"

                        # Submit task across network using selected worker
                        print("\nSubmitting task 'add(40, 2)' to cluster...")
                        resp = await client.submit_task("add", 40, 2)
                        print(f"  Task ID         : {resp.task_id}")
                        print(f"  Status          : {resp.status.value}")
                        print(f"  Result          : {resp.result}")
                        print(f"  Execution Time  : {resp.execution_time * 1000:.2f} ms")
                        print("=" * 76)
                        print("Load-aware task scheduling demonstration completed successfully!")


if __name__ == "__main__":
    asyncio.run(run_scheduling_demo())

"""
MeshWeaver: Asynchronous Task Worker Execution Engine Demonstration.

Week 3, Milestone 1 demonstration:
1. Initializes TaskWorker with configurable concurrency limits and execution timeouts.
2. Accepts valid task envelopes (TaskEnvelope, TaskRequest, and dictionary payloads).
3. Validates task IDs, task names, and 64 KB payload size limits.
4. Executes supported tasks asynchronously where appropriate (async coroutines directly,
   synchronous handlers safely in threads).
5. Observes strict task lifecycle states: pending -> running -> succeeded / failed.
6. Enforces per-task and worker-level execution timeouts.
7. Handles exceptions safely with structured TaskErrorInfo (error code, message, duration).
8. Demonstrates security boundaries: strictly blocks arbitrary untrusted code execution
   (unregistered tasks, eval, exec, os, sys, code injection).
9. Demonstrates worker concurrency control and graceful shutdown.

Usage:
    python examples/task_worker_execution.py
    python examples/task_worker_execution.py --concurrency 3
"""

from __future__ import annotations

import argparse
import asyncio
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
    TaskEnvelope,
    TaskErrorInfo,
    TaskLifecycleState,
    TaskResult,
    TaskWorker,
)


async def run_worker_demo(concurrency: int = 3) -> None:
    """Run an interactive demonstration of the asynchronous task worker."""
    print("=" * 76)
    print("MeshWeaver: Asynchronous Task Worker Execution Engine (Week 3, Milestone 1)")
    print(f"Worker Concurrency: {concurrency} workers | Concurrency Control: Semaphore & Queue")
    print("=" * 76)

    async with TaskWorker(concurrency=concurrency, default_timeout=5.0) as worker:
        # Register custom asynchronous task
        @worker.register("async_compute")
        async def async_compute(val: int, multiplier: int = 10, delay: float = 0.05) -> int:
            await asyncio.sleep(delay)
            return val * multiplier

        # Register custom synchronous task (runs safely in thread without blocking event loop)
        @worker.register("text_transform")
        def text_transform(text: str, operation: str = "upper") -> str:
            if operation == "upper":
                return text.upper()
            elif operation == "reverse":
                return text[::-1]
            return text

        # Register task that simulates an internal exception
        @worker.register("risky_calculation")
        def risky_calculation(numerator: float, denominator: float) -> float:
            return numerator / denominator

        # Register task that simulates a slow operation for timeout demonstration
        @worker.register("long_running_job")
        async def long_running_job(duration: float) -> str:
            await asyncio.sleep(duration)
            return "job_completed"

        # --------------------------------------------------------------------
        # 1. Envelopes and Lifecycle State Demonstration (pending -> running -> succeeded)
        # --------------------------------------------------------------------
        print("\n1. Submitting valid task envelopes...")
        env1 = TaskEnvelope(
            task_name="add",
            args=[125, 375],
            task_id="envelope-add-001",
            metadata={"source": "demo_client"},
        )
        env2 = TaskEnvelope(
            task_name="async_compute",
            args=[42],
            kwargs={"multiplier": 5, "delay": 0.03},
            task_id="envelope-async-002",
        )
        env3 = TaskEnvelope(
            task_name="text_transform",
            args=["MeshWeaver P2P Task Broker"],
            kwargs={"operation": "reverse"},
            task_id="envelope-text-003",
        )

        rec1 = await worker.submit(env1)
        rec2 = await worker.submit(env2)
        rec3 = await worker.submit(env3)

        print(f"   [Task 1] ID: {rec1.task_id} -> Lifecycle State: {rec1.state}")
        print(f"   [Task 2] ID: {rec2.task_id} -> Lifecycle State: {rec2.state}")
        print(f"   [Task 3] ID: {rec3.task_id} -> Lifecycle State: {rec3.state}")

        print("\n2. Awaiting task completions...")
        res1 = await rec1.wait()
        res2 = await rec2.wait()
        res3 = await rec3.wait()

        print(f"   [Task 1 - add]:            Result: {res1.result} | Status: {res1.status} (took {res1.duration:.4f}s)")
        print(f"   [Task 2 - async_compute]:  Result: {res2.result} | Status: {res2.status} (took {res2.duration:.4f}s)")
        print(f"   [Task 3 - text_transform]: Result: {res3.result!r} | Status: {res3.status} (took {res3.duration:.4f}s)")

        # --------------------------------------------------------------------
        # 2. Execution Timeouts Demonstration
        # --------------------------------------------------------------------
        print("\n3. Demonstrating execution timeouts...")
        env_timeout = TaskEnvelope(
            task_name="long_running_job",
            args=[2.0],  # Job takes 2.0s
            timeout=0.08,  # Envelope enforces 0.08s timeout
            task_id="envelope-timeout-004",
        )
        print(f"   Submitting task with 0.08s timeout limit (expected execution: 2.0s)...")
        res_timeout = await worker.execute_envelope(env_timeout)
        print(f"   [Task 4 - Timeout Result]: Status: {res_timeout.status}")
        print(f"   Error Code:    {res_timeout.error.code if res_timeout.error else None}")
        print(f"   Error Message: {res_timeout.error.message if res_timeout.error else None}")

        # --------------------------------------------------------------------
        # 3. Exception Handling & Error Boundaries
        # --------------------------------------------------------------------
        print("\n4. Demonstrating exception boundaries and isolation...")
        env_failing = TaskEnvelope(
            task_name="risky_calculation",
            args=[100.0, 0.0],  # Division by zero
            task_id="envelope-error-005",
        )
        res_failing = await worker.execute_envelope(env_failing)
        print(f"   [Task 5 - Error Result]:   Status: {res_failing.status}")
        print(f"   Error Code:    {res_failing.error.code if res_failing.error else None}")
        print(f"   Error Message: {res_failing.error.message if res_failing.error else None}")
        print(f"   Worker State:  Worker remains healthy and operational.")

        # --------------------------------------------------------------------
        # 4. Security Boundaries: Blocking Untrusted Code Execution
        # --------------------------------------------------------------------
        print("\n5. Demonstrating security boundaries (untrusted code prevention)...")

        # Attempt A: Unregistered task
        res_unregistered = await worker.execute("untrusted_remote_code_execution", "do_evil()")
        print(f"   [Security A - Unregistered Task]: Status: {res_unregistered.status} | Code: {res_unregistered.error.code if res_unregistered.error else None}")

        # Attempt B: Dangerous reserved name (eval, exec, os, sys)
        bad_envelope = {"task_id": "malicious-001", "task_name": "eval", "args": ["1 + 1"]}
        res_eval = await worker.execute_envelope(bad_envelope)
        print(f"   [Security B - Reserved Name 'eval']: Status: {res_eval.status} | Code: {res_eval.error.code if res_eval.error else None}")

        # Attempt C: Code object / forbidden parameter injection
        bad_keys_envelope = {"task_id": "malicious-002", "task_name": "add", "kwargs": {"__import__": "os"}}
        res_forbidden = await worker.execute_envelope(bad_keys_envelope)
        print(f"   [Security C - Forbidden Parameter]: Status: {res_forbidden.status} | Code: {res_forbidden.error.code if res_forbidden.error else None}")

        # --------------------------------------------------------------------
        # 5. Worker Statistics
        # --------------------------------------------------------------------
        print("\n6. Final Worker Statistics:")
        print(f"   Total Tracked Tasks:   {worker.total_tasks}")
        print(f"   Succeeded Tasks:       {worker.succeeded_count}")
        print(f"   Failed Tasks:          {worker.failed_count}")
        print(f"   Active Tasks:          {worker.active_count}")
        print("=" * 76)
        print("Worker execution engine demonstration completed successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MeshWeaver: Asynchronous Task Worker Demonstration"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Worker concurrency limit (default: 3)",
    )
    args = parser.parse_args()
    asyncio.run(run_worker_demo(concurrency=args.concurrency))


if __name__ == "__main__":
    main()

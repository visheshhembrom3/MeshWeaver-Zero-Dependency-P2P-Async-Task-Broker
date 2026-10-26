"""
MeshWeaver: Local Asynchronous Task Execution Engine Demonstration.

Demonstrates the asynchronous task execution engine running locally:
1. Initializes TaskExecutionEngine with configurable concurrency (max_concurrency=2).
2. Submits standard example tasks (add, reverse_string, echo, ping) with automatic UUIDs.
3. Registers custom synchronous and asynchronous tasks with argument validation.
4. Observes lifecycle task states (PENDING -> RUNNING -> COMPLETED).
5. Demonstrates concurrency limits and worker queue scheduling.
6. Demonstrates exception and error handling (transition to FAILED state).
7. Demonstrates task cancellation and graceful engine shutdown.

Usage:
    python examples/local_task_execution.py
    python examples/local_task_execution.py --concurrency 3
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
    TaskErrorInfo,
    TaskExecutionEngine,
    TaskRecord,
    TaskState,
)


async def run_local_execution_demo(concurrency: int = 2) -> None:
    """Run an interactive demonstration of the local task execution engine."""
    print("=" * 70)
    print("MeshWeaver: Asynchronous Task Execution Engine Demonstration")
    print(f"Concurrency Limit: {concurrency} workers")
    print("=" * 70)

    async with TaskExecutionEngine(max_concurrency=concurrency) as engine:
        # Register a custom async task
        @engine.register("delayed_multiply")
        async def delayed_multiply(a: int, b: int, delay: float = 0.05) -> int:
            await asyncio.sleep(delay)
            return a * b

        # Register a task that raises an exception
        @engine.register("safe_divide")
        def safe_divide(x: float, y: float) -> float:
            return x / y

        print("\n1. Submitting built-in and custom tasks...")
        rec1 = await engine.submit("add", 100, 250)
        rec2 = await engine.submit("reverse_string", "MeshWeaver Async Engine")
        rec3 = await engine.submit("delayed_multiply", 7, 8, delay=0.03)
        rec4 = await engine.submit("ping")
        rec5 = await engine.submit("safe_divide", 42, 0)  # Will fail gracefully

        print(f"   Submitted {engine.total_tasks} tasks to engine queue.")
        print(f"   Task 1 ID: {rec1.task_id} (State: {rec1.state})")
        print(f"   Task 2 ID: {rec2.task_id} (State: {rec2.state})")
        print(f"   Task 3 ID: {rec3.task_id} (State: {rec3.state})")
        print(f"   Task 5 ID: {rec5.task_id} (State: {rec5.state})")

        print("\n2. Awaiting task completions...")
        await asyncio.gather(
            rec1.wait(),
            rec2.wait(),
            rec3.wait(),
            rec4.wait(),
            rec5.wait(),
        )

        print("\n3. Execution Results:")
        print(f"   [Task 1 - add]:            Result = {rec1.result} (State = {rec1.state}, Duration = {rec1.duration:.4f}s)")
        print(f"   [Task 2 - reverse_string]: Result = {rec1.result!r} -> {rec2.result!r} (State = {rec2.state})")
        print(f"   [Task 3 - delayed_mult]:   Result = {rec3.result} (State = {rec3.state})")
        print(f"   [Task 4 - ping]:           Result = {rec4.result} (State = {rec4.state})")
        print(f"   [Task 5 - safe_divide]:    Result = {rec5.result} (State = {rec5.state}, Error = {rec5.error.code if rec5.error else None})")

        print("\n4. Demonstrating Task Cancellation...")
        # Submit a long task and cancel it
        long_rec = await engine.submit("delayed_multiply", 99, 99, delay=5.0)
        await asyncio.sleep(0.02)
        cancelled = await engine.cancel_task(long_rec.task_id)
        await asyncio.sleep(0.02)
        print(f"   Cancelled task {long_rec.task_id}: {cancelled} (State = {long_rec.state})")

        print("\n5. Engine Statistics:")
        print(f"   Total Tasks:     {engine.total_tasks}")
        print(f"   Completed Tasks: {engine.completed_count}")
        print(f"   Failed Tasks:    {engine.failed_count}")
        print(f"   Cancelled Tasks: {engine.cancelled_count}")

    print("\n" + "=" * 70)
    print("Demonstration completed with orderly engine shutdown.")
    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="MeshWeaver local task execution demo")
    parser.add_argument("--concurrency", type=int, default=2, help="Worker concurrency limit (default: 2)")
    args = parser.parse_args()
    asyncio.run(run_local_execution_demo(concurrency=args.concurrency))


if __name__ == "__main__":
    main()

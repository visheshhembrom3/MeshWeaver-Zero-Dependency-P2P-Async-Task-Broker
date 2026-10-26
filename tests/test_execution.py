"""
Comprehensive unit and integration tests for the asynchronous TaskExecutionEngine,
TaskRecord, TaskState transitions, concurrency limits, security boundaries, and shutdown.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    DEFAULT_MAX_CONCURRENCY,
    InvalidTaskError,
    TaskErrorInfo,
    TaskExecutionEngine,
    TaskPayloadLimitError,
    TaskRecord,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskState,
    TaskStatus,
    UnknownTaskError,
)


# ============================================================================
# 1. Successful Task Execution Tests
# ============================================================================


@pytest.mark.asyncio
async def test_default_example_tasks_success() -> None:
    """Verify built-in example tasks execute successfully via the engine."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        # 1. add
        record_add = await engine.submit("add", 15, 27)
        await record_add.wait()
        assert record_add.state == TaskState.COMPLETED
        assert record_add.result == 42
        assert record_add.error is None
        assert record_add.duration is not None and record_add.duration >= 0.0

        # 2. reverse_string
        record_rev = await engine.submit("reverse_string", "MeshWeaver")
        await record_rev.wait()
        assert record_rev.state == TaskState.COMPLETED
        assert record_rev.result == "revaeWhseM"

        # 3. echo
        record_echo = await engine.submit("echo", "alpha", key="omega")
        await record_echo.wait()
        assert record_echo.state == TaskState.COMPLETED
        assert record_echo.result == {
            "args": ["alpha"],
            "kwargs": {"key": "omega"},
        }

        # 4. ping
        record_ping = await engine.submit("ping")
        await record_ping.wait()
        assert record_ping.state == TaskState.COMPLETED
        assert record_ping.result == "pong"


@pytest.mark.asyncio
async def test_submit_custom_sync_and_async_tasks() -> None:
    """Verify registration and execution of custom synchronous and asynchronous tasks."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        # Register sync task
        @engine.register("multiply")
        def multiply(a: int, b: int) -> int:
            return a * b

        # Register async task
        @engine.register("delayed_upper")
        async def delayed_upper(text: str) -> str:
            await asyncio.sleep(0.01)
            return text.upper()

        rec_mult = await engine.submit("multiply", 6, 7)
        rec_upper = await engine.submit("delayed_upper", "distributed")

        await asyncio.gather(rec_mult.wait(), rec_upper.wait())

        assert rec_mult.state == TaskState.COMPLETED
        assert rec_mult.result == 42

        assert rec_upper.state == TaskState.COMPLETED
        assert rec_upper.result == "DISTRIBUTED"


@pytest.mark.asyncio
async def test_execute_convenience_method() -> None:
    """Verify engine.execute directly returns the execution result."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        res = await engine.execute("add", 100, 250)
        assert res == 350


@pytest.mark.asyncio
async def test_execute_request_adapter() -> None:
    """Verify executing a TaskRequest envelope and getting a TaskResponse."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        req = TaskRequest.create("add", args=[8, 9], metadata={"user": "tester"})
        resp = await engine.execute_request(req)
        assert isinstance(resp, TaskResponse)
        assert resp.status == TaskStatus.SUCCESS
        assert resp.result == 17
        assert resp.task_id == req.task_id
        assert resp.execution_time is not None and resp.execution_time >= 0.0


@pytest.mark.asyncio
async def test_task_id_and_timestamps() -> None:
    """Verify unique task IDs, explicit custom IDs, and monotonic timestamps."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        t_before = time.time()
        rec_auto = await engine.submit("ping")
        rec_custom = await engine.submit("ping", task_id="my-custom-task-001")

        await asyncio.gather(rec_auto.wait(), rec_custom.wait())
        t_after = time.time()

        # Generated ID matches safe pattern
        assert rec_auto.task_id.startswith("task-")
        assert rec_custom.task_id == "my-custom-task-001"

        # Timestamps are ordered
        for rec in (rec_auto, rec_custom):
            assert rec.created_at >= t_before
            assert rec.started_at is not None
            assert rec.completed_at is not None
            assert rec.created_at <= rec.started_at <= rec.completed_at <= t_after + 0.1
            assert rec.duration is not None and rec.duration >= 0.0

            # Dictionary serialization
            d = rec.to_dict()
            assert d["task_id"] == rec.task_id
            assert d["state"] == "COMPLETED"
            assert d["result"] == "pong"


# ============================================================================
# 2. Invalid Task Names and Arguments (Security Boundaries)
# ============================================================================


@pytest.mark.asyncio
async def test_unknown_task_name_rejected() -> None:
    """Verify that submitting an unregistered task name raises UnknownTaskError."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        with pytest.raises(UnknownTaskError):
            await engine.submit("non_existent_task")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "dangerous_name",
    [
        "os.system",
        "subprocess.run",
        "eval",
        "exec",
        "__import__",
        "open",
        "builtins.open",
        "module:func",
        "/bin/sh",
        "scripts/run.py",
        "my__custom__task",
    ],
)
async def test_dangerous_task_name_rejected(dangerous_name: str) -> None:
    """Verify that dangerous task names and import paths are rejected at submission."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        with pytest.raises(InvalidTaskError):
            await engine.submit(dangerous_name)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_id",
    [
        "",
        "   ",
        "task with spaces",
        "task;rm -rf",
        "task/slash",
        "task.dot",
        "task:colon",
        "x" * 150,
    ],
)
async def test_invalid_task_id_rejected(invalid_id: str) -> None:
    """Verify that malformed or oversized task IDs are rejected."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        with pytest.raises((InvalidTaskError, TaskPayloadLimitError)):
            await engine.submit("ping", task_id=invalid_id)


@pytest.mark.asyncio
async def test_duplicate_task_id_rejected() -> None:
    """Verify that submitting a task with an already tracked ID raises InvalidTaskError."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        await engine.submit("ping", task_id="unique-task-id")
        with pytest.raises(InvalidTaskError, match="already tracked"):
            await engine.submit("ping", task_id="unique-task-id")


@pytest.mark.asyncio
async def test_invalid_argument_types_rejected() -> None:
    """Verify that non-JSON-compatible arguments and forbidden keys are rejected."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        # Lambda argument rejected
        with pytest.raises(InvalidTaskError):
            await engine.submit("echo", lambda x: x)

        # Forbidden keyword key rejected
        with pytest.raises(InvalidTaskError):
            await engine.submit("echo", **{"__import__": "os"})

        with pytest.raises(InvalidTaskError):
            await engine.submit("echo", **{"eval": "1+1"})


@pytest.mark.asyncio
async def test_handler_returning_unserializable_result() -> None:
    """Verify that a handler returning an un-serializable result transitions to FAILED."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        @engine.register("bad_return")
        def bad_return() -> Any:
            return lambda: "not serializable"

        rec = await engine.submit("bad_return")
        await rec.wait()

        assert rec.state == TaskState.FAILED
        assert rec.result is None
        assert rec.error is not None
        assert rec.error.code == "INVALID_RESULT"


# ============================================================================
# 3. Concurrency Limits
# ============================================================================


@pytest.mark.asyncio
async def test_concurrency_limit_enforced() -> None:
    """
    Verify that at no point in time do more tasks execute concurrently
    than the configured max_concurrency.
    """
    max_concurrent = 2
    task_count = 6
    sleep_duration = 0.04

    active_counts: list[int] = []
    current_active = 0
    lock = asyncio.Lock()

    async with TaskExecutionEngine(max_concurrency=max_concurrent) as engine:
        @engine.register("sleep_tracker")
        async def sleep_tracker() -> None:
            nonlocal current_active
            async with lock:
                current_active += 1
                active_counts.append(current_active)

            await asyncio.sleep(sleep_duration)

            async with lock:
                current_active -= 1

        records = [await engine.submit("sleep_tracker") for _ in range(task_count)]
        await asyncio.gather(*(r.wait() for r in records))

        assert len(active_counts) == task_count
        assert max(active_counts) <= max_concurrent
        for r in records:
            assert r.state == TaskState.COMPLETED


def test_invalid_max_concurrency_init() -> None:
    """Verify that non-positive max_concurrency or negative queue size raises ValueError."""
    with pytest.raises(ValueError, match="max_concurrency"):
        TaskExecutionEngine(max_concurrency=0)

    with pytest.raises(ValueError, match="max_concurrency"):
        TaskExecutionEngine(max_concurrency=-5)

    with pytest.raises(ValueError, match="max_queue_size"):
        TaskExecutionEngine(max_queue_size=-1)


# ============================================================================
# 4. Exceptions and Failed Tasks
# ============================================================================


@pytest.mark.asyncio
async def test_task_raising_exception() -> None:
    """Verify that synchronous and asynchronous exceptions are caught and set task to FAILED."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        @engine.register("divide")
        def divide(a: int, b: int) -> float:
            return a / b

        rec = await engine.submit("divide", 10, 0)
        await rec.wait()

        assert rec.state == TaskState.FAILED
        assert rec.result is None
        assert rec.error is not None
        assert rec.error.code == "ZeroDivisionError"
        assert "division by zero" in rec.error.message.lower()
        assert rec.exception is not None
        assert isinstance(rec.exception, ZeroDivisionError)

        # Subsequent tasks continue to execute cleanly
        rec_next = await engine.submit("add", 5, 5)
        await rec_next.wait()
        assert rec_next.state == TaskState.COMPLETED
        assert rec_next.result == 10


@pytest.mark.asyncio
async def test_execute_method_raises_on_failure() -> None:
    """Verify engine.execute raises the underlying exception on failed tasks."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        @engine.register("failing_task")
        def failing_task() -> None:
            raise ValueError("custom task failure message")

        with pytest.raises(ValueError, match="custom task failure message"):
            await engine.execute("failing_task")


@pytest.mark.asyncio
async def test_wait_raise_on_error_flag() -> None:
    """Verify TaskRecord.wait(raise_on_error=True) raises, while default returns record."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        @engine.register("blowup")
        def blowup() -> None:
            raise KeyError("missing_key")

        rec = await engine.submit("blowup")

        # Without raise_on_error, wait() returns the record
        res = await rec.wait(raise_on_error=False)
        assert res is rec
        assert rec.state == TaskState.FAILED

        # With raise_on_error=True, wait() raises KeyError
        with pytest.raises(KeyError):
            await rec.wait(raise_on_error=True)


@pytest.mark.asyncio
async def test_handler_argument_mismatch() -> None:
    """Verify that calling a handler with mismatched arguments sets task to FAILED."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        # 'add' requires 2 arguments; calling with none fails
        rec = await engine.submit("add")
        await rec.wait()

        assert rec.state == TaskState.FAILED
        assert rec.error is not None
        assert rec.error.code == "TypeError"


# ============================================================================
# 5. Queue Shutdown and Task-State Transitions
# ============================================================================


@pytest.mark.asyncio
async def test_task_state_transitions_lifecycle() -> None:
    """Verify state transitions: PENDING -> RUNNING -> COMPLETED."""
    engine = TaskExecutionEngine(max_concurrency=1, auto_start=False)

    started_gate = asyncio.Event()
    finish_gate = asyncio.Event()

    @engine.register("gated_task")
    async def gated_task() -> str:
        started_gate.set()
        await finish_gate.wait()
        return "done"

    # 1. Submitted before engine start -> PENDING
    rec = await engine.submit("gated_task")
    assert rec.state == TaskState.PENDING
    assert rec.is_pending
    assert not rec.is_running
    assert not rec.is_done

    # 2. Start engine -> picked up by worker -> RUNNING
    engine.start()
    await started_gate.wait()
    assert rec.state == TaskState.RUNNING
    assert rec.is_running
    assert not rec.is_done

    # 3. Release gate -> COMPLETED
    finish_gate.set()
    await rec.wait()
    assert rec.state == TaskState.COMPLETED
    assert rec.is_completed
    assert rec.is_done
    assert rec.result == "done"

    await engine.shutdown()


@pytest.mark.asyncio
async def test_cancel_pending_task() -> None:
    """Verify cancelling a task while it is still in PENDING state."""
    engine = TaskExecutionEngine(max_concurrency=1, auto_start=False)

    rec = await engine.submit("ping")
    assert rec.state == TaskState.PENDING

    # Cancel while pending
    cancelled = await engine.cancel_task(rec.task_id)
    assert cancelled is True
    assert rec.state == TaskState.CANCELLED
    assert rec.is_cancelled
    assert rec.is_done

    # Start engine; cancelled task should not execute
    engine.start()
    await asyncio.sleep(0.05)
    assert rec.state == TaskState.CANCELLED
    assert rec.result is None

    await engine.shutdown()


@pytest.mark.asyncio
async def test_cancel_running_task() -> None:
    """Verify cancelling a task while it is actively RUNNING."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        entered_event = asyncio.Event()

        @engine.register("long_sleep")
        async def long_sleep() -> str:
            entered_event.set()
            await asyncio.sleep(10.0)
            return "finished"

        rec = await engine.submit("long_sleep")
        await entered_event.wait()
        assert rec.state == TaskState.RUNNING

        cancelled = await engine.cancel_task(rec.task_id)
        assert cancelled is True

        await asyncio.sleep(0.05)
        assert rec.state == TaskState.CANCELLED
        assert rec.is_cancelled
        assert rec.is_done


@pytest.mark.asyncio
async def test_engine_graceful_shutdown_wait() -> None:
    """Verify shutdown(wait=True) allows queued tasks to complete."""
    engine = TaskExecutionEngine(max_concurrency=2)
    engine.start()

    records = [await engine.submit("add", i, 1) for i in range(5)]
    await engine.shutdown(wait=True)

    for i, r in enumerate(records):
        assert r.state == TaskState.COMPLETED
        assert r.result == i + 1

    # Cannot submit after shutdown
    with pytest.raises(RuntimeError, match="shut down"):
        await engine.submit("ping")


@pytest.mark.asyncio
async def test_engine_shutdown_cancel_pending() -> None:
    """Verify shutdown(cancel_pending=True) cancels tasks sitting in queue."""
    engine = TaskExecutionEngine(max_concurrency=1, auto_start=False)

    records = [await engine.submit("ping") for _ in range(4)]
    for r in records:
        assert r.state == TaskState.PENDING

    await engine.shutdown(wait=False, cancel_pending=True)

    for r in records:
        assert r.state == TaskState.CANCELLED


# ============================================================================
# 6. Task Inspection and State Filtering
# ============================================================================


@pytest.mark.asyncio
async def test_task_filtering_and_counters() -> None:
    """Verify engine counters and filtering by state."""
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        @engine.register("fail")
        def fail() -> None:
            raise RuntimeError("failure")

        rec1 = await engine.submit("add", 1, 2)
        rec2 = await engine.submit("fail")

        await asyncio.gather(rec1.wait(), rec2.wait())

        assert engine.completed_count == 1
        assert engine.failed_count == 1
        assert engine.total_tasks == 2

        completed_tasks = engine.list_tasks(TaskState.COMPLETED)
        assert len(completed_tasks) == 1
        assert completed_tasks[0].task_id == rec1.task_id

        failed_tasks = engine.list_tasks(TaskState.FAILED)
        assert len(failed_tasks) == 1
        assert failed_tasks[0].task_id == rec2.task_id


# ============================================================================
# 7. TaskState Enum Compatibility & Equivalences
# ============================================================================


def test_task_state_enum_and_comparisons() -> None:
    """Verify TaskState equality with strings, case insensitivity, and TaskStatus."""
    assert TaskState.PENDING == "PENDING"
    assert TaskState.PENDING == "pending"
    assert TaskState.PENDING == TaskStatus.PENDING

    assert TaskState.RUNNING == "RUNNING"
    assert TaskState.RUNNING == "running"
    assert TaskState.RUNNING == TaskStatus.RUNNING

    assert TaskState.COMPLETED == "COMPLETED"
    assert TaskState.COMPLETED == "completed"
    assert TaskState.COMPLETED == "SUCCESS"
    assert TaskState.COMPLETED == TaskStatus.SUCCESS

    assert TaskState.FAILED == "FAILED"
    assert TaskState.FAILED == "failed"
    assert TaskState.FAILED == "FAILURE"
    assert TaskState.FAILED == "ERROR"
    assert TaskState.FAILED == TaskStatus.FAILURE
    assert TaskState.FAILED == TaskStatus.ERROR

    assert TaskState.CANCELLED == "CANCELLED"
    assert TaskState.CANCELLED == "cancelled"
    assert TaskState.CANCELLED == TaskStatus.CANCELLED

    assert TaskState.COMPLETED.is_terminal is True
    assert TaskState.FAILED.is_terminal is True
    assert TaskState.CANCELLED.is_terminal is True
    assert TaskState.PENDING.is_terminal is False
    assert TaskState.RUNNING.is_terminal is False


def test_local_task_execution_example_script() -> None:
    """Verify running examples/local_task_execution.py as a standalone script."""
    import subprocess
    import sys
    from pathlib import Path

    script_path = Path(__file__).resolve().parent.parent / "examples" / "local_task_execution.py"
    result = subprocess.run(
        [sys.executable, str(script_path), "--concurrency", "2"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "MeshWeaver: Asynchronous Task Execution Engine Demonstration" in result.stdout
    assert "Demonstration completed with orderly engine shutdown." in result.stdout

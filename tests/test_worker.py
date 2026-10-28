"""
Comprehensive unit and integration tests for Week 3, Milestone 1:
Asynchronous Task Worker Execution Engine (TaskWorker, TaskEnvelope, TaskResult, TaskLifecycleState).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
import pytest

from meshweaver import (
    DEFAULT_WORKER_CONCURRENCY,
    DEFAULT_WORKER_TIMEOUT,
    InvalidTaskError,
    Message,
    MessageType,
    PeerNode,
    TaskEnvelope,
    TaskErrorInfo,
    TaskExecutionError,
    TaskLifecycleState,
    TaskPayloadLimitError,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskResult,
    TaskState,
    TaskStatus,
    TaskTimeoutError,
    TaskWorker,
    TaskWorkerRecord,
    UnknownTaskError,
    Worker,
    WorkerTaskState,
)


# ============================================================================
# 1. TaskLifecycleState Tests
# ============================================================================


def test_task_lifecycle_states() -> None:
    """Verify all required lifecycle states and their properties."""
    # Required states: pending, running, succeeded, failed
    assert TaskLifecycleState.PENDING == "pending"
    assert TaskLifecycleState.RUNNING == "running"
    assert TaskLifecycleState.SUCCEEDED == "succeeded"
    assert TaskLifecycleState.FAILED == "failed"
    assert TaskLifecycleState.CANCELLED == "cancelled"

    # Terminal vs active properties
    assert TaskLifecycleState.PENDING.is_terminal is False
    assert TaskLifecycleState.RUNNING.is_terminal is False
    assert TaskLifecycleState.SUCCEEDED.is_terminal is True
    assert TaskLifecycleState.FAILED.is_terminal is True
    assert TaskLifecycleState.CANCELLED.is_terminal is True

    assert TaskLifecycleState.PENDING.is_active is True
    assert TaskLifecycleState.RUNNING.is_active is True
    assert TaskLifecycleState.SUCCEEDED.is_active is False
    assert TaskLifecycleState.FAILED.is_active is False

    # String comparison (case-insensitive and compatibility aliases)
    assert TaskLifecycleState.SUCCEEDED == "SUCCEEDED"
    assert TaskLifecycleState.SUCCEEDED == "succeeded"
    assert TaskLifecycleState.SUCCEEDED == "COMPLETED"
    assert TaskLifecycleState.SUCCEEDED == "completed"
    assert TaskLifecycleState.SUCCEEDED == "SUCCESS"
    assert TaskLifecycleState.SUCCEEDED == "success"

    assert TaskLifecycleState.FAILED == "FAILED"
    assert TaskLifecycleState.FAILED == "failed"
    assert TaskLifecycleState.FAILED == "FAILURE"
    assert TaskLifecycleState.FAILED == "failure"
    assert TaskLifecycleState.FAILED == "ERROR"

    # Compatibility with TaskState and TaskStatus
    assert TaskLifecycleState.SUCCEEDED == TaskState.COMPLETED
    assert TaskLifecycleState.FAILED == TaskState.FAILED
    assert TaskLifecycleState.PENDING == TaskState.PENDING
    assert TaskLifecycleState.RUNNING == TaskState.RUNNING
    assert TaskLifecycleState.CANCELLED == TaskState.CANCELLED

    assert TaskLifecycleState.SUCCEEDED == TaskStatus.SUCCESS
    assert TaskLifecycleState.FAILED == TaskStatus.FAILURE

    # Enum missing mapping
    assert TaskLifecycleState("completed") == TaskLifecycleState.SUCCEEDED
    assert TaskLifecycleState("success") == TaskLifecycleState.SUCCEEDED
    assert TaskLifecycleState("failure") == TaskLifecycleState.FAILED
    assert TaskLifecycleState("error") == TaskLifecycleState.FAILED


# ============================================================================
# 2. TaskEnvelope Validation and Serialization Tests
# ============================================================================


def test_task_envelope_creation_and_defaults() -> None:
    """Verify default envelope fields, auto-generated task_id, and serialization."""
    env = TaskEnvelope(task_name="add", args=[10, 20], kwargs={"extra": 1})
    assert env.task_name == "add"
    assert env.args == [10, 20]
    assert env.kwargs == {"extra": 1}
    assert env.task_id.startswith("task-")
    assert env.timeout is None
    assert env.metadata == {}

    # To and from dict
    d = env.to_dict()
    assert d["task_name"] == "add"
    assert d["args"] == [10, 20]
    assert d["task_id"] == env.task_id

    reconstructed = TaskEnvelope.from_dict(d)
    assert reconstructed.task_id == env.task_id
    assert reconstructed.task_name == "add"
    assert reconstructed.args == [10, 20]

    # To and from JSON
    j = env.to_json()
    from_j = TaskEnvelope.from_json(j)
    assert from_j.task_id == env.task_id
    assert from_j.task_name == env.task_name


def test_task_envelope_to_request_and_message() -> None:
    """Verify envelope conversion to and from TaskRequest and Message."""
    env = TaskEnvelope(
        task_name="reverse_string",
        args=["MeshWeaver"],
        task_id="custom-task-001",
        timeout=12.5,
        metadata={"priority": "high"},
    )
    req = env.to_request()
    assert isinstance(req, TaskRequest)
    assert req.task_id == "custom-task-001"
    assert req.task_name == "reverse_string"
    assert req.args == ["MeshWeaver"]
    assert req.metadata["timeout"] == 12.5

    re_env = TaskEnvelope.from_request(req)
    assert re_env.task_id == env.task_id
    assert re_env.timeout == 12.5
    assert re_env.metadata["priority"] == "high"

    msg = env.to_message(sender_id="node-a", recipient_id="worker-b")
    assert isinstance(msg, Message)
    assert msg.message_type == MessageType.TASK_REQUEST
    assert msg.payload["task_id"] == "custom-task-001"

    from_msg = TaskEnvelope.from_message(msg)
    assert from_msg.task_id == env.task_id
    assert from_msg.task_name == "reverse_string"


def test_task_envelope_rejection_security() -> None:
    """Verify strict validation rejects invalid task IDs, reserved names, and dangerous keys."""
    # 1. Invalid task IDs
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", task_id="")
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", task_id="bad id with spaces!")
    with pytest.raises(TaskPayloadLimitError):
        TaskEnvelope(task_name="add", task_id="x" * 200)

    # 2. Dangerous and reserved task names
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="eval", args=["1+1"])
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="exec", args=["import os"])
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="os.system", args=["whoami"])
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="sys.exit")
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="__import__")
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="subprocess")

    # 3. Forbidden keys in kwargs
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", kwargs={"code": "evil()"})
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", kwargs={"__class__": "foo"})
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", kwargs={"import": "malicious"})

    # 4. Oversized payload (limit is 64 KB)
    with pytest.raises(TaskPayloadLimitError):
        TaskEnvelope(task_name="echo", args=["A" * (70 * 1024)])

    # 5. Invalid timeout
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", timeout=-1.0)
    with pytest.raises(InvalidTaskError):
        TaskEnvelope(task_name="add", timeout=0)


# ============================================================================
# 3. TaskResult Tests
# ============================================================================


def test_task_result_success_and_failure() -> None:
    """Verify TaskResult factories, states, and serialization."""
    # Success
    res = TaskResult.success(
        task_id="task-123",
        task_name="add",
        result=42,
        execution_time=0.005,
    )
    assert res.is_success is True
    assert res.is_failure is False
    assert res.status == TaskLifecycleState.SUCCEEDED
    assert res.result == 42
    assert res.duration == 0.005

    d = res.to_dict()
    assert d["status"] == "succeeded"
    assert d["result"] == 42
    assert d["error"] is None

    proto_resp = res.to_response()
    assert isinstance(proto_resp, TaskResponse)
    assert proto_resp.status == TaskStatus.SUCCESS
    assert proto_resp.result == 42

    # Failure
    fail_res = TaskResult.failure(
        task_id="task-456",
        task_name="divide",
        error=TaskErrorInfo(code="DIV_ZERO", message="Division by zero"),
        execution_time=0.002,
    )
    assert fail_res.is_success is False
    assert fail_res.is_failure is True
    assert fail_res.status == TaskLifecycleState.FAILED
    assert fail_res.error.code == "DIV_ZERO"

    fail_resp = fail_res.to_response()
    assert fail_resp.status == TaskStatus.FAILURE
    assert fail_resp.error.code == "DIV_ZERO"


# ============================================================================
# 4. Asynchronous TaskWorker Execution Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_executes_builtin_tasks() -> None:
    """Verify TaskWorker executes built-in registered tasks (add, reverse_string, echo, ping)."""
    async with TaskWorker(concurrency=2) as worker:
        # 1. add
        env_add = TaskEnvelope(task_name="add", args=[15, 27])
        res_add = await worker.execute_envelope(env_add)
        assert res_add.is_success is True
        assert res_add.status == TaskLifecycleState.SUCCEEDED
        assert res_add.result == 42
        assert res_add.duration is not None and res_add.duration >= 0.0

        # 2. reverse_string
        res_rev = await worker.execute("reverse_string", "MeshWeaver")
        assert res_rev.is_success is True
        assert res_rev.result == "revaeWhseM"

        # 3. echo
        res_echo = await worker.execute("echo", "data", key="value")
        assert res_echo.is_success is True
        assert res_echo.result == {"args": ["data"], "kwargs": {"key": "value"}}

        # 4. ping
        res_ping = await worker.execute("ping")
        assert res_ping.is_success is True
        assert res_ping.result == "pong"


@pytest.mark.asyncio
async def test_worker_custom_async_and_sync_tasks() -> None:
    """Verify worker registers and executes both async coroutines and sync functions."""
    async with TaskWorker(concurrency=3) as worker:
        # Register async coroutine task
        @worker.register("async_square")
        async def async_square(x: int) -> int:
            await asyncio.sleep(0.01)
            return x * x

        # Register sync task (runs safely in thread without blocking event loop)
        @worker.register("sync_upper")
        def sync_upper(text: str) -> str:
            return text.upper()

        rec1 = await worker.submit("async_square", 9)
        rec2 = await worker.submit("sync_upper", "distributed worker")

        res1 = await rec1.wait()
        res2 = await rec2.wait()

        assert res1.is_success is True
        assert res1.result == 81
        assert res1.status == TaskLifecycleState.SUCCEEDED

        assert res2.is_success is True
        assert res2.result == "DISTRIBUTED WORKER"
        assert res2.status == TaskLifecycleState.SUCCEEDED


@pytest.mark.asyncio
async def test_worker_task_lifecycle_transitions() -> None:
    """Verify explicit lifecycle transitions: PENDING -> RUNNING -> SUCCEEDED."""
    worker = TaskWorker(concurrency=1, auto_start=False)
    state_snapshots: list[TaskLifecycleState] = []

    @worker.register("lifecycle_probe")
    async def probe() -> str:
        state_snapshots.append(record.state)
        await asyncio.sleep(0.02)
        return "done"

    record = await worker.submit("lifecycle_probe")
    assert record.state == TaskLifecycleState.PENDING
    assert record.is_pending is True

    # Start worker and await completion
    worker.start()
    res = await record.wait()

    assert record.state == TaskLifecycleState.SUCCEEDED
    assert record.is_succeeded is True
    assert res.is_success is True
    # Recorded state inside probe handler should have been RUNNING
    assert state_snapshots == [TaskLifecycleState.RUNNING]

    await worker.shutdown()


# ============================================================================
# 5. Execution Timeouts Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_envelope_timeout_enforcement() -> None:
    """Verify task exceeding its per-envelope timeout is cancelled and marked FAILED."""
    async with TaskWorker(concurrency=2) as worker:
        @worker.register("slow_task")
        async def slow_task(duration: float) -> str:
            await asyncio.sleep(duration)
            return "finished"

        # Specify timeout of 0.03s, task takes 0.5s
        env = TaskEnvelope(task_name="slow_task", args=[0.5], timeout=0.03)
        res = await worker.execute_envelope(env)

        assert res.is_failure is True
        assert res.status == TaskLifecycleState.FAILED
        assert res.error is not None
        assert res.error.code == "TASK_TIMEOUT"
        assert "exceeded execution timeout" in res.error.message

        # Verify worker is still healthy for subsequent tasks
        res_ok = await worker.execute("add", 10, 20)
        assert res_ok.is_success is True
        assert res_ok.result == 30


@pytest.mark.asyncio
async def test_worker_default_timeout_enforcement() -> None:
    """Verify worker-level default timeout applies when envelope does not set timeout."""
    async with TaskWorker(concurrency=2, default_timeout=0.04) as worker:
        @worker.register("hanging_task")
        async def hanging_task() -> None:
            await asyncio.sleep(1.0)

        res = await worker.execute("hanging_task")
        assert res.is_failure is True
        assert res.status == TaskLifecycleState.FAILED
        assert res.error.code == "TASK_TIMEOUT"


# ============================================================================
# 6. Exception Handling and Error Boundaries Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_catches_handler_exceptions() -> None:
    """Verify synchronous and asynchronous exceptions are caught safely as structured errors."""
    async with TaskWorker(concurrency=2) as worker:
        @worker.register("failing_sync")
        def failing_sync() -> None:
            raise ZeroDivisionError("division by zero in worker")

        @worker.register("failing_async")
        async def failing_async() -> None:
            await asyncio.sleep(0.01)
            raise ValueError("invalid parameter provided")

        res_sync = await worker.execute("failing_sync")
        assert res_sync.is_failure is True
        assert res_sync.status == TaskLifecycleState.FAILED
        assert res_sync.error.code == "ZeroDivisionError"
        assert "division by zero in worker" in res_sync.error.message

        res_async = await worker.execute("failing_async")
        assert res_async.is_failure is True
        assert res_async.status == TaskLifecycleState.FAILED
        assert res_async.error.code == "ValueError"
        assert "invalid parameter provided" in res_async.error.message

        # Worker still running and accepting tasks
        res_add = await worker.execute("add", 5, 5)
        assert res_add.is_success is True
        assert res_add.result == 10


@pytest.mark.asyncio
async def test_worker_rejects_non_serializable_result() -> None:
    """Verify task returning a non-JSON-serializable object transitions to FAILED with INVALID_RESULT."""
    async with TaskWorker(concurrency=2) as worker:
        @worker.register("return_lambda")
        def return_lambda() -> Any:
            return lambda x: x  # Not JSON serializable

        res = await worker.execute("return_lambda")
        assert res.is_failure is True
        assert res.status == TaskLifecycleState.FAILED
        assert res.error.code == "INVALID_RESULT"


# ============================================================================
# 7. Security Boundaries & Untrusted Code Prevention Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_rejects_unregistered_task() -> None:
    """Verify untrusted/unregistered task names are blocked immediately without execution."""
    async with TaskWorker(concurrency=2) as worker:
        res = await worker.execute("unregistered_malicious_task")
        assert res.is_failure is True
        assert res.status == TaskLifecycleState.FAILED
        assert res.error.code == "UNKNOWN_TASK"
        assert "not registered" in res.error.message


@pytest.mark.asyncio
async def test_worker_rejects_arbitrary_code_injection() -> None:
    """Verify worker rejects attempts to pass reserved names or malformed payloads."""
    async with TaskWorker(concurrency=2) as worker:
        # 1. Attempting to execute eval/exec via envelope dictionary
        bad_envelope = {
            "task_id": "attack-1",
            "task_name": "eval",
            "args": ["1 + 1"],
        }
        res = await worker.execute_envelope(bad_envelope)
        assert res.is_failure is True
        assert res.status == TaskLifecycleState.FAILED
        assert res.error.code == "INVALID_TASK_REQUEST"

        # 2. Attempting to inject forbidden dictionary keys
        bad_kwargs_envelope = {
            "task_id": "attack-2",
            "task_name": "add",
            "kwargs": {"__import__": "os"},
        }
        res2 = await worker.execute_envelope(bad_kwargs_envelope)
        assert res2.is_failure is True
        assert res2.status == TaskLifecycleState.FAILED


# ============================================================================
# 8. Concurrency Limit Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_concurrency_limit() -> None:
    """Verify configured concurrency limit strictly bounds concurrent task executions."""
    concurrency_limit = 2
    active_concurrent = 0
    max_observed_concurrent = 0
    lock = asyncio.Lock()

    async with TaskWorker(concurrency=concurrency_limit) as worker:
        @worker.register("probe_concurrency")
        async def probe_concurrency() -> None:
            nonlocal active_concurrent, max_observed_concurrent
            async with lock:
                active_concurrent += 1
                if active_concurrent > max_observed_concurrent:
                    max_observed_concurrent = active_concurrent

            await asyncio.sleep(0.04)

            async with lock:
                active_concurrent -= 1

        # Submit 6 tasks
        records = [await worker.submit("probe_concurrency") for _ in range(6)]
        await asyncio.gather(*(r.wait() for r in records))

        assert max_observed_concurrent <= concurrency_limit
        assert worker.succeeded_count == 6


def test_worker_invalid_concurrency_raises() -> None:
    """Verify invalid concurrency limits raise ValueError."""
    with pytest.raises(ValueError):
        TaskWorker(concurrency=0)
    with pytest.raises(ValueError):
        TaskWorker(concurrency=-1)
    with pytest.raises(ValueError):
        TaskWorker(concurrency="many")  # type: ignore


# ============================================================================
# 9. Task Cancellation and Worker Shutdown Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_cancel_pending_task() -> None:
    """Verify a pending task can be cancelled before execution begins."""
    # Worker with concurrency=1 paused
    worker = TaskWorker(concurrency=1, auto_start=False)

    @worker.register("noop")
    async def noop() -> None:
        pass

    rec = await worker.submit("noop")
    assert rec.state == TaskLifecycleState.PENDING

    cancelled = await worker.cancel_task(rec.task_id)
    assert cancelled is True
    assert rec.state == TaskLifecycleState.CANCELLED
    assert rec.is_cancelled is True

    await worker.shutdown()


@pytest.mark.asyncio
async def test_worker_cancel_running_task() -> None:
    """Verify a running task can be cancelled during execution."""
    async with TaskWorker(concurrency=2) as worker:
        @worker.register("long_run")
        async def long_run() -> None:
            await asyncio.sleep(2.0)

        rec = await worker.submit("long_run")
        await asyncio.sleep(0.02)  # Give time to transition to RUNNING

        assert rec.state == TaskLifecycleState.RUNNING
        cancelled = await worker.cancel_task(rec.task_id)
        assert cancelled is True

        res = await rec.wait()
        assert res.status == TaskLifecycleState.CANCELLED
        assert res.is_cancelled is True


@pytest.mark.asyncio
async def test_worker_shutdown_cancel_pending() -> None:
    """Verify shutdown with cancel_pending=True cancels queued tasks."""
    worker = TaskWorker(concurrency=1, auto_start=False)

    @worker.register("slow")
    async def slow() -> None:
        await asyncio.sleep(0.05)

    rec1 = await worker.submit("slow")
    rec2 = await worker.submit("slow")

    # Shutdown with cancel_pending
    await worker.shutdown(wait=False, cancel_pending=True)

    assert rec1.state == TaskLifecycleState.CANCELLED
    assert rec2.state == TaskLifecycleState.CANCELLED


# ============================================================================
# 10. Protocol Message & PeerNode Integration Tests
# ============================================================================


@pytest.mark.asyncio
async def test_worker_handles_protocol_message() -> None:
    """Verify handle_message processes TASK_REQUEST and returns valid TASK_RESPONSE Message."""
    async with TaskWorker(concurrency=2) as worker:
        req = TaskRequest.create(task_name="add", args=[40, 2])
        msg = req.to_message(sender_id="client-node")

        resp_msg = await worker.handle_message(msg)
        assert resp_msg.message_type == MessageType.TASK_RESPONSE
        assert resp_msg.payload["status"] == "success"
        assert resp_msg.payload["result"] == 42


@pytest.mark.asyncio
async def test_peernode_exposes_worker() -> None:
    """Verify PeerNode exposes worker instance with shared task registry."""
    node = PeerNode(peer_id="worker-test-node")
    assert hasattr(node, "worker")
    assert isinstance(node.worker, TaskWorker)
    assert node.worker.registry is node.task_registry
    assert node.worker.has_task("add") is True

"""
Asynchronous Task Worker execution engine for local and distributed task processing.

Week 3, Milestone 1 implementation:
- Accepts valid task envelopes (TaskEnvelope, TaskRequest, Message, or dict).
- Validates task IDs, task names, argument structures, and payload limits (64 KB).
- Executes supported tasks asynchronously where appropriate (async coroutines directly,
  synchronous handlers dispatched safely via threads without blocking the event loop).
- Returns structured success or failure results (TaskResult, TaskResponse).
- Introduces strict task lifecycle states: PENDING -> RUNNING -> SUCCEEDED / FAILED / CANCELLED.
- Enforces execution timeouts, exception boundaries, and configurable concurrency limits.
- Enforces strict security: never allows arbitrary untrusted code execution (no pickle,
  no dynamic imports, no eval/exec, and only explicitly registered tasks execute).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
import json
import logging
import time
from typing import Any
import uuid

from meshweaver.exceptions import (
    InvalidTaskError,
    TaskError,
    TaskExecutionError,
    TaskPayloadLimitError,
    TaskTimeoutError,
    UnknownTaskError,
    ValidationError,
)
from meshweaver.protocol import Message, MessageType
from meshweaver.tasks import (
    FORBIDDEN_KEYS,
    MAX_TASK_ARGS_COUNT,
    MAX_TASK_ID_LENGTH,
    MAX_TASK_KWARGS_COUNT,
    MAX_TASK_NAME_LENGTH,
    MAX_TASK_PAYLOAD_SIZE,
    MAX_TASK_RESULT_SIZE,
    RESERVED_TASK_NAMES,
    TaskErrorInfo,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskStatus,
    _check_json_value,
    validate_args,
    validate_kwargs,
    validate_payload_size,
    validate_task_id,
    validate_task_name,
)

logger = logging.getLogger(__name__)

DEFAULT_WORKER_CONCURRENCY: int = 4
DEFAULT_WORKER_TIMEOUT: float = 30.0


class TaskLifecycleState(StrEnum):
    """
    Lifecycle states for the asynchronous task worker.

    Deterministic transitions:
        PENDING -> RUNNING -> SUCCEEDED
                           -> FAILED
                           -> CANCELLED
    """

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"

    @property
    def is_terminal(self) -> bool:
        """Return True if this state is a terminal/final state."""
        return self in (
            TaskLifecycleState.SUCCEEDED,
            TaskLifecycleState.FAILED,
            TaskLifecycleState.CANCELLED,
        )

    @property
    def is_active(self) -> bool:
        """Return True if this task is pending or currently executing."""
        return self in (TaskLifecycleState.PENDING, TaskLifecycleState.RUNNING)

    @property
    def is_succeeded(self) -> bool:
        """Return True if this task completed successfully."""
        return self == TaskLifecycleState.SUCCEEDED

    @property
    def is_failed(self) -> bool:
        """Return True if this task failed with an error."""
        return self == TaskLifecycleState.FAILED

    @property
    def is_cancelled(self) -> bool:
        """Return True if this task was cancelled."""
        return self == TaskLifecycleState.CANCELLED

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TaskLifecycleState):
            return self.value == other.value
        if isinstance(other, str):
            val_lower = other.lower()
            if self.value == val_lower:
                return True
            # Compatibility with TaskStatus and TaskState
            if self == TaskLifecycleState.SUCCEEDED and val_lower in (
                "succeeded",
                "success",
                "completed",
            ):
                return True
            if self == TaskLifecycleState.FAILED and val_lower in (
                "failed",
                "failure",
                "error",
            ):
                return True
            if self == TaskLifecycleState.CANCELLED and val_lower == "cancelled":
                return True
            return False
        if hasattr(other, "value") and isinstance(other.value, str):
            return self.__eq__(other.value)
        return False

    def __hash__(self) -> int:
        return hash(self.value)

    @classmethod
    def _missing_(cls, value: object) -> Any:
        if isinstance(value, str):
            val_lower = value.lower()
            for member in cls:
                if member.value == val_lower or member.name.lower() == val_lower:
                    return member
            if val_lower in ("success", "completed"):
                return cls.SUCCEEDED
            if val_lower in ("failure", "error"):
                return cls.FAILED
        return super()._missing_(value)


# Alias for backward compatibility
WorkerTaskState = TaskLifecycleState


@dataclass(slots=True)
class TaskEnvelope:
    """
    Validated task envelope accepted by the worker execution engine.

    Encapsulates task identification, task name, arguments, keyword arguments,
    optional execution timeout, and metadata. Enforces strict input validation,
    safe naming (blocking arbitrary remote code execution), and payload limits.
    """

    task_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    task_id: str = field(default_factory=lambda: f"task-{uuid.uuid4().hex}")
    timeout: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """
        Validate task ID, task name, argument safety, execution timeout, and payload size.

        Raises:
            InvalidTaskError: If fields fail schema or security boundaries.
            TaskPayloadLimitError: If any parameter or overall payload exceeds size limits.
        """
        validate_task_id(self.task_id)
        validate_task_name(self.task_name)
        validate_args(self.args)
        validate_kwargs(self.kwargs)

        if self.timeout is not None:
            if (
                isinstance(self.timeout, bool)
                or not isinstance(self.timeout, (int, float))
                or self.timeout <= 0
            ):
                raise InvalidTaskError(
                    f"Execution timeout must be a positive number, got {self.timeout!r}"
                )

        if not isinstance(self.metadata, dict):
            raise InvalidTaskError("Field 'metadata' must be a dictionary")
        _check_json_value(self.metadata, current_depth=1)

        # Enforce 64 KB total payload limit
        validate_payload_size(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        """Convert task envelope to a JSON-compatible dictionary."""
        d: dict[str, Any] = {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "metadata": self.metadata,
        }
        if self.timeout is not None:
            d["timeout"] = self.timeout
        return d

    def to_json(self) -> str:
        """Serialize task envelope to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    def to_request(self) -> TaskRequest:
        """Convert this envelope to a validated protocol TaskRequest."""
        meta = dict(self.metadata)
        if self.timeout is not None:
            meta["timeout"] = self.timeout
        return TaskRequest.create(
            task_name=self.task_name,
            args=self.args,
            kwargs=self.kwargs,
            task_id=self.task_id,
            metadata=meta,
        )

    def to_message(
        self,
        sender_id: str,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Message:
        """Convert this envelope to a protocol Message of type TASK_REQUEST."""
        return Message.create(
            MessageType.TASK_REQUEST,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id or self.task_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_dict(cls, data: Any) -> TaskEnvelope:
        """
        Construct and validate a TaskEnvelope from a dictionary.

        Raises:
            InvalidTaskError: If required fields are missing or malformed.
        """
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dict for TaskEnvelope, got {type(data).__name__}")

        raw_id = data.get("task_id")
        task_id = str(raw_id) if raw_id is not None else f"task-{uuid.uuid4().hex}"

        task_name = data.get("task_name") if "task_name" in data else data.get("task_type")
        if task_name is None:
            raise InvalidTaskError("Missing required field 'task_name' or 'task_type' in envelope")

        raw_args = data.get("args", [])
        raw_kwargs = data.get("kwargs") if "kwargs" in data else data.get("params", {})
        raw_metadata = data.get("metadata", {})
        raw_timeout = data.get("timeout")

        return cls(
            task_id=task_id,
            task_name=str(task_name),
            args=list(raw_args) if isinstance(raw_args, (list, tuple)) else raw_args,
            kwargs=dict(raw_kwargs) if isinstance(raw_kwargs, dict) else raw_kwargs,
            timeout=float(raw_timeout) if raw_timeout is not None else None,
            metadata=dict(raw_metadata) if isinstance(raw_metadata, dict) else raw_metadata,
        )

    @classmethod
    def from_json(cls, json_str: str) -> TaskEnvelope:
        """Construct and validate a TaskEnvelope from a JSON string."""
        if not isinstance(json_str, str):
            raise InvalidTaskError(f"Input must be a string, got {type(json_str).__name__}")
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise InvalidTaskError(f"Malformed JSON in TaskEnvelope: {exc.msg}") from exc
        return cls.from_dict(data)

    @classmethod
    def from_request(cls, request: TaskRequest, timeout: float | None = None) -> TaskEnvelope:
        """Construct a TaskEnvelope from a protocol TaskRequest."""
        req_timeout = timeout
        if req_timeout is None and isinstance(request.metadata, dict):
            req_timeout = request.metadata.get("timeout")
        return cls(
            task_id=request.task_id,
            task_name=request.task_name,
            args=list(request.args),
            kwargs=dict(request.kwargs),
            timeout=float(req_timeout) if req_timeout is not None else None,
            metadata=dict(request.metadata),
        )

    @classmethod
    def from_message(cls, message: Message) -> TaskEnvelope:
        """Construct a TaskEnvelope from a protocol Message."""
        if message.message_type != MessageType.TASK_REQUEST:
            raise InvalidTaskError(
                f"Expected MessageType.TASK_REQUEST, got {message.message_type}"
            )
        return cls.from_dict(message.payload)


@dataclass(slots=True)
class TaskResult:
    """
    Structured execution result returned by the worker execution engine.

    Encapsulates task identification, task name, lifecycle state,
    computed result or structured error info, duration, timestamps, and metadata.
    """

    task_id: str
    task_name: str
    status: TaskLifecycleState
    result: Any = None
    error: TaskErrorInfo | None = None
    execution_time: float | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        """Return True if task executed successfully."""
        return self.status == TaskLifecycleState.SUCCEEDED

    @property
    def is_failure(self) -> bool:
        """Return True if task failed with an error or exception."""
        return self.status == TaskLifecycleState.FAILED

    @property
    def is_cancelled(self) -> bool:
        """Return True if task was cancelled."""
        return self.status == TaskLifecycleState.CANCELLED

    @property
    def duration(self) -> float | None:
        """Elapsed execution time in seconds."""
        if self.execution_time is not None:
            return self.execution_time
        if self.started_at is not None and self.completed_at is not None:
            return max(0.0, self.completed_at - self.started_at)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert task result to a structured, JSON-compatible dictionary."""
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "status": self.status.value,
            "result": self.result,
            "error": self.error.to_dict() if self.error is not None else None,
            "execution_time": self.execution_time,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        """Serialize task result to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    def to_response(self) -> TaskResponse:
        """Convert this result into a validated wire-protocol TaskResponse."""
        if self.status == TaskLifecycleState.SUCCEEDED:
            return TaskResponse.success(
                task_id=self.task_id,
                result=self.result,
                execution_time=self.execution_time,
                metadata=self.metadata,
            )
        elif self.status == TaskLifecycleState.FAILED:
            err = self.error or TaskErrorInfo(
                code="TASK_FAILED",
                message="Task execution failed",
            )
            return TaskResponse.failure(
                task_id=self.task_id,
                error=err,
                execution_time=self.execution_time,
                metadata=self.metadata,
            )
        else:
            return TaskResponse(
                task_id=self.task_id,
                status=TaskStatus.CANCELLED,
                result=None,
                error=self.error or TaskErrorInfo(code="TASK_CANCELLED", message="Task cancelled"),
                execution_time=self.execution_time,
                metadata=self.metadata,
            )

    @classmethod
    def success(
        cls,
        task_id: str,
        task_name: str,
        result: Any = None,
        *,
        execution_time: float | None = None,
        created_at: float | None = None,
        started_at: float | None = None,
        completed_at: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Factory method for constructing a successful TaskResult."""
        now = time.time()
        return cls(
            task_id=task_id,
            task_name=task_name,
            status=TaskLifecycleState.SUCCEEDED,
            result=result,
            error=None,
            execution_time=execution_time,
            created_at=created_at if created_at is not None else now,
            started_at=started_at,
            completed_at=completed_at if completed_at is not None else now,
            metadata=dict(metadata) if metadata is not None else {},
        )

    @classmethod
    def failure(
        cls,
        task_id: str,
        task_name: str,
        error: TaskErrorInfo | dict[str, Any] | str,
        *,
        execution_time: float | None = None,
        created_at: float | None = None,
        started_at: float | None = None,
        completed_at: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Factory method for constructing a failed TaskResult."""
        now = time.time()
        if isinstance(error, TaskErrorInfo):
            err_info = error
        elif isinstance(error, dict):
            err_info = TaskErrorInfo.from_dict(error)
        else:
            err_info = TaskErrorInfo(code="TASK_FAILED", message=str(error))

        return cls(
            task_id=task_id,
            task_name=task_name,
            status=TaskLifecycleState.FAILED,
            result=None,
            error=err_info,
            execution_time=execution_time,
            created_at=created_at if created_at is not None else now,
            started_at=started_at,
            completed_at=completed_at if completed_at is not None else now,
            metadata=dict(metadata) if metadata is not None else {},
        )

    @classmethod
    def from_exception(
        cls,
        task_id: str,
        task_name: str,
        exc: BaseException,
        *,
        execution_time: float | None = None,
        created_at: float | None = None,
        started_at: float | None = None,
        completed_at: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> TaskResult:
        """Safely construct a failed TaskResult from an exception without leaking host internals."""
        now = time.time()
        if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            err = TaskErrorInfo(
                code="TASK_TIMEOUT",
                message=f"Task '{task_name}' timed out during execution: {exc}",
            )
        elif isinstance(exc, UnknownTaskError):
            err = TaskErrorInfo(code="UNKNOWN_TASK", message=str(exc))
        elif isinstance(exc, TaskPayloadLimitError):
            err = TaskErrorInfo(code="PAYLOAD_LIMIT_EXCEEDED", message=str(exc))
        elif isinstance(exc, (InvalidTaskError, ValidationError)):
            err = TaskErrorInfo(code="INVALID_TASK_REQUEST", message=str(exc))
        elif isinstance(exc, Exception):
            err = TaskErrorInfo.from_exception(exc)
        else:
            err = TaskErrorInfo(code="FATAL_ERROR", message=f"{type(exc).__name__}: {exc}")

        return cls(
            task_id=task_id,
            task_name=task_name,
            status=TaskLifecycleState.FAILED,
            result=None,
            error=err,
            execution_time=execution_time,
            created_at=created_at if created_at is not None else now,
            started_at=started_at,
            completed_at=completed_at if completed_at is not None else now,
            metadata=dict(metadata) if metadata is not None else {},
        )


@dataclass
class TaskWorkerRecord:
    """
    Mutable state and tracking record for a task submitted to TaskWorker.

    Maintains lifecycle state transitions (PENDING -> RUNNING -> SUCCEEDED/FAILED/CANCELLED),
    timestamps, future completion handles, and final TaskResult.
    """

    envelope: TaskEnvelope
    state: TaskLifecycleState = TaskLifecycleState.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    execution_time: float | None = None
    result: Any = None
    error: TaskErrorInfo | None = None
    exception: BaseException | None = None

    _future: asyncio.Future[TaskResult] | None = field(
        default=None, repr=False, compare=False
    )

    @property
    def task_id(self) -> str:
        return self.envelope.task_id

    @property
    def task_name(self) -> str:
        return self.envelope.task_name

    @property
    def is_done(self) -> bool:
        return self.state.is_terminal

    @property
    def is_succeeded(self) -> bool:
        return self.state == TaskLifecycleState.SUCCEEDED

    @property
    def is_failed(self) -> bool:
        return self.state == TaskLifecycleState.FAILED

    @property
    def is_running(self) -> bool:
        return self.state == TaskLifecycleState.RUNNING

    @property
    def is_pending(self) -> bool:
        return self.state == TaskLifecycleState.PENDING

    @property
    def is_cancelled(self) -> bool:
        return self.state == TaskLifecycleState.CANCELLED

    @property
    def duration(self) -> float | None:
        if self.execution_time is not None:
            return self.execution_time
        if self.started_at is not None:
            if self.completed_at is not None:
                return max(0.0, self.completed_at - self.started_at)
            return max(0.0, time.time() - self.started_at)
        return None

    def _get_or_create_future(self) -> asyncio.Future[TaskResult]:
        if self._future is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
            self._future = loop.create_future()
        return self._future

    def _complete(self, task_result: TaskResult) -> None:
        self.state = task_result.status
        self.result = task_result.result
        self.error = task_result.error
        self.started_at = task_result.started_at or self.started_at
        self.completed_at = task_result.completed_at or time.time()
        self.execution_time = task_result.execution_time or self.duration

        fut = self._get_or_create_future()
        if not fut.done():
            fut.set_result(task_result)

    def _cancel(self, message: str = "Task was cancelled") -> None:
        self.state = TaskLifecycleState.CANCELLED
        self.completed_at = time.time()
        self.execution_time = self.duration
        self.error = TaskErrorInfo(code="TASK_CANCELLED", message=message)
        canc_exc = asyncio.CancelledError(message)
        self.exception = canc_exc

        res = TaskResult(
            task_id=self.task_id,
            task_name=self.task_name,
            status=TaskLifecycleState.CANCELLED,
            result=None,
            error=self.error,
            execution_time=self.execution_time,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            metadata=self.envelope.metadata,
        )
        fut = self._get_or_create_future()
        if not fut.done():
            fut.set_result(res)

    async def wait(
        self,
        timeout: float | None = None,
        raise_on_error: bool = False,
    ) -> TaskResult:
        """
        Asynchronously await task completion.

        Args:
            timeout: Maximum seconds to wait.
            raise_on_error: If True, raise exception if task failed.

        Returns:
            The final TaskResult.
        """
        fut = self._get_or_create_future()
        if timeout is not None:
            res = await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
        else:
            res = await asyncio.shield(fut)

        if raise_on_error and res.is_failure:
            if self.exception is not None:
                raise self.exception
            msg = res.error.message if res.error else "Task failed"
            raise TaskExecutionError(f"Task '{self.task_name}' ({self.task_id}) failed: {msg}")

        return res

    def to_result(self) -> TaskResult:
        """Convert current record to a TaskResult snapshot."""
        return TaskResult(
            task_id=self.task_id,
            task_name=self.task_name,
            status=self.state,
            result=self.result,
            error=self.error,
            execution_time=self.duration,
            created_at=self.created_at,
            started_at=self.started_at,
            completed_at=self.completed_at,
            metadata=self.envelope.metadata,
        )


class TaskWorker:
    """
    Asynchronous Task Worker Execution Engine.

    Accepts valid task envelopes, validates task IDs and payload limits,
    executes registered tasks asynchronously where appropriate (async coroutines
    directly, sync handlers via threads), and returns structured success or
    failure results.

    Enforces:
    - Task lifecycle states: PENDING -> RUNNING -> SUCCEEDED / FAILED / CANCELLED.
    - Configurable worker concurrency limits (bounded by worker pool and semaphore).
    - Execution timeouts per task and worker default timeout.
    - Exception handling and error boundaries without leaking internals or crashing.
    - Strict security: Arbitrary untrusted code execution is prohibited.
      Only explicitly registered handlers execute. Reserved words, dangerous paths,
      pickle, dynamic imports, and dunders are strictly rejected.
    """

    def __init__(
        self,
        *,
        concurrency: int = DEFAULT_WORKER_CONCURRENCY,
        registry: TaskRegistry | None = None,
        default_timeout: float | None = DEFAULT_WORKER_TIMEOUT,
        max_queue_size: int = 0,
        validate_inputs: bool = True,
        run_sync_in_thread: bool = True,
        auto_start: bool = True,
    ) -> None:
        """
        Initialize the TaskWorker.

        Args:
            concurrency: Maximum number of concurrently executing tasks. Must be >= 1.
            registry: TaskRegistry containing explicitly registered handlers.
            default_timeout: Default execution timeout in seconds (None for unbounded).
            max_queue_size: Maximum pending queue depth (0 for unbounded).
            validate_inputs: Whether to validate envelopes, IDs, and payload limits.
            run_sync_in_thread: Whether to run synchronous callables in worker threads
                               so they do not block the asyncio event loop.
            auto_start: Whether to automatically start worker loop on first submission.
        """
        if (
            not isinstance(concurrency, int)
            or isinstance(concurrency, bool)
            or concurrency < 1
        ):
            raise ValueError(f"concurrency must be an integer >= 1, got {concurrency!r}")
        if (
            not isinstance(max_queue_size, int)
            or isinstance(max_queue_size, bool)
            or max_queue_size < 0
        ):
            raise ValueError(
                f"max_queue_size must be a non-negative integer, got {max_queue_size!r}"
            )
        if default_timeout is not None:
            if (
                isinstance(default_timeout, bool)
                or not isinstance(default_timeout, (int, float))
                or default_timeout <= 0
            ):
                raise ValueError(
                    f"default_timeout must be a positive number or None, got {default_timeout!r}"
                )

        self._concurrency: int = concurrency
        self._registry: TaskRegistry = (
            registry if registry is not None else TaskRegistry.create_default()
        )
        self._default_timeout: float | None = default_timeout
        self._max_queue_size: int = max_queue_size
        self._validate_inputs: bool = validate_inputs
        self._run_sync_in_thread: bool = run_sync_in_thread
        self._auto_start: bool = auto_start

        # State tracking
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
        self._tasks: dict[str, TaskWorkerRecord] = {}
        self._queue: asyncio.Queue[TaskWorkerRecord | None] = asyncio.Queue(
            maxsize=max_queue_size
        )
        self._workers: list[asyncio.Task[None]] = []
        self._running_tasks: dict[str, asyncio.Task[Any]] = {}
        self._shutdown_event: asyncio.Event = asyncio.Event()
        self._is_running: bool = False
        self._is_shutting_down: bool = False
        self._running_count: int = 0

    @property
    def registry(self) -> TaskRegistry:
        """Return the underlying TaskRegistry."""
        return self._registry

    @property
    def concurrency(self) -> int:
        """Return the configured concurrency limit."""
        return self._concurrency

    @property
    def max_concurrency(self) -> int:
        """Alias for concurrency limit."""
        return self._concurrency

    @property
    def default_timeout(self) -> float | None:
        """Return the default per-task execution timeout in seconds."""
        return self._default_timeout

    @property
    def is_running(self) -> bool:
        """Return True if background worker coroutines are running."""
        return self._is_running

    @property
    def is_shutting_down(self) -> bool:
        """Return True if worker shutdown has been initiated."""
        return self._is_shutting_down

    @property
    def pending_count(self) -> int:
        """Count of tasks currently queued in PENDING state."""
        return sum(
            1 for t in self._tasks.values() if t.state == TaskLifecycleState.PENDING
        )

    @property
    def running_count(self) -> int:
        """Count of tasks currently executing in RUNNING state."""
        return self._running_count

    @property
    def succeeded_count(self) -> int:
        """Count of tasks that completed successfully."""
        return sum(
            1 for t in self._tasks.values() if t.state == TaskLifecycleState.SUCCEEDED
        )

    @property
    def completed_count(self) -> int:
        """Alias for succeeded_count."""
        return self.succeeded_count

    @property
    def failed_count(self) -> int:
        """Count of tasks that failed with an error."""
        return sum(
            1 for t in self._tasks.values() if t.state == TaskLifecycleState.FAILED
        )

    @property
    def cancelled_count(self) -> int:
        """Count of tasks that were cancelled."""
        return sum(
            1 for t in self._tasks.values() if t.state == TaskLifecycleState.CANCELLED
        )

    @property
    def active_count(self) -> int:
        """Count of tasks currently pending or actively running."""
        return self.pending_count + self.running_count

    @property
    def total_tasks(self) -> int:
        """Total number of tasks submitted and tracked by this worker."""
        return len(self._tasks)

    def register(
        self,
        name_or_func: Any = None,
        handler: Any = None,
    ) -> Any:
        """
        Register a task handler in this worker's TaskRegistry.

        Can be used as a function or decorator:
            @worker.register("compute")
            async def compute(x: int) -> int:
                ...
        """
        return self._registry.register(name_or_func, handler)

    def unregister(self, name: str) -> bool:
        """Unregister a task handler by name. Returns True if removed."""
        return self._registry.unregister(name)

    def has_task(self, name: str) -> bool:
        """Return True if task name is registered."""
        return self._registry.has_task(name)

    def list_tasks(self) -> list[str]:
        """Return a sorted list of registered task names."""
        return self._registry.list_tasks()

    def get_task(self, task_id: str) -> TaskWorkerRecord | None:
        """Retrieve a tracked TaskWorkerRecord by task_id."""
        return self._tasks.get(task_id)

    def list_tracked_tasks(
        self, state: TaskLifecycleState | str | None = None
    ) -> list[TaskWorkerRecord]:
        """List tracked tasks, optionally filtered by lifecycle state."""
        if state is None:
            return list(self._tasks.values())
        return [t for t in self._tasks.values() if t.state == state]

    def start(self) -> None:
        """Start background worker coroutines. Idempotent if already running."""
        if self._is_running:
            return
        if self._is_shutting_down:
            raise RuntimeError("Cannot start TaskWorker: worker has been shut down")

        self._shutdown_event.clear()
        self._is_running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"meshweaver-task-worker-{i}")
            for i in range(self._concurrency)
        ]
        logger.debug(
            "TaskWorker started with %d workers (concurrency=%d)",
            len(self._workers),
            self._concurrency,
        )

    def coerce_envelope(
        self,
        task_name_or_envelope: TaskEnvelope | TaskRequest | Message | dict[str, Any] | str,
        *args: Any,
        task_id: str | None = None,
        timeout: float | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TaskEnvelope:
        """
        Parse and validate any supported task envelope input format into a TaskEnvelope.

        Raises:
            InvalidTaskError / TaskPayloadLimitError: If validation fails.
        """
        if isinstance(task_name_or_envelope, TaskEnvelope):
            envelope = task_name_or_envelope
            if timeout is not None and envelope.timeout is None:
                envelope.timeout = timeout
            return envelope
        elif isinstance(task_name_or_envelope, TaskRequest):
            return TaskEnvelope.from_request(task_name_or_envelope, timeout=timeout)
        elif isinstance(task_name_or_envelope, Message):
            return TaskEnvelope.from_message(task_name_or_envelope)
        elif isinstance(task_name_or_envelope, dict):
            env = TaskEnvelope.from_dict(task_name_or_envelope)
            if timeout is not None and env.timeout is None:
                env.timeout = timeout
            return env
        elif isinstance(task_name_or_envelope, str):
            final_id = task_id or f"task-{uuid.uuid4().hex}"
            return TaskEnvelope(
                task_id=final_id,
                task_name=task_name_or_envelope,
                args=list(args),
                kwargs=dict(kwargs),
                timeout=timeout,
                metadata=dict(metadata) if metadata is not None else {},
            )
        else:
            raise InvalidTaskError(
                f"Unsupported task envelope type: {type(task_name_or_envelope).__name__}"
            )

    async def submit(
        self,
        task_name_or_envelope: TaskEnvelope | TaskRequest | Message | dict[str, Any] | str,
        *args: Any,
        task_id: str | None = None,
        timeout: float | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TaskWorkerRecord:
        """
        Submit a task envelope to the worker queue for asynchronous background execution.

        Transitions to PENDING state and enqueues. Callers can inspect progress or
        call `await record.wait()`.

        Raises:
            RuntimeError: If the worker is shut down.
            InvalidTaskError / TaskPayloadLimitError: If input envelope validation fails.
        """
        if self._is_shutting_down:
            raise RuntimeError("TaskWorker is shut down and not accepting new tasks")

        envelope = self.coerce_envelope(
            task_name_or_envelope,
            *args,
            task_id=task_id,
            timeout=timeout,
            metadata=metadata,
            **kwargs,
        )

        if envelope.task_id in self._tasks:
            raise InvalidTaskError(
                f"Task with ID '{envelope.task_id}' is already tracked in this worker"
            )

        record = TaskWorkerRecord(
            envelope=envelope,
            state=TaskLifecycleState.PENDING,
            created_at=time.time(),
        )
        self._tasks[envelope.task_id] = record

        if self._auto_start and not self._is_running:
            self.start()

        await self._queue.put(record)
        logger.debug(
            "Task %s (%s) enqueued in worker in PENDING state",
            envelope.task_name,
            envelope.task_id,
        )
        return record

    async def execute_envelope(
        self,
        envelope_or_data: TaskEnvelope | TaskRequest | Message | dict[str, Any] | str,
        *,
        timeout: float | None = None,
    ) -> TaskResult:
        """
        Accept and execute a task envelope, returning a structured TaskResult.

        Handles validation errors, unknown tasks, execution timeouts, and exceptions
        gracefully by returning structured failure results without crashing the worker.
        """
        raw_id = "unknown-task"
        raw_name = "unknown"
        if isinstance(envelope_or_data, dict):
            raw_id = str(envelope_or_data.get("task_id", raw_id))
            raw_name = str(envelope_or_data.get("task_name", envelope_or_data.get("task_type", raw_name)))
        elif hasattr(envelope_or_data, "task_id"):
            raw_id = str(getattr(envelope_or_data, "task_id", raw_id))
            raw_name = str(getattr(envelope_or_data, "task_name", raw_name))

        # 1. Parse and validate task envelope
        try:
            envelope = self.coerce_envelope(envelope_or_data, timeout=timeout)
        except (InvalidTaskError, TaskPayloadLimitError, ValidationError) as val_exc:
            return TaskResult.from_exception(raw_id, raw_name, val_exc)
        except Exception as exc:
            return TaskResult.from_exception(raw_id, raw_name, exc)

        # 2. Track record if not already tracked
        record = self._tasks.get(envelope.task_id)
        if record is None:
            record = TaskWorkerRecord(
                envelope=envelope,
                state=TaskLifecycleState.PENDING,
                created_at=time.time(),
            )
            self._tasks[envelope.task_id] = record

        # 3. Execute with concurrency control
        async with self._semaphore:
            result = await self._execute_record(record)
            return result

    async def execute(
        self,
        task_name_or_envelope: TaskEnvelope | TaskRequest | Message | dict[str, Any] | str,
        *args: Any,
        task_id: str | None = None,
        timeout: float | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> TaskResult:
        """
        Convenience execution method: accepts task parameters or envelope,
        executes asynchronously within concurrency and timeout limits,
        and returns a structured TaskResult.
        """
        envelope = self.coerce_envelope(
            task_name_or_envelope,
            *args,
            task_id=task_id,
            timeout=timeout,
            metadata=metadata,
            **kwargs,
        )
        return await self.execute_envelope(envelope, timeout=timeout)

    async def handle_message(self, message: Message) -> Message:
        """
        Process an incoming protocol TASK_REQUEST message and return a TASK_RESPONSE message.
        """
        result = await self.execute_envelope(message)
        response = result.to_response()
        return response.to_message(
            sender_id=message.recipient_id or "worker",
            recipient_id=message.sender_id,
            correlation_id=message.message_id or message.correlation_id,
        )

    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a pending or running task by ID.

        Returns:
            True if cancelled successfully, False if already completed or not found.
        """
        record = self._tasks.get(task_id)
        if record is None or record.is_done:
            return False

        if record.state == TaskLifecycleState.PENDING:
            record._cancel("Task cancelled before worker execution started")
            logger.info("Pending task %s cancelled", task_id)
            return True

        if record.state == TaskLifecycleState.RUNNING:
            active_task = self._running_tasks.get(task_id)
            if active_task is not None and not active_task.done():
                active_task.cancel()
                logger.info("Running task %s cancelled via active coroutine", task_id)
                return True

        return False

    async def wait_for_task(
        self, task_id: str, timeout: float | None = None
    ) -> TaskResult:
        """Wait for a tracked task to reach a terminal state."""
        record = self.get_task(task_id)
        if record is None:
            raise TaskError(f"Task '{task_id}' is not tracked in this worker")
        return await record.wait(timeout=timeout)

    async def wait_all(self, timeout: float | None = None) -> None:
        """Wait until all pending and running tasks finish execution."""
        if not self._is_running and self._queue.empty() and self._running_count == 0:
            return

        async def _join() -> None:
            if self._is_running:
                await self._queue.join()
            while self._running_count > 0:
                await asyncio.sleep(0.01)

        to = timeout if timeout is not None else 10.0
        await asyncio.wait_for(_join(), timeout=to)

    async def shutdown(
        self,
        wait: bool = True,
        timeout: float | None = None,
        cancel_pending: bool = False,
    ) -> None:
        """
        Gracefully shut down the worker execution engine.

        Args:
            wait: Wait for pending and executing tasks to finish before stopping.
            timeout: Maximum seconds to wait.
            cancel_pending: Cancel remaining pending tasks in the queue.
        """
        if self._is_shutting_down and not self._is_running:
            return

        self._is_shutting_down = True
        self._shutdown_event.set()
        logger.debug(
            "Shutting down TaskWorker (wait=%s, cancel_pending=%s)",
            wait,
            cancel_pending,
        )

        if not self._is_running:
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    if item is not None and item.state == TaskLifecycleState.PENDING:
                        item._cancel("Task cancelled due to worker shutdown")
                    self._queue.task_done()
                except (asyncio.QueueEmpty, ValueError):
                    break
            return

        if cancel_pending:
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    if item is not None and item.state == TaskLifecycleState.PENDING:
                        item._cancel("Task cancelled due to worker shutdown")
                    self._queue.task_done()
                except asyncio.QueueEmpty:
                    break

        if wait:
            try:
                await self.wait_all(timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("Shutdown timeout exceeded while waiting for tasks to finish")

        for _ in self._workers:
            try:
                self._queue.put_nowait(None)
            except (asyncio.QueueFull, Exception):
                pass

        if self._workers:
            done, pending = await asyncio.wait(self._workers, timeout=1.0)
            for w in pending:
                w.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

        self._workers.clear()
        self._running_tasks.clear()
        self._running_count = 0
        self._is_running = False
        logger.debug("TaskWorker shutdown complete")

    async def _worker_loop(self, worker_id: int) -> None:
        """Background worker coroutine picking and executing queued tasks."""
        logger.debug("Worker %d loop started", worker_id)
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                break

            if item is None:
                self._queue.task_done()
                break

            if item.state == TaskLifecycleState.CANCELLED:
                self._queue.task_done()
                continue

            try:
                async with self._semaphore:
                    await self._execute_record(item)
            except Exception:
                logger.exception(
                    "Unexpected error executing task %s in worker %d",
                    item.task_id,
                    worker_id,
                )
            finally:
                self._queue.task_done()

        logger.debug("Worker %d loop terminated", worker_id)

    async def _execute_record(self, record: TaskWorkerRecord) -> TaskResult:
        """
        Core task execution boundary:
        1. Transitions state: PENDING -> RUNNING.
        2. Validates registry presence (rejecting untrusted code).
        3. Enforces execution timeout.
        4. Executes async coroutines directly, or sync handlers via threads.
        5. Validates and serializes return values.
        6. Isolates exceptions into structured failure results.
        7. Transitions state: RUNNING -> SUCCEEDED or FAILED.
        """
        record.state = TaskLifecycleState.RUNNING
        record.started_at = time.time()
        perf_start = time.perf_counter()
        self._running_count += 1

        envelope = record.envelope
        task_timeout = (
            envelope.timeout
            if envelope.timeout is not None
            else self._default_timeout
        )

        try:
            # 1. Check registry lookup - only explicitly registered tasks are executed
            try:
                handler = self._registry.get(envelope.task_name)
            except UnknownTaskError as unknown_exc:
                perf_end = time.perf_counter()
                elapsed = max(0.0, perf_end - perf_start)
                res = TaskResult.failure(
                    task_id=envelope.task_id,
                    task_name=envelope.task_name,
                    error=TaskErrorInfo(
                        code="UNKNOWN_TASK",
                        message=f"Task '{envelope.task_name}' is not registered in this worker's TaskRegistry",
                    ),
                    execution_time=elapsed,
                    created_at=record.created_at,
                    started_at=record.started_at,
                    completed_at=time.time(),
                    metadata=envelope.metadata,
                )
                record._complete(res)
                return res

            # 2. Define the execution runner
            async def _run_handler() -> Any:
                if asyncio.iscoroutinefunction(handler):
                    # Asynchronous coroutine handler
                    return await handler(*envelope.args, **envelope.kwargs)
                elif self._run_sync_in_thread:
                    # Synchronous handler: run in thread to keep asyncio event loop responsive
                    raw_res = await asyncio.to_thread(
                        handler, *envelope.args, **envelope.kwargs
                    )
                    if asyncio.iscoroutine(raw_res):
                        return await raw_res
                    return raw_res
                else:
                    raw_res = handler(*envelope.args, **envelope.kwargs)
                    if asyncio.iscoroutine(raw_res):
                        return await raw_res
                    return raw_res

            # 3. Create independent task and track it in _running_tasks
            exec_task = asyncio.create_task(_run_handler())
            self._running_tasks[record.task_id] = exec_task

            try:
                if task_timeout is not None and task_timeout > 0:
                    raw_result = await asyncio.wait_for(asyncio.shield(exec_task), timeout=task_timeout)
                else:
                    raw_result = await exec_task
            except asyncio.CancelledError:
                if not exec_task.done():
                    exec_task.cancel()
                    try:
                        await exec_task
                    except (asyncio.CancelledError, Exception):
                        pass
                perf_end = time.perf_counter()
                elapsed = max(0.0, perf_end - perf_start)
                record._cancel("Task execution was cancelled")
                return record.to_result()
            except (asyncio.TimeoutError, TimeoutError):
                if not exec_task.done():
                    exec_task.cancel()
                    try:
                        await exec_task
                    except (asyncio.CancelledError, Exception):
                        pass
                perf_end = time.perf_counter()
                elapsed = max(0.0, perf_end - perf_start)
                res = TaskResult.failure(
                    task_id=envelope.task_id,
                    task_name=envelope.task_name,
                    error=TaskErrorInfo(
                        code="TASK_TIMEOUT",
                        message=(
                            f"Task '{envelope.task_name}' ({envelope.task_id}) exceeded "
                            f"execution timeout of {task_timeout}s"
                        ),
                        details={"timeout": task_timeout},
                    ),
                    execution_time=elapsed,
                    created_at=record.created_at,
                    started_at=record.started_at,
                    completed_at=time.time(),
                    metadata=envelope.metadata,
                )
                record.exception = TaskTimeoutError(
                    f"Task '{envelope.task_name}' timed out after {task_timeout}s"
                )
                record._complete(res)
                return res

            # 4. Validate result safety (JSON-compatibility and payload size)
            try:
                _check_json_value(raw_result, current_depth=1)
                validate_payload_size(
                    {"result": raw_result}, max_size=MAX_TASK_RESULT_SIZE
                )
            except (InvalidTaskError, TaskPayloadLimitError) as res_exc:
                perf_end = time.perf_counter()
                elapsed = max(0.0, perf_end - perf_start)
                res = TaskResult.failure(
                    task_id=envelope.task_id,
                    task_name=envelope.task_name,
                    error=TaskErrorInfo(
                        code="INVALID_RESULT",
                        message=f"Task returned non-serializable or oversized result: {res_exc}",
                    ),
                    execution_time=elapsed,
                    created_at=record.created_at,
                    started_at=record.started_at,
                    completed_at=time.time(),
                    metadata=envelope.metadata,
                )
                record._complete(res)
                return res

            # 5. Success completion
            perf_end = time.perf_counter()
            elapsed = max(0.0, perf_end - perf_start)
            res = TaskResult.success(
                task_id=envelope.task_id,
                task_name=envelope.task_name,
                result=raw_result,
                execution_time=elapsed,
                created_at=record.created_at,
                started_at=record.started_at,
                completed_at=time.time(),
                metadata=envelope.metadata,
            )
            record._complete(res)
            return res

        except Exception as exc:
            perf_end = time.perf_counter()
            elapsed = max(0.0, perf_end - perf_start)
            res = TaskResult.from_exception(
                task_id=envelope.task_id,
                task_name=envelope.task_name,
                exc=exc,
                execution_time=elapsed,
                created_at=record.created_at,
                started_at=record.started_at,
                completed_at=time.time(),
                metadata=envelope.metadata,
            )
            record.exception = exc
            record._complete(res)
            return res

        finally:
            self._running_tasks.pop(record.task_id, None)
            self._running_count = max(0, self._running_count - 1)

    async def __aenter__(self) -> TaskWorker:
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.shutdown(wait=True)


# Aliases for flexible naming
Worker = TaskWorker
TaskWorkerEngine = TaskWorker

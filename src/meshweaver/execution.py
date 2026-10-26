"""
Asynchronous task execution engine for local and distributed task processing.

Provides:
- TaskState: Task lifecycle states (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED).
- TaskRecord: Immutable/mutable record of task submission, timestamps, duration, and results.
- TaskExecutionEngine: Asyncio-based queue with worker pool, concurrency control,
  strict security boundaries, input validation, exception handling, and graceful shutdown.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import logging
import time
from typing import Any
import uuid

from meshweaver.exceptions import (
    InvalidTaskError,
    TaskError,
    TaskExecutionError,
    TaskPayloadLimitError,
    UnknownTaskError,
)
from meshweaver.tasks import (
    MAX_TASK_ID_LENGTH,
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

DEFAULT_MAX_CONCURRENCY: int = 4


class TaskState(StrEnum):
    """
    Lifecycle states for asynchronous task execution.
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        """Return True if this state is a terminal/final state."""
        return self in (TaskState.COMPLETED, TaskState.FAILED, TaskState.CANCELLED)

    @property
    def is_active(self) -> bool:
        """Return True if this task is currently pending or actively running."""
        return self in (TaskState.PENDING, TaskState.RUNNING)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TaskState):
            return self.value == other.value
        if isinstance(other, str):
            val = other.upper()
            if self.value == val:
                return True
            # Compatibility aliases with TaskStatus
            if self == TaskState.COMPLETED and val in ("SUCCESS", "COMPLETED"):
                return True
            if self == TaskState.FAILED and val in ("FAILURE", "ERROR", "FAILED"):
                return True
            return False
        return False

    def __hash__(self) -> int:
        return hash(self.value)

    @classmethod
    def _missing_(cls, value: object) -> Any:
        if isinstance(value, str):
            val_upper = value.upper()
            for member in cls:
                if member.value == val_upper or member.name == val_upper:
                    return member
            if val_upper == "SUCCESS":
                return cls.COMPLETED
            if val_upper in ("FAILURE", "ERROR"):
                return cls.FAILED
        return super()._missing_(value)


@dataclass
class TaskRecord:
    """
    Detailed tracking record for an asynchronous task submitted to the engine.

    Encapsulates task identification, parameters, lifecycle state,
    timestamps, duration, results, errors, and metadata.
    """

    task_id: str
    task_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    state: TaskState = TaskState.PENDING
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    completed_at: float | None = None
    execution_time: float | None = None
    result: Any = None
    error: TaskErrorInfo | None = None
    exception: BaseException | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    # Internal synchronization primitive for callers waiting on completion
    _future: asyncio.Future[Any] | None = field(default=None, repr=False, compare=False)

    def _get_or_create_future(self) -> asyncio.Future[Any]:
        if self._future is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
            self._future = loop.create_future()
        return self._future

    def _set_result(self, result: Any) -> None:
        fut = self._get_or_create_future()
        if not fut.done():
            fut.set_result(result)

    def _set_exception(self, exc: BaseException) -> None:
        fut = self._get_or_create_future()
        if not fut.done():
            if isinstance(exc, asyncio.CancelledError):
                fut.cancel()
            else:
                fut.set_exception(exc)

    @property
    def duration(self) -> float | None:
        """
        Elapsed execution duration in seconds.

        Returns:
            Recorded execution_time if set, or elapsed duration if completed/running,
            or None if the task has not started yet.
        """
        if self.execution_time is not None:
            return self.execution_time
        if self.started_at is not None:
            if self.completed_at is not None:
                return max(0.0, self.completed_at - self.started_at)
            return max(0.0, time.time() - self.started_at)
        return None

    @property
    def is_done(self) -> bool:
        """Return True if task reached a terminal state."""
        return self.state.is_terminal

    @property
    def is_completed(self) -> bool:
        """Return True if task completed successfully."""
        return self.state == TaskState.COMPLETED

    @property
    def is_failed(self) -> bool:
        """Return True if task failed with an error or exception."""
        return self.state == TaskState.FAILED

    @property
    def is_running(self) -> bool:
        """Return True if task is currently executing."""
        return self.state == TaskState.RUNNING

    @property
    def is_pending(self) -> bool:
        """Return True if task is queued and waiting for worker execution."""
        return self.state == TaskState.PENDING

    @property
    def is_cancelled(self) -> bool:
        """Return True if task was cancelled."""
        return self.state == TaskState.CANCELLED

    async def wait(self, timeout: float | None = None, raise_on_error: bool = False) -> Any:
        """
        Asynchronously wait for this task to reach a terminal state.

        Args:
            timeout: Maximum seconds to wait before raising asyncio.TimeoutError.
            raise_on_error: If True and the task failed, raise the underlying exception.

        Returns:
            The task execution result on success, or this TaskRecord if raise_on_error is False.
        """
        fut = self._get_or_create_future()
        try:
            if timeout is not None:
                await asyncio.wait_for(asyncio.shield(fut), timeout=timeout)
            else:
                await asyncio.shield(fut)
        except (asyncio.CancelledError, Exception):
            if raise_on_error:
                raise
        if raise_on_error and self.exception is not None:
            raise self.exception
        return self.result if self.is_completed else self

    def to_dict(self) -> dict[str, Any]:
        """Convert task record to a JSON-compatible dictionary."""
        return {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "state": self.state.value,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration": self.duration,
            "execution_time": self.execution_time,
            "result": self.result,
            "error": self.error.to_dict() if self.error is not None else None,
            "metadata": self.metadata,
        }

    def to_response(self) -> TaskResponse:
        """Convert task record to a validated protocol TaskResponse."""
        if self.state == TaskState.COMPLETED:
            return TaskResponse.success(
                task_id=self.task_id,
                result=self.result,
                execution_time=self.duration,
                metadata=self.metadata,
            )
        elif self.state == TaskState.FAILED:
            err = self.error if self.error is not None else TaskErrorInfo(
                code="TASK_FAILED",
                message=str(self.exception) if self.exception else "Task execution failed",
            )
            return TaskResponse.failure(
                task_id=self.task_id,
                error=err,
                execution_time=self.duration,
                metadata=self.metadata,
            )
        elif self.state == TaskState.CANCELLED:
            return TaskResponse(
                task_id=self.task_id,
                status=TaskStatus.CANCELLED,
                result=None,
                error=self.error or TaskErrorInfo(code="TASK_CANCELLED", message="Task cancelled"),
                execution_time=self.duration,
                metadata=self.metadata,
            )
        else:
            return TaskResponse(
                task_id=self.task_id,
                status=TaskStatus.RUNNING if self.state == TaskState.RUNNING else TaskStatus.PENDING,
                result=None,
                error=None,
                execution_time=self.duration,
                metadata=self.metadata,
            )


# Alias for compatibility
TaskExecution = TaskRecord


class TaskExecutionEngine:
    """
    Asynchronous task execution engine with an asyncio queue, worker pool,
    configurable concurrency, strict security boundaries, and lifecycle tracking.

    Enforces safe execution boundaries:
    - Tasks must be explicitly registered functions in a TaskRegistry.
    - Remote/local arguments are strictly validated against JSON-safe schema limits.
    - Code injection attempts, dunder keys, and dangerous names are blocked.
    - Independent of networking layer for seamless local testing and modular composition.
    """

    def __init__(
        self,
        *,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        max_queue_size: int = 0,
        registry: TaskRegistry | None = None,
        validate_inputs: bool = True,
        auto_start: bool = True,
    ) -> None:
        """
        Initialize the TaskExecutionEngine.

        Args:
            max_concurrency: Maximum number of worker coroutines executing tasks concurrently.
            max_queue_size: Maximum items in pending queue (0 for unbounded).
            registry: TaskRegistry containing explicitly registered handlers (defaults to create_default()).
            validate_inputs: Whether to validate task IDs, task names, and arguments on submission.
            auto_start: Whether to automatically launch worker pool on first task submission.
        """
        if not isinstance(max_concurrency, int) or isinstance(max_concurrency, bool) or max_concurrency < 1:
            raise ValueError(f"max_concurrency must be an integer >= 1, got {max_concurrency}")
        if not isinstance(max_queue_size, int) or isinstance(max_queue_size, bool) or max_queue_size < 0:
            raise ValueError(f"max_queue_size must be a non-negative integer, got {max_queue_size}")

        self._max_concurrency: int = max_concurrency
        self._max_queue_size: int = max_queue_size
        self._registry: TaskRegistry = registry if registry is not None else TaskRegistry.create_default()
        self._validate_inputs: bool = validate_inputs
        self._auto_start: bool = auto_start

        # State tracking
        self._tasks: dict[str, TaskRecord] = {}
        self._queue: asyncio.Queue[TaskRecord | None] = asyncio.Queue(maxsize=max_queue_size)
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
    def max_concurrency(self) -> int:
        """Return the configured maximum concurrency limit."""
        return self._max_concurrency

    @property
    def is_running(self) -> bool:
        """Return True if the worker pool is actively running."""
        return self._is_running

    @property
    def is_shutting_down(self) -> bool:
        """Return True if shutdown has been initiated."""
        return self._is_shutting_down

    @property
    def pending_count(self) -> int:
        """Return the count of tasks currently queued in PENDING state."""
        return sum(1 for t in self._tasks.values() if t.state == TaskState.PENDING)

    @property
    def running_count(self) -> int:
        """Return the count of tasks currently executing in RUNNING state."""
        return self._running_count

    @property
    def completed_count(self) -> int:
        """Return the count of tasks that completed successfully."""
        return sum(1 for t in self._tasks.values() if t.state == TaskState.COMPLETED)

    @property
    def failed_count(self) -> int:
        """Return the count of tasks that failed with an error."""
        return sum(1 for t in self._tasks.values() if t.state == TaskState.FAILED)

    @property
    def cancelled_count(self) -> int:
        """Return the count of tasks that were cancelled."""
        return sum(1 for t in self._tasks.values() if t.state == TaskState.CANCELLED)

    @property
    def total_tasks(self) -> int:
        """Return total number of tasks tracked by this engine."""
        return len(self._tasks)

    def register(
        self,
        name_or_func: Any = None,
        handler: Any = None,
    ) -> Any:
        """
        Register a task handler in this engine's TaskRegistry. Can be used as a function or decorator.
        """
        return self._registry.register(name_or_func, handler)

    def unregister(self, name: str) -> bool:
        """Unregister a task handler by name. Returns True if removed."""
        return self._registry.unregister(name)

    def has_task(self, name: str) -> bool:
        """Return True if task name is registered."""
        return self._registry.has_task(name)

    def list_registered_tasks(self) -> list[str]:
        """Return a sorted list of registered task handler names."""
        return self._registry.list_tasks()

    def get_task(self, task_id: str) -> TaskRecord | None:
        """Retrieve a TaskRecord by task_id, or None if not found."""
        return self._tasks.get(task_id)

    def list_tasks(self, state: TaskState | str | None = None) -> list[TaskRecord]:
        """
        List tracked tasks, optionally filtered by state.
        """
        if state is None:
            return list(self._tasks.values())
        return [t for t in self._tasks.values() if t.state == state]

    def start(self) -> None:
        """
        Start worker coroutines. Idempotent if already started.
        """
        if self._is_running:
            return
        if self._is_shutting_down:
            raise RuntimeError("Cannot start TaskExecutionEngine: engine has been shut down")

        self._shutdown_event.clear()
        self._is_running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"meshweaver-worker-{i}")
            for i in range(self._max_concurrency)
        ]
        logger.debug(
            "TaskExecutionEngine started with %d workers (max_concurrency=%d)",
            len(self._workers),
            self._max_concurrency,
        )

    async def submit(
        self,
        task_name: str,
        *args: Any,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        validate_registered: bool = True,
        **kwargs: Any,
    ) -> TaskRecord:
        """
        Submit a task for asynchronous execution.

        Args:
            task_name: Registered safe identifier of the task.
            *args: Positional arguments for the task handler.
            task_id: Optional unique identifier. If None, a safe UUID hex is generated.
            metadata: Optional user metadata dictionary.
            validate_registered: If True, immediately verify that task_name is registered.
            **kwargs: Keyword arguments for the task handler.

        Returns:
            TaskRecord representing the submitted task in PENDING state.

        Raises:
            RuntimeError: If engine is shut down.
            InvalidTaskError: If task_id, task_name, args, or kwargs fail security validation.
            UnknownTaskError: If validate_registered is True and task_name is not registered.
        """
        if self._is_shutting_down:
            raise RuntimeError("TaskExecutionEngine is shut down and not accepting new tasks")

        # Validate task ID
        if task_id is None:
            final_task_id = f"task-{uuid.uuid4().hex}"
        else:
            if self._validate_inputs:
                validate_task_id(task_id)
            final_task_id = str(task_id)

        if final_task_id in self._tasks:
            raise InvalidTaskError(f"Task with ID '{final_task_id}' is already tracked in this engine")

        # Validate task name and security boundaries
        if self._validate_inputs:
            validate_task_name(task_name)
            validated_args = validate_args(args)
            validated_kwargs = validate_kwargs(kwargs)
            final_metadata = dict(metadata) if metadata is not None else {}
            _check_json_value(final_metadata, current_depth=1)
            # Check payload total size
            validate_payload_size({
                "task_id": final_task_id,
                "task_name": task_name,
                "args": validated_args,
                "kwargs": validated_kwargs,
                "metadata": final_metadata,
            })
        else:
            validated_args = list(args)
            validated_kwargs = dict(kwargs)
            final_metadata = dict(metadata) if metadata is not None else {}

        # Validate registry registration
        if validate_registered and not self._registry.has_task(task_name):
            raise UnknownTaskError(
                f"Task '{task_name}' is not registered in this engine's TaskRegistry"
            )

        record = TaskRecord(
            task_id=final_task_id,
            task_name=task_name,
            args=validated_args,
            kwargs=validated_kwargs,
            state=TaskState.PENDING,
            created_at=time.time(),
            metadata=final_metadata,
        )
        self._tasks[final_task_id] = record

        # Auto-start if needed
        if self._auto_start and not self._is_running:
            self.start()

        await self._queue.put(record)
        logger.debug("Task %s (%s) enqueued in PENDING state", task_name, final_task_id)
        return record

    async def execute(
        self,
        task_name: str,
        *args: Any,
        timeout: float | None = None,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Convenience method: submit a task, await its execution, and return the result.

        Raises:
            TaskExecutionError / underlying exception: If the task execution fails.
            asyncio.TimeoutError: If execution exceeds timeout.
        """
        record = await self.submit(task_name, *args, task_id=task_id, metadata=metadata, **kwargs)
        await record.wait(timeout=timeout, raise_on_error=True)
        return record.result

    async def execute_request(
        self,
        request: TaskRequest,
        timeout: float | None = None,
    ) -> TaskResponse:
        """
        Execute a TaskRequest envelope through the engine and return a TaskResponse.
        """
        record = await self.submit(
            request.task_name,
            *request.args,
            task_id=request.task_id,
            metadata=request.metadata,
            validate_registered=False,  # Allow worker to produce structured error response
            **request.kwargs,
        )
        await record.wait(timeout=timeout, raise_on_error=False)
        return record.to_response()

    async def cancel_task(self, task_id: str) -> bool:
        """
        Cancel a task by ID.

        If the task is PENDING, it is marked CANCELLED immediately.
        If the task is RUNNING, the executing coroutine is cancelled.
        If the task is already terminal, returns False.

        Returns:
            True if the task was successfully cancelled, False otherwise.
        """
        record = self._tasks.get(task_id)
        if record is None or record.is_done:
            return False

        if record.state == TaskState.PENDING:
            record.state = TaskState.CANCELLED
            record.completed_at = time.time()
            record.error = TaskErrorInfo(
                code="TASK_CANCELLED",
                message="Task was cancelled before execution started",
            )
            canc_exc = asyncio.CancelledError(f"Task {task_id} was cancelled")
            record.exception = canc_exc
            record._set_exception(canc_exc)
            logger.info("Pending task %s cancelled", task_id)
            return True

        if record.state == TaskState.RUNNING:
            worker_task = self._running_tasks.get(task_id)
            if worker_task is not None and not worker_task.done():
                worker_task.cancel()
                logger.info("Running task %s cancelled via worker task", task_id)
                return True

        return False

    async def wait_for_task(self, task_id: str, timeout: float | None = None) -> TaskRecord:
        """
        Wait for a tracked task to reach a terminal state (COMPLETED, FAILED, CANCELLED).

        Raises:
            TaskError: If task_id is not tracked.
            asyncio.TimeoutError: If task does not complete within timeout.
        """
        record = self.get_task(task_id)
        if record is None:
            raise TaskError(f"Task '{task_id}' is not tracked in this engine")
        await record.wait(timeout=timeout, raise_on_error=False)
        return record

    async def wait_all(self, timeout: float | None = None) -> None:
        """
        Wait until all queued and running tasks have finished.
        """
        async def _join() -> None:
            await self._queue.join()
            # Wait for all running tasks to complete
            while self._running_count > 0:
                await asyncio.sleep(0.01)

        if timeout is not None:
            await asyncio.wait_for(_join(), timeout=timeout)
        else:
            await _join()

    async def shutdown(
        self,
        wait: bool = True,
        timeout: float | None = None,
        cancel_pending: bool = False,
    ) -> None:
        """
        Gracefully shut down the execution engine and worker pool.

        Args:
            wait: If True, wait for pending/running tasks to complete before stopping workers.
            timeout: Maximum seconds to wait for task completion.
            cancel_pending: If True, cancel all tasks remaining in PENDING queue.
        """
        if self._is_shutting_down and not self._is_running:
            return

        self._is_shutting_down = True
        self._shutdown_event.set()
        logger.debug("Shutting down TaskExecutionEngine (wait=%s, cancel_pending=%s)", wait, cancel_pending)

        # Cancel pending tasks if requested
        if cancel_pending:
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    if item is not None and item.state == TaskState.PENDING:
                        item.state = TaskState.CANCELLED
                        item.completed_at = time.time()
                        item.error = TaskErrorInfo(
                            code="TASK_CANCELLED",
                            message="Task was cancelled due to engine shutdown",
                        )
                        canc_exc = asyncio.CancelledError("Engine shutdown")
                        item.exception = canc_exc
                        item._set_exception(canc_exc)
                    self._queue.task_done()
                except asyncio.QueueEmpty:
                    break

        # Wait for queue if requested
        if wait:
            try:
                await self.wait_all(timeout=timeout)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                logger.warning("Shutdown timeout exceeded while waiting for tasks to finish")

        # Put sentinels to gracefully stop worker loops
        for _ in self._workers:
            try:
                self._queue.put_nowait(None)
            except (asyncio.QueueFull, Exception):
                pass

        # Await workers
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
        logger.debug("TaskExecutionEngine shutdown complete")

    async def _worker_loop(self, worker_id: int) -> None:
        """Background worker coroutine picking tasks from queue."""
        logger.debug("Worker %d loop started", worker_id)
        while True:
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                break

            # Sentinel for shutdown
            if item is None:
                self._queue.task_done()
                break

            # If task was already cancelled while in queue
            if item.state == TaskState.CANCELLED:
                self._queue.task_done()
                continue

            try:
                await self._execute_task(item)
            except Exception:
                logger.exception("Unexpected error executing task %s in worker %d", item.task_id, worker_id)
            finally:
                self._queue.task_done()

        logger.debug("Worker %d loop terminated", worker_id)

    async def _execute_task(self, record: TaskRecord) -> None:
        """Execute a single TaskRecord, track timing, handle results and exceptions."""
        record.state = TaskState.RUNNING
        record.started_at = time.time()
        perf_start = time.perf_counter()
        self._running_count += 1
        current_coro_task = asyncio.current_task()
        if current_coro_task is not None:
            self._running_tasks[record.task_id] = current_coro_task

        try:
            # 1. Handler lookup
            try:
                handler = self._registry.get(record.task_name)
            except UnknownTaskError as exc:
                perf_end = time.perf_counter()
                record.completed_at = time.time()
                record.execution_time = max(0.0, perf_end - perf_start)
                record.state = TaskState.FAILED
                record.error = TaskErrorInfo(code="UNKNOWN_TASK", message=str(exc))
                record.exception = exc
                record._set_exception(exc)
                return

            # 2. Invoke handler (sync or async)
            if asyncio.iscoroutinefunction(handler):
                result = await handler(*record.args, **record.kwargs)
            else:
                result = handler(*record.args, **record.kwargs)
                if asyncio.iscoroutine(result):
                    result = await result

            # 3. Validate result safety (JSON compatibility & size)
            try:
                _check_json_value(result, current_depth=1)
            except (InvalidTaskError, TaskPayloadLimitError) as exc:
                perf_end = time.perf_counter()
                record.completed_at = time.time()
                record.execution_time = max(0.0, perf_end - perf_start)
                record.state = TaskState.FAILED
                record.error = TaskErrorInfo(
                    code="INVALID_RESULT",
                    message=f"Task returned non-serializable or oversized result: {exc}",
                )
                record.exception = exc
                record._set_exception(exc)
                return

            # 4. Mark COMPLETED
            perf_end = time.perf_counter()
            record.completed_at = time.time()
            record.execution_time = max(0.0, perf_end - perf_start)
            record.result = result
            record.state = TaskState.COMPLETED
            record._set_result(result)

        except asyncio.CancelledError as exc:
            perf_end = time.perf_counter()
            record.completed_at = time.time()
            record.execution_time = max(0.0, perf_end - perf_start)
            record.state = TaskState.CANCELLED
            record.error = TaskErrorInfo(
                code="TASK_CANCELLED",
                message=f"Task execution cancelled: {exc}",
            )
            record.exception = exc
            record._set_exception(exc)
            raise
        except Exception as exc:
            perf_end = time.perf_counter()
            record.completed_at = time.time()
            record.execution_time = max(0.0, perf_end - perf_start)
            record.state = TaskState.FAILED
            record.error = TaskErrorInfo.from_exception(exc)
            record.exception = exc
            record._set_exception(exc)
        finally:
            self._running_tasks.pop(record.task_id, None)
            self._running_count = max(0, self._running_count - 1)

    async def __aenter__(self) -> TaskExecutionEngine:
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.shutdown(wait=True)

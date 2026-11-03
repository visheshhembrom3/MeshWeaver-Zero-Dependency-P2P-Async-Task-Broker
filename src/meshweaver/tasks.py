"""
Validated JSON-compatible task request and response schemas, size limits,
security boundaries, and local task handler registry.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum
import json
import logging
import math
import re
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
from meshweaver.protocol import Message, MessageType

logger = logging.getLogger(__name__)

# Security & size limit constants
MAX_TASK_ID_LENGTH: int = 128
MAX_TASK_NAME_LENGTH: int = 64
MAX_TASK_ARGS_COUNT: int = 100
MAX_TASK_KWARGS_COUNT: int = 100
MAX_TASK_PAYLOAD_SIZE: int = 64 * 1024  # 64 KB
MAX_TASK_RESULT_SIZE: int = 64 * 1024   # 64 KB
MAX_ERROR_CODE_LENGTH: int = 64
MAX_ERROR_MESSAGE_LENGTH: int = 4096
MAX_STRING_LENGTH: int = 65536
MAX_NESTING_DEPTH: int = 10

# Regex patterns for identifiers and safe names
TASK_ID_PATTERN: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9_\-]+$")
TASK_NAME_PATTERN: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9_\-]+$")
IDENTIFIER_PATTERN: re.Pattern[str] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# Reserved words and dangerous names that cannot be used as task names
RESERVED_TASK_NAMES: frozenset[str] = frozenset({
    "eval",
    "exec",
    "compile",
    "open",
    "builtins",
    "__builtins__",
    "globals",
    "locals",
    "getattr",
    "setattr",
    "delattr",
    "system",
    "popen",
    "spawn",
    "import",
    "__import__",
    "importlib",
    "shutil",
    "subprocess",
    "os",
    "sys",
    "breakpoint",
    "input",
    "print",
    "execfile",
})

# Forbidden keys in dictionaries to prevent code/import path injection
FORBIDDEN_KEYS: frozenset[str] = frozenset({
    "import",
    "__import__",
    "module",
    "code",
    "bytecode",
    "function_path",
    "func_path",
    "callable",
    "exec",
    "eval",
    "__reduce__",
    "__class__",
    "__subclasses__",
    "__builtins__",
})


def validate_task_id(task_id: Any) -> str:
    """
    Validate that task_id is a non-empty string conforming to allowed characters and length.

    Raises:
        InvalidTaskError: If task_id is not a string, is empty, or contains invalid characters.
        TaskPayloadLimitError: If task_id exceeds MAX_TASK_ID_LENGTH.
    """
    if not isinstance(task_id, str):
        raise InvalidTaskError(f"Task ID must be a string, got {type(task_id).__name__}")
    stripped = task_id.strip()
    if not stripped:
        raise InvalidTaskError("Task ID must not be empty or whitespace only")
    if len(task_id) > MAX_TASK_ID_LENGTH:
        raise TaskPayloadLimitError(
            f"Task ID exceeds maximum length of {MAX_TASK_ID_LENGTH} characters (got {len(task_id)})"
        )
    if not TASK_ID_PATTERN.match(task_id):
        raise InvalidTaskError(
            f"Task ID '{task_id}' contains invalid characters. Must match {TASK_ID_PATTERN.pattern}"
        )
    return task_id


def validate_task_name(task_name: Any) -> str:
    """
    Validate that task_name is a safe registered identifier.

    Ensures the name contains no dots, slashes, colons, dunders, or reserved words,
    strictly preventing arbitrary Python imports, executable code paths, or filesystem traversal.

    Raises:
        InvalidTaskError: If task_name is invalid, malformed, or specifies forbidden paths/words.
        TaskPayloadLimitError: If task_name exceeds MAX_TASK_NAME_LENGTH.
    """
    if not isinstance(task_name, str):
        raise InvalidTaskError(f"Task name must be a string, got {type(task_name).__name__}")
    stripped = task_name.strip()
    if not stripped:
        raise InvalidTaskError("Task name must not be empty or whitespace only")
    if len(task_name) > MAX_TASK_NAME_LENGTH:
        raise TaskPayloadLimitError(
            f"Task name exceeds maximum length of {MAX_TASK_NAME_LENGTH} characters (got {len(task_name)})"
        )
    if "." in task_name or "/" in task_name or "\\" in task_name or ":" in task_name:
        raise InvalidTaskError(
            f"Task name '{task_name}' specifies an invalid function path or import path"
        )
    if "__" in task_name:
        raise InvalidTaskError(
            f"Task name '{task_name}' cannot contain double underscores ('__')"
        )
    if not TASK_NAME_PATTERN.match(task_name):
        raise InvalidTaskError(
            f"Task name '{task_name}' contains invalid characters. Must match {TASK_NAME_PATTERN.pattern}"
        )
    if task_name.lower() in RESERVED_TASK_NAMES:
        raise InvalidTaskError(
            f"Task name '{task_name}' is a reserved identifier and cannot be used"
        )
    return task_name


def _check_json_value(value: Any, current_depth: int = 0) -> None:
    """
    Recursively validate that value is a strictly JSON-compatible type,
    within size and depth limits, with no forbidden keys, code objects,
    or non-finite numbers.

    Raises:
        InvalidTaskError: If a non-serializable type, forbidden key, or invalid number is encountered.
        TaskPayloadLimitError: If recursion depth or string length exceeds limits.
    """
    if current_depth > MAX_NESTING_DEPTH:
        raise TaskPayloadLimitError(
            f"Payload exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )

    if value is None:
        return

    # Check bool before int, as bool is a subclass of int in Python
    if isinstance(value, bool):
        return

    if isinstance(value, int):
        return

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise InvalidTaskError("Non-finite float values (NaN, Infinity) are not permitted")
        return

    if isinstance(value, str):
        if "\0" in value:
            raise InvalidTaskError("String values cannot contain null bytes")
        if len(value) > MAX_STRING_LENGTH:
            raise TaskPayloadLimitError(
                f"String exceeds maximum allowed length of {MAX_STRING_LENGTH} characters"
            )
        return

    if isinstance(value, (list, tuple)):
        for item in value:
            _check_json_value(item, current_depth + 1)
        return

    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise InvalidTaskError(f"Dictionary keys must be strings, got {type(k).__name__}")
            if k.lower() in FORBIDDEN_KEYS:
                raise InvalidTaskError(f"Payload contains forbidden key: '{k}'")
            if k.startswith("__"):
                raise InvalidTaskError(f"Dictionary keys cannot start with '__': '{k}'")
            if len(k) > MAX_TASK_NAME_LENGTH:
                raise TaskPayloadLimitError(f"Dictionary key exceeds maximum length: '{k[:20]}...'")
            _check_json_value(v, current_depth + 1)
        return

    # Non-primitive types (functions, lambdas, code objects, modules, sets, custom objects)
    raise InvalidTaskError(
        f"Value of type {type(value).__name__} is not JSON-compatible or safe"
    )


def validate_args(args: Any) -> list[Any]:
    """
    Validate positional arguments list.

    Raises:
        InvalidTaskError: If args is not a list or tuple, or contains invalid types.
        TaskPayloadLimitError: If args count exceeds MAX_TASK_ARGS_COUNT.
    """
    if not isinstance(args, (list, tuple)):
        raise InvalidTaskError(f"Task arguments must be a list or tuple, got {type(args).__name__}")
    if len(args) > MAX_TASK_ARGS_COUNT:
        raise TaskPayloadLimitError(
            f"Task arguments count exceeds limit of {MAX_TASK_ARGS_COUNT} (got {len(args)})"
        )
    for item in args:
        _check_json_value(item, current_depth=1)
    return list(args)


def validate_kwargs(kwargs: Any) -> dict[str, Any]:
    """
    Validate keyword arguments dictionary.

    Raises:
        InvalidTaskError: If kwargs is not a dict, contains invalid keys, or forbidden imports.
        TaskPayloadLimitError: If kwargs count exceeds MAX_TASK_KWARGS_COUNT.
    """
    if not isinstance(kwargs, dict):
        raise InvalidTaskError(f"Task keyword arguments must be a dictionary, got {type(kwargs).__name__}")
    if len(kwargs) > MAX_TASK_KWARGS_COUNT:
        raise TaskPayloadLimitError(
            f"Task kwargs count exceeds limit of {MAX_TASK_KWARGS_COUNT} (got {len(kwargs)})"
        )
    for k, v in kwargs.items():
        if not isinstance(k, str):
            raise InvalidTaskError(f"Keyword argument name must be a string, got {type(k).__name__}")
        if k.lower() in FORBIDDEN_KEYS:
            raise InvalidTaskError(f"Payload contains forbidden key: '{k}'")
        if not IDENTIFIER_PATTERN.match(k):
            raise InvalidTaskError(f"Keyword argument name '{k}' must be a valid Python identifier")
        if k.startswith("__"):
            raise InvalidTaskError(f"Keyword argument name '{k}' cannot start with '__'")
        if len(k) > MAX_TASK_NAME_LENGTH:
            raise TaskPayloadLimitError(f"Keyword argument name '{k}' exceeds length limit")
        _check_json_value(v, current_depth=1)
    return kwargs


def validate_payload_size(data_dict: dict[str, Any], max_size: int = MAX_TASK_PAYLOAD_SIZE) -> None:
    """
    Validate that the serialized JSON payload does not exceed the allowed byte limit.

    Raises:
        InvalidTaskError: If data cannot be serialized to JSON.
        TaskPayloadLimitError: If the serialized bytes exceed max_size.
    """
    try:
        serialized = json.dumps(data_dict, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, OverflowError) as exc:
        raise InvalidTaskError(f"Payload cannot be serialized to JSON: {exc}") from exc
    if len(serialized) > max_size:
        raise TaskPayloadLimitError(
            f"Payload serialized size ({len(serialized)} bytes) exceeds limit of {max_size} bytes"
        )


class TaskStatus(StrEnum):
    """Enumeration of possible task execution states."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    SUCCEEDED = "success"
    FAILURE = "failure"
    ERROR = "error"
    CANCELLED = "cancelled"

    @classmethod
    def _missing_(cls, value: object) -> Any:
        if isinstance(value, str):
            val_lower = value.lower()
            if val_lower in ("succeeded", "completed"):
                return cls.SUCCESS
            if val_lower == "failed":
                return cls.FAILURE
        return super()._missing_(value)


@dataclass(frozen=True, slots=True)
class TaskErrorInfo:
    """
    Structured, JSON-compatible error information for failed or rejected tasks.
    """

    code: str
    message: str
    details: dict[str, Any] | None = None

    def validate(self) -> None:
        """Validate error code, message, and details."""
        if not isinstance(self.code, str) or not self.code.strip():
            raise InvalidTaskError("TaskErrorInfo 'code' must be a non-empty string")
        if len(self.code) > MAX_ERROR_CODE_LENGTH:
            raise TaskPayloadLimitError(
                f"TaskErrorInfo 'code' exceeds maximum length of {MAX_ERROR_CODE_LENGTH} characters"
            )
        if not isinstance(self.message, str):
            raise InvalidTaskError("TaskErrorInfo 'message' must be a string")
        if len(self.message) > MAX_ERROR_MESSAGE_LENGTH:
            raise TaskPayloadLimitError(
                f"TaskErrorInfo 'message' exceeds maximum length of {MAX_ERROR_MESSAGE_LENGTH} characters"
            )
        if self.details is not None:
            if not isinstance(self.details, dict):
                raise InvalidTaskError("TaskErrorInfo 'details' must be a dictionary or None")
            _check_json_value(self.details, current_depth=1)

    def to_dict(self) -> dict[str, Any]:
        """Convert error info to a JSON-compatible dictionary."""
        return {
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: Any) -> TaskErrorInfo:
        """Construct and validate TaskErrorInfo from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dictionary for TaskErrorInfo, got {type(data).__name__}")
        code = data.get("code")
        message = data.get("message")
        if code is None or message is None:
            raise InvalidTaskError("TaskErrorInfo requires 'code' and 'message' fields")
        info = cls(code=str(code), message=str(message), details=data.get("details"))
        info.validate()
        return info

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        default_code: str = "TASK_EXECUTION_ERROR",
    ) -> TaskErrorInfo:
        """Construct safe TaskErrorInfo from an exception without leaking internal system state."""
        code = type(exc).__name__
        msg = str(exc) or code
        info = cls(
            code=code[:MAX_ERROR_CODE_LENGTH],
            message=msg[:MAX_ERROR_MESSAGE_LENGTH],
            details=None,
        )
        return info


@dataclass(frozen=True, slots=True)
class TaskRequest:
    """
    Validated, JSON-compatible task request schema.

    Encapsulates task ID, registered task name, arguments (positional and keyword),
    and metadata while enforcing strict type and size boundaries.
    """

    task_id: str
    task_name: str
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    correlation_id: str | None = None

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """
        Validate the structural integrity, security boundaries, and size limits of this request.

        Raises:
            InvalidTaskError: If any field fails schema or security checks.
            TaskPayloadLimitError: If any field or the overall payload exceeds size limits.
        """
        validate_task_id(self.task_id)
        validate_task_name(self.task_name)
        validate_args(self.args)
        validate_kwargs(self.kwargs)
        if not isinstance(self.metadata, dict):
            raise InvalidTaskError("Field 'metadata' must be a dictionary")
        _check_json_value(self.metadata, current_depth=1)
        validate_payload_size(self.to_dict())

    @classmethod
    def create(
        cls,
        task_name: str,
        args: list[Any] | tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        *,
        task_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> TaskRequest:
        """Factory method to construct and validate a TaskRequest instance."""
        final_id = task_id if task_id is not None else uuid.uuid4().hex
        final_args = list(args) if args is not None else []
        final_kwargs = dict(kwargs) if kwargs is not None else {}
        final_metadata = dict(metadata) if metadata is not None else {}
        return cls(
            task_id=final_id,
            task_name=task_name,
            args=final_args,
            kwargs=final_kwargs,
            metadata=final_metadata,
            correlation_id=correlation_id,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert task request to a JSON-compatible dictionary."""
        data: dict[str, Any] = {
            "task_id": self.task_id,
            "task_name": self.task_name,
            "task_type": self.task_name,
            "args": self.args,
            "kwargs": self.kwargs,
            "params": self.kwargs,
            "metadata": self.metadata,
        }
        if self.correlation_id is not None:
            data["correlation_id"] = self.correlation_id
        return data

    def to_json(self) -> str:
        """Serialize task request to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Any) -> TaskRequest:
        """Construct and validate a TaskRequest from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dict for TaskRequest, got {type(data).__name__}")

        task_id = data.get("task_id")
        if task_id is None:
            raise InvalidTaskError("Missing required field 'task_id'")

        task_name = data.get("task_name") if "task_name" in data else data.get("task_type")
        if task_name is None:
            raise InvalidTaskError("Missing required field 'task_name' or 'task_type'")

        raw_args = data.get("args", [])
        raw_kwargs = data.get("kwargs") if "kwargs" in data else data.get("params", {})
        raw_metadata = data.get("metadata", {})
        raw_corr_id = data.get("correlation_id")

        return cls(
            task_id=task_id,
            task_name=task_name,
            args=list(raw_args) if isinstance(raw_args, (list, tuple)) else raw_args,
            kwargs=dict(raw_kwargs) if isinstance(raw_kwargs, dict) else raw_kwargs,
            metadata=dict(raw_metadata) if isinstance(raw_metadata, dict) else raw_metadata,
            correlation_id=str(raw_corr_id) if raw_corr_id is not None else None,
        )

    @classmethod
    def from_json(cls, json_str: str) -> TaskRequest:
        """Construct and validate a TaskRequest from a JSON string."""
        if not isinstance(json_str, str):
            raise InvalidTaskError(f"Input must be a string, got {type(json_str).__name__}")
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise InvalidTaskError(f"Malformed JSON in TaskRequest: {exc.msg}") from exc
        return cls.from_dict(data)

    def to_message(
        self,
        sender_id: str,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        """Convert task request into a protocol Message of type TASK_REQUEST."""
        return Message.create(
            MessageType.TASK_REQUEST,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id or self.correlation_id or self.task_id,
            message_id=message_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_message(cls, message: Message) -> TaskRequest:
        """Construct a TaskRequest from a received protocol Message."""
        if message.message_type != MessageType.TASK_REQUEST:
            raise InvalidTaskError(
                f"Expected MessageType.TASK_REQUEST, got {message.message_type}"
            )
        return cls.from_dict(message.payload)


class _ErrorClassMethodOrProperty:
    """
    Descriptor enabling TaskResponse.error(...) as a factory classmethod,
    while permitting instance.error to access the TaskErrorInfo attribute.
    """

    def __init__(self, func: Any) -> None:
        self.func = func

    def __get__(self, instance: Any, owner: Any = None) -> Any:
        if instance is None:
            return self.func.__get__(owner, owner)
        return instance._error

    def __set__(self, instance: Any, value: Any) -> None:
        instance._error = value


class TaskResponse:
    """
    Validated, JSON-compatible task response schema.

    Encapsulates task ID, status, result, error details, execution time,
    and metadata while enforcing strict type and size boundaries.
    """

    __slots__ = ("task_id", "status", "result", "_error", "execution_time", "metadata", "correlation_id")

    def __init__(
        self,
        task_id: str,
        status: TaskStatus | str,
        result: Any = None,
        error: TaskErrorInfo | dict[str, Any] | str | None = None,
        execution_time: float | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.task_id = task_id
        if isinstance(status, str):
            try:
                self.status: TaskStatus = TaskStatus(status)
            except ValueError:
                raise InvalidTaskError(f"Field 'status' must be a TaskStatus enum, got '{status}'") from None
        elif isinstance(status, TaskStatus):
            self.status = status
        else:
            raise InvalidTaskError(f"Field 'status' must be a TaskStatus enum, got {type(status).__name__}")

        self.result = result
        self._error: TaskErrorInfo | None = self._coerce_error(error) if error is not None else None
        self.execution_time = execution_time
        self.metadata: dict[str, Any] = dict(metadata) if metadata is not None else {}
        self.correlation_id: str | None = correlation_id
        self.validate()

    @property
    def ack(self) -> Any:
        """Return acknowledgement metadata if attached."""
        return self.metadata.get("ack")

    @ack.setter
    def ack(self, value: Any) -> None:
        self.metadata["ack"] = value

    def validate(self) -> None:
        """
        Validate the structural integrity and size limits of this response.

        Raises:
            InvalidTaskError: If any field fails schema or security checks.
            TaskPayloadLimitError: If any field or the overall payload exceeds size limits.
        """
        validate_task_id(self.task_id)
        if not isinstance(self.status, TaskStatus):
            raise InvalidTaskError(f"Field 'status' must be a TaskStatus enum, got {self.status}")

        if self.status == TaskStatus.SUCCESS:
            if self.result is not None:
                _check_json_value(self.result, current_depth=1)
        elif self.status in (TaskStatus.FAILURE, TaskStatus.ERROR):
            if self._error is not None:
                self._error.validate()

        if self.execution_time is not None:
            if (
                isinstance(self.execution_time, bool)
                or not isinstance(self.execution_time, (int, float))
                or self.execution_time < 0
            ):
                raise InvalidTaskError("Field 'execution_time' must be a non-negative number")

        if not isinstance(self.metadata, dict):
            raise InvalidTaskError("Field 'metadata' must be a dictionary")
        _check_json_value(self.metadata, current_depth=1)
        validate_payload_size(self.to_dict(), max_size=MAX_TASK_RESULT_SIZE)

    @classmethod
    def success(
        cls,
        task_id: str,
        result: Any = None,
        *,
        execution_time: float | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> TaskResponse:
        """Factory method to construct a successful TaskResponse."""
        return cls(
            task_id=task_id,
            status=TaskStatus.SUCCESS,
            result=result,
            error=None,
            execution_time=execution_time,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    @classmethod
    def failure(
        cls,
        task_id: str,
        error: TaskErrorInfo | dict[str, Any] | str,
        *,
        execution_time: float | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> TaskResponse:
        """Factory method to construct a failed TaskResponse."""
        return cls(
            task_id=task_id,
            status=TaskStatus.FAILURE,
            result=None,
            error=error,
            execution_time=execution_time,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    @_ErrorClassMethodOrProperty
    def error(
        cls,
        task_id: str,
        error: TaskErrorInfo | dict[str, Any] | str | Exception,
        *,
        execution_time: float | None = None,
        metadata: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> TaskResponse:
        """Factory method to construct an error TaskResponse."""
        if isinstance(error, Exception):
            err_info: TaskErrorInfo = TaskErrorInfo.from_exception(error)
        else:
            err_info = cls._coerce_error(error)
        return cls(
            task_id=task_id,
            status=TaskStatus.ERROR,
            result=None,
            error=err_info,
            execution_time=execution_time,
            metadata=metadata,
            correlation_id=correlation_id,
        )

    @staticmethod
    def _coerce_error(err: TaskErrorInfo | dict[str, Any] | str) -> TaskErrorInfo:
        """Normalize error input into a validated TaskErrorInfo instance."""
        if isinstance(err, TaskErrorInfo):
            return err
        if isinstance(err, dict):
            return TaskErrorInfo.from_dict(err)
        if isinstance(err, str):
            info = TaskErrorInfo(code="ERROR", message=err)
            info.validate()
            return info
        raise InvalidTaskError(f"Cannot convert error of type {type(err).__name__} to TaskErrorInfo")

    def to_dict(self) -> dict[str, Any]:
        """Convert task response to a JSON-compatible dictionary."""
        data: dict[str, Any] = {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self._error.to_dict() if self._error is not None else None,
            "execution_time": self.execution_time,
            "metadata": self.metadata,
        }
        if self.correlation_id is not None:
            data["correlation_id"] = self.correlation_id
        return data

    def to_json(self) -> str:
        """Serialize task response to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Any) -> TaskResponse:
        """Construct and validate a TaskResponse from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dict for TaskResponse, got {type(data).__name__}")

        task_id = data.get("task_id")
        if task_id is None:
            raise InvalidTaskError("Missing required field 'task_id'")

        raw_status = data.get("status")
        if raw_status is None:
            raise InvalidTaskError("Missing required field 'status'")

        return cls(
            task_id=task_id,
            status=raw_status,
            result=data.get("result"),
            error=data.get("error"),
            execution_time=data.get("execution_time"),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> TaskResponse:
        """Construct and validate a TaskResponse from a JSON string."""
        if not isinstance(json_str, str):
            raise InvalidTaskError(f"Input must be a string, got {type(json_str).__name__}")
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise InvalidTaskError(f"Malformed JSON in TaskResponse: {exc.msg}") from exc
        return cls.from_dict(data)

    def to_message(
        self,
        sender_id: str,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        """Convert task response into a protocol Message of type TASK_RESPONSE."""
        return Message.create(
            MessageType.TASK_RESPONSE,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id or self.correlation_id or self.task_id,
            message_id=message_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_message(cls, message: Message) -> TaskResponse:
        """Construct a TaskResponse from a received protocol Message."""
        if message.message_type != MessageType.TASK_RESPONSE:
            raise InvalidTaskError(
                f"Expected MessageType.TASK_RESPONSE, got {message.message_type}"
            )
        data = dict(message.payload)
        # If payload already has correlation_id, use it; otherwise attach message.correlation_id
        if "correlation_id" not in data and message.correlation_id and message.correlation_id != message.payload.get("task_id"):
            data["correlation_id"] = message.correlation_id
        return cls.from_dict(data)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskResponse):
            return False
        return (
            self.task_id == other.task_id
            and self.status == other.status
            and self.result == other.result
            and self.error == other.error
            and self.execution_time == other.execution_time
            and self.metadata == other.metadata
        )

    def __repr__(self) -> str:
        return (
            f"TaskResponse(task_id={self.task_id!r}, status={self.status!r}, "
            f"result={self.result!r}, error={self.error!r}, "
            f"execution_time={self.execution_time!r}, metadata={self.metadata!r}, "
            f"correlation_id={self.correlation_id!r})"
        )


@dataclass(frozen=True, slots=True)
class TaskAck:
    """
    Validated, JSON-compatible task acknowledgement schema.

    Encapsulates acknowledgement status, correlation ID, task ID,
    routed peer ID, and metadata confirming receipt and scheduling.
    """

    task_id: str
    correlation_id: str
    status: str = "accepted"
    routed_to: str | None = None
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate TaskAck field invariants and constraints."""
        validate_task_id(self.task_id)
        if not isinstance(self.correlation_id, str) or not self.correlation_id.strip():
            raise InvalidTaskError("TaskAck 'correlation_id' must be a non-empty string")
        if not isinstance(self.status, str) or not self.status.strip():
            raise InvalidTaskError("TaskAck 'status' must be a non-empty string")
        if self.routed_to is not None and (not isinstance(self.routed_to, str) or not self.routed_to.strip()):
            raise InvalidTaskError("TaskAck 'routed_to' must be a non-empty string or None")
        if not isinstance(self.metadata, dict):
            raise InvalidTaskError("TaskAck 'metadata' must be a dictionary")

    @property
    def is_accepted(self) -> bool:
        """Return True if the task was accepted for execution or routing."""
        return self.status.lower() in ("accepted", "routed", "acknowledged", "executing")

    @property
    def is_duplicate(self) -> bool:
        """Return True if this acknowledgement was generated for a duplicate submission."""
        return bool(self.metadata.get("duplicate") or self.status.lower() == "duplicate")

    @property
    def is_routed(self) -> bool:
        """Return True if the task was scheduled/routed to a designated worker."""
        return self.routed_to is not None

    def to_dict(self) -> dict[str, Any]:
        """Convert acknowledgement to a JSON-compatible dictionary."""
        return {
            "task_id": self.task_id,
            "correlation_id": self.correlation_id,
            "status": self.status,
            "routed_to": self.routed_to,
            "timestamp": self.timestamp,
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        """Serialize acknowledgement to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Any) -> TaskAck:
        """Construct and validate a TaskAck from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dict for TaskAck, got {type(data).__name__}")
        task_id = str(data.get("task_id", ""))
        correlation_id = str(data.get("correlation_id", data.get("task_id", "")))
        status = str(data.get("status", "accepted"))
        routed_to = data.get("routed_to")
        timestamp = float(data.get("timestamp", time.time()))
        metadata = dict(data.get("metadata", {}))
        return cls(
            task_id=task_id,
            correlation_id=correlation_id,
            status=status,
            routed_to=routed_to,
            timestamp=timestamp,
            metadata=metadata,
        )

    @classmethod
    def from_json(cls, json_str: str) -> TaskAck:
        """Construct and validate a TaskAck from a JSON string."""
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise InvalidTaskError(f"Malformed JSON in TaskAck: {exc.msg}") from exc
        return cls.from_dict(data)

    def to_message(
        self,
        sender_id: str,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        """Convert acknowledgement into a protocol Message of type TASK_ACK."""
        return Message.create(
            MessageType.TASK_ACK,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id or self.correlation_id,
            message_id=message_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_message(cls, message: Message) -> TaskAck:
        """Construct a TaskAck from a received protocol Message."""
        if message.message_type != MessageType.TASK_ACK:
            raise InvalidTaskError(
                f"Expected MessageType.TASK_ACK, got {message.message_type}"
            )
        data = dict(message.payload)
        if "correlation_id" not in data and message.correlation_id:
            data["correlation_id"] = message.correlation_id
        return cls.from_dict(data)


@dataclass(frozen=True, slots=True)
class TaskStatusReport:
    """
    Validated, JSON-compatible task execution status query response.

    Encapsulates lifecycle state, timestamps, duration, worker identification,
    and partial or terminal execution results / structured errors.
    """

    task_id: str
    status: TaskStatus
    correlation_id: str | None = None
    worker_peer_id: str | None = None
    created_at: float | None = None
    started_at: float | None = None
    completed_at: float | None = None
    execution_time: float | None = None
    result: Any = None
    error: TaskErrorInfo | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate TaskStatusReport field invariants and constraints."""
        validate_task_id(self.task_id)
        if not isinstance(self.status, TaskStatus):
            raise InvalidTaskError(f"Field 'status' must be a TaskStatus, got {self.status}")
        if self.worker_peer_id is not None and not isinstance(self.worker_peer_id, str):
            raise InvalidTaskError("Field 'worker_peer_id' must be a string or None")
        if not isinstance(self.metadata, dict):
            raise InvalidTaskError("Field 'metadata' must be a dictionary")

    @property
    def is_done(self) -> bool:
        """Return True if the task has reached a terminal execution state."""
        return self.status in (
            TaskStatus.SUCCESS,
            TaskStatus.FAILURE,
            TaskStatus.ERROR,
            TaskStatus.CANCELLED,
        )

    @property
    def is_success(self) -> bool:
        """Return True if the task completed successfully."""
        return self.status == TaskStatus.SUCCESS

    @property
    def is_failed(self) -> bool:
        """Return True if the task failed or resulted in error."""
        return self.status in (TaskStatus.FAILURE, TaskStatus.ERROR)

    @property
    def is_running(self) -> bool:
        """Return True if the task is actively running."""
        return self.status == TaskStatus.RUNNING

    @property
    def is_pending(self) -> bool:
        """Return True if the task is queued and pending execution."""
        return self.status == TaskStatus.PENDING

    def to_dict(self) -> dict[str, Any]:
        """Convert status report to a JSON-compatible dictionary."""
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "correlation_id": self.correlation_id,
            "worker_peer_id": self.worker_peer_id,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "execution_time": self.execution_time,
            "result": self.result,
            "error": self.error.to_dict() if self.error is not None else None,
            "metadata": dict(self.metadata),
        }

    def to_json(self) -> str:
        """Serialize status report to a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    @classmethod
    def from_dict(cls, data: Any) -> TaskStatusReport:
        """Construct and validate a TaskStatusReport from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidTaskError(f"Expected dict for TaskStatusReport, got {type(data).__name__}")
        raw_err = data.get("error")
        err_info = None
        if isinstance(raw_err, TaskErrorInfo):
            err_info = raw_err
        elif isinstance(raw_err, dict):
            err_info = TaskErrorInfo.from_dict(raw_err)
        elif isinstance(raw_err, str):
            err_info = TaskErrorInfo(code="ERROR", message=raw_err)

        raw_status = data.get("status", "pending")
        status_enum = TaskStatus(raw_status) if not isinstance(raw_status, TaskStatus) else raw_status

        return cls(
            task_id=str(data.get("task_id", "")),
            status=status_enum,
            correlation_id=data.get("correlation_id"),
            worker_peer_id=data.get("worker_peer_id"),
            created_at=data.get("created_at"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            execution_time=data.get("execution_time"),
            result=data.get("result"),
            error=err_info,
            metadata=dict(data.get("metadata", {})),
        )

    @classmethod
    def from_json(cls, json_str: str) -> TaskStatusReport:
        """Construct and validate a TaskStatusReport from a JSON string."""
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise InvalidTaskError(f"Malformed JSON in TaskStatusReport: {exc.msg}") from exc
        return cls.from_dict(data)

    def to_message(
        self,
        sender_id: str,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        """Convert status report into a protocol Message of type TASK_STATUS_RESPONSE."""
        return Message.create(
            MessageType.TASK_STATUS_RESPONSE,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id or self.correlation_id or self.task_id,
            message_id=message_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_message(cls, message: Message) -> TaskStatusReport:
        """Construct a TaskStatusReport from a received protocol Message."""
        if message.message_type != MessageType.TASK_STATUS_RESPONSE:
            raise InvalidTaskError(
                f"Expected MessageType.TASK_STATUS_RESPONSE, got {message.message_type}"
            )
        data = dict(message.payload)
        if "correlation_id" not in data and message.correlation_id:
            data["correlation_id"] = message.correlation_id
        return cls.from_dict(data)


class TaskDeduplicationStore:
    """
    In-memory deduplication and task state cache ensuring idempotent submissions,
    preventing duplicate executions, and caching completed task outcomes.
    """

    def __init__(self, max_entries: int = 5000, ttl: float = 3600.0) -> None:
        self.max_entries: int = max_entries
        self.ttl: float = ttl
        self._records: dict[str, dict[str, Any]] = {}
        self._futures: dict[str, asyncio.Future[TaskResponse]] = {}

    def is_duplicate(self, task_id: str) -> bool:
        """Return True if task_id has been seen and is still valid within TTL."""
        rec = self._records.get(task_id)
        if rec is None:
            return False
        if time.time() - float(rec.get("timestamp", 0)) > self.ttl:
            self._records.pop(task_id, None)
            self._futures.pop(task_id, None)
            return False
        return True

    def get_record(self, task_id: str) -> dict[str, Any] | None:
        """Return tracked submission record or None if untracked/expired."""
        if self.is_duplicate(task_id):
            return self._records.get(task_id)
        return None

    def get_result(self, task_id: str) -> TaskResponse | None:
        """Return cached terminal TaskResponse if task has completed, else None."""
        rec = self.get_record(task_id)
        if rec is not None:
            return rec.get("response")
        return None

    def get_future(self, task_id: str) -> asyncio.Future[TaskResponse] | None:
        """Return in-flight future for concurrent submitters awaiting the same task."""
        if self.is_duplicate(task_id):
            return self._futures.get(task_id)
        return None

    def register_submission(
        self,
        task_id: str,
        correlation_id: str,
        routed_to: str | None = None,
        ack: TaskAck | None = None,
    ) -> tuple[bool, dict[str, Any]]:
        """
        Register a task submission.

        Returns:
            tuple[bool, dict]: (is_new, record). If is_new is False, this is a duplicate.
        """
        now = time.time()
        if self.is_duplicate(task_id):
            existing = self._records[task_id]
            return False, existing

        if len(self._records) >= self.max_entries:
            oldest_key = min(self._records, key=lambda k: self._records[k].get("timestamp", 0))
            self._records.pop(oldest_key, None)
            self._futures.pop(oldest_key, None)

        record: dict[str, Any] = {
            "task_id": task_id,
            "correlation_id": correlation_id,
            "routed_to": routed_to,
            "ack": ack,
            "status": TaskStatus.PENDING,
            "response": None,
            "timestamp": now,
        }
        self._records[task_id] = record

        try:
            loop = asyncio.get_running_loop()
            self._futures[task_id] = loop.create_future()
        except RuntimeError:
            pass

        return True, record

    def set_status(self, task_id: str, status: TaskStatus) -> None:
        """Update the in-flight status for a tracked task."""
        rec = self._records.get(task_id)
        if rec is not None:
            rec["status"] = status

    def record_result(self, task_id: str, response: TaskResponse) -> None:
        """Cache terminal execution response and resolve any in-flight futures."""
        now = time.time()
        rec = self._records.get(task_id)
        if rec is not None:
            rec["response"] = response
            rec["status"] = response.status
            rec["completed_at"] = now
        else:
            self._records[task_id] = {
                "task_id": task_id,
                "correlation_id": response.correlation_id or task_id,
                "routed_to": None,
                "ack": None,
                "status": response.status,
                "response": response,
                "timestamp": now,
                "completed_at": now,
            }

        fut = self._futures.get(task_id)
        if fut is not None and not fut.done():
            fut.set_result(response)


# Type alias for task handlers
TaskHandler = Callable[..., Any]


class TaskRegistry:
    """
    In-memory registry of explicitly registered local task handlers.

    Enforces safe execution boundaries:
    - Only tasks explicitly registered in this registry can be executed.
    - Remote payloads cannot specify arbitrary imports, code paths, or filesystem targets.
    - Both synchronous and asynchronous handlers are supported.
    - Exceptions during execution are caught and wrapped into structured TaskResponse objects.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, TaskHandler] = {}

    def register(
        self,
        name_or_func: str | TaskHandler | None = None,
        handler: TaskHandler | None = None,
    ) -> Any:
        """
        Register a task handler by name. Can be used as a function or a decorator.

        Examples:
            registry.register("add", add_func)

            @registry.register("multiply")
            def multiply(a, b): return a * b

            @registry.register
            def echo(*args, **kwargs): return {"args": args, "kwargs": kwargs}
        """
        if callable(name_or_func) and handler is None:
            func = name_or_func
            name = func.__name__
            self._register_handler(name, func)
            return func

        if isinstance(name_or_func, str) and handler is None:
            def decorator(fn: TaskHandler) -> TaskHandler:
                self._register_handler(name_or_func, fn)
                return fn
            return decorator

        if isinstance(name_or_func, str) and handler is not None:
            if not callable(handler):
                raise InvalidTaskError(f"Handler for '{name_or_func}' must be callable")
            self._register_handler(name_or_func, handler)
            return handler

        raise InvalidTaskError(
            "Invalid arguments to register. Provide task name and a callable handler."
        )

    def _register_handler(self, name: str, handler: TaskHandler) -> None:
        validated_name = validate_task_name(name)
        if not callable(handler):
            raise InvalidTaskError(f"Handler for '{name}' must be callable")
        self._handlers[validated_name] = handler
        logger.debug("Registered local task handler: %s", validated_name)

    def unregister(self, name: str) -> bool:
        """Remove a task handler from the registry. Returns True if removed."""
        return self._handlers.pop(name, None) is not None

    def has_task(self, name: str) -> bool:
        """Check if a task name is registered."""
        return name in self._handlers

    def __contains__(self, name: str) -> bool:
        return self.has_task(name)

    def get(self, name: str) -> TaskHandler:
        """
        Retrieve a registered task handler.

        Raises:
            UnknownTaskError: If the task name is not explicitly registered.
        """
        handler = self._handlers.get(name)
        if handler is None:
            raise UnknownTaskError(f"Task '{name}' is not registered in this node's task registry")
        return handler

    def list_tasks(self) -> list[str]:
        """Return a sorted list of registered task names."""
        return sorted(self._handlers.keys())

    async def execute(self, request: TaskRequest) -> TaskResponse:
        """
        Execute a TaskRequest locally against registered task handlers.

        Safely catches exceptions, records timing, validates result schemas,
        and returns a structured TaskResponse.
        """
        start_time = time.perf_counter()
        task_id = request.task_id

        # Handler lookup: only explicitly registered names are permitted
        try:
            handler = self.get(request.task_name)
        except UnknownTaskError as exc:
            elapsed = time.perf_counter() - start_time
            return TaskResponse.error(
                task_id=task_id,
                error=TaskErrorInfo(code="UNKNOWN_TASK", message=str(exc)),
                execution_time=elapsed,
            )

        # Handler invocation (sync or async)
        try:
            if asyncio.iscoroutinefunction(handler):
                result = await handler(*request.args, **request.kwargs)
            else:
                result = handler(*request.args, **request.kwargs)
                if asyncio.iscoroutine(result):
                    result = await result
        except Exception as exc:
            elapsed = time.perf_counter() - start_time
            logger.exception("Error executing local task %s (%s)", request.task_name, task_id)
            return TaskResponse.error(
                task_id=task_id,
                error=TaskErrorInfo.from_exception(exc),
                execution_time=elapsed,
            )

        elapsed = time.perf_counter() - start_time

        # Validate that the handler return value is JSON-safe and bounded
        try:
            return TaskResponse.success(task_id=task_id, result=result, execution_time=elapsed)
        except (InvalidTaskError, TaskPayloadLimitError) as exc:
            return TaskResponse.error(
                task_id=task_id,
                error=TaskErrorInfo(
                    code="INVALID_RESULT",
                    message=f"Task returned invalid result: {exc}",
                ),
                execution_time=elapsed,
            )

    @classmethod
    def create_default(cls) -> TaskRegistry:
        """Create a registry pre-populated with standard safe example tasks."""
        registry = cls()

        @registry.register("ping")
        def ping() -> str:
            return "pong"

        @registry.register("echo")
        def echo(*args: Any, **kwargs: Any) -> dict[str, Any]:
            return {"args": list(args), "kwargs": dict(kwargs)}

        @registry.register("add")
        def add(a: int | float, b: int | float) -> int | float:
            return a + b

        @registry.register("reverse_string")
        def reverse_string(s: str) -> str:
            return s[::-1]

        return registry


# Re-export asynchronous task execution engine for local/standalone execution
from meshweaver.execution import (
    DEFAULT_MAX_CONCURRENCY,
    TaskExecution,
    TaskExecutionEngine,
    TaskRecord,
    TaskState,
)

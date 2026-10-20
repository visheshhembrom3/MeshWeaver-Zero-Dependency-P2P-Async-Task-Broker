"""
Comprehensive unit and integration tests for safe task request/response models,
validation rules, security boundaries, limits, and local task registry.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import pytest

from meshweaver import (
    MAX_TASK_ARGS_COUNT,
    MAX_TASK_ID_LENGTH,
    MAX_TASK_KWARGS_COUNT,
    MAX_TASK_NAME_LENGTH,
    MAX_TASK_PAYLOAD_SIZE,
    MAX_TASK_RESULT_SIZE,
    InvalidTaskError,
    Message,
    MessageType,
    PeerNode,
    TaskErrorInfo,
    TaskPayloadLimitError,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskStatus,
    UnknownTaskError,
    validate_task_id,
    validate_task_name,
)


# ============================================================================
# Task ID and Task Name Validation Tests
# ============================================================================


def test_validate_task_id_valid() -> None:
    """Verify that valid alphanumeric, hyphenated, and underscore task IDs are accepted."""
    assert validate_task_id("task-123") == "task-123"
    assert validate_task_id("t_100") == "t_100"
    assert validate_task_id("a1b2c3d4-e5f6-7890-abcd-ef1234567890") == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"


@pytest.mark.parametrize(
    "invalid_id",
    [
        12345,
        None,
        ["not", "a", "string"],
        "",
        "   ",
        "task id with spaces",
        "task;rm -rf",
        "task/slash",
        "task.dot",
        "task:colon",
        "task'quote",
        "x" * (MAX_TASK_ID_LENGTH + 1),
    ],
)
def test_validate_task_id_invalid(invalid_id: object) -> None:
    """Verify that malformed, empty, oversized, or special-character task IDs are rejected."""
    with pytest.raises((InvalidTaskError, TaskPayloadLimitError)):
        validate_task_id(invalid_id)


def test_validate_task_name_valid() -> None:
    """Verify that safe task names are accepted."""
    assert validate_task_name("add") == "add"
    assert validate_task_name("compute_sum") == "compute_sum"
    assert validate_task_name("process-batch-data") == "process-batch-data"
    assert validate_task_name("TASK_123") == "TASK_123"


@pytest.mark.parametrize(
    "forbidden_name",
    [
        # Dot notation (module / function paths)
        "os.system",
        "subprocess.run",
        "math.sqrt",
        "meshweaver.network.PeerNode",
        # Slashes and backslashes (file/directory paths)
        "/bin/sh",
        "scripts/run.py",
        r"C:\Windows\System32\cmd.exe",
        # Colons
        "module:func",
        # Dunders
        "__import__",
        "my__task",
        "__class__",
        # Reserved dangerous words
        "eval",
        "exec",
        "compile",
        "open",
        "builtins",
        "globals",
        "locals",
        "system",
        "popen",
        "spawn",
        "import",
        # Malformed / characters
        "",
        "   ",
        "task with spaces",
        "task;injection",
        "task$(cmd)",
        "task`whoami`",
        "x" * (MAX_TASK_NAME_LENGTH + 1),
    ],
)
def test_validate_task_name_forbidden_and_invalid(forbidden_name: str) -> None:
    """Verify that arbitrary function paths, imports, dunders, and dangerous names are blocked."""
    with pytest.raises((InvalidTaskError, TaskPayloadLimitError)):
        validate_task_name(forbidden_name)


# ============================================================================
# TaskRequest Schema & Security Validation Tests
# ============================================================================


def test_task_request_creation_defaults() -> None:
    """Verify TaskRequest creation with defaults and valid attributes."""
    req = TaskRequest.create("compute_sum", args=[1, 2], kwargs={"base": 10})

    assert isinstance(req.task_id, str) and len(req.task_id) > 0
    assert req.task_name == "compute_sum"
    assert req.args == [1, 2]
    assert req.kwargs == {"base": 10}
    assert req.metadata == {}


def test_task_request_serialization_roundtrip() -> None:
    """Verify serialization to/from dictionary and JSON strings."""
    req = TaskRequest.create(
        "matrix_multiply",
        args=[[1, 2], [3, 4]],
        kwargs={"transpose": True},
        task_id="t-matrix-1",
        metadata={"priority": "high", "timeout": 30.0},
    )

    # Dict roundtrip
    data_dict = req.to_dict()
    assert data_dict["task_id"] == "t-matrix-1"
    assert data_dict["task_name"] == "matrix_multiply"
    assert data_dict["task_type"] == "matrix_multiply"
    assert data_dict["params"] == {"transpose": True}

    reconstructed_dict = TaskRequest.from_dict(data_dict)
    assert reconstructed_dict == req

    # JSON roundtrip
    json_str = req.to_json()
    reconstructed_json = TaskRequest.from_json(json_str)
    assert reconstructed_json == req


def test_task_request_message_conversion() -> None:
    """Verify bidirectional conversion between TaskRequest and protocol Message."""
    req = TaskRequest.create("echo_test", args=["hello"], kwargs={"count": 3})
    msg = req.to_message(sender_id="node-origin", recipient_id="node-dest")

    assert msg.message_type == MessageType.TASK_REQUEST
    assert msg.sender_id == "node-origin"
    assert msg.recipient_id == "node-dest"
    assert msg.correlation_id == req.task_id

    reconstructed_req = TaskRequest.from_message(msg)
    assert reconstructed_req == req

    # Incompatible message type should raise InvalidTaskError
    ping_msg = Message.create(MessageType.PING, sender_id="node-test")
    with pytest.raises(InvalidTaskError, match="Expected MessageType.TASK_REQUEST"):
        TaskRequest.from_message(ping_msg)


def test_task_request_backward_compatibility_aliases() -> None:
    """Verify support for legacy or alternative field names (task_type, params)."""
    raw_payload = {
        "task_id": "legacy-task-1",
        "task_type": "legacy_handler",
        "params": {"timeout": 10},
    }
    req = TaskRequest.from_dict(raw_payload)
    assert req.task_name == "legacy_handler"
    assert req.kwargs == {"timeout": 10}
    assert req.args == []


@pytest.mark.parametrize(
    "forbidden_key",
    [
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
        "__class__",
        "__reduce__",
    ],
)
def test_task_request_rejects_forbidden_keys(forbidden_key: str) -> None:
    """Verify that payloads attempting to smuggle code/import keys are rejected."""
    with pytest.raises(InvalidTaskError, match="forbidden key"):
        TaskRequest.create("safe_task", kwargs={forbidden_key: "malicious_value"})

    with pytest.raises(InvalidTaskError, match="forbidden key"):
        TaskRequest.create("safe_task", metadata={forbidden_key: "malicious_value"})


def test_task_request_rejects_executable_and_non_json_objects() -> None:
    """Verify that callables, functions, lambdas, and non-primitive objects are rejected."""
    # Lambdas
    with pytest.raises(InvalidTaskError, match="not JSON-compatible"):
        TaskRequest.create("test_task", args=[lambda x: x])

    # Functions
    def dummy_fn() -> None:
        pass

    with pytest.raises(InvalidTaskError, match="not JSON-compatible"):
        TaskRequest.create("test_task", kwargs={"func": dummy_fn})

    # Custom object instances
    @dataclass
    class ArbitraryClass:
        x: int = 1

    with pytest.raises(InvalidTaskError, match="not JSON-compatible"):
        TaskRequest.create("test_task", args=[ArbitraryClass()])

    # Sets
    with pytest.raises(InvalidTaskError, match="not JSON-compatible"):
        TaskRequest.create("test_task", kwargs={"items": {1, 2, 3}})  # type: ignore[dict-item]


def test_task_request_rejects_non_finite_numbers() -> None:
    """Verify that NaN and Infinity are strictly rejected to ensure valid standard JSON."""
    with pytest.raises(InvalidTaskError, match="Non-finite float"):
        TaskRequest.create("test_task", args=[float("nan")])

    with pytest.raises(InvalidTaskError, match="Non-finite float"):
        TaskRequest.create("test_task", kwargs={"val": float("inf")})

    with pytest.raises(InvalidTaskError, match="Non-finite float"):
        TaskRequest.create("test_task", kwargs={"val": float("-inf")})


def test_task_request_rejects_null_bytes() -> None:
    """Verify strings with null bytes are rejected."""
    with pytest.raises(InvalidTaskError, match="null bytes"):
        TaskRequest.create("test_task", args=["hello\0world"])


def test_task_request_size_and_count_limits() -> None:
    """Verify enforcement of arguments count, kwargs count, nesting, and payload size."""
    # Arguments count limit
    with pytest.raises(TaskPayloadLimitError, match="arguments count exceeds limit"):
        TaskRequest.create("test_task", args=list(range(MAX_TASK_ARGS_COUNT + 1)))

    # Kwargs count limit
    oversized_kwargs = {f"k_{i}": i for i in range(MAX_TASK_KWARGS_COUNT + 1)}
    with pytest.raises(TaskPayloadLimitError, match="kwargs count exceeds limit"):
        TaskRequest.create("test_task", kwargs=oversized_kwargs)

    # Oversized payload
    large_string = "A" * (MAX_TASK_PAYLOAD_SIZE + 1000)
    with pytest.raises(TaskPayloadLimitError):
        TaskRequest.create("test_task", kwargs={"large_data": large_string})

    # Deeply nested structures
    nested: dict = {}
    curr = nested
    for i in range(15):
        curr["child"] = {}
        curr = curr["child"]
    with pytest.raises(TaskPayloadLimitError, match="nesting depth"):
        TaskRequest.create("test_task", kwargs={"nested": nested})


# ============================================================================
# TaskResponse & TaskErrorInfo Validation Tests
# ============================================================================


def test_task_response_success_roundtrip() -> None:
    """Verify successful TaskResponse creation and serialization roundtrip."""
    resp = TaskResponse.success(
        task_id="t-success-1",
        result={"computed_sum": 42},
        execution_time=0.0125,
        metadata={"worker_id": "worker-1"},
    )

    assert resp.task_id == "t-success-1"
    assert resp.status == TaskStatus.SUCCESS
    assert resp.result == {"computed_sum": 42}
    assert resp.error is None
    assert resp.execution_time == 0.0125

    # Dict roundtrip
    d = resp.to_dict()
    assert d["status"] == "success"
    reconstructed = TaskResponse.from_dict(d)
    assert reconstructed == resp

    # JSON roundtrip
    j = resp.to_json()
    reconstructed_json = TaskResponse.from_json(j)
    assert reconstructed_json == resp

    # Message conversion
    msg = resp.to_message(sender_id="worker-1", recipient_id="requester-1")
    assert msg.message_type == MessageType.TASK_RESPONSE
    assert TaskResponse.from_message(msg) == resp


def test_task_response_failure_and_error_representations() -> None:
    """Verify failure and error TaskResponse creation with various error representations."""
    # Failure with string
    fail_resp = TaskResponse.failure("t-fail-1", "Input validation failed on argument 'x'")
    assert fail_resp.status == TaskStatus.FAILURE
    assert fail_resp.error is not None
    assert fail_resp.error.code == "ERROR"
    assert "Input validation failed" in fail_resp.error.message

    # Error with Exception
    exc = ValueError("Index out of range")
    err_resp = TaskResponse.error("t-err-1", exc)
    assert err_resp.status == TaskStatus.ERROR
    assert err_resp.error is not None
    assert err_resp.error.code == "ValueError"
    assert "Index out of range" in err_resp.error.message

    # Error with TaskErrorInfo
    custom_err = TaskErrorInfo(code="RESOURCE_EXHAUSTED", message="Memory cap hit", details={"limit_mb": 512})
    err_resp_2 = TaskResponse.error("t-err-2", custom_err)
    assert err_resp_2.error == custom_err


def test_task_response_result_security_and_limits() -> None:
    """Verify that task results enforce JSON safety and size limits."""
    # Non-serializable result
    with pytest.raises(InvalidTaskError, match="not JSON-compatible"):
        TaskResponse.success("t-1", result=lambda: 123)

    # Oversized result
    large_result = "R" * (MAX_TASK_RESULT_SIZE + 1000)
    with pytest.raises(TaskPayloadLimitError):
        TaskResponse.success("t-1", result={"data": large_result})

    # Non-finite numbers in result
    with pytest.raises(InvalidTaskError, match="Non-finite float"):
        TaskResponse.success("t-1", result={"val": float("nan")})


def test_task_response_invalid_fields() -> None:
    """Verify rejection of invalid fields in TaskResponse."""
    with pytest.raises(InvalidTaskError, match="Field 'status'"):
        TaskResponse("t-1", status="unknown_status")  # type: ignore[arg-type]

    with pytest.raises(InvalidTaskError, match="execution_time"):
        TaskResponse("t-1", status=TaskStatus.SUCCESS, execution_time=-5.0)

    with pytest.raises(InvalidTaskError, match="execution_time"):
        TaskResponse("t-1", status=TaskStatus.SUCCESS, execution_time=True)  # type: ignore[arg-type]


# ============================================================================
# TaskRegistry & Local Task Execution Tests
# ============================================================================


def test_task_registry_registration_and_inspection() -> None:
    """Verify handler registration via direct call, decorator with name, and bare decorator."""
    registry = TaskRegistry()

    # Direct registration
    def custom_add(a: int, b: int) -> int:
        return a + b

    registry.register("custom_add", custom_add)

    # Decorator with explicit name
    @registry.register("multiplier")
    def my_mul(x: int, y: int) -> int:
        return x * y

    # Bare decorator
    @registry.register
    def ping() -> str:
        return "pong"

    assert registry.has_task("custom_add")
    assert "multiplier" in registry
    assert "ping" in registry
    assert not registry.has_task("non_existent")

    assert registry.list_tasks() == ["custom_add", "multiplier", "ping"]
    assert registry.get("custom_add") is custom_add

    # Unregister
    assert registry.unregister("ping") is True
    assert not registry.has_task("ping")
    assert registry.unregister("ping") is False


def test_task_registry_rejects_unsafe_registration() -> None:
    """Verify that registering invalid, forbidden, or non-callable tasks fails."""
    registry = TaskRegistry()

    with pytest.raises(InvalidTaskError, match="invalid function path or import path"):
        registry.register("os.system", lambda: None)

    with pytest.raises(InvalidTaskError, match="double underscores"):
        registry.register("__import__", lambda: None)

    with pytest.raises(InvalidTaskError, match="reserved identifier"):
        registry.register("eval", lambda: None)

    with pytest.raises(InvalidTaskError, match="must be callable"):
        registry.register("not_callable", "just a string")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_task_registry_execution_sync_and_async() -> None:
    """Verify executing synchronous and asynchronous registered handlers."""
    registry = TaskRegistry()

    def sync_compute(a: int, b: int, multiplier: int = 1) -> int:
        return (a + b) * multiplier

    async def async_fetch(key: str) -> dict[str, str]:
        await asyncio.sleep(0.01)
        return {"key": key, "value": f"val_{key}"}

    registry.register("compute", sync_compute)
    registry.register("fetch", async_fetch)

    # Execute sync task
    req1 = TaskRequest.create("compute", args=[3, 4], kwargs={"multiplier": 2})
    resp1 = await registry.execute(req1)

    assert resp1.status == TaskStatus.SUCCESS
    assert resp1.result == 14
    assert resp1.task_id == req1.task_id
    assert resp1.execution_time is not None and resp1.execution_time >= 0

    # Execute async task
    req2 = TaskRequest.create("fetch", args=["session_token"])
    resp2 = await registry.execute(req2)

    assert resp2.status == TaskStatus.SUCCESS
    assert resp2.result == {"key": "session_token", "value": "val_session_token"}


@pytest.mark.asyncio
async def test_task_registry_unknown_task_handling() -> None:
    """Verify that requesting an unregistered task name returns a structured error."""
    registry = TaskRegistry()

    # registry.get raises UnknownTaskError
    with pytest.raises(UnknownTaskError, match="is not registered"):
        registry.get("unknown_operation")

    # registry.execute returns error TaskResponse without raising unhandled exception
    req = TaskRequest.create("unknown_operation", args=[1, 2])
    resp = await registry.execute(req)

    assert resp.status == TaskStatus.ERROR
    assert resp.task_id == req.task_id
    assert resp.error is not None
    assert resp.error.code == "UNKNOWN_TASK"
    assert "unknown_operation" in resp.error.message


@pytest.mark.asyncio
async def test_task_registry_handler_exception_handling() -> None:
    """Verify that exceptions raised within registered handlers are caught safely."""
    registry = TaskRegistry()

    @registry.register("divide")
    def divide(a: float, b: float) -> float:
        if b == 0:
            raise ZeroDivisionError("Cannot divide by zero")
        return a / b

    req = TaskRequest.create("divide", args=[10, 0])
    resp = await registry.execute(req)

    assert resp.status == TaskStatus.ERROR
    assert resp.error is not None
    assert resp.error.code == "ZeroDivisionError"
    assert "Cannot divide by zero" in resp.error.message


@pytest.mark.asyncio
async def test_task_registry_handler_invalid_result_handling() -> None:
    """Verify that a handler returning non-JSON or unsafe objects is safely caught."""
    registry = TaskRegistry()

    @registry.register("bad_return")
    def bad_return() -> object:
        # Return a lambda function which is not JSON serializable
        return lambda x: x * 2

    req = TaskRequest.create("bad_return")
    resp = await registry.execute(req)

    assert resp.status == TaskStatus.ERROR
    assert resp.error is not None
    assert resp.error.code == "INVALID_RESULT"


@pytest.mark.asyncio
async def test_task_registry_default_tasks() -> None:
    """Verify that TaskRegistry.create_default() provides standard built-in tasks."""
    registry = TaskRegistry.create_default()
    assert set(registry.list_tasks()) == {"add", "echo", "ping", "reverse_string"}

    # Ping
    r_ping = await registry.execute(TaskRequest.create("ping"))
    assert r_ping.status == TaskStatus.SUCCESS
    assert r_ping.result == "pong"

    # Add
    r_add = await registry.execute(TaskRequest.create("add", args=[15, 27]))
    assert r_add.status == TaskStatus.SUCCESS
    assert r_add.result == 42

    # Reverse string
    r_rev = await registry.execute(TaskRequest.create("reverse_string", args=["MeshWeaver"]))
    assert r_rev.status == TaskStatus.SUCCESS
    assert r_rev.result == "revaeWhseM"

    # Echo
    r_echo = await registry.execute(TaskRequest.create("echo", args=[1, 2], kwargs={"flag": True}))
    assert r_echo.status == TaskStatus.SUCCESS
    assert r_echo.result == {"args": [1, 2], "kwargs": {"flag": True}}


# ============================================================================
# End-to-End Peer Networking Task Request/Response Integration
# ============================================================================


@pytest.mark.asyncio
async def test_peer_node_task_execution_roundtrip() -> None:
    """
    Integration test: Peer A connects to Peer B over TCP.
    Peer B registers a TaskRegistry to handle MessageType.TASK_REQUEST.
    Peer A sends a TaskRequest, and receives a validated TaskResponse.
    """
    server_registry = TaskRegistry.create_default()

    async with PeerNode(peer_id="client-node", host="127.0.0.1", port=0) as client:
        async with PeerNode(peer_id="worker-node", host="127.0.0.1", port=0) as worker:

            # Register TASK_REQUEST handler on worker node
            async def handle_task_request(msg: Message, _conn: object) -> Message:
                task_req = TaskRequest.from_message(msg)
                task_resp = await server_registry.execute(task_req)
                return task_resp.to_message(
                    sender_id=worker.peer_id,
                    recipient_id=msg.sender_id,
                    correlation_id=msg.message_id,
                )

            worker.register_handler(MessageType.TASK_REQUEST, handle_task_request)

            conn = await client.connect_to_peer("127.0.0.1", worker.bound_port)

            # 1. Execute valid task: add(100, 250)
            req1 = TaskRequest.create("add", args=[100, 250])
            msg1 = req1.to_message(sender_id=client.peer_id)
            reply1 = await client.send_request(conn, msg1, timeout=3.0)

            assert reply1.message_type == MessageType.TASK_RESPONSE
            resp1 = TaskResponse.from_message(reply1)
            assert resp1.status == TaskStatus.SUCCESS
            assert resp1.result == 350
            assert resp1.task_id == req1.task_id

            # 2. Execute unknown task
            req2 = TaskRequest.create("non_existent_task")
            msg2 = req2.to_message(sender_id=client.peer_id)
            reply2 = await client.send_request(conn, msg2, timeout=3.0)

            assert reply2.message_type == MessageType.TASK_RESPONSE
            resp2 = TaskResponse.from_message(reply2)
            assert resp2.status == TaskStatus.ERROR
            assert resp2.error is not None
            assert resp2.error.code == "UNKNOWN_TASK"

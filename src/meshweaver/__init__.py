"""
MeshWeaver: Zero-Dependency P2P Async Task Broker.

A modular Python framework demonstrating decentralized peer discovery,
asynchronous peer-to-peer networking, workload-aware task routing,
and fault monitoring without external broker dependencies.
"""

from meshweaver.exceptions import (
    ConnectionClosedError,
    HandshakeError,
    InvalidPayloadError,
    InvalidTaskError,
    MalformedMessageError,
    MeshWeaverError,
    NetworkTimeoutError,
    PeerConnectionError,
    ProtocolError,
    TaskError,
    TaskExecutionError,
    TaskPayloadLimitError,
    UnknownMessageTypeError,
    UnknownTaskError,
    ValidationError,
)
from meshweaver.network import (
    MessageHandler,
    PeerConnection,
    PeerNode,
)
from meshweaver.protocol import (
    MAX_FRAME_SIZE,
    Message,
    MessageType,
    decode_frame,
    encode_frame,
    read_frame,
    write_frame,
)
from meshweaver.tasks import (
    MAX_TASK_ARGS_COUNT,
    MAX_TASK_ID_LENGTH,
    MAX_TASK_KWARGS_COUNT,
    MAX_TASK_NAME_LENGTH,
    MAX_TASK_PAYLOAD_SIZE,
    MAX_TASK_RESULT_SIZE,
    TaskErrorInfo,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskStatus,
    validate_task_id,
    validate_task_name,
)

__version__: str = "0.1.0"

__all__: list[str] = [
    "MAX_FRAME_SIZE",
    "MAX_TASK_ARGS_COUNT",
    "MAX_TASK_ID_LENGTH",
    "MAX_TASK_KWARGS_COUNT",
    "MAX_TASK_NAME_LENGTH",
    "MAX_TASK_PAYLOAD_SIZE",
    "MAX_TASK_RESULT_SIZE",
    "ConnectionClosedError",
    "HandshakeError",
    "InvalidPayloadError",
    "InvalidTaskError",
    "MalformedMessageError",
    "Message",
    "MessageHandler",
    "MessageType",
    "MeshWeaverError",
    "NetworkTimeoutError",
    "PeerConnection",
    "PeerConnectionError",
    "PeerNode",
    "ProtocolError",
    "TaskError",
    "TaskErrorInfo",
    "TaskExecutionError",
    "TaskPayloadLimitError",
    "TaskRegistry",
    "TaskRequest",
    "TaskResponse",
    "TaskStatus",
    "UnknownMessageTypeError",
    "UnknownTaskError",
    "ValidationError",
    "__version__",
    "decode_frame",
    "encode_frame",
    "read_frame",
    "validate_task_id",
    "validate_task_name",
    "write_frame",
]

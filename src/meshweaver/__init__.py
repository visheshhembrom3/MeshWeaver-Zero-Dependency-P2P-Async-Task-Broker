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
    MalformedMessageError,
    MeshWeaverError,
    NetworkTimeoutError,
    PeerConnectionError,
    ProtocolError,
    UnknownMessageTypeError,
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

__version__: str = "0.1.0"

__all__: list[str] = [
    "MAX_FRAME_SIZE",
    "ConnectionClosedError",
    "HandshakeError",
    "InvalidPayloadError",
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
    "UnknownMessageTypeError",
    "ValidationError",
    "__version__",
    "decode_frame",
    "encode_frame",
    "read_frame",
    "write_frame",
]

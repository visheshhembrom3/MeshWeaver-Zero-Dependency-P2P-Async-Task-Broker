"""
Structured message protocol, schema validation, and framing for peer communication.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import json
import struct
import time
from typing import Any
import uuid

from meshweaver.exceptions import (
    InvalidPayloadError,
    MalformedMessageError,
    UnknownMessageTypeError,
    ValidationError,
)

# Maximum allowed frame size in bytes (10 MB) to prevent memory exhaustion attacks
MAX_FRAME_SIZE: int = 10 * 1024 * 1024


class MessageType(StrEnum):
    """Enumeration of supported peer protocol message types."""

    PING = "PING"
    PONG = "PONG"
    HANDSHAKE = "HANDSHAKE"
    HANDSHAKE_ACK = "HANDSHAKE_ACK"
    HEARTBEAT = "HEARTBEAT"
    TASK_REQUEST = "TASK_REQUEST"
    TASK_RESPONSE = "TASK_RESPONSE"
    LOAD_REPORT = "LOAD_REPORT"
    GOSSIP = "GOSSIP"
    FIND_NODE = "FIND_NODE"
    NODES_FOUND = "NODES_FOUND"
    LEAVE = "LEAVE"
    ERROR = "ERROR"


def _validate_report_fields(data: dict[str, Any], type_name: str) -> None:
    """Validate resource and status report payload fields."""
    if "cpu_percent" in data:
        cpu = data["cpu_percent"]
        if (
            isinstance(cpu, bool)
            or not isinstance(cpu, (int, float))
            or not (0.0 <= cpu <= 100.0)
        ):
            raise InvalidPayloadError(f"{type_name} 'cpu_percent' must be a float between 0.0 and 100.0")
    if "memory_percent" in data:
        mem = data["memory_percent"]
        if (
            isinstance(mem, bool)
            or not isinstance(mem, (int, float))
            or not (0.0 <= mem <= 100.0)
        ):
            raise InvalidPayloadError(f"{type_name} 'memory_percent' must be a float between 0.0 and 100.0")
    if "active_task_count" in data:
        active = data["active_task_count"]
        if isinstance(active, bool) or not isinstance(active, int) or active < 0:
            raise InvalidPayloadError(f"{type_name} 'active_task_count' must be a non-negative integer")
    if "active_tasks" in data:
        active = data["active_tasks"]
        if isinstance(active, bool) or not isinstance(active, int) or active < 0:
            raise InvalidPayloadError(f"{type_name} 'active_tasks' must be a non-negative integer")
    if "timestamp" in data:
        ts = data["timestamp"]
        if isinstance(ts, bool) or not isinstance(ts, (int, float)) or ts < 0:
            raise InvalidPayloadError(f"{type_name} 'timestamp' must be a non-negative number")
    if "health_status" in data:
        health = data["health_status"]
        if not isinstance(health, str) or not health.strip():
            raise InvalidPayloadError(f"{type_name} 'health_status' must be a non-empty string")
    if "node_id" in data:
        nid = data["node_id"]
        if not isinstance(nid, str) or not nid.strip():
            raise InvalidPayloadError(f"{type_name} 'node_id' must be a non-empty string")
    if "hops" in data:
        hops = data["hops"]
        if isinstance(hops, bool) or not isinstance(hops, int) or hops < 0:
            raise InvalidPayloadError(f"{type_name} 'hops' must be a non-negative integer")


def _validate_payload_schema(message_type: MessageType, payload: dict[str, Any]) -> None:
    """
    Validate specific payload requirements based on the message type.

    Raises:
        InvalidPayloadError: If the payload violates type-specific schema constraints.
    """
    if message_type == MessageType.TASK_REQUEST:
        task_id = payload.get("task_id")
        task_name = payload.get("task_name") if "task_name" in payload else payload.get("task_type")
        if not isinstance(task_id, str) or not task_id.strip():
            raise InvalidPayloadError(
                "TASK_REQUEST payload must include a non-empty string 'task_id'"
            )
        if not isinstance(task_name, str) or not task_name.strip():
            raise InvalidPayloadError(
                "TASK_REQUEST payload must include a non-empty string 'task_name' or 'task_type'"
            )
        if "params" in payload and not isinstance(payload["params"], dict):
            raise InvalidPayloadError("TASK_REQUEST 'params' must be a dictionary")
        if "kwargs" in payload and not isinstance(payload["kwargs"], dict):
            raise InvalidPayloadError("TASK_REQUEST 'kwargs' must be a dictionary")
        if "args" in payload and not isinstance(payload["args"], (list, tuple)):
            raise InvalidPayloadError("TASK_REQUEST 'args' must be a list or tuple")


    elif message_type == MessageType.TASK_RESPONSE:
        task_id = payload.get("task_id")
        status = payload.get("status")
        if not isinstance(task_id, str) or not task_id.strip():
            raise InvalidPayloadError(
                "TASK_RESPONSE payload must include a non-empty string 'task_id'"
            )
        if status not in {"success", "failure", "error"}:
            raise InvalidPayloadError(
                "TASK_RESPONSE status must be one of: 'success', 'failure', 'error'"
            )

    elif message_type == MessageType.ERROR:
        if "code" not in payload or "message" not in payload:
            raise InvalidPayloadError("ERROR payload must include 'code' and 'message'")
        if not isinstance(payload["message"], str):
            raise InvalidPayloadError("ERROR payload 'message' must be a string")

    elif message_type == MessageType.HANDSHAKE:
        node_id = payload.get("node_id")
        if not isinstance(node_id, str) or not node_id.strip():
            raise InvalidPayloadError(
                "HANDSHAKE payload must include a non-empty string 'node_id'"
            )
        if "port" in payload:
            port = payload["port"]
            if (
                isinstance(port, bool)
                or not isinstance(port, int)
                or not (1 <= port <= 65535)
            ):
                raise InvalidPayloadError("HANDSHAKE 'port' must be an integer between 1 and 65535")

    elif message_type == MessageType.HANDSHAKE_ACK:
        node_id = payload.get("node_id")
        status = payload.get("status")
        if not isinstance(node_id, str) or not node_id.strip():
            raise InvalidPayloadError(
                "HANDSHAKE_ACK payload must include a non-empty string 'node_id'"
            )
        if not isinstance(status, str) or not status.strip():
            raise InvalidPayloadError(
                "HANDSHAKE_ACK payload must include a non-empty string 'status'"
            )

    elif message_type == MessageType.LOAD_REPORT:
        _validate_report_fields(payload, "LOAD_REPORT")

    elif message_type == MessageType.GOSSIP:
        if "reports" in payload:
            reports = payload["reports"]
            if not isinstance(reports, (list, dict)):
                raise InvalidPayloadError("GOSSIP 'reports' must be a list or dictionary")
            if isinstance(reports, list):
                for item in reports:
                    if not isinstance(item, dict):
                        raise InvalidPayloadError("Each item in GOSSIP 'reports' must be a dictionary")
                    _validate_report_fields(item, "GOSSIP")
            elif isinstance(reports, dict):
                for item in reports.values():
                    if not isinstance(item, dict):
                        raise InvalidPayloadError("Each value in GOSSIP 'reports' must be a dictionary")
                    _validate_report_fields(item, "GOSSIP")
        else:
            _validate_report_fields(payload, "GOSSIP")


    elif message_type == MessageType.FIND_NODE:
        target_id = payload.get("target_id")
        if not isinstance(target_id, str) or not target_id.strip():
            raise InvalidPayloadError(
                "FIND_NODE payload must include a non-empty string 'target_id'"
            )
        if "count" in payload:
            count = payload["count"]
            if (
                isinstance(count, bool)
                or not isinstance(count, int)
                or count <= 0
                or count > 1000
            ):
                raise InvalidPayloadError("FIND_NODE 'count' must be an integer between 1 and 1000")

    elif message_type == MessageType.NODES_FOUND:
        target_id = payload.get("target_id")
        if "target_id" in payload and (not isinstance(target_id, str) or not target_id.strip()):
            raise InvalidPayloadError("NODES_FOUND 'target_id' must be a non-empty string if provided")
        nodes = payload.get("nodes")
        if not isinstance(nodes, (list, tuple)):
            raise InvalidPayloadError("NODES_FOUND payload must include a 'nodes' list")
        for node in nodes:
            if not isinstance(node, dict):
                raise InvalidPayloadError("Each item in NODES_FOUND 'nodes' must be a dictionary")
            peer_id = node.get("peer_id") or node.get("node_id")
            if not isinstance(peer_id, str) or not peer_id.strip():
                raise InvalidPayloadError("Each node in 'nodes' must have a non-empty 'peer_id' or 'node_id'")

    elif message_type == MessageType.LEAVE:
        node_id = payload.get("node_id")
        if node_id is not None and (not isinstance(node_id, str) or not node_id.strip()):
            raise InvalidPayloadError("LEAVE payload 'node_id' must be a non-empty string if provided")


@dataclass(frozen=True, slots=True)
class Message:
    """
    Immutable representation of a structured peer-to-peer message.
    """

    message_id: str
    message_type: MessageType
    sender_id: str
    timestamp: float
    payload: dict[str, Any] = field(default_factory=dict)
    recipient_id: str | None = None
    correlation_id: str | None = None

    @classmethod
    def create(
        cls,
        message_type: MessageType | str,
        sender_id: str,
        payload: dict[str, Any] | None = None,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
        timestamp: float | None = None,
    ) -> Message:
        """
        Factory to construct and validate a new Message instance with sensible defaults.
        """
        final_message_id = message_id if message_id is not None else uuid.uuid4().hex
        final_timestamp = timestamp if timestamp is not None else time.time()
        final_payload = payload if payload is not None else {}

        parsed_type: MessageType
        if isinstance(message_type, MessageType):
            parsed_type = message_type
        elif isinstance(message_type, str):
            try:
                parsed_type = MessageType(message_type)
            except ValueError:
                raise UnknownMessageTypeError(
                    f"Unknown message type: {message_type}"
                ) from None
        else:
            raise UnknownMessageTypeError(
                f"Invalid message type value: {message_type}"
            )

        msg = cls(
            message_id=final_message_id,
            message_type=parsed_type,
            sender_id=sender_id,
            timestamp=final_timestamp,
            payload=final_payload,
            recipient_id=recipient_id,
            correlation_id=correlation_id,
        )
        msg.validate()
        return msg

    def validate(self) -> None:
        """
        Validate the structural integrity and payload rules of this message.

        Raises:
            ValidationError: If envelope fields are missing or have invalid types.
            InvalidPayloadError: If payload is not a valid dict or fails schema checks.
        """
        if not isinstance(self.message_id, str) or not self.message_id.strip():
            raise ValidationError("Field 'message_id' must be a non-empty string")

        if not isinstance(self.sender_id, str) or not self.sender_id.strip():
            raise ValidationError("Field 'sender_id' must be a non-empty string")

        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or self.timestamp < 0
        ):
            raise ValidationError("Field 'timestamp' must be a non-negative number")

        if not isinstance(self.message_type, MessageType):
            raise UnknownMessageTypeError(
                f"Field 'message_type' must be a valid MessageType enum: {self.message_type}"
            )

        if self.recipient_id is not None and (
            not isinstance(self.recipient_id, str) or not self.recipient_id.strip()
        ):
            raise ValidationError("Field 'recipient_id' must be None or a non-empty string")

        if self.correlation_id is not None and (
            not isinstance(self.correlation_id, str) or not self.correlation_id.strip()
        ):
            raise ValidationError("Field 'correlation_id' must be None or a non-empty string")

        if not isinstance(self.payload, dict):
            raise InvalidPayloadError("Field 'payload' must be a dictionary")

        try:
            json.dumps(self.payload)
        except (TypeError, OverflowError) as exc:
            raise InvalidPayloadError(
                f"Field 'payload' must contain only JSON-serializable types: {exc}"
            ) from exc

        _validate_payload_schema(self.message_type, self.payload)

    def to_dict(self) -> dict[str, Any]:
        """Convert the message into a JSON-compatible dictionary."""
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
            "recipient_id": self.recipient_id,
            "correlation_id": self.correlation_id,
            "payload": self.payload,
        }

    def to_json(self) -> str:
        """Serialize the message into a compact JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"), ensure_ascii=False)

    def serialize(self) -> bytes:
        """Serialize the message into UTF-8 encoded bytes."""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_dict(cls, data: Any) -> Message:
        """
        Reconstruct and validate a Message instance from a dictionary.

        Raises:
            MalformedMessageError: If the input is not a dictionary.
            ValidationError: If required envelope fields are missing or invalid.
            UnknownMessageTypeError: If the message type is unrecognized.
            InvalidPayloadError: If the payload fails schema validation.
        """
        if not isinstance(data, dict):
            raise MalformedMessageError(
                f"Expected JSON object dictionary, got {type(data).__name__}"
            )

        required_fields = ("message_id", "message_type", "sender_id", "timestamp", "payload")
        for req in required_fields:
            if req not in data:
                raise ValidationError(f"Missing required envelope field: '{req}'")

        raw_type = data["message_type"]
        if not isinstance(raw_type, str):
            raise UnknownMessageTypeError(
                f"Field 'message_type' must be a string, got {type(raw_type).__name__}"
            )

        try:
            parsed_type = MessageType(raw_type)
        except ValueError:
            raise UnknownMessageTypeError(
                f"Unknown or unsupported message type: '{raw_type}'"
            ) from None

        msg = cls(
            message_id=data["message_id"],
            message_type=parsed_type,
            sender_id=data["sender_id"],
            timestamp=data["timestamp"],
            payload=data["payload"],
            recipient_id=data.get("recipient_id"),
            correlation_id=data.get("correlation_id"),
        )
        msg.validate()
        return msg

    @classmethod
    def from_json(cls, json_str: str) -> Message:
        """
        Reconstruct a Message from a JSON string.

        Raises:
            MalformedMessageError: If JSON decoding fails or root is not an object.
        """
        if not isinstance(json_str, str):
            raise MalformedMessageError("Input must be a string")

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise MalformedMessageError(f"Malformed JSON: {exc.msg}") from exc

        return cls.from_dict(data)

    @classmethod
    def deserialize(cls, data: bytes | bytearray | memoryview) -> Message:
        """
        Reconstruct a Message from UTF-8 encoded bytes.

        Raises:
            MalformedMessageError: If UTF-8 decoding or JSON parsing fails.
        """
        try:
            decoded = bytes(data).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MalformedMessageError(f"Invalid UTF-8 encoding: {exc}") from exc

        return cls.from_json(decoded)


def encode_frame(message: Message) -> bytes:
    """
    Encode a message into a length-prefixed frame.
    Format: 4-byte big-endian unsigned integer (frame length) + payload bytes.
    """
    serialized = message.serialize()
    frame_length = len(serialized)
    if frame_length > MAX_FRAME_SIZE:
        raise MalformedMessageError(
            f"Message size ({frame_length} bytes) exceeds MAX_FRAME_SIZE ({MAX_FRAME_SIZE} bytes)"
        )
    return struct.pack("!I", frame_length) + serialized


def decode_frame(buffer: bytes) -> tuple[Message | None, bytes]:
    """
    Attempt to extract a complete message frame from an in-memory byte buffer.

    Returns:
        tuple[Message | None, bytes]: (decoded message or None if incomplete, remaining buffer bytes)
    """
    if len(buffer) < 4:
        return None, buffer

    frame_length = struct.unpack("!I", buffer[:4])[0]
    if frame_length > MAX_FRAME_SIZE:
        raise MalformedMessageError(
            f"Frame length {frame_length} exceeds MAX_FRAME_SIZE {MAX_FRAME_SIZE}"
        )
    if frame_length == 0:
        raise MalformedMessageError("Frame length must be greater than zero")

    total_length = 4 + frame_length
    if len(buffer) < total_length:
        return None, buffer

    payload_bytes = buffer[4:total_length]
    message = Message.deserialize(payload_bytes)
    return message, buffer[total_length:]


async def read_frame(reader: asyncio.StreamReader) -> Message | None:
    """
    Read a length-prefixed frame from an asyncio.StreamReader.

    Returns:
        Message | None: Decoded Message, or None if stream reached clean EOF before header.

    Raises:
        MalformedMessageError: If stream terminates mid-frame or framing limits are violated.
    """
    try:
        header = await reader.readexactly(4)
    except asyncio.IncompleteReadError as exc:
        if len(exc.partial) == 0:
            return None
        raise MalformedMessageError("Stream closed while reading frame header") from exc

    frame_length = struct.unpack("!I", header)[0]
    if frame_length > MAX_FRAME_SIZE:
        raise MalformedMessageError(
            f"Frame length {frame_length} exceeds MAX_FRAME_SIZE {MAX_FRAME_SIZE}"
        )
    if frame_length == 0:
        raise MalformedMessageError("Frame length must be greater than zero")

    try:
        body = await reader.readexactly(frame_length)
    except asyncio.IncompleteReadError as exc:
        raise MalformedMessageError(
            f"Incomplete frame body: expected {frame_length} bytes, received {len(exc.partial)}"
        ) from exc

    return Message.deserialize(body)


def write_frame(writer: asyncio.StreamWriter, message: Message) -> None:
    """
    Write a length-prefixed frame to an asyncio.StreamWriter.
    """
    writer.write(encode_frame(message))

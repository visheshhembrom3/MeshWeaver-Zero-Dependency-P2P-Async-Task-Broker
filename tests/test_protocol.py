"""
Unit tests for the MeshWeaver messaging protocol, validation, and framing.
"""

import asyncio
import json
import struct
import pytest

from meshweaver import (
    MAX_FRAME_SIZE,
    InvalidPayloadError,
    MalformedMessageError,
    Message,
    MessageType,
    UnknownMessageTypeError,
    ValidationError,
    decode_frame,
    encode_frame,
    read_frame,
)


def test_create_valid_message_defaults() -> None:
    """Verify that Message.create generates a valid message with default fields."""
    msg = Message.create(MessageType.PING, sender_id="node-alpha")

    assert isinstance(msg.message_id, str) and len(msg.message_id) > 0
    assert msg.message_type == MessageType.PING
    assert msg.sender_id == "node-alpha"
    assert isinstance(msg.timestamp, float)
    assert msg.payload == {}
    assert msg.recipient_id is None
    assert msg.correlation_id is None


def test_serialization_deserialization_roundtrip() -> None:
    """Verify JSON and bytes round-trip serialization for structured messages."""
    original = Message.create(
        MessageType.TASK_REQUEST,
        sender_id="node-1",
        recipient_id="node-2",
        correlation_id="corr-999",
        payload={
            "task_id": "t-100",
            "task_type": "compute_sum",
            "params": {"a": 10, "b": 20},
        },
    )

    data_bytes = original.serialize()
    reconstructed = Message.deserialize(data_bytes)

    assert reconstructed == original
    assert reconstructed.message_id == original.message_id
    assert reconstructed.message_type == MessageType.TASK_REQUEST
    assert reconstructed.sender_id == "node-1"
    assert reconstructed.recipient_id == "node-2"
    assert reconstructed.correlation_id == "corr-999"
    assert reconstructed.payload["task_id"] == "t-100"
    assert reconstructed.payload["params"] == {"a": 10, "b": 20}


@pytest.mark.parametrize(
    "msg_type,payload",
    [
        (MessageType.PING, {}),
        (MessageType.PONG, {"ack_time": 12345.6}),
        (MessageType.HANDSHAKE, {"node_id": "peer-x", "port": 8000}),
        (MessageType.HANDSHAKE_ACK, {"node_id": "peer-y", "status": "accepted"}),
        (MessageType.HEARTBEAT, {"status": "ok"}),
        (MessageType.TASK_RESPONSE, {"task_id": "t-1", "status": "success", "result": 42}),
        (MessageType.LOAD_REPORT, {"cpu_percent": 24.5, "memory_percent": 65.0}),
        (MessageType.GOSSIP, {"node_id": "peer-z", "cpu_percent": 10.0, "memory_percent": 25.0, "active_task_count": 1, "health_status": "healthy"}),
        (MessageType.ERROR, {"code": "ERR_BUSY", "message": "Peer overloaded"}),
    ],
)
def test_valid_payloads_for_all_message_types(msg_type: MessageType, payload: dict) -> None:
    """Verify that all standard message types validate with appropriate payloads."""
    msg = Message.create(msg_type, sender_id="node-test", payload=payload)
    serialized = msg.to_json()
    reconstructed = Message.from_json(serialized)
    assert reconstructed.message_type == msg_type
    assert reconstructed.payload == payload


def test_malformed_json_handling() -> None:
    """Verify that unparseable JSON or non-object payloads raise MalformedMessageError."""
    with pytest.raises(MalformedMessageError, match="Malformed JSON"):
        Message.from_json("invalid-json{not_valid")

    with pytest.raises(MalformedMessageError, match="Expected JSON object"):
        Message.from_json('["a", "list", "instead", "of", "dict"]')

    with pytest.raises(MalformedMessageError, match="Expected JSON object"):
        Message.from_json('"just a string"')

    with pytest.raises(MalformedMessageError, match="Invalid UTF-8 encoding"):
        Message.deserialize(b"\xff\xfe\x00")

    with pytest.raises(MalformedMessageError, match="Input must be a string"):
        Message.from_json(12345)  # type: ignore[arg-type]


def test_unknown_message_types() -> None:
    """Verify that unrecognized message types raise UnknownMessageTypeError."""
    with pytest.raises(UnknownMessageTypeError, match="Unknown message type"):
        Message.create("NON_EXISTENT_TYPE", sender_id="node-1")

    raw_dict = {
        "message_id": "m-1",
        "message_type": "UNKNOWN_ACTION",
        "sender_id": "node-1",
        "timestamp": 1700000000.0,
        "payload": {},
    }
    with pytest.raises(UnknownMessageTypeError, match="Unknown or unsupported message type"):
        Message.from_dict(raw_dict)

    raw_dict["message_type"] = 12345  # type: ignore[assignment]
    with pytest.raises(UnknownMessageTypeError, match="must be a string"):
        Message.from_dict(raw_dict)


def test_invalid_payload_types() -> None:
    """Verify that non-dict or non-serializable payloads raise InvalidPayloadError."""
    with pytest.raises(InvalidPayloadError, match="Field 'payload' must be a dictionary"):
        Message.create(MessageType.PING, sender_id="node-1", payload="not-a-dict")  # type: ignore[arg-type]

    with pytest.raises(InvalidPayloadError, match="JSON-serializable"):
        Message.create(
            MessageType.PING,
            sender_id="node-1",
            payload={"invalid_object": object()},
        )


def test_task_request_payload_validation() -> None:
    """Verify schema constraints for TASK_REQUEST messages."""
    # Missing task_id
    with pytest.raises(InvalidPayloadError, match="'task_id'"):
        Message.create(
            MessageType.TASK_REQUEST,
            sender_id="node-1",
            payload={"task_type": "compute"},
        )

    # Missing task_type
    with pytest.raises(InvalidPayloadError, match="'task_type'"):
        Message.create(
            MessageType.TASK_REQUEST,
            sender_id="node-1",
            payload={"task_id": "t-1"},
        )

    # Invalid params type
    with pytest.raises(InvalidPayloadError, match="'params' must be a dictionary"):
        Message.create(
            MessageType.TASK_REQUEST,
            sender_id="node-1",
            payload={"task_id": "t-1", "task_type": "compute", "params": "not-a-dict"},
        )


def test_task_response_payload_validation() -> None:
    """Verify schema constraints for TASK_RESPONSE messages."""
    # Missing status
    with pytest.raises(InvalidPayloadError, match="status"):
        Message.create(
            MessageType.TASK_RESPONSE,
            sender_id="node-1",
            payload={"task_id": "t-1"},
        )

    # Invalid status value
    with pytest.raises(InvalidPayloadError, match="must be one of"):
        Message.create(
            MessageType.TASK_RESPONSE,
            sender_id="node-1",
            payload={"task_id": "t-1", "status": "pending"},
        )


def test_error_and_handshake_payload_validation() -> None:
    """Verify schema constraints for ERROR and HANDSHAKE messages."""
    with pytest.raises(InvalidPayloadError, match="must include 'code' and 'message'"):
        Message.create(MessageType.ERROR, sender_id="node-1", payload={"code": 500})

    with pytest.raises(InvalidPayloadError, match="'port' must be an integer"):
        Message.create(
            MessageType.HANDSHAKE,
            sender_id="node-1",
            payload={"node_id": "peer-1", "port": 70000},
        )

    with pytest.raises(InvalidPayloadError, match="'cpu_percent' must be a float"):
        Message.create(
            MessageType.LOAD_REPORT,
            sender_id="node-1",
            payload={"cpu_percent": 150.0},
        )


def test_envelope_validation_errors() -> None:
    """Verify envelope constraints (missing fields, blank strings, invalid timestamps)."""
    base = {
        "message_id": "msg-123",
        "message_type": "PING",
        "sender_id": "peer-1",
        "timestamp": 12345.67,
        "payload": {},
    }

    # Missing required field
    for field_name in ("message_id", "message_type", "sender_id", "timestamp", "payload"):
        tampered = dict(base)
        del tampered[field_name]
        with pytest.raises(ValidationError, match=f"Missing required envelope field: '{field_name}'"):
            Message.from_dict(tampered)

    # Blank sender_id
    tampered = dict(base, sender_id="   ")
    with pytest.raises(ValidationError, match="'sender_id' must be a non-empty string"):
        Message.from_dict(tampered)

    # Boolean timestamp (bool is subclass of int in Python)
    tampered = dict(base, timestamp=True)
    with pytest.raises(ValidationError, match="'timestamp' must be a non-negative number"):
        Message.from_dict(tampered)

    # Negative timestamp
    tampered = dict(base, timestamp=-1.0)
    with pytest.raises(ValidationError, match="'timestamp' must be a non-negative number"):
        Message.from_dict(tampered)


def test_frame_encoding_and_decoding() -> None:
    """Verify length-prefixed framing and stream extraction."""
    msg = Message.create(MessageType.PING, sender_id="node-a")
    frame = encode_frame(msg)

    # Check header
    header_len = struct.unpack("!I", frame[:4])[0]
    assert header_len == len(msg.serialize())

    # Full frame decode
    decoded, remaining = decode_frame(frame)
    assert decoded == msg
    assert remaining == b""

    # Incomplete buffer
    partial_frame = frame[:10]
    incomplete, rem = decode_frame(partial_frame)
    assert incomplete is None
    assert rem == partial_frame

    # Excessive frame size
    tampered_header = struct.pack("!I", MAX_FRAME_SIZE + 1) + frame[4:]
    with pytest.raises(MalformedMessageError, match="exceeds MAX_FRAME_SIZE"):
        decode_frame(tampered_header)


@pytest.mark.asyncio
async def test_async_read_frame() -> None:
    """Verify reading frames asynchronously from StreamReader."""
    msg = Message.create(MessageType.PING, sender_id="node-async")
    frame = encode_frame(msg)

    reader = asyncio.StreamReader()
    reader.feed_data(frame)
    reader.feed_eof()

    result = await read_frame(reader)
    assert result == msg

    # Next read should encounter clean EOF and return None
    eof_result = await read_frame(reader)
    assert eof_result is None

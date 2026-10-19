"""
Exceptions for the MeshWeaver protocol and messaging system.
"""


class MeshWeaverError(Exception):
    """Base exception for all MeshWeaver errors."""


class ProtocolError(MeshWeaverError):
    """Base exception for protocol-related errors."""


class ValidationError(ProtocolError):
    """Raised when a message envelope or field fails structural validation."""


class MalformedMessageError(ProtocolError):
    """Raised when incoming message bytes or string cannot be parsed as valid JSON."""


class UnknownMessageTypeError(ProtocolError):
    """Raised when an unhandled or unrecognized message type is encountered."""


class InvalidPayloadError(ValidationError):
    """Raised when a message payload is structurally invalid or fails schema rules."""

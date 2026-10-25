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


class PeerConnectionError(MeshWeaverError):
    """Base exception for peer network and connection failures."""


class ConnectionClosedError(PeerConnectionError):
    """Raised when an operation is attempted on a closed connection."""


class NetworkTimeoutError(PeerConnectionError):
    """Raised when a network operation exceeds its timeout."""


class HandshakeError(PeerConnectionError):
    """Raised when a peer-to-peer handshake fails."""


class TaskError(MeshWeaverError):
    """Base exception for task-related errors."""


class InvalidTaskError(ValidationError, TaskError):
    """Raised when a task request, response, or payload fails schema validation."""


class TaskPayloadLimitError(InvalidTaskError):
    """Raised when a task payload, field, or result exceeds size, count, or depth limits."""


class UnknownTaskError(TaskError):
    """Raised when a task handler is requested for an unregistered task name."""


class TaskExecutionError(TaskError):
    """Raised when an error occurs during local execution of a registered task."""


class TaskRoutingError(TaskError):
    """Raised when an error occurs while routing a task to peers."""


class NoAvailablePeerError(TaskRoutingError):
    """Raised when no suitable or reachable peer is available to execute a task."""


class TaskTimeoutError(NetworkTimeoutError, TaskError):
    """Raised when remote task execution exceeds configured timeout."""


class PeerRegistryError(MeshWeaverError):
    """Base exception for peer registry and membership tracking errors."""


class InvalidPeerError(ValidationError, PeerRegistryError):
    """Raised when a peer ID, endpoint, or record fails schema validation."""


class DuplicatePeerError(PeerRegistryError):
    """Raised when a duplicate peer registration conflicts with registry constraints."""


class PeerNotFoundError(PeerRegistryError):
    """Raised when an operation targets a peer ID that is not present in the registry."""


class DHTError(MeshWeaverError):
    """Base exception for DHT and peer discovery errors."""


class InvalidNodeIDError(ValidationError, DHTError):
    """Raised when a NodeID is malformed, invalid, or out of range."""


class LookupTimeoutError(NetworkTimeoutError, DHTError):
    """Raised when an iterative DHT lookup exceeds its configured timeout."""


class LookupError(DHTError):
    """Raised when an error occurs during peer lookup or routing."""


class GossipError(MeshWeaverError):
    """Base exception for gossip protocol and resource reporting errors."""


class StaleDataError(GossipError):
    """Raised when an incoming gossip record is rejected because it is stale."""


class InvalidReportError(ValidationError, GossipError):
    """Raised when a resource or gossip report fails validation."""


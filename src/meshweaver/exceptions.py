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


class SecurityError(MeshWeaverError):
    """Base exception for all security, authentication, and integrity errors."""


class AuthenticationError(SecurityError, PeerConnectionError):
    """Raised when peer identity authentication or authorization fails."""


class IntegrityError(SecurityError, ProtocolError):
    """Raised when message HMAC signature or cryptographic integrity check fails."""


class ReplayAttackError(SecurityError, ProtocolError):
    """Raised when a replayed or expired message is detected."""


class PayloadLimitExceededError(SecurityError, ValidationError):
    """Raised when a message or frame size exceeds configured security limits."""


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


class TaskSchedulingError(TaskRoutingError):
    """Raised when an error occurs while scheduling a task to an eligible worker."""


class NoEligibleWorkerError(NoAvailablePeerError, TaskSchedulingError):
    """Raised when no worker meets eligibility criteria for task execution."""


class IncompatibleWorkerError(TaskSchedulingError):
    """Raised when a candidate worker does not support the requested task or requirements."""


class DuplicateSubmissionError(TaskError):
    """Raised when a duplicate task submission is detected and rejected."""


class TaskNotFoundError(TaskError):
    """Raised when an operation targets a task ID that does not exist or cannot be found."""


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


class StaleResourceReportError(StaleDataError, TaskSchedulingError):
    """Raised when a candidate worker's resource report is rejected due to staleness."""


class InvalidReportError(ValidationError, GossipError):
    """Raised when a resource or gossip report fails validation."""


class HeartbeatError(PeerConnectionError):
    """Base exception for heartbeat probing and failure detection errors."""


class HeartbeatTimeoutError(HeartbeatError, NetworkTimeoutError):
    """Raised when a heartbeat probe to a remote peer times out."""


class PeerUnresponsiveError(HeartbeatError):
    """Raised when a peer node misses heartbeats beyond the configured threshold."""


class WorkerDisconnectedError(TaskError, ConnectionClosedError):
    """Raised when a remote worker peer disconnects while executing an in-flight task."""


class TaskRetryExhaustedError(TaskError):
    """Raised when task execution fails across all bounded retry attempts."""


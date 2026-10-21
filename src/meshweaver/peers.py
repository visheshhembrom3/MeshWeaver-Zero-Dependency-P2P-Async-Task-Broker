"""
Dynamic peer registry, membership tracking, network endpoints, and stale record expiration.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from enum import StrEnum
import ipaddress
import json
import logging
import re
import time
from typing import Any

from meshweaver.exceptions import (
    InvalidPeerError,
    PeerNotFoundError,
    PeerRegistryError,
)

logger = logging.getLogger(__name__)

# Constants and limits
MAX_PEER_ID_LENGTH: int = 128
MAX_HOST_LENGTH: int = 253
PEER_ID_PATTERN: re.Pattern[str] = re.compile(r"^[a-zA-Z0-9_\-]+$")
HOSTNAME_PATTERN: re.Pattern[str] = re.compile(
    r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)*[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?$"
)

# Default timing configuration in seconds
DEFAULT_STALE_THRESHOLD: float = 60.0
DEFAULT_TTL: float = 300.0
DEFAULT_MAX_PEERS: int = 1000


def validate_peer_id(peer_id: Any) -> str:
    """
    Validate that peer_id is a non-empty string adhering to identifier constraints.

    Raises:
        InvalidPeerError: If peer_id is not a string, is empty, or contains invalid characters.
    """
    if not isinstance(peer_id, str):
        raise InvalidPeerError(f"Peer ID must be a string, got {type(peer_id).__name__}")
    stripped = peer_id.strip()
    if not stripped:
        raise InvalidPeerError("Peer ID must not be empty or whitespace only")
    if len(peer_id) > MAX_PEER_ID_LENGTH:
        raise InvalidPeerError(
            f"Peer ID exceeds maximum length of {MAX_PEER_ID_LENGTH} characters (got {len(peer_id)})"
        )
    if not PEER_ID_PATTERN.match(peer_id):
        raise InvalidPeerError(
            f"Peer ID '{peer_id}' contains invalid characters. Must match {PEER_ID_PATTERN.pattern}"
        )
    return peer_id


def validate_host(host: Any) -> str:
    """
    Validate that host is a valid IPv4/IPv6 address or standard hostname.

    Raises:
        InvalidPeerError: If host is malformed, invalid, or exceeds length limits.
    """
    if not isinstance(host, str):
        raise InvalidPeerError(f"Host must be a string, got {type(host).__name__}")
    stripped = host.strip()
    if not stripped:
        raise InvalidPeerError("Host must not be empty or whitespace only")
    if len(host) > MAX_HOST_LENGTH:
        raise InvalidPeerError(
            f"Host exceeds maximum length of {MAX_HOST_LENGTH} characters (got {len(host)})"
        )

    # 1. Attempt IP address parsing (IPv4 / IPv6)
    try:
        ipaddress.ip_address(stripped)
        return stripped
    except ValueError:
        pass

    # Reject invalid IPv4-like strings (e.g. 256.256.256.256 or 4 numeric labels)
    parts = stripped.split(".")
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        raise InvalidPeerError(f"Host '{host}' is an invalid IPv4 address")

    # Reject hosts with all-numeric TLDs (RFC 1123 / RFC 3696)
    if len(parts) > 1 and parts[-1].isdigit():
        raise InvalidPeerError(f"Host '{host}' has an invalid numeric TLD")

    # 2. Hostname validation (localhost or standard DNS names)
    if stripped.lower() == "localhost":
        return stripped

    if HOSTNAME_PATTERN.match(stripped):
        return stripped

    raise InvalidPeerError(f"Host '{host}' is neither a valid IP address nor a valid hostname")


def validate_port(port: Any) -> int:
    """
    Validate that port is an integer between 1 and 65535.

    Raises:
        InvalidPeerError: If port is not an integer or outside the valid TCP/UDP port range.
    """
    if isinstance(port, bool) or not isinstance(port, int):
        raise InvalidPeerError(f"Port must be an integer, got {type(port).__name__}")
    if not (1 <= port <= 65535):
        raise InvalidPeerError(f"Port must be between 1 and 65535, got {port}")
    return port


@dataclass(frozen=True, slots=True)
class PeerEndpoint:
    """
    Immutable representation of a validated network endpoint (host and port).
    """

    host: str
    port: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "host", validate_host(self.host))
        object.__setattr__(self, "port", validate_port(self.port))

    def to_tuple(self) -> tuple[str, int]:
        """Return endpoint as a (host, port) tuple."""
        return self.host, self.port

    def to_dict(self) -> dict[str, Any]:
        """Convert endpoint to a JSON-compatible dictionary."""
        return {"host": self.host, "port": self.port}

    @classmethod
    def from_dict(cls, data: Any) -> PeerEndpoint:
        """Construct PeerEndpoint from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidPeerError(f"Expected dict for PeerEndpoint, got {type(data).__name__}")
        if "host" not in data or "port" not in data:
            raise InvalidPeerError("PeerEndpoint requires 'host' and 'port' fields")
        return cls(host=data["host"], port=data["port"])

    @classmethod
    def from_address(cls, addr: tuple[str, int] | str | PeerEndpoint) -> PeerEndpoint:
        """Construct PeerEndpoint from tuple, string ('host:port'), or existing PeerEndpoint."""
        if isinstance(addr, PeerEndpoint):
            return addr
        if isinstance(addr, tuple) and len(addr) == 2:
            return cls(host=addr[0], port=addr[1])
        if isinstance(addr, str):
            # Parse 'host:port' or '[ipv6]:port'
            if addr.startswith("[") and "]:" in addr:
                host_part, port_part = addr[1:].split("]:", 1)
            elif ":" in addr:
                host_part, port_part = addr.rsplit(":", 1)
            else:
                raise InvalidPeerError(f"String address must be in 'host:port' format, got '{addr}'")
            try:
                parsed_port = int(port_part)
            except ValueError:
                raise InvalidPeerError(f"Invalid port in address '{addr}'") from None
            return cls(host=host_part, port=parsed_port)

        raise InvalidPeerError(f"Cannot construct PeerEndpoint from {type(addr).__name__}")

    def __str__(self) -> str:
        if ":" in self.host and not self.host.startswith("["):
            return f"[{self.host}]:{self.port}"
        return f"{self.host}:{self.port}"


class PeerState(StrEnum):
    """Lifecycle and freshness state of a tracked peer."""

    ACTIVE = "active"
    STALE = "stale"
    EXPIRED = "expired"


@dataclass(frozen=True, slots=True)
class PeerRecord:
    """
    Immutable representation of a tracked peer node in the local registry.
    """

    peer_id: str
    endpoint: PeerEndpoint
    last_seen: float
    registered_at: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        if not isinstance(self.endpoint, PeerEndpoint):
            object.__setattr__(self, "endpoint", PeerEndpoint.from_address(self.endpoint))
        if (
            isinstance(self.last_seen, bool)
            or not isinstance(self.last_seen, (int, float))
            or self.last_seen < 0
        ):
            raise InvalidPeerError("Field 'last_seen' must be a non-negative number")
        if (
            isinstance(self.registered_at, bool)
            or not isinstance(self.registered_at, (int, float))
            or self.registered_at < 0
        ):
            raise InvalidPeerError("Field 'registered_at' must be a non-negative number")
        if not isinstance(self.metadata, dict):
            raise InvalidPeerError("Field 'metadata' must be a dictionary")

    @property
    def node_id(self) -> str:
        """Alias for peer_id."""
        return self.peer_id

    @property
    def dht_id(self) -> Any:
        """Return deterministic NodeID for DHT operations."""
        from meshweaver.dht import NodeID

        return NodeID.from_peer(self)

    def to_contact(self, node_id: Any = None) -> Any:
        """Convert this PeerRecord to a DHT PeerContact."""
        from meshweaver.dht import PeerContact

        return PeerContact.from_peer_record(self, node_id=node_id)

    @property
    def host(self) -> str:
        """Host address of the peer's endpoint."""
        return self.endpoint.host

    @property
    def port(self) -> int:
        """Port of the peer's endpoint."""
        return self.endpoint.port

    def state(
        self,
        current_time: float | None = None,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        ttl: float = DEFAULT_TTL,
    ) -> PeerState:
        """Compute the current freshness state of this peer record."""
        now = current_time if current_time is not None else time.time()
        elapsed = now - self.last_seen
        if elapsed > ttl:
            return PeerState.EXPIRED
        if elapsed > stale_threshold:
            return PeerState.STALE
        return PeerState.ACTIVE

    def is_active(
        self,
        current_time: float | None = None,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
    ) -> bool:
        """Return True if peer was seen within stale_threshold."""
        now = current_time if current_time is not None else time.time()
        return (now - self.last_seen) <= stale_threshold

    def is_stale(
        self,
        current_time: float | None = None,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        ttl: float = DEFAULT_TTL,
    ) -> bool:
        """Return True if peer has exceeded stale_threshold but has not yet expired."""
        now = current_time if current_time is not None else time.time()
        elapsed = now - self.last_seen
        return stale_threshold < elapsed <= ttl

    def is_expired(
        self,
        current_time: float | None = None,
        ttl: float = DEFAULT_TTL,
    ) -> bool:
        """Return True if peer has exceeded ttl."""
        now = current_time if current_time is not None else time.time()
        return (now - self.last_seen) > ttl

    def touch(
        self,
        current_time: float | None = None,
        *,
        endpoint: PeerEndpoint | tuple[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PeerRecord:
        """Return a new PeerRecord with updated last_seen and optional endpoint/metadata."""
        now = current_time if current_time is not None else time.time()
        new_endpoint = PeerEndpoint.from_address(endpoint) if endpoint is not None else self.endpoint
        new_metadata = dict(metadata) if metadata is not None else self.metadata
        return PeerRecord(
            peer_id=self.peer_id,
            endpoint=new_endpoint,
            last_seen=now,
            registered_at=self.registered_at,
            metadata=new_metadata,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert peer record to a JSON-compatible dictionary."""
        return {
            "peer_id": self.peer_id,
            "node_id": self.peer_id,
            "endpoint": self.endpoint.to_dict(),
            "host": self.host,
            "port": self.port,
            "last_seen": self.last_seen,
            "registered_at": self.registered_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Any) -> PeerRecord:
        """Construct PeerRecord from a dictionary."""
        if not isinstance(data, dict):
            raise InvalidPeerError(f"Expected dict for PeerRecord, got {type(data).__name__}")
        peer_id = data.get("peer_id") or data.get("node_id")
        if not peer_id:
            raise InvalidPeerError("Missing required field 'peer_id' or 'node_id'")

        endpoint_data = data.get("endpoint")
        if endpoint_data is not None and isinstance(endpoint_data, dict):
            endpoint = PeerEndpoint.from_dict(endpoint_data)
        elif "host" in data and "port" in data:
            endpoint = PeerEndpoint(host=data["host"], port=data["port"])
        else:
            raise InvalidPeerError("PeerRecord requires 'endpoint' or 'host' and 'port'")

        last_seen = data.get("last_seen", time.time())
        registered_at = data.get("registered_at", last_seen)
        metadata = data.get("metadata", {})

        return cls(
            peer_id=peer_id,
            endpoint=endpoint,
            last_seen=last_seen,
            registered_at=registered_at,
            metadata=dict(metadata) if isinstance(metadata, dict) else {},
        )


class PeerRegistry:
    """
    Dynamic in-memory peer registry for local peer membership tracking.

    Responsibilities:
    - Maintains active, validated peer records indexed by peer ID and network endpoint.
    - Idempotently adds or updates peer records without creating duplicate entries.
    - Handles endpoint collisions (superseded records on re-used ports).
    - Classifies peers into ACTIVE, STALE, and EXPIRED states.
    - Purges expired peer records deterministically based on configurable TTL.
    """

    def __init__(
        self,
        *,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        ttl: float = DEFAULT_TTL,
        max_peers: int = DEFAULT_MAX_PEERS,
    ) -> None:
        if stale_threshold < 0:
            raise InvalidPeerError("stale_threshold must be non-negative")
        if ttl <= 0:
            raise InvalidPeerError("ttl must be greater than zero")
        if ttl < stale_threshold:
            raise InvalidPeerError("ttl must be greater than or equal to stale_threshold")
        if max_peers <= 0:
            raise InvalidPeerError("max_peers must be greater than zero")

        self.stale_threshold: float = float(stale_threshold)
        self.ttl: float = float(ttl)
        self.max_peers: int = int(max_peers)

        # Primary store: peer_id -> PeerRecord
        self._peers: dict[str, PeerRecord] = {}
        # Secondary index: (host, port) -> peer_id
        self._endpoint_index: dict[tuple[str, int], str] = {}

    def register(
        self,
        peer_id: str,
        host: str,
        port: int,
        *,
        metadata: dict[str, Any] | None = None,
        current_time: float | None = None,
    ) -> PeerRecord:
        """
        Register or update a peer endpoint.

        If peer_id already exists, its endpoint, last_seen, and metadata are refreshed.
        If a different peer previously occupied (host, port), the old record is superseded.
        """
        now = current_time if current_time is not None else time.time()
        endpoint = PeerEndpoint(host=host, port=port)
        validated_id = validate_peer_id(peer_id)

        existing = self._peers.get(validated_id)
        if existing is not None:
            # Update existing record
            merged_metadata = dict(existing.metadata)
            if metadata is not None:
                merged_metadata.update(metadata)

            updated = existing.touch(current_time=now, endpoint=endpoint, metadata=merged_metadata)
            self._insert_record(updated)
            logger.debug("Updated peer record: %s at %s", validated_id, endpoint)
            return updated

        # New peer record
        record = PeerRecord(
            peer_id=validated_id,
            endpoint=endpoint,
            last_seen=now,
            registered_at=now,
            metadata=dict(metadata) if metadata is not None else {},
        )
        return self.add(record, current_time=now)

    def add(self, record: PeerRecord, *, current_time: float | None = None) -> PeerRecord:
        """
        Add a PeerRecord to the registry, handling capacity limits and deduplication.
        """
        if not isinstance(record, PeerRecord):
            raise InvalidPeerError(f"Expected PeerRecord instance, got {type(record).__name__}")

        now = current_time if current_time is not None else time.time()

        # If new peer and at capacity, attempt to prune expired records first
        if record.peer_id not in self._peers and len(self._peers) >= self.max_peers:
            self.expire_stale_peers(current_time=now)
            if len(self._peers) >= self.max_peers:
                raise PeerRegistryError(
                    f"Peer registry capacity limit of {self.max_peers} peers reached"
                )

        self._insert_record(record)
        logger.debug("Added peer record: %s at %s", record.peer_id, record.endpoint)
        return record

    def _insert_record(self, record: PeerRecord) -> None:
        """Internal helper to insert and maintain index consistency."""
        endpoint_key = record.endpoint.to_tuple()

        # Check if another peer currently occupies this endpoint
        prior_peer_id = self._endpoint_index.get(endpoint_key)
        if prior_peer_id and prior_peer_id != record.peer_id:
            logger.info(
                "Endpoint %s was previously registered to peer %s; superseding with %s",
                endpoint_key,
                prior_peer_id,
                record.peer_id,
            )
            self.remove(prior_peer_id)

        # If this peer had a previous endpoint, clean up its old index entry
        existing = self._peers.get(record.peer_id)
        if existing is not None and existing.endpoint.to_tuple() != endpoint_key:
            self._endpoint_index.pop(existing.endpoint.to_tuple(), None)

        self._peers[record.peer_id] = record
        self._endpoint_index[endpoint_key] = record.peer_id

    def update(
        self,
        peer_id: str,
        *,
        endpoint: PeerEndpoint | tuple[str, int] | None = None,
        metadata: dict[str, Any] | None = None,
        current_time: float | None = None,
    ) -> PeerRecord:
        """
        Update an existing peer's last_seen timestamp and optional endpoint/metadata.

        Raises:
            PeerNotFoundError: If peer_id is not found in the registry.
        """
        existing = self._peers.get(peer_id)
        if existing is None:
            raise PeerNotFoundError(f"Peer '{peer_id}' is not in the registry")

        now = current_time if current_time is not None else time.time()
        new_endpoint = PeerEndpoint.from_address(endpoint) if endpoint is not None else existing.endpoint
        merged_meta = dict(existing.metadata)
        if metadata is not None:
            merged_meta.update(metadata)

        updated = existing.touch(current_time=now, endpoint=new_endpoint, metadata=merged_meta)
        self._insert_record(updated)
        return updated

    def touch(self, peer_id: str, current_time: float | None = None) -> PeerRecord:
        """Refresh the last_seen timestamp for a registered peer."""
        return self.update(peer_id, current_time=current_time)

    def remove(self, peer_id: str) -> PeerRecord | None:
        """
        Remove a peer from the registry by ID.

        Returns:
            PeerRecord | None: The removed record, or None if not found.
        """
        record = self._peers.pop(peer_id, None)
        if record is not None:
            self._endpoint_index.pop(record.endpoint.to_tuple(), None)
            logger.debug("Removed peer record: %s", peer_id)
        return record

    def remove_by_endpoint(self, host: str, port: int) -> PeerRecord | None:
        """Remove a peer from the registry by its network endpoint."""
        key = (validate_host(host), validate_port(port))
        peer_id = self._endpoint_index.get(key)
        if peer_id is not None:
            return self.remove(peer_id)
        return None

    def get(self, peer_id: str) -> PeerRecord | None:
        """Retrieve a PeerRecord by peer ID."""
        return self._peers.get(peer_id)

    def get_by_endpoint(self, host: str, port: int) -> PeerRecord | None:
        """Retrieve a PeerRecord by network endpoint."""
        key = (validate_host(host), validate_port(port))
        peer_id = self._endpoint_index.get(key)
        if peer_id is not None:
            return self._peers.get(peer_id)
        return None

    def has_peer(self, peer_id: str) -> bool:
        """Return True if peer_id is present in the registry."""
        return peer_id in self._peers

    def has_endpoint(self, host: str, port: int) -> bool:
        """Return True if (host, port) is registered."""
        key = (validate_host(host), validate_port(port))
        return key in self._endpoint_index

    def get_peer_state(
        self,
        peer_id: str,
        current_time: float | None = None,
    ) -> PeerState | None:
        """Return the freshness state of a peer, or None if not found."""
        record = self.get(peer_id)
        if record is None:
            return None
        return record.state(
            current_time=current_time,
            stale_threshold=self.stale_threshold,
            ttl=self.ttl,
        )

    def list_peers(
        self,
        *,
        state: PeerState | str | None = None,
        active_only: bool = False,
        include_expired: bool = False,
        current_time: float | None = None,
    ) -> list[PeerRecord]:
        """
        Return a list of tracked peer records matching the requested filter.

        Args:
            state: Filter to only peers in this specific PeerState.
            active_only: If True, only returns peers in PeerState.ACTIVE.
            include_expired: If False, omits expired records unless specifically requested.
            current_time: Deterministic timestamp for state calculation.
        """
        now = current_time if current_time is not None else time.time()
        results: list[PeerRecord] = []

        target_state: PeerState | None = None
        if state is not None:
            target_state = PeerState(state)
        elif active_only:
            target_state = PeerState.ACTIVE

        for record in self._peers.values():
            rec_state = record.state(
                current_time=now,
                stale_threshold=self.stale_threshold,
                ttl=self.ttl,
            )
            if target_state is not None:
                if rec_state == target_state:
                    results.append(record)
            else:
                if not include_expired and rec_state == PeerState.EXPIRED:
                    continue
                results.append(record)

        # Sort primarily by last_seen descending
        results.sort(key=lambda r: r.last_seen, reverse=True)
        return results

    def list_active_peers(self, current_time: float | None = None) -> list[PeerRecord]:
        """Return list of all peers currently in ACTIVE state."""
        return self.list_peers(active_only=True, current_time=current_time)

    def list_stale_peers(self, current_time: float | None = None) -> list[PeerRecord]:
        """Return list of all peers currently in STALE state."""
        return self.list_peers(state=PeerState.STALE, current_time=current_time)

    def find_closest_peers(
        self,
        target: Any,
        count: int = 20,
        *,
        active_only: bool = False,
        exclude: Iterable[str] | None = None,
        current_time: float | None = None,
    ) -> list[PeerRecord]:
        """
        Locate the closest known peers to a target identifier ordered strictly by XOR distance.

        Deduplicates candidates and tie-breaks deterministically by NodeID and peer ID.
        """
        from meshweaver.dht import NodeID

        now = current_time if current_time is not None else time.time()
        candidates = self.list_peers(active_only=active_only, include_expired=False, current_time=now)
        target_node = NodeID.from_peer(target)

        exclude_set: set[str] = set()
        if exclude is not None:
            for ex in exclude:
                if isinstance(ex, str):
                    exclude_set.add(ex)
                elif hasattr(ex, "hex"):
                    exclude_set.add(ex.hex)

        filtered = [p for p in candidates if p.peer_id not in exclude_set]
        filtered.sort(
            key=lambda p: (
                NodeID.from_peer(p).distance(target_node),
                NodeID.from_peer(p).value,
                p.peer_id,
            )
        )
        return filtered[: max(0, count)]

    def list_expired_peers(
        self,
        current_time: float | None = None,
        ttl: float | None = None,
    ) -> list[PeerRecord]:
        """Return list of peers that have exceeded TTL."""
        now = current_time if current_time is not None else time.time()
        effective_ttl = ttl if ttl is not None else self.ttl
        return [
            record
            for record in self._peers.values()
            if record.is_expired(current_time=now, ttl=effective_ttl)
        ]

    def expire_stale_peers(
        self,
        ttl: float | None = None,
        current_time: float | None = None,
    ) -> list[PeerRecord]:
        """
        Purge all peers that have exceeded TTL from the registry.

        Returns:
            list[PeerRecord]: List of expired records that were removed.
        """
        expired = self.list_expired_peers(current_time=current_time, ttl=ttl)
        for record in expired:
            self.remove(record.peer_id)
        if expired:
            logger.info("Expired and removed %d stale peer(s)", len(expired))
        return expired

    def prune(
        self,
        ttl: float | None = None,
        current_time: float | None = None,
    ) -> list[PeerRecord]:
        """Alias for expire_stale_peers."""
        return self.expire_stale_peers(ttl=ttl, current_time=current_time)

    def clear(self) -> None:
        """Remove all peers from the registry."""
        self._peers.clear()
        self._endpoint_index.clear()

    def size(self, active_only: bool = False, current_time: float | None = None) -> int:
        """Return total count of tracked peers, optionally restricted to active."""
        if active_only:
            return len(self.list_active_peers(current_time=current_time))
        return len(self._peers)

    def to_dict(self) -> dict[str, Any]:
        """Export registry state to dictionary."""
        return {
            "stale_threshold": self.stale_threshold,
            "ttl": self.ttl,
            "max_peers": self.max_peers,
            "peers": [p.to_dict() for p in self.list_peers(include_expired=True)],
        }

    def __len__(self) -> int:
        return len(self._peers)

    def __contains__(self, peer_id: str) -> bool:
        return self.has_peer(peer_id)

    def __iter__(self) -> Iterator[PeerRecord]:
        return iter(list(self._peers.values()))

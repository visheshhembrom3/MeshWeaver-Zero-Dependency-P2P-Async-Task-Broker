"""
DHT-inspired peer discovery algorithm, NodeID keyspace, XOR distance metric,
routing buckets, and iterative peer lookup process.

=============================================================================
Kademlia Protocol Alignment and Educational Architecture
=============================================================================

MeshWeaver implements a modular, Kademlia-inspired peer discovery layer.
This module clearly distinguishes a simplified, zero-dependency educational
implementation from a complete production-grade Kademlia protocol (such as
RFC-style implementations, BitTorrent Mainline DHT, or IPFS libp2p DHT).

Educational Simplifications vs Production Kademlia:
--------------------------------------------------
1. Routing Table Representation:
   - Educational: Fixed array of 160 KBuckets indexed directly by the Common
     Prefix Length (CPL) to the local node ID. This provides transparent O(1)
     bucket lookup and eliminates complex dynamic binary tree splitting logic
     while providing identical keyspace partitioning.
   - Production: Dynamic binary prefix tree (trie) of k-buckets that splits
     subtrees as the node discovers peers sharing deeper prefixes with itself.

2. Bucket Replacement Policy:
   - Educational: Contacts in each KBucket are strictly ordered by access/activity
     time (LRU: head is least recently seen, tail is most recently seen). When
     a bucket is full, new contacts are stored in a replacement cache without
     launching active network ping storms.
   - Production: "Ping-before-evict" policy where the least-recently seen node
     is actively queried (PING RPC); if it fails to respond within a timeout,
     it is evicted and the new contact is inserted into the bucket.

3. Distributed Storage RPCs:
   - Educational: Focused on peer discovery and node routing for async task
     distribution (`FIND_NODE` / `NODES_FOUND`). Distributed key-value storage
     (`STORE` and `FIND_VALUE` RPCs) is decoupled from peer membership.
   - Production: Full decentralized storage network where data keys and node
     IDs share the same 160-bit keyspace, with republishing intervals and
     data replication across the k closest nodes.

4. Background Maintenance & Churn:
   - Educational: On-demand iterative lookups with bounded concurrency (alpha)
     and strict per-query and overall timeouts.
   - Production: Autonomous background refresh timers (refreshing buckets
     unvisited in > 1 hour) and Sybil-attack mitigations (cryptographic puzzles
     or IP-diversity restrictions on bucket occupancy).

Implemented Kademlia Mechanisms:
---------------------------------
1. Fixed-Length Identifier Keyspace:
   - 160-bit identifier space (KEY_BITS = 160, 20 bytes), matching standard
     Kademlia with SHA-1 hashing of node identifiers and endpoints.
   - Comprehensive validation, conversion (int, bytes, hex, string hashing), and
     uniform random ID generation.
2. Deterministic Distance Metric:
   - Standard bitwise XOR distance metric: d(x, y) = x ^ y.
   - Satisfies all mathematical metric requirements:
     * Identity: d(x, x) == 0
     * Strict positivity: d(x, y) > 0 for all x != y
     * Symmetry: d(x, y) == d(y, x)
     * Triangle inequality: d(x, z) <= d(x, y) ^ d(y, z)
     * Unidirectional: For any given point and distance Delta > 0, there is exactly
       one point at distance Delta.
3. Routing Buckets (KBucket):
   - Capacity k (default 20), LRU ordering (head = oldest/least-recently seen,
     tail = freshest/most-recently seen).
   - Duplicate updating moves existing contacts to tail; overflow candidates
     tracked in replacement cache.
4. K-Bucket Routing Table (RoutingTable):
   - 160 buckets covering the full keyspace, indexed by Common Prefix Length (cpl).
   - Local node exclusion (a node never stores itself in its own routing table).
   - Multi-bucket closest-peer candidate selection ordered strictly by XOR distance.
5. Closest-Peers Selection:
   - Locates the k closest known peers to any target identifier, sorted strictly
     and deterministically by XOR distance.
6. Iterative Peer Lookup Process:
   - Multi-round iterative discovery converging toward a target key.
   - Concurrency control via alpha parameter (parallel queries per iteration step).
   - Candidate pool management maintaining the closest discovered peers.
7. Duplicate Suppression:
   - Strict tracking of queried, pending, and unresponsive peer identifiers.
   - Automatic suppression of duplicate contacts returned by peers or re-discovered
     during iterative exploration.
8. Bounded Queries and Resource Safeguards:
   - Configurable query bounds (max_queries) and round bounds (max_rounds) to
     prevent runaway query storms or denial-of-service amplification.
9. Resilient Timeout Handling:
   - Per-query timeouts ensuring slow or unresponsive peers do not block progress.
   - Overall lookup timeouts ensuring bounded wall-clock execution time.
10. Protocol Integration:
    - Wire protocol messages (FIND_NODE and NODES_FOUND) with structured schemas
      for distributed peer discovery RPCs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Iterator
from dataclasses import dataclass, field
import hashlib
import logging
import secrets
import time
from typing import Any

from meshweaver.exceptions import (
    InvalidNodeIDError,
    LookupTimeoutError,
    NetworkTimeoutError,
    ValidationError,
)
from meshweaver.peers import (
    DEFAULT_MAX_PEERS,
    PeerEndpoint,
    PeerRecord,
    validate_peer_id,
)

logger = logging.getLogger(__name__)

# Standard Kademlia default constants
KEY_BITS: int = 160
KEY_BYTES: int = KEY_BITS // 8  # 20 bytes
MAX_KEY_INT: int = (1 << KEY_BITS) - 1

DEFAULT_ALPHA: int = 3  # Concurrency factor for iterative lookups
DEFAULT_K: int = 20  # Number of closest peers to return
DEFAULT_LOOKUP_TIMEOUT: float = 5.0  # Overall lookup timeout in seconds
DEFAULT_QUERY_TIMEOUT: float = 2.0  # Per-query timeout in seconds
DEFAULT_MAX_QUERIES: int = 50  # Hard bound on total queries sent
DEFAULT_MAX_ROUNDS: int = 20  # Hard bound on iterative rounds


class NodeID:
    """
    Immutable representation of a fixed-length identifier in the DHT keyspace.

    Default length is 160 bits (20 bytes), compatible with standard Kademlia.
    Supports distance calculation (bitwise XOR), common prefix length matching,
    and conversion from integer, raw bytes, hex string, or arbitrary text strings.
    """

    __slots__ = ("_value", "_bit_length")

    def __init__(self, value: int | bytes | str, bit_length: int = KEY_BITS) -> None:
        if isinstance(bit_length, bool) or not isinstance(bit_length, int) or bit_length <= 0:
            raise InvalidNodeIDError(f"bit_length must be a positive integer, got {bit_length}")

        max_val = (1 << bit_length) - 1

        if isinstance(value, bool):
            raise InvalidNodeIDError("NodeID value cannot be a boolean")
        elif isinstance(value, int):
            if value < 0 or value > max_val:
                raise InvalidNodeIDError(
                    f"NodeID integer value {value} out of range for {bit_length}-bit space [0, {max_val}]"
                )
            self._value = value
        elif isinstance(value, (bytes, bytearray)):
            expected_bytes = (bit_length + 7) // 8
            if len(value) != expected_bytes:
                raise InvalidNodeIDError(
                    f"Expected {expected_bytes} bytes for {bit_length}-bit NodeID, got {len(value)}"
                )
            int_val = int.from_bytes(value, byteorder="big")
            if int_val > max_val:
                raise InvalidNodeIDError(f"Bytes value exceeds maximum {bit_length}-bit integer")
            self._value = int_val
        elif isinstance(value, str):
            clean_str = value.strip().lower()
            if clean_str.startswith("0x"):
                clean_str = clean_str[2:]
            expected_hex_len = (bit_length + 3) // 4
            if len(clean_str) != expected_hex_len:
                raise InvalidNodeIDError(
                    f"Expected {expected_hex_len} hex characters for {bit_length}-bit NodeID, got {len(clean_str)}"
                )
            try:
                int_val = int(clean_str, 16)
            except ValueError as exc:
                raise InvalidNodeIDError(f"Invalid hex string for NodeID: '{value}'") from exc
            if int_val > max_val:
                raise InvalidNodeIDError(f"Hex value exceeds maximum {bit_length}-bit integer")
            self._value = int_val
        else:
            raise InvalidNodeIDError(
                f"NodeID value must be int, bytes, or hex str, got {type(value).__name__}"
            )

        self._bit_length = bit_length

    @property
    def value(self) -> int:
        """Integer representation of the identifier."""
        return self._value

    @property
    def bit_length(self) -> int:
        """Bit length of the keyspace."""
        return self._bit_length

    @property
    def raw_bytes(self) -> bytes:
        """Big-endian bytes representation of the identifier."""
        byte_count = (self._bit_length + 7) // 8
        return self._value.to_bytes(byte_count, byteorder="big")

    @property
    def hex(self) -> str:
        """Hexadecimal string representation of the identifier, zero-padded."""
        hex_len = (self._bit_length + 3) // 4
        return f"{self._value:0{hex_len}x}"

    @classmethod
    def from_int(cls, value: int, bit_length: int = KEY_BITS) -> NodeID:
        """Construct NodeID from integer value."""
        return cls(value, bit_length=bit_length)

    @classmethod
    def from_bytes(cls, data: bytes, bit_length: int = KEY_BITS) -> NodeID:
        """Construct NodeID from raw bytes."""
        return cls(data, bit_length=bit_length)

    @classmethod
    def from_hex(cls, hex_str: str, bit_length: int = KEY_BITS) -> NodeID:
        """Construct NodeID from hex string."""
        return cls(hex_str, bit_length=bit_length)

    @classmethod
    def from_string(cls, text: str, bit_length: int = KEY_BITS) -> NodeID:
        """
        Deterministically hash an arbitrary string (e.g. peer name, UUID, endpoint) into a NodeID.

        Uses SHA-1 for 160-bit keyspace (standard Kademlia), or SHA-256 for 256-bit keyspace.
        """
        if not isinstance(text, str):
            raise InvalidNodeIDError(f"Expected str for hashing, got {type(text).__name__}")
        encoded = text.encode("utf-8")
        if bit_length == 160:
            digest = hashlib.sha1(encoded).digest()
        elif bit_length == 256:
            digest = hashlib.sha256(encoded).digest()
        else:
            # General bit length: hash with sha256 then truncate/pad
            full_digest = hashlib.sha256(encoded).digest()
            byte_count = (bit_length + 7) // 8
            if len(full_digest) >= byte_count:
                digest = full_digest[:byte_count]
            else:
                digest = full_digest.ljust(byte_count, b"\x00")
        return cls.from_bytes(digest, bit_length=bit_length)

    @classmethod
    def generate(cls, bit_length: int = KEY_BITS) -> NodeID:
        """Generate a cryptographically random uniform NodeID."""
        byte_count = (bit_length + 7) // 8
        raw = secrets.token_bytes(byte_count)
        # Mask off excess bits if bit_length is not a multiple of 8
        excess_bits = (byte_count * 8) - bit_length
        if excess_bits > 0:
            int_val = int.from_bytes(raw, byteorder="big") >> excess_bits
            return cls.from_int(int_val, bit_length=bit_length)
        return cls.from_bytes(raw, bit_length=bit_length)

    @classmethod
    def from_peer(cls, peer: Any, bit_length: int = KEY_BITS) -> NodeID:
        """
        Extract or deterministically derive a NodeID from various peer representations:
        NodeID, PeerContact, PeerRecord, hex string, or generic peer string identifier.
        """
        if isinstance(peer, NodeID):
            return peer
        if isinstance(peer, PeerContact):
            return peer.node_id
        if isinstance(peer, PeerRecord):
            # Check metadata for explicit dht_node_id
            explicit_id = peer.metadata.get("dht_node_id") or peer.metadata.get("node_id")
            if explicit_id is not None:
                if isinstance(explicit_id, NodeID):
                    return explicit_id
                if isinstance(explicit_id, str):
                    try:
                        return cls.from_hex(explicit_id, bit_length=bit_length)
                    except InvalidNodeIDError:
                        return cls.from_string(explicit_id, bit_length=bit_length)
            # Default: deterministically hash peer_id
            return cls.from_string(peer.peer_id, bit_length=bit_length)
        if isinstance(peer, str):
            # If 40 hex chars for 160-bit, attempt hex parse first
            expected_hex_len = (bit_length + 3) // 4
            clean = peer.strip().lower()
            if clean.startswith("0x"):
                clean = clean[2:]
            if len(clean) == expected_hex_len and all(c in "0123456789abcdef" for c in clean):
                try:
                    return cls.from_hex(clean, bit_length=bit_length)
                except InvalidNodeIDError:
                    pass
            return cls.from_string(peer, bit_length=bit_length)
        if isinstance(peer, int):
            return cls.from_int(peer, bit_length=bit_length)
        raise InvalidNodeIDError(f"Cannot extract NodeID from object of type {type(peer).__name__}")

    def distance(self, other: NodeID | int) -> int:
        """
        Compute the deterministic bitwise XOR distance to another NodeID or integer.

        Distance properties:
        - d(x, x) == 0
        - d(x, y) == d(y, x) (Symmetric)
        - d(x, y) > 0 for all x != y
        - d(x, z) <= d(x, y) ^ d(y, z) (Triangle inequality)
        """
        if isinstance(other, NodeID):
            if self._bit_length != other._bit_length:
                raise InvalidNodeIDError(
                    f"Cannot calculate distance between NodeIDs with different bit lengths: "
                    f"{self._bit_length} vs {other._bit_length}"
                )
            return self._value ^ other._value
        if isinstance(other, int) and not isinstance(other, bool):
            if other < 0 or other > ((1 << self._bit_length) - 1):
                raise InvalidNodeIDError(f"Integer {other} out of bounds for {self._bit_length}-bit keyspace")
            return self._value ^ other
        raise InvalidNodeIDError(f"Cannot calculate distance to {type(other).__name__}")

    def common_prefix_length(self, other: NodeID) -> int:
        """
        Calculate the number of prefix bits shared between this NodeID and another.
        Used for k-bucket indexing in Kademlia routing tables.
        """
        if not isinstance(other, NodeID):
            raise InvalidNodeIDError(f"Expected NodeID, got {type(other).__name__}")
        if self._bit_length != other._bit_length:
            raise InvalidNodeIDError("Bit lengths must match for common prefix length")

        xor_val = self._value ^ other._value
        if xor_val == 0:
            return self._bit_length
        return self._bit_length - xor_val.bit_length()

    def __xor__(self, other: NodeID | int) -> int:
        return self.distance(other)

    def __int__(self) -> int:
        return self._value

    def __bytes__(self) -> bytes:
        return self.raw_bytes

    def __str__(self) -> str:
        return self.hex

    def __repr__(self) -> str:
        return f"NodeID({self.hex})"

    def __hash__(self) -> int:
        return hash((self._value, self._bit_length))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return self._value == other._value and self._bit_length == other._bit_length
        return False

    def __ne__(self, other: object) -> bool:
        return not (self == other)

    def __lt__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return (self._value, self._bit_length) < (other._value, other._bit_length)
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return (self._value, self._bit_length) <= (other._value, other._bit_length)
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return (self._value, self._bit_length) > (other._value, other._bit_length)
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, NodeID):
            return (self._value, self._bit_length) >= (other._value, other._bit_length)
        return NotImplemented


def xor_distance(
    id1: NodeID | int | str,
    id2: NodeID | int | str,
    bit_length: int = KEY_BITS,
) -> int:
    """
    Calculate deterministic bitwise XOR distance between two identifiers.

    Accepts NodeID instances, integers, or string keys.
    """
    node1 = NodeID.from_peer(id1, bit_length=bit_length)
    node2 = NodeID.from_peer(id2, bit_length=bit_length)
    return node1.distance(node2)


@dataclass(frozen=True, slots=True)
class PeerContact:
    """
    Representation of a contact in the DHT space, pairing a NodeID with network endpoint.
    """

    node_id: NodeID
    endpoint: PeerEndpoint
    peer_id: str
    last_seen: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, NodeID):
            object.__setattr__(self, "node_id", NodeID.from_peer(self.node_id))
        if not isinstance(self.endpoint, PeerEndpoint):
            object.__setattr__(self, "endpoint", PeerEndpoint.from_address(self.endpoint))
        object.__setattr__(self, "peer_id", validate_peer_id(self.peer_id))
        if not isinstance(self.metadata, dict):
            raise ValidationError("Metadata must be a dictionary")

    @property
    def host(self) -> str:
        return self.endpoint.host

    @property
    def port(self) -> int:
        return self.endpoint.port

    def distance_to(self, target: NodeID | int | str) -> int:
        """Compute XOR distance from this contact to a target identifier."""
        target_node = NodeID.from_peer(target, bit_length=self.node_id.bit_length)
        return self.node_id.distance(target_node)

    def to_peer_record(self) -> PeerRecord:
        """Convert this contact to a PeerRecord for registry storage."""
        meta = dict(self.metadata)
        meta["dht_node_id"] = self.node_id.hex
        return PeerRecord(
            peer_id=self.peer_id,
            endpoint=self.endpoint,
            last_seen=self.last_seen if self.last_seen > 0 else time.time(),
            registered_at=self.last_seen if self.last_seen > 0 else time.time(),
            metadata=meta,
        )

    @classmethod
    def from_peer_record(
        cls,
        record: PeerRecord,
        node_id: NodeID | None = None,
        bit_length: int = KEY_BITS,
    ) -> PeerContact:
        """Construct a PeerContact from a PeerRecord."""
        resolved_id = node_id if node_id is not None else NodeID.from_peer(record, bit_length=bit_length)
        return cls(
            node_id=resolved_id,
            endpoint=record.endpoint,
            peer_id=record.peer_id,
            last_seen=record.last_seen,
            metadata=dict(record.metadata),
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert contact to JSON-serializable dictionary."""
        return {
            "node_id": self.node_id.hex,
            "peer_id": self.peer_id,
            "host": self.endpoint.host,
            "port": self.endpoint.port,
            "last_seen": self.last_seen,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any], bit_length: int = KEY_BITS) -> PeerContact:
        """Construct PeerContact from a dictionary."""
        if not isinstance(data, dict):
            raise ValidationError(f"Expected dict for PeerContact, got {type(data).__name__}")
        raw_node_id = data.get("node_id") or data.get("dht_node_id")
        peer_id = data.get("peer_id") or str(raw_node_id or "")
        if not peer_id:
            raise ValidationError("PeerContact requires 'peer_id' or 'node_id'")

        node_id = (
            NodeID.from_peer(raw_node_id, bit_length=bit_length)
            if raw_node_id is not None
            else NodeID.from_string(peer_id, bit_length=bit_length)
        )

        if "endpoint" in data and isinstance(data["endpoint"], dict):
            endpoint = PeerEndpoint.from_dict(data["endpoint"])
        elif "host" in data and "port" in data:
            endpoint = PeerEndpoint(host=data["host"], port=data["port"])
        else:
            raise ValidationError("PeerContact requires 'endpoint' or 'host' and 'port'")

        last_seen = float(data.get("last_seen", time.time()))
        metadata = dict(data.get("metadata", {}))
        return cls(
            node_id=node_id,
            endpoint=endpoint,
            peer_id=peer_id,
            last_seen=last_seen,
            metadata=metadata,
        )


class KBucket:
    """
    A single routing bucket in a Kademlia routing table.

    Holds up to `k` peer contacts sharing a common prefix distance range.
    Contacts are ordered strictly by activity / last-seen time:
    - Head (`self.head` / index 0): least-recently seen contact.
    - Tail (`self.tail` / index -1): most-recently seen contact.

    Educational Architecture & Evolution:
    -------------------------------------
    - Implements standard Kademlia LRU contact ordering.
    - When an existing contact is re-added, it is refreshed and moved to the tail.
    - When a full bucket (len == k) receives a new contact, the new contact is placed
      into the `replacement_cache` (capped at k) rather than triggering an active network
      ping storm.
    - If an active contact is subsequently removed, the oldest replacement candidate
      is automatically promoted to the main bucket.
    """

    __slots__ = ("_k", "_contacts", "_replacement_cache", "_last_updated")

    def __init__(self, k: int = DEFAULT_K) -> None:
        if isinstance(k, bool) or not isinstance(k, int) or k <= 0:
            raise ValidationError(f"Bucket capacity k must be a positive integer, got {k}")
        self._k: int = k
        self._contacts: list[PeerContact] = []
        self._replacement_cache: list[PeerContact] = []
        self._last_updated: float = time.time()

    @property
    def k(self) -> int:
        """Maximum number of contacts this bucket can hold."""
        return self._k

    @property
    def last_updated(self) -> float:
        """Timestamp of the most recent modification to this bucket."""
        return self._last_updated

    @property
    def is_full(self) -> bool:
        """True if the bucket has reached its capacity `k`."""
        return len(self._contacts) >= self._k

    @property
    def head(self) -> PeerContact | None:
        """Return least-recently seen contact (at head), or None if empty."""
        return self._contacts[0] if self._contacts else None

    @property
    def tail(self) -> PeerContact | None:
        """Return most-recently seen contact (at tail), or None if empty."""
        return self._contacts[-1] if self._contacts else None

    @property
    def contacts(self) -> list[PeerContact]:
        """Return a shallow copy of contacts ordered from least to most recently seen."""
        return list(self._contacts)

    @property
    def replacement_cache(self) -> list[PeerContact]:
        """Return a shallow copy of contacts waiting in the replacement cache."""
        return list(self._replacement_cache)

    def add(self, contact: PeerContact | PeerRecord | dict[str, Any]) -> bool:
        """
        Add or update a contact in the bucket.

        - If contact already exists (by NodeID): updates contact details,
          moves it to the tail (most-recently seen), and returns True.
        - If contact is new and bucket is not full (len < k): appends to tail
          and returns True.
        - If contact is new and bucket is full: adds to replacement cache
          (capped at k) and returns False.
        """
        parsed = self._coerce_contact(contact)
        self._last_updated = time.time()

        # Check if already present in main contacts list
        for i, existing in enumerate(self._contacts):
            if existing.node_id == parsed.node_id:
                # Remove from current position and move to tail
                self._contacts.pop(i)
                self._contacts.append(parsed)
                return True

        # New contact: check if bucket has capacity
        if len(self._contacts) < self._k:
            self._contacts.append(parsed)
            # Remove from replacement cache if present
            self._replacement_cache = [c for c in self._replacement_cache if c.node_id != parsed.node_id]
            return True

        # Bucket is full: add to replacement cache
        for i, existing in enumerate(self._replacement_cache):
            if existing.node_id == parsed.node_id:
                self._replacement_cache.pop(i)
                self._replacement_cache.append(parsed)
                return False

        if len(self._replacement_cache) >= self._k:
            self._replacement_cache.pop(0)  # Evict oldest replacement candidate
        self._replacement_cache.append(parsed)
        return False

    def remove(self, node_id: NodeID | str) -> bool:
        """
        Remove a contact from the bucket by NodeID or peer_id string.

        If a contact is removed and the replacement cache is non-empty,
        the oldest replacement candidate is promoted to the bucket.
        Returns True if a contact was removed, False otherwise.
        """
        target_node = self._resolve_node_id(node_id)
        for i, contact in enumerate(self._contacts):
            if (target_node is not None and contact.node_id == target_node) or contact.peer_id == node_id:
                self._contacts.pop(i)
                self._last_updated = time.time()
                # Promote from replacement cache if available
                if self._replacement_cache:
                    promoted = self._replacement_cache.pop(0)
                    self._contacts.append(promoted)
                return True
        return False

    def get(self, node_id: NodeID | str) -> PeerContact | None:
        """Look up a contact in the bucket by NodeID or peer_id."""
        target_node = self._resolve_node_id(node_id)
        for contact in self._contacts:
            if (target_node is not None and contact.node_id == target_node) or contact.peer_id == node_id:
                return contact
        return None

    def _coerce_contact(self, contact: PeerContact | PeerRecord | dict[str, Any]) -> PeerContact:
        if isinstance(contact, PeerContact):
            return contact
        if isinstance(contact, PeerRecord):
            return PeerContact.from_peer_record(contact)
        if isinstance(contact, dict):
            return PeerContact.from_dict(contact)
        raise ValidationError(f"Cannot add object of type {type(contact).__name__} to KBucket")

    def _resolve_node_id(self, node_id: NodeID | str) -> NodeID | None:
        if isinstance(node_id, NodeID):
            return node_id
        if isinstance(node_id, str):
            try:
                return NodeID.from_peer(node_id)
            except Exception:
                return None
        return None

    def __len__(self) -> int:
        return len(self._contacts)

    def __iter__(self) -> Iterator[PeerContact]:
        return iter(list(self._contacts))

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, PeerContact):
            return any(c.node_id == item.node_id for c in self._contacts)
        if isinstance(item, NodeID):
            return any(c.node_id == item for c in self._contacts)
        if isinstance(item, str):
            return any(c.peer_id == item or c.node_id.hex == item for c in self._contacts)
        return False

    def __repr__(self) -> str:
        return f"KBucket(len={len(self._contacts)}/{self._k})"


class RoutingTable:
    """
    Kademlia routing table maintaining k-buckets partitioned by common prefix length.

    Keyspace partitioning:
    - 160 buckets (for 160-bit keyspace).
    - Bucket index i contains contacts sharing exactly i common prefix bits
      with the local node ID: cpl(local_node_id, peer_node_id) == i.
    - Bucket 0: distance [2^159, 2^160 - 1] (half the entire keyspace).
    - Bucket 159: distance 1 (closest possible distinct node).
    - Local node (cpl == 160) is strictly excluded from the routing table.

    Educational Design Notes:
    - Uses a fixed array of 160 KBuckets with O(1) common prefix length indexing,
      avoiding dynamic binary tree trie complexity while providing identical
      keyspace distribution.
    - Full multi-bucket closest-peer search ordered deterministically by XOR distance.
    """

    __slots__ = ("_local_node_id", "_k", "_bit_length", "_buckets")

    def __init__(
        self,
        local_node_id: NodeID | int | str,
        k: int = DEFAULT_K,
        bit_length: int = KEY_BITS,
    ) -> None:
        self._local_node_id: NodeID = NodeID.from_peer(local_node_id, bit_length=bit_length)
        if isinstance(k, bool) or not isinstance(k, int) or k <= 0:
            raise ValidationError(f"Bucket capacity k must be a positive integer, got {k}")
        self._k: int = k
        self._bit_length: int = bit_length
        self._buckets: list[KBucket] = [KBucket(k=k) for _ in range(bit_length)]

    @property
    def local_node_id(self) -> NodeID:
        """The NodeID of the local peer owning this routing table."""
        return self._local_node_id

    @property
    def k(self) -> int:
        """Capacity of each bucket in this routing table."""
        return self._k

    @property
    def bit_length(self) -> int:
        """Bit length of the keyspace."""
        return self._bit_length

    @property
    def buckets(self) -> list[KBucket]:
        """List of all KBuckets in the routing table."""
        return list(self._buckets)

    @property
    def total_contacts(self) -> int:
        """Total number of contacts stored across all buckets."""
        return sum(len(b) for b in self._buckets)

    def bucket_index(self, node_id: NodeID | int | str) -> int:
        """
        Calculate the bucket index for a given identifier.

        Based on common prefix length with the local node ID.
        Raises InvalidNodeIDError if the identifier is identical to the local node ID.
        """
        target = NodeID.from_peer(node_id, bit_length=self._bit_length)
        if target == self._local_node_id:
            raise InvalidNodeIDError("Cannot compute bucket index for the local node ID itself")
        cpl = self._local_node_id.common_prefix_length(target)
        # Cap index to bit_length - 1 (range 0 to bit_length - 1)
        return min(cpl, self._bit_length - 1)

    def get_bucket(self, index: int) -> KBucket:
        """Return the KBucket at the specified index."""
        if index < 0 or index >= len(self._buckets):
            raise IndexError(f"Bucket index {index} out of range [0, {len(self._buckets)-1}]")
        return self._buckets[index]

    def get_bucket_for_node(self, node_id: NodeID | int | str) -> KBucket:
        """Return the KBucket corresponding to the given identifier."""
        idx = self.bucket_index(node_id)
        return self._buckets[idx]

    def add_contact(self, contact: PeerContact | PeerRecord | dict[str, Any]) -> bool:
        """
        Add or update a contact in the appropriate bucket.

        Rejects addition if the contact's NodeID is identical to the local node ID.
        Returns True if contact was added/updated in the bucket, False if bucket was full.
        """
        if isinstance(contact, PeerContact):
            parsed = contact
        elif isinstance(contact, PeerRecord):
            parsed = PeerContact.from_peer_record(contact, bit_length=self._bit_length)
        elif isinstance(contact, dict):
            parsed = PeerContact.from_dict(contact, bit_length=self._bit_length)
        else:
            raise ValidationError(f"Cannot add object of type {type(contact).__name__} to RoutingTable")

        if parsed.node_id == self._local_node_id:
            return False

        idx = self.bucket_index(parsed.node_id)
        return self._buckets[idx].add(parsed)

    def remove_contact(self, node_id: NodeID | str) -> bool:
        """Remove a contact from the appropriate bucket. Returns True if removed."""
        if isinstance(node_id, NodeID):
            if node_id == self._local_node_id:
                return False
            idx = self.bucket_index(node_id)
            return self._buckets[idx].remove(node_id)

        if isinstance(node_id, str):
            clean = node_id.strip().lower()
            if clean.startswith("0x"):
                clean = clean[2:]
            expected_hex_len = (self._bit_length + 3) // 4
            if len(clean) == expected_hex_len and all(c in "0123456789abcdef" for c in clean):
                try:
                    target = NodeID.from_hex(clean, bit_length=self._bit_length)
                    if target != self._local_node_id:
                        idx = self.bucket_index(target)
                        if self._buckets[idx].remove(target):
                            return True
                except Exception:
                    pass

            for b in self._buckets:
                if b.remove(node_id):
                    return True

        return False

    def get_contact(self, node_id: NodeID | str) -> PeerContact | None:
        """Look up a contact across the routing table by NodeID or peer_id."""
        if isinstance(node_id, NodeID):
            if node_id == self._local_node_id:
                return None
            idx = self.bucket_index(node_id)
            return self._buckets[idx].get(node_id)

        if isinstance(node_id, str):
            clean = node_id.strip().lower()
            if clean.startswith("0x"):
                clean = clean[2:]
            expected_hex_len = (self._bit_length + 3) // 4
            if len(clean) == expected_hex_len and all(c in "0123456789abcdef" for c in clean):
                try:
                    target = NodeID.from_hex(clean, bit_length=self._bit_length)
                    if target != self._local_node_id:
                        idx = self.bucket_index(target)
                        found = self._buckets[idx].get(target)
                        if found is not None:
                            return found
                except Exception:
                    pass

            for b in self._buckets:
                found = b.get(node_id)
                if found is not None:
                    return found

        return None

    def all_contacts(self) -> list[PeerContact]:
        """Return a flat list of all contacts stored in the routing table."""
        result: list[PeerContact] = []
        for b in self._buckets:
            result.extend(b.contacts)
        return result

    def find_closest_peers(
        self,
        target: NodeID | int | str,
        count: int = DEFAULT_K,
        *,
        exclude: Iterable[NodeID | str] | None = None,
    ) -> list[PeerContact]:
        """
        Locate up to `count` closest peers to `target` across all buckets,
        sorted strictly and deterministically by XOR distance.
        """
        return find_closest_peers(
            self.all_contacts(),
            target=target,
            count=count,
            exclude=exclude,
            bit_length=self._bit_length,
        )

    def __len__(self) -> int:
        return self.total_contacts

    def __iter__(self) -> Iterator[PeerContact]:
        return iter(self.all_contacts())

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, PeerContact):
            return self.get_contact(item.node_id) is not None
        if isinstance(item, (NodeID, str)):
            return self.get_contact(item) is not None
        return False

    def __repr__(self) -> str:
        return f"RoutingTable(local_node_id={self._local_node_id.hex[:8]}..., contacts={self.total_contacts})"


def find_closest_peers(
    known_peers: Iterable[PeerContact | PeerRecord | Any],
    target: NodeID | int | str,
    count: int = DEFAULT_K,
    *,
    exclude: Iterable[NodeID | str] | None = None,
    bit_length: int = KEY_BITS,
) -> list[PeerContact]:
    """
    Select up to `count` closest peers to `target`, ordered strictly by XOR distance.

    Deduplicates candidates by NodeID (retaining the most recently seen contact)
    and filters out any identifiers specified in `exclude`.
    """
    target_node = NodeID.from_peer(target, bit_length=bit_length)
    exclude_node_ids: set[NodeID] = set()
    exclude_peer_ids: set[str] = set()

    if exclude is not None:
        for ex in exclude:
            if isinstance(ex, NodeID):
                exclude_node_ids.add(ex)
            elif isinstance(ex, str):
                exclude_peer_ids.add(ex)
                try:
                    exclude_node_ids.add(NodeID.from_peer(ex, bit_length=bit_length))
                except Exception:
                    pass

    # Deduplicate contacts by NodeID, keeping the freshest
    contacts_by_id: dict[NodeID, PeerContact] = {}
    for p in known_peers:
        if isinstance(p, PeerContact):
            contact = p
        elif isinstance(p, PeerRecord):
            contact = PeerContact.from_peer_record(p, bit_length=bit_length)
        elif isinstance(p, dict):
            contact = PeerContact.from_dict(p, bit_length=bit_length)
        else:
            continue

        if contact.node_id in exclude_node_ids or contact.peer_id in exclude_peer_ids:
            continue

        existing = contacts_by_id.get(contact.node_id)
        if existing is None or contact.last_seen > existing.last_seen:
            contacts_by_id[contact.node_id] = contact

    # Deterministic sort: distance first, tie-break by node_id value, then peer_id
    sorted_contacts = sorted(
        contacts_by_id.values(),
        key=lambda c: (c.node_id.distance(target_node), c.node_id.value, c.peer_id),
    )

    return sorted_contacts[: max(0, count)]


@dataclass(frozen=True, slots=True)
class LookupResult:
    """
    Outcome and diagnostic telemetry from an iterative DHT peer lookup.
    """

    target: NodeID
    closest_peers: list[PeerContact]
    queried_peers: tuple[NodeID, ...]
    unresponsive_peers: tuple[NodeID, ...]
    all_discovered: tuple[PeerContact, ...]
    queries_sent: int
    rounds: int
    timed_out: bool
    converged: bool

    def __iter__(self) -> Any:
        return iter(self.closest_peers)

    def __len__(self) -> int:
        return len(self.closest_peers)

    def __getitem__(self, idx: int) -> PeerContact:
        return self.closest_peers[idx]

    def __contains__(self, item: Any) -> bool:
        if isinstance(item, PeerContact):
            return item in self.closest_peers
        if isinstance(item, NodeID):
            return any(c.node_id == item for c in self.closest_peers)
        if isinstance(item, str):
            return any(c.peer_id == item or c.node_id.hex == item for c in self.closest_peers)
        return False


# Type signature for the asynchronous peer query function:
# Given a contact and target NodeID, asynchronously returns candidate contacts known to that contact.
PeerQueryFn = Callable[[PeerContact, NodeID], Awaitable[Iterable[PeerContact | PeerRecord | dict[str, Any]]]]


class IterativeLookup:
    """
    Iterative DHT peer lookup coordinator.

    Executes bounded, concurrent searches across network peers for the nodes
    closest to a specified target identifier.

    Guarantees:
    - Duplicate suppression: Nodes are queried at most once per lookup.
    - Bounded queries: Strict limits on total queries (max_queries) and rounds (max_rounds).
    - Concurrency control: At most alpha queries executed concurrently per round.
    - Timeout resilience: Individual query timeouts and overall lookup timeouts.
    - Fault isolation: Failures or timeouts on individual peers do not halt the lookup.
    """

    def __init__(
        self,
        target: NodeID | int | str,
        initial_peers: Iterable[PeerContact | PeerRecord | dict[str, Any]],
        query_fn: PeerQueryFn,
        *,
        alpha: int = DEFAULT_ALPHA,
        k: int = DEFAULT_K,
        query_timeout: float = DEFAULT_QUERY_TIMEOUT,
        overall_timeout: float = DEFAULT_LOOKUP_TIMEOUT,
        max_queries: int = DEFAULT_MAX_QUERIES,
        max_rounds: int = DEFAULT_MAX_ROUNDS,
        local_node_id: NodeID | str | None = None,
        bit_length: int = KEY_BITS,
    ) -> None:
        self.bit_length: int = bit_length
        self.target: NodeID = NodeID.from_peer(target, bit_length=bit_length)
        self.query_fn: PeerQueryFn = query_fn
        self.alpha: int = max(1, int(alpha))
        self.k: int = max(1, int(k))
        self.query_timeout: float = max(0.01, float(query_timeout))
        self.overall_timeout: float = max(0.01, float(overall_timeout))
        self.max_queries: int = max(1, int(max_queries))
        self.max_rounds: int = max(1, int(max_rounds))

        self.local_node_id: NodeID | None = (
            NodeID.from_peer(local_node_id, bit_length=bit_length)
            if local_node_id is not None
            else None
        )

        # Internal state tracking
        self._candidates: dict[NodeID, PeerContact] = {}
        self._queried: set[NodeID] = set()
        self._unresponsive: set[NodeID] = set()
        self._queries_sent: int = 0
        self._rounds: int = 0

        # Seed candidate pool from initial peers
        for p in initial_peers:
            self._add_candidate(p)

    def _add_candidate(self, peer: PeerContact | PeerRecord | dict[str, Any]) -> PeerContact | None:
        """Add or update candidate contact with duplicate suppression and local node filtering."""
        try:
            if isinstance(peer, PeerContact):
                contact = peer
            elif isinstance(peer, PeerRecord):
                contact = PeerContact.from_peer_record(peer, bit_length=self.bit_length)
            elif isinstance(peer, dict):
                contact = PeerContact.from_dict(peer, bit_length=self.bit_length)
            else:
                return None
        except Exception as exc:
            logger.debug("Skipping malformed peer candidate: %s (%s)", peer, exc)
            return None

        # Exclude self
        if self.local_node_id is not None and contact.node_id == self.local_node_id:
            return None

        # Duplicate suppression: maintain freshest record
        existing = self._candidates.get(contact.node_id)
        if existing is None or contact.last_seen > existing.last_seen:
            self._candidates[contact.node_id] = contact
        return contact

    async def execute(self) -> LookupResult:
        """
        Execute the iterative lookup algorithm within the configured overall timeout.
        """
        try:
            return await asyncio.wait_for(self._run_loop(), timeout=self.overall_timeout)
        except TimeoutError:
            logger.info(
                "DHT iterative lookup for target %s exceeded overall timeout of %.2fs",
                self.target.hex,
                self.overall_timeout,
            )
            return self._build_result(timed_out=True, converged=False)

    async def _run_loop(self) -> LookupResult:
        """Core iterative round loop."""
        converged = False

        while self._rounds < self.max_rounds and self._queries_sent < self.max_queries:
            # Sort candidates by distance to target
            sorted_candidates = sorted(
                self._candidates.values(),
                key=lambda c: (c.node_id.distance(self.target), c.node_id.value, c.peer_id),
            )

            # Pick closest unqueried candidates
            unqueried = [
                c
                for c in sorted_candidates
                if c.node_id not in self._queried and c.node_id not in self._unresponsive
            ]

            if not unqueried:
                # No remaining peers to query
                converged = True
                break

            # Convergence condition: If all k closest candidates have been queried
            closest_k = sorted_candidates[: self.k]
            if all(
                c.node_id in self._queried or c.node_id in self._unresponsive for c in closest_k
            ):
                # The k closest known peers have all responded or failed; lookup has converged
                converged = True
                break

            # Bound the number of queries to alpha and remaining query budget
            batch_size = min(self.alpha, len(unqueried), self.max_queries - self._queries_sent)
            if batch_size <= 0:
                break

            batch = unqueried[:batch_size]
            self._rounds += 1
            self._queries_sent += len(batch)

            # Query the batch concurrently
            tasks = [asyncio.create_task(self._query_peer(contact)) for contact in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for contact, res in zip(batch, results):
                if isinstance(res, Exception):
                    logger.debug(
                        "Peer %s query failed: %s",
                        contact.peer_id,
                        res,
                    )
                    self._unresponsive.add(contact.node_id)
                elif res is None:
                    self._unresponsive.add(contact.node_id)
                else:
                    self._queried.add(contact.node_id)
                    # Add discovered peers to candidate pool
                    for discovered in res:
                        self._add_candidate(discovered)

        # Check final convergence status
        if not converged and self._rounds < self.max_rounds and self._queries_sent < self.max_queries:
            converged = True

        return self._build_result(timed_out=False, converged=converged)

    async def _query_peer(
        self,
        contact: PeerContact,
    ) -> Iterable[PeerContact | PeerRecord | dict[str, Any]] | None:
        """Execute a single peer query protected by the per-query timeout."""
        try:
            return await asyncio.wait_for(
                self.query_fn(contact, self.target),
                timeout=self.query_timeout,
            )
        except TimeoutError:
            logger.debug("Query to peer %s timed out after %.2fs", contact.peer_id, self.query_timeout)
            return None
        except Exception as exc:
            logger.debug("Query to peer %s raised error: %s", contact.peer_id, exc)
            return None

    def _build_result(self, timed_out: bool, converged: bool) -> LookupResult:
        """Construct the immutable LookupResult from current state."""
        # Filter out unresponsive peers from the final closest list if possible,
        # but keep candidates sorted strictly by distance
        active_candidates = [
            c for c in self._candidates.values() if c.node_id not in self._unresponsive
        ]
        # If no active candidates, fallback to all known candidates
        candidates_to_sort = active_candidates if active_candidates else list(self._candidates.values())

        sorted_closest = sorted(
            candidates_to_sort,
            key=lambda c: (c.node_id.distance(self.target), c.node_id.value, c.peer_id),
        )[: self.k]

        return LookupResult(
            target=self.target,
            closest_peers=sorted_closest,
            queried_peers=tuple(self._queried),
            unresponsive_peers=tuple(self._unresponsive),
            all_discovered=tuple(self._candidates.values()),
            queries_sent=self._queries_sent,
            rounds=self._rounds,
            timed_out=timed_out,
            converged=converged,
        )


async def iterative_peer_lookup(
    target: NodeID | int | str,
    initial_peers: Iterable[PeerContact | PeerRecord | dict[str, Any]],
    query_fn: PeerQueryFn,
    *,
    alpha: int = DEFAULT_ALPHA,
    k: int = DEFAULT_K,
    query_timeout: float = DEFAULT_QUERY_TIMEOUT,
    overall_timeout: float = DEFAULT_LOOKUP_TIMEOUT,
    max_queries: int = DEFAULT_MAX_QUERIES,
    max_rounds: int = DEFAULT_MAX_ROUNDS,
    local_node_id: NodeID | str | None = None,
    bit_length: int = KEY_BITS,
) -> LookupResult:
    """
    Convenience function to perform an iterative DHT peer lookup.
    """
    lookup = IterativeLookup(
        target=target,
        initial_peers=initial_peers,
        query_fn=query_fn,
        alpha=alpha,
        k=k,
        query_timeout=query_timeout,
        overall_timeout=overall_timeout,
        max_queries=max_queries,
        max_rounds=max_rounds,
        local_node_id=local_node_id,
        bit_length=bit_length,
    )
    return await lookup.execute()

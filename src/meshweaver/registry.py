"""
Compatibility re-export of PeerRegistry and peer models from meshweaver.peers.
"""

from meshweaver.peers import (
    DEFAULT_MAX_PEERS,
    DEFAULT_STALE_THRESHOLD,
    DEFAULT_TTL,
    PeerEndpoint,
    PeerRecord,
    PeerRegistry,
    PeerState,
    validate_host,
    validate_peer_id,
    validate_port,
)

__all__: list[str] = [
    "DEFAULT_MAX_PEERS",
    "DEFAULT_STALE_THRESHOLD",
    "DEFAULT_TTL",
    "PeerEndpoint",
    "PeerRecord",
    "PeerRegistry",
    "PeerState",
    "validate_host",
    "validate_peer_id",
    "validate_port",
]

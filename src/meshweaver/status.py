"""
Node status and peer registry inspection API, health monitoring, and structured logging.

Integrates peer discovery (DHT routing table), dynamic peer registry (membership and state),
gossip resource monitoring (CPU, RAM, health), and connection states into a clean,
observable node-status representation.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
import json
import logging
import time
from typing import Any, TYPE_CHECKING

from meshweaver.gossip import HealthStatus, NodeResourceReport, collect_node_resources
from meshweaver.peers import PeerEndpoint, PeerRecord, PeerRegistry, PeerState

if TYPE_CHECKING:
    from meshweaver.network import PeerNode

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PeerStatus:
    """
    Unified immutable status snapshot for a single peer node in the mesh.
    Combines registry membership, network connection, DHT proximity, and gossip metrics.
    """

    peer_id: str
    endpoint: PeerEndpoint
    state: PeerState
    last_seen: float
    last_seen_seconds_ago: float
    registered_at: float
    is_connected: bool = False
    is_inbound: bool | None = None
    dht_distance: int | None = None
    dht_bucket_index: int | None = None
    cpu_percent: float | None = None
    memory_percent: float | None = None
    active_task_count: int = 0
    health_status: HealthStatus = HealthStatus.UNKNOWN
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def host(self) -> str:
        """Remote host IP or hostname."""
        return self.endpoint.host

    @property
    def port(self) -> int:
        """Remote TCP port."""
        return self.endpoint.port

    @property
    def is_active(self) -> bool:
        """True if peer is strictly in ACTIVE state."""
        return self.state == PeerState.ACTIVE

    @property
    def is_stale(self) -> bool:
        """True if peer has exceeded stale threshold."""
        return self.state == PeerState.STALE

    @property
    def is_expired(self) -> bool:
        """True if peer has exceeded TTL and is eligible for eviction."""
        return self.state == PeerState.EXPIRED

    @property
    def is_healthy(self) -> bool:
        """True if reported operational health is strictly HEALTHY."""
        return self.health_status.is_healthy()

    @property
    def is_usable(self) -> bool:
        """True if peer is able to accept workload."""
        return self.is_active and self.health_status.is_usable()

    def to_dict(self) -> dict[str, Any]:
        """Convert peer status to a JSON-serializable dictionary."""
        return {
            "peer_id": self.peer_id,
            "endpoint": self.endpoint.to_dict(),
            "state": self.state.value,
            "last_seen": round(self.last_seen, 4),
            "last_seen_seconds_ago": round(self.last_seen_seconds_ago, 2),
            "registered_at": round(self.registered_at, 4),
            "is_connected": self.is_connected,
            "is_inbound": self.is_inbound,
            "dht_distance": self.dht_distance,
            "dht_bucket_index": self.dht_bucket_index,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "active_task_count": self.active_task_count,
            "health_status": self.health_status.value,
            "metadata": self.metadata,
        }

    def summary(self) -> str:
        """Single-line human-readable summary of peer status."""
        cpu_str = f"{self.cpu_percent:.1f}%" if self.cpu_percent is not None else "N/A"
        mem_str = f"{self.memory_percent:.1f}%" if self.memory_percent is not None else "N/A"
        dist_str = f"d={self.dht_distance}" if self.dht_distance is not None else "d=N/A"
        conn_str = "conn" if self.is_connected else "disc"
        return (
            f"[{self.state.value.upper()}] {self.peer_id} @ {self.endpoint} "
            f"({conn_str}) | Health: {self.health_status.value} | "
            f"CPU: {cpu_str} | Mem: {mem_str} | Tasks: {self.active_task_count} | "
            f"Seen: {self.last_seen_seconds_ago:.1f}s ago | {dist_str}"
        )


# Alias for naming consistency
PeerStatusSnapshot = PeerStatus


@dataclass(frozen=True, slots=True)
class NodeStatusSnapshot:
    """
    Comprehensive, immutable snapshot of a local node's internal state,
    resource metrics, DHT table, and registered mesh peers.
    """

    node_id: str
    host: str
    port: int
    bound_port: int
    dht_id_hex: str
    is_running: bool
    health_status: HealthStatus
    cpu_percent: float | None
    memory_percent: float | None
    active_task_count: int
    active_connections_count: int
    total_registered_peers: int
    active_peers_count: int
    stale_peers_count: int
    expired_peers_count: int
    routing_table_contacts: int
    gossip_reports_count: int
    peers: list[PeerStatus] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def get_peer(self, peer_id: str) -> PeerStatus | None:
        """Retrieve a specific peer status snapshot by ID."""
        for p in self.peers:
            if p.peer_id == peer_id:
                return p
        return None

    def list_peers(
        self,
        *,
        state: PeerState | str | None = None,
        health: HealthStatus | str | None = None,
        connected_only: bool = False,
    ) -> list[PeerStatus]:
        """
        Filter peers by lifecycle state, health status, or connection flag.
        """
        filtered = self.peers
        if state is not None:
            target_state = PeerState(state)
            filtered = [p for p in filtered if p.state == target_state]
        if health is not None:
            target_health = HealthStatus(health)
            filtered = [p for p in filtered if p.health_status == target_health]
        if connected_only:
            filtered = [p for p in filtered if p.is_connected]
        return filtered

    def to_dict(self) -> dict[str, Any]:
        """Serialize complete node status snapshot to a dictionary."""
        return {
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "bound_port": self.bound_port,
            "dht_id_hex": self.dht_id_hex,
            "is_running": self.is_running,
            "health_status": self.health_status.value,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "active_task_count": self.active_task_count,
            "active_connections_count": self.active_connections_count,
            "total_registered_peers": self.total_registered_peers,
            "active_peers_count": self.active_peers_count,
            "stale_peers_count": self.stale_peers_count,
            "expired_peers_count": self.expired_peers_count,
            "routing_table_contacts": self.routing_table_contacts,
            "gossip_reports_count": self.gossip_reports_count,
            "timestamp": round(self.timestamp, 4),
            "metadata": self.metadata,
            "peers": [p.to_dict() for p in self.peers],
        }

    def to_json(self, indent: int = 2) -> str:
        """Serialize status snapshot to formatted JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def format_text(self) -> str:
        """Generate human-readable terminal status dashboard."""
        return format_node_status(self)


# Alias for naming consistency
NodeStatusInfo = NodeStatusSnapshot


class NodeStatusAPI:
    """
    Internal node status and registry inspection API.

    Exposes:
    - Registered peers with state transitions (ACTIVE, STALE, EXPIRED).
    - Last-seen timestamps and relative age.
    - Reported CPU/memory resource metrics and operational health.
    - Active network connections and DHT routing table proximity.
    - Structured logging and JSON export.
    """

    def __init__(self, node: PeerNode) -> None:
        self._node = node

    @property
    def node(self) -> PeerNode:
        """The underlying PeerNode."""
        return self._node

    def get_node_status(self, *, current_time: float | None = None) -> NodeStatusSnapshot:
        """
        Construct a comprehensive status snapshot of the node and all tracked peers.
        """
        now = current_time if current_time is not None else time.time()
        node = self._node

        # 1. Local resources and health
        local_cpu: float | None = None
        local_mem: float | None = None
        local_tasks: int = 0
        local_health: HealthStatus = HealthStatus.HEALTHY

        if node.gossip_engine is not None:
            local_report = node.gossip_engine.collect_local_status(current_time=now)
            local_cpu = local_report.cpu_percent
            local_mem = local_report.memory_percent
            local_tasks = local_report.active_task_count
            local_health = local_report.health_status
        else:
            local_report = collect_node_resources(node.peer_id, current_time=now)
            local_cpu = local_report.cpu_percent
            local_mem = local_report.memory_percent
            local_tasks = local_report.active_task_count
            local_health = local_report.health_status

        # 2. Peer snapshots
        peers: list[PeerStatus] = []
        active_count = 0
        stale_count = 0
        expired_count = 0

        # Collect from peer_registry
        for record in node.peer_registry._peers.values():
            peer_id = record.peer_id
            endpoint = record.endpoint
            state = record.state(
                current_time=now,
                stale_threshold=node.peer_registry.stale_threshold,
                ttl=node.peer_registry.ttl,
            )
            last_seen = record.last_seen
            last_seen_ago = max(0.0, now - last_seen)

            if state == PeerState.ACTIVE:
                active_count += 1
            elif state == PeerState.STALE:
                stale_count += 1
            elif state == PeerState.EXPIRED:
                expired_count += 1

            # Connection status
            conn = node.connected_peers.get(peer_id)
            is_connected = conn is not None and not conn.is_closed
            is_inbound = conn.is_inbound if conn is not None else None

            # DHT metric
            dht_dist: int | None = None
            dht_bucket: int | None = None
            try:
                if record.dht_id != node.node_id:
                    dht_dist = int(node.node_id.distance(record.dht_id))
                    dht_bucket = node.routing_table.bucket_index(record.dht_id)
            except Exception:
                pass

            # Gossip resource metrics
            peer_cpu: float | None = None
            peer_mem: float | None = None
            peer_tasks: int = 0
            peer_health: HealthStatus = HealthStatus.UNKNOWN

            if node.gossip_engine is not None:
                g_rep = node.gossip_engine.store.get(
                    peer_id, current_time=now, include_expired=True
                )
                if g_rep is not None:
                    peer_cpu = g_rep.cpu_percent
                    peer_mem = g_rep.memory_percent
                    peer_tasks = g_rep.active_task_count
                    peer_health = g_rep.health_status

            # Fallback to metadata
            if peer_cpu is None and "cpu_percent" in record.metadata:
                try:
                    peer_cpu = float(record.metadata["cpu_percent"])
                except (ValueError, TypeError):
                    pass

            if peer_mem is None and "memory_percent" in record.metadata:
                try:
                    peer_mem = float(record.metadata["memory_percent"])
                except (ValueError, TypeError):
                    pass

            if peer_health == HealthStatus.UNKNOWN and "health_status" in record.metadata:
                try:
                    peer_health = HealthStatus(record.metadata["health_status"])
                except ValueError:
                    pass

            # If still UNKNOWN, derive from state
            if peer_health == HealthStatus.UNKNOWN:
                if state == PeerState.ACTIVE:
                    peer_health = HealthStatus.HEALTHY
                elif state == PeerState.STALE:
                    peer_health = HealthStatus.DEGRADED
                elif state == PeerState.EXPIRED:
                    peer_health = HealthStatus.UNHEALTHY

            merged_meta = dict(record.metadata)
            peers.append(
                PeerStatus(
                    peer_id=peer_id,
                    endpoint=endpoint,
                    state=state,
                    last_seen=last_seen,
                    last_seen_seconds_ago=last_seen_ago,
                    registered_at=record.registered_at,
                    is_connected=is_connected,
                    is_inbound=is_inbound,
                    dht_distance=dht_dist,
                    dht_bucket_index=dht_bucket,
                    cpu_percent=peer_cpu,
                    memory_percent=peer_mem,
                    active_task_count=peer_tasks,
                    health_status=peer_health,
                    metadata=merged_meta,
                )
            )

        # Sort peers: active first, then last_seen descending
        peers.sort(
            key=lambda p: (
                0 if p.state == PeerState.ACTIVE else (1 if p.state == PeerState.STALE else 2),
                -p.last_seen,
            )
        )

        dht_contacts = node.routing_table.total_contacts
        gossip_reports_count = (
            len(node.gossip_engine.store.list_reports(include_stale=True, include_expired=True, current_time=now))
            if node.gossip_engine is not None
            else 0
        )

        return NodeStatusSnapshot(
            node_id=node.peer_id,
            host=node.host,
            port=node.port,
            bound_port=node.bound_port,
            dht_id_hex=node.node_id.hex,
            is_running=node.is_running,
            health_status=local_health,
            cpu_percent=local_cpu,
            memory_percent=local_mem,
            active_task_count=local_tasks,
            active_connections_count=len(node.active_connections),
            total_registered_peers=len(peers),
            active_peers_count=active_count,
            stale_peers_count=stale_count,
            expired_peers_count=expired_count,
            routing_table_contacts=dht_contacts,
            gossip_reports_count=gossip_reports_count,
            peers=peers,
            timestamp=now,
        )

    def get_peer_status(self, peer_id: str, *, current_time: float | None = None) -> PeerStatus | None:
        """Retrieve status for a single peer by peer_id."""
        snapshot = self.get_node_status(current_time=current_time)
        return snapshot.get_peer(peer_id)

    def list_peers(
        self,
        *,
        state: PeerState | str | None = None,
        health: HealthStatus | str | None = None,
        connected_only: bool = False,
        current_time: float | None = None,
    ) -> list[PeerStatus]:
        """List and filter peer statuses."""
        snapshot = self.get_node_status(current_time=current_time)
        return snapshot.list_peers(state=state, health=health, connected_only=connected_only)

    def to_dict(self, *, current_time: float | None = None) -> dict[str, Any]:
        """Return full status as dictionary."""
        return self.get_node_status(current_time=current_time).to_dict()

    def to_json(self, indent: int = 2, *, current_time: float | None = None) -> str:
        """Return full status as JSON string."""
        return self.get_node_status(current_time=current_time).to_json(indent=indent)

    def format_summary(self, *, current_time: float | None = None) -> str:
        """Return formatted human-readable summary table."""
        return self.get_node_status(current_time=current_time).format_text()

    def log_status(
        self,
        log_instance: logging.Logger | None = None,
        level: int = logging.INFO,
        *,
        current_time: float | None = None,
        as_json: bool = False,
    ) -> dict[str, Any]:
        """
        Emit a structured log message summarizing node status and peer counts.
        """
        snapshot = self.get_node_status(current_time=current_time)
        return log_node_status(snapshot, logger=log_instance, level=level, as_json=as_json)


def get_node_status(node: PeerNode, *, current_time: float | None = None) -> NodeStatusSnapshot:
    """Convenience function to obtain a NodeStatusSnapshot from a PeerNode."""
    return NodeStatusAPI(node).get_node_status(current_time=current_time)


def format_peer_status_table(peers: list[PeerStatus]) -> str:
    """Format a list of PeerStatus records into a clean ASCII table."""
    if not peers:
        return "  (No peers registered in local mesh registry)\n"

    headers = [
        "Peer ID",
        "Endpoint",
        "State",
        "Health",
        "CPU",
        "Memory",
        "Tasks",
        "Last Seen",
        "Conn",
    ]
    rows: list[list[str]] = []
    for p in peers:
        cpu_str = f"{p.cpu_percent:.1f}%" if p.cpu_percent is not None else "N/A"
        mem_str = f"{p.memory_percent:.1f}%" if p.memory_percent is not None else "N/A"
        seen_str = f"{p.last_seen_seconds_ago:.1f}s ago"
        conn_str = "yes" if p.is_connected else "no"
        rows.append(
            [
                p.peer_id,
                str(p.endpoint),
                p.state.value,
                p.health_status.value,
                cpu_str,
                mem_str,
                str(p.active_task_count),
                seen_str,
                conn_str,
            ]
        )

    # Compute column widths
    widths = [len(h) for h in headers]
    for row in rows:
        for idx, col in enumerate(row):
            widths[idx] = max(widths[idx], len(col))

    def make_line(items: list[str]) -> str:
        return "  " + "  ".join(item.ljust(widths[i]) for i, item in enumerate(items))

    sep = "  " + "  ".join("-" * w for w in widths)
    lines = [make_line(headers), sep]
    for row in rows:
        lines.append(make_line(row))
    return "\n".join(lines) + "\n"


def format_node_status(snapshot: NodeStatusSnapshot) -> str:
    """Format a NodeStatusSnapshot into a readable multi-section dashboard string."""
    cpu_str = f"{snapshot.cpu_percent:.1f}%" if snapshot.cpu_percent is not None else "N/A"
    mem_str = f"{snapshot.memory_percent:.1f}%" if snapshot.memory_percent is not None else "N/A"
    run_state = "RUNNING" if snapshot.is_running else "STOPPED"

    lines = [
        "=" * 80,
        " MeshWeaver Node & Peer Status",
        "=" * 80,
        f" Node ID:           {snapshot.node_id}",
        f" DHT Key (160-bit): {snapshot.dht_id_hex[:16]}...{snapshot.dht_id_hex[-8:]}",
        f" Listening Address: {snapshot.host}:{snapshot.bound_port} [{run_state}]",
        f" Node Health:       {snapshot.health_status.value.upper()}",
        f" Local Resources:   CPU {cpu_str} | Memory {mem_str} | Active Tasks: {snapshot.active_task_count}",
        f" DHT Contacts:      {snapshot.routing_table_contacts} contacts",
        f" Gossip Reports:    {snapshot.gossip_reports_count} active reports",
        f" Mesh Peer Summary: {snapshot.active_peers_count} active | {snapshot.stale_peers_count} stale | "
        f"{snapshot.expired_peers_count} expired | {snapshot.total_registered_peers} total",
        "-" * 80,
        f" Registered Mesh Peers ({snapshot.total_registered_peers}):",
        "-" * 80,
        format_peer_status_table(snapshot.peers),
        "=" * 80,
    ]
    return "\n".join(lines)


def log_node_status(
    snapshot: NodeStatusSnapshot,
    logger: logging.Logger | None = None,
    level: int = logging.INFO,
    *,
    as_json: bool = False,
) -> dict[str, Any]:
    """
    Emit structured log information for a node status snapshot.
    """
    target_logger = logger if logger is not None else logging.getLogger("meshweaver.status")
    data = snapshot.to_dict()

    if as_json:
        target_logger.log(level, "%s", json.dumps(data))
    else:
        target_logger.log(
            level,
            "Node status: node_id=%s address=%s:%d health=%s state=%s "
            "active_peers=%d stale_peers=%d expired_peers=%d total_peers=%d "
            "cpu=%s mem=%s active_tasks=%d",
            snapshot.node_id,
            snapshot.host,
            snapshot.bound_port,
            snapshot.health_status.value,
            "running" if snapshot.is_running else "stopped",
            snapshot.active_peers_count,
            snapshot.stale_peers_count,
            snapshot.expired_peers_count,
            snapshot.total_registered_peers,
            f"{snapshot.cpu_percent:.1f}%" if snapshot.cpu_percent is not None else "N/A",
            f"{snapshot.memory_percent:.1f}%" if snapshot.memory_percent is not None else "N/A",
            snapshot.active_task_count,
            extra={"node_status": data},
        )
    return data


def setup_structured_logging(
    verbose: bool = False,
    as_json: bool = False,
    stream: Any | None = None,
) -> None:
    """
    Configure structured root logging for MeshWeaver processes.
    Directs log messages to sys.stderr by default so stdout remains clean for piping.
    """
    import sys

    log_level = logging.DEBUG if verbose else logging.INFO
    target_stream = stream if stream is not None else sys.stderr
    if as_json:
        log_format = "%(message)s"
    else:
        log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt="%H:%M:%S",
        stream=target_stream,
        force=True,
    )

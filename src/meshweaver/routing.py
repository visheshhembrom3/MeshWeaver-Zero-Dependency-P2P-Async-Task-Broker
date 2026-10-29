"""
Load-aware task routing, candidate ranking, and worker peer selection.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from typing import TYPE_CHECKING, Any, Iterable

from meshweaver.exceptions import (
    IncompatibleWorkerError,
    NoAvailablePeerError,
    NoEligibleWorkerError,
    StaleResourceReportError,
    TaskRoutingError,
    TaskSchedulingError,
)
from meshweaver.gossip import HealthStatus, NodeResourceReport
from meshweaver.peers import PeerRecord, PeerState
from meshweaver.tasks import TaskRequest

if TYPE_CHECKING:
    from meshweaver.network import PeerNode

logger = logging.getLogger(__name__)

# Default weighting coefficients for load-aware peer scoring
DEFAULT_ACTIVE_TASK_WEIGHT: float = 10.0
DEFAULT_CPU_WEIGHT: float = 0.5
DEFAULT_MEMORY_WEIGHT: float = 0.5
DEFAULT_DEGRADED_PENALTY: float = 50.0
DEFAULT_UNHEALTHY_PENALTY: float = 1000.0


@dataclass(frozen=True, slots=True)
class RouteDecision:
    """
    Structured outcome of a task routing decision.
    """

    selected_peer: PeerRecord
    score: float
    active_tasks: int
    cpu_percent: float
    memory_percent: float
    health_status: HealthStatus
    candidates_count: int
    reason: str = "Lowest composite load score"

    def to_dict(self) -> dict[str, Any]:
        """Convert routing decision to a dictionary."""
        return {
            "peer_id": self.selected_peer.peer_id,
            "host": self.selected_peer.host,
            "port": self.selected_peer.port,
            "score": round(self.score, 2),
            "active_tasks": self.active_tasks,
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_percent": round(self.memory_percent, 2),
            "health_status": self.health_status.value,
            "candidates_count": self.candidates_count,
            "reason": self.reason,
        }


class LoadAwareRoutingStrategy:
    """
    Load-aware routing strategy selecting the peer with the lowest composite load score.

    Evaluates:
    1. Active task count (direct queue pressure)
    2. CPU percent utilization
    3. Memory percent utilization
    4. Peer operational health (HEALTHY vs DEGRADED vs UNHEALTHY)
    5. Peer lifecycle state (must be ALIVE/ACTIVE in peer registry)

    Tie-breaking:
    When multiple peers have identical composite scores, uses deterministic XOR distance
    between the task ID (hashed to a 160-bit NodeID) and candidate NodeID to distribute
    tasks evenly across peers without hotspotting.
    """

    def __init__(
        self,
        *,
        active_task_weight: float = DEFAULT_ACTIVE_TASK_WEIGHT,
        cpu_weight: float = DEFAULT_CPU_WEIGHT,
        memory_weight: float = DEFAULT_MEMORY_WEIGHT,
        degraded_penalty: float = DEFAULT_DEGRADED_PENALTY,
        unhealthy_penalty: float = DEFAULT_UNHEALTHY_PENALTY,
    ) -> None:
        self.active_task_weight = active_task_weight
        self.cpu_weight = cpu_weight
        self.memory_weight = memory_weight
        self.degraded_penalty = degraded_penalty
        self.unhealthy_penalty = unhealthy_penalty

    def calculate_peer_load(
        self,
        peer: PeerRecord,
        peer_node: Any,
    ) -> tuple[float, int, float, float, HealthStatus]:
        """
        Calculate composite load score and extracted metrics for a peer.

        Returns:
            tuple[score, active_tasks, cpu_percent, memory_percent, health_status]
        """
        active_tasks = 0
        cpu_percent = 0.0
        memory_percent = 0.0
        health_status = HealthStatus.HEALTHY

        # 1. Inspect gossip store first if gossip engine is running
        report: NodeResourceReport | None = None
        if getattr(peer_node, "gossip_engine", None) is not None:
            report = peer_node.gossip_engine.store.get(peer.peer_id)

        if report is not None:
            active_tasks = report.active_task_count
            cpu_percent = report.cpu_percent
            memory_percent = report.memory_percent
            health_status = report.health_status
        else:
            # 2. Inspect peer record metadata (populated during handshake or load reports)
            meta = peer.metadata
            if "active_task_count" in meta and isinstance(meta["active_task_count"], int):
                active_tasks = meta["active_task_count"]
            elif "active_tasks" in meta and isinstance(meta["active_tasks"], int):
                active_tasks = meta["active_tasks"]

            if "cpu_percent" in meta and isinstance(meta["cpu_percent"], (int, float)):
                cpu_percent = float(meta["cpu_percent"])

            if "memory_percent" in meta and isinstance(meta["memory_percent"], (int, float)):
                memory_percent = float(meta["memory_percent"])

            if "health_status" in meta:
                try:
                    health_status = HealthStatus(meta["health_status"])
                except Exception:
                    health_status = HealthStatus.UNKNOWN

        # Calculate base composite score
        score = (
            (active_tasks * self.active_task_weight)
            + (cpu_percent * self.cpu_weight)
            + (memory_percent * self.memory_weight)
        )

        # Health penalties
        if health_status == HealthStatus.DEGRADED:
            score += self.degraded_penalty
        elif health_status in (HealthStatus.UNHEALTHY, HealthStatus.UNKNOWN):
            score += self.unhealthy_penalty

        return score, active_tasks, cpu_percent, memory_percent, health_status


class TaskRouter:
    """
    Task router selecting target worker nodes from known peers for remote task execution.
    """

    def __init__(
        self,
        peer_node: Any,
        strategy: LoadAwareRoutingStrategy | None = None,
        *,
        scheduler: Any | None = None,
    ) -> None:
        self.peer_node = peer_node
        self.strategy = strategy if strategy is not None else LoadAwareRoutingStrategy()
        if scheduler is not None:
            self._scheduler = scheduler
        else:
            from meshweaver.scheduler import TaskScheduler

            self._scheduler = TaskScheduler(peer_node)

    @property
    def scheduler(self) -> Any:
        """Return the underlying TaskScheduler instance."""
        return self._scheduler

    def rank_candidates(
        self,
        task_request: TaskRequest | None = None,
        *,
        exclude: Iterable[str] | None = None,
        include_stale: bool = False,
    ) -> list[tuple[PeerRecord, float, int, float, float, HealthStatus]]:
        """
        Evaluate and rank all discovered candidate peers in ascending order of load score.
        """
        exclude_set = set(exclude or [])
        exclude_set.add(self.peer_node.peer_id)

        now = time.time()
        registry = self.peer_node.peer_registry
        candidates: list[PeerRecord] = []

        for record in list(registry._peers.values()):
            if record.peer_id in exclude_set:
                continue
            state = record.state(
                current_time=now,
                stale_threshold=registry.stale_threshold,
                ttl=registry.ttl,
            )
            if state == PeerState.EXPIRED:
                continue
            if not include_stale and state == PeerState.STALE:
                continue
            candidates.append(record)

        if not candidates:
            return []

        from meshweaver.dht import NodeID, xor_distance

        # Task ID deterministic anchor for tie-breaking
        task_anchor = (
            NodeID.from_peer(task_request.task_id)
            if task_request is not None and task_request.task_id
            else self.peer_node.node_id
        )

        ranked: list[tuple[PeerRecord, float, int, float, float, HealthStatus, int]] = []
        for peer in candidates:
            score, tasks, cpu, mem, health = self.strategy.calculate_peer_load(
                peer,
                self.peer_node,
            )
            # Tie breaker: XOR distance to task anchor
            dist = int(xor_distance(peer.dht_id, task_anchor))
            ranked.append((peer, score, tasks, cpu, mem, health, dist))

        # Sort: lowest score first; if tied, lowest XOR distance; then peer_id
        ranked.sort(key=lambda item: (item[1], item[6], item[0].peer_id))

        return [(item[0], item[1], item[2], item[3], item[4], item[5]) for item in ranked]

    def select_best_peer(
        self,
        task_request: TaskRequest | None = None,
        *,
        exclude: Iterable[str] | None = None,
    ) -> RouteDecision | None:
        """
        Select the single best candidate peer for executing a task.
        """
        ranked = self.rank_candidates(task_request, exclude=exclude)
        if not ranked:
            return None

        best_peer, score, tasks, cpu, mem, health = ranked[0]
        return RouteDecision(
            selected_peer=best_peer,
            score=score,
            active_tasks=tasks,
            cpu_percent=cpu,
            memory_percent=mem,
            health_status=health,
            candidates_count=len(ranked),
        )

    def select_candidates(
        self,
        task_request: TaskRequest | None = None,
        *,
        count: int = 3,
        exclude: Iterable[str] | None = None,
    ) -> list[PeerRecord]:
        """
        Select up to `count` best ranked candidate peers for task routing / failover.
        """
        ranked = self.rank_candidates(task_request, exclude=exclude)
        return [item[0] for item in ranked[:count]]

    def explain_routing(
        self,
        task_request: TaskRequest | None = None,
        *,
        exclude: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return structured explanation of candidate evaluation for observability/dashboards.
        """
        ranked = self.rank_candidates(task_request, exclude=exclude)
        return [
            {
                "peer_id": peer.peer_id,
                "host": peer.host,
                "port": peer.port,
                "score": round(score, 2),
                "active_tasks": tasks,
                "cpu_percent": round(cpu, 2),
                "memory_percent": round(mem, 2),
                "health_status": health.value,
            }
            for peer, score, tasks, cpu, mem, health in ranked
        ]


# Re-exports from scheduler module for unified import convenience
from meshweaver.scheduler import (
    LoadAwareScheduler,
    SchedulerConfig,
    SchedulingDecision,
    TaskScheduler,
    WorkerEvaluation,
)

__all__: list[str] = [
    "DEFAULT_ACTIVE_TASK_WEIGHT",
    "DEFAULT_CPU_WEIGHT",
    "DEFAULT_DEGRADED_PENALTY",
    "DEFAULT_MEMORY_WEIGHT",
    "DEFAULT_UNHEALTHY_PENALTY",
    "LoadAwareRoutingStrategy",
    "LoadAwareScheduler",
    "RouteDecision",
    "SchedulerConfig",
    "SchedulingDecision",
    "TaskRouter",
    "TaskScheduler",
    "WorkerEvaluation",
]

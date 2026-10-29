"""
Load-aware task scheduling, worker eligibility evaluation, and deterministic tie-breaking.

Week 3, Milestone 2 implementation:
- Selects an eligible worker peer using:
  1. Recent CPU usage percentage
  2. Recent memory usage percentage
  3. Active task count (direct queue concurrency pressure)
  4. Operational peer health (HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN)
  5. Task compatibility (supported tasks, capability tags, worker capacity)
- Documented composite scoring function with configurable weights.
- Deterministic tie-breaking using Kademlia XOR distance and peer ID.
- Strict rejection of stale resource reports (age > stale_report_threshold).
- Strict exclusion of failed, expired, or unhealthy peers.
- Detailed worker evaluation records and explainability for monitoring and dashboards.

Documented Scoring Function:
-----------------------------
For any eligible candidate worker w:

    Score(w) = (w_tasks * active_tasks(w))
             + (w_cpu * cpu_percent(w))
             + (w_mem * memory_percent(w))
             + HealthPenalty(health_status(w))

Where:
- w_tasks (active_task_weight, default: 10.0): Quantifies direct queue/concurrency pressure.
- w_cpu   (cpu_weight, default: 0.5): Scales processor utilization (0.0 to 100.0).
- w_mem   (memory_weight, default: 0.5): Scales memory utilization (0.0 to 100.0).
- HealthPenalty:
    - HEALTHY   -> 0.0
    - DEGRADED  -> degraded_penalty (default: 50.0)
    - UNKNOWN   -> unknown_penalty (default: 200.0)
    - UNHEALTHY -> Disqualified / excluded; or unhealthy_penalty (default: 1000.0)

A lower score indicates lower load and higher suitability.

Deterministic Tie-Breaking:
---------------------------
When multiple eligible workers produce the exact same composite load score:
1. Primary tie-breaker: Kademlia XOR distance between candidate's NodeID and the
   task anchor NodeID (derived from hashing task_id, or local node_id).
2. Secondary tie-breaker: Lexicographical order of candidate's peer_id.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import time
from typing import TYPE_CHECKING, Any, Iterable

from meshweaver.dht import NodeID, xor_distance
from meshweaver.exceptions import (
    IncompatibleWorkerError,
    NoEligibleWorkerError,
    StaleResourceReportError,
    TaskSchedulingError,
)
from meshweaver.gossip import DEFAULT_STALE_THRESHOLD, HealthStatus, NodeResourceReport
from meshweaver.peers import PeerRecord, PeerState
from meshweaver.tasks import TaskRequest

if TYPE_CHECKING:
    from meshweaver.network import PeerNode

logger = logging.getLogger(__name__)

# Default weighting coefficients for load-aware worker scoring
DEFAULT_ACTIVE_TASK_WEIGHT: float = 10.0
DEFAULT_CPU_WEIGHT: float = 0.5
DEFAULT_MEMORY_WEIGHT: float = 0.5
DEFAULT_DEGRADED_PENALTY: float = 50.0
DEFAULT_UNHEALTHY_PENALTY: float = 1000.0
DEFAULT_UNKNOWN_PENALTY: float = 200.0
DEFAULT_STALE_REPORT_THRESHOLD: float = DEFAULT_STALE_THRESHOLD  # 30.0s


@dataclass(frozen=True, slots=True)
class SchedulerConfig:
    """
    Configuration and weighting coefficients for the load-aware task scheduler.

    All weights and penalties are configurable to customize scheduling behavior
    (e.g., favoring CPU-light workers, memory-rich workers, or low queue depths).
    """

    active_task_weight: float = DEFAULT_ACTIVE_TASK_WEIGHT
    cpu_weight: float = DEFAULT_CPU_WEIGHT
    memory_weight: float = DEFAULT_MEMORY_WEIGHT
    degraded_penalty: float = DEFAULT_DEGRADED_PENALTY
    unhealthy_penalty: float = DEFAULT_UNHEALTHY_PENALTY
    unknown_penalty: float = DEFAULT_UNKNOWN_PENALTY
    stale_report_threshold: float = DEFAULT_STALE_REPORT_THRESHOLD
    reject_stale_reports: bool = True
    exclude_failed_peers: bool = True
    check_task_compatibility: bool = True
    exclude_stale_peers: bool = True

    def __post_init__(self) -> None:
        """Validate scheduler configuration coefficients and thresholds."""
        for name, val in [
            ("active_task_weight", self.active_task_weight),
            ("cpu_weight", self.cpu_weight),
            ("memory_weight", self.memory_weight),
            ("degraded_penalty", self.degraded_penalty),
            ("unhealthy_penalty", self.unhealthy_penalty),
            ("unknown_penalty", self.unknown_penalty),
        ]:
            if isinstance(val, bool) or not isinstance(val, (int, float)) or val < 0.0:
                raise ValueError(f"SchedulerConfig '{name}' must be a non-negative float, got {val!r}")

        if (
            isinstance(self.stale_report_threshold, bool)
            or not isinstance(self.stale_report_threshold, (int, float))
            or self.stale_report_threshold <= 0.0
        ):
            raise ValueError(
                f"SchedulerConfig 'stale_report_threshold' must be a positive float, got {self.stale_report_threshold!r}"
            )

    def to_dict(self) -> dict[str, Any]:
        """Convert scheduler configuration to a dictionary."""
        return {
            "active_task_weight": self.active_task_weight,
            "cpu_weight": self.cpu_weight,
            "memory_weight": self.memory_weight,
            "degraded_penalty": self.degraded_penalty,
            "unhealthy_penalty": self.unhealthy_penalty,
            "unknown_penalty": self.unknown_penalty,
            "stale_report_threshold": self.stale_report_threshold,
            "reject_stale_reports": self.reject_stale_reports,
            "exclude_failed_peers": self.exclude_failed_peers,
            "check_task_compatibility": self.check_task_compatibility,
            "exclude_stale_peers": self.exclude_stale_peers,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchedulerConfig:
        """Construct SchedulerConfig from a dictionary."""
        return cls(
            active_task_weight=float(data.get("active_task_weight", DEFAULT_ACTIVE_TASK_WEIGHT)),
            cpu_weight=float(data.get("cpu_weight", DEFAULT_CPU_WEIGHT)),
            memory_weight=float(data.get("memory_weight", DEFAULT_MEMORY_WEIGHT)),
            degraded_penalty=float(data.get("degraded_penalty", DEFAULT_DEGRADED_PENALTY)),
            unhealthy_penalty=float(data.get("unhealthy_penalty", DEFAULT_UNHEALTHY_PENALTY)),
            unknown_penalty=float(data.get("unknown_penalty", DEFAULT_UNKNOWN_PENALTY)),
            stale_report_threshold=float(data.get("stale_report_threshold", DEFAULT_STALE_REPORT_THRESHOLD)),
            reject_stale_reports=bool(data.get("reject_stale_reports", True)),
            exclude_failed_peers=bool(data.get("exclude_failed_peers", True)),
            check_task_compatibility=bool(data.get("check_task_compatibility", True)),
            exclude_stale_peers=bool(data.get("exclude_stale_peers", True)),
        )


@dataclass(frozen=True, slots=True)
class WorkerEvaluation:
    """
    Detailed evaluation snapshot for a candidate worker peer.
    """

    peer: PeerRecord
    is_eligible: bool
    score: float | None
    active_tasks: int
    cpu_percent: float
    memory_percent: float
    health_status: HealthStatus
    report_timestamp: float | None
    report_age: float | None
    is_report_stale: bool
    rejection_reason: str | None
    xor_distance: int | None = None

    @property
    def peer_id(self) -> str:
        """Worker peer ID."""
        return self.peer.peer_id

    def to_dict(self) -> dict[str, Any]:
        """Convert worker evaluation to a dictionary."""
        return {
            "peer_id": self.peer.peer_id,
            "host": self.peer.host,
            "port": self.peer.port,
            "is_eligible": self.is_eligible,
            "score": round(self.score, 2) if self.score is not None else None,
            "active_tasks": self.active_tasks,
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_percent": round(self.memory_percent, 2),
            "health_status": self.health_status.value,
            "report_timestamp": self.report_timestamp,
            "report_age": round(self.report_age, 2) if self.report_age is not None else None,
            "is_report_stale": self.is_report_stale,
            "rejection_reason": self.rejection_reason,
            "xor_distance": self.xor_distance,
        }


@dataclass(frozen=True, slots=True)
class SchedulingDecision:
    """
    Structured outcome of a load-aware task scheduling decision.
    """

    selected_peer: PeerRecord | None
    score: float | None
    active_tasks: int
    cpu_percent: float
    memory_percent: float
    health_status: HealthStatus
    candidates_count: int
    total_evaluated: int
    task_id: str | None
    task_name: str | None
    eligible_workers: list[PeerRecord] = field(default_factory=list)
    rejected_workers: dict[str, str] = field(default_factory=dict)
    evaluations: list[WorkerEvaluation] = field(default_factory=list)
    reason: str = "Lowest composite load score"

    @property
    def is_success(self) -> bool:
        """Return True if an eligible worker was successfully selected."""
        return self.selected_peer is not None

    @property
    def peer_id(self) -> str | None:
        """Selected worker peer ID, or None if no worker was selected."""
        return self.selected_peer.peer_id if self.selected_peer else None

    @property
    def host(self) -> str | None:
        """Selected worker host address."""
        return self.selected_peer.host if self.selected_peer else None

    @property
    def port(self) -> int | None:
        """Selected worker port."""
        return self.selected_peer.port if self.selected_peer else None

    def to_dict(self) -> dict[str, Any]:
        """Convert scheduling decision to a JSON-compatible dictionary."""
        return {
            "selected_peer_id": self.peer_id,
            "host": self.host,
            "port": self.port,
            "score": round(self.score, 2) if self.score is not None else None,
            "active_tasks": self.active_tasks,
            "cpu_percent": round(self.cpu_percent, 2),
            "memory_percent": round(self.memory_percent, 2),
            "health_status": self.health_status.value,
            "candidates_count": self.candidates_count,
            "total_evaluated": self.total_evaluated,
            "task_id": self.task_id,
            "task_name": self.task_name,
            "reason": self.reason,
            "rejected_workers": dict(self.rejected_workers),
        }


class TaskScheduler:
    """
    Load-aware task scheduler selecting the optimal worker peer across the mesh.

    Evaluates candidate worker peers using:
    - Recent CPU utilization percentage
    - Recent memory utilization percentage
    - Active task count (concurrency pressure)
    - Peer operational health (HEALTHY, DEGRADED, UNHEALTHY, UNKNOWN)
    - Task compatibility (supported tasks, capability tags, worker capacity)

    Enforces:
    - Documented composite scoring function with configurable weights.
    - Deterministic tie-breaking (Kademlia XOR distance to task anchor, followed by peer ID).
    - Strict rejection of stale resource reports (rejects reports older than stale_report_threshold).
    - Strict avoidance of failed, unhealthy, or expired peers.
    """

    def __init__(
        self,
        peer_node: Any,
        config: SchedulerConfig | None = None,
        *,
        active_task_weight: float | None = None,
        cpu_weight: float | None = None,
        memory_weight: float | None = None,
        degraded_penalty: float | None = None,
        unhealthy_penalty: float | None = None,
        unknown_penalty: float | None = None,
        stale_report_threshold: float | None = None,
        reject_stale_reports: bool | None = None,
        exclude_failed_peers: bool | None = None,
        check_task_compatibility: bool | None = None,
        exclude_stale_peers: bool | None = None,
    ) -> None:
        """
        Initialize the TaskScheduler.

        Args:
            peer_node: Local PeerNode owning peer registry and gossip engine.
            config: Optional SchedulerConfig object.
            **kwargs: Optional weight/threshold overrides.
        """
        self.peer_node = peer_node

        base_config = config if config is not None else SchedulerConfig()
        overrides: dict[str, Any] = {}
        if active_task_weight is not None:
            overrides["active_task_weight"] = active_task_weight
        if cpu_weight is not None:
            overrides["cpu_weight"] = cpu_weight
        if memory_weight is not None:
            overrides["memory_weight"] = memory_weight
        if degraded_penalty is not None:
            overrides["degraded_penalty"] = degraded_penalty
        if unhealthy_penalty is not None:
            overrides["unhealthy_penalty"] = unhealthy_penalty
        if unknown_penalty is not None:
            overrides["unknown_penalty"] = unknown_penalty
        if stale_report_threshold is not None:
            overrides["stale_report_threshold"] = stale_report_threshold
        if reject_stale_reports is not None:
            overrides["reject_stale_reports"] = reject_stale_reports
        if exclude_failed_peers is not None:
            overrides["exclude_failed_peers"] = exclude_failed_peers
        if check_task_compatibility is not None:
            overrides["check_task_compatibility"] = check_task_compatibility
        if exclude_stale_peers is not None:
            overrides["exclude_stale_peers"] = exclude_stale_peers

        if overrides:
            cfg_dict = base_config.to_dict()
            cfg_dict.update(overrides)
            self.config = SchedulerConfig.from_dict(cfg_dict)
        else:
            self.config = base_config

    @property
    def active_task_weight(self) -> float:
        return self.config.active_task_weight

    @property
    def cpu_weight(self) -> float:
        return self.config.cpu_weight

    @property
    def memory_weight(self) -> float:
        return self.config.memory_weight

    @property
    def degraded_penalty(self) -> float:
        return self.config.degraded_penalty

    @property
    def unhealthy_penalty(self) -> float:
        return self.config.unhealthy_penalty

    @property
    def unknown_penalty(self) -> float:
        return self.config.unknown_penalty

    @property
    def stale_report_threshold(self) -> float:
        return self.config.stale_report_threshold

    def calculate_score(
        self,
        active_tasks: int,
        cpu_percent: float,
        memory_percent: float,
        health_status: HealthStatus | str,
    ) -> float:
        """
        Calculate composite load score for given resource and health metrics.

        Formula:
            Score = (active_tasks * active_task_weight)
                  + (cpu_percent * cpu_weight)
                  + (memory_percent * memory_weight)
                  + HealthPenalty(health_status)

        Returns:
            Computed composite load score (float). Lower is better.
        """
        if isinstance(health_status, str):
            try:
                health_enum = HealthStatus(health_status.lower())
            except ValueError:
                health_enum = HealthStatus.UNKNOWN
        else:
            health_enum = health_status

        base_score = (
            (active_tasks * self.config.active_task_weight)
            + (cpu_percent * self.config.cpu_weight)
            + (memory_percent * self.config.memory_weight)
        )

        penalty = 0.0
        if health_enum == HealthStatus.DEGRADED:
            penalty = self.config.degraded_penalty
        elif health_enum == HealthStatus.UNKNOWN:
            penalty = self.config.unknown_penalty
        elif health_enum == HealthStatus.UNHEALTHY:
            penalty = self.config.unhealthy_penalty

        return round(base_score + penalty, 4)

    def is_task_compatible(
        self,
        peer: PeerRecord,
        task_request: TaskRequest | None,
        report: NodeResourceReport | None = None,
    ) -> tuple[bool, str | None]:
        """
        Verify whether a candidate worker peer is compatible with the given task request.

        Evaluates:
        1. Draining / Maintenance: Worker must not have 'accepting_tasks=False' or 'draining=True'.
        2. Worker capacity: If worker declares 'max_concurrent_tasks' or 'max_tasks',
           active_tasks must not exceed maximum.
        3. Task name support: If worker declares 'supported_tasks' in metadata or report extra,
           task_request.task_name must be present (or wildcard '*' supported).
        4. Capability tags: If task requires capabilities/tags in metadata, worker must provide them.

        Returns:
            tuple[is_compatible, rejection_reason]
        """
        meta = peer.metadata
        extra = report.extra if report is not None else {}

        # 1. Draining / Maintenance mode check
        if meta.get("accepting_tasks") is False or extra.get("accepting_tasks") is False:
            return False, "Worker is not accepting tasks"
        if meta.get("draining") is True or extra.get("draining") is True:
            return False, "Worker is currently draining"

        # 2. Maximum task capacity check
        max_tasks = (
            meta.get("max_concurrent_tasks")
            or meta.get("max_tasks")
            or extra.get("max_concurrent_tasks")
        )
        if isinstance(max_tasks, int) and max_tasks > 0:
            current_active = 0
            if report is not None:
                current_active = report.active_task_count
            elif "active_task_count" in meta and isinstance(meta["active_task_count"], int):
                current_active = meta["active_task_count"]
            elif "active_tasks" in meta and isinstance(meta["active_tasks"], int):
                current_active = meta["active_tasks"]

            if current_active >= max_tasks:
                return (
                    False,
                    f"Worker active tasks ({current_active}) reached capacity limit ({max_tasks})",
                )

        if task_request is None:
            return True, None

        # 2. Supported task names check
        supported = (
            meta.get("supported_tasks")
            or meta.get("supported_task_names")
            or extra.get("supported_tasks")
            or extra.get("supported_task_names")
        )
        if supported is not None:
            if isinstance(supported, (list, tuple, set, frozenset)):
                supported_set = set(supported)
                if "*" not in supported_set and task_request.task_name not in supported_set:
                    return (
                        False,
                        f"Task '{task_request.task_name}' not supported by worker (supports {sorted(supported_set)})",
                    )
            elif isinstance(supported, str):
                if supported != "*" and supported != task_request.task_name:
                    return (
                        False,
                        f"Task '{task_request.task_name}' not supported by worker (supports '{supported}')",
                    )

        # 3. Maximum task capacity check
        max_tasks = (
            meta.get("max_concurrent_tasks")
            or meta.get("max_tasks")
            or extra.get("max_concurrent_tasks")
        )
        if isinstance(max_tasks, int) and max_tasks > 0:
            current_active = 0
            if report is not None:
                current_active = report.active_task_count
            elif "active_task_count" in meta and isinstance(meta["active_task_count"], int):
                current_active = meta["active_task_count"]
            elif "active_tasks" in meta and isinstance(meta["active_tasks"], int):
                current_active = meta["active_tasks"]

            if current_active >= max_tasks:
                return (
                    False,
                    f"Worker active tasks ({current_active}) reached capacity limit ({max_tasks})",
                )

        # 4. Capability / Tag constraints check
        req_caps = task_request.metadata.get("required_capabilities") or task_request.metadata.get(
            "requirements"
        )
        if isinstance(req_caps, (list, tuple, set)):
            worker_caps = set(
                meta.get("capabilities", []) or extra.get("capabilities", [])
            )
            missing = set(req_caps) - worker_caps
            if missing:
                return (
                    False,
                    f"Worker missing required capabilities: {sorted(missing)}",
                )
        elif isinstance(req_caps, dict):
            worker_caps = dict(meta.get("capabilities", {}) or extra.get("capabilities", {}))
            for req_key, req_val in req_caps.items():
                if req_key not in worker_caps or worker_caps[req_key] != req_val:
                    return (
                        False,
                        f"Worker does not meet required capability constraint '{req_key}={req_val}'",
                    )

        return True, None

    def get_peer_resource_report(
        self,
        peer_id: str,
    ) -> NodeResourceReport | None:
        """Retrieve latest resource report for peer from gossip store if available."""
        if getattr(self.peer_node, "gossip_engine", None) is not None:
            return self.peer_node.gossip_engine.store.get(peer_id)
        return None

    def evaluate_worker(
        self,
        peer: PeerRecord,
        task_request: TaskRequest | None = None,
        *,
        now: float | None = None,
        exclude: set[str] | None = None,
    ) -> WorkerEvaluation:
        """
        Evaluate a candidate worker peer for task eligibility and calculate its load score.

        Checks:
        - Exclusion list
        - Registry state (ACTIVE vs STALE vs EXPIRED)
        - Peer operational health (HEALTHY vs DEGRADED vs UNHEALTHY)
        - Resource report freshness (rejects stale reports if age > stale_report_threshold)
        - Task compatibility (supported tasks, capacity, capability tags)
        - Deterministic XOR distance to task anchor

        Returns:
            WorkerEvaluation snapshot
        """
        current_time = now if now is not None else time.time()
        exclude_set = exclude or set()

        # 1. Exclusion list check
        if peer.peer_id in exclude_set:
            return WorkerEvaluation(
                peer=peer,
                is_eligible=False,
                score=None,
                active_tasks=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                health_status=HealthStatus.UNKNOWN,
                report_timestamp=None,
                report_age=None,
                is_report_stale=False,
                rejection_reason="Peer is explicitly excluded",
            )

        # 2. Peer registry lifecycle state check
        registry = getattr(self.peer_node, "peer_registry", None)
        stale_thresh = registry.stale_threshold if registry else DEFAULT_STALE_THRESHOLD
        ttl_val = registry.ttl if registry else 300.0
        peer_state = peer.state(current_time=current_time, stale_threshold=stale_thresh, ttl=ttl_val)

        if peer_state == PeerState.EXPIRED:
            return WorkerEvaluation(
                peer=peer,
                is_eligible=False,
                score=None,
                active_tasks=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                health_status=HealthStatus.UNKNOWN,
                report_timestamp=None,
                report_age=None,
                is_report_stale=False,
                rejection_reason=f"Peer registry record is EXPIRED (last seen {current_time - peer.last_seen:.1f}s ago)",
            )

        if self.config.exclude_stale_peers and peer_state == PeerState.STALE:
            return WorkerEvaluation(
                peer=peer,
                is_eligible=False,
                score=None,
                active_tasks=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                health_status=HealthStatus.UNKNOWN,
                report_timestamp=None,
                report_age=None,
                is_report_stale=False,
                rejection_reason=f"Peer registry record is STALE (last seen {current_time - peer.last_seen:.1f}s ago)",
            )

        # 3. Retrieve resource report or metadata metrics
        report = self.get_peer_resource_report(peer.peer_id)
        report_ts: float | None = None
        report_age: float | None = None
        is_stale_report = False

        active_tasks = 0
        cpu_percent = 0.0
        memory_percent = 0.0
        health_status = HealthStatus.HEALTHY

        if report is not None:
            report_ts = report.timestamp
            report_age = max(0.0, current_time - report.timestamp)
            is_stale_report = report.is_stale(
                current_time=current_time,
                stale_threshold=self.config.stale_report_threshold,
            )
            active_tasks = report.active_task_count
            cpu_percent = report.cpu_percent
            memory_percent = report.memory_percent
            health_status = report.health_status
        else:
            meta = peer.metadata
            if "timestamp" in meta and isinstance(meta["timestamp"], (int, float)):
                report_ts = float(meta["timestamp"])
                report_age = max(0.0, current_time - report_ts)
                is_stale_report = report_age > self.config.stale_report_threshold

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
                    health_status = HealthStatus(str(meta["health_status"]).lower())
                except ValueError:
                    health_status = HealthStatus.UNKNOWN

        # 4. Stale resource report rejection
        if self.config.reject_stale_reports and is_stale_report:
            return WorkerEvaluation(
                peer=peer,
                is_eligible=False,
                score=None,
                active_tasks=active_tasks,
                cpu_percent=cpu_percent,
                memory_percent=memory_percent,
                health_status=health_status,
                report_timestamp=report_ts,
                report_age=report_age,
                is_report_stale=True,
                rejection_reason=(
                    f"Stale resource report rejected (age {report_age:.1f}s > threshold {self.config.stale_report_threshold:.1f}s)"
                ),
            )

        # 5. Peer health / failed peer exclusion
        if self.config.exclude_failed_peers:
            if health_status == HealthStatus.UNHEALTHY:
                return WorkerEvaluation(
                    peer=peer,
                    is_eligible=False,
                    score=None,
                    active_tasks=active_tasks,
                    cpu_percent=cpu_percent,
                    memory_percent=memory_percent,
                    health_status=health_status,
                    report_timestamp=report_ts,
                    report_age=report_age,
                    is_report_stale=is_stale_report,
                    rejection_reason="Worker peer health is UNHEALTHY (failed peer)",
                )

            # Check explicit metadata markers for failed / disconnected peers
            if peer.metadata.get("failed") is True or peer.metadata.get("status") in (
                "failed",
                "dead",
                "unreachable",
            ):
                return WorkerEvaluation(
                    peer=peer,
                    is_eligible=False,
                    score=None,
                    active_tasks=active_tasks,
                    cpu_percent=cpu_percent,
                    memory_percent=memory_percent,
                    health_status=health_status,
                    report_timestamp=report_ts,
                    report_age=report_age,
                    is_report_stale=is_stale_report,
                    rejection_reason="Worker peer is marked as failed/unreachable",
                )

        # 6. Task compatibility check
        if self.config.check_task_compatibility:
            compatible, compat_reason = self.is_task_compatible(peer, task_request, report)
            if not compatible:
                return WorkerEvaluation(
                    peer=peer,
                    is_eligible=False,
                    score=None,
                    active_tasks=active_tasks,
                    cpu_percent=cpu_percent,
                    memory_percent=memory_percent,
                    health_status=health_status,
                    report_timestamp=report_ts,
                    report_age=report_age,
                    is_report_stale=is_stale_report,
                    rejection_reason=f"Task incompatibility: {compat_reason}",
                )

        # 7. Worker is eligible: compute score and tie-breaker distance
        score = self.calculate_score(active_tasks, cpu_percent, memory_percent, health_status)

        # Task anchor for deterministic tie-breaking
        task_anchor = (
            NodeID.from_peer(task_request.task_id)
            if task_request is not None and task_request.task_id
            else getattr(self.peer_node, "node_id", None)
        )
        xor_dist = (
            int(xor_distance(peer.dht_id, task_anchor))
            if task_anchor is not None
            else 0
        )

        return WorkerEvaluation(
            peer=peer,
            is_eligible=True,
            score=score,
            active_tasks=active_tasks,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            health_status=health_status,
            report_timestamp=report_ts,
            report_age=report_age,
            is_report_stale=False,
            rejection_reason=None,
            xor_distance=xor_dist,
        )

    def rank_workers(
        self,
        task_request: TaskRequest | str | None = None,
        *,
        exclude: Iterable[str] | None = None,
        include_stale_peers: bool | None = None,
    ) -> list[WorkerEvaluation]:
        """
        Evaluate and rank all candidate worker peers.

        Eligible workers are sorted by:
        1. Composite load score (ascending: lowest load first)
        2. Kademlia XOR distance to task anchor (deterministic tie-breaker 1)
        3. Lexicographical peer_id (deterministic tie-breaker 2)

        Returns:
            List of WorkerEvaluation instances for all eligible workers in order.
        """
        req: TaskRequest | None = None
        if isinstance(task_request, TaskRequest):
            req = task_request
        elif isinstance(task_request, str) and task_request.strip():
            req = TaskRequest.create(task_name=task_request.strip())

        exclude_set = set(exclude or [])
        local_id = getattr(self.peer_node, "peer_id", None)
        if local_id:
            exclude_set.add(local_id)

        now = time.time()
        registry = getattr(self.peer_node, "peer_registry", None)
        candidates = list(registry._peers.values()) if registry else []

        evaluations: list[WorkerEvaluation] = []
        for peer in candidates:
            ev = self.evaluate_worker(peer, req, now=now, exclude=exclude_set)
            evaluations.append(ev)

        # Filter eligible workers
        eligible = [e for e in evaluations if e.is_eligible and e.score is not None]

        # Deterministic sorting: (score, xor_distance, peer_id)
        eligible.sort(
            key=lambda e: (
                e.score if e.score is not None else float("inf"),
                e.xor_distance if e.xor_distance is not None else 0,
                e.peer.peer_id,
            )
        )

        return eligible

    def schedule(
        self,
        task_request: TaskRequest | str | None = None,
        *,
        exclude: Iterable[str] | None = None,
        raise_if_none: bool = False,
    ) -> SchedulingDecision:
        """
        Perform a load-aware task scheduling decision.

        Selects the single most suitable eligible worker peer based on composite load score,
        with deterministic tie-breaking. Rejects stale resource reports and failed peers.

        Args:
            task_request: TaskRequest or task name string.
            exclude: Optional peer IDs to exclude from scheduling.
            raise_if_none: If True, raises NoEligibleWorkerError if no worker is eligible.

        Returns:
            SchedulingDecision containing the selected worker and evaluation metadata.

        Raises:
            NoEligibleWorkerError: If raise_if_none is True and no eligible worker is found.
        """
        req: TaskRequest | None = None
        if isinstance(task_request, TaskRequest):
            req = task_request
        elif isinstance(task_request, str) and task_request.strip():
            req = TaskRequest.create(task_name=task_request.strip())

        exclude_set = set(exclude or [])
        local_id = getattr(self.peer_node, "peer_id", None)
        if local_id:
            exclude_set.add(local_id)

        now = time.time()
        registry = getattr(self.peer_node, "peer_registry", None)
        candidates = list(registry._peers.values()) if registry else []

        evaluations: list[WorkerEvaluation] = []
        rejected: dict[str, str] = {}
        eligible: list[WorkerEvaluation] = []

        for peer in candidates:
            ev = self.evaluate_worker(peer, req, now=now, exclude=exclude_set)
            evaluations.append(ev)
            if ev.is_eligible and ev.score is not None:
                eligible.append(ev)
            else:
                rejected[peer.peer_id] = ev.rejection_reason or "Ineligible"

        # Deterministic sorting
        eligible.sort(
            key=lambda e: (
                e.score if e.score is not None else float("inf"),
                e.xor_distance if e.xor_distance is not None else 0,
                e.peer.peer_id,
            )
        )

        task_id = req.task_id if req else None
        task_name = req.task_name if req else None

        if not eligible:
            decision = SchedulingDecision(
                selected_peer=None,
                score=None,
                active_tasks=0,
                cpu_percent=0.0,
                memory_percent=0.0,
                health_status=HealthStatus.UNKNOWN,
                candidates_count=0,
                total_evaluated=len(evaluations),
                task_id=task_id,
                task_name=task_name,
                eligible_workers=[],
                rejected_workers=rejected,
                evaluations=evaluations,
                reason="No eligible workers available (all candidates excluded or rejected)",
            )
            if raise_if_none:
                reasons_str = "; ".join(f"{k}: {v}" for k, v in rejected.items())
                raise NoEligibleWorkerError(
                    f"No eligible worker found for task '{task_name or 'unknown'}'. Rejections: {reasons_str}"
                )
            return decision

        best = eligible[0]
        return SchedulingDecision(
            selected_peer=best.peer,
            score=best.score,
            active_tasks=best.active_tasks,
            cpu_percent=best.cpu_percent,
            memory_percent=best.memory_percent,
            health_status=best.health_status,
            candidates_count=len(eligible),
            total_evaluated=len(evaluations),
            task_id=task_id,
            task_name=task_name,
            eligible_workers=[e.peer for e in eligible],
            rejected_workers=rejected,
            evaluations=evaluations,
            reason="Lowest composite load score",
        )

    def select_best_worker(
        self,
        task_request: TaskRequest | str | None = None,
        *,
        exclude: Iterable[str] | None = None,
    ) -> PeerRecord | None:
        """
        Select the single best candidate worker for executing a task.

        Returns:
            PeerRecord of the optimal worker, or None if no eligible worker exists.
        """
        decision = self.schedule(task_request, exclude=exclude)
        return decision.selected_peer

    def select_candidate_workers(
        self,
        task_request: TaskRequest | str | None = None,
        *,
        count: int = 3,
        exclude: Iterable[str] | None = None,
    ) -> list[PeerRecord]:
        """
        Select up to `count` ranked candidate worker peers for task execution and failover.

        Returns:
            List of PeerRecord instances in ascending order of load score.
        """
        ranked = self.rank_workers(task_request, exclude=exclude)
        return [item.peer for item in ranked[:count]]

    def explain(
        self,
        task_request: TaskRequest | str | None = None,
        *,
        exclude: Iterable[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Return structured explanations of worker evaluations for observability and dashboards.
        """
        req: TaskRequest | None = None
        if isinstance(task_request, TaskRequest):
            req = task_request
        elif isinstance(task_request, str) and task_request.strip():
            req = TaskRequest.create(task_name=task_request.strip())

        exclude_set = set(exclude or [])
        local_id = getattr(self.peer_node, "peer_id", None)
        if local_id:
            exclude_set.add(local_id)

        now = time.time()
        registry = getattr(self.peer_node, "peer_registry", None)
        candidates = list(registry._peers.values()) if registry else []

        explanations: list[dict[str, Any]] = []
        for peer in candidates:
            ev = self.evaluate_worker(peer, req, now=now, exclude=exclude_set)
            explanations.append(ev.to_dict())

        # Sort: eligible first (by score), then ineligible
        explanations.sort(
            key=lambda item: (
                0 if item["is_eligible"] else 1,
                item["score"] if item["score"] is not None else float("inf"),
                item["peer_id"],
            )
        )
        return explanations


# Alias for load-aware task scheduler
LoadAwareScheduler = TaskScheduler

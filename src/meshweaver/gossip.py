"""
Asynchronous gossip engine, peer resource monitoring, load reporting, and anti-amplification controls.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
import logging
import os
import random
import sys
import time
from typing import Any

from meshweaver.exceptions import (
    InvalidPayloadError,
    InvalidReportError,
    StaleDataError,
    ValidationError,
)
from meshweaver.peers import PeerRecord, PeerRegistry, validate_peer_id
from meshweaver.protocol import Message, MessageType

logger = logging.getLogger(__name__)

# Gossip protocol defaults and safety limits
DEFAULT_GOSSIP_INTERVAL: float = 5.0
DEFAULT_GOSSIP_FANOUT: int = 3
DEFAULT_STALE_THRESHOLD: float = 30.0
DEFAULT_GOSSIP_TTL: float = 60.0
DEFAULT_MAX_HOPS: int = 3
DEFAULT_CLOCK_SKEW_TOLERANCE: float = 60.0
MAX_GOSSIP_REPORTS_PER_MESSAGE: int = 25


class HealthStatus(StrEnum):
    """Operational and resource health status of a peer node."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

    def is_healthy(self) -> bool:
        """Return True if health status is strictly HEALTHY."""
        return self == HealthStatus.HEALTHY

    def is_usable(self) -> bool:
        """Return True if node is capable of accepting and processing tasks."""
        return self in (HealthStatus.HEALTHY, HealthStatus.DEGRADED)


@dataclass(frozen=True, slots=True)
class NodeResourceReport:
    """
    Immutable representation of a node's resource utilization and operational health.
    """

    node_id: str
    timestamp: float
    cpu_percent: float
    memory_percent: float
    active_task_count: int = 0
    health_status: HealthStatus = HealthStatus.HEALTHY
    # Richer optional metrics
    cpu_count: int | None = None
    memory_total_bytes: int | None = None
    memory_available_bytes: int | None = None
    load_average: tuple[float, float, float] | None = None
    # Gossip dissemination and anti-amplification metadata
    hops: int = 0
    sequence: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """
        Validate structural and value constraints for this resource report.

        Raises:
            InvalidReportError: If any field violates protocol constraints.
        """
        try:
            validate_peer_id(self.node_id)
        except ValidationError as exc:
            raise InvalidReportError(f"Invalid node_id in resource report: {exc}") from exc

        if (
            isinstance(self.timestamp, bool)
            or not isinstance(self.timestamp, (int, float))
            or self.timestamp < 0
        ):
            raise InvalidReportError("Field 'timestamp' must be a non-negative number")

        if (
            isinstance(self.cpu_percent, bool)
            or not isinstance(self.cpu_percent, (int, float))
            or not (0.0 <= self.cpu_percent <= 100.0)
        ):
            raise InvalidReportError(
                f"Field 'cpu_percent' must be a float between 0.0 and 100.0 (got {self.cpu_percent})"
            )

        if (
            isinstance(self.memory_percent, bool)
            or not isinstance(self.memory_percent, (int, float))
            or not (0.0 <= self.memory_percent <= 100.0)
        ):
            raise InvalidReportError(
                f"Field 'memory_percent' must be a float between 0.0 and 100.0 (got {self.memory_percent})"
            )

        if (
            isinstance(self.active_task_count, bool)
            or not isinstance(self.active_task_count, int)
            or self.active_task_count < 0
        ):
            raise InvalidReportError(
                f"Field 'active_task_count' must be a non-negative integer (got {self.active_task_count})"
            )

        if not isinstance(self.health_status, HealthStatus):
            if isinstance(self.health_status, str):
                try:
                    object.__setattr__(self, "health_status", HealthStatus(self.health_status))
                except ValueError:
                    raise InvalidReportError(
                        f"Field 'health_status' must be a valid HealthStatus enum value (got '{self.health_status}')"
                    ) from None
            else:
                raise InvalidReportError(
                    f"Field 'health_status' must be a HealthStatus or string (got {type(self.health_status).__name__})"
                )

        if (
            isinstance(self.hops, bool)
            or not isinstance(self.hops, int)
            or self.hops < 0
        ):
            raise InvalidReportError(f"Field 'hops' must be a non-negative integer (got {self.hops})")

        if (
            isinstance(self.sequence, bool)
            or not isinstance(self.sequence, int)
            or self.sequence < 0
        ):
            raise InvalidReportError(f"Field 'sequence' must be a non-negative integer (got {self.sequence})")

        if self.cpu_count is not None and (
            isinstance(self.cpu_count, bool)
            or not isinstance(self.cpu_count, int)
            or self.cpu_count <= 0
        ):
            raise InvalidReportError("Field 'cpu_count' must be a positive integer if provided")

        if self.memory_total_bytes is not None and (
            isinstance(self.memory_total_bytes, bool)
            or not isinstance(self.memory_total_bytes, int)
            or self.memory_total_bytes < 0
        ):
            raise InvalidReportError("Field 'memory_total_bytes' must be a non-negative integer if provided")

        if self.memory_available_bytes is not None and (
            isinstance(self.memory_available_bytes, bool)
            or not isinstance(self.memory_available_bytes, int)
            or self.memory_available_bytes < 0
        ):
            raise InvalidReportError(
                "Field 'memory_available_bytes' must be a non-negative integer if provided"
            )

        if not isinstance(self.extra, dict):
            raise InvalidReportError("Field 'extra' must be a dictionary")

    @property
    def peer_id(self) -> str:
        """Alias for node_id."""
        return self.node_id

    @property
    def active_tasks(self) -> int:
        """Alias for active_task_count."""
        return self.active_task_count

    def with_incremented_hop(self) -> NodeResourceReport:
        """Return a copy of this report with incremented hop count."""
        return NodeResourceReport(
            node_id=self.node_id,
            timestamp=self.timestamp,
            cpu_percent=self.cpu_percent,
            memory_percent=self.memory_percent,
            active_task_count=self.active_task_count,
            health_status=self.health_status,
            cpu_count=self.cpu_count,
            memory_total_bytes=self.memory_total_bytes,
            memory_available_bytes=self.memory_available_bytes,
            load_average=self.load_average,
            hops=self.hops + 1,
            sequence=self.sequence,
            extra=dict(self.extra),
        )

    def is_stale(
        self,
        current_time: float | None = None,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
    ) -> bool:
        """Return True if report timestamp is older than stale_threshold."""
        now = current_time if current_time is not None else time.time()
        return (now - self.timestamp) > stale_threshold

    def is_expired(
        self,
        current_time: float | None = None,
        ttl: float = DEFAULT_GOSSIP_TTL,
    ) -> bool:
        """Return True if report timestamp is older than ttl."""
        now = current_time if current_time is not None else time.time()
        return (now - self.timestamp) > ttl

    def to_dict(self) -> dict[str, Any]:
        """Convert resource report into a JSON-serializable dictionary."""
        d: dict[str, Any] = {
            "node_id": self.node_id,
            "timestamp": self.timestamp,
            "cpu_percent": self.cpu_percent,
            "memory_percent": self.memory_percent,
            "active_task_count": self.active_task_count,
            "health_status": self.health_status.value,
            "hops": self.hops,
            "sequence": self.sequence,
        }
        if self.cpu_count is not None:
            d["cpu_count"] = self.cpu_count
        if self.memory_total_bytes is not None:
            d["memory_total_bytes"] = self.memory_total_bytes
        if self.memory_available_bytes is not None:
            d["memory_available_bytes"] = self.memory_available_bytes
        if self.load_average is not None:
            d["load_average"] = list(self.load_average)
        if self.extra:
            d["extra"] = self.extra
        return d

    @classmethod
    def from_dict(cls, data: Any) -> NodeResourceReport:
        """
        Reconstruct a NodeResourceReport from a dictionary.

        Raises:
            InvalidReportError: If data is not a dictionary or violates schema.
        """
        if not isinstance(data, dict):
            raise InvalidReportError(f"Expected dict for NodeResourceReport, got {type(data).__name__}")

        node_id = data.get("node_id") or data.get("peer_id")
        if not isinstance(node_id, str) or not node_id.strip():
            raise InvalidReportError("Resource report requires non-empty 'node_id' or 'peer_id'")

        timestamp = data.get("timestamp")
        if timestamp is None:
            timestamp = time.time()

        cpu = data.get("cpu_percent", 0.0)
        mem = data.get("memory_percent", 0.0)
        active = data.get("active_task_count", data.get("active_tasks", 0))

        raw_health = data.get("health_status", HealthStatus.HEALTHY.value)
        health = HealthStatus(raw_health) if isinstance(raw_health, str) else HealthStatus.HEALTHY

        raw_load = data.get("load_average")
        load_tuple: tuple[float, float, float] | None = None
        if isinstance(raw_load, (list, tuple)) and len(raw_load) == 3:
            load_tuple = (float(raw_load[0]), float(raw_load[1]), float(raw_load[2]))

        return cls(
            node_id=str(node_id),
            timestamp=float(timestamp),
            cpu_percent=float(cpu),
            memory_percent=float(mem),
            active_task_count=int(active),
            health_status=health,
            cpu_count=data.get("cpu_count"),
            memory_total_bytes=data.get("memory_total_bytes"),
            memory_available_bytes=data.get("memory_available_bytes"),
            load_average=load_tuple,
            hops=int(data.get("hops", 0)),
            sequence=int(data.get("sequence", 0)),
            extra=dict(data.get("extra", {})),
        )

    def to_message(
        self,
        sender_id: str,
        message_type: MessageType = MessageType.GOSSIP,
        *,
        recipient_id: str | None = None,
        correlation_id: str | None = None,
        message_id: str | None = None,
    ) -> Message:
        """Serialize report into a protocol Message (GOSSIP or LOAD_REPORT)."""
        return Message.create(
            message_type=message_type,
            sender_id=sender_id,
            recipient_id=recipient_id,
            correlation_id=correlation_id,
            message_id=message_id,
            payload=self.to_dict(),
        )

    @classmethod
    def from_message(cls, message: Message) -> NodeResourceReport:
        """Extract and reconstruct a NodeResourceReport from a received Message."""
        if message.message_type not in (MessageType.GOSSIP, MessageType.LOAD_REPORT):
            raise InvalidReportError(
                f"Expected GOSSIP or LOAD_REPORT message, got {message.message_type}"
            )
        payload = dict(message.payload)
        if "node_id" not in payload and "peer_id" not in payload:
            payload["node_id"] = message.sender_id
        if "timestamp" not in payload:
            payload["timestamp"] = message.timestamp
        return cls.from_dict(payload)


# Aliases for flexibility and domain clarity
NodeStatus = NodeResourceReport
ResourceReport = NodeResourceReport


class ResourceCollector:
    """
    Collects system resource utilization metrics and operational health.

    Prioritizes standard Python facilities (ctypes on Windows, /proc on Linux,
    os.getloadavg on Unix) and uses optional psutil if available for richer metrics.
    """

    def __init__(
        self,
        node_id: str,
        *,
        task_count_provider: Callable[[], int] | None = None,
        cpu_threshold_degraded: float = 80.0,
        cpu_threshold_unhealthy: float = 95.0,
        memory_threshold_degraded: float = 85.0,
        memory_threshold_unhealthy: float = 98.0,
        custom_metrics_provider: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self.node_id: str = validate_peer_id(node_id)
        self.task_count_provider: Callable[[], int] | None = task_count_provider
        self.cpu_threshold_degraded: float = float(cpu_threshold_degraded)
        self.cpu_threshold_unhealthy: float = float(cpu_threshold_unhealthy)
        self.memory_threshold_degraded: float = float(memory_threshold_degraded)
        self.memory_threshold_unhealthy: float = float(memory_threshold_unhealthy)
        self.custom_metrics_provider: Callable[[], dict[str, Any]] | None = custom_metrics_provider

        self._sequence_counter: int = 0
        self._last_cpu_sample_time: float = time.perf_counter()
        self._last_process_time: float = time.process_time()

        # Windows ctypes previous times for delta computation
        self._win_prev_idle: int | None = None
        self._win_prev_kernel: int | None = None
        self._win_prev_user: int | None = None

        # Linux /proc/stat previous times
        self._linux_prev_idle: int | None = None
        self._linux_prev_total: int | None = None

    def _sample_active_tasks(self) -> int:
        """Query task count provider if configured, defaulting to 0."""
        if self.task_count_provider is not None:
            try:
                cnt = self.task_count_provider()
                return max(0, int(cnt))
            except Exception as exc:
                logger.debug("Error querying task_count_provider: %s", exc)
        return 0

    def _evaluate_health(self, cpu_percent: float, memory_percent: float) -> HealthStatus:
        """Derive HealthStatus from resource utilization thresholds."""
        if (
            cpu_percent >= self.cpu_threshold_unhealthy
            or memory_percent >= self.memory_threshold_unhealthy
        ):
            return HealthStatus.UNHEALTHY
        if (
            cpu_percent >= self.cpu_threshold_degraded
            or memory_percent >= self.memory_threshold_degraded
        ):
            return HealthStatus.DEGRADED
        return HealthStatus.HEALTHY

    def _collect_psutil(self) -> dict[str, Any] | None:
        """Attempt to collect rich resource metrics using optional psutil library."""
        try:
            import psutil  # type: ignore[import-untyped]
        except (ImportError, ModuleNotFoundError):
            return None

        try:
            cpu = float(psutil.cpu_percent(interval=None))
            mem = psutil.virtual_memory()
            mem_pct = float(mem.percent)
            cpu_count = psutil.cpu_count()
            mem_total = int(mem.total)
            mem_avail = int(mem.available)

            load_avg: tuple[float, float, float] | None = None
            if hasattr(psutil, "getloadavg"):
                try:
                    lavg = psutil.getloadavg()
                    if len(lavg) == 3:
                        load_avg = (float(lavg[0]), float(lavg[1]), float(lavg[2]))
                except (AttributeError, OSError):
                    pass

            return {
                "cpu_percent": min(100.0, max(0.0, round(cpu, 2))),
                "memory_percent": min(100.0, max(0.0, round(mem_pct, 2))),
                "cpu_count": cpu_count,
                "memory_total_bytes": mem_total,
                "memory_available_bytes": mem_avail,
                "load_average": load_avg,
            }
        except Exception as exc:
            logger.debug("psutil collection failed; falling back to stdlib: %s", exc)
            return None

    def _collect_stdlib_windows(self) -> dict[str, Any] | None:
        """Collect memory and CPU metrics on Windows using standard library ctypes."""
        if sys.platform != "win32":
            return None

        try:
            import ctypes
            from ctypes import wintypes

            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", wintypes.DWORD),
                    ("dwMemoryLoad", wintypes.DWORD),
                    ("ullTotalPhys", ctypes.c_uint64),
                    ("ullAvailPhys", ctypes.c_uint64),
                    ("ullTotalPageFile", ctypes.c_uint64),
                    ("ullAvailPageFile", ctypes.c_uint64),
                    ("ullTotalVirtual", ctypes.c_uint64),
                    ("ullAvailVirtual", ctypes.c_uint64),
                    ("ullAvailExtendedVirtual", ctypes.c_uint64),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if hasattr(ctypes, "windll") and hasattr(ctypes.windll, "kernel32"):
                success = ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat))
                if success:
                    mem_pct = float(stat.dwMemoryLoad)
                    mem_total = int(stat.ullTotalPhys)
                    mem_avail = int(stat.ullAvailPhys)
                else:
                    mem_pct, mem_total, mem_avail = 50.0, None, None
            else:
                mem_pct, mem_total, mem_avail = 50.0, None, None

            # CPU percentage via GetSystemTimes
            cpu_pct = self._sample_windows_cpu(ctypes, wintypes)

            return {
                "cpu_percent": min(100.0, max(0.0, round(cpu_pct, 2))),
                "memory_percent": min(100.0, max(0.0, round(mem_pct, 2))),
                "cpu_count": os.cpu_count(),
                "memory_total_bytes": mem_total,
                "memory_available_bytes": mem_avail,
                "load_average": None,
            }
        except Exception as exc:
            logger.debug("Windows stdlib collection failed: %s", exc)
            return None

    def _sample_windows_cpu(self, ctypes: Any, wintypes: Any) -> float:
        """Compute CPU load delta using Windows GetSystemTimes."""
        try:
            class FILETIME(ctypes.Structure):
                _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

            def _to_int(ft: Any) -> int:
                return int((ft.dwHighDateTime << 32) | ft.dwLowDateTime)

            idle_ft, kernel_ft, user_ft = FILETIME(), FILETIME(), FILETIME()
            if hasattr(ctypes.windll.kernel32, "GetSystemTimes"):
                ret = ctypes.windll.kernel32.GetSystemTimes(
                    ctypes.byref(idle_ft),
                    ctypes.byref(kernel_ft),
                    ctypes.byref(user_ft),
                )
                if ret:
                    idle = _to_int(idle_ft)
                    kernel = _to_int(kernel_ft)
                    user = _to_int(user_ft)

                    if self._win_prev_idle is not None and self._win_prev_kernel is not None and self._win_prev_user is not None:
                        delta_idle = idle - self._win_prev_idle
                        delta_kernel = kernel - self._win_prev_kernel
                        delta_user = user - self._win_prev_user
                        delta_total = delta_kernel + delta_user
                        if delta_total > 0:
                            pct = 100.0 * (1.0 - (delta_idle / delta_total))
                            self._win_prev_idle, self._win_prev_kernel, self._win_prev_user = idle, kernel, user
                            return max(0.0, min(100.0, pct))

                    self._win_prev_idle, self._win_prev_kernel, self._win_prev_user = idle, kernel, user
        except Exception:
            pass

        return self._fallback_cpu_estimate()

    def _collect_stdlib_linux(self) -> dict[str, Any] | None:
        """Collect metrics on Linux by inspecting /proc filesystem."""
        if not sys.platform.startswith("linux"):
            return None

        mem_total, mem_avail, mem_pct = None, None, None
        try:
            with open("/proc/meminfo", encoding="ascii") as f:
                lines = f.readlines()
            info: dict[str, int] = {}
            for line in lines:
                parts = line.split(":")
                if len(parts) == 2:
                    k = parts[0].strip()
                    val = parts[1].strip().split()[0]
                    if val.isdigit():
                        info[k] = int(val) * 1024  # kB to bytes
            if "MemTotal" in info and info["MemTotal"] > 0:
                mem_total = info["MemTotal"]
                mem_avail = info.get("MemAvailable", info.get("MemFree", 0))
                used = mem_total - mem_avail
                mem_pct = (used / mem_total) * 100.0
        except Exception:
            pass

        cpu_pct = self._sample_linux_cpu()
        load_avg: tuple[float, float, float] | None = None
        if hasattr(os, "getloadavg"):
            try:
                lavg = os.getloadavg()
                load_avg = (float(lavg[0]), float(lavg[1]), float(lavg[2]))
            except (AttributeError, OSError):
                pass

        if mem_pct is None:
            mem_pct = 50.0

        return {
            "cpu_percent": min(100.0, max(0.0, round(cpu_pct, 2))),
            "memory_percent": min(100.0, max(0.0, round(mem_pct, 2))),
            "cpu_count": os.cpu_count(),
            "memory_total_bytes": mem_total,
            "memory_available_bytes": mem_avail,
            "load_average": load_avg,
        }

    def _sample_linux_cpu(self) -> float:
        """Sample /proc/stat CPU time delta on Linux."""
        try:
            with open("/proc/stat", encoding="ascii") as f:
                first_line = f.readline()
            fields = [int(x) for x in first_line.strip().split()[1:]]
            idle = fields[3] + (fields[4] if len(fields) > 4 else 0)
            total = sum(fields)

            if self._linux_prev_idle is not None and self._linux_prev_total is not None:
                d_idle = idle - self._linux_prev_idle
                d_total = total - self._linux_prev_total
                if d_total > 0:
                    pct = 100.0 * (1.0 - (d_idle / d_total))
                    self._linux_prev_idle, self._linux_prev_total = idle, total
                    return max(0.0, min(100.0, pct))

            self._linux_prev_idle, self._linux_prev_total = idle, total
        except Exception:
            pass

        return self._fallback_cpu_estimate()

    def _fallback_cpu_estimate(self) -> float:
        """Estimate CPU usage via process time delta vs wall clock time."""
        now_perf = time.perf_counter()
        now_proc = time.process_time()

        elapsed_perf = now_perf - self._last_cpu_sample_time
        elapsed_proc = now_proc - self._last_process_time

        self._last_cpu_sample_time = now_perf
        self._last_process_time = now_proc

        if elapsed_perf > 0.001:
            cores = os.cpu_count() or 1
            usage = (elapsed_proc / elapsed_perf) / cores * 100.0
            return max(0.0, min(100.0, usage))

        if hasattr(os, "getloadavg"):
            try:
                lavg = os.getloadavg()
                cores = os.cpu_count() or 1
                return max(0.0, min(100.0, (lavg[0] / cores) * 100.0))
            except Exception:
                pass

        return 15.0

    def collect(self, current_time: float | None = None) -> NodeResourceReport:
        """
        Collect and validate a fresh NodeResourceReport for the local node.
        """
        now = current_time if current_time is not None else time.time()
        self._sequence_counter += 1

        # 1. Try psutil for rich metrics
        metrics = self._collect_psutil()

        # 2. Try OS-specific standard library facilities
        if metrics is None:
            metrics = self._collect_stdlib_windows()
        if metrics is None:
            metrics = self._collect_stdlib_linux()

        # 3. Generic standard library fallback
        if metrics is None:
            cpu_pct = self._fallback_cpu_estimate()
            load_tuple: tuple[float, float, float] | None = None
            if hasattr(os, "getloadavg"):
                try:
                    lavg = os.getloadavg()
                    load_tuple = (float(lavg[0]), float(lavg[1]), float(lavg[2]))
                except Exception:
                    pass
            metrics = {
                "cpu_percent": min(100.0, max(0.0, round(cpu_pct, 2))),
                "memory_percent": 50.0,
                "cpu_count": os.cpu_count(),
                "memory_total_bytes": None,
                "memory_available_bytes": None,
                "load_average": load_tuple,
            }

        active_tasks = self._sample_active_tasks()
        health = self._evaluate_health(metrics["cpu_percent"], metrics["memory_percent"])

        extra_data: dict[str, Any] = {}
        if self.custom_metrics_provider is not None:
            try:
                custom = self.custom_metrics_provider()
                if isinstance(custom, dict):
                    extra_data.update(custom)
            except Exception as exc:
                logger.debug("Custom metrics provider error: %s", exc)

        return NodeResourceReport(
            node_id=self.node_id,
            timestamp=now,
            cpu_percent=metrics["cpu_percent"],
            memory_percent=metrics["memory_percent"],
            active_task_count=active_tasks,
            health_status=health,
            cpu_count=metrics.get("cpu_count"),
            memory_total_bytes=metrics.get("memory_total_bytes"),
            memory_available_bytes=metrics.get("memory_available_bytes"),
            load_average=metrics.get("load_average"),
            hops=0,
            sequence=self._sequence_counter,
            extra=extra_data,
        )


def collect_node_resources(
    node_id: str,
    *,
    task_count_provider: Callable[[], int] | None = None,
    current_time: float | None = None,
) -> NodeResourceReport:
    """Convenience helper to sample local node resources once."""
    collector = ResourceCollector(node_id=node_id, task_count_provider=task_count_provider)
    return collector.collect(current_time=current_time)


class GossipStore:
    """
    In-memory storage and freshness engine for peer resource reports.

    Guarantees:
    - Enforces strict monotonic ordering: rejects out-of-order or duplicate timestamps.
    - Rejects reports exceeding configurable clock skew.
    - Discards expired records on receipt and provides deterministic TTL pruning.
    - Caches seen message signatures to prevent message amplification.
    """

    def __init__(
        self,
        *,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        ttl: float = DEFAULT_GOSSIP_TTL,
        clock_skew_tolerance: float = DEFAULT_CLOCK_SKEW_TOLERANCE,
    ) -> None:
        if stale_threshold < 0:
            raise ValueError("stale_threshold must be non-negative")
        if ttl <= 0 or ttl < stale_threshold:
            raise ValueError("ttl must be positive and greater than or equal to stale_threshold")
        if clock_skew_tolerance < 0:
            raise ValueError("clock_skew_tolerance must be non-negative")

        self.stale_threshold: float = float(stale_threshold)
        self.ttl: float = float(ttl)
        self.clock_skew_tolerance: float = float(clock_skew_tolerance)

        # Primary store: node_id -> NodeResourceReport
        self._reports: dict[str, NodeResourceReport] = {}
        # Seen tracking for anti-amplification: (node_id, timestamp) or node_id -> latest seen timestamp
        self._seen_timestamps: dict[str, float] = {}

    def update(
        self,
        report: NodeResourceReport,
        current_time: float | None = None,
    ) -> bool:
        """
        Incorporate a new resource report into the store.

        Returns:
            bool: True if the report was accepted and updated the store;
                  False if rejected as stale, duplicate, expired, or out-of-order.

        Raises:
            InvalidReportError: If report fails structural validation.
        """
        if not isinstance(report, NodeResourceReport):
            raise InvalidReportError(f"Expected NodeResourceReport, got {type(report).__name__}")
        report.validate()

        now = current_time if current_time is not None else time.time()

        # Reject reports too far into the future (clock skew check)
        if (report.timestamp - now) > self.clock_skew_tolerance:
            logger.warning(
                "Rejected gossip report from %s: timestamp %.2f exceeds clock skew tolerance (now=%.2f)",
                report.node_id,
                report.timestamp,
                now,
            )
            return False

        # Reject reports that have already expired
        if (now - report.timestamp) > self.ttl:
            logger.debug(
                "Rejected already-expired gossip report from %s (age=%.2fs > ttl=%.2fs)",
                report.node_id,
                now - report.timestamp,
                self.ttl,
            )
            return False

        # Monotonicity check against existing stored report for this node
        existing = self._reports.get(report.node_id)
        if existing is not None:
            if report.timestamp <= existing.timestamp:
                logger.debug(
                    "Discarded stale gossip report from %s: new ts %.2f <= existing ts %.2f",
                    report.node_id,
                    report.timestamp,
                    existing.timestamp,
                )
                return False

        # Monotonicity check against anti-amplification seen cache
        seen_ts = self._seen_timestamps.get(report.node_id)
        if seen_ts is not None and report.timestamp <= seen_ts:
            return False

        # Accept novel report
        self._reports[report.node_id] = report
        self._seen_timestamps[report.node_id] = report.timestamp
        return True

    def get(
        self,
        node_id: str,
        *,
        current_time: float | None = None,
        include_expired: bool = False,
    ) -> NodeResourceReport | None:
        """Retrieve the latest report for a node if not expired."""
        rec = self._reports.get(node_id)
        if rec is None:
            return None
        now = current_time if current_time is not None else time.time()
        if not include_expired and rec.is_expired(current_time=now, ttl=self.ttl):
            return None
        return rec

    def is_stale(
        self,
        node_id: str,
        current_time: float | None = None,
    ) -> bool:
        """Return True if stored report for node_id exists and has exceeded stale_threshold."""
        rec = self.get(node_id, current_time=current_time, include_expired=True)
        if rec is None:
            return True
        return rec.is_stale(current_time=current_time, stale_threshold=self.stale_threshold)

    def is_expired(
        self,
        node_id: str,
        current_time: float | None = None,
    ) -> bool:
        """Return True if report for node_id has exceeded TTL or is unknown."""
        rec = self._reports.get(node_id)
        if rec is None:
            return True
        return rec.is_expired(current_time=current_time, ttl=self.ttl)

    def list_reports(
        self,
        *,
        include_stale: bool = True,
        include_expired: bool = False,
        healthy_only: bool = False,
        current_time: float | None = None,
    ) -> list[NodeResourceReport]:
        """Return tracked resource reports matching criteria, sorted by timestamp descending."""
        now = current_time if current_time is not None else time.time()
        results: list[NodeResourceReport] = []

        for rec in self._reports.values():
            if not include_expired and rec.is_expired(current_time=now, ttl=self.ttl):
                continue
            if not include_stale and rec.is_stale(current_time=now, stale_threshold=self.stale_threshold):
                continue
            if healthy_only and not rec.health_status.is_healthy():
                continue
            results.append(rec)

        results.sort(key=lambda r: r.timestamp, reverse=True)
        return results

    def purge_stale(
        self,
        max_age: float | None = None,
        current_time: float | None = None,
    ) -> list[str]:
        """
        Evict records older than max_age (defaulting to configured TTL).

        Returns:
            list[str]: Node IDs of purged records.
        """
        now = current_time if current_time is not None else time.time()
        threshold = max_age if max_age is not None else self.ttl
        purged: list[str] = []

        for node_id, rec in list(self._reports.items()):
            if (now - rec.timestamp) > threshold:
                del self._reports[node_id]
                purged.append(node_id)

        if purged:
            logger.debug("Purged %d expired gossip records: %s", len(purged), purged)
        return purged

    def remove(self, node_id: str) -> bool:
        """
        Remove a node's report and seen cache entry from the store.

        Returns:
            bool: True if a report was removed, False if node_id was not present.
        """
        self._seen_timestamps.pop(node_id, None)
        return self._reports.pop(node_id, None) is not None

    def has_seen(self, node_id: str, timestamp: float) -> bool:
        """Return True if a report for node_id at or after timestamp was already seen."""
        seen_ts = self._seen_timestamps.get(node_id)
        if seen_ts is None:
            return False
        return timestamp <= seen_ts

    def record_seen(self, node_id: str, timestamp: float) -> None:
        """Record node_id and timestamp in seen cache."""
        current = self._seen_timestamps.get(node_id, 0.0)
        if timestamp > current:
            self._seen_timestamps[node_id] = timestamp

    def clear(self) -> None:
        """Clear all stored reports and seen cache."""
        self._reports.clear()
        self._seen_timestamps.clear()

    def __len__(self) -> int:
        return len(self._reports)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._reports


class GossipEngine:
    """
    Asynchronous gossip engine that periodically shares node status with a bounded set of peers.

    Key guarantees:
    - Bounded fanout: Only communicates with at most `fanout` peers per round.
    - Anti-amplification: Hard hop count bound (`max_hops`), seen cache deduplication, and sender exclusion.
    - Staleness detection: Discards out-of-order or expired reports monotonically.
    - Periodic background updates: Asynchronous timer loop with clean lifecycle management.
    """

    def __init__(
        self,
        node_id: str,
        *,
        peer_node: Any | None = None,
        peer_registry: PeerRegistry | None = None,
        gossip_interval: float = DEFAULT_GOSSIP_INTERVAL,
        fanout: int = DEFAULT_GOSSIP_FANOUT,
        stale_threshold: float = DEFAULT_STALE_THRESHOLD,
        ttl: float = DEFAULT_GOSSIP_TTL,
        max_hops: int = DEFAULT_MAX_HOPS,
        clock_skew_tolerance: float = DEFAULT_CLOCK_SKEW_TOLERANCE,
        collector: ResourceCollector | None = None,
        task_count_provider: Callable[[], int] | None = None,
    ) -> None:
        self.node_id: str = validate_peer_id(node_id)
        self.peer_node: Any | None = peer_node
        self.peer_registry: PeerRegistry = (
            peer_registry if peer_registry is not None else PeerRegistry()
        )
        self.gossip_interval: float = float(gossip_interval)
        self.fanout: int = max(1, int(fanout))
        self.max_hops: int = max(1, int(max_hops))

        self.collector: ResourceCollector = (
            collector
            if collector is not None
            else ResourceCollector(node_id=self.node_id, task_count_provider=task_count_provider)
        )
        self.store: GossipStore = GossipStore(
            stale_threshold=stale_threshold,
            ttl=ttl,
            clock_skew_tolerance=clock_skew_tolerance,
        )

        self._is_running: bool = False
        self._loop_task: asyncio.Task[None] | None = None
        self._rounds_completed: int = 0
        self._total_messages_sent: int = 0
        self._total_messages_received: int = 0

    @property
    def is_running(self) -> bool:
        """Return True if background gossip loop is actively running."""
        return self._is_running

    @property
    def rounds_completed(self) -> int:
        """Total count of gossip rounds executed."""
        return self._rounds_completed

    @property
    def total_messages_sent(self) -> int:
        """Total gossip messages transmitted."""
        return self._total_messages_sent

    @property
    def total_messages_received(self) -> int:
        """Total gossip messages received and processed."""
        return self._total_messages_received

    def start(self) -> None:
        """
        Start the background periodic gossip loop.
        """
        if self._is_running:
            return
        self._is_running = True
        self._loop_task = asyncio.create_task(self._periodic_loop())
        logger.info("GossipEngine started for node %s (interval=%.1fs, fanout=%d)", self.node_id, self.gossip_interval, self.fanout)

    async def stop(self) -> None:
        """
        Gracefully stop the background gossip loop and await clean termination.
        """
        if not self._is_running:
            return
        self._is_running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass
            self._loop_task = None
        logger.info("GossipEngine stopped for node %s", self.node_id)

    async def __aenter__(self) -> GossipEngine:
        self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        await self.stop()

    async def _periodic_loop(self) -> None:
        """Background coroutine executing gossip rounds at configured intervals."""
        try:
            while self._is_running:
                try:
                    await self.run_round()
                except Exception as exc:
                    logger.warning("Error during periodic gossip round for %s: %s", self.node_id, exc)

                await asyncio.sleep(self.gossip_interval)
        except asyncio.CancelledError:
            pass

    def collect_local_status(self, current_time: float | None = None) -> NodeResourceReport:
        """Collect fresh local status and update internal store."""
        report = self.collector.collect(current_time=current_time)
        self.store.update(report, current_time=current_time)
        return report

    def _select_peer_targets(self, exclude: Iterable[str] | None = None) -> list[tuple[str, str, int]]:
        """
        Select a bounded set of peer endpoints (at most fanout) to gossip with.

        Returns:
            list[tuple[peer_id, host, port]]: Selected peer targets.
        """
        exclude_set: set[str] = {self.node_id}
        if exclude:
            exclude_set.update(exclude)

        candidates: dict[str, tuple[str, int]] = {}

        # 1. From connected peers if peer_node is available
        if self.peer_node is not None:
            for peer_id, conn in self.peer_node.connected_peers.items():
                if peer_id not in exclude_set and not conn.is_closed:
                    rec = self.peer_registry.get(peer_id)
                    if rec is not None:
                        candidates[peer_id] = (rec.host, rec.port)
                    else:
                        candidates[peer_id] = conn.peer_address

        # 2. From peer_registry
        for record in self.peer_registry.list_active_peers():
            if record.peer_id not in exclude_set and record.peer_id not in candidates:
                candidates[record.peer_id] = (record.host, record.port)

        if not candidates:
            return []

        candidate_list = [(pid, host, port) for pid, (host, port) in candidates.items()]

        # Bound selection to fanout limit
        if len(candidate_list) <= self.fanout:
            return candidate_list

        return random.sample(candidate_list, self.fanout)

    async def run_round(self, current_time: float | None = None) -> int:
        """
        Execute a single round of gossip:
        1. Sample local node status.
        2. Discard stale/expired records from store.
        3. Pick at most `fanout` peers to share status with.
        4. Transmit status report(s) without amplification.

        Returns:
            int: Number of peers contacted in this round.
        """
        now = current_time if current_time is not None else time.time()
        self._rounds_completed += 1

        # 1. Sample and record local status
        local_report = self.collect_local_status(current_time=now)

        # 2. Purge expired records from store
        self.store.purge_stale(current_time=now)

        if self.peer_node is None:
            return 0

        # 3. Select bounded targets (fanout)
        targets = self._select_peer_targets()
        if not targets:
            return 0

        # 4. Prepare gossip payload: local report + fresh reports with hops < max_hops
        reports_to_share: list[dict[str, Any]] = [local_report.to_dict()]
        for other in self.store.list_reports(include_stale=False, include_expired=False, current_time=now):
            if other.node_id != self.node_id and other.hops < self.max_hops:
                # Increment hop count when relaying
                forwarded = other.with_incremented_hop()
                reports_to_share.append(forwarded.to_dict())
                if len(reports_to_share) >= MAX_GOSSIP_REPORTS_PER_MESSAGE:
                    break

        gossip_msg = Message.create(
            MessageType.GOSSIP,
            sender_id=self.node_id,
            payload={
                "reports": reports_to_share,
                "fanout": self.fanout,
                "timestamp": now,
            },
        )

        sent_count = 0
        for peer_id, host, port in targets:
            try:
                conn = self.peer_node.connected_peers.get(peer_id)
                if conn is None or conn.is_closed:
                    conn = await self.peer_node.connect_to_peer(host, port, timeout=2.0)
                await self.peer_node.send_message(conn, gossip_msg)
                sent_count += 1
                self._total_messages_sent += 1
            except Exception as exc:
                logger.debug("Failed sending gossip to %s (%s:%d): %s", peer_id, host, port, exc)

        return sent_count

    async def handle_gossip_message(
        self,
        message: Message,
        connection: Any | None = None,
    ) -> Message | None:
        """
        Handle an incoming GOSSIP or LOAD_REPORT message from a remote peer.

        Validates all incoming reports, discards stale or duplicate updates,
        updates peer registry metadata, and prevents uncontrolled amplification.
        """
        self._total_messages_received += 1
        now = time.time()
        payload = message.payload

        reports_raw: list[dict[str, Any]] = []
        if "reports" in payload:
            raw = payload["reports"]
            if isinstance(raw, list):
                reports_raw.extend(item for item in raw if isinstance(item, dict))
            elif isinstance(raw, dict):
                reports_raw.extend(item for item in raw.values() if isinstance(item, dict))
        else:
            reports_raw.append(payload)

        novel_reports: list[NodeResourceReport] = []

        for item in reports_raw:
            try:
                rec_dict = dict(item)
                if "node_id" not in rec_dict and "peer_id" not in rec_dict:
                    rec_dict["node_id"] = message.sender_id
                if "timestamp" not in rec_dict:
                    rec_dict["timestamp"] = message.timestamp

                report = NodeResourceReport.from_dict(rec_dict)

                # Monotonic and freshness update check
                is_novel = self.store.update(report, current_time=now)
                if is_novel:
                    novel_reports.append(report)

                    # Update peer registry metadata if known
                    if self.peer_registry.has_peer(report.node_id):
                        self.peer_registry.update(
                            report.node_id,
                            metadata={
                                "cpu_percent": report.cpu_percent,
                                "memory_percent": report.memory_percent,
                                "active_tasks": report.active_task_count,
                                "health_status": report.health_status.value,
                                "status_updated_at": report.timestamp,
                            },
                            current_time=now,
                        )
            except Exception as exc:
                logger.warning("Invalid resource report in gossip from %s: %s", message.sender_id, exc)

        logger.debug(
            "Node %s processed gossip message %s: %d total reports, %d novel",
            self.node_id,
            message.message_id,
            len(reports_raw),
            len(novel_reports),
        )
        return None

    def get_status(
        self,
        node_id: str,
        *,
        current_time: float | None = None,
    ) -> NodeResourceReport | None:
        """Retrieve the latest valid resource status for a peer node."""
        return self.store.get(node_id, current_time=current_time)

    def list_statuses(
        self,
        *,
        include_stale: bool = True,
        healthy_only: bool = False,
        current_time: float | None = None,
    ) -> list[NodeResourceReport]:
        """Return all tracked peer resource reports."""
        return self.store.list_reports(
            include_stale=include_stale,
            include_expired=False,
            healthy_only=healthy_only,
            current_time=current_time,
        )

    def get_cluster_summary(self, current_time: float | None = None) -> dict[str, Any]:
        """
        Compute an aggregate summary of cluster health and capacity across known peers.
        """
        now = current_time if current_time is not None else time.time()
        reports = self.list_statuses(include_stale=False, current_time=now)

        total_nodes = len(reports)
        healthy_nodes = sum(1 for r in reports if r.health_status.is_healthy())
        usable_nodes = sum(1 for r in reports if r.health_status.is_usable())
        total_tasks = sum(r.active_task_count for r in reports)

        avg_cpu = sum(r.cpu_percent for r in reports) / total_nodes if total_nodes > 0 else 0.0
        avg_mem = sum(r.memory_percent for r in reports) / total_nodes if total_nodes > 0 else 0.0

        return {
            "total_nodes": total_nodes,
            "healthy_nodes": healthy_nodes,
            "usable_nodes": usable_nodes,
            "total_active_tasks": total_tasks,
            "avg_cpu_percent": round(avg_cpu, 2),
            "avg_memory_percent": round(avg_mem, 2),
            "timestamp": now,
        }

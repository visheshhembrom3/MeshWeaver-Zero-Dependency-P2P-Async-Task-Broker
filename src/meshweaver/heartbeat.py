"""
Heartbeat monitoring and peer failure detection for MeshWeaver.

Provides:
1. Configurable heartbeat intervals, probe timeouts, and missed-heartbeat thresholds.
2. Active peer heartbeat probing (HEARTBEAT -> HEARTBEAT_ACK) with latency measurement.
3. Peer health state transitions: HEALTHY -> DEGRADED -> UNHEALTHY.
4. Automatic detection and marking of unresponsive nodes for scheduling exclusion.
5. Integration with PeerRegistry and event callbacks for failure and recovery hooks.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import logging
import time
from typing import Any, Callable

from meshweaver.exceptions import (
    ConnectionClosedError,
    HeartbeatError,
    HeartbeatTimeoutError,
    NetworkTimeoutError,
    PeerConnectionError,
    PeerNotFoundError,
)
from meshweaver.gossip import HealthStatus
from meshweaver.protocol import Message, MessageType

logger = logging.getLogger(__name__)

DEFAULT_HEARTBEAT_INTERVAL: float = 1.0
DEFAULT_MISSED_THRESHOLD: int = 3
DEFAULT_PROBE_TIMEOUT: float = 1.0


@dataclass(slots=True)
class HeartbeatConfig:
    """
    Configuration parameters for heartbeat probing and failure detection.
    """

    interval: float = DEFAULT_HEARTBEAT_INTERVAL
    missed_threshold: int = DEFAULT_MISSED_THRESHOLD
    probe_timeout: float = DEFAULT_PROBE_TIMEOUT
    degraded_threshold: int = 1
    auto_start: bool = False

    def __post_init__(self) -> None:
        if self.interval <= 0:
            raise ValueError(f"Heartbeat interval must be positive, got {self.interval}")
        if self.missed_threshold <= 0:
            raise ValueError(f"Missed heartbeat threshold must be positive, got {self.missed_threshold}")
        if self.probe_timeout <= 0:
            raise ValueError(f"Probe timeout must be positive, got {self.probe_timeout}")

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "interval": self.interval,
            "missed_threshold": self.missed_threshold,
            "probe_timeout": self.probe_timeout,
            "degraded_threshold": self.degraded_threshold,
            "auto_start": self.auto_start,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HeartbeatConfig:
        """Create configuration from dictionary."""
        return cls(
            interval=float(data.get("interval", DEFAULT_HEARTBEAT_INTERVAL)),
            missed_threshold=int(data.get("missed_threshold", DEFAULT_MISSED_THRESHOLD)),
            probe_timeout=float(data.get("probe_timeout", DEFAULT_PROBE_TIMEOUT)),
            degraded_threshold=int(data.get("degraded_threshold", 1)),
            auto_start=bool(data.get("auto_start", False)),
        )


@dataclass(slots=True)
class PeerHeartbeatState:
    """
    Per-peer heartbeat tracking state, probe statistics, and health transitions.
    """

    peer_id: str
    missed_heartbeats: int = 0
    consecutive_successes: int = 0
    last_heartbeat_sent: float = 0.0
    last_heartbeat_received: float = 0.0
    last_latency_ms: float = 0.0
    health_status: HealthStatus = HealthStatus.HEALTHY
    is_unresponsive: bool = False
    missed_threshold: int = DEFAULT_MISSED_THRESHOLD

    @property
    def health(self) -> HealthStatus:
        """Alias for health_status."""
        return self.health_status

    @property
    def consecutive_misses(self) -> int:
        """Alias for missed_heartbeats."""
        return self.missed_heartbeats

    @property
    def last_rtt_ms(self) -> float:
        """Alias for last_latency_ms."""
        return self.last_latency_ms

    def record_miss(
        self,
        reason: str = "",
        timestamp: float | None = None,
    ) -> HealthStatus:
        """Alias to record_failure using self.missed_threshold."""
        return self.record_failure(missed_threshold=self.missed_threshold, timestamp=timestamp)

    def record_success(
        self,
        latency_ms: float = 0.0,
        rtt_ms: float | None = None,
        timestamp: float | None = None,
    ) -> HealthStatus:
        """
        Record a successful heartbeat acknowledgement.

        Resets consecutive missed heartbeats, updates latency, and transitions health to HEALTHY.
        """
        effective_latency = rtt_ms if rtt_ms is not None else latency_ms
        now = timestamp if timestamp is not None else time.time()
        self.missed_heartbeats = 0
        self.consecutive_successes += 1
        self.last_heartbeat_received = now
        self.last_latency_ms = round(effective_latency, 2)
        self.health_status = HealthStatus.HEALTHY
        self.is_unresponsive = False
        return self.health_status

    def record_failure(
        self,
        missed_threshold: int = DEFAULT_MISSED_THRESHOLD,
        timestamp: float | None = None,
    ) -> HealthStatus:
        """
        Record a missed heartbeat probe or communication failure.

        Increments consecutive missed heartbeats and transitions health state:
        - 1 to (missed_threshold - 1): DEGRADED
        - >= missed_threshold: UNHEALTHY (unresponsive)
        """
        now = timestamp if timestamp is not None else time.time()
        self.missed_heartbeats += 1
        self.consecutive_successes = 0
        self.last_heartbeat_sent = now

        if self.missed_heartbeats >= missed_threshold:
            self.health_status = HealthStatus.UNHEALTHY
            self.is_unresponsive = True
        else:
            self.health_status = HealthStatus.DEGRADED
            self.is_unresponsive = False

        return self.health_status

    def to_dict(self) -> dict[str, Any]:
        """Convert state to a JSON-compatible dictionary."""
        return {
            "peer_id": self.peer_id,
            "missed_heartbeats": self.missed_heartbeats,
            "consecutive_successes": self.consecutive_successes,
            "last_heartbeat_sent": self.last_heartbeat_sent,
            "last_heartbeat_received": self.last_heartbeat_received,
            "last_latency_ms": self.last_latency_ms,
            "health_status": self.health_status.value,
            "is_unresponsive": self.is_unresponsive,
        }


class HeartbeatMonitor:
    """
    Active heartbeat monitor and peer failure detector for a MeshWeaver PeerNode.

    Continuously probes known peers, tracks response latencies, detects missed heartbeats,
    updates PeerRegistry health metadata, and notifies callback listeners of health transitions.
    """

    def __init__(
        self,
        peer_node: Any,
        interval: float = DEFAULT_HEARTBEAT_INTERVAL,
        missed_threshold: int = DEFAULT_MISSED_THRESHOLD,
        probe_timeout: float = DEFAULT_PROBE_TIMEOUT,
        auto_start: bool = False,
        config: HeartbeatConfig | None = None,
        on_peer_unresponsive: Callable[[str, PeerHeartbeatState], Any] | None = None,
        on_peer_recovered: Callable[[str, PeerHeartbeatState], Any] | None = None,
    ) -> None:
        self.peer_node = peer_node
        self.config: HeartbeatConfig = (
            config if config is not None else HeartbeatConfig(
                interval=interval,
                missed_threshold=missed_threshold,
                probe_timeout=probe_timeout,
                auto_start=auto_start,
            )
        )
        self._states: dict[str, PeerHeartbeatState] = {}
        self._running: bool = False
        self._loop_task: asyncio.Task[None] | None = None
        self.on_peer_unresponsive = on_peer_unresponsive
        self.on_peer_recovered = on_peer_recovered

        if self.config.auto_start:
            self.start()

    @property
    def interval(self) -> float:
        return self.config.interval

    @interval.setter
    def interval(self, value: float) -> None:
        if value <= 0:
            raise ValueError("Interval must be positive")
        self.config.interval = value

    @property
    def missed_threshold(self) -> int:
        return self.config.missed_threshold

    @missed_threshold.setter
    def missed_threshold(self, value: int) -> None:
        if value <= 0:
            raise ValueError("Missed threshold must be positive")
        self.config.missed_threshold = value

    @property
    def probe_timeout(self) -> float:
        return self.config.probe_timeout

    @probe_timeout.setter
    def probe_timeout(self, value: float) -> None:
        if value <= 0:
            raise ValueError("Probe timeout must be positive")
        self.config.probe_timeout = value

    @property
    def is_running(self) -> bool:
        return self._running

    def _get_or_create_state(self, peer_id: str) -> PeerHeartbeatState:
        """Retrieve or create tracking state for a peer."""
        state = self._states.get(peer_id)
        if state is None:
            state = PeerHeartbeatState(peer_id=peer_id)
            self._states[peer_id] = state
        return state

    def get_state(self, peer_id: str) -> PeerHeartbeatState | None:
        """Return the heartbeat tracking state for peer_id, or None if untracked."""
        return self._states.get(peer_id)

    def get_peer_state(self, peer_id: str) -> PeerHeartbeatState | None:
        """Return the heartbeat tracking state for peer_id, or None if untracked."""
        return self._states.get(peer_id)

    async def probe_peer(self, peer_id: str, timeout: float | None = None) -> float:
        """
        Probe a peer and return round-trip time (RTT) in milliseconds.

        Raises:
            HeartbeatError: If the heartbeat probe fails or times out.
        """
        success = await self.send_heartbeat(peer_id, timeout=timeout)
        state = self._get_or_create_state(peer_id)
        if not success:
            raise HeartbeatError(f"Heartbeat probe failed for peer '{peer_id}'")
        return state.last_latency_ms

    def get_health_status(self, peer_id: str) -> HealthStatus:
        """Return the current HealthStatus for peer_id."""
        state = self._states.get(peer_id)
        if state is not None:
            return state.health_status
        # Fallback to peer registry if present
        registry = getattr(self.peer_node, "peer_registry", None)
        if registry is not None:
            rec = registry.get(peer_id)
            if rec is not None:
                return rec.health_status
        return HealthStatus.UNKNOWN

    def get_missed_heartbeats(self, peer_id: str) -> int:
        """Return count of consecutive missed heartbeats for peer_id."""
        state = self._states.get(peer_id)
        return state.missed_heartbeats if state else 0

    def is_peer_unresponsive(self, peer_id: str) -> bool:
        """Return True if peer has been detected as unresponsive."""
        state = self._states.get(peer_id)
        if state is not None:
            return state.is_unresponsive
        registry = getattr(self.peer_node, "peer_registry", None)
        if registry is not None:
            rec = registry.get(peer_id)
            if rec is not None:
                return rec.is_unresponsive
        return False

    def is_peer_healthy(self, peer_id: str) -> bool:
        """Return True if peer is strictly HEALTHY."""
        return self.get_health_status(peer_id) == HealthStatus.HEALTHY

    def list_states(self) -> list[PeerHeartbeatState]:
        """Return a snapshot list of all tracked peer heartbeat states."""
        return list(self._states.values())

    def list_unresponsive_peers(self) -> list[str]:
        """Return list of peer IDs currently detected as unresponsive."""
        return [
            peer_id
            for peer_id, state in self._states.items()
            if state.is_unresponsive
        ]

    def record_peer_failure(self, peer_id: str, reason: str = "") -> HealthStatus:
        """
        Directly record a peer failure or disconnection event.

        Useful when external components (like submit_task) detect a socket break
        or connection timeout during task execution.
        """
        state = self._get_or_create_state(peer_id)
        old_status = state.health_status
        new_status = state.record_failure(self.missed_threshold)

        # Mirror update into PeerRegistry
        registry = getattr(self.peer_node, "peer_registry", None)
        if registry is not None and registry.has_peer(peer_id):
            try:
                registry.record_heartbeat_failure(peer_id, max_missed=self.missed_threshold)
            except Exception:
                pass

        logger.debug(
            "Peer %s failure recorded (%s): %s -> %s (missed: %d)",
            peer_id,
            reason or "external failure",
            old_status,
            new_status,
            state.missed_heartbeats,
        )

        if new_status == HealthStatus.UNHEALTHY and old_status != HealthStatus.UNHEALTHY:
            if self.on_peer_unresponsive is not None:
                try:
                    self.on_peer_unresponsive(peer_id, state)
                except Exception as cb_exc:
                    logger.debug("Error in on_peer_unresponsive callback: %s", cb_exc)

        return new_status

    def record_peer_success(self, peer_id: str, latency: float = 0.0) -> HealthStatus:
        """
        Directly record a successful peer interaction or response.
        """
        state = self._get_or_create_state(peer_id)
        old_status = state.health_status
        new_status = state.record_success(latency_ms=latency * 1000.0 if latency < 1.0 else latency)

        registry = getattr(self.peer_node, "peer_registry", None)
        if registry is not None and registry.has_peer(peer_id):
            try:
                registry.record_heartbeat_success(peer_id, latency=latency)
            except Exception:
                pass

        if old_status in (HealthStatus.UNHEALTHY, HealthStatus.DEGRADED) and new_status == HealthStatus.HEALTHY:
            if self.on_peer_recovered is not None:
                try:
                    self.on_peer_recovered(peer_id, state)
                except Exception as cb_exc:
                    logger.debug("Error in on_peer_recovered callback: %s", cb_exc)

        return new_status

    async def send_heartbeat(self, peer_id: str, timeout: float | None = None) -> bool:
        """
        Send an active HEARTBEAT probe to a peer node and await acknowledgement.

        Returns:
            bool: True if acknowledged within timeout, False otherwise.
        """
        effective_timeout = timeout if timeout is not None else self.probe_timeout
        state = self._get_or_create_state(peer_id)
        state.last_heartbeat_sent = time.time()
        old_status = state.health_status

        # Retrieve connection or establish one
        conn = self.peer_node._peer_connections.get(peer_id)
        if conn is None or conn.is_closed:
            rec = self.peer_node.peer_registry.get(peer_id)
            if rec is not None:
                try:
                    conn = await self.peer_node.connect_to_peer(
                        rec.host,
                        rec.port,
                        timeout=effective_timeout,
                        do_handshake=True,
                    )
                except Exception as conn_exc:
                    logger.debug("Heartbeat probe to %s failed to connect: %s", peer_id, conn_exc)
                    self._handle_probe_failure(peer_id, state, old_status, str(conn_exc))
                    return False
            else:
                self._handle_probe_failure(peer_id, state, old_status, "Peer not in registry")
                return False

        # Construct heartbeat probe message
        msg = Message.create(
            MessageType.HEARTBEAT,
            sender_id=self.peer_node.peer_id,
            recipient_id=peer_id,
            payload={
                "node_id": self.peer_node.peer_id,
                "timestamp": time.time(),
            },
        )

        t_start = time.perf_counter()
        try:
            resp = await self.peer_node.send_request(conn, msg, timeout=effective_timeout)
            rtt_ms = (time.perf_counter() - t_start) * 1000.0

            if resp.message_type in (MessageType.HEARTBEAT_ACK, MessageType.PONG):
                new_status = state.record_success(latency_ms=rtt_ms)
                # Mirror in PeerRegistry
                if self.peer_node.peer_registry.has_peer(peer_id):
                    try:
                        self.peer_node.peer_registry.record_heartbeat_success(peer_id, latency=rtt_ms / 1000.0)
                    except Exception:
                        pass

                if old_status in (HealthStatus.UNHEALTHY, HealthStatus.DEGRADED) and new_status == HealthStatus.HEALTHY:
                    if self.on_peer_recovered is not None:
                        try:
                            self.on_peer_recovered(peer_id, state)
                        except Exception as cb_exc:
                            logger.debug("Error in on_peer_recovered callback: %s", cb_exc)
                return True
            else:
                self._handle_probe_failure(
                    peer_id, state, old_status, f"Unexpected message type {resp.message_type}"
                )
                return False

        except (NetworkTimeoutError, TimeoutError) as exc:
            self._handle_probe_failure(peer_id, state, old_status, f"Timeout after {effective_timeout}s: {exc}")
            return False
        except (ConnectionClosedError, PeerConnectionError, OSError) as exc:
            self._handle_probe_failure(peer_id, state, old_status, f"Connection error: {exc}")
            return False
        except Exception as exc:
            self._handle_probe_failure(peer_id, state, old_status, f"Heartbeat error: {exc}")
            return False

    def _handle_probe_failure(
        self,
        peer_id: str,
        state: PeerHeartbeatState,
        old_status: HealthStatus,
        reason: str,
    ) -> None:
        """Helper to process a missed heartbeat probe and trigger transitions."""
        new_status = state.record_failure(self.missed_threshold)

        # Mirror update into PeerRegistry
        if self.peer_node.peer_registry.has_peer(peer_id):
            try:
                self.peer_node.peer_registry.record_heartbeat_failure(
                    peer_id, max_missed=self.missed_threshold
                )
            except Exception:
                pass

        logger.debug(
            "Heartbeat probe missed for peer %s (%s). Missed: %d/%d, Health: %s -> %s",
            peer_id,
            reason,
            state.missed_heartbeats,
            self.missed_threshold,
            old_status,
            new_status,
        )

        if new_status == HealthStatus.UNHEALTHY and old_status != HealthStatus.UNHEALTHY:
            logger.info(
                "Peer %s transitioned to UNHEALTHY (missed %d consecutive heartbeats)",
                peer_id,
                state.missed_heartbeats,
            )
            if self.on_peer_unresponsive is not None:
                try:
                    self.on_peer_unresponsive(peer_id, state)
                except Exception as cb_exc:
                    logger.debug("Error in on_peer_unresponsive callback: %s", cb_exc)

    async def run_round(self) -> dict[str, HealthStatus]:
        """
        Execute one concurrent round of heartbeat probes to all candidate peers.

        Returns:
            dict[str, HealthStatus]: Mapping of peer_id to updated HealthStatus.
        """
        peers_to_probe: set[str] = set()

        # Gather peers from active connections
        for pid in self.peer_node._peer_connections.keys():
            if pid and pid != self.peer_node.peer_id:
                peers_to_probe.add(pid)

        # Gather peers from peer registry
        if hasattr(self.peer_node, "peer_registry"):
            for rec in self.peer_node.peer_registry.list_peers(include_expired=False):
                if rec.peer_id != self.peer_node.peer_id:
                    peers_to_probe.add(rec.peer_id)

        if not peers_to_probe:
            return {}

        results: dict[str, HealthStatus] = {}
        probe_tasks = [self.send_heartbeat(pid) for pid in peers_to_probe]
        await asyncio.gather(*probe_tasks, return_exceptions=True)

        for pid in peers_to_probe:
            results[pid] = self.get_health_status(pid)

        return results

    async def _heartbeat_loop(self) -> None:
        """Continuously probe peers at configured interval."""
        while self._running:
            try:
                await self.run_round()
            except asyncio.CancelledError:
                break
            except Exception as loop_exc:
                logger.debug("Error in heartbeat loop round: %s", loop_exc)

            try:
                await asyncio.sleep(self.interval)
            except asyncio.CancelledError:
                break

    def start(self) -> None:
        """Start the background heartbeat monitoring loop."""
        if self._running:
            return
        self._running = True
        self._loop_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(
            "HeartbeatMonitor started for node %s (interval=%.1fs, threshold=%d)",
            self.peer_node.peer_id,
            self.interval,
            self.missed_threshold,
        )

    async def stop(self) -> None:
        """Gracefully stop the heartbeat monitoring loop."""
        if not self._running and self._loop_task is None:
            return
        self._running = False
        if self._loop_task is not None:
            self._loop_task.cancel()
            try:
                await asyncio.shield(asyncio.wait_for(self._loop_task, timeout=1.0))
            except (asyncio.CancelledError, TimeoutError, Exception):
                pass
            self._loop_task = None
        logger.info("HeartbeatMonitor stopped for node %s", self.peer_node.peer_id)

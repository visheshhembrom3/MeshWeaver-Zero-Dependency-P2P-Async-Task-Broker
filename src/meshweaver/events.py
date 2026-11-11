"""
Event broadcasting, lifecycle logging, and real-time subscription management for MeshWeaver.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import StrEnum
import logging
import time
from typing import Any
import uuid

logger = logging.getLogger(__name__)


class MeshEventType(StrEnum):
    """Enumeration of observable network and task events."""

    PEER_JOINED = "peer_joined"
    PEER_DISCONNECTED = "peer_disconnected"
    PEER_RECOVERED = "peer_recovered"
    GOSSIP_UPDATE = "gossip_update"
    TASK_SUBMITTED = "task_submitted"
    TASK_STARTED = "task_started"
    TASK_COMPLETED = "task_completed"
    TASK_FAILED = "task_failed"
    TASK_REASSIGNED = "task_reassigned"


@dataclass(slots=True)
class MeshEvent:
    """Structured record of an event in the MeshWeaver network."""

    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    timestamp: float = field(default_factory=time.time)
    event_type: str = MeshEventType.PEER_JOINED.value
    severity: str = "info"  # "info" | "success" | "warning" | "error"
    peer_id: str | None = None
    task_id: str | None = None
    message: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert event to a JSON-serializable dictionary."""
        return {
            "id": self.id,
            "timestamp": round(self.timestamp, 4),
            "event_type": self.event_type,
            "severity": self.severity,
            "peer_id": self.peer_id,
            "task_id": self.task_id,
            "message": self.message,
            "details": self.details,
        }


class EventManager:
    """
    Central event hub managing an in-memory ring buffer of events
    and publishing real-time updates to active subscribers (e.g., WebSockets).
    """

    def __init__(self, max_history: int = 1000) -> None:
        self.max_history: int = max_history
        self._events: list[MeshEvent] = []
        self._subscribers: set[asyncio.Queue[MeshEvent]] = set()
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def event_count(self) -> int:
        """Return total stored events count."""
        return len(self._events)

    def get_events(
        self,
        *,
        limit: int = 100,
        event_type: str | None = None,
        severity: str | None = None,
        peer_id: str | None = None,
        task_id: str | None = None,
    ) -> list[MeshEvent]:
        """Query historical events with optional filters, newest first."""
        results = reversed(self._events)
        if event_type:
            results = [e for e in results if e.event_type == event_type]
        if severity:
            results = [e for e in results if e.severity == severity]
        if peer_id:
            results = [e for e in results if e.peer_id == peer_id]
        if task_id:
            results = [e for e in results if e.task_id == task_id]
        return list(results)[:limit]

    def record_event(
        self,
        event_type: str | MeshEventType,
        message: str,
        *,
        severity: str = "info",
        peer_id: str | None = None,
        task_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> MeshEvent:
        """
        Record a new event and broadcast it asynchronously to active subscribers.
        """
        resolved_type = event_type.value if isinstance(event_type, MeshEventType) else str(event_type)
        event = MeshEvent(
            event_type=resolved_type,
            severity=severity,
            peer_id=peer_id,
            task_id=task_id,
            message=message,
            details=details or {},
        )

        self._events.append(event)
        if len(self._events) > self.max_history:
            self._events = self._events[-self.max_history:]

        # Push to all active subscriber queues non-blockingly
        for queue in list(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                logger.debug("Subscriber queue full; dropping event for subscriber")
            except Exception as exc:
                logger.debug("Error delivering event to subscriber: %s", exc)

        return event

    def subscribe(self) -> asyncio.Queue[MeshEvent]:
        """Subscribe to live event stream. Returns an asyncio Queue."""
        queue: asyncio.Queue[MeshEvent] = asyncio.Queue(maxsize=200)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[MeshEvent]) -> None:
        """Unsubscribe from live event stream."""
        self._subscribers.discard(queue)


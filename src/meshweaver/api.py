"""
FastAPI REST and WebSocket adapter for MeshWeaver P2P task broker.

Exposes real-time endpoints for peer registry, DHT routing table,
gossip resource metrics, allowlisted task submission, and live event streaming.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from meshweaver.events import EventManager, MeshEventType
from meshweaver.network import PeerNode

logger = logging.getLogger(__name__)


class TaskSubmitRequest(BaseModel):
    """Validated payload for submitting a task to the mesh."""

    task_name: str = Field(..., description="Allowlisted task name (e.g. add, reverse_string, echo, ping)")
    args: list[Any] = Field(default_factory=list, description="Positional arguments for the task handler")
    kwargs: dict[str, Any] = Field(default_factory=dict, description="Keyword arguments for the task handler")
    timeout: float = Field(default=5.0, ge=0.1, le=120.0, description="Task execution timeout in seconds")
    require_ack: bool = Field(default=True, description="Whether to require worker acknowledgement")


def create_api_app(
    node: PeerNode,
    event_manager: EventManager | None = None,
    static_dist_dir: str | Path | None = None,
) -> FastAPI:
    """
    Create and configure the FastAPI application bound to a running PeerNode instance.
    """
    events = event_manager if event_manager is not None else EventManager()
    known_peer_ids: set[str] = set()
    start_time = time.time()
    sync_task: asyncio.Task[None] | None = None

    # Wire up heartbeat failure & recovery hooks
    def _on_peer_unresponsive(peer_id: str, state: Any) -> None:
        events.record_event(
            MeshEventType.PEER_DISCONNECTED,
            f"Peer '{peer_id}' became unresponsive ({getattr(state, 'missed_heartbeats', '?')} missed heartbeats)",
            severity="warning",
            peer_id=peer_id,
            details={"missed_heartbeats": getattr(state, "missed_heartbeats", 0)},
        )

    def _on_peer_recovered(peer_id: str, state: Any) -> None:
        events.record_event(
            MeshEventType.PEER_RECOVERED,
            f"Peer '{peer_id}' recovered and resumed responsive heartbeats",
            severity="success",
            peer_id=peer_id,
            details={"latency_ms": round(getattr(state, "last_latency_ms", 0.0), 2)},
        )

    # Attach callbacks to heartbeat monitor
    orig_unresp = node.heartbeat_monitor.on_peer_unresponsive
    orig_recov = node.heartbeat_monitor.on_peer_recovered

    def _chained_unresponsive(peer_id: str, state: Any) -> None:
        _on_peer_unresponsive(peer_id, state)
        if callable(orig_unresp):
            orig_unresp(peer_id, state)

    def _chained_recovered(peer_id: str, state: Any) -> None:
        _on_peer_recovered(peer_id, state)
        if callable(orig_recov):
            orig_recov(peer_id, state)

    node.heartbeat_monitor.on_peer_unresponsive = _chained_unresponsive
    node.heartbeat_monitor.on_peer_recovered = _chained_recovered

    async def _periodic_mesh_sync() -> None:
        """Periodically observe mesh peers and record peer join/gossip events."""
        while True:
            try:
                await asyncio.sleep(2.0)
                current_peers = set(node.peer_registry._peers.keys())
                new_peers = current_peers - known_peer_ids
                for p_id in new_peers:
                    rec = node.peer_registry.get(p_id)
                    host_port = f"{rec.endpoint.host}:{rec.endpoint.port}" if rec else "unknown"
                    events.record_event(
                        MeshEventType.PEER_JOINED,
                        f"Discovered new peer '{p_id}' at {host_port}",
                        severity="info",
                        peer_id=p_id,
                        details={"endpoint": host_port},
                    )
                    known_peer_ids.add(p_id)

                # Track disconnected peers removed from registry
                removed_peers = known_peer_ids - current_peers
                for p_id in removed_peers:
                    events.record_event(
                        MeshEventType.PEER_DISCONNECTED,
                        f"Peer '{p_id}' removed from active registry",
                        severity="warning",
                        peer_id=p_id,
                    )
                    known_peer_ids.remove(p_id)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.debug("Error in periodic mesh sync: %s", exc)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal sync_task
        # Initialize initial known peers
        known_peer_ids.update(node.peer_registry._peers.keys())
        events.record_event(
            MeshEventType.GOSSIP_UPDATE,
            f"Node '{node.peer_id}' API initialized on {node.host}:{node.bound_port}",
            severity="info",
            peer_id=node.peer_id,
        )
        sync_task = asyncio.create_task(_periodic_mesh_sync())
        try:
            yield
        finally:
            if sync_task and not sync_task.done():
                sync_task.cancel()
                try:
                    await sync_task
                except asyncio.CancelledError:
                    pass

    app = FastAPI(
        title="MeshWeaver Task Broker API",
        description="REST & WebSocket API for MeshWeaver P2P task distribution network",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS configuration for frontend dev server
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Store references on app state
    app.state.node = node
    app.state.events = events
    app.state.start_time = start_time

    # ---------------------------------------------------------
    # REST Endpoints
    # ---------------------------------------------------------

    @app.get("/api/health")
    async def get_health() -> dict[str, Any]:
        """Health check endpoint indicating node running status and bound port."""
        return {
            "status": "ok" if node.is_running else "stopped",
            "node_id": node.peer_id,
            "host": node.host,
            "port": node.port,
            "bound_port": node.bound_port,
            "uptime_seconds": round(time.time() - start_time, 2),
            "is_running": node.is_running,
            "dht_id_hex": node.node_id.hex if hasattr(node.node_id, "hex") else str(node.node_id),
            "timestamp": time.time(),
        }

    @app.get("/api/node")
    async def get_node_details() -> dict[str, Any]:
        """Detailed status snapshot of the local node."""
        status = node.get_status()
        status_dict = status.to_dict()

        # Add registered tasks allowlist & DHT table details
        status_dict["registered_tasks"] = node.task_registry.list_tasks()
        status_dict["max_concurrency"] = (
            node.execution_engine.max_concurrency
            if hasattr(node, "execution_engine") and node.execution_engine
            else 4
        )
        status_dict["uptime_seconds"] = round(time.time() - start_time, 2)
        status_dict["gossip_enabled"] = node.gossip_engine is not None
        status_dict["heartbeat_enabled"] = node.heartbeat_monitor.is_running
        status_dict["heartbeat_interval"] = node.heartbeat_monitor.interval
        status_dict["missed_threshold"] = node.heartbeat_monitor.missed_threshold

        return status_dict

    @app.get("/api/peers")
    async def list_peers() -> list[dict[str, Any]]:
        """List all known peers with connection, heartbeat, and resource metrics."""
        status = node.get_status()
        peers_list: list[dict[str, Any]] = []

        for p in status.peers:
            p_dict = p.to_dict()
            # Enrich with heartbeat probe stats if available
            hb_state = node.heartbeat_monitor.get_state(p.peer_id)
            if hb_state is not None:
                p_dict["missed_heartbeats"] = hb_state.missed_heartbeats
                p_dict["last_latency_ms"] = round(hb_state.last_latency_ms, 2)
                p_dict["is_unresponsive"] = hb_state.is_unresponsive
            else:
                p_dict["missed_heartbeats"] = 0
                p_dict["last_latency_ms"] = None
                p_dict["is_unresponsive"] = False

            # Add supported tasks if known from metadata
            p_dict["supported_tasks"] = p.metadata.get("supported_tasks", [])
            peers_list.append(p_dict)

        return peers_list

    @app.get("/api/peers/{peer_id}")
    async def get_peer(peer_id: str) -> dict[str, Any]:
        """Get detailed status for a specific peer."""
        p_status = node.get_peer_status(peer_id)
        if p_status is None:
            raise HTTPException(status_code=404, detail=f"Peer '{peer_id}' not found in registry")

        p_dict = p_status.to_dict()
        hb_state = node.heartbeat_monitor.get_state(peer_id)
        if hb_state is not None:
            p_dict["missed_heartbeats"] = hb_state.missed_heartbeats
            p_dict["last_latency_ms"] = round(hb_state.last_latency_ms, 2)
            p_dict["is_unresponsive"] = hb_state.is_unresponsive
            p_dict["last_heartbeat_sent"] = hb_state.last_heartbeat_sent
            p_dict["last_heartbeat_received"] = hb_state.last_heartbeat_received
        else:
            p_dict["missed_heartbeats"] = 0
            p_dict["last_latency_ms"] = None
            p_dict["is_unresponsive"] = False

        p_dict["supported_tasks"] = p_status.metadata.get("supported_tasks", [])
        return p_dict

    @app.get("/api/tasks")
    async def list_tasks(
        status: str | None = Query(None, description="Filter by status (pending, running, success, failure, error)"),
        limit: int = Query(100, ge=1, le=500),
    ) -> list[dict[str, Any]]:
        """List tracked tasks from deduplication and execution stores."""
        tasks: list[dict[str, Any]] = []

        # 1. From deduplication store
        if hasattr(node, "_task_dedup"):
            for t_id, rec in node._task_dedup._records.items():
                resp = rec.get("response")
                ownership = rec.get("ownership")
                ack = rec.get("ack")

                t_status = rec.get("status")
                if hasattr(t_status, "value"):
                    t_status_str = t_status.value
                else:
                    t_status_str = str(t_status or "pending")

                task_item: dict[str, Any] = {
                    "task_id": t_id,
                    "correlation_id": rec.get("correlation_id"),
                    "task_name": rec.get("task_name") or (resp.metadata.get("task_name") if resp else "unknown"),
                    "status": t_status_str,
                    "routed_to": rec.get("routed_to"),
                    "timestamp": rec.get("timestamp"),
                    "completed_at": rec.get("completed_at"),
                    "result": resp.result if resp else None,
                    "error": resp.error.to_dict() if resp and resp.error else None,
                    "execution_time": resp.execution_time if resp else None,
                    "retry_count": ownership.retry_count if ownership else 0,
                    "max_retries": ownership.max_retries if ownership else 2,
                    "worker_history": ownership.worker_history if ownership else [],
                    "ack": ack.to_dict() if ack and hasattr(ack, "to_dict") else None,
                }
                tasks.append(task_item)

        # 2. From local execution engine
        if hasattr(node, "execution_engine") and node.execution_engine:
            for t_id, erec in node.execution_engine._tasks.items():
                if any(t["task_id"] == t_id for t in tasks):
                    continue
                tasks.append({
                    "task_id": t_id,
                    "correlation_id": erec.correlation_id,
                    "task_name": erec.request.task_name if erec.request else "unknown",
                    "status": erec.state.value if hasattr(erec.state, "value") else str(erec.state),
                    "routed_to": node.peer_id,
                    "timestamp": erec.submitted_at,
                    "completed_at": erec.completed_at,
                    "result": erec.response.result if erec.response else None,
                    "error": erec.response.error.to_dict() if erec.response and erec.response.error else None,
                    "execution_time": erec.execution_time,
                    "retry_count": 0,
                    "max_retries": 2,
                    "worker_history": [node.peer_id],
                    "ack": None,
                })

        # Filter by status if requested
        if status:
            tasks = [t for t in tasks if t["status"].lower() == status.lower()]

        # Sort newest first
        tasks.sort(key=lambda t: t.get("timestamp") or 0.0, reverse=True)
        return tasks[:limit]

    @app.get("/api/tasks/available")
    async def get_available_tasks() -> list[dict[str, Any]]:
        """
        List allowlisted task functions supported by the node's TaskRegistry.
        Only functions in this list can be submitted through the web UI.
        """
        registered = node.task_registry.list_tasks()
        task_info: list[dict[str, Any]] = []

        descriptions = {
            "ping": "Test connectivity with worker peer (returns 'pong')",
            "echo": "Echo back provided arguments and keyword arguments",
            "add": "Add two numbers: add(a, b)",
            "reverse_string": "Reverse a text string: reverse_string(s)",
        }

        for name in registered:
            task_info.append({
                "name": name,
                "description": descriptions.get(name, f"Custom registered task function '{name}'"),
                "is_allowlisted": True,
            })

        return task_info

    @app.get("/api/tasks/{task_id}")
    async def get_task_details(task_id: str) -> dict[str, Any]:
        """Get status and result for a specific task."""
        # Check deduplication store
        if hasattr(node, "_task_dedup"):
            rec = node._task_dedup.get_record(task_id)
            if rec is not None:
                resp = rec.get("response")
                ownership = rec.get("ownership")
                ack = rec.get("ack")

                t_status = rec.get("status")
                t_status_str = t_status.value if hasattr(t_status, "value") else str(t_status)

                return {
                    "task_id": task_id,
                    "correlation_id": rec.get("correlation_id"),
                    "task_name": rec.get("task_name") or (resp.metadata.get("task_name") if resp else "unknown"),
                    "status": t_status_str,
                    "routed_to": rec.get("routed_to"),
                    "timestamp": rec.get("timestamp"),
                    "completed_at": rec.get("completed_at"),
                    "result": resp.result if resp else None,
                    "error": resp.error.to_dict() if resp and resp.error else None,
                    "execution_time": resp.execution_time if resp else None,
                    "ownership": ownership.to_dict() if ownership else None,
                    "ack": ack.to_dict() if ack and hasattr(ack, "to_dict") else None,
                }

        # Check local execution engine
        if hasattr(node, "execution_engine") and node.execution_engine:
            erec = node.execution_engine.get_task(task_id)
            if erec is not None:
                return {
                    "task_id": task_id,
                    "correlation_id": erec.correlation_id,
                    "task_name": erec.request.task_name if erec.request else "unknown",
                    "status": erec.state.value if hasattr(erec.state, "value") else str(erec.state),
                    "routed_to": node.peer_id,
                    "timestamp": erec.submitted_at,
                    "completed_at": erec.completed_at,
                    "result": erec.response.result if erec.response else None,
                    "error": erec.response.error.to_dict() if erec.response and erec.response.error else None,
                    "execution_time": erec.execution_time,
                    "ownership": {
                        "task_id": task_id,
                        "owner_id": node.peer_id,
                        "assigned_worker_id": node.peer_id,
                        "retry_count": 0,
                        "max_retries": 2,
                        "status": "completed" if erec.is_completed else "running",
                    },
                    "ack": None,
                }

        raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")

    @app.post("/api/tasks")
    async def submit_task(req: TaskSubmitRequest) -> dict[str, Any]:
        """
        Submit a task for asynchronous execution across the mesh.

        SECURITY GUARANTEE:
        Only tasks registered in TaskRegistry are permitted.
        Arbitrary code execution, arbitrary imports, and untrusted pickling are strictly blocked.
        """
        # Security Allowlist Verification
        available_tasks = node.task_registry.list_tasks()
        if req.task_name not in available_tasks:
            raise HTTPException(
                status_code=400,
                detail=f"Task '{req.task_name}' is not allowed. Must be one of: {available_tasks}",
            )

        events.record_event(
            MeshEventType.TASK_SUBMITTED,
            f"Submitted task '{req.task_name}' with args={req.args}",
            severity="info",
            peer_id=node.peer_id,
            details={"task_name": req.task_name, "args": req.args},
        )

        try:
            # If no remote peers are registered in mesh, target local node directly
            target_worker = node.peer_id if node.peer_registry.peer_count == 0 else None

            resp = await node.submit_task(
                req.task_name,
                *req.args,
                target_peer_id=target_worker,
                timeout=req.timeout,
                **req.kwargs,
            )

            # Record task completion event
            if resp.status.value == "success":
                events.record_event(
                    MeshEventType.TASK_COMPLETED,
                    f"Task '{req.task_name}' ({resp.task_id}) completed successfully",
                    severity="success",
                    task_id=resp.task_id,
                    peer_id=resp.metadata.get("worker", node.peer_id),
                    details={"result": resp.result, "execution_time": resp.execution_time},
                )
            else:
                err_code = resp.error.code if resp.error else "ERROR"
                err_msg = resp.error.message if resp.error else "Unknown failure"
                events.record_event(
                    MeshEventType.TASK_FAILED,
                    f"Task '{req.task_name}' ({resp.task_id}) failed: [{err_code}] {err_msg}",
                    severity="error",
                    task_id=resp.task_id,
                    details={"error": err_msg, "code": err_code},
                )

            # Store task_name in record for dashboard lookup
            if hasattr(node, "_task_dedup") and resp.task_id in node._task_dedup._records:
                node._task_dedup._records[resp.task_id]["task_name"] = req.task_name

            return resp.to_dict()

        except Exception as exc:
            logger.exception("Failed to dispatch task %s", req.task_name)
            events.record_event(
                MeshEventType.TASK_FAILED,
                f"Task submission error: {exc}",
                severity="error",
                details={"error": str(exc)},
            )
            raise HTTPException(status_code=500, detail=f"Task submission failed: {exc}")

    @app.get("/api/metrics")
    async def get_metrics() -> dict[str, Any]:
        """Aggregate system metrics, resource loads, and task execution counts."""
        status = node.get_status()

        # Task metrics from dedup and execution stores
        all_tasks = await list_tasks(limit=500)
        total_tasks = len(all_tasks)
        pending_tasks = sum(1 for t in all_tasks if t.get("status") == "pending")
        running_tasks = sum(1 for t in all_tasks if t.get("status") == "running")
        completed_tasks = sum(1 for t in all_tasks if t.get("status") == "success")
        failed_tasks = sum(1 for t in all_tasks if t.get("status") in ("failure", "error", "timeout"))

        exec_times = [
            float(t["execution_time"])
            for t in all_tasks
            if t.get("execution_time") is not None and isinstance(t.get("execution_time"), (int, float))
        ]
        avg_exec_time = round(sum(exec_times) / len(exec_times), 4) if exec_times else 0.0
        success_rate = round((completed_tasks / total_tasks * 100), 1) if total_tasks > 0 else 100.0

        # Peer metrics breakdown
        peers = status.peers
        active_peers = [p for p in peers if p.is_active and not getattr(node.heartbeat_monitor.get_state(p.peer_id), "is_unresponsive", False)]
        unresponsive_peers = [p for p in peers if getattr(node.heartbeat_monitor.get_state(p.peer_id), "is_unresponsive", False)]

        peer_loads = []
        for p in peers:
            hb = node.heartbeat_monitor.get_state(p.peer_id)
            peer_loads.append({
                "peer_id": p.peer_id,
                "cpu_percent": p.cpu_percent if p.cpu_percent is not None else 0.0,
                "memory_percent": p.memory_percent if p.memory_percent is not None else 0.0,
                "active_tasks": p.active_task_count,
                "latency_ms": round(hb.last_latency_ms, 2) if hb else None,
                "health": p.health_status.value,
                "is_active": p.is_active,
            })

        # Add local node to peer loads
        peer_loads.insert(0, {
            "peer_id": f"{node.peer_id} (Local)",
            "cpu_percent": status.cpu_percent if status.cpu_percent is not None else 0.0,
            "memory_percent": status.memory_percent if status.memory_percent is not None else 0.0,
            "active_tasks": status.active_task_count,
            "latency_ms": 0.0,
            "health": status.health_status.value,
            "is_active": True,
        })

        return {
            "local_node": {
                "node_id": node.peer_id,
                "cpu_percent": status.cpu_percent,
                "memory_percent": status.memory_percent,
                "active_tasks": status.active_task_count,
                "health": status.health_status.value,
            },
            "tasks": {
                "total": total_tasks,
                "pending": pending_tasks,
                "running": running_tasks,
                "completed": completed_tasks,
                "failed": failed_tasks,
                "success_rate": success_rate,
                "avg_execution_time": avg_exec_time,
            },
            "peers": {
                "total": len(peers),
                "active": len(active_peers),
                "unresponsive": len(unresponsive_peers),
                "stale": status.stale_peers_count,
                "expired": status.expired_peers_count,
                "active_connections": status.active_connections_count,
            },
            "peer_loads": peer_loads,
            "timestamp": time.time(),
        }

    @app.get("/api/events")
    async def list_events(
        limit: int = Query(100, ge=1, le=500),
        event_type: str | None = None,
        severity: str | None = None,
        peer_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Query chronological events stream from the event manager."""
        events_list = events.get_events(
            limit=limit,
            event_type=event_type,
            severity=severity,
            peer_id=peer_id,
        )
        return [e.to_dict() for e in events_list]

    # ---------------------------------------------------------
    # WebSocket Live Feed
    # ---------------------------------------------------------

    @app.websocket("/ws/events")
    async def websocket_events(websocket: WebSocket) -> None:
        """
        Live WebSocket stream broadcasting network events and periodic status heartbeats.
        """
        await websocket.accept()
        queue = events.subscribe()

        # Send initial connection greeting with current summary
        status = node.get_status()
        await websocket.send_json({
            "type": "connected",
            "node_id": node.peer_id,
            "peers_count": status.total_registered_peers,
            "timestamp": time.time(),
        })

        async def _heartbeat_sender() -> None:
            """Push lightweight periodic status snapshot every 2.5s."""
            while True:
                try:
                    await asyncio.sleep(2.5)
                    st = node.get_status()
                    await websocket.send_json({
                        "type": "heartbeat",
                        "node_id": node.peer_id,
                        "active_peers": st.active_peers_count,
                        "total_peers": st.total_registered_peers,
                        "active_tasks": st.active_task_count,
                        "timestamp": time.time(),
                    })
                except (WebSocketDisconnect, asyncio.CancelledError):
                    break
                except Exception:
                    break

        hb_task = asyncio.create_task(_heartbeat_sender())

        try:
            while True:
                event = await queue.get()
                await websocket.send_json({
                    "type": "event",
                    "event": event.to_dict(),
                })
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        finally:
            hb_task.cancel()
            events.unsubscribe(queue)

    # ---------------------------------------------------------
    # Static SPA Frontend Mount
    # ---------------------------------------------------------

    resolved_dist = Path(static_dist_dir) if static_dist_dir else Path(__file__).resolve().parent.parent.parent / "web" / "dist"
    if resolved_dist.is_dir():
        logger.info("Serving dashboard static UI from %s", resolved_dist)
        app.mount("/", StaticFiles(directory=str(resolved_dist), html=True), name="static")

    return app

"""
Live terminal monitoring dashboard for MeshWeaver peer nodes.

Provides an optional Rich-based interactive terminal UI that displays:
- Local node identity, bound interface, role, and uptime
- Discovered mesh peers, health states, and resource reports (CPU/RAM)
- Running tasks and active execution states
- Recent task results and failures
- Plain-text ASCII fallback when Rich is unavailable or disabled.

Separate from core networking; imports no private transport internals.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import logging
import sys
import time
from typing import Any

try:
    from meshweaver import __version__
except ImportError:
    __version__ = "0.1.0"

logger = logging.getLogger("meshweaver.dashboard")

# Optional Rich integration with clean fallback detection
try:
    import rich
    from rich.align import Align
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    Console = None  # type: ignore[assignment, misc]
    Group = None  # type: ignore[assignment, misc]
    Layout = None  # type: ignore[assignment, misc]
    Live = None  # type: ignore[assignment, misc]
    Panel = None  # type: ignore[assignment, misc]
    Table = None  # type: ignore[assignment, misc]
    Text = None  # type: ignore[assignment, misc]
    Align = None  # type: ignore[assignment, misc]


@dataclass
class NodeDashboardSummary:
    """Summary of the local MeshWeaver peer node."""

    peer_id: str
    host: str
    port: int
    bound_port: int
    is_running: bool
    uptime_seconds: float
    active_connections: int
    active_peers: int
    total_peers: int
    cpu_percent: float | None = None
    memory_percent: float | None = None
    gossip_reports_count: int = 0
    tls_enabled: bool = False
    auth_enabled: bool = False
    supported_tasks: list[str] = field(default_factory=list)

    @property
    def uptime_formatted(self) -> str:
        """Format uptime into human-readable duration."""
        sec = int(self.uptime_seconds)
        hours, remainder = divmod(sec, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes:02d}m {seconds:02d}s"
        if minutes > 0:
            return f"{minutes}m {seconds:02d}s"
        return f"{seconds}s"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PeerDashboardItem:
    """Status and resource snapshot for a single discovered mesh peer."""

    peer_id: str
    endpoint: str
    is_connected: bool
    health_status: str  # ACTIVE, DEGRADED, UNRESPONSIVE, UNKNOWN
    peer_state: str  # active, stale, expired
    cpu_percent: float | None = None
    memory_percent: float | None = None
    load_score: float | None = None
    last_seen_seconds_ago: float | None = None
    missed_heartbeats: int = 0
    supported_tasks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class TaskDashboardItem:
    """Representation of an active or recent task in the dashboard."""

    task_id: str
    task_name: str
    status: str  # PENDING, RUNNING, SUCCESS, FAILURE, ERROR, CANCELLED
    worker_id: str | None = None
    duration_ms: float | None = None
    result_preview: str | None = None
    error_code: str | None = None
    error_message: str | None = None
    submitted_at: float | None = None
    is_failure: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class DashboardSnapshot:
    """Unified snapshot of all live data displayed in the CLI dashboard."""

    node: NodeDashboardSummary
    peers: list[PeerDashboardItem] = field(default_factory=list)
    running_tasks: list[TaskDashboardItem] = field(default_factory=list)
    recent_tasks: list[TaskDashboardItem] = field(default_factory=list)
    failures: list[TaskDashboardItem] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "node": self.node.to_dict(),
            "peers": [p.to_dict() for p in self.peers],
            "running_tasks": [t.to_dict() for t in self.running_tasks],
            "recent_tasks": [t.to_dict() for t in self.recent_tasks],
            "failures": [t.to_dict() for t in self.failures],
            "timestamp": self.timestamp,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)


def generate_dashboard_snapshot(node: Any, max_recent_tasks: int = 20) -> DashboardSnapshot:
    """
    Extract a live, genuine data snapshot from a running or stopped PeerNode.
    Does not invent fake records or synthetic metrics.
    """
    now = time.time()

    # 1. Local Node Summary
    node_id = getattr(node, "peer_id", "unknown-node")
    host = getattr(node, "host", "127.0.0.1")
    port = getattr(node, "port", 0)
    bound_port = getattr(node, "bound_port", port)
    is_running = getattr(node, "is_running", False)

    started_at = getattr(node, "started_at", None)
    if started_at is None and hasattr(node, "_started_at"):
        started_at = getattr(node, "_started_at")
    uptime = max(0.0, now - started_at) if (is_running and started_at is not None) else 0.0

    # Peer counts and connections
    active_conns = len(getattr(node, "_connections", {}))
    peer_reg = getattr(node, "peer_registry", None)
    if peer_reg is not None:
        total_peers = len(peer_reg.list_peers(include_expired=True, current_time=now))
        active_peers = len(peer_reg.list_active_peers(current_time=now))
    else:
        total_peers = 0
        active_peers = 0

    # Gossip reports
    gossip_engine = getattr(node, "gossip_engine", None)
    gossip_reports_count = 0
    if gossip_engine is not None and hasattr(gossip_engine, "store"):
        try:
            gossip_reports_count = len(gossip_engine.store.list_reports())
        except Exception:
            pass

    # Security flags
    sec_mgr = getattr(node, "security_manager", None)
    tls_enabled = getattr(sec_mgr, "is_tls_enabled", False) if sec_mgr else False
    auth_enabled = getattr(sec_mgr, "is_auth_enabled", False) if sec_mgr else False

    # Supported task functions
    task_reg = getattr(node, "task_registry", None)
    supported_tasks = list(task_reg.list_tasks()) if task_reg is not None else []

    # Local resource metrics
    local_cpu: float | None = None
    local_mem: float | None = None
    res_collector = getattr(node, "resource_collector", None)
    if res_collector is not None and hasattr(res_collector, "get_latest_report"):
        rep = res_collector.get_latest_report()
        if rep is not None:
            local_cpu = rep.cpu_percent
            local_mem = rep.memory_percent

    node_summary = NodeDashboardSummary(
        peer_id=node_id,
        host=host,
        port=port,
        bound_port=bound_port,
        is_running=is_running,
        uptime_seconds=uptime,
        active_connections=active_conns,
        active_peers=active_peers,
        total_peers=total_peers,
        cpu_percent=local_cpu,
        memory_percent=local_mem,
        gossip_reports_count=gossip_reports_count,
        tls_enabled=tls_enabled,
        auth_enabled=auth_enabled,
        supported_tasks=supported_tasks,
    )

    # 2. Discovered Peers
    peers_list: list[PeerDashboardItem] = []
    if peer_reg is not None:
        for p in peer_reg.list_peers(include_expired=True, current_time=now):
            peer_endpoint = f"{p.host}:{p.port}"
            is_connected = p.peer_id in getattr(node, "_peer_connections", {})

            # Health state from heartbeat monitor or fallback metadata
            health_str = "UNKNOWN"
            missed_beats = 0
            hb_monitor = getattr(node, "heartbeat_monitor", None)
            if hb_monitor is not None:
                try:
                    h_stat = hb_monitor.get_health_status(p.peer_id)
                    health_str = (
                        h_stat.value.upper()
                        if hasattr(h_stat, "value")
                        else str(h_stat).upper()
                    )
                    missed_beats = hb_monitor.get_missed_heartbeats(p.peer_id)
                except Exception:
                    pass

            if health_str == "UNKNOWN":
                meta_health = p.metadata.get("health_status")
                if meta_health:
                    health_str = str(meta_health).upper()

            # Lifecycle state (active, stale, expired)
            try:
                state_str = (
                    p.state(current_time=now).value
                    if callable(p.state)
                    else str(p.state)
                )
            except Exception:
                state_str = "unknown"

            # Resource report from gossip
            p_cpu: float | None = None
            p_mem: float | None = None
            if gossip_engine is not None and hasattr(gossip_engine, "store"):
                g_rep = gossip_engine.store.get(p.peer_id)
                if g_rep is not None:
                    p_cpu = g_rep.cpu_percent
                    p_mem = g_rep.memory_percent

            if p_cpu is None and "cpu_percent" in p.metadata:
                try:
                    p_cpu = float(p.metadata["cpu_percent"])
                except Exception:
                    pass
            if p_mem is None and "memory_percent" in p.metadata:
                try:
                    p_mem = float(p.metadata["memory_percent"])
                except Exception:
                    pass

            # Load score from scheduler
            load_score: float | None = None
            scheduler = getattr(node, "scheduler", None)
            if scheduler is not None:
                try:
                    evaluation = scheduler.evaluate_worker(p, now=now)
                    load_score = evaluation.score
                except Exception:
                    pass

            last_seen_ago = max(0.0, now - p.last_seen) if p.last_seen else None
            p_tasks = list(p.metadata.get("supported_tasks", []))

            peers_list.append(
                PeerDashboardItem(
                    peer_id=p.peer_id,
                    endpoint=peer_endpoint,
                    is_connected=is_connected,
                    health_status=health_str,
                    peer_state=state_str,
                    cpu_percent=p_cpu,
                    memory_percent=p_mem,
                    load_score=load_score,
                    last_seen_seconds_ago=last_seen_ago,
                    missed_heartbeats=missed_beats,
                    supported_tasks=p_tasks,
                )
            )

    # 3. Tasks Extraction
    all_tasks: dict[str, TaskDashboardItem] = {}

    # 3a. Tasks from TaskExecutionEngine
    exec_engine = getattr(node, "execution_engine", None)
    if exec_engine is not None and hasattr(exec_engine, "list_tasks"):
        try:
            for rec in exec_engine.list_tasks():
                t_id = rec.task_id
                t_name = rec.task_name
                state_val = (
                    rec.state.value.upper()
                    if hasattr(rec.state, "value")
                    else str(rec.state).upper()
                )
                dur_ms = (
                    round(rec.execution_time * 1000.0, 2)
                    if rec.execution_time is not None
                    else None
                )
                res_prev = None
                if rec.result is not None:
                    res_prev = str(rec.result)
                    if len(res_prev) > 60:
                        res_prev = res_prev[:57] + "..."

                err_code = None
                err_msg = None
                is_fail = state_val in ("FAILED", "FAILURE", "ERROR")
                if rec.error is not None:
                    is_fail = True
                    err_code = getattr(rec.error, "code", None)
                    err_msg = getattr(rec.error, "message", str(rec.error))
                elif rec.exception is not None:
                    is_fail = True
                    err_code = type(rec.exception).__name__
                    err_msg = str(rec.exception)

                all_tasks[t_id] = TaskDashboardItem(
                    task_id=t_id,
                    task_name=t_name,
                    status=state_val,
                    worker_id=node_id,
                    duration_ms=dur_ms,
                    result_preview=res_prev,
                    error_code=err_code,
                    error_message=err_msg,
                    submitted_at=rec.created_at,
                    is_failure=is_fail,
                )
        except Exception as exc:
            logger.debug("Error querying execution engine tasks: %s", exc)

    # 3b. Tasks from TaskWorker
    worker = getattr(node, "worker", None)
    if worker is not None and hasattr(worker, "list_tasks"):
        try:
            for wrec in worker.list_tasks():
                t_id = wrec.task_id
                if t_id in all_tasks:
                    continue
                t_name = getattr(wrec, "task_name", "task")
                w_state = (
                    wrec.state.value.upper()
                    if hasattr(wrec.state, "value")
                    else str(wrec.state).upper()
                )
                dur_ms = (
                    round(wrec.duration * 1000.0, 2)
                    if getattr(wrec, "duration", None) is not None
                    else None
                )
                res_prev = str(wrec.result)[:57] + "..." if getattr(wrec, "result", None) is not None and len(str(wrec.result)) > 60 else (str(wrec.result) if getattr(wrec, "result", None) is not None else None)
                err_code = getattr(wrec.error, "code", None) if getattr(wrec, "error", None) else None
                err_msg = getattr(wrec.error, "message", None) if getattr(wrec, "error", None) else None
                is_fail = w_state in ("FAILED", "FAILURE", "ERROR") or bool(err_msg)

                all_tasks[t_id] = TaskDashboardItem(
                    task_id=t_id,
                    task_name=t_name,
                    status=w_state,
                    worker_id=node_id,
                    duration_ms=dur_ms,
                    result_preview=res_prev,
                    error_code=err_code,
                    error_message=err_msg,
                    submitted_at=getattr(wrec, "submitted_at", now),
                    is_failure=is_fail,
                )
        except Exception as exc:
            logger.debug("Error querying worker tasks: %s", exc)

    # 3c. Tasks from Deduplication Store (submitted / routed tasks)
    dedup = getattr(node, "_task_dedup", None)
    if dedup is not None and hasattr(dedup, "_records"):
        try:
            for d_rec in dedup._records.values():
                t_id = d_rec.get("task_id")
                if not t_id:
                    continue
                routed_to = d_rec.get("routed_to")
                resp = d_rec.get("response")
                stat_obj = d_rec.get("status")
                stat_val = (
                    stat_obj.value.upper()
                    if hasattr(stat_obj, "value")
                    else str(stat_obj).upper()
                )

                dur_ms = None
                res_prev = None
                err_code = None
                err_msg = None
                is_fail = stat_val in ("FAILED", "FAILURE", "ERROR")

                if resp is not None:
                    if hasattr(resp, "execution_time") and resp.execution_time is not None:
                        dur_ms = round(resp.execution_time * 1000.0, 2)
                    if hasattr(resp, "result") and resp.result is not None:
                        res_str = str(resp.result)
                        res_prev = res_str[:57] + "..." if len(res_str) > 60 else res_str
                    if hasattr(resp, "error") and resp.error is not None:
                        is_fail = True
                        err_code = getattr(resp.error, "code", None)
                        err_msg = getattr(resp.error, "message", str(resp.error))

                existing = all_tasks.get(t_id)
                if existing is not None:
                    if routed_to and not existing.worker_id:
                        existing.worker_id = routed_to
                    if dur_ms is not None and existing.duration_ms is None:
                        existing.duration_ms = dur_ms
                    if res_prev and not existing.result_preview:
                        existing.result_preview = res_prev
                    if err_msg and not existing.error_message:
                        existing.error_message = err_msg
                        existing.error_code = err_code
                        existing.is_failure = True
                else:
                    all_tasks[t_id] = TaskDashboardItem(
                        task_id=t_id,
                        task_name=d_rec.get("task_name", "task"),
                        status=stat_val,
                        worker_id=routed_to,
                        duration_ms=dur_ms,
                        result_preview=res_prev,
                        error_code=err_code,
                        error_message=err_msg,
                        submitted_at=d_rec.get("timestamp", now),
                        is_failure=is_fail,
                    )
        except Exception as exc:
            logger.debug("Error querying deduplication store tasks: %s", exc)

    # Categorize tasks
    task_items = list(all_tasks.values())
    task_items.sort(key=lambda t: t.submitted_at or 0.0, reverse=True)

    running_tasks = [t for t in task_items if t.status in ("RUNNING", "PENDING")]
    recent_tasks = [
        t
        for t in task_items
        if t.status in ("SUCCESS", "COMPLETED", "FAILED", "FAILURE", "ERROR", "CANCELLED")
    ][:max_recent_tasks]
    failures = [t for t in task_items if t.is_failure][:max_recent_tasks]

    return DashboardSnapshot(
        node=node_summary,
        peers=peers_list,
        running_tasks=running_tasks,
        recent_tasks=recent_tasks,
        failures=failures,
        timestamp=now,
    )


# =============================================================================
# Rich UI Rendering Components
# =============================================================================


def build_peers_table_rich(peers: list[PeerDashboardItem]) -> Table:
    """Construct a styled Rich Table for discovered peers."""
    table = Table(
        title="[bold blue]Discovered Mesh Peers & Metrics[/bold blue]",
        expand=True,
        header_style="bold cyan",
        border_style="blue",
        show_lines=False,
    )
    table.add_column("Peer ID", style="bold yellow", no_wrap=True)
    table.add_column("Endpoint", style="dim", no_wrap=True)
    table.add_column("Connection", justify="center")
    table.add_column("Health", justify="center")
    table.add_column("CPU %", justify="right")
    table.add_column("RAM %", justify="right")
    table.add_column("Load Score", justify="right")
    table.add_column("Last Seen", justify="right")

    if not peers:
        table.add_row(
            "[dim]No peers discovered[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
        )
        return table

    for p in peers:
        conn_str = "[bold green]Connected[/bold green]" if p.is_connected else "[dim]Known[/dim]"

        # Color-coded health badges
        h_upper = p.health_status.upper()
        if h_upper in ("ACTIVE", "HEALTHY"):
            health_badge = "[bold green]ACTIVE[/bold green]"
        elif h_upper in ("DEGRADED", "WARNING"):
            health_badge = "[bold yellow]DEGRADED[/bold yellow]"
        elif h_upper in ("UNRESPONSIVE", "UNHEALTHY", "DEAD", "FAILED"):
            health_badge = "[bold red]UNRESPONSIVE[/bold red]"
        else:
            health_badge = f"[dim]{h_upper}[/dim]"

        cpu_str = f"{p.cpu_percent:.1f}%" if p.cpu_percent is not None else "[dim]N/A[/dim]"
        ram_str = f"{p.memory_percent:.1f}%" if p.memory_percent is not None else "[dim]N/A[/dim]"
        load_str = f"{p.load_score:.1f}" if p.load_score is not None else "[dim]N/A[/dim]"
        seen_str = (
            f"{p.last_seen_seconds_ago:.1f}s ago"
            if p.last_seen_seconds_ago is not None
            else "[dim]N/A[/dim]"
        )

        table.add_row(
            p.peer_id,
            p.endpoint,
            conn_str,
            health_badge,
            cpu_str,
            ram_str,
            load_str,
            seen_str,
        )

    return table


def build_running_tasks_table_rich(tasks: list[TaskDashboardItem]) -> Table:
    """Construct a styled Rich Table for currently active tasks."""
    table = Table(
        title="[bold yellow]Active Running Tasks[/bold yellow]",
        expand=True,
        header_style="bold yellow",
        border_style="yellow",
        show_lines=False,
    )
    table.add_column("Task ID", style="bold cyan", no_wrap=True)
    table.add_column("Task Name", style="white")
    table.add_column("Worker / Peer", style="magenta")
    table.add_column("Status", justify="center")
    table.add_column("Elapsed", justify="right")

    if not tasks:
        table.add_row(
            "[dim]No active running tasks[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
        )
        return table

    now = time.time()
    for t in tasks:
        elapsed = f"{max(0.0, now - t.submitted_at):.2f}s" if t.submitted_at else "N/A"
        st_badge = "[yellow]RUNNING[/yellow]" if t.status == "RUNNING" else "[cyan]PENDING[/cyan]"
        table.add_row(
            t.task_id[:16] + "..." if len(t.task_id) > 16 else t.task_id,
            t.task_name,
            t.worker_id or "[dim]Unassigned[/dim]",
            st_badge,
            elapsed,
        )

    return table


def build_recent_tasks_table_rich(tasks: list[TaskDashboardItem]) -> Table:
    """Construct a styled Rich Table for completed task outcomes and failures."""
    table = Table(
        title="[bold green]Recent Task Results & Failures[/bold green]",
        expand=True,
        header_style="bold green",
        border_style="green",
        show_lines=False,
    )
    table.add_column("Task ID", style="dim", no_wrap=True)
    table.add_column("Task Name", style="white")
    table.add_column("Status", justify="center")
    table.add_column("Duration", justify="right")
    table.add_column("Worker", style="magenta")
    table.add_column("Outcome / Details", style="white")

    if not tasks:
        table.add_row(
            "[dim]No recent task activity[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
            "[dim]-[/dim]",
        )
        return table

    for t in tasks:
        if t.is_failure or t.status in ("FAILED", "FAILURE", "ERROR"):
            st_badge = "[bold red]FAILED[/bold red]"
            detail = f"[red]{t.error_code or 'ERROR'}: {t.error_message or 'Unknown error'}[/red]"
        elif t.status in ("SUCCESS", "COMPLETED"):
            st_badge = "[bold green]SUCCESS[/bold green]"
            detail = f"[green]Result: {t.result_preview or 'None'}[/green]"
        else:
            st_badge = f"[yellow]{t.status}[/yellow]"
            detail = t.result_preview or t.error_message or ""

        dur_str = f"{t.duration_ms:.1f}ms" if t.duration_ms is not None else "[dim]N/A[/dim]"
        t_id_disp = t.task_id[:12] + "..." if len(t.task_id) > 12 else t.task_id

        table.add_row(
            t_id_disp,
            t.task_name,
            st_badge,
            dur_str,
            t.worker_id or "[dim]local[/dim]",
            detail,
        )

    return table


def render_rich_dashboard(snapshot: DashboardSnapshot) -> Any:
    """
    Render a complete, attractive Rich dashboard layout.
    """
    if not HAS_RICH:
        return render_plain_text_dashboard(snapshot)

    n = snapshot.node

    # Header and local status banner
    status_badge = "[bold green]ONLINE[/bold green]" if n.is_running else "[bold red]STOPPED[/bold red]"
    tls_badge = "[bold green]ENABLED[/bold green]" if n.tls_enabled else "[dim]DISABLED[/dim]"
    auth_badge = "[bold green]ENABLED[/bold green]" if n.auth_enabled else "[dim]DISABLED[/dim]"

    cpu_disp = f"{n.cpu_percent:.1f}%" if n.cpu_percent is not None else "N/A"
    mem_disp = f"{n.memory_percent:.1f}%" if n.memory_percent is not None else "N/A"

    tasks_reg_str = ", ".join(n.supported_tasks) if n.supported_tasks else "None"

    header_text = Text()
    header_text.append(f"Node ID: ", style="bold white")
    header_text.append(f"{n.peer_id}  ", style="bold yellow")
    header_text.append(f"Interface: ", style="bold white")
    header_text.append(f"{n.host}:{n.bound_port}  ", style="bold green")
    header_text.append(f"Status: ", style="bold white")
    header_text.append(f"{status_badge}  ")
    header_text.append(f"Uptime: ", style="bold white")
    header_text.append(f"{n.uptime_formatted}\n", style="bold cyan")

    header_text.append(f"Mesh Peers: ", style="bold white")
    header_text.append(f"{n.active_peers} active / {n.total_peers} total  ", style="cyan")
    header_text.append(f"Connections: ", style="bold white")
    header_text.append(f"{n.active_connections}  ", style="cyan")
    header_text.append(f"Gossip Reports: ", style="bold white")
    header_text.append(f"{n.gossip_reports_count}  ", style="cyan")
    header_text.append(f"Local Resources: ", style="bold white")
    header_text.append(f"CPU: {cpu_disp} | RAM: {mem_disp}\n", style="magenta")

    header_text.append(f"Security: ", style="bold white")
    header_text.append(f"TLS: {tls_badge} | Peer Auth: {auth_badge}  ", style="white")
    header_text.append(f"Registered Tasks: ", style="bold white")
    header_text.append(f"{tasks_reg_str}", style="dim")

    header_panel = Panel(
        header_text,
        title=f"[bold cyan]MeshWeaver P2P Task Broker[/bold cyan] [dim](v{__version__})[/dim]",
        border_style="cyan",
        expand=True,
    )

    peers_table = build_peers_table_rich(snapshot.peers)
    running_table = build_running_tasks_table_rich(snapshot.running_tasks)
    recent_table = build_recent_tasks_table_rich(snapshot.recent_tasks)

    return Group(header_panel, peers_table, running_table, recent_table)


# =============================================================================
# Plain-Text Fallback Rendering Components
# =============================================================================


def render_plain_text_dashboard(snapshot: DashboardSnapshot) -> str:
    """
    Render a clean ASCII plain-text monitoring view.
    Zero external dependencies, safe for non-TTY / pipe execution.
    """
    n = snapshot.node
    lines: list[str] = []

    divider = "=" * 80
    subdivider = "-" * 80

    lines.append(divider)
    lines.append(f"  MeshWeaver P2P Task Broker (v{__version__}) - Terminal Monitoring Dashboard")
    lines.append(divider)

    st_str = "ONLINE" if n.is_running else "STOPPED"
    lines.append(f"Node ID:       {n.peer_id} ({n.host}:{n.bound_port}) [{st_str}]")
    lines.append(f"Uptime:        {n.uptime_formatted}")
    lines.append(
        f"Cluster:       {n.active_peers} active / {n.total_peers} total peers | "
        f"{n.active_connections} connections | {n.gossip_reports_count} gossip reports"
    )

    cpu_s = f"{n.cpu_percent:.1f}%" if n.cpu_percent is not None else "N/A"
    mem_s = f"{n.memory_percent:.1f}%" if n.memory_percent is not None else "N/A"
    lines.append(f"Local Usage:   CPU: {cpu_s} | RAM: {mem_s}")

    tls_s = "ENABLED" if n.tls_enabled else "DISABLED"
    auth_s = "ENABLED" if n.auth_enabled else "DISABLED"
    lines.append(f"Security:      TLS: {tls_s} | Auth: {auth_s}")

    tasks_str = ", ".join(n.supported_tasks) if n.supported_tasks else "None"
    lines.append(f"Allowlisted:   {tasks_str}")

    # Discovered Peers
    lines.append(subdivider)
    lines.append(f"DISCOVERED PEERS ({len(snapshot.peers)}):")
    if not snapshot.peers:
        lines.append("  (No peers discovered yet)")
    else:
        lines.append(
            f"  {'PEER ID':<18} {'ENDPOINT':<20} {'STATUS':<11} {'HEALTH':<13} "
            f"{'CPU %':<8} {'RAM %':<8} {'LOAD':<8} {'LAST SEEN'}"
        )
        for p in snapshot.peers:
            conn = "Connected" if p.is_connected else "Known"
            cpu = f"{p.cpu_percent:.1f}%" if p.cpu_percent is not None else "N/A"
            mem = f"{p.memory_percent:.1f}%" if p.memory_percent is not None else "N/A"
            load = f"{p.load_score:.1f}" if p.load_score is not None else "N/A"
            seen = f"{p.last_seen_seconds_ago:.1f}s ago" if p.last_seen_seconds_ago is not None else "N/A"
            lines.append(
                f"  {p.peer_id:<18} {p.endpoint:<20} {conn:<11} {p.health_status:<13} "
                f"{cpu:<8} {mem:<8} {load:<8} {seen}"
            )

    # Active Running Tasks
    lines.append(subdivider)
    lines.append(f"RUNNING TASKS ({len(snapshot.running_tasks)}):")
    if not snapshot.running_tasks:
        lines.append("  (No active running tasks)")
    else:
        lines.append(f"  {'TASK ID':<18} {'NAME':<16} {'WORKER':<16} {'STATUS':<10} {'ELAPSED'}")
        now = time.time()
        for t in snapshot.running_tasks:
            el = f"{max(0.0, now - t.submitted_at):.2f}s" if t.submitted_at else "N/A"
            lines.append(
                f"  {t.task_id[:16]:<18} {t.task_name:<16} {(t.worker_id or 'local'):<16} {t.status:<10} {el}"
            )

    # Recent Results & Failures
    lines.append(subdivider)
    lines.append(f"RECENT TASK RESULTS & FAILURES ({len(snapshot.recent_tasks)}):")
    if not snapshot.recent_tasks:
        lines.append("  (No recent task activity)")
    else:
        lines.append(
            f"  {'TASK ID':<14} {'NAME':<14} {'STATUS':<9} {'DURATION':<10} {'WORKER':<14} {'DETAILS'}"
        )
        for t in snapshot.recent_tasks:
            dur = f"{t.duration_ms:.1f}ms" if t.duration_ms is not None else "N/A"
            w_disp = t.worker_id or "local"
            if t.is_failure or t.status in ("FAILED", "FAILURE", "ERROR"):
                det = f"{t.error_code or 'ERROR'}: {t.error_message or ''}"
            else:
                det = f"Result: {t.result_preview or 'None'}"
            lines.append(
                f"  {t.task_id[:12]:<14} {t.task_name[:13]:<14} {t.status[:8]:<9} {dur:<10} {w_disp[:13]:<14} {det}"
            )

    lines.append(divider)
    return "\n".join(lines)


# =============================================================================
# CLI Subcommand Output Helpers (peers, tasks, status)
# =============================================================================


def format_peers_output(
    peers: list[PeerDashboardItem],
    *,
    as_json: bool = False,
    force_plain: bool = False,
) -> str | None:
    """Format a list of peers for CLI output (JSON, Rich table, or plain ASCII)."""
    if as_json:
        print(json.dumps([p.to_dict() for p in peers], indent=2))
        return None

    if HAS_RICH and not force_plain:
        c = Console()
        c.print(build_peers_table_rich(peers))
        return None

    # Plain text format
    lines = [
        f"{'PEER ID':<18} {'ENDPOINT':<20} {'STATUS':<11} {'HEALTH':<13} {'CPU %':<8} {'RAM %':<8} {'LOAD':<8} {'LAST SEEN'}",
        "-" * 95,
    ]
    if not peers:
        lines.append("(No discovered peers)")
    else:
        for p in peers:
            conn = "Connected" if p.is_connected else "Known"
            cpu = f"{p.cpu_percent:.1f}%" if p.cpu_percent is not None else "N/A"
            mem = f"{p.memory_percent:.1f}%" if p.memory_percent is not None else "N/A"
            load = f"{p.load_score:.1f}" if p.load_score is not None else "N/A"
            seen = f"{p.last_seen_seconds_ago:.1f}s ago" if p.last_seen_seconds_ago is not None else "N/A"
            lines.append(
                f"{p.peer_id:<18} {p.endpoint:<20} {conn:<11} {p.health_status:<13} {cpu:<8} {mem:<8} {load:<8} {seen}"
            )
    out = "\n".join(lines)
    print(out)
    return out


def format_tasks_output(
    tasks: list[TaskDashboardItem],
    *,
    as_json: bool = False,
    force_plain: bool = False,
) -> str | None:
    """Format a list of tasks for CLI output (JSON, Rich table, or plain ASCII)."""
    if as_json:
        print(json.dumps([t.to_dict() for t in tasks], indent=2))
        return None

    if HAS_RICH and not force_plain:
        c = Console()
        c.print(build_recent_tasks_table_rich(tasks))
        return None

    lines = [
        f"{'TASK ID':<16} {'NAME':<18} {'STATUS':<9} {'DURATION':<10} {'WORKER':<14} {'DETAILS'}",
        "-" * 90,
    ]
    if not tasks:
        lines.append("(No tasks recorded)")
    else:
        for t in tasks:
            dur = f"{t.duration_ms:.1f}ms" if t.duration_ms is not None else "N/A"
            w_disp = t.worker_id or "local"
            if t.is_failure or t.status in ("FAILED", "FAILURE", "ERROR"):
                det = f"{t.error_code or 'ERROR'}: {t.error_message or ''}"
            else:
                det = f"Result: {t.result_preview or 'None'}"
            lines.append(
                f"{t.task_id[:14]:<16} {t.task_name[:17]:<18} {t.status[:8]:<9} {dur:<10} {w_disp[:13]:<14} {det}"
            )
    out = "\n".join(lines)
    print(out)
    return out


# =============================================================================
# Interactive Live Dashboard Refresh Loop
# =============================================================================


async def run_live_dashboard(
    node: Any,
    refresh_rate: float = 1.0,
    duration: float | None = None,
    plain: bool = False,
    console: Any = None,
) -> None:
    """
    Run the live terminal monitoring dashboard until cancelled or duration expires.
    Supports clean Ctrl+C shutdown.
    """
    import asyncio

    rate = max(0.2, float(refresh_rate))
    start_time = time.time()
    use_rich = HAS_RICH and not plain

    if use_rich:
        cons = console if console is not None else Console()
        snapshot = generate_dashboard_snapshot(node)
        renderable = render_rich_dashboard(snapshot)

        with Live(
            renderable,
            console=cons,
            refresh_per_second=int(1.0 / rate) or 1,
            screen=False,
            transient=False,
        ) as live:
            while True:
                if duration is not None and (time.time() - start_time) >= duration:
                    break
                await asyncio.sleep(rate)
                snapshot = generate_dashboard_snapshot(node)
                live.update(render_rich_dashboard(snapshot))
    else:
        while True:
            snapshot = generate_dashboard_snapshot(node)
            print(render_plain_text_dashboard(snapshot))
            if duration is not None and (time.time() - start_time) >= duration:
                break
            await asyncio.sleep(rate)

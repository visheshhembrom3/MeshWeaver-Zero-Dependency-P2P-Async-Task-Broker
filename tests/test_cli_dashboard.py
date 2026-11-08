"""
Tests for the Rich-based CLI terminal monitoring dashboard,
dashboard data snapshot generation, plain-text fallback, and CLI commands.
"""

from __future__ import annotations

import asyncio
import json
import time
from unittest.mock import patch
import pytest

from meshweaver import (
    HAS_RICH,
    DashboardSnapshot,
    HealthStatus,
    NodeDashboardSummary,
    NodeResourceReport,
    PeerDashboardItem,
    PeerNode,
    PeerRecord,
    TaskDashboardItem,
    TaskErrorInfo,
    TaskRequest,
    TaskResponse,
    TaskState,
    TaskStatus,
    format_peers_output,
    format_tasks_output,
    generate_dashboard_snapshot,
    render_plain_text_dashboard,
    render_rich_dashboard,
)
import meshweaver.cli_dashboard as cli_dash
from meshweaver.__main__ import main, run_peers_cmd, run_tasks_cmd, run_monitor_cmd


# =============================================================================
# 1. Unit Tests for Dashboard Data Structures
# =============================================================================


def test_node_dashboard_summary_formatting() -> None:
    """Verify uptime formatting and dictionary conversions for NodeDashboardSummary."""
    summary = NodeDashboardSummary(
        peer_id="node-test",
        host="127.0.0.1",
        port=8001,
        bound_port=8001,
        is_running=True,
        uptime_seconds=3665.0,  # 1h 1m 5s
        active_connections=2,
        active_peers=3,
        total_peers=5,
        cpu_percent=12.5,
        memory_percent=45.0,
        gossip_reports_count=3,
        tls_enabled=True,
        auth_enabled=True,
        supported_tasks=["add", "ping"],
    )

    assert summary.uptime_formatted == "1h 01m 05s"
    data = summary.to_dict()
    assert data["peer_id"] == "node-test"
    assert data["tls_enabled"] is True
    assert data["auth_enabled"] is True

    # Short uptimes
    summary_short = NodeDashboardSummary(
        peer_id="n2",
        host="127.0.0.1",
        port=0,
        bound_port=0,
        is_running=True,
        uptime_seconds=42.0,
        active_connections=0,
        active_peers=0,
        total_peers=0,
    )
    assert summary_short.uptime_formatted == "42s"

    summary_min = NodeDashboardSummary(
        peer_id="n3",
        host="127.0.0.1",
        port=0,
        bound_port=0,
        is_running=True,
        uptime_seconds=125.0,
        active_connections=0,
        active_peers=0,
        total_peers=0,
    )
    assert summary_min.uptime_formatted == "2m 05s"


def test_peer_and_task_dashboard_items_to_dict() -> None:
    """Verify serialization of PeerDashboardItem and TaskDashboardItem."""
    peer_item = PeerDashboardItem(
        peer_id="worker-a",
        endpoint="127.0.0.1:8002",
        is_connected=True,
        health_status="ACTIVE",
        peer_state="active",
        cpu_percent=15.0,
        memory_percent=30.0,
        load_score=22.5,
        last_seen_seconds_ago=1.2,
        missed_heartbeats=0,
        supported_tasks=["add"],
    )
    p_dict = peer_item.to_dict()
    assert p_dict["peer_id"] == "worker-a"
    assert p_dict["is_connected"] is True
    assert p_dict["health_status"] == "ACTIVE"

    task_item = TaskDashboardItem(
        task_id="task-99",
        task_name="add",
        status="SUCCESS",
        worker_id="worker-a",
        duration_ms=14.2,
        result_preview="30",
        is_failure=False,
    )
    t_dict = task_item.to_dict()
    assert t_dict["task_id"] == "task-99"
    assert t_dict["status"] == "SUCCESS"
    assert t_dict["is_failure"] is False

    snapshot = DashboardSnapshot(
        node=NodeDashboardSummary(
            peer_id="local",
            host="127.0.0.1",
            port=0,
            bound_port=0,
            is_running=True,
            uptime_seconds=10.0,
            active_connections=1,
            active_peers=1,
            total_peers=1,
        ),
        peers=[peer_item],
        recent_tasks=[task_item],
    )
    s_dict = snapshot.to_dict()
    assert len(s_dict["peers"]) == 1
    assert len(s_dict["recent_tasks"]) == 1
    s_json = snapshot.to_json()
    assert "worker-a" in s_json
    assert "task-99" in s_json


# =============================================================================
# 2. Live Node Dashboard Snapshot Generation Tests
# =============================================================================


@pytest.mark.asyncio
async def test_generate_dashboard_snapshot_standalone_node() -> None:
    """Verify snapshot generated from a fresh standalone PeerNode."""
    async with PeerNode(peer_id="dash-node-1", host="127.0.0.1", port=0) as node:
        snapshot = generate_dashboard_snapshot(node)

        assert snapshot.node.peer_id == "dash-node-1"
        assert snapshot.node.is_running is True
        assert snapshot.node.bound_port == node.bound_port
        assert snapshot.node.uptime_seconds >= 0.0
        assert snapshot.node.total_peers == 0
        assert snapshot.node.active_peers == 0
        assert "add" in snapshot.node.supported_tasks
        assert "reverse_string" in snapshot.node.supported_tasks
        assert len(snapshot.peers) == 0
        assert len(snapshot.running_tasks) == 0
        assert len(snapshot.recent_tasks) == 0
        assert len(snapshot.failures) == 0


@pytest.mark.asyncio
async def test_generate_dashboard_snapshot_with_peers_and_gossip() -> None:
    """Verify snapshot includes registered peers, health states, and gossip metrics."""
    async with PeerNode(peer_id="coord-node", host="127.0.0.1", port=0) as coord:
        coord.enable_gossip()
        coord.enable_heartbeats()

        now = time.time()
        # Register two peers
        p1 = coord.peer_registry.register(
            "worker-active",
            "127.0.0.1",
            8002,
            metadata={"supported_tasks": ["add", "echo"]},
        )
        p2 = coord.peer_registry.register(
            "worker-degraded",
            "127.0.0.1",
            8003,
            metadata={"supported_tasks": ["ping"]},
        )

        # Inject gossip reports
        rep1 = NodeResourceReport(
            node_id="worker-active",
            timestamp=now,
            cpu_percent=20.0,
            memory_percent=35.0,
            active_task_count=1,
            health_status=HealthStatus.HEALTHY,
        )
        rep2 = NodeResourceReport(
            node_id="worker-degraded",
            timestamp=now,
            cpu_percent=85.0,
            memory_percent=70.0,
            active_task_count=4,
            health_status=HealthStatus.DEGRADED,
        )
        coord.gossip_engine.store.update(rep1)
        coord.gossip_engine.store.update(rep2)

        # Inject heartbeat status
        coord.heartbeat_monitor.record_peer_success("worker-active")
        coord.heartbeat_monitor.record_peer_failure("worker-degraded", "Missed probe")

        snapshot = generate_dashboard_snapshot(coord)

        assert snapshot.node.total_peers == 2
        assert len(snapshot.peers) == 2

        peer_map = {p.peer_id: p for p in snapshot.peers}
        assert "worker-active" in peer_map
        assert "worker-degraded" in peer_map

        w1 = peer_map["worker-active"]
        assert w1.endpoint == "127.0.0.1:8002"
        assert w1.cpu_percent == 20.0
        assert w1.memory_percent == 35.0
        assert w1.health_status in ("ACTIVE", "HEALTHY")
        assert w1.load_score is not None

        w2 = peer_map["worker-degraded"]
        assert w2.endpoint == "127.0.0.1:8003"
        assert w2.cpu_percent == 85.0
        assert w2.memory_percent == 70.0
        assert w2.health_status in ("DEGRADED", "UNRESPONSIVE", "WARNING")


@pytest.mark.asyncio
async def test_generate_dashboard_snapshot_with_tasks_and_failures() -> None:
    """Verify task tracking captures running tasks, successful results, and failures."""
    async with PeerNode(peer_id="exec-node", host="127.0.0.1", port=0) as node:
        # 1. Successful local task execution
        task_succ = await node.execution_engine.submit("add", 100, 200)
        await node.execution_engine.wait_for_task(task_succ.task_id)
        assert task_succ.state == TaskState.COMPLETED

        # 2. Failed task execution (registered task that raises exception)
        @node.register_task("faulty_task")
        def faulty_task() -> None:
            raise ValueError("Something went wrong")

        task_fail = await node.execution_engine.submit("faulty_task")
        await node.execution_engine.wait_for_task(task_fail.task_id)
        assert task_fail.state == TaskState.FAILED

        # 3. Simulate deduplication store records
        node._task_dedup.register_submission(
            task_id="dedup-task-1",
            correlation_id="corr-1",
            routed_to="remote-worker",
        )
        node._task_dedup.record_result(
            "dedup-task-1",
            TaskResponse(
                task_id="dedup-task-1",
                status=TaskStatus.SUCCESS,
                result="remote_result",
                execution_time=0.045,
            ),
        )

        node._task_dedup.register_submission(
            task_id="dedup-task-failed",
            correlation_id="corr-2",
            routed_to="remote-worker",
        )
        node._task_dedup.record_result(
            "dedup-task-failed",
            TaskResponse.error(
                task_id="dedup-task-failed",
                error=TaskErrorInfo(code="REMOTE_TIMEOUT", message="Worker timed out"),
            ),
        )

        snapshot = generate_dashboard_snapshot(node)

        # Check recent tasks contain both successful and failed tasks
        recent_ids = {t.task_id for t in snapshot.recent_tasks}
        assert task_succ.task_id in recent_ids
        assert task_fail.task_id in recent_ids
        assert "dedup-task-1" in recent_ids
        assert "dedup-task-failed" in recent_ids

        # Check failures list
        fail_ids = {t.task_id for t in snapshot.failures}
        assert task_fail.task_id in fail_ids
        assert "dedup-task-failed" in fail_ids
        assert task_succ.task_id not in fail_ids
        assert "dedup-task-1" not in fail_ids


# =============================================================================
# 3. Dashboard Plain-Text & Rich Rendering Tests
# =============================================================================


def test_render_plain_text_dashboard() -> None:
    """Verify that plain-text ASCII dashboard formats all sections cleanly."""
    snapshot = DashboardSnapshot(
        node=NodeDashboardSummary(
            peer_id="node-alpha",
            host="127.0.0.1",
            port=8001,
            bound_port=8001,
            is_running=True,
            uptime_seconds=3600.0,
            active_connections=1,
            active_peers=2,
            total_peers=2,
            cpu_percent=15.0,
            memory_percent=40.0,
            gossip_reports_count=2,
            tls_enabled=True,
            auth_enabled=True,
            supported_tasks=["add", "ping"],
        ),
        peers=[
            PeerDashboardItem(
                peer_id="worker-1",
                endpoint="127.0.0.1:8002",
                is_connected=True,
                health_status="ACTIVE",
                peer_state="active",
                cpu_percent=10.0,
                memory_percent=20.0,
                load_score=15.0,
                last_seen_seconds_ago=1.0,
            )
        ],
        running_tasks=[
            TaskDashboardItem(
                task_id="task-run-1",
                task_name="add",
                status="RUNNING",
                worker_id="worker-1",
                submitted_at=time.time() - 2.0,
            )
        ],
        recent_tasks=[
            TaskDashboardItem(
                task_id="task-ok-1",
                task_name="add",
                status="SUCCESS",
                duration_ms=12.5,
                worker_id="worker-1",
                result_preview="42",
            ),
            TaskDashboardItem(
                task_id="task-err-1",
                task_name="div",
                status="FAILED",
                duration_ms=5.0,
                worker_id="worker-1",
                error_code="ZeroDivisionError",
                error_message="division by zero",
                is_failure=True,
            ),
        ],
        failures=[
            TaskDashboardItem(
                task_id="task-err-1",
                task_name="div",
                status="FAILED",
                duration_ms=5.0,
                worker_id="worker-1",
                error_code="ZeroDivisionError",
                error_message="division by zero",
                is_failure=True,
            )
        ],
    )

    text = render_plain_text_dashboard(snapshot)

    assert "MeshWeaver P2P Task Broker" in text
    assert "node-alpha" in text
    assert "1h 00m 00s" in text
    assert "TLS: ENABLED" in text
    assert "worker-1" in text
    assert "ACTIVE" in text
    assert "task-run-1" in text
    assert "RUNNING" in text
    assert "task-ok-1" in text
    assert "SUCCESS" in text
    assert "Result: 42" in text
    assert "ZeroDivisionError" in text


def test_render_rich_dashboard_when_installed() -> None:
    """Verify that Rich renderable group is created when Rich is installed."""
    snapshot = DashboardSnapshot(
        node=NodeDashboardSummary(
            peer_id="node-rich",
            host="127.0.0.1",
            port=8001,
            bound_port=8001,
            is_running=True,
            uptime_seconds=60.0,
            active_connections=0,
            active_peers=0,
            total_peers=0,
        ),
    )

    res = render_rich_dashboard(snapshot)
    if HAS_RICH:
        assert type(res).__name__ == "Group"
    else:
        assert isinstance(res, str)


def test_rich_fallback_when_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify that dashboard seamlessly falls back to plain-text when HAS_RICH is False."""
    monkeypatch.setattr(cli_dash, "HAS_RICH", False)

    snapshot = DashboardSnapshot(
        node=NodeDashboardSummary(
            peer_id="node-fallback",
            host="127.0.0.1",
            port=0,
            bound_port=0,
            is_running=True,
            uptime_seconds=5.0,
            active_connections=0,
            active_peers=0,
            total_peers=0,
        ),
    )

    rendered = cli_dash.render_rich_dashboard(snapshot)
    assert isinstance(rendered, str)
    assert "node-fallback" in rendered


def test_format_peers_output_plain_and_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify format_peers_output for plain-text and JSON options."""
    peers = [
        PeerDashboardItem(
            peer_id="p-1",
            endpoint="127.0.0.1:9001",
            is_connected=True,
            health_status="ACTIVE",
            peer_state="active",
        )
    ]

    # Plain text
    format_peers_output(peers, as_json=False, force_plain=True)
    captured = capsys.readouterr().out
    assert "p-1" in captured
    assert "127.0.0.1:9001" in captured

    # JSON output
    format_peers_output(peers, as_json=True)
    captured_json = capsys.readouterr().out
    parsed = json.loads(captured_json)
    assert isinstance(parsed, list)
    assert parsed[0]["peer_id"] == "p-1"


def test_format_tasks_output_plain_and_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify format_tasks_output for plain-text and JSON options."""
    tasks = [
        TaskDashboardItem(
            task_id="task-abc",
            task_name="reverse_string",
            status="SUCCESS",
            duration_ms=8.5,
            result_preview="olleh",
        )
    ]

    # Plain text
    format_tasks_output(tasks, as_json=False, force_plain=True)
    captured = capsys.readouterr().out
    assert "task-abc" in captured
    assert "reverse_string" in captured

    # JSON output
    format_tasks_output(tasks, as_json=True)
    captured_json = capsys.readouterr().out
    parsed = json.loads(captured_json)
    assert isinstance(parsed, list)
    assert parsed[0]["task_id"] == "task-abc"


# =============================================================================
# 4. CLI Subcommand Integration Tests
# =============================================================================


def test_cli_subcommand_peers_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify `meshweaver peers --json` outputs valid JSON."""
    exit_code = main(["peers", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert captured.strip().startswith("[")
    data = json.loads(captured)
    assert isinstance(data, list)


def test_cli_subcommand_tasks_json(capsys: pytest.CaptureFixture[str]) -> None:
    """Verify `meshweaver tasks --json` outputs valid JSON."""
    exit_code = main(["tasks", "--json"])
    assert exit_code == 0
    captured = capsys.readouterr().out
    assert captured.strip().startswith("[")
    data = json.loads(captured)
    assert isinstance(data, list)


def test_cli_subcommand_monitor_duration() -> None:
    """Verify `meshweaver monitor --duration 0.2 --plain` runs and exits cleanly."""
    exit_code = main(["monitor", "--duration", "0.2", "--plain"])
    assert exit_code == 0


def test_cli_subcommand_start_with_dashboard_flag() -> None:
    """Verify `meshweaver start --dashboard --duration 0.2 --plain` runs cleanly."""
    exit_code = main(["start", "--dashboard", "--duration", "0.2", "--plain"])
    assert exit_code == 0


@pytest.mark.asyncio
async def test_cli_submit_and_query_task() -> None:
    """Verify starting a peer node and executing `submit` and `tasks` against it."""
    async with PeerNode(peer_id="cli-target-worker", host="127.0.0.1", port=0) as worker:
        seed_addr = f"{worker.host}:{worker.bound_port}"

        # 1. Run submit command: add 55 45
        ret_submit = await asyncio.to_thread(
            main,
            ["submit", "add", "55", "45", "--seeds", seed_addr],
        )
        assert ret_submit == 0

        # 2. Run peers command against worker
        ret_peers = await asyncio.to_thread(
            main,
            ["peers", "--seeds", seed_addr, "--plain"],
        )
        assert ret_peers == 0


@pytest.mark.asyncio
async def test_graceful_shutdown_signal_handling() -> None:
    """Verify graceful shutdown handling when CancelledError is raised in run_monitor_cmd."""
    with patch("meshweaver.cli_dashboard.run_live_dashboard", side_effect=asyncio.CancelledError):
        ret = await run_monitor_cmd(duration=1.0)
        assert ret == 0

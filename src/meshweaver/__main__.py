"""
MeshWeaver package entry point and CLI commands.

Allows running the package directly via `python -m meshweaver`:
- `python -m meshweaver --demo`: Run the two-peer communication demo.
- `python -m meshweaver status`: Inspect current node and mesh peer status.
- `python -m meshweaver status --json`: Output node and peer information as JSON.
- `python -m meshweaver status --seeds 127.0.0.1:8001`: Query mesh status via bootstrap seeds.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import time

from meshweaver import __version__, ConfigurationError
from meshweaver.config import NodeConfig, resolve_node_config

logger = logging.getLogger("meshweaver.cli")


def build_security_config_from_args(args: Any) -> Any:
    """Build SecurityConfig from parsed CLI arguments."""
    from meshweaver.security import SecurityConfig, TLSConfig

    cert_file = getattr(args, "cert_file", None)
    key_file = getattr(args, "key_file", None)
    ca_file = getattr(args, "ca_file", None)
    tls_enabled = getattr(args, "tls", False) or bool(cert_file or ca_file)

    tls_cfg = TLSConfig(
        enabled=tls_enabled,
        certfile=cert_file,
        keyfile=key_file,
        cafile=ca_file,
        require_client_cert=getattr(args, "require_client_cert", False),
    )

    raw_peers = getattr(args, "allowed_peers", None)
    allowed = {p.strip() for p in raw_peers.split(",") if p.strip()} if raw_peers else None

    return SecurityConfig(
        tls=tls_cfg,
        shared_secret=getattr(args, "shared_secret", None),
        auth_token=getattr(args, "auth_token", None),
        allowed_peers=allowed,
    )


def add_security_arguments(p: argparse.ArgumentParser) -> None:
    """Add TLS, authentication, and integrity options to a command subparser."""
    p.add_argument("--tls", action="store_true", help="Enable TLS transport encryption")
    p.add_argument("--cert", dest="cert_file", default=None, help="Path to certificate PEM file")
    p.add_argument("--key", dest="key_file", default=None, help="Path to private key PEM file")
    p.add_argument("--ca", dest="ca_file", default=None, help="Path to CA certificate PEM file")
    p.add_argument("--require-client-cert", action="store_true", help="Require peer client certificate (mTLS)")
    p.add_argument("--shared-secret", dest="shared_secret", default=None, help="Pre-shared HMAC secret for message integrity")
    p.add_argument("--auth-token", dest="auth_token", default=None, help="Pre-shared peer authentication token")
    p.add_argument("--allowed-peers", dest="allowed_peers", default=None, help="Comma-separated allowlist of peer IDs")


async def run_status_cmd(
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    seeds: list[str] | None = None,
    as_json: bool = False,
    timeout: float = 3.0,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """
    Start a local peer node, optionally join seed nodes, and print status.
    """
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-node-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    await node.start()
    try:
        if seeds:
            await node.join_mesh(seeds, timeout=timeout)
            await asyncio.sleep(0.15)

        snapshot = node.get_status()
        node.status_api.log_status(as_json=as_json)

        if as_json:
            print(snapshot.to_json())
        else:
            print(snapshot.format_text())
        return 0
    finally:
        await node.stop()


async def run_start_cmd(
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    seeds: list[str] | None = None,
    duration: float | None = None,
    verbose: bool = False,
    as_json: bool = False,
    security_config: Any | None = None,
    show_dashboard: bool = False,
    refresh_rate: float = 1.0,
    force_plain: bool = False,
) -> int:
    """
    Start a MeshWeaver node that runs persistently, listens for tasks, and gossips.
    Optionally launches the interactive Rich/plain-text terminal dashboard.
    """
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"node-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    node.enable_heartbeats()
    await node.start()

    print(f"[MeshWeaver] Node '{node.peer_id}' started on {node.host}:{node.bound_port}")
    try:
        if seeds:
            print(f"[MeshWeaver] Joining mesh via seeds: {seeds}...")
            await node.join_mesh(seeds, timeout=5.0)
            print("[MeshWeaver] Successfully joined mesh via seeds.")

        if show_dashboard:
            from meshweaver.cli_dashboard import run_live_dashboard

            await run_live_dashboard(
                node,
                refresh_rate=refresh_rate,
                duration=duration,
                plain=force_plain,
            )
            return 0

        print(f"[MeshWeaver] Node '{node.peer_id}' is ready and accepting tasks. Press Ctrl+C to stop.")

        if duration is not None and duration > 0:
            await asyncio.sleep(duration)
        else:
            while True:
                await asyncio.sleep(1.0)
        return 0
    except (asyncio.CancelledError, KeyboardInterrupt):
        return 0
    finally:
        print(f"\n[MeshWeaver] Stopping node '{node.peer_id}' gracefully...")
        try:
            await node.leave_mesh()
        except Exception:
            pass
        await node.stop()
        print(f"[MeshWeaver] Node '{node.peer_id}' stopped gracefully.")


async def run_peers_cmd(
    seeds: list[str] | None = None,
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    as_json: bool = False,
    force_plain: bool = False,
    timeout: float = 3.0,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """Start local peer node, discover mesh peers via seeds, and display peers table."""
    from meshweaver.cli_dashboard import format_peers_output, generate_dashboard_snapshot
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-peers-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    node.enable_heartbeats()
    await node.start()
    try:
        if seeds:
            await node.join_mesh(seeds, timeout=timeout)
            await asyncio.sleep(0.2)

        snapshot = generate_dashboard_snapshot(node)
        format_peers_output(snapshot.peers, as_json=as_json, force_plain=force_plain)
        return 0
    finally:
        try:
            await node.leave_mesh()
        except Exception:
            pass
        await node.stop()


async def run_tasks_cmd(
    task_id: str | None = None,
    seeds: list[str] | None = None,
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    as_json: bool = False,
    force_plain: bool = False,
    timeout: float = 3.0,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """Inspect status of running and recent tasks across the mesh or query specific task ID."""
    from meshweaver.cli_dashboard import TaskDashboardItem, format_tasks_output, generate_dashboard_snapshot
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-tasks-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    await node.start()
    try:
        if seeds:
            await node.join_mesh(seeds, timeout=timeout)
            await asyncio.sleep(0.2)

        snapshot = generate_dashboard_snapshot(node)

        if task_id:
            matched = [t for t in snapshot.running_tasks + snapshot.recent_tasks if t.task_id == task_id]
            if not matched and seeds:
                try:
                    remote_resp = await node.query_task_status(task_id, timeout=timeout)
                    if remote_resp is not None:
                        dur = (
                            round(remote_resp.execution_time * 1000, 2)
                            if remote_resp.execution_time
                            else None
                        )
                        res_p = (
                            str(remote_resp.result)[:57]
                            if remote_resp.result is not None
                            else None
                        )
                        err_c = remote_resp.error.code if remote_resp.error else None
                        err_m = remote_resp.error.message if remote_resp.error else None
                        matched = [
                            TaskDashboardItem(
                                task_id=remote_resp.task_id,
                                task_name="remote_task",
                                status=remote_resp.status.value.upper(),
                                worker_id=remote_resp.metadata.get("worker_id"),
                                duration_ms=dur,
                                result_preview=res_p,
                                error_code=err_c,
                                error_message=err_m,
                                is_failure=remote_resp.status.value in ("failure", "error"),
                            )
                        ]
                except Exception as exc:
                    logger.debug("Failed querying remote task status: %s", exc)

            format_tasks_output(matched, as_json=as_json, force_plain=force_plain)
            return 0 if matched else 1
        else:
            all_displayed = snapshot.running_tasks + snapshot.recent_tasks
            format_tasks_output(all_displayed, as_json=as_json, force_plain=force_plain)
            return 0
    finally:
        try:
            await node.leave_mesh()
        except Exception:
            pass
        await node.stop()


async def run_monitor_cmd(
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    seeds: list[str] | None = None,
    refresh_rate: float = 1.0,
    duration: float | None = None,
    force_plain: bool = False,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """Launch the interactive terminal monitoring dashboard (Rich or plain text)."""
    from meshweaver.cli_dashboard import run_live_dashboard
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=False)

    assigned_id = peer_id if peer_id else f"monitor-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    node.enable_heartbeats()
    await node.start()

    try:
        if seeds:
            print(f"[MeshWeaver] Connecting to mesh via seeds: {seeds}...")
            await node.join_mesh(seeds, timeout=5.0)
            print("[MeshWeaver] Connected to mesh. Launching live dashboard...")

        await run_live_dashboard(
            node,
            refresh_rate=refresh_rate,
            duration=duration,
            plain=force_plain,
        )
        return 0
    except (asyncio.CancelledError, KeyboardInterrupt):
        return 0
    finally:
        print(f"\n[MeshWeaver] Stopping monitoring node '{node.peer_id}' gracefully...")
        try:
            await node.leave_mesh()
        except Exception:
            pass
        await node.stop()
        print(f"[MeshWeaver] Node '{node.peer_id}' stopped gracefully.")


async def run_submit_cmd(
    task_name: str,
    args_list: list[str],
    seeds: list[str],
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    as_json: bool = False,
    timeout: float = 5.0,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """Start local peer node, discover mesh, submit task, and print result."""
    import json
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-client-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    await node.start()
    try:
        if seeds:
            await node.join_mesh(seeds, timeout=timeout)
            await asyncio.sleep(0.15)

        # Parse string args into ints/floats/json if possible
        parsed_args: list[Any] = []
        for a in args_list:
            try:
                parsed_args.append(json.loads(a))
            except Exception:
                parsed_args.append(a)

        resp = await node.submit_task(task_name, *parsed_args, timeout=timeout)
        if as_json:
            print(resp.to_json())
        else:
            if resp.status.value == "success":
                print(f"[SUCCESS] Task ID: {resp.task_id} | Result: {resp.result}")
            else:
                err_msg = resp.error.message if resp.error else "Unknown error"
                err_code = resp.error.code if resp.error else "ERROR"
                print(f"[{resp.status.value.upper()}] Task ID: {resp.task_id} | Code: {err_code} | Error: {err_msg}")
        return 0 if resp.status.value == "success" else 1
    finally:
        await node.stop()


async def run_dashboard_cmd(
    host: str = "127.0.0.1",
    port: int = 0,
    api_host: str = "127.0.0.1",
    api_port: int = 8000,
    peer_id: str | None = None,
    seeds: list[str] | None = None,
    verbose: bool = False,
    security_config: Any | None = None,
) -> int:
    """Start local peer node and launch FastAPI backend server with REST & WebSocket endpoints."""
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging
    from meshweaver.api import create_api_app

    setup_structured_logging(verbose=verbose, as_json=False)

    assigned_id = peer_id if peer_id else f"dashboard-node-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id, security_config=security_config)
    node.enable_gossip()
    node.enable_heartbeats()
    await node.start()

    print(f"[MeshWeaver] PeerNode '{node.peer_id}' started on {node.host}:{node.bound_port}")
    try:
        if seeds:
            print(f"[MeshWeaver] Joining mesh via seeds: {seeds}...")
            await node.join_mesh(seeds, timeout=5.0)
            print("[MeshWeaver] Successfully joined mesh via seeds.")

        import uvicorn

        app = create_api_app(node)
        config = uvicorn.Config(
            app=app,
            host=api_host,
            port=api_port,
            log_level="info" if verbose else "warning",
            access_log=verbose,
        )
        server = uvicorn.Server(config)

        print(f"[MeshWeaver] Dashboard API listening at http://{api_host}:{api_port}")
        print(f"[MeshWeaver] REST API Docs available at http://{api_host}:{api_port}/docs")
        print("[MeshWeaver] Press Ctrl+C to terminate.")

        await server.serve()
        return 0
    except (asyncio.CancelledError, KeyboardInterrupt):
        return 0
    finally:
        print(f"\n[MeshWeaver] Stopping node '{node.peer_id}'...")
        try:
            await node.leave_mesh()
        except Exception:
            pass
        await node.stop()
        print(f"[MeshWeaver] Node '{node.peer_id}' stopped gracefully.")


def main(argv: list[str] | None = None) -> int:
    """Execute the MeshWeaver CLI entrypoint."""
    raw_args = list(sys.argv[1:] if argv is None else argv)

    # Normalize positional arguments
    if "demo" in raw_args:
        raw_args = [arg if arg != "demo" else "--demo" for arg in raw_args]

    # Create root parser
    parser = argparse.ArgumentParser(
        prog="meshweaver",
        description=f"MeshWeaver: Zero-Dependency P2P Async Task Broker (v{__version__})",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run the two-peer local communication demonstration",
    )
    parser.add_argument(
        "--multi-node",
        "--demo-mesh",
        dest="multi_node",
        action="store_true",
        help="Run the multi-node discovery and gossip demonstration",
    )
    parser.add_argument(
        "--tasks",
        "--remote-tasks",
        "--demo-tasks",
        dest="remote_tasks",
        action="store_true",
        help="Run the end-to-end remote task execution demonstration",
    )
    def add_config_argument(p: argparse.ArgumentParser) -> None:
        p.add_argument(
            "--config",
            dest="config_path",
            default=None,
            help="Path to JSON configuration file",
        )

    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Optional path to JSON configuration file",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print current node and mesh peer status",
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Local network host interface (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Local network port to bind (default: 0 for dynamic)",
    )
    parser.add_argument(
        "--peer-id",
        dest="peer_id",
        default=None,
        help="Optional local node/peer identifier",
    )
    parser.add_argument(
        "--seeds",
        default=None,
        help="Comma-separated bootstrap seed peer addresses (e.g. 127.0.0.1:8001)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output node and peer status in structured JSON format",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Connection and discovery timeout in seconds (default: 3.0)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    # Subparsers for commands like `meshweaver status`
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")

    status_parser = subparsers.add_parser(
        "status",
        help="Print current node and mesh peer status",
    )
    add_config_argument(status_parser)
    status_parser.add_argument(
        "--host",
        default=None,
        help="Local network host interface (default: 127.0.0.1)",
    )
    status_parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Local network port to bind (default: 0 for dynamic)",
    )
    status_parser.add_argument(
        "--peer-id",
        dest="peer_id",
        default=None,
        help="Optional local node/peer identifier",
    )
    status_parser.add_argument(
        "--seeds",
        default=None,
        help="Comma-separated bootstrap seed peer addresses (e.g. 127.0.0.1:8001)",
    )
    status_parser.add_argument(
        "--json",
        action="store_true",
        help="Output node and peer status in structured JSON format",
    )
    status_parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Connection and discovery timeout in seconds (default: 3.0)",
    )
    status_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )
    add_security_arguments(status_parser)

    demo_parser = subparsers.add_parser(
        "demo",
        help="Run MeshWeaver local demonstrations",
    )
    demo_parser.add_argument(
        "--multi-node",
        "--mesh",
        action="store_true",
        dest="multi_node",
        help="Run the multi-node discovery and gossip demonstration",
    )
    demo_parser.add_argument(
        "--tasks",
        "--remote-tasks",
        dest="remote_tasks",
        action="store_true",
        help="Run the end-to-end remote task execution demonstration",
    )
    add_config_argument(demo_parser)
    demo_parser.add_argument(
        "--json",
        action="store_true",
        help="Output demo results as JSON",
    )
    demo_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

    submit_parser = subparsers.add_parser(
        "submit",
        help="Submit a task for remote execution across the mesh",
    )
    submit_parser.add_argument("task_name", help="Registered task name (e.g. add, reverse_string, echo, ping)")
    submit_parser.add_argument("task_args", nargs="*", default=[], help="Positional arguments for the task")
    add_config_argument(submit_parser)
    submit_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses")
    submit_parser.add_argument("--host", default=None, help="Local network host interface")
    submit_parser.add_argument("--port", type=int, default=None, help="Local network port to bind")
    submit_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local peer identifier")
    submit_parser.add_argument("--json", action="store_true", help="Output task response as JSON")
    submit_parser.add_argument("--timeout", type=float, default=None, help="Task timeout in seconds")
    submit_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(submit_parser)

    start_parser = subparsers.add_parser(
        "start",
        aliases=["node"],
        help="Start a MeshWeaver peer node to serve tasks and participate in gossip",
    )
    add_config_argument(start_parser)
    start_parser.add_argument("--host", default=None, help="Local network host interface (default: 127.0.0.1)")
    start_parser.add_argument("--port", type=int, default=None, help="Local network port to bind (default: 0 for dynamic)")
    start_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local node/peer identifier")
    start_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses (e.g. 127.0.0.1:8001)")
    start_parser.add_argument("--duration", type=float, default=None, help="Optional duration in seconds to keep node running")
    start_parser.add_argument("--dashboard", "--tui", "--monitor", dest="show_dashboard", action="store_true", help="Launch interactive Rich terminal monitoring dashboard")
    start_parser.add_argument("--refresh-rate", type=float, default=None, help="Terminal dashboard refresh interval in seconds (default: 1.0)")
    start_parser.add_argument("--plain", action="store_true", help="Force plain-text ASCII output without Rich formatting")
    start_parser.add_argument("--json", action="store_true", help="Output status events in JSON format")
    start_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(start_parser)

    peers_parser = subparsers.add_parser(
        "peers",
        aliases=["list-peers", "peer-list"],
        help="Discover and list known mesh peers and health states",
    )
    add_config_argument(peers_parser)
    peers_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses (e.g. 127.0.0.1:8001)")
    peers_parser.add_argument("--host", default=None, help="Local network host interface")
    peers_parser.add_argument("--port", type=int, default=None, help="Local network port to bind")
    peers_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local peer identifier")
    peers_parser.add_argument("--json", action="store_true", help="Output peer list in JSON format")
    peers_parser.add_argument("--plain", action="store_true", help="Force plain-text table output")
    peers_parser.add_argument("--timeout", type=float, default=None, help="Discovery timeout in seconds")
    peers_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(peers_parser)

    tasks_parser = subparsers.add_parser(
        "tasks",
        aliases=["task", "task-status"],
        help="View active running tasks and recent execution results",
    )
    add_config_argument(tasks_parser)
    tasks_parser.add_argument("task_id", nargs="?", default=None, help="Optional specific task ID to query")
    tasks_parser.add_argument("--task-id", dest="task_id_flag", default=None, help="Specific task ID to query")
    tasks_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses")
    tasks_parser.add_argument("--host", default=None, help="Local network host interface")
    tasks_parser.add_argument("--port", type=int, default=None, help="Local network port to bind")
    tasks_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local peer identifier")
    tasks_parser.add_argument("--json", action="store_true", help="Output task information in JSON format")
    tasks_parser.add_argument("--plain", action="store_true", help="Force plain-text table output")
    tasks_parser.add_argument("--timeout", type=float, default=None, help="Query timeout in seconds")
    tasks_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(tasks_parser)

    monitor_parser = subparsers.add_parser(
        "monitor",
        aliases=["tui"],
        help="Launch live interactive Rich terminal monitoring dashboard",
    )
    add_config_argument(monitor_parser)
    monitor_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses")
    monitor_parser.add_argument("--host", default=None, help="Local network host interface (default: 127.0.0.1)")
    monitor_parser.add_argument("--port", type=int, default=None, help="Local network port to bind (default: 0 for dynamic)")
    monitor_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local node/peer identifier")
    monitor_parser.add_argument("--refresh-rate", type=float, default=None, help="Dashboard refresh interval in seconds (default: 1.0)")
    monitor_parser.add_argument("--duration", type=float, default=None, help="Optional duration in seconds before auto-closing")
    monitor_parser.add_argument("--plain", action="store_true", help="Force plain-text ASCII output without Rich formatting")
    monitor_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(monitor_parser)

    dashboard_parser = subparsers.add_parser(
        "dashboard",
        aliases=["web", "ui"],
        help="Launch MeshWeaver peer node and REST/WebSocket dashboard API server",
    )
    add_config_argument(dashboard_parser)
    dashboard_parser.add_argument("--host", default=None, help="P2P network host interface (default: 127.0.0.1)")
    dashboard_parser.add_argument("--port", type=int, default=None, help="P2P network port (default: 0 for dynamic)")
    dashboard_parser.add_argument("--api-host", default="127.0.0.1", help="FastAPI dashboard host (default: 127.0.0.1)")
    dashboard_parser.add_argument("--api-port", type=int, default=8000, help="FastAPI dashboard port (default: 8000)")
    dashboard_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local node/peer identifier")
    dashboard_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses")
    dashboard_parser.add_argument("--tui", "--terminal", dest="use_tui", action="store_true", help="Launch terminal monitoring dashboard instead of web server")
    dashboard_parser.add_argument("--refresh-rate", type=float, default=None, help="Dashboard refresh interval in seconds")
    dashboard_parser.add_argument("--plain", action="store_true", help="Force plain-text output for terminal dashboard")
    dashboard_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")
    add_security_arguments(dashboard_parser)

    try:
        args = parser.parse_args(raw_args)

        # 1. Remote tasks demo mode
        if getattr(args, "remote_tasks", False) or (
            getattr(args, "command", None) == "demo" and getattr(args, "remote_tasks", False)
        ):
            workspace_root = Path(__file__).resolve().parent.parent.parent
            if str(workspace_root) not in sys.path:
                sys.path.insert(0, str(workspace_root))

            from examples.remote_tasks import run_remote_tasks_demo

            as_json = getattr(args, "json", False)
            res = asyncio.run(
                run_remote_tasks_demo(
                    host=getattr(args, "host", None) or "127.0.0.1",
                    as_json=as_json,
                )
            )
            return 0 if res.get("teardown_ok", False) else 1

        # 2. Multi-node demo mode
        if getattr(args, "multi_node", False):
            workspace_root = Path(__file__).resolve().parent.parent.parent
            if str(workspace_root) not in sys.path:
                sys.path.insert(0, str(workspace_root))

            from examples.multi_node import run_multi_node_demo

            config_file = getattr(args, "config_path", None)
            as_json = getattr(args, "json", False)
            asyncio.run(
                run_multi_node_demo(
                    host=getattr(args, "host", None) or "127.0.0.1",
                    config_path=config_file,
                    as_json=as_json,
                )
            )
            return 0

        # 3. Two-peer demo mode
        if args.demo or (getattr(args, "command", None) == "demo" and not getattr(args, "multi_node", False) and not getattr(args, "remote_tasks", False)):
            workspace_root = Path(__file__).resolve().parent.parent.parent
            if str(workspace_root) not in sys.path:
                sys.path.insert(0, str(workspace_root))

            from examples.two_peers import run_two_peers_demo

            asyncio.run(run_two_peers_demo())
            return 0

        # 4. Submit task subcommand
        if getattr(args, "command", None) == "submit":
            cfg = resolve_node_config(args)
            if not cfg.seeds:
                raise ConfigurationError(
                    "Seeds are required to submit tasks to the mesh. Provide --seeds, set MESHWEAVER_SEEDS, or configure 'seeds' in config file."
                )
            return asyncio.run(
                run_submit_cmd(
                    task_name=args.task_name,
                    args_list=getattr(args, "task_args", []),
                    seeds=cfg.seeds,
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    as_json=cfg.as_json,
                    timeout=cfg.timeout,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # 5. Start node subcommand
        if getattr(args, "command", None) in ("start", "node"):
            cfg = resolve_node_config(args)
            return asyncio.run(
                run_start_cmd(
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    seeds=cfg.seeds or None,
                    duration=cfg.duration,
                    verbose=cfg.verbose,
                    as_json=cfg.as_json,
                    security_config=cfg.security,
                    show_dashboard=cfg.show_dashboard,
                    refresh_rate=cfg.refresh_rate,
                    force_plain=cfg.force_plain,
                )
            )

        # 6. Peers subcommand
        if getattr(args, "command", None) in ("peers", "list-peers", "peer-list"):
            cfg = resolve_node_config(args)
            return asyncio.run(
                run_peers_cmd(
                    seeds=cfg.seeds or None,
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    as_json=cfg.as_json,
                    force_plain=cfg.force_plain,
                    timeout=cfg.timeout,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # 7. Tasks subcommand
        if getattr(args, "command", None) in ("tasks", "task", "task-status"):
            cfg = resolve_node_config(args)
            effective_task_id = getattr(args, "task_id_flag", None) or getattr(args, "task_id", None)
            return asyncio.run(
                run_tasks_cmd(
                    task_id=effective_task_id,
                    seeds=cfg.seeds or None,
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    as_json=cfg.as_json,
                    force_plain=cfg.force_plain,
                    timeout=cfg.timeout,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # 8. Monitor / TUI subcommand
        if getattr(args, "command", None) in ("monitor", "tui"):
            cfg = resolve_node_config(args)
            return asyncio.run(
                run_monitor_cmd(
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    seeds=cfg.seeds or None,
                    refresh_rate=cfg.refresh_rate,
                    duration=cfg.duration,
                    force_plain=cfg.force_plain,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # 9. Dashboard / Web server subcommand
        if getattr(args, "command", None) in ("dashboard", "web", "ui"):
            cfg = resolve_node_config(args)
            if getattr(args, "use_tui", False):
                return asyncio.run(
                    run_monitor_cmd(
                        host=cfg.host,
                        port=cfg.port,
                        peer_id=cfg.peer_id,
                        seeds=cfg.seeds or None,
                        refresh_rate=cfg.refresh_rate,
                        force_plain=cfg.force_plain,
                        verbose=cfg.verbose,
                        security_config=cfg.security,
                    )
                )

            return asyncio.run(
                run_dashboard_cmd(
                    host=cfg.host,
                    port=cfg.port,
                    api_host=getattr(args, "api_host", "127.0.0.1"),
                    api_port=getattr(args, "api_port", 8000),
                    peer_id=cfg.peer_id,
                    seeds=cfg.seeds or None,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # 10. Status command
        if getattr(args, "command", None) == "status" or args.status:
            cfg = resolve_node_config(args)
            return asyncio.run(
                run_status_cmd(
                    host=cfg.host,
                    port=cfg.port,
                    peer_id=cfg.peer_id,
                    seeds=cfg.seeds or None,
                    as_json=cfg.as_json,
                    timeout=cfg.timeout,
                    verbose=cfg.verbose,
                    security_config=cfg.security,
                )
            )

        # Default banner output when no flags/subcommands provided
        print(f"MeshWeaver: Zero-Dependency P2P Async Task Broker (v{__version__})")
        print(
            "Run tests with 'pytest'. To see peer status: python -m meshweaver status. "
            "To see the task demo: python -m meshweaver --tasks"
        )
        return 0
    except ConfigurationError as err:
        print(f"[MeshWeaver Configuration Error] {err}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

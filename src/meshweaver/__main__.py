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

from meshweaver import __version__

logger = logging.getLogger("meshweaver.cli")


async def run_status_cmd(
    host: str = "127.0.0.1",
    port: int = 0,
    peer_id: str | None = None,
    seeds: list[str] | None = None,
    as_json: bool = False,
    timeout: float = 3.0,
    verbose: bool = False,
) -> int:
    """
    Start a local peer node, optionally join seed nodes, and print status.
    """
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-node-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id)
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
) -> int:
    """
    Start a MeshWeaver node that runs persistently, listens for tasks, and gossips.
    """
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"node-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id)
    node.enable_gossip()
    await node.start()

    print(f"[MeshWeaver] Node '{node.peer_id}' started on {node.host}:{node.bound_port}")
    try:
        if seeds:
            print(f"[MeshWeaver] Joining mesh via seeds: {seeds}...")
            await node.join_mesh(seeds, timeout=5.0)
            print("[MeshWeaver] Successfully joined mesh via seeds.")

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
        print(f"\n[MeshWeaver] Stopping node '{node.peer_id}'...")
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
) -> int:
    """Start local peer node, discover mesh, submit task, and print result."""
    import json
    from meshweaver.network import PeerNode
    from meshweaver.status import setup_structured_logging

    setup_structured_logging(verbose=verbose, as_json=as_json)

    assigned_id = peer_id if peer_id else f"cli-client-{int(time.time() * 1000) % 100000}"
    node = PeerNode(host=host, port=port, peer_id=assigned_id)
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
    parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Optional path to JSON configuration file for multi-node demo",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Print current node and mesh peer status",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Local network host interface (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=0,
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
        default=3.0,
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
    status_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Local network host interface (default: 127.0.0.1)",
    )
    status_parser.add_argument(
        "--port",
        type=int,
        default=0,
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
        default=3.0,
        help="Connection and discovery timeout in seconds (default: 3.0)",
    )
    status_parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

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
    demo_parser.add_argument(
        "--config",
        dest="config_path",
        default=None,
        help="Optional path to JSON configuration file for multi-node demo",
    )
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
    submit_parser.add_argument("--seeds", required=True, help="Comma-separated bootstrap seed peer addresses")
    submit_parser.add_argument("--host", default="127.0.0.1", help="Local network host interface")
    submit_parser.add_argument("--port", type=int, default=0, help="Local network port to bind")
    submit_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local peer identifier")
    submit_parser.add_argument("--json", action="store_true", help="Output task response as JSON")
    submit_parser.add_argument("--timeout", type=float, default=5.0, help="Task timeout in seconds")
    submit_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")

    start_parser = subparsers.add_parser(
        "start",
        aliases=["node"],
        help="Start a MeshWeaver peer node to serve tasks and participate in gossip",
    )
    start_parser.add_argument("--host", default="127.0.0.1", help="Local network host interface (default: 127.0.0.1)")
    start_parser.add_argument("--port", type=int, default=0, help="Local network port to bind (default: 0 for dynamic)")
    start_parser.add_argument("--peer-id", dest="peer_id", default=None, help="Optional local node/peer identifier")
    start_parser.add_argument("--seeds", default=None, help="Comma-separated bootstrap seed peer addresses (e.g. 127.0.0.1:8001)")
    start_parser.add_argument("--duration", type=float, default=None, help="Optional duration in seconds to keep node running")
    start_parser.add_argument("--json", action="store_true", help="Output status events in JSON format")
    start_parser.add_argument("--verbose", action="store_true", help="Enable verbose debug logging")

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
                host=getattr(args, "host", "127.0.0.1"),
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
                host=getattr(args, "host", "127.0.0.1"),
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
        seed_list = [s.strip() for s in args.seeds.split(",") if s.strip()]
        return asyncio.run(
            run_submit_cmd(
                task_name=args.task_name,
                args_list=getattr(args, "task_args", []),
                seeds=seed_list,
                host=args.host,
                port=args.port,
                peer_id=args.peer_id,
                as_json=args.json,
                timeout=args.timeout,
                verbose=args.verbose,
            )
        )

    # 5. Start node subcommand
    if getattr(args, "command", None) in ("start", "node"):
        seed_list = None
        if args.seeds:
            seed_list = [s.strip() for s in args.seeds.split(",") if s.strip()]

        return asyncio.run(
            run_start_cmd(
                host=args.host,
                port=args.port,
                peer_id=args.peer_id,
                seeds=seed_list,
                duration=getattr(args, "duration", None),
                verbose=args.verbose,
                as_json=args.json,
            )
        )

    # 6. Status command
    if getattr(args, "command", None) == "status" or args.status:
        seed_list = None
        if args.seeds:
            seed_list = [s.strip() for s in args.seeds.split(",") if s.strip()]

        return asyncio.run(
            run_status_cmd(
                host=args.host,
                port=args.port,
                peer_id=args.peer_id,
                seeds=seed_list,
                as_json=args.json,
                timeout=args.timeout,
                verbose=args.verbose,
            )
        )

    # 6. Default banner output when no flags/subcommands provided
    print(f"MeshWeaver: Zero-Dependency P2P Async Task Broker (v{__version__})")
    print(
        "Run tests with 'pytest'. To see peer status: python -m meshweaver status. "
        "To see the task demo: python -m meshweaver --tasks"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

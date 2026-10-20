"""
MeshWeaver package entry point.

Allows running the package directly via `python -m meshweaver`.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

from meshweaver import __version__


def main(argv: list[str] | None = None) -> int:
    """Execute the MeshWeaver CLI entrypoint."""
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
        "-v",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    # Normalize positional "demo" into "--demo" flag if present
    raw_args = list(sys.argv[1:] if argv is None else argv)
    if "demo" in raw_args:
        raw_args = [arg if arg != "demo" else "--demo" for arg in raw_args]

    args = parser.parse_args(raw_args)

    if args.demo:
        # Ensure workspace root is in sys.path to locate examples
        workspace_root = Path(__file__).resolve().parent.parent.parent
        if str(workspace_root) not in sys.path:
            sys.path.insert(0, str(workspace_root))

        from examples.two_peers import run_two_peers_demo

        asyncio.run(run_two_peers_demo())
        return 0

    print(f"MeshWeaver: Zero-Dependency P2P Async Task Broker (v{__version__})")
    print("Run tests with 'pytest'. To see the peer communication demo, run: python -m meshweaver --demo")
    return 0


if __name__ == "__main__":
    sys.exit(main())

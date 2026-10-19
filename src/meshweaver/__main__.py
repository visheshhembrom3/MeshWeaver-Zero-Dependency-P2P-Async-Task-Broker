"""
MeshWeaver package entry point.

Allows running the package directly via `python -m meshweaver`.
"""

import sys
from meshweaver import __version__


def main() -> int:
    """Print package information and exit."""
    print(f"MeshWeaver: Zero-Dependency P2P Async Task Broker (v{__version__})")
    print("Run tests with 'pytest'. Full CLI commands will be introduced in subsequent milestones.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

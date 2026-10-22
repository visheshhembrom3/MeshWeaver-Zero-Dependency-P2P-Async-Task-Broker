"""Tests for basic MeshWeaver package import and metadata."""

import subprocess
import sys

import meshweaver


def test_package_import_and_version() -> None:
    """Verify that the package imports properly and exposes a valid version string."""
    assert hasattr(meshweaver, "__version__")
    assert isinstance(meshweaver.__version__, str)
    assert meshweaver.__version__ == "0.1.0"


def test_main_entrypoint_executes() -> None:
    """Verify that python -m meshweaver runs and exits with status code 0."""
    result = subprocess.run(
        [sys.executable, "-m", "meshweaver"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0
    assert "MeshWeaver" in result.stdout
    assert "0.1.0" in result.stdout


def test_all_exports_present() -> None:
    """Verify that every symbol listed in __all__ exists in meshweaver."""
    for name in meshweaver.__all__:
        assert hasattr(meshweaver, name), f"Missing export: {name}"


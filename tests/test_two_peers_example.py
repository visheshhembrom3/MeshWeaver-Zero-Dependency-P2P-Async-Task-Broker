"""
Integration tests for the two-peer communication example.
Verifies message round trip, task execution, error handling, and orderly shutdown
without relying on fixed ports or external network access.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import pytest

from examples.two_peers import run_two_peers_demo


@pytest.mark.asyncio
async def test_two_peers_demo_execution() -> None:
    """
    Verify programmatic execution of the two-peer communication demonstration
    using dynamic local ports and loopback interface.
    """
    results = await run_two_peers_demo(host="127.0.0.1", port_a=0, port_b=0, quiet=True)

    # 1. Ports are dynamically assigned and distinct
    assert results["worker_port"] > 0
    assert results["client_port"] > 0
    assert results["worker_port"] != results["client_port"]

    # 2. Handshake succeeded
    assert results["handshake_ok"] is True

    # 3. PING / PONG round-trip succeeded with valid latency
    assert results["ping_pong_ok"] is True
    assert isinstance(results["ping_latency_ms"], float)
    assert results["ping_latency_ms"] >= 0.0

    # 4. Validated task execution: add(100, 250)
    assert results["task_add_status"] == "success"
    assert results["task_add_result"] == 350

    # 5. Validated task execution: reverse_string
    assert results["task_rev_status"] == "success"
    assert results["task_rev_result"] == "dezilartneceD revaeWhseM"

    # 6. Safe rejection of unregistered task name
    assert results["unknown_task_rejected"] is True
    assert results["unknown_task_error_code"] == "UNKNOWN_TASK"


def test_two_peers_cli_script_execution() -> None:
    """
    Verify running examples/two_peers.py as a standalone command-line script.
    Checks return code, orderly output progression, and clean exit.
    """
    workspace_root = Path(__file__).resolve().parent.parent
    script_path = workspace_root / "examples" / "two_peers.py"

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=15.0,
    )

    assert result.returncode == 0, f"Script failed with stderr:\n{result.stderr}"
    stdout = result.stdout

    # Verify structured sections in console output
    assert "MeshWeaver: Two-Peer Communication Demonstration" in stdout
    assert "1. Starting local nodes..." in stdout
    assert "2. Connecting Client Node to Worker Node and performing handshake..." in stdout
    assert "Handshake established: client-node-1 <-> worker-node-1" in stdout
    assert "3. Exchanging PING / PONG..." in stdout
    assert "Received PONG from worker-node-1" in stdout
    assert "4. Sending validated task requests..." in stdout
    assert "Result: 350 (Status: success" in stdout
    assert "Result: 'dezilartneceD revaeWhseM' (Status: success" in stdout
    assert "Safely rejected: Status=error, Code=UNKNOWN_TASK" in stdout
    assert "5. Executing orderly shutdown..." in stdout
    assert "Connection closed." in stdout
    assert "[Client Node] client-node-1 stopped." in stdout
    assert "[Worker Node] worker-node-1 stopped." in stdout
    assert "Demonstration completed successfully!" in stdout


def test_meshweaver_main_demo_flag() -> None:
    """
    Verify that python -m meshweaver --demo executes the two-peer demonstration.
    """
    result = subprocess.run(
        [sys.executable, "-m", "meshweaver", "--demo"],
        capture_output=True,
        text=True,
        check=False,
        timeout=15.0,
    )

    assert result.returncode == 0, f"Command failed with stderr:\n{result.stderr}"
    assert "MeshWeaver: Two-Peer Communication Demonstration" in result.stdout
    assert "Demonstration completed successfully!" in result.stdout

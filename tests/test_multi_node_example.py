"""
Integration tests for the multi-node communication and gossip demonstration.

Verifies programmatic execution, CLI script execution, configuration loading,
JSON output, and package CLI flags for the multi-node network demo without
requiring external Internet access.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import pytest

from examples.multi_node import run_multi_node_demo


@pytest.mark.asyncio
async def test_multi_node_demo_programmatic_execution() -> None:
    """
    Verify programmatic execution of run_multi_node_demo across three loopback nodes.
    Checks metrics, dynamic port allocation, bootstrap discovery, peer lookup,
    gossip exchange, status reporting, and graceful shutdown.
    """
    results = await run_multi_node_demo(host="127.0.0.1", quiet=True)

    # 1. Ports are dynamically assigned, distinct, and positive
    assert results["bootstrap_port"] > 0
    assert results["worker1_port"] > 0
    assert results["worker2_port"] > 0
    assert results["distinct_ports"] is True

    # 2. Bootstrap discovery succeeded
    assert results["bootstrap_discovery_ok"] is True

    # 3. DHT peer lookup succeeded and converged
    assert results["peer_lookup_ok"] is True
    assert isinstance(results["dht_distance"], int)
    assert results["dht_distance"] > 0
    assert results["peer_lookup_queries"] > 0

    # 4. Workload gossip was successfully exchanged
    assert results["gossip_exchange_ok"] is True
    assert results["gossip_reports_w1"] >= 2
    assert results["gossip_reports_w2"] >= 2

    # 5. Status reporting snapshot verified
    assert results["status_reporting_ok"] is True
    snapshot = results["bootstrap_status_snapshot"]
    assert snapshot["node_id"] == "bootstrap-node"
    assert snapshot["total_registered_peers"] >= 2

    # 6. Graceful departure and shutdown succeeded
    assert results["graceful_departure_ok"] is True
    assert results["graceful_shutdown_ok"] is True


def test_multi_node_cli_script_execution() -> None:
    """
    Verify running examples/multi_node.py as a standalone command-line script.
    Checks return code, orderly phase progression, and clean exit.
    """
    workspace_root = Path(__file__).resolve().parent.parent
    script_path = workspace_root / "examples" / "multi_node.py"

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=25.0,
    )

    assert result.returncode == 0, f"Script failed with stderr:\n{result.stderr}"
    stdout = result.stdout

    # Verify structured sections in console output
    assert "MeshWeaver: Multi-Node Discovery and Gossip Demonstration" in stdout
    assert "1. Starting 3 local MeshWeaver nodes on loopback interface..." in stdout
    assert "2. Executing bootstrap discovery via seed node..." in stdout
    assert "Bootstrap discovery verification: True" in stdout
    assert "3. Executing DHT peer lookup across network..." in stdout
    assert "Worker 1 registered Worker 2: True" in stdout
    assert "4. Exchanging workload & resource gossip across the mesh..." in stdout
    assert "Worker 1 received gossip for Worker 2: True" in stdout
    assert "5. Inspecting mesh status via NodeStatusAPI..." in stdout
    assert "MeshWeaver Node & Peer Status" in stdout
    assert "6. Executing graceful departure of Worker 2 and orderly shutdown..." in stdout
    assert "Multi-Node Demonstration completed successfully!" in stdout


def test_multi_node_cli_with_config_file() -> None:
    """
    Verify running examples/multi_node.py with --config examples/mesh_config.json.
    """
    workspace_root = Path(__file__).resolve().parent.parent
    script_path = workspace_root / "examples" / "multi_node.py"
    config_path = workspace_root / "examples" / "mesh_config.json"

    result = subprocess.run(
        [sys.executable, str(script_path), "--config", str(config_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=25.0,
    )

    assert result.returncode == 0, f"Script failed with stderr:\n{result.stderr}"
    assert "Loaded configuration from:" in result.stdout
    assert "Multi-Node Demonstration completed successfully!" in result.stdout


def test_multi_node_cli_json_output() -> None:
    """
    Verify running examples/multi_node.py with --json outputs valid JSON status.
    """
    workspace_root = Path(__file__).resolve().parent.parent
    script_path = workspace_root / "examples" / "multi_node.py"

    result = subprocess.run(
        [sys.executable, str(script_path), "--json"],
        capture_output=True,
        text=True,
        check=False,
        timeout=25.0,
    )

    assert result.returncode == 0, f"Script failed with stderr:\n{result.stderr}"
    # Find JSON payload in stdout
    stdout = result.stdout
    json_start = stdout.find("{")
    assert json_start != -1, "No JSON object found in output"
    json_payload = stdout[json_start:].strip()
    data = json.loads(json_payload)

    assert data["node_id"] == "bootstrap-node"
    assert "peers" in data
    assert len(data["peers"]) >= 2


def test_meshweaver_main_multi_node_flags() -> None:
    """
    Verify that python -m meshweaver --multi-node executes the multi-node demonstration.
    """
    result = subprocess.run(
        [sys.executable, "-m", "meshweaver", "--multi-node"],
        capture_output=True,
        text=True,
        check=False,
        timeout=25.0,
    )

    assert result.returncode == 0, f"Command failed with stderr:\n{result.stderr}"
    assert "MeshWeaver: Multi-Node Discovery and Gossip Demonstration" in result.stdout
    assert "Multi-Node Demonstration completed successfully!" in result.stdout


def test_meshweaver_main_demo_subcommand_multi_node() -> None:
    """
    Verify that python -m meshweaver demo --multi-node executes the multi-node demonstration.
    """
    result = subprocess.run(
        [sys.executable, "-m", "meshweaver", "demo", "--multi-node"],
        capture_output=True,
        text=True,
        check=False,
        timeout=25.0,
    )

    assert result.returncode == 0, f"Command failed with stderr:\n{result.stderr}"
    assert "MeshWeaver: Multi-Node Discovery and Gossip Demonstration" in result.stdout
    assert "Multi-Node Demonstration completed successfully!" in result.stdout

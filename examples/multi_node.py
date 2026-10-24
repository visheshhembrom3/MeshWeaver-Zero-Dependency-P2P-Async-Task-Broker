"""
MeshWeaver: Multi-Node Discovery and Gossip Demonstration.

Demonstrates multi-node peer-to-peer networking across three or more MeshWeaver nodes
binding separate loopback TCP ports (Milestone 05 / Week 2):
1. Starts three local nodes (Bootstrap Seed, Worker 1, Worker 2) on dynamic ports.
2. Performs bootstrap discovery: Worker 1 and Worker 2 join the mesh via the seed node,
   and Worker 2 discovers Worker 1 via neighbor discovery (FIND_NODE).
3. Executes an iterative DHT peer lookup: Worker 1 locates Worker 2 across the keyspace
   evaluating XOR distance and updating routing tables.
4. Performs periodic workload gossip: Nodes sample and exchange CPU, memory, active task count,
   and health status reports adhering to bounded fanout and hop limits.
5. Inspects mesh state via NodeStatusAPI: Emits human-readable ASCII dashboards and structured JSON.
6. Executes graceful shutdown: Worker 2 departs with a LEAVE announcement; remaining nodes
   purge the departing peer from registries, routing tables, and gossip stores before final teardown.

Usage:
    python examples/multi_node.py
    python examples/multi_node.py --verbose
    python examples/multi_node.py --json
    python examples/multi_node.py --config examples/mesh_config.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from pathlib import Path
import sys
import time
from typing import Any

# Ensure meshweaver package in src/ is on sys.path
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = WORKSPACE_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from meshweaver import (  # noqa: E402
    HealthStatus,
    Message,
    MessageType,
    NodeID,
    PeerConnection,
    PeerNode,
    PeerState,
    setup_structured_logging,
    xor_distance,
)

logger = logging.getLogger("meshweaver.demo.multi_node")


def setup_logging(verbose: bool = False, as_json: bool = False) -> None:
    """Configure structured logging for the multi-node demonstration."""
    setup_structured_logging(verbose=verbose, as_json=as_json)


async def run_multi_node_demo(
    host: str = "127.0.0.1",
    config_path: str | Path | None = None,
    quiet: bool = False,
    as_json: bool = False,
) -> dict[str, Any]:
    """
    Run an end-to-end multi-node networking experiment verifying bootstrap discovery,
    DHT peer lookup, gossip exchange, node-status reporting, and graceful shutdown.

    Args:
        host: Loopback IP interface to bind nodes to (default: '127.0.0.1').
        config_path: Optional path to a JSON configuration file.
        quiet: If True, suppress console print statements (useful for automated tests).
        as_json: If True, print structured JSON snapshots.

    Returns:
        dict[str, Any]: Execution metrics and verification results.
    """

    def log_print(text: str = "") -> None:
        if not quiet and not as_json:
            print(text)

    log_print("=" * 80)
    log_print("MeshWeaver: Multi-Node Discovery and Gossip Demonstration (Milestone 05)")
    log_print("=" * 80)

    # Load optional configuration
    config: dict[str, Any] = {}
    if config_path is not None:
        cfg_file = Path(config_path)
        if cfg_file.is_file():
            try:
                config = json.loads(cfg_file.read_text(encoding="utf-8"))
                log_print(f"Loaded configuration from: {cfg_file}")
            except Exception as exc:
                logger.warning("Failed to parse config file %s: %s", cfg_file, exc)

    net_cfg = config.get("network", {})
    gossip_cfg = config.get("gossip", {})
    target_host = net_cfg.get("host", host)
    gossip_interval = float(gossip_cfg.get("interval", 0.2))
    gossip_fanout = int(gossip_cfg.get("fanout", 2))
    gossip_max_hops = int(gossip_cfg.get("max_hops", 3))

    results: dict[str, Any] = {}

    # 1. Initialize three nodes
    bootstrap_node = PeerNode(peer_id="bootstrap-node", host=target_host, port=0)
    worker_1 = PeerNode(peer_id="worker-node-1", host=target_host, port=0)
    worker_2 = PeerNode(peer_id="worker-node-2", host=target_host, port=0)

    nodes = [bootstrap_node, worker_1, worker_2]

    # Enable gossip engine on all nodes with fast test intervals
    for n in nodes:
        n.enable_gossip(
            interval=gossip_interval,
            fanout=gossip_fanout,
            stale_threshold=30.0,
            ttl=60.0,
            max_hops=gossip_max_hops,
        )

    try:
        # Phase 1: Start nodes on dynamic loopback ports
        log_print("\n1. Starting 3 local MeshWeaver nodes on loopback interface...")
        for n in nodes:
            await n.start()
            log_print(
                f"   [Node {n.peer_id}] Bound to: {n.host}:{n.bound_port} | "
                f"DHT NodeID: {n.node_id.hex[:12]}..."
            )

        results["bootstrap_port"] = bootstrap_node.bound_port
        results["worker1_port"] = worker_1.bound_port
        results["worker2_port"] = worker_2.bound_port
        results["node_ports"] = [n.bound_port for n in nodes]
        results["distinct_ports"] = len({n.bound_port for n in nodes}) == 3

        # Phase 2: Bootstrap discovery
        log_print("\n2. Executing bootstrap discovery via seed node...")
        seed_addr = f"{target_host}:{bootstrap_node.bound_port}"

        # Worker 1 joins Bootstrap Seed
        w1_joined = await worker_1.join_mesh([seed_addr], timeout=3.0)
        log_print(f"   Worker 1 joined mesh via {seed_addr} (Discovered: {len(w1_joined)} peers)")

        # Worker 2 joins Bootstrap Seed and discovers Worker 1
        w2_joined = await worker_2.join_mesh([seed_addr], timeout=3.0)
        log_print(f"   Worker 2 joined mesh via {seed_addr} (Discovered: {len(w2_joined)} peers)")

        # Give a brief moment for incoming handshakes to settle
        await asyncio.sleep(0.08)

        # Verification of bootstrap discovery
        b_knows_w1 = bootstrap_node.peer_registry.has_peer(worker_1.peer_id)
        b_knows_w2 = bootstrap_node.peer_registry.has_peer(worker_2.peer_id)
        w1_knows_b = worker_1.peer_registry.has_peer(bootstrap_node.peer_id)
        w2_knows_b = worker_2.peer_registry.has_peer(bootstrap_node.peer_id)
        w2_discovered_w1 = worker_2.peer_registry.has_peer(worker_1.peer_id)

        log_print(f"   Bootstrap registry: {list(bootstrap_node.peer_registry._peers.keys())}")
        log_print(f"   Worker 1 registry: {list(worker_1.peer_registry._peers.keys())}")
        log_print(f"   Worker 2 registry: {list(worker_2.peer_registry._peers.keys())}")

        results["bootstrap_discovery_ok"] = bool(
            b_knows_w1 and b_knows_w2 and w1_knows_b and w2_knows_b and w2_discovered_w1
        )
        log_print(f"   Bootstrap discovery verification: {results['bootstrap_discovery_ok']}")

        # Phase 3: Iterative DHT Peer Lookup
        log_print("\n3. Executing DHT peer lookup across network...")
        # Worker 1 searches for Worker 2's DHT NodeID
        lookup_result = await worker_1.iterative_find_node(
            worker_2.node_id,
            alpha=2,
            k=5,
            query_timeout=2.0,
            overall_timeout=4.0,
        )

        target_discovered = any(c.peer_id == worker_2.peer_id for c in lookup_result.closest_peers)
        w1_registered_w2 = worker_1.peer_registry.has_peer(worker_2.peer_id)
        dht_dist = int(xor_distance(worker_1.node_id, worker_2.node_id))

        log_print(f"   Target: {worker_2.peer_id} ({worker_2.node_id.hex[:12]}...)")
        log_print(f"   XOR Distance (Worker 1 <-> Worker 2): {dht_dist}")
        log_print(f"   Discovered peers in lookup: {[c.peer_id for c in lookup_result.closest_peers]}")
        log_print(f"   Worker 1 registered Worker 2: {w1_registered_w2}")

        results["peer_lookup_ok"] = bool(target_discovered and w1_registered_w2)
        results["peer_lookup_target"] = worker_2.node_id.hex
        results["peer_lookup_queries"] = lookup_result.queries_sent
        results["dht_distance"] = dht_dist

        # Phase 4: Gossip Exchange
        log_print("\n4. Exchanging workload & resource gossip across the mesh...")
        # Trigger immediate gossip round and wait for propagation
        for n in nodes:
            if n.gossip_engine is not None and n.gossip_engine.is_running:
                await n.gossip_engine.run_round()

        # Allow asynchronous gossip dissemination across the loopback connections
        deadline = time.perf_counter() + 3.0
        gossip_propagated = False
        while time.perf_counter() < deadline:
            rep_w2_on_w1 = worker_1.gossip_engine.store.get(worker_2.peer_id)
            rep_w1_on_w2 = worker_2.gossip_engine.store.get(worker_1.peer_id)
            if rep_w2_on_w1 is not None and rep_w1_on_w2 is not None:
                gossip_propagated = True
                break
            await asyncio.sleep(0.05)

        w1_store_reports = len(worker_1.gossip_engine.store)
        w2_store_reports = len(worker_2.gossip_engine.store)
        b_store_reports = len(bootstrap_node.gossip_engine.store)

        log_print(f"   Worker 1 received gossip for Worker 2: {worker_1.gossip_engine.store.get(worker_2.peer_id) is not None}")
        log_print(f"   Worker 2 received gossip for Worker 1: {worker_2.gossip_engine.store.get(worker_1.peer_id) is not None}")
        log_print(f"   Gossip stores: Bootstrap={b_store_reports}, Worker1={w1_store_reports}, Worker2={w2_store_reports}")

        results["gossip_exchange_ok"] = gossip_propagated
        results["gossip_reports_w1"] = w1_store_reports
        results["gossip_reports_w2"] = w2_store_reports

        # Phase 5: Node Status Reporting
        log_print("\n5. Inspecting mesh status via NodeStatusAPI...")
        b_status = bootstrap_node.get_status()
        w1_status = worker_1.get_status()
        w2_status = worker_2.get_status()

        log_print("\n--- [Bootstrap Node Status Dashboard] ---")
        log_print(b_status.format_text())

        results["status_reporting_ok"] = bool(
            b_status.total_registered_peers >= 2
            and w1_status.total_registered_peers >= 2
            and w2_status.total_registered_peers >= 2
        )
        results["bootstrap_status_snapshot"] = b_status.to_dict()

        if as_json:
            print(b_status.to_json())

        # Phase 6: Graceful Departure & Shutdown
        log_print("\n6. Executing graceful departure of Worker 2 and orderly shutdown...")
        # Worker 2 announces departure to mesh peers
        await worker_2.leave_mesh(graceful=True)
        log_print(f"   [Worker Node 2] {worker_2.peer_id} broadcasted LEAVE announcement and stopped.")

        # Brief pause for LEAVE message dispatch and socket cleanup
        await asyncio.sleep(0.1)

        # Verify Worker 2 was uninstalled from remaining nodes' registries
        w2_in_b = bootstrap_node.peer_registry.has_peer(worker_2.peer_id)
        w2_in_w1 = worker_1.peer_registry.has_peer(worker_2.peer_id)
        log_print(f"   Bootstrap registry still contains Worker 2: {w2_in_b}")
        log_print(f"   Worker 1 registry still contains Worker 2: {w2_in_w1}")

        results["graceful_departure_ok"] = bool(not w2_in_b and not w2_in_w1 and not worker_2.is_running)

        # Stop remaining nodes
        await worker_1.stop()
        log_print(f"   [Worker Node 1] {worker_1.peer_id} stopped.")

        await bootstrap_node.stop()
        log_print(f"   [Bootstrap Node] {bootstrap_node.peer_id} stopped.")

        results["graceful_shutdown_ok"] = bool(
            results["graceful_departure_ok"]
            and not worker_1.is_running
            and not bootstrap_node.is_running
        )

        log_print("\n" + "=" * 80)
        log_print("Multi-Node Demonstration completed successfully!")
        log_print("=" * 80 + "\n")

    finally:
        # Safety cleanup in case of unexpected errors
        for n in nodes:
            if n.is_running:
                try:
                    await n.stop()
                except Exception:
                    pass

    return results


def main() -> int:
    """Command-line entry point for the multi-node demonstration."""
    parser = argparse.ArgumentParser(
        prog="multi_node.py",
        description="MeshWeaver Multi-Node Discovery and Gossip Demonstration (Milestone 05)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Loopback host interface to bind nodes")
    parser.add_argument("--config", default=None, help="Path to optional JSON configuration file")
    parser.add_argument("--json", action="store_true", help="Output node status snapshot as JSON")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")

    args = parser.parse_args()
    setup_logging(verbose=args.verbose, as_json=args.json)

    try:
        results = asyncio.run(
            run_multi_node_demo(
                host=args.host,
                config_path=args.config,
                as_json=args.json,
            )
        )
        return 0 if results.get("graceful_shutdown_ok", False) else 1
    except KeyboardInterrupt:
        print("\nMulti-node demonstration interrupted by user.")
        return 130
    except Exception as exc:
        logger.exception("Multi-node demonstration failed: %s", exc)
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

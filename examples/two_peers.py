"""
MeshWeaver: Two-Peer Communication Demonstration.

Demonstrates local peer-to-peer communication between two MeshWeaver nodes:
1. Starts two local nodes (Client Node and Worker Node) on dynamic TCP ports.
2. Performs an asynchronous connection and protocol handshake.
3. Exchanges a PING and PONG message with latency measurement.
4. Registers local task handlers on the Worker Node using TaskRegistry.
5. Sends validated TaskRequest messages, executes them, and receives TaskResponse results.
6. Demonstrates safe rejection of an unregistered task name.
7. Executes an orderly, graceful shutdown of all connections and peer nodes.

Usage:
    python examples/two_peers.py
    python examples/two_peers.py --verbose
    python examples/two_peers.py --host 127.0.0.1 --port-a 0 --port-b 0
"""

from __future__ import annotations

import argparse
import asyncio
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
    Message,
    MessageType,
    PeerConnection,
    PeerNode,
    TaskRegistry,
    TaskRequest,
    TaskResponse,
    TaskStatus,
)

logger = logging.getLogger("meshweaver.demo")


def setup_logging(verbose: bool = False) -> None:
    """Configure structured logging for the demonstration."""
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=log_level, format=log_format, datefmt="%H:%M:%S")


async def run_two_peers_demo(
    host: str = "127.0.0.1",
    port_a: int = 0,
    port_b: int = 0,
    quiet: bool = False,
) -> dict[str, Any]:
    """
    Run an end-to-end demonstration of two local MeshWeaver peers communicating.

    Args:
        host: Host IP to bind nodes to (default: '127.0.0.1').
        port_a: Port for Client Node (0 for dynamic port allocation).
        port_b: Port for Worker Node (0 for dynamic port allocation).
        quiet: If True, suppress console print statements (useful for tests).

    Returns:
        dict[str, Any]: Execution metrics and results from the demonstration.
    """

    def log_print(text: str = "") -> None:
        if not quiet:
            print(text)

    log_print("=" * 80)
    log_print("MeshWeaver: Two-Peer Communication Demonstration")
    log_print("=" * 80)

    # 1. Initialize Task Registry for Worker Node
    worker_registry = TaskRegistry()

    @worker_registry.register("add")
    def task_add(a: int | float, b: int | float) -> int | float:
        """Add two numbers together."""
        return a + b

    @worker_registry.register("reverse_string")
    def task_reverse_string(s: str) -> str:
        """Reverse an input string."""
        return s[::-1]

    @worker_registry.register("echo")
    def task_echo(*args: Any, **kwargs: Any) -> dict[str, Any]:
        """Echo arguments back to sender."""
        return {"args": list(args), "kwargs": dict(kwargs)}

    worker = PeerNode(peer_id="worker-node-1", host=host, port=port_b)
    client = PeerNode(peer_id="client-node-1", host=host, port=port_a)

    # Register TASK_REQUEST handler on Worker Node
    async def handle_task_request(msg: Message, _conn: PeerConnection) -> Message:
        logger.debug("[Worker] Received TASK_REQUEST from %s (msg_id: %s)", msg.sender_id, msg.message_id)
        task_req = TaskRequest.from_message(msg)
        task_resp = await worker_registry.execute(task_req)
        logger.debug(
            "[Worker] Completed task '%s' (ID: %s) with status: %s",
            task_req.task_name,
            task_req.task_id,
            task_resp.status.value,
        )
        return task_resp.to_message(
            sender_id=worker.peer_id,
            recipient_id=msg.sender_id,
            correlation_id=msg.message_id,
        )

    worker.register_handler(MessageType.TASK_REQUEST, handle_task_request)

    results: dict[str, Any] = {}
    conn: PeerConnection | None = None

    try:
        # 2. Start nodes
        log_print("\n1. Starting local nodes...")
        await worker.start()
        await client.start()

        log_print(f"   [Worker Node] ID: {worker.peer_id} | Bound to: {worker.host}:{worker.bound_port}")
        log_print(f"   [Client Node] ID: {client.peer_id} | Bound to: {client.host}:{client.bound_port}")
        results["worker_port"] = worker.bound_port
        results["client_port"] = client.bound_port

        # 3. Establish TCP connection and handshake
        log_print("\n2. Connecting Client Node to Worker Node and performing handshake...")
        conn = await client.connect_to_peer(host, worker.bound_port, timeout=5.0)
        log_print(f"   Handshake established: {client.peer_id} <-> {conn.remote_peer_id}")
        results["handshake_ok"] = conn.remote_peer_id == worker.peer_id

        # 4. Exchange PING / PONG
        log_print("\n3. Exchanging PING / PONG...")
        ping_payload = {"sender": client.peer_id, "ping_timestamp": time.time()}
        ping_msg = Message.create(MessageType.PING, sender_id=client.peer_id, payload=ping_payload)

        start_ping = time.perf_counter()
        pong_msg = await client.send_request(conn, ping_msg, timeout=5.0)
        ping_latency = time.perf_counter() - start_ping

        log_print(f"   Sent PING message (ID: {ping_msg.message_id})")
        log_print(f"   Received PONG from {pong_msg.sender_id} in {ping_latency * 1000:.2f} ms")
        log_print(f"   PONG payload echo: {pong_msg.payload.get('echo')}")

        results["ping_pong_ok"] = pong_msg.message_type == MessageType.PONG
        results["ping_latency_ms"] = ping_latency * 1000

        # 5. Send validated task requests
        log_print("\n4. Sending validated task requests...")

        # Task 1: add(100, 250)
        req_add = TaskRequest.create("add", args=[100, 250])
        msg_add = req_add.to_message(sender_id=client.peer_id)
        reply_add = await client.send_request(conn, msg_add, timeout=5.0)
        resp_add = TaskResponse.from_message(reply_add)

        log_print("   -> Task 'add'(100, 250):")
        log_print(f"      Result: {resp_add.result} (Status: {resp_add.status.value}, Time: {(resp_add.execution_time or 0.0) * 1000:.3f} ms)")
        results["task_add_result"] = resp_add.result
        results["task_add_status"] = resp_add.status.value

        # Task 2: reverse_string("MeshWeaver Decentralized")
        input_str = "MeshWeaver Decentralized"
        req_rev = TaskRequest.create("reverse_string", args=[input_str])
        msg_rev = req_rev.to_message(sender_id=client.peer_id)
        reply_rev = await client.send_request(conn, msg_rev, timeout=5.0)
        resp_rev = TaskResponse.from_message(reply_rev)

        log_print(f"   -> Task 'reverse_string'({input_str!r}):")
        log_print(f"      Result: {resp_rev.result!r} (Status: {resp_rev.status.value}, Time: {(resp_rev.execution_time or 0.0) * 1000:.3f} ms)")
        results["task_rev_result"] = resp_rev.result
        results["task_rev_status"] = resp_rev.status.value

        # Task 3: Unknown task rejection demonstration
        req_unknown = TaskRequest.create("unregistered_task", args=["sample_arg"])
        msg_unknown = req_unknown.to_message(sender_id=client.peer_id)
        reply_unknown = await client.send_request(conn, msg_unknown, timeout=5.0)
        resp_unknown = TaskResponse.from_message(reply_unknown)

        err_code = resp_unknown.error.code if resp_unknown.error else "NO_ERROR"
        err_msg = resp_unknown.error.message if resp_unknown.error else ""
        log_print("   -> Task 'unregistered_task' (Security Check):")
        log_print(f"      Safely rejected: Status={resp_unknown.status.value}, Code={err_code}")
        log_print(f"      Error details: {err_msg}")
        results["unknown_task_rejected"] = resp_unknown.status == TaskStatus.ERROR
        results["unknown_task_error_code"] = err_code

    finally:
        # 6. Orderly Shutdown
        log_print("\n5. Executing orderly shutdown...")
        if conn is not None and not conn.is_closed:
            await conn.close()
            log_print("   Connection closed.")

        await client.stop()
        log_print(f"   [Client Node] {client.peer_id} stopped.")

        await worker.stop()
        log_print(f"   [Worker Node] {worker.peer_id} stopped.")

        log_print("\n" + "=" * 80)
        log_print("Demonstration completed successfully!")
        log_print("=" * 80 + "\n")

    return results


def main() -> int:
    """Command-line entry point for the two-peer demonstration."""
    parser = argparse.ArgumentParser(
        description="MeshWeaver Two-Peer Communication Demonstration (Milestone 05)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host IP to bind peer nodes")
    parser.add_argument("--port-a", type=int, default=0, help="Port for Client Node (0 for dynamic)")
    parser.add_argument("--port-b", type=int, default=0, help="Port for Worker Node (0 for dynamic)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose debug logging")

    args = parser.parse_args()
    setup_logging(args.verbose)

    try:
        asyncio.run(run_two_peers_demo(host=args.host, port_a=args.port_a, port_b=args.port_b))
        return 0
    except KeyboardInterrupt:
        print("\nDemonstration interrupted by user.")
        return 130
    except Exception as exc:
        logger.exception("Demonstration failed: %s", exc)
        print(f"\nError: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

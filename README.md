# MeshWeaver: Zero-Dependency P2P Async Task Broker

[![Python](https://img.shields.io/badge/Python-3.11%2B-blue.svg)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Peer--to--Peer-purple.svg)](#architecture)
[![Status](https://img.shields.io/badge/Status-In%20Development-orange.svg)](#roadmap)

MeshWeaver is an educational Python project exploring decentralized task distribution across peer nodes. It aims to let nodes discover one another, exchange workload information, route tasks to suitable workers, and recover from peer failures without requiring a central task broker.

> **Project status:** In development. Features described as planned or in progress should not be considered implemented until they are built and tested in this repository.

## Why MeshWeaver?

Traditional distributed task queues commonly rely on central services such as Redis or RabbitMQ. Those tools are mature and useful, but they require additional infrastructure and introduce a central coordination dependency.

MeshWeaver explores a peer-to-peer alternative for learning and experimentation, especially in edge-computing scenarios where devices may have limited resources or intermittent connectivity.

## Goals

- Use Python's `asyncio` for non-blocking peer communication.
- Discover peers through a Kademlia-style Distributed Hash Table (DHT).
- Exchange CPU and memory load reports between nodes.
- Route tasks to an appropriate available peer.
- Serialize and validate task requests safely.
- Detect lost peers using heartbeats and handle failed tasks.
- Offer a CLI and a web dashboard for observing network and task state.
- Keep the core implementation modular, testable, and documented.

## Planned Architecture

```text
                 Mesh of peer nodes
        ┌────────────┐       ┌────────────┐
        │   Node A   │◄─────►│   Node B   │
        │ Discovery  │       │ Discovery  │
        │ Task API   │       │ Task API   │
        │ Load report│       │ Load report│
        └─────┬──────┘       └─────┬──────┘
              │                    │
              └─────────┬──────────┘
                        ▼
             Task routing and execution
                        │
               CLI / Web Dashboard
```

The diagram is conceptual. The actual network topology and implementation will evolve as features are completed.

## Main Components

| Component | Responsibility |
|---|---|
| Async networking | Non-blocking communication between peers using Python `asyncio` TCP primitives |
| Peer registry | Track known peers by node ID and validated network endpoint with state transitions and TTL expiration |
| Peer discovery | Find and maintain knowledge of other nodes using a DHT-inspired approach with 160-bit identifiers and XOR distance metric |
| Workload gossip | Share approximate CPU and memory utilization with neighboring peers |
| Task serialization | Define a structured, validated format for task requests and results |
| Task execution engine | Local asyncio-based task queue, worker pool, concurrency control, lifecycle state management (PENDING, RUNNING, COMPLETED, FAILED, CANCELLED), exception handling, and cancellation |
| Task routing | Select a suitable peer based on availability and reported load via `TaskRouter` and `LoadAwareRoutingStrategy` |
| Fault monitoring | Use heartbeats, timeouts, and automatic candidate failover to detect and recover from unresponsive peers |
| CLI | Start nodes, submit tasks, and inspect status |
| Web dashboard | Visualize peers, task states, and network activity |

### Kademlia Protocol Alignment and Mechanisms

MeshWeaver implements a modular DHT-inspired peer discovery layer that serves as the foundation for evolving toward the full Kademlia protocol. The table below documents exactly which Kademlia mechanisms are currently implemented and which are planned for future milestones:

| Kademlia Mechanism | Status | Details |
|---|---|---|
| **160-bit Keyspace** | **Implemented** | Fixed-length 160-bit `NodeID` (20 bytes) compatible with SHA-1 hashing of node identifiers and endpoints. |
| **XOR Distance Metric** | **Implemented** | Deterministic bitwise metric $d(x, y) = x \oplus y$ satisfying mathematical guarantees (identity, symmetry, strict positivity, triangle inequality, unidirectionality). |
| **Closest Peers Selection** | **Implemented** | Deterministic local selection of the $k$ closest known peers ordered strictly by XOR distance (`find_closest_peers` and `PeerRegistry.find_closest_peers`). |
| **Iterative Peer Lookup** | **Implemented** | Multi-round asynchronous iterative search converging toward a target key with concurrency control ($\alpha$) via `IterativeLookup` and `iterative_peer_lookup`. |
| **Duplicate Suppression** | **Implemented** | Tracking of candidate, queried, and unresponsive node IDs to ensure no peer is queried twice and cyclic graphs terminate cleanly. |
| **Bounded Queries** | **Implemented** | Hard bounds on total queries (`max_queries`), iterative rounds (`max_rounds`), and concurrency ($\alpha$) to prevent runaway query storms. |
| **Resilient Timeouts** | **Implemented** | Per-query timeout (`query_timeout`) isolating slow or crashed peers, plus overall lookup timeout (`overall_timeout`). |
| **Discovery Wire Protocol** | **Implemented** | `FIND_NODE` and `NODES_FOUND` message schemas and asynchronous handlers integrated into `PeerNode`. |
| **$k$-Bucket Routing Table** | **Implemented** | 160-bucket `RoutingTable` indexed by common prefix length (`cpl`), bounded by capacity $k$, with LRU contact ordering, replacement caching, and XOR distance closest-peer selection (`KBucket`, `RoutingTable`). |
| **Ping-Before-Evict Policy** | Educational / Planned | Educational implementation uses deterministic LRU ordering with replacement caching; active network ping-before-evict RPC hook documented for future background maintenance. |
| **Distributed Key-Value RPCs**| Planned | `STORE` and `FIND_VALUE` RPCs for decentralized content and task storage. |
| **Bucket Refresh Timers** | Planned | Automatic refresh lookups for stale buckets and periodic key republishing. |


## Technology Stack

- **Language:** Python 3.11+
- **Asynchronous runtime:** `asyncio`
- **Networking:** Python socket APIs; TCP/UDP as appropriate to the implemented protocol
- **Peer discovery:** Kademlia-style DHT concepts
- **Serialization:** Start with validated JSON-compatible task messages; only support executable code objects if a carefully restricted, trusted execution model is designed
- **Testing:** `pytest` and `pytest-asyncio` (planned)
- **Interface:** CLI first; web dashboard added in a later development phase

## Security Notes

MeshWeaver is a learning project, not a production-ready remote code execution platform. Never execute arbitrary code received from an untrusted peer. Python `pickle` can execute code during deserialization and must not be used with untrusted network input. Prefer explicit task types, validated JSON payloads, authentication, authorization, timeouts, resource limits, and encrypted transport before considering real-world deployment.

## Getting Started

Requirements: Python 3.11 or later and Git.

```bash
git clone https://github.com/<YOUR-GITHUB-USERNAME>/meshweaver-p2p-async-task-broker.git
cd meshweaver-p2p-async-task-broker

python -m venv .venv
```

Activate the environment:

**Windows PowerShell**
```powershell
.venv\Scripts\Activate.ps1
```

**macOS/Linux**
```bash
source .venv/bin/activate
```

Install development dependencies after the project defines them:

```bash
python -m pip install -U pip
pip install -e ".[dev]"
```

Run the test suite:

```bash
pytest
```

### Running the Local Task Execution Engine Demonstration

Demonstrate the standalone asynchronous task execution engine with an `asyncio` pending queue, configurable worker pool concurrency (`max_concurrency`), task lifecycle state transitions (`PENDING` -> `RUNNING` -> `COMPLETED`/`FAILED`/`CANCELLED`), exception boundaries, duration tracking, and orderly shutdown:

```bash
# Run local asynchronous task execution demonstration:
python examples/local_task_execution.py

# Run with custom worker concurrency:
python examples/local_task_execution.py --concurrency 4
```

### Local Asynchronous Task Execution Example

MeshWeaver's `TaskExecutionEngine` is decoupled from the networking layer, allowing developers to execute and test tasks locally using Python's standard `asyncio`:

```python
import asyncio
from meshweaver import TaskExecutionEngine, TaskState

async def main() -> None:
    # 1. Initialize engine with worker concurrency limit
    async with TaskExecutionEngine(max_concurrency=2) as engine:
        # 2. Register custom synchronous or asynchronous tasks
        @engine.register("multiply")
        async def multiply(a: int, b: int) -> int:
            await asyncio.sleep(0.01)
            return a * b

        # 3. Submit built-in example tasks and custom tasks
        task_add = await engine.submit("add", 15, 27)
        task_rev = await engine.submit("reverse_string", "MeshWeaver")
        task_mult = await engine.submit("multiply", 6, 7)

        # 4. Await task completion
        await asyncio.gather(task_add.wait(), task_rev.wait(), task_mult.wait())

        # 5. Inspect results, states, and duration
        print(f"Task ID: {task_add.task_id} -> Result: {task_add.result} [{task_add.state}] (took {task_add.duration:.4f}s)")
        print(f"Task ID: {task_rev.task_id} -> Result: {task_rev.result} [{task_rev.state}]")
        print(f"Task ID: {task_mult.task_id} -> Result: {task_mult.result} [{task_mult.state}]")

asyncio.run(main())
```

### Running the Two-Peer Demonstration

Demonstrate two local MeshWeaver nodes binding dynamic ports, completing an asynchronous handshake, exchanging a ping/pong message, and executing validated task requests (`add`, `reverse_string`) with orderly shutdown:

```bash
python examples/two_peers.py
```

Or run via the package CLI entry point:

```bash
python -m meshweaver --demo
```

Use `-v` or `--verbose` for detailed network framing and event logs:

```bash
python examples/two_peers.py --verbose
```

### Running the Multi-Node Discovery and Gossip Demonstration

Demonstrate three or more local MeshWeaver nodes binding dynamic loopback ports, completing bootstrap discovery via seed nodes, executing iterative DHT peer lookups, exchanging workload metrics via gossip, reporting unified node status, and executing graceful shutdown:

```bash
# Run multi-node mesh demonstration (Bootstrap + Worker 1 + Worker 2):
python examples/multi_node.py

# Run with custom JSON configuration:
python examples/multi_node.py --config examples/mesh_config.json

# Output structured JSON status snapshot:
python examples/multi_node.py --json

# Enable verbose debug logs:
python examples/multi_node.py --verbose

# Run via package CLI entrypoint:
python -m meshweaver --multi-node
python -m meshweaver demo --multi-node
```

### Example Mesh Configuration

The multi-node cluster can be configured via a JSON configuration file (e.g. `examples/mesh_config.json`):

```json
{
  "network": {
    "host": "127.0.0.1",
    "connection_timeout": 3.0,
    "request_timeout": 3.0
  },
  "gossip": {
    "interval": 0.2,
    "fanout": 2,
    "stale_threshold": 30.0,
    "ttl": 60.0,
    "max_hops": 3
  },
  "dht": {
    "alpha": 3,
    "k": 20,
    "query_timeout": 2.0,
    "overall_timeout": 4.0
  },
  "nodes": [
    { "peer_id": "bootstrap-node", "port": 0, "role": "bootstrap_seed", "is_seed": true },
    { "peer_id": "worker-node-1", "port": 0, "role": "compute_worker", "is_seed": false },
    { "peer_id": "worker-node-2", "port": 0, "role": "compute_worker", "is_seed": false }
  ]
}
```

### Inspecting Node and Peer Status

Inspect the local node and mesh peers directly from the CLI:

```bash
# Print formatted ASCII table dashboard:
python -m meshweaver status

# Emit clean JSON output:
python -m meshweaver status --json

# Query mesh status via bootstrap seed nodes:
python -m meshweaver status --seeds 127.0.0.1:8001
```

### Running the End-to-End Remote Task Execution Demonstration

Demonstrate multi-node task distribution, load-aware worker routing, strict execution boundaries, worker failover resilience, and graceful teardown:

```bash
# Run the remote task execution demonstration:
python examples/remote_tasks.py

# Run via package CLI entrypoint:
python -m meshweaver --tasks
python -m meshweaver demo --tasks

# Emit structured JSON demonstration results:
python examples/remote_tasks.py --json
python -m meshweaver --tasks --json

# Enable verbose debug logs:
python examples/remote_tasks.py --verbose

# Submit a task directly to a running mesh cluster:
python -m meshweaver submit add 125 375 --seeds 127.0.0.1:8001
python -m meshweaver submit reverse_string "MeshWeaver" --seeds 127.0.0.1:8001
```

## Roadmap

### Week 1 — Networking Foundation (Completed)

- [x] **Package structure and configuration:** Initialized zero-dependency `meshweaver` package with Python 3.11+ type hints and `pytest` configuration.
- [x] **Async message protocol:** Implemented length-prefixed binary framing, 10 MB frame limits, and JSON-serialized `Message` envelopes.
- [x] **Async peer networking:** Built `PeerNode` and `PeerConnection` with non-blocking stream handling, dynamic port binding, handshake negotiation, and correlated request-response matching.
- [x] **Validated task schemas & registry:** Created `TaskRequest`, `TaskResponse`, `TaskErrorInfo`, and `TaskRegistry` with strict identifier safety, 64 KB size limits, and code injection prevention.
- [x] **Asynchronous task execution engine:** Implemented standalone `TaskExecutionEngine` with an `asyncio`-based pending queue, worker pool with configurable concurrency (`max_concurrency`), strict lifecycle task states (`PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, `CANCELLED`), monotonic duration/timestamps, exception and error boundary handling, graceful shutdown, and local task cancellation.
- [x] **Two-peer communication demonstration:** Created `examples/two_peers.py` and integration tests demonstrating handshake, ping/pong exchange, local task execution, error handling, and orderly shutdown.

### Asynchronous Task Execution Engine

MeshWeaver's local task engine decouples task scheduling and execution from network transport:

| Mechanism | Description |
|---|---|
| **Unique Task IDs** | Every submitted task receives a unique validated identifier (`task_id`), either user-specified or automatically generated as a UUID hex. |
| **Asyncio Pending Queue** | Uncompleted tasks wait in an `asyncio.Queue` pending execution, decoupling task ingestion rate from worker execution speed. |
| **Strict Lifecycle States** | Tasks deterministically transition through `PENDING` -> `RUNNING` -> `COMPLETED`, `FAILED`, or `CANCELLED`. |
| **Worker Concurrency Control** | Background worker coroutines bounded by `max_concurrency` process tasks without thread-safety overhead or runaway concurrency. |
| **Security & Safety Boundaries** | Only functions explicitly registered in `TaskRegistry` can execute. Arbitrary code, unpickled objects, dunder keys, and dangerous names (`eval`, `exec`, `os`, `sys`) are blocked. |
| **Result & Error Isolation** | Synchronous and asynchronous exceptions are safely caught and encapsulated in `TaskErrorInfo` without crashing worker loops or leaking host internals. |
| **Timestamps & Durations** | Monotonic timestamps (`created_at`, `started_at`, `completed_at`) and duration calculations record sub-millisecond execution metrics. |
| **Graceful Shutdown & Cancellation** | Supports orderly shutdown (`shutdown(wait=True)`), immediate pending queue drainage (`cancel_pending=True`), and per-task cancellation (`cancel_task`). |

### Week 2 — Discovery and Workload Sharing (Completed)
- [x] **Dynamic peer registry:** Implemented `PeerRegistry`, `PeerRecord`, and `PeerEndpoint` with deterministic state transitions (`ACTIVE` -> `STALE` -> `EXPIRED`), collision handling, and TTL pruning.
- [x] **DHT-inspired peer discovery & routing table:** Implemented 160-bit `NodeID` keyspace, deterministic XOR distance metric, `KBucket` routing buckets with LRU ordering and replacement caching, 160-bucket `RoutingTable` partitioned by Common Prefix Length (CPL), closest-peer selection, duplicate suppression, bounded queries, and resilient iterative lookup with timeouts.
- [x] **Routing-table and DHT tests:** Added comprehensive unit and integration tests for XOR distance metric properties, bucket management (capacity, LRU ordering, updates, promotions), peer lookup, and unavailable/unresponsive peers.
- [x] **Gossip-based resource reporting:** Implemented asynchronous `GossipEngine`, `ResourceCollector`, `GossipStore`, and `NodeResourceReport` periodically sharing CPU, RAM, active task count, node identity, timestamp, and health status with bounded fanout peers.
- [x] **Workload gossip & anti-amplification:** Built strict monotonic report validation, clock skew rejection, stale record discarding, TTL expiration, and amplification bounds (`fanout`, `max_hops`, seen cache, and sender exclusion).
- [x] **Node status and registry API:** Integrated peer discovery, registry membership, and gossip records into a clean internal `NodeStatusAPI`, `NodeStatusSnapshot`, and `PeerStatus`. Exposes registered peers, last-seen timestamps, reported resource metrics, peer health, active socket connections, and DHT proximity. Added structured logging and CLI `status` command (`--json`, `--seeds`).
- [x] **Multi-node networking & gossip verification (Milestone 5):** Built automated integration tests and a deterministic loopback demonstration starting at least three MeshWeaver nodes. Verified bootstrap discovery, peer lookup, gossip exchange, node-status reporting, and graceful shutdown without Internet access. Added example configuration (`examples/mesh_config.json`) and documented run commands.
- [x] **End-to-end remote task execution & load-aware routing:** Implemented `TaskRouter`, `LoadAwareRoutingStrategy`, and `PeerNode.submit_task` integrating peer discovery, async TCP networking, workload gossip, and task serialization. Features composite load scoring (active tasks, CPU, memory, health status), automatic candidate failover retry, strict execution boundaries (only registered tasks execute, rejecting code/import injection), structured `TaskResponse` envelopes, and comprehensive multi-node integration tests.

### Load-Aware Task Routing and Execution

MeshWeaver provides an integrated decentralized task routing engine:

| Mechanism | Description |
|---|---|
| **Composite Load Scoring** | Evaluates active task count (weight: 10.0), CPU percent (weight: 0.5), memory percent (weight: 0.5), and health status penalties (`DEGRADED`: +50, `UNHEALTHY`: +1000). Ranks candidate worker peers in ascending order of load score. |
| **Deterministic Tie-Breaking** | Uses deterministic XOR distance between candidate node IDs and the task identifier anchor (`NodeID.from_peer(task_id)`) to break score ties evenly without hotspotting. |
| **Automatic Failover Retry** | If connection or transmission to the primary candidate peer fails or times out, `submit_task` automatically fails over to the next best candidate peer. |
| **Strict Execution Security** | Remote payloads can only execute explicitly registered handlers (`TaskRegistry`). Arbitrary Python imports, code objects, dunders, and reserved names (`eval`, `exec`, `os`, `sys`) are strictly rejected. |
| **Structured Task Envelopes** | Requests and responses are encapsulated in validated `TaskRequest` and `TaskResponse` objects carrying unique task IDs, status (`success`, `failure`, `error`), result or `TaskErrorInfo`, execution timestamps, and metadata. |
| **Observability & Inspection** | `TaskRouter.explain_routing()` emits structured breakdowns of evaluated peers, composite scores, active tasks, CPU/RAM, and health statuses for dashboards and logs. |

### Gossip-Based Workload & Resource Monitoring

MeshWeaver implements an asynchronous gossip engine for periodic workload and health status dissemination across neighboring peers:

| Mechanism | Description |
|---|---|
| **Resource Collection** | Samples CPU percent, memory percent, active task count, node identity, timestamp, and operational health status (`HEALTHY`, `DEGRADED`, `UNHEALTHY`). Uses standard Python facilities (`ctypes` on Windows, `/proc` on Linux, `os.getloadavg` on Unix) with an optional `psutil` dependency for richer hardware metrics. |
| **Monotonic Freshness** | Out-of-order, equal, or stale timestamps are rejected deterministically per node ID (`GossipStore`). Future timestamps exceeding clock skew tolerance (60s) are discarded. |
| **Stale Record Handling** | Configurable staleness threshold (30s) marks inactive peer records as stale, and time-to-live (`ttl=60s`) deterministically purges expired status reports. |
| **Bounded Fanout** | Periodic gossip rounds select at most `fanout` peers (default: 3) rather than broadcasting, avoiding $O(N^2)$ message explosions. |
| **Hop Limit & Anti-Amplification** | Reports carry a hop counter incremented on each relay; reports reaching `max_hops` (default: 3) are halted. Senders are excluded from relays, and a seen timestamp cache prevents cyclic re-transmission. |

### Node Status and Registry Inspection API

The node status subsystem unifies discovery, registry membership, connection state, and gossip metrics into an observable snapshot:

| Feature | Description |
|---|---|
| **`NodeStatusAPI`** | Core internal inspection service accessible via `node.status_api`, providing `get_node_status()`, `get_peer_status(peer_id)`, `list_peers()`, and `log_status()`. |
| **`PeerStatus`** | Unified snapshot for each peer exposing `endpoint`, `state` (`ACTIVE`, `STALE`, `EXPIRED`), `last_seen`, `last_seen_seconds_ago`, `cpu_percent`, `memory_percent`, `active_task_count`, `health_status`, `is_connected`, and `dht_distance`. |
| **Mesh Lifecycle Primitives** | `await node.join_mesh(seeds)` connects and handshakes with bootstrap nodes and discovers mesh peers; `await node.leave_mesh()` broadcasts an orderly `LEAVE` announcement and cleans up sockets. |
| **Structured Logging** | `setup_structured_logging` and `log_node_status` emit structured key-value messages with full snapshot payloads in `extra["node_status"]` for log aggregators. |
| **CLI Status Command** | `python -m meshweaver status` displays an ASCII table dashboard; `python -m meshweaver status --json` emits clean, parseable JSON; `--seeds` enables querying remote mesh status. |


### Week 3 — Routing and Reliability
- Add task submission and task lifecycle states.
- Route tasks based on available peer/load information.
- Add heartbeat monitoring, timeouts, and failure handling.
- Reassign eligible tasks after a peer becomes unavailable.
- Test routing and recovery scenarios.

### Week 4 — Security, CLI, and Dashboard
- Add authentication and secure transport where practical.
- Review message validation and execution boundaries.
- Build a CLI for node management and task submission.
- Add a web dashboard for peer/task monitoring.
- Finalize tests, documentation, and a reproducible demo.

## Development Principles

1. Implement one small, verifiable feature per commit.
2. Run relevant tests before committing.
3. Keep secrets, virtual environments, caches, and local data out of Git.
4. Never claim a feature works unless it has been implemented and tested.
5. Prefer small modules with clear responsibilities and type hints.
6. Document limitations and design decisions honestly.

## Example Commit Plan

Use 20 meaningful commits across four weeks. The actual commit messages should describe the change that was completed, not a feature that is only planned. A suggested sequence is:

- Week 1: repository setup, async protocol, peer client/server, message schema, tests.
- Week 2: peer registry, DHT primitives, load metrics, gossip messages, multi-node tests.
- Week 3: task lifecycle, routing policy, heartbeat monitor, retry/reassignment, reliability tests.
- Week 4: security hardening, CLI commands, dashboard API, dashboard UI, documentation/demo.

## Contributing

This is currently a personal learning project. If you experiment with it, create a branch, make a focused change, run tests, and document the behavior and limitations.

## Disclaimer

MeshWeaver is an educational prototype. It is not intended to replace mature task queues or to run untrusted remote code in production.

## License

Choose a license before publishing or accepting contributions. For example, MIT is a permissive option, but only select it if it matches your intentions.

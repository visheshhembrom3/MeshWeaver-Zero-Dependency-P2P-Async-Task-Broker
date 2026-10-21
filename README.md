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
| Task routing | Select a suitable peer based on availability and reported load |
| Fault monitoring | Use heartbeats and task timeouts to detect unresponsive peers |
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

## Roadmap

### Week 1 — Networking Foundation (Completed)

- [x] **Package structure and configuration:** Initialized zero-dependency `meshweaver` package with Python 3.11+ type hints and `pytest` configuration.
- [x] **Async message protocol:** Implemented length-prefixed binary framing, 10 MB frame limits, and JSON-serialized `Message` envelopes.
- [x] **Async peer networking:** Built `PeerNode` and `PeerConnection` with non-blocking stream handling, dynamic port binding, handshake negotiation, and correlated request-response matching.
- [x] **Validated task schemas & registry:** Created `TaskRequest`, `TaskResponse`, `TaskErrorInfo`, and `TaskRegistry` with strict identifier safety, 64 KB size limits, and code injection prevention.
- [x] **Two-peer communication demonstration:** Created `examples/two_peers.py` and integration tests demonstrating handshake, ping/pong exchange, local task execution, error handling, and orderly shutdown.

### Week 2 — Discovery and Workload Sharing (In Progress)
- [x] **Dynamic peer registry:** Implemented `PeerRegistry`, `PeerRecord`, and `PeerEndpoint` with deterministic state transitions (`ACTIVE` -> `STALE` -> `EXPIRED`), collision handling, and TTL pruning.
- [x] **DHT-inspired peer discovery & routing table:** Implemented 160-bit `NodeID` keyspace, deterministic XOR distance metric, `KBucket` routing buckets with LRU ordering and replacement caching, 160-bucket `RoutingTable` partitioned by Common Prefix Length (CPL), closest-peer selection, duplicate suppression, bounded queries, and resilient iterative lookup with timeouts.
- [x] **Routing-table and DHT tests:** Added comprehensive unit and integration tests for XOR distance metric properties, bucket management (capacity, LRU ordering, updates, promotions), peer lookup, and unavailable/unresponsive peers.
- [ ] Collect CPU/memory load metrics where supported.
- [ ] Exchange workload updates between peers.
- [ ] Run a multi-node local network experiment and document results.

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

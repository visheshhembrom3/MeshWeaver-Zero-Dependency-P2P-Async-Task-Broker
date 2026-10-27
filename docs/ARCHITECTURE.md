# MeshWeaver Architecture & Technical Specification

MeshWeaver is a zero-dependency, peer-to-peer asynchronous task broker implemented in Python 3.11+. It demonstrates decentralized peer discovery, workload telemetry propagation, load-aware task routing, and asynchronous task execution without centralized brokers such as Redis, RabbitMQ, or Celery.

---

## Table of Contents

1. [Executive Overview & Design Principles](#1-executive-overview--design-principles)
2. [High-Level Architecture](#2-high-level-architecture)
3. [Subsystem Breakdown](#3-subsystem-breakdown)
   - [3.1 Binary Framing & Network Protocol (`protocol.py`, `network.py`)](#31-binary-framing--network-protocol)
   - [3.2 Peer Identity, Endpoint & Registry (`peers.py`, `registry.py`)](#32-peer-identity-endpoint--registry)
   - [3.3 Kademlia-Inspired DHT Discovery (`dht.py`)](#33-kademlia-inspired-dht-discovery)
   - [3.4 Workload Telemetry & Gossip Engine (`gossip.py`)](#34-workload-telemetry--gossip-engine)
   - [3.5 Asynchronous Task Execution Engine (`execution.py`, `tasks.py`)](#35-asynchronous-task-execution-engine)
   - [3.6 Load-Aware Task Routing & Failover (`routing.py`)](#36-load-aware-task-routing--failover)
   - [3.7 Node Status & Observability API (`status.py`)](#37-node-status--observability-api)
   - [3.8 Command-Line Interface (`__main__.py`)](#38-command-line-interface)
4. [Wire Protocol & Message Framing Specification](#4-wire-protocol--message-framing-specification)
5. [Kademlia DHT Routing & Iterative Lookup Algorithm](#5-kademlia-dht-routing--iterative-lookup-algorithm)
6. [Gossip Dissemination & Anti-Amplification](#6-gossip-dissemination--anti-amplification)
7. [Task Lifecycle & Load-Aware Routing Flow](#7-task-lifecycle--load-aware-routing-flow)
8. [Security Boundaries & Threat Model](#8-security-boundaries--threat-model)
9. [Failure Modes & Fault Recovery](#9-failure-modes--fault-recovery)
10. [Verification & Test Architecture](#10-verification--test-architecture)

---

## 1. Executive Overview & Design Principles

Traditional distributed task distribution engines rely on a centralized broker for task queues, metadata coordination, and worker heartbeat management. MeshWeaver replaces central coordination with peer-to-peer networking, distributed hash table discovery, and epidemic gossip protocols.

### Core Design Principles

1. **Zero External Dependencies**: MeshWeaver operates entirely within the Python standard library (`asyncio`, `socket`, `hashlib`, `struct`, `json`, `uuid`, `time`, `logging`). An optional dependency (`psutil`) is supported solely for enhanced CPU and memory telemetry.
2. **Strict Safety & Execution Boundaries**: Remote code execution (RCE) via Python `pickle`, `eval`, dynamic imports, or arbitrary code compilation is prohibited. Only explicitly registered functions in a node's local `TaskRegistry` may be dispatched and executed.
3. **Deterministic Mathematical Primitives**: Peer addressing and topology utilize a 160-bit SHA-1 keyspace and the bitwise XOR distance metric ($d(x, y) = x \oplus y$), guaranteeing unidirectional distance properties and preventing routing loops.
4. **Resilient Failure Tolerance**: Network timeouts, crashed workers, and stale records are handled gracefully through automatic candidate failover retry, replacement caching in $k$-buckets, and monotonic gossip freshness pruning.
5. **Decoupled Asynchronous Core**: Networking, discovery, gossip, execution, and routing operate as independent concurrent `asyncio` coroutines communicating across structured boundaries.

---

## 2. High-Level Architecture

The MeshWeaver node architecture integrates seven distinct functional layers:

```
+-------------------------------------------------------------------------+
|                              MeshWeaver CLI                             |
|             (start | status | submit | demo --multi-node)               |
+-------------------------------------------------------------------------+
                                     |
+------------------------------------+------------------------------------+
|                         Node Status & Inspection                        |
|        (NodeStatusAPI, Snapshots, Structured Logs, ASCII Tables)        |
+------------------------------------+------------------------------------+
                                     |
+-------------------+----------------+----------------+-------------------+
|  Routing & Gossip | Task Execution | DHT Discovery  | Registry & Peers  |
| (TaskRouter,      | (TaskExecution | (RoutingTable, | (PeerRegistry,    |
|  LoadAwareStrategy|  Engine, Queue,|  KBucket,      |  PeerRecord,      |
|  GossipEngine)    |  Worker Pool)  |  IterativeLook)|  PeerEndpoint)    |
+-------------------+----------------+----------------+-------------------+
                                     |
+------------------------------------+------------------------------------+
|                         Peer Network Layer                              |
|           (PeerNode, PeerConnection, Async Stream Handlers)             |
+-------------------------------------------------------------------------+
                                     |
+-------------------------------------------------------------------------+
|                         Binary Protocol Layer                           |
|       (Length-Prefixed Framing: 4-byte BE length + JSON Envelope)       |
+-------------------------------------------------------------------------+
```

---

## 3. Subsystem Breakdown

### 3.1 Binary Framing & Network Protocol (`protocol.py`, `network.py`)

- **Length-Prefixed Binary Framing**: Sockets exchange messages serialized with a 4-byte big-endian unsigned integer prefix (`!I`) denoting the byte length of the payload, bounded by `MAX_FRAME_SIZE = 10,485,760` bytes (10 MB).
- **Correlated Request-Response RPC**: Each `Message` carries a unique `correlation_id` (UUID4 hex). Coroutines awaiting responses register temporary `asyncio.Future` handles keyed by `correlation_id` in `PeerConnection._pending_requests`.
- **Peer Connection Lifecycle**: Connections manage bidirectional `asyncio.StreamReader` and `asyncio.StreamWriter` streams. Socket disconnects cleanly resolve pending futures with `ConnectionClosedError`.

### 3.2 Peer Identity, Endpoint & Registry (`peers.py`, `registry.py`)

- **Peer Identifier (`peer_id`)**: A validated alphanumeric string matching `^[a-zA-Z0-9_-]{1,64}$`.
- **NodeID (`NodeID`)**: A 160-bit (20 bytes) cryptographic hash derived from `SHA-1(peer_id.encode('utf-8'))` or a direct 40-character hex string.
- **PeerRecord & Lifecycle Transitions**:
  - `ACTIVE`: Peer seen recently (`last_seen <= stale_threshold`).
  - `STALE`: Peer has not reported within `stale_threshold` (default: 30s).
  - `EXPIRED`: Peer has exceeded `ttl` (default: 60s) and is pruned by `prune_expired()`.

### 3.3 Kademlia-Inspired DHT Discovery (`dht.py`)

- **Routing Table (`RoutingTable`)**: 160 `KBucket` instances indexed by Common Prefix Length ($\text{cpl} \in [0, 159]$) between the host node and known peer contacts.
- **Bucket Management**: Each bucket holds up to $k=20$ contacts ordered by least-recently seen (LRU). When a bucket is full, newly seen peers are held in a `replacement_cache` (capacity $k$) to replace unresponsive peers.
- **Iterative Lookup (`IterativeLookup`)**: Asynchronous multi-round search for a target key with concurrency parameter $\alpha = 3$, bounded by `max_rounds = 10`, `max_queries = 50`, `query_timeout = 2.0s`, and `overall_timeout = 4.0s`.

### 3.4 Workload Telemetry & Gossip Engine (`gossip.py`)

- **Resource Telemetry (`ResourceCollector`)**: Collects instantaneous CPU percent, virtual memory percent, active task count, and assigns a health status (`HEALTHY`, `DEGRADED`, `UNHEALTHY`).
- **Gossip Dissemination (`GossipEngine`)**: Periodic background coroutine (`interval = 0.2s` in tests, default `2.0s`) selecting $\min(\text{fanout}, |\text{peers}|)$ random peers for transmission.
- **Anti-Amplification Controls**:
  - `max_hops = 3`: Incremented on relay; packets reaching maximum hops are dropped.
  - `seen_timestamps`: Cache prevents redundant relaying of identical reports.
  - Sender exclusion: A peer never echoes a gossip message back to its immediate sender.
  - Monotonic freshness: Out-of-order or stale timestamps are rejected by `GossipStore`.

### 3.5 Asynchronous Task Execution Engine (`execution.py`, `tasks.py`)

- **Standalone Execution Engine**: `TaskExecutionEngine` can operate standalone without networking.
- **Asynchronous Task Queue**: Tasks submitted to `submit()` are queued in an internal `asyncio.Queue` in `PENDING` state.
- **Worker Pool**: Configurable concurrency pool (`max_concurrency`) of background worker coroutines dequeue tasks and transition them to `RUNNING`.
- **Deterministic Lifecycle**:
  $$\text{PENDING} \longrightarrow \text{RUNNING} \longrightarrow \begin{cases} \text{COMPLETED} \\ \text{FAILED} \\ \text{CANCELLED} \end{cases}$$
- **Safety Boundaries**: Only functions pre-registered via `@engine.register("name")` or `TaskRegistry.register("name", func)` can execute. Built-in sample tasks include `add` and `reverse_string`.

### 3.6 Load-Aware Task Routing & Failover (`routing.py`)

- **Task Routing Engine (`TaskRouter`)**: Connects discovery, gossip telemetry, and execution into an intelligent distributed dispatcher.
- **Composite Load Scoring**: Evaluates candidate worker peers using weighted metrics:
  $$\text{Score}(P) = w_{\text{tasks}} \cdot \text{Tasks} + w_{\text{cpu}} \cdot \text{CPU}\% + w_{\text{mem}} \cdot \text{RAM}\% + \text{Penalty}(\text{Health})$$
  Default configuration:
  - $w_{\text{tasks}} = 10.0$
  - $w_{\text{cpu}} = 0.5$
  - $w_{\text{mem}} = 0.5$
  - $\text{Penalty}(\text{DEGRADED}) = +50.0$
  - $\text{Penalty}(\text{UNHEALTHY}) = +1000.0$
- **Deterministic Tie-Breaking**: When multiple peers report equal load scores, ties are broken deterministically by XOR distance:
  $$d_{\text{tie}}(P) = \text{XOR}(NodeID(P), NodeID(\text{task\_id}))$$
  This ensures balanced workload distribution without hotspotting.
- **Automatic Failover Retry**: When dispatching via `PeerNode.submit_task()`, candidate workers are ranked in ascending order of score. If connection or task dispatch to the top candidate fails, the client automatically attempts execution on subsequent candidates.

### 3.7 Node Status & Observability API (`status.py`)

- **Unified Status Snapshot (`NodeStatusAPI`)**: Combines local node configuration, active socket connections, registered peers, DHT routing table metrics, and gossip resource reports into `NodeStatusSnapshot`.
- **Presentation Formats**:
  - Human-readable ASCII table dashboard (`format_peer_status_table`).
  - Strict JSON export for automation and health checks (`NodeStatusSnapshot.to_dict()`).
  - Structured logging with embedded extra dictionaries for log aggregators.

### 3.8 Command-Line Interface (`__main__.py`)

Provides a unified CLI interface:
- `python -m meshweaver start`: Start a standalone seed or worker node.
- `python -m meshweaver status`: Query local or remote cluster state.
- `python -m meshweaver submit`: Dispatch a task across the mesh.
- `python -m meshweaver demo`: Run multi-node or two-peer demonstration clusters.

---

## 4. Wire Protocol & Message Framing Specification

MeshWeaver communication flows over length-prefixed binary frames carrying JSON-encoded UTF-8 envelopes.

### Frame Layout

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                  Payload Length (32-bit uint, BE)             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                                                               |
|                   JSON UTF-8 Encoded Payload                  |
|                    (Up to MAX_FRAME_SIZE = 10MB)              |
|                                                               |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### Envelope Structure (`Message`)

```json
{
  "message_type": "TASK_REQUEST",
  "sender_id": "client-node-01",
  "recipient_id": "worker-node-02",
  "correlation_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "timestamp": 1729864200.123456,
  "payload": {
    "task_id": "task-4f7c1e",
    "task_name": "add",
    "args": [10, 32],
    "kwargs": {},
    "timeout": 5.0
  }
}
```

### Supported Message Types

| Message Type | Direction | Description |
|---|---|---|
| `HANDSHAKE` | Initiator $\to$ Receiver | Initial connection setup carrying protocol version, node ID, and port. |
| `HANDSHAKE_ACK` | Receiver $\to$ Initiator | Handshake acknowledgment confirming peer identity and acceptance. |
| `PING` | Bidirectional | Liveness probe. |
| `PONG` | Bidirectional | Liveness acknowledgment matching the ping `correlation_id`. |
| `FIND_NODE` | Requester $\to$ DHT Peer | Request for the $k$ closest known peers to a target `NodeID`. |
| `NODES_FOUND` | DHT Peer $\to$ Requester | Response returning closest peer contacts. |
| `GOSSIP_RESOURCE` | Bidirectional | Workload telemetry broadcast containing CPU, RAM, and task metrics. |
| `TASK_REQUEST` | Client $\to$ Worker | Validated request to execute a named task. |
| `TASK_RESPONSE` | Worker $\to$ Client | Result or error info matching the task request `correlation_id`. |
| `LEAVE` | Node $\to$ Neighbors | Graceful departure announcement prior to socket disconnection. |

---

## 5. Kademlia DHT Routing & Iterative Lookup Algorithm

### XOR Distance Metric Properties

The distance between any two 160-bit node identifiers $x$ and $y$ is defined as:
$$d(x, y) = x \oplus y$$

This metric fulfills all four mathematical metric space axioms:
1. **Identity of Indiscernibles**: $d(x, y) = 0 \iff x = y$
2. **Symmetry**: $d(x, y) = d(y, x)$
3. **Strict Positivity**: $d(x, y) > 0 \quad \forall x \neq y$
4. **Triangle Inequality**: $d(x, z) \le d(x, y) \oplus d(y, z)$
5. **Unidirectionality**: For any given point $x$ and distance $\Delta > 0$, there exists exactly one point $y$ such that $d(x, y) = \Delta$.

### Common Prefix Length (CPL) Partitioning

The `RoutingTable` partitions known contacts into 160 buckets based on the number of leading identical bits:
$$\text{cpl}(x, y) = 160 - \lfloor \log_2(x \oplus y + 1) \rfloor$$

```
Target Keyspace (160 bits)
=============================================================
Bucket 0:   Prefix 0... (cpl = 0)   [Capacity: k=20 contacts]
Bucket 1:   Prefix 10... (cpl = 1)  [Capacity: k=20 contacts]
Bucket 2:   Prefix 110... (cpl = 2) [Capacity: k=20 contacts]
...
Bucket 159: Prefix 111... (cpl = 159)
=============================================================
```

### Iterative Lookup Sequence

```text
Client Node                      Seed / Alpha Peers                Discovered Peers
    |                                   |                                  |
    |-- FIND_NODE(Target) ------------->|                                  |
    |   (Concurrent query to alpha=3)   |                                  |
    |                                   |                                  |
    |<-- NODES_FOUND(k closest) --------|                                  |
    |                                                                      |
    |-- FIND_NODE(Target) ------------------------------------------------>|
    |   (Query closer unqueried contacts)                                  |
    |                                                                      |
    |<-- NODES_FOUND(k closest) -------------------------------------------|
    |                                                                      |
    v (Lookup terminates when no closer peers returned or bounds reached)
```

---

## 6. Gossip Dissemination & Anti-Amplification

The gossip protocol guarantees that workload state propagates to all connected peers within $O(\log N)$ rounds while preventing packet broadcast storms.

```text
           [Node A] (Generates Telemetry)
             /  \
  (Fanout F=2)  (Fanout F=2)
           v      v
      [Node B]  [Node C]
         |         |
      (Hop=1)   (Hop=1)
         v         v
      [Node D]  [Node E]
         |
      (Hop=2)
         v
     [Drop if Hop >= MAX_HOPS (3)]
```

### Anti-Amplification Guarantees

1. **Monotonic Clocks & Timestamps**: Telemetry reports carry float timestamps. A node discards incoming reports if $\text{timestamp} \le \text{last\_seen\_timestamp}$ for that peer.
2. **Clock Skew Bound**: Reports with $\text{timestamp} > \text{now} + 60\text{s}$ are rejected.
3. **Seen Cache**: MD5 hash signatures of `(peer_id, timestamp)` are cached to eliminate re-broadcasting identical telemetry packets.
4. **Sender Exclusion**: When Node B relays a report originated by Node A, Node B explicitly excludes the peer from which it received the frame.

---

## 7. Task Lifecycle & Load-Aware Routing Flow

### End-to-End Task Dispatch Sequence

```text
 Client (Submitter)               Router / Gossip              Selected Worker (Node B)
        |                                |                                |
        |-- 1. submit_task("add", 1, 2)  |                                |
        |   ---------------------------> |                                |
        |                                |-- 2. Query GossipStore         |
        |                                |   Rank candidates by score     |
        |                                |   Best: Node B (Score: 12.4)   |
        |                                |                                |
        |-- 3. Connect & Send TASK_REQUEST -----------------------------> |
        |                                                                 |-- 4. Check TaskRegistry
        |                                                                 |      Validate args
        |                                                                 |      Queue in TaskEngine
        |                                                                 |      Execute handler
        |                                                                 |
        |<-- 5. Send TASK_RESPONSE (Result: 3) -------------------------- |
        |
        v [If Node B fails/times out -> Step 3 retries with next best candidate]
```

---

## 8. Security Boundaries & Threat Model

MeshWeaver implements an intentional educational security architecture designed to prevent arbitrary code execution on host systems:

1. **Pre-Registered Tasks Only**: Only tasks explicitly registered in `TaskRegistry` via `@engine.register` or `registry.register` can execute.
2. **No Arbitrary Deserialization**: Python's `pickle` module is strictly banned across all components. Serialization is exclusively handled by `json.dumps()` and `json.loads()`.
3. **Identifier Sanitization**: Task names and peer identifiers must conform to strict regular expressions:
   - Task names: `^[a-zA-Z0-9_.-]{1,64}$`
   - Peer identifiers: `^[a-zA-Z0-9_-]{1,64}$`
4. **Reserved & Dangerous Word Rejection**: Identifiers containing `__`, `eval`, `exec`, `os`, `sys`, `builtins`, `subprocess`, `shutil`, `importlib`, or `lambda` are rejected immediately with `ValidationError`.
5. **Frame & Payload Bounds**:
   - `MAX_FRAME_SIZE = 10 MB` (Prevents memory exhaustion attacks via TCP streams).
   - `MAX_TASK_PAYLOAD_SIZE = 64 KB` (Limits task arguments to reasonable payloads).
   - `MAX_TASK_ARGS_COUNT = 100` (Guards against call-stack blowouts).
   - `MAX_TASK_KWARGS_COUNT = 100` (Guards against dictionary parameter poisoning).

---

## 9. Failure Modes & Fault Recovery

| Failure Scenario | Detection Mechanism | Recovery & Mitigation |
|---|---|---|
| **Worker Crash Before Execution** | Correlated RPC timeout (`TaskTimeoutError`) or TCP disconnect (`ConnectionClosedError`). | Client scheduler marks primary candidate unresponsive and fails over to secondary candidate worker. |
| **Worker Crash During Execution** | Socket disconnection mid-stream. | Disconnect detected; task error returned to caller. MeshWeaver enforces at-most-once semantics to prevent duplicate side effects. |
| **Peer Unresponsive in DHT Lookup** | Per-query timeout (`query_timeout = 2.0s`). | Node flagged as unresponsive in `IterativeLookup.unresponsive_nodes`; lookup proceeds with remaining candidates. |
| **Routing Table Bucket Saturation** | Bucket capacity $k=20$ reached. | New contact placed in `replacement_cache`. If an existing contact drops or is pruned, cached contact is promoted. |
| **Network Partition / Gossip Stale** | Peer report age exceeds `stale_threshold` (30s). | Peer marked `STALE`. If age exceeds `ttl` (60s), record is purged from `GossipStore` and `PeerRegistry`. |
| **Clock Desynchronization** | Future timestamp exceeding 60s tolerance. | Report discarded with `InvalidReportError`, preventing poisoned timestamps from freezing metrics. |

---

## 10. Verification & Test Architecture

MeshWeaver maintains a 100% automated test suite using `pytest` and `pytest-asyncio`:

- `tests/test_protocol.py`: Verifies binary encoding/decoding, framing limits, corrupted headers, and boundary conditions.
- `tests/test_network.py`: Tests TCP server binding, dynamic ports, handshake exchange, and request-response RPC correlation.
- `tests/test_peers.py`: Verifies `PeerRecord` lifecycle states, TTL pruning, and address validation.
- `tests/test_dht.py`: Verifies XOR distance metric axioms, bucket capacity, LRU replacement, and iterative lookup convergence.
- `tests/test_gossip.py`: Verifies resource collectors, gossip fanout, monotonic clock enforcement, hop limits, and health penalties.
- `tests/test_execution.py`: Verifies task engine pending queues, worker pool concurrency limits, cancellation, and error isolation.
- `tests/test_tasks.py`: Verifies task serialization, parameter limits, and injection prevention.
- `tests/test_status.py`: Verifies `NodeStatusAPI` snapshot generation and CLI formatting.
- `tests/test_remote_tasks.py`: End-to-end integration tests for multi-node task distribution, load-aware routing, and failover retries.
- `tests/test_multi_node.py`: Multi-node cluster verification testing discovery, gossip synchronization, and orderly shutdown.
- `tests/test_two_peers_example.py`: Regression verification for the two-node communication demonstration script.

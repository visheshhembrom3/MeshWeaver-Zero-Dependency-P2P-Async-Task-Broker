MeshWeaver: Zero-Dependency P2P Async Task Broker
� � �
MeshWeaver is an educational Python project exploring decentralized task distribution across peer nodes. It aims to let nodes discover one another, exchange workload information, route tasks to suitable workers, and recover from peer failures without requiring a central task broker.
Project status: In development. Features described as planned or in progress should not be considered implemented until they are built and tested in this repository.
Why MeshWeaver?
Traditional distributed task queues commonly rely on central services such as Redis or RabbitMQ. Those tools are mature and useful, but they require additional infrastructure and introduce a central coordination dependency.
MeshWeaver explores a peer-to-peer alternative for learning and experimentation, especially in edge-computing scenarios where devices may have limited resources or intermittent connectivity.
Goals
Use Python's asyncio for non-blocking peer communication.
Discover peers through a Kademlia-style Distributed Hash Table (DHT).
Exchange CPU and memory load reports between nodes.
Route tasks to an appropriate available peer.
Serialize and validate task requests safely.
Detect lost peers using heartbeats and handle failed tasks.
Offer a CLI and a web dashboard for observing network and task state.
Keep the core implementation modular, testable, and documented.
Planned Architecture
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
The diagram is conceptual. The actual network topology and implementation will evolve as features are completed.
Main Components
Component
Responsibility
Async networking
Non-blocking communication between peers using Python networking primitives
Peer discovery
Find and maintain knowledge of other nodes using a DHT-inspired approach
Workload gossip
Share approximate CPU and memory utilization with neighboring peers
Task serialization
Define a structured, validated format for task requests and results
Task routing
Select a suitable peer based on availability and reported load
Fault monitoring
Use heartbeats and task timeouts to detect unresponsive peers
CLI
Start nodes, submit tasks, and inspect status
Web dashboard
Visualize peers, task states, and network activity
Technology Stack
Language: Python 3.11+
Asynchronous runtime: asyncio
Networking: Python socket APIs; TCP/UDP as appropriate to the implemented protocol
Peer discovery: Kademlia-style DHT concepts
Serialization: Start with validated JSON-compatible task messages; only support executable code objects if a carefully restricted, trusted execution model is designed
Testing: pytest and pytest-asyncio (planned)
Interface: CLI first; web dashboard added in a later development phase
Security Notes
MeshWeaver is a learning project, not a production-ready remote code execution platform. Never execute arbitrary code received from an untrusted peer. Python pickle can execute code during deserialization and must not be used with untrusted network input. Prefer explicit task types, validated JSON payloads, authentication, authorization, timeouts, resource limits, and encrypted transport before considering real-world deployment.
Getting Started
Requirements: Python 3.11 or later and Git.
git clone https://github.com/<YOUR-GITHUB-USERNAME>/meshweaver-p2p-async-task-broker.git
cd meshweaver-p2p-async-task-broker

python -m venv .venv
Activate the environment:
Windows PowerShell
.venv\Scripts\Activate.ps1
macOS/Linux
source .venv/bin/activate
Install development dependencies after the project defines them:
python -m pip install -U pip
pip install -e ".[dev]"
Run tests once the test suite is available:
pytest
Run the CLI using the entry point documented by the implementation. Commands may change while the project is in development; check --help after the CLI is implemented.
Roadmap
Week 1 — Networking Foundation
Create the package structure and project configuration.
Implement a basic asynchronous peer server/client.
Define structured messages and task serialization.
Add input validation, logging, and unit tests.
Demonstrate two local nodes communicating.
Week 2 — Discovery and Workload Sharing
Implement a Kademlia-style peer-discovery layer.
Add peer registry and routing-table tests.
Collect CPU/memory load metrics where supported.
Exchange workload updates between peers.
Run a multi-node local network experiment and document results.
Week 3 — Routing and Reliability
Add task submission and task lifecycle states.
Route tasks based on available peer/load information.
Add heartbeat monitoring, timeouts, and failure handling.
Reassign eligible tasks after a peer becomes unavailable.
Test routing and recovery scenarios.
Week 4 — Security, CLI, and Dashboard
Add authentication and secure transport where practical.
Review message validation and execution boundaries.
Build a CLI for node management and task submission.
Add a web dashboard for peer/task monitoring.
Finalize tests, documentation, and a reproducible demo.
Development Principles
Implement one small, verifiable feature per commit.
Run relevant tests before committing.
Keep secrets, virtual environments, caches, and local data out of Git.
Never claim a feature works unless it has been implemented and tested.
Prefer small modules with clear responsibilities and type hints.
Document limitations and design decisions honestly.
Example Commit Plan
Use 20 meaningful commits across four weeks. The actual commit messages should describe the change that was completed, not a feature that is only planned. A suggested sequence is:
Week 1: repository setup, async protocol, peer client/server, message schema, tests.
Week 2: peer registry, DHT primitives, load metrics, gossip messages, multi-node tests.
Week 3: task lifecycle, routing policy, heartbeat monitor, retry/reassignment, reliability tests.
Week 4: security hardening, CLI commands, dashboard API, dashboard UI, documentation/demo.
Contributing
This is currently a personal learning project. If you experiment with it, create a branch, make a focused change, run tests, and document the behavior and limitations.
Disclaimer
MeshWeaver is an educational prototype. It is not intended to replace mature task queues or to run untrusted remote code in production.
License
Choose a license before publishing or accepting contributions. For example, MIT is a permissive option, but only select it if it matches your intentions.
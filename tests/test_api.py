"""
Integration tests for MeshWeaver FastAPI and WebSocket adapter.
"""

from __future__ import annotations

import asyncio
import httpx
import pytest
from starlette.testclient import TestClient

from meshweaver.api import create_api_app
from meshweaver.events import EventManager
from meshweaver.network import PeerNode


@pytest.fixture
async def running_node():
    """Create and start a lightweight local PeerNode for testing."""
    node = PeerNode(host="127.0.0.1", port=0, peer_id="api-test-node")
    node.enable_gossip()
    await node.start()
    try:
        yield node
    finally:
        await node.stop()


@pytest.fixture
async def async_client(running_node):
    """Create an AsyncClient on the exact same event loop as running_node."""
    events = EventManager()
    app = create_api_app(running_node, event_manager=events)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_api_health(async_client, running_node):
    """Verify health endpoint returns node status and identifiers."""
    response = await async_client.get("/api/health")
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == running_node.peer_id
    assert data["status"] == "ok"
    assert "bound_port" in data
    assert "timestamp" in data


@pytest.mark.asyncio
async def test_api_node_status(async_client, running_node):
    """Verify node status endpoint returns full snapshot and registered tasks."""
    response = await async_client.get("/api/node")
    assert response.status_code == 200
    data = response.json()
    assert data["node_id"] == running_node.peer_id
    assert "registered_tasks" in data
    assert "add" in data["registered_tasks"]
    assert "ping" in data["registered_tasks"]
    assert "uptime_seconds" in data


@pytest.mark.asyncio
async def test_api_peers_listing(async_client, running_node):
    """Verify peers list and peer detail endpoints."""
    # Initially no remote peers
    response = await async_client.get("/api/peers")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

    # Unknown peer should return 404
    p_resp = await async_client.get("/api/peers/non-existent-peer")
    assert p_resp.status_code == 404

    # Register a peer and verify it appears
    running_node.peer_registry.register("peer-worker-1", "127.0.0.1", 9001)
    p_list_resp = await async_client.get("/api/peers")
    p_list = p_list_resp.json()
    assert any(p["peer_id"] == "peer-worker-1" for p in p_list)

    # Query single peer detail
    p_detail = await async_client.get("/api/peers/peer-worker-1")
    assert p_detail.status_code == 200
    assert p_detail.json()["peer_id"] == "peer-worker-1"


@pytest.mark.asyncio
async def test_api_tasks_available(async_client):
    """Verify list of registered allowlisted task functions."""
    response = await async_client.get("/api/tasks/available")
    assert response.status_code == 200
    data = response.json()
    task_names = [t["name"] for t in data]
    assert "add" in task_names
    assert "ping" in task_names
    assert "echo" in task_names
    assert "reverse_string" in task_names


@pytest.mark.asyncio
async def test_api_task_submission_and_retrieval(async_client):
    """Test task submission, local execution, and status retrieval."""
    payload = {
        "task_name": "add",
        "args": [14, 28],
        "kwargs": {},
        "timeout": 5.0,
    }
    sub_resp = await async_client.post("/api/tasks", json=payload)
    assert sub_resp.status_code == 200
    res_data = sub_resp.json()
    assert res_data["status"] == "success"
    assert res_data["result"] == 42
    task_id = res_data["task_id"]

    # Retrieve tasks list
    t_list_resp = await async_client.get("/api/tasks")
    t_list = t_list_resp.json()
    assert any(t["task_id"] == task_id for t in t_list)

    # Retrieve task detail
    t_detail = await async_client.get(f"/api/tasks/{task_id}")
    assert t_detail.status_code == 200
    assert t_detail.json()["result"] == 42


@pytest.mark.asyncio
async def test_api_task_submission_rejects_unallowlisted(async_client):
    """Verify security constraint: unallowlisted tasks are rejected with HTTP 400."""
    payload = {
        "task_name": "malicious_eval",
        "args": ["__import__('os').system('ls')"],
    }
    response = await async_client.post("/api/tasks", json=payload)
    assert response.status_code == 400
    assert "not allowed" in response.json()["detail"].lower()


@pytest.mark.asyncio
async def test_api_metrics(async_client, running_node):
    """Verify aggregate system and task execution metrics."""
    response = await async_client.get("/api/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "local_node" in data
    assert "tasks" in data
    assert "peers" in data
    assert "peer_loads" in data
    assert data["local_node"]["node_id"] == running_node.peer_id


@pytest.mark.asyncio
async def test_api_events_logging(async_client):
    """Verify event recording and retrieval."""
    response = await async_client.get("/api/events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)


@pytest.mark.asyncio
async def test_api_serves_frontend_spa(async_client):
    """Verify frontend static single-page app is mounted and served at root /."""
    response = await async_client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "<div id=\"root\">" in response.text or "<title>" in response.text


@pytest.mark.asyncio
async def test_overview_and_peers_real_data_and_error_handling(async_client, running_node):
    """Verify real data integration for Overview and Peers pages with zero fabricated metrics."""
    # 1. Zero state when node is isolated: no peers, no tasks
    peers_resp = await async_client.get("/api/peers")
    assert peers_resp.status_code == 200
    assert peers_resp.json() == []

    tasks_resp = await async_client.get("/api/tasks")
    assert tasks_resp.status_code == 200
    assert tasks_resp.json() == []

    metrics_resp = await async_client.get("/api/metrics")
    assert metrics_resp.status_code == 200
    metrics_data = metrics_resp.json()
    assert metrics_data["tasks"]["total"] == 0
    assert metrics_data["peers"]["total"] == 0
    assert len(metrics_data["peer_loads"]) == 1  # only local node
    assert metrics_data["peer_loads"][0]["peer_id"] == f"{running_node.peer_id} (Local)"

    # 2. Add real peer with partial metadata and verify NO metric fabrication
    running_node.peer_registry.register(
        "worker-node-real",
        "127.0.0.1",
        8899,
        metadata={"supported_tasks": ["add", "ping"]},
    )

    peers_updated = (await async_client.get("/api/peers")).json()
    assert len(peers_updated) == 1
    peer_item = peers_updated[0]
    assert peer_item["peer_id"] == "worker-node-real"
    assert peer_item["endpoint"]["host"] == "127.0.0.1"
    assert peer_item["endpoint"]["port"] == 8899
    assert peer_item["supported_tasks"] == ["add", "ping"]
    # CPU/RAM should be null (NOT fabricated) because no gossip report arrived yet
    assert peer_item["cpu_percent"] is None
    assert peer_item["memory_percent"] is None
    assert peer_item["last_latency_ms"] is None

    # 3. Verify 404 error handling on missing peer
    err_peer = await async_client.get("/api/peers/unknown-peer-xyz")
    assert err_peer.status_code == 404
    assert "not found" in err_peer.json()["detail"].lower()

    # 4. Verify 404 error handling on missing task
    err_task = await async_client.get("/api/tasks/unknown-task-xyz")
    assert err_task.status_code == 404
    assert "not found" in err_task.json()["detail"].lower()


@pytest.mark.asyncio
async def test_multi_node_api_routing_and_gossip_integration():
    """Verify live multi-peer mesh integration through FastAPI adapter."""
    # 1. Start worker node
    worker = PeerNode(host="127.0.0.1", port=0, peer_id="worker-node-beta")
    worker.enable_gossip(interval=0.2)
    await worker.start()

    # 2. Start coordinator node
    coordinator = PeerNode(host="127.0.0.1", port=0, peer_id="coord-node-alpha")
    coordinator.enable_gossip(interval=0.2)
    coordinator.enable_heartbeats(interval=0.5)
    await coordinator.start()

    try:
        events = EventManager()
        app = create_api_app(coordinator, event_manager=events)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # Connect coordinator to worker
            await coordinator.connect_to_peer(worker.host, worker.bound_port, timeout=3.0)
            await asyncio.sleep(0.3)

            # Query /api/peers: worker should be active and connected
            peers_resp = await client.get("/api/peers")
            assert peers_resp.status_code == 200
            peer_list = peers_resp.json()
            assert any(p["peer_id"] == "worker-node-beta" for p in peer_list)

            # Submit task: should be routed to worker-node-beta
            sub_resp = await client.post(
                "/api/tasks",
                json={"task_name": "add", "args": [100, 250], "timeout": 5.0},
            )
            assert sub_resp.status_code == 200
            task_res = sub_resp.json()
            assert task_res["status"] == "success", f"Task submission failed: {task_res.get('error')}"
            assert task_res["result"] == 350
            assert task_res["task_id"]

            # Query /api/tasks: should record execution duration and worker
            tasks_resp = await client.get("/api/tasks")
            assert tasks_resp.status_code == 200
            tasks_list = tasks_resp.json()
            matching = [t for t in tasks_list if t["task_id"] == task_res["task_id"]]
            assert len(matching) == 1
            assert matching[0]["result"] == 350
            assert matching[0]["status"] == "success"

            # Query /api/events: should have task_submitted and task_completed
            events_resp = await client.get("/api/events")
            assert events_resp.status_code == 200
            events_data = events_resp.json()
            event_types = [e["event_type"] for e in events_data]
            assert "task_submitted" in event_types
            assert "task_completed" in event_types
    finally:
        await coordinator.stop()
        await worker.stop()


def test_api_websocket_connection():
    """Verify WebSocket live feed connects and emits initial handshake message."""
    node = PeerNode(host="127.0.0.1", port=0, peer_id="ws-test-node")
    events = EventManager()
    app = create_api_app(node, event_manager=events)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/events") as websocket:
            init_data = websocket.receive_json()
            assert init_data["type"] == "connected"
            assert init_data["node_id"] == node.peer_id


@pytest.mark.asyncio
async def test_api_privileged_authentication():
    """Verify API key authentication for privileged task submission."""
    node = PeerNode(host="127.0.0.1", port=0, peer_id="auth-test-node")
    await node.start()
    try:
        events = EventManager()
        app = create_api_app(node, event_manager=events, api_key="secret-mesh-key-123")
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. Unauthenticated request to /api/health should succeed (public)
            health_resp = await client.get("/api/health")
            assert health_resp.status_code == 200

            # 2. Unauthenticated task submission should fail with 401
            unauth_resp = await client.post(
                "/api/tasks",
                json={"task_name": "ping", "args": []},
            )
            assert unauth_resp.status_code == 401
            assert "unauthorized" in unauth_resp.json()["detail"].lower()

            # 3. Invalid API key should fail with 401
            bad_key_resp = await client.post(
                "/api/tasks",
                json={"task_name": "ping", "args": []},
                headers={"X-API-Key": "wrong-key"},
            )
            assert bad_key_resp.status_code == 401

            # 4. Valid X-API-Key header should succeed
            valid_x_resp = await client.post(
                "/api/tasks",
                json={"task_name": "ping", "args": []},
                headers={"X-API-Key": "secret-mesh-key-123"},
            )
            assert valid_x_resp.status_code == 200
            assert valid_x_resp.json()["result"] == "pong"

            # 5. Valid Authorization: Bearer <key> header should succeed
            valid_bearer_resp = await client.post(
                "/api/tasks",
                json={"task_name": "ping", "args": []},
                headers={"Authorization": "Bearer secret-mesh-key-123"},
            )
            assert valid_bearer_resp.status_code == 200
            assert valid_bearer_resp.json()["result"] == "pong"
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_api_task_payload_size_and_limits(async_client):
    """Verify enforcement of argument length limits and validation."""
    # Oversized string argument (> 64 KB)
    huge_str = "A" * 70000
    oversized_resp = await async_client.post(
        "/api/tasks",
        json={"task_name": "reverse_string", "args": [huge_str]},
    )
    assert oversized_resp.status_code == 413
    assert "exceeds maximum length" in oversized_resp.json()["detail"]


@pytest.mark.asyncio
async def test_api_config_endpoint(async_client, running_node):
    """Verify /api/config exposes supported settings and restart requirements."""
    resp = await async_client.get("/api/config")
    assert resp.status_code == 200
    cfg = resp.json()
    assert "node_identity" in cfg
    assert cfg["node_identity"]["node_id"] == running_node.peer_id
    assert cfg["node_identity"]["requires_restart"] is True
    assert "network" in cfg
    assert cfg["network"]["requires_restart"] is True
    assert "heartbeat" in cfg
    assert cfg["heartbeat"]["requires_restart"] is True
    assert "gossip" in cfg
    assert cfg["gossip"]["requires_restart"] is True
    assert "execution" in cfg
    assert "security" in cfg
    assert cfg["security"]["tls_enabled"] is False
    assert "runtime_metadata" in cfg


export type PeerLifecycleState = 'active' | 'stale' | 'expired' | 'unresponsive';
export type PeerHealthStatus = 'healthy' | 'degraded' | 'critical' | 'unhealthy' | 'unknown';
export type TaskState = 'pending' | 'running' | 'success' | 'failure' | 'error' | 'timeout' | 'cancelled';
export type EventSeverity = 'info' | 'success' | 'warning' | 'error';

export interface PeerEndpoint {
  host: string;
  port: number;
}

export interface PeerStatus {
  peer_id: string;
  endpoint: PeerEndpoint;
  state: PeerLifecycleState;
  last_seen: number;
  last_seen_seconds_ago: number;
  registered_at: number;
  is_connected: boolean;
  is_inbound: boolean | null;
  dht_distance: number | null;
  dht_bucket_index: number | null;
  cpu_percent: number | null;
  memory_percent: number | null;
  active_task_count: number;
  health_status: PeerHealthStatus;
  missed_heartbeats?: number;
  last_latency_ms?: number | null;
  is_unresponsive?: boolean;
  supported_tasks?: string[];
  metadata?: Record<string, unknown>;
}

export interface NodeStatus {
  node_id: string;
  host: string;
  port: number;
  bound_port: number;
  dht_id_hex: string;
  is_running: boolean;
  health_status: PeerHealthStatus;
  cpu_percent: number | null;
  memory_percent: number | null;
  active_task_count: number;
  active_connections_count: number;
  total_registered_peers: number;
  active_peers_count: number;
  stale_peers_count: number;
  expired_peers_count: number;
  routing_table_contacts: number;
  gossip_reports_count: number;
  peers: PeerStatus[];
  registered_tasks?: string[];
  max_concurrency?: number;
  uptime_seconds?: number;
  gossip_enabled?: boolean;
  heartbeat_enabled?: boolean;
  heartbeat_interval?: number;
  missed_threshold?: number;
  timestamp: number;
}

export interface TaskErrorInfo {
  code: string;
  message: string;
  details?: unknown;
}

export interface TaskOwnership {
  task_id: string;
  owner_id: string;
  assigned_worker_id: string | null;
  retry_count: number;
  max_retries: number;
  status: string;
  created_at: number;
  last_assigned_at?: number | null;
  completed_at?: number | null;
  worker_history: string[];
  error_history: string[];
  execution_semantics?: string;
}

export interface TaskTransition {
  state: string;
  timestamp: number;
  label: string;
}

export interface TaskRecord {
  task_id: string;
  correlation_id?: string;
  task_name: string;
  status: TaskState;
  routed_to?: string | null;
  timestamp?: number;
  completed_at?: number | null;
  result?: unknown;
  error?: TaskErrorInfo | null;
  execution_time?: number | null;
  retry_count?: number;
  max_retries?: number;
  worker_history?: string[];
  ownership?: TaskOwnership | null;
  ack?: Record<string, unknown> | null;
  transitions?: TaskTransition[];
}

export interface ConfigItem {
  requires_restart: boolean;
  description: string;
  [key: string]: unknown;
}

export interface NodeConfigInfo {
  node_identity: {
    node_id: string;
    dht_id_hex: string;
    requires_restart: boolean;
    description: string;
  };
  network: {
    host: string;
    port: number;
    bound_port: number;
    requires_restart: boolean;
    description: string;
  };
  heartbeat: {
    enabled: boolean;
    interval_seconds: number;
    missed_threshold: number;
    requires_restart: boolean;
    description: string;
  };
  gossip: {
    enabled: boolean;
    interval_seconds: number | null;
    fanout: number | null;
    requires_restart: boolean;
    description: string;
  };
  execution: {
    max_concurrency: number;
    registered_tasks: string[];
    requires_restart: boolean;
    description: string;
  };
  security: {
    tls_enabled: boolean;
    mTLS_required: boolean;
    has_pre_shared_secret: boolean;
    has_auth_token: boolean;
    allowed_peers_count: number;
    api_key_auth_enabled: boolean;
    requires_restart: boolean;
    description: string;
  };
  runtime_metadata: {
    uptime_seconds: number;
    is_running: boolean;
    api_auth_required_for_tasks: boolean;
  };
}

export type ThemeMode = 'light' | 'dark' | 'system';

export interface TaskAvailable {
  name: string;
  description: string;
  is_allowlisted: boolean;
}

export interface TaskSubmitPayload {
  task_name: string;
  args?: unknown[];
  kwargs?: Record<string, unknown>;
  timeout?: number;
  require_ack?: boolean;
}

export interface PeerLoadMetric {
  peer_id: string;
  cpu_percent: number;
  memory_percent: number;
  active_tasks: number;
  latency_ms: number | null;
  health: string;
  is_active: boolean;
}

export interface SystemMetrics {
  local_node: {
    node_id: string;
    cpu_percent: number | null;
    memory_percent: number | null;
    active_tasks: number;
    health: string;
  };
  tasks: {
    total: number;
    pending: number;
    running: number;
    completed: number;
    failed: number;
    success_rate: number;
    avg_execution_time: number;
  };
  peers: {
    total: number;
    active: number;
    unresponsive: number;
    stale: number;
    expired: number;
    active_connections: number;
  };
  peer_loads: PeerLoadMetric[];
  timestamp: number;
}

export interface MeshEvent {
  id: string;
  timestamp: number;
  event_type: string;
  severity: EventSeverity;
  peer_id?: string | null;
  task_id?: string | null;
  message: string;
  details?: Record<string, unknown>;
}

export interface HealthInfo {
  status: string;
  node_id: string;
  host: string;
  port: number;
  bound_port: number;
  uptime_seconds: number;
  is_running: boolean;
  dht_id_hex: string;
  timestamp: number;
}


import type {
  HealthInfo,
  MeshEvent,
  NodeStatus,
  PeerStatus,
  SystemMetrics,
  TaskAvailable,
  TaskRecord,
  TaskSubmitPayload,
} from './types';

export const getApiBaseUrl = (): string => {
  const custom = localStorage.getItem('meshweaver_api_url');
  if (custom) return custom.replace(/\/+$/, '');
  // Default to relative /api when proxied, or fallback to current origin
  return '';
};

export const setApiBaseUrl = (url: string): void => {
  if (!url || url.trim() === '') {
    localStorage.removeItem('meshweaver_api_url');
  } else {
    localStorage.setItem('meshweaver_api_url', url.trim().replace(/\/+$/, ''));
  }
};

const handleResponse = async <T>(res: Response): Promise<T> => {
  if (!res.ok) {
    let errorDetail = `HTTP ${res.status}: ${res.statusText}`;
    try {
      const errJson = await res.json();
      if (errJson && errJson.detail) {
        errorDetail = errJson.detail;
      }
    } catch {
      // ignore parse failure
    }
    throw new Error(errorDetail);
  }
  return res.json() as Promise<T>;
};

export const api = {
  async getHealth(): Promise<HealthInfo> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/health`);
    return handleResponse<HealthInfo>(res);
  },

  async getNodeStatus(): Promise<NodeStatus> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/node`);
    return handleResponse<NodeStatus>(res);
  },

  async getPeers(): Promise<PeerStatus[]> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/peers`);
    return handleResponse<PeerStatus[]>(res);
  },

  async getPeer(peerId: string): Promise<PeerStatus> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/peers/${encodeURIComponent(peerId)}`);
    return handleResponse<PeerStatus>(res);
  },

  async getTasks(status?: string, limit: number = 100): Promise<TaskRecord[]> {
    const base = getApiBaseUrl();
    const params = new URLSearchParams();
    if (status) params.set('status', status);
    params.set('limit', limit.toString());
    const res = await fetch(`${base}/api/tasks?${params.toString()}`);
    return handleResponse<TaskRecord[]>(res);
  },

  async getTask(taskId: string): Promise<TaskRecord> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/tasks/${encodeURIComponent(taskId)}`);
    return handleResponse<TaskRecord>(res);
  },

  async getAvailableTasks(): Promise<TaskAvailable[]> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/tasks/available`);
    return handleResponse<TaskAvailable[]>(res);
  },

  async submitTask(payload: TaskSubmitPayload): Promise<Record<string, unknown>> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/tasks`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    return handleResponse<Record<string, unknown>>(res);
  },

  async getMetrics(): Promise<SystemMetrics> {
    const base = getApiBaseUrl();
    const res = await fetch(`${base}/api/metrics`);
    return handleResponse<SystemMetrics>(res);
  },

  async getEvents(limit: number = 100, severity?: string, eventType?: string): Promise<MeshEvent[]> {
    const base = getApiBaseUrl();
    const params = new URLSearchParams();
    params.set('limit', limit.toString());
    if (severity) params.set('severity', severity);
    if (eventType) params.set('event_type', eventType);
    const res = await fetch(`${base}/api/events?${params.toString()}`);
    return handleResponse<MeshEvent[]>(res);
  },
};

export type WebSocketListener = (data: { type: string; event?: MeshEvent;[key: string]: unknown }) => void;

class LiveFeedManager {
  private ws: WebSocket | null = null;
  private listeners: Set<WebSocketListener> = new Set();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private isConnected: boolean = false;

  public subscribe(listener: WebSocketListener): () => void {
    this.listeners.add(listener);
    if (!this.ws || this.ws.readyState === WebSocket.CLOSED) {
      this.connect();
    }
    return () => {
      this.listeners.delete(listener);
      if (this.listeners.size === 0) {
        this.disconnect();
      }
    };
  }

  public get connectionState(): boolean {
    return this.isConnected;
  }

  private connect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    const base = getApiBaseUrl();
    let wsUrl: string;

    if (base.startsWith('http://')) {
      wsUrl = base.replace('http://', 'ws://') + '/ws/events';
    } else if (base.startsWith('https://')) {
      wsUrl = base.replace('https://', 'wss://') + '/ws/events';
    } else {
      const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      wsUrl = `${proto}//${window.location.host}/ws/events`;
    }

    try {
      this.ws = new WebSocket(wsUrl);

      this.ws.onopen = () => {
        this.isConnected = true;
        this.notify({ type: 'connection_change', connected: true });
      };

      this.ws.onmessage = (msgEvent) => {
        try {
          const data = JSON.parse(msgEvent.data);
          this.notify(data);
        } catch {
          // ignore parse errors
        }
      };

      this.ws.onclose = () => {
        this.isConnected = false;
        this.notify({ type: 'connection_change', connected: false });
        this.scheduleReconnect();
      };

      this.ws.onerror = () => {
        this.isConnected = false;
      };
    } catch {
      this.scheduleReconnect();
    }
  }

  private disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this.isConnected = false;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer || this.listeners.size === 0) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, 3000);
  }

  private notify(data: { type: string; event?: MeshEvent;[key: string]: unknown }): void {
    for (const listener of this.listeners) {
      try {
        listener(data);
      } catch {
        // safety
      }
    }
  }
}

export const liveFeed = new LiveFeedManager();

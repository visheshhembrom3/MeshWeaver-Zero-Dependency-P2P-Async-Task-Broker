import React from 'react';
import {
  Users,
  CheckCircle2,
  Clock,
  AlertTriangle,
  Activity,
  Layers,
  ArrowUpRight,
  TrendingUp,
} from 'lucide-react';
import {
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  BarChart,
  Bar,
} from 'recharts';
import type { MeshEvent, NodeStatus, PeerStatus, SystemMetrics, TaskRecord } from '../types';
import { StatusBadge } from '../components/StatusBadge';

interface OverviewProps {
  nodeStatus: NodeStatus | null;
  peers: PeerStatus[];
  tasks: TaskRecord[];
  events: MeshEvent[];
  metrics: SystemMetrics | null;
  onNavigate: (pageId: 'topology' | 'peers' | 'tasks' | 'monitoring' | 'events') => void;
}

export const Overview: React.FC<OverviewProps> = ({
  nodeStatus,
  peers,
  tasks,
  events,
  metrics,
  onNavigate,
}) => {
  const activePeers = peers.filter(
    (p) => p.state === 'active' && !p.is_unresponsive
  );
  const unresponsivePeers = peers.filter(
    (p) => p.is_unresponsive || p.state === 'unresponsive' || p.state === 'expired'
  );

  const pendingTasks = tasks.filter((t) => t.status === 'pending');
  const runningTasks = tasks.filter((t) => t.status === 'running');
  const completedTasks = tasks.filter((t) => t.status === 'success');
  const failedTasks = tasks.filter((t) =>
    ['failure', 'error', 'timeout'].includes(t.status)
  );

  // Real task execution duration metrics for Recharts
  const taskDurationData = React.useMemo(() => {
    if (tasks.length === 0) return [];
    return [...tasks]
      .filter((t) => t.timestamp != null)
      .sort((a, b) => (a.timestamp || 0) - (b.timestamp || 0))
      .slice(-8)
      .map((t, idx) => ({
        index: `#${idx + 1}`,
        name: t.task_name,
        durationMs: t.execution_time != null ? Number((t.execution_time * 1000).toFixed(1)) : 0,
        status: t.status,
      }));
  }, [tasks]);

  // Real peer load data from gossip metrics
  const peerLoadData = React.useMemo(() => {
    if (!metrics?.peer_loads || metrics.peer_loads.length === 0) {
      return [];
    }
    return metrics.peer_loads
      .slice(0, 6)
      .map((pl) => ({
        name: pl.peer_id.replace(' (Local)', '').slice(0, 10),
        cpu: Number((pl.cpu_percent || 0).toFixed(1)),
        memory: Number((pl.memory_percent || 0).toFixed(1)),
      }));
  }, [metrics]);

  return (
    <div className="space-y-6">
      {/* Top Banner / System Status */}
      <div className="bg-white border border-slate-200 rounded-xl p-5 shadow-xs flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div className="flex items-center gap-4">
          <div className="w-12 h-12 rounded-xl bg-blue-50 border border-blue-200 flex items-center justify-center text-blue-600">
            <Activity className="w-6 h-6" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h2 className="text-base font-bold text-slate-900">
                P2P Mesh Coordinator
              </h2>
              <StatusBadge status={nodeStatus?.is_running ? 'active' : 'unresponsive'} size="sm" />
            </div>
            <p className="text-xs text-slate-500 mt-0.5 font-mono">
              Node: <span className="text-slate-800 font-semibold">{nodeStatus?.node_id || 'Initializing...'}</span> |{' '}
              Port: <span className="text-slate-800 font-semibold">{nodeStatus?.bound_port || 'N/A'}</span> |{' '}
              DHT ID: <span className="text-slate-800 font-semibold">{nodeStatus?.dht_id_hex?.slice(0, 12) || 'N/A'}...</span>
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2 text-xs">
          <button
            onClick={() => onNavigate('tasks')}
            className="px-3.5 py-2 bg-blue-600 text-white font-medium rounded-lg hover:bg-blue-700 transition flex items-center gap-1.5 shadow-xs"
          >
            <span>Submit Task</span>
            <ArrowUpRight className="w-4 h-4" />
          </button>
          <button
            onClick={() => onNavigate('topology')}
            className="px-3.5 py-2 bg-slate-50 text-slate-700 font-medium border border-slate-200 rounded-lg hover:bg-slate-100 transition flex items-center gap-1.5 shadow-xs"
          >
            <span>Inspect Topology</span>
          </button>
        </div>
      </div>

      {/* Primary KPI Metric Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Active Peers */}
        <div
          onClick={() => onNavigate('peers')}
          className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs hover:border-blue-300 transition cursor-pointer"
        >
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Active Peers
            </span>
            <div className="p-2 bg-blue-50 text-blue-600 rounded-lg">
              <Users className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold text-slate-900">
              {activePeers.length}
            </span>
            <span className="text-xs text-slate-500">
              / {peers.length} registered
            </span>
          </div>
          <div className="mt-3 text-xs text-slate-500 flex items-center justify-between border-t border-slate-100 pt-2">
            <span>Unresponsive:</span>
            <span className={unresponsivePeers.length > 0 ? 'text-rose-600 font-semibold' : 'text-slate-700'}>
              {unresponsivePeers.length}
            </span>
          </div>
        </div>

        {/* Running & Pending Tasks */}
        <div
          onClick={() => onNavigate('tasks')}
          className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs hover:border-blue-300 transition cursor-pointer"
        >
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              In-Flight Tasks
            </span>
            <div className="p-2 bg-amber-50 text-amber-600 rounded-lg">
              <Clock className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold text-slate-900">
              {runningTasks.length}
            </span>
            <span className="text-xs text-amber-600 font-medium">
              running ({pendingTasks.length} queued)
            </span>
          </div>
          <div className="mt-3 text-xs text-slate-500 flex items-center justify-between border-t border-slate-100 pt-2">
            <span>Max Concurrency:</span>
            <span className="text-slate-700 font-semibold">
              {nodeStatus?.max_concurrency || 4} slots
            </span>
          </div>
        </div>

        {/* Completed Tasks */}
        <div
          onClick={() => onNavigate('tasks')}
          className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs hover:border-blue-300 transition cursor-pointer"
        >
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Completed Tasks
            </span>
            <div className="p-2 bg-emerald-50 text-emerald-600 rounded-lg">
              <CheckCircle2 className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold text-slate-900">
              {completedTasks.length}
            </span>
            <span className="text-xs text-emerald-600 font-medium">
              {metrics ? `${metrics.tasks.success_rate}% success` : '100%'}
            </span>
          </div>
          <div className="mt-3 text-xs text-slate-500 flex items-center justify-between border-t border-slate-100 pt-2">
            <span>Failed / Errors:</span>
            <span className={failedTasks.length > 0 ? 'text-rose-600 font-semibold' : 'text-slate-700'}>
              {failedTasks.length}
            </span>
          </div>
        </div>

        {/* DHT Routing Contacts */}
        <div
          onClick={() => onNavigate('topology')}
          className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs hover:border-blue-300 transition cursor-pointer"
        >
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              DHT Contacts
            </span>
            <div className="p-2 bg-indigo-50 text-indigo-600 rounded-lg">
              <Layers className="w-4 h-4" />
            </div>
          </div>
          <div className="flex items-baseline gap-2">
            <span className="text-2xl font-bold text-slate-900">
              {nodeStatus?.routing_table_contacts || 0}
            </span>
            <span className="text-xs text-slate-500">
              K-Bucket entries
            </span>
          </div>
          <div className="mt-3 text-xs text-slate-500 flex items-center justify-between border-t border-slate-100 pt-2">
            <span>Gossip Reports:</span>
            <span className="text-slate-700 font-semibold">
              {nodeStatus?.gossip_reports_count || 0}
            </span>
          </div>
        </div>
      </div>

      {/* Visual Charts Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Task Duration Telemetry */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">
                Recent Task Execution Durations (ms)
              </h3>
              <p className="text-xs text-slate-500">
                Actual measured execution time per task across workers
              </p>
            </div>
            <TrendingUp className="w-4 h-4 text-slate-400" />
          </div>
          {taskDurationData.length === 0 ? (
            <div className="h-48 flex items-center justify-center text-xs text-slate-400">
              No task executions recorded yet. Submit a task to view real duration metrics.
            </div>
          ) : (
            <div className="h-48 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={taskDurationData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <XAxis dataKey="name" stroke="#94A3B8" fontSize={11} tickLine={false} />
                  <YAxis stroke="#94A3B8" fontSize={11} tickLine={false} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#FFFFFF',
                      borderColor: '#E2E8F0',
                      borderRadius: '0.5rem',
                      fontSize: '12px',
                    }}
                    formatter={(value) => [`${value} ms`, 'Execution Duration']}
                  />
                  <Bar dataKey="durationMs" fill="#2563EB" radius={[4, 4, 0, 0]} name="Duration (ms)" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        {/* Peer Resource Utilization Preview */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">
                Peer Workload & CPU Utilization (%)
              </h3>
              <p className="text-xs text-slate-500">
                Live CPU and RAM consumption via gossip reports
              </p>
            </div>
            <Activity className="w-4 h-4 text-slate-400" />
          </div>
          {peerLoadData.length === 0 ? (
            <div className="h-48 flex items-center justify-center text-xs text-slate-400">
              No peer load reports received yet. Active peers will appear as gossip propagates.
            </div>
          ) : (
            <div className="h-48 w-full">
              <ResponsiveContainer width="100%" height="100%">
                <BarChart data={peerLoadData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                  <XAxis dataKey="name" stroke="#94A3B8" fontSize={11} tickLine={false} />
                  <YAxis stroke="#94A3B8" fontSize={11} tickLine={false} domain={[0, 100]} />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#FFFFFF',
                      borderColor: '#E2E8F0',
                      borderRadius: '0.5rem',
                      fontSize: '12px',
                    }}
                  />
                  <Bar dataKey="cpu" fill="#2563EB" radius={[4, 4, 0, 0]} name="CPU %" />
                  <Bar dataKey="memory" fill="#93C5FD" radius={[4, 4, 0, 0]} name="RAM %" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      </div>

      {/* Lower Dual Tables: Recent Tasks & Live System Events */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Recent Tasks */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs overflow-hidden">
          <div className="p-4 border-b border-slate-100 flex items-center justify-between">
            <h3 className="text-sm font-bold text-slate-900">
              Recent Task Activity
            </h3>
            <button
              onClick={() => onNavigate('tasks')}
              className="text-xs font-semibold text-blue-600 hover:text-blue-800"
            >
              View all ({tasks.length})
            </button>
          </div>
          {tasks.length === 0 ? (
            <div className="p-8 text-center text-xs text-slate-400">
              No tasks have been submitted yet.
            </div>
          ) : (
            <div className="divide-y divide-slate-100">
              {tasks.slice(0, 5).map((t) => (
                <div key={t.task_id} className="p-3.5 px-4 flex items-center justify-between text-xs hover:bg-slate-50 transition">
                  <div className="min-w-0 pr-2">
                    <div className="font-semibold text-slate-900 truncate">
                      {t.task_name}
                      <span className="ml-2 font-mono text-[11px] text-slate-400 font-normal">
                        ({t.task_id.slice(0, 8)}...)
                      </span>
                    </div>
                    <div className="text-slate-500 text-[11px] mt-0.5">
                      Worker: <span className="font-medium text-slate-700">{t.routed_to || 'Local Node'}</span>
                      {t.execution_time != null && (
                        <span className="ml-2">({(t.execution_time * 1000).toFixed(1)} ms)</span>
                      )}
                    </div>
                  </div>
                  <div>
                    <StatusBadge status={t.status} size="sm" />
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Live System Events */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs overflow-hidden">
          <div className="p-4 border-b border-slate-100 flex items-center justify-between">
            <h3 className="text-sm font-bold text-slate-900">
              Recent System Events
            </h3>
            <button
              onClick={() => onNavigate('events')}
              className="text-xs font-semibold text-blue-600 hover:text-blue-800"
            >
              View event log ({events.length})
            </button>
          </div>
          {events.length === 0 ? (
            <div className="p-8 text-center text-xs text-slate-400">
              No events recorded yet.
            </div>
          ) : (
            <div className="divide-y divide-slate-100">
              {events.slice(0, 5).map((ev) => (
                <div key={ev.id} className="p-3.5 px-4 flex items-start gap-3 text-xs hover:bg-slate-50 transition">
                  <div className="mt-0.5">
                    {ev.severity === 'error' ? (
                      <AlertTriangle className="w-3.5 h-3.5 text-rose-500" />
                    ) : ev.severity === 'warning' ? (
                      <AlertTriangle className="w-3.5 h-3.5 text-amber-500" />
                    ) : ev.severity === 'success' ? (
                      <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                    ) : (
                      <Activity className="w-3.5 h-3.5 text-blue-500" />
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-slate-800 font-medium truncate">
                      {ev.message}
                    </p>
                    <p className="text-[11px] text-slate-400 mt-0.5">
                      {new Date(ev.timestamp * 1000).toLocaleTimeString()}
                      {ev.peer_id && (
                        <span className="ml-1.5 font-mono text-slate-500">[{ev.peer_id}]</span>
                      )}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

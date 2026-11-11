import React from 'react';
import {
  Activity,
  Cpu,
  HardDrive,
  PieChart as PieIcon,
  CheckCircle2,
  Clock,
  Layers,
} from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Legend,
} from 'recharts';
import type { SystemMetrics } from '../types';
import { EmptyState } from '../components/EmptyState';

interface MonitoringProps {
  metrics: SystemMetrics | null;
}

export const Monitoring: React.FC<MonitoringProps> = ({ metrics }) => {
  if (!metrics) {
    return (
      <EmptyState
        title="Monitoring Data Awaiting Snapshot"
        description="Connecting to backend metrics adapter to collect peer workloads and task execution rates."
        icon={Activity}
      />
    );
  }

  // CPU chart data
  const cpuData = metrics.peer_loads.map((pl) => ({
    name: pl.peer_id.slice(0, 14),
    cpu: Number(pl.cpu_percent.toFixed(1)),
  }));

  // Memory chart data
  const memData = metrics.peer_loads.map((pl) => ({
    name: pl.peer_id.slice(0, 14),
    memory: Number(pl.memory_percent.toFixed(1)),
  }));

  // Task distribution data
  const taskDistribution = [
    { name: 'Completed', value: metrics.tasks.completed, color: '#10B981' },
    { name: 'Running', value: metrics.tasks.running, color: '#2563EB' },
    { name: 'Pending', value: metrics.tasks.pending, color: '#F59E0B' },
    { name: 'Failed', value: metrics.tasks.failed, color: '#EF4444' },
  ].filter((d) => d.value > 0);

  // Peer availability data
  const peerAvailability = [
    { name: 'Active', value: metrics.peers.active, color: '#10B981' },
    { name: 'Unresponsive', value: metrics.peers.unresponsive, color: '#EF4444' },
    { name: 'Stale', value: metrics.peers.stale, color: '#F59E0B' },
    { name: 'Expired', value: metrics.peers.expired, color: '#94A3B8' },
  ].filter((d) => d.value > 0);

  return (
    <div className="space-y-6">
      {/* Top Resource Summary Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Local CPU */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Coordinator CPU
            </span>
            <Cpu className="w-4 h-4 text-blue-600" />
          </div>
          <div className="text-2xl font-bold text-slate-900">
            {metrics.local_node.cpu_percent != null
              ? `${metrics.local_node.cpu_percent.toFixed(1)}%`
              : 'N/A'}
          </div>
          <div className="w-full bg-slate-100 rounded-full h-1.5 mt-3 overflow-hidden">
            <div
              className="bg-blue-600 h-1.5 rounded-full"
              style={{ width: `${metrics.local_node.cpu_percent || 0}%` }}
            />
          </div>
        </div>

        {/* Local RAM */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Coordinator Memory
            </span>
            <HardDrive className="w-4 h-4 text-indigo-600" />
          </div>
          <div className="text-2xl font-bold text-slate-900">
            {metrics.local_node.memory_percent != null
              ? `${metrics.local_node.memory_percent.toFixed(1)}%`
              : 'N/A'}
          </div>
          <div className="w-full bg-slate-100 rounded-full h-1.5 mt-3 overflow-hidden">
            <div
              className="bg-indigo-600 h-1.5 rounded-full"
              style={{ width: `${metrics.local_node.memory_percent || 0}%` }}
            />
          </div>
        </div>

        {/* Success Rate */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Task Success Rate
            </span>
            <CheckCircle2 className="w-4 h-4 text-emerald-600" />
          </div>
          <div className="text-2xl font-bold text-slate-900">
            {metrics.tasks.success_rate.toFixed(1)}%
          </div>
          <div className="text-xs text-slate-500 mt-2">
            {metrics.tasks.completed} completed of {metrics.tasks.total} total
          </div>
        </div>

        {/* Avg Duration */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between text-slate-500 mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Avg Execution Time
            </span>
            <Clock className="w-4 h-4 text-amber-600" />
          </div>
          <div className="text-2xl font-bold text-slate-900 font-mono">
            {(metrics.tasks.avg_execution_time * 1000).toFixed(1)} ms
          </div>
          <div className="text-xs text-slate-500 mt-2">
            Across distributed workers
          </div>
        </div>
      </div>

      {/* Main Charts: CPU & Memory Breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* CPU Bar Chart */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">CPU Load by Peer (%)</h3>
              <p className="text-xs text-slate-500">Live gossip reports from participating nodes</p>
            </div>
            <Cpu className="w-4 h-4 text-slate-400" />
          </div>
          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={cpuData} margin={{ top: 10, right: 10, left: -20, bottom: 20 }}>
                <XAxis dataKey="name" stroke="#94A3B8" fontSize={10} interval={0} angle={-15} textAnchor="end" />
                <YAxis stroke="#94A3B8" fontSize={11} domain={[0, 100]} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#FFFFFF',
                    borderColor: '#E2E8F0',
                    borderRadius: '0.5rem',
                    fontSize: '12px',
                  }}
                />
                <Bar dataKey="cpu" fill="#2563EB" radius={[4, 4, 0, 0]} name="CPU Utilization %" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Memory Bar Chart */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">RAM Usage by Peer (%)</h3>
              <p className="text-xs text-slate-500">Percentage of resident memory consumed</p>
            </div>
            <HardDrive className="w-4 h-4 text-slate-400" />
          </div>
          <div className="h-56 w-full">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={memData} margin={{ top: 10, right: 10, left: -20, bottom: 20 }}>
                <XAxis dataKey="name" stroke="#94A3B8" fontSize={10} interval={0} angle={-15} textAnchor="end" />
                <YAxis stroke="#94A3B8" fontSize={11} domain={[0, 100]} />
                <Tooltip
                  contentStyle={{
                    backgroundColor: '#FFFFFF',
                    borderColor: '#E2E8F0',
                    borderRadius: '0.5rem',
                    fontSize: '12px',
                  }}
                />
                <Bar dataKey="memory" fill="#6366F1" radius={[4, 4, 0, 0]} name="Memory %" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Secondary Donut Distribution Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Task Status Distribution */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">Task Execution Status Distribution</h3>
              <p className="text-xs text-slate-500">Proportion of tasks across lifecycle stages</p>
            </div>
            <PieIcon className="w-4 h-4 text-slate-400" />
          </div>
          {taskDistribution.length === 0 ? (
            <div className="h-48 flex items-center justify-center text-xs text-slate-400">
              No task records available to graph
            </div>
          ) : (
            <div className="h-48 w-full flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={taskDistribution}
                    innerRadius={50}
                    outerRadius={75}
                    paddingAngle={3}
                    dataKey="value"
                  >
                    {taskDistribution.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#FFFFFF',
                      borderColor: '#E2E8F0',
                      borderRadius: '0.5rem',
                      fontSize: '12px',
                    }}
                  />
                  <Legend verticalAlign="bottom" height={36} iconSize={8} wrapperStyle={{ fontSize: '11px' }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>

        {/* Peer Availability Breakdown */}
        <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="text-sm font-bold text-slate-900">Peer Health & Availability</h3>
              <p className="text-xs text-slate-500">Active vs Unresponsive vs Stale peers</p>
            </div>
            <Layers className="w-4 h-4 text-slate-400" />
          </div>
          {peerAvailability.length === 0 ? (
            <div className="h-48 flex items-center justify-center text-xs text-slate-400">
              No registered peers in mesh
            </div>
          ) : (
            <div className="h-48 w-full flex items-center justify-center">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={peerAvailability}
                    innerRadius={50}
                    outerRadius={75}
                    paddingAngle={3}
                    dataKey="value"
                  >
                    {peerAvailability.map((entry, index) => (
                      <Cell key={`cell-avail-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip
                    contentStyle={{
                      backgroundColor: '#FFFFFF',
                      borderColor: '#E2E8F0',
                      borderRadius: '0.5rem',
                      fontSize: '12px',
                    }}
                  />
                  <Legend verticalAlign="bottom" height={36} iconSize={8} wrapperStyle={{ fontSize: '11px' }} />
                </PieChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

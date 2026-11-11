import React, { useState } from 'react';
import {
  Server,
  Search,
  X,
  RefreshCw,
} from 'lucide-react';
import type { PeerStatus } from '../types';
import { StatusBadge } from '../components/StatusBadge';
import { EmptyState } from '../components/EmptyState';

interface PeersProps {
  peers: PeerStatus[];
  onRefresh: () => void;
  isRefreshing: boolean;
}

export const Peers: React.FC<PeersProps> = ({ peers, onRefresh, isRefreshing }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'stale' | 'unresponsive'>('all');
  const [selectedPeer, setSelectedPeer] = useState<PeerStatus | null>(null);

  const filteredPeers = peers.filter((p) => {
    const matchesSearch =
      p.peer_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      `${p.endpoint.host}:${p.endpoint.port}`.includes(searchQuery);

    if (statusFilter === 'all') return matchesSearch;
    if (statusFilter === 'active') return matchesSearch && p.state === 'active' && !p.is_unresponsive;
    if (statusFilter === 'stale') return matchesSearch && p.state === 'stale';
    if (statusFilter === 'unresponsive') {
      return matchesSearch && (p.is_unresponsive || p.state === 'unresponsive' || p.state === 'expired');
    }
    return matchesSearch;
  });

  return (
    <div className="space-y-6">
      {/* Controls Bar */}
      <div className="bg-white p-4 border border-slate-200 rounded-xl shadow-xs flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        {/* Search */}
        <div className="relative flex-1 max-w-md">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search peer ID or IP:port..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-4 py-2 text-xs border border-slate-200 rounded-lg focus:outline-hidden focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
          </input>
        </div>

        {/* Filter buttons */}
        <div className="flex items-center gap-2 overflow-x-auto pb-1 sm:pb-0">
          {(['all', 'active', 'stale', 'unresponsive'] as const).map((filter) => (
            <button
              key={filter}
              onClick={() => setStatusFilter(filter)}
              className={`px-3 py-1.5 text-xs font-medium rounded-lg capitalize transition whitespace-nowrap ${
                statusFilter === filter
                  ? 'bg-blue-600 text-white shadow-xs'
                  : 'bg-slate-50 text-slate-600 hover:bg-slate-100 border border-slate-200'
              }`}
            >
              {filter}
            </button>
          ))}

          <button
            onClick={onRefresh}
            disabled={isRefreshing}
            className="p-2 border border-slate-200 rounded-lg text-slate-600 hover:bg-slate-50 disabled:opacity-50"
            title="Refresh peers"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-blue-600' : ''}`} />
          </button>
        </div>
      </div>

      {/* Main Peers Table */}
      {filteredPeers.length === 0 ? (
        <EmptyState
          title="No Peers Found"
          description={
            searchQuery || statusFilter !== 'all'
              ? 'No registered peers matched your filter criteria.'
              : 'No remote peers have joined the mesh yet. Join nodes with --seeds to grow the network.'
          }
          icon={Server}
        />
      ) : (
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 text-slate-500 font-semibold border-b border-slate-200">
                <tr>
                  <th className="py-3 px-4">Peer ID</th>
                  <th className="py-3 px-4">Endpoint</th>
                  <th className="py-3 px-4">Lifecycle State</th>
                  <th className="py-3 px-4">Health</th>
                  <th className="py-3 px-4">CPU %</th>
                  <th className="py-3 px-4">RAM %</th>
                  <th className="py-3 px-4">Active Tasks</th>
                  <th className="py-3 px-4">Heartbeat / Latency</th>
                  <th className="py-3 px-4 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 text-slate-700">
                {filteredPeers.map((peer) => {
                  const isUnresponsive = peer.is_unresponsive || peer.state === 'unresponsive' || peer.state === 'expired';
                  return (
                    <tr
                      key={peer.peer_id}
                      onClick={() => setSelectedPeer(peer)}
                      className="hover:bg-slate-50/80 cursor-pointer transition"
                    >
                      <td className="py-3 px-4 font-mono font-medium text-slate-900">
                        <div className="flex items-center gap-2">
                          <span className={`w-2 h-2 rounded-full ${isUnresponsive ? 'bg-rose-500' : peer.is_connected ? 'bg-emerald-500' : 'bg-slate-300'}`} />
                          <span title={peer.peer_id}>{peer.peer_id.slice(0, 14)}</span>
                        </div>
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-600">
                        {peer.endpoint.host}:{peer.endpoint.port}
                      </td>
                      <td className="py-3 px-4">
                        <StatusBadge status={peer.state} size="sm" />
                      </td>
                      <td className="py-3 px-4">
                        <StatusBadge status={peer.health_status} size="sm" />
                      </td>
                      <td className="py-3 px-4">
                        {peer.cpu_percent != null ? (
                          <div className="flex items-center gap-2">
                            <span>{peer.cpu_percent.toFixed(1)}%</span>
                            <div className="w-12 bg-slate-100 rounded-full h-1">
                              <div
                                className="bg-blue-600 h-1 rounded-full"
                                style={{ width: `${Math.min(100, peer.cpu_percent)}%` }}
                              />
                            </div>
                          </div>
                        ) : (
                          <span className="text-slate-400">N/A</span>
                        )}
                      </td>
                      <td className="py-3 px-4">
                        {peer.memory_percent != null ? (
                          <div className="flex items-center gap-2">
                            <span>{peer.memory_percent.toFixed(1)}%</span>
                            <div className="w-12 bg-slate-100 rounded-full h-1">
                              <div
                                className="bg-indigo-500 h-1 rounded-full"
                                style={{ width: `${Math.min(100, peer.memory_percent)}%` }}
                              />
                            </div>
                          </div>
                        ) : (
                          <span className="text-slate-400">N/A</span>
                        )}
                      </td>
                      <td className="py-3 px-4 font-semibold text-slate-800">
                        {peer.active_task_count}
                      </td>
                      <td className="py-3 px-4">
                        <div className="text-slate-600">
                          {peer.last_latency_ms != null ? (
                            <span className="font-mono text-emerald-700 font-medium">
                              {peer.last_latency_ms} ms
                            </span>
                          ) : (
                            <span className="text-slate-400">N/A</span>
                          )}
                          <span className="text-slate-400 text-[11px] block">
                            seen {peer.last_seen_seconds_ago.toFixed(1)}s ago
                          </span>
                        </div>
                      </td>
                      <td className="py-3 px-4 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedPeer(peer);
                          }}
                          className="px-2.5 py-1 text-xs text-blue-600 hover:text-blue-800 font-medium hover:bg-blue-50 rounded"
                        >
                          Details
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Peer Detail Slide-over / Modal */}
      {selectedPeer && (
        <div className="fixed inset-0 z-50 overflow-hidden bg-slate-900/30 backdrop-blur-xs flex justify-end">
          <div className="w-full max-w-md bg-white h-full shadow-2xl p-6 flex flex-col justify-between overflow-y-auto">
            <div>
              <div className="flex items-center justify-between pb-4 border-b border-slate-100 mb-6">
                <div className="flex items-center gap-2">
                  <Server className="w-5 h-5 text-blue-600" />
                  <h3 className="text-base font-bold text-slate-900">Peer Inspector</h3>
                </div>
                <button
                  onClick={() => setSelectedPeer(null)}
                  className="p-1 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {/* Status Header */}
              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl mb-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
                    Peer Identifier
                  </span>
                  <StatusBadge status={selectedPeer.state} size="sm" />
                </div>
                <p className="font-mono text-sm font-bold text-slate-900 break-all">
                  {selectedPeer.peer_id}
                </p>
                <div className="mt-2 text-xs text-slate-500 font-mono">
                  Endpoint: {selectedPeer.endpoint.host}:{selectedPeer.endpoint.port}
                </div>
              </div>

              {/* Telemetry Details */}
              <div className="space-y-3 text-xs">
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Operational Health:</span>
                  <StatusBadge status={selectedPeer.health_status} size="sm" />
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">TCP Stream State:</span>
                  <span className="font-medium text-slate-800">
                    {selectedPeer.is_connected ? 'Connected' : 'Disconnected'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Connection Direction:</span>
                  <span className="text-slate-800 font-medium">
                    {selectedPeer.is_inbound ? 'Inbound' : 'Outbound'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">DHT Proximity Distance:</span>
                  <span className="font-mono text-slate-800 font-medium">
                    {selectedPeer.dht_distance != null ? selectedPeer.dht_distance : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Active Task Counter:</span>
                  <span className="font-semibold text-slate-800">
                    {selectedPeer.active_task_count} tasks
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Missed Heartbeats:</span>
                  <span className={selectedPeer.missed_heartbeats ? 'text-rose-600 font-semibold' : 'text-slate-800'}>
                    {selectedPeer.missed_heartbeats || 0}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Last Probe Latency:</span>
                  <span className="font-mono text-slate-800">
                    {selectedPeer.last_latency_ms != null ? `${selectedPeer.last_latency_ms} ms` : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Registered At:</span>
                  <span className="text-slate-800">
                    {new Date(selectedPeer.registered_at * 1000).toLocaleString()}
                  </span>
                </div>
              </div>

              {/* Supported Tasks */}
              <div className="mt-6 pt-4 border-t border-slate-100">
                <span className="text-xs font-semibold text-slate-700 block mb-2">
                  Declared Task Capabilities
                </span>
                {selectedPeer.supported_tasks && selectedPeer.supported_tasks.length > 0 ? (
                  <div className="flex flex-wrap gap-1.5">
                    {selectedPeer.supported_tasks.map((task) => (
                      <span
                        key={task}
                        className="px-2 py-1 rounded bg-blue-50 text-blue-700 text-[11px] font-mono border border-blue-200"
                      >
                        {task}
                      </span>
                    ))}
                  </div>
                ) : (
                  <p className="text-xs text-slate-400">
                    No explicit task capabilities announced yet (standard tasks supported by default).
                  </p>
                )}
              </div>
            </div>

            <div className="pt-6 border-t border-slate-100">
              <button
                onClick={() => setSelectedPeer(null)}
                className="w-full py-2 bg-slate-100 text-slate-700 font-medium rounded-lg hover:bg-slate-200 text-xs transition"
              >
                Close Inspector
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

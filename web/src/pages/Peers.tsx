import React, { useState, useMemo } from 'react';
import {
  Server,
  Search,
  X,
  RefreshCw,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
} from 'lucide-react';
import type { PeerStatus } from '../types';
import { StatusBadge } from '../components/StatusBadge';
import { EmptyState } from '../components/EmptyState';

interface PeersProps {
  peers: PeerStatus[];
  onRefresh: () => void;
  isRefreshing: boolean;
}

type SortField =
  | 'peer_id'
  | 'endpoint'
  | 'state'
  | 'health_status'
  | 'cpu_percent'
  | 'memory_percent'
  | 'active_task_count'
  | 'latency'
  | 'last_seen';

export const Peers: React.FC<PeersProps> = ({ peers, onRefresh, isRefreshing }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'active' | 'stale' | 'unresponsive'>('all');
  const [selectedPeer, setSelectedPeer] = useState<PeerStatus | null>(null);

  // Sorting state
  const [sortField, setSortField] = useState<SortField>('last_seen');
  const [sortAsc, setSortAsc] = useState<boolean>(true);

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortAsc(!sortAsc);
    } else {
      setSortField(field);
      setSortAsc(true);
    }
  };

  const filteredAndSortedPeers = useMemo(() => {
    const filtered = peers.filter((p) => {
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

    return filtered.sort((a, b) => {
      let valA: unknown;
      let valB: unknown;

      switch (sortField) {
        case 'peer_id':
          valA = a.peer_id.toLowerCase();
          valB = b.peer_id.toLowerCase();
          break;
        case 'endpoint':
          valA = `${a.endpoint.host}:${a.endpoint.port}`;
          valB = `${b.endpoint.host}:${b.endpoint.port}`;
          break;
        case 'state':
          valA = a.state;
          valB = b.state;
          break;
        case 'health_status':
          valA = a.health_status;
          valB = b.health_status;
          break;
        case 'cpu_percent':
          valA = a.cpu_percent ?? -1;
          valB = b.cpu_percent ?? -1;
          break;
        case 'memory_percent':
          valA = a.memory_percent ?? -1;
          valB = b.memory_percent ?? -1;
          break;
        case 'active_task_count':
          valA = a.active_task_count;
          valB = b.active_task_count;
          break;
        case 'latency':
          valA = a.last_latency_ms ?? 99999;
          valB = b.last_latency_ms ?? 99999;
          break;
        case 'last_seen':
          valA = a.last_seen_seconds_ago ?? 99999;
          valB = b.last_seen_seconds_ago ?? 99999;
          break;
        default:
          valA = a.last_seen_seconds_ago;
          valB = b.last_seen_seconds_ago;
      }

      if (valA! < valB!) return sortAsc ? -1 : 1;
      if (valA! > valB!) return sortAsc ? 1 : -1;
      return 0;
    });
  }, [peers, searchQuery, statusFilter, sortField, sortAsc]);

  const renderSortIndicator = (field: SortField) => {
    if (sortField !== field) {
      return <ArrowUpDown className="w-3 h-3 text-slate-300 dark:text-slate-600 inline ml-1 opacity-60" />;
    }
    return sortAsc ? (
      <ArrowUp className="w-3 h-3 text-blue-600 dark:text-blue-400 inline ml-1" />
    ) : (
      <ArrowDown className="w-3 h-3 text-blue-600 dark:text-blue-400 inline ml-1" />
    );
  };

  return (
    <div className="space-y-6">
      {/* Controls Bar */}
      <div className="bg-white dark:bg-slate-900 p-4 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs flex flex-col sm:flex-row sm:items-center justify-between gap-4 transition-colors">
        {/* Search */}
        <div className="relative flex-1 max-w-md">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search peer ID or host:port..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-4 py-2 text-xs bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-hidden focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
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
                  : 'bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-700'
              }`}
            >
              {filter}
            </button>
          ))}

          <button
            onClick={onRefresh}
            disabled={isRefreshing}
            className="p-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-50 transition"
            title="Refresh peers"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-blue-600 dark:text-blue-400' : ''}`} />
          </button>
        </div>
      </div>

      {/* Main Peers Table */}
      {filteredAndSortedPeers.length === 0 ? (
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
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs overflow-hidden transition-colors">
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 dark:bg-slate-800/60 text-slate-500 dark:text-slate-400 font-semibold border-b border-slate-200 dark:border-slate-800 select-none">
                <tr>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('peer_id')}
                  >
                    Peer ID {renderSortIndicator('peer_id')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('endpoint')}
                  >
                    Address {renderSortIndicator('endpoint')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('state')}
                  >
                    Lifecycle {renderSortIndicator('state')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('health_status')}
                  >
                    Health {renderSortIndicator('health_status')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('cpu_percent')}
                  >
                    CPU % {renderSortIndicator('cpu_percent')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('memory_percent')}
                  >
                    RAM % {renderSortIndicator('memory_percent')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('active_task_count')}
                  >
                    Tasks {renderSortIndicator('active_task_count')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('latency')}
                  >
                    Heartbeat Latency {renderSortIndicator('latency')}
                  </th>
                  <th
                    className="py-3 px-4 cursor-pointer hover:text-slate-900 dark:hover:text-slate-200"
                    onClick={() => handleSort('last_seen')}
                  >
                    Last Seen {renderSortIndicator('last_seen')}
                  </th>
                  <th className="py-3 px-4 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 dark:divide-slate-800 text-slate-700 dark:text-slate-300">
                {filteredAndSortedPeers.map((peer) => {
                  const isUnresponsive =
                    peer.is_unresponsive || peer.state === 'unresponsive' || peer.state === 'expired';
                  const isStale = peer.state === 'stale' || (peer.last_seen_seconds_ago ?? 0) > 15;

                  return (
                    <tr
                      key={peer.peer_id}
                      onClick={() => setSelectedPeer(peer)}
                      className="hover:bg-slate-50/80 dark:hover:bg-slate-800/40 cursor-pointer transition"
                    >
                      <td className="py-3 px-4 font-mono font-medium text-slate-900 dark:text-slate-100">
                        <div className="flex items-center gap-2">
                          <span
                            className={`w-2 h-2 rounded-full ${
                              isUnresponsive
                                ? 'bg-rose-500'
                                : peer.is_connected
                                ? 'bg-emerald-500'
                                : 'bg-slate-400'
                            }`}
                          />
                          <span title={peer.peer_id}>{peer.peer_id.slice(0, 14)}</span>
                        </div>
                      </td>
                      <td className="py-3 px-4 font-mono text-slate-600 dark:text-slate-400">
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
                          <div className="flex items-center gap-1.5">
                            <span>{peer.cpu_percent.toFixed(1)}%</span>
                            {isStale && (
                              <span className="text-[10px] font-semibold px-1 py-0.2 bg-amber-50 dark:bg-amber-950/40 text-amber-600 dark:text-amber-400 border border-amber-200 dark:border-amber-800 rounded">
                                Stale
                              </span>
                            )}
                          </div>
                        ) : (
                          <span className="text-slate-400 dark:text-slate-500">N/A</span>
                        )}
                      </td>
                      <td className="py-3 px-4">
                        {peer.memory_percent != null ? (
                          <div className="flex items-center gap-1.5">
                            <span>{peer.memory_percent.toFixed(1)}%</span>
                            {isStale && (
                              <span className="text-[10px] font-semibold px-1 py-0.2 bg-amber-50 dark:bg-amber-950/40 text-amber-600 dark:text-amber-400 border border-amber-200 dark:border-amber-800 rounded">
                                Stale
                              </span>
                            )}
                          </div>
                        ) : (
                          <span className="text-slate-400 dark:text-slate-500">N/A</span>
                        )}
                      </td>
                      <td className="py-3 px-4 font-semibold text-slate-800 dark:text-slate-200">
                        {peer.active_task_count}
                      </td>
                      <td className="py-3 px-4">
                        {peer.last_latency_ms != null ? (
                          <span className="font-mono text-emerald-600 dark:text-emerald-400 font-medium">
                            {peer.last_latency_ms} ms
                          </span>
                        ) : (
                          <span className="text-slate-400 dark:text-slate-500">N/A</span>
                        )}
                      </td>
                      <td className="py-3 px-4">
                        <span className="text-slate-600 dark:text-slate-400">
                          {peer.last_seen_seconds_ago != null
                            ? `${peer.last_seen_seconds_ago.toFixed(1)}s ago`
                            : 'N/A'}
                        </span>
                      </td>
                      <td className="py-3 px-4 text-right">
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setSelectedPeer(peer);
                          }}
                          className="px-2.5 py-1 text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-300 font-medium hover:bg-blue-50 dark:hover:bg-blue-950/40 rounded transition"
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
        <div className="fixed inset-0 z-50 overflow-hidden bg-slate-900/40 backdrop-blur-xs flex justify-end">
          <div className="w-full max-w-md bg-white dark:bg-slate-900 h-full shadow-2xl p-6 flex flex-col justify-between overflow-y-auto border-l border-slate-200 dark:border-slate-800 transition-colors">
            <div>
              <div className="flex items-center justify-between pb-4 border-b border-slate-100 dark:border-slate-800 mb-6">
                <div className="flex items-center gap-2">
                  <Server className="w-5 h-5 text-blue-600 dark:text-blue-400" />
                  <h3 className="text-base font-bold text-slate-900 dark:text-slate-100">Peer Inspector</h3>
                </div>
                <button
                  onClick={() => setSelectedPeer(null)}
                  className="p-1 rounded-lg text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {/* Status Header */}
              <div className="p-4 bg-slate-50 dark:bg-slate-800/60 border border-slate-200 dark:border-slate-700 rounded-xl mb-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-bold text-slate-900 dark:text-slate-100 text-sm font-mono truncate max-w-[220px]">
                    {selectedPeer.peer_id}
                  </span>
                  <StatusBadge status={selectedPeer.state} size="sm" />
                </div>
                <p className="font-mono text-xs text-slate-500 dark:text-slate-400">
                  {selectedPeer.endpoint.host}:{selectedPeer.endpoint.port}
                </p>
              </div>

              {/* Telemetry rows */}
              <div className="space-y-3 text-xs">
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Health Evaluation:</span>
                  <StatusBadge status={selectedPeer.health_status} size="sm" />
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Heartbeat Latency:</span>
                  <span className="font-mono font-medium text-emerald-600 dark:text-emerald-400">
                    {selectedPeer.last_latency_ms != null ? `${selectedPeer.last_latency_ms} ms` : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Missed Heartbeats:</span>
                  <span className={`font-semibold ${selectedPeer.missed_heartbeats ? 'text-amber-600' : 'text-slate-700 dark:text-slate-300'}`}>
                    {selectedPeer.missed_heartbeats ?? 0}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Reported CPU:</span>
                  <span className="font-semibold text-slate-800 dark:text-slate-200">
                    {selectedPeer.cpu_percent != null ? `${selectedPeer.cpu_percent.toFixed(1)}%` : 'N/A (No gossip yet)'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Reported RAM:</span>
                  <span className="font-semibold text-slate-800 dark:text-slate-200">
                    {selectedPeer.memory_percent != null ? `${selectedPeer.memory_percent.toFixed(1)}%` : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Active Tasks:</span>
                  <span className="font-bold text-slate-800 dark:text-slate-200">{selectedPeer.active_task_count}</span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">TCP Socket:</span>
                  <span className="font-medium text-slate-700 dark:text-slate-300">
                    {selectedPeer.is_connected ? 'Connected' : 'Disconnected / Inactive'}
                  </span>
                </div>
              </div>

              {/* Supported Tasks */}
              <div className="mt-6">
                <span className="text-xs font-semibold text-slate-700 dark:text-slate-300 block mb-2">
                  Allowlisted Supported Tasks:
                </span>
                <div className="flex flex-wrap gap-1.5">
                  {selectedPeer.supported_tasks && selectedPeer.supported_tasks.length > 0 ? (
                    selectedPeer.supported_tasks.map((task) => (
                      <span
                        key={task}
                        className="px-2 py-0.5 bg-blue-50 dark:bg-blue-950/50 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800 rounded text-[11px] font-mono"
                      >
                        {task}
                      </span>
                    ))
                  ) : (
                    <span className="text-slate-400 dark:text-slate-500 text-xs italic">
                      Standard core tasks (add, ping, echo, reverse_string)
                    </span>
                  )}
                </div>
              </div>
            </div>

            <div className="pt-6 border-t border-slate-100 dark:border-slate-800">
              <button
                onClick={() => setSelectedPeer(null)}
                className="w-full py-2 bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300 font-medium rounded-lg hover:bg-slate-200 dark:hover:bg-slate-700 text-xs transition"
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

import React, { useState } from 'react';
import {
  Server,
  Network,
  X,
  Share2,
  Terminal,
} from 'lucide-react';
import type { NodeStatus, PeerStatus } from '../types';
import { StatusBadge } from '../components/StatusBadge';

interface TopologyProps {
  nodeStatus: NodeStatus | null;
  peers: PeerStatus[];
}

export const Topology: React.FC<TopologyProps> = ({ nodeStatus, peers }) => {
  const [selectedNode, setSelectedNode] = useState<PeerStatus | 'local' | null>(null);

  // Layout parameters for SVG node graph
  const width = 800;
  const height = 480;
  const centerX = width / 2;
  const centerY = height / 2;
  const radius = Math.min(width, height) * 0.35;

  const totalPeers = peers.length;

  // Calculate coordinates for peer nodes in a circular constellation around local node
  const peerPositions = React.useMemo(() => {
    return peers.map((p, index) => {
      const angle = (2 * Math.PI * index) / (totalPeers || 1) - Math.PI / 2;
      return {
        peer: p,
        x: centerX + radius * Math.cos(angle),
        y: centerY + radius * Math.sin(angle),
      };
    });
  }, [peers, centerX, centerY, radius, totalPeers]);

  const selectedPeerData = selectedNode === 'local' ? null : selectedNode;

  return (
    <div className="space-y-6">
      {/* Top Header Card */}
      <div className="bg-white dark:bg-slate-900 p-5 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs flex flex-col md:flex-row md:items-center justify-between gap-4 transition-colors">
        <div>
          <h2 className="text-base font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <Network className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            Live Mesh Network Topology
          </h2>
          <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">
            Real-time TCP connection graph and Kademlia XOR metric constellation. Click any node to inspect live telemetry.
          </p>
        </div>

        {/* Legend */}
        <div className="flex flex-wrap items-center gap-3 text-xs bg-slate-50 dark:bg-slate-800/80 px-3.5 py-2 rounded-lg border border-slate-200 dark:border-slate-700">
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-blue-600 border border-blue-400" />
            <span className="text-slate-700 dark:text-slate-300 font-medium">Local Coordinator</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-emerald-500 border border-emerald-400" />
            <span className="text-slate-700 dark:text-slate-300 font-medium">Active Peer</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-amber-400 border border-amber-300" />
            <span className="text-slate-700 dark:text-slate-300 font-medium">Degraded / Stale</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-rose-500 border border-rose-400" />
            <span className="text-slate-700 dark:text-slate-300 font-medium">Unresponsive</span>
          </div>
        </div>
      </div>

      {/* Main Interactive Graph & Inspector Panel */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* SVG Graph Canvas */}
        <div className="lg:col-span-2 bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs p-4 flex flex-col items-center justify-center relative overflow-hidden min-h-[460px] transition-colors">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            className="w-full h-auto max-h-[500px] select-none"
          >
            {/* Background grid markings */}
            <circle
              cx={centerX}
              cy={centerY}
              r={radius}
              fill="none"
              stroke="currentColor"
              className="text-slate-200 dark:text-slate-800"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
            <circle
              cx={centerX}
              cy={centerY}
              r={radius * 0.5}
              fill="none"
              stroke="currentColor"
              className="text-slate-100 dark:text-slate-800/50"
              strokeWidth="1"
            />

            {/* Connection Links */}
            {peerPositions.map((pos) => {
              const isUnresponsive =
                pos.peer.is_unresponsive ||
                pos.peer.state === 'unresponsive' ||
                pos.peer.state === 'expired';
              const isSelected = selectedNode !== 'local' && selectedNode?.peer_id === pos.peer.peer_id;

              return (
                <g key={`link-${pos.peer.peer_id}`}>
                  <line
                    x1={centerX}
                    y1={centerY}
                    x2={pos.x}
                    y2={pos.y}
                    stroke={
                      isUnresponsive
                        ? '#EF4444'
                        : isSelected
                        ? '#2563EB'
                        : pos.peer.is_connected
                        ? '#60A5FA'
                        : '#94A3B8'
                    }
                    strokeWidth={isSelected ? 2.5 : pos.peer.is_connected ? 2 : 1}
                    strokeDasharray={isUnresponsive || !pos.peer.is_connected ? '4 4' : undefined}
                    className="transition-all duration-300"
                  />
                  {pos.peer.last_latency_ms != null && (
                    <text
                      x={(centerX + pos.x) / 2}
                      y={(centerY + pos.y) / 2 - 4}
                      fill="#64748B"
                      fontSize="9"
                      fontFamily="monospace"
                      textAnchor="middle"
                      className="select-none"
                    >
                      {pos.peer.last_latency_ms.toFixed(1)}ms
                    </text>
                  )}
                </g>
              );
            })}

            {/* Local Node (Coordinator Center Hub) */}
            <g
              onClick={() => setSelectedNode('local')}
              className="cursor-pointer group"
            >
              <circle
                cx={centerX}
                cy={centerY}
                r={selectedNode === 'local' ? 32 : 28}
                fill="#2563EB"
                stroke="#93C5FD"
                strokeWidth={selectedNode === 'local' ? 4 : 2}
                className="transition-all group-hover:scale-105"
              />
              <text
                x={centerX}
                y={centerY - 3}
                fill="#FFFFFF"
                fontSize="10"
                fontWeight="700"
                textAnchor="middle"
                alignmentBaseline="middle"
              >
                LOCAL
              </text>
              <text
                x={centerX}
                y={centerY + 9}
                fill="#BFDBFE"
                fontSize="8"
                fontFamily="monospace"
                textAnchor="middle"
                alignmentBaseline="middle"
              >
                {nodeStatus?.bound_port ? `:${nodeStatus.bound_port}` : 'Hub'}
              </text>
            </g>

            {/* Satellite Peer Nodes */}
            {peerPositions.map((pos) => {
              const isUnresponsive =
                pos.peer.is_unresponsive ||
                pos.peer.state === 'unresponsive' ||
                pos.peer.state === 'expired';
              const isDegraded = pos.peer.health_status === 'degraded' || pos.peer.state === 'stale';
              const isSelected = selectedNode !== 'local' && selectedNode?.peer_id === pos.peer.peer_id;

              const fillColor = isUnresponsive
                ? '#EF4444'
                : isDegraded
                ? '#F59E0B'
                : '#10B981';

              return (
                <g
                  key={pos.peer.peer_id}
                  onClick={() => setSelectedNode(pos.peer)}
                  className="cursor-pointer group"
                >
                  <circle
                    cx={pos.x}
                    cy={pos.y}
                    r={isSelected ? 26 : 22}
                    fill={fillColor}
                    stroke="#FFFFFF"
                    strokeWidth={isSelected ? 3 : 1.5}
                    strokeDasharray={isUnresponsive ? '3 3' : undefined}
                    className="transition-all group-hover:scale-110 shadow-md"
                  />
                  <text
                    x={pos.x}
                    y={pos.y - 2}
                    fill="#FFFFFF"
                    fontSize="9"
                    fontWeight="700"
                    fontFamily="monospace"
                    textAnchor="middle"
                    alignmentBaseline="middle"
                  >
                    {pos.peer.peer_id.slice(0, 6)}
                  </text>
                  <text
                    x={pos.x}
                    y={pos.y + 9}
                    fill="#F1F5F9"
                    fontSize="8"
                    textAnchor="middle"
                    alignmentBaseline="middle"
                  >
                    {pos.peer.endpoint.port}
                  </text>
                </g>
              );
            })}
          </svg>

          {/* Empty state overlay when no peers have connected */}
          {totalPeers === 0 && (
            <div className="absolute inset-x-8 bottom-6 bg-slate-50/95 dark:bg-slate-800/95 border border-slate-200 dark:border-slate-700 p-4 rounded-xl shadow-xs text-xs text-center backdrop-blur-xs">
              <div className="flex items-center justify-center gap-2 font-bold text-slate-800 dark:text-slate-200 mb-1">
                <Share2 className="w-4 h-4 text-blue-600 dark:text-blue-400" />
                <span>Standalone Coordinator — No Remote Peers Connected</span>
              </div>
              <p className="text-slate-500 dark:text-slate-400 max-w-md mx-auto mb-2 text-[11px]">
                The live mesh graph will automatically draw connections as peers discover each other. Start worker nodes in separate terminals:
              </p>
              <div className="inline-flex items-center gap-1.5 font-mono text-[11px] bg-white dark:bg-slate-900 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 text-slate-800 dark:text-slate-200">
                <Terminal className="w-3.5 h-3.5 text-blue-500" />
                <span>python -m meshweaver start --port 8001 --seeds 127.0.0.1:{nodeStatus?.bound_port || 8000}</span>
              </div>
            </div>
          )}

          {/* Quick Stats Overlay */}
          <div className="absolute top-3 left-4 text-[11px] text-slate-500 dark:text-slate-400 bg-white/90 dark:bg-slate-800/90 backdrop-blur-xs px-2.5 py-1 rounded-md border border-slate-200 dark:border-slate-700">
            Total Nodes: <span className="font-semibold text-slate-800 dark:text-slate-200">{totalPeers + 1}</span> (1 Hub + {totalPeers} Peers)
          </div>
        </div>

        {/* Selected Node Detail Drawer */}
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs p-5 flex flex-col transition-colors">
          <div className="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800 mb-4">
            <h3 className="text-sm font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
              <Server className="w-4 h-4 text-blue-600 dark:text-blue-400" />
              Node Telemetry
            </h3>
            {selectedNode && (
              <button
                onClick={() => setSelectedNode(null)}
                className="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 p-1 rounded"
              >
                <X className="w-4 h-4" />
              </button>
            )}
          </div>

          {selectedNode === 'local' ? (
            <div className="space-y-4 flex-1">
              <div className="p-3 bg-blue-50/60 dark:bg-blue-950/40 border border-blue-200 dark:border-blue-800 rounded-lg">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-blue-900 dark:text-blue-200">Local Coordinator</span>
                  <StatusBadge status="active" size="sm" />
                </div>
                <p className="text-[11px] text-blue-700 dark:text-blue-300 font-mono mt-1 break-all">
                  {nodeStatus?.node_id}
                </p>
              </div>

              <div className="space-y-2 text-xs">
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Interface:</span>
                  <span className="font-mono text-slate-800 dark:text-slate-200 font-medium">
                    {nodeStatus?.host}:{nodeStatus?.bound_port}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">DHT 160-bit Key:</span>
                  <span className="font-mono text-slate-800 dark:text-slate-200 truncate max-w-[150px]" title={nodeStatus?.dht_id_hex}>
                    {nodeStatus?.dht_id_hex}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Worker Concurrency:</span>
                  <span className="font-semibold text-slate-800 dark:text-slate-200">{nodeStatus?.max_concurrency || 4} slots</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Routing Contacts:</span>
                  <span className="font-semibold text-blue-600 dark:text-blue-400">{nodeStatus?.routing_table_contacts || 0}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Uptime:</span>
                  <span className="text-slate-800 dark:text-slate-200">{nodeStatus?.uptime_seconds ? `${nodeStatus.uptime_seconds.toFixed(0)}s` : 'N/A'}</span>
                </div>
              </div>
            </div>
          ) : selectedPeerData ? (
            <div className="space-y-4 flex-1">
              <div className="p-3 bg-slate-50 dark:bg-slate-800/60 border border-slate-200 dark:border-slate-700 rounded-lg">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-slate-900 dark:text-slate-100">Remote Peer</span>
                  <StatusBadge status={selectedPeerData.state} size="sm" />
                </div>
                <p className="text-[11px] text-slate-600 dark:text-slate-400 font-mono mt-1 break-all">
                  {selectedPeerData.peer_id}
                </p>
              </div>

              <div className="space-y-2 text-xs">
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Endpoint:</span>
                  <span className="font-mono text-slate-800 dark:text-slate-200 font-medium">
                    {selectedPeerData.endpoint.host}:{selectedPeerData.endpoint.port}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Health State:</span>
                  <StatusBadge status={selectedPeerData.health_status} size="sm" />
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Active Tasks:</span>
                  <span className="font-semibold text-slate-800 dark:text-slate-200">{selectedPeerData.active_task_count}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Latency:</span>
                  <span className="font-mono font-medium text-emerald-600 dark:text-emerald-400">
                    {selectedPeerData.last_latency_ms != null ? `${selectedPeerData.last_latency_ms} ms` : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                  <span className="text-slate-500 dark:text-slate-400">Last Seen:</span>
                  <span className="text-slate-800 dark:text-slate-200">
                    {selectedPeerData.last_seen_seconds_ago != null ? `${selectedPeerData.last_seen_seconds_ago.toFixed(1)}s ago` : 'N/A'}
                  </span>
                </div>

                <div>
                  <div className="flex justify-between text-slate-600 dark:text-slate-400 mb-1">
                    <span>CPU Load:</span>
                    <span className="font-semibold">{selectedPeerData.cpu_percent != null ? `${selectedPeerData.cpu_percent.toFixed(1)}%` : 'N/A'}</span>
                  </div>
                  <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1.5 overflow-hidden">
                    <div
                      className="bg-blue-600 h-1.5 rounded-full"
                      style={{ width: `${selectedPeerData.cpu_percent || 0}%` }}
                    />
                  </div>
                </div>

                <div>
                  <div className="flex justify-between text-slate-600 dark:text-slate-400 mb-1">
                    <span>Memory:</span>
                    <span className="font-semibold">{selectedPeerData.memory_percent != null ? `${selectedPeerData.memory_percent.toFixed(1)}%` : 'N/A'}</span>
                  </div>
                  <div className="w-full bg-slate-100 dark:bg-slate-800 rounded-full h-1.5 overflow-hidden">
                    <div
                      className="bg-indigo-500 h-1.5 rounded-full"
                      style={{ width: `${selectedPeerData.memory_percent || 0}%` }}
                    />
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="h-64 flex flex-col items-center justify-center text-center text-slate-400 dark:text-slate-500">
              <Network className="w-8 h-8 text-slate-300 dark:text-slate-600 mb-2" />
              <p className="text-xs font-medium text-slate-600 dark:text-slate-300">Select a Node to Inspect</p>
              <p className="text-[11px] text-slate-400 dark:text-slate-500 max-w-[200px] mt-1">
                Click on the coordinator or any satellite peer in the graph to view live status.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

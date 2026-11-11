import React, { useState } from 'react';
import {
  Server,
  Network,
  Wifi,
  WifiOff,
  X,
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
      <div className="bg-white p-5 border border-slate-200 rounded-xl shadow-xs flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-bold text-slate-900 flex items-center gap-2">
            <Network className="w-5 h-5 text-blue-600" />
            Mesh Topology & DHT Proximity
          </h2>
          <p className="text-xs text-slate-500 mt-1">
            Real-time TCP connection graph and Kademlia XOR distance visualization.
            Click any node to view detailed telemetry.
          </p>
        </div>

        {/* Legend */}
        <div className="flex flex-wrap items-center gap-3 text-xs bg-slate-50 px-3.5 py-2 rounded-lg border border-slate-200">
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-blue-600 border border-blue-400" />
            <span className="text-slate-700 font-medium">Local Coordinator</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-emerald-500 border border-emerald-400" />
            <span className="text-slate-700 font-medium">Active Peer</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-amber-400 border border-amber-300" />
            <span className="text-slate-700 font-medium">Degraded / Stale</span>
          </div>
          <div className="flex items-center gap-1.5">
            <span className="w-3 h-3 rounded-full bg-rose-500 border border-rose-400" />
            <span className="text-slate-700 font-medium">Unresponsive</span>
          </div>
        </div>
      </div>

      {/* Main Interactive Graph & Inspector Panel */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* SVG Graph Canvas */}
        <div className="lg:col-span-2 bg-white border border-slate-200 rounded-xl shadow-xs p-4 flex flex-col items-center justify-center relative overflow-hidden">
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
              stroke="#E2E8F0"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
            <circle
              cx={centerX}
              cy={centerY}
              r={radius * 0.5}
              fill="none"
              stroke="#F1F5F9"
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
                        ? '#FCA5A5'
                        : isSelected
                        ? '#2563EB'
                        : pos.peer.is_connected
                        ? '#93C5FD'
                        : '#CBD5E1'
                    }
                    strokeWidth={isSelected ? 2.5 : pos.peer.is_connected ? 2 : 1}
                    strokeDasharray={isUnresponsive || !pos.peer.is_connected ? '4 4' : undefined}
                    className="transition-all duration-300"
                  />
                  {/* Midpoint Latency or DHT distance label */}
                  {pos.peer.last_latency_ms != null && (
                    <text
                      x={(centerX + pos.x) / 2}
                      y={(centerY + pos.y) / 2 - 4}
                      fill="#64748B"
                      fontSize="9"
                      fontFamily="monospace"
                      textAnchor="middle"
                      className="select-none bg-white"
                    >
                      {pos.peer.last_latency_ms.toFixed(1)}ms
                    </text>
                  )}
                </g>
              );
            })}

            {/* Central Local Node */}
            <g
              onClick={() => setSelectedNode('local')}
              className="cursor-pointer group"
            >
              <circle
                cx={centerX}
                cy={centerY}
                r="32"
                fill="#EFF6FF"
                stroke="#2563EB"
                strokeWidth={selectedNode === 'local' ? 3 : 2}
                className="transition-transform group-hover:scale-105"
              />
              <circle
                cx={centerX}
                cy={centerY}
                r="24"
                fill="#2563EB"
              />
              <text
                x={centerX}
                y={centerY - 2}
                fill="#FFFFFF"
                fontSize="11"
                fontWeight="bold"
                textAnchor="middle"
                alignmentBaseline="middle"
              >
                LOCAL
              </text>
              <text
                x={centerX}
                y={centerY + 10}
                fill="#DBEAFE"
                fontSize="8"
                fontFamily="monospace"
                textAnchor="middle"
                alignmentBaseline="middle"
              >
                :{nodeStatus?.bound_port || '0'}
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
                ? '#FEE2E2'
                : isDegraded
                ? '#FEF3C7'
                : '#ECFDF5';

              const strokeColor = isUnresponsive
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
                    stroke={strokeColor}
                    strokeWidth={isSelected ? 3 : 2}
                    strokeDasharray={isUnresponsive ? '3 3' : undefined}
                    className="transition-all group-hover:scale-110"
                  />
                  <text
                    x={pos.x}
                    y={pos.y - 2}
                    fill="#0F172A"
                    fontSize="9"
                    fontWeight="600"
                    fontFamily="monospace"
                    textAnchor="middle"
                    alignmentBaseline="middle"
                  >
                    {pos.peer.peer_id.slice(0, 6)}
                  </text>
                  <text
                    x={pos.x}
                    y={pos.y + 9}
                    fill="#64748B"
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

          {/* Quick Stats Overlay */}
          <div className="absolute bottom-3 left-4 text-[11px] text-slate-500 bg-white/90 backdrop-blur-xs px-2.5 py-1 rounded-md border border-slate-200">
            Total Nodes: <span className="font-semibold text-slate-800">{totalPeers + 1}</span> (1 Coordinator + {totalPeers} Peers)
          </div>
        </div>

        {/* Selected Node Detail Drawer */}
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs p-5 flex flex-col">
          <div className="flex items-center justify-between pb-3 border-b border-slate-100 mb-4">
            <h3 className="text-sm font-bold text-slate-900 flex items-center gap-2">
              <Server className="w-4 h-4 text-blue-600" />
              Node Telemetry
            </h3>
            {selectedNode && (
              <button
                onClick={() => setSelectedNode(null)}
                className="text-slate-400 hover:text-slate-600 p-1 rounded"
              >
                <X className="w-4 h-4" />
              </button>
            )}
          </div>

          {selectedNode === 'local' ? (
            <div className="space-y-4 flex-1">
              <div className="p-3 bg-blue-50/60 border border-blue-200 rounded-lg">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-blue-900">Local Coordinator</span>
                  <StatusBadge status="active" size="sm" />
                </div>
                <p className="text-[11px] text-blue-700 font-mono mt-1 break-all">
                  {nodeStatus?.node_id}
                </p>
              </div>

              <div className="space-y-2 text-xs">
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Interface:</span>
                  <span className="font-mono text-slate-800 font-medium">{nodeStatus?.host}:{nodeStatus?.bound_port}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Kademlia DHT ID:</span>
                  <span className="font-mono text-slate-800 truncate max-w-[140px]" title={nodeStatus?.dht_id_hex}>
                    {nodeStatus?.dht_id_hex || 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Active Tasks:</span>
                  <span className="font-semibold text-slate-800">{nodeStatus?.active_task_count || 0}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Routing Contacts:</span>
                  <span className="font-semibold text-slate-800">{nodeStatus?.routing_table_contacts || 0}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Gossip Enabled:</span>
                  <span className="font-semibold text-emerald-600">{nodeStatus?.gossip_enabled ? 'Yes' : 'No'}</span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Heartbeat Interval:</span>
                  <span className="font-mono text-slate-800">{nodeStatus?.heartbeat_interval || 1.0}s</span>
                </div>
              </div>

              <div className="mt-4 pt-3 border-t border-slate-100">
                <span className="text-xs font-semibold text-slate-700 block mb-2">Supported Tasks</span>
                <div className="flex flex-wrap gap-1.5">
                  {(nodeStatus?.registered_tasks || []).map((t) => (
                    <span key={t} className="px-2 py-0.5 rounded bg-slate-100 text-slate-700 text-[11px] font-mono border border-slate-200">
                      {t}
                    </span>
                  ))}
                </div>
              </div>
            </div>
          ) : selectedPeerData ? (
            <div className="space-y-4 flex-1">
              <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg">
                <div className="flex items-center justify-between">
                  <span className="text-xs font-bold text-slate-900">Peer Node</span>
                  <StatusBadge status={selectedPeerData.state} size="sm" />
                </div>
                <p className="text-[11px] text-slate-700 font-mono mt-1 break-all">
                  {selectedPeerData.peer_id}
                </p>
              </div>

              <div className="space-y-2 text-xs">
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Endpoint:</span>
                  <span className="font-mono text-slate-800 font-medium">
                    {selectedPeerData.endpoint.host}:{selectedPeerData.endpoint.port}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Health:</span>
                  <StatusBadge status={selectedPeerData.health_status} size="sm" />
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">TCP Stream:</span>
                  <span className="flex items-center gap-1 font-medium text-slate-800">
                    {selectedPeerData.is_connected ? (
                      <>
                        <Wifi className="w-3 h-3 text-emerald-600" /> Connected
                      </>
                    ) : (
                      <>
                        <WifiOff className="w-3 h-3 text-slate-400" /> Disconnected
                      </>
                    )}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Last Latency:</span>
                  <span className="font-mono text-slate-800">
                    {selectedPeerData.last_latency_ms != null ? `${selectedPeerData.last_latency_ms} ms` : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Missed Heartbeats:</span>
                  <span className={selectedPeerData.missed_heartbeats ? 'text-rose-600 font-semibold' : 'text-slate-800'}>
                    {selectedPeerData.missed_heartbeats || 0}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">DHT Distance:</span>
                  <span className="font-mono text-slate-800">
                    {selectedPeerData.dht_distance != null ? selectedPeerData.dht_distance : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1 border-b border-slate-100">
                  <span className="text-slate-500">Last Seen:</span>
                  <span className="text-slate-800">
                    {selectedPeerData.last_seen_seconds_ago.toFixed(1)}s ago
                  </span>
                </div>
              </div>

              {/* Resource Bars */}
              <div className="pt-2 space-y-2 text-xs">
                <div>
                  <div className="flex justify-between text-slate-600 mb-1">
                    <span>CPU Load:</span>
                    <span className="font-semibold">{selectedPeerData.cpu_percent != null ? `${selectedPeerData.cpu_percent.toFixed(1)}%` : 'N/A'}</span>
                  </div>
                  <div className="w-full bg-slate-100 rounded-full h-1.5 overflow-hidden">
                    <div
                      className="bg-blue-600 h-1.5 rounded-full"
                      style={{ width: `${selectedPeerData.cpu_percent || 0}%` }}
                    />
                  </div>
                </div>

                <div>
                  <div className="flex justify-between text-slate-600 mb-1">
                    <span>Memory:</span>
                    <span className="font-semibold">{selectedPeerData.memory_percent != null ? `${selectedPeerData.memory_percent.toFixed(1)}%` : 'N/A'}</span>
                  </div>
                  <div className="w-full bg-slate-100 rounded-full h-1.5 overflow-hidden">
                    <div
                      className="bg-indigo-500 h-1.5 rounded-full"
                      style={{ width: `${selectedPeerData.memory_percent || 0}%` }}
                    />
                  </div>
                </div>
              </div>
            </div>
          ) : (
            <div className="h-64 flex flex-col items-center justify-center text-center text-slate-400">
              <Network className="w-8 h-8 text-slate-300 mb-2" />
              <p className="text-xs font-medium text-slate-600">Select a Node to Inspect</p>
              <p className="text-[11px] text-slate-400 max-w-[200px] mt-1">
                Click on the coordinator or any satellite peer in the graph to view live status.
              </p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

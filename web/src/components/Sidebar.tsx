import React from 'react';
import {
  LayoutDashboard,
  Network,
  Server,
  CheckSquare,
  Activity,
  ScrollText,
  Settings,
  X,
  Share2,
} from 'lucide-react';
import type { NodeStatus } from '../types';

export type PageId = 'overview' | 'topology' | 'peers' | 'tasks' | 'monitoring' | 'events' | 'settings';

interface SidebarProps {
  currentPage: PageId;
  onSelectPage: (page: PageId) => void;
  nodeStatus: NodeStatus | null;
  isOpen: boolean;
  onClose: () => void;
}

export const Sidebar: React.FC<SidebarProps> = ({
  currentPage,
  onSelectPage,
  nodeStatus,
  isOpen,
  onClose,
}) => {
  const navItems: { id: PageId; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'overview', label: 'Overview', icon: LayoutDashboard },
    { id: 'topology', label: 'Network Topology', icon: Network },
    { id: 'peers', label: 'Peers', icon: Server },
    { id: 'tasks', label: 'Tasks', icon: CheckSquare },
    { id: 'monitoring', label: 'Monitoring', icon: Activity },
    { id: 'events', label: 'Events & Logs', icon: ScrollText },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  return (
    <>
      {/* Mobile Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-slate-900/40 backdrop-blur-xs lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar container */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 w-64 bg-white border-r border-slate-200 flex flex-col transition-transform duration-200 ease-in-out lg:static lg:translate-x-0 ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        {/* Brand Header */}
        <div className="h-16 px-6 flex items-center justify-between border-b border-slate-100">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-xl bg-blue-600 flex items-center justify-center text-white shadow-xs">
              <Share2 className="w-5 h-5" />
            </div>
            <div>
              <div className="font-bold text-slate-900 tracking-tight flex items-center gap-1.5 text-base">
                MeshWeaver
                <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded-sm bg-blue-50 text-blue-700 border border-blue-200 uppercase">
                  P2P
                </span>
              </div>
              <div className="text-xs text-slate-500 font-medium truncate max-w-[130px]">
                Async Task Broker
              </div>
            </div>
          </div>
          <button
            onClick={onClose}
            className="lg:hidden p-1.5 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Navigation list */}
        <nav className="flex-1 px-3 py-4 space-y-1 overflow-y-auto">
          {navItems.map((item) => {
            const Icon = item.icon;
            const isActive = currentPage === item.id;
            return (
              <button
                key={item.id}
                onClick={() => {
                  onSelectPage(item.id);
                  onClose();
                }}
                className={`w-full flex items-center gap-3 px-3.5 py-2.5 rounded-lg text-sm font-medium transition ${
                  isActive
                    ? 'bg-blue-50 text-blue-700 font-semibold shadow-xs'
                    : 'text-slate-600 hover:bg-slate-50 hover:text-slate-900'
                }`}
              >
                <Icon
                  className={`w-4 h-4 transition ${
                    isActive ? 'text-blue-600' : 'text-slate-400 group-hover:text-slate-600'
                  }`}
                />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>

        {/* Node status footer */}
        <div className="p-4 border-t border-slate-100 bg-slate-50/50">
          <div className="flex items-center justify-between mb-2">
            <span className="text-xs font-semibold uppercase tracking-wider text-slate-400">
              Local Node
            </span>
            <span className="inline-flex items-center gap-1 text-[11px] font-medium text-emerald-700 bg-emerald-50 px-2 py-0.5 rounded-full border border-emerald-200">
              <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
              Online
            </span>
          </div>

          <div className="bg-white p-2.5 rounded-lg border border-slate-200 shadow-xs space-y-1.5 text-xs">
            <div className="flex justify-between items-center text-slate-600">
              <span className="text-slate-400">Node ID:</span>
              <span className="font-mono font-medium text-slate-800 truncate max-w-[120px]" title={nodeStatus?.node_id || 'Connecting...'}>
                {nodeStatus?.node_id || 'Initializing...'}
              </span>
            </div>
            <div className="flex justify-between items-center text-slate-600">
              <span className="text-slate-400">P2P Port:</span>
              <span className="font-mono font-medium text-slate-800">
                {nodeStatus?.bound_port ? `:${nodeStatus.bound_port}` : 'N/A'}
              </span>
            </div>
            <div className="flex justify-between items-center text-slate-600">
              <span className="text-slate-400">Peers in Mesh:</span>
              <span className="font-semibold text-blue-600">
                {nodeStatus ? nodeStatus.total_registered_peers : 0}
              </span>
            </div>
          </div>
        </div>
      </aside>
    </>
  );
};

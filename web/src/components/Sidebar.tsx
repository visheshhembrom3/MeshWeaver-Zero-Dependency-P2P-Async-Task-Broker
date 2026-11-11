import React, { useState } from 'react';
import {
  LayoutDashboard,
  Network,
  Server,
  CheckSquare,
  FileText,
  Activity,
  ScrollText,
  Settings,
  X,
  Share2,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react';
import type { NodeStatus } from '../types';

export type PageId =
  | 'overview'
  | 'topology'
  | 'peers'
  | 'tasks'
  | 'task-details'
  | 'monitoring'
  | 'events'
  | 'settings';

interface SidebarProps {
  currentPage: PageId;
  onSelectPage: (page: PageId) => void;
  nodeStatus: NodeStatus | null;
  isOpen: boolean;
  onClose: () => void;
  isCollapsed?: boolean;
  onToggleCollapse?: () => void;
}

export const Sidebar: React.FC<SidebarProps> = ({
  currentPage,
  onSelectPage,
  nodeStatus,
  isOpen,
  onClose,
  isCollapsed: controlledCollapsed,
  onToggleCollapse,
}) => {
  const [internalCollapsed, setInternalCollapsed] = useState(false);
  const isCollapsed = controlledCollapsed !== undefined ? controlledCollapsed : internalCollapsed;
  const toggleCollapse = onToggleCollapse || (() => setInternalCollapsed((prev) => !prev));

  const navItems: { id: PageId; label: string; icon: React.ComponentType<{ className?: string }> }[] = [
    { id: 'overview', label: 'Overview', icon: LayoutDashboard },
    { id: 'topology', label: 'Network Topology', icon: Network },
    { id: 'peers', label: 'Peer Explorer', icon: Server },
    { id: 'tasks', label: 'Task Manager', icon: CheckSquare },
    { id: 'task-details', label: 'Task Details', icon: FileText },
    { id: 'monitoring', label: 'Monitoring', icon: Activity },
    { id: 'events', label: 'System Events', icon: ScrollText },
    { id: 'settings', label: 'Settings', icon: Settings },
  ];

  return (
    <>
      {/* Mobile Backdrop */}
      {isOpen && (
        <div
          className="fixed inset-0 z-40 bg-slate-900/50 backdrop-blur-xs lg:hidden"
          onClick={onClose}
        />
      )}

      {/* Sidebar container */}
      <aside
        className={`fixed inset-y-0 left-0 z-50 bg-white dark:bg-slate-900 border-r border-slate-200 dark:border-slate-800 flex flex-col transition-all duration-200 ease-in-out lg:static lg:translate-x-0 ${
          isOpen ? 'translate-x-0' : '-translate-x-full'
        } ${isCollapsed ? 'lg:w-20' : 'lg:w-64'} w-64`}
      >
        {/* Brand Header */}
        <div className="h-16 px-4 flex items-center justify-between border-b border-slate-100 dark:border-slate-800">
          <div className="flex items-center gap-3 overflow-hidden">
            <div className="w-9 h-9 rounded-xl bg-blue-600 dark:bg-blue-500 flex items-center justify-center text-white shadow-xs shrink-0">
              <Share2 className="w-5 h-5" />
            </div>
            {!isCollapsed && (
              <div className="truncate">
                <div className="font-bold text-slate-900 dark:text-slate-100 tracking-tight flex items-center gap-1.5 text-base">
                  MeshWeaver
                  <span className="text-[10px] font-semibold px-1.5 py-0.5 rounded-sm bg-blue-50 dark:bg-blue-950/60 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800 uppercase">
                    P2P
                  </span>
                </div>
                <div className="text-xs text-slate-500 dark:text-slate-400 font-medium truncate">
                  Async Task Broker
                </div>
              </div>
            )}
          </div>

          {/* Mobile close button */}
          <button
            onClick={onClose}
            className="lg:hidden p-1.5 rounded-lg text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800"
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
                title={isCollapsed ? item.label : undefined}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition ${
                  isActive
                    ? 'bg-blue-50 dark:bg-blue-950/50 text-blue-700 dark:text-blue-300 font-semibold shadow-xs'
                    : 'text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800/60 hover:text-slate-900 dark:hover:text-slate-100'
                } ${isCollapsed ? 'justify-center' : ''}`}
              >
                <Icon
                  className={`w-4 h-4 shrink-0 transition ${
                    isActive
                      ? 'text-blue-600 dark:text-blue-400'
                      : 'text-slate-400 dark:text-slate-500 group-hover:text-slate-600'
                  }`}
                />
                {!isCollapsed && <span className="truncate">{item.label}</span>}
              </button>
            );
          })}
        </nav>

        {/* Collapse toggle (Desktop only) */}
        <div className="hidden lg:flex px-3 py-2 border-t border-slate-100 dark:border-slate-800 justify-end">
          <button
            onClick={toggleCollapse}
            className="p-1.5 rounded-lg text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800 transition text-xs flex items-center gap-1.5 w-full justify-center"
            title={isCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {isCollapsed ? (
              <ChevronRight className="w-4 h-4" />
            ) : (
              <>
                <ChevronLeft className="w-4 h-4" />
                <span className="text-[11px] font-medium text-slate-500 dark:text-slate-400">Collapse</span>
              </>
            )}
          </button>
        </div>

        {/* Node status footer */}
        {!isCollapsed ? (
          <div className="p-4 border-t border-slate-100 dark:border-slate-800 bg-slate-50/50 dark:bg-slate-900/50">
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500">
                Local Node
              </span>
              <span className="inline-flex items-center gap-1 text-[11px] font-medium text-emerald-700 dark:text-emerald-400 bg-emerald-50 dark:bg-emerald-950/50 px-2 py-0.5 rounded-full border border-emerald-200 dark:border-emerald-800">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" />
                Online
              </span>
            </div>

            <div className="bg-white dark:bg-slate-800 p-2.5 rounded-lg border border-slate-200 dark:border-slate-700 shadow-xs space-y-1.5 text-xs">
              <div className="flex justify-between items-center text-slate-600 dark:text-slate-400">
                <span className="text-slate-400 dark:text-slate-500">Node ID:</span>
                <span
                  className="font-mono font-medium text-slate-800 dark:text-slate-200 truncate max-w-[120px]"
                  title={nodeStatus?.node_id || 'Connecting...'}
                >
                  {nodeStatus?.node_id || 'Initializing...'}
                </span>
              </div>
              <div className="flex justify-between items-center text-slate-600 dark:text-slate-400">
                <span className="text-slate-400 dark:text-slate-500">P2P Port:</span>
                <span className="font-mono font-medium text-slate-800 dark:text-slate-200">
                  {nodeStatus?.bound_port ? `:${nodeStatus.bound_port}` : 'N/A'}
                </span>
              </div>
              <div className="flex justify-between items-center text-slate-600 dark:text-slate-400">
                <span className="text-slate-400 dark:text-slate-500">Peers in Mesh:</span>
                <span className="font-semibold text-blue-600 dark:text-blue-400">
                  {nodeStatus ? nodeStatus.total_registered_peers : 0}
                </span>
              </div>
            </div>
          </div>
        ) : (
          <div className="p-3 border-t border-slate-100 dark:border-slate-800 flex justify-center">
            <span
              className="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"
              title={`Node online (${nodeStatus?.total_registered_peers ?? 0} peers)`}
            />
          </div>
        )}
      </aside>
    </>
  );
};

import React from 'react';
import { Menu, RefreshCw, WifiOff, Globe, Sun, Moon } from 'lucide-react';
import type { PageId } from './Sidebar';
import type { ThemeMode } from '../types';

interface TopNavProps {
  currentPage: PageId;
  onOpenMobileSidebar: () => void;
  isConnected: boolean;
  onRefresh: () => void;
  isRefreshing: boolean;
  lastUpdated: Date | null;
  activePeersCount: number;
  theme: ThemeMode;
  onToggleTheme: () => void;
}

export const TopNav: React.FC<TopNavProps> = ({
  currentPage,
  onOpenMobileSidebar,
  isConnected,
  onRefresh,
  isRefreshing,
  lastUpdated,
  activePeersCount,
  theme,
  onToggleTheme,
}) => {
  const titles: Record<PageId, string> = {
    overview: 'Network Overview',
    topology: 'Network Topology & DHT',
    peers: 'Peer Explorer',
    tasks: 'Distributed Task Manager',
    'task-details': 'Task Details & Execution History',
    monitoring: 'Resource & Workload Monitoring',
    events: 'System Events & Audit Log',
    settings: 'Dashboard Settings',
  };

  return (
    <header className="h-16 bg-white dark:bg-slate-900 border-b border-slate-200 dark:border-slate-800 px-6 flex items-center justify-between sticky top-0 z-30 transition-colors">
      <div className="flex items-center gap-4">
        <button
          onClick={onOpenMobileSidebar}
          className="lg:hidden p-2 rounded-lg text-slate-500 dark:text-slate-400 hover:text-slate-800 dark:hover:text-slate-100 hover:bg-slate-100 dark:hover:bg-slate-800 transition"
          aria-label="Open mobile menu"
        >
          <Menu className="w-5 h-5" />
        </button>
        <div>
          <h1 className="text-lg font-bold text-slate-900 dark:text-slate-100 leading-tight">
            {titles[currentPage]}
          </h1>
          <p className="text-xs text-slate-500 dark:text-slate-400 hidden sm:block">
            P2P task orchestration and gossip monitoring
          </p>
        </div>
      </div>

      <div className="flex items-center gap-2.5 sm:gap-3">
        {/* Network active peers pill */}
        <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50/70 dark:bg-slate-800/70 text-xs font-medium text-slate-700 dark:text-slate-300">
          <Globe className="w-3.5 h-3.5 text-blue-600 dark:text-blue-400" />
          <span>Active Peers:</span>
          <span className="font-bold text-blue-700 dark:text-blue-300 bg-white dark:bg-slate-900 px-1.5 py-0.5 rounded border border-slate-200 dark:border-slate-700">
            {activePeersCount}
          </span>
        </div>

        {/* Live Backend Connection Indicator */}
        <div
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs font-medium transition ${
            isConnected
              ? 'bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 border-emerald-200 dark:border-emerald-800'
              : 'bg-rose-50 dark:bg-rose-950/40 text-rose-700 dark:text-rose-300 border-rose-200 dark:border-rose-800'
          }`}
          title={isConnected ? 'Connected to MeshWeaver Backend' : 'Disconnected from Backend'}
        >
          {isConnected ? (
            <>
              <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse" />
              <span className="hidden md:inline font-semibold">Mesh Core Live</span>
            </>
          ) : (
            <>
              <WifiOff className="w-3.5 h-3.5 text-rose-600 dark:text-rose-400" />
              <span className="font-semibold">Disconnected</span>
            </>
          )}
        </div>

        {/* Theme Switcher Toggle (Dark / Light) */}
        <button
          onClick={onToggleTheme}
          className="p-2 text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700/60 transition shadow-xs"
          title={`Switch to ${theme === 'dark' ? 'Light' : 'Dark'} Theme`}
          aria-label="Toggle theme"
        >
          {theme === 'dark' ? (
            <Sun className="w-4 h-4 text-amber-400" />
          ) : (
            <Moon className="w-4 h-4 text-slate-600" />
          )}
        </button>

        {/* Manual refresh button */}
        <button
          onClick={onRefresh}
          disabled={isRefreshing}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-300 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700/60 transition shadow-xs disabled:opacity-60"
          title={lastUpdated ? `Last updated: ${lastUpdated.toLocaleTimeString()}` : 'Refresh now'}
        >
          <RefreshCw
            className={`w-3.5 h-3.5 text-slate-500 dark:text-slate-400 ${
              isRefreshing ? 'animate-spin text-blue-600 dark:text-blue-400' : ''
            }`}
          />
          <span className="hidden md:inline">Refresh</span>
        </button>
      </div>
    </header>
  );
};

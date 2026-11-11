import React from 'react';
import { Menu, RefreshCw, Wifi, WifiOff, Globe } from 'lucide-react';
import type { PageId } from './Sidebar';

interface TopNavProps {
  currentPage: PageId;
  onOpenMobileSidebar: () => void;
  isConnected: boolean;
  onRefresh: () => void;
  isRefreshing: boolean;
  lastUpdated: Date | null;
  activePeersCount: number;
}

export const TopNav: React.FC<TopNavProps> = ({
  currentPage,
  onOpenMobileSidebar,
  isConnected,
  onRefresh,
  isRefreshing,
  lastUpdated,
  activePeersCount,
}) => {
  const titles: Record<PageId, string> = {
    overview: 'Network Overview',
    topology: 'Network Topology & DHT',
    peers: 'Mesh Peer Directory',
    tasks: 'Distributed Task Broker',
    monitoring: 'Resource & Workload Monitoring',
    events: 'System Events & Audit Log',
    settings: 'Dashboard Settings',
  };

  return (
    <header className="h-16 bg-white border-b border-slate-200 px-6 flex items-center justify-between sticky top-0 z-30">
      <div className="flex items-center gap-4">
        <button
          onClick={onOpenMobileSidebar}
          className="lg:hidden p-2 rounded-lg text-slate-500 hover:text-slate-800 hover:bg-slate-100 transition"
        >
          <Menu className="w-5 h-5" />
        </button>
        <div>
          <h1 className="text-lg font-bold text-slate-900 leading-tight">
            {titles[currentPage]}
          </h1>
          <p className="text-xs text-slate-500 hidden sm:block">
            P2P task orchestration and gossip monitoring
          </p>
        </div>
      </div>

      <div className="flex items-center gap-3">
        {/* Network status pill */}
        <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 rounded-lg border border-slate-200 bg-slate-50/70 text-xs font-medium text-slate-700">
          <Globe className="w-3.5 h-3.5 text-blue-600" />
          <span>Active Peers:</span>
          <span className="font-bold text-blue-700 bg-white px-1.5 py-0.5 rounded border border-slate-200">
            {activePeersCount}
          </span>
        </div>

        {/* Live Backend Connection Indicator */}
        <div
          className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs font-medium transition ${
            isConnected
              ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
              : 'bg-rose-50 text-rose-700 border-rose-200'
          }`}
          title={isConnected ? 'Connected to MeshWeaver Backend' : 'Disconnected from Backend'}
        >
          {isConnected ? (
            <>
              <Wifi className="w-3.5 h-3.5 text-emerald-600 animate-pulse" />
              <span className="hidden md:inline font-semibold">Mesh Core Live</span>
            </>
          ) : (
            <>
              <WifiOff className="w-3.5 h-3.5 text-rose-600" />
              <span className="font-semibold">Disconnected</span>
            </>
          )}
        </div>

        {/* Manual refresh button */}
        <button
          onClick={onRefresh}
          disabled={isRefreshing}
          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 bg-white border border-slate-200 rounded-lg hover:bg-slate-50 active:bg-slate-100 transition shadow-xs disabled:opacity-60"
          title={lastUpdated ? `Last updated: ${lastUpdated.toLocaleTimeString()}` : 'Refresh now'}
        >
          <RefreshCw className={`w-3.5 h-3.5 text-slate-500 ${isRefreshing ? 'animate-spin text-blue-600' : ''}`} />
          <span className="hidden md:inline">Refresh</span>
        </button>
      </div>
    </header>
  );
};

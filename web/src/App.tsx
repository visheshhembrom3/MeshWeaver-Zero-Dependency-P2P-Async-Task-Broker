import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Sidebar } from './components/Sidebar';
import type { PageId } from './components/Sidebar';
import { TopNav } from './components/TopNav';
import { Overview } from './pages/Overview';
import { Topology } from './pages/Topology';
import { Peers } from './pages/Peers';
import { Tasks } from './pages/Tasks';
import { Monitoring } from './pages/Monitoring';
import { Events } from './pages/Events';
import { Settings } from './pages/Settings';
import { LoadingSpinner } from './components/LoadingSpinner';
import { api, liveFeed } from './api';
import type {
  MeshEvent,
  NodeStatus,
  PeerStatus,
  SystemMetrics,
  TaskAvailable,
  TaskRecord,
} from './types';
import { AlertCircle, RefreshCw } from 'lucide-react';

export const App: React.FC = () => {
  const [currentPage, setCurrentPage] = useState<PageId>('overview');
  const [isMobileSidebarOpen, setIsMobileSidebarOpen] = useState(false);

  // Core telemetry state
  const [nodeStatus, setNodeStatus] = useState<NodeStatus | null>(null);
  const [peers, setPeers] = useState<PeerStatus[]>([]);
  const [tasks, setTasks] = useState<TaskRecord[]>([]);
  const [events, setEvents] = useState<MeshEvent[]>([]);
  const [metrics, setMetrics] = useState<SystemMetrics | null>(null);
  const [availableTasks, setAvailableTasks] = useState<TaskAvailable[]>([]);

  // Connectivity & UX state
  const [isConnected, setIsConnected] = useState<boolean>(false);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [refreshInterval, setRefreshInterval] = useState<number>(5000);

  const refreshTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  // Fetch all backend data
  const loadAllData = useCallback(async (isSilent = false) => {
    if (!isSilent) setIsRefreshing(true);
    setErrorMessage(null);

    try {
      const [
        nodeData,
        peersData,
        tasksData,
        eventsData,
        metricsData,
        availData,
      ] = await Promise.all([
        api.getNodeStatus(),
        api.getPeers(),
        api.getTasks(),
        api.getEvents(100),
        api.getMetrics(),
        api.getAvailableTasks(),
      ]);

      setNodeStatus(nodeData);
      setPeers(peersData);
      setTasks(tasksData);
      setEvents(eventsData);
      setMetrics(metricsData);
      setAvailableTasks(availData);
      setIsConnected(true);
      setLastUpdated(new Date());
    } catch (err) {
      setIsConnected(false);
      setErrorMessage(
        err instanceof Error ? err.message : 'Unable to connect to MeshWeaver core API.'
      );
    } finally {
      setIsLoading(false);
      setIsRefreshing(false);
    }
  }, []);

  // Initial load
  useEffect(() => {
    loadAllData();
  }, [loadAllData]);

  // WebSocket Live Subscription
  useEffect(() => {
    const unsubscribe = liveFeed.subscribe((msg) => {
      if (msg.type === 'connection_change') {
        setIsConnected(Boolean(msg.connected));
      } else if (msg.type === 'event' && msg.event) {
        const newEv = msg.event as MeshEvent;
        setEvents((prev) => [newEv, ...prev.slice(0, 99)]);

        // If a task or peer event arrived, silently refresh tasks/peers
        if (newEv.event_type.startsWith('task_') || newEv.event_type.startsWith('peer_')) {
          loadAllData(true);
        }
      } else if (msg.type === 'heartbeat') {
        setIsConnected(true);
        // Refresh periodically on heartbeat if visible
      }
    });

    return () => {
      unsubscribe();
    };
  }, [loadAllData]);

  // Periodic polling interval
  useEffect(() => {
    if (refreshTimerRef.current) {
      clearInterval(refreshTimerRef.current);
      refreshTimerRef.current = null;
    }

    if (refreshInterval > 0) {
      refreshTimerRef.current = setInterval(() => {
        loadAllData(true);
      }, refreshInterval);
    }

    return () => {
      if (refreshTimerRef.current) {
        clearInterval(refreshTimerRef.current);
      }
    };
  }, [refreshInterval, loadAllData]);

  return (
    <div className="min-h-screen bg-[#F8FAFC] text-[#0F172A] flex flex-col lg:flex-row antialiased">
      {/* Sidebar Navigation */}
      <Sidebar
        currentPage={currentPage}
        onSelectPage={setCurrentPage}
        nodeStatus={nodeStatus}
        isOpen={isMobileSidebarOpen}
        onClose={() => setIsMobileSidebarOpen(false)}
      />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top Navbar */}
        <TopNav
          currentPage={currentPage}
          onOpenMobileSidebar={() => setIsMobileSidebarOpen(true)}
          isConnected={isConnected}
          onRefresh={() => loadAllData(false)}
          isRefreshing={isRefreshing}
          lastUpdated={lastUpdated}
          activePeersCount={peers.filter((p) => p.state === 'active' && !p.is_unresponsive).length}
        />

        {/* Connection Error Banner */}
        {errorMessage && (
          <div className="bg-rose-50 border-b border-rose-200 px-6 py-3 flex items-center justify-between text-xs text-rose-800">
            <div className="flex items-center gap-2">
              <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
              <span>
                <strong>Backend Disconnected:</strong> {errorMessage}. Make sure the MeshWeaver node API is running (<code>python -m meshweaver dashboard</code>).
              </span>
            </div>
            <button
              onClick={() => loadAllData(false)}
              className="px-2.5 py-1 bg-white border border-rose-200 rounded font-medium text-rose-700 hover:bg-rose-100 transition shrink-0 ml-4 flex items-center gap-1"
            >
              <RefreshCw className="w-3 h-3" /> Retry
            </button>
          </div>
        )}

        {/* Content Container */}
        <main className="flex-1 p-6 max-w-7xl w-full mx-auto">
          {isLoading ? (
            <div className="py-24">
              <LoadingSpinner message="Connecting to MeshWeaver core node..." size="lg" />
            </div>
          ) : (
            <>
              {currentPage === 'overview' && (
                <Overview
                  nodeStatus={nodeStatus}
                  peers={peers}
                  tasks={tasks}
                  events={events}
                  metrics={metrics}
                  onNavigate={(page) => setCurrentPage(page)}
                />
              )}

              {currentPage === 'topology' && (
                <Topology
                  nodeStatus={nodeStatus}
                  peers={peers}
                />
              )}

              {currentPage === 'peers' && (
                <Peers
                  peers={peers}
                  onRefresh={() => loadAllData(false)}
                  isRefreshing={isRefreshing}
                />
              )}

              {currentPage === 'tasks' && (
                <Tasks
                  tasks={tasks}
                  availableTasks={availableTasks}
                  onRefresh={() => loadAllData(false)}
                  isRefreshing={isRefreshing}
                />
              )}

              {currentPage === 'monitoring' && (
                <Monitoring
                  metrics={metrics}
                />
              )}

              {currentPage === 'events' && (
                <Events
                  events={events}
                  onRefresh={() => loadAllData(false)}
                  isRefreshing={isRefreshing}
                />
              )}

              {currentPage === 'settings' && (
                <Settings
                  nodeStatus={nodeStatus}
                  refreshInterval={refreshInterval}
                  onSetRefreshInterval={setRefreshInterval}
                  onRefreshAll={() => loadAllData(false)}
                />
              )}
            </>
          )}
        </main>
      </div>
    </div>
  );
};

export default App;

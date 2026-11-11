import React, { useState, useEffect } from 'react';
import {
  Server,
  RefreshCw,
  Sun,
  Moon,
  Shield,
  Info,
  CheckCircle2,
  AlertCircle,
  Sliders,
} from 'lucide-react';
import { getApiBaseUrl, setApiBaseUrl, getApiKey, setApiKey, api } from '../api';
import type { NodeConfigInfo, NodeStatus, ThemeMode } from '../types';

interface SettingsProps {
  nodeStatus?: NodeStatus | null;
  refreshInterval: number;
  onSetRefreshInterval: (interval: number) => void;
  onRefreshAll: () => void;
  theme: ThemeMode;
  onSetTheme: (theme: ThemeMode) => void;
}

export const Settings: React.FC<SettingsProps> = ({
  refreshInterval,
  onSetRefreshInterval,
  onRefreshAll,
  theme,
  onSetTheme,
}) => {
  const [apiUrl, setApiUrl] = useState<string>(getApiBaseUrl());
  const [apiKeyInput, setApiKeyInput] = useState<string>(getApiKey());
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [isTesting, setIsTesting] = useState<boolean>(false);
  const [configInfo, setConfigInfo] = useState<NodeConfigInfo | null>(null);
  const [isLoadingConfig, setIsLoadingConfig] = useState<boolean>(false);

  useEffect(() => {
    setIsLoadingConfig(true);
    api
      .getConfig()
      .then((cfg) => setConfigInfo(cfg))
      .catch(() => setConfigInfo(null))
      .finally(() => setIsLoadingConfig(false));
  }, []);

  const handleSaveApiConnection = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsTesting(true);
    setTestResult(null);
    setApiBaseUrl(apiUrl);
    setApiKey(apiKeyInput);

    try {
      const health = await api.getHealth();
      setTestResult({
        ok: true,
        message: `Successfully connected to node '${health.node_id}' on port ${health.bound_port}!`,
      });
      onRefreshAll();
      // Reload config
      const cfg = await api.getConfig();
      setConfigInfo(cfg);
    } catch (err) {
      setTestResult({
        ok: false,
        message: err instanceof Error ? err.message : 'Failed to reach API endpoint',
      });
    } finally {
      setIsTesting(false);
    }
  };

  const handleResetDefaultUrl = () => {
    setApiUrl('');
    setApiBaseUrl('');
    setTestResult(null);
    onRefreshAll();
  };

  return (
    <div className="max-w-4xl space-y-6">
      {/* Backend API Connection & Auth */}
      <div className="bg-white dark:bg-slate-900 p-6 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs transition-colors">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 dark:border-slate-800 mb-5">
          <div className="w-9 h-9 rounded-xl bg-blue-50 dark:bg-blue-950/60 border border-blue-200 dark:border-blue-800 flex items-center justify-center text-blue-600 dark:text-blue-400">
            <Server className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-base font-bold text-slate-900 dark:text-slate-100">Backend API Connection & Credentials</h3>
            <p className="text-xs text-slate-500 dark:text-slate-400">Configure connection target and API key for privileged actions</p>
          </div>
        </div>

        <form onSubmit={handleSaveApiConnection} className="space-y-4">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1.5">
                API Base URL
              </label>
              <input
                type="text"
                placeholder="Leave blank for relative /api (e.g. http://127.0.0.1:8000)"
                value={apiUrl}
                onChange={(e) => setApiUrl(e.target.value)}
                className="w-full px-3.5 py-2 text-xs bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg font-mono text-slate-800 dark:text-slate-200 focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
              />
              <p className="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                Default connects to same-origin or Vite proxy on port 8000.
              </p>
            </div>

            <div>
              <label className="block text-xs font-semibold text-slate-700 dark:text-slate-300 mb-1.5">
                Privileged API Key / Bearer Token
              </label>
              <div className="relative">
                <input
                  type="password"
                  placeholder="Optional secret key (MESHOPS_API_KEY)"
                  value={apiKeyInput}
                  onChange={(e) => setApiKeyInput(e.target.value)}
                  className="w-full px-3.5 py-2 text-xs bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg font-mono text-slate-800 dark:text-slate-200 focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                />
              </div>
              <p className="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                Required for task dispatch if node configured with API authentication.
              </p>
            </div>
          </div>

          <div className="flex items-center gap-2 pt-2">
            <button
              type="submit"
              disabled={isTesting}
              className="px-4 py-2 bg-blue-600 dark:bg-blue-500 text-white rounded-lg text-xs font-semibold hover:bg-blue-700 dark:hover:bg-blue-600 transition disabled:opacity-60 flex items-center gap-1.5"
            >
              {isTesting && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
              <span>Test & Save Connection</span>
            </button>
            <button
              type="button"
              onClick={handleResetDefaultUrl}
              className="px-3 py-2 border border-slate-200 dark:border-slate-700 text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800 rounded-lg text-xs font-medium transition"
            >
              Reset to Defaults
            </button>
          </div>

          {testResult && (
            <div
              className={`p-3 rounded-lg border text-xs flex items-center gap-2 ${
                testResult.ok
                  ? 'bg-emerald-50 dark:bg-emerald-950/40 text-emerald-800 dark:text-emerald-300 border-emerald-200 dark:border-emerald-800'
                  : 'bg-rose-50 dark:bg-rose-950/40 text-rose-800 dark:text-rose-300 border-rose-200 dark:border-rose-800'
              }`}
            >
              {testResult.ok ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-600 dark:text-emerald-400 shrink-0" />
              ) : (
                <AlertCircle className="w-4 h-4 text-rose-600 dark:text-rose-400 shrink-0" />
              )}
              <span>{testResult.message}</span>
            </div>
          )}
        </form>
      </div>

      {/* Live Runtime Preferences (No Restart Needed) */}
      <div className="bg-white dark:bg-slate-900 p-6 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs transition-colors">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 dark:border-slate-800 mb-5">
          <div className="w-9 h-9 rounded-xl bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 flex items-center justify-center text-slate-700 dark:text-slate-300">
            <Sliders className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-base font-bold text-slate-900 dark:text-slate-100">Live Dashboard Preferences</h3>
              <span className="text-[10px] font-semibold bg-emerald-50 dark:bg-emerald-950/50 text-emerald-700 dark:text-emerald-400 border border-emerald-200 dark:border-emerald-800 px-2 py-0.5 rounded-full">
                Active Immediately • No Restart
              </span>
            </div>
            <p className="text-xs text-slate-500 dark:text-slate-400">Adjust user interface styling and polling cadence</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-xs">
          {/* Polling Interval */}
          <div>
            <label className="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5">
              Telemetry Polling Cadence
            </label>
            <div className="grid grid-cols-4 gap-2">
              {[
                { label: '2 sec', val: 2000 },
                { label: '5 sec', val: 5000 },
                { label: '10 sec', val: 10000 },
                { label: 'Manual', val: 0 },
              ].map((opt) => (
                <button
                  key={opt.val}
                  type="button"
                  onClick={() => onSetRefreshInterval(opt.val)}
                  className={`py-2 text-center rounded-lg font-medium border transition ${
                    refreshInterval === opt.val
                      ? 'bg-blue-600 text-white border-blue-600 shadow-xs'
                      : 'bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
            <p className="text-[11px] text-slate-500 dark:text-slate-400 mt-1.5">
              In addition to polling, the dashboard receives instant push updates via live WebSocket.
            </p>
          </div>

          {/* Theme Selector */}
          <div>
            <label className="block font-semibold text-slate-700 dark:text-slate-300 mb-1.5">
              Interface Color Theme
            </label>
            <div className="grid grid-cols-2 gap-2">
              <button
                type="button"
                onClick={() => onSetTheme('light')}
                className={`py-2 px-3 rounded-lg font-medium border flex items-center justify-center gap-2 transition ${
                  theme === 'light'
                    ? 'bg-blue-600 text-white border-blue-600 shadow-xs'
                    : 'bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700'
                }`}
              >
                <Sun className="w-4 h-4" />
                <span>Light Theme</span>
              </button>
              <button
                type="button"
                onClick={() => onSetTheme('dark')}
                className={`py-2 px-3 rounded-lg font-medium border flex items-center justify-center gap-2 transition ${
                  theme === 'dark'
                    ? 'bg-blue-600 text-white border-blue-600 shadow-xs'
                    : 'bg-slate-50 dark:bg-slate-800 text-slate-700 dark:text-slate-300 border-slate-200 dark:border-slate-700 hover:bg-slate-100 dark:hover:bg-slate-700'
                }`}
              >
                <Moon className="w-4 h-4" />
                <span>Dark Theme</span>
              </button>
            </div>
            <p className="text-[11px] text-slate-500 dark:text-slate-400 mt-1.5">
              Persistent theme preference saved to browser local storage.
            </p>
          </div>
        </div>
      </div>

      {/* Backend Supported Settings & Restart Requirements */}
      <div className="bg-white dark:bg-slate-900 p-6 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs transition-colors">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 dark:border-slate-800 mb-5">
          <div className="w-9 h-9 rounded-xl bg-amber-50 dark:bg-amber-950/60 border border-amber-200 dark:border-amber-800 flex items-center justify-center text-amber-600 dark:text-amber-400">
            <Shield className="w-5 h-5" />
          </div>
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-base font-bold text-slate-900 dark:text-slate-100">Core Node Settings</h3>
              <span className="text-[10px] font-semibold bg-amber-50 dark:bg-amber-950/50 text-amber-700 dark:text-amber-400 border border-amber-200 dark:border-amber-800 px-2 py-0.5 rounded-full">
                Requires Node Restart to Modify
              </span>
            </div>
            <p className="text-xs text-slate-500 dark:text-slate-400">
              Settings bound to running sockets, cryptographic engines, and task executors
            </p>
          </div>
        </div>

        {isLoadingConfig ? (
          <div className="p-8 text-center text-slate-400 text-xs">
            <RefreshCw className="w-5 h-5 animate-spin mx-auto mb-2 text-blue-500" />
            Loading backend configuration parameters...
          </div>
        ) : configInfo ? (
          <div className="space-y-4 text-xs">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {/* Network */}
              <div className="p-3.5 bg-slate-50 dark:bg-slate-800/60 rounded-xl border border-slate-200 dark:border-slate-700 space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-bold text-slate-900 dark:text-slate-100">P2P Network Socket</span>
                  <span className="text-[10px] font-mono font-semibold text-rose-600 dark:text-rose-400 bg-rose-50 dark:bg-rose-950/40 px-1.5 py-0.2 rounded border border-rose-200 dark:border-rose-800">
                    Restart Required
                  </span>
                </div>
                <div className="font-mono text-slate-700 dark:text-slate-300">
                  Interface: <span className="font-semibold">{configInfo.network.host}:{configInfo.network.bound_port}</span>
                </div>
                <p className="text-[11px] text-slate-500 dark:text-slate-400">
                  {configInfo.network.description}. Changing host or port requires rebinding TCP listener.
                </p>
              </div>

              {/* Heartbeats */}
              <div className="p-3.5 bg-slate-50 dark:bg-slate-800/60 rounded-xl border border-slate-200 dark:border-slate-700 space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-bold text-slate-900 dark:text-slate-100">Heartbeat Monitor</span>
                  <span className="text-[10px] font-mono font-semibold text-rose-600 dark:text-rose-400 bg-rose-50 dark:bg-rose-950/40 px-1.5 py-0.2 rounded border border-rose-200 dark:border-rose-800">
                    Restart Required
                  </span>
                </div>
                <div className="font-mono text-slate-700 dark:text-slate-300">
                  Cadence: <span className="font-semibold">{configInfo.heartbeat.interval_seconds}s</span> | Missed Limit: <span className="font-semibold">{configInfo.heartbeat.missed_threshold}</span>
                </div>
                <p className="text-[11px] text-slate-500 dark:text-slate-400">
                  {configInfo.heartbeat.description}. Loop interval is bound at monitor startup.
                </p>
              </div>

              {/* Gossip */}
              <div className="p-3.5 bg-slate-50 dark:bg-slate-800/60 rounded-xl border border-slate-200 dark:border-slate-700 space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-bold text-slate-900 dark:text-slate-100">Gossip Engine</span>
                  <span className="text-[10px] font-mono font-semibold text-rose-600 dark:text-rose-400 bg-rose-50 dark:bg-rose-950/40 px-1.5 py-0.2 rounded border border-rose-200 dark:border-rose-800">
                    Restart Required
                  </span>
                </div>
                <div className="font-mono text-slate-700 dark:text-slate-300">
                  Status: <span className="font-semibold">{configInfo.gossip.enabled ? 'Active' : 'Disabled'}</span> | Interval: <span className="font-semibold">{configInfo.gossip.interval_seconds}s</span>
                </div>
                <p className="text-[11px] text-slate-500 dark:text-slate-400">
                  {configInfo.gossip.description}.
                </p>
              </div>

              {/* Security & TLS */}
              <div className="p-3.5 bg-slate-50 dark:bg-slate-800/60 rounded-xl border border-slate-200 dark:border-slate-700 space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="font-bold text-slate-900 dark:text-slate-100">Transport Security & Auth</span>
                  <span className="text-[10px] font-mono font-semibold text-rose-600 dark:text-rose-400 bg-rose-50 dark:bg-rose-950/40 px-1.5 py-0.2 rounded border border-rose-200 dark:border-rose-800">
                    Restart Required
                  </span>
                </div>
                <div className="font-mono text-slate-700 dark:text-slate-300">
                  TLS: <span className="font-semibold">{configInfo.security.tls_enabled ? 'Enabled' : 'Disabled'}</span> | mTLS: <span className="font-semibold">{configInfo.security.mTLS_required ? 'Required' : 'No'}</span>
                </div>
                <p className="text-[11px] text-slate-500 dark:text-slate-400">
                  {configInfo.security.description}. TLS context and certificates cannot be reloaded without socket rebuild.
                </p>
              </div>
            </div>

            <div className="p-3.5 bg-blue-50/70 dark:bg-blue-950/40 border border-blue-200 dark:border-blue-800 rounded-xl text-xs text-blue-900 dark:text-blue-300 flex items-start gap-2.5">
              <Info className="w-4 h-4 text-blue-600 dark:text-blue-400 shrink-0 mt-0.5" />
              <div>
                <span className="font-bold">Modifying Node Startup Settings: </span>
                To reconfigure these parameters, stop the node with <code className="font-mono bg-blue-100 dark:bg-blue-900/60 px-1 rounded">Ctrl+C</code> and pass updated CLI flags (e.g., <code className="font-mono bg-blue-100 dark:bg-blue-900/60 px-1 rounded">--port</code>, <code className="font-mono bg-blue-100 dark:bg-blue-900/60 px-1 rounded">--tls</code>, <code className="font-mono bg-blue-100 dark:bg-blue-900/60 px-1 rounded">--max-concurrency</code>) or edit your <code className="font-mono bg-blue-100 dark:bg-blue-900/60 px-1 rounded">meshweaver.json</code> configuration file.
              </div>
            </div>
          </div>
        ) : (
          <div className="p-4 text-xs text-slate-500 text-center">
            Connect to a running MeshWeaver node to inspect active backend configuration parameters.
          </div>
        )}
      </div>
    </div>
  );
};

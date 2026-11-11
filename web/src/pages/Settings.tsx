import React, { useState } from 'react';
import {
  Server,
  RefreshCw,
  Sun,
  Shield,
  Info,
  CheckCircle2,
  AlertCircle,
} from 'lucide-react';
import { getApiBaseUrl, setApiBaseUrl, api } from '../api';
import type { NodeStatus } from '../types';

interface SettingsProps {
  nodeStatus?: NodeStatus | null;
  refreshInterval: number;
  onSetRefreshInterval: (interval: number) => void;
  onRefreshAll: () => void;
}

export const Settings: React.FC<SettingsProps> = ({
  refreshInterval,
  onSetRefreshInterval,
  onRefreshAll,
}) => {
  const [apiUrl, setApiUrl] = useState<string>(getApiBaseUrl());
  const [testResult, setTestResult] = useState<{ ok: boolean; message: string } | null>(null);
  const [isTesting, setIsTesting] = useState<boolean>(false);

  const handleSaveApiUrl = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsTesting(true);
    setTestResult(null);
    setApiBaseUrl(apiUrl);

    try {
      const health = await api.getHealth();
      setTestResult({
        ok: true,
        message: `Successfully connected to node '${health.node_id}' on port ${health.bound_port}!`,
      });
      onRefreshAll();
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
      {/* Backend API Configuration */}
      <div className="bg-white p-6 border border-slate-200 rounded-xl shadow-xs">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 mb-5">
          <div className="w-9 h-9 rounded-xl bg-blue-50 border border-blue-200 flex items-center justify-center text-blue-600">
            <Server className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-base font-bold text-slate-900">Backend API Connection</h3>
            <p className="text-xs text-slate-500">Configure connection target to MeshWeaver FastAPI broker adapter</p>
          </div>
        </div>

        <form onSubmit={handleSaveApiUrl} className="space-y-4">
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1.5">
              API Base URL
            </label>
            <div className="flex gap-2">
              <input
                type="text"
                placeholder="Leave blank for relative /api (e.g. http://127.0.0.1:8000)"
                value={apiUrl}
                onChange={(e) => setApiUrl(e.target.value)}
                className="flex-1 px-3.5 py-2 text-xs border border-slate-200 rounded-lg font-mono text-slate-800 focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
              />
              <button
                type="submit"
                disabled={isTesting}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg text-xs font-semibold hover:bg-blue-700 transition disabled:opacity-60 flex items-center gap-1.5"
              >
                {isTesting && <RefreshCw className="w-3.5 h-3.5 animate-spin" />}
                <span>Test & Save</span>
              </button>
              <button
                type="button"
                onClick={handleResetDefaultUrl}
                className="px-3 py-2 border border-slate-200 text-slate-600 hover:bg-slate-50 rounded-lg text-xs font-medium"
              >
                Reset Default
              </button>
            </div>
            <p className="text-[11px] text-slate-500 mt-1.5">
              Default uses Vite proxy or same-origin host. Use an absolute URL if running frontend independently on a remote port.
            </p>
          </div>

          {testResult && (
            <div
              className={`p-3 rounded-lg border text-xs flex items-center gap-2 ${
                testResult.ok
                  ? 'bg-emerald-50 text-emerald-800 border-emerald-200'
                  : 'bg-rose-50 text-rose-800 border-rose-200'
              }`}
            >
              {testResult.ok ? (
                <CheckCircle2 className="w-4 h-4 text-emerald-600 shrink-0" />
              ) : (
                <AlertCircle className="w-4 h-4 text-rose-600 shrink-0" />
              )}
              <span>{testResult.message}</span>
            </div>
          )}
        </form>
      </div>

      {/* Preferences & Refresh Cadence */}
      <div className="bg-white p-6 border border-slate-200 rounded-xl shadow-xs">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 mb-5">
          <div className="w-9 h-9 rounded-xl bg-slate-50 border border-slate-200 flex items-center justify-center text-slate-700">
            <RefreshCw className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-base font-bold text-slate-900">Dashboard Cadence & UI</h3>
            <p className="text-xs text-slate-500">Telemetry polling intervals and appearance preferences</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-6 text-xs">
          <div>
            <label className="block font-semibold text-slate-700 mb-1.5">
              Background Polling Interval
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
                      : 'bg-slate-50 text-slate-700 border-slate-200 hover:bg-slate-100'
                  }`}
                >
                  {opt.label}
                </button>
              ))}
            </div>
            <p className="text-[11px] text-slate-500 mt-1.5">
              In addition to periodic polling, the dashboard receives instant push updates via live WebSocket.
            </p>
          </div>

          <div>
            <label className="block font-semibold text-slate-700 mb-1.5">
              UI Theme
            </label>
            <div className="flex items-center gap-3 p-3 bg-slate-50 border border-slate-200 rounded-xl">
              <div className="w-8 h-8 rounded-lg bg-amber-50 border border-amber-200 flex items-center justify-center text-amber-600">
                <Sun className="w-4 h-4" />
              </div>
              <div>
                <span className="font-bold text-slate-900 block">Clean Light Theme</span>
                <span className="text-[11px] text-slate-500">
                  Optimized for professional monitoring with high-contrast slate palette
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Architecture & Reliability Spec Card */}
      <div className="bg-white p-6 border border-slate-200 rounded-xl shadow-xs">
        <div className="flex items-center gap-3 pb-4 border-b border-slate-100 mb-5">
          <div className="w-9 h-9 rounded-xl bg-blue-50 border border-blue-200 flex items-center justify-center text-blue-600">
            <Shield className="w-5 h-5" />
          </div>
          <div>
            <h3 className="text-base font-bold text-slate-900">MeshWeaver System Specifications</h3>
            <p className="text-xs text-slate-500">Core architecture, protocol parameters, and security boundaries</p>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 text-xs">
          <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-1">
            <span className="text-slate-400 font-semibold uppercase tracking-wider text-[10px] block">
              Core Runtime
            </span>
            <div className="font-semibold text-slate-900">Python 3.11+ (Standard Library)</div>
            <div className="text-slate-500 text-[11px]">Zero runtime dependencies for mesh core</div>
          </div>

          <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-1">
            <span className="text-slate-400 font-semibold uppercase tracking-wider text-[10px] block">
              Peer Discovery
            </span>
            <div className="font-semibold text-slate-900">Kademlia DHT (160-bit SHA-1)</div>
            <div className="text-slate-500 text-[11px]">XOR metric routing with k-buckets (k=20)</div>
          </div>

          <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-1">
            <span className="text-slate-400 font-semibold uppercase tracking-wider text-[10px] block">
              Failure Recovery
            </span>
            <div className="font-semibold text-slate-900">Active Heartbeats & Bounded Retries</div>
            <div className="text-slate-500 text-[11px]">Automatic failover with at-least-once delivery</div>
          </div>

          <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg space-y-1">
            <span className="text-slate-400 font-semibold uppercase tracking-wider text-[10px] block">
              Security Boundary
            </span>
            <div className="font-semibold text-emerald-700">Strict Function Allowlisting</div>
            <div className="text-slate-500 text-[11px]">Zero arbitrary eval, code injection, or pickle payloads</div>
          </div>
        </div>

        <div className="mt-5 p-3.5 bg-blue-50/70 border border-blue-200 rounded-xl text-xs text-blue-900 flex items-start gap-2.5">
          <Info className="w-4 h-4 text-blue-600 shrink-0 mt-0.5" />
          <div>
            <span className="font-bold">Distributed Execution Semantics: </span>
            Retrying a task upon worker disconnection or timeout provides at-least-once execution semantics and may execute a task more than once if a worker computed before dropping the connection. MeshWeaver explicitly does not claim exactly-once execution.
          </div>
        </div>
      </div>
    </div>
  );
};

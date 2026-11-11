import React, { useState } from 'react';
import {
  ScrollText,
  Search,
  AlertTriangle,
  CheckCircle2,
  Info,
  ChevronDown,
  ChevronRight,
  RefreshCw,
  Clock,
} from 'lucide-react';
import type { MeshEvent } from '../types';
import { EmptyState } from '../components/EmptyState';

interface EventsProps {
  events: MeshEvent[];
  onRefresh: () => void;
  isRefreshing: boolean;
}

export const Events: React.FC<EventsProps> = ({ events, onRefresh, isRefreshing }) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [severityFilter, setSeverityFilter] = useState<'all' | 'info' | 'success' | 'warning' | 'error'>('all');
  const [expandedEventId, setExpandedEventId] = useState<string | null>(null);

  const filteredEvents = events.filter((ev) => {
    const matchesSearch =
      ev.message.toLowerCase().includes(searchQuery.toLowerCase()) ||
      ev.event_type.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (ev.peer_id && ev.peer_id.toLowerCase().includes(searchQuery.toLowerCase())) ||
      (ev.task_id && ev.task_id.toLowerCase().includes(searchQuery.toLowerCase()));

    if (severityFilter === 'all') return matchesSearch;
    return matchesSearch && ev.severity === severityFilter;
  });

  const getSeverityBadge = (sev: string) => {
    switch (sev) {
      case 'error':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-rose-50 dark:bg-rose-950/40 text-rose-700 dark:text-rose-300 border border-rose-200 dark:border-rose-800">
            <AlertTriangle className="w-3 h-3 text-rose-500" />
            Error
          </span>
        );
      case 'warning':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-amber-50 dark:bg-amber-950/40 text-amber-700 dark:text-amber-300 border border-amber-200 dark:border-amber-800">
            <AlertTriangle className="w-3 h-3 text-amber-500" />
            Warning
          </span>
        );
      case 'success':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-50 dark:bg-emerald-950/40 text-emerald-700 dark:text-emerald-300 border border-emerald-200 dark:border-emerald-800">
            <CheckCircle2 className="w-3 h-3 text-emerald-500" />
            Success
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-blue-50 dark:bg-blue-950/40 text-blue-700 dark:text-blue-300 border border-blue-200 dark:border-blue-800">
            <Info className="w-3 h-3 text-blue-500" />
            Info
          </span>
        );
    }
  };

  return (
    <div className="space-y-6">
      {/* Controls Bar */}
      <div className="bg-white dark:bg-slate-900 p-4 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs flex flex-col sm:flex-row sm:items-center justify-between gap-4 transition-colors">
        {/* Search Input */}
        <div className="relative flex-1 max-w-md">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search events, messages, peer ID, or task ID..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-4 py-2 text-xs bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-hidden focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>

        {/* Severity Filter Tabs */}
        <div className="flex items-center gap-2 overflow-x-auto pb-1 sm:pb-0">
          {(['all', 'info', 'success', 'warning', 'error'] as const).map((sev) => (
            <button
              key={sev}
              onClick={() => setSeverityFilter(sev)}
              className={`px-3 py-1.5 text-xs font-medium rounded-lg capitalize transition whitespace-nowrap ${
                severityFilter === sev
                  ? 'bg-blue-600 text-white shadow-xs'
                  : 'bg-slate-50 dark:bg-slate-800 text-slate-600 dark:text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-700'
              }`}
            >
              {sev}
            </button>
          ))}

          <button
            onClick={onRefresh}
            disabled={isRefreshing}
            className="p-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-50 transition"
            title="Refresh events"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-blue-600 dark:text-blue-400' : ''}`} />
          </button>
        </div>
      </div>

      {/* Events Stream List */}
      {filteredEvents.length === 0 ? (
        <EmptyState
          title="No System Events Logged"
          description={
            searchQuery || severityFilter !== 'all'
              ? 'No events match your current search query or severity filter.'
              : 'Network events, peer connections, heartbeats, and task completions will stream here live.'
          }
          icon={ScrollText}
        />
      ) : (
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs overflow-hidden transition-colors">
          <div className="divide-y divide-slate-100 dark:divide-slate-800/80">
            {filteredEvents.map((ev) => {
              const isExpanded = expandedEventId === ev.id;
              return (
                <div
                  key={ev.id}
                  className="p-4 hover:bg-slate-50/70 dark:hover:bg-slate-800/40 transition cursor-pointer"
                  onClick={() => setExpandedEventId(isExpanded ? null : ev.id)}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="flex items-start gap-3 min-w-0">
                      <div className="mt-1">
                        {isExpanded ? (
                          <ChevronDown className="w-4 h-4 text-slate-400" />
                        ) : (
                          <ChevronRight className="w-4 h-4 text-slate-400" />
                        )}
                      </div>

                      <div className="min-w-0">
                        <div className="flex items-center gap-2 mb-1 flex-wrap">
                          {getSeverityBadge(ev.severity)}
                          <span className="font-mono text-xs font-semibold text-slate-800 dark:text-slate-200 bg-slate-100 dark:bg-slate-800 px-2 py-0.5 rounded border border-slate-200 dark:border-slate-700">
                            {ev.event_type}
                          </span>
                          {ev.peer_id && (
                            <span className="font-mono text-[11px] text-slate-500 dark:text-slate-400">
                              peer: <span className="font-medium text-slate-700 dark:text-slate-300">{ev.peer_id}</span>
                            </span>
                          )}
                          {ev.task_id && (
                            <span className="font-mono text-[11px] text-slate-500 dark:text-slate-400">
                              task: <span className="font-medium text-slate-700 dark:text-slate-300">{ev.task_id.slice(0, 8)}...</span>
                            </span>
                          )}
                        </div>

                        <p className="text-xs text-slate-900 dark:text-slate-100 font-medium">
                          {ev.message}
                        </p>
                      </div>
                    </div>

                    <div className="text-right shrink-0">
                      <span className="text-[11px] text-slate-400 dark:text-slate-500 flex items-center gap-1">
                        <Clock className="w-3 h-3" />
                        {new Date(ev.timestamp * 1000).toLocaleTimeString()}
                      </span>
                    </div>
                  </div>

                  {/* Expanded Details JSON Payload */}
                  {isExpanded && ev.details && Object.keys(ev.details).length > 0 && (
                    <div className="mt-3 pl-7 pt-2 border-t border-slate-100 dark:border-slate-800">
                      <span className="text-[11px] font-semibold text-slate-500 dark:text-slate-400 block mb-1">
                        Event Metadata & Structured Payload:
                      </span>
                      <pre className="text-xs font-mono bg-slate-50 dark:bg-slate-800/80 p-3 rounded-lg border border-slate-200 dark:border-slate-700 overflow-x-auto text-slate-800 dark:text-slate-200">
                        {JSON.stringify(ev.details, null, 2)}
                      </pre>
                    </div>
                  )}
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};

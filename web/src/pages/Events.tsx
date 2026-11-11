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
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-rose-50 text-rose-700 border border-rose-200">
            <AlertTriangle className="w-3 h-3 text-rose-500" />
            Error
          </span>
        );
      case 'warning':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-amber-50 text-amber-700 border border-amber-200">
            <AlertTriangle className="w-3 h-3 text-amber-500" />
            Warning
          </span>
        );
      case 'success':
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-50 text-emerald-700 border border-emerald-200">
            <CheckCircle2 className="w-3 h-3 text-emerald-500" />
            Success
          </span>
        );
      default:
        return (
          <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold bg-blue-50 text-blue-700 border border-blue-200">
            <Info className="w-3 h-3 text-blue-500" />
            Info
          </span>
        );
    }
  };

  return (
    <div className="space-y-6">
      {/* Controls Bar */}
      <div className="bg-white p-4 border border-slate-200 rounded-xl shadow-xs flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        {/* Search Input */}
        <div className="relative flex-1 max-w-md">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search events, messages, peer ID, or task ID..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-4 py-2 text-xs border border-slate-200 rounded-lg focus:outline-hidden focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
                  : 'bg-slate-50 text-slate-600 hover:bg-slate-100 border border-slate-200'
              }`}
            >
              {sev}
            </button>
          ))}

          <button
            onClick={onRefresh}
            disabled={isRefreshing}
            className="p-2 border border-slate-200 rounded-lg text-slate-600 hover:bg-slate-50 disabled:opacity-50"
            title="Refresh events"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-blue-600' : ''}`} />
          </button>
        </div>
      </div>

      {/* Events Stream Feed */}
      {filteredEvents.length === 0 ? (
        <EmptyState
          title="No Events Found"
          description={
            searchQuery || severityFilter !== 'all'
              ? 'No system events matched your filter criteria.'
              : 'No lifecycle events have been recorded yet.'
          }
          icon={ScrollText}
        />
      ) : (
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs overflow-hidden divide-y divide-slate-100">
          {filteredEvents.map((ev) => {
            const isExpanded = expandedEventId === ev.id;
            const hasDetails = ev.details && Object.keys(ev.details).length > 0;

            return (
              <div
                key={ev.id}
                onClick={() => hasDetails && setExpandedEventId(isExpanded ? null : ev.id)}
                className={`p-4 transition ${hasDetails ? 'hover:bg-slate-50/80 cursor-pointer' : ''}`}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="flex items-start gap-3">
                    <div className="mt-0.5">{getSeverityBadge(ev.severity)}</div>
                    <div>
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="px-2 py-0.5 rounded bg-slate-100 text-slate-700 text-[11px] font-mono border border-slate-200 font-semibold">
                          {ev.event_type}
                        </span>
                        {ev.peer_id && (
                          <span className="text-[11px] font-mono text-slate-600 bg-slate-50 px-1.5 py-0.5 rounded border border-slate-200">
                            peer: {ev.peer_id}
                          </span>
                        )}
                        {ev.task_id && (
                          <span className="text-[11px] font-mono text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded border border-blue-200">
                            task: {ev.task_id.slice(0, 10)}...
                          </span>
                        )}
                      </div>
                      <p className="text-xs font-semibold text-slate-900 mt-1">
                        {ev.message}
                      </p>
                    </div>
                  </div>

                  <div className="flex items-center gap-2 shrink-0 text-right">
                    <span className="text-[11px] text-slate-400 font-mono flex items-center gap-1">
                      <Clock className="w-3 h-3" />
                      {new Date(ev.timestamp * 1000).toLocaleTimeString()}
                    </span>
                    {hasDetails && (
                      <span className="text-slate-400">
                        {isExpanded ? (
                          <ChevronDown className="w-4 h-4" />
                        ) : (
                          <ChevronRight className="w-4 h-4" />
                        )}
                      </span>
                    )}
                  </div>
                </div>

                {/* Expanded Details Payload */}
                {isExpanded && hasDetails && (
                  <div className="mt-3 pt-3 border-t border-slate-100 pl-4">
                    <span className="text-[11px] font-semibold text-slate-500 uppercase tracking-wider block mb-1">
                      Event Metadata Details
                    </span>
                    <pre className="text-xs font-mono bg-slate-50 p-2.5 rounded-lg border border-slate-200 text-slate-800 whitespace-pre-wrap">
                      {JSON.stringify(ev.details, null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
};

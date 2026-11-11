import React, { useState, useEffect } from 'react';
import {
  CheckSquare,
  Search,
  Clock,
  RotateCcw,
  CheckCircle2,
  AlertTriangle,
  Server,
  FileCode,
  Copy,
  Check,
  RefreshCw,
} from 'lucide-react';
import type { TaskRecord } from '../types';
import { StatusBadge } from '../components/StatusBadge';
import { EmptyState } from '../components/EmptyState';
import { api } from '../api';

interface TaskDetailsProps {
  tasks: TaskRecord[];
  selectedTaskId?: string | null;
  onSelectTask?: (taskId: string) => void;
  onRefresh: () => void;
  isRefreshing: boolean;
}

export const TaskDetails: React.FC<TaskDetailsProps> = ({
  tasks,
  selectedTaskId: initialTaskId,
  onSelectTask,
  onRefresh,
  isRefreshing,
}) => {
  const [activeTaskId, setActiveTaskId] = useState<string>(initialTaskId || (tasks[0]?.task_id ?? ''));
  const [taskDetail, setTaskDetail] = useState<TaskRecord | null>(null);
  const [isLoadingDetail, setIsLoadingDetail] = useState<boolean>(false);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [copiedResult, setCopiedResult] = useState<boolean>(false);

  // Sync if prop changes
  useEffect(() => {
    if (initialTaskId) {
      setActiveTaskId(initialTaskId);
    }
  }, [initialTaskId]);

  // If no task selected yet and tasks available, pick first
  useEffect(() => {
    if (!activeTaskId && tasks.length > 0) {
      setActiveTaskId(tasks[0].task_id);
    }
  }, [activeTaskId, tasks]);

  // Fetch full details whenever activeTaskId changes
  useEffect(() => {
    if (!activeTaskId) {
      setTaskDetail(null);
      return;
    }

    let isMounted = true;
    setIsLoadingDetail(true);

    api
      .getTask(activeTaskId)
      .then((detail) => {
        if (isMounted) setTaskDetail(detail);
      })
      .catch(() => {
        // Fallback to record in task list if direct API query fails
        if (isMounted) {
          const fallback = tasks.find((t) => t.task_id === activeTaskId) || null;
          setTaskDetail(fallback);
        }
      })
      .finally(() => {
        if (isMounted) setIsLoadingDetail(false);
      });

    return () => {
      isMounted = false;
    };
  }, [activeTaskId, tasks]);

  const handleCopyResult = (content: string) => {
    navigator.clipboard.writeText(content);
    setCopiedResult(true);
    setTimeout(() => setCopiedResult(false), 2000);
  };

  const filteredTasks = tasks.filter(
    (t) =>
      t.task_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.task_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (t.routed_to && t.routed_to.toLowerCase().includes(searchQuery.toLowerCase()))
  );

  if (tasks.length === 0) {
    return (
      <EmptyState
        title="No Tasks Available to Inspect"
        description="No tasks have been submitted yet. Go to Task Manager to submit an allowlisted task and observe its execution state transitions."
        icon={CheckSquare}
      />
    );
  }

  return (
    <div className="space-y-6">
      {/* Top Header Card */}
      <div className="bg-white dark:bg-slate-900 p-5 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
          <h2 className="text-base font-bold text-slate-900 dark:text-slate-100 flex items-center gap-2">
            <CheckSquare className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            Task Details & Execution History
          </h2>
          <p className="text-xs text-slate-500 dark:text-slate-400 mt-1">
            Observe granular lifecycle transitions, worker routing history, retry budgets, and computed outcomes.
          </p>
        </div>

        {/* Task Selector Dropdown & Refresh */}
        <div className="flex items-center gap-3">
          <label className="text-xs font-semibold text-slate-600 dark:text-slate-400 shrink-0">
            Select Task:
          </label>
          <select
            value={activeTaskId}
            onChange={(e) => {
              setActiveTaskId(e.target.value);
              onSelectTask?.(e.target.value);
            }}
            className="text-xs font-mono py-1.5 px-3 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-hidden focus:ring-2 focus:ring-blue-500 max-w-xs truncate"
          >
            {tasks.map((t) => (
              <option key={t.task_id} value={t.task_id}>
                [{t.status.toUpperCase()}] {t.task_name} ({t.task_id.slice(0, 8)}...)
              </option>
            ))}
          </select>
          <button
            onClick={onRefresh}
            disabled={isRefreshing}
            className="p-1.5 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-400 hover:bg-slate-50 dark:hover:bg-slate-800 disabled:opacity-50 transition"
            title="Refresh task telemetry"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isRefreshing ? 'animate-spin text-blue-600 dark:text-blue-400' : ''}`} />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Left: Task Search & List Sidebar */}
        <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl shadow-xs p-4 flex flex-col h-[640px]">
          <div className="relative mb-3">
            <Search className="w-3.5 h-3.5 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
            <input
              type="text"
              placeholder="Filter tasks..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="w-full pl-8 pr-3 py-1.5 text-xs bg-slate-50 dark:bg-slate-800/80 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-800 dark:text-slate-200 focus:outline-hidden focus:ring-2 focus:ring-blue-500"
            />
          </div>

          <div className="flex-1 overflow-y-auto divide-y divide-slate-100 dark:divide-slate-800/60 pr-1">
            {filteredTasks.map((t) => {
              const isSelected = t.task_id === activeTaskId;
              return (
                <div
                  key={t.task_id}
                  onClick={() => {
                    setActiveTaskId(t.task_id);
                    onSelectTask?.(t.task_id);
                  }}
                  className={`p-3 rounded-lg cursor-pointer transition text-xs ${
                    isSelected
                      ? 'bg-blue-50 dark:bg-blue-950/40 border border-blue-200 dark:border-blue-800 shadow-xs'
                      : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'
                  }`}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="font-bold text-slate-900 dark:text-slate-100 font-mono">
                      {t.task_name}
                    </span>
                    <StatusBadge status={t.status} size="sm" />
                  </div>
                  <div className="font-mono text-[11px] text-slate-500 dark:text-slate-400 truncate">
                    {t.task_id}
                  </div>
                  <div className="flex items-center justify-between text-[11px] text-slate-400 dark:text-slate-500 mt-1.5">
                    <span>{t.routed_to || 'Local Node'}</span>
                    <span>
                      {t.execution_time != null
                        ? `${(t.execution_time * 1000).toFixed(1)} ms`
                        : t.timestamp
                        ? new Date(t.timestamp * 1000).toLocaleTimeString()
                        : '—'}
                    </span>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Right: Selected Task Inspector View */}
        <div className="lg:col-span-2 space-y-6">
          {isLoadingDetail && !taskDetail ? (
            <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-12 text-center text-slate-500">
              <Clock className="w-8 h-8 text-blue-500 animate-spin mx-auto mb-2" />
              <p className="text-xs">Loading task execution telemetry...</p>
            </div>
          ) : taskDetail ? (
            <>
              {/* Task Header Card */}
              <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-5 shadow-xs">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 pb-4 border-b border-slate-100 dark:border-slate-800">
                  <div>
                    <div className="flex items-center gap-3">
                      <span className="text-xl font-bold font-mono text-slate-900 dark:text-slate-100">
                        {taskDetail.task_name}
                      </span>
                      <StatusBadge status={taskDetail.status} size="md" />
                    </div>
                    <div className="font-mono text-xs text-slate-500 dark:text-slate-400 mt-1 break-all select-all">
                      Task ID: <span className="font-semibold text-slate-700 dark:text-slate-300">{taskDetail.task_id}</span>
                    </div>
                    {taskDetail.correlation_id && (
                      <div className="font-mono text-[11px] text-slate-400 dark:text-slate-500 mt-0.5 break-all">
                        Correlation ID: {taskDetail.correlation_id}
                      </div>
                    )}
                  </div>

                  <div className="text-right sm:border-l sm:pl-4 border-slate-200 dark:border-slate-800">
                    <span className="text-[11px] text-slate-400 uppercase tracking-wider font-semibold block">
                      Execution Time
                    </span>
                    <span className="text-lg font-bold font-mono text-blue-600 dark:text-blue-400">
                      {taskDetail.execution_time != null
                        ? `${(taskDetail.execution_time * 1000).toFixed(2)} ms`
                        : '—'}
                    </span>
                  </div>
                </div>

                {/* State Transition Timeline */}
                <div className="mt-5">
                  <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 dark:text-slate-500 mb-3">
                    Lifecycle State Transitions
                  </h4>
                  <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
                    <div className="p-3 bg-slate-50 dark:bg-slate-800/60 rounded-lg border border-slate-200 dark:border-slate-700/60">
                      <div className="flex items-center gap-1.5 text-xs font-bold text-slate-800 dark:text-slate-200">
                        <CheckCircle2 className="w-3.5 h-3.5 text-blue-500" />
                        <span>1. Submitted</span>
                      </div>
                      <div className="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                        {taskDetail.timestamp
                          ? new Date(taskDetail.timestamp * 1000).toLocaleTimeString()
                          : 'Recorded'}
                      </div>
                    </div>

                    <div className="p-3 bg-slate-50 dark:bg-slate-800/60 rounded-lg border border-slate-200 dark:border-slate-700/60">
                      <div className="flex items-center gap-1.5 text-xs font-bold text-slate-800 dark:text-slate-200">
                        <Server className="w-3.5 h-3.5 text-indigo-500" />
                        <span>2. Dispatched / Ack</span>
                      </div>
                      <div className="text-[11px] text-slate-500 dark:text-slate-400 mt-1 truncate" title={taskDetail.routed_to || 'Local Node'}>
                        Worker: {taskDetail.routed_to || 'Local'}
                      </div>
                    </div>

                    <div className="p-3 bg-slate-50 dark:bg-slate-800/60 rounded-lg border border-slate-200 dark:border-slate-700/60">
                      <div className="flex items-center gap-1.5 text-xs font-bold text-slate-800 dark:text-slate-200">
                        {taskDetail.status === 'success' ? (
                          <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />
                        ) : ['failure', 'error', 'timeout'].includes(taskDetail.status) ? (
                          <AlertTriangle className="w-3.5 h-3.5 text-rose-500" />
                        ) : (
                          <Clock className="w-3.5 h-3.5 text-amber-500" />
                        )}
                        <span>3. {taskDetail.status.toUpperCase()}</span>
                      </div>
                      <div className="text-[11px] text-slate-500 dark:text-slate-400 mt-1">
                        {taskDetail.completed_at
                          ? new Date(taskDetail.completed_at * 1000).toLocaleTimeString()
                          : taskDetail.status === 'running'
                          ? 'In Execution'
                          : 'Pending'}
                      </div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Execution & Retry History Cards */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-4 shadow-xs space-y-2 text-xs">
                  <div className="font-bold text-slate-900 dark:text-slate-100 flex items-center gap-1.5 mb-2">
                    <Server className="w-4 h-4 text-blue-600 dark:text-blue-400" />
                    <span>Worker Routing</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Assigned Peer:</span>
                    <span className="font-mono font-medium text-slate-800 dark:text-slate-200">
                      {taskDetail.routed_to || 'Local Node'}
                    </span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Worker History:</span>
                    <span className="font-mono text-slate-700 dark:text-slate-300">
                      {taskDetail.worker_history && taskDetail.worker_history.length > 0
                        ? taskDetail.worker_history.join(' -> ')
                        : taskDetail.routed_to || 'Local Node'}
                    </span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Execution Semantics:</span>
                    <span className="font-semibold text-amber-600 dark:text-amber-400 bg-amber-50 dark:bg-amber-950/40 px-2 py-0.5 rounded border border-amber-200 dark:border-amber-800/60 text-[11px]">
                      At-Least-Once Delivery
                    </span>
                  </div>
                </div>

                <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-4 shadow-xs space-y-2 text-xs">
                  <div className="font-bold text-slate-900 dark:text-slate-100 flex items-center gap-1.5 mb-2">
                    <RotateCcw className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
                    <span>Retry Information</span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Attempts / Retries:</span>
                    <span className="font-semibold text-slate-800 dark:text-slate-200">
                      {taskDetail.retry_count ?? 0} used of {taskDetail.max_retries ?? 2} max
                    </span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Worker Acknowledgement:</span>
                    <span className="text-slate-700 dark:text-slate-300 font-mono">
                      {taskDetail.ack ? 'Received & Verified' : 'Standard'}
                    </span>
                  </div>
                  <div className="flex justify-between py-1 border-b border-slate-100 dark:border-slate-800">
                    <span className="text-slate-500 dark:text-slate-400">Allowlist Protection:</span>
                    <span className="text-emerald-600 dark:text-emerald-400 font-medium">
                      Allowlisted Function
                    </span>
                  </div>
                </div>
              </div>

              {/* Outcome / Result / Error Details */}
              <div className="bg-white dark:bg-slate-900 border border-slate-200 dark:border-slate-800 rounded-xl p-5 shadow-xs">
                <div className="flex items-center justify-between pb-3 border-b border-slate-100 dark:border-slate-800 mb-4">
                  <div className="flex items-center gap-2">
                    <FileCode className="w-4 h-4 text-blue-600 dark:text-blue-400" />
                    <h3 className="text-xs font-bold text-slate-900 dark:text-slate-100 uppercase tracking-wider">
                      Execution Outcome & Payload
                    </h3>
                  </div>

                  {taskDetail.result !== undefined && taskDetail.result !== null && (
                    <button
                      onClick={() => handleCopyResult(JSON.stringify(taskDetail.result, null, 2))}
                      className="inline-flex items-center gap-1 px-2.5 py-1 text-xs text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-slate-100 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded transition"
                    >
                      {copiedResult ? (
                        <>
                          <Check className="w-3.5 h-3.5 text-emerald-600" />
                          <span>Copied</span>
                        </>
                      ) : (
                        <>
                          <Copy className="w-3.5 h-3.5" />
                          <span>Copy JSON</span>
                        </>
                      )}
                    </button>
                  )}
                </div>

                {taskDetail.result !== undefined && taskDetail.result !== null ? (
                  <div className="p-4 bg-emerald-50/60 dark:bg-emerald-950/20 border border-emerald-200 dark:border-emerald-800/60 rounded-xl">
                    <span className="text-xs font-bold text-emerald-800 dark:text-emerald-300 block mb-2">
                      Computed Return Value:
                    </span>
                    <pre className="font-mono text-xs text-emerald-950 dark:text-emerald-200 bg-emerald-100/50 dark:bg-emerald-900/30 p-3 rounded-lg overflow-x-auto whitespace-pre-wrap break-all">
                      {JSON.stringify(taskDetail.result, null, 2)}
                    </pre>
                  </div>
                ) : taskDetail.error ? (
                  <div className="p-4 bg-rose-50/70 dark:bg-rose-950/20 border border-rose-200 dark:border-rose-800/60 rounded-xl">
                    <div className="flex items-center gap-2 text-rose-800 dark:text-rose-300 font-bold text-xs mb-2">
                      <AlertTriangle className="w-4 h-4 text-rose-600 shrink-0" />
                      <span>Execution Error [{taskDetail.error.code}]</span>
                    </div>
                    <p className="font-mono text-xs text-rose-900 dark:text-rose-200 bg-rose-100/50 dark:bg-rose-900/30 p-3 rounded-lg break-all">
                      {taskDetail.error.message}
                    </p>
                    {Boolean(taskDetail.error.details) && (
                      <pre className="font-mono text-[11px] text-rose-700 dark:text-rose-300 mt-2 p-2 bg-rose-50 dark:bg-rose-950/40 rounded overflow-x-auto">
                        {JSON.stringify(taskDetail.error.details, null, 2)}
                      </pre>
                    )}
                  </div>
                ) : (
                  <div className="p-6 text-center text-slate-400 dark:text-slate-500 text-xs bg-slate-50 dark:bg-slate-800/40 rounded-xl border border-dashed border-slate-200 dark:border-slate-800">
                    <Clock className="w-6 h-6 mx-auto mb-2 opacity-50" />
                    Task is currently in-progress or queued across mesh workers.
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="p-12 text-center text-slate-400">
              Select a task from the list to view its complete telemetry.
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

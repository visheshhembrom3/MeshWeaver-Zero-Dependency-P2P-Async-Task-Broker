import React, { useState } from 'react';
import {
  CheckSquare,
  Plus,
  Search,
  Clock,
  CheckCircle2,
  AlertTriangle,
  ShieldCheck,
  X,
  Play,
} from 'lucide-react';
import type { TaskAvailable, TaskRecord, TaskSubmitPayload } from '../types';
import { StatusBadge } from '../components/StatusBadge';
import { EmptyState } from '../components/EmptyState';
import { api } from '../api';

interface TasksProps {
  tasks: TaskRecord[];
  availableTasks: TaskAvailable[];
  onRefresh: () => void;
  isRefreshing: boolean;
}

export const Tasks: React.FC<TasksProps> = ({
  tasks,
  availableTasks,
  onRefresh,
}) => {
  const [searchQuery, setSearchQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState<'all' | 'pending' | 'running' | 'success' | 'failed'>('all');
  const [selectedTask, setSelectedTask] = useState<TaskRecord | null>(null);

  // Task Submission Modal State
  const [isSubmitModalOpen, setIsSubmitModalOpen] = useState(false);
  const [selectedTaskName, setSelectedTaskName] = useState<string>('add');
  const [arg1, setArg1] = useState<string>('15');
  const [arg2, setArg2] = useState<string>('27');
  const [strArg, setStrArg] = useState<string>('MeshWeaver P2P');
  const [timeoutSec, setTimeoutSec] = useState<number>(5.0);
  const [requireAck, setRequireAck] = useState<boolean>(true);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [submissionError, setSubmissionError] = useState<string | null>(null);
  const [lastSubmittedResult, setLastSubmittedResult] = useState<Record<string, unknown> | null>(null);

  // Set default selected task name when available tasks arrive
  React.useEffect(() => {
    if (availableTasks.length > 0 && !availableTasks.some((t) => t.name === selectedTaskName)) {
      setSelectedTaskName(availableTasks[0].name);
    }
  }, [availableTasks, selectedTaskName]);

  const filteredTasks = tasks.filter((t) => {
    const matchesSearch =
      t.task_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      t.task_name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      (t.routed_to && t.routed_to.toLowerCase().includes(searchQuery.toLowerCase()));

    if (statusFilter === 'all') return matchesSearch;
    if (statusFilter === 'pending') return matchesSearch && t.status === 'pending';
    if (statusFilter === 'running') return matchesSearch && t.status === 'running';
    if (statusFilter === 'success') return matchesSearch && t.status === 'success';
    if (statusFilter === 'failed') {
      return matchesSearch && ['failure', 'error', 'timeout'].includes(t.status);
    }
    return matchesSearch;
  });

  const handleTaskSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    setSubmissionError(null);
    setLastSubmittedResult(null);

    // Prepare parsed safe JSON arguments based on task function
    let taskArgs: unknown[] = [];
    if (selectedTaskName === 'add') {
      const num1 = Number(arg1);
      const num2 = Number(arg2);
      if (isNaN(num1) || isNaN(num2)) {
        setSubmissionError('Both arguments for "add" must be valid numbers.');
        setIsSubmitting(false);
        return;
      }
      taskArgs = [num1, num2];
    } else if (selectedTaskName === 'reverse_string') {
      taskArgs = [strArg];
    } else if (selectedTaskName === 'echo') {
      taskArgs = [strArg, 42];
    } else if (selectedTaskName === 'ping') {
      taskArgs = [];
    }

    const payload: TaskSubmitPayload = {
      task_name: selectedTaskName,
      args: taskArgs,
      kwargs: {},
      timeout: timeoutSec,
      require_ack: requireAck,
    };

    try {
      const result = await api.submitTask(payload);
      setLastSubmittedResult(result);
      onRefresh();
    } catch (err) {
      setSubmissionError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="space-y-6">
      {/* Top Controls Bar */}
      <div className="bg-white p-4 border border-slate-200 rounded-xl shadow-xs flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        {/* Search */}
        <div className="relative flex-1 max-w-md">
          <Search className="w-4 h-4 text-slate-400 absolute left-3 top-1/2 -translate-y-1/2" />
          <input
            type="text"
            placeholder="Search task ID, function name, or worker..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="w-full pl-9 pr-4 py-2 text-xs border border-slate-200 rounded-lg focus:outline-hidden focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>

        {/* Filters and Submit Button */}
        <div className="flex items-center gap-2 overflow-x-auto pb-1 sm:pb-0">
          {(['all', 'pending', 'running', 'success', 'failed'] as const).map((filter) => (
            <button
              key={filter}
              onClick={() => setStatusFilter(filter)}
              className={`px-3 py-1.5 text-xs font-medium rounded-lg capitalize transition whitespace-nowrap ${
                statusFilter === filter
                  ? 'bg-blue-600 text-white shadow-xs'
                  : 'bg-slate-50 text-slate-600 hover:bg-slate-100 border border-slate-200'
              }`}
            >
              {filter}
            </button>
          ))}

          <button
            onClick={() => {
              setIsSubmitModalOpen(true);
              setSubmissionError(null);
              setLastSubmittedResult(null);
            }}
            className="inline-flex items-center gap-1.5 px-3.5 py-1.5 bg-blue-600 text-white text-xs font-semibold rounded-lg hover:bg-blue-700 transition shadow-xs whitespace-nowrap"
          >
            <Plus className="w-3.5 h-3.5" />
            <span>Submit Task</span>
          </button>
        </div>
      </div>

      {/* Main Tasks Table */}
      {filteredTasks.length === 0 ? (
        <EmptyState
          title="No Tasks Tracked"
          description={
            searchQuery || statusFilter !== 'all'
              ? 'No tasks matched your filter criteria.'
              : 'No tasks have been submitted yet. Submit an allowlisted task to see real-time routing and execution.'
          }
          icon={CheckSquare}
          action={{
            label: 'Submit First Task',
            onClick: () => setIsSubmitModalOpen(true),
          }}
        />
      ) : (
        <div className="bg-white border border-slate-200 rounded-xl shadow-xs overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-left text-xs">
              <thead className="bg-slate-50 text-slate-500 font-semibold border-b border-slate-200">
                <tr>
                  <th className="py-3 px-4">Task ID</th>
                  <th className="py-3 px-4">Function</th>
                  <th className="py-3 px-4">Worker Peer</th>
                  <th className="py-3 px-4">Status</th>
                  <th className="py-3 px-4">Execution Time</th>
                  <th className="py-3 px-4">Submitted</th>
                  <th className="py-3 px-4">Result / Error Preview</th>
                  <th className="py-3 px-4 text-right">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100 text-slate-700">
                {filteredTasks.map((t) => (
                  <tr
                    key={t.task_id}
                    onClick={() => setSelectedTask(t)}
                    className="hover:bg-slate-50/80 cursor-pointer transition"
                  >
                    <td className="py-3 px-4 font-mono font-medium text-slate-900" title={t.task_id}>
                      {t.task_id.slice(0, 12)}...
                    </td>
                    <td className="py-3 px-4 font-bold text-slate-900">
                      <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded border border-blue-200 font-mono">
                        {t.task_name}
                      </span>
                    </td>
                    <td className="py-3 px-4 font-mono text-slate-600">
                      {t.routed_to || 'Local Node'}
                    </td>
                    <td className="py-3 px-4">
                      <StatusBadge status={t.status} size="sm" />
                    </td>
                    <td className="py-3 px-4 font-mono">
                      {t.execution_time != null
                        ? `${(t.execution_time * 1000).toFixed(2)} ms`
                        : '—'}
                    </td>
                    <td className="py-3 px-4 text-slate-500">
                      {t.timestamp ? new Date(t.timestamp * 1000).toLocaleTimeString() : 'N/A'}
                    </td>
                    <td className="py-3 px-4 max-w-[200px] truncate font-mono text-slate-600">
                      {t.result !== undefined && t.result !== null ? (
                        <span className="text-emerald-700 font-medium">
                          {JSON.stringify(t.result)}
                        </span>
                      ) : t.error ? (
                        <span className="text-rose-600">
                          [{t.error.code}] {t.error.message}
                        </span>
                      ) : (
                        <span className="text-slate-400">—</span>
                      )}
                    </td>
                    <td className="py-3 px-4 text-right">
                      <button
                        onClick={(e) => {
                          e.stopPropagation();
                          setSelectedTask(t);
                        }}
                        className="px-2.5 py-1 text-xs text-blue-600 hover:text-blue-800 font-medium hover:bg-blue-50 rounded"
                      >
                        Details
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Task Submission Modal */}
      {isSubmitModalOpen && (
        <div className="fixed inset-0 z-50 bg-slate-900/40 backdrop-blur-xs flex items-center justify-center p-4">
          <div className="bg-white rounded-2xl max-w-lg w-full p-6 shadow-2xl border border-slate-200">
            <div className="flex items-center justify-between pb-3 border-b border-slate-100 mb-4">
              <div className="flex items-center gap-2">
                <div className="w-8 h-8 rounded-lg bg-blue-50 border border-blue-200 flex items-center justify-center text-blue-600">
                  <Play className="w-4 h-4 fill-current" />
                </div>
                <div>
                  <h3 className="text-base font-bold text-slate-900">Submit Distributed Task</h3>
                  <p className="text-[11px] text-slate-500">Dispatches across registered mesh workers</p>
                </div>
              </div>
              <button
                onClick={() => setIsSubmitModalOpen(false)}
                className="p-1 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100"
              >
                <X className="w-5 h-5" />
              </button>
            </div>

            {/* Security Guarantee Notice */}
            <div className="mb-4 p-3 bg-emerald-50/70 border border-emerald-200 rounded-xl flex items-start gap-2.5 text-xs text-emerald-800">
              <ShieldCheck className="w-4 h-4 text-emerald-600 shrink-0 mt-0.5" />
              <div>
                <span className="font-bold">Security Boundary Enforced: </span>
                MeshWeaver permits execution only of registered allowlist functions. Arbitrary code, eval, and pickle payloads are strictly blocked.
              </div>
            </div>

            <form onSubmit={handleTaskSubmit} className="space-y-4 text-xs">
              {/* Function Selector */}
              <div>
                <label className="block font-semibold text-slate-700 mb-1">
                  Registered Allowlist Function
                </label>
                <select
                  value={selectedTaskName}
                  onChange={(e) => setSelectedTaskName(e.target.value)}
                  className="w-full px-3 py-2 border border-slate-200 rounded-lg text-slate-800 font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                >
                  {availableTasks.map((t) => (
                    <option key={t.name} value={t.name}>
                      {t.name} — {t.description}
                    </option>
                  ))}
                </select>
              </div>

              {/* Dynamic Arguments Input */}
              {selectedTaskName === 'add' && (
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <label className="block font-medium text-slate-600 mb-1">Argument A (Number)</label>
                    <input
                      type="number"
                      value={arg1}
                      onChange={(e) => setArg1(e.target.value)}
                      required
                      className="w-full px-3 py-2 border border-slate-200 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                    />
                  </div>
                  <div>
                    <label className="block font-medium text-slate-600 mb-1">Argument B (Number)</label>
                    <input
                      type="number"
                      value={arg2}
                      onChange={(e) => setArg2(e.target.value)}
                      required
                      className="w-full px-3 py-2 border border-slate-200 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                    />
                  </div>
                </div>
              )}

              {selectedTaskName === 'reverse_string' && (
                <div>
                  <label className="block font-medium text-slate-600 mb-1">Input Text to Reverse</label>
                  <input
                    type="text"
                    value={strArg}
                    onChange={(e) => setStrArg(e.target.value)}
                    required
                    className="w-full px-3 py-2 border border-slate-200 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                  />
                </div>
              )}

              {selectedTaskName === 'echo' && (
                <div>
                  <label className="block font-medium text-slate-600 mb-1">Echo Payload String</label>
                  <input
                    type="text"
                    value={strArg}
                    onChange={(e) => setStrArg(e.target.value)}
                    required
                    className="w-full px-3 py-2 border border-slate-200 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                  />
                </div>
              )}

              {selectedTaskName === 'ping' && (
                <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg text-slate-500">
                  The <code className="text-slate-800">ping()</code> function takes no arguments and returns "pong".
                </div>
              )}

              {/* Execution Options */}
              <div className="grid grid-cols-2 gap-3 pt-2">
                <div>
                  <label className="block font-medium text-slate-600 mb-1">Timeout (seconds)</label>
                  <input
                    type="number"
                    min="1"
                    max="60"
                    step="0.5"
                    value={timeoutSec}
                    onChange={(e) => setTimeoutSec(Number(e.target.value))}
                    className="w-full px-3 py-1.5 border border-slate-200 rounded-lg font-mono text-xs focus:ring-2 focus:ring-blue-500 focus:outline-hidden"
                  />
                </div>
                <div className="flex items-center pt-5">
                  <label className="flex items-center gap-2 cursor-pointer text-slate-700">
                    <input
                      type="checkbox"
                      checked={requireAck}
                      onChange={(e) => setRequireAck(e.target.checked)}
                      className="rounded text-blue-600 focus:ring-blue-500"
                    />
                    <span>Require Worker ACK</span>
                  </label>
                </div>
              </div>

              {/* Error Alert */}
              {submissionError && (
                <div className="p-3 bg-rose-50 border border-rose-200 rounded-lg text-rose-700 flex items-start gap-2">
                  <AlertTriangle className="w-4 h-4 shrink-0 mt-0.5" />
                  <div>
                    <span className="font-bold">Submission Failed: </span>
                    {submissionError}
                  </div>
                </div>
              )}

              {/* Last Submitted Success Result */}
              {lastSubmittedResult && (
                <div className="p-3 bg-emerald-50 border border-emerald-200 rounded-lg text-emerald-800 space-y-1">
                  <div className="flex items-center gap-1.5 font-bold">
                    <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                    <span>Task Execution Completed!</span>
                  </div>
                  <div className="text-[11px] font-mono">
                    Task ID: <span className="font-semibold">{String(lastSubmittedResult.task_id)}</span>
                  </div>
                  <div className="text-[11px] font-mono">
                    Result: <span className="font-bold text-emerald-950">{JSON.stringify(lastSubmittedResult.result)}</span>
                  </div>
                  {lastSubmittedResult.execution_time != null && (
                    <div className="text-[11px]">
                      Duration: {(Number(lastSubmittedResult.execution_time) * 1000).toFixed(2)} ms
                    </div>
                  )}
                </div>
              )}

              <div className="pt-3 border-t border-slate-100 flex items-center justify-end gap-2">
                <button
                  type="button"
                  onClick={() => setIsSubmitModalOpen(false)}
                  className="px-4 py-2 border border-slate-200 rounded-lg text-slate-700 hover:bg-slate-50 font-medium"
                >
                  Close
                </button>
                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-semibold flex items-center gap-1.5 shadow-xs disabled:opacity-60"
                >
                  {isSubmitting ? (
                    <>
                      <Clock className="w-3.5 h-3.5 animate-spin" />
                      <span>Dispatching...</span>
                    </>
                  ) : (
                    <>
                      <Play className="w-3.5 h-3.5 fill-current" />
                      <span>Execute Task</span>
                    </>
                  )}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Task Detail Inspector Drawer */}
      {selectedTask && (
        <div className="fixed inset-0 z-50 overflow-hidden bg-slate-900/30 backdrop-blur-xs flex justify-end">
          <div className="w-full max-w-lg bg-white h-full shadow-2xl p-6 flex flex-col justify-between overflow-y-auto">
            <div>
              <div className="flex items-center justify-between pb-4 border-b border-slate-100 mb-6">
                <div className="flex items-center gap-2">
                  <CheckSquare className="w-5 h-5 text-blue-600" />
                  <h3 className="text-base font-bold text-slate-900">Task Details</h3>
                </div>
                <button
                  onClick={() => setSelectedTask(null)}
                  className="p-1 rounded-lg text-slate-400 hover:text-slate-600 hover:bg-slate-100"
                >
                  <X className="w-5 h-5" />
                </button>
              </div>

              {/* Status Header */}
              <div className="p-4 bg-slate-50 border border-slate-200 rounded-xl mb-6">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-bold text-slate-900 text-base font-mono">
                    {selectedTask.task_name}
                  </span>
                  <StatusBadge status={selectedTask.status} size="sm" />
                </div>
                <p className="font-mono text-xs text-slate-500 break-all">
                  ID: {selectedTask.task_id}
                </p>
                {selectedTask.correlation_id && (
                  <p className="font-mono text-[11px] text-slate-400 break-all mt-1">
                    Correlation ID: {selectedTask.correlation_id}
                  </p>
                )}
              </div>

              {/* Execution Telemetry */}
              <div className="space-y-3 text-xs">
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Assigned Worker:</span>
                  <span className="font-mono font-medium text-slate-800">
                    {selectedTask.routed_to || 'Local Node'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Execution Duration:</span>
                  <span className="font-mono font-bold text-slate-800">
                    {selectedTask.execution_time != null
                      ? `${(selectedTask.execution_time * 1000).toFixed(2)} ms`
                      : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Submission Timestamp:</span>
                  <span className="text-slate-800">
                    {selectedTask.timestamp ? new Date(selectedTask.timestamp * 1000).toLocaleString() : 'N/A'}
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Retry Budget:</span>
                  <span className="text-slate-800">
                    {selectedTask.retry_count || 0} / {selectedTask.max_retries || 2} retries
                  </span>
                </div>
                <div className="flex justify-between py-1.5 border-b border-slate-100">
                  <span className="text-slate-500">Execution Semantics:</span>
                  <span className="text-amber-700 bg-amber-50 px-2 py-0.5 rounded font-mono text-[11px] border border-amber-200">
                    At-Least-Once
                  </span>
                </div>
              </div>

              {/* Result or Error Display */}
              <div className="mt-6">
                <span className="text-xs font-semibold text-slate-700 block mb-2">
                  Execution Outcome
                </span>
                {selectedTask.result !== undefined && selectedTask.result !== null ? (
                  <div className="p-3 bg-emerald-50 border border-emerald-200 rounded-lg">
                    <span className="text-[11px] text-emerald-800 font-semibold block mb-1">
                      Computed Return Value:
                    </span>
                    <pre className="text-xs font-mono font-bold text-emerald-950 whitespace-pre-wrap break-all">
                      {JSON.stringify(selectedTask.result, null, 2)}
                    </pre>
                  </div>
                ) : selectedTask.error ? (
                  <div className="p-3 bg-rose-50 border border-rose-200 rounded-lg">
                    <span className="text-[11px] text-rose-800 font-semibold block mb-1">
                      Error [{selectedTask.error.code}]:
                    </span>
                    <p className="text-xs font-mono text-rose-900 break-all">
                      {selectedTask.error.message}
                    </p>
                  </div>
                ) : (
                  <div className="p-3 bg-slate-50 border border-slate-200 rounded-lg text-slate-400 text-xs">
                    Task is in progress or pending worker assignment.
                  </div>
                )}
              </div>
            </div>

            <div className="pt-6 border-t border-slate-100">
              <button
                onClick={() => setSelectedTask(null)}
                className="w-full py-2 bg-slate-100 text-slate-700 font-medium rounded-lg hover:bg-slate-200 text-xs transition"
              >
                Close Task Details
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

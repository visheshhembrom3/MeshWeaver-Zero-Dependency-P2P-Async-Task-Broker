import React from 'react';
import type { PeerHealthStatus, PeerLifecycleState, TaskState } from '../types';

interface StatusBadgeProps {
  status: PeerLifecycleState | PeerHealthStatus | TaskState | string;
  size?: 'sm' | 'md';
}

export const StatusBadge: React.FC<StatusBadgeProps> = ({ status, size = 'md' }) => {
  const norm = String(status).toLowerCase();

  let bg = 'bg-slate-100 text-slate-700 border-slate-200';
  let dot = 'bg-slate-400';

  // Green / Active / Healthy / Success
  if (norm === 'active' || norm === 'healthy' || norm === 'success' || norm === 'completed') {
    bg = 'bg-emerald-50 text-emerald-700 border-emerald-200';
    dot = 'bg-emerald-500';
  }
  // Amber / Stale / Degraded / Pending / Retrying
  else if (norm === 'stale' || norm === 'degraded' || norm === 'pending' || norm === 'retrying' || norm === 'assigned') {
    bg = 'bg-amber-50 text-amber-700 border-amber-200';
    dot = 'bg-amber-500';
  }
  // Blue / Running
  else if (norm === 'running') {
    bg = 'bg-blue-50 text-blue-700 border-blue-200';
    dot = 'bg-blue-500 animate-pulse';
  }
  // Red / Expired / Unresponsive / Unhealthy / Critical / Failure / Error / Timeout
  else if (
    norm === 'unresponsive' ||
    norm === 'expired' ||
    norm === 'unhealthy' ||
    norm === 'critical' ||
    norm === 'failure' ||
    norm === 'error' ||
    norm === 'timeout'
  ) {
    bg = 'bg-rose-50 text-rose-700 border-rose-200';
    dot = 'bg-rose-500';
  }

  const px = size === 'sm' ? 'px-2 py-0.5 text-xs' : 'px-2.5 py-1 text-xs';

  return (
    <span
      className={`inline-flex items-center gap-1.5 font-medium rounded-full border ${bg} ${px}`}
    >
      <span className={`w-1.5 h-1.5 rounded-full ${dot}`} />
      <span className="capitalize">{norm}</span>
    </span>
  );
};

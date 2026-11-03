import { Loader2, RefreshCw } from 'lucide-react';
import type { JobRow } from '../types';
import { displayTime, failedStates, runDuration, runStatus } from '../utils';

// The latest runs of a generated job, shared by the Jobs page and the Overview.
const activeStates = ['RUNNING', 'PENDING', 'QUEUED', 'TERMINATING', 'BLOCKED', 'WAITING_FOR_RETRY'];

/** The latest run's state: green for success, red for a failure, a spinner while running. */
export function StatusBadge({ job }: { job: JobRow }) {
  const status = runStatus(job.runs[0]);
  const style =
    status === 'SUCCESS'
      ? 'check-status-match'
      : failedStates.includes(status)
        ? 'check-status-mismatch'
        : activeStates.includes(status)
          ? 'check-status-running'
          : 'check-status-none';
  const label = status.charAt(0) + status.slice(1).toLowerCase().replaceAll('_', ' ');
  return (
    <span className={`check-status ${style}`}>
      {activeStates.includes(status) && <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}
      {label}
    </span>
  );
}

/** The last runs, oldest first, each linking to its run. */
export function RunTimeline({ job, compact = false }: { job: JobRow; compact?: boolean }) {
  return (
    <div
      className={compact ? 'run-timeline run-timeline-compact' : 'run-timeline'}
      aria-label={`Recent run status for ${job.settings?.name ?? `job ${job.job_id}`}`}
    >
      {[...job.runs].reverse().map((recentRun) => {
        const status = runStatus(recentRun);
        const label = `${status.replaceAll('_', ' ')} · ${displayTime(recentRun.start_time)} · ${runDuration(recentRun)}`;
        return (
          <a
            key={recentRun.run_id}
            className={`run-mark run-mark-${status.toLowerCase()}`}
            title={label}
            aria-label={label}
            href={recentRun.run_url}
            target="_blank"
            rel="noreferrer"
          >
            {status === 'RUNNING' ? (
              <RefreshCw className={compact ? 'h-3 w-3 animate-spin' : 'h-4 w-4 animate-spin'} aria-hidden="true" />
            ) : (
              <span>{status === 'SUCCESS' ? '✓' : '!'}</span>
            )}
          </a>
        );
      })}
    </div>
  );
}

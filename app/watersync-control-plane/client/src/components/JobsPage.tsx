import { useCallback, useEffect, useMemo, useState } from 'react';
import type { FormEvent } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Input,
  Label,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  ToggleGroup,
  ToggleGroupItem,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { Clock3, ExternalLink, LayoutGrid, List, Loader2, Play, Plus, RefreshCw } from 'lucide-react';
import { api } from '../lib/api';
import { reportError, useReportedFailure } from '../lib/logging';
import type { JobRow, Location } from '../types';
import { fqn, displayTime, runStatus, runDuration, formString } from '../utils';
import { useControl } from '../lib/control';
import { PageTitle, ErrorState, ScheduleFields } from './shared';
import { JobComputeFields, NodeTypeOptions, PerformanceTargetField, RunAsFields } from './JobFields';
import { computeFromForm, useNodeTypes } from '../lib/jobCompute';
import { FilterBar } from './FilterBar';
import { Pagination } from './Pagination';
import { usePageFilters } from '../lib/pageFilters';
import { usePagination } from '../lib/pagination';
import type { PerformanceTarget } from '../lib/jobCompute';

export function JobsPage() {
  const { location, revision, refresh } = useControl();
  const filters = usePageFilters();
  const pages = usePagination(filters.key);
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [missing, setMissing] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  // The group a "Create job" button preselects; empty for the header button.
  const [dialogGroup, setDialogGroup] = useState('');
  const [runningJob, setRunningJob] = useState<number | null>(null);
  const [scheduleJob, setScheduleJob] = useState<JobRow | null>(null);
  const [view, setView] = useJobsView();
  const groupParams = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_config')),
      refresh_token: sql.int(revision),
    }),
    [location, revision]
  );
  const {
    data: groups,
    loading: groupsLoading,
    error: groupsError,
  } = useAnalyticsQuery('ingestion_groups', groupParams);
  useReportedFailure('query.ingestion_groups', groupsError, location);
  // One page of the groups matching the filters; each group has at most one generated job.
  const pageParams = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      ...filters.params,
      ...pages.params,
      refresh_token: sql.int(revision),
    }),
    [location, filters.params, pages.params, revision]
  );
  const { data: pageGroups, loading: pageLoading, error: pageError } = useAnalyticsQuery('job_groups', pageParams);
  useReportedFailure('query.job_groups', pageError, { ...location, group: filters.group, search: filters.search });
  const total = Number(pageGroups?.[0]?.total_count ?? 0);
  const groupNames = useMemo(() => (pageGroups ?? []).map((row) => row.ingestion_group), [pageGroups]);
  const load = useCallback(() => {
    if (!pageGroups) return;
    if (!groupNames.length) {
      setJobs([]);
      setMissing([]);
      setLoading(false);
      return;
    }
    setLoading(true);
    setError(null);
    const query = new URLSearchParams({ catalog: location.catalog, schema: location.schema });
    for (const name of groupNames) query.append('group', name);
    api<{ jobs: JobRow[]; missing: string[] }>(`/api/jobs?${query}`)
      .then((x) => {
        setJobs(x.jobs);
        setMissing(x.missing);
      })
      .catch((e: unknown) => setError(reportError('jobs.list', e, { ...location, groups: groupNames.length })))
      .finally(() => setLoading(false));
  }, [location, pageGroups, groupNames]);
  // Reload when the page of groups changes, including after a refresh or an import from the header.
  useEffect(() => {
    const timer = window.setTimeout(load, 0);
    return () => window.clearTimeout(timer);
  }, [load]);
  const noJobsOnPage = 'None of the groups on this page has a job yet.';
  const missingGroups = missing.length > 0 && (
    <div className="flex flex-wrap items-center gap-2 text-sm">
      <span className="mr-1 text-muted-foreground">
        {missing.length === 1 ? 'This group has' : `These ${missing.length} groups have`} no job yet:
      </span>
      {missing.map((group) => (
        <Button key={group} variant="outline" size="sm" onClick={() => openDialog(group)}>
          <Plus className="mr-1.5 h-3.5 w-3.5" />
          {group}
        </Button>
      ))}
    </div>
  );
  const pager = (
    <Pagination
      total={total}
      page={pages.page}
      pageSize={pages.pageSize}
      onPageChange={pages.setPage}
      onPageSizeChange={pages.setPageSize}
      noun="groups"
      bordered={view === 'list'}
    />
  );
  const openDialog = (group = '') => {
    setDialogGroup(group);
    setOpen(true);
  };
  const run = async (id: number, fullRefresh = false) => {
    setRunningJob(id);
    try {
      const x = await api<{ runId: number; fullRefresh?: boolean }>(`/api/jobs/${id}/run`, {
        method: 'POST',
        body: JSON.stringify({ fullRefresh }),
      });
      alert(`Run ${x.runId} started${fullRefresh ? ' (full refresh mode)' : ''}`);
      load();
    } catch (e) {
      setError(reportError('jobs.run', e, { jobId: id, fullRefresh }));
    } finally {
      setRunningJob(null);
    }
  };
  const actions: JobActionHandlers = {
    runningJob,
    run: (id, fullRefresh) => void run(id, fullRefresh),
    editSchedule: setScheduleJob,
  };
  return (
    <>
      <PageTitle
        title="WaterSync Lakeflow Jobs"
        description="Monitor recent run health, trigger ingestion, or open the full job in Databricks."
        action={
          <div className="flex flex-wrap items-center gap-2">
            <ToggleGroup
              type="single"
              variant="outline"
              value={view}
              onValueChange={(value) => {
                // Clicking the selected item would deselect it; keep one view selected.
                if (value === 'blocks' || value === 'list') setView(value);
              }}
              aria-label="Job view"
            >
              <ToggleGroupItem value="blocks" aria-label="Blocks view" title="Blocks: one card per job">
                <LayoutGrid className="mr-2 h-4 w-4" />
                Blocks
              </ToggleGroupItem>
              <ToggleGroupItem value="list" aria-label="List view" title="List: one row per job">
                <List className="mr-2 h-4 w-4" />
                List
              </ToggleGroupItem>
            </ToggleGroup>
            <Button onClick={() => openDialog()}>
              <Plus className="mr-2 h-4 w-4" />
              Create job
            </Button>
          </div>
        }
      />
      <FilterBar location={location} revision={revision} filters={filters} onRefresh={refresh} />
      {error && <ErrorState message={error} />}
      {pageError && <ErrorState message={pageError} />}
      {pageLoading || loading ? (
        <Skeleton className="h-72" />
      ) : !groupNames.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>{filters.filtered ? 'No group matches the filters' : 'No ingestion groups'}</EmptyTitle>
            <EmptyDescription>
              {filters.filtered
                ? 'Change or clear the filters to see more jobs.'
                : 'Add source tables on the Configuration page; each group then gets one job.'}
            </EmptyDescription>
          </EmptyHeader>
          {filters.filtered && (
            <Button variant="outline" size="sm" onClick={filters.clear}>
              Clear filters
            </Button>
          )}
        </Empty>
      ) : (
        <>
          {view === 'list' ? (
            // One card: the job table, the groups without a job, and the pager.
            <Card className="gap-0 overflow-hidden py-0">
              {jobs.length ? (
                <JobTable jobs={jobs} actions={actions} />
              ) : (
                <p className="px-4 py-8 text-center text-sm text-muted-foreground">{noJobsOnPage}</p>
              )}
              {missingGroups && <div className="border-t px-4 py-3">{missingGroups}</div>}
              {pager}
            </Card>
          ) : (
            // Job cards straight on the page, then the groups without a job, then the pager in its own bar.
            <div className="space-y-4">
              {jobs.length ? (
                <div className="grid gap-4 lg:grid-cols-2">
                  {jobs.map((job) => (
                    <JobCard key={job.job_id} job={job} actions={actions} />
                  ))}
                </div>
              ) : (
                <p className="py-8 text-center text-sm text-muted-foreground">{noJobsOnPage}</p>
              )}
              {missingGroups && <div className="rounded-xl border border-dashed px-4 py-3">{missingGroups}</div>}
              <Card className="gap-0 overflow-hidden py-0">{pager}</Card>
            </div>
          )}
        </>
      )}
      <JobDialog
        key={open ? `open-${dialogGroup}` : 'closed'}
        initialGroup={dialogGroup}
        open={open}
        onOpenChange={setOpen}
        location={location}
        groups={groups ?? []}
        groupsLoading={groupsLoading}
        groupsError={groupsError}
        onSaved={() => {
          setOpen(false);
          load();
        }}
      />
      <ScheduleDialog
        job={scheduleJob}
        onClose={() => setScheduleJob(null)}
        onSaved={() => {
          setScheduleJob(null);
          load();
        }}
      />
    </>
  );
}

type JobActionHandlers = {
  runningJob: number | null;
  run: (jobId: number, fullRefresh?: boolean) => void;
  editSchedule: (job: JobRow) => void;
};

type JobsView = 'blocks' | 'list';
const jobsViewKey = 'watersync-jobs-view';

/** The selected view, remembered in this browser. Defaults to blocks. */
function useJobsView() {
  const [view, setView] = useState<JobsView>(() => {
    try {
      return window.localStorage.getItem(jobsViewKey) === 'list' ? 'list' : 'blocks';
    } catch {
      return 'blocks';
    }
  });
  const select = (next: JobsView) => {
    setView(next);
    try {
      window.localStorage.setItem(jobsViewKey, next);
    } catch {
      // Storage may be unavailable (private mode); the choice then lasts until reload.
    }
  };
  return [view, select] as const;
}

const jobName = (job: JobRow) => job.settings?.name ?? `Job ${job.job_id}`;
const fullRefreshPrompt =
  'Run a full refresh? Staging will be replaced with a fresh snapshot and CDC will reprocess from scratch.';

const failedStates = ['FAILED', 'TIMEDOUT', 'CANCELED', 'INTERNAL_ERROR'];
const activeStates = ['RUNNING', 'PENDING', 'QUEUED', 'TERMINATING', 'BLOCKED', 'WAITING_FOR_RETRY'];

/** The latest run's state: green for success, red for a failure, a spinner while running. */
function StatusBadge({ job }: { job: JobRow }) {
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

function ScheduleBadge({ job }: { job: JobRow }) {
  const schedule = job.settings?.schedule;
  return (
    <Badge variant={schedule?.pause_status === 'UNPAUSED' ? 'secondary' : 'outline'}>
      {!schedule ? 'Manual only' : schedule.pause_status === 'UNPAUSED' ? 'Active' : 'Paused'}
    </Badge>
  );
}

/** The group's scheduled basic and detailed row checks: their schedule, or that they are not set up. */
function RowCheckSchedules({ job }: { job: JobRow }) {
  const checks = (['basic', 'detailed'] as const).map((mode) => ({ mode, check: job.row_checks?.[mode] ?? null }));
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
      <span className="font-medium text-foreground">Row checks</span>
      {checks.map(({ mode, check }) => {
        const schedule = check?.schedule;
        const state = !check
          ? 'not set up'
          : !schedule
            ? 'manual only'
            : schedule.pause_status === 'PAUSED'
              ? 'paused'
              : null;
        const label = `${mode} · ${state ?? schedule?.quartz_cron_expression}`;
        return check?.workspace_url ? (
          <a
            key={mode}
            href={check.workspace_url}
            target="_blank"
            rel="noreferrer"
            className="font-mono hover:text-foreground hover:underline"
            title={schedule ? `${schedule.quartz_cron_expression} (${schedule.timezone_id})` : undefined}
          >
            {label}
          </a>
        ) : (
          <span key={mode} className="font-mono">
            {label}
          </span>
        );
      })}
    </div>
  );
}

function RunTimeline({ job, compact = false }: { job: JobRow; compact?: boolean }) {
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

const latestRun = (job: JobRow) =>
  job.runs[0] ? `${displayTime(job.runs[0].start_time)} · ${runDuration(job.runs[0])}` : 'Never run';

/** Run, full refresh, open and schedule actions. Compact renders icon-only buttons for table rows. */
function JobActions({ job, actions, compact = false }: { job: JobRow; actions: JobActionHandlers; compact?: boolean }) {
  const name = jobName(job);
  const starting = actions.runningJob === job.job_id;
  const busy = !job.job_id || starting;
  const fullRefresh = () => {
    if (job.job_id && confirm(fullRefreshPrompt)) actions.run(job.job_id, true);
  };
  if (compact) {
    return (
      <div className="flex justify-end gap-1">
        <Button
          size="icon"
          variant="ghost"
          disabled={busy}
          onClick={() => job.job_id && actions.run(job.job_id)}
          aria-label={`Run ${name} now`}
          title={starting ? 'Starting…' : 'Run now'}
        >
          {starting ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
        </Button>
        <Button
          size="icon"
          variant="ghost"
          disabled={busy}
          onClick={fullRefresh}
          aria-label={`Full refresh of ${name}`}
          title="Full refresh"
        >
          <RefreshCw className="h-4 w-4" />
        </Button>
        <Button
          size="icon"
          variant="ghost"
          disabled={!job.job_id}
          onClick={() => actions.editSchedule(job)}
          aria-label={`Edit schedule of ${name}`}
          title="Edit schedule"
        >
          <Clock3 className="h-4 w-4" />
        </Button>
        <Button size="icon" variant="ghost" asChild>
          <a
            href={job.workspace_url}
            target="_blank"
            rel="noreferrer"
            aria-label={`Open ${name} in Databricks`}
            title="Open in Databricks"
          >
            <ExternalLink className="h-4 w-4" />
          </a>
        </Button>
      </div>
    );
  }
  return (
    <div className="flex flex-wrap gap-2">
      <Button disabled={busy} onClick={() => job.job_id && actions.run(job.job_id)}>
        <Play className="mr-2 h-4 w-4" />
        {starting ? 'Starting…' : 'Run now'}
      </Button>
      <Button variant="outline" disabled={busy} onClick={fullRefresh}>
        <RefreshCw className="mr-2 h-4 w-4" />
        Full refresh
      </Button>
      <Button variant="outline" asChild>
        <a href={job.workspace_url} target="_blank" rel="noreferrer">
          Open in Databricks
          <ExternalLink className="ml-2 h-4 w-4" />
        </a>
      </Button>
      <Button variant="outline" onClick={() => actions.editSchedule(job)} disabled={!job.job_id}>
        <Clock3 className="mr-2 h-4 w-4" />
        Edit schedule
      </Button>
    </div>
  );
}

function JobCard({ job, actions }: { job: JobRow; actions: JobActionHandlers }) {
  return (
    <Card className="job-card">
      <CardHeader>
        <div className="flex justify-between gap-4">
          <div>
            <CardTitle>{jobName(job)}</CardTitle>
            <CardDescription>
              ID {job.job_id} {'·'} created {displayTime(job.created_time)}
            </CardDescription>
          </div>
          <StatusBadge job={job} />
        </div>
      </CardHeader>
      <CardContent className="space-y-5">
        <div className="rounded-md border bg-muted/30 p-3">
          <div className="mb-1 flex items-center justify-between gap-3">
            <div className="flex min-w-0 items-center gap-3">
              <span className="text-sm font-medium">Schedule</span>
              {job.settings?.schedule && (
                <span className="truncate font-mono text-xs text-foreground">
                  {job.settings.schedule.quartz_cron_expression}
                </span>
              )}
            </div>
            <ScheduleBadge job={job} />
          </div>
          {job.settings?.schedule ? (
            <div className="text-xs text-muted-foreground">{job.settings.schedule.timezone_id}</div>
          ) : (
            <p className="text-xs text-muted-foreground">Runs only when manually triggered.</p>
          )}
          <div className="mt-2">
            <RowCheckSchedules job={job} />
          </div>
        </div>
        <div>
          <div className="mb-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
            <span className="font-medium text-foreground">Last 10 runs</span>
            <span>Newest on the right</span>
          </div>
          {job.runs.length ? (
            <RunTimeline job={job} />
          ) : (
            <p className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">No runs recorded yet.</p>
          )}
          <p className="mt-2 text-xs text-muted-foreground">Latest: {latestRun(job)}</p>
        </div>
        <JobActions job={job} actions={actions} />
      </CardContent>
    </Card>
  );
}

/** One row per job, so many jobs can be compared at a glance. */
/** The jobs as table rows; the page puts the table in its card with the pager. */
function JobTable({ jobs, actions }: { jobs: JobRow[]; actions: JobActionHandlers }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Job</TableHead>
          <TableHead>Status</TableHead>
          <TableHead>Schedule</TableHead>
          <TableHead title="Newest on the right">Last 10 runs</TableHead>
          <TableHead className="hidden xl:table-cell">Latest run</TableHead>
          <TableHead className="text-right">Actions</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.map((job) => {
          const schedule = job.settings?.schedule;
          return (
            <TableRow key={job.job_id} className="whitespace-nowrap">
              <TableCell className="max-w-48 lg:max-w-64">
                <div className="truncate font-medium" title={jobName(job)}>
                  {jobName(job)}
                </div>
                <div className="text-xs text-muted-foreground">ID {job.job_id}</div>
              </TableCell>
              <TableCell>
                <StatusBadge job={job} />
              </TableCell>
              <TableCell>
                <div
                  className="flex items-center gap-2"
                  title={schedule ? `${schedule.quartz_cron_expression} (${schedule.timezone_id})` : undefined}
                >
                  <ScheduleBadge job={job} />
                  {schedule && (
                    <span className="hidden font-mono text-xs text-muted-foreground xl:inline">
                      {schedule.quartz_cron_expression}
                    </span>
                  )}
                </div>
                <div className="mt-1">
                  <RowCheckSchedules job={job} />
                </div>
              </TableCell>
              <TableCell>
                {job.runs.length ? (
                  <RunTimeline job={job} compact />
                ) : (
                  <span className="text-xs text-muted-foreground">No runs yet</span>
                )}
              </TableCell>
              <TableCell className="hidden text-xs text-muted-foreground xl:table-cell">{latestRun(job)}</TableCell>
              <TableCell>
                <JobActions job={job} actions={actions} compact />
              </TableCell>
            </TableRow>
          );
        })}
      </TableBody>
    </Table>
  );
}

function JobDialog({
  open,
  onOpenChange,
  location,
  initialGroup,
  groups,
  groupsLoading,
  groupsError,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  location: Location;
  /** Preselected group; the first group when empty. */
  initialGroup: string;
  groups: Array<{
    ingestion_group: string;
    source_count: number;
    enabled_source_count: number;
    enabled_incremental_source_count: number;
  }>;
  groupsLoading: boolean;
  groupsError: string | null;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [ingestionGroup, setIngestionGroup] = useState(initialGroup);
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduleActive, setScheduleActive] = useState(true);
  // The group's scheduled row checks: a daily basic check is on by default, the detailed check is opt-in.
  const [basicCheckEnabled, setBasicCheckEnabled] = useState(true);
  const [basicCheckActive, setBasicCheckActive] = useState(true);
  const [detailedCheckEnabled, setDetailedCheckEnabled] = useState(false);
  const [detailedCheckActive, setDetailedCheckActive] = useState(true);
  const [gitUrl, setGitUrl] = useState('https://github.com/databricks-industry-solutions/watersync');
  const [gitBranch, setGitBranch] = useState('main');
  const [computeMode, setComputeMode] = useState<'SERVERLESS' | 'JOB_CLUSTER'>('SERVERLESS');
  const [performanceTarget, setPerformanceTarget] = useState<PerformanceTarget>('STANDARD');
  const [tab, setTab] = useState('job');
  const [pipelineComputeMode, setPipelineComputeMode] = useState<'SERVERLESS' | 'CLASSIC'>('SERVERLESS');
  const [pipelinePhoton, setPipelinePhoton] = useState(false);
  const [jobRunAsMode, setJobRunAsMode] = useState('DEFAULT');
  const [pipelineRunAsMode, setPipelineRunAsMode] = useState('SAME_AS_JOB');
  const nodeTypes = useNodeTypes(open && (computeMode === 'JOB_CLUSTER' || pipelineComputeMode === 'CLASSIC'));
  // Inactive tabs are hidden, so the browser cannot focus an invalid field there and blocks the submit
  // silently. Reveal the tab holding the first invalid field and say what is missing.
  const revealInvalidField = (event: FormEvent<HTMLFormElement>) => {
    const field = event.target as HTMLInputElement;
    // The invalid event fires once per invalid field; report only the first one.
    if (event.currentTarget.querySelector(':invalid') !== field) return;
    const fieldTab = field.closest<HTMLElement>('[data-tab]')?.dataset.tab;
    if (fieldTab) setTab(fieldTab);
    const label = field.labels?.[0]?.textContent ?? field.getAttribute('aria-label') ?? field.name;
    setError(`Check "${label}": ${field.validationMessage}`);
  };
  useEffect(() => {
    if (open && !ingestionGroup && groups[0]) setIngestionGroup(groups[0].ingestion_group);
  }, [open, ingestionGroup, groups]);
  const selectedGroup = groups.find((group) => group.ingestion_group === ingestionGroup);
  const hasCdcPipeline = Boolean(selectedGroup?.enabled_incremental_source_count);
  // Leave the SDP Compute tab when it disappears after switching to a group without an SDP pipeline.
  useEffect(() => {
    if (tab === 'pipeline' && !hasCdcPipeline) setTab('compute');
  }, [tab, hasCdcPipeline]);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const form = new FormData(e.currentTarget);
      const result = await api<{
        jobId: number;
        action: 'created' | 'updated';
        rowCheckJobs?: Partial<Record<'basic' | 'detailed', { jobId?: number; action: string }>>;
      }>('/api/jobs', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          ingestionGroup,
          gitUrl: form.get('gitUrl'),
          gitBranch: form.get('gitBranch'),
          foreachConcurrency: Number(form.get('foreachConcurrency')),
          cdcPipelineId: selectedGroup?.enabled_incremental_source_count ? form.get('cdcPipelineId') : null,
          schedule: {
            enabled: scheduleEnabled,
            quartzCronExpression: formString(form, 'quartzCronExpression', '0 0 8 * * ?'),
            timezoneId: formString(form, 'timezoneId', 'America/New_York'),
            pauseStatus: scheduleActive ? 'UNPAUSED' : 'PAUSED',
          },
          compute: computeFromForm(form, computeMode, performanceTarget),
          pipelineCompute:
            pipelineComputeMode === 'CLASSIC'
              ? {
                  mode: pipelineComputeMode,
                  nodeTypeId: formString(form, 'pipelineNodeTypeId', ''),
                  driverNodeTypeId: formString(form, 'pipelineDriverNodeTypeId', ''),
                  minWorkers: Number(form.get('pipelineMinWorkers') ?? 1),
                  maxWorkers: Number(form.get('pipelineMaxWorkers') ?? 4),
                  photon: pipelinePhoton,
                }
              : { mode: pipelineComputeMode },
          runAs: {
            job: { mode: jobRunAsMode, principal: formString(form, 'jobRunAsPrincipal', '') },
            pipeline: { mode: pipelineRunAsMode, principal: formString(form, 'pipelineRunAsPrincipal', '') },
          },
          rowChecks: {
            maxParallelTables: Number(form.get('rowCheckParallel') ?? 4),
            basic: {
              enabled: basicCheckEnabled,
              quartzCronExpression: formString(form, 'basicQuartzCronExpression', '0 0 6 * * ?'),
              timezoneId: formString(form, 'basicTimezoneId', 'UTC'),
              pauseStatus: basicCheckActive ? 'UNPAUSED' : 'PAUSED',
            },
            detailed: {
              enabled: detailedCheckEnabled,
              quartzCronExpression: formString(form, 'detailedQuartzCronExpression', '0 0 7 ? * SUN'),
              timezoneId: formString(form, 'detailedTimezoneId', 'UTC'),
              pauseStatus: detailedCheckActive ? 'UNPAUSED' : 'PAUSED',
              rangeFrom: formString(form, 'detailedRangeFrom', ''),
            },
          },
        }),
      });
      const checks = Object.entries(result.rowCheckJobs ?? {})
        .map(([mode, check]) => `${mode} row check job ${check.jobId} ${check.action}`)
        .join(', ');
      alert(`Job ${result.jobId} ${result.action}${checks ? `; ${checks}` : ''}`);
      onSaved();
    } catch (x) {
      setError(reportError('jobs.save', x, { ...location, ingestionGroup, computeMode }));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="w-[calc(100vw-2rem)] sm:max-w-5xl">
        <DialogHeader>
          <DialogTitle>Create or update a WaterSync job</DialogTitle>
          <DialogDescription>
            Select a configured ingestion group. WaterSync generates the parameters and task graph automatically.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} onInvalidCapture={revealInvalidField} className="space-y-4">
          {error && <ErrorState message={error} />}
          {groupsError && <ErrorState message={groupsError} />}
          <NodeTypeOptions nodeTypes={nodeTypes} />
          <Tabs value={tab} onValueChange={setTab} className="w-full">
            <TabsList className="flex h-auto w-full [&>button]:flex-auto [&>button]:px-3">
              <TabsTrigger value="job">Job configuration</TabsTrigger>
              <TabsTrigger value="source">Git source &amp; execution</TabsTrigger>
              <TabsTrigger value="compute">Job Compute</TabsTrigger>
              {hasCdcPipeline && <TabsTrigger value="pipeline">SDP Compute</TabsTrigger>}
              <TabsTrigger value="schedule">Schedule</TabsTrigger>
              <TabsTrigger value="row-checks">Row checks</TabsTrigger>
              <TabsTrigger value="run-as">Run as</TabsTrigger>
            </TabsList>
            <TabsContent value="job" data-tab="job" forceMount className="mt-4 space-y-4 data-[state=inactive]:hidden">
              <div className="space-y-2">
                <Label htmlFor="ingestion-group">Ingestion group</Label>
                {groupsLoading ? (
                  <Skeleton className="h-10" />
                ) : groups.length ? (
                  <Select value={ingestionGroup} onValueChange={setIngestionGroup}>
                    <SelectTrigger id="ingestion-group" className="w-full">
                      <SelectValue placeholder="Select an ingestion group" />
                    </SelectTrigger>
                    <SelectContent>
                      {groups.map((group) => (
                        <SelectItem key={group.ingestion_group} value={group.ingestion_group}>
                          {group.ingestion_group} {'\u00b7'} {group.enabled_source_count} enabled sources
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : (
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No ingestion groups found</EmptyTitle>
                      <EmptyDescription>Add a row to the configuration table before creating a job.</EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                )}
              </div>
              {selectedGroup && (
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Generated job</CardTitle>
                    <CardDescription>
                      [{selectedGroup.ingestion_group}] Ingestion Pipeline {'\u00b7'} {selectedGroup.source_count}{' '}
                      configured sources
                    </CardDescription>
                  </CardHeader>
                  <CardContent className="grid gap-4 text-sm text-muted-foreground md:grid-cols-2">
                    <div className="min-w-0 space-y-1">
                      <div>configuration_fqn</div>
                      <div className="break-all font-mono text-xs text-foreground">
                        {fqn(location, 'jdbc_ingestion_config')}
                      </div>
                    </div>
                    <div className="min-w-0 space-y-1">
                      <div>watermark_fqn</div>
                      <div className="break-all font-mono text-xs text-foreground">
                        {fqn(location, 'jdbc_ingestion_watermark')}
                      </div>
                    </div>
                    <div className="min-w-0 space-y-1">
                      <div>ingestion_group</div>
                      <div className="break-all font-mono text-xs text-foreground">{selectedGroup.ingestion_group}</div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>
            <TabsContent value="source" data-tab="source" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2 md:col-span-2">
                  <Label htmlFor="git-url">GitHub repository URL</Label>
                  <Input
                    id="git-url"
                    name="gitUrl"
                    type="url"
                    value={gitUrl}
                    onChange={(event) => setGitUrl(event.target.value)}
                    placeholder="https://github.com/organization/repository"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="git-branch">Branch</Label>
                  <Input
                    id="git-branch"
                    name="gitBranch"
                    value={gitBranch}
                    onChange={(event) => setGitBranch(event.target.value)}
                    placeholder="main"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="concurrency">Parallel tables</Label>
                  <Input
                    id="concurrency"
                    name="foreachConcurrency"
                    type="number"
                    min="1"
                    max="100"
                    defaultValue="4"
                    required
                  />
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label htmlFor="pipeline-id">Existing SDP pipeline ID (optional)</Label>
                  <Input
                    id="pipeline-id"
                    name="cdcPipelineId"
                    placeholder={
                      selectedGroup?.enabled_incremental_source_count
                        ? 'Leave blank to create or reuse the group SDP pipeline'
                        : 'Not required: this group has no enabled incremental tables'
                    }
                    disabled={!selectedGroup?.enabled_incremental_source_count}
                  />
                  {!selectedGroup?.enabled_incremental_source_count && (
                    <p className="text-xs text-muted-foreground">
                      The SDP pipeline task will be omitted for this ingestion group.
                    </p>
                  )}
                  {Boolean(selectedGroup?.enabled_incremental_source_count) && (
                    <p className="text-xs text-muted-foreground">
                      When blank, WaterSync automatically provisions a serverless CDC SCD2 pipeline for this group.
                    </p>
                  )}
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label>Task library</Label>
                  <div className="break-all rounded-md border bg-muted/30 p-3 font-mono text-xs">
                    watersync@git+{gitUrl.replace(/\.git$/, '')}.git@{gitBranch || 'main'}
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Installed on the planner and every ingestion worker task from the selected branch.
                  </p>
                </div>
              </div>
            </TabsContent>
            <TabsContent value="schedule" data-tab="schedule" forceMount className="mt-4 data-[state=inactive]:hidden">
              <ScheduleFields
                enabled={scheduleEnabled}
                onEnabledChange={setScheduleEnabled}
                active={scheduleActive}
                onActiveChange={setScheduleActive}
              />
            </TabsContent>
            <TabsContent
              value="row-checks"
              data-tab="row-checks"
              forceMount
              className="mt-4 data-[state=inactive]:hidden"
            >
              <div className="space-y-5">
                <p className="text-sm text-muted-foreground">
                  Each check is its own job next to this one,{' '}
                  <span className="font-mono">[{ingestionGroup || 'group'}] Row Check (basic)</span> and{' '}
                  <span className="font-mono">(detailed)</span>, with this job&apos;s Git source, compute and identity.
                  Both check every enabled table of the group and write to the Row checks page. Turning a check off
                  keeps its job without a schedule.
                </p>
                <ScheduleFields
                  prefix="basic"
                  title="Scheduled basic check"
                  description="Compares the row counts of source and target. Cheap: one COUNT(*) per side and table."
                  enabled={basicCheckEnabled}
                  onEnabledChange={setBasicCheckEnabled}
                  active={basicCheckActive}
                  onActiveChange={setBasicCheckActive}
                  schedule={{ quartz_cron_expression: '0 0 6 * * ?', timezone_id: 'UTC' }}
                />
                <ScheduleFields
                  prefix="detailed"
                  title="Scheduled detailed check"
                  description="Compares per-chunk aggregates, then the rows of the chunks that differ. Reads more; schedule it less often."
                  enabled={detailedCheckEnabled}
                  onEnabledChange={setDetailedCheckEnabled}
                  active={detailedCheckActive}
                  onActiveChange={setDetailedCheckActive}
                  schedule={{ quartz_cron_expression: '0 0 7 ? * SUN', timezone_id: 'UTC' }}
                />
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="detailed-range-from">Detailed check: rows from (optional)</Label>
                    <Input
                      id="detailed-range-from"
                      name="detailedRangeFrom"
                      placeholder="-30d"
                      disabled={!detailedCheckEnabled}
                      pattern="^$|^-?\d+(\.\d+)?$|^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^-\d{1,5}d$"
                      title="A number, a date (2026-09-01), a timestamp (2026-09-01 12:00:00) or days back (-30d)"
                    />
                    <p className="text-xs text-muted-foreground">
                      Checks only rows from this value of each table&apos;s chunk column, for example the last 30 days
                      (-30d). Empty checks every row.
                    </p>
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="row-check-parallel">Tables checked in parallel</Label>
                    <Input
                      id="row-check-parallel"
                      name="rowCheckParallel"
                      type="number"
                      min={1}
                      max={32}
                      defaultValue={4}
                      required
                    />
                  </div>
                </div>
              </div>
            </TabsContent>
            <TabsContent value="run-as" data-tab="run-as" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <RunAsFields
                  id="job-run-as"
                  label="Job runs as"
                  mode={jobRunAsMode}
                  onModeChange={setJobRunAsMode}
                  principalName="jobRunAsPrincipal"
                />
                {hasCdcPipeline ? (
                  <RunAsFields
                    id="pipeline-run-as"
                    label="SDP runs as"
                    mode={pipelineRunAsMode}
                    onModeChange={setPipelineRunAsMode}
                    principalName="pipelineRunAsPrincipal"
                    allowSameAsJob
                  />
                ) : (
                  <p className="self-end text-xs text-muted-foreground">
                    This group has no SDP pipeline, so only the job identity applies.
                  </p>
                )}
                <p className="text-xs text-muted-foreground md:col-span-2">
                  The owner is the app service principal that creates the job and SDP pipeline. Running as another user
                  requires workspace admin rights for the app; running as another service principal requires the app to
                  have the Service Principal User role on it. The SDP setting is ignored when an existing SDP pipeline
                  ID is provided.
                </p>
              </div>
            </TabsContent>
            <TabsContent value="compute" data-tab="compute" forceMount className="mt-4 data-[state=inactive]:hidden">
              <JobComputeFields
                mode={computeMode}
                onModeChange={setComputeMode}
                performanceTarget={performanceTarget}
                onPerformanceTargetChange={setPerformanceTarget}
                performanceSharedWith={
                  hasCdcPipeline && pipelineComputeMode === 'SERVERLESS' ? 'the SDP pipeline task' : null
                }
                nodeTypes={nodeTypes}
                clusterNote="The planner and ingestion workers share this autoscaling job cluster. The SDP pipeline task uses its own pipeline compute."
              />
            </TabsContent>
            {hasCdcPipeline && (
              <TabsContent
                value="pipeline"
                data-tab="pipeline"
                forceMount
                className="mt-4 data-[state=inactive]:hidden"
              >
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="pipeline-compute-mode">SDP compute</Label>
                    <Select
                      value={pipelineComputeMode}
                      onValueChange={(value) => setPipelineComputeMode(value as typeof pipelineComputeMode)}
                    >
                      <SelectTrigger id="pipeline-compute-mode" className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="SERVERLESS">Serverless</SelectItem>
                        <SelectItem value="CLASSIC">Classic autoscaling cluster</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      Applies to the SDP pipeline WaterSync creates or updates for this group. It is ignored when an
                      existing SDP pipeline ID is provided.
                    </p>
                  </div>
                  {/* Databricks has one serverless performance mode per job, covering the pipeline task too. It is
                      edited where it is set: Job Compute when the job is serverless, otherwise here. */}
                  {pipelineComputeMode === 'SERVERLESS' && computeMode === 'SERVERLESS' && (
                    <div className="space-y-2">
                      <Label>Serverless performance mode</Label>
                      <div className="flex flex-wrap items-center justify-between gap-3 rounded-md border bg-muted/30 px-3 py-2 text-sm">
                        <span>
                          <span className="font-medium">
                            {performanceTarget === 'PERFORMANCE_OPTIMIZED' ? 'Performance optimized' : 'Standard'}
                          </span>
                          <span className="text-muted-foreground"> · set on the Job Compute tab</span>
                        </span>
                        <Button type="button" variant="outline" size="sm" onClick={() => setTab('compute')}>
                          Change in Job Compute
                        </Button>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        Databricks has one serverless performance mode per job, and it covers the SDP pipeline task too,
                        so the job and its pipeline always run with the same mode.
                      </p>
                    </div>
                  )}
                  {pipelineComputeMode === 'SERVERLESS' && computeMode !== 'SERVERLESS' && (
                    <PerformanceTargetField
                      id="pipeline-performance-target"
                      value={performanceTarget}
                      onChange={setPerformanceTarget}
                      sharedWith={null}
                    />
                  )}
                  {pipelineComputeMode === 'CLASSIC' && (
                    <div className="grid gap-4 md:grid-cols-2">
                      <div className="space-y-2">
                        <Label htmlFor="pipeline-node-type">Pipeline worker node type</Label>
                        <Input
                          key={`pipeline-worker-${nodeTypes?.defaultNodeTypeId ?? ''}`}
                          id="pipeline-node-type"
                          name="pipelineNodeTypeId"
                          list="node-type-options"
                          defaultValue={nodeTypes?.defaultNodeTypeId ?? ''}
                          placeholder={nodeTypes ? 'For example: i3.xlarge' : 'Loading node types\u2026'}
                          required
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="pipeline-driver-node-type">Pipeline driver node type</Label>
                        <Input
                          key={`pipeline-driver-${nodeTypes?.defaultNodeTypeId ?? ''}`}
                          id="pipeline-driver-node-type"
                          name="pipelineDriverNodeTypeId"
                          list="node-type-options"
                          defaultValue={nodeTypes?.defaultNodeTypeId ?? ''}
                          placeholder={nodeTypes ? 'For example: i3.xlarge' : 'Loading node types\u2026'}
                          required
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="pipeline-min-workers">Pipeline minimum workers</Label>
                        <Input
                          id="pipeline-min-workers"
                          name="pipelineMinWorkers"
                          type="number"
                          min="0"
                          defaultValue="1"
                          required
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="pipeline-max-workers">Pipeline maximum workers</Label>
                        <Input
                          id="pipeline-max-workers"
                          name="pipelineMaxWorkers"
                          type="number"
                          min="1"
                          defaultValue="4"
                          required
                        />
                      </div>
                      <div className="flex items-center gap-2 md:col-span-2">
                        <Switch
                          id="pipeline-photon"
                          checked={pipelinePhoton}
                          onCheckedChange={setPipelinePhoton}
                          aria-label="Use Photon for the SDP pipeline"
                        />
                        <Label htmlFor="pipeline-photon">Use Photon</Label>
                      </div>
                      <p className="text-xs text-muted-foreground md:col-span-2">
                        Classic pipelines run on the Advanced edition and install WaterSync from the Git source with
                        %pip when the pipeline starts.
                      </p>
                    </div>
                  )}
                </div>
              </TabsContent>
            )}
          </Tabs>
          <p className="text-xs text-muted-foreground">
            If a job with the generated name already exists, its definition is updated in place.
          </p>
          <DialogFooter>
            <Button type="submit" disabled={busy || !ingestionGroup || groupsLoading}>
              {busy ? 'Saving\u2026' : 'Create or update job'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

// Literal class names so Tailwind keeps them.

function ScheduleDialog({ job, onClose, onSaved }: { job: JobRow | null; onClose: () => void; onSaved: () => void }) {
  const schedule = job?.settings?.schedule;
  const [enabled, setEnabled] = useState(false);
  const [active, setActive] = useState(true);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setEnabled(Boolean(schedule));
    setActive(schedule?.pause_status !== 'PAUSED');
    setError(null);
  }, [job, schedule]);
  const submit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!job?.job_id) return;
    const form = new FormData(event.currentTarget);
    setBusy(true);
    setError(null);
    try {
      await api(`/api/jobs/${job.job_id}/schedule`, {
        method: 'PATCH',
        body: JSON.stringify({
          enabled,
          quartzCronExpression: formString(form, 'quartzCronExpression', '0 0 8 * * ?'),
          timezoneId: formString(form, 'timezoneId', 'America/New_York'),
          pauseStatus: active ? 'UNPAUSED' : 'PAUSED',
        }),
      });
      onSaved();
    } catch (value) {
      setError(reportError('jobs.schedule', value, { jobId: job.job_id, scheduleEnabled: enabled }));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={Boolean(job)} onOpenChange={(value) => !value && onClose()}>
      <DialogContent className="sm:max-w-2xl">
        <DialogHeader>
          <DialogTitle>Edit job schedule</DialogTitle>
          <DialogDescription>{job?.settings?.name ?? `Job ${job?.job_id}`}</DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <ScheduleFields
            key={`${job?.job_id}-${schedule?.quartz_cron_expression ?? 'manual'}`}
            enabled={enabled}
            onEnabledChange={setEnabled}
            active={active}
            onActiveChange={setActive}
            schedule={schedule}
          />
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving\u2026' : enabled ? 'Save schedule' : 'Remove schedule'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router';
import { sql } from '@databricks/appkit-ui/js';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import type { QueryRegistry } from '@databricks/appkit-ui/react';
import {
  CheckCircle2,
  ChevronRight,
  Clock3,
  Droplets,
  ListChecks,
  ScanSearch,
  Settings2,
  TableProperties,
  TriangleAlert,
  Workflow,
  XCircle,
} from 'lucide-react';
import { api } from '../lib/api';
import { reportError, useReportedFailure } from '../lib/logging';
import { fqn, displayTime, failedStates, relativeTime, runStatus } from '../utils';
import { useControl } from '../lib/control';
import { usePagination } from '../lib/pagination';
import { usePageFilters } from '../lib/pageFilters';
import type { JobRow, Location, RowCheckJob } from '../types';
import { PageTitle, ErrorState, Metric, Hint } from './shared';
import { FilterBar } from './FilterBar';
import { Pagination } from './Pagination';
import { RunTimeline, StatusBadge } from './JobRuns';

type Summary = QueryRegistry['config_summary']['result'][number];
type AttentionRow = QueryRegistry['overview_attention']['result'][number];
type GroupRow = QueryRegistry['overview_groups']['result'][number];
type Pages = ReturnType<typeof usePagination>;

// How long since its last run an enabled table counts as stale, remembered in this browser.
const staleOptions = [
  { hours: 6, label: '6 hours' },
  { hours: 24, label: '24 hours' },
  { hours: 72, label: '3 days' },
  { hours: 168, label: '7 days' },
];
const staleHoursKey = 'watersync-stale-hours';
function useStaleHours() {
  const [hours, setHours] = useState(() => {
    const stored = Number(window.localStorage.getItem(staleHoursKey));
    return staleOptions.some((option) => option.hours === stored) ? stored : 24;
  });
  const select = (next: number) => {
    setHours(next);
    try {
      window.localStorage.setItem(staleHoursKey, String(next));
    } catch {
      // Storage may be unavailable (private mode); the choice then lasts until reload.
    }
  };
  return [hours, select] as const;
}
const staleLabel = (hours: number) => staleOptions.find((option) => option.hours === hours)?.label ?? `${hours} hours`;
const count = (value: number | string | null | undefined) => Number(value ?? 0).toLocaleString();
const plural = (value: number, noun: string) => `${value.toLocaleString()} ${noun}${value === 1 ? '' : 's'}`;

type GroupJobs = {
  jobs: JobRow[];
  /** Groups of the page without an ingestion job. */
  missing: string[];
  rowCheckJobs: RowCheckJob[];
  loading: boolean;
  error: string | null;
};

/**
 * The ingestion jobs of the listed groups, and the row check jobs of the location and of those groups. Jobs are
 * found by name, so this follows the page of groups and not every group of the location.
 */
function useGroupJobs(location: Location, groups: string[] | null, revision: number): GroupJobs {
  // The result of the latest request, tagged with it, so a result for other groups or an older revision reads as
  // loading.
  const [result, setResult] = useState<(Omit<GroupJobs, 'loading'> & { request: string }) | null>(null);
  const key = groups?.join('\u0000') ?? null;
  const request = `${location.catalog}.${location.schema}|${revision}|${key}`;
  useEffect(() => {
    if (key === null) return;
    let cancelled = false;
    const query = new URLSearchParams(location);
    for (const group of key ? key.split('\u0000') : []) query.append('group', group);
    Promise.all([
      key
        ? api<{ jobs: JobRow[]; missing: string[] }>(`/api/jobs?${query.toString()}`)
        : Promise.resolve({ jobs: [], missing: [] }),
      api<{ jobs: RowCheckJob[] }>(`/api/row-checks/jobs?${query.toString()}`),
    ])
      .then(([ingestion, rowChecks]) => {
        if (!cancelled) setResult({ ...ingestion, rowCheckJobs: rowChecks.jobs, error: null, request });
      })
      .catch((x: unknown) => {
        if (!cancelled) {
          const error = reportError('overview.jobs', x, { ...location, groups: key.split('\u0000').length });
          setResult({ jobs: [], missing: [], rowCheckJobs: [], error, request });
        }
      });
    return () => {
      cancelled = true;
    };
  }, [location, key, request]);
  const current = result?.request === request ? result : null;
  return {
    jobs: current?.jobs ?? [],
    missing: current?.missing ?? [],
    rowCheckJobs: current?.rowCheckJobs ?? [],
    error: current?.error ?? null,
    loading: !current,
  };
}

export function OverviewPage() {
  const { location, revision, refresh } = useControl();
  const filters = usePageFilters();
  const [staleHours, setStaleHours] = useStaleHours();
  const groupPages = usePagination(filters.key);
  const tables = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      watermark_table: sql.string(fqn(location, 'jdbc_ingestion_watermark')),
      row_check_table: sql.string(fqn(location, '_watersync_row_checks')),
    }),
    [location]
  );
  const params = useMemo(
    () => ({
      config_table: tables.config_table,
      watermark_table: tables.watermark_table,
      ...filters.params,
      stale_hours: sql.int(staleHours),
      refresh_token: sql.int(revision),
    }),
    [tables, filters.params, staleHours, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('config_summary', params);
  useReportedFailure('query.config_summary', error, location);
  const attention = useAnalyticsQuery('overview_attention', params);
  useReportedFailure('query.overview_attention', attention.error, location);
  const groupParams = useMemo(
    () => ({ ...params, row_check_table: tables.row_check_table, ...groupPages.params }),
    [params, tables, groupPages.params]
  );
  const groups = useAnalyticsQuery('overview_groups', groupParams);
  useReportedFailure('query.overview_groups', groups.error, location);
  const groupNames = useMemo(() => (groups.data ? groups.data.map((row) => row.ingestion_group) : null), [groups.data]);
  const jobs = useGroupJobs(location, groupNames, revision);
  const row = data?.[0];

  return (
    <>
      <PageTitle
        title="Pipeline health at a glance"
        description={`Actual state from ${location.catalog}.${location.schema}; refreshes on demand.`}
      />
      <FilterBar location={location} revision={revision} filters={filters} onRefresh={refresh} />
      {loading ? (
        <div className="grid gap-4 md:grid-cols-4">
          {[1, 2, 3, 4].map((i) => (
            <Skeleton key={i} className="h-32" />
          ))}
        </div>
      ) : error ? (
        <ErrorState message={error} />
      ) : !row ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No metadata found</EmptyTitle>
            <EmptyDescription>Choose a catalog and schema containing WaterSync tables.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <Hero location={location} summary={row} filtered={filters.filtered} staleHours={staleHours} />
          <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
            <h2 className="text-lg font-semibold">Pipeline health</h2>
            <label className="flex items-center gap-2 text-xs text-muted-foreground">
              Stale after
              <Select value={String(staleHours)} onValueChange={(value) => setStaleHours(Number(value))}>
                <SelectTrigger className="h-8 w-32" aria-label="Stale after">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {staleOptions.map((option) => (
                    <SelectItem key={option.hours} value={String(option.hours)}>
                      {option.label}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </label>
          </div>
          <div className="mb-7 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <Metric
              icon={TableProperties}
              tone="info"
              title="Sources"
              value={count(row.config_count)}
              note={`${count(row.enabled_count)} enabled · ${plural(Number(row.group_count), 'group')}${
                filters.filtered ? ' · matching the filters' : ''
              }`}
            />
            <Metric
              icon={TriangleAlert}
              title="Failed loads"
              value={count(row.failed_count)}
              note="enabled tables whose latest run failed"
              destructive={Number(row.failed_count) > 0}
            />
            <Metric
              icon={Clock3}
              title={`Not loaded in ${staleLabel(staleHours)}`}
              value={count(Number(row.stale_count) + Number(row.never_loaded_count))}
              note={`${count(row.never_loaded_count)} never loaded · ${count(row.stale_count)} stale`}
            />
            <Metric
              icon={CheckCircle2}
              tone="success"
              title="Latest load"
              value={relativeTime(row.last_run_timestamp)}
              note={row.last_run_timestamp ? displayTime(row.last_run_timestamp) : 'No table has run yet'}
            />
          </div>
        </>
      )}
      <div className="mb-7 grid gap-4 xl:grid-cols-2">
        <AttentionCard
          rows={attention.data}
          loading={attention.loading}
          error={attention.error}
          staleHours={staleHours}
          jobs={jobs}
        />
        <ParityCard location={location} revision={revision} filters={filters} rowCheckJobs={jobs.rowCheckJobs} />
      </div>
      <GroupsCard
        rows={groups.data}
        loading={groups.loading}
        error={groups.error}
        jobs={jobs}
        pages={groupPages}
        staleHours={staleHours}
      />
      <div className="mt-7">
        <SetupGapsCard
          summary={row}
          rows={groups.data}
          jobs={jobs}
          paged={Number(groups.data?.[0]?.total_count ?? 0) > (groups.data?.length ?? 0)}
        />
      </div>
      <Card className="mt-8 overflow-hidden">
        <CardHeader>
          <CardTitle>WaterSync workflow</CardTitle>
          <CardDescription>
            Configure ingestion sources, inspect or reset incremental state, create and run framework jobs, then prove
            the targets match their sources.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-0 p-0 md:grid-cols-2 xl:grid-cols-4">
          <Hint
            number="01"
            icon={Settings2}
            title="Configuration"
            text="Maintain JDBC source mappings and enablement."
          />
          <Hint number="02" icon={Droplets} title="Watermarks" text="Correct state or force an incremental reload." />
          <Hint number="03" icon={Workflow} title="Jobs" text="Create, monitor, and trigger Lakeflow Jobs." />
          <Hint
            number="04"
            icon={ScanSearch}
            title="Row checks"
            text="Compare sources and targets, on a schedule or on demand."
          />
        </CardContent>
      </Card>
    </>
  );
}

/** The location, and in one line whether the latest loads are healthy. */
function Hero({
  location,
  summary,
  filtered,
  staleHours,
}: {
  location: Location;
  summary: Summary;
  filtered: boolean;
  staleHours: number;
}) {
  const failed = Number(summary.failed_count);
  const late = Number(summary.stale_count) + Number(summary.never_loaded_count);
  const health = failed
    ? {
        tone: 'hero-health-danger',
        Icon: XCircle,
        title: `${plural(failed, 'failed load')}`,
        text: 'The latest run of these tables failed',
      }
    : late
      ? {
          tone: 'hero-health-warning',
          Icon: Clock3,
          title: `${plural(late, 'table')} not loaded recently`,
          text: `No successful run in ${staleLabel(staleHours)}, or never loaded`,
        }
      : {
          tone: '',
          Icon: CheckCircle2,
          title: 'Loads healthy',
          text: `Every enabled table ran in the last ${staleLabel(staleHours)}`,
        };
  const breakdown = [
    [summary.success_count, 'succeeded'],
    [summary.skipped_count, 'skipped'],
    [summary.full_refresh_count, 'full refresh pending'],
  ] as const;
  return (
    <Card className="overview-hero mb-7 overflow-hidden">
      <CardContent className="flex flex-wrap items-center justify-between gap-5 p-6">
        <div>
          <div className="eyebrow">Active metadata environment</div>
          <h2 className="mt-1 text-xl font-semibold">{location.catalog}</h2>
          <div className="mt-3 flex flex-wrap gap-2">
            <Badge variant="secondary">Schema · {location.schema}</Badge>
            <Badge variant="outline">SQL warehouse live</Badge>
            <Badge variant="outline">
              {count(summary.enabled_count)} sources enabled
              {filtered && ` · ${plural(Number(summary.group_count), 'filtered group')}`}
            </Badge>
            {breakdown.map(([value, label]) =>
              Number(value) ? (
                <Badge key={label} variant="outline" className="font-normal text-muted-foreground">
                  Latest run {label} · {count(value)}
                </Badge>
              ) : null
            )}
          </div>
        </div>
        <div className={`hero-health ${health.tone}`}>
          <health.Icon className="h-5 w-5" />
          <div className="mode-field space-y-1.5">
            <div className="font-semibold">{health.title}</div>
            <div className="text-xs opacity-80">{health.text}</div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

const reasonBadges = {
  failed: { label: 'Failed', className: 'check-status-mismatch' },
  never_loaded: { label: 'Never loaded', className: 'check-status-error' },
  stale: { label: 'Stale', className: 'check-status-error' },
} as const;

function CardLink({ to, children }: { to: string; children: React.ReactNode }) {
  return (
    <Button asChild variant="ghost" size="sm">
      <Link to={to}>
        {children} <ChevronRight className="ml-1 h-3.5 w-3.5" />
      </Link>
    </Button>
  );
}

/** Tables whose latest load failed, never happened or is old, and ingestion jobs whose last run failed. */
function AttentionCard({
  rows,
  loading,
  error,
  staleHours,
  jobs,
}: {
  rows: AttentionRow[] | null | undefined;
  loading: boolean;
  error: string | null;
  staleHours: number;
  jobs: GroupJobs;
}) {
  const failingJobs = jobs.jobs.filter((job) => failedStates.includes(runStatus(job.runs[0])));
  const total = Number(rows?.[0]?.total_count ?? 0);
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="flex flex-row items-start justify-between gap-3 py-4">
        <div>
          <CardTitle className="text-base">Needs attention</CardTitle>
          <CardDescription>
            Enabled tables whose latest run failed, that never ran, or that did not run in {staleLabel(staleHours)}.
          </CardDescription>
        </div>
        <CardLink to="/watermarks">Watermarks</CardLink>
      </CardHeader>
      {failingJobs.length > 0 && (
        <div className="px-6 pb-4">
          <Alert variant="destructive">
            <TriangleAlert className="h-4 w-4" />
            <AlertTitle>{plural(failingJobs.length, 'ingestion job')} failed their last run</AlertTitle>
            <AlertDescription>
              <span>
                {failingJobs.map((job, index) => (
                  <span key={job.job_id}>
                    {index > 0 && ', '}
                    <a
                      href={job.runs[0]?.run_url ?? job.workspace_url}
                      target="_blank"
                      rel="noreferrer"
                      className="underline"
                    >
                      {job.ingestion_group}
                    </a>
                  </span>
                ))}
                {' · '}
                <Link to="/jobs" className="underline">
                  Open Jobs
                </Link>
              </span>
            </AlertDescription>
          </Alert>
        </div>
      )}
      {loading ? (
        <Skeleton className="mx-6 mb-6 h-40" />
      ) : error ? (
        <div className="px-6 pb-6">
          <ErrorState message={error} />
        </div>
      ) : !rows?.length ? (
        <Empty className="border-t">
          <EmptyHeader>
            <EmptyTitle>Nothing to look at</EmptyTitle>
            <EmptyDescription>
              Every enabled table ran successfully in the last {staleLabel(staleHours)}.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <Table className="border-t">
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Table</TableHead>
                <TableHead>Why</TableHead>
                <TableHead>Latest run</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((item) => {
                const badge = reasonBadges[item.reason];
                return (
                  <TableRow key={`${item.ingestion_group}/${item.source_table_name}`}>
                    <TableCell className="max-w-72">
                      <div className="flex items-center gap-2">
                        <span className="truncate font-mono text-sm" title={item.source_table_name}>
                          {item.source_table_name}
                        </span>
                        <Badge variant="outline" className="shrink-0 font-normal text-muted-foreground">
                          {item.ingestion_group}
                        </Badge>
                      </div>
                      {item.last_error && (
                        <div className="line-clamp-2 text-xs text-destructive" title={item.last_error}>
                          {item.last_error}
                        </div>
                      )}
                    </TableCell>
                    <TableCell>
                      <span className={`check-status ${badge.className}`}>{badge.label}</span>
                    </TableCell>
                    <TableCell
                      className="text-xs text-muted-foreground"
                      title={item.last_run_timestamp ? displayTime(item.last_run_timestamp) : undefined}
                    >
                      {relativeTime(item.last_run_timestamp)}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {total > rows.length && (
            <div className="border-t px-6 py-3 text-xs text-muted-foreground">
              Showing the first {rows.length} of {total.toLocaleString()} tables; the Watermarks page lists them all.
            </div>
          )}
        </>
      )}
    </Card>
  );
}

/** The latest basic row check of the filtered tables, and the tables furthest out of sync. */
function ParityCard({
  location,
  revision,
  filters,
  rowCheckJobs,
}: {
  location: Location;
  revision: number;
  filters: ReturnType<typeof usePageFilters>;
  rowCheckJobs: RowCheckJob[];
}) {
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      row_check_table: sql.string(fqn(location, '_watersync_row_checks')),
      ...filters.params,
      refresh_token: sql.int(revision),
    }),
    [location, filters.params, revision]
  );
  const summary = useAnalyticsQuery('row_check_summary', params);
  useReportedFailure('query.row_check_summary', summary.error, location);
  const worstParams = useMemo(
    () => ({ ...params, status: sql.string('attention'), page_size: sql.int(5), page_offset: sql.int(0) }),
    [params]
  );
  const worst = useAnalyticsQuery('row_check_results', worstParams);
  const row = summary.data?.[0];
  const tables = Number(row?.table_count ?? 0);
  const match = Number(row?.match_count ?? 0);
  const mismatch = Number(row?.mismatch_count ?? 0);
  const errors = Number(row?.error_count ?? 0);
  const scheduled = rowCheckJobs.filter((job) => job.schedule.enabled && job.schedule.pauseStatus === 'UNPAUSED');
  const items = [
    { label: 'match', value: match, tone: 'text-success', Icon: CheckCircle2 },
    { label: 'differ', value: mismatch, tone: 'text-destructive', Icon: XCircle },
    { label: 'errors', value: errors, tone: 'text-warning-foreground', Icon: TriangleAlert },
    { label: 'not checked', value: Math.max(0, tables - match - mismatch - errors), tone: '', Icon: ListChecks },
  ];
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="flex flex-row items-start justify-between gap-3 py-4">
        <div>
          <CardTitle className="text-base">Source parity</CardTitle>
          <CardDescription>
            Latest basic row check of {plural(tables, 'enabled table')} · last check{' '}
            {relativeTime(row?.last_checked_at)}
            {' · '}
            {scheduled.length ? `${plural(scheduled.length, 'scheduled row check job')}` : 'no scheduled row check job'}
          </CardDescription>
        </div>
        <CardLink to="/row-checks">Row checks</CardLink>
      </CardHeader>
      {summary.loading ? (
        <Skeleton className="mx-6 mb-6 h-40" />
      ) : summary.error ? (
        <div className="px-6 pb-6">
          <ErrorState message={summary.error} />
        </div>
      ) : (
        <>
          <div className="flex flex-wrap gap-x-6 gap-y-2 border-t px-6 py-3 text-sm">
            {items.map(({ label, value, tone, Icon }) => (
              <span key={label} className={`flex items-center gap-1.5 ${value ? tone : 'text-muted-foreground'}`}>
                <Icon className="h-4 w-4" aria-hidden />
                <span className="font-semibold tabular-nums">{value.toLocaleString()}</span> {label}
              </span>
            ))}
          </div>
          {worst.data?.length ? (
            <Table className="border-t">
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead>Out of sync</TableHead>
                  <TableHead className="text-right">Difference</TableHead>
                  <TableHead>Result</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {worst.data.map((item) => {
                  const status = item.basic_status === 'MATCH' ? item.detailed_status : item.basic_status;
                  const diff = item.basic_count_diff == null ? null : Number(item.basic_count_diff);
                  return (
                    <TableRow key={`${item.ingestion_group}/${item.source_table_name}`}>
                      <TableCell className="max-w-72">
                        <div className="flex items-center gap-2">
                          <span className="truncate font-mono text-sm" title={item.source_table_name}>
                            {item.source_table_name}
                          </span>
                          <Badge variant="outline" className="shrink-0 font-normal text-muted-foreground">
                            {item.ingestion_group}
                          </Badge>
                        </div>
                      </TableCell>
                      <TableCell
                        className={`text-right tabular-nums ${diff ? 'font-semibold text-destructive' : 'text-muted-foreground'}`}
                      >
                        {diff == null ? '—' : `${diff > 0 ? '+' : ''}${diff.toLocaleString()}`}
                      </TableCell>
                      <TableCell>
                        <span
                          className={`check-status ${status === 'ERROR' ? 'check-status-error' : 'check-status-mismatch'}`}
                        >
                          {status === 'ERROR' ? 'Error' : 'Mismatch'}
                        </span>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          ) : (
            <p className="border-t px-6 py-4 text-sm text-muted-foreground">
              {tables && match === tables
                ? 'Every checked table matches its source.'
                : 'No table differs from its source in the latest checks.'}
            </p>
          )}
        </>
      )}
    </Card>
  );
}

/** How the row checks of a group run: a location job covers every group, else the group's own jobs. */
function rowCheckCoverage(group: string, rowCheckJobs: RowCheckJob[]) {
  const location = rowCheckJobs.filter((job) => !job.ingestionGroup).map((job) => job.checkMode);
  const own = rowCheckJobs.filter((job) => job.ingestionGroup === group).map((job) => job.checkMode);
  const modes = [...new Set([...location, ...own])].sort();
  return { covered: modes.length > 0, label: modes.length ? modes.join(' + ') : 'Not covered' };
}

/** One row per group: its tables, latest loads, ingestion job and row checks. */
function GroupsCard({
  rows,
  loading,
  error,
  jobs,
  pages,
  staleHours,
}: {
  rows: GroupRow[] | null | undefined;
  loading: boolean;
  error: string | null;
  jobs: GroupJobs;
  pages: Pages;
  staleHours: number;
}) {
  const total = Number(rows?.[0]?.total_count ?? 0);
  const jobOf = (group: string) => jobs.jobs.find((job) => job.ingestion_group === group);
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="flex flex-row items-start justify-between gap-3 py-4">
        <div>
          <CardTitle className="text-base">Groups</CardTitle>
          <CardDescription>
            Latest loads, the ingestion job and the row checks of each group; problems first. Stale means no run in{' '}
            {staleLabel(staleHours)}.
          </CardDescription>
        </div>
        <CardLink to="/jobs">Jobs</CardLink>
      </CardHeader>
      {jobs.error && (
        <div className="px-6 pb-4">
          <ErrorState message={jobs.error} />
        </div>
      )}
      {loading ? (
        <Skeleton className="mx-6 mb-6 h-48" />
      ) : error ? (
        <div className="px-6 pb-6">
          <ErrorState message={error} />
        </div>
      ) : !rows?.length ? (
        <Empty className="border-t">
          <EmptyHeader>
            <EmptyTitle>No groups</EmptyTitle>
            <EmptyDescription>No ingestion group has a table matching the filters.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <>
          <Table className="border-t">
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead>Group</TableHead>
                <TableHead className="text-right">Tables</TableHead>
                <TableHead>Loads</TableHead>
                <TableHead>Oldest load</TableHead>
                <TableHead>Ingestion job</TableHead>
                <TableHead>Row checks</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {rows.map((group) => {
                const job = jobOf(group.ingestion_group);
                const coverage = rowCheckCoverage(group.ingestion_group, jobs.rowCheckJobs);
                const loadIssues = [
                  [group.failed_count, 'failed', 'check-status-mismatch'],
                  [group.stale_count, 'stale', 'check-status-error'],
                  [group.never_loaded_count, 'never loaded', 'check-status-error'],
                ].filter(([value]) => Number(value) > 0) as Array<[number, string, string]>;
                const checkIssues = Number(group.mismatch_count) + Number(group.check_error_count);
                return (
                  <TableRow key={group.ingestion_group}>
                    <TableCell className="font-medium">{group.ingestion_group}</TableCell>
                    <TableCell className="text-right tabular-nums" title="Enabled / configured">
                      {count(group.enabled_count)}
                      <span className="text-muted-foreground"> / {count(group.table_count)}</span>
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-wrap gap-1">
                        {loadIssues.length ? (
                          loadIssues.map(([value, label, style]) => (
                            <span key={label} className={`check-status ${style}`}>
                              {count(value)} {label}
                            </span>
                          ))
                        ) : Number(group.enabled_count) ? (
                          <span className="check-status check-status-match">OK</span>
                        ) : (
                          <span className="text-xs text-muted-foreground">No enabled table</span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell
                      className="text-xs text-muted-foreground"
                      title={group.oldest_load ? displayTime(group.oldest_load) : undefined}
                    >
                      {group.oldest_load ? relativeTime(group.oldest_load) : '—'}
                    </TableCell>
                    <TableCell>
                      {jobs.loading ? (
                        <Skeleton className="h-6 w-40" />
                      ) : job ? (
                        <div className="flex flex-wrap items-center gap-2">
                          <StatusBadge job={job} />
                          <RunTimeline job={job} compact />
                          <span className="text-xs text-muted-foreground">
                            {!job.settings?.schedule
                              ? 'manual'
                              : job.settings.schedule.pause_status === 'PAUSED'
                                ? 'paused'
                                : 'scheduled'}
                          </span>
                        </div>
                      ) : (
                        <Badge variant="outline" className="font-normal text-muted-foreground">
                          No job
                        </Badge>
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-col items-start gap-1 text-xs">
                        <span className={coverage.covered ? '' : 'text-muted-foreground'}>
                          {jobs.loading ? '…' : coverage.label}
                        </span>
                        <span
                          className={checkIssues ? 'text-destructive' : 'text-muted-foreground'}
                          title={group.last_checked_at ? `Last check ${displayTime(group.last_checked_at)}` : undefined}
                        >
                          {checkIssues
                            ? `${count(checkIssues)} out of sync or failed`
                            : Number(group.match_count)
                              ? `${count(group.match_count)} match`
                              : 'not checked'}
                          {Number(group.unchecked_count) > 0 &&
                            Number(group.match_count) + checkIssues > 0 &&
                            ` · ${count(group.unchecked_count)} not checked`}
                        </span>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          <Pagination
            total={total}
            page={pages.page}
            pageSize={pages.pageSize}
            onPageChange={pages.setPage}
            onPageSizeChange={pages.setPageSize}
            noun="groups"
          />
        </>
      )}
    </Card>
  );
}

/** What is not set up yet: groups without a job or row checks, tables never loaded, groups with nothing enabled. */
function SetupGapsCard({
  summary,
  rows,
  jobs,
  paged,
}: {
  summary: Summary | undefined;
  rows: GroupRow[] | null | undefined;
  jobs: GroupJobs;
  paged: boolean;
}) {
  const uncovered = jobs.loading
    ? []
    : (rows ?? [])
        .filter((group) => Number(group.enabled_count) > 0)
        .map((group) => group.ingestion_group)
        .filter((group) => !rowCheckCoverage(group, jobs.rowCheckJobs).covered);
  const nothingEnabled = (rows ?? []).filter((group) => !Number(group.enabled_count)).map((g) => g.ingestion_group);
  const neverLoaded = Number(summary?.never_loaded_count ?? 0);
  const names = (groups: string[]) =>
    groups.slice(0, 6).join(', ') + (groups.length > 6 ? ` and ${groups.length - 6} more` : '');
  const gaps = [
    {
      show: !jobs.loading && jobs.missing.length > 0,
      title: `${plural(jobs.missing.length, 'group')} without an ingestion job`,
      text: names(jobs.missing),
      to: '/jobs',
      action: 'Create on Jobs',
    },
    {
      show: uncovered.length > 0,
      title: `${plural(uncovered.length, 'group')} without a row check job`,
      text: `${names(uncovered)}. A row check job for the whole location covers every group.`,
      to: '/row-checks',
      action: 'Create on Row checks',
    },
    {
      show: neverLoaded > 0,
      title: `${plural(neverLoaded, 'enabled table')} never loaded`,
      text: 'No run has written a watermark for them yet: run their group job, or check that it has one.',
      to: '/watermarks',
      action: 'Watermarks',
    },
    {
      show: nothingEnabled.length > 0,
      title: `${plural(nothingEnabled.length, 'group')} with no enabled table`,
      text: names(nothingEnabled),
      to: '/config',
      action: 'Configuration',
    },
  ].filter((gap) => gap.show);
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <CardHeader className="py-4">
        <CardTitle className="text-base">Setup gaps</CardTitle>
        <CardDescription>What is not set up yet{paged ? ', for the groups listed above' : ''}.</CardDescription>
      </CardHeader>
      {jobs.loading && !gaps.length ? (
        <Skeleton className="mx-6 mb-6 h-24" />
      ) : !gaps.length ? (
        <p className="flex items-center gap-2 border-t px-6 py-4 text-sm text-muted-foreground">
          <CheckCircle2 className="h-4 w-4 text-success" aria-hidden />
          Every group has an ingestion job and row checks, and every enabled table has loaded.
        </p>
      ) : (
        <ul className="divide-y border-t">
          {gaps.map((gap) => (
            <li key={gap.title} className="flex flex-wrap items-center justify-between gap-3 px-6 py-3">
              <div className="min-w-0">
                <div className="text-sm font-medium">{gap.title}</div>
                <div className="text-xs text-muted-foreground">{gap.text}</div>
              </div>
              <CardLink to={gap.to}>{gap.action}</CardLink>
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

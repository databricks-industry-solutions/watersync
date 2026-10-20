import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { FormEvent } from 'react';
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
  Table,
  TableBody,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import {
  CheckCircle2,
  ChevronRight,
  ExternalLink,
  Loader2,
  PanelRightOpen,
  Play,
  Plus,
  ScanSearch,
  Settings2,
  Trash2,
  TriangleAlert,
  XCircle,
} from 'lucide-react';
import type { QueryRegistry } from '@databricks/appkit-ui/react';
import { api } from '../lib/api';
import { jsonColumn } from '../lib/jsonColumn';
import { reportError, useReportedFailure } from '../lib/logging';
import type { JobSchedule, Location, RowCheckJob, RowCheckMode, RowCheckRun, Schedule } from '../types';
import { displayTime, formString, fqn, relativeTime } from '../utils';
import { useControl } from '../lib/control';
import { PageTitle, ErrorState, ScheduleFields, Toggle } from './shared';
import { RowCheckDailyChart } from './RowCheckDailyChart';
import { JobComputeFields, NodeTypeOptions, RunAsFields } from './JobFields';
import { computeFromForm, useNodeTypes } from '../lib/jobCompute';
import type { ComputeMode, PerformanceTarget } from '../lib/jobCompute';
import { FilterBar } from './FilterBar';
import { Pagination } from './Pagination';
import { RowCheckStatusMap } from './RowCheckStatusMap';
import { useDailyRowCounts } from '../lib/dailyRowCounts';
import type { DailyLevel } from '../lib/dailyRowCounts';
import { MAX_PICKED, freeSlot, tableKey, usePageFilters } from '../lib/pageFilters';
import type { PickedTable } from '../lib/pageFilters';
import { usePagination } from '../lib/pagination';

type RowCheckRow = QueryRegistry['row_check_results']['result'][number];

const activeStates = ['PENDING', 'QUEUED', 'RUNNING', 'BLOCKED', 'TERMINATING', 'WAITING_FOR_RETRY'];
const isActive = (run: RowCheckRun) => activeStates.includes(run.lifeCycleState ?? '');
// Empty run parameters mean "every group" or "every table".
const runCovers = (run: RowCheckRun, row: RowCheckRow) =>
  (!run.ingestionGroup || run.ingestionGroup === row.ingestion_group) &&
  (!run.sourceTableName || run.sourceTableName === row.source_table_name);
const count = (value: number | string | null | undefined) => (value == null ? '—' : Number(value).toLocaleString());
const signed = (value: number | string | null | undefined) =>
  value == null ? '—' : `${Number(value) > 0 ? '+' : ''}${Number(value).toLocaleString()}`;
const pollIntervalMs = 15_000;
type Pages = ReturnType<typeof usePagination>;
type StatusFilter = '' | 'attention' | 'match' | 'unchecked';

const statusStyles = {
  MATCH: { className: 'check-status-match', label: 'Match', Icon: CheckCircle2 },
  MISMATCH: { className: 'check-status-mismatch', label: 'Mismatch', Icon: XCircle },
  // ERROR means the table could not be checked, not that the data differs.
  ERROR: { className: 'check-status-error', label: 'Error', Icon: TriangleAlert },
} as const;

function CheckStatus({ status, running = false }: { status: string | null; running?: boolean }) {
  if (running) {
    return (
      <span className="check-status check-status-running">
        <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />
        Running
      </span>
    );
  }
  const style = status ? statusStyles[status as keyof typeof statusStyles] : undefined;
  if (!style) return <span className="check-status check-status-none">Not checked</span>;
  return (
    <span className={`check-status ${style.className}`}>
      <style.Icon className="h-3.5 w-3.5" aria-hidden />
      {style.label}
    </span>
  );
}

function CheckedAt({ value }: { value: string | null }) {
  return (
    <span className="text-xs text-muted-foreground" title={value ? displayTime(value) : undefined}>
      {relativeTime(value)}
    </span>
  );
}

/** One line on what the detailed check found; silent when there is nothing to add. */
function DetailedFindings({ row }: { row: RowCheckRow }) {
  if (!row.detailed_status || row.detailed_status === 'ERROR') return null;
  const chunks = Number(row.detailed_chunk_count ?? 0);
  const differing = Number(row.detailed_mismatched_chunks ?? 0);
  if (!differing) {
    return (
      <span className="text-xs text-muted-foreground">
        {chunks === 1 ? 'All rows match' : `${count(chunks)} chunks match`}
      </span>
    );
  }
  const findings = [
    Number(row.detailed_missing_in_target) && `${count(row.detailed_missing_in_target)} missing`,
    Number(row.detailed_extra_in_target) && `${count(row.detailed_extra_in_target)} extra`,
    Number(row.detailed_mismatched_rows) && `${count(row.detailed_mismatched_rows)} changed`,
  ].filter(Boolean);
  return (
    <span className="text-xs">
      <span className="font-medium text-destructive">
        {findings.length ? findings.join(' · ') : 'Aggregates differ'}
      </span>
      <span className="text-muted-foreground">
        {' '}
        in {count(differing)} of {count(chunks)} chunks
      </span>
    </span>
  );
}

function CheckError({ message }: { message: string | null }) {
  if (!message) return null;
  return (
    <span className="block max-w-96 truncate text-xs text-muted-foreground" title={message}>
      {message}
    </span>
  );
}

export function RowChecksPage() {
  const { location, revision, refresh } = useControl();
  const filters = usePageFilters();
  const { group, search, picked, filtered } = filters;
  const [status, setStatus] = useState<StatusFilter>('');
  // The group and table lists each page on their own; both start again when a filter or the status changes.
  const tablePages = usePagination(`${filters.key}|${status}`);
  const groupPages = usePagination(status);
  // Nothing selected: a summary per group. A group or a name search: its tables. Picked tables: only those.
  const level: DailyLevel = filtered ? 'table' : 'group';
  const [jobs, setJobs] = useState<RowCheckJob[]>([]);
  const [jobLoading, setJobLoading] = useState(true);
  const [jobError, setJobError] = useState<string | null>(null);
  // The job being edited, 'new' for a new one, or null when the dialog is closed.
  const [editing, setEditing] = useState<RowCheckJob | 'new' | null>(null);
  const [details, setDetails] = useState<RowCheckRow | null>(null);
  const [detailedFor, setDetailedFor] = useState<RowCheckRow | null>(null);
  const [starting, setStarting] = useState<string | null>(null);
  const [runError, setRunError] = useState<string | null>(null);

  const filterParams = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      row_check_table: sql.string(fqn(location, '_watersync_row_checks')),
      ...filters.params,
      refresh_token: sql.int(revision),
    }),
    [location, filters.params, revision]
  );
  const params = useMemo(
    () => ({ ...filterParams, status: sql.string(status), ...tablePages.params }),
    [filterParams, status, tablePages.params]
  );
  const { data, loading, error } = useAnalyticsQuery('row_check_results', params, { autoStart: level === 'table' });
  useReportedFailure('query.row_check_results', error, { ...location, group, search, status });
  const { data: summaryRows, loading: summaryLoading } = useAnalyticsQuery('row_check_summary', filterParams);
  const groupParams = useMemo(
    () => ({
      config_table: filterParams.config_table,
      row_check_table: filterParams.row_check_table,
      status: sql.string(status),
      ...groupPages.params,
      refresh_token: filterParams.refresh_token,
    }),
    [filterParams, status, groupPages.params]
  );
  const {
    data: groupRows,
    loading: groupsLoading,
    error: groupsError,
  } = useAnalyticsQuery('row_check_groups', groupParams, { autoStart: level === 'group' });
  useReportedFailure('query.row_check_groups', groupsError, { ...location, status });
  const chart = useDailyRowCounts(location, revision, filters, level);
  const totalCount = Number(data?.[0]?.total_count ?? 0);
  const groupCount = Number(groupRows?.[0]?.total_count ?? 0);

  const loadJob = useCallback(async () => {
    try {
      const query = new URLSearchParams(location).toString();
      const result = await api<{ jobs: RowCheckJob[] }>(`/api/row-checks/jobs?${query}`);
      setJobs(result.jobs);
      setJobError(null);
    } catch (x) {
      setJobError(reportError('row_checks.jobs_read', x, location));
    } finally {
      setJobLoading(false);
    }
  }, [location]);
  useEffect(() => {
    setJobLoading(true);
    void loadJob();
  }, [loadJob, revision]);

  // While a check is running, poll the jobs and reload the results once the run ends.
  const runs = useMemo(
    () => jobs.flatMap((job) => job.runs).sort((a, b) => (b.startTime ?? 0) - (a.startTime ?? 0)),
    [jobs]
  );
  const activeRuns = useMemo(() => runs.filter(isActive), [runs]);
  // A check runs on any row check job of the location, or of the checked group.
  const canRun = (ingestionGroup: string) =>
    jobs.some((job) => !job.ingestionGroup || (ingestionGroup && job.ingestionGroup === ingestionGroup));
  // "Run basic check" covers the group the page is filtered to, else every table.
  const runGroup = group && !picked.length && !search ? group : '';
  const previousActive = useRef(0);
  useEffect(() => {
    if (previousActive.current > activeRuns.length) refresh();
    previousActive.current = activeRuns.length;
    if (!activeRuns.length) return;
    const timer = window.setTimeout(() => void loadJob(), pollIntervalMs);
    return () => window.clearTimeout(timer);
  }, [activeRuns, loadJob, refresh]);

  const start = async (checkMode: RowCheckMode, row?: RowCheckRow, range: RowCheckRange = noRange) => {
    const key = row ? `${row.ingestion_group}/${row.source_table_name}` : checkMode;
    const ingestionGroup = row?.ingestion_group ?? runGroup;
    setStarting(key);
    setRunError(null);
    try {
      await api('/api/row-checks/run', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          checkMode,
          ingestionGroup,
          sourceTableName: row?.source_table_name ?? '',
          ...range,
        }),
      });
      await loadJob();
    } catch (x) {
      setRunError(
        reportError('row_checks.run', x, {
          ...location,
          checkMode,
          ingestionGroup,
          sourceTableName: row?.source_table_name,
        })
      );
    } finally {
      setStarting(null);
    }
  };

  const summary = useMemo(() => {
    const row = summaryRows?.[0];
    return {
      tables: Number(row?.table_count ?? 0),
      groups: Number(row?.group_count ?? 0),
      match: Number(row?.match_count ?? 0),
      mismatch: Number(row?.mismatch_count ?? 0),
      error: Number(row?.error_count ?? 0),
      lastCheck: row?.last_checked_at ?? null,
    };
  }, [summaryRows]);

  // Keep the color the table has in the chart when that slot is free, else take the lowest free slot, so
  // picking a table does not repaint it and tables already picked keep their colors.
  const slotFor = (key: string, current: PickedTable[]) => {
    const shown = chart.series.findIndex((item) => item.key === key);
    const onScreen = shown >= 0 && shown < MAX_PICKED && !current.length ? shown : -1;
    return onScreen >= 0 && !current.some((item) => item.slot === onScreen) ? onScreen : freeSlot(current);
  };
  const toggleTable = (tableGroup: string, table: string) =>
    filters.toggle(tableGroup, table, slotFor(tableKey(tableGroup, table), picked));
  // A status map square: show only that table, with any status, and scroll to its row. Clicking the only
  // picked table again shows every table.
  const focusTable = (tableGroup: string, table: string) => {
    const only = picked.length === 1 && tableKey(picked[0].group, picked[0].table) === tableKey(tableGroup, table);
    setStatus('');
    filters.pickOnly(tableGroup, table, slotFor(tableKey(tableGroup, table), []));
    if (!only) {
      // After the page re-renders with the table list.
      window.requestAnimationFrame(() =>
        document.getElementById('row-check-list')?.scrollIntoView({ behavior: 'smooth', block: 'start' })
      );
    }
  };

  return (
    <>
      <PageTitle
        title="Source and target row checks"
        description="Compare each enabled source table with its target. Basic checks compare row counts; detailed checks compare per-chunk sums and hashes, then the rows of the chunks that differ. Row check jobs run them on a schedule, for the whole location or one group, and any check can be started here."
        action={
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => setEditing('new')} disabled={jobLoading}>
              <Plus className="mr-2 h-4 w-4" />
              New row check job
            </Button>
            <Button
              onClick={() => void start('basic')}
              disabled={
                !canRun(runGroup) ||
                starting !== null ||
                activeRuns.some(
                  (run) =>
                    run.checkMode === 'basic' &&
                    !run.sourceTableName &&
                    (!run.ingestionGroup || run.ingestionGroup === runGroup)
                )
              }
              title={
                canRun(runGroup)
                  ? `Count the rows of every enabled table${runGroup ? ` of ${runGroup}` : ''} now`
                  : `Create a row check job for this location${runGroup ? ` or for ${runGroup}` : ''} first`
              }
            >
              <Play className="mr-2 h-4 w-4" />
              {starting === 'basic' ? 'Starting…' : 'Run basic check'}
            </Button>
          </div>
        }
      />
      {jobError && <ErrorState message={jobError} />}
      {runError && <ErrorState message={runError} />}
      <RowCheckJobsCard
        jobs={jobs}
        loading={jobLoading}
        location={location}
        onEdit={setEditing}
        onDeleted={() => void loadJob()}
      />
      <FilterBar
        location={location}
        revision={revision}
        filters={filters}
        onToggle={toggleTable}
        colors={chart.colors}
        enabledOnly
        pickNote=" (one chart color each)"
        onRefresh={refresh}
      />
      <SummaryStrip summary={summary} jobs={jobs} loading={summaryLoading} filtered={filtered} />
      <RowCheckStatusMap
        location={location}
        revision={revision}
        group={group}
        search={search}
        picked={picked}
        onSelect={focusTable}
      />
      <RowCheckDailyChart chart={chart} picked={picked.length} filtered={filtered} />
      {jobs.length > 0 && <RunStrip runs={runs} activeRuns={activeRuns} />}
      <div id="row-check-list" className="mb-3 flex scroll-mt-4 flex-wrap items-center gap-3">
        {level === 'group' ? (
          <h2 className="text-base font-semibold">Groups</h2>
        ) : group && !picked.length && !search ? (
          <h2 className="flex items-center gap-1.5 text-base font-semibold">
            <button
              type="button"
              className="font-normal text-muted-foreground hover:text-foreground hover:underline"
              onClick={() => filters.setGroup('')}
            >
              All groups
            </button>
            <ChevronRight className="h-4 w-4 text-muted-foreground" aria-hidden />
            {group}
          </h2>
        ) : (
          <h2 className="text-base font-semibold">Tables</h2>
        )}
        <Select
          value={status || 'all'}
          onValueChange={(value) => setStatus(value === 'all' ? '' : (value as StatusFilter))}
        >
          <SelectTrigger className="h-8 w-48" aria-label="Status">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All statuses</SelectItem>
            <SelectItem value="attention">Needs attention</SelectItem>
            <SelectItem value="match">Match</SelectItem>
            <SelectItem value="unchecked">Not checked</SelectItem>
          </SelectContent>
        </Select>
        <span className="text-xs text-muted-foreground">
          Problems first{level === 'group' && ' · open a group to see its tables'}
        </span>
      </div>
      {level === 'group' ? (
        <GroupsTable
          rows={groupRows}
          loading={groupsLoading}
          error={groupsError}
          total={groupCount}
          filteredByStatus={Boolean(status)}
          onOpen={filters.setGroup}
          onClearStatus={() => setStatus('')}
          pages={groupPages}
        />
      ) : loading ? (
        <Skeleton className="h-80" />
      ) : error ? (
        <ErrorState message={error} />
      ) : !data?.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>{filtered || status ? 'No table matches the filters' : 'No enabled tables'}</EmptyTitle>
            <EmptyDescription>
              {filtered || status
                ? 'Change or clear the filters to see more tables.'
                : 'Row checks run on the enabled rows of the configuration table.'}
            </EmptyDescription>
          </EmptyHeader>
          {(filtered || status) && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                filters.clear();
                setStatus('');
              }}
            >
              Clear filters
            </Button>
          )}
        </Empty>
      ) : (
        <Card className="gap-0 overflow-hidden py-0">
          <Table>
            <TableHeader>
              <TableRow className="hover:bg-transparent">
                <TableHead />
                <TableHead colSpan={4} className="check-group-head check-group-divider">
                  Basic check · row counts
                </TableHead>
                <TableHead className="check-group-head check-group-divider">Detailed check</TableHead>
                <TableHead />
              </TableRow>
              <TableRow className="hover:bg-transparent">
                <TableHead>Table</TableHead>
                <TableHead className="check-group-divider text-right">Source</TableHead>
                <TableHead className="text-right">Target</TableHead>
                <TableHead className="text-right">Difference</TableHead>
                <TableHead>Result</TableHead>
                <TableHead className="check-group-divider">Result</TableHead>
                <TableHead>
                  <span className="sr-only">Actions</span>
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => {
                const key = `${row.ingestion_group}/${row.source_table_name}`;
                const basicRunning = activeRuns.some((run) => run.checkMode === 'basic' && runCovers(run, row));
                const detailedRunning = activeRuns.some((run) => run.checkMode === 'detailed' && runCovers(run, row));
                const difference = Number(row.basic_count_diff ?? 0);
                return (
                  <TableRow key={key}>
                    <TableCell className="max-w-80">
                      <div className="flex items-center gap-2">
                        <span className="truncate font-mono text-sm font-medium" title={row.source_table_name}>
                          {row.source_table_name}
                        </span>
                        <Badge variant="outline" className="shrink-0 font-normal text-muted-foreground">
                          {row.ingestion_group}
                        </Badge>
                      </div>
                      <div className="truncate font-mono text-xs text-muted-foreground" title={row.target_table_fqn}>
                        → {row.target_table_fqn}
                      </div>
                    </TableCell>
                    <TableCell className="check-group-divider text-right tabular-nums">
                      {count(row.basic_source_count)}
                    </TableCell>
                    <TableCell className="text-right tabular-nums">{count(row.basic_target_count)}</TableCell>
                    <TableCell
                      className={`text-right tabular-nums ${
                        difference ? 'font-semibold text-destructive' : 'text-muted-foreground'
                      }`}
                    >
                      {row.basic_count_diff == null ? '—' : signed(row.basic_count_diff)}
                    </TableCell>
                    <TableCell>
                      <div className="flex flex-col items-start gap-1">
                        <CheckStatus status={row.basic_status} running={basicRunning} />
                        {row.basic_status && <CheckedAt value={row.basic_checked_at} />}
                        <CheckError message={row.basic_error} />
                        <CheckedRange range={row.basic_checked_range} />
                      </div>
                    </TableCell>
                    <TableCell className="check-group-divider">
                      <div className="flex flex-col items-start gap-1">
                        <div className="flex items-center gap-2">
                          <CheckStatus status={row.detailed_status} running={detailedRunning} />
                          {row.detailed_status && <CheckedAt value={row.detailed_checked_at} />}
                        </div>
                        <DetailedFindings row={row} />
                        <CheckError message={row.detailed_error} />
                        <CheckedRange range={row.detailed_checked_range} />
                      </div>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => setDetailedFor(row)}
                          disabled={!canRun(row.ingestion_group) || starting !== null || detailedRunning}
                          title={
                            canRun(row.ingestion_group)
                              ? 'Compare chunk aggregates, then the rows of the chunks that differ'
                              : `Create a row check job for this location or for ${row.ingestion_group} first`
                          }
                        >
                          <ScanSearch className="mr-1.5 h-3.5 w-3.5" />
                          {starting === key ? 'Starting…' : 'Run detailed'}
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => setDetails(row)}
                          disabled={!row.basic_status && !row.detailed_status}
                          aria-label={`Details of ${row.source_table_name}`}
                          title="Details"
                        >
                          <PanelRightOpen className="h-4 w-4" />
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          <Pagination
            total={totalCount}
            page={tablePages.page}
            pageSize={tablePages.pageSize}
            onPageChange={tablePages.setPage}
            onPageSizeChange={tablePages.setPageSize}
            noun="tables"
          />
        </Card>
      )}
      <RowCheckJobDialog
        editing={editing}
        jobs={jobs}
        location={location}
        revision={revision}
        onClose={() => setEditing(null)}
        onSaved={() => {
          setEditing(null);
          void loadJob();
        }}
      />
      <DetailedCheckDialog
        key={detailedFor ? `${detailedFor.ingestion_group}/${detailedFor.source_table_name}` : 'closed'}
        row={detailedFor}
        onClose={() => setDetailedFor(null)}
        onStart={(row, range) => {
          setDetailedFor(null);
          void start('detailed', row, range);
        }}
      />
      <RowCheckDetailsDialog row={details} onClose={() => setDetails(null)} />
    </>
  );
}

type GroupRow = QueryRegistry['row_check_groups']['result'][number];

/** One row per ingestion group with its tables' latest results added up; opening a group filters the page to it. */
function GroupsTable({
  rows,
  loading,
  error,
  total,
  filteredByStatus,
  onOpen,
  onClearStatus,
  pages,
}: {
  rows: GroupRow[] | null;
  loading: boolean;
  error: string | null;
  total: number;
  filteredByStatus: boolean;
  onOpen: (group: string) => void;
  onClearStatus: () => void;
  pages: Pages;
}) {
  if (loading) return <Skeleton className="h-60" />;
  if (error) return <ErrorState message={error} />;
  if (!rows?.length) {
    return (
      <Empty>
        <EmptyHeader>
          <EmptyTitle>{filteredByStatus ? 'No group has a table in that state' : 'No enabled tables'}</EmptyTitle>
          <EmptyDescription>
            {filteredByStatus
              ? 'Choose another status to see more groups.'
              : 'Row checks run on the enabled rows of the configuration table.'}
          </EmptyDescription>
        </EmptyHeader>
        {filteredByStatus && (
          <Button variant="outline" size="sm" onClick={onClearStatus}>
            All statuses
          </Button>
        )}
      </Empty>
    );
  }
  return (
    <Card className="gap-0 overflow-hidden py-0">
      <Table>
        <TableHeader>
          <TableRow className="hover:bg-transparent">
            <TableHead>Group</TableHead>
            <TableHead className="text-right">Tables</TableHead>
            <TableHead>Latest results</TableHead>
            <TableHead className="text-right">Source rows</TableHead>
            <TableHead className="text-right">Target rows</TableHead>
            <TableHead className="text-right" title="Sum of |target − source| over the group's tables">
              Rows out of sync
            </TableHead>
            <TableHead>Last check</TableHead>
            <TableHead>
              <span className="sr-only">Open</span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => {
            const outOfSync = Number(row.rows_out_of_sync ?? 0);
            const states = [
              { count: Number(row.error_count), status: 'ERROR' },
              { count: Number(row.mismatch_count), status: 'MISMATCH' },
              { count: Number(row.match_count), status: 'MATCH' },
              { count: Number(row.unchecked_count), status: null },
            ].filter((item) => item.count > 0);
            return (
              <TableRow
                key={row.ingestion_group}
                className="cursor-pointer"
                onClick={() => onOpen(row.ingestion_group)}
              >
                <TableCell className="font-medium">{row.ingestion_group}</TableCell>
                <TableCell className="text-right tabular-nums">{count(row.table_count)}</TableCell>
                <TableCell>
                  <div className="flex flex-wrap items-center gap-1.5">
                    {states.map((item) => (
                      <span key={item.status ?? 'none'} className="flex items-center gap-1">
                        <CheckStatus status={item.status} />
                        <span className="text-xs tabular-nums text-muted-foreground">{count(item.count)}</span>
                      </span>
                    ))}
                  </div>
                </TableCell>
                <TableCell className="text-right tabular-nums">{count(row.source_count)}</TableCell>
                <TableCell className="text-right tabular-nums">{count(row.target_count)}</TableCell>
                <TableCell
                  className={`text-right tabular-nums ${outOfSync ? 'font-semibold text-destructive' : 'text-muted-foreground'}`}
                >
                  {row.rows_out_of_sync == null ? '—' : count(outOfSync)}
                </TableCell>
                <TableCell>
                  <CheckedAt value={row.last_checked_at} />
                </TableCell>
                <TableCell className="text-right">
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={(event) => {
                      event.stopPropagation();
                      onOpen(row.ingestion_group);
                    }}
                    aria-label={`Open ${row.ingestion_group}`}
                  >
                    Tables
                    <ChevronRight className="ml-1 h-3.5 w-3.5" />
                  </Button>
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
    </Card>
  );
}

type RowCheckRange = { rangeColumn: string; rangeFrom: string; rangeTo: string };
const noRange: RowCheckRange = { rangeColumn: '', rangeFrom: '', rangeTo: '' };

function CheckedRange({ range }: { range: string | null }) {
  if (!range) return null;
  return (
    <span className="font-mono text-xs text-muted-foreground" title="Only these rows were checked">
      range: {range}
    </span>
  );
}

function DetailedCheckDialog({
  row,
  onClose,
  onStart,
}: {
  row: RowCheckRow | null;
  onClose: () => void;
  onStart: (row: RowCheckRow, range: RowCheckRange) => void;
}) {
  // Remounted per table (see the key where it is rendered), so this starts unchecked each time.
  const [ranged, setRanged] = useState(false);
  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!row) return;
    const form = new FormData(e.currentTarget);
    onStart(
      row,
      ranged
        ? {
            rangeColumn: formString(form, 'rangeColumn', ''),
            rangeFrom: formString(form, 'rangeFrom', ''),
            rangeTo: formString(form, 'rangeTo', ''),
          }
        : noRange
    );
  };
  return (
    <Dialog open={!!row} onOpenChange={(value) => !value && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Detailed check</DialogTitle>
          <DialogDescription>
            Compares per-chunk counts, sums and hashes of <span className="font-mono">{row?.source_table_name}</span>{' '}
            with its target, then the rows of the chunks that differ.
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={submit} className="space-y-4">
          <Toggle name="ranged" label="Check a range of rows only" checked={ranged} onCheckedChange={setRanged} />
          {ranged && (
            <div className="grid gap-4 md:grid-cols-2">
              <div className="space-y-2 md:col-span-2">
                <Label htmlFor="range-column">Column</Label>
                <Input
                  id="range-column"
                  name="rangeColumn"
                  placeholder={row?.detailed_chunk_column ?? 'the table\u2019s chunk column'}
                />
                <p className="text-xs text-muted-foreground">
                  Leave empty to use the chunk column: the integer partition column or key, else the timestamp watermark
                  column.
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="range-from">From (inclusive)</Label>
                <Input id="range-from" name="rangeFrom" placeholder="-30d" />
              </div>
              <div className="space-y-2">
                <Label htmlFor="range-to">To (exclusive)</Label>
                <Input id="range-to" name="rangeTo" placeholder="no limit" />
              </div>
              <p className="text-xs text-muted-foreground md:col-span-2">
                A number, a date (<span className="font-mono">2026-09-01</span>), a timestamp (
                <span className="font-mono">2026-09-01 12:00:00</span>) or a number of days back from today (
                <span className="font-mono">-30d</span>). Leave a bound empty for no limit on that side.
              </p>
            </div>
          )}
          <DialogFooter>
            <Button type="button" variant="outline" onClick={onClose}>
              Cancel
            </Button>
            <Button type="submit">
              <ScanSearch className="mr-2 h-4 w-4" />
              {ranged ? 'Check range' : 'Check whole table'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

type Summary = {
  tables: number;
  groups: number;
  match: number;
  mismatch: number;
  error: number;
  lastCheck: string | null;
};

/** The latest basic check across the listed tables, in one line. */
function SummaryStrip({
  summary,
  jobs,
  loading,
  filtered,
}: {
  summary: Summary;
  jobs: RowCheckJob[];
  loading: boolean;
  filtered: boolean;
}) {
  if (loading) return <Skeleton className="mb-6 h-12" />;
  const scheduled = jobs.filter((job) => job.schedule.enabled && job.schedule.pauseStatus === 'UNPAUSED').length;
  const items = [
    { Icon: CheckCircle2, value: summary.match, label: 'match', tone: 'text-success' },
    { Icon: XCircle, value: summary.mismatch, label: 'differ', tone: summary.mismatch ? 'text-destructive' : '' },
    {
      Icon: TriangleAlert,
      value: summary.error,
      label: 'errors',
      tone: summary.error ? 'text-warning-foreground' : '',
    },
  ];
  return (
    // A plain bordered row: Card lays its children out as a column.
    <div className="mb-6 flex flex-wrap items-center gap-x-6 gap-y-2 rounded-xl border bg-card px-4 py-3 text-sm shadow-sm">
      <span className="font-medium">
        Basic check{' '}
        <span className="font-normal text-muted-foreground">
          · {summary.tables.toLocaleString()} {filtered ? 'filtered' : 'enabled'}{' '}
          {summary.tables === 1 ? 'table' : 'tables'}
          {!filtered && ` in ${summary.groups.toLocaleString()} ${summary.groups === 1 ? 'group' : 'groups'}`}
        </span>
      </span>
      {items.map(({ Icon, value, label, tone }) => (
        <span key={label} className={`flex items-center gap-1.5 ${value ? tone : 'text-muted-foreground'}`}>
          <Icon className="h-4 w-4" aria-hidden />
          <span className="font-semibold tabular-nums">{value}</span> {label}
        </span>
      ))}
      <span className="ml-auto flex flex-wrap items-center gap-x-4 gap-y-1 text-muted-foreground">
        <span title={summary.lastCheck ? displayTime(summary.lastCheck) : undefined}>
          Last check {relativeTime(summary.lastCheck)}
        </span>
        <span>
          {scheduled
            ? `${scheduled} scheduled row check ${scheduled === 1 ? 'job' : 'jobs'}`
            : 'No scheduled row check job'}
        </span>
      </span>
    </div>
  );
}

/** A schedule in one line: its cron and time zone, paused, or manual only. */
const scheduleLabel = (schedule: Schedule) =>
  !schedule.enabled
    ? 'Manual only'
    : `${schedule.pauseStatus === 'PAUSED' ? 'Paused · ' : ''}${schedule.quartzCronExpression} (${schedule.timezoneId})`;

function RunState({ run }: { run: RowCheckRun }) {
  const state = run.resultState ?? run.lifeCycleState ?? 'UNKNOWN';
  return (
    // The default Badge is the brand red, which would make a successful run look failed.
    <span
      className={`check-status ${
        state === 'SUCCESS' ? 'check-status-match' : isActive(run) ? 'check-status-running' : 'check-status-mismatch'
      }`}
      title={run.stateMessage ?? ''}
    >
      {isActive(run) && <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden />}
      {state.charAt(0) + state.slice(1).toLowerCase().replace(/_/g, ' ')}
    </span>
  );
}

/** The row check jobs of the location and of its groups, with what each checks and when. */
function RowCheckJobsCard({
  jobs,
  loading,
  location,
  onEdit,
  onDeleted,
}: {
  jobs: RowCheckJob[];
  loading: boolean;
  location: Location;
  onEdit: (job: RowCheckJob | 'new') => void;
  onDeleted: () => void;
}) {
  const [deleting, setDeleting] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);
  const remove = async (job: RowCheckJob) => {
    if (!window.confirm(`Delete the job "${job.name}" and its run history? Check results stay on this page.`)) return;
    setDeleting(job.jobId);
    setError(null);
    try {
      const query = new URLSearchParams({ ...location, ingestionGroup: job.ingestionGroup, checkMode: job.checkMode });
      await api(`/api/row-checks/job?${query.toString()}`, { method: 'DELETE' });
      onDeleted();
    } catch (x) {
      setError(reportError('row_checks.job_delete', x, { ...location, jobId: job.jobId }));
    } finally {
      setDeleting(null);
    }
  };
  if (loading) return <Skeleton className="mb-6 h-32" />;
  return (
    <Card className="mb-6 gap-0 overflow-hidden py-0">
      <CardHeader className="py-4">
        <CardTitle className="text-base">Row check jobs</CardTitle>
        <CardDescription>
          Each job runs a basic or a detailed check of every enabled table, for the whole location or one group, and
          writes to <span className="font-mono">{fqn(location, '_watersync_row_checks')}</span>.
        </CardDescription>
      </CardHeader>
      {error && (
        <div className="px-6 pb-4">
          <ErrorState message={error} />
        </div>
      )}
      {!jobs.length ? (
        <Empty className="border-t">
          <EmptyHeader>
            <EmptyTitle>No row check job yet</EmptyTitle>
            <EmptyDescription>
              Create one to check the tables on a schedule and to start checks from this page.
            </EmptyDescription>
          </EmptyHeader>
          <Button size="sm" onClick={() => onEdit('new')}>
            <Plus className="mr-2 h-4 w-4" />
            New row check job
          </Button>
        </Empty>
      ) : (
        <Table className="border-t">
          <TableHeader>
            <TableRow className="hover:bg-transparent">
              <TableHead>Job</TableHead>
              <TableHead>Checks</TableHead>
              <TableHead>Schedule</TableHead>
              <TableHead>Compute</TableHead>
              <TableHead>Latest run</TableHead>
              <TableHead>
                <span className="sr-only">Actions</span>
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {jobs.map((job) => {
              const latest = job.runs[0];
              return (
                <TableRow key={job.jobId}>
                  <TableCell className="max-w-80">
                    <a
                      href={job.workspaceUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="flex items-center gap-1 truncate font-medium hover:underline"
                      title={`Open job ${job.jobId} in Databricks`}
                    >
                      {job.name}
                      <ExternalLink className="h-3.5 w-3.5 shrink-0 text-muted-foreground" aria-hidden />
                    </a>
                  </TableCell>
                  <TableCell>
                    <div className="flex flex-wrap items-center gap-1.5">
                      <span className="font-medium capitalize">{job.checkMode}</span>
                      {job.ingestionGroup ? (
                        <Badge variant="outline" className="font-normal text-muted-foreground">
                          {job.ingestionGroup}
                        </Badge>
                      ) : (
                        <span className="text-muted-foreground">all groups</span>
                      )}
                    </div>
                    {job.checkMode === 'detailed' && (
                      <div className="text-xs text-muted-foreground">
                        {job.rangeFrom ? `Rows from ${job.rangeFrom}` : 'Every row'}
                      </div>
                    )}
                  </TableCell>
                  <TableCell className="font-mono text-xs">{scheduleLabel(job.schedule)}</TableCell>
                  <TableCell className="text-xs text-muted-foreground">
                    {job.compute.mode === 'SERVERLESS' ? 'Serverless' : 'Job cluster'}
                  </TableCell>
                  <TableCell>
                    {latest ? (
                      <a
                        href={latest.runUrl}
                        target="_blank"
                        rel="noreferrer"
                        className="flex flex-col items-start gap-1 text-xs hover:underline"
                      >
                        <RunState run={latest} />
                        <span className="text-muted-foreground">{displayTime(latest.startTime)}</span>
                      </a>
                    ) : (
                      <span className="text-xs text-muted-foreground">Not run yet</span>
                    )}
                  </TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end gap-1">
                      <Button size="sm" variant="outline" onClick={() => onEdit(job)}>
                        <Settings2 className="mr-1.5 h-3.5 w-3.5" />
                        Edit
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => void remove(job)}
                        disabled={deleting !== null}
                        aria-label={`Delete ${job.name}`}
                        title="Delete job"
                      >
                        {deleting === job.jobId ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <Trash2 className="h-4 w-4" />
                        )}
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      )}
    </Card>
  );
}

/** The latest runs of every row check job, newest first. */
function RunStrip({ runs, activeRuns }: { runs: RowCheckRun[]; activeRuns: RowCheckRun[] }) {
  const recent = runs.slice(0, 6);
  return (
    <Card className="mb-6">
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Recent check runs</CardTitle>
        <CardDescription>
          {activeRuns.length
            ? `${activeRuns.length} running · this page updates when they finish`
            : 'Results appear in the table below when a run finishes.'}
        </CardDescription>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-2">
        {recent.length === 0 ? (
          <span className="text-sm text-muted-foreground">No row check job has run yet.</span>
        ) : (
          recent.map((run) => (
            <a
              key={`${run.jobId}/${run.runId}`}
              href={run.runUrl}
              target="_blank"
              rel="noreferrer"
              className="flex items-center gap-2 rounded-md border px-3 py-1.5 text-xs hover:bg-muted"
              title={run.stateMessage ?? ''}
            >
              <RunState run={run} />
              <span className="font-medium">{run.checkMode}</span>
              <span className="font-mono text-muted-foreground">
                {run.sourceTableName || run.ingestionGroup || 'all tables'}
                {(run.rangeFrom || run.rangeTo) &&
                  ` · ${run.rangeColumn || 'chunk column'} ${[run.rangeFrom && `>= ${run.rangeFrom}`, run.rangeTo && `< ${run.rangeTo}`].filter(Boolean).join(' and ')}`}
              </span>
              <span className="text-muted-foreground">{displayTime(run.startTime)}</span>
            </a>
          ))
        )}
      </CardContent>
    </Card>
  );
}

// A new job's schedule: the basic check daily, the detailed check weekly, since it reads much more.
const defaultSchedules: Record<RowCheckMode, JobSchedule> = {
  basic: { quartz_cron_expression: '0 0 6 * * ?', timezone_id: 'UTC' },
  detailed: { quartz_cron_expression: '0 0 7 ? * SUN', timezone_id: 'UTC' },
};
const scopeLabel = (location: Location, ingestionGroup: string) =>
  ingestionGroup ? `the ${ingestionGroup} group` : `every group of ${location.catalog}.${location.schema}`;
const jobNameOf = (location: Location, ingestionGroup: string, mode: RowCheckMode) =>
  ingestionGroup
    ? `[${ingestionGroup}] Row Check (${mode})`
    : `[${location.catalog}.${location.schema}] Row Checks${mode === 'detailed' ? ' (detailed)' : ''}`;

function RowCheckJobDialog({
  editing,
  jobs,
  location,
  revision,
  onClose,
  onSaved,
}: {
  editing: RowCheckJob | 'new' | null;
  jobs: RowCheckJob[];
  location: Location;
  revision: number;
  onClose: () => void;
  onSaved: () => void;
}) {
  const job = editing === 'new' ? null : editing;
  return (
    <Dialog open={editing !== null} onOpenChange={(value) => !value && onClose()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-3xl">
        <DialogHeader>
          <DialogTitle>{job ? `Edit ${job.name}` : 'New row check job'}</DialogTitle>
          <DialogDescription>
            A job runs one check, basic or detailed, over every enabled table of the location or of one group, and
            writes to <span className="font-mono">{fqn(location, '_watersync_row_checks')}</span>. It offers the same
            compute and identity options as an ingestion job.
          </DialogDescription>
        </DialogHeader>
        {/* Remounted on every open, so the fields start from the job's current settings. */}
        {editing !== null && (
          <RowCheckJobForm
            key={job?.jobId ?? 'new'}
            job={job}
            jobs={jobs}
            location={location}
            revision={revision}
            onClose={onClose}
            onSaved={onSaved}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}

function RowCheckJobForm({
  job,
  jobs,
  location,
  revision,
  onClose,
  onSaved,
}: {
  job: RowCheckJob | null;
  jobs: RowCheckJob[];
  location: Location;
  revision: number;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [tab, setTab] = useState('check');
  // The scope and mode name the job, so they are fixed once it exists.
  const [ingestionGroup, setIngestionGroup] = useState(job?.ingestionGroup ?? '');
  const [checkMode, setCheckMode] = useState<RowCheckMode>(job?.checkMode ?? 'basic');
  // A new job is scheduled on serverless as the app; an existing one keeps what the Databricks API reports.
  const [scheduleEnabled, setScheduleEnabled] = useState(job ? job.schedule.enabled : true);
  const [scheduleActive, setScheduleActive] = useState(job?.schedule.pauseStatus !== 'PAUSED');
  const [computeMode, setComputeMode] = useState<ComputeMode>(job?.compute.mode ?? 'SERVERLESS');
  const [performanceTarget, setPerformanceTarget] = useState<PerformanceTarget>(
    job?.compute.performanceTarget ?? 'STANDARD'
  );
  const [runAsMode, setRunAsMode] = useState<string>(job?.runAs.mode ?? 'DEFAULT');
  const nodeTypes = useNodeTypes(computeMode === 'JOB_CLUSTER');
  const groupParams = useMemo(
    () => ({ table_name: sql.string(fqn(location, 'jdbc_ingestion_config')), refresh_token: sql.int(revision) }),
    [location, revision]
  );
  const { data: groups, loading: groupsLoading } = useAnalyticsQuery('ingestion_groups', groupParams, {
    autoStart: !job,
  });
  // A new job for a scope and mode that already has one updates that job.
  const existing = job
    ? null
    : jobs.find((item) => item.ingestionGroup === ingestionGroup && item.checkMode === checkMode);
  const schedule = job
    ? { quartz_cron_expression: job.schedule.quartzCronExpression, timezone_id: job.schedule.timezoneId }
    : defaultSchedules[checkMode];
  // Inactive tabs are hidden, so the browser cannot focus an invalid field there and blocks the submit
  // silently. Reveal the tab holding the first invalid field and say what is missing.
  const revealInvalidField = (event: FormEvent<HTMLFormElement>) => {
    const field = event.target as HTMLInputElement;
    if (event.currentTarget.querySelector(':invalid') !== field) return;
    const fieldTab = field.closest<HTMLElement>('[data-tab]')?.dataset.tab;
    if (fieldTab) setTab(fieldTab);
    const label = field.labels?.[0]?.textContent ?? field.getAttribute('aria-label') ?? field.name;
    setError(`Check "${label}": ${field.validationMessage}`);
  };
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const form = new FormData(e.currentTarget);
    try {
      await api('/api/row-checks/job', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          ingestionGroup,
          checkMode,
          rangeFrom: checkMode === 'detailed' ? formString(form, 'rangeFrom', '') : '',
          gitUrl: form.get('gitUrl'),
          gitBranch: form.get('gitBranch'),
          maxParallelTables: Number(form.get('maxParallelTables')),
          schedule: {
            enabled: scheduleEnabled,
            quartzCronExpression: formString(form, 'quartzCronExpression', schedule.quartz_cron_expression),
            timezoneId: formString(form, 'timezoneId', 'UTC'),
            pauseStatus: scheduleActive ? 'UNPAUSED' : 'PAUSED',
          },
          compute: computeFromForm(form, computeMode, performanceTarget),
          runAs: { mode: runAsMode, principal: formString(form, 'jobRunAsPrincipal', '') },
        }),
      });
      onSaved();
    } catch (x) {
      setError(reportError('row_checks.job_save', x, { ...location, ingestionGroup, checkMode, computeMode }));
    } finally {
      setBusy(false);
    }
  };
  return (
    <form onSubmit={(event) => void submit(event)} onInvalidCapture={revealInvalidField} className="space-y-4">
      {error && <ErrorState message={error} />}
      <NodeTypeOptions nodeTypes={nodeTypes} />
      <Tabs value={tab} onValueChange={setTab} className="w-full">
        <TabsList className="flex h-auto w-full [&>button]:flex-auto [&>button]:px-3">
          <TabsTrigger value="check">Check</TabsTrigger>
          <TabsTrigger value="source">Git source &amp; execution</TabsTrigger>
          <TabsTrigger value="compute">Job Compute</TabsTrigger>
          <TabsTrigger value="schedule">Schedule</TabsTrigger>
          <TabsTrigger value="run-as">Run as</TabsTrigger>
        </TabsList>
        <TabsContent value="check" data-tab="check" forceMount className="mt-4 data-[state=inactive]:hidden">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="row-check-scope">Tables</Label>
              {groupsLoading ? (
                <Skeleton className="h-9" />
              ) : (
                <Select
                  value={ingestionGroup || 'all'}
                  onValueChange={(value) => setIngestionGroup(value === 'all' ? '' : value)}
                  disabled={Boolean(job)}
                >
                  <SelectTrigger id="row-check-scope" className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">Every group</SelectItem>
                    {job?.ingestionGroup && <SelectItem value={job.ingestionGroup}>{job.ingestionGroup}</SelectItem>}
                    {groups?.map((group) => (
                      <SelectItem key={group.ingestion_group} value={group.ingestion_group}>
                        {group.ingestion_group} {'·'} {group.enabled_source_count} enabled sources
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>
            <div className="space-y-2">
              <Label htmlFor="row-check-mode">Check</Label>
              <Select
                value={checkMode}
                onValueChange={(value) => setCheckMode(value as RowCheckMode)}
                disabled={Boolean(job)}
              >
                <SelectTrigger id="row-check-mode" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="basic">Basic · row counts</SelectItem>
                  <SelectItem value="detailed">Detailed · chunk aggregates, then rows</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <p className="text-xs text-muted-foreground md:col-span-2">
              {checkMode === 'basic'
                ? 'Compares the row counts of source and target. Cheap: one COUNT(*) per side and table.'
                : 'Compares per-chunk aggregates, then the rows of the chunks that differ. Reads more; schedule it less often.'}{' '}
              The job is <span className="font-mono">{jobNameOf(location, ingestionGroup, checkMode)}</span> and checks{' '}
              {scopeLabel(location, ingestionGroup)}.
            </p>
            {checkMode === 'detailed' && (
              <div className="space-y-2 md:col-span-2">
                <Label htmlFor="row-check-range-from">Rows from (optional)</Label>
                <Input
                  id="row-check-range-from"
                  name="rangeFrom"
                  placeholder="-30d"
                  defaultValue={job?.rangeFrom ?? ''}
                  pattern="^$|^-?\d+(\.\d+)?$|^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$|^-\d{1,5}d$"
                  title="A number, a date (2026-09-01), a timestamp (2026-09-01 12:00:00) or days back (-30d)"
                />
                <p className="text-xs text-muted-foreground">
                  Checks only rows from this value of each table&apos;s chunk column, for example the last 30 days
                  (-30d). Empty checks every row.
                </p>
              </div>
            )}
            {existing && (
              <Alert className="md:col-span-2">
                <TriangleAlert className="h-4 w-4" />
                <AlertTitle>This job already exists</AlertTitle>
                <AlertDescription>Saving replaces the settings of {existing.name}.</AlertDescription>
              </Alert>
            )}
          </div>
        </TabsContent>
        <TabsContent value="source" data-tab="source" forceMount className="mt-4 data-[state=inactive]:hidden">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2 md:col-span-2">
              <Label htmlFor="row-check-git-url">GitHub repository</Label>
              <Input
                id="row-check-git-url"
                name="gitUrl"
                type="url"
                required
                defaultValue={job?.gitUrl || 'https://github.com/databricks-industry-solutions/watersync'}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="row-check-git-branch">Branch</Label>
              <Input id="row-check-git-branch" name="gitBranch" required defaultValue={job?.gitBranch ?? 'main'} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="row-check-parallel">Tables checked in parallel</Label>
              <Input
                id="row-check-parallel"
                name="maxParallelTables"
                type="number"
                min={1}
                max={32}
                required
                defaultValue={job?.maxParallelTables ?? 4}
              />
            </div>
          </div>
        </TabsContent>
        <TabsContent value="compute" data-tab="compute" forceMount className="mt-4 data-[state=inactive]:hidden">
          <JobComputeFields
            mode={computeMode}
            onModeChange={setComputeMode}
            performanceTarget={performanceTarget}
            onPerformanceTargetChange={setPerformanceTarget}
            nodeTypes={nodeTypes}
            saved={job?.compute}
            clusterNote="The row check task runs on this autoscaling job cluster. Detailed checks aggregate the whole target table in Spark, so size it like the ingestion cluster."
          />
        </TabsContent>
        <TabsContent value="schedule" data-tab="schedule" forceMount className="mt-4 data-[state=inactive]:hidden">
          {/* Keyed by mode, so a new job's cron follows the check picked. */}
          <ScheduleFields
            key={checkMode}
            enabled={scheduleEnabled}
            onEnabledChange={setScheduleEnabled}
            active={scheduleActive}
            onActiveChange={setScheduleActive}
            schedule={schedule}
          />
          <p className="mt-3 text-xs text-muted-foreground">
            Without a schedule the job only runs when a check is started from this page or with Run now.
          </p>
        </TabsContent>
        <TabsContent value="run-as" data-tab="run-as" forceMount className="mt-4 data-[state=inactive]:hidden">
          <div className="grid gap-4 md:grid-cols-2">
            <RunAsFields
              id="row-check-run-as"
              label="Job runs as"
              mode={runAsMode}
              onModeChange={setRunAsMode}
              principalName="jobRunAsPrincipal"
              defaultPrincipal={job?.runAs.mode === runAsMode ? job.runAs.principal : ''}
            />
            <p className="self-end text-xs text-muted-foreground">
              The identity needs the same source access as the ingestion jobs (UC connections, secrets) and SELECT on
              the target tables.
            </p>
            <p className="text-xs text-muted-foreground md:col-span-2">
              The owner is the app service principal that creates the job. Running as another user requires workspace
              admin rights for the app; running as another service principal requires the app to have the Service
              Principal User role on it.
            </p>
          </div>
        </TabsContent>
      </Tabs>
      <DialogFooter>
        <Button type="button" variant="outline" onClick={onClose} disabled={busy}>
          Cancel
        </Button>
        <Button type="submit" disabled={busy}>
          {busy ? 'Saving…' : job || existing ? 'Update job' : 'Create job'}
        </Button>
      </DialogFooter>
    </form>
  );
}

type ChunkDetails = {
  chunking: 'numeric' | 'month' | 'none';
  summed_columns: string[];
  hashed_columns: string[];
  // Results written before booleans and dates were aggregated have neither.
  boolean_columns?: string[];
  date_columns?: string[];
  note?: string;
  chunks: Array<{
    chunk: number | null;
    label: string;
    source_rows: number;
    target_rows: number;
    differs: string[];
    drilled: boolean;
    missing_in_target?: number;
    extra_in_target?: number;
    mismatched?: number;
  }>;
};
const chunkingLabels = { numeric: 'ranges of', month: 'months of', none: 'one chunk' } as const;

function ChunkRowResult({ chunk }: { chunk: ChunkDetails['chunks'][number] }) {
  if (!chunk.drilled) return <span className="text-muted-foreground">not compared (chunk limit)</span>;
  const found = (chunk.missing_in_target ?? 0) + (chunk.extra_in_target ?? 0) + (chunk.mismatched ?? 0);
  if (!found) return <span className="text-muted-foreground">no row differences</span>;
  return (
    <span>
      {count(chunk.missing_in_target)} missing · {count(chunk.extra_in_target)} extra
      {chunk.mismatched != null && ` · ${count(chunk.mismatched)} changed`}
    </span>
  );
}

function RowCheckDetailsDialog({ row, onClose }: { row: RowCheckRow | null; onClose: () => void }) {
  const chunkDetails = useMemo(
    () => jsonColumn<ChunkDetails>(row?.detailed_chunk_details, 'row_checks.chunk_details_unreadable'),
    [row]
  );
  const samples = useMemo(
    () =>
      jsonColumn<Record<string, Array<Record<string, string>>>>(
        row?.detailed_sample_keys,
        'row_checks.sample_keys_unreadable'
      ),
    [row]
  );
  const sampleLabels: Record<string, string> = {
    missing_in_target: 'Missing in target',
    extra_in_target: 'Extra in target',
    mismatched: 'Changed values',
  };
  return (
    <Dialog open={!!row} onOpenChange={(value) => !value && onClose()}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>Row check details</DialogTitle>
          <DialogDescription className="break-all">
            {row?.ingestion_group} / <span className="font-mono">{row?.source_table_name}</span> →{' '}
            <span className="font-mono">{row?.target_table_fqn}</span>
          </DialogDescription>
        </DialogHeader>
        {row && (
          <div className="space-y-5 text-sm">
            <section className="space-y-1">
              <h3 className="text-sm font-semibold">Basic check · {displayTime(row.basic_checked_at)}</h3>
              <CheckedRange range={row.basic_checked_range} />
              {row.basic_error ? (
                <ErrorState message={row.basic_error} />
              ) : (
                <p>
                  Source {count(row.basic_source_count)} rows · target {count(row.basic_target_count)} active rows ·
                  difference {signed(row.basic_count_diff)}
                </p>
              )}
            </section>
            <section className="space-y-2">
              <h3 className="text-sm font-semibold">Detailed check · {displayTime(row.detailed_checked_at)}</h3>
              <CheckedRange range={row.detailed_checked_range} />
              {!row.detailed_status ? (
                <p className="text-muted-foreground">No detailed check has run for this table.</p>
              ) : row.detailed_error ? (
                <ErrorState message={row.detailed_error} />
              ) : (
                <>
                  <p>
                    Source {count(row.detailed_source_count)} · target {count(row.detailed_target_count)} ·{' '}
                    {count(row.detailed_mismatched_chunks)} of {count(row.detailed_chunk_count)} chunks differ ·{' '}
                    {count(row.detailed_missing_in_target)} missing in target · {count(row.detailed_extra_in_target)}{' '}
                    extra in target ·{' '}
                    {row.detailed_mismatched_rows == null
                      ? 'changed values not compared (no key columns)'
                      : `${count(row.detailed_mismatched_rows)} with changed values`}
                    {row.detailed_duration_ms != null &&
                      ` · took ${Math.round(Number(row.detailed_duration_ms) / 1000)}s`}
                  </p>
                  {chunkDetails && (
                    <div className="space-y-2">
                      <p className="text-xs text-muted-foreground">
                        Aggregates compared over{' '}
                        {chunkDetails.chunking === 'none'
                          ? 'one chunk (no integer key, partition column or timestamp watermark to split by)'
                          : `${count(row.detailed_chunk_count)} chunks: ${chunkingLabels[chunkDetails.chunking]} ${row.detailed_chunk_column}`}
                        .{' '}
                        {(
                          [
                            ['sums of', chunkDetails.summed_columns],
                            ['hashes of', chunkDetails.hashed_columns],
                            ['true/false/null counts of', chunkDetails.boolean_columns ?? []],
                            ['date sums of', chunkDetails.date_columns ?? []],
                          ] as const
                        )
                          .filter(([, columns]) => columns.length > 0)
                          .map(([label, columns], index) => (
                            <span key={label}>
                              {index > 0 && '; '}
                              {index === 0 ? label.charAt(0).toUpperCase() + label.slice(1) : label}{' '}
                              <span className="font-mono">{columns.join(', ')}</span>
                            </span>
                          ))}
                        . Timestamps and floats are compared only in the rows of differing chunks.
                      </p>
                      {chunkDetails.note && <p className="text-xs text-muted-foreground">{chunkDetails.note}</p>}
                      {chunkDetails.chunks.length > 0 && (
                        <Table>
                          <TableHeader>
                            <TableRow>
                              <TableHead>Chunk</TableHead>
                              <TableHead className="text-right">Source</TableHead>
                              <TableHead className="text-right">Target</TableHead>
                              <TableHead>Differs in</TableHead>
                              <TableHead>Rows</TableHead>
                            </TableRow>
                          </TableHeader>
                          <TableBody>
                            {chunkDetails.chunks.map((chunk) => (
                              <TableRow key={chunk.label}>
                                <TableCell className="font-mono text-xs">{chunk.label}</TableCell>
                                <TableCell className="text-right tabular-nums">{count(chunk.source_rows)}</TableCell>
                                <TableCell className="text-right tabular-nums">{count(chunk.target_rows)}</TableCell>
                                <TableCell className="whitespace-normal break-words font-mono text-xs">
                                  {chunk.differs.join(', ')}
                                </TableCell>
                                <TableCell className="whitespace-normal text-xs">
                                  <ChunkRowResult chunk={chunk} />
                                </TableCell>
                              </TableRow>
                            ))}
                          </TableBody>
                        </Table>
                      )}
                    </div>
                  )}
                  {row.detailed_compared_columns && Number(row.detailed_drilled_chunks) > 0 && (
                    <p className="text-xs text-muted-foreground">
                      Rows of the differing chunks compared on:{' '}
                      <span className="font-mono">{row.detailed_compared_columns.split(',').join(', ')}</span>
                    </p>
                  )}
                  {samples &&
                    Object.entries(samples).map(([label, keys]) => (
                      <div key={label}>
                        <div className="text-xs font-medium">
                          {sampleLabels[label] ?? label} · first {keys.length} keys
                        </div>
                        <div className="mt-1 flex flex-wrap gap-1">
                          {keys.map((item) => {
                            const text = Object.entries(item)
                              .map(([column, value]) => `${column}=${value}`)
                              .join(', ');
                            return (
                              <Badge key={text} variant="outline" className="font-mono">
                                {text}
                              </Badge>
                            );
                          })}
                        </div>
                      </div>
                    ))}
                </>
              )}
            </section>
          </div>
        )}
      </DialogContent>
    </Dialog>
  );
}

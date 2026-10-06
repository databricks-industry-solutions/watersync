import { useEffect, useMemo, useState } from 'react';
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
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import {
  Clock3,
  ExternalLink,
  Play,
  Plus,
  RefreshCw,
} from 'lucide-react';
import { api } from '../lib/api';
import { reportError, useReportedFailure } from '../lib/logging';
import type { JobRow, Location } from '../types';
import { fqn, displayTime, runStatus, statusVariant, runDuration, formString } from '../utils';
import { useControl } from './Layout';
import { PageTitle, ErrorState, ScheduleFields } from './shared';

export function JobsPage() {
  const { location, revision } = useControl();
  const [jobs, setJobs] = useState<JobRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const [runningJob, setRunningJob] = useState<number | null>(null);
  const [scheduleJob, setScheduleJob] = useState<JobRow | null>(null);
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
  const load = () => {
    setLoading(true);
    setError(null);
    api<{ jobs: JobRow[] }>('/api/jobs')
      .then((x) => setJobs(x.jobs))
      .catch((e: unknown) => setError(reportError('jobs.list', e)))
      .finally(() => setLoading(false));
  };
  useEffect(() => {
    const timer = window.setTimeout(load, 0);
    return () => window.clearTimeout(timer);
  }, []);
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
  return (
    <>
      <PageTitle
        title="WaterSync Lakeflow Jobs"
        description="Monitor recent run health, trigger ingestion, or open the full job in Databricks."
        action={
          <Button onClick={() => setOpen(true)}>
            <Plus className="mr-2 h-4 w-4" />
            Create job
          </Button>
        }
      />
      {error && <ErrorState message={error} />}{' '}
      {loading ? (
        <Skeleton className="h-72" />
      ) : !jobs.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No jobs visible</EmptyTitle>
            <EmptyDescription>
              Grant the app service principal workspace access or create the first job.
            </EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <div className="grid gap-4 lg:grid-cols-2">
          {jobs.map((job) => (
            <Card key={job.job_id} className="job-card">
              <CardHeader>
                <div className="flex justify-between gap-4">
                  <div>
                    <CardTitle>{job.settings?.name ?? `Job ${job.job_id}`}</CardTitle>
                    <CardDescription>
                      ID {job.job_id} {'\u00b7'} created {displayTime(job.created_time)}
                    </CardDescription>
                  </div>
                  <Badge variant={statusVariant(runStatus(job.runs[0]))}>
                    {runStatus(job.runs[0]).replaceAll('_', ' ')}
                  </Badge>
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
                    <Badge variant={job.settings?.schedule?.pause_status === 'UNPAUSED' ? 'secondary' : 'outline'}>
                      {!job.settings?.schedule
                        ? 'Manual only'
                        : job.settings.schedule.pause_status === 'UNPAUSED'
                          ? 'Active'
                          : 'Paused'}
                    </Badge>
                  </div>
                  {job.settings?.schedule ? (
                    <div className="text-xs text-muted-foreground">{job.settings.schedule.timezone_id}</div>
                  ) : (
                    <p className="text-xs text-muted-foreground">Runs only when manually triggered.</p>
                  )}
                </div>
                <div>
                  <div className="mb-2 flex items-center justify-between gap-3 text-xs text-muted-foreground">
                    <span className="font-medium text-foreground">Last 10 runs</span>
                    <span>Newest on the right</span>
                  </div>
                  {job.runs.length ? (
                    <div
                      className="run-timeline"
                      aria-label={`Recent run status for ${job.settings?.name ?? `job ${job.job_id}`}`}
                    >
                      {[...job.runs].reverse().map((recentRun) => {
                        const status = runStatus(recentRun);
                        const label = `${status.replaceAll('_', ' ')} \u00b7 ${displayTime(recentRun.start_time)} \u00b7 ${runDuration(recentRun)}`;
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
                              <RefreshCw className="h-4 w-4 animate-spin" aria-hidden="true" />
                            ) : (
                              <span>{status === 'SUCCESS' ? '\u2713' : '!'}</span>
                            )}
                          </a>
                        );
                      })}
                    </div>
                  ) : (
                    <p className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                      No runs recorded yet.
                    </p>
                  )}
                  <p className="mt-2 text-xs text-muted-foreground">
                    Latest:{' '}
                    {job.runs[0] ? `${displayTime(job.runs[0].start_time)} \u00b7 ${runDuration(job.runs[0])}` : 'Never run'}
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <Button
                    disabled={!job.job_id || runningJob === job.job_id}
                    onClick={() => {
                      if (job.job_id) void run(job.job_id);
                    }}
                  >
                    <Play className="mr-2 h-4 w-4" />
                    {runningJob === job.job_id ? 'Starting\u2026' : 'Run now'}
                  </Button>
                  <Button
                    variant="outline"
                    disabled={!job.job_id || runningJob === job.job_id}
                    onClick={() => {
                      if (job.job_id && confirm('Run a full refresh? Staging will be replaced with a fresh snapshot and CDC will reprocess from scratch.')) {
                        void run(job.job_id, true);
                      }
                    }}
                  >
                    <RefreshCw className="mr-2 h-4 w-4" />
                    Full refresh
                  </Button>
                  <Button variant="outline" asChild>
                    <a href={job.workspace_url} target="_blank" rel="noreferrer">
                      Open in Databricks
                      <ExternalLink className="ml-2 h-4 w-4" />
                    </a>
                  </Button>
                  <Button variant="outline" onClick={() => setScheduleJob(job)} disabled={!job.job_id}>
                    <Clock3 className="mr-2 h-4 w-4" />
                    Edit schedule
                  </Button>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}
      <JobDialog
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

function JobDialog({
  open,
  onOpenChange,
  location,
  groups,
  groupsLoading,
  groupsError,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  location: Location;
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
  const [ingestionGroup, setIngestionGroup] = useState('');
  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [scheduleActive, setScheduleActive] = useState(true);
  const [gitUrl, setGitUrl] = useState('https://github.com/databricks-industry-solutions/watersync');
  const [gitBranch, setGitBranch] = useState('main');
  const [computeMode, setComputeMode] = useState<'SERVERLESS' | 'JOB_CLUSTER'>('SERVERLESS');
  const [performanceTarget, setPerformanceTarget] = useState<'STANDARD' | 'PERFORMANCE_OPTIMIZED'>('STANDARD');
  useEffect(() => {
    if (open && !ingestionGroup && groups[0]) setIngestionGroup(groups[0].ingestion_group);
  }, [open, ingestionGroup, groups]);
  const selectedGroup = groups.find((group) => group.ingestion_group === ingestionGroup);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const form = new FormData(e.currentTarget);
      const result = await api<{ jobId: number; action: 'created' | 'updated' }>('/api/jobs', {
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
          compute: {
            mode: computeMode,
            performanceTarget,
            sparkVersion: formString(form, 'sparkVersion', ''),
            driverNodeTypeId: formString(form, 'driverNodeTypeId', ''),
            workerNodeTypeId: formString(form, 'workerNodeTypeId', ''),
            minWorkers: Number(form.get('minWorkers') ?? 1),
            maxWorkers: Number(form.get('maxWorkers') ?? 4),
          },
        }),
      });
      alert(`Job ${result.jobId} ${result.action}`);
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
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          {groupsError && <ErrorState message={groupsError} />}
          <Tabs defaultValue="job" className="w-full">
            <TabsList className={`grid w-full ${computeMode === 'JOB_CLUSTER' ? 'grid-cols-5' : 'grid-cols-4'}`}>
              <TabsTrigger value="job">Job configuration</TabsTrigger>
              <TabsTrigger value="source">Git source &amp; execution</TabsTrigger>
              <TabsTrigger value="compute">Compute</TabsTrigger>
              {computeMode === 'JOB_CLUSTER' && <TabsTrigger value="cluster">Job cluster</TabsTrigger>}
              <TabsTrigger value="schedule">Schedule</TabsTrigger>
            </TabsList>
            <TabsContent value="job" forceMount className="mt-4 space-y-4 data-[state=inactive]:hidden">
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
                      [{selectedGroup.ingestion_group}] Ingestion Pipeline {'\u00b7'} {selectedGroup.source_count} configured
                      sources
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
            <TabsContent value="source" forceMount className="mt-4 data-[state=inactive]:hidden">
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
                  <Label htmlFor="pipeline-id">Existing CDC pipeline ID (optional)</Label>
                  <Input
                    id="pipeline-id"
                    name="cdcPipelineId"
                    placeholder={
                      selectedGroup?.enabled_incremental_source_count
                        ? 'Leave blank to create or reuse the group CDC pipeline'
                        : 'Not required: this group has no enabled incremental tables'
                    }
                    disabled={!selectedGroup?.enabled_incremental_source_count}
                  />
                  {!selectedGroup?.enabled_incremental_source_count && (
                    <p className="text-xs text-muted-foreground">
                      The CDC declarative pipeline task will be omitted for this ingestion group.
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
            <TabsContent value="schedule" forceMount className="mt-4 data-[state=inactive]:hidden">
              <ScheduleFields
                enabled={scheduleEnabled}
                onEnabledChange={setScheduleEnabled}
                active={scheduleActive}
                onActiveChange={setScheduleActive}
              />
            </TabsContent>
            <TabsContent value="compute" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="compute-mode">Compute mode</Label>
                  <Select value={computeMode} onValueChange={(value) => setComputeMode(value as typeof computeMode)}>
                    <SelectTrigger id="compute-mode" className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="SERVERLESS">Serverless</SelectItem>
                      <SelectItem value="JOB_CLUSTER">Shared autoscaling job cluster</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {computeMode === 'SERVERLESS' && (
                  <div className="space-y-2">
                    <Label htmlFor="performance-target">Serverless performance mode</Label>
                    <Select
                      value={performanceTarget}
                      onValueChange={(value) => setPerformanceTarget(value as typeof performanceTarget)}
                    >
                      <SelectTrigger id="performance-target" className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="STANDARD">Standard</SelectItem>
                        <SelectItem value="PERFORMANCE_OPTIMIZED">Performance optimized</SelectItem>
                      </SelectContent>
                    </Select>
                    <p className="text-xs text-muted-foreground">
                      Standard favors cost efficiency; performance optimized favors faster startup and execution.
                    </p>
                  </div>
                )}
              </div>
            </TabsContent>
            {computeMode === 'JOB_CLUSTER' && (
              <TabsContent value="cluster" forceMount className="mt-4 data-[state=inactive]:hidden">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2 md:col-span-2">
                    <Label htmlFor="spark-version">Databricks Runtime</Label>
                    <Input
                      id="spark-version"
                      name="sparkVersion"
                      defaultValue="18.x-scala2.13"
                      placeholder="18.x-scala2.13"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="driver-node-type">Driver node type</Label>
                    <Input
                      id="driver-node-type"
                      name="driverNodeTypeId"
                      placeholder="For example: i3.xlarge"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="worker-node-type">Worker node type</Label>
                    <Input
                      id="worker-node-type"
                      name="workerNodeTypeId"
                      placeholder="For example: i3.xlarge"
                      required
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="min-workers">Minimum workers</Label>
                    <Input id="min-workers" name="minWorkers" type="number" min="0" defaultValue="1" required />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="max-workers">Maximum workers</Label>
                    <Input id="max-workers" name="maxWorkers" type="number" min="1" defaultValue="4" required />
                  </div>
                  <p className="text-xs text-muted-foreground md:col-span-2">
                    The planner and ingestion workers share this autoscaling job cluster. The CDC pipeline task uses its
                    own pipeline compute.
                  </p>
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

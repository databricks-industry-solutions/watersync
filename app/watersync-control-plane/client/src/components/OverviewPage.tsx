import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyTitle,
  Skeleton,
  Badge,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import {
  CheckCircle2,
  Clock3,
  Droplets,
  Layers3,
  Settings2,
  TableProperties,
  TriangleAlert,
  Workflow,
} from 'lucide-react';
import { useReportedFailure } from '../lib/logging';
import { fqn, displayTime } from '../utils';
import { useControl } from '../lib/control';
import { PageTitle, ErrorState, Metric, Hint } from './shared';
import { FilterBar } from './FilterBar';
import { usePageFilters } from '../lib/pageFilters';

export function OverviewPage() {
  const { location, revision, refresh } = useControl();
  const filters = usePageFilters();
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      watermark_table: sql.string(fqn(location, 'jdbc_ingestion_watermark')),
      ...filters.params,
      refresh_token: sql.int(revision),
    }),
    [location, filters.params, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('config_summary', params);
  useReportedFailure('query.config_summary', error, location);
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
          <Card className="overview-hero mb-7 overflow-hidden">
            <CardContent className="flex flex-wrap items-center justify-between gap-5 p-6">
              <div>
                <div className="eyebrow">Active metadata environment</div>
                <h2 className="mt-1 text-xl font-semibold">{location.catalog}</h2>
                <div className="mt-3 flex flex-wrap gap-2">
                  <Badge variant="secondary">Schema · {location.schema}</Badge>
                  <Badge variant="outline">SQL warehouse live</Badge>
                  <Badge variant="outline">
                    {Number(row.enabled_count).toLocaleString()} sources enabled
                    {filters.filtered && ` · ${Number(row.group_count).toLocaleString()} filtered groups`}
                  </Badge>
                </div>
              </div>
              <div className="hero-health">
                <CheckCircle2 className="h-5 w-5" />
                <div className="mode-field space-y-1.5">
                  <div className="font-semibold">Control plane ready</div>
                  <div className="text-xs opacity-80">Configuration loaded successfully</div>
                </div>
              </div>
            </CardContent>
          </Card>
          <div className="mb-3 flex items-center justify-between">
            <h2 className="text-lg font-semibold">Pipeline health</h2>
            <span className="text-xs text-muted-foreground">Actual · refreshed on demand</span>
          </div>
          <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <Metric
              icon={TableProperties}
              tone="info"
              title="Configured sources"
              value={Number(row.config_count).toLocaleString()}
              note={filters.filtered ? 'rows · matching the filters' : 'rows · current location'}
            />
            <Metric
              icon={Layers3}
              tone="success"
              title="Enabled sources"
              value={Number(row.enabled_count).toLocaleString()}
              note="actual · current state"
            />
            <Metric
              icon={TriangleAlert}
              title="Failed sources"
              value={Number(row.failed_count).toLocaleString()}
              note="actual status · investigate"
              destructive={Number(row.failed_count) > 0}
            />
            <Metric
              icon={Clock3}
              title="Latest pipeline activity"
              value={displayTime(row.last_run_timestamp)}
              note={`source: ${location.catalog}.${location.schema}`}
            />
          </div>
        </>
      )}
      <Card className="mt-8 overflow-hidden">
        <CardHeader>
          <CardTitle>WaterSync workflow</CardTitle>
          <CardDescription>
            Configure ingestion sources, inspect or reset incremental state, then create and run framework jobs.
          </CardDescription>
        </CardHeader>
        <CardContent className="grid gap-0 p-0 md:grid-cols-3">
          <Hint
            number="01"
            icon={Settings2}
            title="Configuration"
            text="Maintain JDBC source mappings and enablement."
          />
          <Hint number="02" icon={Droplets} title="Watermarks" text="Correct state or force an incremental reload." />
          <Hint number="03" icon={Workflow} title="Jobs" text="Create, monitor, and trigger Lakeflow Jobs." />
        </CardContent>
      </Card>
    </>
  );
}

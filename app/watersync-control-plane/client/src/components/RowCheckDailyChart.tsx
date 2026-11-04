import {
  BarChart,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  LineChart,
  Skeleton,
} from '@databricks/appkit-ui/react';
import { DAILY_DAYS, TOP_TABLES } from '../lib/dailyRowCounts';
import type { DailyRowCounts } from '../lib/dailyRowCounts';
import { ErrorState } from './shared';

const CHART_HEIGHT = 220;
const dayCount = (days: number) => (days === 1 ? '1 day' : `${days} days`);

/** Rows ingested and target − source rows per day, for the tables the page filters select. */
export function RowCheckDailyChart({
  chart,
  picked,
  filtered,
}: {
  chart: DailyRowCounts;
  picked: number;
  filtered: boolean;
}) {
  const labels = chart.series.map((item) => item.label);
  const groups = chart.level === 'group';
  const noun = (count: number) => (groups ? (count === 1 ? 'group' : 'groups') : count === 1 ? 'table' : 'tables');
  const scope = picked
    ? `${picked} picked ${picked === 1 ? 'table' : 'tables'}`
    : chart.entityCount > TOP_TABLES
      ? `Top ${TOP_TABLES} of ${chart.entityCount.toLocaleString()} ${noun(chart.entityCount)} by rows ingested · the other ${(chart.entityCount - TOP_TABLES).toLocaleString()} in Other ${noun(2)}`
      : `${chart.entityCount.toLocaleString()} ${noun(chart.entityCount)}`;
  return (
    <Card className="mb-6">
      <CardHeader className="pb-2">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <CardTitle className="text-base">Daily row counts</CardTitle>
          {!chart.loading && chart.days > 0 && <span className="text-xs text-muted-foreground">{scope}</span>}
        </div>
        <CardDescription>
          Last {DAILY_DAYS} days (UTC), from each day&apos;s last whole-table basic check. Rows ingested is the
          target&apos;s growth since the previous checked day, net of deletes; on the first checked day, since that
          day&apos;s first check.
          {groups && ' Each group adds up its tables; pick a group or tables in the filters to see them one by one.'}
        </CardDescription>
      </CardHeader>
      <CardContent>
        {chart.loading ? (
          <div className="grid gap-6 lg:grid-cols-2">
            <Skeleton style={{ height: CHART_HEIGHT }} />
            <Skeleton style={{ height: CHART_HEIGHT }} />
          </div>
        ) : chart.error ? (
          <ErrorState message={chart.error} />
        ) : !chart.days ? (
          <p className="py-6 text-center text-sm text-muted-foreground">
            {filtered
              ? `No basic checks of the filtered tables in the last ${DAILY_DAYS} days.`
              : `No basic checks in the last ${DAILY_DAYS} days. The chart fills in as the scheduled check runs each day.`}
          </p>
        ) : (
          <div className="grid gap-6 lg:grid-cols-2">
            <section aria-label="Rows ingested per day">
              <h3 className="mb-1 text-sm font-medium">
                Rows ingested per day
                <span className="font-normal text-muted-foreground">
                  {chart.hasIngested ? ` · ${chart.totalIngested.toLocaleString()} in ${dayCount(chart.days)}` : ''}
                </span>
              </h3>
              {chart.hasIngested ? (
                <BarChart
                  data={chart.ingested}
                  xKey="day"
                  yKey={labels}
                  height={CHART_HEIGHT}
                  showLegend={labels.length > 1}
                  options={chart.ingestedOptions}
                  ariaLabel={`Rows ingested per day, by ${groups ? 'ingestion group' : 'source table'}`}
                />
              ) : (
                <p
                  className="flex items-center justify-center text-sm text-muted-foreground"
                  style={{ height: CHART_HEIGHT }}
                >
                  Appears once the basic check has run twice.
                </p>
              )}
            </section>
            <section aria-label="Target minus source rows per day">
              <h3 className="mb-1 text-sm font-medium">
                {/* A group adds up absolute differences, so tables off in opposite directions cannot cancel out. */}
                {groups ? 'Rows out of sync' : 'Target − source rows'}
                <span
                  className={
                    chart.daysWithDifference ? 'font-normal text-destructive' : 'font-normal text-muted-foreground'
                  }
                >
                  {chart.daysWithDifference
                    ? ` · differs on ${chart.daysWithDifference} of ${dayCount(chart.days)}`
                    : chart.days === 1
                      ? ' · equal on the 1 day'
                      : ` · equal on all ${chart.days} days`}
                </span>
              </h3>
              <LineChart
                data={chart.difference}
                xKey="day"
                yKey={labels}
                height={CHART_HEIGHT}
                showLegend={labels.length > 1}
                options={chart.differenceOptions}
                ariaLabel={
                  groups
                    ? 'Rows out of sync per day, by ingestion group'
                    : 'Target minus source row count per day, by source table'
                }
              />
            </section>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

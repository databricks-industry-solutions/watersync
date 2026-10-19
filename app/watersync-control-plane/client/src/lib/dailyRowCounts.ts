import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import { useAnalyticsQuery, useChartUITokens, useThemeColors } from '@databricks/appkit-ui/react';
import type { ChartUITokens, QueryRegistry } from '@databricks/appkit-ui/react';
import { useReportedFailure } from './logging';
import { pickedTablesParam, tableKey } from './pageFilters';
import type { PageFilters, PickedTable } from './pageFilters';
import type { Location } from '../types';
import { fqn } from '../utils';

type DailyRow = QueryRegistry['row_check_daily']['result'][number];

export const DAILY_DAYS = 30;
// With no table picked, the top tables by rows ingested are charted by name and the rest add up into one series.
export const TOP_TABLES = 7;

/** What one chart series is: a group with its tables added up, or one table. */
export type DailyLevel = 'group' | 'table';

const toNumber = (value: number | string | null) => (value == null || value === '' ? null : Number(value));
const isTrue = (value: boolean | string) => value === true || value === 'true';

export type Series = { key: string; label: string; color: string };

/**
 * One series per group or table the query returned, plus "Other groups" / "Other tables" last. Picked tables keep
 * the palette slot they were given when picked; otherwise series take slots in name order. Either way a series
 * keeps its color in both charts, and removing one picked table does not repaint the others.
 */
function buildSeries(
  rows: DailyRow[],
  level: DailyLevel,
  picked: PickedTable[],
  colors: string[],
  otherColor: string
): Series[] {
  const named = new Map<string, { group: string; table: string }>();
  let hasOther = false;
  for (const row of rows) {
    if (isTrue(row.is_other)) hasOther = true;
    else
      named.set(tableKey(row.ingestion_group, row.source_table_name), {
        group: row.ingestion_group,
        table: row.source_table_name,
      });
  }
  const nameCounts = new Map<string, number>();
  for (const { table } of named.values()) nameCounts.set(table, (nameCounts.get(table) ?? 0) + 1);
  const slots = new Map(picked.map((item) => [tableKey(item.group, item.table), item.slot]));
  // A group series has no table name; a table shows its group only when two groups load a table of that name.
  const label = ({ group, table }: { group: string; table: string }) =>
    level === 'group' ? group : (nameCounts.get(table) ?? 0) > 1 ? `${group} · ${table}` : table;
  const series = [...named.entries()]
    .map(([key, entry]) => ({ key, label: label(entry) }))
    .sort((a, b) => a.label.localeCompare(b.label))
    .map(({ key, label: name }, index) => ({
      key,
      label: name,
      color: colors[(slots.get(key) ?? index) % colors.length],
    }));
  if (hasOther) {
    series.push({ key: 'other', label: level === 'group' ? 'Other groups' : 'Other tables', color: otherColor });
  }
  return series;
}

/** One row per day with a column per series; null where a series has no value that day. */
function pivot(rows: DailyRow[], series: Series[], field: 'rows_ingested' | 'count_diff') {
  const byKey = new Map(series.map((item) => [item.key, item.label]));
  const byDay = new Map<string, Record<string, string | number | null>>();
  for (const row of rows) {
    const label = byKey.get(isTrue(row.is_other) ? 'other' : tableKey(row.ingestion_group, row.source_table_name));
    if (!label) continue;
    let day = byDay.get(row.day);
    if (!day) {
      // Every series gets a column, null until the series has a value that day.
      day = { day: row.day };
      for (const item of series) day[item.label] = null;
      byDay.set(row.day, day);
    }
    day[label] = toNumber(row[field]);
  }
  return [...byDay.values()].sort((a, b) => String(a.day).localeCompare(String(b.day)));
}

/**
 * ECharts options replacing the built-in series and axes, which would title-case table names
 * ("dbo.orders" -> "Dbo.Orders"), smooth lines between days and allow fractional row-count ticks.
 */
function dailyOptions(
  days: Array<Record<string, string | number | null>>,
  series: Series[],
  type: 'bar' | 'line',
  ui: ChartUITokens
) {
  return {
    xAxis: {
      type: 'time',
      minInterval: 86_400_000,
      axisLabel: { color: ui.axisLabel, formatter: '{MMM} {d}', hideOverlap: true },
      axisLine: { lineStyle: { color: ui.grid } },
      axisTick: { show: false },
    },
    yAxis: {
      type: 'value',
      minInterval: 1,
      axisLabel: { color: ui.axisLabel, formatter: (value: number) => value.toLocaleString() },
      splitLine: { lineStyle: { color: ui.grid } },
    },
    series: series.map(({ label, color }) => ({
      name: label,
      type,
      data: days.map((day) => [day.day, day[label]]),
      color,
      tooltip: { valueFormatter: (value: number | null) => (value == null ? '—' : value.toLocaleString()) },
      ...(type === 'bar'
        ? { barMaxWidth: 14, itemStyle: { borderRadius: [4, 4, 0, 0] } }
        : { smooth: false, showSymbol: true, symbolSize: 8, lineStyle: { width: 2 }, connectNulls: false }),
    })),
  };
}

export type DailyRowCounts = ReturnType<typeof useDailyRowCounts>;

/** The daily row count query for the page filters, shaped into chart series and per-day rows. */
export function useDailyRowCounts(location: Location, revision: number, filters: PageFilters, level: DailyLevel) {
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      row_check_table: sql.string(fqn(location, '_watersync_row_checks')),
      days: sql.int(DAILY_DAYS),
      ingestion_group: sql.string(filters.group),
      search: sql.string(filters.search),
      tables: sql.string(pickedTablesParam(filters.picked)),
      top: sql.int(TOP_TABLES),
      level: sql.string(level),
      refresh_token: sql.int(revision),
    }),
    [location, filters, level, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('row_check_daily', params);
  useReportedFailure('query.row_check_daily', error, {
    ...location,
    group: filters.group,
    picked: filters.picked.length,
  });

  const colors = useThemeColors('categorical');
  const ui = useChartUITokens();
  return useMemo(() => {
    const rows = data ?? [];
    // "Other tables" is not an entity, so it gets the muted axis color instead of a palette hue.
    const series = buildSeries(rows, level, filters.picked, colors, ui.axisLabel);
    const ingested = pivot(rows, series, 'rows_ingested');
    const difference = pivot(rows, series, 'count_diff');
    return {
      level,
      loading,
      error,
      colors,
      series,
      ingested,
      difference,
      ingestedOptions: dailyOptions(ingested, series, 'bar', ui),
      differenceOptions: dailyOptions(difference, series, 'line', ui),
      days: new Set(rows.map((row) => row.day)).size,
      entityCount: Math.max(0, ...rows.map((row) => Number(row.entity_count) || 0)),
      daysWithDifference: new Set(rows.filter((row) => Number(row.count_diff)).map((row) => row.day)).size,
      totalIngested: rows.reduce((sum, row) => sum + (toNumber(row.rows_ingested) ?? 0), 0),
      hasIngested: rows.some((row) => toNumber(row.rows_ingested) != null),
    };
  }, [data, level, loading, error, filters.picked, colors, ui]);
}

import { useMemo } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Skeleton,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { useReportedFailure } from '../lib/logging';
import { tableKey } from '../lib/pageFilters';
import type { PickedTable } from '../lib/pageFilters';
import type { Location } from '../types';
import { fqn } from '../utils';
import { ErrorState } from './shared';

type State = 'error' | 'mismatch' | 'match' | 'unchecked';
// Matches row_check_status_map.sql's LIMIT.
const MAP_LIMIT = 5000;
// Above this many tables the squares shrink, so the map stays a summary; past the height cap it scrolls.
const COMPACT_ABOVE = 300;

const states: Array<{ state: State; label: string }> = [
  { state: 'error', label: 'Error' },
  { state: 'mismatch', label: 'Mismatch' },
  { state: 'match', label: 'Match' },
  { state: 'unchecked', label: 'Not checked' },
];
const stateLabel = Object.fromEntries(states.map((item) => [item.state, item.label])) as Record<State, string>;

/**
 * One square per enabled table, coloured by its worst latest result (basic or detailed), grouped by ingestion
 * group. Clicking a square narrows the page to that table. Follows the group and name filters but not picked
 * tables, so every table stays visible while one is picked.
 */
export function RowCheckStatusMap({
  location,
  revision,
  group,
  search,
  picked,
  onSelect,
}: {
  location: Location;
  revision: number;
  group: string;
  search: string;
  picked: PickedTable[];
  onSelect: (group: string, table: string) => void;
}) {
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      row_check_table: sql.string(fqn(location, '_watersync_row_checks')),
      ingestion_group: sql.string(group),
      search: sql.string(search),
      refresh_token: sql.int(revision),
    }),
    [location, group, search, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('row_check_status_map', params);
  useReportedFailure('query.row_check_status_map', error, { ...location, group, search });

  const map = useMemo(() => {
    const groups = new Map<string, Array<{ table: string; state: State }>>();
    const counts: Record<State, number> = { error: 0, mismatch: 0, match: 0, unchecked: 0 };
    for (const row of data ?? []) {
      const state = (row.state as State) in counts ? (row.state as State) : 'unchecked';
      counts[state] += 1;
      const tables = groups.get(row.ingestion_group) ?? [];
      tables.push({ table: row.source_table_name, state });
      groups.set(row.ingestion_group, tables);
    }
    return { groups: [...groups.entries()], counts, total: data?.length ?? 0 };
  }, [data]);
  const pickedKeys = new Set(picked.map((item) => tableKey(item.group, item.table)));

  if (!loading && !error && !map.total) return null;
  return (
    <Card className="mb-6">
      <CardHeader className="pb-2">
        <div className="flex flex-wrap items-baseline justify-between gap-2">
          <CardTitle className="text-base">Status of every table</CardTitle>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground" aria-hidden>
            {states.map(({ state, label }) => (
              <span key={state} className="flex items-center gap-1.5">
                <span className={`status-cell status-cell-${state}`} />
                {label} <span className="tabular-nums">{map.counts[state].toLocaleString()}</span>
              </span>
            ))}
          </div>
        </div>
        <CardDescription>
          The worst latest result of each table, basic or detailed. Click a square to show only that table.
          {map.total >= MAP_LIMIT &&
            ` Showing the first ${MAP_LIMIT.toLocaleString()} tables; filter by group or name to see the rest.`}
        </CardDescription>
      </CardHeader>
      <CardContent
        className={`status-map space-y-2 ${map.total > COMPACT_ABOVE ? 'status-map-compact' : ''}`}
        // Scrollable, so keyboard users can reach the rows below the height cap.
        tabIndex={map.total > COMPACT_ABOVE ? 0 : undefined}
        aria-label={map.total > COMPACT_ABOVE ? 'Table statuses, scrollable' : undefined}
      >
        {loading ? (
          <Skeleton className="h-16" />
        ) : error ? (
          <ErrorState message={error} />
        ) : (
          map.groups.map(([groupName, tables]) => (
            <div key={groupName} className="flex items-start gap-3">
              <span className="status-map-label flex w-44 shrink-0 gap-1 text-xs font-medium" title={groupName}>
                <span className="truncate">{groupName}</span>
                <span className="shrink-0 font-normal text-muted-foreground">· {tables.length.toLocaleString()}</span>
              </span>
              <div className="flex flex-wrap gap-[3px]">
                {tables.map(({ table, state }) => {
                  const selected = pickedKeys.has(tableKey(groupName, table));
                  return (
                    <button
                      key={table}
                      type="button"
                      className={`status-cell status-cell-${state} status-cell-button ${selected ? 'status-cell-selected' : ''}`}
                      onClick={() => onSelect(groupName, table)}
                      aria-pressed={selected}
                      aria-label={`${table} in ${groupName}: ${stateLabel[state]}`}
                      title={`${table} · ${groupName}\n${stateLabel[state]}${selected ? '\nClick again to show all tables' : ''}`}
                    />
                  );
                })}
              </div>
            </div>
          ))
        )}
      </CardContent>
    </Card>
  );
}

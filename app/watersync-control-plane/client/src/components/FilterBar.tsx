import { useMemo, useState } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Button,
  Command,
  CommandEmpty,
  CommandInput,
  CommandItem,
  CommandList,
  Input,
  Popover,
  PopoverContent,
  PopoverTrigger,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { Check, ChevronsUpDown, RefreshCw, Search, X } from 'lucide-react';
import { useReportedFailure } from '../lib/logging';
import { MAX_PICKED, tableKey, useDebounced } from '../lib/pageFilters';
import type { PageFilters, PickedTable } from '../lib/pageFilters';
import type { Location } from '../types';
import { fqn } from '../utils';

const ALL_GROUPS = '__all__';
// Matches table_options.sql's LIMIT.
const OPTION_LIMIT = 50;

/** Searchable, server-side list of the location's tables; picks up to MAX_PICKED. */
function TablePicker({
  location,
  group,
  revision,
  picked,
  onToggle,
  enabledOnly,
  pickNote,
}: {
  location: Location;
  group: string;
  revision: number;
  picked: PickedTable[];
  onToggle: (group: string, table: string) => void;
  enabledOnly: boolean;
  pickNote: string;
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const debounced = useDebounced(search.trim());
  const params = useMemo(
    () => ({
      config_table: sql.string(fqn(location, 'jdbc_ingestion_config')),
      ingestion_group: sql.string(group),
      search: sql.string(debounced),
      enabled_only: sql.boolean(enabledOnly),
      refresh_token: sql.int(revision),
    }),
    [location, group, debounced, enabledOnly, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('table_options', params, { autoStart: open });
  useReportedFailure('query.table_options', error, { ...location, group });
  const pickedKeys = new Set(picked.map((item) => tableKey(item.group, item.table)));
  const full = picked.length >= MAX_PICKED;
  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          variant="outline"
          size="sm"
          role="combobox"
          aria-expanded={open}
          // A combobox does not take its name from its text, so name it explicitly.
          aria-label="Source tables"
          className="min-w-56 justify-between"
        >
          {picked.length ? `${picked.length} ${picked.length === 1 ? 'table' : 'tables'} picked` : 'All tables'}
          <ChevronsUpDown className="ml-2 h-3.5 w-3.5 opacity-60" />
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-96 p-0" align="start">
        <Command shouldFilter={false}>
          <CommandInput placeholder="Search source tables" value={search} onValueChange={setSearch} />
          <CommandList>
            {loading ? (
              <div className="p-3 text-sm text-muted-foreground">Searching…</div>
            ) : error ? (
              <div className="p-3 text-sm text-destructive">{error}</div>
            ) : (
              <CommandEmpty>{enabledOnly ? 'No enabled table matches.' : 'No table matches.'}</CommandEmpty>
            )}
            {!loading &&
              (data ?? []).map((row) => {
                const key = tableKey(row.ingestion_group, row.source_table_name);
                const selected = pickedKeys.has(key);
                return (
                  <CommandItem
                    key={key}
                    value={key}
                    disabled={!selected && full}
                    onSelect={() => onToggle(row.ingestion_group, row.source_table_name)}
                  >
                    <Check className={`mr-2 h-4 w-4 ${selected ? 'opacity-100' : 'opacity-0'}`} />
                    <span className="truncate font-mono text-xs">{row.source_table_name}</span>
                    <span className="ml-auto pl-2 text-xs text-muted-foreground">{row.ingestion_group}</span>
                  </CommandItem>
                );
              })}
          </CommandList>
          <div className="border-t px-3 py-2 text-xs text-muted-foreground">
            {full
              ? `${MAX_PICKED} tables picked, the most at once. Remove one to pick another.`
              : `Pick up to ${MAX_PICKED} tables${pickNote}${
                  (data?.length ?? 0) >= OPTION_LIMIT
                    ? `. Showing the first ${OPTION_LIMIT} matches; type to narrow.`
                    : '.'
                }`}
          </div>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

/**
 * A page's filters: an ingestion group, up to MAX_PICKED picked tables and a table-name search. Every list,
 * count and chart on the page follows them; the filtering runs on the SQL warehouse, so it scales to
 * thousands of tables.
 */
export function FilterBar({
  location,
  revision,
  filters,
  onRefresh,
  onToggle = filters.toggle,
  colors,
  enabledOnly = false,
  pickNote = '',
}: {
  location: Location;
  revision: number;
  filters: PageFilters;
  onRefresh: () => void;
  /** Picks or unpicks a table; the Row checks page passes one that keeps chart colors. */
  onToggle?: (group: string, table: string) => void;
  /** Chart colors by slot: shown as a dot on each picked table. */
  colors?: string[];
  /** Offer only enabled tables in the picker. */
  enabledOnly?: boolean;
  /** Added after "Pick up to 8 tables" in the picker, e.g. " (one chart color each)". */
  pickNote?: string;
}) {
  const groupParams = useMemo(
    () => ({ table_name: sql.string(fqn(location, 'jdbc_ingestion_config')), refresh_token: sql.int(revision) }),
    [location, revision]
  );
  const { data: groups } = useAnalyticsQuery('ingestion_groups', groupParams);
  const { group, searchInput, picked } = filters;
  return (
    <div className="mb-6 flex flex-wrap items-center gap-2 rounded-xl border bg-card px-4 py-3 shadow-sm">
      <span className="mr-1 text-sm font-medium">Filter</span>
      <Select
        value={group || ALL_GROUPS}
        onValueChange={(value) => filters.setGroup(value === ALL_GROUPS ? '' : value)}
      >
        <SelectTrigger className="h-8 w-48" aria-label="Ingestion group">
          <SelectValue />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_GROUPS}>All groups</SelectItem>
          {(groups ?? []).map((item) => (
            <SelectItem key={item.ingestion_group} value={item.ingestion_group}>
              {item.ingestion_group}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <TablePicker
        location={location}
        group={group}
        revision={revision}
        picked={picked}
        onToggle={onToggle}
        enabledOnly={enabledOnly}
        pickNote={pickNote}
      />
      <div className="relative">
        <Search className="pointer-events-none absolute left-2.5 top-2 h-4 w-4 text-muted-foreground" aria-hidden />
        <Input
          aria-label="Search table names"
          placeholder="Table name contains…"
          value={searchInput}
          onChange={(e) => filters.setSearchInput(e.target.value)}
          className="h-8 w-64 pl-8"
        />
      </div>
      {picked.map((item) => (
        <span
          key={tableKey(item.group, item.table)}
          className="flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs"
          title={`${item.table} · ${item.group}`}
        >
          {colors && (
            <span
              className="h-2 w-2 rounded-full"
              style={{ background: colors[item.slot % colors.length] }}
              aria-hidden
            />
          )}
          <span className="font-mono">{item.table}</span>
          <button
            type="button"
            onClick={() => onToggle(item.group, item.table)}
            className="text-muted-foreground hover:text-foreground"
            aria-label={`Remove ${item.table}`}
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
      {(group || searchInput || picked.length > 0) && (
        <Button variant="ghost" size="sm" onClick={filters.clear}>
          Clear filters
        </Button>
      )}
      <Button variant="outline" size="sm" className="ml-auto" onClick={onRefresh} aria-label="Refresh">
        <RefreshCw className="h-4 w-4" />
      </Button>
    </div>
  );
}

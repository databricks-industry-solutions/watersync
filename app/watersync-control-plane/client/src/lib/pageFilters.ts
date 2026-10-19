import { useCallback, useEffect, useMemo, useState } from 'react';
import { sql } from '@databricks/appkit-ui/js';

/** A table picked in a page's filter bar; slot is its fixed palette color where a chart shows it. */
export type PickedTable = { group: string; table: string; slot: number };

// One chart color per picked table on the Row checks page; the same limit applies everywhere for consistency.
export const MAX_PICKED = 8;

export const tableKey = (group: string, table: string) => `${group}\u0000${table}`;

/** The picked tables as the JSON array the queries take; empty when none is picked. */
export const pickedTablesParam = (picked: PickedTable[]) =>
  picked.length ? JSON.stringify(picked.map(({ group, table }) => ({ group, table }))) : '';

/** Value after it stopped changing for delayMs, so typing does not send a query per keystroke. */
export function useDebounced<T>(value: T, delayMs = 250) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timer);
  }, [value, delayMs]);
  return debounced;
}

/** The lowest palette slot no picked table uses. */
export const freeSlot = (current: PickedTable[]) => {
  const used = new Set(current.map((item) => item.slot));
  return [...Array(MAX_PICKED).keys()].find((index) => !used.has(index)) ?? 0;
};

export type PageFilters = ReturnType<typeof usePageFilters>;

/**
 * The filters every page offers: an ingestion group, up to MAX_PICKED picked tables and a table-name search.
 * `params` are the SQL parameters each filtered query takes (ingestion_group, search, tables), and `key`
 * changes whenever a filter does, so paging can start again from the first page.
 */
export function usePageFilters() {
  const [group, setGroupState] = useState('');
  const [searchInput, setSearchInput] = useState('');
  const [picked, setPicked] = useState<PickedTable[]>([]);
  const search = useDebounced(searchInput.trim());

  const setGroup = useCallback((next: string) => {
    setGroupState(next);
    // Tables of other groups would match nothing once the group is narrowed.
    if (next) setPicked((current) => current.filter((item) => item.group === next));
  }, []);
  /** Picks or unpicks a table; `slot` (a chart color) defaults to the lowest free one. */
  const toggle = useCallback((tableGroup: string, table: string, slot?: number) => {
    setPicked((current) => {
      const key = tableKey(tableGroup, table);
      if (current.some((item) => tableKey(item.group, item.table) === key)) {
        return current.filter((item) => tableKey(item.group, item.table) !== key);
      }
      if (current.length >= MAX_PICKED) return current;
      return [...current, { group: tableGroup, table, slot: slot ?? freeSlot(current) }];
    });
  }, []);
  /** Shows only this table; when it already is the only one picked, shows every table again. */
  const pickOnly = useCallback((tableGroup: string, table: string, slot = 0) => {
    setPicked((current) => {
      const only = current.length === 1 && tableKey(current[0].group, current[0].table) === tableKey(tableGroup, table);
      return only ? [] : [{ group: tableGroup, table, slot }];
    });
  }, []);
  const clear = useCallback(() => {
    setGroupState('');
    setSearchInput('');
    setPicked([]);
  }, []);

  return useMemo(
    () => ({
      group,
      searchInput,
      search,
      picked,
      filtered: Boolean(group || search || picked.length),
      key: JSON.stringify([group, search, pickedTablesParam(picked)]),
      params: {
        ingestion_group: sql.string(group),
        search: sql.string(search),
        tables: sql.string(pickedTablesParam(picked)),
      },
      setGroup,
      setSearchInput,
      toggle,
      pickOnly,
      clear,
    }),
    [group, searchInput, search, picked, setGroup, toggle, pickOnly, clear]
  );
}

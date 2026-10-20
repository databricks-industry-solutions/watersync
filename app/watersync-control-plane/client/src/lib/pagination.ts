import { useCallback, useState } from 'react';
import { sql } from '@databricks/appkit-ui/js';

export const PAGE_SIZES = [10, 50, 100] as const;
export type PageSize = (typeof PAGE_SIZES)[number];
const DEFAULT_PAGE_SIZE: PageSize = 50;
const pageSizeKey = 'watersync-page-size';

const storedPageSize = (): PageSize => {
  const value = Number(window.localStorage.getItem(pageSizeKey));
  return (PAGE_SIZES as readonly number[]).includes(value) ? (value as PageSize) : DEFAULT_PAGE_SIZE;
};

/** Rows shown and the last page for `total` rows; `page` is 0-based and clamped to the pages that exist. */
export function pageInfo(total: number, page: number, pageSize: number) {
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const current = Math.min(Math.max(page, 0), pageCount - 1);
  const first = total ? current * pageSize + 1 : 0;
  const last = Math.min(total, (current + 1) * pageSize);
  return { pageCount, page: current, first, last };
}

/**
 * The current page of a server-paged list. `resetKey` is the list's filters: when it changes, the list
 * starts again from the first page. The page size is shared by every list and remembered in the browser.
 */
export function usePagination(resetKey: string) {
  // The page belongs to the filters it was chosen for; other filters start on the first page.
  const [state, setState] = useState({ key: resetKey, page: 0 });
  const [pageSize, setPageSizeState] = useState<PageSize>(storedPageSize);
  const page = state.key === resetKey ? state.page : 0;
  const setPage = useCallback((next: number) => setState({ key: resetKey, page: Math.max(0, next) }), [resetKey]);
  const setPageSize = useCallback(
    (next: PageSize) => {
      window.localStorage.setItem(pageSizeKey, String(next));
      setPageSizeState(next);
      setState({ key: resetKey, page: 0 });
    },
    [resetKey]
  );
  return {
    page,
    pageSize,
    setPage,
    setPageSize,
    params: { page_size: sql.int(pageSize), page_offset: sql.int(page * pageSize) },
  };
}

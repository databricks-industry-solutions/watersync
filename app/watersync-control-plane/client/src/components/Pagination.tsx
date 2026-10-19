import { Button, Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@databricks/appkit-ui/react';
import { ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from 'lucide-react';
import { PAGE_SIZES, pageInfo } from '../lib/pagination';
import type { PageSize } from '../lib/pagination';

/** Page controls under a server-paged list: rows per page (10, 50, 100), the range shown and page buttons. */
export function Pagination({
  total,
  page,
  pageSize,
  onPageChange,
  onPageSizeChange,
  noun = 'rows',
}: {
  total: number;
  page: number;
  pageSize: PageSize;
  onPageChange: (page: number) => void;
  onPageSizeChange: (size: PageSize) => void;
  /** Plural, e.g. "tables"; the singular drops the final s. */
  noun?: string;
}) {
  const info = pageInfo(total, page, pageSize);
  const unit = total === 1 ? noun.replace(/s$/, '') : noun;
  const button = (label: string, target: number, Icon: typeof ChevronLeft, disabled: boolean) => (
    <Button
      variant="outline"
      size="sm"
      className="h-8 w-8 p-0"
      onClick={() => onPageChange(target)}
      disabled={disabled}
      aria-label={label}
      title={label}
    >
      <Icon className="h-4 w-4" />
    </Button>
  );
  return (
    <nav className="flex flex-wrap items-center justify-between gap-3 border-t px-4 py-3 text-sm" aria-label="Pages">
      <span className="text-muted-foreground tabular-nums">
        {total
          ? `${info.first.toLocaleString()}–${info.last.toLocaleString()} of ${total.toLocaleString()} ${unit}`
          : `No ${noun}`}
      </span>
      <div className="flex flex-wrap items-center gap-4">
        <label className="flex items-center gap-2 text-muted-foreground">
          Rows per page
          <Select value={String(pageSize)} onValueChange={(value) => onPageSizeChange(Number(value) as PageSize)}>
            <SelectTrigger className="h-8 w-20" aria-label="Rows per page">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PAGE_SIZES.map((size) => (
                <SelectItem key={size} value={String(size)}>
                  {size}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </label>
        <div className="flex items-center gap-1">
          {button('First page', 0, ChevronsLeft, info.page === 0)}
          {button('Previous page', info.page - 1, ChevronLeft, info.page === 0)}
          <span className="min-w-28 px-2 text-center tabular-nums">
            Page {(info.page + 1).toLocaleString()} of {info.pageCount.toLocaleString()}
          </span>
          {button('Next page', info.page + 1, ChevronRight, info.page >= info.pageCount - 1)}
          {button('Last page', info.pageCount - 1, ChevronsRight, info.page >= info.pageCount - 1)}
        </div>
      </div>
    </nav>
  );
}

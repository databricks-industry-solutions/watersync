import { useMemo, useState, useCallback } from 'react';
import type { FormEvent } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Badge,
  Button,
  Card,
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
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import { RotateCcw } from 'lucide-react';
import { api } from '../lib/api';
import { reportError, useReportedFailure } from '../lib/logging';
import type { Location, WatermarkRow } from '../types';
import { fqn, displayTime } from '../utils';
import { useControl } from '../lib/control';
import { PageTitle, ErrorState, Field } from './shared';
import { FilterBar } from './FilterBar';
import { Pagination } from './Pagination';
import { usePageFilters } from '../lib/pageFilters';
import { usePagination } from '../lib/pagination';

export function WatermarksPage() {
  const { location, revision, refresh } = useControl();
  const filters = usePageFilters();
  const pages = usePagination(filters.key);
  const [editing, setEditing] = useState<WatermarkRow | null>(null);
  const [refreshing, setRefreshing] = useState<WatermarkRow | null>(null);

  const params = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_watermark')),
      ...filters.params,
      ...pages.params,
      refresh_token: sql.int(revision),
    }),
    [location, filters.params, pages.params, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('watermark_entries', params);
  useReportedFailure('query.watermark_entries', error, { ...location, group: filters.group, search: filters.search });
  const total = Number(data?.[0]?.total_count ?? 0);
  return (
    <>
      <PageTitle
        title="Incremental watermark state"
        description="Actual persisted state; deleting a row forces the framework's next incremental read to start fresh."
      />
      <FilterBar location={location} revision={revision} filters={filters} onRefresh={refresh} />
      {loading ? (
        <Skeleton className="h-80" />
      ) : error ? (
        <ErrorState message={error} />
      ) : !data?.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>{filters.filtered ? 'No watermark row matches the filters' : 'No watermark rows'}</EmptyTitle>
            <EmptyDescription>
              {filters.filtered
                ? 'Change or clear the filters to see more tables.'
                : 'No incremental state exists at this location.'}
            </EmptyDescription>
          </EmptyHeader>
          {filters.filtered && (
            <Button variant="outline" size="sm" onClick={filters.clear}>
              Clear filters
            </Button>
          )}
        </Empty>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Group</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Last watermark</TableHead>
                <TableHead>Last run</TableHead>
                <TableHead>Status</TableHead>
                <TableHead>Error</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => (
                <TableRow key={`${row.ingestion_group}/${row.source_table_name}`}>
                  <TableCell>{row.ingestion_group}</TableCell>
                  <TableCell className="font-mono text-xs">{row.source_table_name}</TableCell>
                  <TableCell>{row.last_watermark ?? '\u2014'}</TableCell>
                  <TableCell>{displayTime(row.last_run_timestamp)}</TableCell>
                  <TableCell>
                    <Badge variant={row.status === 'FAILED' ? 'destructive' : 'secondary'}>
                      {row.status ?? 'UNKNOWN'}
                    </Badge>
                  </TableCell>
                  <TableCell className="max-w-64 truncate" title={row.last_error ?? ''}>
                    {row.last_error ?? '\u2014'}
                  </TableCell>
                  <TableCell className="text-right space-x-1">
                    <Button
                      size="sm"
                      variant="ghost"
                      className="text-orange-600 hover:text-orange-700 hover:bg-orange-50"
                      onClick={() => setRefreshing(row)}
                      title="Schedule a full refresh for this table"
                    >
                      <RotateCcw className="mr-1 h-3.5 w-3.5" />
                      Full Refresh
                    </Button>
                    <Button size="sm" variant="ghost" onClick={() => setEditing(row)}>
                      Update
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
          <Pagination
            total={total}
            page={pages.page}
            pageSize={pages.pageSize}
            onPageChange={pages.setPage}
            onPageSizeChange={pages.setPageSize}
            noun="tables"
          />
        </Card>
      )}
      <WatermarkDialog
        row={editing}
        location={location}
        onClose={() => setEditing(null)}
        onSaved={() => {
          setEditing(null);
          refresh();
        }}
      />
      <FullRefreshDialog
        row={refreshing}
        location={location}
        onClose={() => setRefreshing(null)}
        onConfirmed={() => {
          setRefreshing(null);
          refresh();
        }}
      />
    </>
  );
}

function FullRefreshDialog({
  row,
  location,
  onClose,
  onConfirmed,
}: {
  row: WatermarkRow | null;
  location: Location;
  onClose: () => void;
  onConfirmed: () => void;
}) {
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const confirm = useCallback(async () => {
    if (!row) return;
    setSubmitting(true);
    setError(null);
    try {
      await api('/api/watermark', {
        method: 'PATCH',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
          lastWatermark: row.last_watermark,
          status: 'FULL_REFRESH',
        }),
      });
      onConfirmed();
    } catch (x) {
      setError(
        reportError('watermark.full_refresh', x, {
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        })
      );
    } finally {
      setSubmitting(false);
    }
  }, [row, location, onConfirmed]);
  return (
    <Dialog open={!!row} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Confirm full refresh</DialogTitle>
          <DialogDescription>
            This will set the status of <span className="font-mono font-semibold">{row?.source_table_name}</span> (group{' '}
            <span className="font-semibold">{row?.ingestion_group}</span>) to{' '}
            <Badge variant="secondary">FULL_REFRESH</Badge>. The next pipeline run will clear and fully reload this
            table.
          </DialogDescription>
        </DialogHeader>
        {error && <ErrorState message={error} />}
        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={submitting}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={() => void confirm()} disabled={submitting}>
            {submitting ? 'Updating…' : 'Confirm full refresh'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function WatermarkDialog({
  row,
  location,
  onClose,
  onSaved,
}: {
  row: WatermarkRow | null;
  location: Location;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!row) return;
    const f = new FormData(e.currentTarget);
    try {
      await api('/api/watermark', {
        method: 'PATCH',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
          lastWatermark: f.get('watermark') || null,
          status: f.get('status'),
        }),
      });
      onSaved();
    } catch (x) {
      setError(
        reportError('watermark.update', x, {
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        })
      );
    }
  };
  return (
    <Dialog open={!!row} onOpenChange={(v) => !v && onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Update watermark</DialogTitle>
          <DialogDescription>
            {row?.ingestion_group} / {row?.source_table_name}
          </DialogDescription>
        </DialogHeader>
        <form onSubmit={(event) => void submit(event)} className="space-y-4">
          {error && <ErrorState message={error} />}
          <Field name="watermark" label="Last watermark" value={row?.last_watermark} />
          <Field name="status" label="Status" value={row?.status ?? 'SUCCESS'} />
          <DialogFooter>
            <Button type="submit">Save state</Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

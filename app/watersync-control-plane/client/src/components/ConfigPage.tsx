import { useEffect, useMemo, useState } from 'react';
import type { FormEvent } from 'react';
import { sql } from '@databricks/appkit-ui/js';
import {
  Alert,
  AlertDescription,
  AlertTitle,
  Badge,
  Button,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  Checkbox,
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
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
  Tabs,
  TabsContent,
  TabsList,
  TabsTrigger,
  useAnalyticsQuery,
} from '@databricks/appkit-ui/react';
import {
  ArrowLeft,
  ArrowRight,
  CheckCircle2,
  Database,
  Plus,
  RefreshCw,
  Trash2,
} from 'lucide-react';
import { api } from '../lib/api';
import { reportError, useReportedFailure } from '../lib/logging';
import type { ConfigRow, Location, SourceTable, SourceColumn, TableDraft, InferenceProgress } from '../types';
import {
  fqn,
  numericTypes,
  temporalTypes,
  timestampTypes,
  stringTypes,
  isType,
  safeTargetName,
  sourceTableLimit,
  matchesTableFilter,
  toBool,
} from '../utils';
import { useControl } from './Layout';
import { PageTitle, ErrorState, ControlledField, ColumnSelect, Field, Toggle } from './shared';

export function ConfigPage() {
  const { location, revision, refresh } = useControl();
  const [search, setSearch] = useState('');
  const [editing, setEditing] = useState<ConfigRow | null>(null);
  const [open, setOpen] = useState(false);
  const [discoveryOpen, setDiscoveryOpen] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const params = useMemo(
    () => ({
      table_name: sql.string(fqn(location, 'jdbc_ingestion_config')),
      search: sql.string(search),
      page_size: sql.int(100),
      page_offset: sql.int(0),
      refresh_token: sql.int(revision),
    }),
    [location, search, revision]
  );
  const { data, loading, error } = useAnalyticsQuery('config_entries', params);
  useReportedFailure('query.config_entries', error, { ...location, search });
  const remove = async (row: ConfigRow) => {
    if (!confirm(`Delete ${row.ingestion_group} / ${row.source_table_name}?`)) return;
    try {
      await api<unknown>('/api/config', {
        method: 'DELETE',
        body: JSON.stringify({
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        }),
      });
      refresh();
    } catch (e) {
      setMessage(
        reportError('config.delete', e, {
          ...location,
          ingestionGroup: row.ingestion_group,
          sourceTableName: row.source_table_name,
        })
      );
    }
  };
  return (
    <>
      <PageTitle
        title="Ingestion configuration"
        description="Exact WaterSync source mappings; first 100 filtered rows."
        action={
          <div className="flex flex-wrap gap-2">
            <Button variant="outline" onClick={() => setDiscoveryOpen(true)}>
              <Database className="mr-2 h-4 w-4" /> Discover tables
            </Button>
            <Button
              onClick={() => {
                setEditing(null);
                setOpen(true);
              }}
            >
              <Plus className="mr-2 h-4 w-4" /> Add entry
            </Button>
          </div>
        }
      />
      <div className="mb-4 flex gap-2">
        <Input
          aria-label="Filter configurations"
          placeholder="Filter group or source table"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
        />
        <Button variant="outline" onClick={refresh}>
          <RefreshCw className="h-4 w-4" />
        </Button>
      </div>
      {message && <ErrorState message={message} />}{' '}
      {loading ? (
        <Skeleton className="h-80" />
      ) : error ? (
        <ErrorState message={error} />
      ) : !data?.length ? (
        <Empty>
          <EmptyHeader>
            <EmptyTitle>No configuration entries</EmptyTitle>
            <EmptyDescription>Add a JDBC source or change the selected metadata location.</EmptyDescription>
          </EmptyHeader>
        </Empty>
      ) : (
        <Card>
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Group</TableHead>
                <TableHead>Source</TableHead>
                <TableHead>Target</TableHead>
                <TableHead>Mode</TableHead>
                <TableHead>Watermark</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {data.map((row) => (
                <TableRow key={`${row.ingestion_group}/${row.source_table_name}`}>
                  <TableCell>{row.ingestion_group}</TableCell>
                  <TableCell className="font-mono text-xs">{row.source_table_name}</TableCell>
                  <TableCell className="font-mono text-xs">{row.target_table_fqn}</TableCell>
                  <TableCell>{row.ingestion_type}</TableCell>
                  <TableCell>{row.watermark_column ?? '\u2014'}</TableCell>
                  <TableCell>
                    <Badge variant={toBool(row.enabled) ? 'default' : 'secondary'}>
                       {toBool(row.enabled) ? 'Enabled' : 'Disabled'}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        setEditing(row);
                        setOpen(true);
                      }}
                    >
                      Edit
                    </Button>
                    <Button
                      size="icon"
                      variant="ghost"
                      aria-label="Delete configuration"
                      onClick={() => void remove(row)}
                    >
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Card>
      )}
      <ConfigDialog
        open={open}
        onOpenChange={setOpen}
        row={editing}
        location={location}
        onSaved={() => {
          setOpen(false);
          refresh();
        }}
      />
      <DiscoveryDialog
        open={discoveryOpen}
        onOpenChange={setDiscoveryOpen}
        location={location}
        onSaved={() => {
          setDiscoveryOpen(false);
          refresh();
        }}
      />
    </>
  );
}

function DiscoveryDialog({
  open,
  onOpenChange,
  location,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (value: boolean) => void;
  location: Location;
  onSaved: () => void;
}) {
  const [step, setStep] = useState(1);
  const [busy, setBusy] = useState(false);
  const [inferenceProgress, setInferenceProgress] = useState<InferenceProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [group, setGroup] = useState('');
  const [databaseType, setDatabaseType] = useState('sqlserver');
  const [connectionMode, setConnectionMode] = useState('uc');
  const [connectionName, setConnectionName] = useState('');
  const [database, setDatabase] = useState('');
  const [jdbcUrl, setJdbcUrl] = useState('');
  const [jdbcUser, setJdbcUser] = useState('');
  const [secretScope, setSecretScope] = useState('');
  const [secretKey, setSecretKey] = useState('');
  const [ucSecretName, setUcSecretName] = useState('');
  const [tables, setTables] = useState<SourceTable[]>([]);
  const [tablesTruncated, setTablesTruncated] = useState(false);
  const [tableFilter, setTableFilter] = useState('');
  const [appliedTableFilter, setAppliedTableFilter] = useState<string | null>(null);
  const [searchingTables, setSearchingTables] = useState(false);
  const [selectedTables, setSelectedTables] = useState<SourceTable[]>([]);
  const [ingestionType, setIngestionType] = useState('incremental');
  const [epicCsa, setEpicCsa] = useState(false);
  const [autoCdcFromSnapshot, setAutoCdcFromSnapshot] = useState(false);
  const [drafts, setDrafts] = useState<TableDraft[]>([]);
  const [reviewPage, setReviewPage] = useState(0);
  const [confirmedPages, setConfirmedPages] = useState<number[]>([]);
  const [baseTargetFqn, setBaseTargetFqn] = useState(`${location.catalog}.${location.schema}`);
  const [baseStagingFqn, setBaseStagingFqn] = useState(`${location.catalog}.${location.schema}`);
  const [threshold, setThreshold] = useState('5');
  const [fetchSize, setFetchSize] = useState('10000');
  const [partitions, setPartitions] = useState('8');

  const reset = () => {
    setStep(1);
    setError(null);
    setTables([]);
    setTablesTruncated(false);
    setSelectedTables([]);
    setDrafts([]);
    setReviewPage(0);
    setConfirmedPages([]);
    setTableFilter('');
    setAppliedTableFilter(null);
    setSearchingTables(false);
    setBaseTargetFqn(`${location.catalog}.${location.schema}`);
    setBaseStagingFqn(`${location.catalog}.${location.schema}`);
    setInferenceProgress(null);
  };
  const changeOpen = (value: boolean) => {
    if (!value && busy) return;
    if (!value) reset();
    onOpenChange(value);
  };
  const fetchSourceTables = async (filter: string) => {
    const result = await api<{ tables: SourceTable[]; truncated?: boolean }>('/api/source-tables', {
      method: 'POST',
      body: JSON.stringify({
        connectionName: connectionMode === 'uc' ? connectionName : '',
        jdbcUrl: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUrl : '',
        jdbcUser: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUser : '',
        jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : '',
        jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : '',
        ucSecretName: connectionMode === 'uc_secret' ? ucSecretName : '',
        database,
        databaseType,
        tableFilter: filter,
      }),
    });
    setTables(result.tables);
    setTablesTruncated(Boolean(result.truncated));
    setAppliedTableFilter(filter);
  };
  const loadTables = async () => {
    setBusy(true);
    setInferenceProgress(null);
    setError(null);
    setSelectedTables([]);
    try {
      await fetchSourceTables(tableFilter);
    } catch (value) {
      setError(reportError('discovery.load_tables', value, { databaseType, connectionMode, tableFilter }));
    } finally {
      setBusy(false);
    }
  };
  useEffect(() => {
    if (busy || appliedTableFilter === null || tableFilter.trim() === appliedTableFilter.trim()) return;
    const timer = setTimeout(() => {
      setSearchingTables(true);
      setError(null);
      fetchSourceTables(tableFilter)
        .catch((value: unknown) =>
          setError(reportError('discovery.search_tables', value, { databaseType, connectionMode, tableFilter }))
        )
        .finally(() => setSearchingTables(false));
    }, 500);
    return () => clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tableFilter, appliedTableFilter, busy]);
  const inferColumns = (next: SourceColumn[]) => {
    const primary = next.filter((column) => column.is_primary_key === '1').map((column) => column.column_name);
    const idFallback = next.find((column) => /(^id$|_id$)/i.test(column.column_name) && column.is_nullable === 'NO');
    const key = primary[0] ?? idFallback?.column_name ?? next[0]?.column_name ?? 'none';
    // EPIC CSA sequences changes from the CSA table, so no timestamp watermark is ever read.
    const watermark = epicCsa
      ? undefined
      : (next.find(
          (column) =>
            isType(column, timestampTypes) && /(update|modified|change|timestamp|date)/i.test(column.column_name)
        ) ??
        next.find((column) => isType(column, timestampTypes)) ??
        next.find(
          (column) =>
            isType(column, temporalTypes) && /(update|modified|change|timestamp|date)/i.test(column.column_name)
        ) ??
        next.find((column) => isType(column, temporalTypes)));
    const partition =
      next.find((column) => column.column_name === key && isType(column, numericTypes)) ??
      next.find((column) => isType(column, numericTypes) && column.is_nullable === 'NO');
    const predicate = partition
      ? undefined
      : (next.find((column) => column.column_name === key && isType(column, stringTypes)) ??
        next.find((column) => isType(column, stringTypes) && column.is_nullable === 'NO'));
    return {
      keyColumn: primary.length ? primary.join(',') : key,
      watermarkColumn: watermark?.column_name ?? 'none',
      partitionColumn: partition?.column_name ?? 'none',
      predicateColumn: predicate?.column_name ?? 'none',
    };
  };
  const continueToSettings = async () => {
    if (!selectedTables.length) return;
    setBusy(true);
    setError(null);
    setInferenceProgress({ completed: 0, total: selectedTables.length });
    setDrafts([]);
    setReviewPage(0);
    setConfirmedPages([]);
    try {
      for (let offset = 0; offset < selectedTables.length; offset += 10) {
        const page = selectedTables.slice(offset, offset + 10);
        const result = await api<{
          tables: Array<{ sourceSchema: string; table: string; columns: SourceColumn[] }>;
        }>('/api/source-columns-batch', {
          method: 'POST',
          body: JSON.stringify({
            connectionName: connectionMode === 'uc' ? connectionName : '',
            jdbcUrl: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUrl : '',
            jdbcUser: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUser : '',
            jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : '',
            jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : '',
            ucSecretName: connectionMode === 'uc_secret' ? ucSecretName : '',
            database,
            databaseType,
            tables: page.map((table) => ({ sourceSchema: table.table_schema, table: table.table_name })),
          }),
        });
        const pageDrafts = result.tables.map((resultTable) => {
          const table = { table_schema: resultTable.sourceSchema, table_name: resultTable.table };
          const target = safeTargetName(resultTable.table);
          return {
            table,
            columns: resultTable.columns,
            ...inferColumns(resultTable.columns),
            targetFqn: `${baseTargetFqn.replace(/\.$/, '')}.${target}`,
            stagingFqn:
              ingestionType === 'incremental' || autoCdcFromSnapshot
                ? `${baseStagingFqn.replace(/\.$/, '')}.staging_${target}`
                : '',
          };
        });
        setDrafts((current) => [...current, ...pageDrafts]);
        setInferenceProgress({
          completed: Math.min(offset + page.length, selectedTables.length),
          total: selectedTables.length,
        });
        if (offset === 0) setStep(2);
      }
      setInferenceProgress(null);
    } catch (value) {
      setError(
        reportError('discovery.infer_columns', value, {
          databaseType,
          connectionMode,
          tableCount: selectedTables.length,
        })
      );
    } finally {
      setBusy(false);
    }
  };
  const save = async () => {
    if (!drafts.length) return;
    setBusy(true);
    setError(null);
    try {
      for (const draft of drafts) {
        await api('/api/config', {
          method: 'POST',
          body: JSON.stringify({
            ...location,
            ingestionGroup: group,
            sourceTableName: `${draft.table.table_schema}.${draft.table.table_name}`,
            stagingTableFqn: draft.stagingFqn || null,
            targetTableFqn: draft.targetFqn,
            ingestionType,
            keyColumns: draft.keyColumn === 'none' ? null : draft.keyColumn,
            watermarkColumn: epicCsa || draft.watermarkColumn === 'none' ? null : draft.watermarkColumn,
            partitionColumn: draft.partitionColumn === 'none' ? null : draft.partitionColumn,
            predicateColumn: draft.predicateColumn === 'none' ? null : draft.predicateColumn,
            epicCsaEnabled: epicCsa,
            autoCdcFromSnapshot,
            jdbcUrl: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUrl : null,
            jdbcUser: connectionMode === 'jdbc' || connectionMode === 'uc_secret' ? jdbcUser : null,
            jdbcSecretScope: connectionMode === 'jdbc' ? secretScope : null,
            jdbcSecretKey: connectionMode === 'jdbc' ? secretKey : null,
            ucSecretName: connectionMode === 'uc_secret' ? ucSecretName : null,
            connectionName: connectionMode === 'uc' ? connectionName : null,
            watermarkThresholdMinutes: Number(threshold),
            fetchSize: Number(fetchSize),
            numPartitions: Number(partitions),
            enabled: true,
          }),
        });
      }
      onSaved();
      reset();
    } catch (value) {
      setError(
        reportError('discovery.save_configs', value, {
          ...location,
          ingestionGroup: group,
          ingestionType,
          draftCount: drafts.length,
        })
      );
    } finally {
      setBusy(false);
    }
  };
  // Once the source search has returned for the current term the list is already narrowed; the local
  // pass only keeps the list responsive while a newly typed term is still debouncing.
  const filteredTables =
    appliedTableFilter !== null && appliedTableFilter.trim() === tableFilter.trim()
      ? tables
      : tables.filter((table) => matchesTableFilter(table, tableFilter));
  const canContinue = Boolean(group && selectedTables.length);
  const canLoadTables =
    connectionMode === 'uc'
      ? Boolean(connectionName)
      : connectionMode === 'uc_secret'
        ? Boolean(database && jdbcUrl && jdbcUser && ucSecretName)
        : Boolean(database && jdbcUrl && jdbcUser && secretScope && secretKey);
  const updateDraft = (index: number, changes: Partial<TableDraft>) => {
    setConfirmedPages((current) => current.filter((page) => page !== Math.floor(index / 10)));
    setDrafts((current) =>
      current.map((draft, draftIndex) => (draftIndex === index ? { ...draft, ...changes } : draft))
    );
  };
  const updateBaseTarget = (value: string) => {
    setBaseTargetFqn(value);
    setConfirmedPages([]);
    const base = value.replace(/\.$/, '');
    setDrafts((current) =>
      current.map((draft) => ({ ...draft, targetFqn: `${base}.${safeTargetName(draft.table.table_name)}` }))
    );
  };
  const updateBaseStaging = (value: string) => {
    setBaseStagingFqn(value);
    setConfirmedPages([]);
    const base = value.replace(/\.$/, '');
    setDrafts((current) =>
      current.map((draft) => ({
        ...draft,
        stagingFqn:
          ingestionType === 'incremental' || autoCdcFromSnapshot
            ? `${base}.staging_${safeTargetName(draft.table.table_name)}`
            : '',
      }))
    );
  };
  const draftValid = (draft: TableDraft) =>
    Boolean(
      draft.targetFqn &&
        (ingestionType === 'full'
          ? !autoCdcFromSnapshot || (draft.stagingFqn && draft.keyColumn !== 'none')
          : draft.keyColumn !== 'none' && (epicCsa || draft.watermarkColumn !== 'none'))
    );
  const reviewPageCount = Math.ceil(drafts.length / 10);
  const reviewPageStart = reviewPage * 10;
  const visibleDrafts = drafts.slice(reviewPageStart, reviewPageStart + 10);
  const reviewPageConfirmed = confirmedPages.includes(reviewPage);
  const reviewPageValid =
    Boolean(baseTargetFqn && (ingestionType === 'full' && !autoCdcFromSnapshot ? true : baseStagingFqn)) &&
    visibleDrafts.every(draftValid);
  const allPagesConfirmed =
    reviewPageCount > 0 &&
    Array.from({ length: reviewPageCount }, (_, page) => page).every((page) => confirmedPages.includes(page));
  const draftsValid =
    !inferenceProgress &&
    allPagesConfirmed &&
    Boolean(baseTargetFqn && (ingestionType === 'full' && !autoCdcFromSnapshot ? true : baseStagingFqn)) &&
    drafts.every(draftValid);
  const confirmCurrentPage = () =>
    setConfirmedPages((current) => (current.includes(reviewPage) ? current : [...current, reviewPage]));
  const renderReviewNavigation = () => (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border bg-muted/30 p-3">
      <div>
        <div className="text-sm font-medium">
          Tables {reviewPageStart + 1}{"\u2013"}{reviewPageStart + visibleDrafts.length} of {drafts.length}
        </div>
        <div className="text-xs text-muted-foreground">
          Page {reviewPage + 1} of {reviewPageCount} {"\u00b7"} up to 10 tables per confirmation
        </div>
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={reviewPage === 0}
          onClick={() => setReviewPage((page) => Math.max(0, page - 1))}
        >
          Previous
        </Button>
        <Button
          type="button"
          size="sm"
          variant={reviewPageConfirmed ? 'outline' : 'default'}
          disabled={!reviewPageValid || reviewPageConfirmed}
          onClick={confirmCurrentPage}
        >
          {reviewPageConfirmed ? (
            <>
              <CheckCircle2 className="mr-2 h-4 w-4" />
              Confirmed
            </>
          ) : (
            `Confirm ${visibleDrafts.length} tables`
          )}
        </Button>
        <Button
          type="button"
          size="sm"
          variant="outline"
          disabled={!reviewPageConfirmed || reviewPage >= reviewPageCount - 1}
          onClick={() => setReviewPage((page) => Math.min(reviewPageCount - 1, page + 1))}
        >
          Next
        </Button>
      </div>
    </div>
  );

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] overflow-y-auto sm:max-w-7xl 2xl:max-w-[90rem]">
        <DialogHeader>
          <DialogTitle>Discover and configure source tables</DialogTitle>
          <DialogDescription>
            Step {step} of 2 {"\u00b7"}{' '}
            {step === 1
              ? 'Connect, discover, and choose the replication mode.'
              : 'Review inferred columns and runtime settings.'}
          </DialogDescription>
        </DialogHeader>
        <div className="wizard-progress" aria-label={`Step ${step} of 2`}>
          <span style={{ width: `${step * 50}%` }} />
        </div>
        {error && <ErrorState message={error} />}
        {inferenceProgress && (
          <Alert>
            <RefreshCw className={`h-4 w-4 ${busy ? 'animate-spin' : ''}`} />
            <AlertTitle>Inspecting table metadata</AlertTitle>
            <AlertDescription>
              {inferenceProgress.completed} of {inferenceProgress.total} tables inspected {"\u00b7"} pages of 10, one metadata
              query per page.
            </AlertDescription>
          </Alert>
        )}
        {step === 1 ? (
          <div className="space-y-5">
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Connection and ingestion group</CardTitle>
                <CardDescription>
                  Use an existing UC connection, a direct JDBC URL with a Databricks secret, or a JDBC URL with a
                  UC secret for authentication.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                <ControlledField
                  id="discover-group"
                  label="Ingestion group"
                  value={group}
                  onChange={setGroup}
                  required
                />
                <div className="space-y-1.5">
                  <Label htmlFor="connection-mode">Connection method</Label>
                  <Select value={connectionMode} onValueChange={setConnectionMode}>
                    <SelectTrigger id="connection-mode">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="uc">Existing UC connection</SelectItem>
                      <SelectItem value="jdbc">Direct JDBC URL</SelectItem>
                      <SelectItem value="uc_secret">UC Secret (JDBC)</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="database-type">Database type</Label>
                  <Select value={databaseType} onValueChange={setDatabaseType}>
                    <SelectTrigger id="database-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="sqlserver">SQL Server</SelectItem>
                      <SelectItem value="postgresql">PostgreSQL</SelectItem>
                      <SelectItem value="mysql">MySQL</SelectItem>
                      <SelectItem value="oracle">Oracle</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {connectionMode === 'uc' ? (
                  <>
                    <div className="md:col-span-2 xl:col-span-2">
                      <ControlledField
                        id="connection-name"
                        label="UC connection name"
                        value={connectionName}
                        onChange={setConnectionName}
                        required
                      />
                    </div>
                    <p className="self-end pb-2 text-xs text-muted-foreground md:col-span-2 xl:col-span-1">
                      The source database is defined by the UC connection and cannot be overridden here.
                    </p>
                  </>
                ) : connectionMode === 'uc_secret' ? (
                  <>
                    <ControlledField
                      id="source-database"
                      label={databaseType === 'oracle' ? 'Oracle service name' : 'Database'}
                      value={database}
                      onChange={setDatabase}
                      required
                    />
                    <ControlledField
                      id="discover-jdbc-user"
                      label="JDBC user"
                      value={jdbcUser}
                      onChange={setJdbcUser}
                      required
                    />
                    <ControlledField
                      id="discover-uc-secret-name"
                      label="UC secret name"
                      value={ucSecretName}
                      onChange={setUcSecretName}
                      hint="Format: catalog.schema.secret_name"
                      required
                    />
                    <div className="md:col-span-2 xl:col-span-2">
                      <ControlledField
                        id="discover-jdbc-url"
                        label="JDBC URL"
                        value={jdbcUrl}
                        onChange={setJdbcUrl}
                        required
                      />
                    </div>
                    <Alert className="md:col-span-2 xl:col-span-3">
                      <AlertTitle>UC secret-backed authentication</AlertTitle>
                      <AlertDescription>
                        Provide the UC secret name as catalog.schema.secret_name. Do not include a password in the
                        JDBC URL. Discovery creates and removes a temporary UC connection using this UC secret
                        reference.
                      </AlertDescription>
                    </Alert>
                  </>
                ) : (
                  <>
                    <ControlledField
                      id="source-database"
                      label={databaseType === 'oracle' ? 'Oracle service name' : 'Database'}
                      value={database}
                      onChange={setDatabase}
                      required
                    />
                    <ControlledField
                      id="discover-jdbc-user"
                      label="JDBC user"
                      value={jdbcUser}
                      onChange={setJdbcUser}
                      required
                    />
                    <ControlledField
                      id="discover-secret-scope"
                      label="Password secret scope"
                      value={secretScope}
                      onChange={setSecretScope}
                      required
                    />
                    <ControlledField
                      id="discover-secret-key"
                      label="Password secret key"
                      value={secretKey}
                      onChange={setSecretKey}
                      required
                    />
                    <div className="md:col-span-2 xl:col-span-2">
                      <ControlledField
                        id="discover-jdbc-url"
                        label="JDBC URL"
                        value={jdbcUrl}
                        onChange={setJdbcUrl}
                        required
                      />
                    </div>
                    <Alert className="md:col-span-2 xl:col-span-3">
                      <AlertTitle>Secret-backed authentication</AlertTitle>
                      <AlertDescription>
                        Do not include a password in the JDBC URL. Discovery creates and removes a temporary UC
                        connection using this secret reference.
                      </AlertDescription>
                    </Alert>
                  </>
                )}
                <div className="flex justify-end border-t pt-4 md:col-span-2 xl:col-span-3">
                  <Button type="button" onClick={() => void loadTables()} disabled={busy || !canLoadTables}>
                    {busy ? 'Loading tables\u2026' : 'Load available tables'}
                  </Button>
                </div>
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Available tables</CardTitle>
                <CardDescription>
                  Select every table you want to replicate. The search runs against the source database, so tables
                  outside the first {sourceTableLimit.toLocaleString()} results are still reachable.
                </CardDescription>
              </CardHeader>
              <CardContent>
                {busy && !tables.length ? (
                  <Skeleton className="h-56" />
                ) : appliedTableFilter === null ? (
                  <Empty>
                    <EmptyHeader>
                      <EmptyTitle>No tables loaded</EmptyTitle>
                      <EmptyDescription>
                        Configure a source connection, then load its available tables.
                      </EmptyDescription>
                    </EmptyHeader>
                  </Empty>
                ) : (
                  <>
                    <div className="mb-4 flex flex-wrap items-center gap-3">
                      <Input
                        aria-label="Filter available tables"
                        placeholder="Search schema or table, for example dbo.customer"
                        value={tableFilter}
                        onChange={(event) => setTableFilter(event.target.value)}
                        className="min-w-64 flex-1"
                      />
                      <Badge variant="secondary">{selectedTables.length} selected</Badge>
                      <div className="flex gap-1">
                        <Button
                          type="button"
                          size="sm"
                          variant="ghost"
                          onClick={() =>
                            setSelectedTables((current) => {
                              const keyed = new Map(
                                current.map((table) => [`${table.table_schema}.${table.table_name}`, table])
                              );
                              for (const table of filteredTables) {
                                keyed.set(`${table.table_schema}.${table.table_name}`, table);
                              }
                              return [...keyed.values()];
                            })
                          }
                        >
                          Select filtered
                        </Button>
                        <Button type="button" size="sm" variant="ghost" onClick={() => setSelectedTables([])}>
                          Clear
                        </Button>
                      </div>
                    </div>
                    {searchingTables ? (
                      <Skeleton className="h-56" />
                    ) : !filteredTables.length ? (
                      <Empty>
                        <EmptyHeader>
                          <EmptyTitle>No matching tables</EmptyTitle>
                          <EmptyDescription>
                            {tableFilter.trim()
                              ? `No source table matches "${tableFilter.trim()}". Try a shorter search term.`
                              : 'The source connection returned no tables.'}
                          </EmptyDescription>
                        </EmptyHeader>
                      </Empty>
                    ) : (
                      <div className="source-table-list">
                        {filteredTables.map((table) => {
                          const selected = selectedTables.some(
                            (selectedTable) =>
                              selectedTable.table_schema === table.table_schema &&
                              selectedTable.table_name === table.table_name
                          );
                          return (
                            <button
                              type="button"
                              key={`${table.table_schema}.${table.table_name}`}
                              className={`source-table-option ${selected ? 'source-table-option-selected' : ''}`}
                              onClick={() =>
                                setSelectedTables((current) =>
                                  selected
                                    ? current.filter(
                                        (selectedTable) =>
                                          selectedTable.table_schema !== table.table_schema ||
                                          selectedTable.table_name !== table.table_name
                                      )
                                    : [...current, table]
                                )
                              }
                            >
                              <Checkbox
                                checked={selected}
                                aria-label={`Select ${table.table_schema}.${table.table_name}`}
                              />
                              <span className="min-w-0 truncate">
                                {table.table_schema}.<strong>{table.table_name}</strong>
                              </span>
                              {selected && <CheckCircle2 className="ml-auto h-4 w-4" />}
                            </button>
                          );
                        })}
                      </div>
                    )}
                    {tablesTruncated && (
                      <p className="mt-3 text-xs text-muted-foreground">
                        Showing the first {sourceTableLimit.toLocaleString()} matches. Refine the search to reach tables
                        beyond this limit.
                      </p>
                    )}
                  </>
                )}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Replication mode</CardTitle>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-2">
                <div className="mode-field space-y-1.5">
                  <Label htmlFor="discover-type">Load type</Label>
                  <Select
                    value={ingestionType}
                    onValueChange={(value) => {
                      setIngestionType(value);
                      if (value === 'full') setEpicCsa(false);
                      else setAutoCdcFromSnapshot(false);
                    }}
                  >
                    <SelectTrigger id="discover-type">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="full">Initial / full load</SelectItem>
                      <SelectItem value="incremental">Incremental</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <label className={`mode-toggle ${ingestionType === 'full' ? 'opacity-50' : ''}`}>
                  <Checkbox
                    checked={epicCsa}
                    disabled={ingestionType === 'full'}
                    onCheckedChange={(checked) => setEpicCsa(checked === true)}
                  />
                  <span>
                    <strong>Use EPIC CSA</strong>
                    <small>Uses change-sequence tracking instead of a timestamp watermark.</small>
                  </span>
                </label>
                {ingestionType === 'full' && (
                  <label className="mode-toggle md:col-span-2">
                    <Checkbox
                      checked={autoCdcFromSnapshot}
                      onCheckedChange={(checked) => setAutoCdcFromSnapshot(checked === true)}
                    />
                    <span>
                      <strong>Auto CDC from snapshots</strong>
                      <small>
                        Compare each full snapshot with the previous version and maintain SCD Type 2 history.
                      </small>
                    </span>
                  </label>
                )}
              </CardContent>
            </Card>
          </div>
        ) : (
          <div className="space-y-5">
            <Alert>
              <CheckCircle2 className="h-4 w-4" />
              <AlertTitle>Metadata inferred</AlertTitle>
              <AlertDescription>
                Primary-key constraints are preferred. Review every suggestion before saving.
              </AlertDescription>
            </Alert>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Review table configurations</CardTitle>
                <CardDescription>
                  Confirm each page before continuing. Editing a confirmed table requires that page to be confirmed
                  again.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid gap-4 rounded-lg border bg-muted/30 p-4 md:grid-cols-2">
                  <ControlledField
                    id="base-target-fqn"
                    label="Base target FQN"
                    value={baseTargetFqn}
                    onChange={updateBaseTarget}
                    required
                    disabled={Boolean(inferenceProgress)}
                  />
                  <ControlledField
                    id="base-staging-fqn"
                    label="Base staging FQN"
                    value={baseStagingFqn}
                    onChange={updateBaseStaging}
                    required={ingestionType === 'incremental'}
                    disabled={Boolean(inferenceProgress)}
                  />
                  <p className="text-xs text-muted-foreground md:col-span-2">
                    WaterSync appends each normalized table name. Staging tables also receive the staging_ prefix.
                  </p>
                </div>
                {renderReviewNavigation()}
                {visibleDrafts.map((draft, pageIndex) => {
                  const index = reviewPageStart + pageIndex;
                  return (
                    <div
                      className="rounded-lg border p-4"
                      key={`${draft.table.table_schema}.${draft.table.table_name}`}
                    >
                      <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
                        <div className="font-mono text-sm font-semibold">
                          {draft.table.table_schema}.{draft.table.table_name}
                        </div>
                        <Badge variant="outline">{draft.columns.length} columns</Badge>
                      </div>
                      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                        <ColumnSelect
                          id={`key-column-${index}`}
                          label="Key column(s)"
                          value={draft.keyColumn}
                          columns={draft.columns}
                          onChange={(value) => updateDraft(index, { keyColumn: value })}
                          allowCombined
                        />
                        <ColumnSelect
                          id={`watermark-column-${index}`}
                          label="Watermark column"
                          value={draft.watermarkColumn}
                          columns={draft.columns}
                          onChange={(value) => updateDraft(index, { watermarkColumn: value })}
                          disabled={ingestionType === 'full' || epicCsa}
                        />
                        <ColumnSelect
                          id={`partition-column-${index}`}
                          label="Numeric partition column"
                          value={draft.partitionColumn}
                          columns={draft.columns.filter((column) => isType(column, numericTypes))}
                          onChange={(value) =>
                            updateDraft(index, {
                              partitionColumn: value,
                              ...(value !== 'none' ? { predicateColumn: 'none' } : {}),
                            })
                          }
                        />
                        <ColumnSelect
                          id={`predicate-column-${index}`}
                          label="String predicate column"
                          value={draft.predicateColumn}
                          columns={draft.columns.filter((column) => isType(column, stringTypes))}
                          onChange={(value) =>
                            updateDraft(index, {
                              predicateColumn: value,
                              ...(value !== 'none' ? { partitionColumn: 'none' } : {}),
                            })
                          }
                        />
                        <ControlledField
                          id={`target-fqn-${index}`}
                          label="Final target FQN"
                          value={draft.targetFqn}
                          onChange={(value) => updateDraft(index, { targetFqn: value })}
                          required
                        />
                        <ControlledField
                          id={`staging-fqn-${index}`}
                          label="Staging table FQN"
                          value={draft.stagingFqn}
                          onChange={(value) => updateDraft(index, { stagingFqn: value })}
                        />
                      </div>
                    </div>
                  );
                })}
                {renderReviewNavigation()}
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle className="text-base">Optional runtime settings</CardTitle>
              </CardHeader>
              <CardContent className="grid gap-4 md:grid-cols-3">
                <ControlledField
                  id="discover-threshold"
                  label="Watermark delay (minutes)"
                  value={threshold}
                  onChange={setThreshold}
                  type="number"
                />
                <ControlledField
                  id="discover-fetch-size"
                  label="JDBC fetch size"
                  value={fetchSize}
                  onChange={setFetchSize}
                  type="number"
                />
                <ControlledField
                  id="discover-partitions"
                  label="JDBC partitions"
                  value={partitions}
                  onChange={setPartitions}
                  type="number"
                />
              </CardContent>
            </Card>
          </div>
        )}
        <DialogFooter className="gap-2">
          <Button
            type="button"
            variant="outline"
            disabled={busy}
            onClick={() => (step === 1 ? changeOpen(false) : setStep(1))}
          >
            <ArrowLeft className="mr-2 h-4 w-4" />
            {step === 1 ? 'Cancel' : 'Back'}
          </Button>
          {step === 1 ? (
            <Button type="button" onClick={() => void continueToSettings()} disabled={busy || !canContinue}>
              {busy && inferenceProgress
                ? `Inspecting ${inferenceProgress.completed}/${inferenceProgress.total}`
                : 'Review inferred settings'}
              <ArrowRight className="ml-2 h-4 w-4" />
            </Button>
          ) : (
            <Button type="button" onClick={() => void save()} disabled={busy || !draftsValid}>
              {inferenceProgress
                ? `Waiting for ${inferenceProgress.total - inferenceProgress.completed} table${inferenceProgress.total - inferenceProgress.completed === 1 ? '' : 's'}`
                : !allPagesConfirmed
                  ? `Confirm ${reviewPageCount - confirmedPages.length} page${reviewPageCount - confirmedPages.length === 1 ? '' : 's'}`
                  : busy
                    ? 'Saving\u2026'
                    : `Save ${drafts.length} configuration${drafts.length === 1 ? '' : 's'}`}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function ConfigDialog({
  open,
  onOpenChange,
  row,
  location,
  onSaved,
}: {
  open: boolean;
  onOpenChange: (v: boolean) => void;
  row: ConfigRow | null;
  location: Location;
  onSaved: () => void;
}) {
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [ingestionType, setIngestionType] = useState(row?.ingestion_type ?? 'incremental');
  const [epicCsa, setEpicCsa] = useState(toBool(row?.epic_csa_enabled));
  useEffect(() => setIngestionType(row?.ingestion_type ?? 'incremental'), [row, open]);
  useEffect(() => setEpicCsa(toBool(row?.epic_csa_enabled)), [row, open]);
  const csaMode = ingestionType === 'incremental' && epicCsa;
  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    const f = new FormData(e.currentTarget);
    try {
      await api('/api/config', {
        method: 'POST',
        body: JSON.stringify({
          ...location,
          originalIngestionGroup: row?.ingestion_group,
          originalSourceTableName: row?.source_table_name,
          ingestionGroup: f.get('group'),
          sourceTableName: f.get('source'),
          stagingTableFqn: f.get('staging_fqn') || null,
          targetTableFqn: f.get('target_fqn'),
          ingestionType: f.get('type'),
          keyColumns: f.get('keys') || null,
          watermarkColumn: csaMode ? null : f.get('watermark') || null,
          partitionColumn: f.get('partition') || null,
          predicateColumn: f.get('predicate') || null,
          epicCsaEnabled: csaMode,
          autoCdcFromSnapshot: f.get('auto_cdc_from_snapshot') === 'on',
          jdbcUrl: f.get('jdbc_url') || null,
          jdbcUser: f.get('jdbc_user') || null,
          jdbcSecretScope: f.get('jdbc_secret_scope') || null,
          jdbcSecretKey: f.get('jdbc_secret_key') || null,
          ucSecretName: f.get('uc_secret_name') || null,
          connectionName: f.get('connection_name') || null,
          watermarkThresholdMinutes: Number(f.get('watermark_threshold_minutes') || 5),
          fetchSize: Number(f.get('fetch_size') || 10000),
          numPartitions: Number(f.get('num_partitions') || 8),
          enabled: f.get('enabled') === 'on',
        }),
      });
      onSaved();
    } catch (x) {
      setError(
        reportError('config.save', x, {
          ...location,
          ingestionGroup: String(f.get('group') ?? ''),
          sourceTableName: String(f.get('source') ?? ''),
          ingestionType,
        })
      );
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[calc(100vh-2rem)] w-[calc(100vw-2rem)] max-w-4xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{row ? 'Edit' : 'Add'} configuration</DialogTitle>
          <DialogDescription>Values map directly to jdbc_ingestion_config.</DialogDescription>
        </DialogHeader>
        <form
          key={`${open}:${row?.ingestion_group ?? 'new'}:${row?.source_table_name ?? 'new'}`}
          onSubmit={(event) => void submit(event)}
          className="space-y-4"
        >
          {error && <ErrorState message={error} />}
          <Tabs defaultValue="mapping" className="w-full">
            <TabsList className="grid w-full grid-cols-3">
              <TabsTrigger value="mapping">Source mapping</TabsTrigger>
              <TabsTrigger value="incremental">Incremental settings</TabsTrigger>
              <TabsTrigger value="connection">Connection &amp; runtime</TabsTrigger>
            </TabsList>
            <TabsContent value="mapping" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <Field name="group" label="Ingestion group" value={row?.ingestion_group} />
                <Field name="source" label="Source table" value={row?.source_table_name} />
                <Field name="target_fqn" label="Final target FQN" value={row?.target_table_fqn} required />
                <Field name="staging_fqn" label="Staging table FQN" value={row?.staging_table_fqn} />
                <div>
                  <Label htmlFor="type">Ingestion type</Label>
                  <Select
                    name="type"
                    value={ingestionType}
                    onValueChange={(value) => {
                      setIngestionType(value);
                      if (value === 'full') setEpicCsa(false);
                    }}
                  >
                    <SelectTrigger id="type" className="w-full">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="incremental">Incremental</SelectItem>
                      <SelectItem value="full">Full</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Toggle name="enabled" label="Enabled" checked={row ? toBool(row.enabled) : true} />
                {ingestionType === 'full' && (
                  <Toggle
                    name="auto_cdc_from_snapshot"
                    label="Auto CDC from snapshots"
                    checked={toBool(row?.auto_cdc_from_snapshot)}
                  />
                )}
              </div>
            </TabsContent>
            <TabsContent value="incremental" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <Field name="keys" label="Key columns" value={row?.key_columns} />
                <Field
                  key={`watermark-${csaMode}`}
                  name="watermark"
                  label="Watermark column"
                  value={csaMode ? '' : row?.watermark_column}
                  disabled={csaMode}
                  hint={csaMode ? 'EPIC CSA tracks changes by sequence, so no watermark column is used.' : undefined}
                />
                <Field name="partition" label="Partition column" value={row?.partition_column} />
                <Field name="predicate" label="Predicate column" value={row?.predicate_column} />
                <Field
                  name="watermark_threshold_minutes"
                  label="Watermark delay (minutes)"
                  value={String(row?.watermark_threshold_minutes ?? 5)}
                />
                <Toggle
                  name="epic"
                  label="EPIC CSA mode"
                  checked={csaMode}
                  onCheckedChange={setEpicCsa}
                  disabled={ingestionType === 'full'}
                />
              </div>
            </TabsContent>
            <TabsContent value="connection" forceMount className="mt-4 data-[state=inactive]:hidden">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="md:col-span-2">
                  <Field name="jdbc_url" label="JDBC URL" value={row?.jdbc_url} />
                </div>
                <Field name="jdbc_user" label="JDBC user" value={row?.jdbc_user} />
                <Field name="connection_name" label="UC connection name" value={row?.connection_name} />
                <Field name="jdbc_secret_scope" label="JDBC secret scope" value={row?.jdbc_secret_scope} />
                <Field name="jdbc_secret_key" label="JDBC secret key" value={row?.jdbc_secret_key} />
                <Field name="uc_secret_name" label="UC secret name" value={row?.uc_secret_name} hint="Format: catalog.schema.secret_name" />
                <Field name="fetch_size" label="JDBC fetch size" value={String(row?.fetch_size ?? 10000)} />
                <Field name="num_partitions" label="JDBC partitions" value={String(row?.num_partitions ?? 8)} />
              </div>
            </TabsContent>
          </Tabs>
          <DialogFooter>
            <Button type="submit" disabled={busy}>
              {busy ? 'Saving\u2026' : 'Save configuration'}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}

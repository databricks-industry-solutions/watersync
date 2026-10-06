import type { Location, JobRun, SourceColumn, SourceTable } from './types';

export const formString = (form: FormData, name: string, fallback: string) => {
  const value = form.get(name);
  return typeof value === 'string' && value ? value : fallback;
};

const locationCookie = 'watersync-location';
const setCookie = (name: string, value: string, days = 365) => {
  const expires = new Date(Date.now() + days * 86_400_000).toUTCString();
  document.cookie = `${name}=${encodeURIComponent(value)}; expires=${expires}; path=/; SameSite=Strict`;
};
const getCookie = (name: string): string | null => {
  const match = document.cookie.match(new RegExp(`(?:^|; )${name}=([^;]*)`));
  return match ? decodeURIComponent(match[1]) : null;
};
export const readLocation = (): Location | null => {
  const raw = getCookie(locationCookie);
  if (!raw) return null;
  try {
    const value: unknown = JSON.parse(raw);
    if (
      typeof value === 'object' &&
      value !== null &&
      'catalog' in value &&
      'schema' in value &&
      typeof (value as Location).catalog === 'string' &&
      typeof (value as Location).schema === 'string'
    ) {
      return { catalog: (value as Location).catalog, schema: (value as Location).schema };
    }
  } catch {
    // invalid cookie, ignore
  }
  return null;
};
export const writeLocation = (location: Location) => {
  setCookie(locationCookie, JSON.stringify(location));
};

export const fqn = (location: Location, table: string) => `${location.catalog}.${location.schema}.${table}`;

export const displayTime = (value: string | number | null | undefined) =>
  value ? new Date(typeof value === 'number' ? value : value).toLocaleString() : 'Never';

export const runStatus = (run?: JobRun) => run?.state?.result_state ?? run?.state?.life_cycle_state ?? 'NEVER_RUN';

export const statusVariant = (status: string): 'default' | 'secondary' | 'destructive' | 'outline' => {
  if (status === 'SUCCESS') return 'default';
  if (['FAILED', 'TIMEDOUT', 'CANCELED', 'INTERNAL_ERROR'].includes(status)) return 'destructive';
  if (['RUNNING', 'PENDING', 'QUEUED', 'TERMINATING'].includes(status)) return 'secondary';
  return 'outline';
};

export const runDuration = (run: JobRun) => {
  const duration =
    run.end_time && run.start_time
      ? run.end_time - run.start_time
      : (run.setup_duration ?? 0) + (run.execution_duration ?? 0) + (run.cleanup_duration ?? 0);
  if (!duration) return 'Duration unavailable';
  if (duration < 60_000) return `${Math.max(1, Math.round(duration / 1000))}s`;
  return `${Math.floor(duration / 60_000)}m ${Math.round((duration % 60_000) / 1000)}s`;
};

export const numericTypes = [
  'smallint',
  'integer',
  'int',
  'bigint',
  'decimal',
  'numeric',
  'number',
  'real',
  'float',
  'double',
];
export const temporalTypes = [
  'date',
  'datetime',
  'datetime2',
  'timestamp',
  'timestamp without time zone',
  'timestamp with time zone',
];
export const timestampTypes = ['datetime', 'datetime2', 'timestamp'];
export const stringTypes = ['char', 'varchar', 'nvarchar', 'text', 'string'];
export const isType = (column: SourceColumn, types: string[]) =>
  types.some((type) => column.data_type.toLowerCase().includes(type));
export const safeTargetName = (value: string) => value.replace(/[^A-Za-z0-9_]/g, '_').toLowerCase();
export const toBool = (value: unknown): boolean =>
  typeof value === 'string' ? value === 'true' : Boolean(value);
export const sourceTableLimit = 1000;
// Mirrors the server predicate: "schema.table" narrows both sides, anything else matches either.
export const matchesTableFilter = (table: SourceTable, filter: string) => {
  const value = filter.trim().toLowerCase();
  if (!value) return true;
  const schema = table.table_schema.toLowerCase();
  const name = table.table_name.toLowerCase();
  const separator = value.lastIndexOf('.');
  if (separator === -1) return schema.includes(value) || name.includes(value);
  const schemaPart = value.slice(0, separator).trim();
  const tablePart = value.slice(separator + 1).trim();
  return (!schemaPart || schema.includes(schemaPart)) && (!tablePart || name.includes(tablePart));
};

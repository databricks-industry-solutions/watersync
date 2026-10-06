import { WorkspaceClient, sql as dbsql } from '@databricks/sdk-experimental';
import { logger, serializeError } from './logging.js';
import { runMigrations, type MigrationContext } from './migrations/index.js';

export const workspace = new WorkspaceClient({});

export const requiredEnv = (name: string) => {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
};

export const workspaceBaseUrl = () => {
  const host = requiredEnv('DATABRICKS_HOST').replace(/\/$/, '');
  return /^https?:\/\//i.test(host) ? host : `https://${host}`;
};

export const warehouseId = requiredEnv('DATABRICKS_WAREHOUSE_ID');

export const tableName = (catalog: string, schema: string, table: string) =>
  `\`${catalog}\`.\`${schema}\`.\`${table}\``;

export const parameter = (
  name: string,
  value: string | null | undefined,
  type = 'STRING'
): dbsql.StatementParameterListItem => ({
  name,
  type,
  ...(value == null ? {} : { value }),
});

export const statementSummary = (statement: string) => statement.replace(/\s+/g, ' ').trim().slice(0, 500);

export async function execute(statement: string, parameters: dbsql.StatementParameterListItem[] = []) {
  const startedAt = Date.now();
  let response: dbsql.StatementResponse;
  try {
    response = await workspace.statementExecution.executeStatement({
      warehouse_id: warehouseId,
      statement,
      parameters,
      wait_timeout: '50s',
      on_wait_timeout: 'CANCEL',
    });
  } catch (error) {
    logger.error('sql.execute_rejected', {
      statement: statementSummary(statement),
      parameterNames: parameters.map((item) => item.name),
      durationMs: Date.now() - startedAt,
      error: serializeError(error),
    });
    throw error;
  }
  if (response.status?.state !== 'SUCCEEDED') {
    logger.error('sql.statement_failed', {
      statement: statementSummary(statement),
      parameterNames: parameters.map((item) => item.name),
      statementId: response.statement_id,
      state: response.status?.state,
      errorCode: response.status?.error?.error_code,
      message: response.status?.error?.message,
      durationMs: Date.now() - startedAt,
    });
    throw new Error(
      response.status?.error?.message ?? `SQL statement ended in ${response.status?.state ?? 'unknown state'}`
    );
  }
  logger.debug('sql.statement_succeeded', {
    statement: statementSummary(statement),
    statementId: response.statement_id,
    durationMs: Date.now() - startedAt,
  });
  return response;
}

export const sqlLiteral = (value: string) => `'${value.replace(/'/g, "''")}'`;

export const responseRows = (response: Awaited<ReturnType<typeof execute>>) => {
  const names = response.manifest?.schema?.columns?.map((column) => column.name ?? '') ?? [];
  return (response.result?.data_array ?? []).map((values) =>
    Object.fromEntries(names.map((name, index) => [name, values[index] ?? null]))
  );
};

// --- Database migrations ---
const migrationContext: MigrationContext = {
  execute,
  tableName,
  logger,
};

const migratedLocations = new Set<string>();

/** Runs pending migrations for a catalog.schema the first time it is accessed. */
export async function ensureMigrations(catalog: string, schema: string) {
  const key = `${catalog}.${schema}`;
  if (migratedLocations.has(key)) return;
  await runMigrations(migrationContext, catalog, schema);
  migratedLocations.add(key);
}

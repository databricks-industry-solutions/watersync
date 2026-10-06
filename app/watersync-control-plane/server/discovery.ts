import { randomUUID } from 'node:crypto';
import type { SourceDiscovery, SourceDatabaseType } from './schemas.js';
import { execute, parameter, sqlLiteral, workspace, workspaceBaseUrl } from './sql.js';
import { logger, serializeError } from './logging.js';

export const remoteQuery = async (
  connectionName: string,
  database: string | undefined,
  query: string,
  databaseType: SourceDatabaseType
) => {
  const parameters = [parameter('connection_name', connectionName), parameter('remote_sql', query)];
  if (!database) {
    return execute(`SELECT * FROM remote_query(:connection_name, query => :remote_sql)`, parameters);
  }
  const databaseOption = databaseType === 'oracle' ? 'service_name' : 'database';
  return execute(`SELECT * FROM remote_query(:connection_name, ${databaseOption} => :database, query => :remote_sql)`, [
    ...parameters,
    parameter('database', database),
  ]);
};

export const jdbcEndpoint = (databaseType: SourceDiscovery['databaseType'], jdbcUrl: string) => {
  const expectedPrefix = `jdbc:${databaseType === 'sqlserver' ? 'sqlserver' : databaseType}:`;
  if (!jdbcUrl.toLowerCase().startsWith(expectedPrefix)) {
    throw new Error(`JDBC URL must start with ${expectedPrefix}`);
  }
  if (databaseType === 'sqlserver') {
    const match = /^jdbc:sqlserver:\/\/([^:;]+)(?::(\d+))?(?:;.*)?$/i.exec(jdbcUrl);
    if (!match) throw new Error('Use a SQL Server JDBC URL such as jdbc:sqlserver://host:1433;databaseName=mydb');
    return { host: match[1], port: match[2] ?? '1433' };
  }
  if (databaseType === 'oracle') {
    const serviceMatch = /^jdbc:oracle:thin:@\/\/([^:/]+)(?::(\d+))?\/[^/?;]+(?:[?;].*)?$/i.exec(jdbcUrl);
    const sidMatch = /^jdbc:oracle:thin:@([^:/]+)(?::(\d+))?:[^:;/?]+(?:[?;].*)?$/i.exec(jdbcUrl);
    const match = serviceMatch ?? sidMatch;
    if (!match) {
      throw new Error('Use an Oracle JDBC URL such as jdbc:oracle:thin:@//host:1521/service_name');
    }
    return { host: match[1], port: match[2] ?? '1521' };
  }
  const match = /^jdbc:(?:postgresql|mysql):\/\/([^/:?;]+)(?::(\d+))?\/[^?;]+(?:[?;].*)?$/i.exec(jdbcUrl);
  if (!match) throw new Error(`Use a ${databaseType} JDBC URL containing a host and database`);
  return { host: match[1], port: match[2] ?? (databaseType === 'postgresql' ? '5432' : '3306') };
};

export async function withDiscoveryConnection<T>(body: SourceDiscovery, action: (connectionName: string) => Promise<T>) {
  if (body.connectionName) return action(body.connectionName);

  const endpoint = jdbcEndpoint(body.databaseType, body.jdbcUrl);
  const connectionName = `_watersync_discovery_${randomUUID().replace(/-/g, '')}`;
  const connectionType = body.databaseType === 'sqlserver' ? 'SQLSERVER' : body.databaseType.toUpperCase();
  let createConnectionSql: string;
  if (body.ucSecretName) {
    const parts = body.ucSecretName.split('.');
    if (parts.length !== 3) {
      throw new Error(`UC secret name '${body.ucSecretName}' must be in 'catalog.schema.secret_name' format`);
    }
    // UC secrets are not supported on SQL warehouses — retrieve the value via REST API
    // and pass it as a string literal in the CREATE CONNECTION statement.
    const authHeaders = new Headers();
    await workspace.config.authenticate(authHeaders);
    const secretUrl = `${workspaceBaseUrl()}/api/2.1/unity-catalog/secrets/${body.ucSecretName}?include_value=true`;
    const secretResponse = await fetch(secretUrl, { headers: authHeaders });
    if (!secretResponse.ok) {
      throw new Error(`Failed to retrieve UC secret '${body.ucSecretName}': ${secretResponse.status} ${await secretResponse.text()}`);
    }
    const secretData = (await secretResponse.json()) as { effective_value?: string };
    if (!secretData.effective_value) {
      throw new Error(`UC secret '${body.ucSecretName}' returned an empty value`);
    }
    createConnectionSql = `CREATE CONNECTION \`${connectionName}\` TYPE ${connectionType} OPTIONS (
      host ${sqlLiteral(endpoint.host)},
      port ${sqlLiteral(endpoint.port)},
      user ${sqlLiteral(body.jdbcUser)},
      password ${sqlLiteral(secretData.effective_value)},
      trustServerCertificate true
    )`;
  } else {
    createConnectionSql = `CREATE CONNECTION \`${connectionName}\` TYPE ${connectionType} OPTIONS (
      host ${sqlLiteral(endpoint.host)},
      port ${sqlLiteral(endpoint.port)},
      user ${sqlLiteral(body.jdbcUser)},
      password secret(${sqlLiteral(body.jdbcSecretScope)}, ${sqlLiteral(body.jdbcSecretKey)}),
      trustServerCertificate true
    )`;
  }
  await execute(createConnectionSql);
  try {
    return await action(connectionName);
  } finally {
    try {
      await execute(`DROP CONNECTION IF EXISTS \`${connectionName}\``);
    } catch (error) {
      logger.warn('discovery.connection_cleanup_failed', { connectionName, error: serializeError(error) });
    }
  }
}

export const sourceTableLimit = 1000;

const likeLiteral = (value: string) => sqlLiteral(`%${value.toLowerCase()}%`);

const tableFilterPredicate = (tableFilter: string, schemaColumn: string, tableColumn: string) => {
  if (!tableFilter) return '';
  const separator = tableFilter.lastIndexOf('.');
  if (separator === -1) {
    return `(LOWER(${schemaColumn}) LIKE ${likeLiteral(tableFilter)}
      OR LOWER(${tableColumn}) LIKE ${likeLiteral(tableFilter)})`;
  }
  const conditions = [
    [schemaColumn, tableFilter.slice(0, separator).trim()],
    [tableColumn, tableFilter.slice(separator + 1).trim()],
  ]
    .filter(([, value]) => value)
    .map(([column, value]) => `LOWER(${column}) LIKE ${likeLiteral(value)}`);
  return conditions.length ? `(${conditions.join(' AND ')})` : '';
};

export const listTablesSql = (databaseType: SourceDatabaseType, tableFilter: string) => {
  if (databaseType === 'oracle') {
    const predicate = tableFilterPredicate(tableFilter, 'OWNER', 'TABLE_NAME');
    return `SELECT OWNER AS table_schema, TABLE_NAME AS table_name
      FROM ALL_TABLES
      ${predicate ? `WHERE ${predicate}` : ''}
      ORDER BY OWNER, TABLE_NAME
      FETCH FIRST ${sourceTableLimit} ROWS ONLY`;
  }
  const predicate = tableFilterPredicate(tableFilter, 'TABLE_SCHEMA', 'TABLE_NAME');
  const base = `SELECT ${databaseType === 'sqlserver' ? `TOP ${sourceTableLimit} ` : ''}TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE IN ('BASE TABLE', 'TABLE')${predicate ? ` AND ${predicate}` : ''}
    ORDER BY TABLE_SCHEMA, TABLE_NAME`;
  return databaseType === 'sqlserver' ? base : `${base} LIMIT ${sourceTableLimit}`;
};

// remote_query wraps the statement as a derived table, and SQL Server rejects ORDER BY there
// unless TOP, OFFSET or FOR XML is present.
const orderBySuffix = (databaseType: SourceDatabaseType) =>
  databaseType === 'sqlserver' ? ' OFFSET 0 ROWS' : '';

export const columnsSql = (databaseType: SourceDatabaseType, sourceSchema: string, table: string) => {
  const schema = sqlLiteral(sourceSchema);
  const tableNameValue = sqlLiteral(table);
  if (databaseType === 'oracle') {
    return `SELECT c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
        CASE c.NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
        c.COLUMN_ID AS ordinal_position,
        CASE WHEN EXISTS (
          SELECT 1
          FROM ALL_CONSTRAINTS tc
          JOIN ALL_CONS_COLUMNS k ON tc.OWNER = k.OWNER AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
          WHERE tc.CONSTRAINT_TYPE = 'P' AND tc.OWNER = c.OWNER
            AND tc.TABLE_NAME = c.TABLE_NAME AND k.COLUMN_NAME = c.COLUMN_NAME
        ) THEN 1 ELSE 0 END AS is_primary_key
      FROM ALL_TAB_COLUMNS c
      WHERE c.OWNER = UPPER(${schema}) AND c.TABLE_NAME = UPPER(${tableNameValue})
      ORDER BY c.COLUMN_ID`;
  }
  const concat =
    databaseType === 'mysql'
      ? 'GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION)'
      : databaseType === 'sqlserver'
        ? "STRING_AGG(CAST(k.COLUMN_NAME AS VARCHAR(MAX)), ',') WITHIN GROUP (ORDER BY k.ORDINAL_POSITION)"
        : "STRING_AGG(k.COLUMN_NAME, ',' ORDER BY k.ORDINAL_POSITION)";
  return `SELECT c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
      c.IS_NULLABLE AS is_nullable, c.ORDINAL_POSITION AS ordinal_position,
      CASE WHEN pk.primary_keys IS NOT NULL AND CONCAT(',', pk.primary_keys, ',') LIKE CONCAT('%,', c.COLUMN_NAME, ',%')
        THEN 1 ELSE 0 END AS is_primary_key
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN (
      SELECT ${concat} AS primary_keys
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        ON tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA AND tc.TABLE_NAME = k.TABLE_NAME
      WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        AND tc.TABLE_SCHEMA = ${schema} AND tc.TABLE_NAME = ${tableNameValue}
    ) pk ON 1 = 1
    WHERE c.TABLE_SCHEMA = ${schema} AND c.TABLE_NAME = ${tableNameValue}
    ORDER BY c.ORDINAL_POSITION${orderBySuffix(databaseType)}`;
};

export const columnsBatchSql = (
  databaseType: SourceDatabaseType,
  tables: Array<{ sourceSchema: string; table: string }>
) => {
  if (databaseType === 'oracle') {
    const filter = tables
      .map(
        (table) =>
          `(c.OWNER = UPPER(${sqlLiteral(table.sourceSchema)}) AND c.TABLE_NAME = UPPER(${sqlLiteral(table.table)}))`
      )
      .join(' OR ');
    return `SELECT c.OWNER AS source_schema, c.TABLE_NAME AS source_table,
        c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
        CASE c.NULLABLE WHEN 'Y' THEN 'YES' ELSE 'NO' END AS is_nullable,
        c.COLUMN_ID AS ordinal_position,
        CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
      FROM ALL_TAB_COLUMNS c
      LEFT JOIN (
        SELECT k.OWNER, k.TABLE_NAME, k.COLUMN_NAME
        FROM ALL_CONSTRAINTS tc
        JOIN ALL_CONS_COLUMNS k ON tc.OWNER = k.OWNER AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        WHERE tc.CONSTRAINT_TYPE = 'P'
      ) pk ON pk.OWNER = c.OWNER AND pk.TABLE_NAME = c.TABLE_NAME AND pk.COLUMN_NAME = c.COLUMN_NAME
      WHERE ${filter}
      ORDER BY c.OWNER, c.TABLE_NAME, c.COLUMN_ID`;
  }
  const filter = tables
    .map(
      (table) => `(c.TABLE_SCHEMA = ${sqlLiteral(table.sourceSchema)} AND c.TABLE_NAME = ${sqlLiteral(table.table)})`
    )
    .join(' OR ');
  return `SELECT c.TABLE_SCHEMA AS source_schema, c.TABLE_NAME AS source_table,
      c.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type,
      c.IS_NULLABLE AS is_nullable, c.ORDINAL_POSITION AS ordinal_position,
      CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END AS is_primary_key
    FROM INFORMATION_SCHEMA.COLUMNS c
    LEFT JOIN (
      SELECT k.TABLE_SCHEMA, k.TABLE_NAME, k.COLUMN_NAME
      FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
        ON tc.CONSTRAINT_CATALOG = k.CONSTRAINT_CATALOG
        AND tc.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA
        AND tc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        AND tc.TABLE_SCHEMA = k.TABLE_SCHEMA AND tc.TABLE_NAME = k.TABLE_NAME
      WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
    ) pk ON pk.TABLE_SCHEMA = c.TABLE_SCHEMA AND pk.TABLE_NAME = c.TABLE_NAME AND pk.COLUMN_NAME = c.COLUMN_NAME
    WHERE ${filter}
    ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION${orderBySuffix(databaseType)}`;
};

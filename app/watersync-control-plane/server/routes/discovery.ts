import type { Application } from 'express';
import { sourceDiscoverySchema, sourceColumnsSchema, sourceColumnsBatchSchema } from '../schemas.js';
import { responseRows } from '../sql.js';
import {
  withDiscoveryConnection,
  remoteQuery,
  listTablesSql,
  columnsSql,
  columnsBatchSql,
  sourceTableLimit,
} from '../discovery.js';
import { apiRoute } from '../logging.js';

/**
 * Registers source database discovery routes (tables and columns).
 * Extracted from the monolithic server.ts for maintainability.
 */
export function registerDiscoveryRoutes(app: Application) {
  app.post(
    '/api/source-tables',
    apiRoute('source_tables', async (req, res) => {
      const body = sourceDiscoverySchema.parse(req.body);
      const response = await withDiscoveryConnection(body, (connectionName) =>
        remoteQuery(
          connectionName,
          body.connectionName ? undefined : body.database,
          listTablesSql(body.databaseType, body.tableFilter),
          body.databaseType
        )
      );
      const tables = responseRows(response);
      res.json({ tables, truncated: tables.length >= sourceTableLimit });
    })
  );

  app.post(
    '/api/source-columns',
    apiRoute('source_columns', async (req, res) => {
      const body = sourceColumnsSchema.parse(req.body);
      const response = await withDiscoveryConnection(body, (connectionName) =>
        remoteQuery(
          connectionName,
          body.connectionName ? undefined : body.database,
          columnsSql(body.databaseType, body.sourceSchema, body.table),
          body.databaseType
        )
      );
      res.json({ columns: responseRows(response) });
    })
  );

  app.post(
    '/api/source-columns-batch',
    apiRoute('source_columns_batch', async (req, res) => {
      const body = sourceColumnsBatchSchema.parse(req.body);
      const rows = await withDiscoveryConnection(body, async (connectionName) => {
        const response = await remoteQuery(
          connectionName,
          body.connectionName ? undefined : body.database,
          columnsBatchSql(body.databaseType, body.tables),
          body.databaseType
        );
        return responseRows(response);
      });
      const rowValue = (row: Record<string, string | null>, name: string) =>
        row[name] ?? row[name.toUpperCase()] ?? null;
      const grouped = new Map<string, Array<Record<string, string | null>>>();
      for (const row of rows) {
        const key = `${rowValue(row, 'source_schema')}\u0000${rowValue(row, 'source_table')}`.toLowerCase();
        const columns = grouped.get(key) ?? [];
        columns.push({
          column_name: rowValue(row, 'column_name'),
          data_type: rowValue(row, 'data_type'),
          is_nullable: rowValue(row, 'is_nullable'),
          ordinal_position: rowValue(row, 'ordinal_position'),
          is_primary_key: rowValue(row, 'is_primary_key'),
        });
        grouped.set(key, columns);
      }
      const tables = body.tables.map((table) => ({
        ...table,
        columns: grouped.get(`${table.sourceSchema}\u0000${table.table}`.toLowerCase()) ?? [],
      }));
      res.json({ tables });
    })
  );
}

import migration_001 from './001_create_tables.js';
import migration_002 from './001_add_uc_secret_name.js';

/**
 * Context passed to every migration so it can run SQL and log progress
 * without depending on server.ts internals directly.
 */
export type MigrationContext = {
  execute: (statement: string) => Promise<unknown>;
  tableName: (catalog: string, schema: string, table: string) => string;
  logger: {
    info: (message: string, meta?: Record<string, unknown>) => void;
    warn: (message: string, meta?: Record<string, unknown>) => void;
  };
};

/** A single database migration. */
export type Migration = {
  id: string;
  description: string;
  run: (ctx: MigrationContext, catalog: string, schema: string) => Promise<void>;
};

/** Ordered list of all migrations. New migrations must be appended here. */
export const migrations: Migration[] = [migration_001, migration_002];

const migrationsTable = '_watersync_migrations';

/**
 * Runs every pending migration for the given catalog.schema.
 *
 * A `_watersync_migrations` tracking table is created (if absent) in the target
 * schema. Each migration that completes successfully is recorded so it is never
 * re-applied on subsequent calls.
 *
 * The caller should cache the result per location to avoid repeated warehouse
 * round-trips.
 */
export async function runMigrations(
  ctx: MigrationContext,
  catalog: string,
  schema: string
): Promise<void> {
  const trackingTable = ctx.tableName(catalog, schema, migrationsTable);

  // Ensure the schema exists (catalog must already exist).
  await ctx.execute(
    `CREATE SCHEMA IF NOT EXISTS \`${catalog}\`.\`${schema}\``
  );

  // Ensure the tracking table exists.
  await ctx.execute(
    `CREATE TABLE IF NOT EXISTS ${trackingTable} (id STRING NOT NULL, applied_at TIMESTAMP)`
  );

  // Fetch already-applied migration ids.
  const response = await ctx.execute(
    `SELECT id FROM ${trackingTable}`
  );
  const rows = (response as { result?: { data_array?: string[][] } }).result?.data_array ?? [];
  const applied = new Set(rows.map((row) => row[0]));

  const pending = migrations.filter((migration) => !applied.has(migration.id));
  if (pending.length === 0) return;

  ctx.logger.info('migrations.start', {
    catalog,
    schema,
    pending: pending.map((m) => m.id),
  });

  for (const migration of pending) {
    ctx.logger.info('migrations.run', { id: migration.id, description: migration.description });
    try {
      await migration.run(ctx, catalog, schema);
      await ctx.execute(
        `INSERT INTO ${trackingTable} VALUES ('${migration.id}', current_timestamp())`
      );
    } catch (error) {
      ctx.logger.warn('migrations.failed', {
        id: migration.id,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  ctx.logger.info('migrations.complete', { catalog, schema, count: pending.length });
}
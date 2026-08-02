/**
 * SQL migration runner.
 *
 * ── Why hand-rolled rather than node-pg-migrate / Flyway ───────────────────
 * The runner is ~120 lines and the migrations stay plain `.sql`. That matters
 * for the same reason as ADR-0010: `ALTER TABLE ... USING` is reviewable, and a
 * migration written in a JavaScript DSL is not. A reviewer can read exactly
 * what will run against production.
 *
 * The two mechanisms below are the reason this is not trivial, and they are
 * what most hand-rolled runners get wrong.
 *
 * ── 1. Advisory lock: concurrent migration ─────────────────────────────────
 * On a rolling deploy, N pods start simultaneously and all of them run
 * migrations. Without coordination they race: two pods both see migration 0007
 * as unapplied, both run it, and the second fails halfway — leaving the schema
 * in a state neither pod expected. On a bad day both partially succeed.
 *
 * A Postgres session-level advisory lock serialises them. The others block,
 * then find the work already done and proceed. No leader election, no
 * distributed lock service, no configuration.
 *
 * ── 2. Checksums: the edited migration ─────────────────────────────────────
 * Someone fixes a typo in an already-applied migration. It runs clean locally
 * on a fresh database, passes review, and does nothing in staging or production
 * because that filename is already recorded as applied. Now three environments
 * have different schemas and nothing reports it.
 *
 * Recording a hash of each file's contents turns that into a loud startup
 * failure naming the file. Applied migrations are immutable; a change is a new
 * file.
 *
 * ── Deliberately not supported ─────────────────────────────────────────────
 * • **Down migrations.** They are a trap: a rollback that drops a column
 *   destroys the data written since the deploy, and in practice nobody runs
 *   them under incident pressure. The safe path is a forward fix, which is why
 *   migrations must be written to be backward-compatible with the previous
 *   release (expand/contract).
 * • **Auto-generation from a model.** There is no model — ADR-0010.
 */

import { createHash } from 'node:crypto';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { Pool, PoolClient } from 'pg';

/**
 * Arbitrary but fixed. Advisory locks share one 64-bit namespace across the
 * database, so this constant must never collide with another subsystem's —
 * hence it lives here as a named constant rather than inline.
 */
const MIGRATION_LOCK_ID = 8_472_910_364_155n;

export interface Migration {
  readonly filename: string;
  readonly sql: string;
  readonly checksum: string;
}

export interface MigrationResult {
  readonly applied: string[];
  readonly skipped: string[];
}

/** Thrown when an already-applied migration file has been edited. */
export class MigrationChecksumError extends Error {
  constructor(filename: string, expected: string, actual: string) {
    super(
      `Migration "${filename}" has changed since it was applied.\n` +
        `  recorded: ${expected}\n` +
        `  on disk:  ${actual}\n\n` +
        `Applied migrations are immutable — environments that already ran this ` +
        `file will not re-run it, so editing it silently produces divergent ` +
        `schemas. Revert the edit and add a new migration instead.`,
    );
    this.name = 'MigrationChecksumError';
  }
}

/**
 * Load migrations from disk in lexicographic filename order.
 *
 * Order comes from the zero-padded numeric prefix (`0001_`, `0002_`). Padding
 * matters: without it `10_` sorts before `2_` and migrations run in the wrong
 * order, which is only discovered when one depends on another.
 */
export async function loadMigrations(directory: string): Promise<Migration[]> {
  const entries = await readdir(directory);
  const files = entries.filter((f) => f.endsWith('.sql')).sort();

  return await Promise.all(
    files.map(async (filename) => {
      const sql = await readFile(join(directory, filename), 'utf8');
      return {
        filename,
        sql,
        checksum: createHash('sha256').update(sql).digest('hex'),
      };
    }),
  );
}

/**
 * Apply any migrations not yet recorded.
 *
 * Idempotent and concurrency-safe: run it from every pod on every start.
 */
export async function migrate(pool: Pool, directory: string): Promise<MigrationResult> {
  const migrations = await loadMigrations(directory);
  const client = await pool.connect();

  try {
    // Session-level, not transaction-level (pg_advisory_xact_lock): the lock
    // must span multiple transactions, since each migration commits separately.
    // Released explicitly in the finally block below.
    await client.query('SELECT pg_advisory_lock($1)', [MIGRATION_LOCK_ID.toString()]);

    await ensureMigrationTable(client);
    const applied = await appliedMigrations(client);

    // Accumulate locally and construct the result once, rather than mutating a
    // `readonly` field through a cast. `readonly` on a property only forbids
    // reassignment — the array itself stays mutable — so the cast was doing
    // nothing except telling a reader it was doing something.
    const appliedNow: string[] = [];
    const skipped: string[] = [];

    for (const migration of migrations) {
      const record = applied.get(migration.filename);

      if (record) {
        if (record !== migration.checksum) {
          throw new MigrationChecksumError(migration.filename, record, migration.checksum);
        }
        skipped.push(migration.filename);
        continue;
      }

      await applyOne(client, migration);
      appliedNow.push(migration.filename);
    }

    return { applied: appliedNow, skipped };
  } finally {
    // Advisory locks are session-scoped, so releasing before returning the
    // client to the pool is mandatory: a pooled connection outlives this
    // function, and a leaked lock blocks every future migration run until the
    // connection happens to be recycled.
    await client.query('SELECT pg_advisory_unlock($1)', [MIGRATION_LOCK_ID.toString()]);
    client.release();
  }
}

async function ensureMigrationTable(client: PoolClient): Promise<void> {
  await client.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename      text PRIMARY KEY,
      checksum      text NOT NULL,
      applied_at    timestamptz NOT NULL DEFAULT now(),
      duration_ms   integer NOT NULL
    )
  `);
}

async function appliedMigrations(client: PoolClient): Promise<Map<string, string>> {
  const { rows } = await client.query<{ filename: string; checksum: string }>(
    'SELECT filename, checksum FROM schema_migrations',
  );
  return new Map(rows.map((r) => [r.filename, r.checksum]));
}

/**
 * Apply one migration and record it, atomically.
 *
 * The DDL and the bookkeeping row commit together. If they did not, a crash
 * between them would leave a migration applied but unrecorded — and the next
 * run would try to apply it again, which for `CREATE TABLE` fails outright and
 * for `ALTER TABLE ADD COLUMN` may succeed and silently double an intent.
 *
 * This works because Postgres has transactional DDL. On MySQL it would not, and
 * the runner would need a different — and much worse — design.
 */
async function applyOne(client: PoolClient, migration: Migration): Promise<void> {
  const startedAt = process.hrtime.bigint();

  await client.query('BEGIN');
  try {
    await client.query(migration.sql);
    await client.query(
      'INSERT INTO schema_migrations (filename, checksum, duration_ms) VALUES ($1, $2, $3)',
      [
        migration.filename,
        migration.checksum,
        Math.round(Number(process.hrtime.bigint() - startedAt) / 1_000_000),
      ],
    );
    await client.query('COMMIT');
  } catch (error: unknown) {
    await client.query('ROLLBACK');
    throw new Error(
      `Migration "${migration.filename}" failed and was rolled back: ${
        error instanceof Error ? error.message : String(error)
      }`,
      { cause: error instanceof Error ? error : undefined },
    );
  }
}

/**
 * Report what would be applied, without applying it.
 *
 * Used by a deploy pipeline to show the pending schema change in a PR before it
 * runs against production.
 */
export async function pendingMigrations(pool: Pool, directory: string): Promise<string[]> {
  const migrations = await loadMigrations(directory);
  const client = await pool.connect();
  try {
    await ensureMigrationTable(client);
    const applied = await appliedMigrations(client);
    return migrations.filter((m) => !applied.has(m.filename)).map((m) => m.filename);
  } finally {
    client.release();
  }
}

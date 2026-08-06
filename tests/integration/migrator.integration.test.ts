/**
 * Migration runner — against a real PostgreSQL.
 *
 * The two mechanisms worth testing here are the two that are impossible to
 * test without a database: the advisory lock serialising concurrent runners,
 * and transactional DDL rolling a failed migration back. Both are properties
 * of Postgres, and both are the reason this runner is not fifteen lines.
 */

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RecordingLogger } from '@platform/core';
import {
  MigrationChecksumError,
  Postgres,
  migrate,
  pendingMigrations,
} from '@platform/persistence';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { startPostgres } from './containers.js';

let container: StartedPostgreSqlContainer;
let postgres: Postgres;
let dir: string;

beforeAll(async () => {
  const fixture = await startPostgres();
  container = fixture.container;
  postgres = new Postgres({ config: fixture.config, logger: new RecordingLogger() });
});

afterAll(async () => {
  await postgres.close();
  await container.stop();
});

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'kep-mig-'));
  // Each test starts from an empty schema. Dropping and recreating `public` is
  // faster and more thorough than dropping known tables — it also removes
  // anything a failed test left behind.
  await postgres.query('DROP SCHEMA public CASCADE');
  await postgres.query('CREATE SCHEMA public');
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

async function write(filename: string, sql: string): Promise<void> {
  await writeFile(join(dir, filename), sql, 'utf8');
}

async function tableExists(name: string): Promise<boolean> {
  const { rows } = await postgres.query<{ exists: boolean }>(
    `SELECT to_regclass($1) IS NOT NULL AS exists`,
    [name],
  );
  return rows[0]?.exists ?? false;
}

describe('applying', () => {
  it('applies pending migrations in order and records them', async () => {
    await write('0001_a.sql', 'CREATE TABLE a (id int PRIMARY KEY);');
    await write('0002_b.sql', 'CREATE TABLE b (id int REFERENCES a(id));');

    const result = await migrate(postgres.pool, dir);

    // 0002 references 0001. If ordering were wrong this fails outright, which
    // is the reason zero-padded prefixes are a rule rather than a convention.
    expect(result.applied).toEqual(['0001_a.sql', '0002_b.sql']);
    expect(await tableExists('b')).toBe(true);
  });

  it('is idempotent across restarts', async () => {
    await write('0001_a.sql', 'CREATE TABLE a (id int);');

    await migrate(postgres.pool, dir);
    const second = await migrate(postgres.pool, dir);

    // Every pod runs this on every start. If it were not idempotent, the
    // second pod in a rolling deploy would crash-loop.
    expect(second.applied).toEqual([]);
    expect(second.skipped).toEqual(['0001_a.sql']);
  });

  it('reports what is pending without applying it', async () => {
    await write('0001_a.sql', 'CREATE TABLE a (id int);');
    await write('0002_b.sql', 'CREATE TABLE b (id int);');
    await migrate(postgres.pool, dir);
    await write('0003_c.sql', 'CREATE TABLE c (id int);');

    expect(await pendingMigrations(postgres.pool, dir)).toEqual(['0003_c.sql']);
    // The point of the method: a deploy pipeline can show the schema change in
    // a PR without running it.
    expect(await tableExists('c')).toBe(false);
  });
});

describe('failure handling', () => {
  it('rolls back a failed migration completely', async () => {
    // Two statements, the second invalid. Postgres has transactional DDL, so
    // the first must not survive. On MySQL it would, and this runner would
    // need a materially worse design.
    await write(
      '0001_partial.sql',
      `CREATE TABLE good (id int);
       CREATE TABLE bad (id int) THIS IS NOT SQL;`,
    );

    await expect(migrate(postgres.pool, dir)).rejects.toThrow(/failed and was rolled back/);

    expect(await tableExists('good')).toBe(false);
  });

  it('does not record a migration that failed', async () => {
    await write('0001_bad.sql', 'NOT VALID SQL;');
    await expect(migrate(postgres.pool, dir)).rejects.toThrow();

    await rm(join(dir, '0001_bad.sql'));
    await write('0001_bad.sql', 'CREATE TABLE fixed (id int);');

    // The fixed file must run. If the failed attempt had been recorded, this
    // filename would be skipped forever and the schema would be permanently
    // one migration behind with nothing reporting it.
    const result = await migrate(postgres.pool, dir);
    expect(result.applied).toEqual(['0001_bad.sql']);
  });

  it('releases the advisory lock even when a migration fails', async () => {
    await write('0001_bad.sql', 'NOT VALID SQL;');
    await expect(migrate(postgres.pool, dir)).rejects.toThrow();

    // A leaked session-level lock on a pooled connection blocks every future
    // migration run until that connection happens to be recycled — an outage
    // that begins on the *next* deploy, hours after the change that caused it.
    const { rows } = await postgres.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM pg_locks WHERE locktype = 'advisory'`,
    );
    expect(rows[0]?.count).toBe('0');
  });
});

describe('checksums', () => {
  it('refuses to start when an applied migration has been edited', async () => {
    await write('0001_a.sql', 'CREATE TABLE a (id int);');
    await migrate(postgres.pool, dir);

    await write('0001_a.sql', 'CREATE TABLE a (id bigint);');

    // The silent-drift scenario: the edit runs clean on a fresh local database
    // and does nothing in staging or production, leaving three environments
    // with different schemas and nothing reporting it.
    await expect(migrate(postgres.pool, dir)).rejects.toBeInstanceOf(MigrationChecksumError);
  });

  it('names the offending file in the error', async () => {
    await write('0001_a.sql', 'CREATE TABLE a (id int);');
    await write('0002_b.sql', 'CREATE TABLE b (id int);');
    await migrate(postgres.pool, dir);
    await write('0002_b.sql', 'CREATE TABLE b (id int, extra text);');

    await expect(migrate(postgres.pool, dir)).rejects.toThrow(/0002_b\.sql/);
  });
});

describe('concurrency', () => {
  it('serialises simultaneous runners so each migration applies once', async () => {
    // The rolling-deploy scenario: N pods start at once and all run
    // migrations. Without the advisory lock they race — two pods both see 0001
    // as unapplied, both run it, and the loser fails on "relation already
    // exists", crash-looping a pod that is otherwise healthy.
    await write('0001_a.sql', 'CREATE TABLE a (id int);');
    await write('0002_b.sql', 'CREATE TABLE b (id int);');

    const results = await Promise.all(
      Array.from({ length: 5 }, async () => await migrate(postgres.pool, dir)),
    );

    const appliedCounts = results.map((r) => r.applied.length);
    // Exactly one runner applies both; the other four find the work done.
    expect(appliedCounts.filter((n) => n === 2)).toHaveLength(1);
    expect(appliedCounts.filter((n) => n === 0)).toHaveLength(4);

    const { rows } = await postgres.query<{ count: string }>(
      'SELECT count(*)::text AS count FROM schema_migrations',
    );
    expect(rows[0]?.count).toBe('2');
  });
});

describe('the real schema', () => {
  it('applies cleanly from empty', async () => {
    // The migration this repository actually ships. Running it here means a
    // syntax error or a bad constraint fails in CI rather than at deploy time.
    const migrations = new URL('../../packages/persistence/migrations/', import.meta.url).pathname;

    const result = await migrate(postgres.pool, migrations);

    expect(result.applied).toContain('0001_initial_schema.sql');
    for (const table of ['topics', 'schema_versions', 'retry_events', 'dlq_events', 'outbox']) {
      expect(await tableExists(table)).toBe(true);
    }
  });
});

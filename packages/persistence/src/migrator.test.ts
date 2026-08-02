/**
 * Migrator unit tests.
 *
 * Only the pure parts — file loading, ordering, checksums — are covered here.
 * The advisory-lock and transactional-apply behaviour needs a real database and
 * is covered by the integration suite, because mocking a lock proves nothing
 * about whether the lock works.
 */

import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { MigrationChecksumError, loadMigrations } from './migrator.js';

let dir: string;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), 'kep-migrations-'));
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

async function write(filename: string, sql: string): Promise<void> {
  await writeFile(join(dir, filename), sql, 'utf8');
}

describe('loading', () => {
  it('applies migrations in zero-padded numeric order', async () => {
    // The reason for padding: without it '10_' sorts before '2_', and
    // migrations run out of order. That only surfaces when one depends on
    // another — usually in the environment you least want it to.
    await write('0001_first.sql', 'SELECT 1;');
    await write('0002_second.sql', 'SELECT 2;');
    await write('0010_tenth.sql', 'SELECT 10;');

    const migrations = await loadMigrations(dir);

    expect(migrations.map((m) => m.filename)).toEqual([
      '0001_first.sql',
      '0002_second.sql',
      '0010_tenth.sql',
    ]);
  });

  it('demonstrates why padding is required', async () => {
    // Same set, unpadded. Lexicographic order puts 10 before 2 — this test
    // exists to make the failure mode visible rather than folklore.
    await write('1_first.sql', 'SELECT 1;');
    await write('2_second.sql', 'SELECT 2;');
    await write('10_tenth.sql', 'SELECT 10;');

    const migrations = await loadMigrations(dir);

    expect(migrations.map((m) => m.filename)).toEqual([
      '10_tenth.sql',
      '1_first.sql',
      '2_second.sql',
    ]);
  });

  it('ignores non-SQL files', async () => {
    // README.md and .DS_Store both end up in migration directories.
    await write('0001_real.sql', 'SELECT 1;');
    await write('README.md', '# notes');
    await write('.DS_Store', 'junk');

    expect(await loadMigrations(dir)).toHaveLength(1);
  });

  it('returns an empty list for an empty directory', async () => {
    expect(await loadMigrations(dir)).toEqual([]);
  });
});

describe('checksums', () => {
  it('produces a stable hash for identical content', async () => {
    await write('0001_a.sql', 'CREATE TABLE t (id int);');
    const first = await loadMigrations(dir);
    const second = await loadMigrations(dir);
    expect(first[0]?.checksum).toBe(second[0]?.checksum);
  });

  it('changes when a single character changes', async () => {
    // The property the whole mechanism depends on: an edit must be detectable.
    await write('0001_a.sql', 'CREATE TABLE t (id int);');
    const before = (await loadMigrations(dir))[0]?.checksum;

    await write('0001_a.sql', 'CREATE TABLE t (id bigint);');
    const after = (await loadMigrations(dir))[0]?.checksum;

    expect(after).not.toBe(before);
  });

  it('changes on whitespace too', async () => {
    // Deliberately not normalised. A whitespace-only edit is still an edit to a
    // file that other environments will never re-run, so it must be caught.
    // Normalising would make "harmless" edits invisible and reintroduce drift.
    await write('0001_a.sql', 'SELECT 1;');
    const before = (await loadMigrations(dir))[0]?.checksum;

    await write('0001_a.sql', 'SELECT  1;');
    expect((await loadMigrations(dir))[0]?.checksum).not.toBe(before);
  });
});

describe('MigrationChecksumError', () => {
  it('names the file and explains why editing is not allowed', () => {
    // The message is the whole value of this error: whoever hits it at 2am
    // needs to know it is not a transient failure and not something to retry.
    const error = new MigrationChecksumError('0007_add_index.sql', 'abc123', 'def456');

    expect(error.message).toContain('0007_add_index.sql');
    expect(error.message).toContain('abc123');
    expect(error.message).toContain('def456');
    expect(error.message).toMatch(/immutable/i);
    expect(error.message).toMatch(/add a new migration/i);
  });
});

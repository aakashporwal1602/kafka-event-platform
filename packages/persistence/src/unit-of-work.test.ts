/**
 * Unit of Work tests.
 *
 * The property under test is the one the outbox pattern depends on: a
 * repository obtained inside `run` issues its statements on the **transaction**,
 * not on the pool. Everything else here is secondary.
 */

import { describe, expect, it } from 'vitest';
import type { Queryable } from './postgres.js';
import { PostgresUnitOfWork, type TransactionRunner } from './unit-of-work.js';

/** Tags every query with which connection ran it, so enlistment is observable. */
function taggedQueryable(tag: string, log: { tag: string; sql: string }[]): Queryable {
  return {
    query<T extends Record<string, unknown>>(
      sql: string,
    ): Promise<{ rows: T[]; rowCount: number }> {
      log.push({ tag, sql });
      const rows: T[] = [];
      return Promise.resolve({ rows, rowCount: 0 });
    },
  };
}

class FakeRunner implements TransactionRunner {
  public readonly log: { tag: string; sql: string }[] = [];
  public committed = 0;
  public rolledBack = 0;
  public serializableAttempts = 0;

  readonly #pool = taggedQueryable('pool', this.log);
  readonly #tx = taggedQueryable('tx', this.log);

  public query<T extends Record<string, unknown>>(sql: string) {
    return this.#pool.query<T>(sql);
  }

  public async transaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T> {
    try {
      const result = await fn(this.#tx);
      this.committed++;
      return result;
    } catch (error: unknown) {
      this.rolledBack++;
      throw error;
    }
  }

  public async serializable<T>(fn: (tx: Queryable) => Promise<T>, maxAttempts = 3): Promise<T> {
    this.serializableAttempts = maxAttempts;
    return await this.transaction(fn);
  }
}

describe('run', () => {
  it('binds repositories to the transaction, not the pool', async () => {
    // This is the whole reason the class exists. A repository constructed at
    // startup with the pool borrows a *different* connection inside a
    // transaction, so its write commits on its own — and the outbox row
    // survives a rollback of the state change it was supposed to describe.
    const runner = new FakeRunner();
    const uow = new PostgresUnitOfWork({ postgres: runner });

    await uow.run(async (u) => {
      await u.outbox.countUnpublished();
    });

    expect(runner.log.map((e) => e.tag)).toEqual(['tx']);
    expect(runner.committed).toBe(1);
  });

  it('propagates the callback error so the transaction rolls back', async () => {
    const runner = new FakeRunner();
    const uow = new PostgresUnitOfWork({ postgres: runner });

    await expect(
      uow.run(async (u) => {
        await u.outbox.countUnpublished();
        throw new Error('domain rule violated');
      }),
    ).rejects.toThrow('domain rule violated');

    expect(runner.rolledBack).toBe(1);
    expect(runner.committed).toBe(0);
  });

  it('returns the callback result unchanged', async () => {
    const uow = new PostgresUnitOfWork({ postgres: new FakeRunner() });
    expect(await uow.run(() => Promise.resolve('value'))).toBe('value');
  });

  it('exposes the raw transaction for queries with no repository yet', async () => {
    const runner = new FakeRunner();
    const uow = new PostgresUnitOfWork({ postgres: runner });

    await uow.run(async (u) => {
      await u.tx.query('UPDATE orders SET status = $1 WHERE id = $2');
    });

    // The escape hatch must run on the same connection as the repositories, or
    // it is not an escape hatch, it is a second transaction.
    expect(runner.log).toEqual([{ tag: 'tx', sql: 'UPDATE orders SET status = $1 WHERE id = $2' }]);
  });
});

describe('runSerializable', () => {
  it('forwards the attempt budget', async () => {
    const runner = new FakeRunner();
    await new PostgresUnitOfWork({ postgres: runner }).runSerializable(() => Promise.resolve(1), 5);

    // Retry is not optional at SERIALIZABLE: Postgres aborts conflicting
    // transactions at commit rather than blocking them, so a caller that does
    // not retry loses writes under contention.
    expect(runner.serializableAttempts).toBe(5);
  });
});

describe('repositories (no transaction)', () => {
  it('runs on the pool', async () => {
    const runner = new FakeRunner();
    const uow = new PostgresUnitOfWork({ postgres: runner });

    await uow.repositories.outbox.countUnpublished();

    // Read-only work outside a transaction is deliberate: BEGIN/COMMIT around
    // one SELECT is two extra round trips and holds a pooled connection for
    // all three.
    expect(runner.log.map((e) => e.tag)).toEqual(['pool']);
    expect(runner.committed).toBe(0);
  });

  it('hands out a fresh graph on each access', () => {
    const uow = new PostgresUnitOfWork({ postgres: new FakeRunner() });

    // Caching would create one long-lived repository holding the pool — the
    // exact object that makes the enlistment mistake writable again.
    expect(uow.repositories.outbox).not.toBe(uow.repositories.outbox);
  });
});

describe('relay sharding', () => {
  it('passes the shard through to the outbox repository', async () => {
    const runner = new FakeRunner();
    const uow = new PostgresUnitOfWork({ postgres: runner, relayShard: { index: 1, total: 3 } });

    await uow.run(async (u) => {
      await u.outbox.claimBatch(10);
    });

    expect(runner.log.at(-1)?.sql).toContain('hashtext');
  });

  it('surfaces an invalid shard at construction of the repository', async () => {
    const uow = new PostgresUnitOfWork({
      postgres: new FakeRunner(),
      relayShard: { index: 9, total: 3 },
    });

    await expect(uow.run(() => Promise.resolve(null))).rejects.toThrow(/out of range/);
  });
});

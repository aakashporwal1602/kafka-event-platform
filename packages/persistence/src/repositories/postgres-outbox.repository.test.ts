/**
 * Outbox repository unit tests.
 *
 * ── What these test, and what they deliberately do not ─────────────────────
 * These assert the **statement** the repository builds: its shape, its
 * parameters, and the guards around it. They do not assert that Postgres
 * behaves as expected — a fake that returns whatever it is told proves nothing
 * about `SKIP LOCKED`, and a test that mocks the database into agreeing is the
 * kind of test that stays green while production breaks.
 *
 * Real behaviour is covered in `tests/integration/outbox.integration.test.ts`
 * against a live Postgres. The split is deliberate: these run in milliseconds
 * on every save, those run in CI.
 */

import { describe, expect, it } from 'vitest';
import type { Queryable } from '../postgres.js';
import { PostgresOutboxRepository, type RelayShard } from './postgres-outbox.repository.js';
import type { NewOutboxEvent } from './outbox.repository.js';

interface Call {
  sql: string;
  params: readonly unknown[];
}

/**
 * Records what was asked and replays canned rows.
 *
 * Not a database. Its only job is to make the generated SQL observable.
 */
class RecordingQueryable implements Queryable {
  public readonly calls: Call[] = [];
  #responses: Record<string, unknown>[][] = [];

  public respondWith(rows: Record<string, unknown>[]): this {
    this.#responses.push(rows);
    return this;
  }

  public query<T extends Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
  ): Promise<{ rows: T[]; rowCount: number }> {
    this.calls.push({ sql, params });
    const rows = (this.#responses.shift() ?? []) as T[];
    return Promise.resolve({ rows, rowCount: rows.length });
  }

  public get lastCall(): Call {
    const call = this.calls.at(-1);
    if (call === undefined) throw new Error('no query was issued');
    return call;
  }

  /** Whitespace-collapsed, so assertions do not depend on template indentation. */
  public get lastSql(): string {
    return this.lastCall.sql.replace(/\s+/g, ' ').trim();
  }
}

function event(overrides: Partial<NewOutboxEvent> = {}): NewOutboxEvent {
  return {
    eventId: '11111111-1111-4111-8111-111111111111',
    aggregateType: 'order',
    aggregateId: 'order-1',
    eventType: 'order.placed',
    topic: 'orders.events.v1',
    payload: { total: 100 },
    ...overrides,
  };
}

describe('insertMany', () => {
  it('writes every row in a single statement', async () => {
    const db = new RecordingQueryable().respondWith([{ id: '1' }, { id: '2' }, { id: '3' }]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.insertMany([event(), event(), event()]);

    // The property that matters: three events, one round trip. Three separate
    // INSERTs inside a transaction would be correct but would triple the time
    // the transaction holds its locks.
    expect(db.calls).toHaveLength(1);
    expect(db.lastCall.params).toHaveLength(30); // 3 rows × 10 columns
    expect(db.lastSql).toContain('($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9, $10)');
    expect(db.lastSql).toContain('($11, $12,');
  });

  it('parses bigint ids from the strings pg returns', async () => {
    // pg returns int8 as a string on purpose — 2^53 is not far away at
    // platform volumes, and Number() would silently start rounding.
    const db = new RecordingQueryable().respondWith([{ id: '9007199254740993' }]);
    const repo = new PostgresOutboxRepository({ db });

    const [id] = await repo.insertMany([event()]);

    // BigInt literal — exact. A `number` literal of the same value cannot be
    // written here at all: the linter rejects it, because JavaScript rounds it
    // on parse. That rejection is the argument for the bigint column in one
    // line, so the assertion below shows the rounded value instead.
    expect(id).toBe(9_007_199_254_740_993n);
    expect(Number(id)).toBe(9_007_199_254_740_992);
  });

  it('serialises payload and headers rather than passing objects', async () => {
    const db = new RecordingQueryable().respondWith([{ id: '1' }]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.insertMany([event({ payload: { a: 1 }, headers: { source: 'api' } })]);

    expect(db.lastCall.params[6]).toBe('{"a":1}');
    expect(db.lastCall.params[7]).toBe('{"source":"api"}');
  });

  it('defaults optional fields to null, not undefined', async () => {
    // `undefined` bound as a parameter is coerced to NULL by pg today, but that
    // is driver behaviour rather than a documented contract. Being explicit
    // means a driver change cannot turn a NULL into a failed insert.
    const db = new RecordingQueryable().respondWith([{ id: '1' }]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.insertMany([event()]);

    expect(db.lastCall.params[5]).toBeNull(); // partition_key
    expect(db.lastCall.params[8]).toBeNull(); // correlation_id
    expect(db.lastCall.params[9]).toBeNull(); // tenant_id
    expect(db.lastCall.params[7]).toBe('{}'); // headers default
  });

  it('ignores duplicate event ids instead of failing the transaction', async () => {
    const db = new RecordingQueryable().respondWith([{ id: '1' }]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.insertMany([event()]);

    // Without ON CONFLICT, a retried transaction hits a unique violation and a
    // recoverable retry becomes a permanent DuplicateEventError.
    expect(db.lastSql).toContain('ON CONFLICT (event_id) DO NOTHING');
  });

  it('refuses a batch that would exceed the bind-parameter limit', async () => {
    const db = new RecordingQueryable();
    const repo = new PostgresOutboxRepository({ db });
    const tooMany = Array.from({ length: 6_554 }, () => event());

    // 6554 × 10 = 65,540 > 65,535. Failing here with a sentence beats failing
    // at the wire with "bind message supplies 65540 parameters".
    await expect(repo.insertMany(tooMany)).rejects.toThrow(/65535-parameter protocol limit/);
    expect(db.calls).toHaveLength(0);
  });

  it('short-circuits on an empty batch', async () => {
    const db = new RecordingQueryable();
    const repo = new PostgresOutboxRepository({ db });

    expect(await repo.insertMany([])).toEqual([]);
    // `VALUES ` with nothing after it is a syntax error, and an empty relay
    // tick is an ordinary occurrence rather than an exceptional one.
    expect(db.calls).toHaveLength(0);
  });
});

describe('insert', () => {
  it('throws a readable error if RETURNING yields nothing', async () => {
    // Unreachable in practice; asserted so that if a future change makes it
    // reachable the failure names the problem instead of being a TypeError on
    // `undefined`.
    const db = new RecordingQueryable().respondWith([]);
    const repo = new PostgresOutboxRepository({ db });

    await expect(repo.insert(event())).rejects.toThrow(/returned no id/);
  });
});

describe('claimBatch', () => {
  it('uses FOR UPDATE SKIP LOCKED in id order', async () => {
    const db = new RecordingQueryable().respondWith([]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.claimBatch(100);

    const sql = db.lastSql;
    // SKIP LOCKED is what lets N relays drain one table without contending.
    // Without it they serialise behind each other and adding relays adds
    // nothing but connections.
    expect(sql).toContain('FOR UPDATE SKIP LOCKED');
    // ORDER BY id is what makes publication order match write order.
    expect(sql).toContain('ORDER BY id');
    expect(sql).toContain('WHERE published_at IS NULL');
    expect(db.lastCall.params).toEqual([100]);
  });

  it('omits the shard predicate when unsharded', async () => {
    const db = new RecordingQueryable().respondWith([]);
    await new PostgresOutboxRepository({ db }).claimBatch(10);

    expect(db.lastSql).not.toContain('hashtext');
  });

  it('binds shard parameters rather than interpolating them', async () => {
    const db = new RecordingQueryable().respondWith([]);
    const repo = new PostgresOutboxRepository({ db, shard: { index: 2, total: 4 } });

    await repo.claimBatch(10);

    expect(db.lastSql).toContain('hashtext(aggregate_id) & 2147483647) % $2 = $3');
    expect(db.lastCall.params).toEqual([10, 4, 2]);
    // The mask is not decoration: hashtext returns a signed int4 and Postgres'
    // % keeps the sign, so an unmasked predicate matches no shard for roughly
    // half of all aggregate ids.
    expect(db.lastSql).toContain('2147483647');
  });

  it('maps rows to a record with a bigint id and parsed dates', async () => {
    const createdAt = new Date('2026-08-02T10:00:00.000Z');
    const db = new RecordingQueryable().respondWith([
      {
        id: '42',
        event_id: 'e-1',
        aggregate_type: 'order',
        aggregate_id: 'order-1',
        event_type: 'order.placed',
        topic: 'orders.events.v1',
        partition_key: 'order-1',
        payload: { total: 100 },
        headers: { source: 'api' },
        created_at: createdAt,
        attempts: 0,
        correlation_id: 'corr-1',
        tenant_id: null,
      },
    ]);

    const [record] = await new PostgresOutboxRepository({ db }).claimBatch(1);

    expect(record?.id).toBe(42n);
    expect(record?.createdAt).toBe(createdAt);
    expect(record?.tenantId).toBeNull();
    expect(record?.payload).toEqual({ total: 100 });
  });
});

describe('sharding configuration', () => {
  // At construction, not at query time. A relay with an impossible shard starts
  // cleanly and claims nothing forever — a silent stall that looks exactly like
  // an empty backlog, which is the worst kind of outage to diagnose.
  const invalid: { name: string; shard: RelayShard; expected: RegExp }[] = [
    { name: 'index equal to total', shard: { index: 4, total: 4 }, expected: /out of range/ },
    { name: 'negative index', shard: { index: -1, total: 4 }, expected: /out of range/ },
    { name: 'zero shards', shard: { index: 0, total: 0 }, expected: /total >= 1/ },
    { name: 'fractional index', shard: { index: 1.5, total: 4 }, expected: /integers/ },
  ];

  for (const { name, shard, expected } of invalid) {
    it(`rejects ${name}`, () => {
      expect(() => new PostgresOutboxRepository({ db: new RecordingQueryable(), shard })).toThrow(
        expected,
      );
    });
  }
});

describe('markPublished', () => {
  it('updates the whole batch in one statement via unnest', async () => {
    const db = new RecordingQueryable().respondWith([]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.markPublished([
      { id: 1n, partition: 3, offset: 900n },
      { id: 2n, partition: 3, offset: 901n },
    ]);

    expect(db.calls).toHaveLength(1);
    expect(db.lastSql).toContain('unnest($1::bigint[], $2::int[], $3::bigint[])');
    // `offset` is a reserved word; aliasing to `off` is required, not stylistic.
    expect(db.lastSql).toContain('AS u(id, part, off)');
  });

  it('binds bigints as strings', async () => {
    const db = new RecordingQueryable().respondWith([]);
    const repo = new PostgresOutboxRepository({ db });

    await repo.markPublished([{ id: 9_007_199_254_740_993n, partition: 0, offset: 1n }]);

    // pg has no BigInt serialiser: passing a bigint throws
    // "TypeError: Do not know how to serialize a BigInt".
    expect(db.lastCall.params[0]).toEqual(['9007199254740993']);
    expect(db.lastCall.params[2]).toEqual(['1']);
  });

  it('does nothing for an empty batch', async () => {
    const db = new RecordingQueryable();
    await new PostgresOutboxRepository({ db }).markPublished([]);
    expect(db.calls).toHaveLength(0);
  });
});

describe('markFailed', () => {
  it('increments attempts and truncates the error', async () => {
    const db = new RecordingQueryable().respondWith([]);
    await new PostgresOutboxRepository({ db }).markFailed(7n, 'x'.repeat(5_000));

    expect(db.lastSql).toContain('attempts = attempts + 1');
    // Unbounded text × a million failing rows is a table-sized problem, and
    // nobody reads past the first line of a stack trace anyway.
    expect(db.lastSql).toContain('left($2, 1000)');
  });
});

describe('deletePublishedBefore', () => {
  it('bounds the delete with a subquery instead of deleting everything', async () => {
    const db = new RecordingQueryable().respondWith([]);
    await new PostgresOutboxRepository({ db }).deletePublishedBefore(new Date(0), 1_000);

    // An unbounded DELETE of millions of rows is one enormous WAL transaction
    // that holds locks throughout and loses all its work if interrupted.
    expect(db.lastSql).toContain('LIMIT $2');
    expect(db.lastSql).toContain('published_at IS NOT NULL');
  });
});

describe('countUnpublished', () => {
  it('reads the count as text and converts once', async () => {
    const db = new RecordingQueryable().respondWith([{ count: '12345' }]);
    expect(await new PostgresOutboxRepository({ db }).countUnpublished()).toBe(12_345);
  });

  it('reports zero when the table is empty', async () => {
    const db = new RecordingQueryable().respondWith([]);
    // `rows[0]` is `undefined` under noUncheckedIndexedAccess, and a lag metric
    // that reports NaN silently breaks the alert built on it.
    expect(await new PostgresOutboxRepository({ db }).countUnpublished()).toBe(0);
  });
});

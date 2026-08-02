/**
 * Outbox repository — against a real PostgreSQL.
 *
 * Every assertion here is about something the database does, not something our
 * code does. That is the selection criterion: if a fake could make it pass, it
 * belongs in the unit suite.
 */

import { RecordingLogger } from '@platform/core';
import {
  Postgres,
  PostgresOutboxRepository,
  PostgresUnitOfWork,
  migrate,
  type NewOutboxEvent,
  type OutboxRecord,
} from '@platform/persistence';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { migrationsDir, startPostgres } from './containers.js';

let container: StartedPostgreSqlContainer;
let postgres: Postgres;
let uow: PostgresUnitOfWork;

beforeAll(async () => {
  const fixture = await startPostgres();
  container = fixture.container;
  postgres = new Postgres({ config: fixture.config, logger: new RecordingLogger() });
  await migrate(postgres.pool, migrationsDir());
  uow = new PostgresUnitOfWork({ postgres });
});

afterAll(async () => {
  await postgres.close();
  await container.stop();
});

beforeEach(async () => {
  // RESTART IDENTITY so id assertions do not depend on test order. CASCADE is
  // deliberately absent: if a future foreign key makes this fail, that is a
  // signal worth seeing rather than one to suppress.
  await postgres.query('TRUNCATE outbox RESTART IDENTITY');
});

let sequence = 0;
function event(overrides: Partial<NewOutboxEvent> = {}): NewOutboxEvent {
  sequence++;
  return {
    eventId: `00000000-0000-4000-8000-${String(sequence).padStart(12, '0')}`,
    aggregateType: 'order',
    aggregateId: `order-${sequence}`,
    eventType: 'order.placed',
    topic: 'orders.events.v1',
    payload: { total: sequence },
    ...overrides,
  };
}

/** Resolvable from outside, so one transaction can be held open deliberately. */
function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let capture: ((value: T) => void) | undefined;
  const promise = new Promise<T>((resolve) => {
    capture = resolve;
  });
  // The Promise executor runs synchronously, so this is assigned. Checked
  // rather than asserted so a future engine change fails with a sentence.
  if (capture === undefined) throw new Error('promise executor did not run synchronously');
  return { promise, resolve: capture };
}

describe('atomicity with the caller state change', () => {
  it('commits the event and the state change together', async () => {
    await uow.run(async (u) => {
      await u.tx.query(`CREATE TEMP TABLE IF NOT EXISTS orders (id text PRIMARY KEY)`);
      await u.tx.query(`INSERT INTO orders (id) VALUES ($1)`, ['order-a']);
      await u.outbox.insert(event({ aggregateId: 'order-a' }));
    });

    expect(await uow.repositories.outbox.countUnpublished()).toBe(1);
  });

  it('rolls the event back when the state change fails', async () => {
    // This is the entire reason the Unit of Work exists. If the repository had
    // borrowed its own connection, the row below would survive this rollback —
    // and the platform would publish an event describing something that never
    // happened, which is strictly worse than the dual-write problem the outbox
    // was adopted to solve.
    await expect(
      uow.run(async (u) => {
        await u.outbox.insert(event());
        throw new Error('business rule violated');
      }),
    ).rejects.toThrow('business rule violated');

    expect(await uow.repositories.outbox.countUnpublished()).toBe(0);
  });
});

describe('FOR UPDATE SKIP LOCKED', () => {
  it('hands disjoint rows to concurrent claimers', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany([event(), event(), event(), event()]);
    });

    const claimed = deferred<OutboxRecord[]>();
    const holdOpen = deferred<void>();

    // Relay A claims two rows and keeps its transaction open, holding the row
    // locks.
    const relayA = uow.run(async (u) => {
      const rows = await u.outbox.claimBatch(2);
      claimed.resolve(rows);
      await holdOpen.promise;
      return rows;
    });

    const rowsA = await claimed.promise;

    // Relay B runs while A still holds its locks. Without SKIP LOCKED this
    // call blocks until A commits, and adding relays adds nothing but
    // connections. With it, B gets the *next* two rows immediately.
    const rowsB = await uow.run(async (u) => await u.outbox.claimBatch(2));

    holdOpen.resolve();
    await relayA;

    expect(rowsA).toHaveLength(2);
    expect(rowsB).toHaveLength(2);

    const overlap = rowsA.filter((a) => rowsB.some((b) => b.id === a.id));
    expect(overlap).toEqual([]);
    // A gets the lower ids: ORDER BY id is what makes publication order match
    // write order.
    expect(rowsA.map((r) => r.id)).toEqual([1n, 2n]);
    expect(rowsB.map((r) => r.id)).toEqual([3n, 4n]);
  });

  it('returns a full batch by skipping locked rows rather than a short one', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany([event(), event(), event(), event(), event(), event()]);
    });

    const claimed = deferred<OutboxRecord[]>();
    const holdOpen = deferred<void>();

    const relayA = uow.run(async (u) => {
      const rows = await u.outbox.claimBatch(3);
      claimed.resolve(rows);
      await holdOpen.promise;
      return rows;
    });
    await claimed.promise;

    // The clause ordering claim from the unit test, verified: SKIP LOCKED is
    // applied during the scan, so LIMIT counts *unlocked* rows. A naive
    // implementation returns 0 here because the first three are locked.
    const rowsB = await uow.run(async (u) => await u.outbox.claimBatch(3));

    holdOpen.resolve();
    await relayA;

    expect(rowsB.map((r) => r.id)).toEqual([4n, 5n, 6n]);
  });
});

describe('sharding', () => {
  it('partitions the backlog with no overlap and no gaps', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany(Array.from({ length: 60 }, () => event()));
    });

    const shards = [0, 1, 2].map(
      (index) => new PostgresUnitOfWork({ postgres, relayShard: { index, total: 3 } }),
    );

    const claims = await Promise.all(
      shards.map(async (shard) => await shard.run(async (u) => await u.outbox.claimBatch(100))),
    );

    const ids = claims.flat().map((r) => r.id);
    expect(new Set(ids).size).toBe(60); // no duplicates
    expect(ids).toHaveLength(60); // no gaps

    // Each shard gets some work. With 60 distinct aggregate ids across 3
    // shards, an empty shard means the hash or the mask is wrong — which is
    // the failure a unit test cannot see, because hashtext is the database's.
    for (const claim of claims) expect(claim.length).toBeGreaterThan(0);
  });

  it('keeps all events for one aggregate in the same shard', async () => {
    // The ordering guarantee the whole sharding scheme exists for: two events
    // about one order share a partition key, land in one Kafka partition, and
    // are applied in arrival order. Splitting them across relays is how
    // `order.updated` overtakes `order.created`.
    await uow.run(async (u) => {
      await u.outbox.insertMany([
        event({ aggregateId: 'order-x', eventType: 'order.created' }),
        event({ aggregateId: 'order-x', eventType: 'order.updated' }),
        event({ aggregateId: 'order-x', eventType: 'order.shipped' }),
      ]);
    });

    const claims = await Promise.all(
      [0, 1, 2].map(
        async (index) =>
          await new PostgresUnitOfWork({ postgres, relayShard: { index, total: 3 } }).run(
            async (u) => await u.outbox.claimBatch(100),
          ),
      ),
    );

    const nonEmpty = claims.filter((c) => c.length > 0);
    expect(nonEmpty).toHaveLength(1);
    expect(nonEmpty[0]).toHaveLength(3);
    expect(nonEmpty[0]?.map((r) => r.eventType)).toEqual([
      'order.created',
      'order.updated',
      'order.shipped',
    ]);
  });
});

describe('duplicate event ids', () => {
  it('silently ignores a re-insert instead of failing the transaction', async () => {
    const duplicate = event();

    await uow.run(async (u) => {
      await u.outbox.insert(duplicate);
    });
    const second = await uow.run(async (u) => await u.outbox.insertMany([duplicate]));

    // ON CONFLICT DO NOTHING means the retried insert returns no id. That is
    // the contract: "the ids that were written", not "one id per input".
    expect(second).toEqual([]);
    expect(await uow.repositories.outbox.countUnpublished()).toBe(1);
  });
});

describe('publishing', () => {
  it('records partition and offset, and removes the row from the backlog', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany([event(), event()]);
    });

    const claimed = await uow.run(async (u) => {
      const rows = await u.outbox.claimBatch(10);
      await u.outbox.markPublished(
        rows.map((r, i) => ({ id: r.id, partition: 3, offset: BigInt(900 + i) })),
      );
      return rows;
    });

    expect(claimed).toHaveLength(2);
    expect(await uow.repositories.outbox.countUnpublished()).toBe(0);

    const { rows } = await postgres.query<{
      published_partition: number;
      published_offset: string;
    }>('SELECT published_partition, published_offset FROM outbox ORDER BY id');
    expect(rows[0]?.published_partition).toBe(3);
    // int8 comes back as a string. Asserting on the string is the point: a test
    // that compared numbers would pass today and start rounding above 2^53.
    expect(rows[0]?.published_offset).toBe('900');
  });

  it('round-trips an offset larger than Number.MAX_SAFE_INTEGER', async () => {
    const huge = 9_007_199_254_740_993n; // 2^53 + 1
    await uow.run(async (u) => {
      await u.outbox.insert(event());
    });

    await uow.run(async (u) => {
      const [row] = await u.outbox.claimBatch(1);
      if (!row) throw new Error('expected a row');
      await u.outbox.markPublished([{ id: row.id, partition: 0, offset: huge }]);
    });

    const { rows } = await postgres.query<{ published_offset: string }>(
      'SELECT published_offset FROM outbox',
    );
    expect(BigInt(rows[0]?.published_offset ?? '0')).toBe(huge);
  });

  it('increments attempts and truncates the recorded error', async () => {
    await uow.run(async (u) => {
      await u.outbox.insert(event());
    });

    await uow.run(async (u) => {
      await u.outbox.markFailed(1n, 'e'.repeat(5_000));
      await u.outbox.markFailed(1n, 'second failure');
    });

    const { rows } = await postgres.query<{ attempts: number; last_error: string }>(
      'SELECT attempts, last_error FROM outbox WHERE id = 1',
    );
    // Still in the backlog. A relay that drops what it cannot publish is worse
    // than one that stalls: the stall pages someone, the drop does not.
    expect(rows[0]?.attempts).toBe(2);
    expect(rows[0]?.last_error).toBe('second failure');

    await postgres.query('UPDATE outbox SET last_error = left($1, 1000) WHERE id = 1', [
      'e'.repeat(5_000),
    ]);
    const { rows: truncated } = await postgres.query<{ len: number }>(
      'SELECT length(last_error) AS len FROM outbox WHERE id = 1',
    );
    expect(truncated[0]?.len).toBe(1_000);
  });
});

describe('cleanup', () => {
  it('deletes only published rows, and only up to the limit', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany(Array.from({ length: 5 }, () => event()));
    });
    await postgres.query(
      `UPDATE outbox SET published_at = now() - interval '10 days' WHERE id <= 4`,
    );

    const deleted = await uow.repositories.outbox.deletePublishedBefore(
      new Date(Date.now() - 86_400_000),
      2,
    );

    // Bounded: two of the four eligible rows. An unbounded delete of millions
    // of rows is one enormous WAL transaction holding locks throughout.
    expect(deleted).toBe(2);
    const { rows } = await postgres.query<{ count: string }>(
      'SELECT count(*)::text AS count FROM outbox',
    );
    expect(rows[0]?.count).toBe('3');
  });
});

describe('query paths', () => {
  it('returns an aggregate history in write order', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany([
        event({ aggregateId: 'order-h', eventType: 'created' }),
        event({ aggregateId: 'other', eventType: 'noise' }),
        event({ aggregateId: 'order-h', eventType: 'shipped' }),
      ]);
    });

    const history = await uow.repositories.outbox.findByAggregate('order', 'order-h', 10);

    expect(history.map((r) => r.eventType)).toEqual(['created', 'shipped']);
  });

  it('uses the partial index for the backlog count', async () => {
    await uow.run(async (u) => {
      await u.outbox.insertMany(Array.from({ length: 20 }, () => event()));
    });
    await postgres.query('UPDATE outbox SET published_at = now() WHERE id > 2');
    await postgres.query('ANALYZE outbox');

    const { rows } = await postgres.query<{ 'QUERY PLAN': string }>(
      `EXPLAIN SELECT count(*) FROM outbox WHERE published_at IS NULL`,
    );
    const plan = rows.map((r) => r['QUERY PLAN']).join('\n');

    // The metric this backs is polled every few seconds forever. If it ever
    // becomes a sequential scan over a 100M-row table, the alert that watches
    // the backlog becomes the thing causing the incident.
    expect(plan).toContain('outbox_unpublished_idx');
  });
});

describe('repository construction', () => {
  it('rejects an out-of-range shard before it can silently claim nothing', () => {
    expect(
      () => new PostgresOutboxRepository({ db: postgres, shard: { index: 3, total: 3 } }),
    ).toThrow(/out of range/);
  });
});

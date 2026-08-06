/**
 * Outbox repository — PostgreSQL implementation.
 *
 * Every method takes its `Queryable` from the constructor rather than from a
 * parameter, so an instance is bound to exactly one transaction (or to the
 * pool). That is what makes `uow.run(async (u) => { ...; u.outbox.insert(e) })`
 * atomic without threading a transaction object through every call — see
 * `unit-of-work.ts`.
 */

import type {
  NewOutboxEvent,
  OutboxRecord,
  OutboxRepository,
  PublishOutcome,
} from '@platform/domain';
import type { Queryable } from '../postgres.js';

/**
 * Postgres' wire protocol length-prefixes the parameter count as an **int16**,
 * so 65535 is a hard protocol limit, not a tunable. Exceeding it fails with a
 * confusing "bind message supplies N parameters" error that does not mention
 * the ceiling.
 */
const MAX_BIND_PARAMS = 65_535;

const INSERT_COLUMNS = [
  'event_id',
  'aggregate_type',
  'aggregate_id',
  'event_type',
  'topic',
  'partition_key',
  'payload',
  'headers',
  'correlation_id',
  'tenant_id',
] as const;

const MAX_ROWS_PER_INSERT = Math.floor(MAX_BIND_PARAMS / INSERT_COLUMNS.length);

const SELECT_COLUMNS = `
  id, event_id, aggregate_type, aggregate_id, event_type, topic,
  partition_key, payload, headers, created_at, attempts,
  correlation_id, tenant_id
`;

/**
 * `pg` returns every column as a driver-decoded value; these are the raw shapes
 * before mapping.
 *
 * It extends `Record<string, unknown>` because `Queryable.query<T>` constrains
 * `T` to that, and an interface — unlike a type alias — has no implicit index
 * signature. Extending explicitly is more honest than the alias was: a database
 * row genuinely *is* a bag of columns, and the driver will happily hand back one
 * this shape does not mention.
 */
interface OutboxRow extends Record<string, unknown> {
  /** int8 arrives as a string — see `OutboxRecord.id`. */
  id: string;
  event_id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  topic: string;
  partition_key: string | null;
  payload: Record<string, unknown>;
  headers: Record<string, string>;
  created_at: Date;
  attempts: number;
  correlation_id: string | null;
  tenant_id: string | null;
}

/**
 * Splits the unpublished backlog across N relay instances.
 *
 * ── The ordering problem this solves ───────────────────────────────────────
 * `FOR UPDATE SKIP LOCKED` is what lets N relays drain one table without
 * contending — but it costs global ordering. Relay 1 can claim rows 100–199,
 * relay 2 claims 200–299, and if relay 1 is slow, row 250 is published before
 * row 150. For unrelated events that is fine: Kafka only orders within a
 * partition anyway.
 *
 * It is **not** fine for two events about the same aggregate, which share a
 * partition key and therefore a partition, and which consumers will apply in
 * the order they arrive. `order.updated` landing before `order.created` is a
 * real bug and this is exactly how it happens.
 *
 * Sharding on `aggregate_id` fixes it: every event for one aggregate is claimed
 * by the same relay, which publishes them in `id` order. Ordering is preserved
 * where it matters and nowhere else, which is the correct scope.
 */
export interface RelayShard {
  /** 0-based index of this relay instance. */
  readonly index: number;
  /** Total number of relay instances. */
  readonly total: number;
}

export interface PostgresOutboxRepositoryOptions {
  readonly db: Queryable;
  /**
   * Omit for a single relay, which needs no sharding and keeps global order for
   * free. Supply it only when scaling past one instance — and note that
   * changing `total` while events are in flight can reorder events for an
   * aggregate that moves between shards. Drain the outbox before rescaling.
   */
  readonly shard?: RelayShard | undefined;
}

export class PostgresOutboxRepository implements OutboxRepository {
  readonly #db: Queryable;
  readonly #shard: RelayShard | undefined;

  constructor(options: PostgresOutboxRepositoryOptions) {
    this.#db = options.db;
    this.#shard = options.shard;

    if (this.#shard) {
      const { index, total } = this.#shard;
      // Validated at construction, not at query time. A relay misconfigured
      // with index 4 of 4 would otherwise start cleanly and simply never claim
      // anything — a silent stall that looks exactly like an empty backlog.
      if (!Number.isInteger(index) || !Number.isInteger(total) || total < 1) {
        throw new RangeError(`Relay shard must be integers with total >= 1, got ${index}/${total}`);
      }
      if (index < 0 || index >= total) {
        throw new RangeError(
          `Relay shard index ${index} is out of range for ${total} shards (expected 0..${total - 1}). ` +
            `A shard that matches no rows never claims work and is indistinguishable from an idle relay.`,
        );
      }
    }
  }

  public async insert(event: NewOutboxEvent): Promise<bigint> {
    const [id] = await this.insertMany([event]);
    if (id === undefined) {
      // Unreachable — RETURNING on a single-row INSERT always yields a row.
      // Asserted rather than non-null-asserted so a future change that makes it
      // reachable fails with a sentence instead of a TypeError.
      throw new Error('outbox insert returned no id');
    }
    return id;
  }

  public async insertMany(events: readonly NewOutboxEvent[]): Promise<bigint[]> {
    if (events.length === 0) return [];

    if (events.length > MAX_ROWS_PER_INSERT) {
      throw new RangeError(
        `Cannot insert ${events.length} outbox rows in one statement: ` +
          `${INSERT_COLUMNS.length} columns × ${events.length} rows exceeds Postgres' ` +
          `${MAX_BIND_PARAMS}-parameter protocol limit. Chunk at ${MAX_ROWS_PER_INSERT}.`,
      );
    }

    const params: unknown[] = [];
    const tuples = events.map((event, row) => {
      const base = row * INSERT_COLUMNS.length;
      params.push(
        event.eventId,
        event.aggregateType,
        event.aggregateId,
        event.eventType,
        event.topic,
        event.partitionKey ?? null,
        // Serialised here rather than passed as an object: `pg` would infer the
        // parameter type from the value and can bind a plain object as `json`
        // in some paths. Explicit `::jsonb` casts below make the type a
        // property of the statement instead of a property of the driver's
        // inference, which is the kind of thing that changes across minor
        // driver versions.
        JSON.stringify(event.payload),
        JSON.stringify(event.headers ?? {}),
        event.correlationId ?? null,
        event.tenantId ?? null,
      );
      return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7}::jsonb, $${base + 8}::jsonb, $${base + 9}, $${base + 10})`;
    });

    // ON CONFLICT DO NOTHING on the event_id unique index. A caller retrying a
    // transaction after a serialisation failure would otherwise hit 23505 and
    // turn a recoverable retry into a permanent DuplicateEventError. Rows that
    // conflict are simply absent from RETURNING — which is why the caller gets
    // back "the ids that were written", not "one id per input".
    const { rows } = await this.#db.query<{ id: string }>(
      `INSERT INTO outbox (${INSERT_COLUMNS.join(', ')})
       VALUES ${tuples.join(', ')}
       ON CONFLICT (event_id) DO NOTHING
       RETURNING id`,
      params,
    );

    return rows.map((r) => BigInt(r.id));
  }

  public async claimBatch(limit: number): Promise<OutboxRecord[]> {
    // ORDER BY id, then LIMIT, then SKIP LOCKED. The order of those clauses is
    // load-bearing: Postgres applies SKIP LOCKED while scanning, so the query
    // returns the first `limit` *unlocked* rows in id order rather than
    // returning fewer rows because some of the first `limit` were locked.
    // The shard predicate is bound, not interpolated. The values are integers
    // from configuration and could not carry an injection today — but "this
    // input is trusted" is a claim about the whole call graph, and it stops
    // being true the moment someone exposes shard count on an admin endpoint.
    // Binding costs nothing and removes the need to keep making that claim.
    //
    // `hashtext` is stable within a Postgres major version but not guaranteed
    // across them. That is acceptable: the assignment only has to hold for the
    // lifetime of a running backlog, and a major upgrade already requires a
    // drain. The mask to 31 bits is not cosmetic — `hashtext` returns a signed
    // int4, and `-3 % 4` is `-3` in Postgres, so an unmasked predicate would
    // match no shard for roughly half of all aggregate ids.
    const shardParams = this.#shard ? [this.#shard.total, this.#shard.index] : [];
    const shardPredicate = this.#shard ? `AND (hashtext(aggregate_id) & 2147483647) % $2 = $3` : '';

    const { rows } = await this.#db.query<OutboxRow>(
      `SELECT ${SELECT_COLUMNS}
         FROM outbox
        WHERE published_at IS NULL
          ${shardPredicate}
        ORDER BY id
        LIMIT $1
        FOR UPDATE SKIP LOCKED`,
      [limit, ...shardParams],
    );

    return rows.map(toRecord);
  }

  public async markPublished(outcomes: readonly PublishOutcome[]): Promise<void> {
    if (outcomes.length === 0) return;

    // One statement via unnest rather than N updates. At a batch size of 500,
    // the difference is 1 round trip versus 500 — and 500 round trips at 0.5ms
    // each is a quarter of a second of the relay doing nothing but waiting.
    //
    // The column aliases are `part` and `off` because `offset` is a reserved
    // word in Postgres and `u(id, partition, offset)` is a syntax error.
    await this.#db.query(
      `UPDATE outbox AS o
          SET published_at = now(),
              published_partition = u.part,
              published_offset = u.off
         FROM unnest($1::bigint[], $2::int[], $3::bigint[]) AS u(id, part, off)
        WHERE o.id = u.id`,
      [
        // bigint arrays are bound as strings: JS numbers cannot hold the range
        // and `pg` has no BigInt serialiser by default.
        outcomes.map((o) => o.id.toString()),
        outcomes.map((o) => o.partition),
        outcomes.map((o) => o.offset.toString()),
      ],
    );
  }

  public async markFailed(id: bigint, error: string): Promise<void> {
    await this.#db.query(
      `UPDATE outbox
          SET attempts = attempts + 1,
              -- Truncated: last_error is unbounded text and a driver stack
              -- trace can be tens of kilobytes. A pathological error on a
              -- million rows is a table-sized problem, and nobody reads past
              -- the first line anyway.
              last_error = left($2, 1000)
        WHERE id = $1`,
      [id.toString(), error],
    );
  }

  public async deletePublishedBefore(cutoff: Date, limit: number): Promise<number> {
    // Bounded by `limit` and driven by a subquery rather than a single
    // unbounded DELETE. An unbounded delete of millions of rows holds locks for
    // its whole duration, generates one enormous WAL transaction, and cannot be
    // interrupted without losing all of it. A bounded loop is interruptible and
    // leaves autovacuum room to keep up.
    const { rowCount } = await this.#db.query(
      `DELETE FROM outbox
        WHERE id IN (
          SELECT id FROM outbox
           WHERE published_at IS NOT NULL
             AND published_at < $1
           ORDER BY published_at
           LIMIT $2
        )`,
      [cutoff, limit],
    );
    return rowCount;
  }

  public async countUnpublished(): Promise<number> {
    // COUNT(*) with the partial index (outbox_unpublished_idx) is an
    // index-only scan over the *backlog*, not the table — so it stays fast at
    // 100M published rows as long as the backlog is small. If the backlog is
    // large this query gets slow, which is precisely when it is being polled
    // most; at that point the metric should come from
    // `max(id) - max(id where published)` instead.
    const { rows } = await this.#db.query<{ count: string }>(
      `SELECT count(*)::text AS count FROM outbox WHERE published_at IS NULL`,
    );
    return Number(rows[0]?.count ?? 0);
  }

  public async findByAggregate(
    aggregateType: string,
    aggregateId: string,
    limit: number,
  ): Promise<OutboxRecord[]> {
    // Served by outbox_aggregate_idx (aggregate_type, aggregate_id, id) —
    // the trailing `id` means the ORDER BY is free rather than a sort.
    const { rows } = await this.#db.query<OutboxRow>(
      `SELECT ${SELECT_COLUMNS}
         FROM outbox
        WHERE aggregate_type = $1 AND aggregate_id = $2
        ORDER BY id
        LIMIT $3`,
      [aggregateType, aggregateId, limit],
    );
    return rows.map(toRecord);
  }
}

function toRecord(row: OutboxRow): OutboxRecord {
  return {
    id: BigInt(row.id),
    eventId: row.event_id,
    aggregateType: row.aggregate_type,
    aggregateId: row.aggregate_id,
    eventType: row.event_type,
    topic: row.topic,
    partitionKey: row.partition_key,
    payload: row.payload,
    headers: row.headers,
    createdAt: row.created_at,
    attempts: row.attempts,
    correlationId: row.correlation_id,
    tenantId: row.tenant_id,
  };
}

/**
 * The outbox relay.
 *
 * This is where the dual-write problem is actually solved. Chapter 3 built the
 * table and the claim query; Chapter 4 built the producer. The relay is the
 * loop that connects them, and every interesting decision in it is about what
 * happens when something fails halfway.
 *
 * ── The loop ───────────────────────────────────────────────────────────────
 *
 *   BEGIN
 *     claim  = SELECT ... WHERE published_at IS NULL
 *                ORDER BY id LIMIT n FOR UPDATE SKIP LOCKED
 *     results = kafka.publishBatch(claim)          ← outside? no: inside. See below.
 *     UPDATE outbox SET published_at = now(), ...
 *   COMMIT
 *
 * ── Publish inside the transaction, not after it ───────────────────────────
 * The instinct is to keep I/O out of a transaction, and normally that instinct
 * is right — a transaction holding row locks across a network call is a
 * classic way to exhaust a pool.
 *
 * Here it is wrong, and the reason is the claim itself. `SKIP LOCKED` works
 * through row locks that exist only for the life of the transaction. Commit
 * before publishing and the rows are unlocked while still unpublished, so a
 * second relay instance claims the same rows and publishes them again. The
 * lock IS the claim; releasing it early releases the claim.
 *
 * The cost is real and bounded: a batch holds its rows for roughly the publish
 * latency (~15 ms at `acks=all`), and `statement_timeout` plus
 * `idle_in_transaction_session_timeout` (postgres.ts) cap the damage if the
 * broker hangs. Batch size is therefore a pool-pressure decision as much as a
 * throughput one.
 *
 * ── Publish, then mark. Never mark, then publish ───────────────────────────
 * A crash between publish and mark republishes on the next tick — a duplicate,
 * which consumers already deduplicate on `idempotencyKey` (ADR-0008).
 * A crash between mark and publish loses the event permanently, with nothing
 * left to detect it.
 *
 * At-least-once is chosen here, in this ordering, and nowhere else.
 *
 * ── Why a lock is NOT used to elect a single relay ─────────────────────────
 * The obvious design is a leader election so only one relay runs. It is worse:
 * a single leader is a throughput ceiling and a failover gap. `SKIP LOCKED`
 * already lets N relays run concurrently without duplicating, and relay
 * sharding (ADR-0010, `RelayShard`) preserves per-aggregate ordering. The
 * distributed lock from Chapter 3 is for the *cleanup* job, which genuinely
 * should not run N times.
 */

import {
  isRetryable,
  toPlatformError,
  type Clock,
  type Logger,
  type PlatformError,
} from '@platform/core';
import type { EventEnvelope, OutboxRecord } from '@platform/domain';
import type { KafkaProducer } from '@platform/kafka';
import type { UnitOfWorkRunner } from '@platform/persistence';

export interface OutboxRelayOptions {
  readonly uow: UnitOfWorkRunner;
  readonly producer: KafkaProducer;
  readonly logger: Logger;
  readonly clock: Clock;

  /**
   * Rows per tick.
   *
   * Bigger batches amortise the `acks=all` round trip, and hold row locks and
   * a pooled connection for longer. 500 × ~1 KB is ~500 KB per request, well
   * under the 2 MB `max.message.bytes` ceiling from `topics.config.ts`.
   */
  readonly batchSize?: number | undefined;

  /**
   * Pause when a tick found nothing.
   *
   * This is a latency/load trade-off with no correct answer: 200 ms means an
   * event waits up to 200 ms before publication and the relay issues five
   * queries per second per shard against an empty table. Postgres' `LISTEN`
   * would remove the polling entirely, and is deliberately not used — see the
   * note on `idleDelayMs` below.
   */
  readonly idleDelayMs?: number | undefined;

  /** Backoff after a failed tick, before jitter. */
  readonly errorDelayMs?: number | undefined;
}

export interface RelayTickResult {
  readonly claimed: number;
  readonly published: number;
  readonly durationMs: number;
}

export class OutboxRelay {
  readonly #uow: UnitOfWorkRunner;
  readonly #producer: KafkaProducer;
  readonly #logger: Logger;
  readonly #clock: Clock;
  readonly #batchSize: number;
  readonly #idleDelayMs: number;
  readonly #errorDelayMs: number;

  #running = false;
  #consecutiveFailures = 0;
  #loop: Promise<void> | undefined;
  #abort: AbortController | undefined;

  constructor(options: OutboxRelayOptions) {
    this.#uow = options.uow;
    this.#producer = options.producer;
    this.#logger = options.logger.child({ component: 'outbox-relay' });
    this.#clock = options.clock;
    this.#batchSize = options.batchSize ?? 500;
    this.#idleDelayMs = options.idleDelayMs ?? 200;
    this.#errorDelayMs = options.errorDelayMs ?? 1_000;
  }

  /**
   * One tick: claim, publish, mark — all in one transaction.
   *
   * Public and returning a result so tests can drive it deterministically
   * rather than starting the loop and racing a timer. A relay whose only entry
   * point is `start()` can only be tested by waiting, and tests that wait are
   * tests that flake.
   */
  public async tick(): Promise<RelayTickResult> {
    const startedAt = this.#clock.monotonic();

    const { claimed, published } = await this.#uow.run(async (u) => {
      const batch = await u.outbox.claimBatch(this.#batchSize);
      if (batch.length === 0) return { claimed: 0, published: 0 };

      const grouped = groupByTopic(batch);
      let publishedCount = 0;

      for (const [topic, records] of grouped) {
        const results = await this.#producer.publishBatch(
          topic,
          records.map((record) => toEnvelope(record)),
        );

        // Zipped by index. KafkaJS returns one metadata record per message in
        // submission order, which is the only correlation available — there is
        // no id echoed back. If that ever stops holding, offsets are recorded
        // against the wrong events, so the invariant is asserted rather than
        // assumed.
        if (results.length !== records.length) {
          throw new Error(
            `Broker returned ${results.length} results for ${records.length} messages on ` +
              `"${topic}"; offsets cannot be attributed and this batch is being rolled back.`,
          );
        }

        await u.outbox.markPublished(
          records.map((record, index) => ({
            id: record.id,
            partition: results[index]?.partition ?? -1,
            offset: results[index]?.offset ?? 0n,
          })),
        );
        publishedCount += records.length;
      }

      return { claimed: batch.length, published: publishedCount };
    });

    return {
      claimed,
      published,
      durationMs: Number(this.#clock.monotonic() - startedAt) / 1_000_000,
    };
  }

  /** Start the loop. Returns immediately; the loop runs until `stop()`. */
  public start(): void {
    if (this.#running) return;
    this.#running = true;
    this.#abort = new AbortController();
    this.#loop = this.#run();
  }

  /**
   * Stop after the current tick finishes.
   *
   * Registered as a lifecycle **drain** hook, not a close hook: the relay must
   * stop claiming before the producer and the pool are torn down, or the last
   * tick publishes into a closed producer and its rows stay claimed until the
   * transaction times out.
   */
  public async stop(): Promise<void> {
    if (!this.#running) return;
    this.#running = false;
    this.#abort?.abort();
    // Awaiting the loop is the whole point: returning early would let the
    // lifecycle close the producer while a publish is still in flight.
    await this.#loop;
    this.#loop = undefined;
  }

  public get running(): boolean {
    return this.#running;
  }

  async #run(): Promise<void> {
    while (this.#running) {
      try {
        const result = await this.tick();
        this.#consecutiveFailures = 0;

        if (result.claimed === 0) {
          await this.#sleep(this.#idleDelayMs);
          continue;
        }

        // Spread, not `result`. `LogFields` is `Record<string, unknown>`, and an
        // interface has no implicit index signature — only a fresh object
        // literal is assignable. The same constraint that forced `OutboxRow` to
        // extend Record, seen from the other side.
        this.#logger.debug('relayed batch', { ...result });

        // No pause after a full batch: a full batch means there is more work,
        // and sleeping would cap throughput at batchSize/idleDelay regardless
        // of load. The loop only rests when the table is empty.
        if (result.claimed < this.#batchSize) {
          await this.#sleep(this.#idleDelayMs);
        }
      } catch (error: unknown) {
        await this.#onFailure(error);
      }
    }
  }

  async #onFailure(error: unknown): Promise<void> {
    const platformError = toPlatformError(error);
    this.#consecutiveFailures++;

    // Retryable failures are ordinary — a broker restart produces a burst of
    // them. Logging each at error level pages somebody for a self-healing
    // condition, so severity escalates with persistence instead.
    const persistent = this.#consecutiveFailures >= 5;
    if (persistent || !isRetryable(platformError)) {
      this.#logger.error('relay tick failed', platformError, {
        consecutiveFailures: this.#consecutiveFailures,
        // The number that matters on the alert: events are written and not
        // published, and the backlog is growing for as long as this is true.
        retryable: platformError.retryable,
      });
    } else {
      this.#logger.warn('relay tick failed, retrying', {
        consecutiveFailures: this.#consecutiveFailures,
        code: platformError.code,
      });
    }

    await this.#sleep(this.#backoffMs(platformError));
  }

  /**
   * Exponential backoff, capped, with jitter.
   *
   * The cap matters more than the growth: without it, a relay that has been
   * failing for an hour sleeps for an hour and does not notice recovery. 30 s
   * bounds how stale the backlog can get after the cluster comes back.
   *
   * Jitter matters because every relay in the fleet fails at the same instant
   * when a broker dies, and without it they all retry in lockstep and hit the
   * recovering broker as a synchronised wave.
   */
  #backoffMs(error: PlatformError): number {
    // A permanent error will not fix itself, so there is no point retrying
    // quickly — but the loop must keep running, because the *next* batch may
    // be fine and stalling entirely on one bad batch is a self-inflicted
    // outage.
    const base = isRetryable(error) ? this.#errorDelayMs : this.#errorDelayMs * 5;
    const exponential = Math.min(base * 2 ** (this.#consecutiveFailures - 1), 30_000);
    return exponential * (0.5 + Math.random() * 0.5);
  }

  async #sleep(ms: number): Promise<void> {
    try {
      await this.#clock.sleep(ms, this.#abort?.signal);
    } catch {
      // Aborted by stop(). Swallowed deliberately: shutdown is not a failure,
      // and letting it propagate would log an error on every clean deploy.
    }
  }
}

/**
 * Group a claimed batch by topic.
 *
 * KafkaJS's `send` takes one topic per call. `sendBatch` accepts several, and
 * is deliberately not used: it does not report per-topic failures distinctly,
 * so a partial failure becomes unattributable — and this loop needs to know
 * exactly which records were published to mark the right ones.
 */
function groupByTopic(records: readonly OutboxRecord[]): Map<string, OutboxRecord[]> {
  const grouped = new Map<string, OutboxRecord[]>();
  for (const record of records) {
    const existing = grouped.get(record.topic);
    if (existing) existing.push(record);
    else grouped.set(record.topic, [record]);
  }
  return grouped;
}

/**
 * Rebuild the envelope from the stored row.
 *
 * The envelope is reconstructed rather than stored whole because the outbox
 * columns are queryable — support asks "every event for order 42", and that is
 * an index lookup on columns, not a jsonb scan. The cost is this mapping
 * function, which is the same trade ADR-0010 accepted for repositories.
 */
function toEnvelope(record: OutboxRecord): EventEnvelope {
  const headers = record.headers;
  return {
    eventId: record.eventId,
    eventType: record.eventType,
    eventVersion: Number(headers['eventVersion'] ?? 1),
    aggregateType: record.aggregateType,
    aggregateId: record.aggregateId,
    // The stored fact time, not now. Replay and retention both key off this,
    // so publishing with a fresh timestamp would silently reset an event's age
    // every time the relay retried it.
    occurredAt: headers['occurredAt'] ?? record.createdAt.toISOString(),
    recordedAt: record.createdAt.toISOString(),
    producer: headers['producer'] ?? 'unknown',
    correlationId: record.correlationId ?? record.eventId,
    idempotencyKey: headers['idempotencyKey'] ?? record.eventId,
    payload: record.payload,
    ...(record.tenantId !== null ? { tenantId: record.tenantId } : {}),
  };
}

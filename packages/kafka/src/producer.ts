/**
 * The Kafka producer.
 *
 * Every durability guarantee this platform makes on the write path is a
 * setting in this file, so each one carries the failure it prevents.
 *
 * ── acks = -1 (all) ────────────────────────────────────────────────────────
 * The leader waits until every in-sync replica has the record. With
 * `min.insync.replicas = 2` (ADR-0001) that means two copies exist before the
 * producer is told the write succeeded.
 *
 *   acks=1 would ack once the leader alone has it. Kill that broker in the
 *   next few milliseconds — a rolling restart, a spot instance reclaim — and
 *   the record is gone, with the producer having been told it succeeded.
 *
 * The cost is latency: p99 roughly 8–15 ms instead of 2–4 ms. For an event
 * platform whose entire value is that events are not lost, that is not a
 * trade-off, it is the product.
 *
 * ── idempotent = true ──────────────────────────────────────────────────────
 * The producer gets a producer id and a per-partition sequence number, so the
 * broker can recognise and discard a retry of a batch it already wrote.
 *
 * Without it, the ambiguous-timeout case is unresolvable: the broker may have
 * written the batch and lost the response, so retrying duplicates and not
 * retrying loses. With it, retrying is simply correct — which is what lets
 * `errors.ts` classify `RequestTimedOut` as retryable. The two decisions are
 * one decision.
 *
 * ── maxInFlightRequests = 5, not 1 ─────────────────────────────────────────
 * This is the setting people get wrong in both directions.
 *
 *   Without idempotence, >1 in flight can REORDER on retry: batch 1 fails,
 *   batch 2 succeeds, batch 1 is retried and lands second. Ordering within
 *   the partition is broken, which for an event-sourced consumer is silent
 *   corruption. So the classic advice is max.in.flight = 1.
 *
 *   WITH idempotence, the broker tracks sequence numbers and refuses to write
 *   a batch out of order — it holds up to 5 in flight per partition and
 *   orders them itself. So 5 is safe AND roughly 3–4× the throughput of 1,
 *   because the producer is not stalled waiting for each round trip.
 *
 * 5 is the maximum the broker supports for an idempotent producer. Setting 6
 * silently disables the ordering guarantee, which is a genuinely dangerous
 * default in some clients.
 *
 * ── Compression: gzip, and an honest divergence from ADR-0004 ──────────────
 * ADR-0004 states lz4. That was written before this file, and it was wrong on
 * one point: **KafkaJS ships only gzip built in.** Snappy and LZ4 require
 * separate codec packages, which reintroduces exactly the install friction
 * that ADR-0004 chose KafkaJS to avoid.
 *
 * So the default here is gzip and the setting is configurable. Gzip costs more
 * CPU for a better ratio; at 10K events/sec on 1 KB events that is a few
 * percent of a core, which is affordable. If produce-side CPU ever becomes the
 * constraint, adding `kafkajs-lz4` is a one-line codec registration — and
 * ADR-0004 should be amended rather than quietly ignored.
 */

import { CompressionTypes, type Kafka, type Producer, type RecordMetadata } from 'kafkajs';
import { correlationId, type Clock, type Config, type Logger } from '@platform/core';
import { partitionKeyFor, type EventEnvelope } from '@platform/domain';
import { envelopeToHeaders } from './headers.js';
import { translateKafkaError } from './errors.js';

/** Where a record landed. Recorded in the outbox so support can trace an event. */
export interface PublishResult {
  readonly topic: string;
  readonly partition: number;
  readonly offset: bigint;
}

export interface KafkaProducerOptions {
  readonly kafka: Kafka;
  readonly config: Config;
  readonly logger: Logger;
  readonly clock: Clock;
  /** Defaults to gzip. See the note above before changing it. */
  readonly compression?: CompressionTypes | undefined;
  /**
   * Per-send broker timeout.
   *
   * Must be comfortably below the caller's own deadline, or the caller gives
   * up while the producer is still waiting and the ambiguous-write window
   * opens on the *caller's* side, where there is no idempotence to close it.
   */
  readonly sendTimeoutMs?: number | undefined;
}

export class KafkaProducer {
  readonly #producer: Producer;
  readonly #logger: Logger;
  readonly #clock: Clock;
  readonly #compression: CompressionTypes;
  readonly #sendTimeoutMs: number;
  #connected = false;

  constructor(options: KafkaProducerOptions) {
    this.#logger = options.logger.child({ component: 'kafka-producer' });
    this.#clock = options.clock;
    this.#compression = options.compression ?? CompressionTypes.GZIP;
    this.#sendTimeoutMs = options.sendTimeoutMs ?? 30_000;

    this.#producer = options.kafka.producer({
      // See the header. These three are the durability contract.
      idempotent: true,
      maxInFlightRequests: 5,

      // Off, matching `auto.create.topics.enable=false` on the brokers
      // (ADR-0001). A topic created implicitly by a producer gets broker
      // defaults — replication factor 1 — which is how a critical topic ends
      // up with no redundancy and nobody notices until a broker dies.
      allowAutoTopicCreation: false,

      // Deliberately no `transactionalId`. Transactions are reserved for the
      // retry engine and replay service (ADR-0008), whose effects are
      // Kafka-only; a transactional producer here would add coordinator
      // round trips to every publish for a guarantee that stops at the first
      // Postgres write anyway.
      retry: {
        // Client-side retries of retriable broker errors. Bounded, because an
        // unbounded retry hides a cluster outage as latency instead of
        // surfacing it as an error the caller can act on.
        retries: 5,
        initialRetryTime: 100,
        // Exponential with jitter, applied by KafkaJS. Jitter matters here:
        // without it every producer in the fleet retries in lockstep after a
        // broker blip and the recovering broker is hit by a synchronised wave.
        factor: 2,
        maxRetryTime: 8_000,
      },
    });

    this.#producer.on('producer.disconnect', () => {
      this.#connected = false;
      this.#logger.warn('producer disconnected');
    });
  }

  public async connect(): Promise<void> {
    try {
      await this.#producer.connect();
      this.#connected = true;
    } catch (error: unknown) {
      throw translateKafkaError(error);
    }
  }

  /** Publish one event. Returns where it landed. */
  public async publish(topic: string, envelope: EventEnvelope): Promise<PublishResult> {
    const [result] = await this.publishBatch(topic, [envelope]);
    if (result === undefined) {
      // Unreachable: one message in, one metadata record out. Asserted with a
      // sentence so a future change that breaks the invariant does not surface
      // as a TypeError on `undefined`.
      throw new Error('publish returned no record metadata');
    }
    return result;
  }

  /**
   * Publish many events to one topic in a single request.
   *
   * ── Why batching is not merely an optimisation ─────────────────────────────
   * Each `send` is a round trip whose latency is dominated by `acks=all`
   * replication, not by payload size. Sending 500 events individually costs
   * 500 × ~10 ms of waiting; sending them as one batch costs ~15 ms. That is
   * the difference between an outbox relay that keeps up and one that falls
   * permanently behind — the relay's throughput ceiling is set here.
   *
   * The batch is not atomic. Kafka splits it per partition, and one partition
   * can fail while others succeed; KafkaJS surfaces that as a rejection for
   * the whole call. Callers must therefore treat a failed batch as "some
   * unknown subset may have been written" — which is safe here only because
   * consumers deduplicate on `idempotencyKey` (ADR-0008).
   */
  public async publishBatch(
    topic: string,
    envelopes: readonly EventEnvelope[],
  ): Promise<PublishResult[]> {
    if (envelopes.length === 0) return [];
    this.#assertConnected();

    const startedAt = this.#clock.monotonic();

    try {
      const metadata = await this.#producer.send({
        topic,
        // -1 is `acks=all`. Written as the literal rather than a named
        // constant because KafkaJS has no enum for it and 1 vs -1 is exactly
        // the durability difference described in the header.
        acks: -1,
        compression: this.#compression,
        timeout: this.#sendTimeoutMs,
        messages: envelopes.map((envelope) => ({
          // The partition key, NOT the event id. Keying by aggregate is what
          // preserves per-aggregate ordering; see `partitionKeyFor`.
          key: partitionKeyFor(envelope),
          value: JSON.stringify(envelope),
          headers: envelopeToHeaders(envelope),
          // Kafka's own record timestamp is set to when the fact happened, so
          // that log retention and time-based seeks (used by replay, Chapter
          // 10) operate on business time rather than on publication time.
          timestamp: String(Date.parse(envelope.occurredAt)),
        })),
      });

      const results = metadata.map((record) => toResult(topic, record));

      this.#logger.debug('published batch', {
        topic,
        count: envelopes.length,
        partitions: [...new Set(results.map((r) => r.partition))].length,
        durationMs: Number(this.#clock.monotonic() - startedAt) / 1_000_000,
        correlationId: correlationId(),
      });

      return results;
    } catch (error: unknown) {
      const translated = translateKafkaError(error);
      this.#logger.error('publish failed', translated, {
        topic,
        count: envelopes.length,
        retryable: translated.retryable,
      });
      throw translated;
    }
  }

  /**
   * Flush and close. Registered as a lifecycle **close** hook, not a drain
   * hook: draining stops accepting new work, and the producer must stay usable
   * until every in-flight request has been handed to it.
   *
   * KafkaJS's `disconnect` waits for outstanding sends, so this is where the
   * "no acknowledged-but-unsent events" guarantee is actually kept.
   */
  public async disconnect(): Promise<void> {
    if (!this.#connected) return;
    try {
      await this.#producer.disconnect();
    } finally {
      this.#connected = false;
    }
  }

  public get connected(): boolean {
    return this.#connected;
  }

  #assertConnected(): void {
    if (!this.#connected) {
      // A clear error rather than KafkaJS's lazy auto-connect. Auto-connecting
      // on first send means connection failures surface inside a business
      // transaction instead of at startup, where a failing readiness probe
      // would have stopped the deploy.
      throw translateKafkaError(
        new Error('The producer is disconnected — connect() must be called during startup'),
      );
    }
  }
}

function toResult(topic: string, record: RecordMetadata): PublishResult {
  return {
    topic: record.topicName === '' ? topic : record.topicName,
    partition: record.partition,
    // KafkaJS returns offsets as strings for the same reason `pg` returns
    // int8 as a string: Kafka offsets are int64 and exceed Number's exact
    // range on a long-lived high-volume partition.
    offset: BigInt(record.baseOffset ?? '0'),
  };
}

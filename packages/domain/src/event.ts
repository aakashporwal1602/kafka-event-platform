/**
 * The event envelope — this platform's central contract.
 *
 * ── Why an envelope at all ─────────────────────────────────────────────────
 * The alternative is publishing the business payload directly: `{ orderId,
 * total, currency }` on a topic named `orders`. It works on day one and fails
 * permanently on day two, because there is nowhere to put the answers to
 * questions every operator eventually asks:
 *
 *   • Which request produced this?           → correlationId
 *   • Which event produced this event?       → causationId
 *   • Is this the same fact I already saw?   → idempotencyKey
 *   • When did it actually happen?           → occurredAt
 *   • Which shape is this payload?           → eventType + eventVersion
 *
 * Adding those to the payload later means changing every producer and every
 * consumer at once, which in a system with nine services is not a migration,
 * it is a rewrite. Separating envelope from payload means metadata evolves
 * independently of business schemas — and it is the reason the retry engine,
 * the DLQ and the replay service can be written **generically**: they operate
 * on envelopes and never need to understand a single business event.
 *
 * ── The two timestamps ─────────────────────────────────────────────────────
 * `occurredAt` is when the fact happened in the business. `recordedAt` is when
 * the platform learned about it. They are usually milliseconds apart and
 * occasionally days:
 *
 *   A mobile client goes offline, the user places an order, the app syncs
 *   two days later. occurredAt is Tuesday; recordedAt is Thursday.
 *
 * A system with one timestamp has to choose which lie to tell. Reporting on
 * `recordedAt` puts Tuesday's revenue in Thursday's report; reporting on a
 * single field that is really `recordedAt` but named `timestamp` produces the
 * same error silently. Both are here, and every consumer has to decide which
 * one it means — which is the point.
 *
 * `recordedAt` is also the only one that is trustworthy: it comes from the
 * platform's clock. `occurredAt` comes from the producer, and a producer with
 * a skewed clock can claim anything, including the future.
 */

import { MalformedPayloadError } from '@platform/core';
import { createHash, randomUUID } from 'node:crypto';

/**
 * Event type grammar: `<aggregate>.<past-tense verb>`, lowercase, dot-separated.
 *
 * The past tense is not style. An event is a **fact that already happened** and
 * cannot be rejected; a command is a request that can. Naming an event
 * `order.place` invites a consumer to treat it as a command — to "handle" it by
 * deciding whether to allow it — and that consumer is now a hidden authority on
 * a decision the producer already made. `order.placed` cannot be read that way.
 *
 * Enforced with a regex rather than a convention document, because conventions
 * that are not checked are aspirations.
 */
const EVENT_TYPE_PATTERN = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?:\.[a-z][a-z0-9]*(?:-[a-z0-9]+)*)+$/;

/** Aggregate and tenant identifiers appear in Redis keys and Kafka headers. */
const IDENTIFIER_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,254}$/;

/**
 * Headers are a per-message cost paid on every broker, every replica and every
 * consumer. 2 KB of envelope on a 200-byte payload is a 10× amplification, and
 * at 10K events/sec that is 20 MB/sec of metadata.
 */
const MAX_ENVELOPE_STRING = 512;

export interface EventEnvelope<TPayload = unknown> {
  /**
   * Unique per **publication attempt**, not per fact.
   *
   * Deliberately distinct from `idempotencyKey`: the same fact republished by
   * a relay retry keeps its idempotency key and gets a new event id. That lets
   * a support query answer "how many times did we publish this?" — which is
   * unanswerable if the two are the same field.
   */
  readonly eventId: string;

  /** `order.placed`. See the grammar above. */
  readonly eventType: string;

  /**
   * Major version of the payload shape.
   *
   * ── Why this exists even though there is a schema registry (Chapter 6) ────
   * The registry enforces *compatibility* — it guarantees a v2 writer's data
   * can be read by a v1 reader under BACKWARD rules. It does not let a consumer
   * *decide* anything, because the schema id is an opaque integer resolved
   * during deserialisation, long after routing.
   *
   * `eventVersion` is in the envelope so a consumer can branch, route or refuse
   * **before** deserialising — which is exactly what is needed during a
   * breaking change, when v1 and v2 consumers run side by side for a week.
   */
  readonly eventVersion: number;

  readonly aggregateType: string;
  readonly aggregateId: string;

  /** When the fact happened, per the producer. ISO-8601 with offset. */
  readonly occurredAt: string;
  /** When the platform recorded it, per the platform's clock. ISO-8601 UTC. */
  readonly recordedAt: string;

  /** Which service published this. Answers "who do I page". */
  readonly producer: string;

  /** Ties every event, log line and span back to one originating request. */
  readonly correlationId: string;

  /**
   * The event that caused this one. Absent for events caused by a user action.
   *
   * correlationId groups; causationId **orders**. With both, an incident
   * timeline reconstructs into a tree rather than a flat list — which is the
   * difference between "these fifty events are related" and "this one caused
   * those three".
   */
  readonly causationId?: string | undefined;

  readonly tenantId?: string | undefined;

  /**
   * Deterministic from business meaning. See `idempotencyKeyFor`.
   *
   * This is what the consumer deduplicates on (ADR-0008), so it must be
   * identical across a relay retry, a replay and a redelivery.
   */
  readonly idempotencyKey: string;

  readonly payload: TPayload;
}

export interface NewEventInput<TPayload> {
  readonly eventType: string;
  readonly eventVersion: number;
  readonly aggregateType: string;
  readonly aggregateId: string;
  readonly payload: TPayload;
  readonly producer: string;
  readonly correlationId: string;
  /** Defaults to now. Supply it when the fact happened earlier than publication. */
  readonly occurredAt?: Date | undefined;
  readonly causationId?: string | undefined;
  readonly tenantId?: string | undefined;
  /**
   * Override the derived key.
   *
   * Only for events whose identity is not `(type, aggregate, version)` — for
   * example an idempotent HTTP request supplying its own `Idempotency-Key`
   * header. Deriving is the default because a caller-supplied key is a caller
   * that can get deduplication wrong.
   */
  readonly idempotencyKey?: string | undefined;
  /**
   * Monotonic version of the aggregate after this event.
   *
   * Feeds the derived idempotency key. Omit it only when the aggregate has no
   * version — and note the cost: without it, two genuinely distinct events of
   * the same type on the same aggregate collide, and the second is silently
   * discarded as a duplicate.
   */
  readonly aggregateVersion?: number | undefined;
}

/**
 * Build a validated envelope.
 *
 * Validation happens **here**, at construction, rather than at the broker
 * boundary. An invalid event caught at the call site has a stack trace pointing
 * at the code that built it; the same event caught during publish has a stack
 * trace pointing at the producer, which is never where the bug is.
 */
export function newEvent<TPayload>(
  input: NewEventInput<TPayload>,
  now: Date = new Date(),
): EventEnvelope<TPayload> {
  assertEventType(input.eventType);
  assertIdentifier(input.aggregateType, 'aggregateType');
  assertIdentifier(input.aggregateId, 'aggregateId');
  assertIdentifier(input.producer, 'producer');
  if (input.tenantId !== undefined) assertIdentifier(input.tenantId, 'tenantId');

  if (!Number.isInteger(input.eventVersion) || input.eventVersion < 1) {
    throw new MalformedPayloadError(
      `eventVersion must be a positive integer, got ${String(input.eventVersion)}`,
      { eventType: input.eventType },
    );
  }

  const occurredAt = input.occurredAt ?? now;
  if (Number.isNaN(occurredAt.getTime())) {
    throw new MalformedPayloadError('occurredAt is not a valid date', {
      eventType: input.eventType,
    });
  }

  return {
    eventId: randomUUID(),
    eventType: input.eventType,
    eventVersion: input.eventVersion,
    aggregateType: input.aggregateType,
    aggregateId: input.aggregateId,
    occurredAt: occurredAt.toISOString(),
    recordedAt: now.toISOString(),
    producer: input.producer,
    correlationId: input.correlationId,
    ...(input.causationId !== undefined ? { causationId: input.causationId } : {}),
    ...(input.tenantId !== undefined ? { tenantId: input.tenantId } : {}),
    idempotencyKey:
      input.idempotencyKey ??
      idempotencyKeyFor({
        eventType: input.eventType,
        aggregateType: input.aggregateType,
        aggregateId: input.aggregateId,
        ...(input.aggregateVersion !== undefined
          ? { aggregateVersion: input.aggregateVersion }
          : {}),
      }),
    payload: input.payload,
  };
}

export interface IdempotencyKeyInput {
  readonly eventType: string;
  readonly aggregateType: string;
  readonly aggregateId: string;
  readonly aggregateVersion?: number | undefined;
}

/**
 * Derive the deduplication key from **business meaning**.
 *
 * ── Why not partition:offset ───────────────────────────────────────────────
 * It is the obvious choice and it is wrong. A replay (Chapter 10) re-publishes
 * a historical event, which lands at a new offset — so the key differs, the
 * consumer sees a new event, and every side effect happens a second time. The
 * replay feature would silently corrupt data, and it would do so only for the
 * events somebody deliberately replayed.
 *
 * ── Why hashed rather than concatenated ────────────────────────────────────
 * The raw tuple is unbounded: aggregate ids are caller-supplied and a 200-byte
 * key is possible. Every key lives in Redis for 24 hours (`TTL.idempotency`),
 * and `redis-keys.ts` derives the memory cost from key length — a 16-byte hash
 * prefix instead of a ~60-byte tuple is a ~35% saving on a number already
 * measured in tens of gigabytes.
 *
 * 128 bits of SHA-256 gives a collision probability below 1e-18 at the volumes
 * in that sizing note. A collision would drop a real event, so the number
 * matters and is stated rather than assumed.
 */
export function idempotencyKeyFor(input: IdempotencyKeyInput): string {
  const parts = [
    input.eventType,
    input.aggregateType,
    input.aggregateId,
    // A marker rather than an empty string, so "no version" and "version 0"
    // are different inputs. Collapsed, they collide and one of two genuinely
    // distinct events is discarded as a duplicate.
    input.aggregateVersion === undefined ? 'v-' : `v${input.aggregateVersion}`,
  ];

  // Length-prefixed, not joined by a separator.
  //
  // `parts.join(':')` is the obvious encoding and it is unsafe: an identifier
  // containing the separator moves the boundary, so ("agg b", "c") and
  // ("agg", "b c") produce the same string and therefore the same key. One of
  // those two events is then silently dropped as a duplicate — the hardest
  // class of bug to notice, because nothing errors and nothing is logged.
  //
  // `assertIdentifier` blocks that at the envelope boundary, but this function
  // is exported and callable directly, and a hashing scheme must not depend on
  // a validation that lives in another function.
  const canonical = parts.map((part) => `${part.length}:${part}`).join('');

  return createHash('sha256').update(canonical).digest('hex').slice(0, 32);
}

/** True if `value` is a well-formed event type. Exported for gateway validation. */
export function isValidEventType(value: string): boolean {
  return value.length <= MAX_ENVELOPE_STRING && EVENT_TYPE_PATTERN.test(value);
}

function assertEventType(value: string): void {
  if (!isValidEventType(value)) {
    throw new MalformedPayloadError(
      `Invalid event type "${value}". Expected lowercase, dot-separated, ` +
        `past-tense — e.g. "order.placed", "payment.settlement-failed". ` +
        `An event names something that already happened; a name in the ` +
        `imperative reads as a command a consumer may think it can refuse.`,
      { eventType: value },
    );
  }
}

function assertIdentifier(value: string, field: string): void {
  if (!IDENTIFIER_PATTERN.test(value)) {
    throw new MalformedPayloadError(
      `Invalid ${field} "${value}". Expected 1–255 characters of ` +
        `[A-Za-z0-9._:-] starting alphanumeric. These values become Redis key ` +
        `segments and Kafka header values, so an unconstrained one can inject ` +
        `a separator and collide with another tenant's key.`,
      { field, value },
    );
  }
}

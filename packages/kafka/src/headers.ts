/**
 * Envelope ↔ Kafka headers.
 *
 * ── Why the envelope goes in headers AND in the value ──────────────────────
 * It looks like duplication. It is, and it is deliberate.
 *
 * Headers can be read **without deserialising the value**. That matters in
 * three places where deserialising is either impossible or wasteful:
 *
 *   1. **The DLQ.** A message reaches the DLQ precisely because something went
 *      wrong, and "the payload could not be deserialised" is one of the most
 *      common reasons. An operator still needs to know the event type, the
 *      aggregate and the correlation id — from a message whose body cannot be
 *      parsed. Headers survive that.
 *   2. **Routing and filtering.** The retry engine decides where a message
 *      goes next from its headers. Deserialising an Avro payload just to read
 *      a routing field would mean the retry path depends on the schema
 *      registry being up.
 *   3. **`kafka-console-consumer`.** At 3am, being able to see the event type
 *      without a schema-aware tool is worth more than the bytes it costs.
 *
 * The cost is real and bounded: roughly 250–400 bytes per message. On a 2 KB
 * payload that is 15%; on a 200-byte payload it is 150%, which is why event
 * payloads should not be tiny and why `event.ts` caps envelope string lengths.
 *
 * ── Naming ────────────────────────────────────────────────────────────────
 * `x-` prefixed and hyphenated, matching HTTP header convention rather than
 * camelCase, because these headers cross into HTTP at the gateway (Chapter 5)
 * and a single spelling avoids a translation table nobody maintains.
 */

import type { EventEnvelope } from '@platform/domain';

export const HEADER = {
  eventId: 'x-event-id',
  eventType: 'x-event-type',
  eventVersion: 'x-event-version',
  aggregateType: 'x-aggregate-type',
  aggregateId: 'x-aggregate-id',
  occurredAt: 'x-occurred-at',
  recordedAt: 'x-recorded-at',
  producer: 'x-producer',
  correlationId: 'x-correlation-id',
  causationId: 'x-causation-id',
  tenantId: 'x-tenant-id',
  idempotencyKey: 'x-idempotency-key',
  /** Set by the retry engine (Chapter 9); absent on a first publish. */
  attempt: 'x-attempt',
} as const;

/** KafkaJS accepts `string | Buffer`; it always hands back `Buffer | undefined`. */
export type KafkaHeaders = Record<string, string>;

export function envelopeToHeaders(envelope: EventEnvelope): KafkaHeaders {
  return {
    [HEADER.eventId]: envelope.eventId,
    [HEADER.eventType]: envelope.eventType,
    [HEADER.eventVersion]: String(envelope.eventVersion),
    [HEADER.aggregateType]: envelope.aggregateType,
    [HEADER.aggregateId]: envelope.aggregateId,
    [HEADER.occurredAt]: envelope.occurredAt,
    [HEADER.recordedAt]: envelope.recordedAt,
    [HEADER.producer]: envelope.producer,
    [HEADER.correlationId]: envelope.correlationId,
    [HEADER.idempotencyKey]: envelope.idempotencyKey,
    // Omitted rather than sent empty. An empty header is indistinguishable
    // from a present-but-blank one on the consumer side, and "no causation"
    // and "causation is the empty string" are different facts.
    ...(envelope.causationId !== undefined ? { [HEADER.causationId]: envelope.causationId } : {}),
    ...(envelope.tenantId !== undefined ? { [HEADER.tenantId]: envelope.tenantId } : {}),
  };
}

/**
 * Read one header as a string.
 *
 * KafkaJS types header values as `Buffer | string | (Buffer | string)[] |
 * undefined` because the protocol permits repeated keys. This platform never
 * writes repeated keys, so the array case takes the first value rather than
 * throwing — a message from a foreign producer should be routable, not fatal.
 */
export function headerString(
  headers: Record<string, Buffer | string | (Buffer | string)[] | undefined> | undefined,
  name: string,
): string | undefined {
  const raw = headers?.[name];
  if (raw === undefined) return undefined;
  const single = Array.isArray(raw) ? raw[0] : raw;
  if (single === undefined) return undefined;
  return typeof single === 'string' ? single : single.toString('utf8');
}

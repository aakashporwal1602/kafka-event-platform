/**
 * @platform/domain — the event contract.
 *
 * This package has exactly one runtime dependency (`@platform/core`) and no
 * knowledge of Kafka, Postgres, Redis or HTTP. That is deliberate and worth
 * defending in review: it is what lets the retry engine, DLQ and replay
 * service be written against envelopes rather than against infrastructure, and
 * it is why the whole business layer can be unit-tested with no containers.
 *
 * If a dependency on a driver ever appears here, the layering has inverted.
 */

export {
  idempotencyKeyFor,
  isValidEventType,
  newEvent,
  type EventEnvelope,
  type IdempotencyKeyInput,
  type NewEventInput,
} from './event.js';

export {
  RETRY_TIER_SUFFIXES,
  assertDomain,
  dlqTopic,
  eventTopic,
  parseTopic,
  partitionKeyFor,
  retryTopic,
  topicFamily,
  type ParsedTopic,
  type RetryTierSuffix,
  type TopicKind,
} from './topics.js';

/**
 * Repository interfaces are owned by the domain (ADR-0010). Implementations
 * live in `@platform/persistence` and are never referenced from here — that
 * direction of dependency is the whole point.
 */
export type {
  NewOutboxEvent,
  OutboxRecord,
  OutboxRepository,
  PublishOutcome,
} from './repositories/outbox.repository.js';

/**
 * Topic naming and partition-key rules.
 *
 * These live in the domain package rather than in the provisioning tool
 * because two independent things must agree on them: `tools/topics.config.ts`,
 * which *creates* the topics, and every producer and consumer, which *resolve*
 * them at runtime. Two copies of a naming rule agree until one is edited.
 *
 * ── The naming scheme ──────────────────────────────────────────────────────
 *   events.<domain>              the fact stream
 *   retry.<domain>.<tier>        5s · 30s · 5m · 1h   (ADR-0005)
 *   dlq.<domain>                 retries exhausted, or permanent failure
 *
 * Prefix-first, not suffix-first (`orders.events` would also read fine). The
 * reason is operational: Kafka ACLs, quotas and metrics selectors all match on
 * **prefixes**, so `events.*` is one ACL granting read on every fact stream
 * while `*.events` is not expressible at all.
 */

import { MalformedPayloadError } from '@platform/core';

/** Retry tiers, in order. Must match `tools/topics.config.ts` provisioning. */
export const RETRY_TIER_SUFFIXES = ['5s', '30s', '5m', '1h'] as const;
export type RetryTierSuffix = (typeof RETRY_TIER_SUFFIXES)[number];

/**
 * Kafka permits `[a-zA-Z0-9._-]`, but `.` and `_` collide in metric names —
 * the broker replaces one with the other, so `a.b` and `a_b` become the same
 * metric and silently merge. Underscores are therefore banned here, leaving
 * `.` as the only separator.
 */
const DOMAIN_PATTERN = /^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$/;

export function assertDomain(domain: string): void {
  if (!DOMAIN_PATTERN.test(domain) || domain.length > 100) {
    throw new MalformedPayloadError(
      `Invalid domain "${domain}". Expected lowercase alphanumeric with ` +
        `hyphens — e.g. "orders", "payment-intents". Underscores are excluded ` +
        `because Kafka's metric names conflate "." and "_", which makes two ` +
        `distinct topics report as one.`,
      { domain },
    );
  }
}

export function eventTopic(domain: string): string {
  assertDomain(domain);
  return `events.${domain}`;
}

export function retryTopic(domain: string, tier: RetryTierSuffix): string {
  assertDomain(domain);
  return `retry.${domain}.${tier}`;
}

export function dlqTopic(domain: string): string {
  assertDomain(domain);
  return `dlq.${domain}`;
}

/** Every topic belonging to one domain, in provisioning order. */
export function topicFamily(domain: string): string[] {
  return [
    eventTopic(domain),
    ...RETRY_TIER_SUFFIXES.map((tier) => retryTopic(domain, tier)),
    dlqTopic(domain),
  ];
}

/**
 * Recover the domain and kind from a topic name.
 *
 * The retry engine needs this: it receives a message on `retry.orders.30s` and
 * must work out both which tier it is on and where to send the message next.
 * Parsing is preferable to carrying the domain in a header, because the topic
 * name is the one piece of routing information Kafka guarantees is correct.
 */
export type TopicKind = 'event' | 'retry' | 'dlq';

export interface ParsedTopic {
  readonly kind: TopicKind;
  readonly domain: string;
  readonly tier?: RetryTierSuffix | undefined;
}

export function parseTopic(topic: string): ParsedTopic | null {
  const segments = topic.split('.');
  const [prefix, domain, tier] = segments;

  if (prefix === undefined || domain === undefined) return null;

  if (prefix === 'events' && segments.length === 2) {
    return { kind: 'event', domain };
  }
  if (prefix === 'dlq' && segments.length === 2) {
    return { kind: 'dlq', domain };
  }
  if (prefix === 'retry' && segments.length === 3 && isRetryTier(tier)) {
    return { kind: 'retry', domain, tier };
  }
  // Returns null rather than throwing: consumers subscribe by pattern and can
  // legitimately receive a topic this platform did not create. That is a
  // routing decision, not an exception.
  return null;
}

function isRetryTier(value: string | undefined): value is RetryTierSuffix {
  return value !== undefined && (RETRY_TIER_SUFFIXES as readonly string[]).includes(value);
}

/**
 * The partition key for an event.
 *
 * ── Why `aggregateId` and not `eventId` ────────────────────────────────────
 * Kafka guarantees order **within a partition only**. Keying by `aggregateId`
 * puts every event about one order in one partition, so `order.created`,
 * `order.paid` and `order.shipped` arrive in the order they were written.
 * Keying by `eventId` distributes perfectly and orders nothing, which for an
 * event-sourced consumer is a data-corruption bug wearing a load-balancing
 * costume.
 *
 * ── The cost, stated ───────────────────────────────────────────────────────
 * Keyed partitioning means a **hot aggregate is a hot partition**. One
 * merchant generating 40% of traffic pins 40% of load to one broker, and no
 * amount of consumer scaling helps because that partition has exactly one
 * consumer in the group. The mitigations, in order of preference:
 *
 *   1. Accept it — most workloads have no such skew.
 *   2. Composite key (`tenantId:aggregateId`) if skew is per-tenant.
 *   3. Drop the ordering requirement for that specific event type and key by
 *      `eventId` — a deliberate, documented downgrade, never a default.
 *
 * Salting the key is *not* on that list: it distributes load and destroys the
 * ordering the key existed to provide, which is the worst of both.
 */
export function partitionKeyFor(event: {
  readonly aggregateId: string;
  readonly tenantId?: string | undefined;
}): string {
  // Tenant-prefixed so that two tenants using the same aggregate id — which is
  // routine, since ids are usually per-tenant sequences — do not share a
  // partition and serialise behind each other for no reason.
  return event.tenantId === undefined
    ? event.aggregateId
    : `${event.tenantId}:${event.aggregateId}`;
}

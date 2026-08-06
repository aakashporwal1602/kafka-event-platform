/**
 * Redis key design.
 *
 * ── Why keys deserve a module ──────────────────────────────────────────────
 * Redis has one flat namespace shared by every feature. Without a scheme:
 *
 *   • Two features collide on `lock:orders` and silently corrupt each other.
 *   • Nobody can answer "how much memory is idempotency using" — `--bigkeys`
 *     reports key patterns, and unstructured keys have none.
 *   • Nothing can be safely bulk-deleted, because no prefix reliably selects
 *     one feature's keys.
 *   • A multi-tenant deployment has no way to scope or evict per tenant.
 *
 * Every key is built here. Nothing else concatenates a Redis key string.
 *
 * ── Structure ──────────────────────────────────────────────────────────────
 *
 *     kep:<family>:<scope>:<identifier>
 *      │     │        │         │
 *      │     │        │         └── the specific thing
 *      │     │        └──────────── tenant, topic or group
 *      │     └───────────────────── the feature (6 of them, listed below)
 *      └─────────────────────────── platform prefix, so this Redis can be
 *                                   shared with another system if it must be
 *
 * ── The one that costs money: idempotency ──────────────────────────────────
 * Dedup keys dominate memory, and the sizing is not intuitive:
 *
 *     10,000 events/sec × 86,400 s          = 864,000,000 keys/day
 *     per key: ~60 B key + ~16 B value
 *              + ~50 B Redis overhead        ≈ 126 B
 *     864M × 126 B                          ≈ 109 GB
 *
 * That is far past a single Redis node, so the 24-hour default TTL is only
 * viable at low volume. Three levers, in order of preference:
 *
 *   1. **Shorten the TTL.** 1 hour → ~4.5 GB. The constraint is that the TTL
 *      must exceed the longest expected replay window (ADR-0008), or a replay
 *      older than the TTL re-executes its side effects.
 *   2. **Hash the key.** A 16-byte SHA-256 prefix instead of a ~60-byte
 *      business key cuts roughly 35%. Collision probability at 864M keys with
 *      128 bits is negligible.
 *   3. **Shard Redis.** Deferred — it is the answer at 5M events/sec, not here.
 *
 * The important part is that the number is derived rather than assumed. A
 * platform that sets a 24-hour TTL without doing this arithmetic discovers it
 * during an incident, when Redis starts evicting and duplicates appear.
 */

/** The six key families. Adding a seventh requires a line here and a TTL. */
export type KeyFamily =
  /** Deduplication marks — the exactly-once-effect mechanism (ADR-0008). */
  | 'idem'
  /** Distributed locks with fencing tokens. */
  | 'lock'
  /** Monotonic fencing-token counters, one per lock name. */
  | 'fence'
  /** Token-bucket rate limiting, per tenant and route. */
  | 'rate'
  /** Cached reads — schemas, topic metadata, availability counts. */
  | 'cache'
  /** Consumer offset snapshots for the ops dashboard. */
  | 'offset';

/** Root prefix. Configurable so two environments can share one Redis if forced. */
export const DEFAULT_PREFIX = 'kep';

/**
 * TTL per family, in seconds.
 *
 * Every family has one. A key with no TTL is a permanent memory leak — Redis
 * will not remove it, and `allkeys-lru` eviction only kicks in at `maxmemory`,
 * by which point it is evicting things that were still needed.
 */
export const TTL = {
  /**
   * 24h default. Must exceed the longest expected replay window, or a replay
   * older than the TTL re-executes its effects. See the sizing note above —
   * this is the value to lower first when memory becomes the constraint.
   */
  idempotency: 86_400,

  /**
   * 30s. Bounds how long a crashed holder can block others.
   *
   * Long enough that a normal critical section finishes well inside it; short
   * enough that a pod killed mid-section does not block its partition for
   * minutes. Any operation that can legitimately exceed 30s must extend the
   * lease rather than raise this ceiling for everyone.
   */
  lock: 30,

  /**
   * 1h. Fencing counters must outlive the locks they protect, or the counter
   * resets to zero and a stale holder's old token compares as valid again —
   * defeating the entire mechanism.
   */
  fence: 3_600,

  /** 2× the largest rate-limit window, so a bucket is never evicted mid-window. */
  rateLimit: 120,

  /**
   * 5m. Short because the underlying data (schemas, topic config) changes
   * rarely but must not be stale for long after it does. Schema IDs are
   * immutable, so schema *content* could be cached forever — but the
   * subject→latest-version mapping cannot.
   */
  cache: 300,

  /** 60s. Dashboard freshness; the authoritative offsets live in Kafka. */
  offset: 60,
} as const;

/**
 * Builds every Redis key the platform uses.
 *
 * A class rather than loose functions so the prefix is bound once at
 * construction. That makes a per-environment or per-tenant prefix a wiring
 * decision instead of a parameter threaded through every call site.
 */
export class RedisKeys {
  readonly #prefix: string;

  constructor(prefix: string = DEFAULT_PREFIX) {
    this.#prefix = prefix;
  }

  /**
   * Deduplication mark for one logical event.
   *
   * The identifier must be **deterministic from business meaning** — see
   * ADR-0008. `{eventType}:{aggregateId}:{version}` is stable across a replay;
   * `partition:offset` is not, and would let a replayed event re-execute.
   */
  public idempotency(tenantId: string, idempotencyKey: string): string {
    return this.#build('idem', tenantId, idempotencyKey);
  }

  /** Mutual exclusion for one named resource. */
  public lock(resource: string): string {
    return this.#build('lock', 'global', resource);
  }

  /** Monotonic counter backing a lock's fencing tokens. */
  public fence(resource: string): string {
    return this.#build('fence', 'global', resource);
  }

  /**
   * Token bucket for one tenant on one route.
   *
   * Scoped per route as well as per tenant so a tenant hammering a cheap
   * endpoint cannot consume the quota for their expensive one.
   */
  public rateLimit(tenantId: string, route: string): string {
    return this.#build('rate', tenantId, route);
  }

  /** Cached latest schema version for a subject. */
  public schemaCache(subject: string): string {
    return this.#build('cache', 'schema', subject);
  }

  /** Cached schema definition by ID. Immutable, so safe to cache aggressively. */
  public schemaById(schemaId: number): string {
    return this.#build('cache', 'schema-id', String(schemaId));
  }

  /** Cached topic metadata. */
  public topicCache(topic: string): string {
    return this.#build('cache', 'topic', topic);
  }

  /** Latest observed committed offset, for the dashboard. */
  public consumerOffset(groupId: string, topic: string, partition: number): string {
    return this.#build('offset', groupId, `${topic}:${partition}`);
  }

  /**
   * Glob for bulk operations on one family.
   *
   * Intended for `SCAN`, never `KEYS` — `KEYS` is O(n) over the entire keyspace
   * and blocks the single-threaded server, which on a production instance with
   * hundreds of millions of keys is an outage.
   */
  public pattern(family: KeyFamily, scope = '*'): string {
    return `${this.#prefix}:${family}:${scope}:*`;
  }

  /** Everything belonging to one tenant. Used for offboarding. */
  public tenantPattern(tenantId: string): string {
    return `${this.#prefix}:*:${tenantId}:*`;
  }

  /**
   * Colons in an identifier would create phantom segments and break parsing —
   * a topic named `a:b` would look like scope `a`, identifier `b`. Encoding
   * rather than rejecting, because topic and tenant names come from callers.
   */
  #build(family: KeyFamily, scope: string, identifier: string): string {
    return `${this.#prefix}:${family}:${escape(scope)}:${escape(identifier)}`;
  }
}

function escape(segment: string): string {
  return segment.includes(':') ? segment.replaceAll(':', '%3A') : segment;
}

/**
 * Estimate memory for an idempotency window.
 *
 * Exists so the sizing above stays honest: a test asserts the 24-hour default
 * at 10K/sec exceeds a single node, which turns the comment into something the
 * build checks rather than something that quietly goes stale.
 */
export function estimateIdempotencyMemoryBytes(
  eventsPerSecond: number,
  ttlSeconds: number,
  averageKeyBytes = 60,
): number {
  const REDIS_OVERHEAD_BYTES = 50; // dictEntry + robj + expires entry, approx
  const VALUE_BYTES = 16;
  return eventsPerSecond * ttlSeconds * (averageKeyBytes + VALUE_BYTES + REDIS_OVERHEAD_BYTES);
}

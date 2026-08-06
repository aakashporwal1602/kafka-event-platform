import { describe, expect, it } from 'vitest';
import { DEFAULT_PREFIX, RedisKeys, TTL, estimateIdempotencyMemoryBytes } from './redis-keys.js';

describe('key structure', () => {
  const keys = new RedisKeys();

  it('namespaces every key under the platform prefix', () => {
    // Redis has one flat keyspace. Without a prefix this store cannot be shared
    // and nothing can be bulk-selected by feature.
    const prefixed = new RegExp(`^${DEFAULT_PREFIX}:`);
    expect(keys.idempotency('acme', 'order-1:v3')).toMatch(prefixed);
    expect(keys.lock('outbox-relay')).toMatch(prefixed);
    expect(keys.rateLimit('acme', '/events')).toMatch(prefixed);
  });

  it('separates families so two features cannot collide', () => {
    // Both are "orders", but a lock and a cache entry must never be the same key.
    expect(keys.lock('orders')).not.toBe(keys.schemaCache('orders'));
    expect(keys.lock('orders')).toContain(':lock:');
    expect(keys.schemaCache('orders')).toContain(':cache:');
  });

  it('scopes idempotency keys per tenant', () => {
    // Two tenants publishing the same business key must not deduplicate each
    // other — that would silently drop one tenant's event.
    expect(keys.idempotency('acme', 'order-1')).not.toBe(keys.idempotency('globex', 'order-1'));
  });

  it('scopes rate limits per tenant AND route', () => {
    // Per-tenant alone lets a tenant spend their quota on a cheap endpoint and
    // starve their expensive one — or the reverse, which is worse.
    expect(keys.rateLimit('acme', '/events')).not.toBe(keys.rateLimit('acme', '/replay'));
  });

  it('supports a custom prefix for a shared Redis', () => {
    expect(new RedisKeys('staging').lock('x')).toMatch(/^staging:/);
  });
});

describe('identifier escaping', () => {
  const keys = new RedisKeys();

  it('escapes colons so an identifier cannot forge a segment', () => {
    // A topic literally named "a:b" would otherwise produce
    // kep:lock:global:a:b — parseable as a different scope entirely.
    const key = keys.lock('events:orders');
    expect(key).toBe('kep:lock:global:events%3Aorders');
    expect(key.split(':')).toHaveLength(4);
  });

  it('keeps the segment count fixed regardless of input', () => {
    const withColons = keys.idempotency('a:b', 'c:d:e');
    expect(withColons.split(':')).toHaveLength(4);
  });
});

describe('TTL policy', () => {
  it('assigns a TTL to every family', () => {
    // A key with no TTL is a permanent leak: Redis will not remove it, and
    // allkeys-lru only engages at maxmemory — by which point it evicts things
    // that were still needed.
    for (const [family, seconds] of Object.entries(TTL)) {
      expect(seconds, `${family} must have a positive TTL`).toBeGreaterThan(0);
    }
  });

  it('outlives locks with the fencing counter', () => {
    // If the counter expires before the lock it protects, it restarts at zero
    // and a stale holder's old token compares as valid again — which defeats
    // the entire fencing mechanism.
    expect(TTL.fence).toBeGreaterThan(TTL.lock);
  });

  it('keeps the lock TTL short enough to bound a crashed holder', () => {
    // A crashed pod blocks its resource for exactly this long. Minutes would
    // stall a partition; a few seconds would expire mid-operation.
    expect(TTL.lock).toBeLessThanOrEqual(60);
    expect(TTL.lock).toBeGreaterThanOrEqual(10);
  });

  it('keeps rate-limit buckets alive past their window', () => {
    // Eviction mid-window resets the count and hands the caller free quota.
    expect(TTL.rateLimit).toBeGreaterThanOrEqual(120);
  });
});

describe('idempotency memory sizing', () => {
  it('shows the 24h default exceeds a single node at 10K events/sec', () => {
    // This is the number that makes the default dangerous at target scale.
    // Asserting it keeps the comment in redis-keys.ts honest — otherwise the
    // arithmetic silently goes stale and somebody trusts it.
    const bytes = estimateIdempotencyMemoryBytes(10_000, TTL.idempotency);
    const gigabytes = bytes / 1024 ** 3;
    expect(gigabytes).toBeGreaterThan(90);
  });

  it('shows a 1h TTL brings it back to a single node', () => {
    // The first lever to pull, and why the TTL is configurable rather than
    // hard-coded.
    const gigabytes = estimateIdempotencyMemoryBytes(10_000, 3_600) / 1024 ** 3;
    expect(gigabytes).toBeLessThan(6);
  });

  it('scales linearly in both rate and window', () => {
    const base = estimateIdempotencyMemoryBytes(1_000, 3_600);
    expect(estimateIdempotencyMemoryBytes(2_000, 3_600)).toBe(base * 2);
    expect(estimateIdempotencyMemoryBytes(1_000, 7_200)).toBe(base * 2);
  });

  it('shows hashing the key is worth roughly a third', () => {
    // Lever 2: a 16-byte hash prefix instead of a ~60-byte business key.
    const business = estimateIdempotencyMemoryBytes(10_000, 3_600, 60);
    const hashed = estimateIdempotencyMemoryBytes(10_000, 3_600, 16);
    expect(1 - hashed / business).toBeGreaterThan(0.3);
  });
});

describe('bulk patterns', () => {
  const keys = new RedisKeys();

  it('builds a family glob for SCAN', () => {
    expect(keys.pattern('idem')).toBe('kep:idem:*:*');
    expect(keys.pattern('cache', 'schema')).toBe('kep:cache:schema:*');
  });

  it('builds a tenant glob for offboarding', () => {
    // Deleting everything belonging to one tenant has to be expressible, or
    // offboarding becomes a manual, error-prone scan.
    expect(keys.tenantPattern('acme')).toBe('kep:*:acme:*');
  });
});

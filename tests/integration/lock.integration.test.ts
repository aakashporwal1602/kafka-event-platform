/**
 * Distributed lock — against a real Redis.
 *
 * These prove the properties that only the server can provide: `SET NX`
 * genuinely excluding a second writer, Lua running atomically against
 * concurrent clients, TTL expiry actually releasing, and the fencing counter
 * being monotonic across holders.
 *
 * The unit suite covers the control flow around all of that. Neither suite is
 * sufficient alone, which is the point of having both.
 */

import { RecordingLogger, SystemClock } from '@platform/core';
import { DistributedLock, IoredisClient, RedisKeys } from '@platform/persistence';
import type { StartedRedisContainer } from '@testcontainers/redis';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { startRedis } from './containers.js';

let container: StartedRedisContainer;
let redis: IoredisClient;
let keys: RedisKeys;
const clock = new SystemClock();

/** Distinct owner ids, because compare-and-delete depends on them differing. */
function lockFor(ownerId: string): DistributedLock {
  return new DistributedLock({
    redis,
    keys,
    clock,
    logger: new RecordingLogger(),
    ownerId,
    jitter: 0,
  });
}

beforeAll(async () => {
  const fixture = await startRedis();
  container = fixture.container;
  redis = new IoredisClient({ config: fixture.config, logger: new RecordingLogger() });
  await redis.connect();
  keys = new RedisKeys(fixture.config.REDIS_KEY_PREFIX);
});

afterAll(async () => {
  await redis.close();
  await container.stop();
});

/** Unique per test, so tests never contend with each other. */
let resourceCounter = 0;
function resource(): string {
  resourceCounter++;
  return `res-${resourceCounter}`;
}

describe('mutual exclusion', () => {
  it('lets exactly one holder in', async () => {
    const r = resource();
    const a = lockFor('pod-a');
    const b = lockFor('pod-b');

    const leaseA = await a.acquire(r);
    const leaseB = await b.acquire(r);

    expect(leaseA).not.toBeNull();
    expect(leaseB).toBeNull();
  });

  it('admits exactly one of many simultaneous contenders', async () => {
    const r = resource();

    // The property `SET NX` exists for. Twenty clients race; nineteen must
    // lose. A check-then-set written as two commands from Node passes this
    // test most of the time and fails under load, which is why the acquire is
    // a single Lua script.
    const results = await Promise.all(
      Array.from({ length: 20 }, async (_, i) => await lockFor(`pod-${i}`).acquire(r)),
    );

    expect(results.filter((lease) => lease !== null)).toHaveLength(1);
  });

  it('releases so the next holder can take it', async () => {
    const r = resource();
    const a = lockFor('pod-a');

    const leaseA = await a.acquire(r);
    if (!leaseA) throw new Error('expected a lease');
    expect(await a.release(leaseA)).toBe(true);

    expect(await lockFor('pod-b').acquire(r)).not.toBeNull();
  });
});

describe('fencing tokens', () => {
  it('increases strictly across successive holders', async () => {
    const r = resource();
    const tokens: number[] = [];

    for (let i = 0; i < 5; i++) {
      const lock = lockFor(`pod-${i}`);
      const lease = await lock.acquire(r);
      if (!lease) throw new Error('expected a lease');
      tokens.push(lease.token);
      await lock.release(lease);
    }

    // Strictly increasing is the entire correctness guarantee. Anything else —
    // repeated, reset, or out of order — means a superseded holder's write
    // compares as current.
    expect(tokens).toEqual([...tokens].sort((x, y) => x - y));
    expect(new Set(tokens).size).toBe(5);
  });

  it('keeps counting after the lock expires rather than restarting', async () => {
    const r = resource();
    const a = lockFor('pod-a');

    const first = await a.acquire(r, { ttlMs: 100 });
    if (!first) throw new Error('expected a lease');

    await new Promise((resolve) => setTimeout(resolve, 250));

    const second = await lockFor('pod-b').acquire(r);
    // If the counter reset here, pod-a's stale token would compare as valid
    // again and fencing would be silently disabled. The fence key's 1h TTL
    // against a 30s lock is what buys this margin.
    expect(second?.token).toBeGreaterThan(first.token);
  });

  it('counts per resource, not globally', async () => {
    const one = await lockFor('pod-a').acquire(resource());
    const two = await lockFor('pod-a').acquire(resource());

    // A shared counter would mean a busy resource inflates every other
    // resource's tokens — harmless for ordering but it destroys the ability to
    // reason about a single resource's history from its token values.
    expect(one?.token).toBe(1);
    expect(two?.token).toBe(1);
  });
});

describe('lease expiry', () => {
  it('becomes available again once the TTL passes', async () => {
    const r = resource();
    const lease = await lockFor('pod-a').acquire(r, { ttlMs: 150 });
    expect(lease).not.toBeNull();

    expect(await lockFor('pod-b').acquire(r)).toBeNull();
    await new Promise((resolve) => setTimeout(resolve, 300));
    // This is the crash-recovery path: a pod killed mid-critical-section must
    // not block its resource forever, which is what a lock without a TTL does.
    expect(await lockFor('pod-b').acquire(r)).not.toBeNull();
  });

  it('will not release a lock that another holder now owns', async () => {
    const r = resource();
    const a = lockFor('pod-a');

    const staleLease = await a.acquire(r, { ttlMs: 120 });
    if (!staleLease) throw new Error('expected a lease');

    await new Promise((resolve) => setTimeout(resolve, 250));
    const newHolder = await lockFor('pod-b').acquire(r);
    expect(newHolder).not.toBeNull();

    // The stale holder wakes up and releases. Compare-and-delete makes this a
    // no-op. A bare DEL here would free pod-b's lock while pod-b is still
    // working, and a third client would enter the critical section — turning
    // one slow holder into a cascade.
    expect(await a.release(staleLease)).toBe(false);

    expect(await lockFor('pod-c').acquire(r)).toBeNull();
  });

  it('reports a failed release from withLock rather than swallowing it', async () => {
    const r = resource();
    const logger = new RecordingLogger();
    const lock = new DistributedLock({
      redis,
      keys,
      clock,
      logger,
      ownerId: 'pod-slow',
      jitter: 0,
    });

    await lock.withLock(
      r,
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 250));
      },
      { ttlMs: 100 },
    );

    // The only signal that the critical section outran its lease — and that
    // another holder may have been running alongside it.
    expect(logger.find('warn', 'lease had already expired')).toBeDefined();
  });
});

describe('extend', () => {
  it('pushes the expiry out for the current holder', async () => {
    const r = resource();
    const a = lockFor('pod-a');
    const lease = await a.acquire(r, { ttlMs: 200 });
    if (!lease) throw new Error('expected a lease');

    expect(await a.extend(lease, 2_000)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 400));

    // Without the extension this would now be free.
    expect(await lockFor('pod-b').acquire(r)).toBeNull();
    expect(await redis.pttl(keys.lock(r))).toBeGreaterThan(0);
  });

  it('refuses to extend a lease that has been superseded', async () => {
    const r = resource();
    const a = lockFor('pod-a');
    const stale = await a.acquire(r, { ttlMs: 120 });
    if (!stale) throw new Error('expected a lease');

    await new Promise((resolve) => setTimeout(resolve, 250));
    await lockFor('pod-b').acquire(r, { ttlMs: 5_000 });

    // Extending a lock you no longer hold hands the current holder's lease to
    // a client that has already lost it — the same bug as a bare DEL, wearing
    // a different name.
    expect(await a.extend(stale, 10_000)).toBe(false);
  });
});

describe('withLock', () => {
  it('serialises concurrent critical sections', async () => {
    const r = resource();
    let concurrent = 0;
    let maxConcurrent = 0;
    let completed = 0;

    await Promise.all(
      Array.from({ length: 8 }, async (_, i) => {
        await lockFor(`pod-${i}`).withLock(
          r,
          async () => {
            concurrent++;
            maxConcurrent = Math.max(maxConcurrent, concurrent);
            await new Promise((resolve) => setTimeout(resolve, 20));
            concurrent--;
            completed++;
          },
          { waitMs: 10_000, retryDelayMs: 10 },
        );
      }),
    );

    expect(completed).toBe(8);
    // In the absence of a pause longer than the lease — which is the case in a
    // 20ms critical section under a 30s TTL — this holds. It is an efficiency
    // guarantee, not a correctness one; see ADR-0011 for why the difference
    // matters and why the token exists.
    expect(maxConcurrent).toBe(1);
  });

  it('releases the lock when the body throws', async () => {
    const r = resource();

    await expect(
      lockFor('pod-a').withLock(r, () => Promise.reject(new Error('boom'))),
    ).rejects.toThrow('boom');

    expect(await lockFor('pod-b').acquire(r)).not.toBeNull();
  });
});

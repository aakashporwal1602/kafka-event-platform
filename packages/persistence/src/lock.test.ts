/**
 * Distributed lock tests.
 *
 * ── Scope ──────────────────────────────────────────────────────────────────
 * These cover the **client-side control flow**: retry and wait budgets,
 * release-on-throw, lease arithmetic, and the shape of what is sent to Redis.
 * They also assert the Lua text carries its compare-and-delete guard, because
 * that guard silently disappearing is a change nobody notices in review and
 * everybody notices in production.
 *
 * What they deliberately do not do is simulate Lua. A fake that "implements"
 * `SET NX` in TypeScript is testing the fake — and the atomicity the whole
 * design rests on is a property of Redis' execution model, which cannot be
 * reproduced in a fake by construction. Mutual exclusion, token monotonicity
 * and expiry behaviour are proved in `tests/integration/lock.integration.test.ts`
 * against a real server. Same principle as `migrator.test.ts`.
 */

import { FixedClock, LockContentionError, RecordingLogger } from '@platform/core';
import { describe, expect, it } from 'vitest';
import { DistributedLock, InMemoryFencingGuard, type Lease } from './lock.js';
import { RedisKeys } from './redis-keys.js';
import type { RedisClient } from './redis.js';

interface EvalCall {
  script: string;
  keys: readonly string[];
  args: readonly (string | number)[];
}

/** Replays queued `eval` results and records what was sent. */
class FakeRedis implements RedisClient {
  public readonly evals: EvalCall[] = [];
  #results: unknown[] = [];
  #failFromCall = Number.POSITIVE_INFINITY;

  public queue(...results: unknown[]): this {
    this.#results.push(...results);
    return this;
  }

  /** Make every `eval` from the Nth call (1-based) reject. */
  public failFrom(call: number): this {
    this.#failFromCall = call;
    return this;
  }

  public eval(
    script: string,
    keys: readonly string[],
    args: readonly (string | number)[],
  ): Promise<unknown> {
    this.evals.push({ script, keys, args });
    if (this.evals.length >= this.#failFromCall) {
      return Promise.reject(new Error('redis down'));
    }
    // Repeat the last queued value once exhausted, so a retry test does not
    // need to queue one entry per attempt.
    return Promise.resolve(this.#results.length > 1 ? this.#results.shift() : this.#results[0]);
  }

  /* Unused by the lock; present to satisfy the interface. */
  public get(): Promise<string | null> {
    return Promise.resolve(null);
  }
  public setIfAbsent(): Promise<boolean> {
    return Promise.resolve(false);
  }
  public set(): Promise<void> {
    return Promise.resolve();
  }
  public del(): Promise<number> {
    return Promise.resolve(0);
  }
  public exists(): Promise<boolean> {
    return Promise.resolve(false);
  }
  public incrBy(): Promise<number> {
    return Promise.resolve(0);
  }
  public expire(): Promise<boolean> {
    return Promise.resolve(false);
  }
  public pttl(): Promise<number> {
    return Promise.resolve(-2);
  }
  public mget(): Promise<(string | null)[]> {
    return Promise.resolve([]);
  }
  public scan(): Promise<[string, string[]]> {
    return Promise.resolve(['0', []]);
  }
}

function build(redis: FakeRedis, clock = new FixedClock(0)) {
  const logger = new RecordingLogger();
  const lock = new DistributedLock({
    redis,
    keys: new RedisKeys('test'),
    clock,
    logger,
    ownerId: 'pod-a',
    jitter: 0, // deterministic backoff; jitter is asserted separately by its absence
  });
  return { lock, clock, logger };
}

/**
 * `FixedClock.sleep` resolves only when time is advanced, so a test driving a
 * retry loop must wait until the loop is actually parked before advancing.
 * Polling `pendingSleepers()` is what makes that deterministic rather than a
 * guess about how many microtasks the loop takes to reach its sleep.
 */
async function advanceOnceSleeping(clock: FixedClock, ms: number): Promise<void> {
  while (clock.pendingSleepers() === 0) {
    await new Promise((resolve) => setImmediate(resolve));
  }
  clock.advance(ms);
}

describe('acquire', () => {
  it('returns a lease carrying the fencing token', async () => {
    const { lock } = build(new FakeRedis().queue(7));

    const lease = await lock.acquire('replay-job-1');

    expect(lease?.token).toBe(7);
    expect(lease?.resource).toBe('replay-job-1');
    // owner:token, not just the token. Two processes must never produce the
    // same value or compare-and-delete compares equal across them, and the
    // safety it provides is gone.
    expect(lease?.value).toBe('pod-a:7');
  });

  it('sends the lock key, the fence key, the TTL and the owner', async () => {
    const redis = new FakeRedis().queue(1);
    const { lock } = build(redis);

    await lock.acquire('job', { ttlMs: 15_000 });

    const call = redis.evals[0];
    expect(call?.keys).toEqual(['test:lock:global:job', 'test:fence:global:job']);
    expect(call?.args[0]).toBe(15_000);
    expect(call?.args[2]).toBe('pod-a');
    // The fence counter must outlive the lock by a wide margin: if it expires
    // while the system runs, INCR restarts at 1 and a stale holder's old token
    // compares valid again.
    expect(Number(call?.args[1])).toBeGreaterThan(15_000 / 1_000);
  });

  it('returns null rather than throwing when the lock is held', async () => {
    // "Someone else is already doing it" is an ordinary outcome for a
    // cron-style job, not an exception.
    const { lock } = build(new FakeRedis().queue(0));
    expect(await lock.acquire('job')).toBeNull();
  });

  it('tries exactly once when no wait budget is given', async () => {
    const redis = new FakeRedis().queue(0);
    const { lock } = build(redis);

    await lock.acquire('job');

    expect(redis.evals).toHaveLength(1);
  });

  it('retries until the wait budget is spent, then gives up', async () => {
    const redis = new FakeRedis().queue(0);
    const { lock, clock } = build(redis);

    const pending = lock.acquire('job', { waitMs: 100, retryDelayMs: 50 });

    await advanceOnceSleeping(clock, 50);
    await advanceOnceSleeping(clock, 50);

    expect(await pending).toBeNull();
    // Attempt at t=0, t=50, t=100. The third is the last one inside budget —
    // the deadline check is `>=`, so the loop stops rather than waiting a
    // fourth time it has no budget for.
    expect(redis.evals).toHaveLength(3);
  });

  it('returns as soon as an attempt succeeds', async () => {
    const redis = new FakeRedis().queue(0, 0, 12);
    const { lock, clock } = build(redis);

    const pending = lock.acquire('job', { waitMs: 1_000, retryDelayMs: 50 });
    await advanceOnceSleeping(clock, 50);
    await advanceOnceSleeping(clock, 50);

    expect((await pending)?.token).toBe(12);
    expect(redis.evals).toHaveLength(3);
  });

  it('measures the wait budget on the monotonic clock', async () => {
    const redis = new FakeRedis().queue(0);
    const { lock, clock } = build(redis);

    const pending = lock.acquire('job', { waitMs: 100, retryDelayMs: 50 });
    await advanceOnceSleeping(clock, 50);

    // An NTP step backwards mid-wait. On wall-clock arithmetic this either ends
    // the wait immediately or extends it by the correction; monotonic time is
    // unaffected, so the budget is unchanged.
    clock.setTime(-10_000);
    await advanceOnceSleeping(clock, 50);

    expect(await pending).toBeNull();
    expect(redis.evals).toHaveLength(3);
  });
});

describe('withLock', () => {
  it('runs the body with the lease and releases afterwards', async () => {
    const redis = new FakeRedis().queue(5, 1);
    const { lock } = build(redis);
    let seen: Lease | undefined;

    const result = await lock.withLock('job', (lease) => {
      seen = lease;
      return Promise.resolve('done');
    });

    expect(result).toBe('done');
    expect(seen?.token).toBe(5);
    expect(redis.evals).toHaveLength(2);
    expect(redis.evals[1]?.args).toEqual(['pod-a:5']);
  });

  it('releases even when the body throws, and rethrows the body error', async () => {
    const redis = new FakeRedis().queue(5, 1);
    const { lock } = build(redis);

    await expect(
      lock.withLock('job', () => Promise.reject(new Error('body failed'))),
    ).rejects.toThrow('body failed');

    expect(redis.evals).toHaveLength(2);
  });

  it('throws LockContentionError when the lock cannot be acquired', async () => {
    const { lock } = build(new FakeRedis().queue(0));

    // Retryable by classification: the caller should go around again, acquire
    // a higher token and redo the work. Marking it permanent would send a
    // recoverable operation to the DLQ.
    const error = await lock.withLock('job', () => Promise.resolve(1)).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(LockContentionError);
    expect((error as LockContentionError).retryable).toBe(true);
  });

  it('warns loudly when the lease had already expired', async () => {
    // Release returning 0 means the key was gone or belonged to someone else —
    // i.e. another holder may have run concurrently. Nothing is corrupted if
    // the body fenced its writes, but this is the only signal that the TTL is
    // too short for this critical section.
    const redis = new FakeRedis().queue(5, 0);
    const { lock, logger } = build(redis);

    await lock.withLock('job', () => Promise.resolve('ok'));

    expect(logger.find('warn', 'lease had already expired')).toBeDefined();
  });

  it('does not let a release failure mask the body error', async () => {
    // Acquisition succeeds, release fails. The caller must still see why their
    // work failed — a cleanup error replacing the real cause is how a
    // five-minute debugging session becomes a two-hour one.
    const redis = new FakeRedis().queue(5).failFrom(2);
    const { lock, logger } = build(redis);

    await expect(
      lock.withLock('job', () => Promise.reject(new Error('real cause'))),
    ).rejects.toThrow('real cause');

    expect(logger.find('error', 'lock release failed')).toBeDefined();
  });
});

describe('lua scripts', () => {
  it('releases by compare-and-delete, never a bare DEL', async () => {
    const redis = new FakeRedis().queue(9, 1);
    const { lock } = build(redis);

    const lease = await lock.acquire('job');
    if (lease) await lock.release(lease);

    const script = redis.evals[1]?.script ?? '';
    // A plain DEL releases whichever lock is currently there — after a lease
    // expiry, somebody else's. That turns one slow holder into a cascade.
    expect(script).toContain("redis.call('GET', KEYS[1]) == ARGV[1]");
    expect(script).toContain("redis.call('DEL', KEYS[1])");
  });

  it('extends by compare-and-expire', async () => {
    const redis = new FakeRedis().queue(9, 1);
    const { lock } = build(redis);

    const lease = await lock.acquire('job');
    if (lease) await lock.extend(lease, 60_000);

    expect(redis.evals[1]?.script).toContain("redis.call('PEXPIRE', KEYS[1], ARGV[2])");
    expect(redis.evals[1]?.args).toEqual(['pod-a:9', 60_000]);
  });

  it('sets value and expiry in one command', async () => {
    const redis = new FakeRedis().queue(1);
    const { lock } = build(redis);

    await lock.acquire('job');

    // SETNX followed by EXPIRE is a bug: a crash between the two leaves a lock
    // with no TTL, held forever, needing a human with redis-cli.
    expect(redis.evals[0]?.script).toContain("'PX', ARGV[1]");
  });
});

describe('remainingMs', () => {
  it('counts down as monotonic time passes', async () => {
    const { lock, clock } = build(new FakeRedis().queue(1));
    const lease = await lock.acquire('job', { ttlMs: 30_000 });
    if (!lease) throw new Error('expected a lease');

    expect(lock.remainingMs(lease)).toBe(30_000);
    clock.advance(10_000);
    expect(lock.remainingMs(lease)).toBe(20_000);
  });

  it('floors at zero rather than going negative', async () => {
    const { lock, clock } = build(new FakeRedis().queue(1));
    const lease = await lock.acquire('job', { ttlMs: 1_000 });
    if (!lease) throw new Error('expected a lease');

    clock.advance(5_000);
    // Negative "time remaining" reads as a huge unsigned value in a metric and
    // as a satisfied condition in `remaining > 0 ? ... : ...` written the
    // other way round. Clamping removes the class of bug.
    expect(lock.remainingMs(lease)).toBe(0);
  });
});

describe('InMemoryFencingGuard', () => {
  it('accepts strictly increasing tokens', async () => {
    const guard = new InMemoryFencingGuard();
    await expect(guard.check('r', 1)).resolves.toBeUndefined();
    await expect(guard.check('r', 2)).resolves.toBeUndefined();
    // Gaps are legal. The contract is strictly increasing, not gapless —
    // tokens can be burned by acquisitions whose holder died.
    await expect(guard.check('r', 99)).resolves.toBeUndefined();
  });

  it('rejects a token that has been superseded', async () => {
    // The scenario in one test: holder A (token 33) pauses past its lease,
    // holder B (34) does the work, A wakes up and tries to write.
    const guard = new InMemoryFencingGuard();
    await guard.check('replay-1', 33);
    await guard.check('replay-1', 34);

    await expect(guard.check('replay-1', 33)).rejects.toBeInstanceOf(LockContentionError);
  });

  it('rejects a repeat of the current token', async () => {
    // Equal, not just lower. Two holders with the same token would mean the
    // counter was reset — the fence key expiring is exactly how that happens.
    const guard = new InMemoryFencingGuard();
    await guard.check('r', 5);
    await expect(guard.check('r', 5)).rejects.toThrow(/stale/);
  });

  it('explains what happened rather than just failing', async () => {
    const guard = new InMemoryFencingGuard();
    await guard.check('r', 10);

    let captured: unknown;
    try {
      await guard.check('r', 3);
    } catch (error: unknown) {
      captured = error;
    }

    expect(captured).toBeInstanceOf(LockContentionError);
    const error = captured as LockContentionError;
    // Whoever reads this at 2am needs to know it is not a transient blip and
    // not something to "just retry harder" — their process was asleep.
    expect(error.message).toContain('lost its lease');
    expect(error.context['highestSeen']).toBe(10);
    expect(error.retryable).toBe(true);
  });

  it('tracks resources independently', async () => {
    const guard = new InMemoryFencingGuard();
    await guard.check('a', 100);
    // A high token on one resource must not lock out a fresh one, or the first
    // aggregate to reach a high number poisons every other aggregate.
    await expect(guard.check('b', 1)).resolves.toBeUndefined();
  });
});

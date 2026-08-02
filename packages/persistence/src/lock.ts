/**
 * Distributed lock with fencing tokens.
 *
 * ── Read this before using it ──────────────────────────────────────────────
 * This lock does **not** guarantee mutual exclusion. Nothing built on Redis
 * does, and implementations that claim otherwise — Redlock included — are
 * making an assumption about clocks and pauses that does not hold on real
 * infrastructure. See ADR-0011 for the full argument.
 *
 * What it actually provides:
 *
 *   • **Efficiency.** In the overwhelmingly common case, one holder runs at a
 *     time, so redundant work is avoided. That is worth a lot and is most of
 *     what a lock is used for.
 *   • **A fencing token.** A strictly increasing number handed to the holder,
 *     which the *protected resource* checks and uses to reject a writer that
 *     has been superseded. This is what provides correctness, and it works
 *     without trusting the lock at all.
 *
 * ── The failure this is designed around ────────────────────────────────────
 *
 *   Client A acquires the lock, token 33.
 *   A stops the world — a 40s major GC pause, or the VM is live-migrated,
 *   or the node is stalled on I/O. A does not know any time has passed.
 *   A's lease (30s) expires. Redis deletes the key.
 *   Client B acquires the lock, token 34, and does its work.
 *   A wakes up, still believing it holds the lock, and writes.
 *
 *   Without fencing: A's stale write lands after B's, silently corrupting state.
 *   With fencing:    the resource has already seen 34 and rejects A's 33.
 *
 * Raising the TTL does not fix this. There is no TTL longer than the longest
 * possible pause, because the longest possible pause is unbounded. The only
 * fix is for the resource to reject stale writers, which is what
 * `FencingGuard` below is for.
 *
 * ── Why the scripts are Lua ────────────────────────────────────────────────
 * Redis executes a script atomically — it is single-threaded and does not
 * interleave a script with other clients' commands. That is the only reason
 * check-then-act (`EXISTS` then `SET`, `GET` then `DEL`) is safe here. The same
 * two commands issued separately from Node are a race, and the release race in
 * particular deletes a lock that someone else now holds.
 */

import { LockContentionError, type Clock, type Logger } from '@platform/core';
import type { RedisClient } from './redis.js';
import type { RedisKeys } from './redis-keys.js';
import { TTL } from './redis-keys.js';

/* ── Scripts ─────────────────────────────────────────────────────────────── */

/**
 * Acquire.
 *
 * KEYS[1] lock key · KEYS[2] fence counter key
 * ARGV[1] lease TTL (ms) · ARGV[2] fence TTL (s) · ARGV[3] owner id
 *
 * Returns the fencing token, or 0 if the lock is held.
 *
 * The counter is incremented only on a *successful* acquisition. It would be
 * harmless to burn tokens on failed attempts — the contract is strictly
 * increasing, not gapless — but doing it inside the same script keeps token
 * order identical to acquisition order, which makes an incident timeline
 * readable.
 *
 * The fence key's TTL (1h) must outlive any lock (30s) by a wide margin. If
 * the counter expires while the system is still running, `INCR` restarts at 1
 * and a stale holder's old token compares as valid again — which defeats the
 * entire mechanism silently.
 */
const ACQUIRE_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
local token = redis.call('INCR', KEYS[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
redis.call('SET', KEYS[1], ARGV[3] .. ':' .. token, 'PX', ARGV[1])
return token
`;

/**
 * Release — compare-and-delete.
 *
 * KEYS[1] lock key · ARGV[1] expected owner:token value
 *
 * The comparison is the whole point. A plain `DEL` releases whatever lock is
 * currently there, which after a lease expiry is *somebody else's*. That turns
 * one slow holder into a cascade: A's late release frees B's lock, C acquires
 * while B is still working, and now three clients are in the critical section.
 */
const RELEASE_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

/**
 * Extend the lease — compare-and-expire.
 *
 * KEYS[1] lock key · ARGV[1] expected value · ARGV[2] new TTL (ms)
 *
 * Same comparison, same reason: extending a lock you no longer hold hands the
 * current holder's lease to a client that has already lost it.
 */
const EXTEND_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

/* ── Lease ───────────────────────────────────────────────────────────────── */

/**
 * Proof of acquisition.
 *
 * `token` is the value that must be passed to the protected resource. A lease
 * that is never used for fencing provides efficiency only, and that should be
 * a conscious choice at the call site rather than an oversight.
 */
export interface Lease {
  readonly resource: string;
  readonly token: number;
  /** `owner:token`, the exact string stored in Redis. Used for compare-and-delete. */
  readonly value: string;
  /** Monotonic nanoseconds at acquisition. Wall clock is not used — see `remainingMs`. */
  readonly acquiredAtMonotonic: bigint;
  readonly ttlMs: number;
}

export interface AcquireOptions {
  /** Lease duration. Defaults to `TTL.lock` (30s). */
  readonly ttlMs?: number;
  /** Total time to keep retrying before giving up. `0` means try once. */
  readonly waitMs?: number;
  /** Base delay between attempts, before jitter. */
  readonly retryDelayMs?: number;
}

export interface DistributedLockOptions {
  readonly redis: RedisClient;
  readonly keys: RedisKeys;
  readonly clock: Clock;
  readonly logger: Logger;
  /**
   * Identifies this process in the lock value. Two holders must never produce
   * the same value, or compare-and-delete compares equal across processes and
   * the safety it provides evaporates.
   */
  readonly ownerId: string;
  /**
   * Jitter fraction applied to the retry delay, 0–1. Defaults to 0.3.
   *
   * Without jitter, N contenders released by the same event retry in lockstep
   * forever: they collide, back off by the same amount, and collide again.
   * Set to 0 in tests to make timing deterministic.
   */
  readonly jitter?: number;
}

export class DistributedLock {
  readonly #redis: RedisClient;
  readonly #keys: RedisKeys;
  readonly #clock: Clock;
  readonly #logger: Logger;
  readonly #ownerId: string;
  readonly #jitter: number;

  constructor(options: DistributedLockOptions) {
    this.#redis = options.redis;
    this.#keys = options.keys;
    this.#clock = options.clock;
    this.#logger = options.logger;
    this.#ownerId = options.ownerId;
    this.#jitter = options.jitter ?? 0.3;
  }

  /**
   * Try to acquire, retrying until `waitMs` elapses.
   *
   * Returns `null` rather than throwing when the lock is unavailable, because
   * "someone else is doing it" is an ordinary outcome for most callers — a
   * cron-style job that skips a tick is behaving correctly. Callers that
   * genuinely need the lock use `withLock`, which throws.
   */
  public async acquire(resource: string, options: AcquireOptions = {}): Promise<Lease | null> {
    const ttlMs = options.ttlMs ?? TTL.lock * 1_000;
    const waitMs = options.waitMs ?? 0;
    const retryDelayMs = options.retryDelayMs ?? 50;

    // Monotonic, not wall clock: an NTP correction mid-wait would otherwise
    // either end the wait immediately or extend it by the correction.
    const deadline = this.#clock.monotonic() + BigInt(waitMs) * 1_000_000n;

    for (;;) {
      const lease = await this.#tryAcquire(resource, ttlMs);
      if (lease !== null) return lease;
      if (this.#clock.monotonic() >= deadline) return null;
      await this.#clock.sleep(this.#backoff(retryDelayMs));
    }
  }

  /**
   * Acquire, run `fn`, release — releasing even if `fn` throws.
   *
   * The lease is passed in so the body can forward `lease.token` to whatever it
   * writes. A body that ignores it is running without fencing.
   */
  public async withLock<T>(
    resource: string,
    fn: (lease: Lease) => Promise<T>,
    options: AcquireOptions = {},
  ): Promise<T> {
    const lease = await this.acquire(resource, options);
    if (lease === null) {
      throw new LockContentionError(`Could not acquire lock for "${resource}"`, {
        resource,
        waitMs: options.waitMs ?? 0,
      });
    }

    try {
      return await fn(lease);
    } finally {
      // A failed release is logged, never thrown: it would replace the body's
      // real error with a cleanup error, and the lock expires by TTL anyway.
      const released = await this.release(lease).catch((error: unknown) => {
        this.#logger.error('lock release failed', error, { resource });
        return false;
      });

      // Loud, because it means the body outran its lease and another holder
      // may have run concurrently. Nothing is corrupted *if* the body fenced
      // its writes — but this is the signal that the TTL is too short for this
      // critical section, or that the body should have been extending.
      if (!released) {
        this.#logger.warn('lease had already expired when the critical section finished', {
          resource,
          token: lease.token,
          ttlMs: lease.ttlMs,
          heldMs: this.#heldMs(lease),
        });
      }
    }
  }

  /** Push the expiry out. Returns `false` if the lease is no longer held. */
  public async extend(lease: Lease, ttlMs?: number): Promise<boolean> {
    const next = ttlMs ?? lease.ttlMs;
    const result = await this.#redis.eval(
      EXTEND_SCRIPT,
      [this.#keys.lock(lease.resource)],
      [lease.value, next],
    );
    return result === 1;
  }

  /** Compare-and-delete. Returns `false` if the lease had already expired. */
  public async release(lease: Lease): Promise<boolean> {
    const result = await this.#redis.eval(
      RELEASE_SCRIPT,
      [this.#keys.lock(lease.resource)],
      [lease.value],
    );
    return result === 1;
  }

  /**
   * Milliseconds left on the lease **as this process sees it**.
   *
   * Computed from the local monotonic clock rather than by asking Redis,
   * because the answer is needed to decide whether to keep going — and asking
   * Redis costs a round trip whose own latency invalidates the answer. Treat
   * it as an upper bound: it does not account for the time the acquisition
   * reply spent in flight.
   */
  public remainingMs(lease: Lease): number {
    return Math.max(0, lease.ttlMs - this.#heldMs(lease));
  }

  async #tryAcquire(resource: string, ttlMs: number): Promise<Lease | null> {
    const acquiredAtMonotonic = this.#clock.monotonic();

    const result = await this.#redis.eval(
      ACQUIRE_SCRIPT,
      [this.#keys.lock(resource), this.#keys.fence(resource)],
      [ttlMs, TTL.fence, this.#ownerId],
    );

    // Redis Lua returns integers as JS numbers. 0 is the script's "held by
    // someone else" sentinel; INCR never yields 0, so there is no ambiguity.
    if (typeof result !== 'number' || result === 0) return null;

    return {
      resource,
      token: result,
      value: `${this.#ownerId}:${result}`,
      acquiredAtMonotonic,
      ttlMs,
    };
  }

  #heldMs(lease: Lease): number {
    return Number(this.#clock.monotonic() - lease.acquiredAtMonotonic) / 1_000_000;
  }

  #backoff(baseMs: number): number {
    const spread = baseMs * this.#jitter;
    return baseMs - spread / 2 + Math.random() * spread;
  }
}

/* ── Fencing ─────────────────────────────────────────────────────────────── */

/**
 * Rejects a writer whose token has been superseded.
 *
 * This is the half of the mechanism that actually provides correctness, and it
 * has to live **at the resource being protected** — the same place the write
 * lands, checked in the same atomic step as the write. A guard consulted a
 * moment before the write is just a smaller version of the original race.
 */
export interface FencingGuard {
  /**
   * Throws `LockContentionError` if `token` is not the highest seen for
   * `resource`. Records it otherwise.
   */
  check(resource: string, token: number): Promise<void>;
}

/**
 * Process-local guard.
 *
 * **This protects a single process only**, which makes it useful for exactly
 * two things: tests, and resources that are themselves process-local (an
 * in-memory cache, a local file). It is deliberately not the default anywhere.
 *
 * The real guard for a shared resource is a column on the row being written:
 *
 *   UPDATE replay_jobs
 *      SET status = $1, fence_token = $2
 *    WHERE id = $3 AND fence_token < $2
 *
 * — one statement, so the check and the write cannot be separated, and the
 * comparison happens under the row lock the UPDATE already takes. That version
 * arrives in Chapter 10 with the replay coordinator, which is the first
 * resource in this platform that needs it.
 */
export class InMemoryFencingGuard implements FencingGuard {
  readonly #highest = new Map<string, number>();

  public check(resource: string, token: number): Promise<void> {
    const highest = this.#highest.get(resource);

    if (highest !== undefined && token <= highest) {
      // Transient, and deliberately so. The correct response is to go back
      // around the loop: re-acquire the lock, receive a higher token, redo the
      // work. Classifying it permanent would send a legitimately recoverable
      // operation to the DLQ.
      return Promise.reject(
        new LockContentionError(
          `Fencing token ${token} for "${resource}" is stale; ${highest} has already been seen. ` +
            `This holder lost its lease — most likely a pause longer than the TTL — and its ` +
            `write has been rejected to prevent it overwriting a newer one.`,
          { resource, token, highestSeen: highest },
        ),
      );
    }

    this.#highest.set(resource, token);
    return Promise.resolve();
  }

  /** Test helper. Never call this in production code. */
  public reset(): void {
    this.#highest.clear();
  }
}

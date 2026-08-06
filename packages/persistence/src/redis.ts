/**
 * Redis client wrapper.
 *
 * ── Why a narrow interface rather than passing `ioredis` around ─────────────
 * `RedisClient` below lists **exactly** the eleven commands this platform uses.
 * That is not minimalism for its own sake — it buys three specific things:
 *
 *   1. **The blast radius of a driver swap is one file.** `ioredis` is
 *      unmaintained-adjacent (it went ~2 years between releases before 5.x);
 *      `node-redis` v4 is the plausible successor. If the driver changes,
 *      `IoredisClient` changes and nothing else does.
 *   2. **Dangerous commands are absent by design.** There is no `keys`, no
 *      `flushdb`, no `scan` without a cursor. `KEYS` is O(n) over the whole
 *      keyspace on a single-threaded server — on a production instance with
 *      hundreds of millions of keys, calling it is an outage, not a slow query.
 *      A reviewer cannot accidentally introduce one because the type does not
 *      offer it.
 *   3. **Fakes are trivial.** Eleven methods, not four hundred.
 *
 * ── Errors are translated at this boundary ─────────────────────────────────
 * Same rule as `postgres.ts`: driver error shapes stop here. What matters
 * downstream is the retryable/permanent split (ADR-0005), and for Redis that
 * split has a wrinkle worth being explicit about — see `translateRedisError`.
 *
 * ── What Redis is and is not trusted with ──────────────────────────────────
 * Redis in this platform holds only **derived or reconstructible** state:
 * deduplication marks, locks, rate-limit buckets, caches, offset snapshots.
 * Nothing here is a source of truth, because Redis persistence is asynchronous
 * — with the default `appendfsync everysec`, a hard node failure loses up to a
 * second of writes, and a failover to a replica can lose more.
 *
 * That constraint is what makes the idempotency design honest: losing a
 * deduplication mark causes a **duplicate delivery**, which the platform
 * already tolerates (at-least-once, ADR-0008), rather than data loss. If Redis
 * held anything whose loss were unrecoverable, this would be the wrong store.
 */

import { Redis as Ioredis, type RedisOptions } from 'ioredis';
import {
  DependencyUnavailableError,
  PlatformError,
  RateLimitedError,
  TimeoutError,
  UnknownError,
  type Config,
  type Logger,
} from '@platform/core';

/** Milliseconds. Named so call sites read as intent rather than as a number. */
export type Millis = number;

/**
 * The commands this platform uses. Adding one is a deliberate, reviewable act.
 *
 * `eval` is here because the lock (`lock.ts`) needs compare-and-delete
 * semantics, and Redis has no such command. A Lua script is the only way to
 * make a read and a conditional write atomic against a concurrent client.
 */
export interface RedisClient {
  get(key: string): Promise<string | null>;

  /**
   * `SET key value PX ttl NX`.
   *
   * Returns `true` if the key was set, `false` if `NX` blocked it. This exact
   * shape — value, expiry and existence check in **one** command — is the
   * reason `SETNX` followed by `EXPIRE` is a bug: a crash between the two
   * leaves a lock with no TTL, held forever, and the only fix is a human with
   * `redis-cli`.
   */
  setIfAbsent(key: string, value: string, ttl: Millis): Promise<boolean>;

  set(key: string, value: string, ttl: Millis): Promise<void>;
  del(...keys: readonly string[]): Promise<number>;
  exists(key: string): Promise<boolean>;
  incrBy(key: string, amount: number): Promise<number>;
  expire(key: string, seconds: number): Promise<boolean>;
  pttl(key: string): Promise<number>;

  /** Pipelined multi-get. One round trip for N keys instead of N. */
  mget(keys: readonly string[]): Promise<(string | null)[]>;

  /**
   * Cursor-based iteration. Deliberately exposes the cursor rather than hiding
   * it behind an async iterator, so a caller cannot forget that this is a
   * multi-round-trip operation over a live keyspace with no snapshot guarantee:
   * keys added during iteration may or may not appear, and keys present
   * throughout are guaranteed to appear at least once — possibly twice.
   */
  scan(cursor: string, pattern: string, count: number): Promise<[string, string[]]>;

  /** Run a Lua script atomically. Redis is single-threaded, so the whole script is one unit. */
  eval(
    script: string,
    keys: readonly string[],
    args: readonly (string | number)[],
  ): Promise<unknown>;
}

export interface RedisOptionsInput {
  readonly config: Config;
  readonly logger: Logger;
  readonly overrides?: Partial<RedisOptions>;
}

export class IoredisClient implements RedisClient {
  readonly #redis: Ioredis;
  readonly #logger: Logger;

  constructor(options: RedisOptionsInput) {
    const { config, logger } = options;

    this.#redis = new Ioredis({
      host: config.REDIS_HOST,
      port: config.REDIS_PORT,
      ...(config.REDIS_PASSWORD !== undefined ? { password: config.REDIS_PASSWORD } : {}),

      // Fail the command rather than queue it while disconnected. The default
      // (`enableOfflineQueue: true`) buffers commands in memory during an
      // outage and replays them on reconnect — which for a *lock acquisition*
      // means acquiring a lock seconds after the caller gave up and moved on.
      // A fast failure is a retryable error; a delayed success is a
      // correctness bug.
      enableOfflineQueue: false,

      // Bounded reconnect backoff. Unbounded retries against a dead node keep
      // a connection storm alive; capping at 2s means a recovered node is
      // picked up quickly without hammering one that is still down.
      retryStrategy: (times: number) => Math.min(times * 200, 2_000),

      // ioredis retries a failed command internally by default. Turned off:
      // this platform decides retry policy centrally from the error taxonomy,
      // and a driver retrying underneath makes a `TimeoutError` mean
      // "failed N times" without saying so.
      maxRetriesPerRequest: 0,

      commandTimeout: 5_000,
      connectTimeout: 5_000,
      lazyConnect: true,
      keyPrefix: '', // Prefixing is RedisKeys' job — see redis-keys.ts.
      ...options.overrides,
    });

    this.#logger = logger;

    // Without a listener, ioredis emits `error` on an EventEmitter with no
    // handler, and Node turns that into an uncaught exception that kills the
    // process — during what is usually a recoverable blip.
    this.#redis.on('error', (error: Error) => {
      this.#logger.error('redis connection error', error);
    });
  }

  public async connect(): Promise<void> {
    try {
      await this.#redis.connect();
    } catch (error: unknown) {
      throw translateRedisError(error);
    }
  }

  public async get(key: string): Promise<string | null> {
    return await this.#run(async () => await this.#redis.get(key));
  }

  public async setIfAbsent(key: string, value: string, ttl: Millis): Promise<boolean> {
    const result = await this.#run(async () => await this.#redis.set(key, value, 'PX', ttl, 'NX'));
    return result === 'OK';
  }

  public async set(key: string, value: string, ttl: Millis): Promise<void> {
    await this.#run(async () => await this.#redis.set(key, value, 'PX', ttl));
  }

  public async del(...keys: readonly string[]): Promise<number> {
    if (keys.length === 0) return 0;
    return await this.#run(async () => await this.#redis.del(...keys));
  }

  public async exists(key: string): Promise<boolean> {
    return await this.#run(async () => (await this.#redis.exists(key)) === 1);
  }

  public async incrBy(key: string, amount: number): Promise<number> {
    return await this.#run(async () => await this.#redis.incrby(key, amount));
  }

  public async expire(key: string, seconds: number): Promise<boolean> {
    return await this.#run(async () => (await this.#redis.expire(key, seconds)) === 1);
  }

  /** `-2` = key does not exist, `-1` = key exists with no TTL. Both are meaningful. */
  public async pttl(key: string): Promise<number> {
    return await this.#run(async () => await this.#redis.pttl(key));
  }

  public async mget(keys: readonly string[]): Promise<(string | null)[]> {
    if (keys.length === 0) return [];
    return await this.#run(async () => await this.#redis.mget(...keys));
  }

  public async scan(cursor: string, pattern: string, count: number): Promise<[string, string[]]> {
    return await this.#run(async () => {
      const [next, keys] = await this.#redis.scan(cursor, 'MATCH', pattern, 'COUNT', count);
      return [next, keys];
    });
  }

  public async eval(
    script: string,
    keys: readonly string[],
    args: readonly (string | number)[],
  ): Promise<unknown> {
    return await this.#run(
      async () => await this.#redis.eval(script, keys.length, ...keys, ...args),
    );
  }

  public async healthy(): Promise<boolean> {
    try {
      return (await this.#redis.ping()) === 'PONG';
    } catch {
      return false;
    }
  }

  /**
   * `quit` rather than `disconnect`: it sends `QUIT` and waits for in-flight
   * replies. `disconnect` severs the socket, so a lock release already on the
   * wire is lost and the lock survives until its TTL — a self-inflicted
   * 30-second stall on every deploy.
   */
  public async close(): Promise<void> {
    try {
      await this.#redis.quit();
    } catch {
      this.#redis.disconnect();
    }
  }

  async #run<T>(fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (error: unknown) {
      throw translateRedisError(error);
    }
  }
}

/**
 * Translate an `ioredis` error into the platform taxonomy.
 *
 * ── The wrinkle: a Redis timeout is not safely retryable ───────────────────
 * With Postgres, a timed-out statement inside a transaction is rolled back, so
 * the effect is known: nothing happened. Redis has no transaction rollback —
 * a command that timed out on the client may well have executed on the server.
 *
 * That is survivable here **only because every Redis write in this platform is
 * idempotent by construction**: `SET NX` either sets or does not, `INCR` on the
 * fencing counter is allowed to skip values, and a duplicated dedup mark is
 * indistinguishable from the original. `TimeoutError` is therefore classified
 * retryable — but that classification is a consequence of the data design, not
 * a property of Redis, and it stops being true the moment a non-idempotent
 * command is added to `RedisClient`.
 */
export function translateRedisError(error: unknown): PlatformError {
  if (error instanceof PlatformError) return error;
  const cause = error instanceof Error ? error : undefined;
  const message = cause?.message ?? String(error);

  if (cause === undefined) return new UnknownError(message, {});

  // Server-side memory ceiling. Transient in the sense that eviction or a
  // memory bump fixes it — but it must page a human, because at `maxmemory`
  // with `noeviction` the dedup writes are failing and duplicates are getting
  // through. See the runbook link in infra/prometheus/rules.
  if (/OOM command not allowed/i.test(message)) {
    return new DependencyUnavailableError(message, { redis: 'oom' }, cause);
  }

  // The replica-of-a-failover case: reads work, writes do not. Transient,
  // because Sentinel/Cluster promotes a new primary within seconds.
  if (/READONLY You can't write against a read only replica/i.test(message)) {
    return new DependencyUnavailableError(message, { redis: 'readonly-replica' }, cause);
  }

  // MOVED/ASK are Cluster redirections. Not used today (single node), matched
  // anyway so that turning Cluster on later fails loudly and retryably rather
  // than surfacing as UnknownError.
  if (/^(MOVED|ASK|CLUSTERDOWN)/.test(message)) {
    return new DependencyUnavailableError(message, { redis: 'cluster-redirect' }, cause);
  }

  if (/^(BUSY|LOADING)/.test(message)) {
    return new DependencyUnavailableError(message, { redis: 'busy' }, cause);
  }

  // Rate-limited by the server's own client-output-buffer limits.
  if (/max number of clients reached/i.test(message)) {
    return new RateLimitedError(message, { redis: 'max-clients' }, cause);
  }

  if (/Command timed out|timeout/i.test(message)) {
    return new TimeoutError(message, { redis: 'command-timeout' }, cause);
  }

  if (/ECONNREFUSED|ETIMEDOUT|ENOTFOUND|EHOSTUNREACH|EPIPE|ECONNRESET/.test(message)) {
    return new DependencyUnavailableError(message, { transport: true }, cause);
  }

  // `enableOfflineQueue: false` produces this while disconnected. Retryable:
  // the reconnect strategy is already working on it.
  if (/Stream isn't writeable|Connection is closed/i.test(message)) {
    return new DependencyUnavailableError(message, { redis: 'disconnected' }, cause);
  }

  // A Lua error or a wrong-type operation is a bug in this repository, not a
  // condition that improves on retry. Default-permanent, per the taxonomy.
  return new UnknownError(message, { redis: 'command-error' }, cause);
}

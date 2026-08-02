/**
 * PostgreSQL connection pool and transaction primitives.
 *
 * ── Pool sizing ────────────────────────────────────────────────────────────
 * The instinct is "more connections, more throughput". It is wrong, and the
 * reason is that a Postgres connection is a **process**, not a thread:
 *
 *   Each backend  ≈ 5–10 MB RSS, its own work_mem allocation, and a slot in
 *                   every internal lock table and snapshot scan.
 *   Beyond ~2–3×  the core count, added connections do not add throughput —
 *   cores         they add context switching, lock contention and cache
 *                   thrashing. Throughput goes DOWN.
 *
 * The practical ceiling is the database's `max_connections`, divided across
 * every client:
 *
 *   max_connections               200   (see infra/docker/docker-compose.yml)
 *   reserved for superuser/admin  −10
 *   ÷ number of service replicas   /9
 *   ────────────────────────────────────
 *   ≈ 20 per replica
 *
 * That is where POSTGRES_POOL_MAX=20 comes from. It is a division, not a guess,
 * and it must be revisited whenever replica count changes — a service scaled to
 * 30 replicas with a pool of 20 wants 600 connections and will exhaust the
 * server, producing "sorry, too many clients already" for everyone including
 * the migration runner.
 *
 * Past roughly 15 replicas the answer is PgBouncer in transaction mode, which
 * multiplexes many client connections onto few server ones. Noted, not built —
 * it would be premature at this scale, and it constrains what SQL is legal
 * (session-level state, including advisory locks and prepared statements,
 * stops working).
 *
 * ── Errors are translated at this boundary ─────────────────────────────────
 * `pg` throws its own error shapes. Letting those escape means every caller
 * imports `pg` and branches on driver-specific codes — the failure that ADR-0010
 * and the Adapter pattern both exist to prevent. Everything here converts to the
 * platform taxonomy, and crucially decides **retryable vs permanent**: a
 * serialisation failure is transient, a unique-violation is not.
 */

import { Pool, type PoolClient, type PoolConfig } from 'pg';
import {
  DependencyUnavailableError,
  DuplicateEventError,
  LockContentionError,
  PlatformError,
  TimeoutError,
  UnknownError,
  type Config,
  type Logger,
} from '@platform/core';

/**
 * A unit of work.
 *
 * Deliberately an interface rather than a concrete class, so a repository can
 * be handed either a pooled client or a transaction and cannot tell the
 * difference. That is what lets `outboxRepository.insert(tx, event)` participate
 * in the caller's transaction — the property the whole outbox pattern depends
 * on (ADR-0007).
 */
export interface Queryable {
  query<T extends Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ): Promise<{ rows: T[]; rowCount: number }>;
}

export interface PostgresOptions {
  readonly config: Config;
  readonly logger: Logger;
  /** Overrides for tests, where a 2-connection pool starts far faster. */
  readonly poolOverrides?: Partial<PoolConfig>;
}

export class Postgres implements Queryable {
  readonly #pool: Pool;
  readonly #logger: Logger;

  constructor(options: PostgresOptions) {
    const { config, logger } = options;

    this.#pool = new Pool({
      host: config.POSTGRES_HOST,
      port: config.POSTGRES_PORT,
      user: config.POSTGRES_USER,
      password: config.POSTGRES_PASSWORD,
      database: config.POSTGRES_DB,
      min: config.POSTGRES_POOL_MIN,
      max: config.POSTGRES_POOL_MAX,

      // Fail fast when the pool is exhausted rather than queueing forever.
      // An unbounded wait turns database saturation into an unbounded request
      // queue, and the service appears hung rather than degraded — which is
      // strictly harder to diagnose and impossible for a load balancer to act on.
      connectionTimeoutMillis: 5_000,

      // Recycle idle connections. Without this, a connection idle for hours can
      // be silently killed by a firewall or NAT gateway, and the next query
      // fails with a confusing "connection terminated unexpectedly".
      idleTimeoutMillis: 30_000,

      // Hard ceiling on any single statement. A missing index turning a query
      // into a sequential scan should fail loudly at 30s, not hold a connection
      // for ten minutes and starve the pool.
      statement_timeout: 30_000,

      // Separate from statement_timeout: this bounds a transaction that has
      // stopped issuing statements — usually application code awaiting
      // something while holding an open transaction. Those hold locks and block
      // VACUUM, which is how table bloat starts.
      idle_in_transaction_session_timeout: 60_000,

      application_name: config.SERVICE_NAME,
      ...options.poolOverrides,
    });

    this.#logger = logger;

    // An idle client erroring is a background failure with no request to
    // attach it to. Without this handler Node treats it as an unhandled error
    // event and terminates the process.
    this.#pool.on('error', (error) => {
      this.#logger.error('idle postgres client error', error);
    });
  }

  public async query<T extends Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
  ): Promise<{ rows: T[]; rowCount: number }> {
    try {
      const result = await this.#pool.query<T>(sql, params as unknown[]);
      return { rows: result.rows, rowCount: result.rowCount ?? 0 };
    } catch (error: unknown) {
      throw translatePostgresError(error);
    }
  }

  /**
   * Run `fn` inside a transaction, committing on success and rolling back on
   * any throw.
   *
   * The callback receives the transaction as a `Queryable`, so a repository
   * called with it enlists in this transaction rather than borrowing a separate
   * connection. Using the pool inside a transaction callback is the classic
   * outbox bug: the state change and the outbox row end up in two different
   * transactions and the atomicity guarantee silently evaporates.
   */
  public async transaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T> {
    const client = await this.#connect();
    try {
      await client.query('BEGIN');
      const result = await fn(wrapClient(client));
      await client.query('COMMIT');
      return result;
    } catch (error: unknown) {
      try {
        await client.query('ROLLBACK');
      } catch (rollbackError: unknown) {
        // A failed rollback usually means the connection is already broken. Log
        // it, but rethrow the ORIGINAL error — the rollback failure is a
        // symptom and reporting it would hide the actual cause.
        this.#logger.error('rollback failed', rollbackError);
      }
      throw error instanceof PlatformError ? error : translatePostgresError(error);
    } finally {
      client.release();
    }
  }

  /**
   * Serialisable transaction with bounded retry.
   *
   * SERIALIZABLE in Postgres uses optimistic Serializable Snapshot Isolation:
   * conflicting transactions are aborted at commit with SQLSTATE 40001 rather
   * than blocked. That makes retry **mandatory**, not optional — the isolation
   * level is only correct if the caller retries, and code that does not
   * silently loses writes under contention.
   */
  public async serializable<T>(fn: (tx: Queryable) => Promise<T>, maxAttempts = 3): Promise<T> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      const client = await this.#connect();
      try {
        await client.query('BEGIN ISOLATION LEVEL SERIALIZABLE');
        const result = await fn(wrapClient(client));
        await client.query('COMMIT');
        return result;
      } catch (error: unknown) {
        await client.query('ROLLBACK').catch(() => undefined);
        lastError = error;
        if (!isSerializationFailure(error)) throw translatePostgresError(error);
        this.#logger.warn('serialization failure, retrying', { attempt, maxAttempts });
      } finally {
        client.release();
      }
    }

    throw new LockContentionError(
      `Transaction could not be serialised after ${maxAttempts} attempts`,
      { maxAttempts },
      lastError instanceof Error ? lastError : undefined,
    );
  }

  /** Liveness probe. `SELECT 1` is the cheapest statement that proves a round trip. */
  public async healthy(): Promise<boolean> {
    try {
      await this.query('SELECT 1');
      return true;
    } catch {
      return false;
    }
  }

  /** Pool saturation, exported as metrics in Chapter 12. */
  public stats(): { total: number; idle: number; waiting: number } {
    return {
      total: this.#pool.totalCount,
      idle: this.#pool.idleCount,
      // The number that matters: sustained non-zero means the pool is
      // undersized or queries are too slow, and latency is being paid in
      // queueing rather than in the database.
      waiting: this.#pool.waitingCount,
    };
  }

  /** Drain the pool. Registered as a lifecycle close hook. */
  public async close(): Promise<void> {
    await this.#pool.end();
  }

  /** Exposed for the migrator, which needs a raw pool for advisory locks. */
  public get pool(): Pool {
    return this.#pool;
  }

  async #connect(): Promise<PoolClient> {
    try {
      return await this.#pool.connect();
    } catch (error: unknown) {
      throw translatePostgresError(error);
    }
  }
}

function wrapClient(client: PoolClient): Queryable {
  return {
    async query<T extends Record<string, unknown>>(sql: string, params: readonly unknown[] = []) {
      try {
        const result = await client.query<T>(sql, params as unknown[]);
        return { rows: result.rows, rowCount: result.rowCount ?? 0 };
      } catch (error: unknown) {
        throw translatePostgresError(error);
      }
    },
  };
}

/** SQLSTATE 40001 (serialization_failure) and 40P01 (deadlock_detected). */
function isSerializationFailure(error: unknown): boolean {
  const code = pgErrorCode(error);
  return code === '40001' || code === '40P01';
}

function pgErrorCode(error: unknown): string | undefined {
  // No cast needed: the `in` operator narrows `error` to a type carrying
  // `code`, and the typeof check below narrows that to string. Asserting on
  // top of narrowing that already happened only hides whether the narrowing
  // is still correct if the guard above ever changes.
  if (typeof error === 'object' && error !== null && 'code' in error) {
    return typeof error.code === 'string' ? error.code : undefined;
  }
  return undefined;
}

/**
 * Translate a `pg` error into the platform taxonomy.
 *
 * The retryable/permanent split here is what the retry engine acts on, so each
 * mapping is a decision rather than a formality. SQLSTATE codes are used rather
 * than message matching because they are part of the Postgres contract and do
 * not change wording between releases.
 */
export function translatePostgresError(error: unknown): PlatformError {
  if (error instanceof PlatformError) return error;
  const cause = error instanceof Error ? error : undefined;
  const message = cause?.message ?? String(error);
  const code = pgErrorCode(error);

  switch (code) {
    // Class 40 — transaction rollback. Retrying is the prescribed response.
    case '40001': // serialization_failure
    case '40P01': // deadlock_detected
      return new LockContentionError(message, { sqlstate: code }, cause);

    // Class 53 — insufficient resources. The server is over capacity now and
    // may not be shortly, so transient.
    case '53300': // too_many_connections
    case '53200': // out_of_memory
    case '53100': // disk_full
      return new DependencyUnavailableError(message, { sqlstate: code }, cause);

    // Class 57 — operator intervention. A failover or restart in progress.
    case '57P01': // admin_shutdown
    case '57P02': // crash_shutdown
    case '57P03': // cannot_connect_now
      return new DependencyUnavailableError(message, { sqlstate: code }, cause);

    // statement_timeout fired. Ambiguous — the statement may have had effects —
    // which is safe to retry only because writes carry idempotency keys.
    case '57014': // query_canceled
      return new TimeoutError(message, { sqlstate: code }, cause);

    // Unique violation. Under the platform's conditional-insert idiom this is
    // usually a duplicate event arriving twice (ADR-0008) rather than a bug,
    // and it must never be retried: the row is already there.
    case '23505':
      return new DuplicateEventError(message, { sqlstate: code }, cause);

    // Class 23 — other integrity violations. Bad data; retrying cannot fix it.
    case '23502': // not_null_violation
    case '23503': // foreign_key_violation
    case '23514': // check_violation
      return new UnknownError(message, { sqlstate: code, category: 'integrity' }, cause);

    default:
      break;
  }

  // Connection-level failures do not carry a SQLSTATE — they never reached the
  // server — so they are matched on the driver's own signals.
  if (cause && /ECONNREFUSED|ETIMEDOUT|ENOTFOUND|EHOSTUNREACH/.test(cause.message)) {
    return new DependencyUnavailableError(message, { transport: true }, cause);
  }
  if (cause && /timeout exceeded when trying to connect/i.test(cause.message)) {
    // Pool exhaustion, not database failure. Distinct because the fix is
    // different: size the pool or speed up queries, not restart Postgres.
    return new DependencyUnavailableError(message, { poolExhausted: true }, cause);
  }

  return new UnknownError(message, code !== undefined ? { sqlstate: code } : {}, cause);
}

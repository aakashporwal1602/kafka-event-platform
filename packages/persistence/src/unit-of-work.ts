/**
 * Unit of Work.
 *
 * ── The problem it solves ──────────────────────────────────────────────────
 * The outbox guarantee (ADR-0007) is one sentence: the event row and the state
 * change commit **together**. Everything else about the pattern follows from
 * that, and the pattern fails completely if it is violated even once.
 *
 * The way it gets violated is banal. A repository is constructed at startup
 * holding the pool, a service starts a transaction, and inside it calls
 * `outboxRepo.insert(...)`. That call borrows a *different* connection from the
 * pool, so it runs in its own implicit transaction and commits immediately.
 * The code reads as if it is transactional. It compiles. Tests that check
 * "was the row written" pass. It fails only when the outer transaction rolls
 * back — and then the system has published an event describing a state change
 * that never happened, which is worse than the dual-write problem the outbox
 * was adopted to fix.
 *
 * ── The fix: make the wrong thing unreachable ──────────────────────────────
 * Repositories are not injectable singletons here. They are constructed **per
 * transaction**, bound to that transaction's `Queryable`, and handed to the
 * caller inside the callback:
 *
 *     await uow.run(async (u) => {
 *       await u.outbox.insert(event);       // enlisted, necessarily
 *     });
 *
 * There is no repository object available outside the callback, so there is no
 * call site at which the mistake can be written. That is the point: a rule
 * enforced by the type system does not need to be enforced by code review.
 *
 * ── Why not `AsyncLocalStorage` for an ambient transaction ─────────────────
 * A popular alternative is to stash the current transaction in async-local
 * storage so repositories can find it implicitly. It reads beautifully and it
 * makes the most important property of the system invisible: whether a call
 * participates in a transaction becomes a runtime fact about the call stack
 * rather than a visible fact about the code. When it goes wrong the symptom is
 * a missing transaction, and the diagnosis requires reconstructing an async
 * chain. Explicit beats magic where correctness is at stake.
 */

import type { Queryable } from './postgres.js';
import {
  PostgresOutboxRepository,
  type RelayShard,
} from './repositories/postgres-outbox.repository.js';
import type { OutboxRepository } from './repositories/outbox.repository.js';

/**
 * The repositories available inside a transaction.
 *
 * Grows as the platform does — schema registry in Chapter 6, DLQ in Chapter 9,
 * replay jobs in Chapter 10. Each addition is one line here and one line in
 * `PostgresUnitOfWork.#repositories`.
 */
export interface UnitOfWork {
  readonly outbox: OutboxRepository;
  /**
   * Escape hatch for queries that have no repository yet.
   *
   * Deliberately named to be visible in review. Reaching for it is fine while
   * prototyping; a `tx.query` that survives into a merged PR is a missing
   * repository, and the reviewer should say so.
   */
  readonly tx: Queryable;
}

export interface UnitOfWorkRunner {
  /** `READ COMMITTED` — the default, and correct for the overwhelming majority. */
  run<T>(fn: (uow: UnitOfWork) => Promise<T>): Promise<T>;

  /**
   * `SERIALIZABLE` with bounded retry.
   *
   * `fn` **must be free of external side effects**, because it can run more
   * than once. Sending an email inside a serialisable block sends it twice
   * under contention, and the retry is invisible from inside the callback.
   */
  runSerializable<T>(fn: (uow: UnitOfWork) => Promise<T>, maxAttempts?: number): Promise<T>;

  /**
   * Read-only access outside any transaction.
   *
   * Separate from `run` so that the absence of a transaction is a deliberate
   * choice at the call site. Wrapping a single `SELECT` in `BEGIN`/`COMMIT`
   * costs two extra round trips and holds a connection for all three, which on
   * a hot read path is a measurable amount of pool time spent on nothing.
   */
  readonly repositories: UnitOfWork;
}

/**
 * The subset of `Postgres` this needs.
 *
 * Structural, so `Postgres` satisfies it without an `implements` clause and a
 * test can supply a two-method fake. Depending on the concrete `Postgres` class
 * here would mean every unit test of a service needs a real pool — which is the
 * coupling ADR-0010 rejected an ORM to avoid, reintroduced one layer up.
 */
export interface TransactionRunner extends Queryable {
  transaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T>;
  serializable<T>(fn: (tx: Queryable) => Promise<T>, maxAttempts?: number): Promise<T>;
}

export interface PostgresUnitOfWorkOptions {
  readonly postgres: TransactionRunner;
  /** Passed through to the outbox repository. See `RelayShard`. */
  readonly relayShard?: RelayShard | undefined;
}

export class PostgresUnitOfWork implements UnitOfWorkRunner {
  readonly #postgres: TransactionRunner;
  readonly #relayShard: RelayShard | undefined;

  constructor(options: PostgresUnitOfWorkOptions) {
    this.#postgres = options.postgres;
    this.#relayShard = options.relayShard;
  }

  public async run<T>(fn: (uow: UnitOfWork) => Promise<T>): Promise<T> {
    return await this.#postgres.transaction(async (tx) => await fn(this.#bind(tx)));
  }

  public async runSerializable<T>(
    fn: (uow: UnitOfWork) => Promise<T>,
    maxAttempts = 3,
  ): Promise<T> {
    return await this.#postgres.serializable(async (tx) => await fn(this.#bind(tx)), maxAttempts);
  }

  /**
   * Non-transactional repositories bound to the pool.
   *
   * A getter rather than a cached field: each access constructs fresh
   * repositories, which are stateless and free to build. Caching would create
   * one long-lived object graph whose `Queryable` is the pool — the exact shape
   * that makes the enlistment mistake possible again.
   */
  public get repositories(): UnitOfWork {
    return this.#bind(this.#postgres);
  }

  #bind(db: Queryable): UnitOfWork {
    return {
      outbox: new PostgresOutboxRepository({ db, shard: this.#relayShard }),
      tx: db,
    };
  }
}

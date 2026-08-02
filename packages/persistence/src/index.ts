/**
 * @platform/persistence — PostgreSQL and Redis access.
 *
 * Explicit re-exports, same reasoning as `@platform/core`: a barrel that
 * re-exports everything makes the public surface invisible, so anything added
 * to any file silently becomes API we can no longer change.
 */

export {
  Postgres,
  translatePostgresError,
  type PostgresOptions,
  type Queryable,
} from './postgres.js';

export {
  MigrationChecksumError,
  loadMigrations,
  migrate,
  pendingMigrations,
  type Migration,
  type MigrationResult,
} from './migrator.js';

export {
  DEFAULT_PREFIX,
  RedisKeys,
  TTL,
  estimateIdempotencyMemoryBytes,
  type KeyFamily,
} from './redis-keys.js';

export {
  IoredisClient,
  translateRedisError,
  type Millis,
  type RedisClient,
  type RedisOptionsInput,
} from './redis.js';

export {
  DistributedLock,
  InMemoryFencingGuard,
  type AcquireOptions,
  type DistributedLockOptions,
  type FencingGuard,
  type Lease,
} from './lock.js';

export {
  PostgresUnitOfWork,
  type PostgresUnitOfWorkOptions,
  type TransactionRunner,
  type UnitOfWork,
  type UnitOfWorkRunner,
} from './unit-of-work.js';

/**
 * Repository interfaces are exported separately from their implementations.
 * When `@platform/domain` arrives in Chapter 4 the interfaces move there and
 * only the second block below remains — see the note at the top of
 * `repositories/outbox.repository.ts`.
 */
export type {
  NewOutboxEvent,
  OutboxRecord,
  OutboxRepository,
  PublishOutcome,
} from './repositories/outbox.repository.js';

export {
  PostgresOutboxRepository,
  type PostgresOutboxRepositoryOptions,
  type RelayShard,
} from './repositories/postgres-outbox.repository.js';

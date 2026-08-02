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

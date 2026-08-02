/**
 * Testcontainers helpers.
 *
 * One place that knows how to start a dependency and hand back a `Config` the
 * production classes accept unchanged. That last part matters: these tests
 * construct `Postgres` and `IoredisClient` exactly as `main()` will, so a
 * constructor default that is wrong in production is wrong here too. A test
 * that builds its own `pg.Pool` bypasses precisely the code most worth
 * exercising.
 */

import { PostgreSqlContainer, type StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import { RedisContainer, type StartedRedisContainer } from '@testcontainers/redis';
import { loadConfig, type Config } from '@platform/core';

/**
 * Pinned by digest-less tag but pinned nonetheless.
 *
 * `latest` would mean a Postgres major upgrade silently lands in CI on an
 * unrelated PR, and the failure — a changed `hashtext` result, or a new
 * reserved word — arrives with no connection to the change that triggered it.
 * These track what `infra/docker/docker-compose.yml` runs; they must be
 * updated together or the tests stop testing production.
 */
export const POSTGRES_IMAGE = 'postgres:16-alpine';
export const REDIS_IMAGE = 'redis:7-alpine';

export interface PostgresFixture {
  readonly container: StartedPostgreSqlContainer;
  readonly config: Config;
}

export interface RedisFixture {
  readonly container: StartedRedisContainer;
  readonly config: Config;
}

/**
 * Base environment for `loadConfig`.
 *
 * Going through `loadConfig` rather than hand-building a `Config` object means
 * the tests exercise the real Zod schema. A config shape that drifts from the
 * schema then fails here rather than at the next deploy.
 */
function baseEnv(): NodeJS.ProcessEnv {
  return {
    NODE_ENV: 'test',
    LOG_LEVEL: 'error',
    SERVICE_NAME: 'integration-test',
    KAFKA_BROKERS: 'localhost:9092', // required by the schema; unused in these tests
    POSTGRES_HOST: 'localhost',
    POSTGRES_USER: 'test',
    POSTGRES_PASSWORD: 'test',
    POSTGRES_DB: 'test',
    REDIS_HOST: 'localhost',
  };
}

export async function startPostgres(): Promise<PostgresFixture> {
  const container = await new PostgreSqlContainer(POSTGRES_IMAGE)
    .withDatabase('platform_test')
    .withUsername('platform')
    .withPassword('platform')
    // fsync off. Safe *only* because this database is discarded at the end of
    // the run — it trades durability for roughly a 3× speedup on the
    // insert-heavy tests. Never do this anywhere the data matters.
    .withCommand(['postgres', '-c', 'fsync=off', '-c', 'full_page_writes=off'])
    .start();

  const config = loadConfig({
    ...baseEnv(),
    POSTGRES_HOST: container.getHost(),
    POSTGRES_PORT: String(container.getPort()),
    POSTGRES_USER: container.getUsername(),
    POSTGRES_PASSWORD: container.getPassword(),
    POSTGRES_DB: container.getDatabase(),
    // Small pool: these tests need a handful of connections and a 20-connection
    // pool spends real time establishing them on every file.
    POSTGRES_POOL_MIN: '1',
    POSTGRES_POOL_MAX: '8',
  });

  return { container, config };
}

export async function startRedis(): Promise<RedisFixture> {
  const container = await new RedisContainer(REDIS_IMAGE).start();

  const config = loadConfig({
    ...baseEnv(),
    REDIS_HOST: container.getHost(),
    REDIS_PORT: String(container.getPort()),
    // A per-run prefix, so a leftover key from a previous run cannot make this
    // one pass or fail. Cheap insurance against the hardest class of flake.
    REDIS_KEY_PREFIX: `it-${Date.now().toString(36)}`,
  });

  return { container, config };
}

/** Absolute path to the migrations directory, resolved from this file. */
export function migrationsDir(): string {
  return new URL('../../packages/persistence/migrations/', import.meta.url).pathname;
}

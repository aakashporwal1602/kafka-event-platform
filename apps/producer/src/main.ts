/**
 * Producer service — composition root.
 *
 * ── Why all the wiring is in one file ──────────────────────────────────────
 * This is the only place in the service that knows which concrete class
 * implements which interface. Everything else takes its dependencies as
 * parameters, which is what makes the rest of the codebase testable without
 * infrastructure. Spreading construction across modules — a `db.ts` that
 * exports a ready-made pool, say — reintroduces the global that the container
 * exists to remove, and makes "what does this service actually depend on"
 * unanswerable without reading every file.
 *
 * ── Registration order is dependency order, and it is load-bearing ─────────
 * `Lifecycle` closes hooks in **reverse** registration order (ADR-0009's
 * companion, `docs/lld/01-core-library.md`). So the order below is not
 * cosmetic:
 *
 *   register  postgres → redis → producer → relay
 *   close     relay → producer → redis → postgres
 *
 * The relay must stop claiming before the producer closes, or its last tick
 * publishes into a closed producer. The producer must flush before the pool
 * closes, or a `markPublished` has nowhere to run. Get this backwards and
 * every deploy drops the batch that happened to be in flight.
 */

import { Lifecycle, SystemClock, loadConfig, loggerFromConfig, type Config } from '@platform/core';
import { KafkaProducer, createKafka } from '@platform/kafka';
import {
  IoredisClient,
  Postgres,
  PostgresUnitOfWork,
  migrate,
  migrationsDir,
} from '@platform/persistence';
import { OutboxRelay } from './outbox-relay.js';

/**
 * Relay shard identity, derived from the pod's ordinal.
 *
 * A StatefulSet gives pods stable names (`producer-0`, `producer-1`), and the
 * ordinal is the shard index. A Deployment would give random names and no
 * stable index — which is why the relay runs as a StatefulSet even though it
 * holds no state. Chapter 16 makes that explicit in the manifests.
 *
 * Absent config means a single unsharded relay, which is correct for one
 * replica and preserves global ordering for free.
 */
function relayShard(): { index: number; total: number } | undefined {
  const index = process.env['RELAY_SHARD_INDEX'];
  const total = process.env['RELAY_SHARD_TOTAL'];
  if (index === undefined || total === undefined) return undefined;
  return { index: Number(index), total: Number(total) };
}

export async function main(config: Config = loadConfig()): Promise<Lifecycle> {
  const logger = loggerFromConfig(config);
  const clock = new SystemClock();
  // Lifecycle takes a plain sink rather than the Logger, deliberately: it must
  // keep reporting after the logger's own transport may have been closed during
  // shutdown. Bridging here keeps shutdown lines in the structured stream while
  // that is still true.
  const lifecycle = new Lifecycle({
    clock,
    log: (level, message, fields) => {
      const bound = fields ? { ...fields } : undefined;
      if (level === 'error') logger.error(message, undefined, bound);
      else if (level === 'warn') logger.warn(message, bound);
      else logger.info(message, bound);
    },
  });

  const postgres = new Postgres({ config, logger });
  const redis = new IoredisClient({ config, logger });
  const kafka = createKafka({ config, logger });
  const producer = new KafkaProducer({ kafka, config, logger, clock });

  const uow = new PostgresUnitOfWork({ postgres, relayShard: relayShard() });
  const relay = new OutboxRelay({ uow, producer, logger, clock });

  lifecycle.onStart('migrate', async () => {
    // Every pod runs this on every start. Safe because the migrator holds a
    // Postgres advisory lock and verifies checksums — see migrator.ts. Doing
    // it here rather than in an init container means the schema a pod needs is
    // guaranteed present before that pod serves anything.
    const result = await migrate(postgres.pool, migrationsDir());
    logger.info('migrations applied', { applied: result.applied.length });
  });

  lifecycle.onStart('redis', async () => {
    await redis.connect();
  });

  lifecycle.onStart('producer', async () => {
    // Connecting at startup rather than lazily on first send: a broker that is
    // unreachable should fail the readiness probe and stop the deploy, not
    // surface inside the first business transaction that needs it.
    await producer.connect();
  });

  lifecycle.onStart('relay', () => {
    relay.start();
    return Promise.resolve();
  });

  // Drain: stop taking on new work, in-flight work continues.
  lifecycle.onDrain('relay', async () => {
    await relay.stop();
  });

  // Close: reverse registration order. See the header.
  lifecycle.onClose('producer', async () => {
    await producer.disconnect();
  });
  lifecycle.onClose('redis', async () => {
    await redis.close();
  });
  lifecycle.onClose('postgres', async () => {
    await postgres.close();
  });

  await lifecycle.start();
  return lifecycle;
}

// `import.meta.url` guard rather than an unconditional call, so this module can
// be imported by an integration test that wires its own containers without the
// import starting a real service against the developer's local config.
if (
  process.argv[1] !== undefined &&
  import.meta.url.endsWith(process.argv[1].split('/').pop() ?? '')
) {
  await main();
}

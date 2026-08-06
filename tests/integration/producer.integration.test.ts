/**
 * The relay end to end — real Postgres, real Kafka.
 *
 * This is the test the whole chapter exists for. Everything up to now has been
 * proved in isolation: the claim query against Postgres, the producer's
 * configuration against a fake, the relay's ordering against a harness. None
 * of that proves an event written inside a business transaction actually
 * arrives on a broker with its envelope intact.
 *
 * Three things here can only be shown with both containers running:
 *
 *   1. A committed outbox row becomes a Kafka record.
 *   2. A rolled-back transaction produces **no** record — the dual-write
 *      guarantee, observed from the far side.
 *   3. Headers survive the round trip, so a DLQ message stays triageable even
 *      when its payload cannot be parsed.
 *
 * ── The one sanctioned exception to the kafkajs boundary ───────────────────
 * `@platform/kafka` says it is the only place that imports `kafkajs`, because
 * that is what keeps ADR-0004's "the choice stays reversible" claim true. This
 * file breaks that rule deliberately, and the distinction matters:
 *
 *   Production code importing kafkajs would mean a driver swap touches N
 *   services. This test imports it to *verify the boundary from outside* — it
 *   has to consume with a raw client, because consuming with our own wrapper
 *   would prove only that the wrapper agrees with itself.
 *
 * A driver swap does change this file. That is correct: the test asserts what
 * arrives on the wire, and the wire is exactly what a driver swap must keep
 * identical. No other test or service may take this exception.
 */

import { RecordingLogger, SystemClock } from '@platform/core';
import { newEvent } from '@platform/domain';
import { KafkaProducer, createKafka, headerString, HEADER } from '@platform/kafka';
import { Postgres, PostgresUnitOfWork, migrate, migrationsDir } from '@platform/persistence';
import { KafkaContainer, type StartedKafkaContainer } from '@testcontainers/kafka';
import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';
import type { Consumer, EachMessagePayload, Kafka } from 'kafkajs';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { OutboxRelay } from '../../apps/producer/src/outbox-relay.js';
import { startPostgres } from './containers.js';

const TOPIC = 'events.orders';

let pgContainer: StartedPostgreSqlContainer;
let kafkaContainer: StartedKafkaContainer;
let postgres: Postgres;
let kafka: Kafka;
let producer: KafkaProducer;
let relay: OutboxRelay;
let consumer: Consumer;

const received: EachMessagePayload[] = [];

beforeAll(async () => {
  const pg = await startPostgres();
  pgContainer = pg.container;
  postgres = new Postgres({ config: pg.config, logger: new RecordingLogger() });
  await migrate(postgres.pool, migrationsDir());

  // KRaft mode, matching docker-compose.yml. A single broker, so the topic is
  // created with replication factor 1 — which means this suite cannot test
  // min.insync.replicas behaviour. That is a three-broker property and belongs
  // in the chaos suite (Chapter 15), not here.
  kafkaContainer = await new KafkaContainer('confluentinc/cp-kafka:7.7.1').withKraft().start();

  const config = {
    ...pg.config,
    KAFKA_BROKERS: [`localhost:${kafkaContainer.getMappedPort(9093)}`],
  };
  const logger = new RecordingLogger();

  kafka = createKafka({ config, logger });
  producer = new KafkaProducer({ kafka, config, logger, clock: new SystemClock() });
  await producer.connect();

  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: TOPIC, numPartitions: 3 }] });
  await admin.disconnect();

  consumer = kafka.consumer({ groupId: `it-${Date.now().toString(36)}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });
  await consumer.run({
    eachMessage: (payload) => {
      received.push(payload);
      return Promise.resolve();
    },
  });

  relay = new OutboxRelay({
    uow: new PostgresUnitOfWork({ postgres }),
    producer,
    logger,
    clock: new SystemClock(),
    batchSize: 100,
  });
}, 180_000);

afterAll(async () => {
  // See lock.integration.test.ts: guarded so a failed startup reports one
  // failure, not two.
  await consumer?.disconnect();
  await producer?.disconnect();
  await postgres?.close();
  await kafkaContainer?.stop();
  await pgContainer?.stop();
});

beforeEach(async () => {
  // Let consumption settle BEFORE clearing.
  //
  // Clearing immediately is a race: the previous test's records are still in
  // flight, arrive a moment later, and land in the next test's array. The
  // "publishes nothing when the transaction rolls back" test then sees three
  // messages and fails for a reason that has nothing to do with what it
  // asserts — a false negative that points at the wrong file entirely.
  //
  // Draining to quiescence rather than sleeping a fixed amount, because the
  // right amount depends on the machine.
  await settle();
  received.length = 0;
  await postgres.query('TRUNCATE outbox RESTART IDENTITY');
});

/** Wait until no new message has arrived for `quietMs`. */
async function settle(quietMs = 400, maxMs = 5_000): Promise<void> {
  const deadline = Date.now() + maxMs;
  let seen = received.length;
  let quietSince = Date.now();

  while (Date.now() - quietSince < quietMs && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (received.length !== seen) {
      seen = received.length;
      quietSince = Date.now();
    }
  }
}

/** Consumption is asynchronous; poll rather than sleep a fixed amount. */
async function waitForMessages(count: number, timeoutMs = 20_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (received.length < count) {
    if (Date.now() > deadline) {
      throw new Error(`expected ${count} messages, saw ${received.length} within ${timeoutMs}ms`);
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
}

function outboxRow(overrides: Record<string, unknown> = {}) {
  const envelope = newEvent({
    eventType: 'order.placed',
    eventVersion: 2,
    aggregateType: 'order',
    aggregateId: 'order-42',
    producer: 'orders-service',
    correlationId: 'corr-abc',
    payload: { total: 999 },
    aggregateVersion: 1,
  });

  return {
    eventId: envelope.eventId,
    aggregateType: envelope.aggregateType,
    aggregateId: envelope.aggregateId,
    eventType: envelope.eventType,
    topic: TOPIC,
    payload: envelope.payload as Record<string, unknown>,
    correlationId: envelope.correlationId,
    headers: {
      eventVersion: String(envelope.eventVersion),
      occurredAt: envelope.occurredAt,
      producer: envelope.producer,
      idempotencyKey: envelope.idempotencyKey,
    },
    ...overrides,
  };
}

describe('committed events reach the broker', () => {
  it('publishes a committed outbox row and records where it landed', async () => {
    const uow = new PostgresUnitOfWork({ postgres });

    await uow.run(async (u) => {
      await u.outbox.insert(outboxRow());
    });

    const result = await relay.tick();
    expect(result).toMatchObject({ claimed: 1, published: 1 });

    await waitForMessages(1);

    // The offset the broker assigned is now in the outbox row, so support can
    // answer "where did event X actually go" without grepping Kafka.
    const { rows } = await postgres.query<{
      published_partition: number;
      published_offset: string;
    }>('SELECT published_partition, published_offset FROM outbox WHERE id = 1');
    expect(rows[0]?.published_partition).toBe(received[0]?.partition);
    expect(rows[0]?.published_offset).toBe(received[0]?.message.offset);
  });

  it('carries the envelope in headers, readable without deserialising', async () => {
    const uow = new PostgresUnitOfWork({ postgres });
    await uow.run(async (u) => {
      await u.outbox.insert(outboxRow());
    });
    await relay.tick();
    await waitForMessages(1);

    const headers = received[0]?.message.headers;
    // The DLQ case: a message is often in the DLQ *because* its payload cannot
    // be parsed, and an operator still needs the event type and correlation id.
    expect(headerString(headers, HEADER.eventType)).toBe('order.placed');
    expect(headerString(headers, HEADER.correlationId)).toBe('corr-abc');
    expect(headerString(headers, HEADER.idempotencyKey)).toHaveLength(32);
  });

  it('keys by aggregate, so one aggregate lands in one partition', async () => {
    const uow = new PostgresUnitOfWork({ postgres });
    await uow.run(async (u) => {
      await u.outbox.insertMany([
        outboxRow({ eventId: crypto.randomUUID(), eventType: 'order.created' }),
        outboxRow({ eventId: crypto.randomUUID(), eventType: 'order.paid' }),
        outboxRow({ eventId: crypto.randomUUID(), eventType: 'order.shipped' }),
      ]);
    });

    await relay.tick();
    await waitForMessages(3);

    // Same aggregateId, so the same key, so one partition — which is the only
    // scope in which Kafka orders anything. A test asserting the partitions are
    // distinct would be asserting the ordering guarantee is broken.
    expect(new Set(received.map((m) => m.partition)).size).toBe(1);
    expect(received.map((m) => headerString(m.message.headers, HEADER.eventType))).toEqual([
      'order.created',
      'order.paid',
      'order.shipped',
    ]);
  });
});

describe('the dual-write guarantee, from the far side', () => {
  it('publishes nothing when the business transaction rolls back', async () => {
    const uow = new PostgresUnitOfWork({ postgres });

    await expect(
      uow.run(async (u) => {
        await u.outbox.insert(outboxRow());
        throw new Error('business rule violated');
      }),
    ).rejects.toThrow('business rule violated');

    const result = await relay.tick();

    // The point of the entire outbox pattern, observed where it matters: no
    // consumer was ever told about a state change that did not happen.
    expect(result.claimed).toBe(0);
    await settle();
    expect(received).toHaveLength(0);
  });

  it('leaves rows unpublished when the broker rejects them', async () => {
    const uow = new PostgresUnitOfWork({ postgres });
    // A topic that does not exist, on a cluster with auto-creation disabled by
    // the producer. The publish fails, so the transaction rolls back and the
    // row stays claimable — at-least-once rather than at-most-once.
    await uow.run(async (u) => {
      await u.outbox.insert(outboxRow({ topic: 'events.does-not-exist' }));
    });

    await expect(relay.tick()).rejects.toThrow();

    expect(await uow.repositories.outbox.countUnpublished()).toBe(1);
  });
});

/**
 * Producer unit tests.
 *
 * These assert the **configuration and the request** — the settings that carry
 * the durability guarantees, and the shape of what is handed to KafkaJS. They
 * do not assert that Kafka replicates, deduplicates or orders anything; those
 * are broker properties and belong in an integration test against a real
 * cluster (Chapter 4, Part 2).
 *
 * The settings are worth unit-testing precisely because they are invisible.
 * `acks: 1` instead of `-1` changes nothing observable in development and
 * silently loses data on the next broker failure — exactly the class of
 * regression that needs a test with a sentence attached.
 */

import { FixedClock, RecordingLogger, TimeoutError, loadConfig } from '@platform/core';
import { newEvent, type EventEnvelope } from '@platform/domain';
import { CompressionTypes, type Kafka, type ProducerConfig, type ProducerRecord } from 'kafkajs';
import { describe, expect, it } from 'vitest';
import { HEADER } from './headers.js';
import { KafkaProducer } from './producer.js';

interface FakeState {
  producerConfig?: ProducerConfig;
  sent: ProducerRecord[];
  failWith?: Error;
}

/**
 * A stand-in for `Kafka` exposing only what the producer touches.
 *
 * The cast is unavoidable and honest: `Kafka` is a concrete class with a large
 * surface, and implementing it would be pretending to build a broker client.
 * The narrowness of what is faked here is itself a signal — if this fake grows,
 * the producer has started depending on more of KafkaJS than it should.
 */
function fakeKafka(state: FakeState): Kafka {
  return {
    producer(config: ProducerConfig) {
      state.producerConfig = config;
      return {
        connect: () => Promise.resolve(),
        disconnect: () => Promise.resolve(),
        on: () => () => undefined,
        send: (record: ProducerRecord) => {
          state.sent.push(record);
          if (state.failWith) return Promise.reject(state.failWith);
          return Promise.resolve(
            record.messages.map((_, index) => ({
              topicName: record.topic,
              partition: 3,
              baseOffset: String(1000 + index),
              errorCode: 0,
            })),
          );
        },
      };
    },
  } as unknown as Kafka;
}

function testConfig() {
  return loadConfig({
    KAFKA_BROKERS: 'broker-1:9092',
    POSTGRES_HOST: 'localhost',
    POSTGRES_USER: 'u',
    POSTGRES_PASSWORD: 'p',
    POSTGRES_DB: 'd',
    REDIS_HOST: 'localhost',
    SERVICE_NAME: 'orders-service',
  });
}

function build(state: FakeState = { sent: [] }) {
  const producer = new KafkaProducer({
    kafka: fakeKafka(state),
    config: testConfig(),
    logger: new RecordingLogger(),
    clock: new FixedClock(0),
  });
  return { producer, state };
}

function event(overrides: Partial<Parameters<typeof newEvent>[0]> = {}): EventEnvelope {
  return newEvent({
    eventType: 'order.placed',
    eventVersion: 1,
    aggregateType: 'order',
    aggregateId: 'order-1',
    producer: 'orders-service',
    correlationId: 'corr-1',
    payload: { total: 100 },
    ...overrides,
  });
}

describe('durability configuration', () => {
  it('enables idempotence', () => {
    const { state } = build();
    // Without it the ambiguous-timeout case is unresolvable: retrying
    // duplicates, not retrying loses. This setting is what lets errors.ts
    // classify RequestTimedOut as retryable.
    expect(state.producerConfig?.idempotent).toBe(true);
  });

  it('allows 5 in-flight requests, which is safe only because of idempotence', () => {
    const { state } = build();
    // Without idempotence, >1 in flight reorders on retry and breaks
    // per-partition ordering. With it, the broker refuses out-of-order
    // batches and orders them itself — so 5 is safe and ~3-4x the throughput
    // of the classic max.in.flight=1 advice. 5 is also the broker's maximum:
    // 6 silently disables the ordering guarantee.
    expect(state.producerConfig?.maxInFlightRequests).toBe(5);
  });

  it('refuses to auto-create topics', () => {
    // A producer-created topic gets broker defaults, i.e. replication factor
    // 1, which is how a critical topic ends up with no redundancy and nobody
    // finds out until a broker dies.
    expect(build().state.producerConfig?.allowAutoTopicCreation).toBe(false);
  });

  it('is not transactional', () => {
    // Transactions are reserved for Kafka-only effects (ADR-0008). Here they
    // would add coordinator round trips to every publish for a guarantee that
    // stops at the first Postgres write anyway.
    expect(build().state.producerConfig?.transactionalId).toBeUndefined();
  });

  it('bounds client-side retries', () => {
    // Unbounded retry turns a cluster outage into unexplained latency instead
    // of an error the caller can act on.
    expect(build().state.producerConfig?.retry?.retries).toBe(5);
  });

  it('sends with acks=all', async () => {
    const { producer, state } = build();
    await producer.connect();
    await producer.publish('events.orders', event());

    // -1 means every in-sync replica. With min.insync.replicas=2 that is two
    // copies before the producer is told it succeeded. acks=1 would ack on
    // the leader alone, and a broker lost seconds later takes the record.
    expect(state.sent[0]?.acks).toBe(-1);
  });

  it('compresses with gzip', async () => {
    const { producer, state } = build();
    await producer.connect();
    await producer.publish('events.orders', event());

    // ADR-0004 says lz4; KafkaJS ships only gzip built in, and adding an lz4
    // codec reintroduces the install friction that ADR chose KafkaJS to
    // avoid. Documented divergence, not an oversight.
    expect(state.sent[0]?.compression).toBe(CompressionTypes.GZIP);
  });
});

describe('message shape', () => {
  it('keys by aggregate, not by event id', async () => {
    const { producer, state } = build();
    await producer.connect();
    await producer.publish('events.orders', event({ aggregateId: 'order-7', tenantId: 'acme' }));

    // Keying by eventId distributes perfectly and orders nothing — which for
    // an event-sourced consumer is corruption, not load balancing.
    expect(state.sent[0]?.messages[0]?.key).toBe('acme:order-7');
  });

  it('mirrors the envelope into headers', async () => {
    const { producer, state } = build();
    await producer.connect();
    const envelope = event({ causationId: 'cause-1' });
    await producer.publish('events.orders', envelope);

    const headers = state.sent[0]?.messages[0]?.headers;
    // Readable without deserialising the value — which is the only way to
    // triage a DLQ message whose payload is the reason it failed.
    expect(headers?.[HEADER.eventType]).toBe('order.placed');
    expect(headers?.[HEADER.idempotencyKey]).toBe(envelope.idempotencyKey);
    expect(headers?.[HEADER.causationId]).toBe('cause-1');
  });

  it('omits absent optional headers instead of sending them blank', async () => {
    const { producer, state } = build();
    await producer.connect();
    await producer.publish('events.orders', event());

    // "no tenant" and "tenant is the empty string" are different facts, and
    // an empty header cannot express the difference.
    expect(state.sent[0]?.messages[0]?.headers).not.toHaveProperty(HEADER.tenantId);
  });

  it('stamps the record timestamp with business time', async () => {
    const { producer, state } = build();
    await producer.connect();
    const occurredAt = new Date('2026-07-31T09:30:00.000Z');
    await producer.publish('events.orders', event({ occurredAt }));

    // Retention and time-based seeks — which replay uses in Chapter 10 —
    // then operate on when the fact happened rather than when we published it.
    expect(state.sent[0]?.messages[0]?.timestamp).toBe(String(occurredAt.getTime()));
  });
});

describe('batching', () => {
  it('sends a batch as one request', async () => {
    const { producer, state } = build();
    await producer.connect();

    const results = await producer.publishBatch('events.orders', [event(), event(), event()]);

    // Latency here is dominated by acks=all replication, not payload size:
    // 500 individual sends cost 500 round trips, one batch costs one. That
    // ratio is the outbox relay's throughput ceiling.
    expect(state.sent).toHaveLength(1);
    expect(results).toHaveLength(3);
  });

  it('returns offsets as bigint', async () => {
    const { producer } = build();
    await producer.connect();
    const [result] = await producer.publishBatch('events.orders', [event()]);

    // Kafka offsets are int64 and exceed Number's exact range on a long-lived
    // high-volume partition — the same reason pg returns int8 as a string.
    expect(result?.offset).toBe(1000n);
    expect(result?.partition).toBe(3);
  });

  it('short-circuits an empty batch', async () => {
    const { producer, state } = build();
    await producer.connect();

    expect(await producer.publishBatch('events.orders', [])).toEqual([]);
    // An empty relay tick is routine, and a send with no messages is a
    // pointless round trip.
    expect(state.sent).toHaveLength(0);
  });
});

describe('connection state', () => {
  it('refuses to publish before connect', async () => {
    const { producer } = build();
    // KafkaJS would lazily auto-connect, which moves connection failures from
    // startup — where a readiness probe stops the deploy — into the middle of
    // a business transaction.
    await expect(producer.publish('events.orders', event())).rejects.toThrow(/disconnected/);
  });

  it('is a no-op to disconnect when never connected', async () => {
    const { producer } = build();
    await expect(producer.disconnect()).resolves.toBeUndefined();
  });

  it('reports connection state', async () => {
    const { producer } = build();
    expect(producer.connected).toBe(false);
    await producer.connect();
    expect(producer.connected).toBe(true);
  });
});

describe('failures', () => {
  it('translates a driver error into the platform taxonomy', async () => {
    const state: FakeState = {
      sent: [],
      failWith: Object.assign(new Error('Request timed out'), { code: 7 }),
    };
    const { producer } = build(state);
    await producer.connect();

    const error = await producer
      .publish('events.orders', event())
      .catch((caught: unknown) => caught);

    // Callers branch on retryable, never on a KafkaJS error type — that is
    // the whole point of the wrapper ADR-0004 promised.
    expect(error).toBeInstanceOf(TimeoutError);
  });
});

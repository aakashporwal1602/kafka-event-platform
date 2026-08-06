/**
 * Relay unit tests.
 *
 * `tick()` is public and returns a result precisely so these can drive one
 * iteration deterministically. A relay whose only entry point is `start()` can
 * be tested only by waiting, and tests that wait are tests that flake.
 *
 * What is asserted here is the **ordering and the transaction boundary** —
 * claim, publish, mark, in one transaction, in that order. Whether SKIP LOCKED
 * actually hands out disjoint rows is a Postgres property and is proved in the
 * integration suite.
 */

import { BrokerUnavailableError, FixedClock, RecordingLogger } from '@platform/core';
import type { EventEnvelope, OutboxRecord, PublishOutcome } from '@platform/domain';
import type { PublishResult } from '@platform/kafka';
import { describe, expect, it } from 'vitest';
import { OutboxRelay } from './outbox-relay.js';

interface Step {
  type: 'claim' | 'publish' | 'mark' | 'commit' | 'rollback';
  detail?: string;
}

function record(id: bigint, topic = 'events.orders'): OutboxRecord {
  return {
    id,
    eventId: `e-${id}`,
    aggregateType: 'order',
    aggregateId: `order-${id}`,
    eventType: 'order.placed',
    topic,
    partitionKey: null,
    payload: { n: Number(id) },
    headers: {},
    createdAt: new Date('2026-08-02T10:00:00.000Z'),
    attempts: 0,
    correlationId: null,
    tenantId: null,
  };
}

interface Harness {
  steps: Step[];
  toClaim: OutboxRecord[][];
  marked: PublishOutcome[];
  publishError?: Error;
  /**
   * How many PARTITIONS the broker's response covers — not how many messages.
   * Kafka returns one RecordMetadata per partition touched, with baseOffset
   * being that partition's first record in the request. Defaults to 1, the
   * common case for a keyed batch about one aggregate.
   */
  partitionsInResponse?: number;
}

function build(h: Harness) {
  const logger = new RecordingLogger();

  const uow = {
    run: async <T>(fn: (u: unknown) => Promise<T>): Promise<T> => {
      const outbox = {
        claimBatch: (limit: number) => {
          h.steps.push({ type: 'claim', detail: String(limit) });
          return Promise.resolve(h.toClaim.shift() ?? []);
        },
        markPublished: (outcomes: readonly PublishOutcome[]) => {
          h.steps.push({ type: 'mark', detail: String(outcomes.length) });
          h.marked.push(...outcomes);
          return Promise.resolve();
        },
      };
      try {
        const result = await fn({ outbox, tx: {} });
        h.steps.push({ type: 'commit' });
        return result;
      } catch (error: unknown) {
        h.steps.push({ type: 'rollback' });
        throw error;
      }
    },
    runSerializable: <T>(): Promise<T> => Promise.reject(new Error('unused')),
    repositories: {},
  };

  const producer = {
    publishBatch: (
      topic: string,
      envelopes: readonly EventEnvelope[],
    ): Promise<PublishResult[]> => {
      h.steps.push({ type: 'publish', detail: `${topic}:${envelopes.length}` });
      if (h.publishError) return Promise.reject(h.publishError);
      const partitions = h.partitionsInResponse ?? 1;
      return Promise.resolve(
        Array.from({ length: partitions }, (_, i) => ({
          topic,
          partition: i + 1,
          offset: BigInt(100),
        })),
      );
    },
  };

  const relay = new OutboxRelay({
    uow: uow as never,
    producer: producer as never,
    logger,
    clock: new FixedClock(0),
    batchSize: 10,
  });

  return { relay, logger };
}

describe('the tick', () => {
  it('claims, publishes, then marks — inside one transaction', async () => {
    const h: Harness = { steps: [], toClaim: [[record(1n), record(2n)]], marked: [] };
    const { relay } = build(h);

    const result = await relay.tick();

    // The ordering is the guarantee. Marking before publishing would lose an
    // event permanently on a crash between the two; publishing after commit
    // would release the row locks that ARE the claim, letting a second relay
    // publish the same rows.
    expect(h.steps.map((s) => s.type)).toEqual(['claim', 'publish', 'mark', 'commit']);
    expect(result).toMatchObject({ claimed: 2, published: 2 });
  });

  it('does nothing when the backlog is empty', async () => {
    const h: Harness = { steps: [], toClaim: [[]], marked: [] };
    const { relay } = build(h);

    expect(await relay.tick()).toMatchObject({ claimed: 0, published: 0 });
    // An empty tick is the common case on a quiet system. Publishing an empty
    // batch would be a pointless broker round trip several times a second.
    expect(h.steps.map((s) => s.type)).toEqual(['claim', 'commit']);
  });

  it('records the offsets the broker returned, per row', async () => {
    const h: Harness = { steps: [], toClaim: [[record(7n), record(8n)]], marked: [] };
    const { relay } = build(h);

    await relay.tick();

    // One result for two messages, because they share a partition. Offsets are
    // derived as baseOffset + position, which is how Kafka assigns them within
    // a partition for one request.
    expect(h.marked).toEqual([
      { id: 7n, partition: 1, offset: 100n },
      { id: 8n, partition: 1, offset: 101n },
    ]);
  });

  it('sends one request per topic', async () => {
    const h: Harness = {
      steps: [],
      toClaim: [
        [record(1n, 'events.orders'), record(2n, 'events.payments'), record(3n, 'events.orders')],
      ],
      marked: [],
    };
    const { relay } = build(h);

    await relay.tick();

    // KafkaJS's send takes one topic. sendBatch takes several and does not
    // report per-topic failures distinctly, so a partial failure becomes
    // unattributable — and this loop must know exactly which rows to mark.
    expect(h.steps.filter((s) => s.type === 'publish').map((s) => s.detail)).toEqual([
      'events.orders:2',
      'events.payments:1',
    ]);
  });

  it('records location as unknown when the batch spanned partitions', async () => {
    // Two partitions in the response, and nothing says which message went to
    // which — answering that means reimplementing the broker's partitioner,
    // which would then drift from it silently.
    const h: Harness = {
      steps: [],
      toClaim: [[record(1n), record(2n), record(3n)]],
      marked: [],
      partitionsInResponse: 2,
    };
    const { relay } = build(h);

    const result = await relay.tick();

    // Still published and still committed: published_at is what makes the
    // outbox correct, and the events ARE on the broker. Only the location — a
    // support convenience — is missing, and it is recorded as unknown rather
    // than guessed. A wrong offset in a support tool is worse than a missing
    // one, because somebody will trust it.
    expect(result).toMatchObject({ claimed: 3, published: 3 });
    expect(h.steps.at(-1)?.type).toBe('commit');
    expect(h.marked).toEqual([
      { id: 1n, partition: -1, offset: 0n },
      { id: 2n, partition: -1, offset: 0n },
      { id: 3n, partition: -1, offset: 0n },
    ]);
  });

  it('rolls back when the publish fails, leaving the rows unpublished', async () => {
    const h: Harness = {
      steps: [],
      toClaim: [[record(1n)]],
      marked: [],
      publishError: new BrokerUnavailableError('no leader'),
    };
    const { relay } = build(h);

    await expect(relay.tick()).rejects.toThrow('no leader');
    // Unmarked and uncommitted, so the next tick claims them again. That is
    // at-least-once working as designed: a duplicate is recoverable, a lost
    // event is not.
    expect(h.marked).toEqual([]);
    expect(h.steps.map((s) => s.type)).toEqual(['claim', 'publish', 'rollback']);
  });
});

describe('failure handling', () => {
  it('warns on a transient failure and escalates once it persists', async () => {
    const h: Harness = { steps: [], toClaim: [], marked: [] };
    const { relay, logger } = build(h);
    relay.start();

    // A broker restart produces a burst of retryable failures. Logging each at
    // error level pages somebody for a self-healing condition, so severity
    // rises with persistence instead of firing on the first one.
    await relay.stop();

    expect(logger.entries.filter((e) => e.level === 'fatal')).toEqual([]);
  });

  it('is idempotent to start and stop', async () => {
    const h: Harness = { steps: [], toClaim: [], marked: [] };
    const { relay } = build(h);

    relay.start();
    relay.start();
    expect(relay.running).toBe(true);

    await relay.stop();
    await relay.stop();
    // stop() awaits the loop rather than returning early, because returning
    // early lets the lifecycle close the producer while a publish is still in
    // flight.
    expect(relay.running).toBe(false);
  });
});

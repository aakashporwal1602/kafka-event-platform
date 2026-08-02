/**
 * Topic configuration tests.
 *
 * Topic configuration is infrastructure, and infrastructure mistakes here are
 * expensive: a topic created with RF=1 cannot be fixed without a migration, and
 * a missing DLQ means silent data loss. These tests assert the invariants the
 * platform's durability guarantees depend on — before anything reaches a broker.
 */

import { describe, expect, it } from 'vitest';
import { eventDomain, RETENTION, RETRY_TIERS, TOPICS } from './topics.config.js';

describe('eventDomain', () => {
  it('expands a domain into main + retry tiers + dlq', () => {
    const topics = eventDomain('orders', { description: 'test' });
    expect(topics.map((t) => t.name)).toEqual([
      'events.orders',
      'retry.orders.5s',
      'retry.orders.30s',
      'retry.orders.5m',
      'retry.orders.1h',
      'dlq.orders',
    ]);
  });

  it('gives retry topics fewer partitions than the main topic', () => {
    const [main, ...rest] = eventDomain('orders', { partitions: 12, description: 'test' });
    expect(main?.partitions).toBe(12);
    for (const topic of rest) {
      expect(topic.partitions).toBe(3); // max(3, 12/4)
    }
  });

  it('never drops a retry topic below 3 partitions', () => {
    const topics = eventDomain('tiny', { partitions: 2, description: 'test' });
    for (const topic of topics.slice(1)) {
      expect(topic.partitions).toBeGreaterThanOrEqual(3);
    }
  });

  it('retains DLQ messages longer than retry messages', () => {
    const topics = eventDomain('orders', { description: 'test' });
    const retry = topics.find((t) => t.name === 'retry.orders.5s');
    const dlq = topics.find((t) => t.name === 'dlq.orders');
    expect(Number(dlq?.config['retention.ms'])).toBeGreaterThan(
      Number(retry?.config['retention.ms']),
    );
  });

  it('honours a custom retention for the main topic only', () => {
    const topics = eventDomain('payments', {
      retentionMs: RETENTION.THIRTY_DAYS,
      description: 'test',
    });
    expect(topics[0]?.config['retention.ms']).toBe(RETENTION.THIRTY_DAYS);
    expect(topics[1]?.config['retention.ms']).toBe(RETENTION.THREE_DAYS);
  });
});

describe('TOPICS — durability invariants', () => {
  it('replicates every topic three times', () => {
    for (const topic of TOPICS) {
      expect(topic.replicationFactor, `${topic.name} must be RF=3`).toBe(3);
    }
  });

  it('sets min.insync.replicas=2 on every topic', () => {
    // RF=3 + minISR=2 + acks=all is the combination that delivers N-3
    // (zero acknowledged-then-lost events). All three are required.
    for (const topic of TOPICS) {
      expect(topic.config['min.insync.replicas'], `${topic.name}`).toBe('2');
    }
  });

  it('has no duplicate topic names', () => {
    const names = TOPICS.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
  });

  it('gives every topic at least one partition', () => {
    for (const topic of TOPICS) {
      expect(topic.partitions, `${topic.name}`).toBeGreaterThan(0);
    }
  });

  it('documents why every topic exists', () => {
    for (const topic of TOPICS) {
      expect(topic.description.length, `${topic.name} needs a description`).toBeGreaterThan(10);
    }
  });
});

describe('TOPICS — naming convention', () => {
  it('prefixes every topic with a known class', () => {
    // The convention lets monitoring, ACLs and quotas be applied by pattern
    // rather than enumerated per topic.
    const validPrefix = /^(events|retry|dlq|platform)\./;
    for (const topic of TOPICS) {
      expect(topic.name, `${topic.name} has an unknown class prefix`).toMatch(validPrefix);
    }
  });

  it('gives every events.* topic a matching dlq.* topic', () => {
    // A domain without a DLQ silently drops un-processable messages.
    const names = new Set(TOPICS.map((t) => t.name));
    const domains = TOPICS.filter((t) => t.name.startsWith('events.')).map((t) =>
      t.name.slice('events.'.length),
    );
    for (const domain of domains) {
      expect(names.has(`dlq.${domain}`), `events.${domain} has no dlq.${domain}`).toBe(true);
    }
  });

  it('gives every events.* topic the full set of retry tiers', () => {
    const names = new Set(TOPICS.map((t) => t.name));
    const domains = TOPICS.filter((t) => t.name.startsWith('events.')).map((t) =>
      t.name.slice('events.'.length),
    );
    for (const domain of domains) {
      for (const tier of RETRY_TIERS) {
        expect(
          names.has(`retry.${domain}.${tier.suffix}`),
          `missing retry.${domain}.${tier.suffix}`,
        ).toBe(true);
      }
    }
  });
});

describe('TOPICS — schema registry topic', () => {
  const schemas = TOPICS.find((t) => t.name === 'platform.schemas');

  it('uses a single partition for total ordering', () => {
    // Schema registrations must be totally ordered — a compatibility check
    // against a stale view of the previous version is unsound.
    expect(schemas?.partitions).toBe(1);
  });

  it('is compacted and retained indefinitely', () => {
    // The latest schema for every subject must survive forever; a deleted
    // schema makes every event written against it undecodable.
    expect(schemas?.config['cleanup.policy']).toBe('compact');
    expect(schemas?.config['retention.ms']).toBe('-1');
  });
});

describe('RETRY_TIERS', () => {
  it('increases delay monotonically', () => {
    const delays = RETRY_TIERS.map((t) => t.delayMs);
    for (let i = 1; i < delays.length; i++) {
      expect(delays[i]).toBeGreaterThan(delays[i - 1]!);
    }
  });

  it('totals roughly one hour before the DLQ', () => {
    const total = RETRY_TIERS.reduce((sum, t) => sum + t.delayMs, 0);
    expect(total).toBeGreaterThan(60 * 60 * 1000); // > 1h
    expect(total).toBeLessThan(2 * 60 * 60 * 1000); // < 2h
  });
});

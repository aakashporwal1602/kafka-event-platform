import { describe, expect, it } from 'vitest';
import {
  RETRY_TIER_SUFFIXES,
  dlqTopic,
  eventTopic,
  parseTopic,
  partitionKeyFor,
  retryTopic,
  topicFamily,
} from './topics.js';

describe('naming', () => {
  it('puts the kind first so ACLs and metrics can match on a prefix', () => {
    // `orders.events` reads fine and is unusable operationally: Kafka ACLs,
    // quotas and metric selectors all match prefixes, so `events.*` is one
    // rule while `*.events` is not expressible.
    expect(eventTopic('orders')).toBe('events.orders');
    expect(retryTopic('orders', '30s')).toBe('retry.orders.30s');
    expect(dlqTopic('orders')).toBe('dlq.orders');
  });

  it('expands a domain into its whole family', () => {
    expect(topicFamily('orders')).toEqual([
      'events.orders',
      'retry.orders.5s',
      'retry.orders.30s',
      'retry.orders.5m',
      'retry.orders.1h',
      'dlq.orders',
    ]);
  });

  it('rejects underscores, which Kafka conflates with dots in metric names', () => {
    // `a.b` and `a_b` become the same metric on the broker, so two distinct
    // topics silently report as one.
    expect(() => eventTopic('order_events')).toThrow(/Underscores are excluded/);
  });

  it.each(['Orders', '1orders', 'orders.sub', '', 'x'.repeat(101)])('rejects "%s"', (domain) => {
    expect(() => eventTopic(domain)).toThrow(/Invalid domain/);
  });
});

describe('parseTopic', () => {
  it('round-trips every name it generates', () => {
    expect(parseTopic('events.orders')).toEqual({ kind: 'event', domain: 'orders' });
    expect(parseTopic('dlq.orders')).toEqual({ kind: 'dlq', domain: 'orders' });
    for (const tier of RETRY_TIER_SUFFIXES) {
      expect(parseTopic(`retry.orders.${tier}`)).toEqual({
        kind: 'retry',
        domain: 'orders',
        tier,
      });
    }
  });

  it('returns null for anything it did not generate', () => {
    // Consumers subscribe by pattern and can legitimately receive a foreign
    // topic. That is a routing decision, not an exception.
    expect(parseTopic('__consumer_offsets')).toBeNull();
    expect(parseTopic('events.orders.extra')).toBeNull();
    expect(parseTopic('retry.orders.90s')).toBeNull();
    expect(parseTopic('retry.orders')).toBeNull();
    expect(parseTopic('')).toBeNull();
  });
});

describe('partitionKeyFor', () => {
  it('keys by aggregate, so events about one thing stay ordered', () => {
    // Keying by eventId would distribute perfectly and order nothing — which
    // for an event-sourced consumer is corruption wearing a load-balancing
    // costume.
    expect(partitionKeyFor({ aggregateId: 'order-1' })).toBe('order-1');
  });

  it('prefixes the tenant so two tenants do not share a partition by accident', () => {
    // Aggregate ids are usually per-tenant sequences, so "order-1" exists in
    // every tenant. Without the prefix they serialise behind each other for
    // no reason.
    expect(partitionKeyFor({ aggregateId: 'order-1', tenantId: 'acme' })).toBe('acme:order-1');
    expect(partitionKeyFor({ aggregateId: 'order-1', tenantId: 'acme' })).not.toBe(
      partitionKeyFor({ aggregateId: 'order-1', tenantId: 'globex' }),
    );
  });

  it('is stable, because the partition assignment depends on it', () => {
    const key = { aggregateId: 'order-1', tenantId: 'acme' };
    expect(partitionKeyFor(key)).toBe(partitionKeyFor(key));
  });
});

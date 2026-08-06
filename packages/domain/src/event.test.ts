import { MalformedPayloadError } from '@platform/core';
import { describe, expect, it } from 'vitest';
import { idempotencyKeyFor, isValidEventType, newEvent } from './event.js';

const base = {
  eventType: 'order.placed',
  eventVersion: 1,
  aggregateType: 'order',
  aggregateId: 'order-1',
  producer: 'orders-service',
  correlationId: 'corr-1',
  payload: { total: 100 },
} as const;

describe('event type grammar', () => {
  it.each(['order.placed', 'payment.settlement-failed', 'a.b.c'])('accepts %s', (type) => {
    expect(isValidEventType(type)).toBe(true);
  });

  it.each([
    'Order.Placed', // uppercase
    'order_placed', // underscore — collides with "." in Kafka metric names
    'order', // no verb
    'order.', // trailing separator
    '.placed', // leading separator
    'order..placed',
    '1order.placed',
  ])('rejects %s', (type) => {
    expect(isValidEventType(type)).toBe(false);
  });

  it('explains the past-tense rule when it rejects', () => {
    // The message has to teach, because the person hitting it has just
    // written `order.place` and believes it is fine.
    let captured: unknown;
    try {
      newEvent({ ...base, eventType: 'Order.Place' });
    } catch (error: unknown) {
      captured = error;
    }
    expect(captured).toBeInstanceOf(MalformedPayloadError);
    expect((captured as Error).message).toMatch(/already happened/);
  });
});

describe('newEvent', () => {
  it('builds an envelope with both timestamps', () => {
    const now = new Date('2026-08-02T12:00:00.000Z');
    const occurred = new Date('2026-07-31T09:30:00.000Z');

    const event = newEvent({ ...base, occurredAt: occurred }, now);

    // The offline-mobile-client case: the fact is two days older than the
    // record of it. A system with one timestamp has to pick which lie to tell.
    expect(event.occurredAt).toBe('2026-07-31T09:30:00.000Z');
    expect(event.recordedAt).toBe('2026-08-02T12:00:00.000Z');
  });

  it('defaults occurredAt to now when the fact is happening now', () => {
    const now = new Date('2026-08-02T12:00:00.000Z');
    const event = newEvent(base, now);
    expect(event.occurredAt).toBe(event.recordedAt);
  });

  it('gives every publication a distinct event id but a stable idempotency key', () => {
    const first = newEvent({ ...base, aggregateVersion: 3 });
    const second = newEvent({ ...base, aggregateVersion: 3 });

    // This pair is the whole reason the two fields are separate: a relay
    // republishing the same fact must be recognisable as a duplicate AND
    // countable as a second attempt.
    expect(first.eventId).not.toBe(second.eventId);
    expect(first.idempotencyKey).toBe(second.idempotencyKey);
  });

  it('omits optional fields rather than setting them undefined', () => {
    const event = newEvent(base);
    expect('causationId' in event).toBe(false);
    expect('tenantId' in event).toBe(false);
  });

  it('carries causation when supplied', () => {
    const event = newEvent({ ...base, causationId: 'cause-1' });
    // correlationId groups; causationId orders. Without the second, an
    // incident timeline is a flat list rather than a tree.
    expect(event.causationId).toBe('cause-1');
  });

  it.each([0, -1, 1.5, Number.NaN])('rejects eventVersion %s', (eventVersion) => {
    expect(() => newEvent({ ...base, eventVersion })).toThrow(/positive integer/);
  });

  it('rejects an aggregateId that could inject a key separator', () => {
    // These values become Redis key segments. An unconstrained one can add a
    // separator and land in another tenant's namespace.
    expect(() => newEvent({ ...base, aggregateId: 'a b' })).toThrow(/Invalid aggregateId/);
    expect(() => newEvent({ ...base, aggregateId: '' })).toThrow(/Invalid aggregateId/);
  });

  it('accepts a caller-supplied idempotency key', () => {
    const event = newEvent({ ...base, idempotencyKey: 'client-supplied' });
    expect(event.idempotencyKey).toBe('client-supplied');
  });
});

describe('idempotencyKeyFor', () => {
  it('is deterministic across processes', () => {
    const input = { eventType: 'order.placed', aggregateType: 'order', aggregateId: 'o-1' };
    expect(idempotencyKeyFor(input)).toBe(idempotencyKeyFor(input));
  });

  it('distinguishes aggregate versions', () => {
    const of = (aggregateVersion: number): string =>
      idempotencyKeyFor({
        eventType: 'order.updated',
        aggregateType: 'order',
        aggregateId: 'o-1',
        aggregateVersion,
      });
    expect(of(1)).not.toBe(of(2));
  });

  it('distinguishes "no version" from version 0', () => {
    // Without the explicit marker these collide, and a genuinely distinct
    // event is discarded as a duplicate.
    const withoutVersion = idempotencyKeyFor({
      eventType: 'order.placed',
      aggregateType: 'order',
      aggregateId: 'o-1',
    });
    const withZero = idempotencyKeyFor({
      eventType: 'order.placed',
      aggregateType: 'order',
      aggregateId: 'o-1',
      aggregateVersion: 0,
    });
    expect(withoutVersion).not.toBe(withZero);
  });

  it('is bounded at 32 hex characters', () => {
    const key = idempotencyKeyFor({
      eventType: 'order.placed',
      aggregateType: 'order',
      aggregateId: 'x'.repeat(200),
    });
    // 128 bits. Every key lives in Redis for 24h, and redis-keys.ts derives
    // memory cost from key length — an unbounded key makes that sizing wrong.
    expect(key).toHaveLength(32);
    expect(key).toMatch(/^[0-9a-f]{32}$/);
  });

  it('does not collide when an identifier contains the separator', () => {
    const a = idempotencyKeyFor({ eventType: 'x.y', aggregateType: 'agg b', aggregateId: 'c' });
    const b = idempotencyKeyFor({ eventType: 'x.y', aggregateType: 'agg', aggregateId: 'b c' });
    // Concatenation without care makes these identical. They are different
    // facts, and one would silently vanish.
    expect(a).not.toBe(b);
  });
});

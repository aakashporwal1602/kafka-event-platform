/**
 * The retryable/permanent split is the single most consequential decision this
 * package makes: get it wrong in one direction and a broker restart fills the
 * DLQ with healthy events; get it wrong in the other and one oversized message
 * blocks its partition forever.
 */

import {
  BrokerUnavailableError,
  MessageTooLargeError,
  NotEnoughReplicasError,
  TimeoutError,
  TopicNotFoundError,
  UnauthorizedError,
  UnknownError,
} from '@platform/core';
import { describe, expect, it } from 'vitest';
import { translateKafkaError } from './errors.js';

function brokerError(code: number, message = 'broker said no', retriable?: boolean): Error {
  return Object.assign(new Error(message), {
    code,
    type: 'KAFKA_ERROR',
    ...(retriable !== undefined ? { retriable } : {}),
  });
}

describe('transient conditions', () => {
  it.each([
    [19, 'NOT_ENOUGH_REPLICAS'],
    [20, 'NOT_ENOUGH_REPLICAS_AFTER_APPEND'],
  ])('maps %i to NotEnoughReplicasError', (code) => {
    const error = translateKafkaError(brokerError(code));
    // Its own class, not a generic broker failure: a sustained rate here
    // means the cluster is one failure away from rejecting every write.
    expect(error).toBeInstanceOf(NotEnoughReplicasError);
    expect(error.retryable).toBe(true);
  });

  it.each([5, 6, 8, 9, 13])('maps %i to a retryable broker error', (code) => {
    expect(translateKafkaError(brokerError(code))).toBeInstanceOf(BrokerUnavailableError);
  });

  it('treats a produce timeout as retryable', () => {
    // Ambiguous on its own — the broker may have written the batch and lost
    // the response. Safe only because the producer is idempotent, so the
    // broker discards the duplicate. If idempotence is ever turned off, this
    // classification becomes a duplication bug.
    const error = translateKafkaError(brokerError(7, 'Request timed out'));
    expect(error).toBeInstanceOf(TimeoutError);
    expect(error.retryable).toBe(true);
  });
});

describe('permanent conditions', () => {
  it.each([10, 43, 44])('maps %i to a permanent message error', (code) => {
    const error = translateKafkaError(brokerError(code));
    // Retrying reproduces the rejection forever, and a message retried
    // forever blocks its partition. This has to reach the DLQ immediately.
    expect(error).toBeInstanceOf(MessageTooLargeError);
    expect(error.retryable).toBe(false);
  });

  it('treats an unknown topic as permanent because auto-create is off', () => {
    // With auto.create.topics.enable=false (ADR-0001), an unknown topic is a
    // skipped provisioning step — a deploy bug. Retrying hides it until
    // somebody notices the backlog.
    const error = translateKafkaError(brokerError(3));
    expect(error).toBeInstanceOf(TopicNotFoundError);
    expect(error.retryable).toBe(false);
  });

  it.each([29, 31])('maps %i to UnauthorizedError', (code) => {
    expect(translateKafkaError(brokerError(code))).toBeInstanceOf(UnauthorizedError);
  });
});

describe('idempotent-producer state', () => {
  it.each([45, 47])('marks %i fatal rather than retryable', (code) => {
    // The producer's sequence state has diverged from the broker's. Only
    // recreating the producer recovers it; retrying the message spins forever.
    const error = translateKafkaError(brokerError(code));
    expect(error.retryable).toBe(false);
    expect(error.context['fatal']).toBe(true);
    expect(error.message).toMatch(/must be recreated/);
  });

  it('flags a deduplicated batch as benign', () => {
    const error = translateKafkaError(brokerError(46));
    expect(error.context['benign']).toBe(true);
  });
});

describe('errors that never reached a broker', () => {
  it.each(['connect ECONNREFUSED 127.0.0.1:9092', 'read ECONNRESET', 'getaddrinfo ENOTFOUND'])(
    'maps "%s" to a retryable broker error',
    (message) => {
      const error = translateKafkaError(new Error(message));
      expect(error).toBeInstanceOf(BrokerUnavailableError);
      expect(error.context['transport']).toBe(true);
    },
  );

  it('maps a disconnected producer to retryable', () => {
    expect(translateKafkaError(new Error('The producer is disconnected'))).toBeInstanceOf(
      BrokerUnavailableError,
    );
  });
});

describe('fallbacks', () => {
  it('trusts the driver when it says an unknown code is retriable', () => {
    // The one place the default is inverted. An unrecognised code the driver
    // knows is retriable is far more likely to be a transient cluster
    // condition than a bad message, and the alternative sends healthy events
    // to the DLQ during an incident.
    const error = translateKafkaError(brokerError(999, 'future error', true));
    expect(error.retryable).toBe(true);
    expect(error.context['driverRetriable']).toBe(true);
  });

  it('defaults an unrecognised, non-retriable error to permanent', () => {
    const error = translateKafkaError(brokerError(998, 'weird', false));
    expect(error).toBeInstanceOf(UnknownError);
    expect(error.retryable).toBe(false);
  });

  it('passes a PlatformError through untouched', () => {
    // Translating twice would relabel an already-classified error and could
    // flip its retryability.
    const original = new TimeoutError('already classified');
    expect(translateKafkaError(original)).toBe(original);
  });

  it('survives a non-Error throw', () => {
    expect(translateKafkaError('a string')).toBeInstanceOf(UnknownError);
    expect(translateKafkaError(undefined)).toBeInstanceOf(UnknownError);
  });

  it('preserves the broker code in context for triage', () => {
    expect(translateKafkaError(brokerError(19)).context['brokerErrorCode']).toBe(19);
  });
});

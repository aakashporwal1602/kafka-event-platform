/**
 * Error taxonomy tests.
 *
 * These assert the property the entire retry engine depends on: that
 * retryability is decided by the error's type, and that anything unclassified
 * defaults to permanent. A regression here does not fail loudly — it silently
 * changes whether production traffic gets retried or quarantined.
 */

import { describe, expect, it } from 'vitest';
import {
  BrokerUnavailableError,
  DuplicateEventError,
  MalformedPayloadError,
  NotEnoughReplicasError,
  PermanentError,
  PlatformError,
  RateLimitedError,
  SchemaNotFoundError,
  SchemaValidationError,
  TimeoutError,
  TopicNotFoundError,
  TransientError,
  UnknownError,
  errorCodeOf,
  isRetryable,
  toPlatformError,
} from './errors.js';

describe('retryability classification', () => {
  it('marks environmental failures as retryable', () => {
    // The test for each of these: would the same message, unchanged, succeed
    // in 30 seconds? Yes — the failure is about the environment, not the data.
    expect(isRetryable(new BrokerUnavailableError('broker down'))).toBe(true);
    expect(isRetryable(new NotEnoughReplicasError('ISR shrunk'))).toBe(true);
    expect(isRetryable(new TimeoutError('deadline exceeded'))).toBe(true);
    expect(isRetryable(new RateLimitedError('429'))).toBe(true);
  });

  it('marks data and contract failures as permanent', () => {
    // These depend on the message content. Retrying cannot change the outcome,
    // and four attempts over an hour delays discovery for no benefit.
    expect(isRetryable(new SchemaValidationError('missing field'))).toBe(false);
    expect(isRetryable(new MalformedPayloadError('truncated'))).toBe(false);
    expect(isRetryable(new MessageTooLargeStub())).toBe(false);
    expect(isRetryable(new TopicNotFoundError('events.typo'))).toBe(false);
  });

  it('treats an unknown schema id as permanent, not as a lookup miss', () => {
    // Schema IDs are immutable and the registry topic is compacted with
    // infinite retention, so an ID that is absent will never appear. Classing
    // this as transient would retry for an hour before DLQ-ing anyway.
    expect(isRetryable(new SchemaNotFoundError('id 4711'))).toBe(false);
  });

  it('defaults an unclassified error to permanent', () => {
    // The safe default. Retrying an unknown error amplifies load during an
    // incident and hides the failure inside normal-looking retry traffic;
    // a DLQ entry is loud and gets looked at.
    expect(isRetryable(new Error('something'))).toBe(false);
    expect(isRetryable('a string')).toBe(false);
    expect(isRetryable(null)).toBe(false);
    expect(isRetryable(undefined)).toBe(false);
    expect(isRetryable(new UnknownError('unclassified'))).toBe(false);
  });

  it('makes retryability a property of the class, not of the throw site', () => {
    // Two instances of the same error can never disagree about retryability.
    // If this were a constructor argument, one call site could get it wrong.
    const a = new BrokerUnavailableError('a');
    const b = new BrokerUnavailableError('b');
    expect(a.retryable).toBe(b.retryable);
    expect(a).toBeInstanceOf(TransientError);
    expect(new SchemaValidationError('x')).toBeInstanceOf(PermanentError);
  });
});

describe('error metadata', () => {
  it('reports a stable code for metrics and alerting', () => {
    expect(errorCodeOf(new RateLimitedError('x'))).toBe('RATE_LIMITED');
    expect(errorCodeOf(new SchemaValidationError('x'))).toBe('SCHEMA_VALIDATION_FAILED');
    expect(errorCodeOf(new Error('x'))).toBe('UNKNOWN');
  });

  it('sets name to the concrete subclass', () => {
    // Without the `new.target.name` assignment every subclass reports "Error",
    // which collapses every distinct failure into one bucket in log aggregation.
    expect(new BrokerUnavailableError('x').name).toBe('BrokerUnavailableError');
    expect(new SchemaValidationError('x').name).toBe('SchemaValidationError');
  });

  it('carries structured context', () => {
    const error = new TopicNotFoundError('unknown topic', {
      topic: 'events.orderz',
      tenantId: 'acme',
    });
    expect(error.context['topic']).toBe('events.orderz');
    expect(error.context['tenantId']).toBe('acme');
  });

  it('preserves the underlying cause rather than flattening it', () => {
    // Flattening to a string loses the original stack, which is the only thing
    // that identifies where a vendor SDK actually failed.
    const cause = new Error('ECONNREFUSED 10.0.0.1:9092');
    const error = new BrokerUnavailableError('cannot reach broker', { broker: 'kafka-1' }, cause);
    expect(error.cause).toBe(cause);
    expect(error.cause?.message).toContain('ECONNREFUSED');
  });

  it('serialises to a shape suitable for logs and DLQ records', () => {
    const error = new SchemaValidationError('field "amount" is required', {
      subject: 'events.orders-value',
      schemaId: 42,
    });
    const json = error.toJSON();
    expect(json['code']).toBe('SCHEMA_VALIDATION_FAILED');
    expect(json['retryable']).toBe(false);
    expect(json['name']).toBe('SchemaValidationError');
    expect(json['context']).toMatchObject({ schemaId: 42 });
  });
});

describe('toPlatformError', () => {
  it('passes through an existing PlatformError unchanged', () => {
    const original = new RateLimitedError('429');
    expect(toPlatformError(original)).toBe(original);
  });

  it('wraps a native Error, keeping it as the cause', () => {
    const native = new Error('boom');
    const wrapped = toPlatformError(native, { topic: 'events.orders' });
    expect(wrapped).toBeInstanceOf(UnknownError);
    expect(wrapped.retryable).toBe(false);
    expect(wrapped.cause).toBe(native);
    expect(wrapped.context['topic']).toBe('events.orders');
  });

  it('handles non-Error throws', () => {
    // JavaScript permits `throw 'oops'` and rejected promises carrying
    // primitives. A catch-all that assumes `.message` crashes on those.
    const fromString = toPlatformError('something went wrong');
    expect(fromString.message).toBe('something went wrong');
    expect(fromString).toBeInstanceOf(PlatformError);

    const fromNumber = toPlatformError(42);
    expect(fromNumber.context['thrownType']).toBe('number');

    expect(() => toPlatformError(null)).not.toThrow();
    expect(() => toPlatformError(undefined)).not.toThrow();
  });
});

describe('DuplicateEventError', () => {
  it('is permanent so a deduplicated event is never retried', () => {
    // A duplicate is the idempotency layer working, not a failure. Retrying it
    // would loop forever, since the dedup key will still be present.
    const error = new DuplicateEventError('already processed', { key: 'order-1:v3' });
    expect(error.retryable).toBe(false);
    expect(error.code).toBe('DUPLICATE_EVENT');
  });
});

/** Local stub — avoids importing a class only used in one assertion. */
class MessageTooLargeStub extends PermanentError {
  public override readonly code = 'MESSAGE_TOO_LARGE' as const;
  constructor() {
    super('payload exceeds max.message.bytes');
  }
}

import { describe, expect, it } from 'vitest';
import {
  contextFields,
  correlationId,
  createContext,
  currentContext,
  newCorrelationId,
  runWithContext,
  withContext,
} from './context.js';

describe('propagation', () => {
  it('survives await boundaries', async () => {
    // The property that makes this worth having. A plain variable would be
    // correct here and wrong the moment two requests interleave.
    await runWithContext(createContext({ correlationId: 'abc' }), async () => {
      expect(correlationId()).toBe('abc');
      await Promise.resolve();
      expect(correlationId()).toBe('abc');
      await new Promise((r) => setTimeout(r, 1));
      expect(correlationId()).toBe('abc');
    });
  });

  it('keeps concurrent operations isolated', async () => {
    // The bug a module-level variable would produce: Node interleaves these on
    // one thread, so B would overwrite A's context at the first await.
    const observed: string[] = [];

    const operation = async (id: string, delayMs: number): Promise<void> =>
      await runWithContext(createContext({ correlationId: id }), async () => {
        await new Promise((r) => setTimeout(r, delayMs));
        observed.push(correlationId());
      });

    await Promise.all([operation('req-1', 20), operation('req-2', 5), operation('req-3', 10)]);

    // Completion order differs from start order — which is exactly the
    // interleaving that breaks a shared variable.
    expect(observed).toEqual(['req-2', 'req-3', 'req-1']);
  });

  it('propagates into timers and nested callbacks', async () => {
    await runWithContext(createContext({ correlationId: 'deep' }), async () => {
      const seen = await new Promise<string>((resolve) => {
        setTimeout(() => {
          void Promise.resolve().then(() => resolve(correlationId()));
        }, 1);
      });
      expect(seen).toBe('deep');
    });
  });
});

describe('outside a context', () => {
  it('returns undefined rather than throwing', () => {
    // A logger must work during startup and shutdown, where no request exists.
    // Throwing here would take the process down at the worst possible moment.
    expect(currentContext()).toBeUndefined();
    expect(contextFields()).toEqual({});
  });

  it('returns a greppable placeholder for the correlation id', () => {
    // Not an empty string: seeing this marker in a log line tells you the
    // context was lost, which is itself a bug worth finding.
    expect(correlationId()).toBe('no-correlation-id');
  });
});

describe('withContext', () => {
  it('merges additions without mutating the parent scope', () => {
    runWithContext(createContext({ correlationId: 'parent', tenantId: 'acme' }), () => {
      withContext({ topic: 'events.orders', partition: 3 }, () => {
        const ctx = currentContext();
        expect(ctx?.correlationId).toBe('parent'); // inherited
        expect(ctx?.tenantId).toBe('acme'); // inherited
        expect(ctx?.topic).toBe('events.orders'); // added
      });

      // Parent is unchanged after the nested scope exits.
      expect(currentContext()?.topic).toBeUndefined();
      expect(currentContext()?.correlationId).toBe('parent');
    });
  });

  it('merges attributes rather than replacing them', () => {
    // Replacing would mean a nested scope adding one attribute silently
    // discards everything its parent set.
    runWithContext(
      createContext({ correlationId: 'x', attributes: { region: 'ap-south-1' } }),
      () => {
        withContext({ attributes: { retryTier: '5s' } }, () => {
          const attrs = currentContext()?.attributes;
          expect(attrs).toEqual({ region: 'ap-south-1', retryTier: '5s' });
        });
      },
    );
  });

  it('creates a fresh context when used outside one', () => {
    withContext({ tenantId: 'acme' }, () => {
      expect(currentContext()?.correlationId).toMatch(/^[0-9a-f-]{36}$/);
      expect(currentContext()?.tenantId).toBe('acme');
    });
  });
});

describe('createContext', () => {
  it('honours an inbound correlation id', () => {
    // The whole point of correlation: generating a fresh id per hop would make
    // one user journey look like N unrelated operations across services.
    expect(createContext({ correlationId: 'from-caller' }).correlationId).toBe('from-caller');
  });

  it('generates one when the caller supplied none', () => {
    expect(createContext().correlationId).toMatch(/^[0-9a-f-]{36}$/);
  });

  it('generates distinct ids', () => {
    const ids = new Set(Array.from({ length: 1_000 }, () => newCorrelationId()));
    expect(ids.size).toBe(1_000);
  });
});

describe('contextFields', () => {
  it('flattens the context for logging', () => {
    runWithContext(
      createContext({
        correlationId: 'abc',
        tenantId: 'acme',
        topic: 'events.orders',
        partition: 7,
        offset: '12345',
      }),
      () => {
        expect(contextFields()).toEqual({
          correlationId: 'abc',
          tenantId: 'acme',
          topic: 'events.orders',
          partition: 7,
          offset: '12345',
        });
      },
    );
  });

  it('omits absent fields instead of emitting nulls', () => {
    // A log backend indexing tenantId: null on every unauthenticated request
    // wastes storage and makes the field useless for filtering.
    runWithContext(createContext({ correlationId: 'abc' }), () => {
      const fields = contextFields();
      expect(Object.keys(fields)).toEqual(['correlationId']);
      expect('tenantId' in fields).toBe(false);
    });
  });

  it('includes free-form attributes', () => {
    runWithContext(
      createContext({ correlationId: 'abc', attributes: { retryTier: '30s' } }),
      () => {
        expect(contextFields()['retryTier']).toBe('30s');
      },
    );
  });
});

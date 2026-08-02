/**
 * Container tests.
 *
 * The properties that matter operationally: singletons really are single (a
 * second Kafka client would silently double the connection count), cycles fail
 * with a readable path rather than a stack overflow, and a failed factory does
 * not corrupt the container for every later resolution.
 */

import { describe, expect, it, vi } from 'vitest';
import {
  CircularDependencyError,
  Container,
  UnregisteredDependencyError,
  token,
} from './container.js';

interface Database {
  query(sql: string): string;
}
interface Cache {
  get(key: string): string | undefined;
}

const DATABASE = token<Database>('Database');
const CACHE = token<Cache>('Cache');
const REPO = token<{ db: Database }>('UserRepository');

describe('resolution', () => {
  it('constructs from a factory', () => {
    const c = new Container();
    c.register(DATABASE, () => ({ query: () => 'row' }));
    expect(c.resolve(DATABASE).query('select 1')).toBe('row');
  });

  it('injects transitive dependencies', () => {
    const c = new Container();
    c.register(DATABASE, () => ({ query: () => 'row' }));
    c.register(REPO, (container) => ({ db: container.resolve(DATABASE) }));
    expect(c.resolve(REPO).db.query('x')).toBe('row');
  });

  it('throws a named error for an unregistered token', () => {
    const c = new Container();
    expect(() => c.resolve(DATABASE)).toThrow(UnregisteredDependencyError);
    // The message must name the token — "cannot resolve Symbol()" is useless.
    expect(() => c.resolve(DATABASE)).toThrow(/Database/);
  });

  it('returns undefined from tryResolve for optional collaborators', () => {
    expect(new Container().tryResolve(CACHE)).toBeUndefined();
  });
});

describe('lifetimes', () => {
  it('constructs a singleton exactly once', () => {
    // This is the property that matters: a second instance would mean a second
    // connection pool, a second consumer group member, a second metrics
    // registry — all of which fail quietly rather than loudly.
    const factory = vi.fn(() => ({ query: () => 'row' }));
    const c = new Container();
    c.register(DATABASE, factory, 'singleton');

    const a = c.resolve(DATABASE);
    const b = c.resolve(DATABASE);

    expect(factory).toHaveBeenCalledTimes(1);
    expect(a).toBe(b);
  });

  it('constructs a transient on every resolution', () => {
    const factory = vi.fn(() => ({ query: () => 'row' }));
    const c = new Container();
    c.register(DATABASE, factory, 'transient');

    expect(c.resolve(DATABASE)).not.toBe(c.resolve(DATABASE));
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it('shares a registered value', () => {
    const value: Database = { query: () => 'fixed' };
    const c = new Container();
    c.registerValue(DATABASE, value);
    expect(c.resolve(DATABASE)).toBe(value);
  });
});

describe('circular dependencies', () => {
  it('reports the full cycle path instead of overflowing the stack', () => {
    // Without detection this is a RangeError with a 10,000-frame stack that
    // names none of the participants. The path is what tells you which edge
    // to break.
    const A = token<object>('ServiceA');
    const B = token<object>('ServiceB');
    const c = new Container();
    c.register(A, (container) => ({ b: container.resolve(B) }));
    c.register(B, (container) => ({ a: container.resolve(A) }));

    expect(() => c.resolve(A)).toThrow(CircularDependencyError);
    expect(() => c.resolve(A)).toThrow(/ServiceA -> ServiceB -> ServiceA/);
  });

  it('does not report a false cycle after a factory throws', () => {
    // Regression guard for the `finally` in resolve(): if the resolving path
    // were popped only on success, one failed construction would poison every
    // later resolution of that token with a phantom cycle error.
    const c = new Container();
    let shouldThrow = true;
    c.register(DATABASE, () => {
      if (shouldThrow) throw new Error('connection refused');
      return { query: () => 'row' };
    });

    expect(() => c.resolve(DATABASE)).toThrow('connection refused');

    shouldThrow = false;
    expect(() => c.resolve(DATABASE)).not.toThrow();
  });
});

describe('overrides and scoping', () => {
  it('lets a later registration replace an earlier one', () => {
    // This is how tests work: build the production container, override the two
    // things that touch the network, leave the rest real.
    const c = new Container();
    c.register(DATABASE, () => ({ query: () => 'production' }));
    c.register(DATABASE, () => ({ query: () => 'fake' }));
    expect(c.resolve(DATABASE).query('x')).toBe('fake');
  });

  it('gives a child scope its own singleton instances', () => {
    // A scope that shared the parent's instances would not be a scope. Request
    // context must not leak between requests.
    const c = new Container();
    c.register(DATABASE, () => ({ query: () => 'row' }), 'singleton');

    const parent = c.resolve(DATABASE);
    const child = c.createScope().resolve(DATABASE);

    expect(child).not.toBe(parent);
  });

  it('shares registered values across scopes', () => {
    // registerValue closes over the instance, so scoping cannot clone it —
    // which is correct for config objects and connection pools.
    const value: Database = { query: () => 'shared' };
    const c = new Container();
    c.registerValue(DATABASE, value);
    expect(c.createScope().resolve(DATABASE)).toBe(value);
  });
});

describe('verify', () => {
  it('eagerly constructs every singleton so wiring fails at startup', () => {
    // The point: a mis-wired graph should fail where a health check catches it
    // and the deploy rolls back, not on the first request that reaches the
    // broken branch — possibly hours later, in production.
    const c = new Container();
    c.register(REPO, (container) => ({ db: container.resolve(DATABASE) }));
    expect(() => c.verify()).toThrow(UnregisteredDependencyError);
  });

  it('passes for a complete graph', () => {
    const c = new Container();
    c.register(DATABASE, () => ({ query: () => 'row' }));
    c.register(REPO, (container) => ({ db: container.resolve(DATABASE) }));
    expect(() => c.verify()).not.toThrow();
  });

  it('does not eagerly construct transients', () => {
    const factory = vi.fn(() => ({ query: () => 'row' }));
    const c = new Container();
    c.register(DATABASE, factory, 'transient');
    c.verify();
    expect(factory).not.toHaveBeenCalled();
  });
});

describe('token typing', () => {
  it('keeps tokens with the same description distinct', () => {
    // Symbols, not strings: two modules can both declare a "Database" token
    // without silently overwriting each other.
    const a = token<Database>('Database');
    const b = token<Database>('Database');
    const c = new Container();
    c.registerValue(a, { query: () => 'a' });
    c.registerValue(b, { query: () => 'b' });
    expect(c.resolve(a).query('')).toBe('a');
    expect(c.resolve(b).query('')).toBe('b');
  });
});

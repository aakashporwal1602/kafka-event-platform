/**
 * Lifecycle tests.
 *
 * These assert the properties that cost data when they regress. A shutdown bug
 * does not fail a build — it fires on every rolling deploy and shows up as a
 * small constant rate of duplicates and 502s that nobody attributes to deploys.
 */

import { describe, expect, it, vi } from 'vitest';
import { Lifecycle } from './lifecycle.js';

/** Silences the default console sink so test output stays readable. */
const quiet = { log: () => undefined };

describe('startup', () => {
  it('runs hooks in registration order', async () => {
    const order: string[] = [];
    const lifecycle = new Lifecycle(quiet)
      .onStart('database', () => void order.push('database'))
      .onStart('kafka', () => void order.push('kafka'))
      .onStart('http', () => void order.push('http'));

    await lifecycle.start();

    expect(order).toEqual(['database', 'kafka', 'http']);
    expect(lifecycle.state).toBe('running');
  });

  it('aborts on the first failure without running later hooks', async () => {
    // A half-started process is worse than one that never started: it may pass
    // a naive health check and then mis-handle traffic.
    const later = vi.fn();
    const lifecycle = new Lifecycle(quiet)
      .onStart('database', () => {
        throw new Error('connection refused');
      })
      .onStart('kafka', later);

    await expect(lifecycle.start()).rejects.toThrow('connection refused');
    expect(later).not.toHaveBeenCalled();
    expect(lifecycle.state).toBe('stopped');
  });

  it('refuses to start twice', async () => {
    const lifecycle = new Lifecycle(quiet);
    await lifecycle.start();
    await expect(lifecycle.start()).rejects.toThrow(/Cannot start/);
  });
});

describe('shutdown ordering', () => {
  it('closes in reverse registration order', async () => {
    // Registration order is dependency order. Registering database then http
    // means http depends on database, so http must close FIRST — otherwise
    // in-flight requests hit a closed pool and return 500s during the drain.
    const order: string[] = [];
    const lifecycle = new Lifecycle(quiet)
      .onClose('database', () => void order.push('database'))
      .onClose('kafka', () => void order.push('kafka'))
      .onClose('http', () => void order.push('http'));

    await lifecycle.shutdown();

    expect(order).toEqual(['http', 'kafka', 'database']);
  });

  it('completes every drain hook before any close hook', async () => {
    // The two-phase rule: stop accepting new work, THEN flush. A single
    // "close everything" phase aborts in-flight work, which is the data loss
    // the drain exists to prevent.
    const order: string[] = [];
    const lifecycle = new Lifecycle(quiet)
      .onDrain('stop-http-listener', async () => {
        await new Promise((r) => setTimeout(r, 5));
        order.push('drain');
      })
      .onClose('flush-producer', () => void order.push('close'));

    await lifecycle.shutdown();

    expect(order).toEqual(['drain', 'close']);
  });

  it('runs drain hooks concurrently', async () => {
    // Drain hooks are independent, and sequencing them would spend the shutdown
    // budget for no ordering benefit — budget that close hooks need.
    const started: string[] = [];
    const lifecycle = new Lifecycle(quiet)
      .onDrain('a', async () => {
        started.push('a');
        await new Promise((r) => setTimeout(r, 20));
      })
      .onDrain('b', async () => {
        started.push('b');
        await new Promise((r) => setTimeout(r, 20));
      });

    const elapsed = await measure(() => lifecycle.shutdown());

    expect(started).toEqual(['a', 'b']);
    expect(elapsed).toBeLessThan(35); // concurrent, not 40ms sequential
  });
});

describe('failure isolation', () => {
  it('keeps running later hooks after one fails', async () => {
    // Aborting on first failure would leave the producer unflushed because an
    // unrelated cache failed to close. Every hook gets its chance to release
    // its own state.
    const ran: string[] = [];
    const lifecycle = new Lifecycle(quiet)
      .onClose('database', () => void ran.push('database'))
      .onClose('cache', () => {
        throw new Error('already disconnected');
      })
      .onClose('http', () => void ran.push('http'));

    await expect(lifecycle.shutdown()).resolves.toBeUndefined();

    // http closes first (reverse order), cache throws, database still runs.
    expect(ran).toEqual(['http', 'database']);
  });

  it('bounds a hanging hook by its own timeout', async () => {
    // Without a per-hook deadline, one hook that never settles consumes the
    // entire global budget and every hook behind it is skipped.
    const after = vi.fn();
    const lifecycle = new Lifecycle({ ...quiet, hookTimeoutMs: 20 })
      .onClose('after-the-bad-one', after)
      .onClose('never-settles', () => new Promise<void>(() => undefined));

    await lifecycle.shutdown();

    expect(after).toHaveBeenCalledOnce();
  });

  it('reports which hook failed', async () => {
    const logged: string[] = [];
    const lifecycle = new Lifecycle({
      log: (_level, message) => void logged.push(message),
    }).onClose('kafka-producer', () => {
      throw new Error('flush failed');
    });

    await lifecycle.shutdown();

    // "shutdown failed" is not actionable at 3am; the hook name is.
    expect(logged.some((m) => m.includes('kafka-producer'))).toBe(true);
  });
});

describe('signal handling', () => {
  it('passes an abort signal to hooks', async () => {
    // Lets a hook abandon a long sleep — a retry consumer waiting on a tier
    // delay — instead of holding the shutdown budget hostage.
    let received: AbortSignal | undefined;
    const lifecycle = new Lifecycle(quiet).onClose('consumer', (signal) => {
      received = signal;
    });

    await lifecycle.shutdown();

    expect(received).toBeInstanceOf(AbortSignal);
    expect(received?.aborted).toBe(false);
  });

  it('is idempotent — a second call awaits the first', async () => {
    // A repeat SIGTERM during a legitimate drain must not start a second
    // shutdown, or hooks run twice and a double flush corrupts state.
    const hook = vi.fn();
    const lifecycle = new Lifecycle(quiet).onClose('kafka', hook);

    await Promise.all([lifecycle.shutdown(), lifecycle.shutdown(), lifecycle.shutdown()]);

    expect(hook).toHaveBeenCalledOnce();
    expect(lifecycle.state).toBe('stopped');
  });
});

describe('state transitions', () => {
  it('moves created → running → stopped', async () => {
    const lifecycle = new Lifecycle(quiet);
    expect(lifecycle.state).toBe('created');
    await lifecycle.start();
    expect(lifecycle.state).toBe('running');
    await lifecycle.shutdown();
    expect(lifecycle.state).toBe('stopped');
  });

  it('allows shutdown without a prior start', async () => {
    // Startup can fail partway; the process still needs to release whatever it
    // did manage to acquire.
    await expect(new Lifecycle(quiet).shutdown()).resolves.toBeUndefined();
  });
});

async function measure(fn: () => Promise<unknown>): Promise<number> {
  const start = process.hrtime.bigint();
  await fn();
  return Number(process.hrtime.bigint() - start) / 1_000_000;
}

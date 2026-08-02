import { describe, expect, it } from 'vitest';
import { FixedClock, SleepAbortedError, SystemClock } from './clock.js';

describe('FixedClock', () => {
  it('does not move unless the test moves it', () => {
    // The whole point: no test can pass by accident because real time elapsed.
    const clock = new FixedClock(1_000);
    expect(clock.now()).toBe(1_000);
    expect(clock.now()).toBe(1_000);
    clock.advance(500);
    expect(clock.now()).toBe(1_500);
  });

  it('resolves a sleep only when time passes its deadline', async () => {
    const clock = new FixedClock(0);
    let resolved = false;
    const sleeping = clock.sleep(1_000).then(() => {
      resolved = true;
    });

    clock.advance(999);
    await Promise.resolve(); // let any continuation run
    expect(resolved).toBe(false);

    clock.advance(1);
    await sleeping;
    expect(resolved).toBe(true);
  });

  it('collapses an hour-long retry schedule into a synchronous assertion', async () => {
    // This is why the abstraction exists. Testing the four retry tiers
    // (5s → 30s → 5m → 1h) against a real clock would take over an hour, so
    // in practice nobody would test the last tier at all.
    const clock = new FixedClock(0);
    const fired: string[] = [];

    const schedule = Promise.all([
      clock.sleep(5_000).then(() => fired.push('5s')),
      clock.sleep(30_000).then(() => fired.push('30s')),
      clock.sleep(300_000).then(() => fired.push('5m')),
      clock.sleep(3_600_000).then(() => fired.push('1h')),
    ]);

    clock.advance(3_600_000);
    await schedule;

    expect(fired).toEqual(['5s', '30s', '5m', '1h']);
  });

  it('cancels a sleep when its signal aborts', async () => {
    // A retry consumer waiting five minutes must abandon that wait on shutdown,
    // or graceful termination takes five minutes and Kubernetes SIGKILLs first.
    const clock = new FixedClock(0);
    const controller = new AbortController();

    const sleeping = clock.sleep(300_000, controller.signal);
    expect(clock.pendingSleepers()).toBe(1);

    controller.abort();
    await expect(sleeping).rejects.toThrow(SleepAbortedError);
    expect(clock.pendingSleepers()).toBe(0);
  });

  it('rejects immediately when the signal is already aborted', async () => {
    const clock = new FixedClock(0);
    const controller = new AbortController();
    controller.abort();
    await expect(clock.sleep(1_000, controller.signal)).rejects.toThrow(SleepAbortedError);
  });

  it('refuses to advance backwards', () => {
    // Monotonic time moving backwards would break every elapsed-time
    // calculation built on it, so the mistake fails loudly.
    expect(() => new FixedClock(0).advance(-1)).toThrow(RangeError);
  });
});

describe('monotonic vs wall-clock', () => {
  it('keeps monotonic time unmoved when wall-clock jumps backwards', () => {
    // Simulates an NTP correction. Code measuring durations with now() sees a
    // negative elapsed time; code using monotonic() is unaffected. That is the
    // entire reason both exist.
    const clock = new FixedClock(10_000);
    const startWall = clock.now();
    const startMono = clock.monotonic();

    clock.setTime(9_000); // NTP steps the clock back one second

    expect(clock.now() - startWall).toBeLessThan(0);
    expect(clock.monotonic() - startMono).toBe(0n);
  });

  it('advances both when time moves forward normally', () => {
    const clock = new FixedClock(0);
    const startMono = clock.monotonic();
    clock.advance(250);
    expect(clock.now()).toBe(250);
    expect(clock.monotonic() - startMono).toBe(250_000_000n); // ms → ns
  });
});

describe('SystemClock', () => {
  it('never reports monotonic time going backwards', () => {
    const clock = new SystemClock();
    let previous = clock.monotonic();
    for (let i = 0; i < 1_000; i++) {
      const current = clock.monotonic();
      expect(current).toBeGreaterThanOrEqual(previous);
      previous = current;
    }
  });

  it('sleeps for at least the requested duration', async () => {
    const clock = new SystemClock();
    const start = clock.monotonic();
    await clock.sleep(20);
    const elapsedMs = Number(clock.monotonic() - start) / 1_000_000;
    // Lower bound only. Timers fire late under load, never early, and asserting
    // an upper bound here is how a suite becomes flaky on a busy CI runner.
    expect(elapsedMs).toBeGreaterThanOrEqual(15);
  });

  it('rejects a real sleep on abort', async () => {
    const clock = new SystemClock();
    const controller = new AbortController();
    const sleeping = clock.sleep(10_000, controller.signal);
    controller.abort();
    await expect(sleeping).rejects.toThrow(SleepAbortedError);
  });
});

/**
 * Time as an injected dependency.
 *
 * ── Why this exists ────────────────────────────────────────────────────────
 * Almost everything downstream is time-dependent: retry backoff schedules
 * (ADR-0005), idempotency-key TTLs (ADR-0008), token-bucket refill, replay
 * windows, lock expiry. Calling `Date.now()` inside that logic makes it
 * untestable by anything except `setTimeout` and hope — and a test that waits
 * one real hour to prove the fourth retry tier fires is a test nobody runs.
 *
 * Worse, it makes the logic *non-deterministic*: a suite that passes locally
 * fails on a slow CI runner because a 5 ms assumption did not hold. The usual
 * response is to add a sleep, which is how a suite becomes flaky.
 *
 * ── Rejected alternatives ──────────────────────────────────────────────────
 * • Global mocking (vi.useFakeTimers, sinon). Works, but it patches globals for
 *   the whole process, so it leaks between tests and interacts badly with
 *   anything that legitimately needs real time (Testcontainers startup, socket
 *   timeouts). It also hides the dependency: reading the code, you cannot see
 *   that this function depends on time.
 * • Passing `now: number` through every signature. Honest, but it pollutes every
 *   API and callers end up threading a timestamp through five layers.
 * • Luxon/dayjs abstractions. Solves formatting, not injectability.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * Every time-dependent class takes one more constructor parameter, and callers
 * must remember not to reach for `Date.now()`. That second part is a convention,
 * not a compiler guarantee — an ESLint `no-restricted-globals` rule would
 * enforce it, and is worth adding once there is enough code to protect.
 *
 * ── Monotonic vs wall-clock ────────────────────────────────────────────────
 * These are genuinely different and confusing them causes real bugs:
 *
 *   now()       wall-clock, ms since epoch. Can jump backwards (NTP correction,
 *               leap-second smearing, VM migration, an operator setting the
 *               clock). Use for: timestamps recorded on an event, TTL deadlines
 *               that must survive a restart.
 *   monotonic() nanoseconds from an arbitrary origin. Never goes backwards.
 *               Use for: measuring elapsed time, latency histograms, timeouts.
 *
 * Measuring a duration with `now()` is the classic mistake: an NTP step during
 * the measurement yields a negative latency, which then poisons a histogram or
 * — worse — makes a timeout appear to have not yet elapsed, so a request hangs
 * until the next correction.
 */

export interface Clock {
  /** Wall-clock milliseconds since the Unix epoch. May move backwards. */
  now(): number;

  /** Wall-clock instant. Convenience over `new Date(clock.now())`. */
  date(): Date;

  /**
   * Monotonic nanoseconds from an arbitrary origin. Never moves backwards.
   * Only differences between two readings are meaningful.
   */
  monotonic(): bigint;

  /**
   * Resolve after at least `ms`, or reject if the signal aborts first.
   *
   * Takes an AbortSignal because every sleep in this platform is cancellable:
   * a retry consumer waiting 5 minutes for a tier delay must abandon that wait
   * immediately on shutdown, or graceful termination takes 5 minutes and
   * Kubernetes SIGKILLs the pod instead.
   */
  sleep(ms: number, signal?: AbortSignal): Promise<void>;
}

/** Production implementation. */
export class SystemClock implements Clock {
  public now(): number {
    return Date.now();
  }

  public date(): Date {
    return new Date();
  }

  public monotonic(): bigint {
    return process.hrtime.bigint();
  }

  public async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new SleepAbortedError();

    // `return await` rather than `return`: without it this frame is gone by the
    // time the promise settles, so a rejection has no stack pointing back here.
    return await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort);
        resolve();
      }, ms);

      const onAbort = (): void => {
        clearTimeout(timer);
        reject(new SleepAbortedError());
      };

      // { once: true } prevents the listener accumulating if a single signal is
      // reused across many sleeps — which it is, since one shutdown signal is
      // shared by every waiting consumer.
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }
}

/**
 * Deterministic clock for tests.
 *
 * Time only moves when the test moves it. That turns "does the fourth retry
 * tier fire after an hour" from a one-hour test into a synchronous assertion.
 */
export class FixedClock implements Clock {
  #nowMs: number;
  #monotonicNs: bigint;
  /** Sleepers waiting for a wake time, so advance() can release them. */
  readonly #pending: { at: number; resolve: () => void; reject: (e: Error) => void }[] = [];

  constructor(startMs = 0) {
    this.#nowMs = startMs;
    this.#monotonicNs = BigInt(startMs) * 1_000_000n;
  }

  public now(): number {
    return this.#nowMs;
  }

  public date(): Date {
    return new Date(this.#nowMs);
  }

  public monotonic(): bigint {
    return this.#monotonicNs;
  }

  /**
   * Register a sleeper. It resolves only when {@link advance} passes its
   * deadline — never on its own, so a test cannot accidentally pass by waiting.
   */
  public async sleep(ms: number, signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) throw new SleepAbortedError();

    return await new Promise<void>((resolve, reject) => {
      const entry = { at: this.#nowMs + ms, resolve, reject };
      this.#pending.push(entry);
      signal?.addEventListener(
        'abort',
        () => {
          const i = this.#pending.indexOf(entry);
          if (i >= 0) this.#pending.splice(i, 1);
          reject(new SleepAbortedError());
        },
        { once: true },
      );
    });
  }

  /** Move time forward and release any sleeper whose deadline has passed. */
  public advance(ms: number): void {
    if (ms < 0) throw new RangeError('advance() cannot move time backwards; use setTime()');
    this.#nowMs += ms;
    this.#monotonicNs += BigInt(ms) * 1_000_000n;

    // Splice out the due sleepers before resolving. Resolving first would let a
    // continuation schedule another sleep into the array we are still iterating.
    const due = this.#pending.filter((p) => p.at <= this.#nowMs);
    for (const entry of due) {
      const i = this.#pending.indexOf(entry);
      if (i >= 0) this.#pending.splice(i, 1);
    }
    for (const entry of due) entry.resolve();
  }

  /**
   * Set wall-clock time directly, including backwards.
   *
   * Exists to test NTP-correction behaviour: code that measures durations with
   * `now()` instead of `monotonic()` produces a negative elapsed time here,
   * which is exactly the bug this class helps catch. Monotonic time is
   * deliberately not moved.
   */
  public setTime(ms: number): void {
    this.#nowMs = ms;
  }

  /** How many sleepers are waiting. Asserts that cancellation actually cancelled. */
  public pendingSleepers(): number {
    return this.#pending.length;
  }
}

/** Thrown when a sleep is cancelled by its AbortSignal. */
export class SleepAbortedError extends Error {
  constructor() {
    super('Sleep aborted');
    this.name = 'SleepAbortedError';
  }
}

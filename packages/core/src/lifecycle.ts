/**
 * Startup and graceful shutdown orchestration.
 *
 * ── Why shutdown is the hard half ──────────────────────────────────────────
 * Startup failing is loud: the process exits, the deploy rolls back. Shutdown
 * failing is silent, and every failure mode costs data or availability:
 *
 *   Consumer killed mid-batch          → messages redelivered. Survivable only
 *                                        because handlers are idempotent
 *                                        (ADR-0008); without that, duplicates.
 *   Producer killed with a full buffer → acknowledged-to-the-caller events that
 *                                        never reached a broker. Silent loss.
 *   HTTP server killed with requests
 *   in flight                          → 502s for users mid-transaction.
 *   Shutdown that never completes      → Kubernetes SIGKILLs after
 *                                        terminationGracePeriodSeconds and every
 *                                        guarantee above is void anyway.
 *
 * On every rolling deploy, every pod does this. A shutdown bug is not rare —
 * it fires several times a day and shows up as a small, constant background
 * rate of duplicates and 502s that nobody attributes to deploys.
 *
 * ── The ordering rule ──────────────────────────────────────────────────────
 * Shutdown runs hooks in REVERSE registration order, because registration order
 * is dependency order. Registering `database` then `httpServer` means the server
 * depends on the database — so the server must stop first, or in-flight requests
 * hit a closed pool and return 500s during what should be a clean drain.
 *
 * This mirrors how a stack unwinds, and is why registration order is the API
 * rather than an explicit priority number: a number is a second thing to keep
 * consistent, and it drifts.
 *
 * ── Stop accepting before you stop working ─────────────────────────────────
 * Correct shutdown has two phases, and conflating them is the usual bug:
 *
 *   1. DRAIN  — stop accepting new work. Close the HTTP listener, pause the
 *               consumer. In-flight work continues.
 *   2. CLOSE  — finish and release. Flush the producer, commit offsets, close
 *               pools.
 *
 * A single "close everything" phase aborts in-flight work, which is exactly the
 * data loss we are trying to avoid.
 *
 * ── The forced-exit timer ──────────────────────────────────────────────────
 * Every hook gets a deadline, and the whole shutdown gets one. If a hook hangs —
 * a socket that never closes, a promise that never settles — we log which one
 * and exit non-zero rather than waiting for SIGKILL. Exiting deliberately means
 * the failure is attributable; being SIGKILLed leaves no evidence at all.
 */

import { type Clock, SystemClock } from './clock.js';

/** Work to perform during shutdown. */
export type ShutdownHook = (signal: AbortSignal) => Promise<void> | void;

/** Work to perform during startup. Failure aborts the process. */
export type StartupHook = () => Promise<void> | void;

export interface LifecycleOptions {
  /**
   * Deadline for the whole shutdown.
   *
   * Must be comfortably below Kubernetes' terminationGracePeriodSeconds
   * (default 30s), or SIGKILL arrives mid-shutdown and the ordering guarantees
   * above are void. 25s leaves 5s of headroom for the kubelet.
   */
  readonly shutdownTimeoutMs?: number;

  /**
   * Per-hook deadline. Bounds the blast radius of one bad hook: without it, a
   * single hanging hook consumes the entire global budget and the hooks after
   * it never run at all.
   */
  readonly hookTimeoutMs?: number;

  /** Injected so shutdown timing is testable without real waiting. */
  readonly clock?: Clock;

  /** Structured sink. Defaults to console because a logger may already be closed. */
  readonly log?: (level: 'info' | 'warn' | 'error', message: string, fields?: object) => void;
}

interface NamedHook {
  readonly name: string;
  readonly hook: ShutdownHook;
}

export type LifecycleState = 'created' | 'starting' | 'running' | 'draining' | 'stopped';

export class Lifecycle {
  readonly #startupHooks: { name: string; hook: StartupHook }[] = [];
  readonly #drainHooks: NamedHook[] = [];
  readonly #closeHooks: NamedHook[] = [];
  readonly #shutdownTimeoutMs: number;
  readonly #hookTimeoutMs: number;
  readonly #clock: Clock;
  readonly #log: NonNullable<LifecycleOptions['log']>;

  #state: LifecycleState = 'created';
  /** Resolves when shutdown completes, so repeat signals can await it. */
  #shutdownPromise: Promise<void> | undefined;

  constructor(options: LifecycleOptions = {}) {
    this.#shutdownTimeoutMs = options.shutdownTimeoutMs ?? 25_000;
    this.#hookTimeoutMs = options.hookTimeoutMs ?? 10_000;
    this.#clock = options.clock ?? new SystemClock();
    // Defaults to console rather than the platform logger: shutdown hooks close
    // the logger's transport, so anything logged after that point would vanish.
    // Only warn/error are used, which is what the no-console rule permits —
    // and shutdown progress genuinely belongs at warn or above anyway, since
    // it is only interesting when something is going wrong.
    this.#log =
      options.log ??
      ((level, message, fields) => {
        const line = `[lifecycle] ${message}`;
        if (level === 'error') console.error(line, fields ?? '');
        else console.warn(line, fields ?? '');
      });
  }

  public get state(): LifecycleState {
    return this.#state;
  }

  /** Register startup work. Runs in registration order. */
  public onStart(name: string, hook: StartupHook): this {
    this.#startupHooks.push({ name, hook });
    return this;
  }

  /**
   * Register phase-1 work: stop accepting new work, let in-flight work finish.
   *
   * Close the HTTP listener here, pause the consumer here. Do NOT close
   * connection pools here — in-flight requests still need them.
   */
  public onDrain(name: string, hook: ShutdownHook): this {
    this.#drainHooks.push({ name, hook });
    return this;
  }

  /**
   * Register phase-2 work: flush and release.
   *
   * Runs in REVERSE registration order, so dependencies close after their
   * dependents. Register in dependency order and this is automatic.
   */
  public onClose(name: string, hook: ShutdownHook): this {
    this.#closeHooks.push({ name, hook });
    return this;
  }

  /** Run startup hooks in order. Any failure aborts — a half-started process is worse than none. */
  public async start(): Promise<void> {
    if (this.#state !== 'created') throw new Error(`Cannot start from state "${this.#state}"`);
    this.#state = 'starting';

    for (const { name, hook } of this.#startupHooks) {
      const startedAt = this.#clock.monotonic();
      try {
        await hook();
      } catch (error: unknown) {
        this.#state = 'stopped';
        this.#log('error', `startup hook "${name}" failed`, { error: String(error) });
        throw error;
      }
      this.#log('info', `started ${name}`, { durationMs: this.#elapsedMs(startedAt) });
    }

    this.#state = 'running';
  }

  /**
   * Install SIGTERM/SIGINT handlers.
   *
   * SIGTERM is what Kubernetes and Docker send; SIGINT is Ctrl-C. Both mean the
   * same thing here.
   *
   * Repeat signals are deliberately ignored rather than escalating: a second
   * SIGTERM during a legitimate 20-second drain would abort it and cause exactly
   * the data loss the drain exists to prevent. An operator who genuinely wants
   * an immediate stop sends SIGKILL, which we cannot and should not intercept.
   */
  public installSignalHandlers(): void {
    const handle = (signal: NodeJS.Signals): void => {
      if (this.#shutdownPromise) {
        this.#log('warn', `${signal} received during shutdown, ignoring`, {
          hint: 'send SIGKILL to force immediate termination',
        });
        return;
      }
      this.#log('info', `${signal} received, shutting down`);
      void this.shutdown().then(
        () => process.exit(0),
        () => process.exit(1),
      );
    };

    process.once('SIGTERM', handle);
    process.once('SIGINT', handle);

    // An unhandled rejection means a promise failed with nobody watching. The
    // process is in an unknown state, so the only safe response is to shut down
    // cleanly and let the orchestrator restart us — continuing risks acting on
    // corrupt state.
    process.once('unhandledRejection', (reason) => {
      this.#log('error', 'unhandled rejection, shutting down', { reason: String(reason) });
      void this.shutdown().finally(() => process.exit(1));
    });

    process.once('uncaughtException', (error) => {
      this.#log('error', 'uncaught exception, shutting down', { error: error.message });
      void this.shutdown().finally(() => process.exit(1));
    });
  }

  /**
   * Drain, then close, within the global deadline. Idempotent.
   *
   * Never rejects for hook failures: a failing hook is logged and the remaining
   * hooks still run. Aborting the sequence on the first failure would leave a
   * producer unflushed because an unrelated cache failed to close.
   */
  public async shutdown(): Promise<void> {
    this.#shutdownPromise ??= this.#doShutdown();
    return await this.#shutdownPromise;
  }

  async #doShutdown(): Promise<void> {
    this.#state = 'draining';
    const startedAt = this.#clock.monotonic();

    // One controller for the whole shutdown. Hooks receive its signal so a
    // long sleep — a retry consumer waiting on a tier delay — can abandon
    // immediately instead of holding the budget hostage.
    const controller = new AbortController();
    const globalTimer = setTimeout(() => {
      controller.abort();
      this.#log('error', 'shutdown exceeded its deadline, forcing exit', {
        timeoutMs: this.#shutdownTimeoutMs,
      });
      process.exit(1);
    }, this.#shutdownTimeoutMs);
    // Do not let this timer keep the event loop alive: if everything else
    // finishes, the process should exit rather than wait out the deadline.
    globalTimer.unref();

    try {
      // Phase 1 — stop accepting. Concurrent: these are independent, and
      // sequencing them would waste the budget for no ordering benefit.
      await Promise.all(this.#drainHooks.map((h) => this.#runHook(h, controller.signal, 'drain')));

      // Phase 2 — flush and release, reverse order so dependencies outlive
      // their dependents. Sequential: order is the entire point.
      for (const hook of [...this.#closeHooks].reverse()) {
        await this.#runHook(hook, controller.signal, 'close');
      }

      this.#state = 'stopped';
      this.#log('info', 'shutdown complete', { durationMs: this.#elapsedMs(startedAt) });
    } finally {
      clearTimeout(globalTimer);
    }
  }

  async #runHook(named: NamedHook, signal: AbortSignal, phase: string): Promise<void> {
    const startedAt = this.#clock.monotonic();
    let timer: NodeJS.Timeout | undefined;

    try {
      // Race the hook against its own deadline. One slow hook must not consume
      // the global budget and starve the hooks queued behind it.
      await Promise.race([
        Promise.resolve(named.hook(signal)),
        new Promise<never>((_, reject) => {
          timer = setTimeout(
            () => reject(new Error(`hook "${named.name}" exceeded ${this.#hookTimeoutMs}ms`)),
            this.#hookTimeoutMs,
          );
        }),
      ]);
      this.#log('info', `${phase}: ${named.name}`, { durationMs: this.#elapsedMs(startedAt) });
    } catch (error: unknown) {
      // Swallowed on purpose — see shutdown(). One failure must not prevent the
      // remaining hooks from flushing their own state.
      this.#log('error', `${phase} hook "${named.name}" failed`, {
        error: String(error),
        durationMs: this.#elapsedMs(startedAt),
      });
    } finally {
      // Without this, a hook that finishes quickly still leaves a pending timer
      // holding the event loop open until its deadline elapses.
      if (timer) clearTimeout(timer);
    }
  }

  #elapsedMs(sinceMonotonicNs: bigint): number {
    return Number(this.#clock.monotonic() - sinceMonotonicNs) / 1_000_000;
  }
}

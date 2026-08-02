/**
 * Request context propagation via AsyncLocalStorage.
 *
 * ── The problem ────────────────────────────────────────────────────────────
 * A single logical operation crosses many stack frames and several async
 * boundaries: gateway → producer → Kafka → consumer → downstream call. When one
 * of those emits a log line or a metric, it needs to say *which* request it
 * belongs to. Without that, a production incident looks like ten thousand
 * unrelated log lines and there is no way to reconstruct one failing journey.
 *
 * ── Rejected alternatives ──────────────────────────────────────────────────
 * • Thread a `ctx` parameter through every signature. Explicit and honest, and
 *   it is what Go does — but it means every function in every layer grows a
 *   parameter it does not use, purely to pass along. In practice teams give up
 *   and the correlation ID is missing exactly where it matters.
 * • A module-level mutable variable. Broken under concurrency: Node handles
 *   interleaved requests on one thread, so request B overwrites request A's
 *   context the moment A awaits anything.
 * • Pass it only in the logger. Works for logs, useless for metrics, traces and
 *   error context.
 *
 * ── How AsyncLocalStorage works ────────────────────────────────────────────
 * Node maintains a context store that follows the *async execution chain*, not
 * the call stack. Anything started inside `run()` — including work resumed after
 * an `await`, a `setTimeout` callback, or a promise continuation — sees the same
 * store. Two concurrent requests each get their own, with no interference.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * • A measurable but small overhead: AsyncLocalStorage is built on async_hooks,
 *   which adds bookkeeping per async resource. Since Node 16 the fast path is
 *   good enough that the cost is not the bottleneck at our target throughput —
 *   but it is not free, and at 100× this scale it is worth re-measuring.
 * • Context is *invisible* in signatures. A function that reads it does not
 *   declare that it does, which is the same criticism we levelled at globals.
 *   The mitigation is discipline: only cross-cutting concerns (logging, metrics,
 *   tracing) read the context. Business logic takes explicit parameters.
 * • It does not cross process boundaries. Propagating over Kafka is a separate
 *   mechanism — headers — implemented in the producer/consumer (Chapters 4, 7).
 *   This module only guarantees continuity *within* one process.
 */

import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';

/**
 * Everything that identifies the logical operation currently executing.
 *
 * Immutable: a context is replaced, never mutated, so a nested scope cannot
 * corrupt its parent's view.
 */
export interface RequestContext {
  /**
   * Correlates every log, metric and event belonging to one logical operation,
   * across services. Generated at the edge if the caller did not supply one.
   */
  readonly correlationId: string;

  /** W3C trace context, when a tracer is active. Chapter 12 populates these. */
  readonly traceId?: string;
  readonly spanId?: string;

  /** Which tenant this operation belongs to. Drives quota and ACL decisions. */
  readonly tenantId?: string;

  /** Authenticated principal, when there is one. */
  readonly userId?: string;

  /** Set when the current work originated from a Kafka message rather than HTTP. */
  readonly eventId?: string;
  readonly topic?: string;
  readonly partition?: number;
  readonly offset?: string;

  /** Free-form additions. Kept narrow so it cannot become a dumping ground. */
  readonly attributes?: Readonly<Record<string, string | number | boolean>>;
}

const storage = new AsyncLocalStorage<RequestContext>();

/**
 * Run `fn` with `context` installed for the whole async subtree.
 *
 * Anything awaited inside — including work resumed on a later tick — observes
 * this context. Two concurrent calls do not interfere.
 */
export function runWithContext<T>(context: RequestContext, fn: () => T): T {
  return storage.run(context, fn);
}

/**
 * The current context, or undefined outside any `runWithContext`.
 *
 * Returns undefined rather than throwing: a logger must work during startup and
 * shutdown, where no request is in flight. A logger that threw because there was
 * no correlation ID would take down the process at exactly the wrong moment.
 */
export function currentContext(): RequestContext | undefined {
  return storage.getStore();
}

/**
 * The current correlation ID, or a placeholder.
 *
 * The placeholder is a visible, greppable marker rather than an empty string:
 * seeing `no-correlation-id` in a log line tells you the context was lost,
 * which is itself a bug worth finding.
 */
export function correlationId(): string {
  return storage.getStore()?.correlationId ?? 'no-correlation-id';
}

/**
 * Run `fn` with additional fields merged into the current context.
 *
 * Used when work narrows — a consumer handling a specific message adds the
 * topic, partition and offset to whatever context it inherited. The parent's
 * context is untouched, because a new object is created.
 */
export function withContext<T>(additions: Partial<RequestContext>, fn: () => T): T {
  const parent = storage.getStore();
  const merged: RequestContext = {
    ...(parent ?? { correlationId: newCorrelationId() }),
    ...additions,
    // Attributes merge rather than replace, so a nested scope adding one field
    // does not silently discard the fields its parent set.
    attributes: { ...parent?.attributes, ...additions.attributes },
  };
  return storage.run(merged, fn);
}

/** Generate a correlation ID. UUIDv4 — no coordination, no collisions in practice. */
export function newCorrelationId(): string {
  return randomUUID();
}

/**
 * Build a context at a system entry point, honouring an inbound ID if present.
 *
 * Honouring the caller's ID is the whole point of correlation: if the gateway
 * generated a fresh one per hop, a single user journey would appear as N
 * unrelated operations and joining them across services would be impossible.
 */
export function createContext(init: Partial<RequestContext> = {}): RequestContext {
  return {
    correlationId: init.correlationId ?? newCorrelationId(),
    ...init,
  };
}

/**
 * Flatten the context into log-ready fields.
 *
 * Undefined values are dropped rather than emitted as nulls — a log backend
 * indexing `tenantId: null` on every unauthenticated request wastes storage and
 * makes the field useless for filtering.
 */
export function contextFields(): Record<string, string | number | boolean> {
  const ctx = storage.getStore();
  if (!ctx) return {};

  const fields: Record<string, string | number | boolean> = {
    correlationId: ctx.correlationId,
  };
  if (ctx.traceId !== undefined) fields['traceId'] = ctx.traceId;
  if (ctx.spanId !== undefined) fields['spanId'] = ctx.spanId;
  if (ctx.tenantId !== undefined) fields['tenantId'] = ctx.tenantId;
  if (ctx.userId !== undefined) fields['userId'] = ctx.userId;
  if (ctx.eventId !== undefined) fields['eventId'] = ctx.eventId;
  if (ctx.topic !== undefined) fields['topic'] = ctx.topic;
  if (ctx.partition !== undefined) fields['partition'] = ctx.partition;
  if (ctx.offset !== undefined) fields['offset'] = ctx.offset;
  return { ...fields, ...ctx.attributes };
}

/**
 * The header name used to propagate correlation across HTTP and Kafka.
 *
 * `x-correlation-id` rather than inventing a name, because it is the de-facto
 * convention and proxies, load balancers and APM agents already recognise it.
 * W3C `traceparent` (Chapter 12) carries the trace context separately — the two
 * are complementary: traceparent identifies the distributed trace, correlationId
 * identifies the business operation, and they do not always have the same
 * lifetime.
 */
export const CORRELATION_HEADER = 'x-correlation-id';

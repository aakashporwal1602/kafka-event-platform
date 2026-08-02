/**
 * Error taxonomy.
 *
 * ── Why this is the most important file in the package ─────────────────────
 * The retry engine (ADR-0005) routes a failed message to a retry tier or to the
 * DLQ based on exactly one question: **is this error worth retrying?**
 *
 * Getting that wrong is expensive in both directions:
 *
 *   Retrying a permanent error   → 4 wasted attempts over an hour, per message.
 *                                  During an incident with a malformed producer,
 *                                  that multiplies traffic against a dependency
 *                                  that was never going to succeed.
 *   DLQ-ing a transient error    → a 3-second downstream blip silently sends
 *                                  thousands of valid events to quarantine, and
 *                                  someone has to redrive them by hand.
 *
 * So retryability is not a judgement call made at the catch site — it is a
 * property carried by the error itself, decided once by the code closest to the
 * failure. Handlers classify; the retry engine only reads.
 *
 * ── Rejected alternatives ──────────────────────────────────────────────────
 * • String matching on error messages. Brittle, and it breaks silently when a
 *   library rewords a message in a patch release.
 * • A retryable-error allowlist in the retry engine. Puts the knowledge in the
 *   wrong place: the engine would need to know about every dependency's error
 *   vocabulary, and every new dependency becomes an edit to shared code.
 * • Error codes only, without a class hierarchy. Loses `instanceof` narrowing
 *   and stack traces.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * Every adapter must translate its vendor's errors into this taxonomy. That is
 * real work at every boundary, and it is the point: an untranslated vendor error
 * escaping into the platform is an adapter that has failed at its one job.
 *
 * ── The default is PERMANENT, deliberately ─────────────────────────────────
 * An unclassified error goes to the DLQ rather than being retried. Retrying an
 * unknown error is the more dangerous default: it amplifies load during an
 * incident, and the failure is invisible because retries look like normal
 * traffic. A DLQ message is loud — it alerts, and a human looks at it.
 */

/** Machine-readable, stable across releases. Safe to alert and branch on. */
export type ErrorCode =
  // Transient — worth retrying
  | 'BROKER_UNAVAILABLE'
  | 'NOT_ENOUGH_REPLICAS'
  | 'REQUEST_TIMEOUT'
  | 'DEPENDENCY_UNAVAILABLE'
  | 'RATE_LIMITED'
  | 'LOCK_CONTENTION'
  // Permanent — retrying cannot help
  | 'SCHEMA_VALIDATION_FAILED'
  | 'SCHEMA_INCOMPATIBLE'
  | 'SCHEMA_NOT_FOUND'
  | 'TOPIC_NOT_FOUND'
  | 'MESSAGE_TOO_LARGE'
  | 'MALFORMED_PAYLOAD'
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'INVALID_CONFIGURATION'
  | 'DUPLICATE_EVENT'
  // Fallback
  | 'UNKNOWN';

/** Structured context attached to an error, emitted with logs and DLQ records. */
export type ErrorContext = Readonly<Record<string, string | number | boolean | null>>;

/**
 * Base class for every error the platform raises deliberately.
 *
 * `retryable` is abstract rather than a constructor argument on purpose: it
 * forces each subclass to take a position, and makes that position visible in
 * the type rather than at each throw site where it could vary by accident.
 */
export abstract class PlatformError extends Error {
  public abstract readonly retryable: boolean;
  public abstract readonly code: ErrorCode;

  /** Correlates this error with the request/message that produced it. */
  public readonly context: ErrorContext;

  /**
   * The underlying error, preserved rather than flattened to a string.
   *
   * Native `cause` (ES2022) is used so that `console.error` and any structured
   * logger walk the chain automatically. Flattening to a message loses the
   * original stack, which is exactly what you need at 3am.
   */
  public override readonly cause?: Error | undefined;

  constructor(message: string, context: ErrorContext = {}, cause?: Error) {
    super(message, cause ? { cause } : undefined);
    // Without this, `error.name` reports "Error" for every subclass, which
    // makes log aggregation useless — every distinct failure looks identical.
    this.name = new.target.name;
    this.context = context;
    this.cause = cause;

    // V8-only: drops this constructor from the stack so the trace starts at the
    // throw site. Guarded because it does not exist on other engines.
    if (Error.captureStackTrace) Error.captureStackTrace(this, new.target);
  }

  /** Serialisable form for structured logs and DLQ records. */
  public toJSON(): Record<string, unknown> {
    return {
      name: this.name,
      code: this.code,
      message: this.message,
      retryable: this.retryable,
      context: this.context,
      cause: this.cause ? { name: this.cause.name, message: this.cause.message } : undefined,
    };
  }
}

/**
 * A failure that a later attempt could plausibly succeed at.
 *
 * The test: **would this same message, unchanged, succeed in 30 seconds?**
 * If the answer depends on the message content rather than the environment,
 * it is not transient.
 */
export abstract class TransientError extends PlatformError {
  public override readonly retryable = true as const;
}

/**
 * A failure that no number of retries can fix.
 *
 * Retrying these is not merely useless — it is harmful. Four attempts against a
 * message that will never validate is four times the load and an hour of delay
 * before anyone sees it in the DLQ.
 */
export abstract class PermanentError extends PlatformError {
  public override readonly retryable = false as const;
}

/* ── Transient ───────────────────────────────────────────────────────────── */

/** Broker unreachable, connection refused, leader election in progress. */
export class BrokerUnavailableError extends TransientError {
  public override readonly code = 'BROKER_UNAVAILABLE' as const;
}

/**
 * ISR fell below `min.insync.replicas`, so the write was rejected.
 *
 * Correctly transient: this is the cluster refusing a write it cannot make
 * durable (HLD §4). It resolves as soon as a broker rejoins the ISR, and
 * retrying is exactly right.
 */
export class NotEnoughReplicasError extends TransientError {
  public override readonly code = 'NOT_ENOUGH_REPLICAS' as const;
}

/**
 * A request exceeded its deadline.
 *
 * ⚠ A timeout is ambiguous, not failed: the operation may have succeeded and
 * the acknowledgement been lost. Retrying is therefore only safe because every
 * write path carries an idempotency key (ADR-0008). Without that, retrying a
 * timeout is how duplicates get created.
 */
export class TimeoutError extends TransientError {
  public override readonly code = 'REQUEST_TIMEOUT' as const;
}

/** A downstream service returned 5xx, refused the connection, or is circuit-broken. */
export class DependencyUnavailableError extends TransientError {
  public override readonly code = 'DEPENDENCY_UNAVAILABLE' as const;
}

/**
 * A quota was exhausted — ours or a downstream's.
 *
 * Transient by definition: the window resets. The retry tier delay should
 * respect `Retry-After` where the dependency supplies one, rather than using
 * the default backoff, which may retry sooner than permitted.
 */
export class RateLimitedError extends TransientError {
  public override readonly code = 'RATE_LIMITED' as const;
}

/** Failed to acquire a distributed lock within its timeout. */
export class LockContentionError extends TransientError {
  public override readonly code = 'LOCK_CONTENTION' as const;
}

/* ── Permanent ───────────────────────────────────────────────────────────── */

/**
 * The payload does not conform to its registered schema.
 *
 * Permanent because the message will never validate — the producer must be
 * fixed. This is precisely the failure the schema registry exists to catch in
 * CI (ADR-0006); reaching a consumer means something bypassed that gate.
 */
export class SchemaValidationError extends PermanentError {
  public override readonly code = 'SCHEMA_VALIDATION_FAILED' as const;
}

/** A schema registration was rejected by the configured compatibility mode. */
export class SchemaIncompatibleError extends PermanentError {
  public override readonly code = 'SCHEMA_INCOMPATIBLE' as const;
}

/**
 * The schema ID in a message's wire header is not in the registry.
 *
 * Permanent despite looking like a lookup failure: a schema ID is immutable and
 * never garbage-collected (the registry topic is compacted with infinite
 * retention). An unknown ID means the message came from a different registry,
 * or is corrupt. Retrying against the same registry cannot change the outcome.
 */
export class SchemaNotFoundError extends PermanentError {
  public override readonly code = 'SCHEMA_NOT_FOUND' as const;
}

/**
 * The topic does not exist.
 *
 * Permanent because auto-creation is disabled cluster-wide (ADR-0001 rationale):
 * the topic will not appear on its own. Almost always a typo in a topic name,
 * which is exactly the failure disabling auto-creation was meant to surface.
 */
export class TopicNotFoundError extends PermanentError {
  public override readonly code = 'TOPIC_NOT_FOUND' as const;
}

/** Payload exceeds `max.message.bytes`. Fix: claim-check pattern, not retry. */
export class MessageTooLargeError extends PermanentError {
  public override readonly code = 'MESSAGE_TOO_LARGE' as const;
}

/** Undecodable bytes — truncated, wrong wire format, corrupt. */
export class MalformedPayloadError extends PermanentError {
  public override readonly code = 'MALFORMED_PAYLOAD' as const;
}

/** Missing or invalid credentials. */
export class UnauthorizedError extends PermanentError {
  public override readonly code = 'UNAUTHORIZED' as const;
}

/** Authenticated, but not permitted to perform this operation on this resource. */
export class ForbiddenError extends PermanentError {
  public override readonly code = 'FORBIDDEN' as const;
}

/** Invalid configuration. Thrown at startup — the process should not begin. */
export class InvalidConfigurationError extends PermanentError {
  public override readonly code = 'INVALID_CONFIGURATION' as const;
}

/**
 * The event was already processed — deduplication hit (ADR-0008).
 *
 * Not really a failure: it is the idempotency layer working. Modelled as a
 * permanent error so it can never be retried, and so it is countable —
 * a sudden rise in duplicates is a real signal about an upstream producer.
 */
export class DuplicateEventError extends PermanentError {
  public override readonly code = 'DUPLICATE_EVENT' as const;
}

/**
 * An error we could not classify.
 *
 * Permanent on purpose — see the header note. If a specific `UnknownError`
 * shows up repeatedly in the DLQ, that is the signal to add a real subclass and
 * translate it at the adapter that produced it.
 */
export class UnknownError extends PermanentError {
  public override readonly code = 'UNKNOWN' as const;
}

/* ── Classification ──────────────────────────────────────────────────────── */

/**
 * The single question the retry engine asks.
 *
 * Anything that is not an explicitly transient `PlatformError` is treated as
 * permanent — including raw `Error`s that escaped an adapter, because an
 * unclassified error is an unknown error.
 */
export function isRetryable(error: unknown): boolean {
  return error instanceof PlatformError && error.retryable;
}

/** Extract a stable error code for metrics and alerting. */
export function errorCodeOf(error: unknown): ErrorCode {
  return error instanceof PlatformError ? error.code : 'UNKNOWN';
}

/**
 * Coerce anything thrown into a `PlatformError`.
 *
 * JavaScript permits throwing non-Errors (`throw 'oops'`, or a rejected promise
 * carrying a string), so the catch-all path must handle values that have no
 * `.message` and no stack.
 */
export function toPlatformError(error: unknown, context: ErrorContext = {}): PlatformError {
  if (error instanceof PlatformError) return error;
  if (error instanceof Error) return new UnknownError(error.message, context, error);
  return new UnknownError(typeof error === 'string' ? error : 'Non-Error value thrown', {
    ...context,
    thrownType: typeof error,
  });
}

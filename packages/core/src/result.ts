/**
 * Result — expected failures as values, not exceptions.
 *
 * ── Why this exists ────────────────────────────────────────────────────────
 * In a consumer, the difference between "this message failed transiently" and
 * "this message is malformed" determines whether it goes to a retry tier or
 * straight to the DLQ (ADR-0005). That decision is the single most important
 * branch in the platform, and exceptions hide it: a function's signature says
 * nothing about what it can throw, so the caller cannot be forced to handle it.
 *
 *   async function publish(e: Event): Promise<Offset>       // throws… what?
 *   async function publish(e: Event): Promise<PublishResult> // exhaustive
 *
 * The second signature makes the failure part of the contract. TypeScript's
 * exhaustiveness checking then makes forgetting a case a compile error.
 *
 * ── Rejected alternatives ──────────────────────────────────────────────────
 * • Exceptions everywhere. Zero type safety on the failure path, and stack
 *   unwinding through async boundaries loses context. We still use exceptions —
 *   but only for genuinely exceptional conditions (see errors.ts).
 * • fp-ts / Effect. Excellent libraries, but they import an entire functional
 *   idiom (pipe, do-notation, fibers) that every contributor must learn. For a
 *   platform whose readers are backend engineers, the cost outweighs the gain.
 *   This file is ~120 lines with no dependencies and no new vocabulary.
 * • Go-style tuples `[value, error]`. No discriminated union means no
 *   exhaustiveness checking and no narrowing — the compiler cannot stop you
 *   reading `value` when `error` is set.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * Callers must unwrap. That is deliberate friction: it puts the failure path
 * in front of the author instead of letting it default to "propagate and hope".
 * Where friction is not worth it — a programming bug, a config error at
 * startup — we throw instead. See the guidance at the bottom of this file.
 *
 * ── Where this stops scaling ───────────────────────────────────────────────
 * Deeply nested Results become unreadable (`Result<Result<T, E1>, E2>`). If a
 * call chain needs more than two levels of unwrapping, the seam is in the wrong
 * place — extract the inner chain into a function that returns a flat Result.
 */

/** A successful outcome carrying a value. */
export interface Ok<T> {
  readonly ok: true;
  readonly value: T;
}

/** A failed outcome carrying a typed error. */
export interface Err<E> {
  readonly ok: false;
  readonly error: E;
}

/**
 * Either a success or a failure, never both.
 *
 * `ok` is the discriminant, so `if (result.ok)` narrows the type — the compiler
 * will not let you read `.error` inside the success branch.
 */
export type Result<T, E> = Ok<T> | Err<E>;

/** Construct a success. */
export function ok<T>(value: T): Ok<T> {
  return { ok: true, value };
}

/** Construct a failure. */
export function err<E>(error: E): Err<E> {
  return { ok: false, error };
}

/** Type guard — narrows to the success branch. */
export function isOk<T, E>(result: Result<T, E>): result is Ok<T> {
  return result.ok;
}

/** Type guard — narrows to the failure branch. */
export function isErr<T, E>(result: Result<T, E>): result is Err<E> {
  return !result.ok;
}

/**
 * Transform the success value, leaving a failure untouched.
 * The error type is preserved, so this cannot accidentally swallow a failure.
 */
export function map<T, U, E>(result: Result<T, E>, fn: (value: T) => U): Result<U, E> {
  return result.ok ? ok(fn(result.value)) : result;
}

/**
 * Transform the error, leaving a success untouched.
 *
 * The main use is translating a vendor error into a domain error at an adapter
 * boundary — the third of the three translations an Adapter owes its caller
 * (request, response, and failure mode).
 */
export function mapErr<T, E, F>(result: Result<T, E>, fn: (error: E) => F): Result<T, F> {
  return result.ok ? result : err(fn(result.error));
}

/**
 * Chain an operation that can itself fail — the flatMap of Result.
 *
 * Without this, sequencing two fallible calls produces `Result<Result<T, E>, E>`.
 */
export function andThen<T, U, E>(
  result: Result<T, E>,
  fn: (value: T) => Result<U, E>,
): Result<U, E> {
  return result.ok ? fn(result.value) : result;
}

/** Extract the value, substituting a default on failure. */
export function unwrapOr<T, E>(result: Result<T, E>, fallback: T): T {
  return result.ok ? result.value : fallback;
}

/** Extract the value, computing a default from the error on failure. */
export function unwrapOrElse<T, E>(result: Result<T, E>, fn: (error: E) => T): T {
  return result.ok ? result.value : fn(result.error);
}

/**
 * Extract the value or throw.
 *
 * Deliberately named to be conspicuous in a diff. Legitimate at a system
 * boundary where there is genuinely no recovery — application startup, or a
 * test asserting the happy path. Anywhere else it defeats the purpose of the
 * type, and a reviewer should ask why.
 */
export function unwrapOrThrow<T, E>(result: Result<T, E>): T {
  if (result.ok) return result.value;
  throw result.error instanceof Error
    ? result.error
    : new Error(`Called unwrapOrThrow on an Err: ${JSON.stringify(result.error)}`);
}

/**
 * Collect an array of Results into a Result of an array, failing on the first error.
 *
 * Used by bulk publish: if any event in a batch fails validation, the whole
 * batch is rejected rather than partially accepted, because a caller cannot act
 * on "some of your events were stored" without knowing which.
 *
 * Note this is fail-fast. Where every failure matters (validating a request
 * body, where the user should see all problems at once), collect them instead
 * of stopping at the first.
 */
export function all<T, E>(results: readonly Result<T, E>[]): Result<T[], E> {
  const values: T[] = [];
  for (const result of results) {
    if (!result.ok) return result;
    values.push(result.value);
  }
  return ok(values);
}

/**
 * Partition results into successes and failures without discarding either.
 *
 * This is the batch counterpart to `all`. Bulk publish uses it to return HTTP
 * 207 Multi-Status: the caller learns exactly which events were accepted and
 * which were rejected, which is the only actionable answer for a partial batch.
 */
export function partition<T, E>(
  results: readonly Result<T, E>[],
): {
  values: T[];
  errors: E[];
} {
  const values: T[] = [];
  const errors: E[] = [];
  for (const result of results) {
    if (result.ok) values.push(result.value);
    else errors.push(result.error);
  }
  return { values, errors };
}

/**
 * Run a throwing function and capture the throw as a Result.
 *
 * The boundary adapter for third-party code we do not control: JSON.parse,
 * a vendor SDK, anything that signals failure by throwing.
 */
export function attempt<T>(fn: () => T): Result<T, unknown> {
  try {
    return ok(fn());
  } catch (error: unknown) {
    return err(error);
  }
}

/** Async form of {@link attempt}. */
export async function attemptAsync<T>(fn: () => Promise<T>): Promise<Result<T, unknown>> {
  try {
    return ok(await fn());
  } catch (error: unknown) {
    return err(error);
  }
}

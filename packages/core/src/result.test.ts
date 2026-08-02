import { describe, expect, expectTypeOf, it } from 'vitest';
import {
  all,
  andThen,
  attempt,
  attemptAsync,
  err,
  isErr,
  isOk,
  map,
  mapErr,
  ok,
  partition,
  unwrapOr,
  unwrapOrElse,
  unwrapOrThrow,
  type Result,
} from './result.js';

describe('construction and narrowing', () => {
  it('narrows on the ok discriminant', () => {
    // The whole point of the discriminated union: the compiler refuses to let
    // you read .value on a failure. If this stops holding, Result is just a
    // tuple with extra steps.
    const result: Result<number, string> = ok(42);
    if (result.ok) {
      expectTypeOf(result.value).toEqualTypeOf<number>();
      expect(result.value).toBe(42);
    } else {
      expect.unreachable();
    }
  });

  it('provides type guards for use outside if-statements', () => {
    expect(isOk(ok(1))).toBe(true);
    expect(isErr(err('boom'))).toBe(true);
  });
});

describe('transformation', () => {
  it('maps a success and leaves a failure untouched', () => {
    expect(map(ok(2), (n) => n * 2)).toEqual(ok(4));
    expect(map(err<string>('boom'), (n: number) => n * 2)).toEqual(err('boom'));
  });

  it('maps an error without touching a success', () => {
    // The adapter-boundary use: translate a vendor error into a domain error.
    const translated = mapErr(err({ vendorCode: 'E_CONN' }), (e) => `translated:${e.vendorCode}`);
    expect(translated).toEqual(err('translated:E_CONN'));
    expect(mapErr(ok(1), () => 'never')).toEqual(ok(1));
  });

  it('chains fallible operations without nesting', () => {
    const parse = (s: string): Result<number, string> => {
      const n = Number(s);
      return Number.isNaN(n) ? err(`not a number: ${s}`) : ok(n);
    };
    const positive = (n: number): Result<number, string> =>
      n > 0 ? ok(n) : err(`not positive: ${n}`);

    expect(andThen(parse('5'), positive)).toEqual(ok(5));
    expect(andThen(parse('-5'), positive)).toEqual(err('not positive: -5'));
    // Short-circuits: `positive` is never reached.
    expect(andThen(parse('abc'), positive)).toEqual(err('not a number: abc'));
  });
});

describe('unwrapping', () => {
  it('substitutes a default on failure', () => {
    expect(unwrapOr(ok(1), 0)).toBe(1);
    expect(unwrapOr(err('boom'), 0)).toBe(0);
  });

  it('computes a default from the error', () => {
    expect(unwrapOrElse(err('boom'), (e) => e.length)).toBe(4);
  });

  it('throws the error itself when it is an Error, preserving the stack', () => {
    const cause = new Error('original');
    expect(() => unwrapOrThrow(err(cause))).toThrow(cause);
  });

  it('wraps a non-Error before throwing', () => {
    expect(() => unwrapOrThrow(err({ code: 'X' }))).toThrow(/unwrapOrThrow/);
  });
});

describe('collections', () => {
  it('collects all successes, failing fast on the first error', () => {
    // Bulk publish semantics: a partially-accepted batch is not actionable for
    // the caller, so validation failures reject the whole batch.
    expect(all([ok(1), ok(2), ok(3)])).toEqual(ok([1, 2, 3]));
    expect(all([ok(1), err('bad'), err('worse')])).toEqual(err('bad'));
  });

  it('partitions without discarding either side', () => {
    // The 207 Multi-Status case: the caller needs to know exactly which events
    // were accepted and which were not.
    const { values, errors } = partition([ok(1), err('a'), ok(2), err('b')]);
    expect(values).toEqual([1, 2]);
    expect(errors).toEqual(['a', 'b']);
  });

  it('treats an empty collection as success', () => {
    expect(all([])).toEqual(ok([]));
  });
});

describe('interop with throwing code', () => {
  it('captures a throw as a failure', () => {
    // The boundary adapter for third-party code: JSON.parse, vendor SDKs,
    // anything that signals failure by throwing.
    const parsed = attempt(() => JSON.parse('{ not json') as unknown);
    expect(parsed.ok).toBe(false);
  });

  it('captures a rejection as a failure', async () => {
    const result = await attemptAsync(async () => {
      await Promise.resolve();
      throw new Error('async boom');
    });
    expect(result.ok).toBe(false);
    if (!result.ok) expect((result.error as Error).message).toBe('async boom');
  });

  it('passes a success through', async () => {
    // Plain arrow, not async: attemptAsync takes `() => Promise<T>`, and a
    // function that returns a promise already satisfies that. Wrapping it in
    // `async` would add a redundant microtask and trip return-await, which is
    // enabled precisely to catch that shape.
    expect(await attemptAsync(() => Promise.resolve(7))).toEqual(ok(7));
  });
});

import { describe, expect, it } from 'vitest';
import { configSchema, loadConfig, postgresUrl, redactedConfig } from './config.js';
import { InvalidConfigurationError } from './errors.js';

/** Minimum viable environment — every variable without a safe default. */
const REQUIRED = {
  KAFKA_BROKERS: 'localhost:19092',
  POSTGRES_HOST: 'localhost',
  POSTGRES_USER: 'platform',
  POSTGRES_PASSWORD: 'secret',
  POSTGRES_DB: 'event_platform',
  REDIS_HOST: 'localhost',
} as const;

describe('validation', () => {
  it('parses a valid environment', () => {
    const config = loadConfig({ ...REQUIRED });
    expect(config.KAFKA_BROKERS).toEqual(['localhost:19092']);
    expect(config.POSTGRES_PORT).toBe(5432); // default applied
    expect(config.NODE_ENV).toBe('development');
  });

  it('refuses to start when a required variable is missing', () => {
    // The alternative is a process that starts, passes its health check, and
    // connects to nothing — discovered hours later as a connection timeout.
    const { KAFKA_BROKERS: _omitted, ...incomplete } = REQUIRED;
    expect(() => loadConfig(incomplete)).toThrow(InvalidConfigurationError);
  });

  it('reports every problem at once', () => {
    // Fixing configuration one CrashLoopBackOff at a time is a miserable way
    // to spend a deploy window.
    let message = '';
    try {
      loadConfig({ POSTGRES_HOST: 'localhost' });
    } catch (error) {
      message = (error as Error).message;
    }
    expect(message).toContain('KAFKA_BROKERS');
    expect(message).toContain('POSTGRES_USER');
    expect(message).toContain('POSTGRES_DB');
  });

  it('points the reader at the contract', () => {
    expect(() => loadConfig({})).toThrow(/\.env\.example/);
  });
});

describe('numeric parsing', () => {
  it('rejects a non-numeric value instead of yielding NaN', () => {
    // This is the specific failure the hand-written transform exists to stop.
    // z.coerce.number() would produce NaN, and every NaN comparison is false —
    // so a misconfigured timeout silently becomes "never time out".
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_PORT: 'not-a-number' })).toThrow(
      InvalidConfigurationError,
    );
  });

  it('rejects an empty string instead of yielding 0', () => {
    // z.coerce.number('') is 0. A pool size of 0 or a port of 0 are both
    // catastrophic and both look like valid numbers downstream.
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_POOL_MAX: '' })).toThrow(
      InvalidConfigurationError,
    );
  });

  it('rejects out-of-range ports', () => {
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_PORT: '70000' })).toThrow();
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_PORT: '0' })).toThrow();
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_PORT: '-1' })).toThrow();
  });

  it('rejects a fractional integer', () => {
    expect(() => loadConfig({ ...REQUIRED, POSTGRES_POOL_MIN: '2.5' })).toThrow();
  });
});

describe('list parsing', () => {
  it('splits and trims a broker list', () => {
    const config = loadConfig({ ...REQUIRED, KAFKA_BROKERS: ' a:1 , b:2,c:3 ' });
    expect(config.KAFKA_BROKERS).toEqual(['a:1', 'b:2', 'c:3']);
  });

  it('rejects a list that parses to nothing', () => {
    // ',,,' would otherwise produce an empty broker array, and the client would
    // fail with an opaque error far from the cause.
    expect(() => loadConfig({ ...REQUIRED, KAFKA_BROKERS: ',,,' })).toThrow();
    expect(() => loadConfig({ ...REQUIRED, KAFKA_BROKERS: '   ' })).toThrow();
  });
});

describe('enum validation', () => {
  it('rejects an unknown NODE_ENV', () => {
    // 'prod' vs 'production' silently changes behaviour in every framework
    // that branches on it.
    expect(() => loadConfig({ ...REQUIRED, NODE_ENV: 'prod' })).toThrow();
  });

  it('rejects an unknown log level', () => {
    expect(() => loadConfig({ ...REQUIRED, LOG_LEVEL: 'verbose' })).toThrow();
  });
});

describe('immutability', () => {
  it('freezes the result', () => {
    // A later "just override it for this one call" becomes a TypeError rather
    // than a mutation that makes behaviour depend on execution order.
    const config = loadConfig({ ...REQUIRED });
    expect(Object.isFrozen(config)).toBe(true);
    expect(() => {
      (config as { NODE_ENV: string }).NODE_ENV = 'production';
    }).toThrow(TypeError);
  });
});

describe('derived values', () => {
  it('builds a postgres url', () => {
    const config = loadConfig({ ...REQUIRED });
    expect(postgresUrl(config)).toBe('postgresql://platform:secret@localhost:5432/event_platform');
  });

  it('percent-encodes a password with url-unsafe characters', () => {
    // An unencoded '@' or ':' in a password silently corrupts the connection
    // string and produces an authentication error that looks like wrong
    // credentials rather than a parsing bug.
    const config = loadConfig({ ...REQUIRED, POSTGRES_PASSWORD: 'p@ss:w/rd' });
    expect(postgresUrl(config)).toContain('p%40ss%3Aw%2Frd');
  });
});

describe('redaction', () => {
  it('redacts secrets for logging', () => {
    // Startup logs the effective config, which is useful — and is also how
    // credentials reach a log aggregator with a much wider audience.
    const redacted = redactedConfig(loadConfig({ ...REQUIRED }));
    expect(redacted['POSTGRES_PASSWORD']).toBe('[REDACTED]');
    expect(redacted['POSTGRES_USER']).toBe('platform');
    expect(redacted['KAFKA_BROKERS']).toEqual(['localhost:19092']);
  });

  it('leaves an absent optional secret alone', () => {
    const redacted = redactedConfig(loadConfig({ ...REQUIRED }));
    expect(redacted['REDIS_PASSWORD']).toBeUndefined();
  });
});

describe('schema as documentation', () => {
  it('requires everything whose wrong value is silent and costly', () => {
    // Defaults exist only where a wrong value is harmless. Anything that could
    // point at the wrong cluster or database must be stated explicitly.
    const shape = configSchema.shape;
    for (const key of ['KAFKA_BROKERS', 'POSTGRES_HOST', 'POSTGRES_PASSWORD', 'REDIS_HOST']) {
      expect(shape[key as keyof typeof shape].isOptional(), `${key} must be required`).toBe(false);
    }
  });
});

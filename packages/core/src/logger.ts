/**
 * Structured logging with automatic context injection.
 *
 * ── Structured, not formatted ──────────────────────────────────────────────
 * `log.info(\`processed order ${id} in ${ms}ms\`)` produces a string a human can
 * read and a machine cannot query. You cannot ask "p99 latency for tenant acme"
 * of a string without a regex that breaks the moment the message is reworded.
 *
 * JSON with typed fields makes every log line queryable, and the message becomes
 * a stable label rather than a data carrier.
 *
 * ── Why pino ───────────────────────────────────────────────────────────────
 * Logging sits on the hot path of every request and every consumed message. At
 * 10,000 events/sec, per-line cost is real. pino serialises directly to a
 * buffer, avoids intermediate object allocation, and can move formatting to a
 * worker thread. Winston is more flexible and measurably slower; the flexibility
 * is not needed here.
 *
 * ── Context injection is the point ─────────────────────────────────────────
 * Every line automatically carries the correlation ID, tenant, topic and offset
 * from AsyncLocalStorage (context.ts). Without it, correlation depends on every
 * author remembering to pass it — and the lines where it is forgotten are, by
 * Murphy, the ones you need during an incident.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * A context lookup per log call. Negligible next to serialisation, and it only
 * happens after the level check — a disabled `debug` call does no work at all.
 */

import pino, { type Logger as PinoLogger } from 'pino';
import { type Config } from './config.js';
import { contextFields } from './context.js';
import { PlatformError } from './errors.js';

export type LogLevel = 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal';

/** Fields must be structured — no interpolated strings carrying data. */
export type LogFields = Record<string, unknown>;

export interface Logger {
  trace(message: string, fields?: LogFields): void;
  debug(message: string, fields?: LogFields): void;
  info(message: string, fields?: LogFields): void;
  warn(message: string, fields?: LogFields): void;
  /** `error` is a first-class parameter, not a field — it needs special serialisation. */
  error(message: string, error?: unknown, fields?: LogFields): void;
  fatal(message: string, error?: unknown, fields?: LogFields): void;
  /** Child logger with permanently-bound fields. For per-component context. */
  child(bindings: LogFields): Logger;
  isLevelEnabled(level: LogLevel): boolean;
}

/**
 * Serialise an error, preserving the cause chain.
 *
 * A flattened `error.toString()` loses the stack and the cause — precisely the
 * two things needed to find where a vendor SDK actually failed. PlatformError
 * already knows how to serialise itself, including its retryability, which is
 * what makes "how many permanent failures today" a queryable question.
 */
function serialiseError(error: unknown): Record<string, unknown> | undefined {
  if (error === undefined || error === null) return undefined;
  if (error instanceof PlatformError) return error.toJSON();
  if (error instanceof Error) {
    return {
      name: error.name,
      message: error.message,
      stack: error.stack,
      ...(error.cause ? { cause: serialiseError(error.cause) } : {}),
    };
  }
  return { thrown: describeUnknown(error), thrownType: typeof error };
}

/**
 * Render a non-Error thrown value as something a human can act on.
 *
 * `String(someObject)` produces `[object Object]`, which in a log line is worse
 * than useless — it looks like information and carries none. JavaScript permits
 * `throw { code: 'E_FAIL' }`, and a rejected promise can carry anything, so this
 * path is reachable in production even though it should not be.
 *
 * JSON.stringify is guarded because a circular structure throws, and a logger
 * that throws while reporting an error turns a handled failure into a crash.
 */
function describeUnknown(value: unknown): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
    case 'boolean':
    case 'bigint':
      return String(value);
    case 'symbol':
      return value.toString();
    case 'function':
      return `[Function ${value.name || 'anonymous'}]`;
    default:
      try {
        return JSON.stringify(value) ?? '[unserialisable]';
      } catch {
        // Circular reference, or a toJSON that threw.
        return '[circular or unserialisable]';
      }
  }
}

class PinoAdapter implements Logger {
  constructor(private readonly pino: PinoLogger) {}

  public trace(message: string, fields?: LogFields): void {
    this.write('trace', message, fields);
  }
  public debug(message: string, fields?: LogFields): void {
    this.write('debug', message, fields);
  }
  public info(message: string, fields?: LogFields): void {
    this.write('info', message, fields);
  }
  public warn(message: string, fields?: LogFields): void {
    this.write('warn', message, fields);
  }

  public error(message: string, error?: unknown, fields?: LogFields): void {
    this.write('error', message, fields, error);
  }
  public fatal(message: string, error?: unknown, fields?: LogFields): void {
    this.write('fatal', message, fields, error);
  }

  public child(bindings: LogFields): Logger {
    return new PinoAdapter(this.pino.child(bindings));
  }

  public isLevelEnabled(level: LogLevel): boolean {
    return this.pino.isLevelEnabled(level);
  }

  private write(level: LogLevel, message: string, fields?: LogFields, error?: unknown): void {
    // Early exit before touching AsyncLocalStorage or serialising an error.
    // A disabled debug call must cost approximately nothing, or debug logging
    // becomes something people strip out for performance.
    if (!this.pino.isLevelEnabled(level)) return;

    const serialised = serialiseError(error);
    this.pino[level](
      {
        ...contextFields(),
        ...fields,
        ...(serialised ? { err: serialised } : {}),
      },
      message,
    );
  }
}

export interface LoggerOptions {
  readonly level?: LogLevel;
  readonly serviceName?: string;
  /** Human-readable output. Development only — pretty-printing is slow. */
  readonly pretty?: boolean;
  /** Extra keys to redact, beyond the defaults. */
  readonly redact?: string[];
}

/**
 * Paths redacted on every line.
 *
 * Denylisting is imperfect — a field named something unexpected still leaks —
 * but it catches the common shapes, and the alternative (allowlisting every
 * loggable field) makes the logger unusable. The real defence is not putting
 * secrets in log fields; this is the safety net.
 *
 * `authorization` and `cookie` appear under several casings because they arrive
 * from HTTP headers, which are case-insensitive on the wire but not in an object.
 */
const DEFAULT_REDACT = [
  'password',
  '*.password',
  'authorization',
  '*.authorization',
  'req.headers.authorization',
  'req.headers.cookie',
  'apiKey',
  '*.apiKey',
  'token',
  '*.token',
  'secret',
  '*.secret',
];

export function createLogger(options: LoggerOptions = {}): Logger {
  const instance = pino({
    level: options.level ?? 'info',
    base: {
      service: options.serviceName ?? 'event-platform',
      // Included so a log line identifies which pod produced it — essential when
      // one replica of twenty is misbehaving.
      pid: process.pid,
    },
    // ISO 8601 rather than epoch millis: a human reading raw logs during an
    // incident should not have to convert timestamps mentally.
    timestamp: pino.stdTimeFunctions.isoTime,
    redact: { paths: [...DEFAULT_REDACT, ...(options.redact ?? [])], censor: '[REDACTED]' },
    formatters: {
      // Default pino emits `level: 30`. Log backends and humans both want the
      // word, and the numeric form causes endless confusion in query filters.
      level: (label) => ({ level: label }),
    },
    ...(options.pretty
      ? {
          transport: {
            target: 'pino-pretty',
            options: { colorize: true, translateTime: 'HH:MM:ss.l', ignore: 'pid,service' },
          },
        }
      : {}),
  });

  return new PinoAdapter(instance);
}

/** Build a logger from validated configuration. */
export function loggerFromConfig(config: Config): Logger {
  return createLogger({
    level: config.LOG_LEVEL,
    serviceName: config.SERVICE_NAME,
    pretty: config.NODE_ENV === 'development',
  });
}

/**
 * Logger that records rather than writes, for tests.
 *
 * Lets a test assert that a failure was logged with the right error and fields —
 * which matters, because "the DLQ path logged the reason" is a behaviour worth
 * protecting, not an implementation detail.
 */
export class RecordingLogger implements Logger {
  public readonly entries: {
    level: LogLevel;
    message: string;
    fields?: LogFields;
    error?: unknown;
  }[] = [];

  public trace(message: string, fields?: LogFields): void {
    this.entries.push({ level: 'trace', message, ...(fields ? { fields } : {}) });
  }
  public debug(message: string, fields?: LogFields): void {
    this.entries.push({ level: 'debug', message, ...(fields ? { fields } : {}) });
  }
  public info(message: string, fields?: LogFields): void {
    this.entries.push({ level: 'info', message, ...(fields ? { fields } : {}) });
  }
  public warn(message: string, fields?: LogFields): void {
    this.entries.push({ level: 'warn', message, ...(fields ? { fields } : {}) });
  }
  public error(message: string, error?: unknown, fields?: LogFields): void {
    this.entries.push({ level: 'error', message, error, ...(fields ? { fields } : {}) });
  }
  public fatal(message: string, error?: unknown, fields?: LogFields): void {
    this.entries.push({ level: 'fatal', message, error, ...(fields ? { fields } : {}) });
  }
  public child(): Logger {
    return this;
  }
  public isLevelEnabled(): boolean {
    return true;
  }

  public find(
    level: LogLevel,
    messageSubstring: string,
  ): (typeof this.entries)[number] | undefined {
    return this.entries.find((e) => e.level === level && e.message.includes(messageSubstring));
  }
}

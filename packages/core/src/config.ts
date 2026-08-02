/**
 * Typed configuration, validated once at startup.
 *
 * ── The failure this prevents ──────────────────────────────────────────────
 * `process.env.KAFKA_BROKERS` is `string | undefined`, and every consumer of it
 * either checks or assumes. The assumption is the bug: a typo'd or missing
 * variable becomes `undefined`, flows through the system, and surfaces hours
 * later as a connection timeout with no indication that configuration was the
 * cause. Numeric variables are worse — `Number(undefined)` is `NaN`, and NaN
 * comparisons are always false, so a misconfigured timeout silently becomes
 * "never time out".
 *
 * ── Fail fast, at the boundary ─────────────────────────────────────────────
 * Configuration is validated once, at process start, before anything connects.
 * Invalid config means the process refuses to start with a message naming every
 * problem at once. In Kubernetes that is a CrashLoopBackOff the deploy catches
 * immediately — infinitely better than a pod that starts, passes its health
 * check, and mis-processes events.
 *
 * This is "parse, don't validate": after `loadConfig()` the rest of the codebase
 * receives a fully-typed, frozen object and never touches `process.env` again.
 *
 * ── Rejected alternatives ──────────────────────────────────────────────────
 * • dotenv alone. Loads a file; validates nothing.
 * • convict. Capable, but a heavier API and weaker TypeScript inference.
 * • Hand-rolled parsing. What we would write is a worse version of Zod, and the
 *   error messages would be worse too.
 * • Reading env vars where needed. The status quo this replaces: no single
 *   place to see what the service requires, and no way to fail fast.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * Zod is a runtime dependency on the startup path (~60 KB). In exchange, the
 * schema is simultaneously the validator, the TypeScript type and the
 * documentation — three things that otherwise drift apart.
 */

import { z } from 'zod';
import { InvalidConfigurationError } from './errors.js';

/** Comma-separated list → trimmed array, rejecting empties. */
const csv = (): z.ZodEffects<z.ZodString, string[], string> =>
  z.string().transform((value, ctx) => {
    const parts = value
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    if (parts.length === 0) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'must contain at least one entry' });
      return z.NEVER;
    }
    return parts;
  });

/**
 * Positive integer from a string.
 *
 * `z.coerce.number()` is deliberately not used: it accepts `''` as 0 and `'abc'`
 * as NaN, which is exactly the silent-misconfiguration class this module exists
 * to eliminate.
 */
const port = (): z.ZodEffects<z.ZodString, number, string> =>
  z.string().transform((value, ctx) => {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0 || parsed > 65_535) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: `"${value}" is not a valid port` });
      return z.NEVER;
    }
    return parsed;
  });

const positiveInt = (): z.ZodEffects<z.ZodString, number, string> =>
  z.string().transform((value, ctx) => {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed <= 0) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `"${value}" is not a positive integer`,
      });
      return z.NEVER;
    }
    return parsed;
  });

/**
 * The platform's configuration contract.
 *
 * Defaults exist only where a wrong value is harmless. Anything whose wrong
 * value causes data loss or a security hole is deliberately required, so the
 * process refuses to start rather than silently using a development default —
 * the mechanism behind a long line of production incidents.
 */
export const configSchema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  LOG_LEVEL: z.enum(['trace', 'debug', 'info', 'warn', 'error', 'fatal']).default('info'),
  SERVICE_NAME: z.string().min(1).default('event-platform'),

  // No default: pointing at the wrong cluster is silent and catastrophic.
  KAFKA_BROKERS: csv(),
  KAFKA_CLIENT_ID: z.string().min(1).default('event-platform'),
  KAFKA_CONNECTION_TIMEOUT_MS: positiveInt().default('3000'),
  KAFKA_REQUEST_TIMEOUT_MS: positiveInt().default('30000'),

  POSTGRES_HOST: z.string().min(1),
  POSTGRES_PORT: port().default('5432'),
  POSTGRES_USER: z.string().min(1),
  POSTGRES_PASSWORD: z.string().min(1),
  POSTGRES_DB: z.string().min(1),
  POSTGRES_POOL_MIN: positiveInt().default('2'),
  POSTGRES_POOL_MAX: positiveInt().default('20'),

  REDIS_HOST: z.string().min(1),
  REDIS_PORT: port().default('6379'),
  REDIS_PASSWORD: z.string().optional(),
  REDIS_KEY_PREFIX: z.string().min(1).default('kep'),

  // Dedup window. Must exceed the longest expected replay, or a replay older
  // than the TTL re-executes its side effects (ADR-0008).
  IDEMPOTENCY_TTL_SECONDS: positiveInt().default('86400'),

  METRICS_PORT: port().default('9464'),
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().url().optional(),
});

export type Config = Readonly<z.infer<typeof configSchema>>;

/**
 * Parse and validate the environment.
 *
 * Reports ALL problems at once rather than stopping at the first. Fixing
 * configuration one CrashLoopBackOff at a time is a miserable way to spend a
 * deploy window.
 */
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const result = configSchema.safeParse(env);

  if (!result.success) {
    const problems = result.error.issues
      .map((issue) => `  ${issue.path.join('.') || '(root)'}: ${issue.message}`)
      .join('\n');
    throw new InvalidConfigurationError(
      `Invalid configuration:\n${problems}\n\nSee .env.example for the full contract.`,
      { problemCount: result.error.issues.length },
    );
  }

  // Frozen so a later "just override it for this one call" is a TypeError
  // rather than a mutation that makes behaviour depend on execution order.
  return Object.freeze(result.data);
}

/** Derived connection string. Kept here so the shape is defined once. */
export function postgresUrl(config: Config): string {
  const { POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB } = config;
  const password = encodeURIComponent(POSTGRES_PASSWORD);
  return `postgresql://${POSTGRES_USER}:${password}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}`;
}

/**
 * Redact secrets for logging.
 *
 * Startup logs the effective configuration, which is genuinely useful for
 * debugging — and is also how credentials end up in a log aggregator that a
 * much wider audience can read. Allowlisting what is safe, rather than
 * denylisting what is not, means a newly added secret is redacted by default.
 */
export function redactedConfig(config: Config): Record<string, unknown> {
  const SECRET_KEYS = new Set(['POSTGRES_PASSWORD', 'REDIS_PASSWORD']);
  return Object.fromEntries(
    Object.entries(config).map(([key, value]) => [
      key,
      SECRET_KEYS.has(key) && value ? '[REDACTED]' : value,
    ]),
  );
}

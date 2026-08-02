/**
 * @platform/core — cross-cutting primitives.
 *
 * Explicit re-exports rather than `export *`: a barrel that re-exports
 * everything makes the package's public surface invisible, so anything added to
 * any file silently becomes API that consumers can depend on and we can no
 * longer change. Listing exports means widening the surface is a deliberate,
 * reviewable act.
 */

export {
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
  type Err,
  type Ok,
  type Result,
} from './result.js';

export {
  BrokerUnavailableError,
  DependencyUnavailableError,
  DuplicateEventError,
  ForbiddenError,
  InvalidConfigurationError,
  LockContentionError,
  MalformedPayloadError,
  MessageTooLargeError,
  NotEnoughReplicasError,
  PermanentError,
  PlatformError,
  RateLimitedError,
  SchemaIncompatibleError,
  SchemaNotFoundError,
  SchemaValidationError,
  TimeoutError,
  TopicNotFoundError,
  TransientError,
  UnauthorizedError,
  UnknownError,
  errorCodeOf,
  isRetryable,
  toPlatformError,
  type ErrorCode,
  type ErrorContext,
} from './errors.js';

export {
  CircularDependencyError,
  Container,
  UnregisteredDependencyError,
  token,
  type Factory,
  type Lifetime,
  type Token,
} from './container.js';

export { FixedClock, SleepAbortedError, SystemClock, type Clock } from './clock.js';

export {
  CORRELATION_HEADER,
  contextFields,
  correlationId,
  createContext,
  currentContext,
  newCorrelationId,
  runWithContext,
  withContext,
  type RequestContext,
} from './context.js';

export { configSchema, loadConfig, postgresUrl, redactedConfig, type Config } from './config.js';

export {
  RecordingLogger,
  createLogger,
  loggerFromConfig,
  type LogFields,
  type LogLevel,
  type Logger,
  type LoggerOptions,
} from './logger.js';

export {
  Lifecycle,
  type LifecycleOptions,
  type LifecycleState,
  type ShutdownHook,
  type StartupHook,
} from './lifecycle.js';

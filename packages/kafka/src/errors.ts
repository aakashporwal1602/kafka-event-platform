/**
 * Kafka error translation.
 *
 * ── Why this file is where the retry decision is made ──────────────────────
 * KafkaJS throws `KafkaJSProtocolError` carrying a numeric broker error code
 * and a `retriable` flag. Letting those escape means every caller imports
 * KafkaJS and branches on `error.type === 'NOT_ENOUGH_REPLICAS'`, which is the
 * vendor coupling ADR-0004's wrapper exists to prevent.
 *
 * More importantly, the retryable/permanent split is a **platform** decision,
 * not the driver's. KafkaJS marks `NOT_ENOUGH_REPLICAS` retriable and it is —
 * but `MESSAGE_TOO_LARGE` is also marked non-retriable, and treating those two
 * identically to "some error happened" is how a broker restart turns into a
 * DLQ full of perfectly valid events.
 *
 * ── The one that is genuinely hard ─────────────────────────────────────────
 * A **request timeout on produce is ambiguous**: the broker may have written
 * the batch and lost the response. Retrying can duplicate. Not retrying can
 * lose. Both are wrong in general — which is precisely why the producer runs
 * with `idempotent: true` (ADR-0013): the broker deduplicates the retry using
 * the producer id and sequence number, so retrying becomes the correct answer
 * rather than the lesser evil. Classifying `TimeoutError` as retryable here is
 * only safe **because** of that setting, and the two must change together.
 */

import {
  BrokerUnavailableError,
  DependencyUnavailableError,
  MessageTooLargeError,
  NotEnoughReplicasError,
  PlatformError,
  TimeoutError,
  TopicNotFoundError,
  UnauthorizedError,
  UnknownError,
} from '@platform/core';

/**
 * Broker error codes, from the Kafka protocol spec.
 *
 * Matched on the numeric code rather than KafkaJS's string `type`, because the
 * numbers are part of the wire protocol and stable across every client and
 * broker version, while the strings are a client-side naming choice.
 */
const BROKER = {
  UnknownTopicOrPartition: 3,
  LeaderNotAvailable: 5,
  NotLeaderForPartition: 6,
  RequestTimedOut: 7,
  BrokerNotAvailable: 8,
  ReplicaNotAvailable: 9,
  MessageTooLarge: 10,
  NetworkException: 13,
  CoordinatorLoadInProgress: 14,
  CoordinatorNotAvailable: 15,
  NotCoordinator: 16,
  NotEnoughReplicas: 19,
  NotEnoughReplicasAfterAppend: 20,
  RebalanceInProgress: 27,
  TopicAuthorizationFailed: 29,
  ClusterAuthorizationFailed: 31,
  UnsupportedForMessageFormat: 43,
  PolicyViolation: 44,
  OutOfOrderSequenceNumber: 45,
  DuplicateSequenceNumber: 46,
  InvalidProducerEpoch: 47,
  ThrottlingQuotaExceeded: 89,
} as const;

interface KafkaLikeError {
  readonly message: string;
  readonly code?: number | undefined;
  readonly type?: string | undefined;
  readonly retriable?: boolean | undefined;
}

/** Narrow an unknown throw into the shape KafkaJS actually produces. */
function asKafkaError(error: unknown): KafkaLikeError | undefined {
  if (typeof error !== 'object' || error === null) return undefined;
  const message = 'message' in error && typeof error.message === 'string' ? error.message : '';
  return {
    message,
    code: 'code' in error && typeof error.code === 'number' ? error.code : undefined,
    type: 'type' in error && typeof error.type === 'string' ? error.type : undefined,
    retriable:
      'retriable' in error && typeof error.retriable === 'boolean' ? error.retriable : undefined,
  };
}

export function translateKafkaError(error: unknown): PlatformError {
  if (error instanceof PlatformError) return error;

  const cause = error instanceof Error ? error : undefined;
  const kafka = asKafkaError(error);
  const message =
    kafka?.message !== undefined && kafka.message !== '' ? kafka.message : String(error);
  const context = {
    ...(kafka?.code !== undefined ? { brokerErrorCode: kafka.code } : {}),
    ...(kafka?.type !== undefined ? { kafkaType: kafka.type } : {}),
  };

  switch (kafka?.code) {
    // ── Durability: the write did not reach enough replicas ────────────────
    // Distinct from a generic broker failure because the *cause* is different
    // and so is the fix: a replica is down or lagging, and the topic's
    // min.insync.replicas=2 is correctly refusing to accept a write that
    // could be lost. Retrying is right; alerting on the rate is essential,
    // because a sustained rate means the cluster is one failure from
    // rejecting all writes.
    case BROKER.NotEnoughReplicas:
    case BROKER.NotEnoughReplicasAfterAppend:
      return new NotEnoughReplicasError(message, context, cause);

    // ── Leadership in motion — a rolling restart or a partition reassignment.
    // Always transient and usually resolved within a second or two, once the
    // client refreshes metadata.
    case BROKER.LeaderNotAvailable:
    case BROKER.NotLeaderForPartition:
    case BROKER.ReplicaNotAvailable:
    case BROKER.BrokerNotAvailable:
    case BROKER.NetworkException:
      return new BrokerUnavailableError(message, context, cause);

    // ── Group coordination — a consumer-side condition, mapped here so the
    // consumer runtime in Chapter 7 inherits it rather than re-deriving it.
    case BROKER.CoordinatorLoadInProgress:
    case BROKER.CoordinatorNotAvailable:
    case BROKER.NotCoordinator:
    case BROKER.RebalanceInProgress:
      return new DependencyUnavailableError(message, context, cause);

    // ── Ambiguous. See the header: safe to retry only because the producer
    // is idempotent, so the broker discards a duplicate of a write that did
    // in fact land.
    case BROKER.RequestTimedOut:
      return new TimeoutError(message, context, cause);

    case BROKER.ThrottlingQuotaExceeded:
      return new DependencyUnavailableError(message, { ...context, throttled: true }, cause);

    // ── Permanent: the message itself is the problem ───────────────────────
    // Retrying an oversized message produces the same rejection forever, and
    // an infinitely retried message blocks its partition. This must reach the
    // DLQ on the first attempt.
    case BROKER.MessageTooLarge:
    case BROKER.UnsupportedForMessageFormat:
    case BROKER.PolicyViolation:
      return new MessageTooLargeError(message, context, cause);

    // ── Topic missing. Permanent by design: auto.create.topics.enable is off
    // on the brokers (ADR-0001), so an unknown topic means the provisioning
    // step was skipped — a deployment bug, not a transient condition. Retrying
    // would hide it until someone noticed the backlog.
    case BROKER.UnknownTopicOrPartition:
      return new TopicNotFoundError(message, context, cause);

    case BROKER.TopicAuthorizationFailed:
    case BROKER.ClusterAuthorizationFailed:
      return new UnauthorizedError(message, context, cause);

    // ── Idempotent-producer state errors ───────────────────────────────────
    // OutOfOrderSequenceNumber means the broker's per-partition sequence has
    // a gap — the producer's state and the broker's have diverged, usually
    // because a batch expired locally after the broker had already accepted a
    // later one. It is NOT retryable at the message level: the producer must
    // be recreated to get a fresh producer id. Marking it retryable would
    // spin forever against a broker that will keep rejecting.
    case BROKER.OutOfOrderSequenceNumber:
    case BROKER.InvalidProducerEpoch:
      return new UnknownError(
        `${message} — the idempotent producer's sequence state diverged from the broker's. ` +
          `The producer must be recreated; retrying this message cannot recover it.`,
        { ...context, fatal: true },
        cause,
      );

    // The broker deduplicated our retry. This is the idempotent producer
    // working exactly as intended, so it is a success, not a failure — but it
    // reaches here only if something unwrapped it incorrectly, hence the note.
    case BROKER.DuplicateSequenceNumber:
      return new UnknownError(
        `${message} — the broker discarded a duplicate batch. This is the ` +
          `idempotent producer succeeding; it should not have surfaced as an error.`,
        { ...context, benign: true },
        cause,
      );

    default:
      break;
  }

  // Connection-level failures never reach a broker and therefore carry no
  // protocol code.
  if (/ECONNREFUSED|ENOTFOUND|EHOSTUNREACH|ECONNRESET|EPIPE/.test(message)) {
    return new BrokerUnavailableError(message, { ...context, transport: true }, cause);
  }
  if (/The producer is disconnected|Connection error|Closed connection/i.test(message)) {
    return new BrokerUnavailableError(message, { ...context, transport: true }, cause);
  }
  if (/timeout/i.test(message)) {
    return new TimeoutError(message, context, cause);
  }

  // Falling back to the driver's own flag rather than to "permanent". This is
  // the one place the default is inverted, and deliberately: an unrecognised
  // broker code that KafkaJS knows is retriable is far more likely to be a
  // transient cluster condition than a bad message, and the alternative sends
  // healthy events to the DLQ during an incident.
  if (kafka?.retriable === true) {
    return new BrokerUnavailableError(message, { ...context, driverRetriable: true }, cause);
  }

  return new UnknownError(message, context, cause);
}

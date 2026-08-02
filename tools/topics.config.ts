/**
 * Declarative topic configuration.
 *
 * Topics are infrastructure, and infrastructure belongs in version control.
 * `auto.create.topics.enable` is deliberately OFF on the brokers (see
 * docker-compose.yml) because auto-created topics get the broker defaults —
 * which is how a critical topic silently ends up with replication factor 1.
 *
 * Every topic here is reviewed in a pull request like any other change.
 */

export type CleanupPolicy = 'delete' | 'compact' | 'compact,delete';

export interface TopicDefinition {
  readonly name: string;
  readonly partitions: number;
  readonly replicationFactor: number;
  /** Why this topic exists — rendered into the provisioning output. */
  readonly description: string;
  readonly config: {
    readonly 'min.insync.replicas'?: string;
    readonly 'retention.ms'?: string;
    readonly 'cleanup.policy'?: CleanupPolicy;
    readonly 'max.message.bytes'?: string;
    readonly 'compression.type'?: string;
    readonly 'segment.ms'?: string;
    readonly [key: string]: string | undefined;
  };
}

/** Retention presets, named so call sites read as intent rather than arithmetic. */
export const RETENTION = {
  ONE_DAY: String(24 * 60 * 60 * 1000),
  THREE_DAYS: String(3 * 24 * 60 * 60 * 1000),
  SEVEN_DAYS: String(7 * 24 * 60 * 60 * 1000),
  THIRTY_DAYS: String(30 * 24 * 60 * 60 * 1000),
  NINETY_DAYS: String(90 * 24 * 60 * 60 * 1000),
} as const;

/** Retry tiers. Total window before DLQ ≈ 1h 5m 35s. See ADR-0005. */
export const RETRY_TIERS = [
  { suffix: '5s', delayMs: 5_000 },
  { suffix: '30s', delayMs: 30_000 },
  { suffix: '5m', delayMs: 300_000 },
  { suffix: '1h', delayMs: 3_600_000 },
] as const;

interface DomainOptions {
  readonly partitions?: number;
  readonly retentionMs?: string;
  readonly description: string;
}

/**
 * Expands one business domain into its full topic family:
 * the main topic, four retry tiers, and a dead-letter topic.
 *
 * Retry topics get FEWER partitions than the main topic on purpose — retry
 * volume is a small fraction of primary volume, and over-provisioning
 * partitions costs file handles, memory and rebalance time on every broker
 * for no throughput benefit.
 *
 * The DLQ gets LONGER retention: a message reaching the DLQ is one somebody
 * will need to investigate, and 7 days is not enough for something discovered
 * after a holiday weekend.
 */
export function eventDomain(domain: string, options: DomainOptions): TopicDefinition[] {
  const partitions = options.partitions ?? 12;
  const retryPartitions = Math.max(3, Math.floor(partitions / 4));
  const retentionMs = options.retentionMs ?? RETENTION.SEVEN_DAYS;

  const main: TopicDefinition = {
    name: `events.${domain}`,
    partitions,
    replicationFactor: 3,
    description: options.description,
    config: {
      'min.insync.replicas': '2',
      'retention.ms': retentionMs,
      'cleanup.policy': 'delete',
      'max.message.bytes': '2097152',
      'compression.type': 'producer',
    },
  };

  const retries: TopicDefinition[] = RETRY_TIERS.map((tier) => ({
    name: `retry.${domain}.${tier.suffix}`,
    partitions: retryPartitions,
    replicationFactor: 3,
    description: `Retry tier ${tier.suffix} for events.${domain}`,
    config: {
      'min.insync.replicas': '2',
      'retention.ms': RETENTION.THREE_DAYS,
      'cleanup.policy': 'delete',
    },
  }));

  const dlq: TopicDefinition = {
    name: `dlq.${domain}`,
    partitions: retryPartitions,
    replicationFactor: 3,
    description: `Dead letter queue for events.${domain} — retries exhausted or permanent failure`,
    config: {
      'min.insync.replicas': '2',
      'retention.ms': RETENTION.THIRTY_DAYS,
      'cleanup.policy': 'delete',
    },
  };

  return [main, ...retries, dlq];
}

/**
 * The complete desired state of the cluster.
 *
 * Adding a domain here and running `pnpm topics:bootstrap` is the only
 * supported way to create topics.
 */
export const TOPICS: TopicDefinition[] = [
  // --- Business domains -----------------------------------------------------
  ...eventDomain('orders', {
    partitions: 12,
    description: 'Order lifecycle events — created, paid, shipped, cancelled',
  }),
  ...eventDomain('payments', {
    partitions: 12,
    retentionMs: RETENTION.THIRTY_DAYS, // financial events retained longer for reconciliation
    description: 'Payment events — authorised, captured, refunded, failed',
  }),
  ...eventDomain('users', {
    partitions: 6,
    description: 'User lifecycle events — registered, updated, deactivated',
  }),
  ...eventDomain('notifications', {
    partitions: 6,
    retentionMs: RETENTION.THREE_DAYS, // transient; no value in long retention
    description: 'Outbound notification requests — email, SMS, push, webhook',
  }),

  // --- Platform internal topics ---------------------------------------------
  {
    name: 'platform.schemas',
    partitions: 1, // single partition: total ordering of schema registrations
    replicationFactor: 3,
    description: 'Schema registry change log — compacted, source of truth for schema state',
    config: {
      'min.insync.replicas': '2',
      'cleanup.policy': 'compact', // keep the latest value per schema key forever
      'retention.ms': '-1',
      'segment.ms': '3600000',
      'min.cleanable.dirty.ratio': '0.1',
    },
  },
  {
    name: 'platform.audit',
    partitions: 6,
    replicationFactor: 3,
    description: 'Immutable audit trail — every privileged platform operation',
    config: {
      'min.insync.replicas': '2',
      'retention.ms': RETENTION.NINETY_DAYS,
      'cleanup.policy': 'delete',
    },
  },
  {
    name: 'platform.replay',
    partitions: 3,
    replicationFactor: 3,
    description: 'Replay job commands and progress checkpoints',
    config: {
      'min.insync.replicas': '2',
      'retention.ms': RETENTION.SEVEN_DAYS,
      'cleanup.policy': 'delete',
    },
  },
  {
    name: 'platform.deadletter.global',
    partitions: 3,
    replicationFactor: 3,
    description: 'Catch-all DLQ for events that fail before their domain can be determined',
    config: {
      'min.insync.replicas': '2',
      'retention.ms': RETENTION.THIRTY_DAYS,
      'cleanup.policy': 'delete',
    },
  },
];

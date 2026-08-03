/**
 * Kafka connection factory.
 *
 * One `Kafka` instance per process, shared by every producer and consumer in
 * it. KafkaJS pools broker connections underneath, so a second instance means
 * a second full set of TCP connections and a second metadata refresh loop —
 * doubling connection count on every broker for no benefit.
 */

import { Kafka, logLevel as KafkaLogLevel, type KafkaConfig, type LogEntry } from 'kafkajs';
import type { Config, Logger } from '@platform/core';

export interface KafkaClientOptions {
  readonly config: Config;
  readonly logger: Logger;
  readonly overrides?: Partial<KafkaConfig> | undefined;
}

export function createKafka(options: KafkaClientOptions): Kafka {
  const { config, logger } = options;

  return new Kafka({
    // Appears in broker logs and in `kafka-consumer-groups` output. Including
    // the service name is what makes "which service is hammering this broker"
    // answerable from the broker side rather than only from ours.
    clientId: `${config.KAFKA_CLIENT_ID}-${config.SERVICE_NAME}`,
    brokers: [...config.KAFKA_BROKERS],

    connectionTimeout: config.KAFKA_CONNECTION_TIMEOUT_MS,
    requestTimeout: config.KAFKA_REQUEST_TIMEOUT_MS,

    // Connection-level retry, distinct from the per-request retry configured
    // on the producer. This one governs reconnection after a broker drops the
    // socket, so it is more patient: a rolling restart takes tens of seconds
    // and a client that gives up in five is a client that reports an outage
    // during routine maintenance.
    retry: {
      initialRetryTime: 300,
      retries: 10,
      maxRetryTime: 30_000,
      factor: 2,
    },

    // KafkaJS logs through its own mechanism by default, which produces
    // unstructured lines that miss the correlation id and never reach the log
    // aggregator's field index. Routing it into the platform logger is what
    // makes a broker warning searchable alongside the request that hit it.
    logLevel: KafkaLogLevel.INFO,
    logCreator: () => (entry: LogEntry) => {
      const { message, ...rest } = entry.log;
      const fields = { kafkaNamespace: entry.namespace, ...rest };

      switch (entry.level) {
        case KafkaLogLevel.ERROR:
        case KafkaLogLevel.NOTHING:
          logger.error(message, undefined, fields);
          return;
        case KafkaLogLevel.WARN:
          logger.warn(message, fields);
          return;
        case KafkaLogLevel.INFO:
          logger.info(message, fields);
          return;
        default:
          logger.debug(message, fields);
          return;
      }
    },

    ...options.overrides,
  });
}

/**
 * @platform/kafka — the only place in this repository that imports `kafkajs`.
 *
 * ADR-0004 chose KafkaJS partly on the grounds that the choice stays
 * reversible. That is only true while this boundary holds: the moment a
 * service imports `kafkajs` directly, swapping the client stops being a
 * one-package change and the ADR's central claim becomes false.
 *
 * A lint rule enforcing that is worth adding when the first service lands.
 */

export { createKafka, type KafkaClientOptions } from './client.js';

export { translateKafkaError } from './errors.js';

export { HEADER, envelopeToHeaders, headerString, type KafkaHeaders } from './headers.js';

export { KafkaProducer, type KafkaProducerOptions, type PublishResult } from './producer.js';

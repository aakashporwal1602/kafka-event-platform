# Architecture Decision Records

An ADR captures **one architecturally significant decision**: the context that forced it, the choice
made, the alternatives rejected, and the consequences accepted. ADRs are immutable once accepted — a
decision that changes gets a _new_ ADR that supersedes the old one, so the reasoning history stays
intact.

Format follows [Michael Nygard's template](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions).

## Index

| #                                           | Title                                                   | Status   | Chapter |
| ------------------------------------------- | ------------------------------------------------------- | -------- | ------- |
| [0001](./0001-kafka-kraft-mode.md)          | Run Kafka in KRaft mode without ZooKeeper               | Accepted | 0       |
| [0002](./0002-fastify-over-express.md)      | Use Fastify as the HTTP framework                       | Accepted | 0       |
| [0003](./0003-pnpm-monorepo.md)             | Single pnpm monorepo with TypeScript project references | Accepted | 0       |
| [0004](./0004-kafkajs-over-node-rdkafka.md) | Use KafkaJS as the Kafka client                         | Accepted | 0       |
| [0005](./0005-tiered-retry-topics.md)       | Non-blocking retries via tiered retry topics            | Accepted | 0       |
| [0006](./0006-avro-schema-registry.md)      | Avro with a schema registry for event contracts         | Accepted | 0       |
| [0007](./0007-transactional-outbox.md)      | Transactional outbox for atomic state-and-event writes  | Accepted | 0       |
| [0008](./0008-idempotency-strategy.md)      | At-least-once delivery with consumer-side idempotency   | Accepted | 0       |
| [0009](./0009-hand-rolled-di-container.md)  | Hand-rolled DI container instead of a framework         | Accepted | 2       |

## Statuses

- **Proposed** — under discussion, not yet binding
- **Accepted** — in force
- **Deprecated** — no longer recommended but still present in the codebase
- **Superseded by ADR-NNNN** — replaced

## Writing a new ADR

```bash
cp docs/adr/TEMPLATE.md docs/adr/00NN-short-title.md
```

Keep it to one page. If it takes longer, the decision is probably two decisions.

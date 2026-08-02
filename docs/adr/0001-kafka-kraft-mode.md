# ADR-0001: Run Kafka in KRaft mode without ZooKeeper

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Kafka historically required a ZooKeeper ensemble for cluster metadata: broker registration,
topic configuration, partition leadership and ACLs. This meant operating **two** distributed
consensus systems with different failure modes, different tuning knobs and different operational
runbooks.

ZooKeeper also imposed a hard ceiling on cluster size. Metadata changes propagated through
ZooKeeper watches, and controller failover required reloading the entire metadata set — an
operation that took minutes on clusters with hundreds of thousands of partitions.

KRaft (KIP-500) replaces ZooKeeper with an internal Raft quorum, storing metadata in a Kafka
log like any other topic. It became production-ready in Kafka 3.3, is the default in 3.5+, and
**ZooKeeper support was removed entirely in Kafka 4.0**.

## Decision

Run all Kafka clusters in **KRaft mode**. In local development, brokers run in combined mode
(broker + controller in one process). Production separates the roles onto dedicated controller nodes.

## Rationale

- ZooKeeper is removed in Kafka 4.0. Building on it in 2026 is building on a deprecated foundation.
- One system to operate, monitor and reason about instead of two.
- Controller failover drops from minutes to sub-second, because metadata is a replicated log with
  offsets rather than a tree that must be re-read.
- Partition-count ceiling rises by roughly an order of magnitude.
- Fewer moving parts in `docker-compose.yml` means the project actually starts on a reviewer's laptop —
  a real consideration for a portfolio repository.

## Alternatives considered

| Option | Why rejected |
|---|---|
| ZooKeeper-based Kafka | Removed in Kafka 4.0; two consensus systems to operate; slow controller failover |
| Redpanda | Excellent engineering and Kafka-API compatible, but the goal is to demonstrate *Kafka* expertise, and some Kafka internals (KRaft, ISR mechanics) would no longer apply |
| Managed Kafka (MSK / Confluent Cloud) | Hides exactly the internals this project exists to demonstrate; also costs money to run |
| NATS JetStream / Pulsar | Different architecture and semantics; not what the industry means by "Kafka experience" |

## Consequences

**Positive**
- Single-system operations; simpler local setup; faster metadata operations.
- Aligned with the only supported path going forward.

**Negative / accepted costs**
- Some older tutorials, Stack Overflow answers and tooling still assume ZooKeeper.
- A few third-party tools have lagged on KRaft support (mitigated: we use Kafka UI, which supports it).
- Combined-mode local dev differs from split-mode production — documented explicitly in the infra README.

**Neutral**
- Client applications are unaffected; KRaft is entirely server-side.

## Revisit when

Never, realistically. This is a one-way door that the upstream project already walked through.

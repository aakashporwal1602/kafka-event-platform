# ADR-0007: Transactional outbox for atomic state-and-event writes

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

A service that changes state and publishes an event about that change has a **dual-write problem**:

```ts
await db.orders.insert(order); // (1) commits
await kafka.publish('order.created'); // (2) crashes here
```

The database has the order; Kafka does not. Every downstream consumer — inventory, notifications,
analytics — is now permanently inconsistent with the source of truth, and nothing in the system
detects it.

Reversing the order is worse: publish first, then fail to commit, and consumers react to an order
that does not exist.

There is no ordering of two independent writes that is safe, because the failure window is between
them and cannot be closed by retrying — a retry after a crash has lost the in-memory state.

## Decision

Services write the event to an **`outbox` table in the same database transaction** as the state
change. A separate **outbox relay** reads that table and publishes to Kafka.

```sql
BEGIN;
  INSERT INTO orders (...) VALUES (...);
  INSERT INTO outbox (aggregate_id, event_type, payload, ...) VALUES (...);
COMMIT;                     -- both or neither. Atomic.
```

Two relay implementations are supported:

1. **Polling** — `SELECT ... WHERE published_at IS NULL ORDER BY id FOR UPDATE SKIP LOCKED` (simple,
   works on any database, adds latency and load).
2. **CDC via Debezium** — tails the Postgres WAL (near-zero latency, no polling load, more moving parts).

## Rationale

- **It converts a distributed-transaction problem into a local one.** The only atomicity requirement is
  within a single database, which databases already provide.
- **Delivery becomes at-least-once by construction.** If the relay crashes after publishing but before
  marking the row published, it republishes on restart. Consumers are idempotent (ADR-0008), so this
  is safe.
- **Ordering is preserved per aggregate**, because the outbox has a monotonic ID and the relay reads
  in order.
- **It is auditable.** The outbox table is a durable record of every event the service intended to emit,
  independent of Kafka retention.
- This is the standard solution — Debezium documents it, and it is the mechanism behind most
  production event-driven systems.

## Alternatives considered

| Option                                                      | Why rejected                                                                                                                                                                           |
| ----------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Naive dual write**                                        | The problem being solved                                                                                                                                                               |
| **Publish first, then persist**                             | Same window, worse failure mode — consumers see events for state that never committed                                                                                                  |
| **Two-phase commit (XA)**                                   | Kafka does not support XA. Even where 2PC exists it is slow, and a coordinator failure blocks participants holding locks                                                               |
| **Kafka as the only source of truth (pure event sourcing)** | Legitimate architecture, but it forces every query to go through a projection and imposes event sourcing on teams that do not need it. Considered in Ch 11 as an option, not a default |
| **Idempotent producer alone**                               | Kafka's idempotent producer prevents duplicates _within_ a producer session. It does nothing about the database/Kafka atomicity gap                                                    |

## Consequences

**Positive**

- No lost events, ever, for services that use the outbox.
- Works with any database that has transactions.
- Provides a durable, queryable audit trail of intent.

**Negative / accepted costs**

- **Added latency** between commit and publish — milliseconds with CDC, up to the poll interval with
  polling.
- **Outbox table growth.** Requires a retention/cleanup job; forgetting it is a classic production
  incident.
- **CDC brings operational weight**: replication slots, and the failure mode everyone hits once —
  _an inactive replication slot prevents WAL cleanup and eventually fills the disk_. Slot lag must be
  monitored and alerted from day one.
- Application code must remember to write to the outbox; mitigated by a repository helper so it is one
  call, not a checklist item.

**Neutral**

- Polling and CDC are interchangeable behind the same relay interface — start with polling, move to CDC
  when latency demands it.

## Revisit when

A service needs sub-millisecond publish latency after commit (rare), or the database becomes a
throughput bottleneck for event volume specifically.

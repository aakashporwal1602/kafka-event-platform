# ADR-0008: At-least-once delivery with consumer-side idempotency

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

"Exactly-once" is the most misunderstood phrase in event streaming. In a distributed system with
independent failure, **exactly-once _delivery_ is provably impossible** — the two-generals problem.
A consumer that crashes between processing a message and committing its offset will receive that
message again on restart, and no protocol can prevent it.

What _is_ achievable is **exactly-once effect**: the message may arrive more than once, but the
observable outcome happens once.

Kafka does offer transactions (`read-process-write` with `isolation.level=read_committed`), which
give exactly-once semantics **within Kafka** — consume from a topic, produce to another topic, commit
offsets, all atomically. But the moment a handler writes to Postgres, calls a payment API or sends an
email, that external effect is outside the transaction and Kafka's guarantee no longer covers it.

## Decision

The platform delivers **at-least-once** and makes effects idempotent at the consumer:

1. **Deduplication in Redis.** Every message carries a deterministic `idempotencyKey`. Before
   processing, the consumer performs `SET key <state> NX EX <ttl>`. If the key already exists in a
   terminal state, the message is skipped.
2. **Idempotent sink writes.** Handlers use upserts, conditional updates (`WHERE version = ?`) or
   unique constraints so that a duplicate write is a no-op at the database level.
3. **Kafka transactions where the effect is Kafka-only.** The retry engine and replay service, which
   consume-and-produce without external side effects, use transactional producers.

Redis is a **fast path, not the source of truth**. If Redis is unavailable we fail _open_ — process the
message and rely on layer 2 (the database constraint), because blocking the entire consumer on a cache
outage is worse than a rare duplicate attempt that the database will reject anyway.

## Rationale

- **It is honest about what is possible.** Claiming exactly-once delivery would be wrong; building on
  that claim would be dangerous.
- **Defence in depth.** Redis catches the common case cheaply; the database constraint catches
  everything Redis misses (eviction, outage, TTL expiry during a long replay).
- **It survives replay.** Chapter 10 replays historical events _deliberately_. Idempotent handlers make
  replay a safe operation rather than a data-corruption event — this is the property that makes the
  whole replay feature viable.
- **Kafka transactions are used where they genuinely fit** and avoided where they would give false
  confidence.

## The idempotency key

Choosing the key is the part people get wrong. It must be **deterministic from the event's business
meaning**, not from delivery metadata:

| Key choice                                | Verdict                                                                  |
| ----------------------------------------- | ------------------------------------------------------------------------ |
| `uuid()` generated at consume time        | ✗ Different on every delivery — useless                                  |
| Kafka `partition:offset`                  | ✗ Changes on replay and on re-publish; a replayed event would re-execute |
| `messageId` from the producer             | ⚠ Works only if the producer is itself idempotent                        |
| **`{eventType}:{aggregateId}:{version}`** | ✓ Deterministic, stable across replay, business-meaningful               |

## Alternatives considered

| Option                                      | Why rejected                                                                                                                                                                  |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Kafka EOS as the primary mechanism**      | Only covers Kafka-to-Kafka. Breaks the moment a handler touches an external system, which is nearly always. Also adds transaction-coordinator overhead and reduces throughput |
| **At-most-once (commit before processing)** | Silently loses messages on crash. Unacceptable for anything that matters                                                                                                      |
| **Database-only dedup (no Redis)**          | Correct but slow — a round trip and an index write per message. Redis handles the 99% case in sub-millisecond                                                                 |
| **Redis-only dedup (no DB constraint)**     | Redis is a cache: it evicts, it can lose data on failover. Sole reliance would produce silent duplicates                                                                      |
| **Bloom filter**                            | Space-efficient but false positives mean _dropping real messages_. Unacceptable                                                                                               |

## Consequences

**Positive**

- No message loss.
- Replay and retry are safe by construction.
- Degrades gracefully — a Redis outage costs efficiency, not correctness.

**Negative / accepted costs**

- **Every handler must be idempotent.** This is a real constraint on handler authors, and it is enforced
  by convention and code review rather than by the type system. The consumer framework (Ch 7) makes the
  correct path the default by requiring an explicit `idempotencyKey` from each handler.
- Redis memory cost proportional to throughput × TTL. At 10K/sec with a 24h window that is ~864M keys —
  so the TTL is tuned per topic, and the sizing math lives in Chapter 8.
- Dedup TTL must exceed the maximum expected replay window, or a replay older than the TTL will
  re-execute. Documented as an operational constraint.
- Two mechanisms to reason about instead of one.

**Neutral**

- Ordering is unaffected; dedup is per-key, not per-partition.

## Revisit when

A workload is genuinely Kafka-to-Kafka end to end, in which case Kafka transactions alone are simpler
and sufficient — as already applied in the retry engine.

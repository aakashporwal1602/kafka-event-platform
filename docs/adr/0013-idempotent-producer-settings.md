# ADR-0013: Idempotent producer, `acks=all`, five in flight

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering
- **Amends:** ADR-0004 (compression codec)

## Context

Four producer settings decide whether this platform loses data, duplicates it,
or reorders it. All four default to something weaker than we need, and none of
them produce a visible symptom in development.

The hard case is the **ambiguous produce timeout**: the broker may have written
the batch and lost the response. Retrying can duplicate. Not retrying can lose.
Both are wrong, and the choice cannot be made correctly at the message level.

## Decision

```
acks                  = -1  (all in-sync replicas)
enable.idempotence    = true
max.in.flight         = 5
compression           = gzip
retries               = 5, exponential with jitter, capped at 8s
allowAutoTopicCreation = false
```

## Rationale

### `acks=-1`

With `min.insync.replicas=2` (ADR-0001), two copies exist before the producer
is told the write succeeded. `acks=1` acknowledges on the leader alone — kill
that broker milliseconds later, during a rolling restart or a spot reclaim, and
the record is gone while the producer believes it landed.

Cost: p99 roughly 8–15 ms instead of 2–4 ms. For a platform whose entire value
proposition is that events are not lost, that is the product, not a trade-off.

### `enable.idempotence=true`

The producer gets a producer id and a per-partition sequence number, so the
broker recognises and discards a retry of a batch it already wrote. This is
what **resolves the ambiguous timeout**: retrying becomes simply correct.

It is also what allows `errors.ts` to classify `RequestTimedOut` as retryable.
**These two decisions are one decision** — turning idempotence off silently
converts every produce timeout into a duplicate.

### `max.in.flight=5`, not 1

This is the setting people get wrong in both directions.

- **Without idempotence**, more than one in-flight request can **reorder on
  retry**: batch 1 fails, batch 2 succeeds, batch 1 is retried and lands
  second. Per-partition ordering — the only ordering Kafka offers — is broken.
  Hence the classic advice, `max.in.flight=1`.
- **With idempotence**, the broker tracks sequence numbers and refuses to write
  a batch out of order, holding up to five in flight per partition and ordering
  them itself. So 5 is _both_ safe and roughly 3–4× the throughput of 1,
  because the producer is not stalled on each round trip.

5 is the broker's maximum for an idempotent producer. **Setting 6 silently
disables the ordering guarantee** rather than erroring, which is why the number
is written down rather than tuned.

### Compression: gzip — amending ADR-0004

ADR-0004 states lz4. That was written before the producer existed and was wrong
on one point: **KafkaJS ships only gzip built in.** Snappy and LZ4 require
separate codec packages, which reintroduces exactly the install friction that
ADR-0004 chose KafkaJS to avoid.

Gzip costs more CPU for a better ratio. At the 10K events/sec baseline on ~1 KB
events that is a few percent of a core. If produce-side CPU becomes the
constraint, registering `kafkajs-lz4` is a one-line change — and this ADR
should be amended again rather than the setting quietly diverging from the
document.

### Bounded retries with jitter

Unbounded retry converts a cluster outage into unexplained latency instead of
an error the caller can act on. Jitter matters more than it looks: without it,
every producer in the fleet retries in lockstep after a broker blip and the
recovering broker is hit by a synchronised wave.

## Alternatives considered

| Option                                    | Why rejected                                                                                                                                                                                                                                    |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`acks=1`**                              | 3–5× lower latency, and a window in which an acknowledged write is lost to a single broker failure. Acceptable for metrics, not for an event platform                                                                                           |
| **`acks=0`**                              | Fire-and-forget. Not a durability story at all                                                                                                                                                                                                  |
| **Idempotence off, `max.in.flight=1`**    | Also ordering-safe, at roughly a quarter of the throughput, and leaves the ambiguous timeout unresolved                                                                                                                                         |
| **Transactional producer everywhere**     | Exactly-once _within Kafka_, at the cost of a coordinator round trip on every publish — for a guarantee that ends at the first Postgres write anyway (ADR-0008). Reserved for the retry engine and replay service, whose effects are Kafka-only |
| **`linger.ms` tuning for larger batches** | Real throughput gains, deferred: the outbox relay already batches at the application level, which is a bigger and more visible lever                                                                                                            |
| **lz4 via `kafkajs-lz4`**                 | Better CPU/ratio balance, at the cost of a codec dependency. Revisit when produce CPU is measured to be the bottleneck                                                                                                                          |

## Consequences

**Positive**

- An acknowledged write exists on at least two replicas.
- A produce retry cannot duplicate, so timeouts are safely retryable.
- Per-partition ordering holds at five in-flight requests.
- A missing topic fails loudly instead of being created with no redundancy.

**Negative / accepted costs**

- **Produce latency roughly 3× that of `acks=1`.** The deliberate cost.
- **Idempotence guarantees are per producer session.** A restart yields a new
  producer id, so a batch in flight across a crash can still duplicate. That is
  the residual case consumer-side idempotency exists for (ADR-0008), and it is
  why "idempotent producer" must never be described as exactly-once.
- **`OutOfOrderSequenceNumber` is fatal, not retryable.** When producer and
  broker sequence state diverge, only recreating the producer recovers it.
  Handled explicitly in `errors.ts`; a naive retry loop would spin forever.
- **Gzip diverges from ADR-0004** until that ADR is amended or a codec is added.
- **Throughput is below `librdkafka`'s**, per ADR-0004's own accepted cost.

## Revisit when

Produce throughput becomes the platform bottleneck — measured, not assumed. The
order of levers at that point: `linger.ms` and batch size first, then the lz4
codec, then `librdkafka`.

# ADR-0005: Non-blocking retries via tiered retry topics

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Consumers fail. A downstream API times out, a database deadlocks, a dependency returns 503. Most of
these are **transient** — the same message would succeed if retried in thirty seconds.

Kafka's ordering guarantee is per-partition, and a consumer processes a partition sequentially. That
creates the central tension: **anything that blocks message N blocks N+1 through infinity on the same
partition.** A single message that takes 60 seconds of retries stalls every message behind it.

## Decision

Retries are **non-blocking**, implemented as a chain of dedicated retry topics with increasing delays:

```
events.<domain> ──fail──► retry.<domain>.5s ──fail──► retry.<domain>.30s
                              │                            │
                              └──► retry.<domain>.5m ──► retry.<domain>.1h ──► dlq.<domain>
```

The main consumer commits its offset **immediately** after forwarding a failed message to the first
retry tier. Each retry topic has its own consumer, which waits until the message's scheduled time
before processing.

## Rationale

- **Head-of-line blocking becomes structurally impossible.** The main partition never waits.
- **Delay is bounded per topic, so waiting is cheap.** A `retry.5s` consumer sleeps at most 5 seconds
  before its next message is due — it does not need a scheduler, just a sleep-until-timestamp check.
- **Retry pressure is observable.** Each tier is a topic with its own lag metric. "How many events are
  in the 5-minute retry tier right now" is a Prometheus query, not a log grep.
- **Tiers encode intent.** A message in `retry.1h` has failed four times over an hour — that is a
  different operational signal from one that just failed once.
- **This is the pattern Uber published** for their consumer platform, and the same shape appears in
  Confluent's reference architectures. It is the industry answer, not an invention.

## Alternatives considered

| Option                                       | Why rejected                                                                                                                                             |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **In-place retry with sleep**                | Blocks the partition. Also risks exceeding `max.poll.interval.ms`, which triggers a rebalance and makes the problem dramatically worse                   |
| **`pause()` + reprocess**                    | Still blocks that partition; only defers the problem                                                                                                     |
| **Single retry topic with delay header**     | The consumer must either sleep for the longest delay (blocking again) or repeatedly re-publish, which produces churn and loses attempt ordering          |
| **External scheduler (Redis ZSET / Quartz)** | Workable and we use a variant for _delayed events_ (Ch 14), but for retries it moves durability out of Kafka and adds a component that can lose messages |
| **Infinite retry**                           | Guarantees a poison message loops forever, consuming capacity indefinitely                                                                               |

## Consequences

**Positive**

- No head-of-line blocking, ever.
- Per-tier lag metrics give precise operational visibility.
- Retry capacity is isolated from primary consumption capacity.

**Negative / accepted costs**

- **Ordering is not preserved across a retry.** A message that fails and is retried will be processed
  _after_ messages that came behind it. This is the fundamental trade-off, and it is acceptable
  because our consumers are idempotent and order-tolerant per ADR-0008. Where strict ordering matters,
  the handler must fail the partition deliberately — documented as an escape hatch.
- More topics to manage (4 retry tiers + 1 DLQ per domain). Mitigated by automated topic provisioning.
- Retry consumers add infrastructure cost even when idle.
- Attempt count and original metadata must be propagated in headers, and every hop must preserve them.

**Neutral**

- Total retry window is 5s + 30s + 5m + 1h ≈ 1h 5m before DLQ — tunable per topic.

## Revisit when

A domain requires strict ordering under failure, in which case that specific consumer opts into
blocking retry with an explicit, documented exception.

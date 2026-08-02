# ADR-0011: Single-node Redis lock with fencing tokens, not Redlock

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Several platform operations must not run twice concurrently:

- The **outbox relay** claiming a batch — although `FOR UPDATE SKIP LOCKED`
  already handles this at the row level (ADR-0010), so no lock is needed.
- The **replay coordinator** advancing a job's cursor (Chapter 10).
- **Schema registration** for a subject, where two concurrent registrations of
  compatible-but-different schemas would both succeed and produce two "latest"
  versions.
- **Partition-scoped compaction** and other periodic maintenance where a second
  runner is wasteful rather than dangerous.

Those are two different requirements wearing the same word. The first three
need **correctness** — a second runner corrupts state. The last needs
**efficiency** — a second runner just burns CPU. Conflating them is how
distributed locks get misused.

## Decision

A single-node Redis lock (`SET NX PX` plus Lua compare-and-delete) that returns
a **strictly increasing fencing token**, and a documented requirement that any
resource needing correctness validates that token **in the same atomic
operation as the write**.

The lock provides efficiency. The token provides correctness. They are separate
mechanisms and the code says so.

## Rationale

### Why no lock on Redis can guarantee mutual exclusion

A lease-based lock is safe only if the holder can tell whether its lease is
still valid. It cannot, because:

- **Process pauses are unbounded.** A stop-the-world GC, a page-fault storm, a
  live VM migration, or a container throttled by its CPU quota can suspend a
  process for tens of seconds. The process observes none of it — from inside,
  no time has passed.
- **Clocks are not monotonic across nodes.** NTP steps, leap-second smearing
  and virtualised clocks all move wall time. Redis expires keys on its own
  clock; the client reasons about the lease on a different one.

So this sequence is always possible, regardless of TTL:

```
A: acquire → token 33
A: ── stalled 40s ──────────────────────────────►
                    Redis: lease expires, key deleted
                    B: acquire → token 34, does the work
A: resumes, believes it holds the lock, writes
```

Raising the TTL does not fix it — there is no TTL longer than the longest
possible pause. This is why the lock cannot be the correctness mechanism, and
why the token must be.

### Why Redlock specifically is rejected

Redlock acquires the lock on a majority of N independent Redis nodes. It is a
well-known algorithm and it is not obviously wrong, but:

- **It does not address the pause above at all.** Majority acquisition protects
  against _node failure_, not against the holder losing track of time. The
  stale-writer sequence is unchanged. This is Kleppmann's central objection and
  it has never been answered — Redlock's own documentation now recommends
  fencing tokens, at which point the majority acquisition is buying protection
  against a different failure than the one people adopt it for.
- **Its safety argument depends on bounded clock drift** between the Redis
  nodes. That is an assumption about operational reality, not a property the
  algorithm enforces, and it fails silently when violated.
- **Redlock cannot produce a monotonic token.** Independent nodes have
  independent counters, so there is no value that is globally increasing —
  the one thing actually needed for correctness is the thing it cannot supply.
- **It costs 5× the infrastructure** and adds a quorum failure mode to a
  dependency that currently has none.

Paying for five nodes to obtain a weaker guarantee than one node plus a token
is not a trade this project will make.

### Why single-node Redis is nonetheless the right host for the lock

- The lock is **advisory and efficiency-oriented**, so its unavailability is a
  degraded mode, not an outage: callers either skip the tick or fall back to
  running unlocked with fencing still enforced.
- It is already a dependency, already monitored, already on the hot path for
  idempotency — no new operational surface.
- Sub-millisecond acquisition. A lock that costs 20 ms is a lock people avoid
  using, which is worse than one they use correctly.

## Alternatives considered

| Option                               | Why rejected                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Redlock (multi-node Redis)**       | Solves node failure, not holder pauses; cannot produce a monotonic token; safety rests on a clock-drift assumption; 5× infrastructure. Detailed above                                                                                                                                                                                                                                                                                                 |
| **Postgres advisory locks**          | Genuinely strong — session-scoped, released automatically on disconnect, no TTL to outrun, and **already used for migrations** where exactly those properties matter. Rejected as the general mechanism because it pins a pooled connection for the whole critical section, and a 30s lock at 20 connections per replica is a fast route to pool exhaustion. Correct for rare, short, startup-time coordination; wrong for anything on a request path |
| **ZooKeeper / etcd ephemeral znode** | The textbook-correct answer: real consensus, session-tied ephemeral nodes, and a monotonic `zxid`/`revision` that _is_ a fencing token. Rejected on operational cost — a third stateful system to run, monitor and upgrade, for a set of use cases that a token on a Postgres row already covers. Revisit if lock-requiring operations become central rather than peripheral                                                                          |
| **Kafka consumer-group assignment**  | Partition assignment already gives single-writer-per-partition semantics, and generation ID is a natural fencing token. Genuinely used for consumer work — but it only covers work that is partitioned by a Kafka key, which the replay coordinator and schema registration are not                                                                                                                                                                   |
| **Postgres row lock only, no Redis** | `SELECT ... FOR UPDATE` on a coordination row. Correct, and used where the work is already a transaction. Rejected as the general mechanism for the same connection-pinning reason as advisory locks                                                                                                                                                                                                                                                  |
| **No lock, idempotent operations**   | The ideal, and preferred wherever achievable — the outbox relay takes exactly this route via `SKIP LOCKED`. Not always achievable: advancing a replay cursor is inherently a read-modify-write on shared state                                                                                                                                                                                                                                        |

## Consequences

**Positive**

- The correctness guarantee does not depend on clocks, pauses, or Redis being
  correct — only on the resource checking a number.
- One Redis node, one round trip, no quorum.
- The distinction between efficiency locks and correctness locks is explicit in
  the API: `acquire` returns a `Lease` whose `token` is the caller's
  responsibility to use.

**Negative / accepted costs**

- **The token is only useful if the resource checks it.** A caller that ignores
  `lease.token` has an efficiency lock and may not realise it. Mitigated by
  naming (`Lease.token` is not optional in the type), by the module docs, and
  by making the guard a required parameter on the operations that need it.
- **A stale holder's work is wasted, not prevented.** Fencing rejects the write
  at the end; it does not stop the holder doing the work. Acceptable — the
  wasted work is bounded by the operation, and preventing it would require the
  guarantee we just argued is unobtainable.
- **Redis loss releases every lock at once.** With `appendfsync everysec` and no
  replication, a node failure drops all lock keys. Every holder then competes
  again, and the fencing counter restarting is the dangerous part — mitigated
  by the counter's 1h TTL being far longer than any lease, and by AOF being
  enabled. If a lock protects something where a counter reset is unacceptable,
  that resource's guard must be persisted in Postgres, not derived from Redis.
- **`InMemoryFencingGuard` protects one process.** It exists for tests and
  process-local resources. The shared-resource guard is a `fence_token` column
  and a `WHERE fence_token < $n` predicate, introduced in Chapter 10.

## Revisit when

A lock-protected operation appears where **no** resource-side check is possible
— i.e. the protected effect is an external side effect that cannot be made
conditional (sending money, calling a non-idempotent third-party API). At that
point the fencing argument no longer applies and a real consensus system
(etcd or ZooKeeper) becomes the honest answer.

## References

- Martin Kleppmann, _How to do distributed locking_ (2016) — the fencing-token
  argument this ADR follows.
- Salvatore Sanfilippo, _Is Redlock safe?_ — the rebuttal. Worth reading;
  its strongest point is that Redlock's guarantees are adequate for efficiency
  use cases, which is precisely the scope granted to the lock here.

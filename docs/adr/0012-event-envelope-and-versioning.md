# ADR-0012: A separate event envelope, and versioning in it

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Nine services publish events that four generic components — the retry engine,
the DLQ, the replay service and the audit log — must handle **without
understanding a single business schema**. Those components need to know where
an event came from, whether they have seen it before, and where to send it
next. None of that is business data.

The default is to publish the business payload directly (`{ orderId, total }`
on a topic named `orders`) and add metadata fields as they turn out to be
needed. That works until the first time metadata must change, at which point
every producer and every consumer changes together.

## Decision

Every message is an **envelope** with a fixed metadata contract and an opaque
`payload`. The envelope is serialised into the message value **and** mirrored
into Kafka headers. Payload shape is identified by `eventType` plus an integer
`eventVersion` carried in the envelope, in addition to the schema registry's
own id (Chapter 6).

## Rationale

- **Generic components stay generic.** The retry engine routes on
  `eventType`, `attempt` and the topic name. It never deserialises a payload,
  so it does not depend on the schema registry being available — which matters
  because the retry path runs precisely when things are already broken.
- **Metadata evolves separately from business schemas.** Adding `causationId`
  is an envelope change; adding a field to `order.placed` is a schema change.
  Conflating them means every metadata addition is a fleet-wide migration.
- **Headers are readable without deserialisation.** A DLQ message is often
  there _because_ its payload cannot be parsed. Event type, aggregate and
  correlation id must still be visible.
- **Two timestamps, because there are two facts.** `occurredAt` is when the
  thing happened; `recordedAt` is when the platform learned of it. They differ
  by days when a mobile client syncs late. A system with one field has to
  choose which of the two questions it answers wrongly.
- **`eventVersion` enables decisions the registry cannot.** The registry
  guarantees _compatibility_; it does not let a consumer branch, because the
  schema id is resolved during deserialisation — after routing. During a
  breaking change, v1 and v2 consumers run side by side and need to route
  before they parse.

### The event-type grammar

`<aggregate>.<past-tense verb>`, lowercase, dot-separated, enforced by regex.

Past tense is not a style preference. An event is a fact that already happened
and cannot be refused; a command is a request that can. `order.place` invites a
consumer to "handle" it by deciding whether to allow it — making that consumer
a hidden authority over a decision the producer already made. `order.placed`
cannot be read that way.

Underscores are excluded because Kafka's metric names conflate `.` and `_`, so
two distinct topics report as one metric.

### Idempotency keys derived from business meaning

`sha256(eventType, aggregateType, aggregateId, aggregateVersion)`, truncated to
128 bits, with each part length-prefixed.

- **Not `partition:offset`.** A replay republishes a historical event at a new
  offset, so the key changes, the consumer sees a new event, and every side
  effect happens again. The replay feature would corrupt data silently, and
  only for events somebody deliberately replayed.
- **Hashed, not concatenated.** Every key lives in Redis for 24 hours and
  `redis-keys.ts` derives memory cost from key length. A bounded 32-character
  key keeps that sizing honest against caller-supplied aggregate ids.
- **Length-prefixed, not separator-joined.** With a separator, `("agg b", "c")`
  and `("agg", "b c")` hash identically and one of two genuinely distinct
  events is dropped as a duplicate. Nothing errors and nothing is logged, which
  makes it the worst failure mode available.

## Alternatives considered

| Option                                     | Why rejected                                                                                                                                                                                                                                                                           |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Payload only, metadata in headers**      | Headers are `Map<string, bytes>` with no schema, no validation and no evolution story. Correlation ids would be untyped strings nobody could refactor                                                                                                                                  |
| **CloudEvents**                            | Genuinely close, and a real standard with tooling. Rejected because its extension mechanism for `idempotencyKey`, `causationId` and `tenantId` is stringly-typed attributes, which is most of what we need — leaving a standard whose main benefit is interop we do not currently have |
| **Metadata only in the value**             | A DLQ message with an unparseable payload becomes untriageable, and the retry engine gains a dependency on the schema registry                                                                                                                                                         |
| **Schema registry version only**           | Cannot be read before deserialisation, so it cannot drive routing during a breaking change                                                                                                                                                                                             |
| **Single `timestamp`**                     | Forces a choice between reporting on business time and on system time, and the choice is invisible at every call site                                                                                                                                                                  |
| **UUID event id as the deduplication key** | Not stable across a relay retry or a replay, so it deduplicates nothing that matters                                                                                                                                                                                                   |

## Consequences

**Positive**

- Retry, DLQ, replay and audit are written once, against envelopes.
- Metadata and business schemas version independently.
- A message is triageable even when its payload is not parseable.
- Deduplication survives replay, which is what makes replay safe to run.

**Negative / accepted costs**

- **Header duplication costs ~250–400 bytes per message.** On a 2 KB payload
  that is 15%; on a 200-byte payload it is 150%. Accepted, and the reason
  `event.ts` caps envelope string lengths — but it means very small events are
  disproportionately expensive and should be batched into meaningful ones.
- **Two timestamps mean every consumer must choose.** A consumer that picks the
  wrong one produces a subtly wrong report rather than an error. Mitigated by
  naming, not solved.
- **The grammar rejects names people will want.** `order.place` and
  `OrderPlaced` both fail. Deliberate, and the error message explains why
  rather than only stating the pattern.
- **CloudEvents interop is now a translation layer** if it is ever needed.

## Revisit when

An external party needs to consume these events directly. At that point
CloudEvents' interop becomes worth its extension-mechanism cost, and the
translation should happen at the boundary rather than by changing the internal
contract.

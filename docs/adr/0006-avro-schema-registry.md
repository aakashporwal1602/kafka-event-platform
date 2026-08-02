# ADR-0006: Avro with a schema registry for event contracts

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

An event published today may be consumed by a service written next year, by a replay job six months
from now, or by a team that has never spoken to the producer. The event payload *is* an API — and
unlike an HTTP API, it has no request/response cycle in which to negotiate versions.

Without an enforced contract the failure mode is well known: a producer adds a required field or
renames one, and consumers break at runtime, in production, at a time nobody chose.

## Decision

All events are serialized with **Apache Avro** against schemas held in a **schema registry**, using
the Confluent wire format:

```
┌────────┬──────────────────┬─────────────────────┐
│ byte 0 │  bytes 1–4       │  bytes 5+           │
│ 0x00   │  schema ID (BE)  │  Avro binary payload│
└────────┴──────────────────┴─────────────────────┘
```

Default compatibility mode is **BACKWARD** (new schema can read data written by the previous schema).

## Rationale

- **Compact.** Avro writes no field names on the wire — just values in schema order. Against JSON this
  is typically 40–60% smaller before compression, which at 864M events/day is measured in terabytes.
- **Schema travels by reference, not by value.** The 4-byte schema ID means the payload carries no
  schema overhead, but any consumer can resolve the exact writer schema.
- **Schema resolution is a first-class Avro feature.** Reader and writer schemas may differ, and Avro
  reconciles them using field defaults. This is *the* mechanism that makes independent deployment safe.
- **Compatibility is machine-checkable.** The registry rejects an incompatible schema at registration
  time — in CI, before deployment — rather than at consumption time in production.
- **It is the Kafka default.** Confluent Schema Registry, Karapace and Apicurio all speak this protocol;
  our implementation is API-compatible, so it can be swapped for a managed registry.

## Alternatives considered

| Option | Why rejected |
|---|---|
| **JSON + JSON Schema** | Human-readable and simple, but 2–3× larger on the wire and, more importantly, JSON Schema has no standard *resolution* mechanism — a reader cannot systematically reconcile a different writer schema |
| **Protobuf** | Genuinely excellent: compact, fast, good tooling, and superior for RPC. Rejected because Avro's dynamic schema resolution suits event streaming better — Protobuf requires generated code on both sides, which couples deployment. Also, Avro remains the Kafka-ecosystem default. |
| **No schema (raw JSON)** | The status quo this project exists to fix |
| **MessagePack / CBOR** | Compact but schemaless; solves size, not contracts |

## Consequences

**Positive**
- Breaking changes are caught in CI, not production.
- Significant storage and bandwidth reduction.
- Producers and consumers deploy independently and safely.
- Every event is self-describing via its schema ID — replay and forensics work years later.

**Negative / accepted costs**
- **Payloads are not human-readable.** `kafka-console-consumer` shows binary. Mitigated by a decode
  endpoint and Kafka UI's registry integration, but it is a real day-to-day friction.
- The registry becomes a dependency on the publish path. Mitigated by aggressive local caching —
  schema IDs are immutable, so caching is safe indefinitely, and producers keep working if the
  registry is down (they simply cannot register *new* schemas).
- Developers must learn Avro's type system and its default-value rules, which have sharp edges
  (notably: a union's default must match the *first* branch).
- Schema evolution requires discipline — adding a required field without a default is a breaking change
  that the registry will correctly refuse.

**Neutral**
- BACKWARD is the default but is configurable per subject; FORWARD suits cases where producers upgrade last.

## Revisit when

We need cross-language codegen ergonomics that Protobuf does better, or adopt a managed registry
(which this decision explicitly keeps possible).

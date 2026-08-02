# ADR-0004: Use KafkaJS as the Kafka client

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Node.js has two serious Kafka clients:

- **KafkaJS** — a pure-JavaScript implementation of the Kafka protocol.
- **node-rdkafka** — Node bindings over `librdkafka`, the battle-tested C++ client that also underpins
  the Python, Go and .NET clients.

They differ on performance, feature coverage and — critically for a repository other people will
clone — installation friction.

## Decision

Use **KafkaJS** for all producers and consumers, behind our own `@platform/kafka-client` abstraction
so the choice is reversible.

## Rationale

- **Zero-friction install.** `node-rdkafka` compiles native code via `node-gyp`. On a reviewer's
  machine that means a toolchain, a Python version and platform-specific failures. A portfolio project
  that fails at `npm install` is worth nothing.
- **Adequate for the target.** KafkaJS comfortably sustains our 10,000 events/sec baseline. The
  performance gap only becomes decisive in the hundreds of thousands per second.
- **Better TypeScript.** First-class types rather than hand-maintained `@types` over a C++ surface.
- **Readable internals.** Being pure JS, the protocol implementation can be read and debugged — which
  matters when the point of the project is to _understand_ Kafka rather than merely use it.
- **Wrapped anyway.** Every service talks to `@platform/kafka-client`, not to KafkaJS directly. If we
  ever need `librdkafka` throughput, we swap one adapter. This is ADR-0002's DIP applied to a vendor
  boundary.

## Alternatives considered

| Option                             | Why rejected                                                                                                                                                |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **node-rdkafka**                   | Faster and more feature-complete (zstd, richer transactional support), but native-build friction, weaker types, and throughput we do not need at this scale |
| **@confluentinc/kafka-javascript** | Confluent's official client, promising and `librdkafka`-backed, but still maturing and shares the native-build cost                                         |
| **Direct protocol implementation** | Educational but absurd; the protocol is large and the wire format evolves                                                                                   |

## Consequences

**Positive**

- `pnpm install` works everywhere with no toolchain.
- Strong typing throughout; debuggable client internals.
- Vendor lock-in avoided by the wrapper.

**Negative / accepted costs**

- **Lower peak throughput** than `librdkafka` — the single most important cost, and the honest answer
  in an interview is "at 500K events/sec I would switch".
- **No zstd compression** (KafkaJS supports gzip, snappy, lz4). We use **lz4**, which is the right
  trade-off anyway: near-snappy speed with better ratios.
- KafkaJS's transactional support is functional but less battle-hardened than `librdkafka`'s. This
  reinforces ADR-0008 — Redis idempotency is our primary correctness mechanism, not Kafka transactions.
- Maintenance cadence has been slower recently; monitored, and the wrapper limits exposure.

**Neutral**

- Consumer group protocol, rebalancing and offset semantics are identical — they are server-side.

## Revisit when

Sustained throughput exceeds ~200K events/sec per service, zstd is required for storage cost, or
KafkaJS maintenance stalls further.

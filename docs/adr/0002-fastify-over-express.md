# ADR-0002: Use Fastify as the HTTP framework

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

The gateway sits on the hot path of every published event. At the 10,000 events/sec baseline it
handles that request rate directly, and every request involves JSON parsing, validation and
serialization. Framework overhead is not a rounding error at this volume.

We also need request/response validation that is **declarative** — hand-written validation drifts
from documentation immediately — and an OpenAPI specification that is generated from the same source
of truth rather than maintained separately.

## Decision

Use **Fastify 5** for all HTTP services, with **TypeBox** for schema definitions, and generate the
OpenAPI document from those schemas via `@fastify/swagger`.

## Rationale

- **Serialization is the bottleneck, and Fastify compiles it.** Fastify pre-compiles response schemas
  into optimised serializer functions rather than calling `JSON.stringify` reflectively. On
  JSON-heavy payloads this is a 2–3× throughput difference — the single biggest framework-level win
  available here.
- **Validation is built in, not bolted on.** JSON Schema validation via Ajv is a first-class concept,
  compiled once at route registration.
- **One source of truth for schema, types and docs.** A TypeBox schema yields the runtime validator,
  the TypeScript type (via `Static<typeof T>`) and the OpenAPI fragment. Express requires three
  separate mechanisms that can silently disagree.
- **Plugin encapsulation gives scoped DI.** Fastify's plugin tree creates natural dependency scopes,
  which composes well with our container (Chapter 2) instead of fighting it.
- **Structured logging by default.** Pino is integrated, with per-request child loggers — exactly the
  correlation-ID propagation model we need.

## Alternatives considered

| Option              | Why rejected                                                                                                                                                                                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Express 5**       | Ubiquitous and well understood, but no built-in validation or serialization compilation, and middleware is untyped. It is the safe answer, not the considered one.                                                                                           |
| **NestJS**          | Excellent structure and DI, but heavy: decorators, metadata reflection and a large runtime. It would also _hide_ the architectural decisions this project exists to demonstrate — the DI container becomes framework magic instead of something we designed. |
| **Hono**            | Very fast and elegant, but its strength is edge/serverless runtimes; the Node ecosystem integration (Kafka, Postgres, OTel) is less mature.                                                                                                                  |
| **Raw `node:http`** | Maximum control, but re-implementing routing, validation and lifecycle is undifferentiated work.                                                                                                                                                             |

## Consequences

**Positive**

- Higher throughput per instance; measurably lower p99 at the gateway.
- Schemas, types and API docs cannot drift apart.
- Encapsulated plugins keep service wiring explicit.

**Negative / accepted costs**

- Smaller ecosystem than Express; some middleware needs a Fastify-specific equivalent.
- The plugin encapsulation model has a genuine learning curve — decorators registered in a child
  scope are not visible to the parent, which surprises people once.
- Team familiarity with Express is more common (noted, not decisive).

**Neutral**

- Fastify supports Express middleware via `@fastify/middie` if we ever need it.

## Revisit when

The gateway is no longer JSON-bound (e.g. we move to gRPC or a binary protocol on the edge), or
Fastify's maintenance cadence materially slows.

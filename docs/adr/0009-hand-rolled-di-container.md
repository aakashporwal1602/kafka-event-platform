# ADR-0009: Hand-rolled DI container instead of a framework

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

Nine services share five libraries, and each service wires together a Kafka
client, a Postgres pool, a Redis client, a schema registry client, a logger and a
clock. Those graphs are four or five levels deep, and the same object graph must
be reconstructible in tests with two or three nodes replaced by fakes.

Manual wiring works and is what the composition root does anyway. It stops
scaling when a dependency four levels down changes signature and every
construction site — production and test — must be edited by hand.

The TypeScript DI ecosystem is decorator-based, and that carries costs that are
not obvious until you hit them.

## Decision

A hand-rolled container in `@platform/core`, roughly 150 lines, with no
dependencies. Tokens are branded symbols carrying a phantom type parameter.
Two lifetimes: singleton and transient. No decorators, no reflection.

## Rationale

- **Interfaces work as tokens.** This is the decisive point. Decorator-based
  containers infer the token from a constructor parameter's _type_, and
  TypeScript erases interfaces at runtime — so `constructor(db: Database)` where
  `Database` is an interface has no runtime token to resolve. The workaround is
  `@inject('Database')` string tokens, which reintroduces stringly-typed keys and
  loses the benefit. Explicit tokens mean services depend on interfaces, which is
  the actual point of dependency inversion (ADR reference: SOLID "D").
- **No global TypeScript config change.** `reflect-metadata` requires
  `experimentalDecorators` and `emitDecoratorMetadata` repo-wide, plus a
  polyfill imported before any other module. Both are viral.
- **Small enough to read.** A contributor can read the container in five minutes
  and know exactly what it does. Container behaviour is never a mystery to debug.
- **Failures surface at startup.** `verify()` eagerly constructs every singleton,
  so a mis-wired graph fails where a health check catches it and the deploy rolls
  back — not on the first request that reaches the broken branch.

## Alternatives considered

| Option                 | Why rejected                                                                                                                                                                                                                            |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **InversifyJS**        | Mature and capable, but decorator + `reflect-metadata` based; interfaces cannot be tokens without falling back to strings; adds ~50 KB and a global config change                                                                       |
| **tsyringe**           | Lighter than Inversify, same fundamental constraint on interfaces                                                                                                                                                                       |
| **NestJS**             | Brings an entire application framework — HTTP layer, module system, CLI — to obtain DI. Would also hide the architectural decisions this repository exists to demonstrate                                                               |
| **Awilix**             | Genuinely good and decorator-free, with `asClass`/`asFunction` registration. Rejected only because its proxy-based auto-injection resolves by _parameter name_, which breaks under minification and makes the dependency implicit again |
| **Manual wiring only** | Viable and still used at the composition root. Rejected as the sole mechanism because scoped resolution (per-request context) and test overrides become hand-maintained duplication                                                     |

## Consequences

**Positive**

- Services depend on interfaces, not concrete classes.
- Zero dependencies, no reflection, no build configuration.
- Test setup is "build the production container, override two things".
- Compile-time type safety on resolution with no casts at call sites.

**Negative / accepted costs**

- **Tokens are declared manually** — `export const KAFKA = token<KafkaClient>('KafkaClient')`.
  One extra line per dependency. This is the direct trade for interface support.
- **No automatic constructor injection.** Factories are written by hand:
  `container.register(REPO, (c) => new Repo(c.resolve(DB)))`. More explicit,
  more verbose.
- **No async factories.** A Kafka client must connect before use; that is handled
  by the lifecycle manager instead. Deliberate — mixing lifecycle into a
  container is how containers become frameworks.
- **Cycles are detected at resolution, not registration.** Detecting earlier
  needs a full graph walk on every `register`, which costs more than it saves
  given the resolution-time check reports the full path.

## Revisit when

The container needs a third lifetime, async factories, or automatic disposal —
at which point it is growing into a framework and adopting a real one becomes
the cheaper option.

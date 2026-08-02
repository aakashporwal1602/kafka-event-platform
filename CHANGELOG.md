# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

While the major version is `0`, each chapter increments the **minor** version. `v1.0.0` marks the
production-ready release at Chapter 17.

## [Unreleased]

_Nothing yet._

---

## [0.3.0] — 2026-08-02

**Chapter 2 — Shared Core Library**

### Added

- **`@platform/core`** — cross-cutting primitives with no knowledge of Kafka,
  HTTP or Postgres, so the domain stays testable without infrastructure
  - `Result<T, E>` — expected failures as values. Exceptions hide the failure
    path, and the transient-vs-permanent branch is too important to leave
    implicit. Includes `all` (fail-fast, for bulk publish) and `partition`
    (for HTTP 207 Multi-Status)
  - **Error taxonomy** — `TransientError` / `PermanentError` hierarchy driving
    the retry-vs-DLQ decision (ADR-0005). `retryable` is an abstract member so
    two throw sites of one class cannot disagree. Unclassified errors default
    to **permanent**: retrying an unknown error amplifies load during an
    incident and hides inside normal retry traffic
  - **DI container** — ~150 lines, no decorators, no `reflect-metadata`.
    Branded symbol tokens give compile-time safety and let interfaces be
    tokens. `verify()` eagerly constructs singletons so mis-wiring fails at
    startup, where a health check catches it
  - **`Clock`** — injectable time with separate wall-clock and monotonic
    readings. `FixedClock` collapses an hour of retry backoff into a
    synchronous assertion; `setTime()` reproduces NTP-correction bugs
  - **Context propagation** — `AsyncLocalStorage`-based correlation IDs that
    survive `await`, timers and promise continuations, with per-request
    isolation under concurrency
  - **Configuration** — Zod schema validated once at startup, reporting every
    problem at once and returning a frozen object. Hand-written numeric
    transforms instead of `z.coerce.number()`, which accepts `''` as `0` and
    `'abc'` as `NaN`
  - **Logger** — pino with automatic context injection, cause-chain
    serialisation and secret redaction. `RecordingLogger` for assertions
  - **Lifecycle** — two-phase graceful shutdown: drain (stop accepting)
    then close (flush and release) in reverse registration order, with
    per-hook and global deadlines below Kubernetes' grace period
- **ADR-0009** — hand-rolled DI container instead of a framework
- **`docs/lld/01-core-library.md`** — module map, error hierarchy, shutdown
  state machine, and the testing seams the package provides
- **The review standard** in `CONTRIBUTING.md` — every non-trivial decision
  must carry its rejected alternative, accepted cost, scaling limit and
  failure mode
- 120 unit tests covering the properties that fail silently

### Fixed

- Type-aware linting now uses a dedicated `tsconfig.eslint.json`. The project
  service resolves the nearest tsconfig per file, leaving every `*.test.ts`
  unlintable because build configs exclude them; `allowDefaultProject` was not
  a fix, since inferred projects run without `strictNullChecks`
- `process.env` accessed with bracket notation, required by
  `noPropertyAccessFromIndexSignature` — the rule turns an env-var typo into a
  build failure instead of a silent `undefined`

[0.3.0]: https://github.com/aakashporwal1602/kafka-event-platform/compare/v0.2.0...v0.3.0

---

## [0.2.0] — 2026-08-02

**Chapter 1 — Infrastructure Foundation**

### Added

- **Local infrastructure stack** (`infra/docker/docker-compose.yml`)
  - 3-broker Kafka cluster in KRaft mode, combined broker/controller roles
  - Durability defaults enforced cluster-wide: `RF=3`, `min.insync.replicas=2`,
    `unclean.leader.election.enable=false`
  - Separate INTERNAL / EXTERNAL / CONTROLLER listeners so both containers and the
    host connect correctly
  - `auto.create.topics.enable=false` — topics are provisioned declaratively
  - PostgreSQL 16 with `wal_level=logical` (pre-enabled for Chapter 11 CDC)
  - Redis 7 with AOF persistence and `allkeys-lru` eviction
  - Prometheus, Grafana, Jaeger, Kafka UI and kafka-exporter
  - Health checks with dependency ordering on every service
- **Declarative topic provisioning** (`tools/`)
  - `topics.config.ts` — desired cluster state in version control; `eventDomain()`
    expands one domain into main topic, four retry tiers and a DLQ
  - `bootstrap-topics.ts` — idempotent reconciler with `--dry-run` and `--prune`;
    increases partitions but never decreases, reports unmanaged topics but never deletes
  - `verify-cluster.ts` — asserts broker count, controller election, partition counts,
    replication factor, `min.insync.replicas`, ISR health and leadership balance
- **Kafka topology documentation** (`docs/hld/02-kafka-topology.md`)
  - Listener model and why bootstrap-succeeds-but-produce-times-out happens
  - Partition sizing arithmetic and why 12 rather than 30
  - ISR mechanics, the RF/minISR/acks interaction, and a failure-scenario table
  - Six interview questions with answers
- **Alerting rules** (`infra/prometheus/rules/kafka-alerts.yml`) — under-replicated
  partitions, broker down, consumer lag, stalled consumers, DLQ growth, retry saturation;
  every alert carries a runbook link
- **Grafana provisioning** — datasources and dashboard provider as code
- **Test infrastructure** — Vitest with separate `unit` and `integration` projects
- `tools/topics.config.test.ts` — 17 tests asserting durability invariants, naming
  convention, and that every `events.*` topic has matching retry tiers and a DLQ
- `Makefile` with a self-documenting `help` target
- `.env.example` covering every service

### Changed

- Root `package.json` — topic and cluster scripts delegate to the `@platform/tools` workspace

[0.2.0]: https://github.com/aakashporwal1602/kafka-event-platform/compare/v0.1.0...v0.2.0

---

## [0.1.0] — 2026-08-02

**Chapter 0 — Architecture & Design Decisions**

### Added

- **Repository scaffold**
  - pnpm workspace configuration (`apps/*`, `packages/*`, `tools`)
  - Strict TypeScript base config with `noUncheckedIndexedAccess`, `exactOptionalPropertyTypes`
    and project-reference support
  - ESLint 9 flat config with type-aware rules, including `no-floating-promises` and
    `no-misused-promises` (async correctness in Kafka consumers)
  - Prettier, EditorConfig, `.nvmrc` (Node 22)
  - `pnpm verify` composite gate: format → lint → typecheck → test
- **High-level design** (`docs/hld/01-system-architecture.md`)
  - Problem statement and the six failure modes the platform addresses
  - 12 functional and 8 non-functional requirements, plus explicit non-goals
  - Capacity model with partition-sizing arithmetic for the 10K events/sec baseline
  - System architecture, publish path, consume path and failure path diagrams
  - Component responsibility matrix and deployment topology
- **Architecture Decision Records** (`docs/adr/`)
  - ADR-0001 — Kafka in KRaft mode, no ZooKeeper
  - ADR-0002 — Fastify over Express
  - ADR-0003 — pnpm monorepo with TypeScript project references
  - ADR-0004 — KafkaJS over node-rdkafka
  - ADR-0005 — Non-blocking retries via tiered retry topics
  - ADR-0006 — Avro with a schema registry
  - ADR-0007 — Transactional outbox for atomic state-and-event writes
  - ADR-0008 — At-least-once delivery with consumer-side idempotency
  - ADR template and index
- **Project tracking**
  - `docs/ROADMAP.md` — 18-chapter roadmap, dependency graph, requirement-coverage matrix
    and per-chapter definition of done
  - This changelog

### Notes

- No runtime code in this chapter by design. Chapter 0 establishes the decisions that every
  subsequent chapter references.

[Unreleased]: https://github.com/aakashporwal1602/kafka-event-platform/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/aakashporwal1602/kafka-event-platform/releases/tag/v0.1.0

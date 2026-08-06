# ADR-0010: Repository pattern over an ORM

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

The platform stores metadata in PostgreSQL: schemas, retry state, DLQ records,
replay jobs, audit trails and the transactional outbox. The access patterns are
narrow but unusual for a typical CRUD application:

- **Conditional inserts and updates** used as concurrency control —
  `INSERT ... ON CONFLICT DO NOTHING`, `UPDATE ... WHERE version = ?`. These are
  correctness mechanisms (ADR-0008), not optimisations.
- **`SELECT ... FOR UPDATE SKIP LOCKED`** for the outbox relay, so N relay
  instances can drain the same table without contending on the same rows.
- **Explicit transaction boundaries** — the outbox write must be in the _same_
  transaction as the caller's state change (ADR-0007), which means the
  transaction must be a first-class, passable thing.
- **Batch inserts** of hundreds of rows in one statement.
- **Partitioned tables** for the append-heavy audit and producer logs.

Every one of those is a place where an ORM's abstraction is either in the way or
silently generates something different from what was intended.

## Decision

Hand-written SQL behind repository interfaces, using `pg` directly. Each
aggregate gets a repository interface owned by the domain; the implementation
lives in `@platform/persistence`.

## Rationale

- **The SQL is the design.** `FOR UPDATE SKIP LOCKED` is not a detail an
  abstraction should hide — it is the mechanism that makes the relay
  horizontally scalable, and a reviewer needs to see it.
- **No hidden N+1.** Every query in this codebase is a query somebody wrote. The
  most common ORM production incident is a lazy-loaded relation inside a loop,
  and it is invisible in the source.
- **Predictable performance.** The generated plan is the plan we wrote. No
  surprise `LEFT JOIN LATERAL`, no unexpected `IN (...)` with 10,000 elements.
- **Repositories still give the seam.** The domain depends on
  `interface OutboxRepository`, not on `pg` — so DIP holds and tests can
  substitute an in-memory implementation. The ORM was never what provided that.
- **Migrations stay SQL.** Reviewing `ALTER TABLE ... ` is meaningful; reviewing
  a generated migration written in a DSL is not.

## Alternatives considered

| Option                            | Why rejected                                                                                                                                                                                                                                                                                   |
| --------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Prisma**                        | Excellent DX and type safety, but it owns the schema, its migration engine is opaque, and raw SQL escapes (`$queryRaw`) lose the type safety that was the reason to adopt it. `SKIP LOCKED` and partitioned tables both need escapes                                                           |
| **TypeORM**                       | Mature but heavy; entity decorators need `reflect-metadata` (see ADR-0009); its transaction handling via `EntityManager` makes the outbox pattern awkward to express                                                                                                                           |
| **Drizzle**                       | Genuinely close. Type-safe, thin, SQL-shaped, and would have been a reasonable choice. Rejected because it still interposes a query builder between author and plan, and this project's value is in the SQL being legible — a reviewer should read the query, not the builder that produced it |
| **Knex**                          | A query builder, not an ORM, so most objections do not apply. Rejected for the same legibility reason, plus weak TypeScript inference on results                                                                                                                                               |
| **Raw `pg` with no repositories** | Then the domain imports `pg` and DIP is gone; testing needs a real database for everything                                                                                                                                                                                                     |

## Consequences

**Positive**

- Every query is visible, reviewable and explainable.
- Concurrency primitives (`SKIP LOCKED`, conditional update, advisory lock) are
  used directly rather than approximated.
- Transactions are explicit objects, which is what the outbox pattern requires.
- Zero ORM version-upgrade risk.

**Negative / accepted costs**

- **Row-to-object mapping is written by hand.** Repetitive, and a mistyped column
  name is a runtime error rather than a compile error. Mitigated by keeping
  mappers next to their queries and covering each repository with an integration
  test against a real database.
- **No compile-time guarantee that a query matches the schema.** A dropped column
  breaks at runtime. Mitigated by integration tests running against migrated
  schemas in CI — which is where a schema mismatch should be caught anyway.
- **More code than Prisma for simple CRUD.** Accepted: the simple cases are a
  minority here, and the complex ones dominate the design.
- Refactoring a column name means editing several strings. A reviewer should
  expect that and check for missed call sites.

## Revisit when

The metadata schema grows past roughly 25 tables with mostly-simple CRUD access,
at which point hand-written mapping becomes the dominant cost rather than a
minor one.

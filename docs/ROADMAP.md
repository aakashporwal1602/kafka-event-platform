# Roadmap & Progress Tracker

> Updated at the end of every chapter. Progress is measured in completed chapters, not lines of code.

**Current version:** `v0.2.0` · **Progress:** 2 / 18 chapters (11.1%) · **Current phase:** Foundations

```
Part I   Foundations        ██████████░░░░░░░░░░  2/4
Part II  Publish path       ░░░░░░░░░░░░░░░░░░░░  0/3
Part III Consume path       ░░░░░░░░░░░░░░░░░░░░  0/2
Part IV  Failure handling   ░░░░░░░░░░░░░░░░░░░░  0/2
Part V   Advanced patterns  ░░░░░░░░░░░░░░░░░░░░  0/4
Part VI  Production         ░░░░░░░░░░░░░░░░░░░░  0/3
─────────────────────────────────────────────────────
Overall                     ██░░░░░░░░░░░░░░░░░░  2/18  (11.1%)
```

---

## Chapters

| #     | Chapter                             | Version   | Status          | Phase    | Commits |
| ----- | ----------------------------------- | --------- | --------------- | -------- | ------- |
| **0** | **Architecture & Design Decisions** | `v0.1.0`  | ✅ **Complete** | —        | 4       |
| **1** | **Infrastructure Foundation**       | `v0.2.0`  | ✅ **Complete** | Phase 1a | 3       |
| 2     | Shared Core Library                 | `v0.3.0`  | ⬜ Not started  | —        | —       |
| 3     | Data Layer (Postgres + Redis)       | `v0.4.0`  | ⬜ Not started  | —        | —       |
| 4     | Producer Service                    | `v0.5.0`  | ⬜ Not started  | Phase 1b | —       |
| 5     | Gateway Service & Public APIs       | `v0.6.0`  | ⬜ Not started  | Phase 2  | —       |
| 6     | Schema Registry                     | `v0.7.0`  | ⬜ Not started  | Phase 3  | —       |
| 7     | Consumer Runtime                    | `v0.8.0`  | ⬜ Not started  | —        | —       |
| 8     | Idempotency & Exactly-Once          | `v0.9.0`  | ⬜ Not started  | Phase 6  | —       |
| 9     | Retry Engine & DLQ                  | `v0.10.0` | ⬜ Not started  | Phase 4  | —       |
| 10    | Replay Service                      | `v0.11.0` | ⬜ Not started  | Phase 5  | —       |
| 11    | Outbox, CDC & Saga                  | `v0.12.0` | ⬜ Not started  | —        | —       |
| 12    | Observability                       | `v0.13.0` | ⬜ Not started  | Phase 7  | —       |
| 13    | Security & Multi-Tenancy            | `v0.14.0` | ⬜ Not started  | Phase 8  | —       |
| 14    | Advanced Delivery Features          | `v0.15.0` | ⬜ Not started  | —        | —       |
| 15    | Testing Strategy                    | `v0.16.0` | ⬜ Not started  | —        | —       |
| 16    | Kubernetes & Production Ops         | `v0.17.0` | ⬜ Not started  | —        | —       |
| 17    | Documentation, Portfolio & Bonus    | `v1.0.0`  | ⬜ Not started  | —        | —       |

**Legend:** ✅ Complete · 🟨 In progress · ⬜ Not started

---

## Dependency graph

```
0 ──► 1 ──► 2 ──► 3 ──► 4 ──► 5 ──► 6 ──► 7 ──► 8 ──► 9 ──► 10
                                                       │
                          ┌────────────────────────────┘
                          ▼
                 11 · 12 · 13 · 14   (independent, any order)
                          │
                          ▼
                    15 ──► 16 ──► 17
```

Chapters 0–3 are foundational — everything depends on them. After Chapter 10 the advanced chapters
can be built in any order.

---

## Requirement coverage

Tracks the requirements in [the HLD](./hld/01-system-architecture.md#2-requirements).

| Requirement                             | Delivered by  | Status |
| --------------------------------------- | ------------- | ------ |
| F-1 Publish single/batch                | Ch 4, 5       | ⬜     |
| F-2 Schema registration & evolution     | Ch 6          | ⬜     |
| F-3 Schema validation on publish        | Ch 4, 6       | ⬜     |
| F-4 At-least-once + exactly-once effect | Ch 7, 8       | ⬜     |
| F-5 Retry with backoff                  | Ch 9          | ⬜     |
| F-6 Dead-letter queue                   | Ch 9          | ⬜     |
| F-7 DLQ inspection & redrive            | Ch 9          | ⬜     |
| F-8 Replay by offset/time/partition     | Ch 10         | ⬜     |
| F-9 Metrics & lag                       | Ch 12         | ⬜     |
| F-10 AuthN / AuthZ / multi-tenancy      | Ch 13         | ⬜     |
| F-11 Delayed / scheduled / priority     | Ch 14         | ⬜     |
| F-12 Webhook delivery                   | Ch 14         | ⬜     |
| N-1..N-8 Non-functional targets         | Ch 12, 15, 16 | ⬜     |

---

## Definition of done (per chapter)

A chapter is complete only when **all** of these hold:

- [ ] `pnpm typecheck` passes with zero errors
- [ ] `pnpm lint` passes with zero warnings
- [ ] `pnpm format:check` passes
- [ ] `pnpm test` passes — unit **and** integration tests for the new code
- [ ] New code has meaningful test coverage (not coverage theatre)
- [ ] Affected documentation updated (HLD, LLD, API docs, runbooks)
- [ ] `CHANGELOG.md` updated under a new version heading
- [ ] This roadmap updated
- [ ] One or more Conventional Commits, each a logical milestone
- [ ] Chapter branch merged to `main` via PR with CI green
- [ ] Version tag pushed

Shortcut: `pnpm verify` runs the first four. Branch and PR conventions are in
[CONTRIBUTING.md](../CONTRIBUTING.md).

---

## Version history

| Version  | Chapter | Milestone                                                        |
| -------- | ------- | ---------------------------------------------------------------- |
| `v0.1.0` | 0       | Project initialisation, architecture and ADRs                    |
| `v0.2.0` | 1       | 3-broker KRaft cluster, declarative topics, cluster verification |

Target: **`v1.0.0`** at Chapter 17 — production-ready release.

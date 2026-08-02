# Contributing

This repository is built chapter by chapter, and it is maintained as though a
team depended on it. That discipline is the point — the git history is part of
the deliverable.

---

## Branch strategy

`main` is always green and always deployable. Nothing is committed to it directly.

Every chapter is a branch:

```
feat/ch02-core-library
feat/ch04-producer-service
feat/ch09-retry-engine
```

Format: `<type>/ch<NN>-<short-slug>`

For work outside the chapter sequence:

```
fix/consumer-offset-commit-race
docs/adr-0009-partition-strategy
chore/bump-kafkajs
```

---

## Workflow

```bash
# 1. Branch from an up-to-date main
git checkout main && git pull
git checkout -b feat/ch02-core-library

# 2. Work in logical commits — one milestone each, never one giant commit
git commit -m "feat(core): add dependency injection container"
git commit -m "feat(core): add Result type and error taxonomy"
git commit -m "test(core): add container and Result unit tests"

# 3. Verify before pushing
pnpm verify

# 4. Push and open a PR
git push -u origin feat/ch02-core-library
gh pr create --fill

# 5. After CI is green, merge preserving individual commits
gh pr merge --merge --delete-branch

# 6. Tag the chapter release on main
git checkout main && git pull
git tag -a v0.3.0 -m "v0.3.0 — Chapter 2: Shared Core Library"
git push origin --tags
```

---

## Commits

[Conventional Commits](https://www.conventionalcommits.org/). The PR title is
linted by CI because it becomes the merge commit subject.

```
<type>(<scope>): <subject in lowercase, imperative>

<body: what changed and WHY — the reasoning, not a restatement of the diff>

Refs: ADR-0005
```

| Type       | Use for                            |
| ---------- | ---------------------------------- |
| `feat`     | New capability                     |
| `fix`      | Bug fix                            |
| `docs`     | Documentation only                 |
| `refactor` | Behaviour-preserving restructure   |
| `perf`     | Performance improvement            |
| `test`     | Tests only                         |
| `build`    | Build system, dependencies, Docker |
| `ci`       | CI configuration                   |
| `chore`    | Everything else                    |

**Commits are never squashed.** A chapter with four logical milestones should
land as four commits. Squashing destroys the reasoning trail, which is exactly
what this repository is trying to preserve.

The body matters more than the subject. `feat(retry): add backoff` says nothing;
explain _why_ jitter is non-optional and what breaks without it.

---

## Definition of done

A chapter is not complete until every one of these holds:

- [ ] `pnpm format:check` passes
- [ ] `pnpm lint` passes with zero warnings
- [ ] `pnpm typecheck` passes
- [ ] `pnpm test` passes, and new code has meaningful tests
- [ ] Documentation updated — HLD, LLD, ADRs, runbooks, API as applicable
- [ ] `CHANGELOG.md` has a new version entry (Keep a Changelog format)
- [ ] `docs/ROADMAP.md` progress updated
- [ ] CI green on the PR

`pnpm verify` covers the first four.

---

## Architecture decisions

Any decision that is expensive to reverse gets an ADR:

```bash
cp docs/adr/TEMPLATE.md docs/adr/00NN-short-title.md
```

An ADR must state the **alternatives rejected** and the **costs accepted**. One
that only lists benefits is marketing, not a decision record. Keep it to a page —
longer usually means it is two decisions.

ADRs are immutable once accepted. A decision that changes gets a new ADR that
supersedes the old one, so the history of reasoning stays readable.

---

## Code conventions

- **TypeScript strict**, including `noUncheckedIndexedAccess` and
  `exactOptionalPropertyTypes`. No `any`, no `@ts-ignore` without a comment
  explaining why.
- **Errors are values.** Use `Result<T, E>` for expected failures; reserve
  exceptions for genuinely exceptional conditions.
- **Dependencies are injected**, never constructed inside business logic —
  including `Clock` and random sources, so behaviour is deterministic in tests.
- **`packages/*` must never import from `apps/*`.** Enforced by TypeScript
  project references.
- **No floating promises.** ESLint errors on them, because a dangling promise in
  a Kafka consumer silently drops messages.

---

## Tests

- **Unit** — pure logic, no I/O, milliseconds. Runs on every save.
- **Integration** — Testcontainers against real Kafka, Postgres and Redis.
- **Contract** — schema compatibility across versions.
- **Chaos** — broker kill, network partition, slow consumer.
- **Load** — k6, sustained throughput and latency percentiles.

Coverage is not a target. A handful of tests asserting real invariants beats a
hundred asserting getters.

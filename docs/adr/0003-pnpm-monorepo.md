# ADR-0003: Single pnpm monorepo with TypeScript project references

- **Status:** Accepted
- **Date:** 2026-08-02
- **Deciders:** Platform Engineering

## Context

The platform is nine services plus five shared libraries. Those libraries — the DI container, the
Kafka client wrapper, the observability layer, the persistence layer and the event contracts — are
consumed by nearly every service, and they change constantly during development.

Two structural options: one repository containing everything, or separate repositories with the
shared code published as versioned packages.

There is also a portfolio constraint that a real company would not have: **a reviewer must be able to
clone one thing and understand the system.** Nine repositories is nine tabs and no coherent story.

## Decision

A single **pnpm workspace** monorepo. Shared code lives in `packages/*`, deployable services in
`apps/*`. TypeScript **project references** enforce the dependency graph and enable incremental builds.

## Rationale

- **Atomic cross-cutting changes.** Adding a field to an event contract and updating the six consumers
  that read it is one commit and one review, not seven PRs coordinated across repositories.
- **No version-skew hell.** In a polyrepo, `@platform/core@1.4.2` in one service and `1.3.0` in another
  is normal, and debugging behaviour differences across versions is miserable. Here there is one version:
  `workspace:*`.
- **pnpm's strict `node_modules`.** Unlike npm/yarn's hoisted layout, pnpm creates a symlinked store
  where a package can only import what it explicitly declares. This catches phantom dependencies at
  development time rather than in a production container.
- **Disk and install speed.** Content-addressable store means one copy of each package version on disk
  regardless of how many workspace packages depend on it.
- **Project references give real incremental builds.** `tsc -b` rebuilds only what changed downstream,
  and enforces that `packages/core` cannot import from `apps/gateway-service`.

## Alternatives considered

| Option                            | Why rejected                                                                                                                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Polyrepo + published packages** | Realistic at large scale, but coordination cost dominates at this size, and it makes the project unreviewable as a portfolio artefact                                                                         |
| **npm workspaces**                | Works, but hoisted `node_modules` permits phantom dependencies and installs are slower                                                                                                                        |
| **Yarn Berry (PnP)**              | Strict and fast, but PnP still causes tooling friction with some Node-native modules                                                                                                                          |
| **Nx / Turborepo**                | Genuinely useful task graphs and caching, but they add a build-orchestration layer whose value only appears at much larger scale. `tsc -b` plus pnpm scripts covers our needs, and the config stays readable. |

## Consequences

**Positive**

- One clone, one `pnpm install`, one `pnpm verify`.
- Refactors that span services are safe and reviewable.
- The dependency graph is explicit and machine-enforced.

**Negative / accepted costs**

- CI runs more than strictly necessary unless we add affected-package detection (deferred; revisit if
  CI exceeds ~10 minutes).
- Repository grows large over time; `git clone --filter=blob:none` mitigates.
- Requires discipline: `packages/*` must never import from `apps/*`. Enforced by project references
  and an ESLint import rule.

**Neutral**

- Each service still builds an independent Docker image; the monorepo is a development-time
  organisation, not a deployment coupling.

## Revisit when

The team exceeds roughly 30 engineers with independent release cadences per service, or CI time
becomes the dominant bottleneck.

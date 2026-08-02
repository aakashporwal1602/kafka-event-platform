<!--
PR title must follow Conventional Commits — it becomes the merge commit subject.
  feat(producer): implement batching and partition strategy
  docs(hld): add consumer group design
-->

## What

<!-- One or two sentences. What does this PR deliver? -->

## Why

<!-- The problem being solved, or the chapter/requirement this satisfies.
     Link the ADR if a significant decision was made or applied. -->

Refs: <!-- ADR-000N · HLD §N · Chapter N -->

## Design notes

<!-- The decisions a reviewer should focus on: what you chose, what you
     rejected, and the cost you accepted. Skip if genuinely trivial. -->

## Definition of done

- [ ] `pnpm format:check` passes
- [ ] `pnpm lint` passes with zero warnings
- [ ] `pnpm typecheck` passes
- [ ] `pnpm test` passes — new code has meaningful tests
- [ ] Documentation updated (HLD / LLD / ADR / runbooks / API)
- [ ] `CHANGELOG.md` updated under a new version heading
- [ ] `docs/ROADMAP.md` progress updated
- [ ] Commits are logical milestones, Conventional Commits format

## Verification

<!-- How you proved it works. Commands run, output, screenshots. -->

```

```

## Risk

<!-- What could this break? Anything a reviewer should look at extra hard?
     "None" is a valid answer for a docs-only change. -->

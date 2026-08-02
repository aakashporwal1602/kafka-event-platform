import { defineConfig } from 'vitest/config';

/**
 * Integration tests — real Postgres, real Redis, via Testcontainers.
 *
 * ── Why these exist as a separate suite ────────────────────────────────────
 * The unit suite runs in ~1s and runs on every save. These start containers
 * and take tens of seconds. Merging them would mean either running containers
 * on every keystroke or nobody running the unit tests at all, and both are
 * worse than maintaining two configs.
 *
 * ── Why real containers rather than mocks ──────────────────────────────────
 * Everything covered here is a property of the *database*, not of our code:
 *
 *   • `FOR UPDATE SKIP LOCKED` handing disjoint rows to concurrent readers
 *   • Transactional DDL rolling back a failed migration
 *   • `SET NX` genuinely excluding a second writer
 *   • Lua executing atomically against a concurrent client
 *   • SQLSTATE codes actually being what the translator claims
 *
 * A mock that returns what we expect proves the mock agrees with us. It cannot
 * fail when the assumption is wrong, which is the only time a test is useful.
 *
 * ── Why not `docker compose up` and point tests at it ──────────────────────
 * Shared state across runs, ports that collide with a developer's own stack,
 * and a cleanup step that is skipped whenever a test crashes. Testcontainers
 * gives each run its own database on a random port and tears it down even when
 * the process dies, which is what makes these safe to run in parallel in CI.
 */
export default defineConfig({
  test: {
    name: 'integration',
    globals: false,
    environment: 'node',
    include: ['tests/integration/**/*.integration.test.ts'],

    // Pulling an image on a cold CI runner is the long pole; the tests
    // themselves are fast. A 30s default would fail the first run on every
    // fresh machine and pass on every subsequent one — the worst kind of flake,
    // because it looks like a real bug exactly once.
    testTimeout: 60_000,
    hookTimeout: 180_000,

    // One file at a time. Each file starts its own containers, and running
    // four files in parallel means four Postgres instances competing for
    // memory on a 2-core GitHub runner — where the failure is an OOM kill that
    // surfaces as an unrelated connection error.
    fileParallelism: false,

    // Retries are deliberately zero. A flaky integration test is either a real
    // race in the code or a real race in the test, and `retry: 2` hides both.
    retry: 0,

    // Vitest cannot know a container is still shutting down. Without this,
    // teardown races the next file's startup on a busy machine.
    teardownTimeout: 30_000,
  },
});

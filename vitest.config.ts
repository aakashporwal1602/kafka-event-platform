import { defineConfig } from 'vitest/config';

/**
 * Unit tests only — pure logic, no I/O, milliseconds. Runs on every save.
 *
 * Integration tests live in tests/integration/ and are Testcontainers-backed
 * against real Kafka/Postgres/Redis. They are slow, so they get their own
 * config (vitest.integration.config.ts, added in Chapter 3 alongside the first
 * real repositories) and run on demand and in CI, not on save.
 */
export default defineConfig({
  test: {
    name: 'unit',
    globals: false, // explicit imports; no ambient magic
    environment: 'node',
    passWithNoTests: true,
    include: ['{apps,packages,tools}/**/*.test.ts'],
    exclude: ['**/node_modules/**', '**/dist/**', 'tests/integration/**'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      exclude: [
        '**/node_modules/**',
        '**/dist/**',
        '**/*.test.ts',
        '**/*.config.ts',
        // Thin CLI wrappers over the kafkajs admin client — meaningfully
        // covered by integration tests against a real broker, not by mocks.
        'tools/bootstrap-topics.ts',
        'tools/verify-cluster.ts',
      ],
    },
  },
});

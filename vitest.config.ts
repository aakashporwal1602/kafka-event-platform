import { defineConfig } from 'vitest/config';

/**
 * Two projects, separated because they have very different runtimes:
 *
 *   unit        — pure logic, no I/O, milliseconds. Runs on every save.
 *   integration — Testcontainers-backed against real Kafka/Postgres/Redis.
 *                 Slow, so it runs on demand and in CI, not on save.
 */
export default defineConfig({
  test: {
    globals: false, // explicit imports; no ambient magic
    passWithNoTests: true,
    projects: [
      {
        test: {
          name: 'unit',
          include: ['{apps,packages,tools}/**/*.test.ts'],
          exclude: ['**/node_modules/**', '**/dist/**'],
          environment: 'node',
        },
      },
      {
        test: {
          name: 'integration',
          include: ['tests/integration/**/*.test.ts'],
          environment: 'node',
          testTimeout: 120_000, // container startup
          hookTimeout: 120_000,
        },
      },
    ],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'lcov'],
      exclude: [
        '**/node_modules/**',
        '**/dist/**',
        '**/*.test.ts',
        '**/*.config.ts',
        'tools/bootstrap-topics.ts', // thin CLI over kafkajs admin; covered by integration
        'tools/verify-cluster.ts',
      ],
    },
  },
});

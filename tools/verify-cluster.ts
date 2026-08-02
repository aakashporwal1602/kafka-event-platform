#!/usr/bin/env tsx
/**
 * Cluster health verification.
 *
 *   pnpm cluster:verify
 *
 * Asserts the durability guarantees the platform depends on are actually in
 * force, rather than assumed. Run after `infra:up`, and in CI before the
 * integration suite.
 *
 * Checks:
 *   1. All three brokers are reachable and a controller is elected
 *   2. Every managed topic exists with the configured partition count
 *   3. Replication factor is 3 everywhere
 *   4. min.insync.replicas is 2 everywhere
 *   5. No under-replicated partitions (ISR == replicas)
 *   6. Partition leadership is spread across brokers, not concentrated
 */

import { Kafka, type Admin } from 'kafkajs';
import { TOPICS } from './topics.config.js';

const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:19092,localhost:19093,localhost:19094')
  .split(',')
  .map((b) => b.trim());

const c = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
} as const;

let failures = 0;
let warnings = 0;

function pass(msg: string): void {
  console.log(`${c.green}  ✓${c.reset} ${msg}`);
}
function fail(msg: string): void {
  console.log(`${c.red}  ✗${c.reset} ${msg}`);
  failures++;
}
function warn(msg: string): void {
  console.log(`${c.yellow}  !${c.reset} ${msg}`);
  warnings++;
}

async function checkCluster(admin: Admin): Promise<void> {
  console.log(`\n${c.bold}Cluster${c.reset}`);
  const info = await admin.describeCluster();

  if (info.brokers.length === 3) {
    pass(
      `3 brokers online — ${info.brokers.map((b) => `${b.nodeId}@${b.host}:${b.port}`).join(', ')}`,
    );
  } else {
    fail(`expected 3 brokers, found ${info.brokers.length}`);
  }

  if (info.controller !== null && info.controller !== undefined) {
    pass(`controller elected — node ${info.controller}`);
  } else {
    fail('no controller elected — KRaft quorum may be unhealthy');
  }
}

async function checkTopics(admin: Admin): Promise<void> {
  console.log(`\n${c.bold}Topics${c.reset}`);
  const existing = new Set(await admin.listTopics());

  const missing = TOPICS.filter((t) => !existing.has(t.name));
  if (missing.length) {
    fail(`${missing.length} topic(s) missing — run \`pnpm topics:bootstrap\``);
    for (const t of missing) console.log(`${c.dim}      ${t.name}${c.reset}`);
    return;
  }
  pass(`all ${TOPICS.length} managed topics present`);

  const metadata = await admin.fetchTopicMetadata({ topics: TOPICS.map((t) => t.name) });
  const leaderCount = new Map<number, number>();

  for (const topic of metadata.topics) {
    const desired = TOPICS.find((t) => t.name === topic.name);
    if (!desired) continue;

    if (topic.partitions.length !== desired.partitions) {
      fail(`${topic.name}: ${topic.partitions.length} partitions, expected ${desired.partitions}`);
    }

    for (const p of topic.partitions) {
      if (p.replicas.length !== desired.replicationFactor) {
        fail(
          `${topic.name}[${p.partitionId}]: RF=${p.replicas.length}, expected ${desired.replicationFactor}`,
        );
      }
      // Under-replicated partitions are the single most important Kafka health
      // signal: durability guarantees silently weaken before anything fails.
      if (p.isr.length < p.replicas.length) {
        fail(
          `${topic.name}[${p.partitionId}]: UNDER-REPLICATED — ISR ${p.isr.length}/${p.replicas.length}`,
        );
      }
      leaderCount.set(p.leader, (leaderCount.get(p.leader) ?? 0) + 1);
    }
  }
  if (failures === 0) pass('all partitions fully replicated (ISR == replicas)');

  const counts = [...leaderCount.entries()].sort(([a], [b]) => a - b);
  const total = counts.reduce((sum, [, n]) => sum + n, 0);
  const ideal = total / Math.max(counts.length, 1);
  const skew = Math.max(...counts.map(([, n]) => Math.abs(n - ideal))) / ideal;

  const distribution = counts.map(([node, n]) => `broker ${node}: ${n}`).join(', ');
  if (skew > 0.3) {
    warn(`leadership skew ${(skew * 100).toFixed(0)}% — ${distribution}`);
    console.log(
      `${c.dim}      consider kafka-leader-election.sh --election-type PREFERRED${c.reset}`,
    );
  } else {
    pass(`leadership balanced across brokers — ${distribution}`);
  }
}

async function checkDurabilityConfig(admin: Admin): Promise<void> {
  console.log(`\n${c.bold}Durability configuration${c.reset}`);
  const result = await admin.describeConfigs({
    includeSynonyms: false,
    resources: TOPICS.map((t) => ({ type: 2 /* TOPIC */, name: t.name })),
  });

  let violations = 0;
  for (const resource of result.resources) {
    const entry = resource.configEntries.find((e) => e.configName === 'min.insync.replicas');
    if (entry?.configValue !== '2') {
      fail(
        `${resource.resourceName}: min.insync.replicas=${entry?.configValue ?? 'unset'}, expected 2`,
      );
      violations++;
    }
  }
  if (violations === 0) {
    pass('min.insync.replicas=2 on all topics');
    console.log(
      `${c.dim}      RF=3 + minISR=2 + acks=all → survives 1 broker loss with no data loss${c.reset}`,
    );
  }
}

async function main(): Promise<void> {
  console.log(`\n${c.bold}Kafka cluster verification${c.reset}`);
  console.log(`${c.dim}brokers: ${BROKERS.join(', ')}${c.reset}`);

  const kafka = new Kafka({ clientId: 'cluster-verify', brokers: BROKERS });
  const admin = kafka.admin();

  try {
    await admin.connect();
    await checkCluster(admin);
    await checkTopics(admin);
    await checkDurabilityConfig(admin);
  } finally {
    await admin.disconnect();
  }

  console.log('');
  if (failures > 0) {
    console.log(
      `${c.red}${c.bold}FAILED${c.reset} — ${failures} error(s), ${warnings} warning(s)\n`,
    );
    process.exitCode = 1;
  } else if (warnings > 0) {
    console.log(`${c.yellow}${c.bold}PASSED WITH WARNINGS${c.reset} — ${warnings} warning(s)\n`);
  } else {
    console.log(`${c.green}${c.bold}HEALTHY${c.reset} — all checks passed\n`);
  }
}

main().catch((error: unknown) => {
  console.error(`${c.red}\nVerification failed to run:${c.reset}`, error);
  process.exitCode = 1;
});

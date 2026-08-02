#!/usr/bin/env tsx
/**
 * Declarative topic provisioner.
 *
 *   pnpm topics:bootstrap            apply the desired state
 *   pnpm topics:bootstrap --dry-run  show the plan without applying
 *   pnpm topics:bootstrap --prune    also report topics not in config
 *
 * Reconciles the live cluster against tools/topics.config.ts. Idempotent —
 * running it twice is a no-op, so it is safe in CI and in a deploy pipeline.
 *
 * Deliberate limitations, because the alternative is silent data loss:
 *   • Partition count is only ever INCREASED, never decreased (Kafka cannot
 *     reduce partitions, and increasing changes key→partition mapping, so it
 *     is warned about loudly).
 *   • Topics absent from config are REPORTED, never deleted. Deletion is a
 *     manual, deliberate act.
 */

import { Kafka, type Admin, type ITopicConfig } from 'kafkajs';
import { TOPICS, type TopicDefinition } from './topics.config.js';

// Bracket access is required: `process.env` is an index signature, and
// noPropertyAccessFromIndexSignature makes dot access an error. The rule exists
// so a typo like process.env.KAKFA_BROKERS fails at compile time instead of
// silently resolving to undefined at runtime.
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:19092,localhost:19093,localhost:19094')
  .split(',')
  .map((b) => b.trim());

const DRY_RUN = process.argv.includes('--dry-run');
const PRUNE = process.argv.includes('--prune');

const c = {
  reset: '\x1b[0m',
  dim: '\x1b[2m',
  bold: '\x1b[1m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  red: '\x1b[31m',
  cyan: '\x1b[36m',
} as const;

interface Plan {
  readonly toCreate: TopicDefinition[];
  readonly toExpand: { topic: TopicDefinition; from: number; to: number }[];
  readonly toReconfigure: { topic: TopicDefinition; changes: [string, string, string][] }[];
  readonly unmanaged: string[];
  readonly unchanged: string[];
}

async function buildPlan(admin: Admin): Promise<Plan> {
  const existingNames = new Set(await admin.listTopics());
  const managedNames = new Set(TOPICS.map((t) => t.name));

  const toCreate: TopicDefinition[] = [];
  const toExpand: Plan['toExpand'] = [];
  const toReconfigure: Plan['toReconfigure'] = [];
  const unchanged: string[] = [];

  const present = TOPICS.filter((t) => existingNames.has(t.name));
  const metadata = present.length
    ? await admin.fetchTopicMetadata({ topics: present.map((t) => t.name) })
    : { topics: [] };
  const partitionCounts = new Map(metadata.topics.map((t) => [t.name, t.partitions.length]));

  const liveConfigs = present.length
    ? await admin.describeConfigs({
        resources: present.map((t) => ({ type: 2 /* TOPIC */, name: t.name })),
        includeSynonyms: false,
      })
    : { resources: [] };
  const configByTopic = new Map(
    liveConfigs.resources.map((r) => [
      r.resourceName,
      new Map(r.configEntries.map((e) => [e.configName, e.configValue])),
    ]),
  );

  for (const topic of TOPICS) {
    if (!existingNames.has(topic.name)) {
      toCreate.push(topic);
      continue;
    }

    let dirty = false;

    const livePartitions = partitionCounts.get(topic.name) ?? 0;
    if (livePartitions < topic.partitions) {
      toExpand.push({ topic, from: livePartitions, to: topic.partitions });
      dirty = true;
    } else if (livePartitions > topic.partitions) {
      console.warn(
        `${c.yellow}  ! ${topic.name} has ${livePartitions} partitions, config wants ${topic.partitions}. ` +
          `Kafka cannot reduce partitions — update the config or recreate the topic.${c.reset}`,
      );
    }

    const live = configByTopic.get(topic.name);
    const changes: [string, string, string][] = [];
    for (const [key, desired] of Object.entries(topic.config)) {
      if (desired === undefined) continue;
      const actual = live?.get(key);
      if (actual !== undefined && actual !== desired) changes.push([key, actual, desired]);
    }
    if (changes.length) {
      toReconfigure.push({ topic, changes });
      dirty = true;
    }

    if (!dirty) unchanged.push(topic.name);
  }

  const unmanaged = [...existingNames]
    .filter((n) => !managedNames.has(n) && !n.startsWith('__')) // ignore internal topics
    .sort();

  return { toCreate, toExpand, toReconfigure, unmanaged, unchanged };
}

function printPlan(plan: Plan): void {
  console.log(`\n${c.bold}Plan${c.reset}`);
  console.log(`${c.dim}${'─'.repeat(72)}${c.reset}`);

  for (const t of plan.toCreate) {
    console.log(
      `${c.green}  + create ${c.bold}${t.name}${c.reset}` +
        `${c.dim}  ${t.partitions}p · RF=${t.replicationFactor} · ${t.description}${c.reset}`,
    );
  }
  for (const { topic, from, to } of plan.toExpand) {
    console.log(
      `${c.yellow}  ~ expand ${c.bold}${topic.name}${c.reset}${c.yellow} ${from} → ${to} partitions${c.reset}`,
    );
    console.log(
      `${c.dim}      warning: changes key→partition mapping; existing keys may land elsewhere${c.reset}`,
    );
  }
  for (const { topic, changes } of plan.toReconfigure) {
    console.log(`${c.cyan}  ~ config ${c.bold}${topic.name}${c.reset}`);
    for (const [key, from, to] of changes) {
      console.log(`${c.dim}      ${key}: ${from} → ${to}${c.reset}`);
    }
  }
  if (PRUNE && plan.unmanaged.length) {
    console.log(
      `\n${c.yellow}  Unmanaged topics (present on cluster, absent from config):${c.reset}`,
    );
    for (const name of plan.unmanaged) console.log(`${c.dim}      ${name}${c.reset}`);
    console.log(`${c.dim}      Not deleted. Remove manually if intentional.${c.reset}`);
  }
  if (plan.unchanged.length) {
    console.log(`${c.dim}  = ${plan.unchanged.length} topic(s) already correct${c.reset}`);
  }
  console.log(`${c.dim}${'─'.repeat(72)}${c.reset}`);
}

async function apply(admin: Admin, plan: Plan): Promise<void> {
  if (plan.toCreate.length) {
    const topics: ITopicConfig[] = plan.toCreate.map((t) => ({
      topic: t.name,
      numPartitions: t.partitions,
      replicationFactor: t.replicationFactor,
      configEntries: Object.entries(t.config)
        .filter((e): e is [string, string] => e[1] !== undefined)
        .map(([name, value]) => ({ name, value })),
    }));
    await admin.createTopics({ topics, waitForLeaders: true, timeout: 30_000 });
    console.log(`${c.green}  ✓ created ${plan.toCreate.length} topic(s)${c.reset}`);
  }

  if (plan.toExpand.length) {
    await admin.createPartitions({
      topicPartitions: plan.toExpand.map(({ topic, to }) => ({ topic: topic.name, count: to })),
      timeout: 30_000,
    });
    console.log(`${c.green}  ✓ expanded ${plan.toExpand.length} topic(s)${c.reset}`);
  }

  for (const { topic, changes } of plan.toReconfigure) {
    await admin.alterConfigs({
      validateOnly: false,
      resources: [
        {
          type: 2,
          name: topic.name,
          configEntries: changes.map(([name, , value]) => ({ name, value })),
        },
      ],
    });
  }
  if (plan.toReconfigure.length) {
    console.log(`${c.green}  ✓ reconfigured ${plan.toReconfigure.length} topic(s)${c.reset}`);
  }
}

async function main(): Promise<void> {
  console.log(`\n${c.bold}Kafka topic provisioning${c.reset}`);
  console.log(`${c.dim}brokers: ${BROKERS.join(', ')}${c.reset}`);
  console.log(`${c.dim}desired: ${TOPICS.length} topics${c.reset}`);
  if (DRY_RUN) console.log(`${c.yellow}DRY RUN — no changes will be applied${c.reset}`);

  const kafka = new Kafka({
    clientId: 'topic-bootstrap',
    brokers: BROKERS,
    retry: { initialRetryTime: 300, retries: 5 },
  });
  const admin = kafka.admin();

  try {
    await admin.connect();
    const plan = await buildPlan(admin);
    printPlan(plan);

    const changeCount = plan.toCreate.length + plan.toExpand.length + plan.toReconfigure.length;
    if (changeCount === 0) {
      console.log(`${c.green}\nCluster already matches desired state.${c.reset}\n`);
      return;
    }
    if (DRY_RUN) {
      console.log(`${c.yellow}\n${changeCount} change(s) would be applied.${c.reset}\n`);
      return;
    }

    console.log('');
    await apply(admin, plan);
    console.log(`${c.green}\nDone. ${changeCount} change(s) applied.${c.reset}\n`);
  } finally {
    await admin.disconnect();
  }
}

main().catch((error: unknown) => {
  console.error(`${c.red}\nProvisioning failed:${c.reset}`, error);
  process.exitCode = 1;
});

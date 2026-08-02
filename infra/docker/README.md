# Local Infrastructure

Everything the platform depends on, in one Docker Compose stack.

## Start

```bash
cp .env.example .env          # from the repository root, once
pnpm infra:up                 # ~60s for brokers to become healthy
pnpm topics:bootstrap         # provision topics declaratively
pnpm cluster:verify           # assert durability guarantees are in force
```

## Services

| Service | Host port | Purpose |
|---|---|---|
| kafka-1 / 2 / 3 | 19092 / 19093 / 19094 | 3-broker KRaft cluster |
| kafka-ui | 8080 | Topic browser, consumer groups, message viewer |
| postgres | 5432 | Platform metadata; `wal_level=logical` for CDC |
| redis | 6379 | Idempotency, locks, rate limits |
| prometheus | 9090 | Metrics |
| grafana | 3001 | Dashboards (`admin` / `admin`) |
| jaeger | 16686 | Distributed traces |
| kafka-exporter | 9308 | Kafka → Prometheus metrics |

## Connecting

**From your machine** — use the EXTERNAL listeners:

```
localhost:19092,localhost:19093,localhost:19094
```

**From inside a container** on `kep-network` — use the INTERNAL listeners:

```
kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Using the wrong one produces the classic symptom: bootstrap succeeds, produce times out.
See [docs/hld/02-kafka-topology.md §2](../../docs/hld/02-kafka-topology.md#2-listeners--the-thing-that-trips-everyone-up).

## Verify by hand

```bash
# Broker list and controller
docker exec kep-kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 | head -5

# KRaft quorum status
docker exec kep-kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status

# Topic detail — partitions, leaders, ISR
docker exec kep-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic events.orders

# Under-replicated partitions — should always be empty
docker exec kep-kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --under-replicated-partitions
```

## Test the durability guarantee

RF=3 + `min.insync.replicas=2` should survive one broker loss and reject writes on two.

```bash
# Kill one broker — writes continue
docker stop kep-kafka-3
pnpm cluster:verify          # reports under-replicated partitions, still writable

# Kill a second — writes now rejected with NOT_ENOUGH_REPLICAS
docker stop kep-kafka-2

# Recover
docker start kep-kafka-2 kep-kafka-3
```

Watch ISR shrink and recover in Kafka UI while this runs. This is the fastest way to build intuition
for what those three settings actually do together.

## Reset

```bash
pnpm infra:down     # stop, keep volumes
pnpm infra:nuke     # stop and DELETE all data — full clean slate
```

## Resource usage

Three JVM brokers at `-Xmx1G` plus the rest is roughly **6 GB RAM**. On a constrained machine, run a
single broker by commenting out `kafka-2`/`kafka-3` and setting the replication-factor variables to
`1` — but note the durability guarantees no longer apply, and `cluster:verify` will correctly fail.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Brokers restart-loop on first run | Stale volume with a different `CLUSTER_ID` | `pnpm infra:nuke` |
| `LEADER_NOT_AVAILABLE` right after start | Topics not provisioned yet | `pnpm topics:bootstrap` |
| Bootstrap OK, produce times out | Wrong listener for your network location | Use `localhost:1909N` from the host |
| `NOT_ENOUGH_REPLICAS` | Two or more brokers down | Restart brokers; check `docker ps` |
| Kafka UI shows no cluster | UI started before brokers were healthy | `docker restart kep-kafka-ui` |

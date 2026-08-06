-- ============================================================================
--  0001 — Initial platform schema
-- ----------------------------------------------------------------------------
--  Conventions, applied throughout and stated once here:
--
--  timestamptz, never timestamp
--      `timestamp` has no timezone and silently reinterprets values when the
--      server TZ changes. Every incident involving "the times are eight hours
--      off" traces back to this. timestamptz stores UTC and converts on read.
--
--  text + CHECK, not enum types
--      Postgres enums cannot drop a value and cannot reorder. Adding
--      'QUARANTINED' to a status is trivial with a CHECK constraint and an
--      ALTER; removing a mistake from an enum type requires recreating the type
--      and every column using it.
--
--  jsonb, never json
--      json stores the raw text and reparses on every access. jsonb is parsed
--      once, comparable, and indexable with GIN.
--
--  Every index is followed by the query it serves
--      An index nobody can name a query for is write amplification with no
--      read benefit — and it is never removed, because nobody can prove it is
--      unused.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Topics — runtime mirror of tools/topics.config.ts
--
-- The config file is the source of truth for provisioning; this table is what
-- the API and dashboard query. Keeping both is deliberate: provisioning must
-- work before the database exists, and the API must answer "what topics are
-- there" without talking to a broker on every request.
-- ---------------------------------------------------------------------------
CREATE TABLE topics (
    -- The topic name IS the identity. It is unique, immutable and meaningful,
    -- so a surrogate key would add a join for nothing.
    name                  text PRIMARY KEY,
    topic_class           text NOT NULL,
    domain                text NOT NULL,
    partitions            integer NOT NULL CHECK (partitions > 0),
    replication_factor    smallint NOT NULL CHECK (replication_factor > 0),
    min_insync_replicas   smallint NOT NULL CHECK (min_insync_replicas > 0),
    retention_ms          bigint NOT NULL,
    cleanup_policy        text NOT NULL DEFAULT 'delete',
    description           text NOT NULL,
    tenant_id             text,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT topics_class_valid
        CHECK (topic_class IN ('events', 'retry', 'dlq', 'platform')),
    CONSTRAINT topics_cleanup_valid
        CHECK (cleanup_policy IN ('delete', 'compact', 'compact,delete')),
    -- Encodes the durability guarantee from the HLD as a database constraint:
    -- min.insync.replicas must be satisfiable by the replica set, or the topic
    -- can never accept a write with acks=all.
    CONSTRAINT topics_isr_satisfiable
        CHECK (min_insync_replicas <= replication_factor)
);

-- "list every topic for this domain" — dashboard and the DLQ triage view.
CREATE INDEX topics_domain_idx ON topics (domain);
-- "list every topic for this tenant" — multi-tenancy (Chapter 13).
CREATE INDEX topics_tenant_idx ON topics (tenant_id) WHERE tenant_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Schema registry
--
-- Two tables because a subject and a version are different things with
-- different lifetimes: the subject carries the compatibility policy and
-- survives forever; versions accumulate under it.
-- ---------------------------------------------------------------------------
CREATE TABLE schema_subjects (
    subject               text PRIMARY KEY,
    compatibility         text NOT NULL DEFAULT 'BACKWARD',
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT schema_compatibility_valid CHECK (compatibility IN (
        'NONE', 'BACKWARD', 'BACKWARD_TRANSITIVE',
        'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE'
    ))
);

CREATE TABLE schema_versions (
    -- int, not bigint or uuid: this value goes into the 4-byte schema-ID field
    -- of the Confluent wire format (ADR-0006). It is physically constrained to
    -- int32, and using a wider type here would let us mint IDs we cannot encode.
    schema_id             integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    subject               text NOT NULL REFERENCES schema_subjects (subject) ON DELETE RESTRICT,
    version               integer NOT NULL CHECK (version > 0),
    schema_definition     jsonb NOT NULL,
    -- Canonical-form hash. Two textually different but semantically identical
    -- schemas (whitespace, field order) must resolve to the same ID, or every
    -- producer redeploy registers a "new" schema and the ID space grows without
    -- bound.
    fingerprint           text NOT NULL,
    registered_at         timestamptz NOT NULL DEFAULT now(),
    registered_by         text,

    CONSTRAINT schema_versions_unique UNIQUE (subject, version),
    -- The deduplication guarantee. Without it, re-registering an identical
    -- schema mints a new ID and consumers cache both.
    CONSTRAINT schema_fingerprint_unique UNIQUE (subject, fingerprint)
);

-- "give me the latest version of this subject" — the hot path on registration,
-- since compatibility is checked against the previous version. DESC so the
-- planner reads the first row rather than sorting.
CREATE INDEX schema_versions_latest_idx ON schema_versions (subject, version DESC);
-- ON DELETE RESTRICT above needs this to avoid a sequential scan on every
-- attempted subject delete.
CREATE INDEX schema_versions_subject_idx ON schema_versions (subject);

-- ---------------------------------------------------------------------------
-- Retry tracking
--
-- Kafka holds the messages (ADR-0005); this table holds the *state* — how many
-- attempts, which tier, last error. That is queryable in a way a Kafka topic is
-- not: "show me everything failing with SCHEMA_VALIDATION_FAILED in the last
-- hour" is a database question, not a topic scan.
-- ---------------------------------------------------------------------------
CREATE TABLE retry_events (
    event_id              uuid PRIMARY KEY,
    original_topic        text NOT NULL,
    current_tier          text NOT NULL,
    attempt               smallint NOT NULL DEFAULT 1 CHECK (attempt > 0),
    partition_key         text,
    error_code            text NOT NULL,
    error_message         text NOT NULL,
    first_failed_at       timestamptz NOT NULL DEFAULT now(),
    last_failed_at        timestamptz NOT NULL DEFAULT now(),
    next_attempt_at       timestamptz NOT NULL,
    correlation_id        text,
    tenant_id             text
);

-- "what is due for retry now" — the retry consumer's only query.
CREATE INDEX retry_events_due_idx ON retry_events (next_attempt_at);
-- "how much is sitting in the 1h tier for orders" — the saturation alert.
CREATE INDEX retry_events_tier_idx ON retry_events (original_topic, current_tier);
-- "what is failing, grouped by cause" — triage.
CREATE INDEX retry_events_error_idx ON retry_events (error_code, last_failed_at DESC);

-- ---------------------------------------------------------------------------
-- Dead letter queue
--
-- Retention here is 30 days in Kafka but this record is kept longer: the
-- payload can expire from the topic while the *fact* that it failed still
-- matters for a post-incident review.
-- ---------------------------------------------------------------------------
CREATE TABLE dlq_events (
    event_id              uuid PRIMARY KEY,
    original_topic        text NOT NULL,
    dlq_topic             text NOT NULL,
    -- Where the message physically sits, so redrive can seek to it exactly
    -- rather than scanning the DLQ topic.
    dlq_partition         integer NOT NULL,
    dlq_offset            bigint NOT NULL,
    partition_key         text,
    payload               bytea,
    headers               jsonb NOT NULL DEFAULT '{}'::jsonb,
    error_code            text NOT NULL,
    error_message         text NOT NULL,
    -- Full attempt history, not just the last error: a message that failed four
    -- times for four different reasons tells a completely different story from
    -- one that failed the same way four times.
    attempt_history       jsonb NOT NULL DEFAULT '[]'::jsonb,
    status                text NOT NULL DEFAULT 'QUARANTINED',
    quarantined_at        timestamptz NOT NULL DEFAULT now(),
    resolved_at           timestamptz,
    resolved_by           text,
    correlation_id        text,
    tenant_id             text,

    CONSTRAINT dlq_status_valid
        CHECK (status IN ('QUARANTINED', 'REDRIVEN', 'DISCARDED')),
    -- A resolved record must say when and by whom. Without this, "who
    -- discarded 4,000 events last Tuesday" is unanswerable.
    CONSTRAINT dlq_resolution_complete CHECK (
        (status = 'QUARANTINED' AND resolved_at IS NULL AND resolved_by IS NULL)
        OR (status <> 'QUARANTINED' AND resolved_at IS NOT NULL AND resolved_by IS NOT NULL)
    )
);

-- "show me everything still quarantined for this topic" — the inspection API's
-- default view. Partial index because resolved rows dominate over time and are
-- almost never listed.
CREATE INDEX dlq_open_idx ON dlq_events (original_topic, quarantined_at DESC)
    WHERE status = 'QUARANTINED';
-- "group the DLQ by failure cause" — triage.
CREATE INDEX dlq_error_idx ON dlq_events (error_code, quarantined_at DESC);
-- "find this specific event" — support answering a customer complaint.
CREATE INDEX dlq_correlation_idx ON dlq_events (correlation_id)
    WHERE correlation_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Replay jobs
--
-- Replay is long-running and resumable, so its state is durable rather than
-- in-memory: a relay restart mid-replay must not restart from the beginning
-- and re-emit everything already emitted.
-- ---------------------------------------------------------------------------
CREATE TABLE replay_jobs (
    job_id                uuid PRIMARY KEY,
    source_topic          text NOT NULL,
    target_topic          text,
    mode                  text NOT NULL,
    -- Selection criteria. Which are populated depends on `mode`; the CHECK
    -- below enforces that the right ones are present.
    from_offset           bigint,
    to_offset             bigint,
    from_timestamp        timestamptz,
    to_timestamp          timestamptz,
    partitions            integer[],
    key_filter            text,
    -- Resumability: the exact position reached per partition.
    checkpoint            jsonb NOT NULL DEFAULT '{}'::jsonb,
    events_replayed       bigint NOT NULL DEFAULT 0,
    events_total          bigint,
    -- Throttle so a backfill cannot starve live traffic.
    max_events_per_second integer,
    status                text NOT NULL DEFAULT 'PENDING',
    error_message         text,
    created_by            text NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),
    started_at            timestamptz,
    completed_at          timestamptz,

    CONSTRAINT replay_mode_valid
        CHECK (mode IN ('OFFSET', 'TIMESTAMP', 'PARTITION', 'FULL')),
    CONSTRAINT replay_status_valid
        CHECK (status IN ('PENDING', 'RUNNING', 'PAUSED', 'COMPLETED', 'FAILED', 'CANCELLED')),
    -- A job whose mode does not match its criteria would silently replay the
    -- wrong range. Enforced here rather than only in application code, because
    -- this table is also written by operators.
    CONSTRAINT replay_criteria_match CHECK (
        (mode = 'OFFSET' AND from_offset IS NOT NULL)
        OR (mode = 'TIMESTAMP' AND from_timestamp IS NOT NULL)
        OR (mode = 'PARTITION' AND partitions IS NOT NULL)
        OR (mode = 'FULL')
    )
);

-- "which jobs are runnable" — the replay service's poll. Partial, because
-- completed jobs accumulate and are never in this query's answer.
CREATE INDEX replay_active_idx ON replay_jobs (status, created_at)
    WHERE status IN ('PENDING', 'RUNNING', 'PAUSED');

-- ---------------------------------------------------------------------------
-- Consumer registry
--
-- Consumer groups are Kafka's state, not ours. This table holds what Kafka
-- does not: who owns the group, what its lag SLO is, whether it is expected to
-- be running. Alerting on "this group has lag" is noise without "and it is
-- supposed to be consuming".
-- ---------------------------------------------------------------------------
CREATE TABLE consumer_registry (
    group_id              text PRIMARY KEY,
    service_name          text NOT NULL,
    subscribed_topics     text[] NOT NULL,
    owner_team            text NOT NULL,
    max_lag_threshold     bigint NOT NULL DEFAULT 10000,
    expected_running      boolean NOT NULL DEFAULT true,
    last_seen_at          timestamptz,
    created_at            timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now()
);

-- "which groups subscribe to this topic" — impact analysis before a schema
-- change. GIN because subscribed_topics is an array.
CREATE INDEX consumer_topics_idx ON consumer_registry USING GIN (subscribed_topics);

-- ---------------------------------------------------------------------------
-- Transactional outbox (ADR-0007)
--
-- The single most performance-sensitive table here: every state change in every
-- service writes a row, and the relay reads and deletes them continuously.
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    -- bigint identity, not uuid. Two reasons, both load-bearing:
    --   1. Ordering. The relay must publish in the order events were written,
    --      and a monotonic sequence gives that for free. UUIDv4 gives none.
    --   2. Index locality. Sequential inserts append to the right edge of the
    --      B-tree; random UUIDs scatter writes across the whole index and cause
    --      page splits, which on a high-write table is a real throughput cost.
    id                    bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    -- Business identity, generated by the application. This is the idempotency
    -- key the consumer deduplicates on (ADR-0008), so it must be stable across
    -- a relay retry.
    event_id              uuid NOT NULL UNIQUE,
    aggregate_type        text NOT NULL,
    aggregate_id          text NOT NULL,
    event_type            text NOT NULL,
    topic                 text NOT NULL,
    -- Determines the Kafka partition, which determines ordering. Nullable
    -- because not every event needs an ordering guarantee.
    partition_key         text,
    payload               jsonb NOT NULL,
    headers               jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at            timestamptz NOT NULL DEFAULT now(),
    -- NULL means unpublished. Rows are marked rather than deleted immediately
    -- so a relay crash between publish and delete does not lose the audit
    -- trail; a separate cleanup job removes old published rows.
    published_at          timestamptz,
    -- Set once published, so a support query can find where an event landed.
    published_partition   integer,
    published_offset      bigint,
    attempts              smallint NOT NULL DEFAULT 0,
    last_error            text,
    correlation_id        text,
    tenant_id             text
);

-- THE query the relay runs, several times a second:
--   SELECT * FROM outbox WHERE published_at IS NULL
--   ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100
--
-- Partial index on unpublished rows only. This is the difference between an
-- index that stays small forever and one that grows with total event volume:
-- published rows are the overwhelming majority and are never in this result.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
-- "delete published rows older than N days" — the cleanup job. Without this,
-- forgetting cleanup fills the disk; without the index, cleanup itself is a
-- sequential scan over the whole table.
CREATE INDEX outbox_cleanup_idx ON outbox (published_at) WHERE published_at IS NOT NULL;
-- "what did we emit for this order" — support and debugging.
CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_type, aggregate_id, id);

-- ---------------------------------------------------------------------------
-- Audit log — every privileged platform operation
--
-- Partitioned by month. Append-only tables that are queried by recent time
-- range and pruned by age are the textbook case: dropping a partition is
-- instant, whereas DELETE on 90 days of rows is a long-running vacuum problem.
-- ---------------------------------------------------------------------------
CREATE TABLE audit_logs (
    id                    bigint GENERATED ALWAYS AS IDENTITY,
    occurred_at           timestamptz NOT NULL DEFAULT now(),
    actor                 text NOT NULL,
    actor_type            text NOT NULL DEFAULT 'USER',
    action                text NOT NULL,
    resource_type         text NOT NULL,
    resource_id           text NOT NULL,
    -- Before/after, so an audit entry answers "what changed" rather than only
    -- "something changed".
    before_state          jsonb,
    after_state           jsonb,
    source_ip             inet,
    correlation_id        text,
    tenant_id             text,
    outcome               text NOT NULL DEFAULT 'SUCCESS',

    -- The partition key must be part of the primary key. This is a Postgres
    -- requirement for partitioned tables, not a modelling choice.
    PRIMARY KEY (id, occurred_at),
    CONSTRAINT audit_actor_type_valid CHECK (actor_type IN ('USER', 'SERVICE', 'SYSTEM')),
    CONSTRAINT audit_outcome_valid CHECK (outcome IN ('SUCCESS', 'DENIED', 'FAILED'))
) PARTITION BY RANGE (occurred_at);

-- Initial partitions. A scheduled job creates future ones; a DEFAULT partition
-- catches anything that slips through so an insert can never fail outright —
-- losing an audit record because a partition was missing would be worse than
-- the row landing in the wrong place.
CREATE TABLE audit_logs_2026_08 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE audit_logs_2026_09 PARTITION OF audit_logs
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE audit_logs_default PARTITION OF audit_logs DEFAULT;

-- "what did this actor do" and "what happened to this resource" — the two
-- questions an audit log exists to answer.
CREATE INDEX audit_actor_idx ON audit_logs (actor, occurred_at DESC);
CREATE INDEX audit_resource_idx ON audit_logs (resource_type, resource_id, occurred_at DESC);

-- ---------------------------------------------------------------------------
-- Producer log — a sampled record of publishes
--
-- Deliberately NOT a row per event: at 10,000 events/sec that is 864M rows a
-- day, which is a data-warehouse problem, not a metadata-table problem. The
-- producer samples, and the authoritative per-event record is the Kafka topic
-- itself. This table exists for spot-checking and support lookups.
-- ---------------------------------------------------------------------------
CREATE TABLE producer_logs (
    id                    bigint GENERATED ALWAYS AS IDENTITY,
    occurred_at           timestamptz NOT NULL DEFAULT now(),
    event_id              uuid NOT NULL,
    topic                 text NOT NULL,
    partition             integer,
    "offset"              bigint,
    schema_id             integer,
    payload_bytes         integer NOT NULL,
    latency_ms            numeric(10, 3),
    outcome               text NOT NULL,
    error_code            text,
    producer_service      text NOT NULL,
    correlation_id        text,
    tenant_id             text,

    PRIMARY KEY (id, occurred_at),
    CONSTRAINT producer_outcome_valid CHECK (outcome IN ('SUCCESS', 'FAILED', 'REJECTED'))
) PARTITION BY RANGE (occurred_at);

CREATE TABLE producer_logs_2026_08 PARTITION OF producer_logs
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE producer_logs_2026_09 PARTITION OF producer_logs
    FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE producer_logs_default PARTITION OF producer_logs DEFAULT;

-- "trace this event" — the support lookup this table exists for.
CREATE INDEX producer_event_idx ON producer_logs (event_id);
-- "what failed on this topic recently" — partial, because failures are rare
-- and this index should stay small even as successes accumulate.
CREATE INDEX producer_failures_idx ON producer_logs (topic, occurred_at DESC)
    WHERE outcome <> 'SUCCESS';

-- ---------------------------------------------------------------------------
-- updated_at maintenance
--
-- A trigger rather than application code: application code forgets, and an
-- updated_at that is sometimes stale is worse than not having one, because
-- people build cache invalidation on it.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER topics_updated_at BEFORE UPDATE ON topics
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER schema_subjects_updated_at BEFORE UPDATE ON schema_subjects
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER consumer_registry_updated_at BEFORE UPDATE ON consumer_registry
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Ledger and context store share one table.
--
-- Rationale (D21 ①): creating a response and storing its items must be atomic.
-- Two tables would need a transaction spanning both and could, on partial
-- failure, leave a ledger row whose content is missing — a chain that breaks
-- only on some later turn, which is the hardest failure mode to diagnose.
-- One row makes that impossible by construction.

CREATE TABLE IF NOT EXISTS responses (
    response_id           TEXT        PRIMARY KEY,
    previous_response_id  TEXT,
    tenant_id             TEXT        NOT NULL,
    model                 TEXT        NOT NULL,

    status                TEXT        NOT NULL,
    -- When false the row carries no items and must not be chained (FR-18).
    stored                BOOLEAN     NOT NULL DEFAULT TRUE,

    node_tag              TEXT        NOT NULL,
    attempt               BIGINT      NOT NULL DEFAULT 0,
    owner                 TEXT,
    exec_deadline_ms      BIGINT,

    -- Idempotency gate with no TTL window: presence alone rejects (INV-2).
    idempotency_key       TEXT        UNIQUE,

    -- Echoed on retrieval; never part of chain resolution (INV-49).
    instructions          TEXT,

    input_items           JSONB       NOT NULL DEFAULT '[]'::jsonb,
    output_items          JSONB       NOT NULL DEFAULT '[]'::jsonb,
    usage                 JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- Usage booked against abandoned attempts, keyed by attempt (INV-51).
    partial_usage         JSONB       NOT NULL DEFAULT '{}'::jsonb,

    integrity             TEXT,
    integrity_alg         TEXT,

    created_at_ms         BIGINT      NOT NULL,
    completed_at_ms       BIGINT,
    expires_at_ms         BIGINT
);

-- Tenant scoping and bulk purge (FR-21).
CREATE INDEX IF NOT EXISTS responses_tenant_idx
    ON responses (tenant_id);

-- Chain walking follows this edge upwards.
CREATE INDEX IF NOT EXISTS responses_previous_idx
    ON responses (previous_response_id)
    WHERE previous_response_id IS NOT NULL;

-- Expiry sweep: partial index so the scan only ever touches rows that actually
-- have a deadline (FR-22).
CREATE INDEX IF NOT EXISTS responses_expiry_idx
    ON responses (expires_at_ms)
    WHERE expires_at_ms IS NOT NULL;

-- Startup orphan reclaim (INV-45): partial index keeps this a fast lookup even
-- when the table is large, because non-terminal rows are a tiny minority.
CREATE INDEX IF NOT EXISTS responses_node_active_idx
    ON responses (node_tag, status)
    WHERE status IN ('queued', 'in_progress');

-- Claim picks the oldest queued row.
CREATE INDEX IF NOT EXISTS responses_queued_idx
    ON responses (created_at_ms)
    WHERE status = 'queued';

-- Execution-side liveness. Separate from `responses` because it is per-agent,
-- not per-response: one heartbeat covers every response that agent holds, which
-- is what keeps heartbeat traffic independent of in-flight volume.
CREATE TABLE IF NOT EXISTS agent_heartbeats (
    agent_id      TEXT    PRIMARY KEY,
    last_seen_ms  BIGINT  NOT NULL
);

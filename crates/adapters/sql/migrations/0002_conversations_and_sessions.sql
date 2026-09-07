-- Conversations (D27) and the self-hosted session layer (D26).
--
-- Both live in the same database as `responses` rather than in a store of their
-- own. Session envelope events are two or three per turn, the same order of
-- magnitude as the context writes already going here — the third storage class
-- (D21) exists for the token-level stream, whose volume is three orders of
-- magnitude higher, and nothing here approaches that.

-- A conversation is a *pointer to the tail of a response chain*, so this table
-- has no items and no child table. The chain is already materialised on
-- `responses.context` (D24); a container here would be a second copy of the same
-- content, free to disagree with the first.
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id   TEXT    PRIMARY KEY,
    tenant_id         TEXT    NOT NULL,

    -- Tail of the chain: the context the next generation inherits. NULL until
    -- the first turn completes.
    --
    -- No foreign key to `responses`: deletion is record-level (D24) and upstream
    -- does not cascade either, so a pointer to a deleted response must stay
    -- addressable rather than cause the conversation row to vanish.
    last_response_id  TEXT,

    metadata          JSONB   NOT NULL DEFAULT '{}'::jsonb,
    created_at_ms     BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS conversations_tenant_idx
    ON conversations (tenant_id);

-- Session state: the turn lock and the sequence allocator, in one row.
--
-- The lock lives here, beside `next_seq`, so taking the lock and appending the
-- event that announces it are one transaction. Held apart, a crash between them
-- would leave either a locked session with nothing on the stream to explain it,
-- or an announced turn no lock is holding — permanent disagreement between every
-- device and the server, unrepairable from outside.
CREATE TABLE IF NOT EXISTS sessions (
    session_id                  TEXT    PRIMARY KEY,
    tenant_id                   TEXT    NOT NULL,
    conversation_id             TEXT    NOT NULL,

    -- NULL means idle. Non-NULL is the response holding the turn.
    lock_response_id            TEXT,
    -- Sequence assigned to that turn's `TurnStarted`, so a re-entrant
    -- `begin_turn` can return it instead of appending a duplicate.
    lock_seq                    BIGINT,

    -- Same idea for the terminal side: `end_turn` may be re-entered by an
    -- execution-side retry after the lock is already released.
    last_completed_response_id  TEXT,
    last_completed_seq          BIGINT,

    -- Next sequence to hand out. Allocated by conditional UPDATE inside the same
    -- transaction as the insert, which is what makes concurrent appends unable to
    -- collide; the composite primary key on `session_events` is the backstop.
    next_seq                    BIGINT  NOT NULL DEFAULT 0,

    created_at_ms               BIGINT  NOT NULL
);

CREATE INDEX IF NOT EXISTS sessions_tenant_idx
    ON sessions (tenant_id);

-- Exclusive binding, and the reverse lookup that makes it useful.
--
-- A plain `POST /v1/responses { conversation: … }` finds the owning session
-- through this index and takes its turn lock, so multi-device delivery needs no
-- request field upstream does not have. Uniqueness is what makes that lookup a
-- function: two sessions over one conversation would be two independent locks
-- guarding the same chain, which is no lock at all.
CREATE UNIQUE INDEX IF NOT EXISTS sessions_conversation_uniq
    ON sessions (tenant_id, conversation_id);

-- The durable session event stream.
--
-- `kind` is the serialised envelope and carries references only — a response id,
-- a status, or a business payload. Conversation content is never copied in; it
-- stays on `responses` where it has exactly one home.
CREATE TABLE IF NOT EXISTS session_events (
    session_id  TEXT    NOT NULL REFERENCES sessions (session_id) ON DELETE CASCADE,
    -- 0-based and contiguous per session (INV-11), matching the per-response
    -- event log so both streams are read with one cursor rule.
    seq         BIGINT  NOT NULL,
    kind        JSONB   NOT NULL,
    ts_ms       BIGINT  NOT NULL,

    -- Doubles as the cursor index: reads are `WHERE session_id = $1 AND seq > $2
    -- ORDER BY seq`, which this serves directly. No OFFSET paging anywhere.
    PRIMARY KEY (session_id, seq)
);

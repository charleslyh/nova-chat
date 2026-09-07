-- Durable per-conversation event stream (D28): turn boundaries and business
-- events, ordered in a per-conversation 0-based contiguous sequence space. The
-- turn boundary events are emitted atomically with the in-flight marker
-- transition on `conversations.active_response_id`, inside the same transaction.
CREATE TABLE IF NOT EXISTS conversation_events (
    conversation_id TEXT    NOT NULL REFERENCES conversations (conversation_id) ON DELETE CASCADE,
    -- 0-based and contiguous per conversation, matching the per-response event log.
    seq             BIGINT  NOT NULL,
    kind            JSONB   NOT NULL,
    ts_ms           BIGINT  NOT NULL,

    PRIMARY KEY (conversation_id, seq)
);

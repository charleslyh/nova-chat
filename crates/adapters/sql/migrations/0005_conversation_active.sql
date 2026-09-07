-- In-flight mutual exclusion marker (D28): the response currently running for
-- this conversation, or NULL when idle. Internal only — never exposed on the
-- official conversation object. Reached through `acquire_active` / `release_active`.
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS active_response_id TEXT;

-- Per-conversation event sequence allocator (D28). Bumped under a row lock so
-- concurrent turn boundaries serialise instead of colliding.
ALTER TABLE conversations ADD COLUMN IF NOT EXISTS next_seq BIGINT NOT NULL DEFAULT 0;

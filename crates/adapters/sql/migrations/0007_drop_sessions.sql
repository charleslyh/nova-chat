-- D28: the self-hosted session layer (D26) is folded into conversations — the
-- conversation now owns the event stream (`conversation_events`, 0006), the
-- in-flight marker (`conversations.active_response_id`, 0005) and the turn lock.
--
-- The `sessions` / `session_events` tables are no longer written, so drop them to
-- keep a stale schema from accepting writes the code no longer issues. `sessions`
-- is dropped last: `session_events` references it.
DROP TABLE IF EXISTS session_events;
DROP TABLE IF EXISTS sessions;

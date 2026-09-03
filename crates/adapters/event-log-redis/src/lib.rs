//! Redis Streams backend for [`ResponseEventLog`] (D25).
//!
//! # Key layout
//!
//! | key | purpose |
//! |---|---|
//! | `{prefix}:events:{id}` | the per-response stream (`XADD` / `XREAD`) |
//! | `{prefix}:seq:{id}` | `INCR` counter for the next sequence number |
//! | `{prefix}:meta:{id}` | tombstone marker, survives the stream briefly so a late subscriber gets `Expired` (410) rather than an ambiguous `Unknown` (404) |
//!
//! # Sequence numbers
//!
//! Sequence numbers are 0-based and contiguous (INV-11). They are encoded into
//! the stream entry id as `0-{n}` (`seq 0` → `0-1`, `seq k` → `0-{k+1}`), which
//! makes `read_after(starting_after)` a direct `XREAD` range — `O(limit)` — rather
//! than a full scan.
//!
//! # Retention
//!
//! `close` sets a TTL on the stream and the counter, and plants a tombstone that
//! outlives the stream by `TOMBSTONE_FACTOR × retain`. Redis expires the keys
//! itself, so [`ResponseEventLog::sweep_expired`] is a no-op returning 0: the
//! work is delegated to Redis's built-in expiry (D25 chooses Redis Streams partly
//! for this reason — per-response streams are cheap and self-bounded).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use nova_responses_core::{
    EventBody, EventLogError, LedgerError, ResponseEvent, ResponseEventKind, ResponseEventLog,
    ResponseId, ResponseLedger,
};
use redis::aio::MultiplexedConnection;

/// Tombstone survives the stream by this multiple of `retain`, matching the
/// in-memory adapter's `TOMBSTONE_FACTOR` (INV-40).
const TOMBSTONE_FACTOR: u64 = 10;

/// Atomic `INCR` + `XADD`.
///
/// Lua is the only way to read the `INCR` result and feed it into the `XADD`
/// entry id in one atomic step. Without it, two concurrent appends could
/// interleave their `INCR` and `XADD` — the later `INCR` wins, then the earlier
/// task's `XADD` of the smaller id is rejected ("ID equal or smaller than the
/// target stream top item"), and a sequence number is silently lost.
const APPEND_LUA: &str = r#"
local n = redis.call('INCR', KEYS[1])
redis.call('XADD', KEYS[2], '0-' .. tostring(n), 'data', ARGV[1])
return n - 1
"#;

pub struct RedisResponseEventLog {
    conn: MultiplexedConnection,
    ledger: Arc<dyn ResponseLedger>,
    prefix: String,
}

impl RedisResponseEventLog {
    /// Connect to Redis and return the adapter. The connection is multiplexed, so
    /// it is safe to share across tasks.
    pub async fn connect(
        url: &str,
        ledger: Arc<dyn ResponseLedger>,
        prefix: impl Into<String>,
    ) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            conn,
            ledger,
            prefix: prefix.into(),
        })
    }

    fn stream_key(&self, id: &ResponseId) -> String {
        format!("{}:events:{id}", self.prefix)
    }

    fn seq_key(&self, id: &ResponseId) -> String {
        format!("{}:seq:{id}", self.prefix)
    }

    fn meta_key(&self, id: &ResponseId) -> String {
        format!("{}:meta:{id}", self.prefix)
    }

    async fn check_append(&self, event: &ResponseEvent) -> Result<(), EventLogError> {
        if self.ledger.is_read_only() {
            return Err(EventLogError::ReadOnly);
        }
        // Attempt fence before the write (INV-6): a reaped holder must not inject
        // events after its attempt was superseded.
        if let Some(attempt) = event.attempt {
            self.ledger
                .check_attempt(&event.response_id, attempt)
                .await
                .map_err(|e| match e {
                    LedgerError::StaleAttempt => EventLogError::StaleAttempt,
                    LedgerError::ReadOnly => EventLogError::ReadOnly,
                    LedgerError::NotFound => EventLogError::Unknown,
                    other => EventLogError::Internal(other.to_string()),
                })?;
        }
        Ok(())
    }
}

/// Internal wire shape: `type` plus the flattened body fields.
///
/// `ResponseEvent` itself only implements `Serialize` (its `body` is a
/// `#[serde(flatten)]` untagged enum, which serde cannot deserialize), so this
/// mirror type carries the fields and [`rebuild_body`] reconstructs the enum.
///
/// The `sequence_number` is **not** read back from the wire: it is the authority
/// of the entry id (`0-{n}` → seq `n-1`), recovered by [`parse_entry_seq`]. The
/// wire copy is a placeholder the caller serialised and is ignored.
#[derive(serde::Deserialize)]
struct WireEvent {
    #[serde(rename = "type")]
    kind: ResponseEventKind,
    #[serde(flatten)]
    rest: serde_json::Map<String, serde_json::Value>,
}

fn get_string(rest: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    rest.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn get_u32(rest: &serde_json::Map<String, serde_json::Value>, key: &str) -> u32 {
    rest.get(key)
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32
}

/// Reconstruct the kind-specific body from the flattened wire fields.
fn rebuild_body(kind: ResponseEventKind, rest: &serde_json::Map<String, serde_json::Value>) -> EventBody {
    match kind {
        ResponseEventKind::Created
        | ResponseEventKind::InProgress
        | ResponseEventKind::Completed
        | ResponseEventKind::Failed
        | ResponseEventKind::Incomplete => EventBody::Response {
            response: rest.get("response").cloned().unwrap_or(serde_json::Value::Null),
        },
        ResponseEventKind::OutputTextDelta | ResponseEventKind::FunctionCallArgumentsDelta => {
            EventBody::Delta {
                item_id: get_string(rest, "item_id"),
                output_index: get_u32(rest, "output_index"),
                content_index: rest
                    .get("content_index")
                    .and_then(|v| v.as_u64())
                    .map(|v| v as u32),
                delta: get_string(rest, "delta"),
            }
        }
        ResponseEventKind::OutputItemAdded | ResponseEventKind::OutputItemDone => EventBody::Item {
            output_index: get_u32(rest, "output_index"),
            item: rest.get("item").cloned().unwrap_or(serde_json::Value::Null),
        },
        ResponseEventKind::FunctionCallArgumentsDone => EventBody::Arguments {
            output_index: get_u32(rest, "output_index"),
            item_id: get_string(rest, "item_id"),
            arguments: get_string(rest, "arguments"),
        },
        ResponseEventKind::OutputTextDone => EventBody::Text {
            item_id: get_string(rest, "item_id"),
            output_index: get_u32(rest, "output_index"),
            content_index: get_u32(rest, "content_index"),
            text: get_string(rest, "text"),
        },
        ResponseEventKind::ContentPartAdded | ResponseEventKind::ContentPartDone => EventBody::Part {
            item_id: get_string(rest, "item_id"),
            output_index: get_u32(rest, "output_index"),
            content_index: get_u32(rest, "content_index"),
            part: rest.get("part").cloned().unwrap_or(serde_json::Value::Null),
        },
    }
}

/// Recover the sequence number from a stream entry id: `0-{n}` ↔ seq `n-1`.
fn parse_entry_seq(entry_id: &str) -> u64 {
    entry_id
        .split_once('-')
        .and_then(|(_, s)| s.parse::<u64>().ok())
        .map(|n| n.saturating_sub(1))
        .unwrap_or(0)
}

fn into_event(wire: WireEvent, response_id: ResponseId, sequence_number: u64) -> ResponseEvent {
    ResponseEvent {
        response_id,
        sequence_number,
        kind: wire.kind,
        // The attempt fence is internal concurrency control; it is not on the
        // wire and is not needed by readers.
        attempt: None,
        body: rebuild_body(wire.kind, &wire.rest),
    }
}

type RawRead = Vec<(String, Vec<(String, HashMap<String, String>)>)>;

/// Decode an `XREAD` result into events, recovering each sequence number from
/// its entry id rather than trusting the wire copy.
fn decode(raw: RawRead, response_id: &ResponseId) -> Result<Vec<ResponseEvent>, EventLogError> {
    let mut out = Vec::new();
    for (_, entries) in raw {
        for (entry_id, fields) in entries {
            if let Some(data) = fields.get("data") {
                let wire: WireEvent = serde_json::from_str(data).map_err(internal)?;
                let seq = parse_entry_seq(&entry_id);
                out.push(into_event(wire, response_id.clone(), seq));
            }
        }
    }
    Ok(out)
}

fn internal(err: impl std::fmt::Display) -> EventLogError {
    EventLogError::Internal(err.to_string())
}

/// `XREAD COUNT limit STREAMS stream start_id` — immediate, exclusive on `start_id`.
async fn xread(
    conn: &mut MultiplexedConnection,
    stream: &str,
    start_id: &str,
    limit: usize,
    response_id: &ResponseId,
) -> Result<Vec<ResponseEvent>, EventLogError> {
    let raw: RawRead = redis::cmd("XREAD")
        .arg("COUNT")
        .arg(limit)
        .arg("STREAMS")
        .arg(stream)
        .arg(start_id)
        .query_async(conn)
        .await
        .map_err(internal)?;

    decode(raw, response_id)
}

#[async_trait]
impl ResponseEventLog for RedisResponseEventLog {
    async fn append(&self, event: ResponseEvent) -> Result<u64, EventLogError> {
        self.check_append(&event).await?;

        let seq_key = self.seq_key(&event.response_id);
        let stream = self.stream_key(&event.response_id);

        // The wire JSON's `sequence_number` is a placeholder; the authoritative
        // seq is assigned by the script's INCR and encoded into the entry id.
        let json = serde_json::to_string(&event).map_err(internal)?;

        let mut conn = self.conn.clone();
        let seq: u64 = redis::Script::new(APPEND_LUA)
            .key(seq_key)
            .key(stream)
            .arg(json)
            .invoke_async(&mut conn)
            .await
            .map_err(internal)?;

        Ok(seq)
    }

    async fn read_after(
        &self,
        response_id: &ResponseId,
        starting_after: Option<u64>,
        limit: usize,
        wait_ms: u64,
    ) -> Result<Vec<ResponseEvent>, EventLogError> {
        let stream = self.stream_key(response_id);
        let meta = self.meta_key(response_id);

        let mut conn = self.conn.clone();

        // Start id is exclusive: `None` reads from seq 0 (`0-1`), `Some(n)` reads
        // from seq `n+1` (`0-{n+2}`), i.e. strictly after `0-{n+1}`.
        let start_id = match starting_after {
            None => "0-0".to_string(),
            Some(n) => format!("0-{}", n.saturating_add(1)),
        };

        let exists: bool = redis::cmd("EXISTS")
            .arg(&stream)
            .query_async(&mut conn)
            .await
            .map_err(internal)?;
        if !exists {
            let tombstone: bool = redis::cmd("EXISTS")
                .arg(&meta)
                .query_async(&mut conn)
                .await
                .map_err(internal)?;
            return if tombstone {
                Err(EventLogError::Expired)
            } else {
                Err(EventLogError::Unknown)
            };
        }

        let batch = xread(&mut conn, &stream, &start_id, limit, response_id).await?;
        if !batch.is_empty() {
            return Ok(batch);
        }

        // Nothing buffered yet: long-poll for new entries, if asked.
        if wait_ms == 0 {
            return Ok(vec![]);
        }
        let raw: RawRead = redis::cmd("XREAD")
            .arg("BLOCK")
            .arg(wait_ms)
            .arg("COUNT")
            .arg(limit)
            .arg("STREAMS")
            .arg(&stream)
            .arg("$")
            .query_async(&mut conn)
            .await
            .map_err(internal)?;

        decode(raw, response_id)
    }

    async fn close(
        &self,
        response_id: &ResponseId,
        _now_ms: u64,
        retain_ms: u64,
    ) -> Result<(), EventLogError> {
        let stream = self.stream_key(response_id);
        let seq = self.seq_key(response_id);
        let meta = self.meta_key(response_id);

        let mut conn = self.conn.clone();
        let retain_s = (retain_ms / 1000).max(1) as u64;
        let tombstone_s = (retain_ms * TOMBSTONE_FACTOR / 1000).max(retain_s) as u64;

        redis::cmd("EXPIRE")
            .arg(&stream)
            .arg(retain_s)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(internal)?;
        redis::cmd("EXPIRE")
            .arg(&seq)
            .arg(retain_s)
            .query_async::<i64>(&mut conn)
            .await
            .map_err(internal)?;
        // Tombstone outlives the stream so a late subscriber sees Expired, not Unknown.
        redis::cmd("SET")
            .arg(&meta)
            .arg("1")
            .arg("EX")
            .arg(tombstone_s)
            .query_async::<String>(&mut conn)
            .await
            .map_err(internal)?;

        Ok(())
    }

    async fn sweep_expired(&self, _now_ms: u64) -> Result<u64, EventLogError> {
        // Retention is delegated to Redis TTL (see module docs); nothing to sweep.
        Ok(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses_core::{Attempt, NodeTag};

    fn id() -> ResponseId {
        ResponseId::new(NodeTag::parse("n1").unwrap())
    }

    fn rest(json: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        json.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn rebuilds_every_body_variant() {
        // Lifecycle → Response.
        let body = rebuild_body(
            ResponseEventKind::Created,
            &rest(serde_json::json!({ "response": { "id": "r1" } })),
        );
        assert!(matches!(body, EventBody::Response { .. }));

        // Delta → Delta (no content_index when absent).
        let body = rebuild_body(
            ResponseEventKind::OutputTextDelta,
            &rest(serde_json::json!({ "item_id": "m1", "output_index": 0, "delta": "hi" })),
        );
        match body {
            EventBody::Delta { item_id, content_index, delta, .. } => {
                assert_eq!(item_id, "m1");
                assert_eq!(content_index, None);
                assert_eq!(delta, "hi");
            }
            other => panic!("expected Delta, got {other:?}"),
        }

        // Item.
        let body = rebuild_body(
            ResponseEventKind::OutputItemAdded,
            &rest(serde_json::json!({ "output_index": 1, "item": { "type": "message" } })),
        );
        assert!(matches!(body, EventBody::Item { .. }));

        // Arguments.
        let body = rebuild_body(
            ResponseEventKind::FunctionCallArgumentsDone,
            &rest(serde_json::json!({ "output_index": 0, "item_id": "c1", "arguments": "{}" })),
        );
        assert!(matches!(body, EventBody::Arguments { .. }));

        // Text.
        let body = rebuild_body(
            ResponseEventKind::OutputTextDone,
            &rest(serde_json::json!({ "item_id": "m1", "output_index": 0, "content_index": 0, "text": "hi" })),
        );
        assert!(matches!(body, EventBody::Text { .. }));

        // Part.
        let body = rebuild_body(
            ResponseEventKind::ContentPartAdded,
            &rest(serde_json::json!({ "item_id": "m1", "output_index": 0, "content_index": 0, "part": { "type": "output_text" } })),
        );
        assert!(matches!(body, EventBody::Part { .. }));
    }

    #[test]
    fn wire_event_round_trips_without_losing_fields() {
        // Serialise a real event, then parse it back through the mirror type and
        // check that every field survives the round trip.
        let event = ResponseEvent::text_delta(
            id(),
            Attempt(1),
            "msg_1".into(),
            0,
            0,
            "hello",
        );
        let mut event = event;
        event.sequence_number = 3;
        let json = serde_json::to_string(&event).unwrap();

        let wire: WireEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(wire.kind, ResponseEventKind::OutputTextDelta);

        // The sequence number is recovered from the entry id, not the wire copy.
        let rebuilt = into_event(wire, id(), parse_entry_seq("0-4"));
        assert_eq!(rebuilt.sequence_number, 3);
        match &rebuilt.body {
            EventBody::Delta { item_id, delta, .. } => {
                assert_eq!(item_id.as_str(), "msg_1");
                assert_eq!(delta.as_str(), "hello");
            }
            other => panic!("expected Delta, got {other:?}"),
        }
        // The round-tripped event serialises back to the same wire JSON.
        assert_eq!(serde_json::to_string(&rebuilt).unwrap(), json);
    }

    #[test]
    fn entry_id_round_trips_to_sequence_number() {
        assert_eq!(parse_entry_seq("0-1"), 0);
        assert_eq!(parse_entry_seq("0-2"), 1);
        assert_eq!(parse_entry_seq("0-20001"), 20000);
        // A malformed id falls back to 0 rather than panicking on parse.
        assert_eq!(parse_entry_seq("not-an-id"), 0);
    }
}

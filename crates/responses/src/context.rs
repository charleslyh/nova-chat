//! Stored response records and chain resolution types (D20).
//!
//! What is stored: the response's own **items** plus a pointer to the previous
//! link. What is *not* stored: the incremental event stream.

use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use crate::conversation::ConversationId;
use crate::protocol::{ResponseItem, Tool, ToolChoice};
use crate::shared::{AgentId, Attempt, IdError, IdempotencyKey, NodeTag, TenantId};

/// `resp_{node}_{uuid}`.
///
/// Carrying the host node inside the id is what makes directed routing possible
/// without any shared registry lookup.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ResponseId {
    node_tag: NodeTag,
    uuid: Uuid,
}

impl ResponseId {
    pub const PREFIX: &'static str = "resp_";

    pub fn new(node_tag: NodeTag) -> Self {
        Self {
            node_tag,
            uuid: Uuid::new_v4(),
        }
    }

    pub fn from_parts(node_tag: NodeTag, uuid: Uuid) -> Self {
        Self { node_tag, uuid }
    }

    pub fn parse(raw: &str) -> Result<Self, IdError> {
        let rest = raw.strip_prefix(Self::PREFIX).ok_or(IdError::MissingPrefix)?;
        // Node tags cannot contain '_', so the first separator is unambiguous.
        let (node, uuid) = rest.split_once('_').ok_or(IdError::MalformedShape)?;
        let node_tag = NodeTag::parse(node)?;
        let uuid = Uuid::parse_str(uuid).map_err(|_| IdError::InvalidUuid)?;
        Ok(Self { node_tag, uuid })
    }

    pub fn node_tag(&self) -> &NodeTag {
        &self.node_tag
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }
}

impl fmt::Display for ResponseId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}_{}", Self::PREFIX, self.node_tag, self.uuid)
    }
}

impl FromStr for ResponseId {
    type Err = IdError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ResponseId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ResponseId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(d)?;
        ResponseId::parse(&raw).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStatus {
    Queued,
    InProgress,
    Completed,
    Failed,
    Incomplete,
    Cancelled,
}

impl ResponseStatus {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            ResponseStatus::Completed
                | ResponseStatus::Failed
                | ResponseStatus::Incomplete
                | ResponseStatus::Cancelled
        )
    }

    pub fn as_str(self) -> &'static str {
        match self {
            ResponseStatus::Queued => "queued",
            ResponseStatus::InProgress => "in_progress",
            ResponseStatus::Completed => "completed",
            ResponseStatus::Failed => "failed",
            ResponseStatus::Incomplete => "incomplete",
            ResponseStatus::Cancelled => "cancelled",
        }
    }
}

/// Token accounting. Integer typed throughout — never routed through `f64`,
/// which would silently lose precision on large counts (D22).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

impl Usage {
    pub fn new(input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            input_tokens,
            output_tokens,
            total_tokens: input_tokens.saturating_add(output_tokens),
        }
    }

    pub fn is_zero(&self) -> bool {
        self.input_tokens == 0 && self.output_tokens == 0 && self.total_tokens == 0
    }

    /// Accumulate across attempts so a mid-flight abort still contributes to
    /// billing (CR-11 / INV-51).
    pub fn add(self, other: Usage) -> Self {
        Self {
            input_tokens: self.input_tokens.saturating_add(other.input_tokens),
            output_tokens: self.output_tokens.saturating_add(other.output_tokens),
            total_tokens: self.total_tokens.saturating_add(other.total_tokens),
        }
    }
}

/// A persisted response record: ledger metadata for one generation.
///
/// Unlike the D24 design, the record carries **no materialised ancestor
/// snapshot**. Long-term history lives in the conversation's snapshot (D30);
/// this record holds only what identifies the response and the context it
/// inherits from (its anchor), plus the turn's own input. Output is appended to
/// the conversation snapshot at terminal and is reconstructable from the event
/// stream within the retention window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseRecord {
    pub response_id: ResponseId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<ResponseId>,

    /// Conversation this response belongs to, if any (D28). Recorded at create
    /// time because the execution side has only the record when it reaches a
    /// terminal status, and the alternative — scanning conversations for one
    /// pointing at this response — cannot work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<ConversationId>,

    pub tenant_id: TenantId,
    pub model: String,

    /// Echoed on retrieval, **never** fed into context assembly (INV-49).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// Functions offered to the model this turn, in outbound provider shape.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Tool>,

    /// Per-response `tool_choice` selection, in outbound provider shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    pub input_items: Vec<ResponseItem>,

    /// Reasoning / thinking text streamed by a reasoning model this turn,
    /// concatenated into one string. Render-only, never fed back as context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,

    pub status: ResponseStatus,
    #[serde(default)]
    pub usage: Usage,

    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,

    /// When false the record is not retained and cannot be referenced as a
    /// previous link (FR-18).
    pub stored: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,

    /// Integrity tag over the canonical encoding of the items.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity_alg: Option<String>,

    /// Host node that owns the in-flight buffer for this response.
    pub node_tag: NodeTag,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<AgentId>,
    #[serde(default)]
    pub attempt: Attempt,
}

impl ResponseRecord {
    /// Where this response inherits its context from.
    ///
    /// The anchor is derived from the record's own fields — it is not a second
    /// copy of state, just the view the execution/service layers consume to know
    /// where to read history from (a conversation snapshot, a previous response,
    /// or nothing).
    pub fn anchor(&self) -> SnapshotRef {
        if let Some(id) = &self.conversation_id {
            SnapshotRef::Conversation(id.clone())
        } else if let Some(id) = &self.previous_response_id {
            SnapshotRef::Previous(id.clone())
        } else {
            SnapshotRef::Root
        }
    }

    /// Whether this record may be used as `previous_response_id` by `tenant`.
    pub fn is_referencable_by(&self, tenant: &TenantId) -> bool {
        self.stored && &self.tenant_id == tenant
    }

    /// The OpenAI-shaped response object for a response whose output is known,
    /// embedded in lifecycle events and returned by `GET`. Only protocol fields
    /// are exposed — the internal bookkeeping (`tenant_id`, `node_tag`,
    /// `attempt`, …) never leaves the node.
    ///
    /// `output_items` is supplied by the caller rather than read from the record:
    /// the record holds no output (output lives in the conversation snapshot and
    /// the event stream), so the renderer passes in what it reconstructed.
    pub fn to_response_value(&self, output_items: &[ResponseItem]) -> Value {
        serde_json::json!({
            "id": self.response_id.to_string(),
            "object": "response",
            "created_at": self.created_at_ms / 1000,
            "status": self.status.as_str(),
            "model": self.model,
            "previous_response_id": self.previous_response_id.as_ref().map(|v| v.to_string()),
            "conversation": self
                .conversation_id
                .as_ref()
                .map(|id| serde_json::json!({ "id": id.to_string() })),
            "instructions": self.instructions,
            "store": self.stored,
            "input": self.input_items,
            "output": output_items,
            "reasoning": self.reasoning,
            "usage": {
                "input_tokens": self.usage.input_tokens,
                "output_tokens": self.usage.output_tokens,
                "total_tokens": self.usage.total_tokens,
            },
        })
    }
}

/// Where a response inherits its context from (D30).
///
/// The three variants replace D24's materialised snapshot: history is read from
/// a single authoritative source — the conversation snapshot for anchored
/// turns, or resolved through a previous response — rather than copied onto
/// every record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SnapshotRef {
    /// No prior context.
    Root,
    /// Continue from a previous response (bare chain, no conversation anchor).
    Previous(ResponseId),
    /// Continue from a conversation's materialised snapshot.
    Conversation(ConversationId),
}

/// Bounds for chain resolution. Exceeding any of them is an **error**, never a
/// silent truncation (INV-41).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainLimits {
    pub max_depth: usize,
    pub max_items: usize,
    pub max_bytes: usize,
}

impl Default for ChainLimits {
    fn default() -> Self {
        Self {
            max_depth: 50,
            max_items: 1000,
            max_bytes: 1024 * 1024,
        }
    }
}

/// Result of resolving a response's context: history in chronological order.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResolvedContext {
    pub items: Vec<ResponseItem>,
    /// Reasoning blocks aligned to `items`: one entry per item, `Some(text)`
    /// meaning a reasoning block precedes that item. Render-only — never fed
    /// back into model context.
    #[serde(default)]
    pub reasoning: Vec<Option<String>>,
    pub depth: usize,
    pub bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(s: &str) -> TenantId {
        TenantId::parse(s).unwrap()
    }

    fn tag(s: &str) -> NodeTag {
        NodeTag::parse(s).unwrap()
    }

    #[test]
    fn round_trips_response_id() {
        let id = ResponseId::new(tag("node-a"));
        let text = id.to_string();
        assert!(text.starts_with("resp_node-a_"));
        assert_eq!(ResponseId::parse(&text).unwrap(), id);
        assert_eq!(id.node_tag().as_str(), "node-a");
    }

    #[test]
    fn round_trips_through_serde() {
        let id = ResponseId::new(tag("n1"));
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(serde_json::from_str::<ResponseId>(&json).unwrap(), id);
    }

    #[test]
    fn rejects_malformed_response_ids() {
        assert_eq!(ResponseId::parse("abc"), Err(IdError::MissingPrefix));
        assert_eq!(ResponseId::parse("resp_nouuid"), Err(IdError::MalformedShape));
        assert_eq!(
            ResponseId::parse("resp_node-a_not-a-uuid"),
            Err(IdError::InvalidUuid)
        );
        assert_eq!(
            ResponseId::parse("resp_NODE_00000000-0000-0000-0000-000000000000"),
            Err(IdError::InvalidNodeTag)
        );
        // Path traversal attempt inside the id.
        assert!(ResponseId::parse("resp_../../etc_x").is_err());
    }

    fn record(stored: bool, tenant_id: &str) -> ResponseRecord {
        ResponseRecord {
            response_id: ResponseId::new(NodeTag::parse("n1").unwrap()),
            previous_response_id: None,
            conversation_id: None,
            tenant_id: tenant(tenant_id),
            model: "m".into(),
            instructions: Some("secret system prompt".into()),
            tools: Vec::new(),
            tool_choice: None,
            input_items: vec![ResponseItem::user_text("in")],
            reasoning: None,
            status: ResponseStatus::Completed,
            usage: Usage::new(1, 2),
            created_at_ms: 0,
            completed_at_ms: Some(1),
            stored,
            expires_at_ms: None,
            integrity: None,
            integrity_alg: None,
            node_tag: NodeTag::parse("n1").unwrap(),
            idempotency_key: None,
            owner: None,
            attempt: Attempt::default(),
        }
    }

    #[test]
    fn anchor_is_derived_from_the_record() {
        let root = record(true, "t1");
        assert_eq!(root.anchor(), SnapshotRef::Root);

        let prev = ResponseId::new(NodeTag::parse("n1").unwrap());
        let mut chained = record(true, "t1");
        chained.previous_response_id = Some(prev.clone());
        assert_eq!(chained.anchor(), SnapshotRef::Previous(prev));

        let conv = ConversationId::new();
        let mut anchored = record(true, "t1");
        anchored.conversation_id = Some(conv.clone());
        assert_eq!(anchored.anchor(), SnapshotRef::Conversation(conv));
    }

    #[test]
    fn referencability_requires_store_and_same_tenant() {
        assert!(record(true, "t1").is_referencable_by(&tenant("t1")));
        assert!(!record(false, "t1").is_referencable_by(&tenant("t1")));
        assert!(!record(true, "t1").is_referencable_by(&tenant("t2")));
    }

    #[test]
    fn usage_totals_and_accumulates() {
        let a = Usage::new(3, 4);
        assert_eq!(a.total_tokens, 7);
        let b = a.add(Usage::new(1, 1));
        assert_eq!(b, Usage { input_tokens: 4, output_tokens: 5, total_tokens: 9 });
        assert!(Usage::default().is_zero());
        assert!(!a.is_zero());
    }

    #[test]
    fn usage_saturates_instead_of_overflowing() {
        let max = Usage {
            input_tokens: u64::MAX,
            output_tokens: u64::MAX,
            total_tokens: u64::MAX,
        };
        assert_eq!(max.add(Usage::new(1, 1)), max);
    }

    #[test]
    fn terminal_status_set() {
        for s in [
            ResponseStatus::Completed,
            ResponseStatus::Failed,
            ResponseStatus::Incomplete,
            ResponseStatus::Cancelled,
        ] {
            assert!(s.is_terminal(), "{s:?}");
        }
        assert!(!ResponseStatus::Queued.is_terminal());
        assert!(!ResponseStatus::InProgress.is_terminal());
    }

    #[test]
    fn default_chain_limits_match_parameters_doc() {
        let limits = ChainLimits::default();
        assert_eq!(limits.max_depth, 50);
        assert_eq!(limits.max_bytes, 1024 * 1024);
    }
}

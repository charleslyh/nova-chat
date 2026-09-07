//! Stored response records and chain resolution types (D20).
//!
//! What is stored: the response's own **items** plus a pointer to the previous
//! link. What is *not* stored: the incremental event stream.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{
    Attempt, AgentId, ConversationId, IdempotencyKey, NodeTag, ResponseId, SessionId, TenantId,
};
use crate::protocol::ResponseItem;

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

/// A persisted response record: the unit of both the ledger and the context
/// store (they share one row / one transaction — D21 ①).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredResponse {
    pub response_id: ResponseId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<ResponseId>,

    /// Conversation whose tail pointer this response advances on completion
    /// (D27).
    ///
    /// Recorded here rather than looked up later because the execution side has
    /// only the record when it reaches a terminal status, and the alternative —
    /// scanning conversations for one pointing at this response — cannot work:
    /// the pointer still refers to the *previous* response until this one
    /// finishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_id: Option<ConversationId>,

    /// Session that holds the turn lock for this response (D26).
    ///
    /// Present only when the generation was started through a session. Every
    /// terminal path must use it to release the lock; a path that does not
    /// leaves the session busy forever.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<SessionId>,

    pub tenant_id: TenantId,
    pub model: String,

    /// Echoed on retrieval, **never** fed into chain resolution (INV-49).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    pub input_items: Vec<ResponseItem>,
    #[serde(default)]
    pub output_items: Vec<ResponseItem>,

    /// Reasoning / thinking text streamed by a reasoning model for this
    /// response, concatenated into one string.
    ///
    /// Persisted so a re-render shows the same thinking it showed during
    /// streaming, but **never** fed back as context: a model does not read its
    /// own thinking, and [`ResponseItem`] keeps `reasoning` out of the subset.
    /// Like `instructions`, it is render-only and therefore not part of the
    /// integrity tag.
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

    /// The full context this response inherited, as a flat copy of every
    /// ancestor's items in chronological order.
    ///
    /// This is the **materialised history** (D24): instead of walking
    /// `previous_response_id` on every read, each response carries the whole
    /// conversation that led up to it. A later response therefore never depends
    /// on its ancestors still existing — deleting a middle response removes only
    /// that response's own record; the flat copy lives on inside every
    /// descendant, exactly as "remove from the conversation" (rather than
    /// "erase from the conversation") requires.
    ///
    /// It is flat on purpose: there is no source tag, because nothing ever needs
    /// to strip a single ancestor out again. Deletion is record-level, not
    /// content-level.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context: Vec<ResponseItem>,

    /// Materialised reasoning of every ancestor, aligned to [`Self::context`]:
    /// one entry per item, where `Some(text)` marks a reasoning block to render
    /// immediately before that item.
    ///
    /// Kept separate from `context` so reasoning can be materialised for
    /// rendering (D24) **without** ever entering the model context, which is
    /// assembled from `context` alone.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub context_reasoning: Vec<Option<String>>,

    /// How many ancestors contributed to `context`.
    ///
    /// Kept separate because a flat list cannot recover this — one turn may hold
    /// any number of items (a tool-call turn contributes several), so the count
    /// of items is not the count of turns. It backs the `ChainTooLong` check at
    /// create time and the `depth` reported by `resolve_chain`.
    #[serde(default)]
    pub context_depth: usize,
}

impl StoredResponse {
    /// Items contributed by this link when walking a chain, in chronological
    /// order: what went in, then what came out.
    ///
    /// Instructions are **absent by construction** — the field is simply not
    /// consulted here (INV-49).
    pub fn chain_items(&self) -> impl Iterator<Item = &ResponseItem> {
        self.input_items.iter().chain(self.output_items.iter())
    }

    pub fn chain_byte_len(&self) -> usize {
        self.chain_items().map(ResponseItem::byte_len).sum()
    }

    /// The resolved history this record anchors: the materialised ancestors plus
    /// this record's own items, paired with reasoning blocks aligned to the item
    /// list (one entry per item; `Some(text)` marks a reasoning block to render
    /// immediately before that item).
    ///
    /// The alignment rule lives here — not in each backend — so both stores
    /// materialise reasoning identically. Reasoning precedes this record's
    /// output: it is the thinking that produced the answer, and it never enters
    /// the model context (that is assembled from `context`/`items` alone).
    pub fn resolved_items_and_reasoning(&self) -> (Vec<ResponseItem>, Vec<Option<String>>) {
        let mut items = self.context.clone();
        let mut reasoning = self.context_reasoning.clone();
        // Defensive: a legacy or hand-built record may carry mismatched lengths.
        // Reasoning is render-only, so recovering by padding is always safe.
        reasoning.resize(items.len(), None);

        items.extend(self.input_items.iter().cloned());
        reasoning.extend(std::iter::repeat(None).take(self.input_items.len()));

        items.extend(self.output_items.iter().cloned());
        // The reasoning block sits at the boundary before the first output item;
        // `resize` pads the remaining output items (or trims when output is
        // empty, where reasoning is meaningless).
        reasoning.push(self.reasoning.clone());
        reasoning.resize(items.len(), None);

        (items, reasoning)
    }

    /// Whether this record may be used as `previous_response_id` by `tenant`.
    pub fn is_referencable_by(&self, tenant: &TenantId) -> bool {
        self.stored && &self.tenant_id == tenant
    }

    /// The OpenAI-shaped response object, embedded in lifecycle events and
    /// returned by `GET`. Only protocol fields are exposed — the internal
    /// bookkeeping (`tenant_id`, `node_tag`, `attempt`, `context`, …) never
    /// leaves the node.
    ///
    /// `session_id` is absent for the same reason: the session layer is ours, not
    /// upstream's, and this object has an upstream shape. Devices learn which
    /// session a response belongs to from the session event stream, which is
    /// where that relationship is expressed.
    pub fn to_response_value(&self) -> Value {
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
            "output": self.output_items,
            "reasoning": self.reasoning,
            "usage": {
                "input_tokens": self.usage.input_tokens,
                "output_tokens": self.usage.output_tokens,
                "total_tokens": self.usage.total_tokens,
            },
        })
    }
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

    fn record(stored: bool, tenant_id: &str) -> StoredResponse {
        StoredResponse {
            response_id: ResponseId::new(NodeTag::parse("n1").unwrap()),
            previous_response_id: None,
            conversation_id: None,
            session_id: None,
            tenant_id: tenant(tenant_id),
            model: "m".into(),
            instructions: Some("secret system prompt".into()),
            input_items: vec![ResponseItem::user_text("in")],
            output_items: vec![ResponseItem::assistant_text("out")],
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
            context: Vec::new(),
            context_reasoning: Vec::new(),
            context_depth: 0,
        }
    }

    #[test]
    fn chain_items_are_input_then_output() {
        let rec = record(true, "t1");
        let items: Vec<_> = rec.chain_items().cloned().collect();
        assert_eq!(items, vec![
            ResponseItem::user_text("in"),
            ResponseItem::assistant_text("out"),
        ]);
    }

    #[test]
    fn chain_items_never_contain_instructions() {
        // INV-49: the instructions text must not leak into chain output even
        // though it is stored on the record for echo purposes.
        let rec = record(true, "t1");
        let encoded = crate::canonical::canonical_items(
            &rec.chain_items().cloned().collect::<Vec<_>>(),
        );
        assert!(
            !encoded.contains("secret system prompt"),
            "instructions leaked into chain items: {encoded}"
        );
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

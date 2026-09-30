//! The response: its id, its lifecycle status, what the caller asked for, and the
//! record that persists both (D20).
//!
//! What is stored: the turn's own **items** plus a pointer to the context it
//! inherits. What is *not* stored: the incremental event stream, and no
//! materialised copy of ancestor history (D30).

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use strum::IntoStaticStr;
use uuid::Uuid;

use crate::conversation::ConversationId;
use crate::identity::{AgentId, Attempt, IdError, IdempotencyKey, NodeTag, TenantId};
use crate::protocol::{MetadataValue, ResponseItem, Tool, ToolChoice};
use crate::usage::Usage;

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

    fn err() -> IdError {
        IdError::new("response id", "resp_<node>_<uuid>")
    }

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
        let rest = raw.strip_prefix(Self::PREFIX).ok_or_else(Self::err)?;
        // Node tags cannot contain '_', so the first separator is unambiguous.
        let (node, uuid) = rest.split_once('_').ok_or_else(Self::err)?;
        // The node tag's own rule is reported as the node tag's own failure, not
        // recast as a malformed response id: the caller learns which part is wrong.
        let node_tag = NodeTag::parse(node)?;
        let uuid = Uuid::parse_str(uuid).map_err(|_| Self::err())?;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
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

    pub fn as_str(&self) -> &'static str {
        // One source of truth: the strum derive above also drives the serde
        // renaming, so the wire name cannot drift from this one.
        self.into()
    }
}

/// Why a response ended [`ResponseStatus::Incomplete`].
///
/// The set is closed at two values, and **neither is about tool calls**: a turn
/// that stops so the caller can run a function is `completed`, carrying a
/// `function_call` item in its output. Reading `incomplete` as "the model wants
/// something" is therefore a category error — it means the output was cut off.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, IntoStaticStr)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum IncompleteReason {
    /// The output budget ran out mid-answer.
    MaxOutputTokens,
    /// A content filter withheld the output.
    ContentFilter,
}

impl IncompleteReason {
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

/// The reason envelope for an incomplete response.
///
/// A struct wrapping a single field because that is the protocol shape, and
/// because it leaves room for the reason set to grow without every caller
/// having to change how it reads the field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncompleteDetails {
    pub reason: IncompleteReason,
}

impl IncompleteDetails {
    pub fn new(reason: IncompleteReason) -> Self {
        Self { reason }
    }
}

/// Why a turn ended [`ResponseStatus::Failed`].
///
/// A struct wrapping the message because that is the protocol shape, and because
/// it leaves the envelope room to grow (`code`) without every reader changing
/// how it reads the field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResponseError {
    pub message: String,
}

/// Ceiling on a persisted failure message: a provider that dumps its whole
/// response body into an error must not be able to bloat the record or the wire
/// payload, and the head is what identifies the fault.
const MAX_ERROR_MESSAGE_CHARS: usize = 512;

/// A terminal transition: the status, and the official reason when that status
/// is `incomplete`.
///
/// One type rather than a `(ResponseStatus, Option<IncompleteDetails>)` pair,
/// so "incomplete without a reason" and "completed with one" are both
/// unrepresentable. Terminal transitions flow through this, which is what lets
/// a caller branch exhaustively instead of falling back on a catch-all arm.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Terminal {
    /// The turn produced its output. Includes a turn that stopped so its caller
    /// can run the function calls it requested: those calls **are** the output.
    Completed,
    /// The output was cut short, for one of the two official reasons.
    Incomplete { reason: IncompleteReason },
    /// The turn or its upstream broke, carrying the reason it broke with.
    ///
    /// The reason is required: a failure that loses its wording is not a lesser
    /// failure, it is an undiagnosable one — which is how a chain once reported
    /// every fault as "upstream reported a failure without a message".
    Failed { message: String },
    /// Cancelled by the caller or reclaimed by the sweeper.
    Cancelled,
}

impl Terminal {
    pub fn incomplete(reason: IncompleteReason) -> Self {
        Self::Incomplete { reason }
    }

    pub fn failed(message: impl std::fmt::Display) -> Self {
        let message = message.to_string();
        let message = match message.char_indices().nth(MAX_ERROR_MESSAGE_CHARS) {
            Some((cut, _)) => format!("{}…", &message[..cut]),
            None => message,
        };
        Self::Failed { message }
    }

    pub fn status(&self) -> ResponseStatus {
        match self {
            Self::Completed => ResponseStatus::Completed,
            Self::Incomplete { .. } => ResponseStatus::Incomplete,
            Self::Failed { .. } => ResponseStatus::Failed,
            Self::Cancelled => ResponseStatus::Cancelled,
        }
    }

    /// The reason envelope to persist, present exactly for `incomplete`.
    pub fn incomplete_details(&self) -> Option<IncompleteDetails> {
        match self {
            Self::Incomplete { reason } => Some(IncompleteDetails::new(*reason)),
            _ => None,
        }
    }

    /// The error envelope to persist, present exactly for `failed`.
    pub fn error(&self) -> Option<ResponseError> {
        match self {
            Self::Failed { message } => Some(ResponseError { message: message.clone() }),
            _ => None,
        }
    }
}

/// Where a response inherits its context from (D30).
///
/// The three variants replace D24's materialised snapshot: history is read from a
/// single authoritative source — the conversation snapshot for anchored turns, or
/// resolved through a previous response — rather than copied onto every record.
///
/// Held as **one field** on the record rather than a `previous_response_id` and a
/// `conversation_id` side by side. Those two could both be set, and something had
/// to decide which won; it did so silently. Here the choice is not expressible.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ContextAnchor {
    /// No prior context.
    Root,
    /// Continue from a previous response (bare chain, no conversation anchor).
    Previous(ResponseId),
    /// Continue from a conversation's materialised snapshot.
    Conversation(ConversationId),
}

impl ContextAnchor {
    pub fn conversation_id(&self) -> Option<&ConversationId> {
        match self {
            ContextAnchor::Conversation(id) => Some(id),
            _ => None,
        }
    }

    pub fn previous_response_id(&self) -> Option<&ResponseId> {
        match self {
            ContextAnchor::Previous(id) => Some(id),
            _ => None,
        }
    }

    pub fn is_root(&self) -> bool {
        matches!(self, ContextAnchor::Root)
    }
}

/// The parameters of one model call.
///
/// Shared by [`TurnSpec`] (what the caller asked to run and store) and the execution
/// side's task (what is actually sent to the model), because they are the same four
/// facts. They used to be copied field by field from the record onto the task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelParams {
    pub model: String,

    /// A system/developer prompt. Echoed on retrieval, **never** fed into context
    /// assembly on a later turn (INV-49).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// Functions offered to the model this turn, declared per request rather than by
    /// static deployment config. Stored in the shape the caller sent; provider
    /// translation happens inside the runner (single source of truth).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<Tool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,

    /// Caller-supplied passthrough key-values (e.g. agent template selection).
    /// Echoed on retrieval, **never** fed into context assembly (INV-49): the
    /// executor reads it to decide *how* to run the turn, not what the turn says.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, MetadataValue>,

    /// Extra skills (ids) the caller asks to offer on top of the agent's
    /// configured `allowed_skills`. The executor reads it to build the skills
    /// section; it is not context itself.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_skills: Vec<String>,
}

impl ModelParams {
    /// The common case: a model and nothing else.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            instructions: None,
            tools: Vec::new(),
            tool_choice: None,
            metadata: BTreeMap::new(),
            extra_skills: Vec::new(),
        }
    }
}

/// What the caller asked for, for one generation.
///
/// This is the whole caller-declared surface of a turn: the wire request has been
/// resolved into domain values (the `input` shorthand expanded, the conversation
/// reference parsed into an anchor) but nothing has been decided by the service yet.
/// It is passed to [`crate::service::ResponsesService::create`] and stored verbatim on
/// the record, so the two cannot disagree about what was requested — previously the
/// same six fields were copied across by hand, and a seventh (the anchor) was split
/// into two that could contradict each other.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnSpec {
    #[serde(flatten)]
    pub params: ModelParams,

    pub input_items: Vec<ResponseItem>,

    /// Persist for later chaining. When false the record is not retained and cannot
    /// be referenced as a previous link (FR-18).
    pub store: bool,

    /// Where this turn inherits its context from.
    pub anchor: ContextAnchor,
}

/// A persisted response record: ledger metadata for one generation.
///
/// Carries **no materialised ancestor snapshot**. Long-term history lives in the
/// conversation's snapshot (D30); this record holds what identifies the response,
/// what was asked of it ([`TurnSpec`]), and the bookkeeping the lifecycle needs.
/// Output is appended to the conversation snapshot at terminal and is
/// reconstructable from the event stream within the retention window.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseRecord {
    pub response_id: ResponseId,
    pub tenant_id: TenantId,

    /// What the caller asked for. Stored as the caller's own value, so "what was
    /// requested" has one representation across the create call, the record and
    /// the rendered object.
    pub spec: TurnSpec,

    /// Execution-side identity context (uid/pid/conversation_id) the caller
    /// attached for the executor to read. Persisted so a crashed run recovers
    /// it, but never echoed on the response object, never written to the
    /// conversation snapshot, and never fed into context assembly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exec_context: Option<Value>,

    pub status: ResponseStatus,
    #[serde(default)]
    pub usage: Usage,

    /// Why the turn ended `incomplete`. Present exactly when [`Self::status`] is
    /// [`ResponseStatus::Incomplete`], because both are written from one
    /// [`Terminal`] value — the same reason `integrity` keeps its algorithm and
    /// tag together rather than as two fields that could disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incomplete_details: Option<IncompleteDetails>,

    /// Why the turn ended `failed`. Present exactly when [`Self::status`] is
    /// [`ResponseStatus::Failed`], for the same reason `incomplete_details`
    /// trails its own status: both are written from one [`Terminal`] value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

    /// Reasoning / thinking text streamed by a reasoning model this turn,
    /// concatenated into one string. Render-only, never fed back as context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,

    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,

    /// Deadline after which stored content is swept. Present exactly when
    /// [`TurnSpec::store`] is set — it is that decision's consequence, computed
    /// once at create time from the configured retention.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,

    /// Integrity tag over the canonical encoding of the items. Algorithm and tag
    /// are one fact — either both present or both absent — so they live in a
    /// single `Option` rather than two parallel fields that could disagree.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integrity: Option<IntegrityTag>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<IdempotencyKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<AgentId>,
    #[serde(default)]
    pub attempt: Attempt,
}

impl ResponseRecord {
    /// A freshly created record in `Queued` state.
    ///
    /// Assembling it here rather than at the call site is what keeps
    /// `expires_at_ms` tied to `spec.store`: the two cannot be set
    /// inconsistently because only one of them is an input.
    pub fn queued(
        response_id: ResponseId,
        tenant_id: TenantId,
        spec: TurnSpec,
        idempotency_key: IdempotencyKey,
        now_ms: u64,
        content_retention_ms: u64,
    ) -> Self {
        let expires_at_ms = spec
            .store
            .then(|| now_ms.saturating_add(content_retention_ms));
        Self {
            response_id,
            tenant_id,
            spec,
            exec_context: None,
            status: ResponseStatus::Queued,
            usage: Usage::default(),
            incomplete_details: None,
            error: None,
            reasoning: None,
            created_at_ms: now_ms,
            completed_at_ms: None,
            expires_at_ms,
            integrity: None,
            idempotency_key: Some(idempotency_key),
            owner: None,
            attempt: Attempt::UNCLAIMED,
        }
    }

    /// Where this response inherits its context from.
    pub fn anchor(&self) -> &ContextAnchor {
        &self.spec.anchor
    }

    /// Conversation this response belongs to, if any (D28).
    pub fn conversation_id(&self) -> Option<&ConversationId> {
        self.spec.anchor.conversation_id()
    }

    pub fn previous_response_id(&self) -> Option<&ResponseId> {
        self.spec.anchor.previous_response_id()
    }

    pub fn is_stored(&self) -> bool {
        self.spec.store
    }

    /// Whether this record may be used as `previous_response_id` by `tenant`.
    pub fn is_referencable_by(&self, tenant: &TenantId) -> bool {
        self.is_stored() && &self.tenant_id == tenant
    }
}

/// A content integrity tag together with the algorithm that produced it (INV-44).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrityTag {
    pub alg: String,
    pub tag: String,
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
    fn a_terminal_carries_a_reason_exactly_when_it_is_incomplete() {
        // The invariant the type exists for: status and reason are one fact, so
        // "incomplete with no reason" and "completed with one" cannot be built.
        for terminal in [
            Terminal::Completed,
            Terminal::incomplete(IncompleteReason::MaxOutputTokens),
            Terminal::incomplete(IncompleteReason::ContentFilter),
            Terminal::failed("boom"),
            Terminal::Cancelled,
        ] {
            assert_eq!(
                terminal.incomplete_details().is_some(),
                terminal.status() == ResponseStatus::Incomplete,
                "status and reason disagree for {terminal:?}"
            );
        }
    }

    #[test]
    fn every_terminal_maps_to_a_terminal_status() {
        for terminal in [
            Terminal::Completed,
            Terminal::incomplete(IncompleteReason::ContentFilter),
            Terminal::failed("boom"),
            Terminal::Cancelled,
        ] {
            assert!(
                terminal.status().is_terminal(),
                "{terminal:?} must not map to a live status"
            );
        }
    }

    #[test]
    fn a_failure_carries_its_reason_and_only_a_failure_carries_one() {
        // What the chain could not see before: `chain.end` had no way to name
        // the fault and fell back to a placeholder for every failure.
        for terminal in [
            Terminal::Completed,
            Terminal::incomplete(IncompleteReason::ContentFilter),
            Terminal::failed("model produced no output items"),
            Terminal::Cancelled,
        ] {
            assert_eq!(
                terminal.error().is_some(),
                terminal.status() == ResponseStatus::Failed,
                "status and error disagree for {terminal:?}"
            );
        }
        assert_eq!(
            Terminal::failed("model produced no output items").error(),
            Some(ResponseError {
                message: "model produced no output items".into()
            })
        );
    }

    #[test]
    fn a_failure_reason_is_bounded_so_a_provider_dump_cannot_bloat_the_record() {
        let terminal = Terminal::failed("x".repeat(10_000));
        let message = terminal.error().unwrap().message;
        assert!(message.len() <= MAX_ERROR_MESSAGE_CHARS + 4);
        assert!(message.ends_with('…'));
    }

    #[test]
    fn a_tool_call_round_is_completed_not_incomplete() {
        // Officially the two incomplete reasons are about truncation only, so a
        // turn stopping for its caller to run functions is a completed turn.
        assert_eq!(Terminal::Completed.status(), ResponseStatus::Completed);
        assert!(Terminal::Completed.incomplete_details().is_none());
    }

    #[test]
    fn incomplete_reason_wire_names_match_the_official_spelling() {
        assert_eq!(IncompleteReason::MaxOutputTokens.as_str(), "max_output_tokens");
        assert_eq!(IncompleteReason::ContentFilter.as_str(), "content_filter");
        assert_eq!(
            serde_json::to_value(IncompleteDetails::new(IncompleteReason::MaxOutputTokens))
                .expect("serialises"),
            serde_json::json!({"reason": "max_output_tokens"})
        );
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
        for bad in ["abc", "resp_nouuid", "resp_node-a_not-a-uuid"] {
            let err = ResponseId::parse(bad).expect_err("must be rejected");
            assert_eq!(err.kind, "response id", "{bad}");
        }
        // A bad node tag is reported as a bad node tag, not disguised as a
        // malformed id: the caller is told which part to fix.
        let err = ResponseId::parse("resp_NODE_00000000-0000-0000-0000-000000000000")
            .expect_err("must be rejected");
        assert_eq!(err.kind, "node tag");
        // Path traversal attempt inside the id.
        assert!(ResponseId::parse("resp_../../etc_x").is_err());
    }

    fn spec(anchor: ContextAnchor, store: bool) -> TurnSpec {
        TurnSpec {
            params: ModelParams {
                instructions: Some("secret system prompt".into()),
                ..ModelParams::new("m")
            },
            input_items: vec![ResponseItem::user_text("in")],
            store,
            anchor,
        }
    }

    fn record(store: bool, tenant_id: &str) -> ResponseRecord {
        ResponseRecord::queued(
            ResponseId::new(tag("n1")),
            tenant(tenant_id),
            spec(ContextAnchor::Root, store),
            IdempotencyKey::parse("k").unwrap(),
            0,
            1_000,
        )
    }

    #[test]
    fn the_anchor_is_the_only_place_context_provenance_lives() {
        let root = record(true, "t1");
        assert_eq!(root.anchor(), &ContextAnchor::Root);
        assert_eq!(root.conversation_id(), None);
        assert_eq!(root.previous_response_id(), None);

        let prev = ResponseId::new(tag("n1"));
        let mut chained = record(true, "t1");
        chained.spec.anchor = ContextAnchor::Previous(prev.clone());
        assert_eq!(chained.previous_response_id(), Some(&prev));
        assert_eq!(chained.conversation_id(), None);

        let conv = ConversationId::new();
        let mut anchored = record(true, "t1");
        anchored.spec.anchor = ContextAnchor::Conversation(conv.clone());
        assert_eq!(anchored.conversation_id(), Some(&conv));
        // A conversation anchor is not also a bare chain: the two used to be
        // separate fields and one silently shadowed the other.
        assert_eq!(anchored.previous_response_id(), None);
    }

    #[test]
    fn retention_is_derived_from_the_store_decision() {
        assert_eq!(record(true, "t1").expires_at_ms, Some(1_000));
        assert_eq!(record(false, "t1").expires_at_ms, None);
    }

    #[test]
    fn referencability_requires_store_and_same_tenant() {
        assert!(record(true, "t1").is_referencable_by(&tenant("t1")));
        assert!(!record(false, "t1").is_referencable_by(&tenant("t1")));
        assert!(!record(true, "t1").is_referencable_by(&tenant("t2")));
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
    fn status_wire_name_has_one_source() {
        for status in [ResponseStatus::InProgress, ResponseStatus::Cancelled] {
            assert_eq!(
                serde_json::to_string(&status).unwrap(),
                format!("\"{}\"", status.as_str())
            );
        }
    }

    #[test]
    fn model_params_are_one_value_shared_with_the_execution_side() {
        // They used to be four fields copied from the record onto the task; a fifth
        // parameter would have had to be added in both places.
        let spec = spec(ContextAnchor::Root, true);
        let params = spec.params.clone();
        assert_eq!(params.model, "m");
        // Flattened on the wire, so the record's shape is unchanged by the grouping.
        let json = serde_json::to_value(&spec).unwrap();
        assert_eq!(json["model"], "m");
        assert_eq!(json["instructions"], "secret system prompt");
        assert!(json.get("params").is_none(), "{json}");
        assert_eq!(serde_json::from_value::<TurnSpec>(json).unwrap(), spec);
    }

    #[test]
    fn record_round_trips_through_serde() {
        let mut r = record(true, "t1");
        r.spec.anchor = ContextAnchor::Conversation(ConversationId::new());
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<ResponseRecord>(&json).unwrap(), r);
    }

    #[test]
    fn exec_context_round_trips_on_the_record_and_defaults_to_absent() {
        let mut r = record(true, "t1");
        r.exec_context = Some(serde_json::json!({"uid": "u-1", "pid": "p-1"}));
        let json = serde_json::to_string(&r).unwrap();
        assert_eq!(serde_json::from_str::<ResponseRecord>(&json).unwrap(), r);

        // Unset stays absent on the wire, so the common case is unchanged.
        let bare = serde_json::to_value(record(true, "t1")).unwrap();
        assert!(bare.get("exec_context").is_none(), "{bare}");
    }
}

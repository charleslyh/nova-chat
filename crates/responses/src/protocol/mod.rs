//! Closed protocol subset (D22).
//!
//! This module defines the *entire* externally accepted surface, and the objects
//! rendered back out. Anything not expressible with these types is rejected with a
//! 400 — never silently ignored and never passed through (INV-50).
//!
//! Design rules that must not be relaxed:
//!
//! 1. **Closed enums.** Item and content variants are exhaustive. Unknown `type`
//!    values fail deserialisation, which *is* the expected behaviour.
//! 2. **`deny_unknown_fields` everywhere.** Matches upstream behaviour, which also
//!    rejects unrecognised request arguments.
//! 3. **No `untagged` fallback on accepted input.** It would swallow the precise
//!    error location; the two genuine unions (`input`, `conversation`,
//!    `tool_choice`) use hand-written visitors so that a malformed item inside an
//!    array reports *that item's* error. (`untagged` is used for one internal
//!    carrier type outside this module — see [`crate::EventBody`] — where the input
//!    is never caller-supplied.)
//! 4. **No inline binary.** Images and files are references only, so the chain byte
//!    budget stays meaningful (see `parameters.md` §4.5).
//! 5. **Rendered objects are structs.** A `json!` literal cannot notice a field it
//!    forgot; [`ResponseObject`] can.

mod content;
mod conversation;
mod item;
mod limits;
mod metadata;
mod request;
mod response_object;
mod tool;
mod url_guard;

pub use content::{ContentPart, ContentViolation, ImageDetail};
pub use metadata::MetadataValue;
pub use conversation::{
    AppendBusinessEventRequest, BusinessEventViolation, ConversationMetadataRequest,
};
pub use item::{ItemStatus, ItemViolation, ResponseItem, Role};
pub use limits::{json_depth, LimitViolation, ProtocolLimits};
pub use request::{
    preflight_unsupported, validate_metadata, ConversationRef, CreateResponseRequest,
    RequestViolation, ResponseInput,
};
pub use response_object::{ObjectKind, ResponseObject};
pub use tool::{Tool, ToolChoice, ToolChoiceMode};
pub use url_guard::{ensure_public_https, is_blocked_ip, UrlRejection};

/// Upstream specification revision this subset was extracted from.
///
/// Recorded so the published subset contract can state *which* upstream revision it
/// is compatible with (D22 ④). CI diffs upstream against this marker and **warns** —
/// it is a product input for "should we widen the subset", not a correctness gate,
/// and must never block the build.
pub const UPSTREAM_SPEC_SOURCE: &str = "github.com/openai/openai-openapi";
pub const UPSTREAM_SPEC_REVISION: &str = "2025-08-07";

/// Request fields that belong to upstream but are deliberately outside this subset.
/// Presence of any of these produces a *specific* error message pointing at the
/// supported alternative, rather than a generic "unknown field" — these are the
/// fields callers are most likely to try.
///
/// `conversation` used to be listed here. It no longer is: the field is accepted
/// (D27). What it is *not* is a container of items — it names a conversation whose
/// tail pointer identifies the chain to inherit. The `items` sub-resource remains
/// outside the subset, and asking for it gets a 404 from routing rather than an
/// entry here, because a missing sub-resource is not a misused request field.
pub const EXPLICITLY_UNSUPPORTED_FIELDS: &[(&str, &str)] = &[
    (
        "context_management",
        "server-side context compaction is not supported; manage history length via previous_response_id chains",
    ),
    (
        "prompt",
        "stored prompt templates are not supported; send instructions and input directly",
    ),
];

//! Closed protocol subset (D22).
//!
//! This module defines the *entire* externally accepted surface. Anything not
//! expressible with these types is rejected with a 400 — never silently
//! ignored and never passed through (INV-50).
//!
//! Design rules that must not be relaxed:
//!
//! 1. **Closed enums.** Item and content variants are exhaustive. Unknown
//!    `type` values fail deserialisation, which *is* the expected behaviour.
//! 2. **`deny_unknown_fields` everywhere.** Matches upstream behaviour, which
//!    also rejects unrecognised request arguments.
//! 3. **No `flatten`, no `untagged` fallback.** Both would swallow the precise
//!    error location; `input` uses a hand-written visitor instead so that a
//!    malformed item inside an array reports *that item's* error.
//! 4. **No inline binary.** Images and files are references only, so the chain
//!    byte budget stays meaningful (see `parameters.md` §4.5).

mod content;
mod conversation;
mod item;
mod limits;
mod request;
mod session;
mod url_guard;

pub use content::{ContentPart, ContentViolation, ImageDetail};
pub use conversation::{CreateConversationRequest, UpdateConversationRequest};
pub use item::{ItemStatus, ItemViolation, ResponseItem, Role};
pub use limits::{json_depth, InputLimits, LimitViolation};
pub use request::{
    preflight_unsupported, validate_metadata, ConversationRef, CreateResponseRequest,
    RequestViolation, ResponseInput, Tool, ToolChoice, ToolChoiceMode, MAX_INSTRUCTIONS_BYTES,
    MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_VALUE_BYTES,
};
pub use session::{
    AppendBusinessEventRequest, CreateSessionRequest, SessionRequestViolation,
    MAX_BUSINESS_KIND_BYTES, MAX_BUSINESS_PAYLOAD_BYTES,
};
pub use url_guard::{ensure_public_https, is_blocked_ip, UrlRejection, MAX_URL_BYTES};

/// Upstream specification revision this subset was extracted from.
///
/// Recorded so the published subset contract can state *which* upstream
/// revision it is compatible with (D22 ④). CI diffs upstream against this
/// marker and **warns** — it is a product input for "should we widen the
/// subset", not a correctness gate, and must never block the build.
pub const UPSTREAM_SPEC_SOURCE: &str = "github.com/openai/openai-openapi";
pub const UPSTREAM_SPEC_REVISION: &str = "2025-08-07";

/// Request fields that belong to upstream but are deliberately outside this
/// subset. Presence of any of these produces a *specific* error message
/// pointing at the supported alternative, rather than a generic
/// "unknown field" — these are the fields callers are most likely to try.
///
/// `conversation` used to be listed here. It no longer is: the field is accepted
/// (D27). What it is *not* is a container of items — it names a conversation
/// whose tail pointer identifies the chain to inherit. The `items`
/// sub-resource remains outside the subset, and asking for it gets a 404 from
/// routing rather than an entry here, because a missing sub-resource is not a
/// misused request field.
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

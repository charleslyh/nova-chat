//! Reading the host's per-round transport values back off a task.
//!
//! The host stamps them at its HTTP boundary and the record carries them here,
//! so a client cannot forge them: this crate only ever READS the namespace,
//! never accepts it from a caller. It is equally opaque to WHAT the values
//! mean — a proxy's accounting id and a caller's key are both just headers
//! this round was asked to send.

use serde_json::Value;

use crate::wire::TransportOverrides;

/// The `exec_context` namespace holding this round's passthrough headers.
pub const LLM_TRANSPORT_EXT_KEY: &str = "llm_transport";

/// Key inside [`LLM_TRANSPORT_EXT_KEY`] holding a `{ name: value }` object.
const HEADERS_KEY: &str = "headers";

/// Read a round's passthrough headers out of `exec_context`. A missing
/// namespace yields empty overrides, which is the normal case for a round that
/// talks to an internal endpoint.
///
/// Malformed entries are skipped rather than failing the round: the upstream
/// deciding it needs a header is a better error than this crate refusing to
/// run. Values are never logged — the host may put a credential in one.
pub fn overrides_from_exec_context(exec_context: Option<&Value>) -> TransportOverrides {
    let Some(headers) = exec_context
        .and_then(|context| context.get(LLM_TRANSPORT_EXT_KEY))
        .and_then(|transport| transport.get(HEADERS_KEY))
        .and_then(Value::as_object)
    else {
        return TransportOverrides::new();
    };

    let mut overrides = TransportOverrides::new();
    for (name, value) in headers {
        let Some(value) = value.as_str() else {
            tracing::warn!(header = %name, "skipping a non-string passthrough header");
            continue;
        };
        overrides = overrides.with_header(name, value);
    }
    overrides
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_absent_namespace_yields_no_overrides() {
        assert!(overrides_from_exec_context(None).is_empty());
        assert!(overrides_from_exec_context(Some(&json!({}))).is_empty());
        assert!(
            overrides_from_exec_context(Some(&json!({ "uid": "u1" }))).is_empty(),
            "only the transport namespace is read"
        );
        assert!(overrides_from_exec_context(Some(&json!({ "llm_transport": {} }))).is_empty());
    }

    #[test]
    fn headers_are_read_back_verbatim() {
        let overrides = overrides_from_exec_context(Some(&json!({
            "llm_transport": {
                "headers": {
                    "x-litellm-customer-id": "alice",
                    "x-litellm-api-key": "sk-caller",
                }
            }
        })));
        assert_eq!(
            overrides.headers().collect::<Vec<_>>(),
            vec![
                ("x-litellm-api-key", "sk-caller"),
                ("x-litellm-customer-id", "alice"),
            ]
        );
    }

    #[test]
    fn a_non_string_value_is_skipped_not_fatal() {
        let overrides = overrides_from_exec_context(Some(&json!({
            "llm_transport": { "headers": { "ok": "v", "bad": 1 } }
        })));
        assert_eq!(overrides.headers().collect::<Vec<_>>(), vec![("ok", "v")]);
    }

    /// The namespace is host-written: a client must not be able to hand itself
    /// a credential by smuggling the key in somewhere else.
    #[test]
    fn only_the_transport_namespace_is_read() {
        let overrides = overrides_from_exec_context(Some(
            &json!({ "headers": { "authorization": "Bearer x" } }),
        ));
        assert!(overrides.is_empty());
    }
}

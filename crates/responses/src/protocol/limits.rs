//! Every bound on caller-supplied input, in one place (SEC-7 / FR-27).
//!
//! Bounds are enforced *after* structural parsing but *before* anything is stored
//! or forwarded. Defaults come from `parameters.md` §4.5.
//!
//! These used to be split in two: a configurable struct for item counts and
//! sizes, and a scatter of `pub const MAX_*` for instructions, metadata, URLs and
//! business payloads. Same kind of fact, two mechanisms, and only half of it
//! reachable by an operator. One struct now carries all of them, so "what is the
//! largest thing a caller may send" has a single answer.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use super::item::ResponseItem;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProtocolLimits {
    pub max_items: usize,
    pub max_item_bytes: usize,
    pub max_total_bytes: usize,
    pub max_json_depth: usize,

    /// A system/developer prompt is not an item, so it is bounded separately.
    pub max_instructions_bytes: usize,

    /// Upstream's own metadata constraints, applied identically to the create
    /// request and the conversation endpoints.
    pub max_metadata_entries: usize,
    pub max_metadata_key_bytes: usize,
    pub max_metadata_value_bytes: usize,

    /// Reference URLs on image/file parts, which the execution side will fetch.
    pub max_url_bytes: usize,

    /// Business events: a short caller-chosen label and a bounded opaque payload.
    /// Generous next to a metadata value because a business event legitimately
    /// carries a small object, tight next to a request body because anything
    /// larger belongs behind a reference.
    pub max_business_kind_bytes: usize,
    pub max_business_payload_bytes: usize,

    /// The private `ext` namespace on the create-request body: an opaque,
    /// execution-side payload (never echoed, never sent to the provider). Sized like
    /// a business payload — anything larger belongs behind a reference. Depth reuses
    /// `max_json_depth`.
    pub max_ext_bytes: usize,
}

impl Default for ProtocolLimits {
    fn default() -> Self {
        Self {
            max_items: 200,
            max_item_bytes: 256 * 1024,
            max_total_bytes: 1024 * 1024,
            max_json_depth: 32,
            max_instructions_bytes: 32 * 1024,
            max_metadata_entries: 16,
            max_metadata_key_bytes: 64,
            max_metadata_value_bytes: 512,
            max_url_bytes: 2048,
            max_business_kind_bytes: 64,
            max_business_payload_bytes: 16 * 1024,
            max_ext_bytes: 16 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LimitViolation {
    #[error("input has {actual} items, limit is {max}")]
    TooManyItems { actual: usize, max: usize },
    #[error("item {index} is {actual} bytes, limit is {max}")]
    ItemTooLarge {
        index: usize,
        actual: usize,
        max: usize,
    },
    #[error("input totals {actual} bytes, limit is {max}")]
    TotalTooLarge { actual: usize, max: usize },
    #[error("json nesting depth {actual} exceeds limit {max}")]
    TooDeep { actual: usize, max: usize },
    #[error("input must not be empty")]
    Empty,
}

impl ProtocolLimits {
    /// Returns the total byte size on success.
    pub fn validate_items(&self, items: &[ResponseItem]) -> Result<usize, LimitViolation> {
        if items.is_empty() {
            return Err(LimitViolation::Empty);
        }
        if items.len() > self.max_items {
            return Err(LimitViolation::TooManyItems {
                actual: items.len(),
                max: self.max_items,
            });
        }
        let mut total = 0usize;
        for (index, item) in items.iter().enumerate() {
            let len = item.byte_len();
            if len > self.max_item_bytes {
                return Err(LimitViolation::ItemTooLarge {
                    index,
                    actual: len,
                    max: self.max_item_bytes,
                });
            }
            total = total.saturating_add(len);
        }
        if total > self.max_total_bytes {
            return Err(LimitViolation::TotalTooLarge {
                actual: total,
                max: self.max_total_bytes,
            });
        }
        Ok(total)
    }

    /// Guard free-form JSON (tool parameter schemas, business payloads) against
    /// deeply nested documents designed to exhaust the stack.
    pub fn validate_depth(&self, value: &Value) -> Result<(), LimitViolation> {
        let actual = json_depth(value);
        if actual > self.max_json_depth {
            Err(LimitViolation::TooDeep {
                actual,
                max: self.max_json_depth,
            })
        } else {
            Ok(())
        }
    }
}

/// Iterative depth measurement — recursion here would reintroduce the very
/// stack-exhaustion problem we are guarding against.
pub fn json_depth(value: &Value) -> usize {
    let mut max_depth = 0usize;
    let mut stack = vec![(value, 1usize)];
    while let Some((node, depth)) = stack.pop() {
        max_depth = max_depth.max(depth);
        match node {
            Value::Array(items) => {
                for item in items {
                    stack.push((item, depth + 1));
                }
            }
            Value::Object(map) => {
                for item in map.values() {
                    stack.push((item, depth + 1));
                }
            }
            _ => {}
        }
    }
    max_depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ContentPart, Role};

    fn text_item(len: usize) -> ResponseItem {
        ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText {
                text: "a".repeat(len),
            }],
            id: None,
            status: None,
        }
    }

    #[test]
    fn accepts_within_limits() {
        let limits = ProtocolLimits::default();
        assert_eq!(limits.validate_items(&[text_item(10)]).unwrap(), 10);
    }

    #[test]
    fn rejects_empty_input() {
        assert_eq!(
            ProtocolLimits::default().validate_items(&[]),
            Err(LimitViolation::Empty)
        );
    }

    #[test]
    fn rejects_too_many_items() {
        let limits = ProtocolLimits {
            max_items: 2,
            ..ProtocolLimits::default()
        };
        let items: Vec<_> = (0..3).map(|_| text_item(1)).collect();
        assert!(matches!(
            limits.validate_items(&items),
            Err(LimitViolation::TooManyItems { actual: 3, max: 2 })
        ));
    }

    #[test]
    fn rejects_oversized_single_item_and_total() {
        let limits = ProtocolLimits {
            max_item_bytes: 10,
            max_total_bytes: 15,
            ..ProtocolLimits::default()
        };
        assert!(matches!(
            limits.validate_items(&[text_item(11)]),
            Err(LimitViolation::ItemTooLarge { .. })
        ));
        assert!(matches!(
            limits.validate_items(&[text_item(8), text_item(8)]),
            Err(LimitViolation::TotalTooLarge {
                actual: 16,
                max: 15
            })
        ));
    }

    #[test]
    fn measures_depth_iteratively() {
        assert_eq!(json_depth(&serde_json::json!(1)), 1);
        assert_eq!(json_depth(&serde_json::json!({"a": 1})), 2);
        assert_eq!(json_depth(&serde_json::json!({"a": {"b": [1]}})), 4);
    }

    #[test]
    fn rejects_deep_json() {
        let mut value = Value::Null;
        for _ in 0..40 {
            value = Value::Array(vec![value]);
        }
        assert!(matches!(
            ProtocolLimits::default().validate_depth(&value),
            Err(LimitViolation::TooDeep { max: 32, .. })
        ));
    }

    #[test]
    fn depth_measurement_is_iterative_not_recursive() {
        // Deliberately deeper than any recursive measurement could survive with a
        // small stack, but still within what serde_json will hand us.
        let mut value = Value::Null;
        for _ in 0..120 {
            value = Value::Array(vec![value]);
        }
        assert_eq!(json_depth(&value), 121);
    }

    #[test]
    fn parser_itself_rejects_pathologically_deep_input() {
        // Defence in depth: serde_json enforces its own recursion limit, so a
        // hostile payload never even reaches `validate_depth`. This test pins that
        // assumption — if it ever stops holding, the depth guard alone would not be
        // enough to prevent stack exhaustion during parsing.
        let hostile = format!("{}null{}", "[".repeat(2000), "]".repeat(2000));
        assert!(
            serde_json::from_str::<Value>(&hostile).is_err(),
            "parser must reject deeply nested input before it is measured"
        );
    }

    #[test]
    fn every_bound_is_operator_reachable() {
        // The reason this struct absorbed the former `MAX_*` constants: a bound an
        // operator cannot change is not a policy, it is a hard-coded opinion.
        let text = r#"
            max_items = 5
            max_instructions_bytes = 10
            max_metadata_entries = 2
            max_url_bytes = 64
            max_business_payload_bytes = 128
        "#;
        let limits: ProtocolLimits = toml::from_str(text).expect("parse");
        assert_eq!(limits.max_items, 5);
        assert_eq!(limits.max_instructions_bytes, 10);
        assert_eq!(limits.max_metadata_entries, 2);
        assert_eq!(limits.max_url_bytes, 64);
        assert_eq!(limits.max_business_payload_bytes, 128);
        // Untouched fields keep the documented default rather than falling to zero.
        assert_eq!(
            limits.max_json_depth,
            ProtocolLimits::default().max_json_depth
        );
    }
}

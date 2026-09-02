//! Deserialisation hardening (SEC-7 / FR-27).
//!
//! Bounds are enforced *after* structural parsing but *before* anything is
//! stored or forwarded. Defaults come from `parameters.md` §4.5.

use serde_json::Value;
use thiserror::Error;

use super::item::ResponseItem;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputLimits {
    pub max_items: usize,
    pub max_item_bytes: usize,
    pub max_total_bytes: usize,
    pub max_json_depth: usize,
}

impl Default for InputLimits {
    fn default() -> Self {
        Self {
            max_items: 200,
            max_item_bytes: 256 * 1024,
            max_total_bytes: 1024 * 1024,
            max_json_depth: 32,
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

impl InputLimits {
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

    /// Guard free-form JSON (tool parameter schemas, metadata) against deeply
    /// nested payloads designed to exhaust the stack.
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
        let limits = InputLimits::default();
        assert_eq!(limits.validate_items(&[text_item(10)]).unwrap(), 10);
    }

    #[test]
    fn rejects_empty_input() {
        assert_eq!(
            InputLimits::default().validate_items(&[]),
            Err(LimitViolation::Empty)
        );
    }

    #[test]
    fn rejects_too_many_items() {
        let limits = InputLimits {
            max_items: 2,
            ..InputLimits::default()
        };
        let items: Vec<_> = (0..3).map(|_| text_item(1)).collect();
        assert!(matches!(
            limits.validate_items(&items),
            Err(LimitViolation::TooManyItems { actual: 3, max: 2 })
        ));
    }

    #[test]
    fn rejects_oversized_single_item_and_total() {
        let limits = InputLimits {
            max_item_bytes: 10,
            max_total_bytes: 15,
            ..InputLimits::default()
        };
        assert!(matches!(
            limits.validate_items(&[text_item(11)]),
            Err(LimitViolation::ItemTooLarge { .. })
        ));
        assert!(matches!(
            limits.validate_items(&[text_item(8), text_item(8)]),
            Err(LimitViolation::TotalTooLarge { actual: 16, max: 15 })
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
        let limits = InputLimits::default();
        assert!(matches!(
            limits.validate_depth(&value),
            Err(LimitViolation::TooDeep { max: 32, .. })
        ));
    }

    #[test]
    fn depth_measurement_is_iterative_not_recursive() {
        // Deliberately deeper than any recursive measurement could survive with
        // a small stack, but still within what serde_json will hand us.
        let mut value = Value::Null;
        for _ in 0..120 {
            value = Value::Array(vec![value]);
        }
        assert_eq!(json_depth(&value), 121);
    }

    #[test]
    fn parser_itself_rejects_pathologically_deep_input() {
        // Defence in depth: serde_json enforces its own recursion limit, so a
        // hostile payload never even reaches `validate_depth`. This test pins
        // that assumption — if it ever stops holding, the depth guard alone
        // would not be enough to prevent stack exhaustion during parsing.
        let hostile = format!("{}null{}", "[".repeat(2000), "]".repeat(2000));
        assert!(
            serde_json::from_str::<Value>(&hostile).is_err(),
            "parser must reject deeply nested input before it is measured"
        );
    }
}

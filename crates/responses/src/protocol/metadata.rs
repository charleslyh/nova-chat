//! The metadata value type shared by the create-response and conversation
//! endpoints.

use serde::{Deserialize, Serialize};
use serde_json::Number;

/// One metadata value. Upstream permits exactly a string, boolean or number —
/// `null`, objects and arrays are rejected, matching the closed-protocol rule
/// (INV-50).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataValue {
    String(String),
    Number(Number),
    Bool(bool),
}

impl MetadataValue {
    /// The string value, or `None` for a number or boolean. The common read for
    /// executors that dispatch on string tags (e.g. agent template selection).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            MetadataValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Byte length of the value in its serialised form, so the size bound applies
    /// to what is stored rather than a string-specific measure.
    pub fn byte_len(&self) -> usize {
        match self {
            MetadataValue::String(s) => s.len(),
            MetadataValue::Number(n) => n.to_string().len(),
            MetadataValue::Bool(b) => {
                if *b {
                    4
                } else {
                    5
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_each_allowed_scalar() {
        let cases = [
            (r#""hello""#, MetadataValue::String("hello".into())),
            (r#"42"#, MetadataValue::Number(Number::from(42))),
            (
                r#"1.5"#,
                MetadataValue::Number(Number::from_f64(1.5).unwrap()),
            ),
            (r#"true"#, MetadataValue::Bool(true)),
            (r#"false"#, MetadataValue::Bool(false)),
        ];
        for (json, expected) in cases {
            let parsed: MetadataValue = serde_json::from_str(json).unwrap();
            assert_eq!(parsed, expected, "{json}");
            assert_eq!(serde_json::to_string(&parsed).unwrap(), json, "{json}");
        }
    }

    #[test]
    fn rejects_null_objects_and_arrays() {
        for bad in ["null", "{}", "[]"] {
            assert!(
                serde_json::from_str::<MetadataValue>(bad).is_err(),
                "must reject {bad}"
            );
        }
    }

    #[test]
    fn byte_len_measures_the_serialised_form() {
        assert_eq!(MetadataValue::String("abc".into()).byte_len(), 3);
        assert_eq!(MetadataValue::Number(Number::from(1024)).byte_len(), 4);
        assert_eq!(MetadataValue::Bool(true).byte_len(), 4);
        assert_eq!(MetadataValue::Bool(false).byte_len(), 5);
    }
}

//! Tool declarations and tool selection.
//!
//! These are not request-only types: a record stores the tools a turn offered, the
//! rendered response object echoes them, and the runner reads them. They therefore
//! live in a module of their own rather than inside the create-request envelope
//! that merely happens to be where a caller first types them.

use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::fmt;

use serde::de::{MapAccess, Visitor};

/// The closed set of tools. Hosted tool types (`web_search`, `mcp_*`, …) are
/// outside the subset, so naming one fails deserialisation — which *is* the
/// intended behaviour (INV-50).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Tool {
    Function {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        /// JSON Schema; opaque to this service but depth-checked (SEC-7).
        parameters: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        strict: Option<bool>,
    },
}

impl Tool {
    pub fn name(&self) -> &str {
        match self {
            Tool::Function { name, .. } => name,
        }
    }

    pub fn parameters(&self) -> &Value {
        match self {
            Tool::Function { parameters, .. } => parameters,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoiceMode {
    Auto,
    None,
    Required,
}

/// `tool_choice` is a genuine union: a mode string, or a named function.
#[derive(Debug, Clone, PartialEq)]
pub enum ToolChoice {
    Mode(ToolChoiceMode),
    Function { name: String },
}

impl Serialize for ToolChoice {
    /// Written out rather than derived `untagged`, which would emit the function form
    /// as a bare `{"name":"f"}` — dropping the `type` discriminator that its own
    /// deserialiser requires. The value would then fail to round-trip, and since the
    /// rendered response object echoes `tool_choice`, a client reading one back and
    /// resending it would be rejected.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ToolChoice::Mode(mode) => mode.serialize(s),
            ToolChoice::Function { name } => {
                use serde::ser::SerializeStruct;
                let mut st = s.serialize_struct("ToolChoice", 2)?;
                st.serialize_field("type", "function")?;
                st.serialize_field("name", name)?;
                st.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ToolChoice {
    /// Hand-written rather than `untagged`, which reports "data did not match any
    /// variant" and discards the real error.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ChoiceVisitor;

        impl<'de> Visitor<'de> for ChoiceVisitor {
            type Value = ToolChoice;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(r#""auto" | "none" | "required" | {"type":"function","name":"…"}"#)
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                match v {
                    "auto" => Ok(ToolChoice::Mode(ToolChoiceMode::Auto)),
                    "none" => Ok(ToolChoice::Mode(ToolChoiceMode::None)),
                    "required" => Ok(ToolChoice::Mode(ToolChoiceMode::Required)),
                    other => Err(E::unknown_variant(other, &["auto", "none", "required"])),
                }
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut kind: Option<String> = None;
                let mut name: Option<String> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "type" => kind = Some(map.next_value()?),
                        "name" => name = Some(map.next_value()?),
                        other => return Err(de::Error::unknown_field(other, &["type", "name"])),
                    }
                }
                match kind.as_deref() {
                    Some("function") => {
                        let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                        Ok(ToolChoice::Function { name })
                    }
                    Some(other) => Err(de::Error::unknown_variant(other, &["function"])),
                    None => Err(de::Error::missing_field("type")),
                }
            }
        }

        deserializer.deserialize_any(ChoiceVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_choice_accepts_both_shapes() {
        let mode: ToolChoice = serde_json::from_str(r#""required""#).unwrap();
        assert_eq!(mode, ToolChoice::Mode(ToolChoiceMode::Required));
        let func: ToolChoice = serde_json::from_str(r#"{"type":"function","name":"f"}"#).unwrap();
        assert_eq!(func, ToolChoice::Function { name: "f".into() });
        assert!(serde_json::from_str::<ToolChoice>(r#""whatever""#).is_err());
        assert!(serde_json::from_str::<ToolChoice>(r#"{"type":"mcp","name":"f"}"#).is_err());
    }

    #[test]
    fn tool_choice_round_trips() {
        for json in [r#""auto""#, r#"{"type":"function","name":"f"}"#] {
            let parsed: ToolChoice = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
        }
    }

    #[test]
    fn rejects_hosted_tool_types() {
        assert!(serde_json::from_str::<Tool>(r#"{"type":"web_search"}"#).is_err());
        let tool: Tool =
            serde_json::from_str(r#"{"type":"function","name":"f","parameters":{}}"#).unwrap();
        assert_eq!(tool.name(), "f");
    }

    #[test]
    fn rejects_unknown_field_on_a_function_tool() {
        assert!(serde_json::from_str::<Tool>(
            r#"{"type":"function","name":"f","parameters":{},"extra":1}"#
        )
        .is_err());
    }
}

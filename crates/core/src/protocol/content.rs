//! Content parts — closed set (D22 §subset).
//!
//! Inline binary is absent *by construction*: images and files carry a file id
//! or an `https` reference, never bytes. That is what keeps the chain byte
//! budget (`parameters.md` §4.5) meaningful.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::url_guard::{ensure_public_https, UrlRejection};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContentPart {
    InputText {
        text: String,
    },
    OutputText {
        text: String,
    },
    Refusal {
        refusal: String,
    },
    /// Reference only. `image_url` must be public https; base64 data URLs are
    /// rejected by the scheme check.
    InputImage {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        image_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    InputFile {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        file_url: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ContentViolation {
    #[error("{part} requires either a file id or a url reference")]
    MissingReference { part: &'static str },
    #[error("{part} url rejected: {source}")]
    RejectedUrl {
        part: &'static str,
        #[source]
        source: UrlRejection,
    },
}

impl ContentPart {
    pub fn part_type(&self) -> &'static str {
        match self {
            ContentPart::InputText { .. } => "input_text",
            ContentPart::OutputText { .. } => "output_text",
            ContentPart::Refusal { .. } => "refusal",
            ContentPart::InputImage { .. } => "input_image",
            ContentPart::InputFile { .. } => "input_file",
        }
    }

    /// Byte cost counted against the chain budget. References are short, which
    /// is precisely why the 1 MiB chain ceiling remains effective.
    pub fn byte_len(&self) -> usize {
        match self {
            ContentPart::InputText { text } | ContentPart::OutputText { text } => text.len(),
            ContentPart::Refusal { refusal } => refusal.len(),
            ContentPart::InputImage {
                image_url, file_id, ..
            } => opt_len(image_url) + opt_len(file_id),
            ContentPart::InputFile {
                file_id,
                file_url,
                filename,
            } => opt_len(file_id) + opt_len(file_url) + opt_len(filename),
        }
    }

    /// Validate reference shape and URL safety.
    ///
    /// Ownership of a `file_id` is **not** checked here: that is the file
    /// service's responsibility and is explicitly outside this service's
    /// boundary (`spec.md` §4.3).
    pub fn validate_references(&self) -> Result<(), ContentViolation> {
        match self {
            ContentPart::InputImage {
                image_url, file_id, ..
            } => {
                match (image_url.as_deref(), file_id.as_deref()) {
                    (None, None) => Err(ContentViolation::MissingReference {
                        part: "input_image",
                    }),
                    (Some(url), _) => ensure_public_https(url).map_err(|source| {
                        ContentViolation::RejectedUrl {
                            part: "input_image",
                            source,
                        }
                    }),
                    (None, Some(_)) => Ok(()),
                }
            }
            ContentPart::InputFile {
                file_id, file_url, ..
            } => match (file_url.as_deref(), file_id.as_deref()) {
                (None, None) => Err(ContentViolation::MissingReference { part: "input_file" }),
                (Some(url), _) => {
                    ensure_public_https(url).map_err(|source| ContentViolation::RejectedUrl {
                        part: "input_file",
                        source,
                    })
                }
                (None, Some(_)) => Ok(()),
            },
            ContentPart::InputText { .. }
            | ContentPart::OutputText { .. }
            | ContentPart::Refusal { .. } => Ok(()),
        }
    }
}

fn opt_len(v: &Option<String>) -> usize {
    v.as_ref().map(String::len).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_text_parts() {
        let json = r#"{"type":"input_text","text":"hi"}"#;
        let part: ContentPart = serde_json::from_str(json).unwrap();
        assert_eq!(part, ContentPart::InputText { text: "hi".into() });
        assert_eq!(serde_json::to_string(&part).unwrap(), json);
    }

    #[test]
    fn rejects_unknown_part_type() {
        let err = serde_json::from_str::<ContentPart>(r#"{"type":"input_audio","data":"AA"}"#)
            .expect_err("unknown part type must be rejected");
        assert!(err.to_string().contains("input_audio"), "{err}");
    }

    #[test]
    fn rejects_unknown_field_inside_known_part() {
        let err = serde_json::from_str::<ContentPart>(r#"{"type":"input_text","text":"a","b":1}"#)
            .expect_err("unknown field must be rejected");
        assert!(err.to_string().contains('b'), "{err}");
    }

    #[test]
    fn rejects_inline_base64_image() {
        // No `b64_json`/`data` field exists at all …
        assert!(serde_json::from_str::<ContentPart>(
            r#"{"type":"input_image","image_data":"AAAA"}"#
        )
        .is_err());
        // … and a data: URL is stopped by the scheme guard.
        let part: ContentPart = serde_json::from_str(
            r#"{"type":"input_image","image_url":"data:image/png;base64,AAAA"}"#,
        )
        .unwrap();
        assert!(matches!(
            part.validate_references(),
            Err(ContentViolation::RejectedUrl { .. })
        ));
    }

    #[test]
    fn requires_at_least_one_reference() {
        let part: ContentPart = serde_json::from_str(r#"{"type":"input_image"}"#).unwrap();
        assert!(matches!(
            part.validate_references(),
            Err(ContentViolation::MissingReference { .. })
        ));
    }

    #[test]
    fn rejects_internal_image_url() {
        let part: ContentPart = serde_json::from_str(
            r#"{"type":"input_image","image_url":"https://169.254.169.254/latest/meta-data/"}"#,
        )
        .unwrap();
        assert!(matches!(
            part.validate_references(),
            Err(ContentViolation::RejectedUrl {
                source: UrlRejection::PrivateAddress,
                ..
            })
        ));
    }
}

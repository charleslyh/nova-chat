//! Model name → endpoint resolution.
//!
//! The `model` field of a request is **not** the name sent upstream: it names a
//! local catalog entry, which carries the wire model name, the base URL and how
//! to obtain the credential. One executor therefore serves every configured
//! model without a second place to remember them.
//!
//! # Credentials are fail-closed
//!
//! The behaviour this replaces treated "the variable is named but unset" as a
//! warning and continued with a no-auth placeholder. In production that turns
//! one typo in a variable name into a silent unauthenticated call, visible only
//! as a `WARN` nobody reads. Here it is an error.
//!
//! Declaring *no* credential is still allowed, because internal endpoints
//! genuinely have none — but it has to be said explicitly by omitting the
//! variable name, not implied by a lookup that failed.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::transport::{ApiKey, ModelEndpoint};

/// Sent when an entry declares no credential. Clients require a non-empty
/// configuration value even where the endpoint ignores the header.
const NO_AUTH_PLACEHOLDER: &str = "*";

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CatalogError {
    #[error("model `{0}` is not configured")]
    UnknownModel(String),
    #[error("model `{model}` names ${env} for its credential, but it is not set")]
    MissingCredential { model: String, env: String },
    #[error("model `{model}` names ${env} for its credential, but it is empty")]
    BlankCredential { model: String, env: String },
}

/// One configured model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    /// Catalog identity — what a request's `model` field names.
    pub id: String,
    /// OpenAI-compatible base URL (`…/v1`).
    pub base_url: String,
    /// The model name sent upstream.
    pub api_model: String,
    /// Environment variable holding the bearer token. Omit for endpoints that
    /// genuinely take none; that omission is the explicit "no auth" statement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key_env: Option<String>,
    /// Provider-specific request fields, passed through untouched.
    #[serde(default)]
    pub extensions: Value,
    /// Context window in tokens. Compaction starts at `window_size - 20_000`.
    /// Absent means this model is not compacted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_size: Option<u64>,
}

/// The configured models, keyed by catalog id.
#[derive(Debug, Clone, Default)]
pub struct ModelCatalog {
    entries: HashMap<String, ModelEntry>,
}

impl ModelCatalog {
    pub fn new(entries: impl IntoIterator<Item = ModelEntry>) -> Self {
        let entries: Vec<_> = entries.into_iter().collect();
        for entry in &entries {
            if entry.window_size.is_some() && tokenizer_for_completion(&entry.id).is_none() {
                tracing::warn!(
                    model = %entry.id,
                    "completion uses conservative context token estimation"
                );
            }
        }
        Self {
            entries: entries
                .into_iter()
                .map(|entry| (entry.id.clone(), entry))
                .collect(),
        }
    }

    pub fn get(&self, model: &str) -> Option<&ModelEntry> {
        self.entries.get(model)
    }

    /// The context window for `model`, when the entry declares one.
    pub fn window_tokens(&self, model: &str) -> Option<usize> {
        self.entries
            .get(model)?
            .window_size
            .map(|tokens| tokens as usize)
    }

    /// Select context accounting from the stable completion identity.
    ///
    /// Upstream wire names are deliberately not inspected: deployments may
    /// alias those names without changing the model family or tokenizer.
    pub fn tokenizer(&self, model: &str) -> Option<&'static str> {
        let entry = self.entries.get(model)?;
        tokenizer_for_completion(&entry.id)
    }

    /// Resolve a request's `model` into a usable endpoint.
    ///
    /// `env` is injected rather than read from the process so credential
    /// handling is testable without mutating global state.
    pub fn resolve(
        &self,
        model: &str,
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<ModelEndpoint, CatalogError> {
        let entry = self
            .entries
            .get(model)
            .ok_or_else(|| CatalogError::UnknownModel(model.to_string()))?;
        let api_key = resolve_api_key(entry, env)?;
        Ok(
            ModelEndpoint::new(api_key, entry.base_url.clone(), entry.api_model.clone())
                .with_extensions(entry.extensions.clone()),
        )
    }
}

fn tokenizer_for_completion(completion_id: &str) -> Option<&'static str> {
    match completion_id {
        "hunyuan-3" => Some("hy3"),
        _ => None,
    }
}

fn resolve_api_key(
    entry: &ModelEntry,
    env: &impl Fn(&str) -> Option<String>,
) -> Result<ApiKey, CatalogError> {
    let Some(name) = entry
        .api_key_env
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    else {
        // No variable named: the entry states it needs no credential.
        return Ok(ApiKey::new(NO_AUTH_PLACEHOLDER));
    };

    match env(name) {
        Some(value) if !value.trim().is_empty() => Ok(ApiKey::new(value)),
        Some(_) => Err(CatalogError::BlankCredential {
            model: entry.id.clone(),
            env: name.to_string(),
        }),
        // Fail closed. Continuing with a placeholder here would send an
        // unauthenticated request and report it as a warning.
        None => Err(CatalogError::MissingCredential {
            model: entry.id.clone(),
            env: name.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, api_key_env: Option<&str>) -> ModelEntry {
        ModelEntry {
            id: id.to_string(),
            base_url: "http://model.internal/v1".to_string(),
            api_model: "upstream-name".to_string(),
            api_key_env: api_key_env.map(str::to_string),
            extensions: Value::Null,
            window_size: None,
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    #[test]
    fn the_request_model_names_a_catalog_entry_not_the_wire_model() {
        let catalog = ModelCatalog::new([entry("house-chat", None)]);
        let endpoint = catalog.resolve("house-chat", &no_env).expect("resolves");
        assert_eq!(
            endpoint.api_model, "upstream-name",
            "the wire name comes from config, not from the request"
        );
    }

    #[test]
    fn an_unknown_model_is_an_error_not_a_default() {
        let catalog = ModelCatalog::new([entry("house-chat", None)]);
        assert_eq!(
            catalog.resolve("nope", &no_env).err(),
            Some(CatalogError::UnknownModel("nope".to_string()))
        );
    }

    #[test]
    fn omitting_the_variable_name_declares_no_auth() {
        let catalog = ModelCatalog::new([entry("internal", None)]);
        assert!(
            catalog.resolve("internal", &no_env).is_ok(),
            "internal endpoints may legitimately take no credential"
        );
    }

    #[test]
    fn a_blank_variable_name_is_also_no_auth() {
        let catalog = ModelCatalog::new([entry("internal", Some("   "))]);
        assert!(catalog.resolve("internal", &no_env).is_ok());
    }

    #[test]
    fn a_named_but_unset_variable_fails_closed() {
        // The regression this module exists for: the previous behaviour warned
        // and sent an unauthenticated request.
        let catalog = ModelCatalog::new([entry("paid", Some("PAID_KEY"))]);
        assert_eq!(
            catalog.resolve("paid", &no_env).err(),
            Some(CatalogError::MissingCredential {
                model: "paid".to_string(),
                env: "PAID_KEY".to_string(),
            })
        );
    }

    #[test]
    fn a_named_but_blank_variable_fails_closed() {
        let catalog = ModelCatalog::new([entry("paid", Some("PAID_KEY"))]);
        let env = |_: &str| Some("   ".to_string());
        assert_eq!(
            catalog.resolve("paid", &env).err(),
            Some(CatalogError::BlankCredential {
                model: "paid".to_string(),
                env: "PAID_KEY".to_string(),
            })
        );
    }

    #[test]
    fn a_set_variable_is_used() {
        let catalog = ModelCatalog::new([entry("paid", Some("PAID_KEY"))]);
        let env = |name: &str| (name == "PAID_KEY").then(|| "sk-live".to_string());
        let endpoint = catalog.resolve("paid", &env).expect("resolves");
        // Asserted through Debug because the key type refuses to reveal itself,
        // which is the point.
        assert_eq!(format!("{:?}", endpoint.api_key), "ApiKey(***)");
    }

    #[test]
    fn window_size_is_read_from_the_entry() {
        let mut model = entry("hy3", None);
        assert_eq!(
            ModelCatalog::new([model.clone()]).window_tokens("hy3"),
            None
        );
        model.window_size = Some(262_144);
        assert_eq!(
            ModelCatalog::new([model]).window_tokens("hy3"),
            Some(262_144)
        );
    }

    #[test]
    fn tokenizer_is_selected_from_the_completion_identity() {
        let catalog =
            ModelCatalog::new([entry("hunyuan-3", None), entry("hunyuan-4-preview", None)]);
        assert_eq!(catalog.tokenizer("hunyuan-3"), Some("hy3"));
        assert_eq!(catalog.tokenizer("hunyuan-4-preview"), None);
    }

    #[test]
    fn extensions_pass_through_to_the_endpoint() {
        let mut model = entry("tuned", None);
        model.extensions = serde_json::json!({"reasoning_effort": "high"});
        let catalog = ModelCatalog::new([model]);
        let endpoint = catalog.resolve("tuned", &no_env).expect("resolves");
        assert_eq!(
            endpoint.extensions,
            serde_json::json!({"reasoning_effort": "high"})
        );
    }

    #[test]
    fn a_credential_error_never_names_the_value() {
        let catalog = ModelCatalog::new([entry("paid", Some("PAID_KEY"))]);
        let env = |_: &str| Some("sk-super-secret".to_string());
        // A set, non-blank value resolves; the failure paths below must only
        // ever mention the variable name.
        assert!(catalog.resolve("paid", &env).is_ok());
        for error in [
            CatalogError::MissingCredential {
                model: "paid".into(),
                env: "PAID_KEY".into(),
            },
            CatalogError::BlankCredential {
                model: "paid".into(),
                env: "PAID_KEY".into(),
            },
        ] {
            let text = error.to_string();
            assert!(text.contains("PAID_KEY"));
            assert!(
                !text.contains("sk-"),
                "error must not carry a value: {text}"
            );
        }
    }
}

//! Gateway (assembly-layer) configuration.
//!
//! The gateway is the composition root: it mounts a concrete backend behind the
//! capability layer's ports. That choice — *which* backend, and how to reach it —
//! is an assembly concern and must not leak into `nova_responses::Config`, which
//! the library keeps backend-agnostic (D25). So the gateway owns its own config
//! that **composes** the capability config with the assembly knobs.
//!
//! On disk the capability config sits under a `[responses]` table and the
//! assembly knobs sit at the top level. Secrets are still absent here (SEC-4) —
//! only the *name* of an environment variable appears.

use std::path::Path;

use anyhow::{Context, Result};
use nova_responses::{Config, RawConfig};
use serde::Deserialize;

/// Raw (on-disk) gateway configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawGatewayConfig {
    /// Name of the environment variable holding the mem carrier's data-plane
    /// URL. Assembly-only: the capability layer never sees this.
    #[serde(default = "default_mem_server_url_env")]
    pub mem_server_url_env: String,

    pub responses: RawConfig,
}

/// Validated gateway configuration.
#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub mem_server_url_env: String,
    pub responses: Config,
}

impl GatewayConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let raw: RawGatewayConfig = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        Self::from_raw(raw)
    }

    pub fn from_raw(raw: RawGatewayConfig) -> Result<Self> {
        Ok(Self {
            mem_server_url_env: raw.mem_server_url_env,
            responses: Config::from_raw(raw.responses)?,
        })
    }
}

fn default_mem_server_url_env() -> String {
    "NOVA_MEM_SERVER_URL".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composes_capability_and_assembly_config() {
        let raw: RawGatewayConfig = toml::from_str(
            r#"
            [responses]
            node_tag = "node-a"
            listen = "127.0.0.1:18080"
            "#,
        )
        .expect("parse");
        let cfg = GatewayConfig::from_raw(raw).expect("validate");
        assert_eq!(cfg.responses.node_tag.as_str(), "node-a");
        assert_eq!(cfg.mem_server_url_env, "NOVA_MEM_SERVER_URL");
    }

    #[test]
    fn rejects_unknown_keys_in_capability_config() {
        // A typo in the `[responses]` table must be rejected by `RawConfig`'s
        // own `deny_unknown_fields`.
        let text = r#"
            [responses]
            node_tag = "node-a"
            listen = "127.0.0.1:18080"
            role = "home"
            "#;
        assert!(
            toml::from_str::<RawGatewayConfig>(text).is_err(),
            "stale keys must be rejected loudly, not ignored"
        );
    }

    #[test]
    fn rejects_unknown_keys_at_the_assembly_level() {
        // A typo at the top level must be rejected too, not folded into the
        // capability config.
        let text = r#"
            mem_server_url_evn = "NOVA_MEM_SERVER_URL"
            [responses]
            node_tag = "node-a"
            listen = "127.0.0.1:18080"
            "#;
        assert!(toml::from_str::<RawGatewayConfig>(text).is_err());
    }
}

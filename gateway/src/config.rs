//! Gateway (assembly-layer) configuration.
//!
//! The gateway is the composition root: it mounts a concrete backend behind the
//! capability layer's ports, binds a socket, reads credentials and decides whether this
//! process also runs background maintenance. Those choices are assembly concerns and
//! must not leak into [`nova_responses::config::Config`], which the library keeps free
//! of anything it does not itself read (D25).
//!
//! Six knobs used to live on the library's config while only this file consumed them —
//! the credentials variable, the pending-limit seed, the conversation page size, the
//! drain budget, the sweeper switch and the heartbeat TTL. They are here now.
//!
//! On disk the capability config sits under `[responses]` and the assembly knobs sit at
//! the top level. Secrets are still absent (SEC-4) — only the *name* of an environment
//! variable appears.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use nova_responses::config::Config;
use serde::Deserialize;

/// Gateway configuration, as written on disk and as used.
///
/// One struct, not a "raw" twin plus a validated copy: every field here either needs no
/// validation or is validated by its own type. What cannot be expressed that way is
/// checked in [`Self::validate`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GatewayConfig {
    /// Socket address this gateway binds. Assembly-only: the capability layer never
    /// opens a listener.
    pub listen: String,

    /// Name of the environment variable holding the mem carrier's data-plane URL.
    #[serde(default = "default_mem_server_url_env")]
    pub mem_server_url_env: String,

    /// Name of the environment variable holding `key:tenant` pairs. Ingress
    /// authentication is this layer's job (SEC-2/SEC-4).
    #[serde(default = "default_api_keys_env")]
    pub api_keys_env: String,

    /// How many conversation events one SSE read may return. Bounds the response size
    /// of a replay-from-zero, which is what a reopened page does.
    #[serde(default = "default_conversation_events_page")]
    pub conversation_events_page: usize,

    /// Graceful shutdown budget (FR-34).
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,

    /// The capability layer's own configuration.
    pub responses: Config,
}

fn default_mem_server_url_env() -> String {
    "NOVA_MEM_SERVER_URL".into()
}
fn default_api_keys_env() -> String {
    "NOVA_API_KEYS".into()
}
fn default_conversation_events_page() -> usize {
    256
}
fn default_drain_timeout_ms() -> u64 {
    600_000
}

impl GatewayConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Self = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        cfg.validate()
            .with_context(|| format!("validating config {}", path.display()))?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        // A zero drain budget silently reintroduces the rolling-deploy loss that
        // graceful shutdown exists to remove (D21).
        anyhow::ensure!(
            self.drain_timeout_ms > 0,
            "drain_timeout_ms must be greater than zero"
        );
        // A zero page would make every subscription return nothing forever, which reads
        // as "the conversation is quiet" rather than as a misconfiguration.
        anyhow::ensure!(
            self.conversation_events_page > 0,
            "conversation_events_page must be at least 1"
        );
        self.responses.validate()?;
        Ok(())
    }

    pub fn drain_timeout(&self) -> Duration {
        Duration::from_millis(self.drain_timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<GatewayConfig> {
        let cfg: GatewayConfig = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn composes_capability_and_assembly_config() {
        let cfg = parse(
            r#"
            listen = "127.0.0.1:18080"
            [responses]
            node_tag = "node-a"
            "#,
        )
        .expect("valid");
        assert_eq!(cfg.responses.node_tag.as_str(), "node-a");
        assert_eq!(cfg.listen, "127.0.0.1:18080");
        assert_eq!(cfg.mem_server_url_env, "NOVA_MEM_SERVER_URL");
        assert_eq!(cfg.api_keys_env, "NOVA_API_KEYS");
    }

    #[test]
    fn assembly_knobs_sit_at_the_top_level() {
        // They were inside `[responses]`, where the library had to carry six fields it
        // never read.
        let cfg = parse(
            r#"
            listen = "127.0.0.1:18080"
            drain_timeout_ms = 3000
            api_keys_env = "OTHER_KEYS"
            [responses]
            node_tag = "node-a"
            "#,
        )
        .expect("valid");
        assert_eq!(cfg.drain_timeout(), Duration::from_millis(3000));
        assert_eq!(cfg.api_keys_env, "OTHER_KEYS");
    }

    #[test]
    fn rejects_unknown_keys_in_capability_config() {
        // A typo in the `[responses]` table must be rejected by the library's own
        // `deny_unknown_fields`.
        assert!(parse(
            r#"
            listen = "127.0.0.1:18080"
            [responses]
            node_tag = "node-a"
            role = "home"
            "#
        )
        .is_err());
    }

    #[test]
    fn rejects_unknown_keys_at_the_assembly_level() {
        assert!(parse(
            r#"
            mem_server_url_evn = "NOVA_MEM_SERVER_URL"
            listen = "127.0.0.1:18080"
            [responses]
            node_tag = "node-a"
            "#
        )
        .is_err());
    }

    #[test]
    fn rejects_unusable_budgets() {
        for bad in [
            "drain_timeout_ms = 0",
            "conversation_events_page = 0",
        ] {
            assert!(
                parse(&format!(
                    "listen = \"127.0.0.1:1\"\n{bad}\n[responses]\nnode_tag = \"node-a\"\n"
                ))
                .is_err(),
                "{bad} must be rejected"
            );
        }
    }

    #[test]
    fn the_capability_layer_validates_its_own_half() {
        assert!(parse(
            r#"
            listen = "127.0.0.1:1"
            [responses]
            node_tag = "node-a"
            [responses.chain]
            max_depth = 0
            "#
        )
        .is_err());
    }
}

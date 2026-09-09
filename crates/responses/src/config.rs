//! Capability-layer configuration.
//!
//! This is the config the capability layer itself consumes. Backend choice and
//! how to reach a specific carrier are assembly concerns and live in the
//! gateway's own `GatewayConfig`, not here (D25).
//!
//! One rule governs this file: **secrets are never values here.** Config carries
//! the *name* of an environment variable; the value is read at startup (SEC-4).
//! A connection string or API key in a TOML file ends up in version control,
//! logs and container images.

use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use crate::{ChainLimits, NodeTag};
use crate::protocol::InputLimits;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    pub node_tag: String,

    /// Name of the environment variable holding `key:tenant` pairs.
    #[serde(default = "default_api_keys_env")]
    pub api_keys_env: String,

    #[serde(default = "default_pending_limit")]
    pub pending_limit: usize,

    /// How many conversation events one subscription read may return.
    ///
    /// Bounds the response size of a replay-from-zero, which is what a reopened
    /// page does. The stream continues from where the batch ended, so this caps
    /// memory per read without capping how much history is reachable.
    #[serde(default = "default_conversation_events_page")]
    pub conversation_events_page: usize,

    /// How long a terminal response's events stay readable.
    #[serde(default = "default_retain_after_terminal_ms")]
    pub retain_after_terminal_ms: u64,

    /// Retention for stored content. Configurable, not a hard-coded policy
    /// (OR-5).
    #[serde(default = "default_content_retention_ms")]
    pub content_retention_ms: u64,

    #[serde(default = "default_chain_max_depth")]
    pub chain_max_depth: usize,
    #[serde(default = "default_chain_max_items")]
    pub chain_max_items: usize,
    #[serde(default = "default_chain_max_bytes")]
    pub chain_max_bytes: usize,

    #[serde(default = "default_input_max_items")]
    pub input_max_items: usize,
    #[serde(default = "default_input_max_item_bytes")]
    pub input_max_item_bytes: usize,
    #[serde(default = "default_input_max_bytes")]
    pub input_max_bytes: usize,
    #[serde(default = "default_input_max_depth")]
    pub input_max_json_depth: usize,

    /// Synchronous mode budget: how long to wait for a terminal event before
    /// returning the current state for the caller to poll.
    #[serde(default = "default_sync_wait_timeout_ms")]
    pub sync_wait_timeout_ms: u64,

    /// Graceful shutdown budget (FR-34).
    #[serde(default = "default_drain_timeout_ms")]
    pub drain_timeout_ms: u64,

    #[serde(default = "default_true")]
    pub run_sweeper: bool,

    #[serde(default = "default_heartbeat_ttl_ms")]
    pub heartbeat_ttl_ms: u64,
}

fn default_api_keys_env() -> String {
    "NOVA_API_KEYS".into()
}
fn default_pending_limit() -> usize {
    10_000
}
fn default_conversation_events_page() -> usize {
    256
}
fn default_retain_after_terminal_ms() -> u64 {
    60_000
}
fn default_content_retention_ms() -> u64 {
    30 * 24 * 60 * 60 * 1000
}
// Chain / input bounds default to the domain structs' own `Default`, so there is
// exactly one source of truth for each number — the on-disk default cannot drift
// from `ChainLimits::default()` / `InputLimits::default()`.
fn default_chain_max_depth() -> usize {
    ChainLimits::default().max_depth
}
fn default_chain_max_items() -> usize {
    ChainLimits::default().max_items
}
fn default_chain_max_bytes() -> usize {
    ChainLimits::default().max_bytes
}
fn default_input_max_items() -> usize {
    InputLimits::default().max_items
}
fn default_input_max_item_bytes() -> usize {
    InputLimits::default().max_item_bytes
}
fn default_input_max_bytes() -> usize {
    InputLimits::default().max_total_bytes
}
fn default_input_max_depth() -> usize {
    InputLimits::default().max_json_depth
}
fn default_sync_wait_timeout_ms() -> u64 {
    30_000
}
fn default_drain_timeout_ms() -> u64 {
    600_000
}
fn default_true() -> bool {
    true
}
fn default_heartbeat_ttl_ms() -> u64 {
    90_000
}

/// Validated configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub node_tag: NodeTag,
    pub api_keys_env: String,
    pub pending_limit: usize,
    pub conversation_events_page: usize,
    pub retain_after_terminal_ms: u64,
    pub content_retention_ms: u64,
    pub chain_limits: ChainLimits,
    pub input_limits: InputLimits,
    pub sync_wait_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    pub run_sweeper: bool,
    pub heartbeat_ttl_ms: u64,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let raw: RawConfig = toml::from_str(&text)
            .with_context(|| format!("parsing config {}", path.display()))?;
        Self::from_raw(raw)
    }

    pub fn from_raw(raw: RawConfig) -> Result<Self> {
        let node_tag = NodeTag::parse(&raw.node_tag)
            .map_err(|e| anyhow!("node_tag `{}`: {e}", raw.node_tag))?;

        if raw.chain_max_depth == 0 {
            bail!("chain_max_depth must be at least 1");
        }
        // A zero page would make every subscription return nothing forever, which
        // reads as "the conversation is quiet" rather than as a misconfiguration.
        if raw.conversation_events_page == 0 {
            bail!("conversation_events_page must be at least 1");
        }
        // A zero drain budget silently reintroduces the rolling-deploy loss that
        // graceful shutdown exists to remove (D21).
        if raw.drain_timeout_ms == 0 {
            bail!("drain_timeout_ms must be greater than zero");
        }

        Ok(Self {
            node_tag,
            api_keys_env: raw.api_keys_env,
            pending_limit: raw.pending_limit,
            conversation_events_page: raw.conversation_events_page,
            retain_after_terminal_ms: raw.retain_after_terminal_ms,
            content_retention_ms: raw.content_retention_ms,
            chain_limits: ChainLimits {
                max_depth: raw.chain_max_depth,
                max_items: raw.chain_max_items,
                max_bytes: raw.chain_max_bytes,
            },
            input_limits: InputLimits {
                max_items: raw.input_max_items,
                max_item_bytes: raw.input_max_item_bytes,
                max_total_bytes: raw.input_max_bytes,
                max_json_depth: raw.input_max_json_depth,
            },
            sync_wait_timeout_ms: raw.sync_wait_timeout_ms,
            drain_timeout_ms: raw.drain_timeout_ms,
            run_sweeper: raw.run_sweeper,
            heartbeat_ttl_ms: raw.heartbeat_ttl_ms,
        })
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(extra: &str) -> RawConfig {
        let text = format!("node_tag = \"node-a\"\n{extra}");
        toml::from_str(&text).expect("parse")
    }

    #[test]
    fn defaults_are_conservative() {
        let cfg = Config::from_raw(raw("")).unwrap();
        assert_eq!(cfg.chain_limits.max_depth, 50);
        assert_eq!(cfg.chain_limits.max_bytes, 1024 * 1024);
        assert!(cfg.run_sweeper);
    }

    #[test]
    fn rejects_unknown_config_keys() {
        let text = "node_tag = \"node-a\"\nrole = \"home\"\n";
        assert!(
            toml::from_str::<RawConfig>(text).is_err(),
            "stale keys such as `role` must be rejected loudly, not ignored"
        );
    }

    #[test]
    fn rejects_zero_drain_budget() {
        assert!(Config::from_raw(raw("drain_timeout_ms = 0\n")).is_err());
    }

    #[test]
    fn rejects_zero_bounds() {
        assert!(Config::from_raw(raw("chain_max_depth = 0\n")).is_err());
        assert!(Config::from_raw(raw("conversation_events_page = 0\n")).is_err());
    }
}

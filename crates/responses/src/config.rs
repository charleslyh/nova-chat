//! Capability-layer configuration: the knobs this crate itself reads.
//!
//! Everything here is consumed by the capability layer. Knobs that belong to whoever
//! *assembles* the service — which credentials table, which address to bind, whether
//! this process runs the sweeper, how long shutdown may drain — live in the
//! assembling binary's own config (D25). They used to live here, which meant a
//! library type carried six fields it never read.
//!
//! One rule governs this file: **secrets are never values here.** Nothing in this
//! struct is a credential; a connection string or API key in a TOML file ends up in
//! version control, logs and container images (SEC-4).

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::identity::NodeTag;
use crate::protocol::ProtocolLimits;

/// Configuration rejected at load time. A typed error (not `anyhow`) because this
/// crate is an embeddable library: callers match on the failure class instead of
/// parsing a message string.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{field} must be at least {min}")]
    TooSmall { field: &'static str, min: u64 },
}

/// Bounds for chain resolution. Exceeding any of them is an **error**, never a silent
/// truncation (INV-41).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChainLimits {
    pub max_depth: usize,
    pub max_items: usize,
    pub max_bytes: usize,
}

impl Default for ChainLimits {
    fn default() -> Self {
        Self {
            max_depth: 50,
            max_items: 1000,
            max_bytes: 1024 * 1024,
        }
    }
}

/// Validated capability-layer configuration.
///
/// Deserialised **directly**, with no separate "raw" twin. The twin existed to turn a
/// `String` into a validated [`NodeTag`], which `NodeTag`'s own `Deserialize` already
/// does — so its only remaining effect was twelve lines of hand-written field copying
/// and two fields that changed name in transit. What genuinely cannot be expressed in
/// the type system is checked by [`Self::validate`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Identity of this node, embedded in every response id it mints.
    pub node_tag: NodeTag,

    /// How long a terminal response's events stay readable.
    #[serde(default = "default_retain_after_terminal_ms")]
    pub retain_after_terminal_ms: u64,

    /// How long a claim may go without a heartbeat before the sweeper reaps it.
    ///
    /// Lives here (not in the assembly layer) because the sweeper is now owned by the
    /// capability layer: [`crate::service::ResponsesService`] spawns it and reads this
    /// bound itself, exactly as it does `retain_after_terminal_ms`.
    #[serde(default = "default_heartbeat_ttl_ms")]
    pub heartbeat_ttl_ms: u64,

    /// Retention for stored content. Configurable, not a hard-coded policy (OR-5).
    #[serde(default = "default_content_retention_ms")]
    pub content_retention_ms: u64,

    /// Synchronous mode budget: how long to wait for a terminal event before
    /// returning the current state for the caller to poll.
    #[serde(default = "default_sync_wait_timeout_ms")]
    pub sync_wait_timeout_ms: u64,

    /// How many response events one internal read may return.
    ///
    /// Bounds the memory a replay holds at once. It was a literal `256` written twice
    /// in the service layer, next to a *different* configurable page size for the
    /// conversation stream.
    #[serde(default = "default_event_page")]
    pub event_page: usize,

    /// Chain-resolution bounds. Nested as the struct itself so the on-disk default
    /// **is** `ChainLimits::default()` — exactly one source of truth per number, and
    /// no per-field forwarding that could drift from it.
    #[serde(default)]
    pub chain: ChainLimits,

    /// Input hardening bounds (SEC-7). Same single-source rule as `chain`.
    #[serde(default)]
    pub limits: ProtocolLimits,
}

fn default_retain_after_terminal_ms() -> u64 {
    60_000
}
fn default_heartbeat_ttl_ms() -> u64 {
    90_000
}
fn default_content_retention_ms() -> u64 {
    30 * 24 * 60 * 60 * 1000
}
fn default_sync_wait_timeout_ms() -> u64 {
    30_000
}
fn default_event_page() -> usize {
    256
}

impl Config {
    /// A default configuration for `node_tag`, for tests and embedders that have no
    /// file to read.
    pub fn for_node(node_tag: NodeTag) -> Self {
        Self {
            node_tag,
            retain_after_terminal_ms: default_retain_after_terminal_ms(),
            heartbeat_ttl_ms: default_heartbeat_ttl_ms(),
            content_retention_ms: default_content_retention_ms(),
            sync_wait_timeout_ms: default_sync_wait_timeout_ms(),
            event_page: default_event_page(),
            chain: ChainLimits::default(),
            limits: ProtocolLimits::default(),
        }
    }

    /// Reject values that parse but cannot work.
    ///
    /// Only bounds that the type system cannot state appear here — everything that is
    /// merely "a number" is one.
    pub fn validate(&self) -> Result<(), ConfigError> {
        // A zero-depth chain would refuse every continuation.
        if self.chain.max_depth == 0 {
            return Err(ConfigError::TooSmall {
                field: "chain.max_depth",
                min: 1,
            });
        }
        if self.chain.max_items == 0 {
            return Err(ConfigError::TooSmall {
                field: "chain.max_items",
                min: 1,
            });
        }
        // A zero page would make every read return nothing forever, which reads as "the
        // stream is quiet" rather than as a misconfiguration.
        if self.event_page == 0 {
            return Err(ConfigError::TooSmall {
                field: "event_page",
                min: 1,
            });
        }
        if self.limits.max_items == 0 {
            return Err(ConfigError::TooSmall {
                field: "limits.max_items",
                min: 1,
            });
        }
        Ok(())
    }

    /// Load, then validate. Kept together so no caller can obtain an unvalidated
    /// `Config` from a file.
    pub fn from_toml(text: &str) -> Result<Self, ConfigLoadError> {
        let cfg: Self = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn sync_wait(&self) -> Duration {
        Duration::from_millis(self.sync_wait_timeout_ms)
    }

    pub fn retain_after_terminal(&self) -> Duration {
        Duration::from_millis(self.retain_after_terminal_ms)
    }

    pub fn heartbeat_ttl(&self) -> Duration {
        Duration::from_millis(self.heartbeat_ttl_ms)
    }
}

/// Failure of [`Config::from_toml`]: bad syntax, or a value that cannot work.
#[derive(Debug, thiserror::Error)]
pub enum ConfigLoadError {
    #[error("parsing config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error(transparent)]
    Invalid(#[from] ConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(extra: &str) -> Result<Config, ConfigLoadError> {
        Config::from_toml(&format!("node_tag = \"node-a\"\n{extra}"))
    }

    #[test]
    fn defaults_are_conservative() {
        let cfg = cfg("").unwrap();
        assert_eq!(cfg.node_tag.as_str(), "node-a");
        assert_eq!(cfg.chain.max_depth, 50);
        assert_eq!(cfg.chain.max_bytes, 1024 * 1024);
        assert_eq!(cfg.limits.max_items, 200);
        assert_eq!(cfg.event_page, 256);
        assert_eq!(cfg.sync_wait(), Duration::from_millis(30_000));
    }

    #[test]
    fn the_node_tag_is_validated_by_its_own_type() {
        // No hand-written conversion step: `NodeTag: Deserialize` is the validator,
        // which is why the "raw config" twin could go.
        let err = cfg("").err();
        assert!(err.is_none());
        assert!(matches!(
            Config::from_toml("node_tag = \"NODE_A\"\n"),
            Err(ConfigLoadError::Parse(_))
        ));
        assert!(matches!(
            Config::from_toml("retain_after_terminal_ms = 1\n"),
            Err(ConfigLoadError::Parse(_)) // node_tag is required
        ));
    }

    #[test]
    fn nested_limit_tables_override_partially() {
        let chain = cfg("[chain]\nmax_depth = 5\n").unwrap();
        assert_eq!(chain.chain.max_depth, 5);
        // Untouched fields fall back to the domain default, not to zero.
        assert_eq!(chain.chain.max_bytes, ChainLimits::default().max_bytes);

        let limits = cfg("[limits]\nmax_url_bytes = 128\n").unwrap();
        assert_eq!(limits.limits.max_url_bytes, 128);
        assert_eq!(limits.limits.max_items, ProtocolLimits::default().max_items);
    }

    #[test]
    fn rejects_unknown_keys_including_ones_that_moved_out() {
        // These keys used to live here and moved elsewhere (assembly layer) or were
        // removed outright (`run_sweeper`). Silently ignoring them would leave an
        // operator believing a setting was in force.
        for stale in [
            "role = \"home\"",
            "api_keys_env = \"X\"",
            "run_sweeper = false",
            "drain_timeout_ms = 1000",
            "pending_limit = 10",
            "conversation_events_page = 10",
        ] {
            assert!(
                matches!(cfg(stale), Err(ConfigLoadError::Parse(_))),
                "stale key must be rejected loudly, not ignored: {stale}"
            );
        }
        // `heartbeat_ttl_ms` is a capability-layer key again — the sweeper is owned by
        // the service, so this crate reads it — so it parses rather than being rejected.
        assert_eq!(cfg("heartbeat_ttl_ms = 2000\n").unwrap().heartbeat_ttl_ms, 2000);
    }

    #[test]
    fn rejects_zero_bounds() {
        assert_eq!(
            cfg("[chain]\nmax_depth = 0\n").unwrap_err().to_string(),
            "chain.max_depth must be at least 1"
        );
        assert!(cfg("[chain]\nmax_items = 0\n").is_err());
        assert!(cfg("event_page = 0\n").is_err());
        assert!(cfg("[limits]\nmax_items = 0\n").is_err());
    }

    #[test]
    fn for_node_matches_the_on_disk_defaults() {
        let from_file = cfg("").unwrap();
        let programmatic = Config::for_node(NodeTag::parse("node-a").unwrap());
        assert_eq!(programmatic.chain, from_file.chain);
        assert_eq!(programmatic.limits, from_file.limits);
        assert_eq!(programmatic.event_page, from_file.event_page);
        programmatic.validate().expect("defaults must be valid");
    }
}

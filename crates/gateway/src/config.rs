//! Gateway configuration.
//!
//! Two rules govern this file:
//!
//! 1. **Secrets are never values here.** Config carries the *name* of an
//!    environment variable; the value is read at startup (SEC-4). A connection
//!    string or API key in a TOML file ends up in version control, logs and
//!    container images.
//! 2. **`peers` is the SSRF allow-list.** Forwarding targets may only ever come
//!    from this map. Nothing derives an address from a request field or header
//!    (SEC-5).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{anyhow, bail, Context, Result};
use nova_responses_core::{ChainLimits, NodeTag};
use nova_responses_core::protocol::InputLimits;
use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StoreBackend {
    /// Verification only; not durable, not shared.
    Mem,
    /// Production carrier.
    Sql,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawConfig {
    pub node_tag: String,
    pub listen: String,

    /// node_tag -> `host:port`. Peers are the only permissible forward targets.
    #[serde(default)]
    pub peers: BTreeMap<String, String>,

    #[serde(default = "default_backend")]
    pub store_backend: StoreBackend,

    /// Name of the environment variable holding the database URL — not the URL.
    #[serde(default = "default_database_url_env")]
    pub database_url_env: String,

    /// Name of the environment variable holding `key:tenant` pairs.
    #[serde(default = "default_api_keys_env")]
    pub api_keys_env: String,

    /// Name of the environment variable holding the shared token that
    /// authenticates node-to-node forwards.
    #[serde(default = "default_internal_token_env")]
    pub internal_token_env: String,

    #[serde(default = "default_pending_limit")]
    pub pending_limit: usize,

    #[serde(default = "default_events_per_response")]
    pub max_events_per_response: usize,

    #[serde(default = "default_max_logs")]
    pub max_event_logs: usize,

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
    pub verify_integrity: bool,

    #[serde(default = "default_true")]
    pub run_sweeper: bool,

    #[serde(default = "default_exec_ttl_ms")]
    pub exec_ttl_ms: u64,

    #[serde(default = "default_heartbeat_ttl_ms")]
    pub heartbeat_ttl_ms: u64,
}

fn default_backend() -> StoreBackend {
    StoreBackend::Mem
}
fn default_database_url_env() -> String {
    "NOVA_DATABASE_URL".into()
}
fn default_api_keys_env() -> String {
    "NOVA_API_KEYS".into()
}
fn default_internal_token_env() -> String {
    "NOVA_INTERNAL_TOKEN".into()
}
fn default_pending_limit() -> usize {
    10_000
}
fn default_events_per_response() -> usize {
    20_000
}
fn default_max_logs() -> usize {
    100_000
}
fn default_retain_after_terminal_ms() -> u64 {
    60_000
}
fn default_content_retention_ms() -> u64 {
    30 * 24 * 60 * 60 * 1000
}
fn default_chain_max_depth() -> usize {
    50
}
fn default_chain_max_items() -> usize {
    1000
}
fn default_chain_max_bytes() -> usize {
    1024 * 1024
}
fn default_input_max_items() -> usize {
    200
}
fn default_input_max_item_bytes() -> usize {
    256 * 1024
}
fn default_input_max_bytes() -> usize {
    1024 * 1024
}
fn default_input_max_depth() -> usize {
    32
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
fn default_exec_ttl_ms() -> u64 {
    3_600_000
}
fn default_heartbeat_ttl_ms() -> u64 {
    90_000
}

/// Validated configuration.
#[derive(Debug, Clone)]
pub struct Config {
    pub node_tag: NodeTag,
    pub listen: String,
    pub peers: BTreeMap<NodeTag, String>,
    pub store_backend: StoreBackend,
    pub database_url_env: String,
    pub api_keys_env: String,
    pub internal_token_env: String,
    pub pending_limit: usize,
    pub max_events_per_response: usize,
    pub max_event_logs: usize,
    pub retain_after_terminal_ms: u64,
    pub content_retention_ms: u64,
    pub chain_limits: ChainLimits,
    pub input_limits: InputLimits,
    pub sync_wait_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    pub verify_integrity: bool,
    pub run_sweeper: bool,
    pub exec_ttl_ms: u64,
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

        let mut peers = BTreeMap::new();
        for (tag, addr) in &raw.peers {
            let parsed = NodeTag::parse(tag).map_err(|e| anyhow!("peer tag `{tag}`: {e}"))?;
            validate_authority(addr).with_context(|| format!("peer `{tag}` address"))?;
            if parsed == node_tag && addr != &raw.listen {
                // Self-entry pointing elsewhere would silently proxy to another
                // process while believing it is local.
                bail!("peer `{tag}` is this node but its address does not match `listen`");
            }
            peers.insert(parsed, addr.clone());
        }

        if raw.chain_max_depth == 0 {
            bail!("chain_max_depth must be at least 1");
        }
        if raw.max_events_per_response == 0 {
            bail!("max_events_per_response must be at least 1");
        }
        // A zero drain budget silently reintroduces the rolling-deploy loss that
        // graceful shutdown exists to remove (D21).
        if raw.drain_timeout_ms == 0 {
            bail!("drain_timeout_ms must be greater than zero");
        }

        Ok(Self {
            node_tag,
            listen: raw.listen,
            peers,
            store_backend: raw.store_backend,
            database_url_env: raw.database_url_env,
            api_keys_env: raw.api_keys_env,
            internal_token_env: raw.internal_token_env,
            pending_limit: raw.pending_limit,
            max_events_per_response: raw.max_events_per_response,
            max_event_logs: raw.max_event_logs,
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
            verify_integrity: raw.verify_integrity,
            run_sweeper: raw.run_sweeper,
            exec_ttl_ms: raw.exec_ttl_ms,
            heartbeat_ttl_ms: raw.heartbeat_ttl_ms,
        })
    }

    /// Resolve a peer address. Returns `None` for unknown tags — callers must
    /// then report "not found" and **never** synthesise an address (SEC-5).
    pub fn peer_addr(&self, tag: &NodeTag) -> Option<&str> {
        self.peers.get(tag).map(String::as_str)
    }

    pub fn is_local(&self, tag: &NodeTag) -> bool {
        &self.node_tag == tag
    }
}

/// Accept only `host:port`, rejecting anything that could smuggle a scheme,
/// path, query or credentials into a forward target.
fn validate_authority(addr: &str) -> Result<()> {
    if addr.is_empty() || addr.len() > 255 {
        bail!("must be 1..=255 bytes");
    }
    if addr.contains("://") {
        bail!("must not include a scheme");
    }
    for bad in ['/', '?', '#', '@', '\\', ' ', '\t'] {
        if addr.contains(bad) {
            bail!("must not contain `{bad}`");
        }
    }
    let (host, port) = addr
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("must be host:port"))?;
    if host.is_empty() {
        bail!("host must not be empty");
    }
    let port: u16 = port.parse().map_err(|_| anyhow!("port must be 1..=65535"))?;
    if port == 0 {
        bail!("port must be 1..=65535");
    }
    let host_ok = host
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'));
    if !host_ok {
        bail!("host contains disallowed characters");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw(extra: &str) -> RawConfig {
        let text = format!("node_tag = \"node-a\"\nlisten = \"127.0.0.1:18080\"\n{extra}");
        toml::from_str(&text).expect("parse")
    }

    #[test]
    fn defaults_are_conservative() {
        let cfg = Config::from_raw(raw("")).unwrap();
        assert_eq!(cfg.store_backend, StoreBackend::Mem);
        assert_eq!(cfg.chain_limits.max_depth, 50);
        assert_eq!(cfg.chain_limits.max_bytes, 1024 * 1024);
        assert!(cfg.verify_integrity);
        assert!(cfg.run_sweeper);
        // Config must not carry secret values, only variable names.
        assert_eq!(cfg.database_url_env, "NOVA_DATABASE_URL");
    }

    #[test]
    fn rejects_unknown_config_keys() {
        let text = "node_tag = \"node-a\"\nlisten = \"1.2.3.4:1\"\nrole = \"home\"\n";
        assert!(
            toml::from_str::<RawConfig>(text).is_err(),
            "stale keys such as `role` must be rejected loudly, not ignored"
        );
    }

    #[test]
    fn accepts_valid_peer_map() {
        let cfg = Config::from_raw(raw(
            "[peers]\nnode-a = \"127.0.0.1:18080\"\nnode-b = \"127.0.0.1:18081\"\n",
        ))
        .unwrap();
        assert_eq!(cfg.peers.len(), 2);
        let b = NodeTag::parse("node-b").unwrap();
        assert_eq!(cfg.peer_addr(&b), Some("127.0.0.1:18081"));
        assert!(cfg.is_local(&NodeTag::parse("node-a").unwrap()));
    }

    #[test]
    fn unknown_peer_has_no_address() {
        let cfg = Config::from_raw(raw("")).unwrap();
        // The caller must translate this into "not found", never into a
        // constructed address.
        assert_eq!(cfg.peer_addr(&NodeTag::parse("ghost").unwrap()), None);
    }

    #[test]
    fn rejects_peer_addresses_that_could_be_abused() {
        for addr in [
            "http://127.0.0.1:80",
            "127.0.0.1:80/admin",
            "user@127.0.0.1:80",
            "127.0.0.1:80?x=1",
            "127.0.0.1",
            "127.0.0.1:0",
            "127.0.0.1:99999",
            ":80",
            "127.0.0.1:80 ",
        ] {
            let cfg = Config::from_raw(raw(&format!("[peers]\nnode-b = \"{addr}\"\n")));
            assert!(cfg.is_err(), "peer address `{addr}` must be rejected");
        }
    }

    #[test]
    fn rejects_invalid_peer_tags() {
        assert!(Config::from_raw(raw("[peers]\n\"node_b\" = \"1.2.3.4:1\"\n")).is_err());
        assert!(Config::from_raw(raw("[peers]\n\"NODE-B\" = \"1.2.3.4:1\"\n")).is_err());
    }

    #[test]
    fn rejects_self_peer_pointing_elsewhere() {
        let cfg = Config::from_raw(raw("[peers]\nnode-a = \"127.0.0.1:9999\"\n"));
        assert!(
            cfg.is_err(),
            "a self-entry with a different address would proxy to another process"
        );
    }

    #[test]
    fn rejects_zero_drain_budget() {
        assert!(Config::from_raw(raw("drain_timeout_ms = 0\n")).is_err());
    }

    #[test]
    fn rejects_zero_bounds() {
        assert!(Config::from_raw(raw("chain_max_depth = 0\n")).is_err());
        assert!(Config::from_raw(raw("max_events_per_response = 0\n")).is_err());
    }
}

//! Response service gateway.
//!
//! Assembly only: load config, mount a backend, validate startup preconditions,
//! reclaim orphans, start the sweeper, serve, drain.

mod auth;
mod config;
mod error;
mod routes;
mod routing;
mod sse;
mod state;
mod sweeper;
mod shutdown;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use nova_responses_core::{
    Clock, ContextStore, MetricsSink, ResponseEventLog, ResponseLedger,
};
use tracing::{info, warn};

use crate::auth::KeyTable;
use crate::config::{Config, StoreBackend};
use crate::state::AppState;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "testing/config/node-a.toml")]
    config: PathBuf,
}

const ADMIN_KEY_ENV: &str = "NOVA_ADMIN_KEY";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = Arc::new(Config::load(&args.config)?);
    let addr: SocketAddr = cfg
        .listen
        .parse()
        .with_context(|| format!("listen address `{}`", cfg.listen))?;

    let keys = Arc::new(
        KeyTable::from_env(&cfg.api_keys_env, &cfg.internal_token_env, ADMIN_KEY_ENV)
            .map_err(|e| anyhow::anyhow!("api keys from ${}: {e}", cfg.api_keys_env))?,
    );
    if keys.is_empty() {
        warn!(
            env = cfg.api_keys_env,
            "no API keys configured; running unauthenticated (verification only)"
        );
    }
    if !cfg.peers.is_empty() && !keys.has_internal_token() {
        // Without a shared token, peers cannot authenticate forwards and the
        // internal tenant header would have to be trusted blindly.
        bail!(
            "peers are configured but ${} is not set; node-to-node forwarding requires it",
            cfg.internal_token_env
        );
    }

    // Mount a backend. Everything downstream sees only trait objects.
    let (ledger, event_log, context, clock, metrics): (
        Arc<dyn ResponseLedger>,
        Arc<dyn ResponseEventLog>,
        Arc<dyn ContextStore>,
        Arc<dyn Clock>,
        Arc<dyn MetricsSink>,
    ) = match cfg.store_backend {
        StoreBackend::Mem => {
            let world = adapters_mem::MemWorld::with_config(adapters_mem::MemWorldConfig {
                events_per_response: cfg.max_events_per_response,
                max_logs: cfg.max_event_logs,
                max_records: 100_000,
                pending_limit: cfg.pending_limit,
                verify_integrity: cfg.verify_integrity,
            });
            warn!("store_backend=mem: not durable and not shared; verification use only");
            (
                world.ledger.clone(),
                world.event_log.clone(),
                world.context.clone(),
                world.clock.clone(),
                world.metrics.clone(),
            )
        }
        StoreBackend::Sql => {
            // Startup fails outright when the integrity key or the database is
            // missing: storing records that cannot later be verified, or
            // accepting traffic that must all be refused, is worse than not
            // starting (INV-44 / INV-46).
            let sql = adapters_sql::SqlWorld::connect_from_env(
                &cfg.database_url_env,
                adapters_sql::SqlConfig {
                    verify_integrity: cfg.verify_integrity,
                    ..Default::default()
                },
            )
            .await
            .with_context(|| {
                format!(
                    "connecting to the context store via ${}",
                    cfg.database_url_env
                )
            })?;
            let mem_side = adapters_mem::MemWorld::with_config(adapters_mem::MemWorldConfig {
                events_per_response: cfg.max_events_per_response,
                max_logs: cfg.max_event_logs,
                verify_integrity: false,
                ..Default::default()
            });
            (
                sql.ledger.clone(),
                // In-flight events stay in process memory regardless of backend:
                // that is the deliberate tiering decision, not an omission (D21).
                mem_side.event_log.clone(),
                sql.context.clone(),
                mem_side.clock.clone(),
                mem_side.metrics.clone(),
            )
        }
    };

    ledger.set_pending_limit(cfg.pending_limit);

    // Liveness probe before serving: a node that cannot store would refuse every
    // `store: true` create, so failing fast is clearer than serving 503s.
    if let Err(e) = context.health().await {
        bail!("context store is not reachable at startup: {e}");
    }

    let state = AppState {
        cfg: cfg.clone(),
        ledger: ledger.clone(),
        event_log: event_log.clone(),
        context: context.clone(),
        clock: clock.clone(),
        metrics: metrics.clone(),
        keys,
        http: reqwest::Client::new(),
        accepting: Arc::new(AtomicBool::new(true)),
    };

    // Orphan reclaim (INV-45): anything non-terminal owned by this node lost its
    // in-flight buffer when the previous process died. Fail it now — seconds
    // instead of a 90 s heartbeat timeout.
    let now = state.now_ms().await;
    match ledger.reclaim_orphans(&cfg.node_tag, now).await {
        Ok(reclaimed) if !reclaimed.is_empty() => {
            info!(
                count = reclaimed.len(),
                node_tag = cfg.node_tag.as_str(),
                "reclaimed orphaned responses from a previous process"
            );
            metrics.incr("orphans_reclaimed", reclaimed.len() as u64).await;
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "orphan reclaim failed"),
    }

    if cfg.run_sweeper {
        sweeper::spawn(state.clone());
    }

    let app = routes::router(state.clone());

    // Readiness marker for the local process harness.
    if let Some(parent) = args.config.parent() {
        let marker = parent.join(format!(".ready-{}", cfg.node_tag));
        let _ = std::fs::write(&marker, b"ok");
    }

    info!(
        node_tag = cfg.node_tag.as_str(),
        %addr,
        backend = ?cfg.store_backend,
        context_store_shared = context.is_shared(),
        "nova-responses-gateway listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::drain(state))
        .await?;

    info!("shutdown complete");
    Ok(())
}

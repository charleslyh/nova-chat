//! Gateway binary over [`nova_responses`]: thin assembly that mounts a backend.
//!
//! Which backend is mounted is a **compile-time** choice, not a runtime config
//! switch:
//!
//! - `feature = "mem"` (default): the mem carrier **client** adapters, reaching
//!   the shared `nova-responses-mem-server`. Execution is the separate
//!   `nova-agentd` process — the same process topology as production, with the
//!   carrier swapped for an in-memory double. This is what protocol-compatibility
//!   checks, local development and L2 verification run.
//! - `feature = "sql"`: the real carriers (Postgres + Redis); execution is the
//!   separate `nova-agentd` process. Built with `--no-default-features
//!   --features sql` so the release binary statically excludes mem.
//!
//! The `nova-responses` library and every port consumer below are unaware of
//! which backend is mounted — the choice exists only at this injection point.

#[cfg(all(feature = "sql", feature = "mem"))]
compile_error!(
    "mem and sql backends are mutually exclusive; build production with \
     `--no-default-features --features sql`"
);

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use nova_responses_core::{Clock, ContextStore, MetricsSink, ResponseEventLog, ResponseLedger};
use nova_responses::{AppState, Config, CountingMetrics, KeyTable, ResponsesService, SystemClock};
use tracing::info;

#[cfg(feature = "sql")]
const DEFAULT_CONFIG: &str = "testing/config/node-a-sql.toml";
#[cfg(not(feature = "sql"))]
const DEFAULT_CONFIG: &str = "testing/config/node-a.toml";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG)]
    config: PathBuf,
}

const ADMIN_KEY_ENV: &str = "NOVA_ADMIN_KEY";

/// The mounted ports, all as trait objects. Downstream sees only these; the
/// concrete adapter type is gone past this struct.
struct Ports {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    context: Arc<dyn ContextStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
}

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
        KeyTable::from_env(&cfg.api_keys_env, ADMIN_KEY_ENV)
            .map_err(|e| anyhow::anyhow!("api keys from ${}: {e}", cfg.api_keys_env))?,
    );

    let ports = mount(&cfg).await?;
    ports.ledger.set_pending_limit(cfg.pending_limit);

    // Liveness probe before serving: a node that cannot store would refuse every
    // `store: true` create, so failing fast is clearer than serving 503s.
    if let Err(e) = ports.context.health().await {
        bail!("context store is not reachable at startup: {e}");
    }

    let service = Arc::new(ResponsesService::new(
        ports.ledger.clone(),
        ports.event_log.clone(),
        ports.context.clone(),
        ports.clock.clone(),
        ports.metrics.clone(),
        cfg.clone(),
    ));

    let state = AppState {
        cfg: cfg.clone(),
        ledger: ports.ledger.clone(),
        event_log: ports.event_log.clone(),
        context: ports.context.clone(),
        clock: ports.clock.clone(),
        metrics: ports.metrics.clone(),
        keys,
        service,
        accepting: Arc::new(AtomicBool::new(true)),
    };

    // The sweep loop runs as a separate process under the shared carrier (its
    // single owner); the in-gateway copy is kept config-gated for the sql build
    // and is disabled in the mem fixtures.
    if cfg.run_sweeper {
        nova_responses::sweeper::spawn(nova_responses::sweeper::SweepDeps {
            ledger: state.ledger.clone(),
            event_log: state.event_log.clone(),
            context: state.context.clone(),
            clock: state.clock.clone(),
            metrics: state.metrics.clone(),
            heartbeat_ttl_ms: state.cfg.heartbeat_ttl_ms,
            retain_after_terminal_ms: state.cfg.retain_after_terminal_ms,
        });
    }

    let app = nova_responses::routes::router(state.clone());

    if let Some(parent) = args.config.parent() {
        let marker = parent.join(format!(".ready-{}", cfg.node_tag));
        let _ = std::fs::write(&marker, b"ok");
    }

    info!(
        node_tag = cfg.node_tag.as_str(),
        %addr,
        "nova-responses-gateway listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(nova_responses::shutdown::drain(state))
        .await?;

    info!("shutdown complete");
    Ok(())
}

/// Real carriers: Postgres + Redis. Startup fails outright when the integrity key
/// or the database is missing (INV-44 / INV-46).
#[cfg(feature = "sql")]
async fn mount(cfg: &Config) -> Result<Ports> {
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

    let redis_url = std::env::var(&cfg.redis_url_env)
        .with_context(|| format!("reading ${}", cfg.redis_url_env))?;
    let event_log = adapters_event_log_redis::RedisResponseEventLog::connect(
        &redis_url,
        sql.ledger.clone(),
        "resp",
    )
    .await
    .with_context(|| format!("connecting to the event buffer via ${}", cfg.redis_url_env))?;

    Ok(Ports {
        ledger: sql.ledger.clone(),
        event_log: Arc::new(event_log),
        context: sql.context.clone(),
        // Real wall clock: created_at / reap deadlines must use wall time, not a
        // frozen virtual clock.
        clock: Arc::new(SystemClock),
        metrics: Arc::new(CountingMetrics::default()),
    })
}

/// In-memory shared carrier (verification). The ledger, event buffer and context
/// are reached through the client adapters; execution is the separate
/// `nova-agentd` process, not embedded here.
#[cfg(feature = "mem")]
async fn mount(cfg: &Config) -> Result<Ports> {
    let url = std::env::var(&cfg.mem_server_url_env)
        .with_context(|| format!("reading ${}", cfg.mem_server_url_env))?;
    let world = adapters_mem_client::MemClientWorld::new(&url);

    Ok(Ports {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
        clock: Arc::new(SystemClock),
        metrics: Arc::new(CountingMetrics::default()),
    })
}

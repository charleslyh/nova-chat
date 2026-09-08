//! Gateway binary over [`nova_responses`]: thin assembly that mounts a backend.
//!
//! The backend is the mem carrier **client** adapters, reaching the shared
//! `nova-responses-mem-server`. Execution is the separate `nova-agentd` process
//! — the same process topology as production, with the carrier an in-memory
//! double. This is what protocol-compatibility checks, local development and L2
//! verification run.
//!
//! The `nova-responses` library and every port consumer below are unaware of
//! which backend is mounted — the choice exists only at this injection point.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use nova_responses_core::{
    Clock, ContextStore, ConversationStore, MetricsSink, ResponseEventLog, ResponseLedger,
};
use nova_responses::{
    AppState, ConversationsService, CountingMetrics, KeyTable, ResponsesService, SystemClock,
};
use tracing::info;

mod config;
use config::GatewayConfig;

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
    conversation: Arc<dyn ConversationStore>,
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
    let gateway_cfg = GatewayConfig::load(&args.config)?;
    let mem_server_url_env = gateway_cfg.mem_server_url_env.clone();
    let addr: SocketAddr = gateway_cfg
        .listen
        .parse()
        .with_context(|| format!("listen address `{}`", gateway_cfg.listen))?;
    let cfg = Arc::new(gateway_cfg.responses);

    let keys = Arc::new(
        KeyTable::from_env(&cfg.api_keys_env, ADMIN_KEY_ENV)
            .map_err(|e| anyhow::anyhow!("api keys from ${}: {e}", cfg.api_keys_env))?,
    );

    let ports = mount(&mem_server_url_env).await?;
    ports.ledger.set_pending_limit(cfg.pending_limit);

    // Liveness probe before serving: a node that cannot store would refuse every
    // `store: true` create, so failing fast is clearer than serving 503s. The
    // conversation store is probed for the same reason — a gateway that answers
    // `/v1/conversations` with a 503 on every call is worse than one that never
    // came up.
    if let Err(e) = ports.context.health().await {
        bail!("context store is not reachable at startup: {e}");
    }
    if let Err(e) = ports.conversation.health().await {
        bail!("conversation store is not reachable at startup: {e}");
    }

    // Assembly order follows the dependency direction: conversations know about
    // content, responses knows about conversations.
    let conversations = Arc::new(ConversationsService::new(
        ports.conversation.clone(),
        ports.context.clone(),
        ports.clock.clone(),
        ports.metrics.clone(),
        cfg.clone(),
    ));
    let service = Arc::new(ResponsesService::new(
        ports.ledger.clone(),
        ports.event_log.clone(),
        ports.context.clone(),
        conversations.clone(),
        ports.clock.clone(),
        ports.metrics.clone(),
        cfg.clone(),
    ));

    let state = AppState {
        cfg: cfg.clone(),
        ledger: ports.ledger.clone(),
        event_log: ports.event_log.clone(),
        context: ports.context.clone(),
        conversation_store: ports.conversation.clone(),
        clock: ports.clock.clone(),
        metrics: ports.metrics.clone(),
        keys,
        service,
        conversations,
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
            conversations: state.conversation_store.clone(),
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

/// In-memory shared carrier. The ledger, event buffer and context are reached
/// through the client adapters; execution is the separate `nova-agentd` process,
/// not embedded here.
async fn mount(mem_server_url_env: &str) -> Result<Ports> {
    let url = std::env::var(mem_server_url_env)
        .with_context(|| format!("reading ${mem_server_url_env}"))?;
    let world = adapters_mem_client::MemClientWorld::new(&url);

    Ok(Ports {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
        conversation: world.conversation.clone(),
        clock: Arc::new(SystemClock),
        metrics: Arc::new(CountingMetrics::default()),
    })
}

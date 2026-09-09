//! Gateway binary over [`nova_responses`]: thin assembly that mounts a backend.
//!
//! The backend is the mem carrier **client** adapters, reaching the shared
//! `nova-responses-mem-server`. Execution is the separate `nova-agentd-mock` process —
//! the same process topology as production, with the carrier an in-memory double. This
//! is what protocol-compatibility checks, local development and L2 verification run.
//!
//! The `nova-responses` library and every port consumer below are unaware of which
//! backend is mounted — the choice exists only at this injection point.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use clap::Parser;
use nova_responses::ports::{ConversationStore, MetricsSink, ResponseEventLog, ResponseLedger};
use nova_responses::service::{ConversationsService, ResponsesDeps, ResponsesService};
use nova_responses::{Clock, SystemClock};
use tracing::info;

use nova_responses_gateway::metrics::NoopMetrics;
use nova_responses_gateway::{AppState, GatewayConfig, KeyTable};

const DEFAULT_CONFIG: &str = "verify/config/node-a.toml";

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = DEFAULT_CONFIG)]
    config: PathBuf,
}

const ADMIN_KEY_ENV: &str = "NOVA_ADMIN_KEY";

/// The mounted ports, all as trait objects. Downstream sees only these; the concrete
/// adapter type is gone past this struct.
struct Ports {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    conversation: Arc<dyn ConversationStore>,
    clock: Arc<dyn Clock>,
    metrics: Arc<dyn MetricsSink>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let cfg = Arc::new(GatewayConfig::load(&args.config)?);
    let addr: SocketAddr = cfg
        .listen
        .parse()
        .with_context(|| format!("listen address `{}`", cfg.listen))?;

    let keys = Arc::new(
        KeyTable::from_env(&cfg.api_keys_env, ADMIN_KEY_ENV)
            .map_err(|e| anyhow::anyhow!("api keys from ${}: {e}", cfg.api_keys_env))?,
    );

    let ports = mount(&cfg.mem_server_url_env).await?;
    ports.ledger.set_pending_limit(cfg.pending_limit);

    // Liveness probe before serving: a gateway that answers `/v1/conversations` with a
    // 503 on every call is worse than one that never came up.
    if let Err(e) = ports.conversation.health().await {
        bail!("conversation store is not reachable at startup: {e}");
    }

    // Assembly order follows the dependency direction.
    let conversations = Arc::new(ConversationsService::new(
        ports.conversation.clone(),
        ports.clock.clone(),
        ports.metrics.clone(),
    ));
    let service = Arc::new(ResponsesService::new(ResponsesDeps {
        ledger: ports.ledger.clone(),
        event_log: ports.event_log.clone(),
        conversations: conversations.clone(),
        turn_lock: ports.conversation.clone(),
        clock: ports.clock.clone(),
        metrics: ports.metrics.clone(),
        cfg: Arc::new(cfg.responses.clone()),
    }));

    let state = AppState {
        cfg: cfg.clone(),
        ledger: ports.ledger.clone(),
        event_log: ports.event_log.clone(),
        // Each consumer takes the facet it needs; the whole store stays here.
        conversation_events: ports.conversation.clone(),
        conversation_repo: ports.conversation.clone(),
        clock: ports.clock.clone(),
        metrics: ports.metrics.clone(),
        keys,
        service: service.clone(),
        conversations,
        accepting: Arc::new(AtomicBool::new(true)),
    };

    // Start the responses service: this also spawns its sweeper. The assembly layer
    // knows only "start the service"; what background work that entails is the
    // service's own business.
    service.start();

    let app = nova_responses_gateway::routes::router(state.clone());

    if let Some(parent) = args.config.parent() {
        let marker = parent.join(format!(".ready-{}", cfg.responses.node_tag));
        let _ = std::fs::write(&marker, b"ok");
    }

    info!(
        node_tag = cfg.responses.node_tag.as_str(),
        %addr,
        "nova-responses-gateway listening"
    );

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(nova_responses_gateway::shutdown::drain(state.clone()))
        .await?;

    // Drain has finished (in-flight work done); stop the service's background work
    // before the process exits.
    state.service.stop();

    info!("shutdown complete");
    Ok(())
}

/// In-memory shared carrier. The ledger, event buffer and conversations are reached
/// through the client adapters; execution is the separate `nova-agentd-mock` process, not
/// embedded here.
async fn mount(mem_server_url_env: &str) -> Result<Ports> {
    let url = std::env::var(mem_server_url_env)
        .with_context(|| format!("reading ${mem_server_url_env}"))?;
    let world = mock_client::MemClientWorld::new(&url);

    Ok(Ports {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        conversation: world.conversation.clone(),
        clock: Arc::new(SystemClock),
        metrics: Arc::new(NoopMetrics),
    })
}

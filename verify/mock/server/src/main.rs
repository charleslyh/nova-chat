//! `nova-responses-mem-server` — the in-memory shared carrier for L2.
//!
//! Two physically separate surfaces:
//!
//! - **data plane** (`--listen`): gateway / agentd / sweeper reach the shared
//!   ledger, event log and context store through `POST /rpc`. This is the
//!   production-shaped path; it must never be touched by test logic.
//! - **control plane** (`--control-listen`): the test controller injects faults
//!   (`unavailable`, tamper, clock advance). This is the only "God" surface.
//!
//! The carrier deliberately performs **no admission control**: the storage
//! degrade switch (`read_only`) is per-node (client-side) state, exactly as the
//! sql adapter holds it as a process-local atomic. The carrier only owns
//! *shared* state and the `unavailable` fault that triggers each node's own
//! degrade.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use clap::Parser;
use nova_responses::{HmacSha256Integrity, ResponseId, ResponseItem};
use nova_responses::ports::ContentIntegrity;
use serde::Deserialize;

use mock_server::proto::{Request, Response};
use mock_server::{server::dispatch, MemWorld, MemWorldConfig};

#[derive(Debug, Parser)]
struct Args {
    /// Data plane: where gateway / agentd / sweeper connect.
    #[arg(long, default_value = "127.0.0.1:19000")]
    listen: String,

    /// Control plane: where the test controller injects faults.
    #[arg(long, default_value = "127.0.0.1:19001")]
    control_listen: String,

    /// Verify content integrity using the key from $NOVA_INTEGRITY_KEY (INV-44).
    #[arg(long, default_value_t = true)]
    verify_integrity: bool,

    #[arg(long, default_value_t = 20_000)]
    events_per_response: usize,
    #[arg(long, default_value_t = 100_000)]
    max_logs: usize,
    #[arg(long, default_value_t = 100_000)]
    max_records: usize,
    /// Upper bound on one conversation's event stream. Reaching it refuses the
    /// append rather than evicting, unlike `--events-per-response`.
    #[arg(long, default_value_t = 100_000)]
    events_per_conversation: usize,
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

    let integrity: Option<Arc<dyn ContentIntegrity>> = if args.verify_integrity {
        Some(Arc::new(
            HmacSha256Integrity::from_env().context("reading $NOVA_INTEGRITY_KEY (INV-44)")?,
        ))
    } else {
        None
    };

    let world = Arc::new(MemWorld::with_integrity(
        MemWorldConfig {
            events_per_response: args.events_per_response,
            max_logs: args.max_logs,
            max_records: args.max_records,
            events_per_conversation: args.events_per_conversation,
            verify_integrity: args.verify_integrity,
        },
        integrity,
    ));

    let data_addr: SocketAddr = args.listen.parse().context("parse --listen")?;
    let control_addr: SocketAddr = args
        .control_listen
        .parse()
        .context("parse --control-listen")?;

    let data = Router::new()
        .route("/rpc", post(rpc_handler))
        .with_state(world.clone());
    let control = control_router(world.clone());

    tracing::info!(%data_addr, %control_addr, "mem-server starting");

    let data_task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(data_addr).await?;
        axum::serve(listener, data).await?;
        Ok::<(), anyhow::Error>(())
    });
    let control_task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(control_addr).await?;
        axum::serve(listener, control).await?;
        Ok::<(), anyhow::Error>(())
    });

    let (a, b) = tokio::join!(data_task, control_task);
    a.context("data plane")??;
    b.context("control plane")??;
    Ok(())
}

async fn rpc_handler(
    State(world): State<Arc<MemWorld>>,
    Json(req): Json<Request>,
) -> Json<Response> {
    Json(dispatch(&world, req).await)
}

// --- control plane ---

fn control_router(world: Arc<MemWorld>) -> Router {
    Router::new()
        .route("/unavailable", post(set_unavailable))
        .route("/tamper", post(tamper))
        .route("/advance_clock", post(advance_clock))
        .route("/set_clock", post(set_clock))
        .with_state(world)
}

#[derive(Deserialize)]
struct UnavailableReq {
    unavailable: bool,
}

/// Simulate the carrier going down / recovering (INV-32 / INV-46). Each node's
/// own health check turns this into its per-node read-only degrade.
async fn set_unavailable(
    State(world): State<Arc<MemWorld>>,
    Json(req): Json<UnavailableReq>,
) -> Json<()> {
    world.store.set_unavailable(req.unavailable);
    Json(())
}

#[derive(Deserialize)]
struct TamperReq {
    response_id: ResponseId,
    items: Vec<ResponseItem>,
}

/// Corrupt stored content without updating the integrity tag (CR-13).
async fn tamper(
    State(world): State<Arc<MemWorld>>,
    Json(req): Json<TamperReq>,
) -> Json<bool> {
    Json(world.ledger.tamper_for_test(&req.response_id, req.items))
}

#[derive(Deserialize)]
struct ClockAdvanceReq {
    delta_ms: u64,
}

#[derive(Deserialize)]
struct ClockSetReq {
    ms: u64,
}

/// Advance the virtual clock to drive reap / retention / expiry deterministically.
async fn advance_clock(
    State(world): State<Arc<MemWorld>>,
    Json(req): Json<ClockAdvanceReq>,
) -> Json<()> {
    world.clock.advance(req.delta_ms);
    Json(())
}

async fn set_clock(
    State(world): State<Arc<MemWorld>>,
    Json(req): Json<ClockSetReq>,
) -> Json<()> {
    world.clock.set(req.ms);
    Json(())
}

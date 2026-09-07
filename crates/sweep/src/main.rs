//! `nova-responses-sweep` — the standalone maintenance process.
//!
//! Under a shared carrier (mem or sql) there is a single owner for reaping lost
//! claims, releasing expired event buffers and clearing expired content. Running
//! it as its own process keeps that ownership explicit and matches the production
//! deployment shape. The gateway does not run the loop in the L2 mem fixtures
//! (and the same is the target topology for sql once `run_sweeper` is retired).

#[cfg(all(feature = "sql", feature = "mem"))]
compile_error!(
    "mem and sql backends are mutually exclusive; build production with \
     `--no-default-features --features sql`"
);

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nova_responses::{CountingMetrics, SystemClock};
use nova_responses_core::{ContextStore, ConversationStore, ResponseEventLog, ResponseLedger};

#[derive(Debug, Parser)]
struct Args {
    /// Env var naming the database URL (SEC-4: never the URL itself).
    #[arg(long, default_value = "NOVA_DATABASE_URL")]
    database_url_env: String,

    /// Env var naming the Redis URL.
    #[arg(long, default_value = "NOVA_REDIS_URL")]
    redis_url_env: String,

    /// Env var naming the mem carrier's data-plane URL (verification only).
    #[arg(long, default_value = "NOVA_MEM_SERVER_URL")]
    mem_server_url_env: String,

    /// Claim heartbeat TTL; claims whose owner stops heartbeating past this are
    /// reaped (short in fixtures so recovery is observable).
    #[arg(long, default_value_t = 90_000)]
    heartbeat_ttl_ms: u64,

    /// How long a terminal response's events stay readable.
    #[arg(long, default_value_t = 60_000)]
    retain_after_terminal_ms: u64,
}

/// The mounted ports, all as trait objects. The concrete adapter is gone past
/// this struct, so the sweep loop is identical across backends.
struct Backend {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    context: Arc<dyn ContextStore>,
    /// Reaping is a terminal transition, so it owes the conversation a marker
    /// release — and it is the only release a reaped response gets, since its
    /// holder is gone and the fence has moved (D28).
    conversation: Arc<dyn ConversationStore>,
}

/// Real carriers: Postgres + Redis.
#[cfg(feature = "sql")]
async fn mount_sql(args: &Args) -> Result<Backend> {
    let sql = adapters_sql::SqlWorld::connect_from_env(&args.database_url_env, Default::default())
        .await?;
    let redis_url = std::env::var(&args.redis_url_env)
        .with_context(|| format!("reading ${}", args.redis_url_env))?;
    let event_log = adapters_event_log_redis::RedisResponseEventLog::connect(
        &redis_url,
        sql.ledger.clone(),
        "resp",
    )
    .await?;
    Ok(Backend {
        ledger: sql.ledger.clone(),
        event_log: Arc::new(event_log),
        context: sql.context.clone(),
        conversation: sql.conversation.clone(),
    })
}

/// In-memory shared carrier (verification).
#[cfg(feature = "mem")]
async fn mount_mem(args: &Args) -> Result<Backend> {
    let url = std::env::var(&args.mem_server_url_env)
        .with_context(|| format!("reading ${}", args.mem_server_url_env))?;
    let world = adapters_mem_client::MemClientWorld::new(&url);
    Ok(Backend {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
        conversation: world.conversation.clone(),
    })
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

    #[cfg(feature = "sql")]
    let backend = mount_sql(&args).await?;
    #[cfg(feature = "mem")]
    let backend = mount_mem(&args).await?;

    nova_responses::sweeper::spawn(nova_responses::sweeper::SweepDeps {
        ledger: backend.ledger,
        event_log: backend.event_log,
        context: backend.context,
        conversations: backend.conversation,
        clock: Arc::new(SystemClock),
        metrics: Arc::new(CountingMetrics::default()),
        heartbeat_ttl_ms: args.heartbeat_ttl_ms,
        retain_after_terminal_ms: args.retain_after_terminal_ms,
    });

    tracing::info!("sweep process starting");
    // Run until signaled; the sweep loop itself is a spawned task.
    tokio::signal::ctrl_c().await.context("waiting for shutdown")?;
    tracing::info!("sweep process shutting down");
    Ok(())
}
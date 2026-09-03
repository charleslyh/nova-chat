//! `nova-agentd` — the standalone execution daemon (D25).
//!
//! Claims queued responses from the shared ledger, runs the ReAct loop, and
//! streams increments into the shared event buffer. It is deliberately **not** an
//! HTTP service: it reaches the ledger and event buffer through ports, so swapping
//! the concrete adapters (Postgres / Redis / mem carrier) never touches this loop.
//!
//! The gateway only enqueues responses and serves delivery modes; execution is
//! fully decoupled here, so the gateway can crash without interrupting a
//! generation, and the fleet of agents can scale independently.
//!
//! Two backends behind features, exactly as the gateway:
//! - `mem` (default): the shared in-memory carrier, for L2 verification.
//! - `sql`: Postgres + Redis, the production carriers.

#[cfg(all(feature = "sql", feature = "mem"))]
compile_error!(
    "mem and sql backends are mutually exclusive; build production with \
     `--no-default-features --features sql`"
);

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use nova_agent::{Agent, AgentConfig, AgentDeps, Executed};
use nova_responses_core::{
    ChainLimits, CompletionsRequestScheduler, ContextStore, NoopToolExecutor, ResponseEventLog,
    ResponseLedger,
};
use tokio::sync::Semaphore;
use tracing::info;

use adapters_completions_mock::{EchoScheduler, ScriptedScheduler};

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

    /// Bounds *this process*: how many generations may execute concurrently.
    #[arg(long, default_value_t = 8)]
    max_concurrent: usize,

    /// Execution deadline per claim, in milliseconds.
    #[arg(long, default_value_t = 3_600_000)]
    exec_ttl_ms: u64,

    /// How long a terminal response's events stay readable, in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    retain_after_terminal_ms: u64,

    /// Chain limits fed to the agent (D24).
    #[arg(long, default_value_t = 50)]
    chain_max_depth: usize,
    #[arg(long, default_value_t = 1000)]
    chain_max_items: usize,
    #[arg(long, default_value_t = 1_048_576)]
    chain_max_bytes: usize,

    /// `echo` or `scripted` (verification schedulers).
    #[arg(long, default_value = "echo")]
    scheduler: String,
    /// Script path, required when `--scheduler scripted`.
    #[arg(long)]
    scheduler_script: Option<String>,
}

/// The mounted ports, all as trait objects. The concrete adapter is gone past
/// this struct, so the agent loop never knows which carrier it is on.
struct Backend {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    context: Arc<dyn ContextStore>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

fn build_scheduler(args: &Args) -> Result<Arc<dyn CompletionsRequestScheduler>> {
    match args.scheduler.as_str() {
        "echo" => Ok(Arc::new(EchoScheduler::new(8))),
        "scripted" => {
            let path = args
                .scheduler_script
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--scheduler-script is required for scripted"))?;
            let src = std::fs::read_to_string(path)
                .with_context(|| format!("reading scheduler script {path}"))?;
            Ok(Arc::new(
                ScriptedScheduler::from_yaml(&src)
                    .with_context(|| format!("parsing scheduler script {path}"))?,
            ))
        }
        other => anyhow::bail!("unknown scheduler `{other}` (expected echo or scripted)"),
    }
}

/// Real carriers: Postgres + Redis.
#[cfg(feature = "sql")]
async fn mount_sql(args: &Args) -> Result<Backend> {
    let sql = adapters_sql::SqlWorld::connect_from_env(&args.database_url_env, Default::default())
        .await?;
    let redis_url = std::env::var(&args.redis_url_env)
        .with_context(|| format!("reading ${}", args.redis_url_env))?;
    let event_log =
        adapters_event_log_redis::RedisResponseEventLog::connect(&redis_url, sql.ledger.clone(), "resp")
            .await?;
    Ok(Backend {
        ledger: sql.ledger.clone(),
        event_log: Arc::new(event_log),
        context: sql.context.clone(),
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
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    #[cfg(feature = "sql")]
    let backend = mount_sql(&args).await?;
    #[cfg(feature = "mem")]
    let backend = mount_mem(&args).await?;

    let scheduler = build_scheduler(&args)?;

    let agent = Arc::new(Agent::new(
        AgentDeps {
            ledger: backend.ledger,
            event_log: backend.event_log,
            context: backend.context,
            scheduler,
            tools: Arc::new(NoopToolExecutor),
        },
        AgentConfig {
            exec_ttl_ms: args.exec_ttl_ms,
            chain_limits: ChainLimits {
                max_depth: args.chain_max_depth,
                max_items: args.chain_max_items,
                max_bytes: args.chain_max_bytes,
            },
            retain_after_terminal_ms: args.retain_after_terminal_ms,
            tool_specs: Vec::new(),
            ..AgentConfig::default()
        },
    ));

    info!(
        max_concurrent = args.max_concurrent,
        exec_ttl_ms = args.exec_ttl_ms,
        "execution daemon starting"
    );

    // Bounded concurrency: a backlog of queued generations must not spawn one
    // outbound call each. Provider-side limits live in the scheduler adapter.
    let permits = Arc::new(Semaphore::new(args.max_concurrent));
    loop {
        // Poll interval. Claim is a low-frequency operation (~ ledger write rate),
        // so a short sleep is enough to keep first-token latency low without a
        // tight spin; the incremental stream itself is pushed to the shared buffer.
        tokio::time::sleep(Duration::from_millis(100)).await;

        let Ok(permit) = permits.clone().try_acquire_owned() else {
            continue; // at capacity; retry next tick
        };

        let agent = agent.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let now = now_ms();
            match agent.run_once(now).await {
                Executed::Idle => {}
                Executed::Completed => info!("generation completed"),
                Executed::Superseded => {}
                Executed::Failed => info!("generation failed"),
            }
        });
    }
}

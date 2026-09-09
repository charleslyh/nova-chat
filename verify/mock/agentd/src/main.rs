//! `nova-agentd-mock` — the mock verification process.
//!
//! Assembles the storage clients (mem), a mock agent runner (completions-mock /
//! completions-http + calculator), and the [`AgentRuntime`] orchestrator, then
//! runs the claim/poll loop. This is a verification fixture, not a production
//! carrier: production execution integrates a real agent SDK against redis/mq.

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use nova_agent_runtime::{AgentRuntime, AgentRuntimeConfig, AgentRuntimeDeps};
use nova_responses::{ChainLimits, ConversationStore, ResponseEventLog, ResponseLedger};
use tracing::info;

use mock_agentd::{
    CalculatorTool, EchoScheduler, HttpChatCompletionsScheduler, MockAgentRunner, Scheduler,
    ScriptedScheduler, ToolExecutor,
};

#[derive(Debug, Parser)]
struct Args {
    /// Env var naming the mem carrier's data-plane URL.
    #[arg(long, default_value = "NOVA_MEM_SERVER_URL")]
    mem_server_url_env: String,

    /// Bounds *this process*: how many generations may execute concurrently.
    #[arg(long, default_value_t = 8)]
    max_concurrent: usize,

    /// Claim poll interval, in milliseconds.
    #[arg(long, default_value_t = 100)]
    poll_interval_ms: u64,

    /// Execution deadline per claim, in milliseconds.
    #[arg(long, default_value_t = 3_600_000)]
    exec_ttl_ms: u64,

    /// Keep-alive heartbeat interval while a generation runs, in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    heartbeat_interval_ms: u64,

    /// How long a terminal response's events stay readable, in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    retain_after_terminal_ms: u64,

    /// Graceful shutdown drain budget, in milliseconds.
    #[arg(long, default_value_t = 60_000)]
    drain_timeout_ms: u64,

    /// Chain limits fed to the orchestrator (D24).
    #[arg(long, default_value_t = 50)]
    chain_max_depth: usize,
    #[arg(long, default_value_t = 1000)]
    chain_max_items: usize,
    #[arg(long, default_value_t = 1_048_576)]
    chain_max_bytes: usize,

    /// `echo` or `scripted` (verification schedulers) or `http` (real provider).
    #[arg(long, default_value = "echo")]
    scheduler: String,
    /// Script path, required when `--scheduler scripted`.
    #[arg(long)]
    scheduler_script: Option<String>,

    /// Env var naming the chat-completions base URL (for `--scheduler http`).
    #[arg(long, default_value = "NOVA_CHAT_BASE_URL")]
    http_base_url_env: String,
    /// Env var naming the chat-completions API key (SEC-4: never the key itself).
    #[arg(long, default_value = "NOVA_CHAT_API_KEY")]
    http_api_key_env: String,
    /// Env var naming the fallback model (for `--scheduler http`).
    #[arg(long, default_value = "NOVA_CHAT_MODEL")]
    http_model_env: String,
}

/// The mounted storage ports, all as trait objects.
struct Backend {
    ledger: Arc<dyn ResponseLedger>,
    event_log: Arc<dyn ResponseEventLog>,
    conversation: Arc<dyn ConversationStore>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

fn build_scheduler(args: &Args) -> Result<Arc<dyn Scheduler>> {
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
        "http" => {
            let base_url = std::env::var(&args.http_base_url_env)
                .with_context(|| format!("reading ${}", args.http_base_url_env))?;
            let api_key = std::env::var(&args.http_api_key_env)
                .with_context(|| format!("reading ${}", args.http_api_key_env))?;
            let model = std::env::var(&args.http_model_env)
                .ok()
                .filter(|v| !v.is_empty());
            Ok(Arc::new(
                HttpChatCompletionsScheduler::new(base_url, api_key, model)
                    .map_err(anyhow::Error::msg)?,
            ))
        }
        other => anyhow::bail!("unknown scheduler `{other}` (expected echo, scripted or http)"),
    }
}

async fn mount_mem(args: &Args) -> Result<Backend> {
    let url = std::env::var(&args.mem_server_url_env)
        .with_context(|| format!("reading ${}", args.mem_server_url_env))?;
    let world = mock_client::MemClientWorld::new(&url);
    Ok(Backend {
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        conversation: world.conversation.clone(),
    })
}

async fn wait_for_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();

    let backend = mount_mem(&args).await?;

    // Assembly order: toolbox → runner (injecting toolbox + scheduler) → runtime.
    let toolbox: Arc<dyn ToolExecutor> = Arc::new(CalculatorTool);
    let scheduler = build_scheduler(&args)?;
    let runner = Arc::new(MockAgentRunner::new(scheduler, toolbox));

    let runtime = Arc::new(AgentRuntime::new(
        AgentRuntimeDeps {
            ledger: backend.ledger,
            event_log: backend.event_log,
            runner,
            now: Arc::new(now_ms),
            conversations: Some(backend.conversation),
        },
        AgentRuntimeConfig {
            exec_ttl_ms: args.exec_ttl_ms,
            heartbeat_interval_ms: args.heartbeat_interval_ms,
            chain_limits: ChainLimits {
                max_depth: args.chain_max_depth,
                max_items: args.chain_max_items,
                max_bytes: args.chain_max_bytes,
            },
            retain_after_terminal_ms: args.retain_after_terminal_ms,
            ..AgentRuntimeConfig::default()
        },
    ));

    info!(
        max_concurrent = args.max_concurrent,
        poll_interval_ms = args.poll_interval_ms,
        "mock execution daemon starting"
    );

    let handle = runtime.start(args.max_concurrent, args.poll_interval_ms);

    wait_for_signal().await;
    info!("shutdown signal received; draining");
    handle.stop(args.drain_timeout_ms).await;
    info!("shutdown complete");
    Ok(())
}

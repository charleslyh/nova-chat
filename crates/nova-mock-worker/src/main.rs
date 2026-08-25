//! Executable mock worker agent for L2 / sim console.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18080")]
    home: String,
    #[arg(long, default_value_t = 8)]
    capacity: u32,
    #[arg(long, default_value = "agent")]
    kind_bias: String,
    /// Stable worker id (sim console assigns); random if omitted.
    #[arg(long)]
    worker_id: Option<String>,
    #[arg(long, default_value = "default")]
    region: String,
    /// Artificial work delay before COMPLETE (ms).
    #[arg(long, default_value_t = 200)]
    work_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let worker_id = match &args.worker_id {
        Some(s) => Uuid::parse_str(s)?,
        None => Uuid::new_v4(),
    };
    info!(
        %worker_id,
        region = %args.region,
        capacity = args.capacity,
        bias = %args.kind_bias,
        "mock-worker starting"
    );

    loop {
        match try_claim_and_complete(&args, worker_id).await {
            Ok(Some(msg)) => info!(region = %args.region, "{msg}"),
            Ok(None) => {}
            Err(e) => tracing::warn!(region = %args.region, "claim err: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

async fn try_claim_and_complete(args: &Args, worker_id: Uuid) -> Result<Option<String>> {
    let mut stream = TcpStream::connect(&args.home).await?;
    let req = format!("CLAIM {worker_id} {}\n", args.capacity);
    stream.write_all(req.as_bytes()).await?;
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await?;
    let resp = std::str::from_utf8(&buf[..n])?.trim().to_string();
    if !resp.starts_with("OK ") {
        return Ok(None);
    }
    // OK task_id attempt units kind
    let parts: Vec<_> = resp.split_whitespace().collect();
    if parts.len() < 5 {
        return Ok(Some(format!("claimed (legacy) {resp}")));
    }
    let task_id = parts[1];
    let attempt = parts[2];
    let units = parts[3];
    let kind = parts[4];
    tokio::time::sleep(Duration::from_millis(args.work_ms)).await;

    let mut stream = TcpStream::connect(&args.home).await?;
    let complete = format!("COMPLETE {task_id} {attempt} {worker_id} {units} 1\n");
    stream.write_all(complete.as_bytes()).await?;
    let mut buf = vec![0u8; 64];
    let _ = stream.read(&mut buf).await?;

    Ok(Some(format!(
        "done task={task_id} kind={kind} attempt={attempt}"
    )))
}

//! Mock agent: pull turns, append text_delta tokens, complete.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use serde::Deserialize;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:18080")]
    home: String,
    #[arg(long)]
    agent_id: Option<String>,
    #[arg(long, default_value = "default")]
    region: String,
    #[arg(long, default_value_t = 40)]
    token_ms: u64,
    #[arg(long, default_value_t = 8)]
    tokens: usize,
}

#[derive(Debug, Deserialize)]
struct ClaimResp {
    turn_id: Uuid,
    session_id: Uuid,
    attempt: u64,
    text: String,
    #[allow(dead_code)]
    exec_deadline_ms: u64,
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
    let agent_id = match &args.agent_id {
        Some(s) => Uuid::parse_str(s)?,
        None => Uuid::new_v4(),
    };
    let base = format!("http://{}", args.home.trim_start_matches("http://"));
    let client = reqwest::Client::new();
    info!(%agent_id, region = %args.region, home = %base, "mock-agent starting");

    loop {
        let _ = client
            .post(format!("{base}/v1/agent/heartbeat"))
            .json(&serde_json::json!({"agent_id": agent_id}))
            .send()
            .await;

        let resp = client
            .post(format!("{base}/v1/agent/claim"))
            .json(&serde_json::json!({"agent_id": agent_id}))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            tokio::time::sleep(Duration::from_millis(400)).await;
            continue;
        }
        if !resp.status().is_success() {
            tracing::warn!(status = %resp.status(), "claim failed");
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }
        let claim: ClaimResp = resp.json().await?;
        info!(
            turn = %claim.turn_id,
            session = %claim.session_id,
            attempt = claim.attempt,
            "claimed"
        );

        let mut assistant = String::new();
        for i in 0..args.tokens {
            let piece = format!("tok{i} ");
            assistant.push_str(&piece);
            let r = client
                .post(format!("{base}/v1/agent/append"))
                .json(&serde_json::json!({
                    "session_id": claim.session_id,
                    "turn_id": claim.turn_id,
                    "attempt": claim.attempt,
                    "kind": "text_delta",
                    "payload": piece,
                }))
                .send()
                .await?;
            if r.status() == reqwest::StatusCode::CONFLICT {
                tracing::warn!("stale attempt — aborting turn");
                break;
            }
            tokio::time::sleep(Duration::from_millis(args.token_ms)).await;
        }

        let _ = client
            .post(format!("{base}/v1/agent/complete"))
            .json(&serde_json::json!({
                "session_id": claim.session_id,
                "turn_id": claim.turn_id,
                "attempt": claim.attempt,
                "ok": true,
                "assistant_text": format!("echo: {}", claim.text),
            }))
            .send()
            .await;
    }
}

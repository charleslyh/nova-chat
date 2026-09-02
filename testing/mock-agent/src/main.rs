//! Mock execution side: claim a response, stream deltas, submit final output.
//!
//! The important detail is in `complete`: the final output items are **sent
//! explicitly**, not reconstructed from the deltas that were streamed. Deriving
//! them from the event stream would force the event log to become a durable
//! source of truth and break the storage boundary (INV-48).
//!
//! It also demonstrates the split: `input` arrives already assembled by the
//! server from the context chain, and `instructions` arrives separately because
//! it is not an item.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use serde::Deserialize;
use serde_json::{json, Value};
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Parser)]
struct Args {
    /// Any peer node: they are equivalent.
    #[arg(long, default_value = "127.0.0.1:18080")]
    gateway: String,
    #[arg(long)]
    agent_id: Option<String>,
    #[arg(long, default_value_t = 40)]
    token_ms: u64,
    #[arg(long, default_value_t = 8)]
    tokens: usize,
}

#[derive(Debug, Deserialize)]
struct ClaimResponse {
    response_id: String,
    attempt: u64,
    #[allow(dead_code)]
    model: String,
    /// Full context assembled server-side: chain history plus this turn's input.
    input: Vec<Value>,
    /// Prepended as a system/developer message. Never inherited from a previous
    /// turn — if this is null, the caller sent none *this* turn.
    instructions: Option<String>,
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
    let base = format!("http://{}", args.gateway.trim_start_matches("http://"));
    let client = reqwest::Client::new();
    info!(%agent_id, gateway = %base, "mock-agent starting");

    loop {
        let _ = client
            .post(format!("{base}/v1/agent/heartbeat"))
            .json(&json!({ "agent_id": agent_id }))
            .send()
            .await;

        let resp = client
            .post(format!("{base}/v1/agent/claim"))
            .json(&json!({ "agent_id": agent_id }))
            .send()
            .await?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            tokio::time::sleep(Duration::from_millis(400)).await;
            continue;
        }
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "claim failed");
            tokio::time::sleep(Duration::from_millis(500)).await;
            continue;
        }

        let claim: ClaimResponse = resp.json().await?;
        info!(
            response = %claim.response_id,
            attempt = claim.attempt,
            context_items = claim.input.len(),
            has_instructions = claim.instructions.is_some(),
            "claimed"
        );

        // Echo the last user text so multi-turn behaviour is visible in the
        // console.
        let last_user_text = extract_last_user_text(&claim.input);
        let answer = format!("echo: {last_user_text}");

        let mut streamed = String::new();
        let mut aborted = false;
        for chunk in chunk_text(&answer, args.tokens) {
            streamed.push_str(&chunk);
            let r = client
                .post(format!("{base}/v1/agent/append"))
                .json(&json!({
                    "response_id": claim.response_id,
                    "attempt": claim.attempt,
                    "kind": "response.output_text.delta",
                    "payload": chunk,
                }))
                .send()
                .await?;
            if r.status() == reqwest::StatusCode::CONFLICT {
                // The fence moved: this attempt was superseded, so its partial
                // output must be abandoned rather than mixed with the new one.
                warn!("stale attempt — abandoning this attempt");
                aborted = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(args.token_ms)).await;
        }
        if aborted {
            continue;
        }

        let r = client
            .post(format!("{base}/v1/agent/complete"))
            .json(&json!({
                "response_id": claim.response_id,
                "attempt": claim.attempt,
                "ok": true,
                // Submitted explicitly. Note this is the *normalised* result, not
                // a concatenation of what happened to be streamed.
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{ "type": "output_text", "text": answer }],
                }],
                "input_tokens": claim.input.len() as u64 * 4,
                "output_tokens": answer.len() as u64 / 4,
            }))
            .send()
            .await;
        match r {
            Ok(resp) if resp.status().is_success() => {
                info!(response = %claim.response_id, "completed");
            }
            Ok(resp) => warn!(status = %resp.status(), "complete rejected"),
            Err(e) => warn!(error = %e, "complete failed"),
        }
    }
}

/// Pull the most recent user text out of assembled context, for a readable echo.
fn extract_last_user_text(input: &[Value]) -> String {
    for item in input.iter().rev() {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        if item.get("role").and_then(Value::as_str) != Some("user") {
            continue;
        }
        if let Some(parts) = item.get("content").and_then(Value::as_array) {
            for part in parts {
                if part.get("type").and_then(Value::as_str) == Some("input_text") {
                    if let Some(text) = part.get("text").and_then(Value::as_str) {
                        return text.to_string();
                    }
                }
            }
        }
    }
    "(no user text)".into()
}

/// Split into roughly `n` pieces so streaming is observable.
fn chunk_text(text: &str, n: usize) -> Vec<String> {
    if n == 0 || text.is_empty() {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    let size = chars.len().div_ceil(n.max(1)).max(1);
    chars
        .chunks(size)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_the_most_recent_user_text() {
        let input = vec![
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"old"}]}),
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"reply"}]}),
            json!({"type":"message","role":"user","content":[{"type":"input_text","text":"new"}]}),
        ];
        assert_eq!(extract_last_user_text(&input), "new");
    }

    #[test]
    fn tolerates_context_without_user_text() {
        assert_eq!(extract_last_user_text(&[]), "(no user text)");
    }

    #[test]
    fn chunking_preserves_content_exactly() {
        // Reassembly must be lossless, otherwise streamed text and submitted
        // output would disagree for confusing reasons.
        let text = "hello world";
        for n in [1, 2, 3, 8, 100] {
            let joined: String = chunk_text(text, n).concat();
            assert_eq!(joined, text, "chunking with n={n} lost content");
        }
    }

    #[test]
    fn chunking_handles_multibyte_text() {
        let text = "你好，世界";
        let joined: String = chunk_text(text, 3).concat();
        assert_eq!(joined, text);
    }
}

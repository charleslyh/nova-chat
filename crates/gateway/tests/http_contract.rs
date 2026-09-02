//! End-to-end HTTP behaviour against a live in-process gateway.
//!
//! Covers what unit tests cannot: the actual wire contract — status codes,
//! event names, cursor semantics and the multi-turn chain.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

// The gateway is a binary crate, so its modules are recompiled here. This keeps
// the test honest: it exercises the same router the binary mounts.
#[path = "../src/auth.rs"]
mod auth;
#[path = "../src/config.rs"]
mod config;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/routes/mod.rs"]
mod routes;
#[path = "../src/routing.rs"]
mod routing;
#[path = "../src/sse.rs"]
mod sse;
#[path = "../src/state.rs"]
mod state;

use crate::config::{Config, RawConfig};
use crate::state::AppState;

struct Harness {
    base: String,
    client: reqwest::Client,
}

async fn start() -> Harness {
    let raw: RawConfig = toml::from_str(
        r#"
        node_tag = "node-a"
        listen = "127.0.0.1:0"
        run_sweeper = false
        sync_wait_timeout_ms = 1500
        content_retention_ms = 600000
        retain_after_terminal_ms = 60000
        "#,
    )
    .expect("config");
    let cfg = Arc::new(Config::from_raw(raw).expect("validate"));

    let world = adapters_mem::MemWorld::new();
    let keys = Arc::new(auth::KeyTable::parse("").expect("keys"));
    let app_state = AppState {
        cfg,
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
        clock: world.clock.clone(),
        metrics: world.metrics.clone(),
        keys,
        http: reqwest::Client::new(),
        accepting: Arc::new(AtomicBool::new(true)),
    };

    let app = routes::router(app_state);
    let listener = tokio::net::TcpListener::bind::<SocketAddr>("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    // Let the listener come up.
    tokio::time::sleep(Duration::from_millis(50)).await;

    Harness {
        base: format!("http://{addr}"),
        client: reqwest::Client::new(),
    }
}

impl Harness {
    async fn post(&self, path: &str, body: Value) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}{path}", self.base))
            .json(&body)
            .send()
            .await
            .expect("post");
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn post_raw(&self, path: &str, body: &str) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .post(format!("{}{path}", self.base))
            .header("content-type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .expect("post");
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn get(&self, path: &str) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("get");
        let status = resp.status();
        let value = resp.json::<Value>().await.unwrap_or(Value::Null);
        (status, value)
    }

    async fn delete(&self, path: &str) -> reqwest::StatusCode {
        self.client
            .delete(format!("{}{path}", self.base))
            .send()
            .await
            .expect("delete")
            .status()
    }

    /// Read an SSE stream for a bounded window, keeping whatever arrived.
    ///
    /// A plain `.timeout(..).text()` is wrong here: an open stream never
    /// completes, so the timeout discards the body entirely and every assertion
    /// silently sees an empty string.
    async fn sse_text(&self, path: &str) -> String {
        use futures::StreamExt;

        let resp = self
            .client
            .get(format!("{}{path}", self.base))
            .send()
            .await
            .expect("sse");
        let mut stream = resp.bytes_stream();
        let mut out = String::new();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(900);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(chunk))) => out.push_str(&String::from_utf8_lossy(&chunk)),
                // Stream ended (terminal event) or errored.
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => break,
            }
        }
        out
    }

    /// Drive one response to completion the way a real execution side would.
    async fn run_agent_turn(&self, deltas: &[&str], output_text: &str) -> String {
        let agent = uuid::Uuid::new_v4().to_string();
        let resp = self
            .client
            .post(format!("{}/v1/agent/claim", self.base))
            .json(&json!({ "agent_id": agent }))
            .send()
            .await
            .expect("claim");
        assert_eq!(resp.status(), reqwest::StatusCode::OK, "expected a claimable response");
        let claimed: Value = resp.json().await.expect("claim body");
        let response_id = claimed["response_id"].as_str().expect("response_id").to_string();
        let attempt = claimed["attempt"].as_u64().expect("attempt");

        for delta in deltas {
            let (status, _) = self
                .post(
                    "/v1/agent/append",
                    json!({
                        "response_id": response_id,
                        "attempt": attempt,
                        "kind": "response.output_text.delta",
                        "payload": delta,
                    }),
                )
                .await;
            assert_eq!(status, reqwest::StatusCode::OK, "append should succeed");
        }

        let resp = self
            .client
            .post(format!("{}/v1/agent/complete", self.base))
            .json(&json!({
                "response_id": response_id,
                "attempt": attempt,
                "ok": true,
                // Output is submitted explicitly, never derived from the deltas
                // above (INV-48).
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{ "type": "output_text", "text": output_text }],
                }],
                "input_tokens": 5,
                "output_tokens": 7,
            }))
            .send()
            .await
            .expect("complete");
        assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);
        response_id
    }
}

#[tokio::test]
async fn health_reports_backend_sharing_and_acceptance() {
    let h = start().await;
    let (status, body) = h.get("/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["accepting"], true);
    // The mem backend is not shared, which is what makes content forwarding
    // necessary in multi-process runs.
    assert_eq!(body["context_store_shared"], false);
}

#[tokio::test]
async fn background_create_returns_immediately_then_streams() {
    let h = start().await;
    let (status, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hello", "background": true }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED);
    let id = body["id"].as_str().expect("id").to_string();
    assert_eq!(body["status"], "queued");
    assert_eq!(body["store"], true, "store must default to true");

    // Subscribe from the beginning: sequence 0 is the created event.
    let text = h.sse_text(&format!("/v1/responses/{id}?stream=true")).await;
    assert!(text.contains("event: response.created"), "got: {text}");
    assert!(text.contains("\"sequence_number\":0"), "got: {text}");
}

#[tokio::test]
async fn synchronous_create_waits_for_the_terminal_event() {
    let h = start().await;
    // Complete the turn concurrently, mimicking the execution side.
    let create = tokio::spawn({
        let base = h.base.clone();
        async move {
            reqwest::Client::new()
                .post(format!("{base}/v1/responses"))
                .json(&json!({ "model": "m", "input": "hi" }))
                .send()
                .await
                .expect("create")
                .json::<Value>()
                .await
                .expect("body")
        }
    });
    tokio::time::sleep(Duration::from_millis(120)).await;
    h.run_agent_turn(&["Hel", "lo"], "Hello").await;

    let body = create.await.expect("join");
    assert_eq!(body["status"], "completed", "sync mode must return a terminal object");
    assert_eq!(body["usage"]["total_tokens"], 12);
    assert_eq!(body["output"][0]["content"][0]["text"], "Hello");
}

#[tokio::test]
async fn multi_turn_chain_assembles_history_server_side() {
    let h = start().await;
    let (_, first) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "first question", "background": true }),
        )
        .await;
    let first_id = first["id"].as_str().unwrap().to_string();
    h.run_agent_turn(&["A"], "first answer").await;

    // Second turn sends only the new input plus the pointer.
    let (status, second) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "second question",
                "previous_response_id": first_id,
                "background": true,
            }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED);
    assert_eq!(second["previous_response_id"], first_id);

    // The execution side receives the full history, assembled by the server.
    let agent = uuid::Uuid::new_v4().to_string();
    let (_, claimed) = h.post("/v1/agent/claim", json!({ "agent_id": agent })).await;
    let input = claimed["input"].as_array().expect("input array");
    assert_eq!(
        input.len(),
        3,
        "expected first input + first output + second input, got {input:#?}"
    );
    let rendered = serde_json::to_string(input).unwrap();
    assert!(rendered.contains("first question"));
    assert!(rendered.contains("first answer"));
    assert!(rendered.contains("second question"));
}

#[tokio::test]
async fn instructions_are_echoed_but_never_inherited() {
    let h = start().await;
    let (_, first) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "q1",
                "instructions": "SPEAK-LIKE-A-PIRATE",
                "background": true,
            }),
        )
        .await;
    let first_id = first["id"].as_str().unwrap().to_string();
    // Echoed on the object.
    assert_eq!(first["instructions"], "SPEAK-LIKE-A-PIRATE");
    h.run_agent_turn(&["y"], "arr").await;

    let (_, _second) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "q2",
                "previous_response_id": first_id,
                "background": true,
            }),
        )
        .await;

    let agent = uuid::Uuid::new_v4().to_string();
    let (_, claimed) = h.post("/v1/agent/claim", json!({ "agent_id": agent })).await;
    // Not carried over: neither as an item nor as the instructions field.
    let rendered = serde_json::to_string(&claimed["input"]).unwrap();
    assert!(
        !rendered.contains("SPEAK-LIKE-A-PIRATE"),
        "previous instructions leaked into chain input: {rendered}"
    );
    assert!(
        claimed["instructions"].is_null(),
        "this turn set no instructions, so none should be delivered"
    );
}

#[tokio::test]
async fn unknown_field_is_rejected_with_the_field_name() {
    let h = start().await;
    let (status, body) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":"hi","truncation":"auto"}"#,
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("truncation"), "unhelpful error: {message}");
}

#[tokio::test]
async fn conversation_field_gets_a_pointed_remedy() {
    let h = start().await;
    let (status, body) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":"hi","conversation":"conv_1"}"#,
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    let message = body["error"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("previous_response_id"),
        "should point at the supported alternative: {message}"
    );
}

#[tokio::test]
async fn item_reference_and_inline_binary_are_rejected() {
    let h = start().await;
    let (status, _) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":[{"type":"item_reference","id":"msg_1"}]}"#,
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);

    let (status, _) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}]}"#,
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn internal_image_url_is_rejected_at_the_edge() {
    let h = start().await;
    let (status, _) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://169.254.169.254/latest/meta-data/"}]}]}"#,
        )
        .await;
    assert_eq!(
        status,
        reqwest::StatusCode::BAD_REQUEST,
        "the execution side would otherwise fetch cloud metadata"
    );
}

#[tokio::test]
async fn unknown_response_id_is_not_found_not_a_parse_error() {
    let h = start().await;
    let (status, _) = h.get("/v1/responses/not-an-id").await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    let (status, _) = h
        .get("/v1/responses/resp_node-a_00000000-0000-0000-0000-000000000000")
        .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unknown_node_tag_does_not_trigger_a_forward() {
    let h = start().await;
    // `node-z` is not in the peer registry. The gateway must report not-found
    // rather than synthesising an address from the tag (SEC-5).
    let (status, _) = h
        .get("/v1/responses/resp_node-z_00000000-0000-0000-0000-000000000000")
        .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn broken_chain_is_reported_not_silently_ignored() {
    let h = start().await;
    let (status, body) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "hi",
                "previous_response_id": "resp_node-a_00000000-0000-0000-0000-000000000000",
                "background": true,
            }),
        )
        .await;
    // A 400 with a `chain_broken` code: `previous_response_id` is a request
    // field, so an invalid value is a bad request rather than a missing route.
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "chain_broken");
}

#[tokio::test]
async fn store_false_response_cannot_be_chained() {
    let h = start().await;
    let (_, first) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "store": false, "background": true }),
        )
        .await;
    let id = first["id"].as_str().unwrap().to_string();
    assert_eq!(first["store"], false);
    h.run_agent_turn(&["x"], "y").await;

    let (status, body) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "next",
                "previous_response_id": id,
                "background": true,
            }),
        )
        .await;
    // Explicit refusal, never a silent downgrade to a single turn.
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "previous_not_stored");
}

#[tokio::test]
async fn starting_after_resumes_without_repeats() {
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();
    h.run_agent_turn(&["a", "b", "c"], "abc").await;

    let all = h.sse_text(&format!("/v1/responses/{id}?stream=true")).await;
    assert!(all.contains("\"sequence_number\":0"));
    let resumed = h
        .sse_text(&format!("/v1/responses/{id}?stream=true&starting_after=1"))
        .await;
    assert!(
        !resumed.contains("\"sequence_number\":0"),
        "starting_after must be exclusive: {resumed}"
    );
    assert!(
        !resumed.contains("\"sequence_number\":1"),
        "starting_after=1 must skip event 1: {resumed}"
    );
    assert!(resumed.contains("\"sequence_number\":2"), "got: {resumed}");
}

#[tokio::test]
async fn terminal_event_uses_the_protocol_name() {
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();
    h.run_agent_turn(&["x"], "x").await;
    let text = h.sse_text(&format!("/v1/responses/{id}?stream=true")).await;
    assert!(text.contains("event: response.output_text.delta"), "got: {text}");
    assert!(text.contains("event: response.completed"), "got: {text}");
}

#[tokio::test]
async fn delete_removes_content_and_breaks_further_chaining() {
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();
    h.run_agent_turn(&["x"], "y").await;

    assert_eq!(
        h.delete(&format!("/v1/responses/{id}")).await,
        reqwest::StatusCode::OK
    );
    let (status, _) = h.get(&format!("/v1/responses/{id}")).await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
    // Deleted content is genuinely unusable afterwards.
    let (status, body) = h
        .post(
            "/v1/responses",
            json!({
                "model": "m",
                "input": "next",
                "previous_response_id": id,
                "background": true,
            }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["type"], "chain_broken");
}

#[tokio::test]
async fn cancel_moves_to_terminal_and_emits_a_failure_event() {
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();

    let (status, cancelled) = h.post(&format!("/v1/responses/{id}/cancel"), Value::Null).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(cancelled["status"], "cancelled");

    let text = h.sse_text(&format!("/v1/responses/{id}?stream=true")).await;
    assert!(text.contains("event: response.failed"), "got: {text}");
    assert!(text.contains("cancelled"), "got: {text}");
}

#[tokio::test]
async fn chain_closure_violation_is_refused_at_complete() {
    let h = start().await;
    h.post(
        "/v1/responses",
        json!({ "model": "m", "input": "hi", "background": true }),
    )
    .await;
    let agent = uuid::Uuid::new_v4().to_string();
    let (_, claimed) = h.post("/v1/agent/claim", json!({ "agent_id": agent })).await;
    let response_id = claimed["response_id"].as_str().unwrap();
    let attempt = claimed["attempt"].as_u64().unwrap();

    // An output item type the input validator would refuse: accepting it would
    // silently break our own chain on the next turn (INV-47).
    let (status, _) = h
        .post_raw(
            "/v1/agent/complete",
            &format!(
                r#"{{"response_id":"{response_id}","attempt":{attempt},"ok":true,"output":[{{"type":"reasoning","summary":[]}}]}}"#
            ),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn stream_and_background_together_is_rejected() {
    let h = start().await;
    let (status, _) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "stream": true, "background": true }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn draining_refuses_creation_but_keeps_serving_reads() {
    // Exercised through the admin read-only switch, which shares the refusal
    // path with draining.
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();

    let (status, _) = h
        .post("/v1/admin/read_only", json!({ "enabled": true }))
        .await;
    assert_eq!(status, reqwest::StatusCode::OK);

    let (status, _) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "again", "background": true }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // Reads keep working while degraded.
    let (status, _) = h.get(&format!("/v1/responses/{id}")).await;
    assert_eq!(status, reqwest::StatusCode::OK);
}

#[tokio::test]
async fn overload_returns_too_many_requests() {
    let h = start().await;
    let (status, _) = h
        .post("/v1/admin/pending_limit", json!({ "pending_limit": 1 }))
        .await;
    assert_eq!(status, reqwest::StatusCode::OK);

    h.post(
        "/v1/responses",
        json!({ "model": "m", "input": "one", "background": true }),
    )
    .await;
    let (status, _) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "two", "background": true }),
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn tenant_purge_clears_stored_content() {
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().unwrap().to_string();

    let (status, purged) = h.post("/v1/tenants/local/purge", Value::Null).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(purged["deleted"], 1);

    let (status, _) = h.get(&format!("/v1/responses/{id}")).await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND);
}

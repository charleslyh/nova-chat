//! End-to-end HTTP behaviour against a live in-process gateway.
//!
//! Covers what unit tests cannot: the actual wire contract — status codes,
//! event names, cursor semantics and the multi-turn chain.

use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

// The gateway binary is thin assembly over `nova-responses`, so these tests drive
// the same library the binary mounts — just with the mem backend injected in
// process instead of a real carrier.
use nova_responses::{AppState, Config, ConversationsService, KeyTable, RawConfig, ResponsesService};

use nova_agent_runtime::{
    AgentEventSink, AgentRuntime, AgentRuntimeConfig, AgentRuntimeDeps, Executed,
};
use nova_agentd_mock::completions::{CompletionsOutcome, CompletionsRequest, FinishReason};
use nova_agentd_mock::{MockAgentRunner, NoopToolExecutor, Scheduler, SchedulerError};

struct Harness {
    base: String,
    client: reqwest::Client,
    /// Kept so a test can run execution in process.
    ///
    /// Execution is no longer an HTTP protocol (D23): a generation is run by the
    /// node that created it. These tests therefore drive the engine directly
    /// instead of impersonating an external worker over `/v1/agent/*`.
    world: adapters_mem::MemWorld,
}

/// Records the request it was handed, so a test can assert on the context the
/// server assembled.
///
/// Replaces what `/v1/agent/claim` used to reveal. The property is unchanged —
/// history is assembled server-side and handed to the execution side complete — but
/// the boundary moved in process (D23), so the assertion moves with it.
struct CapturingScheduler {
    seen: std::sync::Arc<std::sync::Mutex<Option<CompletionsRequest>>>,
}

#[async_trait::async_trait]
impl Scheduler for CapturingScheduler {
    fn name(&self) -> &str {
        "capturing-test"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        _sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        *self.seen.lock().expect("lock") = Some(request.clone());
        Ok(CompletionsOutcome::text("ok", nova_responses_core::Usage::new(1, 1)))
    }
}

/// Produces an item that cannot be stored.
struct InvalidOutcomeScheduler;

#[async_trait::async_trait]
impl Scheduler for InvalidOutcomeScheduler {
    fn name(&self) -> &str {
        "invalid-test"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        _sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        // A message with no content. Note what is no longer expressible: the previous
        // version of this test posted a `reasoning` item as raw JSON, but the item
        // enum is closed, so an out-of-subset type cannot be constructed at all. That
        // half of INV-47 is now enforced by the type system and asserted by the L0
        // `chain-closure` case.
        Ok(CompletionsOutcome {
            items: vec![nova_responses_core::ResponseItem::Message {
                role: nova_responses_core::Role::Assistant,
                content: vec![],
                id: None,
                status: None,
            }],
            usage: nova_responses_core::Usage::new(1, 1),
            finish: FinishReason::Stop,
        })
    }
}

/// Streams one thing and submits another.
///
/// Deliberately asymmetric: the whole point of INV-48 is that stored output is
/// **submitted**, never reassembled from the delta stream. A scheduler whose
/// streamed text always equalled its final answer could not tell the two apart, so
/// the assertion would hold even for an implementation that derived one from the
/// other.
struct DivergentScheduler {
    deltas: Vec<String>,
    output_text: String,
}

#[async_trait::async_trait]
impl Scheduler for DivergentScheduler {
    fn name(&self) -> &str {
        "divergent-test"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        for d in &self.deltas {
            if sink.text_delta(d).await?.should_stop() {
                return Err(SchedulerError::Superseded);
            }
        }
        Ok(CompletionsOutcome::text(
            self.output_text.clone(),
            nova_responses_core::Usage::new(5, 7),
        ))
    }
}

async fn start() -> Harness {
    let raw: RawConfig = toml::from_str(
        r#"
        node_tag = "node-a"
        run_sweeper = false
        sync_wait_timeout_ms = 1500
        content_retention_ms = 600000
        retain_after_terminal_ms = 60000
        "#,
    )
    .expect("config");
    let cfg = Arc::new(Config::from_raw(raw).expect("validate"));

    let world = adapters_mem::MemWorld::new();
    let world_handle = world.clone();
    let keys = Arc::new(KeyTable::parse("").expect("keys"));
    // Assembly order follows the dependency direction, exactly as the gateway
    // does it — this fixture is only worth anything if it is wired the same way.
    let conversations = Arc::new(ConversationsService::new(
        world.conversation.clone(),
        world.context.clone(),
        world.now_fn(),
        world.metrics.clone(),
        cfg.clone(),
    ));
    let service = Arc::new(ResponsesService::new(
        world.ledger.clone(),
        world.event_log.clone(),
        world.context.clone(),
        conversations.clone(),
        world.now_fn(),
        world.metrics.clone(),
        cfg.clone(),
    ));

    let app_state = AppState {
        cfg,
        ledger: world.ledger.clone(),
        event_log: world.event_log.clone(),
        context: world.context.clone(),
        conversation_store: world.conversation.clone(),
        now: world.now_fn(),
        metrics: world.metrics.clone(),
        keys,
        service,
        conversations,
        accepting: Arc::new(AtomicBool::new(true)),
    };

    let app = nova_responses::routes::router(app_state);
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
        world: world_handle,
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

    /// Build an orchestrator over this harness's ports with a caller-supplied
    /// scheduler wrapped in a [`MockAgentRunner`].
    fn engine_with(&self, scheduler: Arc<dyn Scheduler>) -> AgentRuntime {
        let runner = Arc::new(MockAgentRunner::new(scheduler, Arc::new(NoopToolExecutor)));
        AgentRuntime::new(
            AgentRuntimeDeps {
                ledger: self.world.ledger.clone(),
                event_log: self.world.event_log.clone(),
                context: self.world.context.clone(),
                runner,
                now: self.world.now_fn(),
                // Mounted, not `None`: with the port absent every terminal path
                // would skip the marker release and the tail advance, and this
                // harness could not tell working bookkeeping from missing
                // bookkeeping.
                conversations: Some(self.world.conversation.clone()),
            },
            AgentRuntimeConfig::default(),
        )
    }

    /// Drive the oldest queued response to completion, in process.
    ///
    /// `deltas` are streamed; `output_text` is what gets submitted. They differ on
    /// purpose — see [`DivergentScheduler`].
    async fn run_agent_turn(&self, deltas: &[&str], output_text: &str) {
        let engine = self.engine_with(Arc::new(DivergentScheduler {
            deltas: deltas.iter().map(|d| d.to_string()).collect(),
            output_text: output_text.to_string(),
        }));

        assert_eq!(
            engine.run_once(2_000).await,
            Executed::Completed,
            "the turn must complete; every caller of this helper depends on it"
        );
    }
}

#[tokio::test]
async fn health_reports_acceptance() {
    let h = start().await;
    let (status, body) = h.get("/health").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(body["accepting"], true);
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
    // `response.created` embeds the response object with the queued status.
    assert!(text.contains("\"status\":\"queued\""), "got: {text}");
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

    // The execution side receives the full history, assembled by the server. The
    // caller sent only the new input plus a pointer — that is the whole point of
    // `previous_response_id`.
    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let engine = h.engine_with(Arc::new(CapturingScheduler { seen: seen.clone() }));
    assert_eq!(engine.run_once(3_000).await, Executed::Completed);

    let request = seen.lock().expect("lock").clone().expect("a request was built");
    assert_eq!(
        request.messages.len(),
        3,
        "expected first input + first output + second input, got {:#?}",
        request.messages
    );
    let rendered = serde_json::to_string(&request.messages).expect("render");
    assert!(rendered.contains("first question"), "got: {rendered}");
    assert!(rendered.contains("first answer"), "got: {rendered}");
    assert!(rendered.contains("second question"), "got: {rendered}");
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

    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let engine = h.engine_with(Arc::new(CapturingScheduler { seen: seen.clone() }));
    assert_eq!(engine.run_once(3_000).await, Executed::Completed);
    let request = seen.lock().expect("lock").clone().expect("a request was built");

    // Not carried over: neither inside a message nor as a leading system message.
    let rendered = serde_json::to_string(&request.messages).expect("render");
    assert!(
        !rendered.contains("SPEAK-LIKE-A-PIRATE"),
        "previous instructions leaked into this turn's context: {rendered}"
    );
    assert!(
        !request
            .messages
            .iter()
            .any(|m| m.role_name() == "system"),
        "this turn set no instructions, so no system message may be sent: {rendered}"
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

/// `conversation` was the field this covered until D27 accepted it. The
/// mechanism it exercises — a *pointed* remedy rather than a generic "unknown
/// field" — is still worth covering, so it moved to another field on the list
/// rather than being deleted along with the entry.
#[tokio::test]
async fn deliberately_unsupported_fields_get_a_pointed_remedy() {
    let h = start().await;
    let (status, body) = h
        .post_raw(
            "/v1/responses",
            r#"{"model":"m","input":"hi","context_management":{}}"#,
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
    // `response.completed` embeds the finished response object (status + output).
    assert!(text.contains("\"status\":\"completed\""), "got: {text}");
    assert!(text.contains("\"output\""), "got: {text}");
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
    // The cancellation itself is reported by the cancel endpoint's status
    // (`cancelled`), not as a free-text payload on the stream event.
}

#[tokio::test]
async fn an_unusable_outcome_fails_the_response_instead_of_storing_it() {
    // Was `chain_closure_violation_is_refused_at_complete`, which posted a
    // `reasoning` item as raw JSON to `/v1/agent/complete`. Two things changed:
    // execution is no longer an HTTP protocol (D23), and the item enum is closed, so
    // an out-of-subset type can no longer be constructed at all — that half of
    // INV-47 is now enforced by the type system and asserted by the L0
    // `chain-closure` case.
    //
    // What remains reachable, and therefore worth asserting here, is the outcome
    // that *looks* storable and is not. It must fail the response rather than be
    // recorded as a successful empty answer.
    let h = start().await;
    let (_, body) = h
        .post(
            "/v1/responses",
            json!({ "model": "m", "input": "hi", "background": true }),
        )
        .await;
    let id = body["id"].as_str().expect("id").to_string();

    let engine = h.engine_with(Arc::new(InvalidOutcomeScheduler));
    assert_eq!(
        engine.run_once(2_000).await,
        Executed::Failed,
        "an unusable outcome must not complete the response"
    );

    let (status, after) = h.get(&format!("/v1/responses/{id}")).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(
        after["status"], "failed",
        "the caller must be told it failed, not handed an empty success"
    );
    assert!(
        after["output"].as_array().map(|a| a.is_empty()).unwrap_or(true),
        "nothing may be stored from a refused outcome: {after:#?}"
    );
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

//! A real chat-completions [`CompletionsRequestScheduler`] over HTTP(S).
//!
//! The one implementation in this workspace that performs IO. It is the outbound
//! edge's honest form — everything `completions-mock` deliberately fakes (streaming,
//! pacing, transport errors) is real here — and it is what makes the verification
//! stack able to run against a genuine provider while still being swappable through
//! the same port as the mock schedulers (D14).
//!
//! # Placement
//!
//! This is an **adapter**, in the same sense as `adapters-mem` / `adapters-sql`:
//! it satisfies the `CompletionsRequestScheduler` port and nothing else. It is *not*
//! a verification fixture — the verification UI lives in `verifier/` and reaches the
//! running service only through its public HTTP API. The separation that matters is
//! that neither the agent loop nor this adapter knows the UI exists.
//!
//! # Streaming
//!
//! Requests `stream: true` and parses the SSE delta stream, pushing `text_delta`
//! as content arrives so a caller renders progressively. If a provider ignores the
//! flag and answers with a plain JSON body, that is detected by content-type and
//! parsed the non-streaming way — the caller cannot tell which happened.

use async_trait::async_trait;
use futures::StreamExt;
use nova_responses_core::{
    CompletionsOutcome, CompletionsRequest, CompletionsRequestScheduler, CompletionsSink,
    FinishReason, SchedulerError, ToolCall, Usage,
};

/// Calls `POST {base_url}/chat/completions` with the request's messages.
///
/// `base_url` is the provider root, e.g. `https://api.openai.com/v1`; the path is
/// appended here so a caller passes exactly what a provider documents as its base.
#[derive(Debug, Clone)]
pub struct HttpChatCompletionsScheduler {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    /// Model to call. When set, it wins over the request's own `model` — the
    /// operator pins the provider-side model once, and the `model` field on a
    /// responses request stops being a provider routing decision.
    default_model: Option<String>,
}

impl HttpChatCompletionsScheduler {
    /// `base_url` is the provider root (no `/chat/completions` suffix).
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        default_model: Option<String>,
    ) -> Result<Self, String> {
        let base_url = base_url.into().trim().to_string();
        if base_url.is_empty() {
            return Err("base_url must not be empty".into());
        }
        // Fail fast on a blank key rather than sending `Authorization: Bearer `
        // and getting a confusing "Missing Authentication header" back from the
        // provider at generation time.
        let api_key = api_key.into().trim().to_string();
        if api_key.is_empty() {
            return Err("api_key must not be empty (check $NOVA_CHAT_API_KEY)".into());
        }
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            api_key,
            default_model,
        })
    }

    fn model<'a>(&'a self, request: &'a CompletionsRequest) -> &'a str {
        self.default_model.as_deref().unwrap_or(&request.model)
    }

    /// The chat-completions body for `request`.
    ///
    /// `messages` already serialize in chat-completions wire shape, so they are
    /// passed through as-is rather than re-shaped (see `CompletionsMessage`).
    fn body(&self, request: &CompletionsRequest) -> serde_json::Value {
        let mut body = serde_json::json!({
            "model": self.model(request),
            "messages": request.messages,
            "stream": true,
        });
        if let Some(t) = request.temperature {
            body["temperature"] = serde_json::json!(t);
        }
        if let Some(n) = request.max_completion_tokens {
            // `max_tokens` is the widely-supported spelling across compatible
            // providers; `max_completion_tokens` is the newer OpenAI-only name.
            body["max_tokens"] = serde_json::json!(n);
        }
        if !request.tools.is_empty() {
            // `ToolSpec` already serialises in chat-completions wire shape
            // (`{ type: "function", function: { name, … } }`), so it is passed
            // through unchanged rather than re-shaped here.
            body["tools"] = serde_json::json!(request.tools);
        }
        if let Some(choice) = &request.tool_choice {
            // `CompletionsToolChoice` is already the provider's wire shape
            // (`"auto"` / `"none"` / `"required"` / `{ type:"function", function:{name} }`).
            body["tool_choice"] = serde_json::json!(choice);
        }
        body
    }

    /// Rough usage when the provider does not report it (streaming without
    /// `stream_options.include_usage` returns no usage field).
    fn estimated_usage(request: &CompletionsRequest, output: &str) -> Usage {
        Usage::new(
            (request.approx_input_chars() as u64 / 4).max(1),
            (output.len() as u64 / 4).max(1),
        )
    }
}

#[async_trait]
impl CompletionsRequestScheduler for HttpChatCompletionsScheduler {
    fn name(&self) -> &str {
        "http"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn CompletionsSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let resp = self
            .client
            .post(format!("{}/chat/completions", self.base_url))
            .bearer_auth(&self.api_key)
            .json(&self.body(request))
            .send()
            .await
            .map_err(|e| SchedulerError::Unavailable(e.to_string()))?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(match status {
                s if s.is_server_error() => SchedulerError::Unavailable(text),
                // 429 and other client errors are permanent for the *same* request;
                // a fresh attempt with identical input would be refused again.
                _ => SchedulerError::Rejected(text),
            });
        }

        // Streaming is the happy path; a provider that ignores `stream: true` and
        // answers with JSON is handled by the same branch that detects content type.
        let is_event_stream = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false);

        if is_event_stream {
            self.parse_stream(resp, request, sink).await
        } else {
            self.parse_json(resp, request).await
        }
    }
}

impl HttpChatCompletionsScheduler {
    async fn parse_json(
        &self,
        resp: reqwest::Response,
        request: &CompletionsRequest,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| SchedulerError::Unavailable(format!("decode json body: {e}")))?;

        let content = body["choices"][0]["message"]["content"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if content.is_empty() {
            return Err(SchedulerError::EmptyOutcome);
        }
        Ok(CompletionsOutcome::text(
            content.clone(),
            Self::estimated_usage(request, &content),
        ))
    }

    async fn parse_stream(
        &self,
        resp: reqwest::Response,
        request: &CompletionsRequest,
        sink: &mut dyn CompletionsSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let mut stream = resp.bytes_stream();
        let mut buffer = String::new();
        let mut text = String::new();
        let mut refusal: Option<String> = None;
        let mut finish = FinishReason::Stop;
        // Tool calls accumulate across frames: the first chunk for an index
        // carries the id and name, later chunks append argument fragments.
        let mut tool_calls: Vec<ToolCall> = Vec::new();

        // Each SSE event is a sequence of `field: value` lines ending in a blank
        // line; the only field we read is `data:`. Chunks may split a line, so
        // incomplete lines stay buffered until their terminator arrives.
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| SchedulerError::Unavailable(e.to_string()))?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));

            while let Some(nl) = buffer.find('\n') {
                let mut line: String = buffer.drain(..=nl).collect();
                while line.ends_with('\n') || line.ends_with('\r') {
                    line.pop();
                }
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data == "[DONE]" {
                    break;
                }
                let Ok(event) = serde_json::from_str::<serde_json::Value>(data) else {
                    continue; // tolerate a malformed keepalive frame
                };

                let choice = &event["choices"][0];
                // OpenAI shapes the per-frame text in several slightly different
                // ways depending on the provider; we try the common ones rather
                // than only one, so a proxy that wraps `content` in an extra
                // object, or moves it to `delta.text`, does not silently empty
                // the answer.
                let delta_text = delta_text(choice);
                if let Some(piece) = delta_text {
                    text.push_str(&piece);
                    if sink
                        .text_delta(&piece)
                        .await
                        .map_err(SchedulerError::from)?
                        .should_stop()
                    {
                        return Err(SchedulerError::Superseded);
                    }
                }
                // Reasoning is render-only: streamed to the sink but never added
                // to the outcome (it must not become context for the next turn).
                if let Some(piece) = reasoning_text(choice) {
                    if sink
                        .reasoning_text_delta(&piece)
                        .await
                        .map_err(SchedulerError::from)?
                        .should_stop()
                    {
                        return Err(SchedulerError::Superseded);
                    }
                }
                if let Some(r) = refusal_text(choice) {
                    refusal.get_or_insert_with(String::new).push_str(&r);
                }
                accumulate_tool_calls(choice, &mut tool_calls);
                if let Some(reason) = choice["finish_reason"].as_str() {
                    finish = match reason {
                        "length" => FinishReason::Length,
                        "tool_calls" => FinishReason::ToolCalls,
                        _ => FinishReason::Stop,
                    };
                }
            }
        }

        if let Some(reason) = refusal {
            let usage = Self::estimated_usage(request, &reason);
            return Ok(CompletionsOutcome::refusal(reason, usage));
        }
        // Tool calls short-circuit: a tool-calling turn produces no answer text.
        // Each call is announced to the sink so a subscriber sees it live, and the
        // outcome carries the same calls for the agent loop to execute.
        if !tool_calls.is_empty() {
            for call in &tool_calls {
                if sink
                    .tool_call(call)
                    .await
                    .map_err(SchedulerError::from)?
                    .should_stop()
                {
                    return Err(SchedulerError::Superseded);
                }
            }
            let args: String = tool_calls.iter().map(|c| c.arguments.as_str()).collect();
            let usage = Self::estimated_usage(request, &args);
            return Ok(CompletionsOutcome::tool_calls(tool_calls, usage));
        }
        if text.is_empty() {
            return Err(SchedulerError::EmptyOutcome);
        }
        let usage = Self::estimated_usage(request, &text);
        Ok(CompletionsOutcome {
            items: CompletionsOutcome::text(text.clone(), Usage::default()).items,
            usage,
            finish,
        })
    }
}

/// Extract the textual delta of one SSE choice frame, tolerating the small set
/// of provider shapes seen in the wild (see the comment in `parse_stream`).
fn delta_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;

    // Bare string: some proxies return the whole delta as a JSON string.
    if let Some(s) = delta.as_str() {
        return (!s.is_empty()).then(|| s.to_string());
    }

    // `delta.content` is the canonical OpenAI spelling and may be a string…
    if let Some(s) = delta["content"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    // …or an object `{ "text": "..." }` (Anthropic-style, some proxies).
    if let Some(s) = delta["content"]["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    // `delta.text` — used by a few compatible APIs.
    if let Some(s) = delta["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

/// Extract the reasoning / thinking text of one SSE choice frame.
///
/// The field name varies by provider: `reasoning_content` (DeepSeek-R1, QwQ),
/// `reasoning` (OpenAI o1 / some proxies), `thinking` (Anthropic-style wrappers).
fn reasoning_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;
    for key in ["reasoning_content", "reasoning", "thinking"] {
        if let Some(s) = delta[key].as_str() {
            if !s.is_empty() {
                return Some(s.to_string());
            }
        }
    }
    None
}

/// Extract the refusal text of one SSE choice frame, same shape tolerance as
/// `delta_text`.
fn refusal_text(choice: &serde_json::Value) -> Option<String> {
    let delta = choice.get("delta")?;
    if let Some(s) = delta["refusal"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    if let Some(s) = delta["refusal"]["text"].as_str() {
        if !s.is_empty() {
            return Some(s.to_string());
        }
    }
    None
}

/// Accumulate tool-call fragments from one SSE delta frame.
///
/// Chat-completions streams tool calls incrementally: the first chunk for a given
/// `index` carries `id` and `function.name`, subsequent chunks carry only
/// `function.arguments` fragments (with `id`/`name` null). Calls are grouped by
/// `index` and their argument fragments concatenated in arrival order.
fn accumulate_tool_calls(choice: &serde_json::Value, tool_calls: &mut Vec<ToolCall>) {
    let Some(delta) = choice.get("delta") else {
        return;
    };
    let Some(array) = delta["tool_calls"].as_array() else {
        return;
    };
    for tc in array {
        let index = tc["index"].as_u64().unwrap_or(0) as usize;
        while tool_calls.len() <= index {
            tool_calls.push(ToolCall {
                id: String::new(),
                name: String::new(),
                arguments: String::new(),
            });
        }
        let entry = &mut tool_calls[index];
        if let Some(id) = tc["id"].as_str() {
            if !id.is_empty() {
                entry.id = id.to_string();
            }
        }
        if let Some(name) = tc["function"]["name"].as_str() {
            if !name.is_empty() {
                entry.name = name.to_string();
            }
        }
        if let Some(args) = tc["function"]["arguments"].as_str() {
            entry.arguments.push_str(args);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses_core::{CollectingSink, CompletionsMessage, RequestProvenance};

    fn request(model: &str) -> CompletionsRequest {
        CompletionsRequest {
            model: model.into(),
            messages: vec![CompletionsMessage::user_text("hello")],
            tools: vec![],
            tool_choice: None,
            max_completion_tokens: None,
            temperature: None,
            provenance: RequestProvenance {
                response_id: "r".into(),
                attempt: 1,
                exec_deadline_ms: 60_000,
            },
        }
    }

    #[test]
    fn the_configured_model_wins_over_the_request_model() {
        let s = HttpChatCompletionsScheduler::new(
            "https://example.test/v1",
            "sk-test",
            Some("gpt-fallback".into()),
        )
        .expect("build");
        // The operator's pin wins even when the request names its own model.
        assert_eq!(s.model(&request("gpt-4o")), "gpt-fallback");

        // Without a pin, the request's model is used verbatim.
        let s = HttpChatCompletionsScheduler::new("https://example.test/v1", "sk", None)
            .expect("build");
        assert_eq!(s.model(&request("gpt-4o")), "gpt-4o");
    }

    #[test]
    fn the_body_passes_messages_through_and_streams() {
        let s = HttpChatCompletionsScheduler::new("https://example.test/v1", "sk", None)
            .expect("build");
        let body = s.body(&request("gpt-4o"));
        assert_eq!(body["model"], "gpt-4o");
        assert_eq!(body["stream"], true);
        assert!(body["messages"].is_array());
    }

    #[test]
    fn a_missing_base_url_is_refused() {
        assert!(HttpChatCompletionsScheduler::new("", "sk", None).is_err());
    }

    #[test]
    fn a_blank_api_key_is_refused_at_construction() {
        // A blank key would otherwise surface only at generation time as a
        // provider-side "Missing Authentication header", far from the real cause.
        assert!(HttpChatCompletionsScheduler::new("https://example.test/v1", "", None).is_err());
        assert!(HttpChatCompletionsScheduler::new("https://example.test/v1", "   ", None).is_err());
    }

    #[test]
    fn reasoning_text_tolerates_provider_field_names() {
        // DeepSeek-R1 / QwQ use `reasoning_content`, OpenAI o1 uses `reasoning`,
        // some wrappers use `thinking`. All must surface, and a plain text delta
        // must not.
        assert_eq!(
            reasoning_text(&serde_json::json!({"delta": {"reasoning_content": "t1"}})).as_deref(),
            Some("t1")
        );
        assert_eq!(
            reasoning_text(&serde_json::json!({"delta": {"reasoning": "t2"}})).as_deref(),
            Some("t2")
        );
        assert_eq!(
            reasoning_text(&serde_json::json!({"delta": {"thinking": "t3"}})).as_deref(),
            Some("t3")
        );
        assert_eq!(
            reasoning_text(&serde_json::json!({"delta": {"content": "t4"}})),
            None
        );
    }

    #[test]
    fn accumulate_tool_calls_groups_fragments_by_index() {
        let mut calls: Vec<ToolCall> = Vec::new();
        // First frame carries the call's identity plus the start of the arguments.
        accumulate_tool_calls(
            &serde_json::json!({"delta": {"tool_calls": [
                {"index": 0, "id": "call_1", "function": {"name": "calculate", "arguments": "{\"expr"}},
            ]}}),
            &mut calls,
        );
        // Later frames omit id/name and append only argument fragments.
        accumulate_tool_calls(
            &serde_json::json!({"delta": {"tool_calls": [
                {"index": 0, "function": {"arguments": "ession\":\"3*7\"}"}},
            ]}}),
            &mut calls,
        );
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, "call_1");
        assert_eq!(calls[0].name, "calculate");
        assert_eq!(calls[0].arguments, "{\"expression\":\"3*7\"}");
    }

    #[test]
    fn the_body_sends_tools_when_declared() {
        let s = HttpChatCompletionsScheduler::new("https://example.test/v1", "sk", None)
            .expect("build");
        let mut req = request("gpt-4o");
        req.tools = vec![nova_responses_core::ToolSpec::new(
            "calculate".into(),
            Some("evaluate".into()),
            serde_json::json!({"type": "object"}),
        )];
        let body = s.body(&req);
        assert!(body["tools"].is_array());
        assert_eq!(body["tools"][0]["type"], "function");
        assert_eq!(body["tools"][0]["function"]["name"], "calculate");

        // No tools declared → the field is absent rather than sent as an empty array.
        let plain = s.body(&request("gpt-4o"));
        assert!(plain.get("tools").is_none());
    }

    #[test]
    fn the_body_sends_tool_choice_when_declared() {
        let s = HttpChatCompletionsScheduler::new("https://example.test/v1", "sk", None)
            .expect("build");
        let mut req = request("gpt-4o");
        req.tool_choice = Some(nova_responses_core::CompletionsToolChoice::Specific {
            kind: "function".to_string(),
            function: nova_responses_core::SpecificFunction {
                name: "calculate".to_string(),
            },
        });
        let body = s.body(&req);
        assert_eq!(
            body["tool_choice"],
            serde_json::json!({"type": "function", "function": {"name": "calculate"}})
        );

        // Absent → the field is omitted.
        let plain = s.body(&request("gpt-4o"));
        assert!(plain.get("tool_choice").is_none());
    }

    #[tokio::test]
    async fn a_collecting_sink_used_for_compile_coverage() {
        // The sink contract is exercised end-to-end by the integration stack; this
        // only pins that the sink type is usable with this scheduler's signature.
        let mut sink = CollectingSink::new();
        let _ = sink.text_delta("x").await.expect("push");
        assert_eq!(sink.streamed(), "x");
    }
}

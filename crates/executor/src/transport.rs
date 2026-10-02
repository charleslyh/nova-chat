//! The chat-completions HTTP transport.
//!
//! Ported from the retiring in-process runner, where it earned several scars
//! that are preserved verbatim below and individually commented:
//! cumulative-snapshot normalisation, tool-chunk index merging, refusal
//! precedence in finish-reason mapping, and the trailing usage drain.
//!
//! Three things changed in the move:
//!
//! - **`let`-chains rewritten** as `let ... else` / filtered `if let`, because
//!   this workspace is on edition 2021. The rewrites are semantically exact;
//!   where that mattered (the usage drain) it is called out inline.
//! - **Logs redacted.** The original wrote whole request bodies and whole
//!   response texts at `info!`, which put every user message into the log at
//!   normal verbosity. That is deliberately restored for troubleshooting: the
//!   `[llm]` request/response logs carry the full request body and the
//!   complete accumulated result at `info!`, on by default.
//! - **The API key cannot be printed.** It is wrapped in a type whose `Debug`
//!   is opaque, so adding `#[derive(Debug)]` upstream of it cannot leak it.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_openai::config::OpenAIConfig;
use async_openai::types::chat::{
    ChatCompletionMessageToolCall as WireMessageToolCall,
    ChatCompletionMessageToolCallChunk as WireToolCallChunk,
    ChatCompletionMessageToolCalls as WireMessageToolCalls,
    ChatCompletionRequestAssistantMessage as WireAssistantMessage,
    ChatCompletionRequestAssistantMessageContent as WireAssistantContent,
    ChatCompletionRequestMessage as WireMessage,
    ChatCompletionRequestSystemMessage as WireSystemMessage,
    ChatCompletionRequestSystemMessageContent as WireSystemContent,
    ChatCompletionRequestToolMessage as WireToolMessage,
    ChatCompletionRequestToolMessageContent as WireToolContent,
    ChatCompletionRequestUserMessage as WireUserMessage,
    ChatCompletionRequestUserMessageContent as WireUserContent, ChatCompletionStreamOptions,
    ChatCompletionTool as WireTool, ChatCompletionTools as WireTools, CompletionUsage,
    CreateChatCompletionRequest, FinishReason as WireFinishReason, FunctionCall, FunctionObject,
    ReasoningEffort,
};
use async_openai::Client;
use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;

use crate::runner::StreamTimeouts;
use crate::think::{Split, ThinkSplitter};
use crate::wire::{
    ChatCompletion, ChunkStream, FinishReason, ModelError, ModelUsage, RequestMessage,
    ResponseChunk, ToolCallRequest, ToolManifest, TransportOverrides,
};

/// Cap on model text reaching a log line. A trace records what happened, not
/// what was said.
const LOG_TEXT_LIMIT: usize = 256;

/// Whether the `[llm]` request/response logs are on. They carry the full
/// request body and the complete (accumulated) result at `info!`, so they are
/// on by default; the check only skips serialisation when filtered out.
/// Headers (and thus credentials) are never logged.
fn wire_enabled() -> bool {
    tracing::enabled!(tracing::Level::INFO)
}

fn to_json(value: &impl serde::Serialize) -> String {
    serde_json::to_string(value).unwrap_or_else(|error| format!("<unserializable: {error}>"))
}

/// The accumulated outcome of one streamed round, dumped once at its end.
#[allow(clippy::too_many_arguments)]
fn log_stream_result(
    model: &str,
    outcome: &str,
    finish_reason: Option<&WireFinishReason>,
    content: &str,
    reasoning: &str,
    refusal: &str,
    tool_calls: &[ToolCallRequest],
    usage: Option<&CompletionUsage>,
) {
    if !wire_enabled() {
        return;
    }
    let result = serde_json::json!({
        "model": model,
        "outcome": outcome,
        "finish_reason": finish_reason,
        "content": content,
        "reasoning_content": reasoning,
        "refusal": refusal,
        "tool_calls": tool_calls,
        "usage": usage,
    });
    tracing::info!(
        finish_reason = ?finish_reason,
        chars = content.len(),
        result = %result,
        "[llm] response (stream, accumulated)"
    );
}

/// An API key that cannot be printed by accident.
#[derive(Clone)]
pub struct ApiKey(String);

impl ApiKey {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Deliberately opaque: a derived Debug anywhere upstream must not be
        // able to put a credential in a log line.
        f.write_str("ApiKey(***)")
    }
}

/// Where a model lives and how to address it.
#[derive(Clone, Debug)]
pub struct ModelEndpoint {
    pub api_key: ApiKey,
    pub base_url: String,
    /// The wire model name sent upstream.
    pub api_model: String,
    /// Provider-specific request fields, passed through.
    pub extensions: Value,
}

impl ModelEndpoint {
    pub fn new(api_key: ApiKey, base_url: impl Into<String>, api_model: impl Into<String>) -> Self {
        Self {
            api_key,
            base_url: base_url.into(),
            api_model: api_model.into(),
            extensions: Value::Object(Default::default()),
        }
    }

    pub fn with_extensions(mut self, extensions: Value) -> Self {
        self.extensions = extensions;
        self
    }
}

/// Chat-completions transport over an OpenAI-compatible endpoint.
pub struct HttpChatCompletion {
    client: Client<OpenAIConfig>,
    /// The config `client` was built from, so a round carrying passthrough
    /// headers can derive a variant without re-reading anything.
    config: OpenAIConfig,
    /// Shared connection pool. A round's own client MUST be built with it
    /// (`Client::build`) rather than `Client::with_config`, which spins up a
    /// fresh pool and drops keep-alive on every request.
    http: openai_reqwest::Client,
    api_model: String,
    reasoning_effort: Option<ReasoningEffort>,
    /// The level the endpoint named, verbatim. Not the enum above: a
    /// deployment's own vocabulary carries further than the protocol's —
    /// HunYuan's `no_think` is not a level, and its chat template is where
    /// the switch is read.
    reasoning_effort_name: Option<String>,
    max_completion_tokens: Option<u32>,
    wire_options: WireOptions,
    effort_via: ReasoningEffortVia,
    timeouts: EndpointTimeouts,
    /// The closing think marker the upstream used, verbatim, suffix included.
    /// Remembered because replaying a thought has to wrap it in the very marker
    /// this endpoint emits, and the suffix is deployment-specific.
    /// `Arc` because the stream generator has to share it without borrowing
    /// `self`.
    seen_marker: Arc<Mutex<Option<String>>>,
}

/// How an assistant turn's reasoning goes back to the upstream. It must match
/// how the endpoint delivers reasoning, since that is the shape its chat
/// template renders history in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReasoningReplay {
    /// As a message field: endpoints with a server-side reasoning parser.
    Field(ReasoningField),
    /// Wrapped in think markers ahead of `content`: endpoints that inline the
    /// chain of thought in the body (Hy3 without a reasoning parser), whose
    /// template expects it back the way the model wrote it.
    Inline,
    Off,
}

/// Which message field carries replayed reasoning. `reasoning_content` is the
/// DeepSeek-era name most OpenAI-compatible servers and gateways read;
/// vLLM 0.16+ emits `reasoning`, and a server may come to read only that.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReasoningField {
    ReasoningContent,
    Reasoning,
}

/// The markers an inlined thought is wrapped in on replay. Configurable
/// because some deployments suffix them (`</think:abcd>`) and their template
/// only recognises the exact form the model was trained on — and because some
/// emit no opening marker at all, which an empty `open` replays faithfully.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ThinkMarkers {
    open: String,
    close: String,
}

/// Where the thinking switch is sent. Not every deployment reads both: one may
/// reject a level outside the protocol's enum outright, another may only honour
/// the field and ignore the template.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReasoningEffortVia {
    /// The protocol's field, when the value is a level of its enum, and the
    /// template either way.
    Both,
    /// Only the protocol's field.
    Standard,
    /// Only the chat template: for a deployment that rejects the field.
    Template,
}

/// How long one round of this endpoint may go quiet, when it says.
///
/// An endpoint knows its own queueing: one behind a busy gateway can spend
/// minutes before anything arrives, a local one answers in under a second.
/// Defaults, which stay in force when nothing is configured, cannot tell them
/// apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EndpointTimeouts {
    first_chunk: Option<Duration>,
    idle: Option<Duration>,
    total: Option<Duration>,
}

impl EndpointTimeouts {
    fn from_extensions(extensions: &Value) -> Self {
        let millis = |key: &str| {
            extensions
                .get(key)
                .and_then(Value::as_u64)
                .filter(|ms| *ms > 0)
                .map(Duration::from_millis)
        };
        Self {
            first_chunk: millis("stream_timeout_first_chunk_ms"),
            idle: millis("stream_timeout_idle_ms"),
            total: millis("stream_timeout_total_ms"),
        }
    }

    fn or_default(self, fallback: StreamTimeouts) -> StreamTimeouts {
        StreamTimeouts {
            first_chunk: self.first_chunk.unwrap_or(fallback.first_chunk),
            idle: self.idle.unwrap_or(fallback.idle),
            total: self.total.unwrap_or(fallback.total),
        }
    }
}

/// Endpoint switches for what history carries to the upstream. Read from the
/// endpoint's extensions; none of them is sent upstream as a field.
#[derive(Clone, Debug, PartialEq, Eq)]
struct WireOptions {
    /// `reasoning_replay = "field" | "inline" | "off"`, default `field`; for
    /// `field`, `reasoning_field = "reasoning_content" | "reasoning"`, default
    /// `reasoning_content`.
    reasoning_replay: ReasoningReplay,
    /// `reasoning_inline_open_tag` / `reasoning_inline_close_tag`; default `<think>` / `</think>`.
    think_markers: ThinkMarkers,
    /// Forward the caller's per-tool `strict` flag. Off by default, since
    /// strict mode turns on constrained decoding, which not every serving
    /// stack supports for every tool-call format.
    strict_tools: bool,
}

impl WireOptions {
    fn from_extensions(extensions: &Value) -> Self {
        let field = match extensions.get("reasoning_field").and_then(Value::as_str) {
            None | Some("reasoning_content") => ReasoningField::ReasoningContent,
            Some("reasoning") => ReasoningField::Reasoning,
            Some(other) => {
                tracing::warn!(
                    value = other,
                    "unknown reasoning_field; replaying as reasoning_content"
                );
                ReasoningField::ReasoningContent
            }
        };
        let reasoning_replay = match extensions.get("reasoning_replay").and_then(Value::as_str) {
            None | Some("field") => ReasoningReplay::Field(field),
            Some("inline") => ReasoningReplay::Inline,
            Some("off") => ReasoningReplay::Off,
            Some(other) => {
                tracing::warn!(
                    value = other,
                    "unknown reasoning_replay; replaying as a field"
                );
                ReasoningReplay::Field(field)
            }
        };
        // An empty closing marker would wrap the thought in nothing at all, so
        // only the opening one may be empty.
        let marker = |name: &str, default: &str, may_be_empty: bool| match extensions.get(name) {
            None => default.to_string(),
            Some(Value::String(text)) if may_be_empty || !text.trim().is_empty() => text.clone(),
            Some(other) => {
                tracing::warn!(
                    key = name,
                    value = %other,
                    default,
                    "invalid think marker; using the default"
                );
                default.to_string()
            }
        };
        Self {
            reasoning_replay,
            think_markers: ThinkMarkers {
                open: marker("reasoning_inline_open_tag", "<think>", true),
                close: marker("reasoning_inline_close_tag", "</think>", false),
            },
            strict_tools: extensions
                .get("strict_tools")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        }
    }
}

impl HttpChatCompletion {
    pub fn new(endpoint: ModelEndpoint) -> Self {
        let config = OpenAIConfig::new()
            .with_api_key(endpoint.api_key.expose())
            .with_api_base(endpoint.base_url);
        let http = openai_reqwest::Client::new();
        Self {
            client: Client::build(http.clone(), config.clone()),
            config,
            http,
            api_model: endpoint.api_model,
            reasoning_effort: reasoning_effort(&endpoint.extensions),
            reasoning_effort_name: reasoning_effort_name(&endpoint.extensions),
            max_completion_tokens: max_completion_tokens(&endpoint.extensions),
            wire_options: WireOptions::from_extensions(&endpoint.extensions),
            effort_via: effort_via(&endpoint.extensions),
            timeouts: EndpointTimeouts::from_extensions(&endpoint.extensions),
            seen_marker: Arc::new(Mutex::new(None)),
        }
    }

    /// The closing marker seen from this endpoint, or the configured fallback.
    fn close_marker(&self) -> String {
        self.seen_marker
            .lock()
            .expect("lock")
            .clone()
            .unwrap_or_else(|| self.wire_options.think_markers.close.clone())
    }

    fn remember_close_marker(&self, marker: Option<String>) {
        if let Some(marker) = marker {
            if !marker.is_empty() {
                *self.seen_marker.lock().expect("lock") = Some(marker);
            }
        }
    }

    /// The client for one round: the shared one when the round carries nothing
    /// to pass through, else a variant carrying its headers. The variant reuses
    /// `http`, so building it costs a clone and keeps the connection pool.
    ///
    /// Headers are applied as `custom_headers`, which `OpenAIConfig::headers()`
    /// merges LAST — that precedence is what lets a round override
    /// `authorization`. `passthrough_headers_reach_the_wire` pins it, so a
    /// dependency bump cannot silently downgrade us to the static key.
    fn client_for(&self, transport: &TransportOverrides) -> Cow<'_, Client<OpenAIConfig>> {
        if transport.is_empty() {
            return Cow::Borrowed(&self.client);
        }
        let mut config = self.config.clone();
        for (name, value) in transport.headers() {
            // Names and values are validated where the override is read, but a
            // header that is legal JSON can still be illegal on the wire. Drop
            // that one entry rather than fail the round; the upstream deciding
            // it is missing is the better error. The name is owned because
            // `IntoHeaderName` for `&str` demands `'static`.
            let applied = openai_reqwest::header::HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| error.to_string())
                .and_then(|name| {
                    config
                        .clone()
                        .with_header(name, value)
                        .map_err(|error| error.to_string())
                });
            match applied {
                Ok(next) => config = next,
                Err(error) => {
                    tracing::warn!(header = %name, error = %error, "dropping an invalid passthrough header")
                }
            }
        }
        Cow::Owned(Client::build(self.http.clone(), config))
    }

    /// Whether this turn asks the model to think. `none` is a level of the enum,
    /// not an absent one: such a turn is sent a request that says "do not
    /// think", so no CoT is coming and its body must not be held back for a
    /// think marker that never arrives.
    /// The protocol's `reasoning_effort` field, when this endpoint is to be
    /// sent one: the level, and only ever a value the enum has.
    fn standard_effort(&self) -> Option<ReasoningEffort> {
        match self.effort_via {
            ReasoningEffortVia::Template => None,
            _ => self.reasoning_effort.clone(),
        }
    }

    /// The chat-template switch, when this endpoint is to be sent one: the
    /// level verbatim, since a deployment's own vocabulary is what its template
    /// reads — `no_think` is not a level the protocol has.
    fn template_effort(&self) -> Option<Value> {
        match self.effort_via {
            ReasoningEffortVia::Standard => None,
            _ => self
                .reasoning_effort_name
                .as_ref()
                .map(|level| serde_json::json!({ "reasoning_effort": level })),
        }
    }

    /// What a thought opens with on this endpoint, `""` when thoughts announce
    /// themselves only by closing.
    ///
    /// Only an endpoint that delivers its thinking inline declares one: it is
    /// the same declaration that says how a thought is replayed. Elsewhere
    /// there is nothing to wait for, and the body keeps being read on trust.
    fn thought_opening(&self) -> &str {
        match self.wire_options.reasoning_replay {
            ReasoningReplay::Inline => self.wire_options.think_markers.open.as_str(),
            _ => "",
        }
    }

    fn thinks(&self) -> bool {
        if self
            .reasoning_effort_name
            .as_deref()
            .is_some_and(thinking_off)
        {
            return false;
        }
        self.reasoning_effort.is_some()
            || matches!(self.wire_options.reasoning_replay, ReasoningReplay::Inline)
    }

    /// The request this turn would put on the wire.
    pub(crate) fn request(
        &self,
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        stream: bool,
    ) -> CreateChatCompletionRequest {
        CreateChatCompletionRequest {
            model: self.api_model.clone(),
            messages: to_wire_messages(
                messages,
                self.wire_options.reasoning_replay,
                &self.wire_options.think_markers.open,
                &self.close_marker(),
            ),
            tools: map_tools(tools, self.wire_options.strict_tools),
            stream: Some(stream),
            stream_options: stream.then_some(ChatCompletionStreamOptions {
                include_usage: Some(true),
                include_obfuscation: None,
            }),
            reasoning_effort: self.standard_effort(),
            chat_template_kwargs: self.template_effort(),
            max_completion_tokens: self.max_completion_tokens,
            ..Default::default()
        }
    }
}

#[async_trait]
impl ChatCompletion for HttpChatCompletion {
    async fn completion(
        &self,
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        stream: bool,
        transport: &TransportOverrides,
    ) -> Result<ChunkStream, ModelError> {
        let client = self.client_for(transport);
        let request = self.request(messages, tools, stream);
        if wire_enabled() {
            tracing::info!(
                model = %self.api_model,
                messages = messages.len(),
                tools = tools.len(),
                stream,
                passthrough_headers = transport.headers().count(),
                body = %to_json(&request),
                "[llm] request"
            );
        }
        if !stream {
            return self.blocking_turn(request, &client).await;
        }
        self.streaming_turn(request, &client, Arc::clone(&self.seen_marker))
            .await
    }

    fn stream_timeouts(&self) -> Option<StreamTimeouts> {
        Some(self.timeouts.or_default(StreamTimeouts::default()))
    }
}

impl Clone for HttpChatCompletion {
    fn clone(&self) -> Self {
        Self {
            client: Client::build(self.http.clone(), self.config.clone()),
            config: self.config.clone(),
            http: self.http.clone(),
            api_model: self.api_model.clone(),
            reasoning_effort: self.reasoning_effort.clone(),
            reasoning_effort_name: self.reasoning_effort_name.clone(),
            max_completion_tokens: self.max_completion_tokens,
            wire_options: self.wire_options.clone(),
            effort_via: self.effort_via,
            timeouts: EndpointTimeouts::from_extensions(&serde_json::Value::Null),
            seen_marker: Arc::clone(&self.seen_marker),
        }
    }
}

impl HttpChatCompletion {
    async fn blocking_turn(
        &self,
        request: CreateChatCompletionRequest,
        client: &Client<OpenAIConfig>,
    ) -> Result<ChunkStream, ModelError> {
        let response = client.chat().create(request).await.map_err(|error| {
            tracing::info!(error = %error, "[llm] request failed (non-stream)");
            provider_error(error)
        })?;
        let choice = response
            .choices
            .first()
            .ok_or_else(|| ModelError::new("model returned no choices"))?;

        let mut chunks = Vec::new();
        let mut splitter = ThinkSplitter::new(self.thinks(), self.thought_opening());
        if let Some(reasoning) = extract_reasoning(&response, "message") {
            // The provider carries the CoT in its own field: nothing to split.
            splitter.disable();
            chunks.push(Ok(ResponseChunk::Think(reasoning)));
            chunks.push(Ok(ResponseChunk::ThinkDone));
        }
        if let Some(content) = choice
            .message
            .content
            .as_deref()
            .filter(|content| !content.is_empty())
        {
            let mut parts = splitter.push(content);
            self.remember_close_marker(splitter.close_marker().map(str::to_string));
            parts.extend(splitter.finish());
            let mut thought = false;
            for part in parts {
                match part {
                    Split::Think(text) => {
                        thought = true;
                        chunks.push(Ok(ResponseChunk::Think(text)));
                    }
                    Split::Text(text) => chunks.push(Ok(ResponseChunk::TextBlock(text))),
                }
            }
            if thought {
                chunks.push(Ok(ResponseChunk::ThinkDone));
            }
            chunks.push(Ok(ResponseChunk::TextDone));
        }
        let mut call_count = 0;
        if let Some(tool_calls) = &choice.message.tool_calls {
            for call in tool_calls_from_message(tool_calls) {
                call_count += 1;
                chunks.push(Ok(ResponseChunk::ToolCall(call)));
            }
        }
        let reason = reconcile_tool_calls(
            finish_reason(
                choice.finish_reason.as_ref(),
                choice.message.refusal.clone().unwrap_or_default(),
            ),
            choice.finish_reason.as_ref(),
            call_count,
        );
        if wire_enabled() {
            tracing::info!(
                finish_reason = ?reason,
                chars = choice.message.content.as_deref().unwrap_or_default().len(),
                body = %to_json(&response),
                "[llm] response (non-stream)"
            );
        }
        chunks.push(Ok(ResponseChunk::Done {
            reason,
            usage: response.usage.as_ref().map(map_usage),
        }));
        Ok(Box::pin(futures::stream::iter(chunks)))
    }

    /// `seen_marker` receives the closing think marker the stream met, so the
    /// next round on this endpoint replays a thought wrapped in the same one.
    async fn streaming_turn(
        &self,
        request: CreateChatCompletionRequest,
        client: &Client<OpenAIConfig>,
        seen_marker: Arc<Mutex<Option<String>>>,
    ) -> Result<ChunkStream, ModelError> {
        let mut upstream = client
            .chat()
            .create_stream(request)
            .await
            .map_err(|error| {
                tracing::info!(error = %error, "[llm] request failed (stream)");
                provider_error(error)
            })?;
        // Whether this turn can be receiving an INLINED CoT: only such a turn
        // holds its body. The opening marker it would announce one with is
        // cloned because the stream generator outlives this borrow.
        let thinking = self.thinks();
        let open_marker = self.thought_opening().to_string();
        let model = self.api_model.clone();
        let output = async_stream::stream! {
            let seen_marker = seen_marker;
            let mut calls: HashMap<u32, (String, String, String)> = HashMap::new();
            let mut refusal = String::new();
            let mut usage = None;
            let mut saw_reasoning = false;
            let mut saw_text = false;
            let mut acc_text = String::new();
            // Raw `reasoning_content` field, accumulated for the wire dump only.
            let mut acc_reasoning = String::new();
            let mut splitter = ThinkSplitter::new(thinking, &open_marker);

            while let Some(item) = upstream.next().await {
                // Only a whole marker is worth remembering: one straddling
                // chunks is first seen half-written.
                if let Some(marker) = splitter.close_marker() {
                    *seen_marker.lock().expect("lock") = Some(marker.to_string());
                }
                let response = match item {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::warn!(
                            chars = acc_text.len(),
                            preview = %redact(&acc_text),
                            "[llm] stream failed mid-way"
                        );
                        if wire_enabled() {
                            let partial: Vec<_> = calls
                                .values()
                                .map(|(call_id, name, arguments)| ToolCallRequest {
                                    call_id: call_id.clone(),
                                    name: name.clone(),
                                    arguments: Value::String(arguments.clone()),
                                })
                                .collect();
                            log_stream_result(
                                &model,
                                &format!("error: {error}"),
                                None,
                                &acc_text,
                                &acc_reasoning,
                                &refusal,
                                &partial,
                                usage.as_ref(),
                            );
                        }
                        yield Err(provider_error(error));
                        return;
                    }
                };
                let reasoning = extract_reasoning(&response, "delta");
                if let Some(chunk_usage) = response.usage {
                    usage = Some(chunk_usage);
                }
                let Some(choice) = response.choices.first() else {
                    continue;
                };
                if let Some(reasoning) = reasoning {
                    acc_reasoning.push_str(&reasoning);
                    // The provider carries the CoT in its own field: stop
                    // splitting and give back whatever body is being held.
                    for part in splitter.disable() {
                        if let Split::Text(text) = part {
                            saw_text = true;
                            yield Ok(ResponseChunk::TextBlock(text));
                        }
                    }
                    saw_reasoning = true;
                    yield Ok(ResponseChunk::Think(reasoning));
                }
                // Only a whole marker is worth remembering: one straddling
                // chunks is first seen half-written.
                if let Some(marker) = splitter.close_marker() {
                    *seen_marker.lock().expect("lock") = Some(marker.to_string());
                }
                if let Some(content) = choice.delta.content.as_deref().filter(|text| !text.is_empty()) {
                    let delta = normalize_text_delta(&acc_text, content);
                    if !delta.is_empty() {
                        acc_text.push_str(delta);
                        for part in splitter.push(delta) {
                            match part {
                                Split::Think(text) => {
                                    saw_reasoning = true;
                                    yield Ok(ResponseChunk::Think(text));
                                }
                                Split::Text(text) => {
                                    saw_text = true;
                                    yield Ok(ResponseChunk::TextBlock(text));
                                }
                            }
                        }
                    }
                }
                if let Some(part) = choice.delta.refusal.as_deref() {
                    refusal.push_str(part);
                }
                if let Some(tool_calls) = &choice.delta.tool_calls {
                    for call in tool_calls {
                        merge_tool_chunk(&mut calls, call);
                    }
                }

                if let Some(reason) = choice.finish_reason.as_ref() {
                    // With `include_usage`, totals arrive in a chunk AFTER the
                    // one carrying finish_reason. Drain for them, stopping at
                    // the first chunk that is not a usage carrier — the exact
                    // semantics of the `let`-chain this replaces.
                    loop {
                        let Some(extra) = upstream.next().await else { break };
                        let Ok(extra) = extra else { break };
                        let Some(extra_usage) = extra.usage else { break };
                        usage = Some(extra_usage);
                    }
                    for part in splitter.finish() {
                        match part {
                            Split::Think(text) => {
                                saw_reasoning = true;
                                yield Ok(ResponseChunk::Think(text));
                            }
                            Split::Text(text) => {
                                saw_text = true;
                                yield Ok(ResponseChunk::TextBlock(text));
                            }
                        }
                    }
                    if saw_reasoning {
                        yield Ok(ResponseChunk::ThinkDone);
                    }
                    if saw_text {
                        yield Ok(ResponseChunk::TextDone);
                    }
                    let finalized = finalize_calls(calls);
                    log_stream_result(
                        &model,
                        "finished",
                        Some(reason),
                        &acc_text,
                        &acc_reasoning,
                        &refusal,
                        &finalized,
                        usage.as_ref(),
                    );
                    let call_count = finalized.len();
                    for call in finalized {
                        yield Ok(ResponseChunk::ToolCall(call));
                    }
                    let reason = reconcile_tool_calls(
                        finish_reason(Some(reason), refusal),
                        Some(reason),
                        call_count,
                    );
                    yield Ok(ResponseChunk::Done {
                        reason,
                        usage: usage.as_ref().map(map_usage),
                    });
                    return;
                }
            }

            // The stream ended without a finish_reason: the upstream (or a
            // gateway in front of it) closed mid-round. What was collected is
            // still emitted, but the round is reported as interrupted so the
            // runner neither commits it as finished nor trusts its tool calls.
            for part in splitter.finish() {
                match part {
                    Split::Think(text) => {
                        saw_reasoning = true;
                        yield Ok(ResponseChunk::Think(text));
                    }
                    Split::Text(text) => {
                        saw_text = true;
                        yield Ok(ResponseChunk::TextBlock(text));
                    }
                }
            }
            if saw_reasoning {
                yield Ok(ResponseChunk::ThinkDone);
            }
            if saw_text {
                yield Ok(ResponseChunk::TextDone);
            }
            let finalized = finalize_calls(calls);
            tracing::warn!(
                model = %model,
                chars = acc_text.len(),
                calls = finalized.len(),
                "[llm] stream ended without finish_reason; reporting the round as interrupted"
            );
            log_stream_result(
                &model,
                "ended without finish_reason",
                None,
                &acc_text,
                &acc_reasoning,
                &refusal,
                &finalized,
                usage.as_ref(),
            );
            for call in finalized {
                yield Ok(ResponseChunk::ToolCall(call));
            }
            let reason = if refusal.is_empty() {
                FinishReason::Interrupted
            } else {
                finish_reason(None, refusal)
            };
            yield Ok(ResponseChunk::Done {
                reason,
                usage: usage.as_ref().map(map_usage),
            });
        };
        Ok(Box::pin(output))
    }
}

/// OpenAI-compatible endpoints normally stream text deltas, but some provider
/// nodes intermittently stream the complete text generated so far. Convert
/// those cumulative snapshots into deltas before forwarding them; otherwise
/// every prefix is appended again and the response grows quadratically.
fn normalize_text_delta<'a>(accumulated: &str, incoming: &'a str) -> &'a str {
    incoming.strip_prefix(accumulated).unwrap_or(incoming)
}

/// Tool calls arrive as indexed fragments; only the assembled set is stable, so
/// they are sorted by call id before being emitted.
fn finalize_calls(calls: HashMap<u32, (String, String, String)>) -> Vec<ToolCallRequest> {
    let mut finalized: Vec<_> = calls
        .into_values()
        .map(|(call_id, name, arguments)| ToolCallRequest {
            call_id,
            name,
            arguments: parse_arguments(&arguments),
        })
        .filter(|call| !call.name.is_empty())
        .collect();
    finalized.sort_by(|left, right| left.call_id.cmp(&right.call_id));
    finalized
}

fn redact(text: &str) -> String {
    if text.len() <= LOG_TEXT_LIMIT {
        return text.to_string();
    }
    // Char boundary, not byte boundary: the text is routinely non-ASCII.
    let cut = text
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= LOG_TEXT_LIMIT)
        .last()
        .unwrap_or(0);
    format!("{}…[+{} bytes]", &text[..cut], text.len() - cut)
}

fn to_wire_messages(
    messages: &[RequestMessage],
    replay: ReasoningReplay,
    open: &str,
    close: &str,
) -> Vec<WireMessage> {
    messages
        .iter()
        .map(|message| match message {
            RequestMessage::System { content } => WireMessage::System(WireSystemMessage {
                content: WireSystemContent::Text(content.clone()),
                name: None,
            }),
            RequestMessage::User { content } => WireMessage::User(WireUserMessage {
                content: WireUserContent::Text(content.clone()),
                name: None,
            }),
            RequestMessage::Assistant {
                content,
                tool_calls,
                reasoning,
            } => {
                let reasoning = reasoning.as_deref().filter(|text| !text.is_empty());
                let (content, replayed) = match (replay, reasoning) {
                    (ReasoningReplay::Inline, Some(thought)) => {
                        (format!("{open}{thought}{close}{content}"), None)
                    }
                    (ReasoningReplay::Field(field), Some(thought)) => {
                        (content.clone(), Some((field, thought.to_string())))
                    }
                    _ => (content.clone(), None),
                };
                wire_assistant(content, tool_calls.as_deref(), replayed)
            }
            RequestMessage::Tool { content, call_id } => WireMessage::Tool(WireToolMessage {
                content: WireToolContent::Text(content.clone()),
                tool_call_id: call_id.clone(),
            }),
        })
        .collect()
}

fn wire_assistant(
    content: String,
    tool_calls: Option<&[ToolCallRequest]>,
    reasoning: Option<(ReasoningField, String)>,
) -> WireMessage {
    let (reasoning_content, reasoning) = match reasoning {
        Some((ReasoningField::ReasoningContent, text)) => (Some(text), None),
        Some((ReasoningField::Reasoning, text)) => (None, Some(text)),
        None => (None, None),
    };
    WireMessage::Assistant(WireAssistantMessage {
        // A tool-call-only assistant turn must send no content field
        // at all; an empty string is rejected by some providers.
        content: if content.is_empty() && tool_calls.is_some() {
            None
        } else {
            Some(WireAssistantContent::Text(content))
        },
        tool_calls: tool_calls.map(|calls| {
            calls
                .iter()
                .map(|call| {
                    WireMessageToolCalls::Function(WireMessageToolCall {
                        id: call.call_id.clone(),
                        function: FunctionCall {
                            name: call.name.clone(),
                            arguments: wire_arguments(call),
                        },
                    })
                })
                .collect()
        }),
        reasoning_content,
        reasoning,
        ..Default::default()
    })
}

fn map_tools(tools: &[ToolManifest], forward_strict: bool) -> Option<Vec<WireTools>> {
    (!tools.is_empty()).then(|| {
        tools
            .iter()
            .map(|tool| {
                WireTools::Function(WireTool {
                    function: FunctionObject {
                        name: tool.name.clone(),
                        description: Some(tool.description.clone()),
                        // Already a Value: no re-parse, so a schema can no
                        // longer be silently dropped on the way out.
                        parameters: Some(tool.parameters.clone()),
                        strict: tool.strict.filter(|_| forward_strict),
                    },
                })
            })
            .collect()
    })
}

/// A round the upstream ended with `finish_reason: "tool_calls"` yet whose
/// calls all failed to survive parsing is not a plain stop: the model meant to
/// call, so the runner gets the chance to recover the call.
fn reconcile_tool_calls(
    reason: FinishReason,
    wire: Option<&WireFinishReason>,
    calls: usize,
) -> FinishReason {
    if calls == 0
        && reason == FinishReason::Stop
        && matches!(wire, Some(WireFinishReason::ToolCalls))
    {
        tracing::warn!("[llm] finish_reason is tool_calls but no tool call was parsed");
        return FinishReason::MissingToolCalls;
    }
    reason
}

fn tool_calls_from_message(calls: &[WireMessageToolCalls]) -> Vec<ToolCallRequest> {
    let mut calls: Vec<_> = calls
        .iter()
        .filter_map(|call| match call {
            WireMessageToolCalls::Function(call) if !call.function.name.is_empty() => {
                Some(ToolCallRequest {
                    call_id: call.id.clone(),
                    name: call.function.name.clone(),
                    arguments: parse_arguments(&call.function.arguments),
                })
            }
            _ => None,
        })
        .collect();
    calls.sort_by(|left, right| left.call_id.cmp(&right.call_id));
    calls
}

/// Fragments of one call arrive across chunks keyed by `index`: the id and name
/// appear once, the arguments accumulate.
fn merge_tool_chunk(calls: &mut HashMap<u32, (String, String, String)>, chunk: &WireToolCallChunk) {
    let entry = calls.entry(chunk.index).or_insert_with(|| {
        (
            chunk.id.clone().unwrap_or_default(),
            chunk
                .function
                .as_ref()
                .and_then(|function| function.name.clone())
                .unwrap_or_default(),
            String::new(),
        )
    });
    if let Some(id) = chunk.id.as_deref().filter(|id| !id.is_empty()) {
        entry.0 = id.to_string();
    }
    if let Some(function) = &chunk.function {
        if let Some(name) = function.name.as_deref().filter(|name| !name.is_empty()) {
            entry.1 = name.to_string();
        }
        if let Some(arguments) = &function.arguments {
            entry.2.push_str(arguments);
        }
    }
}

/// Refusal wording wins over the wire reason: a provider that both filtered the
/// output and reported `stop` has still refused.
fn finish_reason(reason: Option<&WireFinishReason>, refusal: String) -> FinishReason {
    if !refusal.is_empty() {
        return FinishReason::Refusal {
            reason: Some(refusal),
        };
    }
    match reason {
        Some(WireFinishReason::Length) => FinishReason::Length,
        Some(WireFinishReason::ContentFilter) => FinishReason::Refusal { reason: None },
        // Includes `tool_calls`: the requested calls are already expressed as
        // ToolCall chunks, so nothing about the reason needs to say so.
        _ => FinishReason::Stop,
    }
}

fn map_usage(usage: &CompletionUsage) -> ModelUsage {
    ModelUsage {
        prompt_tokens: usage.prompt_tokens,
        completion_tokens: usage.completion_tokens,
        total_tokens: usage.total_tokens,
        cached_tokens: 0,
        reasoning_tokens: 0,
    }
}

/// Reasoning is dug out of the serialised form,
/// `choices[0].{message|delta}.{reasoning_content|reasoning}`: the DeepSeek-era
/// name first, then the one vLLM 0.16+ emits. An endpoint sending both carries
/// the same text in each, so only one is taken.
fn extract_reasoning(value: &impl serde::Serialize, container: &str) -> Option<String> {
    let value = serde_json::to_value(value).ok()?;
    let message = value
        .get("choices")?
        .as_array()?
        .first()?
        .get(container)?
        .clone();
    ["reasoning_content", "reasoning"].iter().find_map(|field| {
        message
            .get(*field)
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
    })
}

fn parse_arguments(arguments: &str) -> Value {
    let arguments = arguments.trim();
    if arguments.is_empty() {
        Value::Object(Default::default())
    } else {
        // A model that emits malformed JSON still made a call; keep the raw
        // text so the tool can reject it with a useful message.
        serde_json::from_str(arguments).unwrap_or_else(|_| Value::String(arguments.to_string()))
    }
}

fn serialize_arguments(arguments: &Value) -> String {
    match arguments {
        // A raw string is what `parse_arguments` kept for output that was not
        // JSON. The gateway validates replayed history and rejects the whole
        // request on one such call, so it goes out as `{}`.
        Value::String(raw) if serde_json::from_str::<Value>(raw).is_ok() => raw.clone(),
        Value::String(_) | Value::Null => "{}".to_string(),
        arguments => serde_json::to_string(arguments).unwrap_or_else(|_| "{}".to_string()),
    }
}

/// [`serialize_arguments`] for one replayed call, noting a substitution.
fn wire_arguments(call: &ToolCallRequest) -> String {
    let wire = serialize_arguments(&call.arguments);
    if let Value::String(raw) = &call.arguments {
        if wire != *raw {
            tracing::warn!(
                call_id = %call.call_id,
                name = %call.name,
                raw_bytes = raw.len(),
                "replaying a tool call whose arguments are not JSON; sending {{}} instead"
            );
        }
    }
    wire
}

/// The `reasoning_effort` the endpoint named, verbatim.
///
/// Kept beside the enum because the two do not carry the same thing: the enum
/// is what the protocol accepts, this is what the deployment says. A value
/// outside the enum is still a switch this deployment understands.
fn reasoning_effort_name(extensions: &Value) -> Option<String> {
    extensions
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|level| !level.is_empty())
        .map(str::to_string)
}

/// Whether a level says "do not think". `no_think` is HunYuan's word for it,
/// and it is not a value the protocol's enum has — which is why the switch
/// has to travel as a template kwarg.
fn thinking_off(level: &str) -> bool {
    matches!(
        level.trim().to_ascii_lowercase().as_str(),
        "" | "none" | "no_think" | "no-think"
    )
}

/// Where an endpoint wants the thinking switch sent: `"standard"`, `"template"`,
/// or `"both"` (the default). A deployment that rejects the protocol's field
/// outright — a `no_think` or even a `none` there is a 400 — names `template`.
fn effort_via(extensions: &Value) -> ReasoningEffortVia {
    match extensions
        .get("reasoning_effort_via")
        .and_then(Value::as_str)
    {
        Some("standard") => ReasoningEffortVia::Standard,
        Some("template") => ReasoningEffortVia::Template,
        None | Some("both") => ReasoningEffortVia::Both,
        Some(other) => {
            tracing::warn!(value = other, "unknown reasoning_effort_via; sending both");
            ReasoningEffortVia::Both
        }
    }
}

/// The output budget a configured endpoint names, if any. Thinking draws on the
/// same budget as the tool arguments, so a deployment can raise it explicitly
/// rather than leave it to the upstream default.
fn max_completion_tokens(extensions: &Value) -> Option<u32> {
    extensions
        .get("max_completion_tokens")
        .and_then(Value::as_u64)
        .filter(|tokens| *tokens > 0)
        .and_then(|tokens| u32::try_from(tokens).ok())
}

/// The `reasoning_effort` a configured endpoint names: the protocol's own enum,
/// or `None` when it names none — no field on the wire, which is not the same
/// thing as `none`.
fn reasoning_effort(extensions: &Value) -> Option<ReasoningEffort> {
    match extensions
        .get("reasoning_effort")
        .and_then(Value::as_str)?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "none" => Some(ReasoningEffort::None),
        "minimal" => Some(ReasoningEffort::Minimal),
        "low" => Some(ReasoningEffort::Low),
        "medium" => Some(ReasoningEffort::Medium),
        "high" => Some(ReasoningEffort::High),
        "xhigh" => Some(ReasoningEffort::Xhigh),
        "max" => Some(ReasoningEffort::Max),
        _ => None,
    }
}

fn provider_error(error: impl fmt::Display) -> ModelError {
    ModelError::new(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every level of the protocol's enum reaches the wire, and `none` travels
    /// as its own value rather than as an absent field — otherwise a caller
    /// asking for "do not think" gets whatever the upstream defaults to.
    #[test]
    fn the_configured_reasoning_effort_is_the_protocols_enum() {
        let level =
            |value: &str| reasoning_effort(&serde_json::json!({ "reasoning_effort": value }));
        for (value, expected) in [
            ("none", ReasoningEffort::None),
            ("minimal", ReasoningEffort::Minimal),
            ("low", ReasoningEffort::Low),
            ("medium", ReasoningEffort::Medium),
            ("high", ReasoningEffort::High),
            ("xhigh", ReasoningEffort::Xhigh),
            ("max", ReasoningEffort::Max),
        ] {
            assert_eq!(level(value), Some(expected.clone()), "{value}");
        }
        assert_eq!(level(" HIGH "), Some(ReasoningEffort::High));
        // No field at all: the request stays silent about thinking.
        assert_eq!(reasoning_effort(&serde_json::json!({})), None);
        // Outside the enum: not a level the protocol defines.
        assert_eq!(level("x-high"), None);
    }

    /// A turn told not to think gets no CoT, so inline think-marker splitting
    /// stays off for it — as it does for a turn naming no level at all.
    #[test]
    fn none_asks_the_model_not_to_think() {
        let thinks = |extensions: Value| {
            HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .thinks()
        };
        assert!(thinks(serde_json::json!({ "reasoning_effort": "high" })));
        assert!(!thinks(serde_json::json!({ "reasoning_effort": "none" })));
        assert!(!thinks(serde_json::json!({})));
    }

    /// A busy endpoint and a local one do not wait alike, and only the endpoint
    /// knows which it is. Whatever it does not name keeps the default.
    #[test]
    fn an_endpoint_names_its_own_timeouts() {
        let timeouts = |extensions: Value| {
            HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .stream_timeouts()
            .expect("the transport reports the timeouts it was given")
        };

        let named = timeouts(serde_json::json!({
            "stream_timeout_first_chunk_ms": 240_000,
            "stream_timeout_idle_ms": 30_000,
            "stream_timeout_total_ms": 900_000,
        }));
        assert_eq!(named.first_chunk, Duration::from_secs(240));
        assert_eq!(named.idle, Duration::from_secs(30));
        assert_eq!(named.total, Duration::from_secs(900));

        let partial = timeouts(serde_json::json!({
            "stream_timeout_first_chunk_ms": 1_000,
            "stream_timeout_total_ms": 0,
        }));
        let default = StreamTimeouts::default();
        assert_eq!(partial.first_chunk, Duration::from_secs(1));
        assert_eq!(partial.idle, default.idle, "unnamed parts keep the default");
        assert_eq!(partial.total, default.total, "zero is not a timeout");
    }

    /// Where the switch goes is the endpoint's call: a deployment that answers
    /// the protocol's field with a 400 on `no_think` — even on `none` — has to
    /// be able to keep it out of there.
    #[test]
    fn the_thinking_switch_goes_where_the_endpoint_says() {
        let sent = |extensions: Value| {
            let request = HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .request(&[], &[], true);
            (request.reasoning_effort, request.chat_template_kwargs)
        };
        let kwargs = |level: &str| Some(serde_json::json!({ "reasoning_effort": level }));

        let (standard, template) = sent(serde_json::json!({ "reasoning_effort": "high" }));
        assert_eq!(standard, Some(ReasoningEffort::High));
        assert_eq!(template, kwargs("high"), "both is the default");

        let (standard, template) = sent(serde_json::json!({
            "reasoning_effort": "no_think",
            "reasoning_effort_via": "template",
        }));
        assert!(standard.is_none(), "the field would be a 400");
        assert_eq!(template, kwargs("no_think"));

        let (standard, template) = sent(serde_json::json!({
            "reasoning_effort": "none",
            "reasoning_effort_via": "standard",
        }));
        assert_eq!(standard, Some(ReasoningEffort::None));
        assert!(template.is_none());
    }

    /// HunYuan's switch is not a level of the protocol's enum, so it travels in
    /// the chat template kwargs — that is the field its serving stack reads —
    /// while the protocol's own field only carries what it can express.
    #[test]
    fn the_thinking_switch_reaches_the_chat_template() {
        let request = |extensions: Value| {
            HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .request(&[], &[], true)
        };

        let high = request(serde_json::json!({ "reasoning_effort": "high" }));
        assert_eq!(high.reasoning_effort, Some(ReasoningEffort::High));
        assert_eq!(
            high.chat_template_kwargs,
            Some(serde_json::json!({ "reasoning_effort": "high" }))
        );

        let off = request(serde_json::json!({ "reasoning_effort": "no_think" }));
        assert!(
            off.reasoning_effort.is_none(),
            "no_think is not a level the protocol has"
        );
        assert_eq!(
            off.chat_template_kwargs,
            Some(serde_json::json!({ "reasoning_effort": "no_think" })),
            "but it is a switch this deployment reads"
        );

        let silent = request(serde_json::json!({}));
        assert!(silent.reasoning_effort.is_none());
        assert!(
            silent.chat_template_kwargs.is_none(),
            "an endpoint that names no level says nothing about thinking"
        );
    }

    /// An endpoint that replays thinking inline is one that delivers it inline:
    /// it thinks whether or not it was asked to, so its body is split even when
    /// no level was named. An explicit switch off still wins.
    #[test]
    fn an_endpoint_replaying_thinking_inline_splits_without_a_level() {
        let thinks = |extensions: Value| {
            HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .thinks()
        };
        assert!(thinks(serde_json::json!({ "reasoning_replay": "inline" })));
        assert!(!thinks(serde_json::json!({})));
        assert!(!thinks(
            serde_json::json!({ "reasoning_replay": "inline", "reasoning_effort": "no_think" })
        ));
    }

    /// Over the wire, the endpoint that started all this: no level named, the
    /// CoT inlined, thoughts announced only by closing.
    #[tokio::test]
    async fn an_endpoint_that_only_closes_its_thoughts_splits_them_anyway() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "content": "ponder" }),
            serde_json::json!({ "content": "</think:abcd>Hi there!" }),
        ])
        .await;
        let (thinking, body) = one_turn(
            &base_url,
            serde_json::json!({ "reasoning_replay": "inline", "reasoning_inline_open_tag": "" }),
            TransportOverrides::new(),
        )
        .await;
        assert_eq!(thinking, "ponder");
        assert_eq!(body, "Hi there!");
    }

    #[test]
    fn preserves_regular_stream_deltas() {
        assert_eq!(normalize_text_delta("你好！", "我是 Nova"), "我是 Nova");
    }

    #[test]
    fn converts_cumulative_stream_snapshots_to_deltas() {
        assert_eq!(
            normalize_text_delta("你好！", "你好！我是 Nova"),
            "我是 Nova"
        );
        assert_eq!(
            normalize_text_delta("你好！我是 Nova", "你好！我是 Nova"),
            ""
        );
    }

    #[test]
    fn refusal_wording_outranks_the_wire_reason() {
        let reason = finish_reason(Some(&WireFinishReason::Stop), "declined".to_string());
        assert_eq!(
            reason,
            FinishReason::Refusal {
                reason: Some("declined".to_string())
            }
        );
    }

    #[test]
    fn content_filter_is_a_refusal_without_wording() {
        assert_eq!(
            finish_reason(Some(&WireFinishReason::ContentFilter), String::new()),
            FinishReason::Refusal { reason: None }
        );
    }

    #[test]
    fn tool_calls_reason_is_a_plain_stop() {
        // The calls themselves carry that information; see `FinishReason`.
        assert_eq!(
            finish_reason(Some(&WireFinishReason::ToolCalls), String::new()),
            FinishReason::Stop
        );
    }

    #[test]
    fn length_maps_to_its_own_reason() {
        assert_eq!(
            finish_reason(Some(&WireFinishReason::Length), String::new()),
            FinishReason::Length
        );
    }

    #[test]
    fn unparseable_arguments_survive_as_raw_text() {
        assert_eq!(parse_arguments("  "), Value::Object(Default::default()));
        assert_eq!(
            parse_arguments("{not json"),
            Value::String("{not json".to_string())
        );
        assert_eq!(parse_arguments(r#"{"a":1}"#), serde_json::json!({"a": 1}));
    }

    #[test]
    fn redaction_caps_text_and_never_splits_a_character() {
        let long = "中".repeat(400);
        let out = redact(&long);
        assert!(out.contains('…'), "long text must be elided");
        assert!(out.len() < long.len());
        // Would panic on a byte-boundary slice of multi-byte characters.
        assert!(out.starts_with('中'));

        let short = "短文本";
        assert_eq!(redact(short), short);
    }

    #[test]
    fn api_key_debug_is_opaque() {
        let key = ApiKey::new("sk-secret-value");
        assert_eq!(format!("{key:?}"), "ApiKey(***)");
        assert!(!format!("{key:?}").contains("secret"));
    }

    #[test]
    fn tool_schemas_pass_through_without_a_reparse() {
        let schema = serde_json::json!({"type": "object", "properties": {"a": {"type": "string"}}});
        let tools = vec![ToolManifest {
            name: "t".into(),
            description: "d".into(),
            parameters: schema.clone(),
            strict: None,
        }];
        let mapped = map_tools(&tools, false).expect("tools present");
        match &mapped[0] {
            WireTools::Function(tool) => {
                assert_eq!(tool.function.parameters.as_ref(), Some(&schema));
            }
            other => panic!("only function tools are declarable, got {other:?}"),
        }
    }

    #[test]
    fn no_tools_declares_no_tools_field() {
        assert!(map_tools(&[], false).is_none());
    }

    #[test]
    fn tool_call_fragments_merge_by_index() {
        let mut calls = HashMap::new();
        merge_tool_chunk(
            &mut calls,
            &WireToolCallChunk {
                index: 0,
                id: Some("call_1".into()),
                r#type: None,
                function: Some(async_openai::types::chat::FunctionCallStream {
                    name: Some("draw".into()),
                    arguments: Some("{\"x\":".into()),
                }),
            },
        );
        merge_tool_chunk(
            &mut calls,
            &WireToolCallChunk {
                index: 0,
                id: None,
                r#type: None,
                function: Some(async_openai::types::chat::FunctionCallStream {
                    name: None,
                    arguments: Some("1}".into()),
                }),
            },
        );
        let finalized = finalize_calls(calls);
        assert_eq!(finalized.len(), 1);
        assert_eq!(finalized[0].call_id, "call_1");
        assert_eq!(finalized[0].name, "draw");
        assert_eq!(finalized[0].arguments, serde_json::json!({"x": 1}));
    }

    #[test]
    fn nameless_calls_are_dropped() {
        let mut calls = HashMap::new();
        calls.insert(0, ("call_1".to_string(), String::new(), "{}".to_string()));
        assert!(finalize_calls(calls).is_empty());
    }

    #[test]
    fn assistant_tool_call_turn_sends_no_content_field() {
        let wire = to_wire_messages(
            &[RequestMessage::Assistant {
                content: String::new(),
                tool_calls: Some(vec![ToolCallRequest {
                    call_id: "c1".into(),
                    name: "t".into(),
                    arguments: serde_json::json!({}),
                }]),
                reasoning: None,
            }],
            ReasoningReplay::Field(ReasoningField::ReasoningContent),
            "<think>",
            "</think>",
        );
        match &wire[0] {
            WireMessage::Assistant(message) => assert!(message.content.is_none()),
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    /// Serve `deltas` as the upstream endpoint's SSE stream, then a stop frame.
    /// Returns the base URL a transport is built against.
    async fn serve_deltas(deltas: Vec<Value>) -> String {
        serve_deltas_ending(deltas, Some("stop")).await
    }

    /// Like [`serve_deltas`], closing on `finish` — or, given `None`, closing
    /// the stream with no finish frame at all, the way a timed-out gateway does.
    async fn serve_deltas_ending(deltas: Vec<Value>, finish: Option<&str>) -> String {
        use axum::response::sse::Event;

        let mut frames: Vec<Result<Event, std::convert::Infallible>> = deltas
            .into_iter()
            .map(|delta| {
                Ok(Event::default().data(
                    serde_json::json!({
                        "id": "chatcmpl-test",
                        "object": "chat.completion.chunk",
                        "created": 0,
                        "model": "test-model",
                        "choices": [{ "index": 0, "delta": delta, "finish_reason": null }],
                    })
                    .to_string(),
                ))
            })
            .collect();
        if let Some(finish) = finish {
            frames.push(Ok(Event::default().data(
                serde_json::json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion.chunk",
                    "created": 0,
                    "model": "test-model",
                    "choices": [{ "index": 0, "delta": {}, "finish_reason": finish }],
                })
                .to_string(),
            )));
        }
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move |_body: String| async move {
                axum::response::Sse::new(futures::stream::iter(frames))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v1")
    }

    /// What a caller of one turn ends up with: the reasoning and the reply.
    async fn one_turn(
        base_url: &str,
        extensions: Value,
        transport: TransportOverrides,
    ) -> (String, String) {
        let completion = HttpChatCompletion::new(
            ModelEndpoint::new(ApiKey::new("key"), base_url, "test-model")
                .with_extensions(extensions),
        );
        let messages = vec![RequestMessage::User {
            content: "hi".to_string(),
        }];
        let mut stream = completion
            .completion(&messages, &[], true, &transport)
            .await
            .unwrap();
        let mut thinking = String::new();
        let mut body = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk.unwrap() {
                ResponseChunk::Think(part) => thinking.push_str(&part),
                ResponseChunk::TextBlock(part) => body.push_str(&part),
                _ => {}
            }
        }
        (thinking, body)
    }

    /// Over the wire, in the exact shape Hy3 sends: the CoT inlined in
    /// `content`, `reasoning_content` permanently null. It must reach the caller
    /// as reasoning and the reply as body, or the UI cannot tell them apart.
    #[tokio::test]
    async fn a_streamed_reply_with_inline_thinking_splits_over_the_wire() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "content": "ponder", "reasoning_content": null }),
            serde_json::json!({ "content": " more", "reasoning_content": null }),
            serde_json::json!({ "content": "</think>", "reasoning_content": null }),
            serde_json::json!({ "content": "Hi there!", "reasoning_content": null }),
        ])
        .await;
        let (thinking, body) = one_turn(
            &base_url,
            serde_json::json!({ "reasoning_effort": "high" }),
            TransportOverrides::new(),
        )
        .await;
        assert_eq!(thinking, "ponder more");
        assert_eq!(body, "Hi there!");
    }

    /// A provider carrying the CoT in its own field needs no splitting — and
    /// that field only exists in the typed schema at all because the dependency
    /// is pinned to a rev that carries it.
    #[tokio::test]
    async fn a_streamed_reasoning_field_becomes_thinking() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "reasoning_content": "ponder" }),
            serde_json::json!({ "content": "Hi!", "reasoning_content": null }),
        ])
        .await;
        let (thinking, body) =
            one_turn(&base_url, serde_json::json!({}), TransportOverrides::new()).await;
        assert_eq!(thinking, "ponder");
        assert_eq!(body, "Hi!");
    }

    /// A proxied upstream needs an accounting id the endpoint declaration
    /// cannot express, and sometimes a caller-chosen key. Both must ride along
    /// — and the key must WIN over the endpoint's own, which is the whole point
    /// of stamping it per round.
    #[tokio::test]
    async fn passthrough_headers_reach_the_wire() {
        use axum::body::Body as WireBody;
        use axum::http::Request;
        use axum::response::sse::Event;
        use std::sync::{Arc, Mutex};

        let seen: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let app = axum::Router::new().route(
            "/v1/chat/completions",
            axum::routing::post(move |request: Request<WireBody>| {
                let sink = sink.clone();
                async move {
                    for name in ["x-litellm-customer-id", "authorization"] {
                        if let Some(value) = request.headers().get(name) {
                            sink.lock().expect("lock").push((
                                name.to_string(),
                                value.to_str().unwrap_or_default().to_string(),
                            ));
                        }
                    }
                    axum::response::Sse::new(futures::stream::iter(vec![Ok::<
                        Event,
                        std::convert::Infallible,
                    >(
                        Event::default().data(
                            serde_json::json!({
                                "id": "chatcmpl-test",
                                "object": "chat.completion.chunk",
                                "created": 0,
                                "model": "test-model",
                                "choices": [{ "index": 0, "delta": { "content": "hi" }, "finish_reason": "stop" }],
                            })
                            .to_string(),
                        ),
                    )]))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let base_url = format!("http://{addr}/v1");

        let transport = TransportOverrides::new()
            .with_header("x-litellm-customer-id", "alice")
            .with_header("authorization", "Bearer sk-caller");
        let (_thinking, body) = one_turn(&base_url, serde_json::json!({}), transport).await;

        assert_eq!(body, "hi");
        let seen = seen.lock().expect("lock").clone();
        assert!(
            seen.contains(&("x-litellm-customer-id".to_string(), "alice".to_string())),
            "the accounting id must ride along: {seen:?}"
        );
        assert!(
            seen.contains(&("authorization".to_string(), "Bearer sk-caller".to_string())),
            "a per-round key must win over the endpoint's own: {seen:?}"
        );
    }

    /// A turn that never asked the model to think cannot be holding its body
    /// back: there is no marker coming, so the whole reply streams through.
    #[tokio::test]
    async fn a_turn_that_did_not_ask_to_think_streams_its_whole_body() {
        let base_url =
            serve_deltas(vec![serde_json::json!({ "content": "Just the answer." })]).await;
        let (thinking, body) =
            one_turn(&base_url, serde_json::json!({}), TransportOverrides::new()).await;
        assert_eq!(thinking, "");
        assert_eq!(body, "Just the answer.");
    }

    /// Every chunk one streamed turn yields, in order.
    async fn turn_chunks(base_url: &str) -> Vec<ResponseChunk> {
        let completion = HttpChatCompletion::new(ModelEndpoint::new(
            ApiKey::new("key"),
            base_url,
            "test-model",
        ));
        let messages = vec![RequestMessage::User {
            content: "hi".to_string(),
        }];
        let mut stream = completion
            .completion(&messages, &[], true, &TransportOverrides::new())
            .await
            .unwrap();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.unwrap());
        }
        chunks
    }

    fn final_reason(chunks: &[ResponseChunk]) -> Option<FinishReason> {
        chunks.iter().rev().find_map(|chunk| match chunk {
            ResponseChunk::Done { reason, .. } => Some(reason.clone()),
            _ => None,
        })
    }

    /// The incident shape: a gateway timeout closes the SSE stream while a tool
    /// call's arguments are half written. That must surface as an interruption,
    /// not as the natural stop that once committed the fragment as a finished call.
    #[tokio::test]
    async fn a_stream_closed_without_a_finish_reason_is_interrupted() {
        let base_url = serve_deltas_ending(
            vec![serde_json::json!({
                "tool_calls": [{
                    "index": 0,
                    "id": "call_1",
                    "type": "function",
                    "function": { "name": "video_tool", "arguments": "{\"op\":\"t2v\",\"prompt\":\"雨" },
                }],
            })],
            None,
        )
        .await;
        let chunks = turn_chunks(&base_url).await;
        assert_eq!(final_reason(&chunks), Some(FinishReason::Interrupted));
    }

    #[tokio::test]
    async fn a_stream_closed_on_a_finish_reason_is_not_interrupted() {
        let base_url =
            serve_deltas_ending(vec![serde_json::json!({ "content": "done" })], Some("stop")).await;
        let chunks = turn_chunks(&base_url).await;
        assert_eq!(final_reason(&chunks), Some(FinishReason::Stop));
    }

    /// Replaying a malformed argument string verbatim is what the gateway 400s
    /// on, failing the whole chain. The call's own tool output already tells the
    /// model its arguments were broken, so the wire only needs valid JSON.
    #[test]
    fn replayed_malformed_arguments_go_out_as_an_empty_object() {
        let wire = to_wire_messages(
            &[RequestMessage::Assistant {
                content: String::new(),
                tool_calls: Some(vec![
                    ToolCallRequest {
                        call_id: "broken".into(),
                        name: "video_tool".into(),
                        arguments: Value::String("{\"op\":\"t2v\",\"prompt\":\"雨".into()),
                    },
                    ToolCallRequest {
                        call_id: "fine".into(),
                        name: "video_tool".into(),
                        arguments: serde_json::json!({ "op": "t2v" }),
                    },
                ]),
                reasoning: None,
            }],
            ReasoningReplay::Field(ReasoningField::ReasoningContent),
            "<think>",
            "</think>",
        );
        let WireMessage::Assistant(message) = &wire[0] else {
            panic!("expected an assistant message, got {:?}", wire[0]);
        };
        let arguments: Vec<_> = message
            .tool_calls
            .as_ref()
            .expect("tool calls present")
            .iter()
            .map(|call| match call {
                WireMessageToolCalls::Function(call) => call.function.arguments.clone(),
                other => panic!("expected a function call, got {other:?}"),
            })
            .collect();
        assert_eq!(
            arguments,
            vec!["{}".to_string(), r#"{"op":"t2v"}"#.to_string()]
        );
    }

    #[test]
    fn a_raw_argument_string_that_is_valid_json_goes_out_verbatim() {
        assert_eq!(
            serialize_arguments(&Value::String(r#"{"a": 1}"#.into())),
            r#"{"a": 1}"#
        );
    }

    /// The output budget is an endpoint setting: absent means the request names
    /// none, and a value that is not a positive integer is not a budget.
    #[test]
    fn max_completion_tokens_comes_from_the_endpoint_extensions() {
        let budget = |extensions: Value| {
            HttpChatCompletion::new(
                ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m")
                    .with_extensions(extensions),
            )
            .request(&[], &[], true)
            .max_completion_tokens
        };
        assert_eq!(
            budget(serde_json::json!({ "max_completion_tokens": 32000 })),
            Some(32000)
        );
        assert_eq!(budget(serde_json::json!({})), None);
        assert_eq!(
            budget(serde_json::json!({ "max_completion_tokens": 0 })),
            None
        );
        assert_eq!(
            budget(serde_json::json!({ "max_completion_tokens": "32000" })),
            None
        );
    }

    /// The assistant turn as replayed under `extensions`: (content, reasoning_content).
    fn replayed(extensions: Value, content: &str) -> (Option<String>, Option<String>) {
        let (content, reasoning_content, _) = replayed_fields(extensions, content);
        (content, reasoning_content)
    }

    /// (content, reasoning_content, reasoning) of the replayed assistant turn.
    fn replayed_fields(
        extensions: Value,
        content: &str,
    ) -> (Option<String>, Option<String>, Option<String>) {
        let completion = HttpChatCompletion::new(
            ModelEndpoint::new(ApiKey::new("k"), "http://h/v1", "m").with_extensions(extensions),
        );
        let request = completion.request(
            &[RequestMessage::Assistant {
                content: content.into(),
                tool_calls: (content.is_empty()).then(|| {
                    vec![ToolCallRequest {
                        call_id: "c1".into(),
                        name: "t".into(),
                        arguments: serde_json::json!({}),
                    }]
                }),
                reasoning: Some("plan".into()),
            }],
            &[],
            true,
        );
        match &request.messages[0] {
            WireMessage::Assistant(message) => (
                message.content.as_ref().map(|content| match content {
                    WireAssistantContent::Text(text) => text.clone(),
                    other => panic!("expected text content, got {other:?}"),
                }),
                message.reasoning_content.clone(),
                message.reasoning.clone(),
            ),
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    /// Interleaved thinking: the assistant turn goes back with the reasoning
    /// it was produced with, as a field by default.
    #[test]
    fn assistant_reasoning_is_replayed_as_reasoning_content_by_default() {
        assert_eq!(
            replayed(serde_json::json!({}), "on it"),
            (Some("on it".into()), Some("plan".into()))
        );
        assert_eq!(
            replayed(serde_json::json!({ "reasoning_replay": "off" }), "on it"),
            (Some("on it".into()), None)
        );
    }

    /// An endpoint that inlines its thought in the body gets it back the way
    /// the model wrote it, ahead of the reply — including on a turn that only
    /// called tools, which otherwise sends no content at all.
    #[test]
    fn inline_replay_puts_the_thought_back_in_think_markers() {
        let inline = serde_json::json!({ "reasoning_replay": "inline" });
        assert_eq!(
            replayed(inline.clone(), "on it"),
            (Some("<think>plan</think>on it".into()), None)
        );
        assert_eq!(
            replayed(inline, ""),
            (Some("<think>plan</think>".into()), None)
        );
    }

    /// The field the reasoning rides in follows the endpoint: the newer
    /// `reasoning` name only when asked for, and never both.
    #[test]
    fn field_replay_uses_the_configured_field_name() {
        assert_eq!(
            replayed_fields(serde_json::json!({}), "on it"),
            (Some("on it".into()), Some("plan".into()), None)
        );
        assert_eq!(
            replayed_fields(
                serde_json::json!({ "reasoning_field": "reasoning" }),
                "on it"
            ),
            (Some("on it".into()), None, Some("plan".into()))
        );
        assert_eq!(
            replayed_fields(serde_json::json!({ "reasoning_field": "bogus" }), "on it"),
            (Some("on it".into()), Some("plan".into()), None)
        );
    }

    /// vLLM 0.16+ streams its thinking as `reasoning`; it must still reach the
    /// caller as reasoning, not be dropped.
    #[tokio::test]
    async fn a_streamed_reasoning_field_under_the_newer_name_becomes_thinking() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "reasoning": "ponder" }),
            serde_json::json!({ "content": "Hi!" }),
        ])
        .await;
        let (thinking, body) =
            one_turn(&base_url, serde_json::json!({}), TransportOverrides::new()).await;
        assert_eq!(thinking, "ponder");
        assert_eq!(body, "Hi!");
    }

    /// The off-format body a live session produced — the model closed a thought
    /// it never wrote, thought, then closed again — reaches the caller as
    /// reasoning and reply, with no marker in either.
    #[tokio::test]
    async fn an_off_format_body_is_split_at_its_last_marker_over_the_wire() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "content": "</think:6124c78e>Asset card locked? " }),
            serde_json::json!({ "content": "Let me check the card.</think:6124c78e>" }),
            serde_json::json!({ "content": "Asset card locked: asset_87fd." }),
        ])
        .await;
        let completion = HttpChatCompletion::new(
            ModelEndpoint::new(ApiKey::new("key"), &base_url, "test-model")
                .with_extensions(serde_json::json!({ "reasoning_effort": "high" })),
        );
        let mut stream = completion
            .completion(
                &[RequestMessage::User {
                    content: "hi".to_string(),
                }],
                &[],
                true,
                &TransportOverrides::new(),
            )
            .await
            .unwrap();

        let mut reasoning = String::new();
        let mut body = String::new();
        while let Some(chunk) = stream.next().await {
            match chunk.unwrap() {
                ResponseChunk::Think(text) => reasoning.push_str(&text),
                ResponseChunk::TextBlock(text) => body.push_str(&text),
                _ => {}
            }
        }
        assert_eq!(reasoning, "Asset card locked? Let me check the card.");
        assert_eq!(body, "Asset card locked: asset_87fd.");
        assert!(!body.contains("</think"), "no marker may reach the reply");
    }

    /// A deployment whose template suffixes its markers gets them back in
    /// exactly that form; one that emits no opening marker gets none, and a
    /// non-string closing marker falls back.
    #[test]
    fn inline_replay_uses_the_configured_think_markers() {
        let suffixed = serde_json::json!({
            "reasoning_replay": "inline",
            "reasoning_inline_open_tag": "<think:abcd>",
            "reasoning_inline_close_tag": "</think:abcd>",
        });
        assert_eq!(
            replayed(suffixed, "on it"),
            (Some("<think:abcd>plan</think:abcd>on it".into()), None)
        );
        // Hy3-H20-Temp sends no opening marker, so its thought must not be
        // replayed wrapped in one it never wrote.
        let no_open = serde_json::json!({
            "reasoning_replay": "inline",
            "reasoning_inline_open_tag": "",
            "reasoning_inline_close_tag": "</think:6124c78e>",
        });
        assert_eq!(
            replayed(no_open, "on it"),
            (Some("plan</think:6124c78e>on it".into()), None)
        );
        let invalid =
            serde_json::json!({ "reasoning_replay": "inline", "reasoning_inline_close_tag": 1 });
        assert_eq!(
            replayed(invalid, "on it"),
            (Some("<think>plan</think>on it".into()), None)
        );
    }

    /// A suffixed marker need not be configured: the one the endpoint was seen
    /// to close a thought with is used from the next round on.
    #[tokio::test]
    async fn the_closing_marker_the_upstream_used_is_replayed_with() {
        let base_url = serve_deltas(vec![
            serde_json::json!({ "content": "pon" }),
            serde_json::json!({ "content": "der" }),
            serde_json::json!({ "content": "</think:abcd>Hi" }),
        ])
        .await;
        let completion = HttpChatCompletion::new(
            ModelEndpoint::new(ApiKey::new("key"), &base_url, "test-model").with_extensions(
                serde_json::json!({
                    "reasoning_effort": "high",
                    "reasoning_replay": "inline",
                    "reasoning_inline_open_tag": "",
                }),
            ),
        );
        let mut stream = completion
            .completion(
                &[RequestMessage::User {
                    content: "hi".to_string(),
                }],
                &[],
                true,
                &TransportOverrides::new(),
            )
            .await
            .unwrap();
        while stream.next().await.is_some() {}

        let request = completion.request(
            &[RequestMessage::Assistant {
                content: "Hi".into(),
                tool_calls: None,
                reasoning: Some("plan".into()),
            }],
            &[],
            true,
        );
        match &request.messages[0] {
            WireMessage::Assistant(message) => {
                assert_eq!(
                    message.content.as_ref().and_then(|content| match content {
                        WireAssistantContent::Text(text) => Some(text.clone()),
                        _ => None,
                    }),
                    Some("plan</think:abcd>Hi".into())
                );
            }
            other => panic!("expected an assistant message, got {other:?}"),
        }
    }

    #[test]
    fn strict_reaches_the_wire_only_when_the_endpoint_opts_in() {
        let tools = vec![ToolManifest {
            name: "t".into(),
            description: "d".into(),
            parameters: serde_json::json!({ "type": "object" }),
            strict: Some(true),
        }];
        let strict_of = |forward| match &map_tools(&tools, forward).expect("tools")[0] {
            WireTools::Function(tool) => tool.function.strict,
            other => panic!("expected a function tool, got {other:?}"),
        };
        assert_eq!(strict_of(false), None);
        assert_eq!(strict_of(true), Some(true));
    }

    /// The Hy3 failure: the server parsed no call out of malformed markup but
    /// still reported `tool_calls`, keeping only the announcement as text.
    #[tokio::test]
    async fn tool_calls_reported_but_none_parsed_is_flagged() {
        let base_url = serve_deltas_ending(
            vec![serde_json::json!({ "content": "我来帮你生成。" })],
            Some("tool_calls"),
        )
        .await;
        let chunks = turn_chunks(&base_url).await;
        assert_eq!(final_reason(&chunks), Some(FinishReason::MissingToolCalls));
    }

    #[test]
    fn tool_calls_reported_with_calls_is_a_plain_stop() {
        assert_eq!(
            reconcile_tool_calls(FinishReason::Stop, Some(&WireFinishReason::ToolCalls), 1),
            FinishReason::Stop
        );
        assert_eq!(
            reconcile_tool_calls(FinishReason::Stop, Some(&WireFinishReason::Stop), 0),
            FinishReason::Stop
        );
    }
}

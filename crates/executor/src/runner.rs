//! The executor: exactly one model round per call.
//!
//! # Why there is no loop here
//!
//! Developer-declared function tools are executed by whoever called this
//! service, not by this service. So a round that asks for tools **ends** — it
//! ends `completed`, carrying the requested calls as output items, and the
//! caller resumes the work by submitting the outputs as the next round. That
//! is the only shape the protocol defines for function tools, and it is why a
//! step ceiling is not a concept this module can have.
//!
//! Hosted tools (the kind a model host executes on the caller's behalf) would
//! loop inside a round, but this service hosts none, so the set is empty and
//! the loop is absent rather than degenerate.
//!
//! # Contracts owed to the orchestrator
//!
//! - **Status is `completed` or `incomplete`.** The orchestrator folds anything
//!   else into `completed`, so returning `failed` here would be silently
//!   mistranslated; genuine failures are returned as [`AgentError::Failed`].
//! - **Items are this round's only.** The task's input history must not be
//!   echoed back, or it would be archived twice.
//! - **A moved fence stops work at once.** Either signal — a sink refusing an
//!   append, or the cancellation probe firing — becomes
//!   [`AgentError::Superseded`].
//! - **A failure still books its tokens.** Whatever was consumed before the
//!   failure rides along on the error.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentTask, CancelProbe, SinkVerdict,
};
use nova_responses::{ContentPart, ItemStatus, ResponseItem, Role, Usage};
use tokio::time::Instant;

use crate::catalog::ModelCatalog;
use crate::compact::{prepare_messages, ContextCache, ModelSummarizer};
use crate::outcome::Termination;
use crate::passthrough::overrides_from_exec_context;
use crate::transport::{HttpChatCompletion, ModelEndpoint};
use crate::wire::{
    ChatCompletion, FinishReason, ModelUsage, RequestMessage, ResponseChunk, ToolCallRequest,
    ToolManifest, TransportOverrides,
};

/// Builds a provider for a resolved endpoint.
///
/// Injectable for one reason: tests script a chunk sequence rather than
/// standing up an HTTP endpoint, so every behaviour below is verifiable
/// without a model.
pub type ProviderFactory = Arc<dyn Fn(ModelEndpoint) -> Arc<dyn ChatCompletion> + Send + Sync>;

/// Reads an environment variable. Injectable so credential handling is
/// testable without mutating process state.
pub type EnvReader = Arc<dyn Fn(&str) -> Option<String> + Send + Sync>;

pub struct SingleTurnExecutor {
    catalog: ModelCatalog,
    provider: ProviderFactory,
    env: EnvReader,
    /// Compressed completion context from earlier responses, keyed by response id.
    context_cache: ContextCache,
    timeouts: StreamTimeouts,
}

/// How long one round's stream may go quiet before it is abandoned.
///
/// The model client itself has no timeout, so without these a gateway's own
/// cut-off (600s at the LiteLLM proxy) ends the round instead — silently, with
/// a stream that simply stops, which once committed half-written tool arguments
/// as a finished round. Abandoning first keeps the decision here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamTimeouts {
    /// From sending the request to the first chunk of the stream.
    pub first_chunk: Duration,
    /// Between two consecutive chunks once the stream has started.
    pub idle: Duration,
    /// The whole round. Kept under the gateway's 600s so we cut, not it.
    pub total: Duration,
}

impl Default for StreamTimeouts {
    /// Sized against observed rounds: first chunk p99 ≈ 7s, longest healthy
    /// round ≈ 310s.
    fn default() -> Self {
        Self {
            first_chunk: Duration::from_secs(90),
            idle: Duration::from_secs(120),
            total: Duration::from_secs(570),
        }
    }
}

impl SingleTurnExecutor {
    pub fn new(catalog: ModelCatalog) -> Self {
        Self {
            catalog,
            provider: Arc::new(|endpoint| Arc::new(HttpChatCompletion::new(endpoint))),
            env: Arc::new(|name| std::env::var(name).ok()),
            context_cache: ContextCache::new(),
            timeouts: StreamTimeouts::default(),
        }
    }

    /// Replace the provider builder (scripted models in tests).
    pub fn with_provider(mut self, provider: ProviderFactory) -> Self {
        self.provider = provider;
        self
    }

    /// Replace the environment reader (credential tests).
    pub fn with_env(mut self, env: EnvReader) -> Self {
        self.env = env;
        self
    }

    /// Replace the stream timeouts.
    pub fn with_stream_timeouts(mut self, timeouts: StreamTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }
}

/// How many times a round is attempted when it produced nothing: a normal
/// finish with no output, or a stream interrupted before any output reached
/// the caller. Two, not unbounded: the point is to survive one bad round, not
/// to spend until the model talks. A round interrupted AFTER output is never
/// retried, since the caller has already seen part of it.
const ROUND_ATTEMPTS: usize = 2;

/// Why a round's stream stopped without the upstream finishing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Interruption {
    FirstChunkTimeout,
    IdleTimeout,
    TotalTimeout,
    /// The stream closed with no finish reason.
    StreamClosed,
}

impl Interruption {
    fn as_str(self) -> &'static str {
        match self {
            Self::FirstChunkTimeout => "first_chunk_timeout",
            Self::IdleTimeout => "idle_timeout",
            Self::TotalTimeout => "total_timeout",
            Self::StreamClosed => "stream_closed",
        }
    }
}

/// Everything accumulated while reading one round's stream.
#[derive(Default)]
struct RoundState {
    text: String,
    calls: Vec<ToolCallRequest>,
    usage: Usage,
    /// Whether the text item has been announced but not yet closed.
    ///
    /// Tracked rather than inferred from `text`: an adapter may or may not send
    /// `TextDone`, and closing the item twice would archive the answer twice.
    message_open: bool,
    /// Whether anything reached the sink. Decides whether an interrupted round
    /// may be retried: a second attempt after output would show it twice.
    emitted: bool,
    interruption: Option<Interruption>,
    /// The thinking streamed this round, so a continuation can carry it.
    reasoning: String,
    /// The upstream reported `tool_calls` but no call survived its parser.
    missing_calls: bool,
}

#[async_trait]
impl AgentRunner for SingleTurnExecutor {
    fn name(&self) -> &str {
        "single-turn"
    }

    async fn run(
        &self,
        task: &AgentTask,
        sink: &mut dyn AgentEventSink,
        cancel: &dyn CancelProbe,
    ) -> Result<AgentOutcome, AgentError> {
        // Nothing has been spent yet, so every failure below this point books
        // whatever `state.usage` holds at the time.
        let endpoint = self
            .catalog
            .resolve(&task.params.model, &|name| (self.env)(name))
            .map_err(|error| failed(error, Usage::default()))?;
        let tools = declared_tools(task);
        let provider = (self.provider)(endpoint);
        // What the host stamped for this round: headers a proxied upstream
        // needs and the endpoint declaration cannot express.
        let transport = overrides_from_exec_context(task.exec_context.as_ref());
        let summarizer = ModelSummarizer::new(provider.as_ref(), &transport);
        let window = self.catalog.window_tokens(&task.params.model);
        let tokenizer = self.catalog.tokenizer(&task.params.model);
        let messages = prepare_messages(
            &self.context_cache,
            task,
            window,
            tokenizer,
            &tools,
            &summarizer,
        )
        .await
        .map_err(|error| failed(error, Usage::default()))?;

        tracing::info!(
            target: "agent_trace",
            phase = "exec.model_request",
            response_id = %task.provenance.response_id,
            messages = messages.len(),
            tools = tools.len(),
        );

        // One retry, and only for a round that gave the caller nothing: one that
        // finished normally yet produced nothing, or one whose stream was cut
        // before any output. A blank final answer used to fail a chain that had
        // already done its tool work — eight productive rounds discarded for one
        // silent turn. Bounded because a model that is deterministically silent
        // is unlikely to answer on the second ask.
        let mut spent = Usage::default();
        let mut attempt = 1;
        loop {
            let (state, termination) = stream_round(
                provider.as_ref(),
                &messages,
                &tools,
                &transport,
                task,
                sink,
                cancel,
                self.timeouts,
                &item_id(task, 1),
            )
            .await?;

            let items = round_items(&state, termination);
            // A completed round with no output at all is a provider anomaly, not
            // an empty answer: reporting it as success would render a blank
            // reply. A truncated round legitimately may carry nothing, so only a
            // `completed` round is retried and then failed.
            let blank = items.is_empty() && termination == Termination::Completed;
            let cut_silent = state.interruption.filter(|_| !state.emitted);
            let lost = lost_tool_call(&state, termination, &tools);
            let lost_silent = lost.filter(|_| !state.emitted);
            if (blank || cut_silent.is_some() || lost_silent.is_some()) && attempt < ROUND_ATTEMPTS
            {
                spent = accumulate(spent, state.usage);
                attempt += 1;
                tracing::warn!(
                    response_id = %task.provenance.response_id,
                    attempt,
                    interruption = cut_silent.map_or("", Interruption::as_str),
                    lost_tool_call = lost_silent.unwrap_or(""),
                    "round produced no usable output; asking once more"
                );
                continue;
            }
            if let Some(cause) = cut_silent {
                let usage = accumulate(spent, state.usage);
                return Err(failed(
                    format!(
                        "model stream interrupted before any output ({})",
                        cause.as_str()
                    ),
                    usage,
                ));
            }
            if blank {
                let usage = accumulate(spent, state.usage);
                return Err(failed("model produced no output items", usage));
            }
            if let Some(cause) = state.interruption {
                tracing::warn!(
                    response_id = %task.provenance.response_id,
                    interruption = cause.as_str(),
                    chars = state.text.len(),
                    "round interrupted after output; reporting it incomplete"
                );
            }

            let mut items = items;
            let mut termination = termination;
            let mut calls = state.calls.len();
            let mut chars = state.text.len();
            let mut usage = accumulate(spent, state.usage);
            if let Some(cause) = lost {
                // The user already saw "I'll use the tool…": re-asking would show
                // it twice, so the round is continued instead — the announcement
                // is kept as said, and the model is told its call went missing.
                let mut followup = messages.clone();
                followup.push(RequestMessage::Assistant {
                    content: state.text.clone(),
                    tool_calls: None,
                    reasoning: (!state.reasoning.is_empty()).then(|| state.reasoning.clone()),
                });
                followup.push(RequestMessage::User {
                    content: TOOL_CALL_NUDGE.to_string(),
                });
                let (second, second_termination) = stream_round(
                    provider.as_ref(),
                    &followup,
                    &tools,
                    &transport,
                    task,
                    sink,
                    cancel,
                    self.timeouts,
                    &item_id(task, 2),
                )
                .await?;
                tracing::warn!(
                    response_id = %task.provenance.response_id,
                    cause,
                    recovered = !second.calls.is_empty(),
                    calls = second.calls.len(),
                    "round announced a tool call that never arrived; continued once"
                );
                // A follow-up that produced nothing leaves the first answer
                // standing as it was.
                if second.emitted {
                    items.extend(round_items(&second, second_termination));
                    termination = second_termination;
                }
                calls += second.calls.len();
                chars += second.text.len();
                usage = accumulate(usage, second.usage);
            } else if announces_a_tool_call(&state, termination, &tools) {
                // Not acted on: without a structural signal, phrasing alone is too
                // weak to override an answer. Logged to size the problem.
                tracing::info!(
                    response_id = %task.provenance.response_id,
                    chars = state.text.len(),
                    "round reads like a tool announcement but called no tool"
                );
            }

            tracing::info!(
                target: "agent_trace",
                phase = "exec.settled",
                response_id = %task.provenance.response_id,
                status = termination.status().as_str(),
                reason = termination
                    .incomplete_reason()
                    .map_or("", |reason| reason.as_str()),
                interruption = state.interruption.map_or("", Interruption::as_str),
                lost_tool_call = lost.unwrap_or(""),
                calls,
                chars,
            );

            return Ok(AgentOutcome {
                items,
                usage,
                termination: termination.report(),
            });
        }
    }
}

/// Said to the model when its round announced a tool call that never arrived.
const TOOL_CALL_NUDGE: &str = "[system notice] Your previous message announced a tool call, \
but no tool call reached the system: it was malformed or lost. Make that tool call now, in the \
proper tool-call format. Do not repeat what you already said.";

/// Tool-call markup that only shows up in text when the server failed to parse
/// it into a call (Hy3, Qwen XML and Hermes-style formats).
const LEAKED_TOOL_MARKUP: &[&str] = &["<tool_call", "<tool_sep", "<arg_key>", "<function="];

/// Why a finished round that offered tools is known to have lost its call:
/// the upstream said it stopped for tools but none survived, or the call's
/// markup leaked into the text. Both are structural, so neither misfires on an
/// ordinary answer.
fn lost_tool_call(
    state: &RoundState,
    termination: Termination,
    tools: &[ToolManifest],
) -> Option<&'static str> {
    if tools.is_empty() || !state.calls.is_empty() || termination != Termination::Completed {
        return None;
    }
    if state.missing_calls {
        return Some("finish_reason_tool_calls");
    }
    LEAKED_TOOL_MARKUP
        .iter()
        .any(|marker| state.text.contains(marker))
        .then_some("leaked_markup")
}

/// A short answer that only announces work, in a round that offered tools and
/// called none.
fn announces_a_tool_call(
    state: &RoundState,
    termination: Termination,
    tools: &[ToolManifest],
) -> bool {
    const PHRASES: &[&str] = &[
        "我来", "让我", "我将", "我先", "I'll ", "I will ", "Let me ",
    ];
    let text = state.text.trim();
    !tools.is_empty()
        && state.calls.is_empty()
        && termination == Termination::Completed
        && !text.is_empty()
        && text.chars().count() <= 200
        && (text.ends_with(['：', ':']) || PHRASES.iter().any(|phrase| text.contains(phrase)))
}

/// One attempt at the round: open the stream, consume it, and close whatever
/// item is still open. Its own function so a round that comes back blank can be
/// asked again without re-deriving the context.
#[allow(clippy::too_many_arguments)]
async fn stream_round(
    provider: &dyn ChatCompletion,
    messages: &[RequestMessage],
    tools: &[ToolManifest],
    transport: &TransportOverrides,
    task: &AgentTask,
    sink: &mut dyn AgentEventSink,
    cancel: &dyn CancelProbe,
    timeouts: StreamTimeouts,
    item_id: &str,
) -> Result<(RoundState, Termination), AgentError> {
    let mut state = RoundState::default();
    let started = Instant::now();
    let deadline = started + timeouts.total;
    let first_deadline = (started + timeouts.first_chunk).min(deadline);
    let cut = |state: &mut RoundState, cause| {
        state.interruption = Some(cause);
        Termination::classify(&FinishReason::Interrupted)
    };

    let opened = tokio::time::timeout_at(
        first_deadline,
        provider.completion(messages, tools, true, transport),
    )
    .await;
    let Ok(opened) = opened else {
        let termination = cut(&mut state, Interruption::FirstChunkTimeout);
        return Ok((state, termination));
    };
    let mut stream = opened.map_err(|error| failed(error, Usage::default()))?;

    let mut termination = None;
    let mut first_chunk = true;
    // Tool calls are announced only once the round is known to have finished:
    // the transport emits them right before `Done`, and an interrupted round's
    // calls may carry truncated arguments that must never be executed.
    let mut pending_calls = Vec::new();

    loop {
        let wait_until = if first_chunk {
            first_deadline
        } else {
            (Instant::now() + timeouts.idle).min(deadline)
        };
        // `biased` so a fence that has already moved wins over a chunk that
        // happens to be ready: the point is to stop spending promptly.
        let next = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(AgentError::Superseded),
            item = tokio::time::timeout_at(wait_until, stream.next()) => item,
        };
        let Ok(next) = next else {
            let cause = if first_chunk {
                Interruption::FirstChunkTimeout
            } else if Instant::now() >= deadline {
                Interruption::TotalTimeout
            } else {
                Interruption::IdleTimeout
            };
            termination = Some(cut(&mut state, cause));
            break;
        };
        let Some(item) = next else { break };
        let chunk = item.map_err(|error| failed(error, state.usage))?;

        if first_chunk {
            first_chunk = false;
            tracing::info!(
                target: "agent_trace",
                phase = "exec.first_chunk",
                response_id = %task.provenance.response_id,
            );
        }

        match chunk {
            ResponseChunk::Think(text) => {
                state.emitted = true;
                state.reasoning.push_str(&text);
                guard(sink.reasoning_text_delta(&text).await)?;
            }
            ResponseChunk::TextBlock(text) => {
                state.emitted = true;
                if !state.message_open {
                    open_message(sink, item_id).await?;
                    state.message_open = true;
                }
                state.text.push_str(&text);
                guard(sink.text_delta(&text).await)?;
            }
            ResponseChunk::ThinkDone => {}
            ResponseChunk::TextDone => {
                if state.message_open {
                    close_message(sink, item_id, &state.text).await?;
                    state.message_open = false;
                }
            }
            ResponseChunk::ToolCall(call) => pending_calls.push(call),
            ResponseChunk::Done { reason, usage } => {
                state.usage = map_usage(usage.as_ref());
                state.missing_calls = reason == FinishReason::MissingToolCalls;
                termination = Some(if reason == FinishReason::Interrupted {
                    cut(&mut state, Interruption::StreamClosed)
                } else {
                    Termination::classify(&reason)
                });
                break;
            }
        }
    }

    // A stream that ended without `Done` was cut off, not finished: treating
    // the silence as a stop once committed half-written tool arguments.
    let termination = termination.unwrap_or_else(|| cut(&mut state, Interruption::StreamClosed));

    // `TextDone` is optional for adapters, so an item may still be open here.
    // Closing it keeps the lifecycle well-formed without risking a second close
    // of one already finished above.
    if state.message_open {
        close_message(sink, item_id, &state.text).await?;
    }

    if state.interruption.is_some() {
        if !pending_calls.is_empty() {
            tracing::warn!(
                response_id = %task.provenance.response_id,
                calls = pending_calls.len(),
                "dropping the tool calls of an interrupted round"
            );
        }
    } else {
        for call in pending_calls {
            state.emitted = true;
            let arguments = arguments_to_string(&call.arguments);
            guard(sink.tool_call(&call.call_id, &call.name, &arguments).await)?;
            state.calls.push(call);
        }
    }

    Ok((state, termination))
}

/// This round's own output, in emission order: the answer, then the calls it
/// asked for.
fn round_items(state: &RoundState, termination: Termination) -> Vec<ResponseItem> {
    let mut items = Vec::new();
    if !state.text.is_empty() {
        items.push(ResponseItem::Message {
            role: Role::Assistant,
            content: vec![ContentPart::OutputText {
                text: state.text.clone(),
            }],
            id: None,
            // A truncated answer is marked as such on the item, so a reader
            // does not have to consult the response status to know the text is
            // cut off.
            status: Some(match termination {
                Termination::Completed => ItemStatus::Completed,
                Termination::Truncated(_) => ItemStatus::Incomplete,
            }),
        });
    }
    for call in &state.calls {
        items.push(ResponseItem::FunctionCall {
            call_id: call.call_id.clone(),
            name: call.name.clone(),
            arguments: arguments_to_string(&call.arguments),
            id: None,
            status: None,
        });
    }
    items
}

/// The tools this round offers, translated from the caller's declaration.
///
/// The schema stays a `Value` the whole way, so a schema this service cannot
/// interpret is still forwarded verbatim rather than dropped.
fn declared_tools(task: &AgentTask) -> Vec<ToolManifest> {
    task.params
        .tools
        .iter()
        .map(|tool| match tool {
            nova_responses::protocol::Tool::Function {
                name,
                description,
                parameters,
                strict,
            } => ToolManifest {
                name: name.clone(),
                description: description.clone().unwrap_or_default(),
                parameters: parameters.clone(),
                strict: *strict,
            },
        })
        .collect()
}

/// Stable within a response, which is all an item id has to be. A round has
/// one text item per pass; only a continued round (see [`TOOL_CALL_NUDGE`])
/// has a second.
fn item_id(task: &AgentTask, pass: usize) -> String {
    if pass <= 1 {
        format!("msg_{}", task.provenance.response_id)
    } else {
        format!("msg_{}_{pass}", task.provenance.response_id)
    }
}

async fn open_message(sink: &mut dyn AgentEventSink, item_id: &str) -> Result<(), AgentError> {
    let opening = ResponseItem::Message {
        role: Role::Assistant,
        content: Vec::new(),
        id: Some(item_id.to_string()),
        status: None,
    };
    guard(sink.output_item_added(&opening).await)?;
    guard(sink.content_part_added(item_id, 0).await)?;
    Ok(())
}

async fn close_message(
    sink: &mut dyn AgentEventSink,
    item_id: &str,
    text: &str,
) -> Result<(), AgentError> {
    guard(sink.output_text_done(text).await)?;
    guard(sink.content_part_done(item_id, 0, text).await)?;
    let done = ResponseItem::Message {
        role: Role::Assistant,
        content: vec![ContentPart::OutputText {
            text: text.to_string(),
        }],
        id: Some(item_id.to_string()),
        status: Some(ItemStatus::Completed),
    };
    guard(sink.output_item_done(&done).await)?;
    Ok(())
}

/// A sink verdict of `Stop` means the fence moved; a sink error is a genuine
/// transport failure. Both end the round, with different meanings.
fn guard(result: Result<SinkVerdict, nova_agent_runtime::SinkError>) -> Result<(), AgentError> {
    match result {
        Ok(SinkVerdict::Continue) => Ok(()),
        Ok(SinkVerdict::Stop) => Err(AgentError::Superseded),
        // Usage is unknown at this point in the borrow; the orchestrator books
        // what the sink already recorded.
        Err(error) => Err(failed(error, Usage::default())),
    }
}

fn map_usage(usage: Option<&ModelUsage>) -> Usage {
    usage
        .map(|usage| Usage::new(usage.prompt_tokens as u64, usage.completion_tokens as u64))
        .unwrap_or_default()
}

/// Tokens spent across every attempt of one round: a round that was asked twice
/// still owes what the first attempt burned.
fn accumulate(spent: Usage, last: Usage) -> Usage {
    Usage::new(
        spent.input_tokens + last.input_tokens,
        spent.output_tokens + last.output_tokens,
    )
}

/// Arguments cross the protocol boundary as an opaque string, because
/// re-encoding them would change what the model produced.
fn arguments_to_string(arguments: &serde_json::Value) -> String {
    match arguments {
        serde_json::Value::String(raw) => raw.clone(),
        serde_json::Value::Null => "{}".to_string(),
        value => serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string()),
    }
}

fn failed(message: impl std::fmt::Display, usage: Usage) -> AgentError {
    AgentError::Failed {
        message: message.to_string(),
        usage,
    }
}

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

use async_trait::async_trait;
use futures::StreamExt;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentTask, CancelProbe, SinkVerdict,
};
use nova_responses::{ContentPart, ItemStatus, ResponseItem, Role, Usage};

use crate::catalog::ModelCatalog;
use crate::compact::{prepare_messages, ContextCache, ModelSummarizer};
use crate::outcome::Termination;
use crate::passthrough::overrides_from_exec_context;
use crate::transport::{HttpChatCompletion, ModelEndpoint};
use crate::wire::{ChatCompletion, ModelUsage, ResponseChunk, ToolCallRequest, ToolManifest};

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
}

impl SingleTurnExecutor {
    pub fn new(catalog: ModelCatalog) -> Self {
        Self {
            catalog,
            provider: Arc::new(|endpoint| Arc::new(HttpChatCompletion::new(endpoint))),
            env: Arc::new(|name| std::env::var(name).ok()),
            context_cache: ContextCache::new(),
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

        let mut stream = provider
            .completion(&messages, &tools, true, &transport)
            .await
            .map_err(|error| failed(error, Usage::default()))?;

        let mut state = RoundState::default();
        let mut termination = None;
        let mut first_chunk = true;

        loop {
            // `biased` so a fence that has already moved wins over a chunk that
            // happens to be ready: the point is to stop spending promptly.
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(AgentError::Superseded),
                item = stream.next() => item,
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
                    guard(sink.reasoning_text_delta(&text).await)?;
                }
                ResponseChunk::TextBlock(text) => {
                    if !state.message_open {
                        open_message(sink, &item_id(task)).await?;
                        state.message_open = true;
                    }
                    state.text.push_str(&text);
                    guard(sink.text_delta(&text).await)?;
                }
                ResponseChunk::ThinkDone => {}
                ResponseChunk::TextDone => {
                    if state.message_open {
                        close_message(sink, &item_id(task), &state.text).await?;
                        state.message_open = false;
                    }
                }
                ResponseChunk::ToolCall(call) => {
                    let arguments = arguments_to_string(&call.arguments);
                    guard(sink.tool_call(&call.call_id, &call.name, &arguments).await)?;
                    state.calls.push(call);
                }
                ResponseChunk::Done { reason, usage } => {
                    state.usage = map_usage(usage.as_ref());
                    termination = Some(Termination::classify(&reason));
                    break;
                }
            }
        }

        // A stream that ended without `Done` still produced a round; treat the
        // silence as a natural stop rather than inventing a failure.
        let termination = termination.unwrap_or(Termination::Completed);

        // `TextDone` is optional for adapters, so an item may still be open
        // here. Closing it keeps the lifecycle well-formed without risking a
        // second close of one already finished above.
        if state.message_open {
            close_message(sink, &item_id(task), &state.text).await?;
            state.message_open = false;
        }

        let items = round_items(&state, termination);
        if items.is_empty() && termination == Termination::Completed {
            // A completed round with no output at all is a provider anomaly,
            // not an empty answer: reporting it as success would render a blank
            // reply. A truncated round legitimately may carry nothing.
            return Err(failed("model produced no output items", state.usage));
        }

        tracing::info!(
            target: "agent_trace",
            phase = "exec.settled",
            response_id = %task.provenance.response_id,
            status = termination.status().as_str(),
            reason = termination
                .incomplete_reason()
                .map_or("", |reason| reason.as_str()),
            calls = state.calls.len(),
            chars = state.text.len(),
        );

        Ok(AgentOutcome {
            items,
            usage: state.usage,
            termination: termination.report(),
        })
    }
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
                ..
            } => ToolManifest {
                name: name.clone(),
                description: description.clone().unwrap_or_default(),
                parameters: parameters.clone(),
            },
        })
        .collect()
}

/// Stable within a response, which is all an item id has to be: this executor
/// produces at most one text item per round.
fn item_id(task: &AgentTask) -> String {
    format!("msg_{}", task.provenance.response_id)
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

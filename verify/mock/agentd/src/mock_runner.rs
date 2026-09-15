//! The mock agent runner: a ReAct loop over a pluggable scheduler + tool executor.
//!
//! This is the first [`AgentRunner`] implementation — the loop the orchestrator
//! deliberately does not contain. It schedules completions and carries out tool
//! calls until the model stops, truncates, refuses, or hits the round ceiling.

use std::sync::Arc;

use async_trait::async_trait;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentTask, CancelProbe, SinkVerdict,
};
use nova_responses::protocol::ProtocolLimits;
use nova_responses::{ResponseItem, ResponseStatus, Usage};

use crate::completions::{
    CompletionsRequest, CompletionsToolChoice, FinishReason, ToolSpec,
};
use crate::scheduler::{validate_outcome, Scheduler, SchedulerError};
use crate::tool::ToolExecutor;

/// Runs a task to a terminal state using completions + tool execution.
pub struct MockAgentRunner {
    scheduler: Arc<dyn Scheduler>,
    tools: Arc<dyn ToolExecutor>,
    /// The bounds our own output must clear, which are the bounds caller input clears:
    /// anything we emit has to be acceptable as input next turn (INV-47), so there is one
    /// set of numbers, not an output-side copy.
    limits: ProtocolLimits,
}

impl MockAgentRunner {
    pub fn new(scheduler: Arc<dyn Scheduler>, tools: Arc<dyn ToolExecutor>) -> Self {
        Self::with_limits(scheduler, tools, ProtocolLimits::default())
    }

    pub fn with_limits(
        scheduler: Arc<dyn Scheduler>,
        tools: Arc<dyn ToolExecutor>,
        limits: ProtocolLimits,
    ) -> Self {
        Self {
            scheduler,
            tools,
            limits,
        }
    }
}

#[async_trait]
impl AgentRunner for MockAgentRunner {
    fn name(&self) -> &str {
        self.scheduler.name()
    }

    async fn run(
        &self,
        task: &AgentTask,
        sink: &mut dyn AgentEventSink,
        cancel: &dyn CancelProbe,
    ) -> Result<AgentOutcome, AgentError> {
        let mut conversation = task.items.clone();
        let base_len = conversation.len();
        let mut usage = Usage::default();
        let mut tool_rounds = 0usize;

        // Provider shape, translated from the caller's inbound declaration.
        let tool_specs: Vec<ToolSpec> =
            task.params.tools.clone().into_iter().map(ToolSpec::from).collect();
        let tool_choice: Option<CompletionsToolChoice> =
            task.params.tool_choice.clone().map(CompletionsToolChoice::from);

        loop {
            let request = match CompletionsRequest::from_context(
                task.params.model.clone(),
                task.params.instructions.as_deref(),
                &conversation,
                task.provenance.clone(),
            )
            .map(|r| r.with_tools(tool_specs.clone()).with_tool_choice(tool_choice.clone()).with_metadata(task.params.metadata.clone()))
            {
                Ok(r) => r,
                Err(e) => {
                    return Err(AgentError::Failed {
                        message: format!("context: {e}"),
                        usage,
                    });
                }
            };

            // The scheduler call is the other blocking stretch where no event append may
            // happen for a long time (a slow stream, or a hung provider). Race it against
            // the active cancellation probe, exactly like a tool call: dropping the
            // schedule future is safe here because we return immediately and never touch
            // the sink again.
            let outcome = match tokio::select! {
                out = self.scheduler.schedule(&request, sink) => out,
                _ = cancel.cancelled() => return Err(AgentError::Superseded),
            } {
                Ok(outcome) => outcome,
                Err(SchedulerError::Superseded) => return Err(AgentError::Superseded),
                Err(e) => {
                    return Err(AgentError::Failed {
                        message: e.to_string(),
                        usage,
                    });
                }
            };

            if let Err(e) = validate_outcome(&outcome, &self.limits) {
                return Err(AgentError::Failed {
                    message: e.to_string(),
                    usage,
                });
            }

            let calls: Vec<(String, String, String)> = outcome
                .items
                .iter()
                .filter_map(|item| match item {
                    ResponseItem::FunctionCall {
                        call_id,
                        name,
                        arguments,
                        ..
                    } => Some((call_id.clone(), name.clone(), arguments.clone())),
                    _ => None,
                })
                .collect();

            let finish = outcome.finish;

            match finish {
                FinishReason::Stop | FinishReason::Refusal => {
                    usage = usage.accumulate(outcome.usage);
                    conversation.extend(outcome.items);
                    return Ok(AgentOutcome {
                        items: conversation[base_len..].to_vec(),
                        usage,
                        status: ResponseStatus::Completed,
                    });
                }

                FinishReason::Length => {
                    usage = usage.accumulate(outcome.usage);
                    conversation.extend(outcome.items);
                    return Ok(AgentOutcome {
                        items: conversation[base_len..].to_vec(),
                        usage,
                        status: ResponseStatus::Incomplete,
                    });
                }

                FinishReason::ToolCalls => {
                    if tool_rounds >= task.max_tool_rounds {
                        usage = usage.accumulate(outcome.usage);
                        return Ok(AgentOutcome {
                            items: conversation[base_len..].to_vec(),
                            usage,
                            status: ResponseStatus::Incomplete,
                        });
                    }
                    tool_rounds += 1;
                    if calls.is_empty() {
                        return Err(AgentError::Failed {
                            message: "finish=ToolCalls but no tool calls were produced"
                                .to_string(),
                            usage,
                        });
                    }
                    usage = usage.accumulate(outcome.usage);
                    conversation.extend(outcome.items);
                    for (call_id, name, arguments) in calls {
                        // A tool call is a blocking await with no event append, so the
                        // passive fence check never fires during it. Race it against the
                        // active cancellation probe so a cancelled/reaped attempt stops
                        // promptly instead of burning the full tool round.
                        let output = tokio::select! {
                            out = self.tools.call(&name, &arguments) => out,
                            _ = cancel.cancelled() => return Err(AgentError::Superseded),
                        };
                        match output {
                            Ok(output) => {
                                let item = ResponseItem::FunctionCallOutput {
                                    call_id,
                                    output,
                                    id: None,
                                    status: None,
                                };
                                match sink.output_item_added(&item).await {
                                    Ok(SinkVerdict::Stop) => return Err(AgentError::Superseded),
                                    Ok(SinkVerdict::Continue) => {}
                                    Err(e) => {
                                        return Err(AgentError::Failed {
                                            message: e.to_string(),
                                            usage,
                                        });
                                    }
                                }
                                match sink.output_item_done(&item).await {
                                    Ok(SinkVerdict::Stop) => return Err(AgentError::Superseded),
                                    Ok(SinkVerdict::Continue) => {}
                                    Err(e) => {
                                        return Err(AgentError::Failed {
                                            message: e.to_string(),
                                            usage,
                                        });
                                    }
                                }
                                conversation.push(item);
                            }
                            Err(e) => {
                                return Err(AgentError::Failed {
                                    message: e.to_string(),
                                    usage,
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

//! The mock agent runner: a ReAct loop over a pluggable scheduler + tool executor.
//!
//! This is the first [`AgentRunner`] implementation — the loop the orchestrator
//! deliberately does not contain. It schedules completions and carries out tool
//! calls until the model stops, truncates, refuses, or hits the round ceiling.

use std::sync::Arc;

use async_trait::async_trait;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentTask, SinkVerdict,
};
use nova_responses_core::{ResponseItem, ResponseStatus, Usage};

use crate::completions::{
    CompletionsRequest, CompletionsToolChoice, FinishReason, ToolSpec,
};
use crate::scheduler::{validate_outcome, Scheduler, SchedulerError};
use crate::tool::ToolExecutor;

/// Runs a task to a terminal state using completions + tool execution.
pub struct MockAgentRunner {
    scheduler: Arc<dyn Scheduler>,
    tools: Arc<dyn ToolExecutor>,
}

impl MockAgentRunner {
    pub fn new(scheduler: Arc<dyn Scheduler>, tools: Arc<dyn ToolExecutor>) -> Self {
        Self { scheduler, tools }
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
    ) -> Result<AgentOutcome, AgentError> {
        let mut conversation = task.items.clone();
        let base_len = conversation.len();
        let mut usage = Usage::default();
        let mut tool_rounds = 0usize;

        // Provider shape, translated from the caller's inbound declaration.
        let tool_specs: Vec<ToolSpec> =
            task.tools.clone().into_iter().map(ToolSpec::from).collect();
        let tool_choice: Option<CompletionsToolChoice> =
            task.tool_choice.clone().map(CompletionsToolChoice::from);

        loop {
            let request = match CompletionsRequest::from_context(
                task.model.clone(),
                task.instructions.as_deref(),
                &conversation,
                task.provenance.clone(),
            )
            .map(|r| r.with_tools(tool_specs.clone()).with_tool_choice(tool_choice.clone()))
            {
                Ok(r) => r,
                Err(e) => {
                    return Err(AgentError::Failed {
                        message: format!("context: {e}"),
                        usage,
                    });
                }
            };

            let outcome = match self.scheduler.schedule(&request, sink).await {
                Ok(outcome) => outcome,
                Err(SchedulerError::Superseded) => return Err(AgentError::Superseded),
                Err(e) => {
                    return Err(AgentError::Failed {
                        message: e.to_string(),
                        usage,
                    });
                }
            };

            if let Err(e) = validate_outcome(&outcome) {
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
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    return Ok(AgentOutcome {
                        items: conversation[base_len..].to_vec(),
                        usage,
                        status: ResponseStatus::Completed,
                    });
                }

                FinishReason::Length => {
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    return Ok(AgentOutcome {
                        items: conversation[base_len..].to_vec(),
                        usage,
                        status: ResponseStatus::Incomplete,
                    });
                }

                FinishReason::ToolCalls => {
                    if tool_rounds >= task.max_tool_rounds {
                        usage = usage.add(outcome.usage);
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
                    usage = usage.add(outcome.usage);
                    conversation.extend(outcome.items);
                    for (call_id, name, arguments) in calls {
                        match self.tools.call(&name, &arguments).await {
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

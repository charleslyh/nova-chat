//! Answers from declared rules. No model, no network, no variance.

use async_trait::async_trait;
use nova_agent_runtime::{AgentEventSink, SinkVerdict};
use nova_responses_core::{ResponseItem, Usage};
use serde::{Deserialize, Serialize};

use crate::completions::{
    assistant_text_message, CompletionsOutcome, CompletionsRequest, FinishReason, ToolCall,
};
use crate::scheduler::{chunk_text, Scheduler, SchedulerError};

/// How a rule matches the incoming task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Match {
    Exact(String),
    Contains(String),
    Prefix(String),
    Any,
}

impl Match {
    fn matches(&self, request: &CompletionsRequest) -> bool {
        let last = request.last_user_text();
        match self {
            Match::Exact(s) => last == s,
            Match::Contains(s) => last.contains(s.as_str()),
            Match::Prefix(s) => last.starts_with(s.as_str()),
            Match::Any => true,
        }
    }
}

/// What to do when a rule matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Script {
    Text {
        text: String,
        #[serde(default = "one")]
        chunks: usize,
    },
    Truncated {
        text: String,
        #[serde(default = "one")]
        chunks: usize,
    },
    Refuse { reason: String },
    ToolCall {
        name: String,
        arguments: String,
        #[serde(default = "default_call_id")]
        call_id: String,
    },
    Fail { message: String },
    Reject { message: String },
    Stall {
        #[serde(default)]
        before: String,
    },
    Empty,
    Hang,
}

fn one() -> usize {
    1
}

fn default_call_id() -> String {
    "call_scripted_1".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScriptRule {
    pub when: Match,
    pub then: Script,
}

/// Answers according to the first matching rule.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScriptedScheduler {
    #[serde(default)]
    rules: Vec<ScriptRule>,
    #[serde(default)]
    default: Option<Script>,
}

impl ScriptedScheduler {
    pub fn from_rules(rules: Vec<ScriptRule>) -> Self {
        Self {
            rules,
            default: None,
        }
    }

    pub fn from_yaml(src: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(src)
    }

    pub fn answering_by_default(mut self, text: impl Into<String>, chunks: usize) -> Self {
        self.default = Some(Script::Text {
            text: text.into(),
            chunks: chunks.max(1),
        });
        self
    }

    pub fn failing_by_default(mut self, message: impl Into<String>) -> Self {
        self.default = Some(Script::Fail {
            message: message.into(),
        });
        self
    }

    pub fn with_rule(mut self, when: Match, then: Script) -> Self {
        self.rules.push(ScriptRule { when, then });
        self
    }

    fn select(&self, request: &CompletionsRequest) -> Option<&Script> {
        self.rules
            .iter()
            .find(|r| r.when.matches(request))
            .map(|r| &r.then)
            .or(self.default.as_ref())
    }

    async fn stream(
        text: &str,
        chunks: usize,
        sink: &mut dyn AgentEventSink,
    ) -> Result<ResponseItem, SchedulerError> {
        let message = assistant_text_message(text);
        let item_id = match &message {
            ResponseItem::Message { id, .. } => id.clone().unwrap_or_default(),
            _ => String::new(),
        };
        sink.output_item_added(&message).await?;
        sink.content_part_added(&item_id, 0).await?;
        for piece in chunk_text(text, chunks) {
            if matches!(sink.text_delta(&piece).await?, SinkVerdict::Stop) {
                return Err(SchedulerError::Superseded);
            }
        }
        sink.output_text_done(text).await?;
        sink.content_part_done(&item_id, 0, text).await?;
        sink.output_item_done(&message).await?;
        Ok(message)
    }

    fn usage(request: &CompletionsRequest, output: &str) -> Usage {
        Usage::new(
            (request.approx_input_chars() as u64 / 4).max(1),
            (output.len() as u64 / 4).max(1),
        )
    }
}

#[async_trait]
impl Scheduler for ScriptedScheduler {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let Some(script) = self.select(request) else {
            return Err(SchedulerError::Other(format!(
                "no scripted rule matched, and no default was set. Last user text: {:?}",
                request.last_user_text()
            )));
        };

        match script {
            Script::Text { text, chunks } => {
                let message = Self::stream(text, *chunks, sink).await?;
                Ok(CompletionsOutcome {
                    items: vec![message],
                    finish: FinishReason::Stop,
                    usage: Self::usage(request, text),
                })
            }

            Script::Truncated { text, chunks } => {
                let message = Self::stream(text, *chunks, sink).await?;
                Ok(CompletionsOutcome {
                    items: vec![message],
                    usage: Self::usage(request, text),
                    finish: FinishReason::Length,
                })
            }

            Script::Refuse { reason } => {
                Ok(CompletionsOutcome::refusal(reason, Self::usage(request, reason)))
            }

            Script::ToolCall {
                name,
                arguments,
                call_id,
            } => {
                let call = ToolCall {
                    id: call_id.clone(),
                    name: name.clone(),
                    arguments: arguments.clone(),
                };
                if matches!(
                    sink.tool_call(&call.id, &call.name, &call.arguments).await?,
                    SinkVerdict::Stop
                ) {
                    return Err(SchedulerError::Superseded);
                }
                Ok(CompletionsOutcome::tool_calls(
                    vec![call],
                    Self::usage(request, arguments),
                ))
            }

            Script::Fail { message } => Err(SchedulerError::Unavailable(message.clone())),

            Script::Reject { message } => Err(SchedulerError::Rejected(message.clone())),

            Script::Stall { before } => {
                if !before.is_empty() {
                    Self::stream(before, 1, sink).await?;
                }
                Err(SchedulerError::DeadlineExceeded)
            }

            Script::Hang => {
                std::future::pending::<()>().await;
                unreachable!("pending() never resolves")
            }

            Script::Empty => Ok(CompletionsOutcome {
                items: vec![],
                usage: Usage::default(),
                finish: FinishReason::Stop,
            }),
        }
    }
}

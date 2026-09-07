//! Answers from declared rules. No model, no network, no variance.
//!
//! This is what makes the abstraction pay for itself. A scenario states what the
//! model says, and the assertion can then be exact:
//!
//! ```yaml
//! rules:
//!   - when: { contains: "weather" }
//!     then: { tool_call: { name: get_weather, arguments: '{"city":"Paris"}' } }
//!   - when: { contains: "secret" }
//!     then: { refuse: "I can't help with that" }
//!   - when: { exact: "fail" }
//!     then: { fail: "provider unavailable" }
//! default: { text: "I don't know", chunks: 2 }
//! ```
//!
//! The failure modes are the valuable part. A refusal, a truncated answer, a stall
//! past the deadline and a malformed tool call are all routine in production and
//! all awkward to provoke against a live model — so the paths that handle them are
//! usually the least tested in a system that only ever talks to a real provider.

use async_trait::async_trait;
use nova_responses_core::{
    assistant_text_message, CompletionsOutcome, CompletionsRequest, CompletionsRequestScheduler,
    CompletionsSink, FinishReason, ResponseItem, SchedulerError, ToolCall, Usage,
};
use serde::{Deserialize, Serialize};

use crate::chunk_text;

/// How a rule matches the incoming task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Match {
    /// The last user text equals this exactly.
    Exact(String),
    /// The last user text contains this substring.
    Contains(String),
    /// The last user text starts with this.
    Prefix(String),
    /// Any task. Useful as a final catch-all rule.
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
    /// Stream `text` in `chunks` pieces and submit it.
    Text {
        text: String,
        #[serde(default = "one")]
        chunks: usize,
    },
    /// Stream `text` but report truncation, so the caller's handling of a partial
    /// answer can be tested. `finish` is `Length`, which is deliberately *not* a
    /// complete answer.
    Truncated { text: String, #[serde(default = "one")] chunks: usize },
    /// Decline. A completed turn, not an error.
    Refuse { reason: String },
    /// Emit a tool call. `arguments` is passed through verbatim, so a deliberately
    /// malformed value can be used to test the caller's parsing.
    ToolCall {
        name: String,
        arguments: String,
        #[serde(default = "default_call_id")]
        call_id: String,
    },
    /// Fail with a retryable error.
    Fail { message: String },
    /// Fail with a non-retryable error, so the two paths can be told apart.
    Reject { message: String },
    /// Stream `before` and then report the deadline exceeded, without completing.
    Stall {
        #[serde(default)]
        before: String,
    },
    /// Return an outcome with no items. Exists to prove the caller refuses it
    /// rather than completing a response with no content.
    Empty,
    /// Never return.
    ///
    /// Models a provider that accepts the request and then hangs indefinitely — the
    /// generation stays in flight and keeps occupying its admission slot.
    ///
    /// Distinct from [`Script::Stall`], which reports the deadline exceeded and so
    /// reaches a terminal state promptly. Both are real failures, and a test that
    /// needs work to *remain* in flight needs this one: with `Stall` the slot is
    /// freed immediately and an overload condition can never be observed.
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
    /// Used when no rule matches. Absent means an unmatched task is an error,
    /// which is usually what a test wants: a silent fallback hides the case where
    /// the input was not what the test thought it was.
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

    /// Parse from YAML, so scenarios can declare model behaviour as data.
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

    /// Stream text as `output_item.added` → `content_part.added` →
    /// `output_text.delta`* → `output_text.done` → `content_part.done` →
    /// `output_item.done`, translating a moved fence into `Superseded`.
    /// Returns the message (with its id) so the outcome carries the same item.
    async fn stream(
        text: &str,
        chunks: usize,
        sink: &mut dyn CompletionsSink,
    ) -> Result<ResponseItem, SchedulerError> {
        let message = assistant_text_message(text);
        let item_id = match &message {
            ResponseItem::Message { id, .. } => id.clone().unwrap_or_default(),
            _ => String::new(),
        };
        sink.output_item_added(&message).await?;
        sink.content_part_added(&item_id, 0).await?;
        for piece in chunk_text(text, chunks) {
            if sink.text_delta(&piece).await?.should_stop() {
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
impl CompletionsRequestScheduler for ScriptedScheduler {
    fn name(&self) -> &str {
        "scripted"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn CompletionsSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let Some(script) = self.select(request) else {
            // Loud on purpose. A test whose input did not match any rule is a test
            // that is not exercising what it claims to.
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
                // Not streamed: a refusal has no incremental form.
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
                if sink.tool_call(&call).await?.should_stop() {
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
                // No tokio dependency needed, and no busy loop: this future is
                // simply never woken.
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

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses_core::{CollectingSink, CompletionsMessage, RequestProvenance};

    fn request_with(text: &str) -> CompletionsRequest {
        CompletionsRequest {
            model: "m".into(),
            messages: vec![CompletionsMessage::user_text(text)],
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

    #[tokio::test]
    async fn the_first_matching_rule_wins() {
        // Order must be significant and stable, or a scenario's behaviour depends
        // on rule iteration order.
        let e = ScriptedScheduler::from_rules(vec![])
            .with_rule(
                Match::Contains("weather".into()),
                Script::Text {
                    text: "sunny".into(),
                    chunks: 1,
                },
            )
            .with_rule(
                Match::Any,
                Script::Text {
                    text: "fallback".into(),
                    chunks: 1,
                },
            );

        let mut sink = CollectingSink::new();
        e.schedule(&request_with("what is the weather"), &mut sink)
            .await
            .expect("execute");
        assert_eq!(sink.streamed(), "sunny");

        let mut sink = CollectingSink::new();
        e.schedule(&request_with("something else"), &mut sink)
            .await
            .expect("execute");
        assert_eq!(sink.streamed(), "fallback");
    }

    #[tokio::test]
    async fn an_unmatched_task_is_an_error_not_a_silent_default() {
        // A test whose input matched nothing is not exercising what it claims to,
        // and a silent fallback would hide that.
        let e = ScriptedScheduler::from_rules(vec![ScriptRule {
            when: Match::Exact("known".into()),
            then: Script::Text {
                text: "ok".into(),
                chunks: 1,
            },
        }]);
        let mut sink = CollectingSink::new();
        let err = e
            .schedule(&request_with("unknown"), &mut sink)
            .await
            .expect_err("must not answer silently");
        assert!(err.to_string().contains("no scripted rule matched"));
        assert!(
            err.to_string().contains("unknown"),
            "the message must show what was actually received"
        );
    }

    #[tokio::test]
    async fn refusals_complete_the_turn() {
        let e = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Refuse {
                reason: "cannot help".into(),
            },
        );
        let mut sink = CollectingSink::new();
        let o = e.schedule(&request_with("q"), &mut sink).await.expect("execute");

        assert_eq!(o.finish, FinishReason::Refusal);
        nova_responses_core::validate_outcome(&o).expect("a refusal must be storable");
        assert!(
            sink.streamed().is_empty(),
            "a refusal has no incremental form"
        );
    }

    #[tokio::test]
    async fn truncation_is_reported_as_incomplete() {
        // The distinction the caller must act on: this succeeded but is partial.
        let e = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Truncated {
                text: "half an ans".into(),
                chunks: 2,
            },
        );
        let mut sink = CollectingSink::new();
        let o = e.schedule(&request_with("q"), &mut sink).await.expect("execute");
        assert_eq!(o.finish, FinishReason::Length);
        assert!(!o.finish.is_complete_answer());
        nova_responses_core::validate_outcome(&o).expect("still storable, just not complete");
    }

    #[tokio::test]
    async fn tool_calls_pass_arguments_through_verbatim() {
        // Including malformed JSON, so the caller's parsing can be tested.
        let raw = r#"{"city": "Paris", broken"#;
        let e = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::ToolCall {
                name: "get_weather".into(),
                arguments: raw.into(),
                call_id: "call_7".into(),
            },
        );
        let mut sink = CollectingSink::new();
        let o = e.schedule(&request_with("weather"), &mut sink).await.expect("execute");

        assert_eq!(o.finish, FinishReason::ToolCalls);
        assert_eq!(sink.calls.len(), 1, "the call must be announced to the sink");
        assert_eq!(sink.calls[0].arguments, raw, "arguments must not be re-encoded");
        nova_responses_core::validate_outcome(&o).expect("a tool call must be storable");
    }

    #[tokio::test]
    async fn retryable_and_permanent_failures_are_distinguishable() {
        let fail = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Fail {
                message: "503".into(),
            },
        );
        let mut sink = CollectingSink::new();
        let e = fail.schedule(&request_with("q"), &mut sink).await.expect_err("fails");
        assert!(e.is_retryable(), "Fail must model a transient cause");

        let reject = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Reject {
                message: "unknown model".into(),
            },
        );
        let mut sink = CollectingSink::new();
        let e = reject.schedule(&request_with("q"), &mut sink).await.expect_err("fails");
        assert!(!e.is_retryable(), "Reject must model a permanent cause");
    }

    #[tokio::test]
    async fn a_stall_streams_then_exceeds_the_deadline() {
        // Reproduces the case where a model starts answering and then hangs — the
        // partial output exists, but the attempt must not complete.
        let e = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Stall {
                before: "starting to ans".into(),
            },
        );
        let mut sink = CollectingSink::new();
        let err = e.schedule(&request_with("q"), &mut sink).await.expect_err("stalls");
        assert!(matches!(err, SchedulerError::DeadlineExceeded));
        assert!(!sink.streamed().is_empty(), "partial output was streamed");
    }

    #[tokio::test]
    async fn the_fence_is_honoured_mid_script() {
        let e = ScriptedScheduler::from_rules(vec![]).with_rule(
            Match::Any,
            Script::Text {
                text: "a long scripted answer".into(),
                chunks: 10,
            },
        );
        let mut sink = CollectingSink::stopping_after(1);
        let err = e.schedule(&request_with("q"), &mut sink).await.expect_err("abandons");
        assert!(matches!(err, SchedulerError::Superseded));
        assert_eq!(sink.deltas.len(), 1);
    }

    #[test]
    fn scripts_load_from_yaml() {
        // So a scenario can declare model behaviour as data rather than code.
        let e = ScriptedScheduler::from_yaml(
            r#"
rules:
  - when: !contains "weather"
    then: !tool_call { name: get_weather, arguments: '{"city":"Paris"}' }
  - when: !exact "fail"
    then: !fail { message: "provider down" }
default: !text { text: "I don't know", chunks: 2 }
"#,
        )
        .expect("parse");
        assert_eq!(e.rules.len(), 2);
        assert!(e.default.is_some());
    }

    #[tokio::test]
    async fn identical_input_yields_identical_output() {
        // The property that makes this usable as a CI oracle.
        let e = ScriptedScheduler::from_rules(vec![]).answering_by_default("stable", 3);
        let mut first = CollectingSink::new();
        let a = e.schedule(&request_with("q"), &mut first).await.expect("execute");
        let mut second = CollectingSink::new();
        let b = e.schedule(&request_with("q"), &mut second).await.expect("execute");

        assert_eq!(a, b);
        assert_eq!(first.deltas, second.deltas, "even the fragmentation must match");
    }
}

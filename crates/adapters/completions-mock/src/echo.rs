//! Echoes the last user message. Preserves the original mock agent's behaviour, so
//! existing scenarios keep working unchanged.

use async_trait::async_trait;
use nova_responses_core::{
    CompletionsOutcome, CompletionsRequest, CompletionsRequestScheduler, CompletionsSink,
    SchedulerError, Usage,
};

use crate::chunk_text;

/// Streams `echo: <last user text>` in `chunks` pieces.
#[derive(Debug, Clone)]
pub struct EchoScheduler {
    chunks: usize,
}

impl EchoScheduler {
    pub fn new(chunks: usize) -> Self {
        Self {
            chunks: chunks.max(1),
        }
    }
}

impl Default for EchoScheduler {
    fn default() -> Self {
        Self::new(8)
    }
}

#[async_trait]
impl CompletionsRequestScheduler for EchoScheduler {
    fn name(&self) -> &str {
        "echo"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn CompletionsSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let last = request.last_user_text();
        let answer = if last.is_empty() {
            "echo: (no user text)".to_string()
        } else {
            format!("echo: {last}")
        };

        for piece in chunk_text(&answer, self.chunks) {
            if sink.text_delta(&piece).await?.should_stop() {
                // Stopping here rather than finishing the stream: the remaining
                // deltas would be billed against an attempt whose output is
                // already discarded.
                return Err(SchedulerError::Superseded);
            }
        }

        let usage = Usage::new(
            request.approx_input_chars() as u64 / 4,
            answer.len() as u64 / 4,
        );
        Ok(CompletionsOutcome::text(answer, usage))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses_core::{CollectingSink, CompletionsMessage, RequestProvenance};

    fn request(messages: Vec<CompletionsMessage>) -> CompletionsRequest {
        CompletionsRequest {
            model: "m".into(),
            messages,
            tools: vec![],
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
    async fn echoes_the_last_user_message() {
        let mut sink = CollectingSink::new();
        let outcome = EchoScheduler::new(3)
            .schedule(&request(vec![CompletionsMessage::user_text("hi there")]), &mut sink)
            .await
            .expect("execute");

        assert_eq!(sink.streamed(), "echo: hi there");
        // Streamed text and the submitted answer agree because this executor
        // chooses to make them agree — not because one is derived from the other.
        assert_eq!(
            outcome.items,
            CompletionsOutcome::text("echo: hi there", Usage::default()).items
        );
    }

    #[tokio::test]
    async fn handles_a_turn_with_no_user_text() {
        // Legal input: a tool-result-only turn. Must not panic or stream nothing.
        let mut sink = CollectingSink::new();
        let outcome = EchoScheduler::new(2)
            .schedule(&request(vec![CompletionsMessage::assistant_text("prior")]), &mut sink)
            .await
            .expect("execute");
        assert!(!sink.streamed().is_empty());
        nova_responses_core::validate_outcome(&outcome).expect("still storable");
    }

    #[tokio::test]
    async fn abandons_the_attempt_when_the_fence_moves() {
        let mut sink = CollectingSink::stopping_after(1);
        let err = EchoScheduler::new(10)
            .schedule(&request(vec![CompletionsMessage::user_text("a longer message")]), &mut sink)
            .await
            .expect_err("must abandon rather than return an outcome");
        assert!(matches!(err, SchedulerError::Superseded));
        assert_eq!(
            sink.deltas.len(),
            1,
            "must stop at the first Stop, not finish the stream"
        );
    }

    #[tokio::test]
    async fn reports_nonzero_usage() {
        // Zero usage for real work makes billing under-count silently.
        let mut sink = CollectingSink::new();
        let outcome = EchoScheduler::new(2)
            .schedule(
                &request(vec![CompletionsMessage::user_text("a reasonably long question")]),
                &mut sink,
            )
            .await
            .expect("execute");
        assert!(outcome.usage.total_tokens > 0);
    }
}

//! Echoes the last user message. Preserves the original mock agent's behaviour, so
//! existing scenarios keep working unchanged.

use async_trait::async_trait;
use nova_responses_core::{
    assistant_text_message, CompletionsOutcome, CompletionsRequest, CompletionsRequestScheduler,
    CompletionsSink, FinishReason, ResponseItem, SchedulerError, Usage,
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

        let message = assistant_text_message(answer.clone());
        let item_id = match &message {
            ResponseItem::Message { id, .. } => id.clone().unwrap_or_default(),
            _ => String::new(),
        };

        sink.output_item_added(&message).await?;
        sink.content_part_added(&item_id, 0).await?;
        for piece in chunk_text(&answer, self.chunks) {
            if sink.text_delta(&piece).await?.should_stop() {
                // Stopping here rather than finishing the stream: the remaining
                // deltas would be billed against an attempt whose output is
                // already discarded.
                return Err(SchedulerError::Superseded);
            }
        }
        sink.output_text_done(&answer).await?;
        sink.content_part_done(&item_id, 0, &answer).await?;
        sink.output_item_done(&message).await?;

        let usage = Usage::new(
            request.approx_input_chars() as u64 / 4,
            answer.len() as u64 / 4,
        );
        Ok(CompletionsOutcome {
            items: vec![message],
            finish: FinishReason::Stop,
            usage,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses_core::{
        CollectingSink, CompletionsMessage, RequestProvenance, ResponseItem,
    };

    fn request(messages: Vec<CompletionsMessage>) -> CompletionsRequest {
        CompletionsRequest {
            model: "m".into(),
            messages,
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
    async fn echoes_the_last_user_message() {
        let mut sink = CollectingSink::new();
        let outcome = EchoScheduler::new(3)
            .schedule(&request(vec![CompletionsMessage::user_text("hi there")]), &mut sink)
            .await
            .expect("execute");

        assert_eq!(sink.streamed(), "echo: hi there");
        // The message is announced as output_item.added/done, with a stable id
        // that the text deltas carry as item_id.
        assert_eq!(sink.added.len(), 1);
        assert_eq!(sink.done.len(), 1);
        assert_eq!(sink.added[0], sink.done[0]);
        assert!(matches!(
            &sink.added[0],
            ResponseItem::Message { id: Some(id), .. } if id.starts_with("msg_")
        ));
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

//! Echoes the last user message.

use async_trait::async_trait;
use nova_agent_runtime::{AgentEventSink, SinkVerdict};
use nova_responses::{ResponseItem, Usage};

use crate::completions::{
    assistant_text_message, CompletionsOutcome, CompletionsRequest, FinishReason,
};
use crate::scheduler::{chunk_text, Scheduler, SchedulerError};

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
impl Scheduler for EchoScheduler {
    fn name(&self) -> &str {
        "echo"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
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
            if matches!(sink.text_delta(&piece).await?, SinkVerdict::Stop) {
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

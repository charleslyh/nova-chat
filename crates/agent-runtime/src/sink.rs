//! The orchestrator's event sink: appends agent events to the event log.

use std::sync::Arc;

use async_trait::async_trait;
use nova_responses::ports::{EventLogError, ResponseEventLog};
use nova_responses::{AppendEvent, Attempt, ContentPart, ItemStatus, ResponseId, ResponseItem};

use crate::runner::{AgentEventSink, SinkError, SinkVerdict};

/// Streams agent events into this response's event log.
///
/// Owns the per-stream state (output index, current item id, content index, accumulated
/// reasoning) and the fencing `attempt`: an append refused as stale flips `stopped`, and
/// every later call returns [`SinkVerdict::Stop`].
pub struct EventSink {
    event_log: Arc<dyn ResponseEventLog>,
    response_id: ResponseId,
    attempt: Attempt,
    stopped: bool,
    /// Index of the output item currently being streamed, mirroring the OpenAI
    /// `response.output_item.*` events' `output_index`.
    output_index: u32,
    /// Id of the output item currently being streamed, carried on delta events as
    /// `item_id`.
    current_item_id: Option<String>,
    /// Index of the content part currently being streamed.
    current_content_index: Option<u32>,
    /// Reasoning / thinking text accumulated across the whole loop, persisted with the
    /// final output (render-only, never re-enters context).
    reasoning: String,
    /// Output items that reached a `done` boundary, in order. Carried so the
    /// orchestrator can archive completed items when a turn ends without a full outcome
    /// (failure / cancellation), rather than losing them.
    completed: Vec<ResponseItem>,
    /// The output item currently being streamed (announced via `added`, not yet
    /// `done`). Kept so a turn that dies mid-stream can still archive the text the
    /// user was reading, as an incomplete message.
    open_item: Option<ResponseItem>,
    /// Text streamed into `open_item` so far.
    open_text: String,
}

impl EventSink {
    pub fn new(
        event_log: Arc<dyn ResponseEventLog>,
        response_id: ResponseId,
        attempt: Attempt,
    ) -> Self {
        Self {
            event_log,
            response_id,
            attempt,
            stopped: false,
            output_index: 0,
            current_item_id: None,
            current_content_index: None,
            reasoning: String::new(),
            completed: Vec::new(),
            open_item: None,
            open_text: String::new(),
        }
    }

    /// Whether the fence moved mid-generation (a later append was refused).
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    /// Reasoning / thinking text accumulated across the whole loop.
    pub fn reasoning(&self) -> &str {
        &self.reasoning
    }

    /// Output items that reached a `done` boundary, in order. This is the partial
    /// result the orchestrator archives when a turn ends without a full outcome
    /// (failure / cancellation): completed tool calls and finished text parts survive.
    pub fn completed(&self) -> &[ResponseItem] {
        &self.completed
    }

    /// The item still being streamed, reconstructed from its accumulated deltas as
    /// a message with `ItemStatus::Incomplete`, when it carries any text. This is
    /// what a turn that dies mid-stream archives: the tokens the user was reading
    /// are kept, rather than vanishing on refresh.
    pub fn partial_item(&self) -> Option<ResponseItem> {
        let item = self.open_item.as_ref()?;
        let ResponseItem::Message { role, id, .. } = item else {
            return None;
        };
        if self.open_text.is_empty() {
            return None;
        }
        Some(ResponseItem::Message {
            role: role.clone(),
            content: vec![ContentPart::OutputText {
                text: self.open_text.clone(),
            }],
            id: id.clone(),
            status: Some(ItemStatus::Incomplete),
        })
    }

    fn id(&self) -> ResponseId {
        self.response_id.clone()
    }

    /// The index of the item currently open. `output_index` counts items *added*, so
    /// the open one is the previous number.
    fn current_output_index(&self) -> u32 {
        self.output_index.saturating_sub(1)
    }

    fn current_item(&self) -> String {
        self.current_item_id.clone().unwrap_or_default()
    }

    /// Append one already-well-formed event.
    ///
    /// Every caller below builds its event through an [`AppendEvent`] constructor, so
    /// there is no path here that can pair an event name with a body that does not
    /// belong to it.
    async fn push(&mut self, event: AppendEvent) -> Result<SinkVerdict, SinkError> {
        if self.stopped {
            return Ok(SinkVerdict::Stop);
        }
        match self.event_log.append(event).await {
            Ok(_) => Ok(SinkVerdict::Continue),
            Err(EventLogError::StaleAttempt) => {
                self.stopped = true;
                Ok(SinkVerdict::Stop)
            }
            Err(e) => Err(SinkError::Transport(e.to_string())),
        }
    }
}

#[async_trait]
impl AgentEventSink for EventSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        // Recorded even if the append below is refused as stale: those tokens were
        // already streamed to the user, and `partial_item` is what archives them when
        // the turn ends without a full outcome.
        self.open_text.push_str(text);
        let event = AppendEvent::text_delta(
            self.id(),
            self.attempt,
            self.current_item(),
            self.current_output_index(),
            self.current_content_index.unwrap_or_default(),
            text,
        );
        self.push(event).await
    }

    async fn reasoning_text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.reasoning.push_str(text);
        let event = AppendEvent::reasoning_text_delta(self.id(), self.attempt, text);
        self.push(event).await
    }

    async fn output_item_added(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        let index = self.output_index;
        self.output_index += 1;
        self.current_item_id = Some(item.stream_item_id().to_string());
        self.open_item = Some(item.clone());
        self.open_text.clear();
        let event = AppendEvent::item(self.id(), self.attempt, false, index, item.clone());
        self.push(event).await
    }

    async fn output_item_done(&mut self, item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        // A `done` event carries the item's complete content; remember it so the
        // orchestrator can archive completed items even when the turn ends without a
        // full outcome (failure / cancellation).
        self.completed.push(item.clone());
        self.open_item = None;
        self.open_text.clear();
        let event = AppendEvent::item(
            self.id(),
            self.attempt,
            true,
            self.current_output_index(),
            item.clone(),
        );
        self.push(event).await
    }

    async fn function_call_arguments_delta(
        &mut self,
        item_id: &str,
        delta: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::arguments_delta(
            self.id(),
            self.attempt,
            item_id,
            self.current_output_index(),
            delta,
        );
        self.push(event).await
    }

    async fn function_call_arguments_done(
        &mut self,
        item_id: &str,
        arguments: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::arguments(
            self.id(),
            self.attempt,
            self.current_output_index(),
            item_id,
            arguments,
        );
        self.push(event).await
    }

    async fn content_part_added(
        &mut self,
        item_id: &str,
        content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        self.current_content_index = Some(content_index);
        let event = AppendEvent::content_part(
            self.id(),
            self.attempt,
            false,
            item_id,
            self.current_output_index(),
            content_index,
            ContentPart::OutputText {
                text: String::new(),
            },
        );
        self.push(event).await
    }

    async fn output_text_done(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::output_text_done(
            self.id(),
            self.attempt,
            self.current_item(),
            self.current_output_index(),
            self.current_content_index.unwrap_or_default(),
            text,
        );
        self.push(event).await
    }

    async fn content_part_done(
        &mut self,
        item_id: &str,
        content_index: u32,
        text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        let event = AppendEvent::content_part(
            self.id(),
            self.attempt,
            true,
            item_id,
            self.current_output_index(),
            content_index,
            ContentPart::OutputText {
                text: text.to_string(),
            },
        );
        self.push(event).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nova_responses::ports::EventLogError;
    use nova_responses::{ResponseEvent, Role};
    use std::sync::Mutex;

    fn id() -> ResponseId {
        ResponseId::new(nova_responses::NodeTag::parse("n1").unwrap())
    }

    /// Accepting in-memory log: enough of the port to drive the sink.
    struct Log {
        events: Mutex<Vec<ResponseEvent>>,
    }

    impl Log {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl ResponseEventLog for Log {
        async fn append(&self, event: AppendEvent) -> Result<u64, EventLogError> {
            let mut g = self.events.lock().unwrap();
            let seq = g.len() as u64;
            g.push(ResponseEvent::from_parts(seq, event));
            Ok(seq)
        }

        async fn read_after(
            &self,
            _response_id: &ResponseId,
            _starting_after: Option<u64>,
            _limit: usize,
            _wait: std::time::Duration,
        ) -> Result<Vec<ResponseEvent>, EventLogError> {
            Ok(self.events.lock().unwrap().clone())
        }

        async fn close(
            &self,
            _response_id: &ResponseId,
            _now_ms: u64,
            _retain: std::time::Duration,
        ) -> Result<(), EventLogError> {
            Ok(())
        }

        async fn sweep_expired(&self, _now_ms: u64) -> Result<u64, EventLogError> {
            Ok(0)
        }

        async fn remove(&self, _response_id: &ResponseId) -> Result<(), EventLogError> {
            Ok(())
        }
    }

    fn sink() -> EventSink {
        EventSink::new(std::sync::Arc::new(Log::new()), id(), Attempt(1))
    }

    async fn open_message(sink: &mut EventSink) {
        sink.output_item_added(&ResponseItem::Message {
            role: Role::Assistant,
            content: vec![],
            id: Some("m0".into()),
            status: None,
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn no_partial_before_any_item_is_streamed() {
        let sink = sink();
        assert!(sink.partial_item().is_none());
    }

    #[tokio::test]
    async fn an_open_item_without_text_has_no_partial() {
        let mut sink = sink();
        open_message(&mut sink).await;
        assert!(sink.partial_item().is_none());
    }

    #[tokio::test]
    async fn streamed_text_comes_back_as_an_incomplete_message() {
        let mut sink = sink();
        open_message(&mut sink).await;
        sink.text_delta("half ").await.unwrap();
        sink.text_delta("way").await.unwrap();
        match sink.partial_item() {
            Some(ResponseItem::Message { status, content, .. }) => {
                assert_eq!(status, Some(ItemStatus::Incomplete));
                assert_eq!(
                    content,
                    vec![ContentPart::OutputText {
                        text: "half way".into()
                    }]
                );
            }
            other => panic!("expected an incomplete message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn done_clears_the_partial() {
        let mut sink = sink();
        open_message(&mut sink).await;
        sink.text_delta("full").await.unwrap();
        let item = ResponseItem::assistant_text("full");
        sink.output_item_done(&item).await.unwrap();
        assert!(sink.partial_item().is_none());
        assert_eq!(sink.completed(), &[item]);
    }

    #[tokio::test]
    async fn a_non_message_open_item_has_no_partial() {
        let mut sink = sink();
        sink.output_item_added(&ResponseItem::FunctionCall {
            call_id: "call_1".into(),
            name: "search".into(),
            arguments: String::new(),
            id: None,
            status: None,
        })
        .await
        .unwrap();
        sink.function_call_arguments_delta("call_1", "{\"q\":")
            .await
            .unwrap();
        assert!(sink.partial_item().is_none());
    }
}

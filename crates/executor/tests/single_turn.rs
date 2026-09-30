//! The executor's own contracts, driven by a scripted model.
//!
//! No HTTP and no real provider: the runner takes its provider through a
//! factory, so a fixed chunk sequence is enough to pin down every behaviour the
//! orchestrator depends on — the four terminal states, the item lifecycle, the
//! "this round only" rule, and both stop signals.

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use futures::Stream;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentRunner, AgentTask, AgentTermination, CancelProbe, SinkError,
    SinkVerdict,
};
use nova_executor::{
    ChatCompletion, ChunkStream, FinishReason, ModelCatalog, ModelEntry, ModelError, ModelUsage,
    RequestMessage, ResponseChunk, SingleTurnExecutor, ToolCallRequest, ToolManifest,
    TransportOverrides,
};
use nova_responses::{
    IncompleteReason, ItemStatus, ModelParams, NodeTag, RequestProvenance, ResponseId,
    ResponseItem, ResponseStatus, Role,
};

// --- fixtures ---------------------------------------------------------------

/// Replays a fixed chunk sequence and records what it was asked.
struct ScriptedModel {
    chunks: Vec<ResponseChunk>,
    seen: Arc<Mutex<Vec<RequestMessage>>>,
    seen_tools: Arc<Mutex<Vec<ToolManifest>>>,
    /// What this round was asked to carry to the upstream.
    seen_transport: Arc<Mutex<TransportOverrides>>,
}

impl ScriptedModel {
    fn new(chunks: Vec<ResponseChunk>) -> Self {
        Self {
            chunks,
            seen: Arc::new(Mutex::new(Vec::new())),
            seen_tools: Arc::new(Mutex::new(Vec::new())),
            seen_transport: Arc::new(Mutex::new(TransportOverrides::new())),
        }
    }
}

#[async_trait]
impl ChatCompletion for ScriptedModel {
    async fn completion(
        &self,
        messages: &[RequestMessage],
        tools: &[ToolManifest],
        _stream: bool,
        transport: &TransportOverrides,
    ) -> Result<ChunkStream, ModelError> {
        *self.seen_transport.lock().expect("lock") = transport.clone();
        *self.seen.lock().expect("lock") = messages.to_vec();
        *self.seen_tools.lock().expect("lock") = tools.to_vec();
        let chunks: Vec<_> = self.chunks.iter().cloned().map(Ok).collect();
        let stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(futures::stream::iter(chunks));
        Ok(stream)
    }
}

/// Answers every call from its own script, so a round that is asked twice can
/// answer differently the second time. Calls past the script stay silent, which
/// is what a model that keeps producing nothing looks like.
struct SequencedModel {
    rounds: Vec<Vec<ResponseChunk>>,
    calls: AtomicUsize,
}

impl SequencedModel {
    fn new(rounds: Vec<Vec<ResponseChunk>>) -> Self {
        Self {
            rounds,
            calls: AtomicUsize::new(0),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl ChatCompletion for SequencedModel {
    async fn completion(
        &self,
        _messages: &[RequestMessage],
        _tools: &[ToolManifest],
        _stream: bool,
        _transport: &TransportOverrides,
    ) -> Result<ChunkStream, ModelError> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        let chunks: Vec<_> = self
            .rounds
            .get(index)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Ok)
            .collect();
        let stream: Pin<Box<dyn Stream<Item = _> + Send>> = Box::pin(futures::stream::iter(chunks));
        Ok(stream)
    }
}

/// A model that refuses to connect at all.
struct BrokenModel;

#[async_trait]
impl ChatCompletion for BrokenModel {
    async fn completion(
        &self,
        _messages: &[RequestMessage],
        _tools: &[ToolManifest],
        _stream: bool,
        _transport: &TransportOverrides,
    ) -> Result<ChunkStream, ModelError> {
        Err(ModelError::new("connection refused"))
    }
}

#[derive(Default, Clone)]
struct SinkLog {
    calls: Vec<String>,
    text: String,
    reasoning: String,
}

/// Records the event sequence; can be told to refuse appends from the start.
struct RecordingSink {
    log: Arc<Mutex<SinkLog>>,
    verdict: SinkVerdict,
}

impl RecordingSink {
    fn new(verdict: SinkVerdict) -> (Self, Arc<Mutex<SinkLog>>) {
        let log = Arc::new(Mutex::new(SinkLog::default()));
        (
            Self {
                log: Arc::clone(&log),
                verdict,
            },
            log,
        )
    }

    fn note(&self, what: impl Into<String>) -> Result<SinkVerdict, SinkError> {
        self.log.lock().expect("lock").calls.push(what.into());
        Ok(self.verdict)
    }
}

#[async_trait]
impl AgentEventSink for RecordingSink {
    async fn text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.log.lock().expect("lock").text.push_str(text);
        self.note("text_delta")
    }

    async fn reasoning_text_delta(&mut self, text: &str) -> Result<SinkVerdict, SinkError> {
        self.log.lock().expect("lock").reasoning.push_str(text);
        self.note("reasoning_text_delta")
    }

    async fn output_item_added(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        self.note("output_item_added")
    }

    async fn output_item_done(&mut self, _item: &ResponseItem) -> Result<SinkVerdict, SinkError> {
        self.note("output_item_done")
    }

    async fn content_part_added(
        &mut self,
        _item_id: &str,
        _content_index: u32,
    ) -> Result<SinkVerdict, SinkError> {
        self.note("content_part_added")
    }

    async fn output_text_done(&mut self, _text: &str) -> Result<SinkVerdict, SinkError> {
        self.note("output_text_done")
    }

    async fn content_part_done(
        &mut self,
        _item_id: &str,
        _content_index: u32,
        _text: &str,
    ) -> Result<SinkVerdict, SinkError> {
        self.note("content_part_done")
    }
}

struct NeverCancel;

#[async_trait]
impl CancelProbe for NeverCancel {
    async fn cancelled(&self) {
        // Resolves never, which is the contract: the probe only completes when
        // the attempt has actually been superseded.
        std::future::pending::<()>().await
    }
}

struct AlreadyCancelled;

#[async_trait]
impl CancelProbe for AlreadyCancelled {
    async fn cancelled(&self) {}
}

fn catalog() -> ModelCatalog {
    ModelCatalog::new([ModelEntry {
        id: "house".to_string(),
        base_url: "http://model.internal/v1".to_string(),
        api_model: "upstream".to_string(),
        api_key_env: None,
        extensions: serde_json::Value::Null,
        window_size: None,
    }])
}

fn task(items: Vec<ResponseItem>, tools: Vec<nova_responses::protocol::Tool>) -> AgentTask {
    task_with_context(items, tools, None)
}

fn task_with_context(
    items: Vec<ResponseItem>,
    tools: Vec<nova_responses::protocol::Tool>,
    exec_context: Option<serde_json::Value>,
) -> AgentTask {
    let mut params = ModelParams::new("house");
    params.tools = tools;
    params.instructions = Some("be brief".to_string());
    AgentTask {
        params,
        exec_context,
        items,
        inherited_len: 0,
        previous_response_id: None,
        conversation_id: None,
        provenance: RequestProvenance {
            response_id: ResponseId::new(NodeTag::parse("test").expect("valid tag")),
            attempt: nova_responses::Attempt::UNCLAIMED.next(),
            exec_deadline_ms: u64::MAX,
        },
        max_tool_rounds: 8,
    }
}

fn executor(model: Arc<dyn ChatCompletion>) -> SingleTurnExecutor {
    SingleTurnExecutor::new(catalog()).with_provider(Arc::new(move |_| Arc::clone(&model)))
}

fn done(reason: FinishReason) -> ResponseChunk {
    ResponseChunk::Done {
        reason,
        usage: Some(ModelUsage {
            prompt_tokens: 10,
            completion_tokens: 4,
            total_tokens: 14,
            cached_tokens: 0,
            reasoning_tokens: 0,
        }),
    }
}

// --- the four terminal states ----------------------------------------------

#[tokio::test]
async fn a_plain_answer_completes_with_one_message_item() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("he".into()),
        ResponseChunk::TextBlock("llo".into()),
        ResponseChunk::TextDone,
        done(FinishReason::Stop),
    ]));
    let (mut sink, log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("hi")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(outcome.termination.status(), ResponseStatus::Completed);
    assert_eq!(outcome.items.len(), 1);
    assert_eq!(log.lock().expect("lock").text, "hello");
    assert_eq!(outcome.usage.input_tokens, 10);
    assert_eq!(outcome.usage.output_tokens, 4);
}

#[tokio::test]
async fn a_round_that_asks_for_tools_completes_rather_than_reporting_incomplete() {
    // The correction at the heart of the four-state split: a pending call is
    // output, so the round succeeded. Reporting `incomplete` here is what made
    // a broken provider indistinguishable from ordinary progress.
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::ToolCall(ToolCallRequest {
            call_id: "c1".into(),
            name: "draw".into(),
            arguments: serde_json::json!({"shape": "circle"}),
        }),
        done(FinishReason::Stop),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("draw")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(
        outcome.termination.status(),
        ResponseStatus::Completed,
        "a pending tool call is not an unfinished response"
    );
    match &outcome.items[0] {
        ResponseItem::FunctionCall {
            call_id,
            name,
            arguments,
            ..
        } => {
            assert_eq!(call_id, "c1");
            assert_eq!(name, "draw");
            assert_eq!(arguments, r#"{"shape":"circle"}"#);
        }
        other => panic!("expected a function call, got {other:?}"),
    }
}

#[tokio::test]
async fn a_truncated_answer_is_incomplete_and_keeps_its_text() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("half an ans".into()),
        done(FinishReason::Length),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("long")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("returns an outcome, not an error");

    assert_eq!(
        outcome.termination,
        AgentTermination::Incomplete(IncompleteReason::MaxOutputTokens),
        "a budget overrun must name the official reason, not just a status"
    );
    match &outcome.items[0] {
        ResponseItem::Message {
            status, content, ..
        } => {
            assert_eq!(
                *status,
                Some(ItemStatus::Incomplete),
                "the item says it was cut off, so a reader need not consult the status"
            );
            assert!(!content.is_empty(), "partial text must survive");
        }
        other => panic!("expected a message, got {other:?}"),
    }
}

#[tokio::test]
async fn a_filtered_round_is_incomplete() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("x".into()),
        done(FinishReason::Refusal { reason: None }),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("returns an outcome");

    assert_eq!(
        outcome.termination,
        AgentTermination::Incomplete(IncompleteReason::ContentFilter),
        "a filtered round is content_filter, the other official reason"
    );
}

#[tokio::test]
async fn a_spoken_refusal_completes() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("I will not".into()),
        done(FinishReason::Refusal {
            reason: Some("policy".into()),
        }),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("returns an outcome");

    assert_eq!(outcome.termination.status(), ResponseStatus::Completed);
}

#[tokio::test]
async fn an_upstream_failure_is_reported_as_a_failure() {
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);
    let error = executor(Arc::new(BrokenModel))
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect_err("a dead provider is a failure, never a quiet completion");

    match error {
        AgentError::Failed { message, .. } => assert!(message.contains("connection refused")),
        other => panic!("expected a failure, got {other:?}"),
    }
}

// --- contracts owed to the orchestrator -------------------------------------

#[tokio::test]
async fn the_outcome_carries_only_this_rounds_items() {
    // Echoing the input history back would archive it a second time.
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("new".into()),
        done(FinishReason::Stop),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let history = vec![
        ResponseItem::user_text("older question"),
        ResponseItem::assistant_text("older answer"),
        ResponseItem::user_text("current question"),
    ];
    let outcome = executor(model)
        .run(&task(history, vec![]), &mut sink, &NeverCancel)
        .await
        .expect("completes");

    assert_eq!(
        outcome.items.len(),
        1,
        "three input items must not reappear in the output"
    );
}

#[tokio::test]
async fn a_moved_fence_supersedes_the_round() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("partial".into()),
        done(FinishReason::Stop),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Stop);

    let error = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect_err("a refused append means this attempt is void");

    assert!(matches!(error, AgentError::Superseded));
}

#[tokio::test]
async fn an_already_cancelled_attempt_stops_before_spending() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("should never stream".into()),
        done(FinishReason::Stop),
    ]));
    let (mut sink, log) = RecordingSink::new(SinkVerdict::Continue);

    let error = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &AlreadyCancelled,
        )
        .await
        .expect_err("a cancelled attempt must not keep going");

    assert!(matches!(error, AgentError::Superseded));
    assert!(
        log.lock().expect("lock").text.is_empty(),
        "the probe is polled first, so no token reaches the stream"
    );
}

#[tokio::test]
async fn an_empty_completed_round_is_a_failure_not_a_blank_answer() {
    let model = Arc::new(ScriptedModel::new(vec![done(FinishReason::Stop)]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let error = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect_err("a completed round with no output is a provider anomaly");

    assert!(matches!(error, AgentError::Failed { .. }));
}

#[tokio::test]
async fn a_blank_round_is_asked_once_more_and_the_second_answer_is_kept() {
    // What the chain could not survive before: one silent turn discarded the
    // rounds of tool work behind it.
    let model = Arc::new(SequencedModel::new(vec![
        vec![done(FinishReason::Stop)],
        vec![
            ResponseChunk::TextBlock("answer".into()),
            done(FinishReason::Stop),
        ],
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model.clone())
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("the second attempt answers");

    assert_eq!(model.calls(), 2, "one retry, no more");
    assert_eq!(outcome.items.len(), 1, "the answer is this round's output");
    assert_eq!(
        outcome.usage.input_tokens, 20,
        "both attempts' tokens are owed, not just the last"
    );
}

#[tokio::test]
async fn a_round_that_stays_blank_is_failed_after_exactly_one_retry() {
    let model = Arc::new(SequencedModel::new(vec![
        vec![done(FinishReason::Stop)],
        vec![done(FinishReason::Stop)],
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let error = executor(model.clone())
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect_err("a model that never answers is still a failure");

    assert_eq!(model.calls(), 2, "the retry is bounded, not unbounded");
    match error {
        AgentError::Failed { message, usage } => {
            assert!(message.contains("no output items"));
            assert_eq!(usage.input_tokens, 20);
        }
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[tokio::test]
async fn a_truncated_round_with_no_text_is_still_an_outcome() {
    // Unlike the case above: the budget ran out, which is honest information,
    // not an anomaly to fail on.
    let model = Arc::new(ScriptedModel::new(vec![done(FinishReason::Length)]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("truncation is a valid terminal state");

    assert_eq!(outcome.termination.status(), ResponseStatus::Incomplete);
    assert!(outcome.items.is_empty());
}

// --- streaming shape --------------------------------------------------------

#[tokio::test]
async fn text_streams_through_a_well_formed_item_lifecycle() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("a".into()),
        ResponseChunk::TextBlock("b".into()),
        ResponseChunk::TextDone,
        done(FinishReason::Stop),
    ]));
    let (mut sink, log) = RecordingSink::new(SinkVerdict::Continue);

    executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(
        log.lock().expect("lock").calls,
        vec![
            "output_item_added",
            "content_part_added",
            "text_delta",
            "text_delta",
            "output_text_done",
            "content_part_done",
            "output_item_done",
        ],
        "an item must be opened before its deltas and closed after them"
    );
}

#[tokio::test]
async fn an_omitted_text_done_still_closes_the_item_exactly_once() {
    // `TextDone` is adapter-optional, so the item must be closed on the way
    // out — but only if it is still open. Closing twice would archive the
    // answer twice, which is what the paired assertion below guards.
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("no terminator".into()),
        done(FinishReason::Stop),
    ]));
    let (mut sink, log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    let calls = log.lock().expect("lock").calls.clone();
    assert_eq!(
        calls
            .iter()
            .filter(|call| *call == "output_item_added")
            .count(),
        1
    );
    assert_eq!(
        calls
            .iter()
            .filter(|call| *call == "output_item_done")
            .count(),
        1,
        "the item is closed once, whether or not the adapter sent TextDone"
    );
    assert_eq!(outcome.items.len(), 1);
}

#[tokio::test]
async fn reasoning_is_streamed_but_is_not_an_output_item() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::Think("pondering".into()),
        ResponseChunk::ThinkDone,
        ResponseChunk::TextBlock("answer".into()),
        ResponseChunk::TextDone,
        done(FinishReason::Stop),
    ]));
    let (mut sink, log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(log.lock().expect("lock").reasoning, "pondering");
    assert_eq!(
        outcome.items.len(),
        1,
        "reasoning is render-only and never becomes an item"
    );
}

#[tokio::test]
async fn a_stream_that_ends_without_done_is_treated_as_a_stop() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("truncated stream".into()),
        ResponseChunk::TextDone,
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("q")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("a missing terminator is not a failure");

    assert_eq!(outcome.termination.status(), ResponseStatus::Completed);
}

// --- what reaches the model -------------------------------------------------

#[tokio::test]
async fn instructions_lead_the_context_and_tools_are_declared() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("ok".into()),
        done(FinishReason::Stop),
    ]));
    let seen = Arc::clone(&model.seen);
    let seen_tools = Arc::clone(&model.seen_tools);
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let schema = serde_json::json!({"type": "object"});
    let tools = vec![nova_responses::protocol::Tool::Function {
        name: "draw".to_string(),
        description: Some("draws".to_string()),
        parameters: schema.clone(),
        strict: None,
    }];

    executor(model)
        .run(
            &task(vec![ResponseItem::user_text("hi")], tools),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    let messages = seen.lock().expect("lock").clone();
    assert_eq!(
        messages[0],
        RequestMessage::System {
            content: "be brief".to_string()
        },
        "the caller's instructions are the only prompt source"
    );
    assert_eq!(
        messages[1],
        RequestMessage::User {
            content: "hi".to_string()
        }
    );

    let declared = seen_tools.lock().expect("lock").clone();
    assert_eq!(declared.len(), 1);
    assert_eq!(declared[0].name, "draw");
    assert_eq!(
        declared[0].parameters, schema,
        "the schema passes through untouched"
    );
}

#[tokio::test]
async fn an_unknown_model_fails_before_any_provider_call() {
    let model = Arc::new(ScriptedModel::new(vec![done(FinishReason::Stop)]));
    let seen = Arc::clone(&model.seen);
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let mut bad = task(vec![ResponseItem::user_text("q")], vec![]);
    bad.params.model = "not-configured".to_string();

    let error = executor(model)
        .run(&bad, &mut sink, &NeverCancel)
        .await
        .expect_err("an unconfigured model is a failure");

    assert!(matches!(error, AgentError::Failed { .. }));
    assert!(
        seen.lock().expect("lock").is_empty(),
        "resolution happens before the provider is built"
    );
}

#[tokio::test]
async fn a_dangling_tool_output_fails_the_round_with_a_named_cause() {
    let model = Arc::new(ScriptedModel::new(vec![done(FinishReason::Stop)]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let items = vec![
        ResponseItem::user_text("q"),
        ResponseItem::FunctionCallOutput {
            call_id: "ghost".to_string(),
            output: "done".to_string(),
            id: None,
            status: None,
        },
    ];
    let error = executor(model)
        .run(&task(items, vec![]), &mut sink, &NeverCancel)
        .await
        .expect_err("an output with no call cannot be sent to a provider");

    match error {
        AgentError::Failed { message, .. } => assert!(
            message.contains("ghost"),
            "the cause names the offending call: {message}"
        ),
        other => panic!("expected a failure, got {other:?}"),
    }
}

#[tokio::test]
async fn parallel_calls_all_reach_the_outcome() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::ToolCall(ToolCallRequest {
            call_id: "c1".into(),
            name: "a".into(),
            arguments: serde_json::json!({}),
        }),
        ResponseChunk::ToolCall(ToolCallRequest {
            call_id: "c2".into(),
            name: "b".into(),
            arguments: serde_json::json!({}),
        }),
        done(FinishReason::Stop),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("both")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(outcome.items.len(), 2);
    assert!(outcome
        .items
        .iter()
        .all(|item| matches!(item, ResponseItem::FunctionCall { .. })));
}

#[tokio::test]
async fn text_and_calls_in_one_round_are_both_output_answer_first() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("let me draw".into()),
        ResponseChunk::TextDone,
        ResponseChunk::ToolCall(ToolCallRequest {
            call_id: "c1".into(),
            name: "draw".into(),
            arguments: serde_json::json!({}),
        }),
        done(FinishReason::Stop),
    ]));
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let outcome = executor(model)
        .run(
            &task(vec![ResponseItem::user_text("draw")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert_eq!(outcome.items.len(), 2);
    assert!(matches!(
        outcome.items[0],
        ResponseItem::Message {
            role: Role::Assistant,
            ..
        }
    ));
    assert!(matches!(
        outcome.items[1],
        ResponseItem::FunctionCall { .. }
    ));
}

/// A proxied upstream needs values the endpoint declaration cannot express. The
/// host stamps them on the record; the runner's job is only to carry them
/// through — what they MEAN is none of this crate's business.
#[tokio::test]
async fn a_round_carries_the_host_stamped_headers() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("hi".into()),
        ResponseChunk::TextDone,
        done(FinishReason::Stop),
    ]));
    let transport = model.seen_transport.clone();
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    let task = task_with_context(
        vec![ResponseItem::user_text("hi")],
        vec![],
        Some(serde_json::json!({
            "llm_transport": { "headers": { "x-litellm-customer-id": "alice" } }
        })),
    );
    executor(model)
        .run(&task, &mut sink, &NeverCancel)
        .await
        .expect("completes");

    assert_eq!(
        transport
            .lock()
            .expect("lock")
            .headers()
            .collect::<Vec<_>>(),
        vec![("x-litellm-customer-id", "alice")]
    );
}

/// The common case: a round on an internal endpoint carries nothing, and must
/// not invent headers of its own.
#[tokio::test]
async fn a_round_without_a_stamp_carries_nothing() {
    let model = Arc::new(ScriptedModel::new(vec![
        ResponseChunk::TextBlock("hi".into()),
        ResponseChunk::TextDone,
        done(FinishReason::Stop),
    ]));
    let transport = model.seen_transport.clone();
    let (mut sink, _log) = RecordingSink::new(SinkVerdict::Continue);

    executor(model)
        .run(
            &task(vec![ResponseItem::user_text("hi")], vec![]),
            &mut sink,
            &NeverCancel,
        )
        .await
        .expect("completes");

    assert!(transport.lock().expect("lock").is_empty());
}

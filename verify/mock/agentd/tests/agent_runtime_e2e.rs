//! End-to-end execution against the in-memory adapters and a mock scheduler.
//!
//! No socket, no database, no model. This is what the port-based runtime buys: the
//! paths that matter — a moved fence, an unusable outcome, a provider failure —
//! are all reachable and cheap.
//!
//! Durable output now lives in the conversation snapshot (D30), so tests that
//! assert persistence read `read_snapshot` rather than a per-response record.

use std::sync::Arc;
use std::time::Duration;

use nova_responses::ports::{ConversationRepo, ConversationSnapshots};

use mock_server::MemWorld;
use async_trait::async_trait;
use nova_agent_runtime::{
    AgentError, AgentEventSink, AgentOutcome, AgentRunner, AgentRuntime, AgentRuntimeConfig,
    AgentRuntimeDeps, AgentTask, CancelProbe, Executed,
};
use mock_agentd::completions::{
    CompletionsMessage, CompletionsOutcome, CompletionsRequest, ToolCall,
};
use mock_agentd::{
    EchoScheduler, Match, MockAgentRunner, NoopToolExecutor, Scheduler, SchedulerError, Script,
    ScriptedScheduler, ToolError, ToolExecutor,
};
use nova_responses::protocol::{ContentPart, ResponseItem, Role};
use nova_responses::ports::{ResponseEventLog, ResponseLedger};
use nova_responses::{
    Clock, ContextAnchor, Conversation, ConversationId, EventBody, IdempotencyKey, ModelParams,
    NodeTag, ResolvedContext, ResponseEventKind, ResponseId, ResponseRecord, ResponseStatus,
    TenantId, TurnSpec, Usage,
};
use tokio::sync::Notify;

const NODE: &str = "node-a";

fn node() -> NodeTag {
    NodeTag::parse(NODE).expect("static tag")
}

fn tenant() -> TenantId {
    TenantId::parse("t-engine").expect("static tenant")
}

fn record(id: &ResponseId, text: &str, stored: bool) -> ResponseRecord {
    ResponseRecord::queued(
        id.clone(),
        tenant(),
        TurnSpec {
            params: ModelParams::new("m"),
            input_items: vec![ResponseItem::Message {
                role: Role::User,
                content: vec![ContentPart::InputText { text: text.into() }],
                id: None,
                status: None,
            }],
            store: stored,
            ext: None,
            anchor: ContextAnchor::Root,
        },
        IdempotencyKey::parse(&id.to_string()).expect("a response id is a valid key"),
        1_000,
        0,
    )
}

fn engine(world: &MemWorld, scheduler: Arc<dyn Scheduler>) -> AgentRuntime {
    agent_with(world, scheduler, Arc::new(NoopToolExecutor), AgentRuntimeConfig::default())
}

fn agent_with(
    world: &MemWorld,
    scheduler: Arc<dyn Scheduler>,
    tools: Arc<dyn ToolExecutor>,
    cfg: AgentRuntimeConfig,
) -> AgentRuntime {
    let runner = Arc::new(MockAgentRunner::new(scheduler, tools));
    AgentRuntime::new(
        AgentRuntimeDeps {
            ledger: world.ledger.clone(),
            event_log: world.event_log.clone(),
            runner,
            clock: world.clock.clone(),
            conversations: Some(world.conversation.clone()),
        },
        cfg,
    )
}

/// Queue one response, anchored to a fresh conversation so its output has a
/// durable home (D30). Returns the response id and its conversation.
async fn queue(world: &MemWorld, text: &str, stored: bool) -> (ResponseId, ConversationId) {
    let conv = world
        .conversation
        .create(Conversation::new(
            ConversationId::new(),
            tenant(),
            Default::default(),
            1_000,
        ))
        .await
        .expect("create conversation");
    let id = ResponseId::new(node());
    let mut rec = record(&id, text, stored);
    rec.spec.anchor = ContextAnchor::Conversation(conv.id.clone());
    world
        .ledger
        .create(rec, IdempotencyKey::parse(&uuid::Uuid::new_v4().to_string()).expect("a uuid is a valid key"), 1_000)
        .await
        .expect("create");
    (id, conv.id)
}

async fn snapshot(world: &MemWorld, conv: &ConversationId) -> ResolvedContext {
    world
        .conversation
        .read_snapshot(&tenant(), conv)
        .await
        .expect("read snapshot")
}

/// The output half of a single-turn snapshot: everything after the input item.
fn output_of(snap: &ResolvedContext) -> Vec<ResponseItem> {
    // One turn contributes input then output; the input is the first item.
    snap.items().skip(1).cloned().collect()
}

#[tokio::test]
async fn a_queued_response_runs_to_completion_with_no_socket_and_no_model() {
    let world = MemWorld::new();
    let (id, conv) = queue(&world, "hello", true).await;

    let e = engine(&world, Arc::new(EchoScheduler::new(4)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
    assert!(rec.usage.total_tokens() > 0, "usage must be booked");

    let snap = snapshot(&world, &conv).await;
    let output = output_of(&snap);
    assert!(
        nova_responses::canonical_items(&output).contains("hello"),
        "the echoed answer must be persisted to the snapshot"
    );

    let events = world
        .event_log
        .read_after(&id, None, 64, Duration::from_millis(0))
        .await
        .expect("read");
    assert!(events.iter().any(|ev| ev.kind().is_terminal()));
    assert!(events.iter().any(|ev| matches!(ev.kind(), ResponseEventKind::OutputTextDelta)));
}

#[tokio::test]
async fn an_empty_queue_is_idle_not_an_error() {
    let world = MemWorld::new();
    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(1_000).await, Executed::Idle);
}

#[tokio::test]
async fn any_nodes_work_can_be_executed_here() {
    let world = MemWorld::new();
    let foreign = ResponseId::new(NodeTag::parse("node-b").expect("tag"));
    // store=false: the test only asserts global claim, not persistence.
    world
        .ledger
        .create(
            record(&foreign, "not yours", false),
            IdempotencyKey::parse(&uuid::Uuid::new_v4().to_string()).expect("a uuid is a valid key"),
            1_000,
        )
        .await
        .expect("create");

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&foreign).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
}

/// A runner that records only the task's `ext`, so the record→task wiring can be
/// asserted at the seam itself rather than through a scheduler that never sees it.
struct ExtCapturingRunner {
    seen: std::sync::Arc<std::sync::Mutex<Option<serde_json::Value>>>,
}

#[async_trait]
impl AgentRunner for ExtCapturingRunner {
    fn name(&self) -> &str {
        "ext-capturing"
    }

    async fn run(
        &self,
        task: &AgentTask,
        _sink: &mut dyn AgentEventSink,
        _cancel: &dyn CancelProbe,
    ) -> Result<AgentOutcome, AgentError> {
        *self.seen.lock().expect("lock") = task.ext.clone();
        Ok(AgentOutcome {
            items: Vec::new(),
            usage: Usage::new(1, 1),
            status: ResponseStatus::Completed,
        })
    }
}

#[tokio::test]
async fn the_tasks_ext_is_carried_from_the_record_to_the_runner_seam() {
    let world = MemWorld::new();
    let ext = serde_json::json!({ "tool_context": { "project": "demo" } });

    // A record with ext set, the way the gateway's create path stores it.
    let fresh = ResponseId::new(node());
    let mut rec = record(&fresh, "with ext", false);
    rec.spec.ext = Some(ext.clone());
    world
        .ledger
        .create(rec, IdempotencyKey::parse(&uuid::Uuid::new_v4().to_string()).expect("key"), 1_000)
        .await
        .expect("create");

    let seen = std::sync::Arc::new(std::sync::Mutex::new(None));
    let runner = Arc::new(ExtCapturingRunner { seen: seen.clone() });
    let e = AgentRuntime::new(
        AgentRuntimeDeps {
            ledger: world.ledger.clone(),
            event_log: world.event_log.clone(),
            runner,
            clock: world.clock.clone(),
            conversations: Some(world.conversation.clone()),
        },
        AgentRuntimeConfig::default(),
    );
    assert_eq!(e.run_once(2_000).await, Executed::Completed);
    assert_eq!(seen.lock().expect("lock").clone(), Some(ext));
}

#[tokio::test]
async fn a_scheduler_failure_terminates_the_response() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "flaky", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(
        Match::Any,
        Script::Fail {
            message: "provider returned 503".into(),
        },
    );
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Failed);
    assert!(rec.status.is_terminal());

    let events = world
        .event_log
        .read_after(&id, None, 64, Duration::from_millis(0))
        .await
        .expect("read");
    assert!(events.iter().any(|ev| ev.kind().is_terminal()));
}

#[tokio::test]
async fn an_unusable_outcome_is_refused_before_submission() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "anything", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(Match::Any, Script::Empty);
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Failed);
}

#[tokio::test]
async fn a_refusal_completes_the_turn_and_is_stored() {
    let world = MemWorld::new();
    let (_id, conv) = queue(&world, "secret", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(
        Match::Any,
        Script::Refuse {
            reason: "I can't help with that".into(),
        },
    );
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let snap = snapshot(&world, &conv).await;
    assert!(
        nova_responses::canonical_items(&output_of(&snap)).contains("can't help"),
        "the refusal must be readable afterwards"
    );
}

#[tokio::test]
async fn store_false_completes_without_persisting_content() {
    let world = MemWorld::new();
    let (id, conv) = queue(&world, "ephemeral", false).await;

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
    assert!(!rec.is_stored(), "the flag must survive the round trip");

    let snap = snapshot(&world, &conv).await;
    assert_eq!(snap.turns, 0, "store=false must not write a snapshot turn");
    assert!(snap.is_empty());
}

#[tokio::test]
async fn a_stall_leaves_partial_output_but_does_not_complete() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "stall", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(
        Match::Any,
        Script::Stall {
            before: "starting to ans".into(),
        },
    );
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let events = world
        .event_log
        .read_after(&id, None, 64, Duration::from_millis(0))
        .await
        .expect("read");
    assert!(events.iter().any(|ev| matches!(ev.kind(), ResponseEventKind::OutputTextDelta)));
    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Failed);
}

#[tokio::test]
async fn a_failed_turn_archives_its_input() {
    // The turn fails (the scheduler stalls), but its input must still land in the
    // snapshot (D30 incomplete-turn archival) so the chain keeps the user's question.
    let world = MemWorld::new();
    let (_, conv) = queue(&world, "stall", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(
        Match::Any,
        Script::Stall {
            before: "starting to ans".into(),
        },
    );
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let snap = snapshot(&world, &conv).await;
    assert_eq!(snap.turns, 1, "the failed turn is still a turn");
    // The input plus the one item that reached a `done` boundary before the stall are
    // archived; the stall script completes its item before failing, so there is no
    // half-streamed message to reconstruct here (that case is covered in the gateway
    // contract tests).
    assert_eq!(snap.item_count(), 2, "input + the one completed output item");
    let rendered = nova_responses::canonical_items(&snap.clone().into_items());
    assert!(
        rendered.contains("stall"),
        "the failed turn's input must be archived: {rendered}"
    );
    assert!(
        rendered.contains("starting to ans"),
        "completed output must be archived even though the turn failed: {rendered}"
    );
}

#[tokio::test]
async fn drain_clears_the_backlog_and_then_reports_idle() {
    let world = MemWorld::new();
    for i in 0..3 {
        queue(&world, &format!("q{i}"), true).await;
    }

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    let results = e.drain(2_000, 10).await;

    assert_eq!(
        results.iter().filter(|r| **r == Executed::Completed).count(),
        3
    );
    assert_eq!(results.last(), Some(&Executed::Idle));
}

#[tokio::test]
async fn history_is_assembled_from_the_conversation_snapshot() {
    let world = MemWorld::new();

    let (first, conv) = queue(&world, "my name is Ada", true).await;
    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    // Second turn anchored to the same conversation (no previous_response_id):
    // the server reads the conversation snapshot for history.
    let second = ResponseId::new(node());
    let mut rec = record(&second, "what is my name", true);
    rec.spec.anchor = ContextAnchor::Conversation(conv.clone());
    world
        .ledger
        .create(rec, IdempotencyKey::parse(&uuid::Uuid::new_v4().to_string()).expect("a uuid is a valid key"), 3_000)
        .await
        .expect("create");

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(4_000).await, Executed::Completed);

    // The snapshot now holds both turns.
    let snap = snapshot(&world, &conv).await;
    assert_eq!(snap.turns, 2);
    assert_eq!(snap.item_count(), 4, "2 turns x (input + output)");
    let rendered = nova_responses::canonical_items(&snap.clone().into_items());
    assert!(rendered.contains("my name is Ada"));
    assert!(rendered.contains("what is my name"));
    let _ = first;
}

// ===== Agent loop (ReAct) =====

#[derive(Default)]
struct MapTools {
    outputs: std::collections::HashMap<String, String>,
}

impl MapTools {
    fn with(mut self, name: &str, output: &str) -> Self {
        self.outputs.insert(name.to_string(), output.to_string());
        self
    }
}

#[async_trait]
impl ToolExecutor for MapTools {
    fn name(&self) -> &str {
        "map"
    }

    async fn call(&self, tool: &str, _arguments: &str) -> Result<String, ToolError> {
        match self.outputs.get(tool) {
            Some(output) => Ok(output.clone()),
            None => Err(ToolError::UnknownTool(tool.to_string())),
        }
    }
}

struct ToolThenAnswer;

#[async_trait]
impl Scheduler for ToolThenAnswer {
    fn name(&self) -> &str {
        "tool-then-answer"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let saw_tool_output = request
            .messages
            .iter()
            .any(|m| matches!(m, CompletionsMessage::Tool { .. }));

        if saw_tool_output {
            let text = "It is sunny in Paris.";
            sink.text_delta(text).await?;
            Ok(CompletionsOutcome::text(text, Usage::new(4, 5)))
        } else {
            let call = ToolCall {
                id: "call_weather_1".into(),
                name: "get_weather".into(),
                arguments: r#"{"city":"Paris"}"#.into(),
            };
            sink.tool_call(&call.id, &call.name, &call.arguments).await?;
            Ok(CompletionsOutcome::tool_calls(vec![call], Usage::new(2, 1)))
        }
    }
}

#[tokio::test]
async fn a_tool_using_turn_runs_the_loop_and_stores_the_whole_trace() {
    let world = MemWorld::new();
    let (id, conv) = queue(&world, "what is the weather in Paris", true).await;

    let scheduler = Arc::new(ToolThenAnswer);
    let tools = Arc::new(MapTools::default().with("get_weather", r#"{"temp":20}"#));
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);

    let snap = snapshot(&world, &conv).await;
    let output = output_of(&snap);
    assert_eq!(output.len(), 3, "call + output + answer");
    assert!(matches!(&output[0], ResponseItem::FunctionCall { name, .. } if name == "get_weather"));
    assert!(matches!(&output[1], ResponseItem::FunctionCallOutput { output, .. } if output.contains("20")));
    assert!(matches!(&output[2], ResponseItem::Message { role: Role::Assistant, content, .. }
        if content.iter().any(|p| matches!(p, ContentPart::OutputText { text } if text.contains("sunny")))));

    let events = world.event_log.read_after(&id, None, 64, Duration::from_millis(0)).await.expect("read");
    let kinds: Vec<_> = events.iter().map(|ev| ev.kind()).collect();
    assert!(kinds.contains(&ResponseEventKind::OutputItemAdded), "{kinds:?}");
    assert!(kinds.contains(&ResponseEventKind::OutputItemDone), "{kinds:?}");
    assert!(events.iter().any(|ev| ev.kind().is_terminal()));
}

#[tokio::test]
async fn a_tool_error_fails_the_response_loudly() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "what is the weather in Paris", true).await;

    let scheduler = Arc::new(ToolThenAnswer);
    let tools = Arc::new(MapTools::default());
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Failed);
    assert!(rec.status.is_terminal());
}

struct AlwaysToolCall;

#[async_trait]
impl Scheduler for AlwaysToolCall {
    fn name(&self) -> &str {
        "always-tool-call"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let call = ToolCall {
            id: "call_loop".into(),
            name: "ping".into(),
            arguments: "{}".into(),
        };
        sink.tool_call(&call.id, &call.name, &call.arguments).await?;
        Ok(CompletionsOutcome::tool_calls(vec![call], Usage::new(1, 1)))
    }
}

#[tokio::test]
async fn a_model_that_keeps_calling_tools_hits_the_round_ceiling() {
    let world = MemWorld::new();
    let (id, conv) = queue(&world, "loop forever", true).await;

    let scheduler = Arc::new(AlwaysToolCall);
    let tools = Arc::new(MapTools::default().with("ping", "pong"));
    let cfg = AgentRuntimeConfig {
        max_tool_rounds: 3,
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, tools, cfg);

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Incomplete);

    let snap = snapshot(&world, &conv).await;
    assert_eq!(output_of(&snap).len(), 6);
}

struct StreamingToolThenAnswer;

#[async_trait]
impl Scheduler for StreamingToolThenAnswer {
    fn name(&self) -> &str {
        "streaming-tool-then-answer"
    }

    async fn schedule(
        &self,
        request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        let saw_tool_output = request
            .messages
            .iter()
            .any(|m| matches!(m, CompletionsMessage::Tool { .. }));

        if saw_tool_output {
            let text = "It is sunny.";
            sink.text_delta(text).await?;
            return Ok(CompletionsOutcome::text(text, Usage::new(4, 5)));
        }

        let item_id = "call_stream_1";
        let name = "get_weather";
        let args = r#"{"city":"Paris"}"#;

        let empty = ResponseItem::FunctionCall {
            call_id: item_id.to_string(),
            name: name.to_string(),
            arguments: String::new(),
            id: None,
            status: None,
        };
        sink.output_item_added(&empty).await?;

        for fragment in ["{\"ci", "ty\":\"P", "aris\"}"] {
            sink.function_call_arguments_delta(item_id, fragment).await?;
        }
        sink.function_call_arguments_done(item_id, args).await?;

        let full = ResponseItem::FunctionCall {
            call_id: item_id.to_string(),
            name: name.to_string(),
            arguments: args.to_string(),
            id: None,
            status: None,
        };
        sink.output_item_done(&full).await?;

        Ok(CompletionsOutcome::tool_calls(
            vec![ToolCall {
                id: item_id.to_string(),
                name: name.to_string(),
                arguments: args.to_string(),
            }],
            Usage::new(2, 1),
        ))
    }
}

#[tokio::test]
async fn tool_calls_stream_incrementally_to_the_subscriber() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "what is the weather", true).await;

    let scheduler = Arc::new(StreamingToolThenAnswer);
    let tools = Arc::new(MapTools::default().with("get_weather", "20"));
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let events = world.event_log.read_after(&id, None, 128, Duration::from_millis(0)).await.expect("read");
    let kinds: Vec<_> = events.iter().map(|ev| ev.kind()).collect();

    assert!(kinds.contains(&ResponseEventKind::OutputItemAdded), "{kinds:?}");
    assert!(kinds.contains(&ResponseEventKind::FunctionCallArgumentsDelta), "{kinds:?}");
    assert!(kinds.contains(&ResponseEventKind::FunctionCallArgumentsDone), "{kinds:?}");
    assert!(kinds.contains(&ResponseEventKind::OutputItemDone), "{kinds:?}");

    let deltas: Vec<String> = events
        .iter()
        .filter(|ev| ev.kind() == ResponseEventKind::FunctionCallArgumentsDelta)
        .map(|ev| match &ev.body() {
            EventBody::Delta { delta, .. } => delta.clone(),
            _ => String::new(),
        })
        .collect();
    assert_eq!(deltas, vec!["{\"ci", "ty\":\"P", "aris\"}"]);
    assert_eq!(deltas.concat(), r#"{"city":"Paris"}"#);
}

struct ReasoningThenAnswer;

#[async_trait]
impl Scheduler for ReasoningThenAnswer {
    fn name(&self) -> &str {
        "reasoning-then-answer"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        sink.reasoning_text_delta("Let me think").await?;
        sink.reasoning_text_delta(" about this.").await?;
        let text = "The answer.";
        sink.text_delta(text).await?;
        Ok(CompletionsOutcome::text(text, Usage::new(2, 2)))
    }
}

#[tokio::test]
async fn reasoning_is_persisted_but_never_fed_back_as_context() {
    let world = MemWorld::new();
    let (id, conv) = queue(&world, "think", true).await;

    let e = engine(&world, Arc::new(ReasoningThenAnswer));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let snap = snapshot(&world, &conv).await;
    let output = output_of(&snap);
    assert_eq!(output.len(), 1, "reasoning must not become an item");
    assert!(
        !nova_responses::canonical_items(&output).contains("Let me think"),
        "reasoning must not leak into output items"
    );
    // The reasoning block travels on the entry it belongs to, so "which item does this
    // reasoning precede" is not a length agreement between two vectors any more.
    let reasoning: Vec<Option<String>> =
        snap.entries.iter().map(|e| e.reasoning.clone()).collect();
    assert_eq!(
        reasoning,
        vec![None, Some("Let me think about this.".into())]
    );
    let _ = id;
}

// ===== Heartbeat: long generation vs. reap =====

struct GatedScheduler {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl Scheduler for GatedScheduler {
    fn name(&self) -> &str {
        "gated"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        self.started.notify_one();
        self.release.notified().await;
        let text = "done";
        sink.text_delta(text).await?;
        Ok(CompletionsOutcome::text(text, Usage::new(1, 1)))
    }
}

async fn settle() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(start_paused = true)]
async fn a_long_generation_is_not_reaped_while_its_heartbeat_stays_fresh() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "long", true).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let scheduler = Arc::new(GatedScheduler {
        started: started.clone(),
        release: release.clone(),
    });

    let cfg = AgentRuntimeConfig {
        heartbeat_interval: Duration::from_millis(1_000),
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, Arc::new(NoopToolExecutor), cfg);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    started.notified().await;

    world.clock.advance(2_000);
    tokio::time::advance(Duration::from_millis(2_000)).await;
    settle().await;

    let aborted = world.ledger.reap(2_000, Duration::from_millis(1_500)).await.expect("reap");
    assert!(aborted.is_empty());

    release.notify_one();
    let result = handle.await.expect("join");
    assert_eq!(result, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
}

#[tokio::test(start_paused = true)]
async fn a_generation_is_reaped_once_its_heartbeat_stops() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "long", true).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let scheduler = Arc::new(GatedScheduler {
        started: started.clone(),
        release: release.clone(),
    });

    let cfg = AgentRuntimeConfig {
        heartbeat_interval: Duration::MAX,
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, Arc::new(NoopToolExecutor), cfg);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    started.notified().await;

    world.clock.advance(5_000);
    tokio::time::advance(Duration::from_millis(5_000)).await;
    settle().await;

    let aborted = world.ledger.reap(5_000, Duration::from_millis(2_000)).await.expect("reap");
    assert_eq!(aborted.len(), 1, "a stopped heartbeat must be reaped");
    assert_eq!(aborted[0].response_id, id);

    release.notify_one();
    let result = handle.await.expect("join");
    assert_eq!(result, Executed::Superseded);
}

// ===== Cancellation propagates into a blocking tool call =====

/// A tool that announces it started, then blocks forever — only the active
/// cancellation probe can interrupt it.
struct BlockingTool {
    started: Arc<Notify>,
}

#[async_trait]
impl ToolExecutor for BlockingTool {
    fn name(&self) -> &str {
        "blocking"
    }

    async fn call(&self, _tool: &str, _arguments: &str) -> Result<String, ToolError> {
        self.started.notify_one();
        std::future::pending::<()>().await;
        unreachable!("the tool call is interrupted, never completed")
    }
}

#[tokio::test(start_paused = true)]
async fn a_cancelled_tool_call_is_interrupted() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "tool", true).await;

    let started = Arc::new(Notify::new());
    let tools = Arc::new(BlockingTool {
        started: started.clone(),
    });
    let scheduler = Arc::new(ToolThenAnswer);
    let cfg = AgentRuntimeConfig {
        cancel_poll_interval: Duration::from_millis(100),
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, tools, cfg);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    // Wait until the runner is inside the blocking tool call.
    started.notified().await;

    // Cancel: raises the attempt fence (INV-6), which the active probe polls.
    let t = tenant();
    world.ledger.cancel(&t, &id, 1_000).await.expect("cancel");

    // Advance past a poll interval so the probe's sleep elapses and it observes
    // `StaleAttempt`, then let the runner act on it.
    tokio::time::advance(Duration::from_millis(200)).await;
    settle().await;

    let result = handle.await.expect("join");
    assert_eq!(
        result,
        Executed::Superseded,
        "cancellation must interrupt a blocking tool call promptly"
    );
}

// ===== Cancellation propagates during streaming (passive fence check) =====

/// Streams one delta, then blocks on a gate before the next: cancellation during the
/// second delta's append is caught by the sink's passive fence check, the counterpart
/// to the active probe exercised by the tool-call test above.
struct StreamThenGateScheduler {
    started: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl Scheduler for StreamThenGateScheduler {
    fn name(&self) -> &str {
        "stream-then-gate"
    }

    async fn schedule(
        &self,
        _request: &CompletionsRequest,
        sink: &mut dyn AgentEventSink,
    ) -> Result<CompletionsOutcome, SchedulerError> {
        sink.text_delta("before cancel").await?;
        self.started.notify_one();
        self.release.notified().await;
        // The append now carries the superseded attempt, so the sink refuses it.
        if matches!(
            sink.text_delta("after cancel").await?,
            nova_agent_runtime::SinkVerdict::Stop
        ) {
            return Err(SchedulerError::Superseded);
        }
        Ok(CompletionsOutcome::text("done", Usage::new(1, 1)))
    }
}

#[tokio::test(start_paused = true)]
async fn cancellation_interrupts_a_streaming_generation() {
    let world = MemWorld::new();
    let (id, _conv) = queue(&world, "stream", true).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let scheduler = Arc::new(StreamThenGateScheduler {
        started: started.clone(),
        release: release.clone(),
    });
    let e = engine(&world, scheduler);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    // The first delta has streamed; the runner is now gated before the second.
    started.notified().await;

    // Cancel raises the attempt fence (INV-60), so the next append is refused.
    let t = tenant();
    world.ledger.cancel(&t, &id, 1_000).await.expect("cancel");

    release.notify_one();
    let result = handle.await.expect("join");
    assert_eq!(
        result,
        Executed::Superseded,
        "cancellation must interrupt a streaming generation via the passive fence check"
    );
}

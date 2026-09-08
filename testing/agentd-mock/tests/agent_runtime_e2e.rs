//! End-to-end execution against the in-memory adapters and a mock scheduler.
//!
//! No socket, no database, no model. This is what the port-based runtime buys: the
//! paths that matter — a moved fence, an unusable outcome, a broken chain, a
//! provider failure — are all reachable and cheap.

use std::sync::Arc;
use std::time::Duration;

use adapters_mem::MemWorld;
use async_trait::async_trait;
use nova_agent_runtime::{
    AgentEventSink, AgentRuntime, AgentRuntimeConfig, AgentRuntimeDeps, Executed,
};
use nova_agentd_mock::completions::{
    CompletionsMessage, CompletionsOutcome, CompletionsRequest, ToolCall,
};
use nova_agentd_mock::{
    EchoScheduler, Match, MockAgentRunner, NoopToolExecutor, Scheduler, SchedulerError, Script,
    ScriptedScheduler, ToolError, ToolExecutor,
};
use nova_responses_core::protocol::{ContentPart, ResponseItem, Role};
use nova_responses_core::{
    Attempt, ContextStore, EventBody, IdempotencyKey, NodeTag, ResponseEventKind,
    ResponseEventLog, ResponseId, ResponseLedger, ResponseStatus, StoredResponse, TenantId, Usage,
};
use tokio::sync::Notify;

const NODE: &str = "node-a";

fn node() -> NodeTag {
    NodeTag::parse(NODE).expect("static tag")
}

fn tenant() -> TenantId {
    TenantId::parse("t-engine").expect("static tenant")
}

fn record(id: &ResponseId, text: &str, stored: bool) -> StoredResponse {
    StoredResponse {
        conversation_id: None,
        response_id: id.clone(),
        previous_response_id: None,
        tenant_id: tenant(),
        model: "m".into(),
        instructions: None,
        tools: Vec::new(),
        tool_choice: None,
        input_items: vec![ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText { text: text.into() }],
            id: None,
            status: None,
        }],
        output_items: vec![],
        reasoning: None,
        status: ResponseStatus::Queued,
        usage: Usage::default(),
        created_at_ms: 1_000,
        completed_at_ms: None,
        stored,
        expires_at_ms: None,
        integrity: None,
        integrity_alg: None,
        node_tag: id.node_tag().clone(),
        idempotency_key: None,
        owner: None,
        attempt: Attempt::default(),
        context: Vec::new(),
        context_reasoning: Vec::new(),
        context_depth: 0,
    }
}

fn engine(world: &MemWorld, scheduler: Arc<dyn Scheduler>) -> AgentRuntime {
    agent_with(world, scheduler, Arc::new(NoopToolExecutor), AgentRuntimeConfig::default())
}

/// An orchestrator wired with a specific tool executor and config, for the
/// multi-round tests.
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
            context: world.context.clone(),
            runner,
            now: world.now_fn(),
            conversations: Some(world.conversation.clone()),
        },
        cfg,
    )
}

async fn queue(world: &MemWorld, text: &str, stored: bool) -> ResponseId {
    let id = ResponseId::new(node());
    let rec = record(&id, text, stored);
    world
        .ledger
        .create(
            rec.clone(),
            IdempotencyKey(uuid::Uuid::new_v4().to_string()),
            1_000,
        )
        .await
        .expect("create");
    if stored {
        world.context.put(rec).await.expect("put");
    }
    id
}

#[tokio::test]
async fn a_queued_response_runs_to_completion_with_no_socket_and_no_model() {
    let world = MemWorld::new();
    let id = queue(&world, "hello", true).await;

    let e = engine(&world, Arc::new(EchoScheduler::new(4)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
    assert!(rec.usage.total_tokens > 0, "usage must be booked");

    let stored = world
        .context
        .get(&tenant(), &id)
        .await
        .expect("ctx get")
        .expect("stored");
    assert!(
        nova_responses_core::canonical_items(&stored.output_items).contains("hello"),
        "the echoed answer must be persisted"
    );

    let events = world
        .event_log
        .read_after(&id, None, 64, 0)
        .await
        .expect("read");
    assert!(
        events.iter().any(|ev| ev.kind.is_terminal()),
        "the stream must reach a terminal event or subscribers hang forever"
    );
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev.kind, ResponseEventKind::OutputTextDelta)),
        "increments must land in this node's buffer"
    );
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
    world
        .ledger
        .create(
            record(&foreign, "not yours", true),
            IdempotencyKey(uuid::Uuid::new_v4().to_string()),
            1_000,
        )
        .await
        .expect("create");

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(
        e.run_once(2_000).await,
        Executed::Completed,
        "any execution process must be able to run any node's response"
    );

    let rec = world.ledger.get(&foreign).await.expect("get").expect("present");
    assert_eq!(
        rec.status,
        ResponseStatus::Completed,
        "and the response must reach a terminal state"
    );
}

#[tokio::test]
async fn a_scheduler_failure_terminates_the_response() {
    let world = MemWorld::new();
    let id = queue(&world, "flaky", true).await;

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
        .read_after(&id, None, 64, 0)
        .await
        .expect("read");
    assert!(
        events.iter().any(|ev| ev.kind.is_terminal()),
        "a failed attempt must still terminate the stream"
    );
}

#[tokio::test]
async fn an_unusable_outcome_is_refused_before_submission() {
    let world = MemWorld::new();
    let id = queue(&world, "anything", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(Match::Any, Script::Empty);
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(
        rec.status,
        ResponseStatus::Failed,
        "an empty outcome must not complete the response as an empty success"
    );
}

#[tokio::test]
async fn a_refusal_completes_the_turn_and_is_stored() {
    let world = MemWorld::new();
    let id = queue(&world, "secret", true).await;

    let scheduler = ScriptedScheduler::from_rules(vec![]).with_rule(
        Match::Any,
        Script::Refuse {
            reason: "I can't help with that".into(),
        },
    );
    let e = engine(&world, Arc::new(scheduler));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let stored = world
        .context
        .get(&tenant(), &id)
        .await
        .expect("ctx get")
        .expect("stored");
    assert!(
        nova_responses_core::canonical_items(&stored.output_items).contains("can't help"),
        "the refusal must be readable afterwards"
    );
}

#[tokio::test]
async fn store_false_completes_without_persisting_content() {
    let world = MemWorld::new();
    let id = queue(&world, "ephemeral", false).await;

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
    assert!(!rec.stored, "the flag must survive the round trip");

    assert!(
        matches!(
            world
                .context
                .resolve_chain(&tenant(), &id, Default::default())
                .await,
            Err(nova_responses_core::ContextError::NotStored)
        ),
        "a response created with store=false must be refused as a chain anchor"
    );

    if let Some(stored) = world.context.get(&tenant(), &id).await.expect("ctx get") {
        assert!(
            stored.output_items.is_empty(),
            "store=false must not retain output items for later turns"
        );
    }
}

#[tokio::test]
async fn a_stall_leaves_partial_output_but_does_not_complete() {
    let world = MemWorld::new();
    let id = queue(&world, "stall", true).await;

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
        .read_after(&id, None, 64, 0)
        .await
        .expect("read");
    assert!(
        events
            .iter()
            .any(|ev| matches!(ev.kind, ResponseEventKind::OutputTextDelta)),
        "the partial output that was streamed is still visible"
    );
    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(
        rec.status,
        ResponseStatus::Failed,
        "but the response must not be recorded as complete"
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
    assert_eq!(
        results.last(),
        Some(&Executed::Idle),
        "drain must stop on the first empty claim rather than spinning"
    );
}

#[tokio::test]
async fn history_is_assembled_server_side_from_the_chain() {
    let world = MemWorld::new();

    let first = queue(&world, "my name is Ada", true).await;
    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let second = ResponseId::new(node());
    let mut rec = record(&second, "what is my name", true);
    rec.previous_response_id = Some(first.clone());
    world
        .ledger
        .create(
            rec.clone(),
            IdempotencyKey(uuid::Uuid::new_v4().to_string()),
            3_000,
        )
        .await
        .expect("create");
    world.context.put(rec).await.expect("put");

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(e.run_once(4_000).await, Executed::Completed);

    let stored = world
        .context
        .get(&tenant(), &second)
        .await
        .expect("ctx get")
        .expect("stored");
    assert!(
        nova_responses_core::canonical_items(&stored.output_items).contains("what is my name"),
        "the newest input must still be the one answered"
    );
}

#[tokio::test]
async fn a_deleted_ancestor_does_not_strand_execution() {
    let world = MemWorld::new();
    let missing = ResponseId::new(node());

    let id = ResponseId::new(node());
    let mut rec = record(&id, "continue", true);
    rec.previous_response_id = Some(missing.clone());
    rec.context = vec![ResponseItem::assistant_text("earlier answer")];
    rec.context_depth = 1;
    world
        .ledger
        .create(
            rec.clone(),
            IdempotencyKey(uuid::Uuid::new_v4().to_string()),
            1_000,
        )
        .await
        .expect("create");
    world.context.put(rec).await.expect("put");

    let e = engine(&world, Arc::new(EchoScheduler::new(2)));
    assert_eq!(
        e.run_once(2_000).await,
        Executed::Completed,
        "execution must use the snapshot, not re-walk a broken pointer"
    );

    let after = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(after.status, ResponseStatus::Completed);
}

// ===== Agent loop (ReAct) =====

/// A tool executor whose answers are declared up front.
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

/// First schedule asks for a tool; the next one answers.
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
    let id = queue(&world, "what is the weather in Paris", true).await;

    let scheduler = Arc::new(ToolThenAnswer);
    let tools = Arc::new(MapTools::default().with("get_weather", r#"{"temp":20}"#));
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);

    let stored = world
        .context
        .get(&tenant(), &id)
        .await
        .expect("ctx get")
        .expect("stored");

    assert_eq!(stored.output_items.len(), 3, "call + output + answer");
    assert!(matches!(
        &stored.output_items[0],
        ResponseItem::FunctionCall { name, .. } if name == "get_weather"
    ));
    assert!(matches!(
        &stored.output_items[1],
        ResponseItem::FunctionCallOutput { output, .. } if output.contains("20")
    ));
    assert!(matches!(
        &stored.output_items[2],
        ResponseItem::Message { role: Role::Assistant, content, .. }
            if content.iter().any(|p| matches!(p, ContentPart::OutputText { text } if text.contains("sunny")))
    ));

    let events = world
        .event_log
        .read_after(&id, None, 64, 0)
        .await
        .expect("read");
    let kinds: Vec<_> = events.iter().map(|ev| ev.kind).collect();
    assert!(
        kinds.contains(&ResponseEventKind::OutputItemAdded),
        "a function_call item must be announced: {kinds:?}"
    );
    assert!(
        kinds.contains(&ResponseEventKind::OutputItemDone),
        "a function_call item must be completed: {kinds:?}"
    );
    assert!(events.iter().any(|ev| ev.kind.is_terminal()));
}

#[tokio::test]
async fn a_tool_error_fails_the_response_loudly() {
    let world = MemWorld::new();
    let id = queue(&world, "what is the weather in Paris", true).await;

    let scheduler = Arc::new(ToolThenAnswer);
    let tools = Arc::new(MapTools::default());
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Failed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Failed);
    assert!(rec.status.is_terminal());
}

/// Always asks for a tool, never answers.
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
    let id = queue(&world, "loop forever", true).await;

    let scheduler = Arc::new(AlwaysToolCall);
    let tools = Arc::new(MapTools::default().with("ping", "pong"));
    let cfg = AgentRuntimeConfig {
        max_tool_rounds: 3,
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, tools, cfg);

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(
        rec.status,
        ResponseStatus::Incomplete,
        "a never-ending tool loop must be cut off, not left running"
    );

    let stored = world
        .context
        .get(&tenant(), &id)
        .await
        .expect("ctx get")
        .expect("stored");
    assert_eq!(stored.output_items.len(), 6);
}

/// Streams a tool call incrementally.
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
    let id = queue(&world, "what is the weather", true).await;

    let scheduler = Arc::new(StreamingToolThenAnswer);
    let tools = Arc::new(MapTools::default().with("get_weather", "20"));
    let e = agent_with(&world, scheduler, tools, AgentRuntimeConfig::default());

    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let events = world
        .event_log
        .read_after(&id, None, 128, 0)
        .await
        .expect("read");
    let kinds: Vec<_> = events.iter().map(|ev| ev.kind).collect();

    assert!(kinds.contains(&ResponseEventKind::OutputItemAdded), "{kinds:?}");
    assert!(
        kinds.contains(&ResponseEventKind::FunctionCallArgumentsDelta),
        "{kinds:?}"
    );
    assert!(
        kinds.contains(&ResponseEventKind::FunctionCallArgumentsDone),
        "{kinds:?}"
    );
    assert!(kinds.contains(&ResponseEventKind::OutputItemDone), "{kinds:?}");

    let deltas: Vec<String> = events
        .iter()
        .filter(|ev| ev.kind == ResponseEventKind::FunctionCallArgumentsDelta)
        .map(|ev| match &ev.body {
            EventBody::Delta { delta, .. } => delta.clone(),
            _ => String::new(),
        })
        .collect();
    assert_eq!(deltas, vec!["{\"ci", "ty\":\"P", "aris\"}"]);
    assert_eq!(deltas.concat(), r#"{"city":"Paris"}"#);
}

/// Emits reasoning text, then the answer.
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
    let id = queue(&world, "think", true).await;

    let e = engine(&world, Arc::new(ReasoningThenAnswer));
    assert_eq!(e.run_once(2_000).await, Executed::Completed);

    let stored = world
        .context
        .get(&tenant(), &id)
        .await
        .expect("ctx get")
        .expect("stored");
    assert_eq!(stored.reasoning.as_deref(), Some("Let me think about this."));
    assert_eq!(stored.output_items.len(), 1, "reasoning must not become an item");
    assert!(
        !nova_responses_core::canonical_items(&stored.output_items).contains("Let me think"),
        "reasoning must not leak into output items"
    );

    let resolved = world
        .context
        .resolve_chain(&tenant(), &id, Default::default())
        .await
        .expect("resolve");
    assert_eq!(resolved.reasoning, vec![None, Some("Let me think about this.".into())]);
    assert_eq!(resolved.items.len(), 2, "input + answer, no reasoning item");
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
    let id = queue(&world, "long", true).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let scheduler = Arc::new(GatedScheduler {
        started: started.clone(),
        release: release.clone(),
    });

    let cfg = AgentRuntimeConfig {
        heartbeat_interval_ms: 1_000,
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, Arc::new(NoopToolExecutor), cfg);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    started.notified().await;

    world.clock.advance(2_000);
    tokio::time::advance(Duration::from_millis(2_000)).await;
    settle().await;

    let aborted = world.ledger.reap(2_000, 1_500).await.expect("reap");
    assert!(
        aborted.is_empty(),
        "a generation whose owner keeps heartbeating must not be reaped"
    );

    release.notify_one();
    let result = handle.await.expect("join");
    assert_eq!(result, Executed::Completed);

    let rec = world.ledger.get(&id).await.expect("get").expect("present");
    assert_eq!(rec.status, ResponseStatus::Completed);
}

#[tokio::test(start_paused = true)]
async fn a_generation_is_reaped_once_its_heartbeat_stops() {
    let world = MemWorld::new();
    let id = queue(&world, "long", true).await;

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let scheduler = Arc::new(GatedScheduler {
        started: started.clone(),
        release: release.clone(),
    });

    let cfg = AgentRuntimeConfig {
        heartbeat_interval_ms: u64::MAX,
        ..AgentRuntimeConfig::default()
    };
    let e = agent_with(&world, scheduler, Arc::new(NoopToolExecutor), cfg);
    let clock = world.clock.clone();

    let handle = tokio::spawn(async move { e.run_once(clock.now_ms()).await });

    started.notified().await;

    world.clock.advance(5_000);
    tokio::time::advance(Duration::from_millis(5_000)).await;
    settle().await;

    let aborted = world.ledger.reap(5_000, 2_000).await.expect("reap");
    assert_eq!(aborted.len(), 1, "a stopped heartbeat must be reaped");
    assert_eq!(aborted[0].response_id, id);

    release.notify_one();
    let result = handle.await.expect("join");
    assert_eq!(result, Executed::Superseded);
}

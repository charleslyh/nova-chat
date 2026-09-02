//! This node's execution engine: a supervised loop over its own queued work.
//!
//! # Why the loop lives here rather than in a separate process
//!
//! A generation is executed by the node that created it, because that node holds
//! its in-flight event buffer — a `VecDeque` in this process's heap by deliberate
//! design (D21). The pull protocol this replaced allowed a worker attached to one
//! node to claim another node's generation; the increments then landed in the wrong
//! process while subscribers, routed by the node tag inside the id, saw the created
//! event and then silence. No error surfaced anywhere (D23).
//!
//! # Concurrency
//!
//! Bounded by `max_concurrent_executions`, which protects **this process**: a
//! backlog must not spawn one outbound call per queued generation. Provider-side
//! limits belong to the scheduler adapter — conflating the two would make a
//! provider's quota a property of the fleet's shape.

use std::sync::Arc;

use nova_agent::{Agent, AgentConfig, AgentDeps, Executed};
use nova_responses_core::{CompletionsRequestScheduler, NoopToolExecutor};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::state::AppState;

/// Start the engine loop for this node.
pub fn spawn(state: AppState, scheduler: Arc<dyn CompletionsRequestScheduler>) {
    let engine = Arc::new(Agent::new(
        AgentDeps {
            ledger: state.ledger.clone(),
            event_log: state.event_log.clone(),
            context: state.context.clone(),
            scheduler,
            tools: Arc::new(NoopToolExecutor),
            node_tag: state.cfg.node_tag.clone(),
        },
        AgentConfig {
            exec_ttl_ms: state.cfg.exec_ttl_ms,
            chain_limits: state.cfg.chain_limits,
            retain_after_terminal_ms: state.cfg.retain_after_terminal_ms,
            tool_specs: Vec::new(),
            ..AgentConfig::default()
        },
    ));

    let permits = Arc::new(Semaphore::new(state.cfg.max_concurrent_executions));

    info!(
        scheduler = engine.scheduler_name(),
        max_concurrent = state.cfg.max_concurrent_executions,
        "execution engine starting"
    );

    tokio::spawn(async move {
        // Recover first: generations accepted just before a restart are already
        // queued, and nothing external will trigger them.
        let now = state.now_ms().await;
        let recovered = engine.drain(now, 1_000).await;
        let ran = recovered
            .iter()
            .filter(|r| **r != Executed::Idle)
            .count();
        if ran > 0 {
            info!(count = ran, "ran generations queued before startup");
        }

        loop {
            // Woken by an accepted create; the timeout is a safety net for work that
            // arrived while a previous iteration was mid-flight, and for another
            // node's writes becoming visible in a shared ledger.
            tokio::select! {
                _ = state.work_ready.notified() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            }

            // Draining: finish what is in flight, start nothing new (FR-34).
            if !state.is_accepting() {
                continue;
            }

            let Ok(permit) = permits.clone().acquire_owned().await else {
                warn!("execution permits closed; engine stopping");
                return;
            };

            let engine = engine.clone();
            let state_for_task = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let now = state_for_task.now_ms().await;
                match engine.run_once(now).await {
                    Executed::Idle => {}
                    // More may be waiting; wake the loop again rather than sleeping
                    // out the interval with a non-empty queue.
                    _ => state_for_task.notify_work(),
                }
            });
        }
    });
}

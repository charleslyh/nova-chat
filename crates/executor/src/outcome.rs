//! How one model round ends, in protocol terms.
//!
//! This module exists so the mapping has exactly one home. The behaviour it
//! replaces overloaded a single `incomplete` status with three unrelated
//! meanings — "the model wants a tool", "the round budget ran out", and
//! "the provider broke" — which made a genuine failure indistinguishable from
//! ordinary progress, and forced the UI to report both as "cancelled".
//!
//! # The four terminations
//!
//! | The round … | Protocol terminal | Why |
//! | --- | --- | --- |
//! | asked for tools | `completed` (+ `function_call` items) | tool calls are output, not an unfinished state |
//! | answered | `completed` | — |
//! | was cut off / filtered | `incomplete` + reason | the only two official reasons |
//! | failed upstream | `failed` (as an error) | reported truthfully |
//!
//! The first row is the correction that matters: a turn stopping so its caller
//! can run a function is a **successful** turn. The caller notices the pending
//! call by reading the output items, not by reading the status.
//!
//! There is deliberately no "round limit" termination here. This executor runs
//! exactly one round, so a step ceiling is not a concept it can have; that
//! belongs to whoever drives the chain.

use nova_agent_runtime::AgentTermination;
use nova_responses::{IncompleteReason, ResponseStatus};

use crate::wire::FinishReason;

/// How a round ended, before it is flattened into a protocol status.
///
/// A thin local mirror of [`AgentTermination`] on purpose: classification is
/// this crate's decision and gets its own tests, while the port type is what
/// crosses the seam. [`Self::report`] is the single conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Termination {
    /// The model finished its turn. It may have asked for tools; that is
    /// visible in the output items and does not change the terminal status.
    Completed,
    /// The output was cut short. Carries the official reason.
    Truncated(IncompleteReason),
}

impl Termination {
    /// What the orchestrator is told.
    pub fn report(self) -> AgentTermination {
        match self {
            Self::Completed => AgentTermination::Completed,
            Self::Truncated(reason) => AgentTermination::Incomplete(reason),
        }
    }

    /// Classify a finish reason.
    ///
    /// Two subtleties, both load-bearing:
    ///
    /// - A refusal **with wording** is an answer: the model said why it
    ///   declined, and that text is its output for the turn.
    /// - A refusal **without wording** is the wire's content filter, which is
    ///   one of the two official incomplete reasons.
    pub fn classify(reason: &FinishReason) -> Self {
        match reason {
            FinishReason::Stop => Self::Completed,
            FinishReason::Length => Self::Truncated(IncompleteReason::MaxOutputTokens),
            FinishReason::Refusal { reason: Some(_) } => Self::Completed,
            FinishReason::Refusal { reason: None } => {
                Self::Truncated(IncompleteReason::ContentFilter)
            }
        }
    }

    pub fn status(self) -> ResponseStatus {
        match self {
            Self::Completed => ResponseStatus::Completed,
            Self::Truncated(_) => ResponseStatus::Incomplete,
        }
    }

    /// The official reason, present exactly when the status is `incomplete`.
    pub fn incomplete_reason(self) -> Option<IncompleteReason> {
        match self {
            Self::Completed => None,
            Self::Truncated(reason) => Some(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_normal_stop_completes() {
        let termination = Termination::classify(&FinishReason::Stop);
        assert_eq!(termination, Termination::Completed);
        assert_eq!(termination.status(), ResponseStatus::Completed);
        assert_eq!(termination.incomplete_reason(), None);
    }

    #[test]
    fn a_round_that_asked_for_tools_also_completes() {
        // The wire reports `tool_calls`, which the transport maps to Stop
        // because the calls themselves are the signal. Asserted here too so
        // the intent survives a future refactor of either side.
        assert_eq!(
            Termination::classify(&FinishReason::Stop).status(),
            ResponseStatus::Completed,
            "a pending tool call is output, not an unfinished response"
        );
    }

    #[test]
    fn truncation_is_incomplete_with_the_official_reason() {
        let termination = Termination::classify(&FinishReason::Length);
        assert_eq!(termination.status(), ResponseStatus::Incomplete);
        assert_eq!(
            termination.incomplete_reason(),
            Some(IncompleteReason::MaxOutputTokens)
        );
    }

    #[test]
    fn a_filtered_round_is_incomplete_for_content_filter() {
        let termination = Termination::classify(&FinishReason::Refusal { reason: None });
        assert_eq!(termination.status(), ResponseStatus::Incomplete);
        assert_eq!(
            termination.incomplete_reason(),
            Some(IncompleteReason::ContentFilter)
        );
    }

    #[test]
    fn a_spoken_refusal_is_a_completed_answer() {
        let termination = Termination::classify(&FinishReason::Refusal {
            reason: Some("I will not do that".to_string()),
        });
        assert_eq!(
            termination.status(),
            ResponseStatus::Completed,
            "the model produced its refusal as output"
        );
        assert_eq!(termination.incomplete_reason(), None);
    }

    #[test]
    fn a_reason_accompanies_incomplete_and_nothing_else() {
        // The invariant the four-state split rests on: status and reason are
        // one fact, so they cannot disagree.
        for reason in [
            FinishReason::Stop,
            FinishReason::Length,
            FinishReason::Refusal { reason: None },
            FinishReason::Refusal {
                reason: Some("no".to_string()),
            },
        ] {
            let termination = Termination::classify(&reason);
            assert_eq!(
                termination.incomplete_reason().is_some(),
                termination.status() == ResponseStatus::Incomplete,
                "status and reason disagree for {reason:?}"
            );
        }
    }
}

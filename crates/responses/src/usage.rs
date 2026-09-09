//! Token accounting.

use serde::{Deserialize, Serialize};

/// Token accounting. Integer typed throughout — never routed through `f64`,
/// which would silently lose precision on large counts (D22).
///
/// `total_tokens` is **derived, never stored**: a stored total that disagreed with
/// `input + output` would be two sources of truth for one fact. It appears on the
/// wire for OpenAI compatibility only, and is ignored on read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "UsageWire", from = "UsageWire")]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// The wire shape, carrying the derived total. Private: it exists so the
/// conversion is declarative rather than two hand-written serde impls.
#[derive(Serialize, Deserialize)]
struct UsageWire {
    input_tokens: u64,
    output_tokens: u64,
    /// Written for compatibility, ignored on read — the stored fields are
    /// authoritative.
    #[serde(default, skip_deserializing)]
    total_tokens: u64,
}

impl From<Usage> for UsageWire {
    fn from(u: Usage) -> Self {
        Self {
            input_tokens: u.input_tokens,
            output_tokens: u.output_tokens,
            total_tokens: u.total_tokens(),
        }
    }
}

impl From<UsageWire> for Usage {
    fn from(w: UsageWire) -> Self {
        Usage::new(w.input_tokens, w.output_tokens)
    }
}

impl Usage {
    pub fn new(input_tokens: u64, output_tokens: u64) -> Self {
        Self {
            input_tokens,
            output_tokens,
        }
    }

    pub fn total_tokens(self) -> u64 {
        self.input_tokens.saturating_add(self.output_tokens)
    }

    pub fn is_zero(self) -> bool {
        self.input_tokens == 0 && self.output_tokens == 0
    }

    /// Accumulate across attempts so a mid-flight abort still contributes to
    /// billing (CR-11 / INV-51). Named `accumulate` rather than implementing
    /// `std::ops::Add`: the semantics are saturating, which the operator would
    /// not advertise.
    pub fn accumulate(self, other: Usage) -> Self {
        Self::new(
            self.input_tokens.saturating_add(other.input_tokens),
            self.output_tokens.saturating_add(other.output_tokens),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn totals_and_accumulates() {
        let a = Usage::new(3, 4);
        assert_eq!(a.total_tokens(), 7);
        assert_eq!(a.accumulate(Usage::new(1, 1)), Usage::new(4, 5));
        assert!(Usage::default().is_zero());
        assert!(!a.is_zero());
    }

    #[test]
    fn saturates_instead_of_overflowing() {
        let max = Usage::new(u64::MAX, u64::MAX);
        assert_eq!(max.accumulate(Usage::new(1, 1)), max);
        assert_eq!(max.total_tokens(), u64::MAX);
    }

    #[test]
    fn serialises_the_derived_total_and_ignores_it_on_read() {
        let json = serde_json::to_value(Usage::new(2, 3)).unwrap();
        assert_eq!(json["total_tokens"], 5);

        // A stored total that disagrees must not win: the parts are the source of
        // truth, so the total is recomputed.
        let parsed: Usage =
            serde_json::from_str(r#"{"input_tokens":2,"output_tokens":3,"total_tokens":99}"#)
                .unwrap();
        assert_eq!(parsed, Usage::new(2, 3));
        assert_eq!(parsed.total_tokens(), 5);
    }
}

//! Model-free implementations of [`CompletionsRequestScheduler`].
//!
//! An adapter, in the same sense as `adapters-mem`: it satisfies a port without
//! the production carrier behind it. `adapters-mem` gives L0–L2 a substrate with
//! no database; this gives them one with no model.
//!
//! # Why this is worth a crate
//!
//! Every integration test that spans more than one turn would otherwise pay
//! latency, money and non-determinism for output nobody asserts on. Worse, the
//! *interesting* failures would stay unreachable: a refusal, a truncated answer, a
//! stall past the deadline, a malformed tool call are all routine in production and
//! all awkward to provoke from a live provider — so in a system that only ever
//! talks to a real one, the code handling them is the least tested.
//!
//! | scheduler | answers with | use |
//! |---|---|---|
//! | [`EchoScheduler`] | the last user message, echoed | smoke tests, multi-turn visibility |
//! | [`ScriptedScheduler`] | whatever the rules declare | asserting on specific behaviour, including failure |
//!
//! Neither performs IO. Pacing, retries and connection reuse belong to a real
//! adapter, because they are properties of a transport rather than of an answer.

mod echo;
mod scripted;

pub use echo::EchoScheduler;
pub use scripted::{Match, Script, ScriptRule, ScriptedScheduler};

/// Split text into roughly `n` pieces, preserving content exactly.
///
/// Shared so every mock scheduler fragments the same way. Splits on `char`
/// boundaries: cutting inside a multi-byte scalar would corrupt the text, whereas
/// cutting between combining marks is legitimate — the receiver reassembles before
/// interpreting, which the canonicalisation property tests in `core` pin down.
pub(crate) fn chunk_text(text: &str, n: usize) -> Vec<String> {
    if n <= 1 || text.is_empty() {
        return vec![text.to_string()];
    }
    let chars: Vec<char> = text.chars().collect();
    let size = chars.len().div_ceil(n).max(1);
    chars
        .chunks(size)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunking_is_lossless_at_every_count() {
        // Streamed text and submitted output would otherwise disagree for reasons
        // that look like a storage bug.
        for text in [
            "hello world",
            "你好，世界",
            "cafe\u{0301} au lait",
            "\u{1f469}\u{200d}\u{1f4bb}",
            "x",
            "",
        ] {
            for n in [0, 1, 2, 3, 8, 100] {
                let joined: String = chunk_text(text, n).concat();
                assert_eq!(joined, text, "chunking {text:?} with n={n} lost content");
            }
        }
    }

    #[test]
    fn chunking_never_yields_an_empty_piece_for_nonempty_text() {
        // An empty delta is legal on the wire but useless, and it makes delta
        // counts in assertions misleading.
        for n in [2, 3, 8] {
            for piece in chunk_text("hello", n) {
                assert!(!piece.is_empty());
            }
        }
    }
}

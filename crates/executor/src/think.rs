//! Splitting a chain of thought the provider inlined in the body.
//!
//! Some deployments ship the CoT in the reply itself and leave
//! `reasoning_content` permanently null — Hy3 does exactly that: its whole
//! answer arrives as `<thought>…</thought></think><reply>`, with no opening
//! marker at all and a closing one whose suffix is deployment-specific
//! (`</think>`, `</think:abcd>`). Read literally, the thinking is then part of
//! the answer the user sees.
//!
//! [`ThinkSplitter`] holds the leading block back until it can be classified:
//! everything before the closing marker is reasoning, everything after is the
//! reply. Two properties make it streamable:
//!
//! - The closing marker arrives LAST, so the leading block is classified on
//!   trust — this deployment is ours and always thinks when it is asked to.
//!   Waiting for proof would only cost the streamed feel of the reasoning. A
//!   turn that never asked the model to think cannot be receiving a CoT and is
//!   left alone, rather than stalling the whole reply behind a marker that is
//!   never coming.
//! - A marker can straddle chunks, so a tail that could still become one is
//!   withheld until more arrives.

/// One classified piece of a provider's body.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Split {
    /// Part of the chain of thought.
    Think(String),
    /// Part of the reply.
    Text(String),
}

/// Classifies a streamed body into reasoning before the closing think marker
/// and reply after it.
pub(crate) struct ThinkSplitter {
    /// Body held until it can be classified.
    pending: String,
    /// The closing marker was seen: everything from here on is reply.
    closed: bool,
    /// Set when this turn is not splitting — the provider carries its own
    /// reasoning field, or nothing asked the model to think.
    disabled: bool,
    /// Something was already classified as reasoning, so a leading opening
    /// marker is no longer expected.
    emitted: bool,
}

impl ThinkSplitter {
    /// `thinking` is whether this turn asked the model to think. A turn that did
    /// not cannot be receiving a CoT, and holding its body would stall the
    /// whole reply behind a marker that is never coming.
    pub(crate) fn new(thinking: bool) -> Self {
        Self {
            pending: String::new(),
            closed: false,
            disabled: !thinking,
            emitted: false,
        }
    }

    /// A provider carrying reasoning in its own field needs no splitting, and
    /// whatever is already held is reply text.
    pub(crate) fn disable(&mut self) -> Vec<Split> {
        self.disabled = true;
        self.release()
    }

    pub(crate) fn push(&mut self, piece: &str) -> Vec<Split> {
        self.pending.push_str(piece);
        if self.disabled || self.closed {
            return self.release();
        }
        if let Some((start, end)) = close_marker(&self.pending) {
            self.closed = true;
            let thought = self.pending[..start].to_string();
            self.pending = self.pending[end..].to_string();
            let mut out = Vec::new();
            let thought = strip_open_marker(&thought);
            if !thought.is_empty() {
                self.emitted = true;
                out.push(Split::Think(thought.to_string()));
            }
            out.extend(self.release());
            return out;
        }
        // Everything but a tail that could still turn into a marker: a marker
        // opens with `<`, and one can straddle chunks.
        let cut = self.emittable();
        if cut == 0 {
            return Vec::new();
        }
        let thought = self.pending[..cut].to_string();
        self.pending = self.pending[cut..].to_string();
        let thought = strip_open_marker(&thought);
        if thought.is_empty() {
            return Vec::new();
        }
        self.emitted = true;
        vec![Split::Think(thought.to_string())]
    }

    pub(crate) fn finish(mut self) -> Vec<Split> {
        self.release()
    }

    fn release(&mut self) -> Vec<Split> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        vec![Split::Text(std::mem::take(&mut self.pending))]
    }

    /// Bytes of the held block that can be classified right now.
    fn emittable(&self) -> usize {
        let text = self.pending.as_str();
        if text.is_empty() {
            return 0;
        }
        // An opening marker is dropped rather than shown, and it can arrive
        // split too, so an incomplete one at the head is withheld.
        if !self.emitted && text.trim_start().starts_with("<think") && !text.contains('>') {
            return 0;
        }
        match text.rfind('<') {
            Some(start) if is_marker_tail(&text[start..]) => start,
            _ => text.len(),
        }
    }
}

/// Byte range of a COMPLETE closing think marker. The suffix after `</think` is
/// deployment-specific, so it is matched to the closing `>` rather than spelled
/// out.
fn close_marker(text: &str) -> Option<(usize, usize)> {
    let start = text.find("</think")?;
    let end = text[start..].find('>')? + start + 1;
    Some((start, end))
}

/// Whether this tail may still become a marker once more chunks arrive.
fn is_marker_tail(tail: &str) -> bool {
    "</think".starts_with(tail) || (tail.starts_with("</think") && !tail.contains('>'))
}

/// Drop a leading opening marker: it delimits the CoT, it is not part of it.
/// Hy3 sends none, delimiting by the closing one alone.
fn strip_open_marker(text: &str) -> &str {
    let rest = text.trim_start();
    match rest.find("<think") {
        Some(0) => rest.find('>').map_or(text, |end| &rest[end + 1..]),
        _ => text,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Push `pieces` one at a time — a stream arrives in chunks a provider
    /// chooses, never in the pieces that suit the reader — and collect what was
    /// classified.
    fn split_all(pieces: &[&str], thinking: bool) -> (String, String) {
        let mut splitter = ThinkSplitter::new(thinking);
        let mut think = String::new();
        let mut text = String::new();
        for piece in pieces {
            for part in splitter.push(piece) {
                match part {
                    Split::Think(part) => think.push_str(&part),
                    Split::Text(part) => text.push_str(&part),
                }
            }
        }
        for part in splitter.finish() {
            if let Split::Text(part) = part {
                text.push_str(&part);
            }
        }
        (think, text)
    }

    /// Hy3 as it answers today: no opening marker, the thought first, `</think>`
    /// last, then the reply.
    #[test]
    fn inline_thinking_is_reasoning_and_the_reply_stays_body() {
        let (think, text) = split_all(
            &[
                "The user is asking. ",
                "1 + 1 = 2 is all.",
                "</think>",
                "1 + 1 = 2",
            ],
            true,
        );
        assert_eq!(think, "The user is asking. 1 + 1 = 2 is all.");
        assert_eq!(text, "1 + 1 = 2");
    }

    /// The marker is cut wherever the chunks fall: `</thin` + `k>` is one
    /// marker, and the thought must not leak into the reply while it is held.
    #[test]
    fn a_marker_straddling_chunks_is_one_marker() {
        let (think, text) = split_all(&["pondering", "</thi", "nk>", "the reply"], true);
        assert_eq!(think, "pondering");
        assert_eq!(text, "the reply");
    }

    /// The suffix after `</think` is deployment-specific.
    #[test]
    fn a_suffixed_marker_closes_the_thought() {
        let (think, text) = split_all(&["pondering", "</think:abcd>", "the reply"], true);
        assert_eq!(think, "pondering");
        assert_eq!(text, "the reply");
    }

    /// A provider that carries reasoning in its own field has nothing to split,
    /// and a body already held is released as reply.
    #[test]
    fn a_provider_with_its_own_reasoning_field_stops_splitting() {
        let mut splitter = ThinkSplitter::new(true);
        splitter.disable();
        assert!(
            matches!(splitter.push("held").as_slice(), [Split::Text(body)] if body == "held"),
            "a provider with its own field has nothing to split"
        );
        assert!(splitter.finish().is_empty(), "nothing is held back");
    }

    /// Streamed as it arrives, not dumped when the marker finally closes: the
    /// marker is proof, but it comes last, and waiting for it would cost the
    /// whole point of streaming.
    #[test]
    fn a_thinking_turn_streams_the_cot_as_it_arrives() {
        let mut splitter = ThinkSplitter::new(true);
        let mut think = String::new();
        for piece in ["The", " user", " says hi", ".</thi", "nk:1>", "Hi!"] {
            for part in splitter.push(piece) {
                if let Split::Think(part) = part {
                    think.push_str(&part);
                }
            }
        }
        assert_eq!(think, "The user says hi.");
    }

    /// A turn that never asked the model to think cannot be holding its body
    /// back: there is no marker coming, so every delta must stream through.
    #[test]
    fn thinking_off_never_holds_the_body() {
        let mut splitter = ThinkSplitter::new(false);
        assert!(
            matches!(splitter.push("Hello").as_slice(), [Split::Text(body)] if body == "Hello")
        );
    }

    /// The accepted gamble: a turn that asked the model to think gets its leading
    /// block treated as thinking even if the marker never comes. Hy3 always
    /// thinks when it is asked to, so the marker is a formality, not a question.
    #[test]
    fn a_turn_asked_to_think_treats_the_leading_block_as_thinking() {
        let (think, text) = split_all(&["Just the answer."], true);
        assert_eq!(think, "Just the answer.");
        assert!(text.is_empty());
    }

    /// An opening marker, when a deployment sends one, delimits the thought
    /// rather than becoming part of it.
    #[test]
    fn an_opening_marker_is_not_part_of_the_thought() {
        let (think, text) = split_all(&["<think:1>", "pondering", "</think:1>", "the reply"], true);
        assert_eq!(think, "pondering");
        assert_eq!(text, "the reply");
    }
}

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
//! reply. A turn may think more than once — Hy3 interleaves a short thought with
//! each tool call — so several segments can appear in one body and the reply is
//! only what follows the last closing marker. Two properties make it
//! streamable:
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

/// Classifies a streamed body into reasoning and reply.
///
/// A deployment that inlines its CoT delimits it with a closing marker only, so
/// the shape is `thought</think>reply` and a body that opens with a marker has
/// no thought in it at all. The model sometimes goes further off-format — it
/// closes an empty thought, thinks, and closes again — and splitting at the
/// first marker would then show that second thought, marker included, as reply.
/// Such a body is held and split at its *last* marker instead of streamed.
pub(crate) struct ThinkSplitter {
    /// Body held until it can be classified.
    pending: String,
    /// What the held body is: reasoning, or reply.
    thinking: bool,
    /// Off-format body: nothing is emitted until `finish`, which splits the
    /// whole body at its last marker. Set by a marker with no thought before it
    /// (the turn opened by closing a thought it never wrote), or by any marker
    /// met while already replying.
    buffered: bool,
    /// Set when this turn is not splitting — the provider carries its own
    /// reasoning field, or nothing asked the model to think.
    disabled: bool,
    /// The closing marker this turn used, verbatim, suffix included.
    close_marker: Option<String>,
    /// Whether the current segment already produced reasoning. A marker with
    /// nothing in front of it is only "unopened" when this is false: the
    /// thought it closes may simply have arrived in an earlier chunk.
    thought_emitted: bool,
}

impl ThinkSplitter {
    /// `thinking` is whether this turn asked the model to think. A turn that did
    /// not cannot be receiving a CoT, and holding its body would stall the
    /// whole reply behind a marker that is never coming.
    ///
    /// `open` is what a thought opens with on this endpoint, `""` when thoughts
    /// announce themselves only by closing — Hy3, whose body is
    /// `thought</think>reply`. Such a turn can only be read on trust, so its
    /// leading block is taken as reasoning. An endpoint whose thoughts DO open
    /// with a marker needs no trust: everything before the first marker is
    /// reply, which is what keeps a turn that never thought from being read as
    /// one.
    pub(crate) fn new(thinking: bool, open: &str) -> Self {
        let announced = !open.trim().is_empty();
        Self {
            pending: String::new(),
            thinking: thinking && !announced,
            buffered: false,
            disabled: !thinking,
            close_marker: None,
            thought_emitted: false,
        }
    }

    /// A provider carrying reasoning in its own field needs no splitting, and
    /// whatever is already held is reply text.
    pub(crate) fn disable(&mut self) -> Vec<Split> {
        self.disabled = true;
        self.thinking = false;
        self.release()
    }

    /// The closing marker this turn used, suffix included — `</think:abcd>` on a
    /// deployment that suffixes it. Recording it beats guessing: replaying a
    /// thought has to wrap it in the very marker this server emits.
    pub(crate) fn close_marker(&self) -> Option<&str> {
        self.close_marker.as_deref()
    }

    pub(crate) fn push(&mut self, piece: &str) -> Vec<Split> {
        self.pending.push_str(piece);
        if self.disabled {
            return self.release();
        }
        if self.buffered {
            return Vec::new();
        }
        let mut out = Vec::new();
        // One pass per segment the held body already contains.
        loop {
            if self.thinking {
                let Some((start, end)) = close_marker(&self.pending) else {
                    break;
                };
                let whole = std::mem::take(&mut self.pending);
                self.close_marker = Some(whole[start..end].to_string());
                let before = &whole[..start];
                self.pending = whole[end..].to_string();
                if before.trim().is_empty() && !self.thought_emitted {
                    // Nothing was thought: this turn opened by closing a
                    // thought it never wrote. Hold the rest and split it at its
                    // last marker rather than call everything after it reply.
                    self.buffered = true;
                    return out;
                }
                self.thinking = false;
                self.thought_emitted = false;
                let thought = strip_markers(before);
                if !thought.is_empty() {
                    out.push(Split::Think(thought));
                }
            } else if let Some((start, end)) = close_marker(&self.pending) {
                // A marker while replying is off-format as well: what follows
                // it is a thought the model forgot to open. Hold it; showing
                // the marker as prose is never right.
                let whole = std::mem::take(&mut self.pending);
                self.close_marker = Some(whole[start..end].to_string());
                let before = whole[..start].to_string();
                self.pending = whole[end..].to_string();
                self.buffered = true;
                if !before.is_empty() {
                    out.push(Split::Text(before));
                }
                return out;
            } else {
                // Another thought can open after the close; only a complete
                // marker starts one, and the text before it is reply.
                let Some(open) = open_marker(&self.pending) else {
                    break;
                };
                let whole = std::mem::take(&mut self.pending);
                let reply = whole[..open.start].to_string();
                self.pending = whole[open.end..].to_string();
                self.thinking = true;
                self.thought_emitted = false;
                if !reply.is_empty() {
                    out.push(Split::Text(reply));
                }
            }
        }
        // Everything but a tail that could still become a marker: one opens with
        // `<`, and one can straddle chunks.
        let cut = self.emittable();
        if cut == 0 {
            return out;
        }
        let held = self.pending[..cut].to_string();
        self.pending = self.pending[cut..].to_string();
        if self.thinking {
            let held = strip_markers(&held);
            if !held.is_empty() {
                self.thought_emitted = true;
                out.push(Split::Think(held));
            }
        } else {
            out.push(Split::Text(held));
        }
        out
    }

    pub(crate) fn finish(mut self) -> Vec<Split> {
        if self.buffered {
            return self.release_buffered();
        }
        self.release()
    }

    /// The held off-format body: thought up to its last marker, reply after it,
    /// with no marker left in either.
    fn release_buffered(&mut self) -> Vec<Split> {
        let body = std::mem::take(&mut self.pending);
        let (thought, reply) = match last_close_marker(&body) {
            Some((start, end)) => {
                self.close_marker = Some(body[start..end].to_string());
                (strip_markers(&body[..start]), strip_markers(&body[end..]))
            }
            None => (String::new(), body),
        };
        let mut out = Vec::new();
        if !thought.is_empty() {
            out.push(Split::Think(thought));
        }
        if !reply.is_empty() {
            out.push(Split::Text(reply));
        }
        out
    }

    fn release(&mut self) -> Vec<Split> {
        if self.pending.is_empty() {
            return Vec::new();
        }
        let pending = std::mem::take(&mut self.pending);
        // A tail the stream ended inside is not prose either.
        let body = match pending.rfind('<') {
            Some(start) if is_marker_tail(&pending[start..], self.thinking) => &pending[..start],
            _ => pending.as_str(),
        };
        if body.is_empty() {
            return Vec::new();
        }
        // A thought the turn never closed is a thought that ran out of stream.
        // It was classified on the way in — by this endpoint's marker, or by
        // the trust an inlined CoT requires — and reclassifying the tail here
        // would split one answer across the thinking panel and the reply.
        if self.thinking {
            let thought = strip_markers(body);
            if thought.is_empty() {
                return Vec::new();
            }
            return vec![Split::Think(thought)];
        }
        vec![Split::Text(body.to_string())]
    }

    /// Bytes of the held block that can be classified right now.
    fn emittable(&self) -> usize {
        let text = self.pending.as_str();
        if text.is_empty() {
            return 0;
        }
        match text.rfind('<') {
            Some(start) if is_marker_tail(&text[start..], self.thinking) => start,
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

/// Whether this tail may still become a marker once more chunks arrive. While
/// thinking only a closing marker ends the thought; while replying either one
/// matters, since a closing marker there is what an unopened thought looks like.
fn is_marker_tail(tail: &str, thinking: bool) -> bool {
    let markers: &[&str] = if thinking {
        &["</think"]
    } else {
        &["</think", "<think"]
    };
    markers.iter().any(|marker| {
        (!tail.is_empty() && marker.starts_with(tail))
            || (tail.starts_with(marker) && !tail.contains('>'))
    })
}

/// Byte range of the LAST complete closing marker in `text`, if any.
fn last_close_marker(text: &str) -> Option<(usize, usize)> {
    let mut found = None;
    let mut from = 0;
    while let Some(offset) = text[from..].find("</think") {
        let start = from + offset;
        let Some(close) = text[start..].find('>') else {
            break;
        };
        found = Some((start, start + close + 1));
        from = start + close + 1;
    }
    found
}

/// Drop every complete think marker from `text`: a marker delimits, it is never
/// part of what the user reads.
fn strip_markers(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    loop {
        let next = ["</think", "<think"]
            .iter()
            .filter_map(|marker| rest.find(marker).map(|at| (at, *marker)))
            .min();
        let Some((at, marker)) = next else {
            out.push_str(rest);
            return out;
        };
        out.push_str(&rest[..at]);
        let tail = &rest[at + marker.len()..];
        match tail.find('>') {
            Some(close) => rest = &tail[close + 1..],
            // An unterminated marker is not one: keep it as text.
            None => {
                out.push_str(&rest[at..]);
                return out;
            }
        }
    }
}

/// Byte range of a COMPLETE opening marker at the head of `text`. Only a whole
/// one counts: a partial `<thi` has to wait for more chunks.
fn open_marker(text: &str) -> Option<std::ops::Range<usize>> {
    let rest = text.trim_start();
    let offset = text.len() - rest.len();
    if !rest.starts_with("<think") {
        return None;
    }
    let end = rest.find('>')? + 1;
    Some(offset..offset + end)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Push `pieces` one at a time — a stream arrives in chunks a provider
    /// chooses, never in the pieces that suit the reader — and collect what was
    /// classified.
    fn split_all(pieces: &[&str], thinking: bool) -> (String, String) {
        split_all_opening(pieces, thinking, "")
    }

    /// [`split_all`] on an endpoint whose thoughts open with `open`, `<think>`
    /// being the one the protocol's own deployments use.
    fn split_all_opening(pieces: &[&str], thinking: bool, open: &str) -> (String, String) {
        let mut splitter = ThinkSplitter::new(thinking, open);
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
            match part {
                Split::Think(part) => think.push_str(&part),
                Split::Text(part) => text.push_str(&part),
            }
        }
        (think, text)
    }

    /// The closing marker a turn used, verbatim; `None` when no segment closed.
    fn marker_of(pieces: &[&str]) -> Option<String> {
        let mut splitter = ThinkSplitter::new(true, "");
        for piece in pieces {
            splitter.push(piece);
        }
        splitter.close_marker().map(str::to_string)
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
        let mut splitter = ThinkSplitter::new(true, "");
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
        let mut splitter = ThinkSplitter::new(true, "");
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
        let mut splitter = ThinkSplitter::new(false, "");
        assert!(
            matches!(splitter.push("Hello").as_slice(), [Split::Text(body)] if body == "Hello")
        );
    }

    /// An endpoint whose thoughts open with a marker needs no trust: a body
    /// with no marker anywhere is the reply. This is the shape that used to
    /// hide whole answers in the thinking panel.
    #[test]
    fn a_body_with_no_marker_is_the_reply_when_thoughts_announce_themselves() {
        let (think, text) = split_all_opening(&["Just the answer."], true, "<think>");
        assert!(think.is_empty());
        assert_eq!(text, "Just the answer.");
    }

    /// The same body on Hy3, whose thoughts only close: there is nothing to
    /// announce them, so the leading block is taken on trust.
    #[test]
    fn a_body_with_no_marker_is_reasoning_on_an_endpoint_that_only_closes() {
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

    /// The deployment's off-format body, as a live turn produced it: the model
    /// closed a thought it never wrote, thought, then closed again. Splitting at
    /// the first marker put that second thought *and* its marker into the reply.
    #[test]
    fn a_body_that_opens_by_closing_an_unwritten_thought_splits_at_its_last_marker() {
        let (think, text) = split_all(
            &[
                "</think:6124c78e>Asset card locked? Let me check.\n\nActually: generate",
                " serially.</think:6124c78e>Asset card locked: asset_87fd.",
            ],
            true,
        );
        assert_eq!(
            think,
            "Asset card locked? Let me check.\n\nActually: generate serially."
        );
        assert_eq!(text, "Asset card locked: asset_87fd.");
        assert!(!text.contains("</think"), "no marker may reach the reply");
    }

    /// The common off-format turn: the model closes an empty thought and replies.
    /// Nothing was thought, and the marker is not prose.
    #[test]
    fn a_body_that_only_closes_an_empty_thought_is_all_reply() {
        let (think, text) = split_all(&["</think:6124c78e>", "设定图: asset_2e30"], true);
        assert_eq!(think, "");
        assert_eq!(text, "设定图: asset_2e30");
    }

    /// A body that is nothing but a marker: the model said nothing at all.
    #[test]
    fn a_body_that_is_only_a_marker_says_nothing() {
        let (think, text) = split_all(&["</think:6124c78e>"], true);
        assert_eq!(think, "");
        assert_eq!(text, "");
    }

    /// A marker met while replying starts a thought the model forgot to open:
    /// that segment is reasoning, the text on either side of it is reply, and
    /// the marker itself is never prose.
    #[test]
    fn a_marker_met_while_replying_opens_a_thought_segment() {
        let (think, text) = split_all(
            &[
                "thought one</think:6124c78e>reply one</think:6124c78e>",
                "thinking again</think:6124c78e>reply two",
            ],
            true,
        );
        assert_eq!(think, "thought onethinking again");
        assert_eq!(text, "reply onereply two");
    }

    /// A stray marker split across chunks is never emitted as prose either.
    #[test]
    fn a_stray_marker_split_across_chunks_still_drops() {
        let (think, text) = split_all(
            &["</thi", "nk:6124c78e>reply", "</thi", "nk:6124c78e>more"],
            true,
        );
        assert_eq!(think, "reply");
        assert_eq!(text, "more");
    }

    /// A tool loop thinks once per tool, so one body can hold several segments
    /// and the reply is only what follows the last closing marker. Stopping at
    /// the first close left every later thought shown as reply.
    #[test]
    fn a_second_thought_after_the_first_close_is_reasoning_too() {
        let (think, text) = split_all(
            &[
                "first thought",
                "</think:6124c78e>first answer",
                "<think>second thought",
                "</think:6124c78e>second answer",
            ],
            true,
        );
        assert_eq!(think, "first thoughtsecond thought");
        assert_eq!(text, "first answersecond answer");
    }

    /// A thought the model opened and never closed is reasoning wherever the
    /// stream stopped: its own marker is proof the turn delimits thoughts.
    #[test]
    fn an_opened_but_unclosed_thought_stays_reasoning() {
        let (think, text) = split_all_opening(&["<think>still pondering"], true, "<think>");
        assert_eq!(think, "still pondering");
        assert_eq!(text, "");
    }

    /// The same body on an endpoint whose thoughts announce themselves: no
    /// marker means nothing was thought.
    #[test]
    fn an_unclosed_body_is_the_reply_when_thoughts_announce_themselves() {
        let (think, text) = split_all_opening(&["still pondering"], true, "<think>");
        assert!(think.is_empty());
        assert_eq!(text, "still pondering");
    }

    /// A stream that ends inside a marker: the tail is not prose either.
    #[test]
    fn a_marker_the_stream_ended_inside_never_reaches_the_reply() {
        let (think, text) = split_all(&["ponder</think>the reply", "</thi"], true);
        assert_eq!(think, "ponder");
        assert_eq!(text, "the reply");
    }

    /// An opening marker anywhere in the body delimits, it is not shown.
    #[test]
    fn a_thought_opening_within_the_body_is_dropped() {
        let (think, text) = split_all(&["<think>ponder", "ing</think>reply"], true);
        assert_eq!(think, "pondering");
        assert_eq!(text, "reply");
    }

    /// The suffix is deployment-specific, so replaying a thought has to use the
    /// marker this server actually emits rather than the plain one.
    #[test]
    fn the_close_marker_is_reported_verbatim() {
        assert_eq!(
            marker_of(&["ponder", "</thi", "nk:abcd>", "reply"]),
            Some("</think:abcd>".to_string())
        );
        assert_eq!(marker_of(&["no marker here"]), None);
    }

    #[test]
    fn probe_close_marker_only_segments() {
        let (think, text) = split_all(&["seg1", "</think:abcd>seg2", "</think:abcd>reply"], true);
        println!("think={think:?} text={text:?}");
    }
}

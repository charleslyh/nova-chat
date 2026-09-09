//! The context a generation inherits: history in chronological order.

use serde::{Deserialize, Serialize};

use crate::protocol::ResponseItem;

/// One position in a conversation's history: an item, plus the reasoning block
/// that precedes it if there is one.
///
/// The reasoning used to travel as a second `Vec<Option<String>>` that had to
/// stay the same length as the items. Nothing enforced that, and one producer
/// already got it wrong: it returned items with an empty reasoning vector, and the
/// consumer `zip`ped the two — silently dropping the entire transcript. Fusing
/// them removes the alignment as a thing that can be wrong.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextEntry {
    pub item: ResponseItem,
    /// Render-only: a model does not read its own thinking, so this is never fed
    /// back into model context.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
}

impl ContextEntry {
    pub fn new(item: ResponseItem) -> Self {
        Self {
            item,
            reasoning: None,
        }
    }

    pub fn with_reasoning(item: ResponseItem, reasoning: Option<String>) -> Self {
        Self { item, reasoning }
    }
}

/// Resolved history, oldest first.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ResolvedContext {
    pub entries: Vec<ContextEntry>,
    /// Completed turns behind this context. Not derivable from the entries — a
    /// turn contributes a variable number of items — so it is carried.
    pub turns: usize,
}

impl ResolvedContext {
    pub fn new(entries: Vec<ContextEntry>, turns: usize) -> Self {
        Self { entries, turns }
    }

    /// Build from bare items, for a source that has no reasoning to report.
    pub fn from_items(items: impl IntoIterator<Item = ResponseItem>, turns: usize) -> Self {
        Self::new(items.into_iter().map(ContextEntry::new).collect(), turns)
    }

    pub fn items(&self) -> impl Iterator<Item = &ResponseItem> {
        self.entries.iter().map(|e| &e.item)
    }

    pub fn into_items(self) -> Vec<ResponseItem> {
        self.entries.into_iter().map(|e| e.item).collect()
    }

    pub fn item_count(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Byte cost of the history, **computed** rather than carried: a stored size
    /// beside the items it describes is a second source of truth that goes stale
    /// the moment anything is appended.
    pub fn bytes(&self) -> usize {
        self.entries
            .iter()
            .map(|e| e.item.byte_len())
            .fold(0usize, |acc, len| acc.saturating_add(len))
    }

    pub fn extend(&mut self, other: ResolvedContext) {
        self.entries.extend(other.entries);
        self.turns = self.turns.saturating_add(other.turns);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_track_the_items_without_a_second_field_to_maintain() {
        let mut ctx = ResolvedContext::from_items([ResponseItem::user_text("abcd")], 1);
        assert_eq!(ctx.bytes(), 4);
        ctx.entries.push(ContextEntry::new(ResponseItem::user_text("ef")));
        // No `bytes` field to forget to update.
        assert_eq!(ctx.bytes(), 6);
        assert_eq!(ctx.item_count(), 2);
    }

    #[test]
    fn reasoning_cannot_be_misaligned_with_items() {
        let ctx = ResolvedContext::new(
            vec![
                ContextEntry::new(ResponseItem::user_text("q")),
                ContextEntry::with_reasoning(
                    ResponseItem::assistant_text("a"),
                    Some("thinking".into()),
                ),
            ],
            1,
        );
        // Every item is reachable regardless of how many carry reasoning — the
        // parallel-array version dropped all of them when the lengths differed.
        assert_eq!(ctx.items().count(), 2);
        let annotated: Vec<_> = ctx
            .entries
            .iter()
            .filter(|e| e.reasoning.is_some())
            .collect();
        assert_eq!(annotated.len(), 1);
    }

    #[test]
    fn from_items_is_the_no_reasoning_case() {
        let ctx = ResolvedContext::from_items([ResponseItem::user_text("x")], 1);
        assert!(ctx.entries.iter().all(|e| e.reasoning.is_none()));
        assert_eq!(ctx.into_items().len(), 1);
    }
}

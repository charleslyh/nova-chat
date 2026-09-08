//! Canonicalisation for stable content fingerprints (INV-44).
//!
//! Without canonicalisation a fingerprint is useless: the same logical content
//! serialised twice can differ in key order, whitespace or Unicode composition,
//! producing false integrity failures.
//!
//! Rules:
//! - object keys sorted lexicographically by byte value
//! - compact separators (no insignificant whitespace)
//! - all strings normalised to NFC
//!
//! Recursion depth is bounded by the parser limit and by
//! [`crate::protocol::InputLimits::validate_depth`], both of which run before
//! anything reaches this module.

use serde_json::{Map, Value};
use unicode_normalization::UnicodeNormalization;

use crate::protocol::ResponseItem;

/// Normalise a string to NFC.
pub fn nfc(input: &str) -> String {
    input.nfc().collect()
}

/// Deterministic JSON encoding.
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => write_json_string(&nfc(s), out),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (key, val)) in sorted_entries(map).into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_json_string(&nfc(key), out);
                out.push(':');
                write_canonical(val, out);
            }
            out.push('}');
        }
    }
}

fn sorted_entries(map: &Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<_> = map.iter().collect();
    entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    entries
}

fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Canonical encoding of an item list, used as the signing input for stored
/// input/output items.
pub fn canonical_items(items: &[ResponseItem]) -> String {
    let value = serde_json::to_value(items).unwrap_or(Value::Null);
    canonical_json(&value)
}

/// Canonical encoding of streamed output text.
///
/// Deltas are concatenated **in sequence order** and then normalised once.
/// Normalising per-delta would be wrong: a grapheme split across two deltas
/// would normalise differently than the joined text, producing a spurious
/// integrity mismatch.
pub fn canonical_output_text<'a, I>(deltas: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let joined: String = deltas.into_iter().collect();
    nfc(&joined)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{ContentPart, Role};

    #[test]
    fn sorts_object_keys_and_drops_whitespace() {
        let a: Value = serde_json::from_str(r#"{ "b": 1, "a": { "d": 2, "c": 3 } }"#).unwrap();
        let b: Value = serde_json::from_str(r#"{"a":{"c":3,"d":2},"b":1}"#).unwrap();
        assert_eq!(canonical_json(&a), canonical_json(&b));
        assert_eq!(canonical_json(&a), r#"{"a":{"c":3,"d":2},"b":1}"#);
    }

    #[test]
    fn preserves_array_order() {
        let a: Value = serde_json::from_str(r#"[1,2,3]"#).unwrap();
        let b: Value = serde_json::from_str(r#"[3,2,1]"#).unwrap();
        assert_ne!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn normalises_unicode_composition() {
        // NFC "é" (U+00E9) vs NFD "e" + combining acute (U+0065 U+0301).
        let composed = Value::String("caf\u{00e9}".to_string());
        let decomposed = Value::String("cafe\u{0301}".to_string());
        assert_eq!(canonical_json(&composed), canonical_json(&decomposed));
    }

    #[test]
    fn delta_chunk_boundaries_do_not_affect_the_fingerprint() {
        // This is the failure mode canonicalisation exists to prevent: the same
        // text arriving in different chunk splits must fingerprint identically.
        let whole = canonical_output_text(["caf\u{00e9} au lait"]);
        let split_mid_word = canonical_output_text(["caf", "\u{00e9} au", " lait"]);
        // Worst case: a combining mark separated from its base character.
        let split_mid_grapheme = canonical_output_text(["cafe", "\u{0301} au lait"]);
        assert_eq!(whole, split_mid_word);
        assert_eq!(whole, split_mid_grapheme);
    }

    #[test]
    fn escapes_control_characters_and_quotes() {
        let value = Value::String("a\"b\\c\nd\te\u{0001}".to_string());
        assert_eq!(
            canonical_json(&value),
            r#""a\"b\\c\nd\te\u0001""#
        );
    }

    #[test]
    fn item_encoding_is_stable_across_equal_values() {
        let items = vec![ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText {
                text: "cafe\u{0301}".into(),
            }],
            id: None,
            status: None,
        }];
        let other = vec![ResponseItem::Message {
            role: Role::User,
            content: vec![ContentPart::InputText {
                text: "caf\u{00e9}".into(),
            }],
            id: None,
            status: None,
        }];
        assert_eq!(canonical_items(&items), canonical_items(&other));
    }

    #[test]
    fn different_items_produce_different_encodings() {
        let a = vec![ResponseItem::user_text("a")];
        let b = vec![ResponseItem::user_text("b")];
        assert_ne!(canonical_items(&a), canonical_items(&b));
    }

    #[test]
    fn empty_inputs_are_well_defined() {
        assert_eq!(canonical_items(&[]), "[]");
        assert_eq!(canonical_output_text(Vec::<&str>::new()), "");
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;

    /// Every way of cutting a string into deltas must fingerprint identically.
    ///
    /// The existing case checks three hand-picked splits. That is enough to catch
    /// the mistake it was written for, but not enough to establish the property:
    /// the failure mode is boundary-dependent, so the interesting split is the one
    /// nobody thought of. Enumerating every boundary removes the guesswork.
    #[test]
    fn fingerprint_is_independent_of_every_possible_split_point() {
        let inputs = [
            "cafe\u{0301} au lait",        // combining acute — the classic trap
            "caf\u{00e9} au lait",         // precomposed form of the same text
            "\u{4f60}\u{597d}\u{ff0c}\u{4e16}\u{754c}", // multibyte, no combining marks
            "a\u{0301}\u{0328}b",          // two stacked combining marks
            "\u{1f469}\u{200d}\u{1f4bb}",  // ZWJ sequence (emoji), 3 scalars
            "plain ascii text",
            "",
            "x",
        ];

        for input in inputs {
            let whole = canonical_output_text([input]);
            let chars: Vec<char> = input.chars().collect();

            // Two-way splits at every character boundary.
            for cut in 0..=chars.len() {
                let head: String = chars[..cut].iter().collect();
                let tail: String = chars[cut..].iter().collect();
                let split = canonical_output_text([head.as_str(), tail.as_str()]);
                assert_eq!(
                    split, whole,
                    "split of {input:?} at char {cut} changed the fingerprint; \
                     normalising per-delta instead of after joining produces exactly \
                     this failure, and only for some boundaries"
                );
            }

            // Maximal fragmentation: one delta per character.
            let per_char: Vec<String> = chars.iter().map(|c| c.to_string()).collect();
            let refs: Vec<&str> = per_char.iter().map(String::as_str).collect();
            assert_eq!(
                canonical_output_text(refs),
                whole,
                "one-delta-per-character split of {input:?} changed the fingerprint"
            );

            // Empty deltas must not contribute, since a stream may legitimately
            // carry a zero-length payload.
            let with_empties: Vec<&str> = vec!["", input, "", ""];
            assert_eq!(canonical_output_text(with_empties), whole);
        }
    }

    #[test]
    fn normalisation_is_idempotent() {
        // Required for the fingerprint to be stable across a re-sign: if applying
        // NFC twice differed from applying it once, verifying a record after any
        // re-encoding would fail spuriously.
        for input in [
            "cafe\u{0301}",
            "caf\u{00e9}",
            "a\u{0301}\u{0328}",
            "\u{4f60}\u{597d}",
            "",
            "\u{fb01}",   // ligature: NFC leaves it alone, NFKC would not
        ] {
            let once = nfc(input);
            let twice = nfc(&once);
            assert_eq!(once, twice, "nfc is not idempotent for {input:?}");
        }
    }

    #[test]
    fn equivalent_forms_agree_and_distinct_text_does_not() {
        // The two halves matter equally. Collapsing genuinely different strings to
        // one fingerprint would make tampering undetectable — a normalisation bug
        // that is invisible if only the "equivalent forms agree" half is tested.
        assert_eq!(
            canonical_output_text(["cafe\u{0301}"]),
            canonical_output_text(["caf\u{00e9}"]),
            "canonically equivalent text must fingerprint identically"
        );
        assert_ne!(
            canonical_output_text(["cafe"]),
            canonical_output_text(["caf\u{00e9}"]),
            "different text must not collapse to the same fingerprint"
        );
        assert_ne!(
            canonical_output_text(["a b"]),
            canonical_output_text(["ab"]),
            "whitespace is content, not formatting, inside a payload"
        );
    }

    #[test]
    fn concatenation_order_is_significant() {
        // Guards against an implementation that sorts or otherwise reorders
        // deltas: streamed text is ordered, and losing that would let two
        // different outputs share a fingerprint.
        assert_ne!(
            canonical_output_text(["one", "two"]),
            canonical_output_text(["two", "one"])
        );
    }
}

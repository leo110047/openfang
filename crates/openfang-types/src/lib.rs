//! Core types and traits for the OpenFang Agent Operating System.
//!
//! This crate defines all shared data structures used across the OpenFang kernel,
//! runtime, memory substrate, and wire protocol. It contains no business logic.

pub mod agent;
pub mod approval;
pub mod capability;
pub mod commands;
pub mod comms;
pub mod config;
pub mod error;
pub mod event;
pub mod manifest_signing;
pub mod media;
pub mod memory;
pub mod message;
pub mod model_catalog;
pub mod scheduler;
pub mod serde_compat;
pub mod taint;
pub mod tool;
pub mod tool_compat;
pub mod webhook;

/// Safely truncate a string to at most `max_bytes`, never splitting a UTF-8 char.
pub fn truncate_str(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Safely truncate a string to at most `max_chars`, never splitting a Unicode
/// scalar value. This is useful for platform limits expressed in characters
/// rather than bytes.
pub fn truncate_chars(s: &str, max_chars: usize) -> String {
    let mut chars = s.chars();
    let truncated: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        truncated
    } else {
        s.to_string()
    }
}

/// Truncate to a character limit and append an ellipsis when truncation
/// happened. The returned string is never longer than `max_chars`.
pub fn truncate_chars_with_ellipsis(s: &str, max_chars: usize) -> String {
    let total = s.chars().count();
    if total <= max_chars {
        return s.to_string();
    }
    if max_chars == 0 {
        return String::new();
    }
    if max_chars == 1 {
        return "\u{2026}".to_string();
    }
    let mut out: String = s.chars().take(max_chars - 1).collect();
    out.push('\u{2026}');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_str_ascii() {
        assert_eq!(truncate_str("hello world", 5), "hello");
    }

    #[test]
    fn truncate_str_chinese() {
        // Each Chinese character is 3 bytes
        let s = "\u{4F60}\u{597D}\u{4E16}\u{754C}"; // 你好世界
        assert_eq!(truncate_str(s, 6), "\u{4F60}\u{597D}"); // 你好
        assert_eq!(truncate_str(s, 7), "\u{4F60}\u{597D}"); // still 你好 (7 is mid-char)
        assert_eq!(truncate_str(s, 9), "\u{4F60}\u{597D}\u{4E16}"); // 你好世
    }

    #[test]
    fn truncate_str_emoji() {
        let s = "hi\u{1F600}there"; // hi😀there — emoji is 4 bytes
        assert_eq!(truncate_str(s, 3), "hi"); // 3 is mid-emoji
        assert_eq!(truncate_str(s, 6), "hi\u{1F600}"); // after emoji
    }

    #[test]
    fn truncate_str_em_dash() {
        // Em dash (—) is 3 bytes (0xE2 0x80 0x94) — the exact char that caused
        // production panics in kernel.rs and session.rs (issue #104)
        let s = "Here is a summary — with details";
        assert_eq!(truncate_str(s, 19), "Here is a summary ");
        assert_eq!(truncate_str(s, 20), "Here is a summary ");
        assert_eq!(truncate_str(s, 21), "Here is a summary \u{2014}");
    }

    #[test]
    fn truncate_str_no_truncation() {
        assert_eq!(truncate_str("short", 100), "short");
    }

    #[test]
    fn truncate_str_empty() {
        assert_eq!(truncate_str("", 10), "");
    }

    #[test]
    fn truncate_chars_adds_no_marker() {
        assert_eq!(
            truncate_chars("\u{4F60}\u{597D}abc", 3),
            "\u{4F60}\u{597D}a"
        );
        assert_eq!(truncate_chars("short", 10), "short");
    }

    #[test]
    fn truncate_chars_with_ellipsis_marks_truncation() {
        assert_eq!(truncate_chars_with_ellipsis("abcdef", 4), "abc\u{2026}");
        assert_eq!(truncate_chars_with_ellipsis("abcdef", 1), "\u{2026}");
        assert_eq!(truncate_chars_with_ellipsis("abcdef", 0), "");
        assert_eq!(truncate_chars_with_ellipsis("abc", 4), "abc");
    }
}

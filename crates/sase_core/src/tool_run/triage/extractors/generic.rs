//! Generic fallback extractor.

use regex::Regex;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

use super::raw::RawItem;

fn digits_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\d+").unwrap())
}

/// Build the generic key: stage_key + sha256 of the last 40 non-empty lines
/// with every digit run replaced by N.
#[allow(dead_code)]
pub fn generic_key(stage_key: &str, lines: &[String]) -> String {
    let non_empty: Vec<&String> = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let start = non_empty.len().saturating_sub(40);
    let mut tail: Vec<String> = non_empty[start..]
        .iter()
        .map(|line| digits_re().replace_all(line, "N").into_owned())
        .collect();
    if tail.is_empty() {
        tail.push(String::new());
    }
    let joined = tail.join("\n");
    let digest = hex::encode(Sha256::digest(joined.as_bytes()));
    format!("{stage_key}|{digest}")
}

pub fn extract_generic(stage_key: &str, lines: &[String]) -> Vec<RawItem> {
    let non_empty: Vec<&String> = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let display_source = non_empty
        .last()
        .map(|line| line.as_str())
        .unwrap_or(stage_key);
    vec![RawItem::new(
        generic_key(stage_key, lines),
        display_source,
        Vec::new(),
    )]
}

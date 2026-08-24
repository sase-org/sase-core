//! Canonical glossary validation, catalog normalization, and text matching.
//!
//! Python owns config and strand file discovery plus source-preserving YAML
//! parsing. This module owns the deterministic glossary domain contract that
//! editor, memory, and generated-document callers can share. Multiword phrases
//! match across horizontal whitespace or one line break with surrounding
//! indentation, but never across a blank line.

use std::collections::{BTreeMap, BTreeSet};

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::editor::{DocumentSnapshot, EditorPosition, EditorRange};
use crate::prompt_literal_zone_ranges;

pub const GLOSSARY_WIRE_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GlossarySourceWire {
    #[serde(
        default,
        alias = "config_path",
        skip_serializing_if = "Option::is_none"
    )]
    pub source_path: Option<String>,
    #[serde(default, alias = "config_key_path")]
    pub key_path: Vec<String>,
    #[serde(
        default,
        alias = "term_range",
        skip_serializing_if = "Option::is_none"
    )]
    pub keyword_range: Option<EditorRange>,
    #[serde(
        default,
        alias = "definition_range",
        skip_serializing_if = "Option::is_none"
    )]
    pub body_range: Option<EditorRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub aliases_range: Option<EditorRange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossaryInputEntryWire {
    pub term: String,
    pub definition: String,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<GlossarySourceWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossaryDiagnosticWire {
    pub severity: String,
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossaryEntryWire {
    pub index: usize,
    pub term: String,
    pub normalized_term: String,
    pub definition: String,
    /// Normalized aliases authored in project config.
    pub configured_aliases: Vec<String>,
    /// Normalized configured aliases that should be rendered in generated docs.
    #[serde(default)]
    pub display_aliases: Vec<String>,
    /// Normalized aliases used for matching, including accepted derived plurals.
    pub effective_aliases: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<GlossarySourceWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossaryCatalogWire {
    pub schema_version: u32,
    pub entries: Vec<GlossaryEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossarySegmentWire {
    pub byte_start: usize,
    pub byte_end: usize,
    pub range: EditorRange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlossarySpanWire {
    pub term: String,
    pub entry_index: usize,
    pub alias_index: usize,
    pub alias: String,
    pub matched_text: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub range: EditorRange,
    #[serde(default)]
    pub segments: Vec<GlossarySegmentWire>,
}

#[derive(Debug, Error)]
pub enum GlossaryError {
    #[error("invalid glossary entries: {0}")]
    Validation(String),
    #[error("failed to compile glossary matcher for `{alias}`: {source}")]
    Regex {
        alias: String,
        #[source]
        source: regex::Error,
    },
}

#[derive(Debug, Clone)]
pub struct CompiledGlossaryCatalog {
    catalog: GlossaryCatalogWire,
    patterns: Vec<AliasPattern>,
}

#[derive(Debug, Clone)]
struct AliasPattern {
    entry_index: usize,
    alias_index: usize,
    alias: String,
    regex: Regex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CandidateSpan {
    entry_index: usize,
    alias_index: usize,
    alias: String,
    byte_start: usize,
    byte_end: usize,
}

pub fn validate_glossary_entries(
    entries: &[GlossaryInputEntryWire],
) -> Vec<GlossaryDiagnosticWire> {
    let mut diagnostics = Vec::new();
    let mut alias_owner: BTreeMap<String, (usize, usize)> = BTreeMap::new();

    for (entry_index, entry) in entries.iter().enumerate() {
        let normalized_term = normalize_phrase(&entry.term);
        if normalized_term.is_empty() {
            diagnostics.push(error(
                "blank_term",
                "Glossary terms must not be blank",
                entry_path(entry, None),
            ));
        } else if contains_line_break(&entry.term) {
            diagnostics.push(error(
                "multiline_term",
                "Glossary terms must stay on one line",
                entry_path(entry, None),
            ));
        }
        if entry.definition.trim().is_empty() {
            diagnostics.push(error(
                "blank_definition",
                format!(
                    "Glossary entry `{normalized_term}` needs a definition"
                ),
                definition_path(entry),
            ));
        }

        for (alias_index, alias) in entry.aliases.iter().enumerate() {
            if contains_line_break(alias) {
                diagnostics.push(error(
                    "multiline_alias",
                    format!(
                        "Glossary alias `{}` for `{normalized_term}` must stay \
                         on one line",
                        normalize_phrase(alias)
                    ),
                    alias_path(entry, alias_index),
                ));
            }
        }

        let mut local_aliases = BTreeSet::new();
        for (alias_index, alias) in authored_aliases(entry).iter().enumerate() {
            let normalized_alias = normalize_phrase(alias);
            if normalized_alias.is_empty() {
                diagnostics.push(error(
                    "blank_alias",
                    format!(
                        "Glossary alias {alias_index} for `{normalized_term}` \
                         must not be blank"
                    ),
                    alias_path(entry, alias_index.saturating_sub(1)),
                ));
                continue;
            }
            if contains_line_break(alias) {
                diagnostics.push(error(
                    "multiline_alias",
                    format!(
                        "Glossary alias `{normalized_alias}` for \
                         `{normalized_term}` must stay on one line"
                    ),
                    alias_path(entry, alias_index.saturating_sub(1)),
                ));
                continue;
            }

            let key = case_key(&normalized_alias);
            if !local_aliases.insert(key.clone()) {
                diagnostics.push(error(
                    "duplicate_alias",
                    format!(
                        "Glossary entry `{normalized_term}` repeats alias \
                         `{normalized_alias}`"
                    ),
                    alias_path(entry, alias_index.saturating_sub(1)),
                ));
                continue;
            }
            if let Some((other_entry, _)) = alias_owner.get(&key) {
                if *other_entry != entry_index {
                    diagnostics.push(error(
                        "alias_conflict",
                        format!(
                            "Glossary alias `{normalized_alias}` is used by \
                             more than one entry"
                        ),
                        alias_path(entry, alias_index.saturating_sub(1)),
                    ));
                    continue;
                }
            }
            alias_owner.insert(key, (entry_index, alias_index));
        }
    }

    diagnostics
}

pub fn build_glossary_catalog(
    entries: Vec<GlossaryInputEntryWire>,
) -> Result<GlossaryCatalogWire, GlossaryError> {
    ensure_valid(&entries)?;
    Ok(catalog_from_entries(&entries))
}

pub fn compile_glossary_catalog(
    entries: Vec<GlossaryInputEntryWire>,
) -> Result<CompiledGlossaryCatalog, GlossaryError> {
    let catalog = build_glossary_catalog(entries)?;
    CompiledGlossaryCatalog::new(catalog)
}

impl CompiledGlossaryCatalog {
    pub fn new(catalog: GlossaryCatalogWire) -> Result<Self, GlossaryError> {
        let mut patterns = Vec::new();
        for entry in &catalog.entries {
            for (alias_index, alias) in
                entry.effective_aliases.iter().enumerate()
            {
                let regex = RegexBuilder::new(&alias_regex(alias))
                    .case_insensitive(true)
                    .unicode(true)
                    .build()
                    .map_err(|source| GlossaryError::Regex {
                        alias: alias.clone(),
                        source,
                    })?;
                patterns.push(AliasPattern {
                    entry_index: entry.index,
                    alias_index,
                    alias: alias.clone(),
                    regex,
                });
            }
        }
        Ok(Self { catalog, patterns })
    }

    pub fn catalog(&self) -> &GlossaryCatalogWire {
        &self.catalog
    }

    pub fn len(&self) -> usize {
        self.catalog.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.catalog.entries.is_empty()
    }

    pub fn scan(&self, text: &str) -> Vec<GlossarySpanWire> {
        let document = DocumentSnapshot::new(text);
        self.candidate_spans(text)
            .into_iter()
            .filter_map(|candidate| candidate.to_wire(&document, &self.catalog))
            .collect()
    }

    pub fn lookup(
        &self,
        text: &str,
        position: EditorPosition,
    ) -> Option<GlossarySpanWire> {
        let document = DocumentSnapshot::new(text);
        let cursor = document.position_to_byte_offset(position)?;
        self.scan(text)
            .into_iter()
            .find(|span| span.byte_start <= cursor && cursor < span.byte_end)
    }

    fn candidate_spans(&self, text: &str) -> Vec<CandidateSpan> {
        let literal_ranges = prompt_literal_zone_ranges(text);
        let mut candidates = Vec::new();
        for pattern in &self.patterns {
            for hit in pattern.regex.find_iter(text) {
                if literal_ranges.iter().any(|literal| {
                    ranges_intersect((hit.start(), hit.end()), *literal)
                }) {
                    continue;
                }
                if is_word_boundary(text, hit.start(), hit.end()) {
                    candidates.push(CandidateSpan {
                        entry_index: pattern.entry_index,
                        alias_index: pattern.alias_index,
                        alias: pattern.alias.clone(),
                        byte_start: hit.start(),
                        byte_end: hit.end(),
                    });
                }
            }
        }

        candidates.sort_by(|left, right| {
            left.byte_start
                .cmp(&right.byte_start)
                .then_with(|| span_len(right).cmp(&span_len(left)))
                .then_with(|| left.entry_index.cmp(&right.entry_index))
                .then_with(|| left.alias_index.cmp(&right.alias_index))
        });

        let mut accepted: Vec<CandidateSpan> = Vec::new();
        for candidate in candidates {
            if accepted.iter().any(|span| overlaps(span, &candidate)) {
                continue;
            }
            accepted.push(candidate);
        }
        accepted.sort_by_key(|span| span.byte_start);
        accepted
    }
}

impl CandidateSpan {
    fn to_wire(
        &self,
        document: &DocumentSnapshot,
        catalog: &GlossaryCatalogWire,
    ) -> Option<GlossarySpanWire> {
        let entry = catalog.entries.get(self.entry_index)?;
        let range =
            document.byte_range_to_range(self.byte_start, self.byte_end)?;
        let segments =
            glossary_segments(document, self.byte_start, self.byte_end)?;
        Some(GlossarySpanWire {
            term: entry.term.clone(),
            entry_index: self.entry_index,
            alias_index: self.alias_index,
            alias: self.alias.clone(),
            matched_text: document
                .text()
                .get(self.byte_start..self.byte_end)?
                .to_string(),
            byte_start: self.byte_start,
            byte_end: self.byte_end,
            range,
            segments,
        })
    }
}

fn ensure_valid(
    entries: &[GlossaryInputEntryWire],
) -> Result<(), GlossaryError> {
    let diagnostics = validate_glossary_entries(entries);
    let errors: Vec<&GlossaryDiagnosticWire> = diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity == "error")
        .collect();
    if errors.is_empty() {
        return Ok(());
    }
    Err(GlossaryError::Validation(
        errors
            .iter()
            .map(|diagnostic| diagnostic.message.as_str())
            .collect::<Vec<_>>()
            .join("; "),
    ))
}

fn catalog_from_entries(
    entries: &[GlossaryInputEntryWire],
) -> GlossaryCatalogWire {
    let authored_aliases_by_entry =
        entries.iter().map(authored_aliases).collect::<Vec<_>>();
    let configured_aliases_by_entry =
        entries.iter().map(configured_aliases).collect::<Vec<_>>();
    let authored_claims = authored_aliases_by_entry
        .iter()
        .flat_map(|aliases| aliases.iter())
        .filter(|alias| !alias.is_empty())
        .map(|alias| case_key(alias))
        .collect::<BTreeSet<_>>();
    let mut accepted_derived_claims = BTreeSet::new();

    GlossaryCatalogWire {
        schema_version: GLOSSARY_WIRE_SCHEMA_VERSION,
        entries: entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let normalized_term = normalize_phrase(&entry.term);
                let configured_aliases =
                    configured_aliases_by_entry[index].clone();
                let display_aliases = derive_display_aliases(
                    &normalized_term,
                    &configured_aliases,
                );
                let effective_aliases = effective_aliases(
                    &authored_aliases_by_entry[index],
                    &authored_claims,
                    &mut accepted_derived_claims,
                );
                GlossaryEntryWire {
                    index,
                    term: normalized_term.clone(),
                    normalized_term,
                    definition: entry.definition.trim().to_string(),
                    configured_aliases,
                    display_aliases,
                    effective_aliases,
                    source: entry.source.clone(),
                }
            })
            .collect(),
    }
}

fn configured_aliases(entry: &GlossaryInputEntryWire) -> Vec<String> {
    entry
        .aliases
        .iter()
        .map(|alias| normalize_phrase(alias))
        .filter(|alias| !alias.is_empty())
        .collect()
}

fn authored_aliases(entry: &GlossaryInputEntryWire) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut aliases = Vec::new();
    for alias in std::iter::once(&entry.term).chain(entry.aliases.iter()) {
        let normalized = normalize_phrase(alias);
        if normalized.is_empty() {
            aliases.push(normalized);
            continue;
        }
        if seen.insert(case_key(&normalized)) {
            aliases.push(normalized);
        }
    }
    aliases
}

fn effective_aliases(
    authored_aliases: &[String],
    authored_claims: &BTreeSet<String>,
    accepted_derived_claims: &mut BTreeSet<String>,
) -> Vec<String> {
    let mut aliases = authored_aliases
        .iter()
        .filter(|alias| !alias.is_empty())
        .cloned()
        .collect::<Vec<_>>();

    for alias in authored_aliases.iter().filter(|alias| !alias.is_empty()) {
        let Some(plural) = pluralize_phrase(alias) else {
            continue;
        };
        let key = case_key(&plural);
        if authored_claims.contains(&key) {
            continue;
        }
        if accepted_derived_claims.insert(key) {
            aliases.push(plural);
        }
    }

    aliases
}

fn derive_display_aliases(
    term: &str,
    configured_aliases: &[String],
) -> Vec<String> {
    configured_aliases
        .iter()
        .enumerate()
        .filter_map(|(alias_index, alias)| {
            if alias_is_derivable_from_other_source(
                alias,
                alias_index,
                term,
                configured_aliases,
            ) {
                None
            } else {
                Some(alias.clone())
            }
        })
        .collect()
}

fn alias_is_derivable_from_other_source(
    alias: &str,
    alias_index: usize,
    term: &str,
    configured_aliases: &[String],
) -> bool {
    let alias_key = case_key(alias);
    if pluralize_phrase(term)
        .is_some_and(|plural| case_key(&plural) == alias_key)
    {
        return true;
    }

    configured_aliases
        .iter()
        .enumerate()
        .any(|(source_index, source)| {
            source_index != alias_index
                && pluralize_phrase(source)
                    .is_some_and(|plural| case_key(&plural) == alias_key)
        })
}

fn normalize_phrase(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn pluralize_phrase(phrase: &str) -> Option<String> {
    let phrase = phrase.trim();
    if phrase.is_empty() {
        return None;
    }
    let (prefix, last_word) = phrase
        .rsplit_once(' ')
        .map_or(("", phrase), |(prefix, word)| (prefix, word));
    let final_char = last_word.chars().next_back()?;
    if !last_word.chars().all(|ch| ch.is_ascii_alphabetic()) {
        return None;
    }

    let lower = last_word.to_ascii_lowercase();
    if lower.ends_with('s') {
        return None;
    }

    let plural_word = if lower.ends_with('x')
        || lower.ends_with('z')
        || lower.ends_with("ch")
        || lower.ends_with("sh")
    {
        format!("{last_word}es")
    } else if lower.ends_with('y') && has_consonant_before_final_y(last_word) {
        let stem = &last_word[..last_word.len() - final_char.len_utf8()];
        format!("{stem}ies")
    } else {
        format!("{last_word}s")
    };

    if prefix.is_empty() {
        Some(plural_word)
    } else {
        Some(format!("{prefix} {plural_word}"))
    }
}

fn has_consonant_before_final_y(word: &str) -> bool {
    let mut chars = word.chars().rev();
    let Some(_) = chars.next() else {
        return false;
    };
    let Some(previous) = chars.next() else {
        return false;
    };
    previous.is_ascii_alphabetic()
        && !matches!(previous.to_ascii_lowercase(), 'a' | 'e' | 'i' | 'o' | 'u')
}

const PHRASE_GAP: &str = r"(?:[\t ]*\r?\n[\t ]*|[\t ]+)";

fn alias_regex(alias: &str) -> String {
    alias
        .split_whitespace()
        .map(regex::escape)
        .collect::<Vec<_>>()
        .join(PHRASE_GAP)
}

fn glossary_segments(
    document: &DocumentSnapshot,
    byte_start: usize,
    byte_end: usize,
) -> Option<Vec<GlossarySegmentWire>> {
    document.text().get(byte_start..byte_end)?;
    let mut segments = Vec::new();
    let mut line_start = byte_start;

    loop {
        let line_end = document.text()[line_start..byte_end]
            .find('\n')
            .map_or(byte_end, |relative| line_start + relative);
        let (segment_start, segment_end) =
            trim_segment_edges(document.text(), line_start, line_end);
        if segment_start < segment_end {
            segments.push(GlossarySegmentWire {
                byte_start: segment_start,
                byte_end: segment_end,
                range: document
                    .byte_range_to_range(segment_start, segment_end)?,
            });
        }
        if line_end == byte_end {
            break;
        }
        line_start = line_end + 1;
    }

    Some(segments)
}

fn trim_segment_edges(
    text: &str,
    mut byte_start: usize,
    mut byte_end: usize,
) -> (usize, usize) {
    let bytes = text.as_bytes();
    while byte_start < byte_end && is_segment_edge_byte(bytes[byte_start]) {
        byte_start += 1;
    }
    while byte_start < byte_end && is_segment_edge_byte(bytes[byte_end - 1]) {
        byte_end -= 1;
    }
    (byte_start, byte_end)
}

fn is_segment_edge_byte(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\r')
}

fn case_key(value: &str) -> String {
    value.to_lowercase()
}

fn contains_line_break(value: &str) -> bool {
    value.contains('\n') || value.contains('\r')
}

fn is_word_boundary(text: &str, start: usize, end: usize) -> bool {
    let before = text[..start].chars().next_back();
    let after = text[end..].chars().next();
    !before.is_some_and(is_word_char) && !after.is_some_and(is_word_char)
}

fn is_word_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_' || ch == '-'
}

fn span_len(span: &CandidateSpan) -> usize {
    span.byte_end.saturating_sub(span.byte_start)
}

fn overlaps(left: &CandidateSpan, right: &CandidateSpan) -> bool {
    left.byte_start < right.byte_end && right.byte_start < left.byte_end
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

fn error(
    code: impl Into<String>,
    message: impl Into<String>,
    path: Option<String>,
) -> GlossaryDiagnosticWire {
    GlossaryDiagnosticWire {
        severity: "error".to_string(),
        code: code.into(),
        message: message.into(),
        path,
    }
}

fn entry_path(
    entry: &GlossaryInputEntryWire,
    suffix: Option<&str>,
) -> Option<String> {
    if let Some(source) = &entry.source {
        if !source.key_path.is_empty() {
            let mut path = source.key_path.join(".");
            if let Some(suffix) = suffix {
                path.push('.');
                path.push_str(suffix);
            }
            return Some(path);
        }
    }
    let term = normalize_phrase(&entry.term);
    if term.is_empty() {
        Some("glossary".to_string())
    } else if let Some(suffix) = suffix {
        Some(format!("glossary.{term}.{suffix}"))
    } else {
        Some(format!("glossary.{term}"))
    }
}

fn definition_path(entry: &GlossaryInputEntryWire) -> Option<String> {
    entry_path(entry, Some("definition"))
}

fn alias_path(
    entry: &GlossaryInputEntryWire,
    alias_index: usize,
) -> Option<String> {
    entry_path(entry, Some(&format!("aliases[{alias_index}]")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        term: &str,
        definition: &str,
        aliases: &[&str],
    ) -> GlossaryInputEntryWire {
        GlossaryInputEntryWire {
            term: term.to_string(),
            definition: definition.to_string(),
            aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
            source: None,
        }
    }

    #[test]
    fn glossary_source_wire_accepts_v1_payload_names() {
        let source: GlossarySourceWire =
            serde_json::from_value(serde_json::json!({
                "config_path": "/repo/sase/sase.yml",
                "config_key_path": ["memory", "glossary", "Agent Clan"],
                "term_range": {
                    "start": {"line": 2, "character": 4},
                    "end": {"line": 2, "character": 14}
                },
                "definition_range": {
                    "start": {"line": 5, "character": 18},
                    "end": {"line": 7, "character": 19}
                },
                "aliases_range": {
                    "start": {"line": 4, "character": 8},
                    "end": {"line": 4, "character": 14}
                }
            }))
            .unwrap();

        assert_eq!(source.source_path.as_deref(), Some("/repo/sase/sase.yml"));
        assert_eq!(source.key_path, ["memory", "glossary", "Agent Clan"]);
        assert_eq!(source.keyword_range, Some(range(2, 4, 2, 14)));
        assert_eq!(source.body_range, Some(range(5, 18, 7, 19)));
        assert_eq!(source.aliases_range, Some(range(4, 8, 4, 14)));
    }

    #[test]
    fn glossary_source_wire_emits_v2_payload_names() {
        let source = GlossarySourceWire {
            source_path: Some(
                "/repo/sase/memory/glossary/agent-clan.md".to_string(),
            ),
            key_path: Vec::new(),
            keyword_range: Some(range(1, 9, 1, 20)),
            body_range: Some(range(4, 0, 6, 12)),
            aliases_range: None,
        };

        let payload = serde_json::to_value(&source).unwrap();

        assert_eq!(
            payload,
            serde_json::json!({
                "source_path": "/repo/sase/memory/glossary/agent-clan.md",
                "key_path": [],
                "keyword_range": {
                    "start": {"line": 1, "character": 9},
                    "end": {"line": 1, "character": 20}
                },
                "body_range": {
                    "start": {"line": 4, "character": 0},
                    "end": {"line": 6, "character": 12}
                }
            })
        );
        assert_eq!(
            serde_json::from_value::<GlossarySourceWire>(payload).unwrap(),
            source
        );
    }

    #[test]
    fn pluralizes_phrases_with_conservative_ascii_rules() {
        let cases = [
            ("", None),
            ("Status", None),
            ("README.md", None),
            ("Box", Some("Boxes")),
            ("Buzz", Some("Buzzes")),
            ("Patch", Some("Patches")),
            ("Brush", Some("Brushes")),
            ("Family", Some("Families")),
            ("Play", Some("Plays")),
            ("Repo", Some("Repos")),
            ("agents.md file", Some("agents.md files")),
        ];

        for (phrase, expected) in cases {
            assert_eq!(pluralize_phrase(phrase).as_deref(), expected);
        }
    }

    #[test]
    fn validation_diagnostics_stay_authored_for_alias_edge_cases() {
        let diagnostics = validate_glossary_entries(&[
            entry("Blank Alias", "Definition.", &[""]),
            entry("Repeated Alias", "Definition.", &["dup", " DUP "]),
            entry("Workspace", "Definition.", &["two\nlines"]),
            entry("Agent Clan", "A named rootless container.", &[]),
            entry("Clan", "Another thing.", &["agent clan"]),
        ]);

        assert_eq!(
            diagnostics
                .iter()
                .map(|diagnostic| (
                    diagnostic.code.as_str(),
                    diagnostic.path.as_deref(),
                    diagnostic.message.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "blank_alias",
                    Some("glossary.Blank Alias.aliases[0]"),
                    "Glossary alias 1 for `Blank Alias` must not be blank",
                ),
                (
                    "multiline_alias",
                    Some("glossary.Workspace.aliases[0]"),
                    "Glossary alias `two lines` for `Workspace` must stay on one line",
                ),
                (
                    "alias_conflict",
                    Some("glossary.Clan.aliases[0]"),
                    "Glossary alias `agent clan` is used by more than one entry",
                ),
            ]
        );
        assert!(!diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "duplicate_alias"));
    }

    #[test]
    fn builds_effective_aliases_with_term_first() {
        let catalog = build_glossary_catalog(vec![entry(
            " Agent   Clan ",
            " A named rootless container. ",
            &["agent clans", "Agent Clan"],
        )])
        .unwrap();

        assert_eq!(catalog.schema_version, 2);
        assert_eq!(catalog.entries[0].term, "Agent Clan");
        assert_eq!(
            catalog.entries[0].configured_aliases,
            vec!["agent clans", "Agent Clan"]
        );
        assert_eq!(catalog.entries[0].display_aliases, vec!["Agent Clan"]);
        assert_eq!(
            catalog.entries[0].effective_aliases,
            vec!["Agent Clan", "agent clans"]
        );
    }

    #[test]
    fn entry_path_falls_back_when_source_key_path_is_empty() {
        let entry = GlossaryInputEntryWire {
            term: "Agent Clan".to_string(),
            definition: "A named rootless container.".to_string(),
            aliases: Vec::new(),
            source: Some(GlossarySourceWire {
                source_path: Some(
                    "/repo/sase/memory/glossary/agent-clan.md".to_string(),
                ),
                key_path: Vec::new(),
                keyword_range: None,
                body_range: None,
                aliases_range: None,
            }),
        };

        assert_eq!(
            entry_path(&entry, Some("definition")).as_deref(),
            Some("glossary.Agent Clan.definition")
        );
    }

    #[test]
    fn builds_effective_aliases_with_derived_plurals() {
        let catalog = build_glossary_catalog(vec![entry(
            "Agent Clan",
            "A named rootless container.",
            &["clan"],
        )])
        .unwrap();

        assert_eq!(
            catalog.entries[0].effective_aliases,
            vec!["Agent Clan", "clan", "Agent Clans", "clans"]
        );
    }

    #[test]
    fn skips_derived_plural_claimed_by_authored_alias_without_diagnostic() {
        let entries = vec![
            entry("Agent Clan", "A named rootless container.", &[]),
            entry("Group", "Another name.", &["agent clans"]),
        ];

        assert!(validate_glossary_entries(&entries).is_empty());
        let catalog = build_glossary_catalog(entries).unwrap();

        assert_eq!(catalog.entries[0].effective_aliases, vec!["Agent Clan"]);
        assert_eq!(
            catalog.entries[1].effective_aliases,
            vec!["Group", "agent clans", "Groups"]
        );
    }

    #[test]
    fn filters_display_aliases_to_non_derivable_configured_aliases() {
        let catalog = build_glossary_catalog(vec![
            entry(
                "Agent Clan",
                "A named rootless container.",
                &["agent clans", "clan"],
            ),
            entry(
                "Widget",
                "A test fixture.",
                &["widget boxes", "widget box", "bespoke"],
            ),
            entry("Patch", "A local unit of change.", &["patches"]),
        ])
        .unwrap();

        assert_eq!(catalog.entries[0].display_aliases, vec!["clan"]);
        assert_eq!(
            catalog.entries[1].display_aliases,
            vec!["widget box", "bespoke"]
        );
        assert!(catalog.entries[2].display_aliases.is_empty());
    }

    #[test]
    fn scans_derived_plural_for_term_without_configured_aliases() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Agent Clan",
            "A named rootless container.",
            &[],
        )])
        .unwrap();

        let spans = catalog.scan("Two Agent Clans coordinated.");

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].term, "Agent Clan");
        assert_eq!(spans[0].alias, "Agent Clans");
        assert_eq!(spans[0].matched_text, "Agent Clans");
    }

    #[test]
    fn scans_case_insensitively_with_longest_non_overlapping_matches() {
        let catalog = compile_glossary_catalog(vec![
            entry("Agent", "A worker.", &[]),
            entry("Agent Clan", "A named rootless container.", &["clan"]),
        ])
        .unwrap();

        let spans = catalog
            .scan("The agent \t clan joined another agent-clan and clan.");
        assert_eq!(
            spans
                .iter()
                .map(|span| (
                    span.term.as_str(),
                    span.alias.as_str(),
                    span.matched_text.as_str()
                ))
                .collect::<Vec<_>>(),
            vec![
                ("Agent Clan", "Agent Clan", "agent \t clan"),
                ("Agent Clan", "clan", "clan"),
            ]
        );
    }

    #[test]
    fn scan_skips_fenced_and_inline_code_literals() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Workspace",
            "A checkout.",
            &[],
        )])
        .unwrap();

        let spans = catalog.scan("Workspace `Workspace`\n```\nWorkspace\n```");

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].matched_text, "Workspace");
    }

    #[test]
    fn lookup_uses_utf16_editor_positions() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Workspace",
            "A checkout.",
            &[],
        )])
        .unwrap();
        let text = "x\nSee 🧪 Workspace here.";
        let span = catalog
            .lookup(
                text,
                EditorPosition {
                    line: 1,
                    character: 8,
                },
            )
            .unwrap();

        assert_eq!(span.term, "Workspace");
        assert_eq!(span.range.start.line, 1);
        assert_eq!(span.range.start.character, 7);
        assert_eq!(
            catalog.lookup(
                text,
                EditorPosition {
                    line: 1,
                    character: 16
                }
            ),
            None
        );
    }

    fn range(
        start_line: u32,
        start_character: u32,
        end_line: u32,
        end_character: u32,
    ) -> EditorRange {
        EditorRange {
            start: EditorPosition {
                line: start_line,
                character: start_character,
            },
            end: EditorPosition {
                line: end_line,
                character: end_character,
            },
        }
    }

    fn segment_texts<'a>(
        text: &'a str,
        span: &'a GlossarySpanWire,
    ) -> Vec<&'a str> {
        span.segments
            .iter()
            .map(|segment| &text[segment.byte_start..segment.byte_end])
            .collect()
    }

    #[test]
    fn scans_wrapped_phrase_with_trimmed_segments() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Xprompt Memory",
            "A generated memory note.",
            &[],
        )])
        .unwrap();
        let text = "Start xprompt\n  memory file";
        let spans = catalog.scan(text);

        assert_eq!(spans.len(), 1);
        let span = &spans[0];
        assert_eq!(span.term, "Xprompt Memory");
        assert_eq!(span.matched_text, "xprompt\n  memory");
        assert_eq!(span.range, range(0, 6, 1, 8));
        assert_eq!(segment_texts(text, span), vec!["xprompt", "memory"]);
        assert_eq!(
            span.segments
                .iter()
                .map(|segment| segment.range)
                .collect::<Vec<_>>(),
            vec![range(0, 6, 0, 13), range(1, 2, 1, 8)]
        );
    }

    #[test]
    fn wrapped_phrase_does_not_cross_block_boundaries() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Xprompt Memory",
            "A generated memory note.",
            &[],
        )])
        .unwrap();

        for text in [
            "xprompt\n\nmemory",
            "xprompt\n- memory",
            "xprompt\n> memory",
            "xprompt\n## Memory",
            "xprompt\n---\nmemory",
        ] {
            assert_eq!(catalog.scan(text), Vec::new(), "{text:?}");
        }
    }

    #[test]
    fn wrapped_longer_match_wins_over_shorter_at_same_start() {
        let catalog = compile_glossary_catalog(vec![
            entry("Xprompt", "A prompt template.", &[]),
            entry("Xprompt Memory", "A generated memory note.", &[]),
        ])
        .unwrap();

        let spans = catalog.scan("Create an xprompt\n  memory file.");

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].term, "Xprompt Memory");
        assert_eq!(spans[0].matched_text, "xprompt\n  memory");
    }

    #[test]
    fn wrapped_phrase_accepts_crlf_without_segment_carriage_returns() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Xprompt Memory",
            "A generated memory note.",
            &[],
        )])
        .unwrap();
        let text = "xprompt\r\n\tmemory";
        let spans = catalog.scan(text);

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].matched_text, "xprompt\r\n\tmemory");
        assert_eq!(segment_texts(text, &spans[0]), vec!["xprompt", "memory"]);
        assert!(spans[0]
            .segments
            .iter()
            .all(|segment| !text[segment.byte_start..segment.byte_end]
                .contains('\r')));
    }

    #[test]
    fn three_word_term_wraps_across_three_lines() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Agent Instruction File",
            "An agents.md file.",
            &[],
        )])
        .unwrap();
        let text = "agent\n  instruction\n\tfile";
        let spans = catalog.scan(text);

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].term, "Agent Instruction File");
        assert_eq!(
            segment_texts(text, &spans[0]),
            vec!["agent", "instruction", "file"]
        );
        assert_eq!(
            spans[0]
                .segments
                .iter()
                .map(|segment| segment.range)
                .collect::<Vec<_>>(),
            vec![range(0, 0, 0, 5), range(1, 2, 1, 13), range(2, 1, 2, 5)]
        );
    }

    #[test]
    fn single_line_match_has_one_span_equal_segment() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Agent Clan",
            "A named rootless container.",
            &[],
        )])
        .unwrap();
        let text = "See agent clan here.";
        let span = catalog.scan(text).pop().unwrap();

        assert_eq!(span.segments.len(), 1);
        assert_eq!(span.segments[0].byte_start, span.byte_start);
        assert_eq!(span.segments[0].byte_end, span.byte_end);
        assert_eq!(span.segments[0].range, span.range);
    }

    #[test]
    fn literal_zone_filter_skips_candidates_but_keeps_prose_match() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Xprompt Memory",
            "A generated memory note.",
            &[],
        )])
        .unwrap();
        let text = concat!(
            "`xprompt memory`\n",
            "Prose xprompt\n",
            "  memory\n",
            "```\n",
            "xprompt\n",
            "  memory\n",
            "```\n",
        );
        let spans = catalog.scan(text);

        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].matched_text, "xprompt\n  memory");
    }

    #[test]
    fn lookup_on_continuation_word_returns_wrapped_span() {
        let catalog = compile_glossary_catalog(vec![entry(
            "Xprompt Memory",
            "A generated memory note.",
            &[],
        )])
        .unwrap();
        let text = "See xprompt\n  memory here.";
        let span = catalog
            .lookup(
                text,
                EditorPosition {
                    line: 1,
                    character: 4,
                },
            )
            .unwrap();

        assert_eq!(span.term, "Xprompt Memory");
        assert_eq!(span.matched_text, "xprompt\n  memory");
        assert_eq!(segment_texts(text, &span), vec!["xprompt", "memory"]);
    }
}

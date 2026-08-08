//! Canonical glossary validation, catalog normalization, and text matching.
//!
//! Python owns config discovery and source-preserving YAML parsing. This
//! module owns the deterministic glossary domain contract that editor, memory,
//! and generated-document callers can share.

use std::collections::{BTreeMap, BTreeSet};

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::editor::{DocumentSnapshot, EditorPosition, EditorRange};

pub const GLOSSARY_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GlossarySourceWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_path: Option<String>,
    #[serde(default)]
    pub config_key_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub term_range: Option<EditorRange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_range: Option<EditorRange>,
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
    pub configured_aliases: Vec<String>,
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
pub struct GlossarySpanWire {
    pub term: String,
    pub entry_index: usize,
    pub alias_index: usize,
    pub alias: String,
    pub matched_text: String,
    pub byte_start: usize,
    pub byte_end: usize,
    pub range: EditorRange,
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
        for (alias_index, alias) in effective_aliases(entry).iter().enumerate()
        {
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
                let regex = RegexBuilder::new(&regex::escape(alias))
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
        let mut candidates = Vec::new();
        for pattern in &self.patterns {
            for hit in pattern.regex.find_iter(text) {
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
    GlossaryCatalogWire {
        schema_version: GLOSSARY_WIRE_SCHEMA_VERSION,
        entries: entries
            .iter()
            .enumerate()
            .map(|(index, entry)| {
                let configured_aliases = entry
                    .aliases
                    .iter()
                    .map(|alias| normalize_phrase(alias))
                    .filter(|alias| !alias.is_empty())
                    .collect::<Vec<_>>();
                GlossaryEntryWire {
                    index,
                    term: normalize_phrase(&entry.term),
                    normalized_term: normalize_phrase(&entry.term),
                    definition: entry.definition.trim().to_string(),
                    configured_aliases,
                    effective_aliases: effective_aliases(entry),
                    source: entry.source.clone(),
                }
            })
            .collect(),
    }
}

fn effective_aliases(entry: &GlossaryInputEntryWire) -> Vec<String> {
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

fn normalize_phrase(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
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
        if !source.config_key_path.is_empty() {
            let mut path = source.config_key_path.join(".");
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
    fn validates_blank_and_conflicting_aliases() {
        let diagnostics = validate_glossary_entries(&[
            entry("Agent Clan", "A named rootless container.", &["clan"]),
            entry("Clan", "Another thing.", &["agent clan"]),
            entry("Workspace", "", &["two\nlines"]),
        ]);

        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "alias_conflict"
                && diagnostic.path.as_deref()
                    == Some("glossary.Clan.aliases[0]")
        }));
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "blank_definition"));
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "multiline_alias"));
    }

    #[test]
    fn builds_effective_aliases_with_term_first() {
        let catalog = build_glossary_catalog(vec![entry(
            " Agent   Clan ",
            " A named rootless container. ",
            &["agent clans", "Agent Clan"],
        )])
        .unwrap();

        assert_eq!(catalog.schema_version, GLOSSARY_WIRE_SCHEMA_VERSION);
        assert_eq!(catalog.entries[0].term, "Agent Clan");
        assert_eq!(
            catalog.entries[0].configured_aliases,
            vec!["agent clans", "Agent Clan"]
        );
        assert_eq!(
            catalog.entries[0].effective_aliases,
            vec!["Agent Clan", "agent clans"]
        );
    }

    #[test]
    fn scans_case_insensitively_with_longest_non_overlapping_matches() {
        let catalog = compile_glossary_catalog(vec![
            entry("Agent", "A worker.", &[]),
            entry("Agent Clan", "A named rootless container.", &["clan"]),
        ])
        .unwrap();

        let spans =
            catalog.scan("The agent clan joined another agent-clan and clan.");
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
                ("Agent Clan", "Agent Clan", "agent clan"),
                ("Agent Clan", "clan", "clan"),
            ]
        );
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
}

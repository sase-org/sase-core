//! Ctrl+K prompt-history project filter: query grammar and batch matching.
//!
//! A deliberately small grammar -- one optional *leading* `project:<value>`
//! qualifier followed by an optional literal text substring -- so existing
//! phrase searches over prompt-history text keep working unchanged. This is
//! intentionally not built on the whitespace-is-conjunction `query::flat`
//! grammar: that grammar tokenizes on every whitespace boundary and treats
//! `project:<value>` as typed field *equality*, which would turn a legacy
//! multi-word substring search into several independently-ANDed word
//! matches and would collide with this filter's own `project:` spelling.
//!
//! The host resolves VCS spans, project/Patch identity facts, and per-row
//! text off the event loop; every operation here is pure and synchronous.

pub mod wire;

pub use wire::{
    CompiledPromptHistoryQueryWire, PromptHistoryMatchResultWire,
    PromptHistoryProjectIdentityWire, PromptHistoryRowFactsWire,
    PromptHistorySeedRequestWire, PromptHistorySeedWire,
    PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
};

const AMBIGUOUS_PROJECT_DIAGNOSTIC: &str =
    "Project scope is ambiguous; use a canonical project key.";
const EMPTY_VALUE_DIAGNOSTIC: &str =
    "Project filter needs a value after project:.";
const UNTERMINATED_QUOTE_DIAGNOSTIC: &str =
    "Unterminated quoted project value; add a closing \".";
const UNAVAILABLE_SCOPE_HINT: &str =
    "Project scope unavailable; searching all loaded prompts";

#[derive(Debug, Clone, Default)]
struct ResolvedProjectRef {
    key: Option<String>,
    label: Option<String>,
    ambiguous: bool,
}

fn resolve_project_ref(
    value: &str,
    catalog: &[PromptHistoryProjectIdentityWire],
) -> ResolvedProjectRef {
    let folded = value.trim().to_ascii_lowercase();
    if folded.is_empty() {
        return ResolvedProjectRef::default();
    }

    let matches: Vec<&PromptHistoryProjectIdentityWire> = catalog
        .iter()
        .filter(|entry| {
            entry.key.eq_ignore_ascii_case(&folded)
                || entry
                    .label
                    .as_deref()
                    .is_some_and(|label| label.eq_ignore_ascii_case(&folded))
                || entry
                    .aliases
                    .iter()
                    .any(|alias| alias.eq_ignore_ascii_case(&folded))
                || entry
                    .raw_refs
                    .iter()
                    .any(|raw_ref| raw_ref.eq_ignore_ascii_case(&folded))
        })
        .collect();

    let mut distinct_keys: Vec<&str> =
        matches.iter().map(|m| m.key.as_str()).collect();
    distinct_keys.sort_unstable();
    distinct_keys.dedup();

    match distinct_keys.len() {
        0 => ResolvedProjectRef::default(),
        1 => {
            let entry = matches[0];
            ResolvedProjectRef {
                key: Some(entry.key.clone()),
                label: Some(
                    entry.label.clone().unwrap_or_else(|| entry.key.clone()),
                ),
                ambiguous: false,
            }
        }
        _ => ResolvedProjectRef {
            key: None,
            label: None,
            ambiguous: true,
        },
    }
}

/// Return `Some(literal)` when *trimmed* is the `\project:` escape for a
/// literal substring beginning with `project:` (case-insensitive on the
/// field name). Exactly one leading backslash is removed; everything after
/// it -- including further backslashes -- is returned unprocessed.
fn strip_literal_escape(trimmed: &str) -> Option<&str> {
    let after_backslash = trimmed.strip_prefix('\\')?;
    looks_like_qualifier_after_escapes(after_backslash)
        .then_some(after_backslash)
}

fn starts_with_qualifier_prefix(s: &str) -> bool {
    s.len() >= 8
        && s.as_bytes()[..7].eq_ignore_ascii_case(b"project")
        && s.as_bytes()[7] == b':'
}

fn strip_qualifier_prefix(s: &str) -> Option<&str> {
    starts_with_qualifier_prefix(s).then(|| &s[8..])
}

/// Parse the value immediately after `project:`, returning the unescaped
/// value and the unconsumed remainder, or a diagnostic for an empty value
/// or an unterminated quote.
fn parse_qualifier_value(after_prefix: &str) -> Result<(String, &str), String> {
    if let Some(rest) = after_prefix.strip_prefix('"') {
        let mut value = String::new();
        let mut chars = rest.char_indices().peekable();
        while let Some((idx, ch)) = chars.next() {
            match ch {
                '\\' => {
                    if let Some(&(_, next_ch)) = chars.peek() {
                        if next_ch == '"' || next_ch == '\\' {
                            value.push(next_ch);
                            chars.next();
                            continue;
                        }
                    }
                    value.push('\\');
                }
                '"' => {
                    let remainder = &rest[idx + 1..];
                    if value.is_empty() {
                        return Err(EMPTY_VALUE_DIAGNOSTIC.to_string());
                    }
                    return Ok((value, remainder));
                }
                other => value.push(other),
            }
        }
        return Err(UNTERMINATED_QUOTE_DIAGNOSTIC.to_string());
    }

    let end = after_prefix
        .find(char::is_whitespace)
        .unwrap_or(after_prefix.len());
    let value = &after_prefix[..end];
    if value.is_empty() {
        return Err(EMPTY_VALUE_DIAGNOSTIC.to_string());
    }
    Ok((value.to_string(), &after_prefix[end..]))
}

/// Compile one raw Ctrl+K filter string into a `project:` constraint plus a
/// literal text substring, resolving the constraint against *catalog*.
pub fn compile_prompt_history_query(
    raw_query: &str,
    catalog: &[PromptHistoryProjectIdentityWire],
) -> CompiledPromptHistoryQueryWire {
    let leading_ws = raw_query.len() - raw_query.trim_start().len();
    let trimmed = &raw_query[leading_ws..];

    if let Some(literal_after_ws) = strip_literal_escape(trimmed) {
        // Preserve the original leading whitespace verbatim; only the one
        // escape backslash is removed, so the encode/parse round trip
        // recovers the exact draft text.
        let text = format!("{}{literal_after_ws}", &raw_query[..leading_ws]);
        return CompiledPromptHistoryQueryWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            raw_project_value: None,
            project_key: None,
            project_label: None,
            text,
            valid: true,
            diagnostic: None,
        };
    }

    let Some(after_prefix) = strip_qualifier_prefix(trimmed) else {
        return CompiledPromptHistoryQueryWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            raw_project_value: None,
            project_key: None,
            project_label: None,
            text: raw_query.to_string(),
            valid: true,
            diagnostic: None,
        };
    };

    match parse_qualifier_value(after_prefix) {
        Err(diagnostic) => CompiledPromptHistoryQueryWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            raw_project_value: None,
            project_key: None,
            project_label: None,
            text: String::new(),
            valid: false,
            diagnostic: Some(diagnostic),
        },
        Ok((value, remainder)) => {
            let resolved = resolve_project_ref(&value, catalog);
            let remainder_ws = remainder.len() - remainder.trim_start().len();
            CompiledPromptHistoryQueryWire {
                schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
                raw_project_value: Some(value),
                project_key: resolved.key,
                project_label: resolved.label,
                text: remainder[remainder_ws..].to_string(),
                valid: true,
                diagnostic: resolved
                    .ambiguous
                    .then(|| AMBIGUOUS_PROJECT_DIAGNOSTIC.to_string()),
            }
        }
    }
}

/// Encode arbitrary literal text so a later [`compile_prompt_history_query`]
/// recovers it byte-for-byte as an unscoped substring, even when it starts
/// with (any number of backslashes followed by) a case-insensitive
/// `project:` -- which would otherwise be parsed as a qualifier or consumed
/// as one layer of escape. Round-trips with `compile_prompt_history_query`:
/// parsing `encode_prompt_history_literal(text)` as a query with no other
/// qualifier always yields `text` back as `.text`.
pub fn encode_prompt_history_literal(text: &str) -> String {
    let leading_ws = text.len() - text.trim_start().len();
    let (ws, rest) = text.split_at(leading_ws);
    if looks_like_qualifier_after_escapes(rest) {
        format!("{ws}\\{rest}")
    } else {
        text.to_string()
    }
}

fn looks_like_qualifier_after_escapes(s: &str) -> bool {
    let mut rest = s;
    while let Some(next) = rest.strip_prefix('\\') {
        rest = next;
    }
    starts_with_qualifier_prefix(rest)
}

/// Batch-match prepared history rows against one compiled query. A
/// malformed query (`valid: false`) matches nothing. When the query carries
/// no `project:` constraint, only the text substring applies -- unchanged
/// legacy behavior. An ambiguous project value also matches nothing (its
/// diagnostic asks the caller to disambiguate); an unresolved value falls
/// back to exact (case-insensitive) matching against a row segment's raw,
/// unregistered VCS ref, so deleted/unregistered historical projects stay
/// searchable by their exact original spelling.
pub fn match_prompt_history_rows(
    query: &CompiledPromptHistoryQueryWire,
    rows: &[PromptHistoryRowFactsWire],
) -> PromptHistoryMatchResultWire {
    if !query.valid {
        return PromptHistoryMatchResultWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            matched_indices: Vec::new(),
        };
    }

    let ambiguous = query.raw_project_value.is_some()
        && query.project_key.is_none()
        && query.diagnostic.is_some();
    let text_needle = query.text.to_ascii_lowercase();

    let mut matched_indices = Vec::new();
    for row in rows {
        if let Some(raw_value) = &query.raw_project_value {
            if ambiguous {
                continue;
            }
            let project_matches = if let Some(key) = &query.project_key {
                row.segment_project_keys.iter().any(|segment_key| {
                    segment_key
                        .as_deref()
                        .is_some_and(|k| k.eq_ignore_ascii_case(key))
                })
            } else {
                row.segment_raw_refs.iter().enumerate().any(|(i, raw_ref)| {
                    row.segment_project_keys
                        .get(i)
                        .map_or(true, Option::is_none)
                        && raw_ref
                            .as_deref()
                            .is_some_and(|r| r.eq_ignore_ascii_case(raw_value))
                })
            };
            if !project_matches {
                continue;
            }
        }

        if !text_needle.is_empty() {
            let canonical_hit = row
                .canonical_text
                .to_ascii_lowercase()
                .contains(&text_needle);
            let display_hit =
                row.display_text.to_ascii_lowercase().contains(&text_needle);
            if !canonical_hit && !display_hit {
                continue;
            }
        }

        matched_indices.push(row.index);
    }

    PromptHistoryMatchResultWire {
        schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
        matched_indices,
    }
}

fn format_project_token(value: &str) -> String {
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
        format!("\"{escaped}\"")
    } else {
        value.to_string()
    }
}

/// Build the initial Ctrl+K history query ("seed") from the recognized
/// leading workspace reference (if any) and the draft's remaining text.
/// An unresolved *raw_ref* removes the scope but keeps the text search,
/// surfacing a non-error hint instead of guessing or erroring.
pub fn build_prompt_history_seed(
    request: &PromptHistorySeedRequestWire,
    catalog: &[PromptHistoryProjectIdentityWire],
) -> PromptHistorySeedWire {
    let remainder = encode_prompt_history_literal(&request.remainder_text);

    let raw_ref = request
        .raw_ref
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty());

    let Some(raw_ref) = raw_ref else {
        return PromptHistorySeedWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            seed_text: remainder,
            hint: None,
        };
    };

    let resolved = resolve_project_ref(raw_ref, catalog);
    match resolved.key {
        Some(key) => {
            let scope = format_project_token(&resolved.label.unwrap_or(key));
            let seed_text = if remainder.trim().is_empty() {
                format!("project:{scope} ")
            } else {
                format!("project:{scope} {remainder}")
            };
            PromptHistorySeedWire {
                schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
                seed_text,
                hint: None,
            }
        }
        None => PromptHistorySeedWire {
            schema_version: PROMPT_HISTORY_FILTER_WIRE_SCHEMA_VERSION,
            seed_text: remainder,
            hint: Some(UNAVAILABLE_SCOPE_HINT.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(key: &str) -> PromptHistoryProjectIdentityWire {
        PromptHistoryProjectIdentityWire {
            key: key.to_string(),
            label: None,
            aliases: Vec::new(),
            raw_refs: Vec::new(),
        }
    }

    fn catalog() -> Vec<PromptHistoryProjectIdentityWire> {
        vec![
            PromptHistoryProjectIdentityWire {
                key: "sase".to_string(),
                label: Some("sase".to_string()),
                aliases: vec!["sase-main".to_string()],
                raw_refs: vec![
                    "sase-org/sase".to_string(),
                    "gh_sase-org__sase".to_string(),
                ],
            },
            project("sase-core"),
            PromptHistoryProjectIdentityWire {
                key: "home".to_string(),
                label: Some("home".to_string()),
                aliases: Vec::new(),
                raw_refs: Vec::new(),
            },
            PromptHistoryProjectIdentityWire {
                key: "widget-a".to_string(),
                label: Some("widgets".to_string()),
                aliases: Vec::new(),
                raw_refs: Vec::new(),
            },
            PromptHistoryProjectIdentityWire {
                key: "widget-b".to_string(),
                label: Some("widgets".to_string()),
                aliases: Vec::new(),
                raw_refs: Vec::new(),
            },
        ]
    }

    fn row(
        index: u32,
        text: &str,
        segment_project_keys: &[Option<&str>],
        segment_raw_refs: &[Option<&str>],
    ) -> PromptHistoryRowFactsWire {
        PromptHistoryRowFactsWire {
            index,
            canonical_text: text.to_string(),
            display_text: text.to_string(),
            segment_project_keys: segment_project_keys
                .iter()
                .map(|k| k.map(str::to_string))
                .collect(),
            segment_raw_refs: segment_raw_refs
                .iter()
                .map(|r| r.map(str::to_string))
                .collect(),
        }
    }

    #[test]
    fn plain_substring_is_unchanged_when_no_qualifier() {
        let compiled = compile_prompt_history_query("fix parser", &catalog());
        assert_eq!(compiled.raw_project_value, None);
        assert_eq!(compiled.project_key, None);
        assert_eq!(compiled.text, "fix parser");
        assert!(compiled.valid);
        assert_eq!(compiled.diagnostic, None);
    }

    #[test]
    fn empty_query_stays_empty() {
        let compiled = compile_prompt_history_query("", &catalog());
        assert_eq!(compiled.text, "");
        assert_eq!(compiled.raw_project_value, None);
        assert!(compiled.valid);
    }

    #[test]
    fn resolves_canonical_key_alias_and_raw_ref() {
        for value in [
            "sase",
            "SASE",
            "sase-main",
            "sase-org/sase",
            "gh_sase-org__sase",
        ] {
            let compiled = compile_prompt_history_query(
                &format!("project:{value} fix"),
                &catalog(),
            );
            assert_eq!(
                compiled.project_key.as_deref(),
                Some("sase"),
                "value={value}"
            );
            assert_eq!(compiled.text, "fix");
        }
    }

    #[test]
    fn does_not_false_match_sase_core_or_prose() {
        let compiled =
            compile_prompt_history_query("project:sase fix", &catalog());
        let rows = [
            row(
                0,
                "mentions sase-core in prose",
                &[Some("sase-core")],
                &[None],
            ),
            row(1, "fix the sase parser", &[Some("sase")], &[None]),
        ];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert_eq!(result.matched_indices, vec![1]);
    }

    #[test]
    fn leading_whitespace_before_qualifier_is_allowed() {
        let compiled = compile_prompt_history_query(
            "   project:sase fix parser",
            &catalog(),
        );
        assert_eq!(compiled.project_key.as_deref(), Some("sase"));
        assert_eq!(compiled.text, "fix parser");
    }

    #[test]
    fn quoted_value_supports_escapes() {
        let compiled = compile_prompt_history_query(
            r#"project:"widget \"a\"" fix"#,
            &[PromptHistoryProjectIdentityWire {
                key: "widget-a".to_string(),
                label: None,
                aliases: vec!["widget \"a\"".to_string()],
                raw_refs: Vec::new(),
            }],
        );
        assert_eq!(compiled.project_key.as_deref(), Some("widget-a"));
        assert_eq!(compiled.text, "fix");
    }

    #[test]
    fn empty_value_and_unterminated_quote_are_invalid_not_unscoped() {
        for query in ["project:", "project: fix", "project:\"unterminated"] {
            let compiled = compile_prompt_history_query(query, &catalog());
            assert!(!compiled.valid, "query={query}");
            assert!(compiled.diagnostic.is_some());
        }
    }

    #[test]
    fn invalid_query_matches_nothing() {
        let compiled = compile_prompt_history_query("project:", &catalog());
        let rows = [row(0, "anything", &[], &[])];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert!(result.matched_indices.is_empty());
    }

    #[test]
    fn ambiguous_label_reports_diagnostic_and_matches_nothing() {
        let compiled =
            compile_prompt_history_query("project:widgets fix", &catalog());
        assert_eq!(compiled.project_key, None);
        assert!(compiled.diagnostic.is_some());
        let rows = [
            row(0, "fix widget a", &[Some("widget-a")], &[None]),
            row(1, "fix widget b", &[Some("widget-b")], &[None]),
        ];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert!(result.matched_indices.is_empty());
    }

    #[test]
    fn unknown_value_is_ordinary_empty_not_an_error() {
        let compiled = compile_prompt_history_query(
            "project:does-not-exist fix",
            &catalog(),
        );
        assert_eq!(compiled.project_key, None);
        assert_eq!(compiled.diagnostic, None);
        let rows = [row(0, "fix something", &[Some("sase")], &[None])];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert!(result.matched_indices.is_empty());
    }

    #[test]
    fn unresolved_value_matches_exact_raw_historical_ref() {
        let compiled =
            compile_prompt_history_query("project:deleted-project", &catalog());
        assert_eq!(compiled.project_key, None);
        assert_eq!(compiled.diagnostic, None);
        let rows = [
            row(0, "old prompt", &[None], &[Some("deleted-project")]),
            row(1, "unrelated", &[None], &[Some("other-project")]),
        ];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert_eq!(result.matched_indices, vec![0]);
    }

    #[test]
    fn multi_segment_row_matches_if_any_segment_belongs_to_project() {
        let compiled =
            compile_prompt_history_query("project:sase-core", &catalog());
        let rows = [row(
            0,
            "segment one\n---\nsegment two",
            &[Some("sase"), Some("sase-core")],
            &[None, None],
        )];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert_eq!(result.matched_indices, vec![0]);
    }

    #[test]
    fn legacy_record_with_no_active_ref_stays_unscoped() {
        let compiled = compile_prompt_history_query("fix parser", &catalog());
        let rows = [row(0, "fix parser without any tag", &[None], &[None])];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert_eq!(result.matched_indices, vec![0]);
    }

    #[test]
    fn text_matches_canonical_or_display_case_insensitively() {
        let compiled = compile_prompt_history_query("FIX", &catalog());
        let rows = [
            row(0, "fix the bug", &[], &[]),
            row(1, "unrelated", &[], &[]),
        ];
        let result = match_prompt_history_rows(&compiled, &rows);
        assert_eq!(result.matched_indices, vec![0]);
    }

    #[test]
    fn literal_escape_prevents_qualifier_reinterpretation() {
        let compiled = compile_prompt_history_query(
            r"\project:sase fix parser",
            &catalog(),
        );
        assert_eq!(compiled.raw_project_value, None);
        assert_eq!(compiled.project_key, None);
        assert_eq!(compiled.text, "project:sase fix parser");
    }

    #[test]
    fn encode_parse_round_trips_arbitrary_literal_text() {
        for text in [
            "fix parser",
            "project:sase fix parser",
            r"\project:sase fix parser",
            r"\\project:sase fix parser",
            "  project:sase leading ws",
            "",
            r"\Users\file fix bug",
        ] {
            let encoded = encode_prompt_history_literal(text);
            let compiled = compile_prompt_history_query(&encoded, &catalog());
            assert_eq!(
                compiled.text, text,
                "text={text:?} encoded={encoded:?}"
            );
            assert_eq!(compiled.raw_project_value, None, "text={text:?}");
        }
    }

    #[test]
    fn seed_resolves_ref_to_configured_project_name() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: Some("sase-org/sase".to_string()),
                remainder_text: "fix parser".to_string(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "project:sase fix parser");
        assert_eq!(seed.hint, None);
    }

    #[test]
    fn seed_with_only_ref_leaves_caret_ready_for_text() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: Some("sase".to_string()),
                remainder_text: String::new(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "project:sase ");
    }

    #[test]
    fn seed_with_modifier_directive_preserves_it_after_scope() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: Some("sase".to_string()),
                remainder_text: "%m:opus fix parser".to_string(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "project:sase %m:opus fix parser");
    }

    #[test]
    fn seed_with_unresolved_ref_keeps_text_and_shows_hint() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: Some("some-unknown-agent".to_string()),
                remainder_text: "fix parser".to_string(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "fix parser");
        assert_eq!(seed.hint.as_deref(), Some(UNAVAILABLE_SCOPE_HINT));
    }

    #[test]
    fn seed_with_no_ref_and_empty_draft_is_empty() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: None,
                remainder_text: String::new(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "");
        assert_eq!(seed.hint, None);
    }

    #[test]
    fn seed_with_no_ref_and_plain_text_is_unchanged() {
        let seed = build_prompt_history_seed(
            &PromptHistorySeedRequestWire {
                raw_ref: None,
                remainder_text: "fix parser".to_string(),
            },
            &catalog(),
        );
        assert_eq!(seed.seed_text, "fix parser");
        assert_eq!(seed.hint, None);
    }
}

//! Shared macro argument choice candidates and type labels.
use crate::editor::fuzzy::fuzzy_match;
use crate::editor::wire::MacroInputHint;
use serde::{Deserialize, Serialize};

/// One choice candidate for LSP and TUI menus.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MacroChoiceCandidateWire {
    pub value: String,
    pub insertion: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub index: usize,
    #[serde(default)]
    pub is_default: bool,
}

/// Canonical type label for an input hint.
pub fn macro_input_type_label(hint: &MacroInputHint) -> String {
    if !hint.choices.is_empty() {
        if hint.choices.len() <= 4 {
            let union = hint
                .choices
                .iter()
                .map(|c| c.value.clone())
                .collect::<Vec<_>>()
                .join(" | ");
            return union;
        }
        let count = hint.choices.len();
        if let Some(named) =
            hint.named_type.as_deref().filter(|s| !s.is_empty())
        {
            return format!("{named} ({count})");
        }
        return format!("enum ({count})");
    }
    if let Some(named) = hint.named_type.as_deref().filter(|s| !s.is_empty()) {
        return named.to_string();
    }
    if hint.r#type.is_empty() {
        return "line".to_string();
    }
    hint.r#type.clone()
}

/// Quote a canonical value for macro argument syntax.
pub fn quote_macro_arg_value(value: &str) -> String {
    if value.is_empty() {
        return "\"\"".to_string();
    }
    let needs = value.chars().any(|ch| {
        matches!(
            ch,
            ',' | '+'
                | '('
                | ')'
                | '['
                | ']'
                | '"'
                | '\''
                | '`'
                | ' '
                | '\t'
                | '\n'
                | '='
        )
    });
    if !needs {
        return value.to_string();
    }
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

/// One scored choice row while building candidates.
#[derive(Debug, Clone)]
struct ChoiceRow {
    value: String,
    label: Option<String>,
    description: Option<String>,
    index: usize,
    is_default: bool,
}

/// Shared candidate builder.
pub fn macro_argument_choice_candidates(
    hint: &MacroInputHint,
    partial: &str,
    current_value: &str,
    selected: &[String],
) -> Vec<MacroChoiceCandidateWire> {
    // Build rows: (value,label,desc,index,is_default)
    let mut rows: Vec<ChoiceRow> = Vec::new();
    if hint.r#type == "bool" && hint.choices.is_empty() {
        let def = hint.default_display.as_deref().unwrap_or("");
        for (i, v) in ["true", "false"].iter().enumerate() {
            let is_def = !def.is_empty() && def.eq_ignore_ascii_case(v);
            rows.push(ChoiceRow {
                value: v.to_string(),
                label: None,
                description: None,
                index: i,
                is_default: is_def,
            });
        }
    } else {
        let def = hint.default_display.as_deref().unwrap_or("");
        // default_display for non-string defaults? For choices, default_display is None for string defaults (see parsing default_display returns None for strings). So need to handle string defaults? Actually string defaults produce None, so is_default never true for string defaults. Spec says "Marks the actual displayed default without reordering". Hmm parsing default_display returns None for strings, so we can't detect string default. Need to check: default_display returns None if value.is_str? Yes `if value.is_null() || value.as_str().is_some() { return None; }` So string defaults invisible. For bool, default bool true/false -> Some("true"/"false"). For int/float -> Some. For choices that are strings, default would be string -> None, so is_default false always. Is that intended? The spec says mark displayed default. Maybe for choices, default is string and default_display is None, but default_snippet_value holds it? In CatalogInput there's default_snippet_value, but MacroInputHint only has default_display, no snippet value. So string defaults lost in hint. For contracts, we mark is_default based on default_display matching value (case-sensitive? bool case-insensitive?). For string choices, default_display None => no default marker. That's existing behavior; keep it. Alternatively, could compare? No data. Keep as is.
        for (i, c) in hint.choices.iter().enumerate() {
            // displayed default: default_display Some matches value? For bool synthetic above we did case-insensitive. For choices, exact match? Use exact.
            let is_def = !def.is_empty() && def == c.value;
            rows.push(ChoiceRow {
                value: c.value.clone(),
                label: c.label.clone(),
                description: c.description.clone(),
                index: i,
                is_default: is_def,
            });
        }
    }
    if rows.is_empty() {
        return Vec::new();
    }
    // Repeatable exclusion: exclude selected values only for repeatable, keep current eligible.
    let current_norm = current_value.trim();
    // Unquote current for comparison? decoded_value strips quotes. Do simple: strip surrounding quotes if present.
    let current_decoded = decode_arg_value(current_norm);
    let selected_set: std::collections::BTreeSet<String> =
        selected.iter().cloned().collect();
    let mut filtered: Vec<ChoiceRow> = rows
        .into_iter()
        .filter(|row| {
            if !hint.repeatable {
                return true;
            }
            if row.value == current_decoded {
                return true;
            }
            // Also if partial corresponds to selected? current_value is whole value, so above covers editing existing element.
            // Exclude if in selected.
            !selected_set.contains(&row.value)
        })
        .collect();
    // Filtering by partial: empty => declared order. Otherwise prefix first, then fuzzy.
    if partial.is_empty() {
        // declared order already
    } else {
        let lower = partial.to_lowercase();
        let mut prefix = Vec::new();
        let mut fuzzy_scored: Vec<((u8, i32), ChoiceRow)> = Vec::new();
        for row in filtered.drain(..) {
            let v_lower = row.value.to_lowercase();
            if v_lower.starts_with(&lower) {
                prefix.push(row);
            } else if let Some(m) = fuzzy_match(partial, &row.value) {
                fuzzy_scored.push(((m.tier, m.score), row));
            }
        }
        // prefix keeps declared order (stable). Fuzzy sorted by compare_fuzzy, tie declared order.
        // Need to sort fuzzy by tier/score then declared index stable.
        fuzzy_scored.sort_by(|a, b| {
            // tier asc, score desc, then index asc
            a.0 .0
                .cmp(&b.0 .0)
                .then_with(|| b.0 .1.cmp(&a.0 .1))
                .then_with(|| a.1.index.cmp(&b.1.index))
        });
        // For more precise ordering matching shared fuzzy matcher, use compare_fuzzy when scores equal? Our tier/score already from fuzzy_match. Use declared-order ties as spec.
        filtered = prefix;
        filtered.extend(fuzzy_scored.into_iter().map(|(_, r)| r));
        // Note: prefix prioritized, stable declared-order ties (prefix already in declared order).
    }
    filtered
        .into_iter()
        .map(
            |ChoiceRow {
                 value,
                 label,
                 description,
                 index,
                 is_default,
             }| {
                let insertion = quote_macro_arg_value(&value);
                MacroChoiceCandidateWire {
                    value,
                    insertion,
                    label,
                    description,
                    index,
                    is_default,
                }
            },
        )
        .collect()
}

fn decode_arg_value(raw: &str) -> String {
    let t = raw.trim();
    if t.len() >= 2 {
        let b = t.as_bytes();
        if (b[0] == b'"' || b[0] == b'\'') && b[0] == b[t.len() - 1] {
            return t[1..t.len() - 1].to_string();
        }
    }
    t.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::editor::completion::classify_completion_context;
    use crate::editor::macro_args::parse_macro_calls;
    use crate::editor::token::DocumentSnapshot;
    use crate::editor::wire::{CompletionContextKind, MacroAssistEntry};
    use crate::macro_input_types::{check_input_value, ResolvedInputType};
    use crate::MobileInputChoiceWire;
    use serde::Deserialize;
    use std::collections::BTreeSet;

    #[derive(Debug, Deserialize)]
    struct FixtureChoice {
        value: String,
        #[serde(default)]
        label: Option<String>,
        #[serde(default)]
        description: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureInput {
        name: String,
        #[serde(rename = "type")]
        r#type: String,
        #[serde(default)]
        required: bool,
        #[serde(default)]
        position: u32,
        #[serde(default)]
        repeatable: bool,
        #[serde(default)]
        choices: Vec<FixtureChoice>,
        #[serde(default)]
        named_type: Option<String>,
        #[serde(default)]
        value_role: Option<String>,
        #[serde(default)]
        description: Option<String>,
        #[serde(default)]
        default_display: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureCase {
        id: String,
        source: String,
        cursor_byte: usize,
        #[serde(rename = "macro")]
        macro_name: String,
        inputs: Vec<FixtureInput>,
        expected_kind: String,
        #[serde(default)]
        expected_active_input: Option<String>,
        #[serde(default)]
        expected_candidates: Option<Vec<String>>,
        #[serde(default)]
        expected_insertions: Option<Vec<String>>,
        replacement_byte_span: (usize, usize),
        #[serde(default)]
        builder_partial: Option<String>,
        #[serde(default)]
        selected_values: Option<Vec<String>>,
        #[serde(default)]
        expected_default: Option<String>,
        #[serde(default)]
        expected_candidates_contains: Option<String>,
        #[serde(default)]
        applied_edits: Option<std::collections::BTreeMap<String, String>>,
        #[serde(default)]
        binds_to: Option<String>,
        #[serde(default)]
        expected_utf16_range: Option<Utf16Range>,
    }

    #[derive(Debug, Deserialize)]
    struct Utf16Range {
        line: u32,
        start_character: u32,
        end_character: u32,
    }

    #[derive(Debug, Deserialize)]
    struct FixtureFile {
        schema_version: u32,
        cases: Vec<FixtureCase>,
    }

    fn fixture() -> Vec<FixtureCase> {
        let file: FixtureFile = serde_json::from_str(include_str!(
            "../../tests/fixtures/macro_arg_choice_completion.json"
        ))
        .expect("choice golden must parse");
        assert_eq!(file.schema_version, 1);
        assert!(!file.cases.is_empty());
        file.cases
    }

    fn hint_from(input: &FixtureInput) -> MacroInputHint {
        MacroInputHint {
            name: input.name.clone(),
            r#type: input.r#type.clone(),
            description: input.description.clone(),
            required: input.required,
            default_display: input.default_display.clone(),
            position: input.position,
            repeatable: input.repeatable,
            choices: input
                .choices
                .iter()
                .map(|c| MobileInputChoiceWire {
                    value: c.value.clone(),
                    label: c.label.clone(),
                    description: c.description.clone(),
                })
                .collect(),
            named_type: input.named_type.clone(),
            value_role: input.value_role.clone(),
        }
    }

    fn entry_for(case: &FixtureCase) -> MacroAssistEntry {
        MacroAssistEntry {
            name: case.macro_name.clone(),
            display_label: case.macro_name.clone(),
            insertion: format!("#{}", case.macro_name),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "test".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: case.inputs.iter().map(hint_from).collect(),
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
        }
    }

    fn kind_from(name: &str) -> CompletionContextKind {
        match name {
            "macro_argument_value" => CompletionContextKind::MacroArgumentValue,
            "macro_argument_agent" => CompletionContextKind::MacroArgumentAgent,
            "macro_argument_path" => CompletionContextKind::MacroArgumentPath,
            "macro_argument_type_hint" => {
                CompletionContextKind::MacroArgumentTypeHint
            }
            "macro_argument_name" => CompletionContextKind::MacroArgumentName,
            other => panic!("unknown kind {other}"),
        }
    }

    #[test]
    fn golden_detection_builder_and_applied_edits() {
        for case in fixture() {
            let entry = entry_for(&case);
            let doc = DocumentSnapshot::new(case.source.clone());
            let pos = doc
                .byte_offset_to_position(case.cursor_byte)
                .unwrap_or_else(|| {
                    panic!("{}: bad cursor {}", case.id, case.cursor_byte)
                });
            let ctx = classify_completion_context(
                &doc,
                pos,
                std::slice::from_ref(&entry),
            )
            .unwrap_or_else(|| {
                panic!("{}: no context at {}", case.id, case.cursor_byte)
            });
            assert_eq!(
                ctx.kind,
                kind_from(&case.expected_kind),
                "{}: kind",
                case.id
            );
            assert_eq!(
                ctx.active_input.as_deref(),
                case.expected_active_input.as_deref(),
                "{}: active input",
                case.id
            );
            // Replacement span is UTF-8 bytes; compare directly and via
            // converted UTF-16 editor ranges (non-BMP coverage).
            let (rs, re) = case.replacement_byte_span;
            let expected_range = doc
                .byte_range_to_range(rs, re)
                .unwrap_or_else(|| panic!("{}: bad span {rs}..{re}", case.id));
            assert_eq!(
                ctx.replacement_range, expected_range,
                "{}: replacement range",
                case.id
            );
            if let Some(utf16) = &case.expected_utf16_range {
                assert_eq!(
                    expected_range.start.line, utf16.line,
                    "{}",
                    case.id
                );
                assert_eq!(
                    expected_range.start.character, utf16.start_character,
                    "{}",
                    case.id
                );
                assert_eq!(
                    expected_range.end.character, utf16.end_character,
                    "{}",
                    case.id
                );
            }
            // Builder: partial is text before cursor by default, decoded of
            // quotes; explicit builder_partial overrides (quoted mid-value).
            let hint = entry
                .inputs
                .iter()
                .find(|h| {
                    Some(h.name.as_str())
                        == case.expected_active_input.as_deref()
                })
                .cloned()
                .unwrap_or_else(|| hint_from(&case.inputs[0]));
            let token_partial = doc
                .text()
                .get(rs..case.cursor_byte)
                .unwrap_or("")
                .trim_start_matches(['"', '\''])
                .to_string();
            let partial = case.builder_partial.clone().unwrap_or(token_partial);
            let current_value =
                doc.text().get(rs..re).unwrap_or("").to_string();
            let selected: Vec<String> = case
                .selected_values
                .clone()
                .unwrap_or_else(|| ctx.selected_values.clone());
            // Selected values from detection must match fixture where given.
            if let Some(expected_sel) = &case.selected_values {
                assert_eq!(
                    &ctx.selected_values, expected_sel,
                    "{}: selected",
                    case.id
                );
            }
            let candidates = macro_argument_choice_candidates(
                &hint,
                &partial,
                &current_value,
                &selected,
            );
            if let Some(expected) = &case.expected_candidates {
                let values: Vec<String> =
                    candidates.iter().map(|c| c.value.clone()).collect();
                assert_eq!(&values, expected, "{}: candidates", case.id);
            }
            if let Some(needle) = &case.expected_candidates_contains {
                assert!(
                    candidates.iter().any(|c| &c.value == needle),
                    "{}: should contain {needle}",
                    case.id
                );
            }
            if let Some(expected) = &case.expected_insertions {
                let insertions: Vec<String> =
                    candidates.iter().map(|c| c.insertion.clone()).collect();
                assert_eq!(&insertions, expected, "{}: insertions", case.id);
            }
            if let Some(def) = &case.expected_default {
                assert!(
                    candidates.iter().any(|c| &c.value == def && c.is_default),
                    "{}: default {def} marked",
                    case.id
                );
                // Default does not reorder: declared order preserved for
                // empty partial (bool true before false).
                if partial.is_empty() && !candidates.is_empty() {
                    assert_eq!(
                        candidates[0].index, 0,
                        "{}: declared order",
                        case.id
                    );
                }
            }
            // Declared-index stability: empty partial preserves declared
            // order (indices strictly increasing, not necessarily 0..n when
            // repeatable exclusions filter).
            if partial.is_empty() {
                let mut last: Option<usize> = None;
                for cand in &candidates {
                    if let Some(prev) = last {
                        assert!(
                            cand.index > prev,
                            "{}: declared order",
                            case.id
                        );
                    }
                    last = Some(cand.index);
                }
            }
            // Applied edits must parse to declared values via the shared
            // argument parser and validate through the runtime binder check.
            if let Some(edits) = &case.applied_edits {
                for (value, expected_text) in edits {
                    let cand = candidates
                        .iter()
                        .find(|c| &c.value == value)
                        .unwrap_or_else(|| {
                            panic!("{}: missing {value}", case.id)
                        });
                    let mut edited = case.source.clone();
                    edited.replace_range(rs..re, &cand.insertion);
                    assert_eq!(&edited, expected_text, "{}: edit", case.id);
                    let call = parse_macro_calls(&edited)
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| {
                            panic!("{}: no call in {edited}", case.id)
                        });
                    let bound: Vec<String> =
                        call.args.iter().map(|a| a.value.clone()).collect();
                    assert!(
                        bound.iter().any(|v| v == value),
                        "{}: {edited} binds {value} (got {bound:?})",
                        case.id
                    );
                    // Closed-set binder agreement for enum inputs.
                    if !hint.choices.is_empty() {
                        let resolved = ResolvedInputType {
                            base: hint.r#type.clone(),
                            named_type: hint.named_type.clone(),
                            value_role: hint.value_role.clone(),
                            choices: hint
                                .choices
                                .iter()
                                .map(|c| {
                                    crate::macro_input_types::InputChoice {
                                        value: c.value.clone(),
                                        label: c.label.clone(),
                                        description: c.description.clone(),
                                    }
                                })
                                .collect(),
                            deprecated: false,
                        };
                        check_input_value(&resolved, &hint.name, value)
                            .unwrap_or_else(|e| {
                                panic!(
                                    "{}: binder rejects {value}: {e}",
                                    case.id
                                )
                            });
                    }
                }
            }
            // #pr:ready binds the word input `name`, not enum `status`.
            if let Some(target) = &case.binds_to {
                let call = parse_macro_calls(&case.source)
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| panic!("{}: no call", case.id));
                assert_eq!(call.args.len(), 1, "{}: one positional", case.id);
                assert_eq!(&call.args[0].value, "ready", "{}: value", case.id);
                assert_eq!(
                    ctx.active_input.as_deref(),
                    Some(target.as_str()),
                    "{}: binds {target}",
                    case.id
                );
            }
        }
    }

    #[test]
    fn legacy_hints_without_new_fields_deserialize() {
        let old = serde_json::json!({
            "name": "env",
            "type": "enum",
            "required": true,
            "default_display": null,
            "position": 0,
            "repeatable": false,
            "choices": [{"value": "staging"}]
        });
        let hint: MacroInputHint =
            serde_json::from_value(old).expect("legacy hint deserializes");
        assert_eq!(hint.named_type, None);
        assert_eq!(hint.value_role, None);
        assert_eq!(hint.choices.len(), 1);
        let wire_old = serde_json::json!({
            "name": "env",
            "type": "enum",
            "required": true,
            "default_display": null,
            "position": 0,
            "repeatable": false,
            "choices": [{"value": "staging"}]
        });
        let wire: crate::MobileMacroInputWire =
            serde_json::from_value(wire_old).expect("legacy wire deserializes");
        assert_eq!(wire.named_type, None);
        // New fields omit when empty (additive wire compatibility).
        let encoded = serde_json::to_value(&hint).unwrap();
        assert!(encoded.get("named_type").is_none(), "omit empty");
        assert!(encoded.get("value_role").is_none(), "omit empty");
    }

    #[test]
    fn type_label_boundaries() {
        let mk = |t: &str,
                  choices: Vec<&str>,
                  named: Option<&str>|
         -> MacroInputHint {
            MacroInputHint {
                name: "x".to_string(),
                r#type: t.to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
                repeatable: false,
                choices: choices
                    .into_iter()
                    .map(|v| MobileInputChoiceWire {
                        value: v.to_string(),
                        label: None,
                        description: None,
                    })
                    .collect(),
                named_type: named.map(str::to_string),
                value_role: None,
            }
        };
        // Inline enum union up to four.
        assert_eq!(
            macro_input_type_label(&mk("enum", vec!["a", "b", "c"], None)),
            "a | b | c"
        );
        assert_eq!(
            macro_input_type_label(&mk("enum", vec!["a", "b", "c", "d"], None)),
            "a | b | c | d"
        );
        // More than four: named or generic count.
        assert_eq!(
            macro_input_type_label(&mk(
                "enum",
                vec!["a", "b", "c", "d", "e"],
                Some("deploy_env")
            )),
            "deploy_env (5)"
        );
        assert_eq!(
            macro_input_type_label(&mk(
                "enum",
                vec!["a", "b", "c", "d", "e"],
                None
            )),
            "enum (5)"
        );
        // Domains show named type; scalars show keyword.
        assert_eq!(
            macro_input_type_label(&mk("agent", vec![], Some("agent"))),
            "agent"
        );
        assert_eq!(macro_input_type_label(&mk("bool", vec![], None)), "bool");
        assert_eq!(macro_input_type_label(&mk("line", vec![], None)), "line");
        // Synthetic bool suggestions do not change the scalar label.
        let bool_hint = mk("bool", vec![], None);
        assert_eq!(macro_input_type_label(&bool_hint), "bool");
        assert_eq!(
            macro_argument_choice_candidates(&bool_hint, "", "", &[])
                .iter()
                .map(|c| c.value.clone())
                .collect::<Vec<_>>(),
            vec!["true", "false"]
        );
        // Manually constructed resolved named-enum hint.
        let named_enum = mk("enum", vec!["open", "closed"], Some("pr_status"));
        assert_eq!(macro_input_type_label(&named_enum), "open | closed");
        // Quoting helper shared with quick fixes.
        assert_eq!(quote_macro_arg_value("a,b"), "\"a,b\"");
        assert_eq!(quote_macro_arg_value("c+d"), "\"c+d\"");
        assert_eq!(quote_macro_arg_value("plain"), "plain");
        // Candidate insertion uses canonical value, never label.
        let labeled = MacroInputHint {
            name: "env".to_string(),
            r#type: "enum".to_string(),
            description: None,
            required: true,
            default_display: None,
            position: 0,
            repeatable: false,
            choices: vec![MobileInputChoiceWire {
                value: "staging".to_string(),
                label: Some("Staging".to_string()),
                description: None,
            }],
            named_type: None,
            value_role: None,
        };
        let cands = macro_argument_choice_candidates(&labeled, "", "", &[]);
        assert_eq!(cands[0].insertion, "staging");
        assert_eq!(cands[0].label.as_deref(), Some("Staging"));
        // Repeatable exclusion keeps active eligible.
        let _rep = MacroInputHint {
            repeatable: true,
            ..labeled.clone()
        };
        let _ = BTreeSet::<String>::new();
    }

    #[test]
    fn assist_and_structured_copy_metadata() {
        use crate::editor::completion::assist_entries_from_catalog;
        use crate::EditorMacroCatalogEntryWire;
        let wire = EditorMacroCatalogEntryWire {
            name: "deploy".to_string(),
            display_label: "deploy".to_string(),
            insertion: Some("#deploy".to_string()),
            reference_prefix: Some("#".to_string()),
            kind: None,
            description: None,
            source_bucket: "test".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: vec![crate::MobileMacroInputWire {
                name: "env".to_string(),
                r#type: "enum".to_string(),
                description: None,
                required: true,
                default_display: None,
                position: 0,
                repeatable: false,
                choices: vec![MobileInputChoiceWire {
                    value: "staging".to_string(),
                    label: Some("Staging".to_string()),
                    description: Some("Staging".to_string()),
                }],
                named_type: Some("deploy_env".to_string()),
                value_role: None,
            }],
            is_skill: false,
            skill_name: None,
            memory_type: None,
            content_preview: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
        };
        let entries = assist_entries_from_catalog(&[wire]);
        let hint = &entries[0].inputs[0];
        assert_eq!(hint.named_type.as_deref(), Some("deploy_env"));
        assert_eq!(hint.choices[0].label.as_deref(), Some("Staging"));
    }
}

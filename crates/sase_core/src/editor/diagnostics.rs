use regex::Regex;
use serde_yaml::{Mapping, Value};
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::macro_input_types::{
    check_input_value, resolve_input_type, suggest_closest,
    validate_enum_choices_yaml, InputChoice, InputTypeRegistry,
    ResolvedInputType,
};
use crate::model_validity::{classify_model_value, ModelValiditySnapshot};
use crate::{
    content_layout::is_reserved_memory_reference, fenced_block_ranges,
    inline_code_ranges, parse_artifact_ref, prompt_literal_zone_ranges,
    resolve_artifact_ref, scan_artifact_refs, scan_directive_owned_fences,
    typed_launch_units_flag_key, ArtifactRefContextWire, ArtifactRefKindWire,
    MobileInputChoiceWire,
};

use super::alternation::{scan_alternations, AlternationFormWire};
use super::at_reference::BUILTIN_ARTIFACT_REF_KINDS;
use super::directive::canonical_directive_name;
use super::frontmatter;
use super::macro_arg_choices::quote_macro_arg_value;
use super::macro_args::{parse_macro_calls, MacroArgSyntax, ParsedMacroArg};
use super::placeholder::extract_placeholder_spans;
use super::token::DocumentSnapshot;
use super::wire::{
    DiagnosticSeverity, EditorDiagnostic, EditorDiagnosticData,
    EditorDiagnosticSuggestion, EditorTextEdit, MacroAssistEntry,
    MacroInputHint,
};

/// Externally visible code for an invalid closed-set argument value.
pub(crate) const INVALID_MACRO_ARG_CHOICE: &str = "invalid_macro_arg_choice";

/// Externally visible code for an invalid model argument value.
pub(crate) const INVALID_MACRO_ARG_MODEL: &str = "invalid_macro_arg_model";

pub fn analyze_document(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
) -> Vec<EditorDiagnostic> {
    analyze_document_with_snapshot(document, entries, None)
}

pub fn analyze_document_with_snapshot(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
    snapshot: Option<&ModelValiditySnapshot>,
) -> Vec<EditorDiagnostic> {
    let local_entries = local_macro_entries(document);
    let combined_entries;
    let entries = if local_entries.is_empty() {
        entries
    } else {
        combined_entries = merged_entries(local_entries, entries);
        combined_entries.as_slice()
    };

    let mut diagnostics = Vec::new();
    diagnostics
        .extend(frontmatter::diagnostics_with_snapshot(document, snapshot));
    diagnostics.extend(macro_diagnostics(document, entries));
    diagnostics.extend(slash_skill_diagnostics(document, entries));
    diagnostics.extend(directive_diagnostics(document));
    diagnostics.extend(alternation_diagnostics(document));
    diagnostics.extend(argument_diagnostics_with_snapshot(
        document, entries, snapshot,
    ));
    diagnostics
}

pub fn argument_diagnostics(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
) -> Vec<EditorDiagnostic> {
    argument_diagnostics_with_snapshot(document, entries, None)
}

pub fn argument_diagnostics_with_snapshot(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
    snapshot: Option<&ModelValiditySnapshot>,
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for call in parse_macro_calls(document.text()) {
        let Some(entry) = entries.iter().find(|entry| entry.name == call.name)
        else {
            continue;
        };
        if matches!(call.syntax, MacroArgSyntax::Malformed) {
            let Some((start, end)) = call.malformed_span else {
                continue;
            };
            push_diagnostic(
                document,
                &mut out,
                start,
                end,
                "malformed_macro_argument",
                "Malformed macro argument form".to_string(),
            );
            if entry.inputs.is_empty() || call.is_open {
                continue;
            }
        }
        if call.is_open || entry.inputs.is_empty() {
            continue;
        }
        validate_call_args_with_snapshot(
            document, entry, &call, &mut out, snapshot,
        );
    }
    out
}

pub fn analyze_artifact_refs(
    document: &DocumentSnapshot,
    context: &ArtifactRefContextWire,
) -> Vec<EditorDiagnostic> {
    let literal_ranges = prompt_literal_zone_ranges(document.text());
    let known_kinds = known_artifact_ref_kinds(context);
    let mut diagnostics = Vec::new();
    for candidate in scan_artifact_refs(document.text()) {
        let span =
            (candidate.candidate_span.start, candidate.candidate_span.end);
        if !known_kinds.contains(&candidate.kind)
            || literal_ranges
                .iter()
                .any(|literal| ranges_intersect(span, *literal))
        {
            continue;
        }

        let parsed = match parse_artifact_ref(&candidate.reference) {
            Ok(parsed) => parsed,
            Err(error) => {
                push_diagnostic(
                    document,
                    &mut diagnostics,
                    span.0,
                    span.1,
                    "malformed_artifact_ref",
                    format!(
                        "Malformed artifact reference `{}`: {}",
                        candidate.text, error.message
                    ),
                );
                continue;
            }
        };
        if matches!(
            &parsed.kind,
            ArtifactRefKindWire::Commit | ArtifactRefKindWire::Bug
        ) {
            continue;
        }
        match resolve_artifact_ref(&parsed, context) {
            Ok(resolution) if resolution.resolved_path.is_some() => {}
            Ok(resolution) => push_diagnostic(
                document,
                &mut diagnostics,
                span.0,
                span.1,
                "unresolved_artifact_ref",
                format!(
                    "Unresolved artifact reference `@{}` ({})",
                    resolution.rendered, resolution.status
                ),
            ),
            Err(error) => push_diagnostic(
                document,
                &mut diagnostics,
                span.0,
                span.1,
                "unresolved_artifact_ref",
                format!(
                    "Unable to resolve artifact reference `@{}`: {}",
                    parsed.rendered, error.message
                ),
            ),
        }
    }
    diagnostics
}

pub fn queue_directive_diagnostics(
    document: &DocumentSnapshot,
    _queue_directive_enabled: bool,
) -> Vec<EditorDiagnostic> {
    let _ = document;
    Vec::new()
}

pub fn typed_launch_directive_diagnostics(
    document: &DocumentSnapshot,
    typed_launch_units_enabled: bool,
) -> Vec<EditorDiagnostic> {
    if !typed_launch_units_enabled {
        return disabled_typed_launch_directive_diagnostics(document);
    }

    scan_directive_owned_fences(document.text())
        .diagnostics
        .into_iter()
        .filter_map(|diagnostic| {
            document
                .byte_range_to_range(diagnostic.span[0], diagnostic.span[1])
                .map(|range| {
                    EditorDiagnostic::new(
                        range,
                        DiagnosticSeverity::Error,
                        format!("typed_launch_{}", diagnostic.code),
                        diagnostic.message,
                    )
                })
        })
        .collect()
}

fn disabled_typed_launch_directive_diagnostics(
    document: &DocumentSnapshot,
) -> Vec<EditorDiagnostic> {
    let mut literal_ranges = fenced_block_ranges(document.text());
    literal_ranges.extend(inline_code_ranges(document.text(), &literal_ranges));
    typed_launch_directive_re()
        .captures_iter(document.text())
        .filter_map(|captures| {
            let marker = captures.get(0)?;
            let span = (marker.start(), marker.end());
            if literal_ranges
                .iter()
                .any(|literal| ranges_intersect(span, *literal))
            {
                return None;
            }
            let name = captures.name("name")?.as_str();
            if name == "if" && document.text()[marker.end()..].starts_with('(')
            {
                return None;
            }
            document.byte_range_to_range(span.0, span.1).map(|range| {
                EditorDiagnostic::new(
                    range,
                    DiagnosticSeverity::Error,
                    "typed_launch_units_disabled",
                    format!(
                        "%{name} requires the {} feature flag",
                        typed_launch_units_flag_key()
                    ),
                )
            })
        })
        .collect()
}

fn known_artifact_ref_kinds(
    context: &ArtifactRefContextWire,
) -> HashSet<String> {
    BUILTIN_ARTIFACT_REF_KINDS
        .iter()
        .map(|kind| (*kind).to_string())
        .chain(context.document_roots.iter().map(|root| root.kind.clone()))
        .collect()
}

fn ranges_intersect(left: (usize, usize), right: (usize, usize)) -> bool {
    left.0 < right.1 && right.0 < left.1
}

fn merged_entries(
    mut local_entries: Vec<MacroAssistEntry>,
    entries: &[MacroAssistEntry],
) -> Vec<MacroAssistEntry> {
    local_entries.extend_from_slice(entries);
    local_entries
}

fn macro_diagnostics(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in macro_ref_re().captures_iter(document.text()) {
        let Some(marker) = caps.name("marker") else {
            continue;
        };
        let Some(name_match) = caps.name("name") else {
            continue;
        };
        let name = name_match.as_str().replace("__", "/");
        let Some(entry) = entries.iter().find(|entry| entry.name == name)
        else {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), name_match.end())
            {
                out.push(EditorDiagnostic::new(
                    range,
                    DiagnosticSeverity::Warning,
                    "unknown_macro",
                    format!("Unknown macro `{name}`"),
                ));
            }
            continue;
        };
        if entry.reference_prefix != marker.as_str() {
            if let Some(range) =
                document.byte_range_to_range(marker.start(), marker.end())
            {
                out.push(EditorDiagnostic::new(
                    range,
                    DiagnosticSeverity::Information,
                    "canonical_marker_mismatch",
                    format!(
                        "`{}` is canonical for `{}`",
                        entry.reference_prefix, entry.name
                    ),
                ));
            }
        }
    }
    out
}

fn slash_skill_diagnostics(
    document: &DocumentSnapshot,
    entries: &[MacroAssistEntry],
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in slash_skill_re().captures_iter(document.text()) {
        let Some(skill) = caps.name("skill") else {
            continue;
        };
        if entries.iter().any(|entry| {
            entry.is_skill
                && entry.skill_name.as_deref() == Some(skill.as_str())
        }) {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(skill.start() - 1, skill.end())
        {
            out.push(EditorDiagnostic::new(
                range,
                DiagnosticSeverity::Warning,
                "unknown_slash_skill",
                format!("Unknown slash skill `/{}`", skill.as_str()),
            ));
        }
    }
    out
}

fn directive_diagnostics(document: &DocumentSnapshot) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for caps in directive_re().captures_iter(document.text()) {
        let Some(name) = caps.name("name") else {
            continue;
        };
        if canonical_directive_name(name.as_str()).is_some() {
            continue;
        }
        if let Some(range) =
            document.byte_range_to_range(name.start() - 1, name.end())
        {
            out.push(EditorDiagnostic::new(
                range,
                DiagnosticSeverity::Information,
                "unknown_directive",
                format!("Unknown directive `%{}`", name.as_str()),
            ));
        }
    }
    out
}

/// One error per unclosed alternation opener outside literal zones,
/// reusing launch's wording. The marker span is the error span.
fn alternation_diagnostics(
    document: &DocumentSnapshot,
) -> Vec<EditorDiagnostic> {
    let mut out = Vec::new();
    for record in scan_alternations(document.text()) {
        if record.close.is_some() {
            continue;
        }
        let (name, close) = match record.form {
            AlternationFormWire::Brace => ("%{", '}'),
            AlternationFormWire::Paren => ("%alt", ')'),
        };
        push_diagnostic(
            document,
            &mut out,
            record.marker_start,
            record.opener_end,
            "unclosed_alternation",
            format!("unclosed {name} directive: missing closing '{close}'"),
        );
    }
    out
}

fn validate_call_args_with_snapshot(
    document: &DocumentSnapshot,
    entry: &MacroAssistEntry,
    call: &super::macro_args::ParsedMacroCall,
    out: &mut Vec<EditorDiagnostic>,
    snapshot: Option<&ModelValiditySnapshot>,
) {
    for validation in
        validate_macro_call_args_with_snapshot(entry, call, snapshot)
    {
        let data = if validation.code == INVALID_MACRO_ARG_MODEL {
            model_suggestion_data(
                document,
                validation.span,
                &validation.suggestion_values,
            )
        } else {
            choice_suggestion_data(
                document,
                validation.span,
                &validation.suggestion_values,
            )
        };
        push_diagnostic_with_severity_and_data(
            document,
            out,
            validation.span.0,
            validation.span.1,
            validation.severity,
            validation.code,
            validation.message,
            data,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MacroArgValidationKind {
    DuplicateKey,
    UnknownKey,
    TypeMismatch,
    TooManyArgs,
    MissingRequiredArg,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MacroArgValidation {
    pub(crate) kind: MacroArgValidationKind,
    pub(crate) arg_index: Option<usize>,
    pub(crate) span: (usize, usize),
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) suggestion_values: Vec<String>,
    pub(crate) severity: DiagnosticSeverity,
}

fn arg_validation(
    kind: MacroArgValidationKind,
    arg_index: Option<usize>,
    span: (usize, usize),
    code: &'static str,
    message: String,
) -> MacroArgValidation {
    MacroArgValidation {
        kind,
        arg_index,
        span,
        code,
        message,
        suggestion_values: Vec::new(),
        severity: DiagnosticSeverity::Error,
    }
}

pub(crate) fn validate_macro_call_args(
    entry: &MacroAssistEntry,
    call: &super::macro_args::ParsedMacroCall,
) -> Vec<MacroArgValidation> {
    validate_macro_call_args_with_snapshot(entry, call, None)
}

pub(crate) fn validate_macro_call_args_with_snapshot(
    entry: &MacroAssistEntry,
    call: &super::macro_args::ParsedMacroCall,
    snapshot: Option<&ModelValiditySnapshot>,
) -> Vec<MacroArgValidation> {
    let mut out = Vec::new();
    let mut supplied_inputs = HashSet::new();
    let mut seen_named_args = HashSet::new();
    let mut positional_index = 0usize;

    for (arg_index, arg) in call.args.iter().enumerate() {
        if let Some(name) = &arg.name {
            if !seen_named_args.insert(name.value.clone()) {
                out.push(arg_validation(
                    MacroArgValidationKind::DuplicateKey,
                    Some(arg_index),
                    name.span,
                    "duplicate_macro_arg",
                    format!("Duplicate macro argument `{}`", name.value),
                ));
                continue;
            }
            let Some(input) =
                entry.inputs.iter().find(|input| input.name == name.value)
            else {
                out.push(arg_validation(
                    MacroArgValidationKind::UnknownKey,
                    Some(arg_index),
                    name.span,
                    "unknown_macro_arg",
                    format!(
                        "Unknown argument `{}` for macro `{}`",
                        name.value, entry.name
                    ),
                ));
                continue;
            };
            supplied_inputs.insert(input.name.clone());
            validate_type_with_snapshot(
                entry, input, arg_index, arg, &mut out, snapshot,
            );
        } else {
            let Some(input) = input_for_position(entry, positional_index)
            else {
                out.push(arg_validation(
                    MacroArgValidationKind::TooManyArgs,
                    Some(arg_index),
                    arg.value_span,
                    "too_many_args",
                    format!(
                        "Too many positional arguments for `{}`",
                        entry.name
                    ),
                ));
                positional_index += 1;
                continue;
            };
            supplied_inputs.insert(input.name.clone());
            validate_type_with_snapshot(
                entry, input, arg_index, arg, &mut out, snapshot,
            );
            positional_index += 1;
        }
    }

    for input in entry.inputs.iter().filter(|input| input.required) {
        if supplied_inputs.contains(&input.name) {
            continue;
        }
        out.push(arg_validation(
            MacroArgValidationKind::MissingRequiredArg,
            None,
            call.name_span,
            "missing_required_arg",
            format!(
                "Missing required argument `{}` for macro `{}`",
                input.name, entry.name
            ),
        ));
    }
    out
}

fn is_model_hint(input: &MacroInputHint) -> bool {
    input.value_role.as_deref() == Some("model")
}

fn validate_type_with_snapshot(
    entry: &MacroAssistEntry,
    input: &MacroInputHint,
    arg_index: usize,
    arg: &ParsedMacroArg,
    out: &mut Vec<MacroArgValidation>,
    snapshot: Option<&ModelValiditySnapshot>,
) {
    if arg.value == "null" || macro_arg_value_unresolvable(&arg.value) {
        return;
    }
    if is_model_hint(input) {
        let Some(snapshot) = snapshot else {
            return;
        };
        let stripped = strip_arg_quotes(&arg.value);
        if stripped.is_empty() {
            return;
        }
        match classify_model_value(&input.name, stripped, snapshot) {
            Ok(result) if result.ok => return,
            Ok(result) => {
                out.push(MacroArgValidation {
                    kind: MacroArgValidationKind::TypeMismatch,
                    arg_index: Some(arg_index),
                    span: arg.value_span,
                    code: INVALID_MACRO_ARG_MODEL,
                    message: result.message,
                    suggestion_values: result.suggestions,
                    severity: DiagnosticSeverity::Warning,
                });
                return;
            }
            Err(_) => return,
        }
    }
    if !input.choices.is_empty() {
        let resolved = resolved_hint(input);
        if let Err(message) =
            check_input_value(&resolved, &input.name, &arg.value)
        {
            let suggestion_values = suggest_closest(
                &arg.value,
                input.choices.iter().map(|choice| choice.value.as_str()),
            );
            out.push(MacroArgValidation {
                kind: MacroArgValidationKind::TypeMismatch,
                arg_index: Some(arg_index),
                span: arg.value_span,
                code: INVALID_MACRO_ARG_CHOICE,
                message,
                suggestion_values,
                severity: DiagnosticSeverity::Error,
            });
        }
        return;
    }
    if value_matches_input_type(&arg.value, input) {
        return;
    }
    out.push(arg_validation(
        MacroArgValidationKind::TypeMismatch,
        Some(arg_index),
        arg.value_span,
        "invalid_macro_arg_type",
        format!(
            "Argument `{}` for macro `{}` expects {}",
            input.name, entry.name, input.r#type
        ),
    ));
}

fn value_matches_input_type(value: &str, input: &MacroInputHint) -> bool {
    if macro_arg_value_unresolvable(value) {
        return true;
    }
    match input.r#type.as_str() {
        "word" | "agent" => {
            !value.is_empty() && !value.chars().any(char::is_whitespace)
        }
        "path" => !value.contains('\n') && !value.contains('\r'),
        "line" => !value.contains('\n'),
        "text" => true,
        "int" | "integer" => value.parse::<i64>().is_ok(),
        "float" => value.parse::<f64>().is_ok(),
        "bool" | "boolean" => matches!(
            value.to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
        ),
        "enum" => {
            input.choices.is_empty()
                || input.choices.iter().any(|choice| choice.value == value)
        }
        _ => true,
    }
}

fn resolved_hint(input: &MacroInputHint) -> ResolvedInputType {
    ResolvedInputType {
        base: input.r#type.clone(),
        named_type: input.named_type.clone(),
        value_role: input.value_role.clone(),
        choices: input
            .choices
            .iter()
            .map(|choice| InputChoice {
                value: choice.value.clone(),
                label: choice.label.clone(),
                description: choice.description.clone(),
            })
            .collect(),
        deprecated: false,
    }
}

fn choice_suggestion_data(
    document: &DocumentSnapshot,
    span: (usize, usize),
    suggestion_values: &[String],
) -> Option<EditorDiagnosticData> {
    if suggestion_values.is_empty() {
        return None;
    }
    let range = document.byte_range_to_range(span.0, span.1)?;
    Some(EditorDiagnosticData {
        suggestions: suggestion_values
            .iter()
            .enumerate()
            .map(|(index, value)| EditorDiagnosticSuggestion {
                value: value.clone(),
                title: format!("Replace with `{value}`"),
                edit: EditorTextEdit {
                    range,
                    new_text: quote_macro_arg_value(value),
                },
                preferred: index == 0,
            })
            .collect(),
    })
}

pub(crate) fn macro_arg_value_unresolvable(value: &str) -> bool {
    value.contains("{{")
        || value.contains("{%")
        || value.contains("{#")
        || value.contains("$(")
        || !scan_artifact_refs(value).is_empty()
        || extract_placeholder_spans(&DocumentSnapshot::new(value))
            .iter()
            .any(|span| span.raw)
}

fn input_for_position(
    entry: &MacroAssistEntry,
    position: usize,
) -> Option<&MacroInputHint> {
    entry
        .inputs
        .get(position)
        .or_else(|| entry.inputs.last().filter(|input| input.repeatable))
}

fn local_macro_entries(document: &DocumentSnapshot) -> Vec<MacroAssistEntry> {
    let Some(frontmatter) = frontmatter_mapping(document.text()) else {
        return Vec::new();
    };
    // Discover helpers from both the canonical `macros:` section and the
    // retired `xprompts:` spelling. The canonical entry wins on a name
    // conflict; a malformed canonical entry falls back to the retired one.
    let mut seen = HashSet::new();
    let mut entries = Vec::new();
    for key in ["macros", "xprompts"]
    // legacy xprompt spelling
    {
        let Some(section) =
            mapping_get(&frontmatter, key).and_then(Value::as_mapping)
        else {
            continue;
        };
        for (name, value) in section {
            let Some(name) = value_as_string(name) else {
                continue;
            };
            if seen.contains(&name) {
                continue;
            }
            let Some(entry) = local_macro_entry_from_config(&name, value)
            else {
                continue;
            };
            seen.insert(name);
            entries.push(entry);
        }
    }
    entries
}

fn local_macro_entry_from_config(
    name: &str,
    value: &Value,
) -> Option<MacroAssistEntry> {
    if !is_referenceable_macro_name(name) {
        return None;
    }
    let inputs = if value.as_str().is_some() {
        Vec::new()
    } else {
        let mapping = value.as_mapping()?;
        mapping_get(mapping, "content").and_then(value_as_string)?;
        mapping_get(mapping, "input")
            .map(parse_local_inputs)
            .unwrap_or_default()
    };
    let insertion = format!("#{name}");
    Some(MacroAssistEntry {
        name: name.to_string(),
        display_label: name.to_string(),
        insertion,
        reference_prefix: "#".to_string(),
        kind: Some("local_macro".to_string()),
        source_bucket: "current_document".to_string(),
        project: None,
        tags: Vec::new(),
        input_signature: None,
        inputs,
        content_preview: None,
        description: None,
        source_path_display: None,
        definition_path: None,
        definition_range: None,
        is_skill: false,
        skill_name: None,
        memory_type: None,
    })
}

pub(crate) fn parse_local_inputs(value: &Value) -> Vec<MacroInputHint> {
    if let Some(mapping) = value.as_mapping() {
        return mapping
            .iter()
            .enumerate()
            .filter_map(|(position, (name, raw))| {
                let name = value_as_string(name)?;
                let parsed = parse_short_input_hint(raw);
                let declared = input_choices(raw);
                let choices = if declared.is_empty() {
                    parsed.resolved_choices
                } else {
                    declared
                };
                Some(MacroInputHint {
                    name,
                    r#type: parsed.type_name,
                    description: input_description(raw),
                    required: parsed.required,
                    default_display: parsed.default_display,
                    position: position as u32,
                    repeatable: parsed.repeatable,
                    choices,
                    named_type: parsed.named_type,
                    value_role: parsed.value_role,
                })
            })
            .collect();
    }
    if let Some(sequence) = value.as_sequence() {
        return sequence
            .iter()
            .enumerate()
            .filter_map(|(position, item)| {
                let mapping = item.as_mapping()?;
                let name =
                    mapping_get(mapping, "name").and_then(value_as_string)?;
                let (type_name, named_type, value_role, resolved_choices) =
                    mapping_get(mapping, "type")
                        .and_then(value_as_string)
                        .map(|raw| resolve_local_rich(&raw))
                        .unwrap_or_else(|| {
                            ("line".to_string(), None, None, Vec::new())
                        });
                let default = mapping_get(mapping, "default");
                let declared = mapping_get(mapping, "choices")
                    .map(parse_local_input_choices)
                    .unwrap_or_default();
                let choices = if declared.is_empty() {
                    resolved_choices
                } else {
                    declared
                };
                Some(MacroInputHint {
                    name,
                    r#type: type_name,
                    description: mapping_get(mapping, "description")
                        .and_then(value_as_string),
                    required: default.is_none(),
                    default_display: default.and_then(default_display),
                    position: position as u32,
                    repeatable: mapping_get(mapping, "repeatable")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    choices,
                    named_type,
                    value_role,
                })
            })
            .collect();
    }
    Vec::new()
}

struct ParsedShortInputHint {
    type_name: String,
    required: bool,
    default_display: Option<String>,
    repeatable: bool,
    named_type: Option<String>,
    value_role: Option<String>,
    resolved_choices: Vec<MobileInputChoiceWire>,
}

fn parse_short_input_hint(value: &Value) -> ParsedShortInputHint {
    if let Some(mapping) = value.as_mapping() {
        let (type_name, named_type, value_role, resolved_choices) =
            mapping_get(mapping, "type")
                .and_then(value_as_string)
                .map(|raw| resolve_local_rich(&raw))
                .unwrap_or_else(|| {
                    ("line".to_string(), None, None, Vec::new())
                });
        let default = mapping_get(mapping, "default");
        ParsedShortInputHint {
            type_name,
            required: default.is_none(),
            default_display: default.and_then(default_display),
            repeatable: mapping_get(mapping, "repeatable")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            named_type,
            value_role,
            resolved_choices,
        }
    } else {
        let (type_name, named_type, value_role, resolved_choices) =
            resolve_local_rich(
                &value_as_string(value).unwrap_or_else(|| "line".to_string()),
            );
        ParsedShortInputHint {
            type_name,
            required: true,
            default_display: None,
            repeatable: false,
            named_type,
            value_role,
            resolved_choices,
        }
    }
}

fn input_description(value: &Value) -> Option<String> {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "description"))
        .and_then(value_as_string)
}

fn input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    value
        .as_mapping()
        .and_then(|mapping| mapping_get(mapping, "choices"))
        .map(parse_local_input_choices)
        .unwrap_or_default()
}

fn parse_local_input_choices(value: &Value) -> Vec<MobileInputChoiceWire> {
    let Some(items) = value.as_sequence() else {
        return Vec::new();
    };
    validate_enum_choices_yaml(items)
        .choices
        .into_iter()
        .map(|choice| MobileInputChoiceWire {
            value: choice.value,
            label: choice.label,
            description: choice.description,
        })
        .collect()
}

fn registry_from_env() -> InputTypeRegistry {
    let raw = std::env::var(
        crate::macro_catalog::SASE_MACRO_PLUGIN_INPUT_TYPES_JSON_ENV,
    )
    .unwrap_or_default();
    if raw.trim().is_empty() {
        return InputTypeRegistry::builtin();
    }
    let files: Vec<crate::macro_input_types::PluginInputTypeFileRecord> =
        serde_json::from_str(&raw).unwrap_or_default();
    let (registry, _diagnostics) =
        crate::macro_input_types::load_plugin_input_type_registry(&files);
    registry
}

fn resolve_local_rich(
    raw: &str,
) -> (
    String,
    Option<String>,
    Option<String>,
    Vec<MobileInputChoiceWire>,
) {
    match resolve_input_type("input", raw, &registry_from_env()) {
        Ok(resolved) => (
            resolved.base,
            resolved.named_type,
            resolved.value_role,
            resolved
                .choices
                .into_iter()
                .map(|choice| MobileInputChoiceWire {
                    value: choice.value,
                    label: choice.label,
                    description: choice.description,
                })
                .collect(),
        ),
        Err(_) => ("line".to_string(), None, None, Vec::new()),
    }
}

pub(crate) fn default_display(value: &Value) -> Option<String> {
    if value.is_null() || value.as_str().is_some() {
        return None;
    }
    if let Some(value) = value.as_bool() {
        return Some(if value { "true" } else { "false" }.to_string());
    }
    if let Some(value) = value.as_i64() {
        return Some(value.to_string());
    }
    value.as_f64().map(|value| value.to_string())
}

pub(crate) fn frontmatter_mapping(text: &str) -> Option<Mapping> {
    let opening_line_end = text.find('\n')?;
    if text[..opening_line_end].trim_end_matches('\r') != "---" {
        return None;
    }

    let frontmatter_start = opening_line_end + 1;
    let mut line_start = frontmatter_start;
    while line_start <= text.len() {
        let line_end = text[line_start..]
            .find('\n')
            .map(|idx| line_start + idx)
            .unwrap_or(text.len());
        if text[line_start..line_end].trim_end_matches('\r') == "---" {
            return serde_yaml::from_str::<Value>(
                &text[frontmatter_start..line_start],
            )
            .ok()
            .and_then(|value| value.as_mapping().cloned());
        }
        if line_end == text.len() {
            break;
        }
        line_start = line_end + 1;
    }
    None
}

pub(crate) fn mapping_get<'a>(
    mapping: &'a Mapping,
    key: &str,
) -> Option<&'a Value> {
    mapping.get(Value::String(key.to_string()))
}

pub(crate) fn value_as_string(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        Some(value.to_string())
    } else if let Some(value) = value.as_i64() {
        Some(value.to_string())
    } else if let Some(value) = value.as_bool() {
        Some(value.to_string())
    } else {
        value.as_f64().map(|value| value.to_string())
    }
}

/// Document-local macro names follow the ordinary reference grammar and may
/// never claim the reserved macro-memory namespace.
fn is_referenceable_macro_name(name: &str) -> bool {
    !is_reserved_memory_reference(name)
        && name.split('/').all(is_jinja_identifier)
}

fn is_jinja_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn push_diagnostic(
    document: &DocumentSnapshot,
    out: &mut Vec<EditorDiagnostic>,
    start: usize,
    end: usize,
    code: &str,
    message: String,
) {
    push_diagnostic_with_data(document, out, start, end, code, message, None);
}

fn push_diagnostic_with_data(
    document: &DocumentSnapshot,
    out: &mut Vec<EditorDiagnostic>,
    start: usize,
    end: usize,
    code: &str,
    message: String,
    data: Option<EditorDiagnosticData>,
) {
    push_diagnostic_with_severity_and_data(
        document,
        out,
        start,
        end,
        DiagnosticSeverity::Error,
        code,
        message,
        data,
    );
}

#[allow(clippy::too_many_arguments)]
fn push_diagnostic_with_severity_and_data(
    document: &DocumentSnapshot,
    out: &mut Vec<EditorDiagnostic>,
    start: usize,
    end: usize,
    severity: DiagnosticSeverity,
    code: &str,
    message: String,
    data: Option<EditorDiagnosticData>,
) {
    let Some(range) = document.byte_range_to_range(start, end) else {
        return;
    };
    let mut diagnostic = EditorDiagnostic::new(range, severity, code, message);
    if let Some(data) = data {
        diagnostic = diagnostic.with_data(data);
    }
    out.push(diagnostic);
}

fn strip_arg_quotes(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        if (bytes[0] == b'"' && bytes[trimmed.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[trimmed.len() - 1] == b'\'')
        {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn model_suggestion_data(
    document: &DocumentSnapshot,
    span: (usize, usize),
    suggestion_values: &[String],
) -> Option<EditorDiagnosticData> {
    if suggestion_values.is_empty() {
        return None;
    }
    let range = document.byte_range_to_range(span.0, span.1)?;
    Some(EditorDiagnosticData {
        suggestions: suggestion_values
            .iter()
            .enumerate()
            .map(|(index, value)| EditorDiagnosticSuggestion {
                value: value.clone(),
                title: format!("Replace with `{value}`"),
                edit: EditorTextEdit {
                    range,
                    new_text: value.clone(),
                },
                preferred: index == 0,
            })
            .collect(),
    })
}

fn macro_ref_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"']|[^\x00-\x7F])(?P<marker>#!|#)(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:(?:/|__)[A-Za-z_][A-Za-z0-9_]*)*)(?:!!|\?\?)?"#,
        )
        .unwrap()
    })
}

fn slash_skill_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"(?m)(?:^|\s)/(?P<skill>[A-Za-z0-9_]+)(?:\s|$)").unwrap()
    })
}

fn directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"(?m)(?:^|[\s\(\[\{"'])(?:%(?P<name>[A-Za-z_][A-Za-z0-9_]*))"#,
        )
        .unwrap()
    })
}

fn typed_launch_directive_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"(?m)(?:^|[\s\(\[\{"'])(?:%(?P<name>if|proc)\b)"#).unwrap()
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::{ArtifactRefBeadStoreWire, ArtifactRefDocumentRootWire};

    fn catalog() -> Vec<MacroAssistEntry> {
        vec![
            MacroAssistEntry {
                name: "review".to_string(),
                display_label: "review".to_string(),
                insertion: "#review".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "run".to_string(),
                display_label: "run".to_string(),
                insertion: "#!run".to_string(),
                reference_prefix: "#!".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "skill/plan".to_string(),
                display_label: "skill/plan".to_string(),
                insertion: "#skill/plan".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: true,
                skill_name: Some("plan".to_string()),
                memory_type: None,
            },
            MacroAssistEntry {
                name: "typed".to_string(),
                display_label: "typed".to_string(),
                insertion: "#typed".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![
                    input("path", "path", true, 0),
                    input("count", "int", true, 1),
                    input("enabled", "bool", false, 2),
                ],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "pr".to_string(),
                display_label: "pr".to_string(),
                insertion: "#pr".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![input("bug_id", "int", true, 0)],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "ns/foo".to_string(),
                display_label: "ns/foo".to_string(),
                insertion: "#ns/foo".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![input("arg", "word", true, 0)],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "merge".to_string(),
                display_label: "merge".to_string(),
                insertion: "#merge".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![repeatable_input("names", "agent", false, 0)],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "choose".to_string(),
                display_label: "choose".to_string(),
                insertion: "#choose".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![enum_input(
                    "edition",
                    true,
                    0,
                    false,
                    &[
                        ("brief", Some("Brief"), Some("Short edition")),
                        ("full", Some("Full"), Some("Complete edition")),
                    ],
                )],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
            MacroAssistEntry {
                name: "editions".to_string(),
                display_label: "editions".to_string(),
                insertion: "#editions".to_string(),
                reference_prefix: "#".to_string(),
                kind: None,
                source_bucket: "builtin".to_string(),
                project: None,
                tags: Vec::new(),
                input_signature: None,
                inputs: vec![enum_input(
                    "edition",
                    false,
                    0,
                    true,
                    &[("brief", None, None), ("full", None, None)],
                )],
                content_preview: None,
                description: None,
                source_path_display: None,
                definition_path: None,
                definition_range: None,
                is_skill: false,
                skill_name: None,
                memory_type: None,
            },
        ]
    }

    fn input(
        name: &str,
        r#type: &str,
        required: bool,
        position: u32,
    ) -> MacroInputHint {
        MacroInputHint {
            name: name.to_string(),
            r#type: r#type.to_string(),
            description: None,
            required,
            default_display: None,
            position,
            repeatable: false,
            choices: Vec::new(),
            named_type: None,
            value_role: None,
        }
    }

    fn repeatable_input(
        name: &str,
        r#type: &str,
        required: bool,
        position: u32,
    ) -> MacroInputHint {
        MacroInputHint {
            repeatable: true,
            ..input(name, r#type, required, position)
        }
    }

    fn enum_input(
        name: &str,
        required: bool,
        position: u32,
        repeatable: bool,
        choices: &[(&str, Option<&str>, Option<&str>)],
    ) -> MacroInputHint {
        let mut hint = input(name, "enum", required, position);
        hint.repeatable = repeatable;
        hint.choices = choices
            .iter()
            .map(|(value, label, description)| crate::MobileInputChoiceWire {
                value: (*value).to_string(),
                label: label.map(str::to_string),
                description: description.map(str::to_string),
            })
            .collect();
        hint
    }

    fn diagnostic_text(text: &str, diagnostic: &EditorDiagnostic) -> String {
        let doc = DocumentSnapshot::new(text);
        let start =
            doc.position_to_byte_offset(diagnostic.range.start).unwrap();
        let end = doc.position_to_byte_offset(diagnostic.range.end).unwrap();
        text[start..end].to_string()
    }

    fn diagnostics_for(text: &str) -> Vec<EditorDiagnostic> {
        analyze_document(&DocumentSnapshot::new(text), &catalog())
    }

    fn diagnostic<'a>(
        diagnostics: &'a [EditorDiagnostic],
        code: &str,
    ) -> &'a EditorDiagnostic {
        diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == code)
            .unwrap_or_else(|| panic!("missing {code}: {diagnostics:?}"))
    }

    fn diagnostic_count(diagnostics: &[EditorDiagnostic], code: &str) -> usize {
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.code == code)
            .count()
    }

    fn artifact_context(root: &std::path::Path) -> ArtifactRefContextWire {
        ArtifactRefContextWire {
            document_roots: vec![ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: root.join("designs").to_string_lossy().into_owned(),
                path_globs: None,
            }],
            chats_root: Some(root.join("chats").to_string_lossy().into_owned()),
            artifact_index_path: Some(
                root.join("artifact-index.jsonl")
                    .to_string_lossy()
                    .into_owned(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn artifact_diagnostics_report_malformed_and_unresolved_known_kinds() {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        let text = "@designs:guide.md#page=0 @designs:missing.md @user:handle";
        let document = DocumentSnapshot::new(text);

        let diagnostics = analyze_artifact_refs(&document, &context);

        assert_eq!(diagnostic_count(&diagnostics, "malformed_artifact_ref"), 1);
        assert_eq!(
            diagnostic_count(&diagnostics, "unresolved_artifact_ref"),
            1
        );
        assert_eq!(
            diagnostic_text(
                text,
                diagnostic(&diagnostics, "malformed_artifact_ref")
            ),
            "@designs:guide.md#page=0"
        );
        assert!(diagnostics
            .iter()
            .all(|diagnostic| !diagnostic.message.contains("@user:handle")));
    }

    #[test]
    fn artifact_diagnostics_resolve_document_chat_and_file_locally() {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        fs::create_dir_all(temp.path().join("designs/202607")).unwrap();
        fs::create_dir_all(temp.path().join("chats/202607")).unwrap();
        fs::write(temp.path().join("designs/202607/design.md"), "design")
            .unwrap();
        fs::write(temp.path().join("chats/202607/agent.md"), "chat").unwrap();
        fs::write(
            temp.path().join("artifact-index.jsonl"),
            "{\"schema_version\":1,\"artifact\":{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/tmp/image.png\"}}\n",
        )
        .unwrap();
        let document = DocumentSnapshot::new(
            "@designs:202607/design.md @chat:202607/agent.md @file:default:52895d68931185056fd0e49f",
        );

        let diagnostics = analyze_artifact_refs(&document, &context);

        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    #[test]
    fn artifact_diagnostics_skip_literal_ranges_and_unknown_kinds() {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        let document = DocumentSnapshot::new(
            "`@designs:missing.md` `@bead:sase-9z`\n```\n@designs:fenced.md\n```\n@user:handle",
        );

        let diagnostics = analyze_artifact_refs(&document, &context);

        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    #[test]
    fn artifact_diagnostics_report_unresolved_bead_pages() {
        let temp = tempfile::tempdir().unwrap();
        let mut context = artifact_context(temp.path());
        context.bead_stores.push(ArtifactRefBeadStoreWire {
            project: "sase".to_string(),
            prefix: "sase".to_string(),
            root: temp.path().join("beads").to_string_lossy().into_owned(),
        });
        let text = "@bead:sase-9z";
        let document = DocumentSnapshot::new(text);

        let diagnostics = analyze_artifact_refs(&document, &context);

        assert_eq!(
            diagnostic_count(&diagnostics, "unresolved_artifact_ref"),
            1
        );
        assert_eq!(
            diagnostic_text(
                text,
                diagnostic(&diagnostics, "unresolved_artifact_ref")
            ),
            "@bead:sase-9z"
        );
    }

    #[test]
    fn artifact_diagnostics_shape_check_commit_and_bug_without_resolution() {
        let temp = tempfile::tempdir().unwrap();
        let context = artifact_context(temp.path());
        let document = DocumentSnapshot::new(
            "@commit:missing@0123456 @bug:missing#42 @commit:missing@bad @bug:missing#0",
        );

        let diagnostics = analyze_artifact_refs(&document, &context);

        assert_eq!(diagnostic_count(&diagnostics, "malformed_artifact_ref"), 2);
        assert_eq!(
            diagnostic_count(&diagnostics, "unresolved_artifact_ref"),
            0
        );
    }

    #[test]
    fn slash_skills_resolve_by_provider_name_not_macro_reference() {
        // `/plan` is the installed skill; its macro reference is
        // `#skill/plan`, and the namespaced form is not a slash skill.
        let known = DocumentSnapshot::new("/plan #skill/plan");
        assert_eq!(
            diagnostic_count(
                &analyze_document(&known, &catalog()),
                "unknown_slash_skill"
            ),
            0
        );
        // The macro namespace segment is not itself a slash skill.
        let namespaced = DocumentSnapshot::new("/skill");
        assert_eq!(
            diagnostic_count(
                &analyze_document(&namespaced, &catalog()),
                "unknown_slash_skill"
            ),
            1
        );
        // `#plan` never falls back to the skill source.
        let unnamespaced = DocumentSnapshot::new("#plan");
        assert_eq!(
            diagnostic_count(
                &analyze_document(&unnamespaced, &catalog()),
                "unknown_macro"
            ),
            1
        );
    }

    #[test]
    fn reports_initial_diagnostics() {
        let doc = DocumentSnapshot::new("#missing #run /missing %wat");
        let diagnostics = analyze_document(&doc, &catalog());
        assert!(diagnostics.iter().any(|d| d.code == "unknown_macro"));
        assert!(diagnostics
            .iter()
            .any(|d| d.code == "canonical_marker_mismatch"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_slash_skill"));
        assert!(diagnostics.iter().any(|d| d.code == "unknown_directive"));
    }

    #[test]
    fn recognizes_current_directives_but_rejects_removed_spellings() {
        let current = DocumentSnapshot::new(
            "%id(worker, clan=research) %id(worker, family=review) %id(worker, tribe=review) %id(tribe=review) %i:worker %clan(research, tribe=research) %c:research",
        );
        assert_eq!(
            diagnostic_count(
                &analyze_document(&current, &catalog()),
                "unknown_directive"
            ),
            0
        );

        let removed = DocumentSnapshot::new(
            "%name:x %n:x %family:x %f:x %group:x %g:x %tribe:x %t:x %wat:x",
        );
        let diagnostics = analyze_document(&removed, &catalog());
        assert_eq!(diagnostic_count(&diagnostics, "unknown_directive"), 9);
        for name in [
            "name", "n", "family", "f", "group", "g", "tribe", "t", "wat",
        ] {
            assert!(
                diagnostics.iter().any(|diagnostic| diagnostic.message
                    == format!("Unknown directive `%{name}`")),
                "missing diagnostic for %{name}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn typed_launch_diagnostics_follow_flag_and_scanner_results() {
        let valid = DocumentSnapshot::new("%if::\n\n```bash\ntrue\n```");
        let disabled = typed_launch_directive_diagnostics(&valid, false);
        assert_eq!(
            diagnostic_count(&disabled, "typed_launch_units_disabled"),
            1
        );
        let static_if = DocumentSnapshot::new("%if(should_run=false)\nSkip");
        assert!(
            typed_launch_directive_diagnostics(&static_if, false).is_empty()
        );

        let missing = DocumentSnapshot::new("%if::\n\nReview");
        let enabled = typed_launch_directive_diagnostics(&missing, true);
        assert_eq!(diagnostic_count(&enabled, "typed_launch_missing_fence"), 1);

        let enabled_valid = typed_launch_directive_diagnostics(&valid, true);
        assert!(enabled_valid.is_empty(), "{enabled_valid:?}");
    }

    #[test]
    fn reports_unclosed_alternation_with_launch_wording() {
        let text = "foo%{bar";
        let diagnostics = diagnostics_for(text);
        assert_eq!(diagnostic_count(&diagnostics, "unclosed_alternation"), 1);
        let found = diagnostic(&diagnostics, "unclosed_alternation");
        assert_eq!(found.severity, DiagnosticSeverity::Error);
        assert_eq!(found.message, "unclosed %{ directive: missing closing '}'");
        assert_eq!(diagnostic_text(text, found), "%{");
    }

    #[test]
    fn reports_unclosed_paren_alternation_with_alt_wording() {
        let text = "x %alt(a, b";
        let diagnostics = diagnostics_for(text);
        assert_eq!(diagnostic_count(&diagnostics, "unclosed_alternation"), 1);
        assert_eq!(
            diagnostic(&diagnostics, "unclosed_alternation").message,
            "unclosed %alt directive: missing closing ')'"
        );
    }

    #[test]
    fn closed_alternations_and_literal_zones_have_no_diagnostic() {
        for text in [
            "foo%{bar | baz}qux",
            "x %(a, b)",
            "`%{a | b}`",
            "```text\n%{a | b}\n```",
            "```\nfoo%{bar\n```",
        ] {
            assert_eq!(
                diagnostic_count(
                    &diagnostics_for(text),
                    "unclosed_alternation"
                ),
                0,
                "{text:?}"
            );
        }
    }

    #[test]
    fn queue_directive_diagnostics_are_retired() {
        let document = DocumentSnapshot::new("%q:5 Review");
        let diagnostics = queue_directive_diagnostics(&document, false);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
        assert_eq!(
            diagnostic_count(
                &analyze_document(&document, &catalog()),
                "unknown_directive"
            ),
            0
        );
    }

    #[test]
    fn reports_macro_argument_contract_diagnostics() {
        for (text, code) in [
            ("#typed", "missing_required_arg"),
            ("#typed(src/main.rs)", "missing_required_arg"),
            ("#typed(src/main.rs, 3, true, extra)", "too_many_args"),
            (
                "#typed(path=src/main.rs, nope=1, count=3)",
                "unknown_macro_arg",
            ),
            ("#typed(path=a, path=b, count=3)", "duplicate_macro_arg"),
            (
                "#typed(path=\"bad\nvalue\", count=3)",
                "invalid_macro_arg_type",
            ),
            (
                "#typed(path=src/main.rs, count=nope)",
                "invalid_macro_arg_type",
            ),
            ("#typed:path(x)", "malformed_macro_argument"),
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                diagnostics.iter().any(|d| d.code == code),
                "{text}: {diagnostics:?}"
            );
        }

        let diagnostics =
            diagnostics_for("#typed(src/main.rs, path=other, count=3)");
        assert_eq!(diagnostic_count(&diagnostics, "conflicting_macro_arg"), 0);
    }

    fn model_snapshot() -> ModelValiditySnapshot {
        ModelValiditySnapshot {
            schema_version: 1,
            providers: ["claude", "codex", "fakey"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            models: [("opus", "claude"), ("fakey-large", "fakey")]
                .into_iter()
                .map(|(model, provider)| {
                    (model.to_string(), provider.to_string())
                })
                .collect(),
            aliases: vec!["large".to_string()],
            effort_levels: [
                "none", "minimal", "low", "medium", "high", "xhigh", "max",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
        }
    }

    fn model_catalog() -> Vec<MacroAssistEntry> {
        let mut hint = input("claude_model", "word", true, 0);
        hint.named_type = Some("model".to_string());
        hint.value_role = Some("model".to_string());
        vec![MacroAssistEntry {
            name: "launch".to_string(),
            display_label: "launch".to_string(),
            insertion: "#launch".to_string(),
            reference_prefix: "#".to_string(),
            kind: None,
            source_bucket: "builtin".to_string(),
            project: None,
            tags: Vec::new(),
            input_signature: None,
            inputs: vec![hint],
            content_preview: None,
            description: None,
            source_path_display: None,
            definition_path: None,
            definition_range: None,
            is_skill: false,
            skill_name: None,
            memory_type: None,
        }]
    }

    #[test]
    fn invalid_model_argument_warns_with_preferred_replace_fix() {
        let entries = model_catalog();
        let snapshot = model_snapshot();
        let text = "#launch(claude_model=opsu)";
        let diagnostics = analyze_document_with_snapshot(
            &DocumentSnapshot::new(text),
            &entries,
            Some(&snapshot),
        );
        let found = diagnostic(&diagnostics, "invalid_macro_arg_model");
        assert_eq!(found.severity, DiagnosticSeverity::Warning);
        assert!(
            found.message.contains("expects a model"),
            "unexpected message: {}",
            found.message
        );
        let data = found.data.as_ref().expect("model fix data");
        assert_eq!(data.suggestions.len(), 1);
        assert_eq!(data.suggestions[0].title, "Replace with `opus`");
        assert_eq!(data.suggestions[0].value, "opus");
        assert!(data.suggestions[0].preferred);
        assert_eq!(diagnostic_text(text, found), "opsu");

        for accepted in [
            "#launch(claude_model=claude/opus@xhigh)",
            "#launch(claude_model=@large)",
            "#launch(claude_model=fakey-large)",
        ] {
            let diagnostics = analyze_document_with_snapshot(
                &DocumentSnapshot::new(accepted),
                &entries,
                Some(&snapshot),
            );
            assert_eq!(
                diagnostic_count(&diagnostics, "invalid_macro_arg_model"),
                0,
                "{accepted}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn model_argument_is_unchecked_without_routing_snapshot() {
        let entries = model_catalog();
        let text = "#launch(claude_model=opsu)";
        let diagnostics = analyze_document_with_snapshot(
            &DocumentSnapshot::new(text),
            &entries,
            None,
        );
        assert_eq!(
            diagnostic_count(&diagnostics, "invalid_macro_arg_model"),
            0,
            "{diagnostics:?}"
        );
        let legacy = analyze_document(&DocumentSnapshot::new(text), &entries);
        assert_eq!(
            diagnostic_count(&legacy, "invalid_macro_arg_model"),
            0,
            "{legacy:?}"
        );
    }

    #[test]
    fn unresolvable_values_do_not_report_type_mismatches() {
        for text in [
            "#pr(bug_id={{ number }})",
            "#pr(bug_id=<bug id>)",
            "#pr(bug_id=@bead:sase-123)",
            "#pr(bug_id=$(cat bug-id))",
        ] {
            let diagnostics = diagnostics_for(text);
            assert_eq!(
                diagnostic_count(&diagnostics, "invalid_macro_arg_type"),
                0,
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn repeatable_tail_accepts_and_validates_every_element() {
        for text in ["#merge:planner,coder", "#merge(planner, coder)"] {
            let diagnostics = diagnostics_for(text);
            assert_eq!(diagnostic_count(&diagnostics, "too_many_args"), 0);
            assert_eq!(
                diagnostic_count(&diagnostics, "invalid_macro_arg_type"),
                0
            );
        }

        for text in ["#merge:planner,,coder", "#merge(planner,,coder)"] {
            let diagnostics = diagnostics_for(text);
            assert_eq!(
                diagnostic_count(&diagnostics, "invalid_macro_arg_type"),
                1,
                "{diagnostics:?}"
            );
        }
    }

    #[test]
    fn reports_shortform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  name: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn accepts_known_frontmatter_input_type_aliases() {
        let text = "---\ninput:\n  a: word\n  b: line\n  c: text\n  d: path\n  e: int\n  f: integer\n  g: bool\n  h: boolean\n  i: float\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());

        assert!(
            !diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_longform_frontmatter_input_type_diagnostic() {
        let text = "---\ninput:\n  - name: foo\n    type: wordd\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn reports_flow_style_frontmatter_input_type_diagnostic() {
        let text = "---\ninput: {name: wordd}\n---\nBody";
        let doc = DocumentSnapshot::new(text);
        let diagnostics = analyze_document(&doc, &catalog());
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            })
            .unwrap();

        assert_eq!(diagnostic_text(text, diagnostic), "wordd");
    }

    #[test]
    fn reports_frontmatter_yaml_and_shape_diagnostics() {
        let diagnostics = diagnostics_for("---\ninput: [\n---\nBody");
        let yaml_diagnostic =
            diagnostic(&diagnostics, "invalid_macro_frontmatter_yaml");
        assert_eq!(yaml_diagnostic.severity, DiagnosticSeverity::Error);

        let diagnostics = diagnostics_for("---\n[not, mapping]\n---\nBody");
        let shape_diagnostic =
            diagnostic(&diagnostics, "invalid_macro_frontmatter_shape");
        assert_eq!(shape_diagnostic.severity, DiagnosticSeverity::Error);

        let diagnostics = diagnostics_for("input:\n  name: wordd\nBody");
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| !diagnostic.code.contains("frontmatter")),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn reports_unknown_top_level_and_invalid_name() {
        let diagnostics =
            diagnostics_for("---\nname: bad-name\nowner: me\n---\nBody");
        assert_eq!(
            diagnostic(&diagnostics, "unknown_macro_frontmatter_field")
                .severity,
            DiagnosticSeverity::Information
        );
        assert_eq!(
            diagnostic(&diagnostics, "unreferenceable_macro_frontmatter_name")
                .severity,
            DiagnosticSeverity::Warning
        );

        let diagnostics = diagnostics_for("---\nname: []\n---\nBody");
        assert_eq!(
            diagnostic(&diagnostics, "invalid_macro_frontmatter_name").severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn accepts_frontmatter_local_macros() {
        let text = "---\ndescription: Example\ninput:\n  topic: text\nmacros:\n  _helper:\n    content: Helper {{ topic }}\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| diagnostic.code
                != "unknown_macro_frontmatter_field"),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn accepts_current_document_local_macros_and_validates_args() {
        let text = "---\nmacros:\n  _helper:\n    input:\n      topic: word\n    content: Helper {{ topic }}\n---\n#_helper(docs)\n#_missing\n";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| {
                diagnostic.code != "unknown_macro"
                    || !diagnostic.message.contains("_helper")
            }),
            "{diagnostics:?}"
        );
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "unknown_macro"
                    && diagnostic.message.contains("_missing")
            }),
            "{diagnostics:?}"
        );
        assert_eq!(
            diagnostic_count(&diagnostics, "missing_required_arg"),
            0,
            "{diagnostics:?}"
        );

        let diagnostics =
            diagnostics_for("---\nmacros:\n  _helper:\n    input:\n      topic: word\n    content: Helper {{ topic }}\n---\n#_helper\n");
        let diagnostic = diagnostic(&diagnostics, "missing_required_arg");
        assert!(diagnostic.message.contains("topic"));
        assert!(diagnostic.message.contains("_helper"));
    }

    #[test]
    fn local_enum_choices_keep_descriptions_and_suggest_near_misses() {
        let inputs: Value = serde_yaml::from_str(
            "edition:\n  type: enum\n  choices:\n    - value: brief\n      label: Brief\n      description: Short edition\n    - value: full\n      description: Complete edition\n",
        )
        .unwrap();
        let parsed = parse_local_inputs(&inputs);
        assert_eq!(parsed[0].r#type, "enum");
        assert_eq!(
            parsed[0].choices[0].description.as_deref(),
            Some("Short edition")
        );

        let unresolved: Value =
            serde_yaml::from_str("mode:\n  type: enmu\n").unwrap();
        assert_eq!(parse_local_inputs(&unresolved)[0].r#type, "line");
        let deprecated: Value =
            serde_yaml::from_str("mode:\n  type: string\n").unwrap();
        assert_eq!(parse_local_inputs(&deprecated)[0].r#type, "line");

        let text = "---\nmacros:\n  choose:\n    input:\n      edition:\n        type: enum\n        choices: [brief, full]\n    content: Choose an edition\n---\n#choose(edition=breif)";
        let diagnostics = diagnostics_for(text);
        let mismatch = diagnostic(&diagnostics, INVALID_MACRO_ARG_CHOICE);
        assert_eq!(
            mismatch.message,
            "Argument `edition` expects one of brief | full, got `breif`; did you mean `brief`?"
        );
        let data = mismatch.data.as_ref().expect("choice diagnostic data");
        assert_eq!(data.suggestions[0].value, "brief");
        assert_eq!(data.suggestions[0].title, "Replace with `brief`");
        assert!(data.suggestions[0].preferred);
        assert_eq!(data.suggestions[0].edit.new_text, "brief");
        assert_eq!(diagnostic_text(text, mismatch), "breif");
    }

    #[test]
    fn closed_set_diagnostics_cover_forms_labels_and_repeatable_elements() {
        for text in
            ["#choose(edition=breif)", "#choose(breif)", "#choose:breif"]
        {
            let diagnostics = diagnostics_for(text);
            let mismatch = diagnostic(&diagnostics, INVALID_MACRO_ARG_CHOICE);
            assert_eq!(diagnostic_text(text, mismatch), "breif");
            assert_eq!(
                mismatch.data.as_ref().unwrap().suggestions[0].value,
                "brief"
            );
        }

        let label = diagnostics_for("#choose(edition=Brief)");
        let mismatch = diagnostic(&label, INVALID_MACRO_ARG_CHOICE);
        assert!(mismatch.message.contains("got `Brief`"));

        let valid = diagnostics_for("#choose(edition=brief)");
        assert_eq!(diagnostic_count(&valid, INVALID_MACRO_ARG_CHOICE), 0);

        let defaulted = diagnostics_for("#choose(edition=null)");
        assert_eq!(diagnostic_count(&defaulted, INVALID_MACRO_ARG_CHOICE), 0);

        let repeatable = diagnostics_for("#editions(brief, ful)");
        assert_eq!(diagnostic_count(&repeatable, INVALID_MACRO_ARG_CHOICE), 1);
        assert_eq!(
            diagnostic_text(
                "#editions(brief, ful)",
                diagnostic(&repeatable, INVALID_MACRO_ARG_CHOICE)
            ),
            "ful"
        );

        let text = "😀#choose(edition=breif)";
        let diagnostics = diagnostics_for(text);
        let mismatch = diagnostic(&diagnostics, INVALID_MACRO_ARG_CHOICE);
        assert_eq!(diagnostic_text(text, mismatch), "breif");
        assert_eq!(
            mismatch.range.start.character,
            3 + "choose(edition=".len() as u32
        );
    }

    #[test]
    fn old_diagnostic_payloads_deserialize_without_data() {
        let json = r#"{
            "range": {"start": {"line": 0, "character": 0}, "end": {"line": 0, "character": 1}},
            "severity": "error",
            "code": "unknown_macro",
            "message": "Unknown macro `missing`"
        }"#;
        let diagnostic: EditorDiagnostic = serde_json::from_str(json).unwrap();
        assert!(diagnostic.data.is_none());
    }

    #[test]
    fn path_values_allow_spaces_but_reject_line_breaks() {
        let path = input("path", "path", true, 0);
        assert!(value_matches_input_type("src/my file.rs", &path));
        assert!(!value_matches_input_type("src/my\nfile.rs", &path));
    }

    #[test]
    fn reports_input_shape_name_duplicate_identifier_and_unknown_fields() {
        for (text, code) in [
            (
                "---\ninput: nope\n---\nBody",
                "invalid_macro_frontmatter_input_shape",
            ),
            (
                "---\ninput:\n  - type: word\n---\nBody",
                "invalid_macro_frontmatter_input_name",
            ),
            (
                "---\ninput:\n  - name: target\n  - name: target\n---\nBody",
                "duplicate_macro_frontmatter_input",
            ),
            (
                "---\ninput:\n  bad-name: word\n---\nBody",
                "invalid_macro_frontmatter_input_identifier",
            ),
            (
                "---\ninput:\n  target:\n    type: word\n    extra: ignored\n---\nBody",
                "unknown_macro_frontmatter_input_field",
            ),
        ] {
            let diagnostics = diagnostics_for(text);
            assert!(
                diagnostics.iter().any(|diagnostic| diagnostic.code == code),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn reports_invalid_input_defaults() {
        let text = "---\ninput:\n  wordy:\n    type: word\n    default: \"two words\"\n  count:\n    type: int\n    default: nope\n  ratio:\n    type: float\n    default: nope\n  enabled:\n    type: bool\n    default: maybe\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert_eq!(
            diagnostic_count(
                &diagnostics,
                "invalid_macro_frontmatter_input_default"
            ),
            4,
            "{diagnostics:?}"
        );
    }

    #[test]
    fn accepts_valid_input_aliases_and_defaults() {
        let text = "---\ninput:\n  a:\n    type: word\n    default: docs\n  b:\n    type: path\n    default: src/main.rs\n  c:\n    type: line\n    default: hello world\n  d:\n    type: text\n    default: |\n      hello\n      world\n  e:\n    type: integer\n    default: 3\n  f:\n    type: boolean\n    default: true\n  g:\n    type: float\n    default: 3.5\n  h:\n    type: int\n    default:\n---\nBody";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| {
                diagnostic.code != "invalid_macro_frontmatter_input_type"
                    && diagnostic.code
                        != "invalid_macro_frontmatter_input_default"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn accepts_input_descriptions_and_reports_invalid_shapes() {
        for valid in [
            "---\ninput:\n  short:\n    type: word\n    description: Short input description\n---\nBody",
            "---\ninput:\n  - name: long\n    type: text\n    description: Long input description\n---\nBody",
            "---\ninput: {short: {type: word, description: Short input description}}\n---\nBody",
            "---\ninput: [{name: long, type: text, description: Long input description}]\n---\nBody",
        ] {
            let diagnostics = diagnostics_for(valid);
            assert!(
                diagnostics.iter().all(|diagnostic| {
                    diagnostic.code != "unknown_macro_frontmatter_input_field"
                        && diagnostic.code
                            != "invalid_macro_frontmatter_input_description"
                }),
                "{diagnostics:?}"
            );
        }

        let invalid =
            "---\ninput:\n  short:\n    type: word\n    description: {}\n---\nBody";
        let diagnostics = diagnostics_for(invalid);
        assert_eq!(
            diagnostic(
                &diagnostics,
                "invalid_macro_frontmatter_input_description"
            )
            .severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn reports_invalid_snippet_tags_keywords_and_skill_metadata() {
        let diagnostics = diagnostics_for(
            "---\nsnippet: bad-trigger!\ntags: [mentor, {}]\nkeywords: [topic, {}]\nskill: true\n---\nBody",
        );

        assert_eq!(
            diagnostic(
                &diagnostics,
                "invalid_macro_frontmatter_snippet_trigger"
            )
            .severity,
            DiagnosticSeverity::Error
        );
        assert_eq!(
            diagnostic(&diagnostics, "invalid_macro_frontmatter_tags").severity,
            DiagnosticSeverity::Error
        );
        assert_eq!(
            diagnostic(&diagnostics, "invalid_macro_frontmatter_keywords")
                .severity,
            DiagnosticSeverity::Error
        );
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code
                    != "missing_macro_memory_tag"),
            "{diagnostics:?}"
        );
        assert_eq!(
            diagnostic(&diagnostics, "missing_macro_skill_description")
                .severity,
            DiagnosticSeverity::Warning
        );
    }

    #[test]
    fn keywords_do_not_restore_dynamic_memory_matching() {
        let text = "---\nkeywords: [topic]\n---\nBody";
        let diagnostics = diagnostics_for(text);
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code
                    != "missing_macro_memory_tag"),
            "{diagnostics:?}"
        );

        let doc = DocumentSnapshot::with_source_path(
            text,
            "/repo/sase/memory/generated_skills.md",
        );
        let diagnostics = analyze_document(&doc, &catalog());
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code
                    != "missing_macro_memory_tag"),
            "{diagnostics:?}"
        );

        let tagged_doc = DocumentSnapshot::new(
            "---\ntags: [memory]\nkeywords: [topic]\n---\nBody",
        );
        let diagnostics = analyze_document(&tagged_doc, &catalog());
        assert!(
            diagnostics
                .iter()
                .all(|diagnostic| diagnostic.code
                    != "missing_macro_memory_tag"),
            "{diagnostics:?}"
        );

        let invalid_doc = DocumentSnapshot::with_source_path(
            "---\nkeywords: [{}]\n---\nBody",
            "/repo/sase/memory/generated_skills.md",
        );
        let diagnostics = analyze_document(&invalid_doc, &catalog());
        assert_eq!(
            diagnostic(&diagnostics, "invalid_macro_frontmatter_keywords")
                .severity,
            DiagnosticSeverity::Error
        );
    }

    #[test]
    fn reports_flow_style_input_default_on_offending_scalar() {
        let text = "---\ninput: [{name: target, type: word, default: \"two words\"}]\n---\nBody";
        let diagnostics = diagnostics_for(text);
        let diagnostic =
            diagnostic(&diagnostics, "invalid_macro_frontmatter_input_default");

        assert_eq!(diagnostic_text(text, diagnostic), "two words");
    }

    #[test]
    fn accepts_valid_argument_forms_and_bool_spellings() {
        for text in [
            "#typed(path=src/main.rs, count=3, enabled=true)",
            "#typed(path=\"src/my file.rs\", count=3)",
            "#typed(src/main.rs, 3, yes)",
            "#typed:src/main.rs,3,on",
            "#typed(path=null, count=null)",
            "#ns/foo(arg=hello)",
            "#ns__foo!!(arg=hello)",
        ] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics
                    .iter()
                    .any(|d| d.severity == DiagnosticSeverity::Error),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn incomplete_forms_do_not_emit_required_arg_noise() {
        for text in ["#typed(", "#typed(path=", "#typed:"] {
            let doc = DocumentSnapshot::new(text);
            let diagnostics = analyze_document(&doc, &catalog());
            assert!(
                !diagnostics.iter().any(|d| d.code == "missing_required_arg"),
                "{text}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn discovers_macros_section_helpers_and_validates_args() {
        let text = "---\nmacros:\n  _helper:\n    input:\n      topic: word\n    content: Helper {{ topic }}\n---\n#_helper(docs)\n#_missing\n";
        let diagnostics = diagnostics_for(text);

        assert!(
            diagnostics.iter().all(|diagnostic| {
                diagnostic.code != "unknown_macro"
                    || !diagnostic.message.contains("_helper")
            }),
            "{diagnostics:?}"
        );
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "unknown_macro"
                    && diagnostic.message.contains("_missing")
            }),
            "{diagnostics:?}"
        );

        let diagnostics =
            diagnostics_for("---\nmacros:\n  _helper:\n    input:\n      topic: word\n    content: Helper {{ topic }}\n---\n#_helper\n");
        let diagnostic = diagnostic(&diagnostics, "missing_required_arg");
        assert!(diagnostic.message.contains("topic"));
        assert!(diagnostic.message.contains("_helper"));
    }

    #[test]
    fn canonical_local_section_wins_on_helper_name_conflict() {
        let text = "---\nmacros:\n  _helper:\n    input:\n      topic: word\n    content: Canonical {{ topic }}\nxprompts:\n  _helper:\n    content: Retired\n---\n#_helper\n"; // legacy xprompt spelling
        let diagnostics = diagnostics_for(text);
        // Canonical declares required `topic`, so the bare call is missing
        // an argument; the retired spelling would accept it.
        let diagnostic = diagnostic(&diagnostics, "missing_required_arg");
        assert!(diagnostic.message.contains("_helper"));
    }
}

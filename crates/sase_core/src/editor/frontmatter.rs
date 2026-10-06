use serde_yaml::{Mapping, Value};
use std::collections::{HashMap, HashSet};

use crate::macro_input_types::{
    check_closed_set_default, pyyaml_plain_scalar_is_non_string,
    resolve_input_type, suggest_closest, unquoted_plain_scalar_choice_error,
    validate_enum_choices_yaml, ChoiceIssueSeverity, InputChoice,
    InputTypeRegistry,
};
use crate::model_validity::ModelValiditySnapshot;

use super::token::DocumentSnapshot;
use super::wire::{
    DiagnosticSeverity, EditorDiagnostic, EditorDiagnosticData,
    EditorDiagnosticSuggestion, EditorPosition, EditorRange, EditorTextEdit,
    FrontmatterFieldKind, FrontmatterFieldSchema, FrontmatterInputType,
    HoverPayload,
};

/// Ordered panel field descriptors: `(name, kind, allowed_values, example)`.
///
/// This is the field set the prompt frontmatter panel offers (macro `.md`
/// parity). Descriptions are not duplicated here; they are sourced from
/// [`TOP_LEVEL_FIELD_DOCS`] via [`top_level_field_doc`] so the panel and the
/// hover/LSP guidance never drift. `keywords` is a valid macro field but is
/// intentionally outside the ad-hoc prompt panel's parity set.
const PANEL_FIELD_SCHEMA: &[(
    &str,
    FrontmatterFieldKind,
    Option<&str>,
    &str,
)] = &[
    ("name", FrontmatterFieldKind::Scalar, None, "my_prompt"),
    (
        "description",
        FrontmatterFieldKind::Scalar,
        None,
        "Refactor the auth module across services",
    ),
    (
        "tags",
        FrontmatterFieldKind::List,
        None,
        "refactor, backend",
    ),
    (
        "input",
        FrontmatterFieldKind::Structured,
        None,
        "service: word",
    ),
    (
        "macros",
        FrontmatterFieldKind::Structured,
        None,
        "_rules: \"Follow the team review checklist\"",
    ),
    (
        "skill",
        FrontmatterFieldKind::BoolOrList,
        Some("true, false, or a provider list"),
        "false",
    ),
    (
        "snippet",
        FrontmatterFieldKind::BoolOrScalar,
        Some("true, false, or a trigger string"),
        "false",
    ),
];

const TOP_LEVEL_FIELD_DOCS: &[(&str, &str)] = &[
    (
        "name",
        "Overrides the macro reference name used in catalogs and completions.",
    ),
    (
        "input",
        "Declares named inputs accepted by this macro. Supports shortform mappings and longform sequences.",
    ),
    (
        "tags",
        "Adds tags for catalog filtering.",
    ),
    (
        "description",
        "Provides the single-line summary shown in completions, hovers, and picker previews.",
    ),
    (
        "skill",
        "Marks this macro as a slash skill. Use true, false, or a provider list.",
    ),
    (
        "snippet",
        "Exposes this macro as a completion snippet. Use true, false, or a custom trigger.",
    ),
    (
        "log_skill_use",
        "Controls whether generated skill files include the `sase skill use ...` audit directive. Use true or false; defaults to true and only applies to skill macros.",
    ),
    (
        "keywords",
        "Legacy macro metadata retained for compatibility; it does not trigger memory matching.",
    ),
    (
        "macros",
        "Defines local macros available only within the current file. Reference them from the body with `#name`.",
    ),
    (
        "xprompts", // legacy xprompt spelling
        "Defines local macros available only within the current file. Reference them from the body with `#name`.",
    ),
];

#[derive(Debug, Clone, Copy)]
struct FrontmatterBlock<'a> {
    text: &'a str,
    start: usize,
}

#[derive(Debug, Clone, Copy)]
struct FrontmatterLine<'a> {
    start: usize,
    text: &'a str,
}

#[derive(Debug, Clone)]
struct KeyValueSource {
    key: String,
    key_range: (usize, usize),
    value_range: (usize, usize),
    item_range: (usize, usize),
    scalar: Option<ScalarSource>,
}

#[derive(Debug, Clone)]
struct ScalarSource {
    range: (usize, usize),
}

#[derive(Debug, Clone)]
struct FrontmatterSourceIndex {
    text: String,
    fields: Vec<KeyValueSource>,
    input: Option<InputSourceIndex>,
    fallback_range: (usize, usize),
}

#[derive(Debug, Clone, Default)]
struct InputSourceIndex {
    shortform: Vec<ShortInputSource>,
    longform: Vec<LongInputSource>,
}

#[derive(Debug, Clone)]
struct ShortInputSource {
    name: String,
    name_range: (usize, usize),
    value_range: (usize, usize),
    item_range: (usize, usize),
    type_value: Option<ScalarSource>,
    default_value: Option<ScalarSource>,
    fields: Vec<KeyValueSource>,
}

#[derive(Debug, Clone)]
struct LongInputSource {
    item_range: (usize, usize),
    fields: Vec<KeyValueSource>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputType {
    Word,
    Agent,
    Line,
    Text,
    Path,
    Int,
    Bool,
    Float,
    Enum,
    Code,
}

pub(super) fn diagnostics_with_snapshot(
    document: &DocumentSnapshot,
    snapshot: Option<&ModelValiditySnapshot>,
) -> Vec<EditorDiagnostic> {
    diagnostics_with_registry_and_snapshot(
        document,
        snapshot,
        &registry_from_env(),
    )
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

pub fn diagnostics_with_registry(
    document: &DocumentSnapshot,
    registry: &InputTypeRegistry,
) -> Vec<EditorDiagnostic> {
    diagnostics_with_registry_and_snapshot(document, None, registry)
}

pub fn diagnostics_with_registry_and_snapshot(
    document: &DocumentSnapshot,
    snapshot: Option<&ModelValiditySnapshot>,
    registry: &InputTypeRegistry,
) -> Vec<EditorDiagnostic> {
    let Some(frontmatter) = extract_frontmatter(document.text()) else {
        return Vec::new();
    };
    let index = FrontmatterSourceIndex::new(frontmatter.text);
    let mut builder = FrontmatterDiagnosticBuilder::new(
        document,
        frontmatter.start,
        index,
        registry.clone(),
    );

    let value = match serde_yaml::from_str::<Value>(frontmatter.text) {
        Ok(value) => value,
        Err(error) => {
            let range = yaml_error_range(frontmatter.text, &error)
                .unwrap_or(builder.index.fallback_range);
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_yaml",
                format!("Invalid macro frontmatter YAML: {error}"),
            );
            return builder.finish();
        }
    };

    validate_frontmatter_value_with_snapshot(&mut builder, &value, snapshot);
    builder.finish()
}

pub(super) fn hover(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<HoverPayload> {
    let frontmatter = extract_frontmatter(document.text())?;
    let cursor = document.position_to_byte_offset(position)?;
    if cursor < frontmatter.start {
        return None;
    }
    let cursor = cursor - frontmatter.start;
    if cursor > frontmatter.text.len() {
        return None;
    }

    let index = FrontmatterSourceIndex::new(frontmatter.text);
    let field = index.fields.iter().find(|field| {
        field.key_range.0 <= cursor && cursor < field.key_range.1
    })?;
    let description = top_level_field_doc(&field.key)?;
    let start = frontmatter.start + field.key_range.0;
    let end = frontmatter.start + field.key_range.1;
    Some(HoverPayload {
        range: document.byte_range_to_range(start, end)?,
        markdown: format!("**{}**\n\n{}", field.key, description),
    })
}

/// Ordered, panel-oriented descriptors for every supported frontmatter field.
///
/// Shared source of truth for the prompt frontmatter panel's "add property"
/// picker and inline guidance. Descriptions come from the same constant that
/// powers hover and the macro LSP, so the panel never drifts from the editor.
pub fn field_schema() -> Vec<FrontmatterFieldSchema> {
    PANEL_FIELD_SCHEMA
        .iter()
        .map(
            |(name, kind, allowed_values, example)| FrontmatterFieldSchema {
                name: (*name).to_string(),
                kind: *kind,
                required: false,
                description: top_level_field_doc(name)
                    .unwrap_or_default()
                    .to_string(),
                allowed_values: allowed_values.map(str::to_string),
                example: (*example).to_string(),
            },
        )
        .collect()
}

/// A projection of the shared macro input-type catalog for frontmatter editors.
pub fn input_type_schema() -> Vec<FrontmatterInputType> {
    input_type_schema_with_registry(&InputTypeRegistry::builtin())
}

/// Registry-aware projection of the shared input-type catalog.
pub fn input_type_schema_with_registry(
    registry: &InputTypeRegistry,
) -> Vec<FrontmatterInputType> {
    registry
        .entries()
        .iter()
        .cloned()
        .map(|entry| FrontmatterInputType {
            name: entry.name,
            aliases: entry.aliases,
            rule: entry.rule,
            kind: entry.kind,
            description: entry.description,
            source: entry.source,
            advertised: entry.advertised,
        })
        .collect()
}

/// Cursor is in a frontmatter input type-value slot.
///
/// The source index recognizes shortform scalars (`env: word`) and
/// dict/longform `type:` values even when the YAML is incomplete. The
/// replacement covers the whole current type token so accepting a row
/// does not leave a suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontmatterInputTypeCompletion {
    pub replacement_range: EditorRange,
    pub partial: String,
}

/// Detect catalog-backed `type` completion in a macro frontmatter block.
pub fn input_type_completion_at(
    document: &DocumentSnapshot,
    position: EditorPosition,
) -> Option<FrontmatterInputTypeCompletion> {
    let frontmatter = extract_frontmatter(document.text())?;
    let cursor = document.position_to_byte_offset(position)?;
    if cursor < frontmatter.start {
        return None;
    }
    let rel = cursor - frontmatter.start;
    if rel > frontmatter.text.len() {
        return None;
    }
    let index = FrontmatterSourceIndex::new(frontmatter.text);
    let (start, end) = type_value_slot_at(&index, rel)?;
    let abs_start = frontmatter.start + start;
    let abs_end = frontmatter.start + end;
    let replacement_range = document.byte_range_to_range(abs_start, abs_end)?;
    let partial_end = rel.clamp(start, end);
    let partial = frontmatter
        .text
        .get(start..partial_end)
        .unwrap_or_default()
        .trim_start_matches(['"', '\''])
        .to_string();
    Some(FrontmatterInputTypeCompletion {
        replacement_range,
        partial,
    })
}

/// Validate a whole frontmatter block, returning diagnostics that match the
/// macro LSP output exactly (it runs the same engine).
///
/// `text` may be a complete `---`-delimited frontmatter block (the canonical
/// form the panel serializes) or a bare YAML body without delimiters; either
/// is normalized to a complete block before validation.
pub fn validate(text: &str) -> Vec<EditorDiagnostic> {
    validate_with_registry(text, &InputTypeRegistry::builtin())
}

/// Registry-aware frontmatter validation.
pub fn validate_with_registry(
    text: &str,
    registry: &InputTypeRegistry,
) -> Vec<EditorDiagnostic> {
    if extract_frontmatter(text).is_some() {
        return diagnostics_with_registry(
            &DocumentSnapshot::new(text),
            registry,
        );
    }
    let body = strip_delimiter_lines(text);
    diagnostics_with_registry(
        &DocumentSnapshot::new(format!("---\n{body}\n---\n")),
        registry,
    )
}

/// Validate a single field's value in isolation, returning diagnostics that
/// match the LSP output for that field.
///
/// `value` is the YAML text that would follow `field:`. A single-line value is
/// placed inline; a multi-line value is indented as a YAML block so list and
/// structured values validate as written.
pub fn validate_field(field: &str, value: &str) -> Vec<EditorDiagnostic> {
    validate_field_with_registry(field, value, &InputTypeRegistry::builtin())
}

/// Registry-aware single-field validation.
pub fn validate_field_with_registry(
    field: &str,
    value: &str,
    registry: &InputTypeRegistry,
) -> Vec<EditorDiagnostic> {
    let body = if value.contains('\n') {
        let indented = value
            .lines()
            .map(|line| format!("  {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        format!("{field}:\n{indented}")
    } else {
        format!("{field}: {value}")
    };
    validate_with_registry(&body, registry)
}

/// Strip leading/trailing `---` delimiter lines so a bare body or a partially
/// delimited block normalizes to plain YAML before re-wrapping.
fn strip_delimiter_lines(text: &str) -> String {
    let mut lines: Vec<&str> = text.lines().collect();
    while lines.first().map(|line| line.trim_end_matches('\r').trim())
        == Some("---")
    {
        lines.remove(0);
    }
    while lines.last().map(|line| line.trim_end_matches('\r').trim())
        == Some("---")
    {
        lines.pop();
    }
    lines.join("\n")
}

struct FrontmatterDiagnosticBuilder<'a> {
    document: &'a DocumentSnapshot,
    frontmatter_start: usize,
    index: FrontmatterSourceIndex,
    diagnostics: Vec<EditorDiagnostic>,
    registry: InputTypeRegistry,
}

impl<'a> FrontmatterDiagnosticBuilder<'a> {
    fn new(
        document: &'a DocumentSnapshot,
        frontmatter_start: usize,
        index: FrontmatterSourceIndex,
        registry: InputTypeRegistry,
    ) -> Self {
        Self {
            document,
            frontmatter_start,
            index,
            diagnostics: Vec::new(),
            registry,
        }
    }

    fn finish(self) -> Vec<EditorDiagnostic> {
        self.diagnostics
    }

    fn push(
        &mut self,
        range: (usize, usize),
        severity: DiagnosticSeverity,
        code: &str,
        message: impl Into<String>,
    ) {
        self.push_with_data(range, severity, code, message, None);
    }

    fn push_with_data(
        &mut self,
        range: (usize, usize),
        severity: DiagnosticSeverity,
        code: &str,
        message: impl Into<String>,
        data: Option<EditorDiagnosticData>,
    ) {
        let start = self.frontmatter_start + range.0;
        let end = self.frontmatter_start + range.1;
        let Some(editor_range) = self.document.byte_range_to_range(start, end)
        else {
            return;
        };
        let mut diagnostic =
            EditorDiagnostic::new(editor_range, severity, code, message);
        if let Some(data) = data {
            diagnostic = diagnostic.with_data(data);
        }
        self.diagnostics.push(diagnostic);
    }

    fn replace_fix(
        &self,
        range: (usize, usize),
        title: String,
        value: String,
        preferred: bool,
    ) -> Option<EditorDiagnosticData> {
        let start = self.frontmatter_start + range.0;
        let end = self.frontmatter_start + range.1;
        let editor_range = self.document.byte_range_to_range(start, end)?;
        Some(EditorDiagnosticData {
            suggestions: vec![EditorDiagnosticSuggestion {
                value: value.clone(),
                title,
                edit: EditorTextEdit {
                    range: editor_range,
                    new_text: value,
                },
                preferred,
            }],
        })
    }

    fn field_key_range(&self, key: &str) -> (usize, usize) {
        self.index
            .field(key)
            .map(|field| field.key_range)
            .unwrap_or(self.index.fallback_range)
    }

    fn field_value_range(&self, key: &str) -> (usize, usize) {
        self.index
            .field(key)
            .map(|field| field.value_range)
            .unwrap_or_else(|| self.field_key_range(key))
    }

    fn field_item_range(&self, key: &str) -> (usize, usize) {
        self.index
            .field(key)
            .map(|field| field.item_range)
            .unwrap_or(self.index.fallback_range)
    }
}

impl FrontmatterSourceIndex {
    fn new(frontmatter: &str) -> Self {
        let lines = frontmatter_lines(frontmatter);
        let fallback_range = frontmatter_nonempty_range(frontmatter);
        let fields = scan_top_level_fields(frontmatter, &lines);
        let input = fields
            .iter()
            .find(|field| field.key == "input")
            .map(|field| scan_input_source(frontmatter, &lines, field));
        Self {
            text: frontmatter.to_string(),
            fields,
            input,
            fallback_range,
        }
    }

    fn field(&self, key: &str) -> Option<&KeyValueSource> {
        self.fields.iter().find(|field| field.key == key)
    }
}

fn type_value_slot_at(
    index: &FrontmatterSourceIndex,
    cursor: usize,
) -> Option<(usize, usize)> {
    type_value_slots(index)
        .into_iter()
        .find(|(start, end)| *start <= cursor && cursor <= *end)
}

fn type_value_slots(index: &FrontmatterSourceIndex) -> Vec<(usize, usize)> {
    let Some(input) = &index.input else {
        return Vec::new();
    };
    let mut slots = Vec::new();
    for short in &input.shortform {
        if let Some(field) = short.field("type") {
            if let Some(slot) = slot_for_field(&index.text, field) {
                slots.push(slot);
            }
        } else if let Some(slot) = slot_for_shortform_scalar(&index.text, short)
        {
            slots.push(slot);
        }
    }
    for long in &input.longform {
        if let Some(field) = long.field("type") {
            if let Some(slot) = slot_for_field(&index.text, field) {
                slots.push(slot);
            }
        }
    }
    slots
}

fn slot_for_field(
    text: &str,
    field: &KeyValueSource,
) -> Option<(usize, usize)> {
    same_line_type_slot(text, field.value_range, field.scalar.as_ref())
}

fn slot_for_shortform_scalar(
    text: &str,
    source: &ShortInputSource,
) -> Option<(usize, usize)> {
    if !source.fields.is_empty() {
        return None;
    }
    same_line_type_slot(text, source.value_range, source.type_value.as_ref())
}

fn same_line_type_slot(
    text: &str,
    value_range: (usize, usize),
    scalar: Option<&ScalarSource>,
) -> Option<(usize, usize)> {
    if let Some(scalar) = scalar {
        return Some(scalar.range);
    }
    let line_end = text
        .get(value_range.0..)
        .and_then(|rest| rest.find('\n').map(|idx| value_range.0 + idx))
        .unwrap_or(text.len());
    let start = value_range.0.min(line_end);
    let raw = text.get(start..line_end)?;
    if let Some((rel_start, rel_end, _)) = scalar_value_range(raw) {
        return Some((start + rel_start, start + rel_end));
    }
    let trimmed_start = start + leading_whitespace_len(raw);
    let rest = text.get(trimmed_start..line_end).unwrap_or("");
    if rest.is_empty() || rest.starts_with('#') {
        return Some((trimmed_start, trimmed_start));
    }
    if rest.starts_with('{')
        || rest.starts_with('[')
        || rest.starts_with('|')
        || rest.starts_with('>')
    {
        return None;
    }
    let token_len = rest
        .find(|ch: char| {
            ch.is_whitespace() || matches!(ch, '#' | ',' | '}' | ']')
        })
        .unwrap_or(rest.len());
    Some((trimmed_start, trimmed_start + token_len))
}

fn validate_frontmatter_value_with_snapshot(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    value: &Value,
    snapshot: Option<&ModelValiditySnapshot>,
) {
    let Some(mapping) = value.as_mapping() else {
        builder.push(
            builder.index.fallback_range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_shape",
            "Macro frontmatter must be a YAML mapping",
        );
        return;
    };

    validate_top_level_fields(builder);
    validate_local_section_keys(builder, mapping);
    validate_name(builder, mapping);
    validate_input_with_snapshot(builder, mapping, snapshot);
    validate_tags(builder, mapping);
    validate_description(builder, mapping);
    validate_skill(builder, mapping);
    validate_snippet(builder, mapping);
    validate_log_skill_use(builder, mapping);
    validate_keywords(builder, mapping);
}

fn validate_top_level_fields(builder: &mut FrontmatterDiagnosticBuilder<'_>) {
    let fields = builder.index.fields.clone();
    for field in fields {
        if top_level_field_doc(&field.key).is_some() {
            continue;
        }
        builder.push(
            field.key_range,
            DiagnosticSeverity::Information,
            "unknown_macro_frontmatter_field",
            format!(
                "Unknown macro frontmatter field `{}` will be ignored",
                field.key
            ),
        );
    }
}

/// Both `macros:` (canonical) and `xprompts:` (retired) are detected by
/// presence before their values are parsed, so empty or null input still
/// counts as a conflict. Supplying both spellings is an error naming
/// `macros`; the retired spelling under a false legacy policy is rejected
/// by catalog loading instead, which owns the policy.
fn validate_local_section_keys(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    if yaml_mapping_get(mapping, "macros").is_none()
        || yaml_mapping_get(mapping, "xprompts").is_none()
    // legacy xprompt spelling
    {
        return;
    }
    let range = builder
        .index
        .fields
        .iter()
        .filter(|field| field.key == "macros" || field.key == "xprompts") // legacy xprompt spelling
        .max_by_key(|field| field.key_range.0)
        .map(|field| field.key_range)
        .unwrap_or(builder.index.fallback_range);
    builder.push(
        range,
        DiagnosticSeverity::Error,
        "duplicate_macro_frontmatter_section",
        "Duplicate macro definition keys `macros` and `xprompts`; keep only `macros`", // legacy xprompt spelling
    );
}

fn top_level_field_doc(field: &str) -> Option<&'static str> {
    TOP_LEVEL_FIELD_DOCS.iter().find_map(|(name, description)| {
        (*name == field).then_some(*description)
    })
}

fn validate_name(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "name") else {
        return;
    };
    let range = builder.field_value_range("name");
    let Some(raw) = yaml_scalar_to_string(value) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_name",
            "Macro name must be a non-empty scalar",
        );
        return;
    };
    let name = raw.trim();
    if name.is_empty() {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_name",
            "Macro name must not be empty",
        );
    } else if !is_referenceable_macro_name(name) {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "unreferenceable_macro_frontmatter_name",
            "Macro name cannot be referenced with the current #name grammar",
        );
    }
}

fn validate_input_with_snapshot(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
    snapshot: Option<&ModelValiditySnapshot>,
) {
    let Some(input) = yaml_mapping_get(mapping, "input") else {
        return;
    };
    let input_range = builder.field_value_range("input");
    if let Some(inputs) = input.as_mapping() {
        validate_shortform_inputs_with_snapshot(builder, inputs, snapshot);
    } else if let Some(inputs) = input.as_sequence() {
        validate_longform_inputs_with_snapshot(builder, inputs, snapshot);
    } else {
        builder.push(
            input_range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_shape",
            "Macro input must be a mapping or sequence",
        );
    }
}

fn validate_shortform_inputs_with_snapshot(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    inputs: &Mapping,
    snapshot: Option<&ModelValiditySnapshot>,
) {
    validate_shortform_duplicates(builder);

    for (idx, (name_value, raw)) in inputs.iter().enumerate() {
        let source = builder
            .index
            .input
            .as_ref()
            .and_then(|input| input.shortform.get(idx))
            .cloned();
        let name_range = source
            .as_ref()
            .map(|source| source.name_range)
            .unwrap_or_else(|| builder.field_item_range("input"));
        let item_range = source
            .as_ref()
            .map(|source| source.item_range)
            .unwrap_or(name_range);

        let Some(name) = yaml_scalar_to_string(name_value) else {
            builder.push(
                name_range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_name",
                "Macro input name must be a non-empty scalar",
            );
            continue;
        };
        validate_input_name(builder, &name, name_range);

        let (declared_type, type_known, named_type, catalog_choices) =
            validate_shortform_input_type(
                builder,
                raw,
                &name,
                source.as_ref(),
                item_range,
            );
        let choices = validate_input_choices(
            builder,
            raw.as_mapping()
                .and_then(|mapping| yaml_mapping_get(mapping, "choices")),
            declared_type,
            type_known,
            named_type.as_deref(),
            &catalog_choices,
            source.as_ref().and_then(|source| source.field("choices")),
            item_range,
        );
        validate_input_default_with_snapshot(
            builder,
            raw.as_mapping()
                .and_then(|mapping| yaml_mapping_get(mapping, "default")),
            &name,
            declared_type,
            type_known,
            named_type.as_deref(),
            &choices,
            source.as_ref().and_then(|source| {
                source.default_value.as_ref().map(|default| default.range)
            }),
            item_range,
            snapshot,
        );
        if let Some(mapping) = raw.as_mapping() {
            validate_input_description(
                builder,
                yaml_mapping_get(mapping, "description"),
                source
                    .as_ref()
                    .and_then(|source| source.field("description")),
                item_range,
            );
            validate_input_repeatable(
                builder,
                yaml_mapping_get(mapping, "repeatable"),
                source
                    .as_ref()
                    .and_then(|source| source.field("repeatable")),
                item_range,
                idx + 1 == inputs.len(),
            );
        }
        validate_nested_input_unknown_keys(builder, source.as_ref());
    }
}

fn validate_longform_inputs_with_snapshot(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    inputs: &[Value],
    snapshot: Option<&ModelValiditySnapshot>,
) {
    let mut seen_names = HashMap::<String, (usize, usize)>::new();
    for (idx, item) in inputs.iter().enumerate() {
        let source = builder
            .index
            .input
            .as_ref()
            .and_then(|input| input.longform.get(idx))
            .cloned();
        let item_range = source
            .as_ref()
            .map(|source| source.item_range)
            .unwrap_or_else(|| builder.field_item_range("input"));

        let Some(mapping) = item.as_mapping() else {
            builder.push(
                item_range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_item",
                "Longform macro input items must be mappings",
            );
            continue;
        };

        let name_source = source
            .as_ref()
            .and_then(|source| source.field("name"))
            .cloned();
        let name_range = name_source
            .as_ref()
            .and_then(|field| field.scalar.as_ref())
            .map(|scalar| scalar.range)
            .or_else(|| name_source.as_ref().map(|field| field.value_range))
            .unwrap_or(item_range);

        match yaml_mapping_get(mapping, "name").and_then(yaml_scalar_to_string)
        {
            Some(name) => {
                validate_input_name(builder, &name, name_range);
                if !name.trim().is_empty()
                    && seen_names.insert(name.clone(), name_range).is_some()
                {
                    builder.push(
                        name_range,
                        DiagnosticSeverity::Error,
                        "duplicate_macro_frontmatter_input",
                        format!("Duplicate macro input `{name}`"),
                    );
                }
            }
            None => builder.push(
                name_range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_name",
                "Longform macro input item needs a non-empty scalar name",
            ),
        }

        let input_name = yaml_mapping_get(mapping, "name")
            .and_then(yaml_scalar_to_string)
            .unwrap_or_else(|| "input".to_string());
        let type_source = source
            .as_ref()
            .and_then(|source| source.field("type"))
            .cloned();
        let (declared_type, type_known, named_type, catalog_choices) =
            validate_longform_input_type(
                builder,
                mapping,
                &input_name,
                type_source.as_ref(),
                item_range,
            );
        let choices = validate_input_choices(
            builder,
            yaml_mapping_get(mapping, "choices"),
            declared_type,
            type_known,
            named_type.as_deref(),
            &catalog_choices,
            source.as_ref().and_then(|source| source.field("choices")),
            item_range,
        );
        validate_input_default_with_snapshot(
            builder,
            yaml_mapping_get(mapping, "default"),
            &input_name,
            declared_type,
            type_known,
            named_type.as_deref(),
            &choices,
            source.as_ref().and_then(|source| {
                source
                    .field("default")
                    .and_then(|field| field.scalar.as_ref())
                    .map(|scalar| scalar.range)
            }),
            item_range,
            snapshot,
        );
        validate_input_description(
            builder,
            yaml_mapping_get(mapping, "description"),
            source
                .as_ref()
                .and_then(|source| source.field("description")),
            item_range,
        );
        validate_input_repeatable(
            builder,
            yaml_mapping_get(mapping, "repeatable"),
            source
                .as_ref()
                .and_then(|source| source.field("repeatable")),
            item_range,
            idx + 1 == inputs.len(),
        );
        validate_longform_unknown_keys(builder, source.as_ref());
    }
}

fn validate_shortform_duplicates(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
) {
    let Some(input) = builder.index.input.clone() else {
        return;
    };
    let mut seen = HashSet::<String>::new();
    for source in input.shortform {
        if seen.insert(source.name.clone()) {
            continue;
        }
        builder.push(
            source.name_range,
            DiagnosticSeverity::Error,
            "duplicate_macro_frontmatter_input",
            format!("Duplicate macro input `{}`", source.name),
        );
    }
}

fn validate_input_name(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    name: &str,
    range: (usize, usize),
) {
    let name = name.trim();
    if name.is_empty() {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_name",
            "Macro input name must not be empty",
        );
    } else if !is_jinja_identifier(name) {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "invalid_macro_frontmatter_input_identifier",
            "Macro input name should be a valid named-argument identifier",
        );
    }
}

fn validate_shortform_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    raw: &Value,
    input_name: &str,
    source: Option<&ShortInputSource>,
    fallback_range: (usize, usize),
) -> (InputType, bool, Option<String>, Vec<InputChoice>) {
    let (type_value, range) = if let Some(mapping) = raw.as_mapping() {
        (
            yaml_mapping_get(mapping, "type"),
            source
                .and_then(|source| source.type_value.as_ref())
                .map(|scalar| scalar.range),
        )
    } else {
        (
            Some(raw),
            source
                .and_then(|source| source.type_value.as_ref())
                .map(|scalar| scalar.range),
        )
    };
    validate_explicit_input_type(
        builder,
        type_value,
        input_name,
        range.unwrap_or(fallback_range),
        InputType::Line,
    )
}

fn validate_longform_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
    input_name: &str,
    source: Option<&KeyValueSource>,
    fallback_range: (usize, usize),
) -> (InputType, bool, Option<String>, Vec<InputChoice>) {
    let range = source
        .and_then(|source| source.scalar.as_ref())
        .map(|scalar| scalar.range)
        .or_else(|| source.map(|source| source.value_range))
        .unwrap_or(fallback_range);
    validate_explicit_input_type(
        builder,
        yaml_mapping_get(mapping, "type"),
        input_name,
        range,
        InputType::Line,
    )
}

fn validate_explicit_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    value: Option<&Value>,
    input_name: &str,
    range: (usize, usize),
    missing_type: InputType,
) -> (InputType, bool, Option<String>, Vec<InputChoice>) {
    let Some(value) = value else {
        return (missing_type, true, None, Vec::new());
    };
    let Some(raw) = yaml_scalar_to_string(value) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_type",
            "Macro input type must be a scalar",
        );
        return (InputType::Line, false, None, Vec::new());
    };
    match resolve_input_type(input_name, &raw, &builder.registry.clone()) {
        Ok(resolved) => {
            if resolved.deprecated {
                let data = builder.replace_fix(
                    range,
                    "Use `line`".to_string(),
                    "line".to_string(),
                    true,
                );
                builder.push_with_data(
                    range,
                    DiagnosticSeverity::Warning,
                    "deprecated_macro_frontmatter_input_type",
                    "Input type `string` is deprecated; use `line` instead",
                    data,
                );
            }
            (
                InputType::from_base(&resolved.base),
                true,
                resolved.named_type.clone(),
                resolved.choices.clone(),
            )
        }
        Err(error) => {
            let names =
                advertised_type_names_with_registry(&builder.registry.clone());
            let suggestions =
                suggest_closest(&raw, names.iter().map(String::as_str));
            let data = type_change_fixes(builder, range, &suggestions);
            builder.push_with_data(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_type",
                error.message,
                data,
            );
            (InputType::Line, false, None, Vec::new())
        }
    }
}

fn advertised_type_names_with_registry(
    registry: &InputTypeRegistry,
) -> Vec<String> {
    registry
        .entries()
        .iter()
        .filter(|entry| entry.advertised)
        .flat_map(|entry| {
            std::iter::once(entry.name.clone()).chain(entry.aliases.clone())
        })
        .collect()
}

fn type_change_fixes(
    builder: &FrontmatterDiagnosticBuilder<'_>,
    range: (usize, usize),
    suggestions: &[String],
) -> Option<EditorDiagnosticData> {
    if suggestions.is_empty() {
        return None;
    }
    let start = builder.frontmatter_start + range.0;
    let end = builder.frontmatter_start + range.1;
    let editor_range = builder.document.byte_range_to_range(start, end)?;
    Some(EditorDiagnosticData {
        suggestions: suggestions
            .iter()
            .enumerate()
            .map(|(index, value)| EditorDiagnosticSuggestion {
                value: value.clone(),
                title: format!("Change type to `{value}`"),
                edit: EditorTextEdit {
                    range: editor_range,
                    new_text: value.clone(),
                },
                preferred: index == 0,
            })
            .collect(),
    })
}

fn quote_yaml_plain(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 2);
    out.push('"');
    for ch in raw.chars() {
        if ch == '"' || ch == '\\' {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

/// Validate a declared `input`'s `choices` and return valid catalog choices.
///
/// `choices` is required and non-empty for `enum` and forbidden for every
/// other type. Choice validation is delegated to the shared input-type
/// catalog, and the returned values feed
/// [`validate_input_default_with_snapshot`]'s `enum` membership check.
#[allow(clippy::too_many_arguments)]
fn validate_input_choices(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    choices: Option<&Value>,
    declared_type: InputType,
    type_known: bool,
    named_type: Option<&str>,
    catalog_choices: &[InputChoice],
    source: Option<&KeyValueSource>,
    fallback_range: (usize, usize),
) -> Vec<InputChoice> {
    if !type_known {
        return Vec::new();
    }
    let range = source
        .and_then(|field| field.scalar.as_ref())
        .map(|scalar| scalar.range)
        .or_else(|| source.map(|field| field.value_range))
        .unwrap_or(fallback_range);
    if named_type.is_some_and(|name| name == "effort") {
        if choices.is_some() {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                "Macro input `effort` already defines its values; remove `choices`",
            );
        }
        return catalog_choices.to_vec();
    }
    if named_type.is_some_and(|name| name == "model") {
        if choices.is_some() {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                "Macro input choices is only valid for type `enum`",
            );
        }
        return Vec::new();
    }
    if declared_type != InputType::Enum {
        if choices.is_some() {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                "Macro input choices is only valid for type `enum`",
            );
        }
        return Vec::new();
    }
    let items = match choices.and_then(|value| value.as_sequence()) {
        Some(items) if !items.is_empty() => items,
        _ => {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                "Macro input type `enum` requires a non-empty `choices` list",
            );
            return Vec::new();
        }
    };
    let item_ranges = source
        .map(|source| choice_item_ranges(builder, source))
        .unwrap_or_default();
    let mut valid_choices = Vec::new();
    let mut seen = HashSet::<String>::new();
    for (idx, item) in items.iter().enumerate() {
        let source_item_range = item_ranges.get(idx).copied();
        let item_range = source_item_range.unwrap_or(range);
        let value_range = source_item_range.and_then(|item_range| {
            choice_value_source_range(&builder.index.text, item_range)
        });
        if let Some(message) = value_range.and_then(|value_range| {
            unquoted_choice_error(&builder.index.text, value_range)
        }) {
            let quote_range = value_range.unwrap_or(item_range);
            let raw = builder
                .index
                .text
                .get(quote_range.0..quote_range.1)
                .unwrap_or_default();
            let data = builder.replace_fix(
                quote_range,
                format!("Quote `{raw}`"),
                quote_yaml_plain(raw),
                true,
            );
            builder.push_with_data(
                quote_range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                message,
                data,
            );
            continue;
        }

        let validated = validate_enum_choices_yaml(std::slice::from_ref(item));
        for issue in validated.issues {
            let severity = match issue.severity {
                ChoiceIssueSeverity::Error => DiagnosticSeverity::Error,
                ChoiceIssueSeverity::Warning => DiagnosticSeverity::Warning,
            };
            builder.push(
                value_range.unwrap_or(item_range),
                severity,
                "invalid_macro_frontmatter_input_choices",
                issue.message,
            );
        }
        let Some(choice) = validated.choices.into_iter().next() else {
            continue;
        };
        if !seen.insert(choice.value.clone()) {
            builder.push(
                value_range.unwrap_or(item_range),
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_choices",
                format!("choice `{}` is declared twice", choice.value),
            );
            continue;
        }
        valid_choices.push(choice);
    }
    valid_choices
}

#[allow(clippy::too_many_arguments)]
fn validate_input_default_with_snapshot(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    default: Option<&Value>,
    input_name: &str,
    declared_type: InputType,
    type_known: bool,
    named_type: Option<&str>,
    choices: &[InputChoice],
    source_range: Option<(usize, usize)>,
    fallback_range: (usize, usize),
    snapshot: Option<&ModelValiditySnapshot>,
) {
    if !type_known {
        return;
    }
    let Some(default) = default else {
        return;
    };
    if default.is_null() {
        return;
    }
    let range = source_range.unwrap_or(fallback_range);
    if named_type.is_some_and(|name| name == "model") {
        let Some(raw) = yaml_scalar_to_string(default) else {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_default",
                "Macro input default must be a scalar or null",
            );
            return;
        };
        if raw.is_empty() || raw.chars().any(char::is_whitespace) {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_default",
                format!(
                    "Default value does not match input type `{}`",
                    declared_type_name(declared_type)
                ),
            );
            return;
        }
        let Some(snapshot) = snapshot else {
            return;
        };
        match crate::model_validity::classify_model_value(
            input_name, &raw, snapshot,
        ) {
            Ok(result) if result.ok => {}
            Ok(result) => {
                let mut data = None;
                if !result.suggestions.is_empty() {
                    let first = result.suggestions[0].clone();
                    data = builder.replace_fix(
                        range,
                        format!("Replace with `{first}`"),
                        first.clone(),
                        true,
                    );
                    // Expand to all suggestions with first preferred.
                    if let Some(mut full) = data {
                        full.suggestions = result
                            .suggestions
                            .iter()
                            .enumerate()
                            .map(|(index, value)| {
                                let start = builder.frontmatter_start + range.0;
                                let end = builder.frontmatter_start + range.1;
                                let editor_range = builder
                                    .document
                                    .byte_range_to_range(start, end)
                                    .unwrap_or(crate::editor::wire::EditorRange {
                                        start: crate::editor::wire::EditorPosition {
                                            line: 0,
                                            character: 0,
                                        },
                                        end: crate::editor::wire::EditorPosition {
                                            line: 0,
                                            character: 0,
                                        },
                                    });
                                crate::editor::wire::EditorDiagnosticSuggestion {
                                    value: value.clone(),
                                    title: format!("Replace with `{value}`"),
                                    edit:
                                        crate::editor::wire::EditorTextEdit {
                                            range: editor_range,
                                            new_text: value.clone(),
                                        },
                                    preferred: index == 0,
                                }
                            })
                            .collect();
                        data = Some(full);
                    }
                }
                builder.push_with_data(
                    range,
                    DiagnosticSeverity::Warning,
                    "invalid_macro_frontmatter_input_default",
                    result.message,
                    data,
                );
            }
            Err(_) => {}
        }
        return;
    }
    if declared_type == InputType::Enum {
        if let Some(message) = source_range.and_then(|source_range| {
            unquoted_choice_error(&builder.index.text, source_range)
        }) {
            let raw = source_range
                .and_then(|span| builder.index.text.get(span.0..span.1))
                .unwrap_or_default();
            let data = builder.replace_fix(
                range,
                format!("Quote `{raw}`"),
                quote_yaml_plain(raw),
                true,
            );
            builder.push_with_data(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_default",
                message,
                data,
            );
            return;
        }
        let raw = yaml_scalar_to_string(default).unwrap_or_default();
        if default.as_str().is_none() {
            let message = check_closed_set_default(&raw, choices)
                .unwrap_or_else(|| {
                    format!("default `{raw}` must be a string choice")
                });
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_default",
                message,
            );
            return;
        }
        if let Some(message) = check_closed_set_default(&raw, choices) {
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_macro_frontmatter_input_default",
                message,
            );
        }
        return;
    }
    let Some(raw) = yaml_scalar_to_string(default) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_default",
            "Macro input default must be a scalar or null",
        );
        return;
    };
    let valid = match declared_type {
        InputType::Word | InputType::Agent => {
            !raw.is_empty() && !raw.chars().any(char::is_whitespace)
        }
        InputType::Path => !raw.contains('\n') && !raw.contains('\r'),
        InputType::Line => !raw.contains('\n'),
        InputType::Text => true,
        InputType::Int => raw.parse::<i64>().is_ok(),
        InputType::Float => raw.parse::<f64>().is_ok(),
        InputType::Bool => is_bool_spelling(&raw),
        InputType::Enum => true,
        InputType::Code => true,
    };
    if !valid {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_default",
            format!(
                "Default value does not match input type `{}`",
                declared_type_name(declared_type)
            ),
        );
    }
}

fn validate_input_repeatable(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    repeatable: Option<&Value>,
    source: Option<&KeyValueSource>,
    fallback_range: (usize, usize),
    is_final: bool,
) {
    let Some(repeatable) = repeatable else {
        return;
    };
    let range = source
        .and_then(|field| field.scalar.as_ref())
        .map(|scalar| scalar.range)
        .or_else(|| source.map(|field| field.value_range))
        .unwrap_or(fallback_range);
    let Some(enabled) = repeatable.as_bool() else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_repeatable",
            "Macro input repeatable must be true or false",
        );
        return;
    };
    if enabled && !is_final {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "non_final_macro_frontmatter_repeatable_input",
            "A repeatable macro input must be the final positional input",
        );
    }
}

fn validate_input_description(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    description: Option<&Value>,
    source: Option<&KeyValueSource>,
    fallback_range: (usize, usize),
) {
    let Some(description) = description else {
        return;
    };
    let range = source
        .and_then(|source| source.scalar.as_ref())
        .map(|scalar| scalar.range)
        .or_else(|| source.map(|source| source.value_range))
        .unwrap_or(fallback_range);
    let Some(raw) = yaml_scalar_to_string(description) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_input_description",
            "Macro input description must be a scalar string value",
        );
        return;
    };
    if raw.contains('\n') {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "multiline_macro_frontmatter_input_description",
            "Macro input description should be a single line",
        );
    }
}

fn validate_nested_input_unknown_keys(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    source: Option<&ShortInputSource>,
) {
    let Some(source) = source.cloned() else {
        return;
    };
    for field in source.fields {
        if matches!(
            field.key.as_str(),
            "type" | "default" | "description" | "repeatable" | "choices"
        ) {
            continue;
        }
        builder.push(
            field.key_range,
            DiagnosticSeverity::Information,
            "unknown_macro_frontmatter_input_field",
            format!(
                "Unknown macro input field `{}` will be ignored",
                field.key
            ),
        );
    }
}

fn validate_longform_unknown_keys(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    source: Option<&LongInputSource>,
) {
    let Some(source) = source.cloned() else {
        return;
    };
    for field in source.fields {
        if matches!(
            field.key.as_str(),
            "name"
                | "type"
                | "default"
                | "description"
                | "repeatable"
                | "choices"
        ) {
            continue;
        }
        builder.push(
            field.key_range,
            DiagnosticSeverity::Information,
            "unknown_macro_frontmatter_input_field",
            format!(
                "Unknown macro input field `{}` will be ignored",
                field.key
            ),
        );
    }
}

fn validate_tags(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "tags") else {
        return;
    };
    if value.as_str().is_some() {
        return;
    }
    let Some(items) = value.as_sequence() else {
        builder.push(
            builder.field_value_range("tags"),
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_tags",
            "Macro tags must be a comma-separated string or sequence",
        );
        return;
    };
    let tag_ranges = flow_or_sequence_item_ranges(builder, "tags");
    for (idx, item) in items.iter().enumerate() {
        let valid = yaml_scalar_to_string(item)
            .map(|tag| !tag.trim().is_empty())
            .unwrap_or(false);
        if valid {
            continue;
        }
        builder.push(
            tag_ranges
                .get(idx)
                .copied()
                .unwrap_or_else(|| builder.field_value_range("tags")),
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_tags",
            "Macro tag entries must be non-empty scalars",
        );
    }
}

fn validate_description(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "description") else {
        return;
    };
    let range = builder.field_value_range("description");
    let Some(raw) = yaml_scalar_to_string(value) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_description",
            "Macro description must be a scalar string value",
        );
        return;
    };
    if raw.contains('\n') {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "multiline_macro_frontmatter_description",
            "Macro description should be a single line",
        );
    }
}

fn validate_skill(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "skill") else {
        return;
    };
    if value.as_bool().is_some() {
        validate_skill_description(builder, mapping);
        return;
    }
    let Some(items) = value.as_sequence() else {
        builder.push(
            builder.field_value_range("skill"),
            DiagnosticSeverity::Warning,
            "invalid_macro_frontmatter_skill",
            "Macro skill must be true, false, or a provider list",
        );
        return;
    };
    if items.is_empty() {
        builder.push(
            builder.field_value_range("skill"),
            DiagnosticSeverity::Warning,
            "empty_macro_frontmatter_skill",
            "Macro skill provider list should not be empty",
        );
        return;
    }
    let provider_ranges = flow_or_sequence_item_ranges(builder, "skill");
    for (idx, item) in items.iter().enumerate() {
        let valid = item
            .as_str()
            .map(|provider| !provider.trim().is_empty())
            .unwrap_or(false);
        if valid {
            continue;
        }
        builder.push(
            provider_ranges
                .get(idx)
                .copied()
                .unwrap_or_else(|| builder.field_value_range("skill")),
            DiagnosticSeverity::Warning,
            "invalid_macro_frontmatter_skill",
            "Macro skill providers must be non-empty strings",
        );
    }
    validate_skill_description(builder, mapping);
}

fn validate_skill_description(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    if !yaml_mapping_get(mapping, "skill").is_some_and(value_is_truthy) {
        return;
    }
    let description = yaml_mapping_get(mapping, "description")
        .and_then(yaml_scalar_to_string)
        .unwrap_or_default();
    if description.trim().is_empty() {
        builder.push(
            builder.field_key_range("skill"),
            DiagnosticSeverity::Warning,
            "missing_macro_skill_description",
            "Skill macros should include a useful description",
        );
    }
}

fn validate_snippet(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "snippet") else {
        return;
    };
    if value.as_bool().is_some() {
        return;
    }
    let range = builder.field_value_range("snippet");
    let Some(trigger) = value.as_str() else {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "invalid_macro_frontmatter_snippet",
            "Macro snippet must be true, false, or a trigger string",
        );
        return;
    };
    if !is_valid_snippet_trigger(trigger) {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_snippet_trigger",
            "Macro snippet trigger must use only ASCII letters, digits, or underscores",
        );
    }
}

fn validate_log_skill_use(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "log_skill_use") else {
        return;
    };
    if value.as_bool().is_some() {
        return;
    }
    builder.push(
        builder.field_value_range("log_skill_use"),
        DiagnosticSeverity::Warning,
        "invalid_macro_frontmatter_log_skill_use",
        "Macro log_skill_use must be true or false",
    );
}

fn validate_keywords(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(value) = yaml_mapping_get(mapping, "keywords") else {
        return;
    };
    let Some(items) = value.as_sequence() else {
        builder.push(
            builder.field_value_range("keywords"),
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_keywords",
            "Macro keywords must be a sequence of non-empty scalars",
        );
        return;
    };
    let keyword_ranges = flow_or_sequence_item_ranges(builder, "keywords");
    for (idx, item) in items.iter().enumerate() {
        let valid = yaml_scalar_to_string(item)
            .map(|keyword| !keyword.trim().is_empty())
            .unwrap_or(false);
        if valid {
            continue;
        }
        builder.push(
            keyword_ranges
                .get(idx)
                .copied()
                .unwrap_or_else(|| builder.field_value_range("keywords")),
            DiagnosticSeverity::Error,
            "invalid_macro_frontmatter_keywords",
            "Macro keyword entries must be non-empty scalars",
        );
    }
}

fn scan_top_level_fields(
    frontmatter: &str,
    lines: &[FrontmatterLine<'_>],
) -> Vec<KeyValueSource> {
    let mut fields = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.text.trim_start();
        if trimmed.is_empty()
            || trimmed.starts_with('#')
            || leading_whitespace_len(line.text) != 0
        {
            continue;
        }
        let Some(key) = yaml_line_key_colon_source(line.text) else {
            continue;
        };
        let block_end = block_end_line(lines, idx + 1, 0);
        let item_end = lines
            .get(block_end)
            .map(|line| line.start)
            .unwrap_or(frontmatter.len());
        let value = line_value_source(line, &key, item_end);
        fields.push(KeyValueSource {
            key: key.key,
            key_range: (line.start + key.key_start, line.start + key.key_end),
            value_range: value.value_range,
            item_range: (line.start, item_end),
            scalar: value.scalar,
        });
    }
    fields
}

fn scan_input_source(
    frontmatter: &str,
    lines: &[FrontmatterLine<'_>],
    input_field: &KeyValueSource,
) -> InputSourceIndex {
    if let Some(inline) = input_inline_value(frontmatter, input_field) {
        let trimmed_start = leading_whitespace_len(inline);
        let trimmed = &inline[trimmed_start..];
        let base = input_field.value_range.0 + trimmed_start;
        if trimmed.starts_with('{') {
            return InputSourceIndex {
                shortform: scan_flow_shortform_inputs(trimmed, base, 0),
                longform: Vec::new(),
            };
        }
        if trimmed.starts_with('[') {
            return InputSourceIndex {
                shortform: Vec::new(),
                longform: scan_flow_longform_inputs(trimmed, base, 0),
            };
        }
    }

    let Some((input_idx, input_indent)) =
        input_field_line(lines, input_field.key_range.0)
    else {
        return InputSourceIndex {
            ..InputSourceIndex::default()
        };
    };
    let block_end = lines
        .iter()
        .position(|line| line.start >= input_field.item_range.1)
        .unwrap_or(lines.len());
    let input_lines = &lines[input_idx + 1..block_end];
    let Some(first_child_indent) = child_indent(input_lines, input_indent)
    else {
        return InputSourceIndex {
            ..InputSourceIndex::default()
        };
    };
    let first_child = input_lines
        .iter()
        .find(|line| {
            let trimmed = line.text.trim_start();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && leading_whitespace_len(line.text) == first_child_indent
        })
        .map(|line| line.text[first_child_indent..].trim_start())
        .unwrap_or_default();
    if first_child.starts_with('-') {
        InputSourceIndex {
            shortform: Vec::new(),
            longform: scan_block_longform_inputs(
                frontmatter,
                input_lines,
                first_child_indent,
            ),
        }
    } else {
        InputSourceIndex {
            shortform: scan_block_shortform_inputs(
                frontmatter,
                input_lines,
                first_child_indent,
            ),
            longform: Vec::new(),
        }
    }
}

fn scan_block_shortform_inputs(
    frontmatter: &str,
    lines: &[FrontmatterLine<'_>],
    child_indent: usize,
) -> Vec<ShortInputSource> {
    let mut inputs = Vec::new();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx];
        if !is_significant_indent(line, child_indent) {
            idx += 1;
            continue;
        }
        let content = &line.text[child_indent..];
        let Some(key) = yaml_line_key_colon_source(content) else {
            idx += 1;
            continue;
        };
        let content_line = FrontmatterLine {
            start: line.start + child_indent,
            text: content,
        };
        let next_idx = next_sibling_line(lines, idx + 1, child_indent)
            .unwrap_or(lines.len());
        let item_end = lines
            .get(next_idx)
            .map(|line| line.start)
            .unwrap_or(frontmatter.len());
        let value = line_value_source(&content_line, &key, item_end);
        let mut source = ShortInputSource {
            name: key.key,
            name_range: (
                content_line.start + key.key_start,
                content_line.start + key.key_end,
            ),
            value_range: value.value_range,
            item_range: (line.start, item_end),
            type_value: None,
            default_value: None,
            fields: Vec::new(),
        };

        if let Some(scalar) = value.scalar {
            source.type_value = Some(scalar);
        } else if let Some(raw) = input_inline_value(
            frontmatter,
            &KeyValueSource {
                key: source.name.clone(),
                key_range: source.name_range,
                value_range: source.value_range,
                item_range: source.item_range,
                scalar: None,
            },
        ) {
            let trimmed_start = leading_whitespace_len(raw);
            let trimmed = &raw[trimmed_start..];
            let base = source.value_range.0 + trimmed_start;
            if trimmed.starts_with('{') {
                source.fields = scan_flow_mapping_entries(trimmed, base, 0);
            }
        }
        if source.fields.is_empty() {
            source.fields = scan_nested_block_fields(
                frontmatter,
                &lines[idx + 1..next_idx],
                child_indent,
            );
        }
        attach_type_and_default(&mut source);
        inputs.push(source);
        idx = next_idx;
    }
    inputs
}

fn scan_block_longform_inputs(
    frontmatter: &str,
    lines: &[FrontmatterLine<'_>],
    sequence_indent: usize,
) -> Vec<LongInputSource> {
    let mut inputs = Vec::new();
    let mut idx = 0usize;
    while idx < lines.len() {
        let line = lines[idx];
        if !is_significant_indent(line, sequence_indent) {
            idx += 1;
            continue;
        }
        let content = &line.text[sequence_indent..];
        let Some((item_offset, item)) = sequence_item_content(content) else {
            idx += 1;
            continue;
        };
        let next_idx = lines[idx + 1..]
            .iter()
            .position(|line| {
                is_significant_indent(*line, sequence_indent)
                    && sequence_item_content(&line.text[sequence_indent..])
                        .is_some()
            })
            .map(|offset| idx + 1 + offset)
            .unwrap_or(lines.len());
        let item_end = lines
            .get(next_idx)
            .map(|line| line.start)
            .unwrap_or(frontmatter.len());
        let item_start = line.start;
        let mut fields = Vec::new();
        let item_content_start = line.start + sequence_indent + item_offset;
        let item_trimmed_offset = leading_whitespace_len(item);
        let item_trimmed = &item[item_trimmed_offset..];
        let item_trimmed_start = item_content_start + item_trimmed_offset;
        if item_trimmed.starts_with('{') {
            fields.extend(scan_flow_mapping_entries(
                item_trimmed,
                item_trimmed_start,
                0,
            ));
        } else if let Some(key) = yaml_line_key_colon_source(item_trimmed) {
            let value = line_value_source(
                &FrontmatterLine {
                    start: item_trimmed_start,
                    text: item_trimmed,
                },
                &key,
                item_end,
            );
            fields.push(KeyValueSource {
                key: key.key,
                key_range: (
                    item_trimmed_start + key.key_start,
                    item_trimmed_start + key.key_end,
                ),
                value_range: value.value_range,
                item_range: (item_trimmed_start, item_end),
                scalar: value.scalar,
            });
        }
        fields.extend(scan_nested_block_fields(
            frontmatter,
            &lines[idx + 1..next_idx],
            sequence_indent,
        ));
        inputs.push(LongInputSource {
            item_range: (item_start, item_end),
            fields,
        });
        idx = next_idx;
    }
    inputs
}

fn scan_nested_block_fields(
    frontmatter: &str,
    lines: &[FrontmatterLine<'_>],
    parent_indent: usize,
) -> Vec<KeyValueSource> {
    let Some(field_indent) = child_indent(lines, parent_indent) else {
        return Vec::new();
    };
    let mut fields = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        if !is_significant_indent(*line, field_indent) {
            continue;
        }
        let content = &line.text[field_indent..];
        let Some(key) = yaml_line_key_colon_source(content) else {
            continue;
        };
        let content_line = FrontmatterLine {
            start: line.start + field_indent,
            text: content,
        };
        let next_idx = next_sibling_line(lines, idx + 1, field_indent)
            .unwrap_or(lines.len());
        let item_end = lines
            .get(next_idx)
            .map(|line| line.start)
            .unwrap_or(frontmatter.len());
        let value = line_value_source(&content_line, &key, item_end);
        fields.push(KeyValueSource {
            key: key.key,
            key_range: (
                content_line.start + key.key_start,
                content_line.start + key.key_end,
            ),
            value_range: value.value_range,
            item_range: (line.start, item_end),
            scalar: value.scalar,
        });
    }
    fields
}

fn scan_flow_shortform_inputs(
    text: &str,
    base_offset: usize,
    open_idx: usize,
) -> Vec<ShortInputSource> {
    scan_flow_mapping_entries(text, base_offset, open_idx)
        .into_iter()
        .map(|field| {
            let mut source = ShortInputSource {
                name: field.key.clone(),
                name_range: field.key_range,
                value_range: field.value_range,
                item_range: field.item_range,
                type_value: field.scalar.clone(),
                default_value: None,
                fields: Vec::new(),
            };
            if let Some(raw) = text.get(
                field.value_range.0.saturating_sub(base_offset)
                    ..field.value_range.1.saturating_sub(base_offset),
            ) {
                let trimmed_offset = leading_whitespace_len(raw);
                let trimmed = &raw[trimmed_offset..];
                let nested_base = field.value_range.0 + trimmed_offset;
                if trimmed.starts_with('{') {
                    source.type_value = None;
                    source.fields =
                        scan_flow_mapping_entries(trimmed, nested_base, 0);
                    attach_type_and_default(&mut source);
                }
            }
            source
        })
        .collect()
}

fn scan_flow_longform_inputs(
    text: &str,
    base_offset: usize,
    open_idx: usize,
) -> Vec<LongInputSource> {
    let Some(close_idx) = matching_flow_end(text, open_idx) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut idx = open_idx + 1;
    while idx < close_idx {
        idx = skip_flow_separators(text, idx, close_idx);
        if idx >= close_idx {
            break;
        }
        let value_end = skip_flow_value(text, idx, close_idx);
        let trimmed_offset = leading_whitespace_len(&text[idx..value_end]);
        let trimmed_start = idx + trimmed_offset;
        let trimmed = &text[trimmed_start..value_end];
        let fields = if trimmed.starts_with('{') {
            scan_flow_mapping_entries(trimmed, base_offset + trimmed_start, 0)
        } else {
            Vec::new()
        };
        out.push(LongInputSource {
            item_range: (base_offset + idx, base_offset + value_end),
            fields,
        });
        idx = value_end;
        if text.get(idx..idx + 1) == Some(",") {
            idx += 1;
        }
    }
    out
}

fn scan_flow_mapping_entries(
    text: &str,
    base_offset: usize,
    open_idx: usize,
) -> Vec<KeyValueSource> {
    let Some(close_idx) = matching_flow_end(text, open_idx) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut idx = open_idx + 1;
    while idx < close_idx {
        idx = skip_flow_separators(text, idx, close_idx);
        if idx >= close_idx {
            break;
        }
        let Some(key) = flow_key_colon_source(text, idx, close_idx) else {
            idx = skip_flow_value(text, idx, close_idx);
            continue;
        };
        let value_start = key.colon + 1;
        let value_end = skip_flow_value(text, value_start, close_idx);
        let raw = &text[value_start..value_end];
        let value_range = trimmed_or_raw_range(raw, base_offset + value_start);
        let scalar =
            scalar_value_range(raw).map(|(start, end, _value)| ScalarSource {
                range: (
                    base_offset + value_start + start,
                    base_offset + value_start + end,
                ),
            });
        out.push(KeyValueSource {
            key: key.key,
            key_range: (base_offset + key.key_start, base_offset + key.key_end),
            value_range,
            item_range: (base_offset + idx, base_offset + value_end),
            scalar,
        });
        idx = value_end;
        if text.get(idx..idx + 1) == Some(",") {
            idx += 1;
        }
    }
    out
}

fn attach_type_and_default(source: &mut ShortInputSource) {
    for field in &source.fields {
        if field.key == "type" {
            source.type_value = field.scalar.clone();
        } else if field.key == "default" {
            source.default_value = field.scalar.clone();
        }
    }
}

impl ShortInputSource {
    fn field(&self, key: &str) -> Option<&KeyValueSource> {
        self.fields.iter().find(|field| field.key == key)
    }
}

impl LongInputSource {
    fn field(&self, key: &str) -> Option<&KeyValueSource> {
        self.fields.iter().find(|field| field.key == key)
    }
}

fn flow_or_sequence_item_ranges(
    builder: &FrontmatterDiagnosticBuilder<'_>,
    field: &str,
) -> Vec<(usize, usize)> {
    let Some(source) = builder.index.field(field) else {
        return Vec::new();
    };
    let Some(raw) = input_inline_value(&builder.index.text, source) else {
        return block_sequence_item_ranges(
            &builder.index.text,
            source.item_range,
        );
    };
    let trimmed_offset = leading_whitespace_len(raw);
    let trimmed = &raw[trimmed_offset..];
    let base = source.value_range.0 + trimmed_offset;
    if !trimmed.starts_with('[') {
        return Vec::new();
    }
    flow_sequence_item_ranges(trimmed, base, 0)
}

fn choice_item_ranges(
    builder: &FrontmatterDiagnosticBuilder<'_>,
    source: &KeyValueSource,
) -> Vec<(usize, usize)> {
    if let Some(raw) = input_inline_value(&builder.index.text, source) {
        let trimmed_offset = leading_whitespace_len(raw);
        let trimmed = &raw[trimmed_offset..];
        if trimmed.starts_with('[') {
            return flow_sequence_item_ranges(
                trimmed,
                source.value_range.0 + trimmed_offset,
                0,
            );
        }
    }
    block_choice_item_ranges(&builder.index.text, source.item_range)
}

fn block_choice_item_ranges(
    text: &str,
    source_range: (usize, usize),
) -> Vec<(usize, usize)> {
    let Some(block) = text.get(source_range.0..source_range.1) else {
        return Vec::new();
    };
    let lines = frontmatter_lines(block);
    let Some(sequence_indent) = lines.iter().find_map(|line| {
        let indent = leading_whitespace_len(line.text);
        sequence_item_content(&line.text[indent..]).map(|_| indent)
    }) else {
        return Vec::new();
    };
    let starts: Vec<(usize, usize)> = lines
        .iter()
        .enumerate()
        .filter_map(|(idx, line)| {
            if leading_whitespace_len(line.text) != sequence_indent {
                return None;
            }
            let (offset, _) =
                sequence_item_content(&line.text[sequence_indent..])?;
            Some((idx, line.start + sequence_indent + offset))
        })
        .collect();
    starts
        .iter()
        .enumerate()
        .filter_map(|(idx, (line_idx, content_offset))| {
            let start = source_range.0 + content_offset;
            let end = starts
                .get(idx + 1)
                .map(|(next_line_idx, _)| {
                    source_range.0 + lines[*next_line_idx].start
                })
                .unwrap_or(source_range.1);
            (start <= end && *line_idx < lines.len())
                .then_some((start, end.max(start)))
        })
        .collect()
}

fn choice_value_source_range(
    text: &str,
    item_range: (usize, usize),
) -> Option<(usize, usize)> {
    let raw = text.get(item_range.0..item_range.1)?;
    let trimmed_offset = leading_whitespace_len(raw);
    let trimmed = &raw[trimmed_offset..];
    let base = item_range.0 + trimmed_offset;
    if trimmed.starts_with('{') {
        return scan_flow_mapping_entries(trimmed, base, 0)
            .into_iter()
            .find(|field| field.key == "value")
            .and_then(|field| field.scalar.map(|scalar| scalar.range));
    }
    for line in frontmatter_lines(raw) {
        let indent = leading_whitespace_len(line.text);
        let content = &line.text[indent..];
        let Some(key) = yaml_line_key_colon_source(content) else {
            continue;
        };
        if key.key != "value" {
            continue;
        }
        let source_line = FrontmatterLine {
            start: item_range.0 + line.start + indent,
            text: content,
        };
        return line_value_source(&source_line, &key, item_range.1)
            .scalar
            .map(|scalar| scalar.range);
    }
    scalar_value_range(trimmed)
        .map(|(start, end, _value)| (base + start, base + end))
}

fn unquoted_choice_error(
    text: &str,
    source_range: (usize, usize),
) -> Option<String> {
    let raw = text.get(source_range.0..source_range.1)?;
    let previous = text[..source_range.0].chars().next_back();
    if matches!(previous, Some('\'') | Some('"'))
        || !pyyaml_plain_scalar_is_non_string(raw)
    {
        return None;
    }
    unquoted_plain_scalar_choice_error(raw)
}

fn block_sequence_item_ranges(
    text: &str,
    item_range: (usize, usize),
) -> Vec<(usize, usize)> {
    let lines = frontmatter_lines(&text[item_range.0..item_range.1]);
    lines
        .iter()
        .filter_map(|line| {
            let indent = leading_whitespace_len(line.text);
            let content = &line.text[indent..];
            sequence_item_content(content).map(|(offset, item)| {
                let start = item_range.0 + line.start + indent + offset;
                let end = start + item.trim_end().len();
                (start, end.max(start + 1))
            })
        })
        .collect()
}

fn flow_sequence_item_ranges(
    text: &str,
    base_offset: usize,
    open_idx: usize,
) -> Vec<(usize, usize)> {
    let Some(close_idx) = matching_flow_end(text, open_idx) else {
        return Vec::new();
    };
    let mut ranges = Vec::new();
    let mut idx = open_idx + 1;
    while idx < close_idx {
        idx = skip_flow_separators(text, idx, close_idx);
        if idx >= close_idx {
            break;
        }
        let end = skip_flow_value(text, idx, close_idx);
        ranges.push(trimmed_or_raw_range(&text[idx..end], base_offset + idx));
        idx = end;
        if text.get(idx..idx + 1) == Some(",") {
            idx += 1;
        }
    }
    ranges
}

#[derive(Debug, Clone)]
struct KeyColonSource {
    key: String,
    key_start: usize,
    key_end: usize,
    colon: usize,
}

#[derive(Debug, Clone)]
struct LineValueSource {
    value_range: (usize, usize),
    scalar: Option<ScalarSource>,
}

fn line_value_source(
    line: &FrontmatterLine<'_>,
    key: &KeyColonSource,
    item_end: usize,
) -> LineValueSource {
    let value_start = key.colon + 1;
    let value = &line.text[value_start..];
    let scalar =
        scalar_value_range(value).map(|(start, end, _value)| ScalarSource {
            range: (
                line.start + value_start + start,
                line.start + value_start + end,
            ),
        });
    let value_range = scalar
        .as_ref()
        .map(|scalar| scalar.range)
        .unwrap_or_else(|| {
            let inline = value.trim();
            if inline.is_empty() || inline.starts_with('#') {
                (line.start + value_start, item_end)
            } else {
                trimmed_or_raw_range(value, line.start + value_start)
            }
        });
    LineValueSource {
        value_range,
        scalar,
    }
}

fn input_inline_value<'a>(
    text: &'a str,
    source: &KeyValueSource,
) -> Option<&'a str> {
    let line_end = text[source.value_range.0..]
        .find('\n')
        .map(|offset| source.value_range.0 + offset)
        .unwrap_or(text.len());
    let raw = text.get(source.value_range.0..line_end)?;
    let trimmed = raw.trim_start();
    (!trimmed.is_empty() && !trimmed.starts_with('#')).then_some(raw)
}

fn input_field_line(
    lines: &[FrontmatterLine<'_>],
    key_start: usize,
) -> Option<(usize, usize)> {
    lines.iter().enumerate().find_map(|(idx, line)| {
        (line.start <= key_start && key_start <= line.start + line.text.len())
            .then_some((idx, leading_whitespace_len(line.text)))
    })
}

fn child_indent(
    lines: &[FrontmatterLine<'_>],
    parent_indent: usize,
) -> Option<usize> {
    lines
        .iter()
        .filter_map(|line| {
            let trimmed = line.text.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }
            let indent = leading_whitespace_len(line.text);
            (indent > parent_indent).then_some(indent)
        })
        .min()
}

fn next_sibling_line(
    lines: &[FrontmatterLine<'_>],
    start_idx: usize,
    sibling_indent: usize,
) -> Option<usize> {
    lines[start_idx..]
        .iter()
        .position(|line| {
            let trimmed = line.text.trim_start();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && leading_whitespace_len(line.text) <= sibling_indent
        })
        .map(|offset| start_idx + offset)
}

fn block_end_line(
    lines: &[FrontmatterLine<'_>],
    start_idx: usize,
    parent_indent: usize,
) -> usize {
    lines[start_idx..]
        .iter()
        .position(|line| {
            let trimmed = line.text.trim_start();
            !trimmed.is_empty()
                && !trimmed.starts_with('#')
                && leading_whitespace_len(line.text) <= parent_indent
        })
        .map(|offset| start_idx + offset)
        .unwrap_or(lines.len())
}

fn is_significant_indent(line: FrontmatterLine<'_>, indent: usize) -> bool {
    let trimmed = line.text.trim_start();
    !trimmed.is_empty()
        && !trimmed.starts_with('#')
        && leading_whitespace_len(line.text) == indent
}

fn yaml_error_range(
    frontmatter: &str,
    error: &serde_yaml::Error,
) -> Option<(usize, usize)> {
    let location = error.location()?;
    let mut idx = location.index();
    if idx > frontmatter.len() || !frontmatter.is_char_boundary(idx) {
        idx = line_column_to_offset(
            frontmatter,
            location.line(),
            location.column(),
        )?;
    }
    let end = next_char_boundary(frontmatter, idx).unwrap_or(idx);
    Some((idx, end.max(idx + (idx < frontmatter.len()) as usize)))
}

fn line_column_to_offset(
    text: &str,
    line: usize,
    column: usize,
) -> Option<usize> {
    let target_line = line.saturating_sub(1);
    let target_column = column.saturating_sub(1);
    let mut line_idx = 0usize;
    let mut line_start = 0usize;
    for (idx, byte) in text.bytes().enumerate() {
        if line_idx == target_line {
            line_start = idx;
            break;
        }
        if byte == b'\n' {
            line_idx += 1;
            line_start = idx + 1;
        }
    }
    if target_line > line_idx {
        return None;
    }
    let line_text = text[line_start..]
        .split_once('\n')
        .map(|(line, _)| line)
        .unwrap_or(&text[line_start..]);
    let mut columns = 0usize;
    for (byte_idx, ch) in line_text.char_indices() {
        if columns == target_column {
            return Some(line_start + byte_idx);
        }
        columns += ch.len_utf16();
    }
    (columns == target_column).then_some(line_start + line_text.len())
}

fn extract_frontmatter(text: &str) -> Option<FrontmatterBlock<'_>> {
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
            return Some(FrontmatterBlock {
                text: &text[frontmatter_start..line_start],
                start: frontmatter_start,
            });
        }
        if line_end == text.len() {
            break;
        }
        line_start = line_end + 1;
    }
    None
}

fn frontmatter_lines(text: &str) -> Vec<FrontmatterLine<'_>> {
    let mut lines = Vec::new();
    let mut start = 0usize;
    while start < text.len() {
        let end = text[start..]
            .find('\n')
            .map(|idx| start + idx)
            .unwrap_or(text.len());
        lines.push(FrontmatterLine {
            start,
            text: text[start..end].trim_end_matches('\r'),
        });
        if end == text.len() {
            break;
        }
        start = end + 1;
    }
    lines
}

fn frontmatter_nonempty_range(text: &str) -> (usize, usize) {
    let start = text.len() - text.trim_start().len();
    let end = text.trim_end().len();
    if start < end {
        (start, end)
    } else {
        (0, text.len())
    }
}

fn yaml_line_key_colon_source(line: &str) -> Option<KeyColonSource> {
    let colon = line.find(':')?;
    let raw_key = &line[..colon];
    let leading = leading_whitespace_len(raw_key);
    let key = raw_key.trim();
    if key.is_empty() || key.starts_with('#') || key.starts_with('-') {
        return None;
    }
    let trailing = raw_key.len() - raw_key.trim_end().len();
    let (key_start, key_end, key_value) =
        unquoted_key_span(key, leading, raw_key.len() - trailing);
    Some(KeyColonSource {
        key: key_value.to_string(),
        key_start,
        key_end,
        colon,
    })
}

fn unquoted_key_span(
    key: &str,
    raw_start: usize,
    raw_end: usize,
) -> (usize, usize, &str) {
    let Some(first) = key.chars().next() else {
        return (raw_start, raw_end, key);
    };
    if !matches!(first, '"' | '\'') {
        return (raw_start, raw_end, key);
    }
    let quote_len = first.len_utf8();
    let Some(unquoted) = key
        .strip_prefix(first)
        .and_then(|key| key.strip_suffix(first))
    else {
        return (raw_start, raw_end, key);
    };
    (
        raw_start + quote_len,
        raw_end.saturating_sub(quote_len),
        unquoted,
    )
}

fn flow_key_colon_source(
    text: &str,
    start: usize,
    limit: usize,
) -> Option<KeyColonSource> {
    let key_start = start + leading_whitespace_len(&text[start..limit]);
    if key_start >= limit {
        return None;
    }
    let first = text[key_start..].chars().next()?;
    let (key_end, key_range_start, key_range_end, key) =
        if first == '"' || first == '\'' {
            let after_quote = skip_quoted(text, key_start)?;
            let content_start = key_start + first.len_utf8();
            let content_end = after_quote - first.len_utf8();
            (
                after_quote,
                content_start,
                content_end,
                text[content_start..content_end].to_string(),
            )
        } else {
            let mut end = key_start;
            for (offset, ch) in text[key_start..limit].char_indices() {
                if ch == ':' || ch.is_whitespace() || ch == ',' || ch == '}' {
                    break;
                }
                end = key_start + offset + ch.len_utf8();
            }
            if end == key_start {
                return None;
            }
            (end, key_start, end, text[key_start..end].trim().to_string())
        };
    let colon = key_end + leading_whitespace_len(text.get(key_end..limit)?);
    (colon < limit && text.get(colon..colon + 1) == Some(":")).then_some(
        KeyColonSource {
            key,
            key_start: key_range_start,
            key_end: key_range_end,
            colon,
        },
    )
}

fn sequence_item_content(content: &str) -> Option<(usize, &str)> {
    let after_dash = content.strip_prefix('-')?;
    if after_dash.is_empty() {
        return Some((1, after_dash));
    }
    let whitespace_len = leading_whitespace_len(after_dash);
    if whitespace_len == 0 {
        return None;
    }
    Some((1 + whitespace_len, &after_dash[whitespace_len..]))
}

fn scalar_value_range(value: &str) -> Option<(usize, usize, String)> {
    let start = leading_whitespace_len(value);
    let rest = &value[start..];
    if rest.is_empty() || rest.starts_with('#') {
        return None;
    }
    let first = rest.chars().next()?;
    if first == '"' || first == '\'' {
        let quote_end = skip_quoted(value, start)?;
        let content_start = start + first.len_utf8();
        let content_end = quote_end - first.len_utf8();
        return Some((
            content_start,
            content_end,
            value[content_start..content_end].to_string(),
        ));
    }
    if matches!(first, '{' | '[' | '|' | '>') {
        return None;
    }

    let mut end = value.len();
    for (offset, ch) in value[start..].char_indices() {
        if matches!(ch, ',' | '}' | ']' | '#') || ch == '\r' || ch == '\n' {
            end = start + offset;
            break;
        }
    }
    let trimmed = value[start..end].trim_end();
    if trimmed.is_empty() {
        return None;
    }
    let end = start + trimmed.len();
    Some((start, end, trimmed.to_string()))
}

fn trimmed_or_raw_range(value: &str, base_offset: usize) -> (usize, usize) {
    let start = leading_whitespace_len(value);
    let trimmed = &value[start..];
    let end = start + trimmed.trim_end().len();
    if start < end {
        (base_offset + start, base_offset + end)
    } else {
        (base_offset, base_offset + value.len())
    }
}

fn leading_whitespace_len(text: &str) -> usize {
    text.len() - text.trim_start().len()
}

fn skip_flow_separators(text: &str, start: usize, limit: usize) -> usize {
    let mut idx = start;
    while idx < limit {
        let Some(ch) = text[idx..].chars().next() else {
            break;
        };
        if ch == ',' || ch.is_whitespace() {
            idx += ch.len_utf8();
        } else {
            break;
        }
    }
    idx
}

fn skip_flow_value(text: &str, start: usize, limit: usize) -> usize {
    let mut idx = start + leading_whitespace_len(&text[start..limit]);
    let mut stack: Vec<char> = Vec::new();
    while idx < limit {
        let Some(ch) = text[idx..].chars().next() else {
            break;
        };
        if ch == '"' || ch == '\'' {
            idx = skip_quoted(text, idx).unwrap_or(limit);
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' if stack.last() == Some(&ch) => {
                stack.pop();
            }
            ',' | '}' | ']' if stack.is_empty() => break,
            _ => {}
        }
        idx += ch.len_utf8();
    }
    idx
}

fn matching_flow_end(text: &str, open_idx: usize) -> Option<usize> {
    let open = text[open_idx..].chars().next()?;
    let close = match open {
        '{' => '}',
        '[' => ']',
        _ => return None,
    };
    let mut stack = vec![close];
    let mut idx = open_idx + open.len_utf8();
    while idx < text.len() {
        let ch = text[idx..].chars().next()?;
        if ch == '"' || ch == '\'' {
            idx = skip_quoted(text, idx)?;
            continue;
        }
        match ch {
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' if stack.last() == Some(&ch) => {
                stack.pop();
                if stack.is_empty() {
                    return Some(idx);
                }
            }
            _ => {}
        }
        idx += ch.len_utf8();
    }
    None
}

fn skip_quoted(text: &str, quote_start: usize) -> Option<usize> {
    let quote = text[quote_start..].chars().next()?;
    if !matches!(quote, '"' | '\'') {
        return None;
    }
    let mut escaped = false;
    let mut idx = quote_start + quote.len_utf8();
    while idx < text.len() {
        let ch = text[idx..].chars().next()?;
        if quote == '"' && escaped {
            escaped = false;
            idx += ch.len_utf8();
            continue;
        }
        if quote == '"' && ch == '\\' {
            escaped = true;
            idx += ch.len_utf8();
            continue;
        }
        idx += ch.len_utf8();
        if ch == quote {
            return Some(idx);
        }
    }
    None
}

fn next_char_boundary(text: &str, byte_idx: usize) -> Option<usize> {
    let ch = text.get(byte_idx..)?.chars().next()?;
    Some(byte_idx + ch.len_utf8())
}

fn yaml_mapping_get<'a>(mapping: &'a Mapping, key: &str) -> Option<&'a Value> {
    mapping.get(Value::String(key.to_string()))
}

fn yaml_scalar_to_string(value: &Value) -> Option<String> {
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

impl InputType {
    fn from_base(base: &str) -> Self {
        match base {
            "word" => InputType::Word,
            "agent" => InputType::Agent,
            "text" => InputType::Text,
            "path" => InputType::Path,
            "int" => InputType::Int,
            "float" => InputType::Float,
            "bool" => InputType::Bool,
            "enum" => InputType::Enum,
            "code" => InputType::Code,
            _ => InputType::Line,
        }
    }
}

fn declared_type_name(input_type: InputType) -> &'static str {
    match input_type {
        InputType::Word => "word",
        InputType::Agent => "agent",
        InputType::Line => "line",
        InputType::Text => "text",
        InputType::Path => "path",
        InputType::Int => "int",
        InputType::Bool => "bool",
        InputType::Float => "float",
        InputType::Enum => "enum",
        InputType::Code => "code",
    }
}

fn is_bool_spelling(raw: &str) -> bool {
    matches!(
        raw.to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
    )
}

fn is_referenceable_macro_name(name: &str) -> bool {
    name.split('/').all(is_jinja_identifier)
}

fn is_jinja_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == '_')
        && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn is_valid_snippet_trigger(trigger: &str) -> bool {
    !trigger.is_empty()
        && trigger
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

pub(crate) fn value_is_truthy(value: &Value) -> bool {
    value.as_bool().unwrap_or_else(|| {
        value
            .as_sequence()
            .map(|items| !items.is_empty())
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::macro_input_types::builtin_catalog;

    fn has_error(diagnostics: &[EditorDiagnostic]) -> bool {
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }

    fn diagnostic_text(text: &str, diagnostic: &EditorDiagnostic) -> String {
        let document = DocumentSnapshot::new(text);
        let start = document
            .position_to_byte_offset(diagnostic.range.start)
            .unwrap();
        let end = document
            .position_to_byte_offset(diagnostic.range.end)
            .unwrap();
        text[start..end].to_string()
    }

    #[test]
    fn field_schema_is_ordered_documented_and_parity_scoped() {
        let schema = field_schema();
        let names: Vec<&str> =
            schema.iter().map(|field| field.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "name",
                "description",
                "tags",
                "input",
                "macros",
                "skill",
                "snippet"
            ]
        );
        for field in &schema {
            assert!(!field.required);
            assert!(
                !field.description.is_empty(),
                "{} has no description",
                field.name
            );
            assert!(!field.example.is_empty(), "{} has no example", field.name);
            // Descriptions must come from the shared hover/LSP source.
            assert_eq!(
                field.description.as_str(),
                top_level_field_doc(&field.name).unwrap(),
                "{} description drifted from TOP_LEVEL_FIELD_DOCS",
                field.name
            );
        }
        let skill = schema.iter().find(|field| field.name == "skill").unwrap();
        assert_eq!(skill.kind, FrontmatterFieldKind::BoolOrList);
        assert!(skill.allowed_values.is_some());
        let input = schema.iter().find(|field| field.name == "input").unwrap();
        assert_eq!(input.kind, FrontmatterFieldKind::Structured);
    }

    #[test]
    fn input_type_schema_matches_parser_spellings() {
        let schema = input_type_schema();
        let catalog = builtin_catalog();
        let names: Vec<&str> =
            schema.iter().map(|input| input.name.as_str()).collect();
        assert_eq!(
            names,
            catalog
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>()
        );
        for input in &schema {
            assert!(!input.rule.is_empty(), "{} has no rule", input.name);
            assert!(!input.description.is_empty());
            let entry = catalog
                .iter()
                .find(|entry| entry.name == input.name)
                .unwrap();
            assert_eq!(input.kind, entry.kind);
            assert_eq!(input.source, entry.source);
            assert_eq!(input.advertised, entry.advertised);
            let registry = InputTypeRegistry::builtin();
            assert!(
                resolve_input_type(&input.name, &input.name, &registry).is_ok()
            );
            for alias in &input.aliases {
                assert!(
                    resolve_input_type(&input.name, alias, &registry).is_ok()
                );
            }
        }
        let int = schema.iter().find(|input| input.name == "int").unwrap();
        assert_eq!(int.aliases, vec!["integer".to_string()]);
        let bool_type =
            schema.iter().find(|input| input.name == "bool").unwrap();
        assert_eq!(bool_type.aliases, vec!["boolean".to_string()]);
        let string =
            schema.iter().find(|input| input.name == "string").unwrap();
        assert!(!string.advertised);
        assert_eq!(
            string.kind,
            crate::macro_input_types::InputTypeKind::Scalar
        );
    }

    fn type_slot(
        text: &str,
        cursor_needle: &str,
    ) -> FrontmatterInputTypeCompletion {
        let document = DocumentSnapshot::new(text);
        let byte = text
            .find(cursor_needle)
            .unwrap_or_else(|| panic!("missing {cursor_needle:?}"))
            + cursor_needle.len();
        let position = document.byte_offset_to_position(byte).unwrap();
        input_type_completion_at(&document, position).unwrap_or_else(|| {
            panic!("expected type slot at {cursor_needle:?}")
        })
    }

    fn type_slot_text(
        text: &str,
        completion: &FrontmatterInputTypeCompletion,
    ) -> String {
        let document = DocumentSnapshot::new(text);
        let start = document
            .position_to_byte_offset(completion.replacement_range.start)
            .unwrap();
        let end = document
            .position_to_byte_offset(completion.replacement_range.end)
            .unwrap();
        text[start..end].to_string()
    }

    #[test]
    fn completes_shortform_scalar_type_even_when_yaml_is_incomplete() {
        let text = "---\nname: deploy\ninput:\n  env: wo\n---\nbody\n";
        let completion = type_slot(text, "env: wo");
        assert_eq!(completion.partial, "wo");
        assert_eq!(type_slot_text(text, &completion), "wo");
    }

    #[test]
    fn completes_empty_shortform_scalar_type() {
        let text = "---\nname: deploy\ninput:\n  env: \n---\n";
        let completion = type_slot(text, "env: ");
        assert_eq!(completion.partial, "");
        assert_eq!(type_slot_text(text, &completion), "");
    }

    #[test]
    fn completes_longform_type_field() {
        let text = "---\ninput:\n  - name: env\n    type: en\n---\n";
        let completion = type_slot(text, "type: en");
        assert_eq!(completion.partial, "en");
        assert_eq!(type_slot_text(text, &completion), "en");
    }

    #[test]
    fn completes_dict_type_field() {
        let text = "---\ninput:\n  env:\n    type: \n    choices: [a]\n---\n";
        let completion = type_slot(text, "type: ");
        assert_eq!(completion.partial, "");
    }

    #[test]
    fn replaces_the_whole_current_type_value() {
        let text = "---\ninput:\n  env: word\n---\n";
        let document = DocumentSnapshot::new(text);
        let byte = text.find("wo").unwrap() + 2;
        let position = document.byte_offset_to_position(byte).unwrap();
        let completion = input_type_completion_at(&document, position).unwrap();
        assert_eq!(completion.partial, "wo");
        assert_eq!(type_slot_text(text, &completion), "word");
    }

    #[test]
    fn does_not_complete_type_names_in_unrelated_yaml_or_body() {
        let text = "---\nname: deploy\ndescription: word\ninput:\n  env: word\n---\nbody word\n";
        let document = DocumentSnapshot::new(text);
        for byte in [
            text.find("description: word").unwrap() + "description: word".len(),
            text.find("name: deploy").unwrap() + "name: deploy".len(),
            text.find("body word").unwrap() + "body word".len(),
        ] {
            let position = document.byte_offset_to_position(byte).unwrap();
            assert!(
                input_type_completion_at(&document, position).is_none(),
                "unexpected type slot at byte {byte}"
            );
        }
    }

    #[test]
    fn frontmatter_input_type_wire_defaults_new_catalog_fields() {
        let input: FrontmatterInputType =
            serde_json::from_value(serde_json::json!({
                "name": "line",
                "aliases": [],
                "rule": "A single line.",
            }))
            .unwrap();
        assert_eq!(input.kind, crate::macro_input_types::InputTypeKind::Scalar);
        assert!(input.description.is_empty());
        assert_eq!(
            input.source,
            crate::macro_input_types::CatalogSource::Builtin
        );
        assert!(input.advertised);
    }

    #[test]
    fn validates_repeatable_input_metadata_and_final_position() {
        let valid = validate(
            "---\ninput:\n  names:\n    type: agent\n    repeatable: true\n---\n",
        );
        assert!(!has_error(&valid), "{valid:?}");

        let non_final = validate(
            "---\ninput:\n  names:\n    type: agent\n    repeatable: true\n  mode: word\n---\n",
        );
        assert!(non_final.iter().any(|diagnostic| {
            diagnostic.code == "non_final_macro_frontmatter_repeatable_input"
        }));

        let non_boolean = validate(
            "---\ninput:\n  names:\n    type: agent\n    repeatable: yes\n---\n",
        );
        assert!(non_boolean.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_repeatable"
        }));
    }

    #[test]
    fn validate_accepts_known_good_block() {
        let text = "---\ndescription: Refactor the auth module\ntags: refactor, backend\ninput:\n  service: word\n---\n";
        assert!(!has_error(&validate(text)), "{:?}", validate(text));
    }

    #[test]
    fn validate_accepts_bare_body_without_delimiters() {
        let diagnostics = validate("description: Refactor the auth module");
        assert!(!has_error(&diagnostics), "{diagnostics:?}");
    }

    #[test]
    fn validate_flags_known_bad_input_type() {
        let diagnostics = validate("---\ninput:\n  service: wordd\n---\n");
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            })
            .unwrap();
        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(
            diagnostic.message,
            "input `service` has unknown type `wordd`; did you mean `word`?"
        );
    }

    #[test]
    fn unknown_type_suggests_enum_and_string_is_deprecated_warning() {
        let unknown = validate("---\ninput:\n  mode:\n    type: enmu\n---\n");
        let diagnostic = unknown
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_type"
            })
            .unwrap();
        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(
            diagnostic.message,
            "input `mode` has unknown type `enmu`; did you mean `enum`?"
        );

        let deprecated = validate("---\ninput:\n  mode: string\n---\n");
        let warning = deprecated
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "deprecated_macro_frontmatter_input_type"
            })
            .unwrap();
        assert_eq!(warning.severity, DiagnosticSeverity::Warning);
        assert_eq!(
            warning.message,
            "Input type `string` is deprecated; use `line` instead"
        );
        let string_fix = warning.data.as_ref().expect("string quick fix");
        assert_eq!(string_fix.suggestions[0].title, "Use `line`");
        assert_eq!(string_fix.suggestions[0].edit.new_text, "line");
        assert!(string_fix.suggestions[0].preferred);

        let type_fix =
            diagnostic.data.as_ref().expect("unknown type quick fix");
        assert_eq!(type_fix.suggestions[0].title, "Change type to `enum`");
        assert_eq!(type_fix.suggestions[0].edit.new_text, "enum");
        assert!(type_fix.suggestions[0].preferred);
    }

    #[test]
    fn validate_accepts_enum_choices_shortform_and_longform() {
        let shortform = validate(
            "---\ninput:\n  mode:\n    type: enum\n    choices: [fast, slow]\n---\n",
        );
        assert!(!has_error(&shortform), "{shortform:?}");

        let longform = validate(
            "---\ninput:\n  - name: mode\n    type: enum\n    choices:\n      - value: fast\n        label: Fast\n      - value: slow\n---\n",
        );
        assert!(!has_error(&longform), "{longform:?}");
    }

    #[test]
    fn validate_flags_enum_without_choices() {
        let diagnostics =
            validate("---\ninput:\n  mode:\n    type: enum\n---\n");
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_choices"
        }));
    }

    #[test]
    fn validate_flags_choices_on_non_enum_type() {
        let diagnostics = validate(
            "---\ninput:\n  mode:\n    type: word\n    choices: [fast, slow]\n---\n",
        );
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_choices"
        }));
    }

    #[test]
    fn validate_flags_duplicate_choice_values() {
        let diagnostics = validate(
            "---\ninput:\n  mode:\n    type: enum\n    choices: [fast, fast]\n---\n",
        );
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_choices"
        }));
    }

    #[test]
    fn validate_quotes_yaml_typed_enum_choices_at_each_item() {
        for text in [
            "---\ninput:\n  mode:\n    type: enum\n    choices: [yes, no]\n---\n",
            "---\ninput:\n  mode:\n    type: enum\n    choices:\n      - value: yes\n        description: Boolean-like token\n      - value: no\n---\n",
        ] {
            let diagnostics = validate(text);
            let diagnostic = diagnostics
                .iter()
                .find(|diagnostic| {
                    diagnostic.code
                        == "invalid_macro_frontmatter_input_choices"
                })
                .unwrap();
            assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
            assert_eq!(diagnostic_text(text, diagnostic), "yes");
            assert_eq!(
                diagnostic.message,
                "choice `yes` must be quoted (\"yes\"): YAML reads it as a boolean"
            );
            let quote = diagnostic.data.as_ref().expect("quote quick fix");
            assert_eq!(quote.suggestions[0].title, "Quote `yes`");
            assert_eq!(quote.suggestions[0].edit.new_text, "\"yes\"");
            assert_eq!(diagnostic_text(text, diagnostic), "yes");
        }
    }

    #[test]
    fn validate_checks_enum_default_membership() {
        let valid = validate(
            "---\ninput:\n  mode:\n    type: enum\n    choices: [fast, slow]\n    default: fast\n---\n",
        );
        assert!(!has_error(&valid), "{valid:?}");

        let invalid = validate(
            "---\ninput:\n  mode:\n    type: enum\n    choices: [fast, slow]\n    default: turbo\n---\n",
        );
        let diagnostic = invalid
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_default"
            })
            .unwrap();
        assert_eq!(diagnostic.severity, DiagnosticSeverity::Error);
        assert_eq!(
            diagnostic.message,
            "default `turbo` is not one of fast | slow"
        );
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

    fn validate_with_snapshot(
        text: &str,
        snapshot: Option<&ModelValiditySnapshot>,
    ) -> Vec<EditorDiagnostic> {
        diagnostics_with_snapshot(&DocumentSnapshot::new(text), snapshot)
    }

    #[test]
    fn model_default_warns_and_effort_default_errors() {
        let snapshot = model_snapshot();
        let bad_model = "---\ninput:\n  claude_model:\n    type: model\n    default: opsu\n---\n";
        let diagnostics = validate_with_snapshot(bad_model, Some(&snapshot));
        let found = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_default"
            })
            .expect("model default warning");
        assert_eq!(found.severity, DiagnosticSeverity::Warning);
        assert!(
            found.message.contains("expects a model"),
            "unexpected message: {}",
            found.message
        );

        let bad_effort = "---\ninput:\n  effort:\n    type: effort\n    default: turbo\n---\n";
        let diagnostics = validate_with_snapshot(bad_effort, Some(&snapshot));
        let found = diagnostics
            .iter()
            .find(|diagnostic| {
                diagnostic.code == "invalid_macro_frontmatter_input_default"
            })
            .expect("effort default error");
        assert_eq!(found.severity, DiagnosticSeverity::Error);

        for default in ["claude/opus@xhigh", "@large", "fakey-large"] {
            let text = format!(
                "---\ninput:\n  claude_model:\n    type: model\n    default: {default}\n---\n"
            );
            let diagnostics = validate_with_snapshot(&text, Some(&snapshot));
            assert!(
                diagnostics.iter().all(|diagnostic| diagnostic.code
                    != "invalid_macro_frontmatter_input_default"),
                "{default}: {diagnostics:?}"
            );
        }

        let skipped = validate_with_snapshot(bad_model, None);
        assert!(
            skipped.iter().all(|diagnostic| diagnostic.code
                != "invalid_macro_frontmatter_input_default"),
            "{skipped:?}"
        );
    }

    #[test]
    fn path_defaults_allow_spaces_and_reject_line_breaks() {
        let spaced = validate(
            "---\ninput:\n  file:\n    type: path\n    default: 'src/my file.rs'\n---\n",
        );
        assert!(!has_error(&spaced), "{spaced:?}");

        let multiline = validate(
            "---\ninput:\n  file:\n    type: path\n    default: \"src/my\\nfile.rs\"\n---\n",
        );
        assert!(multiline.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_default"
                && diagnostic.severity == DiagnosticSeverity::Error
        }));
    }

    #[test]
    fn validate_accepts_log_skill_use_boolean() {
        let text = "---\nskill: true\ndescription: Plan helper\nlog_skill_use: false\n---\n";
        let diagnostics = validate(text);
        assert!(!has_error(&diagnostics), "{diagnostics:?}");
        assert!(
            !diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "unknown_macro_frontmatter_field"
            }),
            "{diagnostics:?}"
        );
    }

    #[test]
    fn validate_flags_non_boolean_log_skill_use() {
        for text in [
            "---\nlog_skill_use: \"false\"\n---\n",
            "---\nlog_skill_use: no thanks\n---\n",
        ] {
            let diagnostics = validate(text);
            assert!(
                diagnostics.iter().any(|diagnostic| {
                    diagnostic.code == "invalid_macro_frontmatter_log_skill_use"
                }),
                "{text:?} -> {diagnostics:?}"
            );
        }
    }

    #[test]
    fn hover_documents_log_skill_use_field() {
        let doc = DocumentSnapshot::new(
            "---\nskill: true\ndescription: Plan helper\nlog_skill_use: false\n---\n",
        );
        let field_start = doc.text().find("log_skill_use").unwrap();
        let payload = hover(
            &doc,
            EditorPosition {
                line: 3,
                character: 2,
            },
        )
        .unwrap();

        assert_eq!(
            payload.range,
            doc.byte_range_to_range(
                field_start,
                field_start + "log_skill_use".len()
            )
            .unwrap()
        );
        assert!(payload.markdown.contains("**log_skill_use**"));
        assert!(payload.markdown.contains("audit directive"));
    }

    #[test]
    fn validate_field_isolates_a_single_property() {
        assert!(!has_error(&validate_field(
            "description",
            "Refactor the auth module"
        )));
        let bad = validate_field("snippet", "bad-trigger!");
        assert!(bad.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_snippet_trigger"
        }));
    }

    #[test]
    fn validate_field_handles_block_values() {
        // A multi-line value is indented as a YAML block under the field key.
        let diagnostics =
            validate_field("input", "service: wordd\nregion: word");
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_macro_frontmatter_input_type"
        }));
    }
}

#[cfg(test)]
mod authored_inputs_tests {
    use super::*;

    fn has_error(diagnostics: &[EditorDiagnostic]) -> bool {
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    }

    #[test]
    fn accepts_macros_section_without_unknown_field() {
        let diagnostics =
            validate("---\nmacros:\n  _helper:\n    content: Hi\n---\nBody");
        assert!(
            diagnostics.iter().all(|diagnostic| diagnostic.code
                != "unknown_macro_frontmatter_field"),
            "{diagnostics:?}"
        );
        assert!(!has_error(&diagnostics), "{diagnostics:?}");
    }

    #[test]
    fn duplicate_local_sections_are_an_error_naming_macros() {
        for body in [
            "xprompts:\n  a:\n    content: A\nmacros:\n  b:\n    content: B\n", // legacy xprompt spelling
            "macros:\nxprompts:\n", // legacy xprompt spelling
        ] {
            let diagnostics = validate(&format!("---\n{body}---\n"));
            let hit = diagnostics
                .iter()
                .find(|diagnostic| {
                    diagnostic.code == "duplicate_macro_frontmatter_section"
                })
                .expect("duplicate authored keys diagnose, {diagnostics:?}");
            assert_eq!(hit.severity, DiagnosticSeverity::Error);
            assert!(hit.message.contains("macros"), "{hit:?}");
        }
    }
}

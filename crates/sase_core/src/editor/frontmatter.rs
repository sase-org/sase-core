use serde_yaml::{Mapping, Value};
use std::collections::{HashMap, HashSet};

use super::token::DocumentSnapshot;
use super::wire::{
    DiagnosticSeverity, EditorDiagnostic, EditorPosition, FrontmatterFieldKind,
    FrontmatterFieldSchema, FrontmatterInputType, HoverPayload,
};

const XPROMPT_INPUT_TYPE_EXPECTED: &str =
    "word, line, text, path, agent, int/integer, bool/boolean, float";

/// Ordered panel field descriptors: `(name, kind, allowed_values, example)`.
///
/// This is the field set the prompt frontmatter panel offers (xprompt `.md`
/// parity). Descriptions are not duplicated here; they are sourced from
/// [`TOP_LEVEL_FIELD_DOCS`] via [`top_level_field_doc`] so the panel and the
/// hover/LSP guidance never drift. `keywords` is a valid xprompt field but is
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
        "xprompts",
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
        "Overrides the xprompt reference name used in catalogs and completions.",
    ),
    (
        "input",
        "Declares named inputs accepted by this xprompt. Supports shortform mappings and longform sequences.",
    ),
    (
        "tags",
        "Adds tags for catalog filtering and dynamic-memory matching.",
    ),
    (
        "description",
        "Provides the single-line summary shown in completions, hovers, and picker previews.",
    ),
    (
        "skill",
        "Marks this xprompt as a slash skill. Use true, false, or a provider list.",
    ),
    (
        "snippet",
        "Exposes this xprompt as a completion snippet. Use true, false, or a custom trigger.",
    ),
    (
        "log_skill_use",
        "Controls whether generated skill files include the `sase skill use ...` audit directive. Use true or false; defaults to true and only applies to skill xprompts.",
    ),
    (
        "keywords",
        "Defines dynamic-memory keywords. They are matched when tags include `memory` or the file is under `sase/memory/`.",
    ),
    (
        "xprompts",
        "Defines local xprompts available only within the current file. Reference them from the body with `#name`.",
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
}

pub(super) fn diagnostics(
    document: &DocumentSnapshot,
) -> Vec<EditorDiagnostic> {
    let Some(frontmatter) = extract_frontmatter(document.text()) else {
        return Vec::new();
    };
    let index = FrontmatterSourceIndex::new(frontmatter.text);
    let mut builder =
        FrontmatterDiagnosticBuilder::new(document, frontmatter.start, index);

    let value = match serde_yaml::from_str::<Value>(frontmatter.text) {
        Ok(value) => value,
        Err(error) => {
            let range = yaml_error_range(frontmatter.text, &error)
                .unwrap_or(builder.index.fallback_range);
            builder.push(
                range,
                DiagnosticSeverity::Error,
                "invalid_xprompt_frontmatter_yaml",
                format!("Invalid xprompt frontmatter YAML: {error}"),
            );
            return builder.finish();
        }
    };

    validate_frontmatter_value(&mut builder, &value);
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
/// powers hover and the xprompt LSP, so the panel never drifts from the editor.
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

/// The catalog of supported `input` types, with aliases and per-type guidance.
///
/// Powers the input collection modal's per-type rule text. The names and
/// aliases mirror [`parse_input_type`]'s accepted spellings.
pub fn input_type_schema() -> Vec<FrontmatterInputType> {
    InputType::ALL
        .iter()
        .map(|input_type| FrontmatterInputType {
            name: declared_type_name(*input_type).to_string(),
            aliases: input_type
                .aliases()
                .iter()
                .map(|alias| (*alias).to_string())
                .collect(),
            rule: input_type.rule().to_string(),
        })
        .collect()
}

/// Validate a whole frontmatter block, returning diagnostics that match the
/// xprompt LSP output exactly (it runs the same engine).
///
/// `text` may be a complete `---`-delimited frontmatter block (the canonical
/// form the panel serializes) or a bare YAML body without delimiters; either
/// is normalized to a complete block before validation.
pub fn validate(text: &str) -> Vec<EditorDiagnostic> {
    if extract_frontmatter(text).is_some() {
        return diagnostics(&DocumentSnapshot::new(text));
    }
    let body = strip_delimiter_lines(text);
    diagnostics(&DocumentSnapshot::new(format!("---\n{body}\n---\n")))
}

/// Validate a single field's value in isolation, returning diagnostics that
/// match the LSP output for that field.
///
/// `value` is the YAML text that would follow `field:`. A single-line value is
/// placed inline; a multi-line value is indented as a YAML block so list and
/// structured values validate as written.
pub fn validate_field(field: &str, value: &str) -> Vec<EditorDiagnostic> {
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
    validate(&body)
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
}

impl<'a> FrontmatterDiagnosticBuilder<'a> {
    fn new(
        document: &'a DocumentSnapshot,
        frontmatter_start: usize,
        index: FrontmatterSourceIndex,
    ) -> Self {
        Self {
            document,
            frontmatter_start,
            index,
            diagnostics: Vec::new(),
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
        let start = self.frontmatter_start + range.0;
        let end = self.frontmatter_start + range.1;
        let Some(range) = self.document.byte_range_to_range(start, end) else {
            return;
        };
        self.diagnostics.push(EditorDiagnostic {
            range,
            severity,
            code: code.to_string(),
            message: message.into(),
        });
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

fn validate_frontmatter_value(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    value: &Value,
) {
    let Some(mapping) = value.as_mapping() else {
        builder.push(
            builder.index.fallback_range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_shape",
            "Xprompt frontmatter must be a YAML mapping",
        );
        return;
    };

    validate_top_level_fields(builder);
    validate_name(builder, mapping);
    validate_input(builder, mapping);
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
            "unknown_xprompt_frontmatter_field",
            format!(
                "Unknown xprompt frontmatter field `{}` will be ignored",
                field.key
            ),
        );
    }
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
            "invalid_xprompt_frontmatter_name",
            "Xprompt name must be a non-empty scalar",
        );
        return;
    };
    let name = raw.trim();
    if name.is_empty() {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_name",
            "Xprompt name must not be empty",
        );
    } else if !is_referenceable_xprompt_name(name) {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "unreferenceable_xprompt_frontmatter_name",
            "Xprompt name cannot be referenced with the current #name grammar",
        );
    }
}

fn validate_input(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
) {
    let Some(input) = yaml_mapping_get(mapping, "input") else {
        return;
    };
    let input_range = builder.field_value_range("input");
    if let Some(inputs) = input.as_mapping() {
        validate_shortform_inputs(builder, inputs);
    } else if let Some(inputs) = input.as_sequence() {
        validate_longform_inputs(builder, inputs);
    } else {
        builder.push(
            input_range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_input_shape",
            "Xprompt input must be a mapping or sequence",
        );
    }
}

fn validate_shortform_inputs(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    inputs: &Mapping,
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
                "invalid_xprompt_frontmatter_input_name",
                "Xprompt input name must be a non-empty scalar",
            );
            continue;
        };
        validate_input_name(builder, &name, name_range);

        let (declared_type, type_known) = validate_shortform_input_type(
            builder,
            raw,
            source.as_ref(),
            item_range,
        );
        validate_input_default(
            builder,
            raw.as_mapping()
                .and_then(|mapping| yaml_mapping_get(mapping, "default")),
            declared_type,
            type_known,
            source.as_ref().and_then(|source| {
                source.default_value.as_ref().map(|default| default.range)
            }),
            item_range,
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

fn validate_longform_inputs(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    inputs: &[Value],
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
                "invalid_xprompt_frontmatter_input_item",
                "Longform xprompt input items must be mappings",
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
                        "duplicate_xprompt_frontmatter_input",
                        format!("Duplicate xprompt input `{name}`"),
                    );
                }
            }
            None => builder.push(
                name_range,
                DiagnosticSeverity::Error,
                "invalid_xprompt_frontmatter_input_name",
                "Longform xprompt input item needs a non-empty scalar name",
            ),
        }

        let type_source = source
            .as_ref()
            .and_then(|source| source.field("type"))
            .cloned();
        let (declared_type, type_known) = validate_longform_input_type(
            builder,
            mapping,
            type_source.as_ref(),
            item_range,
        );
        validate_input_default(
            builder,
            yaml_mapping_get(mapping, "default"),
            declared_type,
            type_known,
            source.as_ref().and_then(|source| {
                source
                    .field("default")
                    .and_then(|field| field.scalar.as_ref())
                    .map(|scalar| scalar.range)
            }),
            item_range,
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
            "duplicate_xprompt_frontmatter_input",
            format!("Duplicate xprompt input `{}`", source.name),
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
            "invalid_xprompt_frontmatter_input_name",
            "Xprompt input name must not be empty",
        );
    } else if !is_jinja_identifier(name) {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "invalid_xprompt_frontmatter_input_identifier",
            "Xprompt input name should be a valid named-argument identifier",
        );
    }
}

fn validate_shortform_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    raw: &Value,
    source: Option<&ShortInputSource>,
    fallback_range: (usize, usize),
) -> (InputType, bool) {
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
        range.unwrap_or(fallback_range),
        InputType::Line,
    )
}

fn validate_longform_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    mapping: &Mapping,
    source: Option<&KeyValueSource>,
    fallback_range: (usize, usize),
) -> (InputType, bool) {
    let range = source
        .and_then(|source| source.scalar.as_ref())
        .map(|scalar| scalar.range)
        .or_else(|| source.map(|source| source.value_range))
        .unwrap_or(fallback_range);
    validate_explicit_input_type(
        builder,
        yaml_mapping_get(mapping, "type"),
        range,
        InputType::Line,
    )
}

fn validate_explicit_input_type(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    value: Option<&Value>,
    range: (usize, usize),
    missing_type: InputType,
) -> (InputType, bool) {
    let Some(value) = value else {
        return (missing_type, true);
    };
    let Some(raw) = yaml_scalar_to_string(value) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_input_type",
            format!(
                "Invalid xprompt input type. Expected one of: {XPROMPT_INPUT_TYPE_EXPECTED}"
            ),
        );
        return (InputType::Line, false);
    };
    let Some(input_type) = parse_input_type(&raw) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_input_type",
            format!(
                "Invalid xprompt input type `{raw}`. Expected one of: {XPROMPT_INPUT_TYPE_EXPECTED}"
            ),
        );
        return (InputType::Line, false);
    };
    (input_type, true)
}

fn validate_input_default(
    builder: &mut FrontmatterDiagnosticBuilder<'_>,
    default: Option<&Value>,
    declared_type: InputType,
    type_known: bool,
    source_range: Option<(usize, usize)>,
    fallback_range: (usize, usize),
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
    let Some(raw) = yaml_scalar_to_string(default) else {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_input_default",
            "Xprompt input default must be a scalar or null",
        );
        return;
    };
    let valid = match declared_type {
        InputType::Word | InputType::Agent => {
            !raw.is_empty() && !raw.chars().any(char::is_whitespace)
        }
        InputType::Path => !raw.chars().any(char::is_whitespace),
        InputType::Line => !raw.contains('\n'),
        InputType::Text => true,
        InputType::Int => raw.parse::<i64>().is_ok(),
        InputType::Float => raw.parse::<f64>().is_ok(),
        InputType::Bool => is_bool_spelling(&raw),
    };
    if !valid {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_input_default",
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
            "invalid_xprompt_frontmatter_input_repeatable",
            "Xprompt input repeatable must be true or false",
        );
        return;
    };
    if enabled && !is_final {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "non_final_xprompt_frontmatter_repeatable_input",
            "A repeatable xprompt input must be the final positional input",
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
            "invalid_xprompt_frontmatter_input_description",
            "Xprompt input description must be a scalar string value",
        );
        return;
    };
    if raw.contains('\n') {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "multiline_xprompt_frontmatter_input_description",
            "Xprompt input description should be a single line",
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
            "type" | "default" | "description" | "repeatable"
        ) {
            continue;
        }
        builder.push(
            field.key_range,
            DiagnosticSeverity::Information,
            "unknown_xprompt_frontmatter_input_field",
            format!(
                "Unknown xprompt input field `{}` will be ignored",
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
            "name" | "type" | "default" | "description" | "repeatable"
        ) {
            continue;
        }
        builder.push(
            field.key_range,
            DiagnosticSeverity::Information,
            "unknown_xprompt_frontmatter_input_field",
            format!(
                "Unknown xprompt input field `{}` will be ignored",
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
            "invalid_xprompt_frontmatter_tags",
            "Xprompt tags must be a comma-separated string or sequence",
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
            "invalid_xprompt_frontmatter_tags",
            "Xprompt tag entries must be non-empty scalars",
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
            "invalid_xprompt_frontmatter_description",
            "Xprompt description must be a scalar string value",
        );
        return;
    };
    if raw.contains('\n') {
        builder.push(
            range,
            DiagnosticSeverity::Warning,
            "multiline_xprompt_frontmatter_description",
            "Xprompt description should be a single line",
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
            "invalid_xprompt_frontmatter_skill",
            "Xprompt skill must be true, false, or a provider list",
        );
        return;
    };
    if items.is_empty() {
        builder.push(
            builder.field_value_range("skill"),
            DiagnosticSeverity::Warning,
            "empty_xprompt_frontmatter_skill",
            "Xprompt skill provider list should not be empty",
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
            "invalid_xprompt_frontmatter_skill",
            "Xprompt skill providers must be non-empty strings",
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
            "missing_xprompt_skill_description",
            "Skill xprompts should include a useful description",
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
            "invalid_xprompt_frontmatter_snippet",
            "Xprompt snippet must be true, false, or a trigger string",
        );
        return;
    };
    if !is_valid_snippet_trigger(trigger) {
        builder.push(
            range,
            DiagnosticSeverity::Error,
            "invalid_xprompt_frontmatter_snippet_trigger",
            "Xprompt snippet trigger must use only ASCII letters, digits, or underscores",
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
        "invalid_xprompt_frontmatter_log_skill_use",
        "Xprompt log_skill_use must be true or false",
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
            "invalid_xprompt_frontmatter_keywords",
            "Xprompt keywords must be a sequence of non-empty scalars",
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
            "invalid_xprompt_frontmatter_keywords",
            "Xprompt keyword entries must be non-empty scalars",
        );
    }
    if !tags_contain_memory(mapping)
        && !builder.document.has_implicit_memory_tag()
    {
        builder.push(
            builder.field_key_range("keywords"),
            DiagnosticSeverity::Warning,
            "missing_xprompt_memory_tag",
            "Xprompt keywords are only matched dynamically when tags include `memory`",
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
    /// Every input type, in catalog order, for schema enumeration.
    const ALL: [InputType; 8] = [
        InputType::Word,
        InputType::Agent,
        InputType::Line,
        InputType::Text,
        InputType::Path,
        InputType::Int,
        InputType::Float,
        InputType::Bool,
    ];

    /// Accepted aliases for this type's canonical name (see
    /// [`parse_input_type`]).
    fn aliases(self) -> &'static [&'static str] {
        match self {
            InputType::Int => &["integer"],
            InputType::Bool => &["boolean"],
            _ => &[],
        }
    }

    /// One-line human rule describing the values this type accepts. Mirrors the
    /// checks in [`validate_input_default`] so guidance and validation agree.
    fn rule(self) -> &'static str {
        match self {
            InputType::Word => "A single word with no whitespace.",
            InputType::Agent => "A non-empty agent name with no whitespace.",
            InputType::Line => "A single line of text with no line breaks.",
            InputType::Text => "Free-form text that may span multiple lines.",
            InputType::Path => "A filesystem path with no whitespace.",
            InputType::Int => "A whole number.",
            InputType::Float => "A number, optionally with a decimal point.",
            InputType::Bool => {
                "A boolean: true or false (also yes/no, on/off, 1/0)."
            }
        }
    }
}

fn parse_input_type(raw: &str) -> Option<InputType> {
    match raw.to_ascii_lowercase().as_str() {
        "word" => Some(InputType::Word),
        "agent" => Some(InputType::Agent),
        "line" => Some(InputType::Line),
        "text" => Some(InputType::Text),
        "path" => Some(InputType::Path),
        "int" | "integer" => Some(InputType::Int),
        "bool" | "boolean" => Some(InputType::Bool),
        "float" => Some(InputType::Float),
        _ => None,
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
    }
}

fn is_bool_spelling(raw: &str) -> bool {
    matches!(
        raw.to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "on" | "false" | "0" | "no" | "off"
    )
}

fn is_referenceable_xprompt_name(name: &str) -> bool {
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

fn tags_contain_memory(mapping: &Mapping) -> bool {
    let Some(tags) = yaml_mapping_get(mapping, "tags") else {
        return false;
    };
    if let Some(raw) = tags.as_str() {
        return raw.split(',').map(str::trim).any(|tag| tag == "memory");
    }
    tags.as_sequence().is_some_and(|items| {
        items.iter().any(|item| {
            item.as_str()
                .map(|tag| tag.trim() == "memory")
                .unwrap_or(false)
        })
    })
}

fn value_is_truthy(value: &Value) -> bool {
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

    fn has_error(diagnostics: &[EditorDiagnostic]) -> bool {
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
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
                "xprompts",
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
        let names: Vec<&str> =
            schema.iter().map(|input| input.name.as_str()).collect();
        assert_eq!(
            names,
            ["word", "agent", "line", "text", "path", "int", "float", "bool"]
        );
        for input in &schema {
            assert!(!input.rule.is_empty(), "{} has no rule", input.name);
            // Canonical name and every alias must parse back to a known type.
            assert!(parse_input_type(&input.name).is_some());
            for alias in &input.aliases {
                assert!(
                    parse_input_type(alias).is_some(),
                    "alias {alias} does not parse"
                );
            }
        }
        let int = schema.iter().find(|input| input.name == "int").unwrap();
        assert_eq!(int.aliases, vec!["integer".to_string()]);
        let bool_type =
            schema.iter().find(|input| input.name == "bool").unwrap();
        assert_eq!(bool_type.aliases, vec!["boolean".to_string()]);
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
            diagnostic.code == "non_final_xprompt_frontmatter_repeatable_input"
        }));

        let non_boolean = validate(
            "---\ninput:\n  names:\n    type: agent\n    repeatable: yes\n---\n",
        );
        assert!(non_boolean.iter().any(|diagnostic| {
            diagnostic.code == "invalid_xprompt_frontmatter_input_repeatable"
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
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_xprompt_frontmatter_input_type"
        }));
    }

    #[test]
    fn validate_accepts_log_skill_use_boolean() {
        let text = "---\nskill: true\ndescription: Plan helper\nlog_skill_use: false\n---\n";
        let diagnostics = validate(text);
        assert!(!has_error(&diagnostics), "{diagnostics:?}");
        assert!(
            !diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "unknown_xprompt_frontmatter_field"
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
                    diagnostic.code
                        == "invalid_xprompt_frontmatter_log_skill_use"
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
            diagnostic.code == "invalid_xprompt_frontmatter_snippet_trigger"
        }));
    }

    #[test]
    fn validate_field_handles_block_values() {
        // A multi-line value is indented as a YAML block under the field key.
        let diagnostics =
            validate_field("input", "service: wordd\nregion: word");
        assert!(diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "invalid_xprompt_frontmatter_input_type"
        }));
    }
}

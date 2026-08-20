//! Wire types, validators, and a stable digest for one task-type spec.

use std::collections::BTreeSet;

use regex::Regex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use unicode_width::UnicodeWidthStr;

use crate::artifact_ref::is_known_property_type;
use crate::bead::PhaseSizeWire;

use super::error::TaskTypeError;

pub const TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION: u64 = 1;
pub const TASK_TYPE_SLUG_MAX_LEN: usize = 32;
pub const TASK_TYPE_FIELD_NAME_MAX_LEN: usize = 64;
pub const TASK_TYPE_SUMMARY_MAX_CHARS: usize = 120;
pub const TASK_TYPE_WHEN_TO_USE_MAX_CHARS: usize = 400;
pub const TASK_TYPE_CREATE_REFUSAL_MAX_CHARS: usize = 400;

/// Reserved slugs: the three bead issue types (`plan`, `phase`, `task`) plus
/// the four filter sentinels (`untyped`, `unknown`, `all`, `none`).
pub const RESERVED_TASK_TYPE_SLUGS: &[&str] =
    &["plan", "phase", "task", "untyped", "unknown", "all", "none"];

/// Scalar subset of the artifact-ref `PROPERTY_TYPES` vocabulary.
pub const TASK_TYPE_FIELD_TYPES: &[&str] =
    &["string", "enum", "integer", "date"];

pub const TASK_TYPE_FIELD_ROLES: &[&str] = &["data", "template"];

const ACCENT_COLOR_PATTERN: &str = r"^#[0-9A-Fa-f]{6}$";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeSpecWire {
    pub schema_version: u64,
    pub task_type: String,
    pub label: String,
    pub summary: String,
    pub when_to_use: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_refusal: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glyph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accent_color: Option<String>,
    #[serde(default = "default_true")]
    pub agent_creatable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_size: Option<String>,
    #[serde(default)]
    pub fields: Vec<TaskTypeFieldSpecWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_template: Option<String>,
    #[serde(default)]
    pub triage: TaskTypeTriageWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeFieldSpecWire {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default = "default_field_roles")]
    pub role: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub help: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minimum: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeTriageWire {
    #[serde(default)]
    pub min_plus_ones: u64,
}

fn default_true() -> bool {
    true
}

fn default_field_roles() -> Vec<String> {
    vec!["data".to_string(), "template".to_string()]
}

/// Validate one assembled task-type spec.
pub fn validate_task_type_spec(
    spec: &TaskTypeSpecWire,
) -> Result<(), TaskTypeError> {
    if spec.schema_version != TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION {
        return Err(TaskTypeError::validation(format!(
            "unsupported task type spec schema_version {}; expected {}",
            spec.schema_version, TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION
        )));
    }
    validate_snake_case_slug(
        &spec.task_type,
        "task_type",
        TASK_TYPE_SLUG_MAX_LEN,
    )?;
    if RESERVED_TASK_TYPE_SLUGS.contains(&spec.task_type.as_str()) {
        return Err(TaskTypeError::validation(format!(
            "task_type '{}' is reserved and cannot be claimed by a spec",
            spec.task_type
        )));
    }
    validate_required_text(&spec.label, "label")?;
    validate_summary(&spec.summary)?;
    validate_when_to_use(&spec.when_to_use)?;
    if let Some(create_refusal) = &spec.create_refusal {
        validate_create_refusal(create_refusal)?;
    }
    if let Some(glyph) = &spec.glyph {
        validate_glyph(glyph)?;
    }
    if let Some(accent) = &spec.accent_color {
        validate_accent_color(accent)?;
    }
    if let Some(size) = &spec.default_size {
        if PhaseSizeWire::from_name(size).is_none() {
            return Err(TaskTypeError::validation(format!(
                "default_size must be one of {}; got '{size}'",
                PhaseSizeWire::NAMES.join(", ")
            )));
        }
    }

    let mut seen_names = BTreeSet::new();
    for field in &spec.fields {
        validate_field_spec(field)?;
        if !seen_names.insert(field.name.as_str()) {
            return Err(TaskTypeError::validation(format!(
                "duplicate field name '{}'",
                field.name
            )));
        }
    }

    validate_body_template(spec)?;
    Ok(())
}

/// Compute a stable sha256 hex digest over the normalized spec.
pub fn task_type_spec_digest(
    spec: &TaskTypeSpecWire,
) -> Result<String, TaskTypeError> {
    let normalized = serde_json::to_vec(spec).map_err(|error| {
        TaskTypeError::validation(format!(
            "unable to normalize task type spec for digest: {error}"
        ))
    })?;
    Ok(hex::encode(Sha256::digest(&normalized)))
}

pub(super) fn field_by_name<'a>(
    spec: &'a TaskTypeSpecWire,
    name: &str,
) -> Option<&'a TaskTypeFieldSpecWire> {
    spec.fields.iter().find(|field| field.name == name)
}

pub(super) fn field_has_role(
    field: &TaskTypeFieldSpecWire,
    role: &str,
) -> bool {
    field.role.iter().any(|entry| entry == role)
}

pub(super) fn validate_snake_case_slug(
    value: &str,
    what: &str,
    max_len: usize,
) -> Result<(), TaskTypeError> {
    if value.len() <= max_len && is_snake_case_key(value) {
        Ok(())
    } else {
        Err(TaskTypeError::validation(format!(
            "{what} must be a non-empty snake_case slug at most {max_len} characters: {value:?}"
        )))
    }
}

pub(super) fn is_snake_case_key(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let mut prev_underscore = false;
    for character in chars {
        if character == '_' {
            if prev_underscore {
                return false;
            }
            prev_underscore = true;
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            prev_underscore = false;
        } else {
            return false;
        }
    }
    !prev_underscore
}

pub(super) fn extract_body_placeholders(
    template: &str,
) -> Result<Vec<&str>, TaskTypeError> {
    let mut placeholders = Vec::new();
    let mut seen = BTreeSet::new();
    for token in tokenize_body_template(template)? {
        if let BodyToken::Placeholder(name) = token {
            if seen.insert(name) {
                placeholders.push(name);
            }
        }
    }
    Ok(placeholders)
}

pub(super) enum BodyToken<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

pub(super) fn tokenize_body_template(
    template: &str,
) -> Result<Vec<BodyToken<'_>>, TaskTypeError> {
    let bytes = template.as_bytes();
    let mut tokens = Vec::new();
    let mut literal_start = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        if bytes[index] == b'{' && bytes.get(index + 1) == Some(&b'{') {
            if index > literal_start {
                tokens
                    .push(BodyToken::Literal(&template[literal_start..index]));
            }
            let name_start = index + 2;
            let Some(relative_end) = template[name_start..].find("}}") else {
                return Err(TaskTypeError::validation(format!(
                    "unbalanced '{{{{' in body_template at byte {index}"
                )));
            };
            let name_end = name_start + relative_end;
            let name = template[name_start..name_end].trim();
            if !is_snake_case_key(name) {
                return Err(TaskTypeError::validation(format!(
                    "body_template placeholder must be a snake_case field name; got '{name}'"
                )));
            }
            tokens.push(BodyToken::Placeholder(name));
            index = name_end + 2;
            literal_start = index;
        } else {
            index += 1;
        }
    }
    if literal_start < template.len() {
        tokens.push(BodyToken::Literal(&template[literal_start..]));
    }
    Ok(tokens)
}

fn validate_field_spec(
    field: &TaskTypeFieldSpecWire,
) -> Result<(), TaskTypeError> {
    validate_snake_case_slug(
        &field.name,
        "field name",
        TASK_TYPE_FIELD_NAME_MAX_LEN,
    )?;
    if let Some(label) = &field.label {
        validate_required_text(
            label,
            &format!("field '{}' label", field.name),
        )?;
    }
    if let Some(help) = &field.help {
        validate_required_text(help, &format!("field '{}' help", field.name))?;
    }
    if !is_known_property_type(&field.field_type)
        || !TASK_TYPE_FIELD_TYPES.contains(&field.field_type.as_str())
    {
        return Err(TaskTypeError::validation(format!(
            "field '{}' has unsupported type '{}'; expected one of {}",
            field.name,
            field.field_type,
            TASK_TYPE_FIELD_TYPES.join(", ")
        )));
    }
    validate_field_roles(field)?;
    validate_field_validators(field)?;
    Ok(())
}

fn validate_field_roles(
    field: &TaskTypeFieldSpecWire,
) -> Result<(), TaskTypeError> {
    if field.role.is_empty() {
        return Err(TaskTypeError::validation(format!(
            "field '{}' role must be a non-empty subset of {{{}}}",
            field.name,
            TASK_TYPE_FIELD_ROLES.join(", ")
        )));
    }
    let mut seen = BTreeSet::new();
    for role in &field.role {
        if !TASK_TYPE_FIELD_ROLES.contains(&role.as_str()) {
            return Err(TaskTypeError::validation(format!(
                "field '{}' has unsupported role '{role}'; expected a subset of {{{}}}",
                field.name,
                TASK_TYPE_FIELD_ROLES.join(", ")
            )));
        }
        if !seen.insert(role.as_str()) {
            return Err(TaskTypeError::validation(format!(
                "field '{}' declares role '{role}' more than once",
                field.name
            )));
        }
    }
    Ok(())
}

fn validate_field_validators(
    field: &TaskTypeFieldSpecWire,
) -> Result<(), TaskTypeError> {
    let name = &field.name;
    let field_type = field.field_type.as_str();
    if field.pattern.is_some() && field_type != "string" {
        return Err(TaskTypeError::validation(format!(
            "field '{name}' of type '{field_type}' cannot declare pattern"
        )));
    }
    if field.max_length.is_some() && field_type != "string" {
        return Err(TaskTypeError::validation(format!(
            "field '{name}' of type '{field_type}' cannot declare max_length"
        )));
    }
    if !field.values.is_empty() && field_type != "enum" {
        return Err(TaskTypeError::validation(format!(
            "field '{name}' of type '{field_type}' cannot declare values"
        )));
    }
    if field.minimum.is_some() && field_type != "integer" {
        return Err(TaskTypeError::validation(format!(
            "field '{name}' of type '{field_type}' cannot declare minimum"
        )));
    }
    if field.maximum.is_some() && field_type != "integer" {
        return Err(TaskTypeError::validation(format!(
            "field '{name}' of type '{field_type}' cannot declare maximum"
        )));
    }

    match field_type {
        "string" => {
            if let Some(max_length) = field.max_length {
                if max_length == 0 {
                    return Err(TaskTypeError::validation(format!(
                        "field '{name}' max_length must be at least 1"
                    )));
                }
            }
            if let Some(pattern) = &field.pattern {
                Regex::new(pattern).map_err(|error| {
                    TaskTypeError::validation(format!(
                        "field '{name}' pattern is not a valid regex: {error}"
                    ))
                })?;
            }
        }
        "enum" => {
            if field.values.is_empty() {
                return Err(TaskTypeError::validation(format!(
                    "enum field '{name}' must declare at least one value"
                )));
            }
            let mut seen = BTreeSet::new();
            for value in &field.values {
                if value.is_empty() {
                    return Err(TaskTypeError::validation(format!(
                        "enum field '{name}' cannot include an empty value"
                    )));
                }
                if !seen.insert(value.as_str()) {
                    return Err(TaskTypeError::validation(format!(
                        "enum field '{name}' declares duplicate value '{value}'"
                    )));
                }
            }
        }
        "integer" => {
            if let (Some(minimum), Some(maximum)) =
                (field.minimum, field.maximum)
            {
                if minimum > maximum {
                    return Err(TaskTypeError::validation(format!(
                        "field '{name}' minimum {minimum} is greater than maximum {maximum}"
                    )));
                }
            }
        }
        "date" => {}
        _ => {}
    }
    Ok(())
}

fn validate_body_template(
    spec: &TaskTypeSpecWire,
) -> Result<(), TaskTypeError> {
    let Some(template) = spec.body_template.as_deref() else {
        return Ok(());
    };
    if template.is_empty() {
        return Ok(());
    }
    for name in extract_body_placeholders(template)? {
        let Some(field) = field_by_name(spec, name) else {
            return Err(TaskTypeError::validation(format!(
                "body_template placeholder '{{{{{name}}}}}' does not name a declared field"
            )));
        };
        if !field_has_role(field, "template") {
            return Err(TaskTypeError::validation(format!(
                "body_template placeholder '{{{{{name}}}}}' names a field without the template role"
            )));
        }
    }
    Ok(())
}

fn validate_required_text(
    value: &str,
    what: &str,
) -> Result<(), TaskTypeError> {
    if value.is_empty() {
        return Err(TaskTypeError::validation(format!(
            "{what} must not be empty"
        )));
    }
    if value.trim() != value {
        return Err(TaskTypeError::validation(format!(
            "{what} must not have leading or trailing whitespace"
        )));
    }
    Ok(())
}

fn validate_summary(summary: &str) -> Result<(), TaskTypeError> {
    validate_required_text(summary, "summary")?;
    if summary.contains('\n') || summary.contains('\r') {
        return Err(TaskTypeError::validation("summary must be a single line"));
    }
    let chars = summary.chars().count();
    if chars > TASK_TYPE_SUMMARY_MAX_CHARS {
        return Err(TaskTypeError::validation(format!(
            "summary must be at most {TASK_TYPE_SUMMARY_MAX_CHARS} characters, got {chars}"
        )));
    }
    Ok(())
}

fn validate_when_to_use(when_to_use: &str) -> Result<(), TaskTypeError> {
    validate_required_text(when_to_use, "when_to_use")?;
    let chars = when_to_use.chars().count();
    if chars > TASK_TYPE_WHEN_TO_USE_MAX_CHARS {
        return Err(TaskTypeError::validation(format!(
            "when_to_use must be at most {TASK_TYPE_WHEN_TO_USE_MAX_CHARS} characters, got {chars}"
        )));
    }
    Ok(())
}

fn validate_create_refusal(create_refusal: &str) -> Result<(), TaskTypeError> {
    validate_required_text(create_refusal, "create_refusal")?;
    let chars = create_refusal.chars().count();
    if chars > TASK_TYPE_CREATE_REFUSAL_MAX_CHARS {
        return Err(TaskTypeError::validation(format!(
            "create_refusal must be at most {TASK_TYPE_CREATE_REFUSAL_MAX_CHARS} characters, got {chars}"
        )));
    }
    Ok(())
}

fn validate_glyph(glyph: &str) -> Result<(), TaskTypeError> {
    if glyph.is_empty() {
        return Err(TaskTypeError::validation("glyph must not be empty"));
    }
    if glyph.trim() != glyph {
        return Err(TaskTypeError::validation(
            "glyph must not have leading or trailing whitespace",
        ));
    }
    if glyph.chars().any(char::is_control) {
        return Err(TaskTypeError::validation(
            "glyph must not contain control characters",
        ));
    }
    if glyph.chars().count() > 8 || glyph.len() > 32 {
        return Err(TaskTypeError::validation(
            "glyph must be at most 8 chars and 32 UTF-8 bytes",
        ));
    }
    let width = glyph.width();
    if width != 1 {
        return Err(TaskTypeError::validation(format!(
            "glyph must display as 1 cell wide, got {width}"
        )));
    }
    Ok(())
}

fn validate_accent_color(accent: &str) -> Result<(), TaskTypeError> {
    let valid = Regex::new(ACCENT_COLOR_PATTERN)
        .expect("accent color pattern is valid")
        .is_match(accent);
    if valid {
        Ok(())
    } else {
        Err(TaskTypeError::validation(format!(
            "accent_color must be #RRGGBB, got '{accent}'"
        )))
    }
}

#[cfg(test)]
pub(super) fn valid_spec() -> TaskTypeSpecWire {
    TaskTypeSpecWire {
        schema_version: TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION,
        task_type: "flake".to_string(),
        label: "Flaky test".to_string(),
        summary: "A test that fails and then passes on an unchanged tree."
            .to_string(),
        when_to_use: "File one when a test failed, a rerun on the same tree passed, and you did not cause the failure.".to_string(),
        create_refusal: None,
        glyph: Some("≈".to_string()),
        accent_color: Some("#00D7D7".to_string()),
        agent_creatable: true,
        default_size: None,
        fields: vec![
            TaskTypeFieldSpecWire {
                name: "node_id".to_string(),
                label: Some("Test node ID".to_string()),
                field_type: "string".to_string(),
                required: true,
                role: vec!["data".to_string(), "template".to_string()],
                help: Some(
                    "The pytest node ID, e.g. tests/foo.py::test_bar"
                        .to_string(),
                ),
                pattern: Some(r"\S+::\S+".to_string()),
                max_length: Some(256),
                values: Vec::new(),
                minimum: None,
                maximum: None,
            },
            TaskTypeFieldSpecWire {
                name: "evidence".to_string(),
                label: None,
                field_type: "string".to_string(),
                required: true,
                role: vec!["template".to_string()],
                help: None,
                pattern: None,
                max_length: None,
                values: Vec::new(),
                minimum: None,
                maximum: None,
            },
        ],
        body_template: Some(
            "## Flake report\n\n- **Test:** `{{ node_id }}`\n\n{{ evidence }}\n"
                .to_string(),
        ),
        triage: TaskTypeTriageWire { min_plus_ones: 1 },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_spec_passes_and_digest_is_stable() {
        let spec = valid_spec();
        validate_task_type_spec(&spec).unwrap();
        let first = task_type_spec_digest(&spec).unwrap();
        let second = task_type_spec_digest(&spec).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 64);

        let mut changed = spec.clone();
        changed.task_type = "flaky_test".to_string();
        let third = task_type_spec_digest(&changed).unwrap();
        assert_ne!(first, third);
    }

    #[test]
    fn digest_is_stable_across_omitted_and_explicit_defaults() {
        let explicit = valid_spec();
        let mut omitted = serde_json::to_value(&explicit).unwrap();
        omitted.as_object_mut().unwrap().remove("agent_creatable");
        omitted.as_object_mut().unwrap().remove("create_refusal");
        let decoded: TaskTypeSpecWire =
            serde_json::from_value(omitted).unwrap();
        assert!(decoded.agent_creatable);
        assert_eq!(decoded.create_refusal, None);
        assert_eq!(
            task_type_spec_digest(&explicit).unwrap(),
            task_type_spec_digest(&decoded).unwrap()
        );
    }

    #[test]
    fn omitted_create_refusal_does_not_change_digest() {
        let without = valid_spec();
        let encoded = serde_json::to_value(&without).unwrap();
        assert!(!encoded.as_object().unwrap().contains_key("create_refusal"));
        let decoded: TaskTypeSpecWire =
            serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.create_refusal, None);
        assert_eq!(
            task_type_spec_digest(&without).unwrap(),
            task_type_spec_digest(&decoded).unwrap()
        );
    }

    #[test]
    fn create_refusal_changes_digest_and_rejects_empty_or_overlong() {
        let mut with_refusal = valid_spec();
        with_refusal.create_refusal =
            Some("Agents never create this type.".to_string());
        assert_ne!(
            task_type_spec_digest(&valid_spec()).unwrap(),
            task_type_spec_digest(&with_refusal).unwrap()
        );
        validate_task_type_spec(&with_refusal).unwrap();

        let mut empty = valid_spec();
        empty.create_refusal = Some(String::new());
        assert!(validate_task_type_spec(&empty)
            .unwrap_err()
            .message
            .contains("create_refusal"));

        let mut padded = valid_spec();
        padded.create_refusal =
            Some(" Agents never create this type. ".to_string());
        assert!(validate_task_type_spec(&padded)
            .unwrap_err()
            .message
            .contains("create_refusal"));

        let mut overlong = valid_spec();
        overlong.create_refusal =
            Some("y".repeat(TASK_TYPE_CREATE_REFUSAL_MAX_CHARS + 1));
        assert!(validate_task_type_spec(&overlong)
            .unwrap_err()
            .message
            .contains("at most 400"));
    }

    #[test]
    fn rejects_unsupported_schema_version() {
        let mut spec = valid_spec();
        spec.schema_version = 99;
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("schema_version"));
    }

    #[test]
    fn reserved_slugs_are_the_three_issue_types_and_four_filter_sentinels() {
        assert_eq!(
            RESERVED_TASK_TYPE_SLUGS,
            &["plan", "phase", "task", "untyped", "unknown", "all", "none"]
        );
    }

    #[test]
    fn accepts_flag_as_a_claimable_task_type_slug() {
        let mut spec = valid_spec();
        spec.task_type = "flag".to_string();
        validate_task_type_spec(&spec).unwrap();
    }

    #[test]
    fn rejects_reserved_and_malformed_slugs() {
        for slug in RESERVED_TASK_TYPE_SLUGS {
            let mut spec = valid_spec();
            spec.task_type = (*slug).to_string();
            let error = validate_task_type_spec(&spec).unwrap_err();
            assert!(
                error.message.contains("reserved"),
                "slug {slug} should be reserved: {error}"
            );
        }

        let mut spec = valid_spec();
        spec.task_type = "Not_Snake".to_string();
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("snake_case"));

        let mut spec = valid_spec();
        spec.task_type = "a".repeat(TASK_TYPE_SLUG_MAX_LEN + 1);
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("at most"));
    }

    #[test]
    fn rejects_missing_label_summary_and_when_to_use_caps() {
        let mut spec = valid_spec();
        spec.label = String::new();
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("label"));

        let mut spec = valid_spec();
        spec.summary = "line one\nline two".to_string();
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("single line"));

        let mut spec = valid_spec();
        spec.summary = "x".repeat(TASK_TYPE_SUMMARY_MAX_CHARS + 1);
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("at most 120"));

        let mut spec = valid_spec();
        spec.when_to_use = "y".repeat(TASK_TYPE_WHEN_TO_USE_MAX_CHARS + 1);
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("at most 400"));
    }

    #[test]
    fn rejects_malformed_glyph_and_accent() {
        let mut spec = valid_spec();
        spec.glyph = Some(String::new());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("glyph"));

        let mut spec = valid_spec();
        spec.glyph = Some("🔥".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("1 cell"));

        let mut spec = valid_spec();
        spec.accent_color = Some("#00D7D".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("#RRGGBB"));

        let mut spec = valid_spec();
        spec.accent_color = Some("00D7D7".to_string());
        assert!(validate_task_type_spec(&spec).is_err());
    }

    #[test]
    fn accepts_single_cell_glyph_and_hex_accent() {
        let mut spec = valid_spec();
        spec.glyph = Some("⨯".to_string());
        spec.accent_color = Some("#ff5f5f".to_string());
        validate_task_type_spec(&spec).unwrap();
    }

    #[test]
    fn rejects_unknown_default_size() {
        let mut spec = valid_spec();
        spec.default_size = Some("huge".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("default_size"));
    }

    #[test]
    fn rejects_duplicate_and_bad_field_names() {
        let mut spec = valid_spec();
        spec.fields.push(spec.fields[0].clone());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("duplicate field name"));

        let mut spec = valid_spec();
        spec.fields[0].name = "NodeId".to_string();
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("snake_case"));
    }

    #[test]
    fn rejects_unsupported_field_types_outside_scalar_subset() {
        for field_type in
            ["boolean", "number", "datetime", "string_list", "object"]
        {
            let mut spec = valid_spec();
            spec.fields[0].field_type = field_type.to_string();
            spec.fields[0].pattern = None;
            spec.fields[0].max_length = None;
            assert!(
                validate_task_type_spec(&spec).is_err(),
                "{field_type} should be rejected"
            );
        }
    }

    #[test]
    fn rejects_validator_keys_on_the_wrong_field_type() {
        let mut spec = valid_spec();
        spec.fields.push(TaskTypeFieldSpecWire {
            name: "retries".to_string(),
            label: None,
            field_type: "integer".to_string(),
            required: false,
            role: vec!["data".to_string()],
            help: None,
            pattern: Some(r"\d+".to_string()),
            max_length: None,
            values: Vec::new(),
            minimum: Some(0),
            maximum: Some(10),
        });
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("cannot declare pattern"));

        let mut spec = valid_spec();
        spec.fields[0].values = vec!["a".to_string()];
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("cannot declare values"));
    }

    #[test]
    fn rejects_empty_enum_values_and_invalid_regex() {
        let mut spec = valid_spec();
        spec.fields.push(TaskTypeFieldSpecWire {
            name: "severity".to_string(),
            label: None,
            field_type: "enum".to_string(),
            required: true,
            role: vec!["data".to_string()],
            help: None,
            pattern: None,
            max_length: None,
            values: Vec::new(),
            minimum: None,
            maximum: None,
        });
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("at least one value"));

        let mut spec = valid_spec();
        spec.fields[0].pattern = Some("(".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("not a valid regex"));
    }

    #[test]
    fn rejects_empty_or_unknown_roles() {
        let mut spec = valid_spec();
        spec.fields[0].role = Vec::new();
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("non-empty subset"));

        let mut spec = valid_spec();
        spec.fields[0].role = vec!["query".to_string()];
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("unsupported role"));
    }

    #[test]
    fn rejects_template_placeholders_that_are_undeclared_or_data_only() {
        let mut spec = valid_spec();
        spec.body_template = Some("{{ missing }}".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("does not name a declared field"));

        let mut spec = valid_spec();
        spec.fields.push(TaskTypeFieldSpecWire {
            name: "retries".to_string(),
            label: None,
            field_type: "integer".to_string(),
            required: false,
            role: vec!["data".to_string()],
            help: None,
            pattern: None,
            max_length: None,
            values: Vec::new(),
            minimum: None,
            maximum: None,
        });
        spec.body_template = Some("{{ retries }}".to_string());
        assert!(validate_task_type_spec(&spec)
            .unwrap_err()
            .message
            .contains("without the template role"));
    }

    #[test]
    fn accepts_spec_with_no_body_template() {
        let mut spec = valid_spec();
        spec.body_template = None;
        spec.fields.retain(|field| field.name != "evidence");
        validate_task_type_spec(&spec).unwrap();
    }

    #[test]
    fn field_types_are_the_scalar_subset_of_property_types() {
        for field_type in TASK_TYPE_FIELD_TYPES {
            assert!(
                is_known_property_type(field_type),
                "{field_type} must stay in PROPERTY_TYPES"
            );
        }
    }
}

//! Per-field value validation against an assembled task-type spec.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::NaiveDate;
use regex::Regex;

use super::error::{TaskTypeError, TaskTypeFieldValueError};
use super::spec::{
    field_by_name, validate_task_type_spec, TaskTypeFieldSpecWire,
    TaskTypeSpecWire,
};

/// Validate `values` against `spec`.
///
/// Returns one typed error per problem: missing required fields, unknown
/// field names, and per-type validator failures. An invalid spec is a
/// hard error rather than a field-value diagnostic.
pub fn validate_task_type_field_values(
    spec: &TaskTypeSpecWire,
    values: &BTreeMap<String, String>,
) -> Result<Vec<TaskTypeFieldValueError>, TaskTypeError> {
    validate_task_type_spec(spec)?;
    Ok(collect_field_value_errors(spec, values))
}

fn collect_field_value_errors(
    spec: &TaskTypeSpecWire,
    values: &BTreeMap<String, String>,
) -> Vec<TaskTypeFieldValueError> {
    let mut errors = Vec::new();
    for name in values.keys() {
        if field_by_name(spec, name).is_none() {
            errors.push(TaskTypeFieldValueError::new(
                "unknown_field",
                name,
                format!("unknown field '{name}'"),
            ));
        }
    }
    for field in &spec.fields {
        let provided = values.get(&field.name);
        if field.required && value_missing(provided) {
            errors.push(TaskTypeFieldValueError::new(
                "missing_required",
                &field.name,
                format!("required field '{}' is missing", field.name),
            ));
            continue;
        }
        if let Some(value) = provided {
            if !value.is_empty() {
                errors.extend(validate_one_field(field, value));
            }
        }
    }
    errors
}

fn value_missing(value: Option<&String>) -> bool {
    match value {
        None => true,
        Some(entry) => entry.is_empty(),
    }
}

fn validate_one_field(
    field: &TaskTypeFieldSpecWire,
    value: &str,
) -> Vec<TaskTypeFieldValueError> {
    match field.field_type.as_str() {
        "string" => validate_string_field(field, value),
        "enum" => validate_enum_field(field, value),
        "integer" => validate_integer_field(field, value),
        "date" => validate_date_field(field, value),
        _ => Vec::new(),
    }
}

fn validate_string_field(
    field: &TaskTypeFieldSpecWire,
    value: &str,
) -> Vec<TaskTypeFieldValueError> {
    let mut errors = Vec::new();
    if let Some(max_length) = field.max_length {
        let chars = value.chars().count() as u64;
        if chars > max_length {
            errors.push(TaskTypeFieldValueError::new(
                "invalid_string_length",
                &field.name,
                format!(
                    "field '{}' must be at most {max_length} characters, got {chars}",
                    field.name
                ),
            ));
        }
    }
    if let Some(pattern) = &field.pattern {
        match Regex::new(pattern) {
            Ok(regex) if !regex.is_match(value) => {
                errors.push(TaskTypeFieldValueError::new(
                    "invalid_string_pattern",
                    &field.name,
                    format!(
                        "field '{}' does not match pattern {pattern}",
                        field.name
                    ),
                ));
            }
            Ok(_) => {}
            Err(_) => {
                errors.push(TaskTypeFieldValueError::new(
                    "invalid_string_pattern",
                    &field.name,
                    format!(
                        "field '{}' pattern is not a valid regex",
                        field.name
                    ),
                ));
            }
        }
    }
    errors
}

fn validate_enum_field(
    field: &TaskTypeFieldSpecWire,
    value: &str,
) -> Vec<TaskTypeFieldValueError> {
    if field.values.iter().any(|allowed| allowed == value) {
        Vec::new()
    } else {
        vec![TaskTypeFieldValueError::new(
            "invalid_enum",
            &field.name,
            format!(
                "field '{}' must be one of {}; got '{value}'",
                field.name,
                field
                    .values
                    .iter()
                    .map(|entry| format!("'{entry}'"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )]
    }
}

fn validate_integer_field(
    field: &TaskTypeFieldSpecWire,
    value: &str,
) -> Vec<TaskTypeFieldValueError> {
    let parsed = i64::from_str(value).ok().filter(|_| {
        !value.is_empty()
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte == b'-')
            && !value.ends_with('-')
            && value != "-"
            && !(value.starts_with('-')
                && value.len() > 1
                && value.as_bytes()[1] == b'0')
            && !(value.starts_with('0') && value.len() > 1)
    });
    let Some(number) = parsed else {
        return vec![TaskTypeFieldValueError::new(
            "invalid_integer",
            &field.name,
            format!("field '{}' must be an integer, got '{value}'", field.name),
        )];
    };
    let mut errors = Vec::new();
    if let Some(minimum) = field.minimum {
        if number < minimum {
            errors.push(TaskTypeFieldValueError::new(
                "invalid_integer_range",
                &field.name,
                format!(
                    "field '{}' must be at least {minimum}, got {number}",
                    field.name
                ),
            ));
        }
    }
    if let Some(maximum) = field.maximum {
        if number > maximum {
            errors.push(TaskTypeFieldValueError::new(
                "invalid_integer_range",
                &field.name,
                format!(
                    "field '{}' must be at most {maximum}, got {number}",
                    field.name
                ),
            ));
        }
    }
    errors
}

fn validate_date_field(
    field: &TaskTypeFieldSpecWire,
    value: &str,
) -> Vec<TaskTypeFieldValueError> {
    let valid = value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value.bytes().enumerate().all(|(index, byte)| {
            matches!(index, 4 | 7) || byte.is_ascii_digit()
        })
        && NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok();
    if valid {
        Vec::new()
    } else {
        vec![TaskTypeFieldValueError::new(
            "invalid_date",
            &field.name,
            format!(
                "field '{}' must be an ISO date YYYY-MM-DD, got '{value}'",
                field.name
            ),
        )]
    }
}

#[cfg(test)]
mod tests {
    use super::super::spec::{
        valid_spec, TaskTypeFieldSpecWire, TaskTypeSpecWire,
    };
    use super::*;

    fn values(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn kinds(errors: &[TaskTypeFieldValueError]) -> Vec<(&str, &str)> {
        errors
            .iter()
            .map(|error| (error.kind.as_str(), error.field.as_str()))
            .collect()
    }

    #[test]
    fn valid_values_return_no_errors() {
        let spec = valid_spec();
        let errors = validate_task_type_field_values(
            &spec,
            &values(&[
                ("node_id", "tests/foo.py::test_bar"),
                ("evidence", "failed then passed"),
            ]),
        )
        .unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn reports_missing_unknown_and_invalid_together() {
        let spec = valid_spec();
        let errors = validate_task_type_field_values(
            &spec,
            &values(&[("node_id", "not-a-node"), ("extra", "nope")]),
        )
        .unwrap();
        assert_eq!(
            kinds(&errors),
            [
                ("unknown_field", "extra"),
                ("invalid_string_pattern", "node_id"),
                ("missing_required", "evidence"),
            ]
        );
    }

    #[test]
    fn empty_required_string_is_missing() {
        let spec = valid_spec();
        let errors = validate_task_type_field_values(
            &spec,
            &values(&[("node_id", ""), ("evidence", "ok")]),
        )
        .unwrap();
        assert_eq!(kinds(&errors), [("missing_required", "node_id")]);
    }

    #[test]
    fn validates_enum_integer_and_date() {
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
            values: vec!["low".to_string(), "high".to_string()],
            minimum: None,
            maximum: None,
        });
        spec.fields.push(TaskTypeFieldSpecWire {
            name: "retries".to_string(),
            label: None,
            field_type: "integer".to_string(),
            required: true,
            role: vec!["data".to_string()],
            help: None,
            pattern: None,
            max_length: None,
            values: Vec::new(),
            minimum: Some(0),
            maximum: Some(5),
        });
        spec.fields.push(TaskTypeFieldSpecWire {
            name: "seen_on".to_string(),
            label: None,
            field_type: "date".to_string(),
            required: true,
            role: vec!["data".to_string()],
            help: None,
            pattern: None,
            max_length: None,
            values: Vec::new(),
            minimum: None,
            maximum: None,
        });
        spec.body_template = None;

        let errors = validate_task_type_field_values(
            &spec,
            &values(&[
                ("node_id", "tests/foo.py::test_bar"),
                ("evidence", "ok"),
                ("severity", "medium"),
                ("retries", "9"),
                ("seen_on", "2026-13-01"),
            ]),
        )
        .unwrap();
        assert_eq!(
            kinds(&errors),
            [
                ("invalid_enum", "severity"),
                ("invalid_integer_range", "retries"),
                ("invalid_date", "seen_on"),
            ]
        );

        let errors = validate_task_type_field_values(
            &spec,
            &values(&[
                ("node_id", "tests/foo.py::test_bar"),
                ("evidence", "ok"),
                ("severity", "high"),
                ("retries", "2"),
                ("seen_on", "2026-08-17"),
            ]),
        )
        .unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn rejects_padded_or_non_decimal_integers_and_short_dates() {
        let spec = TaskTypeSpecWire {
            fields: vec![
                TaskTypeFieldSpecWire {
                    name: "retries".to_string(),
                    label: None,
                    field_type: "integer".to_string(),
                    required: true,
                    role: vec!["data".to_string()],
                    help: None,
                    pattern: None,
                    max_length: None,
                    values: Vec::new(),
                    minimum: None,
                    maximum: None,
                },
                TaskTypeFieldSpecWire {
                    name: "seen_on".to_string(),
                    label: None,
                    field_type: "date".to_string(),
                    required: true,
                    role: vec!["data".to_string()],
                    help: None,
                    pattern: None,
                    max_length: None,
                    values: Vec::new(),
                    minimum: None,
                    maximum: None,
                },
            ],
            body_template: None,
            ..valid_spec()
        };
        let errors = validate_task_type_field_values(
            &spec,
            &values(&[("retries", "01"), ("seen_on", "2026-8-17")]),
        )
        .unwrap();
        assert_eq!(
            kinds(&errors),
            [("invalid_integer", "retries"), ("invalid_date", "seen_on"),]
        );
    }

    #[test]
    fn reports_string_max_length_and_allows_optional_fields_to_be_absent() {
        let mut spec = valid_spec();
        spec.fields[0].required = false;
        spec.fields[0].max_length = Some(4);
        spec.fields[1].required = false;
        let errors = validate_task_type_field_values(
            &spec,
            &values(&[("node_id", "too-long::id")]),
        )
        .unwrap();
        assert_eq!(kinds(&errors), [("invalid_string_length", "node_id")]);

        let errors =
            validate_task_type_field_values(&spec, &BTreeMap::new()).unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn invalid_spec_is_a_hard_error() {
        let mut spec = valid_spec();
        spec.task_type = "task".to_string();
        let error = validate_task_type_field_values(&spec, &BTreeMap::new())
            .unwrap_err();
        assert!(error.message.contains("reserved"));
    }
}

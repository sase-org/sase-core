//! Focused JSON Schema (draft-07 subset) validator for candidate configs.
//!
//! Supports the keywords the SASE config schema actually uses: `$ref`
//! (local `#/definitions/...`), `type` (scalar or array form), `enum`,
//! `properties`, `required`, `additionalProperties` (bool or schema),
//! `items`, `oneOf`, `anyOf`, the numeric bounds
//! (`minimum`/`maximum`/`exclusiveMinimum`/`exclusiveMaximum`), and the string
//! constraints (`minLength`/`maxLength`/`pattern`). Diagnostics carry a dotted
//! instance path; deeper branch errors inside `oneOf`/`anyOf` are summarized
//! at the branch point rather than flattened, to avoid noise.

use regex::Regex;
use serde_json::Value;

use super::schema::resolve_ref;
use super::wire::ConfigDiagnosticWire;

/// Validate `config` against `schema`, returning all diagnostics.
pub fn validate_config(
    schema: &Value,
    config: &Value,
) -> Vec<ConfigDiagnosticWire> {
    validate_against(schema, schema, config, "")
}

fn error(
    code: &str,
    path: &str,
    message: impl Into<String>,
) -> ConfigDiagnosticWire {
    ConfigDiagnosticWire {
        severity: "error".to_string(),
        code: code.to_string(),
        message: message.into(),
        path: (!path.is_empty()).then(|| path.to_string()),
        layer: None,
    }
}

fn child_path(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_string()
    } else {
        format!("{path}.{key}")
    }
}

fn validate_against(
    root: &Value,
    schema: &Value,
    instance: &Value,
    path: &str,
) -> Vec<ConfigDiagnosticWire> {
    let schema = resolve_ref(root, schema);
    let Some(obj) = schema.as_object() else {
        // `true`/empty schema accepts anything; non-object schema is a no-op.
        return Vec::new();
    };
    let mut out = Vec::new();

    if let Some(type_value) = obj.get("type") {
        if !type_matches(type_value, instance) {
            out.push(error(
                "type_mismatch",
                path,
                format!(
                    "expected type {}, found {}",
                    describe_type(type_value),
                    json_type_name(instance)
                ),
            ));
            // Type is wrong; deeper keyword checks would be noise.
            return out;
        }
    }

    if let Some(Value::Array(values)) = obj.get("enum") {
        if !values.iter().any(|candidate| candidate == instance) {
            out.push(error(
                "enum_mismatch",
                path,
                "value is not one of the allowed values",
            ));
        }
    }

    check_numeric(obj, instance, path, &mut out);
    check_string(obj, instance, path, &mut out);
    check_object(root, obj, instance, path, &mut out);
    check_array(root, obj, instance, path, &mut out);
    check_combinators(root, obj, instance, path, &mut out);

    out
}

fn check_combinators(
    root: &Value,
    obj: &serde_json::Map<String, Value>,
    instance: &Value,
    path: &str,
    out: &mut Vec<ConfigDiagnosticWire>,
) {
    if let Some(Value::Array(branches)) = obj.get("oneOf") {
        let matches = branches
            .iter()
            .filter(|branch| {
                validate_against(root, branch, instance, path).is_empty()
            })
            .count();
        if matches != 1 {
            out.push(error(
                "one_of_mismatch",
                path,
                format!(
                    "value must match exactly one schema (matched {matches})"
                ),
            ));
        }
    }
    if let Some(Value::Array(branches)) = obj.get("anyOf") {
        let matched = branches.iter().any(|branch| {
            validate_against(root, branch, instance, path).is_empty()
        });
        if !matched {
            out.push(error(
                "any_of_mismatch",
                path,
                "value does not match any allowed schema",
            ));
        }
    }
}

fn check_object(
    root: &Value,
    obj: &serde_json::Map<String, Value>,
    instance: &Value,
    path: &str,
    out: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(map) = instance.as_object() else {
        return;
    };
    if let Some(Value::Array(required)) = obj.get("required") {
        for entry in required {
            if let Some(key) = entry.as_str() {
                if !map.contains_key(key) {
                    out.push(error(
                        "required_missing",
                        &child_path(path, key),
                        format!("required property `{key}` is missing"),
                    ));
                }
            }
        }
    }
    let properties = obj.get("properties").and_then(Value::as_object);
    if let Some(props) = properties {
        for (key, sub_schema) in props {
            if let Some(value) = map.get(key) {
                out.extend(validate_against(
                    root,
                    sub_schema,
                    value,
                    &child_path(path, key),
                ));
            }
        }
    }
    match obj.get("additionalProperties") {
        Some(Value::Bool(false)) => {
            for key in map.keys() {
                let known =
                    properties.is_some_and(|props| props.contains_key(key));
                if !known {
                    out.push(error(
                        "additional_property",
                        &child_path(path, key),
                        format!("property `{key}` is not allowed"),
                    ));
                }
            }
        }
        Some(additional @ Value::Object(_)) => {
            for (key, value) in map {
                let known =
                    properties.is_some_and(|props| props.contains_key(key));
                if !known {
                    out.extend(validate_against(
                        root,
                        additional,
                        value,
                        &child_path(path, key),
                    ));
                }
            }
        }
        _ => {}
    }
}

fn check_array(
    root: &Value,
    obj: &serde_json::Map<String, Value>,
    instance: &Value,
    path: &str,
    out: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(items_schema) = obj.get("items") else {
        return;
    };
    let Some(array) = instance.as_array() else {
        return;
    };
    for (idx, item) in array.iter().enumerate() {
        out.extend(validate_against(
            root,
            items_schema,
            item,
            &format!("{path}[{idx}]"),
        ));
    }
}

fn check_numeric(
    obj: &serde_json::Map<String, Value>,
    instance: &Value,
    path: &str,
    out: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(number) = instance.as_f64() else {
        return;
    };
    if let Some(minimum) = obj.get("minimum").and_then(Value::as_f64) {
        if number < minimum {
            out.push(error(
                "minimum",
                path,
                format!("value {number} is below minimum {minimum}"),
            ));
        }
    }
    if let Some(maximum) = obj.get("maximum").and_then(Value::as_f64) {
        if number > maximum {
            out.push(error(
                "maximum",
                path,
                format!("value {number} is above maximum {maximum}"),
            ));
        }
    }
    if let Some(bound) = obj.get("exclusiveMinimum").and_then(Value::as_f64) {
        if number <= bound {
            out.push(error(
                "exclusive_minimum",
                path,
                format!("value {number} must be greater than {bound}"),
            ));
        }
    }
    if let Some(bound) = obj.get("exclusiveMaximum").and_then(Value::as_f64) {
        if number >= bound {
            out.push(error(
                "exclusive_maximum",
                path,
                format!("value {number} must be less than {bound}"),
            ));
        }
    }
}

fn check_string(
    obj: &serde_json::Map<String, Value>,
    instance: &Value,
    path: &str,
    out: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(text) = instance.as_str() else {
        return;
    };
    let length = text.chars().count() as u64;
    if let Some(min_length) = obj.get("minLength").and_then(Value::as_u64) {
        if length < min_length {
            out.push(error(
                "min_length",
                path,
                format!("string is shorter than minLength {min_length}"),
            ));
        }
    }
    if let Some(max_length) = obj.get("maxLength").and_then(Value::as_u64) {
        if length > max_length {
            out.push(error(
                "max_length",
                path,
                format!("string is longer than maxLength {max_length}"),
            ));
        }
    }
    if let Some(pattern) = obj.get("pattern").and_then(Value::as_str) {
        // Unparseable schema patterns are ignored rather than reported.
        if let Ok(regex) = Regex::new(pattern) {
            if !regex.is_match(text) {
                out.push(error(
                    "pattern",
                    path,
                    format!("string does not match pattern `{pattern}`"),
                ));
            }
        }
    }
}

fn type_matches(type_value: &Value, instance: &Value) -> bool {
    match type_value {
        Value::String(name) => instance_is_type(name, instance),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .any(|name| instance_is_type(name, instance)),
        _ => true,
    }
}

fn instance_is_type(name: &str, instance: &Value) -> bool {
    match name {
        "object" => instance.is_object(),
        "array" => instance.is_array(),
        "string" => instance.is_string(),
        "boolean" => instance.is_boolean(),
        "null" => instance.is_null(),
        "number" => instance.is_number(),
        // JSON Schema integers permit a float with a zero fraction.
        "integer" => {
            instance.is_i64()
                || instance.is_u64()
                || instance.as_f64().is_some_and(|value| value.fract() == 0.0)
        }
        _ => true,
    }
}

fn json_type_name(instance: &Value) -> &'static str {
    match instance {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn describe_type(type_value: &Value) -> String {
    match type_value {
        Value::String(name) => name.clone(),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" or "),
        _ => "any".to_string(),
    }
}

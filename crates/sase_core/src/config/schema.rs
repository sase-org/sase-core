//! JSON Schema → flattened field model.
//!
//! Turns the draft-07 `config/sase.schema.json` document into an ordered list
//! of dotted-path fields with type, description, default, enum, constraints,
//! and nesting info. Closed objects (with named `properties`) become
//! containers and are recursed into; open objects (`additionalProperties` is a
//! schema) become `"map"` leaves whose user-defined keys are not enumerated.

use serde_json::{Map, Value};

use super::wire::{
    ConfigConstraintsWire, ConfigError, ConfigFieldModelWire, ConfigFieldWire,
    CONFIG_WIRE_SCHEMA_VERSION,
};

/// Flatten a schema document into the ordered field model.
pub fn build_field_model(
    schema: &Value,
) -> Result<ConfigFieldModelWire, ConfigError> {
    let root = schema
        .as_object()
        .ok_or_else(|| ConfigError::schema("schema must be a JSON object"))?;
    let mut fields = Vec::new();
    if let Some(Value::Object(props)) = root.get("properties") {
        flatten(schema, props, "", 0, &mut fields);
    }
    Ok(ConfigFieldModelWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        fields,
    })
}

fn flatten(
    root: &Value,
    props: &Map<String, Value>,
    parent: &str,
    depth: usize,
    out: &mut Vec<ConfigFieldWire>,
) {
    for (key, raw) in props {
        let resolved = resolve_ref(root, raw);
        let obj = resolved.as_object().cloned().unwrap_or_default();
        let path = if parent.is_empty() {
            key.clone()
        } else {
            format!("{parent}.{key}")
        };
        let types = collect_types(root, &obj);
        let has_props =
            obj.get("properties").and_then(Value::as_object).is_some();
        let kind = classify(&types, has_props);
        let leaf = kind != "object";
        let (has_default, default) = match obj.get("default") {
            Some(value) => (true, value.clone()),
            None => (false, Value::Null),
        };
        let additional_properties_allowed = !matches!(
            obj.get("additionalProperties"),
            Some(Value::Bool(false))
        );
        out.push(ConfigFieldWire {
            path: path.clone(),
            name: key.clone(),
            depth,
            parent: (!parent.is_empty()).then(|| parent.to_string()),
            kind: kind.clone(),
            leaf,
            types,
            description: obj
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            has_default,
            default,
            enum_values: collect_enum(root, &obj),
            deprecated: obj
                .get("deprecated")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            deprecated_replacement: obj
                .get("deprecated_replacement")
                .and_then(Value::as_str)
                .map(str::to_string),
            constraints: collect_constraints(&obj),
            additional_properties_allowed,
        });
        if kind == "object" {
            if let Some(Value::Object(child)) = obj.get("properties") {
                flatten(root, child, &path, depth + 1, out);
            }
        }
    }
}

/// Classify a field. Order matters: a node with named `properties` is always a
/// closed object container; otherwise an object-typed node is an open `"map"`,
/// an array-typed node is `"array"`, and everything else is a `"scalar"`
/// (including scalar unions like boolean-or-string).
fn classify(types: &[String], has_props: bool) -> String {
    if has_props {
        return "object".to_string();
    }
    if types.iter().any(|t| t == "object") {
        return "map".to_string();
    }
    if types.iter().any(|t| t == "array") {
        return "array".to_string();
    }
    "scalar".to_string()
}

/// Collect declared JSON types, descending into `oneOf`/`anyOf` branches so
/// unions like `["boolean", "string"]` surface every member type.
fn collect_types(root: &Value, obj: &Map<String, Value>) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let push = |value: &str, out: &mut Vec<String>| {
        if !out.iter().any(|t| t == value) {
            out.push(value.to_string());
        }
    };
    match obj.get("type") {
        Some(Value::String(s)) => push(s, &mut out),
        Some(Value::Array(arr)) => {
            for value in arr {
                if let Some(s) = value.as_str() {
                    push(s, &mut out);
                }
            }
        }
        _ => {}
    }
    for branch_key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(branches)) = obj.get(branch_key) {
            for branch in branches {
                let resolved = resolve_ref(root, branch);
                if let Some(branch_obj) = resolved.as_object() {
                    for t in collect_types(root, branch_obj) {
                        push(&t, &mut out);
                    }
                }
            }
        }
    }
    out
}

/// Collect enum values, including those declared inside `oneOf`/`anyOf`
/// branches (e.g. the string-enum half of a boolean-or-enum union).
fn collect_enum(root: &Value, obj: &Map<String, Value>) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    if let Some(Value::Array(values)) = obj.get("enum") {
        for value in values {
            if !out.contains(value) {
                out.push(value.clone());
            }
        }
    }
    for branch_key in ["oneOf", "anyOf"] {
        if let Some(Value::Array(branches)) = obj.get(branch_key) {
            for branch in branches {
                let resolved = resolve_ref(root, branch);
                if let Some(branch_obj) = resolved.as_object() {
                    for value in collect_enum(root, branch_obj) {
                        if !out.contains(&value) {
                            out.push(value);
                        }
                    }
                }
            }
        }
    }
    out
}

fn collect_constraints(obj: &Map<String, Value>) -> ConfigConstraintsWire {
    let num = |key: &str| obj.get(key).and_then(Value::as_f64);
    let uint = |key: &str| obj.get(key).and_then(Value::as_u64);
    ConfigConstraintsWire {
        minimum: num("minimum"),
        maximum: num("maximum"),
        exclusive_minimum: num("exclusiveMinimum"),
        exclusive_maximum: num("exclusiveMaximum"),
        min_length: uint("minLength"),
        max_length: uint("maxLength"),
        pattern: obj
            .get("pattern")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// Resolve a local `$ref` (`#/definitions/...` or `#`) against the schema
/// root, following chained refs. Non-local or unresolvable refs return the
/// node unchanged.
pub fn resolve_ref<'a>(root: &'a Value, schema: &'a Value) -> &'a Value {
    if let Some(Value::String(reference)) =
        schema.as_object().and_then(|obj| obj.get("$ref"))
    {
        if let Some(target) = resolve_pointer(root, reference) {
            return resolve_ref(root, target);
        }
    }
    schema
}

fn resolve_pointer<'a>(root: &'a Value, reference: &str) -> Option<&'a Value> {
    let pointer = reference.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root);
    }
    let mut cur = root;
    for raw in pointer.trim_start_matches('/').split('/') {
        let part = raw.replace("~1", "/").replace("~0", "~");
        cur = cur.as_object()?.get(&part)?;
    }
    Some(cur)
}

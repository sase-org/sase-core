//! Enum choice parsing and value-rule checks.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

const ALLOWED_KEYS: [&str; 3] = ["value", "label", "description"];
const SHORTHAND_HOSTILE: [char; 9] =
    [',', '+', '(', ')', '[', ']', '"', '\'', '`'];

/// One declared enum choice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputChoice {
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Severity of a choice-validation issue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChoiceIssueSeverity {
    Error,
    Warning,
}

/// One issue found while validating enum choices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChoiceIssue {
    pub severity: ChoiceIssueSeverity,
    pub message: String,
}

/// Parsed choices plus issues. Invalid items are omitted from `choices`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidateEnumChoicesResult {
    pub choices: Vec<InputChoice>,
    pub issues: Vec<ChoiceIssue>,
}

/// Validate choice items given as JSON values (Python's raw YAML scalars).
pub fn validate_enum_choices(items: &[JsonValue]) -> ValidateEnumChoicesResult {
    let mut choices = Vec::new();
    let mut issues = Vec::new();
    let mut seen = Vec::new();
    for item in items {
        match parse_item(item) {
            ItemParse::Choice(choice, warnings) => {
                if seen.iter().any(|value| value == &choice.value) {
                    issues.push(error(format!(
                        "choice `{}` is declared twice",
                        choice.value
                    )));
                    continue;
                }
                seen.push(choice.value.clone());
                issues.extend(warnings);
                choices.push(choice);
            }
            ItemParse::Failed(item_issues) => issues.extend(item_issues),
        }
    }
    ValidateEnumChoicesResult { choices, issues }
}

/// Validate choice items given as YAML values (Rust frontmatter).
pub fn validate_enum_choices_yaml(
    items: &[serde_yaml::Value],
) -> ValidateEnumChoicesResult {
    let json_items: Vec<JsonValue> = items.iter().map(yaml_to_json).collect();
    validate_enum_choices(&json_items)
}

enum ItemParse {
    Choice(InputChoice, Vec<ChoiceIssue>),
    Failed(Vec<ChoiceIssue>),
}

fn parse_item(item: &JsonValue) -> ItemParse {
    match item {
        JsonValue::String(value) => parse_string_choice(value, None, None),
        JsonValue::Object(map) => parse_mapping_choice(map),
        other => ItemParse::Failed(vec![error(non_string_item_message(other))]),
    }
}

fn parse_mapping_choice(map: &serde_json::Map<String, JsonValue>) -> ItemParse {
    let mut issues = Vec::new();
    for key in map.keys() {
        if !ALLOWED_KEYS.contains(&key.as_str()) {
            issues.push(error(format!(
                "choice item has unknown key `{key}`; allowed keys are \
                 value, label, and description"
            )));
        }
    }
    let Some(value_entry) = map.get("value") else {
        issues.push(error(
            "choice item is missing required key `value`".to_string(),
        ));
        return ItemParse::Failed(issues);
    };
    let value = match value_entry {
        JsonValue::String(value) => value.clone(),
        other => {
            issues.push(error(non_string_item_message(other)));
            return ItemParse::Failed(issues);
        }
    };
    let label = match optional_string_field(map, "label") {
        Ok(label) => label,
        Err(message) => {
            issues.push(error(message));
            return ItemParse::Failed(issues);
        }
    };
    let description = match optional_string_field(map, "description") {
        Ok(description) => description,
        Err(message) => {
            issues.push(error(message));
            return ItemParse::Failed(issues);
        }
    };
    if !issues.is_empty() {
        return ItemParse::Failed(issues);
    }
    parse_string_choice(&value, label, description)
}

fn optional_string_field(
    map: &serde_json::Map<String, JsonValue>,
    key: &str,
) -> Result<Option<String>, String> {
    match map.get(key) {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(other) => Err(format!(
            "choice `{key}` arrived as {} and must be a string",
            json_kind_phrase(other)
        )),
    }
}

fn parse_string_choice(
    value: &str,
    label: Option<String>,
    description: Option<String>,
) -> ItemParse {
    if let Some(message) = string_value_error(value) {
        return ItemParse::Failed(vec![error(message)]);
    }
    let mut warnings = Vec::new();
    if value.chars().any(|ch| SHORTHAND_HOSTILE.contains(&ch)) {
        warnings.push(warning(format!(
            "choice `{value}` contains characters that need quoting in \
             shorthand"
        )));
    }
    ItemParse::Choice(
        InputChoice {
            value: value.to_string(),
            label,
            description,
        },
        warnings,
    )
}

fn string_value_error(value: &str) -> Option<String> {
    if value.is_empty() {
        return Some("choice value is empty".to_string());
    }
    if value.chars().any(char::is_whitespace) {
        return Some(format!(
            "choice `{value}` contains whitespace; choice values are \
             single words"
        ));
    }
    if value == "null" {
        return Some(
            "choice `null` is reserved (it means \"use the default\")"
                .to_string(),
        );
    }
    None
}

fn non_string_item_message(value: &JsonValue) -> String {
    format!(
        "choice arrived as {} and must be quoted",
        json_kind_phrase(value)
    )
}

fn json_kind_phrase(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Bool(_) => "a boolean",
        JsonValue::Number(number) if number.is_i64() || number.is_u64() => {
            "an int"
        }
        JsonValue::Number(_) => "a float",
        JsonValue::Null => "null",
        JsonValue::Array(_) => "a list",
        JsonValue::Object(_) => "a mapping",
        JsonValue::String(_) => "a string",
    }
}

fn error(message: String) -> ChoiceIssue {
    ChoiceIssue {
        severity: ChoiceIssueSeverity::Error,
        message,
    }
}

fn warning(message: String) -> ChoiceIssue {
    ChoiceIssue {
        severity: ChoiceIssueSeverity::Warning,
        message,
    }
}

pub(crate) fn yaml_to_json(value: &serde_yaml::Value) -> JsonValue {
    match value {
        serde_yaml::Value::Null => JsonValue::Null,
        serde_yaml::Value::Bool(flag) => JsonValue::Bool(*flag),
        serde_yaml::Value::Number(number) => yaml_number_to_json(number),
        serde_yaml::Value::String(text) => JsonValue::String(text.clone()),
        serde_yaml::Value::Sequence(items) => {
            JsonValue::Array(items.iter().map(yaml_to_json).collect())
        }
        serde_yaml::Value::Mapping(mapping) => {
            let mut object = serde_json::Map::new();
            for (key, nested) in mapping {
                object.insert(yaml_key_to_string(key), yaml_to_json(nested));
            }
            JsonValue::Object(object)
        }
        serde_yaml::Value::Tagged(tagged) => yaml_to_json(&tagged.value),
    }
}

fn yaml_number_to_json(number: &serde_yaml::Number) -> JsonValue {
    if let Some(int) = number.as_i64() {
        JsonValue::Number(int.into())
    } else if let Some(uint) = number.as_u64() {
        JsonValue::Number(uint.into())
    } else if let Some(float) = number.as_f64() {
        serde_json::Number::from_f64(float)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null)
    } else {
        JsonValue::Null
    }
}

fn yaml_key_to_string(key: &serde_yaml::Value) -> String {
    match key {
        serde_yaml::Value::String(text) => text.clone(),
        serde_yaml::Value::Bool(flag) => flag.to_string(),
        serde_yaml::Value::Number(number) => number.to_string(),
        serde_yaml::Value::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

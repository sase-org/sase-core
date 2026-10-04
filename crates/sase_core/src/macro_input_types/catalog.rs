//! Builtin catalog rows for this epic's input-type vocabulary.

use serde::{Deserialize, Serialize};

use super::InputChoice;

/// Kind of a catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputTypeKind {
    Scalar,
    InlineEnum,
    NamedEnum,
    Domain,
}

/// Where a catalog entry comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatalogSource {
    Builtin,
    Plugin { distribution: String, path: String },
}

/// One row of the input-type catalog.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub name: String,
    pub aliases: Vec<String>,
    pub kind: InputTypeKind,
    pub base: String,
    pub value_role: Option<String>,
    pub choices: Vec<InputChoice>,
    pub description: String,
    pub rule: String,
    pub source: CatalogSource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated_alias_of: Option<String>,
    pub advertised: bool,
}

/// Builtin catalog rows for this epic: scalars, `string`, `enum`, and `agent`.
pub fn builtin_catalog() -> Vec<CatalogEntry> {
    vec![
        scalar("word", &[], "A single word with no whitespace."),
        scalar("line", &[], "A single line of text with no line breaks."),
        scalar("text", &[], "Free-form text that may span multiple lines."),
        scalar("path", &[], "A filesystem path with no whitespace."),
        scalar("int", &["integer"], "A whole number."),
        scalar("float", &[], "A number, optionally with a decimal point."),
        scalar(
            "bool",
            &["boolean"],
            "A boolean: true or false (also yes/no, on/off, 1/0).",
        ),
        scalar(
            "code",
            &[],
            "Structured source plus language, not a plain string convention.",
        ),
        CatalogEntry {
            name: "string".to_string(),
            aliases: Vec::new(),
            kind: InputTypeKind::Scalar,
            base: "line".to_string(),
            value_role: None,
            choices: Vec::new(),
            description: "Deprecated alias of `line`.".to_string(),
            rule: "A single line of text with no line breaks.".to_string(),
            source: CatalogSource::Builtin,
            deprecated_alias_of: Some("line".to_string()),
            advertised: false,
        },
        CatalogEntry {
            name: "enum".to_string(),
            aliases: Vec::new(),
            kind: InputTypeKind::InlineEnum,
            base: "enum".to_string(),
            value_role: None,
            choices: Vec::new(),
            description: "One of the values declared under `choices`."
                .to_string(),
            rule: "One of the values declared under `choices`.".to_string(),
            source: CatalogSource::Builtin,
            deprecated_alias_of: None,
            advertised: true,
        },
        CatalogEntry {
            name: "agent".to_string(),
            aliases: Vec::new(),
            kind: InputTypeKind::Domain,
            base: "agent".to_string(),
            value_role: Some("agent".to_string()),
            choices: Vec::new(),
            description: "A non-empty agent name with no whitespace."
                .to_string(),
            rule: "A non-empty agent name with no whitespace.".to_string(),
            source: CatalogSource::Builtin,
            deprecated_alias_of: None,
            advertised: true,
        },
    ]
}

fn scalar(name: &str, aliases: &[&str], rule: &str) -> CatalogEntry {
    CatalogEntry {
        name: name.to_string(),
        aliases: aliases.iter().map(|alias| (*alias).to_string()).collect(),
        kind: InputTypeKind::Scalar,
        base: name.to_string(),
        value_role: None,
        choices: Vec::new(),
        description: rule.to_string(),
        rule: rule.to_string(),
        source: CatalogSource::Builtin,
        deprecated_alias_of: None,
        advertised: true,
    }
}

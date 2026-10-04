//! Resolve a declared type name against a registry.

use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::catalog::{CatalogEntry, InputTypeKind};
use super::suggest::{did_you_mean_suffix, suggest_closest};
use super::{InputChoice, InputTypeRegistry};

const BUILTIN_PLUGIN: &str = "builtin";

/// Successfully resolved input type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedInputType {
    pub base: String,
    #[serde(default)]
    pub named_type: Option<String>,
    #[serde(default)]
    pub value_role: Option<String>,
    #[serde(default)]
    pub choices: Vec<InputChoice>,
    pub deprecated: bool,
}

/// Resolver failure whose `Display` is a verbatim contract message.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{message}")]
pub struct ResolveInputTypeError {
    pub message: String,
}

impl ResolveInputTypeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Resolve `raw` as the type of input `name`.
///
/// Bare names and `builtin@<name>` match case-insensitively. Unknown names
/// are errors with suggestions; there is no silent `line` fallback.
pub fn resolve_input_type(
    name: &str,
    raw: &str,
    registry: &InputTypeRegistry,
) -> Result<ResolvedInputType, ResolveInputTypeError> {
    let trimmed = raw.trim();
    if let Some(id) = builtin_alias_name(trimmed) {
        return resolve_bare(name, id, registry);
    }
    if let Some((plugin, id)) = parse_qualified_id(trimmed) {
        return resolve_qualified(name, trimmed, &plugin, &id, registry);
    }
    resolve_bare(name, trimmed, registry)
}

pub(crate) fn pep503_normalize(distribution: &str) -> String {
    pep503_re().replace_all(distribution, "-").to_lowercase()
}

fn pep503_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"[-_.]+").expect("pep503 regex"))
}

fn resolve_bare(
    name: &str,
    raw: &str,
    registry: &InputTypeRegistry,
) -> Result<ResolvedInputType, ResolveInputTypeError> {
    let needle = raw.to_lowercase();
    if let Some(entry) = lookup_entry(registry, &needle) {
        return Ok(resolved_from_entry(entry));
    }
    let suggestions = suggest_closest(&needle, suggestion_candidates(registry));
    Err(ResolveInputTypeError::new(format!(
        "input `{name}` has unknown type `{raw}`{}",
        did_you_mean_suffix(&suggestions)
    )))
}

fn resolve_qualified(
    name: &str,
    raw: &str,
    plugin: &str,
    id: &str,
    registry: &InputTypeRegistry,
) -> Result<ResolvedInputType, ResolveInputTypeError> {
    let canonical = pep503_normalize(plugin);
    if !registry.has_plugin(&canonical) {
        return Err(ResolveInputTypeError::new(format!(
            "input `{name}` uses `{raw}`, but plugin `{canonical}` is not \
             installed; run `sase plugin install {canonical}`"
        )));
    }
    let ids = registry.plugin_type_ids(&canonical).unwrap_or(&[]);
    if ids.iter().any(|declared| declared == id) {
        return Ok(ResolvedInputType {
            base: "enum".to_string(),
            named_type: Some(format!("{canonical}@{id}")),
            value_role: None,
            choices: Vec::new(),
            deprecated: false,
        });
    }
    let suggestions = suggest_closest(id, ids);
    Err(ResolveInputTypeError::new(format!(
        "plugin `{canonical}` declares no input type `{id}`{}",
        did_you_mean_suffix(&suggestions)
    )))
}

fn lookup_entry<'a>(
    registry: &'a InputTypeRegistry,
    needle: &str,
) -> Option<&'a CatalogEntry> {
    registry.entries().iter().find(|entry| {
        entry.name == needle
            || entry.aliases.iter().any(|alias| alias == needle)
    })
}

fn resolved_from_entry(entry: &CatalogEntry) -> ResolvedInputType {
    let named_type = match entry.kind {
        InputTypeKind::Domain | InputTypeKind::NamedEnum => {
            Some(entry.name.clone())
        }
        InputTypeKind::Scalar | InputTypeKind::InlineEnum => None,
    };
    ResolvedInputType {
        base: entry.base.clone(),
        named_type,
        value_role: entry.value_role.clone(),
        choices: entry.choices.clone(),
        deprecated: entry.deprecated_alias_of.is_some(),
    }
}

fn suggestion_candidates(
    registry: &InputTypeRegistry,
) -> impl Iterator<Item = &str> {
    registry
        .entries()
        .iter()
        .filter(|entry| entry.advertised)
        .flat_map(|entry| {
            std::iter::once(entry.name.as_str())
                .chain(entry.aliases.iter().map(String::as_str))
        })
}

fn builtin_alias_name(raw: &str) -> Option<&str> {
    let (plugin, id) = raw.split_once('@')?;
    plugin.eq_ignore_ascii_case(BUILTIN_PLUGIN).then_some(id)
}

fn parse_qualified_id(raw: &str) -> Option<(String, String)> {
    let captures = qualified_id_re().captures(raw)?;
    Some((
        captures.get(1)?.as_str().to_string(),
        captures.get(2)?.as_str().to_string(),
    ))
}

fn qualified_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^([A-Za-z0-9._-]+)@([a-z0-9][a-z0-9_-]*)$")
            .expect("qualified-id regex")
    })
}

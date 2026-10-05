//! Builtin, fixture, and plugin-file input-type registries.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use regex::Regex;
use serde::{Deserialize, Serialize};

use super::catalog::{builtin_catalog, CatalogEntry, CatalogSource, InputTypeKind};
use super::choices::ChoiceIssueSeverity;
use super::{
    pyyaml_plain_scalar_is_non_string, unquoted_plain_scalar_choice_error,
    validate_enum_choices_yaml,
};

/// Catalog rows plus the plugin types known to this process.
///
/// `builtin()` has an empty plugin list. Tests pass a fixture map of PEP 503
/// distribution names to declared type ids. Production builds the registry
/// from `input_types.yml` discovery records via
/// [`load_plugin_input_type_registry`]; that path carries real choices and
/// never treats an ID-only fixture as a valid empty closed set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTypeRegistry {
    entries: Vec<CatalogEntry>,
    #[serde(default)]
    plugins: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    known_distributions: BTreeSet<String>,
}

impl InputTypeRegistry {
    /// Builtin scalars, `string`, `enum`, and `agent`, with no plugins.
    pub fn builtin() -> Self {
        Self {
            entries: builtin_catalog(),
            plugins: BTreeMap::new(),
            known_distributions: BTreeSet::new(),
        }
    }

    /// Builtin catalog plus a fixture plugin map (`distribution` → type ids).
    ///
    /// Fixture-only: resolution of a fixture ID yields an empty closed set
    /// for backward compatibility. Production registries built by the loader
    /// carry real choices in `entries` and never rely on this path.
    pub fn with_plugins(plugins: BTreeMap<String, Vec<String>>) -> Self {
        let mut canonical = BTreeMap::new();
        for (distribution, ids) in plugins {
            canonical
                .entry(super::resolve::pep503_normalize(&distribution))
                .or_insert_with(Vec::new)
                .extend(ids);
        }
        let known = canonical.keys().cloned().collect();
        Self {
            entries: builtin_catalog(),
            plugins: canonical,
            known_distributions: known,
        }
    }

    /// Builtin catalog plus loaded plugin entries and known distributions.
    pub fn with_plugin_entries(
        entries: Vec<CatalogEntry>,
        known_distributions: BTreeSet<String>,
    ) -> Self {
        let mut all = builtin_catalog();
        let mut sorted = entries;
        sorted.sort_by(|a, b| a.name.cmp(&b.name));
        all.extend(sorted);
        Self {
            entries: all,
            plugins: BTreeMap::new(),
            known_distributions,
        }
    }

    pub fn entries(&self) -> &[CatalogEntry] {
        &self.entries
    }

    pub fn plugins(&self) -> &BTreeMap<String, Vec<String>> {
        &self.plugins
    }

    pub fn known_distributions(&self) -> &BTreeSet<String> {
        &self.known_distributions
    }

    /// Type ids declared by `distribution`, after PEP 503 normalization.
    ///
    /// Combines loaded plugin entries with the legacy fixture map so
    /// suggestions work on both paths.
    pub fn plugin_type_ids(&self, distribution: &str) -> Option<Vec<String>> {
        let canonical = super::resolve::pep503_normalize(distribution);
        let mut ids = BTreeSet::new();
        let prefix = format!("{canonical}@");
        for entry in &self.entries {
            if let Some(id) = entry.name.strip_prefix(&prefix) {
                ids.insert(id.to_string());
            }
        }
        if let Some(fixture) = self.plugins.get(&canonical) {
            for id in fixture {
                ids.insert(id.clone());
            }
        }
        if ids.is_empty() {
            if self.known_distributions.contains(&canonical)
                || self.plugins.contains_key(&canonical)
            {
                return Some(Vec::new());
            }
            return None;
        }
        Some(ids.into_iter().collect())
    }

    /// Legacy slice view of fixture-only ids (fixture compatibility).
    pub fn fixture_type_ids(&self, distribution: &str) -> Option<&[String]> {
        self.plugins
            .get(&super::resolve::pep503_normalize(distribution))
            .map(Vec::as_slice)
    }

    pub fn has_plugin(&self, distribution: &str) -> bool {
        let canonical = super::resolve::pep503_normalize(distribution);
        self.known_distributions.contains(&canonical)
            || self.plugins.contains_key(&canonical)
    }

    /// Loaded plugin entry for `distribution@id`, if present.
    pub fn plugin_entry(
        &self,
        distribution: &str,
        id: &str,
    ) -> Option<&CatalogEntry> {
        let canonical = super::resolve::pep503_normalize(distribution);
        let qualified = format!("{canonical}@{id}");
        self.entries.iter().find(|entry| entry.name == qualified)
    }
}

/// One discovery record for a plugin `input_types.yml` manifest.
///
/// Produced by Python discovery from `sase_macros` entry points (plus legacy
/// `sase_xprompts` while enabled). `path` is the concrete manifest file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginInputTypeFileRecord {
    pub distribution: String,
    pub module: String,
    pub path: String,
}

/// Structured diagnostic from loading plugin `input_types.yml` files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PluginRegistryDiagnostic {
    pub severity: ChoiceIssueSeverity,
    pub distribution: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    pub message: String,
}

/// Load a registry snapshot from discovery records.
///
/// Each manifest is read once per call. File I/O and all schema/choice
/// validation live here. A file read/parse failure never discards valid
/// files; a type with any error is wholly skipped while valid siblings stay
/// usable. Duplicate qualified definitions emit deterministic diagnostics and
/// keep the first definition. Entry-point aliases that resolve to the same
/// file load it once. Known distribution identity is preserved even when a
/// distribution declares no valid types.
pub fn load_plugin_input_type_registry(
    records: &[PluginInputTypeFileRecord],
) -> (InputTypeRegistry, Vec<PluginRegistryDiagnostic>) {
    load_plugin_input_type_registry_with_known(records, &[])
}

/// Load a registry snapshot, preserving additional known distributions.
///
/// `extra_known` covers installed macro plugins that ship no manifest, so
/// they still resolve as "declares no input type" rather than "not
/// installed".
pub fn load_plugin_input_type_registry_with_known(
    records: &[PluginInputTypeFileRecord],
    extra_known: &[String],
) -> (InputTypeRegistry, Vec<PluginRegistryDiagnostic>) {
    let mut sorted: Vec<&PluginInputTypeFileRecord> = records.iter().collect();
    sorted.sort_by(|a, b| {
        (
            super::resolve::pep503_normalize(&a.distribution),
            a.path.as_str(),
            a.module.as_str(),
        )
            .cmp(&(
                super::resolve::pep503_normalize(&b.distribution),
                b.path.as_str(),
                b.module.as_str(),
            ))
    });
    let mut known = BTreeSet::new();
    for record in &sorted {
        known.insert(super::resolve::pep503_normalize(&record.distribution));
    }
    for name in extra_known {
        known.insert(super::resolve::pep503_normalize(name));
    }
    let mut seen_paths = BTreeSet::new();
    let mut entries = Vec::new();
    let mut seen_qualified = BTreeSet::new();
    let mut diagnostics = Vec::new();
    for record in sorted {
        let canonical_dist =
            super::resolve::pep503_normalize(&record.distribution);
        let canonical_path = canonicalize_record_path(&record.path);
        if !seen_paths.insert(canonical_path.clone()) {
            continue;
        }
        let text = match std::fs::read_to_string(Path::new(&record.path)) {
            Ok(text) => text,
            Err(error) => {
                diagnostics.push(PluginRegistryDiagnostic {
                    severity: ChoiceIssueSeverity::Error,
                    distribution: canonical_dist,
                    path: record.path.clone(),
                    type_id: None,
                    line: None,
                    message: format!(
                        "failed to read input_types.yml: {error}"
                    ),
                });
                continue;
            }
        };
        load_one_manifest(
            &canonical_dist,
            &record.path,
            &text,
            &mut entries,
            &mut seen_qualified,
            &mut diagnostics,
        );
    }
    entries.sort_by(|a, b| a.name.cmp(&b.name));
    (InputTypeRegistry::with_plugin_entries(entries, known), diagnostics)
}

fn canonicalize_record_path(path: &str) -> String {
    if let Ok(canonical) = Path::new(path).canonicalize() {
        return canonical.to_string_lossy().into_owned();
    }
    PathBuf::from(path).to_string_lossy().into_owned()
}

fn load_one_manifest(
    distribution: &str,
    path: &str,
    text: &str,
    entries: &mut Vec<CatalogEntry>,
    seen_qualified: &mut BTreeSet<String>,
    diagnostics: &mut Vec<PluginRegistryDiagnostic>,
) {
    let value: serde_yaml::Value = match serde_yaml::from_str(text) {
        Ok(value) => value,
        Err(error) => {
            diagnostics.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: None,
                line: error.location().map(|loc| loc.line() as u32),
                message: format!("failed to parse input_types.yml: {error}"),
            });
            return;
        }
    };
    let Some(mapping) = value.as_mapping() else {
        diagnostics.push(PluginRegistryDiagnostic {
            severity: ChoiceIssueSeverity::Error,
            distribution: distribution.to_string(),
            path: path.to_string(),
            type_id: None,
            line: None,
            message: "input_types.yml must be a mapping with `schema_version` and `types`"
                .to_string(),
        });
        return;
    };
    let allowed_envelope = ["schema_version", "types"];
    let mut envelope_ok = true;
    for key in mapping.keys() {
        let key_text = yaml_key_text(key);
        if !allowed_envelope.contains(&key_text.as_str()) {
            diagnostics.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: None,
                line: find_line(text, &key_text),
                message: format!(
                    "input_types.yml has unknown key `{key_text}`; allowed keys are schema_version and types"
                ),
            });
            envelope_ok = false;
        }
    }
    let schema_version = mapping
        .iter()
        .find(|(key, _)| yaml_key_text(key) == "schema_version")
        .map(|(_, value)| value);
    let types_value = mapping
        .iter()
        .find(|(key, _)| yaml_key_text(key) == "types")
        .map(|(_, value)| value);
    match schema_version {
        Some(serde_yaml::Value::Number(number))
            if number.as_u64() == Some(1) => {}
        _ => {
            diagnostics.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: None,
                line: find_line(text, "schema_version"),
                message: "input_types.yml `schema_version` must be 1"
                    .to_string(),
            });
            envelope_ok = false;
        }
    }
    let Some(types_value) = types_value else {
        diagnostics.push(PluginRegistryDiagnostic {
            severity: ChoiceIssueSeverity::Error,
            distribution: distribution.to_string(),
            path: path.to_string(),
            type_id: None,
            line: find_line(text, "types"),
            message: "input_types.yml is missing required key `types`"
                .to_string(),
        });
        return;
    };
    if !envelope_ok {
        // Malformed envelopes produce file diagnostics without breaking
        // unrelated plugins; still attempt type loading when `types` parses
        // as a mapping so valid siblings elsewhere are unaffected. This
        // file's types are skipped when the envelope itself is invalid.
        if !matches!(types_value, serde_yaml::Value::Mapping(_)) {
            return;
        }
        // When only unknown envelope keys are present, skip the file to
        // avoid silently accepting a future schema.
        return;
    }
    let Some(types_mapping) = types_value.as_mapping() else {
        diagnostics.push(PluginRegistryDiagnostic {
            severity: ChoiceIssueSeverity::Error,
            distribution: distribution.to_string(),
            path: path.to_string(),
            type_id: None,
            line: find_line(text, "types"),
            message: "input_types.yml `types` must be a mapping".to_string(),
        });
        return;
    };
    let mut sorted_types: Vec<(String, &serde_yaml::Value)> = types_mapping
        .iter()
        .map(|(key, value)| (yaml_key_text(key), value))
        .collect();
    sorted_types.sort_by(|a, b| a.0.cmp(&b.0));
    for (id, type_value) in sorted_types {
        let qualified = format!("{distribution}@{id}");
        if !type_id_re().is_match(&id) {
            diagnostics.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: Some(id.clone()),
                line: find_line(text, &id),
                message: format!(
                    "input type `{id}` must match `[a-z0-9][a-z0-9_-]*`"
                ),
            });
            continue;
        }
        if !seen_qualified.insert(qualified.clone()) {
            diagnostics.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: Some(id.clone()),
                line: find_line(text, &id),
                message: format!(
                    "input type `{id}` is declared twice for plugin `{distribution}`"
                ),
            });
            continue;
        }
        let (entry, mut issues) =
            load_one_type(distribution, path, text, &id, type_value);
        diagnostics.append(&mut issues);
        if let Some(entry) = entry {
            entries.push(entry);
        }
    }
}

fn load_one_type(
    distribution: &str,
    path: &str,
    source_text: &str,
    id: &str,
    value: &serde_yaml::Value,
) -> (Option<CatalogEntry>, Vec<PluginRegistryDiagnostic>) {
    let mut issues = Vec::new();
    let error = |message: String| PluginRegistryDiagnostic {
        severity: ChoiceIssueSeverity::Error,
        distribution: distribution.to_string(),
        path: path.to_string(),
        type_id: Some(id.to_string()),
        line: find_line(source_text, id),
        message,
    };
    let Some(mapping) = value.as_mapping() else {
        return (
            None,
            vec![error(format!(
                "input type `{id}` must be a mapping with `description` and `choices`"
            ))],
        );
    };
    for key in mapping.keys() {
        let key_text = yaml_key_text(key);
        if key_text != "description" && key_text != "choices" {
            issues.push(error(format!(
                "input type `{id}` has unknown key `{key_text}`; allowed keys are description and choices"
            )));
        }
    }
    let description_value =
        mapping.iter().find(|(key, _)| yaml_key_text(key) == "description").map(
            |(_, value)| value,
        );
    let choices_value =
        mapping.iter().find(|(key, _)| yaml_key_text(key) == "choices").map(
            |(_, value)| value,
        );
    let Some(description_value) = description_value else {
        issues.push(error(format!(
            "input type `{id}` is missing required key `description`"
        )));
        return (None, issues);
    };
    let Some(choices_value) = choices_value else {
        issues.push(error(format!(
            "input type `{id}` is missing required key `choices`"
        )));
        return (None, issues);
    };
    if !issues.is_empty() {
        return (None, issues);
    }
    let Some(description) = description_value.as_str() else {
        return (
            None,
            vec![error(format!(
                "input type `{id}` `description` must be a string"
            ))],
        );
    };
    let Some(items) = choices_value.as_sequence() else {
        return (
            None,
            vec![error(format!(
                "input type `{id}` `choices` must be a non-empty list"
            ))],
        );
    };
    if items.is_empty() {
        return (
            None,
            vec![error(format!(
                "input type `{id}` `choices` must be a non-empty list"
            ))],
        );
    }
    let validated = validate_enum_choices_yaml(items);
    let mut has_error = false;
    for issue in &validated.issues {
        let severity = issue.severity;
        let mut message = issue.message.clone();
        // Attach the type context when the shared validator message does
        // not name it.
        if !message.contains(id) && severity == ChoiceIssueSeverity::Error {
            message = format!("input type `{id}`: {message}");
        }
        issues.push(PluginRegistryDiagnostic {
            severity,
            distribution: distribution.to_string(),
            path: path.to_string(),
            type_id: Some(id.to_string()),
            line: find_line(source_text, id),
            message,
        });
        if severity == ChoiceIssueSeverity::Error {
            has_error = true;
        }
    }
    // YAML 1.1 parity for string choices that serde_yaml keeps as strings
    // but PyYAML would type (timestamps): the manifest source must quote
    // them. Quoted occurrences (`"yes"`/`'yes'`) are allowed.
    for choice in &validated.choices {
        if pyyaml_plain_scalar_is_non_string(&choice.value)
            && !is_quoted_in_source(source_text, &choice.value)
        {
            let quoted_message =
                unquoted_plain_scalar_choice_error(&choice.value)
                    .unwrap_or_else(|| {
                        format!(
                            "choice `{}` must be quoted",
                            choice.value
                        )
                    });
            issues.push(PluginRegistryDiagnostic {
                severity: ChoiceIssueSeverity::Error,
                distribution: distribution.to_string(),
                path: path.to_string(),
                type_id: Some(id.to_string()),
                line: find_line(source_text, &choice.value),
                message: format!("input type `{id}`: {quoted_message}"),
            });
            has_error = true;
        }
    }
    if has_error {
        return (None, issues);
    }
    // Warnings (shorthand quoting) are reported but do not skip the type.
    let qualified = format!("{distribution}@{id}");
    let entry = CatalogEntry {
        name: qualified,
        aliases: Vec::new(),
        kind: InputTypeKind::NamedEnum,
        base: "enum".to_string(),
        value_role: None,
        choices: validated.choices,
        description: description.to_string(),
        rule: description.to_string(),
        source: CatalogSource::Plugin {
            distribution: distribution.to_string(),
            path: path.to_string(),
        },
        deprecated_alias_of: None,
        advertised: true,
    };
    (Some(entry), issues)
}

fn is_quoted_in_source(source: &str, value: &str) -> bool {
    let double_quoted = format!("\"{value}\"");
    let single_quoted = format!("'{value}'");
    source.contains(&double_quoted) || source.contains(&single_quoted)
}

fn yaml_key_text(key: &serde_yaml::Value) -> String {
    match key {
        serde_yaml::Value::String(text) => text.clone(),
        serde_yaml::Value::Bool(flag) => flag.to_string(),
        serde_yaml::Value::Number(number) => number.to_string(),
        serde_yaml::Value::Null => "null".to_string(),
        other => format!("{other:?}"),
    }
}

fn find_line(source: &str, needle: &str) -> Option<u32> {
    source
        .lines()
        .enumerate()
        .find(|(_, line)| line.contains(needle))
        .map(|(index, _)| (index + 1) as u32)
}

fn type_id_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^[a-z0-9][a-z0-9_-]*$").expect("type id regex")
    })
}

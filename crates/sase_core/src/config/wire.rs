//! Wire records for the `sase_core::config` domain.
//!
//! Mirrors the bead/editor wire split: owned, serde-friendly data with no
//! PyO3 types so later binding/TUI/web work can reuse the same model. The
//! config backend is JSON-in / JSON-out: Python performs plugin discovery,
//! file IO, and YAML parsing, then hands the already-decoded layer data here
//! as plain JSON values. This crate owns the deterministic domain logic
//! (schema field model, layer merge + per-field provenance, schema
//! validation, and the logical write-plan).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

/// Wire schema version for the config contract. Bumped on breaking shape
/// changes so Python can detect a binding/contract mismatch.
pub const CONFIG_WIRE_SCHEMA_VERSION: u32 = 1;

/// Error raised by the config domain. Surfaced to Python as a `ValueError`
/// whose message is this error's `Display` form.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct ConfigError {
    pub kind: String,
    pub message: String,
}

impl ConfigError {
    /// A request was structurally invalid (e.g. unknown target layer).
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    /// The supplied JSON Schema document was malformed.
    pub fn schema(message: impl Into<String>) -> Self {
        Self {
            kind: "schema".to_string(),
            message: message.into(),
        }
    }
}

/// List merge behavior for a single layer, mirroring the Python
/// `_deep_merge(..., list_strategy=...)` contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListStrategy {
    /// Override list is appended to the base list (default).
    Concatenate,
    /// Override list replaces the base list entirely.
    Replace,
}

impl ListStrategy {
    /// Parse a layer's `list_strategy` string. Anything other than the exact
    /// token `"replace"` is treated as concatenate, matching the Python
    /// default.
    pub fn from_token(token: &str) -> Self {
        if token == "replace" {
            ListStrategy::Replace
        } else {
            ListStrategy::Concatenate
        }
    }
}

fn default_list_strategy() -> String {
    "concatenate".to_string()
}

// --- Field model ----------------------------------------------------------

/// Numeric/string constraints flattened from a JSON Schema field.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConfigConstraintsWire {
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub exclusive_minimum: Option<f64>,
    pub exclusive_maximum: Option<f64>,
    pub min_length: Option<u64>,
    pub max_length: Option<u64>,
    pub pattern: Option<String>,
}

/// One flattened schema field.
///
/// `kind` is one of `"scalar"`, `"array"`, `"map"` (an open object whose keys
/// are user-defined, e.g. `additionalProperties`), or `"object"` (a closed
/// section with named `properties`, which is recursed into). `leaf` is false
/// only for `"object"` containers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigFieldWire {
    pub path: String,
    pub name: String,
    pub depth: usize,
    pub parent: Option<String>,
    pub kind: String,
    pub leaf: bool,
    pub types: Vec<String>,
    pub description: String,
    pub has_default: bool,
    pub default: Value,
    pub enum_values: Vec<Value>,
    pub deprecated: bool,
    pub deprecated_replacement: Option<String>,
    pub constraints: ConfigConstraintsWire,
    pub additional_properties_allowed: bool,
}

/// The ordered, flattened field model for a schema document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigFieldModelWire {
    pub schema_version: u32,
    pub fields: Vec<ConfigFieldWire>,
}

// --- Diagnostics ----------------------------------------------------------

/// A single diagnostic. `severity` is `"error"`, `"warning"`, or `"info"`.
/// `path` is a dotted instance path when field-scoped; `layer` is set when
/// the diagnostic is attributed to a specific layer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigDiagnosticWire {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub path: Option<String>,
    pub layer: Option<String>,
}

// --- Layer input ----------------------------------------------------------

/// One config layer supplied by Python (already discovered + decoded).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigLayerInputWire {
    pub name: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub value: Value,
    #[serde(default = "default_list_strategy")]
    pub list_strategy: String,
    #[serde(default)]
    pub writable: bool,
    #[serde(default)]
    pub exists: Option<bool>,
    #[serde(default)]
    pub error: Option<String>,
}

// --- Inventory ------------------------------------------------------------

/// Inventory request: the schema plus the ordered layer stack (lowest →
/// highest priority) and the deprecation/unsupported policy Python owns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigInventoryRequestWire {
    pub schema: Value,
    #[serde(default)]
    pub layers: Vec<ConfigLayerInputWire>,
    #[serde(default)]
    pub deprecations: BTreeMap<String, String>,
    #[serde(default)]
    pub unsupported: Vec<String>,
}

/// Per-layer summary in the inventory output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigSourceWire {
    pub name: String,
    pub kind: String,
    pub path: Option<String>,
    pub exists: bool,
    pub writable: bool,
    pub list_strategy: String,
    pub key_count: usize,
    pub unsupported_keys: Vec<String>,
    pub deprecated_keys: Vec<String>,
    pub error: Option<String>,
}

/// A single layer's contribution to one field. `winning` marks the
/// highest-priority contributor (the layer that "had the last word"); the
/// full stack is always listed in priority order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigContributionWire {
    pub layer: String,
    pub raw_value: Value,
    pub winning: bool,
}

/// Effective state + provenance for one field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigFieldStateWire {
    pub path: String,
    pub has_default: bool,
    pub default: Value,
    pub has_effective: bool,
    pub effective_value: Value,
    pub contributions: Vec<ConfigContributionWire>,
    pub deprecated_replacement: Option<String>,
    pub write_capabilities: Vec<String>,
}

/// Full inventory: source rail, per-field provenance, and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigInventoryWire {
    pub schema_version: u32,
    pub sources: Vec<ConfigSourceWire>,
    pub fields: Vec<ConfigFieldStateWire>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
}

// --- Edit plan ------------------------------------------------------------

/// A single set/unset operation on a field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEditOpWire {
    pub kind: String,
    #[serde(default)]
    pub value: Value,
}

/// Edit request: the schema, the layer stack, the chosen writable target
/// layer, the field path, and the operation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEditRequestWire {
    pub schema: Value,
    #[serde(default)]
    pub layers: Vec<ConfigLayerInputWire>,
    pub target_layer: String,
    /// Legacy dotted field path. New callers should prefer `key_path` when
    /// mapping keys may themselves contain dots.
    #[serde(default)]
    pub path: Option<String>,
    /// Exact field path segments. When both path forms are supplied they
    /// must describe the same display path.
    #[serde(default)]
    pub key_path: Option<Vec<String>>,
    pub op: ConfigEditOpWire,
    #[serde(default)]
    pub deprecations: BTreeMap<String, String>,
    #[serde(default)]
    pub unsupported: Vec<String>,
}

impl ConfigEditRequestWire {
    /// Resolve the legacy/exact path inputs into exact segments and a stable
    /// dotted display path.
    pub fn resolved_path(&self) -> Result<(Vec<String>, String), ConfigError> {
        let dotted = self.path.as_deref();
        if dotted.is_some_and(str::is_empty) {
            return Err(ConfigError::validation("edit path must not be empty"));
        }
        let exact = self.key_path.as_ref();
        if exact.is_some_and(|segments| {
            segments.is_empty() || segments.iter().any(String::is_empty)
        }) {
            return Err(ConfigError::validation(
                "edit key_path must contain non-empty segments",
            ));
        }

        match (dotted, exact) {
            (None, None) => Err(ConfigError::validation(
                "edit request requires path or key_path",
            )),
            (Some(path), None) => Ok((
                path.split('.').map(str::to_string).collect(),
                path.to_string(),
            )),
            (None, Some(segments)) => {
                Ok((segments.clone(), segments.join(".")))
            }
            (Some(path), Some(segments)) => {
                let display = segments.join(".");
                if path != display {
                    return Err(ConfigError::validation(format!(
                        "contradictory edit path `{path}` and key_path `{display}`"
                    )));
                }
                Ok((segments.clone(), path.to_string()))
            }
        }
    }
}

/// The logical, frontend-agnostic write plan. `key_path` is the dotted path
/// split into segments; a future CLI/web executor applies it to bytes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigWritePlanWire {
    pub file: Option<String>,
    pub layer: String,
    pub key_path: Vec<String>,
    pub op: String,
    pub has_value: bool,
    pub new_value: Value,
}

/// The before/after effective value for the edited field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEffectivePreviewWire {
    pub path: String,
    pub has_before: bool,
    pub before: Value,
    pub has_after: bool,
    pub after: Value,
    pub changed: bool,
}

/// Full edit plan: the write plan, the candidate merged config, the
/// effective-merge preview, candidate validation, and plan-level diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigEditPlanWire {
    pub schema_version: u32,
    pub write_plan: ConfigWritePlanWire,
    pub candidate_config: Value,
    pub effective_preview: ConfigEffectivePreviewWire,
    pub validation: Vec<ConfigDiagnosticWire>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
}

// --- Validate -------------------------------------------------------------

/// Validation request: a schema plus a candidate merged config to check.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigValidateRequestWire {
    pub schema: Value,
    pub config: Value,
}

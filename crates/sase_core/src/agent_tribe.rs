//! Shared agent-tribe identity and public-alias behavior.
//!
//! Stored agent metadata and assignment files keep their authored tribe bytes.
//! Public inputs may use the current automation name `job`, which resolves to
//! the historical stored `chop` identity for new built-in automation
//! assignments and wait targets.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

pub const LEGACY_JOB_TRIBE: &str = "chop";
pub const PUBLIC_JOB_TRIBE: &str = "job";
pub const RESERVED_DEFAULT_TRIBE: &str = "default";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentTribeError {
    #[error("tribe name must be a non-empty string")]
    Empty,
    #[error(
        "tribe name {0:?} must not start with '@' (the '@' is added on display only — drop it from the input)"
    )]
    AtPrefix(String),
    #[error(
        "tribe name {0:?} must match ^[A-Za-z0-9_.-]+$ (letters, digits, underscore, dot, dash)"
    )]
    Invalid(String),
}

pub fn is_reserved_tribe_name(tribe: &str) -> bool {
    tribe == RESERVED_DEFAULT_TRIBE
}

pub fn reserved_tribe_target_reason(tribe: &str) -> String {
    format!(
        "the reserved @{tribe} panel is the untagged bucket, not a real \
         tribe, so it can never resolve — target a named tribe, an agent, \
         a family, or a clan instead"
    )
}

pub fn validate_tribe_name(tribe: &str) -> Result<&str, AgentTribeError> {
    if tribe.is_empty() {
        return Err(AgentTribeError::Empty);
    }
    if tribe.starts_with('@') {
        return Err(AgentTribeError::AtPrefix(tribe.to_string()));
    }
    if !tribe.chars().all(|char| {
        char.is_ascii_alphanumeric() || matches!(char, '_' | '.' | '-')
    }) {
        return Err(AgentTribeError::Invalid(tribe.to_string()));
    }
    Ok(tribe)
}

pub fn canonicalize_public_tribe_name(
    tribe: &str,
) -> Result<String, AgentTribeError> {
    let validated = validate_tribe_name(tribe)?;
    Ok(if validated == PUBLIC_JOB_TRIBE {
        LEGACY_JOB_TRIBE.to_string()
    } else {
        validated.to_string()
    })
}

pub fn public_tribe_name(tribe: &str) -> String {
    if tribe == LEGACY_JOB_TRIBE {
        PUBLIC_JOB_TRIBE.to_string()
    } else {
        tribe.to_string()
    }
}

pub fn parse_tribe_reference(
    value: &str,
) -> Result<Option<String>, AgentTribeError> {
    if let Some(raw) = value.strip_prefix('@') {
        return canonicalize_public_tribe_name(raw).map(Some);
    }
    Ok(None)
}

pub fn valid_stored_tribe(value: &Value) -> Option<&str> {
    let tribe = value.as_str()?;
    validate_tribe_name(tribe).ok()?;
    Some(tribe)
}

pub fn canonicalize_agent_tribe_metadata(
    mut data: Map<String, Value>,
) -> Map<String, Value> {
    if !data.contains_key("tribe") {
        if let Some(legacy_tribe) = data
            .get("tag")
            .and_then(valid_stored_tribe)
            .map(str::to_string)
        {
            data.insert("tribe".to_string(), Value::String(legacy_tribe));
        }
    }
    let stored_tribe = data
        .get("tribe")
        .and_then(valid_stored_tribe)
        .map(str::to_string);
    if let Some(tribe) = stored_tribe {
        data.insert("tribe".to_string(), Value::String(tribe));
    }
    data.remove("tag");
    data
}

pub fn agent_tribe_display_key(
    stored_tribe: &str,
    configured_keys: &[String],
) -> Result<String, AgentTribeError> {
    let stored = validate_tribe_name(stored_tribe)?;
    if stored != LEGACY_JOB_TRIBE {
        return Ok(stored.to_string());
    }
    let keys: BTreeSet<&str> =
        configured_keys.iter().map(String::as_str).collect();
    if keys.contains(LEGACY_JOB_TRIBE) {
        Ok(LEGACY_JOB_TRIBE.to_string())
    } else if keys.contains(PUBLIC_JOB_TRIBE) {
        Ok(PUBLIC_JOB_TRIBE.to_string())
    } else {
        Ok(LEGACY_JOB_TRIBE.to_string())
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentTribeDisplayLayerWire {
    pub name: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub value: Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentTribeDisplayResolutionRequestWire {
    #[serde(default)]
    pub layers: Vec<AgentTribeDisplayLayerWire>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentTribeDisplayDiagnosticWire {
    pub code: String,
    pub message: String,
    pub layer: String,
    pub path: String,
    pub source_path: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentTribeDisplayResolutionWire {
    pub display_keys: BTreeMap<String, String>,
    pub diagnostics: Vec<AgentTribeDisplayDiagnosticWire>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentTribeIdentityResolutionRequestWire {
    pub tribe: String,
    #[serde(default)]
    pub layers: Vec<AgentTribeDisplayLayerWire>,
    #[serde(default)]
    pub stored_tribes: Vec<String>,
    #[serde(default)]
    pub current_tribe: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct AgentTribeIdentityResolutionWire {
    pub tribe: Option<String>,
    pub display_keys: BTreeMap<String, String>,
    pub diagnostics: Vec<AgentTribeDisplayDiagnosticWire>,
}

pub fn resolve_agent_tribe_display_config(
    request: &AgentTribeDisplayResolutionRequestWire,
) -> AgentTribeDisplayResolutionWire {
    let mut display_keys = BTreeMap::new();
    let mut diagnostics = Vec::new();

    for layer in &request.layers {
        let Some(tribes) = layer_tribes(layer) else {
            continue;
        };
        let legacy = tribes.get(LEGACY_JOB_TRIBE);
        let public = tribes.get(PUBLIC_JOB_TRIBE);
        match (legacy, public) {
            (Some(legacy_value), Some(public_value)) => {
                if !display_configs_equivalent(legacy_value, public_value) {
                    diagnostics.push(conflicting_job_alias_diagnostic(layer));
                    display_keys.insert(
                        LEGACY_JOB_TRIBE.to_string(),
                        LEGACY_JOB_TRIBE.to_string(),
                    );
                } else {
                    display_keys.insert(
                        LEGACY_JOB_TRIBE.to_string(),
                        PUBLIC_JOB_TRIBE.to_string(),
                    );
                }
            }
            (Some(_), None) => {
                display_keys.insert(
                    LEGACY_JOB_TRIBE.to_string(),
                    LEGACY_JOB_TRIBE.to_string(),
                );
            }
            (None, Some(_)) => {
                display_keys.insert(
                    LEGACY_JOB_TRIBE.to_string(),
                    PUBLIC_JOB_TRIBE.to_string(),
                );
            }
            (None, None) => {}
        }
    }
    diagnostics.extend(cross_layer_job_alias_diagnostics(&request.layers));

    AgentTribeDisplayResolutionWire {
        display_keys,
        diagnostics,
    }
}

pub fn resolve_agent_tribe_identity(
    request: &AgentTribeIdentityResolutionRequestWire,
) -> Result<AgentTribeIdentityResolutionWire, AgentTribeError> {
    let validated = validate_tribe_name(&request.tribe)?;
    let display_resolution = resolve_agent_tribe_display_config(
        &AgentTribeDisplayResolutionRequestWire {
            layers: request.layers.clone(),
        },
    );
    if validated != PUBLIC_JOB_TRIBE {
        return Ok(AgentTribeIdentityResolutionWire {
            tribe: Some(validated.to_string()),
            display_keys: display_resolution.display_keys,
            diagnostics: Vec::new(),
        });
    }

    if !display_resolution.diagnostics.is_empty() {
        return Ok(AgentTribeIdentityResolutionWire {
            tribe: None,
            display_keys: display_resolution.display_keys,
            diagnostics: display_resolution.diagnostics,
        });
    }

    let stored_tribes: BTreeSet<&str> = request
        .stored_tribes
        .iter()
        .filter_map(|tribe| validate_tribe_name(tribe).ok())
        .collect();
    let current_tribe = request
        .current_tribe
        .as_deref()
        .and_then(|tribe| validate_tribe_name(tribe).ok());
    let resolved = if current_tribe == Some(PUBLIC_JOB_TRIBE)
        || (stored_tribes.contains(PUBLIC_JOB_TRIBE)
            && current_tribe != Some(LEGACY_JOB_TRIBE))
    {
        PUBLIC_JOB_TRIBE
    } else {
        LEGACY_JOB_TRIBE
    };

    Ok(AgentTribeIdentityResolutionWire {
        tribe: Some(resolved.to_string()),
        display_keys: display_resolution.display_keys,
        diagnostics: Vec::new(),
    })
}

fn layer_tribes(
    layer: &AgentTribeDisplayLayerWire,
) -> Option<&Map<String, Value>> {
    layer
        .value
        .get("ace")?
        .as_object()?
        .get("tribes")?
        .as_object()
}

fn display_configs_equivalent(left: &Value, right: &Value) -> bool {
    left == right
}

fn conflicting_job_alias_diagnostic(
    layer: &AgentTribeDisplayLayerWire,
) -> AgentTribeDisplayDiagnosticWire {
    let source = layer.path.as_deref().unwrap_or(&layer.name);
    AgentTribeDisplayDiagnosticWire {
        code: "agent_tribe_job_alias_collision".to_string(),
        message: format!(
            "ace.tribes.{legacy} and ace.tribes.{public} both configure the \
             built-in automation tribe in layer {layer}; keep one spelling or \
             make the two records identical before @job can be displayed \
             unambiguously",
            legacy = LEGACY_JOB_TRIBE,
            public = PUBLIC_JOB_TRIBE,
            layer = layer.name,
        ),
        layer: layer.name.clone(),
        path: format!(
            "ace.tribes.{LEGACY_JOB_TRIBE}|ace.tribes.{PUBLIC_JOB_TRIBE}"
        ),
        source_path: format!(
            "{source}:ace.tribes.{legacy},{source}:ace.tribes.{public}",
            legacy = LEGACY_JOB_TRIBE,
            public = PUBLIC_JOB_TRIBE,
        ),
    }
}

fn cross_layer_job_alias_diagnostics(
    layers: &[AgentTribeDisplayLayerWire],
) -> Vec<AgentTribeDisplayDiagnosticWire> {
    let authored: Vec<(&AgentTribeDisplayLayerWire, &str)> = layers
        .iter()
        .filter(|layer| !is_builtin_layer(layer))
        .flat_map(|layer| {
            let keys: Vec<&str> = layer_tribes(layer)
                .map(|tribes| {
                    [LEGACY_JOB_TRIBE, PUBLIC_JOB_TRIBE]
                        .into_iter()
                        .filter(|key| tribes.contains_key(*key))
                        .collect()
                })
                .unwrap_or_default();
            keys.into_iter().map(move |key| (layer, key))
        })
        .collect();
    let legacy = authored
        .iter()
        .find(|(_, key)| *key == LEGACY_JOB_TRIBE)
        .map(|(layer, _)| *layer);
    let public = authored
        .iter()
        .find(|(_, key)| *key == PUBLIC_JOB_TRIBE)
        .map(|(layer, _)| *layer);
    match (legacy, public) {
        (Some(legacy_layer), Some(public_layer))
            if legacy_layer.name != public_layer.name =>
        {
            vec![cross_layer_job_alias_diagnostic(legacy_layer, public_layer)]
        }
        _ => Vec::new(),
    }
}

fn is_builtin_layer(layer: &AgentTribeDisplayLayerWire) -> bool {
    layer.kind == "builtin" || layer.name == "default"
}

fn cross_layer_job_alias_diagnostic(
    legacy_layer: &AgentTribeDisplayLayerWire,
    public_layer: &AgentTribeDisplayLayerWire,
) -> AgentTribeDisplayDiagnosticWire {
    let legacy_source =
        legacy_layer.path.as_deref().unwrap_or(&legacy_layer.name);
    let public_source =
        public_layer.path.as_deref().unwrap_or(&public_layer.name);
    AgentTribeDisplayDiagnosticWire {
        code: "agent_tribe_job_alias_collision".to_string(),
        message: format!(
            "ace.tribes.{legacy} and ace.tribes.{public} are authored in \
             separate non-built-in layers ({legacy_layer} and {public_layer}); \
             keep one spelling or make the historical {public} identity \
             explicit before @job can be resolved unambiguously",
            legacy = LEGACY_JOB_TRIBE,
            public = PUBLIC_JOB_TRIBE,
            legacy_layer = legacy_layer.name,
            public_layer = public_layer.name,
        ),
        layer: format!("{},{}", legacy_layer.name, public_layer.name),
        path: format!(
            "ace.tribes.{LEGACY_JOB_TRIBE}|ace.tribes.{PUBLIC_JOB_TRIBE}"
        ),
        source_path: format!(
            "{legacy_source}:ace.tribes.{legacy},{public_source}:ace.tribes.{public}",
            legacy = LEGACY_JOB_TRIBE,
            public = PUBLIC_JOB_TRIBE,
        ),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn public_job_input_canonicalizes_to_stored_chop() {
        assert_eq!(canonicalize_public_tribe_name("job").unwrap(), "chop");
        assert_eq!(parse_tribe_reference("@job").unwrap(), Some("chop".into()));
        assert_eq!(public_tribe_name("chop"), "job");
    }

    #[test]
    fn metadata_shape_normalization_preserves_stored_job() {
        let mut data = Map::new();
        data.insert("tribe".to_string(), Value::String("job".to_string()));
        data.insert("tag".to_string(), Value::String("chop".to_string()));

        let normalized = canonicalize_agent_tribe_metadata(data);

        assert_eq!(normalized.get("tribe"), Some(&Value::String("job".into())));
        assert!(!normalized.contains_key("tag"));
    }

    #[test]
    fn display_key_prefers_legacy_override_over_public_default() {
        assert_eq!(
            agent_tribe_display_key("chop", &["job".to_string()]).unwrap(),
            "job"
        );
        assert_eq!(
            agent_tribe_display_key(
                "chop",
                &["job".to_string(), "chop".to_string()]
            )
            .unwrap(),
            "chop"
        );
        assert_eq!(
            agent_tribe_display_key("job", &["job".to_string()]).unwrap(),
            "job"
        );
    }

    #[test]
    fn display_resolution_reports_same_layer_alias_collision() {
        let request = AgentTribeDisplayResolutionRequestWire {
            layers: vec![AgentTribeDisplayLayerWire {
                name: "user".to_string(),
                kind: "user".to_string(),
                path: Some("/tmp/sase.yml".to_string()),
                value: json!({
                    "ace": {
                        "tribes": {
                            "chop": {"icon": "C", "description": "Built-in"},
                            "job": {"icon": "J", "description": "Independent"}
                        }
                    }
                }),
            }],
        };

        let result = resolve_agent_tribe_display_config(&request);

        assert_eq!(
            result.display_keys.get("chop").map(String::as_str),
            Some("chop")
        );
        assert_eq!(result.diagnostics.len(), 1);
        let diagnostic = &result.diagnostics[0];
        assert_eq!(diagnostic.code, "agent_tribe_job_alias_collision");
        assert_eq!(diagnostic.layer, "user");
        assert!(diagnostic
            .source_path
            .contains("/tmp/sase.yml:ace.tribes.chop"));
        assert!(diagnostic
            .source_path
            .contains("/tmp/sase.yml:ace.tribes.job"));
    }

    #[test]
    fn display_resolution_allows_cross_layer_legacy_customization() {
        let request = AgentTribeDisplayResolutionRequestWire {
            layers: vec![
                AgentTribeDisplayLayerWire {
                    name: "default".to_string(),
                    kind: "builtin".to_string(),
                    path: None,
                    value: json!({"ace": {"tribes": {"job": {"icon": "J"}}}}),
                },
                AgentTribeDisplayLayerWire {
                    name: "user".to_string(),
                    kind: "user".to_string(),
                    path: Some("/tmp/sase.yml".to_string()),
                    value: json!({"ace": {"tribes": {"chop": {"icon": "C"}}}}),
                },
            ],
        };

        let result = resolve_agent_tribe_display_config(&request);

        assert!(result.diagnostics.is_empty());
        assert_eq!(
            result.display_keys.get("chop").map(String::as_str),
            Some("chop")
        );
    }

    #[test]
    fn identity_resolution_preserves_current_stored_job_assignment() {
        let result = resolve_agent_tribe_identity(
            &AgentTribeIdentityResolutionRequestWire {
                tribe: "job".to_string(),
                layers: Vec::new(),
                stored_tribes: vec!["job".to_string()],
                current_tribe: Some("job".to_string()),
            },
        )
        .unwrap();

        assert_eq!(result.tribe.as_deref(), Some("job"));
        assert!(result.diagnostics.is_empty());
    }

    #[test]
    fn identity_resolution_keeps_builtin_job_canonicalization_without_history()
    {
        let result = resolve_agent_tribe_identity(
            &AgentTribeIdentityResolutionRequestWire {
                tribe: "job".to_string(),
                layers: Vec::new(),
                stored_tribes: Vec::new(),
                current_tribe: None,
            },
        )
        .unwrap();

        assert_eq!(result.tribe.as_deref(), Some("chop"));
    }

    #[test]
    fn identity_resolution_rejects_source_ambiguous_job_aliases() {
        let result = resolve_agent_tribe_identity(
            &AgentTribeIdentityResolutionRequestWire {
                tribe: "job".to_string(),
                layers: vec![AgentTribeDisplayLayerWire {
                    name: "user".to_string(),
                    kind: "user".to_string(),
                    path: Some("/tmp/sase.yml".to_string()),
                    value: json!({
                        "ace": {
                            "tribes": {
                                "chop": {"icon": "C"},
                                "job": {"icon": "J"}
                            }
                        }
                    }),
                }],
                stored_tribes: Vec::new(),
                current_tribe: None,
            },
        )
        .unwrap();

        assert_eq!(result.tribe, None);
        assert_eq!(result.diagnostics.len(), 1);
    }
}

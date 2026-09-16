//! Layer merge + per-field provenance → the config inventory.
//!
//! Builds the source rail (one row per layer), the per-field effective value
//! with its full contribution stack (the `git config --show-origin`
//! analogue), and diagnostics (deprecated/unsupported keys, layer load
//! errors, and schema-validation issues against the effective merge).

use super::axe::{
    normalize_config_key_path, normalize_config_layer, public_project_root,
    remove_value_at_path, replacement_paths_for_layer, NormalizedConfigLayer,
};
use super::merge::{canonicalize_value, deep_merge_objects, get_at_path};
use super::schema::build_field_model;
use super::validate::validate_config;
use super::wire::{
    ConfigContributionWire, ConfigDiagnosticWire, ConfigError,
    ConfigFieldStateWire, ConfigInventoryRequestWire, ConfigInventoryWire,
    ConfigLayerInputWire, ConfigSourceWire, ListStrategy,
    CONFIG_WIRE_SCHEMA_VERSION,
};
use crate::agent_tribe::{
    resolve_agent_tribe_display_config, AgentTribeDisplayLayerWire,
    AgentTribeDisplayResolutionRequestWire,
};
use serde_json::Map;
use serde_json::Value;

/// Build the full config inventory from the schema + layer stack.
pub fn build_inventory(
    request: &ConfigInventoryRequestWire,
) -> Result<ConfigInventoryWire, ConfigError> {
    let model = build_field_model(&request.schema)?;
    let merged = merge_layers_for_general_view(
        &request.layers,
        request.routine_job_contract,
    );

    let mut diagnostics = merged.diagnostics;
    diagnostics.extend(agent_tribe_alias_diagnostics(&request.layers));
    let sources = build_sources(request, &mut diagnostics);

    let writable_layers: Vec<String> = request
        .layers
        .iter()
        .filter(|layer| layer.writable)
        .map(|layer| layer.name.clone())
        .collect();

    let mut fields = Vec::with_capacity(model.fields.len());
    for field in &model.fields {
        let segments: Vec<&str> = field.path.split('.').collect();
        let effective = get_at_path(&merged.view, &segments);
        let internal_key_path = normalize_config_key_path(
            &segments
                .iter()
                .map(|segment| (*segment).to_string())
                .collect::<Vec<_>>(),
        );
        let internal_segments: Vec<&str> =
            internal_key_path.iter().map(String::as_str).collect();

        let mut contributions = Vec::new();
        for (layer, normalized) in request.layers.iter().zip(&merged.layers) {
            if let Some(raw) =
                get_at_path(&normalized.value, &internal_segments)
            {
                contributions.push(ConfigContributionWire {
                    layer: layer.name.clone(),
                    raw_value: canonicalize_value(raw),
                    winning: false,
                });
            }
        }
        if let Some(last) = contributions.last_mut() {
            last.winning = true;
        }

        let deprecated_replacement = request
            .deprecations
            .get(&field.path)
            .cloned()
            .or_else(|| field.deprecated_replacement.clone());

        fields.push(ConfigFieldStateWire {
            path: field.path.clone(),
            has_default: field.has_default,
            default: field.default.clone(),
            has_effective: effective.is_some(),
            effective_value: effective.cloned().unwrap_or(Value::Null),
            contributions,
            deprecated_replacement,
            write_capabilities: writable_layers.clone(),
        });
    }

    diagnostics.extend(validate_config(&request.schema, &merged.view));

    Ok(ConfigInventoryWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        sources,
        fields,
        diagnostics,
    })
}

fn agent_tribe_alias_diagnostics(
    layers: &[ConfigLayerInputWire],
) -> Vec<ConfigDiagnosticWire> {
    let resolution = resolve_agent_tribe_display_config(
        &AgentTribeDisplayResolutionRequestWire {
            layers: layers
                .iter()
                .map(|layer| AgentTribeDisplayLayerWire {
                    name: layer.name.clone(),
                    kind: layer.kind.clone(),
                    path: layer.path.clone(),
                    value: layer.value.clone(),
                })
                .collect(),
        },
    );
    resolution
        .diagnostics
        .into_iter()
        .map(|diagnostic| ConfigDiagnosticWire {
            severity: "error".to_string(),
            code: diagnostic.code,
            message: diagnostic.message,
            path: Some(diagnostic.path),
            layer: Some(diagnostic.layer),
        })
        .collect()
}

struct GeneralMergedConfig {
    view: Value,
    layers: Vec<NormalizedConfigLayer>,
    diagnostics: Vec<ConfigDiagnosticWire>,
}

fn merge_layers_for_general_view(
    layers: &[ConfigLayerInputWire],
    routine_job_contract: bool,
) -> GeneralMergedConfig {
    let mut internal = Value::Object(Map::new());
    let mut normalized_layers = Vec::with_capacity(layers.len());
    let mut diagnostics = Vec::new();

    for layer in layers {
        let normalized =
            normalize_config_layer(layer, &internal, &mut diagnostics);
        if let Some(obj) = normalized.value.as_object() {
            let strategy = ListStrategy::from_token(&layer.list_strategy);
            for path in replacement_paths_for_layer(&normalized.value, strategy)
            {
                remove_value_at_path(&mut internal, &path);
            }
            let base = internal.as_object().cloned().unwrap_or_default();
            internal = Value::Object(deep_merge_objects(&base, obj, strategy));
        }
        normalized_layers.push(normalized);
    }

    let view = if routine_job_contract {
        public_project_root(&internal)
    } else {
        internal.clone()
    };

    GeneralMergedConfig {
        view: canonicalize_value(&view),
        layers: normalized_layers,
        diagnostics,
    }
}

fn build_sources(
    request: &ConfigInventoryRequestWire,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> Vec<ConfigSourceWire> {
    let mut sources = Vec::with_capacity(request.layers.len());
    for layer in &request.layers {
        let object = layer.value.as_object();
        let keys: Vec<String> = object
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();
        let exists = layer.exists.unwrap_or(object.is_some());

        let mut unsupported_keys: Vec<String> = keys
            .iter()
            .filter(|key| request.unsupported.contains(key))
            .cloned()
            .collect();
        unsupported_keys.sort();

        let mut deprecated_keys: Vec<String> = keys
            .iter()
            .filter(|key| request.deprecations.contains_key(*key))
            .cloned()
            .collect();
        deprecated_keys.sort();

        for key in &deprecated_keys {
            let replacement =
                request.deprecations.get(key).cloned().unwrap_or_default();
            diagnostics.push(ConfigDiagnosticWire {
                severity: "warning".to_string(),
                code: "deprecated_key".to_string(),
                message: format!(
                    "`{key}` is deprecated; use `{replacement}` instead"
                ),
                path: Some(key.clone()),
                layer: Some(layer.name.clone()),
            });
        }
        for key in &unsupported_keys {
            diagnostics.push(ConfigDiagnosticWire {
                severity: "warning".to_string(),
                code: "unsupported_key".to_string(),
                message: format!(
                    "`{key}` is not a recognized config key and is ignored"
                ),
                path: Some(key.clone()),
                layer: Some(layer.name.clone()),
            });
        }
        if layer.kind != "local" {
            for path in glossary_scope_paths(&layer.value) {
                diagnostics.push(ConfigDiagnosticWire {
                    severity: "error".to_string(),
                    code: "glossary_scope".to_string(),
                    message: format!(
                        "`{path}` is only valid in project-local sase.yml"
                    ),
                    path: Some(path),
                    layer: Some(layer.name.clone()),
                });
            }
        }
        if let Some(err) = &layer.error {
            diagnostics.push(ConfigDiagnosticWire {
                severity: "error".to_string(),
                code: "layer_error".to_string(),
                message: err.clone(),
                path: None,
                layer: Some(layer.name.clone()),
            });
        }

        sources.push(ConfigSourceWire {
            name: layer.name.clone(),
            kind: layer.kind.clone(),
            path: layer.path.clone(),
            exists,
            writable: layer.writable,
            list_strategy: layer.list_strategy.clone(),
            key_count: keys.len(),
            unsupported_keys,
            deprecated_keys,
            error: layer.error.clone(),
        });
    }
    sources
}

fn glossary_scope_paths(value: &Value) -> Vec<String> {
    let Some(object) = value.as_object() else {
        return Vec::new();
    };

    let mut paths = Vec::new();
    if object
        .get("memory")
        .and_then(Value::as_object)
        .is_some_and(|memory| memory.contains_key("glossary"))
    {
        paths.push("memory.glossary".to_string());
    }
    paths
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn layer(name: &str, value: Value) -> ConfigLayerInputWire {
        ConfigLayerInputWire {
            name: name.to_string(),
            kind: "user".to_string(),
            path: Some(format!("{name}.yml")),
            value,
            list_strategy: "replace".to_string(),
            writable: name == "user",
            exists: Some(true),
            error: None,
        }
    }

    fn canonical_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "axe": {
                    "type": "object",
                    "properties": {
                        "job_script_dirs": {"type": "array", "items": {"type": "string"}},
                        "routines": {
                            "type": "object",
                            "properties": {
                                "checks": {
                                    "type": "object",
                                    "properties": {
                                        "interval": {"type": "integer"},
                                        "job_timeout": {"type": "string"},
                                        "jobs": {
                                            "type": "object",
                                            "properties": {
                                                "hook": {
                                                    "type": "object",
                                                    "properties": {
                                                        "job_timeout": {"type": "string"}
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    fn legacy_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "axe": {
                    "type": "object",
                    "properties": {
                        "lumberjacks": {
                            "type": "object",
                            "properties": {
                                "checks": {
                                    "type": "object",
                                    "properties": {
                                        "interval": {"type": "integer"}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    fn field<'a>(
        inventory: &'a ConfigInventoryWire,
        path: &str,
    ) -> &'a ConfigFieldStateWire {
        inventory
            .fields
            .iter()
            .find(|field| field.path == path)
            .unwrap_or_else(|| panic!("missing field {path}"))
    }

    #[test]
    fn inventory_normalizes_axe_aliases_and_projects_public_contract() {
        let request = ConfigInventoryRequestWire {
            schema: canonical_schema(),
            layers: vec![
                layer(
                    "defaults",
                    json!({"axe": {"lumberjacks": {"checks": {
                        "interval": 5,
                        "chop_timeout": "30s",
                        "chops": {"hook": {"script": "sase_chop_test"}}
                    }}}}),
                ),
                layer(
                    "user",
                    json!({"axe": {
                        "job_script_dirs": ["jobs"],
                        "routines": {"checks": {
                            "interval": 19,
                            "job_timeout": "2m",
                            "jobs": {"hook": {"script": "sase_job_test"}}
                        }}
                    }}),
                ),
            ],
            deprecations: BTreeMap::new(),
            unsupported: Vec::new(),
            routine_job_contract: true,
        };

        let inventory = build_inventory(&request).unwrap();

        let interval = field(&inventory, "axe.routines.checks.interval");
        assert_eq!(interval.effective_value, json!(19));
        assert_eq!(interval.contributions.len(), 2);
        assert_eq!(interval.contributions[0].raw_value, json!(5));
        assert!(!interval.contributions[0].winning);
        assert_eq!(interval.contributions[1].raw_value, json!(19));
        assert!(interval.contributions[1].winning);

        let timeout = field(&inventory, "axe.routines.checks.job_timeout");
        assert_eq!(timeout.effective_value, json!("2m"));
        assert_eq!(timeout.contributions.len(), 2);
        assert_eq!(timeout.contributions[0].raw_value, json!("30s"));
        assert_eq!(timeout.contributions[1].raw_value, json!("2m"));

        let dirs = field(&inventory, "axe.job_script_dirs");
        assert_eq!(dirs.effective_value, json!(["jobs"]));
        assert!(
            inventory.diagnostics.is_empty(),
            "{:?}",
            inventory.diagnostics
        );
    }

    #[test]
    fn inventory_legacy_projection_still_merges_canonical_aliases() {
        let request = ConfigInventoryRequestWire {
            schema: legacy_schema(),
            layers: vec![
                layer(
                    "defaults",
                    json!({"axe": {"lumberjacks": {"checks": {"interval": 5}}}}),
                ),
                layer(
                    "user",
                    json!({"axe": {"routines": {"checks": {"interval": 19}}}}),
                ),
            ],
            deprecations: BTreeMap::new(),
            unsupported: Vec::new(),
            routine_job_contract: false,
        };

        let inventory = build_inventory(&request).unwrap();
        let interval = field(&inventory, "axe.lumberjacks.checks.interval");
        assert_eq!(interval.effective_value, json!(19));
        assert_eq!(interval.contributions.len(), 2);
    }

    #[test]
    fn inventory_reports_same_layer_conflicting_alias_paths() {
        let request = ConfigInventoryRequestWire {
            schema: canonical_schema(),
            layers: vec![layer(
                "user",
                json!({"axe": {
                    "lumberjacks": {"checks": {"interval": 5}},
                    "routines": {"checks": {"interval": 19}}
                }}),
            )],
            deprecations: BTreeMap::new(),
            unsupported: Vec::new(),
            routine_job_contract: true,
        };

        let inventory = build_inventory(&request).unwrap();

        let diagnostic = inventory
            .diagnostics
            .iter()
            .find(|item| item.code == "conflicting_axe_config_aliases")
            .expect("expected alias conflict diagnostic");
        assert_eq!(diagnostic.layer.as_deref(), Some("user:user.yml"));
        assert_eq!(
            diagnostic.path.as_deref(),
            Some("axe.lumberjacks.checks.interval")
        );
        assert!(diagnostic
            .message
            .contains("axe.lumberjacks.checks.interval"));
        assert!(diagnostic.message.contains("axe.routines.checks.interval"));
    }
}

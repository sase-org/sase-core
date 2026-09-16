//! Logical write-plan for a single config edit.
//!
//! "What to change" is shared domain logic and lives here: the target file,
//! the dotted key path, set-vs-unset, the resulting candidate merged config,
//! the effective-merge preview for the edited field, and schema validation of
//! the candidate. "Apply the change to bytes while preserving comments" is a
//! Python (`ruamel.yaml`) concern and is intentionally out of scope.

use serde_json::{Map, Value};

use super::axe::{
    display_path as display_config_path, normalize_config_key_path,
    normalize_config_layer, public_key_path, public_project_root,
    remove_value_at_path, replacement_paths_for_layer,
};
use super::merge::{
    canonicalize_value, deep_merge_objects, get_at_path, set_at_path,
    unset_at_path,
};
use super::validate::validate_config;
use super::wire::{
    ConfigDiagnosticWire, ConfigEditPlanWire, ConfigEditRequestWire,
    ConfigEffectivePreviewWire, ConfigError, ConfigLayerInputWire,
    ConfigWritePlanWire, ListStrategy, CONFIG_WIRE_SCHEMA_VERSION,
};

/// Plan a set/unset edit against the chosen target layer.
pub fn plan_edit(
    request: &ConfigEditRequestWire,
) -> Result<ConfigEditPlanWire, ConfigError> {
    let (requested_key_path, _display_path) = request.resolved_path()?;
    let merge_key_path = normalize_config_key_path(&requested_key_path);
    let view_key_path = if request.routine_job_contract {
        public_key_path(&merge_key_path)
    } else {
        merge_key_path.clone()
    };
    let display_path = display_config_path(&view_key_path);
    let target_idx = request
        .layers
        .iter()
        .position(|layer| layer.name == request.target_layer)
        .ok_or_else(|| {
            ConfigError::validation(format!(
                "unknown target layer `{}`",
                request.target_layer
            ))
        })?;

    let mut diagnostics = Vec::new();
    let target = &request.layers[target_idx];
    if !target.writable {
        diagnostics.push(ConfigDiagnosticWire {
            severity: "warning".to_string(),
            code: "target_not_writable".to_string(),
            message: format!("layer `{}` is not writable", target.name),
            path: Some(display_path.clone()),
            layer: Some(target.name.clone()),
        });
    }

    let mut target_obj: Map<String, Value> =
        target.value.as_object().cloned().unwrap_or_default();
    let mut target_normalization_diagnostics = Vec::new();
    let target_normalized = normalize_config_layer(
        target,
        &Value::Object(Map::new()),
        &mut target_normalization_diagnostics,
    );
    let write_key_path = target_normalized
        .sources
        .get(&merge_key_path)
        .cloned()
        .unwrap_or_else(|| {
            if request.routine_job_contract {
                public_key_path(&merge_key_path)
            } else {
                requested_key_path.clone()
            }
        });

    let (op, new_value, has_value) = match request.op.kind.as_str() {
        "set" => {
            let value = canonicalize_value(&request.op.value);
            set_at_path(&mut target_obj, &write_key_path, value.clone())?;
            ("set", value, true)
        }
        "unset" => {
            unset_at_path(&mut target_obj, &write_key_path);
            ("unset", Value::Null, false)
        }
        other => {
            return Err(ConfigError::validation(format!(
                "unknown op kind `{other}`"
            )));
        }
    };

    let original_merged = merge_layers_for_general_view(
        &request.layers,
        request.routine_job_contract,
    );
    let mut candidate_layers = request.layers.clone();
    candidate_layers[target_idx].value = Value::Object(target_obj);
    let candidate_merged = merge_layers_for_general_view(
        &candidate_layers,
        request.routine_job_contract,
    );
    diagnostics.extend(original_merged.diagnostics);
    diagnostics.extend(candidate_merged.diagnostics);

    let segments: Vec<&str> =
        view_key_path.iter().map(String::as_str).collect();
    let before = get_at_path(&original_merged.view, &segments).cloned();
    let after = get_at_path(&candidate_merged.view, &segments).cloned();
    let preview = ConfigEffectivePreviewWire {
        path: display_path,
        has_before: before.is_some(),
        before: before.clone().unwrap_or(Value::Null),
        has_after: after.is_some(),
        after: after.clone().unwrap_or(Value::Null),
        changed: before != after,
    };

    let validation = validate_config(&request.schema, &candidate_merged.view);

    let write_plan = ConfigWritePlanWire {
        file: target.path.clone(),
        layer: target.name.clone(),
        key_path: write_key_path,
        op: op.to_string(),
        has_value,
        new_value,
    };

    Ok(ConfigEditPlanWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        write_plan,
        candidate_config: candidate_merged.view,
        effective_preview: preview,
        validation,
        diagnostics,
    })
}

struct GeneralMergedConfig {
    view: Value,
    diagnostics: Vec<ConfigDiagnosticWire>,
}

fn merge_layers_for_general_view(
    layers: &[ConfigLayerInputWire],
    routine_job_contract: bool,
) -> GeneralMergedConfig {
    let mut internal = Value::Object(Map::new());
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
    }
    let view = if routine_job_contract {
        public_project_root(&internal)
    } else {
        internal
    };
    GeneralMergedConfig {
        view: canonicalize_value(&view),
        diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::wire::ConfigEditOpWire;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "axe": {
                    "type": "object",
                    "properties": {
                        "routines": {
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

    fn layer(name: &str, value: Value) -> ConfigLayerInputWire {
        ConfigLayerInputWire {
            name: name.to_string(),
            kind: "user".to_string(),
            path: Some(format!("{name}.yml")),
            value,
            list_strategy: "replace".to_string(),
            writable: true,
            exists: Some(true),
            error: None,
        }
    }

    #[test]
    fn edit_plan_targets_existing_legacy_source_for_public_alias() {
        let request = ConfigEditRequestWire {
            schema: schema(),
            layers: vec![layer(
                "user",
                json!({"axe": {"lumberjacks": {"checks": {"interval": 5}}}}),
            )],
            target_layer: "user".to_string(),
            path: Some("axe.routines.checks.interval".to_string()),
            key_path: None,
            op: ConfigEditOpWire {
                kind: "set".to_string(),
                value: json!(19),
            },
            deprecations: BTreeMap::new(),
            unsupported: Vec::new(),
            routine_job_contract: true,
        };

        let plan = plan_edit(&request).unwrap();

        assert_eq!(
            plan.write_plan.key_path,
            vec!["axe", "lumberjacks", "checks", "interval"]
        );
        assert_eq!(
            plan.candidate_config["axe"]["routines"]["checks"]["interval"],
            json!(19)
        );
        assert_eq!(plan.effective_preview.path, "axe.routines.checks.interval");
        assert_eq!(plan.effective_preview.before, json!(5));
        assert_eq!(plan.effective_preview.after, json!(19));
        assert!(plan.effective_preview.changed);
    }

    #[test]
    fn edit_plan_uses_canonical_path_for_new_public_alias() {
        let request = ConfigEditRequestWire {
            schema: schema(),
            layers: vec![layer("user", json!({}))],
            target_layer: "user".to_string(),
            path: Some("axe.routines.checks.interval".to_string()),
            key_path: None,
            op: ConfigEditOpWire {
                kind: "set".to_string(),
                value: json!(19),
            },
            deprecations: BTreeMap::new(),
            unsupported: Vec::new(),
            routine_job_contract: true,
        };

        let plan = plan_edit(&request).unwrap();

        assert_eq!(
            plan.write_plan.key_path,
            vec!["axe", "routines", "checks", "interval"]
        );
        assert_eq!(
            plan.candidate_config["axe"]["routines"]["checks"]["interval"],
            json!(19)
        );
        assert_eq!(plan.effective_preview.before, Value::Null);
        assert_eq!(plan.effective_preview.after, json!(19));
    }
}

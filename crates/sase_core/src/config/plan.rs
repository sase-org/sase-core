//! Logical write-plan for a single config edit.
//!
//! "What to change" is shared domain logic and lives here: the target file,
//! the dotted key path, set-vs-unset, the resulting candidate merged config,
//! the effective-merge preview for the edited field, and schema validation of
//! the candidate. "Apply the change to bytes while preserving comments" is a
//! Python (`ruamel.yaml`) concern and is intentionally out of scope.

use serde_json::{Map, Value};

use super::merge::{
    canonicalize_value, get_at_path, merge_layers, set_at_path, unset_at_path,
};
use super::validate::validate_config;
use super::wire::{
    ConfigDiagnosticWire, ConfigEditPlanWire, ConfigEditRequestWire,
    ConfigEffectivePreviewWire, ConfigError, ConfigWritePlanWire,
    CONFIG_WIRE_SCHEMA_VERSION,
};

/// Plan a set/unset edit against the chosen target layer.
pub fn plan_edit(
    request: &ConfigEditRequestWire,
) -> Result<ConfigEditPlanWire, ConfigError> {
    let (key_path, display_path) = request.resolved_path()?;
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

    let (op, new_value, has_value) = match request.op.kind.as_str() {
        "set" => {
            let value = canonicalize_value(&request.op.value);
            set_at_path(&mut target_obj, &key_path, value.clone())?;
            ("set", value, true)
        }
        "unset" => {
            unset_at_path(&mut target_obj, &key_path);
            ("unset", Value::Null, false)
        }
        other => {
            return Err(ConfigError::validation(format!(
                "unknown op kind `{other}`"
            )));
        }
    };

    let original_merged = merge_layers(&request.layers);
    let mut candidate_layers = request.layers.clone();
    candidate_layers[target_idx].value = Value::Object(target_obj);
    let candidate_merged = merge_layers(&candidate_layers);

    let segments: Vec<&str> = key_path.iter().map(String::as_str).collect();
    let before = get_at_path(&original_merged, &segments).cloned();
    let after = get_at_path(&candidate_merged, &segments).cloned();
    let preview = ConfigEffectivePreviewWire {
        path: display_path,
        has_before: before.is_some(),
        before: before.clone().unwrap_or(Value::Null),
        has_after: after.is_some(),
        after: after.clone().unwrap_or(Value::Null),
        changed: before != after,
    };

    let validation = validate_config(&request.schema, &candidate_merged);

    let write_plan = ConfigWritePlanWire {
        file: target.path.clone(),
        layer: target.name.clone(),
        key_path,
        op: op.to_string(),
        has_value,
        new_value,
    };

    Ok(ConfigEditPlanWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        write_plan,
        candidate_config: candidate_merged,
        effective_preview: preview,
        validation,
        diagnostics,
    })
}

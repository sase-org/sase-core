//! Layer merge + per-field provenance → the config inventory.
//!
//! Builds the source rail (one row per layer), the per-field effective value
//! with its full contribution stack (the `git config --show-origin`
//! analogue), and diagnostics (deprecated/unsupported keys, layer load
//! errors, and schema-validation issues against the effective merge).

use super::merge::{canonicalize_value, get_at_path, merge_layers};
use super::schema::build_field_model;
use super::validate::validate_config;
use super::wire::{
    ConfigContributionWire, ConfigDiagnosticWire, ConfigError,
    ConfigFieldStateWire, ConfigInventoryRequestWire, ConfigInventoryWire,
    ConfigSourceWire, CONFIG_WIRE_SCHEMA_VERSION,
};
use serde_json::Value;

/// Build the full config inventory from the schema + layer stack.
pub fn build_inventory(
    request: &ConfigInventoryRequestWire,
) -> Result<ConfigInventoryWire, ConfigError> {
    let model = build_field_model(&request.schema)?;
    let merged = merge_layers(&request.layers);

    let mut diagnostics = Vec::new();
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
        let effective = get_at_path(&merged, &segments);

        let mut contributions = Vec::new();
        for layer in &request.layers {
            if let Some(raw) = get_at_path(&layer.value, &segments) {
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

    diagnostics.extend(validate_config(&request.schema, &merged));

    Ok(ConfigInventoryWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        sources,
        fields,
        diagnostics,
    })
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

//! Exact-key AXE layer composition, inventory, and entry mutation planning.
//!
//! AXE's lumberjacks and chops are keyed entities rather than dotted scalar
//! paths. This module keeps their identities as segment vectors throughout
//! composition and planning so a key such as `checks.release` is never
//! confused with two nested keys.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::axe_chop::{
    expand_chop_targets, validate_axe_config, AxeConfigValidationRequestWire,
    ChopForEachConfigWire, ChopTargetExpansionRequestWire,
    CHOP_ENGINE_SCHEMA_VERSION,
};

use super::merge::{set_at_path, unset_at_path};
use super::validate::validate_config;
use super::wire::{
    ConfigDiagnosticWire, ConfigError, ConfigLayerInputWire,
    ConfigWritePlanWire, ListStrategy, CONFIG_WIRE_SCHEMA_VERSION,
};

const AXE: &str = "axe";
const LUMBERJACKS: &str = "lumberjacks";
const CHOPS: &str = "chops";

/// One exact config path and the layer that supplied its effective value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AxeFieldProvenanceWire {
    pub key_path: Vec<String>,
    pub path: String,
    pub layer: String,
}

/// Exact selector for a lumberjack or base chop.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AxeEntrySelectorWire {
    /// `lumberjack` or `chop`.
    pub kind: String,
    pub lumberjack: String,
    #[serde(default)]
    pub chop: Option<String>,
}

impl AxeEntrySelectorWire {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.lumberjack.is_empty() {
            return Err(ConfigError::validation(
                "lumberjack identity must not be empty",
            ));
        }
        match self.kind.as_str() {
            "lumberjack" if self.chop.is_none() => Ok(()),
            "chop"
                if self.chop.as_ref().is_some_and(|name| !name.is_empty()) =>
            {
                Ok(())
            }
            "lumberjack" => Err(ConfigError::validation(
                "lumberjack selector must not include a chop identity",
            )),
            "chop" => Err(ConfigError::validation(
                "chop selector requires a non-empty chop identity",
            )),
            other => Err(ConfigError::validation(format!(
                "unknown AXE selector kind `{other}`"
            ))),
        }
    }

    fn key_path(&self) -> Vec<String> {
        let mut path = vec![
            AXE.to_string(),
            LUMBERJACKS.to_string(),
            self.lumberjack.clone(),
        ];
        if let Some(chop) = self.chop.as_ref() {
            path.extend([CHOPS.to_string(), chop.clone()]);
        }
        path
    }
}

/// One writable layer's raw sparse contribution to an effective entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeRawContributionWire {
    pub layer: String,
    pub file: Option<String>,
    pub writable: bool,
    /// `absent`, `legacy_list`, or `keyed_map`.
    pub representation: String,
    pub has_value: bool,
    pub value: Value,
}

/// One effective lumberjack, base chop, or generated chop instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeInventoryEntryWire {
    pub selector: AxeEntrySelectorWire,
    pub key_path: Vec<String>,
    pub path: String,
    pub effective: Value,
    pub enabled: bool,
    pub mutable: bool,
    pub generated: bool,
    #[serde(default)]
    pub base_selector: Option<AxeEntrySelectorWire>,
    #[serde(default)]
    pub target_key: Option<String>,
    pub field_provenance: Vec<AxeFieldProvenanceWire>,
    pub contributions: Vec<AxeRawContributionWire>,
}

/// AXE composition request over the standard ordered config layer input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeConfigComposeRequestWire {
    #[serde(default)]
    pub layers: Vec<ConfigLayerInputWire>,
}

/// Effective AXE config, exact provenance, entity inventory, and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeConfigCompositionWire {
    pub schema_version: u32,
    pub effective_config: Value,
    pub provenance: Vec<AxeFieldProvenanceWire>,
    pub entries: Vec<AxeInventoryEntryWire>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
}

/// One ordered set/reset operation relative to the selected entity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeFieldOperationWire {
    pub kind: String,
    pub key_path: Vec<String>,
    #[serde(default)]
    pub value: Value,
}

/// Plan an add/edit of one exact AXE entity contribution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeEntryMutationRequestWire {
    pub schema: Value,
    #[serde(default)]
    pub layers: Vec<ConfigLayerInputWire>,
    pub target_layer: String,
    pub selector: AxeEntrySelectorWire,
    pub operations: Vec<AxeFieldOperationWire>,
}

/// Entity-level effective before/after preview.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeEntryPreviewWire {
    pub selector: AxeEntrySelectorWire,
    pub has_before: bool,
    pub before: Value,
    pub has_after: bool,
    pub after: Value,
    pub changed: bool,
}

/// Complete AXE mutation plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeEntryMutationPlanWire {
    pub schema_version: u32,
    pub write_plan: ConfigWritePlanWire,
    pub effective_preview: AxeEntryPreviewWire,
    pub candidate_config: Value,
    pub candidate_composition: AxeConfigCompositionWire,
    pub validation: Vec<ConfigDiagnosticWire>,
    pub axe_diagnostics: Vec<ConfigDiagnosticWire>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
    pub target_representation: String,
    pub promoted_legacy_list: bool,
}

type ExactProvenance = BTreeMap<Vec<String>, String>;

/// Compose all AXE layers with exact-key provenance and entity inventory.
pub fn compose_axe_config(
    request: &AxeConfigComposeRequestWire,
) -> Result<AxeConfigCompositionWire, ConfigError> {
    let (effective_config, exact_provenance, diagnostics) =
        compose_values(&request.layers)?;
    let provenance = provenance_wire(&exact_provenance);
    let entries =
        build_inventory(&effective_config, &request.layers, &exact_provenance);
    Ok(AxeConfigCompositionWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        effective_config,
        provenance,
        entries,
        diagnostics,
    })
}

/// Plan a sparse add/edit/reset against one writable AXE contribution.
pub fn plan_axe_entry_mutation(
    request: &AxeEntryMutationRequestWire,
) -> Result<AxeEntryMutationPlanWire, ConfigError> {
    request.selector.validate()?;
    if request.operations.is_empty() {
        return Err(ConfigError::validation(
            "AXE mutation requires at least one field operation",
        ));
    }
    for operation in &request.operations {
        if operation.key_path.is_empty()
            || operation.key_path.iter().any(String::is_empty)
        {
            return Err(ConfigError::validation(
                "AXE field operation paths must contain non-empty segments",
            ));
        }
        if !matches!(operation.kind.as_str(), "set" | "unset") {
            return Err(ConfigError::validation(format!(
                "unknown AXE field operation `{}`",
                operation.kind
            )));
        }
    }

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
    let target = &request.layers[target_idx];
    let mut diagnostics = Vec::new();
    if !target.writable {
        diagnostics.push(ConfigDiagnosticWire {
            severity: "warning".to_string(),
            code: "target_not_writable".to_string(),
            message: format!("layer `{}` is not writable", target.name),
            path: Some(display_path(&request.selector.key_path())),
            layer: Some(target.name.clone()),
        });
    }

    let original = compose_axe_config(&AxeConfigComposeRequestWire {
        layers: request.layers.clone(),
    })?;
    if let Some(entry) = original
        .entries
        .iter()
        .find(|entry| entry.selector == request.selector && !entry.mutable)
    {
        let base = entry
            .base_selector
            .as_ref()
            .and_then(|selector| selector.chop.as_deref())
            .unwrap_or("base chop");
        return Err(ConfigError::validation(format!(
            "generated chop `{}` is not independently mutable; edit `{base}` instead",
            request.selector.chop.as_deref().unwrap_or_default()
        )));
    }
    let before =
        selected_value(&original.effective_config, &request.selector).cloned();

    let mut candidate_layers = request.layers.clone();
    let target_value = &mut candidate_layers[target_idx].value;
    if !target_value.is_object() {
        *target_value = Value::Object(Map::new());
    }
    let root = target_value
        .as_object_mut()
        .expect("target value was normalized to an object");

    let (representation, promoted, write_key_path, contribution) =
        mutate_target_contribution(
            root,
            &request.selector,
            &request.operations,
        )?;

    let candidate = compose_axe_config(&AxeConfigComposeRequestWire {
        layers: candidate_layers,
    })?;
    let after =
        selected_value(&candidate.effective_config, &request.selector).cloned();
    let validation =
        validate_config(&request.schema, &candidate.effective_config);
    let axe_diagnostics = candidate.diagnostics.clone();
    let write_plan = ConfigWritePlanWire {
        file: target.path.clone(),
        layer: target.name.clone(),
        key_path: write_key_path,
        op: "set".to_string(),
        has_value: true,
        new_value: contribution,
    };

    Ok(AxeEntryMutationPlanWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        write_plan,
        effective_preview: AxeEntryPreviewWire {
            selector: request.selector.clone(),
            has_before: before.is_some(),
            before: before.clone().unwrap_or(Value::Null),
            has_after: after.is_some(),
            after: after.clone().unwrap_or(Value::Null),
            changed: before != after,
        },
        candidate_config: candidate.effective_config.clone(),
        candidate_composition: candidate,
        validation,
        axe_diagnostics,
        diagnostics,
        target_representation: representation,
        promoted_legacy_list: promoted,
    })
}

fn mutate_target_contribution(
    root: &mut Map<String, Value>,
    selector: &AxeEntrySelectorWire,
    operations: &[AxeFieldOperationWire],
) -> Result<(String, bool, Vec<String>, Value), ConfigError> {
    let lumberjacks = ensure_object(root, AXE, LUMBERJACKS)?;
    if selector.kind == "lumberjack" {
        let representation = if lumberjacks.contains_key(&selector.lumberjack) {
            "keyed_map"
        } else {
            "absent"
        };
        let current = lumberjacks
            .get(&selector.lumberjack)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut contribution = current;
        apply_operations(&mut contribution, operations)?;
        lumberjacks.insert(
            selector.lumberjack.clone(),
            Value::Object(contribution.clone()),
        );
        let path = selector.key_path();
        return Ok((
            representation.to_string(),
            false,
            path,
            Value::Object(contribution),
        ));
    }

    let lumberjack = lumberjacks
        .entry(selector.lumberjack.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    if !lumberjack.is_object() {
        return Err(ConfigError::validation(format!(
            "cannot edit lumberjack `{}` because its target contribution is not a mapping",
            selector.lumberjack
        )));
    }
    let lumberjack = lumberjack
        .as_object_mut()
        .expect("checked lumberjack object");
    let (representation, promoted) = match lumberjack.get_mut(CHOPS) {
        None => {
            lumberjack.insert(CHOPS.to_string(), Value::Object(Map::new()));
            ("absent".to_string(), false)
        }
        Some(raw_chops) if raw_chops.is_array() => {
            let keyed = normalize_chop_list(
                raw_chops.as_array().expect("checked array"),
            );
            *raw_chops = Value::Object(keyed);
            ("legacy_list".to_string(), true)
        }
        Some(raw_chops) if raw_chops.is_object() => {
            ("keyed_map".to_string(), false)
        }
        Some(_) => {
            return Err(ConfigError::validation(format!(
                "cannot edit `{}` chops because the target contribution is neither a list nor a map",
                selector.lumberjack
            )));
        }
    };
    let raw_chops = lumberjack
        .get_mut(CHOPS)
        .expect("chops inserted or already present");
    let chops = raw_chops.as_object_mut().expect("chops normalized to map");
    let chop_name = selector.chop.as_ref().expect("validated chop selector");
    let current = chops
        .get(chop_name)
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut contribution = current;
    apply_operations(&mut contribution, operations)?;
    chops.insert(chop_name.clone(), Value::Object(contribution.clone()));

    if promoted {
        let path = vec![
            AXE.to_string(),
            LUMBERJACKS.to_string(),
            selector.lumberjack.clone(),
            CHOPS.to_string(),
        ];
        Ok((representation, true, path, raw_chops.clone()))
    } else {
        Ok((
            representation,
            false,
            selector.key_path(),
            Value::Object(contribution),
        ))
    }
}

fn apply_operations(
    contribution: &mut Map<String, Value>,
    operations: &[AxeFieldOperationWire],
) -> Result<(), ConfigError> {
    for operation in operations {
        if operation.kind == "set" {
            set_at_path(
                contribution,
                &operation.key_path,
                operation.value.clone(),
            )?;
        } else {
            unset_at_path(contribution, &operation.key_path);
        }
    }
    Ok(())
}

fn ensure_object<'a>(
    root: &'a mut Map<String, Value>,
    parent: &str,
    child: &str,
) -> Result<&'a mut Map<String, Value>, ConfigError> {
    let parent_value = root
        .entry(parent.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(parent_obj) = parent_value.as_object_mut() else {
        return Err(ConfigError::validation(format!(
            "cannot edit `{parent}` because it is not a mapping"
        )));
    };
    let child_value = parent_obj
        .entry(child.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    child_value.as_object_mut().ok_or_else(|| {
        ConfigError::validation(format!(
            "cannot edit `{parent}.{child}` because it is not a mapping"
        ))
    })
}

fn compose_values(
    layers: &[ConfigLayerInputWire],
) -> Result<(Value, ExactProvenance, Vec<ConfigDiagnosticWire>), ConfigError> {
    let mut merged = Value::Object(Map::new());
    let mut provenance = ExactProvenance::new();
    let mut diagnostics = Vec::new();

    for layer in layers {
        let Some(raw_axe) = layer.value.get(AXE) else {
            continue;
        };
        let label = layer_label(layer);
        let raw_request = AxeConfigValidationRequestWire {
            schema_version: CHOP_ENGINE_SCHEMA_VERSION,
            config: serde_json::json!({"axe": raw_axe}),
            provenance: BTreeMap::from([(AXE.to_string(), label.clone())]),
        };
        diagnostics.extend(validate_axe_config(&raw_request).map_err(
            |error| {
                ConfigError::validation(format!(
                    "AXE validation failed: {error}"
                ))
            },
        )?);

        detect_cross_layer_list_duplicates(
            &merged,
            raw_axe,
            layer,
            &label,
            &mut diagnostics,
        );
        let mut normalized = serde_json::json!({"axe": raw_axe});
        let replacement_paths = normalize_layer_chops(&mut normalized, layer);
        for path in replacement_paths {
            remove_value_at_path(&mut merged, &path);
            clear_provenance(&mut provenance, &path);
        }
        merged = merge_with_provenance(
            &merged,
            &normalized,
            &[],
            &label,
            ListStrategy::from_token(&layer.list_strategy),
            &mut provenance,
        );
    }

    let dotted_provenance = provenance
        .iter()
        .map(|(path, layer)| (display_path(path), layer.clone()))
        .collect();
    let final_request = AxeConfigValidationRequestWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        config: merged.clone(),
        provenance: dotted_provenance,
    };
    diagnostics.extend(validate_axe_config(&final_request).map_err(
        |error| {
            ConfigError::validation(format!("AXE validation failed: {error}"))
        },
    )?);
    dedupe_diagnostics(&mut diagnostics);
    Ok((merged, provenance, diagnostics))
}

fn layer_label(layer: &ConfigLayerInputWire) -> String {
    match layer.path.as_deref() {
        Some(path) => format!("{}:{path}", layer.name),
        None => layer.name.clone(),
    }
}

fn normalize_layer_chops(
    value: &mut Value,
    layer: &ConfigLayerInputWire,
) -> Vec<Vec<String>> {
    let mut replacements = Vec::new();
    let Some(lumberjacks) = value
        .get_mut(AXE)
        .and_then(Value::as_object_mut)
        .and_then(|axe| axe.get_mut(LUMBERJACKS))
        .and_then(Value::as_object_mut)
    else {
        return replacements;
    };
    for (name, config) in lumberjacks {
        let Some(config) = config.as_object_mut() else {
            continue;
        };
        let Some(chops) = config.get_mut(CHOPS) else {
            continue;
        };
        let Some(list) = chops.as_array() else {
            continue;
        };
        *chops = Value::Object(normalize_chop_list(list));
        if ListStrategy::from_token(&layer.list_strategy)
            == ListStrategy::Replace
        {
            replacements.push(vec![
                AXE.to_string(),
                LUMBERJACKS.to_string(),
                name.clone(),
                CHOPS.to_string(),
            ]);
        }
    }
    replacements
}

fn normalize_chop_list(list: &[Value]) -> Map<String, Value> {
    let mut keyed = Map::new();
    for entry in list {
        let (Some(name), config) = (
            chop_identity(entry),
            match entry {
                Value::String(_) => Value::Object(Map::new()),
                Value::Object(_) => entry.clone(),
                _ => continue,
            },
        ) else {
            continue;
        };
        keyed.entry(name.to_string()).or_insert(config);
    }
    keyed
}

fn chop_identity(value: &Value) -> Option<&str> {
    match value {
        Value::String(name) => Some(name),
        Value::Object(config) => config.get("name").and_then(Value::as_str),
        _ => None,
    }
}

fn detect_cross_layer_list_duplicates(
    merged: &Value,
    raw_axe: &Value,
    layer: &ConfigLayerInputWire,
    label: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    if ListStrategy::from_token(&layer.list_strategy) == ListStrategy::Replace {
        return;
    }
    let Some(raw_lumberjacks) =
        raw_axe.get(LUMBERJACKS).and_then(Value::as_object)
    else {
        return;
    };
    for (lumberjack, raw_config) in raw_lumberjacks {
        let Some(raw_list) = raw_config.get(CHOPS).and_then(Value::as_array)
        else {
            continue;
        };
        let existing = merged
            .get(AXE)
            .and_then(|axe| axe.get(LUMBERJACKS))
            .and_then(|items| items.get(lumberjack))
            .and_then(|config| config.get(CHOPS))
            .and_then(Value::as_object);
        let Some(existing) = existing else {
            continue;
        };
        for (index, entry) in raw_list.iter().enumerate() {
            let Some(identity) = chop_identity(entry) else {
                continue;
            };
            if existing.contains_key(identity) {
                diagnostics.push(ConfigDiagnosticWire {
                    severity: "error".to_string(),
                    code: "duplicate_chop_identity".to_string(),
                    message: format!("duplicate chop identity `{identity}`"),
                    path: Some(format!(
                        "axe.lumberjacks.{lumberjack}.chops[{index}]"
                    )),
                    layer: Some(label.to_string()),
                });
            }
        }
    }
}

fn merge_with_provenance(
    base: &Value,
    over: &Value,
    path: &[String],
    layer: &str,
    strategy: ListStrategy,
    provenance: &mut ExactProvenance,
) -> Value {
    match (base, over) {
        (Value::Object(base_map), Value::Object(over_map)) => {
            let mut result = base_map.clone();
            provenance.insert(path.to_vec(), layer.to_string());
            for (key, over_value) in over_map {
                let mut child_path = path.to_vec();
                child_path.push(key.clone());
                let value = match result.get(key) {
                    Some(base_value) => merge_with_provenance(
                        base_value,
                        over_value,
                        &child_path,
                        layer,
                        strategy,
                        provenance,
                    ),
                    None => {
                        record_provenance(
                            over_value,
                            &child_path,
                            layer,
                            provenance,
                        );
                        over_value.clone()
                    }
                };
                result.insert(key.clone(), value);
            }
            Value::Object(result)
        }
        (Value::Array(base_list), Value::Array(over_list)) => {
            if strategy == ListStrategy::Replace {
                clear_provenance(provenance, path);
                record_provenance(over, path, layer, provenance);
                over.clone()
            } else {
                provenance.insert(path.to_vec(), layer.to_string());
                let mut result = base_list.clone();
                let offset = result.len();
                result.extend(over_list.iter().cloned());
                for (index, child) in over_list.iter().enumerate() {
                    let mut child_path = path.to_vec();
                    child_path.push(format!("[{}]", offset + index));
                    record_provenance(child, &child_path, layer, provenance);
                }
                Value::Array(result)
            }
        }
        _ => {
            clear_provenance(provenance, path);
            record_provenance(over, path, layer, provenance);
            over.clone()
        }
    }
}

fn record_provenance(
    value: &Value,
    path: &[String],
    layer: &str,
    provenance: &mut ExactProvenance,
) {
    provenance.insert(path.to_vec(), layer.to_string());
    if let Value::Object(map) = value {
        for (key, child) in map {
            let mut child_path = path.to_vec();
            child_path.push(key.clone());
            record_provenance(child, &child_path, layer, provenance);
        }
    } else if let Value::Array(items) = value {
        for (index, child) in items.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(format!("[{index}]"));
            record_provenance(child, &child_path, layer, provenance);
        }
    }
}

fn clear_provenance(provenance: &mut ExactProvenance, path: &[String]) {
    provenance.retain(|candidate, _| !candidate.starts_with(path));
}

fn remove_value_at_path(value: &mut Value, path: &[String]) {
    let Some((last, prefix)) = path.split_last() else {
        return;
    };
    let mut current = value;
    for segment in prefix {
        let Some(next) = current.get_mut(segment) else {
            return;
        };
        current = next;
    }
    if let Some(map) = current.as_object_mut() {
        map.remove(last);
    }
}

fn selected_value<'a>(
    config: &'a Value,
    selector: &AxeEntrySelectorWire,
) -> Option<&'a Value> {
    let lumberjack = config
        .get(AXE)?
        .get(LUMBERJACKS)?
        .get(&selector.lumberjack)?;
    match selector.chop.as_ref() {
        None => Some(lumberjack),
        Some(chop) => lumberjack.get(CHOPS)?.get(chop),
    }
}

fn provenance_wire(
    provenance: &ExactProvenance,
) -> Vec<AxeFieldProvenanceWire> {
    provenance
        .iter()
        .map(|(path, layer)| AxeFieldProvenanceWire {
            key_path: path.clone(),
            path: display_path(path),
            layer: layer.clone(),
        })
        .collect()
}

fn display_path(path: &[String]) -> String {
    let mut display = String::new();
    for segment in path {
        if segment.starts_with('[') {
            display.push_str(segment);
        } else {
            if !display.is_empty() {
                display.push('.');
            }
            display.push_str(segment);
        }
    }
    display
}

fn build_inventory(
    effective: &Value,
    layers: &[ConfigLayerInputWire],
    provenance: &ExactProvenance,
) -> Vec<AxeInventoryEntryWire> {
    let Some(lumberjacks) = effective
        .get(AXE)
        .and_then(|axe| axe.get(LUMBERJACKS))
        .and_then(Value::as_object)
    else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    for (lumberjack_name, lumberjack) in lumberjacks {
        let selector = AxeEntrySelectorWire {
            kind: "lumberjack".to_string(),
            lumberjack: lumberjack_name.clone(),
            chop: None,
        };
        let path = selector.key_path();
        entries.push(AxeInventoryEntryWire {
            selector: selector.clone(),
            key_path: path.clone(),
            path: display_path(&path),
            effective: lumberjack.clone(),
            enabled: true,
            mutable: true,
            generated: false,
            base_selector: None,
            target_key: None,
            field_provenance: entity_provenance(provenance, &path),
            contributions: writable_contributions(layers, &selector),
        });

        let Some(chops) = lumberjack.get(CHOPS).and_then(Value::as_object)
        else {
            continue;
        };
        for (chop_name, chop) in chops {
            let selector = AxeEntrySelectorWire {
                kind: "chop".to_string(),
                lumberjack: lumberjack_name.clone(),
                chop: Some(chop_name.clone()),
            };
            let path = selector.key_path();
            let enabled =
                chop.get("enabled").and_then(Value::as_bool).unwrap_or(true);
            let contributions = writable_contributions(layers, &selector);
            let field_provenance = entity_provenance(provenance, &path);
            entries.push(AxeInventoryEntryWire {
                selector: selector.clone(),
                key_path: path.clone(),
                path: display_path(&path),
                effective: chop.clone(),
                enabled,
                mutable: true,
                generated: false,
                base_selector: None,
                target_key: None,
                field_provenance: field_provenance.clone(),
                contributions: contributions.clone(),
            });
            if enabled {
                entries.extend(generated_entries(
                    &selector,
                    chop,
                    &field_provenance,
                    &contributions,
                ));
            }
        }
    }
    entries
}

fn entity_provenance(
    provenance: &ExactProvenance,
    prefix: &[String],
) -> Vec<AxeFieldProvenanceWire> {
    provenance
        .iter()
        .filter(|(path, _)| path.starts_with(prefix))
        .map(|(path, layer)| AxeFieldProvenanceWire {
            key_path: path.clone(),
            path: display_path(path),
            layer: layer.clone(),
        })
        .collect()
}

fn writable_contributions(
    layers: &[ConfigLayerInputWire],
    selector: &AxeEntrySelectorWire,
) -> Vec<AxeRawContributionWire> {
    layers
        .iter()
        .filter(|layer| layer.writable)
        .map(|layer| {
            let lumberjack = layer
                .value
                .get(AXE)
                .and_then(|axe| axe.get(LUMBERJACKS))
                .and_then(|items| items.get(&selector.lumberjack));
            let (representation, value) =
                if let Some(chop_name) = selector.chop.as_ref() {
                    let chops = lumberjack.and_then(|item| item.get(CHOPS));
                    match chops {
                        Some(Value::Array(list)) => (
                            "legacy_list",
                            list.iter().find(|item| {
                                chop_identity(item) == Some(chop_name)
                            }),
                        ),
                        Some(Value::Object(map)) => {
                            ("keyed_map", map.get(chop_name))
                        }
                        _ => ("absent", None),
                    }
                } else {
                    (
                        if lumberjack.is_some() {
                            "keyed_map"
                        } else {
                            "absent"
                        },
                        lumberjack,
                    )
                };
            AxeRawContributionWire {
                layer: layer.name.clone(),
                file: layer.path.clone(),
                writable: layer.writable,
                representation: representation.to_string(),
                has_value: value.is_some(),
                value: value.cloned().unwrap_or(Value::Null),
            }
        })
        .collect()
}

fn generated_entries(
    base_selector: &AxeEntrySelectorWire,
    chop: &Value,
    provenance: &[AxeFieldProvenanceWire],
    contributions: &[AxeRawContributionWire],
) -> Vec<AxeInventoryEntryWire> {
    let Some(for_each) = chop.get("for_each") else {
        return Vec::new();
    };
    let Ok(for_each) =
        serde_json::from_value::<ChopForEachConfigWire>(for_each.clone())
    else {
        return Vec::new();
    };
    let Some(base_name) = base_selector.chop.as_ref() else {
        return Vec::new();
    };
    let request = ChopTargetExpansionRequestWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        chop_name: base_name.clone(),
        for_each: Some(for_each),
        source_rows: Vec::new(),
    };
    let Ok(expansion) = expand_chop_targets(&request) else {
        return Vec::new();
    };
    expansion
        .instances
        .into_iter()
        .map(|instance| {
            let mut effective = chop.as_object().cloned().unwrap_or_default();
            effective.remove("for_each");
            deep_patch_map(&mut effective, &instance.overrides);
            let selector = AxeEntrySelectorWire {
                kind: "chop".to_string(),
                lumberjack: base_selector.lumberjack.clone(),
                chop: Some(instance.instance_id),
            };
            let path = selector.key_path();
            let base_path = base_selector.key_path();
            let mut generated_provenance: Vec<_> = provenance
                .iter()
                .filter_map(|item| {
                    let relative =
                        item.key_path.strip_prefix(base_path.as_slice())?;
                    let mut key_path = path.clone();
                    key_path.extend_from_slice(relative);
                    Some(AxeFieldProvenanceWire {
                        path: display_path(&key_path),
                        key_path,
                        layer: item.layer.clone(),
                    })
                })
                .collect();
            for (key, value) in &instance.overrides {
                let mut override_path = path.clone();
                override_path.push(key.clone());
                replace_provenance_tree(
                    value,
                    &override_path,
                    "for_each target override",
                    &mut generated_provenance,
                );
            }
            AxeInventoryEntryWire {
                selector,
                key_path: path.clone(),
                path: display_path(&path),
                effective: Value::Object(effective.clone()),
                enabled: effective
                    .get("enabled")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
                mutable: false,
                generated: true,
                base_selector: Some(base_selector.clone()),
                target_key: Some(instance.target_key),
                field_provenance: generated_provenance,
                contributions: contributions.to_vec(),
            }
        })
        .collect()
}

fn replace_provenance_tree(
    value: &Value,
    path: &[String],
    layer: &str,
    provenance: &mut Vec<AxeFieldProvenanceWire>,
) {
    provenance.retain(|item| !item.key_path.starts_with(path));
    provenance.push(AxeFieldProvenanceWire {
        key_path: path.to_vec(),
        path: display_path(path),
        layer: layer.to_string(),
    });
    if let Value::Object(map) = value {
        for (key, child) in map {
            let mut child_path = path.to_vec();
            child_path.push(key.clone());
            replace_provenance_tree(child, &child_path, layer, provenance);
        }
    }
}

fn deep_patch_map(
    base: &mut Map<String, Value>,
    over: &BTreeMap<String, Value>,
) {
    for (key, value) in over {
        if let (Some(base_map), Some(over_map)) = (
            base.get_mut(key).and_then(Value::as_object_mut),
            value.as_object(),
        ) {
            let ordered: BTreeMap<String, Value> = over_map
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            deep_patch_map(base_map, &ordered);
        } else {
            base.insert(key.clone(), value.clone());
        }
    }
}

fn dedupe_diagnostics(diagnostics: &mut Vec<ConfigDiagnosticWire>) {
    let mut seen = BTreeSet::new();
    diagnostics.retain(|item| {
        seen.insert((
            item.path.clone(),
            item.code.clone(),
            item.layer.clone(),
            item.message.clone(),
        ))
    });
    diagnostics.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.layer.cmp(&right.layer))
            .then_with(|| left.message.cmp(&right.message))
    });
}

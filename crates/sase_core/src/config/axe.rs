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
const ROUTINES: &str = "routines";
const JOBS: &str = "jobs";

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourcePath {
    layer: String,
    key_path: Vec<String>,
}

/// One exact config path and the layer that supplied its effective value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AxeFieldProvenanceWire {
    pub key_path: Vec<String>,
    pub path: String,
    pub source_key_path: Vec<String>,
    pub source_path: String,
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
                "routine identity must not be empty",
            ));
        }
        match self.kind.as_str() {
            "lumberjack" | "routine" if self.chop.is_none() => Ok(()),
            "chop" | "job"
                if self.chop.as_ref().is_some_and(|name| !name.is_empty()) =>
            {
                Ok(())
            }
            "lumberjack" | "routine" => Err(ConfigError::validation(
                "routine selector must not include a job identity",
            )),
            "chop" | "job" => Err(ConfigError::validation(
                "job selector requires a non-empty job identity",
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
    pub key_path: Vec<String>,
    pub path: String,
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
    #[serde(default)]
    pub require_descriptions: bool,
    #[serde(default)]
    pub require_description_shape: bool,
    #[serde(default)]
    pub routine_job_contract: bool,
}

/// Effective AXE config, exact provenance, entity inventory, and diagnostics.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AxeConfigCompositionWire {
    pub schema_version: u32,
    pub effective_config: Value,
    pub public_config: Value,
    pub provenance: Vec<AxeFieldProvenanceWire>,
    pub public_provenance: Vec<AxeFieldProvenanceWire>,
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
    #[serde(default)]
    pub require_descriptions: bool,
    #[serde(default)]
    pub require_description_shape: bool,
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

type ExactProvenance = BTreeMap<Vec<String>, SourcePath>;

/// Compose all AXE layers with exact-key provenance and entity inventory.
pub fn compose_axe_config(
    request: &AxeConfigComposeRequestWire,
) -> Result<AxeConfigCompositionWire, ConfigError> {
    let (effective_config, exact_provenance, diagnostics) = compose_values(
        &request.layers,
        request.require_descriptions,
        request.require_description_shape,
    )?;
    let provenance = provenance_wire(&exact_provenance);
    let (public_config, public_provenance) = if request.routine_job_contract {
        public_projection(&effective_config, &provenance)
    } else {
        (effective_config.clone(), provenance.clone())
    };
    let entries =
        build_inventory(&effective_config, &request.layers, &exact_provenance);
    Ok(AxeConfigCompositionWire {
        schema_version: CONFIG_WIRE_SCHEMA_VERSION,
        effective_config,
        public_config,
        provenance,
        public_provenance,
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
        require_descriptions: request.require_descriptions,
        require_description_shape: request.require_description_shape,
        routine_job_contract: false,
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
            .unwrap_or("base job");
        return Err(ConfigError::validation(format!(
            "generated job `{}` is not independently mutable; edit `{base}` instead",
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
        require_descriptions: request.require_descriptions,
        require_description_shape: request.require_description_shape,
        routine_job_contract: false,
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
    let routine_key = if selector.chop.is_some() {
        routine_key_for_job_edit(root, selector)
    } else {
        routine_key_for_routine_edit(root, selector)
    };
    let jobs_key = jobs_key_for_edit(root, selector, routine_key);
    let axe = ensure_map(root, AXE, AXE)?;
    let routines_display = format!("{AXE}.{routine_key}");
    let routines = ensure_map(axe, routine_key, &routines_display)?;
    if selector.chop.is_none() {
        let representation = if routines.contains_key(&selector.lumberjack) {
            "keyed_map"
        } else {
            "absent"
        };
        let current = routines
            .get(&selector.lumberjack)
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let mut contribution = current;
        apply_operations(&mut contribution, operations)?;
        routines.insert(
            selector.lumberjack.clone(),
            Value::Object(contribution.clone()),
        );
        let path = vec![
            AXE.to_string(),
            routine_key.to_string(),
            selector.lumberjack.clone(),
        ];
        return Ok((
            representation.to_string(),
            false,
            path,
            Value::Object(contribution),
        ));
    }

    let lumberjack = routines
        .entry(selector.lumberjack.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    if !lumberjack.is_object() {
        return Err(ConfigError::validation(format!(
            "cannot edit routine `{}` because its target contribution is not a mapping",
            selector.lumberjack
        )));
    }
    let lumberjack = lumberjack
        .as_object_mut()
        .expect("checked lumberjack object");
    let (representation, promoted) = match lumberjack.get_mut(jobs_key) {
        None => {
            lumberjack.insert(jobs_key.to_string(), Value::Object(Map::new()));
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
                "cannot edit `{}` jobs because the target contribution is neither a list nor a map",
                selector.lumberjack
            )));
        }
    };
    let raw_chops = lumberjack
        .get_mut(jobs_key)
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
            routine_key.to_string(),
            selector.lumberjack.clone(),
            jobs_key.to_string(),
        ];
        Ok((representation, true, path, raw_chops.clone()))
    } else {
        let path = vec![
            AXE.to_string(),
            routine_key.to_string(),
            selector.lumberjack.clone(),
            jobs_key.to_string(),
            chop_name.clone(),
        ];
        Ok((representation, false, path, Value::Object(contribution)))
    }
}

fn routine_key_for_routine_edit(
    root: &Map<String, Value>,
    selector: &AxeEntrySelectorWire,
) -> &'static str {
    let Some(axe) = root.get(AXE).and_then(Value::as_object) else {
        return ROUTINES;
    };
    if axe
        .get(LUMBERJACKS)
        .and_then(Value::as_object)
        .is_some_and(|items| items.contains_key(&selector.lumberjack))
    {
        LUMBERJACKS
    } else {
        ROUTINES
    }
}

fn routine_key_for_job_edit(
    root: &Map<String, Value>,
    selector: &AxeEntrySelectorWire,
) -> &'static str {
    let Some(chop_name) = selector.chop.as_deref() else {
        return routine_key_for_routine_edit(root, selector);
    };
    for routine_key in [LUMBERJACKS, ROUTINES] {
        if routine_contains_job(
            root,
            routine_key,
            &selector.lumberjack,
            chop_name,
        ) {
            return routine_key;
        }
    }
    ROUTINES
}

fn jobs_key_for_edit(
    root: &Map<String, Value>,
    selector: &AxeEntrySelectorWire,
    routine_key: &str,
) -> &'static str {
    let Some(chop_name) = selector.chop.as_deref() else {
        return JOBS;
    };
    let Some(routine) = root
        .get(AXE)
        .and_then(Value::as_object)
        .and_then(|axe| axe.get(routine_key))
        .and_then(Value::as_object)
        .and_then(|routines| routines.get(&selector.lumberjack))
    else {
        return JOBS;
    };
    if collection_contains_job(routine.get(CHOPS), chop_name) {
        CHOPS
    } else {
        JOBS
    }
}

fn routine_contains_job(
    root: &Map<String, Value>,
    routine_key: &str,
    routine_name: &str,
    chop_name: &str,
) -> bool {
    let Some(routine) = root
        .get(AXE)
        .and_then(Value::as_object)
        .and_then(|axe| axe.get(routine_key))
        .and_then(Value::as_object)
        .and_then(|routines| routines.get(routine_name))
    else {
        return false;
    };
    collection_contains_job(routine.get(CHOPS), chop_name)
        || collection_contains_job(routine.get(JOBS), chop_name)
}

fn collection_contains_job(
    collection: Option<&Value>,
    chop_name: &str,
) -> bool {
    match collection {
        Some(Value::Array(list)) => list
            .iter()
            .any(|item| chop_identity(item) == Some(chop_name)),
        Some(Value::Object(map)) => map.contains_key(chop_name),
        _ => false,
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

fn ensure_map<'a>(
    root: &'a mut Map<String, Value>,
    key: &str,
    display: &str,
) -> Result<&'a mut Map<String, Value>, ConfigError> {
    let value = root
        .entry(key.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    value.as_object_mut().ok_or_else(|| {
        ConfigError::validation(format!(
            "cannot edit `{display}` because it is not a mapping"
        ))
    })
}

fn compose_values(
    layers: &[ConfigLayerInputWire],
    require_descriptions: bool,
    require_description_shape: bool,
) -> Result<(Value, ExactProvenance, Vec<ConfigDiagnosticWire>), ConfigError> {
    let mut merged = Value::Object(Map::new());
    let mut provenance = ExactProvenance::new();
    let mut diagnostics = Vec::new();

    for layer in layers {
        let Some(raw_axe) = layer.value.get(AXE) else {
            continue;
        };
        let label = layer_label(layer);
        detect_cross_layer_list_duplicates(
            &merged,
            raw_axe,
            layer,
            &label,
            &mut diagnostics,
        );
        let normalized_layer =
            normalize_layer_axe(raw_axe, &label, &mut diagnostics);
        let raw_request = AxeConfigValidationRequestWire {
            schema_version: CHOP_ENGINE_SCHEMA_VERSION,
            config: normalized_layer.value.clone(),
            require_descriptions: false,
            require_description_shape: false,
            provenance: BTreeMap::from([(AXE.to_string(), label.clone())]),
        };
        let mut layer_diagnostics =
            validate_axe_config(&raw_request).map_err(|error| {
                ConfigError::validation(format!(
                    "AXE validation failed: {error}"
                ))
            })?;
        remap_diagnostics_to_source_paths(
            &mut layer_diagnostics,
            &normalized_layer.sources,
        );
        diagnostics.extend(layer_diagnostics);

        for path in replacement_paths_for_layer(
            &normalized_layer.value,
            ListStrategy::from_token(&layer.list_strategy),
        ) {
            remove_value_at_path(&mut merged, &path);
            clear_provenance(&mut provenance, &path);
        }
        merged = merge_with_provenance(
            &merged,
            &normalized_layer.value,
            &[],
            &label,
            ListStrategy::from_token(&layer.list_strategy),
            &normalized_layer.sources,
            &mut provenance,
        );
    }

    let dotted_provenance = provenance
        .iter()
        .map(|(path, source)| (display_path(path), source.layer.clone()))
        .collect();
    let final_request = AxeConfigValidationRequestWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        config: merged.clone(),
        require_descriptions,
        require_description_shape,
        provenance: dotted_provenance,
    };
    let mut final_diagnostics =
        validate_axe_config(&final_request).map_err(|error| {
            ConfigError::validation(format!("AXE validation failed: {error}"))
        })?;
    remap_diagnostics_to_effective_source_paths(
        &mut final_diagnostics,
        &provenance,
    );
    diagnostics.extend(final_diagnostics);
    dedupe_diagnostics(&mut diagnostics);
    Ok((merged, provenance, diagnostics))
}

fn layer_label(layer: &ConfigLayerInputWire) -> String {
    match layer.path.as_deref() {
        Some(path) => format!("{}:{path}", layer.name),
        None => layer.name.clone(),
    }
}

fn remap_diagnostics_to_source_paths(
    diagnostics: &mut [ConfigDiagnosticWire],
    sources: &BTreeMap<Vec<String>, Vec<String>>,
) {
    let display_sources: BTreeMap<String, String> = sources
        .iter()
        .map(|(path, source)| (display_path(path), display_path(source)))
        .collect();
    for diagnostic in diagnostics {
        let Some(path) = diagnostic.path.as_ref() else {
            continue;
        };
        if let Some(source_path) = display_sources.get(path) {
            diagnostic.path = Some(source_path.clone());
        }
    }
}

fn remap_diagnostics_to_effective_source_paths(
    diagnostics: &mut [ConfigDiagnosticWire],
    provenance: &ExactProvenance,
) {
    let display_sources = provenance
        .iter()
        .map(|(path, source)| {
            (display_path(path), display_path(&source.key_path))
        })
        .collect::<BTreeMap<_, _>>();
    for diagnostic in diagnostics {
        let Some(path) = diagnostic.path.as_ref() else {
            continue;
        };
        let Some((normalized, source)) = display_sources
            .iter()
            .filter(|(normalized, _)| {
                path.as_str() == normalized.as_str()
                    || path.strip_prefix(normalized.as_str()).is_some_and(
                        |suffix| {
                            suffix.starts_with('.') || suffix.starts_with('[')
                        },
                    )
            })
            .max_by_key(|(normalized, _)| normalized.len())
        else {
            continue;
        };
        let suffix = &path[normalized.len()..];
        diagnostic.path = Some(format!("{source}{suffix}"));
    }
}

#[derive(Debug, Clone)]
struct NormalizedNode {
    value: Value,
    sources: BTreeMap<Vec<String>, Vec<String>>,
}

#[derive(Debug, Clone)]
pub(super) struct NormalizedConfigLayer {
    pub(super) value: Value,
    pub(super) sources: BTreeMap<Vec<String>, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) struct GenericAxeWritePlan {
    pub(super) key_path: Vec<String>,
    pub(super) op: String,
    pub(super) has_value: bool,
    pub(super) new_value: Value,
}

#[derive(Debug, Clone, Copy)]
enum NormalizeContext {
    Axe,
    Routines,
    Routine,
    Jobs,
    Generic,
}

fn normalize_layer_axe(
    raw_axe: &Value,
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> NormalizedNode {
    let axe_path = vec![AXE.to_string()];
    let axe = normalize_value(
        raw_axe,
        NormalizeContext::Axe,
        &axe_path,
        &axe_path,
        layer,
        diagnostics,
    );
    let mut root = Map::new();
    root.insert(AXE.to_string(), axe.value);
    let mut sources = axe.sources;
    sources.insert(Vec::new(), Vec::new());
    NormalizedNode {
        value: Value::Object(root),
        sources,
    }
}

pub(super) fn normalize_config_layer(
    layer: &ConfigLayerInputWire,
    merged_so_far: &Value,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> NormalizedConfigLayer {
    let label = layer_label(layer);
    if let Some(raw_axe) = layer.value.get(AXE) {
        detect_cross_layer_list_duplicates(
            merged_so_far,
            raw_axe,
            layer,
            &label,
            diagnostics,
        );
    }

    let Some(root) = layer.value.as_object() else {
        let node = copy_with_sources(&layer.value, &[], &[]);
        return NormalizedConfigLayer {
            value: node.value,
            sources: node.sources,
        };
    };

    let mut result = Map::new();
    let mut sources = source_map(&[], &[]);
    for (key, child) in root {
        let norm_path = vec![key.clone()];
        let source_path = vec![key.clone()];
        let node = if key == AXE {
            normalize_value(
                child,
                NormalizeContext::Axe,
                &norm_path,
                &source_path,
                &label,
                diagnostics,
            )
        } else {
            copy_with_sources(child, &norm_path, &source_path)
        };
        sources.extend(node.sources);
        result.insert(key.clone(), node.value);
    }

    NormalizedConfigLayer {
        value: Value::Object(result),
        sources,
    }
}

pub(super) fn normalize_config_key_path(path: &[String]) -> Vec<String> {
    if !matches!(path.first(), Some(root) if root == AXE) {
        return path.to_vec();
    }
    let mut normalized = Vec::with_capacity(path.len());
    normalized.push(AXE.to_string());
    if let Some(key) = path.get(1) {
        normalized.push(axe_key_alias(key).to_string());
    }
    if path.len() >= 3 {
        normalized.push(path[2].clone());
    }
    if path.len() >= 4
        && normalized.get(1).is_some_and(|key| key == LUMBERJACKS)
    {
        normalized.push(routine_key_alias(&path[3]).to_string());
        normalized.extend_from_slice(&path[4..]);
    } else if path.len() > 3 {
        normalized.extend_from_slice(&path[3..]);
    }
    normalized
}

pub(super) fn source_path_for_normalized_edit(
    normalized_path: &[String],
    requested_path: &[String],
    sources: &BTreeMap<Vec<String>, Vec<String>>,
    routine_job_contract: bool,
) -> Vec<String> {
    if let Some(source) = sources.get(normalized_path) {
        return source.clone();
    }

    let Some((normalized_prefix, source_prefix)) = sources
        .iter()
        .filter(|(path, _)| {
            !path.is_empty() && normalized_path.starts_with(path.as_slice())
        })
        .max_by_key(|(path, _)| path.len())
    else {
        return fallback_public_or_requested_path(
            normalized_path,
            requested_path,
            routine_job_contract,
        );
    };

    let mut result = source_prefix.clone();
    for index in normalized_prefix.len()..normalized_path.len() {
        result.push(source_segment_for_normalized_path(
            index,
            normalized_path,
            requested_path,
            source_prefix,
        ));
    }
    result
}

pub(super) fn plan_generic_axe_list_edit(
    root: &mut Map<String, Value>,
    normalized_path: &[String],
    op_kind: &str,
    value: Value,
) -> Result<Option<GenericAxeWritePlan>, ConfigError> {
    let Some((field_path, chop_name, routine_name)) =
        split_job_field_path(normalized_path)
    else {
        return Ok(None);
    };
    if field_path.is_empty() {
        return Ok(None);
    }
    if !target_has_list_job(root, routine_name, chop_name) {
        return Ok(None);
    }

    let operation = AxeFieldOperationWire {
        kind: op_kind.to_string(),
        key_path: field_path.to_vec(),
        value,
    };
    let selector = AxeEntrySelectorWire {
        kind: "chop".to_string(),
        lumberjack: routine_name.to_string(),
        chop: Some(chop_name.to_string()),
    };
    let (_representation, _promoted, key_path, contribution) =
        mutate_target_contribution(root, &selector, &[operation])?;
    Ok(Some(GenericAxeWritePlan {
        key_path,
        op: "set".to_string(),
        has_value: true,
        new_value: contribution,
    }))
}

fn fallback_public_or_requested_path(
    normalized_path: &[String],
    requested_path: &[String],
    routine_job_contract: bool,
) -> Vec<String> {
    if routine_job_contract {
        public_key_path(normalized_path)
    } else {
        requested_path.to_vec()
    }
}

fn source_segment_for_normalized_path(
    index: usize,
    normalized_path: &[String],
    requested_path: &[String],
    source_prefix: &[String],
) -> String {
    let normalized = &normalized_path[index];
    if normalized_path.first().map(String::as_str) != Some(AXE) {
        return requested_segment_or_normalized(
            index,
            normalized_path,
            requested_path,
        );
    }

    if index == 1 {
        return requested_segment_or_normalized(
            index,
            normalized_path,
            requested_path,
        );
    }

    if normalized_path.get(1).map(String::as_str) == Some(LUMBERJACKS)
        && index == 3
    {
        if source_prefix.get(1).map(String::as_str) == Some(ROUTINES) {
            return public_routine_key(normalized).to_string();
        }
        return normalized.clone();
    }

    requested_segment_or_normalized(index, normalized_path, requested_path)
}

fn requested_segment_or_normalized(
    index: usize,
    normalized_path: &[String],
    requested_path: &[String],
) -> String {
    let Some(requested) = requested_path.get(index) else {
        return normalized_path[index].clone();
    };
    if normalize_config_key_path(&requested_path[..=index]).as_slice()
        == &normalized_path[..=index]
    {
        requested.clone()
    } else {
        normalized_path[index].clone()
    }
}

fn split_job_field_path(path: &[String]) -> Option<(&[String], &str, &str)> {
    if path.len() < 6
        || path.first().map(String::as_str) != Some(AXE)
        || path.get(1).map(String::as_str) != Some(LUMBERJACKS)
        || path.get(3).map(String::as_str) != Some(CHOPS)
    {
        return None;
    }
    let routine = path.get(2)?;
    let chop = path.get(4)?;
    Some((&path[5..], chop.as_str(), routine.as_str()))
}

fn target_has_list_job(
    root: &Map<String, Value>,
    routine_name: &str,
    chop_name: &str,
) -> bool {
    [LUMBERJACKS, ROUTINES].into_iter().any(|routine_key| {
        [CHOPS, JOBS].into_iter().any(|jobs_key| {
            root.get(AXE)
                .and_then(Value::as_object)
                .and_then(|axe| axe.get(routine_key))
                .and_then(Value::as_object)
                .and_then(|routines| routines.get(routine_name))
                .and_then(|routine| routine.get(jobs_key))
                .and_then(Value::as_array)
                .is_some_and(|items| {
                    items
                        .iter()
                        .any(|item| chop_identity(item) == Some(chop_name))
                })
        })
    })
}

fn normalize_value(
    value: &Value,
    context: NormalizeContext,
    norm_path: &[String],
    source_path: &[String],
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> NormalizedNode {
    match context {
        NormalizeContext::Axe => normalize_object(
            value,
            norm_path,
            source_path,
            layer,
            diagnostics,
            axe_key_alias,
            |key| {
                if key == LUMBERJACKS {
                    NormalizeContext::Routines
                } else {
                    NormalizeContext::Generic
                }
            },
        ),
        NormalizeContext::Routines => {
            let Some(map) = value.as_object() else {
                return copy_with_sources(value, norm_path, source_path);
            };
            let mut result = Map::new();
            let mut sources = source_map(norm_path, source_path);
            for (name, child) in map {
                let child_norm = child_path(norm_path, name);
                let child_source = child_path(source_path, name);
                let node = normalize_value(
                    child,
                    NormalizeContext::Routine,
                    &child_norm,
                    &child_source,
                    layer,
                    diagnostics,
                );
                insert_normalized_child(
                    &mut result,
                    &mut sources,
                    name.clone(),
                    node,
                    &child_norm,
                    layer,
                    diagnostics,
                );
            }
            NormalizedNode {
                value: Value::Object(result),
                sources,
            }
        }
        NormalizeContext::Routine => {
            detect_unequal_synonym_lists(
                value,
                norm_path,
                source_path,
                layer,
                diagnostics,
            );
            normalize_object(
                value,
                norm_path,
                source_path,
                layer,
                diagnostics,
                routine_key_alias,
                |key| {
                    if key == CHOPS {
                        NormalizeContext::Jobs
                    } else {
                        NormalizeContext::Generic
                    }
                },
            )
        }
        NormalizeContext::Jobs => {
            normalize_jobs(value, norm_path, source_path, layer, diagnostics)
        }
        NormalizeContext::Generic => {
            copy_with_sources(value, norm_path, source_path)
        }
    }
}

fn normalize_object(
    value: &Value,
    norm_path: &[String],
    source_path: &[String],
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
    alias: fn(&str) -> &str,
    context_for_key: fn(&str) -> NormalizeContext,
) -> NormalizedNode {
    let Some(map) = value.as_object() else {
        return copy_with_sources(value, norm_path, source_path);
    };
    let mut result = Map::new();
    let mut sources = source_map(norm_path, source_path);
    for (key, child) in map {
        let normalized_key = alias(key);
        let child_norm = child_path(norm_path, normalized_key);
        let child_source = child_path(source_path, key);
        let node = normalize_value(
            child,
            context_for_key(normalized_key),
            &child_norm,
            &child_source,
            layer,
            diagnostics,
        );
        insert_normalized_child(
            &mut result,
            &mut sources,
            normalized_key.to_string(),
            node,
            &child_norm,
            layer,
            diagnostics,
        );
    }
    NormalizedNode {
        value: Value::Object(result),
        sources,
    }
}

fn axe_key_alias(key: &str) -> &str {
    match key {
        ROUTINES => LUMBERJACKS,
        "job_script_dirs" => "chop_script_dirs",
        "routine_log_max_bytes" => "lumberjack_log_max_bytes",
        "routine_log_temp_max_age_seconds" => {
            "lumberjack_log_temp_max_age_seconds"
        }
        "routine_restart_backoff_max_seconds" => {
            "lumberjack_restart_backoff_max_seconds"
        }
        "verbose_routine_diagnostics" => "verbose_lumberjack_diagnostics",
        _ => key,
    }
}

fn routine_key_alias(key: &str) -> &str {
    match key {
        JOBS => CHOPS,
        "job_timeout" => "chop_timeout",
        _ => key,
    }
}

fn normalize_jobs(
    value: &Value,
    norm_path: &[String],
    source_path: &[String],
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> NormalizedNode {
    match value {
        Value::Array(items) => {
            let mut result = Map::new();
            let mut sources = source_map(norm_path, source_path);
            let mut identities = BTreeSet::new();
            for (index, item) in items.iter().enumerate() {
                let item_source = indexed_path(source_path, index);
                let Some(identity) = chop_identity(item) else {
                    let path = display_path(&item_source);
                    diagnostics.push(ConfigDiagnosticWire {
                        severity: "error".to_string(),
                        code: "type_mismatch".to_string(),
                        message: "list-form jobs must be strings or objects"
                            .to_string(),
                        path: Some(path),
                        layer: Some(layer.to_string()),
                    });
                    continue;
                };
                if !identities.insert(identity.to_string()) {
                    diagnostics.push(ConfigDiagnosticWire {
                        severity: "error".to_string(),
                        code: "duplicate_chop_identity".to_string(),
                        message: format!("duplicate job identity `{identity}`"),
                        path: Some(display_path(&item_source)),
                        layer: Some(layer.to_string()),
                    });
                    continue;
                }
                let config = match item {
                    Value::String(_) => Value::Object(Map::new()),
                    Value::Object(_) => item.clone(),
                    _ => continue,
                };
                let item_norm = child_path(norm_path, identity);
                let node = copy_with_sources(&config, &item_norm, &item_source);
                insert_normalized_child(
                    &mut result,
                    &mut sources,
                    identity.to_string(),
                    node,
                    &item_norm,
                    layer,
                    diagnostics,
                );
            }
            NormalizedNode {
                value: Value::Object(result),
                sources,
            }
        }
        Value::Object(map) => {
            let mut result = Map::new();
            let mut sources = source_map(norm_path, source_path);
            for (name, child) in map {
                let child_norm = child_path(norm_path, name);
                let child_source = child_path(source_path, name);
                let node = copy_with_sources(child, &child_norm, &child_source);
                insert_normalized_child(
                    &mut result,
                    &mut sources,
                    name.clone(),
                    node,
                    &child_norm,
                    layer,
                    diagnostics,
                );
            }
            NormalizedNode {
                value: Value::Object(result),
                sources,
            }
        }
        _ => copy_with_sources(value, norm_path, source_path),
    }
}

fn detect_unequal_synonym_lists(
    value: &Value,
    norm_path: &[String],
    source_path: &[String],
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let Some(map) = value.as_object() else {
        return;
    };
    let (Some(legacy), Some(canonical)) = (map.get(CHOPS), map.get(JOBS))
    else {
        return;
    };
    if legacy.is_array() && canonical.is_array() && legacy != canonical {
        diagnostics.push(ConfigDiagnosticWire {
            severity: "error".to_string(),
            code: "conflicting_axe_config_aliases".to_string(),
            message: format!(
                "conflicting synonym list values authored at `{}` and `{}`",
                display_path(&child_path(source_path, CHOPS)),
                display_path(&child_path(source_path, JOBS))
            ),
            path: Some(display_path(&child_path(norm_path, CHOPS))),
            layer: Some(layer.to_string()),
        });
    }
}

fn insert_normalized_child(
    result: &mut Map<String, Value>,
    sources: &mut BTreeMap<Vec<String>, Vec<String>>,
    key: String,
    node: NormalizedNode,
    path: &[String],
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    match result.remove(&key) {
        Some(existing) => {
            let merged = merge_same_layer_aliases(
                existing,
                node.value,
                path,
                sources,
                &node.sources,
                layer,
                diagnostics,
            );
            for (source_path, source) in node.sources {
                sources.entry(source_path).or_insert(source);
            }
            result.insert(key, merged);
        }
        None => {
            sources.extend(node.sources);
            result.insert(key, node.value);
        }
    }
}

fn merge_same_layer_aliases(
    existing: Value,
    incoming: Value,
    path: &[String],
    existing_sources: &BTreeMap<Vec<String>, Vec<String>>,
    incoming_sources: &BTreeMap<Vec<String>, Vec<String>>,
    layer: &str,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> Value {
    match (existing, incoming) {
        (Value::Object(mut left), Value::Object(right)) => {
            for (key, right_value) in right {
                let child = child_path(path, &key);
                match left.remove(&key) {
                    Some(left_value) => {
                        let merged = merge_same_layer_aliases(
                            left_value,
                            right_value,
                            &child,
                            existing_sources,
                            incoming_sources,
                            layer,
                            diagnostics,
                        );
                        left.insert(key, merged);
                    }
                    None => {
                        left.insert(key, right_value);
                    }
                }
            }
            Value::Object(left)
        }
        (left, right) if left == right => left,
        (left, _right) => {
            let left_source = existing_sources
                .get(path)
                .cloned()
                .unwrap_or_else(|| path.to_vec());
            let right_source = incoming_sources
                .get(path)
                .cloned()
                .unwrap_or_else(|| path.to_vec());
            diagnostics.push(ConfigDiagnosticWire {
                severity: "error".to_string(),
                code: "conflicting_axe_config_aliases".to_string(),
                message: format!(
                    "conflicting values for `{}` authored at `{}` and `{}`",
                    display_path(path),
                    display_path(&left_source),
                    display_path(&right_source),
                ),
                path: Some(display_path(path)),
                layer: Some(layer.to_string()),
            });
            left
        }
    }
}

fn copy_with_sources(
    value: &Value,
    norm_path: &[String],
    source_path: &[String],
) -> NormalizedNode {
    let mut sources = source_map(norm_path, source_path);
    match value {
        Value::Object(map) => {
            let mut copied = Map::new();
            for (key, child) in map {
                let child_norm = child_path(norm_path, key);
                let child_source = child_path(source_path, key);
                let node = copy_with_sources(child, &child_norm, &child_source);
                sources.extend(node.sources);
                copied.insert(key.clone(), node.value);
            }
            NormalizedNode {
                value: Value::Object(copied),
                sources,
            }
        }
        Value::Array(items) => {
            let mut copied = Vec::with_capacity(items.len());
            for (index, child) in items.iter().enumerate() {
                let child_norm = indexed_path(norm_path, index);
                let child_source = indexed_path(source_path, index);
                let node = copy_with_sources(child, &child_norm, &child_source);
                sources.extend(node.sources);
                copied.push(node.value);
            }
            NormalizedNode {
                value: Value::Array(copied),
                sources,
            }
        }
        _ => NormalizedNode {
            value: value.clone(),
            sources,
        },
    }
}

fn source_map(
    norm_path: &[String],
    source_path: &[String],
) -> BTreeMap<Vec<String>, Vec<String>> {
    BTreeMap::from([(norm_path.to_vec(), source_path.to_vec())])
}

fn child_path(path: &[String], child: &str) -> Vec<String> {
    let mut result = path.to_vec();
    result.push(child.to_string());
    result
}

fn indexed_path(path: &[String], index: usize) -> Vec<String> {
    let mut result = path.to_vec();
    result.push(format!("[{index}]"));
    result
}

pub(super) fn replacement_paths_for_layer(
    value: &Value,
    strategy: ListStrategy,
) -> Vec<Vec<String>> {
    let mut replacements = Vec::new();
    if strategy != ListStrategy::Replace {
        return replacements;
    }
    let Some(lumberjacks) = value
        .get(AXE)
        .and_then(|axe| axe.get(LUMBERJACKS))
        .and_then(Value::as_object)
    else {
        return replacements;
    };
    for (name, config) in lumberjacks {
        if config.get(CHOPS).is_some() {
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
    for (routine_key, jobs_key) in [(LUMBERJACKS, CHOPS), (ROUTINES, JOBS)] {
        let Some(raw_lumberjacks) =
            raw_axe.get(routine_key).and_then(Value::as_object)
        else {
            continue;
        };
        for (lumberjack, raw_config) in raw_lumberjacks {
            let Some(raw_list) =
                raw_config.get(jobs_key).and_then(Value::as_array)
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
                        message: format!("duplicate job identity `{identity}`"),
                        path: Some(format!(
                        "axe.{routine_key}.{lumberjack}.{jobs_key}[{index}]"
                    )),
                        layer: Some(label.to_string()),
                    });
                }
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
    source_paths: &BTreeMap<Vec<String>, Vec<String>>,
    provenance: &mut ExactProvenance,
) -> Value {
    match (base, over) {
        (Value::Object(base_map), Value::Object(over_map)) => {
            let mut result = base_map.clone();
            record_path_provenance(path, layer, source_paths, provenance);
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
                        source_paths,
                        provenance,
                    ),
                    None => {
                        record_provenance(
                            over_value,
                            &child_path,
                            layer,
                            source_paths,
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
                record_provenance(over, path, layer, source_paths, provenance);
                over.clone()
            } else {
                record_path_provenance(path, layer, source_paths, provenance);
                let mut result = base_list.clone();
                let offset = result.len();
                result.extend(over_list.iter().cloned());
                for (index, child) in over_list.iter().enumerate() {
                    let mut child_path = path.to_vec();
                    child_path.push(format!("[{}]", offset + index));
                    record_provenance(
                        child,
                        &child_path,
                        layer,
                        source_paths,
                        provenance,
                    );
                }
                Value::Array(result)
            }
        }
        _ => {
            clear_provenance(provenance, path);
            record_provenance(over, path, layer, source_paths, provenance);
            over.clone()
        }
    }
}

fn record_provenance(
    value: &Value,
    path: &[String],
    layer: &str,
    source_paths: &BTreeMap<Vec<String>, Vec<String>>,
    provenance: &mut ExactProvenance,
) {
    record_path_provenance(path, layer, source_paths, provenance);
    if let Value::Object(map) = value {
        for (key, child) in map {
            let mut child_path = path.to_vec();
            child_path.push(key.clone());
            record_provenance(
                child,
                &child_path,
                layer,
                source_paths,
                provenance,
            );
        }
    } else if let Value::Array(items) = value {
        for (index, child) in items.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(format!("[{index}]"));
            record_provenance(
                child,
                &child_path,
                layer,
                source_paths,
                provenance,
            );
        }
    }
}

fn record_path_provenance(
    path: &[String],
    layer: &str,
    source_paths: &BTreeMap<Vec<String>, Vec<String>>,
    provenance: &mut ExactProvenance,
) {
    provenance.insert(
        path.to_vec(),
        SourcePath {
            layer: layer.to_string(),
            key_path: source_paths
                .get(path)
                .cloned()
                .unwrap_or_else(|| path.to_vec()),
        },
    );
}

fn clear_provenance(provenance: &mut ExactProvenance, path: &[String]) {
    provenance.retain(|candidate, _| !candidate.starts_with(path));
}

pub(super) fn remove_value_at_path(value: &mut Value, path: &[String]) {
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
        .map(|(path, source)| AxeFieldProvenanceWire {
            key_path: path.clone(),
            path: display_path(path),
            source_key_path: source.key_path.clone(),
            source_path: display_path(&source.key_path),
            layer: source.layer.clone(),
        })
        .collect()
}

fn public_projection(
    effective_config: &Value,
    provenance: &[AxeFieldProvenanceWire],
) -> (Value, Vec<AxeFieldProvenanceWire>) {
    (
        public_project_root(effective_config),
        provenance
            .iter()
            .map(|item| {
                let key_path = public_key_path(&item.key_path);
                AxeFieldProvenanceWire {
                    path: display_path(&key_path),
                    key_path,
                    source_key_path: item.source_key_path.clone(),
                    source_path: item.source_path.clone(),
                    layer: item.layer.clone(),
                }
            })
            .collect(),
    )
}

pub(super) fn public_project_root(value: &Value) -> Value {
    let Some(root) = value.as_object() else {
        return value.clone();
    };
    let mut projected = Map::new();
    for (key, child) in root {
        if key == AXE {
            projected.insert(key.clone(), public_project_axe(child));
        } else {
            projected.insert(key.clone(), child.clone());
        }
    }
    Value::Object(projected)
}

fn public_project_axe(value: &Value) -> Value {
    let Some(axe) = value.as_object() else {
        return value.clone();
    };
    let mut projected = Map::new();
    for (key, child) in axe {
        let public_key = public_axe_key(key);
        let public_value = if key == LUMBERJACKS {
            public_project_routines(child)
        } else {
            child.clone()
        };
        projected.insert(public_key.to_string(), public_value);
    }
    Value::Object(projected)
}

fn public_project_routines(value: &Value) -> Value {
    let Some(routines) = value.as_object() else {
        return value.clone();
    };
    let mut projected = Map::new();
    for (name, routine) in routines {
        projected.insert(name.clone(), public_project_routine(routine));
    }
    Value::Object(projected)
}

fn public_project_routine(value: &Value) -> Value {
    let Some(routine) = value.as_object() else {
        return value.clone();
    };
    let mut projected = Map::new();
    for (key, child) in routine {
        let public_key = public_routine_key(key);
        projected.insert(public_key.to_string(), child.clone());
    }
    Value::Object(projected)
}

pub(super) fn public_key_path(path: &[String]) -> Vec<String> {
    if path.len() >= 2 && path[0] == AXE {
        let mut public = Vec::with_capacity(path.len());
        public.push(AXE.to_string());
        public.push(public_axe_key(&path[1]).to_string());
        if path.len() >= 4 && path[1] == LUMBERJACKS {
            public.push(path[2].clone());
            public.push(public_routine_key(&path[3]).to_string());
            public.extend_from_slice(&path[4..]);
        } else {
            public.extend_from_slice(&path[2..]);
        }
        public
    } else {
        path.to_vec()
    }
}

fn public_axe_key(key: &str) -> &str {
    match key {
        LUMBERJACKS => ROUTINES,
        "chop_script_dirs" => "job_script_dirs",
        "lumberjack_log_max_bytes" => "routine_log_max_bytes",
        "lumberjack_log_temp_max_age_seconds" => {
            "routine_log_temp_max_age_seconds"
        }
        "lumberjack_restart_backoff_max_seconds" => {
            "routine_restart_backoff_max_seconds"
        }
        "verbose_lumberjack_diagnostics" => "verbose_routine_diagnostics",
        _ => key,
    }
}

fn public_routine_key(key: &str) -> &str {
    match key {
        CHOPS => JOBS,
        "chop_timeout" => "job_timeout",
        _ => key,
    }
}

pub(super) fn display_path(path: &[String]) -> String {
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
        .map(|(path, source)| AxeFieldProvenanceWire {
            key_path: path.clone(),
            path: display_path(path),
            source_key_path: source.key_path.clone(),
            source_path: display_path(&source.key_path),
            layer: source.layer.clone(),
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
            let (representation, value, key_path) =
                raw_contribution(layer, selector);
            AxeRawContributionWire {
                layer: layer.name.clone(),
                file: layer.path.clone(),
                writable: layer.writable,
                representation: representation.to_string(),
                key_path: key_path.clone(),
                path: display_path(&key_path),
                has_value: value.is_some(),
                value: value.cloned().unwrap_or(Value::Null),
            }
        })
        .collect()
}

fn raw_contribution<'a>(
    layer: &'a ConfigLayerInputWire,
    selector: &AxeEntrySelectorWire,
) -> (&'static str, Option<&'a Value>, Vec<String>) {
    let (routine, routine_path) =
        raw_routine_value(layer, &selector.lumberjack);
    let Some(chop_name) = selector.chop.as_ref() else {
        let representation = if routine.is_some() {
            "keyed_map"
        } else {
            "absent"
        };
        return (representation, routine, routine_path);
    };
    let Some(routine) = routine else {
        return (
            "absent",
            None,
            vec![
                AXE.to_string(),
                ROUTINES.to_string(),
                selector.lumberjack.clone(),
                JOBS.to_string(),
                chop_name.clone(),
            ],
        );
    };
    for (jobs_key, public_jobs_key) in [(CHOPS, CHOPS), (JOBS, JOBS)] {
        let Some(jobs) = routine.get(jobs_key) else {
            continue;
        };
        let jobs_path = {
            let mut path = routine_path.clone();
            path.push(public_jobs_key.to_string());
            path
        };
        match jobs {
            Value::Array(list) => {
                for (index, item) in list.iter().enumerate() {
                    if chop_identity(item) == Some(chop_name) {
                        return (
                            "legacy_list",
                            Some(item),
                            indexed_path(&jobs_path, index),
                        );
                    }
                }
            }
            Value::Object(map) => {
                if let Some(value) = map.get(chop_name) {
                    return (
                        "keyed_map",
                        Some(value),
                        child_path(&jobs_path, chop_name),
                    );
                }
            }
            _ => {}
        }
    }
    (
        "absent",
        None,
        vec![
            AXE.to_string(),
            ROUTINES.to_string(),
            selector.lumberjack.clone(),
            JOBS.to_string(),
            chop_name.clone(),
        ],
    )
}

fn raw_routine_value<'a>(
    layer: &'a ConfigLayerInputWire,
    name: &str,
) -> (Option<&'a Value>, Vec<String>) {
    let axe = layer.value.get(AXE);
    if let Some(value) = axe
        .and_then(|axe| axe.get(LUMBERJACKS))
        .and_then(|items| items.get(name))
    {
        return (
            Some(value),
            vec![AXE.to_string(), LUMBERJACKS.to_string(), name.to_string()],
        );
    }
    if let Some(value) = axe
        .and_then(|axe| axe.get(ROUTINES))
        .and_then(|items| items.get(name))
    {
        return (
            Some(value),
            vec![AXE.to_string(), ROUTINES.to_string(), name.to_string()],
        );
    }
    (
        None,
        vec![AXE.to_string(), ROUTINES.to_string(), name.to_string()],
    )
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
                        source_key_path: item.source_key_path.clone(),
                        source_path: item.source_path.clone(),
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
        source_key_path: path.to_vec(),
        source_path: display_path(path),
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

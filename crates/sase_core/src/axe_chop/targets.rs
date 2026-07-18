use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;
use sha2::{Digest, Sha256};

use super::wire::{
    ChopEngineError, ChopForEachConfigWire, ChopTargetExpansionRequestWire,
    ChopTargetExpansionWire, ChopTargetInstanceWire,
    ChopTargetSourceFiltersWire, CHOP_ENGINE_SCHEMA_VERSION,
};

type TargetParts = (BTreeMap<String, Value>, BTreeMap<String, Value>);

/// Expand a literal or host-provided target source into stable instances.
pub fn expand_chop_targets(
    request: &ChopTargetExpansionRequestWire,
) -> Result<ChopTargetExpansionWire, ChopEngineError> {
    validate_request(request)?;
    let Some(for_each) = request.for_each.as_ref() else {
        return Ok(ChopTargetExpansionWire {
            schema_version: CHOP_ENGINE_SCHEMA_VERSION,
            instances: vec![ChopTargetInstanceWire {
                instance_id: request.chop_name.clone(),
                target_key: String::new(),
                target: BTreeMap::new(),
                overrides: BTreeMap::new(),
            }],
        });
    };

    let rows = match for_each {
        ChopForEachConfigWire::Literal { targets } => targets.clone(),
        ChopForEachConfigWire::Source { source, filters } => {
            if source != "projects" {
                return Err(ChopEngineError::new(
                    "unknown_target_source",
                    "$.for_each.source",
                    format!(
                        "unknown target source `{source}`; supported sources: projects"
                    ),
                ));
            }
            filter_project_rows(&request.source_rows, filters)
        }
    };

    let mut instance_ids = BTreeSet::new();
    let mut instances = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        let (target, overrides) = split_target_row(row, index)?;
        let target_key = stable_target_key(&target)?;
        let instance_component = sanitize_instance_component(&target_key);
        if instance_component.is_empty() {
            return Err(ChopEngineError::new(
                "invalid_target_key",
                format!("$.for_each.targets[{index}]"),
                "target identity must contain at least one letter or digit",
            ));
        }
        let instance_id =
            format!("{}[{instance_component}]", request.chop_name);
        if !instance_ids.insert(instance_id.clone()) {
            return Err(ChopEngineError::new(
                "duplicate_target",
                format!("$.for_each.targets[{index}]"),
                format!("target expands to duplicate instance `{instance_id}`"),
            ));
        }
        instances.push(ChopTargetInstanceWire {
            instance_id,
            target_key,
            target,
            overrides,
        });
    }

    Ok(ChopTargetExpansionWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        instances,
    })
}

fn validate_request(
    request: &ChopTargetExpansionRequestWire,
) -> Result<(), ChopEngineError> {
    if request.schema_version != CHOP_ENGINE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_ENGINE_SCHEMA_VERSION}",
                request.schema_version
            ),
        ));
    }
    if request.chop_name.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            "$.chop_name",
            "chop name must not be blank",
        ));
    }
    if request.chop_name.contains('[') || request.chop_name.contains(']') {
        return Err(ChopEngineError::new(
            "invalid_chop_name",
            "$.chop_name",
            "chop name must not contain target-instance brackets",
        ));
    }
    Ok(())
}

fn filter_project_rows(
    rows: &[BTreeMap<String, Value>],
    filters: &ChopTargetSourceFiltersWire,
) -> Vec<BTreeMap<String, Value>> {
    rows.iter()
        .filter(|row| {
            row.get("enabled").and_then(Value::as_bool).unwrap_or(true)
        })
        .filter(|row| {
            filters.names.is_empty()
                || row_identity(row).is_some_and(|name| {
                    filters.names.iter().any(|allowed| allowed == name)
                })
        })
        .filter(|row| {
            filters.vcs.is_empty()
                || row.get("vcs").and_then(Value::as_str).is_some_and(|vcs| {
                    filters.vcs.iter().any(|allowed| allowed == vcs)
                })
        })
        .cloned()
        .collect()
}

fn split_target_row(
    mut row: BTreeMap<String, Value>,
    index: usize,
) -> Result<TargetParts, ChopEngineError> {
    let overrides = row
        .remove("overrides")
        .or_else(|| row.remove("_overrides"))
        .map(|value| {
            serde_json::from_value(value).map_err(|_| {
                ChopEngineError::new(
                    "invalid_target_overrides",
                    format!("$.for_each.targets[{index}].overrides"),
                    "target overrides must be an object",
                )
            })
        })
        .transpose()?
        .unwrap_or_default();

    if let Some(nested) = row.remove("target") {
        if !row.is_empty() {
            return Err(ChopEngineError::new(
                "invalid_target",
                format!("$.for_each.targets[{index}]"),
                "a row using `target` may only contain `target` and `overrides`",
            ));
        }
        row = serde_json::from_value(nested).map_err(|_| {
            ChopEngineError::new(
                "invalid_target",
                format!("$.for_each.targets[{index}].target"),
                "target must be an object",
            )
        })?;
    }
    if row.is_empty() {
        return Err(ChopEngineError::new(
            "invalid_target",
            format!("$.for_each.targets[{index}]"),
            "target object must not be empty",
        ));
    }
    Ok((row, overrides))
}

fn stable_target_key(
    target: &BTreeMap<String, Value>,
) -> Result<String, ChopEngineError> {
    if let Some(value) = ["id", "name", "project", "key"]
        .iter()
        .find_map(|key| target.get(*key))
    {
        if let Some(text) = scalar_key(value) {
            if !text.trim().is_empty() {
                return Ok(text);
            }
        }
    }
    let encoded = serde_json::to_vec(target).map_err(|error| {
        ChopEngineError::new(
            "target_encoding_failed",
            "$.for_each",
            format!("could not canonicalize target: {error}"),
        )
    })?;
    let digest = Sha256::digest(encoded);
    Ok(format!("target-{}", hex::encode(&digest[..6])))
}

fn scalar_key(value: &Value) -> Option<String> {
    match value {
        Value::String(item) => Some(item.clone()),
        Value::Number(item) => Some(item.to_string()),
        Value::Bool(item) => Some(item.to_string()),
        _ => None,
    }
}

fn row_identity(row: &BTreeMap<String, Value>) -> Option<&str> {
    row.get("name")
        .or_else(|| row.get("project"))
        .and_then(Value::as_str)
}

fn sanitize_instance_component(value: &str) -> String {
    let mut result = String::new();
    let mut separator = false;
    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric()
            || matches!(character, '-' | '_' | '.')
        {
            result.push(character);
            separator = false;
        } else if !result.is_empty() && !separator {
            result.push('-');
            separator = true;
        }
    }
    result.trim_matches('-').to_string()
}

//! Deep merge + path helpers for the config domain.
//!
//! [`deep_merge_objects`] reproduces the Python `_deep_merge` semantics
//! (`sase.config.core._deep_merge`) exactly: dicts merge recursively, lists
//! concatenate or replace per the layer's `list_strategy`, and scalars in the
//! override win. The strategy propagates into nested dict merges, just like
//! the Python keyword argument.

use serde_json::{Map, Value};

use super::wire::{ConfigError, ConfigLayerInputWire, ListStrategy};

/// Recursively merge `over` into `base`, returning a new map. Neither input
/// is mutated. Mirrors `_deep_merge` (lists obey `strategy`, scalars/dicts as
/// in the Python implementation).
pub fn deep_merge_objects(
    base: &Map<String, Value>,
    over: &Map<String, Value>,
    strategy: ListStrategy,
) -> Map<String, Value> {
    let mut result = base.clone();
    for (key, over_val) in over {
        match result.get(key) {
            Some(base_val) => {
                if let (Some(base_obj), Some(over_obj)) =
                    (base_val.as_object(), over_val.as_object())
                {
                    result.insert(
                        key.clone(),
                        Value::Object(deep_merge_objects(
                            base_obj, over_obj, strategy,
                        )),
                    );
                } else if let (Some(base_arr), Some(over_arr)) =
                    (base_val.as_array(), over_val.as_array())
                {
                    let merged = match strategy {
                        ListStrategy::Replace => over_arr.clone(),
                        ListStrategy::Concatenate => {
                            let mut merged = base_arr.clone();
                            merged.extend(over_arr.iter().cloned());
                            merged
                        }
                    };
                    result.insert(key.clone(), Value::Array(merged));
                } else {
                    result.insert(key.clone(), over_val.clone());
                }
            }
            None => {
                result.insert(key.clone(), over_val.clone());
            }
        }
    }
    result
}

/// Fold the ordered layer stack (lowest → highest priority) into a single
/// merged config object.
///
/// Starting from an empty object means the first layer's entries are all new
/// keys (no list merging), and every later layer merges with its own
/// `list_strategy` — exactly matching `load_merged_config`, where empty
/// layers are no-ops and the base default seeds the result.
pub fn merge_layers(layers: &[ConfigLayerInputWire]) -> Value {
    let mut result = Map::new();
    for layer in layers {
        if let Some(obj) = layer.value.as_object() {
            let strategy = ListStrategy::from_token(&layer.list_strategy);
            result = deep_merge_objects(&result, obj, strategy);
        }
    }
    canonicalize_value(&Value::Object(result))
}

/// Restore the generic config backend's historical lexical object ordering.
///
/// AXE composition preserves authored mapping order, but the generic config
/// contracts predate that requirement and expose lexically ordered JSON
/// objects. Keep that public behavior stable even though the workspace now
/// enables `serde_json`'s insertion-order map representation.
pub fn canonicalize_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(canonicalize_value).collect())
        }
        Value::Object(items) => {
            let mut keys: Vec<_> = items.keys().collect();
            keys.sort();
            Value::Object(
                keys.into_iter()
                    .map(|key| (key.clone(), canonicalize_value(&items[key])))
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

/// Resolve a dotted key path against a value, returning the nested value if
/// every segment exists as a mapping key.
pub fn get_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut cur = value;
    for segment in path {
        cur = cur.as_object()?.get(*segment)?;
    }
    Some(cur)
}

/// Set `value` at the dotted key path inside `obj`, creating intermediate
/// mappings as needed. Errors if an intermediate segment exists but is not a
/// mapping (a structural conflict the caller must surface).
pub fn set_at_path(
    obj: &mut Map<String, Value>,
    path: &[String],
    value: Value,
) -> Result<(), ConfigError> {
    let Some((last, prefix)) = path.split_last() else {
        return Err(ConfigError::validation("empty key path"));
    };
    let mut cur = obj;
    for segment in prefix {
        let entry = cur
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            return Err(ConfigError::validation(format!(
                "cannot set `{}`: `{segment}` is not a mapping",
                path.join(".")
            )));
        }
        cur = entry.as_object_mut().expect("checked is_object above");
    }
    cur.insert(last.clone(), value);
    Ok(())
}

/// Remove the value at the dotted key path inside `obj`. Returns whether a
/// key was actually removed. Intermediate empty mappings are left in place.
pub fn unset_at_path(obj: &mut Map<String, Value>, path: &[String]) -> bool {
    let Some((last, prefix)) = path.split_last() else {
        return false;
    };
    let mut cur = obj;
    for segment in prefix {
        match cur.get_mut(segment).and_then(Value::as_object_mut) {
            Some(next) => cur = next,
            None => return false,
        }
    }
    cur.remove(last).is_some()
}

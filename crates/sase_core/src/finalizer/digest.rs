use serde::Serialize;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::FinalizerError;

/// Serialize JSON with object keys sorted at every depth.
pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, FinalizerError> {
    serde_json::to_vec(&canonical_json_value(value)).map_err(|error| {
        FinalizerError::validation(format!(
            "unable to encode canonical finalizer JSON: {error}"
        ))
    })
}

pub fn canonical_json_sha256(value: &Value) -> Result<String, FinalizerError> {
    let encoded = canonical_json_bytes(value)?;
    Ok(hex::encode(Sha256::digest(&encoded)))
}

pub fn finalizer_digest_json_value(
    value: &Value,
) -> Result<String, FinalizerError> {
    canonical_json_sha256(value)
}

pub fn finalizer_digest_serializable<T: Serialize>(
    value: &T,
) -> Result<String, FinalizerError> {
    let value = serde_json::to_value(value).map_err(|error| {
        FinalizerError::validation(format!(
            "unable to normalize finalizer wire value for digest: {error}"
        ))
    })?;
    canonical_json_sha256(&value)
}

fn canonical_json_value(value: &Value) -> Value {
    match value {
        Value::Array(entries) => {
            Value::Array(entries.iter().map(canonical_json_value).collect())
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut sorted = Map::new();
            for key in keys {
                sorted.insert(key.clone(), canonical_json_value(&map[key]));
            }
            Value::Object(sorted)
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_digest_sorts_nested_object_keys() {
        let left = json!({
            "z": 1,
            "a": {"b": 2, "a": [{"y": true, "x": false}]}
        });
        let right = json!({
            "a": {"a": [{"x": false, "y": true}], "b": 2},
            "z": 1
        });

        assert_eq!(
            canonical_json_bytes(&left).unwrap(),
            canonical_json_bytes(&right).unwrap()
        );
        assert_eq!(
            canonical_json_sha256(&left).unwrap(),
            canonical_json_sha256(&right).unwrap()
        );
    }
}

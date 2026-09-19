//! Canonical JSON hashing for ToolRun identity and fingerprints.

use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn canonical_digest<T: Serialize>(value: &T) -> Result<String, String> {
    let json = serde_json::to_value(value).map_err(|error| {
        format!("unable to serialize value for digest: {error}")
    })?;
    let encoded = serde_json::to_vec(&canonicalize_json(json))
        .map_err(|error| format!("unable to encode canonical JSON: {error}"))?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

pub fn canonicalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries = map.into_iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, child)| (key, canonicalize_json(child)))
                    .collect(),
            )
        }
        Value::Array(items) => {
            Value::Array(items.into_iter().map(canonicalize_json).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_keys_are_sorted_before_hashing() {
        let left = json!({"b": 1, "a": 2});
        let right = json!({"a": 2, "b": 1});
        assert_eq!(
            canonical_digest(&left).unwrap(),
            canonical_digest(&right).unwrap()
        );
    }
}

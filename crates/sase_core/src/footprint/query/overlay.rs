//! Overlay matching for report and card queries.

use std::collections::BTreeMap;

use serde_json::Value;

use super::super::catalog::validate_overlay_shape;
use super::super::FootprintError;

#[derive(Debug, Clone, Default)]
pub(crate) struct OverlayFeature {
    pub protected: Option<String>,
    pub keep: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OverlayIndex {
    exact: BTreeMap<String, OverlayFeature>,
    globs: Vec<(String, OverlayFeature)>,
    retire_with: Vec<Vec<String>>,
}

impl OverlayIndex {
    pub(crate) fn parse(overlay: &Value) -> Result<Self, FootprintError> {
        validate_overlay_shape(overlay)?;
        let Some(object) = overlay.as_object() else {
            return Ok(Self::default());
        };
        let mut index = Self::default();
        if let Some(features) =
            object.get("features").and_then(Value::as_object)
        {
            for (key, value) in features {
                let parsed = parse_feature(value);
                if let Some(prefix) = key.strip_suffix('*') {
                    index.globs.push((prefix.to_string(), parsed));
                } else {
                    index.exact.insert(key.clone(), parsed);
                }
            }
            index
                .globs
                .sort_by_key(|item| std::cmp::Reverse(item.0.len()));
        }
        if let Some(groups) =
            object.get("retire_with").and_then(Value::as_array)
        {
            for group in groups {
                let Some(ids) = group.as_array() else {
                    continue;
                };
                let parsed: Vec<String> = ids
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect();
                if parsed.len() >= 2 {
                    index.retire_with.push(parsed);
                }
            }
        }
        Ok(index)
    }

    pub(crate) fn get(&self, feature_id: &str) -> Option<&OverlayFeature> {
        if let Some(feature) = self.exact.get(feature_id) {
            return Some(feature);
        }
        self.globs
            .iter()
            .find(|(prefix, _)| feature_id.starts_with(prefix.as_str()))
            .map(|(_, feature)| feature)
    }

    pub(crate) fn retire_with(&self, feature_id: &str) -> Vec<String> {
        let mut siblings = Vec::new();
        for group in &self.retire_with {
            if group.iter().any(|id| id == feature_id) {
                for id in group {
                    if id != feature_id && !siblings.contains(id) {
                        siblings.push(id.clone());
                    }
                }
            }
        }
        siblings
    }
}

fn parse_feature(value: &Value) -> OverlayFeature {
    let object = match value.as_object() {
        Some(object) => object,
        None => return OverlayFeature::default(),
    };
    OverlayFeature {
        protected: object
            .get("protected")
            .and_then(Value::as_str)
            .map(str::to_string),
        keep: object
            .get("keep")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

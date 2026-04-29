//! Wire records for the status state machine, serde-compatible mirrors of
//! `sase_100/src/sase/core/status_wire.py`.
//!
//! Keeping these types here (separate from `crate::wire`) matches the
//! Python module split: the status wire evolves on its own
//! `STATUS_WIRE_SCHEMA_VERSION` independently of the ChangeSpec parser
//! wire's `CHANGESPEC_WIRE_SCHEMA_VERSION`.
//!
//! JSON shape rules (mirroring the Python dataclasses and enforced by
//! tests):
//!
//! - `Option<T>::None` serialises as JSON `null` (never omitted).
//! - Empty list fields serialise as `[]`.
//! - Action fields (`suffix_action`, `mentor_draft_action`,
//!   `archive_action`) carry their literal string variant, not an enum
//!   index — so the Python adapter can compare against the same
//!   `SUFFIX_ACTION_*` / `MENTOR_ACTION_*` / `ARCHIVE_ACTION_*` constants.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Status wire schema version. Must match Python's
/// `STATUS_WIRE_SCHEMA_VERSION` in `status_wire.py`.
pub const STATUS_WIRE_SCHEMA_VERSION: u32 = 1;

// Suffix actions on a successful transition.
pub const SUFFIX_ACTION_NONE: &str = "none";
pub const SUFFIX_ACTION_STRIP: &str = "strip";
pub const SUFFIX_ACTION_APPEND: &str = "append";

// Mentor draft-flag actions.
pub const MENTOR_ACTION_NONE: &str = "none";
pub const MENTOR_ACTION_SET: &str = "set_draft";
pub const MENTOR_ACTION_CLEAR: &str = "clear_draft";

// File-classification actions (main vs. archive).
pub const ARCHIVE_ACTION_NONE: &str = "none";
pub const ARCHIVE_ACTION_TO_ARCHIVE: &str = "to_archive";
pub const ARCHIVE_ACTION_FROM_ARCHIVE: &str = "from_archive";

/// A child reference used by the parent/children decision rules. Mirrors
/// `ChangespecChildWire` in `status_wire.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangespecChildWire {
    pub name: String,
    pub status: String,
}

/// Pure inputs the host has gathered for one transition decision. Mirrors
/// `StatusTransitionRequestWire` in `status_wire.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusTransitionRequestWire {
    pub schema_version: u32,
    pub changespec_name: String,
    pub old_status: String,
    pub new_status: String,
    pub validate: bool,
    pub parent_status: Option<String>,
    #[serde(default)]
    pub blocking_children: Vec<ChangespecChildWire>,
    #[serde(default)]
    pub siblings_with_unreverted_children: Vec<String>,
    #[serde(default)]
    pub existing_names: Vec<String>,
}

/// The decision the planner reached, ready for the host to execute.
/// Mirrors `StatusTransitionPlanWire` in `status_wire.py`.
///
/// Field declaration order matches the Python dataclass so a JSON
/// serialisation comparison stays meaningful.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusTransitionPlanWire {
    pub schema_version: u32,
    pub success: bool,
    pub old_status: Option<String>,
    pub error: Option<String>,
    #[serde(default)]
    pub status_update_target: Option<String>,
    #[serde(default = "default_suffix_action")]
    pub suffix_action: String,
    #[serde(default)]
    pub suffixed_name: Option<String>,
    #[serde(default)]
    pub base_name: Option<String>,
    #[serde(default = "default_mentor_action")]
    pub mentor_draft_action: String,
    #[serde(default = "default_archive_action")]
    pub archive_action: String,
    #[serde(default)]
    pub timestamp_event: Option<String>,
    #[serde(default)]
    pub timestamp_target_name: Option<String>,
    #[serde(default)]
    pub revert_siblings: bool,
}

fn default_suffix_action() -> String {
    SUFFIX_ACTION_NONE.to_string()
}

fn default_mentor_action() -> String {
    MENTOR_ACTION_NONE.to_string()
}

fn default_archive_action() -> String {
    ARCHIVE_ACTION_NONE.to_string()
}

/// Inputs for `read_status_from_lines`. Mirrors `StatusFieldReadWire` in
/// `status_wire.py`. Provided as a structured type so the PyO3 binding can
/// accept the same shape as the dataclass (`{"lines": [...],
/// "changespec_name": "..."}`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusFieldReadWire {
    pub lines: Vec<String>,
    pub changespec_name: String,
}

/// Inputs for `apply_status_update`. Mirrors `StatusFieldUpdateWire` in
/// `status_wire.py`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusFieldUpdateWire {
    pub lines: Vec<String>,
    pub changespec_name: String,
    pub new_status: String,
}

/// Decode a [`StatusTransitionRequestWire`] from a `serde_json::Value`,
/// validating the schema version. Mirrors `status_request_from_dict` in
/// `status_wire.py`. Returns the rehydrated record on success; on schema
/// mismatch returns `Err` with the exact message format the Python helper
/// raises so caller-side parity tests can compare verbatim.
pub fn status_request_from_json_value(
    value: &JsonValue,
) -> Result<StatusTransitionRequestWire, String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            "status wire missing or non-integer schema_version".to_string()
        })?;
    if schema != STATUS_WIRE_SCHEMA_VERSION as u64 {
        return Err(format!(
            "status wire schema mismatch: got {schema}, expected {}",
            STATUS_WIRE_SCHEMA_VERSION
        ));
    }
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

/// Decode a [`StatusTransitionPlanWire`] from a `serde_json::Value`,
/// validating the schema version. Mirrors `status_plan_from_dict` in
/// `status_wire.py`.
pub fn status_plan_from_json_value(
    value: &JsonValue,
) -> Result<StatusTransitionPlanWire, String> {
    let schema = value
        .get("schema_version")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            "status wire missing or non-integer schema_version".to_string()
        })?;
    if schema != STATUS_WIRE_SCHEMA_VERSION as u64 {
        return Err(format!(
            "status wire schema mismatch: got {schema}, expected {}",
            STATUS_WIRE_SCHEMA_VERSION
        ));
    }
    serde_json::from_value(value.clone()).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_round_trips_through_json() {
        let req = StatusTransitionRequestWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            changespec_name: "proj_foo".into(),
            old_status: "Ready (proj_2)".into(),
            new_status: "Draft".into(),
            validate: true,
            parent_status: None,
            blocking_children: vec![ChangespecChildWire {
                name: "proj_foo_child".into(),
                status: "Ready".into(),
            }],
            siblings_with_unreverted_children: vec![],
            existing_names: vec!["proj_foo".into(), "proj_foo_1".into()],
        };
        let value = serde_json::to_value(&req).unwrap();
        let rebuilt = status_request_from_json_value(&value).unwrap();
        assert_eq!(rebuilt, req);
    }

    #[test]
    fn plan_round_trips_through_json() {
        let plan = StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: true,
            old_status: Some("Ready".into()),
            error: None,
            status_update_target: Some("Draft".into()),
            suffix_action: SUFFIX_ACTION_APPEND.into(),
            suffixed_name: Some("proj_foo_3".into()),
            base_name: Some("proj_foo".into()),
            mentor_draft_action: MENTOR_ACTION_SET.into(),
            archive_action: ARCHIVE_ACTION_NONE.into(),
            timestamp_event: Some("Ready -> Draft".into()),
            timestamp_target_name: Some("proj_foo_3".into()),
            revert_siblings: false,
        };
        let value = serde_json::to_value(&plan).unwrap();
        let rebuilt = status_plan_from_json_value(&value).unwrap();
        assert_eq!(rebuilt, plan);
    }

    #[test]
    fn schema_mismatch_returns_error() {
        let bad_request = json!({
            "schema_version": STATUS_WIRE_SCHEMA_VERSION + 99,
            "changespec_name": "x",
            "old_status": "WIP",
            "new_status": "Draft",
            "validate": true,
            "parent_status": null,
        });
        let err = status_request_from_json_value(&bad_request).unwrap_err();
        assert!(err.contains("schema mismatch"), "unexpected error: {err}");

        let bad_plan = json!({
            "schema_version": STATUS_WIRE_SCHEMA_VERSION + 99,
            "success": true,
            "old_status": "WIP",
            "error": null,
        });
        let err = status_plan_from_json_value(&bad_plan).unwrap_err();
        assert!(err.contains("schema mismatch"), "unexpected error: {err}");
    }

    #[test]
    fn json_shape_keeps_optional_fields_as_null() {
        let plan = StatusTransitionPlanWire {
            schema_version: STATUS_WIRE_SCHEMA_VERSION,
            success: false,
            old_status: Some("Ready".into()),
            error: Some("nope".into()),
            status_update_target: None,
            suffix_action: SUFFIX_ACTION_NONE.into(),
            suffixed_name: None,
            base_name: None,
            mentor_draft_action: MENTOR_ACTION_NONE.into(),
            archive_action: ARCHIVE_ACTION_NONE.into(),
            timestamp_event: None,
            timestamp_target_name: None,
            revert_siblings: false,
        };
        let value = serde_json::to_value(&plan).unwrap();
        // None fields must be JSON null, not omitted.
        assert!(value.get("status_update_target").unwrap().is_null());
        assert!(value.get("suffixed_name").unwrap().is_null());
        assert!(value.get("base_name").unwrap().is_null());
        assert!(value.get("timestamp_event").unwrap().is_null());
        assert!(value.get("timestamp_target_name").unwrap().is_null());
    }
}

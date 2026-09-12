//! Versioned bead-action policy wires.
//!
//! Hosts collect assigned-bead, commit-method, repository, and status facts.
//! This module owns the closed `close`/`keep` vocabulary, the distinction
//! between an omitted action and an explicit one, and rejection of the
//! retired `do_not_close_bead` input.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

pub const BEAD_ACTION_WIRE_SCHEMA_VERSION: u64 = 1;

pub const BEAD_ACTION_USAGE: &str = "use -B keep or -B close";

pub const BEAD_ACTION_CODE_MISSING: &str = "missing_bead_action";
pub const BEAD_ACTION_CODE_INVALID: &str = "invalid_bead_action";
pub const BEAD_ACTION_CODE_LEGACY: &str = "legacy_do_not_close_bead";
pub const BEAD_ACTION_CODE_CLOSE_WITHOUT_BEAD: &str =
    "close_without_assigned_bead";
pub const BEAD_ACTION_CODE_CLOSE_ON_PROPOSAL: &str = "close_on_proposal";
pub const BEAD_ACTION_CODE_CLOSE_REQUIRES_PRIMARY: &str =
    "close_requires_primary_repository";
pub const BEAD_ACTION_CODE_UNREADABLE_STATUS: &str = "unreadable_bead_status";
pub const BEAD_ACTION_CODE_INELIGIBLE_STATUS: &str = "close_ineligible_status";
pub const BEAD_ACTION_CODE_ASSIGNMENT_MISMATCH: &str = "assignment_mismatch";
pub const BEAD_ACTION_CODE_UNSUPPORTED_SCHEMA: &str =
    "unsupported_bead_action_schema";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{code}: {message}")]
pub struct BeadActionError {
    pub code: String,
    pub message: String,
}

impl BeadActionError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            message: message.into(),
        }
    }
}

/// Explicit stitch/commit bead decision. Serialized as `bead_action`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadActionWire {
    Close,
    Keep,
}

impl BeadActionWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Close => "close",
            Self::Keep => "keep",
        }
    }
}

/// Canonical commit methods the host must normalize to before policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadCommitMethodWire {
    CreateCommit,
    CreatePullRequest,
    CreateProposal,
}

/// Repository scope relative to the assigned bead's owning primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadRepositoryScopeWire {
    Primary,
    Linked,
    External,
    Sdd,
    Unknown,
}

/// Status fact needed to validate an explicit `close`.
///
/// `keep` must not require a successful lookup. `unchecked` means the host
/// did not read status; `unreadable` means the lookup failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadActionStatusFactWire {
    InProgress,
    Closed,
    Other,
    Unreadable,
    Unchecked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadActionDispositionWire {
    Keep,
    Close,
    CloseIdempotent,
    Unassociated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeadActionRequestWire {
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assigned_bead_id: Option<String>,
    pub commit_method: BeadCommitMethodWire,
    pub repository_scope: BeadRepositoryScopeWire,
    /// Positive identification that this repository is the owning primary.
    /// A failed lookup must not set this, even if scope claims primary.
    #[serde(default)]
    pub primary_repository_identified: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_action: Option<BeadActionWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_status: Option<BeadActionStatusFactWire>,
    #[serde(default)]
    pub legacy_do_not_close_bead: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BeadActionDecisionWire {
    pub schema_version: u64,
    pub disposition: BeadActionDispositionWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_id: Option<String>,
    pub close_bead: bool,
    pub message: String,
}

/// One `builtin@commit` repository decision to check against host bead context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FinalizerBeadDecisionWire {
    pub repo_id: String,
    pub commit_method: BeadCommitMethodWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_action: Option<BeadActionWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bead_status: Option<BeadActionStatusFactWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repository_scope: Option<BeadRepositoryScopeWire>,
    #[serde(default)]
    pub legacy_do_not_close_bead: bool,
}

fn legacy_error() -> BeadActionError {
    BeadActionError::new(
        BEAD_ACTION_CODE_LEGACY,
        format!(
            "--do-not-close-bead is no longer accepted; {BEAD_ACTION_USAGE}"
        ),
    )
}

fn invalid_action_error(value: &JsonValue) -> BeadActionError {
    BeadActionError::new(
        BEAD_ACTION_CODE_INVALID,
        format!("invalid bead_action {value}; {BEAD_ACTION_USAGE}"),
    )
}

/// Parse a JSON `bead_action` value. Rejects boolean, null, empty, and
/// unknown values. Does not treat JSON null as omitted; omission is the
/// absence of the field, handled by [`parse_bead_action_field`].
pub fn parse_bead_action_value(
    value: &JsonValue,
) -> Result<BeadActionWire, BeadActionError> {
    match value.as_str() {
        Some("close") => Ok(BeadActionWire::Close),
        Some("keep") => Ok(BeadActionWire::Keep),
        _ => Err(invalid_action_error(value)),
    }
}

/// Parse `bead_action` from a payload object, preserving absent vs explicit.
///
/// Presence of `do_not_close_bead` or `legacy_do_not_close_bead` is always
/// an error, including when the value is false.
pub fn parse_bead_action_field(
    payload: &JsonValue,
) -> Result<Option<BeadActionWire>, BeadActionError> {
    let Some(object) = payload.as_object() else {
        return Err(BeadActionError::new(
            BEAD_ACTION_CODE_INVALID,
            format!(
                "bead-action payload must be an object; {BEAD_ACTION_USAGE}"
            ),
        ));
    };
    if object.contains_key("do_not_close_bead")
        || object.get("legacy_do_not_close_bead")
            == Some(&JsonValue::Bool(true))
    {
        return Err(legacy_error());
    }
    match object.get("bead_action") {
        None => Ok(None),
        Some(value) => parse_bead_action_value(value).map(Some),
    }
}

pub fn assigned_bead_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod parse_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn parses_close_and_keep_and_preserves_absence() {
        assert_eq!(
            parse_bead_action_field(&json!({"bead_action": "close"})).unwrap(),
            Some(BeadActionWire::Close)
        );
        assert_eq!(
            parse_bead_action_field(&json!({"bead_action": "keep"})).unwrap(),
            Some(BeadActionWire::Keep)
        );
        assert_eq!(parse_bead_action_field(&json!({})).unwrap(), None);
    }

    #[test]
    fn rejects_boolean_null_empty_and_unknown_values() {
        for value in [
            json!(true),
            json!(false),
            json!(null),
            json!(""),
            json!("auto"),
            json!("yes"),
            json!(1),
            json!([]),
        ] {
            let error = parse_bead_action_value(&value).unwrap_err();
            assert_eq!(error.code, BEAD_ACTION_CODE_INVALID);
            assert!(error.message.contains(BEAD_ACTION_USAGE));
        }
    }

    #[test]
    fn rejects_legacy_do_not_close_bead_even_when_false() {
        let error = parse_bead_action_field(&json!({
            "do_not_close_bead": false,
            "bead_action": "keep"
        }))
        .unwrap_err();
        assert_eq!(error.code, BEAD_ACTION_CODE_LEGACY);
        assert!(error.message.contains("-B keep"));
        assert!(error.message.contains("-B close"));
    }

    #[test]
    fn bead_action_round_trips_through_serde() {
        assert_eq!(
            serde_json::to_value(BeadActionWire::Close).unwrap(),
            json!("close")
        );
        assert_eq!(
            serde_json::to_value(BeadActionWire::Keep).unwrap(),
            json!("keep")
        );
        let parsed: BeadActionWire =
            serde_json::from_value(json!("close")).unwrap();
        assert_eq!(parsed, BeadActionWire::Close);
    }
}

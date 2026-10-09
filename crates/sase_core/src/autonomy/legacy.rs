//! Legacy `%auto` meta translation: pre-E1 keys to and from the record.
//!
//! Readers that find no record translate the legacy keys exactly as
//! today's readers do (`get_auto_plan_approval_action`,
//! `_raw_auto_plan_argument`, and the `_plan_gate_metadata` coverage
//! tables). Writers behind the `autonomy_record_only` sunset flag
//! reproduce today's writer output from `record.selection` through
//! [`autonomy_legacy_projection`].

use serde_json::{Map, Value};

use super::profiles::{policy_digest, profile_policy};
use super::wires::{
    AutonomyLastWire, AutonomyLegacyProjectionWire, AutonomyRecordWire,
    AUTONOMY_PROFILE_EPIC, AUTONOMY_PROFILE_MANUAL, AUTONOMY_PROFILE_STANDARD,
    AUTONOMY_PROFILE_TALE, AUTONOMY_SOURCE_LEGACY,
    AUTONOMY_WIRE_SCHEMA_VERSION,
};

/// Valid legacy `auto_approve_plan_action` values; anything else (notably
/// the legacy `"plan"` action) normalizes to `None`.
const LEGACY_ACTIONS: &[&str] = &["approve", "epic", "tale"];

fn legacy_raw_argument(meta: &Map<String, Value>) -> (Option<String>, bool) {
    if !meta.contains_key("auto_approve_argument") {
        return (None, false);
    }
    match meta.get("auto_approve_argument") {
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                (None, true)
            } else {
                (Some(trimmed.to_string()), true)
            }
        }
        _ => (None, false),
    }
}

fn legacy_action(meta: &Map<String, Value>) -> Option<String> {
    match meta.get("auto_approve_plan_action") {
        Some(Value::String(raw)) => {
            let normalized = raw.trim().to_ascii_lowercase();
            if LEGACY_ACTIONS.contains(&normalized.as_str()) {
                Some(normalized)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn legacy_approve(meta: &Map<String, Value>) -> bool {
    match meta.get("approve") {
        Some(Value::Bool(flag)) => *flag,
        Some(Value::Number(number)) => {
            number.as_i64().is_some_and(|value| value != 0)
        }
        Some(Value::String(raw)) => {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        }
        _ => false,
    }
}

fn tale_covered(argument: Option<&str>) -> bool {
    matches!(argument, None | Some("") | Some("plan") | Some("tale"))
}

fn epic_covered(argument: Option<&str>) -> bool {
    matches!(argument, None | Some("") | Some("epic") | Some("epic_plan"))
}

/// Translate pre-E1 `%auto` meta keys to an autonomy record.
///
/// Covers every shape today's writers produce: bare (`approve`), `:plan`
/// (`approve` plus argument), `:tale`/`:epic` (action plus argument plus
/// `plan`), toggle-on bare, revive action-only, and the legacy `"plan"`
/// action. `source` is always `legacy`.
pub fn autonomy_record_from_legacy_meta(
    meta: &Map<String, Value>,
) -> AutonomyRecordWire {
    let (raw_argument, has_raw_argument) = legacy_raw_argument(meta);
    let action = legacy_action(meta);
    let approve = legacy_approve(meta);
    let enabled = has_raw_argument || action.is_some() || approve;
    // The action stands in for a missing argument, exactly as
    // `effective_plan_auto_argument` does.
    let effective: Option<String> = if has_raw_argument {
        raw_argument
    } else {
        match action.as_deref() {
            Some("tale") | Some("epic") => action.clone(),
            _ => None,
        }
    };
    let (profile, selection) = if !enabled {
        (AUTONOMY_PROFILE_MANUAL, "manual".to_string())
    } else {
        match effective.as_deref() {
            None | Some("") => (AUTONOMY_PROFILE_STANDARD, String::new()),
            Some("plan") | Some("tale") => {
                (AUTONOMY_PROFILE_TALE, effective.clone().unwrap_or_default())
            }
            Some("epic") | Some("epic_plan") => {
                (AUTONOMY_PROFILE_EPIC, "epic".to_string())
            }
            // Enabled but covering no tier (a stale unknown argument):
            // fail closed to manual.
            Some(_) => (AUTONOMY_PROFILE_MANUAL, "manual".to_string()),
        }
    };
    // Sanity: the profile must agree with the coverage tables.
    debug_assert_eq!(
        profile == AUTONOMY_PROFILE_STANDARD,
        tale_covered(effective.as_deref())
            && epic_covered(effective.as_deref())
            && enabled
    );
    let policy = profile_policy(profile);
    let digest = policy_digest(&policy);
    let last = if profile == AUTONOMY_PROFILE_MANUAL {
        None
    } else {
        Some(AutonomyLastWire {
            profile: profile.to_string(),
            selection: selection.clone(),
        })
    };
    AutonomyRecordWire {
        schema_version: AUTONOMY_WIRE_SCHEMA_VERSION,
        profile: profile.to_string(),
        selection,
        policy,
        overrides: Default::default(),
        source: AUTONOMY_SOURCE_LEGACY.to_string(),
        inherited_from: None,
        last,
        revision: 1,
        digest,
        updated_at: None,
        updated_by: None,
    }
}

/// Reproduce today's writer output from a record's selection.
///
/// Selection `""` gives bare output (`approve` only); `"plan"` adds the
/// argument; `"tale"`/`"epic"` give action plus argument plus `plan`;
/// `"manual"` (and anything unrecognized) writes no keys. `prompt_mode`
/// is the mode for `set_prompt_auto_mode`.
pub fn autonomy_legacy_projection(
    record: &AutonomyRecordWire,
) -> AutonomyLegacyProjectionWire {
    match record.selection.as_str() {
        "" => AutonomyLegacyProjectionWire {
            approve: true,
            auto_approve_argument: None,
            auto_approve_plan_action: None,
            plan: false,
            prompt_mode: Some("plan".to_string()),
        },
        "plan" => AutonomyLegacyProjectionWire {
            approve: true,
            auto_approve_argument: Some("plan".to_string()),
            auto_approve_plan_action: None,
            plan: false,
            prompt_mode: Some("plan".to_string()),
        },
        "tale" => AutonomyLegacyProjectionWire {
            approve: false,
            auto_approve_argument: Some("tale".to_string()),
            auto_approve_plan_action: Some("tale".to_string()),
            plan: true,
            prompt_mode: Some("tale".to_string()),
        },
        "epic" => AutonomyLegacyProjectionWire {
            approve: false,
            auto_approve_argument: Some("epic".to_string()),
            auto_approve_plan_action: Some("epic".to_string()),
            plan: true,
            prompt_mode: Some("epic".to_string()),
        },
        _ => AutonomyLegacyProjectionWire {
            approve: false,
            auto_approve_argument: None,
            auto_approve_plan_action: None,
            plan: false,
            prompt_mode: None,
        },
    }
}

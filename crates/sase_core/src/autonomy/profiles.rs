//! Built-in E1 compatibility profiles and the policy digest.
//!
//! The four profiles are fixed core code, not configuration: a remote E1
//! host resolves a dispatched prompt's `%auto` to the identical record.
//! Config profiles arrive in E3.

use sha2::{Digest, Sha256};

use super::wires::{
    AutonomyGatesWire, AutonomyPolicyWire, AUTONOMY_ON_ASK_PARK,
    AUTONOMY_PROFILE_EPIC, AUTONOMY_PROFILE_MANUAL, AUTONOMY_PROFILE_STANDARD,
    AUTONOMY_PROFILE_TALE, AUTONOMY_VALUE_APPROVE,
    AUTONOMY_VALUE_APPROVE_ARCHIVE, AUTONOMY_VALUE_ASK, AUTONOMY_VALUE_FIRST,
};

/// The option IDs a policy value requires, in canonical order. The host
/// never executes part of a selection: every ID must be present.
pub fn required_option_ids(value: &str) -> Option<&'static [&'static str]> {
    match value {
        AUTONOMY_VALUE_APPROVE_ARCHIVE => Some(&["approve", "commit"]),
        AUTONOMY_VALUE_APPROVE => Some(&["approve"]),
        AUTONOMY_VALUE_FIRST => Some(&["submit"]),
        AUTONOMY_VALUE_ASK => Some(&[]),
        _ => None,
    }
}

/// The policy block for one built-in profile name.
pub fn profile_policy(profile: &str) -> AutonomyPolicyWire {
    let gates = match profile {
        AUTONOMY_PROFILE_STANDARD => AutonomyGatesWire {
            plan: Some(AUTONOMY_VALUE_APPROVE_ARCHIVE.to_string()),
            epic: Some(AUTONOMY_VALUE_APPROVE.to_string()),
            question: Some(AUTONOMY_VALUE_FIRST.to_string()),
        },
        AUTONOMY_PROFILE_TALE => AutonomyGatesWire {
            plan: Some(AUTONOMY_VALUE_APPROVE_ARCHIVE.to_string()),
            epic: Some(AUTONOMY_VALUE_ASK.to_string()),
            question: Some(AUTONOMY_VALUE_FIRST.to_string()),
        },
        AUTONOMY_PROFILE_EPIC => AutonomyGatesWire {
            plan: Some(AUTONOMY_VALUE_ASK.to_string()),
            epic: Some(AUTONOMY_VALUE_APPROVE.to_string()),
            question: Some(AUTONOMY_VALUE_FIRST.to_string()),
        },
        _ => AutonomyGatesWire {
            plan: Some(AUTONOMY_VALUE_ASK.to_string()),
            epic: Some(AUTONOMY_VALUE_ASK.to_string()),
            question: Some(AUTONOMY_VALUE_ASK.to_string()),
        },
    };
    AutonomyPolicyWire {
        gates,
        on_ask: AUTONOMY_ON_ASK_PARK.to_string(),
    }
}

/// The four reserved E1 profile names.
pub fn builtin_profiles() -> &'static [&'static str] {
    &[
        AUTONOMY_PROFILE_MANUAL,
        AUTONOMY_PROFILE_STANDARD,
        AUTONOMY_PROFILE_TALE,
        AUTONOMY_PROFILE_EPIC,
    ]
}

/// Canonical JSON of a policy block: fixed key order, every gate key
/// always present (`null` when unmentioned), values JSON-escaped.
fn canonical_policy_json(policy: &AutonomyPolicyWire) -> String {
    fn slot(value: &Option<String>) -> String {
        match value {
            Some(text) => serde_json::to_string(text)
                .unwrap_or_else(|_| "null".to_string()),
            None => "null".to_string(),
        }
    }
    let on_ask = serde_json::to_string(&policy.on_ask)
        .unwrap_or_else(|_| "null".to_string());
    format!(
        "{{\"gates\":{{\"epic\":{},\"plan\":{},\"question\":{}}},\"on_ask\":{}}}",
        slot(&policy.gates.epic),
        slot(&policy.gates.plan),
        slot(&policy.gates.question),
        on_ask,
    )
}

/// `canonical_json_sha256` of a policy block, stored as the record digest.
pub fn policy_digest(policy: &AutonomyPolicyWire) -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_policy_json(policy).as_bytes());
    hex::encode(hasher.finalize())
}

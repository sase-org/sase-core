//! `evaluate(record, request)`: one gate's automatic decision.
//!
//! The decision never depends on `primary_branch`, `default_selected`,
//! or option order: it runs over explicit option IDs only, and combined
//! selections are all-or-nothing. `deny` is reserved for E3 and never
//! produced here.

use std::collections::BTreeSet;

use super::profiles::required_option_ids;
use super::wires::{
    AutonomyDecisionWire, AutonomyEvaluateRequestWire, AutonomyRecordWire,
};

/// Gate kinds with an auto-allowable policy key.
const POLICY_KEYS: &[(&str, &str)] = &[
    ("plan", "plan"),
    ("epic_plan", "epic"),
    ("question", "question"),
];

/// Known gate kinds that always ask (launch, sudo, custom, hitl,
/// triage, snooze, stale-cleanup, plugins-required). Anything else
/// unmapped is an unknown kind; both ask.
const PRIVILEGED_KINDS: &[&str] = &[
    "launch",
    "sudo",
    "custom",
    "hitl",
    "task_triage",
    "bead_snooze",
    "flag_triage",
    "bead_stale_cleanup",
    "plugins_required",
];

fn ask(
    record: &AutonomyRecordWire,
    rule: &str,
    reason: String,
) -> AutonomyDecisionWire {
    AutonomyDecisionWire {
        outcome: "ask".to_string(),
        value: None,
        option_ids: Vec::new(),
        rule: rule.to_string(),
        reason,
        profile: record.profile.clone(),
        selection: record.selection.clone(),
        revision: record.revision,
        digest: record.digest.clone(),
        source: record.source.clone(),
    }
}

/// Evaluate one gate request against an autonomy record.
pub fn evaluate(
    record: &AutonomyRecordWire,
    request: &AutonomyEvaluateRequestWire,
) -> AutonomyDecisionWire {
    let key = match POLICY_KEYS
        .iter()
        .find(|(kind, _)| *kind == request.gate_kind.as_str())
    {
        Some((_, key)) => *key,
        None => {
            let rule = if PRIVILEGED_KINDS.contains(&request.gate_kind.as_str())
            {
                "not_auto_allowable"
            } else {
                "unknown_kind"
            };
            return ask(
                record,
                rule,
                format!(
                    "gate kind '{}' is not auto-allowable",
                    request.gate_kind
                ),
            );
        }
    };
    let gates = &record.policy.gates;
    let value = match key {
        "plan" => gates.plan.as_ref(),
        "epic" => gates.epic.as_ref(),
        _ => gates.question.as_ref(),
    };
    let Some(value) = value else {
        return ask(
            record,
            "unmentioned",
            format!("policy has no value for '{key}'"),
        );
    };
    if value == "ask" {
        return ask(
            record,
            &format!("gates.{key}"),
            format!("policy value for '{key}' is 'ask'"),
        );
    }
    if !request.capabilities.iter().any(|item| item == value) {
        return ask(
            record,
            "not_capable",
            format!("value '{value}' is outside the gate's capabilities"),
        );
    }
    let Some(required) = required_option_ids(value) else {
        return ask(
            record,
            "not_capable",
            format!("value '{value}' has no executable option mapping"),
        );
    };
    let offered: BTreeSet<&str> =
        request.option_ids.iter().map(String::as_str).collect();
    let missing: Vec<&&str> = required
        .iter()
        .filter(|id| !offered.contains(**id))
        .collect();
    if !missing.is_empty() {
        let names: Vec<String> =
            missing.iter().map(|id| id.to_string()).collect();
        return ask(
            record,
            "missing_options",
            format!(
                "required option ids missing from the request: {}",
                names.join(", ")
            ),
        );
    }
    let mut selected: Vec<String> =
        required.iter().map(|id| id.to_string()).collect();
    selected.sort();
    AutonomyDecisionWire {
        outcome: "auto".to_string(),
        value: Some(value.clone()),
        option_ids: selected,
        rule: format!("gates.{key}"),
        reason: format!("policy value '{value}' auto-selected for '{key}'"),
        profile: record.profile.clone(),
        selection: record.selection.clone(),
        revision: record.revision,
        digest: record.digest.clone(),
        source: record.source.clone(),
    }
}

/// Tier-aware auto-approval for fleet facts: `true` when `evaluate` over
/// the record (or the translated legacy keys when there is none) gives
/// `auto` for the plan tier's gate kind.
///
/// `approve`/`action` are the scanned legacy wire fields and `record` the
/// scanned `autonomy` record, if present. `plan_tier` is `tale`, `epic`,
/// or `None` when unreadable; an unreadable tier falls back to the legacy
/// boolean so facts never go missing.
pub fn fleet_auto_approved(
    approve: bool,
    action: Option<&str>,
    record: Option<&AutonomyRecordWire>,
    plan_tier: Option<&str>,
) -> bool {
    let legacy_bool = approve || action.is_some_and(|value| !value.is_empty());
    let gate_kind = match plan_tier {
        Some("tale") => "plan",
        Some("epic") => "epic_plan",
        _ => return legacy_bool,
    };
    let owned;
    let resolved: &AutonomyRecordWire = match record {
        Some(record) => record,
        None => {
            owned = super::legacy::autonomy_record_from_legacy_meta(
                &legacy_meta_map(approve, action),
            );
            &owned
        }
    };
    let decision = evaluate(
        resolved,
        &AutonomyEvaluateRequestWire {
            gate_kind: gate_kind.to_string(),
            option_ids: vec![
                "approve".to_string(),
                "commit".to_string(),
                "submit".to_string(),
            ],
            capabilities: vec![
                "approve_archive".to_string(),
                "approve".to_string(),
                "first".to_string(),
            ],
            request_id: None,
        },
    );
    decision.outcome == "auto"
}

fn legacy_meta_map(
    approve: bool,
    action: Option<&str>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut meta = serde_json::Map::new();
    if approve {
        meta.insert("approve".to_string(), serde_json::Value::Bool(true));
    }
    if let Some(action) = action.filter(|value| !value.is_empty()) {
        meta.insert(
            "auto_approve_plan_action".to_string(),
            serde_json::Value::String(action.to_string()),
        );
    }
    meta
}

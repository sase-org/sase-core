use std::collections::BTreeMap;

use crate::command_line::wire::{RunPolicyOutcomeWire, RunPolicyRuleWire};

pub fn json_scalar_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            value.to_string()
        }
    }
}

pub fn evaluate_run_policy(
    rules: &[RunPolicyRuleWire],
    parsed: &BTreeMap<String, Vec<String>>,
) -> RunPolicyOutcomeWire {
    for rule in rules {
        if rule_matches(rule, parsed) {
            return RunPolicyOutcomeWire {
                policy: rule.policy.clone(),
                note: rule.note.clone(),
            };
        }
    }
    RunPolicyOutcomeWire {
        policy: "proc".to_string(),
        note: None,
    }
}

fn rule_matches(
    rule: &RunPolicyRuleWire,
    parsed: &BTreeMap<String, Vec<String>>,
) -> bool {
    let Some(when) = &rule.when else {
        return true;
    };
    if let Some(absent) = &when.absent {
        for dest in absent {
            if parsed.get(dest).is_some_and(|v| !v.is_empty()) {
                return false;
            }
        }
        if when.equals.is_none() {
            return true;
        }
    }
    if let Some(equals) = &when.equals {
        for (dest, expected) in equals {
            let expected = json_scalar_string(expected);
            let Some(values) = parsed.get(dest) else {
                return false;
            };
            let Some(last) = values.last() else {
                return false;
            };
            if last != &expected {
                return false;
            }
        }
        return true;
    }
    true
}

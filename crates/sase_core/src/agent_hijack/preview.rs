//! Read-only recovery preview assembly.
//!
//! Python gathers ledger, header, and already-computed escalation facts.
//! This module does not walk the ladder or strip launch directives.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::escalation::AgentHijackEscalationResultWire;
use super::wire::*;

/// Wire schema version for recovery preview request/result types.
pub const AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION: u32 = 1;

const NOT_YET_AVAILABLE: &str = "not yet available";
const EXPIRED_REASON: &str = "workspace already released";
const RESOLVING_REASON: &str = "already being resolved";
const UNKNOWN_MODEL_REASON: &str = "stalled model is unknown";

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHijackPreviewError {
    #[error("unsupported hijack recovery preview wire schema_version {0}")]
    UnsupportedSchema(u32),
}

/// Header facts copied onto the preview for CLI/TUI rendering.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRecoveryHeaderWire {
    #[serde(default)]
    pub agent_name: String,
    #[serde(default)]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub model_display: String,
    #[serde(default)]
    pub run_duration_display: String,
    #[serde(default)]
    pub hijacked_ago: String,
    #[serde(default)]
    pub files_changed: u64,
    #[serde(default)]
    pub insertions: i64,
    #[serde(default)]
    pub deletions: i64,
    #[serde(default)]
    pub last_tool: String,
    #[serde(default)]
    pub last_tool_age: String,
}

/// One recovery-menu row.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRecoveryActionWire {
    #[serde(default)]
    pub action: String,
    #[serde(default)]
    pub available: bool,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub label: String,
    #[serde(default)]
    pub detail: String,
    #[serde(default)]
    pub target: Option<String>,
}

/// Preview request: record identity plus precomputed model facts.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRecoveryPreviewRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub hijack_id: String,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub state: String,
    #[serde(default)]
    pub header: AgentHijackRecoveryHeaderWire,
    #[serde(default)]
    pub escalate: Option<AgentHijackEscalationResultWire>,
    #[serde(default)]
    pub continue_target: Option<String>,
    #[serde(default)]
    pub restart_reason: Option<String>,
    #[serde(default)]
    pub edit_reason: Option<String>,
    #[serde(default)]
    pub dismiss_reason: Option<String>,
}

/// Side-effect-free recovery preview.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHijackRecoveryPreviewWire {
    pub schema_version: u32,
    #[serde(default)]
    pub hijack_id: String,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub header: AgentHijackRecoveryHeaderWire,
    #[serde(default)]
    pub default_action: Option<String>,
    #[serde(default)]
    pub actions: Vec<AgentHijackRecoveryActionWire>,
}

/// Assemble the keep-work recovery preview from already-computed facts.
pub fn hijack_recovery_preview(
    request: &AgentHijackRecoveryPreviewRequestWire,
) -> Result<AgentHijackRecoveryPreviewWire, AgentHijackPreviewError> {
    if request.schema_version
        != AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION
    {
        return Err(AgentHijackPreviewError::UnsupportedSchema(
            request.schema_version,
        ));
    }

    let keep_reason = keep_work_disabled_reason(&request.state);
    let escalate = escalate_action(request, keep_reason.as_deref());
    let continue_action = continue_action(request, keep_reason.as_deref());
    let instruct = instruct_action(&continue_action);
    let restart = unavailable_action(
        HIJACK_ACTION_RESTART,
        "Restart",
        request.restart_reason.as_deref(),
    );
    let edit = unavailable_action(
        HIJACK_ACTION_EDIT,
        "Edit",
        request.edit_reason.as_deref(),
    );
    let dismiss = unavailable_action(
        HIJACK_ACTION_DISMISS,
        "Dismiss",
        request.dismiss_reason.as_deref(),
    );

    let actions =
        vec![escalate, continue_action, instruct, restart, edit, dismiss];
    let default_action = default_keep_work_action(&actions);

    Ok(AgentHijackRecoveryPreviewWire {
        schema_version: AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION,
        hijack_id: request.hijack_id.clone(),
        revision: request.revision,
        header: request.header.clone(),
        default_action,
        actions,
    })
}

fn keep_work_disabled_reason(state: &str) -> Option<String> {
    match state {
        HIJACK_HELD => None,
        HIJACK_EXPIRED => Some(EXPIRED_REASON.to_owned()),
        HIJACK_RESOLVING => Some(RESOLVING_REASON.to_owned()),
        _ => Some(format!("keep-work recovery is not available in {state}")),
    }
}

fn escalate_action(
    request: &AgentHijackRecoveryPreviewRequestWire,
    keep_reason: Option<&str>,
) -> AgentHijackRecoveryActionWire {
    if let Some(reason) = keep_reason {
        return disabled(HIJACK_ACTION_ESCALATE, "Escalate", reason, None);
    }
    let Some(result) = request.escalate.as_ref() else {
        return disabled(
            HIJACK_ACTION_ESCALATE,
            "Escalate",
            "escalation target is unknown",
            None,
        );
    };
    let switch = result.kind.as_deref() == Some("switch");
    let label = if switch { "Switch" } else { "Escalate" };
    if !result.available {
        let reason = result.reason.as_deref().unwrap_or(
            "no different model available; use Continue or Instruct",
        );
        return disabled(
            HIJACK_ACTION_ESCALATE,
            label,
            reason,
            result.concrete.clone(),
        );
    }
    let target = result.concrete.clone();
    let detail = escalate_detail(result, switch, target.as_deref());
    AgentHijackRecoveryActionWire {
        action: HIJACK_ACTION_ESCALATE.to_owned(),
        available: true,
        reason: None,
        label: label.to_owned(),
        detail,
        target,
    }
}

fn escalate_detail(
    result: &AgentHijackEscalationResultWire,
    switch: bool,
    target: Option<&str>,
) -> String {
    let concrete = target.unwrap_or("");
    match (
        result.from_rung.as_deref(),
        result.to_rung.as_deref(),
        switch,
    ) {
        (Some(from), Some(to), false) => {
            format!("{from} → {to} · {concrete}")
        }
        (Some(_from), Some(to), true) => {
            format!("already {to} → {concrete}")
        }
        (None, Some(to), _) => format!("→ {to} · {concrete}"),
        _ => concrete.to_owned(),
    }
}

fn continue_action(
    request: &AgentHijackRecoveryPreviewRequestWire,
    keep_reason: Option<&str>,
) -> AgentHijackRecoveryActionWire {
    if let Some(reason) = keep_reason {
        return disabled(HIJACK_ACTION_CONTINUE, "Continue", reason, None);
    }
    let target = request
        .continue_target
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(target) = target else {
        return disabled(
            HIJACK_ACTION_CONTINUE,
            "Continue",
            UNKNOWN_MODEL_REASON,
            None,
        );
    };
    AgentHijackRecoveryActionWire {
        action: HIJACK_ACTION_CONTINUE.to_owned(),
        available: true,
        reason: None,
        label: "Continue".to_owned(),
        detail: format!("same model · {target}"),
        target: Some(target.to_owned()),
    }
}

fn instruct_action(
    continue_action: &AgentHijackRecoveryActionWire,
) -> AgentHijackRecoveryActionWire {
    AgentHijackRecoveryActionWire {
        action: HIJACK_ACTION_INSTRUCT.to_owned(),
        available: continue_action.available,
        reason: continue_action.reason.clone(),
        label: "Instruct".to_owned(),
        detail: if continue_action.available {
            "add guidance, then continue".to_owned()
        } else {
            String::new()
        },
        target: continue_action.target.clone(),
    }
}

fn unavailable_action(
    action: &str,
    label: &str,
    reason: Option<&str>,
) -> AgentHijackRecoveryActionWire {
    disabled(action, label, reason.unwrap_or(NOT_YET_AVAILABLE), None)
}

fn disabled(
    action: &str,
    label: &str,
    reason: &str,
    target: Option<String>,
) -> AgentHijackRecoveryActionWire {
    AgentHijackRecoveryActionWire {
        action: action.to_owned(),
        available: false,
        reason: Some(reason.to_owned()),
        label: label.to_owned(),
        detail: String::new(),
        target,
    }
}

fn default_keep_work_action(
    actions: &[AgentHijackRecoveryActionWire],
) -> Option<String> {
    for wanted in [
        HIJACK_ACTION_ESCALATE,
        HIJACK_ACTION_CONTINUE,
        HIJACK_ACTION_INSTRUCT,
    ] {
        if actions
            .iter()
            .any(|action| action.action == wanted && action.available)
        {
            return Some(wanted.to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_hijack::escalation::AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION;

    fn request() -> AgentHijackRecoveryPreviewRequestWire {
        AgentHijackRecoveryPreviewRequestWire {
            schema_version: AGENT_HIJACK_RECOVERY_PREVIEW_WIRE_SCHEMA_VERSION,
            hijack_id: "h-1".to_owned(),
            revision: 4,
            state: HIJACK_HELD.to_owned(),
            header: AgentHijackRecoveryHeaderWire {
                agent_name: "sase-7f.2".to_owned(),
                workspace_num: Some(4),
                model_display: "claude/opus@high".to_owned(),
                ..Default::default()
            },
            escalate: Some(AgentHijackEscalationResultWire {
                schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
                available: true,
                kind: Some("escalate".to_owned()),
                from_rung: Some("@large".to_owned()),
                to_rung: Some("@xlarge".to_owned()),
                concrete: Some("claude/opus@xhigh".to_owned()),
                reason: None,
            }),
            continue_target: Some("claude/opus@high".to_owned()),
            ..Default::default()
        }
    }

    fn action<'a>(
        preview: &'a AgentHijackRecoveryPreviewWire,
        name: &str,
    ) -> &'a AgentHijackRecoveryActionWire {
        preview
            .actions
            .iter()
            .find(|action| action.action == name)
            .unwrap()
    }

    #[test]
    fn held_preview_defaults_to_escalate_and_pins_continue() {
        let preview = hijack_recovery_preview(&request()).unwrap();
        assert_eq!(preview.hijack_id, "h-1");
        assert_eq!(preview.revision, 4);
        assert_eq!(
            preview.default_action.as_deref(),
            Some(HIJACK_ACTION_ESCALATE)
        );
        let escalate = action(&preview, HIJACK_ACTION_ESCALATE);
        assert!(escalate.available);
        assert_eq!(escalate.label, "Escalate");
        assert_eq!(escalate.detail, "@large → @xlarge · claude/opus@xhigh");
        assert_eq!(escalate.target.as_deref(), Some("claude/opus@xhigh"));
        let continued = action(&preview, HIJACK_ACTION_CONTINUE);
        assert!(continued.available);
        assert_eq!(continued.target.as_deref(), Some("claude/opus@high"));
        assert_eq!(continued.detail, "same model · claude/opus@high");
        assert!(action(&preview, HIJACK_ACTION_INSTRUCT).available);
        assert!(!action(&preview, HIJACK_ACTION_RESTART).available);
        assert_eq!(
            action(&preview, HIJACK_ACTION_RESTART).reason.as_deref(),
            Some(NOT_YET_AVAILABLE)
        );
        assert_eq!(
            action(&preview, HIJACK_ACTION_DISMISS).reason.as_deref(),
            Some(NOT_YET_AVAILABLE)
        );
    }

    #[test]
    fn switch_is_the_escalate_row_and_unavailable_escalate_falls_to_continue() {
        let mut req = request();
        req.escalate = Some(AgentHijackEscalationResultWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            available: true,
            kind: Some("switch".to_owned()),
            from_rung: Some("@xlarge".to_owned()),
            to_rung: Some("@xlarge".to_owned()),
            concrete: Some("codex/gpt-6.1-sol@xhigh".to_owned()),
            reason: None,
        });
        let preview = hijack_recovery_preview(&req).unwrap();
        let escalate = action(&preview, HIJACK_ACTION_ESCALATE);
        assert_eq!(escalate.label, "Switch");
        assert_eq!(
            escalate.detail,
            "already @xlarge → codex/gpt-6.1-sol@xhigh"
        );

        req.escalate = Some(AgentHijackEscalationResultWire {
            schema_version: AGENT_HIJACK_ESCALATION_WIRE_SCHEMA_VERSION,
            available: false,
            kind: Some("switch".to_owned()),
            from_rung: Some("@xlarge".to_owned()),
            to_rung: Some("@xlarge".to_owned()),
            concrete: None,
            reason: Some(
                "no different model available; use Continue or Instruct"
                    .to_owned(),
            ),
        });
        let preview = hijack_recovery_preview(&req).unwrap();
        assert!(!action(&preview, HIJACK_ACTION_ESCALATE).available);
        assert_eq!(
            preview.default_action.as_deref(),
            Some(HIJACK_ACTION_CONTINUE)
        );
    }

    #[test]
    fn expired_and_resolving_disable_keep_work() {
        let mut req = request();
        req.state = HIJACK_EXPIRED.to_owned();
        let preview = hijack_recovery_preview(&req).unwrap();
        assert!(preview.default_action.is_none());
        assert_eq!(
            action(&preview, HIJACK_ACTION_ESCALATE).reason.as_deref(),
            Some(EXPIRED_REASON)
        );
        assert_eq!(
            action(&preview, HIJACK_ACTION_CONTINUE).reason.as_deref(),
            Some(EXPIRED_REASON)
        );

        req.state = HIJACK_RESOLVING.to_owned();
        let preview = hijack_recovery_preview(&req).unwrap();
        assert_eq!(
            action(&preview, HIJACK_ACTION_CONTINUE).reason.as_deref(),
            Some(RESOLVING_REASON)
        );
    }

    #[test]
    fn rejects_unsupported_schema() {
        let mut req = request();
        req.schema_version = 2;
        assert!(hijack_recovery_preview(&req).is_err());
    }
}

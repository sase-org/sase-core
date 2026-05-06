use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::{json, Value};
use thiserror::Error;

use crate::wire::GATEWAY_WIRE_SCHEMA_VERSION;

pub fn api_v1_contract_snapshot() -> Value {
    json!({
        "schema_version": GATEWAY_WIRE_SCHEMA_VERSION,
        "contract": "sase_mobile_gateway_api_v1",
        "base_path": "/api/v1",
        "response_shape": {
            "success": "direct_json_record",
            "error": "ApiErrorWire",
            "optional_fields": "explicit_null"
        },
        "auth": {
            "scheme": "bearer",
            "header": "Authorization",
            "unauthenticated_routes": [
                "GET /api/v1/health",
                "POST /api/v1/session/pair/start",
                "POST /api/v1/session/pair/finish"
            ]
        },
        "routes": [
            {
                "method": "GET",
                "path": "/api/v1/health",
                "auth": false,
                "success": "HealthResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/session/pair/start",
                "auth": false,
                "request": "PairStartRequestWire",
                "success": "PairStartResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/session/pair/finish",
                "auth": false,
                "request": "PairFinishRequestWire",
                "success": "PairFinishResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/session",
                "auth": true,
                "success": "SessionResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/events",
                "auth": true,
                "success": "EventRecordWire stream",
                "protocol": "server_sent_events",
                "resume_header": "Last-Event-ID",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/agents",
                "auth": true,
                "query": "MobileAgentListRequestWire fields as URL query parameters",
                "success": "MobileAgentListResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/agents/resume-options",
                "auth": true,
                "success": "MobileAgentResumeOptionsResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/agents/launch",
                "auth": true,
                "request": "MobileAgentTextLaunchRequestWire",
                "success": "MobileAgentLaunchResultWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/agents/launch-image",
                "auth": true,
                "request": "MobileAgentImageLaunchRequestWire",
                "success": "MobileAgentLaunchResultWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/agents/{name}/kill",
                "auth": true,
                "request": "MobileAgentKillRequestWire",
                "success": "MobileAgentKillResultWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/agents/{name}/retry",
                "auth": true,
                "request": "MobileAgentRetryRequestWire",
                "success": "MobileAgentRetryResultWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/notifications",
                "auth": true,
                "query": "MobileNotificationListRequestWire fields as URL query parameters",
                "success": "MobileNotificationListResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/notifications/{id}",
                "auth": true,
                "success": "MobileNotificationDetailResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/notifications/{id}/mark-read",
                "auth": true,
                "success": "NotificationStateMutationResponseWire",
                "events_on_success": ["notifications_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/notifications/{id}/dismiss",
                "auth": true,
                "success": "NotificationStateMutationResponseWire",
                "events_on_success": ["notifications_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/attachments/{token}",
                "auth": true,
                "success": "attachment bytes",
                "protocol": "http_download",
                "headers": [
                    "Content-Length",
                    "Content-Type when known",
                    "Content-Disposition"
                ],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/approve",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/run",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/reject",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/epic",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/legend",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/plan/{prefix}/feedback",
                "auth": true,
                "request": "PlanActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/hitl/{prefix}/accept",
                "auth": true,
                "request": "HitlActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/hitl/{prefix}/reject",
                "auth": true,
                "request": "HitlActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/hitl/{prefix}/feedback",
                "auth": true,
                "request": "HitlActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/question/{prefix}/answer",
                "auth": true,
                "request": "QuestionActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/actions/question/{prefix}/custom",
                "auth": true,
                "request": "QuestionActionRequestWire without path-derived prefix/choice",
                "success": "ActionResultWire",
                "errors": ["ApiErrorWire"]
            }
        ],
        "records": {
            "ApiErrorWire": {
                "schema_version": "u32",
                "code": [
                    "unauthorized",
                    "not_found",
                    "invalid_request",
                    "pairing_expired",
                    "pairing_rejected",
                    "conflict_already_handled",
                    "gone_stale",
                    "ambiguous_prefix",
                    "unsupported_action",
                    "attachment_expired",
                    "agent_not_found",
                    "agent_not_running",
                    "launch_failed",
                    "invalid_upload",
                    "bridge_unavailable",
                    "permission_denied",
                    "internal"
                ],
                "message": "string",
                "target": "string|null",
                "details": "json|null"
            },
            "ActionResultWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "action_kind": "plan_approval|hitl|user_question|non_action|unsupported",
                "prefix": "string",
                "notification_id": "string|null",
                "state": "available|already_handled|stale|missing_request|missing_target|unsupported",
                "response_file": "plan_response.json|hitl_response.json|question_response.json",
                "response_json": "json",
                "message": "string|null"
            },
            "DeviceRecordWire": {
                "schema_version": "u32",
                "device_id": "string",
                "display_name": "string",
                "platform": "string",
                "app_version": "string|null",
                "paired_at": "rfc3339|null",
                "last_seen_at": "rfc3339|null",
                "revoked_at": "rfc3339|null"
            },
            "EventRecordWire": {
                "schema_version": "u32",
                "id": "string",
                "created_at": "rfc3339",
                "payload": "EventPayloadWire"
            },
            "EventPayloadWire": {
                "heartbeat": {
                    "sequence": "u64"
                },
                "session": {
                    "device_id": "string"
                },
                "resync_required": {
                    "reason": "string"
                },
                "notifications_changed": {
                    "reason": "string",
                    "notification_id": "string|null"
                },
                "agents_changed": {
                    "reason": "string",
                    "agent_name": "string|null",
                    "timestamp": "rfc3339|null"
                }
            },
            "GatewayBindWire": {
                "address": "host:port",
                "is_loopback": "bool"
            },
            "GatewayBuildWire": {
                "package_version": "string",
                "git_sha": "string|null"
            },
            "HealthResponseWire": {
                "schema_version": "u32",
                "status": "ok",
                "service": "sase_gateway",
                "version": "string",
                "build": "GatewayBuildWire",
                "bind": "GatewayBindWire"
            },
            "HitlActionRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "prefix": "string",
                "choice": "accept|reject|feedback",
                "feedback": "string|null"
            },
            "MobileAttachmentManifestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "id": "string",
                "token": "short-lived string|null; only detail responses mint downloadable tokens",
                "display_name": "string",
                "kind": "markdown|pdf|diff|image|text|json|directory|unknown",
                "content_type": "string|null",
                "byte_size": "u64|null",
                "source_notification_id": "string",
                "downloadable": "bool; false for missing, oversized, symlinked, traversal, directory, or unknown-risk files",
                "download_requires_auth": "bool",
                "can_inline": "bool",
                "path_available": "bool"
            },
            "MobileAgentActionAffordancesWire": {
                "can_resume": "bool",
                "can_wait": "bool",
                "can_kill": "bool",
                "can_retry": "bool"
            },
            "MobileAgentDisplayLabelsWire": {
                "title": "string",
                "subtitle": "string|null",
                "status_label": "string"
            },
            "MobileAgentImageLaunchRequestWire": {
                "schema_version": "u32",
                "prompt": "string",
                "original_filename": "string",
                "content_type": "string",
                "byte_length": "u64",
                "base64_image": "base64 string",
                "device_id": "string|null; host-injected before bridge dispatch",
                "display_name": "string|null",
                "name": "string|null",
                "model": "string|null",
                "provider": "string|null",
                "runtime": "string|null",
                "project": "string|null",
                "dry_run": "bool|null"
            },
            "MobileAgentKillRequestWire": {
                "schema_version": "u32",
                "reason": "string|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileAgentKillResultWire": {
                "schema_version": "u32",
                "name": "string",
                "status": "string",
                "pid": "u32|null",
                "changed": "bool",
                "message": "string|null"
            },
            "MobileAgentLaunchResultWire": {
                "schema_version": "u32",
                "primary": "MobileAgentLaunchSlotResultWire|null",
                "slots": "MobileAgentLaunchSlotResultWire[]"
            },
            "MobileAgentLaunchSlotResultWire": {
                "slot_id": "string",
                "name": "string|null",
                "status": "launched|dry_run|failed",
                "artifact_dir": "string|null",
                "message": "string|null"
            },
            "MobileAgentListRequestWire": {
                "schema_version": "u32",
                "include_recent": "bool",
                "status": "string|null",
                "project": "string|null",
                "limit": "u32|null"
            },
            "MobileAgentListResponseWire": {
                "schema_version": "u32",
                "agents": "MobileAgentSummaryWire[]",
                "total_count": "u64"
            },
            "MobileAgentResumeOptionWire": {
                "id": "string",
                "agent_name": "string",
                "kind": "resume|wait",
                "label": "string",
                "prompt_text": "string",
                "direct_launch_supported": "bool"
            },
            "MobileAgentResumeOptionsResponseWire": {
                "schema_version": "u32",
                "options": "MobileAgentResumeOptionWire[]"
            },
            "MobileAgentRetryLineageWire": {
                "retry_of_timestamp": "string|null",
                "retried_as_timestamp": "string|null",
                "retry_chain_root_timestamp": "string|null",
                "retry_attempt": "u32|null",
                "parent_agent_name": "string|null"
            },
            "MobileAgentRetryRequestWire": {
                "schema_version": "u32",
                "prompt_override": "string|null",
                "dry_run": "bool|null",
                "kill_source_first": "bool|null",
                "device_id": "string|null"
            },
            "MobileAgentRetryResultWire": {
                "schema_version": "u32",
                "source_agent": "string",
                "launch": "MobileAgentLaunchResultWire"
            },
            "MobileAgentSummaryWire": {
                "name": "string",
                "project": "string|null",
                "status": "string",
                "pid": "u32|null",
                "model": "string|null",
                "provider": "string|null",
                "workspace_number": "u32|null",
                "started_at": "rfc3339|null",
                "duration_seconds": "u64|null",
                "prompt_snippet": "string|null",
                "has_artifact_dir": "bool",
                "retry_lineage": "MobileAgentRetryLineageWire",
                "actions": "MobileAgentActionAffordancesWire",
                "display": "MobileAgentDisplayLabelsWire"
            },
            "MobileAgentTextLaunchRequestWire": {
                "schema_version": "u32",
                "prompt": "string",
                "display_name": "string|null",
                "name": "string|null",
                "model": "string|null",
                "provider": "string|null",
                "runtime": "string|null",
                "project": "string|null",
                "dry_run": "bool|null"
            },
            "MobileNotificationCardWire": {
                "defined_by": "sase_core::notifications::mobile",
                "id": "string",
                "timestamp": "rfc3339",
                "sender": "string",
                "priority": "bool",
                "actionable": "bool",
                "read": "bool",
                "dismissed": "bool",
                "silent": "bool",
                "muted": "bool",
                "notes_summary": "string",
                "file_count": "u64",
                "action_summary": "MobileActionSummaryWire|null"
            },
            "MobileNotificationDetailResponseWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "notification": "MobileNotificationCardWire",
                "notes": "string[]",
                "attachments": "MobileAttachmentManifestWire[]",
                "action": "MobileActionDetailWire"
            },
            "MobileNotificationListRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "unread_only": "bool",
                "include_dismissed": "bool",
                "include_silent": "bool",
                "limit": "u32|null",
                "newer_than": "string|null"
            },
            "MobileNotificationListResponseWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "notifications": "MobileNotificationCardWire[]",
                "total_count": "u64",
                "next_high_water": "string|null"
            },
            "NotificationStateMutationResponseWire": {
                "schema_version": "u32",
                "notification_id": "string",
                "read": "bool",
                "dismissed": "bool",
                "changed": "bool; false when the requested state was already set"
            },
            "PairFinishRequestWire": {
                "schema_version": "u32",
                "pairing_id": "string",
                "code": "string",
                "device": "PairingDeviceMetadataWire"
            },
            "PairFinishResponseWire": {
                "schema_version": "u32",
                "device": "DeviceRecordWire",
                "token_type": "bearer",
                "token": "string"
            },
            "PairStartRequestWire": {
                "schema_version": "u32",
                "host_label": "string|null"
            },
            "PairStartResponseWire": {
                "schema_version": "u32",
                "pairing_id": "string",
                "code": "string",
                "expires_at": "rfc3339",
                "host_label": "string",
                "host_fingerprint": "string|null"
            },
            "PairingDeviceMetadataWire": {
                "display_name": "string",
                "platform": "string",
                "app_version": "string|null"
            },
            "PlanActionRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "prefix": "string",
                "choice": "approve|run|reject|epic|legend|feedback",
                "feedback": "string|null",
                "commit_plan": "bool|null",
                "run_coder": "bool|null",
                "coder_prompt": "string|null",
                "coder_model": "string|null"
            },
            "HitlActionRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "prefix": "string",
                "choice": "accept|reject|feedback",
                "feedback": "string|null"
            },
            "QuestionActionRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "prefix": "string",
                "choice": "answer|custom",
                "question_index": "u32|null",
                "selected_option_id": "string|null",
                "selected_option_label": "string|null",
                "selected_option_index": "u32|null",
                "custom_answer": "string|null",
                "global_note": "string|null"
            },
            "SessionResponseWire": {
                "schema_version": "u32",
                "device": "DeviceRecordWire",
                "capabilities": "string[]"
            }
        },
        "examples": {
            "pair_start_request": {
                "schema_version": 1,
                "host_label": "workstation"
            },
            "pair_finish_request": {
                "schema_version": 1,
                "pairing_id": "pair_abc123",
                "code": "123456",
                "device": {
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0"
                }
            },
            "authorization_header": "Authorization: Bearer sase_mobile_<token>"
        }
    })
}

pub fn write_api_v1_contract_snapshot(
    path: impl AsRef<Path>,
) -> Result<(), ContractSnapshotError> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| {
            ContractSnapshotError::CreateParent {
                path: parent.to_path_buf(),
                source,
            }
        })?;
    }
    let mut bytes = serde_json::to_vec_pretty(&api_v1_contract_snapshot())?;
    bytes.push(b'\n');
    fs::write(path, bytes).map_err(|source| ContractSnapshotError::Write {
        path: path.to_path_buf(),
        source,
    })
}

#[derive(Debug, Error)]
pub enum ContractSnapshotError {
    #[error("failed to create contract snapshot parent {path}: {source}")]
    CreateParent {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to serialize contract snapshot: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("failed to write contract snapshot {path}: {source}")]
    Write {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_contract_snapshot_is_current() {
        let committed = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("contracts/api_v1/mobile_api_v1.json"),
        )
        .unwrap();
        let mut expected =
            serde_json::to_string_pretty(&api_v1_contract_snapshot()).unwrap();
        expected.push('\n');
        assert_eq!(committed, expected);
    }
}

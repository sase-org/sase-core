use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::{json, Value};
use thiserror::Error;

use crate::wire::{
    GATEWAY_WIRE_SCHEMA_VERSION, LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
    LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION, LOCAL_DAEMON_MAX_PAGE_LIMIT,
    LOCAL_DAEMON_MAX_PAYLOAD_BYTES, LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
    LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
};

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
                "path": "/api/v1/session/push-subscriptions",
                "auth": true,
                "success": "PushSubscriptionListResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/session/push-subscriptions",
                "auth": true,
                "request": "PushSubscriptionRequestWire",
                "success": "PushSubscriptionRegisterResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "DELETE",
                "path": "/api/v1/session/push-subscriptions/{id}",
                "auth": true,
                "success": "PushSubscriptionDeleteResponseWire",
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
                "path": "/api/v1/changespec-tags",
                "auth": true,
                "query": "MobileChangeSpecTagListRequestWire fields as URL query parameters",
                "success": "MobileChangeSpecTagListResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/xprompts/catalog",
                "auth": true,
                "query": "MobileXpromptCatalogRequestWire fields as URL query parameters",
                "success": "MobileXpromptCatalogResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/beads",
                "auth": true,
                "query": "MobileBeadListRequestWire fields as URL query parameters",
                "success": "MobileBeadListResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/beads/{id}",
                "auth": true,
                "query": "MobileBeadShowRequestWire fields as URL query parameters plus path bead_id",
                "success": "MobileBeadShowResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/v1/update/start",
                "auth": true,
                "request": "MobileUpdateStartRequestWire",
                "success": "MobileUpdateStartResponseWire",
                "events_on_success": ["helpers_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/v1/update/{job_id}",
                "auth": true,
                "success": "MobileUpdateStatusResponseWire",
                "events_on_success": ["helpers_changed"],
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
                    "helper_not_found",
                    "update_already_running",
                    "update_job_not_found",
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
                },
                "helpers_changed": {
                    "reason": "string",
                    "helper": "string|null",
                    "job_id": "string|null",
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
                "bind": "GatewayBindWire",
                "push": "PushGatewayStatusWire"
            },
            "PushGatewayStatusWire": {
                "provider": "disabled|test|fcm",
                "enabled": "bool",
                "attempted": "u64",
                "succeeded": "u64",
                "failed": "u64",
                "last_attempt_at": "rfc3339|null",
                "last_success_at": "rfc3339|null",
                "last_failure_at": "rfc3339|null",
                "last_failure": "string|null; non-secret diagnostic summary"
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
                "request_id": "string|null; client-provided launch correlation ID preserved in mobile launch context",
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
                "device_id": "string|null; host-injected before bridge dispatch",
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
                "request_id": "string|null; client-provided retry correlation ID preserved in mobile launch context",
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
                "request_id": "string|null; client-provided launch correlation ID preserved in mobile launch context",
                "display_name": "string|null",
                "name": "string|null",
                "model": "string|null",
                "provider": "string|null",
                "runtime": "string|null",
                "project": "string|null",
                "device_id": "string|null; host-injected before bridge dispatch",
                "dry_run": "bool|null"
            },
            "MobileHelperResultWire": {
                "status": "success|partial_success|skipped|failed",
                "message": "string|null",
                "warnings": "string[]",
                "skipped": "MobileHelperSkippedWire[]",
                "partial_failure_count": "u32|null"
            },
            "MobileHelperSkippedWire": {
                "target": "string|null",
                "reason": "string"
            },
            "MobileHelperProjectContextWire": {
                "project": "string|null",
                "scope": "explicit|device_default|all_known|unspecified"
            },
            "MobileChangeSpecTagListRequestWire": {
                "schema_version": "u32",
                "project": "string|null",
                "limit": "u32|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileChangeSpecTagListResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "context": "MobileHelperProjectContextWire",
                "tags": "MobileChangeSpecTagEntryWire[]",
                "total_count": "u64"
            },
            "MobileChangeSpecTagEntryWire": {
                "tag": "string",
                "project": "string|null",
                "changespec": "string",
                "title": "string|null",
                "status": "string",
                "workflow": "string|null",
                "source_path_display": "string|null"
            },
            "MobileXpromptCatalogRequestWire": {
                "schema_version": "u32",
                "project": "string|null",
                "source": "string|null",
                "tag": "string|null",
                "query": "string|null",
                "include_pdf": "bool",
                "limit": "u32|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileXpromptCatalogResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "context": "MobileHelperProjectContextWire",
                "entries": "MobileXpromptCatalogEntryWire[]",
                "stats": "MobileXpromptCatalogStatsWire",
                "catalog_attachment": "MobileXpromptCatalogAttachmentWire|null"
            },
            "MobileXpromptCatalogEntryWire": {
                "name": "string",
                "display_label": "string",
                "insertion": "string|null; fallback to #<name> when absent",
                "reference_prefix": "string|null",
                "kind": "string|null",
                "description": "string|null",
                "source_bucket": "string",
                "project": "string|null",
                "tags": "string[]",
                "input_signature": "string|null",
                "inputs": "MobileXpromptInputWire[]; default [] when absent",
                "is_skill": "bool",
                "content_preview": "string|null",
                "source_path_display": "string|null"
            },
            "MobileXpromptInputWire": {
                "name": "string",
                "type": "string",
                "required": "bool",
                "default_display": "string|null",
                "position": "u32"
            },
            "MobileXpromptCatalogStatsWire": {
                "total_count": "u64",
                "project_count": "u64",
                "skill_count": "u64",
                "pdf_requested": "bool"
            },
            "MobileXpromptCatalogAttachmentWire": {
                "display_name": "string",
                "content_type": "string|null",
                "byte_size": "u64|null",
                "path_display": "string|null",
                "generated": "bool"
            },
            "MobileBeadListRequestWire": {
                "schema_version": "u32",
                "project": "string|null",
                "all_projects": "bool",
                "status": "string|null",
                "bead_type": "string|null",
                "tier": "string|null",
                "include_closed": "bool",
                "limit": "u32|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileBeadListResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "context": "MobileHelperProjectContextWire",
                "beads": "MobileBeadSummaryWire[]",
                "total_count": "u64"
            },
            "MobileBeadShowRequestWire": {
                "schema_version": "u32",
                "bead_id": "string",
                "project": "string|null",
                "all_projects": "bool",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileBeadShowResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "context": "MobileHelperProjectContextWire",
                "bead": "MobileBeadDetailWire"
            },
            "MobileBeadSummaryWire": {
                "id": "string",
                "title": "string",
                "status": "string",
                "bead_type": "string",
                "tier": "string|null",
                "project": "string|null",
                "parent_id": "string|null",
                "assignee": "string|null",
                "updated_at": "rfc3339|null",
                "dependency_count": "u64",
                "block_count": "u64",
                "child_count": "u64",
                "plan_path_display": "string|null",
                "changespec_name": "string|null",
                "changespec_status": "string|null"
            },
            "MobileBeadDetailWire": {
                "summary": "MobileBeadSummaryWire",
                "description": "string|null",
                "notes": "string|null",
                "design_path_display": "string|null",
                "dependencies": "string[]",
                "blocks": "string[]",
                "children": "string[]",
                "workspace_display": "string|null"
            },
            "MobileUpdateStartRequestWire": {
                "schema_version": "u32",
                "request_id": "string|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileUpdateStartResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "job": "MobileUpdateJobWire"
            },
            "MobileUpdateStatusRequestWire": {
                "schema_version": "u32",
                "job_id": "string",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobileUpdateStatusResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "job": "MobileUpdateJobWire"
            },
            "MobileUpdateJobWire": {
                "job_id": "string",
                "status": "queued|running|succeeded|failed",
                "started_at": "rfc3339|null",
                "finished_at": "rfc3339|null",
                "message": "string|null",
                "log_path_display": "string|null",
                "completion_path_display": "string|null"
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
            "PushHintWire": {
                "schema_version": "u32",
                "id": "string; event id used as push hint id",
                "created_at": "rfc3339",
                "category": "notifications|agents|helpers|update|session",
                "reason": "string",
                "notification_id": "string|null",
                "agent_name": "string|null; only simple non-sensitive identifiers are included",
                "helper": "string|null",
                "job_id": "string|null",
                "title": "short safe display text",
                "body": "short safe display text"
            },
            "PushSubscriptionRequestWire": {
                "schema_version": "u32",
                "provider": "fcm|unified_push|ntfy|test",
                "provider_token": "opaque provider device token or endpoint; treated as sensitive",
                "app_instance_id": "string|null",
                "device_display_name": "string|null",
                "platform": "string|null",
                "app_version": "string|null",
                "hint_categories": "notifications|agents|helpers|update|session[]"
            },
            "PushSubscriptionRecordWire": {
                "schema_version": "u32",
                "id": "string",
                "device_id": "string",
                "provider": "fcm|unified_push|ntfy|test",
                "provider_token": "opaque provider device token or endpoint; returned only to the owning authenticated device for reconciliation",
                "app_instance_id": "string|null",
                "device_display_name": "string|null",
                "platform": "string|null",
                "app_version": "string|null",
                "hint_categories": "notifications|agents|helpers|update|session[]",
                "enabled_at": "rfc3339",
                "disabled_at": "rfc3339|null",
                "last_seen_at": "rfc3339|null"
            },
            "PushSubscriptionListResponseWire": {
                "schema_version": "u32",
                "subscriptions": "PushSubscriptionRecordWire[]; active subscriptions for the authenticated device"
            },
            "PushSubscriptionRegisterResponseWire": {
                "schema_version": "u32",
                "subscription": "PushSubscriptionRecordWire",
                "created": "bool; false when an existing provider/token/app_instance record was updated"
            },
            "PushSubscriptionDeleteResponseWire": {
                "schema_version": "u32",
                "subscription": "PushSubscriptionRecordWire",
                "revoked": "bool; false when the subscription was already disabled"
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

pub fn local_daemon_contract_snapshot() -> Value {
    json!({
        "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
        "contract": "sase_local_daemon_framed_json_v1",
        "transport": {
            "phase": "contract_only",
            "implemented": false,
            "future_transport": "unix_domain_socket_framed_json",
            "frame": {
                "encoding": "utf-8 json",
                "length_prefix": "u32 big-endian byte length",
                "max_payload_bytes": LOCAL_DAEMON_MAX_PAYLOAD_BYTES
            },
            "production_routing": "not implemented by Epic 1D; current source stores remain authoritative"
        },
        "versioning": {
            "contract_name": "sase_local_daemon_framed_json_v1",
            "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
            "min_compatible_client_schema_version": LOCAL_DAEMON_MIN_CLIENT_SCHEMA_VERSION,
            "max_compatible_client_schema_version": LOCAL_DAEMON_MAX_CLIENT_SCHEMA_VERSION,
            "additive_fields": "allowed when clients ignore unknown fields and servers preserve existing required fields",
            "nullable_fields": "nullable fields are explicit JSON null and may not be repurposed without a new schema version",
            "enum_compatibility": "clients must treat unknown enum variants as unsupported and fall back when possible; removing or renaming variants requires a new schema version",
            "deprecation_policy": "fields are deprecated for at least one compatible schema window before removal",
            "removal_policy": "removal, required-field semantic changes, or enum narrowing require a new contract snapshot and schema version bump",
            "error_shape": "LocalDaemonErrorWire inside LocalDaemonResponsePayloadWire::error",
            "snapshot_identifiers": "snapshot_id is an opaque stable view identifier scoped to one daemon state generation",
            "cursor_identifiers": "cursor is an opaque page token scoped to collection, filters, stable_handle, and snapshot_id",
            "stable_handles": "handles are opaque stable resource identifiers for follow-up reads within compatible schemas",
            "no_daemon_fallback": "LocalDaemonFallbackWire and daemon_unavailable/host_adapter_required errors signal direct source-store fallback"
        },
        "capabilities": [
            "health.read",
            "capabilities.read",
            "mocked.list",
            "mocked.events",
            "batch.request"
        ],
        "bounds": {
            "default_page_limit": LOCAL_DAEMON_DEFAULT_PAGE_LIMIT,
            "max_page_limit": LOCAL_DAEMON_MAX_PAGE_LIMIT,
            "max_payload_bytes": LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
            "batch_requests": "bounded by max_payload_bytes; future servers may reject large batches with payload_too_large"
        },
        "request_envelope": {
            "record": "LocalDaemonRequestEnvelopeWire",
            "schema_version": "u32",
            "request_id": "string; client-generated correlation id",
            "client": "LocalDaemonClientWire",
            "payload": "LocalDaemonRequestPayloadWire"
        },
        "response_envelope": {
            "record": "LocalDaemonResponseEnvelopeWire",
            "schema_version": "u32",
            "request_id": "string; echoes request_id",
            "snapshot_id": "string|null; present for stateful reads and event batches",
            "payload": "LocalDaemonResponsePayloadWire"
        },
        "requests": [
            {
                "type": "health",
                "request": {"include_capabilities": "bool"},
                "success": "LocalDaemonHealthResponseWire",
                "errors": ["LocalDaemonErrorWire"]
            },
            {
                "type": "capabilities",
                "request": "no fields",
                "success": "LocalDaemonCapabilitiesResponseWire",
                "errors": ["LocalDaemonErrorWire"]
            },
            {
                "type": "list",
                "request": "LocalDaemonListRequestWire",
                "success": "LocalDaemonListResponseWire",
                "errors": ["LocalDaemonErrorWire"],
                "notes": "mocked and future collection reads use pages, cursors, snapshot IDs, stable handles, filters, and payload bounds"
            },
            {
                "type": "events",
                "request": "LocalDaemonEventRequestWire",
                "success": "LocalDaemonEventBatchWire",
                "errors": ["LocalDaemonErrorWire"],
                "notes": "contract-only delta/heartbeat batch shape for future stream or polling transport"
            },
            {
                "type": "batch",
                "request": "LocalDaemonBatchRequestWire[]",
                "success": "LocalDaemonBatchResponseWire[]",
                "errors": ["LocalDaemonErrorWire"],
                "notes": "batching is shape-only in Epic 1D and must remain bounded by max_payload_bytes"
            }
        ],
        "records": {
            "LocalDaemonClientWire": {
                "schema_version": "u32",
                "name": "string",
                "version": "string"
            },
            "LocalDaemonRequestEnvelopeWire": {
                "schema_version": "u32",
                "request_id": "string",
                "client": "LocalDaemonClientWire",
                "payload": "LocalDaemonRequestPayloadWire"
            },
            "LocalDaemonResponseEnvelopeWire": {
                "schema_version": "u32",
                "request_id": "string",
                "snapshot_id": "string|null",
                "payload": "LocalDaemonResponsePayloadWire"
            },
            "LocalDaemonRequestPayloadWire": {
                "health": {"include_capabilities": "bool"},
                "capabilities": "no fields",
                "list": "LocalDaemonListRequestWire",
                "events": "LocalDaemonEventRequestWire",
                "batch": {"requests": "LocalDaemonBatchRequestWire[]"}
            },
            "LocalDaemonResponsePayloadWire": {
                "health": "LocalDaemonHealthResponseWire",
                "capabilities": "LocalDaemonCapabilitiesResponseWire",
                "list": "LocalDaemonListResponseWire",
                "events": "LocalDaemonEventBatchWire",
                "batch": {"responses": "LocalDaemonBatchResponseWire[]"},
                "error": "LocalDaemonErrorWire"
            },
            "LocalDaemonBatchRequestWire": {
                "request_id": "string",
                "payload": "LocalDaemonRequestPayloadWire"
            },
            "LocalDaemonBatchResponseWire": {
                "request_id": "string",
                "snapshot_id": "string|null",
                "payload": "LocalDaemonResponsePayloadWire"
            },
            "LocalDaemonHealthResponseWire": {
                "schema_version": "u32",
                "status": "ok|degraded|unavailable",
                "service": "sase_local_daemon",
                "daemon_started": "bool; false is expected in Epic 1D contract tests",
                "version": "string",
                "min_client_schema_version": "u32",
                "max_client_schema_version": "u32",
                "fallback": "LocalDaemonFallbackWire"
            },
            "LocalDaemonFallbackWire": {
                "available": "bool",
                "reason": "daemon_not_running|unsupported_client_version|recovery_mode|host_adapter_required|null",
                "message": "string|null"
            },
            "LocalDaemonCapabilitiesResponseWire": {
                "schema_version": "u32",
                "contract": "string",
                "contract_version": "u32",
                "min_client_schema_version": "u32",
                "max_client_schema_version": "u32",
                "capabilities": "string[]",
                "max_payload_bytes": "u32",
                "default_page_limit": "u32",
                "max_page_limit": "u32"
            },
            "LocalDaemonListRequestWire": {
                "collection": "agents|artifacts|beads|changespecs|notifications|workflows|xprompts|mocked",
                "page": "LocalDaemonPageRequestWire",
                "snapshot_id": "string|null",
                "stable_handle": "string|null",
                "max_payload_bytes": "u32|null",
                "filters": "json|null"
            },
            "LocalDaemonPageRequestWire": {
                "limit": "u32; default 100, max 500",
                "cursor": "string|null"
            },
            "LocalDaemonListResponseWire": {
                "schema_version": "u32",
                "collection": "LocalDaemonCollectionWire",
                "snapshot_id": "string",
                "items": "LocalDaemonListItemWire[]",
                "next_cursor": "string|null",
                "stable_handle": "string|null",
                "bounded": "LocalDaemonPayloadBoundWire"
            },
            "LocalDaemonListItemWire": {
                "handle": "string",
                "schema_version": "u32",
                "summary": "json"
            },
            "LocalDaemonPayloadBoundWire": {
                "max_payload_bytes": "u32",
                "truncated": "bool"
            },
            "LocalDaemonEventRequestWire": {
                "since_event_id": "string|null",
                "snapshot_id": "string|null",
                "max_events": "u32"
            },
            "LocalDaemonEventBatchWire": {
                "schema_version": "u32",
                "snapshot_id": "string",
                "events": "LocalDaemonEventRecordWire[]",
                "heartbeat": "LocalDaemonHeartbeatWire|null",
                "next_event_id": "string|null"
            },
            "LocalDaemonEventRecordWire": {
                "schema_version": "u32",
                "event_id": "string",
                "snapshot_id": "string",
                "created_at": "rfc3339",
                "source": "daemon|filesystem|host_adapter|mock",
                "payload": "LocalDaemonEventPayloadWire"
            },
            "LocalDaemonEventPayloadWire": {
                "heartbeat": {"sequence": "u64"},
                "delta": {
                    "collection": "LocalDaemonCollectionWire",
                    "handle": "string",
                    "operation": "upsert|delete|invalidate",
                    "fields": "json"
                },
                "resync_required": {"reason": "string"}
            },
            "LocalDaemonHeartbeatWire": {
                "schema_version": "u32",
                "sequence": "u64",
                "created_at": "rfc3339"
            },
            "LocalDaemonErrorWire": {
                "schema_version": "u32",
                "code": [
                    "invalid_request",
                    "unsupported_client_version",
                    "unsupported_capability",
                    "cursor_expired",
                    "snapshot_expired",
                    "payload_too_large",
                    "resource_not_found",
                    "host_adapter_required",
                    "daemon_unavailable",
                    "internal"
                ],
                "message": "string",
                "retryable": "bool",
                "target": "string|null",
                "details": "json|null",
                "fallback": "LocalDaemonFallbackWire"
            }
        },
        "examples": {
            "health_request": {
                "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                "request_id": "req_001",
                "client": {
                    "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    "name": "sase-cli",
                    "version": "0.1.1"
                },
                "payload": {
                    "type": "health",
                    "data": {
                        "include_capabilities": true
                    }
                }
            },
            "mocked_list_response": {
                "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                "request_id": "req_002",
                "snapshot_id": "snap_mock_001",
                "payload": {
                    "type": "list",
                    "data": {
                        "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                        "collection": "mocked",
                        "snapshot_id": "snap_mock_001",
                        "items": [
                            {
                                "handle": "mocked:item:1",
                                "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                                "summary": {"title": "contract fixture"}
                            }
                        ],
                        "next_cursor": null,
                        "stable_handle": "mocked:list:contract",
                        "bounded": {
                            "max_payload_bytes": LOCAL_DAEMON_MAX_PAYLOAD_BYTES,
                            "truncated": false
                        }
                    }
                }
            },
            "daemon_unavailable_error": {
                "type": "error",
                "data": {
                    "schema_version": LOCAL_DAEMON_WIRE_SCHEMA_VERSION,
                    "code": "daemon_unavailable",
                    "message": "local daemon transport is not implemented in this phase",
                    "retryable": false,
                    "target": null,
                    "details": null,
                    "fallback": {
                        "available": true,
                        "reason": "daemon_not_running",
                        "message": "use direct source-store readers"
                    }
                }
            }
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

pub fn write_local_daemon_contract_snapshot(
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
    let mut bytes =
        serde_json::to_vec_pretty(&local_daemon_contract_snapshot())?;
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

    #[test]
    fn committed_local_daemon_contract_snapshot_is_current() {
        let committed = fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("contracts/local_daemon/v1/local_daemon_v1.json"),
        )
        .unwrap();
        let mut expected =
            serde_json::to_string_pretty(&local_daemon_contract_snapshot())
                .unwrap();
        expected.push('\n');
        assert_eq!(committed, expected);
    }

    #[test]
    fn local_daemon_contract_is_separate_from_mobile_api_contract() {
        let mobile = api_v1_contract_snapshot();
        let local = local_daemon_contract_snapshot();

        assert_eq!(mobile["contract"], "sase_mobile_gateway_api_v1");
        assert_eq!(mobile["base_path"], "/api/v1");
        assert_eq!(local["contract"], "sase_local_daemon_framed_json_v1");
        assert_eq!(local["transport"]["implemented"], false);
        assert!(local.get("base_path").is_none());
    }
}

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::{json, Map, Value};
use thiserror::Error;

use crate::fleet_auth::{
    FLEET_SCOPE_ATTENTION_READ, FLEET_SCOPE_ATTENTION_RESOLVE,
    FLEET_SCOPE_BATCH_READ, FLEET_SCOPE_CATALOG_READ, FLEET_SCOPE_CONTENT_READ,
    FLEET_SCOPE_DETAIL_READ, FLEET_SCOPE_EVENTS_READ, FLEET_SCOPE_HELLO,
    FLEET_SCOPE_LAUNCH, FLEET_SCOPE_MUTATE, FLEET_SCOPE_PROJECTS_READ,
    FLEET_SCOPE_REVOKE, FLEET_SCOPE_ROTATE, FLEET_SCOPE_SUMMARY_READ,
};
use crate::wire::{
    FLEET_API_WIRE_SCHEMA_VERSION, FLEET_PROTOCOL_VERSION,
    GATEWAY_WIRE_SCHEMA_VERSION,
};
use sase_core::{
    FLEET_READ_DEFAULT_CONTENT_BYTES, FLEET_READ_DEFAULT_PAGE_ROWS,
    FLEET_READ_DEFAULT_REPLAY_EVENTS, FLEET_READ_MAX_BATCH_IDS,
    FLEET_READ_MAX_CONTENT_BYTES, FLEET_READ_MAX_FILTER_BYTES,
    FLEET_READ_MAX_PAGE_ROWS, FLEET_READ_MAX_PROJECT_IDS,
    FLEET_READ_MAX_QUERY_BYTES, FLEET_READ_MAX_REPLAY_EVENTS,
};

pub fn api_v1_contract_snapshot() -> Value {
    sort_object_keys(json!({
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
                "path": "/api/v1/patch-tags",
                "auth": true,
                "query": "MobilePatchTagListRequestWire fields as URL query parameters",
                "success": "MobilePatchTagListResponseWire",
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
                "path": "/api/v1/actions/gate/{prefix}",
                "auth": true,
                "request": "GateActionRequestWire without path-derived prefix",
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
                "action_kind": "plan_approval|epic_approval|hitl|user_question|launch_approval|custom_gate|non_action|unsupported",
                "prefix": "string",
                "notification_id": "string|null",
                "state": "available|already_handled|stale|missing_request|missing_target|unsupported",
                "response_file": "plan_response.json|hitl_response.json|question_response.json|launch_response.json|response.json",
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
                    "notification_id": "string|null",
                    "activity_cursor": "activity_at|notification_id|null"
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
                "push": "PushGatewayStatusWire",
                "fleet": "FleetHealthWire"
            },
            "FleetHealthWire": {
                "supported_protocol_versions": "u32[]; canonical non-secret fleet protocol versions supported by this gateway"
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
            "MobilePatchTagListRequestWire": {
                "schema_version": "u32",
                "project": "string|null",
                "limit": "u32|null",
                "device_id": "string|null; host-injected before bridge dispatch"
            },
            "MobilePatchTagListResponseWire": {
                "schema_version": "u32",
                "result": "MobileHelperResultWire",
                "context": "MobileHelperProjectContextWire",
                "tags": "MobilePatchTagEntryWire[]",
                "total_count": "u64"
            },
            "MobilePatchTagEntryWire": {
                "tag": "string",
                "project": "string|null",
                "patch": "string",
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
                "skill_name": "string|null; provider skill name for `/<skill_name>`, absent for non-skills; `name` stays the `#skill/<skill_name>` reference",
                "memory_type": "string|null; `core` or `reference` (legacy `short`/`long` still accepted) for an xprompt memory referenced as `#memory/<stem>`, absent otherwise; a non-null value means `kind` is `memory`",
                "content_preview": "string|null",
                "source_path_display": "string|null"
            },
            "MobileXpromptInputWire": {
                "name": "string",
                "type": "string",
                "description": "string|null; default null when absent",
                "required": "bool",
                "default_display": "string|null",
                "position": "u32",
                "repeatable": "bool; default false when absent",
                "choices": "MobileInputChoiceWire[]; default [] when absent"
            },
            "MobileInputChoiceWire": {
                "value": "string",
                "label": "string|null"
            },
            "MobileXpromptCatalogStatsWire": {
                "total_count": "u64",
                "project_count": "u64",
                "skill_count": "u64",
                "memory_count": "u64; default 0 when absent",
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
                "resurfaced_at": "rfc3339|null",
                "sender": "string",
                "icon": "string|null",
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
            "GateOptionWire": {
                "defined_by": "sase_core::notifications::mobile",
                "id": "string",
                "label": "string",
                "icon": "string|null",
                "feedback": "disabled|optional|required",
                "default_selected": "bool",
                "inputs": "MobileGateInputFieldWire[]; default [] when absent"
            },
            "GateSubmitWire": {
                "defined_by": "sase_core::notifications::mobile",
                "label": "string",
                "icon": "string|null"
            },
            "GateBranchWire": {
                "defined_by": "sase_core::notifications::mobile",
                "options": "GateOptionWire[]",
                "submit": "GateSubmitWire|null"
            },
            "MobileGateInputFieldWire": {
                "defined_by": "sase_core::notifications::mobile",
                "id": "string",
                "label": "string",
                "type": "word|line|text|path|agent|int|bool|float|enum",
                "required": "bool; default false when absent",
                "default": "json|null; the declared default value, verbatim",
                "choices": "MobileInputChoiceWire[]; default [] when absent",
                "placeholder": "string|null",
                "help": "string|null",
                "secret": "bool; default false when absent",
                "repeatable": "bool; default false when absent"
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
                "newer_than": "activity_at|notification_id|null; legacy rfc3339 timestamps accepted"
            },
            "MobileNotificationListResponseWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "notifications": "MobileNotificationCardWire[]",
                "total_count": "u64",
                "next_high_water": "activity_at|notification_id|null"
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
            "GateActionRequestWire": {
                "defined_by": "sase_core::notifications::mobile",
                "schema_version": "u32",
                "prefix": "string",
                "selected_option_ids": "string[]; non-empty subset of one branch",
                "feedback": "string|null",
                "option_inputs": "{option_id: json}|null; per-option submitted values, mutually exclusive with the shared value"
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
    }))
}

pub fn fleet_api_v1_contract_snapshot() -> Value {
    sort_object_keys(json!({
        "schema_version": FLEET_API_WIRE_SCHEMA_VERSION,
        "contract": "sase_fleet_gateway_api_v1",
        "base_path": "/api/fleet/v1",
        "response_shape": {
            "success": "direct_json_record",
            "error": "ApiErrorWire",
            "optional_fields": "explicit_null"
        },
        "auth": {
            "scheme": "bearer",
            "header": "Authorization",
            "unauthenticated_routes": [
                "POST /api/fleet/v1/enroll"
            ],
            "bootstrap": {
                "issuer": "local-only Rust FleetCredentialStore::issue_bootstrap API",
                "secret_storage": "sha256(domain || secret)",
                "single_use": true,
                "default_ttl_seconds": 600
            },
            "request_hot_path": {
                "durable_writes": false,
                "cache_refresh": "only when credentials.json metadata changes"
            }
        },
        "protocol_negotiation": {
            "supported_versions": [FLEET_PROTOCOL_VERSION],
            "selection": "highest mutually supported version",
            "hello_header": "X-SASE-Fleet-Protocol-Versions",
            "incompatible_error": "incompatible_protocol"
        },
        "limits": {
            "request_body_bytes": 16384,
            "enrollment_attempts": 8,
            "enrollment_window_seconds": 60,
            "catalog_default_page_rows": FLEET_READ_DEFAULT_PAGE_ROWS,
            "catalog_max_page_rows": FLEET_READ_MAX_PAGE_ROWS,
            "batch_max_logical_keys": FLEET_READ_MAX_BATCH_IDS,
            "project_eligibility_max_project_ids": FLEET_READ_MAX_PROJECT_IDS,
            "query_max_bytes": FLEET_READ_MAX_QUERY_BYTES,
            "filter_max_bytes": FLEET_READ_MAX_FILTER_BYTES,
            "content_default_bytes": FLEET_READ_DEFAULT_CONTENT_BYTES,
            "content_max_bytes": FLEET_READ_MAX_CONTENT_BYTES,
            "event_default_replay_events": FLEET_READ_DEFAULT_REPLAY_EVENTS,
            "event_max_replay_events": FLEET_READ_MAX_REPLAY_EVENTS
        },
        "routes": [
            {
                "method": "POST",
                "path": "/api/fleet/v1/enroll",
                "auth": false,
                "request": "FleetEnrollmentRequestWire",
                "success": "FleetEnrollmentResponseWire",
                "quarantine": "409 FleetEnrollmentResponseWire outcome=quarantined",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/fleet/v1/hello",
                "auth": true,
                "required_scope": FLEET_SCOPE_HELLO,
                "success": "FleetHelloResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/fleet/v1/summary",
                "auth": true,
                "required_scope": FLEET_SCOPE_SUMMARY_READ,
                "success": "FleetSummaryResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/catalog",
                "auth": true,
                "required_scope": FLEET_SCOPE_CATALOG_READ,
                "request": "FleetCatalogQueryWire",
                "success": "FleetCatalogPageWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/batch",
                "auth": true,
                "required_scope": FLEET_SCOPE_BATCH_READ,
                "request": "FleetLogicalBatchRequestWire",
                "success": "FleetLogicalBatchResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/detail",
                "auth": true,
                "required_scope": FLEET_SCOPE_DETAIL_READ,
                "request": "FleetDetailRequestWire",
                "success": "FleetDetailResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/content",
                "auth": true,
                "required_scope": FLEET_SCOPE_CONTENT_READ,
                "request": "FleetContentReadRequestWire",
                "success": "FleetContentReadResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/projects/eligibility",
                "auth": true,
                "required_scope": FLEET_SCOPE_PROJECTS_READ,
                "request": "FleetProjectEligibilityRequestWire",
                "success": "FleetProjectEligibilityResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "GET",
                "path": "/api/fleet/v1/events",
                "auth": true,
                "required_scope": FLEET_SCOPE_EVENTS_READ,
                "success": "FleetEventStreamItemWire stream",
                "protocol": "server_sent_events",
                "cursor_query": "store_generation and sequence query parameters",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/launch",
                "auth": true,
                "required_scope": FLEET_SCOPE_LAUNCH,
                "request": "FleetLaunchRequestWire",
                "success": "FleetLaunchResponseWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/mutate",
                "auth": true,
                "required_scope": FLEET_SCOPE_MUTATE,
                "request": "FleetMutationRequestWire",
                "success": "FleetMutationResponseWire",
                "events_on_success": ["agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/attention",
                "auth": true,
                "required_scope": FLEET_SCOPE_ATTENTION_READ,
                "request": "FleetLogicalBatchRequestWire",
                "success": "FleetAttentionSnapshotWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/attention/resolve",
                "auth": true,
                "required_scope": FLEET_SCOPE_ATTENTION_RESOLVE,
                "request": "FleetAttentionRequestWire",
                "success": "FleetAttentionResponseWire",
                "already_settled": "200 FleetAttentionResponseWire receipt.outcome=already_settled",
                "events_on_success": ["notifications_changed", "agents_changed"],
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/credential/rotate",
                "auth": true,
                "required_scope": FLEET_SCOPE_ROTATE,
                "request": "FleetTokenRotateRequestWire",
                "success": "FleetTokenRotateResponseWire",
                "errors": ["ApiErrorWire"]
            },
            {
                "method": "POST",
                "path": "/api/fleet/v1/credential/revoke",
                "auth": true,
                "required_scope": FLEET_SCOPE_REVOKE,
                "request": "FleetCredentialRevokeRequestWire",
                "success": "FleetCredentialRevokeResponseWire",
                "errors": ["ApiErrorWire"]
            }
        ],
        "records": {
            "ApiErrorWire": {
                "schema_version": "u32",
                "code": [
                    "unauthorized",
                    "invalid_request",
                    "bootstrap_consumed",
                    "bootstrap_expired",
                    "bootstrap_rejected",
                    "credential_expired",
                    "credential_revoked",
                    "incompatible_protocol",
                    "installation_pin_mismatch",
                    "payload_too_large",
                    "rate_limited",
                    "timeout",
                    "resync_required",
                    "scope_denied",
                    "internal"
                ],
                "message": "string",
                "target": "string|null",
                "details": "json|null"
            },
            "CapabilitySetWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "resource": "string[]",
                "host": "string[]; fleet gateway scopes",
                "protocol": "string[]"
            },
            "FleetBootstrapIssueRequestWire": {
                "local_only": true,
                "schema_version": "u32",
                "requested_scopes": "string[]; empty means default fleet scopes",
                "supported_protocol_versions": "u32[]; empty means the current fleet protocol version",
                "expires_at_unix": "f64|null; default now + 600s",
                "installation_pin": "string|null; optional current-installation precondition"
            },
            "FleetBootstrapIssueResponseWire": {
                "local_only": true,
                "schema_version": "u32",
                "bootstrap_id": "string",
                "bootstrap_secret": "high-entropy string; returned once",
                "expires_at_unix": "f64",
                "allowed_scopes": "string[]",
                "pinned_installation_id": "string",
                "protocol_versions": "u32[]"
            },
            "FleetControllerMetadataWire": {
                "schema_version": "u32",
                "controller_id": "string|null; generated when absent",
                "display_name": "string|null",
                "platform": "string|null",
                "app_version": "string|null"
            },
            "FleetCredentialRecordWire": {
                "schema_version": "u32",
                "credential_id": "string",
                "controller_id": "string|null",
                "controller": "FleetControllerMetadataWire",
                "scopes": "string[]",
                "issued_at_unix": "f64",
                "expires_at_unix": "f64|null",
                "rotated_at_unix": "f64|null",
                "revoked_at_unix": "f64|null",
                "revoked_reason": "string|null"
            },
            "StoreCursorWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "store_generation": "string",
                "sequence": "u64"
            },
            "FleetSnapshotFreshnessWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "freshness": "fresh|aging|stale|unknown",
                "partial": "bool",
                "refreshed_at_unix": "f64|null",
                "error": "string|null; safe diagnostic code only"
            },
            "FleetAuthoritativeSnapshotWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "catalog_scope": "presentation|history; defaults to presentation",
                "catalog_snapshot_id": "string; catsnap_v1_ + 64 lowercase hex",
                "counts": "FleetLogicalAgentCountsWire",
                "count_revision": "u64|null",
                "summaries": "ResolvedAgentSummaryWire[]",
                "freshness": "FleetSnapshotFreshnessWire"
            },
            "FleetSummaryResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "catalog_scope": "presentation|history; defaults to presentation",
                "catalog_snapshot_id": "string; catsnap_v1_ + 64 lowercase hex",
                "counts": "FleetLogicalAgentCountsWire",
                "count_revision": "u64|null",
                "freshness": "FleetSnapshotFreshnessWire"
            },
            "FleetCatalogScopeWire": {
                "defined_by": "sase_core::fleet_contract",
                "values": ["presentation", "history"]
            },
            "FleetCatalogContinuationStateWire": {
                "defined_by": "sase_core::fleet_contract",
                "values": ["ready", "finished", "resync_required"]
            },
            "FleetCatalogResetReasonWire": {
                "defined_by": "sase_core::fleet_contract",
                "values": ["scope_mismatch", "snapshot_mismatch", "restart_required"]
            },
            "FleetCatalogQueryWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "scope": "FleetCatalogScopeWire; defaults to presentation",
                "snapshot_id": "string|null; optional previous snapshot evidence",
                "cursor": "string|null; catcur_v1 scope/snapshot/offset cursor",
                "limit": "u32|null; default 50, max 100",
                "project_ids": "string[]; bounded",
                "query": "string|null; bounded, path/token rejected",
                "status_buckets": "stopped|failed|starting|running|queued|waiting|done[]; sorted and deduplicated",
                "include_terminal": "bool"
            },
            "FleetCatalogPageSelectionWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "scope": "FleetCatalogScopeWire",
                "snapshot_id": "string; catsnap_v1_ + 64 lowercase hex",
                "rows": "ResolvedAgentSummaryWire[]",
                "limit": "u32",
                "total_matching_rows": "u64",
                "next_cursor": "string|null; catcur_v1 scope/snapshot/offset cursor",
                "has_more": "bool",
                "state": "FleetCatalogContinuationStateWire",
                "reset_reason": "FleetCatalogResetReasonWire|null; present only for resync_required"
            },
            "FleetCatalogPageWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "counts": "FleetLogicalAgentCountsWire",
                "count_revision": "u64|null",
                "freshness": "FleetSnapshotFreshnessWire",
                "page": "FleetCatalogPageSelectionWire"
            },
            "FleetLogicalBatchRequestWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "logical_keys": "string[]; max 200"
            },
            "FleetLogicalBatchEntryWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "requested_logical_key": "string",
                "summary": "ResolvedAgentSummaryWire|null"
            },
            "FleetLogicalBatchResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "counts": "FleetLogicalAgentCountsWire",
                "count_revision": "u64|null",
                "freshness": "FleetSnapshotFreshnessWire",
                "entries": "FleetLogicalBatchEntryWire[]"
            },
            "FleetDetailRequestWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "logical_key": "string"
            },
            "FleetDetailResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "freshness": "FleetSnapshotFreshnessWire",
                "detail": "ResolvedAgentDetailWire"
            },
            "FleetContentReadRequestWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "handle_id": "string; opaque content handle",
                "row_revision": "ResourceRevisionWire",
                "offset": "u64",
                "limit": "u64|null; default 65536, max 262144"
            },
            "FleetContentReadResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "handle": "ContentHandleWire",
                "offset": "u64",
                "returned_bytes": "u64",
                "total_byte_len": "u64",
                "next_offset": "u64|null",
                "eof": "bool",
                "supports_growth": "bool",
                "sha256": "string",
                "data_base64": "string"
            },
            "FleetProjectEligibilityRequestWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "project_ids": "string[]; empty means all bounded projects",
                "limit": "u32|null; max 200"
            },
            "FleetProjectEligibilityWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "project_id": "string",
                "display_name": "string|null",
                "state": "string",
                "eligible": "bool",
                "launchable": "bool",
                "active_claim_count": "u32",
                "reason": "string|null"
            },
            "FleetProjectEligibilityResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "projects": "FleetProjectEligibilityWire[]",
                "limit": "u32",
                "total_matching_projects": "u64",
                "truncated": "bool"
            },
            "FleetInvalidationEventWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "cursor": "StoreCursorWire",
                "kind": "launched|lifecycle_changed|attention_changed|revision_changed|deleted|process_exited|snapshot_replaced",
                "logical_key": "string|null",
                "row_revision": "ResourceRevisionWire|null",
                "reason": "string"
            },
            "FleetResyncRequiredWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "reason": "initial_cursor|generation_mismatch|sequence_ahead_of_authority|replay_gap|incomplete_deletion_history|receiver_lag|ring_rolled_over|generation_replaced",
                "snapshot": "FleetAuthoritativeSnapshotWire"
            },
            "FleetEventStreamItemWire": {
                "defined_by": "sase_core::fleet_contract",
                "shape": "serde tagged union {type,data}",
                "variants": [
                    "invalidation: FleetInvalidationEventWire; SSE id is store_generation:sequence",
                    "resync_required: FleetResyncRequiredWire; SSE id is snapshot cursor",
                    "heartbeat: { cursor: StoreCursorWire }; no SSE id"
                ]
            },
            "FleetLaunchProjectContextWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "provider_ref": "string|null",
                "project_id": "string; portable project identity, never a path",
                "revision": "string|null; published revision evidence",
                "patch_ref": "string|null; Patch evidence"
            },
            "FleetLaunchIntentWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "prompt": "string; stripped source %dispatch directive",
                "request_id": "string|null",
                "display_name": "string|null",
                "name": "string|null",
                "model": "string|null",
                "provider": "string|null",
                "runtime": "string|null",
                "project": "FleetLaunchProjectContextWire",
                "dry_run": "bool|null",
                "follow": "bool",
                "references": "FleetLaunchReferenceWire[]"
            },
            "FleetLaunchRequestWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "target_installation_id": "string; pinned target installation",
                "intent": "FleetLaunchIntentWire",
                "payload_fingerprint": "PayloadFingerprintWire; canonical intent digest",
                "acceptance_window_seconds": "f64"
            },
            "FleetLaunchReceiptWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "payload_fingerprint": "PayloadFingerprintWire",
                "target_installation_id": "string",
                "accepted_at_unix_ms": "u64",
                "expires_at_unix_ms": "u64",
                "state": "accepted|pending|settled",
                "logical_locator": "LogicalAgentLocatorWire|null",
                "instance_locator": "AgentInstanceLocatorWire|null",
                "message": "string|null; path/token-redacted"
            },
            "FleetLaunchResponseWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "decision": "accept_new|return_original_receipt|conflict|expired|precondition_mismatch",
                "reason": "unseen_in_window|same_scoped_key_and_payload|same_scoped_key_different_payload|expired_or_tombstoned_key|target_or_revision_mismatch",
                "receipt": "FleetLaunchReceiptWire"
            },
            "FleetMutationIntentWire": {
                "defined_by": "sase_core::fleet_mutation",
                "schema_version": "u32",
                "kind": "stop|retry|fork",
                "target": "AgentInstanceLocatorWire",
                "row_revision": "ResourceRevisionWire",
                "reason": "string|null; bounded, path/secret-redacted",
                "fork_prompt": "string|null; required for fork",
                "kill_source_first": "bool|null; retry only",
                "follow": "bool"
            },
            "FleetMutationRequestWire": {
                "defined_by": "sase_core::fleet_mutation",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "target_installation_id": "string; pinned target installation",
                "intent": "FleetMutationIntentWire",
                "payload_fingerprint": "PayloadFingerprintWire; canonical intent digest",
                "acceptance_window_seconds": "f64"
            },
            "FleetMutationReceiptWire": {
                "defined_by": "sase_core::fleet_mutation",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "payload_fingerprint": "PayloadFingerprintWire",
                "target_installation_id": "string",
                "target": "AgentInstanceLocatorWire",
                "resource_revision": "ResourceRevisionWire",
                "accepted_at_unix_ms": "u64",
                "expires_at_unix_ms": "u64",
                "state": "accepted|pending|settled",
                "outcome": "applied|already_settled|precondition_failed|capability_missing|already_terminal|unknown_row|instance_mismatch|stale_revision|null",
                "logical_locator": "LogicalAgentLocatorWire|null",
                "instance_locator": "AgentInstanceLocatorWire|null",
                "message": "string|null; path/token-redacted"
            },
            "FleetMutationResponseWire": {
                "defined_by": "sase_core::fleet_mutation",
                "schema_version": "u32",
                "decision": "accept_new|return_original_receipt|conflict|expired|precondition_mismatch",
                "reason": "unseen_in_window|same_scoped_key_and_payload|same_scoped_key_different_payload|expired_or_tombstoned_key|target_or_revision_mismatch",
                "receipt": "FleetMutationReceiptWire"
            },
            "FleetAttentionRequestKeyWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "origin_installation_id": "string",
                "request_id": "string; owner's opaque notification identity",
                "pending_action_prefix": "string; the same prefix `sase gate answer` consumes"
            },
            "FleetAttentionOptionWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "id": "string; opaque",
                "label": "string"
            },
            "FleetAttentionEntryWire": {
                "defined_by": "sase_core::fleet_attention",
                "path_privacy": "never includes a bundle path, response directory, request path, preview path, PID, or credential",
                "schema_version": "u32",
                "kind": "question|gate",
                "state": "pending|settled|expired|unknown",
                "request_key": "FleetAttentionRequestKeyWire",
                "revision": "u64; content fingerprint, stable across reconnects",
                "logical_key": "string|null",
                "logical_locator": "LogicalAgentLocatorWire|null",
                "title": "string",
                "summary": "string; bounded",
                "options": "FleetAttentionOptionWire[]",
                "feedback_required": "bool",
                "question_form": "object|null",
                "preview": "ContentHandleWire|null",
                "settled_by_host_label": "string|null",
                "settled_response": "object|null"
            },
            "FleetAttentionSnapshotWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "entries": "FleetAttentionEntryWire[]",
                "observed_at_unix": "f64"
            },
            "FleetAttentionIntentWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "kind": "question|gate",
                "request_key": "FleetAttentionRequestKeyWire",
                "observed_revision": "u64",
                "selected_option_ids": "string[]; gate only",
                "feedback": "string|null; gate only",
                "question_choice": "answer|custom|null; question only",
                "question_index": "u32|null",
                "selected_option_id": "string|null",
                "selected_option_label": "string|null",
                "selected_option_index": "u32|null",
                "custom_answer": "string|null",
                "global_note": "string|null"
            },
            "FleetAttentionRequestWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "target_installation_id": "string; pinned target installation",
                "intent": "FleetAttentionIntentWire",
                "payload_fingerprint": "PayloadFingerprintWire; canonical intent digest",
                "acceptance_window_seconds": "f64"
            },
            "FleetAttentionReceiptWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "key": "ScopedOperationKeyWire",
                "payload_fingerprint": "PayloadFingerprintWire",
                "target_installation_id": "string",
                "request_key": "FleetAttentionRequestKeyWire",
                "observed_revision": "u64",
                "accepted_at_unix_ms": "u64",
                "expires_at_unix_ms": "u64",
                "state": "accepted|pending|settled",
                "outcome": "applied|already_settled|stale_revision|unknown_request|capability_missing|precondition_failed|null",
                "settled_by_host_label": "string|null",
                "settled_response": "object|null",
                "message": "string|null; path/token-redacted"
            },
            "FleetAttentionResponseWire": {
                "defined_by": "sase_core::fleet_attention",
                "schema_version": "u32",
                "decision": "accept_new|return_original_receipt|conflict|expired|precondition_mismatch",
                "reason": "unseen_in_window|same_scoped_key_and_payload|same_scoped_key_different_payload|expired_or_tombstoned_key|target_or_revision_mismatch",
                "receipt": "FleetAttentionReceiptWire"
            },
            "ScopedOperationKeyWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "controller_id": "string; authenticated controller scope",
                "operation_id": "string; idempotency key within controller scope"
            },
            "PayloadFingerprintWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "sha256": "lowercase canonical SHA-256 digest"
            },
            "FleetLogicalAgentCountsWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "basis": "FleetCountBasisWire",
                "logical_agent_total": "u64",
                "running": "u64",
                "waiting": "u64",
                "attention": "u64",
                "occupied_runner_slots": "u64"
            },
            "ResourceRevisionWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "logical_key": "string",
                "revision": "u64"
            },
            "ContentHandleWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "id": "string; opaque",
                "kind": "transcript|output|diff|log|artifact|question",
                "revision": "ResourceRevisionWire|null",
                "digest": "string|null",
                "byte_len": "u64|null",
                "supports_range": "bool",
                "supports_growth": "bool"
            },
            "ResolvedAgentSummaryWire": {
                "defined_by": "sase_core::fleet_contract",
                "path_privacy": "never includes local paths, PIDs, process groups, bearer tokens, or auth headers",
                "identity": "logical and optional exact locators plus logical/exact keys",
                "state": "lifecycle, liveness, connection health, freshness, status bucket, labels, capabilities, content metadata",
                "family": "normalized family_role (root/member/monitor/gate/proc/historical_shell) plus optional parent_timestamp lineage, for viewer folding"
            },
            "ResolvedAgentDetailWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "summary": "ResolvedAgentSummaryWire",
                "content_handles": "ContentHandleWire[]"
            },
            "FleetEnrollmentRequestWire": {
                "schema_version": "u32",
                "bootstrap_id": "string",
                "bootstrap_secret": "string",
                "controller": "FleetControllerMetadataWire",
                "requested_scopes": "string[]; empty means bootstrap-allowed defaults",
                "supported_protocol_versions": "u32[]; empty means the current fleet protocol version",
                "pinned_installation_id": "string"
            },
            "FleetEnrollmentResponseWire": {
                "schema_version": "u32",
                "outcome": "enrolled|quarantined",
                "protocol_version": "u32|null",
                "installation": "InstallationIdentityRecordWire",
                "machine_selector": "string",
                "capabilities": "CapabilitySetWire",
                "credential": "FleetCredentialRecordWire|null",
                "token_type": "bearer|null",
                "token": "string|null; returned only on enrollment",
                "quarantine": "FleetQuarantineWire|null"
            },
            "FleetHelloResponseWire": {
                "schema_version": "u32",
                "protocol_version": "u32",
                "installation": "InstallationIdentityRecordWire",
                "machine_selector": "string",
                "capabilities": "CapabilitySetWire",
                "credential": "FleetCredentialRecordWire",
                "cursor": "StoreCursorWire",
                "counts": "FleetLogicalAgentCountsWire",
                "count_revision": "u64|null",
                "freshness": "FleetSnapshotFreshnessWire"
            },
            "FleetQuarantineWire": {
                "schema_version": "u32",
                "reason": "installation_pin_mismatch",
                "presented_installation_id": "string",
                "authoritative_installation_id": "string"
            },
            "FleetTokenRotateRequestWire": {
                "schema_version": "u32",
                "supported_protocol_versions": "u32[]; empty means the current fleet protocol version"
            },
            "FleetTokenRotateResponseWire": {
                "schema_version": "u32",
                "protocol_version": "u32",
                "credential": "FleetCredentialRecordWire",
                "token_type": "bearer",
                "token": "string; returned once"
            },
            "FleetCredentialRevokeRequestWire": {
                "schema_version": "u32",
                "reason": "string|null"
            },
            "FleetCredentialRevokeResponseWire": {
                "schema_version": "u32",
                "credential": "FleetCredentialRecordWire",
                "revoked": "bool"
            },
            "InstallationIdentityRecordWire": {
                "defined_by": "sase_core::fleet_contract",
                "schema_version": "u32",
                "installation_id": "string",
                "created_at_unix": "f64",
                "generation": "u64",
                "prior_installation_id": "string|null",
                "rotated_at_unix": "f64|null",
                "adopted_at_unix": "f64|null",
                "reason": "string|null"
            }
        },
        "storage": {
            "root": "<sase_home>/fleet_gateway",
            "credentials_file": "credentials.json",
            "lock_file": "credentials.lock",
            "file_mode": "0600",
            "directory_mode": "0700",
            "stored_secret_material": "hashes only",
            "read_model_source": "<sase_home>/agent_artifact_index.sqlite plus project records",
            "wire_privacy": "read responses never expose local filesystem paths, PIDs, process groups, bearer tokens, or auth headers",
            "content_access": "opaque handles resolve server-side only and return bounded base64 ranges",
            "credential_default_ttl_seconds": 7776000
        }
    }))
}

fn sort_object_keys(value: Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.into_iter().map(sort_object_keys).collect())
        }
        Value::Object(items) => {
            let mut entries: Vec<_> = items.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, sort_object_keys(value)))
                    .collect::<Map<_, _>>(),
            )
        }
        scalar => scalar,
    }
}

pub fn write_api_v1_contract_snapshot(
    path: impl AsRef<Path>,
) -> Result<(), ContractSnapshotError> {
    write_contract_snapshot(path, &api_v1_contract_snapshot())
}

pub fn write_fleet_api_v1_contract_snapshot(
    path: impl AsRef<Path>,
) -> Result<(), ContractSnapshotError> {
    write_contract_snapshot(path, &fleet_api_v1_contract_snapshot())
}

fn write_contract_snapshot(
    path: impl AsRef<Path>,
    snapshot: &Value,
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
    let mut bytes = serde_json::to_vec_pretty(snapshot)?;
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
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("contracts/api_v1/mobile_api_v1.json");
        if std::env::var("UPDATE_MOBILE_CONTRACT").ok().as_deref() == Some("1")
        {
            write_api_v1_contract_snapshot(&path).unwrap();
        }
        let committed = fs::read_to_string(&path).unwrap();
        let mut expected =
            serde_json::to_string_pretty(&api_v1_contract_snapshot()).unwrap();
        expected.push('\n');
        assert_eq!(committed, expected);
    }

    #[test]
    fn committed_fleet_contract_snapshot_is_current() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("contracts/api_fleet_v1/fleet_api_v1.json");
        if std::env::var("UPDATE_FLEET_CONTRACT").ok().as_deref() == Some("1") {
            write_fleet_api_v1_contract_snapshot(&path).unwrap();
        }
        let committed = fs::read_to_string(&path).unwrap();
        let mut expected =
            serde_json::to_string_pretty(&fleet_api_v1_contract_snapshot())
                .unwrap();
        expected.push('\n');
        assert_eq!(committed, expected);
    }
}

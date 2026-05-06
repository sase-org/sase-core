use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

pub const GATEWAY_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBuildWire {
    pub package_version: String,
    pub git_sha: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayBindWire {
    pub address: String,
    pub is_loopback: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponseWire {
    pub schema_version: u32,
    pub status: String,
    pub service: String,
    pub version: String,
    pub build: GatewayBuildWire,
    pub bind: GatewayBindWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecordWire {
    pub schema_version: u32,
    pub device_id: String,
    pub display_name: String,
    pub platform: String,
    pub app_version: Option<String>,
    pub paired_at: Option<String>,
    pub last_seen_at: Option<String>,
    pub revoked_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionResponseWire {
    pub schema_version: u32,
    pub device: DeviceRecordWire,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairStartRequestWire {
    pub schema_version: u32,
    pub host_label: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairStartResponseWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    pub expires_at: String,
    pub host_label: String,
    pub host_fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairingDeviceMetadataWire {
    pub display_name: String,
    pub platform: String,
    pub app_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairFinishRequestWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    pub device: PairingDeviceMetadataWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PairFinishResponseWire {
    pub schema_version: u32,
    pub device: DeviceRecordWire,
    pub token_type: String,
    pub token: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiErrorCodeWire {
    Unauthorized,
    NotFound,
    InvalidRequest,
    PairingExpired,
    PairingRejected,
    ConflictAlreadyHandled,
    GoneStale,
    AmbiguousPrefix,
    UnsupportedAction,
    AttachmentExpired,
    AgentNotFound,
    AgentNotRunning,
    LaunchFailed,
    InvalidUpload,
    BridgeUnavailable,
    PermissionDenied,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApiErrorWire {
    pub schema_version: u32,
    pub code: ApiErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
    pub details: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type", content = "data")]
pub enum EventPayloadWire {
    Heartbeat {
        sequence: u64,
    },
    Session {
        device_id: String,
    },
    ResyncRequired {
        reason: String,
    },
    NotificationsChanged {
        reason: String,
        notification_id: Option<String>,
    },
    AgentsChanged {
        reason: String,
        agent_name: Option<String>,
        timestamp: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecordWire {
    pub schema_version: u32,
    pub id: String,
    pub created_at: String,
    pub payload: EventPayloadWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationStateMutationResponseWire {
    pub schema_version: u32,
    pub notification_id: String,
    pub read: bool,
    pub dismissed: bool,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentListRequestWire {
    pub schema_version: u32,
    pub include_recent: bool,
    pub status: Option<String>,
    pub project: Option<String>,
    pub device_id: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentListResponseWire {
    pub schema_version: u32,
    pub agents: Vec<MobileAgentSummaryWire>,
    pub total_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentSummaryWire {
    pub name: String,
    pub project: Option<String>,
    pub status: String,
    pub pid: Option<u32>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub workspace_number: Option<u32>,
    pub started_at: Option<String>,
    pub duration_seconds: Option<u64>,
    pub prompt_snippet: Option<String>,
    pub has_artifact_dir: bool,
    pub retry_lineage: MobileAgentRetryLineageWire,
    pub actions: MobileAgentActionAffordancesWire,
    pub display: MobileAgentDisplayLabelsWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryLineageWire {
    pub retry_of_timestamp: Option<String>,
    pub retried_as_timestamp: Option<String>,
    pub retry_chain_root_timestamp: Option<String>,
    pub retry_attempt: Option<u32>,
    pub parent_agent_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentActionAffordancesWire {
    pub can_resume: bool,
    pub can_wait: bool,
    pub can_kill: bool,
    pub can_retry: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentDisplayLabelsWire {
    pub title: String,
    pub subtitle: Option<String>,
    pub status_label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentResumeOptionsResponseWire {
    pub schema_version: u32,
    pub options: Vec<MobileAgentResumeOptionWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentResumeOptionWire {
    pub id: String,
    pub agent_name: String,
    pub kind: MobileAgentResumeOptionKindWire,
    pub label: String,
    pub prompt_text: String,
    pub direct_launch_supported: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileAgentResumeOptionKindWire {
    Resume,
    Wait,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentTextLaunchRequestWire {
    pub schema_version: u32,
    pub prompt: String,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: Option<String>,
    pub device_id: Option<String>,
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentImageLaunchRequestWire {
    pub schema_version: u32,
    pub prompt: String,
    pub original_filename: String,
    pub content_type: String,
    pub byte_length: u64,
    pub base64_image: String,
    pub device_id: Option<String>,
    pub display_name: Option<String>,
    pub name: Option<String>,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub runtime: Option<String>,
    pub project: Option<String>,
    pub dry_run: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentLaunchResultWire {
    pub schema_version: u32,
    pub primary: Option<MobileAgentLaunchSlotResultWire>,
    pub slots: Vec<MobileAgentLaunchSlotResultWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentLaunchSlotResultWire {
    pub slot_id: String,
    pub name: Option<String>,
    pub status: MobileAgentLaunchSlotStatusWire,
    pub artifact_dir: Option<String>,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileAgentLaunchSlotStatusWire {
    Launched,
    DryRun,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentKillRequestWire {
    pub schema_version: u32,
    pub reason: Option<String>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentKillResultWire {
    pub schema_version: u32,
    pub name: String,
    pub status: String,
    pub pid: Option<u32>,
    pub changed: bool,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryRequestWire {
    pub schema_version: u32,
    pub prompt_override: Option<String>,
    pub dry_run: Option<bool>,
    pub kill_source_first: Option<bool>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAgentRetryResultWire {
    pub schema_version: u32,
    pub source_agent: String,
    pub launch: MobileAgentLaunchResultWire,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn sample_device() -> DeviceRecordWire {
        DeviceRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device_id: "device_123".to_string(),
            display_name: "Pixel 9".to_string(),
            platform: "android".to_string(),
            app_version: Some("0.1.0".to_string()),
            paired_at: Some("2026-05-06T14:30:00Z".to_string()),
            last_seen_at: None,
            revoked_at: None,
        }
    }

    #[test]
    fn health_wire_json_snapshot() {
        let wire = HealthResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            status: "ok".to_string(),
            service: "sase_gateway".to_string(),
            version: "0.1.1".to_string(),
            build: GatewayBuildWire {
                package_version: "0.1.1".to_string(),
                git_sha: None,
            },
            bind: GatewayBindWire {
                address: "127.0.0.1:7629".to_string(),
                is_loopback: true,
            },
        };

        assert_eq!(
            serde_json::to_value(wire).unwrap(),
            json!({
                "schema_version": 1,
                "status": "ok",
                "service": "sase_gateway",
                "version": "0.1.1",
                "build": {
                    "package_version": "0.1.1",
                    "git_sha": null
                },
                "bind": {
                    "address": "127.0.0.1:7629",
                    "is_loopback": true
                }
            })
        );
    }

    #[test]
    fn pairing_wire_json_snapshot() {
        let start = PairStartResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            pairing_id: "pair_123".to_string(),
            code: "123456".to_string(),
            expires_at: "2026-05-06T14:35:00Z".to_string(),
            host_label: "workstation".to_string(),
            host_fingerprint: None,
        };
        assert_eq!(
            serde_json::to_value(start).unwrap(),
            json!({
                "schema_version": 1,
                "pairing_id": "pair_123",
                "code": "123456",
                "expires_at": "2026-05-06T14:35:00Z",
                "host_label": "workstation",
                "host_fingerprint": null
            })
        );

        let finish = PairFinishResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device: sample_device(),
            token_type: "bearer".to_string(),
            token: "sase_device_token".to_string(),
        };
        assert_eq!(
            serde_json::to_value(finish).unwrap(),
            json!({
                "schema_version": 1,
                "device": {
                    "schema_version": 1,
                    "device_id": "device_123",
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0",
                    "paired_at": "2026-05-06T14:30:00Z",
                    "last_seen_at": null,
                    "revoked_at": null
                },
                "token_type": "bearer",
                "token": "sase_device_token"
            })
        );
    }

    #[test]
    fn session_wire_json_snapshot() {
        let session = SessionResponseWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device: sample_device(),
            capabilities: vec![
                "session.read".to_string(),
                "events.read".to_string(),
            ],
        };
        assert_eq!(
            serde_json::to_value(session).unwrap(),
            json!({
                "schema_version": 1,
                "device": {
                    "schema_version": 1,
                    "device_id": "device_123",
                    "display_name": "Pixel 9",
                    "platform": "android",
                    "app_version": "0.1.0",
                    "paired_at": "2026-05-06T14:30:00Z",
                    "last_seen_at": null,
                    "revoked_at": null
                },
                "capabilities": ["session.read", "events.read"]
            })
        );
    }

    #[test]
    fn error_wire_json_snapshot() {
        let errors = vec![
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::Unauthorized,
                message: "authentication is required for this endpoint"
                    .to_string(),
                target: Some("authorization".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::ConflictAlreadyHandled,
                message: "action was already handled".to_string(),
                target: Some("abcdef12".to_string()),
                details: Some(json!({"notification_id": "abcdef1234567890"})),
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::GoneStale,
                message: "action request is stale".to_string(),
                target: Some("abcdef12".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AmbiguousPrefix,
                message: "notification prefix matches multiple actions"
                    .to_string(),
                target: Some("abcd".to_string()),
                details: Some(json!({"matches": 2})),
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::UnsupportedAction,
                message: "notification action is not supported by mobile"
                    .to_string(),
                target: Some("JumpToChangeSpec".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::AttachmentExpired,
                message: "attachment token is expired".to_string(),
                target: Some("token".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::BridgeUnavailable,
                message: "agent bridge is unavailable".to_string(),
                target: Some("agent_bridge".to_string()),
                details: None,
            },
            ApiErrorWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                code: ApiErrorCodeWire::PermissionDenied,
                message: "permission denied".to_string(),
                target: Some("agent_bridge:kill-agent".to_string()),
                details: None,
            },
        ];
        assert_eq!(
            serde_json::to_value(errors).unwrap(),
            json!([
                {
                    "schema_version": 1,
                    "code": "unauthorized",
                    "message": "authentication is required for this endpoint",
                    "target": "authorization",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "conflict_already_handled",
                    "message": "action was already handled",
                    "target": "abcdef12",
                    "details": {"notification_id": "abcdef1234567890"}
                },
                {
                    "schema_version": 1,
                    "code": "gone_stale",
                    "message": "action request is stale",
                    "target": "abcdef12",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "ambiguous_prefix",
                    "message": "notification prefix matches multiple actions",
                    "target": "abcd",
                    "details": {"matches": 2}
                },
                {
                    "schema_version": 1,
                    "code": "unsupported_action",
                    "message": "notification action is not supported by mobile",
                    "target": "JumpToChangeSpec",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "attachment_expired",
                    "message": "attachment token is expired",
                    "target": "token",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "bridge_unavailable",
                    "message": "agent bridge is unavailable",
                    "target": "agent_bridge",
                    "details": null
                },
                {
                    "schema_version": 1,
                    "code": "permission_denied",
                    "message": "permission denied",
                    "target": "agent_bridge:kill-agent",
                    "details": null
                }
            ])
        );
    }

    #[test]
    fn event_wire_json_snapshot() {
        let event = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000001".to_string(),
            created_at: "2026-05-06T14:30:00Z".to_string(),
            payload: EventPayloadWire::Heartbeat { sequence: 1 },
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000001",
                "created_at": "2026-05-06T14:30:00Z",
                "payload": {
                    "type": "heartbeat",
                    "data": {
                        "sequence": 1
                    }
                }
            })
        );

        let changed = EventRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: "0000000000000002".to_string(),
            created_at: "2026-05-06T14:31:00Z".to_string(),
            payload: EventPayloadWire::AgentsChanged {
                reason: "launch".to_string(),
                agent_name: Some("mobile-demo".to_string()),
                timestamp: Some("2026-05-06T14:31:00Z".to_string()),
            },
        };
        assert_eq!(
            serde_json::to_value(changed).unwrap(),
            json!({
                "schema_version": 1,
                "id": "0000000000000002",
                "created_at": "2026-05-06T14:31:00Z",
                "payload": {
                    "type": "agents_changed",
                    "data": {
                        "reason": "launch",
                        "agent_name": "mobile-demo",
                        "timestamp": "2026-05-06T14:31:00Z"
                    }
                }
            })
        );
    }

    #[test]
    fn mobile_agent_wire_json_snapshot() {
        let summary = MobileAgentSummaryWire {
            name: "mobile-demo".to_string(),
            project: Some("sase".to_string()),
            status: "running".to_string(),
            pid: Some(4242),
            model: Some("gpt-5.5".to_string()),
            provider: Some("codex".to_string()),
            workspace_number: Some(102),
            started_at: Some("2026-05-06T14:30:00Z".to_string()),
            duration_seconds: Some(90),
            prompt_snippet: Some("Implement gateway skeleton".to_string()),
            has_artifact_dir: true,
            retry_lineage: MobileAgentRetryLineageWire {
                retry_of_timestamp: None,
                retried_as_timestamp: None,
                retry_chain_root_timestamp: Some(
                    "2026-05-06T14:30:00Z".to_string(),
                ),
                retry_attempt: Some(0),
                parent_agent_name: None,
            },
            actions: MobileAgentActionAffordancesWire {
                can_resume: true,
                can_wait: true,
                can_kill: true,
                can_retry: false,
            },
            display: MobileAgentDisplayLabelsWire {
                title: "mobile-demo".to_string(),
                subtitle: Some("sase".to_string()),
                status_label: "Running".to_string(),
            },
        };
        assert_eq!(
            serde_json::to_value(MobileAgentListResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                agents: vec![summary],
                total_count: 1,
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "agents": [{
                    "name": "mobile-demo",
                    "project": "sase",
                    "status": "running",
                    "pid": 4242,
                    "model": "gpt-5.5",
                    "provider": "codex",
                    "workspace_number": 102,
                    "started_at": "2026-05-06T14:30:00Z",
                    "duration_seconds": 90,
                    "prompt_snippet": "Implement gateway skeleton",
                    "has_artifact_dir": true,
                    "retry_lineage": {
                        "retry_of_timestamp": null,
                        "retried_as_timestamp": null,
                        "retry_chain_root_timestamp": "2026-05-06T14:30:00Z",
                        "retry_attempt": 0,
                        "parent_agent_name": null
                    },
                    "actions": {
                        "can_resume": true,
                        "can_wait": true,
                        "can_kill": true,
                        "can_retry": false
                    },
                    "display": {
                        "title": "mobile-demo",
                        "subtitle": "sase",
                        "status_label": "Running"
                    }
                }],
                "total_count": 1
            })
        );

        assert_eq!(
            serde_json::to_value(MobileAgentResumeOptionsResponseWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                options: vec![MobileAgentResumeOptionWire {
                    id: "mobile-demo-resume".to_string(),
                    agent_name: "mobile-demo".to_string(),
                    kind: MobileAgentResumeOptionKindWire::Resume,
                    label: "Resume".to_string(),
                    prompt_text: "#resume:mobile-demo".to_string(),
                    direct_launch_supported: true,
                }],
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "options": [{
                    "id": "mobile-demo-resume",
                    "agent_name": "mobile-demo",
                    "kind": "resume",
                    "label": "Resume",
                    "prompt_text": "#resume:mobile-demo",
                    "direct_launch_supported": true
                }]
            })
        );

        let slot = MobileAgentLaunchSlotResultWire {
            slot_id: "0".to_string(),
            name: Some("mobile-demo".to_string()),
            status: MobileAgentLaunchSlotStatusWire::Launched,
            artifact_dir: Some("/tmp/sase/agents/mobile-demo".to_string()),
            message: None,
        };
        assert_eq!(
            serde_json::to_value(MobileAgentLaunchResultWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                primary: Some(slot.clone()),
                slots: vec![slot],
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "primary": {
                    "slot_id": "0",
                    "name": "mobile-demo",
                    "status": "launched",
                    "artifact_dir": "/tmp/sase/agents/mobile-demo",
                    "message": null
                },
                "slots": [{
                    "slot_id": "0",
                    "name": "mobile-demo",
                    "status": "launched",
                    "artifact_dir": "/tmp/sase/agents/mobile-demo",
                    "message": null
                }]
            })
        );

        assert_eq!(
            serde_json::to_value(MobileAgentImageLaunchRequestWire {
                schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
                prompt: "Review this screenshot".to_string(),
                original_filename: "screen.png".to_string(),
                content_type: "image/png".to_string(),
                byte_length: 8,
                base64_image: "iVBORw0K".to_string(),
                device_id: Some("device_123".to_string()),
                display_name: None,
                name: Some("mobile-demo".to_string()),
                model: None,
                provider: None,
                runtime: None,
                project: Some("sase".to_string()),
                dry_run: Some(false),
            })
            .unwrap(),
            json!({
                "schema_version": 1,
                "prompt": "Review this screenshot",
                "original_filename": "screen.png",
                "content_type": "image/png",
                "byte_length": 8,
                "base64_image": "iVBORw0K",
                "device_id": "device_123",
                "display_name": null,
                "name": "mobile-demo",
                "model": null,
                "provider": null,
                "runtime": null,
                "project": "sase",
                "dry_run": false
            })
        );
    }
}

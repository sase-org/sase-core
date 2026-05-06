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
    Heartbeat { sequence: u64 },
    Session { device_id: String },
    ResyncRequired { reason: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventRecordWire {
    pub schema_version: u32,
    pub id: String,
    pub created_at: String,
    pub payload: EventPayloadWire,
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
        let error = ApiErrorWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            code: ApiErrorCodeWire::Unauthorized,
            message: "authentication is required for this endpoint".to_string(),
            target: Some("authorization".to_string()),
            details: None,
        };
        assert_eq!(
            serde_json::to_value(error).unwrap(),
            json!({
                "schema_version": 1,
                "code": "unauthorized",
                "message": "authentication is required for this endpoint",
                "target": "authorization",
                "details": null
            })
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
    }
}

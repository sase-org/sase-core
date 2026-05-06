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
                "status": "placeholder_until_sse_phase",
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
                    "internal"
                ],
                "message": "string",
                "target": "string|null",
                "details": "json|null"
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

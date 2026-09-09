//! Health-payload classification for Tailnet discovery.

use serde_json::{Map, Value};

use super::wire::{
    MachineSetupDiagnosticWire, TailnetHealthRequestWire,
    TailnetHealthResultWire, COMPATIBILITY_COMPATIBLE,
    COMPATIBILITY_INCOMPATIBLE, COMPATIBILITY_UNKNOWN,
    MACHINE_SETUP_WIRE_SCHEMA_VERSION, SASE_GATEWAY_HEALTH_SERVICE,
};
use super::MachineSetupError;
use crate::fleet_contract::FLEET_PROTOCOL_VERSION;

/// Classify one health payload or host-collected probe error.
///
/// An older SASE gateway that returns `status=ok` without a fleet
/// advertisement is `unknown`. An explicitly unrelated healthy service
/// (`status=ok` with a non-SASE `service`) is `incompatible`.
pub fn classify_tailnet_health(
    request: &TailnetHealthRequestWire,
) -> Result<TailnetHealthResultWire, MachineSetupError> {
    MachineSetupError::check_schema(request.schema_version)?;
    if !request.error_code.trim().is_empty() {
        return Ok(classify_probe_error(
            &request.alias,
            request.error_code.trim(),
            request.error_reason.trim(),
        ));
    }
    match &request.payload {
        Some(Value::Object(map)) => {
            Ok(classify_health_object(&request.alias, map))
        }
        Some(_) => Ok(unrelated(
            &request.alias,
            "health response was not an object",
            format!(
                "{} returned non-object health JSON",
                display_alias(&request.alias)
            ),
        )),
        None => Err(MachineSetupError::Validation(
            "classify_tailnet_health requires payload or error_code"
                .to_string(),
        )),
    }
}

pub(super) fn classify_health_object(
    alias: &str,
    payload: &Map<String, Value>,
) -> TailnetHealthResultWire {
    match payload.get("fleet") {
        None | Some(Value::Null) => {
            classify_legacy_or_unrelated(alias, payload)
        }
        Some(Value::Object(fleet)) => {
            classify_fleet_advertisement(alias, fleet)
        }
        Some(_) => incompatible(
            alias,
            "tailnet_probe_fleet_malformed",
            "fleet advertisement was malformed",
            format!(
                "{} returned a malformed fleet advertisement",
                display_alias(alias)
            ),
        ),
    }
}

fn classify_legacy_or_unrelated(
    alias: &str,
    payload: &Map<String, Value>,
) -> TailnetHealthResultWire {
    if is_recognized_legacy_sase_health(payload) {
        TailnetHealthResultWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            compatibility: COMPATIBILITY_UNKNOWN.to_string(),
            reason: "health response has no fleet advertisement".to_string(),
            diagnostic: Some(MachineSetupDiagnosticWire::new(
                "tailnet_probe_fleet_unknown",
                "info",
                alias,
                format!(
                    "{} health did not advertise fleet protocol support",
                    display_alias(alias)
                ),
            )),
        }
    } else {
        unrelated(
            alias,
            "health response did not look like a SASE gateway",
            format!(
                "{} did not return a recognizable SASE health response",
                display_alias(alias)
            ),
        )
    }
}

fn is_recognized_legacy_sase_health(payload: &Map<String, Value>) -> bool {
    match payload.get("status") {
        Some(Value::String(status)) if status == "ok" => {}
        _ => return false,
    }
    match payload.get("service") {
        None | Some(Value::Null) => true,
        Some(Value::String(service)) => {
            service.is_empty() || service == SASE_GATEWAY_HEALTH_SERVICE
        }
        Some(_) => false,
    }
}

fn classify_fleet_advertisement(
    alias: &str,
    fleet: &Map<String, Value>,
) -> TailnetHealthResultWire {
    let Some(Value::Array(versions)) = fleet.get("supported_protocol_versions")
    else {
        return incompatible(
            alias,
            "tailnet_probe_fleet_malformed",
            "fleet protocol versions were malformed",
            format!(
                "{} returned malformed fleet protocol versions",
                display_alias(alias)
            ),
        );
    };

    let mut normalized = Vec::new();
    for version in versions {
        match json_protocol_version(version) {
            Some(value) => normalized.push(value),
            None => {
                return incompatible(
                    alias,
                    "tailnet_probe_fleet_malformed",
                    "fleet protocol versions were malformed",
                    format!(
                        "{} returned malformed fleet protocol versions",
                        display_alias(alias)
                    ),
                );
            }
        }
    }

    if normalized.contains(&FLEET_PROTOCOL_VERSION) {
        TailnetHealthResultWire {
            schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
            compatibility: COMPATIBILITY_COMPATIBLE.to_string(),
            reason: format!(
                "fleet protocol v{FLEET_PROTOCOL_VERSION} advertised"
            ),
            diagnostic: None,
        }
    } else {
        incompatible(
            alias,
            "tailnet_probe_fleet_incompatible",
            "fleet protocol version is unsupported",
            format!(
                "{} did not advertise fleet protocol v{FLEET_PROTOCOL_VERSION}",
                display_alias(alias)
            ),
        )
    }
}

fn json_protocol_version(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .or_else(|| {
                number.as_i64().and_then(|value| u32::try_from(value).ok())
            }),
        _ => None,
    }
}

pub(super) fn classify_probe_error(
    alias: &str,
    code: &str,
    reason: &str,
) -> TailnetHealthResultWire {
    let (compatibility, message) = match code {
        "tailnet_probe_http_error" => (
            COMPATIBILITY_INCOMPATIBLE,
            if reason.starts_with("health returned HTTP ") {
                format!("{} {reason}", display_alias(alias))
            } else {
                format!(
                    "{} health probe returned HTTP {reason}",
                    display_alias(alias)
                )
            },
        ),
        "tailnet_probe_unrelated_service" => (
            COMPATIBILITY_INCOMPATIBLE,
            if reason.is_empty() {
                format!(
                    "{} did not return a recognizable SASE health response",
                    display_alias(alias)
                )
            } else {
                format!("{} {reason}", display_alias(alias))
            },
        ),
        "tailnet_probe_fleet_malformed"
        | "tailnet_probe_fleet_incompatible" => (
            COMPATIBILITY_INCOMPATIBLE,
            format!("{} health probe failed: {reason}", display_alias(alias)),
        ),
        _ => (
            COMPATIBILITY_UNKNOWN,
            format!("{} health probe failed: {reason}", display_alias(alias)),
        ),
    };
    TailnetHealthResultWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        compatibility: compatibility.to_string(),
        reason: if reason.is_empty() {
            code.to_string()
        } else {
            reason.to_string()
        },
        diagnostic: Some(MachineSetupDiagnosticWire::new(
            code, "warning", alias, message,
        )),
    }
}

fn unrelated(
    alias: &str,
    reason: &str,
    message: String,
) -> TailnetHealthResultWire {
    incompatible(alias, "tailnet_probe_unrelated_service", reason, message)
}

fn incompatible(
    alias: &str,
    code: &str,
    reason: &str,
    message: String,
) -> TailnetHealthResultWire {
    TailnetHealthResultWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        compatibility: COMPATIBILITY_INCOMPATIBLE.to_string(),
        reason: reason.to_string(),
        diagnostic: Some(MachineSetupDiagnosticWire::new(
            code, "warning", alias, message,
        )),
    }
}

fn display_alias(alias: &str) -> &str {
    if alias.is_empty() {
        "peer"
    } else {
        alias
    }
}

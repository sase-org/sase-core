//! Versioned wire contract for Tailnet discovery and enrollment
//! reconciliation policy.
//!
//! Callers supply already-observed Tailscale status JSON and optional
//! health payloads; this module never runs subprocesses or HTTP.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Schema version for machine-setup request and result envelopes.
pub const MACHINE_SETUP_WIRE_SCHEMA_VERSION: u32 = 1;

/// Built-in Tailnet discovery provider reference written onto candidates.
pub const TAILNET_PROVIDER_REF: &str = "builtin@tailnet";

/// Public health `service` value advertised by a SASE gateway.
pub const SASE_GATEWAY_HEALTH_SERVICE: &str = "sase_gateway";

pub const COMPATIBILITY_COMPATIBLE: &str = "compatible";
pub const COMPATIBILITY_UNKNOWN: &str = "unknown";
pub const COMPATIBILITY_INCOMPATIBLE: &str = "incompatible";

pub const ENDPOINT_SOURCE_DNS: &str = "dns";
pub const ENDPOINT_SOURCE_OVERRIDE: &str = "override";

pub const RECONCILE_STATUS_NEW: &str = "new";
pub const RECONCILE_STATUS_ENROLLED: &str = "enrolled";
pub const RECONCILE_STATUS_REPAIR: &str = "repair";

fn default_schema_version() -> u32 {
    MACHINE_SETUP_WIRE_SCHEMA_VERSION
}

/// One structured diagnostic produced by discovery classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineSetupDiagnosticWire {
    pub code: String,
    pub message: String,
    #[serde(default = "default_warning_severity")]
    pub severity: String,
    #[serde(default)]
    pub alias: String,
}

fn default_warning_severity() -> String {
    "warning".to_string()
}

impl MachineSetupDiagnosticWire {
    pub fn new(
        code: impl Into<String>,
        severity: impl Into<String>,
        alias: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            severity: severity.into(),
            alias: alias.into(),
            message: message.into(),
        }
    }
}

/// One discovery candidate. Tailnet classification never infers a SASE
/// machine selector or installation pin from unauthenticated observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DiscoveryCandidateWire {
    #[serde(default)]
    pub provider_ref: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub machine_selector: String,
    #[serde(default)]
    pub installation_pin: String,
    #[serde(default)]
    pub detail: String,
}

/// One classified Tailnet peer, including peers that lack a usable endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailnetPeerWire {
    pub peer_key: String,
    pub alias: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub endpoint_source: String,
    #[serde(default)]
    pub online: Option<bool>,
    #[serde(default)]
    pub os_hint: String,
}

/// Host-collected health observation for one candidate endpoint.
///
/// Supply `payload` for a decoded JSON health body. Supply `error_code` /
/// `error_reason` for transport failures classified outside this module.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct TailnetHealthObservationWire {
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub error_code: String,
    #[serde(default)]
    pub error_reason: String,
}

/// Request envelope for classifying one health payload or probe error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TailnetHealthRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub payload: Option<Value>,
    #[serde(default)]
    pub error_code: String,
    #[serde(default)]
    pub error_reason: String,
}

/// Compatibility classification for one health observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailnetHealthResultWire {
    pub schema_version: u32,
    pub compatibility: String,
    pub reason: String,
    #[serde(default)]
    pub diagnostic: Option<MachineSetupDiagnosticWire>,
}

/// Request envelope for parsing Tailscale status and assembling candidates.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TailnetDiscoveryRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub status: Value,
    #[serde(default)]
    pub endpoint_overrides: Value,
    #[serde(default)]
    pub health_observations: Vec<TailnetHealthObservationWire>,
}

/// Parsed peers, candidates, and diagnostics from a status observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TailnetDiscoveryResultWire {
    pub schema_version: u32,
    #[serde(default)]
    pub peers: Vec<TailnetPeerWire>,
    #[serde(default)]
    pub candidates: Vec<DiscoveryCandidateWire>,
    #[serde(default)]
    pub diagnostics: Vec<MachineSetupDiagnosticWire>,
}

/// One enrolled machine record used for reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct EnrolledMachineWire {
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub provider_ref: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub pinned_installation_id: String,
}

/// Request envelope for enrollment reconciliation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineReconcileRequestWire {
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub candidates: Vec<DiscoveryCandidateWire>,
    #[serde(default)]
    pub enrolled: Vec<EnrolledMachineWire>,
}

/// One candidate classified against the local registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciledCandidateWire {
    pub candidate: DiscoveryCandidateWire,
    pub status: String,
    #[serde(default)]
    pub alias: String,
    #[serde(default)]
    pub reason: String,
}

/// Reconciliation result envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineReconcileResultWire {
    pub schema_version: u32,
    #[serde(default)]
    pub items: Vec<ReconciledCandidateWire>,
}

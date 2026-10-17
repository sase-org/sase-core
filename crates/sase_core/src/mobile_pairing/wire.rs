use serde::{Deserialize, Serialize};

/// Wire schema for local pairing issue/consume records.
pub const MOBILE_PAIRING_WIRE_SCHEMA_VERSION: u32 = 1;

/// Health/capability marker for owner-local challenge issuance.
pub const MOBILE_PAIRING_ISSUANCE: &str = "owner_local_v1";

/// Default challenge lifetime.
pub const MOBILE_PAIRING_TTL: chrono::TimeDelta = chrono::TimeDelta::minutes(5);

/// Request a new owner-local pairing challenge.
#[derive(Clone, Serialize, Deserialize)]
pub struct MobilePairingIssueRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub host_label: Option<String>,
    #[serde(default)]
    pub state_root: Option<String>,
}

impl std::fmt::Debug for MobilePairingIssueRequestWire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MobilePairingIssueRequestWire")
            .field("schema_version", &self.schema_version)
            .field("host_label", &self.host_label)
            .field("state_root", &self.state_root)
            .finish()
    }
}

/// One-time challenge returned to the local owner CLI.
#[derive(Clone, Serialize, Deserialize)]
pub struct MobilePairingIssueResponseWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    pub expires_at: String,
    pub host_label: String,
    pub host_fingerprint: Option<String>,
}

impl std::fmt::Debug for MobilePairingIssueResponseWire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MobilePairingIssueResponseWire")
            .field("schema_version", &self.schema_version)
            .field("pairing_id", &redacted())
            .field("code", &redacted())
            .field("expires_at", &self.expires_at)
            .field("host_label", &self.host_label)
            .field("host_fingerprint", &self.host_fingerprint)
            .finish()
    }
}

/// Redeem a locally issued pairing challenge.
#[derive(Clone, Serialize, Deserialize)]
pub struct MobilePairingConsumeRequestWire {
    pub schema_version: u32,
    pub pairing_id: String,
    pub code: String,
    #[serde(default)]
    pub state_root: Option<String>,
}

impl std::fmt::Debug for MobilePairingConsumeRequestWire {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MobilePairingConsumeRequestWire")
            .field("schema_version", &self.schema_version)
            .field("pairing_id", &redacted())
            .field("code", &redacted())
            .field("state_root", &self.state_root)
            .finish()
    }
}

/// Result of consuming a pairing challenge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MobilePairingConsumeOutcome {
    Accepted,
    Rejected,
    Expired,
}

fn redacted() -> &'static str {
    "<redacted>"
}

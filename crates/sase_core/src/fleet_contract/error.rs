use std::io;
use std::path::PathBuf;
use std::time::Duration;
use thiserror::Error;

/// Schema version shared by the fleet-data contract surface.
///
/// This is independent of the hello envelope schema, the capability-set
/// schema, and the fleet protocol version. Gateways advertise it as
/// `fleet_contract_schema_version` on hello; older hellos omit the field.
pub const FLEET_CONTRACT_SCHEMA_VERSION: u32 = 5;

pub(crate) const FLEET_CONTRACT_MIN_READABLE_SCHEMA_VERSION: u32 = 1;

/// Current fleet protocol version advertised by gateways and required by
/// viewers. Discovery compatibility is derived from this constant.
pub const FLEET_PROTOCOL_VERSION: u32 = 1;

/// Current persisted installation-identity file schema.
pub const FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION: u32 = 1;

/// File under SASE home that stores this user's opaque installation identity.
pub const FLEET_INSTALLATION_IDENTITY_FILENAME: &str =
    "installation_identity.json";

/// Reject identity state above this byte limit without overwriting it.
pub const FLEET_INSTALLATION_IDENTITY_MAX_BYTES: usize = 16 * 1024;

/// Versioned, recognizable prefix for opaque installation IDs.
pub const FLEET_INSTALLATION_ID_PREFIX: &str = "sase_inst_v1_";

/// The explicit zero cursor generation.
pub const FLEET_INITIAL_CURSOR_GENERATION: &str = "initial";

pub(crate) const LOCK_TIMEOUT_ENV: &str = "SASE_FLEET_IDENTITY_LOCK_TIMEOUT";

pub(crate) const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);

pub(crate) const STALE_TEMP_MAX_AGE: Duration =
    Duration::from_secs(24 * 60 * 60);

pub(crate) const MAX_IDENTIFIER_BYTES: usize = 128;

pub(crate) const MAX_LABEL_BYTES: usize = 256;

pub(crate) const MAX_KEY_BYTES: usize = 1024;

pub(crate) const MAX_CAPABILITY_BYTES: usize = 80;

pub(crate) const MAX_INTENT_BYTES: usize = 512;

pub(crate) const MAX_LAUNCH_PROMPT_BYTES: usize = 64 * 1024;

pub(crate) const PAYLOAD_FINGERPRINT_DOMAIN: &[u8] =
    b"sase-fleet-operation-payload-v1\0";

/// Default catalog page size for fleet reads.
pub const FLEET_READ_DEFAULT_PAGE_ROWS: u32 = 50;

/// Hard cap for one fleet catalog page.
pub const FLEET_READ_MAX_PAGE_ROWS: u32 = 100;

/// Hard cap for one logical-ID batch lookup.
pub const FLEET_READ_MAX_BATCH_IDS: usize = 200;

/// Hard cap for one project-eligibility lookup.
pub const FLEET_READ_MAX_PROJECT_IDS: usize = 200;

/// Hard cap for catalog free-text query values.
pub const FLEET_READ_MAX_QUERY_BYTES: usize = 512;

/// Hard cap for exact string filters.
pub const FLEET_READ_MAX_FILTER_BYTES: usize = 128;

/// Default bytes returned by one content read.
pub const FLEET_READ_DEFAULT_CONTENT_BYTES: u64 = 64 * 1024;

/// Hard cap for one content read.
pub const FLEET_READ_MAX_CONTENT_BYTES: u64 = 256 * 1024;

/// Default replay ring capacity for durable fleet invalidations.
pub const FLEET_READ_DEFAULT_REPLAY_EVENTS: usize = 128;

/// Hard cap for durable fleet invalidation replay.
pub const FLEET_READ_MAX_REPLAY_EVENTS: usize = 512;

/// Versioned, recognizable prefix for opaque catalog snapshot IDs.
pub const FLEET_CATALOG_SNAPSHOT_ID_PREFIX: &str = "catsnap_v1_";

pub(crate) const FLEET_CATALOG_CURSOR_PREFIX: &str = "catcur_v1";

#[derive(Debug, Error)]
pub enum FleetContractError {
    #[error("{0}")]
    Validation(String),
    #[error(
        "timed out after {waited_ms}ms waiting for {mode} lock {}: holder: {holder}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
        holder: String,
    },
    #[error("fleet contract I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet contract JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

pub fn fleet_contract_schema_version() -> u32 {
    FLEET_CONTRACT_SCHEMA_VERSION
}

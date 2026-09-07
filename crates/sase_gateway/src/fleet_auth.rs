use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::SystemTime,
};

use fs2::FileExt;
use rand::{distributions::Alphanumeric, Rng};
use sase_core::fleet_contract::{
    ensure_installation_identity, CapabilitySetWire, FleetContractError,
    InstallationIdentityRecordWire,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    storage::generate_prefixed_id,
    wire::{
        FleetBootstrapIssueRequestWire, FleetBootstrapIssueResponseWire,
        FleetControllerMetadataWire, FleetCredentialRecordWire,
        FleetCredentialRevokeRequestWire, FleetCredentialRevokeResponseWire,
        FleetEnrollmentRequestWire, FleetEnrollmentResponseWire,
        FleetQuarantineWire, FleetTokenRotateResponseWire,
        FLEET_API_WIRE_SCHEMA_VERSION, FLEET_PROTOCOL_VERSION,
    },
};

pub const FLEET_AUTH_DIR: &str = "fleet_gateway";
pub const FLEET_AUTH_FILE: &str = "credentials.json";
pub const FLEET_AUTH_LOCK_FILE: &str = "credentials.lock";
pub const FLEET_AUTH_STORE_SCHEMA_VERSION: u32 = 1;
pub const FLEET_BOOTSTRAP_TTL_SECONDS: f64 = 10.0 * 60.0;
pub const FLEET_CREDENTIAL_TTL_SECONDS: f64 = 90.0 * 24.0 * 60.0 * 60.0;
pub const FLEET_SCOPE_HELLO: &str = "fleet.hello";
pub const FLEET_SCOPE_LAUNCH: &str = "fleet.launch";
pub const FLEET_SCOPE_ROTATE: &str = "fleet.credential.rotate";
pub const FLEET_SCOPE_REVOKE: &str = "fleet.credential.revoke";
pub const FLEET_SCOPE_SUMMARY_READ: &str = "fleet.summary.read";
pub const FLEET_SCOPE_CATALOG_READ: &str = "fleet.catalog.read";
pub const FLEET_SCOPE_BATCH_READ: &str = "fleet.batch.read";
pub const FLEET_SCOPE_DETAIL_READ: &str = "fleet.detail.read";
pub const FLEET_SCOPE_CONTENT_READ: &str = "fleet.content.read";
pub const FLEET_SCOPE_PROJECTS_READ: &str = "fleet.projects.read";
pub const FLEET_SCOPE_EVENTS_READ: &str = "fleet.events.read";

const MAX_AUTH_FILE_BYTES: u64 = 512 * 1024;
const MAX_LABEL_BYTES: usize = 256;
const MAX_SCOPE_BYTES: usize = 80;
const SECRET_HASH_DOMAIN: &[u8] = b"sase-fleet-bootstrap-v1\0";
const TOKEN_HASH_DOMAIN: &[u8] = b"sase-fleet-token-v1\0";

#[derive(Clone, Debug)]
pub struct FleetCredentialStore {
    sase_home: Arc<PathBuf>,
    state_dir: Arc<PathBuf>,
    auth_path: Arc<PathBuf>,
    lock_path: Arc<PathBuf>,
    cache: Arc<Mutex<FleetAuthCache>>,
}

impl FleetCredentialStore {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        let sase_home = sase_home.into();
        let state_dir = sase_home.join(FLEET_AUTH_DIR);
        Self {
            sase_home: Arc::new(sase_home),
            auth_path: Arc::new(state_dir.join(FLEET_AUTH_FILE)),
            lock_path: Arc::new(state_dir.join(FLEET_AUTH_LOCK_FILE)),
            state_dir: Arc::new(state_dir),
            cache: Arc::new(Mutex::new(FleetAuthCache::default())),
        }
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn auth_path(&self) -> &Path {
        &self.auth_path
    }

    pub fn issue_bootstrap(
        &self,
        request: FleetBootstrapIssueRequestWire,
        now_unix: f64,
    ) -> Result<FleetBootstrapIssueResponseWire, FleetStoreError> {
        validate_schema(request.schema_version)?;
        validate_timestamp("now_unix", now_unix)?;
        let installation = self.ensure_installation_identity()?;
        if let Some(pin) = request.installation_pin.as_deref() {
            validate_label("installation_pin", pin)?;
            if pin != installation.installation_id {
                return Err(FleetStoreError::Validation(
                    "installation_pin does not match the current installation"
                        .to_string(),
                ));
            }
        }
        let allowed_scopes = normalize_scopes_or_default(
            "requested_scopes",
            &request.requested_scopes,
        )?;
        let protocol_versions =
            normalize_protocol_versions(&request.supported_protocol_versions);
        if negotiate_fleet_protocol_version(&protocol_versions).is_none() {
            return Err(FleetStoreError::IncompatibleProtocol);
        }
        let expires_at_unix = request
            .expires_at_unix
            .unwrap_or(now_unix + FLEET_BOOTSTRAP_TTL_SECONDS);
        validate_timestamp("expires_at_unix", expires_at_unix)?;
        if expires_at_unix <= now_unix {
            return Err(FleetStoreError::Validation(
                "expires_at_unix must be in the future".to_string(),
            ));
        }

        let bootstrap_id = generate_prefixed_id("boot");
        let bootstrap_secret = generate_secret("sase_bootstrap", 64);
        let record = StoredFleetBootstrap {
            bootstrap_id: bootstrap_id.clone(),
            secret_hash: hash_secret(SECRET_HASH_DOMAIN, &bootstrap_secret),
            allowed_scopes: allowed_scopes.clone(),
            supported_protocol_versions: protocol_versions.clone(),
            pinned_installation_id: installation.installation_id.clone(),
            issued_at_unix: now_unix,
            expires_at_unix,
            consumed_at_unix: None,
            consumed_by_credential_id: None,
            revoked_at_unix: None,
        };

        let _lock = self.lock_file()?;
        let mut file = self.read_auth_file_unlocked()?;
        file.bootstraps.push(record);
        self.write_auth_file_unlocked(&file)?;
        Ok(FleetBootstrapIssueResponseWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            bootstrap_id,
            bootstrap_secret,
            expires_at_unix,
            allowed_scopes,
            pinned_installation_id: installation.installation_id,
            protocol_versions,
        })
    }

    pub fn enroll(
        &self,
        request: FleetEnrollmentRequestWire,
        now_unix: f64,
    ) -> Result<FleetEnrollmentResult, FleetStoreError> {
        validate_schema(request.schema_version)?;
        validate_timestamp("now_unix", now_unix)?;
        validate_label("bootstrap_id", &request.bootstrap_id)?;
        validate_label("bootstrap_secret", &request.bootstrap_secret)?;
        validate_label(
            "pinned_installation_id",
            &request.pinned_installation_id,
        )?;
        let requested_scopes = normalize_scopes_or_default(
            "requested_scopes",
            &request.requested_scopes,
        )?;
        let protocol_version = negotiate_fleet_protocol_version(
            &request.supported_protocol_versions,
        )
        .ok_or(FleetStoreError::IncompatibleProtocol)?;
        let controller = normalize_controller(request.controller)?;

        let _lock = self.lock_file()?;
        let mut file = self.read_auth_file_unlocked()?;
        let secret_hash =
            hash_secret(SECRET_HASH_DOMAIN, &request.bootstrap_secret);
        let mut matched_index = None;
        for (index, bootstrap) in file.bootstraps.iter().enumerate() {
            let id_matches = bootstrap.bootstrap_id == request.bootstrap_id;
            let secret_matches = constant_time_eq(
                bootstrap.secret_hash.as_bytes(),
                secret_hash.as_bytes(),
            );
            if id_matches && secret_matches && matched_index.is_none() {
                matched_index = Some(index);
            }
        }
        let Some(index) = matched_index else {
            return Err(FleetStoreError::BootstrapRejected);
        };
        let bootstrap = &file.bootstraps[index];
        if bootstrap.revoked_at_unix.is_some() {
            return Err(FleetStoreError::BootstrapRejected);
        }
        if bootstrap.consumed_at_unix.is_some() {
            return Err(FleetStoreError::BootstrapConsumed);
        }
        if bootstrap.expires_at_unix <= now_unix {
            return Err(FleetStoreError::BootstrapExpired);
        }
        let installation = self.ensure_installation_identity()?;
        if bootstrap.pinned_installation_id != request.pinned_installation_id {
            return Ok(FleetEnrollmentResult::Quarantined(Box::new(
                FleetEnrollmentResponseWire {
                    schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
                    outcome: "quarantined".to_string(),
                    protocol_version: Some(protocol_version),
                    installation: installation.clone(),
                    machine_selector: String::new(),
                    capabilities: fleet_capabilities(&[]),
                    credential: None,
                    token_type: None,
                    token: None,
                    quarantine: Some(FleetQuarantineWire {
                        schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
                        reason: "installation_pin_mismatch".to_string(),
                        presented_installation_id: request
                            .pinned_installation_id,
                        authoritative_installation_id: installation
                            .installation_id,
                    }),
                },
            )));
        }
        let bootstrap_scopes = normalize_scopes_or_default(
            "bootstrap.allowed_scopes",
            &bootstrap.allowed_scopes,
        )?;
        if !bootstrap
            .supported_protocol_versions
            .contains(&protocol_version)
        {
            return Err(FleetStoreError::IncompatibleProtocol);
        }
        for scope in &requested_scopes {
            if !bootstrap_scopes.iter().any(|allowed| allowed == scope) {
                return Err(FleetStoreError::ScopeDenied(scope.clone()));
            }
        }

        let scopes = requested_scopes;
        let credential_id = generate_prefixed_id("cred");
        let token = generate_secret("sase_fleet", 72);
        let credential = StoredFleetCredential {
            credential_id: credential_id.clone(),
            controller,
            token_hash: hash_secret(TOKEN_HASH_DOMAIN, &token),
            scopes: scopes.clone(),
            issued_at_unix: now_unix,
            expires_at_unix: Some(now_unix + FLEET_CREDENTIAL_TTL_SECONDS),
            rotated_at_unix: None,
            revoked_at_unix: None,
            revoked_reason: None,
        };
        file.bootstraps[index].consumed_at_unix = Some(now_unix);
        file.bootstraps[index].consumed_by_credential_id =
            Some(credential_id.clone());
        file.credentials.push(credential.clone());
        self.write_auth_file_unlocked(&file)?;
        let wire = credential.to_wire();
        Ok(FleetEnrollmentResult::Enrolled(Box::new(
            FleetEnrollmentSuccess {
                installation,
                credential: wire,
                token,
                protocol_version,
            },
        )))
    }

    pub fn authenticate_token(
        &self,
        token: &str,
        now_unix: f64,
    ) -> Result<FleetAuthentication, FleetStoreError> {
        validate_timestamp("now_unix", now_unix)?;
        let token_hash = hash_secret(TOKEN_HASH_DOMAIN, token);
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| FleetStoreError::LockPoisoned)?;
        let fingerprint = self.file_fingerprint()?;
        if cache.fingerprint != fingerprint {
            let file = self.read_auth_file_unlocked()?;
            cache.fingerprint = fingerprint;
            cache.credentials = file
                .credentials
                .iter()
                .cloned()
                .map(CachedFleetCredential::from)
                .collect();
            cache.refresh_count = cache.refresh_count.saturating_add(1);
        }
        let (record, _) =
            find_cached_credential(&cache.credentials, &token_hash);
        let Some(record) = record else {
            return Ok(FleetAuthentication::Missing);
        };
        if record.revoked_at_unix.is_some() {
            return Ok(FleetAuthentication::Revoked(record.to_wire()));
        }
        if record
            .expires_at_unix
            .is_some_and(|expires_at| expires_at <= now_unix)
        {
            return Ok(FleetAuthentication::Expired(record.to_wire()));
        }
        Ok(FleetAuthentication::Active(record.to_wire()))
    }

    pub fn rotate_credential(
        &self,
        credential_id: &str,
        now_unix: f64,
    ) -> Result<FleetTokenRotateResponseWire, FleetStoreError> {
        validate_label("credential_id", credential_id)?;
        validate_timestamp("now_unix", now_unix)?;
        let _lock = self.lock_file()?;
        let mut file = self.read_auth_file_unlocked()?;
        let Some(record) = file
            .credentials
            .iter_mut()
            .find(|record| record.credential_id == credential_id)
        else {
            return Err(FleetStoreError::CredentialMissing);
        };
        ensure_credential_active(record, now_unix)?;
        let token = generate_secret("sase_fleet", 72);
        record.token_hash = hash_secret(TOKEN_HASH_DOMAIN, &token);
        record.rotated_at_unix = Some(now_unix);
        record.expires_at_unix = Some(now_unix + FLEET_CREDENTIAL_TTL_SECONDS);
        let credential = record.to_wire();
        self.write_auth_file_unlocked(&file)?;
        Ok(FleetTokenRotateResponseWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            protocol_version: FLEET_PROTOCOL_VERSION,
            credential,
            token_type: "bearer".to_string(),
            token,
        })
    }

    pub fn revoke_credential(
        &self,
        credential_id: &str,
        request: FleetCredentialRevokeRequestWire,
        now_unix: f64,
    ) -> Result<FleetCredentialRevokeResponseWire, FleetStoreError> {
        validate_schema(request.schema_version)?;
        validate_label("credential_id", credential_id)?;
        validate_timestamp("now_unix", now_unix)?;
        if let Some(reason) = request.reason.as_deref() {
            validate_label("reason", reason)?;
        }
        let _lock = self.lock_file()?;
        let mut file = self.read_auth_file_unlocked()?;
        let Some(record) = file
            .credentials
            .iter_mut()
            .find(|record| record.credential_id == credential_id)
        else {
            return Err(FleetStoreError::CredentialMissing);
        };
        let revoked = record.revoked_at_unix.is_none();
        if revoked {
            record.revoked_at_unix = Some(now_unix);
            record.revoked_reason =
                request.reason.map(|reason| reason.trim().to_string());
        }
        let credential = record.to_wire();
        self.write_auth_file_unlocked(&file)?;
        Ok(FleetCredentialRevokeResponseWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            credential,
            revoked,
        })
    }

    pub fn ensure_installation_identity(
        &self,
    ) -> Result<InstallationIdentityRecordWire, FleetStoreError> {
        Ok(ensure_installation_identity(&self.sase_home)?.record)
    }

    fn lock_file(&self) -> Result<HeldFleetStoreLock, FleetStoreError> {
        ensure_private_dir(&self.state_dir)?;
        let file = open_private_file(&self.lock_path)?;
        file.lock_exclusive()
            .map_err(|source| FleetStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            })?;
        Ok(HeldFleetStoreLock { file })
    }

    fn read_auth_file_unlocked(
        &self,
    ) -> Result<StoredFleetAuthFile, FleetStoreError> {
        if !self.auth_path.exists() {
            return Ok(StoredFleetAuthFile::default());
        }
        let metadata =
            fs::metadata(self.auth_path.as_ref()).map_err(|source| {
                FleetStoreError::Io {
                    path: self.auth_path.as_ref().clone(),
                    source,
                }
            })?;
        if metadata.len() > MAX_AUTH_FILE_BYTES {
            return Err(FleetStoreError::Validation(format!(
                "fleet auth file exceeds {MAX_AUTH_FILE_BYTES} bytes"
            )));
        }
        let bytes = fs::read(self.auth_path.as_ref()).map_err(|source| {
            FleetStoreError::Io {
                path: self.auth_path.as_ref().clone(),
                source,
            }
        })?;
        serde_json::from_slice(&bytes).map_err(|source| FleetStoreError::Json {
            path: self.auth_path.as_ref().clone(),
            source,
        })
    }

    fn write_auth_file_unlocked(
        &self,
        file: &StoredFleetAuthFile,
    ) -> Result<(), FleetStoreError> {
        ensure_private_dir(&self.state_dir)?;
        let mut normalized = file.clone();
        normalized.schema_version = FLEET_AUTH_STORE_SCHEMA_VERSION;
        normalized.revision = normalized.revision.saturating_add(1);
        let bytes =
            serde_json::to_vec_pretty(&normalized).map_err(|source| {
                FleetStoreError::Json {
                    path: self.auth_path.as_ref().clone(),
                    source,
                }
            })?;
        let temp_path = self.state_dir.join(format!(
            "{FLEET_AUTH_FILE}.{}.tmp",
            generate_prefixed_id("write")
        ));
        {
            let mut temp = create_private_file(&temp_path)?;
            temp.write_all(&bytes)
                .map_err(|source| FleetStoreError::Io {
                    path: temp_path.clone(),
                    source,
                })?;
            temp.write_all(b"\n")
                .map_err(|source| FleetStoreError::Io {
                    path: temp_path.clone(),
                    source,
                })?;
            temp.sync_all().map_err(|source| FleetStoreError::Io {
                path: temp_path.clone(),
                source,
            })?;
        }
        fs::rename(&temp_path, self.auth_path.as_ref()).map_err(|source| {
            FleetStoreError::Io {
                path: self.auth_path.as_ref().clone(),
                source,
            }
        })?;
        set_private_file_mode(self.auth_path.as_ref())?;
        self.refresh_cache_after_write(&normalized)?;
        Ok(())
    }

    fn refresh_cache_after_write(
        &self,
        file: &StoredFleetAuthFile,
    ) -> Result<(), FleetStoreError> {
        let fingerprint = self.file_fingerprint()?;
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| FleetStoreError::LockPoisoned)?;
        cache.fingerprint = fingerprint;
        cache.credentials = file
            .credentials
            .iter()
            .cloned()
            .map(CachedFleetCredential::from)
            .collect();
        cache.refresh_count = cache.refresh_count.saturating_add(1);
        Ok(())
    }

    fn file_fingerprint(
        &self,
    ) -> Result<Option<FileFingerprint>, FleetStoreError> {
        match fs::metadata(self.auth_path.as_ref()) {
            Ok(metadata) => Ok(Some(FileFingerprint {
                len: metadata.len(),
                modified: metadata.modified().ok(),
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FleetStoreError::Io {
                path: self.auth_path.as_ref().clone(),
                source,
            }),
        }
    }

    #[cfg(test)]
    fn cache_refresh_count(&self) -> usize {
        self.cache.lock().unwrap().refresh_count
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FleetEnrollmentResult {
    Enrolled(Box<FleetEnrollmentSuccess>),
    Quarantined(Box<FleetEnrollmentResponseWire>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FleetEnrollmentSuccess {
    pub installation: InstallationIdentityRecordWire,
    pub credential: FleetCredentialRecordWire,
    pub token: String,
    pub protocol_version: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FleetAuthentication {
    Active(FleetCredentialRecordWire),
    Expired(FleetCredentialRecordWire),
    Revoked(FleetCredentialRecordWire),
    Missing,
}

#[derive(Debug, Error)]
pub enum FleetStoreError {
    #[error("fleet credential store lock was poisoned")]
    LockPoisoned,
    #[error("fleet credential store validation failed: {0}")]
    Validation(String),
    #[error("fleet credential store I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet credential store JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("fleet contract failed: {0}")]
    FleetContract(#[from] FleetContractError),
    #[error("bootstrap secret is expired")]
    BootstrapExpired,
    #[error("bootstrap secret was already used")]
    BootstrapConsumed,
    #[error("bootstrap secret was rejected")]
    BootstrapRejected,
    #[error("credential is expired")]
    CredentialExpired,
    #[error("credential was not found")]
    CredentialMissing,
    #[error("credential is revoked")]
    CredentialRevoked,
    #[error("no mutually supported fleet protocol version")]
    IncompatibleProtocol,
    #[error("scope {0} is not authorized")]
    ScopeDenied(String),
}

#[derive(Debug)]
struct HeldFleetStoreLock {
    file: File,
}

impl Drop for HeldFleetStoreLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredFleetAuthFile {
    schema_version: u32,
    revision: u64,
    bootstraps: Vec<StoredFleetBootstrap>,
    credentials: Vec<StoredFleetCredential>,
}

impl Default for StoredFleetAuthFile {
    fn default() -> Self {
        Self {
            schema_version: FLEET_AUTH_STORE_SCHEMA_VERSION,
            revision: 0,
            bootstraps: Vec::new(),
            credentials: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredFleetBootstrap {
    bootstrap_id: String,
    secret_hash: String,
    allowed_scopes: Vec<String>,
    supported_protocol_versions: Vec<u32>,
    pinned_installation_id: String,
    issued_at_unix: f64,
    expires_at_unix: f64,
    consumed_at_unix: Option<f64>,
    consumed_by_credential_id: Option<String>,
    revoked_at_unix: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredFleetCredential {
    credential_id: String,
    controller: FleetControllerMetadataWire,
    token_hash: String,
    scopes: Vec<String>,
    issued_at_unix: f64,
    expires_at_unix: Option<f64>,
    rotated_at_unix: Option<f64>,
    revoked_at_unix: Option<f64>,
    revoked_reason: Option<String>,
}

impl StoredFleetCredential {
    fn to_wire(&self) -> FleetCredentialRecordWire {
        FleetCredentialRecordWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            credential_id: self.credential_id.clone(),
            controller_id: self.controller.controller_id.clone(),
            controller: self.controller.clone(),
            scopes: self.scopes.clone(),
            issued_at_unix: self.issued_at_unix,
            expires_at_unix: self.expires_at_unix,
            rotated_at_unix: self.rotated_at_unix,
            revoked_at_unix: self.revoked_at_unix,
            revoked_reason: self.revoked_reason.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct CachedFleetCredential {
    credential_id: String,
    controller: FleetControllerMetadataWire,
    token_hash: String,
    scopes: Vec<String>,
    issued_at_unix: f64,
    expires_at_unix: Option<f64>,
    rotated_at_unix: Option<f64>,
    revoked_at_unix: Option<f64>,
    revoked_reason: Option<String>,
}

impl From<StoredFleetCredential> for CachedFleetCredential {
    fn from(value: StoredFleetCredential) -> Self {
        Self {
            credential_id: value.credential_id,
            controller: value.controller,
            token_hash: value.token_hash,
            scopes: value.scopes,
            issued_at_unix: value.issued_at_unix,
            expires_at_unix: value.expires_at_unix,
            rotated_at_unix: value.rotated_at_unix,
            revoked_at_unix: value.revoked_at_unix,
            revoked_reason: value.revoked_reason,
        }
    }
}

impl CachedFleetCredential {
    fn to_wire(&self) -> FleetCredentialRecordWire {
        FleetCredentialRecordWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            credential_id: self.credential_id.clone(),
            controller_id: self.controller.controller_id.clone(),
            controller: self.controller.clone(),
            scopes: self.scopes.clone(),
            issued_at_unix: self.issued_at_unix,
            expires_at_unix: self.expires_at_unix,
            rotated_at_unix: self.rotated_at_unix,
            revoked_at_unix: self.revoked_at_unix,
            revoked_reason: self.revoked_reason.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Debug, Default)]
struct FleetAuthCache {
    fingerprint: Option<FileFingerprint>,
    credentials: Vec<CachedFleetCredential>,
    refresh_count: usize,
}

pub fn default_fleet_scopes() -> Vec<String> {
    vec![
        FLEET_SCOPE_BATCH_READ.to_string(),
        FLEET_SCOPE_CATALOG_READ.to_string(),
        FLEET_SCOPE_CONTENT_READ.to_string(),
        FLEET_SCOPE_DETAIL_READ.to_string(),
        FLEET_SCOPE_EVENTS_READ.to_string(),
        FLEET_SCOPE_LAUNCH.to_string(),
        FLEET_SCOPE_REVOKE.to_string(),
        FLEET_SCOPE_ROTATE.to_string(),
        FLEET_SCOPE_HELLO.to_string(),
        FLEET_SCOPE_PROJECTS_READ.to_string(),
        FLEET_SCOPE_SUMMARY_READ.to_string(),
    ]
}

pub fn fleet_capabilities(scopes: &[String]) -> CapabilitySetWire {
    let host = scopes
        .iter()
        .map(|scope| scope.trim())
        .filter(|scope| !scope.is_empty())
        .map(str::to_string)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    CapabilitySetWire {
        schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
        resource: Vec::new(),
        host,
        protocol: vec!["fleet.v1".to_string()],
    }
}

pub fn negotiate_fleet_protocol_version(
    client_versions: &[u32],
) -> Option<u32> {
    let normalized = normalize_protocol_versions(client_versions);
    normalized
        .into_iter()
        .filter(|version| *version == FLEET_PROTOCOL_VERSION)
        .max()
}

pub fn normalize_protocol_versions(client_versions: &[u32]) -> Vec<u32> {
    let mut versions = BTreeSet::new();
    if client_versions.is_empty() {
        versions.insert(FLEET_PROTOCOL_VERSION);
    } else {
        versions.extend(
            client_versions
                .iter()
                .copied()
                .filter(|version| *version > 0),
        );
    }
    versions.into_iter().collect()
}

pub fn credential_has_scope(
    credential: &FleetCredentialRecordWire,
    scope: &str,
) -> bool {
    credential.scopes.iter().any(|allowed| allowed == scope)
}

pub fn current_unix_time() -> f64 {
    let now = chrono::Utc::now();
    now.timestamp() as f64
        + f64::from(now.timestamp_subsec_micros()) / 1_000_000.0
}

fn normalize_controller(
    mut controller: FleetControllerMetadataWire,
) -> Result<FleetControllerMetadataWire, FleetStoreError> {
    if let Some(controller_id) = controller.controller_id.as_deref() {
        validate_label("controller_id", controller_id)?;
    } else {
        controller.controller_id = Some(generate_prefixed_id("ctrl"));
    }
    if let Some(display_name) = controller.display_name.as_deref() {
        validate_label("display_name", display_name)?;
    }
    if let Some(platform) = controller.platform.as_deref() {
        validate_label("platform", platform)?;
    }
    if let Some(app_version) = controller.app_version.as_deref() {
        validate_label("app_version", app_version)?;
    }
    Ok(controller)
}

fn normalize_scopes_or_default(
    field: &str,
    scopes: &[String],
) -> Result<Vec<String>, FleetStoreError> {
    if scopes.is_empty() {
        return Ok(default_fleet_scopes());
    }
    let mut normalized = BTreeSet::new();
    for scope in scopes {
        let scope = scope.trim();
        if scope.is_empty() {
            return Err(FleetStoreError::Validation(format!(
                "{field} contains an empty scope"
            )));
        }
        if scope.len() > MAX_SCOPE_BYTES {
            return Err(FleetStoreError::Validation(format!(
                "{field} scope exceeds {MAX_SCOPE_BYTES} bytes"
            )));
        }
        if !scope.chars().all(|ch| {
            ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-')
        }) {
            return Err(FleetStoreError::Validation(format!(
                "{field} scope contains unsupported characters"
            )));
        }
        normalized.insert(scope.to_string());
    }
    Ok(normalized.into_iter().collect())
}

fn validate_schema(schema_version: u32) -> Result<(), FleetStoreError> {
    if schema_version == FLEET_API_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(FleetStoreError::Validation(
            "unsupported schema_version".to_string(),
        ))
    }
}

fn validate_timestamp(field: &str, value: f64) -> Result<(), FleetStoreError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(FleetStoreError::Validation(format!(
            "{field} must be a finite non-negative Unix timestamp"
        )))
    }
}

fn validate_label(field: &str, value: &str) -> Result<(), FleetStoreError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(FleetStoreError::Validation(format!(
            "{field} must be non-empty"
        )));
    }
    if value.len() > MAX_LABEL_BYTES {
        return Err(FleetStoreError::Validation(format!(
            "{field} exceeds {MAX_LABEL_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FleetStoreError::Validation(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(())
}

fn ensure_credential_active(
    credential: &StoredFleetCredential,
    now_unix: f64,
) -> Result<(), FleetStoreError> {
    if credential.revoked_at_unix.is_some() {
        return Err(FleetStoreError::CredentialRevoked);
    }
    if credential
        .expires_at_unix
        .is_some_and(|expires_at| expires_at <= now_unix)
    {
        return Err(FleetStoreError::CredentialExpired);
    }
    Ok(())
}

fn find_cached_credential(
    credentials: &[CachedFleetCredential],
    token_hash: &str,
) -> (Option<CachedFleetCredential>, usize) {
    let mut matched = None;
    let mut checked = 0;
    for credential in credentials {
        checked += 1;
        if constant_time_eq(
            credential.token_hash.as_bytes(),
            token_hash.as_bytes(),
        ) && matched.is_none()
        {
            matched = Some(credential.clone());
        }
    }
    (matched, checked)
}

fn generate_secret(prefix: &str, len: usize) -> String {
    let suffix: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect();
    format!("{prefix}_{suffix}")
}

fn hash_secret(domain: &[u8], secret: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(secret.as_bytes());
    hex::encode(hasher.finalize())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

fn ensure_private_dir(path: &Path) -> Result<(), FleetStoreError> {
    fs::create_dir_all(path).map_err(|source| FleetStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    set_private_dir_mode(path)
}

fn open_private_file(path: &Path) -> Result<File, FleetStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(path)
            .map_err(|source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            })?;
        set_private_file_mode(path)?;
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)
            .map_err(|source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            })
    }
}

fn create_private_file(path: &Path) -> Result<File, FleetStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(path)
            .map_err(|source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            })
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(|source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            })
    }
}

fn set_private_dir_mode(path: &Path) -> Result<(), FleetStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
            |source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn set_private_file_mode(path: &Path) -> Result<(), FleetStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(
            |source| FleetStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    fn bootstrap_request(scopes: &[&str]) -> FleetBootstrapIssueRequestWire {
        FleetBootstrapIssueRequestWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            requested_scopes: scopes
                .iter()
                .map(|scope| scope.to_string())
                .collect(),
            supported_protocol_versions: vec![1],
            expires_at_unix: None,
            installation_pin: None,
        }
    }

    fn enroll_request(
        bootstrap: &FleetBootstrapIssueResponseWire,
        scopes: &[&str],
    ) -> FleetEnrollmentRequestWire {
        FleetEnrollmentRequestWire {
            schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
            bootstrap_id: bootstrap.bootstrap_id.clone(),
            bootstrap_secret: bootstrap.bootstrap_secret.clone(),
            controller: FleetControllerMetadataWire {
                schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
                controller_id: Some("controller-a".to_string()),
                display_name: Some("Controller A".to_string()),
                platform: Some("linux".to_string()),
                app_version: Some("1.0.0".to_string()),
            },
            requested_scopes: scopes
                .iter()
                .map(|scope| scope.to_string())
                .collect(),
            supported_protocol_versions: vec![1],
            pinned_installation_id: bootstrap.pinned_installation_id.clone(),
        }
    }

    fn enroll_token(
        store: &FleetCredentialStore,
        bootstrap: &FleetBootstrapIssueResponseWire,
    ) -> (FleetCredentialRecordWire, String) {
        match store.enroll(enroll_request(bootstrap, &[]), 2.0).unwrap() {
            FleetEnrollmentResult::Enrolled(success) => {
                (success.credential, success.token)
            }
            FleetEnrollmentResult::Quarantined(_) => {
                panic!("unexpected quarantine")
            }
        }
    }

    #[test]
    fn bootstrap_and_credential_secrets_are_hashed_at_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FleetCredentialStore::new(tmp.path());
        let bootstrap =
            store.issue_bootstrap(bootstrap_request(&[]), 1.0).unwrap();
        let after_bootstrap =
            fs::read_to_string(store.auth_path()).expect("auth file");
        assert!(!after_bootstrap.contains(&bootstrap.bootstrap_secret));
        assert!(after_bootstrap.contains("secret_hash"));

        let (_credential, token) = enroll_token(&store, &bootstrap);
        let after_enroll = fs::read_to_string(store.auth_path()).unwrap();
        assert!(!after_enroll.contains(&token));
        assert!(after_enroll.contains("token_hash"));
    }

    #[test]
    fn bootstrap_is_single_use_and_expiring() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FleetCredentialStore::new(tmp.path());
        let bootstrap =
            store.issue_bootstrap(bootstrap_request(&[]), 1.0).unwrap();
        enroll_token(&store, &bootstrap);
        assert!(matches!(
            store.enroll(enroll_request(&bootstrap, &[]), 3.0),
            Err(FleetStoreError::BootstrapConsumed)
        ));

        let expired = store
            .issue_bootstrap(
                FleetBootstrapIssueRequestWire {
                    expires_at_unix: Some(5.0),
                    ..bootstrap_request(&[])
                },
                4.0,
            )
            .unwrap();
        assert!(matches!(
            store.enroll(enroll_request(&expired, &[]), 6.0),
            Err(FleetStoreError::BootstrapExpired)
        ));
    }

    #[test]
    fn rotation_and_revocation_reject_old_or_revoked_tokens() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FleetCredentialStore::new(tmp.path());
        let bootstrap =
            store.issue_bootstrap(bootstrap_request(&[]), 1.0).unwrap();
        let (credential, token) = enroll_token(&store, &bootstrap);
        assert!(matches!(
            store.authenticate_token(&token, 3.0).unwrap(),
            FleetAuthentication::Active(_)
        ));

        let rotated = store
            .rotate_credential(&credential.credential_id, 4.0)
            .unwrap();
        assert!(matches!(
            store.authenticate_token(&token, 5.0).unwrap(),
            FleetAuthentication::Missing
        ));
        assert!(matches!(
            store.authenticate_token(&rotated.token, 5.0).unwrap(),
            FleetAuthentication::Active(_)
        ));

        store
            .revoke_credential(
                &credential.credential_id,
                FleetCredentialRevokeRequestWire {
                    schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
                    reason: Some("retired".to_string()),
                },
                6.0,
            )
            .unwrap();
        assert!(matches!(
            store.authenticate_token(&rotated.token, 7.0).unwrap(),
            FleetAuthentication::Revoked(_)
        ));
    }

    #[test]
    fn authentication_cache_refreshes_after_durable_changes_only() {
        let tmp = tempfile::tempdir().unwrap();
        let store = FleetCredentialStore::new(tmp.path());
        let bootstrap =
            store.issue_bootstrap(bootstrap_request(&[]), 1.0).unwrap();
        let (credential, token) = enroll_token(&store, &bootstrap);
        let before_auth = store.cache_refresh_count();
        assert!(matches!(
            store.authenticate_token(&token, 3.0).unwrap(),
            FleetAuthentication::Active(_)
        ));
        assert_eq!(store.cache_refresh_count(), before_auth);
        let metadata_before =
            fs::metadata(store.auth_path()).unwrap().modified().ok();
        assert!(matches!(
            store.authenticate_token(&token, 3.5).unwrap(),
            FleetAuthentication::Active(_)
        ));
        assert_eq!(
            fs::metadata(store.auth_path()).unwrap().modified().ok(),
            metadata_before
        );

        let second_process_view = FleetCredentialStore::new(tmp.path());
        second_process_view
            .revoke_credential(
                &credential.credential_id,
                FleetCredentialRevokeRequestWire {
                    schema_version: FLEET_API_WIRE_SCHEMA_VERSION,
                    reason: Some("external revoke".to_string()),
                },
                4.0,
            )
            .unwrap();
        assert!(matches!(
            store.authenticate_token(&token, 5.0).unwrap(),
            FleetAuthentication::Revoked(_)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn auth_store_files_use_private_modes() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let store = FleetCredentialStore::new(tmp.path());
        store.issue_bootstrap(bootstrap_request(&[]), 1.0).unwrap();
        let dir_mode = fs::metadata(store.state_dir())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        let file_mode = fs::metadata(store.auth_path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);
        assert_eq!(file_mode, 0o600);
    }

    #[test]
    fn credential_lookup_checks_every_cached_record() {
        let records = vec![
            CachedFleetCredential {
                credential_id: "first".to_string(),
                controller: FleetControllerMetadataWire {
                    schema_version: 1,
                    controller_id: Some("first".to_string()),
                    display_name: None,
                    platform: None,
                    app_version: None,
                },
                token_hash: "a".repeat(64),
                scopes: default_fleet_scopes(),
                issued_at_unix: 1.0,
                expires_at_unix: None,
                rotated_at_unix: None,
                revoked_at_unix: None,
                revoked_reason: None,
            },
            CachedFleetCredential {
                credential_id: "last".to_string(),
                controller: FleetControllerMetadataWire {
                    schema_version: 1,
                    controller_id: Some("last".to_string()),
                    display_name: None,
                    platform: None,
                    app_version: None,
                },
                token_hash: "b".repeat(64),
                scopes: default_fleet_scopes(),
                issued_at_unix: 1.0,
                expires_at_unix: None,
                rotated_at_unix: None,
                revoked_at_unix: None,
                revoked_reason: None,
            },
        ];

        let (first, first_count) =
            find_cached_credential(&records, &"a".repeat(64));
        let (last, last_count) =
            find_cached_credential(&records, &"b".repeat(64));
        let (missing, missing_count) =
            find_cached_credential(&records, &"c".repeat(64));
        assert_eq!(first.unwrap().credential_id, "first");
        assert_eq!(last.unwrap().credential_id, "last");
        assert!(missing.is_none());
        assert_eq!(first_count, records.len());
        assert_eq!(last_count, records.len());
        assert_eq!(missing_count, records.len());
    }

    #[test]
    fn concurrent_store_instances_preserve_each_others_updates() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().to_path_buf();
        let handles: Vec<_> = (0..4)
            .map(|index| {
                let home = home.clone();
                thread::spawn(move || {
                    let store = FleetCredentialStore::new(home);
                    let mut request = bootstrap_request(&[]);
                    request.installation_pin = None;
                    let bootstrap = store
                        .issue_bootstrap(request, 10.0 + f64::from(index))
                        .unwrap();
                    bootstrap.bootstrap_id
                })
            })
            .collect();
        let ids: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        let store = FleetCredentialStore::new(tmp.path());
        let file = store.read_auth_file_unlocked().unwrap();
        assert_eq!(file.bootstraps.len(), ids.len());
        for id in ids {
            assert!(file
                .bootstraps
                .iter()
                .any(|record| record.bootstrap_id == id));
        }
    }
}

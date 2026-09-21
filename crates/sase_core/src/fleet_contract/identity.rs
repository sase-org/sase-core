use super::error::FleetContractError;
use super::error::FLEET_CONTRACT_SCHEMA_VERSION;
use super::error::FLEET_INSTALLATION_IDENTITY_FILENAME;
use super::error::FLEET_INSTALLATION_IDENTITY_MAX_BYTES;
use super::error::FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION;
use super::error::FLEET_INSTALLATION_ID_PREFIX;
use super::error::LOCK_TIMEOUT_DEFAULT;
use super::error::LOCK_TIMEOUT_ENV;
use super::error::MAX_LABEL_BYTES;
use super::error::STALE_TEMP_MAX_AGE;
use super::validation::validate_installation_id;
use super::validation::validate_label;
use super::validation::validate_schema;
use super::validation::validate_timestamp;
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

/// Persisted per-user origin identity.
///
/// `installation_id` is opaque identity. It is intentionally unrelated to
/// local network identity, configured machine name, provider reference,
/// endpoint, display alias, username, or any filesystem location.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRecordWire {
    pub schema_version: u32,
    pub installation_id: String,
    pub created_at_unix: f64,
    pub generation: u64,
    pub prior_installation_id: Option<String>,
    pub rotated_at_unix: Option<f64>,
    pub adopted_at_unix: Option<f64>,
    pub reason: Option<String>,
}

/// Outcome of ensuring the persisted installation identity exists.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityEnsureOutcomeWire {
    pub schema_version: u32,
    pub record: InstallationIdentityRecordWire,
    pub created: bool,
    pub path: String,
}

/// Read-only load result for callers that must not create a missing identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityLoadOutcomeWire {
    pub schema_version: u32,
    pub record: Option<InstallationIdentityRecordWire>,
    pub path: String,
}

/// Fenced request to rotate a known current installation identity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRotateRequestWire {
    pub schema_version: u32,
    pub expected_installation_id: String,
    pub reason: String,
    pub rotated_at_unix: Option<f64>,
}

/// Result of a successful explicit identity rotation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityRotateOutcomeWire {
    pub schema_version: u32,
    pub old_record: InstallationIdentityRecordWire,
    pub new_record: InstallationIdentityRecordWire,
    pub path: String,
}

/// Fenced request to adopt an existing identity during reinstall recovery.
///
/// `expected_current_installation_id = null` means the caller expects the
/// store to be missing. A non-null value means the caller expects to replace
/// exactly that current identity. Ordinary ensure/load calls never adopt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityMigrateRequestWire {
    pub schema_version: u32,
    pub expected_current_installation_id: Option<String>,
    pub adopted_installation_id: String,
    pub reason: String,
    pub adopted_at_unix: Option<f64>,
}

/// Result of a successful explicit identity adoption.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationIdentityMigrateOutcomeWire {
    pub schema_version: u32,
    pub prior_record: Option<InstallationIdentityRecordWire>,
    pub new_record: InstallationIdentityRecordWire,
    pub path: String,
}

enum LoadedIdentity {
    Missing,
    Valid(InstallationIdentityRecordWire),
    Unusable { message: String },
}

enum WritePrecondition<'a> {
    Missing,
    Current(&'a str),
}

pub fn installation_identity_path(sase_home: &Path) -> PathBuf {
    sase_home.join(FLEET_INSTALLATION_IDENTITY_FILENAME)
}

pub fn load_installation_identity(
    sase_home: &Path,
) -> Result<InstallationIdentityLoadOutcomeWire, FleetContractError> {
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_load", || {
        let loaded = load_identity_unlocked(&path)?;
        match loaded {
            LoadedIdentity::Missing => {
                Ok(InstallationIdentityLoadOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record: None,
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Valid(record) => {
                Ok(InstallationIdentityLoadOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record: Some(record),
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Unusable { message } => {
                Err(mutation_blocked(&path, &message))
            }
        }
    })
}

pub fn ensure_installation_identity(
    sase_home: &Path,
) -> Result<InstallationIdentityEnsureOutcomeWire, FleetContractError> {
    ensure_installation_identity_with_generator(
        sase_home,
        current_unix_time()?,
        generate_installation_id,
    )
}

pub fn ensure_installation_identity_with_generator(
    sase_home: &Path,
    now_unix: f64,
    mut generator: impl FnMut() -> String,
) -> Result<InstallationIdentityEnsureOutcomeWire, FleetContractError> {
    validate_timestamp("created_at_unix", now_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_ensure", || {
        match load_identity_unlocked(&path)? {
            LoadedIdentity::Valid(record) => {
                Ok(InstallationIdentityEnsureOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record,
                    created: false,
                    path: path.display().to_string(),
                })
            }
            LoadedIdentity::Unusable { message } => {
                Err(mutation_blocked(&path, &message))
            }
            LoadedIdentity::Missing => {
                let installation_id = generator();
                validate_installation_id(&installation_id)?;
                let record = InstallationIdentityRecordWire {
                    schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
                    installation_id,
                    created_at_unix: now_unix,
                    generation: 1,
                    prior_installation_id: None,
                    rotated_at_unix: None,
                    adopted_at_unix: None,
                    reason: None,
                };
                validate_installation_record(&record)?;
                write_identity_atomic(
                    &path,
                    &record,
                    WritePrecondition::Missing,
                )?;
                Ok(InstallationIdentityEnsureOutcomeWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    record,
                    created: true,
                    path: path.display().to_string(),
                })
            }
        }
    })
}

pub fn rotate_installation_identity(
    sase_home: &Path,
    request: &InstallationIdentityRotateRequestWire,
) -> Result<InstallationIdentityRotateOutcomeWire, FleetContractError> {
    rotate_installation_identity_with_generator(
        sase_home,
        request,
        generate_installation_id,
    )
}

pub fn rotate_installation_identity_with_generator(
    sase_home: &Path,
    request: &InstallationIdentityRotateRequestWire,
    mut generator: impl FnMut() -> String,
) -> Result<InstallationIdentityRotateOutcomeWire, FleetContractError> {
    validate_schema(
        "installation identity rotate request",
        request.schema_version,
    )?;
    validate_installation_id(&request.expected_installation_id)?;
    validate_label("rotation reason", &request.reason, MAX_LABEL_BYTES)?;
    let rotated_at_unix =
        request.rotated_at_unix.map_or_else(current_unix_time, Ok)?;
    validate_timestamp("rotated_at_unix", rotated_at_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_rotate", || {
        let old_record = match load_identity_unlocked(&path)? {
            LoadedIdentity::Valid(record) => record,
            LoadedIdentity::Missing => {
                return Err(FleetContractError::Validation(format!(
                    "cannot rotate missing installation identity at {}",
                    path.display()
                )))
            }
            LoadedIdentity::Unusable { message } => {
                return Err(mutation_blocked(&path, &message))
            }
        };
        if old_record.installation_id != request.expected_installation_id {
            return Err(FleetContractError::Validation(
                "expected_installation_id does not match the current identity"
                    .to_string(),
            ));
        }
        let installation_id = generator();
        validate_installation_id(&installation_id)?;
        if installation_id == old_record.installation_id {
            return Err(FleetContractError::Validation(
                "rotated installation identity must be distinct".to_string(),
            ));
        }
        let new_record = InstallationIdentityRecordWire {
            schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
            installation_id,
            created_at_unix: rotated_at_unix,
            generation: old_record.generation.saturating_add(1),
            prior_installation_id: Some(old_record.installation_id.clone()),
            rotated_at_unix: Some(rotated_at_unix),
            adopted_at_unix: None,
            reason: Some(request.reason.trim().to_string()),
        };
        validate_installation_record(&new_record)?;
        write_identity_atomic(
            &path,
            &new_record,
            WritePrecondition::Current(&old_record.installation_id),
        )?;
        Ok(InstallationIdentityRotateOutcomeWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            old_record,
            new_record,
            path: path.display().to_string(),
        })
    })
}

pub fn migrate_installation_identity(
    sase_home: &Path,
    request: &InstallationIdentityMigrateRequestWire,
) -> Result<InstallationIdentityMigrateOutcomeWire, FleetContractError> {
    validate_schema(
        "installation identity migrate request",
        request.schema_version,
    )?;
    if let Some(expected) = &request.expected_current_installation_id {
        validate_installation_id(expected)?;
    }
    validate_installation_id(&request.adopted_installation_id)?;
    validate_label("migration reason", &request.reason, MAX_LABEL_BYTES)?;
    let adopted_at_unix =
        request.adopted_at_unix.map_or_else(current_unix_time, Ok)?;
    validate_timestamp("adopted_at_unix", adopted_at_unix)?;
    let path = installation_identity_path(sase_home);
    with_identity_lock(sase_home, "fleet_identity_migrate", || {
        let loaded = load_identity_unlocked(&path)?;
        let (prior_record, precondition, generation) = match loaded {
            LoadedIdentity::Missing => {
                if request.expected_current_installation_id.is_some() {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id was supplied but the identity store is missing"
                            .to_string(),
                    ));
                }
                (None, WritePrecondition::Missing, 1)
            }
            LoadedIdentity::Valid(record) => {
                let Some(expected) = &request.expected_current_installation_id
                else {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id is required when replacing an existing identity"
                            .to_string(),
                    ));
                };
                if expected != &record.installation_id {
                    return Err(FleetContractError::Validation(
                        "expected_current_installation_id does not match the current identity"
                            .to_string(),
                    ));
                }
                let generation = record.generation.saturating_add(1);
                (
                    Some(record),
                    WritePrecondition::Current(expected.as_str()),
                    generation,
                )
            }
            LoadedIdentity::Unusable { message } => {
                return Err(mutation_blocked(&path, &message))
            }
        };
        if prior_record.as_ref().is_some_and(|record| {
            record.installation_id == request.adopted_installation_id
        }) {
            return Err(FleetContractError::Validation(
                "adopted identity must differ from the current identity"
                    .to_string(),
            ));
        }
        let new_record = InstallationIdentityRecordWire {
            schema_version: FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION,
            installation_id: request.adopted_installation_id.clone(),
            created_at_unix: adopted_at_unix,
            generation,
            prior_installation_id: prior_record
                .as_ref()
                .map(|record| record.installation_id.clone()),
            rotated_at_unix: None,
            adopted_at_unix: Some(adopted_at_unix),
            reason: Some(request.reason.trim().to_string()),
        };
        validate_installation_record(&new_record)?;
        write_identity_atomic(&path, &new_record, precondition)?;
        Ok(InstallationIdentityMigrateOutcomeWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            prior_record,
            new_record,
            path: path.display().to_string(),
        })
    })
}

fn validate_installation_record(
    record: &InstallationIdentityRecordWire,
) -> Result<(), FleetContractError> {
    if record.schema_version != FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION {
        return Err(FleetContractError::Validation(format!(
            "installation identity version {} is not supported (expected {})",
            record.schema_version, FLEET_INSTALLATION_IDENTITY_SCHEMA_VERSION
        )));
    }
    validate_installation_id(&record.installation_id)?;
    validate_timestamp("created_at_unix", record.created_at_unix)?;
    if record.generation == 0 {
        return Err(FleetContractError::Validation(
            "installation identity generation must be positive".to_string(),
        ));
    }
    if let Some(prior) = &record.prior_installation_id {
        validate_installation_id(prior)?;
        if prior == &record.installation_id {
            return Err(FleetContractError::Validation(
                "prior_installation_id must differ from installation_id"
                    .to_string(),
            ));
        }
    }
    if let Some(rotated_at) = record.rotated_at_unix {
        validate_timestamp("rotated_at_unix", rotated_at)?;
    }
    if let Some(adopted_at) = record.adopted_at_unix {
        validate_timestamp("adopted_at_unix", adopted_at)?;
    }
    if let Some(reason) = &record.reason {
        validate_label("identity reason", reason, MAX_LABEL_BYTES)?;
    }
    Ok(())
}

fn load_identity_unlocked(
    path: &Path,
) -> Result<LoadedIdentity, FleetContractError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(LoadedIdentity::Missing)
        }
        Err(error) => return Err(io_error(path, error)),
    };
    if bytes.len() > FLEET_INSTALLATION_IDENTITY_MAX_BYTES {
        return Ok(LoadedIdentity::Unusable {
            message: format!(
                "identity file exceeds {FLEET_INSTALLATION_IDENTITY_MAX_BYTES} bytes"
            ),
        });
    }
    let record: InstallationIdentityRecordWire =
        match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(error) => {
                return Ok(LoadedIdentity::Unusable {
                    message: format!(
                        "identity file is not valid JSON: {error}"
                    ),
                })
            }
        };
    match validate_installation_record(&record) {
        Ok(()) => Ok(LoadedIdentity::Valid(record)),
        Err(error) => Ok(LoadedIdentity::Unusable {
            message: error.to_string(),
        }),
    }
}

fn write_identity_atomic(
    path: &Path,
    record: &InstallationIdentityRecordWire,
    precondition: WritePrecondition<'_>,
) -> Result<(), FleetContractError> {
    validate_installation_record(record)?;
    let parent = path.parent().ok_or_else(|| {
        FleetContractError::Validation(format!(
            "installation identity path has no parent: {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
    restrict_dir(parent)?;
    reap_stale_temp_siblings(path, SystemTime::now());
    check_write_precondition(path, &precondition)?;
    let mut bytes = serde_json::to_vec_pretty(record).map_err(|source| {
        FleetContractError::Json {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    if bytes.len() > FLEET_INSTALLATION_IDENTITY_MAX_BYTES {
        return Err(FleetContractError::Validation(format!(
            "serialized installation identity exceeds {FLEET_INSTALLATION_IDENTITY_MAX_BYTES} bytes"
        )));
    }
    let tmp_path = temp_path_for(path);
    let write_result = (|| -> Result<(), FleetContractError> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let handle = options
            .open(&tmp_path)
            .map_err(|error| io_error(&tmp_path, error))?;
        let mut writer = BufWriter::new(handle);
        writer
            .write_all(&bytes)
            .map_err(|error| io_error(&tmp_path, error))?;
        writer.flush().map_err(|error| io_error(&tmp_path, error))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| io_error(&tmp_path, error))?;
        check_write_precondition(path, &precondition)?;
        fs::rename(&tmp_path, path).map_err(|error| io_error(path, error))?;
        restrict_file(path)?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    write_result
}

fn check_write_precondition(
    path: &Path,
    precondition: &WritePrecondition<'_>,
) -> Result<(), FleetContractError> {
    match precondition {
        WritePrecondition::Missing => {
            if path.exists() {
                return Err(FleetContractError::Validation(format!(
                    "identity store at {} was created concurrently and was left unchanged",
                    path.display()
                )));
            }
            Ok(())
        }
        WritePrecondition::Current(expected) => {
            match load_identity_unlocked(path)? {
                LoadedIdentity::Valid(record)
                    if record.installation_id == *expected =>
                {
                    Ok(())
                }
                LoadedIdentity::Valid(_) => {
                    Err(FleetContractError::Validation(
                        "identity store changed before atomic replacement"
                            .to_string(),
                    ))
                }
                LoadedIdentity::Missing => Err(FleetContractError::Validation(
                    "identity store disappeared before atomic replacement"
                        .to_string(),
                )),
                LoadedIdentity::Unusable { message } => {
                    Err(mutation_blocked(path, &message))
                }
            }
        }
    }
}

fn with_identity_lock<T>(
    sase_home: &Path,
    operation_name: &str,
    operation: impl FnOnce() -> Result<T, FleetContractError>,
) -> Result<T, FleetContractError> {
    fs::create_dir_all(sase_home)
        .map_err(|error| io_error(sase_home, error))?;
    restrict_dir(sase_home)?;
    let path = installation_identity_path(sase_home);
    let lock_path = lock_path_for(&path);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        operation_name,
    )
    .map_err(|error| lock_error_to_fleet(error, &lock_path))?;
    let result = operation();
    let unlock = unlock(lock, &lock_path);
    match (result, unlock) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

fn lock_error_to_fleet(
    error: StoreLockError,
    path: &Path,
) -> FleetContractError {
    match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => FleetContractError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        StoreLockError::Open { source, .. }
        | StoreLockError::Acquire { source, .. } => io_error(path, source),
    }
}

fn unlock(lock: HeldStoreLock, path: &Path) -> Result<(), FleetContractError> {
    lock.release().map_err(|error| io_error(path, error))
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn reap_stale_temp_siblings(path: &Path, now: SystemTime) {
    let Some(parent) = path.parent() else {
        return;
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FLEET_INSTALLATION_IDENTITY_FILENAME);
    let prefix = format!(".{filename}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&prefix)
            || !name.ends_with(".tmp")
            || name.len() <= prefix.len() + ".tmp".len()
        {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age > STALE_TEMP_MAX_AGE {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn generate_installation_id() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    format!("{FLEET_INSTALLATION_ID_PREFIX}{}", hex::encode(bytes))
}

fn current_unix_time() -> Result<f64, FleetContractError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .map_err(|error| {
            FleetContractError::Validation(format!(
                "could not read current Unix timestamp: {error}"
            ))
        })
}

fn restrict_dir(path: &Path) -> Result<(), FleetContractError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), FleetContractError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|error| io_error(path, error))?;
    }
    Ok(())
}

fn mutation_blocked(path: &Path, reason: &str) -> FleetContractError {
    FleetContractError::Validation(format!(
        "cannot update installation identity at {}: {reason}. The file was left unchanged; move or repair it, then retry.",
        path.display()
    ))
}

fn io_error(path: &Path, source: io::Error) -> FleetContractError {
    FleetContractError::Io {
        path: path.to_path_buf(),
        source,
    }
}

pub(crate) fn canonical_json_into(
    value: &Value,
    out: &mut Sha256,
) -> Result<(), FleetContractError> {
    match value {
        Value::Null => out.update(b"null"),
        Value::Bool(true) => out.update(b"true"),
        Value::Bool(false) => out.update(b"false"),
        Value::Number(number) => out.update(number.to_string().as_bytes()),
        Value::String(text) => {
            let encoded = serde_json::to_string(text).map_err(|source| {
                FleetContractError::Json {
                    path: PathBuf::from("<payload>"),
                    source,
                }
            })?;
            out.update(encoded.as_bytes());
        }
        Value::Array(values) => {
            out.update(b"[");
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    out.update(b",");
                }
                canonical_json_into(item, out)?;
            }
            out.update(b"]");
        }
        Value::Object(map) => {
            out.update(b"{");
            let mut sorted = BTreeMap::new();
            for (key, item) in map {
                sorted.insert(key, item);
            }
            for (index, (key, item)) in sorted.iter().enumerate() {
                if index > 0 {
                    out.update(b",");
                }
                let encoded_key =
                    serde_json::to_string(key).map_err(|source| {
                        FleetContractError::Json {
                            path: PathBuf::from("<payload>"),
                            source,
                        }
                    })?;
                out.update(encoded_key.as_bytes());
                out.update(b":");
                canonical_json_into(item, out)?;
            }
            out.update(b"}");
        }
    }
    Ok(())
}

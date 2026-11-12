//! Machine-local digest acknowledgments for project-local commands.
//!
//! The host resolves checkout identity and supplies it here. This module owns
//! validation, locked read/modify/write semantics, and the private atomic
//! store under `sase_home/local_config_trust/`.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

use super::trust::LOCAL_COMMAND_TRUST_POLICY_VERSION;

/// Wire schema version for local-command trust-store operations.
pub const LOCAL_COMMAND_TRUST_STORE_WIRE_SCHEMA_VERSION: u32 = 1;
/// On-disk store version.
pub const LOCAL_COMMAND_TRUST_STORE_VERSION: u32 = 1;
/// Store filename under `sase_home/local_config_trust/`.
pub const LOCAL_COMMAND_TRUST_STORE_FILENAME: &str = "records.json";
/// Maximum accepted serialized store size.
pub const LOCAL_COMMAND_TRUST_STORE_MAX_BYTES: usize = 1024 * 1024;

const TRUST_DIRNAME: &str = "local_config_trust";
const LOCK_TIMEOUT_ENV: &str = "SASE_LOCAL_COMMAND_TRUST_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCommandTrustRecordWire {
    pub identity: String,
    pub policy_version: u32,
    pub digest: String,
    pub acknowledged_at: String,
    pub acknowledged_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LocalCommandTrustStoreFileWire {
    version: u32,
    records: BTreeMap<String, LocalCommandTrustRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCommandTrustStoreSnapshotWire {
    pub schema_version: u32,
    pub store_version: u32,
    pub path: String,
    pub status: String,
    pub reason: Option<String>,
    pub record: Option<LocalCommandTrustRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalCommandTrustStoreMutationWire {
    pub schema_version: u32,
    pub store_version: u32,
    pub path: String,
    pub changed: bool,
    pub record: Option<LocalCommandTrustRecordWire>,
}

#[derive(Debug, Error)]
pub enum LocalCommandTrustStoreError {
    #[error("invalid local-command trust store request at {}: {message}", path.display())]
    Invalid { path: PathBuf, message: String },
    #[error("local-command trust store lock failed: {message}")]
    Lock { message: String, timeout: bool },
    #[error("local-command trust store I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

enum LoadedStore {
    Missing,
    Valid(BTreeMap<String, LocalCommandTrustRecordWire>),
    Unusable(String),
}

/// Return the fixed trust-store path for the supplied SASE home.
pub fn local_command_trust_store_path(sase_home: &Path) -> PathBuf {
    sase_home
        .join(TRUST_DIRNAME)
        .join(LOCAL_COMMAND_TRUST_STORE_FILENAME)
}

/// Read one project record without creating a directory, lock, or store file.
pub fn local_command_trust_store_get(
    sase_home: &Path,
    identity: &str,
) -> Result<LocalCommandTrustStoreSnapshotWire, LocalCommandTrustStoreError> {
    let path = local_command_trust_store_path(sase_home);
    validate_identity(&path, identity)?;
    let loaded = load_store(&path)?;
    let (status, reason, record) = match loaded {
        LoadedStore::Missing => ("available", None, None),
        LoadedStore::Valid(records) => {
            ("available", None, records.get(identity).cloned())
        }
        LoadedStore::Unusable(reason) => ("untrusted", Some(reason), None),
    };
    Ok(LocalCommandTrustStoreSnapshotWire {
        schema_version: LOCAL_COMMAND_TRUST_STORE_WIRE_SCHEMA_VERSION,
        store_version: LOCAL_COMMAND_TRUST_STORE_VERSION,
        path: path.display().to_string(),
        status: status.to_string(),
        reason,
        record,
    })
}

/// Replace the current project acknowledgment in one locked transaction.
pub fn local_command_trust_store_allow(
    sase_home: &Path,
    identity: &str,
    digest: &str,
    policy_version: u32,
    acknowledged_by: &str,
) -> Result<LocalCommandTrustStoreMutationWire, LocalCommandTrustStoreError> {
    let path = local_command_trust_store_path(sase_home);
    validate_identity(&path, identity)?;
    validate_digest(&path, digest)?;
    if policy_version != LOCAL_COMMAND_TRUST_POLICY_VERSION {
        return Err(invalid(
            &path,
            format!(
                "unsupported policy version {policy_version} (expected {})",
                LOCAL_COMMAND_TRUST_POLICY_VERSION
            ),
        ));
    }
    if acknowledged_by.trim().is_empty() || acknowledged_by.len() > 256 {
        return Err(invalid(
            &path,
            "acknowledgment identity is empty or too long",
        ));
    }

    let lock = lock_store(&path, "local_command_trust_allow")?;
    let result = (|| {
        let mut records = load_for_mutation(&path)?;
        let record = LocalCommandTrustRecordWire {
            identity: identity.to_string(),
            policy_version,
            digest: digest.to_string(),
            acknowledged_at: Utc::now()
                .to_rfc3339_opts(SecondsFormat::Millis, true),
            acknowledged_by: acknowledged_by.trim().to_string(),
        };
        // Timestamp changes on every invocation, so idempotence is decided by
        // the semantic acknowledgment fields rather than the new timestamp.
        let changed = records.get(identity).is_none_or(|previous| {
            previous.policy_version != record.policy_version
                || previous.digest != record.digest
        });
        if changed {
            records.insert(identity.to_string(), record.clone());
            write_store_atomic(&path, &records)?;
        }
        Ok(LocalCommandTrustStoreMutationWire {
            schema_version: LOCAL_COMMAND_TRUST_STORE_WIRE_SCHEMA_VERSION,
            store_version: LOCAL_COMMAND_TRUST_STORE_VERSION,
            path: path.display().to_string(),
            changed,
            record: records.get(identity).cloned(),
        })
    })();
    release_lock(lock, &path)?;
    result
}

/// Remove one project acknowledgment in a locked transaction.
pub fn local_command_trust_store_revoke(
    sase_home: &Path,
    identity: &str,
) -> Result<LocalCommandTrustStoreMutationWire, LocalCommandTrustStoreError> {
    let path = local_command_trust_store_path(sase_home);
    validate_identity(&path, identity)?;
    let lock = lock_store(&path, "local_command_trust_revoke")?;
    let result = (|| {
        let mut records = load_for_mutation(&path)?;
        let changed = records.remove(identity).is_some();
        if changed {
            write_store_atomic(&path, &records)?;
        }
        Ok(LocalCommandTrustStoreMutationWire {
            schema_version: LOCAL_COMMAND_TRUST_STORE_WIRE_SCHEMA_VERSION,
            store_version: LOCAL_COMMAND_TRUST_STORE_VERSION,
            path: path.display().to_string(),
            changed,
            record: None,
        })
    })();
    release_lock(lock, &path)?;
    result
}

fn validate_identity(
    path: &Path,
    identity: &str,
) -> Result<(), LocalCommandTrustStoreError> {
    if identity.trim().is_empty()
        || identity.len() > 4096
        || identity.chars().any(char::is_control)
        || !(identity.starts_with("project:v1:")
            || identity.starts_with("checkout:v1:"))
    {
        return Err(invalid(
            path,
            "project identity must be versioned and contain no controls",
        ));
    }
    Ok(())
}

fn validate_digest(
    path: &Path,
    digest: &str,
) -> Result<(), LocalCommandTrustStoreError> {
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(
            path,
            "digest must be a lowercase SHA-256 hex string",
        ));
    }
    Ok(())
}

fn load_for_mutation(
    path: &Path,
) -> Result<
    BTreeMap<String, LocalCommandTrustRecordWire>,
    LocalCommandTrustStoreError,
> {
    match load_store(path)? {
        LoadedStore::Missing => Ok(BTreeMap::new()),
        LoadedStore::Valid(records) => Ok(records),
        LoadedStore::Unusable(reason) => Err(invalid(path, reason)),
    }
}

fn load_store(path: &Path) -> Result<LoadedStore, LocalCommandTrustStoreError> {
    if let Err(error) = ensure_not_symlinked_directory(path) {
        if let LocalCommandTrustStoreError::Invalid { message, .. } = error {
            return Ok(LoadedStore::Unusable(message));
        }
        return Err(error);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(LoadedStore::Missing)
        }
        Err(error) => return Err(io_error(path, error)),
    };
    if metadata.file_type().is_symlink() {
        return Ok(LoadedStore::Unusable(
            "trust store file is a symlink".to_string(),
        ));
    }
    if !metadata.is_file() {
        return Ok(LoadedStore::Unusable(
            "trust store path is not a regular file".to_string(),
        ));
    }
    if !is_private_file(&metadata) {
        return Ok(LoadedStore::Unusable(
            "trust store file permissions are not private".to_string(),
        ));
    }
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Ok(LoadedStore::Unusable(format!(
                "trust store is unreadable: {error}"
            )));
        }
    };
    if bytes.len() > LOCAL_COMMAND_TRUST_STORE_MAX_BYTES {
        return Ok(LoadedStore::Unusable(format!(
            "trust store exceeds {LOCAL_COMMAND_TRUST_STORE_MAX_BYTES} bytes"
        )));
    }
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(_) => {
            return Ok(LoadedStore::Unusable(
                "trust store is not valid UTF-8".to_string(),
            ))
        }
    };
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => {
            return Ok(LoadedStore::Unusable(
                "trust store is not valid JSON".to_string(),
            ))
        }
    };
    let Some(object) = value.as_object() else {
        return Ok(LoadedStore::Unusable(
            "trust store must be a JSON object".to_string(),
        ));
    };
    match object.get("version").and_then(Value::as_u64) {
        Some(version)
            if version == u64::from(LOCAL_COMMAND_TRUST_STORE_VERSION) => {}
        Some(version) => {
            return Ok(LoadedStore::Unusable(format!(
                "trust store version {version} is unsupported (expected {LOCAL_COMMAND_TRUST_STORE_VERSION})"
            )));
        }
        None => {
            return Ok(LoadedStore::Unusable(
                "trust store version is invalid".to_string(),
            ))
        }
    }
    let file: LocalCommandTrustStoreFileWire =
        match serde_json::from_value(value) {
            Ok(file) => file,
            Err(_) => {
                return Ok(LoadedStore::Unusable(
                    "trust store has an invalid schema".to_string(),
                ))
            }
        };
    for (key, record) in &file.records {
        if key != &record.identity
            || validate_identity(path, key).is_err()
            || validate_digest(path, &record.digest).is_err()
            || record.policy_version != LOCAL_COMMAND_TRUST_POLICY_VERSION
            || DateTime::parse_from_rfc3339(&record.acknowledged_at).is_err()
            || record.acknowledged_by.trim().is_empty()
            || record.acknowledged_by.len() > 256
            || record.acknowledged_by.chars().any(char::is_control)
        {
            return Ok(LoadedStore::Unusable(
                "trust store contains an invalid project record".to_string(),
            ));
        }
    }
    Ok(LoadedStore::Valid(file.records))
}

fn ensure_not_symlinked_directory(
    path: &Path,
) -> Result<(), LocalCommandTrustStoreError> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    let Some(trust_dir) = parent.file_name() else {
        return Ok(());
    };
    if trust_dir != TRUST_DIRNAME {
        return Err(invalid(
            path,
            "trust store path does not use the fixed directory",
        ));
    }
    match fs::symlink_metadata(parent) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(invalid(path, "trust store directory is a symlink"));
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(invalid(
                path,
                "trust store directory is not a directory",
            ));
        }
        Ok(metadata) if !is_private_dir(&metadata) => {
            return Err(invalid(
                path,
                "trust store directory permissions are not private",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => return Err(io_error(parent, error)),
    }
    Ok(())
}

fn write_store_atomic(
    path: &Path,
    records: &BTreeMap<String, LocalCommandTrustRecordWire>,
) -> Result<(), LocalCommandTrustStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid(path, "trust store path has no parent"))?;
    ensure_private_dir(parent, path)?;
    // Recheck immediately before replacing so a symlink planted after the
    // read is never mistaken for an ordinary record.
    ensure_not_symlinked_directory(path)?;
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if metadata.file_type().is_symlink() {
            return Err(invalid(path, "trust store file is a symlink"));
        }
        if !metadata.is_file() {
            return Err(invalid(
                path,
                "trust store path is not a regular file",
            ));
        }
    }
    let file = LocalCommandTrustStoreFileWire {
        version: LOCAL_COMMAND_TRUST_STORE_VERSION,
        records: records.clone(),
    };
    let mut bytes = serde_json::to_vec_pretty(&file).map_err(|error| {
        invalid(path, format!("failed to serialize trust store: {error}"))
    })?;
    bytes.push(b'\n');
    if bytes.len() > LOCAL_COMMAND_TRUST_STORE_MAX_BYTES {
        return Err(invalid(path, "serialized trust store exceeds size limit"));
    }
    let tmp_path = temp_path_for(path);
    let result = (|| {
        let handle = open_private_new(&tmp_path)
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
        fs::rename(&tmp_path, path).map_err(|error| io_error(path, error))?;
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

fn lock_store(
    path: &Path,
    operation: &str,
) -> Result<HeldStoreLock, LocalCommandTrustStoreError> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid(path, "trust store path has no parent"))?;
    ensure_private_dir(parent, path)?;
    let lock_path = lock_path_for(path);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        operation,
    )
    .map_err(|error| LocalCommandTrustStoreError::Lock {
        timeout: matches!(error, StoreLockError::Timeout { .. }),
        message: error.to_string(),
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&lock_path, fs::Permissions::from_mode(0o600))
            .map_err(|error| io_error(&lock_path, error))?;
    }
    Ok(lock)
}

fn release_lock(
    lock: HeldStoreLock,
    path: &Path,
) -> Result<(), LocalCommandTrustStoreError> {
    lock.release().map_err(|error| io_error(path, error))
}

fn ensure_private_dir(
    parent: &Path,
    path: &Path,
) -> Result<(), LocalCommandTrustStoreError> {
    match fs::symlink_metadata(parent) {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {
            if let Some(parent_parent) = parent.parent() {
                fs::create_dir_all(parent_parent)
                    .map_err(|error| io_error(parent_parent, error))?;
            }
            match create_private_dir(parent) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
                Err(error) => return Err(io_error(parent, error)),
            }
        }
        Err(error) => return Err(io_error(parent, error)),
    }
    let metadata = fs::symlink_metadata(parent)
        .map_err(|error| io_error(parent, error))?;
    if metadata.file_type().is_symlink() {
        return Err(invalid(path, "trust store directory is a symlink"));
    }
    if !metadata.is_dir() {
        return Err(invalid(path, "trust store directory is not a directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(parent, error))?;
    }
    Ok(())
}

#[cfg(unix)]
fn is_private_dir(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn is_private_dir(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(unix)]
fn is_private_file(metadata: &fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o077 == 0
}

#[cfg(not(unix))]
fn is_private_file(_metadata: &fs::Metadata) -> bool {
    true
}

#[cfg(unix)]
fn open_private_new(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(unix)]
fn create_private_dir(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    let mut builder = fs::DirBuilder::new();
    builder.mode(0o700).create(path)
}

#[cfg(not(unix))]
fn create_private_dir(path: &Path) -> io::Result<()> {
    fs::create_dir(path)
}

#[cfg(not(unix))]
fn open_private_new(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}

fn lock_path_for(path: &Path) -> PathBuf {
    path.with_extension("json.lock")
}

fn temp_path_for(path: &Path) -> PathBuf {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    path.with_extension(format!("json.{}.{}.tmp", std::process::id(), nonce))
}

fn invalid(
    path: &Path,
    message: impl Into<String>,
) -> LocalCommandTrustStoreError {
    LocalCommandTrustStoreError::Invalid {
        path: path.to_path_buf(),
        message: message.into(),
    }
}

fn io_error(path: &Path, source: io::Error) -> LocalCommandTrustStoreError {
    LocalCommandTrustStoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use tempfile::tempdir;

    use super::*;

    const DIGEST_A: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const DIGEST_B: &str =
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    #[test]
    fn missing_read_is_empty_and_does_not_create_store() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let result =
            local_command_trust_store_get(&home, "checkout:v1:/repo").unwrap();

        assert_eq!(result.status, "available");
        assert!(result.record.is_none());
        assert!(!home.exists());
    }

    #[test]
    fn allow_is_atomic_private_and_idempotent_and_revoke_is_scoped() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let identity_a = "project:v1:alpha:/work/alpha";
        let identity_b = "project:v1:beta:/work/beta";

        let first = local_command_trust_store_allow(
            &home,
            identity_a,
            DIGEST_A,
            LOCAL_COMMAND_TRUST_POLICY_VERSION,
            "bryan",
        )
        .unwrap();
        assert!(first.changed);
        assert_eq!(first.record.as_ref().unwrap().digest, DIGEST_A);
        let repeated = local_command_trust_store_allow(
            &home,
            identity_a,
            DIGEST_A,
            LOCAL_COMMAND_TRUST_POLICY_VERSION,
            "another-human",
        )
        .unwrap();
        assert!(!repeated.changed);
        assert_eq!(repeated.record.as_ref().unwrap().acknowledged_by, "bryan");
        local_command_trust_store_allow(
            &home,
            identity_b,
            DIGEST_B,
            LOCAL_COMMAND_TRUST_POLICY_VERSION,
            "bryan",
        )
        .unwrap();

        let directory = home.join(TRUST_DIRNAME);
        let path = local_command_trust_store_path(&home);
        let record = local_command_trust_store_get(&home, identity_a).unwrap();
        assert_eq!(record.record.as_ref().unwrap().identity, identity_a);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let revoked =
            local_command_trust_store_revoke(&home, identity_a).unwrap();
        assert!(revoked.changed);
        assert!(local_command_trust_store_get(&home, identity_a)
            .unwrap()
            .record
            .is_none());
        assert_eq!(
            local_command_trust_store_get(&home, identity_b)
                .unwrap()
                .record
                .unwrap()
                .digest,
            DIGEST_B
        );
    }

    #[test]
    fn corrupt_store_is_untrusted_and_mutations_do_not_overwrite_it() {
        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let directory = home.join(TRUST_DIRNAME);
        fs::create_dir_all(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
                .unwrap();
        }
        let path = local_command_trust_store_path(&home);
        fs::write(&path, b"not-json").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .unwrap();
        }

        let status =
            local_command_trust_store_get(&home, "checkout:v1:/repo").unwrap();
        assert_eq!(status.status, "untrusted");
        assert!(status.reason.as_deref().unwrap().contains("valid JSON"));
        assert!(local_command_trust_store_allow(
            &home,
            "checkout:v1:/repo",
            DIGEST_A,
            LOCAL_COMMAND_TRUST_POLICY_VERSION,
            "bryan",
        )
        .is_err());
        assert_eq!(fs::read(path).unwrap(), b"not-json");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_record_is_untrusted_and_not_followed_for_mutation() {
        use std::os::unix::fs::symlink;

        let temp = tempdir().unwrap();
        let home = temp.path().join("home");
        let directory = home.join(TRUST_DIRNAME);
        fs::create_dir_all(&directory).unwrap();
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))
            .unwrap();
        let target = temp.path().join("outside.json");
        fs::write(&target, b"outside").unwrap();
        let path = local_command_trust_store_path(&home);
        symlink(&target, &path).unwrap();

        let status =
            local_command_trust_store_get(&home, "checkout:v1:/repo").unwrap();
        assert_eq!(status.status, "untrusted");
        assert!(status.reason.as_deref().unwrap().contains("symlink"));
        assert!(local_command_trust_store_revoke(&home, "checkout:v1:/repo")
            .is_err());
        assert_eq!(fs::read(target).unwrap(), b"outside");
    }

    #[test]
    fn concurrent_project_updates_preserve_unrelated_records() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().join("home"));
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for (identity, digest) in [
            ("checkout:v1:/one", DIGEST_A),
            ("checkout:v1:/two", DIGEST_B),
        ] {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                local_command_trust_store_allow(
                    home.as_path(),
                    identity,
                    digest,
                    LOCAL_COMMAND_TRUST_POLICY_VERSION,
                    "bryan",
                )
                .unwrap();
            }));
        }
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(
            local_command_trust_store_get(&home, "checkout:v1:/one")
                .unwrap()
                .record
                .unwrap()
                .digest,
            DIGEST_A
        );
        assert_eq!(
            local_command_trust_store_get(&home, "checkout:v1:/two")
                .unwrap()
                .record
                .unwrap()
                .digest,
            DIGEST_B
        );
    }
}

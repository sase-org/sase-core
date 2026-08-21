//! Versioned machine-local feature-flag preference store.
//!
//! Persistent enable/disable choices live under SASE home as
//! `feature_flags.json`. This module owns wire shape, snake_case keys, a
//! bounded exclusive lock, and crash-safe atomic writes. It does not own the
//! Python feature-flag registry: unknown but valid keys are preserved.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

/// On-disk and API schema version for the preference store.
pub const FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION: u32 = 1;
/// Filename under SASE home (`~/.sase/feature_flags.json` by default).
pub const FEATURE_FLAG_STATE_FILENAME: &str = "feature_flags.json";
/// Reject whole-file reads and writes above this UTF-8 byte budget.
pub const FEATURE_FLAG_STATE_MAX_BYTES: usize = 64 * 1024;

const LOCK_TIMEOUT_ENV: &str = "SASE_FEATURE_FLAG_STATE_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);
const STALE_TEMP_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Persisted snapshot plus any non-destructive read diagnostic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureFlagStateSnapshotWire {
    pub version: u32,
    pub flags: BTreeMap<String, bool>,
    pub path: String,
    pub diagnostics: Vec<FeatureFlagStateDiagnosticWire>,
}

/// Outcome of one exclusive-lock read/modify/write set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureFlagStateSetOutcomeWire {
    pub version: u32,
    pub flag: String,
    pub enabled: bool,
    pub previous: Option<bool>,
    pub changed: bool,
    pub flags: BTreeMap<String, bool>,
    pub path: String,
    pub diagnostics: Vec<FeatureFlagStateDiagnosticWire>,
}

/// Structured diagnostic attached to a snapshot or described by an error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureFlagStateDiagnosticWire {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeatureFlagStateFileWire {
    version: u32,
    flags: BTreeMap<String, bool>,
}

#[derive(Debug, Error)]
pub enum FeatureFlagStateError {
    #[error("{message}")]
    Invalid { path: PathBuf, message: String },
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
    #[error("feature-flag state I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

enum LoadedState {
    Missing,
    Valid(BTreeMap<String, bool>),
    Unusable { code: &'static str, message: String },
}

/// Resolve the state file path under `sase_home`.
pub fn feature_flag_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(FEATURE_FLAG_STATE_FILENAME)
}

/// Read the preference snapshot. Missing state is empty. Malformed,
/// oversized, or unsupported files are left on disk and reported as
/// diagnostics with no preferences.
pub fn feature_flag_state_get(
    sase_home: &Path,
) -> Result<FeatureFlagStateSnapshotWire, FeatureFlagStateError> {
    let path = feature_flag_state_path(sase_home);
    let lock = lock_store(sase_home, "feature_flag_state_get")?;
    let loaded = load_unlocked(&path);
    unlock(lock, &path)?;
    let loaded = loaded?;
    Ok(snapshot_from_loaded(&path, loaded))
}

/// Set one snake_case flag inside a single exclusive-lock transaction.
///
/// Concurrent writers of different keys serialize here so neither write is
/// lost. Same-value sets are idempotent and skip the atomic replace.
/// Unusable existing files fail without being overwritten.
pub fn feature_flag_state_set(
    sase_home: &Path,
    flag: &str,
    enabled: bool,
) -> Result<FeatureFlagStateSetOutcomeWire, FeatureFlagStateError> {
    let path = feature_flag_state_path(sase_home);
    if !is_feature_flag_key(flag) {
        return Err(FeatureFlagStateError::Invalid {
            path: path.clone(),
            message: format!("feature flag key must be snake_case: {flag:?}"),
        });
    }
    let lock = lock_store(sase_home, "feature_flag_state_set")?;
    let result = set_unlocked(&path, flag, enabled);
    unlock(lock, &path)?;
    result
}

fn set_unlocked(
    path: &Path,
    flag: &str,
    enabled: bool,
) -> Result<FeatureFlagStateSetOutcomeWire, FeatureFlagStateError> {
    let mut flags = match load_unlocked(path)? {
        LoadedState::Missing => BTreeMap::new(),
        LoadedState::Valid(flags) => flags,
        LoadedState::Unusable { message, .. } => {
            return Err(mutation_blocked(path, &message));
        }
    };
    let previous = flags.get(flag).copied();
    let changed = previous != Some(enabled);
    if changed {
        flags.insert(flag.to_string(), enabled);
        write_snapshot_atomic(path, &flags)?;
    }
    Ok(FeatureFlagStateSetOutcomeWire {
        version: FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
        flag: flag.to_string(),
        enabled,
        previous,
        changed,
        flags,
        path: path.display().to_string(),
        diagnostics: Vec::new(),
    })
}

fn snapshot_from_loaded(
    path: &Path,
    loaded: LoadedState,
) -> FeatureFlagStateSnapshotWire {
    match loaded {
        LoadedState::Missing => FeatureFlagStateSnapshotWire {
            version: FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
            flags: BTreeMap::new(),
            path: path.display().to_string(),
            diagnostics: Vec::new(),
        },
        LoadedState::Valid(flags) => FeatureFlagStateSnapshotWire {
            version: FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
            flags,
            path: path.display().to_string(),
            diagnostics: Vec::new(),
        },
        LoadedState::Unusable { code, message } => {
            FeatureFlagStateSnapshotWire {
                version: FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
                flags: BTreeMap::new(),
                path: path.display().to_string(),
                diagnostics: vec![diagnostic(code, &message, path)],
            }
        }
    }
}

fn load_unlocked(path: &Path) -> Result<LoadedState, FeatureFlagStateError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(LoadedState::Missing);
        }
        Err(error) => return Err(io_error(path, error)),
    };
    if bytes.len() > FEATURE_FLAG_STATE_MAX_BYTES {
        return Ok(LoadedState::Unusable {
            code: "oversized",
            message: format!(
                "state file exceeds {FEATURE_FLAG_STATE_MAX_BYTES} bytes"
            ),
        });
    }
    let text = match std::str::from_utf8(&bytes) {
        Ok(text) => text,
        Err(_) => {
            return Ok(LoadedState::Unusable {
                code: "invalid_utf8",
                message: "state file is not valid UTF-8".to_string(),
            });
        }
    };
    let value: Value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => {
            return Ok(LoadedState::Unusable {
                code: "malformed_json",
                message: "state file is not valid JSON".to_string(),
            });
        }
    };
    Ok(decode_state_value(value))
}

fn decode_state_value(value: Value) -> LoadedState {
    let Some(object) = value.as_object() else {
        return LoadedState::Unusable {
            code: "invalid_schema",
            message: "state file must be a JSON object".to_string(),
        };
    };
    match object.get("version") {
        Some(version)
            if version.as_u64()
                == Some(u64::from(FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION)) => {}
        Some(version) if version.as_u64().is_some() => {
            return LoadedState::Unusable {
                code: "unsupported_version",
                message: format!(
                    "state file version {} is not supported (expected {})",
                    version, FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION
                ),
            };
        }
        _ => {
            return LoadedState::Unusable {
                code: "invalid_schema",
                message: "state file version must be a positive integer"
                    .to_string(),
            };
        }
    }
    let file: FeatureFlagStateFileWire = match serde_json::from_value(value) {
        Ok(file) => file,
        Err(_) => {
            return LoadedState::Unusable {
                code: "invalid_schema",
                message: "state file must contain version and a flags object of booleans"
                    .to_string(),
            };
        }
    };
    if let Some(key) = file.flags.keys().find(|key| !is_feature_flag_key(key)) {
        return LoadedState::Unusable {
            code: "invalid_key",
            message: format!(
                "state file contains a non-snake_case flag key: {key:?}"
            ),
        };
    }
    LoadedState::Valid(file.flags)
}

fn write_snapshot_atomic(
    path: &Path,
    flags: &BTreeMap<String, bool>,
) -> Result<(), FeatureFlagStateError> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|error| io_error(parent, error))?;
    reap_stale_temp_siblings(path, SystemTime::now());
    let file = FeatureFlagStateFileWire {
        version: FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
        flags: flags.clone(),
    };
    let mut bytes = serde_json::to_vec_pretty(&file).map_err(|error| {
        FeatureFlagStateError::Invalid {
            path: path.to_path_buf(),
            message: format!("failed to serialize feature-flag state: {error}"),
        }
    })?;
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    if bytes.len() > FEATURE_FLAG_STATE_MAX_BYTES {
        return Err(FeatureFlagStateError::Invalid {
            path: path.to_path_buf(),
            message: format!(
                "serialized feature-flag state exceeds {FEATURE_FLAG_STATE_MAX_BYTES} bytes"
            ),
        });
    }
    let tmp_path = temp_path_for(path);
    let write_result = (|| -> Result<(), FeatureFlagStateError> {
        let handle = OpenOptions::new()
            .create_new(true)
            .write(true)
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
        fs::rename(&tmp_path, path).map_err(|error| io_error(path, error))?;
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

fn lock_store(
    sase_home: &Path,
    operation: &str,
) -> Result<HeldStoreLock, FeatureFlagStateError> {
    lock_store_with_timeout(
        sase_home,
        operation,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
    )
}

fn lock_store_with_timeout(
    sase_home: &Path,
    operation: &str,
    timeout: Duration,
) -> Result<HeldStoreLock, FeatureFlagStateError> {
    fs::create_dir_all(sase_home)
        .map_err(|error| io_error(sase_home, error))?;
    let path = feature_flag_state_path(sase_home);
    let lock_path = lock_path_for(&path);
    acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout,
        operation,
    )
    .map_err(|error| match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => FeatureFlagStateError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        error => FeatureFlagStateError::Invalid {
            path: path.clone(),
            message: error.to_string(),
        },
    })
}

fn unlock(
    lock: HeldStoreLock,
    path: &Path,
) -> Result<(), FeatureFlagStateError> {
    lock.release().map_err(|error| io_error(path, error))
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FEATURE_FLAG_STATE_FILENAME);
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FEATURE_FLAG_STATE_FILENAME);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn ensure_parent(path: &Path) -> Result<&Path, FeatureFlagStateError> {
    path.parent().ok_or_else(|| FeatureFlagStateError::Invalid {
        path: path.to_path_buf(),
        message: format!(
            "feature-flag state path has no parent: {}",
            path.display()
        ),
    })
}

fn reap_stale_temp_siblings(path: &Path, now: SystemTime) {
    let Some(parent) = path.parent() else {
        return;
    };
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(FEATURE_FLAG_STATE_FILENAME);
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
        if age <= STALE_TEMP_MAX_AGE {
            continue;
        }
        let _ = fs::remove_file(entry.path());
    }
}

fn is_feature_flag_key(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let mut prev_underscore = false;
    for character in chars {
        if character == '_' {
            if prev_underscore {
                return false;
            }
            prev_underscore = true;
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            prev_underscore = false;
        } else {
            return false;
        }
    }
    !prev_underscore
}

fn diagnostic(
    code: &str,
    message: &str,
    path: &Path,
) -> FeatureFlagStateDiagnosticWire {
    FeatureFlagStateDiagnosticWire {
        severity: "error".to_string(),
        code: code.to_string(),
        message: message.to_string(),
        path: path.display().to_string(),
    }
}

fn mutation_blocked(path: &Path, reason: &str) -> FeatureFlagStateError {
    FeatureFlagStateError::Invalid {
        path: path.to_path_buf(),
        message: format!(
            "cannot update feature-flag state at {}: {reason}. The file was left unchanged; move or repair it, then retry.",
            path.display()
        ),
    }
}

fn io_error(path: &Path, source: io::Error) -> FeatureFlagStateError {
    FeatureFlagStateError::Io {
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;
    use tempfile::tempdir;

    fn state_bytes(home: &Path) -> Vec<u8> {
        fs::read(feature_flag_state_path(home)).unwrap()
    }

    fn tmp_names(dir: &Path) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| {
                let name = entry.ok()?.file_name();
                let name = name.to_string_lossy().into_owned();
                (name.starts_with(".feature_flags.json.")
                    && name.ends_with(".tmp"))
                .then_some(name)
            })
            .collect()
    }

    #[test]
    fn missing_state_is_an_empty_snapshot() {
        let temp = tempdir().unwrap();
        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.version, FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION);
        assert!(snapshot.flags.is_empty());
        assert!(snapshot.diagnostics.is_empty());
        assert_eq!(
            snapshot.path,
            feature_flag_state_path(temp.path()).display().to_string()
        );
        assert!(!feature_flag_state_path(temp.path()).exists());
    }

    #[test]
    fn both_booleans_round_trip_in_stable_order() {
        let temp = tempdir().unwrap();
        let prettier =
            feature_flag_state_set(temp.path(), "prettier_enabled", false)
                .unwrap();
        assert_eq!(prettier.previous, None);
        assert!(prettier.changed);
        assert!(!prettier.enabled);

        let epic =
            feature_flag_state_set(temp.path(), "epic_resume_gate", true)
                .unwrap();
        assert_eq!(epic.previous, None);
        let keys: Vec<&str> = epic.flags.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["epic_resume_gate", "prettier_enabled"]);

        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.flags, epic.flags);
        assert_eq!(
            String::from_utf8(state_bytes(temp.path())).unwrap(),
            "{\n  \"version\": 1,\n  \"flags\": {\n    \"epic_resume_gate\": true,\n    \"prettier_enabled\": false\n  }\n}\n"
        );
    }

    #[test]
    fn same_value_set_is_idempotent_and_skips_rewrite() {
        let temp = tempdir().unwrap();
        feature_flag_state_set(temp.path(), "artifact_links", true).unwrap();
        let first = state_bytes(temp.path());
        let metadata =
            fs::metadata(feature_flag_state_path(temp.path())).unwrap();
        let outcome =
            feature_flag_state_set(temp.path(), "artifact_links", true)
                .unwrap();
        assert_eq!(outcome.previous, Some(true));
        assert!(!outcome.changed);
        assert_eq!(state_bytes(temp.path()), first);
        assert_eq!(
            fs::metadata(feature_flag_state_path(temp.path()))
                .unwrap()
                .modified()
                .unwrap(),
            metadata.modified().unwrap()
        );
    }

    #[test]
    fn concurrent_writers_preserve_distinct_keys() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for (flag, enabled) in [("alpha_flag", true), ("beta_flag", false)] {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                feature_flag_state_set(&home, flag, enabled).unwrap()
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.flags.get("alpha_flag"), Some(&true));
        assert_eq!(snapshot.flags.get("beta_flag"), Some(&false));
    }

    #[test]
    fn lock_timeout_names_the_holder() {
        let temp = tempdir().unwrap();
        let path = feature_flag_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        let lock_path = lock_path_for(&path);
        let holder = acquire_store_lock(
            &lock_path,
            &holder_path_for(&lock_path),
            LockMode::Exclusive,
            Duration::from_secs(1),
            "holding-operation",
        )
        .unwrap();

        let started = Instant::now();
        let error = lock_store_with_timeout(
            temp.path(),
            "contender",
            Duration::from_millis(50),
        )
        .unwrap_err();
        let FeatureFlagStateError::LockTimeout {
            holder: recorded, ..
        } = error
        else {
            panic!("expected lock timeout, got {error}");
        };
        assert!(recorded.contains("operation=holding-operation"));
        assert!(started.elapsed() < Duration::from_secs(2));
        holder.release().unwrap();
    }

    #[test]
    fn malformed_wrong_version_type_and_key_are_non_destructive() {
        let temp = tempdir().unwrap();
        let path = feature_flag_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        for (body, code) in [
            ("not json", "malformed_json"),
            (r#"{"version":2,"flags":{}}"#, "unsupported_version"),
            (r#"{"version":1,"flags":[]}"#, "invalid_schema"),
            (
                r#"{"version":1,"flags":{"epic_resume_gate":"yes"}}"#,
                "invalid_schema",
            ),
            (r#"{"version":1,"flags":{"NotSnake":true}}"#, "invalid_key"),
        ] {
            fs::write(&path, body).unwrap();
            let snapshot = feature_flag_state_get(temp.path()).unwrap();
            assert!(snapshot.flags.is_empty(), "{code}");
            assert_eq!(snapshot.diagnostics[0].code, code);
            assert_eq!(fs::read_to_string(&path).unwrap(), body);
            let error =
                feature_flag_state_set(temp.path(), "epic_resume_gate", true)
                    .unwrap_err();
            let message = error.to_string();
            assert!(message.contains(path.to_str().unwrap()), "{message}");
            assert!(message.contains("left unchanged"), "{message}");
            assert_eq!(fs::read_to_string(&path).unwrap(), body);
        }
    }

    #[test]
    fn invalid_utf8_and_size_limit_are_non_destructive() {
        let temp = tempdir().unwrap();
        let path = feature_flag_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        fs::write(&path, [0xff, 0xfe]).unwrap();
        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.diagnostics[0].code, "invalid_utf8");
        assert_eq!(fs::read(&path).unwrap(), [0xff, 0xfe]);

        let oversized = vec![b'x'; FEATURE_FLAG_STATE_MAX_BYTES + 1];
        fs::write(&path, &oversized).unwrap();
        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.diagnostics[0].code, "oversized");
        assert_eq!(fs::read(&path).unwrap(), oversized);
        assert!(feature_flag_state_set(temp.path(), "alpha_flag", true)
            .unwrap_err()
            .to_string()
            .contains("left unchanged"));
        assert_eq!(fs::read(&path).unwrap(), oversized);
    }

    #[test]
    fn set_rejects_invalid_keys_without_creating_state() {
        let temp = tempdir().unwrap();
        for key in ["", "NotSnake", "foo_", "_foo", "foo__bar", "Foo"] {
            let error =
                feature_flag_state_set(temp.path(), key, true).unwrap_err();
            assert!(
                error.to_string().contains("snake_case"),
                "{key:?} -> {error}"
            );
        }
        assert!(!feature_flag_state_path(temp.path()).exists());
    }

    #[test]
    fn unknown_valid_keys_are_preserved_across_writes() {
        let temp = tempdir().unwrap();
        feature_flag_state_set(temp.path(), "future_release_flag", true)
            .unwrap();
        let outcome =
            feature_flag_state_set(temp.path(), "epic_resume_gate", false)
                .unwrap();
        assert_eq!(outcome.flags.get("future_release_flag"), Some(&true));
        assert_eq!(outcome.flags.get("epic_resume_gate"), Some(&false));
        let snapshot = feature_flag_state_get(temp.path()).unwrap();
        assert_eq!(snapshot.flags, outcome.flags);
    }

    #[test]
    fn failed_atomic_write_cleans_temp_and_leaves_destination() {
        let temp = tempdir().unwrap();
        let dest = temp.path().join(FEATURE_FLAG_STATE_FILENAME);
        fs::create_dir(&dest).unwrap();
        let mut flags = BTreeMap::new();
        flags.insert("epic_resume_gate".to_string(), true);
        assert!(write_snapshot_atomic(&dest, &flags).is_err());
        assert!(dest.is_dir());
        assert!(tmp_names(temp.path()).is_empty());
    }

    #[test]
    fn failed_set_leaves_previous_state_and_no_temp_litter() {
        let temp = tempdir().unwrap();
        feature_flag_state_set(temp.path(), "epic_resume_gate", true).unwrap();
        let before = state_bytes(temp.path());
        let original = fs::metadata(temp.path()).unwrap().permissions();
        fs::set_permissions(temp.path(), fs::Permissions::from_mode(0o555))
            .unwrap();
        let error =
            feature_flag_state_set(temp.path(), "prettier_enabled", false);
        fs::set_permissions(temp.path(), original).unwrap();
        assert!(error.is_err());
        assert_eq!(state_bytes(temp.path()), before);
        assert!(tmp_names(temp.path()).is_empty());
    }

    #[test]
    fn successful_write_leaves_no_temp_litter() {
        let temp = tempdir().unwrap();
        feature_flag_state_set(temp.path(), "epic_resume_gate", true).unwrap();
        assert!(tmp_names(temp.path()).is_empty());
    }
}

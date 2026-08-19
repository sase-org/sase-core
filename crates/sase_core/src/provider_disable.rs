//! Machine-wide temporary LLM provider-disable state.
//!
//! The store is deliberately independent from configuration and provider
//! registration. Every operation accepts the SASE home and clock at the
//! domain seam so callers can honor `$SASE_HOME` and tests remain
//! deterministic.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const PROVIDER_DISABLE_WIRE_SCHEMA_VERSION: u32 = 2;
pub const PROVIDER_DISABLE_STATE_FILENAME: &str = "llm_provider_disables.json";
const PROVIDER_DISABLE_WIRE_SCHEMA_V1: u32 = 1;
const PROVIDER_DISABLE_LOCK_FILENAME: &str = "llm_provider_disables.lock";
const LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

/// How a disable participates in routing.
///
/// `hard` is today's fail-closed disable. `soft` spares the provider in
/// load-balanced pools while another member can cover; it never diverts a
/// `||` fallback and never blocks an explicit request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProviderDisableMode {
    Hard,
    Soft,
}

impl ProviderDisableMode {
    /// Stable lowercase name stored on the wire.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hard => "hard",
            Self::Soft => "soft",
        }
    }

    /// Parse the two exact wire strings. Anything else is a validation error.
    pub fn parse(value: &str) -> Result<Self, ProviderDisableError> {
        match value {
            "hard" => Ok(Self::Hard),
            "soft" => Ok(Self::Soft),
            _ => Err(ProviderDisableError::Validation(format!(
                "mode must be hard or soft, got {value:?}"
            ))),
        }
    }
}

/// Stable record returned to frontends and stored on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDisableWire {
    pub version: u32,
    pub provider: String,
    pub created_at: f64,
    pub expires_at: Option<f64>,
    pub source: String,
    pub mode: ProviderDisableMode,
}

/// Schema-1 on-disk record. Migrated in place to [`ProviderDisableWire`]
/// with `mode: Hard`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderDisableWireV1 {
    version: u32,
    provider: String,
    created_at: f64,
    expires_at: Option<f64>,
    source: String,
}

/// Ordered active provider-disable snapshot returned to frontends.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDisableSnapshotWire {
    pub version: u32,
    pub disables: Vec<ProviderDisableWire>,
}

/// Outcome of a conditional first-writer provider-disable write.
///
/// `inserted` is true only when this caller created the active record.
/// Losing callers receive the unchanged stored record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderDisableWriteOutcomeWire {
    pub version: u32,
    pub inserted: bool,
    pub record: ProviderDisableWire,
}

#[derive(Debug, Serialize, Deserialize)]
struct ProviderDisableStateWire {
    version: u32,
    disables: BTreeMap<String, ProviderDisableWire>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawProviderDisableStateWire {
    version: u32,
    disables: BTreeMap<String, Value>,
}

#[derive(Debug, Error)]
pub enum ProviderDisableError {
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the provider-disable state lock")]
    LockTimeout,
    #[error("provider-disable state I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("provider-disable state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn provider_disable_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(PROVIDER_DISABLE_STATE_FILENAME)
}

fn provider_disable_lock_path(sase_home: &Path) -> PathBuf {
    sase_home.join(PROVIDER_DISABLE_LOCK_FILENAME)
}

/// Read active disables, pruning malformed or expired per-provider entries.
pub fn get_provider_disables(
    sase_home: &Path,
    now: f64,
) -> Result<ProviderDisableSnapshotWire, ProviderDisableError> {
    validate_now(now)?;
    with_lock(sase_home, || {
        let records = read_records_locked(sase_home, Some(now))?;
        Ok(snapshot_from_records(records))
    })
}

/// Set or replace a provider disable for a relative duration.
///
/// `duration_seconds = None` means until cleared.
pub fn set_provider_disable_relative(
    sase_home: &Path,
    provider: &str,
    duration_seconds: Option<f64>,
    source: &str,
    now: f64,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWire, ProviderDisableError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    let expires_at = validate_relative_expires_at(duration_seconds, now)?;
    write_provider_record(sase_home, &provider, &source, now, expires_at, mode)
}

/// Set or replace a provider disable until an exact future Unix timestamp.
pub fn set_provider_disable_until(
    sase_home: &Path,
    provider: &str,
    expires_at: f64,
    source: &str,
    now: f64,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWire, ProviderDisableError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    let expires_at = validate_until_expires_at(expires_at, now)?;
    write_provider_record(
        sase_home,
        &provider,
        &source,
        now,
        Some(expires_at),
        mode,
    )
}

/// Write a relative-duration disable only when no active record exists.
pub fn try_set_provider_disable_relative(
    sase_home: &Path,
    provider: &str,
    duration_seconds: Option<f64>,
    source: &str,
    now: f64,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWriteOutcomeWire, ProviderDisableError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    let expires_at = validate_relative_expires_at(duration_seconds, now)?;
    write_provider_record_if_absent(
        sase_home, &provider, &source, now, expires_at, mode,
    )
}

/// Write an exact-expiry disable only when no active record exists.
pub fn try_set_provider_disable_until(
    sase_home: &Path,
    provider: &str,
    expires_at: f64,
    source: &str,
    now: f64,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWriteOutcomeWire, ProviderDisableError> {
    let provider = validate_provider(provider)?;
    let source = validate_source(source)?;
    validate_now(now)?;
    let expires_at = validate_until_expires_at(expires_at, now)?;
    write_provider_record_if_absent(
        sase_home,
        &provider,
        &source,
        now,
        Some(expires_at),
        mode,
    )
}

/// Clear one provider disable. Missing state is a successful idempotent no-op.
pub fn clear_provider_disable(
    sase_home: &Path,
    provider: &str,
) -> Result<bool, ProviderDisableError> {
    let provider = validate_provider(provider)?;
    with_lock(sase_home, || {
        let path = provider_disable_state_path(sase_home);
        let mut records = read_records_locked(sase_home, None)?;
        let removed = records.remove(&provider).is_some();
        if !removed {
            return Ok(false);
        }
        if records.is_empty() {
            remove_invalid_state(&path)?;
        } else {
            write_state_atomic(&path, &records)?;
        }
        Ok(true)
    })
}

fn write_provider_record(
    sase_home: &Path,
    provider: &str,
    source: &str,
    now: f64,
    expires_at: Option<f64>,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWire, ProviderDisableError> {
    let record = candidate_record(provider, source, now, expires_at, mode);
    with_lock(sase_home, || {
        let mut records = read_records_locked(sase_home, Some(now))?;
        records.insert(provider.to_string(), record.clone());
        write_state_atomic(&provider_disable_state_path(sase_home), &records)?;
        Ok(record.clone())
    })
}

fn write_provider_record_if_absent(
    sase_home: &Path,
    provider: &str,
    source: &str,
    now: f64,
    expires_at: Option<f64>,
    mode: ProviderDisableMode,
) -> Result<ProviderDisableWriteOutcomeWire, ProviderDisableError> {
    let candidate = candidate_record(provider, source, now, expires_at, mode);
    with_lock(sase_home, || {
        let mut records = read_records_locked(sase_home, Some(now))?;
        if let Some(existing) = records.get(provider) {
            return Ok(write_outcome(false, existing.clone()));
        }
        records.insert(provider.to_string(), candidate.clone());
        write_state_atomic(&provider_disable_state_path(sase_home), &records)?;
        Ok(write_outcome(true, candidate))
    })
}

fn candidate_record(
    provider: &str,
    source: &str,
    now: f64,
    expires_at: Option<f64>,
    mode: ProviderDisableMode,
) -> ProviderDisableWire {
    ProviderDisableWire {
        version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
        provider: provider.to_string(),
        created_at: now,
        expires_at,
        source: source.to_string(),
        mode,
    }
}

fn write_outcome(
    inserted: bool,
    record: ProviderDisableWire,
) -> ProviderDisableWriteOutcomeWire {
    ProviderDisableWriteOutcomeWire {
        version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
        inserted,
        record,
    }
}

fn read_records_locked(
    sase_home: &Path,
    now: Option<f64>,
) -> Result<BTreeMap<String, ProviderDisableWire>, ProviderDisableError> {
    let path = provider_disable_state_path(sase_home);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new())
        }
        Err(error) => return Err(error.into()),
    };
    let raw: RawProviderDisableStateWire = match serde_json::from_slice(&bytes)
    {
        Ok(raw) => raw,
        Err(_) => {
            remove_invalid_state(&path)?;
            return Ok(BTreeMap::new());
        }
    };
    if raw.version != PROVIDER_DISABLE_WIRE_SCHEMA_VERSION
        && raw.version != PROVIDER_DISABLE_WIRE_SCHEMA_V1
    {
        remove_invalid_state(&path)?;
        return Ok(BTreeMap::new());
    }

    let migrating_v1 = raw.version == PROVIDER_DISABLE_WIRE_SCHEMA_V1;
    let mut changed = migrating_v1;
    let mut records = BTreeMap::new();
    for (provider, value) in raw.disables {
        let record = if migrating_v1 {
            match migrate_v1_record(value) {
                Some(record) => record,
                None => {
                    changed = true;
                    continue;
                }
            }
        } else {
            match serde_json::from_value::<ProviderDisableWire>(value) {
                Ok(record) => record,
                Err(_) => {
                    changed = true;
                    continue;
                }
            }
        };
        if !is_valid_record_for_key(&provider, &record) {
            changed = true;
            continue;
        }
        if let Some(current) = now {
            if record
                .expires_at
                .is_some_and(|expires_at| current >= expires_at)
            {
                changed = true;
                continue;
            }
        }
        records.insert(provider, record);
    }

    if changed {
        if records.is_empty() {
            remove_invalid_state(&path)?;
        } else {
            write_state_atomic(&path, &records)?;
        }
    }

    Ok(records)
}

fn snapshot_from_records(
    records: BTreeMap<String, ProviderDisableWire>,
) -> ProviderDisableSnapshotWire {
    ProviderDisableSnapshotWire {
        version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
        disables: records.into_values().collect(),
    }
}

fn validate_provider(provider: &str) -> Result<String, ProviderDisableError> {
    let cleaned = provider.trim();
    if cleaned.is_empty() {
        return Err(ProviderDisableError::Validation(
            "provider must be non-empty".to_string(),
        ));
    }
    if provider != cleaned {
        return Err(ProviderDisableError::Validation(
            "provider must not contain leading or trailing whitespace"
                .to_string(),
        ));
    }
    if cleaned.chars().any(char::is_control) {
        return Err(ProviderDisableError::Validation(
            "provider must not contain control characters".to_string(),
        ));
    }
    Ok(cleaned.to_string())
}

fn validate_source(source: &str) -> Result<String, ProviderDisableError> {
    let cleaned = source.trim();
    if cleaned.is_empty() {
        return Err(ProviderDisableError::Validation(
            "source must be non-empty".to_string(),
        ));
    }
    if cleaned.chars().any(char::is_control) {
        return Err(ProviderDisableError::Validation(
            "source must not contain control characters".to_string(),
        ));
    }
    Ok(cleaned.to_string())
}

fn validate_now(now: f64) -> Result<(), ProviderDisableError> {
    if !now.is_finite() || now <= 0.0 {
        return Err(ProviderDisableError::Validation(
            "current timestamp must be finite and positive".to_string(),
        ));
    }
    Ok(())
}

fn validate_relative_expires_at(
    duration_seconds: Option<f64>,
    now: f64,
) -> Result<Option<f64>, ProviderDisableError> {
    if let Some(duration) = duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(ProviderDisableError::Validation(
                "duration_seconds must be finite and positive or None"
                    .to_string(),
            ));
        }
    }
    let expires_at = duration_seconds.map(|duration| now + duration);
    if expires_at.is_some_and(|expiry| !expiry.is_finite()) {
        return Err(ProviderDisableError::Validation(
            "computed expires_at must be finite".to_string(),
        ));
    }
    Ok(expires_at)
}

fn validate_until_expires_at(
    expires_at: f64,
    now: f64,
) -> Result<f64, ProviderDisableError> {
    if !expires_at.is_finite() {
        return Err(ProviderDisableError::Validation(
            "expires_at must be finite".to_string(),
        ));
    }
    if expires_at <= now {
        return Err(ProviderDisableError::Validation(
            "expires_at must be in the future".to_string(),
        ));
    }
    Ok(expires_at)
}

fn migrate_v1_record(value: Value) -> Option<ProviderDisableWire> {
    let record: ProviderDisableWireV1 = serde_json::from_value(value).ok()?;
    if record.version != PROVIDER_DISABLE_WIRE_SCHEMA_V1 {
        return None;
    }
    Some(ProviderDisableWire {
        version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
        provider: record.provider,
        created_at: record.created_at,
        expires_at: record.expires_at,
        source: record.source,
        mode: ProviderDisableMode::Hard,
    })
}

fn is_valid_record_for_key(key: &str, record: &ProviderDisableWire) -> bool {
    record.version == PROVIDER_DISABLE_WIRE_SCHEMA_VERSION
        && record.provider == key
        && validate_provider(&record.provider).is_ok()
        && record.created_at.is_finite()
        && record.created_at > 0.0
        && record.expires_at.map_or(true, |expires_at| {
            expires_at.is_finite() && expires_at > record.created_at
        })
        && validate_source(&record.source).is_ok()
}

fn write_state_atomic(
    path: &Path,
    records: &BTreeMap<String, ProviderDisableWire>,
) -> Result<(), ProviderDisableError> {
    let parent = path.parent().ok_or_else(|| {
        ProviderDisableError::Validation(
            "provider-disable path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let state = ProviderDisableStateWire {
        version: PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
        disables: records.clone(),
    };
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, &state)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn remove_invalid_state(path: &Path) -> Result<(), ProviderDisableError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn with_lock<T>(
    sase_home: &Path,
    operation: impl FnOnce() -> Result<T, ProviderDisableError>,
) -> Result<T, ProviderDisableError> {
    fs::create_dir_all(sase_home)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(provider_disable_lock_path(sase_home))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LOCK_TIMEOUT {
                    return Err(ProviderDisableError::LockTimeout);
                }
                thread::sleep(LOCK_RETRY_DELAY);
            }
            Err(error) => return Err(error.into()),
        }
    }
    let result = operation();
    FileExt::unlock(&lock)?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::fs::File;
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;
    const HARD: ProviderDisableMode = ProviderDisableMode::Hard;
    const SOFT: ProviderDisableMode = ProviderDisableMode::Soft;

    #[test]
    fn mode_parse_accepts_exact_wire_strings() {
        assert_eq!(ProviderDisableMode::parse("hard").unwrap(), HARD);
        assert_eq!(ProviderDisableMode::parse("soft").unwrap(), SOFT);
        assert_eq!(HARD.as_str(), "hard");
        assert_eq!(SOFT.as_str(), "soft");
        for value in ["", "Hard", "SOFT", "hard ", " medium", "unknown"] {
            assert!(matches!(
                ProviderDisableMode::parse(value),
                Err(ProviderDisableError::Validation(message))
                    if message.contains("hard or soft")
            ));
        }
    }

    #[test]
    fn concurrent_entries_are_returned_in_provider_order() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];
        for provider in ["codex", "claude"] {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                set_provider_disable_relative(
                    &home,
                    provider,
                    Some(900.0),
                    "test",
                    NOW,
                    HARD,
                )
                .unwrap();
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(
            snapshot
                .disables
                .iter()
                .map(|record| record.provider.as_str())
                .collect::<Vec<_>>(),
            vec!["claude", "codex"]
        );
    }

    #[test]
    fn replacing_one_provider_preserves_siblings() {
        let temp = tempdir().unwrap();
        let claude = set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(60.0),
            "test",
            NOW,
            HARD,
        )
        .unwrap();
        let codex = set_provider_disable_relative(
            temp.path(),
            "codex",
            None,
            "test",
            NOW,
            SOFT,
        )
        .unwrap();
        let replacement = set_provider_disable_until(
            temp.path(),
            "claude",
            NOW + 300.0,
            "ace",
            NOW,
            SOFT,
        )
        .unwrap();

        assert_ne!(replacement, claude);
        assert_eq!(claude.mode, HARD);
        assert_eq!(codex.mode, SOFT);
        assert_eq!(replacement.mode, SOFT);
        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.disables, vec![replacement, codex]);
    }

    #[test]
    fn clear_one_and_missing_clear_are_idempotent() {
        let temp = tempdir().unwrap();
        set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(60.0),
            "test",
            NOW,
            HARD,
        )
        .unwrap();
        let codex = set_provider_disable_relative(
            temp.path(),
            "codex",
            None,
            "test",
            NOW,
            SOFT,
        )
        .unwrap();

        assert!(clear_provider_disable(temp.path(), "claude").unwrap());
        assert!(!clear_provider_disable(temp.path(), "claude").unwrap());
        assert_eq!(
            get_provider_disables(temp.path(), NOW).unwrap().disables,
            vec![codex]
        );
    }

    #[test]
    fn exact_boundary_expiry_and_until_cleared_persistence() {
        let temp = tempdir().unwrap();
        let exact = set_provider_disable_until(
            temp.path(),
            "claude",
            NOW + 10.0,
            "ace",
            NOW,
            HARD,
        )
        .unwrap();
        assert_eq!(
            get_provider_disables(temp.path(), NOW + 9.999)
                .unwrap()
                .disables,
            vec![exact]
        );
        assert!(get_provider_disables(temp.path(), NOW + 10.0)
            .unwrap()
            .disables
            .is_empty());
        assert!(!provider_disable_state_path(temp.path()).exists());

        let permanent = set_provider_disable_relative(
            temp.path(),
            "codex",
            None,
            "ace",
            NOW,
            SOFT,
        )
        .unwrap();
        assert_eq!(permanent.mode, SOFT);
        assert_eq!(
            get_provider_disables(temp.path(), NOW + 1_000_000.0)
                .unwrap()
                .disables,
            vec![permanent]
        );
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        let temp = tempdir().unwrap();
        for provider in ["", " ", "claude\n"] {
            assert!(set_provider_disable_relative(
                temp.path(),
                provider,
                Some(1.0),
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(set_provider_disable_relative(
                temp.path(),
                "claude",
                Some(duration),
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        for expiry in [NOW, NOW - 1.0, f64::NAN, f64::INFINITY] {
            assert!(set_provider_disable_until(
                temp.path(),
                "claude",
                expiry,
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        for now in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(get_provider_disables(temp.path(), now).is_err());
        }
        assert!(set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(1.0),
            " ",
            NOW,
            HARD
        )
        .is_err());
    }

    #[test]
    fn malformed_envelope_deletes_state() {
        let temp = tempdir().unwrap();
        let path = provider_disable_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        for body in [
            "not json".to_string(),
            json!({
                "version": 99,
                "disables": {},
            })
            .to_string(),
            json!({
                "version": 3,
                "disables": {},
            })
            .to_string(),
            json!({
                "version": 1,
                "disables": [],
            })
            .to_string(),
            json!({
                "version": 1,
                "disables": {},
                "extra": true,
            })
            .to_string(),
            json!({
                "version": 2,
                "disables": {},
                "extra": true,
            })
            .to_string(),
        ] {
            fs::write(&path, body).unwrap();
            assert!(get_provider_disables(temp.path(), NOW)
                .unwrap()
                .disables
                .is_empty());
            assert!(!path.exists());
        }
    }

    #[test]
    fn malformed_and_expired_entries_are_pruned_independently() {
        let temp = tempdir().unwrap();
        let path = provider_disable_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        let valid = json!({
            "version": 2,
            "provider": "codex",
            "created_at": NOW - 10.0,
            "expires_at": null,
            "source": "test",
            "mode": "soft",
        });
        fs::write(
            &path,
            serde_json::to_string(&json!({
                "version": 2,
                "disables": {
                    "claude": {
                        "version": 2,
                        "provider": "",
                        "created_at": NOW,
                        "expires_at": null,
                        "source": "test",
                        "mode": "hard"
                    },
                    "codex": valid,
                    "grok": {
                        "version": 2,
                        "provider": "grok",
                        "created_at": NOW - 10.0,
                        "expires_at": NOW - 1.0,
                        "source": "test",
                        "mode": "hard"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.disables.len(), 1);
        assert_eq!(snapshot.disables[0].provider, "codex");
        assert_eq!(snapshot.disables[0].mode, SOFT);
        let rewritten: ProviderDisableStateWire =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(rewritten.version, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION);
        assert_eq!(rewritten.disables.len(), 1);
        assert!(rewritten.disables.contains_key("codex"));
    }

    #[test]
    fn canonical_active_file_is_not_rewritten() {
        let temp = tempdir().unwrap();
        set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(60.0),
            "test",
            NOW,
            HARD,
        )
        .unwrap();
        let path = provider_disable_state_path(temp.path());
        let before = fs::read(&path).unwrap();

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();

        assert_eq!(snapshot.disables.len(), 1);
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    fn write_v1_state(path: &Path, disables: serde_json::Value) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(
            path,
            serde_json::to_string_pretty(&json!({
                "version": 1,
                "disables": disables,
            }))
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn v1_file_migrates_in_place_to_hard_v2() {
        let temp = tempdir().unwrap();
        let path = provider_disable_state_path(temp.path());
        write_v1_state(
            &path,
            json!({
                "claude": {
                    "version": 1,
                    "provider": "claude",
                    "created_at": NOW - 10.0,
                    "expires_at": NOW + 60.0,
                    "source": "usage_limit",
                },
                "codex": {
                    "version": 1,
                    "provider": "codex",
                    "created_at": NOW - 5.0,
                    "expires_at": null,
                    "source": "ace",
                }
            }),
        );

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.version, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION);
        assert_eq!(snapshot.disables.len(), 2);
        assert_eq!(snapshot.disables[0].provider, "claude");
        assert_eq!(snapshot.disables[0].created_at, NOW - 10.0);
        assert_eq!(snapshot.disables[0].expires_at, Some(NOW + 60.0));
        assert_eq!(snapshot.disables[0].source, "usage_limit");
        assert_eq!(snapshot.disables[0].mode, HARD);
        assert_eq!(snapshot.disables[1].provider, "codex");
        assert_eq!(snapshot.disables[1].created_at, NOW - 5.0);
        assert_eq!(snapshot.disables[1].expires_at, None);
        assert_eq!(snapshot.disables[1].source, "ace");
        assert_eq!(snapshot.disables[1].mode, HARD);

        let rewritten: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(rewritten["version"], json!(2));
        assert_eq!(rewritten["disables"]["claude"]["version"], json!(2));
        assert_eq!(rewritten["disables"]["claude"]["mode"], json!("hard"));
        assert_eq!(
            rewritten["disables"]["claude"]["created_at"],
            json!(NOW - 10.0)
        );
        assert_eq!(
            rewritten["disables"]["claude"]["expires_at"],
            json!(NOW + 60.0)
        );
        assert_eq!(
            rewritten["disables"]["claude"]["source"],
            json!("usage_limit")
        );
        assert_eq!(rewritten["disables"]["codex"]["mode"], json!("hard"));
        assert_eq!(rewritten["disables"]["codex"]["expires_at"], json!(null));
        assert_eq!(rewritten["disables"]["codex"]["source"], json!("ace"));
    }

    #[test]
    fn v1_expired_record_is_pruned_during_migration() {
        let temp = tempdir().unwrap();
        let path = provider_disable_state_path(temp.path());
        write_v1_state(
            &path,
            json!({
                "claude": {
                    "version": 1,
                    "provider": "claude",
                    "created_at": NOW - 10.0,
                    "expires_at": NOW - 1.0,
                    "source": "usage_limit",
                },
                "codex": {
                    "version": 1,
                    "provider": "codex",
                    "created_at": NOW - 10.0,
                    "expires_at": NOW + 60.0,
                    "source": "ace",
                }
            }),
        );

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.disables.len(), 1);
        assert_eq!(snapshot.disables[0].provider, "codex");
        assert_eq!(snapshot.disables[0].mode, HARD);
        let rewritten: serde_json::Value =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(rewritten["version"], json!(2));
        assert!(rewritten["disables"].get("claude").is_none());
        assert_eq!(rewritten["disables"]["codex"]["mode"], json!("hard"));
    }

    #[test]
    fn unknown_v2_mode_is_pruned_without_deleting_valid_siblings() {
        let temp = tempdir().unwrap();
        let path = provider_disable_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        fs::write(
            &path,
            serde_json::to_string(&json!({
                "version": 2,
                "disables": {
                    "claude": {
                        "version": 2,
                        "provider": "claude",
                        "created_at": NOW,
                        "expires_at": null,
                        "source": "ace",
                        "mode": "medium"
                    },
                    "codex": {
                        "version": 2,
                        "provider": "codex",
                        "created_at": NOW,
                        "expires_at": null,
                        "source": "ace",
                        "mode": "soft"
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.disables.len(), 1);
        assert_eq!(snapshot.disables[0].provider, "codex");
        assert_eq!(snapshot.disables[0].mode, SOFT);
        assert!(path.exists());
        let rewritten: ProviderDisableStateWire =
            serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert_eq!(rewritten.disables.len(), 1);
        assert!(rewritten.disables.contains_key("codex"));
    }

    #[test]
    fn v2_round_trip_get_set_try_set_and_clear_for_each_mode() {
        let temp = tempdir().unwrap();
        for mode in [HARD, SOFT] {
            let relative = set_provider_disable_relative(
                temp.path(),
                "claude",
                Some(60.0),
                "ace",
                NOW,
                mode,
            )
            .unwrap();
            assert_eq!(relative.mode, mode);
            assert_eq!(relative.version, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION);
            let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
            assert_eq!(snapshot.disables, vec![relative.clone()]);

            let until = set_provider_disable_until(
                temp.path(),
                "codex",
                NOW + 30.0,
                "usage_limit",
                NOW,
                mode,
            )
            .unwrap();
            assert_eq!(until.mode, mode);

            let lost = try_set_provider_disable_relative(
                temp.path(),
                "claude",
                Some(9_000.0),
                "other",
                NOW,
                if mode == HARD { SOFT } else { HARD },
            )
            .unwrap();
            assert!(!lost.inserted);
            assert_eq!(lost.record, relative);

            let inserted = try_set_provider_disable_until(
                temp.path(),
                "grok",
                NOW + 90.0,
                "ace",
                NOW,
                mode,
            )
            .unwrap();
            assert!(inserted.inserted);
            assert_eq!(inserted.record.mode, mode);

            assert!(clear_provider_disable(temp.path(), "claude").unwrap());
            assert!(clear_provider_disable(temp.path(), "codex").unwrap());
            assert!(clear_provider_disable(temp.path(), "grok").unwrap());
            assert!(get_provider_disables(temp.path(), NOW)
                .unwrap()
                .disables
                .is_empty());
        }
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path()).unwrap();
        let holder =
            File::create(provider_disable_lock_path(temp.path())).unwrap();
        FileExt::lock_exclusive(&holder).unwrap();
        let started = Instant::now();
        let result = get_provider_disables(temp.path(), NOW);
        assert!(matches!(result, Err(ProviderDisableError::LockTimeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        FileExt::unlock(&holder).unwrap();
    }

    #[test]
    fn first_writer_wins_under_contention_without_extending_or_losing_siblings()
    {
        let temp = tempdir().unwrap();
        let sibling = set_provider_disable_relative(
            temp.path(),
            "codex",
            Some(1_200.0),
            "sibling",
            NOW,
            SOFT,
        )
        .unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(9));
        let mut handles = vec![];
        for index in 0..8 {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                try_set_provider_disable_relative(
                    &home,
                    "claude",
                    Some(60.0 + f64::from(index)),
                    &format!("contender-{index}"),
                    NOW + f64::from(index),
                    HARD,
                )
                .unwrap()
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        let winners: Vec<_> =
            outcomes.iter().filter(|outcome| outcome.inserted).collect();
        assert_eq!(winners.len(), 1);
        let winner = winners[0];
        for outcome in &outcomes {
            assert_eq!(outcome.record, winner.record);
            assert_eq!(outcome.version, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION);
        }

        let snapshot = get_provider_disables(temp.path(), NOW).unwrap();
        assert_eq!(snapshot.disables, vec![winner.record.clone(), sibling]);
        assert_eq!(winner.record.provider, "claude");
        assert_eq!(
            winner.record.expires_at,
            Some(
                winner.record.created_at + 60.0 + {
                    let source =
                        winner.record.source.strip_prefix("contender-");
                    source.unwrap().parse::<f64>().unwrap()
                }
            )
        );
    }

    #[test]
    fn try_set_returns_existing_record_without_mutating_it() {
        let temp = tempdir().unwrap();
        let first = set_provider_disable_until(
            temp.path(),
            "claude",
            NOW + 30.0,
            "usage_limit",
            NOW,
            SOFT,
        )
        .unwrap();
        let lost = try_set_provider_disable_until(
            temp.path(),
            "claude",
            NOW + 3_600.0,
            "ace",
            NOW + 1.0,
            HARD,
        )
        .unwrap();

        assert!(!lost.inserted);
        assert_eq!(lost.record, first);
        assert_eq!(
            get_provider_disables(temp.path(), NOW).unwrap().disables,
            vec![first]
        );
    }

    #[test]
    fn try_set_replaces_an_expired_record() {
        let temp = tempdir().unwrap();
        set_provider_disable_until(
            temp.path(),
            "claude",
            NOW + 10.0,
            "usage_limit",
            NOW,
            HARD,
        )
        .unwrap();
        let replacement = try_set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(60.0),
            "usage_limit",
            NOW + 10.0,
            SOFT,
        )
        .unwrap();

        assert!(replacement.inserted);
        assert_eq!(replacement.record.created_at, NOW + 10.0);
        assert_eq!(replacement.record.expires_at, Some(NOW + 70.0));
        assert_eq!(replacement.record.mode, SOFT);
        assert_eq!(
            get_provider_disables(temp.path(), NOW + 10.0)
                .unwrap()
                .disables,
            vec![replacement.record]
        );
    }

    #[test]
    fn try_set_rejects_invalid_inputs_and_times_out_on_lock() {
        let temp = tempdir().unwrap();
        for provider in ["", " ", "claude\n"] {
            assert!(try_set_provider_disable_relative(
                temp.path(),
                provider,
                Some(1.0),
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(try_set_provider_disable_relative(
                temp.path(),
                "claude",
                Some(duration),
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        for expiry in [NOW, NOW - 1.0, f64::NAN, f64::INFINITY] {
            assert!(try_set_provider_disable_until(
                temp.path(),
                "claude",
                expiry,
                "ace",
                NOW,
                HARD
            )
            .is_err());
        }
        assert!(try_set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(1.0),
            " ",
            NOW,
            HARD
        )
        .is_err());

        fs::create_dir_all(temp.path()).unwrap();
        let holder =
            File::create(provider_disable_lock_path(temp.path())).unwrap();
        FileExt::lock_exclusive(&holder).unwrap();
        let started = Instant::now();
        let result = try_set_provider_disable_relative(
            temp.path(),
            "claude",
            Some(1.0),
            "ace",
            NOW,
            HARD,
        );
        assert!(matches!(result, Err(ProviderDisableError::LockTimeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        FileExt::unlock(&holder).unwrap();
    }
}

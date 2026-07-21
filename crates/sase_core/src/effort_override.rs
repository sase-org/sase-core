//! Machine-wide temporary default reasoning-effort override state.
//!
//! The store deliberately uses its own versioned file instead of sharing the
//! legacy provider/model override envelope.  Every operation accepts the SASE
//! home and current timestamp at the domain seam so callers can honor
//! `$SASE_HOME` and tests remain deterministic.

use crate::effort::is_valid_effort;
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const EFFORT_OVERRIDE_STATE_FILENAME: &str = "llm_effort_override.json";
const EFFORT_OVERRIDE_LOCK_FILENAME: &str = "llm_effort_override.lock";
const LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

/// Stable record returned to frontends and stored on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EffortOverrideWire {
    pub version: u32,
    pub effort: String,
    pub created_at: f64,
    pub expires_at: Option<f64>,
    pub source: String,
}

#[derive(Debug, Error)]
pub enum EffortOverrideError {
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the effort override state lock")]
    LockTimeout,
    #[error("effort override state I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("effort override state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn effort_override_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(EFFORT_OVERRIDE_STATE_FILENAME)
}

fn effort_override_lock_path(sase_home: &Path) -> PathBuf {
    sase_home.join(EFFORT_OVERRIDE_LOCK_FILENAME)
}

/// Read the active override, pruning malformed or expired state.
pub fn get_effort_override(
    sase_home: &Path,
    now: f64,
) -> Result<Option<EffortOverrideWire>, EffortOverrideError> {
    validate_now(now)?;
    with_lock(sase_home, || {
        let path = effort_override_state_path(sase_home);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        };
        let record: EffortOverrideWire = match serde_json::from_slice(&bytes) {
            Ok(record) => record,
            Err(_) => {
                remove_state_best_effort(&path);
                return Ok(None);
            }
        };
        if !is_valid_record(&record) {
            remove_state_best_effort(&path);
            return Ok(None);
        }
        if record
            .expires_at
            .is_some_and(|expires_at| now >= expires_at)
        {
            remove_state_best_effort(&path);
            return Ok(None);
        }
        Ok(Some(record))
    })
}

/// Set or replace an override for a relative duration (`None` = until clear).
pub fn set_effort_override_relative(
    sase_home: &Path,
    effort: &str,
    duration_seconds: Option<f64>,
    source: &str,
    now: f64,
) -> Result<EffortOverrideWire, EffortOverrideError> {
    validate_set_inputs(effort, source, now)?;
    if let Some(duration) = duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(EffortOverrideError::Validation(
                "duration_seconds must be finite and positive or None"
                    .to_string(),
            ));
        }
    }
    let expires_at = duration_seconds.map(|duration| now + duration);
    if expires_at.is_some_and(|expiry| !expiry.is_finite()) {
        return Err(EffortOverrideError::Validation(
            "computed expires_at must be finite".to_string(),
        ));
    }
    write_record(sase_home, effort, source, now, expires_at)
}

/// Set or replace an override until an exact future Unix timestamp.
pub fn set_effort_override_until(
    sase_home: &Path,
    effort: &str,
    expires_at: f64,
    source: &str,
    now: f64,
) -> Result<EffortOverrideWire, EffortOverrideError> {
    validate_set_inputs(effort, source, now)?;
    if !expires_at.is_finite() {
        return Err(EffortOverrideError::Validation(
            "expires_at must be finite".to_string(),
        ));
    }
    if expires_at <= now {
        return Err(EffortOverrideError::Validation(
            "expires_at must be in the future".to_string(),
        ));
    }
    write_record(sase_home, effort, source, now, Some(expires_at))
}

/// Clear override state. Missing state is a successful idempotent no-op.
pub fn clear_effort_override(
    sase_home: &Path,
) -> Result<bool, EffortOverrideError> {
    with_lock(sase_home, || {
        let path = effort_override_state_path(sase_home);
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    })
}

fn write_record(
    sase_home: &Path,
    effort: &str,
    source: &str,
    now: f64,
    expires_at: Option<f64>,
) -> Result<EffortOverrideWire, EffortOverrideError> {
    let record = EffortOverrideWire {
        version: EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION,
        effort: effort.to_string(),
        created_at: now,
        expires_at,
        source: source.trim().to_string(),
    };
    with_lock(sase_home, || {
        write_record_atomic(&effort_override_state_path(sase_home), &record)?;
        Ok(record.clone())
    })
}

fn validate_set_inputs(
    effort: &str,
    source: &str,
    now: f64,
) -> Result<(), EffortOverrideError> {
    validate_now(now)?;
    if !is_valid_effort(effort) {
        return Err(EffortOverrideError::Validation(format!(
            "invalid reasoning effort {effort:?}"
        )));
    }
    if source.trim().is_empty() {
        return Err(EffortOverrideError::Validation(
            "source must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_now(now: f64) -> Result<(), EffortOverrideError> {
    if !now.is_finite() {
        return Err(EffortOverrideError::Validation(
            "current timestamp must be finite".to_string(),
        ));
    }
    Ok(())
}

fn is_valid_record(record: &EffortOverrideWire) -> bool {
    record.version == EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION
        && is_valid_effort(&record.effort)
        && record.created_at.is_finite()
        && record.expires_at.map_or(true, f64::is_finite)
        && !record.source.trim().is_empty()
}

fn write_record_atomic(
    path: &Path,
    record: &EffortOverrideWire,
) -> Result<(), EffortOverrideError> {
    let parent = path.parent().ok_or_else(|| {
        EffortOverrideError::Validation(
            "effort override path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, record)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn remove_state_best_effort(path: &Path) {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != io::ErrorKind::NotFound {
            // Cleanup is intentionally best effort: malformed state must not
            // make launches fail merely because its removal raced or failed.
        }
    }
}

fn with_lock<T>(
    sase_home: &Path,
    operation: impl FnOnce() -> Result<T, EffortOverrideError>,
) -> Result<T, EffortOverrideError> {
    fs::create_dir_all(sase_home)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(effort_override_lock_path(sase_home))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LOCK_TIMEOUT {
                    return Err(EffortOverrideError::LockTimeout);
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
    use std::fs::File;
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;

    #[test]
    fn every_canonical_level_round_trips_and_replaces() {
        let temp = tempdir().unwrap();
        for level in crate::effort::EFFORT_LEVELS_ORDERED {
            let record = set_effort_override_relative(
                temp.path(),
                level,
                Some(900.0),
                "test",
                NOW,
            )
            .unwrap();
            assert_eq!(record.effort, *level);
            assert_eq!(record.expires_at, Some(NOW + 900.0));
            assert_eq!(
                get_effort_override(temp.path(), NOW).unwrap(),
                Some(record)
            );
        }
    }

    #[test]
    fn exact_and_no_expiry_records_obey_boundary_expiry() {
        let temp = tempdir().unwrap();
        let exact = set_effort_override_until(
            temp.path(),
            "high",
            NOW + 10.0,
            "ace",
            NOW,
        )
        .unwrap();
        assert_eq!(
            get_effort_override(temp.path(), NOW + 9.999).unwrap(),
            Some(exact)
        );
        assert_eq!(get_effort_override(temp.path(), NOW + 10.0).unwrap(), None);
        assert!(!effort_override_state_path(temp.path()).exists());

        let permanent =
            set_effort_override_relative(temp.path(), "max", None, "ace", NOW)
                .unwrap();
        assert_eq!(permanent.expires_at, None);
        assert_eq!(
            get_effort_override(temp.path(), NOW + 1_000_000.0).unwrap(),
            Some(permanent)
        );
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        let temp = tempdir().unwrap();
        for level in ["", "HIGH", "turbo"] {
            assert!(set_effort_override_relative(
                temp.path(),
                level,
                Some(1.0),
                "ace",
                NOW
            )
            .is_err());
        }
        for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(set_effort_override_relative(
                temp.path(),
                "high",
                Some(duration),
                "ace",
                NOW
            )
            .is_err());
        }
        for expiry in [NOW, NOW - 1.0, f64::NAN, f64::INFINITY] {
            assert!(set_effort_override_until(
                temp.path(),
                "high",
                expiry,
                "ace",
                NOW
            )
            .is_err());
        }
        assert!(set_effort_override_relative(
            temp.path(),
            "high",
            Some(1.0),
            " ",
            NOW
        )
        .is_err());
    }

    #[test]
    fn malformed_stale_and_missing_field_state_self_cleans() {
        let temp = tempdir().unwrap();
        let path = effort_override_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        for body in [
            "not json",
            r#"{"version":99,"effort":"high","created_at":1,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"created_at":1,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"effort":"turbo","created_at":1,"expires_at":null,"source":"test"}"#,
        ] {
            fs::write(&path, body).unwrap();
            assert_eq!(get_effort_override(temp.path(), NOW).unwrap(), None);
            assert!(!path.exists());
        }
    }

    #[test]
    fn clear_is_idempotent() {
        let temp = tempdir().unwrap();
        assert!(!clear_effort_override(temp.path()).unwrap());
        set_effort_override_relative(temp.path(), "low", None, "test", NOW)
            .unwrap();
        assert!(clear_effort_override(temp.path()).unwrap());
        assert!(!clear_effort_override(temp.path()).unwrap());
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path()).unwrap();
        let holder =
            File::create(effort_override_lock_path(temp.path())).unwrap();
        FileExt::lock_exclusive(&holder).unwrap();
        let started = Instant::now();
        let result = get_effort_override(temp.path(), NOW);
        assert!(matches!(result, Err(EffortOverrideError::LockTimeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        FileExt::unlock(&holder).unwrap();
    }
}

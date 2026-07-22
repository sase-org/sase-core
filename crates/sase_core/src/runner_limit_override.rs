//! Machine-wide temporary maximum-running-agents override state.
//!
//! The store is deliberately independent from configuration and the default
//! reasoning-effort override. Every operation accepts the SASE home and clock
//! at the domain seam so callers can honor `$SASE_HOME` and tests remain
//! deterministic.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use thiserror::Error;

pub const RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const RUNNER_LIMIT_OVERRIDE_STATE_FILENAME: &str =
    "max_running_agents_override.json";
const RUNNER_LIMIT_OVERRIDE_LOCK_FILENAME: &str =
    "max_running_agents_override.lock";
const LOCK_TIMEOUT: Duration = Duration::from_millis(250);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

/// Stable record returned to frontends and stored on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerLimitOverrideWire {
    pub version: u32,
    pub limit: u64,
    pub created_at: f64,
    pub expires_at: Option<f64>,
    pub source: String,
}

#[derive(Debug, Error)]
pub enum RunnerLimitOverrideError {
    #[error("{0}")]
    Validation(String),
    #[error("timed out waiting for the runner-limit override state lock")]
    LockTimeout,
    #[error("runner-limit override state I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("runner-limit override state serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn runner_limit_override_state_path(sase_home: &Path) -> PathBuf {
    sase_home.join(RUNNER_LIMIT_OVERRIDE_STATE_FILENAME)
}

fn runner_limit_override_lock_path(sase_home: &Path) -> PathBuf {
    sase_home.join(RUNNER_LIMIT_OVERRIDE_LOCK_FILENAME)
}

/// Read the active override, pruning malformed, invalid, or expired state.
pub fn get_runner_limit_override(
    sase_home: &Path,
    now: f64,
) -> Result<Option<RunnerLimitOverrideWire>, RunnerLimitOverrideError> {
    validate_now(now)?;
    with_lock(sase_home, || {
        let path = runner_limit_override_state_path(sase_home);
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None)
            }
            Err(error) => return Err(error.into()),
        };
        let record: RunnerLimitOverrideWire =
            match serde_json::from_slice(&bytes) {
                Ok(record) => record,
                Err(_) => {
                    remove_invalid_state(&path)?;
                    return Ok(None);
                }
            };
        if !is_valid_record(&record) {
            remove_invalid_state(&path)?;
            return Ok(None);
        }
        if record
            .expires_at
            .is_some_and(|expires_at| now >= expires_at)
        {
            remove_invalid_state(&path)?;
            return Ok(None);
        }
        Ok(Some(record))
    })
}

/// Set or replace an override for a relative duration (`None` = until clear).
pub fn set_runner_limit_override_relative(
    sase_home: &Path,
    limit: u64,
    duration_seconds: Option<f64>,
    source: &str,
    now: f64,
) -> Result<RunnerLimitOverrideWire, RunnerLimitOverrideError> {
    validate_set_inputs(limit, source, now)?;
    if let Some(duration) = duration_seconds {
        if !duration.is_finite() || duration <= 0.0 {
            return Err(RunnerLimitOverrideError::Validation(
                "duration_seconds must be finite and positive or None"
                    .to_string(),
            ));
        }
    }
    let expires_at = duration_seconds.map(|duration| now + duration);
    if expires_at.is_some_and(|expiry| !expiry.is_finite()) {
        return Err(RunnerLimitOverrideError::Validation(
            "computed expires_at must be finite".to_string(),
        ));
    }
    write_record(sase_home, limit, source, now, expires_at)
}

/// Set or replace an override until an exact future Unix timestamp.
pub fn set_runner_limit_override_until(
    sase_home: &Path,
    limit: u64,
    expires_at: f64,
    source: &str,
    now: f64,
) -> Result<RunnerLimitOverrideWire, RunnerLimitOverrideError> {
    validate_set_inputs(limit, source, now)?;
    if !expires_at.is_finite() {
        return Err(RunnerLimitOverrideError::Validation(
            "expires_at must be finite".to_string(),
        ));
    }
    if expires_at <= now {
        return Err(RunnerLimitOverrideError::Validation(
            "expires_at must be in the future".to_string(),
        ));
    }
    write_record(sase_home, limit, source, now, Some(expires_at))
}

/// Clear override state. Missing state is a successful idempotent no-op.
pub fn clear_runner_limit_override(
    sase_home: &Path,
) -> Result<bool, RunnerLimitOverrideError> {
    with_lock(sase_home, || {
        let path = runner_limit_override_state_path(sase_home);
        match fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    })
}

fn write_record(
    sase_home: &Path,
    limit: u64,
    source: &str,
    now: f64,
    expires_at: Option<f64>,
) -> Result<RunnerLimitOverrideWire, RunnerLimitOverrideError> {
    let record = RunnerLimitOverrideWire {
        version: RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION,
        limit,
        created_at: now,
        expires_at,
        source: source.trim().to_string(),
    };
    with_lock(sase_home, || {
        write_record_atomic(
            &runner_limit_override_state_path(sase_home),
            &record,
        )?;
        Ok(record.clone())
    })
}

fn validate_set_inputs(
    limit: u64,
    source: &str,
    now: f64,
) -> Result<(), RunnerLimitOverrideError> {
    validate_now(now)?;
    if limit == 0 {
        return Err(RunnerLimitOverrideError::Validation(
            "limit must be a positive integer".to_string(),
        ));
    }
    if source.trim().is_empty() {
        return Err(RunnerLimitOverrideError::Validation(
            "source must be non-empty".to_string(),
        ));
    }
    Ok(())
}

fn validate_now(now: f64) -> Result<(), RunnerLimitOverrideError> {
    if !now.is_finite() || now <= 0.0 {
        return Err(RunnerLimitOverrideError::Validation(
            "current timestamp must be finite and positive".to_string(),
        ));
    }
    Ok(())
}

fn is_valid_record(record: &RunnerLimitOverrideWire) -> bool {
    record.version == RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION
        && record.limit >= 1
        && record.created_at.is_finite()
        && record.created_at > 0.0
        && record.expires_at.map_or(true, |expires_at| {
            expires_at.is_finite() && expires_at > record.created_at
        })
        && !record.source.trim().is_empty()
}

fn write_record_atomic(
    path: &Path,
    record: &RunnerLimitOverrideWire,
) -> Result<(), RunnerLimitOverrideError> {
    let parent = path.parent().ok_or_else(|| {
        RunnerLimitOverrideError::Validation(
            "runner-limit override path has no parent directory".to_string(),
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

fn remove_invalid_state(path: &Path) -> Result<(), RunnerLimitOverrideError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn with_lock<T>(
    sase_home: &Path,
    operation: impl FnOnce() -> Result<T, RunnerLimitOverrideError>,
) -> Result<T, RunnerLimitOverrideError> {
    fs::create_dir_all(sase_home)?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(runner_limit_override_lock_path(sase_home))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LOCK_TIMEOUT {
                    return Err(RunnerLimitOverrideError::LockTimeout);
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
    fn positive_limits_round_trip_and_replace() {
        let temp = tempdir().unwrap();
        for limit in [1, 10, 128] {
            let record = set_runner_limit_override_relative(
                temp.path(),
                limit,
                Some(900.0),
                "test",
                NOW,
            )
            .unwrap();
            assert_eq!(record.limit, limit);
            assert_eq!(record.expires_at, Some(NOW + 900.0));
            assert_eq!(
                get_runner_limit_override(temp.path(), NOW).unwrap(),
                Some(record)
            );
        }
    }

    #[test]
    fn exact_and_no_expiry_records_obey_boundary_expiry() {
        let temp = tempdir().unwrap();
        let exact = set_runner_limit_override_until(
            temp.path(),
            4,
            NOW + 10.0,
            "ace",
            NOW,
        )
        .unwrap();
        assert_eq!(
            get_runner_limit_override(temp.path(), NOW + 9.999).unwrap(),
            Some(exact)
        );
        assert_eq!(
            get_runner_limit_override(temp.path(), NOW + 10.0).unwrap(),
            None
        );
        assert!(!runner_limit_override_state_path(temp.path()).exists());

        let permanent = set_runner_limit_override_relative(
            temp.path(),
            20,
            None,
            "ace",
            NOW,
        )
        .unwrap();
        assert_eq!(permanent.expires_at, None);
        assert_eq!(
            get_runner_limit_override(temp.path(), NOW + 1_000_000.0).unwrap(),
            Some(permanent)
        );
    }

    #[test]
    fn invalid_inputs_are_rejected() {
        let temp = tempdir().unwrap();
        assert!(set_runner_limit_override_relative(
            temp.path(),
            0,
            Some(1.0),
            "ace",
            NOW
        )
        .is_err());
        for duration in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(set_runner_limit_override_relative(
                temp.path(),
                1,
                Some(duration),
                "ace",
                NOW
            )
            .is_err());
        }
        for expiry in [NOW, NOW - 1.0, f64::NAN, f64::INFINITY] {
            assert!(set_runner_limit_override_until(
                temp.path(),
                1,
                expiry,
                "ace",
                NOW
            )
            .is_err());
        }
        assert!(set_runner_limit_override_relative(
            temp.path(),
            1,
            Some(1.0),
            " ",
            NOW
        )
        .is_err());
        for now in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(get_runner_limit_override(temp.path(), now).is_err());
        }
    }

    #[test]
    fn malformed_stale_and_invalid_state_self_cleans() {
        let temp = tempdir().unwrap();
        let path = runner_limit_override_state_path(temp.path());
        fs::create_dir_all(temp.path()).unwrap();
        for body in [
            "not json",
            r#"{"version":99,"limit":4,"created_at":1,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"created_at":1,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"limit":0,"created_at":1,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"limit":4,"created_at":0,"expires_at":null,"source":"test"}"#,
            r#"{"version":1,"limit":4,"created_at":2,"expires_at":1,"source":"test"}"#,
            r#"{"version":1,"limit":4,"created_at":1,"expires_at":null,"source":""}"#,
        ] {
            fs::write(&path, body).unwrap();
            assert_eq!(
                get_runner_limit_override(temp.path(), NOW).unwrap(),
                None
            );
            assert!(!path.exists());
        }
    }

    #[test]
    fn persisted_json_is_complete_and_clear_is_idempotent() {
        let temp = tempdir().unwrap();
        assert!(!clear_runner_limit_override(temp.path()).unwrap());
        let record = set_runner_limit_override_relative(
            temp.path(),
            7,
            None,
            "test",
            NOW,
        )
        .unwrap();
        let persisted: RunnerLimitOverrideWire = serde_json::from_slice(
            &fs::read(runner_limit_override_state_path(temp.path())).unwrap(),
        )
        .unwrap();
        assert_eq!(persisted, record);
        assert!(clear_runner_limit_override(temp.path()).unwrap());
        assert!(!clear_runner_limit_override(temp.path()).unwrap());
    }

    #[test]
    fn lock_wait_is_bounded() {
        let temp = tempdir().unwrap();
        fs::create_dir_all(temp.path()).unwrap();
        let holder =
            File::create(runner_limit_override_lock_path(temp.path())).unwrap();
        FileExt::lock_exclusive(&holder).unwrap();
        let started = Instant::now();
        let result = get_runner_limit_override(temp.path(), NOW);
        assert!(matches!(result, Err(RunnerLimitOverrideError::LockTimeout)));
        assert!(started.elapsed() < Duration::from_secs(2));
        FileExt::unlock(&holder).unwrap();
    }
}

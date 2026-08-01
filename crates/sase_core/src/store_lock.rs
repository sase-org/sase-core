//! Shared bounded file-lock acquisition for SASE's local stores.

use std::fs::{self, File, OpenOptions};
use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, SecondsFormat, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

const INITIAL_BACKOFF: Duration = Duration::from_millis(5);
const MAX_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::Exclusive => "exclusive",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LockHolder {
    pub(crate) pid: u32,
    pub(crate) operation: String,
    pub(crate) acquired_at: String,
}

impl std::fmt::Display for LockHolder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "pid={} operation={} acquired_at={}",
            self.pid, self.operation, self.acquired_at
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreLockError {
    #[error(
        "timed out after {waited_ms}ms waiting for {mode} lock {}; holder: {holder}",
        lock_path.display(),
        holder = holder.as_ref().map(ToString::to_string).unwrap_or_else(|| "unknown".to_string())
    )]
    Timeout {
        mode: &'static str,
        lock_path: PathBuf,
        waited_ms: u128,
        holder: Option<LockHolder>,
    },
    #[error("failed to open store lock {}: {source}", lock_path.display())]
    Open {
        lock_path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "failed to acquire {mode} store lock {}: {source}",
        lock_path.display()
    )]
    Acquire {
        mode: &'static str,
        lock_path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug)]
pub(crate) struct HeldStoreLock {
    file: File,
    holder_path: PathBuf,
    holder: LockHolder,
    waited: Duration,
    released: bool,
}

impl HeldStoreLock {
    pub(crate) fn waited_ms(&self) -> u64 {
        self.waited.as_millis().min(u128::from(u64::MAX)) as u64
    }

    pub(crate) fn release(mut self) -> io::Result<()> {
        remove_holder_if_current(&self.holder_path, &self.holder);
        let result = FileExt::unlock(&self.file);
        self.released = true;
        result
    }
}

impl Drop for HeldStoreLock {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        remove_holder_if_current(&self.holder_path, &self.holder);
        let _ = FileExt::unlock(&self.file);
    }
}

pub(crate) fn acquire_store_lock(
    lock_path: &Path,
    holder_path: &Path,
    mode: LockMode,
    timeout: Duration,
    operation: &str,
) -> Result<HeldStoreLock, StoreLockError> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|source| StoreLockError::Open {
            lock_path: lock_path.to_path_buf(),
            source,
        })?;
    wait_for_lock(file, lock_path, holder_path, mode, timeout, operation)
}

fn wait_for_lock(
    file: File,
    lock_path: &Path,
    holder_path: &Path,
    mode: LockMode,
    timeout: Duration,
    operation: &str,
) -> Result<HeldStoreLock, StoreLockError> {
    let started = Instant::now();
    let mut backoff = INITIAL_BACKOFF;
    let mut attempt = 0_u64;
    loop {
        let result = match mode {
            LockMode::Shared => FileExt::try_lock_shared(&file),
            LockMode::Exclusive => FileExt::try_lock_exclusive(&file),
        };
        match result {
            Ok(()) => {
                let acquired_at: DateTime<Utc> = SystemTime::now().into();
                let holder = LockHolder {
                    pid: std::process::id(),
                    operation: operation.to_string(),
                    acquired_at: acquired_at
                        .to_rfc3339_opts(SecondsFormat::Millis, true),
                };
                write_holder_best_effort(holder_path, &holder);
                return Ok(HeldStoreLock {
                    file,
                    holder_path: holder_path.to_path_buf(),
                    holder,
                    waited: started.elapsed(),
                    released: false,
                });
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => {
                let elapsed = started.elapsed();
                if elapsed >= timeout {
                    return Err(StoreLockError::Timeout {
                        mode: mode.label(),
                        lock_path: lock_path.to_path_buf(),
                        waited_ms: elapsed.as_millis(),
                        holder: read_holder(holder_path),
                    });
                }
                let remaining = timeout - elapsed;
                thread::sleep(
                    jittered_backoff(backoff, attempt).min(remaining),
                );
                backoff = next_backoff(backoff);
                attempt = attempt.saturating_add(1);
            }
            Err(source) => {
                return Err(StoreLockError::Acquire {
                    mode: mode.label(),
                    lock_path: lock_path.to_path_buf(),
                    source,
                });
            }
        }
    }
}

pub(crate) fn timeout_from_env(name: &str, default: Duration) -> Duration {
    std::env::var(name)
        .ok()
        .as_deref()
        .and_then(parse_timeout_seconds)
        .unwrap_or(default)
}

pub(crate) fn holder_path_for(lock_path: &Path) -> PathBuf {
    let filename = lock_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("store.lock");
    lock_path.with_file_name(format!("{filename}.holder"))
}

fn parse_timeout_seconds(value: &str) -> Option<Duration> {
    let seconds = value.trim().parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return None;
    }
    Duration::try_from_secs_f64(seconds).ok()
}

fn next_backoff(current: Duration) -> Duration {
    current.mul_f64(1.6).min(MAX_BACKOFF)
}

fn jittered_backoff(backoff: Duration, attempt: u64) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()))
        .unwrap_or(0);
    let percent = 80 + nanos.wrapping_add(attempt.wrapping_mul(37)) % 41;
    backoff.mul_f64(percent as f64 / 100.0)
}

fn write_holder_best_effort(path: &Path, holder: &LockHolder) {
    let Ok(bytes) = serde_json::to_vec(holder) else {
        return;
    };
    let _ = fs::write(path, bytes);
}

fn read_holder(path: &Path) -> Option<LockHolder> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn remove_holder_if_current(path: &Path, holder: &LockHolder) {
    if read_holder(path).as_ref() == Some(holder) {
        let _ = fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn timeout_parser_accepts_positive_floats_and_rejects_bad_values() {
        assert_eq!(
            parse_timeout_seconds("2.5"),
            Some(Duration::from_millis(2500))
        );
        for rejected in ["", "garbage", "0", "-1", "NaN", "inf"] {
            assert_eq!(parse_timeout_seconds(rejected), None, "{rejected:?}");
        }
    }

    #[test]
    fn waiter_acquires_after_more_than_the_old_two_second_bound() {
        let temp = tempdir().unwrap();
        let lock_path = temp.path().join("store.lock");
        let holder_path = temp.path().join("store.lock.holder");
        let holder = acquire_store_lock(
            &lock_path,
            &holder_path,
            LockMode::Exclusive,
            Duration::from_secs(1),
            "holder",
        )
        .unwrap();

        let waiter_lock_path = lock_path.clone();
        let waiter_holder_path = holder_path.clone();
        let waiter = thread::spawn(move || {
            acquire_store_lock(
                &waiter_lock_path,
                &waiter_holder_path,
                LockMode::Exclusive,
                Duration::from_secs(5),
                "waiter",
            )
        });
        thread::sleep(Duration::from_millis(2100));
        holder.release().unwrap();

        let acquired = waiter.join().unwrap().unwrap();
        assert!(acquired.waited_ms() >= 2000);
        acquired.release().unwrap();
    }

    #[test]
    fn timeout_names_holder_and_does_not_materially_overshoot_deadline() {
        let temp = tempdir().unwrap();
        let lock_path = temp.path().join("store.lock");
        let holder_path = temp.path().join("store.lock.holder");
        let holder = acquire_store_lock(
            &lock_path,
            &holder_path,
            LockMode::Exclusive,
            Duration::from_secs(1),
            "holding-operation",
        )
        .unwrap();

        let started = Instant::now();
        let error = acquire_store_lock(
            &lock_path,
            &holder_path,
            LockMode::Exclusive,
            Duration::from_millis(60),
            "contender",
        )
        .unwrap_err();
        let elapsed = started.elapsed();

        let StoreLockError::Timeout {
            holder: recorded, ..
        } = error
        else {
            panic!("expected timeout");
        };
        assert_eq!(
            recorded.as_ref().map(|value| value.pid),
            Some(std::process::id())
        );
        assert_eq!(
            recorded.as_ref().map(|value| value.operation.as_str()),
            Some("holding-operation")
        );
        assert!(elapsed >= Duration::from_millis(60));
        assert!(elapsed < Duration::from_millis(250));
        holder.release().unwrap();
    }
}

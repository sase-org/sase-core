//! Registry of managed SASE temp roots writers actually used.
//!
//! Where scratch lands is decided by whichever process wrote it, so the set
//! of roots to reap is whatever writers actually used, recorded durably at
//! write time. `get_sase_managed_tmpdir()` registers its resolved root here
//! (under `$SASE_HOME/managed_tmp/roots.json`); every managed-tmp reaper
//! entry point reaps the effective root plus every registered root instead
//! of only the root its own environment resolves.

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::NamedTempFile;
use thiserror::Error;

use crate::managed_tmp::validate_reap_root;

pub const MANAGED_TMP_ROOTS_WIRE_SCHEMA_VERSION: u32 = 1;
pub const MANAGED_TMP_ROOTS_RELATIVE_PATH: &str = "managed_tmp/roots.json";
const MANAGED_TMP_ROOTS_LOCK_RELATIVE_PATH: &str = "managed_tmp/roots.lock";
/// Registrations fresher than this do not rewrite `last_seen`, so repeated
/// process starts against one root cost no write amplification.
const LAST_SEEN_REWRITE_STALENESS_SECONDS: f64 = 3600.0;
const LOCK_TIMEOUT: Duration = Duration::from_secs(2);
const LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

/// One root a writer process recorded itself using.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTmpRootEntryWire {
    pub path: String,
    pub first_seen_epoch: f64,
    pub last_seen_epoch: f64,
}

/// Ordered registry snapshot returned to frontends and stored on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedTmpRootsSnapshotWire {
    pub schema_version: u32,
    pub roots: Vec<ManagedTmpRootEntryWire>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ManagedTmpRootsStateWire {
    schema_version: u32,
    roots: Vec<ManagedTmpRootEntryWire>,
}

#[derive(Debug, Error)]
pub enum ManagedTmpRootsError {
    #[error("managed temp root must be an absolute directory path, not {0}")]
    InvalidRoot(String),
    #[error(transparent)]
    UnsafeRoot(#[from] crate::managed_tmp::ManagedTmpReapError),
    #[error("timed out waiting for the managed-temp-roots lock")]
    LockTimeout,
    #[error("managed-temp-roots I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("managed-temp-roots serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn managed_tmp_roots_file_path(sase_home: &Path) -> PathBuf {
    sase_home.join(MANAGED_TMP_ROOTS_RELATIVE_PATH)
}

fn managed_tmp_roots_lock_path(sase_home: &Path) -> PathBuf {
    sase_home.join(MANAGED_TMP_ROOTS_LOCK_RELATIVE_PATH)
}

/// List registered roots. Missing or unreadable state reads as empty: the
/// registry is advisory, and readers always union it with the effective
/// root.
pub fn list_managed_tmp_roots(
    sase_home: &Path,
) -> Result<ManagedTmpRootsSnapshotWire, ManagedTmpRootsError> {
    let path = managed_tmp_roots_file_path(sase_home);
    with_lock(sase_home, || {
        Ok(snapshot_from_entries(read_entries_at(&path)?))
    })
}

/// Register *candidate* as a root writers use, merging with concurrent
/// registrations under the registry lock.
///
/// Only absolute, non-symlink directory paths are registered, and the
/// reaper's broad-root refusal applies, so a transient bad `SASE_TMPDIR`
/// can never enroll `/tmp`, `$HOME`, or `/`. Entries whose path no longer
/// exists are dropped on the next write. A registration whose `last_seen`
/// is still fresh rewrites nothing.
pub fn register_managed_tmp_root(
    sase_home: &Path,
    candidate: &Path,
    now: f64,
) -> Result<ManagedTmpRootsSnapshotWire, ManagedTmpRootsError> {
    validate_now(now)?;
    let resolved = validate_root_candidate(candidate)?;
    let key = resolved.to_string_lossy().into_owned();
    let path = managed_tmp_roots_file_path(sase_home);
    with_lock(sase_home, || {
        let before = read_entries_at(&path)?;
        let mut entries: Vec<ManagedTmpRootEntryWire> = before
            .iter()
            .filter(|entry| Path::new(&entry.path).is_dir())
            .cloned()
            .collect();
        let pruned_any = entries.len() != before.len();
        if let Some(entry) = entries.iter_mut().find(|entry| entry.path == key)
        {
            let stale = now - entry.last_seen_epoch
                > LAST_SEEN_REWRITE_STALENESS_SECONDS;
            if !stale && !pruned_any {
                return Ok(snapshot_from_entries(entries));
            }
            if stale {
                entry.last_seen_epoch = now;
            }
        } else {
            entries.push(ManagedTmpRootEntryWire {
                path: key,
                first_seen_epoch: now,
                last_seen_epoch: now,
            });
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        write_state_atomic(&path, &entries)?;
        Ok(snapshot_from_entries(entries))
    })
}

fn validate_now(now: f64) -> Result<(), ManagedTmpRootsError> {
    if !now.is_finite() || now <= 0.0 {
        return Err(ManagedTmpRootsError::InvalidRoot(format!(
            "current timestamp must be finite and positive, got {now}"
        )));
    }
    Ok(())
}

fn validate_root_candidate(
    candidate: &Path,
) -> Result<PathBuf, ManagedTmpRootsError> {
    if !candidate.is_absolute() {
        return Err(ManagedTmpRootsError::InvalidRoot(
            candidate.to_string_lossy().into_owned(),
        ));
    }
    let metadata = fs::symlink_metadata(candidate).map_err(|_| {
        ManagedTmpRootsError::InvalidRoot(
            candidate.to_string_lossy().into_owned(),
        )
    })?;
    if metadata.file_type().is_symlink() {
        return Err(ManagedTmpRootsError::InvalidRoot(
            candidate.to_string_lossy().into_owned(),
        ));
    }
    if !metadata.is_dir() {
        return Err(ManagedTmpRootsError::InvalidRoot(
            candidate.to_string_lossy().into_owned(),
        ));
    }
    // Reuse the reaper's broad-root refusal (the sase-157.1 guard): a
    // transient bad SASE_TMPDIR can never enroll TMPDIR/HOME-shaped roots.
    // Store the canonical path so comparisons, broad-root refusal, and reaping
    // all use the same identity.
    let resolved = candidate
        .canonicalize()
        .unwrap_or_else(|_| candidate.to_path_buf());
    validate_reap_root(&resolved)?;
    Ok(resolved)
}

/// Read entries from an explicit state path without repairing it. Corrupt
/// or version-mismatched state reads as empty; the next registration
/// overwrites it.
fn read_entries_at(
    path: &Path,
) -> Result<Vec<ManagedTmpRootEntryWire>, ManagedTmpRootsError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => return Err(error.into()),
    };
    let state: ManagedTmpRootsStateWire = match serde_json::from_slice(&bytes) {
        Ok(state) => state,
        Err(_) => return Ok(Vec::new()),
    };
    if state.schema_version != MANAGED_TMP_ROOTS_WIRE_SCHEMA_VERSION {
        return Ok(Vec::new());
    }
    Ok(state
        .roots
        .into_iter()
        .filter(|entry| {
            Path::new(&entry.path).is_absolute()
                && entry.first_seen_epoch.is_finite()
                && entry.last_seen_epoch.is_finite()
        })
        .collect())
}

fn snapshot_from_entries(
    entries: Vec<ManagedTmpRootEntryWire>,
) -> ManagedTmpRootsSnapshotWire {
    ManagedTmpRootsSnapshotWire {
        schema_version: MANAGED_TMP_ROOTS_WIRE_SCHEMA_VERSION,
        roots: entries,
    }
}

fn write_state_atomic(
    path: &Path,
    entries: &[ManagedTmpRootEntryWire],
) -> Result<(), ManagedTmpRootsError> {
    let parent = path.parent().ok_or_else(|| {
        ManagedTmpRootsError::InvalidRoot(
            "managed-temp-roots path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let state = ManagedTmpRootsStateWire {
        schema_version: MANAGED_TMP_ROOTS_WIRE_SCHEMA_VERSION,
        roots: entries.to_vec(),
    };
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, &state)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

fn with_lock<T>(
    sase_home: &Path,
    operation: impl FnOnce() -> Result<T, ManagedTmpRootsError>,
) -> Result<T, ManagedTmpRootsError> {
    fs::create_dir_all(sase_home.join("managed_tmp"))?;
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(managed_tmp_roots_lock_path(sase_home))?;
    let started = Instant::now();
    loop {
        match FileExt::try_lock_exclusive(&lock) {
            Ok(()) => break,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if started.elapsed() >= LOCK_TIMEOUT {
                    return Err(ManagedTmpRootsError::LockTimeout);
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
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;
    const HOUR: f64 = 3600.0;

    fn root_dir(home: &Path, name: &str) -> PathBuf {
        let path = home.join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn missing_state_lists_empty() {
        let temp = tempdir().unwrap();
        let snapshot = list_managed_tmp_roots(temp.path()).unwrap();
        assert_eq!(
            snapshot.schema_version,
            MANAGED_TMP_ROOTS_WIRE_SCHEMA_VERSION
        );
        assert!(snapshot.roots.is_empty());
    }

    #[test]
    fn register_and_list_round_trip() {
        let temp = tempdir().unwrap();
        let root = root_dir(temp.path(), "writer-root");
        let snapshot =
            register_managed_tmp_root(temp.path(), &root, NOW).unwrap();
        assert_eq!(snapshot.roots.len(), 1);
        assert_eq!(snapshot.roots[0].first_seen_epoch, NOW);
        assert_eq!(snapshot.roots[0].last_seen_epoch, NOW);

        let listed = list_managed_tmp_roots(temp.path()).unwrap();
        assert_eq!(listed, snapshot);
    }

    #[test]
    fn fresh_reregistration_rewrites_nothing() {
        let temp = tempdir().unwrap();
        let root = root_dir(temp.path(), "writer-root");
        register_managed_tmp_root(temp.path(), &root, NOW).unwrap();
        let state_path = managed_tmp_roots_file_path(temp.path());
        let before = fs::read(&state_path).unwrap();

        let snapshot =
            register_managed_tmp_root(temp.path(), &root, NOW + HOUR).unwrap();

        assert_eq!(fs::read(&state_path).unwrap(), before);
        assert_eq!(snapshot.roots[0].last_seen_epoch, NOW);
    }

    #[test]
    fn stale_reregistration_refreshes_last_seen() {
        let temp = tempdir().unwrap();
        let root = root_dir(temp.path(), "writer-root");
        register_managed_tmp_root(temp.path(), &root, NOW).unwrap();
        let snapshot =
            register_managed_tmp_root(temp.path(), &root, NOW + 2.0 * HOUR)
                .unwrap();
        assert_eq!(snapshot.roots[0].first_seen_epoch, NOW);
        assert_eq!(snapshot.roots[0].last_seen_epoch, NOW + 2.0 * HOUR);
    }

    #[test]
    fn missing_roots_are_pruned_on_next_write() {
        let temp = tempdir().unwrap();
        let gone = root_dir(temp.path(), "gone");
        let kept = root_dir(temp.path(), "kept");
        register_managed_tmp_root(temp.path(), &gone, NOW).unwrap();
        register_managed_tmp_root(temp.path(), &kept, NOW).unwrap();
        fs::remove_dir_all(&gone).unwrap();

        let snapshot =
            register_managed_tmp_root(temp.path(), &kept, NOW + 2.0 * HOUR)
                .unwrap();

        assert_eq!(
            snapshot
                .roots
                .iter()
                .map(|entry| entry.path.as_str())
                .collect::<Vec<_>>(),
            vec![kept.canonicalize().unwrap().to_string_lossy().to_string()]
        );
    }

    #[test]
    fn concurrent_registrations_merge() {
        let temp = tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = Vec::new();
        for name in ["root-a", "root-b"] {
            let dir = home.join(name);
            fs::create_dir_all(&dir).unwrap();
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                register_managed_tmp_root(&home, &dir, NOW).unwrap();
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = list_managed_tmp_roots(temp.path()).unwrap();
        assert_eq!(snapshot.roots.len(), 2);
    }

    #[test]
    fn relative_symlink_and_file_roots_are_refused() {
        let temp = tempdir().unwrap();
        let file = temp.path().join("file");
        fs::write(&file, b"x").unwrap();
        #[cfg(unix)]
        {
            let target = root_dir(temp.path(), "target");
            let link = temp.path().join("link");
            std::os::unix::fs::symlink(&target, &link).unwrap();
            assert!(matches!(
                register_managed_tmp_root(temp.path(), &link, NOW),
                Err(ManagedTmpRootsError::InvalidRoot(_))
            ));
        }
        assert!(matches!(
            register_managed_tmp_root(temp.path(), Path::new("relative"), NOW),
            Err(ManagedTmpRootsError::InvalidRoot(_))
        ));
        assert!(matches!(
            register_managed_tmp_root(temp.path(), &file, NOW),
            Err(ManagedTmpRootsError::InvalidRoot(_))
        ));
        assert!(matches!(
            register_managed_tmp_root(
                temp.path(),
                &temp.path().join("absent"),
                NOW
            ),
            Err(ManagedTmpRootsError::InvalidRoot(_))
        ));
    }

    #[test]
    fn broad_roots_are_rejected() {
        let temp = tempdir().unwrap();
        for root in [Path::new("/"), Path::new("/tmp")] {
            assert!(
                register_managed_tmp_root(temp.path(), root, NOW).is_err(),
                "root {} was not rejected",
                root.display()
            );
        }
    }

    #[test]
    fn corrupt_state_reads_empty_and_next_write_repairs() {
        let temp = tempdir().unwrap();
        let state_path = managed_tmp_roots_file_path(temp.path());
        fs::create_dir_all(state_path.parent().unwrap()).unwrap();
        fs::write(&state_path, b"not json").unwrap();

        assert!(list_managed_tmp_roots(temp.path())
            .unwrap()
            .roots
            .is_empty());

        let root = root_dir(temp.path(), "writer-root");
        let snapshot =
            register_managed_tmp_root(temp.path(), &root, NOW).unwrap();
        assert_eq!(snapshot.roots.len(), 1);
    }
}

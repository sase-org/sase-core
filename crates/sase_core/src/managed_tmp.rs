//! Managed SASE temp-root reaping.
//!
//! The host owns resolving the managed temp root, but the retention/pressure
//! decision belongs here so every frontend gets the same safety rules.  The
//! reaper only removes files or whole directories whose own metadata and all
//! descendants are stale, never follows symlinks, and refuses broad roots.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION: u32 = 1;

const TOP_LEVEL_BUCKET: &str = "<root>";

#[derive(Debug, Error)]
pub enum ManagedTmpReapError {
    #[error("managed SASE temp root must be a dedicated directory, not {0}")]
    UnsafeRoot(String),
    #[error("could not resolve current directory: {0}")]
    CurrentDir(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedTmpReapRequestWire {
    pub schema_version: u32,
    pub root: String,
    pub now_epoch_seconds: f64,
    pub horizons: BTreeMap<String, f64>,
    pub default_horizon_seconds: f64,
    pub max_removals: u32,
    pub pressure_max_bytes: Option<u64>,
    pub pressure_target_bytes: u64,
    pub pressure_min_available_bytes: Option<u64>,
    pub pressure_recovery_available_bytes: u64,
    pub pressure_min_age_seconds: f64,
    #[serde(default)]
    pub pressure_low_free_space_min_age_seconds: Option<f64>,
    pub pressure_min_entry_bytes: u64,
    pub pressure_reap_buckets: Vec<String>,
    pub filesystem_available_bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ManagedTmpReapResultWire {
    pub schema_version: u32,
    pub root: String,
    pub scanned: u64,
    pub removed: u64,
    pub removed_by_subdir: BTreeMap<String, u64>,
    pub removed_directories: Vec<String>,
    pub capped: bool,
    pub pressure_removed: u64,
    pub pressure_reclaimed_bytes: u64,
    pub pressure_trigger: Option<String>,
    pub pressure_root_size_bytes: u64,
    pub pressure_available_bytes: Option<u64>,
    pub pressure_recovery_available_bytes: u64,
    #[serde(default)]
    pub pressure_effective_min_age_seconds: Option<f64>,
}

#[derive(Debug, Clone)]
struct RemovalOutcome {
    kind: RemovedKind,
    size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovedKind {
    File,
    Directory,
}

#[derive(Debug, Clone)]
struct EntrySnapshot {
    is_file: bool,
    is_dir: bool,
    is_symlink: bool,
    size_bytes: u64,
    latest_mtime: f64,
}

#[derive(Debug, Clone)]
struct PressureCandidate {
    path: PathBuf,
    bucket: String,
    size_bytes: u64,
    latest_mtime: f64,
}

#[derive(Debug, Clone)]
struct PressurePlan {
    trigger: Option<&'static str>,
    root_size: u64,
    available: Option<u64>,
    free_space_floor_breached: bool,
    target_size: u64,
    recovery_available: u64,
}

#[derive(Debug, Default)]
struct PressureReapResult {
    scanned: u64,
    removed: u64,
    removed_by_subdir: BTreeMap<String, u64>,
    removed_directories: Vec<PathBuf>,
    reclaimed_bytes: u64,
    capped: bool,
    trigger: Option<String>,
    root_size: u64,
    available: Option<u64>,
    recovery_available: u64,
    effective_min_age_seconds: Option<f64>,
}

pub fn reap_managed_tmpdir(
    request: &ManagedTmpReapRequestWire,
) -> Result<ManagedTmpReapResultWire, ManagedTmpReapError> {
    let root = validate_reap_root(Path::new(&request.root))?;
    let root_string = root.to_string_lossy().into_owned();
    let clock = request.now_epoch_seconds;
    let mut scanned = 0_u64;
    let mut removed_by_subdir: BTreeMap<String, u64> = BTreeMap::new();
    let mut removed_directories: Vec<PathBuf> = Vec::new();
    let mut budget = u64::from(request.max_removals);
    let mut capped = false;

    for entry in iter_children(&root) {
        if budget == 0 {
            capped = true;
            break;
        }

        let candidates = match entry_snapshot(&entry) {
            Some(snapshot) if snapshot.is_dir && !snapshot.is_symlink => {
                let horizon = request
                    .horizons
                    .get(path_name(&entry).as_str())
                    .copied()
                    .unwrap_or(request.default_horizon_seconds);
                iter_children(&entry)
                    .into_iter()
                    .map(|child| (child, clock - horizon, path_name(&entry)))
                    .collect::<Vec<_>>()
            }
            _ => vec![(
                entry,
                clock - request.default_horizon_seconds,
                TOP_LEVEL_BUCKET.to_string(),
            )],
        };

        for (candidate, cutoff, bucket) in candidates {
            if budget == 0 {
                capped = true;
                break;
            }
            scanned += 1;
            let Some(outcome) = remove_if_stale(&candidate, cutoff) else {
                continue;
            };
            if outcome.kind == RemovedKind::Directory {
                removed_directories.push(candidate);
            }
            *removed_by_subdir.entry(bucket).or_insert(0) += 1;
            budget -= 1;
        }
    }

    let mut pressure = PressureReapResult::default();
    if budget > 0 {
        pressure = reap_pressure_candidates(&root, request, budget);
        scanned += pressure.scanned;
        capped = capped || pressure.capped;
        removed_directories.extend(pressure.removed_directories);
        for (bucket, count) in pressure.removed_by_subdir {
            *removed_by_subdir.entry(bucket).or_insert(0) += count;
        }
    } else {
        pressure.root_size = tree_snapshot(&root)
            .map(|snapshot| snapshot.size_bytes)
            .unwrap_or(0);
        pressure.available = filesystem_available_bytes(&root)
            .or(request.filesystem_available_bytes);
        pressure.recovery_available = request.pressure_recovery_available_bytes;
    }

    let removed = removed_by_subdir.values().sum();
    Ok(ManagedTmpReapResultWire {
        schema_version: MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION,
        root: root_string,
        scanned,
        removed,
        removed_by_subdir,
        removed_directories: removed_directories
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect(),
        capped,
        pressure_removed: pressure.removed,
        pressure_reclaimed_bytes: pressure.reclaimed_bytes,
        pressure_trigger: pressure.trigger,
        pressure_root_size_bytes: pressure.root_size,
        pressure_available_bytes: pressure.available,
        pressure_recovery_available_bytes: pressure.recovery_available,
        pressure_effective_min_age_seconds: pressure.effective_min_age_seconds,
    })
}

fn reap_pressure_candidates(
    root: &Path,
    request: &ManagedTmpReapRequestWire,
    current_budget: u64,
) -> PressureReapResult {
    let Some(plan) = pressure_plan(root, request) else {
        let root_size = tree_snapshot(root)
            .map(|snapshot| snapshot.size_bytes)
            .unwrap_or(0);
        return PressureReapResult {
            root_size,
            available: request
                .filesystem_available_bytes
                .or_else(|| filesystem_available_bytes(root)),
            recovery_available: request.pressure_recovery_available_bytes,
            ..PressureReapResult::default()
        };
    };
    if current_budget == 0 {
        let effective_min_age_seconds =
            pressure_effective_min_age_seconds(request, &plan);
        return PressureReapResult {
            capped: true,
            trigger: plan.trigger.map(str::to_string),
            root_size: plan.root_size,
            available: plan.available,
            recovery_available: plan.recovery_available,
            effective_min_age_seconds: Some(effective_min_age_seconds),
            ..PressureReapResult::default()
        };
    }

    let buckets = request
        .pressure_reap_buckets
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let effective_min_age_seconds =
        pressure_effective_min_age_seconds(request, &plan);
    let cutoff = request.now_epoch_seconds - effective_min_age_seconds;
    let mut candidates = Vec::new();
    let mut scanned = 0_u64;
    for entry in iter_children(root) {
        let name = path_name(&entry);
        if buckets.contains(&name)
            && entry_snapshot(&entry)
                .is_some_and(|snapshot| snapshot.is_dir && !snapshot.is_symlink)
        {
            for child in iter_children(&entry) {
                scanned += 1;
                if let Some(candidate) = pressure_candidate(
                    &child,
                    &name,
                    cutoff,
                    request.pressure_min_entry_bytes,
                ) {
                    candidates.push(candidate);
                }
            }
            continue;
        }

        if !request.horizons.contains_key(&name)
            && is_build_target_residue_name(&name)
        {
            scanned += 1;
            if let Some(candidate) = pressure_candidate(
                &entry,
                TOP_LEVEL_BUCKET,
                cutoff,
                request.pressure_min_entry_bytes,
            ) {
                candidates.push(candidate);
            }
        }
    }

    candidates.sort_by(|left, right| {
        right
            .size_bytes
            .cmp(&left.size_bytes)
            .then_with(|| {
                left.latest_mtime
                    .partial_cmp(&right.latest_mtime)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.path.cmp(&right.path))
    });

    let mut budget = current_budget;
    let mut estimated_size = plan.root_size;
    let mut estimated_available = plan.available;
    let mut removed = 0_u64;
    let mut reclaimed_bytes = 0_u64;
    let mut removed_by_subdir = BTreeMap::new();
    let mut removed_directories = Vec::new();
    let mut capped = false;

    for candidate in candidates {
        if pressure_recovered(
            plan.trigger,
            estimated_size,
            plan.target_size,
            estimated_available,
            plan.recovery_available,
        ) {
            break;
        }
        if budget == 0 {
            capped = true;
            break;
        }
        let Some(outcome) = remove_if_stale(&candidate.path, cutoff) else {
            continue;
        };
        if outcome.kind == RemovedKind::Directory {
            removed_directories.push(candidate.path);
        }
        removed += 1;
        reclaimed_bytes = reclaimed_bytes.saturating_add(candidate.size_bytes);
        *removed_by_subdir.entry(candidate.bucket).or_insert(0) += 1;
        estimated_size = estimated_size.saturating_sub(outcome.size_bytes);
        estimated_available = estimated_available
            .map(|free| free.saturating_add(outcome.size_bytes));
        budget -= 1;
    }

    PressureReapResult {
        scanned,
        removed,
        removed_by_subdir,
        removed_directories,
        reclaimed_bytes,
        capped,
        trigger: plan.trigger.map(str::to_string),
        root_size: plan.root_size,
        available: plan.available,
        recovery_available: plan.recovery_available,
        effective_min_age_seconds: Some(effective_min_age_seconds),
    }
}

fn pressure_plan(
    root: &Path,
    request: &ManagedTmpReapRequestWire,
) -> Option<PressurePlan> {
    let root_size = tree_snapshot(root)
        .map(|snapshot| snapshot.size_bytes)
        .unwrap_or(0);
    let available = request
        .filesystem_available_bytes
        .or_else(|| filesystem_available_bytes(root));
    let size_pressure = request
        .pressure_max_bytes
        .is_some_and(|max| root_size > max);
    let free_pressure = request
        .pressure_min_available_bytes
        .zip(available)
        .is_some_and(|(minimum, observed)| observed < minimum);

    if !size_pressure && !free_pressure {
        return None;
    }

    let target_size = request
        .pressure_max_bytes
        .map(|max| request.pressure_target_bytes.min(max))
        .unwrap_or(request.pressure_target_bytes);
    let trigger = match (size_pressure, free_pressure) {
        (true, true) => "size_and_free_space",
        (true, false) => "size",
        (false, true) => "free_space",
        (false, false) => unreachable!(),
    };

    Some(PressurePlan {
        trigger: Some(trigger),
        root_size,
        available,
        free_space_floor_breached: free_pressure,
        target_size,
        recovery_available: request.pressure_recovery_available_bytes,
    })
}

fn pressure_effective_min_age_seconds(
    request: &ManagedTmpReapRequestWire,
    plan: &PressurePlan,
) -> f64 {
    if !plan.free_space_floor_breached {
        return request.pressure_min_age_seconds;
    }
    request
        .pressure_low_free_space_min_age_seconds
        .map(|low_space_min_age| {
            request
                .pressure_min_age_seconds
                .min(low_space_min_age.max(0.0))
        })
        .unwrap_or(request.pressure_min_age_seconds)
}

fn pressure_recovered(
    trigger: Option<&str>,
    estimated_size: u64,
    target_size: u64,
    estimated_available: Option<u64>,
    recovery_available: u64,
) -> bool {
    match trigger {
        Some("size") => estimated_size <= target_size,
        Some("free_space") => estimated_available
            .is_some_and(|available| available >= recovery_available),
        Some("size_and_free_space") => {
            estimated_size <= target_size
                && estimated_available
                    .is_some_and(|available| available >= recovery_available)
        }
        _ => true,
    }
}

fn validate_reap_root(root: &Path) -> Result<PathBuf, ManagedTmpReapError> {
    let resolved = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let cwd = std::env::current_dir()
        .map_err(|error| ManagedTmpReapError::CurrentDir(error.to_string()))?
        .canonicalize()
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    for unsafe_root in
        [Path::new("/"), Path::new("/tmp"), Path::new("/var/tmp")]
    {
        if resolved == unsafe_root {
            return Err(ManagedTmpReapError::UnsafeRoot(
                resolved.to_string_lossy().into_owned(),
            ));
        }
    }
    if resolved == cwd || cwd.starts_with(&resolved) {
        return Err(ManagedTmpReapError::UnsafeRoot(
            resolved.to_string_lossy().into_owned(),
        ));
    }
    Ok(resolved)
}

fn pressure_candidate(
    path: &Path,
    bucket: &str,
    cutoff: f64,
    min_entry_bytes: u64,
) -> Option<PressureCandidate> {
    let snapshot = tree_snapshot(path)?;
    if snapshot.is_symlink || !(snapshot.is_file || snapshot.is_dir) {
        return None;
    }
    if snapshot.latest_mtime >= cutoff {
        return None;
    }
    if snapshot.size_bytes < min_entry_bytes {
        return None;
    }
    Some(PressureCandidate {
        path: path.to_path_buf(),
        bucket: bucket.to_string(),
        size_bytes: snapshot.size_bytes,
        latest_mtime: snapshot.latest_mtime,
    })
}

fn remove_if_stale(path: &Path, cutoff: f64) -> Option<RemovalOutcome> {
    let snapshot = tree_snapshot(path)?;
    if snapshot.is_symlink || snapshot.latest_mtime >= cutoff {
        return None;
    }

    if snapshot.is_dir {
        if fs::remove_dir_all(path).is_ok() {
            return Some(RemovalOutcome {
                kind: RemovedKind::Directory,
                size_bytes: snapshot.size_bytes,
            });
        }
        return None;
    }
    if snapshot.is_file && fs::remove_file(path).is_ok() {
        return Some(RemovalOutcome {
            kind: RemovedKind::File,
            size_bytes: snapshot.size_bytes,
        });
    }
    None
}

fn tree_snapshot(path: &Path) -> Option<EntrySnapshot> {
    let metadata = fs::symlink_metadata(path).ok()?;
    let is_file = metadata.is_file();
    let is_dir = metadata.is_dir();
    let is_symlink = metadata.file_type().is_symlink();
    let own_mtime = metadata_mtime_seconds(&metadata).unwrap_or(0.0);
    if is_symlink {
        return Some(EntrySnapshot {
            is_file: false,
            is_dir: false,
            is_symlink: true,
            size_bytes: 0,
            latest_mtime: own_mtime,
        });
    }
    if is_file {
        return Some(EntrySnapshot {
            is_file,
            is_dir,
            is_symlink,
            size_bytes: metadata.len(),
            latest_mtime: own_mtime,
        });
    }
    if !is_dir {
        return Some(EntrySnapshot {
            is_file,
            is_dir,
            is_symlink,
            size_bytes: 0,
            latest_mtime: own_mtime,
        });
    }

    let mut size_bytes = 0_u64;
    let mut latest_mtime = own_mtime;
    for child in iter_children(path) {
        if let Some(child_snapshot) = tree_snapshot(&child) {
            size_bytes = size_bytes.saturating_add(child_snapshot.size_bytes);
            latest_mtime = latest_mtime.max(child_snapshot.latest_mtime);
        }
    }
    Some(EntrySnapshot {
        is_file,
        is_dir,
        is_symlink,
        size_bytes,
        latest_mtime,
    })
}

fn entry_snapshot(path: &Path) -> Option<EntrySnapshot> {
    let metadata = fs::symlink_metadata(path).ok()?;
    Some(EntrySnapshot {
        is_file: metadata.is_file(),
        is_dir: metadata.is_dir(),
        is_symlink: metadata.file_type().is_symlink(),
        size_bytes: metadata.len(),
        latest_mtime: metadata_mtime_seconds(&metadata).unwrap_or(0.0),
    })
}

fn iter_children(directory: &Path) -> Vec<PathBuf> {
    fs::read_dir(directory)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect()
        })
        .unwrap_or_default()
}

fn path_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string()
}

fn is_build_target_residue_name(name: &str) -> bool {
    name.contains("cargo-target")
        || name.contains("core-target")
        || name.ends_with("-target")
        || name == "target"
}

#[cfg(unix)]
fn metadata_mtime_seconds(metadata: &fs::Metadata) -> Option<f64> {
    use std::os::unix::fs::MetadataExt;
    Some(
        metadata.mtime() as f64
            + metadata.mtime_nsec() as f64 / 1_000_000_000.0,
    )
}

#[cfg(not(unix))]
fn metadata_mtime_seconds(metadata: &fs::Metadata) -> Option<f64> {
    metadata
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs_f64())
}

#[cfg(unix)]
fn filesystem_available_bytes(path: &Path) -> Option<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), stats.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let stats = unsafe { stats.assume_init() };
    // statvfs field widths are platform-dependent (f_bavail is u32 on
    // macOS, u64 on Linux); widen before multiplying, then saturate back.
    let available = u128::from(stats.f_bavail) * u128::from(stats.f_frsize);
    Some(u64::try_from(available).unwrap_or(u64::MAX))
}

#[cfg(not(unix))]
fn filesystem_available_bytes(_path: &Path) -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    const NOW: f64 = 1_800_000_000.0;
    const HOUR: f64 = 3600.0;
    const DAY: f64 = 24.0 * HOUR;

    fn request(root: &Path) -> ManagedTmpReapRequestWire {
        ManagedTmpReapRequestWire {
            schema_version: MANAGED_TMP_REAP_WIRE_SCHEMA_VERSION,
            root: root.to_string_lossy().into_owned(),
            now_epoch_seconds: NOW,
            horizons: BTreeMap::from([
                ("agent-tmp".to_string(), 12.0 * HOUR),
                ("cargo-targets".to_string(), 3.0 * DAY),
                ("handoff".to_string(), 3.0 * DAY),
                ("workflow-artifacts".to_string(), 14.0 * DAY),
            ]),
            default_horizon_seconds: 3.0 * DAY,
            max_removals: 2000,
            pressure_max_bytes: Some(16 * 1024),
            pressure_target_bytes: 8 * 1024,
            pressure_min_available_bytes: Some(32 * 1024),
            pressure_recovery_available_bytes: 48 * 1024,
            pressure_min_age_seconds: 12.0 * HOUR,
            pressure_low_free_space_min_age_seconds: None,
            pressure_min_entry_bytes: 1024,
            pressure_reap_buckets: vec![
                "build-targets".to_string(),
                "cargo-targets".to_string(),
            ],
            filesystem_available_bytes: None,
        }
    }

    fn aged_file(
        root: &Path,
        relative: &str,
        age_seconds: f64,
        bytes: usize,
    ) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file = File::create(&path).unwrap();
        file.write_all(&vec![b'x'; bytes]).unwrap();
        set_mtime(&path, NOW - age_seconds);
        path
    }

    fn aged_dir(
        root: &Path,
        relative: &str,
        age_seconds: f64,
        bytes: usize,
    ) -> PathBuf {
        let path = root.join(relative);
        fs::create_dir_all(&path).unwrap();
        aged_file(root, &format!("{relative}/payload.bin"), age_seconds, bytes);
        set_mtime(&path, NOW - age_seconds);
        path
    }

    #[cfg(unix)]
    fn set_mtime(path: &Path, timestamp: f64) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let seconds = timestamp.trunc() as libc::time_t;
        let nanos = ((timestamp.fract()) * 1_000_000_000.0) as libc::c_long;
        let times = [
            libc::timespec {
                tv_sec: seconds,
                tv_nsec: nanos,
            },
            libc::timespec {
                tv_sec: seconds,
                tv_nsec: nanos,
            },
        ];
        let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let rc = unsafe {
            libc::utimensat(libc::AT_FDCWD, c_path.as_ptr(), times.as_ptr(), 0)
        };
        assert_eq!(rc, 0, "utimensat({}) failed", path.display());
    }

    #[cfg(not(unix))]
    fn set_mtime(_path: &Path, _timestamp: f64) {}

    #[test]
    fn unknown_bucket_survives_while_stale_child_is_pruned() {
        let temp = tempdir().unwrap();
        let stale = aged_file(temp.path(), "future/old.tmp", 4.0 * DAY, 8);
        let fresh = aged_file(temp.path(), "future/new.tmp", HOUR, 8);
        set_mtime(&temp.path().join("future"), NOW - 13.0 * HOUR);

        let result = reap_managed_tmpdir(&request(temp.path())).unwrap();

        assert!(temp.path().join("future").is_dir());
        assert!(!stale.exists());
        assert!(fresh.exists());
        assert_eq!(result.removed_by_subdir.get("future"), Some(&1));
    }

    #[test]
    fn pressure_does_not_delete_unknown_bucket_with_fresh_child() {
        let temp = tempdir().unwrap();
        let bucket =
            aged_dir(temp.path(), "unknown-bucket/fresh-handoff", HOUR, 4096);
        set_mtime(&temp.path().join("unknown-bucket"), NOW - 13.0 * HOUR);
        let mut req = request(temp.path());
        req.horizons.clear();
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_entry_bytes = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(bucket.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_trigger.as_deref(), Some("size"));
    }

    #[test]
    fn free_space_pressure_reaps_build_scratch_below_size_ceiling() {
        let temp = tempdir().unwrap();
        let old_large =
            aged_dir(temp.path(), "cargo-targets/run-old", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!old_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_trigger.as_deref(), Some("free_space"));
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_free_space_age_reaps_recent_large_target_when_size_also_triggers() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(
            result.pressure_trigger.as_deref(),
            Some("size_and_free_space")
        );
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn low_free_space_age_reaps_recent_large_target_under_free_space_trigger() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_trigger.as_deref(), Some("free_space"));
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn low_free_space_age_does_not_apply_to_size_only_pressure() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(1024);
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(recent_large.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_trigger.as_deref(), Some("size"));
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );

        set_mtime(&recent_large, NOW - DAY);
        set_mtime(&recent_large.join("payload.bin"), NOW - DAY);
        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!recent_large.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_free_space_age_still_respects_fresh_descendant() {
        let temp = tempdir().unwrap();
        let target =
            aged_dir(temp.path(), "cargo-targets/run-live", 2.0 * HOUR, 8192);
        let fresh = aged_file(
            temp.path(),
            "cargo-targets/run-live/deep/object.o",
            0.5 * HOUR,
            1,
        );
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.pressure_low_free_space_min_age_seconds = Some(HOUR);
        req.pressure_min_entry_bytes = 1;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(target.exists());
        assert!(fresh.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn absent_low_free_space_age_preserves_base_pressure_age() {
        let temp = tempdir().unwrap();
        let recent_large =
            aged_dir(temp.path(), "cargo-targets/run-recent", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 16 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(recent_large.exists());
        assert_eq!(result.pressure_removed, 0);
        assert_eq!(
            result.pressure_trigger.as_deref(),
            Some("size_and_free_space")
        );
        assert_eq!(
            result.pressure_effective_min_age_seconds,
            Some(12.0 * HOUR)
        );
    }

    #[test]
    fn low_space_age_never_lengthens_base_pressure_age() {
        let temp = tempdir().unwrap();
        let two_hour_target =
            aged_dir(temp.path(), "cargo-targets/two-hour", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(1024);
        req.pressure_target_bytes = 0;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_low_free_space_min_age_seconds = Some(DAY);
        req.filesystem_available_bytes = Some(8 * 1024);
        req.pressure_min_age_seconds = HOUR;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!two_hour_target.exists());
        assert_eq!(result.pressure_removed, 1);
        assert_eq!(result.pressure_effective_min_age_seconds, Some(HOUR));
    }

    #[test]
    fn pressure_stops_at_free_space_recovery_threshold() {
        let temp = tempdir().unwrap();
        let largest = aged_dir(temp.path(), "cargo-targets/largest", DAY, 8192);
        let smaller = aged_dir(temp.path(), "cargo-targets/smaller", DAY, 4096);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(64 * 1024);
        req.pressure_target_bytes = 32 * 1024;
        req.pressure_min_available_bytes = Some(10 * 1024);
        req.pressure_recovery_available_bytes = 15 * 1024;
        req.filesystem_available_bytes = Some(8 * 1024);

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!largest.exists());
        assert!(smaller.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn pressure_orders_largest_first_and_oldest_within_equal_size() {
        let temp = tempdir().unwrap();
        let smaller =
            aged_dir(temp.path(), "cargo-targets/smaller", 3.0 * DAY, 4096);
        let newer_equal =
            aged_dir(temp.path(), "cargo-targets/newer-equal", DAY, 8192);
        let older_equal =
            aged_dir(temp.path(), "cargo-targets/older-equal", 2.0 * DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(16 * 1024);
        req.pressure_target_bytes = 12 * 1024;
        req.pressure_min_available_bytes = None;
        req.max_removals = 2;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(smaller.exists());
        assert!(newer_equal.exists());
        assert!(!older_equal.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn live_build_tree_with_fresh_descendant_survives_pressure() {
        let temp = tempdir().unwrap();
        let target = aged_dir(temp.path(), "cargo-targets/run-live", DAY, 8192);
        let fresh = aged_file(
            temp.path(),
            "cargo-targets/run-live/deep/object.o",
            HOUR,
            1,
        );
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_entry_bytes = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(target.exists());
        assert!(fresh.exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn expired_build_tree_without_fresh_descendant_is_removed() {
        let temp = tempdir().unwrap();
        let target = aged_dir(temp.path(), "cargo-targets/run-dead", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(!target.exists());
        assert_eq!(result.pressure_removed, 1);
    }

    #[test]
    fn pressure_skips_generic_agent_tmp_and_handoff_buckets() {
        let temp = tempdir().unwrap();
        let agent_tmp =
            aged_dir(temp.path(), "agent-tmp/payload", 2.0 * HOUR, 8192);
        let handoff =
            aged_dir(temp.path(), "handoff/payload", 2.0 * HOUR, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.pressure_min_age_seconds = HOUR;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(agent_tmp.exists());
        assert!(handoff.exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn pressure_keeps_symlink_entries() {
        let temp = tempdir().unwrap();
        let outside = tempdir().unwrap();
        let bucket = temp.path().join("cargo-targets");
        fs::create_dir(&bucket).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(outside.path(), bucket.join("linked"))
            .unwrap();
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert!(bucket.join("linked").exists());
        assert_eq!(result.pressure_removed, 0);
    }

    #[test]
    fn pressure_respects_removal_budget() {
        let temp = tempdir().unwrap();
        aged_dir(temp.path(), "cargo-targets/one", DAY, 8192);
        aged_dir(temp.path(), "cargo-targets/two", DAY, 8192);
        let mut req = request(temp.path());
        req.pressure_max_bytes = Some(100);
        req.pressure_target_bytes = 50;
        req.max_removals = 1;

        let result = reap_managed_tmpdir(&req).unwrap();

        assert_eq!(result.pressure_removed, 1);
        assert!(result.capped);
    }

    #[test]
    fn broad_cleanup_roots_are_rejected() {
        let mut req = request(Path::new("/tmp"));
        req.root = "/tmp".to_string();

        let error = reap_managed_tmpdir(&req).unwrap_err();

        assert!(error.to_string().contains("dedicated directory"));
    }
}

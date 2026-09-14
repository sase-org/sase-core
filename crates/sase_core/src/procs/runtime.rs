use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use crate::store_lock::LockMode;

use super::store::{
    is_active_status, lock_with_timeout, proc_store_lock_timeout,
    read_rows_unlocked, unlock,
};
use super::wire::{
    ProcRuntimeRetentionEntryWire, ProcRuntimeRetentionRequestWire,
    ProcRuntimeRetentionResultWire, PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION,
};
use super::ProcStoreError;

const PROC_ID_LENGTH: usize = 12;
const PROC_ID_ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";

#[derive(Debug, Clone)]
struct EntrySnapshot {
    is_dir: bool,
    is_symlink: bool,
    size_bytes: u64,
    latest_mtime: f64,
}

pub fn apply_proc_runtime_retention(
    request: &ProcRuntimeRetentionRequestWire,
) -> Result<ProcRuntimeRetentionResultWire, ProcStoreError> {
    if request.schema_version != PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION {
        return Err(ProcStoreError::Store(format!(
            "proc runtime retention requires schema_version {}",
            PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION
        )));
    }
    if !request.now_epoch_seconds.is_finite()
        || request.orphan_horizon_seconds < 0.0
        || !request.orphan_horizon_seconds.is_finite()
    {
        return Err(ProcStoreError::Store(
            "proc runtime retention received invalid clock or horizon"
                .to_string(),
        ));
    }

    let store_path = Path::new(&request.store_path);
    let runtime_root = Path::new(&request.runtime_root);
    let mut result =
        ProcRuntimeRetentionResultWire::new(runtime_root, request.apply);
    match root_status(runtime_root) {
        RootStatus::Missing => return Ok(result),
        RootStatus::Ready => {}
        RootStatus::Unsafe(reason) => {
            return Err(ProcStoreError::Store(reason))
        }
    }

    let lock = lock_with_timeout(
        store_path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "proc_runtime_retention",
    )?;
    let retention_result = (|| {
        let (rows, _) = read_rows_unlocked(store_path)?;
        let retained_ids = rows
            .iter()
            .map(|row| row.proc_id.clone())
            .collect::<BTreeSet<_>>();
        let active_ids = rows
            .iter()
            .filter(|row| is_active_status(&row.status))
            .map(|row| row.proc_id.clone())
            .collect::<BTreeSet<_>>();
        let context = CandidateEvaluationContext {
            runtime_root,
            retained_ids: &retained_ids,
            active_ids: &active_ids,
            apply: request.apply,
        };

        for proc_id in &request.pruned_proc_ids {
            evaluate_candidate(
                &context,
                proc_id,
                CandidateKind::PrunedRow,
                None,
                &mut result,
            );
        }

        if request.sweep_orphans {
            sweep_orphans(request, &context, &mut result)?;
        }

        Ok::<_, ProcStoreError>(result)
    })();
    unlock(lock)?;
    retention_result
}

fn sweep_orphans(
    request: &ProcRuntimeRetentionRequestWire,
    context: &CandidateEvaluationContext<'_>,
    result: &mut ProcRuntimeRetentionResultWire,
) -> Result<(), ProcStoreError> {
    let mut paths = fs::read_dir(context.runtime_root)
        .map_err(|error| ProcStoreError::Store(error.to_string()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();

    let cutoff = request.now_epoch_seconds - request.orphan_horizon_seconds;
    let mut selected_orphans = 0_u32;
    for (index, path) in paths.iter().enumerate() {
        if selected_orphans >= request.max_orphan_removals {
            result.capped = paths[index..].iter().any(|candidate| {
                orphan_would_select(
                    context.runtime_root,
                    candidate,
                    context.retained_ids,
                    context.active_ids,
                    cutoff,
                )
            });
            break;
        }
        let proc_id = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .to_string();
        let before = result.selected;
        evaluate_candidate(
            context,
            &proc_id,
            CandidateKind::Orphan { cutoff },
            Some(path.clone()),
            result,
        );
        if result.selected > before {
            selected_orphans = selected_orphans.saturating_add(1);
        }
    }
    Ok(())
}

fn orphan_would_select(
    runtime_root: &Path,
    path: &Path,
    retained_ids: &BTreeSet<String>,
    active_ids: &BTreeSet<String>,
    cutoff: f64,
) -> bool {
    let proc_id = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !valid_proc_id(proc_id)
        || !is_direct_child(runtime_root, path)
        || active_ids.contains(proc_id)
        || retained_ids.contains(proc_id)
    {
        return false;
    }
    tree_snapshot(path).is_some_and(|snapshot| {
        snapshot.is_dir
            && !snapshot.is_symlink
            && snapshot.latest_mtime < cutoff
    })
}

#[derive(Debug, Clone, Copy)]
enum CandidateKind {
    PrunedRow,
    Orphan { cutoff: f64 },
}

#[derive(Debug, Clone, Copy)]
struct CandidateEvaluationContext<'a> {
    runtime_root: &'a Path,
    retained_ids: &'a BTreeSet<String>,
    active_ids: &'a BTreeSet<String>,
    apply: bool,
}

fn evaluate_candidate(
    context: &CandidateEvaluationContext<'_>,
    proc_id: &str,
    kind: CandidateKind,
    observed_path: Option<PathBuf>,
    result: &mut ProcRuntimeRetentionResultWire,
) {
    result.scanned = result.scanned.saturating_add(1);
    let path =
        observed_path.unwrap_or_else(|| context.runtime_root.join(proc_id));
    let path_string = path.to_string_lossy().into_owned();

    if !valid_proc_id(proc_id) {
        result.push_skip(proc_id, path_string, "invalid_proc_id", 0);
        return;
    }
    if !is_direct_child(context.runtime_root, &path) {
        result.push_skip(proc_id, path_string, "outside_runtime_root", 0);
        return;
    }
    if context.active_ids.contains(proc_id) {
        result.push_skip(proc_id, path_string, "active_proc_row", 0);
        return;
    }
    if context.retained_ids.contains(proc_id) {
        result.push_skip(proc_id, path_string, "retained_proc_row", 0);
        return;
    }

    let Some(snapshot) = tree_snapshot(&path) else {
        result.push_skip(proc_id, path_string, "missing", 0);
        return;
    };
    if snapshot.is_symlink {
        result.push_skip(proc_id, path_string, "symlink", 0);
        return;
    }
    if !snapshot.is_dir {
        result.push_skip(proc_id, path_string, "not_directory", 0);
        return;
    }
    if let CandidateKind::Orphan { cutoff } = kind {
        if snapshot.latest_mtime >= cutoff {
            result.push_skip(
                proc_id,
                path_string,
                "fresh",
                snapshot.size_bytes,
            );
            return;
        }
    }

    result.selected = result.selected.saturating_add(1);
    result.reclaimable_bytes =
        result.reclaimable_bytes.saturating_add(snapshot.size_bytes);
    if !context.apply {
        result.entries.push(ProcRuntimeRetentionEntryWire {
            proc_id: proc_id.to_string(),
            path: path_string,
            status: "would_remove".to_string(),
            reason: reason_for(kind).to_string(),
            size_bytes: snapshot.size_bytes,
        });
        return;
    }
    match fs::remove_dir_all(&path) {
        Ok(()) => {
            result.removed = result.removed.saturating_add(1);
            result.reclaimed_bytes =
                result.reclaimed_bytes.saturating_add(snapshot.size_bytes);
            result.entries.push(ProcRuntimeRetentionEntryWire {
                proc_id: proc_id.to_string(),
                path: path_string,
                status: "removed".to_string(),
                reason: reason_for(kind).to_string(),
                size_bytes: snapshot.size_bytes,
            });
        }
        Err(error) => {
            result.errors = result.errors.saturating_add(1);
            result.entries.push(ProcRuntimeRetentionEntryWire {
                proc_id: proc_id.to_string(),
                path: path_string,
                status: "error".to_string(),
                reason: error.to_string(),
                size_bytes: snapshot.size_bytes,
            });
        }
    }
}

fn reason_for(kind: CandidateKind) -> &'static str {
    match kind {
        CandidateKind::PrunedRow => "pruned_proc_row",
        CandidateKind::Orphan { .. } => "rowless_stale_orphan",
    }
}

fn valid_proc_id(proc_id: &str) -> bool {
    proc_id.len() == PROC_ID_LENGTH
        && proc_id
            .bytes()
            .all(|byte| PROC_ID_ALPHABET.as_bytes().contains(&byte))
}

fn is_direct_child(root: &Path, path: &Path) -> bool {
    path.parent() == Some(root) && path.file_name().is_some()
}

enum RootStatus {
    Ready,
    Missing,
    Unsafe(String),
}

fn root_status(root: &Path) -> RootStatus {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return RootStatus::Missing;
        }
        Err(error) => return RootStatus::Unsafe(error.to_string()),
    };
    if metadata.file_type().is_symlink() {
        return RootStatus::Unsafe(format!(
            "proc runtime root must not be a symlink: {}",
            root.display()
        ));
    }
    if !metadata.is_dir() {
        return RootStatus::Unsafe(format!(
            "proc runtime root must be a directory: {}",
            root.display()
        ));
    }
    RootStatus::Ready
}

fn tree_snapshot(path: &Path) -> Option<EntrySnapshot> {
    let metadata = fs::symlink_metadata(path).ok()?;
    let is_dir = metadata.is_dir();
    let is_symlink = metadata.file_type().is_symlink();
    let own_mtime = metadata_mtime_seconds(&metadata).unwrap_or(0.0);
    if is_symlink {
        return Some(EntrySnapshot {
            is_dir: false,
            is_symlink: true,
            size_bytes: 0,
            latest_mtime: own_mtime,
        });
    }
    if !is_dir {
        return Some(EntrySnapshot {
            is_dir: false,
            is_symlink,
            size_bytes: metadata.len(),
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
        is_dir,
        is_symlink,
        size_bytes,
        latest_mtime,
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

impl ProcRuntimeRetentionResultWire {
    fn new(runtime_root: &Path, apply: bool) -> Self {
        Self {
            schema_version: PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION,
            runtime_root: runtime_root.to_string_lossy().into_owned(),
            apply,
            ..Self::default()
        }
    }

    fn push_skip(
        &mut self,
        proc_id: &str,
        path: String,
        reason: &str,
        size_bytes: u64,
    ) {
        self.skipped = self.skipped.saturating_add(1);
        self.entries.push(ProcRuntimeRetentionEntryWire {
            proc_id: proc_id.to_string(),
            path,
            status: "skipped".to_string(),
            reason: reason.to_string(),
            size_bytes,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;

    use tempfile::tempdir;

    use crate::procs::store::write_procs_atomic;
    use crate::procs::wire::ProcWire;

    use super::*;

    const NOW: f64 = 1_800_000_000.0;
    const DAY: f64 = 24.0 * 3600.0;

    fn request(
        store_path: &Path,
        runtime_root: &Path,
    ) -> ProcRuntimeRetentionRequestWire {
        ProcRuntimeRetentionRequestWire {
            schema_version: PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION,
            store_path: store_path.to_string_lossy().into_owned(),
            runtime_root: runtime_root.to_string_lossy().into_owned(),
            now_epoch_seconds: NOW,
            orphan_horizon_seconds: DAY,
            max_orphan_removals: 2000,
            apply: true,
            pruned_proc_ids: Vec::new(),
            sweep_orphans: true,
        }
    }

    fn proc_row(proc_id: &str, status: &str) -> ProcWire {
        ProcWire {
            schema_version: 3,
            proc_id: proc_id.to_string(),
            label: format!("proc {proc_id}"),
            kind: "command".to_string(),
            status: status.to_string(),
            lifecycle: "legacy".to_string(),
            argv: vec!["true".to_string()],
            command: vec!["true".to_string()],
            cwd: "/tmp".to_string(),
            project: None,
            workspace_num: None,
            session_id: None,
            session_label: None,
            origin: "test".to_string(),
            cl_name: None,
            tags: Vec::new(),
            pid: None,
            pgid: None,
            exit_code: if matches!(status, "success" | "error" | "killed") {
                Some(0)
            } else {
                None
            },
            phase: None,
            message: None,
            created_at: "2026-09-01T00:00:00Z".to_string(),
            started_at: None,
            finished_at: if matches!(status, "success" | "error" | "killed") {
                Some("2026-09-01T00:01:00Z".to_string())
            } else {
                None
            },
            log_path: format!("/tmp/{proc_id}.log"),
            log_owner: "proc-store".to_string(),
            shell_name: None,
            shell_kind: None,
            concurrency_keys: Vec::new(),
            request_fingerprint: None,
            reserved_by: None,
            reserved_at: None,
            supervisor_id: None,
            supervisor_claimed_at: None,
            stop_requested_by: None,
            stop_requested_at: None,
            stop_reason: None,
            timeout_seconds: None,
            idle_timeout_seconds: None,
            settling_started_at: None,
            settled_by: None,
            settled_at: None,
            finished_by: None,
            result: None,
            xprompt_proc: None,
        }
    }

    fn aged_dir(root: &Path, proc_id: &str, age_seconds: f64) -> PathBuf {
        let path = root.join(proc_id);
        fs::create_dir_all(&path).unwrap();
        let mut file = File::create(path.join("payload.bin")).unwrap();
        file.write_all(b"payload").unwrap();
        set_mtime(&path.join("payload.bin"), NOW - age_seconds);
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
    fn orphan_sweep_converges_with_budget_and_validates_ids() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        write_procs_atomic(&store, &[]).unwrap();
        for index in 0..4000 {
            aged_dir(&runtime, &format!("{index:012x}"), 2.0 * DAY);
        }
        aged_dir(&runtime, "not-a-proc-0", 2.0 * DAY);
        let symlink_target = tempdir().unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            symlink_target.path(),
            runtime.join("00000000000z"),
        )
        .unwrap();

        let mut req = request(&store, &runtime);
        req.max_orphan_removals = 2000;

        let first = apply_proc_runtime_retention(&req).unwrap();
        assert_eq!(first.removed, 2000);
        assert!(first.capped);
        assert!(runtime.join("not-a-proc-0").exists());
        assert!(runtime.join("00000000000z").exists());

        let second = apply_proc_runtime_retention(&req).unwrap();
        assert_eq!(second.removed, 2000);
        assert!(!second.capped);
        assert!(runtime.join("not-a-proc-0").exists());
        assert!(runtime.join("00000000000z").exists());
    }

    #[test]
    fn fresh_and_retained_runtime_dirs_are_preserved() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        let active = "0123456789ab";
        let terminal = "0123456789ac";
        let fresh = "0123456789ad";
        write_procs_atomic(
            &store,
            &[proc_row(active, "running"), proc_row(terminal, "success")],
        )
        .unwrap();
        aged_dir(&runtime, active, 2.0 * DAY);
        aged_dir(&runtime, terminal, 2.0 * DAY);
        aged_dir(&runtime, fresh, 0.5 * DAY);

        let result =
            apply_proc_runtime_retention(&request(&store, &runtime)).unwrap();

        assert_eq!(result.removed, 0);
        assert!(runtime.join(active).exists());
        assert!(runtime.join(terminal).exists());
        assert!(runtime.join(fresh).exists());
    }

    #[test]
    fn pruned_runtime_delete_revalidates_against_new_rows() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        let reused = "0123456789ab";
        let gone = "0123456789ac";
        write_procs_atomic(&store, &[proc_row(reused, "running")]).unwrap();
        aged_dir(&runtime, reused, 0.0);
        aged_dir(&runtime, gone, 0.0);
        let mut req = request(&store, &runtime);
        req.sweep_orphans = false;
        req.pruned_proc_ids = vec![reused.to_string(), gone.to_string()];

        let result = apply_proc_runtime_retention(&req).unwrap();

        assert_eq!(result.removed, 1);
        assert!(runtime.join(reused).exists());
        assert!(!runtime.join(gone).exists());
    }

    #[test]
    fn failed_store_read_preserves_runtime_dirs() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&store).unwrap();
        fs::create_dir_all(&runtime).unwrap();
        let orphan = "0123456789ab";
        aged_dir(&runtime, orphan, 2.0 * DAY);

        let error = apply_proc_runtime_retention(&request(&store, &runtime))
            .unwrap_err();

        assert!(error.to_string().contains("directory"));
        assert!(runtime.join(orphan).exists());
    }
}

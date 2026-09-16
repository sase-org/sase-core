use std::collections::BTreeSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use crate::store_lock::LockMode;

use super::store::{
    is_active_status, lock_with_timeout, proc_store_lock_timeout,
    read_existing_rows_unlocked, unlock,
};
use super::wire::{
    ProcRuntimeRetentionEntryWire, ProcRuntimeRetentionRequestWire,
    ProcRuntimeRetentionResultWire, PROC_RUNTIME_RETENTION_WIRE_SCHEMA_VERSION,
};
use super::ProcStoreError;

const PROC_ID_LENGTH: usize = 12;
const PROC_ID_ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";
const MAX_SWEEP_ROOT_ENTRIES: usize = 50_000;
const MAX_CAPPED_LOOKAHEAD_ENTRIES: usize = 512;
const MAX_TREE_SNAPSHOT_NODES: u64 = 50_000;
const MAX_TREE_SNAPSHOT_DEPTH: u32 = 64;

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
        let rows =
            read_trustworthy_rows_for_retention(store_path, runtime_root)?;
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
        let mut scan_budget = RetentionScanBudget::new();

        for proc_id in &request.pruned_proc_ids {
            evaluate_candidate(
                &context,
                proc_id,
                CandidateKind::PrunedRow,
                None,
                &mut result,
                &mut scan_budget,
            );
        }

        if request.sweep_orphans {
            sweep_orphans(request, &context, &mut result, &mut scan_budget)?;
        }

        Ok::<_, ProcStoreError>(result)
    })();
    unlock(lock)?;
    retention_result
}

fn read_trustworthy_rows_for_retention(
    store_path: &Path,
    runtime_root: &Path,
) -> Result<Vec<super::wire::ProcWire>, ProcStoreError> {
    match fs::symlink_metadata(store_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(ProcStoreError::Store(format!(
                    "proc runtime retention requires a regular proc store, found symlink: {}",
                    store_path.display()
                )));
            }
            if !metadata.is_file() {
                return Err(ProcStoreError::Store(format!(
                    "proc runtime retention requires a regular proc store, found non-file: {}",
                    store_path.display()
                )));
            }
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Err(ProcStoreError::Store(format!(
                "proc runtime retention refuses to remove runtime dirs under {} without an initialized proc store at {}",
                runtime_root.display(),
                store_path.display()
            )));
        }
        Err(error) => {
            return Err(ProcStoreError::Store(format!(
                "proc runtime retention could not inspect proc store {}: {error}",
                store_path.display()
            )));
        }
    }

    let (rows, stats) = read_existing_rows_unlocked(store_path)
        .map_err(ProcStoreError::Store)?;
    if stats.invalid_json_lines > 0 || stats.invalid_record_lines > 0 {
        return Err(ProcStoreError::Store(format!(
            "proc runtime retention refuses incomplete proc store snapshot at {}: invalid_json_lines={}, invalid_record_lines={}",
            store_path.display(),
            stats.invalid_json_lines,
            stats.invalid_record_lines
        )));
    }
    Ok(rows)
}

fn sweep_orphans(
    request: &ProcRuntimeRetentionRequestWire,
    context: &CandidateEvaluationContext<'_>,
    result: &mut ProcRuntimeRetentionResultWire,
    scan_budget: &mut RetentionScanBudget,
) -> Result<(), ProcStoreError> {
    let (mut paths, root_listing_capped) =
        bounded_runtime_children(context.runtime_root)?;
    if root_listing_capped {
        result.capped = true;
    }
    paths.sort();

    let cutoff = request.now_epoch_seconds - request.orphan_horizon_seconds;
    let mut selected_orphans = 0_u32;
    for (index, path) in paths.iter().enumerate() {
        if selected_orphans >= request.max_orphan_removals {
            result.capped = result.capped
                || remaining_orphans_may_select(
                    &paths[index..],
                    context.runtime_root,
                    context.retained_ids,
                    context.active_ids,
                    cutoff,
                    scan_budget,
                );
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
            scan_budget,
        );
        if result.selected > before {
            selected_orphans = selected_orphans.saturating_add(1);
        }
    }
    Ok(())
}

fn bounded_runtime_children(
    runtime_root: &Path,
) -> Result<(Vec<PathBuf>, bool), ProcStoreError> {
    let mut paths = Vec::new();
    let entries = fs::read_dir(runtime_root).map_err(|error| {
        ProcStoreError::Store(format!(
            "failed to read proc runtime root {}: {error}",
            runtime_root.display()
        ))
    })?;
    for entry in entries {
        if paths.len() >= MAX_SWEEP_ROOT_ENTRIES {
            return Ok((paths, true));
        }
        let entry = entry.map_err(|error| {
            ProcStoreError::Store(format!(
                "failed to read proc runtime root entry under {}: {error}",
                runtime_root.display()
            ))
        })?;
        paths.push(entry.path());
    }
    Ok((paths, false))
}

fn remaining_orphans_may_select(
    paths: &[PathBuf],
    runtime_root: &Path,
    retained_ids: &BTreeSet<String>,
    active_ids: &BTreeSet<String>,
    cutoff: f64,
    scan_budget: &mut RetentionScanBudget,
) -> bool {
    for (index, candidate) in paths.iter().enumerate() {
        if index >= MAX_CAPPED_LOOKAHEAD_ENTRIES {
            return true;
        }
        match orphan_would_select(
            runtime_root,
            candidate,
            retained_ids,
            active_ids,
            cutoff,
            scan_budget,
        ) {
            Ok(true) => return true,
            Ok(false) => {}
            Err(_) => return true,
        }
    }
    false
}

fn orphan_would_select(
    runtime_root: &Path,
    path: &Path,
    retained_ids: &BTreeSet<String>,
    active_ids: &BTreeSet<String>,
    cutoff: f64,
    scan_budget: &mut RetentionScanBudget,
) -> Result<bool, SnapshotError> {
    let proc_id = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if !valid_proc_id(proc_id)
        || !is_direct_child(runtime_root, path)
        || active_ids.contains(proc_id)
        || retained_ids.contains(proc_id)
    {
        return Ok(false);
    }
    Ok(tree_snapshot(path, scan_budget)?.is_some_and(|snapshot| {
        snapshot.is_dir
            && !snapshot.is_symlink
            && snapshot.latest_mtime < cutoff
    }))
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
    scan_budget: &mut RetentionScanBudget,
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

    let snapshot = match tree_snapshot(&path, scan_budget) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            result.push_skip(proc_id, path_string, "missing", 0);
            return;
        }
        Err(error) => {
            if error.budget_exhausted {
                result.capped = true;
            }
            result.push_error(proc_id, path_string, &error.to_string(), 0);
            return;
        }
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
    let removal_snapshot = match revalidate_candidate_before_removal(
        context,
        proc_id,
        kind,
        &path,
        result,
        scan_budget,
    ) {
        Some(snapshot) => snapshot,
        None => return,
    };
    match fs::remove_dir_all(&path) {
        Ok(()) => {
            result.removed = result.removed.saturating_add(1);
            result.reclaimed_bytes = result
                .reclaimed_bytes
                .saturating_add(removal_snapshot.size_bytes);
            result.entries.push(ProcRuntimeRetentionEntryWire {
                proc_id: proc_id.to_string(),
                path: path_string,
                status: "removed".to_string(),
                reason: reason_for(kind).to_string(),
                size_bytes: removal_snapshot.size_bytes,
            });
        }
        Err(error) => {
            result.errors = result.errors.saturating_add(1);
            result.entries.push(ProcRuntimeRetentionEntryWire {
                proc_id: proc_id.to_string(),
                path: path_string,
                status: "error".to_string(),
                reason: error.to_string(),
                size_bytes: removal_snapshot.size_bytes,
            });
        }
    }
}

fn revalidate_candidate_before_removal(
    context: &CandidateEvaluationContext<'_>,
    proc_id: &str,
    kind: CandidateKind,
    path: &Path,
    result: &mut ProcRuntimeRetentionResultWire,
    scan_budget: &mut RetentionScanBudget,
) -> Option<EntrySnapshot> {
    let path_string = path.to_string_lossy().into_owned();
    match root_status(context.runtime_root) {
        RootStatus::Ready => {}
        RootStatus::Missing => {
            result.push_error(
                proc_id,
                path_string,
                "proc runtime root disappeared before removal",
                0,
            );
            return None;
        }
        RootStatus::Unsafe(reason) => {
            result.push_error(proc_id, path_string, &reason, 0);
            return None;
        }
    }
    if !is_direct_child(context.runtime_root, path) {
        result.push_error(proc_id, path_string, "outside_runtime_root", 0);
        return None;
    }
    let snapshot = match tree_snapshot(path, scan_budget) {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            result.push_skip(proc_id, path_string, "missing", 0);
            return None;
        }
        Err(error) => {
            if error.budget_exhausted {
                result.capped = true;
            }
            result.push_error(proc_id, path_string, &error.to_string(), 0);
            return None;
        }
    };
    if snapshot.is_symlink {
        result.push_skip(proc_id, path_string, "symlink", 0);
        return None;
    }
    if !snapshot.is_dir {
        result.push_skip(proc_id, path_string, "not_directory", 0);
        return None;
    }
    if let CandidateKind::Orphan { cutoff } = kind {
        if snapshot.latest_mtime >= cutoff {
            result.push_skip(
                proc_id,
                path_string,
                "fresh",
                snapshot.size_bytes,
            );
            return None;
        }
    }
    Some(snapshot)
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

#[derive(Debug)]
struct RetentionScanBudget {
    tree_nodes_remaining: u64,
}

impl RetentionScanBudget {
    fn new() -> Self {
        Self {
            tree_nodes_remaining: MAX_TREE_SNAPSHOT_NODES,
        }
    }

    fn consume_tree_node(&mut self, path: &Path) -> Result<(), SnapshotError> {
        if self.tree_nodes_remaining == 0 {
            return Err(SnapshotError::budget(path));
        }
        self.tree_nodes_remaining -= 1;
        Ok(())
    }
}

#[derive(Debug)]
struct SnapshotError {
    path: PathBuf,
    reason: String,
    budget_exhausted: bool,
}

impl SnapshotError {
    fn new(path: &Path, reason: String) -> Self {
        Self {
            path: path.to_path_buf(),
            reason,
            budget_exhausted: false,
        }
    }

    fn budget(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
            reason: "proc runtime tree inspection budget exhausted".to_string(),
            budget_exhausted: true,
        }
    }
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.path.display(), self.reason)
    }
}

fn tree_snapshot(
    path: &Path,
    scan_budget: &mut RetentionScanBudget,
) -> Result<Option<EntrySnapshot>, SnapshotError> {
    tree_snapshot_inner(path, scan_budget, 0, true)
}

fn tree_snapshot_inner(
    path: &Path,
    scan_budget: &mut RetentionScanBudget,
    depth: u32,
    allow_missing: bool,
) -> Result<Option<EntrySnapshot>, SnapshotError> {
    if depth > MAX_TREE_SNAPSHOT_DEPTH {
        return Err(SnapshotError::new(
            path,
            format!(
                "proc runtime tree inspection depth exceeded {MAX_TREE_SNAPSHOT_DEPTH}"
            ),
        ));
    }
    scan_budget.consume_tree_node(path)?;
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if allow_missing && error.kind() == ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(SnapshotError::new(path, error.to_string())),
    };
    let is_dir = metadata.is_dir();
    let is_symlink = metadata.file_type().is_symlink();
    let own_mtime = metadata_mtime_seconds(&metadata).unwrap_or(0.0);
    if is_symlink {
        return Ok(Some(EntrySnapshot {
            is_dir: false,
            is_symlink: true,
            size_bytes: 0,
            latest_mtime: own_mtime,
        }));
    }
    if !is_dir {
        return Ok(Some(EntrySnapshot {
            is_dir: false,
            is_symlink,
            size_bytes: metadata.len(),
            latest_mtime: own_mtime,
        }));
    }

    let mut size_bytes = 0_u64;
    let mut latest_mtime = own_mtime;
    let entries = fs::read_dir(path).map_err(|error| {
        SnapshotError::new(path, format!("failed to read directory: {error}"))
    })?;
    for child in entries {
        let child = child.map_err(|error| {
            SnapshotError::new(path, format!("failed to read entry: {error}"))
        })?;
        if let Some(child_snapshot) = tree_snapshot_inner(
            &child.path(),
            scan_budget,
            depth.saturating_add(1),
            false,
        )? {
            size_bytes = size_bytes.saturating_add(child_snapshot.size_bytes);
            latest_mtime = latest_mtime.max(child_snapshot.latest_mtime);
        }
    }
    Ok(Some(EntrySnapshot {
        is_dir,
        is_symlink,
        size_bytes,
        latest_mtime,
    }))
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

    fn push_error(
        &mut self,
        proc_id: &str,
        path: String,
        reason: &str,
        size_bytes: u64,
    ) {
        self.errors = self.errors.saturating_add(1);
        self.entries.push(ProcRuntimeRetentionEntryWire {
            proc_id: proc_id.to_string(),
            path,
            status: "error".to_string(),
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
            service: None,
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
    fn missing_store_refuses_to_sweep_preexisting_runtime_tree() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        let orphan = "0123456789ab";
        aged_dir(&runtime, orphan, 2.0 * DAY);

        let error = apply_proc_runtime_retention(&request(&store, &runtime))
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("without an initialized proc store"));
        assert!(runtime.join(orphan).exists());
    }

    #[test]
    fn valid_empty_store_allows_rowless_orphan_sweep() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        write_procs_atomic(&store, &[]).unwrap();
        let orphan = "0123456789ab";
        aged_dir(&runtime, orphan, 2.0 * DAY);

        let result =
            apply_proc_runtime_retention(&request(&store, &runtime)).unwrap();

        assert_eq!(result.removed, 1);
        assert!(!runtime.join(orphan).exists());
    }

    #[test]
    fn malformed_store_refuses_to_sweep_runtime_tree() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        fs::write(&store, "not-json\n").unwrap();
        let orphan = "0123456789ab";
        aged_dir(&runtime, orphan, 2.0 * DAY);

        let error = apply_proc_runtime_retention(&request(&store, &runtime))
            .unwrap_err();

        assert!(error.to_string().contains("incomplete proc store snapshot"));
        assert!(runtime.join(orphan).exists());
    }

    #[test]
    fn mixed_validity_store_refuses_to_sweep_runtime_tree() {
        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        let valid = serde_json::to_string(&proc_row("0123456789ac", "success"))
            .unwrap();
        fs::write(&store, format!("{valid}\n{{\"proc_id\":\"broken\"}}\n"))
            .unwrap();
        let orphan = "0123456789ab";
        aged_dir(&runtime, orphan, 2.0 * DAY);

        let error = apply_proc_runtime_retention(&request(&store, &runtime))
            .unwrap_err();

        assert!(error.to_string().contains("invalid_record_lines=1"));
        assert!(runtime.join(orphan).exists());
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_runtime_tree_is_reported_and_preserved() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let store = temp.path().join("procs.jsonl");
        let runtime = temp.path().join("runtime");
        fs::create_dir_all(&runtime).unwrap();
        write_procs_atomic(&store, &[]).unwrap();
        let orphan = "0123456789ab";
        let path = aged_dir(&runtime, orphan, 2.0 * DAY);
        let unreadable = path.join("private");
        fs::create_dir_all(&unreadable).unwrap();
        set_mtime(&unreadable, NOW - 2.0 * DAY);
        set_mtime(&path, NOW - 2.0 * DAY);
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000))
            .unwrap();

        let result =
            apply_proc_runtime_retention(&request(&store, &runtime)).unwrap();

        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o700))
            .unwrap();
        assert_eq!(result.removed, 0);
        assert_eq!(result.errors, 1);
        assert!(result.entries.iter().any(|entry| {
            entry.proc_id == orphan
                && entry.status == "error"
                && entry.reason.contains("failed to read directory")
        }));
        assert!(path.exists());
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

        assert!(error.to_string().contains("non-file"));
        assert!(runtime.join(orphan).exists());
    }
}

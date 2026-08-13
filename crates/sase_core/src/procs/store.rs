use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, FixedOffset};

use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, HeldStoreLock,
    LockMode, StoreLockError,
};

use super::wire::{
    ProcAppendOutcomeWire, ProcPruneOutcomeWire, ProcStoreSnapshotWire,
    ProcStoreStatsWire, ProcUpdateOutcomeWire, ProcUpdateWire, ProcWire,
    PROC_WIRE_SCHEMA_VERSION,
};

/// Every proc kind the store accepts on write.
const PROC_KINDS: [&str; 3] = ["command", "tui", "detached"];

const LOCK_TIMEOUT_ENV: &str = "SASE_PROC_STORE_LOCK_TIMEOUT";
const LEGACY_LOCK_TIMEOUT_ENV: &str = "SASE_TASK_STORE_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(120);

#[derive(Debug, thiserror::Error)]
pub enum ProcStoreError {
    #[error(
        "proc store lock timed out after {waited_ms}ms waiting for {mode} lock: {}; holder: {holder}",
        path.display()
    )]
    LockTimeout {
        mode: &'static str,
        path: PathBuf,
        waited_ms: u128,
        holder: String,
    },
    #[error(
        "proc {proc_id:?} cannot transition from terminal status {from:?} to {to:?}"
    )]
    InvalidTransition {
        proc_id: String,
        from: String,
        to: String,
    },
    #[error("invalid proc {proc_id:?}: {reason}")]
    InvalidProc { proc_id: String, reason: String },
    #[error("{0}")]
    Store(String),
}

impl From<String> for ProcStoreError {
    fn from(message: String) -> Self {
        Self::Store(message)
    }
}

type ProcStoreResult<T> = Result<T, ProcStoreError>;

/// Read a stable, newest-first snapshot of the proc store.
#[allow(clippy::incompatible_msrv)]
pub fn read_procs_snapshot(
    path: &Path,
) -> ProcStoreResult<ProcStoreSnapshotWire> {
    if !path.exists() {
        return Ok(snapshot_from_rows(
            Vec::new(),
            ProcStoreStatsWire::default(),
        ));
    }
    let lock = lock_with_timeout(
        path,
        LockMode::Shared,
        proc_store_lock_timeout(),
        "read_procs_snapshot",
    )?;
    let result = read_rows_unlocked(path);
    unlock(lock)?;
    let (procs, stats) = result?;
    Ok(snapshot_from_rows(procs, stats))
}

/// Append a proc and enforce terminal-row retention atomically.
pub fn append_proc(
    path: &Path,
    proc: &ProcWire,
    history_limit: i64,
) -> ProcStoreResult<ProcAppendOutcomeWire> {
    let mut proc = proc.clone();
    normalize_and_validate_proc(&mut proc)?;
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "append_proc",
    )?;
    let result: ProcStoreResult<ProcAppendOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        rows.push(proc);
        let (kept, pruned_proc_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_procs_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(ProcAppendOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_proc_ids,
        })
    })();
    unlock(lock)?;
    result
}

/// Apply a partial proc update. Missing ids are successful no-ops.
pub fn update_proc(
    path: &Path,
    update: &ProcUpdateWire,
) -> ProcStoreResult<ProcUpdateOutcomeWire> {
    if update.proc_id.is_empty() {
        return Err(ProcStoreError::InvalidProc {
            proc_id: String::new(),
            reason: "proc_id must not be empty".to_string(),
        });
    }
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "update_proc",
    )?;
    let result: ProcStoreResult<ProcUpdateOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        let matched_index =
            rows.iter().position(|row| row.proc_id == update.proc_id);
        let Some(index) = matched_index else {
            // An update is a rewrite operation even when retention has already
            // removed the id, so malformed rows observed above are cleaned up.
            write_procs_atomic(path, &rows)?;
            return Ok(ProcUpdateOutcomeWire {
                schema_version: PROC_WIRE_SCHEMA_VERSION,
                proc: None,
                matched: false,
            });
        };

        apply_update(&mut rows[index], update)?;
        let proc = rows[index].clone();
        write_procs_atomic(path, &rows)?;
        Ok(ProcUpdateOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            proc: Some(proc),
            matched: true,
        })
    })();
    unlock(lock)?;
    result
}

/// Enforce terminal-row retention without appending a proc.
pub fn prune_procs(
    path: &Path,
    history_limit: i64,
) -> ProcStoreResult<ProcPruneOutcomeWire> {
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "prune_procs",
    )?;
    let result: ProcStoreResult<ProcPruneOutcomeWire> = (|| {
        let (rows, _) = read_rows_unlocked(path)?;
        let (kept, pruned_proc_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_procs_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(ProcPruneOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_proc_ids,
        })
    })();
    unlock(lock)?;
    result
}

fn read_rows_unlocked(
    path: &Path,
) -> Result<(Vec<ProcWire>, ProcStoreStatsWire), String> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok((Vec::new(), ProcStoreStatsWire::default()));
        }
        Err(error) => return Err(error.to_string()),
    };

    let reader = BufReader::new(file);
    let mut rows = Vec::new();
    let mut stats = ProcStoreStatsWire::default();
    for line in reader.lines() {
        let line = line.map_err(|error| error.to_string())?;
        stats.total_lines += 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            stats.blank_lines += 1;
            continue;
        }
        let value: serde_json::Value = match serde_json::from_str(trimmed) {
            Ok(value) => value,
            Err(_) => {
                stats.invalid_json_lines += 1;
                continue;
            }
        };
        let mut proc = match serde_json::from_value::<ProcWire>(value) {
            Ok(proc) => proc,
            Err(_) => {
                stats.invalid_record_lines += 1;
                continue;
            }
        };
        if normalize_and_validate_proc(&mut proc).is_err() {
            stats.invalid_record_lines += 1;
            continue;
        }
        stats.loaded_rows += 1;
        rows.push(proc);
    }
    Ok((rows, stats))
}

fn snapshot_from_rows(
    rows: Vec<ProcWire>,
    stats: ProcStoreStatsWire,
) -> ProcStoreSnapshotWire {
    ProcStoreSnapshotWire {
        schema_version: PROC_WIRE_SCHEMA_VERSION,
        procs: newest_first(rows),
        stats,
    }
}

fn newest_first(rows: Vec<ProcWire>) -> Vec<ProcWire> {
    let mut indexed: Vec<(usize, ProcWire)> =
        rows.into_iter().enumerate().collect();
    indexed.sort_by(|(left_index, left), (right_index, right)| {
        compare_proc_recency(*left_index, left, *right_index, right).reverse()
    });
    indexed.into_iter().map(|(_, proc)| proc).collect()
}

fn compare_proc_recency(
    left_index: usize,
    left: &ProcWire,
    right_index: usize,
    right: &ProcWire,
) -> Ordering {
    let left_created = parse_utc_timestamp(&left.created_at)
        .expect("validated proc timestamp must parse");
    let right_created = parse_utc_timestamp(&right.created_at)
        .expect("validated proc timestamp must parse");
    left_created
        .cmp(&right_created)
        .then_with(|| left_index.cmp(&right_index))
}

fn apply_retention(
    rows: Vec<ProcWire>,
    history_limit: usize,
) -> (Vec<ProcWire>, Vec<String>) {
    let mut terminals: Vec<(usize, &ProcWire)> = rows
        .iter()
        .enumerate()
        .filter(|(_, proc)| is_terminal_status(&proc.status))
        .collect();
    terminals.sort_by(|(left_index, left), (right_index, right)| {
        compare_proc_recency(*left_index, left, *right_index, right).reverse()
    });

    let mut keep = vec![true; rows.len()];
    for (index, _) in terminals.into_iter().skip(history_limit) {
        keep[index] = false;
    }

    let mut kept = Vec::with_capacity(rows.len());
    let mut pruned_proc_ids = Vec::new();
    for (index, proc) in rows.into_iter().enumerate() {
        if keep[index] {
            kept.push(proc);
        } else {
            pruned_proc_ids.push(proc.proc_id);
        }
    }
    (kept, pruned_proc_ids)
}

fn clamped_history_limit(history_limit: i64) -> usize {
    usize::try_from(history_limit.max(1)).unwrap_or(usize::MAX)
}

fn apply_update(
    proc: &mut ProcWire,
    update: &ProcUpdateWire,
) -> ProcStoreResult<()> {
    let was_terminal = is_terminal_status(&proc.status);
    let finished_at_was_set = was_terminal && proc.finished_at.is_some();

    if let Some(status) = update.status.as_deref() {
        validate_status(status).map_err(|reason| invalid_proc(proc, reason))?;
        if was_terminal && !is_terminal_status(status) {
            return Err(ProcStoreError::InvalidTransition {
                proc_id: proc.proc_id.clone(),
                from: proc.status.clone(),
                to: status.to_string(),
            });
        }
        // The first terminal status is final. Later terminal reports may
        // refine outcome fields, but cannot change the recorded disposition.
        if !was_terminal {
            proc.status = status.to_string();
        }
    }
    if let Some(value) = &update.label {
        proc.label.clone_from(value);
    }
    if let Some(value) = &update.kind {
        proc.kind.clone_from(value);
    }
    if let Some(value) = &update.command {
        proc.command.clone_from(value);
    }
    if let Some(value) = &update.cwd {
        proc.cwd.clone_from(value);
    }
    if let Some(value) = &update.project {
        proc.project.clone_from(value);
    }
    if let Some(value) = update.workspace_num {
        proc.workspace_num = value;
    }
    if let Some(value) = &update.session_id {
        proc.session_id.clone_from(value);
    }
    if let Some(value) = &update.session_label {
        proc.session_label.clone_from(value);
    }
    if let Some(value) = &update.origin {
        proc.origin.clone_from(value);
    }
    if let Some(value) = &update.cl_name {
        proc.cl_name.clone_from(value);
    }
    if let Some(value) = &update.tags {
        proc.tags.clone_from(value);
    }
    if let Some(value) = update.pid {
        proc.pid = value;
    }
    if let Some(value) = update.pgid {
        proc.pgid = value;
    }
    if let Some(value) = update.exit_code {
        proc.exit_code = value;
    }
    if let Some(value) = &update.phase {
        proc.phase.clone_from(value);
    }
    if let Some(value) = &update.message {
        proc.message.clone_from(value);
    }
    if let Some(value) = &update.created_at {
        proc.created_at.clone_from(value);
    }
    if let Some(value) = &update.started_at {
        proc.started_at.clone_from(value);
    }
    if !finished_at_was_set {
        if let Some(value) = &update.finished_at {
            proc.finished_at.clone_from(value);
        }
    }
    if let Some(value) = &update.log_path {
        proc.log_path.clone_from(value);
    }

    normalize_and_validate_proc(proc)
}

fn normalize_and_validate_proc(proc: &mut ProcWire) -> ProcStoreResult<()> {
    proc.tags.sort();
    proc.tags.dedup();

    for (field, value) in [
        ("proc_id", proc.proc_id.as_str()),
        ("label", proc.label.as_str()),
        ("cwd", proc.cwd.as_str()),
        ("origin", proc.origin.as_str()),
        ("created_at", proc.created_at.as_str()),
        ("log_path", proc.log_path.as_str()),
    ] {
        if value.is_empty() {
            return Err(invalid_proc(
                proc,
                format!("{field} must not be empty"),
            ));
        }
    }
    validate_kind(&proc.kind).map_err(|reason| invalid_proc(proc, reason))?;
    validate_status(&proc.status)
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("created_at", Some(&proc.created_at))
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("started_at", proc.started_at.as_ref())
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("finished_at", proc.finished_at.as_ref())
        .map_err(|reason| invalid_proc(proc, reason))?;
    Ok(())
}

fn validate_kind(kind: &str) -> Result<(), String> {
    if PROC_KINDS.contains(&kind) {
        Ok(())
    } else {
        Err(format!("unknown kind {kind:?}"))
    }
}

fn validate_status(status: &str) -> Result<(), String> {
    if matches!(
        status,
        "pending" | "running" | "success" | "error" | "killed"
    ) {
        Ok(())
    } else {
        Err(format!("unknown status {status:?}"))
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "success" | "error" | "killed")
}

fn validate_timestamp_field(
    field: &str,
    value: Option<&String>,
) -> Result<(), String> {
    if let Some(value) = value {
        parse_utc_timestamp(value)
            .map(|_| ())
            .map_err(|reason| format!("{field} {reason}"))
    } else {
        Ok(())
    }
}

fn parse_utc_timestamp(value: &str) -> Result<DateTime<FixedOffset>, String> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|error| format!("must be an RFC3339 timestamp: {error}"))?;
    if timestamp.offset().local_minus_utc() != 0 {
        return Err("must use a UTC offset".to_string());
    }
    Ok(timestamp)
}

fn invalid_proc(proc: &ProcWire, reason: String) -> ProcStoreError {
    ProcStoreError::InvalidProc {
        proc_id: proc.proc_id.clone(),
        reason,
    }
}

fn write_procs_atomic(path: &Path, procs: &[ProcWire]) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let tmp_path = temp_path_for(path);
    let write_result = (|| {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp_path)
            .map_err(|error| error.to_string())?;
        let mut writer = BufWriter::new(file);
        for proc in procs {
            serde_json::to_writer(&mut writer, proc).map_err(|error| {
                format!("failed to serialize proc: {error}")
            })?;
            writer.write_all(b"\n").map_err(|error| error.to_string())?;
        }
        writer.flush().map_err(|error| error.to_string())?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        fs::rename(&tmp_path, path).map_err(|error| error.to_string())?;
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

fn proc_store_lock_timeout() -> Duration {
    if std::env::var_os(LOCK_TIMEOUT_ENV).is_some() {
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT)
    } else {
        timeout_from_env(LEGACY_LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT)
    }
}

fn lock_with_timeout(
    path: &Path,
    mode: LockMode,
    timeout: Duration,
    operation: &str,
) -> ProcStoreResult<HeldStoreLock> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let lock_path = lock_path_for(path);
    acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        mode,
        timeout,
        operation,
    )
    .map_err(|error| match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => ProcStoreError::LockTimeout {
            mode,
            path: lock_path,
            waited_ms,
            holder: holder
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
        },
        error => ProcStoreError::Store(error.to_string()),
    })
}

fn lock_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("procs.jsonl");
    path.with_file_name(format!("{filename}.lock"))
}

fn temp_path_for(path: &Path) -> PathBuf {
    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("procs.jsonl");
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{filename}.{}.{}.tmp", process::id(), nanos))
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    path.parent().ok_or_else(|| {
        format!("proc store path has no parent: {}", path.display())
    })
}

#[allow(clippy::incompatible_msrv)]
fn unlock(lock: HeldStoreLock) -> Result<(), String> {
    lock.release().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Instant;

    use serde_json::json;
    use tempfile::tempdir;

    use super::*;

    fn proc(proc_id: &str, status: &str, created_at: &str) -> ProcWire {
        ProcWire {
            proc_id: proc_id.to_string(),
            label: format!("Proc {proc_id}"),
            kind: "command".to_string(),
            status: status.to_string(),
            command: vec!["true".to_string()],
            cwd: "/tmp".to_string(),
            project: Some("sase".to_string()),
            workspace_num: Some(1),
            session_id: None,
            session_label: None,
            origin: "test".to_string(),
            cl_name: None,
            tags: vec![
                "zeta".to_string(),
                "alpha".to_string(),
                "zeta".to_string(),
            ],
            pid: None,
            pgid: None,
            exit_code: None,
            phase: None,
            message: None,
            created_at: created_at.to_string(),
            started_at: None,
            finished_at: None,
            log_path: format!("/tmp/{proc_id}.log"),
        }
    }

    #[test]
    fn append_and_read_round_trip_is_newest_first_and_normalizes_tags() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        append_proc(
            &path,
            &proc("older", "pending", "2026-07-25T12:00:00Z"),
            10,
        )
        .unwrap();
        let outcome = append_proc(
            &path,
            &proc("newer", "running", "2026-07-25T12:00:01Z"),
            10,
        )
        .unwrap();

        assert_eq!(
            outcome
                .snapshot
                .procs
                .iter()
                .map(|proc| proc.proc_id.as_str())
                .collect::<Vec<_>>(),
            vec!["newer", "older"]
        );
        assert_eq!(outcome.snapshot.procs[0].tags, vec!["alpha", "zeta"]);
        assert_eq!(read_procs_snapshot(&path).unwrap(), outcome.snapshot);
    }

    #[test]
    fn detached_kind_round_trips_and_unknown_kinds_are_rejected() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let mut detached = proc("detached", "running", "2026-07-25T12:00:00Z");
        detached.kind = "detached".to_string();
        let outcome = append_proc(&path, &detached, 10).unwrap();
        assert_eq!(outcome.snapshot.procs[0].kind, "detached");
        assert_eq!(
            read_procs_snapshot(&path).unwrap().procs[0].kind,
            "detached"
        );

        let mut unknown = proc("unknown", "running", "2026-07-25T12:00:01Z");
        unknown.kind = "daemon".to_string();
        let error = append_proc(&path, &unknown, 10).unwrap_err();
        assert!(matches!(
            &error,
            ProcStoreError::InvalidProc { proc_id, reason }
                if proc_id == "unknown"
                    && reason == "unknown kind \"daemon\""
        ));
    }

    #[test]
    fn updating_an_existing_row_to_the_detached_kind_validates() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        append_proc(
            &path,
            &proc("promoted", "running", "2026-07-25T12:00:00Z"),
            10,
        )
        .unwrap();

        let updated = update_proc(
            &path,
            &ProcUpdateWire {
                proc_id: "promoted".to_string(),
                kind: Some("detached".to_string()),
                ..ProcUpdateWire::default()
            },
        )
        .unwrap();
        assert_eq!(updated.proc.unwrap().kind, "detached");

        let error = update_proc(
            &path,
            &ProcUpdateWire {
                proc_id: "promoted".to_string(),
                kind: Some("daemon".to_string()),
                ..ProcUpdateWire::default()
            },
        )
        .unwrap_err();
        assert!(matches!(error, ProcStoreError::InvalidProc { .. }));
        assert_eq!(
            read_procs_snapshot(&path).unwrap().procs[0].kind,
            "detached"
        );
    }

    #[test]
    fn retention_keeps_newest_terminal_rows_at_and_beyond_limit() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        for (id, created_at) in [
            ("one", "2026-07-25T12:00:01Z"),
            ("two", "2026-07-25T12:00:02Z"),
            ("three", "2026-07-25T12:00:03Z"),
        ] {
            append_proc(&path, &proc(id, "success", created_at), 2).unwrap();
        }

        let snapshot = read_procs_snapshot(&path).unwrap();
        assert_eq!(
            snapshot
                .procs
                .iter()
                .map(|proc| proc.proc_id.as_str())
                .collect::<Vec<_>>(),
            vec!["three", "two"]
        );

        let outcome = prune_procs(&path, 1).unwrap();
        assert_eq!(outcome.pruned_proc_ids, vec!["two"]);
        assert_eq!(outcome.snapshot.procs[0].proc_id, "three");
        assert!(prune_procs(&path, 0).unwrap().pruned_proc_ids.is_empty());
    }

    #[test]
    fn running_rows_survive_retention() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        append_proc(
            &path,
            &proc("running-old", "running", "2026-07-25T12:00:00Z"),
            1,
        )
        .unwrap();
        append_proc(
            &path,
            &proc("done-old", "success", "2026-07-25T12:00:01Z"),
            1,
        )
        .unwrap();
        let outcome = append_proc(
            &path,
            &proc("done-new", "error", "2026-07-25T12:00:02Z"),
            1,
        )
        .unwrap();

        assert_eq!(outcome.pruned_proc_ids, vec!["done-old"]);
        assert!(outcome
            .snapshot
            .procs
            .iter()
            .any(|proc| proc.proc_id == "running-old"));
    }

    #[test]
    fn terminal_transition_guards_and_repeat_writes_preserve_final_fields() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let mut finished = proc("finished", "success", "2026-07-25T12:00:00Z");
        finished.finished_at = Some("2026-07-25T12:00:10Z".to_string());
        finished.message = Some("first".to_string());
        append_proc(&path, &finished, 10).unwrap();

        let backward = ProcUpdateWire {
            proc_id: "finished".to_string(),
            status: Some("running".to_string()),
            ..ProcUpdateWire::default()
        };
        assert!(matches!(
            update_proc(&path, &backward),
            Err(ProcStoreError::InvalidTransition { .. })
        ));

        let repeated = ProcUpdateWire {
            proc_id: "finished".to_string(),
            status: Some("error".to_string()),
            exit_code: Some(Some(7)),
            message: Some(Some("second".to_string())),
            finished_at: Some(Some("2026-07-25T12:00:20Z".to_string())),
            ..ProcUpdateWire::default()
        };
        let updated = update_proc(&path, &repeated).unwrap().proc.unwrap();
        assert_eq!(updated.status, "success");
        assert_eq!(updated.exit_code, Some(7));
        assert_eq!(updated.message.as_deref(), Some("second"));
        assert_eq!(
            updated.finished_at.as_deref(),
            Some("2026-07-25T12:00:10Z")
        );

        let missing = update_proc(
            &path,
            &ProcUpdateWire {
                proc_id: "pruned".to_string(),
                ..ProcUpdateWire::default()
            },
        )
        .unwrap();
        assert!(!missing.matched);
        assert!(missing.proc.is_none());
    }

    #[test]
    fn unknown_fields_are_tolerated_and_malformed_rows_are_dropped_on_rewrite()
    {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let mut value = serde_json::to_value(proc(
            "valid",
            "pending",
            "2026-07-25T12:00:00Z",
        ))
        .unwrap();
        value["future_field"] = json!({"anything": true});
        fs::write(
            &path,
            format!("not-json\n{}\n{{\"proc_id\":\"broken\"}}\n", value),
        )
        .unwrap();

        let snapshot = read_procs_snapshot(&path).unwrap();
        assert_eq!(snapshot.procs.len(), 1);
        assert_eq!(snapshot.stats.invalid_json_lines, 1);
        assert_eq!(snapshot.stats.invalid_record_lines, 1);

        prune_procs(&path, 10).unwrap();
        let rewritten = fs::read_to_string(&path).unwrap();
        assert_eq!(rewritten.lines().count(), 1);
        assert!(!rewritten.contains("future_field"));
    }

    #[test]
    fn legacy_task_id_rows_are_accepted_and_rewritten_with_proc_id() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let mut value = serde_json::to_value(proc(
            "legacy",
            "success",
            "2026-07-25T12:00:00Z",
        ))
        .unwrap();
        let object = value.as_object_mut().unwrap();
        let proc_id = object.remove("proc_id").unwrap();
        object.insert("task_id".to_string(), proc_id);
        fs::write(&path, format!("{value}\n")).unwrap();

        let snapshot = read_procs_snapshot(&path).unwrap();
        assert_eq!(snapshot.schema_version, PROC_WIRE_SCHEMA_VERSION);
        assert_eq!(snapshot.procs[0].proc_id, "legacy");

        prune_procs(&path, 10).unwrap();
        let rewritten = fs::read_to_string(&path).unwrap();
        assert!(rewritten.contains("\"proc_id\":\"legacy\""));
        assert!(!rewritten.contains("\"task_id\""));
    }

    #[test]
    fn concurrent_writers_do_not_lose_rows() {
        let temp = tempdir().unwrap();
        let path = Arc::new(temp.path().join("procs.jsonl"));
        let barrier = Arc::new(Barrier::new(8));
        let mut writers = Vec::new();
        for index in 0..8 {
            let path = Arc::clone(&path);
            let barrier = Arc::clone(&barrier);
            writers.push(thread::spawn(move || {
                barrier.wait();
                append_proc(
                    &path,
                    &proc(
                        &format!("proc-{index}"),
                        "running",
                        &format!("2026-07-25T12:00:{index:02}Z"),
                    ),
                    1,
                )
                .unwrap();
            }));
        }
        for writer in writers {
            writer.join().unwrap();
        }

        assert_eq!(read_procs_snapshot(&path).unwrap().procs.len(), 8);
    }

    #[test]
    fn held_exclusive_lock_bounds_reader_and_writer_waits() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let holder = lock_with_timeout(
            &path,
            LockMode::Exclusive,
            Duration::from_secs(1),
            "test_holder",
        )
        .unwrap();

        for mode in [LockMode::Shared, LockMode::Exclusive] {
            let started = Instant::now();
            let error = lock_with_timeout(
                &path,
                mode,
                Duration::from_millis(50),
                "test_contender",
            )
            .unwrap_err();
            assert!(matches!(&error, ProcStoreError::LockTimeout { .. }));
            assert!(error.to_string().contains("operation=test_holder"));
            assert!(started.elapsed() < Duration::from_secs(1));
        }

        holder.release().unwrap();
    }
}

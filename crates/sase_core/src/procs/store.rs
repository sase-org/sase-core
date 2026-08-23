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
    ProcAppendOutcomeWire, ProcFinishWire, ProcPruneOutcomeWire,
    ProcReserveOutcomeWire, ProcReserveWire, ProcSettlementWire,
    ProcStopRequestWire, ProcStoreSnapshotWire, ProcStoreStatsWire,
    ProcSupervisorClaimWire, ProcUpdateOutcomeWire, ProcUpdateWire, ProcWire,
    PROC_WIRE_SCHEMA_VERSION, SUPPORTED_PROC_WIRE_SCHEMA_VERSIONS,
};

/// Every proc kind the store accepts on write.
const PROC_KINDS: [&str; 3] = ["command", "tui", "detached"];
const PROC_LIFECYCLES: [&str; 2] = ["legacy", "proc-shell"];
const STORE_LOG_OWNER: &str = "proc-store";
const PROC_SHELL_LIFECYCLE: &str = "proc-shell";

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
    #[error(
        "proc conflict on {field} {value:?}: active proc {proc_id:?}: {reason}"
    )]
    Conflict {
        proc_id: String,
        field: String,
        value: String,
        reason: String,
    },
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
    normalize_and_validate_proc(&mut proc, ValidationMode::LegacyWrite)?;
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "append_proc",
    )?;
    let result: ProcStoreResult<ProcAppendOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        rows.push(proc);
        let (kept, pruned_proc_ids, pruned_log_proc_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_procs_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(ProcAppendOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_proc_ids,
            pruned_log_proc_ids,
        })
    })();
    unlock(lock)?;
    result
}

/// Atomically reserve a proc-shell row or replay an identical active request.
pub fn reserve_proc(
    path: &Path,
    request: &ProcReserveWire,
    history_limit: i64,
) -> ProcStoreResult<ProcReserveOutcomeWire> {
    validate_reserve_request(request)?;
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        "reserve_proc",
    )?;
    let result: ProcStoreResult<ProcReserveOutcomeWire> = (|| {
        let (mut rows, stats) = read_rows_unlocked(path)?;
        if let Some(proc) = find_idempotent_replay(&rows, request) {
            let snapshot = snapshot_from_rows(rows, stats);
            return Ok(ProcReserveOutcomeWire {
                schema_version: PROC_WIRE_SCHEMA_VERSION,
                proc,
                snapshot,
                reserved: false,
                replayed: true,
                pruned_proc_ids: Vec::new(),
                pruned_log_proc_ids: Vec::new(),
            });
        }
        reject_reserve_conflicts(&rows, request)?;
        let proc = proc_from_reserve_request(request)?;
        rows.push(proc.clone());
        let (kept, pruned_proc_ids, pruned_log_proc_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_procs_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(ProcReserveOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            proc,
            snapshot: snapshot_from_rows(rows, stats),
            reserved: true,
            replayed: false,
            pruned_proc_ids,
            pruned_log_proc_ids,
        })
    })();
    unlock(lock)?;
    result
}

/// Mark a reserved proc-shell as claimed by one supervisor identity.
pub fn claim_proc_supervisor(
    path: &Path,
    claim: &ProcSupervisorClaimWire,
) -> ProcStoreResult<ProcUpdateOutcomeWire> {
    validate_non_empty("proc_id", &claim.proc_id)?;
    validate_non_empty("supervisor_id", &claim.supervisor_id)?;
    validate_timestamp_field("claimed_at", Some(&claim.claimed_at)).map_err(
        |reason| ProcStoreError::InvalidProc {
            proc_id: claim.proc_id.clone(),
            reason,
        },
    )?;
    mutate_proc(path, "claim_proc_supervisor", &claim.proc_id, |proc| {
        ensure_proc_shell(proc)?;
        ensure_not_terminal(proc, "claim supervisor")?;
        ensure_supervisor_owner(proc, &claim.supervisor_id)?;
        proc.supervisor_id = Some(claim.supervisor_id.clone());
        proc.supervisor_claimed_at = Some(claim.claimed_at.clone());
        proc.pid = claim.pid;
        proc.pgid = claim.pgid;
        if proc.started_at.is_none() {
            proc.started_at = Some(claim.claimed_at.clone());
        }
        if proc.status == "pending" {
            proc.status = "running".to_string();
        }
        Ok(())
    })
}

/// Persist stop intent without publishing a terminal outcome.
pub fn request_proc_stop(
    path: &Path,
    request: &ProcStopRequestWire,
) -> ProcStoreResult<ProcUpdateOutcomeWire> {
    validate_non_empty("proc_id", &request.proc_id)?;
    validate_non_empty("requested_by", &request.requested_by)?;
    validate_timestamp_field("requested_at", Some(&request.requested_at))
        .map_err(|reason| ProcStoreError::InvalidProc {
            proc_id: request.proc_id.clone(),
            reason,
        })?;
    mutate_proc(path, "request_proc_stop", &request.proc_id, |proc| {
        ensure_proc_shell(proc)?;
        ensure_not_terminal(proc, "request stop")?;
        if proc.stop_requested_at.is_none() {
            proc.stop_requested_by = Some(request.requested_by.clone());
            proc.stop_requested_at = Some(request.requested_at.clone());
            proc.stop_reason.clone_from(&request.reason);
        }
        Ok(())
    })
}

/// Enter the durable settlement phase before any terminal status is recorded.
pub fn begin_proc_settlement(
    path: &Path,
    settlement: &ProcSettlementWire,
) -> ProcStoreResult<ProcUpdateOutcomeWire> {
    validate_non_empty("proc_id", &settlement.proc_id)?;
    validate_non_empty("supervisor_id", &settlement.supervisor_id)?;
    validate_timestamp_field("settling_at", Some(&settlement.settling_at))
        .map_err(|reason| ProcStoreError::InvalidProc {
            proc_id: settlement.proc_id.clone(),
            reason,
        })?;
    mutate_proc(path, "begin_proc_settlement", &settlement.proc_id, |proc| {
        ensure_proc_shell(proc)?;
        ensure_not_terminal(proc, "begin settlement")?;
        ensure_supervisor_matches(proc, &settlement.supervisor_id)?;
        proc.status = "settling".to_string();
        proc.settling_started_at = Some(settlement.settling_at.clone());
        if settlement.exit_code.is_some() {
            proc.exit_code = settlement.exit_code;
        }
        if settlement.message.is_some() {
            proc.message.clone_from(&settlement.message);
        }
        Ok(())
    })
}

/// Publish one terminal result after settlement has durably completed.
pub fn finish_proc(
    path: &Path,
    finish: &ProcFinishWire,
) -> ProcStoreResult<ProcUpdateOutcomeWire> {
    validate_non_empty("proc_id", &finish.proc_id)?;
    validate_non_empty("supervisor_id", &finish.supervisor_id)?;
    validate_status(&finish.status).map_err(|reason| {
        ProcStoreError::InvalidProc {
            proc_id: finish.proc_id.clone(),
            reason,
        }
    })?;
    if !is_terminal_status(&finish.status) {
        return Err(ProcStoreError::InvalidProc {
            proc_id: finish.proc_id.clone(),
            reason: "finish status must be terminal".to_string(),
        });
    }
    validate_timestamp_field("finished_at", Some(&finish.finished_at))
        .map_err(|reason| ProcStoreError::InvalidProc {
            proc_id: finish.proc_id.clone(),
            reason,
        })?;
    mutate_proc(path, "finish_proc", &finish.proc_id, |proc| {
        ensure_proc_shell(proc)?;
        ensure_supervisor_matches(proc, &finish.supervisor_id)?;
        if is_terminal_status(&proc.status) {
            if proc.finished_by.as_deref()
                == Some(finish.supervisor_id.as_str())
            {
                return Ok(());
            }
            return Err(ProcStoreError::InvalidTransition {
                proc_id: proc.proc_id.clone(),
                from: proc.status.clone(),
                to: finish.status.clone(),
            });
        }
        if proc.status != "settling" {
            return Err(ProcStoreError::InvalidTransition {
                proc_id: proc.proc_id.clone(),
                from: proc.status.clone(),
                to: finish.status.clone(),
            });
        }
        proc.status.clone_from(&finish.status);
        proc.finished_at = Some(finish.finished_at.clone());
        proc.finished_by = Some(finish.supervisor_id.clone());
        proc.settled_by = Some(finish.supervisor_id.clone());
        proc.settled_at = Some(finish.finished_at.clone());
        proc.exit_code = finish.exit_code;
        proc.message.clone_from(&finish.message);
        proc.result.clone_from(&finish.result);
        Ok(())
    })
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
        let (kept, pruned_proc_ids, pruned_log_proc_ids) =
            apply_retention(rows, clamped_history_limit(history_limit));
        write_procs_atomic(path, &kept)?;
        let (rows, stats) = read_rows_unlocked(path)?;
        Ok(ProcPruneOutcomeWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            snapshot: snapshot_from_rows(rows, stats),
            pruned_proc_ids,
            pruned_log_proc_ids,
        })
    })();
    unlock(lock)?;
    result
}

fn mutate_proc<F>(
    path: &Path,
    operation: &'static str,
    proc_id: &str,
    mutate: F,
) -> ProcStoreResult<ProcUpdateOutcomeWire>
where
    F: FnOnce(&mut ProcWire) -> ProcStoreResult<()>,
{
    let lock = lock_with_timeout(
        path,
        LockMode::Exclusive,
        proc_store_lock_timeout(),
        operation,
    )?;
    let result: ProcStoreResult<ProcUpdateOutcomeWire> = (|| {
        let (mut rows, _) = read_rows_unlocked(path)?;
        let Some(index) = rows.iter().position(|row| row.proc_id == proc_id)
        else {
            write_procs_atomic(path, &rows)?;
            return Ok(ProcUpdateOutcomeWire {
                schema_version: PROC_WIRE_SCHEMA_VERSION,
                proc: None,
                matched: false,
            });
        };
        mutate(&mut rows[index])?;
        normalize_and_validate_proc(
            &mut rows[index],
            ValidationMode::ProcShellWrite,
        )?;
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
        if normalize_and_validate_proc(&mut proc, ValidationMode::Read).is_err()
        {
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
) -> (Vec<ProcWire>, Vec<String>, Vec<String>) {
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
    let mut pruned_log_proc_ids = Vec::new();
    for (index, proc) in rows.into_iter().enumerate() {
        if keep[index] {
            kept.push(proc);
        } else {
            if proc.log_owner == STORE_LOG_OWNER {
                pruned_log_proc_ids.push(proc.proc_id.clone());
            }
            pruned_proc_ids.push(proc.proc_id);
        }
    }
    (kept, pruned_proc_ids, pruned_log_proc_ids)
}

fn clamped_history_limit(history_limit: i64) -> usize {
    usize::try_from(history_limit.max(1)).unwrap_or(usize::MAX)
}

fn validate_reserve_request(request: &ProcReserveWire) -> ProcStoreResult<()> {
    if request.schema_version != PROC_WIRE_SCHEMA_VERSION {
        return Err(ProcStoreError::InvalidProc {
            proc_id: request.proc_id.clone(),
            reason: format!(
                "reserve requires schema_version {PROC_WIRE_SCHEMA_VERSION}"
            ),
        });
    }
    for (field, value) in [
        ("proc_id", request.proc_id.as_str()),
        ("label", request.label.as_str()),
        ("cwd", request.cwd.as_str()),
        ("origin", request.origin.as_str()),
        ("created_at", request.created_at.as_str()),
        ("log_path", request.log_path.as_str()),
        ("request_fingerprint", request.request_fingerprint.as_str()),
        ("reserved_by", request.reserved_by.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ProcStoreError::InvalidProc {
                proc_id: request.proc_id.clone(),
                reason: format!("{field} must not be empty"),
            });
        }
    }
    if request.argv.iter().all(|value| value.trim().is_empty()) {
        return Err(ProcStoreError::InvalidProc {
            proc_id: request.proc_id.clone(),
            reason: "argv must not be empty".to_string(),
        });
    }
    validate_kind(&request.kind).map_err(|reason| {
        ProcStoreError::InvalidProc {
            proc_id: request.proc_id.clone(),
            reason,
        }
    })?;
    validate_timestamp_field("created_at", Some(&request.created_at)).map_err(
        |reason| ProcStoreError::InvalidProc {
            proc_id: request.proc_id.clone(),
            reason,
        },
    )?;
    Ok(())
}

fn proc_from_reserve_request(
    request: &ProcReserveWire,
) -> ProcStoreResult<ProcWire> {
    let mut proc = ProcWire {
        schema_version: PROC_WIRE_SCHEMA_VERSION,
        proc_id: request.proc_id.trim().to_string(),
        label: request.label.trim().to_string(),
        kind: request.kind.trim().to_string(),
        status: "pending".to_string(),
        lifecycle: PROC_SHELL_LIFECYCLE.to_string(),
        argv: request.argv.clone(),
        command: request.argv.clone(),
        cwd: request.cwd.trim().to_string(),
        project: request.project.clone(),
        workspace_num: request.workspace_num,
        session_id: request.session_id.clone(),
        session_label: request.session_label.clone(),
        origin: request.origin.trim().to_string(),
        cl_name: request.cl_name.clone(),
        tags: request.tags.clone(),
        pid: None,
        pgid: None,
        exit_code: None,
        phase: Some("reserved".to_string()),
        message: None,
        created_at: request.created_at.trim().to_string(),
        started_at: None,
        finished_at: None,
        log_path: request.log_path.trim().to_string(),
        log_owner: request.log_owner.trim().to_string(),
        shell_name: request.shell_name.clone(),
        shell_kind: request.shell_kind.clone(),
        concurrency_keys: request.concurrency_keys.clone(),
        request_fingerprint: Some(
            request.request_fingerprint.trim().to_string(),
        ),
        reserved_by: Some(request.reserved_by.trim().to_string()),
        reserved_at: Some(request.created_at.trim().to_string()),
        supervisor_id: None,
        supervisor_claimed_at: None,
        stop_requested_by: None,
        stop_requested_at: None,
        stop_reason: None,
        timeout_seconds: request.timeout_seconds,
        idle_timeout_seconds: request.idle_timeout_seconds,
        settling_started_at: None,
        settled_by: None,
        settled_at: None,
        finished_by: None,
        result: None,
        xprompt_proc: request.xprompt_proc.clone(),
    };
    normalize_and_validate_proc(&mut proc, ValidationMode::ProcShellWrite)?;
    Ok(proc)
}

fn find_idempotent_replay(
    rows: &[ProcWire],
    request: &ProcReserveWire,
) -> Option<ProcWire> {
    rows.iter()
        .find(|row| {
            is_active_status(&row.status)
                && same_project(
                    row.project.as_deref(),
                    request.project.as_deref(),
                )
                && row.shell_name.as_deref() == request.shell_name.as_deref()
                && row.request_fingerprint.as_deref()
                    == Some(request.request_fingerprint.as_str())
        })
        .cloned()
}

fn reject_reserve_conflicts(
    rows: &[ProcWire],
    request: &ProcReserveWire,
) -> ProcStoreResult<()> {
    let request_keys = normalized_conflict_keys(
        request.project.as_deref(),
        request.shell_name.as_deref(),
        &request.concurrency_keys,
    );
    for row in rows.iter().filter(|row| is_active_status(&row.status)) {
        if row.proc_id == request.proc_id {
            return Err(ProcStoreError::Conflict {
                proc_id: row.proc_id.clone(),
                field: "proc_id".to_string(),
                value: request.proc_id.clone(),
                reason: "active proc id already exists".to_string(),
            });
        }
        if !same_project(row.project.as_deref(), request.project.as_deref()) {
            continue;
        }
        if let (Some(existing), Some(requested)) =
            (row.shell_name.as_deref(), request.shell_name.as_deref())
        {
            if existing == requested {
                return Err(ProcStoreError::Conflict {
                    proc_id: row.proc_id.clone(),
                    field: "shell_name".to_string(),
                    value: requested.to_string(),
                    reason: "active shell name is already reserved".to_string(),
                });
            }
        }
        let row_keys = normalized_conflict_keys(
            row.project.as_deref(),
            row.shell_name.as_deref(),
            &row.concurrency_keys,
        );
        if let Some(key) =
            request_keys.iter().find(|key| row_keys.contains(*key))
        {
            return Err(ProcStoreError::Conflict {
                proc_id: row.proc_id.clone(),
                field: "concurrency_key".to_string(),
                value: key.clone(),
                reason: "active concurrency key overlaps".to_string(),
            });
        }
    }
    Ok(())
}

fn normalized_conflict_keys(
    project: Option<&str>,
    shell_name: Option<&str>,
    keys: &[String],
) -> Vec<String> {
    let mut values: Vec<String> = keys
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .collect();
    if let Some(shell_name) = shell_name {
        if !shell_name.trim().is_empty() {
            values.push(format!(
                "shell:{}:{}",
                project.unwrap_or(""),
                shell_name.trim()
            ));
        }
    }
    values.sort();
    values.dedup();
    values
}

fn same_project(left: Option<&str>, right: Option<&str>) -> bool {
    left.unwrap_or("") == right.unwrap_or("")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ValidationMode {
    Read,
    LegacyWrite,
    ProcShellWrite,
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
            if is_proc_shell(proc)
                && is_terminal_status(status)
                && proc.status != "settling"
            {
                return Err(ProcStoreError::InvalidTransition {
                    proc_id: proc.proc_id.clone(),
                    from: proc.status.clone(),
                    to: status.to_string(),
                });
            }
            proc.status = status.to_string();
        }
    }
    if let Some(value) = update.schema_version {
        proc.schema_version = value;
    }
    if let Some(value) = &update.label {
        proc.label.clone_from(value);
    }
    if let Some(value) = &update.kind {
        proc.kind.clone_from(value);
    }
    if let Some(value) = &update.lifecycle {
        proc.lifecycle.clone_from(value);
    }
    if let Some(value) = &update.argv {
        if is_proc_shell(proc) && *value != proc.argv {
            return Err(invalid_proc(proc, "argv is immutable".to_string()));
        }
        proc.argv.clone_from(value);
    }
    if let Some(value) = &update.command {
        if is_proc_shell(proc) && *value != proc.command {
            return Err(invalid_proc(proc, "command is immutable".to_string()));
        }
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
    if let Some(value) = &update.log_owner {
        proc.log_owner.clone_from(value);
    }
    if let Some(value) = &update.shell_name {
        proc.shell_name.clone_from(value);
    }
    if let Some(value) = &update.shell_kind {
        proc.shell_kind.clone_from(value);
    }
    if let Some(value) = &update.concurrency_keys {
        proc.concurrency_keys.clone_from(value);
    }
    if let Some(value) = &update.request_fingerprint {
        proc.request_fingerprint.clone_from(value);
    }
    if let Some(value) = &update.reserved_by {
        proc.reserved_by.clone_from(value);
    }
    if let Some(value) = &update.reserved_at {
        proc.reserved_at.clone_from(value);
    }
    if let Some(value) = &update.supervisor_id {
        proc.supervisor_id.clone_from(value);
    }
    if let Some(value) = &update.supervisor_claimed_at {
        proc.supervisor_claimed_at.clone_from(value);
    }
    if let Some(value) = &update.stop_requested_by {
        proc.stop_requested_by.clone_from(value);
    }
    if let Some(value) = &update.stop_requested_at {
        proc.stop_requested_at.clone_from(value);
    }
    if let Some(value) = &update.stop_reason {
        proc.stop_reason.clone_from(value);
    }
    if let Some(value) = update.timeout_seconds {
        proc.timeout_seconds = value;
    }
    if let Some(value) = update.idle_timeout_seconds {
        proc.idle_timeout_seconds = value;
    }
    if let Some(value) = &update.settling_started_at {
        proc.settling_started_at.clone_from(value);
    }
    if let Some(value) = &update.settled_by {
        proc.settled_by.clone_from(value);
    }
    if let Some(value) = &update.settled_at {
        proc.settled_at.clone_from(value);
    }
    if let Some(value) = &update.finished_by {
        proc.finished_by.clone_from(value);
    }
    if let Some(value) = &update.result {
        proc.result.clone_from(value);
    }
    if let Some(value) = &update.xprompt_proc {
        proc.xprompt_proc.clone_from(value);
    }

    normalize_and_validate_proc(proc, ValidationMode::LegacyWrite)
}

fn normalize_and_validate_proc(
    proc: &mut ProcWire,
    mode: ValidationMode,
) -> ProcStoreResult<()> {
    if !SUPPORTED_PROC_WIRE_SCHEMA_VERSIONS.contains(&proc.schema_version) {
        return Err(invalid_proc(
            proc,
            format!("unsupported schema_version {}", proc.schema_version),
        ));
    }
    if proc.lifecycle.is_empty() {
        proc.lifecycle = "legacy".to_string();
    }
    if proc.log_owner.is_empty() {
        proc.log_owner = STORE_LOG_OWNER.to_string();
    }
    if proc.argv.is_empty() && !proc.command.is_empty() {
        proc.argv.clone_from(&proc.command);
    }
    if proc.command.is_empty() && !proc.argv.is_empty() {
        proc.command.clone_from(&proc.argv);
    }
    normalize_optional_string(&mut proc.shell_name);
    normalize_optional_string(&mut proc.shell_kind);
    normalize_optional_string(&mut proc.request_fingerprint);
    normalize_optional_string(&mut proc.reserved_by);
    normalize_optional_string(&mut proc.reserved_at);
    normalize_optional_string(&mut proc.supervisor_id);
    normalize_optional_string(&mut proc.supervisor_claimed_at);
    normalize_optional_string(&mut proc.stop_requested_by);
    normalize_optional_string(&mut proc.stop_requested_at);
    normalize_optional_string(&mut proc.stop_reason);
    normalize_optional_string(&mut proc.settling_started_at);
    normalize_optional_string(&mut proc.settled_by);
    normalize_optional_string(&mut proc.settled_at);
    normalize_optional_string(&mut proc.finished_by);
    proc.tags.sort();
    proc.tags.dedup();
    trim_string_vec(&mut proc.argv);
    trim_string_vec(&mut proc.command);
    normalize_string_vec(&mut proc.concurrency_keys);

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
    validate_lifecycle(&proc.lifecycle)
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_status(&proc.status)
        .map_err(|reason| invalid_proc(proc, reason))?;
    if mode == ValidationMode::ProcShellWrite || is_proc_shell(proc) {
        if proc.argv.is_empty() {
            return Err(invalid_proc(
                proc,
                "argv must not be empty".to_string(),
            ));
        }
        if proc.request_fingerprint.as_deref().unwrap_or("").is_empty() {
            return Err(invalid_proc(
                proc,
                "request_fingerprint must not be empty".to_string(),
            ));
        }
        if proc.reserved_by.as_deref().unwrap_or("").is_empty() {
            return Err(invalid_proc(
                proc,
                "reserved_by must not be empty".to_string(),
            ));
        }
        if is_terminal_status(&proc.status) && proc.settled_at.is_none() {
            return Err(invalid_proc(
                proc,
                "terminal proc-shell rows must be settled first".to_string(),
            ));
        }
    }
    validate_timestamp_field("created_at", Some(&proc.created_at))
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("started_at", proc.started_at.as_ref())
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("finished_at", proc.finished_at.as_ref())
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("reserved_at", proc.reserved_at.as_ref())
        .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field(
        "supervisor_claimed_at",
        proc.supervisor_claimed_at.as_ref(),
    )
    .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field(
        "stop_requested_at",
        proc.stop_requested_at.as_ref(),
    )
    .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field(
        "settling_started_at",
        proc.settling_started_at.as_ref(),
    )
    .map_err(|reason| invalid_proc(proc, reason))?;
    validate_timestamp_field("settled_at", proc.settled_at.as_ref())
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

fn validate_lifecycle(lifecycle: &str) -> Result<(), String> {
    if PROC_LIFECYCLES.contains(&lifecycle) {
        Ok(())
    } else {
        Err(format!("unknown lifecycle {lifecycle:?}"))
    }
}

fn validate_status(status: &str) -> Result<(), String> {
    if matches!(
        status,
        "pending" | "running" | "settling" | "success" | "error" | "killed"
    ) {
        Ok(())
    } else {
        Err(format!("unknown status {status:?}"))
    }
}

fn is_active_status(status: &str) -> bool {
    matches!(status, "pending" | "running" | "settling")
}

fn is_terminal_status(status: &str) -> bool {
    matches!(status, "success" | "error" | "killed")
}

fn is_proc_shell(proc: &ProcWire) -> bool {
    proc.lifecycle == PROC_SHELL_LIFECYCLE
}

fn normalize_optional_string(value: &mut Option<String>) {
    if let Some(inner) = value {
        let trimmed = inner.trim().to_string();
        if trimmed.is_empty() {
            *value = None;
        } else if *inner != trimmed {
            *inner = trimmed;
        }
    }
}

fn trim_string_vec(values: &mut Vec<String>) {
    for value in values.iter_mut() {
        let trimmed = value.trim().to_string();
        if *value != trimmed {
            *value = trimmed;
        }
    }
    values.retain(|value| !value.is_empty());
}

fn normalize_string_vec(values: &mut Vec<String>) {
    for value in values.iter_mut() {
        let trimmed = value.trim().to_string();
        if *value != trimmed {
            *value = trimmed;
        }
    }
    values.retain(|value| !value.is_empty());
    values.sort();
    values.dedup();
}

fn validate_non_empty(field: &str, value: &str) -> ProcStoreResult<()> {
    if value.is_empty() {
        return Err(ProcStoreError::InvalidProc {
            proc_id: String::new(),
            reason: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

fn ensure_proc_shell(proc: &ProcWire) -> ProcStoreResult<()> {
    if !is_proc_shell(proc) {
        return Err(invalid_proc(
            proc,
            "operation requires a proc-shell lifecycle row".to_string(),
        ));
    }
    Ok(())
}

fn ensure_not_terminal(proc: &ProcWire, action: &str) -> ProcStoreResult<()> {
    if is_terminal_status(&proc.status) {
        return Err(invalid_proc(
            proc,
            format!("cannot {action} for terminal status {:?}", proc.status),
        ));
    }
    Ok(())
}

fn ensure_supervisor_owner(
    proc: &ProcWire,
    supervisor_id: &str,
) -> ProcStoreResult<()> {
    if let Some(existing) = proc.supervisor_id.as_deref() {
        if existing != supervisor_id {
            return Err(ProcStoreError::Conflict {
                proc_id: proc.proc_id.clone(),
                field: "supervisor_id".to_string(),
                value: supervisor_id.to_string(),
                reason: format!("already claimed by {existing:?}"),
            });
        }
    }
    Ok(())
}

fn ensure_supervisor_matches(
    proc: &ProcWire,
    supervisor_id: &str,
) -> ProcStoreResult<()> {
    ensure_supervisor_owner(proc, supervisor_id)?;
    if proc.supervisor_id.is_none() {
        return Err(invalid_proc(
            proc,
            "proc has not been claimed by a supervisor".to_string(),
        ));
    }
    Ok(())
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

    use crate::procs::wire::XpromptProcMetaWire;

    use super::*;

    fn proc(proc_id: &str, status: &str, created_at: &str) -> ProcWire {
        ProcWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            proc_id: proc_id.to_string(),
            label: format!("Proc {proc_id}"),
            kind: "command".to_string(),
            status: status.to_string(),
            lifecycle: "legacy".to_string(),
            argv: vec!["true".to_string()],
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
            log_owner: STORE_LOG_OWNER.to_string(),
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

    fn reserve_request(
        proc_id: &str,
        shell_name: &str,
        fingerprint: &str,
    ) -> ProcReserveWire {
        ProcReserveWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            proc_id: proc_id.to_string(),
            label: format!("Proc {proc_id}"),
            kind: "detached".to_string(),
            argv: vec!["sleep".to_string(), "1".to_string()],
            cwd: "/tmp".to_string(),
            project: Some("sase".to_string()),
            workspace_num: Some(10),
            session_id: Some("session".to_string()),
            session_label: Some("ACE".to_string()),
            origin: "test".to_string(),
            cl_name: None,
            tags: vec!["beta".to_string(), "alpha".to_string()],
            created_at: "2026-07-25T12:00:00Z".to_string(),
            log_path: format!("/tmp/{proc_id}.log"),
            log_owner: STORE_LOG_OWNER.to_string(),
            shell_name: Some(shell_name.to_string()),
            shell_kind: Some("proc".to_string()),
            concurrency_keys: vec!["shared".to_string()],
            request_fingerprint: fingerprint.to_string(),
            reserved_by: "agent-one".to_string(),
            timeout_seconds: Some(30),
            idle_timeout_seconds: Some(10),
            xprompt_proc: None,
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
    fn xprompt_proc_meta_preserves_label_provenance() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let mut request = reserve_request("proc-meta", "checks", "fp-meta");
        request.xprompt_proc = Some(XpromptProcMetaWire {
            logical_id: Some("unit-1".to_string()),
            label: Some("Verify docs".to_string()),
            shell_name: Some("checks".to_string()),
            ..XpromptProcMetaWire::default()
        });

        let reserved = reserve_proc(&path, &request, 10).unwrap().proc;
        let meta = reserved.xprompt_proc.unwrap();
        assert_eq!(meta.label.as_deref(), Some("Verify docs"));
        assert_eq!(meta.shell_name.as_deref(), Some("checks"));

        let updated = update_proc(
            &path,
            &ProcUpdateWire {
                proc_id: "proc-meta".to_string(),
                xprompt_proc: Some(Some(XpromptProcMetaWire {
                    logical_id: Some("unit-1".to_string()),
                    label: Some("Verify docs".to_string()),
                    shell_name: Some("checks".to_string()),
                    code_preview: Some("just check".to_string()),
                    ..XpromptProcMetaWire::default()
                })),
                ..ProcUpdateWire::default()
            },
        )
        .unwrap()
        .proc
        .unwrap();
        let meta = updated.xprompt_proc.unwrap();
        assert_eq!(meta.label.as_deref(), Some("Verify docs"));
        assert_eq!(meta.shell_name.as_deref(), Some("checks"));
        assert_eq!(meta.code_preview.as_deref(), Some("just check"));
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
    fn reserve_replays_identical_shell_request_and_rejects_conflicts() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let first = reserve_proc(
            &path,
            &reserve_request("first", "agent--build", "fp-1"),
            10,
        )
        .unwrap();
        assert!(first.reserved);
        assert!(!first.replayed);
        assert_eq!(first.proc.status, "pending");
        assert_eq!(first.proc.lifecycle, PROC_SHELL_LIFECYCLE);
        assert_eq!(first.proc.argv, vec!["sleep".to_string(), "1".to_string()]);
        assert_eq!(first.proc.command, first.proc.argv);

        let replay = reserve_proc(
            &path,
            &reserve_request("other", "agent--build", "fp-1"),
            10,
        )
        .unwrap();
        assert!(!replay.reserved);
        assert!(replay.replayed);
        assert_eq!(replay.proc.proc_id, "first");

        let shell_conflict = reserve_proc(
            &path,
            &reserve_request("second", "agent--build", "fp-2"),
            10,
        )
        .unwrap_err();
        assert!(matches!(
            shell_conflict,
            ProcStoreError::Conflict { ref field, .. } if field == "shell_name"
        ));

        let mut key_request = reserve_request("third", "agent--test", "fp-3");
        key_request.concurrency_keys = vec!["shared".to_string()];
        let key_conflict = reserve_proc(&path, &key_request, 10).unwrap_err();
        assert!(matches!(
            key_conflict,
            ProcStoreError::Conflict { ref field, .. } if field == "concurrency_key"
        ));
    }

    #[test]
    fn proc_shell_lifecycle_requires_settlement_and_single_supervisor_finish() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        reserve_proc(
            &path,
            &reserve_request("managed", "agent--build", "fp-1"),
            10,
        )
        .unwrap();

        let early_finish = finish_proc(
            &path,
            &ProcFinishWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                status: "success".to_string(),
                finished_at: "2026-07-25T12:00:10Z".to_string(),
                exit_code: Some(0),
                message: None,
                result: Some(json!({"ok": true})),
            },
        )
        .unwrap_err();
        assert!(matches!(early_finish, ProcStoreError::InvalidProc { .. }));

        let claimed = claim_proc_supervisor(
            &path,
            &ProcSupervisorClaimWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                claimed_at: "2026-07-25T12:00:01Z".to_string(),
                pid: Some(123),
                pgid: Some(123),
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(claimed.status, "running");
        assert_eq!(claimed.supervisor_id.as_deref(), Some("supervisor-a"));

        let stop = request_proc_stop(
            &path,
            &ProcStopRequestWire {
                proc_id: "managed".to_string(),
                requested_by: "agent-one".to_string(),
                requested_at: "2026-07-25T12:00:02Z".to_string(),
                reason: Some("user".to_string()),
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(stop.status, "running");
        assert_eq!(stop.stop_requested_by.as_deref(), Some("agent-one"));

        let argv_update = update_proc(
            &path,
            &ProcUpdateWire {
                proc_id: "managed".to_string(),
                argv: Some(vec!["false".to_string()]),
                ..ProcUpdateWire::default()
            },
        )
        .unwrap_err();
        assert!(matches!(argv_update, ProcStoreError::InvalidProc { .. }));

        let settling = begin_proc_settlement(
            &path,
            &ProcSettlementWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                settling_at: "2026-07-25T12:00:03Z".to_string(),
                exit_code: Some(0),
                message: Some("done".to_string()),
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(settling.status, "settling");
        assert_eq!(
            settling.settling_started_at.as_deref(),
            Some("2026-07-25T12:00:03Z")
        );

        let finished = finish_proc(
            &path,
            &ProcFinishWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                status: "success".to_string(),
                finished_at: "2026-07-25T12:00:10Z".to_string(),
                exit_code: Some(0),
                message: Some("done".to_string()),
                result: Some(json!({"ok": true})),
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(finished.status, "success");
        assert_eq!(finished.finished_by.as_deref(), Some("supervisor-a"));
        assert_eq!(
            finished.settled_at.as_deref(),
            Some("2026-07-25T12:00:10Z")
        );

        let replay = finish_proc(
            &path,
            &ProcFinishWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                status: "success".to_string(),
                finished_at: "2026-07-25T12:00:10Z".to_string(),
                exit_code: Some(0),
                message: Some("done".to_string()),
                result: Some(json!({"ok": true})),
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(replay.status, "success");

        let wrong_owner = finish_proc(
            &path,
            &ProcFinishWire {
                proc_id: "managed".to_string(),
                supervisor_id: "supervisor-b".to_string(),
                status: "success".to_string(),
                finished_at: "2026-07-25T12:00:11Z".to_string(),
                exit_code: Some(0),
                message: None,
                result: None,
            },
        )
        .unwrap_err();
        assert!(matches!(
            wrong_owner,
            ProcStoreError::Conflict { ref field, .. } if field == "supervisor_id"
        ));
    }

    #[test]
    fn legacy_commandless_tui_rows_remain_readable() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        fs::write(
            &path,
            json!({
                "proc_id": "legacy-tui",
                "label": "TUI",
                "kind": "tui",
                "status": "running",
                "command": [],
                "cwd": "/tmp",
                "project": "sase",
                "workspace_num": 1,
                "session_id": "session",
                "session_label": "ACE",
                "origin": "test",
                "cl_name": null,
                "tags": [],
                "pid": null,
                "pgid": null,
                "exit_code": null,
                "phase": null,
                "message": null,
                "created_at": "2026-07-25T12:00:00Z",
                "started_at": null,
                "finished_at": null,
                "log_path": "/tmp/legacy-tui.log"
            })
            .to_string()
                + "\n",
        )
        .unwrap();

        let snapshot = read_procs_snapshot(&path).unwrap();
        assert_eq!(snapshot.procs.len(), 1);
        assert_eq!(snapshot.procs[0].proc_id, "legacy-tui");
        assert!(snapshot.procs[0].argv.is_empty());
    }

    #[test]
    fn retention_reports_only_store_owned_logs_for_deletion() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        let store_owned = proc("store-log", "success", "2026-07-25T12:00:00Z");
        let mut artifact_owned =
            proc("artifact-log", "success", "2026-07-25T12:00:01Z");
        artifact_owned.log_owner = "artifact".to_string();
        let newest = proc("newest", "success", "2026-07-25T12:00:02Z");

        append_proc(&path, &store_owned, 10).unwrap();
        append_proc(&path, &artifact_owned, 10).unwrap();
        append_proc(&path, &newest, 10).unwrap();

        let outcome = prune_procs(&path, 1).unwrap();
        assert_eq!(outcome.pruned_proc_ids, vec!["store-log", "artifact-log"]);
        assert_eq!(outcome.pruned_log_proc_ids, vec!["store-log"]);
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

//! Retention preview, apply, and log reclamation.
//!
//! Owns the retention transaction, settled-run log candidate selection, and
//! quarantined-store discovery. Filesystem log accounting lives here, not in
//! the query side.

use super::super::wire::{
    ToolRunDeletionCandidateWire, ToolRunRetentionPolicyWire,
    ToolRunRetentionRequestWire, ToolRunRetentionResultWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    touch_write_meta, unix_now, validate_schema, with_read_store,
    with_write_store,
};
use rusqlite::{Connection, TransactionBehavior};
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn retention_preview(
    store_path: &Path,
    request: ToolRunRetentionRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunRetentionResultWire, ToolRunError> {
    let mut preview = request;
    preview.dry_run = true;
    retention(store_path, preview, busy_timeout)
}

pub fn retention_apply(
    store_path: &Path,
    request: ToolRunRetentionRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunRetentionResultWire, ToolRunError> {
    let mut apply = request;
    apply.dry_run = false;
    retention(store_path, apply, busy_timeout)
}

fn retention(
    store_path: &Path,
    request: ToolRunRetentionRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunRetentionResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_retention_policy(&request.policy)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    let summary_cut =
        now.saturating_sub(i64::from(request.policy.summary_days) * 86400);
    let detail_cut =
        now.saturating_sub(i64::from(request.policy.detail_days) * 86400);
    let log_cut =
        now.saturating_sub(i64::from(request.policy.log_days) * 86400);
    // Quarantined stores match no row, event, or log selector, so scan them
    // from the filesystem once, outside the store transaction. They are
    // reclaimed at the explicit log horizon: age is read from the quarantine
    // timestamp in the file name (the rename moment), not from mtime (the
    // last live write, which can long predate the corruption).
    let quarantine = select_quarantined_stores(store_path, log_cut);
    if !store_path.exists() {
        // Orphaned quarantines stay reclaimable without a live ledger.
        let retained_bytes = quarantine.remaining_bytes;
        return Ok(ToolRunRetentionResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            dry_run: request.dry_run,
            summary_rows: 0,
            detail_rows: 0,
            file_candidates: quarantine.candidates.clone(),
            protected_unsettled: 0,
            retained_bytes,
            protected_bytes: 0,
            over_target_bytes: retained_bytes
                .saturating_sub(request.policy.log_max_bytes),
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    let inspect = |conn: &Connection| -> Result<ToolRunRetentionResultWire, ToolRunError> {
        let protected_unsettled: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runs WHERE state IN ('created', 'running')",
            [],
            |row| row.get(0),
        )?;
        let summary_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM runs
             WHERE state NOT IN ('created', 'running')
               AND settled_ts IS NOT NULL
               AND settled_ts < ?1",
            [summary_cut],
            |row| row.get(0),
        )?;
        let detail_rows: i64 = conn.query_row(
            "SELECT
                (SELECT COUNT(*) FROM events e
                    JOIN runs r ON r.run_id = e.run_id
                    WHERE r.state NOT IN ('created', 'running')
                      AND e.kind IN ('stage_started', 'stage_finished', 'sample')
                      AND e.created_ts < ?1)
              + (SELECT COUNT(*) FROM stages s
                    JOIN runs r ON r.run_id = s.run_id
                    WHERE r.state NOT IN ('created', 'running')
                      AND COALESCE(s.finished_ts, s.started_ts, 0) < ?1)
              + (SELECT COUNT(*) FROM samples m
                    JOIN runs r ON r.run_id = m.run_id
                    WHERE r.state NOT IN ('created', 'running')
                      AND m.observed_ts < ?1)",
            [detail_cut],
            |row| row.get(0),
        )?;
        let mut file_candidates = Vec::new();
        let mut stmt = conn.prepare(
            "SELECT run_id, log_stdout_path, log_stderr_path, events_path
             FROM runs
             WHERE state NOT IN ('created', 'running')
               AND settled_ts IS NOT NULL
               AND settled_ts < ?1",
        )?;
        let rows = stmt.query_map([log_cut], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            let (run_id, stdout, stderr, events) = row?;
            for (kind, path) in [
                ("stdout", stdout),
                ("stderr", stderr),
                ("events", events),
            ] {
                if let Some(path) = path {
                    file_candidates.push(ToolRunDeletionCandidateWire {
                        kind: kind.to_string(),
                        run_id: Some(run_id.clone()),
                        path: Some(path),
                        protected: false,
                        reason: "log_days elapsed after settlement".to_string(),
                    });
                }
            }
        }
        let usage = select_aggregate_log_candidates(
            conn,
            &mut file_candidates,
            request.policy.log_max_bytes,
        )?;
        // Rust only selects quarantine files; the `tool_run_retention`
        // reaper deletes the listed paths. Remaining (young or unreadable)
        // quarantine bytes stay in retained accounting so the report keeps
        // honest physical-byte totals.
        file_candidates.extend(quarantine.candidates.iter().cloned());
        let retained_bytes = usage
            .retained_bytes
            .saturating_add(quarantine.remaining_bytes);
        Ok(ToolRunRetentionResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            dry_run: request.dry_run,
            summary_rows: summary_rows as u64,
            detail_rows: detail_rows as u64,
            file_candidates,
            protected_unsettled: protected_unsettled as u64,
            retained_bytes,
            protected_bytes: usage.protected_bytes,
            over_target_bytes: retained_bytes
                .saturating_sub(request.policy.log_max_bytes),
            diagnostics: Vec::new(),
        })
    };
    if request.dry_run {
        return with_read_store(store_path, busy_timeout, inspect);
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let report = inspect(&tx)?;
        tx.execute(
            "DELETE FROM samples WHERE sample_id IN (
                SELECT m.sample_id FROM samples m
                JOIN runs r ON r.run_id = m.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND m.observed_ts < ?1
            )",
            [detail_cut],
        )?;
        tx.execute(
            "DELETE FROM stages WHERE stage_id IN (
                SELECT s.stage_id FROM stages s
                JOIN runs r ON r.run_id = s.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND COALESCE(s.finished_ts, s.started_ts, 0) < ?1
            )",
            [detail_cut],
        )?;
        tx.execute(
            "DELETE FROM events WHERE event_id IN (
                SELECT e.event_id FROM events e
                JOIN runs r ON r.run_id = e.run_id
                WHERE r.state NOT IN ('created', 'running')
                  AND e.kind IN ('stage_started', 'stage_finished', 'sample')
                  AND e.created_ts < ?1
            )",
            [detail_cut],
        )?;
        tx.execute(
            "DELETE FROM events WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
            [summary_cut],
        )?;
        tx.execute(
            "DELETE FROM stages WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
            [summary_cut],
        )?;
        tx.execute(
            "DELETE FROM samples WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
            [summary_cut],
        )?;
        tx.execute(
            "DELETE FROM attempts WHERE run_id IN (
                SELECT run_id FROM runs
                WHERE state NOT IN ('created', 'running')
                  AND settled_ts IS NOT NULL
                  AND settled_ts < ?1
            )",
            [summary_cut],
        )?;
        tx.execute(
            "DELETE FROM runs
             WHERE state NOT IN ('created', 'running')
               AND settled_ts IS NOT NULL
               AND settled_ts < ?1",
            [summary_cut],
        )?;
        touch_write_meta(&tx, now)?;
        tx.commit()?;
        Ok(ToolRunRetentionResultWire {
            dry_run: false,
            ..report
        })
    })
}

/// One settled run's retained files that survived age-based selection.
struct SettledRunLogs {
    run_id: String,
    bytes: u64,
    files: Vec<(&'static str, String)>,
}

struct AggregateLogUsage {
    retained_bytes: u64,
    protected_bytes: u64,
}

/// Size of a retained file without following symlinks; anything that is not a
/// regular file (missing, symlink, directory) counts as zero bytes.
fn retained_file_bytes(path: &str) -> u64 {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => metadata.len(),
        _ => 0,
    }
}

/// Add the oldest settled runs' files as candidates until the retained bytes fit
/// `log_max_bytes`. Unsettled runs are never selected; when their bytes alone
/// exceed the target the excess is reported, not hidden.
fn select_aggregate_log_candidates(
    conn: &Connection,
    candidates: &mut Vec<ToolRunDeletionCandidateWire>,
    log_max_bytes: u64,
) -> Result<AggregateLogUsage, ToolRunError> {
    let already: HashSet<String> = candidates
        .iter()
        .filter_map(|candidate| candidate.path.clone())
        .collect();
    let mut stmt = conn.prepare(
        "SELECT run_id, state, log_stdout_path, log_stderr_path, events_path
         FROM runs
         WHERE log_stdout_path IS NOT NULL
            OR log_stderr_path IS NOT NULL
            OR events_path IS NOT NULL
         ORDER BY COALESCE(settled_ts, created_ts), run_id",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            [
                ("stdout", row.get::<_, Option<String>>(2)?),
                ("stderr", row.get::<_, Option<String>>(3)?),
                ("events", row.get::<_, Option<String>>(4)?),
            ],
        ))
    })?;
    let mut protected_bytes = 0u64;
    let mut settled: Vec<SettledRunLogs> = Vec::new();
    for row in rows {
        let (run_id, state, files) = row?;
        let unsettled = state == "created" || state == "running";
        let mut run_bytes = 0u64;
        let mut run_files = Vec::new();
        for (kind, path) in files {
            let Some(path) = path else { continue };
            if already.contains(&path) {
                continue;
            }
            run_bytes += retained_file_bytes(&path);
            run_files.push((kind, path));
        }
        if unsettled {
            protected_bytes += run_bytes;
        } else if run_bytes > 0 {
            settled.push(SettledRunLogs {
                run_id,
                bytes: run_bytes,
                files: run_files,
            });
        }
    }
    let mut retained =
        protected_bytes + settled.iter().map(|run| run.bytes).sum::<u64>();
    for run in settled {
        if retained <= log_max_bytes {
            break;
        }
        for (kind, path) in run.files {
            candidates.push(ToolRunDeletionCandidateWire {
                kind: kind.to_string(),
                run_id: Some(run.run_id.clone()),
                path: Some(path),
                protected: false,
                reason: "log_max_bytes aggregate target exceeded".to_string(),
            });
        }
        retained = retained.saturating_sub(run.bytes);
    }
    Ok(AggregateLogUsage {
        retained_bytes: retained,
        protected_bytes,
    })
}

/// Quarantined corrupt stores selected for reclamation plus the quarantine
/// bytes that stay retained.
struct QuarantinedStoreUsage {
    candidates: Vec<ToolRunDeletionCandidateWire>,
    remaining_bytes: u64,
}

/// Select quarantined `<store>.corrupt-*` siblings older than `log_cut`.
///
/// Quarantined stores are dead full copies of the ledger: they match no row,
/// event, or log selector, so without this they accumulate forever under the
/// tools directory. Rust only selects; the `tool_run_retention` reaper
/// deletes the listed paths. Files whose quarantine timestamp is unreadable
/// are retained and counted but never selected.
fn select_quarantined_stores(
    store_path: &Path,
    log_cut: i64,
) -> QuarantinedStoreUsage {
    let mut usage = QuarantinedStoreUsage {
        candidates: Vec::new(),
        remaining_bytes: 0,
    };
    let file_name = store_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "runs.sqlite".to_string());
    let prefix = format!("{file_name}.corrupt-");
    let Some(dir) = store_path
        .parent()
        .filter(|dir| !dir.as_os_str().is_empty())
    else {
        return usage;
    };
    let Ok(entries) = fs::read_dir(dir) else {
        return usage;
    };
    let mut selected: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let Ok(entry) = entry else { continue };
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(&prefix) {
            continue;
        }
        let path = entry.path();
        let bytes = match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_file() => meta.len(),
            _ => continue,
        };
        match quarantine_timestamp_secs(&name, &prefix) {
            Some(quarantined_ts) if quarantined_ts < log_cut => {
                selected.push(path);
            }
            _ => {
                usage.remaining_bytes =
                    usage.remaining_bytes.saturating_add(bytes);
            }
        }
    }
    selected.sort();
    for path in selected {
        usage.candidates.push(ToolRunDeletionCandidateWire {
            kind: "quarantined_store".to_string(),
            run_id: None,
            path: Some(path.to_string_lossy().into_owned()),
            protected: false,
            reason: "log_days elapsed since quarantine".to_string(),
        });
    }
    usage
}

/// Seconds since the epoch when `name` was quarantined, parsed from the
/// `.corrupt-<nanos>` suffix (`-wal`/`-shm` sidecars included). Returns
/// `None` when the suffix is missing or malformed.
fn quarantine_timestamp_secs(name: &str, prefix: &str) -> Option<i64> {
    let rest = name.strip_prefix(prefix)?;
    let rest = rest
        .strip_suffix("-wal")
        .or_else(|| rest.strip_suffix("-shm"))
        .unwrap_or(rest);
    let nanos = rest.parse::<u128>().ok()?;
    i64::try_from(nanos / 1_000_000_000).ok()
}

fn validate_retention_policy(
    policy: &ToolRunRetentionPolicyWire,
) -> Result<(), ToolRunError> {
    validate_schema(policy.schema_version)?;
    if policy.summary_days == 0
        || policy.detail_days == 0
        || policy.log_days == 0
        || policy.log_max_bytes == 0
        || policy.run_log_max_bytes == 0
        || policy.event_max_bytes == 0
    {
        return Err(ToolRunError::invalid(
            "tool run retention limits must be positive",
        ));
    }
    if policy.detail_days > policy.summary_days {
        return Err(ToolRunError::invalid(
            "detail_days must be <= summary_days",
        ));
    }
    if policy.log_days > policy.detail_days {
        return Err(ToolRunError::invalid("log_days must be <= detail_days"));
    }
    Ok(())
}

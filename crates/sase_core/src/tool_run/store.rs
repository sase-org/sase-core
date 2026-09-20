//! SQLite ToolRun store: event+projection transactions, reconciliation,
//! retention, and query side-effect boundaries.

use std::collections::HashSet;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::{rngs::OsRng, RngCore};
use rusqlite::{
    params, Connection, ErrorCode, OpenFlags, OptionalExtension, Transaction,
    TransactionBehavior,
};

use crate::store_lock::{acquire_store_lock, LockMode, StoreLockError};

use super::catalog::{extra_args_digest, normalize_tool_definition};
use super::fingerprint::{canonicalize_tool_fingerprint, fingerprint_digest};
use super::wire::{
    ToolAttemptWire, ToolEvidenceCompletenessWire, ToolFingerprintWire,
    ToolLivenessObservationWire, ToolLoadSampleWire, ToolRunAppendRequestWire,
    ToolRunAppendResultWire, ToolRunBeginRequestWire, ToolRunBeginResultWire,
    ToolRunDeletionCandidateWire, ToolRunEventKindWire, ToolRunEventWire,
    ToolRunExecutorWire, ToolRunFinishRequestWire, ToolRunFinishResultWire,
    ToolRunListRequestWire, ToolRunListResultWire, ToolRunLogMetadataWire,
    ToolRunReconcileRequestWire, ToolRunReconcileResultWire,
    ToolRunRetentionPolicyWire, ToolRunRetentionRequestWire,
    ToolRunRetentionResultWire, ToolRunShowRequestWire, ToolRunShowResultWire,
    ToolRunSourceWire, ToolRunStateWire, ToolRunStoreStatsWire,
    ToolRunSummaryRequestWire, ToolRunSummaryResultWire, ToolRunWire,
    ToolStageWire, TOOL_RUN_DEFAULT_BUSY_TIMEOUT, TOOL_RUN_LIST_MAX_LIMIT,
    TOOL_RUN_LOST_REASON_RUNNER_EXITED, TOOL_RUN_MAX_BUSY_TIMEOUT,
    TOOL_RUN_TYPICAL_SAMPLE_LIMIT, TOOL_RUN_TYPICAL_WINDOW_DAYS,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::{lock_paths, ToolRunError};

const SCHEMA_SQL: &str = r#"
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    state TEXT NOT NULL,
    source TEXT NOT NULL,
    executor TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 1,
    tool_name TEXT,
    definition_digest TEXT NOT NULL,
    extra_args_digest TEXT NOT NULL,
    display_argv_json TEXT NOT NULL,
    private_argv_json TEXT,
    project TEXT,
    agent TEXT,
    workspace TEXT,
    bead TEXT,
    owner_kind TEXT,
    owner_id TEXT,
    parent_run_id TEXT,
    created_ts INTEGER NOT NULL,
    running_ts INTEGER,
    settled_ts INTEGER,
    duration_ms INTEGER,
    duration_missing TEXT,
    exit_code INTEGER,
    signal INTEGER,
    interruption_reason TEXT,
    lost_reason TEXT,
    wrapper_pid INTEGER,
    boot_id TEXT,
    process_start_identity TEXT,
    child_pid INTEGER,
    child_pgid INTEGER,
    mutated_input INTEGER,
    fingerprint_before_json TEXT,
    fingerprint_after_json TEXT,
    log_stdout_path TEXT,
    log_stderr_path TEXT,
    events_path TEXT,
    evidence_json TEXT NOT NULL,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (parent_run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS attempts (
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL,
    started_ts INTEGER,
    settled_ts INTEGER,
    exit_code INTEGER,
    signal INTEGER,
    diagnostics_json TEXT NOT NULL,
    PRIMARY KEY (run_id, attempt),
    FOREIGN KEY (run_id) REFERENCES runs(run_id)
);
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    kind TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_ts INTEGER NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS stages (
    stage_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    description TEXT NOT NULL,
    started_ts INTEGER,
    finished_ts INTEGER,
    elapsed_ms INTEGER,
    exit_code INTEGER,
    output_bytes INTEGER,
    incomplete INTEGER NOT NULL DEFAULT 1,
    diagnostics_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE TABLE IF NOT EXISTS samples (
    sample_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    observed_ts INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    FOREIGN KEY (run_id, attempt) REFERENCES attempts(run_id, attempt)
);
CREATE INDEX IF NOT EXISTS idx_tool_runs_created
    ON runs(created_ts DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_project_tool
    ON runs(project, tool_name, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_runs_state
    ON runs(state, created_ts DESC);
CREATE INDEX IF NOT EXISTS idx_tool_events_run
    ON events(run_id, created_ts);
CREATE INDEX IF NOT EXISTS idx_tool_stages_run
    ON stages(run_id, started_ts);
CREATE INDEX IF NOT EXISTS idx_tool_samples_run
    ON samples(run_id, observed_ts);
"#;

pub fn begin(
    store_path: &Path,
    request: ToolRunBeginRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunBeginResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    let run_id = nonempty_or_generated(request.run_id.as_deref());
    let created_event_id =
        nonempty_or_generated(request.created_event_id.as_deref());
    let running_event_id = if request.commit_running {
        Some(nonempty_or_generated(request.running_event_id.as_deref()))
    } else {
        None
    };
    let (tool_name, definition_digest, extra_args_digest) =
        identity_for_begin(&request)?;
    if let Some(parent) = request.parent_run_id.as_deref() {
        if parent == run_id {
            return Err(ToolRunError::invalid(
                "parent_run_id must not equal run_id",
            ));
        }
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(parent) = request.parent_run_id.as_deref() {
            let exists: Option<String> = tx
                .query_row(
                    "SELECT run_id FROM runs WHERE run_id = ?1",
                    [parent],
                    |row| row.get(0),
                )
                .optional()?;
            if exists.is_none() {
                return Err(ToolRunError::invalid(format!(
                    "parent run {parent} was not found"
                )));
            }
        }
        let state = if request.commit_running {
            ToolRunStateWire::Running
        } else {
            ToolRunStateWire::Created
        };
        insert_run(
            &tx,
            &run_id,
            state,
            &tool_name,
            &definition_digest,
            &extra_args_digest,
            &request,
            now,
        )?;
        tx.execute(
            "INSERT INTO attempts(
                run_id, attempt, state, started_ts, settled_ts, exit_code,
                signal, diagnostics_json
             ) VALUES (?1, 1, ?2, ?3, NULL, NULL, NULL, '[]')",
            params![
                run_id,
                state.as_str(),
                if request.commit_running {
                    Some(now)
                } else {
                    None
                }
            ],
        )?;
        insert_lifecycle_event(
            &tx,
            &created_event_id,
            &run_id,
            ToolRunEventKindWire::Created,
            now,
            None,
            None,
            None,
        )?;
        if let Some(running_id) = running_event_id.as_deref() {
            insert_lifecycle_event(
                &tx,
                running_id,
                &run_id,
                ToolRunEventKindWire::Running,
                now,
                None,
                None,
                None,
            )?;
        }
        touch_write_meta(&tx, now)?;
        let run = load_run(&tx, &run_id)?.expect("inserted run");
        tx.commit()?;
        Ok(ToolRunBeginResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run,
            created_event_id: created_event_id.clone(),
            running_event_id: running_event_id.clone(),
            diagnostics: Vec::new(),
        })
    })
}

pub fn append_event(
    store_path: &Path,
    request: ToolRunAppendRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunAppendResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_schema(request.event.schema_version)?;
    if request.event.event_id.trim().is_empty() {
        return Err(ToolRunError::invalid("event_id must not be empty"));
    }
    if request.event.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let replayed = ingest_event(&tx, &request.event)?;
        touch_write_meta(&tx, request.event.created_ts)?;
        let run = load_run(&tx, &request.event.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.event.run_id.clone(),
            }
        })?;
        tx.commit()?;
        Ok(ToolRunAppendResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event_id: request.event.event_id.clone(),
            replayed,
            run,
            diagnostics: Vec::new(),
        })
    })
}

pub fn finish(
    store_path: &Path,
    request: ToolRunFinishRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunFinishResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if !request.state.is_terminal() {
        return Err(ToolRunError::invalid(format!(
            "finish state {} is not terminal",
            request.state.as_str()
        )));
    }
    let event_id = nonempty_or_generated(request.event_id.as_deref());
    let now = request.now_ts.unwrap_or_else(unix_now);
    let event = ToolRunEventWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        event_id: event_id.clone(),
        run_id: request.run_id.clone(),
        attempt: 1,
        kind: match request.state {
            ToolRunStateWire::Succeeded => ToolRunEventKindWire::Succeeded,
            ToolRunStateWire::Failed => ToolRunEventKindWire::Failed,
            ToolRunStateWire::Signaled => ToolRunEventKindWire::Signaled,
            ToolRunStateWire::Interrupted => ToolRunEventKindWire::Interrupted,
            ToolRunStateWire::Lost => ToolRunEventKindWire::Lost,
            other => {
                return Err(ToolRunError::invalid(format!(
                    "cannot finish in state {}",
                    other.as_str()
                )));
            }
        },
        created_ts: now,
        stage: None,
        sample: None,
        exit_code: request.exit_code,
        signal: request.signal,
        reason: request
            .lost_reason
            .clone()
            .or(request.interruption_reason.clone()),
        diagnostics: request.diagnostics.clone(),
    };
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ingest_event(&tx, &event)?;
        if request.child_pid.is_some() || request.child_pgid.is_some() {
            tx.execute(
                "UPDATE runs SET child_pid = COALESCE(?2, child_pid),
                    child_pgid = COALESCE(?3, child_pgid)
                 WHERE run_id = ?1",
                params![request.run_id, request.child_pid, request.child_pgid],
            )?;
        }
        if let Some(duration_ms) = request.duration_ms {
            tx.execute(
                "UPDATE runs SET duration_ms = ?2, duration_missing = NULL
                 WHERE run_id = ?1",
                params![request.run_id, duration_ms],
            )?;
        }
        let fingerprint_before = persist_optional_fingerprint(
            &tx,
            &request.run_id,
            "fingerprint_before_json",
            request.fingerprint_before.as_ref(),
        )?;
        let fingerprint_after = persist_optional_fingerprint(
            &tx,
            &request.run_id,
            "fingerprint_after_json",
            request.fingerprint_after.as_ref(),
        )?;
        let stored = load_run(&tx, &request.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            }
        })?;
        let before = fingerprint_before.or(stored.fingerprint_before);
        let after = fingerprint_after.or(stored.fingerprint_after);
        let mutated = request.mutated_input.or_else(|| {
            mutated_input_from_fingerprints(before.as_ref(), after.as_ref())
        });
        if let Some(mutated) = mutated {
            tx.execute(
                "UPDATE runs SET mutated_input = ?2 WHERE run_id = ?1",
                params![request.run_id, i64::from(mutated)],
            )?;
        }
        let evidence =
            evidence_from_fingerprints(before.as_ref(), after.as_ref());
        tx.execute(
            "UPDATE runs SET evidence_json = ?2 WHERE run_id = ?1",
            params![
                request.run_id,
                serde_json::to_string(&evidence)
                    .map_err(|error| ToolRunError::store(error.to_string()))?
            ],
        )?;
        touch_write_meta(&tx, now)?;
        let run = load_run(&tx, &request.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            }
        })?;
        tx.commit()?;
        Ok(ToolRunFinishResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run,
            event_id: event_id.clone(),
            diagnostics: Vec::new(),
        })
    })
}

pub fn reconcile(
    store_path: &Path,
    request: ToolRunReconcileRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunReconcileResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    if !store_path.exists() {
        return Ok(ToolRunReconcileResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            marked_lost: Vec::new(),
            persisted: false,
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    match with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut marked = Vec::new();
        let mut diagnostics = Vec::new();
        for fact in &request.facts {
            match fact.observation {
                ToolLivenessObservationWire::Unknown => {
                    diagnostics.push(format!(
                        "liveness for {} is unknown; not proof of death",
                        fact.run_id
                    ));
                }
                ToolLivenessObservationWire::Alive => {}
                ToolLivenessObservationWire::Dead => {
                    if let Some(run) = load_run(&tx, &fact.run_id)? {
                        if run.state.is_unsettled() {
                            let event_id = new_hex_id();
                            let event = ToolRunEventWire {
                                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                                event_id,
                                run_id: fact.run_id.clone(),
                                attempt: 1,
                                kind: ToolRunEventKindWire::Lost,
                                created_ts: now,
                                stage: None,
                                sample: None,
                                exit_code: None,
                                signal: None,
                                reason: Some(
                                    fact.reason.clone().unwrap_or_else(|| {
                                        TOOL_RUN_LOST_REASON_RUNNER_EXITED
                                            .to_string()
                                    }),
                                ),
                                diagnostics: Vec::new(),
                            };
                            ingest_event(&tx, &event)?;
                            marked.push(fact.run_id.clone());
                        }
                    }
                }
            }
        }
        touch_write_meta(&tx, now)?;
        tx.commit()?;
        Ok(ToolRunReconcileResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            marked_lost: marked,
            persisted: true,
            diagnostics,
        })
    }) {
        Ok(result) => Ok(result),
        Err(ToolRunError::Busy { message })
        | Err(ToolRunError::ReadOnly { message }) => {
            Ok(ToolRunReconcileResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                marked_lost: Vec::new(),
                persisted: false,
                diagnostics: vec![format!(
                    "reconciliation could not persist: {message}"
                )],
            })
        }
        Err(error) => Err(error),
    }
}

pub fn list_runs(
    store_path: &Path,
    request: ToolRunListRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunListResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.limit == 0 || request.limit > TOOL_RUN_LIST_MAX_LIMIT {
        return Err(ToolRunError::invalid(format!(
            "limit must be 1..={TOOL_RUN_LIST_MAX_LIMIT}"
        )));
    }
    if !store_path.exists() {
        return Ok(ToolRunListResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            runs: Vec::new(),
            truncated: false,
            next_cursor: None,
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        let mut sql = String::from("SELECT run_id FROM runs WHERE 1=1");
        let mut values: Vec<String> = Vec::new();
        if let Some(tool) = &request.tool {
            sql.push_str(" AND tool_name = ?");
            values.push(tool.clone());
        }
        if let Some(state) = request.state {
            sql.push_str(" AND state = ?");
            values.push(state.as_str().to_string());
        }
        if let Some(agent) = &request.agent {
            sql.push_str(" AND agent = ?");
            values.push(agent.clone());
        }
        if let Some(project) = &request.project {
            sql.push_str(" AND project = ?");
            values.push(project.clone());
        }
        if let Some(cursor) = &request.cursor {
            let (ts, run_id) = parse_cursor(cursor)?;
            sql.push_str(
                " AND (created_ts < ? OR (created_ts = ? AND run_id < ?))",
            );
            values.push(ts.to_string());
            values.push(ts.to_string());
            values.push(run_id);
        }
        sql.push_str(" ORDER BY created_ts DESC, run_id DESC LIMIT ?");
        let fetch = request.limit as usize + 1;
        values.push(fetch.to_string());
        let mut stmt = conn.prepare(&sql)?;
        let ids = stmt
            .query_map(rusqlite::params_from_iter(values.iter()), |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let truncated = ids.len() > request.limit as usize;
        let ids = if truncated {
            ids[..request.limit as usize].to_vec()
        } else {
            ids
        };
        let mut runs = Vec::new();
        for id in &ids {
            if let Some(run) = load_run(conn, id)? {
                runs.push(run);
            }
        }
        let next_cursor = if truncated {
            runs.last()
                .map(|run| format!("{}:{}", run.created_ts, run.run_id))
        } else {
            None
        };
        Ok(ToolRunListResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            runs,
            truncated,
            next_cursor,
            diagnostics: Vec::new(),
        })
    })
}

pub fn show_run(
    store_path: &Path,
    request: ToolRunShowRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunShowResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if !store_path.exists() {
        return Ok(ToolRunShowResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run: None,
            attempt: None,
            events: Vec::new(),
            stages: Vec::new(),
            samples: Vec::new(),
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        let Some(run) = load_run(conn, &request.run_id)? else {
            return Ok(ToolRunShowResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run: None,
                attempt: None,
                events: Vec::new(),
                stages: Vec::new(),
                samples: Vec::new(),
                diagnostics: vec![format!(
                    "tool run {} was not found",
                    request.run_id
                )],
            });
        };
        Ok(ToolRunShowResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            attempt: load_attempt(conn, &request.run_id)?,
            events: load_events(conn, &request.run_id)?,
            stages: load_stages(conn, &request.run_id)?,
            samples: load_samples(conn, &request.run_id)?,
            run: Some(run),
            diagnostics: Vec::new(),
        })
    })
}

pub fn summarize(
    store_path: &Path,
    request: ToolRunSummaryRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunSummaryResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    if !store_path.exists() {
        return Ok(empty_summary("tool run store does not exist"));
    }
    with_read_store(store_path, busy_timeout, |conn| {
        let last_id: Option<String> = conn
            .query_row(
                "SELECT run_id FROM runs
                 WHERE project = ?1 AND tool_name = ?2
                 ORDER BY created_ts DESC, run_id DESC LIMIT 1",
                params![request.project, request.tool_name],
                |row| row.get(0),
            )
            .optional()?;
        let last = match last_id {
            Some(id) => load_run(conn, &id)?,
            None => None,
        };
        let empty_extra = extra_args_digest(&[])?;
        let window_start =
            now.saturating_sub(i64::from(TOOL_RUN_TYPICAL_WINDOW_DAYS) * 86400);
        let mut stmt = conn.prepare(
            "SELECT run_id FROM runs
             WHERE project = ?1 AND tool_name = ?2
               AND definition_digest = ?3
               AND extra_args_digest = ?4
               AND source = 'native'
               AND state IN ('succeeded', 'failed')
               AND created_ts >= ?5
             ORDER BY created_ts DESC, run_id DESC
             LIMIT ?6",
        )?;
        let ids = stmt
            .query_map(
                params![
                    request.project,
                    request.tool_name,
                    request.definition_digest,
                    empty_extra,
                    window_start,
                    TOOL_RUN_TYPICAL_SAMPLE_LIMIT
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<Result<Vec<_>, _>>()?;
        let mut durations = Vec::new();
        let mut breakdown = std::collections::BTreeMap::new();
        for id in ids {
            if let Some(run) = load_run(conn, &id)? {
                *breakdown
                    .entry(run.state.as_str().to_string())
                    .or_insert(0) += 1;
                if let Some(duration) = run.duration_ms {
                    durations.push(duration);
                }
            }
        }
        durations.sort_unstable();
        let typical = if durations.is_empty() {
            None
        } else {
            Some(durations[durations.len() / 2])
        };
        let mut diagnostics = Vec::new();
        if typical.is_none() {
            diagnostics.push(
                "no typical duration samples; duration is unknown, not zero"
                    .to_string(),
            );
        }
        Ok(ToolRunSummaryResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            last,
            typical_duration_ms: typical,
            typical_sample_count: durations.len() as u32,
            typical_status_breakdown: breakdown,
            diagnostics,
        })
    })
}

pub fn store_stats(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<ToolRunStoreStatsWire, ToolRunError> {
    if !store_path.exists() {
        return Ok(ToolRunStoreStatsWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_path: store_path.to_string_lossy().into_owned(),
            exists: false,
            db_size_bytes: 0,
            run_count: 0,
            attempt_count: 0,
            event_count: 0,
            stage_count: 0,
            sample_count: 0,
            unsettled_count: 0,
            last_write_ts: None,
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        Ok(ToolRunStoreStatsWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_path: store_path.to_string_lossy().into_owned(),
            exists: true,
            db_size_bytes: fs::metadata(store_path)
                .map(|meta| meta.len())
                .unwrap_or(0),
            run_count: count_rows(conn, "runs")?,
            attempt_count: count_rows(conn, "attempts")?,
            event_count: count_rows(conn, "events")?,
            stage_count: count_rows(conn, "stages")?,
            sample_count: count_rows(conn, "samples")?,
            unsettled_count: conn.query_row(
                "SELECT COUNT(*) FROM runs WHERE state IN ('created', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )? as u64,
            last_write_ts: read_meta_i64(conn, "last_write_ts")?,
            diagnostics: Vec::new(),
        })
    })
}

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
    if !store_path.exists() {
        return Ok(ToolRunRetentionResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            dry_run: request.dry_run,
            summary_rows: 0,
            detail_rows: 0,
            file_candidates: Vec::new(),
            protected_unsettled: 0,
            retained_bytes: 0,
            protected_bytes: 0,
            over_target_bytes: 0,
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let summary_cut =
        now.saturating_sub(i64::from(request.policy.summary_days) * 86400);
    let detail_cut =
        now.saturating_sub(i64::from(request.policy.detail_days) * 86400);
    let log_cut =
        now.saturating_sub(i64::from(request.policy.log_days) * 86400);
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
        Ok(ToolRunRetentionResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            dry_run: request.dry_run,
            summary_rows: summary_rows as u64,
            detail_rows: detail_rows as u64,
            file_candidates,
            protected_unsettled: protected_unsettled as u64,
            retained_bytes: usage.retained_bytes,
            protected_bytes: usage.protected_bytes,
            over_target_bytes: usage.over_target_bytes,
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
    over_target_bytes: u64,
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
        over_target_bytes: retained.saturating_sub(log_max_bytes),
    })
}

fn identity_for_begin(
    request: &ToolRunBeginRequestWire,
) -> Result<(Option<String>, String, String), ToolRunError> {
    let extra = extra_args_digest(&request.extra_args)?;
    if let Some(name) = &request.tool_name {
        let mut definition = request.definition.clone();
        if definition.name.is_empty() {
            definition.name = name.clone();
        }
        let normalized = normalize_tool_definition(definition)?;
        Ok((Some(normalized.definition.name), normalized.digest, extra))
    } else {
        let argv = request
            .private_argv
            .as_ref()
            .unwrap_or(&request.definition.argv);
        Ok((None, extra_args_digest(argv)?, extra))
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_run(
    tx: &Transaction<'_>,
    run_id: &str,
    state: ToolRunStateWire,
    tool_name: &Option<String>,
    definition_digest: &str,
    extra_args_digest: &str,
    request: &ToolRunBeginRequestWire,
    now: i64,
) -> Result<(), ToolRunError> {
    let display_argv = serde_json::to_string(&request.display_argv)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let private_argv = request
        .private_argv
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let evidence =
        serde_json::to_string(&ToolEvidenceCompletenessWire::default())
            .map_err(|error| ToolRunError::store(error.to_string()))?;
    tx.execute(
        "INSERT INTO runs(
            run_id, state, source, executor, attempt, tool_name,
            definition_digest, extra_args_digest, display_argv_json,
            private_argv_json, project, agent, workspace, bead, owner_kind,
            owner_id, parent_run_id, created_ts, running_ts, wrapper_pid,
            boot_id, process_start_identity, log_stdout_path, log_stderr_path,
            events_path, evidence_json, diagnostics_json
         ) VALUES (
            ?1, ?2, 'native', 'inline', 1, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
            '[]'
         )",
        params![
            run_id,
            state.as_str(),
            tool_name,
            definition_digest,
            extra_args_digest,
            display_argv,
            private_argv,
            request.project,
            request.agent,
            request.workspace,
            request.bead,
            request.owner_kind,
            request.owner_id,
            request.parent_run_id,
            now,
            if request.commit_running {
                Some(now)
            } else {
                None
            },
            request.wrapper_pid,
            request.boot_id,
            request.process_start_identity,
            request.log_stdout_path,
            request.log_stderr_path,
            request.events_path,
            evidence,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_lifecycle_event(
    tx: &Transaction<'_>,
    event_id: &str,
    run_id: &str,
    kind: ToolRunEventKindWire,
    created_ts: i64,
    exit_code: Option<i32>,
    signal: Option<i32>,
    reason: Option<String>,
) -> Result<(), ToolRunError> {
    let event = ToolRunEventWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        run_id: run_id.to_string(),
        attempt: 1,
        kind,
        created_ts,
        stage: None,
        sample: None,
        exit_code,
        signal,
        reason,
        diagnostics: Vec::new(),
    };
    insert_event_row(tx, &event)?;
    Ok(())
}

fn insert_event_row(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<(), ToolRunError> {
    let payload = serde_json::to_string(&canonical_event(event))
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    tx.execute(
        "INSERT INTO events(
            event_id, run_id, attempt, kind, payload_json, created_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event.event_id,
            event.run_id,
            event.attempt as i64,
            event.kind.as_str(),
            payload,
            event.created_ts
        ],
    )?;
    Ok(())
}

fn ingest_event(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<bool, ToolRunError> {
    if event.attempt != 1 {
        return Err(ToolRunError::invalid(
            "native tool runs only support attempt 1",
        ));
    }
    let payload = serde_json::to_string(&canonical_event(event))
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM events WHERE event_id = ?1",
            [&event.event_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        if existing == payload {
            return Ok(true);
        }
        return Err(ToolRunError::ConflictingEvent {
            event_id: event.event_id.clone(),
            reason: "payload does not match the stored event".to_string(),
        });
    }
    let run =
        load_run(tx, &event.run_id)?.ok_or_else(|| ToolRunError::NotFound {
            run_id: event.run_id.clone(),
        })?;
    if let Some(target) = event.kind.target_state() {
        if !can_transition(run.state, target) {
            return Err(ToolRunError::InvalidTransition {
                from: run.state.as_str().to_string(),
                to: target.as_str().to_string(),
            });
        }
    }
    insert_event_row(tx, event)?;
    apply_event_projection(tx, event)?;
    Ok(false)
}

fn apply_event_projection(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<(), ToolRunError> {
    if let Some(target) = event.kind.target_state() {
        let settled = target.is_terminal().then_some(event.created_ts);
        let running_ts =
            (target == ToolRunStateWire::Running).then_some(event.created_ts);
        tx.execute(
            "UPDATE runs SET
                state = ?2,
                running_ts = COALESCE(?3, running_ts),
                settled_ts = COALESCE(?4, settled_ts),
                exit_code = COALESCE(?5, exit_code),
                signal = COALESCE(?6, signal),
                lost_reason = CASE WHEN ?2 = 'lost'
                    THEN COALESCE(?7, lost_reason) ELSE lost_reason END,
                interruption_reason = CASE WHEN ?2 = 'interrupted'
                    THEN COALESCE(?7, interruption_reason)
                    ELSE interruption_reason END,
                duration_missing = CASE WHEN ?2 IN (
                    'lost', 'interrupted', 'signaled'
                ) AND duration_ms IS NULL
                    THEN 'exit time was not observed'
                    ELSE duration_missing END
             WHERE run_id = ?1",
            params![
                event.run_id,
                target.as_str(),
                running_ts,
                settled,
                event.exit_code,
                event.signal,
                event.reason,
            ],
        )?;
        tx.execute(
            "UPDATE attempts SET
                state = ?2,
                started_ts = COALESCE(?3, started_ts),
                settled_ts = COALESCE(?4, settled_ts),
                exit_code = COALESCE(?5, exit_code),
                signal = COALESCE(?6, signal)
             WHERE run_id = ?1 AND attempt = ?7",
            params![
                event.run_id,
                target.as_str(),
                running_ts,
                settled,
                event.exit_code,
                event.signal,
                event.attempt as i64
            ],
        )?;
        return Ok(());
    }
    match event.kind {
        ToolRunEventKindWire::StageStarted
        | ToolRunEventKindWire::StageFinished => {
            let Some(stage) = &event.stage else {
                return Err(ToolRunError::invalid(
                    "stage events require a stage payload",
                ));
            };
            if stage.run_id != event.run_id {
                return Err(ToolRunError::invalid(
                    "stage.run_id must match event.run_id",
                ));
            }
            let diagnostics = serde_json::to_string(&stage.diagnostics)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            tx.execute(
                "INSERT INTO stages(
                    stage_id, run_id, attempt, event_id, description,
                    started_ts, finished_ts, elapsed_ms, exit_code,
                    output_bytes, incomplete, diagnostics_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(stage_id) DO UPDATE SET
                    event_id = excluded.event_id,
                    finished_ts = COALESCE(excluded.finished_ts, stages.finished_ts),
                    elapsed_ms = COALESCE(excluded.elapsed_ms, stages.elapsed_ms),
                    exit_code = COALESCE(excluded.exit_code, stages.exit_code),
                    output_bytes = COALESCE(excluded.output_bytes, stages.output_bytes),
                    incomplete = excluded.incomplete,
                    diagnostics_json = excluded.diagnostics_json",
                params![
                    stage.stage_id,
                    stage.run_id,
                    stage.attempt as i64,
                    event.event_id,
                    stage.description,
                    stage.started_ts,
                    stage.finished_ts,
                    stage.elapsed_ms,
                    stage.exit_code,
                    stage.output_bytes,
                    i64::from(stage.incomplete
                        || event.kind == ToolRunEventKindWire::StageStarted),
                    diagnostics,
                ],
            )?;
        }
        ToolRunEventKindWire::Sample => {
            let Some(sample) = &event.sample else {
                return Err(ToolRunError::invalid(
                    "sample events require a sample payload",
                ));
            };
            if sample.run_id != event.run_id {
                return Err(ToolRunError::invalid(
                    "sample.run_id must match event.run_id",
                ));
            }
            let payload = serde_json::to_string(sample)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            tx.execute(
                "INSERT OR IGNORE INTO samples(
                    sample_id, run_id, attempt, event_id, observed_ts,
                    payload_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    sample.sample_id,
                    sample.run_id,
                    sample.attempt as i64,
                    event.event_id,
                    sample.observed_ts,
                    payload
                ],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn can_transition(from: ToolRunStateWire, to: ToolRunStateWire) -> bool {
    matches!(
        (from, to),
        (ToolRunStateWire::Created, ToolRunStateWire::Running)
            | (ToolRunStateWire::Created, ToolRunStateWire::Lost)
            | (ToolRunStateWire::Running, ToolRunStateWire::Succeeded)
            | (ToolRunStateWire::Running, ToolRunStateWire::Failed)
            | (ToolRunStateWire::Running, ToolRunStateWire::Signaled)
            | (ToolRunStateWire::Running, ToolRunStateWire::Interrupted)
            | (ToolRunStateWire::Running, ToolRunStateWire::Lost)
    )
}

fn canonical_event(event: &ToolRunEventWire) -> ToolRunEventWire {
    let mut cloned = event.clone();
    cloned.diagnostics = Vec::new();
    cloned
}

fn persist_optional_fingerprint(
    tx: &Transaction<'_>,
    run_id: &str,
    column: &str,
    fingerprint: Option<&ToolFingerprintWire>,
) -> Result<Option<ToolFingerprintWire>, ToolRunError> {
    let Some(fingerprint) = fingerprint else {
        return Ok(None);
    };
    let canonical = canonicalize_tool_fingerprint(fingerprint.clone())?;
    let json = serde_json::to_string(&canonical.fingerprint)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    match column {
        "fingerprint_before_json" => {
            tx.execute(
                "UPDATE runs SET fingerprint_before_json = ?2 WHERE run_id = ?1",
                params![run_id, json],
            )?;
        }
        "fingerprint_after_json" => {
            tx.execute(
                "UPDATE runs SET fingerprint_after_json = ?2 WHERE run_id = ?1",
                params![run_id, json],
            )?;
        }
        other => {
            return Err(ToolRunError::store(format!(
                "unknown fingerprint column {other}"
            )));
        }
    }
    Ok(Some(canonical.fingerprint))
}

fn mutated_input_from_fingerprints(
    before: Option<&ToolFingerprintWire>,
    after: Option<&ToolFingerprintWire>,
) -> Option<bool> {
    let before = before?;
    let after = after?;
    if !before.completeness.complete || !after.completeness.complete {
        return None;
    }
    let left = fingerprint_digest(before).ok()?;
    let right = fingerprint_digest(after).ok()?;
    Some(left != right)
}

fn evidence_from_fingerprints(
    before: Option<&ToolFingerprintWire>,
    after: Option<&ToolFingerprintWire>,
) -> ToolEvidenceCompletenessWire {
    let mut missing = Vec::new();
    match before {
        None => missing.push("fingerprint_before not observed".to_string()),
        Some(fingerprint) if !fingerprint.completeness.complete => {
            missing.extend(fingerprint.completeness.missing.iter().cloned());
        }
        Some(_) => {}
    }
    match after {
        None => missing.push("fingerprint_after not observed".to_string()),
        Some(fingerprint) if !fingerprint.completeness.complete => {
            missing.extend(fingerprint.completeness.missing.iter().cloned());
        }
        Some(_) => {}
    }
    if missing.is_empty() {
        ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        }
    } else {
        ToolEvidenceCompletenessWire {
            complete: false,
            missing,
        }
    }
}

fn load_run(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<ToolRunWire>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT run_id, state, source, executor, attempt, tool_name,
                definition_digest, extra_args_digest, display_argv_json,
                private_argv_json, project, agent, workspace, bead,
                owner_kind, owner_id, parent_run_id, created_ts, running_ts,
                settled_ts, duration_ms, duration_missing, exit_code, signal,
                interruption_reason, lost_reason, wrapper_pid, boot_id,
                process_start_identity, child_pid, child_pgid, mutated_input,
                fingerprint_before_json, fingerprint_after_json,
                log_stdout_path, log_stderr_path, events_path, evidence_json,
                diagnostics_json
         FROM runs WHERE run_id = ?1",
    )?;
    let mut rows = stmt.query([run_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let private_argv: Option<String> = row.get(9)?;
    let display_argv: String = row.get(8)?;
    let evidence: String = row.get(37)?;
    let diagnostics: String = row.get(38)?;
    let fingerprint_before: Option<String> = row.get(32)?;
    let fingerprint_after: Option<String> = row.get(33)?;
    let mutated: Option<i64> = row.get(31)?;
    Ok(Some(ToolRunWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: row.get(0)?,
        state: ToolRunStateWire::from_db(&row.get::<_, String>(1)?)
            .map_err(ToolRunError::store)?,
        source: ToolRunSourceWire::from_db(&row.get::<_, String>(2)?)
            .map_err(ToolRunError::store)?,
        executor: ToolRunExecutorWire::from_db(&row.get::<_, String>(3)?)
            .map_err(ToolRunError::store)?,
        attempt: row.get::<_, i64>(4)? as u32,
        tool_name: row.get(5)?,
        definition_digest: row.get(6)?,
        extra_args_digest: row.get(7)?,
        display_argv: serde_json::from_str(&display_argv)
            .map_err(|error| ToolRunError::store(error.to_string()))?,
        project: row.get(10)?,
        agent: row.get(11)?,
        workspace: row.get(12)?,
        bead: row.get(13)?,
        owner_kind: row.get(14)?,
        owner_id: row.get(15)?,
        parent_run_id: row.get(16)?,
        created_ts: row.get(17)?,
        running_ts: row.get(18)?,
        settled_ts: row.get(19)?,
        duration_ms: row.get(20)?,
        duration_missing: row.get(21)?,
        exit_code: row.get(22)?,
        signal: row.get(23)?,
        interruption_reason: row.get(24)?,
        lost_reason: row.get(25)?,
        wrapper_pid: row.get(26)?,
        boot_id: row.get(27)?,
        process_start_identity: row.get(28)?,
        child_pid: row.get(29)?,
        child_pgid: row.get(30)?,
        mutated_input: mutated.map(|value| value != 0),
        fingerprint_before: parse_optional_json(fingerprint_before)?,
        fingerprint_after: parse_optional_json(fingerprint_after)?,
        logs: ToolRunLogMetadataWire {
            stdout_path: row.get(34)?,
            stderr_path: row.get(35)?,
            events_path: row.get(36)?,
            has_private_argv: private_argv.is_some(),
        },
        evidence_completeness: serde_json::from_str(&evidence)
            .map_err(|error| ToolRunError::store(error.to_string()))?,
        diagnostics: serde_json::from_str(&diagnostics)
            .map_err(|error| ToolRunError::store(error.to_string()))?,
    }))
}

fn load_attempt(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<ToolAttemptWire>, ToolRunError> {
    conn.query_row(
        "SELECT attempt, state, started_ts, settled_ts, exit_code, signal,
                diagnostics_json
         FROM attempts WHERE run_id = ?1 AND attempt = 1",
        [run_id],
        |row| {
            let diagnostics: String = row.get(6)?;
            Ok(ToolAttemptWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.to_string(),
                attempt: row.get::<_, i64>(0)? as u32,
                state: ToolRunStateWire::from_db(&row.get::<_, String>(1)?)
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(ToolRunError::store(error)),
                        )
                    })?,
                started_ts: row.get(2)?,
                settled_ts: row.get(3)?,
                exit_code: row.get(4)?,
                signal: row.get(5)?,
                diagnostics: serde_json::from_str(&diagnostics)
                    .unwrap_or_default(),
            })
        },
    )
    .optional()
    .map_err(Into::into)
}

fn load_events(
    conn: &Connection,
    run_id: &str,
) -> Result<Vec<ToolRunEventWire>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT payload_json FROM events WHERE run_id = ?1
         ORDER BY created_ts, event_id",
    )?;
    let rows = stmt.query_map([run_id], |row| row.get::<_, String>(0))?;
    let mut events = Vec::new();
    for row in rows {
        let payload = row?;
        events.push(
            serde_json::from_str(&payload)
                .map_err(|error| ToolRunError::store(error.to_string()))?,
        );
    }
    Ok(events)
}

fn load_stages(
    conn: &Connection,
    run_id: &str,
) -> Result<Vec<ToolStageWire>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT stage_id, run_id, attempt, description, started_ts,
                finished_ts, elapsed_ms, exit_code, output_bytes, incomplete,
                diagnostics_json
         FROM stages WHERE run_id = ?1 ORDER BY started_ts, stage_id",
    )?;
    let rows = stmt.query_map([run_id], |row| {
        let diagnostics: String = row.get(10)?;
        Ok(ToolStageWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            stage_id: row.get(0)?,
            run_id: row.get(1)?,
            attempt: row.get::<_, i64>(2)? as u32,
            description: row.get(3)?,
            started_ts: row.get(4)?,
            finished_ts: row.get(5)?,
            elapsed_ms: row.get(6)?,
            exit_code: row.get(7)?,
            output_bytes: row.get(8)?,
            incomplete: row.get::<_, i64>(9)? != 0,
            diagnostics: serde_json::from_str(&diagnostics).unwrap_or_default(),
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_samples(
    conn: &Connection,
    run_id: &str,
) -> Result<Vec<ToolLoadSampleWire>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT payload_json FROM samples WHERE run_id = ?1
         ORDER BY observed_ts, sample_id",
    )?;
    let rows = stmt.query_map([run_id], |row| row.get::<_, String>(0))?;
    let mut samples = Vec::new();
    for row in rows {
        samples.push(
            serde_json::from_str(&row?)
                .map_err(|error| ToolRunError::store(error.to_string()))?,
        );
    }
    Ok(samples)
}

fn parse_optional_json<T: serde::de::DeserializeOwned>(
    raw: Option<String>,
) -> Result<Option<T>, ToolRunError> {
    raw.map(|value| {
        serde_json::from_str(&value)
            .map_err(|error| ToolRunError::store(error.to_string()))
    })
    .transpose()
}

fn empty_summary(diagnostic: &str) -> ToolRunSummaryResultWire {
    ToolRunSummaryResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        last: None,
        typical_duration_ms: None,
        typical_sample_count: 0,
        typical_status_breakdown: Default::default(),
        diagnostics: vec![diagnostic.to_string()],
    }
}

fn validate_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
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

fn parse_cursor(cursor: &str) -> Result<(i64, String), ToolRunError> {
    let (ts, run_id) = cursor.split_once(':').ok_or_else(|| {
        ToolRunError::invalid("cursor must have the form <created_ts>:<run_id>")
    })?;
    let ts = ts
        .parse::<i64>()
        .map_err(|_| ToolRunError::invalid("cursor timestamp is invalid"))?;
    if run_id.is_empty() {
        return Err(ToolRunError::invalid("cursor run id must not be empty"));
    }
    Ok((ts, run_id.to_string()))
}

fn nonempty_or_generated(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.trim().is_empty() => value.to_string(),
        _ => new_hex_id(),
    }
}

fn new_hex_id() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

fn count_rows(conn: &Connection, table: &str) -> Result<u64, ToolRunError> {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
    Ok(count as u64)
}

fn read_meta_i64(
    conn: &Connection,
    key: &str,
) -> Result<Option<i64>, ToolRunError> {
    let raw: Option<String> = conn
        .query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    Ok(raw.and_then(|value| value.parse().ok()))
}

fn touch_write_meta(
    tx: &Transaction<'_>,
    now: i64,
) -> Result<(), ToolRunError> {
    tx.execute(
        "INSERT INTO meta(key, value) VALUES ('last_write_ts', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [now.to_string()],
    )?;
    Ok(())
}

fn bounded_busy_timeout(timeout: Duration) -> Duration {
    if timeout.is_zero() {
        TOOL_RUN_DEFAULT_BUSY_TIMEOUT
    } else {
        timeout.min(TOOL_RUN_MAX_BUSY_TIMEOUT)
    }
}

fn with_write_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&mut Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    match open_write_store(store_path, timeout)
        .and_then(|mut conn| operation(&mut conn))
    {
        Ok(value) => Ok(value),
        Err(error) if should_quarantine(&error) && store_path.exists() => {
            quarantine_corrupt_store(store_path, timeout)?;
            let mut conn = open_write_store(store_path, timeout)?;
            operation(&mut conn)
        }
        Err(error) => Err(error),
    }
}

fn with_read_store<T>(
    store_path: &Path,
    busy_timeout: Duration,
    mut operation: impl FnMut(&Connection) -> Result<T, ToolRunError>,
) -> Result<T, ToolRunError> {
    let timeout = bounded_busy_timeout(busy_timeout);
    let conn = open_read_store(store_path, timeout)?;
    operation(&conn)
}

fn should_quarantine(error: &ToolRunError) -> bool {
    match error {
        ToolRunError::Busy { .. }
        | ToolRunError::ReadOnly { .. }
        | ToolRunError::NewerSchema { .. } => false,
        ToolRunError::Store { message } | ToolRunError::Io { message } => {
            is_sqlite_corruption_error(message)
        }
        _ => false,
    }
}

fn is_sqlite_corruption_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database disk image is malformed")
        || lower.contains("file is not a database")
        || lower.contains("not a database")
        || lower.contains("malformed database schema")
        || lower.contains("unsupported file format")
}

fn quarantine_corrupt_store(
    store_path: &Path,
    timeout: Duration,
) -> Result<(), ToolRunError> {
    let (lock_path, holder_path) = lock_paths(store_path);
    let _lock = acquire_store_lock(
        &lock_path,
        &holder_path,
        LockMode::Exclusive,
        timeout,
        "tool_run_quarantine",
    )
    .map_err(|error| match error {
        StoreLockError::Timeout { .. } => ToolRunError::Busy {
            message: error.to_string(),
        },
        other => ToolRunError::store(other.to_string()),
    })?;
    let quarantined = corrupt_store_quarantine_path(store_path);
    match fs::rename(store_path, &quarantined) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(ToolRunError::store(format!(
                "failed to quarantine unusable tool run store {}: {error}",
                store_path.display()
            )));
        }
    }
    for suffix in ["-wal", "-shm"] {
        let source = sqlite_sidecar_path(store_path, suffix);
        let target = sqlite_sidecar_path(&quarantined, suffix);
        match fs::rename(&source, &target) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(ToolRunError::store(format!(
                    "failed to quarantine tool run store sidecar {}: {error}",
                    source.display()
                )));
            }
        }
    }
    Ok(())
}

fn corrupt_store_quarantine_path(store_path: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let file_name = store_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| "runs.sqlite".into());
    store_path.with_file_name(format!("{file_name}.corrupt-{nanos}"))
}

fn sqlite_sidecar_path(store_path: &Path, suffix: &str) -> PathBuf {
    let mut raw = store_path.as_os_str().to_os_string();
    raw.push(suffix);
    PathBuf::from(raw)
}

fn open_write_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, ToolRunError> {
    if let Some(parent) = store_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
            restrict_mode(parent, 0o700)?;
        }
    }
    let conn = Connection::open(store_path)?;
    restrict_mode(store_path, 0o600)?;
    conn.busy_timeout(busy_timeout)?;
    enable_wal_mode(&conn, busy_timeout)?;
    conn.execute_batch("PRAGMA synchronous = NORMAL;")?;
    conn.execute_batch(SCHEMA_SQL)?;
    enforce_schema_version(&conn, true)?;
    Ok(conn)
}

fn open_read_store(
    store_path: &Path,
    busy_timeout: Duration,
) -> Result<Connection, ToolRunError> {
    let conn = Connection::open_with_flags(
        store_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        if matches!(
            error.sqlite_error_code(),
            Some(ErrorCode::CannotOpen | ErrorCode::ReadOnly)
        ) {
            ToolRunError::ReadOnly {
                message: error.to_string(),
            }
        } else {
            error.into()
        }
    })?;
    conn.busy_timeout(busy_timeout)?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    enforce_schema_version(&conn, false)?;
    Ok(conn)
}

fn enforce_schema_version(
    conn: &Connection,
    write: bool,
) -> Result<(), ToolRunError> {
    let prior: Option<u32> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .and_then(|raw| raw.parse().ok());
    if let Some(version) = prior {
        if version > TOOL_RUN_WIRE_SCHEMA_VERSION {
            return Err(ToolRunError::NewerSchema {
                expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
                actual: version,
            });
        }
    }
    if write {
        conn.execute(
            "INSERT INTO meta(key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [TOOL_RUN_WIRE_SCHEMA_VERSION.to_string()],
        )?;
    }
    Ok(())
}

fn enable_wal_mode(
    conn: &Connection,
    busy_timeout: Duration,
) -> Result<(), ToolRunError> {
    let started = Instant::now();
    let result = loop {
        let remaining = busy_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break Err(ToolRunError::Busy {
                message: "timed out waiting to enable WAL journal mode"
                    .to_string(),
            });
        }
        conn.busy_timeout(remaining)?;
        match conn.query_row("PRAGMA journal_mode = WAL", [], |row| {
            row.get::<_, String>(0)
        }) {
            Ok(mode) if mode.eq_ignore_ascii_case("wal") => break Ok(()),
            Ok(mode) => {
                break Err(ToolRunError::store(format!(
                    "failed to enable WAL journal mode: SQLite returned {mode:?}"
                )));
            }
            Err(error)
                if matches!(
                    error.sqlite_error_code(),
                    Some(ErrorCode::DatabaseBusy | ErrorCode::DatabaseLocked)
                ) =>
            {
                let remaining = busy_timeout.saturating_sub(started.elapsed());
                if remaining.is_zero() {
                    break Err(error.into());
                }
                thread::sleep(Duration::from_millis(5).min(remaining));
            }
            Err(error) => break Err(error.into()),
        }
    };
    conn.busy_timeout(busy_timeout)?;
    result
}

fn restrict_mode(path: &Path, mode: u32) -> Result<(), ToolRunError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    let _ = (path, mode);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_run::catalog::normalize_tool_definition;
    use crate::tool_run::wire::{
        ToolArgsPolicyWire, ToolDefinitionWire, ToolFingerprintSpecWire,
        ToolStagesWire,
    };
    use std::sync::{Arc, Barrier};
    use tempfile::tempdir;

    fn definition() -> ToolDefinitionWire {
        ToolDefinitionWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            name: "check".into(),
            argv: vec!["just".into(), "check".into()],
            description: "check".into(),
            stages: ToolStagesWire::RunSilent,
            inputs: vec!["Justfile".into()],
            env: Vec::new(),
            args: ToolArgsPolicyWire::Deny,
            fingerprint: ToolFingerprintSpecWire::default(),
            diagnostics: Vec::new(),
        }
    }

    fn begin_named(path: &Path, now: i64) -> ToolRunBeginResultWire {
        let normalized = normalize_tool_definition(definition()).unwrap();
        begin(
            path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition,
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: Some("agent-1".into()),
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: Some(4242),
                boot_id: Some("boot-1".into()),
                process_start_identity: Some("start-1".into()),
                events_path: Some("logs/run/events.jsonl".into()),
                log_stdout_path: Some("logs/run/stdout.log".into()),
                log_stderr_path: Some("logs/run/stderr.log".into()),
                now_ts: Some(now),
                commit_running: true,
            },
            Duration::from_secs(1),
        )
        .unwrap()
    }

    fn store() -> (tempfile::TempDir, PathBuf) {
        let temp = tempdir().unwrap();
        let path = temp.path().join("tools").join("runs.sqlite");
        (temp, path)
    }

    #[test]
    fn begin_commits_created_and_running_together() {
        let (_temp, path) = store();
        let started = begin_named(&path, 1_700_000_000);
        assert_eq!(started.run.state, ToolRunStateWire::Running);
        assert!(started.running_event_id.is_some());
        assert_eq!(started.run.created_ts, started.run.running_ts.unwrap());
        assert!(!started.run.logs.has_private_argv);
    }

    #[test]
    fn every_terminal_state_round_trips() {
        let (_temp, path) = store();
        for (state, exit, signal) in [
            (ToolRunStateWire::Succeeded, Some(0), None),
            (ToolRunStateWire::Failed, Some(3), None),
            (ToolRunStateWire::Signaled, None, Some(15)),
            (ToolRunStateWire::Interrupted, None, Some(2)),
            (ToolRunStateWire::Lost, None, None),
        ] {
            let started = begin_named(&path, 10);
            let finished = finish(
                &path,
                ToolRunFinishRequestWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    run_id: started.run.run_id,
                    event_id: None,
                    state,
                    exit_code: exit,
                    signal,
                    interruption_reason: (state
                        == ToolRunStateWire::Interrupted)
                        .then(|| "wrapper SIGINT".into()),
                    lost_reason: (state == ToolRunStateWire::Lost).then(|| {
                        TOOL_RUN_LOST_REASON_RUNNER_EXITED.to_string()
                    }),
                    child_pid: Some(99),
                    child_pgid: Some(99),
                    duration_ms: (state != ToolRunStateWire::Lost)
                        .then_some(12),
                    fingerprint_before: None,
                    fingerprint_after: None,
                    mutated_input: None,
                    now_ts: Some(20),
                    diagnostics: Vec::new(),
                },
                Duration::from_secs(1),
            )
            .unwrap();
            assert_eq!(finished.run.state, state);
            assert_eq!(finished.run.exit_code, exit);
            assert_eq!(finished.run.signal, signal);
            if state == ToolRunStateWire::Lost {
                assert!(finished.run.duration_ms.is_none());
                assert_eq!(
                    finished.run.duration_missing.as_deref(),
                    Some("exit time was not observed")
                );
            }
        }
    }

    #[test]
    fn invalid_transition_is_an_error() {
        let (_temp, path) = store();
        let started = begin_named(&path, 1);
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id.clone(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(1),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(2),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let error = finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
                event_id: None,
                state: ToolRunStateWire::Failed,
                exit_code: Some(1),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(1),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(3),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(error, ToolRunError::InvalidTransition { .. }));
    }

    #[test]
    fn event_replay_is_idempotent_and_conflicts_are_integrity_errors() {
        let (_temp, path) = store();
        let started = begin_named(&path, 1);
        let event = ToolRunEventWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event_id: "evt-stage".into(),
            run_id: started.run.run_id.clone(),
            attempt: 1,
            kind: ToolRunEventKindWire::StageStarted,
            created_ts: 2,
            stage: Some(ToolStageWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_id: "st1".into(),
                run_id: started.run.run_id.clone(),
                attempt: 1,
                description: "fmt".into(),
                started_ts: Some(2),
                finished_ts: None,
                elapsed_ms: None,
                exit_code: None,
                output_bytes: None,
                incomplete: true,
                diagnostics: Vec::new(),
            }),
            sample: None,
            exit_code: None,
            signal: None,
            reason: None,
            diagnostics: Vec::new(),
        };
        let first = append_event(
            &path,
            ToolRunAppendRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event: event.clone(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!first.replayed);
        let second = append_event(
            &path,
            ToolRunAppendRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event: event.clone(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(second.replayed);
        let mut conflicting = event;
        conflicting.stage.as_mut().unwrap().description = "other".into();
        let error = append_event(
            &path,
            ToolRunAppendRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event: conflicting,
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(error, ToolRunError::ConflictingEvent { .. }));
    }

    #[test]
    fn queries_do_not_create_a_missing_store() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("missing").join("runs.sqlite");
        let listed = list_runs(
            &path,
            ToolRunListRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                tool: None,
                state: None,
                agent: None,
                project: None,
                include_all: false,
                limit: 50,
                cursor: None,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(listed.runs.is_empty());
        assert!(!path.exists());
        let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert!(!stats.exists);
        assert!(!path.exists());
    }

    #[test]
    fn newer_schema_is_refused_without_modification() {
        let (_temp, path) = store();
        begin_named(&path, 1);
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "UPDATE meta SET value = '2' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
        drop(conn);
        let before = fs::read(&path).unwrap();
        let error = list_runs(
            &path,
            ToolRunListRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                tool: None,
                state: None,
                agent: None,
                project: None,
                include_all: false,
                limit: 10,
                cursor: None,
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(error, ToolRunError::NewerSchema { actual: 2, .. }));
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn corrupt_store_is_quarantined_on_write_and_recreated() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("runs.sqlite");
        fs::write(&path, b"not a sqlite database").unwrap();
        begin_named(&path, 1);
        let quarantined = fs::read_dir(temp.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt-"))
            .count();
        assert_eq!(quarantined, 1);
        assert_eq!(
            store_stats(&path, Duration::from_secs(1))
                .unwrap()
                .run_count,
            1
        );
    }

    #[test]
    fn retention_protects_unsettled_runs() {
        let (_temp, path) = store();
        let running = begin_named(&path, 1);
        let settled = begin_named(&path, 2);
        finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: settled.run.run_id,
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(3),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let preview = retention_preview(
            &path,
            ToolRunRetentionRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                policy: ToolRunRetentionPolicyWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    summary_days: 1,
                    detail_days: 1,
                    log_days: 1,
                    ..ToolRunRetentionPolicyWire::default()
                },
                now_ts: Some(3 + 2 * 86400),
                dry_run: true,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(preview.protected_unsettled >= 1);
        assert!(preview.summary_rows >= 1);
        let applied = retention_apply(
            &path,
            ToolRunRetentionRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                policy: ToolRunRetentionPolicyWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    summary_days: 1,
                    detail_days: 1,
                    log_days: 1,
                    ..ToolRunRetentionPolicyWire::default()
                },
                now_ts: Some(3 + 2 * 86400),
                dry_run: false,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(!applied.dry_run);
        let shown = show_run(
            &path,
            ToolRunShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: running.run.run_id,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(shown.run.is_some());
    }

    fn begin_with_log(
        path: &Path,
        now: i64,
        log: &Path,
        bytes: usize,
    ) -> String {
        fs::create_dir_all(log.parent().unwrap()).unwrap();
        fs::write(log, vec![b'x'; bytes]).unwrap();
        let normalized = normalize_tool_definition(definition()).unwrap();
        begin(
            path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: normalized.definition,
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: Some(4242),
                boot_id: Some("boot-1".into()),
                process_start_identity: Some("start-1".into()),
                events_path: None,
                log_stdout_path: Some(log.to_string_lossy().into_owned()),
                log_stderr_path: None,
                now_ts: Some(now),
                commit_running: true,
            },
            Duration::from_secs(1),
        )
        .unwrap()
        .run
        .run_id
    }

    fn settle_succeeded(path: &Path, run_id: &str, now: i64) {
        finish(
            path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: run_id.to_string(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(5),
                fingerprint_before: None,
                fingerprint_after: None,
                mutated_input: None,
                now_ts: Some(now),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
    }

    fn aggregate_preview(
        path: &Path,
        log_max_bytes: u64,
    ) -> ToolRunRetentionResultWire {
        retention_preview(
            path,
            ToolRunRetentionRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                policy: ToolRunRetentionPolicyWire {
                    log_max_bytes,
                    ..ToolRunRetentionPolicyWire::default()
                },
                now_ts: Some(1_000),
                dry_run: true,
            },
            Duration::from_secs(1),
        )
        .unwrap()
    }

    #[test]
    fn retention_aggregate_target_deletes_oldest_settled_logs_first() {
        let (temp, path) = store();
        let logs = temp.path().join("logs");
        let live = begin_with_log(&path, 1, &logs.join("live/stdout.log"), 900);
        let mut settled_logs = Vec::new();
        for (index, name) in ["oldest", "middle", "newest"].iter().enumerate() {
            let log = logs.join(name).join("stdout.log");
            let run_id = begin_with_log(&path, 10 + index as i64, &log, 1000);
            settle_succeeded(&path, &run_id, 20 + index as i64);
            settled_logs.push(log.to_string_lossy().into_owned());
        }

        // 3900 retained bytes against a 2500 target: the two oldest settled
        // runs go, the newest settled run and the unsettled run stay.
        let fits = aggregate_preview(&path, 2500);
        let selected: Vec<_> = fits
            .file_candidates
            .iter()
            .filter_map(|candidate| candidate.path.clone())
            .collect();
        assert_eq!(
            selected,
            vec![settled_logs[0].clone(), settled_logs[1].clone()]
        );
        assert!(fits
            .file_candidates
            .iter()
            .all(|candidate| candidate.reason.contains("log_max_bytes")));
        assert_eq!(fits.retained_bytes, 1900);
        assert_eq!(fits.protected_bytes, 900);
        assert_eq!(fits.over_target_bytes, 0);

        // Protected bytes alone exceed a 500-byte target: every settled log is
        // selected, the excess is reported, and the live run is never listed.
        let over = aggregate_preview(&path, 500);
        assert_eq!(over.file_candidates.len(), 3);
        assert!(over.file_candidates.iter().all(|candidate| {
            candidate.run_id.as_deref() != Some(live.as_str())
                && !candidate
                    .path
                    .as_deref()
                    .unwrap_or_default()
                    .contains("live")
        }));
        assert_eq!(over.retained_bytes, 900);
        assert_eq!(over.over_target_bytes, 400);

        // A target that already fits selects nothing; Rust never deletes files.
        let roomy = aggregate_preview(&path, 10_000);
        assert!(roomy.file_candidates.is_empty());
        assert_eq!(roomy.retained_bytes, 3900);
        assert!(logs.join("oldest/stdout.log").exists());
    }

    #[test]
    fn reconcile_marks_dead_wrappers_lost_without_inventing_duration() {
        let (_temp, path) = store();
        let started = begin_named(&path, 10);
        let result = reconcile(
            &path,
            ToolRunReconcileRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                facts: vec![crate::tool_run::wire::ToolRunLivenessFactWire {
                    run_id: started.run.run_id.clone(),
                    wrapper_pid: Some(4242),
                    boot_id: Some("boot-other".into()),
                    process_start_identity: Some("start-1".into()),
                    observation: ToolLivenessObservationWire::Dead,
                    reason: Some("boot_changed".into()),
                }],
                now_ts: Some(11),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(result.marked_lost.len(), 1);
        assert_eq!(result.marked_lost[0], started.run.run_id);
        let shown = show_run(
            &path,
            ToolRunShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let run = shown.run.unwrap();
        assert_eq!(run.state, ToolRunStateWire::Lost);
        assert!(run.duration_ms.is_none());
        assert_eq!(run.lost_reason.as_deref(), Some("boot_changed"));
    }

    #[test]
    fn unknown_liveness_is_not_proof_of_death() {
        let (_temp, path) = store();
        let started = begin_named(&path, 10);
        let result = reconcile(
            &path,
            ToolRunReconcileRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                facts: vec![crate::tool_run::wire::ToolRunLivenessFactWire {
                    run_id: started.run.run_id.clone(),
                    wrapper_pid: Some(4242),
                    boot_id: None,
                    process_start_identity: None,
                    observation: ToolLivenessObservationWire::Unknown,
                    reason: Some("permission_denied".into()),
                }],
                now_ts: Some(11),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(result.marked_lost.is_empty());
        let shown = show_run(
            &path,
            ToolRunShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(shown.run.unwrap().state, ToolRunStateWire::Running);
    }

    #[test]
    fn version_rejection_and_unknown_evidence_slots() {
        let error = begin(
            Path::new("/tmp/unused"),
            ToolRunBeginRequestWire {
                schema_version: 9,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: definition(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into()],
                private_argv: None,
                project: None,
                agent: None,
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: None,
                boot_id: None,
                process_start_identity: None,
                events_path: None,
                log_stdout_path: None,
                log_stderr_path: None,
                now_ts: Some(1),
                commit_running: true,
            },
            Duration::from_secs(1),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ToolRunError::SchemaVersion { actual: 9, .. }
        ));
        let fingerprint = crate::tool_run::unknown_evidence("not observed");
        assert!(!fingerprint.completeness.complete);
        assert!(fingerprint.project_identity.is_none());
    }

    #[test]
    fn finish_stores_canonical_fingerprints_and_mutated_input() {
        use crate::tool_run::wire::{
            ToolDirtyPathWire, ToolEvidenceCompletenessWire,
            ToolRepoFingerprintWire,
        };

        let (_temp, path) = store();
        let started = begin_named(&path, 10);
        let mut before = crate::tool_run::unknown_evidence("unused");
        before.project_identity = Some("sase".into());
        before.completeness = ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        };
        before.diagnostics.clear();
        before.repos = vec![ToolRepoFingerprintWire {
            identity: "sase".into(),
            head: Some("aaa".into()),
            index_tree: Some("bbb".into()),
            dirty_paths: vec![ToolDirtyPathWire {
                path: "z.py".into(),
                status: "modified".into(),
                kind: "file".into(),
                mode: Some("100644".into()),
                content_hash: Some("1".into()),
                incomplete: None,
            }],
            incomplete: None,
        }];
        let mut after = before.clone();
        after.repos[0].dirty_paths[0].content_hash = Some("2".into());
        let finished = finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id.clone(),
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(12),
                fingerprint_before: Some(before),
                fingerprint_after: Some(after),
                mutated_input: None,
                now_ts: Some(22),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert_eq!(finished.run.mutated_input, Some(true));
        assert!(finished.run.evidence_completeness.complete);
        let shown = show_run(
            &path,
            ToolRunShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        let run = shown.run.expect("run");
        assert_eq!(run.fingerprint_before.unwrap().repos[0].identity, "sase");
        assert_eq!(
            run.fingerprint_after.unwrap().repos[0].dirty_paths[0]
                .content_hash
                .as_deref(),
            Some("2")
        );
    }

    #[test]
    fn incomplete_fingerprints_do_not_guess_mutated_input() {
        let (_temp, path) = store();
        let started = begin_named(&path, 10);
        let before = crate::tool_run::unknown_evidence("probe timeout");
        let after = crate::tool_run::unknown_evidence("probe timeout");
        let finished = finish(
            &path,
            ToolRunFinishRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: started.run.run_id,
                event_id: None,
                state: ToolRunStateWire::Succeeded,
                exit_code: Some(0),
                signal: None,
                interruption_reason: None,
                lost_reason: None,
                child_pid: None,
                child_pgid: None,
                duration_ms: Some(3),
                fingerprint_before: Some(before),
                fingerprint_after: Some(after),
                mutated_input: None,
                now_ts: Some(13),
                diagnostics: Vec::new(),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(finished.run.mutated_input.is_none());
        assert!(!finished.run.evidence_completeness.complete);
        assert!(finished
            .run
            .evidence_completeness
            .missing
            .iter()
            .any(|item| item.contains("probe timeout")));
    }

    #[test]
    fn concurrent_begins_do_not_livelock() {
        let (_temp, path) = store();
        begin_named(&path, 1);
        let barrier = Arc::new(Barrier::new(2));
        let path_a = path.clone();
        let path_b = path.clone();
        let barrier_a = barrier.clone();
        let handle = thread::spawn(move || {
            barrier_a.wait();
            begin(
                &path_a,
                ToolRunBeginRequestWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    run_id: None,
                    created_event_id: None,
                    running_event_id: None,
                    tool_name: Some("check".into()),
                    definition: definition(),
                    extra_args: Vec::new(),
                    display_argv: vec!["just".into(), "check".into()],
                    private_argv: None,
                    project: Some("sase".into()),
                    agent: None,
                    workspace: None,
                    bead: None,
                    owner_kind: None,
                    owner_id: None,
                    parent_run_id: None,
                    wrapper_pid: None,
                    boot_id: None,
                    process_start_identity: None,
                    events_path: None,
                    log_stdout_path: None,
                    log_stderr_path: None,
                    now_ts: Some(2),
                    commit_running: true,
                },
                Duration::from_millis(250),
            )
        });
        barrier.wait();
        let second = begin(
            &path_b,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: definition(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: None,
                boot_id: None,
                process_start_identity: None,
                events_path: None,
                log_stdout_path: None,
                log_stderr_path: None,
                now_ts: Some(3),
                commit_running: true,
            },
            Duration::from_millis(250),
        );
        let first = handle.join().unwrap();
        assert!(first.is_ok() || second.is_ok());
        let stats = store_stats(&path, Duration::from_secs(1)).unwrap();
        assert!(stats.run_count >= 2);
    }

    #[test]
    fn busy_failure_is_bounded() {
        let (_temp, path) = store();
        begin_named(&path, 1);
        let holder = Connection::open(&path).unwrap();
        holder.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let started = Instant::now();
        let result = begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: Some("check".into()),
                definition: definition(),
                extra_args: Vec::new(),
                display_argv: vec!["just".into(), "check".into()],
                private_argv: None,
                project: Some("sase".into()),
                agent: None,
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: None,
                boot_id: None,
                process_start_identity: None,
                events_path: None,
                log_stdout_path: None,
                log_stderr_path: None,
                now_ts: Some(2),
                commit_running: true,
            },
            Duration::from_millis(25),
        );
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        holder.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn summary_never_substitutes_zero_for_missing_typical() {
        let (_temp, path) = store();
        let started = begin_named(&path, 1);
        let digest = started.run.definition_digest.clone();
        let summary = summarize(
            &path,
            ToolRunSummaryRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                project: "sase".into(),
                tool_name: "check".into(),
                definition_digest: digest,
                now_ts: Some(1),
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(summary.typical_duration_ms.is_none());
        assert_eq!(summary.typical_sample_count, 0);
        assert!(summary
            .diagnostics
            .iter()
            .any(|item| item.contains("unknown")));
    }

    #[test]
    fn private_argv_is_not_serialized_on_queries() {
        let (_temp, path) = store();
        let started = begin(
            &path,
            ToolRunBeginRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: None,
                created_event_id: None,
                running_event_id: None,
                tool_name: None,
                definition: ToolDefinitionWire {
                    name: String::new(),
                    argv: vec!["secret".into(), "--token".into(), "abc".into()],
                    ..definition()
                },
                extra_args: Vec::new(),
                display_argv: vec![
                    "secret".into(),
                    "--token".into(),
                    "***".into(),
                ],
                private_argv: Some(vec![
                    "secret".into(),
                    "--token".into(),
                    "abc".into(),
                ]),
                project: Some("sase".into()),
                agent: None,
                workspace: None,
                bead: None,
                owner_kind: None,
                owner_id: None,
                parent_run_id: None,
                wrapper_pid: None,
                boot_id: None,
                process_start_identity: None,
                events_path: None,
                log_stdout_path: None,
                log_stderr_path: None,
                now_ts: Some(1),
                commit_running: true,
            },
            Duration::from_secs(1),
        )
        .unwrap();
        assert!(started.run.logs.has_private_argv);
        let encoded = serde_json::to_string(&started.run).unwrap();
        assert!(!encoded.contains("abc"));
        assert!(encoded.contains("***"));
    }
}

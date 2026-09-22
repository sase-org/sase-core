//! Run queries and loaders.
//!
//! Owns the read-only `list_runs`, `show_run`, `summarize`, and `store_stats`
//! entry points plus the row loaders (`load_run`, `load_attempt`,
//! `load_events`, `load_stages`, `load_samples`) shared with the write side.

use super::super::catalog::extra_args_digest;
use super::super::wire::{
    ToolAttemptWire, ToolLoadSampleWire, ToolRunEventWire, ToolRunExecutorWire,
    ToolRunListRequestWire, ToolRunListResultWire, ToolRunLogMetadataWire,
    ToolRunShowRequestWire, ToolRunShowResultWire, ToolRunSourceWire,
    ToolRunStateWire, ToolRunStoreStatsWire, ToolRunSummaryRequestWire,
    ToolRunSummaryResultWire, ToolRunWire, ToolStageWire,
    TOOL_RUN_LIST_MAX_LIMIT, TOOL_RUN_TYPICAL_SAMPLE_LIMIT,
    TOOL_RUN_TYPICAL_WINDOW_DAYS, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    runs_has_child_observation_column, unix_now, validate_schema,
    with_read_store,
};
use rusqlite::{params, Connection, OptionalExtension};
use std::fs;
use std::path::Path;
use std::time::Duration;

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

pub(super) fn load_run(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<ToolRunWire>, ToolRunError> {
    // Stores written before the child-observation column existed select a
    // NULL placeholder so the positional mapping below holds for both
    // layouts; the read path never migrates.
    let child_identity_projection = if runs_has_child_observation_column(conn)?
    {
        "child_process_start_identity"
    } else {
        "NULL"
    };
    let sql = format!(
        "SELECT run_id, state, source, executor, attempt, tool_name,
                definition_digest, extra_args_digest, display_argv_json,
                private_argv_json, project, agent, workspace, bead,
                owner_kind, owner_id, parent_run_id, created_ts, running_ts,
                settled_ts, duration_ms, duration_missing, exit_code, signal,
                interruption_reason, lost_reason, wrapper_pid, boot_id,
                process_start_identity, child_pid, child_pgid,
                {child_identity_projection}, mutated_input,
                fingerprint_before_json, fingerprint_after_json,
                log_stdout_path, log_stderr_path, events_path, evidence_json,
                diagnostics_json
         FROM runs WHERE run_id = ?1"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([run_id])?;
    let Some(row) = rows.next()? else {
        return Ok(None);
    };
    let private_argv: Option<String> = row.get(9)?;
    let display_argv: String = row.get(8)?;
    let evidence: String = row.get(38)?;
    let diagnostics: String = row.get(39)?;
    let fingerprint_before: Option<String> = row.get(33)?;
    let fingerprint_after: Option<String> = row.get(34)?;
    let mutated: Option<i64> = row.get(32)?;
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
        child_process_start_identity: row.get(31)?,
        mutated_input: mutated.map(|value| value != 0),
        fingerprint_before: parse_optional_json(fingerprint_before)?,
        fingerprint_after: parse_optional_json(fingerprint_after)?,
        logs: ToolRunLogMetadataWire {
            stdout_path: row.get(35)?,
            stderr_path: row.get(36)?,
            events_path: row.get(37)?,
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

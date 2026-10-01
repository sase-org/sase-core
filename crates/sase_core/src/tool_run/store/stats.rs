//! Bounded read-only ToolRun stats report.
//!
//! Loads in-window plus backtest-lookback rows in one newest-first query,
//! then folds them with the pure `stats` computation. Reads only; never
//! migrates, quarantines, or writes the ledger.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;
use serde::Deserialize;

use super::super::stats::{
    compute_stats_report, stats_thresholds, StatsReportScope, StatsRunRow,
    StatsSampleRow, StatsStageRow, ToolRunStatsPressureWire,
    ToolRunStatsRequestWire, ToolRunStatsResultWire,
    STATS_BACKTEST_LOOKBACK_DAYS, STATS_MAX_DAYS, STATS_MAX_RUNS,
    STATS_MAX_SAMPLES, STATS_MAX_STAGES,
};
use super::super::wire::{ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION};
use super::super::ToolRunError;
use super::connection::{
    runs_column_set, unix_now, validate_schema, with_read_store,
};

pub fn tool_run_stats_report(
    store_path: &Path,
    request: ToolRunStatsRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunStatsResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.days < 1 || request.days > STATS_MAX_DAYS {
        return Err(ToolRunError::invalid(format!(
            "days must be 1..={STATS_MAX_DAYS}"
        )));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let since = now.saturating_sub(request.days.saturating_mul(86_400));
    let lookback = since
        .saturating_sub(STATS_BACKTEST_LOOKBACK_DAYS.saturating_mul(86_400));
    if !store_path.exists() {
        return Ok(empty_stats_report(
            &request,
            since,
            now,
            "tool run store does not exist",
        ));
    }
    with_read_store(store_path, busy_timeout, |conn| {
        if !table_present(conn, "runs")? {
            return Ok(empty_stats_report(
                &request,
                since,
                now,
                "tool run store has no runs table",
            ));
        }
        let (window, priors, truncated) = load_rows(
            conn,
            request.project.as_deref(),
            request.tool_name.as_deref(),
            since,
            lookback,
        )?;
        let (stages, prior_stages, stages_truncated) =
            load_stage_rows(conn, &window, &priors)?;
        let (samples, samples_truncated, skipped_samples) =
            load_sample_rows(conn, &window)?;
        let mut report = compute_stats_report(
            &window,
            &priors,
            &stages,
            &prior_stages,
            &samples,
            &StatsReportScope {
                project: request.project.clone(),
                tool_name: request.tool_name.clone(),
                days: request.days,
                since_ts: since,
                now_ts: now,
                utc_offset_seconds: request.utc_offset_seconds,
                runs_truncated: truncated,
                stages_truncated,
                samples_truncated,
            },
        )?;
        if skipped_samples > 0 {
            report.diagnostics.push(format!(
                "skipped {skipped_samples} unparseable load samples"
            ));
        }
        Ok(report)
    })
}

fn empty_stats_report(
    request: &ToolRunStatsRequestWire,
    since_ts: i64,
    now_ts: i64,
    diagnostic: &str,
) -> ToolRunStatsResultWire {
    ToolRunStatsResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: request.project.clone(),
        tool_name: request.tool_name.clone(),
        window: super::super::stats::ToolRunStatsWindowWire {
            days: request.days,
            since_ts,
            now_ts,
            utc_offset_seconds: request.utc_offset_seconds,
        },
        runs_scanned: 0,
        runs_truncated: false,
        stages_truncated: false,
        samples_truncated: false,
        adhoc_runs: 0,
        tools: Vec::new(),
        pressure: ToolRunStatsPressureWire::default(),
        thresholds: stats_thresholds(),
        diagnostics: vec![diagnostic.to_string()],
    }
}

fn table_present(conn: &Connection, table: &str) -> Result<bool, ToolRunError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn load_rows(
    conn: &Connection,
    project: Option<&str>,
    tool_name: Option<&str>,
    since_ts: i64,
    lookback_ts: i64,
) -> Result<(Vec<StatsRunRow>, Vec<StatsRunRow>, bool), ToolRunError> {
    // Stores written before a column existed select a NULL placeholder so
    // the positional mapping below holds for every layout; reads never
    // migrate.
    let columns = runs_column_set(conn)?;
    let projection = |name: &str| {
        if columns.contains(name) {
            name.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let terminal_projection = projection("terminal_cause");
    let starter_projection = projection("starter_json");
    let join_projection = projection("join_json");
    let demand_projection = projection("demand_json");
    let launch_projection = projection("launch_mode");
    let mut sql = format!(
        "SELECT run_id, tool_name, project, agent, owner_kind, state,
                created_ts, running_ts, settled_ts, duration_ms,
                definition_digest, extra_args_digest,
                {terminal_projection}, {starter_projection},
                {join_projection}, {demand_projection},
                fingerprint_before_json, {launch_projection}
         FROM runs
         WHERE source = 'native' AND created_ts >= ?1"
    );
    let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(lookback_ts)];
    if let Some(project) = project {
        let index = args.len() + 1;
        sql.push_str(&format!(" AND project = ?{index}"));
        args.push(Box::new(project.to_string()));
    }
    if let Some(tool) = tool_name {
        let index = args.len() + 1;
        sql.push_str(&format!(" AND tool_name = ?{index}"));
        args.push(Box::new(tool.to_string()));
    }
    let limit = args.len() + 1;
    sql.push_str(&format!(
        " ORDER BY created_ts DESC, run_id DESC LIMIT ?{limit}"
    ));
    args.push(Box::new((STATS_MAX_RUNS + 1) as i64));
    let arg_refs: Vec<&dyn rusqlite::ToSql> =
        args.iter().map(AsRef::as_ref).collect();
    let mut stmt = conn.prepare(&sql)?;
    let fetched = stmt.query_map(arg_refs.as_slice(), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Option<String>>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, i64>(6)?,
            row.get::<_, Option<i64>>(7)?,
            row.get::<_, Option<i64>>(8)?,
            row.get::<_, Option<i64>>(9)?,
            row.get::<_, String>(10)?,
            row.get::<_, String>(11)?,
            row.get::<_, Option<String>>(12)?,
            row.get::<_, Option<String>>(13)?,
            row.get::<_, Option<String>>(14)?,
            row.get::<_, Option<String>>(15)?,
            row.get::<_, Option<String>>(16)?,
            row.get::<_, Option<String>>(17)?,
        ))
    })?;
    #[allow(clippy::type_complexity)]
    let mut collected: Vec<(
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
        i64,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = Vec::new();
    for row in fetched {
        collected.push(row?);
    }
    let truncated = collected.len() > STATS_MAX_RUNS;
    collected.truncate(STATS_MAX_RUNS);
    let mut window = Vec::new();
    let mut priors = Vec::new();
    for (
        run_id,
        tool,
        project,
        agent,
        owner_kind,
        state,
        created_ts,
        running_ts,
        settled_ts,
        duration_ms,
        definition_digest,
        extra_args_digest,
        terminal_cause,
        starter_json,
        join_json,
        demand_json,
        fingerprint_before_json,
        launch_mode,
    ) in collected
    {
        let row = StatsRunRow {
            run_id,
            tool_name: tool,
            project,
            agent,
            owner_kind,
            state: ToolRunStateWire::from_db(&state)
                .map_err(ToolRunError::store)?,
            created_ts,
            running_ts,
            settled_ts,
            duration_ms,
            definition_digest,
            extra_args_digest,
            terminal_cause,
            launch_mode,
            has_starter: is_record_present(starter_json),
            has_join: is_record_present(join_json),
            demand: parse_json(demand_json),
            fingerprint_before: parse_json(fingerprint_before_json),
        };
        if created_ts >= since_ts {
            window.push(row);
        } else {
            priors.push(row);
        }
    }
    Ok((window, priors, truncated))
}

fn is_record_present(raw: Option<String>) -> bool {
    match raw {
        None => false,
        Some(text) => {
            let trimmed = text.trim();
            !trimmed.is_empty() && trimmed != "null"
        }
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    raw: Option<String>,
) -> Option<T> {
    let text = raw?;
    if text.trim().is_empty() {
        return None;
    }
    serde_json::from_str(&text).ok()
}

/// Sample payload fields the pressure section needs. Lenient on
/// purpose: no `deny_unknown_fields`, so a newer writer's extra fields
/// never drop a sample.
#[derive(Debug, Default, Deserialize)]
struct SamplePayload {
    #[serde(default)]
    psi_cpu_some: Option<f64>,
    #[serde(default)]
    psi_memory_some: Option<f64>,
    #[serde(default)]
    psi_io_some: Option<f64>,
    #[serde(default)]
    loadavg_1: Option<f64>,
    #[serde(default)]
    logical_cpus: Option<u32>,
}

struct StageChunkRow {
    stage_id: String,
    run_id: String,
    description: String,
    started_ts: Option<i64>,
    elapsed_ms: Option<i64>,
    exit_code: Option<i32>,
    incomplete: i64,
}

/// Run one `WHERE run_id IN (...)` chunk over the stages table.
fn query_stage_chunk(
    conn: &Connection,
    chunk: &[&str],
) -> Result<Vec<StageChunkRow>, ToolRunError> {
    let placeholders = chunk
        .iter()
        .enumerate()
        .map(|(index, _)| format!("?{}", index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT stage_id, run_id, description, started_ts, elapsed_ms,
                exit_code, incomplete
         FROM stages WHERE run_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> =
        chunk.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
    let fetched = stmt.query_map(params.as_slice(), |row| {
        Ok(StageChunkRow {
            stage_id: row.get(0)?,
            run_id: row.get(1)?,
            description: row.get(2)?,
            started_ts: row.get(3)?,
            elapsed_ms: row.get(4)?,
            exit_code: row.get(5)?,
            incomplete: row.get(6)?,
        })
    })?;
    fetched.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Stage rows of the loaded window and lookback runs, split by window
/// membership. Stages whose run has no tool never join a group and are
/// skipped. Keeps the newest rows under `STATS_MAX_STAGES`.
fn load_stage_rows(
    conn: &Connection,
    window: &[StatsRunRow],
    priors: &[StatsRunRow],
) -> Result<(Vec<StatsStageRow>, Vec<StatsStageRow>, bool), ToolRunError> {
    if !table_present(conn, "stages")? {
        return Ok((Vec::new(), Vec::new(), false));
    }
    let mut lookup: HashMap<&str, (&StatsRunRow, bool)> = HashMap::new();
    for row in window {
        lookup.insert(row.run_id.as_str(), (row, true));
    }
    for row in priors {
        lookup.insert(row.run_id.as_str(), (row, false));
    }
    let mut ids: Vec<&str> = lookup.keys().copied().collect();
    ids.sort_unstable();
    let mut collected: Vec<StatsStageRow> = Vec::new();
    for chunk in ids.chunks(500) {
        for fetched in query_stage_chunk(conn, chunk)? {
            let Some((run, in_window)) = lookup.get(fetched.run_id.as_str())
            else {
                continue;
            };
            let Some(tool) = run.tool_name.clone() else {
                continue;
            };
            collected.push(StatsStageRow {
                stage_id: fetched.stage_id,
                run_id: fetched.run_id,
                project: run.project.clone().unwrap_or_default(),
                tool_name: tool,
                description: fetched.description,
                started_ts: fetched.started_ts,
                elapsed_ms: fetched.elapsed_ms,
                exit_code: fetched.exit_code,
                incomplete: fetched.incomplete != 0,
                run_running_ts: run.running_ts,
                in_window: *in_window,
            });
        }
    }
    // Newest first; rows without a start stamp sort last.
    collected.sort_by(|left, right| {
        match (left.started_ts, right.started_ts) {
            (Some(left_ts), Some(right_ts)) => right_ts
                .cmp(&left_ts)
                .then_with(|| right.stage_id.cmp(&left.stage_id)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => right.stage_id.cmp(&left.stage_id),
        }
    });
    let truncated = collected.len() > STATS_MAX_STAGES;
    collected.truncate(STATS_MAX_STAGES);
    let mut stages = Vec::new();
    let mut prior_stages = Vec::new();
    for stage in collected {
        if stage.in_window {
            stages.push(stage);
        } else {
            prior_stages.push(stage);
        }
    }
    Ok((stages, prior_stages, truncated))
}

/// Run one `WHERE run_id IN (...)` chunk over the samples table.
fn query_sample_chunk(
    conn: &Connection,
    chunk: &[&str],
) -> Result<Vec<(String, i64, String)>, ToolRunError> {
    let placeholders = chunk
        .iter()
        .enumerate()
        .map(|(index, _)| format!("?{}", index + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT run_id, observed_ts, payload_json
         FROM samples WHERE run_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let params: Vec<&dyn rusqlite::ToSql> =
        chunk.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
    let fetched = stmt.query_map(params.as_slice(), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    fetched.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

/// Sample rows of the loaded window runs for the report-level
/// pressure section. A payload that fails to parse is skipped and
/// counted; the count surfaces as a result diagnostic. Keeps the
/// newest rows under `STATS_MAX_SAMPLES`.
fn load_sample_rows(
    conn: &Connection,
    window: &[StatsRunRow],
) -> Result<(Vec<StatsSampleRow>, bool, usize), ToolRunError> {
    if !table_present(conn, "samples")? {
        return Ok((Vec::new(), false, 0));
    }
    let mut ids: Vec<&str> =
        window.iter().map(|row| row.run_id.as_str()).collect();
    ids.sort_unstable();
    let mut collected: Vec<StatsSampleRow> = Vec::new();
    let mut skipped_unparseable = 0;
    for chunk in ids.chunks(500) {
        for (run_id, observed_ts, payload) in query_sample_chunk(conn, chunk)? {
            let parsed: SamplePayload = match serde_json::from_str(&payload) {
                Ok(parsed) => parsed,
                Err(_) => {
                    skipped_unparseable += 1;
                    continue;
                }
            };
            let load_per_cpu = match (parsed.loadavg_1, parsed.logical_cpus) {
                (Some(load), Some(cpus)) if cpus > 0 => {
                    Some(load / f64::from(cpus))
                }
                _ => None,
            };
            collected.push(StatsSampleRow {
                run_id,
                observed_ts,
                psi_cpu_some: parsed.psi_cpu_some,
                psi_memory_some: parsed.psi_memory_some,
                psi_io_some: parsed.psi_io_some,
                load_per_cpu,
            });
        }
    }
    collected.sort_by(|left, right| {
        right
            .observed_ts
            .cmp(&left.observed_ts)
            .then_with(|| right.run_id.cmp(&left.run_id))
    });
    let truncated = collected.len() > STATS_MAX_SAMPLES;
    collected.truncate(STATS_MAX_SAMPLES);
    Ok((collected, truncated, skipped_unparseable))
}

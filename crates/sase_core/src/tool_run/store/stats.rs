//! Bounded read-only ToolRun stats report.
//!
//! Loads in-window plus backtest-lookback rows in one newest-first query,
//! then folds them with the pure `stats` computation. Reads only; never
//! migrates, quarantines, or writes the ledger.

use std::path::Path;
use std::time::Duration;

use rusqlite::Connection;

use super::super::stats::{
    compute_stats_report, stats_thresholds, StatsReportScope, StatsRunRow,
    ToolRunStatsRequestWire, ToolRunStatsResultWire,
    STATS_BACKTEST_LOOKBACK_DAYS, STATS_MAX_DAYS, STATS_MAX_RUNS,
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
        compute_stats_report(
            &window,
            &priors,
            &StatsReportScope {
                project: request.project.clone(),
                tool_name: request.tool_name.clone(),
                days: request.days,
                since_ts: since,
                now_ts: now,
                utc_offset_seconds: request.utc_offset_seconds,
                runs_truncated: truncated,
            },
        )
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
        adhoc_runs: 0,
        tools: Vec::new(),
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

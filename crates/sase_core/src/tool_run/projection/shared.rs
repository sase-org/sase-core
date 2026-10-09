//! Shared lean loaders for the glance projections.
//!
//! Every loader selects lean columns in one statement per table and batches
//! by `run_id IN (...)`. Nothing here selects `private_argv_json`,
//! `fingerprint_*_json`, `launch_envelope_json`, or `launcher_json`, and
//! nothing calls the per-row `load_run`.

use std::collections::HashMap;

use rusqlite::{Connection, OptionalExtension};

use super::super::handoff_wire::ToolRunJoinRecordWire;
use super::super::store::connection::runs_column_set;
use super::super::store::{triage_tables_present, typical_duration_for};
use super::super::triage::{
    tool_run_triage_verdict, ToolRunTriageVerdictItemWire,
    ToolRunTriageVerdictRequestWire, TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
use super::super::wire::{ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION};
use super::super::ToolRunError;
use super::wire::{
    ToolRunBriefWire, ToolRunGlanceStageWire, ToolRunGlanceWire,
    ToolRunVerdictBucketWire, ToolRunVerdictSummaryWire,
    TOOL_RUN_LABEL_MAX_CHARS,
};

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

pub(super) fn missing_store_diagnostic() -> String {
    "tool run store does not exist".to_string()
}

/// `"?, …, ?"` for a batched `IN (...)` list.
pub(super) fn placeholders(count: usize) -> String {
    std::iter::repeat_n("?", count)
        .collect::<Vec<_>>()
        .join(",")
}

/// One stored run's facts for the verdict request builder: state,
/// exit code, terminal cause, signal, interruption reason, lost reason.
type StoredRunFacts = (
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

pub(super) fn read_last_write_ts(conn: &Connection) -> Option<i64> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'last_write_ts'",
            [],
            |row| row.get(0),
        )
        .optional()
        .ok()?
        .flatten();
    raw.and_then(|value| value.parse().ok())
}

/// One lean run row. Only columns the glance surfaces need; newer columns
/// (`launch_mode`, `terminal_cause`, `stop_request_json`) read as `NULL` on
/// pre-update stores so reads keep working before the next write open.
pub(super) struct LeanRunRow {
    pub(super) run_id: String,
    pub(super) state: ToolRunStateWire,
    pub(super) tool_name: Option<String>,
    pub(super) definition_digest: String,
    pub(super) extra_args_digest: String,
    pub(super) display_argv: Vec<String>,
    pub(super) project: Option<String>,
    pub(super) agent: Option<String>,
    pub(super) workspace: Option<String>,
    pub(super) bead: Option<String>,
    pub(super) owner_kind: Option<String>,
    pub(super) owner_id: Option<String>,
    pub(super) parent_run_id: Option<String>,
    pub(super) created_ts: i64,
    pub(super) running_ts: Option<i64>,
    pub(super) settled_ts: Option<i64>,
    pub(super) duration_ms: Option<i64>,
    pub(super) exit_code: Option<i32>,
    pub(super) signal: Option<i32>,
    pub(super) launch_mode: Option<String>,
    pub(super) terminal_cause: Option<String>,
    pub(super) stop_requested: bool,
}

pub(super) fn lean_projection_sql(
    conn: &Connection,
    extra_where: &str,
    extra_order_limit: &str,
) -> Result<String, ToolRunError> {
    let columns = runs_column_set(conn)?;
    let projection = |name: &str| {
        if columns.contains(name) {
            name.to_string()
        } else {
            "NULL".to_string()
        }
    };
    Ok(format!(
        "SELECT run_id, state, tool_name, definition_digest,
                extra_args_digest, display_argv_json, project, agent,
                workspace, bead, owner_kind, owner_id, parent_run_id,
                created_ts, running_ts, settled_ts, duration_ms,
                exit_code, signal, {launch_mode}, {terminal_cause},
                {stop_request}
         FROM runs WHERE {extra_where} {extra_order_limit}",
        launch_mode = projection("launch_mode"),
        terminal_cause = projection("terminal_cause"),
        stop_request = projection("stop_request_json"),
    ))
}

fn lean_row_from(row: &rusqlite::Row<'_>) -> Result<LeanRunRow, ToolRunError> {
    let state_raw: String = row.get(1)?;
    let display_raw: String = row.get(5)?;
    let stop_raw: Option<String> = row.get(21)?;
    Ok(LeanRunRow {
        run_id: row.get(0)?,
        state: ToolRunStateWire::from_db(&state_raw)
            .map_err(ToolRunError::store)?,
        tool_name: row.get(2)?,
        definition_digest: row.get(3)?,
        extra_args_digest: row.get(4)?,
        display_argv: serde_json::from_str(&display_raw)
            .map_err(|error| ToolRunError::store(error.to_string()))?,
        project: row.get(6)?,
        agent: row.get(7)?,
        workspace: row.get(8)?,
        bead: row.get(9)?,
        owner_kind: row.get(10)?,
        owner_id: row.get(11)?,
        parent_run_id: row.get(12)?,
        created_ts: row.get(13)?,
        running_ts: row.get(14)?,
        settled_ts: row.get(15)?,
        duration_ms: row.get(16)?,
        exit_code: row.get(17)?,
        signal: row.get(18)?,
        launch_mode: row.get(19)?,
        terminal_cause: row.get(20)?,
        stop_requested: stop_raw.is_some(),
    })
}

pub(super) fn collect_lean(
    conn: &Connection,
    sql: &str,
    values: Vec<String>,
) -> Result<Vec<LeanRunRow>, ToolRunError> {
    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(values.iter()))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(lean_row_from(row)?);
    }
    Ok(out)
}

/// Lean rows for an explicit id list, newest first. Empty input runs no
/// query (an empty `IN ()` is a syntax error).
pub(super) fn lean_rows_for_ids(
    conn: &Connection,
    run_ids: &[String],
) -> Result<Vec<LeanRunRow>, ToolRunError> {
    if run_ids.is_empty() {
        return Ok(Vec::new());
    }
    let placeholders = placeholders(run_ids.len());
    let sql = lean_projection_sql(
        conn,
        &format!("run_id IN ({placeholders})"),
        "ORDER BY created_ts DESC, run_id DESC",
    )?;
    collect_lean(conn, &sql, run_ids.to_vec())
}

/// Human label: the tool name, or for ad-hoc runs the basename of
/// `display_argv[0]`, capped at 12 characters.
pub(super) fn label_for(
    tool_name: Option<&str>,
    display_argv: &[String],
) -> String {
    if let Some(name) = tool_name {
        if !name.is_empty() {
            return truncate_label(name);
        }
    }
    if let Some(first) = display_argv.first() {
        if let Some(base) = first.rsplit('/').find(|part| !part.is_empty()) {
            return truncate_label(base);
        }
    }
    "run".to_string()
}

fn truncate_label(value: &str) -> String {
    if value.chars().count() > TOOL_RUN_LABEL_MAX_CHARS {
        value.chars().take(TOOL_RUN_LABEL_MAX_CHARS).collect()
    } else {
        value.to_string()
    }
}

/// Per-run stage rollup from one batched stages query.
pub(super) struct StageAgg {
    pub(super) done: u32,
    pub(super) total: u32,
    pub(super) current: Option<(String, i64)>,
    pub(super) max_stage_s: Option<i64>,
}

/// Stage progress and freshness in one `run_id IN (...)` statement. Stage
/// stamps are epoch milliseconds; the freshness bound converts to seconds.
pub(super) fn batch_stage_aggs(
    conn: &Connection,
    run_ids: &[String],
) -> Result<HashMap<String, StageAgg>, ToolRunError> {
    let mut aggs: HashMap<String, StageAgg> = HashMap::new();
    if run_ids.is_empty() {
        return Ok(aggs);
    }
    let placeholders = placeholders(run_ids.len());
    let sql = format!(
        "SELECT run_id, description, started_ts, finished_ts, incomplete
         FROM stages WHERE run_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
    // The in-flight stage is the incomplete row with the latest start; a
    // started-only row always carries its start stamp.
    let mut current_best: HashMap<String, (Option<i64>, String, i64)> =
        HashMap::new();
    while let Some(row) = rows.next()? {
        let run_id: String = row.get(0)?;
        let description: String = row.get(1)?;
        let started: Option<i64> = row.get(2)?;
        let finished: Option<i64> = row.get(3)?;
        let incomplete: i64 = row.get(4)?;
        let agg = aggs.entry(run_id.clone()).or_insert(StageAgg {
            done: 0,
            total: 0,
            current: None,
            max_stage_s: None,
        });
        agg.total += 1;
        if incomplete == 0 {
            agg.done += 1;
        } else if let Some(started_ms) = started {
            let better = current_best
                .get(&run_id)
                .is_none_or(|(best, _, _)| started_ms > best.unwrap_or(-1));
            if better {
                current_best
                    .insert(run_id.clone(), (started, description, started_ms));
            }
        }
        for stamp in [started, finished].into_iter().flatten() {
            let seconds = stamp.div_euclid(1000);
            agg.max_stage_s =
                Some(agg.max_stage_s.map_or(seconds, |best| best.max(seconds)));
        }
    }
    for (run_id, (_, description, started_ms)) in current_best {
        if let Some(agg) = aggs.get_mut(&run_id) {
            agg.current = Some((description, started_ms));
        }
    }
    Ok(aggs)
}

/// Newest sample `observed_ts` (seconds) per run, in one statement.
pub(super) fn batch_sample_max(
    conn: &Connection,
    run_ids: &[String],
) -> Result<HashMap<String, i64>, ToolRunError> {
    let mut out = HashMap::new();
    if run_ids.is_empty() {
        return Ok(out);
    }
    let placeholders = placeholders(run_ids.len());
    let sql = format!(
        "SELECT run_id, MAX(observed_ts) FROM samples
         WHERE run_id IN ({placeholders}) GROUP BY run_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
    while let Some(row) = rows.next()? {
        let run_id: String = row.get(0)?;
        let max_ts: Option<i64> = row.get(1)?;
        if let Some(max_ts) = max_ts {
            out.insert(run_id, max_ts);
        }
    }
    Ok(out)
}

/// One `(kind, id)` join record per run, in one `run_id IN (...)`
/// statement over `join_json`. Malformed records read as absent, and stores
/// written before the column existed select a NULL placeholder, so reads
/// keep working before the next write open. `release_join` clears the
/// record, so the glance shows `None` again.
pub(super) fn batch_joins(
    conn: &Connection,
    run_ids: &[String],
) -> Result<HashMap<String, (String, String)>, ToolRunError> {
    let mut out = HashMap::new();
    if run_ids.is_empty() {
        return Ok(out);
    }
    let columns = runs_column_set(conn)?;
    if !columns.contains("join_json") {
        return Ok(out);
    }
    let placeholders = placeholders(run_ids.len());
    let sql = format!(
        "SELECT run_id, join_json FROM runs WHERE run_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
    while let Some(row) = rows.next()? {
        let run_id: String = row.get(0)?;
        let raw: Option<String> = row.get(1)?;
        let Some(raw) = raw else { continue };
        if raw.trim().is_empty() {
            continue;
        }
        if let Ok(record) = serde_json::from_str::<ToolRunJoinRecordWire>(&raw)
        {
            out.insert(run_id, (record.kind, record.id));
        }
    }
    Ok(out)
}

pub(super) fn last_activity_ts(
    row: &LeanRunRow,
    stage_max_s: Option<i64>,
    sample_max: Option<i64>,
) -> i64 {
    let mut activity = row.created_ts;
    if let Some(running) = row.running_ts {
        activity = activity.max(running);
    }
    if let Some(stage) = stage_max_s {
        activity = activity.max(stage);
    }
    if let Some(sample) = sample_max {
        activity = activity.max(sample);
    }
    activity
}

struct VerdictInputs {
    state: String,
    exit_code: Option<i32>,
    terminal_cause: Option<String>,
    signal: Option<i32>,
    interruption: Option<String>,
    lost: Option<String>,
    has_completed_stage: bool,
    has_setup_marker: bool,
    has_failed_stage: bool,
    recipe_finished: bool,
    is_stageful: bool,
    triaged: bool,
    has_unparsed_failed_stage: bool,
    classes: Vec<Option<String>>,
}

/// Bucket precedence: live first, then terminal cause, then state lost,
/// then the triage verdict. First match wins.
pub(super) fn bucket_for(
    state: ToolRunStateWire,
    terminal_cause: Option<&str>,
    verdict: Option<&str>,
) -> ToolRunVerdictBucketWire {
    use ToolRunStateWire::{Created, Lost, Running};
    if matches!(state, Created | Running) {
        return ToolRunVerdictBucketWire::Running;
    }
    match terminal_cause {
        Some("stop_requested") | Some("interrupt") => {
            return ToolRunVerdictBucketWire::Stopped;
        }
        Some("signal") | Some("timeout") => {
            return ToolRunVerdictBucketWire::Killed;
        }
        Some("owner_lost") | Some("wrapper_lost") | Some("launch_failed") => {
            return ToolRunVerdictBucketWire::Lost;
        }
        _ => {}
    }
    if matches!(state, Lost) {
        return ToolRunVerdictBucketWire::Lost;
    }
    match verdict {
        Some("pass") => ToolRunVerdictBucketWire::Pass,
        Some("new_failures") => ToolRunVerdictBucketWire::NewFailures,
        Some("no_new_failures") => ToolRunVerdictBucketWire::KnownOnly,
        _ => ToolRunVerdictBucketWire::Undetermined,
    }
}

fn verdict_request_for(
    inputs: &VerdictInputs,
) -> ToolRunTriageVerdictRequestWire {
    let has_terminal = inputs.terminal_cause.is_some();
    ToolRunTriageVerdictRequestWire {
        schema_version: schema_version(),
        exit_code: inputs.exit_code,
        terminal_cause: inputs.terminal_cause.clone(),
        legacy_state: if has_terminal {
            None
        } else {
            Some(inputs.state.clone())
        },
        legacy_exit_code: if has_terminal { None } else { inputs.exit_code },
        legacy_signal: if has_terminal { None } else { inputs.signal },
        legacy_interruption_reason: if has_terminal {
            None
        } else {
            inputs.interruption.clone()
        },
        legacy_lost_reason: if has_terminal {
            None
        } else {
            inputs.lost.clone()
        },
        has_completed_stage: inputs.has_completed_stage,
        has_setup_marker: inputs.has_setup_marker,
        has_failed_stage: inputs.has_failed_stage,
        all_stages_complete: true,
        recipe_finished: inputs.recipe_finished,
        is_stageful_tool: inputs.is_stageful,
        triaged: inputs.triaged,
        has_unparsed_failed_stage: inputs.has_unparsed_failed_stage,
        items: inputs
            .classes
            .iter()
            .map(|class| ToolRunTriageVerdictItemWire {
                class: class.clone(),
            })
            .collect(),
    }
}

fn unstored_inputs(
    state: &str,
    exit_code: Option<i32>,
    terminal_cause: Option<String>,
    signal: Option<i32>,
    interruption: Option<String>,
    lost: Option<String>,
) -> VerdictInputs {
    VerdictInputs {
        state: state.to_string(),
        exit_code,
        terminal_cause,
        signal,
        interruption,
        lost,
        has_completed_stage: false,
        has_setup_marker: false,
        has_failed_stage: state == "failed",
        recipe_finished: false,
        is_stageful: true,
        triaged: false,
        has_unparsed_failed_stage: false,
        classes: Vec::new(),
    }
}

/// One shared verdict summary per run through the existing
/// `tool_run_triage_verdict` logic, with the state-vocabulary bucket
/// precedence applied. Runs the run-facts, stage, item, and triage-run
/// lookups batched by `run_id IN (...)`.
///
/// This mirrors the `triage_show` request-building semantics. The two
/// store-side copies (`triage_show`, `receipt::load_triage_facts`) stay on
/// their own copies until their outputs are proven identical
/// (PROPOSED FOLLOW-UP on the phase bead).
pub(super) fn verdict_summary_for_runs(
    conn: &Connection,
    run_ids: &[String],
) -> Result<HashMap<String, ToolRunVerdictSummaryWire>, ToolRunError> {
    let mut out = HashMap::new();
    if run_ids.is_empty() {
        return Ok(out);
    }
    let placeholders = placeholders(run_ids.len());
    let columns = runs_column_set(conn)?;
    let terminal_projection = if columns.contains("terminal_cause") {
        "terminal_cause".to_string()
    } else {
        "NULL".to_string()
    };
    let facts_sql = format!(
        "SELECT run_id, state, exit_code, {terminal_projection}, signal,
                interruption_reason, lost_reason
         FROM runs WHERE run_id IN ({placeholders})"
    );
    let mut stmt = conn.prepare(&facts_sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
    let mut facts: HashMap<String, StoredRunFacts> = HashMap::new();
    while let Some(row) = rows.next()? {
        let run_id: String = row.get(0)?;
        facts.insert(
            run_id,
            (
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ),
        );
    }
    let has_triage = triage_tables_present(conn)?;
    // Per-run triage facts: stage keys (for completed/stageless checks),
    // unparsed extraction statuses, item classes, environment-extractor
    // setup markers, and recipe/triaged timestamps.
    let mut stage_keys: HashMap<String, Vec<String>> = HashMap::new();
    let mut unparsed: HashMap<String, bool> = HashMap::new();
    let mut classes: HashMap<String, Vec<Option<String>>> = HashMap::new();
    let mut setup_marker: HashMap<String, bool> = HashMap::new();
    let mut recipe: HashMap<String, (bool, bool)> = HashMap::new();
    if has_triage {
        let stages_sql = format!(
            "SELECT run_id, stage_key, extraction_status
             FROM tool_triage_stages WHERE run_id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&stages_sql)?;
        let mut rows =
            stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
        while let Some(row) = rows.next()? {
            let run_id: String = row.get(0)?;
            let stage_key: String = row.get(1)?;
            let status: String = row.get(2)?;
            stage_keys
                .entry(run_id.clone())
                .or_default()
                .push(stage_key);
            if status != "parsed" {
                unparsed.insert(run_id, true);
            }
        }
        let items_sql = format!(
            "SELECT run_id, extractor, class FROM tool_triage_items
             WHERE run_id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&items_sql)?;
        let mut rows =
            stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
        while let Some(row) = rows.next()? {
            let run_id: String = row.get(0)?;
            let extractor: String = row.get(1)?;
            let class: Option<String> = row.get(2)?;
            if extractor == "environment" {
                setup_marker.insert(run_id.clone(), true);
            }
            classes.entry(run_id).or_default().push(class);
        }
        let runs_sql = format!(
            "SELECT run_id, recipe_finished_ts, triaged_ts
             FROM tool_triage_runs WHERE run_id IN ({placeholders})"
        );
        let mut stmt = conn.prepare(&runs_sql)?;
        let mut rows =
            stmt.query(rusqlite::params_from_iter(run_ids.iter()))?;
        while let Some(row) = rows.next()? {
            let run_id: String = row.get(0)?;
            let recipe_ts: Option<i64> = row.get(1)?;
            let triaged_ts: Option<i64> = row.get(2)?;
            recipe.insert(run_id, (recipe_ts.is_some(), triaged_ts.is_some()));
        }
    }
    for run_id in run_ids {
        let Some((
            state,
            exit_code,
            terminal_cause,
            signal,
            interruption,
            lost,
        )) = facts.get(run_id)
        else {
            continue;
        };
        let state_wire =
            ToolRunStateWire::from_db(state).map_err(ToolRunError::store)?;
        let inputs = if !has_triage {
            unstored_inputs(
                state,
                exit_code.map(|code| code as i32),
                terminal_cause.clone(),
                signal.map(|code| code as i32),
                interruption.clone(),
                lost.clone(),
            )
        } else {
            let keys = stage_keys.get(run_id);
            let completed = keys.is_some_and(|keys| {
                keys.iter()
                    .any(|key| key != TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT)
            });
            // A run_output-only triage stage set means a stageless tool.
            let is_stageful = keys.is_none_or(|keys| {
                keys.is_empty()
                    || keys
                        .iter()
                        .any(|key| key != TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT)
            });
            let run_classes = classes.get(run_id).cloned().unwrap_or_default();
            let (recipe_finished, triaged) =
                recipe.get(run_id).copied().unwrap_or((false, false));
            VerdictInputs {
                state: state.clone(),
                exit_code: exit_code.map(|code| code as i32),
                terminal_cause: terminal_cause.clone(),
                signal: signal.map(|code| code as i32),
                interruption: interruption.clone(),
                lost: lost.clone(),
                has_completed_stage: completed,
                has_setup_marker: setup_marker
                    .get(run_id)
                    .copied()
                    .unwrap_or(false),
                has_failed_stage: state == "failed"
                    && (completed || !run_classes.is_empty()),
                recipe_finished,
                is_stageful,
                triaged,
                has_unparsed_failed_stage: unparsed
                    .get(run_id)
                    .copied()
                    .unwrap_or(false)
                    && state == "failed",
                classes: run_classes,
            }
        };
        out.insert(run_id.clone(), summarize_inputs(&inputs, state_wire)?);
    }
    Ok(out)
}

fn summarize_inputs(
    inputs: &VerdictInputs,
    state_wire: ToolRunStateWire,
) -> Result<ToolRunVerdictSummaryWire, ToolRunError> {
    let mut new = 0u32;
    let mut known = 0u32;
    let mut flaky = 0u32;
    let mut unknown = 0u32;
    let mut unlabeled = 0u32;
    for class in &inputs.classes {
        match class.as_deref() {
            Some("new") => new += 1,
            Some("known") => known += 1,
            Some("flaky") => flaky += 1,
            Some("unknown") => unknown += 1,
            _ => unlabeled += 1,
        }
    }
    let request = verdict_request_for(inputs);
    match tool_run_triage_verdict(request) {
        Ok(result) => {
            let verdict = result.verdict.as_str().to_string();
            let bucket = bucket_for(
                state_wire,
                inputs.terminal_cause.as_deref(),
                Some(&verdict),
            );
            Ok(ToolRunVerdictSummaryWire {
                schema_version: schema_version(),
                bucket,
                verdict: Some(verdict),
                failure_kind: Some(result.kind.as_str().to_string()),
                reasons: vec![result.reason],
                new,
                known,
                flaky,
                unknown,
                unlabeled,
            })
        }
        Err(_) => Ok(ToolRunVerdictSummaryWire {
            schema_version: schema_version(),
            bucket: bucket_for(
                state_wire,
                inputs.terminal_cause.as_deref(),
                None,
            ),
            verdict: None,
            failure_kind: None,
            reasons: Vec::new(),
            new,
            known,
            flaky,
            unknown,
            unlabeled,
        }),
    }
}

/// Newest run with the same project, tool, and digest in a terminal
/// succeeded/failed state whose stages are all complete. Returns the
/// reference id plus its stage count (`None` when it recorded no stages).
/// Runs with incomplete stages are skipped; `None` when there is no match.
pub(super) fn reference_for(
    conn: &Connection,
    project: &str,
    tool_name: &str,
    definition_digest: &str,
) -> Result<Option<(String, Option<u32>)>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT run_id FROM runs
         WHERE project = ?1 AND tool_name = ?2 AND definition_digest = ?3
           AND state IN ('succeeded', 'failed')
         ORDER BY created_ts DESC, run_id DESC LIMIT 10",
    )?;
    let candidates = stmt
        .query_map(
            rusqlite::params![project, tool_name, definition_digest],
            |row| row.get::<_, String>(0),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    if candidates.is_empty() {
        return Ok(None);
    }
    let placeholders = placeholders(candidates.len());
    let sql = format!(
        "SELECT run_id, COUNT(*), COALESCE(SUM(incomplete), 0)
         FROM stages WHERE run_id IN ({placeholders}) GROUP BY run_id"
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(rusqlite::params_from_iter(candidates.iter()))?;
    let mut stats: HashMap<String, (i64, i64)> = HashMap::new();
    while let Some(row) = rows.next()? {
        let run_id: String = row.get(0)?;
        let count: i64 = row.get(1)?;
        let incomplete: i64 = row.get(2)?;
        stats.insert(run_id, (count, incomplete));
    }
    for candidate in candidates {
        match stats.get(&candidate) {
            // No stage rows: vacuously complete, but no expected stages.
            None => return Ok(Some((candidate, None))),
            Some((count, incomplete)) if *incomplete == 0 => {
                let expected = if *count > 0 {
                    Some(*count as u32)
                } else {
                    None
                };
                return Ok(Some((candidate, expected)));
            }
            // Incomplete stages: skip to the next older candidate.
            Some(_) => {}
        }
    }
    Ok(None)
}

/// Typical duration through the helper `summarize` uses.
pub(super) fn typical_for(
    conn: &Connection,
    project: &str,
    tool_name: &str,
    definition_digest: &str,
    extra_args_digest: &str,
    now: i64,
) -> Result<(Option<i64>, u32), ToolRunError> {
    typical_duration_for(
        conn,
        project,
        tool_name,
        definition_digest,
        extra_args_digest,
        now,
    )
}

/// Cursors have the `list_runs` form `<created_ts>:<run_id>`.
pub(super) fn parse_cursor(
    cursor: &str,
) -> Result<(i64, String), ToolRunError> {
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

pub(super) fn format_cursor(created_ts: i64, run_id: &str) -> String {
    format!("{created_ts}:{run_id}")
}

fn fallback_summary(
    state: ToolRunStateWire,
    terminal_cause: Option<&str>,
) -> ToolRunVerdictSummaryWire {
    ToolRunVerdictSummaryWire {
        schema_version: schema_version(),
        bucket: bucket_for(state, terminal_cause, None),
        verdict: None,
        failure_kind: None,
        reasons: Vec::new(),
        new: 0,
        known: 0,
        flaky: 0,
        unknown: 0,
        unlabeled: 0,
    }
}

/// Batched per-run lookups shared by all three projections, plus
/// per-key reference/typical caches. Callers gather every id they will
/// render first so stages, samples, and verdicts each cost one statement.
pub(super) struct BatchContext {
    stages: HashMap<String, StageAgg>,
    samples: HashMap<String, i64>,
    joins: HashMap<String, (String, String)>,
    verdicts: HashMap<String, ToolRunVerdictSummaryWire>,
    references: HashMap<String, Option<(String, Option<u32>)>>,
    typicals: HashMap<String, (Option<i64>, u32)>,
    now: i64,
}

impl BatchContext {
    pub(super) fn load(
        conn: &Connection,
        run_ids: &[String],
        now: i64,
    ) -> Result<Self, ToolRunError> {
        Ok(Self {
            stages: batch_stage_aggs(conn, run_ids)?,
            samples: batch_sample_max(conn, run_ids)?,
            joins: batch_joins(conn, run_ids)?,
            verdicts: verdict_summary_for_runs(conn, run_ids)?,
            references: HashMap::new(),
            typicals: HashMap::new(),
            now,
        })
    }

    fn reference_key(row: &LeanRunRow) -> Option<String> {
        match (&row.project, &row.tool_name) {
            (Some(project), Some(tool)) => Some(format!(
                "{project}\x1f{tool}\x1f{}",
                row.definition_digest
            )),
            _ => None,
        }
    }

    fn reference_for_row(
        &mut self,
        conn: &Connection,
        row: &LeanRunRow,
    ) -> Result<Option<(String, Option<u32>)>, ToolRunError> {
        let Some(key) = Self::reference_key(row) else {
            return Ok(None);
        };
        if let Some(cached) = self.references.get(&key) {
            return Ok(cached.clone());
        }
        let found = reference_for(
            conn,
            row.project.as_deref().unwrap_or_default(),
            row.tool_name.as_deref().unwrap_or_default(),
            &row.definition_digest,
        )?;
        self.references.insert(key, found.clone());
        Ok(found)
    }

    fn typical_for_row(
        &mut self,
        conn: &Connection,
        row: &LeanRunRow,
    ) -> Result<(Option<i64>, u32), ToolRunError> {
        let Some(mut key) = Self::reference_key(row) else {
            return Ok((None, 0));
        };
        key.push_str(&format!("\x1f{}", row.extra_args_digest));
        if let Some(cached) = self.typicals.get(&key) {
            return Ok(*cached);
        }
        let found = typical_for(
            conn,
            row.project.as_deref().unwrap_or_default(),
            row.tool_name.as_deref().unwrap_or_default(),
            &row.definition_digest,
            &row.extra_args_digest,
            self.now,
        )?;
        self.typicals.insert(key, found);
        Ok(found)
    }

    pub(super) fn glance_for_row(
        &mut self,
        conn: &Connection,
        row: &LeanRunRow,
    ) -> Result<ToolRunGlanceWire, ToolRunError> {
        let (stages_done, current, stage_max) =
            match self.stages.get(&row.run_id) {
                Some(agg) => (agg.done, agg.current.clone(), agg.max_stage_s),
                None => (0, None, None),
            };
        let sample_max = self.samples.get(&row.run_id).copied();
        let (join_kind, join_id) = self
            .joins
            .get(&row.run_id)
            .cloned()
            .map(|(kind, id)| (Some(kind), Some(id)))
            .unwrap_or((None, None));
        let (reference_run_id, stages_expected) =
            match self.reference_for_row(conn, row)? {
                Some((id, expected)) => (Some(id), expected),
                None => (None, None),
            };
        let (typical_ms, typical_samples) = self.typical_for_row(conn, row)?;
        let current_stage =
            current.map(|(description, started_ms)| ToolRunGlanceStageWire {
                schema_version: schema_version(),
                description,
                started_ms,
            });
        Ok(ToolRunGlanceWire {
            schema_version: schema_version(),
            run_id: row.run_id.clone(),
            tool_name: row.tool_name.clone(),
            label: label_for(row.tool_name.as_deref(), &row.display_argv),
            state: row.state,
            launch_mode: row.launch_mode.clone(),
            project: row.project.clone(),
            agent: row.agent.clone(),
            workspace: row.workspace.clone(),
            bead: row.bead.clone(),
            owner_kind: row.owner_kind.clone(),
            owner_id: row.owner_id.clone(),
            join_kind,
            join_id,
            parent_run_id: row.parent_run_id.clone(),
            created_ts: row.created_ts,
            running_ts: row.running_ts,
            last_activity_ts: last_activity_ts(row, stage_max, sample_max),
            current_stage,
            stages_done,
            stages_expected,
            reference_run_id,
            typical_ms,
            typical_samples,
            stop_requested: row.stop_requested,
        })
    }

    pub(super) fn brief_for_row(
        &mut self,
        conn: &Connection,
        row: &LeanRunRow,
    ) -> Result<ToolRunBriefWire, ToolRunError> {
        let stage_total =
            self.stages.get(&row.run_id).map_or(0, |agg| agg.total);
        let (typical_ms, _) = self.typical_for_row(conn, row)?;
        let verdict =
            self.verdicts.get(&row.run_id).cloned().unwrap_or_else(|| {
                fallback_summary(row.state, row.terminal_cause.as_deref())
            });
        Ok(ToolRunBriefWire {
            schema_version: schema_version(),
            run_id: row.run_id.clone(),
            tool_name: row.tool_name.clone(),
            label: label_for(row.tool_name.as_deref(), &row.display_argv),
            state: row.state,
            launch_mode: row.launch_mode.clone(),
            exit_code: row.exit_code,
            signal: row.signal,
            terminal_cause: row.terminal_cause.clone(),
            project: row.project.clone(),
            agent: row.agent.clone(),
            workspace: row.workspace.clone(),
            bead: row.bead.clone(),
            owner_kind: row.owner_kind.clone(),
            owner_id: row.owner_id.clone(),
            parent_run_id: row.parent_run_id.clone(),
            created_ts: row.created_ts,
            running_ts: row.running_ts,
            settled_ts: row.settled_ts,
            duration_ms: row.duration_ms,
            typical_ms,
            verdict,
            // Retention deletes stage rows while the run row stays, so a
            // settled run with no stages reads as pruned detail.
            detail_pruned: row.state.is_terminal() && stage_total == 0,
            stop_requested: row.stop_requested,
        })
    }
}

//! `tool_run_detail`: one run for one card block.
//!
//! The run's brief, safe argv, millisecond stages, the reference run's
//! expected stages, witnessed triage items, child runs, log metadata, and
//! pruning facts. Read-only: lean columns in one statement per table,
//! batched lookups, no `private_argv` content anywhere, and no reconcile.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::time::Duration;

use rusqlite::OptionalExtension;

use super::super::store::connection::{
    runs_column_set, unix_now, validate_schema, with_read_store,
};
use super::super::store::triage_tables_present;
use super::super::wire::{
    ToolRunLogMetadataWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::shared::{
    lean_rows_for_ids, missing_store_diagnostic, placeholders, reference_for,
    BatchContext, LeanRunRow,
};
use super::wire::{
    ToolRunBriefWire, ToolRunDetailRequestWire, ToolRunDetailResultWire,
    ToolRunDetailStageCountsWire, ToolRunDetailStageWire,
    ToolRunDetailTriageItemWire, ToolRunExpectedStageWire,
    TOOL_RUN_DETAIL_MAX_ITEM_LIMIT, TOOL_RUN_DETAIL_MAX_LOCATORS,
    TOOL_RUN_DETAIL_MAX_WINDOW_DAYS,
};

/// Empty result for a missing store or an unknown run id.
fn empty_detail(
    store_exists: bool,
    found: bool,
    diagnostic: String,
) -> ToolRunDetailResultWire {
    ToolRunDetailResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        store_exists,
        found,
        brief: None,
        display_argv: Vec::new(),
        stages: Vec::new(),
        expected_stages: Vec::new(),
        triage_items: Vec::new(),
        items_truncated: false,
        child_runs: Vec::new(),
        logs: None,
        detail_pruned: false,
        diagnostics: vec![diagnostic],
    }
}

/// Read-only detail for one run. Never reconciles, never writes.
pub fn tool_run_detail(
    store_path: &Path,
    request: ToolRunDetailRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunDetailResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    if request.witness_window_days == 0
        || request.witness_window_days > TOOL_RUN_DETAIL_MAX_WINDOW_DAYS
    {
        return Err(ToolRunError::invalid(format!(
            "witness_window_days must be 1..={TOOL_RUN_DETAIL_MAX_WINDOW_DAYS}"
        )));
    }
    if request.item_limit == 0
        || request.item_limit > TOOL_RUN_DETAIL_MAX_ITEM_LIMIT
    {
        return Err(ToolRunError::invalid(format!(
            "item_limit must be 1..={TOOL_RUN_DETAIL_MAX_ITEM_LIMIT}"
        )));
    }
    if !store_path.exists() {
        return Ok(empty_detail(false, false, missing_store_diagnostic()));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let cutoff =
        now.saturating_sub(i64::from(request.witness_window_days) * 24 * 3600);
    let item_limit = request.item_limit as usize;
    with_read_store(store_path, busy_timeout, |conn| {
        let rows =
            lean_rows_for_ids(conn, std::slice::from_ref(&request.run_id))?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(empty_detail(
                true,
                false,
                format!("tool run {} was not found", request.run_id),
            ));
        };
        let child_ids = child_run_ids(conn, &row.run_id)?;
        let mut ids = vec![row.run_id.clone()];
        ids.extend(child_ids.iter().cloned());
        let mut context = BatchContext::load(conn, &ids, now)?;
        let brief = context.brief_for_row(conn, &row)?;
        let detail_pruned = brief.detail_pruned;
        let items = load_items(conn, &row.run_id)?;
        let stages = load_stages(conn, &row.run_id, &items)?;
        let expected_stages = load_expected_stages(conn, &row, &stages)?;
        let (triage_items, items_truncated) =
            witnessed_items(conn, &items, cutoff, item_limit)?;
        let child_runs = load_child_runs(conn, &mut context, &child_ids)?;
        let logs = load_logs(conn, &row.run_id)?;
        let mut diagnostics = Vec::new();
        if !triage_tables_present(conn)? && !detail_pruned {
            diagnostics.push("triage tables are absent".to_string());
        }
        Ok(ToolRunDetailResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            store_exists: true,
            found: true,
            brief: Some(brief),
            display_argv: row.display_argv.clone(),
            stages,
            expected_stages,
            triage_items,
            items_truncated,
            child_runs,
            logs: Some(logs),
            detail_pruned,
            diagnostics,
        })
    })
}

/// One triage item of the run: grouping keys plus display facts. Witness
/// counts join later in one batched statement.
struct DetailItem {
    stage_id: Option<String>,
    stage_key: String,
    class: Option<String>,
    display: String,
    locator_paths: Vec<String>,
    occurrences: u32,
    signature: String,
    extractor_version: i64,
    created_ts: i64,
}

/// The run's triage items, or an empty vec when triage tables are absent
/// (old stores stay readable). Unknown classes read as unlabeled.
fn load_items(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<Vec<DetailItem>, ToolRunError> {
    if !triage_tables_present(conn)? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT stage_id, stage_key, class, display, locator_paths_json,
                occurrences, signature, extractor_version, created_ts
         FROM tool_triage_items WHERE run_id = ?1",
    )?;
    let rows = stmt.query_map([run_id], |row| {
        Ok((
            row.get::<_, Option<String>>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
        ))
    })?;
    let mut items = Vec::new();
    for row in rows {
        let (
            stage_id,
            stage_key,
            class_raw,
            display,
            locators_raw,
            occurrences,
            signature,
            extractor_version,
            created_ts,
        ) = row?;
        let class = class_raw.and_then(|class| {
            matches!(class.as_str(), "new" | "known" | "flaky" | "unknown")
                .then_some(class)
        });
        let locator_paths: Vec<String> =
            serde_json::from_str(&locators_raw).unwrap_or_default();
        items.push(DetailItem {
            stage_id,
            stage_key,
            class,
            display,
            locator_paths,
            occurrences: u32::try_from(occurrences).unwrap_or(0),
            signature,
            extractor_version,
            created_ts,
        });
    }
    // Card order: NEW, UNKNOWN, unlabeled, KNOWN, FLAKY; created order
    // within a class, then identity for stability.
    items.sort_by(|left, right| {
        (
            class_rank(left.class.as_deref()),
            left.created_ts,
            &left.signature,
            &left.display,
        )
            .cmp(&(
                class_rank(right.class.as_deref()),
                right.created_ts,
                &right.signature,
                &right.display,
            ))
    });
    Ok(items)
}

fn class_rank(class: Option<&str>) -> u8 {
    match class {
        Some("new") => 0,
        Some("unknown") => 1,
        None => 2,
        Some("known") => 3,
        _ => 4,
    }
}

/// The run's stages with per-stage class counts joined from its triage
/// items by `stage_id`. Unlabeled items count toward no bucket.
fn load_stages(
    conn: &rusqlite::Connection,
    run_id: &str,
    items: &[DetailItem],
) -> Result<Vec<ToolRunDetailStageWire>, ToolRunError> {
    let mut counts: HashMap<String, ToolRunDetailStageCountsWire> =
        HashMap::new();
    for item in items.iter() {
        let (Some(stage_id), Some(class)) =
            (item.stage_id.as_ref(), item.class.as_ref())
        else {
            continue;
        };
        let entry = counts.entry(stage_id.clone()).or_default();
        match class.as_str() {
            "new" => entry.new += 1,
            "known" => entry.known += 1,
            "flaky" => entry.flaky += 1,
            "unknown" => entry.unknown += 1,
            _ => {}
        }
    }
    let mut stmt = conn.prepare(
        "SELECT stage_id, description, started_ts, finished_ts, elapsed_ms,
                exit_code, output_bytes, incomplete
         FROM stages WHERE run_id = ?1 ORDER BY started_ts, stage_id",
    )?;
    let rows = stmt.query_map([run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, Option<i64>>(4)?,
            row.get::<_, Option<i32>>(5)?,
            row.get::<_, Option<i64>>(6)?,
            row.get::<_, i64>(7)?,
        ))
    })?;
    let mut stages = Vec::new();
    for row in rows {
        let (
            stage_id,
            description,
            started_ms,
            finished_ms,
            elapsed_ms,
            exit_code,
            output_bytes,
            incomplete,
        ) = row?;
        stages.push(ToolRunDetailStageWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            description,
            started_ms,
            finished_ms,
            elapsed_ms,
            exit_code,
            incomplete: incomplete != 0,
            output_bytes,
            counts: counts.remove(&stage_id).unwrap_or_default(),
        });
    }
    Ok(stages)
}

/// The reference run's stage timeline. Filled when the run is live, or
/// when it settled before reaching every reference stage, so the card can
/// show pending and not-reached stages.
fn load_expected_stages(
    conn: &rusqlite::Connection,
    row: &LeanRunRow,
    stages: &[ToolRunDetailStageWire],
) -> Result<Vec<ToolRunExpectedStageWire>, ToolRunError> {
    let (Some(project), Some(tool)) =
        (row.project.as_ref(), row.tool_name.as_ref())
    else {
        return Ok(Vec::new());
    };
    let Some((reference_id, expected)) =
        reference_for(conn, project, tool, &row.definition_digest)?
    else {
        return Ok(Vec::new());
    };
    let Some(expected) = expected else {
        return Ok(Vec::new());
    };
    let done = stages.iter().filter(|stage| !stage.incomplete).count();
    if !row.state.is_unsettled() && done >= expected as usize {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT description, elapsed_ms FROM stages
         WHERE run_id = ?1 ORDER BY started_ts, stage_id",
    )?;
    let rows = stmt.query_map([reference_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?))
    })?;
    let mut expected_stages = Vec::new();
    for row in rows {
        let (description, elapsed_ms) = row?;
        expected_stages.push(ToolRunExpectedStageWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            description,
            elapsed_ms,
        });
    }
    Ok(expected_stages)
}

/// Witness counts share the item's `(signature, extractor_version)`
/// inside the window, with the failures grouping semantics: distinct
/// runs, distinct non-empty agents, and the window's first/last sight.
/// Items beyond `limit` are cut; an empty witness group (the item itself
/// pruned out of the window) reads as zero witnesses.
fn witnessed_items(
    conn: &rusqlite::Connection,
    items: &[DetailItem],
    cutoff: i64,
    limit: usize,
) -> Result<(Vec<ToolRunDetailTriageItemWire>, bool), ToolRunError> {
    let items_truncated = items.len() > limit;
    let shown = &items[..items.len().min(limit)];
    if shown.is_empty() {
        return Ok((Vec::new(), items_truncated));
    }
    let mut pairs: BTreeSet<(String, i64)> = BTreeSet::new();
    for item in shown {
        pairs.insert((item.signature.clone(), item.extractor_version));
    }
    let signatures: Vec<String> = pairs
        .iter()
        .map(|(signature, _)| signature.clone())
        .collect();
    let versions: Vec<i64> =
        pairs.iter().map(|(_, version)| *version).collect();
    let sql = format!(
        "SELECT items.signature, items.extractor_version,
                items.run_id, runs.agent, items.created_ts
         FROM tool_triage_items AS items
         JOIN runs ON runs.run_id = items.run_id
         WHERE items.created_ts >= ?1
           AND items.signature IN ({})
           AND items.extractor_version IN ({})",
        placeholders(signatures.len()),
        placeholders(versions.len()),
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut values: Vec<String> = vec![cutoff.to_string()];
    values.extend(signatures);
    values.extend(versions.iter().map(i64::to_string));
    let rows =
        stmt.query_map(rusqlite::params_from_iter(values.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
    #[allow(clippy::type_complexity)]
    let mut witnesses: BTreeMap<
        (String, i64),
        (BTreeSet<String>, BTreeSet<String>, Option<i64>, Option<i64>),
    > = BTreeMap::new();
    for row in rows {
        let (signature, version, run_id, agent, created_ts) = row?;
        if !pairs.contains(&(signature.clone(), version)) {
            continue;
        }
        let entry = witnesses
            .entry((signature, version))
            .or_insert_with(|| (BTreeSet::new(), BTreeSet::new(), None, None));
        entry.0.insert(run_id);
        if let Some(agent) = agent {
            if !agent.is_empty() {
                entry.1.insert(agent);
            }
        }
        entry.2 =
            Some(entry.2.map_or(created_ts, |first| first.min(created_ts)));
        entry.3 = Some(entry.3.map_or(created_ts, |last| last.max(created_ts)));
    }
    let mut out = Vec::with_capacity(shown.len());
    for item in shown {
        let key = (item.signature.clone(), item.extractor_version);
        let (runs, agents, first_seen_ts, last_seen_ts) = witnesses
            .remove(&key)
            .unwrap_or_else(|| (BTreeSet::new(), BTreeSet::new(), None, None));
        out.push(ToolRunDetailTriageItemWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            class: item.class.clone(),
            stage_key: item.stage_key.clone(),
            display: item.display.clone(),
            locator_paths: item
                .locator_paths
                .iter()
                .take(TOOL_RUN_DETAIL_MAX_LOCATORS)
                .cloned()
                .collect(),
            occurrences: item.occurrences,
            witness_runs: runs.len() as u32,
            witness_agents: agents.len() as u32,
            first_seen_ts,
            last_seen_ts,
        });
    }
    Ok((out, items_truncated))
}

/// Newest-first child run ids for nested `parent_run_id` lines.
fn child_run_ids(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<Vec<String>, ToolRunError> {
    let mut stmt = conn.prepare(
        "SELECT run_id FROM runs WHERE parent_run_id = ?1
         ORDER BY created_ts DESC, run_id DESC",
    )?;
    let rows = stmt.query_map([run_id], |row| row.get::<_, String>(0))?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

fn load_child_runs(
    conn: &rusqlite::Connection,
    context: &mut BatchContext,
    child_ids: &[String],
) -> Result<Vec<ToolRunBriefWire>, ToolRunError> {
    if child_ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = lean_rows_for_ids(conn, child_ids)?;
    let by_id: HashMap<&str, &LeanRunRow> =
        rows.iter().map(|row| (row.run_id.as_str(), row)).collect();
    let mut children = Vec::with_capacity(child_ids.len());
    for child_id in child_ids {
        if let Some(row) = by_id.get(child_id.as_str()) {
            children.push(context.brief_for_row(conn, row)?);
        }
    }
    Ok(children)
}

/// Log paths and the private-argv presence flag. The private argv
/// content is never selected, only its presence.
fn load_logs(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<ToolRunLogMetadataWire, ToolRunError> {
    let columns = runs_column_set(conn)?;
    let projection = |name: &str| {
        if columns.contains(name) {
            name.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let sql = format!(
        "SELECT {stdout}, {stderr}, {events},
                private_argv_json IS NOT NULL, {owner}
         FROM runs WHERE run_id = ?1",
        stdout = projection("log_stdout_path"),
        stderr = projection("log_stderr_path"),
        events = projection("events_path"),
        owner = projection("owner_log_path"),
    );
    let (stdout_path, stderr_path, events_path, has_private, owner_log_path) =
        conn.query_row(sql.as_str(), [run_id], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        })
        .optional()?
        .unwrap_or((None, None, None, 0, None));
    Ok(ToolRunLogMetadataWire {
        stdout_path,
        stderr_path,
        events_path,
        has_private_argv: has_private != 0,
        owner_log_path,
    })
}

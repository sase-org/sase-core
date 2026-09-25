//! Read-only failures aggregation grouped by signature.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use super::super::triage::{
    ToolRunFailuresGroupWire, ToolRunFailuresRequestWire,
    ToolRunFailuresResultWire, TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::connection::{validate_schema, with_read_store};
use super::triage::triage_tables_present;

pub fn tool_run_failures(
    store_path: &Path,
    request: ToolRunFailuresRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunFailuresResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if !store_path.exists() {
        return Ok(ToolRunFailuresResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            groups: Vec::new(),
            diagnostics: Vec::new(),
        });
    }
    if let Some(class) = request.class.as_deref() {
        if !matches!(class, "new" | "known" | "flaky" | "unknown") {
            return Err(ToolRunError::invalid(
                "failures class must be new, known, flaky, or unknown",
            ));
        }
    }
    let now = request.now_ts.unwrap_or_else(super::connection::unix_now);
    let cutoff = now.saturating_sub(i64::from(request.days) * 24 * 3600);
    with_read_store(store_path, busy_timeout, |conn| {
        if !triage_tables_present(conn)? {
            return Ok(ToolRunFailuresResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                groups: Vec::new(),
                diagnostics: Vec::new(),
            });
        }
        // One row per (project, tool, stage_key, extractor, version,
        // signature, run) inside the lookback, then group in Rust for
        // deterministic agent/workspace counts and newest-class picks.
        let mut sql = String::from(
            "SELECT runs.project, runs.tool_name,
                    items.stage_key, items.extractor, items.extractor_version,
                    items.signature, items.display, items.created_ts,
                    runs.run_id, runs.agent, runs.workspace,
                    items.class, items.possible_owners_json
             FROM tool_triage_items AS items
             JOIN runs ON runs.run_id = items.run_id
             WHERE items.created_ts >= ?1",
        );
        let mut args: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(cutoff)];
        if !request.all_projects {
            if let Some(project) = request.project.as_deref() {
                sql.push_str(" AND runs.project = ?2");
                args.push(Box::new(project.to_string()));
            } else {
                // Without a project filter and without all_projects, scope
                // to rows with a project (linked-project isolation: callers
                // must opt into all projects explicitly).
                sql.push_str(" AND runs.project IS NOT NULL");
            }
        } else if let Some(project) = request.project.as_deref() {
            sql.push_str(" AND runs.project = ?2");
            args.push(Box::new(project.to_string()));
        }
        if let Some(tool) = request.tool.as_deref() {
            let index = args.len() + 1;
            sql.push_str(&format!(" AND runs.tool_name = ?{index}"));
            args.push(Box::new(tool.to_string()));
        }
        sql.push_str(" ORDER BY items.created_ts DESC, items.run_id DESC");
        let arg_refs: Vec<&dyn rusqlite::ToSql> =
            args.iter().map(AsRef::as_ref).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(arg_refs.as_slice(), |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, String>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
            ))
        })?;
        #[allow(clippy::type_complexity)]
        let mut grouped: BTreeMap<
            (String, String, String, String, i64, String),
            Vec<(
                Option<String>,
                Option<String>,
                String,
                i64,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
            )>,
        > = BTreeMap::new();
        // display per group: newest display wins (rows are newest first).
        let mut displays: BTreeMap<
            (String, String, String, String, i64, String),
            String,
        > = BTreeMap::new();
        for row in rows {
            let (
                project,
                tool,
                stage_key,
                extractor,
                version,
                signature,
                display,
                created_ts,
                run_id,
                agent,
                workspace,
                class,
                owners,
            ) = row?;
            let key = (
                project.clone().unwrap_or_default(),
                tool.clone().unwrap_or_default(),
                stage_key.clone(),
                extractor.clone(),
                version,
                signature.clone(),
            );
            displays.entry(key.clone()).or_insert(display);
            grouped.entry(key).or_default().push((
                project,
                tool,
                run_id,
                created_ts,
                agent.unwrap_or_default(),
                workspace,
                class,
                owners,
                None,
            ));
        }
        let _ = TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT;
        let mut groups = Vec::new();
        for (key, rows) in grouped {
            let (project, tool, stage_key, extractor, version, signature) = key;
            let runs = rows.len() as u64;
            let agents: BTreeSet<String> = rows
                .iter()
                .map(|row| row.4.clone())
                .filter(|agent| !agent.is_empty())
                .collect();
            let workspaces: BTreeSet<String> = rows
                .iter()
                .filter_map(|row| row.5.clone())
                .filter(|workspace| !workspace.is_empty())
                .collect();
            let first_seen = rows.iter().map(|row| row.3).min();
            let last_seen = rows.iter().map(|row| row.3).max();
            // Rows are newest first, so the first row is the newest run.
            let newest = rows.first().cloned();
            let (last_run_id, newest_class, newest_owners) = match newest {
                None => (None, None, serde_json::Value::Array(Vec::new())),
                Some(row) => {
                    let owners = row
                        .7
                        .as_deref()
                        .and_then(|raw| serde_json::from_str(raw).ok())
                        .unwrap_or(serde_json::Value::Array(Vec::new()));
                    (Some(row.2), row.6, owners)
                }
            };
            if let Some(filter) = request.class.as_deref() {
                if newest_class.as_deref() != Some(filter) {
                    continue;
                }
            }
            groups.push(ToolRunFailuresGroupWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                project: if project.is_empty() {
                    None
                } else {
                    Some(project)
                },
                tool: if tool.is_empty() { None } else { Some(tool) },
                stage_key,
                extractor,
                extractor_version: version as u32,
                signature,
                display: None,
                runs,
                agents: agents.len() as u64,
                workspaces: workspaces.len() as u64,
                first_seen_ts: first_seen,
                last_seen_ts: last_seen,
                last_run_id,
                newest_class,
                newest_owners,
            });
        }
        // Fix displays (computed above by key) and sort deterministically
        // by agent count then recency, then identity for stability.
        for group in &mut groups {
            let key = (
                group.project.clone().unwrap_or_default(),
                group.tool.clone().unwrap_or_default(),
                group.stage_key.clone(),
                group.extractor.clone(),
                i64::from(group.extractor_version),
                group.signature.clone(),
            );
            group.display = displays.get(&key).cloned();
        }
        groups.sort_by(|left, right| {
            right
                .agents
                .cmp(&left.agents)
                .then(right.last_seen_ts.cmp(&left.last_seen_ts))
                .then(left.project.cmp(&right.project))
                .then(left.tool.cmp(&right.tool))
                .then(left.stage_key.cmp(&right.stage_key))
                .then(left.extractor.cmp(&right.extractor))
                .then(left.extractor_version.cmp(&right.extractor_version))
                .then(left.signature.cmp(&right.signature))
        });
        groups.truncate(request.limit as usize);
        Ok(ToolRunFailuresResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            groups,
            diagnostics: Vec::new(),
        })
    })
}

//! Merge-on-write per-run demand records.
//!
//! Demand is metadata: [`record_demand`] is allowed in any run state and
//! never fails a request for an invalid grant. Invalid grants are dropped
//! with a diagnostic naming the `grant_id`; the rest of the request lands.

use super::super::demand_wire::{
    ToolRunDemandWire, ToolRunRecordDemandRequestWire,
    ToolRunRecordDemandResultWire, ToolRunWorkerGrantWire,
    DEMAND_MAX_DIAGNOSTICS, DEMAND_MAX_WORKER_GRANTS,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::connection::with_write_store;
use super::connection::{touch_write_meta, unix_now, validate_schema};
use super::query::load_run;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

const GRANT_ID_MAX_CHARS: usize = 64;

fn grant_field_invalid(value: &str) -> bool {
    value.is_empty() || value.chars().count() > GRANT_ID_MAX_CHARS
}

fn grant_display_id(grant: &ToolRunWorkerGrantWire) -> String {
    if grant.grant_id.is_empty() {
        "<empty grant_id>".to_string()
    } else {
        grant.grant_id.clone()
    }
}

fn valid_grant(grant: &ToolRunWorkerGrantWire) -> bool {
    if grant_field_invalid(&grant.grant_id)
        || grant_field_invalid(&grant.source)
        || grant_field_invalid(&grant.path)
    {
        return false;
    }
    if let Some(lane) = grant.lane.as_deref() {
        if grant_field_invalid(lane) {
            return false;
        }
    }
    if grant.requested_floor > grant.requested_ceiling {
        return false;
    }
    true
}

fn push_unique(capped: &mut Vec<String>, value: String) {
    if !capped.contains(&value) {
        capped.push(value);
    }
}

fn stored_demand(
    raw: Option<String>,
) -> (Option<ToolRunDemandWire>, Option<String>) {
    let Some(raw) = raw else {
        return (None, None);
    };
    match serde_json::from_str::<ToolRunDemandWire>(&raw) {
        Ok(demand) => (Some(demand), None),
        Err(_) => (None, Some("stored demand was unreadable".to_string())),
    }
}

pub fn record_demand(
    store_path: &Path,
    request: ToolRunRecordDemandRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunRecordDemandResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: Option<String> = tx
            .query_row(
                "SELECT run_id FROM runs WHERE run_id = ?1",
                [&request.run_id],
                |row| row.get(0),
            )
            .optional()?;
        if exists.is_none() {
            return Err(ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            });
        }
        // The write connection already ran `ensure_child_observation_columns`,
        // so a legacy store gains `demand_json` before this read.
        let stored: Option<String> = tx
            .query_row(
                "SELECT demand_json FROM runs WHERE run_id = ?1",
                [&request.run_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let (parsed, unreadable) = stored_demand(stored);
        let mut demand = parsed.unwrap_or(ToolRunDemandWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            context: None,
            usage: None,
            worker_grants: Vec::new(),
            diagnostics: Vec::new(),
        });
        let before = demand.clone();
        let mut call_diagnostics = Vec::new();
        if let Some(diagnostic) = unreadable {
            push_unique(&mut demand.diagnostics, diagnostic);
        }
        if let Some(context) = request.context.clone() {
            demand.context = Some(context);
        }
        if let Some(usage) = request.usage.clone() {
            demand.usage = Some(usage);
        }
        for grant in &request.worker_grants {
            if !valid_grant(grant) {
                let diagnostic = format!(
                    "dropped invalid worker grant {}",
                    grant_display_id(grant)
                );
                push_unique(&mut demand.diagnostics, diagnostic.clone());
                push_unique(&mut call_diagnostics, diagnostic);
                continue;
            }
            if demand
                .worker_grants
                .iter()
                .any(|stored| stored.grant_id == grant.grant_id)
            {
                continue;
            }
            if demand.worker_grants.len() >= DEMAND_MAX_WORKER_GRANTS {
                let diagnostic = format!(
                    "demand worker grants truncated to {DEMAND_MAX_WORKER_GRANTS}"
                );
                push_unique(&mut demand.diagnostics, diagnostic.clone());
                push_unique(&mut call_diagnostics, diagnostic);
                break;
            }
            demand.worker_grants.push(grant.clone());
        }
        for diagnostic in &request.diagnostics {
            push_unique(&mut demand.diagnostics, diagnostic.clone());
        }
        if demand.diagnostics.len() > DEMAND_MAX_DIAGNOSTICS {
            demand.diagnostics.truncate(DEMAND_MAX_DIAGNOSTICS);
            let overflow = format!(
                "demand diagnostics truncated to {DEMAND_MAX_DIAGNOSTICS}"
            );
            if let Some(last) = demand.diagnostics.last_mut() {
                *last = overflow;
            }
        }
        let replayed = demand == before && call_diagnostics.is_empty();
        if !replayed {
            let json = serde_json::to_string(&demand)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            tx.execute(
                "UPDATE runs SET demand_json = ?2 WHERE run_id = ?1",
                params![request.run_id, json],
            )?;
            touch_write_meta(&tx, unix_now())?;
        }
        let run = load_run(&tx, &request.run_id)?.expect("recorded run");
        let stored_demand = run.demand.clone().unwrap_or(ToolRunDemandWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            context: None,
            usage: None,
            worker_grants: Vec::new(),
            diagnostics: Vec::new(),
        });
        tx.commit()?;
        Ok(ToolRunRecordDemandResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            demand: stored_demand,
            replayed,
            diagnostics: call_diagnostics,
        })
    })
}

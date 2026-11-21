//! Host-wide named-tool admission over the ToolRun ledger.
//!
//! The queue is the set of unsettled runs whose `demand.admission` record
//! carries a key: holders say `admitted`, waiters say `waiting` and order
//! FIFO by `(requested_ts_ms, seq)`. Settlement by any path releases a slot
//! because settled rows leave the unsettled set, so there is no release
//! call. `enter`, `poll`, and `force` each decide inside one
//! `BEGIN IMMEDIATE` transaction, so concurrent pollers never grant more
//! than the limit. `poll` first evaluates on a read connection and takes
//! the write lock only when the run may be eligible, so idle waiters do not
//! serialize the store.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Deserialize;

use super::super::admission_wire::{
    ToolRunAdmissionEnterRequestWire, ToolRunAdmissionForceReasonWire,
    ToolRunAdmissionForceRequestWire, ToolRunAdmissionOutcomeWire,
    ToolRunAdmissionPeerWire, ToolRunAdmissionPollRequestWire,
    ToolRunAdmissionResultWire, ToolRunAdmissionStateWire,
    ToolRunAdmissionViewWire, ToolRunAdmissionWire, ToolRunAdmittedByWire,
    TOOL_RUN_ADMISSION_KEY_MAX_CHARS, TOOL_RUN_ADMISSION_RECORD_SCHEMA_VERSION,
};
use super::super::demand_wire::ToolRunDemandWire;
use super::super::wire::{ToolRunStateWire, TOOL_RUN_WIRE_SCHEMA_VERSION};
use super::super::ToolRunError;
use super::connection::{
    runs_column_set, touch_write_meta, unix_now_ms, validate_schema,
    with_read_store, with_write_store,
};

/// `meta` key holding the last assigned admission `seq`.
const ADMISSION_SEQ_META_KEY: &str = "admission_seq";

/// Lenient view of `demand_json` that reads only the admission record, so
/// an unrelated unreadable demand field never hides a queue entry.
#[derive(Deserialize)]
struct AdmissionOnly {
    #[serde(default)]
    admission: Option<ToolRunAdmissionWire>,
}

fn parse_admission(raw: Option<&str>) -> Option<ToolRunAdmissionWire> {
    let raw = raw?;
    if !raw.contains("\"admission\"") {
        return None;
    }
    serde_json::from_str::<AdmissionOnly>(raw)
        .ok()
        .and_then(|only| only.admission)
}

/// One unsettled run carrying an admission record.
struct QueueEntry {
    record: ToolRunAdmissionWire,
    peer: ToolRunAdmissionPeerWire,
}

/// The holder/waiter query. It reads only the unsettled set through the
/// `(state, created_ts)` index and never scans settled history; a test pins
/// the query plan.
pub(super) fn queue_sql(launch_mode_projection: &str) -> String {
    format!(
        "SELECT run_id, project, tool_name, agent, owner_kind, owner_id,
                wrapper_pid, boot_id, process_start_identity,
                {launch_mode_projection}, demand_json
         FROM runs
         WHERE state IN ('created', 'running')
           AND demand_json IS NOT NULL
           AND instr(demand_json, '\"admission\"') > 0"
    )
}

fn load_queue(conn: &Connection) -> Result<Vec<QueueEntry>, ToolRunError> {
    let columns = runs_column_set(conn)?;
    // A read-only open of a store written before `demand_json` existed
    // has no admission records at all.
    if !columns.contains("demand_json") {
        return Ok(Vec::new());
    }
    let launch_mode = if columns.contains("launch_mode") {
        "launch_mode"
    } else {
        "NULL"
    };
    let mut stmt = conn.prepare(&queue_sql(launch_mode))?;
    let mut rows = stmt.query([])?;
    let mut entries = Vec::new();
    while let Some(row) = rows.next()? {
        let raw: Option<String> = row.get(10)?;
        let Some(record) = parse_admission(raw.as_deref()) else {
            continue;
        };
        if record.state == ToolRunAdmissionStateWire::Abandoned {
            continue;
        }
        entries.push(QueueEntry {
            peer: ToolRunAdmissionPeerWire {
                run_id: row.get(0)?,
                project: row.get(1)?,
                tool_name: row.get(2)?,
                agent: row.get(3)?,
                owner_kind: row.get(4)?,
                owner_id: row.get(5)?,
                wrapper_pid: row.get(6)?,
                boot_id: row.get(7)?,
                process_start_identity: row.get(8)?,
                launch_mode: row.get(9)?,
                seq: record.seq,
                requested_ts_ms: record.requested_ts_ms,
                admitted_ts_ms: record.admitted_ts_ms,
                admitted_by: record.admitted_by,
            },
            record,
        });
    }
    Ok(entries)
}

fn fifo_order(left: &QueueEntry, right: &QueueEntry) -> Ordering {
    (
        left.record.requested_ts_ms,
        left.record.seq,
        &left.peer.run_id,
    )
        .cmp(&(
            right.record.requested_ts_ms,
            right.record.seq,
            &right.peer.run_id,
        ))
}

/// One key's holders and FIFO-ordered waiters.
struct KeyQueue<'a> {
    holders: Vec<&'a QueueEntry>,
    waiters: Vec<&'a QueueEntry>,
}

impl<'a> KeyQueue<'a> {
    fn of(entries: &'a [QueueEntry], key: &str) -> Self {
        let mut holders = Vec::new();
        let mut waiters = Vec::new();
        for entry in entries.iter().filter(|entry| entry.record.key == key) {
            match entry.record.state {
                ToolRunAdmissionStateWire::Admitted => holders.push(entry),
                ToolRunAdmissionStateWire::Waiting => waiters.push(entry),
                ToolRunAdmissionStateWire::Abandoned => {}
            }
        }
        holders.sort_by(|left, right| fifo_order(left, right));
        waiters.sort_by(|left, right| fifo_order(left, right));
        Self { holders, waiters }
    }

    fn waiter_index(&self, run_id: &str) -> Option<usize> {
        self.waiters
            .iter()
            .position(|entry| entry.peer.run_id == run_id)
    }
}

/// Grant iff `H < limit` and `i < limit - H`, where `H` counts holders and
/// `i` is the waiter's FIFO index. Never grants more than the limit.
fn eligible(holders: usize, index: usize, limit: u32) -> bool {
    let limit = limit as usize;
    holders < limit && index < limit - holders
}

/// Waiters ahead plus holders over the limit.
fn ahead_count(holders: usize, index: usize, limit: u32) -> u32 {
    let over = holders.saturating_sub(limit as usize);
    u32::try_from(index + over).unwrap_or(u32::MAX)
}

fn validate_run_id(run_id: &str) -> Result<(), ToolRunError> {
    if run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<(), ToolRunError> {
    if key.trim().is_empty() {
        return Err(ToolRunError::invalid("admission key must not be empty"));
    }
    if key.chars().count() > TOOL_RUN_ADMISSION_KEY_MAX_CHARS {
        return Err(ToolRunError::invalid(format!(
            "admission key must be at most \
             {TOOL_RUN_ADMISSION_KEY_MAX_CHARS} characters"
        )));
    }
    if key.chars().any(char::is_control) {
        return Err(ToolRunError::invalid(
            "admission key must not contain control characters",
        ));
    }
    Ok(())
}

/// The polled run's own state and admission record.
struct TargetRun {
    state: ToolRunStateWire,
    record: Option<ToolRunAdmissionWire>,
}

fn load_target(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<TargetRun>, ToolRunError> {
    let columns = runs_column_set(conn)?;
    let demand = if columns.contains("demand_json") {
        "demand_json"
    } else {
        "NULL"
    };
    let row: Option<(String, Option<String>)> = conn
        .query_row(
            &format!("SELECT state, {demand} FROM runs WHERE run_id = ?1"),
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((state, raw)) = row else {
        return Ok(None);
    };
    Ok(Some(TargetRun {
        state: ToolRunStateWire::from_db(&state)
            .map_err(ToolRunError::store)?,
        record: parse_admission(raw.as_deref()),
    }))
}

fn empty_demand() -> ToolRunDemandWire {
    ToolRunDemandWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        context: None,
        usage: None,
        worker_grants: Vec::new(),
        diagnostics: Vec::new(),
        admission: None,
    }
}

/// Replace `demand.admission` and keep every other demand field. An
/// unreadable stored demand is replaced, with a diagnostic, exactly as
/// `tool_run_record_demand` does.
fn write_record(
    conn: &Connection,
    run_id: &str,
    record: &ToolRunAdmissionWire,
) -> Result<Vec<String>, ToolRunError> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT demand_json FROM runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let mut diagnostics = Vec::new();
    let mut demand = match raw.as_deref() {
        None => empty_demand(),
        Some(raw) if raw.trim() == "null" => empty_demand(),
        Some(raw) => match serde_json::from_str::<ToolRunDemandWire>(raw) {
            Ok(demand) => demand,
            Err(_) => {
                let diagnostic = "stored demand was unreadable".to_string();
                let mut fresh = empty_demand();
                fresh.diagnostics.push(diagnostic.clone());
                diagnostics.push(diagnostic);
                fresh
            }
        },
    };
    demand.admission = Some(record.clone());
    let json = serde_json::to_string(&demand)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    conn.execute(
        "UPDATE runs SET demand_json = ?2 WHERE run_id = ?1",
        params![run_id, json],
    )?;
    Ok(diagnostics)
}

fn next_seq(conn: &Connection) -> Result<i64, ToolRunError> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [ADMISSION_SEQ_META_KEY],
            |row| row.get(0),
        )
        .optional()?;
    let next = raw
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0)
        .saturating_add(1);
    conn.execute(
        "INSERT INTO meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![ADMISSION_SEQ_META_KEY, next.to_string()],
    )?;
    Ok(next)
}

fn admitted(
    mut record: ToolRunAdmissionWire,
    by: ToolRunAdmittedByWire,
    now_ms: i64,
) -> ToolRunAdmissionWire {
    record.state = ToolRunAdmissionStateWire::Admitted;
    record.admitted_by = Some(by);
    record.admitted_ts_ms = Some(now_ms);
    record.wait_ms =
        Some(now_ms.saturating_sub(record.requested_ts_ms).max(0) as u64);
    record
}

/// Grant the run a slot when it is waiting and eligible. Returns the
/// (possibly updated) record and whether it was written.
fn maybe_grant(
    conn: &Connection,
    run_id: &str,
    record: ToolRunAdmissionWire,
    now_ms: i64,
) -> Result<(ToolRunAdmissionWire, bool), ToolRunError> {
    if record.state != ToolRunAdmissionStateWire::Waiting {
        return Ok((record, false));
    }
    let entries = load_queue(conn)?;
    let queue = KeyQueue::of(&entries, &record.key);
    let Some(index) = queue.waiter_index(run_id) else {
        return Ok((record, false));
    };
    if !eligible(queue.holders.len(), index, record.limit) {
        return Ok((record, false));
    }
    let granted = admitted(record, ToolRunAdmittedByWire::Slot, now_ms);
    write_record(conn, run_id, &granted)?;
    Ok((granted, true))
}

fn bare_result(
    run_id: &str,
    outcome: ToolRunAdmissionOutcomeWire,
    record: Option<&ToolRunAdmissionWire>,
) -> ToolRunAdmissionResultWire {
    ToolRunAdmissionResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: run_id.to_string(),
        outcome,
        key: record.map(|record| record.key.clone()),
        limit: record.map(|record| record.limit),
        ahead: 0,
        holders: Vec::new(),
        waiters_ahead: Vec::new(),
        admission: record.cloned(),
        changed: false,
        diagnostics: Vec::new(),
    }
}

/// The queue picture for an unsettled run that carries a record.
fn queue_result(
    conn: &Connection,
    run_id: &str,
    record: &ToolRunAdmissionWire,
) -> Result<ToolRunAdmissionResultWire, ToolRunError> {
    let outcome = match record.state {
        ToolRunAdmissionStateWire::Admitted => {
            ToolRunAdmissionOutcomeWire::Admitted
        }
        ToolRunAdmissionStateWire::Waiting => {
            ToolRunAdmissionOutcomeWire::Waiting
        }
        ToolRunAdmissionStateWire::Abandoned => {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::Settled,
                Some(record),
            ));
        }
    };
    let entries = load_queue(conn)?;
    let queue = KeyQueue::of(&entries, &record.key);
    let mut result = bare_result(run_id, outcome, Some(record));
    result.holders = queue
        .holders
        .iter()
        .map(|entry| entry.peer.clone())
        .collect();
    if let Some(index) = queue.waiter_index(run_id) {
        result.waiters_ahead = queue.waiters[..index]
            .iter()
            .map(|entry| entry.peer.clone())
            .collect();
        result.ahead = ahead_count(queue.holders.len(), index, record.limit);
    }
    Ok(result)
}

fn missing_record(run_id: &str) -> ToolRunError {
    ToolRunError::invalid(format!(
        "tool run {run_id} has no admission record; call \
         tool_run_admission_enter first"
    ))
}

/// Record a `waiting` admission for a running run, then evaluate it as
/// `poll` does. Idempotent per run: a second enter keeps the first record
/// (its key, limit, seq, and request time) and only re-evaluates.
pub fn admission_enter(
    store_path: &Path,
    request: ToolRunAdmissionEnterRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunAdmissionResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_run_id(&request.run_id)?;
    validate_key(&request.key)?;
    if request.limit == 0 {
        return Err(ToolRunError::invalid(
            "admission limit must be at least 1",
        ));
    }
    let now_ms = request.now_ms.unwrap_or_else(unix_now_ms);
    let run_id = request.run_id.as_str();
    if !store_path.exists() {
        return Ok(bare_result(
            run_id,
            ToolRunAdmissionOutcomeWire::NotFound,
            None,
        ));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(target) = load_target(&tx, run_id)? else {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::NotFound,
                None,
            ));
        };
        if target.state.is_terminal() {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::Settled,
                target.record.as_ref(),
            ));
        }
        if target.state != ToolRunStateWire::Running {
            return Err(ToolRunError::invalid(format!(
                "tool run {run_id} is {}; admission requires a running run",
                target.state.as_str()
            )));
        }
        let mut changed = false;
        let mut diagnostics = Vec::new();
        let record = match target.record {
            Some(record) => {
                if record.key != request.key || record.limit != request.limit {
                    diagnostics.push(format!(
                        "admission already recorded under key {} limit {}; \
                         kept the first record",
                        record.key, record.limit
                    ));
                }
                record
            }
            None => {
                let seq = next_seq(&tx)?;
                let entries = load_queue(&tx)?;
                let queue = KeyQueue::of(&entries, &request.key);
                let index = queue
                    .waiters
                    .iter()
                    .filter(|entry| {
                        (entry.record.requested_ts_ms, entry.record.seq)
                            < (now_ms, seq)
                    })
                    .count();
                let record = ToolRunAdmissionWire {
                    schema_version: TOOL_RUN_ADMISSION_RECORD_SCHEMA_VERSION,
                    key: request.key.clone(),
                    limit: request.limit,
                    state: ToolRunAdmissionStateWire::Waiting,
                    admitted_by: None,
                    seq,
                    requested_ts_ms: now_ms,
                    admitted_ts_ms: None,
                    wait_ms: None,
                    ahead_at_request: ahead_count(
                        queue.holders.len(),
                        index,
                        request.limit,
                    ),
                };
                diagnostics.extend(write_record(&tx, run_id, &record)?);
                changed = true;
                record
            }
        };
        let (record, granted) = maybe_grant(&tx, run_id, record, now_ms)?;
        changed |= granted;
        let mut result = queue_result(&tx, run_id, &record)?;
        result.changed = changed;
        result.diagnostics = diagnostics;
        if changed {
            touch_write_meta(&tx, now_ms.div_euclid(1000))?;
            tx.commit()?;
        }
        Ok(result)
    })
}

/// Outcome of the read-only pre-check.
enum Precheck {
    Decided(Box<ToolRunAdmissionResultWire>),
    MayGrant,
}

fn precheck(conn: &Connection, run_id: &str) -> Result<Precheck, ToolRunError> {
    let Some(target) = load_target(conn, run_id)? else {
        return Ok(Precheck::Decided(Box::new(bare_result(
            run_id,
            ToolRunAdmissionOutcomeWire::NotFound,
            None,
        ))));
    };
    if target.state.is_terminal() {
        return Ok(Precheck::Decided(Box::new(bare_result(
            run_id,
            ToolRunAdmissionOutcomeWire::Settled,
            target.record.as_ref(),
        ))));
    }
    let Some(record) = target.record else {
        return Err(missing_record(run_id));
    };
    if record.state == ToolRunAdmissionStateWire::Waiting {
        let entries = load_queue(conn)?;
        let queue = KeyQueue::of(&entries, &record.key);
        if let Some(index) = queue.waiter_index(run_id) {
            if eligible(queue.holders.len(), index, record.limit) {
                return Ok(Precheck::MayGrant);
            }
        }
    }
    Ok(Precheck::Decided(Box::new(queue_result(
        conn, run_id, &record,
    )?)))
}

/// Report the run's queue position, granting a free slot when the run is
/// eligible. A waiting run that cannot be admitted is answered from a read
/// connection without taking the write lock.
pub fn admission_poll(
    store_path: &Path,
    request: ToolRunAdmissionPollRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunAdmissionResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_run_id(&request.run_id)?;
    let run_id = request.run_id.as_str();
    if !store_path.exists() {
        return Ok(bare_result(
            run_id,
            ToolRunAdmissionOutcomeWire::NotFound,
            None,
        ));
    }
    let now_ms = request.now_ms.unwrap_or_else(unix_now_ms);
    match with_read_store(store_path, busy_timeout, |conn| {
        precheck(conn, run_id)
    }) {
        Ok(Precheck::Decided(result)) => return Ok(*result),
        Ok(Precheck::MayGrant) | Err(ToolRunError::ReadOnly { .. }) => {}
        Err(error) => return Err(error),
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(target) = load_target(&tx, run_id)? else {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::NotFound,
                None,
            ));
        };
        if target.state.is_terminal() {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::Settled,
                target.record.as_ref(),
            ));
        }
        let Some(record) = target.record else {
            return Err(missing_record(run_id));
        };
        let (record, granted) = maybe_grant(&tx, run_id, record, now_ms)?;
        let mut result = queue_result(&tx, run_id, &record)?;
        result.changed = granted;
        if granted {
            touch_write_meta(&tx, now_ms.div_euclid(1000))?;
            tx.commit()?;
        }
        Ok(result)
    })
}

/// Admit a waiting run regardless of the limit. The forced run counts as a
/// holder like any other until it settles. Idempotent for admitted runs.
pub fn admission_force(
    store_path: &Path,
    request: ToolRunAdmissionForceRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunAdmissionResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_run_id(&request.run_id)?;
    let by = match request.reason {
        ToolRunAdmissionForceReasonWire::MaxWait => {
            ToolRunAdmittedByWire::MaxWait
        }
    };
    let now_ms = request.now_ms.unwrap_or_else(unix_now_ms);
    let run_id = request.run_id.as_str();
    if !store_path.exists() {
        return Ok(bare_result(
            run_id,
            ToolRunAdmissionOutcomeWire::NotFound,
            None,
        ));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(target) = load_target(&tx, run_id)? else {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::NotFound,
                None,
            ));
        };
        if target.state.is_terminal() {
            return Ok(bare_result(
                run_id,
                ToolRunAdmissionOutcomeWire::Settled,
                target.record.as_ref(),
            ));
        }
        let Some(record) = target.record else {
            return Err(missing_record(run_id));
        };
        if record.state != ToolRunAdmissionStateWire::Waiting {
            return queue_result(&tx, run_id, &record);
        }
        let forced = admitted(record, by, now_ms);
        let diagnostics = write_record(&tx, run_id, &forced)?;
        let mut result = queue_result(&tx, run_id, &forced)?;
        result.changed = true;
        result.diagnostics = diagnostics;
        touch_write_meta(&tx, now_ms.div_euclid(1000))?;
        tx.commit()?;
        Ok(result)
    })
}

/// Milliseconds for a settlement stamped `event_ts` seconds. An event
/// stamped from the live clock settles at millisecond precision; an
/// injected (test or replayed) second stamp is honored as given.
fn settle_clock_ms(event_ts: i64) -> i64 {
    let now_ms = unix_now_ms();
    if (now_ms.div_euclid(1000) - event_ts).abs() <= 1 {
        now_ms
    } else {
        event_ts.saturating_mul(1000)
    }
}

/// Finalize a still-`waiting` record as `abandoned` inside the settling
/// transaction (finish, reconcile, or any other terminal event). An
/// unreadable stored demand is left untouched.
pub(super) fn abandon_waiting_admission(
    conn: &Connection,
    run_id: &str,
    settled_ts: i64,
) -> Result<(), ToolRunError> {
    let columns = runs_column_set(conn)?;
    if !columns.contains("demand_json") {
        return Ok(());
    }
    let raw: Option<String> = conn
        .query_row(
            "SELECT demand_json FROM runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?
        .flatten();
    let Some(raw) = raw else {
        return Ok(());
    };
    if !raw.contains("\"admission\"") {
        return Ok(());
    }
    let Ok(mut demand) = serde_json::from_str::<ToolRunDemandWire>(&raw) else {
        return Ok(());
    };
    let Some(record) = demand.admission.as_mut() else {
        return Ok(());
    };
    if record.state != ToolRunAdmissionStateWire::Waiting {
        return Ok(());
    }
    let settled_ms = settle_clock_ms(settled_ts);
    record.state = ToolRunAdmissionStateWire::Abandoned;
    record.wait_ms =
        Some(settled_ms.saturating_sub(record.requested_ts_ms).max(0) as u64);
    let json = serde_json::to_string(&demand)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    conn.execute(
        "UPDATE runs SET demand_json = ?2 WHERE run_id = ?1",
        params![run_id, json],
    )?;
    Ok(())
}

/// Live admission projections for every unsettled run with a record,
/// computed from one queue read.
pub(crate) struct AdmissionViews {
    views: HashMap<String, ToolRunAdmissionViewWire>,
}

impl AdmissionViews {
    pub(crate) fn get(&self, run_id: &str) -> Option<ToolRunAdmissionViewWire> {
        self.views.get(run_id).cloned()
    }
}

pub(crate) fn admission_views(
    conn: &Connection,
) -> Result<AdmissionViews, ToolRunError> {
    let entries = load_queue(conn)?;
    let mut keys: Vec<&str> = entries
        .iter()
        .map(|entry| entry.record.key.as_str())
        .collect();
    keys.sort_unstable();
    keys.dedup();
    let mut views = HashMap::with_capacity(entries.len());
    for key in keys {
        let queue = KeyQueue::of(&entries, key);
        let holders = queue.holders.len();
        let holders_u32 = u32::try_from(holders).unwrap_or(u32::MAX);
        for entry in &queue.holders {
            views.insert(
                entry.peer.run_id.clone(),
                view_for(entry, 0, holders_u32),
            );
        }
        for (index, entry) in queue.waiters.iter().enumerate() {
            let ahead = ahead_count(holders, index, entry.record.limit);
            views.insert(
                entry.peer.run_id.clone(),
                view_for(entry, ahead, holders_u32),
            );
        }
    }
    Ok(AdmissionViews { views })
}

fn view_for(
    entry: &QueueEntry,
    ahead: u32,
    holders: u32,
) -> ToolRunAdmissionViewWire {
    ToolRunAdmissionViewWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        state: entry.record.state,
        key: entry.record.key.clone(),
        limit: entry.record.limit,
        ahead,
        holders,
        requested_ts_ms: entry.record.requested_ts_ms,
        admitted_ts_ms: entry.record.admitted_ts_ms,
        admitted_by: entry.record.admitted_by,
    }
}

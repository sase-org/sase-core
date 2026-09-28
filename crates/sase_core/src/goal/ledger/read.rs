//! Goal ledger reads: the O(unsettled) hot read, single-goal show,
//! and the explicit history scan.
//!
//! The hot read ([`goal_ledger_list`]) opens `STORE.json`, `live/`, and
//! only the live goals' `items/<id>/events/*`. Settled goals'
//! directories are never opened; the I/O probe proves it.

use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use super::super::reduce::reduce_goal_events;
use super::super::wire::{
    GoalEventWire, GoalStateWire, GOAL_WIRE_SCHEMA_VERSION,
};
use super::layout::{
    goal_events_dir, goal_items_dir, goal_live_dir, goal_now_rfc3339,
    read_goal_store, GoalLedgerError,
};
use super::probe::GoalLedgerProbeCountsWire;

/// Reborrow a probe slot for one nested call.
fn reborrow<'a>(
    probe: &'a mut Option<&mut GoalLedgerProbeCountsWire>,
) -> Option<&'a mut GoalLedgerProbeCountsWire> {
    probe.as_mut().map(|slot| &mut **slot)
}

/// Filter for [`goal_ledger_list`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalListFilterWire {
    /// Status filter word: `unsettled`, `active`, `review`, `draft`,
    /// `done`, `dropped`, `settled`, or `all`. Defaults to `unsettled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Maximum goals returned. Defaults to no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// The hot-read result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalListWire {
    /// State wire schema version.
    #[serde(default)]
    pub schema_version: u32,
    /// Owning project (from the first reduced goal, else `""`).
    #[serde(default)]
    pub project: String,
    /// RFC3339 generation time.
    #[serde(default)]
    pub generated_at: String,
    /// Marked goals that reduced to a settled state.
    #[serde(default)]
    pub stale_markers: u64,
    /// Reduced goals in stable lane order.
    #[serde(default)]
    pub goals: Vec<GoalStateWire>,
}

/// Filter for [`goal_ledger_history`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalHistoryFilterWire {
    /// `done`, `dropped`, `settled`, or `all`. Defaults to `settled`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Maximum goals returned. Defaults to 20.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

/// Read and parse every event file for one goal, in filename order.
///
/// Unknown fields are ignored by the envelope; files that fail to parse
/// are skipped with an `unparseable_event` diagnostic contribution left
/// to the reducer's unreadable marking via a synthetic record. In
/// practice the writer only emits well-formed events, so a corrupt file
/// surfaces here as data error detail instead.
pub fn read_goal_events(
    root: &Path,
    goal_id: &str,
    mut probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<Vec<GoalEventWire>, GoalLedgerError> {
    let dir = goal_events_dir(root, goal_id);
    if let Some(counts) = probe.as_mut() {
        counts.event_dir_stats += 1;
    }
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                dir.display()
            )));
        }
    };
    let mut names: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to list {}: {error}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            names.push(stem.to_string());
        }
    }
    names.sort();
    let mut events = Vec::with_capacity(names.len());
    for name in names {
        let path = dir.join(format!("{name}.json"));
        if let Some(counts) = probe.as_mut() {
            counts.event_files_opened += 1;
        }
        let contents = fs::read_to_string(&path).map_err(|error| {
            if error.kind() == ErrorKind::NotFound {
                // Raced with a concurrent repair; skip it.
                return GoalLedgerError::Io(format!(
                    "vanished during read: {}",
                    path.display()
                ));
            }
            GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                path.display()
            ))
        });
        let contents = match contents {
            Ok(contents) => contents,
            Err(error) if error.to_string().starts_with("vanished") => {
                continue;
            }
            Err(error) => return Err(error),
        };
        let event: GoalEventWire =
            serde_json::from_str(&contents).map_err(|error| {
                GoalLedgerError::Data(format!(
                    "unparseable event {}: {error}",
                    path.display()
                ))
            })?;
        events.push(event);
    }
    Ok(events)
}

/// Reduce one goal's current events to its state.
pub fn reduce_goal(
    root: &Path,
    goal_id: &str,
    probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<GoalStateWire, GoalLedgerError> {
    let events = read_goal_events(root, goal_id, probe)?;
    Ok(reduce_goal_events(goal_id, &events))
}

/// List the live marker ids in sorted order.
pub fn read_live_markers(
    root: &Path,
    mut probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<Vec<String>, GoalLedgerError> {
    let dir = goal_live_dir(root);
    if let Some(counts) = probe.as_mut() {
        counts.live_dir_reads += 1;
    }
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(Vec::new());
        }
        Err(error) => {
            return Err(GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                dir.display()
            )));
        }
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to list {}: {error}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        let is_file = entry
            .file_type()
            .map(|kind| kind.is_file() || kind.is_symlink())
            .unwrap_or(true);
        if !is_file || !path.is_file() {
            continue;
        }
        if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
            if !name.starts_with('.') {
                ids.push(name.to_string());
            }
        }
    }
    ids.sort();
    Ok(ids)
}

/// Lane rank for stable list ordering: review, active, draft, then rest.
fn lane_rank(state: &GoalStateWire) -> u8 {
    match state.status.as_str() {
        "review" => 0,
        "active" => 1,
        "draft" => 2,
        _ => 3,
    }
}

/// Stable row order: lane, then most recent activity, then id.
fn sort_states(states: &mut [GoalStateWire]) {
    states.sort_by(|a, b| {
        lane_rank(a)
            .cmp(&lane_rank(b))
            .then_with(|| b.updated_at.cmp(&a.updated_at))
            .then_with(|| a.id.cmp(&b.id))
    });
}

fn matches_list_filter(state: &GoalStateWire, word: &str) -> bool {
    match word {
        "all" => true,
        "unsettled" => state.status.is_unsettled(),
        "settled" => state.status.is_settled(),
        other => state.status.as_str() == other,
    }
}

/// The hot read: reduce every marked goal, omit settled ones, and count
/// `stale_marker`s. Settled goals' directories are never opened.
pub fn goal_ledger_list(
    root: &Path,
    filter: &GoalListFilterWire,
    probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<GoalListWire, GoalLedgerError> {
    goal_ledger_list_inner(root, filter, probe)
}

fn goal_ledger_list_inner(
    root: &Path,
    filter: &GoalListFilterWire,
    mut probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<GoalListWire, GoalLedgerError> {
    read_goal_store(root)?;
    let word = filter.status.as_deref().unwrap_or("unsettled");
    let markers = read_live_markers(root, reborrow(&mut probe))?;
    let mut states = Vec::new();
    let mut stale_markers: u64 = 0;
    let mut project = String::new();
    for id in markers {
        let state = reduce_goal(root, &id, reborrow(&mut probe))?;
        if state.status.is_settled() {
            stale_markers += 1;
            if word == "all"
                || word == "settled"
                || word == state.status.as_str()
            {
                states.push(state);
            }
            continue;
        }
        if project.is_empty() {
            project = state.project.clone();
        }
        if matches_list_filter(&state, word) {
            states.push(state);
        }
    }
    sort_states(&mut states);
    if let Some(limit) = filter.limit {
        states.truncate(limit);
    }
    let now: chrono::DateTime<Utc> = chrono::Utc::now();
    Ok(GoalListWire {
        schema_version: GOAL_WIRE_SCHEMA_VERSION,
        project,
        generated_at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
        stale_markers,
        goals: states,
    })
}

/// Show one goal: O(events of one goal).
pub fn goal_ledger_show(
    root: &Path,
    goal_id: &str,
    probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<GoalStateWire, GoalLedgerError> {
    use super::super::ids::parse_goal_id;
    read_goal_store(root)?;
    let parsed = parse_goal_id(goal_id)
        .map_err(|error| GoalLedgerError::InvalidId(error.to_string()))?;
    let events = read_goal_events(root, &parsed, probe)?;
    if events.is_empty() {
        return Err(GoalLedgerError::UnknownGoal(parsed));
    }
    Ok(reduce_goal_events(&parsed, &events))
}

/// Explicit history scan for `done`/`dropped`/`settled`/`all`, newest
/// first. Documented as outside the O(unsettled) contract: it opens
/// every goal's events directory.
pub fn goal_ledger_history(
    root: &Path,
    filter: &GoalHistoryFilterWire,
    mut probe: Option<&mut GoalLedgerProbeCountsWire>,
) -> Result<GoalListWire, GoalLedgerError> {
    read_goal_store(root)?;
    let word = filter.status.as_deref().unwrap_or("settled");
    let limit = filter.limit.unwrap_or(20);
    let dir = goal_items_dir(root);
    if let Some(counts) = probe.as_mut() {
        counts.history_scan = true;
    }
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let now: chrono::DateTime<Utc> = chrono::Utc::now();
            return Ok(GoalListWire {
                schema_version: GOAL_WIRE_SCHEMA_VERSION,
                project: String::new(),
                generated_at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
                stale_markers: 0,
                goals: Vec::new(),
            });
        }
        Err(error) => {
            return Err(GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                dir.display()
            )));
        }
    };
    let mut ids: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to list {}: {error}",
                dir.display()
            ))
        })?;
        if entry.path().join("events").is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                ids.push(name.to_string());
            }
        }
    }
    let mut states = Vec::new();
    let mut project = String::new();
    for id in ids {
        let state = reduce_goal(root, &id, reborrow(&mut probe))?;
        if project.is_empty() {
            project = state.project.clone();
        }
        let keep = match word {
            "all" => true,
            "settled" => state.status.is_settled(),
            other => state.status.as_str() == other,
        };
        if keep {
            states.push(state);
        }
    }
    states.sort_by(|a, b| {
        b.updated_at
            .cmp(&a.updated_at)
            .then_with(|| a.id.cmp(&b.id))
    });
    states.truncate(limit);
    Ok(GoalListWire {
        schema_version: GOAL_WIRE_SCHEMA_VERSION,
        project,
        generated_at: goal_now_rfc3339(),
        stale_markers: 0,
        goals: states,
    })
}

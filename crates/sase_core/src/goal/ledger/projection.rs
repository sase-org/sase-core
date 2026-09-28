//! Goal hot projection: the machine-local `goals-hot.json` cache.
//!
//! ```json
//! {"schema_version":1,"project":"sase","mode":"shared","ledger_root":"…",
//!  "watermark_path":"…","outbox_path":"…","generated_at":"…",
//!  "goals":{"7k2mq":{"sig":[1711111111111111111,3],"row":{…},"state_digest":"…"}}}
//! ```
//!
//! A warm read is `readdir(live/)`, one stat per live goal's `events/`
//! directory, and a re-reduce of only the goals whose signature changed.
//! The projection is rebuildable from `live/` alone, and it carries the
//! root pointer the CLI fast path needs so the fast path never loads
//! config.

use std::collections::BTreeMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::super::view::GoalRowViewWire;
use super::layout::{goal_ledger_init, GoalLedgerError};
use super::read::read_live_markers;
use crate::fs_sig::{mtime_ns, write_json_atomic};
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
};

/// Hot projection schema version.
pub const GOAL_PROJECTION_SCHEMA_VERSION: u32 = 1;

/// Default projection filename under `~/.sase/projects/<key>/`.
pub const GOALS_HOT_FILENAME: &str = "goals-hot.json";

/// Environment override for the projection-write lock timeout.
const PROJECTION_LOCK_TIMEOUT_ENV: &str = "SASE_GOAL_PROJECTION_LOCK_TIMEOUT";

/// Default projection-write lock timeout in seconds.
const PROJECTION_LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);

/// Stat signature of one goal: `[events_dir_mtime_ns, entry_count]`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjectionSigWire(pub i64, pub u64);

/// One cached goal: its signature, row view, and state digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjectionGoalWire {
    /// `[events_dir_mtime_ns, entry_count]`.
    #[serde(default)]
    pub sig: GoalProjectionSigWire,
    /// Presentation-neutral row view.
    pub row: GoalRowViewWire,
    /// FNV-1a digest of the canonical reduced state JSON.
    #[serde(default)]
    pub state_digest: String,
}

/// The machine-local hot projection file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjectionWire {
    /// Projection schema version.
    #[serde(default)]
    pub schema_version: u32,
    /// Owning project key.
    #[serde(default)]
    pub project: String,
    /// `shared` or `local`.
    #[serde(default)]
    pub mode: String,
    /// Ledger root this projection was built from.
    #[serde(default)]
    pub ledger_root: String,
    /// Watermark path the fast path reports for freshness.
    #[serde(default)]
    pub watermark_path: String,
    /// Outbox path the fast path reports for publish state.
    #[serde(default)]
    pub outbox_path: String,
    /// RFC3339 generation time.
    #[serde(default)]
    pub generated_at: String,
    /// Cached goals by id.
    #[serde(default)]
    pub goals: BTreeMap<String, GoalProjectionGoalWire>,
}

/// Freshness of a projection file against its ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum GoalProjectionStatusNameWire {
    /// No projection file exists.
    Missing,
    /// The file's schema is not supported.
    SchemaMismatch,
    /// At least one signature or marker changed.
    Stale,
    /// Every signature matches and the marker set is identical.
    Fresh,
}

/// Status report for [`goal_projection_status`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjectionReportWire {
    /// `Missing`, `SchemaMismatch`, `Stale`, or `Fresh`.
    pub status: GoalProjectionStatusNameWire,
    /// Projection goals cached, when the file parses.
    #[serde(default)]
    pub cached_goals: u64,
    /// Live markers that changed signature or are uncached.
    #[serde(default)]
    pub changed_goals: u64,
    /// Whether the projection file was read successfully.
    #[serde(default)]
    pub readable: bool,
}

/// Refresh outcome for [`refresh_goal_projection`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalProjectionRefreshWire {
    /// Status before the refresh.
    pub status: GoalProjectionStatusNameWire,
    /// True when the file was rewritten.
    #[serde(default)]
    pub wrote: bool,
    /// The refreshed projection.
    pub projection: GoalProjectionWire,
}

/// FNV-1a 64-bit digest rendered as 16 lowercase hex chars.
fn fnv1a_hex(input: &str) -> String {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in input.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

/// Current signature of one goal's events directory.
fn goal_sig(
    root: &Path,
    goal_id: &str,
) -> Result<GoalProjectionSigWire, GoalLedgerError> {
    let dir = super::layout::goal_events_dir(root, goal_id);
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(GoalProjectionSigWire(0, 0));
        }
        Err(error) => {
            return Err(GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                dir.display()
            )));
        }
    };
    let mut count: u64 = 0;
    for entry in entries {
        let entry = entry.map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to list {}: {error}",
                dir.display()
            ))
        })?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            count += 1;
        }
    }
    let modified = fs::metadata(&dir).ok().and_then(|m| m.modified().ok());
    Ok(GoalProjectionSigWire(mtime_ns(modified), count))
}

/// Lock path guarding a projection write: a sibling `.lock`.
fn projection_lock_path(projection_path: &Path) -> PathBuf {
    let filename = projection_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(GOALS_HOT_FILENAME);
    projection_path.with_file_name(format!("{filename}.lock"))
}

/// Read a projection file. `Ok(None)` means missing.
fn load_projection(
    projection_path: &Path,
) -> Result<Option<GoalProjectionWire>, GoalLedgerError> {
    let contents = match fs::read_to_string(projection_path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => {
            return Err(GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                projection_path.display()
            )));
        }
    };
    let projection: GoalProjectionWire = serde_json::from_str(&contents)
        .map_err(|error| {
            GoalLedgerError::Data(format!(
                "unreadable projection {}: {error}",
                projection_path.display()
            ))
        })?;
    Ok(Some(projection))
}

/// Classify a projection file against its ledger without reducing.
pub fn goal_projection_status(
    root: &Path,
    projection_path: &Path,
) -> Result<GoalProjectionReportWire, GoalLedgerError> {
    let Some(projection) = load_projection(projection_path)? else {
        return Ok(GoalProjectionReportWire {
            status: GoalProjectionStatusNameWire::Missing,
            cached_goals: 0,
            changed_goals: 0,
            readable: false,
        });
    };
    if projection.schema_version != GOAL_PROJECTION_SCHEMA_VERSION {
        return Ok(GoalProjectionReportWire {
            status: GoalProjectionStatusNameWire::SchemaMismatch,
            cached_goals: 0,
            changed_goals: 0,
            readable: true,
        });
    }
    let markers = read_live_markers(root, None)?;
    let marker_set: BTreeMap<&str, ()> =
        markers.iter().map(|id| (id.as_str(), ())).collect();
    let mut changed: u64 = 0;
    for id in &markers {
        let current = goal_sig(root, id)?;
        match projection.goals.get(id) {
            Some(cached) if cached.sig == current => {}
            _ => changed += 1,
        }
    }
    for id in projection.goals.keys() {
        if !marker_set.contains_key(id.as_str()) {
            changed += 1;
        }
    }
    Ok(GoalProjectionReportWire {
        status: if changed == 0 {
            GoalProjectionStatusNameWire::Fresh
        } else {
            GoalProjectionStatusNameWire::Stale
        },
        cached_goals: projection.goals.len() as u64,
        changed_goals: changed,
        readable: true,
    })
}

/// Refresh (or rebuild) a projection file.
///
/// Only goals whose signature changed are re-reduced; unchanged goals
/// keep their cached row verbatim. The write is skipped when nothing
/// changed.
#[allow(clippy::too_many_arguments)]
pub fn refresh_goal_projection(
    root: &Path,
    projection_path: &Path,
    project: &str,
    mode: &str,
    watermark_path: &str,
    outbox_path: &str,
) -> Result<GoalProjectionRefreshWire, GoalLedgerError> {
    goal_ledger_init(root)?;
    let report = goal_projection_status(root, projection_path)?;
    let mut cached: BTreeMap<String, GoalProjectionGoalWire> =
        match load_projection(projection_path)? {
            Some(projection)
                if projection.schema_version
                    == GOAL_PROJECTION_SCHEMA_VERSION =>
            {
                projection.goals
            }
            _ => BTreeMap::new(),
        };
    let markers = read_live_markers(root, None)?;
    let mut goals: BTreeMap<String, GoalProjectionGoalWire> = BTreeMap::new();
    let mut reused_all = report.status == GoalProjectionStatusNameWire::Fresh;
    for id in &markers {
        let current = goal_sig(root, id)?;
        if let Some(entry) = cached.remove(id) {
            if entry.sig == current {
                goals.insert(id.clone(), entry);
                continue;
            }
        }
        reused_all = false;
        let state = super::read::reduce_goal(root, id, None)?;
        let digest =
            fnv1a_hex(&serde_json::to_string(&state).unwrap_or_default());
        let row = super::super::view::goal_row_view(&state);
        goals.insert(
            id.clone(),
            GoalProjectionGoalWire {
                sig: current,
                row,
                state_digest: digest,
            },
        );
    }
    if !cached.is_empty() {
        reused_all = false;
    }
    let projection = GoalProjectionWire {
        schema_version: GOAL_PROJECTION_SCHEMA_VERSION,
        project: project.to_string(),
        mode: mode.to_string(),
        ledger_root: root.display().to_string(),
        watermark_path: watermark_path.to_string(),
        outbox_path: outbox_path.to_string(),
        generated_at: super::layout::goal_now_rfc3339(),
        goals,
    };
    let wrote = if reused_all
        && report.status == GoalProjectionStatusNameWire::Fresh
    {
        false
    } else {
        let lock_path = projection_lock_path(projection_path);
        let holder_path = holder_path_for(&lock_path);
        let _lock = acquire_store_lock(
            &lock_path,
            &holder_path,
            LockMode::Exclusive,
            timeout_from_env(
                PROJECTION_LOCK_TIMEOUT_ENV,
                PROJECTION_LOCK_TIMEOUT_DEFAULT,
            ),
            "goal_projection_refresh",
        )
        .map_err(|error| GoalLedgerError::Lock(error.to_string()))?;
        if let Some(parent) = projection_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|error| {
                    GoalLedgerError::Io(format!(
                        "failed to create {}: {error}",
                        parent.display()
                    ))
                })?;
            }
        }
        write_json_atomic(projection_path, &serde_json::to_value(&projection)?)
            .map_err(|error| {
                GoalLedgerError::Io(format!(
                    "failed to write {}: {error}",
                    projection_path.display()
                ))
            })?;
        true
    };
    Ok(GoalProjectionRefreshWire {
        status: report.status,
        wrote,
        projection,
    })
}

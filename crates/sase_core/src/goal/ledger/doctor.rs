//! Goal ledger doctor: scan for drift and repair markers.
//!
//! Checks `STORE.json`, full marker⇔unsettled reconciliation, orphan
//! markers, unreadable goals (including `id_collision`), stray non-event
//! files under `items/`, and projection status. Repair only adds or
//! removes markers and rebuilds the projection; it never touches an
//! event file.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::layout::{
    goal_items_dir, goal_marker_path, read_goal_store, GoalLedgerError,
};
use super::projection::refresh_goal_projection;
use super::read::{read_goal_events, read_live_markers, reduce_read_goal};

/// Request for [`goal_ledger_doctor`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct GoalDoctorRequestWire {
    /// Repair by adding/removing markers and rebuilding the projection.
    #[serde(default)]
    pub repair: bool,
    /// Scope the marker reconciliation to these goal ids. Defaults to all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<String>>,
    /// Projection file for the status check and rebuild.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_path: Option<String>,
    /// Project key stamped on a rebuilt projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Mode stamped on a rebuilt projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    /// Watermark path stamped on a rebuilt projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watermark_path: Option<String>,
    /// Outbox path stamped on a rebuilt projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_path: Option<String>,
    /// Fetch TTL stamped on a rebuilt projection, in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_ttl_seconds: Option<f64>,
}

/// One doctor finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalDoctorCheckWire {
    /// Stable snake_case code.
    pub code: String,
    /// True when this check passed.
    pub ok: bool,
    /// Human-readable detail.
    #[serde(default)]
    pub message: String,
    /// Goal the finding attaches to, when any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub goal_id: Option<String>,
}

/// Doctor outcome for [`goal_ledger_doctor`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalDoctorWire {
    /// True when every check passed.
    pub ok: bool,
    /// One entry per check.
    #[serde(default)]
    pub checks: Vec<GoalDoctorCheckWire>,
    /// Ledger-relative paths the repair changed.
    #[serde(default)]
    pub changed_paths: Vec<String>,
    /// Goals that reduced to `readable: false`.
    #[serde(default)]
    pub unreadable: Vec<String>,
    /// Marked goals that reduced to a settled state.
    #[serde(default)]
    pub stale_markers: Vec<String>,
    /// Unsettled goals missing their marker.
    #[serde(default)]
    pub missing_markers: Vec<String>,
}

/// Scan the ledger and optionally repair markers plus the projection.
pub fn goal_ledger_doctor(
    root: &Path,
    request: &GoalDoctorRequestWire,
) -> Result<GoalDoctorWire, GoalLedgerError> {
    let mut checks: Vec<GoalDoctorCheckWire> = Vec::new();
    let mut changed_paths: Vec<String> = Vec::new();
    let mut unreadable: Vec<String> = Vec::new();
    let mut stale_markers: Vec<String> = Vec::new();
    let mut missing_markers: Vec<String> = Vec::new();

    // 1. STORE.json exists and is supported.
    match read_goal_store(root) {
        Ok(_) => checks.push(GoalDoctorCheckWire {
            code: "store".to_string(),
            ok: true,
            message: "STORE.json is supported".to_string(),
            goal_id: None,
        }),
        Err(error) => {
            checks.push(GoalDoctorCheckWire {
                code: "store".to_string(),
                ok: false,
                message: error.to_string(),
                goal_id: None,
            });
            return Ok(GoalDoctorWire {
                ok: false,
                checks,
                changed_paths,
                unreadable,
                stale_markers,
                missing_markers,
            });
        }
    }

    // 2. Full marker⇔unsettled reconciliation, scoped to `ids` when given.
    let markers = read_live_markers(root, None)?;
    let mut item_ids: Vec<String> = Vec::new();
    if let Ok(entries) = fs::read_dir(goal_items_dir(root)) {
        for entry in entries.flatten() {
            if entry.path().join("events").is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    item_ids.push(name.to_string());
                }
            }
        }
    }
    item_ids.sort();
    let scope: Option<BTreeSet<String>> = request.ids.as_ref().map(|ids| {
        ids.iter()
            .filter_map(|id| super::super::ids::parse_goal_id(id).ok())
            .collect()
    });
    let in_scope = |id: &str| -> bool {
        scope.as_ref().is_none_or(|set| set.contains(id))
    };

    let mut all_ids: BTreeSet<String> = BTreeSet::new();
    for id in markers.iter().chain(item_ids.iter()) {
        if in_scope(id) {
            all_ids.insert(id.clone());
        }
    }
    for id in &all_ids {
        let read = read_goal_events(root, id, None)?;
        let state = reduce_read_goal(id, &read);
        let events = &read.events;
        if !state.readable {
            unreadable.push(id.clone());
            checks.push(GoalDoctorCheckWire {
                code: "unreadable".to_string(),
                ok: false,
                message: format!(
                    "goal {id} is unreadable: {}",
                    state.unreadable_reason.unwrap_or_default()
                ),
                goal_id: Some(id.clone()),
            });
        }
        let marked = goal_marker_path(root, id).exists();
        if state.status.is_unsettled() {
            if !marked {
                missing_markers.push(id.clone());
                checks.push(GoalDoctorCheckWire {
                    code: "missing_marker".to_string(),
                    ok: false,
                    message: format!(
                        "goal {id} is unsettled but has no live marker"
                    ),
                    goal_id: Some(id.clone()),
                });
                if request.repair {
                    fs::write(goal_marker_path(root, id), b"").map_err(
                        |error| {
                            GoalLedgerError::Io(format!(
                                "failed to write marker for {id}: {error}"
                            ))
                        },
                    )?;
                    changed_paths.push(format!("live/{id}"));
                }
            }
        } else if marked && !events.is_empty() {
            stale_markers.push(id.clone());
            checks.push(GoalDoctorCheckWire {
                code: "stale_marker".to_string(),
                ok: false,
                message: format!(
                    "goal {id} is {} but still has a live marker",
                    state.status.as_str()
                ),
                goal_id: Some(id.clone()),
            });
            if request.repair {
                fs::remove_file(goal_marker_path(root, id)).map_err(
                    |error| {
                        GoalLedgerError::Io(format!(
                            "failed to remove marker for {id}: {error}"
                        ))
                    },
                )?;
                changed_paths.push(format!("live/{id}"));
            }
        }
    }
    // Orphan markers: marked ids with no events at all. They are extras
    // under the superset invariant; repair removes them.
    for id in &markers {
        if !in_scope(id) {
            continue;
        }
        let read = read_goal_events(root, id, None)?;
        if read.events.is_empty() && read.unparseable.is_empty() {
            checks.push(GoalDoctorCheckWire {
                code: "orphan_marker".to_string(),
                ok: false,
                message: format!("marker {id} has no events; removing it"),
                goal_id: Some(id.clone()),
            });
            if request.repair {
                let marker = goal_marker_path(root, id);
                if marker.exists() {
                    fs::remove_file(&marker).map_err(|error| {
                        GoalLedgerError::Io(format!(
                            "failed to remove orphan marker {id}: {error}"
                        ))
                    })?;
                    changed_paths.push(format!("live/{id}"));
                }
            }
        }
    }

    // 3. Stray non-event files under items/.
    let mut strays: Vec<PathBuf> = Vec::new();
    if let Ok(entries) = fs::read_dir(goal_items_dir(root)) {
        for entry in entries.flatten() {
            let item = entry.path();
            if !item.is_dir() {
                strays.push(item);
                continue;
            }
            let events_dir = item.join("events");
            if !events_dir.is_dir() {
                continue;
            }
            if let Ok(files) = fs::read_dir(&events_dir) {
                for file in files.flatten() {
                    let path = file.path();
                    let is_event = path.is_file()
                        && path.extension().and_then(|ext| ext.to_str())
                            == Some("json");
                    if !is_event {
                        strays.push(path);
                    }
                }
            }
        }
    }
    strays.sort();
    for stray in &strays {
        checks.push(GoalDoctorCheckWire {
            code: "stray_file".to_string(),
            ok: false,
            message: format!("stray file under items/: {}", stray.display()),
            goal_id: None,
        });
    }

    // 4. Projection status (informational; rebuild on repair).
    if let Some(projection_path) = request.projection_path.as_ref() {
        let path = Path::new(projection_path);
        match super::projection::goal_projection_status(root, path) {
            Ok(report) => {
                let ok = matches!(
                    report.status,
                    super::projection::GoalProjectionStatusNameWire::Fresh
                );
                checks.push(GoalDoctorCheckWire {
                    code: "projection".to_string(),
                    ok,
                    message: format!(
                        "projection is {:?} ({} cached, {} changed)",
                        report.status,
                        report.cached_goals,
                        report.changed_goals
                    ),
                    goal_id: None,
                });
                if request.repair
                    && !matches!(
                        report.status,
                        super::projection::GoalProjectionStatusNameWire::Fresh
                    )
                {
                    // Absent fields keep the existing projection
                    // header's value instead of blanking it: a repair
                    // of a stale marker must leave the header
                    // untouched.
                    let header =
                        super::projection::load_projection_header(path);
                    let project = request.project.clone().or_else(|| {
                        header.as_ref().map(|head| head.project.clone())
                    });
                    let mode = request.mode.clone().or_else(|| {
                        header.as_ref().map(|head| head.mode.clone())
                    });
                    let watermark =
                        request.watermark_path.clone().or_else(|| {
                            header
                                .as_ref()
                                .map(|head| head.watermark_path.clone())
                        });
                    let outbox = request.outbox_path.clone().or_else(|| {
                        header.as_ref().map(|head| head.outbox_path.clone())
                    });
                    let fetch_ttl = request.fetch_ttl_seconds.or_else(|| {
                        header.as_ref().map(|head| head.fetch_ttl_seconds)
                    });
                    let refreshed = refresh_goal_projection(
                        root,
                        path,
                        project.as_deref().unwrap_or(""),
                        mode.as_deref().unwrap_or("local"),
                        watermark.as_deref().unwrap_or(""),
                        outbox.as_deref().unwrap_or(""),
                        fetch_ttl.unwrap_or(
                            super::projection::GOAL_DEFAULT_FETCH_TTL_SECONDS,
                        ),
                    )?;
                    if refreshed.wrote {
                        changed_paths.push(projection_display(root, path));
                    }
                }
            }
            Err(error) => checks.push(GoalDoctorCheckWire {
                code: "projection".to_string(),
                ok: false,
                message: error.to_string(),
                goal_id: None,
            }),
        }
    }

    if checks.iter().all(|check| check.ok) {
        checks.push(GoalDoctorCheckWire {
            code: "healthy".to_string(),
            ok: true,
            message: "ledger is healthy".to_string(),
            goal_id: None,
        });
    }
    changed_paths.sort();
    changed_paths.dedup();
    unreadable.sort();
    stale_markers.sort();
    missing_markers.sort();
    let ok = checks.iter().all(|check| check.ok);
    Ok(GoalDoctorWire {
        ok,
        checks,
        changed_paths,
        unreadable,
        stale_markers,
        missing_markers,
    })
}

fn projection_display(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) => relative.display().to_string(),
        Err(_) => path.display().to_string(),
    }
}

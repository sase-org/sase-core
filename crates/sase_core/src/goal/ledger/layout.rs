//! On-disk goal ledger: file layout, `STORE.json` fence, and shared errors.
//!
//! Layout under a ledger root:
//!
//! ```text
//! goals/
//!   STORE.json                          {"schema_version":1,"layout":"sase-goal-ledger",...}
//!   live/<id>                           empty marker; the hot index
//!   items/<id>/events/<event_id>.json   immutable events, the only source of truth
//! ```
//!
//! Nothing is edited in place. Writers add uniquely named event files and
//! add or remove markers; event files are never rewritten or deleted.

use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::super::wire::GOAL_LEDGER_SCHEMA_VERSION;

/// Layout tag written into `STORE.json`.
pub const GOAL_LEDGER_LAYOUT: &str = "sase-goal-ledger";

/// Reserved draft root directory name (G4 builds the draft store there).
pub const GOAL_DRAFT_ROOT_NAME: &str = "goal-drafts";

/// Ledger store descriptor filename.
pub const GOAL_STORE_FILENAME: &str = "STORE.json";

/// Live-marker directory name.
pub const GOAL_LIVE_DIR_NAME: &str = "live";

/// Goal item directory name.
pub const GOAL_ITEMS_DIR_NAME: &str = "items";

/// Per-goal events directory name.
pub const GOAL_EVENTS_DIR_NAME: &str = "events";

/// Errors from goal ledger I/O.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GoalLedgerError {
    /// A filesystem operation failed.
    #[error("goal ledger io: {0}")]
    Io(String),
    /// A JSON payload failed to parse or serialize.
    #[error("goal ledger data: {0}")]
    Data(String),
    /// `STORE.json` is missing, unreadable, or unsupported.
    #[error("goal ledger store: {0}")]
    Store(String),
    /// The goal has no events in this ledger.
    #[error("goal ledger unknown goal: {0}")]
    UnknownGoal(String),
    /// A goal id failed validation.
    #[error("goal ledger invalid id: {0}")]
    InvalidId(String),
    /// The ledger lock could not be taken.
    #[error("goal ledger lock: {0}")]
    Lock(String),
}

impl GoalLedgerError {
    /// Stable snake_case code for the Python `ValueError` prefix.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::Data(_) => "data",
            Self::Store(_) => "store",
            Self::UnknownGoal(_) => "unknown_goal",
            Self::InvalidId(_) => "invalid_id",
            Self::Lock(_) => "lock",
        }
    }

    /// The `goal_ledger:<code>: <detail>` message the bindings raise.
    pub fn prefixed(&self) -> String {
        format!("goal_ledger:{}: {self}", self.code())
    }
}

impl From<serde_json::Error> for GoalLedgerError {
    fn from(error: serde_json::Error) -> Self {
        Self::Data(error.to_string())
    }
}

/// The `STORE.json` descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalStoreWire {
    /// Ledger schema version, currently 1.
    #[serde(default)]
    pub schema_version: u32,
    /// Layout tag, always `"sase-goal-ledger"`.
    #[serde(default)]
    pub layout: String,
    /// RFC3339 creation time.
    #[serde(default)]
    pub created_at: String,
}

/// Outcome of [`goal_ledger_init`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerInitWire {
    /// Absolute-or-as-given store path.
    pub store_path: String,
    /// True when this call created `STORE.json`.
    #[serde(default)]
    pub created: bool,
}

/// Path to `STORE.json` under a ledger root.
pub fn goal_store_path(root: &Path) -> PathBuf {
    root.join(GOAL_STORE_FILENAME)
}

/// Path to the live-marker directory under a ledger root.
pub fn goal_live_dir(root: &Path) -> PathBuf {
    root.join(GOAL_LIVE_DIR_NAME)
}

/// Path to the goal items directory under a ledger root.
pub fn goal_items_dir(root: &Path) -> PathBuf {
    root.join(GOAL_ITEMS_DIR_NAME)
}

/// Path to one goal's events directory.
pub fn goal_events_dir(root: &Path, goal_id: &str) -> PathBuf {
    goal_items_dir(root)
        .join(goal_id)
        .join(GOAL_EVENTS_DIR_NAME)
}

/// Path to one goal's live marker.
pub fn goal_marker_path(root: &Path, goal_id: &str) -> PathBuf {
    goal_live_dir(root).join(goal_id)
}

/// Reserved G4 draft root next to a ledger root's parent project dir.
pub fn goal_draft_root(project_dir: &Path) -> PathBuf {
    project_dir.join(GOAL_DRAFT_ROOT_NAME)
}

/// Current RFC3339 millisecond clock reading.
pub fn goal_now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Read and validate `STORE.json`. Fails closed on an unknown schema
/// or layout with a `run sase update` hint.
pub fn read_goal_store(root: &Path) -> Result<GoalStoreWire, GoalLedgerError> {
    let path = goal_store_path(root);
    let contents = fs::read_to_string(&path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            GoalLedgerError::Store(format!(
                "missing {} at {}; run `sase goal doctor` after the ledger is initialized",
                GOAL_STORE_FILENAME,
                path.display()
            ))
        } else {
            GoalLedgerError::Io(format!(
                "failed to read {}: {error}",
                path.display()
            ))
        }
    })?;
    let store: GoalStoreWire =
        serde_json::from_str(&contents).map_err(|error| {
            GoalLedgerError::Store(format!(
                "unreadable {} at {}; run `sase update`: {error}",
                path.display(),
                GOAL_STORE_FILENAME,
            ))
        })?;
    if store.schema_version != GOAL_LEDGER_SCHEMA_VERSION
        || store.layout != GOAL_LEDGER_LAYOUT
    {
        return Err(GoalLedgerError::Store(format!(
            "unsupported {} (schema_version={}, layout={:?}) at {}; run `sase update`",
            GOAL_STORE_FILENAME,
            store.schema_version,
            store.layout,
            path.display(),
        )));
    }
    Ok(store)
}

/// Initialize a ledger root: create the directories and `STORE.json`
/// when missing, and fail closed when an existing store is unsupported.
pub fn goal_ledger_init(
    root: &Path,
) -> Result<GoalLedgerInitWire, GoalLedgerError> {
    fs::create_dir_all(goal_live_dir(root)).map_err(|error| {
        GoalLedgerError::Io(format!(
            "failed to create {}: {error}",
            goal_live_dir(root).display()
        ))
    })?;
    fs::create_dir_all(goal_items_dir(root)).map_err(|error| {
        GoalLedgerError::Io(format!(
            "failed to create {}: {error}",
            goal_items_dir(root).display()
        ))
    })?;
    let path = goal_store_path(root);
    if path.exists() {
        read_goal_store(root)?;
        return Ok(GoalLedgerInitWire {
            store_path: path.display().to_string(),
            created: false,
        });
    }
    let now: chrono::DateTime<Utc> = chrono::Utc::now();
    let store = GoalStoreWire {
        schema_version: GOAL_LEDGER_SCHEMA_VERSION,
        layout: GOAL_LEDGER_LAYOUT.to_string(),
        created_at: now.to_rfc3339_opts(SecondsFormat::Millis, true),
    };
    crate::fs_sig::write_json_atomic(&path, &serde_json::to_value(&store)?)
        .map_err(|error| {
            GoalLedgerError::Io(format!(
                "failed to write {}: {error}",
                path.display()
            ))
        })?;
    Ok(GoalLedgerInitWire {
        store_path: path.display().to_string(),
        created: true,
    })
}

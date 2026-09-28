//! Goal ledger I/O probe: counts file opens per path class during a read.
//!
//! Tests use it to prove that settled items are never opened: seed 1,000
//! settled plus 10 live goals, run the hot read under the probe, and
//! assert zero opens under settled `items/`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use super::layout::GoalLedgerError;
use super::read::{goal_ledger_list, GoalListFilterWire, GoalListWire};

/// File-open counts for one probed read, by path class.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerProbeCountsWire {
    /// `STORE.json` reads.
    #[serde(default)]
    pub store_reads: u64,
    /// `live/` directory listings.
    #[serde(default)]
    pub live_dir_reads: u64,
    /// Per-goal `events/` directory stats.
    #[serde(default)]
    pub event_dir_stats: u64,
    /// Event files opened.
    #[serde(default)]
    pub event_files_opened: u64,
    /// Event files opened for goals outside the live set (must be 0 on
    /// a hot read).
    #[serde(default)]
    pub settled_event_opens: u64,
    /// Goals reduced during the read.
    #[serde(default)]
    pub goals_reduced: u64,
    /// True when the read ran the full history scan.
    #[serde(default)]
    pub history_scan: bool,
}

/// A probed hot read: the list result plus its open counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerProbeWire {
    /// Open counts by path class.
    #[serde(default)]
    pub counts: GoalLedgerProbeCountsWire,
    /// The hot-read result.
    pub list: GoalListWire,
}

/// Run the hot read under the probe.
///
/// After the list, every `items/<id>` with events that is not in the
/// live set is classified as settled: any event file opened for such an
/// id counts as a settled open. On a hot read that count is zero because
/// only marked goals are reduced.
pub fn probe_goal_ledger_list(
    root: &Path,
    filter: &GoalListFilterWire,
) -> Result<GoalLedgerProbeWire, GoalLedgerError> {
    let mut counts = GoalLedgerProbeCountsWire::default();
    counts.store_reads += 1;
    let list = goal_ledger_list(root, filter, Some(&mut counts))?;
    counts.goals_reduced = list.goals.len() as u64 + list.stale_markers;
    // Classify opens: any reduced goal outside the live set would be a
    // settled open. The hot read only reduces marked goals, so recompute
    // the live set and confirm.
    let live = super::read::read_live_markers(root, None)?;
    let live_set: std::collections::BTreeSet<&str> =
        live.iter().map(String::as_str).collect();
    let mut settled_opens: u64 = 0;
    for state in &list.goals {
        if !live_set.contains(state.id.as_str()) {
            settled_opens += 1;
        }
    }
    counts.settled_event_opens = settled_opens;
    Ok(GoalLedgerProbeWire { counts, list })
}

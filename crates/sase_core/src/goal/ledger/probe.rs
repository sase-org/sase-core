//! Goal ledger I/O probe: counts file opens per path class during a read.
//!
//! Tests use it to prove that settled items are never opened: seed 1,000
//! settled plus 10 live goals, run the hot read under the probe, and
//! assert zero opens under settled `items/`.

use std::path::Path;

use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use super::layout::GoalLedgerError;
use super::read::{
    goal_ledger_history, goal_ledger_list, GoalHistoryFilterWire,
    GoalListFilterWire, GoalListWire,
};

/// File-open counts for one probed read, by path class.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoalLedgerProbeCountsWire {
    /// `STORE.json` reads.
    #[serde(default)]
    pub store_reads: u64,
    /// `live/` directory listings.
    #[serde(default)]
    pub live_dir_reads: u64,
    /// Per-goal `events/` directories actually opened.
    #[serde(default)]
    pub event_dir_stats: u64,
    /// Event files opened.
    #[serde(default)]
    pub event_files_opened: u64,
    /// `items/<id>/events` directories opened for goals outside the
    /// live set (must be 0 on a hot read).
    #[serde(default)]
    pub settled_event_opens: u64,
    /// Goals reduced during the read.
    #[serde(default)]
    pub goals_reduced: u64,
    /// True when the read ran the full history scan.
    #[serde(default)]
    pub history_scan: bool,
    /// Goal ids whose `items/<id>/events` directory was actually
    /// opened during the read, in open order. Never serialized: the
    /// probe reports aggregates, this is the evidence behind them.
    #[serde(skip)]
    pub opened_event_dirs: Vec<String>,
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

/// Count the opened `items/<id>/events` directories that fall outside
/// the live set: those are settled opens.
fn settled_opens(counts: &GoalLedgerProbeCountsWire, live: &[String]) -> u64 {
    let live_set: BTreeSet<&str> = live.iter().map(String::as_str).collect();
    counts
        .opened_event_dirs
        .iter()
        .filter(|id| !live_set.contains(id.as_str()))
        .count() as u64
}

/// Run the hot read under the probe.
///
/// `settled_event_opens` counts the `items/<id>/events` directories
/// actually opened for goals with no `live/` marker. On a hot read
/// that count is zero because only marked goals' directories open.
pub fn probe_goal_ledger_list(
    root: &Path,
    filter: &GoalListFilterWire,
) -> Result<GoalLedgerProbeWire, GoalLedgerError> {
    let mut counts = GoalLedgerProbeCountsWire::default();
    let list = goal_ledger_list(root, filter, Some(&mut counts))?;
    counts.goals_reduced = list.goals.len() as u64 + list.stale_markers;
    let live = super::read::read_live_markers(root, None)?;
    counts.settled_event_opens = settled_opens(&counts, &live);
    Ok(GoalLedgerProbeWire { counts, list })
}

/// Run the explicit history scan under the probe.
///
/// The scan opens every goal's events directory by design, so this is
/// the negative control: `settled_event_opens` must be non-zero
/// whenever settled goals exist.
pub fn probe_goal_ledger_history(
    root: &Path,
    filter: &GoalHistoryFilterWire,
) -> Result<GoalLedgerProbeWire, GoalLedgerError> {
    let mut counts = GoalLedgerProbeCountsWire::default();
    let list = goal_ledger_history(root, filter, Some(&mut counts))?;
    counts.goals_reduced = list.goals.len() as u64;
    let live = super::read::read_live_markers(root, None)?;
    counts.settled_event_opens = settled_opens(&counts, &live);
    Ok(GoalLedgerProbeWire { counts, list })
}

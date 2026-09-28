//! Goal ledger file I/O: layout, appends, hot reads, projection,
//! doctor, and the I/O probe.
//!
//! Implements the `ledger-io` phase of the goal-ledger epic plan: the
//! `STORE.json` fence, marker-superset write ordering, appends, the
//! O(unsettled) hot read, the stat-signature projection, history scans,
//! doctor scan and repair, and the I/O probe.

mod append;
mod doctor;
mod layout;
mod probe;
mod projection;
mod read;
#[cfg(test)]
mod tests;

pub use append::{
    goal_ledger_append, goal_ledger_init_root, GoalLedgerAppendOutcomeWire,
    GoalLedgerAppendRequestWire, GOAL_APPEND_APPLIED, GOAL_APPEND_REFUSED,
    GOAL_APPEND_STALE_BASIS,
};
pub use doctor::{
    goal_ledger_doctor, GoalDoctorCheckWire, GoalDoctorRequestWire,
    GoalDoctorWire,
};
pub use layout::{
    goal_draft_root, goal_events_dir, goal_items_dir, goal_ledger_init,
    goal_live_dir, goal_marker_path, goal_now_rfc3339, goal_store_path,
    read_goal_store, GoalLedgerError, GoalLedgerInitWire, GoalStoreWire,
    GOAL_DRAFT_ROOT_NAME, GOAL_EVENTS_DIR_NAME, GOAL_ITEMS_DIR_NAME,
    GOAL_LEDGER_LAYOUT, GOAL_LIVE_DIR_NAME, GOAL_STORE_FILENAME,
};
pub use probe::{
    probe_goal_ledger_list, GoalLedgerProbeCountsWire, GoalLedgerProbeWire,
};
pub use projection::{
    goal_projection_status, refresh_goal_projection, GoalProjectionGoalWire,
    GoalProjectionRefreshRequestWire, GoalProjectionRefreshWire,
    GoalProjectionReportWire, GoalProjectionSigWire,
    GoalProjectionStatusNameWire, GoalProjectionWire, GOALS_HOT_FILENAME,
    GOAL_DEFAULT_FETCH_TTL_SECONDS, GOAL_PROJECTION_SCHEMA_VERSION,
};
pub use read::{
    goal_ledger_history, goal_ledger_list, goal_ledger_show, read_goal_events,
    read_live_markers, reduce_goal, GoalHistoryFilterWire, GoalListFilterWire,
    GoalListWire,
};

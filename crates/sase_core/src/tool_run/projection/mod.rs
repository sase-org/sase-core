//! Fingerprint-free ToolRun glance projections for live UI surfaces.
//!
//! The TUI, CLI watchers, and future frontends read these instead of
//! `tool_run_list`: lean columns in one statement per table, batched
//! `run_id IN (...)` lookups, and no `private_argv`, fingerprint, launch
//! envelope, or launcher payload anywhere in the result.

mod briefs;
mod detail;
mod glance;
mod nodes;
mod shared;
#[cfg(test)]
mod tests;
mod wire;

pub use briefs::tool_run_briefs;
pub use detail::tool_run_detail;
pub use glance::tool_run_live_glance;
pub use nodes::tool_run_node_summaries;
pub use wire::{
    ToolRunBriefOwnerWire, ToolRunBriefWire, ToolRunBriefsRequestWire,
    ToolRunBriefsResultWire, ToolRunDetailRequestWire, ToolRunDetailResultWire,
    ToolRunDetailStageCountsWire, ToolRunDetailStageWire,
    ToolRunDetailTriageItemWire, ToolRunExpectedStageWire,
    ToolRunGlanceStageWire, ToolRunGlanceWire, ToolRunLiveGlanceRequestWire,
    ToolRunLiveGlanceResultWire, ToolRunNodeSelectorWire,
    ToolRunNodeSummariesRequestWire, ToolRunNodeSummariesResultWire,
    ToolRunNodeSummaryWire, ToolRunVerdictBucketWire,
    ToolRunVerdictSummaryWire, TOOL_RUN_BRIEFS_DEFAULT_LIMIT,
    TOOL_RUN_BRIEFS_MAX_LIMIT, TOOL_RUN_DETAIL_DEFAULT_ITEM_LIMIT,
    TOOL_RUN_DETAIL_DEFAULT_WINDOW_DAYS, TOOL_RUN_DETAIL_MAX_ITEM_LIMIT,
    TOOL_RUN_DETAIL_MAX_LOCATORS, TOOL_RUN_DETAIL_MAX_WINDOW_DAYS,
    TOOL_RUN_GLANCE_MAX_RUNS, TOOL_RUN_LABEL_MAX_CHARS,
    TOOL_RUN_NODE_DEFAULT_LIMIT, TOOL_RUN_NODE_MAX_LIMIT,
    TOOL_RUN_NODE_MAX_SELECTORS, TOOL_RUN_SILENT_AFTER_SECONDS,
};

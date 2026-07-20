//! Phase 3B: pure-Rust artifact filesystem snapshot scanner.
//!
//! Mirrors the Phase 3A Python wire contract in
//! `sase_100/src/sase/core/agent_scan_wire.py` and the pure-Python scanner
//! in `sase_100/src/sase/core/agent_scan_facade.py::scan_agent_artifacts_python`.
//! The crate stays free of PyO3 types so later UniFFI/WASM/server work can
//! reuse the same logic; the PyO3 binding lives in `sase_core_py`
//! (Phase 3C).
//!
//! See `sase_100/plans/202604/rust_backend_phase3_agent_scan.md` for the
//! full phase plan and `..._phase3a_handoff.md` for the wire contract
//! Phase 3B reproduces here.

mod context;
pub mod index;
pub mod layout;
pub mod scanner;
pub mod wire;

pub use index::{
    agent_artifact_index_status, delete_agent_artifact_index_row,
    delete_agent_artifact_index_row_with_busy_timeout,
    query_agent_artifact_index, query_related_agent_artifact_dirs,
    read_agent_artifact_index_meta, rebuild_agent_artifact_index,
    replace_agent_artifact_index_dismissed_agents,
    terminalize_stale_active_agent_artifact_index_rows,
    upsert_agent_artifact_index_row, write_agent_artifact_index_meta,
    AgentArtifactIndexQueryWire, AgentArtifactIndexStatusWire,
    AgentArtifactIndexUpdateWire, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
};
pub use layout::{
    canonical_agent_artifact_path, collect_workflow_artifact_candidates,
    is_artifact_timestamp, is_day_sharded_workflow, legacy_agent_artifact_path,
    parse_agent_artifact_path, resolve_agent_artifact_path,
    resolve_agent_artifact_timestamp_path, AgentArtifactPathInfo,
    WorkflowArtifactCandidate, WorkflowArtifactCandidates,
    ACE_RUN_WORKFLOW_DIR, DAY_SHARDED_LAYOUT_VERSION, LEGACY_LAYOUT_VERSION,
};
pub use scanner::{
    scan_agent_artifact_dir, scan_agent_artifact_dirs, scan_agent_artifacts,
};
pub use wire::{
    is_supported_workflow_dir, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentClanContextWire, AgentMetaWire, DoneMarkerWire,
    PlanPathMarkerWire, PromptStepMarkerWire, RunningMarkerWire,
    WaitingMarkerWire, WorkflowStateWire, WorkflowStepStateWire,
    AGENT_SCAN_WIRE_SCHEMA_VERSION, DONE_WORKFLOW_DIR_NAMES,
    DONE_WORKFLOW_DIR_PREFIXES, WORKFLOW_STATE_DIR_NAMES,
    WORKFLOW_STATE_DIR_PREFIXES,
};

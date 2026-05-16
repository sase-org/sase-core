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

pub mod index;
pub mod scanner;
pub mod wire;

pub use index::{
    delete_agent_artifact_index_row, delete_dismissed_agent_visibility,
    query_agent_artifact_index, rebuild_agent_artifact_index,
    replace_dismissed_agent_visibility, upsert_agent_artifact_index_row,
    upsert_dismissed_agent_visibility, AgentArtifactIndexQueryWire,
    AgentArtifactIndexUpdateWire, DismissedAgentIdentityWire,
    AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
};
pub use scanner::{scan_agent_artifact_dir, scan_agent_artifacts};
pub use wire::{
    is_supported_workflow_dir, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentMetaWire, DoneMarkerWire, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
    WorkflowStateWire, WorkflowStepStateWire, AGENT_SCAN_WIRE_SCHEMA_VERSION,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};

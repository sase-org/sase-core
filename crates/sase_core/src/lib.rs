//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A modeled the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B added a minimal full-file parser for
//! ChangeSpec boundaries and scalar fields. Phase 1C adds section parity:
//! structured `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
//! `DELTAS` entries that match the Python parser output for the golden
//! corpus. The crate is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod agent_scan;
pub mod parser;
pub mod query;
pub mod sections;
pub mod status;
pub mod suffix;
pub mod wire;

pub use agent_scan::{
    is_supported_workflow_dir, scan_agent_artifacts, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentMetaWire, DoneMarkerWire, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
    WorkflowStateWire, WorkflowStepStateWire, AGENT_SCAN_WIRE_SCHEMA_VERSION,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
pub use parser::parse_project_bytes;
pub use query::{
    canonicalize_query, compile_query, evaluate_query_many, evaluate_query_one,
    get_base_status, get_searchable_text, has_any_status_suffix, parse_query,
    project_dir_name, strip_reverted_suffix, tokenize_query, QueryErrorWire,
    QueryEvaluationContext, QueryExprWire, QueryProgram, QueryProgramWire,
    QueryTokenKind, QueryTokenWire,
};
pub use status::{
    apply_status_update as apply_status_update_lines, get_next_suffix_number,
    has_suffix as has_changespec_suffix, is_valid_transition,
    plan_status_transition, read_status_from_lines, remove_workspace_suffix,
    status_plan_from_json_value, status_request_from_json_value,
    valid_transitions_from, ChangespecChildWire, StatusFieldReadWire,
    StatusFieldUpdateWire, StatusTransitionPlanWire,
    StatusTransitionRequestWire, ARCHIVE_ACTION_FROM_ARCHIVE,
    ARCHIVE_ACTION_NONE, ARCHIVE_ACTION_TO_ARCHIVE, ARCHIVE_STATUSES,
    MENTOR_ACTION_CLEAR, MENTOR_ACTION_NONE, MENTOR_ACTION_SET,
    STATUS_WIRE_SCHEMA_VERSION, SUFFIX_ACTION_APPEND, SUFFIX_ACTION_NONE,
    SUFFIX_ACTION_STRIP, VALID_STATUSES,
};
pub use suffix::{is_entry_ref_suffix, parse_suffix_prefix, ParsedSuffix};
pub use wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookStatusLineWire,
    HookWire, MentorStatusLineWire, MentorWire, ParseErrorWire, SourceSpanWire,
    TimestampWire, CHANGESPEC_WIRE_SCHEMA_VERSION,
};

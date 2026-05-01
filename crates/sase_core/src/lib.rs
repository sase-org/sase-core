//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A modeled the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B added a minimal full-file parser for
//! ChangeSpec boundaries and scalar fields. Phase 1C adds section parity:
//! structured `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
//! `DELTAS` entries that match the Python parser output for the golden
//! corpus. The crate is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod agent_cleanup;
pub mod agent_launch;
pub mod agent_scan;
pub mod git_query;
pub mod notifications;
pub mod parser;
pub mod query;
pub mod sections;
pub mod status;
pub mod suffix;
pub mod wire;

pub use agent_cleanup::{
    cleanup_plan_from_json_value, cleanup_request_from_json_value,
    delete_agent_artifact_markers, mark_comment_agents_as_killed,
    mark_hook_agents_as_killed, mark_mentor_agents_as_killed,
    plan_agent_cleanup, release_workspace_from_content,
    save_dismissed_agents_index, save_dismissed_bundle_json,
    AgentCleanupArtifactDeleteIntentWire, AgentCleanupArtifactDeleteResultWire,
    AgentCleanupBundleSaveIntentWire, AgentCleanupBundleWriteResultWire,
    AgentCleanupCountsWire, AgentCleanupDismissItemWire,
    AgentCleanupDismissalRenameIntentWire, AgentCleanupIdentityWire,
    AgentCleanupKillItemWire, AgentCleanupNotificationDismissIntentWire,
    AgentCleanupPlanWire, AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
    AgentCleanupSkippedItemWire, AgentCleanupTargetWire,
    AgentCleanupWorkspaceReleaseIntentWire,
    AgentCleanupWorkspaceReleaseResultWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
    CLEANUP_MODE_DISMISS_COMPLETED, CLEANUP_MODE_KILL_AND_DISMISS,
    CLEANUP_MODE_PREVIEW_ONLY, CLEANUP_SCOPE_ALL_PANELS,
    CLEANUP_SCOPE_CUSTOM_SELECTION, CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
    CLEANUP_SCOPE_FOCUSED_GROUP, CLEANUP_SCOPE_FOCUSED_PANEL,
    CLEANUP_SCOPE_TAG, CONFIRMATION_SEVERITY_DESTRUCTIVE,
    CONFIRMATION_SEVERITY_DISMISS, CONFIRMATION_SEVERITY_NONE, KILL_KIND_CRS,
    KILL_KIND_HOOK, KILL_KIND_MENTOR, KILL_KIND_RUNNING, KILL_KIND_WORKFLOW,
    SKIPPED_DUPLICATE, SKIPPED_NOT_DISMISSABLE, SKIPPED_NOT_IN_SCOPE,
    SKIPPED_NOT_KILLABLE, SKIPPED_UNKNOWN_KILL_KIND,
    SKIPPED_WORKFLOW_CHILD_CASCADE_ONLY,
};
pub use agent_launch::{
    allocate_and_claim_workspace_from_content,
    list_workspace_claims_from_content, plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content, prepare_agent_launch,
    safe_launch_name, AgentLaunchPreparationError, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, LaunchFanoutPlanWire, LaunchFanoutSlotWire,
    WorkspaceClaimOutcomeWire, WorkspaceClaimPlanWire,
    WorkspaceClaimRequestWire, WorkspaceClaimWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
};
pub use agent_scan::{
    is_supported_workflow_dir, scan_agent_artifacts, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentMetaWire, DoneMarkerWire, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
    WorkflowStateWire, WorkflowStepStateWire, AGENT_SCAN_WIRE_SCHEMA_VERSION,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
pub use git_query::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z, GitNameStatusEntryWire,
    GIT_QUERY_WIRE_SCHEMA_VERSION,
};
pub use notifications::{
    append_notification, apply_notification_state_update,
    read_notifications_snapshot, read_notifications_snapshot_with_options,
    rewrite_notifications, NotificationAgentKeyWire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationWire, NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
};
pub use parser::parse_project_bytes;
pub use query::{
    canonicalize_query, compile_query, evaluate_query_many,
    evaluate_query_many_in_corpus, evaluate_query_one, get_base_status,
    get_searchable_text, has_any_status_suffix, parse_query, project_dir_name,
    strip_reverted_suffix, tokenize_query, QueryCorpus, QueryErrorWire,
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

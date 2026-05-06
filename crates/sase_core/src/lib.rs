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
pub mod bead;
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
    allocate_and_claim_workspace_from_content, allocate_launch_timestamp_batch,
    list_workspace_claims_from_content, plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content, prepare_agent_launch,
    safe_launch_name, AgentLaunchPreparationError, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, LaunchFanoutPlanWire, LaunchFanoutSlotWire,
    TimestampBatchAllocationError, WorkspaceClaimOutcomeWire,
    WorkspaceClaimPlanWire, WorkspaceClaimRequestWire, WorkspaceClaimWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
};
pub use agent_scan::{
    delete_agent_artifact_index_row, is_supported_workflow_dir,
    query_agent_artifact_index, rebuild_agent_artifact_index,
    scan_agent_artifact_dir, scan_agent_artifacts,
    upsert_agent_artifact_index_row, AgentArtifactIndexQueryWire,
    AgentArtifactIndexUpdateWire, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentMetaWire, DoneMarkerWire, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
    WorkflowStateWire, WorkflowStepStateWire,
    AGENT_ARTIFACT_INDEX_SCHEMA_VERSION, AGENT_SCAN_WIRE_SCHEMA_VERSION,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
pub use bead::{
    add_dependency as bead_add_dependency,
    blocked_issues as bead_blocked_issues,
    blocked_merged_issues as bead_blocked_merged_issues,
    changespec_metadata_migration_sql, close_issues as bead_close_issues,
    create_issue as bead_create_issue, default_config, doctor as bead_doctor,
    epic_count_migration_sql, export_issues_to_jsonl,
    export_jsonl as bead_export_jsonl,
    get_epic_children as bead_get_epic_children,
    get_merged_epic_children as bead_get_merged_epic_children,
    import_issues_from_jsonl, init_store as bead_init_store,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    list_issues as bead_list_issues,
    list_merged_issues as bead_list_merged_issues, load_config,
    load_config_from_str, mark_ready_to_work as bead_mark_ready_to_work,
    merge_workspace_issues as bead_merge_workspace_issues,
    merged_stats as bead_merged_stats, missing_changespec_metadata_columns,
    needs_epic_count_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, open_issue as bead_open_issue,
    parse_issues_jsonl, read_store_issues, ready_issues as bead_ready_issues,
    ready_merged_issues as bead_ready_merged_issues,
    remove_issue as bead_remove_issue, save_config,
    show_issue as bead_show_issue, show_merged_issue as bead_show_merged_issue,
    stats as bead_stats, sync_is_clean as bead_sync_is_clean,
    unmark_ready_to_work as bead_unmark_ready_to_work,
    update_issue as bead_update_issue, BeadConfigWire, BeadCreateRequestWire,
    BeadError, BeadMutationOutcomeWire, BeadUpdateFieldsWire, DependencyWire,
    IssueTypeWire, IssueWire, JsonlLoadOutcome, OperationOutcomeWire,
    StatusWire, BEAD_READ_WIRE_SCHEMA_VERSION, BEAD_SQLITE_SCHEMA,
};
pub use git_query::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z, GitNameStatusEntryWire,
    GIT_QUERY_WIRE_SCHEMA_VERSION,
};
pub use notifications::{
    append_notification, apply_notification_state_update,
    cleanup_stale_pending_actions, current_unix_time,
    legacy_telegram_pending_actions_path,
    mobile_action_detail_from_notification,
    mobile_attachment_manifest_from_path, mobile_notification_card_from_wire,
    pending_action_from_notification, pending_action_identity,
    pending_action_state_for_notification, pending_action_state_from_store,
    pending_action_store_path, plan_hitl_action_response,
    plan_plan_action_response, plan_question_action_response,
    plan_question_action_response_from_bytes, read_notifications_snapshot,
    read_notifications_snapshot_with_options, read_pending_action_store,
    register_pending_action, resolve_notification_prefix,
    resolve_pending_action_prefix, rewrite_notifications, ActionResultWire,
    HitlActionChoiceWire, HitlActionRequestWire, MobileActionDetailWire,
    MobileActionKindWire, MobileActionPlanErrorCodeWire,
    MobileActionPlanErrorWire, MobileActionStateWire, MobileActionSummaryWire,
    MobileAttachmentKindWire, MobileAttachmentManifestWire,
    MobileNotificationCardWire, MobileNotificationDetailResponseWire,
    MobileNotificationListRequestWire, MobileNotificationListResponseWire,
    NotificationAgentKeyWire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationWire, PendingActionIdentityWire,
    PendingActionPrefixResolutionWire, PendingActionStoreWire,
    PendingActionTransportWire, PendingActionWire, PlanActionChoiceWire,
    PlanActionRequestWire, QuestionActionChoiceWire, QuestionActionRequestWire,
    DEFAULT_PENDING_ACTION_PREFIX_LEN, DEFAULT_PENDING_ACTION_STALE_SECONDS,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
    PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
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

//! Pure-Rust core for the sase ChangeSpec wire contract.
//!
//! Phase 1A modeled the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B added a minimal full-file parser for
//! ChangeSpec boundaries and scalar fields. Phase 1C adds section parity:
//! structured `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
//! `DELTAS` entries that match the Python parser output for the golden
//! corpus. The crate is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod agent_archive;
pub mod agent_cleanup;
pub mod agent_family;
pub mod agent_group_archive;
pub mod agent_launch;
pub mod agent_name_template;
pub mod agent_scan;
pub mod bead;
pub mod config;
pub mod editor;
pub mod effort;
pub mod git_query;
pub mod host_bridge;
pub mod notifications;
pub mod parser;
pub mod plan;
pub mod project_spec;
pub mod prompt_stash;
pub mod query;
pub mod sections;
pub mod status;
pub mod suffix;
pub mod wire;
pub mod xprompt_catalog;

pub use agent_archive::{
    agent_archive_facet_counts, mark_agent_archive_bundles_revived,
    query_agent_archive, verify_agent_archive_index,
    AgentArchiveFacetCountWire, AgentArchiveFacetCountsWire,
    AgentArchiveFacetRequestWire, AgentArchiveLifecycleFailureWire,
    AgentArchivePurgeReportWire, AgentArchiveQueryPageWire,
    AgentArchiveQueryRequestWire, AgentArchiveReviveMarkReportWire,
    AgentArchiveReviveMarkRequestWire, AgentArchiveScrubReportWire,
    AgentArchiveSummaryWire, AgentArchiveVerifyReportWire,
    AGENT_ARCHIVE_WIRE_SCHEMA_VERSION,
};
pub use agent_cleanup::{
    cleanup_plan_from_json_value, cleanup_request_from_json_value,
    delete_agent_artifact_markers, mark_comment_agents_as_killed,
    mark_hook_agents_as_killed, mark_mentor_agents_as_killed,
    plan_agent_cleanup, release_workspace_from_content,
    save_dismissed_agents_index, save_dismissed_bundle_json,
    AgentCleanupArtifactDeleteIntentWire, AgentCleanupArtifactDeleteResultWire,
    AgentCleanupBundleSaveIntentWire, AgentCleanupBundleWriteResultWire,
    AgentCleanupCountsWire, AgentCleanupDismissItemWire,
    AgentCleanupIdentityWire, AgentCleanupKillItemWire,
    AgentCleanupNotificationDismissIntentWire, AgentCleanupPlanWire,
    AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
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
pub use agent_family::{
    resolve_agent_family_parent, AgentFamilyDismissedIdentityWire,
    AgentFamilyParentCandidateWire, AgentFamilyParentResolutionRequestWire,
    AgentFamilyParentResolutionWire,
    AGENT_FAMILY_RESOLUTION_WIRE_SCHEMA_VERSION,
};
pub use agent_group_archive::{
    list_dismissed_agent_groups, list_recent_dismissed_agent_groups,
    load_dismissed_agent_group, load_recent_dismissed_agent_group,
    mark_dismissed_agent_group_revived,
    mark_recent_dismissed_agent_group_revived,
    record_recent_dismissed_agent_group, save_dismissed_agent_group,
    SavedAgentGroupPageWire, SavedAgentGroupRefWire,
    SavedAgentGroupSummaryWire, SavedAgentGroupWire,
    AGENT_GROUP_ARCHIVE_WIRE_SCHEMA_VERSION,
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
pub use agent_name_template::{
    agent_name_template_namespace_template, agent_name_template_tokens_after,
    compare_agent_name_template_tokens, is_agent_name_template,
    is_valid_agent_name_template_token, match_agent_name_template,
    next_agent_name_template_token, parse_agent_name_template,
    render_agent_name_template, validate_agent_name_template_token,
    AgentNameTemplate, AgentNameTemplateError, AGENT_NAME_TEMPLATE_ALPHABET,
    AGENT_NAME_TEMPLATE_MARKER,
};
pub use agent_scan::{
    agent_artifact_index_status, canonical_agent_artifact_path,
    collect_workflow_artifact_candidates, delete_agent_artifact_index_row,
    is_artifact_timestamp, is_day_sharded_workflow, is_supported_workflow_dir,
    legacy_agent_artifact_path, parse_agent_artifact_path,
    query_agent_artifact_index, query_related_agent_artifact_dirs,
    read_agent_artifact_index_meta, rebuild_agent_artifact_index,
    replace_agent_artifact_index_dismissed_agents, resolve_agent_artifact_path,
    resolve_agent_artifact_timestamp_path, scan_agent_artifact_dir,
    scan_agent_artifact_dirs, scan_agent_artifacts,
    terminalize_stale_active_agent_artifact_index_rows,
    upsert_agent_artifact_index_row, write_agent_artifact_index_meta,
    AgentArtifactIndexQueryWire, AgentArtifactIndexStatusWire,
    AgentArtifactIndexUpdateWire, AgentArtifactPathInfo,
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire, AgentMetaWire,
    DoneMarkerWire, PlanPathMarkerWire, PromptStepMarkerWire,
    RunningMarkerWire, WaitingMarkerWire, WorkflowArtifactCandidate,
    WorkflowArtifactCandidates, WorkflowStateWire, WorkflowStepStateWire,
    ACE_RUN_WORKFLOW_DIR, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
    AGENT_SCAN_WIRE_SCHEMA_VERSION, DAY_SHARDED_LAYOUT_VERSION,
    DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES, LEGACY_LAYOUT_VERSION,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
pub use bead::{
    add_dependency as bead_add_dependency,
    blocked_issues as bead_blocked_issues, changespec_metadata_migration_sql,
    close_issues as bead_close_issues, create_issue as bead_create_issue,
    default_config, doctor as bead_doctor, epic_count_migration_sql,
    export_issues_to_jsonl, export_jsonl as bead_export_jsonl,
    get_epic_children as bead_get_epic_children, import_issues_from_jsonl,
    import_issues_to_event_streams, init_store as bead_init_store,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    list_issues as bead_list_issues, load_config, load_config_from_str,
    mark_ready_to_work as bead_mark_ready_to_work, merge_bead_event_streams,
    missing_changespec_metadata_columns, model_migration_sql,
    needs_epic_count_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, needs_model_migration,
    open_issue as bead_open_issue, parse_issues_jsonl,
    preclaim_epic_work_plan as bead_preclaim_epic_work_plan,
    read_event_store_issues as bead_read_event_store_issues,
    read_legacy_jsonl_issues as bead_read_legacy_jsonl_issues,
    read_store_issues, ready_issues as bead_ready_issues, reduce_event_streams,
    remove_issue as bead_remove_issue, save_config,
    search_issues as bead_search_issues, show_issue as bead_show_issue,
    stats as bead_stats, sync_is_clean as bead_sync_is_clean,
    unmark_ready_to_work as bead_unmark_ready_to_work,
    update_issue as bead_update_issue, BeadConfigWire, BeadCreateRequestWire,
    BeadError, BeadEventOperationWire, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadIssueUpdateEventFieldsWire, BeadMutationOutcomeWire,
    BeadPreclaimAssignmentWire, BeadPreclaimRollbackWire, BeadSearchMatchWire,
    BeadTierWire, BeadUpdateFieldsWire, DependencyWire, IssueTypeWire,
    IssueWire, JsonlLoadOutcome, OperationOutcomeWire, StatusWire,
    BEAD_EVENT_SCHEMA_VERSION, BEAD_READ_WIRE_SCHEMA_VERSION,
    BEAD_SEARCH_FIELD_NAMES, BEAD_SQLITE_SCHEMA,
};
pub use config::{
    config_field_model, config_inventory, config_plan_edit, config_validate,
    ConfigConstraintsWire, ConfigContributionWire, ConfigDiagnosticWire,
    ConfigEditOpWire, ConfigEditPlanWire, ConfigEditRequestWire,
    ConfigEffectivePreviewWire, ConfigError, ConfigFieldModelWire,
    ConfigFieldStateWire, ConfigFieldWire, ConfigInventoryRequestWire,
    ConfigInventoryWire, ConfigLayerInputWire, ConfigSourceWire,
    ConfigValidateRequestWire, ConfigWritePlanWire,
    ListStrategy as ConfigListStrategy, CONFIG_WIRE_SCHEMA_VERSION,
};
pub use editor::{
    analyze_document as editor_analyze_document,
    apply_vcs_project_selection as editor_apply_vcs_project_selection,
    apply_vcs_ref_selection as editor_apply_vcs_ref_selection,
    apply_vcs_repo_selection as editor_apply_vcs_repo_selection,
    assist_entries_from_catalog as editor_assist_entries_from_catalog,
    build_directive_completion_candidates as editor_build_directive_completion_candidates,
    build_file_completion_candidates as editor_build_file_completion_candidates,
    build_file_completion_candidates_with_base as editor_build_file_completion_candidates_with_base,
    build_file_history_completion_candidates as editor_build_file_history_completion_candidates,
    build_snippet_completion_candidates as editor_build_snippet_completion_candidates,
    build_vcs_project_completion_candidates as editor_build_vcs_project_completion_candidates,
    build_vcs_ref_completion_candidates as editor_build_vcs_ref_completion_candidates,
    build_vcs_repo_completion_candidates as editor_build_vcs_repo_completion_candidates,
    build_xprompt_arg_name_candidates as editor_build_xprompt_arg_name_candidates,
    build_xprompt_completion_candidates as editor_build_xprompt_completion_candidates,
    canonical_directive_name as editor_canonical_directive_name,
    classify_completion_context as editor_classify_completion_context,
    classify_completion_context_with_workflows as editor_classify_completion_context_with_workflows,
    colon_args_skeleton as editor_colon_args_skeleton,
    definition_at_position as editor_definition_at_position,
    detect_vcs_ref_context_at_position as editor_detect_vcs_ref_context_at_position,
    detect_vcs_repo_context_at_position as editor_detect_vcs_repo_context_at_position,
    directive_argument_candidates as editor_directive_argument_candidates,
    directive_metadata as editor_directive_metadata,
    extract_token_at_position as editor_extract_token_at_position,
    frontmatter_field_schema as editor_frontmatter_field_schema,
    frontmatter_input_type_schema as editor_frontmatter_input_type_schema,
    hover_at_position as editor_hover_at_position,
    is_path_like_token as editor_is_path_like_token,
    is_slash_skill_like_token as editor_is_slash_skill_like_token,
    is_snippet_trigger_token as editor_is_snippet_trigger_token,
    is_vcs_project_trigger_token as editor_is_vcs_project_trigger_token,
    is_xprompt_like_token as editor_is_xprompt_like_token,
    named_args_skeleton as editor_named_args_skeleton,
    validate_frontmatter as editor_validate_frontmatter,
    validate_frontmatter_field as editor_validate_frontmatter_field,
    vcs_project_trigger_token as editor_vcs_project_trigger_token,
    CompletionCandidate, CompletionContext, CompletionContextKind,
    CompletionList, DefinitionTarget, DiagnosticSeverity, DirectiveMetadata,
    DocumentSnapshot, EditorDiagnostic, EditorPosition, EditorRange,
    EditorTextEdit, FrontmatterFieldKind, FrontmatterFieldSchema,
    FrontmatterInputType, HoverPayload, TokenInfo, VcsNamespaceEntry,
    VcsProjectEntry, VcsRefTrigger, VcsRepoCatalogRequest,
    VcsRepoCatalogResponse, VcsRepoEntry, VcsRepoTrigger, XpromptAssistEntry,
    XpromptInputHint, DIRECTIVES as EDITOR_DIRECTIVES,
    EDITOR_WIRE_SCHEMA_VERSION, VCS_REPO_CATALOG_SCHEMA_VERSION,
};
pub use effort::{is_valid_effort, split_model_effort, EFFORT_LEVELS_ORDERED};
pub use git_query::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z, GitNameStatusEntryWire,
    GIT_QUERY_WIRE_SCHEMA_VERSION,
};
pub use host_bridge::{
    split_command_words, CommandHelperHostBridge, DynHelperHostBridge,
    EditorSnippetCatalogRequestWire, EditorSnippetCatalogResponseWire,
    EditorSnippetCatalogStatsWire, EditorSnippetEntryWire,
    EditorXpromptCatalogEntryWire, EditorXpromptCatalogRequestWire,
    EditorXpromptCatalogResponseWire, EditorXpromptCatalogStatsWire,
    EditorXpromptInputWire, HelperHostBridge, HostBridgeError,
    MobileBeadDetailWire, MobileBeadListRequestWire,
    MobileBeadListResponseWire, MobileBeadShowRequestWire,
    MobileBeadShowResponseWire, MobileBeadSummaryWire,
    MobileChangeSpecTagEntryWire, MobileChangeSpecTagListRequestWire,
    MobileChangeSpecTagListResponseWire, MobileHelperProjectContextWire,
    MobileHelperProjectScopeWire, MobileHelperResultWire,
    MobileHelperSkippedWire, MobileHelperStatusWire, MobileUpdateJobStatusWire,
    MobileUpdateJobWire, MobileUpdateStartRequestWire,
    MobileUpdateStartResponseWire, MobileUpdateStatusRequestWire,
    MobileUpdateStatusResponseWire, MobileXpromptCatalogAttachmentWire,
    MobileXpromptCatalogEntryWire, MobileXpromptCatalogRequestWire,
    MobileXpromptCatalogResponseWire, MobileXpromptCatalogStatsWire,
    MobileXpromptInputWire, StaticHelperHostBridge,
    UnavailableHelperHostBridge,
};
pub use notifications::{
    append_notification, append_notification_counts,
    apply_notification_state_update, apply_notification_state_update_counts,
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
    resolve_pending_action_prefix, rewrite_notifications,
    rewrite_notifications_counts, ActionResultWire, HitlActionChoiceWire,
    HitlActionRequestWire, MobileActionDetailWire, MobileActionKindWire,
    MobileActionPlanErrorCodeWire, MobileActionPlanErrorWire,
    MobileActionStateWire, MobileActionSummaryWire, MobileAttachmentKindWire,
    MobileAttachmentManifestWire, MobileNotificationCardWire,
    MobileNotificationDetailResponseWire, MobileNotificationListRequestWire,
    MobileNotificationListResponseWire, NotificationAgentKeyWire,
    NotificationCountsWire, NotificationStateUpdateWire,
    NotificationStoreSnapshotWire, NotificationStoreStatsWire,
    NotificationUpdateOutcomeWire, NotificationWire, PendingActionIdentityWire,
    PendingActionPrefixResolutionWire, PendingActionStoreWire,
    PendingActionTransportWire, PendingActionWire, PlanActionChoiceWire,
    PlanActionRequestWire, QuestionActionChoiceWire, QuestionActionRequestWire,
    DEFAULT_PENDING_ACTION_PREFIX_LEN, DEFAULT_PENDING_ACTION_STALE_SECONDS,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
    PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
};
pub use parser::parse_project_bytes;
pub use plan::{
    read_plans, search_plans, PlanError, PlanSearchMatchWire, PlanWire,
    PLAN_READ_WIRE_SCHEMA_VERSION, PLAN_SEARCH_FIELD_NAMES,
    PLAN_SEARCH_WIRE_SCHEMA_VERSION, PLAN_WIRE_SCHEMA_VERSION,
};
pub use project_spec::{
    active_project_spec_filename, apply_project_aliases_update,
    apply_project_lifecycle_update, archive_project_spec_filename,
    is_archive_project_spec, legacy_active_project_spec_filename,
    legacy_archive_project_spec_filename, list_project_records,
    preferred_project_spec_path, project_spec_basename,
    read_project_lifecycle_from_content, to_active_project_spec_path,
    to_archive_project_spec_path, ProjectLifecycleError, ProjectLifecycleState,
    ProjectLifecycleWire, ProjectRecordWire, LEGACY_PROJECT_SPEC_EXTENSION,
    PROJECT_LIFECYCLE_WIRE_SCHEMA_VERSION, PROJECT_SPEC_ARCHIVE_SUFFIX,
    PROJECT_SPEC_EXTENSION,
};
pub use prompt_stash::{
    append_prompt_stash, pop_prompt_stash, read_prompt_stash_snapshot,
    rewrite_prompt_stash, set_prompt_stash_pinned, PromptStashEntryWire,
    PromptStashPopOutcomeWire, PromptStashSnapshotWire,
    PromptStashStoreStatsWire, PROMPT_STASH_WIRE_SCHEMA_VERSION,
};
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
pub use xprompt_catalog::{
    load_editor_snippet_catalog, load_editor_xprompt_catalog,
    XpromptCatalogLoadError, XpromptCatalogLoadOptions,
};

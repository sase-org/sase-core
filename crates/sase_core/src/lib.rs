//! Pure-Rust core for the sase Patch wire contract.
//!
//! Phase 1A modeled the wire types defined in `sase_100/src/sase/core/wire.py`
//! using owned data only. Phase 1B added a minimal full-file parser for
//! Patch boundaries and scalar fields. Phase 1C adds section parity:
//! structured `COMMITS`, `HOOKS`, `COMMENTS`, `MENTORS`, `TIMESTAMPS`, and
//! `DELTAS` entries that match the Python parser output for the golden
//! corpus. Patch/Stitch is now the canonical Rust contract while the legacy
//! ChangeSpec/Commit wire shape remains a compatibility boundary. The crate
//! is deliberately free of PyO3 types so later
//! UniFFI/WASM/server work can reuse the same logic.

pub mod agent_archive;
pub mod agent_clan_tribe;
pub mod agent_cleanup;
pub mod agent_family;
pub mod agent_group_archive;
pub mod agent_identity;
pub mod agent_launch;
pub mod agent_name_template;
pub mod agent_runtime;
pub mod agent_scan;
pub mod agent_stats;
pub mod artifact_consumption;
pub mod artifact_file;
pub mod artifact_link;
pub mod artifact_object_store;
pub mod artifact_ref;
pub mod axe_chop;
pub mod axe_overrun;
pub mod axe_status;
pub mod bead;
pub mod commit_footer;
pub mod commit_sha;
pub mod commit_subject;
pub mod config;
pub mod content_layout;
pub mod editor;
pub mod effort;
pub mod effort_override;
pub mod external_pr;
pub mod feature_flag_state;
pub mod fenced_code;
pub mod finalizer;
pub mod git_query;
pub mod glossary;
pub mod host_bridge;
pub mod machine_hood;
pub mod markdown_link_refs;
pub mod model_completion;
pub mod model_route;
pub mod notifications;
pub mod parser;
pub mod perf_logs;
pub mod plan;
pub mod procs;
pub mod project_spec;
pub mod prompt_artifact;
pub mod prompt_literals;
mod prompt_rewrite;
pub mod prompt_stash;
pub mod provider_disable;
pub mod query;
mod reference_path;
pub mod referenced_by;
pub mod runner_limit_override;
pub mod sections;
mod serde_option;
pub mod snippet_catalog;
pub mod snippet_session;
pub mod status;
mod store_lock;
pub mod suffix;
pub mod task_type;
pub mod telemetry;
pub mod vcs_log;
pub mod wire;
pub mod workspace_lease;
pub mod xprompt_catalog;
mod xprompt_text_block;

/// Return launch-inert literal zones as UTF-8 byte ranges.
///
/// The ranges cover fenced code blocks, disabled regions, and inline code
/// using the same composition as agent launch parsing. They may overlap or be
/// unsorted; consumers that require normalized ranges should normalize them
/// locally.
pub fn prompt_literal_zone_ranges(text: &str) -> Vec<(usize, usize)> {
    agent_launch::launch_literal_zone_ranges(text)
}

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
pub use agent_clan_tribe::{
    resolve_clan_summary, resolve_clan_tribe, ClanSummaryResolutionWire,
    ClanTribeMemberWire, ClanTribeResolutionRequestWire,
    ClanTribeResolutionWire,
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
    AgentCleanupMonitorStopIntentWire,
    AgentCleanupNotificationDismissIntentWire, AgentCleanupPlanWire,
    AgentCleanupRequestWire, AgentCleanupSideEffectsWire,
    AgentCleanupSkippedItemWire, AgentCleanupTargetWire,
    AgentCleanupWorkspaceReleaseIntentWire,
    AgentCleanupWorkspaceReleaseResultWire, AGENT_CLEANUP_WIRE_SCHEMA_VERSION,
    CLEANUP_MODE_DISMISS_COMPLETED, CLEANUP_MODE_KILL_AND_DISMISS,
    CLEANUP_MODE_PREVIEW_ONLY, CLEANUP_SCOPE_ALL_PANELS,
    CLEANUP_SCOPE_CUSTOM_SELECTION, CLEANUP_SCOPE_EXPLICIT_IDENTITIES,
    CLEANUP_SCOPE_FOCUSED_GROUP, CLEANUP_SCOPE_FOCUSED_PANEL,
    CLEANUP_SCOPE_TRIBE, CONFIRMATION_SEVERITY_DESTRUCTIVE,
    CONFIRMATION_SEVERITY_DISMISS, CONFIRMATION_SEVERITY_NONE, KILL_KIND_CRS,
    KILL_KIND_HOOK, KILL_KIND_MENTOR, KILL_KIND_MONITOR, KILL_KIND_RUNNING,
    KILL_KIND_WORKFLOW, SKIPPED_DUPLICATE, SKIPPED_NOT_DISMISSABLE,
    SKIPPED_NOT_IN_SCOPE, SKIPPED_NOT_KILLABLE, SKIPPED_UNKNOWN_KILL_KIND,
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
pub use agent_identity::{
    agent_link_target, agent_local_hood, agent_name_ancestors,
    agent_name_in_hood, classify_agent_ownership,
    classify_legacy_v1_group_ownership, globalize_agent_name,
    globalize_legacy_agent_name, localize_agent_name,
    normalize_agent_archive_name, parse_agent_family_name,
    rewrite_agent_relationship_batch, strip_global_agent_name,
    validate_agent_name, validate_agent_relationship_batch,
    validate_agent_username, AgentContainerKind, AgentFamilyNameWire,
    AgentIdentityError, AgentLinkTargetWire, AgentOwnerIdentity,
    AgentOwnershipClassification, AgentRelationshipBatchWire,
    AgentRelationshipError, AgentRelationshipKind, AgentRelationshipTargetWire,
    AgentRelationshipWire, AgentRunContainerWire, AgentRunWire,
    AgentSourceOwnerIdentity, LegacyV1GroupOwnershipClassification,
    LegacyV1GroupOwnershipEvidence, RewrittenAgentRelationshipBatchWire,
    RewrittenAgentRelationshipTargetWire, RewrittenAgentRelationshipWire,
    RewrittenAgentRunContainerWire, RewrittenAgentRunWire,
    ValidatedAgentRelationshipSummaryWire, AGENT_RELATIONSHIP_SCHEMA_VERSION,
};
pub use agent_launch::{
    admission_unit_results, agent_unit_dispatch_prompt,
    allocate_and_claim_workspace_from_content, allocate_launch_timestamp_batch,
    build_condition_context, classify_condition_status,
    cleanup_proc_private_inputs, condition_command_argv,
    condition_context_digest, decide_workspace_occupant_conflict,
    dispatch_fingerprint, evaluate_launch_condition,
    list_workspace_claims_from_content, next_admission_actions,
    parse_proc_duration_seconds, plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content, plan_typed_launch_units,
    prepare_agent_launch, prepare_proc_script, proc_script_argv,
    reconcile_admission_journal, resolve_proc_execution_cwd, safe_launch_name,
    sanitize_safe_inputs, sanitized_condition_env, sanitized_proc_env,
    summarize_admission, validate_proc_workspace_intent,
    validate_standalone_proc_shell_name, wait_target_key,
    AgentLaunchPreparationError, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, AgentUnitWire, ConditionCheckWire,
    ConditionContextWire, ConditionEvalRequestWire, ConditionEvalResultWire,
    ConditionLogicalUnitWire, ConditionWaitedOutcomeWire,
    LaunchAdmissionActionWire, LaunchAdmissionJournalEntryWire,
    LaunchAdmissionSummaryWire, LaunchAdmissionUnitStateWire,
    LaunchAdmissionWaitFactWire, LaunchConditionWire, LaunchFanoutPlanWire,
    LaunchFanoutSlotWire, LaunchOutcomeWire, LaunchPlanDiagnosticWire,
    LaunchPlanWire, LaunchUnitPayloadWire, LaunchUnitPhaseWire,
    LaunchUnitResultWire, LaunchUnitWire, OccupancyCallerWire,
    OccupancyConflictDecisionWire, OccupantRecordWire,
    ProcDispatchPreparedWire, ProcDispatchRequestWire, ProcUnitWire,
    TimestampBatchAllocationError, WaitTargetWire, WaitedOutcomeWire,
    WorkspaceClaimOutcomeWire, WorkspaceClaimPlanWire,
    WorkspaceClaimRequestWire, WorkspaceClaimWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION, CONDITION_CONTEXT_SCHEMA_VERSION,
    CONDITION_DEFAULT_TIMEOUT_SECONDS, CONDITION_EVAL_WIRE_SCHEMA_VERSION,
    CONDITION_MAX_TIMEOUT_SECONDS, CONDITION_OUTPUT_CAP_BYTES,
    LAUNCH_ADMISSION_JOURNAL_SCHEMA_VERSION, LAUNCH_PLAN_WIRE_SCHEMA_VERSION,
    PROC_DISPATCH_WIRE_SCHEMA_VERSION, PROC_PHASE_ACQUIRING_WORKSPACE,
    PROC_PHASE_CHECKING, PROC_PHASE_PREPARING_SCRIPT, PROC_PHASE_RUNNING,
    PROC_PHASE_SETTLING, PROC_PHASE_WAITING, XPROMPT_PROC_ORIGIN,
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
pub use agent_runtime::{
    aggregate_clan_runtime, aggregate_clan_runtime_records,
    ClanRuntimeMemberWire, ClanRuntimeWire,
};
pub use agent_scan::{
    agent_artifact_index_status, canonical_agent_artifact_path,
    collect_workflow_artifact_candidates, delete_agent_artifact_index_row,
    delete_agent_artifact_index_row_with_busy_timeout, is_artifact_timestamp,
    is_day_sharded_workflow, is_supported_workflow_dir,
    legacy_agent_artifact_path, parse_agent_artifact_path,
    parse_output_variable_selector,
    prune_hidden_terminal_agent_artifact_index_rows, query_agent_alias_history,
    query_agent_artifact_index, query_agent_output_variable_history,
    query_agent_output_variable_selectors, query_related_agent_artifact_dirs,
    read_agent_artifact_index_meta, rebuild_agent_artifact_index,
    replace_agent_artifact_index_dismissed_agents, resolve_agent_artifact_path,
    resolve_agent_artifact_timestamp_path, scan_agent_artifact_dir,
    scan_agent_artifact_dirs, scan_agent_artifacts,
    terminalize_stale_active_agent_artifact_index_rows,
    upsert_agent_artifact_index_row, write_agent_artifact_index_meta,
    AgentAliasHistoryGroupWire, AgentAliasHistoryLimitWire,
    AgentAliasHistoryQueryWire, AgentAliasHistoryWire, AgentAliasRunWire,
    AgentArtifactIndexQueryWire, AgentArtifactIndexStatusWire,
    AgentArtifactIndexUpdateWire, AgentArtifactPathInfo,
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire,
    AgentArtifactScanStatsWire, AgentArtifactScanWire, AgentClanContextWire,
    AgentMetaWire, AgentOutputVariableHistoryQueryWire,
    AgentOutputVariableHistoryWire, AgentOutputVariableKeyGroupWire,
    AgentOutputVariableLimitWire, AgentOutputVariableOccurrenceWire,
    AgentOutputVariableSelectorMatchWire, AgentOutputVariableSelectorQueryWire,
    AgentOutputVariableSelectorResultWire, AgentOutputVariableValueGroupWire,
    DoneMarkerWire, FamilyShellGateWire, FamilyShellMonitorWire,
    FamilyShellWire, OutputVariableSelectorError,
    OutputVariableSelectorPathWire, OutputVariableSelectorScopeWire,
    OutputVariableSelectorWire, OutputVariableValue, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, UsedXPromptWire,
    WaitingMarkerWire, WorkflowArtifactCandidate, WorkflowArtifactCandidates,
    WorkflowStateWire, WorkflowStepStateWire, ACE_RUN_WORKFLOW_DIR,
    AGENT_ALIAS_HISTORY_WIRE_SCHEMA_VERSION,
    AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
    AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION,
    AGENT_OUTPUT_VARIABLE_SELECTOR_WIRE_SCHEMA_VERSION,
    AGENT_SCAN_WIRE_SCHEMA_VERSION, DAY_SHARDED_LAYOUT_VERSION,
    DEFAULT_HIDDEN_TERMINAL_HOT_ROWS, DONE_WORKFLOW_DIR_NAMES,
    DONE_WORKFLOW_DIR_PREFIXES, LEGACY_LAYOUT_VERSION,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
/// Legacy Rust alias retained for compatibility with older stats callers.
pub use agent_stats::AgentChangeSpecWorkStatsWire;
pub use agent_stats::{
    query_activity_stats, query_run_stats, AgentActivityCountWire,
    AgentActivityStatsRequestWire, AgentActivityStatsResponseWire,
    AgentCommitDistributionWire, AgentCommitStatsWire, AgentPatchWorkStatsWire,
    AgentPlanActivityStatsWire, AgentPlanStatsWire, AgentProjectWorkStatsWire,
    AgentProviderStatsWire, AgentQuestionActivityStatsWire,
    AgentQuestionStatsWire, AgentRetryStatsWire, AgentRunBucketWire,
    AgentRunStatsRequestWire, AgentRunStatsResponseWire, AgentRunTotalsWire,
    AgentRunnerOccupancyWire, AgentRunnerStatsWire, AgentRunnerTrendSliceWire,
    AgentRuntimeGroupStatsWire, AgentStatsCountWire,
    AgentStatsDistributionWire, AgentStatsRuntimeGroupByWire,
    AgentWorkStatsWire, AgentWorkspaceStatsWire, AgentXPromptFocusWire,
    AgentXPromptStatsRowWire, AgentXPromptStatsWire,
    AGENT_STATS_WIRE_SCHEMA_VERSION,
};
pub use artifact_consumption::{
    consumed_artifact_file_refs, read_artifact_consumption_log,
    summarize_artifact_consumption, ArtifactConsumptionEventWire,
    ArtifactConsumptionSummaryWire,
    ARTIFACT_CONSUMPTION_LOG_MAX_SCHEMA_VERSION,
    ARTIFACT_CONSUMPTION_LOG_MIN_SCHEMA_VERSION,
    ARTIFACT_CONSUMPTION_WIRE_SCHEMA_VERSION,
};
pub use artifact_file::{
    artifact_file_is_vcs_backed, artifact_file_store_economics,
    list_artifact_file_trash, materialize_vcs_artifact_file,
    plan_artifact_file_retention, purge_artifact_file_trash,
    query_artifact_files, read_artifact_file_index,
    restore_artifact_file_trash, trash_artifact_file,
    ArtifactFileEconomicsGroupWire, ArtifactFileEconomicsOptionsWire,
    ArtifactFileEconomicsWire, ArtifactFileGenerationProjectionWire,
    ArtifactFileProtectedItemWire, ArtifactFileQueryError,
    ArtifactFileQueryFiltersWire, ArtifactFileRetentionCountsWire,
    ArtifactFileRetentionItemWire, ArtifactFileRetentionPlanWire,
    ArtifactFileRetentionPolicyWire, ArtifactFileTrashEntryWire,
    ArtifactFileTrashListWire, ArtifactFileTrashPurgeRequestWire,
    ArtifactFileTrashPurgeWire, ArtifactFileTrashRequestWire,
    ArtifactFileTrashRestoreRequestWire, ArtifactFileTrashRestoreWire,
    ArtifactFileVcsMaterializationRequestWire,
    ArtifactFileVcsMaterializationWire, ArtifactFileWire,
    ARTIFACT_FILE_INDEX_MAX_SCHEMA_VERSION,
    ARTIFACT_FILE_INDEX_MIN_SCHEMA_VERSION,
    ARTIFACT_FILE_LIFECYCLE_WIRE_SCHEMA_VERSION,
    ARTIFACT_FILE_QUERY_WIRE_SCHEMA_VERSION,
};
pub use artifact_link::{
    artifact_link_dedup_key, artifact_md_path, bead_lineage_root,
    bead_page_relpath, builtin_artifact_relations,
    canonicalize_artifact_link_ref, companion_md_path,
    lookup_artifact_relation, parse_artifact_link_frontmatter_inlet,
    parse_links_block, parse_managed_table_block,
    relation_label_from_perspective, remove_links_block, render_links_block,
    render_managed_table_block, reserved_artifact_relation_slugs,
    strip_links_block, upsert_artifact_link_row, upsert_links_block,
    upsert_managed_table_block, validate_artifact_link_description,
    validate_artifact_link_row, ArtifactCompanionPathWire,
    ArtifactLinkAggregateWire, ArtifactLinkDedupKeyWire, ArtifactLinkError,
    ArtifactLinkFrontmatterInletKindWire, ArtifactLinkFrontmatterInletWire,
    ArtifactLinkIndexWire, ArtifactLinkInletEntryWire, ArtifactLinkOriginWire,
    ArtifactLinkRowWire, ArtifactLinkUpsertKindWire, ArtifactLinkUpsertWire,
    ArtifactMdPathKindWire, ArtifactMdPathRequestWire, ArtifactMdPathWire,
    ArtifactRelationWire, BeadLinkDirectionWire, BeadLinkWire,
    ManagedTableAnchorWire, ManagedTableBlock, ManagedTableColumnWire,
    ManagedTableDocumentWire, ManagedTableRowWire, ManagedTableTableWire,
    ARTIFACT_LINK_INLET_WIRE_SCHEMA_VERSION, ARTIFACT_LINK_ROW_SCHEMA_VERSION,
    ARTIFACT_MD_PATH_WIRE_SCHEMA_VERSION,
    ARTIFACT_RELATION_WIRE_SCHEMA_VERSION, LINKS_BLOCK_END_MARKER,
    LINKS_BLOCK_HEADING, LINKS_BLOCK_START_MARKER,
    LINKS_BLOCK_WIRE_SCHEMA_VERSION, MAX_RENDERED_MANAGED_TABLE_ROWS,
    RESERVED_ARTIFACT_RELATION_SLUGS,
};
pub use artifact_object_store::{
    artifact_object_prompt_link, artifact_object_relpath,
    ARTIFACT_OBJECT_STORE_DIR,
};
pub use artifact_ref::{
    canonicalize_artifact_ref, filter_artifact_ref_path_payloads,
    fold_artifact_ref_files, normalize_artifact_ref_list, parse_artifact_ref,
    parse_artifact_ref_file_index, parse_artifact_ref_list,
    render_artifact_ref, render_artifact_ref_file_row, resolve_artifact_ref,
    resolve_artifact_ref_list, scan_artifact_refs,
    validate_artifact_ref_context, validate_artifact_ref_file_row,
    ArtifactFileSourceWire, ArtifactRefAgentOwnerWire,
    ArtifactRefAgentRootWire, ArtifactRefBeadStoreWire, ArtifactRefContextWire,
    ArtifactRefDocumentRootWire, ArtifactRefError, ArtifactRefFileRootWire,
    ArtifactRefFileVersionRowWire, ArtifactRefFileVersionWire,
    ArtifactRefFragmentWire, ArtifactRefKindWire, ArtifactRefListEntryWire,
    ArtifactRefListResolutionWire, ArtifactRefLogicalFileWire,
    ArtifactRefPathFilterBatchWire, ArtifactRefPayloadWire,
    ArtifactRefProjectWire, ArtifactRefPromptCandidateWire,
    ArtifactRefRepositoryWire, ArtifactRefResolutionWire, ArtifactRefSpanWire,
    ParsedArtifactRefWire, ARTIFACT_REF_CONTEXT_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_FILE_INDEX_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_LIST_RESOLUTION_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_PARSE_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_PATH_FILTER_WIRE_SCHEMA_VERSION,
    ARTIFACT_REF_RESOLUTION_WIRE_SCHEMA_VERSION,
};
/// Legacy Rust alias retained for compatibility with older AXE callers.
pub use axe_chop::ChopChangespecSnapshotWire;
pub use axe_chop::{
    apply_checkpoint_update, check_and_record_once_per, derive_chop_agent_name,
    evaluate_chop_decision, expand_chop_targets, parse_chop_duration,
    parse_chop_result, release_chop_once_per, split_axe_description,
    validate_axe_config, validate_chop_proposal, validate_chop_result,
    AxeConfigValidationRequestWire, ChopAgentSnapshotWire,
    ChopCheckpointDocumentWire, ChopCheckpointEntryWire,
    ChopCheckpointEventWire, ChopCheckpointPolicyWire,
    ChopCheckpointUpdateRequestWire, ChopDecisionRequestWire, ChopDecisionWire,
    ChopEngineError, ChopForEachConfigWire, ChopGitSnapshotWire,
    ChopGuardConfigWire, ChopLaunchProposalWire, ChopOncePerConfigWire,
    ChopOncePerDecisionWire, ChopOncePerReleaseRequestWire,
    ChopOncePerReleaseWire, ChopOncePerRequestWire, ChopResultDocumentWire,
    ChopResultStatusWire, ChopSeenEntryWire, ChopSeenStoreDocumentWire,
    ChopTargetExpansionRequestWire, ChopTargetExpansionWire,
    ChopTargetInstanceWire, ChopTargetSourceFiltersWire, ChopTriggerConfigWire,
    ProposalWaitOnWire, CHOP_ENGINE_SCHEMA_VERSION, CHOP_RESULT_SCHEMA_VERSION,
    CHOP_STATE_SCHEMA_VERSION,
};
pub use axe_status::{
    classify_axe_status, AxeDesiredStateValueWire, AxeDesiredStateWire,
    AxeLifecycleEventKindWire, AxeLifecycleEventWire,
    AxeLumberjackObservationWire, AxeLumberjackReportedStateWire,
    AxeLumberjackStateWire, AxeLumberjackStatusWire, AxeMaintenanceWire,
    AxeOrchestratorCoherenceWire, AxeOrchestratorObservationWire,
    AxeOrchestratorStateWire, AxeOrchestratorStatusWire,
    AxeProcessObservationWire, AxeRunnerOccupancyWire,
    AxeStatusCollectionErrorWire, AxeStatusError, AxeStatusHealthWire,
    AxeStatusIssueSeverityWire, AxeStatusIssueWire, AxeStatusRequestWire,
    AxeStatusSnapshotWire, AxeStatusStateWire, AXE_STATUS_SCHEMA_VERSION,
};
pub use bead::{
    add_bead_link, add_bead_references, add_dependency as bead_add_dependency,
    bead_history, blocked_issues as bead_blocked_issues,
    cancel_task_snooze as bead_cancel_task_snooze,
    changespec_metadata_migration_sql,
    claim_for_agent_launch as bead_claim_for_agent_launch,
    close_issues as bead_close_issues,
    close_issues_with_note as bead_close_issues_with_note,
    create_issue as bead_create_issue, default_config, doctor as bead_doctor,
    doctor_report as bead_doctor_report,
    doctor_report_with_contexts as bead_doctor_report_with_contexts,
    doctor_with_contexts as bead_doctor_with_contexts,
    doctor_with_plan_roots as bead_doctor_with_plan_roots,
    drop_flag_type_migration_sql, export_issues_to_jsonl,
    export_jsonl as bead_export_jsonl, external_ref_migration_sql,
    flag_type_migration_sql, get_epic_children as bead_get_epic_children,
    import_issues_from_jsonl, import_issues_to_event_streams,
    init_store as bead_init_store, is_ready_to_work_migration_sql,
    issue_type_migration_sql, list_issues as bead_list_issues, load_config,
    load_config_from_str, mark_ready_to_work as bead_mark_ready_to_work,
    merge_bead_event_streams, merge_bead_event_streams_with_relocation,
    missing_changespec_metadata_columns, model_migration_sql,
    needs_drop_flag_type_migration, needs_external_ref_migration,
    needs_flag_type_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, needs_model_migration, needs_refs_migration,
    needs_size_check_relax_migration, needs_size_migration,
    needs_snoozed_status_migration, needs_task_ready_migration,
    needs_task_type_migration, notes_text, open_issue as bead_open_issue,
    parse_issues_jsonl, parse_snooze_timestamp,
    preclaim_epic_work_plan as bead_preclaim_epic_work_plan,
    prune_removed_flag_event_streams,
    read_event_store_issues as bead_read_event_store_issues,
    read_legacy_jsonl_issues as bead_read_legacy_jsonl_issues,
    read_store_issues, ready_issues as bead_ready_issues, reduce_event_streams,
    reference_diagnostics, refs_migration_sql, remove_bead_link,
    remove_bead_references, remove_issue as bead_remove_issue,
    repair_event_store_manifest, save_config,
    search_issues as bead_search_issues, show_issue as bead_show_issue,
    show_issue_detail as bead_show_issue_detail,
    show_issue_detail_with_options as bead_show_issue_detail_with_options,
    size_check_relax_migration_sql, size_migration_sql,
    snooze_task as bead_snooze_task, snoozed_status_migration_sql,
    stats as bead_stats, sync_is_clean as bead_sync_is_clean,
    task_ready_migration_sql, task_type_migration_sql,
    unmark_ready_to_work as bead_unmark_ready_to_work,
    update_issue as bead_update_issue, update_issues as bead_update_issues,
    BeadCloseRecordWire, BeadConfigWire, BeadCreateRequestWire,
    BeadDoctorReportWire, BeadError, BeadEventManifestRepairOutcomeWire,
    BeadEventManifestRepairStatusWire, BeadEventOperationWire,
    BeadEventPayloadWire, BeadEventRecordWire, BeadEventStoreManifestWire,
    BeadEventStreamMergeWire, BeadEventStreamWire, BeadHistoryChangeWire,
    BeadHistoryEntryWire, BeadHistoryWire, BeadIdRelocationKindWire,
    BeadIdRelocationWire, BeadIssueDetailWire, BeadIssueUpdateEventFieldsWire,
    BeadMutationOutcomeWire, BeadNoteWire, BeadPreclaimAssignmentWire,
    BeadPreclaimRollbackWire, BeadProjectionDriftWire, BeadReopenCauseWire,
    BeadResolutionWire, BeadSearchMatchWire, BeadSnoozeWakeCauseWire,
    BeadSnoozeWire, BeadTierWire, BeadUpdateFieldsWire, DependencyWire,
    IssueTypeWire, IssueWire, JsonlLoadOutcome, PhaseSizeWire,
    RemovedFlagStreamPruneOutcomeWire, StatusWire, BEAD_EVENT_SCHEMA_VERSION,
    BEAD_HISTORY_WIRE_SCHEMA_VERSION, BEAD_READ_WIRE_SCHEMA_VERSION,
    BEAD_SEARCH_FIELD_NAMES, BEAD_SQLITE_SCHEMA,
};
pub use commit_footer::{
    parse_commit_footer, update_commit_footer, CommitFooterReferenceWire,
    CommitFooterTagWire, CommitFooterUpdateWire, CommitFooterWire,
    COMMIT_FOOTER_WIRE_SCHEMA_VERSION,
};
pub use commit_sha::commit_shas_equivalent;
pub use commit_subject::{
    default_commit_subject_types, parse_commit_subject, CommitSubjectWire,
    COMMIT_SUBJECT_WIRE_SCHEMA_VERSION,
};
pub use config::{
    compose_axe_config, config_field_model, config_inventory, config_plan_edit,
    config_validate, plan_axe_entry_mutation, AxeConfigComposeRequestWire,
    AxeConfigCompositionWire, AxeEntryMutationPlanWire,
    AxeEntryMutationRequestWire, AxeEntryPreviewWire, AxeEntrySelectorWire,
    AxeFieldOperationWire, AxeFieldProvenanceWire, AxeInventoryEntryWire,
    AxeRawContributionWire, ConfigConstraintsWire, ConfigContributionWire,
    ConfigDiagnosticWire, ConfigEditOpWire, ConfigEditPlanWire,
    ConfigEditRequestWire, ConfigEffectivePreviewWire, ConfigError,
    ConfigFieldModelWire, ConfigFieldStateWire, ConfigFieldWire,
    ConfigInventoryRequestWire, ConfigInventoryWire, ConfigLayerInputWire,
    ConfigSourceWire, ConfigValidateRequestWire, ConfigWritePlanWire,
    ListStrategy as ConfigListStrategy, CONFIG_WIRE_SCHEMA_VERSION,
};
pub use content_layout::{
    is_invokable_memory_stem, is_reserved_memory_reference, memory_note_issue,
    memory_reference_name, memory_reference_stem,
    reserved_memory_namespace_issue, resolve_layout_candidates,
    sase_content_layout, skill_placement_issue, skill_reference_name,
    split_skill_reference_name, ChezmoiContentLayoutWire,
    CompatibleLayoutPathWire, HomeContentLayoutWire,
    LayoutCandidateResolutionWire, LayoutCollisionPolicyWire,
    LayoutPathRoleWire, LayoutPathWire, LayoutTrackingWire, MemorySourceWire,
    MemoryTierWire, MemoryXpromptIssueWire, MemoryXpromptRuleWire,
    ProjectContentLayoutWire, SaseContentLayoutWire, SkillPlacementIssueWire,
    SkillPlacementRuleWire, SkillSourceWire, XpromptSourceWire,
    CONTENT_LAYOUT_SCHEMA_VERSION, MEMORY_NAMESPACE_SEGMENT,
    MEMORY_README_FILENAME, REF_DIRECTORY_SEGMENT, SKILL_DIRECTORY_SEGMENT,
    SKILL_NAMESPACE_SEGMENT,
};
pub use editor::{
    analyze_artifact_refs as editor_analyze_artifact_refs,
    analyze_document as editor_analyze_document,
    apply_vcs_project_selection as editor_apply_vcs_project_selection,
    apply_vcs_ref_selection as editor_apply_vcs_ref_selection,
    apply_vcs_repo_selection as editor_apply_vcs_repo_selection,
    assist_entries_from_catalog as editor_assist_entries_from_catalog,
    build_agent_completion_candidates as editor_build_agent_completion_candidates,
    build_artifact_ref_kind_completion_candidates as editor_build_artifact_ref_kind_completion_candidates,
    build_artifact_ref_payload_completion_candidates as editor_build_artifact_ref_payload_completion_candidates,
    build_artifact_ref_payload_inventory as editor_build_artifact_ref_payload_inventory,
    build_at_reference_menu as editor_build_at_reference_menu,
    build_at_reference_menu_with_options as editor_build_at_reference_menu_with_options,
    build_at_reference_menu_with_payload_index as editor_build_at_reference_menu_with_payload_index,
    build_bead_completion_candidates as editor_build_bead_completion_candidates,
    build_directive_clause_candidates as editor_build_directive_clause_candidates,
    build_directive_completion_candidates as editor_build_directive_completion_candidates,
    build_directive_completion_candidates_with_flags as editor_build_directive_completion_candidates_with_flags,
    build_directive_keyword_candidates as editor_build_directive_keyword_candidates,
    build_file_completion_candidates as editor_build_file_completion_candidates,
    build_file_completion_candidates_with_base as editor_build_file_completion_candidates_with_base,
    build_file_history_completion_candidates as editor_build_file_history_completion_candidates,
    build_identity_target_candidates as editor_build_identity_target_candidates,
    build_placeholder_completion_candidates as editor_build_placeholder_completion_candidates,
    build_snippet_completion_candidates as editor_build_snippet_completion_candidates,
    build_vcs_project_completion_candidates as editor_build_vcs_project_completion_candidates,
    build_vcs_ref_completion_candidates as editor_build_vcs_ref_completion_candidates,
    build_vcs_repo_completion_candidates as editor_build_vcs_repo_completion_candidates,
    build_wait_completion_candidates as editor_build_wait_completion_candidates,
    build_wait_completion_candidates_for_form as editor_build_wait_completion_candidates_for_form,
    build_xprompt_arg_name_candidates as editor_build_xprompt_arg_name_candidates,
    build_xprompt_completion_candidates as editor_build_xprompt_completion_candidates,
    canonical_directive_name as editor_canonical_directive_name,
    classify_completion_context as editor_classify_completion_context,
    classify_completion_context_with_artifacts_and_workflows as editor_classify_completion_context_with_artifacts_and_workflows,
    classify_completion_context_with_workflows as editor_classify_completion_context_with_workflows,
    colon_args_skeleton as editor_colon_args_skeleton,
    compare_fuzzy as editor_compare_fuzzy,
    definition_at_position as editor_definition_at_position,
    detect_artifact_ref_context_at_position as editor_detect_artifact_ref_context_at_position,
    detect_at_reference_context as editor_detect_at_reference_context,
    detect_directive_context_at_position as editor_detect_directive_context_at_position,
    detect_placeholder_context_at_position as editor_detect_placeholder_context_at_position,
    detect_vcs_ref_context_at_position as editor_detect_vcs_ref_context_at_position,
    detect_vcs_repo_context_at_position as editor_detect_vcs_repo_context_at_position,
    directive_allows_keywords as editor_directive_allows_keywords,
    directive_argument_candidates as editor_directive_argument_candidates,
    directive_contract as editor_directive_contract,
    directive_is_hidden_from_name_completion as editor_directive_is_hidden_from_name_completion,
    directive_is_hidden_from_name_completion_with_flags as editor_directive_is_hidden_from_name_completion_with_flags,
    directive_metadata as editor_directive_metadata,
    directive_snippet_recipes as editor_directive_snippet_recipes,
    extract_placeholder_spans as editor_extract_placeholder_spans,
    extract_token_at_position as editor_extract_token_at_position,
    frontmatter_field_schema as editor_frontmatter_field_schema,
    frontmatter_input_type_schema as editor_frontmatter_input_type_schema,
    fuzzy_match as editor_fuzzy_match,
    hover_at_position as editor_hover_at_position,
    is_path_like_token as editor_is_path_like_token,
    is_slash_skill_like_token as editor_is_slash_skill_like_token,
    is_snippet_trigger_token as editor_is_snippet_trigger_token,
    is_vcs_project_trigger_token as editor_is_vcs_project_trigger_token,
    is_xprompt_like_token as editor_is_xprompt_like_token,
    named_args_skeleton as editor_named_args_skeleton,
    placeholder_input_names as editor_placeholder_input_names,
    rank_and_filter_bead_entries as editor_rank_and_filter_bead_entries,
    raw_placeholder_fields as editor_raw_placeholder_fields,
    substitute_raw_placeholders as editor_substitute_raw_placeholders,
    typed_launch_directive_diagnostics as editor_typed_launch_directive_diagnostics,
    validate_frontmatter as editor_validate_frontmatter,
    validate_frontmatter_field as editor_validate_frontmatter_field,
    vcs_project_trigger_token as editor_vcs_project_trigger_token,
    AgentCatalogRequest, AgentCatalogResponse, AgentCompletionEntry,
    ArtifactRefCompletionMode, ArtifactRefCompletionTrigger,
    AtReferenceContextWire, AtReferenceGroup, AtReferenceInventoryWire,
    AtReferenceKindRowWire, AtReferenceMenuOptionsWire, AtReferenceMenuWire,
    AtReferencePathQueryWire, AtReferencePathRowWire, AtReferencePayloadIndex,
    AtReferencePayloadRowWire, AtReferenceRowWire, AtReferenceStage,
    BeadCompletionEntry, CompletionCandidate, CompletionContext,
    CompletionContextKind, CompletionList, DefinitionTarget,
    DiagnosticSeverity, DirectiveBodyKind, DirectiveClauseContext,
    DirectiveClauseKind, DirectiveCompletionInventories,
    DirectiveContractEntry, DirectiveFinalizerEntry, DirectiveMetadata,
    DirectiveModelAliasKey, DirectiveModelEntry,
    DirectiveSnippetRecipeContract, DirectiveSyntaxForm, DirectiveValueRole,
    DocumentSnapshot, EditorDiagnostic, EditorPosition, EditorRange,
    EditorTextEdit, FinalizerCatalogRequest, FinalizerCatalogResponse,
    FrontmatterFieldKind, FrontmatterFieldSchema, FrontmatterInputType,
    FuzzyMatch, HoverPayload, PlaceholderCandidate, PlaceholderCandidateSource,
    PlaceholderCompletion, PlaceholderContext, PlaceholderSpan,
    RawPlaceholderField, TokenInfo, VcsNamespaceEntry, VcsProjectEntry,
    VcsRefTrigger, VcsRepoCatalogRequest, VcsRepoCatalogResponse, VcsRepoEntry,
    VcsRepoTrigger, XpromptAssistEntry, XpromptInputHint,
    AGENT_CATALOG_SCHEMA_VERSION, AT_REFERENCE_MAX_GROUP_ROWS,
    BEAD_COMPLETION_LIMIT, DIRECTIVES as EDITOR_DIRECTIVES,
    EDITOR_WIRE_SCHEMA_VERSION, FINALIZER_CATALOG_SCHEMA_VERSION,
    PLACEHOLDER_MAX_INNER_CHARS, VCS_REPO_CATALOG_SCHEMA_VERSION,
};
pub use effort::{
    is_valid_effort, resolve_effective_effort, split_model_effort,
    EffectiveEffortResolutionWire, EffectiveEffortSource,
    EFFORT_LEVELS_ORDERED,
};
pub use effort_override::{
    clear_effort_override, effort_override_state_path, get_effort_override,
    set_effort_override_relative, set_effort_override_until,
    EffortOverrideError, EffortOverrideWire, EFFORT_OVERRIDE_STATE_FILENAME,
    EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION,
};
pub use external_pr::{
    canonical_pull_request_url, plan_external_pr_import,
    CanonicalPullRequestUrl, ExternalPrImportPlanWire,
    ExternalPrImportRequestWire, LocalPatchWire, RemotePullRequestWire,
    EXTERNAL_PR_WIRE_SCHEMA_VERSION,
};
pub use feature_flag_state::{
    feature_flag_state_get, feature_flag_state_path, feature_flag_state_set,
    FeatureFlagStateDiagnosticWire, FeatureFlagStateError,
    FeatureFlagStateSetOutcomeWire, FeatureFlagStateSnapshotWire,
    FEATURE_FLAG_STATE_FILENAME, FEATURE_FLAG_STATE_MAX_BYTES,
    FEATURE_FLAG_STATE_WIRE_SCHEMA_VERSION,
};
pub use fenced_code::{
    fenced_block_details, fenced_block_details_wire, fenced_block_ranges,
    language_from_info_string, scan_directive_owned_fences,
    typed_launch_units_flag_key, CodeDirectiveDiagnosticWire,
    CodeDirectiveScanWire, CodeDirectiveSpanWire, CodeLanguage, CodeValue,
    CodeValueWire, FencedBlock, FencedBlockWire,
    CODE_VALUE_WIRE_SCHEMA_VERSION,
};
pub use finalizer::{
    aggregate_finalizer_outcomes, authenticate_finalizer_plan,
    canonical_json_bytes, canonical_json_sha256, finalizer_context_digest,
    finalizer_digest_json_value, finalizer_digest_serializable,
    finalizer_instance_spec_digest, finalizer_plan_digest,
    finalizer_provider_spec_digest, resolve_finalizer_plan,
    validate_finalizer_context, validate_finalizer_deferral,
    validate_finalizer_instance_results, validate_finalizer_instance_spec,
    validate_finalizer_plan, validate_finalizer_provider_spec,
    validate_finalizer_submission, FinalizerAggregateResultWire,
    FinalizerAggregateStatusWire, FinalizerAttemptWire, FinalizerContextWire,
    FinalizerDeferralReasonWire, FinalizerDeferralWire,
    FinalizerDiagnosticSeverityWire, FinalizerDiagnosticWire, FinalizerError,
    FinalizerInstancePolicyWire, FinalizerInstanceResultWire,
    FinalizerInstanceSpecWire, FinalizerInstanceStatusWire,
    FinalizerObligationWire, FinalizerOutcomeEvidenceWire,
    FinalizerPayloadRequirementWire, FinalizerPlanEntryWire,
    FinalizerPlanInputWire, FinalizerPlanWire, FinalizerProviderCapabilityWire,
    FinalizerProviderSpecWire, FinalizerRefusalPolicyWire,
    FinalizerSelectorOpWire, FinalizerSubmissionEnvelopeWire,
    FinalizerSubmissionPayloadWire, FinalizerSubmissionValidationWire,
    FinalizerTriggerKindWire, FINALIZER_WIRE_SCHEMA_VERSION,
};
pub use git_query::{
    derive_git_workspace_name, parse_git_branch_name,
    parse_git_conflicted_files, parse_git_local_changes,
    parse_git_name_status_z, GitNameStatusEntryWire,
    GIT_QUERY_WIRE_SCHEMA_VERSION,
};
pub use glossary::{
    build_glossary_catalog, compile_glossary_catalog,
    validate_glossary_entries, CompiledGlossaryCatalog, GlossaryCatalogWire,
    GlossaryDiagnosticWire, GlossaryEntryWire, GlossaryError,
    GlossaryInputEntryWire, GlossarySegmentWire, GlossarySourceWire,
    GlossarySpanWire, GLOSSARY_WIRE_SCHEMA_VERSION,
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
    MobileHelperSkippedWire, MobileHelperStatusWire, MobileInputChoiceWire,
    MobileUpdateJobStatusWire, MobileUpdateJobWire,
    MobileUpdateStartRequestWire, MobileUpdateStartResponseWire,
    MobileUpdateStatusRequestWire, MobileUpdateStatusResponseWire,
    MobileXpromptCatalogAttachmentWire, MobileXpromptCatalogEntryWire,
    MobileXpromptCatalogRequestWire, MobileXpromptCatalogResponseWire,
    MobileXpromptCatalogStatsWire, MobileXpromptInputWire,
    StaticHelperHostBridge, UnavailableHelperHostBridge,
};
pub use machine_hood::{
    machine_hood_of, qualify_machine_agent_name, strip_machine_agent_name,
    validate_machine_name, MachineNameError,
};
pub use markdown_link_refs::{
    allocate_markdown_reference_label, append_markdown_reference_definitions,
    scan_markdown_reference_links, MarkdownReferenceDefinitionWire,
    MarkdownReferenceScanWire, MARKDOWN_LINK_REFS_WIRE_SCHEMA_VERSION,
};
pub use model_completion::{
    filter_model_completion_candidates, filter_model_completion_entries,
    ModelCompletionCandidateWire, ModelCompletionEntryWire,
    MODEL_COMPLETION_ENTRY_WIRE_FIELDS,
};
pub use model_route::{
    public_size_alias, select_epic_land_model, size_model_route,
    size_model_route_from_name, EpicLandModelRouteWire, EpicLandModelSource,
    ModelRouteError, SizeModelRouteWire, PUBLIC_SIZE_ALIASES,
    PUBLIC_SIZE_ALIAS_NAMES,
};
pub use notifications::{
    append_notification, append_notification_counts,
    apply_notification_state_update, apply_notification_state_update_counts,
    cleanup_stale_pending_actions, current_unix_time,
    legacy_telegram_pending_actions_path,
    mobile_action_detail_from_notification,
    mobile_attachment_manifest_from_path, mobile_notification_card_from_wire,
    notification_activity_at, notification_activity_cursor,
    pending_action_from_notification, pending_action_identity,
    pending_action_state_for_notification, pending_action_state_from_store,
    pending_action_store_path, plan_question_action_response,
    plan_question_action_response_from_bytes,
    read_current_notifications_snapshot, read_notifications_snapshot,
    read_notifications_snapshot_with_options, read_pending_action_store,
    register_pending_action, resolve_notification_prefix,
    resolve_pending_action_prefix, rewrite_notifications,
    rewrite_notifications_counts, ActionResultWire, GateActionRequestWire,
    GateBranchWire, GateFeedbackModeWire, GateOptionWire, GateSubmitWire,
    MobileActionDetailWire, MobileActionKindWire,
    MobileActionPlanErrorCodeWire, MobileActionPlanErrorWire,
    MobileActionStateWire, MobileActionSummaryWire, MobileAttachmentKindWire,
    MobileAttachmentManifestWire, MobileGateInputFieldWire,
    MobileNotificationCardWire, MobileNotificationDetailResponseWire,
    MobileNotificationListRequestWire, MobileNotificationListResponseWire,
    NotificationAgentKeyWire, NotificationCountsWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationStoreStatsWire, NotificationUpdateOutcomeWire,
    NotificationWire, PendingActionIdentityWire,
    PendingActionPrefixResolutionWire, PendingActionStoreWire,
    PendingActionTransportWire, PendingActionWire, QuestionActionChoiceWire,
    QuestionActionRequestWire, DEFAULT_PENDING_ACTION_PREFIX_LEN,
    DEFAULT_PENDING_ACTION_STALE_SECONDS,
    MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
    NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
    PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
};
pub use parser::{parse_patch_project_bytes, parse_project_bytes};
pub use perf_logs::{
    perf_logs_query, PerfAgentLoadStageWire, PerfAgentLoadsSnapshotWire,
    PerfCountWire, PerfExternalToolWaitsSnapshotWire, PerfGitOpsSnapshotWire,
    PerfLaunchStageWire, PerfLaunchesSnapshotWire, PerfLogCoverageWire,
    PerfLogSourceIdWire, PerfLogSourceWire, PerfLogsQueryWire,
    PerfLogsSnapshotWire, PerfNumericSummaryWire, PerfStageSummaryWire,
    PerfStallEventStatsWire, PerfStallsSnapshotWire,
    PerfStartupSeriesPointWire, PerfStartupSessionWire,
    PerfStartupSnapshotWire, PERF_LOGS_WIRE_SCHEMA_VERSION,
};
pub use plan::{
    canonicalize_plan_reference, parse_plan_reference, parse_sdd_artifact_link,
    plan_frontmatter_schema, plan_validate, plan_validate_with_mode,
    read_plans, render_plan_reference, render_sdd_artifact_link,
    resolve_plan_reference, sdd_plan_header_block_span, search_plans,
    upsert_sdd_artifact_link, ParsedPlanReference, PlanDiagnosticWire,
    PlanError, PlanFrontmatterFieldSpecWire, PlanPhaseWire,
    PlanReferenceResolutionWire, PlanSearchMatchWire, PlanValidationMode,
    PlanValidationResultWire, PlanWire, SddArtifactDocumentWire,
    SddArtifactLinkKindWire, SddArtifactLinkTypeWire,
    SddLegacyArtifactLinkWire, ValidatedPlanWire,
    PLAN_READ_WIRE_SCHEMA_VERSION,
    PLAN_REFERENCE_RESOLUTION_WIRE_SCHEMA_VERSION, PLAN_SEARCH_FIELD_NAMES,
    PLAN_SEARCH_WIRE_SCHEMA_VERSION, PLAN_WIRE_SCHEMA_VERSION,
};
pub use procs::{
    append_proc, begin_proc_settlement, claim_proc_supervisor, finish_proc,
    prune_procs, read_procs_snapshot, request_proc_stop, reserve_proc,
    update_proc, ProcAppendOutcomeWire, ProcFinishWire, ProcPruneOutcomeWire,
    ProcReserveOutcomeWire, ProcReserveWire, ProcSettlementWire,
    ProcStopRequestWire, ProcStoreError, ProcStoreSnapshotWire,
    ProcStoreStatsWire, ProcSupervisorClaimWire, ProcUpdateOutcomeWire,
    ProcUpdateWire, ProcWire, XpromptProcMetaWire, PROC_WIRE_SCHEMA_VERSION,
    SUPPORTED_PROC_WIRE_SCHEMA_VERSIONS,
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
pub use prompt_literals::inline_code_ranges;
pub use prompt_stash::{
    append_prompt_stash, pop_prompt_stash, read_prompt_stash_snapshot,
    rewrite_prompt_stash, set_prompt_stash_pinned, PromptStashCursorWire,
    PromptStashEntryWire, PromptStashPopOutcomeWire, PromptStashSnapshotWire,
    PromptStashStoreError, PromptStashStoreStatsWire,
    PROMPT_STASH_WIRE_SCHEMA_VERSION,
};
pub use provider_disable::{
    clear_provider_disable, get_provider_disables, provider_disable_state_path,
    set_provider_disable_relative, set_provider_disable_until,
    try_set_provider_disable_relative, try_set_provider_disable_until,
    ProviderDisableError, ProviderDisableMode, ProviderDisableSnapshotWire,
    ProviderDisableWire, ProviderDisableWriteOutcomeWire,
    PROVIDER_DISABLE_STATE_FILENAME, PROVIDER_DISABLE_WIRE_SCHEMA_VERSION,
};
pub use query::{
    canonicalize_query, canonicalize_query_with_profile, compile_query,
    compile_query_with_profile, effective_project_name, evaluate_query_many,
    evaluate_query_many_in_corpus, evaluate_query_one, get_base_status,
    get_searchable_text, has_any_status_suffix, parse_query,
    parse_query_with_profile, patch_query_profile, patch_rows_from_specs,
    project_dir_name, strip_reverted_suffix, tokenize_query,
    tokenize_query_with_profile, try_evaluate_query_many_in_corpus,
    CompiledQueryProfile, FieldValueKind, QueryCorpus, QueryErrorWire,
    QueryEvaluationContext, QueryExprWire, QueryFieldSpec, QueryFieldValues,
    QueryMacroSpec, QueryPredicateFacts, QueryProgram, QueryProgramWire,
    QueryRow, QuerySigilSpec, QueryTokenKind, QueryTokenWire,
};
pub use referenced_by::{
    parse_referenced_by_block, remove_referenced_by_block,
    render_referenced_by_block, strip_referenced_by_block,
    upsert_referenced_by_block, ReferencedByColumnWire,
    ReferencedByDocumentWire, ReferencedByRowWire, ReferencedByTableWire,
    MAX_RENDERED_REFERENCED_BY_ROWS, REFERENCED_BY_BLOCK_WIRE_SCHEMA_VERSION,
};
pub use runner_limit_override::{
    clear_runner_limit_override, get_runner_limit_override,
    runner_limit_override_state_path, set_runner_limit_override_relative,
    set_runner_limit_override_until, RunnerLimitOverrideError,
    RunnerLimitOverrideWire, RUNNER_LIMIT_OVERRIDE_STATE_FILENAME,
    RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION,
};
pub use snippet_catalog::{
    compose_snippet_catalog, is_valid_snippet_trigger,
    validate_snippet_trigger, ComposedSnippetCatalog, SnippetCall,
    SnippetCallStatus, SnippetDiagnostic, SnippetSourceSpan,
    SnippetTriggerValidation,
};
/// Legacy Rust alias retained for compatibility with older status callers.
pub use status::has_suffix as has_changespec_suffix;
pub use status::{
    apply_status_update as apply_status_update_lines, get_next_suffix_number,
    has_suffix as has_patch_suffix, is_valid_transition,
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
pub use task_type::{
    parse_task_type_snapshot, render_task_type_body,
    serialize_task_type_snapshot, task_type_spec_digest,
    validate_task_type_field_values, validate_task_type_spec, TaskTypeError,
    TaskTypeFieldSpecWire, TaskTypeFieldValueError, TaskTypeSnapshotEntryWire,
    TaskTypeSnapshotWire, TaskTypeSourceWire, TaskTypeSpecWire,
    TaskTypeTriageWire, RESERVED_TASK_TYPE_SLUGS, TASK_TYPE_FIELD_ROLES,
    TASK_TYPE_FIELD_TYPES, TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION,
};
pub use telemetry::{
    cleanup_matching_labels as telemetry_cleanup_matching_labels,
    prune as telemetry_prune, query_instant as telemetry_query_instant,
    query_range as telemetry_query_range,
    record_batch as telemetry_record_batch,
    store_stats as telemetry_store_stats, TelemetryCleanupReportWire,
    TelemetryCleanupRequestWire, TelemetryHistogramBucketWire,
    TelemetryInstantQueryResultWire, TelemetryInstantQueryWire,
    TelemetryInstantValueWire, TelemetryMetricKindWire, TelemetryPointWire,
    TelemetryPruneReportWire, TelemetryPruneRequestWire,
    TelemetryRangeQueryResultWire, TelemetryRangeQueryWire,
    TelemetryRecordBatchResultWire, TelemetryRecordBatchWire,
    TelemetryRetentionWire, TelemetrySampleWire, TelemetrySeriesWire,
    TelemetryStoreStatsWire, TELEMETRY_MAX_BUSY_TIMEOUT,
    TELEMETRY_WIRE_SCHEMA_VERSION,
};
pub use vcs_log::{
    aggregate_commit_log, classify_commit_origin, classify_commit_types,
    classify_commit_types_for_commit, parse_git_log, AggregatedCommitWire,
    CommitOriginWire, VcsCommitWire, VCS_LOG_WIRE_SCHEMA_VERSION,
};
pub use wire::{
    ChangeSpecWire, CommentWire, CommitWire, DeltaWire, HookStatusLineWire,
    HookWire, MentorStatusLineWire, MentorWire, ParseErrorWire,
    PatchHookStatusLineWire, PatchHookWire, PatchMentorWire, PatchWire,
    SourceSpanWire, StitchWire, TimestampWire, CHANGESPEC_WIRE_SCHEMA_VERSION,
    PATCH_WIRE_SCHEMA_VERSION,
};
pub use workspace_lease::{
    authorize_operational_lease_workspace, is_operational_lease_policy_kind,
    is_operational_lease_workspace, normalize_workspace_num,
    operational_lease_failure_message, operational_lease_pool_bounds,
    validate_operational_lease_policy, OperationalLeaseError,
    OperationalLeaseFailureKind, OperationalLeasePolicyWire,
    LEGACY_PRIMARY_WORKSPACE_NUM, MACHINE_OWNED_MIN_WORKSPACE,
    OPERATIONAL_LEASE_POLICY_KIND, PRIMARY_WORKSPACE_NUM,
    UNIFIED_MAX_WORKSPACE,
};
pub use xprompt_catalog::{
    load_editor_snippet_catalog, load_editor_xprompt_catalog,
    XpromptCatalogLoadError, XpromptCatalogLoadOptions,
};

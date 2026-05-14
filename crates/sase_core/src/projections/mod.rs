//! Append-only event log and SQLite projection foundation.
//!
//! The projection database is an indexed cache, not the source of truth. Domain
//! modules append canonical events, update projection rows in the same immediate
//! transaction, and can rebuild those rows by replaying `event_log` in sequence.

pub mod agents;
pub mod beads;
pub mod catalog_indexing;
pub mod catalogs;
pub mod changespec;
pub mod db;
pub mod error;
pub mod event;
pub mod indexing;
pub mod maintenance;
pub mod migrations;
pub mod mutations;
pub mod notifications;
pub mod read;
pub mod rebuild;
pub mod replay;
pub mod workflows;

pub use agents::{
    agent_archive_bundle_indexed_event_request,
    agent_archive_bundle_purged_event_request,
    agent_archive_bundle_revived_event_request,
    agent_artifact_associated_event_request,
    agent_artifact_dir_for_source_path, agent_attempt_created_event_request,
    agent_attempt_updated_event_request,
    agent_dismissed_identity_changed_event_request,
    agent_edge_observed_event_request,
    agent_lifecycle_marker_observed_event_request,
    agent_projection_active_page, agent_projection_archive_page,
    agent_projection_artifacts, agent_projection_children,
    agent_projection_dismissed_identities,
    agent_projection_event_request_for_artifact_dir,
    agent_projection_recent_page, agent_projection_shadow_diff,
    agent_source_change_from_path, backfill_agent_projection_from_scan,
    is_agent_indexed_marker_file_name, AgentArchiveProjectionPageWire,
    AgentArtifactAssociationWire, AgentAttemptProjectionWire,
    AgentDismissedIdentityWire, AgentEdgeProjectionWire,
    AgentProjectionApplier, AgentProjectionEventContextWire,
    AgentProjectionEventPayloadWire, AgentProjectionPageWire,
    AgentProjectionSummaryWire, AGENT_EVENT_ARCHIVE_BUNDLE_INDEXED,
    AGENT_EVENT_ARCHIVE_BUNDLE_PURGED, AGENT_EVENT_ARCHIVE_BUNDLE_REVIVED,
    AGENT_EVENT_ARTIFACT_ASSOCIATED, AGENT_EVENT_ATTEMPT_CREATED,
    AGENT_EVENT_ATTEMPT_UPDATED, AGENT_EVENT_DISMISSED_IDENTITY_CHANGED,
    AGENT_EVENT_EDGE_OBSERVED, AGENT_EVENT_MARKER_OBSERVED,
    AGENT_PROJECTION_NAME,
};
pub use beads::{
    bead_backfill_snapshot, bead_backfill_snapshot_event_request,
    bead_closed_event_request, bead_created_event_request,
    bead_dependency_added_event_request, bead_dependency_removed_event_request,
    bead_mutation_event_request, bead_projection_blocked,
    bead_projection_epic_children, bead_projection_epic_work_plan,
    bead_projection_events, bead_projection_issues,
    bead_projection_legend_work_plan, bead_projection_list,
    bead_projection_ready, bead_projection_shadow_diff, bead_projection_show,
    bead_projection_stats, bead_ready_to_work_changed_event_request,
    bead_removed_event_request, bead_reopened_event_request,
    bead_snapshot_observed_event_request, bead_source_identity,
    bead_store_shadow_diff, bead_store_source, bead_updated_event_request,
    bead_work_preclaimed_event_request, discover_bead_stores,
    BeadDependencyEventPayloadWire, BeadDependencyRemovedEventPayloadWire,
    BeadIssueEventPayloadWire, BeadIssuesEventPayloadWire,
    BeadProjectedEventWire, BeadProjectionApplier,
    BeadProjectionEventContextWire, BeadRemovedEventPayloadWire,
    BeadSnapshotObservedEventPayloadWire, BeadStoreSourceWire,
    BeadWorkPreclaimedEventPayloadWire, BEAD_EVENT_CLOSED, BEAD_EVENT_CREATED,
    BEAD_EVENT_DEPENDENCY_ADDED, BEAD_EVENT_DEPENDENCY_REMOVED,
    BEAD_EVENT_READY_TO_WORK_CHANGED, BEAD_EVENT_REMOVED, BEAD_EVENT_REOPENED,
    BEAD_EVENT_SNAPSHOT_OBSERVED, BEAD_EVENT_UPDATED,
    BEAD_EVENT_WORK_PRECLAIMED, BEAD_INDEXING_DOMAIN, BEAD_PROJECTION_NAME,
};
pub use catalog_indexing::{
    catalog_backfill_plan, catalog_shadow_diff, CatalogBackfillOptionsWire,
    CatalogBackfillPlanWire, CatalogWatchedSourceWire,
};
pub use catalogs::{
    catalog_config_source_observed_event_request,
    catalog_config_source_removed_event_request,
    catalog_file_history_removed_event_request,
    catalog_file_history_updated_event_request,
    catalog_memory_source_observed_event_request,
    catalog_memory_source_removed_event_request,
    catalog_projection_config_sources,
    catalog_projection_file_history_completion,
    catalog_projection_file_history_rows, catalog_projection_memory_sources,
    catalog_projection_xprompts, catalog_xprompt_source_observed_event_request,
    catalog_xprompt_source_removed_event_request, CatalogProjectionApplier,
    CatalogProjectionEventContextWire, ConfigCatalogProjectionWire,
    FileHistoryProjectionWire, MemoryCatalogProjectionWire,
    XpromptCatalogProjectionWire, CATALOG_EVENT_CONFIG_SOURCE_OBSERVED,
    CATALOG_EVENT_CONFIG_SOURCE_REMOVED, CATALOG_EVENT_FILE_HISTORY_REMOVED,
    CATALOG_EVENT_FILE_HISTORY_UPDATED, CATALOG_EVENT_MEMORY_SOURCE_OBSERVED,
    CATALOG_EVENT_MEMORY_SOURCE_REMOVED, CATALOG_EVENT_XPROMPT_SOURCE_OBSERVED,
    CATALOG_EVENT_XPROMPT_SOURCE_REMOVED, CATALOG_PROJECTION_NAME,
};
pub use changespec::{
    active_archive_moved_event, changespec_handle, sections_updated_event,
    source_file_observed_event, source_file_reparsed_event, spec_created_event,
    spec_deleted_event, spec_updated_event, status_transitioned_event,
    ChangeSpecDetailWire, ChangeSpecListPageWire, ChangeSpecListRequestWire,
    ChangeSpecProjectionApplier, ChangeSpecProjectionErrorWire,
    ChangeSpecSearchRequestWire, ChangeSpecSectionRowWire,
    ChangeSpecSummaryWire, CHANGESPEC_PROJECTION_NAME,
};
pub use db::ProjectionDb;
pub use error::{ProjectionDuplicateEventWire, ProjectionError};
pub use event::{
    EventAppendOutcomeWire, EventAppendRequestWire, EventCausalityWire,
    EventEnvelopeWire, EventSourceWire, PROJECTION_EVENT_SCHEMA_VERSION,
};
pub use indexing::{
    changespec_event_request_for_source_change,
    changespec_source_change_from_path, changespec_source_file_from_path,
    discover_changespec_source_files, source_event_idempotency_key,
    source_fingerprint_from_path, source_identity_for_changespec_source,
    ChangeSpecSourceFileWire, IndexingDomainReportWire, ShadowDiffCategoryWire,
    ShadowDiffCountsWire, ShadowDiffRecordWire, ShadowDiffReportWire,
    SourceChangeOperationWire, SourceChangeWire, SourceFingerprintWire,
    SourceIdentityWire, INDEXING_WIRE_SCHEMA_VERSION,
};
pub use maintenance::{
    projection_checkpoint_decision, projection_reconciliation_plan,
    ProjectionBackupReportWire, ProjectionCheckpointDecisionWire,
    ProjectionCheckpointPolicyWire, ProjectionCheckpointReportWire,
    ProjectionReconciliationDiagnosticWire, ProjectionReconciliationPlanWire,
    ProjectionRetentionPolicyWire, ProjectionRetentionReportWire,
    ProjectionWalCheckpointModeWire, DEFAULT_CHECKPOINT_INTERVAL_SECS,
    DEFAULT_WAL_SOFT_CAP_BYTES,
};
pub use migrations::{
    applied_migrations, known_migrations, run_migrations, AppliedMigrationWire,
    MigrationWire, PROJECTION_SCHEMA_VERSION,
};
pub use mutations::{
    apply_source_export, list_pending_source_exports_tx,
    mark_source_export_applied_tx, mark_source_export_failed_tx,
    retry_source_export_once_tx, source_export_row_tx,
    LocalDaemonMutationOutcomeWire, MutationActorWire,
    MutationConflictKindWire, MutationConflictWire, SourceExportKindWire,
    SourceExportOutboxRowWire, SourceExportPlanWire, SourceExportReportWire,
    SourceExportStatusWire, MUTATION_WIRE_SCHEMA_VERSION,
};
pub use notifications::{
    notification_append_event_request, notification_backfill_event_requests,
    notification_event_requests_for_source_change,
    notification_projection_counts, notification_projection_detail,
    notification_projection_page, notification_projection_snapshot,
    notification_rewrite_event_request, notification_shadow_diff,
    notification_source_change_from_path, notification_source_kind_from_path,
    notification_source_paths, notification_state_update_event_request,
    pending_action_cleanup_event_request,
    pending_action_register_event_request,
    pending_action_store_rewrite_event_request,
    pending_action_update_event_request, projected_pending_action_store,
    NotificationProjectionApplier, NotificationProjectionEventContextWire,
    NotificationProjectionEventPayloadWire,
    NotificationProjectionFacetCountsWire, NotificationSourceKindWire,
    NotificationSourcePathsWire, NOTIFICATION_EVENT_APPENDED,
    NOTIFICATION_EVENT_REWRITTEN, NOTIFICATION_EVENT_STATE_UPDATED,
    NOTIFICATION_PROJECTION_NAME, PENDING_ACTION_EVENT_CLEANED_UP,
    PENDING_ACTION_EVENT_REGISTERED, PENDING_ACTION_EVENT_STORE_REWRITTEN,
    PENDING_ACTION_EVENT_UPDATED,
};
pub use read::{
    projection_read_schema_version, AgentArchiveReadResponseWire,
    AgentReadDetailRequestWire, AgentReadDetailResponseWire,
    AgentReadListRequestWire, AgentReadListResponseWire,
    BeadReadDetailRequestWire, BeadReadDetailResponseWire,
    BeadReadListRequestWire, BeadReadListResponseWire,
    BeadReadStatsResponseWire, CatalogReadListRequestWire,
    CatalogReadResponseWire, ChangeSpecReadDetailRequestWire,
    ChangeSpecReadDetailResponseWire, ChangeSpecReadListRequestWire,
    ChangeSpecReadListResponseWire, NotificationPendingActionsReadResponseWire,
    NotificationReadDetailRequestWire, NotificationReadDetailResponseWire,
    NotificationReadListRequestWire, NotificationReadListResponseWire,
    ProjectionPageInfoWire, ProjectionPageRequestWire,
    ProjectionPayloadBoundWire, ProjectionReadErrorCodeWire,
    ProjectionReadErrorWire, ProjectionSnapshotReadWire,
    PROJECTION_READ_WIRE_SCHEMA_VERSION,
};
pub use rebuild::{
    ProjectionRebuildOptionsWire, ProjectionRebuildReportWire,
    ProjectionSourceDiffWire, ProjectionTableResetReportWire,
};
pub use replay::{
    ProjectionApplier, ProjectionGapWire, ProjectionRecoveryIssueWire,
    ProjectionReplayReportWire, ProjectionStartupRepairReportWire,
    ReplayProjectionApplier,
};
pub use workflows::{
    workflow_hitl_paused_event_request, workflow_hitl_resumed_event_request,
    workflow_projection_detail, workflow_projection_list,
    workflow_retry_requested_event_request, workflow_run_created_event_request,
    workflow_run_updated_event_request,
    workflow_step_transitioned_event_request,
    workflow_terminal_state_reached_event_request, WorkflowDetailWire,
    WorkflowEventProjectionWire, WorkflowProjectionApplier,
    WorkflowProjectionEventContextWire, WorkflowProjectionPageWire,
    WorkflowRunProjectionWire, WorkflowStepProjectionWire, WorkflowSummaryWire,
    WORKFLOW_EVENT_HITL_PAUSED, WORKFLOW_EVENT_HITL_RESUMED,
    WORKFLOW_EVENT_RETRY_REQUESTED, WORKFLOW_EVENT_RUN_CREATED,
    WORKFLOW_EVENT_RUN_UPDATED, WORKFLOW_EVENT_STEP_TRANSITIONED,
    WORKFLOW_EVENT_TERMINAL_STATE_REACHED, WORKFLOW_PROJECTION_NAME,
};

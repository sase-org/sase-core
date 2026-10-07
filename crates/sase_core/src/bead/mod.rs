//! Pure-Rust bead storage contract.
//!
//! This module mirrors the Python `sase.bead` model and portable storage
//! codecs without exposing command handlers yet. Later phases build read and
//! mutation engines on top of these wire records.

pub mod artifact_refs;
pub mod attachments;
pub mod board;
pub mod cli;
pub mod config;
pub mod events;
pub mod fingerprint;
pub mod history;
pub mod jsonl;
pub mod mutation;
pub mod read;
pub mod read_model;
pub mod routing;
pub mod schema;
pub mod seal_watch;
pub mod search;
pub mod target_probe;
pub mod touch_index;
pub mod wire;
pub mod work;

pub use crate::artifact_link::BeadLinkWire;
pub use artifact_refs::{
    bead_referenced_artifact_ids, referenced_artifact_ids_in_issues,
    referenced_artifact_ids_in_projection_text,
    BEAD_REFERENCED_ARTIFACT_IDS_WIRE_SCHEMA_VERSION,
};
pub use attachments::{bead_attachment_references, bead_attachment_roster};
pub use board::{
    board_snapshot, BeadBoardSnapshotWire,
    BEAD_BOARD_SNAPSHOT_WIRE_SCHEMA_VERSION,
};
pub use cli::{
    execute_bead_cli, BeadCliMutationSummaryWire, BeadCliOutcomeWire,
    BeadCliStatusTransitionWire,
};
pub use config::{
    default_config, load_config, load_config_from_str, save_config,
    BeadConfigWire,
};
pub use events::{
    import_issues_to_event_streams, merge_bead_event_streams,
    merge_bead_event_streams_with_relocation, reduce_event_streams,
    BeadEventOperationWire, BeadEventPayloadWire, BeadEventRecordWire,
    BeadEventStoreManifestWire, BeadEventStreamMergeWire, BeadEventStreamWire,
    BeadIdRelocationKindWire, BeadIdRelocationWire,
    BeadIssueUpdateEventFieldsWire, BeadSnoozeWakeCauseWire,
    BEAD_EVENT_SCHEMA_VERSION,
};
pub use fingerprint::{
    bead_store_fingerprint, BeadStoreFingerprintWire,
    BEAD_STORE_FINGERPRINT_LAYOUT_EVENTS, BEAD_STORE_FINGERPRINT_LAYOUT_LEGACY,
    BEAD_STORE_FINGERPRINT_WIRE_SCHEMA_VERSION,
};
pub use history::{
    bead_history, bead_lost_notes, BeadHistoryChangeWire, BeadHistoryEntryWire,
    BeadHistoryWire, BeadLostNoteRevisionWire, BeadLostNotesWire,
    BEAD_HISTORY_WIRE_SCHEMA_VERSION,
};
pub use jsonl::{
    export_issues_to_jsonl, import_issues_from_jsonl, parse_issues_jsonl,
    prune_removed_flag_event_streams, repair_event_store_manifest,
    BeadEventManifestRepairOutcomeWire, BeadEventManifestRepairStatusWire,
    JsonlLoadOutcome, RemovedFlagStreamPruneOutcomeWire,
};
pub use mutation::{
    add_bead_link, add_bead_references, add_dependency, add_task_plus_one,
    append_issue_note, cancel_task_snooze, claim_for_agent_launch,
    claim_for_agent_wait, close_issues, close_issues_with_note, create_issue,
    edit_issue_note, export_jsonl, init_store, mark_ready_to_work, open_issue,
    preclaim_epic_work_plan, release_agent_claim, remove_bead_link,
    remove_bead_references, remove_dependencies, remove_issue,
    remove_issue_note, remove_issues, set_bead_link_projection,
    set_bead_link_projections, snooze_task, sync_is_clean,
    unmark_ready_to_work, update_issue, update_issues, BeadCreateRequestWire,
    BeadLinkProjectionRequestWire, BeadMutationOutcomeWire,
    BeadPreclaimAssignmentWire, BeadPreclaimRollbackWire, BeadUpdateFieldsWire,
};
pub use read::{
    blocked_issues, closed_ids, doctor, doctor_report,
    doctor_report_with_contexts, doctor_with_contexts, doctor_with_plan_roots,
    get_epic_children, list_issue_page, list_issues, read_event_store_issues,
    read_legacy_jsonl_issues, read_store_issues, ready_issues,
    reference_diagnostics, resolve_issue_id, resolve_issue_id_in_issues,
    resolve_issue_ids, show_issue, show_issue_detail,
    show_issue_detail_with_options, stats, statuses_for_ids,
    BeadDoctorReportWire, BeadIssueDetailWire, BeadProjectionDriftWire,
    BEAD_READ_WIRE_SCHEMA_VERSION,
};
pub use read_model::{
    cached_store_snapshot, read_model_cache_path_for_store, read_model_status,
    read_model_verify_cache, BeadReadModelStatusWire, BeadReadModelVerifyWire,
    CachedStoreSnapshot, BEAD_READ_MODEL_STATUS_WIRE_SCHEMA_VERSION,
    BEAD_READ_MODEL_VERIFY_WIRE_SCHEMA_VERSION, READ_MODEL_REDUCER_VERSION,
    READ_MODEL_SCHEMA_VERSION, READ_MODEL_SWEEP_INTERVAL_SECS,
};
pub use routing::{
    route_bead_targets, BeadTargetRouteErrorWire, BeadTargetRouteWire,
    BeadTargetRoutingOutcomeWire, BeadTargetRoutingRequestWire,
    BeadTargetStoreDescriptorWire, BeadTargetStoreRouteWire,
    BEAD_TARGET_ROUTING_WIRE_SCHEMA_VERSION,
};
pub use schema::{
    changespec_metadata_migration_sql, drop_flag_type_migration_sql,
    external_ref_migration_sql, flag_type_migration_sql,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    missing_changespec_metadata_columns, model_migration_sql,
    needs_drop_flag_type_migration, needs_external_ref_migration,
    needs_flag_type_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, needs_model_migration,
    needs_plus_one_evidence_migration, needs_refs_migration,
    needs_resolution_migration, needs_size_check_relax_migration,
    needs_size_migration, needs_snoozed_status_migration,
    needs_task_ready_migration, needs_task_type_migration,
    plus_one_evidence_migration_sql, refs_migration_sql,
    resolution_migration_sql, size_check_relax_migration_sql,
    size_migration_sql, snoozed_status_migration_sql, task_ready_migration_sql,
    task_type_migration_sql, BEAD_SQLITE_SCHEMA,
};
pub use seal_watch::{
    bead_seal_watch_triggers, classify_seal_watch_triggers,
    BeadSealWatchTriggerWire, BeadSealWatchWire,
    BEAD_SEAL_WATCH_WIRE_SCHEMA_VERSION, SEAL_WATCH_HOT_STREAM_FILES_WARN,
    SEAL_WATCH_SWEEP_MS_WARN, SEAL_WATCH_TREE_BYTES_WARN,
};
pub use search::{search_issues, BEAD_SEARCH_FIELD_NAMES};
pub use target_probe::{
    probe_bead_target_owner, BeadTargetProbeOutcomeWire,
    BeadTargetProbeStatusWire,
};
pub use touch_index::{
    bead_touch_index_status, query_bead_touches, reduce_stream_touches,
    refresh_bead_touch_index, verb_for_operation, BeadNotePreviewWire,
    BeadStreamSignatureWire, BeadTouchIndexStateWire, BeadTouchIndexStatusWire,
    BeadTouchIndexWire, BeadTouchQueryWire, BeadTouchRefreshWire,
    BeadTouchWire, BEAD_TOUCH_INDEX_WIRE_SCHEMA_VERSION,
    CREATION_REASON_PREVIEW_LIMIT, NOTE_PREVIEW_TEXT_LIMIT,
};
pub use wire::{
    flag_thresholds_due, normalize_creation_reason, notes_text,
    parse_snooze_timestamp, validate_model_value, BeadCloseRecordWire,
    BeadError, BeadNoteWire, BeadReopenCauseWire, BeadResolutionWire,
    BeadSearchMatchWire, BeadSnoozeWire, BeadTierWire, DependencyWire,
    IssueTypeWire, IssueWire, PhaseSizeWire, StatusWire,
    TaskPlusOneEvidenceWire, CREATION_REASON_MAX_LEN,
};
pub use work::{
    build_epic_work_plan, build_epic_work_plan_from_issues, EpicWorkPlanWire,
    PhaseAssignmentWire,
};

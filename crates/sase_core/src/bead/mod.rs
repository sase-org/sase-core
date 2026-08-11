//! Pure-Rust bead storage contract.
//!
//! This module mirrors the Python `sase.bead` model and portable storage
//! codecs without exposing command handlers yet. Later phases build read and
//! mutation engines on top of these wire records.

pub mod cli;
pub mod config;
pub mod events;
pub mod history;
pub mod jsonl;
pub mod mutation;
pub mod read;
pub mod schema;
pub mod search;
pub mod wire;
pub mod work;

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
    BeadIssueUpdateEventFieldsWire, BeadSnoozeWakeCauseWire,
    BEAD_EVENT_SCHEMA_VERSION,
};
pub use history::{
    bead_history, bead_lost_notes, BeadHistoryChangeWire, BeadHistoryEntryWire,
    BeadHistoryWire, BeadLostNoteRevisionWire, BeadLostNotesWire,
    BEAD_HISTORY_WIRE_SCHEMA_VERSION,
};
pub use jsonl::{
    export_issues_to_jsonl, import_issues_from_jsonl, parse_issues_jsonl,
    repair_event_store_manifest, BeadEventManifestRepairOutcomeWire,
    BeadEventManifestRepairStatusWire, JsonlLoadOutcome,
};
pub use mutation::{
    add_bead_references, add_dependency, add_task_plus_one, append_issue_note,
    cancel_task_snooze, claim_for_agent_launch, claim_for_agent_wait,
    close_issues, close_issues_with_note, create_issue, export_jsonl,
    init_store, mark_ready_to_work, open_issue, preclaim_epic_work_plan,
    release_agent_claim, remove_bead_references, remove_dependencies,
    remove_issue, remove_issues, snooze_task, sync_is_clean,
    unmark_ready_to_work, update_issue, update_issues, BeadCreateRequestWire,
    BeadMutationOutcomeWire, BeadPreclaimAssignmentWire,
    BeadPreclaimRollbackWire, BeadUpdateFieldsWire,
};
pub use read::{
    blocked_issues, doctor, doctor_report, doctor_report_with_contexts,
    doctor_with_contexts, doctor_with_plan_roots, get_epic_children,
    list_issues, read_event_store_issues, read_legacy_jsonl_issues,
    read_store_issues, ready_issues, reference_diagnostics, resolve_issue_id,
    resolve_issue_id_in_issues, resolve_issue_ids, show_issue,
    show_issue_detail, stats, BeadDoctorReportWire, BeadIssueDetailWire,
    BeadProjectionDriftWire, BEAD_READ_WIRE_SCHEMA_VERSION,
};
pub use schema::{
    changespec_metadata_migration_sql, external_ref_migration_sql,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    missing_changespec_metadata_columns, model_migration_sql,
    needs_external_ref_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, needs_model_migration,
    needs_plus_one_evidence_migration, needs_refs_migration,
    needs_resolution_migration, needs_size_check_relax_migration,
    needs_size_migration, needs_snoozed_status_migration,
    needs_task_ready_migration, plus_one_evidence_migration_sql,
    refs_migration_sql, resolution_migration_sql,
    size_check_relax_migration_sql, size_migration_sql,
    snoozed_status_migration_sql, task_ready_migration_sql, BEAD_SQLITE_SCHEMA,
};
pub use search::{search_issues, BEAD_SEARCH_FIELD_NAMES};
pub use wire::{
    parse_snooze_timestamp, validate_model_value, BeadCloseRecordWire,
    BeadError, BeadReopenCauseWire, BeadResolutionWire, BeadSearchMatchWire,
    BeadSnoozeWire, BeadTierWire, DependencyWire, IssueTypeWire, IssueWire,
    PhaseSizeWire, StatusWire, TaskPlusOneEvidenceWire,
};
pub use work::{
    build_epic_work_plan, build_epic_work_plan_from_issues, EpicWorkPlanWire,
    PhaseAssignmentWire,
};

//! Pure-Rust bead storage contract.
//!
//! This module mirrors the Python `sase.bead` model and portable storage
//! codecs without exposing command handlers yet. Later phases build read and
//! mutation engines on top of these wire records.

pub mod cli;
pub mod config;
pub mod events;
pub mod jsonl;
pub mod mutation;
pub mod read;
pub mod schema;
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
    import_issues_to_event_streams, reduce_event_streams,
    BeadEventOperationWire, BeadEventPayloadWire, BeadEventRecordWire,
    BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadIssueUpdateEventFieldsWire, BEAD_EVENT_SCHEMA_VERSION,
};
pub use jsonl::{
    export_issues_to_jsonl, import_issues_from_jsonl, parse_issues_jsonl,
    JsonlLoadOutcome,
};
pub use mutation::{
    add_dependency, close_issues, create_issue, export_jsonl, init_store,
    mark_ready_to_work, open_issue, preclaim_epic_work_plan, remove_issue,
    sync_is_clean, unmark_ready_to_work, update_issue, BeadCreateRequestWire,
    BeadMutationOutcomeWire, BeadPreclaimAssignmentWire,
    BeadPreclaimRollbackWire, BeadUpdateFieldsWire,
};
pub use read::{
    blocked_issues, doctor, get_epic_children, list_issues,
    read_event_store_issues, read_legacy_jsonl_issues, read_store_issues,
    ready_issues, show_issue, stats, BEAD_READ_WIRE_SCHEMA_VERSION,
};
pub use schema::{
    changespec_metadata_migration_sql, epic_count_migration_sql,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    missing_changespec_metadata_columns, model_migration_sql,
    needs_epic_count_migration, needs_is_ready_to_work_migration,
    needs_issue_type_migration, needs_model_migration, BEAD_SQLITE_SCHEMA,
};
pub use wire::{
    validate_model_value, BeadError, BeadTierWire, DependencyWire,
    IssueTypeWire, IssueWire, OperationOutcomeWire, StatusWire,
};
pub use work::{
    build_epic_work_plan, build_epic_work_plan_from_issues,
    build_legend_work_plan, build_legend_work_plan_from_issues,
    EpicWorkPlanWire, LegendEpicAssignmentWire, LegendWorkPlanWire,
    PhaseAssignmentWire,
};

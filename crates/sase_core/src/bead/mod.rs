//! Pure-Rust bead storage contract.
//!
//! This module mirrors the Python `sase.bead` model and portable storage
//! codecs without exposing command handlers yet. Later phases build read and
//! mutation engines on top of these wire records.

pub mod cli;
pub mod config;
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
pub use jsonl::{
    export_issues_to_jsonl, import_issues_from_jsonl, parse_issues_jsonl,
    JsonlLoadOutcome,
};
pub use mutation::{
    add_dependency, close_issues, create_issue, export_jsonl, init_store,
    mark_ready_to_work, open_issue, remove_issue, sync_is_clean,
    unmark_ready_to_work, update_issue, BeadCreateRequestWire,
    BeadMutationOutcomeWire, BeadUpdateFieldsWire,
};
pub use read::{
    blocked_issues, blocked_merged_issues, doctor, get_epic_children,
    get_merged_epic_children, list_issues, list_merged_issues,
    merge_workspace_issues, merged_stats, read_store_issues, ready_issues,
    ready_merged_issues, show_issue, show_merged_issue, stats,
    BEAD_READ_WIRE_SCHEMA_VERSION,
};
pub use schema::{
    changespec_metadata_migration_sql, epic_count_migration_sql,
    is_ready_to_work_migration_sql, issue_type_migration_sql,
    missing_changespec_metadata_columns, needs_epic_count_migration,
    needs_is_ready_to_work_migration, needs_issue_type_migration,
    BEAD_SQLITE_SCHEMA,
};
pub use wire::{
    BeadError, DependencyWire, IssueTypeWire, IssueWire, OperationOutcomeWire,
    StatusWire,
};
pub use work::{
    build_epic_work_plan, build_epic_work_plan_from_issues, EpicWorkPlanWire,
    PhaseAssignmentWire,
};

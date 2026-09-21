//! Bead store mutations backed by JSONL persistence.

mod claims;
mod close_remove;
mod create;
mod dependencies;
mod links;
mod mutation_wire;
mod notes_update;
mod plus_one_snooze;
mod store;

#[cfg(test)]
mod tests;

pub use claims::{
    claim_for_agent_launch, claim_for_agent_wait, preclaim_epic_work_plan,
    release_agent_claim,
};
pub use close_remove::{
    close_issues, close_issues_with_note, open_issue, remove_issue,
    remove_issues,
};
pub use create::{create_issue, init_store};
pub use dependencies::{
    add_bead_references, add_dependency, remove_bead_references,
    remove_dependencies,
};
pub use links::{
    add_bead_link, remove_bead_link, set_bead_link_projection,
    set_bead_link_projections,
};
pub use mutation_wire::{
    BeadCreateRequestWire, BeadLinkProjectionRequestWire,
    BeadMutationOutcomeWire, BeadPreclaimAssignmentWire,
    BeadPreclaimRollbackWire, BeadUpdateFieldsWire,
};
pub use notes_update::{
    append_issue_note, edit_issue_note, remove_issue_note, update_issue,
    update_issues,
};
pub use plus_one_snooze::{add_task_plus_one, cancel_task_snooze, snooze_task};
pub use store::{
    export_jsonl, mark_ready_to_work, sync_is_clean, unmark_ready_to_work,
};

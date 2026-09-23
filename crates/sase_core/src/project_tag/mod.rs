//! Xprompt project tags (`+sase`): lexer, resolver, expander, and accept.
//!
//! A project tag names the project an agent runs in without naming its VCS
//! type. The backend expands each resolved tag in place into the canonical
//! `#<workflow_type>:<key>` ref the existing alias canonicalizer produces,
//! so everything downstream (VCS extraction, swarm/alt inheritance, the typed
//! planner, workflows, MRU) runs unchanged.

mod accept;
mod expand;
mod resolve;
mod scan;
mod wire;

#[cfg(test)]
mod tests;

pub use accept::{apply_project_tag_selection, project_tag_trigger};
pub use expand::expand_project_tags;
pub use resolve::resolve_project_tag;
pub use scan::scan_project_tags;
pub use wire::{
    ProjectTagApplyWire, ProjectTagExpansionEntryWire, ProjectTagExpansionWire,
    ProjectTagResolutionWire, ProjectTagSpanWire, ProjectTagTargetWire,
    ProjectTagTriggerWire,
};

pub(crate) use accept::{project_tag_selection_edits, ProjectTagSelectionEdit};
pub use scan::is_tag_name;

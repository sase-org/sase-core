//! Project tag expansion: rewrite resolved tags to VCS refs in place.

use super::resolve::resolve_project_tag;
use super::scan::scan_project_tags;
use super::wire::{
    ProjectTagExpansionEntryWire, ProjectTagExpansionWire,
    ProjectTagResolutionWire, ProjectTagTargetWire,
};

/// Rewrite each resolved tag in place into `#<workflow_type>:<key>`.
///
/// That is byte-for-byte the canonical form the existing alias canonicalizer
/// already produces from `#gh:sase`, so nothing downstream changes. Only
/// resolved tags whose target has a `workflow_type` are rewritten; every tag
/// still gets a report entry. Never errors: launch policy (anchored-strict
/// vs. forgiving, disabled/provider-less/ambiguous/unknown handling) belongs
/// to the callers.
pub fn expand_project_tags(
    text: &str,
    targets: &[ProjectTagTargetWire],
) -> ProjectTagExpansionWire {
    let mut tags = Vec::new();
    for span in scan_project_tags(text) {
        let resolution = resolve_project_tag(&span.name, targets);
        let replacement = match &resolution {
            ProjectTagResolutionWire::Resolved { target_index } => {
                match targets.get(*target_index) {
                    Some(target)
                        if target
                            .workflow_type
                            .as_deref()
                            .is_some_and(|kind| !kind.is_empty()) =>
                    {
                        Some(format!(
                            "#{}:{}",
                            target.workflow_type.as_deref().unwrap_or_default(),
                            target.key,
                        ))
                    }
                    _ => None,
                }
            }
            ProjectTagResolutionWire::Ambiguous { .. }
            | ProjectTagResolutionWire::Unknown { .. } => None,
        };
        tags.push(ProjectTagExpansionEntryWire {
            start: span.start,
            end: span.end,
            name_start: span.name_start,
            name: span.name,
            anchored: span.anchored,
            resolution,
            replacement,
        });
    }
    let mut expanded = text.to_string();
    for tag in tags.iter().rev() {
        if let Some(replacement) = tag.replacement.as_deref() {
            expanded.replace_range(tag.start..tag.end, replacement);
        }
    }
    ProjectTagExpansionWire {
        text: expanded,
        tags,
    }
}

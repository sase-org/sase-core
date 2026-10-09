use super::mutation_wire::BeadMutationOutcomeWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::view::MutationView;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::BeadError;
use crate::bead::wire::DependencyWire;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::path::Path;

pub fn add_dependency(
    beads_dir: &Path,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    run_mutation(beads_dir, "add_dependency", |view| {
        run_add_dependency(view, issue_id, depends_on_id, now.clone())
    })
}

pub fn remove_dependencies(
    beads_dir: &Path,
    issue_id: &str,
    depends_on_ids: &[String],
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if depends_on_ids.is_empty() {
        return Err(BeadError::validation(
            "remove_dependencies() requires at least one dependency ID",
        ));
    }
    run_mutation(beads_dir, "remove_dependency", |view| {
        run_remove_dependencies(view, issue_id, depends_on_ids, now.clone())
    })
}

pub fn add_bead_references(
    beads_dir: &Path,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if references.is_empty() {
        return Err(BeadError::validation(
            "add_bead_references() requires at least one artifact reference",
        ));
    }
    let references = normalize_references(references)?;
    run_mutation(beads_dir, "add_reference", |view| {
        run_add_references(view, issue_id, &references, now.clone())
    })
}

pub fn remove_bead_references(
    beads_dir: &Path,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if references.is_empty() {
        return Err(BeadError::validation(
            "remove_bead_references() requires at least one artifact reference",
        ));
    }
    let references = normalize_references(references)?;
    run_mutation(beads_dir, "remove_reference", |view| {
        run_remove_references(view, issue_id, &references, now.clone())
    })
}

/// The single `add_dependency` algorithm, over the view on both backings.
///
/// Both IDs resolve through the view, exactly as the locked replay load
/// did, with an existence check for the target. The row stages before
/// its event so stream routing sees it; one `commit` persists both
/// backings. A decline retries the same closure on the replay backing.
fn run_add_dependency(
    view: &mut MutationView,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let resolved_dep = view.resolve(depends_on_id)?;
    // Existence check for the target, exactly as replay does.
    view.get(&resolved_dep)?;
    let mut issue = view.get(&resolved)?;
    if issue
        .dependencies
        .iter()
        .any(|dep| dep.depends_on_id == resolved_dep)
    {
        return Err(BeadError::validation(format!(
            "Dependency already exists: {resolved} depends on {resolved_dep}"
        )));
    }
    let owner = view.config()?.owner.clone();
    let dep = DependencyWire {
        issue_id: resolved.clone(),
        depends_on_id: resolved_dep.clone(),
        created_at: now.unwrap_or_else(now_utc),
        created_by: owner,
    };
    issue.dependencies.push(dep.clone());
    view.stage_issue(issue.clone());
    let Some(_) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::DependencyAdded,
        BeadEventPayloadWire::DependencyAdded {
            dependency: dep.clone(),
        },
        &dep.created_at,
        &dep.created_by,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let _corrected = rows.pop().expect("one dependency row");
    // The replay outcome carries only the new edge (no issue row), so
    // the corrected row is verified by the commit and then dropped,
    // exactly as the staged write plus publish already applied it.
    let mut result = outcome("dep_add", true, vec![resolved]);
    result.dependency = Some(dep);
    Ok(MutationStep::Done(result))
}

/// The single `remove_dependencies` algorithm, over the view on both
/// backings.
///
/// The whole batch validates before anything stages, so a bad batch
/// leaves bytes untouched. Blocker status comes from dependency-target
/// point lookups, never a whole-store status map.
fn run_remove_dependencies(
    view: &mut MutationView,
    issue_id: &str,
    depends_on_ids: &[String],
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut seen = BTreeSet::new();
    let mut requested: Vec<String> = Vec::new();
    for depends_on_id in depends_on_ids {
        let entry = view.resolve(depends_on_id)?;
        if seen.insert(entry.clone()) {
            requested.push(entry);
        }
    }
    let mut issue = view.get(&resolved)?;
    let removed = requested
        .iter()
        .map(|depends_on_id| {
            issue
                .dependencies
                .iter()
                .find(|dependency| {
                    dependency.depends_on_id == *depends_on_id
                })
                .cloned()
                .ok_or_else(|| {
                    BeadError::validation(format!(
                        "Dependency does not exist: {resolved} does not depend on {depends_on_id}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let removed_ids: BTreeSet<&str> = removed
        .iter()
        .map(|dependency| dependency.depends_on_id.as_str())
        .collect();
    issue.dependencies.retain(|dependency| {
        !removed_ids.contains(dependency.depends_on_id.as_str())
    });
    // Stage before the blocker computation so the overlay reflects the
    // post-removal edge set while target statuses still read from rows.
    view.stage_issue(issue.clone());
    let removed_at = now.unwrap_or_else(now_utc);
    let actor = view.config()?.owner.clone();
    for dependency in &removed {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::DependencyRemoved,
            BeadEventPayloadWire::DependencyRemoved {
                dependency: dependency.clone(),
            },
            &removed_at,
            &actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let updated_issue = rows.pop().expect("one dependency row");
    let mut issue_ids = Vec::with_capacity(removed.len() + 1);
    issue_ids.push(resolved.clone());
    issue_ids.extend(
        removed
            .iter()
            .map(|dependency| dependency.depends_on_id.clone()),
    );
    let mut result = outcome("dep_rm", true, issue_ids);
    result.issue = Some(updated_issue);
    result.dependencies = removed;
    result.active_blocker_ids = active_blockers_via_view(view, &resolved)?;
    Ok(MutationStep::Done(result))
}

/// The single `add_bead_references` algorithm, over the view on both
/// backings.
///
/// `references` are already normalized by the caller, exactly as replay
/// receives them. A no-op (nothing new) returns a no-change outcome
/// without writing.
fn run_add_references(
    view: &mut MutationView,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut issue = view.get(&resolved)?;
    let added = references
        .iter()
        .filter(|reference| !issue.refs.contains(*reference))
        .cloned()
        .collect::<Vec<_>>();
    if added.is_empty() {
        let mut result = outcome("ref_add", false, vec![resolved.clone()]);
        result.issue = Some(issue);
        return Ok(MutationStep::Done(result));
    }
    issue.refs.extend(added.iter().cloned());
    view.stage_issue(issue.clone());
    let added_at = now.unwrap_or_else(now_utc);
    let actor = view.config()?.owner.clone();
    for reference in &added {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::ReferenceAdded,
            BeadEventPayloadWire::ReferenceAdded {
                reference: reference.clone(),
            },
            &added_at,
            &actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one reference row");
    let mut result = outcome("ref_add", true, vec![resolved]);
    result.issue = Some(issue);
    result.references = added;
    Ok(MutationStep::Done(result))
}

/// The single `remove_bead_references` algorithm, over the view on both
/// backings.
///
/// A no-op (nothing present) returns a no-change outcome without writing.
fn run_remove_references(
    view: &mut MutationView,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let resolved = view.resolve(issue_id)?;
    let mut issue = view.get(&resolved)?;
    let removed = references
        .iter()
        .filter(|reference| issue.refs.contains(*reference))
        .cloned()
        .collect::<Vec<_>>();
    if removed.is_empty() {
        let mut result = outcome("ref_rm", false, vec![resolved.clone()]);
        result.issue = Some(issue);
        return Ok(MutationStep::Done(result));
    }
    let removed_set = removed.iter().collect::<HashSet<_>>();
    issue
        .refs
        .retain(|reference| !removed_set.contains(reference));
    view.stage_issue(issue.clone());
    let removed_at = now.unwrap_or_else(now_utc);
    let actor = view.config()?.owner.clone();
    for reference in &removed {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::ReferenceRemoved,
            BeadEventPayloadWire::ReferenceRemoved {
                reference: reference.clone(),
            },
            &removed_at,
            &actor,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one reference row");
    let mut result = outcome("ref_rm", true, vec![resolved]);
    result.issue = Some(issue);
    result.references = removed;
    Ok(MutationStep::Done(result))
}

/// Blocker status from dependency-target point lookups.
///
/// Reads only the issue's own dependency targets through the view (each
/// a single indexed row), never a whole-store status map. A target that
/// no longer resolves is not a blocker, exactly as the replay oracle's
/// status map treats a missing target. Genuine cache faults (`io`)
/// propagate; only semantic `not_found` is skipped.
fn active_blockers_via_view(
    view: &MutationView,
    issue_id: &str,
) -> Result<Vec<String>, BeadError> {
    use crate::bead::wire::StatusWire;
    let issue = view.get(issue_id)?;
    let mut blockers = Vec::new();
    for dependency in &issue.dependencies {
        match view.get(&dependency.depends_on_id) {
            Ok(target) => {
                if matches!(
                    target.status,
                    StatusWire::Open
                        | StatusWire::Claimed
                        | StatusWire::Ready
                        | StatusWire::Snoozed
                        | StatusWire::InProgress
                ) {
                    blockers.push(dependency.depends_on_id.clone());
                }
            }
            Err(error) if error.kind == "not_found" => continue,
            Err(error) => return Err(error),
        }
    }
    Ok(blockers)
}

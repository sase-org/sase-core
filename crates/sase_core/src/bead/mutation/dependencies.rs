use super::mutation_wire::BeadMutationOutcomeWire;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
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
    with_bead_mutation_lock(beads_dir, "add_dependency", || {
        let mut store = MutableStore::load(beads_dir)?;
        store.get_issue(depends_on_id)?;
        let owner = store.config.owner.clone();
        let index = store.issue_index(issue_id)?;
        if store.issues[index]
            .dependencies
            .iter()
            .any(|dep| dep.depends_on_id == depends_on_id)
        {
            return Err(BeadError::validation(format!(
                "Dependency already exists: {issue_id} depends on {depends_on_id}"
            )));
        }
        let dep = DependencyWire {
            issue_id: issue_id.to_string(),
            depends_on_id: depends_on_id.to_string(),
            created_at: now.unwrap_or_else(now_utc),
            created_by: owner,
        };
        store.issues[index].dependencies.push(dep.clone());
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::DependencyAdded,
            BeadEventPayloadWire::DependencyAdded {
                dependency: dep.clone(),
            },
            &dep.created_at,
            &dep.created_by,
        )?;
        store.save()?;

        let mut result = outcome("dep_add", true, vec![issue_id.to_string()]);
        result.dependency = Some(dep);
        Ok(result)
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
    with_bead_mutation_lock(beads_dir, "remove_dependency", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let mut seen = BTreeSet::new();
        let requested: Vec<String> = depends_on_ids
            .iter()
            .filter(|depends_on_id| seen.insert((*depends_on_id).clone()))
            .cloned()
            .collect();
        let removed = requested
            .iter()
            .map(|depends_on_id| {
                store.issues[index]
                    .dependencies
                    .iter()
                    .find(|dependency| {
                        dependency.depends_on_id == *depends_on_id
                    })
                    .cloned()
                    .ok_or_else(|| {
                        BeadError::validation(format!(
                            "Dependency does not exist: {issue_id} does not depend on {depends_on_id}"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let removed_ids: BTreeSet<&str> = removed
            .iter()
            .map(|dependency| dependency.depends_on_id.as_str())
            .collect();
        store.issues[index].dependencies.retain(|dependency| {
            !removed_ids.contains(dependency.depends_on_id.as_str())
        });
        let updated_issue = store.issues[index].clone();
        let removed_at = now.unwrap_or_else(now_utc);
        let actor = store.config.owner.clone();
        for dependency in &removed {
            store.append_issue_event(
                issue_id,
                BeadEventOperationWire::DependencyRemoved,
                BeadEventPayloadWire::DependencyRemoved {
                    dependency: dependency.clone(),
                },
                &removed_at,
                &actor,
            )?;
        }
        store.save()?;

        let mut issue_ids = Vec::with_capacity(removed.len() + 1);
        issue_ids.push(issue_id.to_string());
        issue_ids.extend(
            removed
                .iter()
                .map(|dependency| dependency.depends_on_id.clone()),
        );
        let mut result = outcome("dep_rm", true, issue_ids);
        result.issue = Some(updated_issue);
        result.dependencies = removed;
        Ok(result)
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
    with_bead_mutation_lock(beads_dir, "add_reference", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let added = references
            .iter()
            .filter(|reference| !store.issues[index].refs.contains(*reference))
            .cloned()
            .collect::<Vec<_>>();
        if added.is_empty() {
            let mut result =
                outcome("ref_add", false, vec![issue_id.to_string()]);
            result.issue = Some(store.issues[index].clone());
            return Ok(result);
        }

        store.issues[index].refs.extend(added.iter().cloned());
        let issue = store.issues[index].clone();
        let added_at = now.unwrap_or_else(now_utc);
        let actor = store.config.owner.clone();
        for reference in &added {
            store.append_issue_event(
                issue_id,
                BeadEventOperationWire::ReferenceAdded,
                BeadEventPayloadWire::ReferenceAdded {
                    reference: reference.clone(),
                },
                &added_at,
                &actor,
            )?;
        }
        store.save()?;

        let mut result = outcome("ref_add", true, vec![issue_id.to_string()]);
        result.issue = Some(issue);
        result.references = added;
        Ok(result)
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
    with_bead_mutation_lock(beads_dir, "remove_reference", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let removed = references
            .iter()
            .filter(|reference| store.issues[index].refs.contains(*reference))
            .cloned()
            .collect::<Vec<_>>();
        if removed.is_empty() {
            let mut result =
                outcome("ref_rm", false, vec![issue_id.to_string()]);
            result.issue = Some(store.issues[index].clone());
            return Ok(result);
        }

        let removed_set = removed.iter().collect::<HashSet<_>>();
        store.issues[index]
            .refs
            .retain(|reference| !removed_set.contains(reference));
        let issue = store.issues[index].clone();
        let removed_at = now.unwrap_or_else(now_utc);
        let actor = store.config.owner.clone();
        for reference in &removed {
            store.append_issue_event(
                issue_id,
                BeadEventOperationWire::ReferenceRemoved,
                BeadEventPayloadWire::ReferenceRemoved {
                    reference: reference.clone(),
                },
                &removed_at,
                &actor,
            )?;
        }
        store.save()?;

        let mut result = outcome("ref_rm", true, vec![issue_id.to_string()]);
        result.issue = Some(issue);
        result.references = removed;
        Ok(result)
    })
}

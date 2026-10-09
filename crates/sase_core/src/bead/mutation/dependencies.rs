use super::mutation_wire::BeadMutationOutcomeWire;
use super::shared::commit_staged_write;
use super::shared::load_mutation_stream;
use super::shared::mint_stream_event;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use super::view::MutationView;
use crate::bead::config::default_config;
use crate::bead::config::load_config;
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
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_add_dependency(
                beads_dir,
                view,
                issue_id,
                depends_on_id,
                now.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        let issue_id = store.resolve_issue_id(issue_id)?;
        let depends_on_id = store.resolve_issue_id(depends_on_id)?;
        store.get_issue(&depends_on_id)?;
        let owner = store.config.owner.clone();
        let index = store.issue_index(&issue_id)?;
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
            &issue_id,
            BeadEventOperationWire::DependencyAdded,
            BeadEventPayloadWire::DependencyAdded {
                dependency: dep.clone(),
            },
            &dep.created_at,
            &dep.created_by,
        )?;
        store.save()?;

        let mut result = outcome("dep_add", true, vec![issue_id.clone()]);
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
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_remove_dependencies(
                beads_dir,
                view,
                issue_id,
                depends_on_ids,
                now.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        // Resolve inside the locked load: the single store read is the
        // authority for existence and ambiguity, so callers pass raw IDs.
        let issue_id = store.resolve_issue_id(issue_id)?;
        let mut seen = BTreeSet::new();
        let mut requested: Vec<String> = Vec::new();
        for depends_on_id in depends_on_ids {
            let resolved = store.resolve_issue_id(depends_on_id)?;
            if seen.insert(resolved.clone()) {
                requested.push(resolved);
            }
        }
        let issue_id = issue_id.as_str();
        let index = store.issue_index(issue_id)?;
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
        result.active_blocker_ids =
            crate::bead::read::active_blocker_ids(&store.issues, issue_id);
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
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_add_references(
                beads_dir,
                view,
                issue_id,
                &references,
                now.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        let issue_id = store.resolve_issue_id(issue_id)?;
        let index = store.issue_index(&issue_id)?;
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
                &issue_id,
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
        if let Some(view) = MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_remove_references(
                beads_dir,
                view,
                issue_id,
                &references,
                now.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        let issue_id = store.resolve_issue_id(issue_id)?;
        let index = store.issue_index(&issue_id)?;
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
                &issue_id,
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

/// Cached `add_dependency` over the view: two point lookups plus one stream.
///
/// Both IDs resolve through the view, exactly as the locked replay load
/// does. Returns `Ok(None)` when the affected stream file is missing so
/// replay owns the corruption error.
fn try_cached_add_dependency(
    beads_dir: &Path,
    mut view: MutationView,
    issue_id: &str,
    depends_on_id: &str,
    now: Option<String>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
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
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let dep = crate::bead::wire::DependencyWire {
        issue_id: resolved.clone(),
        depends_on_id: resolved_dep.clone(),
        created_at: now.unwrap_or_else(now_utc),
        created_by: config.owner.clone(),
    };
    issue.dependencies.push(dep.clone());
    view.stage_issue(issue.clone());
    let stream_id = view.stream_id_for_issue(&issue.id)?;
    let Some(mut stream) = load_mutation_stream(beads_dir, &stream_id)? else {
        return Ok(None);
    };
    let base_len = stream.events.len();
    mint_stream_event(
        &mut stream,
        BeadEventOperationWire::DependencyAdded,
        BeadEventPayloadWire::DependencyAdded {
            dependency: dep.clone(),
        },
        &dep.created_at,
        &dep.created_by,
        &resolved,
    )?;
    let expected = vec![(issue.id.clone(), issue.clone())];
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        std::slice::from_ref(&stream),
        std::slice::from_ref(&base_len),
        &expected,
    )?;
    let Some(mut rows) = committed else {
        return Ok(None);
    };
    let _corrected = rows.pop().expect("one dependency row");
    // The replay outcome carries only the new edge (no issue row), so
    // the corrected row is verified by the commit and then dropped,
    // exactly as the staged write plus publish already applied it.
    let mut result = outcome("dep_add", true, vec![resolved]);
    result.dependency = Some(dep);
    Ok(Some(result))
}

/// Cached `remove_dependencies` over the view.
///
/// The whole batch validates before anything stages, so a bad batch
/// leaves bytes untouched. Blocker status comes from dependency-target
/// point lookups, never a whole-store status map.
fn try_cached_remove_dependencies(
    beads_dir: &Path,
    mut view: MutationView,
    issue_id: &str,
    depends_on_ids: &[String],
    now: Option<String>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
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
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let removed_at = now.unwrap_or_else(now_utc);
    let actor = config.owner.clone();
    let stream_id = view.stream_id_for_issue(&issue.id)?;
    let Some(mut stream) = load_mutation_stream(beads_dir, &stream_id)? else {
        return Ok(None);
    };
    let base_len = stream.events.len();
    for dependency in &removed {
        mint_stream_event(
            &mut stream,
            BeadEventOperationWire::DependencyRemoved,
            BeadEventPayloadWire::DependencyRemoved {
                dependency: dependency.clone(),
            },
            &removed_at,
            &actor,
            &resolved,
        )?;
    }
    let expected = vec![(issue.id.clone(), issue.clone())];
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        std::slice::from_ref(&stream),
        std::slice::from_ref(&base_len),
        &expected,
    )?;
    let Some(mut rows) = committed else {
        return Ok(None);
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
    result.active_blocker_ids = active_blockers_via_view(&view, &resolved)?;
    Ok(Some(result))
}

/// Cached `add_bead_references` over the view: one row plus one stream.
///
/// `references` are already normalized by the caller, exactly as replay
/// receives them. A no-op (nothing new) returns a no-change outcome
/// without writing.
fn try_cached_add_references(
    beads_dir: &Path,
    mut view: MutationView,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
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
        return Ok(Some(result));
    }
    issue.refs.extend(added.iter().cloned());
    view.stage_issue(issue.clone());
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let added_at = now.unwrap_or_else(now_utc);
    let actor = config.owner.clone();
    let stream_id = view.stream_id_for_issue(&issue.id)?;
    let Some(mut stream) = load_mutation_stream(beads_dir, &stream_id)? else {
        return Ok(None);
    };
    let base_len = stream.events.len();
    for reference in &added {
        mint_stream_event(
            &mut stream,
            BeadEventOperationWire::ReferenceAdded,
            BeadEventPayloadWire::ReferenceAdded {
                reference: reference.clone(),
            },
            &added_at,
            &actor,
            &resolved,
        )?;
    }
    let expected = vec![(issue.id.clone(), issue.clone())];
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        std::slice::from_ref(&stream),
        std::slice::from_ref(&base_len),
        &expected,
    )?;
    let Some(mut rows) = committed else {
        return Ok(None);
    };
    let issue = rows.pop().expect("one reference row");
    let mut result = outcome("ref_add", true, vec![resolved]);
    result.issue = Some(issue);
    result.references = added;
    Ok(Some(result))
}

/// Cached `remove_bead_references` over the view: one row plus one stream.
///
/// A no-op (nothing present) returns a no-change outcome without writing.
fn try_cached_remove_references(
    beads_dir: &Path,
    mut view: MutationView,
    issue_id: &str,
    references: &[String],
    now: Option<String>,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
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
        return Ok(Some(result));
    }
    let removed_set = removed.iter().collect::<HashSet<_>>();
    issue
        .refs
        .retain(|reference| !removed_set.contains(reference));
    view.stage_issue(issue.clone());
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let removed_at = now.unwrap_or_else(now_utc);
    let actor = config.owner.clone();
    let stream_id = view.stream_id_for_issue(&issue.id)?;
    let Some(mut stream) = load_mutation_stream(beads_dir, &stream_id)? else {
        return Ok(None);
    };
    let base_len = stream.events.len();
    for reference in &removed {
        mint_stream_event(
            &mut stream,
            BeadEventOperationWire::ReferenceRemoved,
            BeadEventPayloadWire::ReferenceRemoved {
                reference: reference.clone(),
            },
            &removed_at,
            &actor,
            &resolved,
        )?;
    }
    let expected = vec![(issue.id.clone(), issue.clone())];
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let committed = commit_staged_write(
        beads_dir,
        cache_path_buf.as_deref(),
        view.witness(),
        &config,
        std::slice::from_ref(&stream),
        std::slice::from_ref(&base_len),
        &expected,
    )?;
    let Some(mut rows) = committed else {
        return Ok(None);
    };
    let issue = rows.pop().expect("one reference row");
    let mut result = outcome("ref_rm", true, vec![resolved]);
    result.issue = Some(issue);
    result.references = removed;
    Ok(Some(result))
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

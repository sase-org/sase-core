use super::mutation_wire::BeadLinkProjectionRequestWire;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::store::link_mutation_error;
use super::store::not_found;
use super::store::now_utc;
use super::store::outcome;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::artifact_link::canonicalize_artifact_link_ref;
use crate::artifact_link::lookup_artifact_relation;
use crate::artifact_link::validate_artifact_link_description;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::artifact_link::BeadLinkWire;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::read::resolve_issue_id_in_issues;
use crate::bead::wire::BeadError;
use crate::bead::wire::IssueWire;
use std::path::Path;

/// Write one `LinkAdded` event on the bead identified by `issue_id`.
///
/// `direction` says which role `issue_id` plays in the row being recorded:
/// `Out` (the historical shape) when `issue_id` is the row's source, `In`
/// when it is the target. `target_ref` always names the *other* endpoint,
/// regardless of direction.
///
/// `Out` preserves the original undirected-relation consolidation: writing
/// the same undirected edge from either bead updates the one bead that
/// already holds it rather than creating a second stored copy.
/// `In` always writes directly on `issue_id`'s own stream — the caller
/// already knows which bead is which endpoint, so no holder redirection
/// applies.
#[allow(clippy::too_many_arguments)]
pub fn add_bead_link(
    beads_dir: &Path,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    description: &str,
    origin: ArtifactLinkOriginWire,
    direction: BeadLinkDirectionWire,
    uses: u64,
    now: Option<String>,
    operation_id: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let operation_id =
        validate_artifact_link_operation_id_option(operation_id)?;
    with_bead_mutation_lock(beads_dir, "add_link", || {
        let mut store = MutableStore::load(beads_dir)?;
        let source_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let target_ref =
            canonicalize_bead_link_target(&store.issues, target_ref)?;
        lookup_artifact_relation(relation).map_err(link_mutation_error)?;
        let description = validate_artifact_link_description(description)
            .map_err(link_mutation_error)?;
        let source_ref = canonical_bead_source_ref(&source_id);
        if source_ref == target_ref {
            return Err(BeadError::validation(
                "artifact link cannot target itself",
            ));
        }
        let holder_id = match direction {
            BeadLinkDirectionWire::Out => undirected_holder_issue_id(
                &store.issues,
                &source_id,
                &source_ref,
                &target_ref,
                relation,
            )?,
            BeadLinkDirectionWire::In => source_id.clone(),
        };
        let holder_index = store.issue_index(&holder_id)?;
        let stored_direction = if holder_id == source_id {
            direction
        } else {
            BeadLinkDirectionWire::Out
        };
        let event_target = if holder_id == source_id {
            target_ref.clone()
        } else {
            source_ref.clone()
        };
        if let Some(operation_id) = operation_id.as_deref() {
            if store.artifact_link_projection_receipt_seen_on(
                &holder_id,
                operation_id,
                BeadEventOperationWire::LinkAdded,
                &event_target,
                relation,
                stored_direction,
            )? {
                let mut result =
                    outcome("link_add", false, vec![source_id.clone()]);
                result.issue =
                    Some(store.issues[store.issue_index(&source_id)?].clone());
                return Ok(result);
            }
        }
        let existing =
            store.issues[holder_index].links.iter().position(|link| {
                link_matches(
                    link,
                    relation,
                    &target_ref,
                    &source_ref,
                    stored_direction,
                )
            });
        let stored_uses;
        if let Some(index) = existing {
            let current = &store.issues[holder_index].links[index];
            if operation_id.is_none()
                && current.description == description
                && current.origin == origin
            {
                let mut result =
                    outcome("link_add", false, vec![source_id.clone()]);
                result.issue =
                    Some(store.issues[store.issue_index(&source_id)?].clone());
                return Ok(result);
            }
            stored_uses = if origin.increments_uses() {
                current
                    .uses
                    .saturating_add(if uses == 0 { 1 } else { uses })
            } else {
                current.uses
            };
            store.issues[holder_index].links[index].description =
                description.clone();
            store.issues[holder_index].links[index].origin = origin;
            store.issues[holder_index].links[index].uses = stored_uses;
        } else {
            let stored_target = if holder_id == source_id {
                target_ref.clone()
            } else {
                source_ref.clone()
            };
            stored_uses = if uses == 0 { 1 } else { uses };
            store.issues[holder_index].links.push(BeadLinkWire {
                target_ref: stored_target,
                relation: relation.to_string(),
                description: description.clone(),
                origin,
                direction: stored_direction,
                uses: stored_uses,
            });
        }
        let added_at = now.unwrap_or_else(now_utc);
        let actor = store.config.owner.clone();
        store.append_issue_event(
            &holder_id,
            BeadEventOperationWire::LinkAdded,
            BeadEventPayloadWire::LinkAdded {
                target_ref: event_target,
                relation: relation.to_string(),
                description,
                origin,
                direction: stored_direction,
                uses: stored_uses,
                operation_id: operation_id.clone(),
            },
            &added_at,
            &actor,
        )?;
        store.save()?;
        let mut result = outcome("link_add", true, vec![source_id.clone()]);
        if holder_id != source_id {
            result.issue_ids.push(holder_id);
        }
        result.issue =
            Some(store.issues[store.issue_index(&source_id)?].clone());
        Ok(result)
    })
}

/// Install the exact artifact-link projection for one bead endpoint.
///
/// Unlike [`add_bead_link`], this does not apply read-style counter
/// accumulation. It records a projection receipt scoped to the immutable
/// artifact-link operation plus the stored edge/direction, then sets the
/// materialized bead link to the supplied reduced state or records its
/// absence with a `LinkRemoved` event.
#[allow(clippy::too_many_arguments)]
pub fn set_bead_link_projection(
    beads_dir: &Path,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
    present: bool,
    description: Option<String>,
    origin: Option<ArtifactLinkOriginWire>,
    uses: u64,
    now: Option<String>,
    operation_id: String,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    set_bead_link_projections(
        beads_dir,
        &[BeadLinkProjectionRequestWire {
            issue_id: issue_id.to_string(),
            target_ref: target_ref.to_string(),
            relation: relation.to_string(),
            direction,
            present,
            operation_id,
            description,
            origin,
            uses,
            now,
        }],
    )
}

/// Install an ordered batch of bead-link projections under one lock.
///
/// Loads the store once, applies every request with the same validation,
/// canonicalization, holder selection, receipt, and event payload as
/// [`set_bead_link_projection`], and saves once only when at least one
/// request changed state. An invalid request leaves the event stream and
/// `issues.jsonl` projection untouched.
pub fn set_bead_link_projections(
    beads_dir: &Path,
    requests: &[BeadLinkProjectionRequestWire],
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let prepared = requests
        .iter()
        .map(PreparedLinkProjection::from_request)
        .collect::<Result<Vec<_>, _>>()?;
    with_bead_mutation_lock(beads_dir, "set_link_projection", || {
        apply_prepared_link_projections(beads_dir, &prepared)
    })
}

struct PreparedLinkProjection {
    issue_id: String,
    target_ref: String,
    relation: String,
    direction: BeadLinkDirectionWire,
    operation_id: String,
    desired: Option<(String, ArtifactLinkOriginWire, u64)>,
    now: Option<String>,
}

impl PreparedLinkProjection {
    fn from_request(
        request: &BeadLinkProjectionRequestWire,
    ) -> Result<Self, BeadError> {
        let operation_id = validate_artifact_link_operation_id_option(Some(
            request.operation_id.clone(),
        ))?
        .ok_or_else(|| {
            BeadError::validation(
                "artifact link projection operation_id is required",
            )
        })?;
        let desired = if request.present {
            let description = validate_artifact_link_description(
                request.description.as_deref().unwrap_or(""),
            )
            .map_err(link_mutation_error)?;
            let origin = request.origin.ok_or_else(|| {
                BeadError::validation(
                    "artifact link projection origin is required when present",
                )
            })?;
            Some((
                description,
                origin,
                if request.uses == 0 { 1 } else { request.uses },
            ))
        } else {
            None
        };
        lookup_artifact_relation(&request.relation)
            .map_err(link_mutation_error)?;
        Ok(Self {
            issue_id: request.issue_id.clone(),
            target_ref: request.target_ref.clone(),
            relation: request.relation.clone(),
            direction: request.direction,
            operation_id,
            desired,
            now: request.now.clone(),
        })
    }
}

struct LinkProjectionApply {
    source_id: String,
    holder_id: String,
    changed: bool,
}

fn apply_prepared_link_projections(
    beads_dir: &Path,
    requests: &[PreparedLinkProjection],
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let mut store = MutableStore::load(beads_dir)?;
    let mut changed = false;
    let mut issue_ids = Vec::new();
    let mut source_ids = Vec::new();
    for request in requests {
        let applied = apply_prepared_link_projection(&mut store, request)?;
        if !source_ids.contains(&applied.source_id) {
            source_ids.push(applied.source_id.clone());
        }
        if !issue_ids.contains(&applied.source_id) {
            issue_ids.push(applied.source_id.clone());
        }
        if applied.changed
            && applied.holder_id != applied.source_id
            && !issue_ids.contains(&applied.holder_id)
        {
            issue_ids.push(applied.holder_id);
        }
        changed |= applied.changed;
    }
    if changed {
        store.save()?;
    }
    let mut result = outcome("link_project", changed, issue_ids);
    if let Some(source_id) = source_ids.first() {
        result.issue =
            Some(store.issues[store.issue_index(source_id)?].clone());
    }
    Ok(result)
}

fn apply_prepared_link_projection(
    store: &mut MutableStore,
    request: &PreparedLinkProjection,
) -> Result<LinkProjectionApply, BeadError> {
    let source_id =
        resolve_issue_id_in_issues(&store.issues, &request.issue_id)?;
    let target_ref =
        canonicalize_bead_link_target(&store.issues, &request.target_ref)?;
    let source_ref = canonical_bead_source_ref(&source_id);
    if source_ref == target_ref {
        return Err(BeadError::validation(
            "artifact link cannot target itself",
        ));
    }
    let holder_id = match request.direction {
        BeadLinkDirectionWire::Out => undirected_holder_issue_id(
            &store.issues,
            &source_id,
            &source_ref,
            &target_ref,
            &request.relation,
        )?,
        BeadLinkDirectionWire::In => source_id.clone(),
    };
    let stored_direction = if holder_id == source_id {
        request.direction
    } else {
        BeadLinkDirectionWire::Out
    };
    let event_target = if holder_id == source_id {
        target_ref
    } else {
        source_ref
    };
    let present = request.desired.is_some();
    let event_operation = if present {
        BeadEventOperationWire::LinkAdded
    } else {
        BeadEventOperationWire::LinkRemoved
    };
    let receipt_seen = store.artifact_link_projection_receipt_seen_on(
        &holder_id,
        &request.operation_id,
        event_operation,
        &event_target,
        &request.relation,
        stored_direction,
    )?;

    let holder_index = store.issue_index(&holder_id)?;
    let existing = store.issues[holder_index].links.iter().position(|link| {
        link.target_ref == event_target
            && link.relation == request.relation
            && link.direction == stored_direction
    });
    if receipt_seen
        && link_projection_matches_desired(
            existing.map(|index| &store.issues[holder_index].links[index]),
            &request.relation,
            &event_target,
            stored_direction,
            request.desired.as_ref(),
        )
    {
        return Ok(LinkProjectionApply {
            source_id,
            holder_id,
            changed: false,
        });
    }
    let timestamp = request.now.clone().unwrap_or_else(now_utc);
    let actor = store.config.owner.clone();
    match &request.desired {
        Some((description, origin, desired_uses)) => {
            if let Some(index) = existing {
                store.issues[holder_index].links[index].description =
                    description.clone();
                store.issues[holder_index].links[index].origin = *origin;
                store.issues[holder_index].links[index].uses = *desired_uses;
            } else {
                store.issues[holder_index].links.push(BeadLinkWire {
                    target_ref: event_target.clone(),
                    relation: request.relation.clone(),
                    description: description.clone(),
                    origin: *origin,
                    direction: stored_direction,
                    uses: *desired_uses,
                });
            }
            store.append_issue_event(
                &holder_id,
                BeadEventOperationWire::LinkAdded,
                BeadEventPayloadWire::LinkAdded {
                    target_ref: event_target,
                    relation: request.relation.clone(),
                    description: description.clone(),
                    origin: *origin,
                    direction: stored_direction,
                    uses: *desired_uses,
                    operation_id: Some(request.operation_id.clone()),
                },
                &timestamp,
                &actor,
            )?;
        }
        None => {
            if existing.is_some() {
                store.issues[holder_index].links.retain(|link| {
                    !(link.target_ref == event_target
                        && link.relation == request.relation
                        && link.direction == stored_direction)
                });
            }
            store.append_issue_event(
                &holder_id,
                BeadEventOperationWire::LinkRemoved,
                BeadEventPayloadWire::LinkRemoved {
                    target_ref: event_target,
                    relation: request.relation.clone(),
                    direction: stored_direction,
                    operation_id: Some(request.operation_id.clone()),
                },
                &timestamp,
                &actor,
            )?;
        }
    }
    Ok(LinkProjectionApply {
        source_id,
        holder_id,
        changed: true,
    })
}

/// Remove one stored bead link, identified the same way it was added.
///
/// See [`add_bead_link`] for the meaning of `direction`. `Out` preserves the
/// original peer-scan that lets removing an undirected edge from either
/// bead find the single holder that actually stores it. `In` only ever
/// looks at `issue_id`'s own stream.
pub fn remove_bead_link(
    beads_dir: &Path,
    issue_id: &str,
    target_ref: &str,
    relation: Option<&str>,
    direction: BeadLinkDirectionWire,
    now: Option<String>,
    operation_id: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let operation_id =
        validate_artifact_link_operation_id_option(operation_id)?;
    if let Some(relation) = relation {
        lookup_artifact_relation(relation).map_err(link_mutation_error)?;
    }
    with_bead_mutation_lock(beads_dir, "remove_link", || {
        let mut store = MutableStore::load(beads_dir)?;
        let source_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let target_ref =
            canonicalize_bead_link_target(&store.issues, target_ref)?;
        let source_ref = canonical_bead_source_ref(&source_id);
        let removed_at = now.unwrap_or_else(now_utc);
        let actor = store.config.owner.clone();
        let mut removed: Vec<(String, String, String, BeadLinkDirectionWire)> =
            Vec::new();
        collect_removable_bead_links(
            &store.issues,
            &source_id,
            &source_ref,
            &target_ref,
            relation,
            direction,
            &mut removed,
        )?;
        if removed.is_empty() {
            let mut result = outcome("link_rm", false, vec![source_id.clone()]);
            result.issue =
                Some(store.issues[store.issue_index(&source_id)?].clone());
            return Ok(result);
        }
        let mut touched: Vec<String> = Vec::new();
        for (holder_id, stored_target, stored_relation, stored_direction) in
            &removed
        {
            if let Some(operation_id) = operation_id.as_deref() {
                if store.artifact_link_projection_receipt_seen_on(
                    holder_id,
                    operation_id,
                    BeadEventOperationWire::LinkRemoved,
                    stored_target,
                    stored_relation,
                    *stored_direction,
                )? {
                    continue;
                }
            }
            let index = store.issue_index(holder_id)?;
            store.issues[index].links.retain(|link| {
                !(link.target_ref == *stored_target
                    && link.relation == *stored_relation
                    && link.direction == *stored_direction)
            });
            store.append_issue_event(
                holder_id,
                BeadEventOperationWire::LinkRemoved,
                BeadEventPayloadWire::LinkRemoved {
                    target_ref: stored_target.clone(),
                    relation: stored_relation.clone(),
                    direction: *stored_direction,
                    operation_id: operation_id.clone(),
                },
                &removed_at,
                &actor,
            )?;
            if !touched.contains(holder_id) {
                touched.push(holder_id.clone());
            }
        }
        store.save()?;
        let mut issue_ids = vec![source_id.clone()];
        for holder_id in touched {
            if holder_id != source_id {
                issue_ids.push(holder_id);
            }
        }
        let mut result = outcome("link_rm", true, issue_ids);
        result.issue =
            Some(store.issues[store.issue_index(&source_id)?].clone());
        Ok(result)
    })
}

fn canonical_bead_source_ref(issue_id: &str) -> String {
    format!("bead:{issue_id}")
}

fn canonicalize_bead_link_target(
    issues: &[IssueWire],
    target_ref: &str,
) -> Result<String, BeadError> {
    let canonical = canonicalize_artifact_link_ref(target_ref)
        .map_err(link_mutation_error)?;
    let Some(bead_id) = canonical.strip_prefix("bead:") else {
        return Ok(canonical);
    };
    match resolve_issue_id_in_issues(issues, bead_id) {
        Ok(resolved) => Ok(format!("bead:{resolved}")),
        Err(_) => Ok(canonical),
    }
}

fn undirected_holder_issue_id(
    issues: &[IssueWire],
    source_id: &str,
    source_ref: &str,
    target_ref: &str,
    relation: &str,
) -> Result<String, BeadError> {
    let directed = lookup_artifact_relation(relation)
        .map_err(link_mutation_error)?
        .directed;
    if directed {
        return Ok(source_id.to_string());
    }
    if let Some(target_id) = target_ref.strip_prefix("bead:") {
        if let Ok(resolved) = resolve_issue_id_in_issues(issues, target_id) {
            if let Some(issue) =
                issues.iter().find(|issue| issue.id == resolved)
            {
                if issue.links.iter().any(|link| {
                    link.relation == relation && link.target_ref == source_ref
                }) {
                    return Ok(resolved);
                }
            }
        }
    }
    Ok(source_id.to_string())
}

fn link_matches(
    link: &BeadLinkWire,
    relation: &str,
    target_ref: &str,
    source_ref: &str,
    direction: BeadLinkDirectionWire,
) -> bool {
    if link.relation != relation || link.direction != direction {
        return false;
    }
    link.target_ref == target_ref || link.target_ref == source_ref
}

fn link_projection_matches_desired(
    link: Option<&BeadLinkWire>,
    relation: &str,
    target_ref: &str,
    direction: BeadLinkDirectionWire,
    desired: Option<&(String, ArtifactLinkOriginWire, u64)>,
) -> bool {
    match (link, desired) {
        (None, None) => true,
        (Some(link), Some((description, origin, uses))) => {
            link.target_ref == target_ref
                && link.relation == relation
                && link.direction == direction
                && link.description == *description
                && link.origin == *origin
                && link.uses == *uses
        }
        _ => false,
    }
}

fn validate_artifact_link_operation_id_option(
    operation_id: Option<String>,
) -> Result<Option<String>, BeadError> {
    let Some(raw) = operation_id else {
        return Ok(None);
    };
    let value = raw.trim();
    if value.len() != 32 || !is_lowercase_hex(value) {
        return Err(BeadError::validation(
            "artifact link operation_id must be 32 lowercase hexadecimal characters",
        ));
    }
    Ok(Some(value.to_string()))
}

fn is_lowercase_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[allow(clippy::too_many_arguments)]
fn collect_removable_bead_links(
    issues: &[IssueWire],
    source_id: &str,
    source_ref: &str,
    target_ref: &str,
    relation: Option<&str>,
    direction: BeadLinkDirectionWire,
    removed: &mut Vec<(String, String, String, BeadLinkDirectionWire)>,
) -> Result<(), BeadError> {
    let source = issues
        .iter()
        .find(|issue| issue.id == source_id)
        .ok_or_else(|| not_found(source_id))?;
    for link in &source.links {
        if link.target_ref == *target_ref
            && link.direction == direction
            && relation
                .map(|wanted| link.relation == wanted)
                .unwrap_or(true)
        {
            removed.push((
                source_id.to_string(),
                link.target_ref.clone(),
                link.relation.clone(),
                link.direction,
            ));
        }
    }
    if direction != BeadLinkDirectionWire::Out {
        return Ok(());
    }
    let Some(target_id) = target_ref.strip_prefix("bead:") else {
        return Ok(());
    };
    let Ok(resolved) = resolve_issue_id_in_issues(issues, target_id) else {
        return Ok(());
    };
    let Some(peer) = issues.iter().find(|issue| issue.id == resolved) else {
        return Ok(());
    };
    for link in &peer.links {
        if link.target_ref == *source_ref
            && relation
                .map(|wanted| {
                    wanted == link.relation
                        && lookup_artifact_relation(&link.relation)
                            .map(|item| !item.directed)
                            .unwrap_or(false)
                })
                .unwrap_or_else(|| {
                    lookup_artifact_relation(&link.relation)
                        .map(|item| !item.directed)
                        .unwrap_or(false)
                })
        {
            removed.push((
                resolved.clone(),
                link.target_ref.clone(),
                link.relation.clone(),
                link.direction,
            ));
        }
    }
    Ok(())
}

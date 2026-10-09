use super::mutation_wire::BeadLinkProjectionRequestWire;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::link_mutation_error;
use super::store::now_utc;
use super::store::outcome;
use super::view::MutationView;
use crate::artifact_link::canonicalize_artifact_link_ref;
use crate::artifact_link::lookup_artifact_relation;
use crate::artifact_link::validate_artifact_link_description;
use crate::artifact_link::ArtifactLinkOriginWire;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::artifact_link::BeadLinkWire;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::BeadError;
use std::collections::BTreeSet;
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
    run_mutation(beads_dir, "add_link", |view| {
        run_add_link(
            view,
            issue_id,
            target_ref,
            relation,
            description,
            origin,
            direction,
            uses,
            now.clone(),
            operation_id.clone(),
        )
    })
}

/// The single `add_bead_link` algorithm, over the view on both backings.
///
/// Resolution, canonicalization, holder selection, receipt idempotency,
/// deduplication and `uses` accumulation all read through the view, events
/// mint with `stage_event`, and one `commit` persists both backings. A
/// missing stream file or manifest declines to the replay backing before
/// any durable write; store and decode faults stay errors.
#[allow(clippy::too_many_arguments)]
fn run_add_link(
    view: &mut MutationView,
    issue_id: &str,
    target_ref: &str,
    relation: &str,
    description: &str,
    origin: ArtifactLinkOriginWire,
    direction: BeadLinkDirectionWire,
    uses: u64,
    now: Option<String>,
    operation_id: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let source_id = view.resolve(issue_id)?;
    let target_ref = canonicalize_bead_link_target_via_view(view, target_ref)?;
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
        BeadLinkDirectionWire::Out => undirected_holder_issue_id_via_view(
            view,
            &source_id,
            &source_ref,
            &target_ref,
            relation,
        )?,
        BeadLinkDirectionWire::In => source_id.clone(),
    };
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
        let Some(seen) = link_add_receipt_seen(
            view,
            &holder_id,
            operation_id,
            &event_target,
            relation,
            stored_direction,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        if seen {
            let mut result =
                outcome("link_add", false, vec![source_id.clone()]);
            result.issue = Some(view.get(&source_id)?);
            return Ok(MutationStep::Done(result));
        }
    }
    let mut holder = view.get(&holder_id)?;
    let existing = holder.links.iter().position(|link| {
        link_matches(link, relation, &target_ref, &source_ref, stored_direction)
    });
    let stored_uses;
    if let Some(index) = existing {
        let current = &holder.links[index];
        if operation_id.is_none()
            && current.description == description
            && current.origin == origin
        {
            let mut result =
                outcome("link_add", false, vec![source_id.clone()]);
            result.issue = Some(view.get(&source_id)?);
            return Ok(MutationStep::Done(result));
        }
        stored_uses = if origin.increments_uses() {
            current
                .uses
                .saturating_add(if uses == 0 { 1 } else { uses })
        } else {
            current.uses
        };
        holder.links[index].description = description.clone();
        holder.links[index].origin = origin;
        holder.links[index].uses = stored_uses;
    } else {
        let stored_target = if holder_id == source_id {
            target_ref.clone()
        } else {
            source_ref.clone()
        };
        stored_uses = if uses == 0 { 1 } else { uses };
        holder.links.push(BeadLinkWire {
            target_ref: stored_target,
            relation: relation.to_string(),
            description: description.clone(),
            origin,
            direction: stored_direction,
            uses: stored_uses,
        });
    }
    let added_at = now.unwrap_or_else(now_utc);
    let actor = view.config()?.owner.clone();
    holder.validate()?;
    view.stage_issue(holder.clone());
    let Some(_) = view.stage_event(
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
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    let Some(mut rows) = view.commit(std::slice::from_ref(&holder_id))? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let corrected = rows.pop().expect("one linked row");
    let mut issue_ids = vec![source_id.clone()];
    if holder_id != source_id {
        issue_ids.push(holder_id.clone());
    }
    let mut result = outcome("link_add", true, issue_ids);
    result.issue = Some(if holder_id == source_id {
        corrected
    } else {
        view.get(&source_id)?
    });
    Ok(MutationStep::Done(result))
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
    run_mutation(beads_dir, "set_link_projection", |view| {
        run_link_projections(view, &prepared)
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

/// The single link-projection algorithm, over the view on both backings.
///
/// One lock, one commit for the whole batch: every request resolves,
/// canonicalizes, selects its holder and checks its receipt through the
/// view, mints with `stage_event`, and stages its holder. Receipts combine
/// the stored stream with the operation IDs this batch already staged, so
/// a repeated operation ID in one batch stays idempotent exactly as on
/// the replay path. A missing stream file or manifest declines the whole
/// batch to the replay backing before any durable write.
fn run_link_projections(
    view: &mut MutationView,
    requests: &[PreparedLinkProjection],
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let actor = view.config()?.owner.clone();
    let mut changed = false;
    let mut issue_ids: Vec<String> = Vec::new();
    let mut source_ids: Vec<String> = Vec::new();
    let mut touched_holders: Vec<String> = Vec::new();
    // Operation IDs this batch already staged, keyed by holder stream and
    // edge identity: the view's file-backed receipt check cannot see them
    // yet, while the replay oracle's in-memory streams could.
    let mut staged_receipts: BTreeSet<String> = BTreeSet::new();
    for request in requests {
        let source_id = view.resolve(&request.issue_id)?;
        let target_ref =
            canonicalize_bead_link_target_via_view(view, &request.target_ref)?;
        let source_ref = canonical_bead_source_ref(&source_id);
        if source_ref == target_ref {
            return Err(BeadError::validation(
                "artifact link cannot target itself",
            ));
        }
        let holder_id = match request.direction {
            BeadLinkDirectionWire::Out => undirected_holder_issue_id_via_view(
                view,
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
            target_ref.clone()
        } else {
            source_ref.clone()
        };
        let present = request.desired.is_some();
        let event_operation = if present {
            BeadEventOperationWire::LinkAdded
        } else {
            BeadEventOperationWire::LinkRemoved
        };
        let receipt_key = format!(
            "{holder_id}\0{}\0{event_target}\0{}\0{stored_direction:?}",
            request.operation_id, request.relation,
        );
        let staged_seen = staged_receipts.contains(&receipt_key);
        let Some(file_seen) = link_projection_receipt_seen(
            view,
            &holder_id,
            &request.operation_id,
            event_operation,
            &event_target,
            &request.relation,
            stored_direction,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        let receipt_seen = staged_seen || file_seen;
        let mut holder = view.get(&holder_id)?;
        let existing = holder.links.iter().position(|link| {
            link.target_ref == event_target
                && link.relation == request.relation
                && link.direction == stored_direction
        });
        if receipt_seen
            && link_projection_matches_desired(
                existing.map(|index| &holder.links[index]),
                &request.relation,
                &event_target,
                stored_direction,
                request.desired.as_ref(),
            )
        {
            if !source_ids.contains(&source_id) {
                source_ids.push(source_id.clone());
            }
            if !issue_ids.contains(&source_id) {
                issue_ids.push(source_id.clone());
            }
            continue;
        }
        let timestamp = request.now.clone().unwrap_or_else(now_utc);
        match &request.desired {
            Some((description, origin, desired_uses)) => {
                if let Some(index) = existing {
                    holder.links[index].description = description.clone();
                    holder.links[index].origin = *origin;
                    holder.links[index].uses = *desired_uses;
                } else {
                    holder.links.push(BeadLinkWire {
                        target_ref: event_target.clone(),
                        relation: request.relation.clone(),
                        description: description.clone(),
                        origin: *origin,
                        direction: stored_direction,
                        uses: *desired_uses,
                    });
                }
                holder.validate()?;
                view.stage_issue(holder.clone());
                let Some(_) = view.stage_event(
                    &holder_id,
                    BeadEventOperationWire::LinkAdded,
                    BeadEventPayloadWire::LinkAdded {
                        target_ref: event_target.clone(),
                        relation: request.relation.clone(),
                        description: description.clone(),
                        origin: *origin,
                        direction: stored_direction,
                        uses: *desired_uses,
                        operation_id: Some(request.operation_id.clone()),
                    },
                    &timestamp,
                    &actor,
                )?
                else {
                    return Ok(MutationStep::NeedsReplay);
                };
            }
            None => {
                if existing.is_some() {
                    holder.links.retain(|link| {
                        !(link.target_ref == event_target
                            && link.relation == request.relation
                            && link.direction == stored_direction)
                    });
                }
                holder.validate()?;
                view.stage_issue(holder.clone());
                let Some(_) = view.stage_event(
                    &holder_id,
                    BeadEventOperationWire::LinkRemoved,
                    BeadEventPayloadWire::LinkRemoved {
                        target_ref: event_target.clone(),
                        relation: request.relation.clone(),
                        direction: stored_direction,
                        operation_id: Some(request.operation_id.clone()),
                    },
                    &timestamp,
                    &actor,
                )?
                else {
                    return Ok(MutationStep::NeedsReplay);
                };
            }
        }
        staged_receipts.insert(receipt_key);
        if !touched_holders.contains(&holder_id) {
            touched_holders.push(holder_id.clone());
        }
        if !source_ids.contains(&source_id) {
            source_ids.push(source_id.clone());
        }
        if !issue_ids.contains(&source_id) {
            issue_ids.push(source_id.clone());
        }
        if holder_id != source_id && !issue_ids.contains(&holder_id) {
            issue_ids.push(holder_id.clone());
        }
        changed = true;
    }
    if !changed {
        let mut result = outcome("link_project", false, issue_ids);
        if let Some(source_id) = source_ids.first() {
            result.issue = Some(view.get(source_id)?);
        }
        return Ok(MutationStep::Done(result));
    }
    let Some(committed) = view.commit(&touched_holders)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut result = outcome("link_project", true, issue_ids);
    if let Some(source_id) = source_ids.first() {
        result.issue = Some(
            committed
                .iter()
                .find(|issue| &issue.id == source_id)
                .cloned()
                .unwrap_or(view.get(source_id)?),
        );
    }
    Ok(MutationStep::Done(result))
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
    run_mutation(beads_dir, "remove_link", |view| {
        run_remove_link(
            view,
            issue_id,
            target_ref,
            relation,
            direction,
            now.clone(),
            operation_id.clone(),
        )
    })
}

/// The single `remove_bead_link` algorithm, over the view on both backings.
///
/// Collects the removable edges through the view (the source row's matching
/// links plus, for `Out`, the peer's undirected reverse links), skips
/// receipted removals, mints one `LinkRemoved` per remaining holder with
/// `stage_event`, and finishes with one `commit`. When every collected
/// removal is already receipted the outcome still reports `changed: true`
/// without writing, exactly as the replay path does after its save. A
/// missing stream file or manifest declines before any durable write.
#[allow(clippy::too_many_arguments)]
fn run_remove_link(
    view: &mut MutationView,
    issue_id: &str,
    target_ref: &str,
    relation: Option<&str>,
    direction: BeadLinkDirectionWire,
    now: Option<String>,
    operation_id: Option<String>,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let source_id = view.resolve(issue_id)?;
    let target_ref = canonicalize_bead_link_target_via_view(view, target_ref)?;
    let source_ref = canonical_bead_source_ref(&source_id);
    let removed_at = now.unwrap_or_else(now_utc);
    let actor = view.config()?.owner.clone();
    let mut removed: Vec<(String, String, String, BeadLinkDirectionWire)> =
        Vec::new();
    collect_removable_bead_links_via_view(
        view,
        &source_id,
        &source_ref,
        &target_ref,
        relation,
        direction,
        &mut removed,
    )?;
    if removed.is_empty() {
        let mut result = outcome("link_rm", false, vec![source_id.clone()]);
        result.issue = Some(view.get(&source_id)?);
        return Ok(MutationStep::Done(result));
    }
    let mut touched: Vec<String> = Vec::new();
    // Receipts this removal already staged, mirroring the replay path where
    // the in-memory streams carry staged events.
    let mut staged_receipts: BTreeSet<String> = BTreeSet::new();
    for (holder_id, stored_target, stored_relation, stored_direction) in
        &removed
    {
        if let Some(operation_id) = operation_id.as_deref() {
            let receipt_key = format!(
                "{holder_id}\0{operation_id}\0{stored_target}\0{stored_relation}\0{stored_direction:?}",
            );
            if !staged_receipts.contains(&receipt_key) {
                let Some(seen) = link_removal_receipt_seen(
                    view,
                    holder_id,
                    operation_id,
                    stored_target,
                    stored_relation,
                    *stored_direction,
                )?
                else {
                    return Ok(MutationStep::NeedsReplay);
                };
                if seen {
                    continue;
                }
            } else {
                continue;
            }
            // Record the key now so a repeated holder edge later in this
            // same removal stays skipped; a minted removal below inserts it
            // again, which a set absorbs.
            staged_receipts.insert(receipt_key);
        }
        let mut holder = view.get(holder_id)?;
        holder.links.retain(|link| {
            !(link.target_ref == *stored_target
                && link.relation == *stored_relation
                && link.direction == *stored_direction)
        });
        holder.validate()?;
        view.stage_issue(holder.clone());
        let Some(_) = view.stage_event(
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
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        if !touched.contains(holder_id) {
            touched.push(holder_id.clone());
        }
    }
    if touched.is_empty() {
        let mut result = outcome("link_rm", true, vec![source_id.clone()]);
        result.issue = Some(view.get(&source_id)?);
        return Ok(MutationStep::Done(result));
    }
    let Some(committed) = view.commit(&touched)? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let mut issue_ids = vec![source_id.clone()];
    for holder_id in &touched {
        if holder_id != &source_id {
            issue_ids.push(holder_id.clone());
        }
    }
    let mut result = outcome("link_rm", true, issue_ids);
    result.issue = Some(
        committed
            .iter()
            .find(|issue| issue.id == source_id)
            .cloned()
            .unwrap_or(view.get(&source_id)?),
    );
    Ok(MutationStep::Done(result))
}

fn canonical_bead_source_ref(issue_id: &str) -> String {
    format!("bead:{issue_id}")
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

/// Canonicalize a link target through the view, resolving `bead:` IDs
/// against the affected rows only. An unresolvable bead ID keeps its
/// canonical spelling, exactly as the replay path does.
fn canonicalize_bead_link_target_via_view(
    view: &MutationView,
    target_ref: &str,
) -> Result<String, BeadError> {
    let canonical = canonicalize_artifact_link_ref(target_ref)
        .map_err(link_mutation_error)?;
    let Some(bead_id) = canonical.strip_prefix("bead:") else {
        return Ok(canonical);
    };
    match view.resolve(bead_id) {
        Ok(resolved) => Ok(format!("bead:{resolved}")),
        Err(_) => Ok(canonical),
    }
}

/// Undirected holder selection through the view: a directed relation stays
/// on the source, while an undirected edge consolidates onto the peer that
/// already holds the reverse link. Only the source and the candidate peer
/// rows load.
fn undirected_holder_issue_id_via_view(
    view: &MutationView,
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
        if let Ok(resolved) = view.resolve(target_id) {
            if let Ok(peer) = view.get(&resolved) {
                if peer.links.iter().any(|link| {
                    link.relation == relation && link.target_ref == source_ref
                }) {
                    return Ok(resolved);
                }
            }
        }
    }
    Ok(source_id.to_string())
}

/// Removable-link collection through the view: the source row's matching
/// links plus, for `Out`, the peer's undirected reverse links. A missing or
/// ambiguous peer contributes nothing, exactly as on the replay path.
#[allow(clippy::too_many_arguments)]
fn collect_removable_bead_links_via_view(
    view: &MutationView,
    source_id: &str,
    source_ref: &str,
    target_ref: &str,
    relation: Option<&str>,
    direction: BeadLinkDirectionWire,
    removed: &mut Vec<(String, String, String, BeadLinkDirectionWire)>,
) -> Result<(), BeadError> {
    let source = view.get(source_id)?;
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
    let Ok(resolved) = view.resolve(target_id) else {
        return Ok(());
    };
    let Ok(peer) = view.get(&resolved) else {
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

/// `LinkAdded` receipt check for the single add algorithm.
///
/// Reads the stored holder stream through the view. Returns `Ok(None)`
/// when the caller must decline to the replay backing: an unreadable
/// stream file on the cached backing, where replay owns the corruption
/// error. Store and decode faults stay errors.
fn link_add_receipt_seen(
    view: &MutationView,
    holder_id: &str,
    operation_id: &str,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
) -> Result<Option<bool>, BeadError> {
    match view.projection_receipt_seen(
        holder_id,
        operation_id,
        BeadEventOperationWire::LinkAdded,
        target_ref,
        relation,
        direction,
    ) {
        Ok(seen) => Ok(Some(seen)),
        Err(error) if error.kind == "io" && view.is_cached() => Ok(None),
        Err(error) => Err(error),
    }
}

/// Projection receipt check for the single batch algorithm.
///
/// Same decline contract as [`link_add_receipt_seen`]: an unreadable
/// stream file on the cached backing declines the whole batch, while
/// genuine errors propagate.
fn link_projection_receipt_seen(
    view: &MutationView,
    holder_id: &str,
    operation_id: &str,
    operation: BeadEventOperationWire,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
) -> Result<Option<bool>, BeadError> {
    match view.projection_receipt_seen(
        holder_id,
        operation_id,
        operation,
        target_ref,
        relation,
        direction,
    ) {
        Ok(seen) => Ok(Some(seen)),
        Err(error) if error.kind == "io" && view.is_cached() => Ok(None),
        Err(error) => Err(error),
    }
}

/// `LinkRemoved` receipt check for the single remove algorithm.
///
/// Same decline contract as [`link_add_receipt_seen`].
fn link_removal_receipt_seen(
    view: &MutationView,
    holder_id: &str,
    operation_id: &str,
    target_ref: &str,
    relation: &str,
    direction: BeadLinkDirectionWire,
) -> Result<Option<bool>, BeadError> {
    match view.projection_receipt_seen(
        holder_id,
        operation_id,
        BeadEventOperationWire::LinkRemoved,
        target_ref,
        relation,
        direction,
    ) {
        Ok(seen) => Ok(Some(seen)),
        Err(error) if error.kind == "io" && view.is_cached() => Ok(None),
        Err(error) => Err(error),
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

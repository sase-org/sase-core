use super::mutation_wire::BeadCreateRequestWire;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::notes_update::append_note_to_store;
use super::store::next_child_id;
use super::store::next_top_level_counter;
use super::store::normalize_model;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::store::to_base36;
use super::store::with_bead_mutation_lock;
use super::store::MutableStore;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::normalize_creation_reason;
use crate::bead::wire::validate_unique_external_refs;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use std::fs;
use std::path::Path;

pub fn init_store(
    root_dir: &Path,
    beads_dirname: &str,
    issue_prefix: &str,
    owner: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let beads_dir = root_dir.join(beads_dirname);
    fs::create_dir_all(&beads_dir)?;
    save_config(&beads_dir, &default_config(issue_prefix, owner))?;
    if !beads_dir.join("issues.jsonl").exists() {
        fs::write(beads_dir.join("issues.jsonl"), "")?;
    }
    if !beads_dir.join("beads.db").exists() {
        fs::write(beads_dir.join("beads.db"), "")?;
    }
    Ok(outcome("init", true, Vec::new()))
}

pub fn create_issue(
    beads_dir: &Path,
    request: BeadCreateRequestWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if request.issue_type == IssueTypeWire::Task && request.size.is_none() {
        return Err(BeadError::validation(
            "new task issue creation requires an explicit size",
        ));
    }
    if request.issue_type == IssueTypeWire::Task && request.task_type.is_none()
    {
        return Err(BeadError::validation(
            "new task issue creation requires an explicit task type",
        ));
    }
    // Gate the reason before the lock opens a mutation: a blank or
    // overlong value never reaches the store, while an absent one stays
    // the historical empty-reason state for older clients.
    let creation_reason =
        normalize_creation_reason(request.creation_reason.as_deref())?;
    with_bead_mutation_lock(beads_dir, "create", || {
        let mut store = MutableStore::load(beads_dir)?;
        let tier = default_create_tier(&request);
        let references = normalize_references(&request.refs)?;
        let now = request.now.unwrap_or_else(now_utc);
        let owner = store.config.owner.clone();
        let created_by = request
            .created_by
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                (request.issue_type == IssueTypeWire::Phase)
                    .then_some(request.parent_id.as_deref())
                    .flatten()
                    .and_then(|parent_id| {
                        store.issues.iter().find(|issue| issue.id == parent_id)
                    })
                    .map(|parent| parent.created_by.trim())
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| owner.clone());
        let issue_id = match request.parent_id.as_deref() {
            Some(parent_id) => next_child_id(parent_id, &store.issues),
            None => {
                let counter = next_top_level_counter(
                    &store.config.issue_prefix,
                    store.config.next_counter,
                    &store.issues,
                );
                store.config.next_counter = counter + 1;
                format!("{}-{}", store.config.issue_prefix, to_base36(counter))
            }
        };

        let initial_note = request.notes;
        let issue = IssueWire {
            id: issue_id,
            title: request.title,
            status: StatusWire::Open,
            issue_type: request.issue_type.clone(),
            tier,
            parent_id: request.parent_id,
            owner: owner.clone(),
            assignee: request.assignee,
            created_at: now.clone(),
            created_by,
            updated_at: now,
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: request.description,
            notes: Vec::new(),
            design: request.design,
            refs: references.clone(),
            links: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            model: normalize_model(request.model)?,
            size: request.size,
            task_type: request.task_type,
            task_type_fields: request.task_type_fields,
            is_ready_to_work: false,
            changespec_name: request.changespec_name,
            changespec_bug_id: request.changespec_bug_id,
            external_ref: request.external_ref,
            creation_reason: creation_reason.clone(),
            dependencies: Vec::new(),
        };
        issue.validate()?;
        let mut candidate_issues = store.issues.clone();
        candidate_issues.push(issue.clone());
        validate_unique_external_refs(&candidate_issues)?;
        store.issues.push(issue.clone());
        let mut event_issue = issue.clone();
        event_issue.dependencies.clear();
        event_issue.refs.clear();
        event_issue.links.clear();
        store.append_issue_event(
            &issue.id,
            BeadEventOperationWire::IssueCreated,
            BeadEventPayloadWire::IssueCreated { issue: event_issue },
            &issue.updated_at,
            &issue.created_by,
        )?;
        for reference in &references {
            store.append_issue_event(
                &issue.id,
                BeadEventOperationWire::ReferenceAdded,
                BeadEventPayloadWire::ReferenceAdded {
                    reference: reference.clone(),
                },
                &issue.updated_at,
                &issue.created_by,
            )?;
        }
        let issue = if initial_note.trim().is_empty() {
            issue
        } else {
            let index = store.issue_index(&issue.id)?;
            append_note_to_store(
                &mut store,
                index,
                &initial_note,
                &issue.created_by,
                &issue.created_at,
            )?
        };
        store.save()?;

        let mut result = outcome("create", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        result.references = references;
        result.next_counter = Some(store.config.next_counter);
        Ok(result)
    })
}

fn default_create_tier(
    request: &BeadCreateRequestWire,
) -> Option<BeadTierWire> {
    match request.issue_type {
        IssueTypeWire::Plan => {
            Some(request.tier.clone().unwrap_or(BeadTierWire::Epic))
        }
        IssueTypeWire::Phase => request.tier.clone(),
        IssueTypeWire::Task => request.tier.clone(),
    }
}

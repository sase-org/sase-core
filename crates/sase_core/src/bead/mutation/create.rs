use super::mutation_wire::BeadCreateRequestWire;
use super::mutation_wire::BeadMutationOutcomeWire;
use super::runner::run_mutation;
use super::runner::MutationStep;
use super::store::normalize_model;
use super::store::normalize_references;
use super::store::now_utc;
use super::store::outcome;
use super::store::to_base36;
use super::view::MutationView;
use crate::bead::config::default_config;
use crate::bead::config::save_config;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::wire::normalize_creation_reason;
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
    run_mutation(beads_dir, "create", |view| {
        run_create(view, &request, creation_reason.clone())
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

/// The single create algorithm, over the view on both backings.
///
/// Reads resolve through the view (raw caller IDs, exactly as the locked
/// replay load did), allocates through the view's overlay-aware counters,
/// stages the row so later view reads see it, mints every event with
/// `stage_event`, and finishes with one `commit`. A decline (`Ok(None)`
/// from staging or the commit) retries the same closure on the replay
/// backing; errors preserve the replay oracle's kinds and messages.
fn run_create(
    view: &mut MutationView,
    request: &BeadCreateRequestWire,
    creation_reason: String,
) -> Result<MutationStep<BeadMutationOutcomeWire>, BeadError> {
    let mut request = request.clone();
    if let Some(parent_id) = request.parent_id.as_deref() {
        let resolved = view.resolve(parent_id)?;
        // Full IDs pass resolution through untouched, so verify the
        // parent exists in this same locked view: a missing parent must
        // fail exactly as the old caller-side pre-read did.
        view.get(&resolved)?;
        request.parent_id = Some(resolved);
    }
    let tier = default_create_tier(&request);
    let references = normalize_references(&request.refs)?;
    let now = request.now.clone().unwrap_or_else(now_utc);
    let (owner, issue_prefix) = {
        let config = view.config()?;
        (config.owner.clone(), config.issue_prefix.clone())
    };
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
                .and_then(|parent_id| view.get(parent_id).ok())
                .map(|parent| parent.created_by.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| owner.clone());

    let issue_id = match request.parent_id.as_deref() {
        Some(parent_id) => view.next_child_id(parent_id)?,
        None => {
            let counter = {
                let config = view.config()?;
                view.next_top_level_counter(
                    &config.issue_prefix,
                    config.next_counter,
                )?
            };
            view.config_mut()?.next_counter = counter + 1;
            format!("{}-{}", issue_prefix, to_base36(counter))
        }
    };

    let initial_note = request.notes.clone();
    let mut issue = IssueWire {
        id: issue_id.clone(),
        title: request.title.clone(),
        status: StatusWire::Open,
        issue_type: request.issue_type.clone(),
        tier,
        parent_id: request.parent_id.clone(),
        owner: owner.clone(),
        assignee: request.assignee.clone(),
        created_at: now.clone(),
        created_by: created_by.clone(),
        updated_at: now.clone(),
        closed_at: None,
        close_reason: None,
        resolution: None,
        close_history: Vec::new(),
        description: request.description.clone(),
        notes: Vec::new(),
        design: request.design.clone(),
        refs: references.clone(),
        links: Vec::new(),
        plus_one_evidence: Vec::new(),
        snooze: None,
        model: normalize_model(request.model.clone())?,
        size: request.size.clone(),
        task_type: request.task_type.clone(),
        task_type_fields: request.task_type_fields.clone(),
        is_ready_to_work: false,
        changespec_name: request.changespec_name.clone(),
        changespec_bug_id: request.changespec_bug_id.clone(),
        external_ref: request.external_ref.clone(),
        creation_reason: creation_reason.clone(),
        dependencies: Vec::new(),
    };
    issue.validate()?;
    let external_ref = issue.external_ref.trim().to_string();
    if !external_ref.is_empty() {
        if let Some(owner_row) = view.external_ref_owner(&external_ref)? {
            return Err(BeadError::conflict(format!(
                "external_ref {external_ref} already belongs to {}; cannot also assign it to {}",
                owner_row.id, issue.id
            )));
        }
    }

    // Stage the pre-note row first so stream routing and later view reads
    // see it.
    view.stage_issue(issue.clone());
    let mut event_issue = issue.clone();
    event_issue.dependencies.clear();
    event_issue.refs.clear();
    event_issue.links.clear();
    let Some(_) = view.stage_event(
        &issue.id,
        BeadEventOperationWire::IssueCreated,
        BeadEventPayloadWire::IssueCreated { issue: event_issue },
        &issue.updated_at,
        &issue.created_by,
    )?
    else {
        return Ok(MutationStep::NeedsReplay);
    };
    for reference in &references {
        let Some(_) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::ReferenceAdded,
            BeadEventPayloadWire::ReferenceAdded {
                reference: reference.clone(),
            },
            &issue.updated_at,
            &issue.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
    }
    if !initial_note.trim().is_empty() {
        let entry = initial_note.clone();
        let Some(event_id) = view.stage_event(
            &issue.id,
            BeadEventOperationWire::NoteAppended,
            BeadEventPayloadWire::NoteAppended {
                entry: entry.clone(),
                attachments: Vec::new(),
            },
            &issue.created_at,
            &issue.created_by,
        )?
        else {
            return Ok(MutationStep::NeedsReplay);
        };
        if let Some(note) = crate::bead::wire::BeadNoteWire::from_event(
            &event_id,
            &issue.created_at,
            &issue.created_by,
            &entry,
            Vec::new(),
        ) {
            issue.notes.push(note);
        }
        issue.validate()?;
    }

    view.stage_issue(issue.clone());
    let Some(mut rows) = view.commit(&[issue.id.clone()])? else {
        return Ok(MutationStep::NeedsReplay);
    };
    let issue = rows.pop().expect("one created row");

    let mut result = outcome("create", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    result.references = references;
    result.next_counter = Some(view.config()?.next_counter);
    Ok(MutationStep::Done(result))
}

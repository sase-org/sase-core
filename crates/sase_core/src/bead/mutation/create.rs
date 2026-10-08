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
use crate::bead::config::load_config;
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
        if let Some(view) = super::view::MutationView::load_cached(beads_dir)? {
            if let Some(outcome) = try_cached_create(
                beads_dir,
                view,
                &request,
                creation_reason.clone(),
            )? {
                return Ok(outcome);
            }
        }
        let mut store = MutableStore::load(beads_dir)?;
        // Resolve inside the locked load: the single store read is the
        // authority for existence and ambiguity, so callers pass raw IDs.
        // This is also the existence check for the parent: a missing
        // parent fails here exactly as the old caller-side pre-resolve did.
        let mut request = request;
        if let Some(parent_id) = request.parent_id.as_deref() {
            let resolved = store.resolve_issue_id(parent_id)?;
            // Full IDs pass resolution through untouched, so verify the
            // parent exists in this same locked load: a missing parent
            // must fail exactly as the old caller-side pre-read did.
            store.issue_index(&resolved)?;
            request.parent_id = Some(resolved);
        }
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
                &[],
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

/// Indexed create: affected rows plus one physical stream only.
///
/// Returns `Ok(None)` when the replay path must run instead (uncached
/// store already excluded by admission; a missing parent stream file
/// falls back so replay owns the corruption error). Errors preserve the
/// replay path's kinds and messages.
fn try_cached_create(
    beads_dir: &Path,
    view: super::view::MutationView<'_>,
    request: &BeadCreateRequestWire,
    creation_reason: String,
) -> Result<Option<BeadMutationOutcomeWire>, BeadError> {
    use super::shared::commit_staged_write;
    use super::shared::load_mutation_stream;
    use super::shared::mint_stream_event;
    use super::shared::new_mutation_stream;

    let mut request = request.clone();
    if let Some(parent_id) = request.parent_id.as_deref() {
        let resolved = view.resolve(parent_id)?;
        view.get(&resolved)?;
        request.parent_id = Some(resolved);
    }
    let tier = default_create_tier(&request);
    let references = normalize_references(&request.refs)?;
    let now = request.now.clone().unwrap_or_else(now_utc);
    let fallback = default_config("beads", "");
    let mut config = load_config(beads_dir, fallback)?;
    let owner = config.owner.clone();
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

    let mut view = view;
    let issue_id = match request.parent_id.as_deref() {
        Some(parent_id) => view.next_child_id(parent_id)?,
        None => {
            let counter = view.next_top_level_counter(
                &config.issue_prefix,
                config.next_counter,
            )?;
            config.next_counter = counter + 1;
            format!("{}-{}", config.issue_prefix, to_base36(counter))
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

    // Physical stream routing preserves the writer rule: a plan owns its
    // stream, and a non-plan follows its parent's stream when the parent
    // exists. Stage the pre-note row first so later view reads see it.
    view.stage_issue(issue.clone());
    let stream_id = if issue.issue_type == IssueTypeWire::Plan {
        issue.id.clone()
    } else if let Some(parent_id) = issue.parent_id.as_deref() {
        if view.get(parent_id).is_ok() {
            parent_id.to_string()
        } else {
            issue.id.clone()
        }
    } else {
        issue.id.clone()
    };
    // Load the single affected stream: a brand-new stream costs no
    // stream I/O, while a missing existing stream falls back so replay
    // owns the corruption error.
    let mut stream = match load_mutation_stream(beads_dir, &stream_id)? {
        Some(stream) => stream,
        None => {
            if stream_id == issue.id {
                new_mutation_stream(&stream_id)
            } else {
                return Ok(None);
            }
        }
    };
    let base_len = stream.events.len();

    let mut event_issue = issue.clone();
    event_issue.dependencies.clear();
    event_issue.refs.clear();
    event_issue.links.clear();
    mint_stream_event(
        &mut stream,
        BeadEventOperationWire::IssueCreated,
        BeadEventPayloadWire::IssueCreated { issue: event_issue },
        &issue.updated_at,
        &issue.created_by,
        &issue.id,
    )?;
    for reference in &references {
        mint_stream_event(
            &mut stream,
            BeadEventOperationWire::ReferenceAdded,
            BeadEventPayloadWire::ReferenceAdded {
                reference: reference.clone(),
            },
            &issue.updated_at,
            &issue.created_by,
            &issue.id,
        )?;
    }
    if !initial_note.trim().is_empty() {
        let entry = initial_note.clone();
        let event_id = mint_stream_event(
            &mut stream,
            BeadEventOperationWire::NoteAppended,
            BeadEventPayloadWire::NoteAppended {
                entry: entry.clone(),
                attachments: Vec::new(),
            },
            &issue.created_at,
            &issue.created_by,
            &issue.id,
        )?;
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
    let expected = vec![(issue.id.clone(), issue.clone())];
    let cache_path_buf =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    let cache_path = cache_path_buf.as_deref();
    let witness = view.witness();
    let committed = commit_staged_write(
        beads_dir,
        cache_path,
        witness,
        &config,
        std::slice::from_ref(&stream),
        std::slice::from_ref(&base_len),
        &expected,
    )?;
    let Some(mut rows) = committed else {
        return Ok(None);
    };
    issue = rows.pop().expect("one created row");

    let mut result = outcome("create", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    result.references = references;
    result.next_counter = Some(config.next_counter);
    Ok(Some(result))
}

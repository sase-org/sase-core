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
use std::collections::BTreeSet;
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
    use crate::bead::events::mint_bead_event_id;
    use crate::bead::events::BeadEventRecordWire;
    use crate::bead::events::BeadEventStreamWire;
    use crate::bead::events::BEAD_EVENT_SCHEMA_VERSION;
    use crate::bead::jsonl::event_streams_dir;
    use crate::bead::jsonl::read_event_stream_file;
    use crate::bead::jsonl::write_event_store_changed_with_total_and_signatures;
    use crate::bead::read_model::AppendedStream;

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
    let stream_path =
        event_streams_dir(beads_dir).join(format!("{stream_id}.jsonl"));
    let is_new_stream = !stream_path.is_file();
    let mut stream = if is_new_stream {
        BeadEventStreamWire {
            stream_id: stream_id.clone(),
            root_issue_id: stream_id.clone(),
            events: Vec::new(),
        }
    } else {
        match read_event_stream_file(&stream_path) {
            Ok((stream, _)) => stream,
            Err(_) => return Ok(None),
        }
    };
    #[cfg(test)]
    super::store::store_io_stats::record_stream_reads(1);
    let base_len = stream.events.len();

    fn push_event(
        stream: &mut BeadEventStreamWire,
        operation: BeadEventOperationWire,
        payload: BeadEventPayloadWire,
        timestamp: &str,
        actor: &str,
        issue_id: &str,
    ) -> Result<(), BeadError> {
        let ordinal = stream.events.len() + 1;
        let event_id = mint_bead_event_id(
            &stream.stream_id,
            ordinal,
            timestamp,
            actor,
            operation,
            issue_id,
            &payload,
        )?;
        let event = BeadEventRecordWire {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            event_id,
            timestamp: timestamp.to_string(),
            actor: actor.to_string(),
            operation,
            issue_id: issue_id.to_string(),
            payload,
        };
        event.validate()?;
        stream.events.push(event);
        Ok(())
    }

    let mut event_issue = issue.clone();
    event_issue.dependencies.clear();
    event_issue.refs.clear();
    event_issue.links.clear();
    push_event(
        &mut stream,
        BeadEventOperationWire::IssueCreated,
        BeadEventPayloadWire::IssueCreated { issue: event_issue },
        &issue.updated_at,
        &issue.created_by,
        &issue.id,
    )?;
    for reference in &references {
        push_event(
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
        push_event(
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
            stream
                .events
                .last()
                .map(|event| event.event_id.as_str())
                .unwrap_or(""),
            &issue.created_at,
            &issue.created_by,
            &entry,
            Vec::new(),
        ) {
            issue.notes.push(note);
        }
        issue.validate()?;
    }

    let total =
        match std::fs::read_to_string(beads_dir.join("events/manifest.json"))
            .ok()
            .and_then(|text| {
                serde_json::from_str::<serde_json::Value>(&text).ok()
            })
            .and_then(|value| value.get("stream_count")?.as_u64())
            .map(|count| count as usize)
        {
            Some(total) => total + usize::from(is_new_stream),
            None => return Ok(None),
        };
    let mut changed = BTreeSet::new();
    changed.insert(stream_id.clone());
    #[cfg(test)]
    super::store::store_io_stats::record_save();
    #[cfg(test)]
    super::store::store_io_stats::record_validation_runs(1);
    let mut signatures = write_event_store_changed_with_total_and_signatures(
        beads_dir,
        std::slice::from_ref(&stream),
        &changed,
        total,
    )?;
    save_config(beads_dir, &config)?;
    view.stage_issue(issue.clone());

    let cache_path_opt =
        crate::bead::read_model::read_model_cache_path_for_store(beads_dir);
    if let Some(cache_path) = cache_path_opt {
        // A stream the writer skipped has no signature to publish
        // against; the durable events stand and the next read tails
        // them in.
        let appended = signatures
            .remove(&stream_id)
            .map(|signature| {
                vec![AppendedStream {
                    stream_id: stream_id.clone(),
                    events: stream.events[base_len..].to_vec(),
                    signature,
                }]
            })
            .unwrap_or_default();
        let expected = vec![(issue.id.clone(), issue.clone())];
        let witness = (!appended.is_empty()).then(|| view.witness()).flatten();
        let corrected = super::publish::publish_cached_write(
            beads_dir,
            &cache_path,
            witness,
            &appended,
            &expected,
        )
        .corrections();
        let mut returned = vec![issue.clone()];
        super::publish::apply_corrections(&mut returned, &corrected);
        issue = returned.pop().expect("one created row");
    }

    let mut result = outcome("create", true, vec![issue.id.clone()]);
    result.issue = Some(issue);
    result.references = references;
    result.next_counter = Some(config.next_counter);
    Ok(Some(result))
}

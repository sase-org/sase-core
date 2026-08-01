//! Bead store mutations backed by JSONL persistence.

use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::artifact_ref::normalize_artifact_ref_list;
use crate::store_lock::{
    acquire_store_lock, timeout_from_env, LockMode, StoreLockError,
};

use super::config::{default_config, load_config, save_config, BeadConfigWire};
use super::events::{
    appended_note_text, import_issues_to_event_streams, mint_bead_event_id,
    reduce_event_streams, BeadEventOperationWire, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStreamWire, BeadIssueUpdateEventFieldsWire,
    BEAD_EVENT_SCHEMA_VERSION,
};
use super::jsonl::{
    event_store_present, import_issues_from_jsonl, read_event_store,
    write_event_store, write_issues_jsonl,
};
use super::read::resolve_issue_id_in_issues;
use super::wire::{
    deserialize_option_phase_size, validate_model_value, BeadError,
    BeadResolutionWire, BeadTierWire, DependencyWire, IssueTypeWire, IssueWire,
    PhaseSizeWire, StatusWire, TaskPlusOneEvidenceWire,
};

// Reuse the ignored compatibility database as the advisory lock file so a
// successful claim cannot introduce durable bead-store content of its own.
// Deleting and recreating beads.db while this lock is held would split
// contenders across different inodes, so store maintenance must preserve it.
const BEAD_MUTATION_LOCK_FILENAME: &str = "beads.db";
const BEAD_MUTATION_HOLDER_FILENAME: &str = ".bead-mutation-lock.holder";
const BEAD_MUTATION_LOCK_TIMEOUT_ENV: &str = "SASE_BEAD_MUTATION_LOCK_TIMEOUT";
const BEAD_MUTATION_LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadCreateRequestWire {
    pub title: String,
    pub issue_type: IssueTypeWire,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub parent_id: Option<String>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub notes: String,
    #[serde(default)]
    pub design: String,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default)]
    pub model: String,
    #[serde(default, deserialize_with = "deserialize_option_phase_size")]
    pub size: Option<PhaseSizeWire>,
    #[serde(default)]
    pub assignee: String,
    #[serde(default)]
    pub created_by: Option<String>,
    #[serde(default)]
    pub changespec_name: String,
    #[serde(default)]
    pub changespec_bug_id: String,
    #[serde(default)]
    pub now: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadUpdateFieldsWire {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_phase_size")]
    pub size: Option<PhaseSizeWire>,
    #[serde(default)]
    pub closed_at: Option<Option<String>>,
    #[serde(default)]
    pub close_reason: Option<Option<String>>,
    #[serde(default)]
    pub resolution: Option<Option<BeadResolutionWire>>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub changespec_bug_id: Option<String>,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub is_ready_to_work: Option<bool>,
    #[serde(default)]
    pub now: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadPreclaimAssignmentWire {
    pub bead_id: String,
    pub agent_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadPreclaimRollbackWire {
    pub bead_id: String,
    pub status: StatusWire,
    pub assignee: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadMutationOutcomeWire {
    pub operation: String,
    pub changed: bool,
    #[serde(default)]
    pub lock_wait_ms: u64,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub closed_ids: Vec<String>,
    #[serde(default)]
    pub already_closed_ids: Vec<String>,
    #[serde(default)]
    pub noted_ids: Vec<String>,
    #[serde(default)]
    pub cascade_closed_ids: Vec<String>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub issue: Option<IssueWire>,
    #[serde(default)]
    pub issues: Vec<IssueWire>,
    #[serde(default)]
    pub dependency: Option<DependencyWire>,
    #[serde(default)]
    pub dependencies: Vec<DependencyWire>,
    #[serde(default)]
    pub references: Vec<String>,
    #[serde(default)]
    pub next_counter: Option<u64>,
    #[serde(default)]
    pub rollback_preclaims: Vec<BeadPreclaimRollbackWire>,
    #[serde(default)]
    pub reopened_ancestor_ids: Vec<String>,
    #[serde(default)]
    pub unchanged_ids: Vec<String>,
}

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
            description: request.description,
            notes: request.notes,
            design: request.design,
            refs: references.clone(),
            plus_one_evidence: Vec::new(),
            model: normalize_model(request.model)?,
            size: request.size,
            is_ready_to_work: false,
            changespec_name: request.changespec_name,
            changespec_bug_id: request.changespec_bug_id,
            dependencies: Vec::new(),
        };
        issue.validate()?;
        store.issues.push(issue.clone());
        let mut event_issue = issue.clone();
        event_issue.dependencies.clear();
        event_issue.refs.clear();
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
        store.save()?;

        let mut result = outcome("create", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        result.references = references;
        result.next_counter = Some(store.config.next_counter);
        Ok(result)
    })
}

/// Append one independently attributed report to an existing task bead.
///
/// The evidence, referenced artifacts, and any draft/closed-to-ready status
/// promotion are persisted together under the bead mutation lock. Repeating
/// the creator or an existing reporter is an exact no-op.
pub fn add_task_plus_one(
    beads_dir: &Path,
    issue_id: &str,
    reporter: &str,
    note: &str,
    references: &[String],
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let reporter = reporter.trim().to_string();
    if reporter.is_empty() {
        return Err(BeadError::validation(
            "task +1 reporter cannot be empty or blank",
        ));
    }
    let note = note.trim().to_string();
    if note.is_empty() {
        return Err(BeadError::validation(
            "task +1 note cannot be empty or blank",
        ));
    }
    let references = normalize_references(references)?;

    with_bead_mutation_lock(beads_dir, "plus_one", || {
        let mut store = MutableStore::load(beads_dir)?;
        let resolved_id = resolve_issue_id_in_issues(&store.issues, issue_id)?;
        let index = store.issue_index(&resolved_id)?;
        let current = store.issues[index].clone();
        if current.issue_type != IssueTypeWire::Task {
            return Err(BeadError::validation(format!(
                "task +1 only applies to task beads: {resolved_id}"
            )));
        }
        if reporter == current.created_by
            || current
                .plus_one_evidence
                .iter()
                .any(|evidence| evidence.reporter == reporter)
        {
            let mut result =
                outcome("plus_one", false, vec![resolved_id.clone()]);
            result.issue = Some(current);
            result.message =
                "reporter already represented; use sase bead note for supplementary evidence"
                    .to_string();
            return Ok(result);
        }

        let timestamp = now.unwrap_or_else(now_utc);
        let evidence = TaskPlusOneEvidenceWire {
            timestamp: timestamp.clone(),
            reporter: reporter.clone(),
            note,
            refs: references.clone(),
        };
        evidence.validate()?;

        let issue = &mut store.issues[index];
        issue.plus_one_evidence.push(evidence.clone());
        for reference in &references {
            if !issue.refs.contains(reference) {
                issue.refs.push(reference.clone());
            }
        }
        if matches!(issue.status, StatusWire::Open | StatusWire::Closed) {
            issue.status = StatusWire::Ready;
            issue.closed_at = None;
            issue.close_reason = None;
            issue.resolution = None;
        }
        issue.updated_at = timestamp.clone();
        issue.validate()?;
        let issue = issue.clone();

        store.append_issue_event(
            &resolved_id,
            BeadEventOperationWire::TaskPlusOneRecorded,
            BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
            &timestamp,
            &reporter,
        )?;
        store.save()?;

        let mut result = outcome("plus_one", true, vec![resolved_id]);
        result.issue = Some(issue);
        result.references = references;
        Ok(result)
    })
}

pub fn update_issue(
    beads_dir: &Path,
    issue_id: &str,
    fields: BeadUpdateFieldsWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let requested = vec![issue_id.to_string()];
    let mut result = update_issues(beads_dir, &requested, fields)?;
    let issue = result.issues.pop().expect(
        "update_issues returns exactly one issue for a single-ID request",
    );
    result.issue = Some(issue);
    result.issue_ids = vec![issue_id.to_string()];
    result.issues = Vec::new();
    Ok(result)
}

/// Apply the same field changes to every named issue as one atomic mutation.
///
/// Every ID is resolved and every resulting issue is validated before
/// anything is written, so an unknown ID or an invalid field value leaves the
/// store byte-identical. Duplicate IDs (including a shorthand alongside its
/// resolved full form) collapse to a single update.
pub fn update_issues(
    beads_dir: &Path,
    issue_ids: &[String],
    fields: BeadUpdateFieldsWire,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if fields.is_ready_to_work.is_some() {
        return Err(BeadError::validation(
            "is_ready_to_work cannot be set via update(); use mark_ready_to_work() instead.",
        ));
    }
    with_bead_mutation_lock(beads_dir, "update", || {
        let mut store = MutableStore::load(beads_dir)?;

        let mut seen = HashSet::new();
        let targets: Vec<String> = issue_ids
            .iter()
            .filter(|issue_id| seen.insert((*issue_id).clone()))
            .cloned()
            .collect();

        let indexes = targets
            .iter()
            .map(|issue_id| store.issue_index(issue_id))
            .collect::<Result<Vec<_>, _>>()?;

        if fields.status.as_deref() == Some("closed") {
            reject_unclosed_descendants_in_batch(&store.issues, &targets)?;
        }

        let event_fields = event_fields_from_update_fields(&fields)?;
        let now = fields.now.clone().unwrap_or_else(now_utc);

        let mut planned: Vec<(usize, IssueWire, bool)> = Vec::new();
        let mut unchanged_ids = Vec::new();
        let mut resulting_issues = Vec::with_capacity(targets.len());
        for (target_id, index) in targets.iter().zip(indexes.iter().copied()) {
            let current = store.issues[index].clone();
            let was_closed = current.status == StatusWire::Closed;
            let mut issue = current.clone();
            apply_update_fields(&mut issue, fields.clone())?;
            if issue == current {
                unchanged_ids.push(target_id.clone());
                resulting_issues.push(current);
                continue;
            }
            issue.updated_at = now.clone();
            issue.validate()?;
            resulting_issues.push(issue.clone());
            planned.push((index, issue, was_closed));
        }

        if planned.is_empty() {
            let mut result = outcome("update", false, Vec::new());
            result.unchanged_ids = unchanged_ids;
            result.issues = resulting_issues;
            return Ok(result);
        }

        let mut changed_ids = Vec::with_capacity(planned.len());
        let mut reopened_ancestors: Vec<IssueWire> = Vec::new();
        for (index, issue, was_closed) in planned {
            store.issues[index] = issue.clone();
            changed_ids.push(issue.id.clone());
            store.append_issue_event(
                &issue.id,
                BeadEventOperationWire::IssueUpdated,
                BeadEventPayloadWire::IssueUpdated {
                    fields: event_fields.clone(),
                },
                &issue.updated_at,
                &issue.created_by,
            )?;
            if was_closed && issue.status != StatusWire::Closed {
                let newly_reopened = reopen_closed_ancestors(
                    &mut store,
                    &issue.id,
                    &issue.updated_at,
                )?;
                reopened_ancestors.extend(newly_reopened);
            }
        }
        store.save()?;

        let mut result = outcome("update", true, changed_ids);
        result.unchanged_ids = unchanged_ids;
        result.issues = resulting_issues;
        result.reopened_ancestor_ids = reopened_ancestors
            .iter()
            .map(|ancestor| ancestor.id.clone())
            .collect();
        Ok(result)
    })
}

pub fn append_issue_note(
    beads_dir: &Path,
    issue_id: &str,
    entry: &str,
    author: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let entry = entry.trim();
    if entry.is_empty() {
        return Err(BeadError::validation(
            "note entry cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "note", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let now = now.unwrap_or_else(now_utc);
        let author = author
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| store.config.owner.clone());
        let issue =
            append_note_to_store(&mut store, index, entry, &author, &now)?;
        store.save()?;

        let mut result = outcome("note", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

fn append_note_to_store(
    store: &mut MutableStore,
    issue_index: usize,
    entry: &str,
    author: &str,
    now: &str,
) -> Result<IssueWire, BeadError> {
    let issue_id = store.issues[issue_index].id.clone();
    let new_notes = appended_note_text(
        &store.issues[issue_index].notes,
        now,
        author,
        entry,
    );
    store.issues[issue_index].notes = new_notes.clone();
    store.issues[issue_index].updated_at = now.to_string();
    let issue = store.issues[issue_index].clone();
    issue.validate()?;
    store.append_issue_event(
        &issue_id,
        BeadEventOperationWire::NoteAppended,
        BeadEventPayloadWire::NoteAppended {
            entry: entry.to_string(),
        },
        now,
        author,
    )?;
    Ok(issue)
}

pub fn claim_for_agent_launch(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead launch claim cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "claim_for_launch", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        if store.issues[index].status == StatusWire::Closed {
            return Err(BeadError {
                kind: "closed".to_string(),
                message: format!(
                    "cannot claim closed bead for agent launch: {issue_id}"
                ),
            });
        }

        let current = store.issues[index].clone();
        if current.status == StatusWire::InProgress
            && current.assignee == agent_name
        {
            let mut result = outcome(
                "claim_for_agent_launch",
                false,
                vec![current.id.clone()],
            );
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::InProgress;
        store.issues[index].assignee = agent_name.to_string();
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::InProgress),
                    assignee: Some(agent_name.to_string()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("claim_for_agent_launch", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn claim_for_agent_wait(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead wait claim cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "claim_for_wait", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        let current = store.issues[index].clone();

        if matches!(
            current.status,
            StatusWire::Claimed | StatusWire::InProgress
        ) && current.assignee == agent_name
        {
            let mut result = outcome(
                "claim_for_agent_wait",
                false,
                vec![current.id.clone()],
            );
            result.issue = Some(current);
            return Ok(result);
        }

        if current.status != StatusWire::Open {
            let holder = if current.assignee.is_empty() {
                "<unassigned>"
            } else {
                current.assignee.as_str()
            };
            let mut result = outcome(
                "claim_for_agent_wait",
                false,
                vec![current.id.clone()],
            );
            result.message = format!(
                "cannot claim bead {issue_id} for agent wait: current status is {} and holder is {holder}",
                mutation_status_value(&current.status)
            );
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Claimed;
        store.issues[index].assignee = agent_name.to_string();
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::Claimed),
                    assignee: Some(agent_name.to_string()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("claim_for_agent_wait", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn release_agent_claim(
    beads_dir: &Path,
    issue_id: &str,
    agent_name: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if agent_name.trim().is_empty() {
        return Err(BeadError::validation(
            "agent name for bead claim release cannot be empty or blank",
        ));
    }

    with_bead_mutation_lock(beads_dir, "release_wait_claim", || {
        let mut store = MutableStore::load(beads_dir)
            .map_err(|error| durable_store_error("read", beads_dir, error))?;
        let index = store.issue_index(issue_id)?;
        let current = store.issues[index].clone();

        if current.status != StatusWire::Claimed
            || current.assignee != agent_name
        {
            let mut result =
                outcome("release_agent_claim", false, vec![current.id.clone()]);
            result.issue = Some(current);
            return Ok(result);
        }

        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Open;
        store.issues[index].assignee.clear();
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueUpdated,
            BeadEventPayloadWire::IssueUpdated {
                fields: BeadIssueUpdateEventFieldsWire {
                    status: Some(StatusWire::Open),
                    assignee: Some(String::new()),
                    ..Default::default()
                },
            },
            &now,
            &issue.created_by,
        )?;
        store
            .save()
            .map_err(|error| durable_store_error("write", beads_dir, error))?;

        let mut result =
            outcome("release_agent_claim", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

pub fn preclaim_epic_work_plan(
    beads_dir: &Path,
    epic_id: &str,
    assignments: &[BeadPreclaimAssignmentWire],
    epic_agent_name: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, "preclaim_epic_work", || {
        let mut store = MutableStore::load(beads_dir)?;
        let epic_index = store.issue_index(epic_id)?;
        let epic = store.issues[epic_index].clone();
        if epic.issue_type != IssueTypeWire::Plan {
            return Err(BeadError {
                kind: "not_a_plan".to_string(),
                message: format!(
                    "sase bead work preclaim only applies to epic plan beads (got phase for {epic_id})"
                ),
            });
        }
        if !matches!(epic.tier.as_ref(), Some(BeadTierWire::Epic)) {
            return Err(BeadError {
                kind: "not_workable_plan".to_string(),
                message: format!(
                    "sase bead work preclaim only applies to epic plan beads (got {} for {epic_id})",
                    tier_label(epic.tier.as_ref())
                ),
            });
        }
        if let Some(agent_name) = epic_agent_name.as_deref() {
            if agent_name.trim().is_empty() {
                return Err(BeadError::validation(
                    "epic agent name for work preclaim cannot be empty or blank",
                ));
            }
            if epic.status == StatusWire::Closed {
                return Err(BeadError::validation(format!(
                    "preclaim target is closed: {epic_id}"
                )));
            }
        }

        let mut seen = HashSet::new();
        let target_count =
            assignments.len() + usize::from(epic_agent_name.is_some());
        let mut indexes = Vec::with_capacity(assignments.len());
        let mut rollback = Vec::with_capacity(target_count);
        for assignment in assignments {
            if !seen.insert(assignment.bead_id.as_str()) {
                return Err(BeadError::validation(format!(
                    "duplicate preclaim target: {}",
                    assignment.bead_id
                )));
            }
            let index = store.issue_index(&assignment.bead_id)?;
            let issue = &store.issues[index];
            if issue.issue_type != IssueTypeWire::Phase {
                return Err(BeadError::validation(format!(
                    "preclaim target is not a phase bead: {}",
                    assignment.bead_id
                )));
            }
            if issue.parent_id.as_deref() != Some(epic_id) {
                return Err(BeadError::validation(format!(
                    "preclaim target {} is not a child of epic {}",
                    assignment.bead_id, epic_id
                )));
            }
            if issue.status == StatusWire::Closed {
                return Err(BeadError::validation(format!(
                    "preclaim target is closed: {}",
                    assignment.bead_id
                )));
            }
            indexes.push(index);
            rollback.push(BeadPreclaimRollbackWire {
                bead_id: issue.id.clone(),
                status: issue.status.clone(),
                assignee: issue.assignee.clone(),
            });
        }
        if epic_agent_name.is_some() {
            rollback.push(BeadPreclaimRollbackWire {
                bead_id: epic.id.clone(),
                status: epic.status.clone(),
                assignee: epic.assignee.clone(),
            });
        }

        let now = now.unwrap_or_else(now_utc);
        let mut updated = Vec::with_capacity(target_count);
        for (assignment, index) in assignments.iter().zip(indexes) {
            let issue = &mut store.issues[index];
            issue.status = StatusWire::InProgress;
            issue.assignee = assignment.agent_name.clone();
            issue.updated_at = now.clone();
            issue.validate()?;
            updated.push(issue.clone());
        }
        for (assignment, issue) in assignments.iter().zip(&updated) {
            store.append_issue_event(
                &assignment.bead_id,
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed {
                    agent_name: assignment.agent_name.clone(),
                },
                &now,
                &issue.created_by,
            )?;
        }
        if let Some(agent_name) = epic_agent_name {
            let issue = &mut store.issues[epic_index];
            issue.status = StatusWire::InProgress;
            issue.assignee = agent_name.clone();
            issue.updated_at = now.clone();
            issue.validate()?;
            let updated_epic = issue.clone();
            store.append_issue_event(
                epic_id,
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed { agent_name },
                &now,
                &updated_epic.created_by,
            )?;
            updated.push(updated_epic);
        }

        if !updated.is_empty() {
            store.save()?;
        }
        let mut result = outcome(
            "preclaim_epic_work",
            !updated.is_empty(),
            updated.iter().map(|issue| issue.id.clone()).collect(),
        );
        result.issues = updated;
        result.rollback_preclaims = rollback;
        Ok(result)
    })
}

pub fn open_issue(
    beads_dir: &Path,
    issue_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, "open", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(issue_id)?;
        let was_closed = store.issues[index].status == StatusWire::Closed;
        let now = now.unwrap_or_else(now_utc);
        store.issues[index].status = StatusWire::Open;
        store.issues[index].resolution = None;
        store.issues[index].updated_at = now.clone();
        let issue = store.issues[index].clone();
        issue.validate()?;
        store.append_issue_event(
            issue_id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            &now,
            &issue.created_by,
        )?;
        let reopened_ancestors = if was_closed {
            reopen_closed_ancestors(&mut store, issue_id, &now)?
        } else {
            Vec::new()
        };
        store.save()?;

        let mut result = outcome("open", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        result.reopened_ancestor_ids = reopened_ancestors
            .iter()
            .map(|ancestor| ancestor.id.clone())
            .collect();
        result.issues = reopened_ancestors;
        Ok(result)
    })
}

pub fn close_issues(
    beads_dir: &Path,
    issue_ids: &[String],
    reason: Option<String>,
    resolution: Option<BeadResolutionWire>,
    force: bool,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    close_issues_with_note(
        beads_dir, issue_ids, reason, resolution, force, None, None, now,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn close_issues_with_note(
    beads_dir: &Path,
    issue_ids: &[String],
    reason: Option<String>,
    resolution: Option<BeadResolutionWire>,
    force: bool,
    note: Option<String>,
    note_author: Option<String>,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let note = match note {
        None => None,
        Some(entry) => {
            let entry = entry.trim().to_string();
            if entry.is_empty() {
                return Err(BeadError::validation(
                    "note entry cannot be empty or blank",
                ));
            }
            Some((entry, note_author))
        }
    };

    with_bead_mutation_lock(beads_dir, "close", || {
        let mut store = MutableStore::load(beads_dir)?;
        let now = now.unwrap_or_else(now_utc);
        let effective_resolution =
            resolution.clone().unwrap_or(BeadResolutionWire::Done);
        if force {
            if reason
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(BeadError::validation(
                    "forced close requires a non-empty --reason",
                ));
            }
            if effective_resolution == BeadResolutionWire::Done {
                return Err(BeadError::validation(
                    "forced close requires --resolution canceled or superseded; 'done' is not allowed",
                ));
            }
        }
        let mut standard_close_ids = BTreeSet::new();
        let mut requested_ids = BTreeSet::new();
        let mut unresolved_by_request = Vec::new();
        let mut already_closed_ids = Vec::new();

        for issue_id in issue_ids {
            let issue = store.get_issue(issue_id)?;
            if !requested_ids.insert(issue.id.clone()) {
                continue;
            }
            if issue.status == StatusWire::Closed {
                reject_conflicting_close(
                    issue,
                    resolution.as_ref(),
                    reason.as_deref(),
                )?;
                already_closed_ids.push(issue.id.clone());
            }
            standard_close_ids.insert(issue.id.clone());
            let unresolved = unresolved_descendants(&store.issues, issue_id);
            if !force && !unresolved.is_empty() {
                return Err(unclosed_descendants_error(issue_id, &unresolved));
            }
            standard_close_ids.extend(
                sorted_descendants(&store.issues, issue_id)
                    .into_iter()
                    .map(|descendant| descendant.id.clone()),
            );
            unresolved_by_request.push((
                issue.id.clone(),
                unresolved
                    .into_iter()
                    .map(|descendant| descendant.id.clone())
                    .collect::<Vec<_>>(),
            ));
        }

        let mut noted_ids = Vec::new();
        if let Some((entry, requested_author)) = note.as_ref() {
            let author = requested_author
                .as_deref()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(&store.config.owner)
                .to_string();
            for issue_id in &requested_ids {
                let index = store.issue_index(issue_id)?;
                append_note_to_store(&mut store, index, entry, &author, &now)?;
                noted_ids.push(issue_id.clone());
            }
        }
        let mut batch = CloseBatch {
            standard_close_ids,
            ..Default::default()
        };

        for (issue_id, swept_ids) in unresolved_by_request {
            let mut forced_descendant_ids = Vec::new();
            if force {
                let forced_reason = Some(format!(
                    "forced by {issue_id}: {}",
                    reason.as_deref().expect("forced reason was validated")
                ));
                for child_id in &swept_ids {
                    if close_one_and_delegated_parent(
                        &mut store,
                        child_id,
                        &now,
                        forced_reason.clone(),
                        effective_resolution.clone(),
                        Vec::new(),
                        &mut batch,
                    )? {
                        forced_descendant_ids.push(child_id.clone());
                    }
                }
            }
            if !close_one_and_delegated_parent(
                &mut store,
                &issue_id,
                &now,
                reason.clone(),
                effective_resolution.clone(),
                forced_descendant_ids,
                &mut batch,
            )? {
                batch.returned.push(store.get_issue(&issue_id)?.clone());
            }
        }
        for event in &batch.event_closed {
            store.append_issue_event(
                &event.issue.id,
                BeadEventOperationWire::IssueClosed,
                BeadEventPayloadWire::IssueClosed {
                    close_reason: event.issue.close_reason.clone(),
                    resolution: event.issue.resolution.clone(),
                    forced_descendant_ids: event.forced_descendant_ids.clone(),
                },
                &now,
                &event.issue.created_by,
            )?;
        }

        let closed_ids = batch.closed_ids;
        let cascade_closed_ids = closed_ids
            .iter()
            .filter(|issue_id| !requested_ids.contains(*issue_id))
            .cloned()
            .collect::<Vec<_>>();
        let changed = !closed_ids.is_empty() || !noted_ids.is_empty();
        if changed {
            store.save()?;
        }
        let mut affected_ids = closed_ids.clone();
        for issue_id in &noted_ids {
            if !affected_ids.contains(issue_id) {
                affected_ids.push(issue_id.clone());
            }
        }
        let mut result = outcome("close", changed, affected_ids);
        if !changed {
            result.message =
                "all requested issues were already closed".to_string();
        }
        result.issues = batch.returned;
        result.closed_ids = closed_ids;
        result.already_closed_ids = already_closed_ids;
        result.noted_ids = noted_ids;
        result.cascade_closed_ids = cascade_closed_ids;
        Ok(result)
    })
}

fn reject_conflicting_close(
    issue: &IssueWire,
    requested_resolution: Option<&BeadResolutionWire>,
    requested_reason: Option<&str>,
) -> Result<(), BeadError> {
    let resolution_conflicts = requested_resolution
        .is_some_and(|requested| issue.resolution.as_ref() != Some(requested));
    let requested_reason =
        requested_reason.filter(|value| !value.trim().is_empty());
    let reason_conflicts = requested_reason.is_some_and(|requested| {
        issue.close_reason.as_deref() != Some(requested)
    });
    if !resolution_conflicts && !reason_conflicts {
        return Ok(());
    }

    let recorded_resolution = issue
        .resolution
        .as_ref()
        .map(BeadResolutionWire::as_str)
        .unwrap_or("(unrecorded)");
    let requested_resolution = requested_resolution
        .map(BeadResolutionWire::as_str)
        .unwrap_or("(unspecified)");
    let recorded_reason = issue.close_reason.as_deref().unwrap_or("(none)");
    let requested_reason = requested_reason.unwrap_or("(unspecified)");
    let closed_at = issue.closed_at.as_deref().unwrap_or("(unknown)");
    Err(BeadError::validation(format!(
        "close request conflicts with already-closed bead {} (closed at {}, resolution {}, reason {:?}); requested resolution {}, reason {:?}. Reopen it with `sase bead open {}` before closing it again, or append evidence without re-closing it with `sase bead note {} '…'`",
        issue.id,
        closed_at,
        recorded_resolution,
        recorded_reason,
        requested_resolution,
        requested_reason,
        issue.id,
        issue.id,
    )))
}

#[derive(Default)]
struct CloseBatch {
    standard_close_ids: BTreeSet<String>,
    closed_ids: Vec<String>,
    event_closed: Vec<CloseEvent>,
    returned: Vec<IssueWire>,
}

struct CloseEvent {
    issue: IssueWire,
    forced_descendant_ids: Vec<String>,
}

fn close_one_and_delegated_parent(
    store: &mut MutableStore,
    issue_id: &str,
    closed_at: &str,
    reason: Option<String>,
    resolution: BeadResolutionWire,
    forced_descendant_ids: Vec<String>,
    batch: &mut CloseBatch,
) -> Result<bool, BeadError> {
    let Some(issue) =
        store.close_one(issue_id, closed_at, reason, resolution)?
    else {
        return Ok(false);
    };
    batch.closed_ids.push(issue.id.clone());
    batch.event_closed.push(CloseEvent {
        issue: issue.clone(),
        forced_descendant_ids,
    });
    batch.returned.push(issue.clone());

    if issue.issue_type != IssueTypeWire::Plan {
        return Ok(true);
    }
    let Some(parent_id) = issue.parent_id.as_deref() else {
        return Ok(true);
    };
    if batch.standard_close_ids.contains(parent_id) {
        return Ok(true);
    }
    let Some(parent) = store
        .issues
        .iter()
        .find(|candidate| candidate.id == parent_id)
    else {
        return Ok(true);
    };
    if parent.issue_type != IssueTypeWire::Phase
        || parent.status == StatusWire::Closed
    {
        return Ok(true);
    }
    let all_children_closed = store.issues.iter().all(|candidate| {
        candidate.parent_id.as_deref() != Some(parent_id)
            || candidate.status == StatusWire::Closed
    });
    if !all_children_closed {
        return Ok(true);
    }

    let parent = store
        .close_one(
            parent_id,
            closed_at,
            Some("delegated work landed".to_string()),
            BeadResolutionWire::Done,
        )?
        .expect("non-closed delegated parent phase closes");
    batch.closed_ids.push(parent.id.clone());
    batch.event_closed.push(CloseEvent {
        issue: parent.clone(),
        forced_descendant_ids: Vec::new(),
    });
    batch.returned.push(parent);
    Ok(true)
}

const UNRESOLVED_DESCENDANT_DISPLAY_LIMIT: usize = 8;

fn unresolved_descendants<'a>(
    issues: &'a [IssueWire],
    issue_id: &str,
) -> Vec<&'a IssueWire> {
    sorted_descendants(issues, issue_id)
        .into_iter()
        .filter(|descendant| descendant.status != StatusWire::Closed)
        .collect()
}

/// Reject closing any batch target whose unresolved descendants are not
/// themselves also being closed by the same batch.
fn reject_unclosed_descendants_in_batch(
    issues: &[IssueWire],
    targets: &[String],
) -> Result<(), BeadError> {
    let target_set: BTreeSet<&str> =
        targets.iter().map(String::as_str).collect();
    for issue_id in targets {
        let unresolved: Vec<&IssueWire> =
            unresolved_descendants(issues, issue_id)
                .into_iter()
                .filter(|descendant| {
                    !target_set.contains(descendant.id.as_str())
                })
                .collect();
        if !unresolved.is_empty() {
            return Err(unclosed_descendants_error(issue_id, &unresolved));
        }
    }
    Ok(())
}

fn unclosed_descendants_error(
    issue_id: &str,
    unresolved: &[&IssueWire],
) -> BeadError {
    let shown = unresolved
        .iter()
        .take(UNRESOLVED_DESCENDANT_DISPLAY_LIMIT)
        .map(|descendant| {
            format!(
                "{} ({})",
                descendant.id,
                mutation_status_value(&descendant.status)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let remainder = unresolved
        .len()
        .saturating_sub(UNRESOLVED_DESCENDANT_DISPLAY_LIMIT);
    let remainder_text = if remainder == 0 {
        String::new()
    } else {
        format!(", and {remainder} more")
    };
    BeadError::validation(format!(
        "cannot close {issue_id}: {} descendant(s) are not closed: {shown}{remainder_text}; close them first or use --force with --reason and --resolution canceled|superseded",
        unresolved.len()
    ))
}

fn reopen_closed_ancestors(
    store: &mut MutableStore,
    issue_id: &str,
    opened_at: &str,
) -> Result<Vec<IssueWire>, BeadError> {
    let mut parent_id = store.get_issue(issue_id)?.parent_id.clone();
    let mut visited = BTreeSet::new();
    let mut reopened = Vec::new();
    while let Some(current_id) = parent_id {
        if !visited.insert(current_id.clone()) {
            break;
        }
        let Some(index) =
            store.issues.iter().position(|issue| issue.id == current_id)
        else {
            break;
        };
        parent_id = store.issues[index].parent_id.clone();
        if store.issues[index].status != StatusWire::Closed {
            continue;
        }
        store.issues[index].status = StatusWire::Open;
        store.issues[index].resolution = None;
        store.issues[index].updated_at = opened_at.to_string();
        let ancestor = store.issues[index].clone();
        ancestor.validate()?;
        store.append_issue_event(
            &ancestor.id,
            BeadEventOperationWire::IssueOpened,
            BeadEventPayloadWire::IssueOpened,
            opened_at,
            &ancestor.created_by,
        )?;
        reopened.push(ancestor);
    }
    Ok(reopened)
}

pub fn remove_issues(
    beads_dir: &Path,
    issue_ids: &[String],
) -> Result<BeadMutationOutcomeWire, BeadError> {
    if issue_ids.is_empty() {
        return Err(BeadError::validation(
            "remove_issues() requires at least one issue ID",
        ));
    }

    with_bead_mutation_lock(beads_dir, "remove", || {
        let mut store = MutableStore::load(beads_dir)?;
        let mut requested = Vec::new();
        let mut requested_ids = BTreeSet::new();
        for issue_id in issue_ids {
            let issue = store.get_issue(issue_id)?.clone();
            if requested_ids.insert(issue.id.clone()) {
                requested.push(issue);
            }
        }

        let mut removed = Vec::new();
        let mut removed_ids = BTreeSet::new();
        for issue in &requested {
            if issue.issue_type == IssueTypeWire::Plan {
                for descendant in sorted_descendants(&store.issues, &issue.id) {
                    if removed_ids.insert(descendant.id.clone()) {
                        removed.push(descendant.clone());
                    }
                }
            }
            if removed_ids.insert(issue.id.clone()) {
                removed.push(issue.clone());
            }
        }

        let removed_at = now_utc();
        for issue in &requested {
            let cascade_removed_issue_ids =
                if issue.issue_type == IssueTypeWire::Plan {
                    sorted_descendants(&store.issues, &issue.id)
                        .into_iter()
                        .map(|descendant| descendant.id.clone())
                        .collect()
                } else {
                    Vec::new()
                };
            store.append_issue_event(
                &issue.id,
                BeadEventOperationWire::IssueRemoved,
                BeadEventPayloadWire::IssueRemoved {
                    cascade_removed_issue_ids,
                },
                &removed_at,
                &issue.created_by,
            )?;
        }

        store
            .issues
            .retain(|issue| !removed_ids.contains(&issue.id));
        for issue in &mut store.issues {
            issue.dependencies.retain(|dep| {
                !removed_ids.contains(&dep.issue_id)
                    && !removed_ids.contains(&dep.depends_on_id)
            });
        }
        store.save()?;

        let mut result = outcome(
            "rm",
            true,
            removed.iter().map(|issue| issue.id.clone()).collect(),
        );
        result.issues = removed;
        Ok(result)
    })
}

pub fn remove_issue(
    beads_dir: &Path,
    issue_id: &str,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    remove_issues(beads_dir, &[issue_id.to_string()])
}

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

pub fn mark_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    set_ready_to_work(beads_dir, epic_id, true, true, now).map(|mut outcome| {
        outcome.operation = "mark_ready_to_work".to_string();
        outcome
    })
}

pub fn unmark_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    set_ready_to_work(beads_dir, epic_id, false, false, now).map(
        |mut outcome| {
            outcome.operation = "unmark_ready_to_work".to_string();
            outcome
        },
    )
}

pub fn export_jsonl(
    beads_dir: &Path,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let store = MutableStore::load(beads_dir)?;
    store.save_issues()?;
    Ok(outcome(
        "export_jsonl",
        true,
        store.issues.iter().map(|issue| issue.id.clone()).collect(),
    ))
}

pub fn sync_is_clean(beads_dir: &Path) -> Result<bool, BeadError> {
    let jsonl_path = beads_dir.join("issues.jsonl");
    if !jsonl_path.exists() {
        return Ok(true);
    }
    let repo_root = find_git_root(beads_dir)?;
    let Some(repo_root) = repo_root else {
        return Ok(true);
    };
    let status = Command::new("git")
        .arg("diff")
        .arg("--quiet")
        .arg(&jsonl_path)
        .current_dir(repo_root)
        .status()?;
    Ok(status.success())
}

fn set_ready_to_work(
    beads_dir: &Path,
    epic_id: &str,
    ready: bool,
    reject_already_ready: bool,
    now: Option<String>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    with_bead_mutation_lock(beads_dir, "set_ready_to_work", || {
        let mut store = MutableStore::load(beads_dir)?;
        let index = store.issue_index(epic_id)?;
        if store.issues[index].issue_type != IssueTypeWire::Plan {
            return Err(BeadError {
                kind: "not_a_plan".to_string(),
                message: format!(
                    "is_ready_to_work only applies to plan beads (got phase for {epic_id})"
                ),
            });
        }
        let tier = store.issues[index].tier.as_ref();
        if !matches!(tier, Some(BeadTierWire::Epic)) {
            return Err(BeadError {
                kind: "not_workable_plan".to_string(),
                message: format!(
                    "sase bead work only applies to epic plan beads (got {} for {epic_id})",
                    tier_label(tier)
                ),
            });
        }
        if reject_already_ready && store.issues[index].is_ready_to_work {
            return Err(BeadError {
                kind: "already_ready".to_string(),
                message: format!(
                    "{epic_id} is already marked is_ready_to_work=True"
                ),
            });
        }
        store.issues[index].is_ready_to_work = ready;
        store.issues[index].updated_at = now.unwrap_or_else(now_utc);
        let issue = store.issues[index].clone();
        store.append_issue_event(
            epic_id,
            if ready {
                BeadEventOperationWire::ReadyMarked
            } else {
                BeadEventOperationWire::ReadyUnmarked
            },
            if ready {
                BeadEventPayloadWire::ReadyMarked
            } else {
                BeadEventPayloadWire::ReadyUnmarked
            },
            &issue.updated_at,
            &issue.created_by,
        )?;
        store.save()?;

        let mut result = outcome("ready_to_work", true, vec![issue.id.clone()]);
        result.issue = Some(issue);
        Ok(result)
    })
}

fn apply_update_fields(
    issue: &mut IssueWire,
    fields: BeadUpdateFieldsWire,
) -> Result<(), BeadError> {
    if let Some(value) = fields.title {
        issue.title = value;
    }
    if let Some(value) = fields.status {
        issue.status = parse_status(&value)?;
        if issue.status != StatusWire::Closed {
            issue.resolution = None;
        }
    }
    if let Some(value) = fields.assignee {
        issue.assignee = value;
    }
    if let Some(value) = fields.description {
        issue.description = value;
    }
    if let Some(value) = fields.notes {
        issue.notes = value;
    }
    if let Some(value) = fields.design {
        issue.design = value;
    }
    if let Some(value) = fields.model {
        issue.model = normalize_model(value)?;
    }
    if let Some(value) = fields.size {
        issue.size = Some(value);
    }
    if let Some(value) = fields.closed_at {
        issue.closed_at = value;
    }
    if let Some(value) = fields.close_reason {
        issue.close_reason = value;
    }
    if let Some(value) = fields.resolution {
        issue.resolution = value;
    }
    if let Some(value) = fields.changespec_name {
        issue.changespec_name = value;
    }
    if let Some(value) = fields.changespec_bug_id {
        issue.changespec_bug_id = value;
    }
    if let Some(value) = fields.tier {
        issue.tier = Some(value);
    }
    Ok(())
}

fn event_fields_from_update_fields(
    fields: &BeadUpdateFieldsWire,
) -> Result<BeadIssueUpdateEventFieldsWire, BeadError> {
    let status = fields.status.as_deref().map(parse_status).transpose()?;
    let resolution = match (&status, &fields.resolution) {
        (Some(status), None) if *status != StatusWire::Closed => Some(None),
        _ => fields.resolution.clone(),
    };
    let event_fields = BeadIssueUpdateEventFieldsWire {
        title: fields.title.clone(),
        status,
        assignee: fields.assignee.clone(),
        description: fields.description.clone(),
        notes: fields.notes.clone(),
        design: fields.design.clone(),
        model: fields.model.clone().map(normalize_model).transpose()?,
        size: fields.size.clone(),
        closed_at: fields.closed_at.clone(),
        close_reason: fields.close_reason.clone(),
        resolution,
        changespec_name: fields.changespec_name.clone(),
        changespec_bug_id: fields.changespec_bug_id.clone(),
        tier: fields.tier.clone(),
        is_ready_to_work: fields.is_ready_to_work,
    };
    if event_fields == BeadIssueUpdateEventFieldsWire::default() {
        return Err(BeadError::validation(
            "update() requires at least one mutable bead field",
        ));
    }
    Ok(event_fields)
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

fn normalize_model(value: String) -> Result<String, BeadError> {
    let model = value.trim().to_string();
    validate_model_value(&model)?;
    Ok(model)
}

fn normalize_references<T: AsRef<str>>(
    references: &[T],
) -> Result<Vec<String>, BeadError> {
    normalize_artifact_ref_list(references).map_err(|error| BeadError {
        kind: error.kind,
        message: error.message,
    })
}

fn tier_label(tier: Option<&BeadTierWire>) -> &'static str {
    match tier {
        Some(BeadTierWire::Plan) => "plan",
        Some(BeadTierWire::Epic) => "epic",
        None => "missing tier",
    }
}

struct MutableStore {
    beads_dir: PathBuf,
    config: BeadConfigWire,
    issues: Vec<IssueWire>,
    streams: Vec<BeadEventStreamWire>,
}

impl MutableStore {
    fn load(beads_dir: &Path) -> Result<Self, BeadError> {
        if !beads_dir.is_dir() {
            return Err(BeadError::io(format!(
                "No beads directory found at {}",
                beads_dir.display()
            )));
        }
        let fallback = default_config("beads", "");
        let config = load_config(beads_dir, fallback)?;
        let (issues, streams) = if event_store_present(beads_dir) {
            let (_manifest, streams) = read_event_store(beads_dir)?;
            let issues = reduce_event_streams(&streams)?;
            (issues, streams)
        } else {
            let issues =
                import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))?
                    .issues;
            let streams = import_issues_to_event_streams(&issues)?;
            (issues, streams)
        };
        Ok(Self {
            beads_dir: beads_dir.to_path_buf(),
            config,
            issues,
            streams,
        })
    }

    fn save(&self) -> Result<(), BeadError> {
        write_event_store(&self.beads_dir, &self.streams)?;
        save_config(&self.beads_dir, &self.config)?;
        self.save_issues()
    }

    fn save_issues(&self) -> Result<(), BeadError> {
        write_issues_jsonl(&self.beads_dir, &self.issues)
    }

    fn issue_index(&self, issue_id: &str) -> Result<usize, BeadError> {
        self.issues
            .iter()
            .position(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    fn get_issue(&self, issue_id: &str) -> Result<&IssueWire, BeadError> {
        self.issues
            .iter()
            .find(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    fn append_issue_event(
        &mut self,
        issue_id: &str,
        operation: BeadEventOperationWire,
        payload: BeadEventPayloadWire,
        timestamp: &str,
        actor: &str,
    ) -> Result<(), BeadError> {
        let stream_id = self.stream_id_for_issue(issue_id)?;
        let stream = self.stream_for_mut(&stream_id)?;
        let ordinal = stream.events.len() + 1;
        let event_id = mint_bead_event_id(
            &stream_id, ordinal, timestamp, actor, operation, issue_id,
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

    fn stream_for_mut(
        &mut self,
        stream_id: &str,
    ) -> Result<&mut BeadEventStreamWire, BeadError> {
        if let Some(index) = self
            .streams
            .iter()
            .position(|stream| stream.stream_id == stream_id)
        {
            return Ok(&mut self.streams[index]);
        }
        self.streams.push(BeadEventStreamWire {
            stream_id: stream_id.to_string(),
            root_issue_id: stream_id.to_string(),
            events: Vec::new(),
        });
        self.streams.last_mut().ok_or_else(|| {
            BeadError::validation(format!(
                "failed to create bead event stream {stream_id}"
            ))
        })
    }

    fn stream_id_for_issue(&self, issue_id: &str) -> Result<String, BeadError> {
        let issue = self.get_issue(issue_id)?;
        if issue.issue_type == IssueTypeWire::Plan {
            return Ok(issue.id.clone());
        }
        Ok(issue
            .parent_id
            .as_ref()
            .filter(|parent_id| {
                self.issues
                    .iter()
                    .any(|candidate| candidate.id == **parent_id)
            })
            .cloned()
            .unwrap_or_else(|| issue.id.clone()))
    }

    fn close_one(
        &mut self,
        issue_id: &str,
        closed_at: &str,
        reason: Option<String>,
        resolution: BeadResolutionWire,
    ) -> Result<Option<IssueWire>, BeadError> {
        let index = self.issue_index(issue_id)?;
        if self.issues[index].status == StatusWire::Closed {
            return Ok(None);
        }
        self.issues[index].status = StatusWire::Closed;
        self.issues[index].closed_at = Some(closed_at.to_string());
        self.issues[index].close_reason = reason;
        self.issues[index].resolution = Some(resolution);
        self.issues[index].updated_at = closed_at.to_string();
        Ok(Some(self.issues[index].clone()))
    }
}

fn sorted_children<'a>(
    issues: &'a [IssueWire],
    parent_id: &str,
) -> Vec<&'a IssueWire> {
    let mut children: Vec<&IssueWire> = issues
        .iter()
        .filter(|issue| issue.parent_id.as_deref() == Some(parent_id))
        .collect();
    children.sort_by(|a, b| a.created_at.cmp(&b.created_at));
    children
}

fn sorted_descendants<'a>(
    issues: &'a [IssueWire],
    parent_id: &str,
) -> Vec<&'a IssueWire> {
    let mut descendants = Vec::new();
    let mut visited = BTreeSet::from([parent_id.to_string()]);
    collect_descendants(issues, parent_id, &mut visited, &mut descendants);
    descendants
}

fn collect_descendants<'a>(
    issues: &'a [IssueWire],
    parent_id: &str,
    visited: &mut BTreeSet<String>,
    descendants: &mut Vec<&'a IssueWire>,
) {
    for child in sorted_children(issues, parent_id) {
        if !visited.insert(child.id.clone()) {
            continue;
        }
        collect_descendants(issues, &child.id, visited, descendants);
        descendants.push(child);
    }
}

fn next_top_level_counter(
    issue_prefix: &str,
    config_counter: u64,
    issues: &[IssueWire],
) -> u64 {
    std::cmp::max(
        config_counter,
        max_top_level_counter(issue_prefix, issues) + 1,
    )
}

fn next_child_id(parent_id: &str, issues: &[IssueWire]) -> String {
    let local_max = issues
        .iter()
        .filter_map(|issue| direct_child_counter(parent_id, &issue.id))
        .max()
        .unwrap_or(0);
    format!("{parent_id}.{}", local_max + 1)
}

fn max_top_level_counter(issue_prefix: &str, issues: &[IssueWire]) -> u64 {
    let expected_prefix = format!("{issue_prefix}-");
    issues
        .iter()
        .map(|issue| issue.id.as_str())
        .filter_map(|issue_id| {
            issue_id.strip_prefix(&expected_prefix).map(str::to_string)
        })
        .filter(|suffix| !suffix.contains('.'))
        .filter_map(|suffix| from_base36(&suffix))
        .max()
        .unwrap_or(0)
}

fn direct_child_counter(parent_id: &str, issue_id: &str) -> Option<u64> {
    let prefix = format!("{parent_id}.");
    let suffix = issue_id.strip_prefix(&prefix)?;
    if suffix.contains('.') {
        return None;
    }
    suffix.parse::<u64>().ok()
}

fn parse_status(value: &str) -> Result<StatusWire, BeadError> {
    match value {
        "open" => Ok(StatusWire::Open),
        "claimed" => Ok(StatusWire::Claimed),
        "ready" => Ok(StatusWire::Ready),
        "in_progress" => Ok(StatusWire::InProgress),
        "closed" => Ok(StatusWire::Closed),
        _ => Err(BeadError::validation(format!(
            "invalid bead status: {value}"
        ))),
    }
}

fn mutation_status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

fn to_base36(mut n: u64) -> String {
    const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut digits = Vec::new();
    while n > 0 {
        digits.push(ALPHABET[(n % 36) as usize] as char);
        n /= 36;
    }
    digits.iter().rev().collect()
}

fn from_base36(value: &str) -> Option<u64> {
    u64::from_str_radix(value, 36).ok()
}

fn now_utc() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn find_git_root(path: &Path) -> Result<Option<PathBuf>, BeadError> {
    let cwd = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    let output = Command::new("git")
        .arg("rev-parse")
        .arg("--show-toplevel")
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Ok(None);
    }
    let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if root.is_empty() {
        Ok(None)
    } else {
        Ok(Some(PathBuf::from(root)))
    }
}

fn not_found(issue_id: &str) -> BeadError {
    BeadError {
        kind: "not_found".to_string(),
        message: format!("Issue not found: {issue_id}"),
    }
}

fn durable_store_error(
    operation: &str,
    beads_dir: &Path,
    error: BeadError,
) -> BeadError {
    BeadError {
        kind: error.kind,
        message: format!(
            "failed to {operation} durable bead store {}: {}",
            beads_dir.display(),
            error.message
        ),
    }
}

fn with_bead_mutation_lock(
    beads_dir: &Path,
    operation_name: &str,
    mutation: impl FnOnce() -> Result<BeadMutationOutcomeWire, BeadError>,
) -> Result<BeadMutationOutcomeWire, BeadError> {
    let lock_path = bead_mutation_lock_path(beads_dir);
    let lock = lock_bead_mutation_with_timeout(
        beads_dir,
        &lock_path,
        timeout_from_env(
            BEAD_MUTATION_LOCK_TIMEOUT_ENV,
            BEAD_MUTATION_LOCK_TIMEOUT_DEFAULT,
        ),
        operation_name,
    )?;
    let lock_wait_ms = lock.waited_ms();
    let result = mutation();
    let unlock_result = lock.release().map_err(|error| BeadError {
        kind: "lock_release".to_string(),
        message: format!(
            "failed to release bead mutation lock {} for store {}: {error}",
            lock_path.display(),
            beads_dir.display()
        ),
    });
    match (result, unlock_result) {
        (Ok(mut value), Ok(())) => {
            value.lock_wait_ms = lock_wait_ms;
            Ok(value)
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(unlock_error)) => Err(unlock_error),
        (Err(error), Err(unlock_error)) => Err(BeadError {
            kind: unlock_error.kind,
            message: format!(
                "{}; the locked mutation also failed with {}: {}",
                unlock_error.message, error.kind, error.message
            ),
        }),
    }
}

fn lock_bead_mutation_with_timeout(
    beads_dir: &Path,
    lock_path: &Path,
    timeout: Duration,
    operation: &str,
) -> Result<crate::store_lock::HeldStoreLock, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    let holder_path = bead_mutation_holder_path(beads_dir);
    acquire_store_lock(
        lock_path,
        &holder_path,
        LockMode::Exclusive,
        timeout,
        operation,
    )
    .map_err(|error| match error {
        error @ StoreLockError::Timeout { .. } => BeadError {
            kind: "lock_timeout".to_string(),
            message: format!("{error} for store {}", beads_dir.display()),
        },
        error => BeadError {
            kind: "lock".to_string(),
            message: format!("{error} for store {}", beads_dir.display()),
        },
    })
}

fn bead_mutation_holder_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join(BEAD_MUTATION_HOLDER_FILENAME)
}

fn bead_mutation_lock_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join(BEAD_MUTATION_LOCK_FILENAME)
}

fn outcome(
    operation: &str,
    changed: bool,
    issue_ids: Vec<String>,
) -> BeadMutationOutcomeWire {
    BeadMutationOutcomeWire {
        operation: operation.to_string(),
        changed,
        lock_wait_ms: 0,
        issue_ids,
        closed_ids: Vec::new(),
        already_closed_ids: Vec::new(),
        noted_ids: Vec::new(),
        cascade_closed_ids: Vec::new(),
        message: String::new(),
        issue: None,
        issues: Vec::new(),
        dependency: None,
        dependencies: Vec::new(),
        references: Vec::new(),
        next_counter: None,
        rollback_preclaims: Vec::new(),
        reopened_ancestor_ids: Vec::new(),
        unchanged_ids: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{mpsc, Arc, Barrier};
    use std::thread;
    use std::time::Instant;
    use tempfile::tempdir;

    fn task_plus_one_fixture(
        status: StatusWire,
    ) -> (tempfile::TempDir, PathBuf, String) {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let task = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Corroborated task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                created_by: Some("creator-agent".to_string()),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        if status == StatusWire::Closed {
            close_issues(
                &beads_dir,
                std::slice::from_ref(&task.id),
                Some("stale close".to_string()),
                Some(BeadResolutionWire::Canceled),
                false,
                Some("2026-01-01T00:01:00Z".to_string()),
            )
            .unwrap();
        }
        (temp, beads_dir, task.id)
    }

    #[test]
    fn task_plus_one_is_atomic_normalized_and_promotes_closed_task() {
        let (_temp, beads_dir, task_id) =
            task_plus_one_fixture(StatusWire::Closed);

        let result = add_task_plus_one(
            &beads_dir,
            task_id.rsplit('-').next().unwrap(),
            " reporter-agent ",
            " reproduced on a clean checkout ",
            &[
                "research:202608/repro.md".to_string(),
                "research:202608/repro.md".to_string(),
                "bead:sase-related".to_string(),
            ],
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert!(result.changed);
        let issue = result.issue.unwrap();
        assert_eq!(issue.status, StatusWire::Ready);
        assert_eq!(issue.closed_at, None);
        assert_eq!(issue.close_reason, None);
        assert_eq!(issue.resolution, None);
        assert_eq!(issue.plus_one_count(), 1);
        assert_eq!(issue.plus_one_evidence[0].reporter, "reporter-agent");
        assert_eq!(
            issue.plus_one_evidence[0].note,
            "reproduced on a clean checkout"
        );
        assert_eq!(issue.refs, issue.plus_one_evidence[0].refs);

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let event = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.operation == BeadEventOperationWire::TaskPlusOneRecorded
            })
            .unwrap();
        assert_eq!(event.actor, "reporter-agent");
        assert_eq!(reduce_event_streams(&streams).unwrap(), vec![issue]);
    }

    #[test]
    fn task_plus_one_creator_and_repeat_are_byte_identical_noops() {
        let (_temp, beads_dir, task_id) =
            task_plus_one_fixture(StatusWire::Open);
        let before_creator = persisted_claim_state(&beads_dir);

        let creator = add_task_plus_one(
            &beads_dir,
            &task_id,
            "creator-agent",
            "creator retry",
            &[],
            None,
        )
        .unwrap();
        assert!(!creator.changed);
        assert_eq!(persisted_claim_state(&beads_dir), before_creator);

        add_task_plus_one(
            &beads_dir,
            &task_id,
            "reporter-agent",
            "first independent report",
            &[],
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();
        let before_repeat = persisted_claim_state(&beads_dir);
        let repeat = add_task_plus_one(
            &beads_dir,
            &task_id,
            "reporter-agent",
            "later supplemental detail",
            &["research:202608/later.md".to_string()],
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap();
        assert!(!repeat.changed);
        assert_eq!(persisted_claim_state(&beads_dir), before_repeat);
    }

    #[test]
    fn concurrent_task_plus_ones_preserve_reporters_and_deduplicate_retries() {
        let (_temp, beads_dir, task_id) =
            task_plus_one_fixture(StatusWire::Open);
        let barrier = Arc::new(Barrier::new(4));
        let mut handles = Vec::new();
        for reporter in ["agent-a", "agent-b", "agent-a"] {
            let beads_dir = beads_dir.clone();
            let task_id = task_id.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                add_task_plus_one(
                    &beads_dir,
                    &task_id,
                    reporter,
                    "independent reproduction",
                    &[],
                    Some("2026-01-02T00:00:00Z".to_string()),
                )
                .unwrap()
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        let issue = MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&task_id)
            .unwrap()
            .clone();
        assert_eq!(issue.plus_one_count(), 2);
        assert_eq!(
            issue
                .plus_one_evidence
                .iter()
                .map(|evidence| evidence.reporter.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["agent-a", "agent-b"])
        );
    }

    #[test]
    fn create_requires_size_only_for_new_tasks() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let before = fs::read(beads_dir.join("issues.jsonl")).unwrap();

        let error = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Missing size".to_string(),
                issue_type: IssueTypeWire::Task,
                ..Default::default()
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("requires an explicit size"));
        assert_eq!(fs::read(beads_dir.join("issues.jsonl")).unwrap(), before);
    }

    #[test]
    fn create_add_and_remove_references_use_individual_events_and_noop_cleanly()
    {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let created = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Referenced plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                refs: vec![
                    "research:202607/report.md".to_string(),
                    "research:202607/report.md".to_string(),
                    "bead:sase-bb.1".to_string(),
                ],
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        let issue = created.issue.unwrap();
        assert_eq!(
            issue.refs,
            vec![
                "research:202607/report.md".to_string(),
                "bead:sase-bb.1".to_string(),
            ]
        );
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert_eq!(
            streams[0]
                .events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::ReferenceAdded,
                BeadEventOperationWire::ReferenceAdded,
            ]
        );

        let added = add_bead_references(
            &beads_dir,
            &issue.id,
            &[
                "bead:sase-bb.1".to_string(),
                "agent:bbugyi200.athena.9w".to_string(),
            ],
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap();
        assert!(added.changed);
        assert_eq!(
            added.references,
            vec!["agent:bbugyi200.athena.9w".to_string()]
        );
        let event_count =
            read_event_store(&beads_dir).unwrap().1[0].events.len();

        let duplicate = add_bead_references(
            &beads_dir,
            &issue.id,
            &["agent:bbugyi200.athena.9w".to_string()],
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        assert!(!duplicate.changed);
        assert_eq!(
            read_event_store(&beads_dir).unwrap().1[0].events.len(),
            event_count
        );

        let removed = remove_bead_references(
            &beads_dir,
            &issue.id,
            &[
                "bead:sase-missing".to_string(),
                "research:202607/report.md".to_string(),
            ],
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(removed.changed);
        assert_eq!(
            removed.references,
            vec!["research:202607/report.md".to_string()]
        );
        assert_eq!(
            removed.issue.unwrap().refs,
            vec![
                "bead:sase-bb.1".to_string(),
                "agent:bbugyi200.athena.9w".to_string(),
            ]
        );

        let absent = remove_bead_references(
            &beads_dir,
            &issue.id,
            &["bead:sase-missing".to_string()],
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        assert!(!absent.changed);
    }

    #[test]
    fn reference_mutations_reject_malformed_entries_without_writing() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let before = persisted_claim_state(&beads_dir);

        let error = add_bead_references(
            &beads_dir,
            &issue.id,
            &["not-a-reference".to_string()],
            None,
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("artifact reference list entry 1"));
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn init_store_writes_a_root_level_store_for_a_dot_dirname() {
        let temp = tempdir().unwrap();
        let root = temp.path().join("beads-sidecar");
        fs::create_dir_all(&root).unwrap();

        init_store(&root, ".", "sase", "").unwrap();

        assert!(root.join("config.json").is_file());
        assert!(root.join("issues.jsonl").is_file());
        assert!(root.join("beads.db").is_file());
    }

    #[test]
    fn create_uses_explicit_creator_for_issue_and_reference_events() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");

        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Attributed task".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                refs: vec!["bead:sase-parent".to_string()],
                created_by: Some("  bbugyi200.athena.q8  ".to_string()),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        assert_eq!(issue.owner, "owner@example.com");
        assert_eq!(issue.created_by, "bbugyi200.athena.q8");
        let (_, streams) = read_event_store(&beads_dir).unwrap();
        let stream = streams
            .iter()
            .find(|stream| stream.events[0].issue_id == issue.id)
            .unwrap();
        assert_eq!(stream.events.len(), 2);
        assert_eq!(
            stream
                .events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            [
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::ReferenceAdded,
            ]
        );
        assert!(stream
            .events
            .iter()
            .all(|event| event.actor == "bbugyi200.athena.q8"));
    }

    #[test]
    fn create_resolves_creator_from_phase_parent_then_store_owner() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "owner@example.com").unwrap();
        let beads_dir = temp.path().join("beads");

        let blank = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Blank explicit creator".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                created_by: Some("   ".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(blank.created_by, "owner@example.com");
        let absent = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Absent explicit creator".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Small),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(absent.created_by, "owner@example.com");

        let parent = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Attributed epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                created_by: Some("bbugyi200.athena.q8--plan".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let inherited = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Inherited phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(parent.id.clone()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(inherited.created_by, "bbugyi200.athena.q8--plan");
        let overridden = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Explicit phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(parent.id.clone()),
                created_by: Some("bbugyi200.athena.other".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(overridden.created_by, "bbugyi200.athena.other");

        let child_plan = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Child plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                parent_id: Some(parent.id),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(child_plan.created_by, "owner@example.com");
        let missing_parent = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Missing-parent phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some("sase-missing".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(missing_parent.created_by, "owner@example.com");
    }

    #[test]
    fn phase_with_blank_parent_creator_falls_back_to_store_owner() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "sase", "").unwrap();
        let beads_dir = temp.path().join("beads");
        let parent = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Legacy unattributed epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert!(parent.created_by.is_empty());
        let mut config =
            load_config(&beads_dir, default_config("sase", "")).unwrap();
        config.owner = "owner@example.com".to_string();
        save_config(&beads_dir, &config).unwrap();

        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Fallback phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(parent.id),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(phase.created_by, "owner@example.com");
    }

    #[test]
    fn phase_size_round_trips_through_create_update_events_and_projection() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Sized phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                size: Some(PhaseSizeWire::Medium),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(phase.size, Some(PhaseSizeWire::Medium));

        let updated = update_issue(
            &beads_dir,
            &phase.id,
            BeadUpdateFieldsWire {
                size: Some(PhaseSizeWire::Large),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(updated.size, Some(PhaseSizeWire::Large));

        let jsonl = fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(jsonl.contains(r#""size":"large""#));
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert!(streams.iter().flat_map(|stream| &stream.events).any(
            |event| {
                matches!(
                    &event.payload,
                    BeadEventPayloadWire::IssueUpdated { fields }
                        if fields.size == Some(PhaseSizeWire::Large)
                )
            }
        ));
        let reloaded = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            reloaded.get_issue(&phase.id).unwrap().size,
            Some(PhaseSizeWire::Large)
        );

        let error = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Invalid sized plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                size: Some(PhaseSizeWire::Small),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(
            error.message,
            "Only phase and task issues can carry size metadata"
        );
    }

    #[test]
    fn task_create_and_ready_updates_round_trip_through_events() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let task = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Discovered follow-up".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(PhaseSizeWire::Medium),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(task.issue_type, IssueTypeWire::Task);
        assert_eq!(task.status, StatusWire::Open);
        assert_eq!(task.parent_id, None);
        assert_eq!(task.tier, None);

        let ready = update_issue(
            &beads_dir,
            &task.id,
            BeadUpdateFieldsWire {
                status: Some("ready".to_string()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(ready.status, StatusWire::Ready);
        assert_eq!(
            MutableStore::load(&beads_dir)
                .unwrap()
                .get_issue(&task.id)
                .unwrap()
                .status,
            StatusWire::Ready
        );
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert!(streams.iter().flat_map(|stream| &stream.events).any(
            |event| {
                matches!(
                    &event.payload,
                    BeadEventPayloadWire::IssueUpdated { fields }
                        if fields.status == Some(StatusWire::Ready)
                )
            }
        ));

        let plan = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Plan".to_string(),
                issue_type: IssueTypeWire::Plan,
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(plan.id),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let error = update_issue(
            &beads_dir,
            &phase.id,
            BeadUpdateFieldsWire {
                status: Some("ready".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();
        assert_eq!(error.message, "Only task issues can have ready status");
    }

    #[test]
    fn update_with_matching_fields_is_a_quiet_no_op() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        let before = persisted_claim_state(&beads_dir);

        let result = update_issue(
            &beads_dir,
            &phase_id,
            BeadUpdateFieldsWire {
                title: Some("Phase".to_string()),
                status: Some("open".to_string()),
                assignee: Some(String::new()),
                model: Some(String::new()),
                now: Some("2026-01-01T00:09:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.operation, "update");
        assert!(!result.changed);
        assert_eq!(result.issue_ids, vec![phase_id]);
        assert_eq!(result.issue.unwrap().updated_at, "2026-01-01T00:01:00Z");
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn create_top_level_uses_current_store_max_and_persists_counter() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sase/sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(
            &beads_dir,
            &BeadConfigWire {
                issue_prefix: "sase".to_string(),
                next_counter: 1,
                owner: String::new(),
            },
        )
        .unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            r#"{"id":"sase-z","title":"Other","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        )
        .unwrap();

        let result = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Next".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(result.issue.unwrap().id, "sase-10");
        assert_eq!(
            load_config(&beads_dir, default_config("x", ""))
                .unwrap()
                .next_counter,
            37
        );
    }

    #[test]
    fn forced_close_plan_sweeps_open_children_before_parent() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Plan",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "A",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.2",
                    "B",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            Some("Canceled unfinished work".to_string()),
            Some(BeadResolutionWire::Canceled),
            true,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1", "sase-1"]);
        assert_eq!(result.closed_ids, vec!["sase-1.1", "sase-1"]);
        assert_eq!(result.cascade_closed_ids, vec!["sase-1.1"]);
        assert!(result.already_closed_ids.is_empty());
        assert!(result.noted_ids.is_empty());
        let exported =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(exported
            .contains(r#""id":"sase-1.1","title":"A","status":"closed""#));
        assert!(exported.contains(
            r#""close_reason":"forced by sase-1: Canceled unfinished work""#
        ));
        assert!(exported.contains(r#""resolution":"canceled""#));
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let forced_ids = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find_map(|event| match &event.payload {
                BeadEventPayloadWire::IssueClosed {
                    forced_descendant_ids,
                    ..
                } if event.issue_id == "sase-1" => {
                    Some(forced_descendant_ids.clone())
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(forced_ids, vec!["sase-1.1"]);
    }

    #[test]
    fn unforced_close_with_open_descendant_fails_without_writing() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = [
            issue(
                "sase-1",
                "Plan",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "Unfinished",
                "phase",
                Some("sase-1"),
                "in_progress",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            None,
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap_err();

        assert!(error.message.contains("cannot close sase-1"));
        assert!(error.message.contains("sase-1.1 (in_progress)"));
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
        assert!(!beads_dir.join("events").exists());
    }

    #[test]
    fn batch_close_preflights_every_request_before_writing() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = [
            issue(
                "sase-1",
                "First",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-2",
                "Second",
                "plan",
                None,
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-2.1",
                "Unfinished",
                "phase",
                Some("sase-2"),
                "open",
                "2026-01-01T00:02:00Z",
            ),
        ]
        .join("\n")
            + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = close_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-2".to_string()],
            None,
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap_err();

        assert!(error.message.contains("cannot close sase-2"));
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
        assert!(!beads_dir.join("events").exists());
    }

    #[test]
    fn repeat_close_is_write_free_and_classified_as_already_closed() {
        let (_temp, beads_dir, issue_id) =
            closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));
        let before = persisted_claim_state(&beads_dir);

        let result = close_issues(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            None,
            None,
            false,
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap();

        assert!(!result.changed);
        assert!(result.issue_ids.is_empty());
        assert!(result.closed_ids.is_empty());
        assert_eq!(result.already_closed_ids, vec![issue_id.clone()]);
        assert!(result.noted_ids.is_empty());
        assert!(result.cascade_closed_ids.is_empty());
        assert_eq!(result.issues.len(), 1);
        assert_eq!(
            result.issues[0].closed_at.as_deref(),
            Some("2026-01-02T00:00:00Z")
        );
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn repeat_close_with_note_writes_only_the_note() {
        let (_temp, beads_dir, issue_id) =
            closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));

        let result = close_issues_with_note(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            None,
            None,
            false,
            Some("extra evidence".to_string()),
            Some("agent-1".to_string()),
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap();

        assert!(result.changed);
        assert_eq!(result.issue_ids, vec![issue_id.clone()]);
        assert!(result.closed_ids.is_empty());
        assert_eq!(result.already_closed_ids, vec![issue_id.clone()]);
        assert_eq!(result.noted_ids, vec![issue_id.clone()]);
        assert!(result.cascade_closed_ids.is_empty());
        let issue = MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&issue_id)
            .unwrap()
            .clone();
        assert_eq!(issue.closed_at.as_deref(), Some("2026-01-02T00:00:00Z"));
        assert_eq!(issue.close_reason.as_deref(), Some("verified"));
        assert!(issue.notes.contains("extra evidence"));
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert_eq!(
            streams
                .iter()
                .flat_map(|stream| &stream.events)
                .filter(|event| {
                    event.issue_id == issue_id
                        && event.operation
                            == BeadEventOperationWire::IssueClosed
                })
                .count(),
            1
        );
    }

    #[test]
    fn conflicting_resolution_aborts_mixed_batch_before_writing() {
        let (_temp, beads_dir, closed_id) =
            closed_issue_fixture(BeadResolutionWire::Done, Some("verified"));
        let open_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Still open".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-02T01:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        let before = persisted_claim_state(&beads_dir);

        let error = close_issues_with_note(
            &beads_dir,
            &[open_id, closed_id.clone()],
            None,
            Some(BeadResolutionWire::Canceled),
            false,
            Some("must not land".to_string()),
            Some("agent-1".to_string()),
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert!(error.message.contains(&closed_id));
        assert!(error.message.contains("closed at 2026-01-02T00:00:00Z"));
        assert!(error.message.contains("resolution done"));
        assert!(error.message.contains("requested resolution canceled"));
        assert!(error.message.contains("sase bead open"));
        assert!(error.message.contains("sase bead note"));
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn conflicting_reason_aborts_before_writing() {
        let (_temp, beads_dir, issue_id) = closed_issue_fixture(
            BeadResolutionWire::Done,
            Some("original reason"),
        );
        let before = persisted_claim_state(&beads_dir);

        let error = close_issues(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            Some("different reason".to_string()),
            None,
            false,
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert!(error.message.contains("\"original reason\""));
        assert!(error.message.contains("\"different reason\""));
        assert!(error.message.contains("requested resolution (unspecified)"));
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn absent_resolution_does_not_conflict_with_recorded_canceled_close() {
        let (_temp, beads_dir, issue_id) = closed_issue_fixture(
            BeadResolutionWire::Canceled,
            Some("canceled intentionally"),
        );
        let before = persisted_claim_state(&beads_dir);

        let result = close_issues(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            None,
            None,
            false,
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap();

        assert!(!result.changed);
        assert_eq!(result.already_closed_ids, vec![issue_id]);
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn forced_close_requires_reason_and_non_done_resolution() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = issue(
            "sase-1",
            "Plan",
            "plan",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ) + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let no_reason = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            None,
            Some(BeadResolutionWire::Canceled),
            true,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap_err();
        assert!(no_reason.message.contains("requires a non-empty --reason"));

        let done = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            Some("Finished".to_string()),
            Some(BeadResolutionWire::Done),
            true,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap_err();
        assert!(done.message.contains("'done' is not allowed"));
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
        assert!(!beads_dir.join("events").exists());
    }

    #[test]
    fn update_status_closed_rejects_open_descendants() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = [
            issue(
                "sase-1",
                "Plan",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "Unfinished",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = update_issue(
            &beads_dir,
            "sase-1",
            BeadUpdateFieldsWire {
                status: Some("closed".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(error.message.contains("cannot close sase-1"));
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn reopening_grandchild_reopens_closed_ancestors_and_clears_resolution() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let closed_with_resolution = |value: String| {
            value.replace(
                r#","changespec_name":""#,
                r#","resolution":"done","changespec_name":""#,
            )
        };
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                closed_with_resolution(issue(
                    "sase-1",
                    "Root",
                    "plan",
                    None,
                    "closed",
                    "2026-01-01T00:00:00Z",
                )),
                closed_with_resolution(issue(
                    "sase-1.1",
                    "Parent",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:01:00Z",
                )),
                closed_with_resolution(issue(
                    "sase-1.1.1",
                    "Grandchild",
                    "plan",
                    Some("sase-1.1"),
                    "closed",
                    "2026-01-01T00:02:00Z",
                )),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = open_issue(
            &beads_dir,
            "sase-1.1.1",
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1.1", "sase-1"]);
        let store = MutableStore::load(&beads_dir).unwrap();
        for issue_id in ["sase-1", "sase-1.1", "sase-1.1.1"] {
            let reopened = store.get_issue(issue_id).unwrap();
            assert_eq!(reopened.status, StatusWire::Open);
            assert_eq!(reopened.resolution, None);
        }
        let opened_ids: Vec<String> = store
            .streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueOpened
            })
            .map(|event| event.issue_id.clone())
            .collect();
        assert_eq!(opened_ids, vec!["sase-1.1.1", "sase-1.1", "sase-1"]);
    }

    #[test]
    fn update_out_of_closed_reopens_closed_ancestor() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root",
                    "plan",
                    None,
                    "closed",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Child",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:01:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = update_issue(
            &beads_dir,
            "sase-1.1",
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1"]);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().status,
            StatusWire::InProgress
        );
    }

    #[test]
    fn update_issues_applies_same_fields_to_every_target_in_one_pass() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "First",
                    "task",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-2",
                    "Second",
                    "task",
                    None,
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-3",
                    "Third",
                    "task",
                    None,
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = update_issues(
            &beads_dir,
            &[
                "sase-1".to_string(),
                "sase-2".to_string(),
                "sase-3".to_string(),
            ],
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(outcome.changed);
        assert_eq!(outcome.issue_ids, vec!["sase-1", "sase-2", "sase-3"]);
        assert!(outcome.unchanged_ids.is_empty());
        assert_eq!(outcome.issues.len(), 3);
        for issue in &outcome.issues {
            assert_eq!(issue.status, StatusWire::InProgress);
        }

        let store = MutableStore::load(&beads_dir).unwrap();
        for issue_id in ["sase-1", "sase-2", "sase-3"] {
            assert_eq!(
                store.get_issue(issue_id).unwrap().status,
                StatusWire::InProgress
            );
        }
    }

    #[test]
    fn update_issues_mixed_batch_reports_changed_and_unchanged() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "First",
                    "task",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-2",
                    "Second",
                    "task",
                    None,
                    "in_progress",
                    "2026-01-01T00:01:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = update_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-2".to_string()],
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert!(outcome.changed);
        assert_eq!(outcome.issue_ids, vec!["sase-1".to_string()]);
        assert_eq!(outcome.unchanged_ids, vec!["sase-2".to_string()]);
        assert_eq!(outcome.issues.len(), 2);
        assert_eq!(outcome.issues[0].id, "sase-1");
        assert_eq!(outcome.issues[0].updated_at, "2026-01-02T00:00:00Z");
        assert_eq!(outcome.issues[1].id, "sase-2");
        assert_eq!(outcome.issues[1].updated_at, "2026-01-01T00:01:00Z");
    }

    #[test]
    fn update_issues_unknown_id_leaves_store_untouched() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = issue(
            "sase-1",
            "First",
            "task",
            None,
            "open",
            "2026-01-01T00:00:00Z",
        ) + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = update_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-missing".to_string()],
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, "not_found");
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn update_issues_invalid_field_value_leaves_every_target_unmodified() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = [
            issue(
                "sase-1",
                "First",
                "task",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-2",
                "Second",
                "task",
                None,
                "open",
                "2026-01-01T00:01:00Z",
            ),
        ]
        .join("\n")
            + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = update_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-2".to_string()],
            BeadUpdateFieldsWire {
                model: Some("bad\nmodel".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn update_issues_collapses_duplicate_ids_to_one_update() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            issue(
                "sase-1",
                "First",
                "task",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ) + "\n",
        )
        .unwrap();

        let outcome = update_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-1".to_string()],
            BeadUpdateFieldsWire {
                title: Some("Renamed".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(outcome.issue_ids, vec!["sase-1".to_string()]);
        assert_eq!(outcome.issues.len(), 1);
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let update_events = streams[0]
            .events
            .iter()
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueUpdated
            })
            .count();
        assert_eq!(update_events, 1);
    }

    #[test]
    fn update_issues_closes_parent_and_child_regardless_of_argument_order() {
        for order in [
            vec!["sase-1".to_string(), "sase-1.1".to_string()],
            vec!["sase-1.1".to_string(), "sase-1".to_string()],
        ] {
            let temp = tempdir().unwrap();
            let beads_dir = temp.path().join("sdd/beads");
            fs::create_dir_all(&beads_dir).unwrap();
            save_config(&beads_dir, &default_config("sase", "")).unwrap();
            fs::write(
                beads_dir.join("issues.jsonl"),
                [
                    issue(
                        "sase-1",
                        "Parent",
                        "plan",
                        None,
                        "open",
                        "2026-01-01T00:00:00Z",
                    ),
                    issue(
                        "sase-1.1",
                        "Child",
                        "phase",
                        Some("sase-1"),
                        "open",
                        "2026-01-01T00:01:00Z",
                    ),
                ]
                .join("\n")
                    + "\n",
            )
            .unwrap();

            let outcome = update_issues(
                &beads_dir,
                &order,
                BeadUpdateFieldsWire {
                    status: Some("closed".to_string()),
                    now: Some("2026-01-02T00:00:00Z".to_string()),
                    ..Default::default()
                },
            )
            .unwrap();

            assert!(outcome.changed);
            let store = MutableStore::load(&beads_dir).unwrap();
            assert_eq!(
                store.get_issue("sase-1").unwrap().status,
                StatusWire::Closed
            );
            assert_eq!(
                store.get_issue("sase-1.1").unwrap().status,
                StatusWire::Closed
            );
        }
    }

    #[test]
    fn update_issues_status_closed_rejects_out_of_batch_descendant() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        let original = [
            issue(
                "sase-1",
                "Parent",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ),
            issue(
                "sase-1.1",
                "In batch",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:01:00Z",
            ),
            issue(
                "sase-1.2",
                "Out of batch",
                "phase",
                Some("sase-1"),
                "open",
                "2026-01-01T00:02:00Z",
            ),
        ]
        .join("\n")
            + "\n";
        fs::write(beads_dir.join("issues.jsonl"), &original).unwrap();

        let error = update_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-1.1".to_string()],
            BeadUpdateFieldsWire {
                status: Some("closed".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(error.message.contains("sase-1.2"));
        assert_eq!(
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap(),
            original
        );
    }

    #[test]
    fn update_issues_reopens_shared_ancestor_only_once() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Parent",
                    "plan",
                    None,
                    "closed",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "First child",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.2",
                    "Second child",
                    "phase",
                    Some("sase-1"),
                    "closed",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let outcome = update_issues(
            &beads_dir,
            &["sase-1.1".to_string(), "sase-1.2".to_string()],
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                now: Some("2026-01-02T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        assert_eq!(outcome.reopened_ancestor_ids, vec!["sase-1".to_string()]);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
    }

    #[test]
    fn close_records_explicit_resolution_and_reopen_update_clears_it() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            issue(
                "sase-1",
                "Superseded plan",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ) + "\n",
        )
        .unwrap();

        let closed = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            Some("A replacement shipped".to_string()),
            Some(BeadResolutionWire::Superseded),
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();
        assert_eq!(
            closed.issues[0].resolution,
            Some(BeadResolutionWire::Superseded)
        );

        let reopened = update_issue(
            &beads_dir,
            "sase-1",
            BeadUpdateFieldsWire {
                status: Some("open".to_string()),
                now: Some("2026-01-03T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(reopened.issue.unwrap().resolution, None);
        assert_eq!(
            MutableStore::load(&beads_dir).unwrap().issues[0].resolution,
            None
        );
    }

    #[test]
    fn forced_close_plan_sweeps_through_nested_child_epics() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Root phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-1.1.1.1",
                    "Child phase",
                    "phase",
                    Some("sase-1.1.1"),
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            Some("Superseded unfinished tree".to_string()),
            Some(BeadResolutionWire::Superseded),
            true,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(
            result.issue_ids,
            vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1", "sase-1"]
        );
        let store = MutableStore::load(&beads_dir).unwrap();
        assert!(store
            .issues
            .iter()
            .all(|issue| issue.status == StatusWire::Closed));
        assert!(store
            .issues
            .iter()
            .all(|issue| issue.resolution
                == Some(BeadResolutionWire::Superseded)));
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let forced_ids = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find_map(|event| match &event.payload {
                BeadEventPayloadWire::IssueClosed {
                    forced_descendant_ids,
                    ..
                } if event.issue_id == "sase-1" => {
                    Some(forced_descendant_ids.clone())
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(forced_ids, vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1"]);
    }

    #[test]
    fn closing_child_epic_closes_completed_parent_phase() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Delegated phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-1.1.1.1",
                    "Child phase",
                    "phase",
                    Some("sase-1.1.1"),
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        close_issues(
            &beads_dir,
            &["sase-1.1.1.1".to_string()],
            Some("phase complete".to_string()),
            None,
            false,
            Some("2026-01-01T12:00:00Z".to_string()),
        )
        .unwrap();
        let result = close_issues(
            &beads_dir,
            &["sase-1.1.1".to_string()],
            Some("landed".to_string()),
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1.1", "sase-1.1"]);
        assert_eq!(result.closed_ids, vec!["sase-1.1.1", "sase-1.1"]);
        assert_eq!(result.cascade_closed_ids, vec!["sase-1.1"]);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().close_reason.as_deref(),
            Some("delegated work landed")
        );
        assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let parent_close_events: Vec<&BeadEventRecordWire> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.issue_id == "sase-1.1"
                    && event.operation == BeadEventOperationWire::IssueClosed
            })
            .collect();
        assert_eq!(parent_close_events.len(), 1);
        assert!(matches!(
            &parent_close_events[0].payload,
            BeadEventPayloadWire::IssueClosed { close_reason, .. }
                if close_reason.as_deref() == Some("delegated work landed")
        ));
        let projected = reduce_event_streams(&streams).unwrap();
        let projected_parent = projected
            .iter()
            .find(|issue| issue.id == "sase-1.1")
            .unwrap();
        assert_eq!(projected_parent.status, StatusWire::Closed);
        assert_eq!(
            projected_parent.close_reason.as_deref(),
            Some("delegated work landed")
        );
    }

    #[test]
    fn open_sibling_delegated_work_keeps_parent_phase_open() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Delegated phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "First child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-1.1.2",
                    "Second child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = close_issues(
            &beads_dir,
            &["sase-1.1.1".to_string()],
            None,
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1.1"]);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().status,
            StatusWire::Open
        );
    }

    #[test]
    fn nested_delegation_closes_only_phase_parents() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Root phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-1.1.1.1",
                    "Nested delegated phase",
                    "phase",
                    Some("sase-1.1.1"),
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
                issue(
                    "sase-1.1.1.1.1",
                    "Grandchild epic",
                    "plan",
                    Some("sase-1.1.1.1"),
                    "open",
                    "2026-01-01T00:04:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        close_issues(
            &beads_dir,
            &["sase-1.1.1.1.1".to_string()],
            Some("grandchild landed".to_string()),
            None,
            false,
            Some("2026-01-01T12:00:00Z".to_string()),
        )
        .unwrap();
        let result = close_issues(
            &beads_dir,
            &["sase-1.1.1".to_string()],
            Some("child landed".to_string()),
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1.1", "sase-1.1"]);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store
                .get_issue("sase-1.1.1.1")
                .unwrap()
                .close_reason
                .as_deref(),
            Some("delegated work landed")
        );
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().close_reason.as_deref(),
            Some("delegated work landed")
        );
        assert_eq!(store.get_issue("sase-1").unwrap().status, StatusWire::Open);
    }

    #[test]
    fn explicitly_closing_parent_and_child_emits_one_explicit_parent_event() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Delegated phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "closed",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = close_issues(
            &beads_dir,
            &["sase-1.1.1".to_string(), "sase-1.1".to_string()],
            None,
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1"]);
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let parent_close_events: Vec<&BeadEventRecordWire> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.issue_id == "sase-1.1"
                    && event.operation == BeadEventOperationWire::IssueClosed
            })
            .collect();
        assert_eq!(parent_close_events.len(), 1);
        assert!(matches!(
            &parent_close_events[0].payload,
            BeadEventPayloadWire::IssueClosed { close_reason, .. }
                if close_reason.is_none()
        ));
    }

    #[test]
    fn remove_plan_cascades_through_nested_child_epics() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Root phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-1.1.1.1",
                    "Child phase",
                    "phase",
                    Some("sase-1.1.1"),
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
                issue(
                    "sase-2",
                    "Unrelated",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:04:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        let result = remove_issue(&beads_dir, "sase-1").unwrap();

        assert_eq!(
            result.issue_ids,
            vec!["sase-1.1.1.1", "sase-1.1.1", "sase-1.1", "sase-1"]
        );
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store
                .issues
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-2"]
        );
    }

    #[test]
    fn remove_issues_removes_independent_roots_in_argument_order() {
        let (_temp, beads_dir) = batch_remove_fixture();

        let result = remove_issues(
            &beads_dir,
            &["sase-2".to_string(), "sase-1.1".to_string()],
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-2", "sase-1.1"]);
        assert_eq!(
            result
                .issues
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-2", "sase-1.1"]
        );
        let reloaded = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            reloaded
                .issues
                .iter()
                .map(|issue| issue.id.as_str())
                .collect::<Vec<_>>(),
            vec!["sase-1", "sase-1.2"]
        );

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let reduced = reduce_event_streams(&streams).unwrap();
        assert_eq!(reduced, reloaded.issues);
        let removal_events: Vec<&BeadEventRecordWire> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueRemoved
            })
            .collect();
        assert_eq!(removal_events.len(), 2);
        assert_eq!(removal_events[0].timestamp, removal_events[1].timestamp);
    }

    #[test]
    fn remove_issues_deduplicates_overlapping_roots_in_both_orders() {
        let (_temp, beads_dir) = batch_remove_fixture();
        let plan_first = remove_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-1.2".to_string()],
        )
        .unwrap();
        assert_eq!(
            plan_first.issue_ids,
            vec!["sase-1.1", "sase-1.2", "sase-1"]
        );

        let (_temp, beads_dir) = batch_remove_fixture();
        let descendant_first = remove_issues(
            &beads_dir,
            &["sase-1.2".to_string(), "sase-1".to_string()],
        )
        .unwrap();
        assert_eq!(
            descendant_first.issue_ids,
            vec!["sase-1.2", "sase-1.1", "sase-1"]
        );
    }

    #[test]
    fn remove_issues_deduplicates_duplicate_requests_and_events() {
        let (_temp, beads_dir) = batch_remove_fixture();

        let result = remove_issues(
            &beads_dir,
            &["sase-1".to_string(), "sase-1".to_string()],
        )
        .unwrap();

        assert_eq!(result.issue_ids, vec!["sase-1.1", "sase-1.2", "sase-1"]);
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let removal_events = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueRemoved
            })
            .count();
        assert_eq!(removal_events, 1);
    }

    #[test]
    fn remove_issues_missing_later_id_leaves_store_unchanged() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let first = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let second = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Second".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        add_dependency(
            &beads_dir,
            &second.id,
            &first.id,
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        let projection_before =
            fs::read(beads_dir.join("issues.jsonl")).unwrap();
        let config_before = fs::read(beads_dir.join("config.json")).unwrap();
        let (_, streams_before) = read_event_store(&beads_dir).unwrap();

        let error = remove_issues(
            &beads_dir,
            &[first.id.clone(), "sase-missing".to_string()],
        )
        .unwrap_err();

        assert_eq!(error.kind, "not_found");
        assert_eq!(error.message, "Issue not found: sase-missing");
        assert_eq!(
            fs::read(beads_dir.join("issues.jsonl")).unwrap(),
            projection_before
        );
        assert_eq!(
            fs::read(beads_dir.join("config.json")).unwrap(),
            config_before
        );
        let (_, streams_after) = read_event_store(&beads_dir).unwrap();
        assert_eq!(streams_after, streams_before);
        let reloaded = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            reloaded
                .get_issue(&second.id)
                .unwrap()
                .dependencies
                .iter()
                .map(|dependency| dependency.depends_on_id.as_str())
                .collect::<Vec<_>>(),
            vec![first.id.as_str()]
        );
    }

    #[test]
    fn remove_issues_rejects_an_empty_request() {
        let (_temp, beads_dir) = batch_remove_fixture();

        let error = remove_issues(&beads_dir, &[]).unwrap_err();

        assert_eq!(error.kind, "validation");
        assert_eq!(
            error.message,
            "remove_issues() requires at least one issue ID"
        );
    }

    #[test]
    fn removing_child_epic_does_not_close_parent_phase() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Root epic",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "Delegated phase",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.1.1",
                    "Child epic",
                    "plan",
                    Some("sase-1.1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        remove_issue(&beads_dir, "sase-1.1.1").unwrap();

        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue("sase-1.1").unwrap().status,
            StatusWire::Open
        );
        assert!(store.get_issue("sase-1.1.1").is_err());
    }

    #[test]
    fn close_skips_already_closed_issues_without_new_events() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            issue(
                "sase-1",
                "Already done",
                "plan",
                None,
                "open",
                "2026-01-01T00:00:00Z",
            ) + "\n",
        )
        .unwrap();
        close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            None,
            None,
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let event_count_before = streams
            .iter()
            .flat_map(|stream| stream.events.iter())
            .count();
        let before = persisted_claim_state(&beads_dir);

        let result = close_issues(
            &beads_dir,
            &["sase-1".to_string()],
            None,
            None,
            false,
            Some("2026-01-03T00:00:00Z".to_string()),
        )
        .unwrap();

        assert!(!result.changed);
        assert!(result.issue_ids.is_empty());
        assert_eq!(result.issues.len(), 1);
        assert_eq!(result.issues[0].id, "sase-1");
        assert_eq!(result.issues[0].status, StatusWire::Closed);
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let event_count_after = streams
            .iter()
            .flat_map(|stream| stream.events.iter())
            .count();
        assert_eq!(event_count_after, event_count_before);
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn mark_ready_rejects_phase_and_idempotent_plan() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Plan",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "A",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();

        assert_eq!(
            mark_ready_to_work(&beads_dir, "sase-1.1", None)
                .unwrap_err()
                .kind,
            "not_a_plan"
        );
        mark_ready_to_work(&beads_dir, "sase-1", None).unwrap();
        assert_eq!(
            mark_ready_to_work(&beads_dir, "sase-1", None)
                .unwrap_err()
                .kind,
            "already_ready"
        );
    }

    #[test]
    fn create_and_update_model() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let created = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                model: " codex/gpt-5.5 ".to_string(),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(created.model, "codex/gpt-5.5");

        let updated = update_issue(
            &beads_dir,
            &created.id,
            BeadUpdateFieldsWire {
                model: Some("#pro".to_string()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(updated.model, "#pro");
    }

    #[test]
    fn create_rejects_model_control_characters() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let err = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                model: "codex/gpt-5.5\n%tag:bad".to_string(),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap_err();

        assert!(err.message.contains("model cannot contain"));
    }

    #[test]
    fn append_issue_note_appends_attributed_entries_and_event() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Notes".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        let first = append_issue_note(
            &beads_dir,
            &issue.id,
            " first note ",
            Some("agent-1".to_string()),
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap();
        let second = append_issue_note(
            &beads_dir,
            &issue.id,
            "second note",
            Some("agent-1".to_string()),
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();

        assert_eq!(first.operation, "note");
        assert_eq!(first.issue_ids, vec![issue.id.clone()]);
        assert_eq!(
            second.notes,
            "[2026-01-01T00:01:00Z · agent-1] first note\n\n[2026-01-01T00:02:00Z · agent-1] second note"
        );
        assert_eq!(second.updated_at, "2026-01-01T00:02:00Z");

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let note_events: Vec<_> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.issue_id == issue.id
                    && event.operation == BeadEventOperationWire::NoteAppended
            })
            .collect();
        assert_eq!(note_events.len(), 2);
        assert_eq!(note_events[0].actor, "agent-1");
        assert!(matches!(
            &note_events[1].payload,
            BeadEventPayloadWire::NoteAppended { entry }
                if entry == "second note"
        ));

        let reduced = reduce_event_streams(&streams).unwrap();
        let reduced_issue =
            reduced.iter().find(|issue| issue.id == second.id).unwrap();
        assert_eq!(reduced_issue.notes, second.notes);
    }

    #[test]
    fn append_issue_note_defaults_blank_author_to_store_owner() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Notes".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        let noted = append_issue_note(
            &beads_dir,
            &issue.id,
            "owner note",
            Some("  ".to_string()),
            Some("2026-01-01T00:01:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();

        assert_eq!(
            noted.notes,
            "[2026-01-01T00:01:00Z · owner@example.com] owner note"
        );
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let note_event = streams[0].events.last().unwrap();
        assert_eq!(note_event.actor, "owner@example.com");
    }

    #[test]
    fn append_issue_note_rejects_blank_entry_without_writing() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Notes".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let before = persisted_claim_state(&beads_dir);

        let error =
            append_issue_note(&beads_dir, &issue.id, " \t ", None, None)
                .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert_eq!(error.message, "note entry cannot be empty or blank");
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn close_with_note_appends_to_every_requested_issue_before_close() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let first = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First".to_string(),
                issue_type: IssueTypeWire::Plan,
                notes: "Existing context".to_string(),
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let second = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Second".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        let result = close_issues_with_note(
            &beads_dir,
            &[first.id.clone(), second.id.clone()],
            None,
            None,
            false,
            Some(" verified with cargo test ".to_string()),
            Some("agent-1".to_string()),
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        assert!(result.changed);
        assert_eq!(result.issue_ids, vec![first.id.clone(), second.id.clone()]);
        assert_eq!(result.issues.len(), 2);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue(&first.id).unwrap().notes,
            "Existing context\n\n[2026-01-01T00:02:00Z · agent-1] verified with cargo test"
        );
        assert_eq!(
            store.get_issue(&second.id).unwrap().notes,
            "[2026-01-01T00:02:00Z · agent-1] verified with cargo test"
        );
        for issue_id in [&first.id, &second.id] {
            let issue = store.get_issue(issue_id).unwrap();
            assert_eq!(issue.status, StatusWire::Closed);
            assert_eq!(issue.updated_at, "2026-01-01T00:02:00Z");
            let stream = store
                .streams
                .iter()
                .find(|stream| stream.root_issue_id == *issue_id)
                .unwrap();
            assert_eq!(
                stream
                    .events
                    .iter()
                    .rev()
                    .take(2)
                    .map(|event| event.operation)
                    .collect::<Vec<_>>(),
                vec![
                    BeadEventOperationWire::IssueClosed,
                    BeadEventOperationWire::NoteAppended,
                ]
            );
            assert_eq!(stream.events[stream.events.len() - 2].actor, "agent-1");
        }
    }

    #[test]
    fn close_with_note_rejects_blank_entry_without_writing() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        let before = persisted_claim_state(&beads_dir);

        let error = close_issues_with_note(
            &beads_dir,
            &[phase_id],
            None,
            None,
            false,
            Some(" \t ".to_string()),
            None,
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert_eq!(error.message, "note entry cannot be empty or blank");
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn claim_for_agent_launch_claims_open_and_reassigns_in_progress_issue() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        let claimed_epic = claim_for_agent_launch(
            &beads_dir,
            &epic.id,
            "land-agent",
            Some("2026-01-01T00:01:30Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(claimed_epic.issue_type, IssueTypeWire::Plan);
        assert_eq!(claimed_epic.status, StatusWire::InProgress);
        assert_eq!(claimed_epic.assignee, "land-agent");

        let first = claim_for_agent_launch(
            &beads_dir,
            &phase.id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        let first_issue = first.issue.unwrap();
        assert_eq!(first.operation, "claim_for_agent_launch");
        assert!(first.changed);
        assert_eq!(first.issue_ids, vec![phase.id.clone()]);
        assert_eq!(first_issue.status, StatusWire::InProgress);
        assert_eq!(first_issue.assignee, "agent-1");
        assert_eq!(first_issue.updated_at, "2026-01-01T00:02:00Z");

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let claim_event = streams[0].events.last().unwrap();
        assert_eq!(claim_event.operation, BeadEventOperationWire::IssueUpdated);
        assert!(matches!(
            &claim_event.payload,
            BeadEventPayloadWire::IssueUpdated { fields }
                if fields.status == Some(StatusWire::InProgress)
                    && fields.assignee.as_deref() == Some("agent-1")
        ));
        let reduced = reduce_event_streams(&streams).unwrap();
        let reduced_phase =
            reduced.iter().find(|issue| issue.id == phase.id).unwrap();
        assert_eq!(reduced_phase.assignee, "agent-1");

        let before_repeated = persisted_claim_state(&beads_dir);
        let repeated = claim_for_agent_launch(
            &beads_dir,
            &phase.id,
            "agent-1",
            Some("2026-01-01T00:02:30Z".to_string()),
        )
        .unwrap();
        assert!(!repeated.changed);
        assert!(repeated.message.is_empty());
        assert_eq!(repeated.issue.unwrap().updated_at, "2026-01-01T00:02:00Z");
        assert_eq!(persisted_claim_state(&beads_dir), before_repeated);

        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let reassigned = claim_for_agent_launch(
            &beads_dir,
            &phase.id,
            "agent-2",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(reassigned.status, StatusWire::InProgress);
        assert_eq!(reassigned.assignee, "agent-2");
        assert_eq!(reassigned.updated_at, "2026-01-01T00:03:00Z");
        let projection =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(projection.contains(r#""assignee":"agent-2""#));
        assert!(projection.contains(r#""updated_at":"2026-01-01T00:03:00Z""#));
    }

    #[test]
    fn claim_for_agent_launch_rejects_missing_closed_and_blank_requests() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            issue(
                "sase-1",
                "Closed plan",
                "plan",
                None,
                "closed",
                "2026-01-01T00:00:00Z",
            ) + "\n",
        )
        .unwrap();

        let missing =
            claim_for_agent_launch(&beads_dir, "sase-missing", "agent", None)
                .unwrap_err();
        assert_eq!(missing.kind, "not_found");
        assert!(missing.message.contains("sase-missing"));

        let closed =
            claim_for_agent_launch(&beads_dir, "sase-1", "agent", None)
                .unwrap_err();
        assert_eq!(closed.kind, "closed");
        assert!(closed.message.contains("closed bead"));

        for agent_name in ["", "  \t"] {
            let invalid =
                claim_for_agent_launch(&beads_dir, "sase-1", agent_name, None)
                    .unwrap_err();
            assert_eq!(invalid.kind, "validation");
            assert!(invalid.message.contains("cannot be empty or blank"));
        }
    }

    #[test]
    fn claim_for_agent_wait_claims_open_and_is_idempotent_for_same_agent() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();

        let first = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();
        let issue = first.issue.unwrap();
        assert_eq!(first.operation, "claim_for_agent_wait");
        assert!(first.changed);
        assert_eq!(first.issue_ids, vec![phase_id.clone()]);
        assert_eq!(issue.status, StatusWire::Claimed);
        assert_eq!(issue.assignee, "agent-1");
        assert_eq!(issue.updated_at, "2026-01-01T00:02:00Z");

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let claim_event = streams[0].events.last().unwrap();
        assert_eq!(claim_event.operation, BeadEventOperationWire::IssueUpdated);
        assert!(matches!(
            &claim_event.payload,
            BeadEventPayloadWire::IssueUpdated { fields }
                if fields.status == Some(StatusWire::Claimed)
                    && fields.assignee.as_deref() == Some("agent-1")
        ));

        let before = persisted_claim_state(&beads_dir);
        let repeated = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(!repeated.changed);
        assert!(repeated.message.is_empty());
        assert_eq!(repeated.issue.unwrap().updated_at, "2026-01-01T00:02:00Z");
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn claim_for_agent_wait_declines_other_claims_and_terminal_states_without_writes(
    ) {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        let before_other_claim = persisted_claim_state(&beads_dir);
        let other_claim = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-2",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(!other_claim.changed);
        assert!(other_claim.message.contains("status is claimed"));
        assert!(other_claim.message.contains("holder is agent-1"));
        assert_eq!(persisted_claim_state(&beads_dir), before_other_claim);

        claim_for_agent_launch(
            &beads_dir,
            &phase_id,
            "agent-2",
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        let before_in_progress = persisted_claim_state(&beads_dir);
        let retained = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-2",
            Some("2026-01-01T00:04:30Z".to_string()),
        )
        .unwrap();
        assert!(!retained.changed);
        assert!(retained.message.is_empty());
        assert_eq!(retained.issue.unwrap().updated_at, "2026-01-01T00:04:00Z");
        assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

        let in_progress = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-3",
            Some("2026-01-01T00:05:00Z".to_string()),
        )
        .unwrap();
        assert!(!in_progress.changed);
        assert!(in_progress.message.contains("status is in_progress"));
        assert!(in_progress.message.contains("holder is agent-2"));
        assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

        close_issues(
            &beads_dir,
            std::slice::from_ref(&phase_id),
            None,
            None,
            false,
            Some("2026-01-01T00:06:00Z".to_string()),
        )
        .unwrap();
        let before_closed = persisted_claim_state(&beads_dir);
        let closed = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-3",
            Some("2026-01-01T00:07:00Z".to_string()),
        )
        .unwrap();
        assert!(!closed.changed);
        assert!(closed.message.contains("status is closed"));
        assert!(closed.message.contains("holder is agent-2"));
        assert_eq!(persisted_claim_state(&beads_dir), before_closed);
    }

    #[test]
    fn release_agent_claim_is_owner_guarded_and_round_trips_to_open() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        let before_wrong_agent = persisted_claim_state(&beads_dir);
        let wrong_agent = release_agent_claim(
            &beads_dir,
            &phase_id,
            "agent-2",
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        assert!(!wrong_agent.changed);
        assert_eq!(persisted_claim_state(&beads_dir), before_wrong_agent);

        let released = release_agent_claim(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        let released_issue = released.issue.unwrap();
        assert_eq!(released.operation, "release_agent_claim");
        assert!(released.changed);
        assert_eq!(released_issue.status, StatusWire::Open);
        assert!(released_issue.assignee.is_empty());
        assert_eq!(released_issue.updated_at, "2026-01-01T00:04:00Z");

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let release_event = streams[0].events.last().unwrap();
        assert!(matches!(
            &release_event.payload,
            BeadEventPayloadWire::IssueUpdated { fields }
                if fields.status == Some(StatusWire::Open)
                    && fields.assignee.as_deref() == Some("")
        ));

        let before_open_release = persisted_claim_state(&beads_dir);
        assert!(
            !release_agent_claim(
                &beads_dir,
                &phase_id,
                "agent-1",
                Some("2026-01-01T00:05:00Z".to_string()),
            )
            .unwrap()
            .changed
        );
        assert_eq!(persisted_claim_state(&beads_dir), before_open_release);

        let reclaimed = claim_for_agent_wait(
            &beads_dir,
            &phase_id,
            "agent-2",
            Some("2026-01-01T00:06:00Z".to_string()),
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(reclaimed.status, StatusWire::Claimed);
        assert_eq!(reclaimed.assignee, "agent-2");
    }

    #[test]
    fn release_agent_claim_declines_in_progress_and_closed_without_writes() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        claim_for_agent_launch(
            &beads_dir,
            &phase_id,
            "agent-1",
            Some("2026-01-01T00:02:00Z".to_string()),
        )
        .unwrap();

        let before_in_progress = persisted_claim_state(&beads_dir);
        assert!(
            !release_agent_claim(
                &beads_dir,
                &phase_id,
                "agent-1",
                Some("2026-01-01T00:03:00Z".to_string()),
            )
            .unwrap()
            .changed
        );
        assert_eq!(persisted_claim_state(&beads_dir), before_in_progress);

        close_issues(
            &beads_dir,
            std::slice::from_ref(&phase_id),
            None,
            None,
            false,
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();
        let before_closed = persisted_claim_state(&beads_dir);
        assert!(
            !release_agent_claim(
                &beads_dir,
                &phase_id,
                "agent-1",
                Some("2026-01-01T00:05:00Z".to_string()),
            )
            .unwrap()
            .changed
        );
        assert_eq!(persisted_claim_state(&beads_dir), before_closed);
    }

    #[test]
    fn agent_claim_mutations_reject_missing_and_blank_requests() {
        let (_temp, beads_dir, _phase_id) = claim_mutation_fixture();

        for mutation in [
            claim_for_agent_wait
                as fn(
                    &Path,
                    &str,
                    &str,
                    Option<String>,
                )
                    -> Result<BeadMutationOutcomeWire, BeadError>,
            release_agent_claim,
        ] {
            let missing = mutation(&beads_dir, "sase-missing", "agent", None)
                .unwrap_err();
            assert_eq!(missing.kind, "not_found");
            assert!(missing.message.contains("sase-missing"));

            for agent_name in ["", "  \t"] {
                let invalid =
                    mutation(&beads_dir, "sase-missing", agent_name, None)
                        .unwrap_err();
                assert_eq!(invalid.kind, "validation");
                assert!(invalid.message.contains("cannot be empty or blank"));
            }
        }
    }

    #[test]
    fn concurrent_update_and_claim_preserve_both_events_and_projection() {
        let (_temp, beads_dir, phase_id) = claim_mutation_fixture();
        let lock_path = bead_mutation_lock_path(&beads_dir);
        let holder = lock_bead_mutation_with_timeout(
            &beads_dir,
            &lock_path,
            Duration::from_secs(1),
            "test_holder",
        )
        .unwrap();

        let update_beads_dir = beads_dir.clone();
        let update_phase_id = phase_id.clone();
        let (update_tx, update_rx) = mpsc::channel();
        let update_handle = thread::spawn(move || {
            let result = update_issue(
                &update_beads_dir,
                &update_phase_id,
                BeadUpdateFieldsWire {
                    title: Some("Updated concurrently".to_string()),
                    now: Some("2026-01-01T00:02:00Z".to_string()),
                    ..Default::default()
                },
            );
            update_tx.send(()).unwrap();
            result
        });
        assert!(matches!(
            update_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        let claim_beads_dir = beads_dir.clone();
        let claim_phase_id = phase_id.clone();
        let (claim_tx, claim_rx) = mpsc::channel();
        let claim_handle = thread::spawn(move || {
            let result = claim_for_agent_launch(
                &claim_beads_dir,
                &claim_phase_id,
                "agent-1",
                Some("2026-01-01T00:03:00Z".to_string()),
            );
            claim_tx.send(()).unwrap();
            result
        });
        assert!(matches!(
            claim_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        holder.release().unwrap();
        let update_outcome = update_handle.join().unwrap().unwrap();
        let claim_outcome = claim_handle.join().unwrap().unwrap();
        assert!(update_outcome.lock_wait_ms > 0);
        assert!(claim_outcome.lock_wait_ms > 0);

        let projected =
            import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))
                .unwrap()
                .issues;
        let phase =
            projected.iter().find(|issue| issue.id == phase_id).unwrap();
        assert_eq!(phase.title, "Updated concurrently");
        assert_eq!(phase.status, StatusWire::InProgress);
        assert_eq!(phase.assignee, "agent-1");

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let update_events: Vec<_> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.issue_id == phase_id
                    && event.operation == BeadEventOperationWire::IssueUpdated
            })
            .collect();
        assert_eq!(update_events.len(), 2);
        assert!(update_events.iter().any(|event| matches!(
            &event.payload,
            BeadEventPayloadWire::IssueUpdated { fields }
                if fields.title.as_deref() == Some("Updated concurrently")
        )));
        assert!(update_events.iter().any(|event| matches!(
            &event.payload,
            BeadEventPayloadWire::IssueUpdated { fields }
                if fields.status == Some(StatusWire::InProgress)
                    && fields.assignee.as_deref() == Some("agent-1")
        )));
    }

    #[test]
    fn concurrent_launch_claims_preserve_sibling_events_and_projection() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phases: Vec<IssueWire> = ["First", "Second"]
            .into_iter()
            .map(|title| {
                create_issue(
                    &beads_dir,
                    BeadCreateRequestWire {
                        title: title.to_string(),
                        issue_type: IssueTypeWire::Phase,
                        parent_id: Some(epic.id.clone()),
                        ..Default::default()
                    },
                )
                .unwrap()
                .issue
                .unwrap()
            })
            .collect();

        let barrier = Arc::new(Barrier::new(3));
        let handles: Vec<_> = phases
            .iter()
            .enumerate()
            .map(|(index, phase)| {
                let beads_dir = beads_dir.clone();
                let bead_id = phase.id.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    claim_for_agent_launch(
                        &beads_dir,
                        &bead_id,
                        &format!("agent-{}", index + 1),
                        Some(format!("2026-01-01T00:0{}:00Z", index + 1)),
                    )
                })
            })
            .collect();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap().unwrap();
        }

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let claim_events: Vec<_> = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .filter(|event| {
                event.operation == BeadEventOperationWire::IssueUpdated
                    && phases.iter().any(|phase| phase.id == event.issue_id)
            })
            .collect();
        assert_eq!(claim_events.len(), 2);
        let projected =
            import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))
                .unwrap()
                .issues;
        for (index, phase) in phases.iter().enumerate() {
            let issue =
                projected.iter().find(|issue| issue.id == phase.id).unwrap();
            assert_eq!(issue.status, StatusWire::InProgress);
            assert_eq!(issue.assignee, format!("agent-{}", index + 1));
        }
    }

    #[test]
    fn bead_mutation_lock_contention_times_out() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        let lock_path = bead_mutation_lock_path(&beads_dir);
        let holder = lock_bead_mutation_with_timeout(
            &beads_dir,
            &lock_path,
            Duration::from_secs(1),
            "test_holder",
        )
        .unwrap();

        let started = Instant::now();
        let error = lock_bead_mutation_with_timeout(
            &beads_dir,
            &lock_path,
            Duration::from_millis(50),
            "test_contender",
        )
        .unwrap_err();

        assert_eq!(error.kind, "lock_timeout");
        assert!(error.message.contains("timed out"));
        assert!(error
            .message
            .contains(&format!("pid={}", std::process::id())));
        assert!(error.message.contains("operation=test_holder"));
        assert!(started.elapsed() < Duration::from_secs(1));
        holder.release().unwrap();
        assert!(!bead_mutation_holder_path(&beads_dir).exists());
    }

    #[test]
    fn preclaim_epic_work_plan_updates_once_and_returns_rollback() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let p1 = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "P1".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                assignee: "previous".to_string(),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        update_issue(
            &beads_dir,
            &p1.id,
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                assignee: Some("previous".to_string()),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        let p2 = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "P2".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:03:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        let outcome = preclaim_epic_work_plan(
            &beads_dir,
            &epic.id,
            &[
                BeadPreclaimAssignmentWire {
                    bead_id: p1.id.clone(),
                    agent_name: "agent-1".to_string(),
                },
                BeadPreclaimAssignmentWire {
                    bead_id: p2.id.clone(),
                    agent_name: "agent-2".to_string(),
                },
            ],
            Some("land-agent".to_string()),
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(outcome.operation, "preclaim_epic_work");
        assert_eq!(
            outcome.issue_ids,
            vec![p1.id.clone(), p2.id.clone(), epic.id.clone()]
        );
        assert_eq!(
            outcome.rollback_preclaims,
            vec![
                BeadPreclaimRollbackWire {
                    bead_id: p1.id.clone(),
                    status: StatusWire::InProgress,
                    assignee: "previous".to_string(),
                },
                BeadPreclaimRollbackWire {
                    bead_id: p2.id.clone(),
                    status: StatusWire::Open,
                    assignee: String::new(),
                },
                BeadPreclaimRollbackWire {
                    bead_id: epic.id.clone(),
                    status: StatusWire::Open,
                    assignee: String::new(),
                },
            ]
        );

        let store = MutableStore::load(&beads_dir).unwrap();
        let updated_epic = store.get_issue(&epic.id).unwrap();
        assert_eq!(updated_epic.status, StatusWire::InProgress);
        assert_eq!(updated_epic.assignee, "land-agent");
        assert_eq!(updated_epic.updated_at, "2026-01-01T00:04:00Z");
        let updated_p1 = store.get_issue(&p1.id).unwrap();
        assert_eq!(updated_p1.status, StatusWire::InProgress);
        assert_eq!(updated_p1.assignee, "agent-1");
        assert_eq!(updated_p1.updated_at, "2026-01-01T00:04:00Z");
        let updated_p2 = store.get_issue(&p2.id).unwrap();
        assert_eq!(updated_p2.status, StatusWire::InProgress);
        assert_eq!(updated_p2.assignee, "agent-2");
    }

    #[test]
    fn preclaim_epic_work_plan_validation_is_all_or_nothing() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let p1 = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "P1".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let p2 = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "P2".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        close_issues(
            &beads_dir,
            std::slice::from_ref(&p2.id),
            Some("done".to_string()),
            None,
            false,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();

        let err = preclaim_epic_work_plan(
            &beads_dir,
            &epic.id,
            &[
                BeadPreclaimAssignmentWire {
                    bead_id: p1.id.clone(),
                    agent_name: "agent-1".to_string(),
                },
                BeadPreclaimAssignmentWire {
                    bead_id: p2.id.clone(),
                    agent_name: "agent-2".to_string(),
                },
            ],
            None,
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap_err();

        assert!(err.message.contains("preclaim target is closed"));
        let store = MutableStore::load(&beads_dir).unwrap();
        let unchanged_p1 = store.get_issue(&p1.id).unwrap();
        assert_eq!(unchanged_p1.status, StatusWire::Open);
        assert_eq!(unchanged_p1.assignee, "");
        assert_eq!(store.get_issue(&p2.id).unwrap().status, StatusWire::Closed);
    }

    #[test]
    fn mutations_create_canonical_events_and_regenerate_projection() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let child = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Child".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        update_issue(
            &beads_dir,
            &child.id,
            BeadUpdateFieldsWire {
                status: Some("in_progress".to_string()),
                assignee: Some("agent".to_string()),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(
            streams[0]
                .events
                .iter()
                .map(|event| event.operation)
                .collect::<Vec<_>>(),
            vec![
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::IssueCreated,
                BeadEventOperationWire::IssueUpdated,
            ]
        );

        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        export_jsonl(&beads_dir).unwrap();
        let regenerated =
            fs::read_to_string(beads_dir.join("issues.jsonl")).unwrap();
        assert!(regenerated.contains(r#""id":"sase-1""#));
        assert!(regenerated.contains(r#""id":"sase-1.1""#));
        assert!(regenerated.contains(r#""assignee":"agent""#));
    }

    #[test]
    fn projection_writers_are_byte_stable_for_the_same_store_state() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let first_epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Second epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(first_epic.id),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap();

        let mutation_projection =
            fs::read(beads_dir.join("issues.jsonl")).unwrap();
        let manifest_before =
            fs::read(beads_dir.join("events/manifest.json")).unwrap();
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let reduced = reduce_event_streams(&streams).unwrap();

        // The conflict/rebuild binding returns reducer rows for its caller to
        // serialize directly, so reducer order is itself a writer contract.
        let reduced_projection = reduced
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("\n")
            + "\n";
        assert_eq!(reduced_projection.as_bytes(), mutation_projection);

        let direct_export =
            crate::bead::jsonl::export_issues_to_jsonl(&reduced).unwrap();
        assert_eq!(direct_export.as_bytes(), mutation_projection);

        fs::write(beads_dir.join("issues.jsonl"), "stale projection\n")
            .unwrap();
        export_jsonl(&beads_dir).unwrap();
        assert_eq!(
            fs::read(beads_dir.join("issues.jsonl")).unwrap(),
            mutation_projection
        );

        let mut reversed_streams = streams;
        reversed_streams.reverse();
        write_event_store(&beads_dir, &reversed_streams).unwrap();
        assert_eq!(
            fs::read(beads_dir.join("events/manifest.json")).unwrap(),
            manifest_before
        );
        assert_eq!(
            fs::read(beads_dir.join("issues.jsonl")).unwrap(),
            mutation_projection
        );
    }

    #[test]
    fn mutable_appends_mint_stable_content_hashed_event_ids() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let alpha_payload = BeadEventPayloadWire::IssueUpdated {
            fields: BeadIssueUpdateEventFieldsWire {
                title: Some("Alpha".to_string()),
                ..Default::default()
            },
        };
        let beta_payload = BeadEventPayloadWire::IssueUpdated {
            fields: BeadIssueUpdateEventFieldsWire {
                title: Some("Beta".to_string()),
                ..Default::default()
            },
        };
        let mut alpha = MutableStore::load(&beads_dir).unwrap();
        let mut duplicate = MutableStore::load(&beads_dir).unwrap();
        let mut beta = MutableStore::load(&beads_dir).unwrap();
        for (store, payload) in [
            (&mut alpha, alpha_payload.clone()),
            (&mut duplicate, alpha_payload),
            (&mut beta, beta_payload),
        ] {
            store
                .append_issue_event(
                    &epic.id,
                    BeadEventOperationWire::IssueUpdated,
                    payload,
                    "2026-01-01T00:01:00Z",
                    "owner@example.com",
                )
                .unwrap();
        }

        let alpha_id = &alpha.streams[0].events.last().unwrap().event_id;
        let duplicate_id =
            &duplicate.streams[0].events.last().unwrap().event_id;
        let beta_id = &beta.streams[0].events.last().unwrap().event_id;

        assert_eq!(alpha_id, duplicate_id);
        assert_ne!(alpha_id, beta_id);
        assert_eq!(
            alpha_id.rsplit_once(':').unwrap().0,
            beta_id.rsplit_once(':').unwrap().0
        );
        assert!(alpha_id.rsplit(':').next().is_some_and(|digest| {
            digest.len() == 64
                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        }));
    }

    #[test]
    fn event_backed_child_id_reuse_after_remove_matches_jsonl_semantics() {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let first = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First child".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        remove_issue(&beads_dir, &first.id).unwrap();
        let second = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Replacement child".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id.clone()),
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        assert_eq!(second.id, first.id);
        let store = MutableStore::load(&beads_dir).unwrap();
        assert_eq!(
            store.get_issue(&second.id).unwrap().title,
            "Replacement child"
        );
    }

    #[test]
    fn remove_dependencies_records_the_full_removed_edge() {
        let (_temp, beads_dir, source_id, target_ids) =
            dependency_mutation_fixture();

        let result = remove_dependencies(
            &beads_dir,
            &source_id,
            &[target_ids[0].clone()],
            Some("2026-01-01T00:10:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(result.operation, "dep_rm");
        assert_eq!(
            result.issue_ids,
            vec![source_id.clone(), target_ids[0].clone()]
        );
        assert_eq!(result.dependencies.len(), 1);
        assert_eq!(result.dependencies[0].issue_id, source_id);
        assert_eq!(result.dependencies[0].depends_on_id, target_ids[0]);
        assert_eq!(result.dependencies[0].created_at, "2026-01-01T00:03:00Z");
        assert_eq!(result.dependencies[0].created_by, "owner@example.com");
        assert_eq!(
            result
                .issue
                .unwrap()
                .dependencies
                .iter()
                .map(|dependency| dependency.depends_on_id.as_str())
                .collect::<Vec<_>>(),
            vec![target_ids[1].as_str()]
        );

        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        let event = streams
            .iter()
            .flat_map(|stream| &stream.events)
            .find(|event| {
                event.operation == BeadEventOperationWire::DependencyRemoved
            })
            .unwrap();
        assert_eq!(event.timestamp, "2026-01-01T00:10:00Z");
        assert_eq!(event.actor, "owner@example.com");
        assert!(matches!(
            &event.payload,
            BeadEventPayloadWire::DependencyRemoved { dependency }
                if dependency.depends_on_id == target_ids[0]
                    && dependency.created_at == "2026-01-01T00:03:00Z"
        ));
    }

    #[test]
    fn remove_dependencies_batches_and_deduplicates_targets() {
        let (_temp, beads_dir, source_id, target_ids) =
            dependency_mutation_fixture();

        let result = remove_dependencies(
            &beads_dir,
            &source_id,
            &[
                target_ids[0].clone(),
                target_ids[1].clone(),
                target_ids[0].clone(),
            ],
            Some("2026-01-01T00:10:00Z".to_string()),
        )
        .unwrap();

        assert_eq!(
            result.issue_ids,
            vec![
                source_id.clone(),
                target_ids[0].clone(),
                target_ids[1].clone()
            ]
        );
        assert_eq!(result.dependencies.len(), 2);
        assert!(MutableStore::load(&beads_dir)
            .unwrap()
            .get_issue(&source_id)
            .unwrap()
            .dependencies
            .is_empty());
        let (_manifest, streams) = read_event_store(&beads_dir).unwrap();
        assert_eq!(
            streams
                .iter()
                .flat_map(|stream| &stream.events)
                .filter(|event| {
                    event.operation == BeadEventOperationWire::DependencyRemoved
                })
                .count(),
            2
        );
    }

    #[test]
    fn remove_dependencies_validates_the_whole_batch_before_writing() {
        let (_temp, beads_dir, source_id, target_ids) =
            dependency_mutation_fixture();
        let before = persisted_claim_state(&beads_dir);

        let error = remove_dependencies(
            &beads_dir,
            &source_id,
            &[target_ids[0].clone(), "sase-missing-edge".to_string()],
            Some("2026-01-01T00:10:00Z".to_string()),
        )
        .unwrap_err();

        assert_eq!(error.kind, "validation");
        assert_eq!(
            error.message,
            format!(
                "Dependency does not exist: {source_id} does not depend on sase-missing-edge"
            )
        );
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    #[test]
    fn remove_dependencies_rejects_an_unknown_source_without_writing() {
        let (_temp, beads_dir, _source_id, target_ids) =
            dependency_mutation_fixture();
        let before = persisted_claim_state(&beads_dir);

        let error = remove_dependencies(
            &beads_dir,
            "sase-missing",
            &[target_ids[0].clone()],
            None,
        )
        .unwrap_err();

        assert_eq!(error.kind, "not_found");
        assert_eq!(error.message, "Issue not found: sase-missing");
        assert_eq!(persisted_claim_state(&beads_dir), before);
    }

    fn dependency_mutation_fixture(
    ) -> (tempfile::TempDir, PathBuf, String, Vec<String>) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let source = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Source".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let first = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "First target".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let second = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Second target".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:02:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        add_dependency(
            &beads_dir,
            &source.id,
            &first.id,
            Some("2026-01-01T00:03:00Z".to_string()),
        )
        .unwrap();
        add_dependency(
            &beads_dir,
            &source.id,
            &second.id,
            Some("2026-01-01T00:04:00Z".to_string()),
        )
        .unwrap();

        (temp, beads_dir, source.id, vec![first.id, second.id])
    }

    fn issue(
        id: &str,
        title: &str,
        issue_type: &str,
        parent_id: Option<&str>,
        status: &str,
        timestamp: &str,
    ) -> String {
        let parent = parent_id.map_or_else(
            || "null".to_string(),
            |value| format!(r#""{value}""#),
        );
        format!(
            r#"{{"id":"{id}","title":"{title}","status":"{status}","issue_type":"{issue_type}","parent_id":{parent},"owner":"","assignee":"","created_at":"{timestamp}","created_by":"","updated_at":"{timestamp}","closed_at":null,"close_reason":null,"description":"","notes":"","design":"","is_ready_to_work":false,"changespec_name":"","changespec_bug_id":"","dependencies":[]}}"#
        )
    }

    fn closed_issue_fixture(
        resolution: BeadResolutionWire,
        reason: Option<&str>,
    ) -> (tempfile::TempDir, PathBuf, String) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();
        let issue_id = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Closed issue".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap()
        .id;
        close_issues(
            &beads_dir,
            std::slice::from_ref(&issue_id),
            reason.map(str::to_string),
            Some(resolution),
            false,
            Some("2026-01-02T00:00:00Z".to_string()),
        )
        .unwrap();
        (temp, beads_dir, issue_id)
    }

    fn claim_mutation_fixture() -> (tempfile::TempDir, PathBuf, String) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "owner@example.com"))
            .unwrap();
        fs::write(beads_dir.join("issues.jsonl"), "").unwrap();

        let epic = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Epic".to_string(),
                issue_type: IssueTypeWire::Plan,
                now: Some("2026-01-01T00:00:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        let phase = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "Phase".to_string(),
                issue_type: IssueTypeWire::Phase,
                parent_id: Some(epic.id),
                now: Some("2026-01-01T00:01:00Z".to_string()),
                ..Default::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();

        (temp, beads_dir, phase.id)
    }

    fn persisted_claim_state(
        beads_dir: &Path,
    ) -> (Vec<u8>, Vec<(String, Vec<u8>)>) {
        let issues = fs::read(beads_dir.join("issues.jsonl")).unwrap();
        let streams_dir = beads_dir.join("events/streams");
        let mut streams: Vec<_> = fs::read_dir(streams_dir)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().to_string_lossy().into_owned(),
                    fs::read(entry.path()).unwrap(),
                )
            })
            .collect();
        streams.sort_by(|left, right| left.0.cmp(&right.0));
        (issues, streams)
    }

    fn batch_remove_fixture() -> (tempfile::TempDir, PathBuf) {
        let temp = tempdir().unwrap();
        let beads_dir = temp.path().join("sdd/beads");
        fs::create_dir_all(&beads_dir).unwrap();
        save_config(&beads_dir, &default_config("sase", "")).unwrap();
        fs::write(
            beads_dir.join("issues.jsonl"),
            [
                issue(
                    "sase-1",
                    "Plan",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:00:00Z",
                ),
                issue(
                    "sase-1.1",
                    "First child",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:01:00Z",
                ),
                issue(
                    "sase-1.2",
                    "Second child",
                    "phase",
                    Some("sase-1"),
                    "open",
                    "2026-01-01T00:02:00Z",
                ),
                issue(
                    "sase-2",
                    "Independent",
                    "plan",
                    None,
                    "open",
                    "2026-01-01T00:03:00Z",
                ),
            ]
            .join("\n")
                + "\n",
        )
        .unwrap();
        (temp, beads_dir)
    }
}

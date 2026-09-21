use super::mutation_wire::BeadMutationOutcomeWire;
use crate::artifact_link::canonicalize_artifact_link_ref;
use crate::artifact_link::BeadLinkDirectionWire;
use crate::artifact_ref::normalize_artifact_ref_list;
use crate::bead::config::default_config;
use crate::bead::config::load_config;
use crate::bead::config::save_config;
use crate::bead::config::BeadConfigWire;
use crate::bead::events::clear_snooze_record;
use crate::bead::events::import_issues_to_event_streams;
use crate::bead::events::mint_bead_event_id;
use crate::bead::events::reduce_event_streams;
use crate::bead::events::BeadEventOperationWire;
use crate::bead::events::BeadEventPayloadWire;
use crate::bead::events::BeadEventRecordWire;
use crate::bead::events::BeadEventStreamWire;
use crate::bead::events::BEAD_EVENT_SCHEMA_VERSION;
use crate::bead::jsonl::event_store_present;
use crate::bead::jsonl::import_issues_from_jsonl;
use crate::bead::jsonl::read_event_store;
use crate::bead::jsonl::write_event_store_changed;
use crate::bead::jsonl::write_issues_jsonl;
use crate::bead::wire::validate_model_value;
use crate::bead::wire::validate_unique_external_refs;
use crate::bead::wire::BeadError;
use crate::bead::wire::BeadResolutionWire;
use crate::bead::wire::BeadTierWire;
use crate::bead::wire::IssueTypeWire;
use crate::bead::wire::IssueWire;
use crate::bead::wire::StatusWire;
use crate::store_lock::acquire_store_lock;
use crate::store_lock::timeout_from_env;
use crate::store_lock::LockMode;
use crate::store_lock::StoreLockError;
use chrono::DateTime;
use chrono::SecondsFormat;
use chrono::Utc;
use std::collections::BTreeSet;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;
use std::time::SystemTime;

// Reuse the ignored compatibility database as the advisory lock file so a
// successful claim cannot introduce durable bead-store content of its own.
// Deleting and recreating beads.db while this lock is held would split
// contenders across different inodes, so store maintenance must preserve it.
pub(crate) const BEAD_MUTATION_LOCK_FILENAME: &str = "beads.db";
pub(crate) const BEAD_MUTATION_HOLDER_FILENAME: &str =
    ".bead-mutation-lock.holder";
pub(crate) const BEAD_MUTATION_LOCK_TIMEOUT_ENV: &str =
    "SASE_BEAD_MUTATION_LOCK_TIMEOUT";
pub(crate) const BEAD_MUTATION_LOCK_TIMEOUT_DEFAULT: Duration =
    Duration::from_secs(600);

pub(crate) fn link_mutation_error(
    error: crate::artifact_link::ArtifactLinkError,
) -> BeadError {
    BeadError {
        kind: error.kind,
        message: error.message,
    }
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

pub(crate) fn set_ready_to_work(
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

pub(crate) fn normalize_model(value: String) -> Result<String, BeadError> {
    let model = value.trim().to_string();
    validate_model_value(&model)?;
    Ok(model)
}

pub(crate) fn normalize_references<T: AsRef<str>>(
    references: &[T],
) -> Result<Vec<String>, BeadError> {
    normalize_artifact_ref_list(references).map_err(|error| BeadError {
        kind: error.kind,
        message: error.message,
    })
}

pub(crate) fn tier_label(tier: Option<&BeadTierWire>) -> &'static str {
    match tier {
        Some(BeadTierWire::Plan) => "plan",
        Some(BeadTierWire::Epic) => "epic",
        None => "missing tier",
    }
}

pub(crate) mod tracked_streams {
    use std::collections::BTreeSet;

    use super::{BeadError, BeadEventStreamWire};

    pub(crate) struct TrackedEventStreams {
        streams: Vec<BeadEventStreamWire>,
        changed: BTreeSet<String>,
    }

    impl TrackedEventStreams {
        pub(crate) fn loaded(streams: Vec<BeadEventStreamWire>) -> Self {
            Self {
                streams,
                changed: BTreeSet::new(),
            }
        }

        pub(crate) fn imported(streams: Vec<BeadEventStreamWire>) -> Self {
            let changed = streams
                .iter()
                .map(|stream| stream.stream_id.clone())
                .collect();
            Self { streams, changed }
        }

        pub(crate) fn all(&self) -> &[BeadEventStreamWire] {
            &self.streams
        }

        pub(crate) fn changed(&self) -> &BTreeSet<String> {
            &self.changed
        }

        pub(crate) fn stream_mut(
            &mut self,
            stream_id: &str,
        ) -> Result<&mut BeadEventStreamWire, BeadError> {
            self.changed.insert(stream_id.to_string());
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
    }
}

pub(crate) struct MutableStore {
    pub(crate) beads_dir: PathBuf,
    pub(crate) config: BeadConfigWire,
    pub(crate) issues: Vec<IssueWire>,
    pub(crate) streams: tracked_streams::TrackedEventStreams,
}

impl MutableStore {
    pub(crate) fn load(beads_dir: &Path) -> Result<Self, BeadError> {
        #[cfg(test)]
        store_io_stats::record_load();
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
            (
                issues,
                tracked_streams::TrackedEventStreams::loaded(streams),
            )
        } else {
            let issues =
                import_issues_from_jsonl(&beads_dir.join("issues.jsonl"))?
                    .issues;
            let streams = import_issues_to_event_streams(&issues)?;
            (
                issues,
                tracked_streams::TrackedEventStreams::imported(streams),
            )
        };
        validate_unique_external_refs(&issues)?;
        Ok(Self {
            beads_dir: beads_dir.to_path_buf(),
            config,
            issues,
            streams,
        })
    }

    pub(crate) fn save(&self) -> Result<(), BeadError> {
        #[cfg(test)]
        store_io_stats::record_save();
        // Nothing durable is written until the derived issue set is known to
        // be valid.  An event stream persisted ahead of the state it derives
        // is unrecoverable: every later load replays the same event and
        // re-derives the same invalid record, so the store never opens again.
        // Rejecting here leaves both files exactly as they were.
        for issue in &self.issues {
            issue.validate()?;
        }
        validate_unique_external_refs(&self.issues)?;
        write_event_store_changed(
            &self.beads_dir,
            self.streams.all(),
            self.streams.changed(),
        )?;
        save_config(&self.beads_dir, &self.config)?;
        self.save_issues()
    }

    pub(crate) fn save_issues(&self) -> Result<(), BeadError> {
        write_issues_jsonl(&self.beads_dir, &self.issues)
    }

    pub(crate) fn issue_index(
        &self,
        issue_id: &str,
    ) -> Result<usize, BeadError> {
        self.issues
            .iter()
            .position(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    pub(crate) fn get_issue(
        &self,
        issue_id: &str,
    ) -> Result<&IssueWire, BeadError> {
        self.issues
            .iter()
            .find(|issue| issue.id == issue_id)
            .ok_or_else(|| not_found(issue_id))
    }

    pub(crate) fn artifact_link_projection_receipt_seen_on(
        &self,
        issue_id: &str,
        operation_id: &str,
        operation: BeadEventOperationWire,
        target_ref: &str,
        relation: &str,
        direction: BeadLinkDirectionWire,
    ) -> Result<bool, BeadError> {
        let target_ref = canonicalize_artifact_link_ref(target_ref)
            .map_err(link_mutation_error)?;
        let stream_id = self.stream_id_for_issue(issue_id)?;
        let Some(stream) = self
            .streams
            .all()
            .iter()
            .find(|stream| stream.stream_id == stream_id)
        else {
            return Ok(false);
        };
        Ok(stream.events.iter().any(|event| match &event.payload {
            BeadEventPayloadWire::LinkAdded {
                operation_id: Some(existing),
                target_ref: existing_target,
                relation: existing_relation,
                direction: existing_direction,
                ..
            } if operation == BeadEventOperationWire::LinkAdded => {
                existing == operation_id
                    && existing_relation == relation
                    && *existing_direction == direction
                    && canonicalize_artifact_link_ref(existing_target)
                        .map(|canonical| canonical == target_ref)
                        .unwrap_or(false)
            }
            BeadEventPayloadWire::LinkRemoved {
                operation_id: Some(existing),
                target_ref: existing_target,
                relation: existing_relation,
                direction: existing_direction,
                ..
            } if operation == BeadEventOperationWire::LinkRemoved => {
                existing == operation_id
                    && existing_relation == relation
                    && *existing_direction == direction
                    && canonicalize_artifact_link_ref(existing_target)
                        .map(|canonical| canonical == target_ref)
                        .unwrap_or(false)
            }
            _ => false,
        }))
    }

    pub(crate) fn append_issue_event(
        &mut self,
        issue_id: &str,
        operation: BeadEventOperationWire,
        payload: BeadEventPayloadWire,
        timestamp: &str,
        actor: &str,
    ) -> Result<String, BeadError> {
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
        let event_id = event.event_id.clone();
        stream.events.push(event);
        Ok(event_id)
    }

    pub(crate) fn stream_for_mut(
        &mut self,
        stream_id: &str,
    ) -> Result<&mut BeadEventStreamWire, BeadError> {
        self.streams.stream_mut(stream_id)
    }

    pub(crate) fn stream_id_for_issue(
        &self,
        issue_id: &str,
    ) -> Result<String, BeadError> {
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

    pub(crate) fn close_one(
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
        clear_snooze_record(&mut self.issues[index]);
        self.issues[index].updated_at = closed_at.to_string();
        Ok(Some(self.issues[index].clone()))
    }
}

pub(crate) fn sorted_children<'a>(
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

pub(crate) fn sorted_descendants<'a>(
    issues: &'a [IssueWire],
    parent_id: &str,
) -> Vec<&'a IssueWire> {
    let mut descendants = Vec::new();
    let mut visited = BTreeSet::from([parent_id.to_string()]);
    collect_descendants(issues, parent_id, &mut visited, &mut descendants);
    descendants
}

pub(crate) fn collect_descendants<'a>(
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

pub(crate) fn next_top_level_counter(
    issue_prefix: &str,
    config_counter: u64,
    issues: &[IssueWire],
) -> u64 {
    std::cmp::max(
        config_counter,
        max_top_level_counter(issue_prefix, issues) + 1,
    )
}

pub(crate) fn next_child_id(parent_id: &str, issues: &[IssueWire]) -> String {
    let local_max = issues
        .iter()
        .filter_map(|issue| direct_child_counter(parent_id, &issue.id))
        .max()
        .unwrap_or(0);
    format!("{parent_id}.{}", local_max + 1)
}

pub(crate) fn max_top_level_counter(
    issue_prefix: &str,
    issues: &[IssueWire],
) -> u64 {
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

pub(crate) fn direct_child_counter(
    parent_id: &str,
    issue_id: &str,
) -> Option<u64> {
    let prefix = format!("{parent_id}.");
    let suffix = issue_id.strip_prefix(&prefix)?;
    if suffix.contains('.') {
        return None;
    }
    suffix.parse::<u64>().ok()
}

pub(crate) fn mutation_status_value(status: &StatusWire) -> &'static str {
    match status {
        StatusWire::Open => "open",
        StatusWire::Claimed => "claimed",
        StatusWire::Ready => "ready",
        StatusWire::Snoozed => "snoozed",
        StatusWire::InProgress => "in_progress",
        StatusWire::Closed => "closed",
    }
}

pub(crate) fn to_base36(mut n: u64) -> String {
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

pub(crate) fn from_base36(value: &str) -> Option<u64> {
    u64::from_str_radix(value, 36).ok()
}

pub(crate) fn now_utc() -> String {
    let now: DateTime<Utc> = SystemTime::now().into();
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(crate) fn find_git_root(path: &Path) -> Result<Option<PathBuf>, BeadError> {
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

pub(crate) fn not_found(issue_id: &str) -> BeadError {
    BeadError {
        kind: "not_found".to_string(),
        message: format!("Issue not found: {issue_id}"),
    }
}

pub(crate) fn with_bead_mutation_lock(
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

pub(crate) fn lock_bead_mutation_with_timeout(
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

pub(crate) fn bead_mutation_holder_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join(BEAD_MUTATION_HOLDER_FILENAME)
}

pub(crate) fn bead_mutation_lock_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join(BEAD_MUTATION_LOCK_FILENAME)
}

pub(crate) fn outcome(
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
        reopen_withheld: false,
        reopen_withheld_closed_at: None,
    }
}

#[cfg(test)]
pub(crate) mod store_io_stats {
    use std::cell::Cell;

    thread_local! {
        static LOADS: Cell<u64> = const { Cell::new(0) };
        static SAVES: Cell<u64> = const { Cell::new(0) };
    }

    pub fn reset() {
        LOADS.with(|cell| cell.set(0));
        SAVES.with(|cell| cell.set(0));
    }

    pub fn loads() -> u64 {
        LOADS.with(Cell::get)
    }

    pub fn saves() -> u64 {
        SAVES.with(Cell::get)
    }

    pub fn record_load() {
        LOADS.with(|cell| cell.set(cell.get().saturating_add(1)));
    }

    pub fn record_save() {
        SAVES.with(|cell| cell.set(cell.get().saturating_add(1)));
    }
}

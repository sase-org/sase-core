//! Deterministic bead event-store prefix migration.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::store_lock::{
    acquire_store_lock, timeout_from_env, LockMode, StoreLockError,
};

use super::config::{default_config, load_config, BeadConfigWire};
use super::events::{
    mint_bead_event_id, reduce_event_streams, BeadEventPayloadWire,
    BeadEventRecordWire, BeadEventStoreManifestWire, BeadEventStreamWire,
    BeadIssueUpdateEventFieldsWire, BEAD_EVENT_SCHEMA_VERSION,
};
use super::identity::{
    rewrite_id_tokens, rewrite_issue_id_prefix, validate_id_aliases,
    validate_issue_prefix,
};
use super::jsonl::{
    event_manifest_path, event_store_present, event_streams_dir,
    export_issues_to_jsonl, read_event_store,
};
use super::wire::{
    BeadError, DependencyWire, IssueWire, StatusWire, TaskPlusOneEvidenceWire,
};

const BEAD_PREFIX_MIGRATION_SCHEMA_VERSION: u64 = 1;
const BEAD_MUTATION_LOCK_FILENAME: &str = "beads.db";
const BEAD_MUTATION_HOLDER_FILENAME: &str = ".bead-mutation-lock.holder";
const BEAD_MUTATION_LOCK_TIMEOUT_ENV: &str = "SASE_BEAD_MUTATION_LOCK_TIMEOUT";
const BEAD_MUTATION_LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(600);

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadPrefixMigrationRequestWire {
    pub from_prefix: String,
    pub to_prefix: String,
    #[serde(default)]
    pub agent_name_map: BTreeMap<String, String>,
    #[serde(default)]
    pub expected_preimage_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadPrefixMigrationOutcomeWire {
    pub schema_version: u64,
    pub preimage_digest: String,
    pub postimage_digest: String,
    pub changed: bool,
    pub bead_id_map: BTreeMap<String, String>,
    pub event_id_map: BTreeMap<String, String>,
    pub token_counts: BTreeMap<String, u64>,
    pub total_token_replacements: u64,
    pub stream_count: usize,
    pub event_count: usize,
    pub issue_count: usize,
    pub alias_additions: BTreeMap<String, String>,
    #[serde(default)]
    pub lock_wait_ms: u64,
}

struct MigrationPlan {
    outcome: BeadPrefixMigrationOutcomeWire,
    streams: Vec<BeadEventStreamWire>,
    issues: Vec<IssueWire>,
    managed: ManagedBytes,
}

struct ManagedBytes {
    config_json: Vec<u8>,
    issues_jsonl: Vec<u8>,
    manifest_json: Vec<u8>,
    streams: Vec<(String, Vec<u8>)>,
}

pub fn preview_prefix_migration(
    beads_dir: &Path,
    request: BeadPrefixMigrationRequestWire,
) -> Result<BeadPrefixMigrationOutcomeWire, BeadError> {
    Ok(plan_prefix_migration(beads_dir, &request)?.outcome)
}

pub fn apply_prefix_migration(
    beads_dir: &Path,
    request: BeadPrefixMigrationRequestWire,
) -> Result<BeadPrefixMigrationOutcomeWire, BeadError> {
    with_reprefix_lock(beads_dir, || {
        let plan = plan_prefix_migration(beads_dir, &request)?;
        if let Some(expected) = request.expected_preimage_digest.as_deref() {
            if expected != plan.outcome.preimage_digest {
                return Err(BeadError::validation(format!(
                    "bead prefix migration preimage digest mismatch: expected {expected}, got {}",
                    plan.outcome.preimage_digest
                )));
            }
        }
        if plan.outcome.changed {
            install_plan(beads_dir, &plan.managed)?;
            let installed_digest = digest_managed_preimage(beads_dir)?;
            if installed_digest != plan.outcome.postimage_digest {
                return Err(BeadError::validation(format!(
                    "bead prefix migration postimage digest mismatch after install: expected {}, got {installed_digest}",
                    plan.outcome.postimage_digest
                )));
            }
            let (_manifest, installed_streams) = read_event_store(beads_dir)?;
            let installed_issues = reduce_event_streams(&installed_streams)?;
            if installed_streams != plan.streams
                || installed_issues != plan.issues
            {
                return Err(BeadError::validation(
                    "installed bead prefix migration store does not match planned postimage",
                ));
            }
        }
        Ok(plan.outcome)
    })
}

fn plan_prefix_migration(
    beads_dir: &Path,
    request: &BeadPrefixMigrationRequestWire,
) -> Result<MigrationPlan, BeadError> {
    validate_request(request)?;
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    if !event_store_present(beads_dir) {
        return Err(BeadError::validation(
            "bead prefix migration requires an event-backed bead store",
        ));
    }

    let preimage_digest = digest_managed_preimage(beads_dir)?;
    let fallback = default_config("beads", "");
    let config = load_config(beads_dir, fallback)?;
    let (_manifest, streams) = read_event_store(beads_dir)?;
    let issues = reduce_event_streams(&streams)?;
    let canonical_ids = issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect::<BTreeSet<_>>();
    validate_id_aliases(&config.id_aliases, &canonical_ids)?;

    let bead_id_map = collect_bead_id_map(
        &streams,
        &issues,
        &request.from_prefix,
        &request.to_prefix,
    )?;
    reject_active_affected_issues(&issues, &bead_id_map)?;
    reject_destination_collisions(&canonical_ids, &bead_id_map)?;

    if bead_id_map.is_empty() {
        let outcome = BeadPrefixMigrationOutcomeWire {
            schema_version: BEAD_PREFIX_MIGRATION_SCHEMA_VERSION,
            preimage_digest: preimage_digest.clone(),
            postimage_digest: preimage_digest,
            changed: false,
            bead_id_map,
            event_id_map: BTreeMap::new(),
            token_counts: BTreeMap::new(),
            total_token_replacements: 0,
            stream_count: streams.len(),
            event_count: streams.iter().map(|stream| stream.events.len()).sum(),
            issue_count: issues.len(),
            alias_additions: BTreeMap::new(),
            lock_wait_ms: 0,
        };
        let managed = read_current_managed_bytes(beads_dir)?;
        return Ok(MigrationPlan {
            outcome,
            streams,
            issues,
            managed,
        });
    }

    let mut token_counts = BTreeMap::new();
    let mut total_token_replacements = 0;
    let rewritten_streams = rewrite_streams(
        &streams,
        &bead_id_map,
        &request.agent_name_map,
        &mut token_counts,
        &mut total_token_replacements,
    )?;
    let rewritten_issues = reduce_event_streams(&rewritten_streams)?;
    let mut transformed_projection = issues.clone();
    for issue in &mut transformed_projection {
        rewrite_issue(
            issue,
            &bead_id_map,
            &request.agent_name_map,
            &mut None,
            &mut 0,
        )?;
    }
    transformed_projection.sort_by(|left, right| left.id.cmp(&right.id));
    let mut sorted_rewritten_issues = rewritten_issues.clone();
    sorted_rewritten_issues.sort_by(|left, right| left.id.cmp(&right.id));
    if transformed_projection != sorted_rewritten_issues {
        return Err(BeadError::validation(
            "bead prefix migration projection isomorphism check failed",
        ));
    }

    let mut new_config = config.clone();
    new_config.issue_prefix = request.to_prefix.clone();
    new_config.next_counter = next_top_level_counter(
        &request.to_prefix,
        new_config.next_counter,
        &rewritten_issues,
    );
    for target in new_config.id_aliases.values_mut() {
        if let Some(rewritten) = bead_id_map.get(target) {
            *target = rewritten.clone();
        }
    }
    for (old_id, new_id) in &bead_id_map {
        new_config.id_aliases.insert(old_id.clone(), new_id.clone());
    }
    let rewritten_canonical_ids = rewritten_issues
        .iter()
        .map(|issue| issue.id.clone())
        .collect::<BTreeSet<_>>();
    validate_id_aliases(&new_config.id_aliases, &rewritten_canonical_ids)?;

    let managed = render_managed_bytes(
        &new_config,
        &rewritten_streams,
        &rewritten_issues,
    )?;
    let postimage_digest = digest_managed_bytes(&managed);
    let event_id_map = event_id_map(&streams, &rewritten_streams);
    let alias_additions = bead_id_map.clone();
    let outcome = BeadPrefixMigrationOutcomeWire {
        schema_version: BEAD_PREFIX_MIGRATION_SCHEMA_VERSION,
        preimage_digest,
        postimage_digest,
        changed: true,
        bead_id_map,
        event_id_map,
        token_counts,
        total_token_replacements,
        stream_count: rewritten_streams.len(),
        event_count: rewritten_streams
            .iter()
            .map(|stream| stream.events.len())
            .sum(),
        issue_count: rewritten_issues.len(),
        alias_additions,
        lock_wait_ms: 0,
    };
    Ok(MigrationPlan {
        outcome,
        streams: rewritten_streams,
        issues: rewritten_issues,
        managed,
    })
}

fn validate_request(
    request: &BeadPrefixMigrationRequestWire,
) -> Result<(), BeadError> {
    validate_issue_prefix(&request.from_prefix)?;
    validate_issue_prefix(&request.to_prefix)?;
    if request.from_prefix == request.to_prefix {
        return Err(BeadError::validation(
            "bead prefix migration requires distinct prefixes",
        ));
    }
    for (source, destination) in &request.agent_name_map {
        if source.trim().is_empty() || destination.trim().is_empty() {
            return Err(BeadError::validation(
                "agent name map entries cannot be empty or blank",
            ));
        }
        if source == destination {
            return Err(BeadError::validation(format!(
                "agent name map entry {source} targets itself"
            )));
        }
    }
    Ok(())
}

fn collect_bead_id_map(
    streams: &[BeadEventStreamWire],
    issues: &[IssueWire],
    from_prefix: &str,
    to_prefix: &str,
) -> Result<BTreeMap<String, String>, BeadError> {
    let mut map = BTreeMap::new();
    for issue in issues {
        collect_issue_ids(issue, from_prefix, to_prefix, &mut map)?;
    }
    for stream in streams {
        collect_id(&stream.stream_id, from_prefix, to_prefix, &mut map)?;
        collect_id(&stream.root_issue_id, from_prefix, to_prefix, &mut map)?;
        for event in &stream.events {
            collect_id(&event.issue_id, from_prefix, to_prefix, &mut map)?;
            collect_payload_ids(
                &event.payload,
                from_prefix,
                to_prefix,
                &mut map,
            )?;
        }
    }
    Ok(map)
}

fn collect_issue_ids(
    issue: &IssueWire,
    from_prefix: &str,
    to_prefix: &str,
    map: &mut BTreeMap<String, String>,
) -> Result<(), BeadError> {
    collect_id(&issue.id, from_prefix, to_prefix, map)?;
    if let Some(parent_id) = issue.parent_id.as_deref() {
        collect_id(parent_id, from_prefix, to_prefix, map)?;
    }
    for dependency in &issue.dependencies {
        collect_id(&dependency.issue_id, from_prefix, to_prefix, map)?;
        collect_id(&dependency.depends_on_id, from_prefix, to_prefix, map)?;
    }
    for reference in &issue.refs {
        if let Some(bead_id) = reference.strip_prefix("bead:") {
            collect_id(bead_id, from_prefix, to_prefix, map)?;
        }
    }
    Ok(())
}

fn collect_payload_ids(
    payload: &BeadEventPayloadWire,
    from_prefix: &str,
    to_prefix: &str,
    map: &mut BTreeMap<String, String>,
) -> Result<(), BeadError> {
    match payload {
        BeadEventPayloadWire::IssueCreated { issue } => {
            collect_issue_ids(issue, from_prefix, to_prefix, map)
        }
        BeadEventPayloadWire::IssueUpdated { .. }
        | BeadEventPayloadWire::NoteAppended { .. }
        | BeadEventPayloadWire::IssueOpened
        | BeadEventPayloadWire::ReadyMarked
        | BeadEventPayloadWire::ReadyUnmarked
        | BeadEventPayloadWire::EpicWorkPreclaimed { .. } => Ok(()),
        BeadEventPayloadWire::IssueClosed {
            forced_descendant_ids,
            ..
        } => {
            for issue_id in forced_descendant_ids {
                collect_id(issue_id, from_prefix, to_prefix, map)?;
            }
            Ok(())
        }
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => {
            for issue_id in cascade_removed_issue_ids {
                collect_id(issue_id, from_prefix, to_prefix, map)?;
            }
            Ok(())
        }
        BeadEventPayloadWire::DependencyAdded { dependency }
        | BeadEventPayloadWire::DependencyRemoved { dependency } => {
            collect_id(&dependency.issue_id, from_prefix, to_prefix, map)?;
            collect_id(&dependency.depends_on_id, from_prefix, to_prefix, map)
        }
        BeadEventPayloadWire::ReferenceAdded { reference }
        | BeadEventPayloadWire::ReferenceRemoved { reference } => {
            if let Some(bead_id) = reference.strip_prefix("bead:") {
                collect_id(bead_id, from_prefix, to_prefix, map)?;
            }
            Ok(())
        }
        BeadEventPayloadWire::TaskPlusOneRecorded { evidence } => {
            for reference in &evidence.refs {
                if let Some(bead_id) = reference.strip_prefix("bead:") {
                    collect_id(bead_id, from_prefix, to_prefix, map)?;
                }
            }
            Ok(())
        }
    }
}

fn collect_id(
    issue_id: &str,
    from_prefix: &str,
    to_prefix: &str,
    map: &mut BTreeMap<String, String>,
) -> Result<(), BeadError> {
    if let Some(rewritten) =
        rewrite_issue_id_prefix(issue_id, from_prefix, to_prefix)?
    {
        if let Some(existing) =
            map.insert(issue_id.to_string(), rewritten.clone())
        {
            if existing != rewritten {
                return Err(BeadError::validation(format!(
                    "inconsistent bead ID rewrite for {issue_id}: {existing} != {rewritten}"
                )));
            }
        }
    }
    Ok(())
}

fn reject_active_affected_issues(
    issues: &[IssueWire],
    bead_id_map: &BTreeMap<String, String>,
) -> Result<(), BeadError> {
    let active = issues
        .iter()
        .filter(|issue| bead_id_map.contains_key(&issue.id))
        .filter(|issue| {
            matches!(issue.status, StatusWire::Claimed | StatusWire::InProgress)
        })
        .map(|issue| issue.id.clone())
        .collect::<Vec<_>>();
    if active.is_empty() {
        Ok(())
    } else {
        Err(BeadError::validation(format!(
            "cannot re-prefix active bead IDs: {}",
            active.join(", ")
        )))
    }
}

fn reject_destination_collisions(
    canonical_ids: &BTreeSet<String>,
    bead_id_map: &BTreeMap<String, String>,
) -> Result<(), BeadError> {
    let mut destinations = BTreeSet::new();
    for (old_id, new_id) in bead_id_map {
        if !destinations.insert(new_id.clone()) {
            return Err(BeadError::validation(format!(
                "bead prefix migration produces duplicate destination {new_id}"
            )));
        }
        if canonical_ids.contains(new_id) && old_id != new_id {
            return Err(BeadError::validation(format!(
                "bead prefix migration destination {new_id} already exists"
            )));
        }
    }
    Ok(())
}

fn rewrite_streams(
    streams: &[BeadEventStreamWire],
    bead_id_map: &BTreeMap<String, String>,
    agent_name_map: &BTreeMap<String, String>,
    token_counts: &mut BTreeMap<String, u64>,
    total_token_replacements: &mut u64,
) -> Result<Vec<BeadEventStreamWire>, BeadError> {
    let mut result = Vec::with_capacity(streams.len());
    let mut event_ids = BTreeSet::new();
    for stream in streams {
        let stream_id = rewrite_id(&stream.stream_id, bead_id_map);
        let root_issue_id = rewrite_id(&stream.root_issue_id, bead_id_map);
        let mut events = Vec::with_capacity(stream.events.len());
        for (index, event) in stream.events.iter().enumerate() {
            let issue_id = rewrite_id(&event.issue_id, bead_id_map);
            let actor = rewrite_agent_name(&event.actor, agent_name_map);
            let payload = rewrite_payload(
                &event.payload,
                bead_id_map,
                agent_name_map,
                token_counts,
                total_token_replacements,
            )?;
            let event_id = mint_bead_event_id(
                &stream_id,
                index + 1,
                &event.timestamp,
                &actor,
                event.operation,
                &issue_id,
                &payload,
            )?;
            if !event_ids.insert(event_id.clone()) {
                return Err(BeadError::validation(format!(
                    "bead prefix migration produced duplicate event ID {event_id}"
                )));
            }
            let rewritten = BeadEventRecordWire {
                schema_version: BEAD_EVENT_SCHEMA_VERSION,
                event_id,
                timestamp: event.timestamp.clone(),
                actor,
                operation: event.operation,
                issue_id,
                payload,
            };
            rewritten.validate()?;
            events.push(rewritten);
        }
        let rewritten = BeadEventStreamWire {
            stream_id,
            root_issue_id,
            events,
        };
        rewritten.validate()?;
        result.push(rewritten);
    }
    result.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    Ok(result)
}

fn rewrite_payload(
    payload: &BeadEventPayloadWire,
    bead_id_map: &BTreeMap<String, String>,
    agent_name_map: &BTreeMap<String, String>,
    token_counts: &mut BTreeMap<String, u64>,
    total_token_replacements: &mut u64,
) -> Result<BeadEventPayloadWire, BeadError> {
    Ok(match payload {
        BeadEventPayloadWire::IssueCreated { issue } => {
            let mut issue = issue.clone();
            rewrite_issue(
                &mut issue,
                bead_id_map,
                agent_name_map,
                &mut Some(token_counts),
                total_token_replacements,
            )?;
            BeadEventPayloadWire::IssueCreated { issue }
        }
        BeadEventPayloadWire::IssueUpdated { fields } => {
            let mut fields = fields.clone();
            rewrite_update_fields(
                &mut fields,
                bead_id_map,
                agent_name_map,
                &mut Some(token_counts),
                total_token_replacements,
            );
            BeadEventPayloadWire::IssueUpdated { fields }
        }
        BeadEventPayloadWire::NoteAppended { entry } => {
            BeadEventPayloadWire::NoteAppended {
                entry: rewrite_text(
                    entry,
                    bead_id_map,
                    &mut Some(token_counts),
                    total_token_replacements,
                ),
            }
        }
        BeadEventPayloadWire::IssueOpened => BeadEventPayloadWire::IssueOpened,
        BeadEventPayloadWire::IssueClosed {
            close_reason,
            resolution,
            forced_descendant_ids,
        } => BeadEventPayloadWire::IssueClosed {
            close_reason: close_reason.as_ref().map(|reason| {
                rewrite_text(
                    reason,
                    bead_id_map,
                    &mut Some(token_counts),
                    total_token_replacements,
                )
            }),
            resolution: resolution.clone(),
            forced_descendant_ids: forced_descendant_ids
                .iter()
                .map(|issue_id| rewrite_id(issue_id, bead_id_map))
                .collect(),
        },
        BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids,
        } => BeadEventPayloadWire::IssueRemoved {
            cascade_removed_issue_ids: cascade_removed_issue_ids
                .iter()
                .map(|issue_id| rewrite_id(issue_id, bead_id_map))
                .collect(),
        },
        BeadEventPayloadWire::DependencyAdded { dependency } => {
            BeadEventPayloadWire::DependencyAdded {
                dependency: rewrite_dependency(dependency, bead_id_map),
            }
        }
        BeadEventPayloadWire::DependencyRemoved { dependency } => {
            BeadEventPayloadWire::DependencyRemoved {
                dependency: rewrite_dependency(dependency, bead_id_map),
            }
        }
        BeadEventPayloadWire::ReferenceAdded { reference } => {
            BeadEventPayloadWire::ReferenceAdded {
                reference: rewrite_reference(reference, bead_id_map),
            }
        }
        BeadEventPayloadWire::ReferenceRemoved { reference } => {
            BeadEventPayloadWire::ReferenceRemoved {
                reference: rewrite_reference(reference, bead_id_map),
            }
        }
        BeadEventPayloadWire::ReadyMarked => BeadEventPayloadWire::ReadyMarked,
        BeadEventPayloadWire::ReadyUnmarked => {
            BeadEventPayloadWire::ReadyUnmarked
        }
        BeadEventPayloadWire::EpicWorkPreclaimed { agent_name } => {
            BeadEventPayloadWire::EpicWorkPreclaimed {
                agent_name: rewrite_agent_name(agent_name, agent_name_map),
            }
        }
        BeadEventPayloadWire::TaskPlusOneRecorded { evidence } => {
            let mut evidence = evidence.clone();
            rewrite_plus_one_evidence(
                &mut evidence,
                bead_id_map,
                agent_name_map,
                &mut Some(token_counts),
                total_token_replacements,
            );
            BeadEventPayloadWire::TaskPlusOneRecorded { evidence }
        }
    })
}

fn rewrite_issue(
    issue: &mut IssueWire,
    bead_id_map: &BTreeMap<String, String>,
    agent_name_map: &BTreeMap<String, String>,
    token_counts: &mut Option<&mut BTreeMap<String, u64>>,
    total_token_replacements: &mut u64,
) -> Result<(), BeadError> {
    issue.id = rewrite_id(&issue.id, bead_id_map);
    issue.parent_id = issue
        .parent_id
        .as_ref()
        .map(|parent_id| rewrite_id(parent_id, bead_id_map));
    issue.assignee = rewrite_agent_name(&issue.assignee, agent_name_map);
    issue.created_by = rewrite_agent_name(&issue.created_by, agent_name_map);
    issue.title = rewrite_text(
        &issue.title,
        bead_id_map,
        token_counts,
        total_token_replacements,
    );
    issue.description = rewrite_text(
        &issue.description,
        bead_id_map,
        token_counts,
        total_token_replacements,
    );
    issue.notes = rewrite_text(
        &issue.notes,
        bead_id_map,
        token_counts,
        total_token_replacements,
    );
    issue.design = rewrite_text(
        &issue.design,
        bead_id_map,
        token_counts,
        total_token_replacements,
    );
    issue.refs = issue
        .refs
        .iter()
        .map(|reference| rewrite_reference(reference, bead_id_map))
        .collect();
    for evidence in &mut issue.plus_one_evidence {
        rewrite_plus_one_evidence(
            evidence,
            bead_id_map,
            agent_name_map,
            token_counts,
            total_token_replacements,
        );
    }
    issue.dependencies = issue
        .dependencies
        .iter()
        .map(|dependency| rewrite_dependency(dependency, bead_id_map))
        .collect();
    issue.validate()?;
    Ok(())
}

fn rewrite_update_fields(
    fields: &mut BeadIssueUpdateEventFieldsWire,
    bead_id_map: &BTreeMap<String, String>,
    agent_name_map: &BTreeMap<String, String>,
    token_counts: &mut Option<&mut BTreeMap<String, u64>>,
    total_token_replacements: &mut u64,
) {
    if let Some(value) = fields.title.as_mut() {
        *value = rewrite_text(
            value,
            bead_id_map,
            token_counts,
            total_token_replacements,
        );
    }
    if let Some(value) = fields.assignee.as_mut() {
        *value = rewrite_agent_name(value, agent_name_map);
    }
    if let Some(value) = fields.description.as_mut() {
        *value = rewrite_text(
            value,
            bead_id_map,
            token_counts,
            total_token_replacements,
        );
    }
    if let Some(value) = fields.notes.as_mut() {
        *value = rewrite_text(
            value,
            bead_id_map,
            token_counts,
            total_token_replacements,
        );
    }
    if let Some(value) = fields.design.as_mut() {
        *value = rewrite_text(
            value,
            bead_id_map,
            token_counts,
            total_token_replacements,
        );
    }
}

fn rewrite_plus_one_evidence(
    evidence: &mut TaskPlusOneEvidenceWire,
    bead_id_map: &BTreeMap<String, String>,
    agent_name_map: &BTreeMap<String, String>,
    token_counts: &mut Option<&mut BTreeMap<String, u64>>,
    total_token_replacements: &mut u64,
) {
    evidence.reporter = rewrite_agent_name(&evidence.reporter, agent_name_map);
    evidence.note = rewrite_text(
        &evidence.note,
        bead_id_map,
        token_counts,
        total_token_replacements,
    );
    evidence.refs = evidence
        .refs
        .iter()
        .map(|reference| rewrite_reference(reference, bead_id_map))
        .collect();
}

fn rewrite_dependency(
    dependency: &DependencyWire,
    bead_id_map: &BTreeMap<String, String>,
) -> DependencyWire {
    DependencyWire {
        issue_id: rewrite_id(&dependency.issue_id, bead_id_map),
        depends_on_id: rewrite_id(&dependency.depends_on_id, bead_id_map),
        created_at: dependency.created_at.clone(),
        created_by: dependency.created_by.clone(),
    }
}

fn rewrite_reference(
    reference: &str,
    bead_id_map: &BTreeMap<String, String>,
) -> String {
    if let Some(bead_id) = reference.strip_prefix("bead:") {
        if let Some(rewritten) = bead_id_map.get(bead_id) {
            return format!("bead:{rewritten}");
        }
    }
    reference.to_string()
}

fn rewrite_id(
    issue_id: &str,
    bead_id_map: &BTreeMap<String, String>,
) -> String {
    bead_id_map
        .get(issue_id)
        .cloned()
        .unwrap_or_else(|| issue_id.to_string())
}

fn rewrite_agent_name(
    value: &str,
    agent_name_map: &BTreeMap<String, String>,
) -> String {
    agent_name_map
        .get(value)
        .cloned()
        .unwrap_or_else(|| value.to_string())
}

fn rewrite_text(
    text: &str,
    bead_id_map: &BTreeMap<String, String>,
    token_counts: &mut Option<&mut BTreeMap<String, u64>>,
    total_token_replacements: &mut u64,
) -> String {
    let outcome = rewrite_id_tokens(text, bead_id_map);
    if outcome.total_replacements > 0 {
        if let Some(counts) = token_counts.as_deref_mut() {
            for (issue_id, count) in outcome.replacement_counts {
                *counts.entry(issue_id).or_insert(0) += count;
            }
        }
        *total_token_replacements += outcome.total_replacements;
    }
    outcome.text
}

fn event_id_map(
    old_streams: &[BeadEventStreamWire],
    new_streams: &[BeadEventStreamWire],
) -> BTreeMap<String, String> {
    let old_events = old_streams
        .iter()
        .flat_map(|stream| stream.events.iter())
        .collect::<Vec<_>>();
    let new_events = new_streams
        .iter()
        .flat_map(|stream| stream.events.iter())
        .collect::<Vec<_>>();
    old_events
        .into_iter()
        .zip(new_events)
        .filter(|(old, new)| old.event_id != new.event_id)
        .map(|(old, new)| (old.event_id.clone(), new.event_id.clone()))
        .collect()
}

fn render_managed_bytes(
    config: &BeadConfigWire,
    streams: &[BeadEventStreamWire],
    issues: &[IssueWire],
) -> Result<ManagedBytes, BeadError> {
    let mut config_json = serde_json::to_vec_pretty(config)?;
    config_json.push(b'\n');
    let issues_jsonl = export_issues_to_jsonl(issues)?.into_bytes();
    let mut sorted_streams = streams.to_vec();
    sorted_streams.sort_by(|left, right| left.stream_id.cmp(&right.stream_id));
    let manifest = BeadEventStoreManifestWire::from_streams(&sorted_streams);
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    let streams = sorted_streams
        .iter()
        .map(|stream| {
            stream.validate()?;
            let mut bytes = Vec::new();
            for event in &stream.events {
                bytes.extend_from_slice(
                    serde_json::to_string(event)?.as_bytes(),
                );
                bytes.push(b'\n');
            }
            Ok((format!("{}.jsonl", stream.stream_id), bytes))
        })
        .collect::<Result<Vec<_>, BeadError>>()?;
    Ok(ManagedBytes {
        config_json,
        issues_jsonl,
        manifest_json,
        streams,
    })
}

fn read_current_managed_bytes(
    beads_dir: &Path,
) -> Result<ManagedBytes, BeadError> {
    let config_json = fs::read(beads_dir.join("config.json"))?;
    let issues_jsonl = fs::read(beads_dir.join("issues.jsonl"))?;
    let manifest_json = fs::read(event_manifest_path(beads_dir))?;
    let mut streams = Vec::new();
    for entry in fs::read_dir(event_streams_dir(beads_dir))? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    BeadError::io(format!(
                        "invalid bead event stream path {}",
                        path.display()
                    ))
                })?
                .to_string();
            streams.push((name, fs::read(path)?));
        }
    }
    streams.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(ManagedBytes {
        config_json,
        issues_jsonl,
        manifest_json,
        streams,
    })
}

fn digest_managed_preimage(beads_dir: &Path) -> Result<String, BeadError> {
    Ok(digest_managed_bytes(&read_current_managed_bytes(
        beads_dir,
    )?))
}

fn digest_managed_bytes(managed: &ManagedBytes) -> String {
    let mut hasher = Sha256::new();
    digest_one(&mut hasher, "config.json", &managed.config_json);
    digest_one(&mut hasher, "issues.jsonl", &managed.issues_jsonl);
    digest_one(&mut hasher, "events/manifest.json", &managed.manifest_json);
    for (name, bytes) in &managed.streams {
        digest_one(&mut hasher, &format!("events/streams/{name}"), bytes);
    }
    hex::encode(hasher.finalize())
}

fn digest_one(hasher: &mut Sha256, path: &str, bytes: &[u8]) {
    hasher.update(path.as_bytes());
    hasher.update([0]);
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update([0]);
    hasher.update(bytes);
    hasher.update([0]);
}

fn install_plan(
    beads_dir: &Path,
    managed: &ManagedBytes,
) -> Result<(), BeadError> {
    let stage_dir =
        beads_dir.join(format!(".bead-reprefix-stage-{}", std::process::id()));
    let backup_dir =
        beads_dir.join(format!(".bead-reprefix-backup-{}", std::process::id()));
    let _ = fs::remove_dir_all(&stage_dir);
    let _ = fs::remove_dir_all(&backup_dir);
    fs::create_dir_all(stage_dir.join("events/streams"))?;
    fs::write(stage_dir.join("config.json"), &managed.config_json)?;
    fs::write(stage_dir.join("issues.jsonl"), &managed.issues_jsonl)?;
    fs::write(
        stage_dir.join("events/manifest.json"),
        &managed.manifest_json,
    )?;
    for (name, bytes) in &managed.streams {
        fs::write(stage_dir.join("events/streams").join(name), bytes)?;
    }
    read_event_store(&stage_dir)?;

    let install_result = install_staged(beads_dir, &stage_dir, &backup_dir);
    let cleanup_stage = fs::remove_dir_all(&stage_dir);
    match install_result {
        Ok(()) => {
            let _ = fs::remove_dir_all(&backup_dir);
            if let Err(error) = cleanup_stage {
                return Err(BeadError::io(format!(
                    "failed to remove bead prefix migration stage {}: {error}",
                    stage_dir.display()
                )));
            }
            Ok(())
        }
        Err(error) => {
            let _ = rollback_install(beads_dir, &backup_dir);
            let _ = fs::remove_dir_all(&stage_dir);
            let _ = fs::remove_dir_all(&backup_dir);
            Err(error)
        }
    }
}

fn install_staged(
    beads_dir: &Path,
    stage_dir: &Path,
    backup_dir: &Path,
) -> Result<(), BeadError> {
    fs::create_dir_all(backup_dir)?;
    replace_path(beads_dir, stage_dir, backup_dir, Path::new("config.json"))?;
    replace_path(beads_dir, stage_dir, backup_dir, Path::new("issues.jsonl"))?;
    replace_path(
        beads_dir,
        stage_dir,
        backup_dir,
        Path::new("events/manifest.json"),
    )?;
    replace_path(
        beads_dir,
        stage_dir,
        backup_dir,
        Path::new("events/streams"),
    )?;
    Ok(())
}

fn replace_path(
    beads_dir: &Path,
    stage_dir: &Path,
    backup_dir: &Path,
    relative: &Path,
) -> Result<(), BeadError> {
    let destination = beads_dir.join(relative);
    let source = stage_dir.join(relative);
    let backup = backup_dir.join(relative);
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = backup.parent() {
        fs::create_dir_all(parent)?;
    }
    if destination.exists() {
        fs::rename(&destination, &backup)?;
    }
    fs::rename(&source, &destination).map_err(|error| {
        BeadError::io(format!(
            "failed to install bead prefix migration path {}: {error}",
            destination.display()
        ))
    })
}

fn rollback_install(
    beads_dir: &Path,
    backup_dir: &Path,
) -> Result<(), BeadError> {
    for relative in [
        Path::new("events/streams"),
        Path::new("events/manifest.json"),
        Path::new("issues.jsonl"),
        Path::new("config.json"),
    ] {
        let destination = beads_dir.join(relative);
        let backup = backup_dir.join(relative);
        if !backup.exists() {
            continue;
        }
        if destination.exists() {
            if destination.is_dir() {
                fs::remove_dir_all(&destination)?;
            } else {
                fs::remove_file(&destination)?;
            }
        }
        fs::rename(backup, destination)?;
    }
    Ok(())
}

fn with_reprefix_lock(
    beads_dir: &Path,
    mutation: impl FnOnce() -> Result<BeadPrefixMigrationOutcomeWire, BeadError>,
) -> Result<BeadPrefixMigrationOutcomeWire, BeadError> {
    if !beads_dir.is_dir() {
        return Err(BeadError::io(format!(
            "No beads directory found at {}",
            beads_dir.display()
        )));
    }
    let lock_path = beads_dir.join(BEAD_MUTATION_LOCK_FILENAME);
    let holder_path = beads_dir.join(BEAD_MUTATION_HOLDER_FILENAME);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path,
        LockMode::Exclusive,
        timeout_from_env(
            BEAD_MUTATION_LOCK_TIMEOUT_ENV,
            BEAD_MUTATION_LOCK_TIMEOUT_DEFAULT,
        ),
        "bead_prefix_migration",
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
    })?;
    let wait_ms = lock.waited_ms();
    let result = mutation();
    let unlock = lock.release().map_err(|error| BeadError {
        kind: "lock_release".to_string(),
        message: format!(
            "failed to release bead mutation lock {} for store {}: {error}",
            lock_path.display(),
            beads_dir.display()
        ),
    });
    match (result, unlock) {
        (Ok(mut outcome), Ok(())) => {
            outcome.lock_wait_ms = wait_ms;
            Ok(outcome)
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(unlock_error)) => Err(BeadError {
            kind: unlock_error.kind,
            message: format!(
                "{}; the locked mutation also failed with {}: {}",
                unlock_error.message, error.kind, error.message
            ),
        }),
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

fn max_top_level_counter(issue_prefix: &str, issues: &[IssueWire]) -> u64 {
    let expected_prefix = format!("{issue_prefix}-");
    issues
        .iter()
        .filter_map(|issue| issue.id.strip_prefix(&expected_prefix))
        .filter(|suffix| !suffix.contains('.'))
        .filter_map(from_base36)
        .max()
        .unwrap_or(0)
}

fn from_base36(value: &str) -> Option<u64> {
    let mut result = 0_u64;
    for ch in value.chars() {
        let digit = ch.to_digit(36)? as u64;
        result = result.checked_mul(36)?.checked_add(digit)?;
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::mutation::{
        create_issue, init_store, BeadCreateRequestWire,
    };
    use crate::bead::wire::IssueTypeWire;
    use tempfile::tempdir;

    #[test]
    fn preview_and_apply_reprefix_store_with_aliases() {
        let temp = tempdir().unwrap();
        init_store(temp.path(), "beads", "old", "owner").unwrap();
        let beads_dir = temp.path().join("beads");
        let issue = create_issue(
            &beads_dir,
            BeadCreateRequestWire {
                title: "mentions old-1".to_string(),
                issue_type: IssueTypeWire::Task,
                size: Some(crate::bead::wire::PhaseSizeWire::Small),
                description: "see bead:old-1 and https://host/old-1"
                    .to_string(),
                ..BeadCreateRequestWire::default()
            },
        )
        .unwrap()
        .issue
        .unwrap();
        assert_eq!(issue.id, "old-1");

        let request = BeadPrefixMigrationRequestWire {
            from_prefix: "old".to_string(),
            to_prefix: "new".to_string(),
            ..BeadPrefixMigrationRequestWire::default()
        };
        let preview =
            preview_prefix_migration(&beads_dir, request.clone()).unwrap();
        assert!(preview.changed);
        assert_eq!(preview.bead_id_map["old-1"], "new-1");
        assert_eq!(preview.alias_additions["old-1"], "new-1");
        assert!(preview.total_token_replacements >= 2);

        let applied = apply_prefix_migration(
            &beads_dir,
            BeadPrefixMigrationRequestWire {
                expected_preimage_digest: Some(preview.preimage_digest.clone()),
                ..request.clone()
            },
        )
        .unwrap();
        assert_eq!(applied.postimage_digest, preview.postimage_digest);
        let config =
            load_config(&beads_dir, default_config("fallback", "")).unwrap();
        assert_eq!(config.issue_prefix, "new");
        assert_eq!(config.id_aliases["old-1"], "new-1");

        let rerun = preview_prefix_migration(&beads_dir, request).unwrap();
        assert!(!rerun.changed);
        assert!(rerun.bead_id_map.is_empty());
    }
}

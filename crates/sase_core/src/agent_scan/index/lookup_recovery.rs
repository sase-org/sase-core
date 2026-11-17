//! Bounded unavailable-index recovery for identity lookups.
//!
//! Direct artifact pointers are tried first. Remaining work is an
//! ace-run walk capped at [`LOOKUP_FALLBACK_BUDGET`] directory entries and
//! hydrations, with prompt/step reads disabled.

use super::lookup::{
    canonical_path, AgentArtifactLookupHitWire, AgentArtifactLookupQueryWire,
    AgentArtifactLookupTargetWire,
};
use super::lookup_identity::{lookup_clan_from_meta, lookup_session_from_meta};
use crate::agent_scan::layout::{is_artifact_timestamp, ACE_RUN_WORKFLOW_DIR};
use crate::agent_scan::scanner::{
    agent_meta_from_object, scan_agent_artifact_dir,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire, AgentMetaWire,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

pub(super) const LOOKUP_FALLBACK_BUDGET: u32 = 200;

pub(super) struct LookupRecovery {
    pub(super) hits: Vec<AgentArtifactLookupHitWire>,
    pub(super) truncated: bool,
    pub(super) exhausted: bool,
    pub(super) dirs_visited: u64,
    pub(super) records_decoded: u64,
}

pub(super) fn bounded_lookup_recovery(
    projects_root: &Path,
    query: &AgentArtifactLookupQueryWire,
) -> LookupRecovery {
    let mut recovery = LookupRecovery {
        hits: Vec::new(),
        truncated: false,
        exhausted: false,
        dirs_visited: 0,
        records_decoded: 0,
    };
    let options = fallback_scan_options();
    let mut seen = BTreeSet::new();
    if let AgentArtifactLookupTargetWire::Pointers { artifact_dirs } =
        &query.target
    {
        for artifact_dir in artifact_dirs {
            if recovery.dirs_visited >= u64::from(LOOKUP_FALLBACK_BUDGET) {
                recovery.truncated = true;
                break;
            }
            recovery.dirs_visited += 1;
            consider_dir(
                projects_root,
                Path::new(artifact_dir),
                query,
                &options,
                &mut seen,
                &mut recovery,
            );
        }
        recovery.exhausted = !recovery.truncated;
        return recovery;
    }
    walk_lookup_dirs(
        projects_root,
        projects_root,
        0,
        query,
        &options,
        &mut seen,
        &mut recovery,
    );
    recovery.exhausted = !recovery.truncated;
    recovery
}

/// Re-read only matching source records. The index discovers candidates;
/// current metadata and markers determine their identity and actionable state.
pub(super) fn revalidate_lookup_hits(
    projects_root: &Path,
    query: &AgentArtifactLookupQueryWire,
    hits: Vec<AgentArtifactLookupHitWire>,
) -> Vec<AgentArtifactLookupHitWire> {
    let options = fallback_scan_options();
    let mut seen = BTreeSet::new();
    let mut recovery = LookupRecovery {
        hits: Vec::new(),
        truncated: false,
        exhausted: false,
        dirs_visited: 0,
        records_decoded: 0,
    };
    for hit in hits {
        let pointer_query;
        let query = if matches!(
            query.target,
            AgentArtifactLookupTargetWire::Pointers { .. }
        ) {
            pointer_query = AgentArtifactLookupQueryWire {
                target: AgentArtifactLookupTargetWire::Pointers {
                    artifact_dirs: vec![hit.artifact_dir.clone()],
                },
                ..query.clone()
            };
            &pointer_query
        } else {
            query
        };
        consider_dir(
            projects_root,
            Path::new(&hit.artifact_dir),
            query,
            &options,
            &mut seen,
            &mut recovery,
        );
    }
    // Exact pointers remain useful before their lifecycle upsert reaches the index.
    if let AgentArtifactLookupTargetWire::Pointers { artifact_dirs } =
        &query.target
    {
        for path in artifact_dirs {
            consider_dir(
                projects_root,
                Path::new(path),
                query,
                &options,
                &mut seen,
                &mut recovery,
            );
        }
    }
    recovery.hits
}

fn consider_dir(
    projects_root: &Path,
    artifact_dir: &Path,
    query: &AgentArtifactLookupQueryWire,
    options: &AgentArtifactScanOptionsWire,
    seen: &mut BTreeSet<String>,
    recovery: &mut LookupRecovery,
) {
    let key = canonical_path(&artifact_dir.to_string_lossy());
    if !seen.insert(key) {
        return;
    }
    let Some(match_kind) = cheap_match(artifact_dir, query) else {
        return;
    };
    let Some(record) =
        scan_agent_artifact_dir(projects_root, artifact_dir, options)
    else {
        return;
    };
    recovery.records_decoded += 1;
    if !query.include_hidden && record_is_hidden(&record) {
        return;
    }
    if let Some(hit) = hit_from_record(&record, &match_kind) {
        recovery.hits.push(hit);
    }
}

fn cheap_match(
    artifact_dir: &Path,
    query: &AgentArtifactLookupQueryWire,
) -> Option<String> {
    if !artifact_dir.join("agent_meta.json").is_file() {
        return None;
    }
    match &query.target {
        AgentArtifactLookupTargetWire::Pointers { artifact_dirs } => {
            let canonical = canonical_path(&artifact_dir.to_string_lossy());
            artifact_dirs
                .iter()
                .any(|dir| {
                    dir == &canonical || canonical_path(dir) == canonical
                })
                .then(|| "pointer".to_string())
        }
        AgentArtifactLookupTargetWire::Names { names } => {
            let meta = read_meta(artifact_dir)?;
            name_match_kind(&meta, names)
        }
        AgentArtifactLookupTargetWire::Session { names } => {
            let meta = read_meta(artifact_dir)?;
            let session = lookup_session_from_meta(&meta)?;
            names.contains(&session).then(|| "session".to_string())
        }
        AgentArtifactLookupTargetWire::Clan { names } => {
            let meta = read_meta(artifact_dir)?;
            let clan = lookup_clan_from_meta(&meta)?;
            names.contains(&clan).then(|| "clan".to_string())
        }
    }
}

fn name_match_kind(meta: &AgentMetaWire, names: &[String]) -> Option<String> {
    let name_set: BTreeSet<&str> = names.iter().map(String::as_str).collect();
    if meta
        .name
        .as_deref()
        .is_some_and(|name| name_set.contains(name))
    {
        return Some("exact".to_string());
    }
    if meta
        .workflow_name
        .as_deref()
        .is_some_and(|name| name_set.contains(name))
    {
        return Some("workflow".to_string());
    }
    None
}

fn read_meta(artifact_dir: &Path) -> Option<AgentMetaWire> {
    let bytes = fs::read(artifact_dir.join("agent_meta.json")).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value.as_object().map(agent_meta_from_object)
}

fn fallback_scan_options() -> AgentArtifactScanOptionsWire {
    AgentArtifactScanOptionsWire {
        include_prompt_step_markers: false,
        include_raw_prompt_snippets: false,
        newest_first: true,
        only_workflow_dirs: vec![ACE_RUN_WORKFLOW_DIR.to_string()],
        max_records: Some(LOOKUP_FALLBACK_BUDGET),
        ..AgentArtifactScanOptionsWire::default()
    }
}

fn record_is_hidden(record: &AgentArtifactRecordWire) -> bool {
    record.agent_meta.as_ref().is_some_and(|meta| meta.hidden)
        || record.done.as_ref().is_some_and(|done| done.hidden)
}

fn hit_from_record(
    record: &AgentArtifactRecordWire,
    match_kind: &str,
) -> Option<AgentArtifactLookupHitWire> {
    let meta = record.agent_meta.as_ref()?;
    let lookup_session = lookup_session_from_meta(meta);
    let clan = lookup_clan_from_meta(meta);
    let generation = meta
        .agent_clan_generation
        .clone()
        .or_else(|| meta.parent_timestamp.clone())
        .or_else(|| Some(record.timestamp.clone()));
    Some(AgentArtifactLookupHitWire {
        artifact_dir: record.artifact_dir.clone(),
        project_name: record.project_name.clone(),
        workflow_dir_name: record.workflow_dir_name.clone(),
        timestamp: record.timestamp.clone(),
        agent_name: meta.name.clone(),
        workflow_name: meta.workflow_name.clone(),
        agent_session: meta.agent_session.clone(),
        lookup_session,
        agent_clan: clan,
        agent_clan_generation: generation.filter(|_| {
            meta.agent_clan.is_some() || meta.agent_session_parallel
        }),
        parent_timestamp: meta.parent_timestamp.clone(),
        has_done_marker: record.has_done_marker,
        has_running_marker: record.running.is_some(),
        hidden: record_is_hidden(record),
        match_kind: Some(match_kind.to_string()),
    })
}

/// Visit only recognized layout levels, charging every directory entry before
/// inspecting it. Never collect or sort an unbounded archive before truncation.
fn walk_lookup_dirs(
    projects_root: &Path,
    directory: &Path,
    level: u8,
    query: &AgentArtifactLookupQueryWire,
    options: &AgentArtifactScanOptionsWire,
    seen: &mut BTreeSet<String>,
    recovery: &mut LookupRecovery,
) {
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            if error.kind() != std::io::ErrorKind::NotFound {
                recovery.truncated = true;
            }
            return;
        }
    };
    for entry in entries {
        if recovery.dirs_visited >= u64::from(LOOKUP_FALLBACK_BUDGET) {
            recovery.truncated = true;
            return;
        }
        recovery.dirs_visited += 1;
        let Ok(entry) = entry else {
            recovery.truncated = true;
            continue;
        };
        let Ok(kind) = entry.file_type() else {
            recovery.truncated = true;
            continue;
        };
        if !kind.is_dir() && !(kind.is_symlink() && entry.path().is_dir()) {
            continue;
        }
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if level == 0 && !name.starts_with('.') {
            walk_lookup_dirs(
                projects_root,
                &path.join("artifacts").join(ACE_RUN_WORKFLOW_DIR),
                1,
                query,
                options,
                seen,
                recovery,
            );
        } else if (level == 1 || level == 3) && is_artifact_timestamp(&name) {
            consider_dir(projects_root, &path, query, options, seen, recovery);
        } else if (level == 1 && name.len() == 6
            || level == 2 && name.len() == 2)
            && name.bytes().all(|b| b.is_ascii_digit())
        {
            walk_lookup_dirs(
                projects_root,
                &path,
                level + 1,
                query,
                options,
                seen,
                recovery,
            );
        } else if level == 1 && !name.starts_with('.') {
            // Legacy unsharded artifacts also use non-timestamp suffixes.
            consider_dir(projects_root, &path, query, options, seen, recovery);
        }
        if recovery.truncated {
            return;
        }
    }
}

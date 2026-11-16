//! Bounded unavailable-index recovery for identity lookups.
//!
//! Direct artifact pointers are tried first. Remaining work is a newest-first
//! ace-run walk capped at [`LOOKUP_FALLBACK_BUDGET`] directory visits and
//! hydrations, with prompt/step reads disabled.

use super::lookup::{
    canonical_path, AgentArtifactLookupHitWire, AgentArtifactLookupQueryWire,
    AgentArtifactLookupTargetWire,
};
use super::lookup_identity::{lookup_clan_from_meta, lookup_session_from_meta};
use crate::agent_scan::layout::{is_artifact_timestamp, ACE_RUN_WORKFLOW_DIR};
use crate::agent_scan::scanner::scan_agent_artifact_dir;
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentArtifactScanOptionsWire, AgentMetaWire,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

pub(super) const LOOKUP_FALLBACK_BUDGET: u32 = 200;

pub(super) struct LookupRecovery {
    pub(super) hits: Vec<AgentArtifactLookupHitWire>,
    pub(super) truncated: bool,
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
        dirs_visited: 0,
        records_decoded: 0,
    };
    if !projects_root.is_dir() {
        return recovery;
    }

    let options = fallback_scan_options();
    let mut seen = BTreeSet::new();

    if let AgentArtifactLookupTargetWire::Pointers { artifact_dirs } =
        &query.target
    {
        for artifact_dir in artifact_dirs {
            if artifact_dir.is_empty() {
                continue;
            }
            let path = PathBuf::from(canonical_path(artifact_dir));
            consider_dir(
                projects_root,
                &path,
                query,
                &options,
                &mut seen,
                &mut recovery,
            );
        }
        return recovery;
    }

    let dirs = newest_ace_run_dirs(
        projects_root,
        LOOKUP_FALLBACK_BUDGET.saturating_add(1),
    );
    if dirs.len() > LOOKUP_FALLBACK_BUDGET as usize {
        recovery.truncated = true;
    }
    for dir in dirs.into_iter().take(LOOKUP_FALLBACK_BUDGET as usize) {
        recovery.dirs_visited += 1;
        consider_dir(
            projects_root,
            &dir,
            query,
            &options,
            &mut seen,
            &mut recovery,
        );
        if recovery.records_decoded >= u64::from(LOOKUP_FALLBACK_BUDGET) {
            recovery.truncated = true;
            break;
        }
    }
    recovery
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
    serde_json::from_slice(&bytes).ok()
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

fn newest_ace_run_dirs(projects_root: &Path, limit: u32) -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    let Ok(projects) = fs::read_dir(projects_root) else {
        return dirs;
    };
    let mut project_dirs: Vec<PathBuf> = projects
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    project_dirs.sort();
    for project_dir in project_dirs {
        let project_name = project_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if project_name.starts_with('.') {
            continue;
        }
        let workflow_dir =
            project_dir.join("artifacts").join(ACE_RUN_WORKFLOW_DIR);
        collect_newest_workflow_dirs(&workflow_dir, limit, &mut dirs);
        if dirs.len() >= limit as usize {
            break;
        }
    }
    dirs
}

fn collect_newest_workflow_dirs(
    workflow_dir: &Path,
    limit: u32,
    dirs: &mut Vec<PathBuf>,
) {
    let Ok(entries) = fs::read_dir(workflow_dir) else {
        return;
    };
    let mut month_dirs = Vec::new();
    let mut legacy = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if is_month_shard(&name) {
            month_dirs.push((name, path));
        } else if is_artifact_timestamp(&name) {
            legacy.push((name, path));
        }
    }
    month_dirs.sort_by(|left, right| right.0.cmp(&left.0));
    for (_, month_dir) in month_dirs {
        collect_newest_day_dirs(&month_dir, limit, dirs);
        if dirs.len() >= limit as usize {
            return;
        }
    }
    if !legacy.is_empty() {
        push_newest_limited(legacy, limit, dirs);
    }
}

fn collect_newest_day_dirs(
    month_dir: &Path,
    limit: u32,
    dirs: &mut Vec<PathBuf>,
) {
    let Ok(days) = fs::read_dir(month_dir) else {
        return;
    };
    let mut day_dirs = Vec::new();
    for entry in days.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.len() == 2 && name.bytes().all(|b| b.is_ascii_digit()) {
            day_dirs.push((name, path));
        }
    }
    day_dirs.sort_by(|left, right| right.0.cmp(&left.0));
    for (_, day_dir) in day_dirs {
        let Ok(artifacts) = fs::read_dir(&day_dir) else {
            continue;
        };
        let mut timestamps = Vec::new();
        for entry in artifacts.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let timestamp = entry.file_name().to_string_lossy().into_owned();
            if is_artifact_timestamp(&timestamp) {
                timestamps.push((timestamp, path));
            }
        }
        push_newest_limited(timestamps, limit, dirs);
        if dirs.len() >= limit as usize {
            return;
        }
    }
}

fn push_newest_limited(
    mut candidates: Vec<(String, PathBuf)>,
    limit: u32,
    dirs: &mut Vec<PathBuf>,
) {
    candidates.sort_by(|left, right| right.0.cmp(&left.0));
    for (_, path) in candidates {
        if dirs.len() >= limit as usize {
            return;
        }
        dirs.push(path);
    }
}

fn is_month_shard(value: &str) -> bool {
    value.len() == 6 && value.bytes().all(|b| b.is_ascii_digit())
}

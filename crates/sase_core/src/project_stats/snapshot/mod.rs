//! Committed-tree code snapshots with an incremental per-path blob cache.

mod attributes;
mod blobs;
mod cache;
mod tree;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};

use self::attributes::AttrSession;
use self::cache::{CachedBlobFacts, CodeSnapshotCacheEntry};
use self::tree::TreeEntry;
use super::wire::ProjectStatsBudgetWire;
use crate::project_stats::classify::{
    classify_file, classify_file_evidenced, BlobEvidence, Classification,
    CommittedAttributes, SkippedEntry,
};
use crate::project_stats::counter;
use crate::project_stats::git::{self, ResolvedRepo};
use crate::project_stats::wire::{
    ProjectStatsCodeCounterWire, ProjectStatsExclusionCountWire,
    ProjectStatsExclusionReasonCountWire, ProjectStatsExclusionReasonWire,
    ProjectStatsFileExclusionWire, ProjectStatsFileRoleWire,
    ProjectStatsIssueWire, ProjectStatsLanguageWire, ProjectStatsRepoSpecWire,
    ProjectStatsRoleCountWire, ProjectStatsSnapshotWire,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CodeSnapshot {
    pub snapshot: ProjectStatsSnapshotWire,
    pub tree_oid: String,
    pub blob_oids_requested: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MeasuredPath {
    pub path: Vec<u8>,
    pub mode: u32,
    pub oid: String,
    pub size: u64,
    pub attributes: CommittedAttributes,
    pub classification: Classification,
    pub selected_lines: Option<u64>,
    pub nonblank_lines: Option<u64>,
}

/// Measure one resolved repository at its committed tip.
pub(crate) fn measure_code_snapshot(
    repo: &ResolvedRepo,
    spec: &ProjectStatsRepoSpecWire,
    budget: &ProjectStatsBudgetWire,
    cache_dir: &str,
    counter: ProjectStatsCodeCounterWire,
) -> Result<CodeSnapshot, ProjectStatsIssueWire> {
    if repo.tip.is_empty() {
        return Ok(CodeSnapshot {
            snapshot: empty_snapshot(counter),
            tree_oid: String::new(),
            blob_oids_requested: Vec::new(),
        });
    }
    if counter == ProjectStatsCodeCounterWire::Sloc {
        return Err(issue(
            spec,
            "counter_unavailable",
            "the sloc counter is not enabled".to_owned(),
            Some("Use counter nonblank until the tokei dependency gate lands."),
        ));
    }

    let tree_oid = tree::read_tree_oid(repo, spec, budget)?;
    let key = cache::cache_key(repo, counter);
    let cache_path =
        (!cache_dir.is_empty()).then(|| cache::cache_path(cache_dir, &key));
    let cached = cache_path
        .as_deref()
        .and_then(cache::load)
        .filter(|entry| entry.matches_repo(&key, repo, counter));
    if let Some(entry) = cached.as_deref() {
        if entry.tree_oid() == tree_oid {
            return Ok(CodeSnapshot {
                snapshot: entry.snapshot().clone(),
                tree_oid,
                blob_oids_requested: Vec::new(),
            });
        }
    }

    let entries = tree::list_tree(repo, spec, budget)?;
    let attr_paths: Vec<Vec<u8>> = entries
        .iter()
        .filter(|entry| entry.is_regular_blob())
        .map(|entry| entry.path.clone())
        .collect();
    let session = AttrSession::open(repo, spec, budget)?;
    let attributes = session.lookup(repo, spec, budget, &attr_paths)?;

    let previous_rows = previous_rows(cached.as_deref());
    let mut blob_facts = previous_blobs(cached.as_deref());
    let mut measured = Vec::with_capacity(entries.len());
    let mut pending: Vec<PendingRead> = Vec::new();

    for entry in &entries {
        match measure_entry(entry, &attributes, &previous_rows, &blob_facts) {
            EntryDecision::Done(row) => measured.push(row),
            EntryDecision::Read(pending_read) => pending.push(pending_read),
        }
    }

    let mut wanted: Vec<(String, u64)> = Vec::new();
    let mut seen = HashMap::new();
    for item in &pending {
        if let Some(index) = seen.get(&item.oid) {
            let index: usize = *index;
            wanted[index].1 = wanted[index].1.max(item.size);
            continue;
        }
        seen.insert(item.oid.clone(), wanted.len());
        wanted.push((item.oid.clone(), item.size));
    }
    let blobs = blobs::read_blobs(repo, spec, budget, &wanted)?;
    let blob_oids_requested = wanted.into_iter().map(|(oid, _)| oid).collect();

    for item in pending {
        let Some(bytes) = blobs.get(&item.oid) else {
            return Err(issue(
                spec,
                "blob_unavailable",
                format!("blob {} was not returned by cat-file", item.oid),
                None,
            ));
        };
        if bytes.len() as u64 != item.size {
            return Err(issue(
                spec,
                "blob_unavailable",
                format!(
                    "blob {} length {} does not match the tree size {}",
                    item.oid,
                    bytes.len(),
                    item.size
                ),
                None,
            ));
        }
        let evidence = BlobEvidence::from_bytes(bytes);
        let classification = classify_file(
            &item.path,
            item.mode,
            item.size,
            item.attributes,
            Some(bytes),
        );
        let (selected_lines, nonblank_lines) =
            counts_for(counter, &classification, bytes)?;
        blob_facts.insert(
            item.oid.clone(),
            CachedBlobFacts {
                oid: item.oid.clone(),
                size: item.size,
                evidence,
                nonblank_lines: nonblank_lines.unwrap_or(0),
            },
        );
        measured.push(MeasuredPath {
            path: item.path,
            mode: item.mode,
            oid: item.oid,
            size: item.size,
            attributes: item.attributes,
            classification,
            selected_lines,
            nonblank_lines,
        });
    }

    let snapshot = assemble(&measured, counter);
    if let Some(path) = cache_path.as_deref() {
        let entry = CodeSnapshotCacheEntry::new(
            key,
            repo,
            tree_oid.clone(),
            counter,
            snapshot.clone(),
            measured,
            blob_facts,
        );
        cache::persist(path, &entry);
    }
    Ok(CodeSnapshot {
        snapshot,
        tree_oid,
        blob_oids_requested,
    })
}

struct PendingRead {
    path: Vec<u8>,
    mode: u32,
    oid: String,
    size: u64,
    attributes: CommittedAttributes,
}

enum EntryDecision {
    Done(MeasuredPath),
    Read(PendingRead),
}

fn measure_entry(
    entry: &TreeEntry,
    attributes: &HashMap<Vec<u8>, CommittedAttributes>,
    previous_rows: &HashMap<Vec<u8>, MeasuredPath>,
    blob_facts: &HashMap<String, CachedBlobFacts>,
) -> EntryDecision {
    let mode = entry.classify_mode();
    let attrs = if entry.is_regular_blob() {
        attributes.get(&entry.path).copied().unwrap_or_default()
    } else {
        CommittedAttributes::default()
    };
    if let Some(previous) = previous_rows.get(&entry.path) {
        if previous.oid == entry.oid
            && previous.mode == mode
            && previous.size == entry.size
            && previous.attributes == attrs
        {
            return EntryDecision::Done(previous.clone());
        }
    }

    if entry.is_gitlink() {
        return EntryDecision::Done(MeasuredPath {
            path: entry.path.clone(),
            mode,
            oid: entry.oid.clone(),
            size: entry.size,
            attributes: attrs,
            classification: Classification::Skipped(SkippedEntry::Gitlink),
            selected_lines: None,
            nonblank_lines: None,
        });
    }
    if entry.is_symlink() {
        return EntryDecision::Done(MeasuredPath {
            path: entry.path.clone(),
            mode,
            oid: entry.oid.clone(),
            size: entry.size,
            attributes: attrs,
            classification: Classification::Skipped(SkippedEntry::Symlink),
            selected_lines: None,
            nonblank_lines: None,
        });
    }
    if !entry.is_regular_blob() {
        return EntryDecision::Done(MeasuredPath {
            path: entry.path.clone(),
            mode,
            oid: entry.oid.clone(),
            size: entry.size,
            attributes: attrs,
            classification: Classification::Skipped(
                SkippedEntry::UnsupportedMode,
            ),
            selected_lines: None,
            nonblank_lines: None,
        });
    }

    let without_blob =
        classify_file(&entry.path, mode, entry.size, attrs, None);
    if matches!(
        without_blob,
        Classification::Excluded { .. } | Classification::Skipped(_)
    ) {
        return EntryDecision::Done(MeasuredPath {
            path: entry.path.clone(),
            mode,
            oid: entry.oid.clone(),
            size: entry.size,
            attributes: attrs,
            classification: without_blob,
            selected_lines: None,
            nonblank_lines: None,
        });
    }

    if let Some(facts) = blob_facts.get(&entry.oid) {
        if facts.size == entry.size {
            let classification = classify_file_evidenced(
                &entry.path,
                mode,
                entry.size,
                attrs,
                Some(&facts.evidence),
            );
            let (selected_lines, nonblank_lines) = match &classification {
                Classification::Included { .. } => {
                    (Some(facts.nonblank_lines), Some(facts.nonblank_lines))
                }
                _ => (None, None),
            };
            return EntryDecision::Done(MeasuredPath {
                path: entry.path.clone(),
                mode,
                oid: entry.oid.clone(),
                size: entry.size,
                attributes: attrs,
                classification,
                selected_lines,
                nonblank_lines,
            });
        }
    }

    EntryDecision::Read(PendingRead {
        path: entry.path.clone(),
        mode,
        oid: entry.oid.clone(),
        size: entry.size,
        attributes: attrs,
    })
}

fn counts_for(
    policy: ProjectStatsCodeCounterWire,
    classification: &Classification,
    bytes: &[u8],
) -> Result<(Option<u64>, Option<u64>), ProjectStatsIssueWire> {
    match classification {
        Classification::Included { language, .. } => {
            let counted = counter::count_blob(
                policy,
                language.as_deref(),
                bytes,
            )
            .map_err(|error| ProjectStatsIssueWire {
                code: "counter_unavailable".to_owned(),
                message: error.to_string(),
                hint: Some(
                    "Use counter nonblank until the tokei dependency gate lands."
                        .to_owned(),
                ),
                ..ProjectStatsIssueWire::default()
            })?;
            Ok((Some(counted.selected_lines), Some(counted.nonblank_lines)))
        }
        _ => Ok((None, None)),
    }
}

fn previous_rows(
    cached: Option<&CodeSnapshotCacheEntry>,
) -> HashMap<Vec<u8>, MeasuredPath> {
    let mut rows = HashMap::new();
    let Some(entry) = cached else {
        return rows;
    };
    for row in entry.rows() {
        if let Some(measured) = row.to_measured() {
            rows.insert(measured.path.clone(), measured);
        }
    }
    rows
}

fn previous_blobs(
    cached: Option<&CodeSnapshotCacheEntry>,
) -> HashMap<String, CachedBlobFacts> {
    let mut blobs = HashMap::new();
    let Some(entry) = cached else {
        return blobs;
    };
    for facts in entry.blobs() {
        blobs.insert(facts.oid.clone(), facts.clone());
    }
    blobs
}

fn assemble(
    rows: &[MeasuredPath],
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsSnapshotWire {
    let mut role_files: BTreeMap<ProjectStatsFileRoleWire, u64> =
        BTreeMap::new();
    let mut role_lines: BTreeMap<ProjectStatsFileRoleWire, u64> =
        BTreeMap::new();
    for role in [
        ProjectStatsFileRoleWire::Source,
        ProjectStatsFileRoleWire::Tests,
        ProjectStatsFileRoleWire::Docs,
        ProjectStatsFileRoleWire::AgentContext,
        ProjectStatsFileRoleWire::ConfigData,
        ProjectStatsFileRoleWire::Other,
    ] {
        role_files.insert(role, 0);
        role_lines.insert(role, 0);
    }
    let mut exclusion_files: BTreeMap<ProjectStatsFileExclusionWire, u64> =
        BTreeMap::new();
    let mut exclusion_reasons: BTreeMap<
        ProjectStatsFileExclusionWire,
        BTreeMap<ProjectStatsExclusionReasonWire, u64>,
    > = BTreeMap::new();
    for exclusion in [
        ProjectStatsFileExclusionWire::Generated,
        ProjectStatsFileExclusionWire::Vendored,
        ProjectStatsFileExclusionWire::Binary,
    ] {
        exclusion_files.insert(exclusion, 0);
        exclusion_reasons.insert(exclusion, BTreeMap::new());
    }
    let mut languages: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut skipped_symlinks = 0u64;
    let mut skipped_gitlinks = 0u64;
    let mut tracked_files = 0u64;

    for row in rows {
        match &row.classification {
            Classification::Included { role, language } => {
                tracked_files += 1;
                let lines = role_lines_for(*role, row);
                *role_files.entry(*role).or_default() += 1;
                *role_lines.entry(*role).or_default() += lines;
                if matches!(
                    role,
                    ProjectStatsFileRoleWire::Source
                        | ProjectStatsFileRoleWire::Tests
                ) {
                    if let Some(name) = language {
                        let slot = languages.entry(name.clone()).or_default();
                        slot.0 += 1;
                        slot.1 += row.selected_lines.unwrap_or(0);
                    }
                }
            }
            Classification::Excluded { kind, reason } => {
                tracked_files += 1;
                *exclusion_files.entry(*kind).or_default() += 1;
                *exclusion_reasons
                    .entry(*kind)
                    .or_default()
                    .entry(*reason)
                    .or_default() += 1;
            }
            Classification::Skipped(SkippedEntry::Symlink) => {
                tracked_files += 1;
                skipped_symlinks += 1;
            }
            Classification::Skipped(SkippedEntry::Gitlink) => {
                skipped_gitlinks += 1;
            }
            Classification::Skipped(SkippedEntry::UnsupportedMode) => {}
        }
    }

    let tracked_lines = [
        ProjectStatsFileRoleWire::Source,
        ProjectStatsFileRoleWire::Tests,
        ProjectStatsFileRoleWire::Docs,
        ProjectStatsFileRoleWire::AgentContext,
        ProjectStatsFileRoleWire::ConfigData,
        ProjectStatsFileRoleWire::Other,
    ]
    .into_iter()
    .map(|role| role_lines.get(&role).copied().unwrap_or(0))
    .sum();

    ProjectStatsSnapshotWire {
        tracked_files,
        tracked_lines: Some(tracked_lines),
        test_files: Some(
            role_files
                .get(&ProjectStatsFileRoleWire::Tests)
                .copied()
                .unwrap_or(0),
        ),
        test_lines: Some(
            role_lines
                .get(&ProjectStatsFileRoleWire::Tests)
                .copied()
                .unwrap_or(0),
        ),
        doc_files: Some(
            role_files
                .get(&ProjectStatsFileRoleWire::Docs)
                .copied()
                .unwrap_or(0),
        ),
        doc_lines: Some(
            role_lines
                .get(&ProjectStatsFileRoleWire::Docs)
                .copied()
                .unwrap_or(0),
        ),
        agent_context_files: Some(
            role_files
                .get(&ProjectStatsFileRoleWire::AgentContext)
                .copied()
                .unwrap_or(0),
        ),
        agent_context_lines: Some(
            role_lines
                .get(&ProjectStatsFileRoleWire::AgentContext)
                .copied()
                .unwrap_or(0),
        ),
        lifetime_commits: None,
        lifetime_agent_commits: None,
        languages: Some(
            languages
                .into_iter()
                .map(|(name, (files, lines))| ProjectStatsLanguageWire {
                    name,
                    files,
                    lines,
                })
                .collect(),
        ),
        as_of_ts: None,
        counter: Some(counter),
        role_counts: Some(
            role_files
                .into_iter()
                .map(|(role, files)| ProjectStatsRoleCountWire {
                    role,
                    files,
                    lines: role_lines.get(&role).copied().unwrap_or(0),
                })
                .collect(),
        ),
        exclusion_counts: Some(
            exclusion_files
                .into_iter()
                .map(|(exclusion, files)| ProjectStatsExclusionCountWire {
                    exclusion,
                    files,
                    reasons: exclusion_reasons
                        .remove(&exclusion)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|(reason, count)| {
                            ProjectStatsExclusionReasonCountWire {
                                reason,
                                files: count,
                            }
                        })
                        .collect(),
                })
                .collect(),
        ),
        skipped_symlinks: Some(skipped_symlinks),
        skipped_gitlinks: Some(skipped_gitlinks),
        first_commit_ts: None,
        last_commit_ts: None,
    }
}

fn role_lines_for(role: ProjectStatsFileRoleWire, row: &MeasuredPath) -> u64 {
    match role {
        ProjectStatsFileRoleWire::Source | ProjectStatsFileRoleWire::Tests => {
            row.selected_lines.unwrap_or(0)
        }
        ProjectStatsFileRoleWire::Docs
        | ProjectStatsFileRoleWire::AgentContext
        | ProjectStatsFileRoleWire::ConfigData
        | ProjectStatsFileRoleWire::Other => row.nonblank_lines.unwrap_or(0),
    }
}

fn empty_snapshot(
    counter: ProjectStatsCodeCounterWire,
) -> ProjectStatsSnapshotWire {
    assemble(&[], counter)
}

pub(crate) fn issue(
    spec: &ProjectStatsRepoSpecWire,
    code: &str,
    message: String,
    hint: Option<&str>,
) -> ProjectStatsIssueWire {
    let mut issue = git::issue(spec, code, message);
    issue.hint = hint.map(str::to_owned);
    issue
}

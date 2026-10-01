//! Version classes, summaries, and commit provenance.
//!
//! [`classify_subjects`] fills `class`, `hidden_by_default`, `summary`,
//! `provenance`, `boilerplate`, and the memory-source part of `cause`
//! for every version of [`derive_subjects`] output. Blob text comes
//! from [`read_blobs`]; commit messages come from the file-history
//! index the subjects were derived from, so this performs no commit
//! walk of its own. Missing blobs never fail the call: the version
//! still classifies from its file-history kind with empty prose
//! stats. Assets skip `compare_prose` entirely.
//!
//! The priority list is the plan's §3 order: deleted, created, moved,
//! promoted/demoted, frontmatter, reflow, whitespace, instruction
//! roles, regenerated, authored. Instruction `config` detection needs
//! a `diff-tree` over paths outside the walk, so a version with no
//! memory source stays `regen_only` here and `causes-feed` refines it
//! into `config` when a config or renderer path matched.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::subjects::memory_history_pathspecs;
use super::wire::{
    MemoryHistoryCauseSourceWire, MemoryHistoryClassWire, MemoryHistoryError,
    MemoryHistoryFooterTagWire, MemoryHistoryProvenanceWire,
    MemoryHistoryScopeWire, MemoryHistorySubjectKindWire,
    MemoryHistorySubjectWire,
};
use crate::commit_footer::parse_commit_footer;
use crate::file_history::wire::{FileChangeKindWire, FileHistoryIndexWire};
use crate::file_history::{read_blobs, BlobReadBudget};
use crate::prose_diff::{compare_prose, ProseCompareRequestWire};
use crate::vcs_log::classify_commit_types;

/// Classify every version in *subjects* in place. *index* is the
/// file-history index the subjects were derived from (commit-message
/// source); *repo* is the checkout for blob reads. Rows are never
/// reordered.
pub fn classify_subjects(
    repo: &Path,
    index: &FileHistoryIndexWire,
    subjects: &mut [MemoryHistorySubjectWire],
    scope: &MemoryHistoryScopeWire,
) -> Result<(), MemoryHistoryError> {
    let _ = memory_history_pathspecs(scope)?;
    let messages = commit_messages(index);
    let (memory_commits, memory_sources) = memory_source_map(subjects);
    let managed = instruction_managed(scope);

    let blobs = read_version_blobs(repo, subjects)?;

    for subject in subjects.iter_mut() {
        let is_instructions =
            subject.kind == MemoryHistorySubjectKindWire::Instructions;
        let is_asset = subject.kind == MemoryHistorySubjectKindWire::Asset;
        let managed_flag = managed.get(&subject.id).copied();
        for version in subject.versions.iter_mut() {
            let current = version
                .blob_oid
                .as_deref()
                .and_then(|oid| blobs.get(oid))
                .and_then(Option::as_deref);
            let previous = version
                .prev_blob_oid
                .as_deref()
                .and_then(|oid| blobs.get(oid))
                .and_then(Option::as_deref);
            let comparison = if is_asset
                || version.kind == FileChangeKindWire::Created
                || version.kind == FileChangeKindWire::Deleted
            {
                None
            } else {
                match (previous, current) {
                    (Some(base), Some(target)) => {
                        Some(compare_prose(&ProseCompareRequestWire {
                            base: base.to_string(),
                            target: target.to_string(),
                            format: "markdown".to_string(),
                            context_lines: 3,
                        }))
                    }
                    _ => None,
                }
            };

            let class = decide_class(
                version.kind,
                version.blob_oid.as_deref(),
                version.prev_blob_oid.as_deref(),
                comparison.as_ref(),
                current,
                previous,
                is_instructions,
                managed_flag,
                memory_commits.contains(&version.commit),
                subject.generated,
            );
            version.class = class;
            version.hidden_by_default = matches!(
                class,
                MemoryHistoryClassWire::Moved
                    | MemoryHistoryClassWire::Reflow
                    | MemoryHistoryClassWire::Whitespace
            );

            fill_summary(
                version,
                class,
                is_asset,
                comparison.as_ref(),
                current,
                previous,
            );
            fill_provenance(version, &messages);
            version.boilerplate = is_boilerplate(&version.provenance.subject);
            let sources = memory_sources.get(&version.commit).cloned();
            fill_cause(version, is_instructions, class, sources);
        }
    }
    Ok(())
}

/// Commit SHA to `(subject, body)` from the file-history index.
fn commit_messages(
    index: &FileHistoryIndexWire,
) -> BTreeMap<String, (String, String)> {
    let mut map = BTreeMap::new();
    for lineage in &index.lineages {
        for version in &lineage.versions {
            map.entry(version.commit.clone()).or_insert_with(|| {
                (version.subject.clone(), version.body.clone())
            });
        }
    }
    map
}

/// Commits touched by a non-generated note, web, or strand, plus the
/// `(subject id, ordinal)` witnesses behind each commit for the
/// instruction `cause.sources`.
fn memory_source_map(
    subjects: &[MemoryHistorySubjectWire],
) -> (
    BTreeSet<String>,
    BTreeMap<String, Vec<MemoryHistoryCauseSourceWire>>,
) {
    let mut commits = BTreeSet::new();
    let mut sources: BTreeMap<String, Vec<MemoryHistoryCauseSourceWire>> =
        BTreeMap::new();
    for subject in subjects {
        let is_memory = matches!(
            subject.kind,
            MemoryHistorySubjectKindWire::Note
                | MemoryHistorySubjectKindWire::Web
                | MemoryHistorySubjectKindWire::Strand
        ) && !subject.generated;
        if !is_memory {
            continue;
        }
        for version in &subject.versions {
            commits.insert(version.commit.clone());
            sources.entry(version.commit.clone()).or_default().push(
                MemoryHistoryCauseSourceWire {
                    subject_id: subject.id.clone(),
                    ordinal: version.ordinal,
                },
            );
        }
    }
    for witnesses in sources.values_mut() {
        witnesses.sort_by(|left, right| {
            left.subject_id
                .cmp(&right.subject_id)
                .then(left.ordinal.cmp(&right.ordinal))
        });
    }
    (commits, sources)
}

/// Instruction subject id to its entry's `managed` flag.
fn instruction_managed(
    scope: &MemoryHistoryScopeWire,
) -> BTreeMap<String, bool> {
    scope
        .instruction_files
        .iter()
        .map(|entry| {
            (
                format!("instructions:{}/{}", scope.scope_key, entry.dir),
                entry.managed,
            )
        })
        .collect()
}

/// Blob bytes by lowercase OID for every OID named by *subjects*.
/// Absent OIDs map to `None`; a git failure still errors.
fn read_version_blobs(
    repo: &Path,
    subjects: &[MemoryHistorySubjectWire],
) -> Result<BTreeMap<String, Option<String>>, MemoryHistoryError> {
    let mut oids: BTreeSet<String> = BTreeSet::new();
    for subject in subjects {
        for version in &subject.versions {
            for oid in [&version.blob_oid, &version.prev_blob_oid]
                .into_iter()
                .flatten()
            {
                oids.insert(oid.to_ascii_lowercase());
            }
        }
    }
    let ordered: Vec<String> = oids.into_iter().collect();
    let bodies = read_blobs(repo, &ordered, &BlobReadBudget::default())?;
    Ok(ordered
        .into_iter()
        .zip(bodies)
        .map(|(oid, bytes)| {
            (
                oid,
                bytes.map(|raw| String::from_utf8_lossy(&raw).into_owned()),
            )
        })
        .collect())
}

#[allow(clippy::too_many_arguments)]
fn decide_class(
    kind: FileChangeKindWire,
    blob_oid: Option<&str>,
    prev_blob_oid: Option<&str>,
    comparison: Option<&crate::prose_diff::ProseComparisonWire>,
    current: Option<&str>,
    previous: Option<&str>,
    is_instructions: bool,
    managed: Option<bool>,
    has_memory_source: bool,
    generated: bool,
) -> MemoryHistoryClassWire {
    if kind == FileChangeKindWire::Deleted {
        return MemoryHistoryClassWire::Deleted;
    }
    if kind == FileChangeKindWire::Created {
        return MemoryHistoryClassWire::Created;
    }
    if kind == FileChangeKindWire::Moved
        && moved_is_pure(blob_oid, prev_blob_oid, comparison, current, previous)
    {
        return MemoryHistoryClassWire::Moved;
    }
    if let Some(change) = comparison
        .as_ref()
        .and_then(|compared| compared.frontmatter.type_change.as_deref())
    {
        if change == "promoted" {
            return MemoryHistoryClassWire::Promoted;
        }
        if change == "demoted" {
            return MemoryHistoryClassWire::Demoted;
        }
    }
    if let Some(compared) = comparison {
        if compared.stats.frontmatter_only {
            return MemoryHistoryClassWire::Frontmatter;
        }
        match whitespace_shape(
            compared.stats.reflow_only,
            compared.stats.whitespace_only,
            current,
            previous,
        ) {
            WhitespaceShape::Reflow => {
                return MemoryHistoryClassWire::Reflow;
            }
            WhitespaceShape::Whitespace => {
                return MemoryHistoryClassWire::Whitespace;
            }
            WhitespaceShape::Neither => {}
        }
    }
    if is_instructions && kind != FileChangeKindWire::Moved {
        if managed == Some(false) {
            return MemoryHistoryClassWire::HandEdited;
        }
        if has_memory_source {
            return MemoryHistoryClassWire::Rendered;
        }
        return MemoryHistoryClassWire::RegenOnly;
    }
    if generated {
        return MemoryHistoryClassWire::Regenerated;
    }
    MemoryHistoryClassWire::Authored
}

/// A moved file-history row is a pure move when the blob is
/// unchanged, or when prose stats show only reflow or whitespace.
fn moved_is_pure(
    blob_oid: Option<&str>,
    prev_blob_oid: Option<&str>,
    comparison: Option<&crate::prose_diff::ProseComparisonWire>,
    current: Option<&str>,
    previous: Option<&str>,
) -> bool {
    match (blob_oid, prev_blob_oid) {
        (Some(next), Some(prev)) if next == prev => return true,
        _ => {}
    }
    match comparison {
        Some(compared) => {
            matches!(
                whitespace_shape(
                    compared.stats.reflow_only,
                    compared.stats.whitespace_only,
                    current,
                    previous,
                ),
                WhitespaceShape::Reflow | WhitespaceShape::Whitespace
            )
        }
        None => false,
    }
}

/// Reflow and whitespace flags overlap: a trailing-space edit and a
/// rewrapped paragraph both leave word counts at zero with identical
/// whitespace-folded text. Break the tie on line shape — equal
/// trimmed lines mean the edit never moved words across lines.
enum WhitespaceShape {
    Reflow,
    Whitespace,
    Neither,
}

fn whitespace_shape(
    reflow_only: bool,
    whitespace_only: bool,
    current: Option<&str>,
    previous: Option<&str>,
) -> WhitespaceShape {
    match (reflow_only, whitespace_only) {
        (true, false) => WhitespaceShape::Reflow,
        (false, true) => WhitespaceShape::Whitespace,
        (true, true) => {
            let same_trimmed = match (previous, current) {
                (Some(base), Some(target)) => {
                    trimmed_lines(base) == trimmed_lines(target)
                }
                _ => true,
            };
            if same_trimmed {
                WhitespaceShape::Whitespace
            } else {
                WhitespaceShape::Reflow
            }
        }
        _ => WhitespaceShape::Neither,
    }
}

fn trimmed_lines(text: &str) -> Vec<&str> {
    text.lines().map(str::trim).collect()
}

fn fill_summary(
    version: &mut super::wire::MemoryHistoryVersionWire,
    class: MemoryHistoryClassWire,
    is_asset: bool,
    comparison: Option<&crate::prose_diff::ProseComparisonWire>,
    current: Option<&str>,
    previous: Option<&str>,
) {
    let mut summary = super::wire::MemoryHistorySummaryWire::default();
    if let Some(compared) = comparison {
        summary.words_added = compared.stats.words_added as u64;
        summary.words_removed = compared.stats.words_removed as u64;
        summary.section_paths = section_paths(compared);
        summary.frontmatter_phrase = frontmatter_phrase(compared);
    }
    summary.created_words = match (class, is_asset, current) {
        (MemoryHistoryClassWire::Created, false, Some(text)) => {
            Some(count_words(text))
        }
        _ => None,
    };
    summary.volume = match class {
        MemoryHistoryClassWire::Moved
        | MemoryHistoryClassWire::Reflow
        | MemoryHistoryClassWire::Whitespace => 0,
        MemoryHistoryClassWire::Created => current
            .map(|text| if is_asset { 0 } else { count_words(text) })
            .unwrap_or(0),
        MemoryHistoryClassWire::Deleted => {
            if is_asset {
                0
            } else {
                previous.or(current).map(count_words).unwrap_or(0)
            }
        }
        _ if is_asset => 0,
        _ => summary.words_added + summary.words_removed,
    };
    version.summary = summary;
}

/// Deduped hunk breadcrumbs in first-seen order. A hunk's path
/// segments join with `" / "`; hunks outside any heading contribute
/// nothing.
fn section_paths(
    comparison: &crate::prose_diff::ProseComparisonWire,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut ordered = Vec::new();
    for hunk in &comparison.hunks {
        if hunk.section_path.is_empty() {
            continue;
        }
        let joined = hunk.section_path.join(" / ");
        if seen.insert(joined.clone()) {
            ordered.push(joined);
        }
    }
    ordered
}

/// Summary phrase for a frontmatter delta: `promoted reference → core`
/// style when `type_change` is set, otherwise `key: before → after`
/// joined with `", "` for the remaining entries.
fn frontmatter_phrase(
    comparison: &crate::prose_diff::ProseComparisonWire,
) -> Option<String> {
    let delta = &comparison.frontmatter;
    if let Some(change) = delta.type_change.as_deref() {
        let entry = delta.entries.iter().find(|item| item.key == "type");
        match entry {
            Some(item) => {
                let before = item.before.as_deref().unwrap_or("");
                let after = item.after.as_deref().unwrap_or("");
                return Some(format!("{change} {before} → {after}"));
            }
            None => return Some(change.to_string()),
        }
    }
    if delta.entries.is_empty() {
        return None;
    }
    Some(
        delta
            .entries
            .iter()
            .map(|item| {
                let before = item.before.as_deref().unwrap_or("");
                let after = item.after.as_deref().unwrap_or("");
                format!("{}: {before} → {after}", item.key)
            })
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// Whitespace-delimited word count of one document.
fn count_words(text: &str) -> u64 {
    text.split_whitespace().count() as u64
}

fn fill_provenance(
    version: &mut super::wire::MemoryHistoryVersionWire,
    messages: &BTreeMap<String, (String, String)>,
) {
    let (subject, body) =
        messages.get(&version.commit).cloned().unwrap_or_default();
    let message = if body.is_empty() {
        subject.clone()
    } else {
        format!("{subject}\n\n{body}")
    };
    let footer = parse_commit_footer(&message);
    let agent = footer
        .tags
        .iter()
        .find(|tag| tag.key == "AGENT")
        .map(|tag| tag.label.clone());
    let bead = footer
        .tags
        .iter()
        .find(|tag| tag.key == "BEAD")
        .map(|tag| tag.label.clone());
    let footer_tags = footer
        .tags
        .iter()
        .map(|tag| MemoryHistoryFooterTagWire {
            key: tag.key.clone(),
            label: tag.label.clone(),
        })
        .collect();
    let conventional_type = conventional_type(&subject);
    let commit_types =
        classify_commit_types(&message, version.parents.len() > 1);
    version.provenance = MemoryHistoryProvenanceWire {
        agent,
        bead,
        footer_tags,
        subject,
        conventional_type,
        commit_types,
    };
}

/// Leading `[A-Za-z]+` before `:` or `(`, lowercased. Absent when the
/// subject starts any other way.
fn conventional_type(subject: &str) -> Option<String> {
    let run: String = subject
        .chars()
        .take_while(|cell| cell.is_ascii_alphabetic())
        .collect();
    if run.is_empty() {
        return None;
    }
    match subject[run.len()..].chars().next() {
        Some(':') | Some('(') => Some(run.to_ascii_lowercase()),
        _ => None,
    }
}

/// Boilerplate commits are `chore: run sase init` renders or exactly
/// `chore: initialize sase memory`, after trimming.
fn is_boilerplate(subject: &str) -> bool {
    let trimmed = subject.trim();
    trimmed.starts_with("chore: run sase init")
        || trimmed == "chore: initialize sase memory"
}

fn fill_cause(
    version: &mut super::wire::MemoryHistoryVersionWire,
    is_instructions: bool,
    class: MemoryHistoryClassWire,
    sources: Option<Vec<MemoryHistoryCauseSourceWire>>,
) {
    if !is_instructions {
        return;
    }
    let witnesses = sources.unwrap_or_default();
    version.cause.sources = witnesses;
    version.cause.regen_only = class == MemoryHistoryClassWire::RegenOnly;
}

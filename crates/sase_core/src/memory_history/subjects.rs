//! Subject identity over a file-history index.
//!
//! [`derive_subjects`] derives note, web, strand, instructions, and
//! asset identity from `file_history` lineages: instruction subjects
//! are claimed from lineages that touch an `agents_path`, each shim
//! lineage folds into its directory's instruction subject by blob
//! equality (equal blobs alias, unequal blobs append a diverged row),
//! and every remaining lineage classifies from its latest path. No
//! `prose_diff` runs here; versions stay `unclassified` until
//! `classify` runs.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;

use super::wire::{
    MemoryHistoryClassWire, MemoryHistoryError, MemoryHistoryScopeWire,
    MemoryHistorySubjectKindWire, MemoryHistorySubjectWire,
    MemoryHistoryVersionWire,
};
use crate::file_history::{
    looks_like_full_sha, run_git_unchecked, safe_pathspec, FileLineageWire,
    FileVersionWire,
};

/// Per-command timeout for the shim-comparison probes.
const SHIM_PROBE_TIMEOUT: Duration = Duration::from_millis(10_000);
/// Stdout byte cap for the shim-comparison probes.
const SHIM_PROBE_MAX_BYTES: u64 = 64 * 1024;

/// Pathspecs for the `file_history` walk: the memory roots plus every
/// `agents_path` and shim path, sorted and deduped. Config paths and
/// renderer prefixes stay out of the walk. Rejects unsafe pathspecs.
pub fn memory_history_pathspecs(
    scope: &MemoryHistoryScopeWire,
) -> Result<Vec<String>, MemoryHistoryError> {
    let mut specs: Vec<String> = Vec::new();
    specs.extend(scope.memory_roots.iter().cloned());
    for entry in &scope.instruction_files {
        specs.push(entry.agents_path.clone());
        specs.extend(entry.shim_paths.iter().cloned());
    }
    specs.sort();
    specs.dedup();
    for spec in &specs {
        if !safe_pathspec(spec) {
            return Err(MemoryHistoryError::InvalidPath(spec.clone()));
        }
    }
    Ok(specs)
}

/// Derive subjects from a file-history index built over
/// [`memory_history_pathspecs`]. Never reimplements rename folding:
/// one lineage is one identity, and historical paths are aliases, not
/// extra subjects. Instruction subjects sort by `dir`; every other
/// subject sorts by id. Returns an error only for an invalid scope or
/// a failed shim-comparison probe.
pub fn derive_subjects(
    index: &crate::file_history::FileHistoryIndexWire,
    scope: &MemoryHistoryScopeWire,
) -> Result<Vec<MemoryHistorySubjectWire>, MemoryHistoryError> {
    if scope.scope_key.is_empty() {
        return Err(MemoryHistoryError::InvalidScope(
            "scope_key is empty".to_string(),
        ));
    }
    let _ = memory_history_pathspecs(scope)?;
    let repo = Path::new(&index.repo_root);

    let mut claimed: BTreeSet<u64> = BTreeSet::new();
    let mut subjects: Vec<MemoryHistorySubjectWire> = Vec::new();

    let mut entries = scope.instruction_files.clone();
    entries.sort_by(|left, right| left.dir.cmp(&right.dir));
    for entry in &entries {
        let agents_lineages: Vec<&FileLineageWire> = index
            .lineages
            .iter()
            .filter(|lineage| {
                lineage.paths.iter().any(|path| path == &entry.agents_path)
            })
            .collect();
        for lineage in &agents_lineages {
            claimed.insert(lineage.id);
        }
        let mut paths: Vec<String> = Vec::new();
        let mut versions: Vec<MemoryHistoryVersionWire> = Vec::new();
        for lineage in &agents_lineages {
            append_paths(&mut paths, &lineage.paths);
            versions.extend(
                lineage.versions.iter().map(|version| {
                    canonical_version(version, &entry.agents_path)
                }),
            );
        }
        let mut diverged_count: u64 = 0;
        for shim_path in &entry.shim_paths {
            let shim_lineages: Vec<&FileLineageWire> = index
                .lineages
                .iter()
                .filter(|lineage| {
                    lineage.paths.iter().any(|path| path == shim_path)
                })
                .collect();
            for lineage in &shim_lineages {
                claimed.insert(lineage.id);
            }
            append_paths(&mut paths, std::slice::from_ref(shim_path));
            for lineage in &shim_lineages {
                append_paths(&mut paths, &lineage.paths);
                if agents_lineages.iter().any(|agents| agents.id == lineage.id)
                {
                    continue;
                }
                for shim_version in &lineage.versions {
                    if let Some(row) = fold_shim_version(
                        repo,
                        &entry.agents_path,
                        &agents_lineages,
                        &mut versions,
                        shim_path,
                        shim_version,
                    )? {
                        versions.push(row);
                        diverged_count += 1;
                    }
                }
            }
        }
        sort_instruction_versions(&mut versions);
        assign_ordinals(&mut versions);
        subjects.push(MemoryHistorySubjectWire {
            id: format!("instructions:{}/{}", scope.scope_key, entry.dir),
            kind: MemoryHistorySubjectKindWire::Instructions,
            display_name: file_name(&entry.agents_path),
            generated: false,
            managed: entry.managed,
            template: entry.template,
            diverged_count,
            paths,
            versions,
        });
    }

    let latest = latest_paths_by_root(&index.lineages, &scope.memory_roots);
    let mut rest: Vec<MemoryHistorySubjectWire> = Vec::new();
    for lineage in &index.lineages {
        if claimed.contains(&lineage.id) {
            continue;
        }
        rest.push(classify_lineage(lineage, scope, &latest));
    }
    rest.sort_by(|left, right| left.id.cmp(&right.id));
    subjects.extend(rest);
    Ok(subjects)
}

/// Map every historical path, including shim paths, to its subject
/// id.
pub fn subject_path_aliases(
    subjects: &[MemoryHistorySubjectWire],
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for subject in subjects {
        for path in &subject.paths {
            map.insert(path.clone(), subject.id.clone());
        }
    }
    map
}

fn canonical_version(
    version: &FileVersionWire,
    source_path: &str,
) -> MemoryHistoryVersionWire {
    let mut row = base_version(version);
    row.source_path = source_path.to_string();
    row
}

fn base_version(version: &FileVersionWire) -> MemoryHistoryVersionWire {
    MemoryHistoryVersionWire {
        ordinal: 0,
        commit: version.commit.clone(),
        parents: version.parents.clone(),
        committer_time: version.committer_time,
        author_time: version.author_time,
        author_name: version.author_name.clone(),
        author_email: version.author_email.clone(),
        path: version.path.clone(),
        blob_oid: version.blob_oid.clone(),
        prev_blob_oid: version.prev_blob_oid.clone(),
        kind: version.kind,
        similarity: version.similarity,
        gap_before: version.gap_before,
        diverged: false,
        source_path: version.path.clone(),
        aliased_paths: Vec::new(),
        class: MemoryHistoryClassWire::Unclassified,
        hidden_by_default: false,
        summary: Default::default(),
        provenance: Default::default(),
        boilerplate: false,
        cause: Default::default(),
    }
}

fn diverged_version(
    version: &FileVersionWire,
    shim_path: &str,
) -> MemoryHistoryVersionWire {
    let mut row = base_version(version);
    row.diverged = true;
    row.source_path = shim_path.to_string();
    row
}

/// Fold one shim version into its instruction subject, returning the
/// diverged row to append or `None` when the shim aliased an
/// `AGENTS.md` version and added no row. Equal blobs append the shim
/// path to an `AGENTS.md` version's `aliased_paths`; unequal blobs, a
/// missing shim blob, or no `AGENTS.md` blob yet append a diverged
/// row. Blob equality is OID equality: the object store is
/// content-addressed, so equal OIDs are equal bytes and unequal OIDs
/// are different bytes without reading either body.
fn fold_shim_version(
    repo: &Path,
    agents_path: &str,
    agents_lineages: &[&FileLineageWire],
    versions: &mut [MemoryHistoryVersionWire],
    shim_path: &str,
    shim_version: &FileVersionWire,
) -> Result<Option<MemoryHistoryVersionWire>, MemoryHistoryError> {
    let Some(shim_oid) = shim_version.blob_oid.as_deref() else {
        return Ok(Some(diverged_version(shim_version, shim_path)));
    };
    let touched = agents_lineages
        .iter()
        .flat_map(|lineage| &lineage.versions)
        .find(|version| version.commit == shim_version.commit);
    let agents_oid =
        match touched.and_then(|version| version.blob_oid.as_deref()) {
            Some(oid) => Some(oid.to_string()),
            None => agents_blob_at(repo, &shim_version.commit, agents_path)?,
        };
    if agents_oid.as_deref() != Some(shim_oid) {
        return Ok(Some(diverged_version(shim_version, shim_path)));
    }
    let target = match touched {
        Some(_) => versions
            .iter_mut()
            .find(|row| !row.diverged && row.commit == shim_version.commit),
        None => versions.iter_mut().find(|row| {
            !row.diverged && row.blob_oid.as_deref() == Some(shim_oid)
        }),
    };
    match target {
        Some(row) => {
            if !row.aliased_paths.contains(&shim_path.to_string()) {
                row.aliased_paths.push(shim_path.to_string());
            }
            Ok(None)
        }
        // Truncated history: the blob predates the walk, so no row
        // carries it. Keep the alias on the oldest witness rather
        // than inventing a row.
        None => {
            let oldest = versions.iter_mut().filter(|row| !row.diverged).last();
            match oldest {
                Some(row) => {
                    if !row.aliased_paths.contains(&shim_path.to_string()) {
                        row.aliased_paths.push(shim_path.to_string());
                    }
                    Ok(None)
                }
                None => Ok(Some(diverged_version(shim_version, shim_path))),
            }
        }
    }
}

/// The `AGENTS.md` blob OID at *commit*: the empty result when the
/// path does not exist there yet. A non-zero git exit carries meaning
/// (no blob yet), so this uses the unchecked runner.
fn agents_blob_at(
    repo: &Path,
    commit: &str,
    agents_path: &str,
) -> Result<Option<String>, MemoryHistoryError> {
    let spec = format!("{commit}:{agents_path}");
    let result = run_git_unchecked(
        repo,
        &["rev-parse", "--verify", &spec],
        SHIM_PROBE_TIMEOUT,
        SHIM_PROBE_MAX_BYTES,
    )
    .map_err(|error| MemoryHistoryError::GitFailed(error.to_string()))?;
    if result.code != Some(0) {
        return Ok(None);
    }
    let oid = String::from_utf8_lossy(&result.stdout).trim().to_string();
    if looks_like_full_sha(&oid) {
        Ok(Some(oid))
    } else {
        Ok(None)
    }
}

/// Newest-first with a diverged shim row sharing its commit's place:
/// committer time descending, then path ascending so a rebuild is
/// deterministic.
fn sort_instruction_versions(versions: &mut [MemoryHistoryVersionWire]) {
    versions.sort_by(|left, right| {
        right
            .committer_time
            .cmp(&left.committer_time)
            .then(left.path.cmp(&right.path))
    });
}

/// Assign ordinals oldest-first over newest-first rows.
fn assign_ordinals(versions: &mut [MemoryHistoryVersionWire]) {
    let total = versions.len() as u64;
    for (index, row) in versions.iter_mut().enumerate() {
        row.ordinal = total - index as u64;
    }
}

fn append_paths(paths: &mut Vec<String>, extra: &[String]) {
    for path in extra {
        if !paths.contains(path) {
            paths.push(path.clone());
        }
    }
}

fn file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "AGENTS.md".to_string())
}

/// Latest lineage path per memory root: `(root, root-relative)` pairs
/// for the web/strand descriptor checks.
fn latest_paths_by_root(
    lineages: &[FileLineageWire],
    roots: &[String],
) -> BTreeSet<(String, String)> {
    let mut set = BTreeSet::new();
    for lineage in lineages {
        if let Some((root, relative)) = strip_root(&lineage.current_path, roots)
        {
            set.insert((root, relative));
        }
    }
    set
}

/// Strip the longest matching memory root (`root/` prefix).
fn strip_root(path: &str, roots: &[String]) -> Option<(String, String)> {
    let mut best: Option<(String, String)> = None;
    for root in roots {
        let prefix = format!("{root}/");
        let Some(relative) = path.strip_prefix(&prefix) else {
            continue;
        };
        if relative.is_empty() {
            continue;
        }
        let longer = match &best {
            None => true,
            Some((best_root, _)) => root.len() > best_root.len(),
        };
        if longer {
            best = Some((root.clone(), relative.to_string()));
        }
    }
    best
}

fn classify_lineage(
    lineage: &FileLineageWire,
    scope: &MemoryHistoryScopeWire,
    latest: &BTreeSet<(String, String)>,
) -> MemoryHistorySubjectWire {
    let current = &lineage.current_path;
    let stripped = strip_root(current, &scope.memory_roots);
    let (kind, id, display_name) = match &stripped {
        Some((root, relative)) if is_web(relative, root, latest) => {
            let slug = relative.strip_suffix(".md").unwrap_or(relative);
            (
                MemoryHistorySubjectKindWire::Web,
                format!("web:{}/{slug}", scope.scope_key),
                slug.to_string(),
            )
        }
        Some((root, relative)) if is_strand(relative, root, latest) => {
            let (web, file) =
                relative.rsplit_once('/').unwrap_or(("", relative));
            let slug = file.strip_suffix(".md").unwrap_or(file);
            (
                MemoryHistorySubjectKindWire::Strand,
                format!("strand:{}/{web}/{slug}", scope.scope_key),
                slug.to_string(),
            )
        }
        Some((_, relative))
            if relative.ends_with(".md") || relative.ends_with(".md.tmpl") =>
        {
            let name = relative
                .strip_suffix(".tmpl")
                .unwrap_or(relative)
                .strip_suffix(".md")
                .unwrap_or(relative);
            (
                MemoryHistorySubjectKindWire::Note,
                format!("note:{}/{name}", scope.scope_key),
                file_name(current),
            )
        }
        _ => (
            MemoryHistorySubjectKindWire::Asset,
            format!("asset:{}/{current}", scope.scope_key),
            file_name(current),
        ),
    };
    let generated = lineage.paths.iter().any(|path| {
        scope.generated_notes.iter().any(|generated| {
            generated == path
                || strip_root(path, &scope.memory_roots)
                    .is_some_and(|(_, relative)| &relative == generated)
        })
    });
    let mut versions: Vec<MemoryHistoryVersionWire> =
        lineage.versions.iter().map(base_version).collect();
    assign_ordinals(&mut versions);
    MemoryHistorySubjectWire {
        id,
        kind,
        display_name,
        generated,
        managed: false,
        template: false,
        diverged_count: 0,
        paths: lineage.paths.clone(),
        versions,
    }
}

/// Latest path is `{slug}.md` and some latest path under the same
/// root is `{slug}/{file}.md`.
fn is_web(
    relative: &str,
    root: &str,
    latest: &BTreeSet<(String, String)>,
) -> bool {
    let Some(slug) = relative.strip_suffix(".md") else {
        return false;
    };
    if slug.is_empty() || slug.contains('/') {
        return false;
    }
    let prefix = format!("{slug}/");
    latest.iter().any(|(other_root, other)| {
        other_root == root
            && other.starts_with(&prefix)
            && other.ends_with(".md")
    })
}

/// Latest path is `{web}/{slug}.md` and `{web}.md` is some lineage's
/// latest path under the same root.
fn is_strand(
    relative: &str,
    root: &str,
    latest: &BTreeSet<(String, String)>,
) -> bool {
    let Some(file) = relative.strip_suffix(".md") else {
        return false;
    };
    let Some((web, _)) = file.rsplit_once('/') else {
        return false;
    };
    if web.is_empty() {
        return false;
    }
    latest.contains(&(root.to_string(), format!("{web}.md")))
}

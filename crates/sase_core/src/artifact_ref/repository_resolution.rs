//! Repository-aware resolution for document-owned source-path targets.
//!
//! `resolve_document_file_path` (`file_roots.rs`) resolves against explicit,
//! caller-configured file roots, and `resolve_document` resolves typed
//! document payloads against their kind's sidecar root. Neither covers an
//! ordinary unqualified source path that a rendered document merely
//! mentions (a Swift source file named from a plan, for example): that path
//! must be found in the repository that actually owns it, never guessed
//! from a same-named file that happens to sit under the viewer's cwd.
//!
//! This module resolves such a path using the shared repository inventory
//! (`ArtifactRefContextWire.repositories`) plus optional caller-attached
//! provenance, in this order:
//!
//! 1. Optional owner `path_globs`. A denial is terminal and is never
//!    recovered by a later probe.
//! 2. `owner.source_directory` when that directory is valid provenance.
//! 3. Caller-attached checkout candidates, identified against the
//!    inventory (or the owner's repository name) rather than treated as a
//!    first-hit list of unrelated roots.
//! 4. Live checkouts of the same identified repository, including when
//!    every attached path is stale.
//! 5. A bounded suffix search across each candidate repository's first
//!    available checkout, for a path that has drifted under a renamed
//!    parent directory.
//!
//! Files and directories share this decision path. Distinct repositories
//! that both contain a matching path are ambiguous, not a first-hit guess;
//! copies of the same repository are ordered and then deduplicated. An
//! explicit owner revision must match the checkout HEAD (or peel to it)
//! before a worktree path is labeled exact; a historical blob that Git can
//! name is evidence for `unavailable_revision`, not a successful landing
//! of the current worktree.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use crate::artifact_file::{
    checkout_head_sha, git_object_exists_at_revision, peel_to_commit_sha,
};

use super::filter::ArtifactPathFilter;
use super::{
    validate_artifact_ref_context, validate_path_payload,
    ArtifactRefContextWire, ArtifactRefDocumentOwnerWire, ArtifactRefError,
    ArtifactRefRepositoryWire, ArtifactRefTargetCandidateWire,
    ArtifactRefTargetFailureCategoryWire, ArtifactRefTargetResolutionWire,
    ARTIFACT_REF_TARGET_RESOLUTION_WIRE_SCHEMA_VERSION,
};

/// Directory names never worth descending into during a bounded suffix
/// search: version-control internals and dependency/build output that
/// cannot contain an author-authored source path.
const SKIPPED_DIR_NAMES: &[&str] = &[
    ".git",
    ".hg",
    ".svn",
    "node_modules",
    "target",
    "dist",
    "build",
    ".venv",
    "venv",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    ".tox",
];

/// Total directory entries a bounded suffix search may visit across all
/// candidate repositories before giving up with a temporary-error outcome
/// rather than an unbounded filesystem walk.
const SUFFIX_SEARCH_MAX_ENTRIES: usize = 20_000;

/// Resolve *path*, an unqualified repo-relative source path named by a
/// rendered document, in the repository that owns that document.
pub fn resolve_document_source_target(
    path: &str,
    owner: &ArtifactRefDocumentOwnerWire,
    context: &ArtifactRefContextWire,
) -> Result<ArtifactRefTargetResolutionWire, ArtifactRefError> {
    resolve_with_budget(path, owner, context, SUFFIX_SEARCH_MAX_ENTRIES)
}

fn resolve_with_budget(
    path: &str,
    owner: &ArtifactRefDocumentOwnerWire,
    context: &ArtifactRefContextWire,
    suffix_budget: usize,
) -> Result<ArtifactRefTargetResolutionWire, ArtifactRefError> {
    validate_artifact_ref_context(context)?;
    let payload = normalize_payload(path)?;
    let filter = ArtifactPathFilter::compile(owner.path_globs.as_deref())?;
    let filter_text = posix_payload(&payload);
    if !filter.allows(&filter_text)? {
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::DeniedFiltered,
            Vec::new(),
            Some(format!(
                "source path {filter_text:?} is denied by owner path policy ({})",
                filter.summary()
            )),
        ));
    }

    let searches = build_repo_searches(owner, context);
    let any_live_checkout = searches
        .iter()
        .any(|search| search.bases.iter().any(|base| base.root.is_dir()));

    let mut candidates = Vec::new();
    let mut direct_hits = Vec::new();
    let mut revision_failures = Vec::new();
    if let Some(hit) = probe_source_relative(&payload, owner, &searches) {
        candidates.push(evidence_from_hit(&hit));
        match apply_revision(hit, owner.revision.as_deref()) {
            Probe::Hit(hit) => {
                if let Some(outcome) =
                    decide(vec![hit], owner.revision.as_deref())
                {
                    return Ok(outcome);
                }
            }
            Probe::RevisionUnavailable(hit) => {
                if let Some(search) =
                    searches.iter().find(|search| search.id == hit.id)
                {
                    match probe_repo_direct(
                        search,
                        &payload,
                        owner.revision.as_deref(),
                    ) {
                        Probe::Hit(same_repo) => {
                            if let Some(outcome) = decide(
                                vec![same_repo],
                                owner.revision.as_deref(),
                            ) {
                                return Ok(outcome);
                            }
                        }
                        Probe::RevisionUnavailable(same_repo) => {
                            return Ok(revision_unavailable(
                                owner.revision.as_deref(),
                                &[hit, same_repo],
                            ));
                        }
                        Probe::Miss(inspected) => {
                            candidates.extend(inspected);
                        }
                    }
                }
                return Ok(revision_unavailable(
                    owner.revision.as_deref(),
                    &[hit],
                ));
            }
            Probe::Miss(_) => {}
        }
    }
    for search in &searches {
        match probe_repo_direct(search, &payload, owner.revision.as_deref()) {
            Probe::Hit(hit) => {
                candidates.push(evidence_from_hit(&hit));
                direct_hits.push(hit);
            }
            Probe::RevisionUnavailable(hit) => {
                candidates.push(evidence_from_hit(&hit));
                revision_failures.push(hit);
            }
            Probe::Miss(inspected) => candidates.extend(inspected),
        }
    }

    if let Some(outcome) = decide(direct_hits, owner.revision.as_deref()) {
        return Ok(outcome);
    }
    if !revision_failures.is_empty() {
        return Ok(revision_unavailable(
            owner.revision.as_deref(),
            &revision_failures,
        ));
    }

    if searches.is_empty() || !any_live_checkout {
        let diagnostic = missing_checkout_diagnostic(owner, &searches);
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::MissingCheckout,
            candidates,
            Some(diagnostic),
        ));
    }

    if payload.components().count() < 2 {
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::ProvenMissing,
            candidates,
            Some("path was not found in any known checkout".to_string()),
        ));
    }

    let mut budget = suffix_budget;
    let mut suffix_hits = Vec::new();
    let mut suffix_revision_failures = Vec::new();
    for search in &searches {
        let Some(root) = search
            .bases
            .iter()
            .find(|base| base.root.is_dir())
            .map(|base| base.root.clone())
        else {
            continue;
        };
        let git_root = search
            .bases
            .iter()
            .find(|base| base.root == root)
            .map(|base| base.git_root.clone())
            .unwrap_or_else(|| root.clone());
        let mut found = Vec::new();
        if !collect_suffix_matches(&root, &payload, &mut budget, &mut found) {
            return Ok(unresolved(
                ArtifactRefTargetFailureCategoryWire::TemporaryError,
                candidates,
                Some(
                    "bounded repository search budget was exhausted"
                        .to_string(),
                ),
            ));
        }
        for path in found {
            let path = path.canonicalize().unwrap_or_else(|_| path.clone());
            let hit = Hit {
                id: search.id.clone(),
                path,
                git_root: git_root.clone(),
                evidence: "suffix_match",
                status: "drifted",
            };
            candidates.push(evidence_from_hit(&hit));
            match apply_revision(hit, owner.revision.as_deref()) {
                Probe::Hit(hit) => suffix_hits.push(hit),
                Probe::RevisionUnavailable(hit) => {
                    suffix_revision_failures.push(hit);
                }
                Probe::Miss(_) => {}
            }
        }
    }

    if let Some(outcome) = decide(suffix_hits, owner.revision.as_deref()) {
        return Ok(outcome);
    }
    if !suffix_revision_failures.is_empty() {
        return Ok(revision_unavailable(
            owner.revision.as_deref(),
            &suffix_revision_failures,
        ));
    }

    Ok(unresolved(
        ArtifactRefTargetFailureCategoryWire::ProvenMissing,
        candidates,
        Some("path was not found in any known checkout".to_string()),
    ))
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
enum RepoId {
    Named(String),
    Anonymous(PathBuf),
}

impl RepoId {
    fn wire_name(&self) -> Option<String> {
        match self {
            Self::Named(name) => Some(name.clone()),
            Self::Anonymous(_) => None,
        }
    }
}

struct SearchBase {
    root: PathBuf,
    git_root: PathBuf,
    evidence: &'static str,
}

struct RepoSearch {
    id: RepoId,
    bases: Vec<SearchBase>,
}

struct Hit {
    id: RepoId,
    path: PathBuf,
    git_root: PathBuf,
    evidence: &'static str,
    status: &'static str,
}

enum Probe {
    Hit(Hit),
    RevisionUnavailable(Hit),
    Miss(Vec<ArtifactRefTargetCandidateWire>),
}

fn normalize_payload(path: &str) -> Result<PathBuf, ArtifactRefError> {
    validate_path_payload("source", path)?;
    let normalized: PathBuf = Path::new(path)
        .components()
        .filter(|component| !matches!(component, Component::CurDir))
        .collect();
    if normalized.as_os_str().is_empty() {
        return Err(ArtifactRefError::validation(
            "source path must not be empty",
        ));
    }
    Ok(normalized)
}

fn posix_payload(path: &Path) -> String {
    path.components()
        .filter_map(|component| match component {
            Component::Normal(part) => Some(part.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn build_repo_searches(
    owner: &ArtifactRefDocumentOwnerWire,
    context: &ArtifactRefContextWire,
) -> Vec<RepoSearch> {
    let mut searches: Vec<RepoSearch> = Vec::new();
    for checkout in &owner.checkout_candidates {
        let root = PathBuf::from(checkout);
        let Some(id) = classify_attached(&root, owner, &context.repositories)
        else {
            continue;
        };
        let Some(canonical) = live_dir(&root) else {
            continue;
        };
        push_base(
            &mut searches,
            id,
            SearchBase {
                git_root: canonical.clone(),
                root: canonical,
                evidence: "attached_checkout",
            },
        );
    }

    let scoped_inventory: Vec<&ArtifactRefRepositoryWire> =
        match owner.repository.as_deref() {
            Some(name) => context
                .repositories
                .iter()
                .filter(|repo| repository_matches(repo, name))
                .collect(),
            None => context.repositories.iter().collect(),
        };
    for repo in scoped_inventory {
        let id = RepoId::Named(repo.name.clone());
        for checkout in &repo.checkout_paths {
            let root = PathBuf::from(checkout);
            let Some(canonical) = live_dir(&root) else {
                continue;
            };
            push_base(
                &mut searches,
                id.clone(),
                SearchBase {
                    git_root: canonical.clone(),
                    root: canonical,
                    evidence: "repository_checkout",
                },
            );
        }
    }
    searches
}

fn classify_attached(
    checkout: &Path,
    owner: &ArtifactRefDocumentOwnerWire,
    repositories: &[ArtifactRefRepositoryWire],
) -> Option<RepoId> {
    let normalized = normalize_path(checkout);
    if let Some(name) = inventory_name_for_path(&normalized, repositories) {
        if let Some(requested) = owner.repository.as_deref() {
            let matches_owner = repositories.iter().any(|repo| {
                repo.name == name && repository_matches(repo, requested)
            });
            if !matches_owner {
                return None;
            }
        }
        return Some(RepoId::Named(name));
    }
    if let Some(requested) = owner.repository.as_deref() {
        let name = canonical_repo_name(repositories, requested)
            .unwrap_or(requested)
            .to_string();
        return Some(RepoId::Named(name));
    }
    Some(RepoId::Anonymous(normalized))
}

fn inventory_name_for_path(
    path: &Path,
    repositories: &[ArtifactRefRepositoryWire],
) -> Option<String> {
    let mut best: Option<(usize, String)> = None;
    for repo in repositories {
        for checkout in &repo.checkout_paths {
            let root = normalize_path(Path::new(checkout));
            if path == root.as_path() || path.starts_with(&root) {
                let len = root.as_os_str().len();
                if best.as_ref().map_or(true, |(best_len, _)| len > *best_len) {
                    best = Some((len, repo.name.clone()));
                }
            }
        }
    }
    best.map(|(_, name)| name)
}

fn canonical_repo_name<'a>(
    repositories: &'a [ArtifactRefRepositoryWire],
    name: &str,
) -> Option<&'a str> {
    repositories
        .iter()
        .find(|repo| repository_matches(repo, name))
        .map(|repo| repo.name.as_str())
}

fn push_base(searches: &mut Vec<RepoSearch>, id: RepoId, base: SearchBase) {
    if let Some(existing) = searches.iter_mut().find(|search| search.id == id) {
        if existing.bases.iter().any(|seen| seen.root == base.root) {
            return;
        }
        existing.bases.push(base);
        return;
    }
    searches.push(RepoSearch {
        id,
        bases: vec![base],
    });
}

fn probe_source_relative(
    payload: &Path,
    owner: &ArtifactRefDocumentOwnerWire,
    searches: &[RepoSearch],
) -> Option<Hit> {
    let source_directory = owner.source_directory.as_deref()?;
    let source_root = live_dir(Path::new(source_directory))?;
    let path = join_contained(&source_root, payload)?;
    if !is_existing_target(&path) {
        return None;
    }
    let (id, git_root) = identify_source_hit(&source_root, owner, searches);
    if !source_hit_in_owner_scope(&id, &path, owner, searches) {
        return None;
    }
    Some(Hit {
        id,
        path,
        git_root,
        evidence: "source_relative",
        status: "exact",
    })
}

fn source_hit_in_owner_scope(
    id: &RepoId,
    path: &Path,
    owner: &ArtifactRefDocumentOwnerWire,
    searches: &[RepoSearch],
) -> bool {
    let Some(requested) = owner.repository.as_deref() else {
        return true;
    };
    match id {
        RepoId::Named(name) => {
            name == requested
                || searches.iter().any(|search| {
                    matches!(&search.id, RepoId::Named(existing) if existing == name)
                        && search.bases.iter().any(|base| {
                            path.starts_with(&base.root)
                                || path.starts_with(&base.git_root)
                        })
                })
        }
        RepoId::Anonymous(_) => searches.is_empty()
            || searches.iter().any(|search| {
                search.bases.iter().any(|base| {
                    path.starts_with(&base.root)
                        || path.starts_with(&base.git_root)
                })
            }),
    }
}

fn identify_source_hit(
    source_root: &Path,
    owner: &ArtifactRefDocumentOwnerWire,
    searches: &[RepoSearch],
) -> (RepoId, PathBuf) {
    let mut best: Option<(usize, RepoId, PathBuf)> = None;
    for search in searches {
        for base in &search.bases {
            if source_root == base.root.as_path()
                || source_root.starts_with(&base.root)
            {
                let len = base.root.as_os_str().len();
                if best
                    .as_ref()
                    .map_or(true, |(best_len, _, _)| len > *best_len)
                {
                    best =
                        Some((len, search.id.clone(), base.git_root.clone()));
                }
            }
        }
    }
    if let Some((_, id, git_root)) = best {
        return (id, git_root);
    }
    if let Some(name) = owner.repository.clone() {
        return (RepoId::Named(name), source_root.to_path_buf());
    }
    (
        RepoId::Anonymous(source_root.to_path_buf()),
        source_root.to_path_buf(),
    )
}

fn probe_repo_direct(
    search: &RepoSearch,
    payload: &Path,
    requested_revision: Option<&str>,
) -> Probe {
    let mut inspected = Vec::new();
    let mut revision_miss: Option<Hit> = None;
    for base in &search.bases {
        let Some(path) = join_contained(&base.root, payload) else {
            inspected.push(evidence(
                search.id.wire_name(),
                &base.root.join(payload),
                base.evidence,
            ));
            continue;
        };
        inspected.push(evidence(search.id.wire_name(), &path, base.evidence));
        if !is_existing_target(&path) {
            continue;
        }
        let hit = Hit {
            id: search.id.clone(),
            path,
            git_root: base.git_root.clone(),
            evidence: base.evidence,
            status: "exact",
        };
        match apply_revision(hit, requested_revision) {
            Probe::Hit(hit) => return Probe::Hit(hit),
            Probe::RevisionUnavailable(hit) => {
                if revision_miss.is_none() {
                    revision_miss = Some(hit);
                }
            }
            Probe::Miss(_) => {}
        }
    }
    if let Some(hit) = revision_miss {
        return Probe::RevisionUnavailable(hit);
    }
    Probe::Miss(inspected)
}

fn apply_revision(hit: Hit, requested: Option<&str>) -> Probe {
    let Some(requested) = requested else {
        return Probe::Hit(hit);
    };
    match checkout_matches_revision(&hit.git_root, requested) {
        Some(true) => Probe::Hit(hit),
        Some(false) | None => Probe::RevisionUnavailable(hit),
    }
}

fn checkout_matches_revision(git_root: &Path, requested: &str) -> Option<bool> {
    let head = checkout_head_sha(git_root)?;
    let peeled = peel_to_commit_sha(git_root, requested)
        .unwrap_or_else(|| requested.trim().to_ascii_lowercase());
    Some(revisions_agree(&peeled, &head))
}

fn revisions_agree(requested: &str, observed: &str) -> bool {
    let requested = requested.trim().to_ascii_lowercase();
    let observed = observed.trim().to_ascii_lowercase();
    if requested.len() < 7 || observed.len() < 7 {
        return !requested.is_empty() && requested == observed;
    }
    requested == observed
        || requested.starts_with(&observed)
        || observed.starts_with(&requested)
}

fn decide(
    hits: Vec<Hit>,
    requested_revision: Option<&str>,
) -> Option<ArtifactRefTargetResolutionWire> {
    let unique = dedupe_hits(hits);
    match unique.len() {
        0 => None,
        1 => {
            let hit = unique.into_iter().next()?;
            let revision = reported_revision(&hit, requested_revision);
            Some(exact(hit.path, hit.id.wire_name(), revision, hit.status))
        }
        _ => {
            let distinct_repos: HashSet<_> =
                unique.iter().map(|hit| hit.id.clone()).collect();
            if distinct_repos.len() == 1 {
                let hit = unique.into_iter().next()?;
                let revision = reported_revision(&hit, requested_revision);
                return Some(exact(
                    hit.path,
                    hit.id.wire_name(),
                    revision,
                    hit.status,
                ));
            }
            let candidates = unique.iter().map(evidence_from_hit).collect();
            Some(unresolved(
                ArtifactRefTargetFailureCategoryWire::Ambiguous,
                candidates,
                Some(
                    "more than one equally plausible target was found"
                        .to_string(),
                ),
            ))
        }
    }
}

fn dedupe_hits(hits: Vec<Hit>) -> Vec<Hit> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for hit in hits {
        let key = (hit.id.clone(), hit.path.clone());
        if seen.insert(key) {
            unique.push(hit);
        }
    }
    unique
}

fn reported_revision(hit: &Hit, requested: Option<&str>) -> Option<String> {
    let requested = requested?;
    peel_to_commit_sha(&hit.git_root, requested)
        .or_else(|| Some(requested.to_string()))
}

fn revision_unavailable(
    requested: Option<&str>,
    hits: &[Hit],
) -> ArtifactRefTargetResolutionWire {
    let requested = requested.unwrap_or("");
    let mut diagnostic = format!(
        "requested revision {requested:?} does not match the checkout HEAD"
    );
    if let Some(hit) = hits.first() {
        let relpath = posix_payload(
            hit.path.strip_prefix(&hit.git_root).unwrap_or(&hit.path),
        );
        if git_object_exists_at_revision(&hit.git_root, requested, &relpath) {
            diagnostic.push_str(
                "; Git can name the path at that revision, but the current worktree is not that revision",
            );
        }
    }
    unresolved(
        ArtifactRefTargetFailureCategoryWire::UnavailableRevision,
        hits.iter().map(evidence_from_hit).collect(),
        Some(diagnostic),
    )
}

fn missing_checkout_diagnostic(
    owner: &ArtifactRefDocumentOwnerWire,
    searches: &[RepoSearch],
) -> String {
    if searches.is_empty() {
        return match owner.repository.as_deref() {
            Some(name) => format!(
                "repository {name:?} is not in the resolution context's repository inventory"
            ),
            None => {
                "no repositories are configured in the resolution context"
                    .to_string()
            }
        };
    }
    "none of the identified repository's checkouts exist locally".to_string()
}

fn repository_matches(repo: &ArtifactRefRepositoryWire, name: &str) -> bool {
    repo.name == name || repo.aliases.iter().any(|alias| alias == name)
}

fn live_dir(path: &Path) -> Option<PathBuf> {
    path.is_dir()
        .then(|| path.canonicalize().unwrap_or_else(|_| path.to_path_buf()))
}

fn normalize_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn join_contained(root: &Path, payload: &Path) -> Option<PathBuf> {
    let joined = root.join(payload);
    let root_canon = root.canonicalize().ok()?;
    match joined.canonicalize() {
        Ok(candidate) if candidate.starts_with(&root_canon) => Some(candidate),
        Ok(_) => None,
        Err(_) => None,
    }
}

fn is_existing_target(path: &Path) -> bool {
    path.is_file() || path.is_dir()
}

fn evidence_from_hit(hit: &Hit) -> ArtifactRefTargetCandidateWire {
    evidence(hit.id.wire_name(), &hit.path, hit.evidence)
}

/// Recursively search *root* for a file whose path, relative to *root*, ends
/// with *payload*'s components. Decrements *budget* per directory entry
/// visited and returns `false` without completing when it reaches zero.
fn collect_suffix_matches(
    root: &Path,
    payload: &Path,
    budget: &mut usize,
    matches: &mut Vec<PathBuf>,
) -> bool {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            if *budget == 0 {
                return false;
            }
            *budget -= 1;
            let entry_path = entry.path();
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() {
                let skipped = is_skipped_dir(&entry_path);
                if !skipped {
                    stack.push(entry_path.clone());
                }
                if skipped {
                    continue;
                }
            } else if !file_type.is_file() {
                continue;
            }
            let Ok(relative) = entry_path.strip_prefix(root) else {
                continue;
            };
            if path_has_suffix(relative, payload) {
                matches.push(entry_path);
            }
        }
    }
    true
}

fn is_skipped_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| SKIPPED_DIR_NAMES.contains(&name))
}

fn path_has_suffix(relative: &Path, payload: &Path) -> bool {
    let relative_parts: Vec<_> = relative.components().collect();
    let payload_parts: Vec<_> = payload.components().collect();
    if payload_parts.len() > relative_parts.len() {
        return false;
    }
    relative_parts[relative_parts.len() - payload_parts.len()..]
        == payload_parts[..]
}

fn evidence(
    repository: Option<String>,
    path: &Path,
    tag: &str,
) -> ArtifactRefTargetCandidateWire {
    ArtifactRefTargetCandidateWire {
        repository,
        path: path.to_string_lossy().into_owned(),
        evidence: tag.to_string(),
    }
}

fn exact(
    path: PathBuf,
    repository: Option<String>,
    revision: Option<String>,
    status: &str,
) -> ArtifactRefTargetResolutionWire {
    ArtifactRefTargetResolutionWire {
        schema_version: ARTIFACT_REF_TARGET_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        resolved_path: Some(path.to_string_lossy().into_owned()),
        repository,
        revision,
        candidates: Vec::new(),
        failure_category: None,
        retryable: false,
        diagnostic: None,
    }
}

fn unresolved(
    category: ArtifactRefTargetFailureCategoryWire,
    candidates: Vec<ArtifactRefTargetCandidateWire>,
    diagnostic: Option<String>,
) -> ArtifactRefTargetResolutionWire {
    let status = match category {
        ArtifactRefTargetFailureCategoryWire::MissingCheckout => {
            "missing_checkout"
        }
        ArtifactRefTargetFailureCategoryWire::UnavailableRevision => {
            "unavailable_revision"
        }
        ArtifactRefTargetFailureCategoryWire::Ambiguous => "ambiguous",
        ArtifactRefTargetFailureCategoryWire::DeniedFiltered => "denied",
        ArtifactRefTargetFailureCategoryWire::TemporaryError => "error",
        ArtifactRefTargetFailureCategoryWire::ProvenMissing => "missing",
    };
    ArtifactRefTargetResolutionWire {
        schema_version: ARTIFACT_REF_TARGET_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        resolved_path: None,
        repository: None,
        revision: None,
        candidates,
        retryable: category.retryable(),
        failure_category: Some(category),
        diagnostic,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::process::Command;

    use tempfile::tempdir;

    use super::*;
    use crate::artifact_ref::ArtifactRefRepositoryWire;

    fn repo(name: &str, checkout_paths: &[&Path]) -> ArtifactRefRepositoryWire {
        ArtifactRefRepositoryWire {
            kind: "git".to_string(),
            name: name.to_string(),
            aliases: Vec::new(),
            shas: Vec::new(),
            checkout_paths: checkout_paths
                .iter()
                .map(|path| path.to_string_lossy().into_owned())
                .collect(),
        }
    }

    fn owner() -> ArtifactRefDocumentOwnerWire {
        ArtifactRefDocumentOwnerWire::default()
    }

    fn path_str(path: &Path) -> String {
        path.canonicalize()
            .unwrap_or_else(|_| path.to_path_buf())
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn resolves_a_source_path_in_its_owning_linked_repository() {
        let temp = tempdir().unwrap();
        let primary = temp.path().join("bob-cli");
        let linked = temp.path().join("bob-mac-capture");
        fs::create_dir_all(primary.join("plans/202609")).unwrap();
        fs::write(
            primary.join("plans/202609/capture_line_edge_cycling.md"),
            "plan",
        )
        .unwrap();
        fs::create_dir_all(linked.join("Sources/BobMacCapture")).unwrap();
        fs::write(
            linked.join("Sources/BobMacCapture/CaptureKeyCommandRouter.swift"),
            "swift",
        )
        .unwrap();
        // An unrelated same-named file must not be a false match.
        let unrelated = temp.path().join("unrelated");
        fs::create_dir_all(&unrelated).unwrap();
        fs::write(unrelated.join("CaptureKeyCommandRouter.swift"), "decoy")
            .unwrap();

        let context = ArtifactRefContextWire {
            repositories: vec![
                repo("bob-cli", &[&primary]),
                repo("bob-mac-capture", &[&linked]),
            ],
            ..Default::default()
        };

        let resolution = resolve_document_source_target(
            "Sources/BobMacCapture/CaptureKeyCommandRouter.swift",
            &owner(),
            &context,
        )
        .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.repository.as_deref(), Some("bob-mac-capture"));
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(
                path_str(&linked.join(
                    "Sources/BobMacCapture/CaptureKeyCommandRouter.swift"
                ))
                .as_str()
            )
        );
    }

    #[test]
    fn two_ambiguous_linked_repos_report_both_candidates() {
        let temp = tempdir().unwrap();
        let repo_a = temp.path().join("repo-a");
        let repo_b = temp.path().join("repo-b");
        fs::create_dir_all(repo_a.join("src")).unwrap();
        fs::create_dir_all(repo_b.join("src")).unwrap();
        fs::write(repo_a.join("src/shared.rs"), "a").unwrap();
        fs::write(repo_b.join("src/shared.rs"), "b").unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![
                repo("repo-a", &[&repo_a]),
                repo("repo-b", &[&repo_b]),
            ],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/shared.rs", &owner(), &context)
                .unwrap();

        assert_eq!(resolution.status, "ambiguous");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::Ambiguous)
        );
        assert!(!resolution.retryable);
        assert_eq!(resolution.candidates.len(), 2);
        let repos: std::collections::BTreeSet<_> = resolution
            .candidates
            .iter()
            .filter_map(|candidate| candidate.repository.clone())
            .collect();
        assert_eq!(
            repos,
            std::collections::BTreeSet::from([
                "repo-a".to_string(),
                "repo-b".to_string()
            ])
        );
    }

    #[test]
    fn deleted_producer_workspace_falls_back_to_a_live_alternate_checkout() {
        let temp = tempdir().unwrap();
        let deleted = temp.path().join("does-not-exist");
        let live = temp.path().join("live-checkout");
        fs::create_dir_all(live.join("src")).unwrap();
        fs::write(live.join("src/lib.rs"), "code").unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&deleted, &live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/lib.rs", &owner(), &context)
                .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.repository.as_deref(), Some("core"));
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(path_str(&live.join("src/lib.rs")).as_str())
        );
    }

    #[test]
    fn absent_checkout_is_a_retryable_missing_checkout() {
        let temp = tempdir().unwrap();
        let absent = temp.path().join("absent");
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&absent])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/lib.rs", &owner(), &context)
                .unwrap();

        assert_eq!(resolution.status, "missing_checkout");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::MissingCheckout)
        );
        assert!(resolution.retryable);
    }

    #[test]
    fn no_repositories_configured_is_missing_checkout_not_a_guess() {
        let context = ArtifactRefContextWire::default();
        let resolution =
            resolve_document_source_target("src/lib.rs", &owner(), &context)
                .unwrap();
        assert_eq!(resolution.status, "missing_checkout");
        assert!(resolution.resolved_path.is_none());
    }

    #[test]
    fn drifted_path_resolves_through_bounded_suffix_search() {
        // The document names `nested/router.swift`, relative to a package
        // subdirectory rather than the repository root; the real file is one
        // ancestor directory deeper than that logical relative path.
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        fs::create_dir_all(live.join("new_location/nested")).unwrap();
        fs::write(live.join("new_location/nested/router.swift"), "code")
            .unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![repo("capture", &[&live])],
            ..Default::default()
        };

        let resolution = resolve_document_source_target(
            "nested/router.swift",
            &owner(),
            &context,
        )
        .unwrap();

        assert_eq!(resolution.status, "drifted");
        assert_eq!(resolution.repository.as_deref(), Some("capture"));
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(
                path_str(&live.join("new_location/nested/router.swift"))
                    .as_str()
            )
        );
    }

    #[test]
    fn proven_missing_when_every_known_checkout_was_searched() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        fs::create_dir_all(&live).unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![repo("capture", &[&live])],
            ..Default::default()
        };

        let resolution = resolve_document_source_target(
            "src/nowhere/missing.swift",
            &owner(),
            &context,
        )
        .unwrap();

        assert_eq!(resolution.status, "missing");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::ProvenMissing)
        );
        assert!(resolution.retryable);
    }

    #[test]
    fn explicit_owner_repository_scopes_the_search_and_skips_others() {
        let temp = tempdir().unwrap();
        let scoped_repo = temp.path().join("scoped");
        let other_repo = temp.path().join("other");
        fs::create_dir_all(scoped_repo.join("src")).unwrap();
        fs::create_dir_all(other_repo.join("src")).unwrap();
        fs::write(other_repo.join("src/shared.rs"), "other").unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![
                repo("scoped", &[&scoped_repo]),
                repo("other", &[&other_repo]),
            ],
            ..Default::default()
        };
        let mut owner = owner();
        owner.repository = Some("scoped".to_string());

        let resolution =
            resolve_document_source_target("src/shared.rs", &owner, &context)
                .unwrap();

        // `other` actually has the file, but explicit repository provenance
        // must never let an unrelated repository's same-named file win.
        assert_eq!(resolution.status, "missing");
        assert!(resolution
            .candidates
            .iter()
            .all(|candidate| candidate.repository.as_deref() != Some("other")));
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::ProvenMissing)
        );
    }

    #[test]
    fn caller_attached_checkout_candidate_wins_without_repository_inventory() {
        let temp = tempdir().unwrap();
        let attached = temp.path().join("attached");
        fs::create_dir_all(attached.join("src")).unwrap();
        fs::write(attached.join("src/lib.rs"), "code").unwrap();
        let mut owner = owner();
        owner.checkout_candidates =
            vec![attached.to_string_lossy().into_owned()];
        owner.repository = Some("attached-repo".to_string());

        let resolution = resolve_document_source_target(
            "src/lib.rs",
            &owner,
            &ArtifactRefContextWire::default(),
        )
        .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.repository.as_deref(), Some("attached-repo"));
    }

    #[test]
    fn traversal_payload_is_rejected_before_any_search() {
        let context = ArtifactRefContextWire::default();
        let error =
            resolve_document_source_target("../escape.rs", &owner(), &context)
                .unwrap_err();
        assert_eq!(error.kind, "validation");
    }

    #[test]
    fn stale_attached_checkout_falls_through_to_live_same_repo_inventory() {
        let temp = tempdir().unwrap();
        let deleted = temp.path().join("does-not-exist");
        let live = temp.path().join("live-checkout");
        fs::create_dir_all(live.join("src")).unwrap();
        fs::write(live.join("src/lib.rs"), "code").unwrap();
        let mut owner = owner();
        owner.repository = Some("core".to_string());
        owner.checkout_candidates =
            vec![deleted.to_string_lossy().into_owned()];
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/lib.rs", &owner, &context)
                .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.repository.as_deref(), Some("core"));
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(path_str(&live.join("src/lib.rs")).as_str())
        );
    }

    #[test]
    fn source_directory_resolves_a_one_component_path_beside_the_document() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        let source_dir = live.join("Sources/BobMacCapture");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("Router.swift"), "swift").unwrap();
        let mut owner = owner();
        owner.source_directory =
            Some(source_dir.to_string_lossy().into_owned());
        let context = ArtifactRefContextWire {
            repositories: vec![repo("capture", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("Router.swift", &owner, &context)
                .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.repository.as_deref(), Some("capture"));
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(path_str(&source_dir.join("Router.swift")).as_str())
        );
    }

    #[test]
    fn distinct_attached_repositories_are_ambiguous_not_first_hit() {
        let temp = tempdir().unwrap();
        let repo_a = temp.path().join("repo-a");
        let repo_b = temp.path().join("repo-b");
        fs::create_dir_all(repo_a.join("src")).unwrap();
        fs::create_dir_all(repo_b.join("src")).unwrap();
        fs::write(repo_a.join("src/shared.rs"), "a").unwrap();
        fs::write(repo_b.join("src/shared.rs"), "b").unwrap();
        let mut owner = owner();
        owner.checkout_candidates = vec![
            repo_a.to_string_lossy().into_owned(),
            repo_b.to_string_lossy().into_owned(),
        ];

        let resolution = resolve_document_source_target(
            "src/shared.rs",
            &owner,
            &ArtifactRefContextWire::default(),
        )
        .unwrap();

        assert_eq!(resolution.status, "ambiguous");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::Ambiguous)
        );
        assert_eq!(resolution.candidates.len(), 2);
    }

    #[test]
    fn directory_targets_use_the_same_decision_path() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        fs::create_dir_all(live.join("src/pkg")).unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("./src/pkg", &owner(), &context)
                .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(
            resolution.resolved_path.as_deref(),
            Some(path_str(&live.join("src/pkg")).as_str())
        );
    }

    #[test]
    fn same_named_directories_in_distinct_repos_are_ambiguous() {
        let temp = tempdir().unwrap();
        let repo_a = temp.path().join("repo-a");
        let repo_b = temp.path().join("repo-b");
        fs::create_dir_all(repo_a.join("shared")).unwrap();
        fs::create_dir_all(repo_b.join("shared")).unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![
                repo("repo-a", &[&repo_a]),
                repo("repo-b", &[&repo_b]),
            ],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("shared", &owner(), &context)
                .unwrap();

        assert_eq!(resolution.status, "ambiguous");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::Ambiguous)
        );
    }

    #[test]
    fn owner_path_globs_deny_before_any_checkout_probe() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        fs::create_dir_all(live.join("src")).unwrap();
        fs::write(live.join("src/secret.rs"), "secret").unwrap();
        fs::write(live.join("src/lib.rs"), "code").unwrap();
        let mut owner = owner();
        owner.path_globs =
            Some(vec!["src/**".to_string(), "!src/secret.rs".to_string()]);
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let denied =
            resolve_document_source_target("src/secret.rs", &owner, &context)
                .unwrap();
        assert_eq!(denied.status, "denied");
        assert_eq!(
            denied.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::DeniedFiltered)
        );
        assert!(!denied.retryable);
        assert!(denied.resolved_path.is_none());

        let allowed =
            resolve_document_source_target("src/lib.rs", &owner, &context)
                .unwrap();
        assert_eq!(allowed.status, "exact");
    }

    #[test]
    fn matching_revision_is_required_before_a_worktree_path_is_exact() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        let head = init_git_repo(&live);
        let mut owner = owner();
        owner.revision = Some(head.clone());
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/lib.rs", &owner, &context)
                .unwrap();

        assert_eq!(resolution.status, "exact");
        assert_eq!(resolution.revision.as_deref(), Some(head.as_str()));
    }

    #[test]
    fn mismatching_revision_is_unavailable_even_when_the_path_exists() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        let first = init_git_repo(&live);
        fs::write(live.join("src/lib.rs"), "v2").unwrap();
        git(&live, &["add", "src/lib.rs"]);
        git(&live, &["commit", "-m", "v2"]);
        let mut owner = owner();
        owner.revision = Some(first);
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_document_source_target("src/lib.rs", &owner, &context)
                .unwrap();

        assert_eq!(resolution.status, "unavailable_revision");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::UnavailableRevision)
        );
        assert!(resolution.retryable);
        assert!(resolution.resolved_path.is_none());
        assert!(resolution
            .diagnostic
            .as_deref()
            .is_some_and(|text| text.contains("current worktree")));
    }

    #[test]
    fn suffix_budget_exhaustion_is_a_temporary_error() {
        let temp = tempdir().unwrap();
        let live = temp.path().join("repo");
        for index in 0..8 {
            let dir = live.join(format!("d{index}"));
            fs::create_dir_all(&dir).unwrap();
            fs::write(dir.join("file.rs"), "x").unwrap();
        }
        fs::create_dir_all(live.join("nested/deep")).unwrap();
        fs::write(live.join("nested/deep/router.swift"), "code").unwrap();
        let context = ArtifactRefContextWire {
            repositories: vec![repo("core", &[&live])],
            ..Default::default()
        };

        let resolution =
            resolve_with_budget("nested/router.swift", &owner(), &context, 1)
                .unwrap();

        assert_eq!(resolution.status, "error");
        assert_eq!(
            resolution.failure_category,
            Some(ArtifactRefTargetFailureCategoryWire::TemporaryError)
        );
        assert!(resolution.retryable);
    }

    fn git(repo: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    }

    fn init_git_repo(root: &Path) -> String {
        fs::create_dir_all(root.join("src")).unwrap();
        git(root, &["init", "--initial-branch=master"]);
        git(root, &["config", "user.name", "SASE Test"]);
        git(root, &["config", "user.email", "sase@example.com"]);
        fs::write(root.join("src/lib.rs"), "v1").unwrap();
        git(root, &["add", "src/lib.rs"]);
        git(root, &["commit", "-m", "v1"]);
        git(root, &["rev-parse", "HEAD"])
    }
}

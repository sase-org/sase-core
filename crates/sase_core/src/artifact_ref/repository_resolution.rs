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
//! 1. Caller-attached checkout candidates (explicit, already-known evidence).
//! 2. Every known checkout of the explicitly named owning repository, when
//!    one is given.
//! 3. Every known checkout of every repository in context, when no owning
//!    repository is named; a unique repository match is required.
//! 4. A bounded suffix search across each candidate repository's first
//!    available checkout, for a path that has drifted under a renamed
//!    parent directory.
//!
//! Distinct repositories that both contain a matching path are ambiguous,
//! not a first-hit guess; copies of the same repository never count as
//! distinct evidence because at most one checkout is searched per
//! repository.

use std::path::{Path, PathBuf};

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
    validate_artifact_ref_context(context)?;
    validate_path_payload("source", path)?;
    let payload = Path::new(path);

    if !owner.checkout_candidates.is_empty() {
        if let Some(outcome) = resolve_against_attached_checkouts(
            payload,
            owner.repository.as_deref(),
            owner.revision.as_deref(),
            &owner.checkout_candidates,
        ) {
            return Ok(outcome);
        }
    }

    let scoped: Vec<&ArtifactRefRepositoryWire> = match &owner.repository {
        Some(name) => context
            .repositories
            .iter()
            .filter(|repo| repository_matches(repo, name))
            .collect(),
        None => context.repositories.iter().collect(),
    };

    if scoped.is_empty() {
        let diagnostic = match &owner.repository {
            Some(name) => format!(
                "repository {name:?} is not in the resolution context's repository inventory"
            ),
            None => "no repositories are configured in the resolution context".to_string(),
        };
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::MissingCheckout,
            Vec::new(),
            Some(diagnostic),
        ));
    }

    let mut candidates = Vec::new();
    let mut direct_hits: Vec<(String, PathBuf)> = Vec::new();
    let mut any_checkout_exists = false;
    for repo in &scoped {
        for checkout in &repo.checkout_paths {
            let root = PathBuf::from(checkout);
            if !root.is_dir() {
                continue;
            }
            any_checkout_exists = true;
            let candidate = root.join(payload);
            candidates.push(evidence(
                Some(repo.name.clone()),
                &candidate,
                "repository_checkout",
            ));
            if candidate.is_file() {
                direct_hits.push((repo.name.clone(), candidate));
                break;
            }
        }
    }

    if let Some(outcome) =
        decide(direct_hits, owner.revision.as_deref(), "exact")
    {
        return Ok(outcome);
    }

    if !any_checkout_exists {
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::MissingCheckout,
            candidates,
            Some(
                "none of the identified repository's checkouts exist locally"
                    .to_string(),
            ),
        ));
    }

    if payload.components().count() < 2 {
        return Ok(unresolved(
            ArtifactRefTargetFailureCategoryWire::ProvenMissing,
            candidates,
            Some("path was not found in any known checkout".to_string()),
        ));
    }

    let mut budget = SUFFIX_SEARCH_MAX_ENTRIES;
    let mut suffix_hits: Vec<(String, PathBuf)> = Vec::new();
    for repo in &scoped {
        let Some(root) = repo
            .checkout_paths
            .iter()
            .map(PathBuf::from)
            .find(|root| root.is_dir())
        else {
            continue;
        };
        let mut found = Vec::new();
        if !collect_suffix_matches(&root, payload, &mut budget, &mut found) {
            return Ok(unresolved(
                ArtifactRefTargetFailureCategoryWire::TemporaryError,
                candidates,
                Some(
                    "bounded repository search budget was exhausted"
                        .to_string(),
                ),
            ));
        }
        for candidate in found {
            suffix_hits.push((repo.name.clone(), candidate));
        }
    }
    for (repository, path) in &suffix_hits {
        candidates.push(evidence(
            Some(repository.clone()),
            path,
            "suffix_match",
        ));
    }

    if let Some(outcome) =
        decide(suffix_hits, owner.revision.as_deref(), "drifted")
    {
        return Ok(outcome);
    }

    Ok(unresolved(
        ArtifactRefTargetFailureCategoryWire::ProvenMissing,
        candidates,
        Some("path was not found in any known checkout".to_string()),
    ))
}

fn resolve_against_attached_checkouts(
    payload: &Path,
    repository: Option<&str>,
    revision: Option<&str>,
    checkouts: &[String],
) -> Option<ArtifactRefTargetResolutionWire> {
    let mut any_exists = false;
    for checkout in checkouts {
        let root = PathBuf::from(checkout);
        if !root.is_dir() {
            continue;
        }
        any_exists = true;
        let candidate = root.join(payload);
        if candidate.is_file() {
            return Some(exact(
                candidate,
                repository.map(str::to_string),
                revision.map(str::to_string),
                "exact",
            ));
        }
    }
    if any_exists {
        return None;
    }
    Some(unresolved(
        ArtifactRefTargetFailureCategoryWire::MissingCheckout,
        Vec::new(),
        Some(
            "caller-attached checkout candidates do not exist locally"
                .to_string(),
        ),
    ))
}

fn repository_matches(repo: &ArtifactRefRepositoryWire, name: &str) -> bool {
    repo.name == name || repo.aliases.iter().any(|alias| alias == name)
}

fn decide(
    hits: Vec<(String, PathBuf)>,
    revision: Option<&str>,
    status: &str,
) -> Option<ArtifactRefTargetResolutionWire> {
    match hits.len() {
        0 => None,
        1 => {
            let (repository, path) = hits.into_iter().next().expect("len == 1");
            Some(exact(
                path,
                Some(repository),
                revision.map(str::to_string),
                status,
            ))
        }
        _ => {
            let candidates = hits
                .into_iter()
                .map(|(repository, path)| {
                    evidence(Some(repository), &path, "repository_checkout")
                })
                .collect();
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
                if !is_skipped_dir(&entry_path) {
                    stack.push(entry_path);
                }
                continue;
            }
            if !file_type.is_file() {
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
                linked
                    .join("Sources/BobMacCapture/CaptureKeyCommandRouter.swift")
                    .to_string_lossy()
                    .as_ref()
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
            Some(live.join("src/lib.rs").to_string_lossy().as_ref())
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
                live.join("new_location/nested/router.swift")
                    .to_string_lossy()
                    .as_ref()
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
}

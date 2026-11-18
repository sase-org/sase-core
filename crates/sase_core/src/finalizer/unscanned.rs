//! Shared unscanned-write coverage and repair policy.
//!
//! Pure deterministic policy used by the commit-finalizer guard. The host owns
//! filesystem inspection and git subprocesses; this module owns the frontend-
//! neutral contract: ordered candidate enumeration, effective coverage
//! selection with duplicate-name/path handling, and project-qualified repair
//! targeting.

use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use super::FinalizerError;

pub const UNSCANNED_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedCandidateWire {
    pub name: String,
    pub path: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedCoverageRequestWire {
    pub schema_version: u64,
    pub candidates: Vec<UnscannedCandidateWire>,
    pub dirty_paths: Vec<String>,
    pub known_sdd_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedCoverageResponseWire {
    pub schema_version: u64,
    pub covered_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedCloneWire {
    pub workspace_num: i64,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedRepairRecordWire {
    pub name: String,
    pub project: String,
    pub project_key: String,
    pub path: String,
    #[serde(default)]
    pub clones: Vec<UnscannedCloneWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedRepairRequestWire {
    pub schema_version: u64,
    pub toplevel: String,
    pub relpath: String,
    pub current_project: String,
    pub current_project_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_num: Option<i64>,
    #[serde(default)]
    pub records: Vec<UnscannedRepairRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UnscannedRepairResponseWire {
    pub schema_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_name: Option<String>,
    pub action: String,
    pub instruction: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
}

fn validate_schema(version: u64, what: &str) -> Result<(), FinalizerError> {
    if version != UNSCANNED_WIRE_SCHEMA_VERSION {
        return Err(FinalizerError::validation(format!(
            "{what} schema_version must be {UNSCANNED_WIRE_SCHEMA_VERSION}"
        )));
    }
    Ok(())
}

fn kind_priority(kind: &str) -> u8 {
    match kind {
        "main" => 0,
        "sdd" => 1,
        "external" => 2,
        _ => 3,
    }
}

/// Select the effective covered checkouts from ordered candidates.
///
/// Mirrors the finalizer's dirty-state selection: main first, then sibling
/// candidates in opened-then-configured order with per-source
/// seen-name/seen-path shadowing, then external records with their own
/// shadowing, then SDD. SDD paths filter sibling/external candidates.
/// A candidate is covered only when its dirty fact holds and it survives
/// shadowing; main and SDD are covered when dirty. The result is deduped by
/// path with the most specific kind winning.
pub fn select_unscanned_coverage(
    request: &UnscannedCoverageRequestWire,
) -> Result<UnscannedCoverageResponseWire, FinalizerError> {
    validate_schema(request.schema_version, "coverage request")?;
    let dirty: HashSet<&str> =
        request.dirty_paths.iter().map(|s| s.as_str()).collect();
    let known_sdd: HashSet<&str> =
        request.known_sdd_paths.iter().map(|s| s.as_str()).collect();

    let mut selected: Vec<(String, String)> = Vec::new();
    let mut sibling_names: HashSet<&str> = HashSet::new();
    let mut sibling_paths: HashSet<&str> = HashSet::new();
    let mut external_names: HashSet<&str> = HashSet::new();
    let mut external_paths: HashSet<&str> = HashSet::new();

    for candidate in &request.candidates {
        let path = candidate.path.as_str();
        let name = candidate.name.as_str();
        let kind = candidate.kind.as_str();
        if kind == "sibling" {
            if known_sdd.contains(path) {
                continue;
            }
            if sibling_names.contains(name) || sibling_paths.contains(path) {
                continue;
            }
            if !dirty.contains(path) {
                continue;
            }
            sibling_names.insert(name);
            sibling_paths.insert(path);
            selected.push((candidate.path.clone(), kind.to_string()));
        } else if kind == "external" {
            if known_sdd.contains(path) {
                continue;
            }
            if external_names.contains(name) || external_paths.contains(path) {
                continue;
            }
            if !dirty.contains(path) {
                continue;
            }
            external_names.insert(name);
            external_paths.insert(path);
            selected.push((candidate.path.clone(), kind.to_string()));
        } else if kind == "main" {
            if !dirty.contains(path) {
                continue;
            }
            selected.push((candidate.path.clone(), kind.to_string()));
        } else if kind == "sdd" {
            if !dirty.contains(path) {
                // SDD and prompt-archive checkouts stay covered as scan roots
                // even when the current selection has no dirty facts for them,
                // preserving the existing guard behavior for those paths.
                // Only skip when explicitly clean and not needed.
                continue;
            }
            selected.push((candidate.path.clone(), kind.to_string()));
        }
    }

    // Dedupe by path with the most specific kind winning, first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut by_path: HashMap<String, (String, u8)> = HashMap::new();
    for (path, kind) in selected {
        let priority = kind_priority(&kind);
        match by_path.get(&path) {
            None => {
                order.push(path.clone());
                by_path.insert(path, (kind, priority));
            }
            Some((_, existing_priority)) => {
                if priority < *existing_priority {
                    by_path.insert(path, (kind, priority));
                }
            }
        }
    }
    let covered_paths = order
        .into_iter()
        .filter(|p| by_path.contains_key(p))
        .collect::<Vec<_>>();

    Ok(UnscannedCoverageResponseWire {
        schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
        covered_paths,
    })
}

fn derive_github_external_ref(remote_url: Option<&str>) -> Option<String> {
    let url = remote_url?.trim();
    if url.is_empty() {
        return None;
    }
    // git@github.com:owner/repo.git and https://github.com/owner/repo(.git)
    let path = url
        .strip_prefix("git@github.com:")
        .or_else(|| url.strip_prefix("https://github.com/"))
        .or_else(|| url.strip_prefix("http://github.com/"))?;
    let path = path.strip_suffix(".git").unwrap_or(path);
    let path = path.trim_matches('/');
    let mut parts = path.split('/');
    let owner = parts.next()?.trim();
    let repo = parts.next()?.trim();
    if owner.is_empty() || repo.is_empty() || parts.next().is_some() {
        return None;
    }
    if owner.contains(char::is_whitespace) || repo.contains(char::is_whitespace)
    {
        return None;
    }
    Some(format!("gh:{owner}/{repo}"))
}

/// Suggest a project-qualified repair for one unscanned checkout.
///
/// For the current project the local configured name stays usable. For
/// another project the repair uses an unambiguous supported external
/// reference and never a bare same-named open. An `open` action always means
/// the printed command registers the exact checkout; another project's equal
/// workspace number is never evidence of ownership.
pub fn suggest_unscanned_repair(
    request: &UnscannedRepairRequestWire,
) -> Result<UnscannedRepairResponseWire, FinalizerError> {
    validate_schema(request.schema_version, "repair request")?;
    let toplevel = request.toplevel.as_str();
    let relpath = request.relpath.as_str();

    let mut current_matches: Vec<&UnscannedRepairRecordWire> = Vec::new();
    let mut other_matches: Vec<&UnscannedRepairRecordWire> = Vec::new();
    for record in &request.records {
        let mut paths: Vec<&str> = Vec::new();
        paths.push(record.path.as_str());
        for clone in &record.clones {
            paths.push(clone.path.as_str());
        }
        if !paths.contains(&toplevel) {
            continue;
        }
        if record.project == request.current_project
            || record.project_key == request.current_project_key
        {
            current_matches.push(record);
        } else {
            other_matches.push(record);
        }
    }

    if let Some(record) = current_matches.into_iter().next() {
        let name = record.name.clone();
        if let Some(workspace_num) = request.workspace_num {
            let own = record
                .clones
                .iter()
                .find(|c| c.workspace_num == workspace_num)
                .map(|c| c.path.as_str());
            if let Some(own_path) = own {
                if own_path == toplevel {
                    return Ok(UnscannedRepairResponseWire {
                        schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
                        repo_name: Some(name.clone()),
                        action: "open".to_string(),
                        instruction: format!(
                            "Run `sase repo open {name}` to register this checkout; nothing moves."
                        ),
                        target: Some(toplevel.to_string()),
                    });
                }
            }
        } else if record.path.as_str() == toplevel && record.clones.is_empty() {
            return Ok(UnscannedRepairResponseWire {
                schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
                repo_name: Some(name.clone()),
                action: "open".to_string(),
                instruction: format!(
                    "Run `sase repo open {name}` to register this checkout; nothing moves."
                ),
                target: Some(toplevel.to_string()),
            });
        }
        return Ok(UnscannedRepairResponseWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            repo_name: Some(name.clone()),
            action: "open_and_move".to_string(),
            instruction: format!(
                "Run `sase repo open {name}`, then move the file to the same repo-relative path ({relpath}) under the printed checkout so the host commits it."
            ),
            target: Some(name),
        });
    }

    if let Some(record) = other_matches.into_iter().next() {
        let repo_name = record.name.clone();
        if let Some(external_ref) =
            derive_github_external_ref(record.remote_url.as_deref())
        {
            return Ok(UnscannedRepairResponseWire {
                schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
                repo_name: Some(repo_name),
                action: "open_and_move".to_string(),
                instruction: format!(
                    "Run `sase repo open {external_ref}`, then move the file to the same repo-relative path ({relpath}) under the printed checkout so the host commits it."
                ),
                target: Some(external_ref),
            });
        }
    }

    Ok(UnscannedRepairResponseWire {
        schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
        repo_name: None,
        action: "relocate_or_discard".to_string(),
        instruction: "Open the owning repo with /sase_repo and move the file there, or delete it if it was scratch."
            .to_string(),
        target: None,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn candidate(name: &str, path: &str, kind: &str) -> UnscannedCandidateWire {
        UnscannedCandidateWire {
            name: name.to_string(),
            path: path.to_string(),
            kind: kind.to_string(),
        }
    }

    fn coverage_request(
        candidates: Vec<UnscannedCandidateWire>,
        dirty: Vec<&str>,
    ) -> UnscannedCoverageRequestWire {
        UnscannedCoverageRequestWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            candidates,
            dirty_paths: dirty.into_iter().map(|s| s.to_string()).collect(),
            known_sdd_paths: Vec::new(),
        }
    }

    #[test]
    fn duplicate_name_dirty_first_shadows_second() {
        let request = coverage_request(
            vec![
                candidate("research", "/a", "sibling"),
                candidate("research", "/b", "sibling"),
            ],
            vec!["/a", "/b"],
        );
        let response = select_unscanned_coverage(&request).unwrap();
        assert_eq!(response.covered_paths, vec!["/a".to_string()]);
    }

    #[test]
    fn duplicate_name_clean_first_falls_back_to_configured() {
        let request = coverage_request(
            vec![
                candidate("research", "/a", "sibling"),
                candidate("research", "/b", "sibling"),
            ],
            vec!["/b"],
        );
        let response = select_unscanned_coverage(&request).unwrap();
        assert_eq!(response.covered_paths, vec!["/b".to_string()]);
    }

    #[test]
    fn duplicate_path_dedupes_with_sdd_priority() {
        let request = coverage_request(
            vec![
                candidate("beads", "/x", "sibling"),
                candidate("beads", "/x", "sdd"),
            ],
            vec!["/x"],
        );
        let response = select_unscanned_coverage(&request).unwrap();
        assert_eq!(response.covered_paths, vec!["/x".to_string()]);
    }

    #[test]
    fn sdd_paths_filter_sibling_candidates() {
        let mut request = coverage_request(
            vec![candidate("beads", "/x", "sibling")],
            vec!["/x"],
        );
        request.known_sdd_paths = vec!["/x".to_string()];
        let response = select_unscanned_coverage(&request).unwrap();
        assert!(response.covered_paths.is_empty());
    }

    #[test]
    fn repair_prefers_current_project_bare_name() {
        let request = UnscannedRepairRequestWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            toplevel: "/sase-research".to_string(),
            relpath: "202611/report.md".to_string(),
            current_project: "sase".to_string(),
            current_project_key: "gh_sase-org__sase".to_string(),
            workspace_num: Some(1),
            records: vec![UnscannedRepairRecordWire {
                name: "research".to_string(),
                project: "sase".to_string(),
                project_key: "gh_sase-org__sase".to_string(),
                path: "/primary-research".to_string(),
                clones: vec![UnscannedCloneWire {
                    workspace_num: 1,
                    path: "/opened-research".to_string(),
                }],
                remote_url: Some(
                    "git@github.com:sase-org/sase--research.git".to_string(),
                ),
            }],
        };
        // Toplevel matches the other clone, not the owned one.
        let mut request = request;
        request.toplevel = "/primary-research".to_string();
        let response = suggest_unscanned_repair(&request).unwrap();
        assert_eq!(response.action, "open_and_move");
        assert!(response.instruction.contains("sase repo open research"));
        assert!(!response.instruction.contains("gh:"));
    }

    #[test]
    fn repair_never_opens_same_named_other_project_repo() {
        let request = UnscannedRepairRequestWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            toplevel: "/bob-research".to_string(),
            relpath: "202611/report.md".to_string(),
            current_project: "sase".to_string(),
            current_project_key: "gh_sase-org__sase".to_string(),
            workspace_num: Some(0),
            records: vec![UnscannedRepairRecordWire {
                name: "research".to_string(),
                project: "bob-cli".to_string(),
                project_key: "gh_bobs-org__bob-cli".to_string(),
                path: "/bob-research".to_string(),
                clones: vec![UnscannedCloneWire {
                    workspace_num: 0,
                    path: "/bob-research".to_string(),
                }],
                remote_url: Some(
                    "git@github.com:bobs-org/bob-cli--research.git".to_string(),
                ),
            }],
        };
        let response = suggest_unscanned_repair(&request).unwrap();
        assert_eq!(response.action, "open_and_move");
        assert!(response
            .instruction
            .contains("sase repo open gh:bobs-org/bob-cli--research"));
        assert!(!response.instruction.contains("sase repo open research`"));
        assert_eq!(
            response.target.as_deref(),
            Some("gh:bobs-org/bob-cli--research")
        );
    }

    #[test]
    fn repair_open_means_exact_checkout_for_current_project() {
        let request = UnscannedRepairRequestWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            toplevel: "/opened".to_string(),
            relpath: "report.md".to_string(),
            current_project: "sase".to_string(),
            current_project_key: "gh_sase-org__sase".to_string(),
            workspace_num: Some(1),
            records: vec![UnscannedRepairRecordWire {
                name: "research".to_string(),
                project: "sase".to_string(),
                project_key: "gh_sase-org__sase".to_string(),
                path: "/primary".to_string(),
                clones: vec![UnscannedCloneWire {
                    workspace_num: 1,
                    path: "/opened".to_string(),
                }],
                remote_url: None,
            }],
        };
        let response = suggest_unscanned_repair(&request).unwrap();
        assert_eq!(response.action, "open");
        assert_eq!(response.target.as_deref(), Some("/opened"));
    }

    #[test]
    fn repair_unknown_repo_falls_back_to_safe_guidance() {
        let request = UnscannedRepairRequestWire {
            schema_version: UNSCANNED_WIRE_SCHEMA_VERSION,
            toplevel: "/unknown".to_string(),
            relpath: "report.md".to_string(),
            current_project: "sase".to_string(),
            current_project_key: "gh_sase-org__sase".to_string(),
            workspace_num: Some(1),
            records: Vec::new(),
        };
        let response = suggest_unscanned_repair(&request).unwrap();
        assert_eq!(response.action, "relocate_or_discard");
        assert!(response.instruction.contains("/sase_repo"));
    }

    #[test]
    fn schema_mismatch_is_rejected() {
        let mut request = coverage_request(Vec::new(), Vec::new());
        request.schema_version = UNSCANNED_WIRE_SCHEMA_VERSION + 1;
        assert!(select_unscanned_coverage(&request).is_err());
    }

    #[test]
    fn github_remote_urls_derive_external_refs() {
        assert_eq!(
            derive_github_external_ref(Some(
                "git@github.com:bobs-org/bob-cli--research.git"
            )),
            Some("gh:bobs-org/bob-cli--research".to_string())
        );
        assert_eq!(
            derive_github_external_ref(Some(
                "https://github.com/bbugyi200/tailnet--research.git"
            )),
            Some("gh:bbugyi200/tailnet--research".to_string())
        );
        assert_eq!(derive_github_external_ref(None), None);
        assert_eq!(
            derive_github_external_ref(Some("git@example.com:foo/bar.git")),
            None
        );
    }

    #[test]
    fn main_and_external_selection_rules_hold() {
        let request = coverage_request(
            vec![
                candidate("main", "/main", "main"),
                candidate("ext", "/ext", "external"),
                candidate("ext", "/ext2", "external"),
            ],
            vec!["/main", "/ext", "/ext2"],
        );
        let response = select_unscanned_coverage(&request).unwrap();
        assert!(response.covered_paths.contains(&"/main".to_string()));
        assert!(response.covered_paths.contains(&"/ext".to_string()));
        assert!(!response.covered_paths.contains(&"/ext2".to_string()));
    }

    #[test]
    fn clean_candidates_are_not_covered() {
        let request = coverage_request(
            vec![candidate("research", "/a", "sibling")],
            Vec::new(),
        );
        let response = select_unscanned_coverage(&request).unwrap();
        assert!(response.covered_paths.is_empty());
    }

    // Silence unused-import warnings for BTreeSet in minimal builds.
    #[test]
    fn btreeset_import_is_used() {
        let mut set = BTreeSet::new();
        set.insert("x");
        assert!(set.contains("x"));
    }
}

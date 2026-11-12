//! Structured comparison for stale conditional-completion fingerprints.
//!
//! The comparison uses the exact observations and decided-repository scope
//! used by `worktree_fingerprint`. It records only repository metadata,
//! hashes, and paths; file contents are never included.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::completion::{
    ObservedPathKindWire, ObservedPathWire, RepositoryDecisionWire,
    RepositoryObservationWire,
};

const MAX_FINGERPRINT_DIFFERENCES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct FingerprintDifferencesWire {
    #[serde(default)]
    pub total_count: usize,
    #[serde(default)]
    pub omitted_count: usize,
    #[serde(default)]
    pub items: Vec<FingerprintDifferenceWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintDifferenceWire {
    pub repo_id: String,
    #[serde(default)]
    pub repo_name: Option<String>,
    #[serde(default)]
    pub repo_kind: Option<String>,
    pub component: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub expected: Option<String>,
    #[serde(default)]
    pub observed: Option<String>,
}

pub(super) fn compare_fingerprint_observations(
    sealed: &[RepositoryObservationWire],
    current: &[RepositoryObservationWire],
    decisions: &[RepositoryDecisionWire],
    sealed_fingerprint: &str,
    current_fingerprint: &str,
) -> FingerprintDifferencesWire {
    let decided: BTreeSet<&str> = decisions
        .iter()
        .map(|decision| decision.repo_id.as_str())
        .collect();
    let mut sealed_by_id =
        BTreeMap::<String, Vec<&RepositoryObservationWire>>::new();
    let mut current_by_id =
        BTreeMap::<String, Vec<&RepositoryObservationWire>>::new();
    for observation in sealed {
        if decided.contains(observation.repo_id.as_str()) {
            sealed_by_id
                .entry(observation.repo_id.clone())
                .or_default()
                .push(observation);
        }
    }
    for observation in current {
        if decided.contains(observation.repo_id.as_str()) {
            current_by_id
                .entry(observation.repo_id.clone())
                .or_default()
                .push(observation);
        }
    }

    let mut all = Vec::new();
    for repo_id in &decided {
        let sealed_observations =
            sealed_by_id.get(*repo_id).map(Vec::as_slice).unwrap_or(&[]);
        let current_observations = current_by_id
            .get(*repo_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let representative = current_observations
            .first()
            .copied()
            .or_else(|| sealed_observations.first().copied());

        if sealed_observations.is_empty() || current_observations.is_empty() {
            let (component, expected, observed) =
                if sealed_observations.is_empty() {
                    (
                        "repository_observation_extra",
                        "0".to_string(),
                        current_observations.len().to_string(),
                    )
                } else {
                    (
                        "repository_observation_missing",
                        sealed_observations.len().to_string(),
                        "0".to_string(),
                    )
                };
            push_difference(
                &mut all,
                repo_id,
                representative,
                component,
                None,
                Some(expected),
                Some(observed),
            );
            continue;
        }

        if sealed_observations.len() != current_observations.len() {
            push_difference(
                &mut all,
                repo_id,
                representative,
                "repository_observation_count",
                None,
                Some(sealed_observations.len().to_string()),
                Some(current_observations.len().to_string()),
            );
        }
        for (expected, observed) in
            sealed_observations.iter().zip(current_observations.iter())
        {
            compare_repository(&mut all, repo_id, expected, observed);
        }
    }

    if all.is_empty() && sealed_fingerprint != current_fingerprint {
        all.push(FingerprintDifferenceWire {
            repo_id: "<decided-repositories>".to_string(),
            repo_name: Some("decided repositories".to_string()),
            repo_kind: Some("scope".to_string()),
            component: "digest_mismatch".to_string(),
            path: None,
            expected: Some(sealed_fingerprint.to_string()),
            observed: Some(current_fingerprint.to_string()),
        });
    }

    all.sort_by(difference_order);
    let total_count = all.len();
    let mut retained = Vec::new();
    if total_count > MAX_FINGERPRINT_DIFFERENCES {
        // The number of decided repositories is capped by the intent schema;
        // retain one explanatory component for every mismatching repository
        // before filling the remaining budget with path-level detail.
        let mut represented = BTreeSet::new();
        for item in &all {
            if represented.insert(item.repo_id.as_str()) {
                retained.push(item.clone());
            }
        }
        for item in &all {
            if retained.len() >= MAX_FINGERPRINT_DIFFERENCES {
                break;
            }
            if !retained.contains(item) {
                retained.push(item.clone());
            }
        }
        retained.sort_by(difference_order);
    } else {
        retained = all;
    }
    let omitted_count = total_count.saturating_sub(retained.len());
    FingerprintDifferencesWire {
        total_count,
        omitted_count,
        items: retained,
    }
}

fn compare_repository(
    all: &mut Vec<FingerprintDifferenceWire>,
    repo_id: &str,
    expected: &RepositoryObservationWire,
    observed: &RepositoryObservationWire,
) {
    compare_value(
        all,
        repo_id,
        expected,
        "name",
        &expected.name,
        &observed.name,
    );
    compare_value(
        all,
        repo_id,
        expected,
        "kind",
        &expected.kind,
        &observed.kind,
    );
    compare_value(
        all,
        repo_id,
        expected,
        "head",
        &expected.head,
        &observed.head,
    );
    compare_value(
        all,
        repo_id,
        expected,
        "head_tree",
        &expected.head_tree,
        &observed.head_tree,
    );
    compare_value(
        all,
        repo_id,
        expected,
        "index_tree",
        &expected.index_tree,
        &observed.index_tree,
    );
    compare_value(
        all,
        repo_id,
        expected,
        "complete",
        &expected.complete.to_string(),
        &observed.complete.to_string(),
    );

    let expected_order =
        expected.paths.iter().map(path_encoding).collect::<Vec<_>>();
    let observed_order =
        observed.paths.iter().map(path_encoding).collect::<Vec<_>>();
    let mut expected_sorted = expected_order.clone();
    let mut observed_sorted = observed_order.clone();
    expected_sorted.sort();
    observed_sorted.sort();
    if expected_order != observed_order && expected_sorted == observed_sorted {
        push_difference(
            all,
            repo_id,
            Some(observed),
            "path_order",
            None,
            Some(path_order_summary(&expected.paths)),
            Some(path_order_summary(&observed.paths)),
        );
    }

    let expected_paths = group_paths(&expected.paths);
    let observed_paths = group_paths(&observed.paths);
    let paths: BTreeSet<&str> = expected_paths
        .keys()
        .chain(observed_paths.keys())
        .copied()
        .collect();
    for path in paths {
        let expected_group =
            expected_paths.get(path).map(Vec::as_slice).unwrap_or(&[]);
        let observed_group =
            observed_paths.get(path).map(Vec::as_slice).unwrap_or(&[]);
        if expected_group.is_empty() || observed_group.is_empty() {
            let (component, expected_value, observed_value) =
                if expected_group.is_empty() {
                    (
                        "path_added",
                        "0".to_string(),
                        observed_group.len().to_string(),
                    )
                } else {
                    (
                        "path_removed",
                        expected_group.len().to_string(),
                        "0".to_string(),
                    )
                };
            push_difference(
                all,
                repo_id,
                Some(observed),
                component,
                Some(path.to_string()),
                Some(expected_value),
                Some(observed_value),
            );
            continue;
        }
        if expected_group.len() != observed_group.len() {
            push_difference(
                all,
                repo_id,
                Some(observed),
                "path_occurrence_count",
                Some(path.to_string()),
                Some(expected_group.len().to_string()),
                Some(observed_group.len().to_string()),
            );
        }
        for (expected_path, observed_path) in
            expected_group.iter().zip(observed_group.iter())
        {
            compare_path(
                all,
                repo_id,
                observed,
                path,
                expected_path,
                observed_path,
            );
        }
    }
}

fn compare_path(
    all: &mut Vec<FingerprintDifferenceWire>,
    repo_id: &str,
    repository: &RepositoryObservationWire,
    path: &str,
    expected: &ObservedPathWire,
    observed: &ObservedPathWire,
) {
    compare_path_value(
        all,
        repo_id,
        repository,
        path,
        "xy",
        &expected.xy,
        &observed.xy,
    );
    compare_path_value(
        all,
        repo_id,
        repository,
        path,
        "content_hash",
        &expected.content_hash,
        &observed.content_hash,
    );
    compare_path_value(
        all,
        repo_id,
        repository,
        path,
        "mode",
        &expected.mode,
        &observed.mode,
    );
    compare_value(
        all,
        repo_id,
        repository,
        "path_kind",
        path_kind_name(expected.kind),
        path_kind_name(observed.kind),
    );
    compare_value(
        all,
        repo_id,
        repository,
        "protected",
        &expected.protected.to_string(),
        &observed.protected.to_string(),
    );
    compare_value(
        all,
        repo_id,
        repository,
        "foreign",
        &expected.foreign.to_string(),
        &observed.foreign.to_string(),
    );
}

fn compare_path_value(
    all: &mut Vec<FingerprintDifferenceWire>,
    repo_id: &str,
    repository: &RepositoryObservationWire,
    path: &str,
    component: &str,
    expected: &Option<String>,
    observed: &Option<String>,
) {
    if expected != observed {
        push_difference(
            all,
            repo_id,
            Some(repository),
            component,
            Some(path.to_string()),
            expected.clone(),
            observed.clone(),
        );
    }
}

fn compare_value(
    all: &mut Vec<FingerprintDifferenceWire>,
    repo_id: &str,
    repository: &RepositoryObservationWire,
    component: &str,
    expected: &str,
    observed: &str,
) {
    if expected != observed {
        push_difference(
            all,
            repo_id,
            Some(repository),
            component,
            None,
            Some(expected.to_string()),
            Some(observed.to_string()),
        );
    }
}

fn group_paths(
    paths: &[ObservedPathWire],
) -> BTreeMap<&str, Vec<&ObservedPathWire>> {
    let mut grouped = BTreeMap::<&str, Vec<&ObservedPathWire>>::new();
    for path in paths {
        grouped.entry(path.path.as_str()).or_default().push(path);
    }
    for group in grouped.values_mut() {
        group.sort_by_key(|path| path_encoding(path));
    }
    grouped
}

fn path_encoding(path: &ObservedPathWire) -> String {
    serde_json::to_string(path).unwrap_or_else(|_| path.path.clone())
}

fn path_order_summary(paths: &[ObservedPathWire]) -> String {
    paths
        .iter()
        .map(|path| path.path.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn path_kind_name(kind: ObservedPathKindWire) -> &'static str {
    match kind {
        ObservedPathKindWire::File => "file",
        ObservedPathKindWire::Symlink => "symlink",
        ObservedPathKindWire::Deleted => "deleted",
        ObservedPathKindWire::Untracked => "untracked",
        ObservedPathKindWire::Other => "other",
    }
}

fn push_difference(
    all: &mut Vec<FingerprintDifferenceWire>,
    repo_id: &str,
    repository: Option<&RepositoryObservationWire>,
    component: &str,
    path: Option<String>,
    expected: Option<String>,
    observed: Option<String>,
) {
    all.push(FingerprintDifferenceWire {
        repo_id: repo_id.to_string(),
        repo_name: repository.map(|repo| repo.name.clone()),
        repo_kind: repository.map(|repo| repo.kind.clone()),
        component: component.to_string(),
        path,
        expected,
        observed,
    });
}

fn difference_order(
    left: &FingerprintDifferenceWire,
    right: &FingerprintDifferenceWire,
) -> std::cmp::Ordering {
    (
        &left.repo_id,
        &left.component,
        &left.path,
        &left.expected,
        &left.observed,
        &left.repo_name,
        &left.repo_kind,
    )
        .cmp(&(
            &right.repo_id,
            &right.component,
            &right.path,
            &right.expected,
            &right.observed,
            &right.repo_name,
            &right.repo_kind,
        ))
}

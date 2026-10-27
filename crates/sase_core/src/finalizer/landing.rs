//! Repository landing-proof assessment for failed-close recovery.
//!
//! The host supplies already-normalized marker, attempt, and identity facts.
//! This module decides whether one obligated repository has proven coverage
//! for the outstanding attempt. It performs no filesystem or git I/O.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::selection::{validate_list_len, validate_required_text};
use super::FinalizerError;

/// One commit-ledger, checkpoint, or operation-record marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingMarkerFactWire {
    pub cwd: String,
    pub run_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed: Option<bool>,
    #[serde(default)]
    pub settled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_digest: Option<String>,
}

/// One recorded stitch attempt for a repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingAttemptFactWire {
    pub repo_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dirty_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_file: Option<String>,
}

/// Host-normalized facts for one obligated repository.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryLandingProofRequestWire {
    pub run_id: String,
    pub repo_path: String,
    #[serde(default)]
    pub markers: Vec<LandingMarkerFactWire>,
    #[serde(default)]
    pub attempts: Vec<LandingAttemptFactWire>,
    /// Prepared-intent source run whose closes are being assessed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    /// Monitor/executor run that wrote markers for a prepared completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_run_id: Option<String>,
}

/// Whether one obligated repository has proven landing coverage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryLandingProofWire {
    pub landed: bool,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covering_commit_sha: Option<String>,
}

/// Assess whether one repository's outstanding attempt is proven landed.
pub fn assess_repository_landing_proof(
    request: &RepositoryLandingProofRequestWire,
) -> Result<RepositoryLandingProofWire, FinalizerError> {
    validate_required_text(&request.run_id, "run_id")?;
    validate_required_text(&request.repo_path, "repo_path")?;
    validate_list_len(request.markers.len(), "markers")?;
    validate_list_len(request.attempts.len(), "attempts")?;
    if let Some(source) = request.source_run_id.as_deref() {
        validate_required_text(source, "source_run_id")?;
    }
    if let Some(executor) = request.executor_run_id.as_deref() {
        validate_required_text(executor, "executor_run_id")?;
    }

    let accepted_runs = accepted_run_ids(request);
    let markers: Vec<&LandingMarkerFactWire> = request
        .markers
        .iter()
        .filter(|marker| marker.cwd == request.repo_path)
        .filter(|marker| accepted_runs.contains(marker.run_id.as_str()))
        .filter(|marker| !marker.settled && marker.superseded_by.is_none())
        .collect();
    let attempts: Vec<&LandingAttemptFactWire> = request
        .attempts
        .iter()
        .filter(|attempt| attempt.repo_path == request.repo_path)
        .collect();

    let Some(latest_marker) = markers.last().copied() else {
        return Ok(not_landed("no landing marker"));
    };
    if let Some(unlanded) = marker_unlanded(latest_marker) {
        return Ok(unlanded);
    }
    let latest_attempt = attempts.last().copied();
    if let Some(attempt) = latest_attempt {
        if !marker_covers_attempt(latest_marker, attempt, &markers, &attempts) {
            let sha = latest_marker
                .commit_sha
                .as_deref()
                .unwrap_or("")
                .chars()
                .take(12)
                .collect::<String>();
            return Ok(not_landed(format!(
                "newer attempt is not covered by commit {sha}"
            )));
        }
    }
    let sha = latest_marker
        .commit_sha
        .as_deref()
        .unwrap_or("")
        .chars()
        .take(12)
        .collect::<String>();
    Ok(RepositoryLandingProofWire {
        landed: true,
        detail: format!("commit {sha}"),
        covering_commit_sha: latest_marker.commit_sha.clone(),
    })
}

fn accepted_run_ids(
    request: &RepositoryLandingProofRequestWire,
) -> BTreeSet<&str> {
    let mut runs = BTreeSet::new();
    runs.insert(request.run_id.as_str());
    if let (Some(source), Some(executor)) = (
        request.source_run_id.as_deref(),
        request.executor_run_id.as_deref(),
    ) {
        if source == request.run_id {
            runs.insert(executor);
        }
    }
    runs
}

fn marker_unlanded(
    marker: &LandingMarkerFactWire,
) -> Option<RepositoryLandingProofWire> {
    let sha = marker
        .commit_sha
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let Some(sha) = sha else {
        return Some(not_landed("no landing marker"));
    };
    if marker.pushed == Some(false) {
        let short: String = sha.chars().take(12).collect();
        return Some(not_landed(format!("commit {short} is unpushed")));
    }
    None
}

fn marker_covers_attempt(
    latest_marker: &LandingMarkerFactWire,
    attempt: &LandingAttemptFactWire,
    markers: &[&LandingMarkerFactWire],
    attempts: &[&LandingAttemptFactWire],
) -> bool {
    if let Some(operation_id) = attempt.operation_id.as_deref() {
        return markers.iter().any(|marker| {
            marker.operation_id.as_deref() == Some(operation_id)
                && marker_unlanded(marker).is_none()
        });
    }
    if fingerprints_conflict(latest_marker, attempt) {
        return false;
    }
    // An unmarked latest attempt is outstanding unless it is the sole
    // attempt that produced this marker.
    attempts.len() == 1
}

fn fingerprints_conflict(
    marker: &LandingMarkerFactWire,
    attempt: &LandingAttemptFactWire,
) -> bool {
    optional_mismatch(marker.head.as_deref(), attempt.head.as_deref())
        || optional_mismatch(
            marker.dirty_fingerprint.as_deref(),
            attempt.dirty_fingerprint.as_deref(),
        )
        || optional_mismatch(
            marker.message_digest.as_deref(),
            attempt.message_digest.as_deref(),
        )
}

fn optional_mismatch(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(left), Some(right)) if left != right)
}

fn not_landed(detail: impl Into<String>) -> RepositoryLandingProofWire {
    RepositoryLandingProofWire {
        landed: false,
        detail: detail.into(),
        covering_commit_sha: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(
        markers: Vec<LandingMarkerFactWire>,
        attempts: Vec<LandingAttemptFactWire>,
    ) -> RepositoryLandingProofRequestWire {
        RepositoryLandingProofRequestWire {
            run_id: "run-1".into(),
            repo_path: "/repo".into(),
            markers,
            attempts,
            source_run_id: None,
            executor_run_id: None,
        }
    }

    fn marker(
        operation_id: &str,
        sha: &str,
        pushed: Option<bool>,
    ) -> LandingMarkerFactWire {
        LandingMarkerFactWire {
            cwd: "/repo".into(),
            run_id: "run-1".into(),
            operation_id: Some(operation_id.into()),
            commit_sha: Some(sha.into()),
            pushed,
            settled: false,
            superseded_by: None,
            head: None,
            dirty_fingerprint: None,
            message_digest: None,
        }
    }

    fn attempt(operation_id: Option<&str>) -> LandingAttemptFactWire {
        LandingAttemptFactWire {
            repo_path: "/repo".into(),
            operation_id: operation_id.map(str::to_string),
            head: None,
            dirty_fingerprint: None,
            message_digest: None,
            message_file: None,
        }
    }

    #[test]
    fn latest_unpushed_marker_wins_over_older_pushed_marker() {
        let proof = assess_repository_landing_proof(&request(
            vec![
                marker("op-old", &"a".repeat(40), Some(true)),
                marker("op-new", &"b".repeat(40), Some(false)),
            ],
            vec![],
        ))
        .unwrap();
        assert!(!proof.landed);
        assert!(proof.detail.contains("unpushed"));
    }

    #[test]
    fn older_success_does_not_cover_a_newer_unmarked_attempt() {
        let proof = assess_repository_landing_proof(&request(
            vec![marker("op-old", &"a".repeat(40), Some(true))],
            vec![attempt(Some("op-old")), attempt(None)],
        ))
        .unwrap();
        assert!(!proof.landed);
        assert!(proof.detail.contains("not covered"));
    }

    #[test]
    fn successful_retry_covers_the_latest_attempt() {
        let proof = assess_repository_landing_proof(&request(
            vec![
                marker("op-old", &"a".repeat(40), Some(true)),
                marker("op-new", &"c".repeat(40), Some(true)),
            ],
            vec![attempt(Some("op-old")), attempt(Some("op-new"))],
        ))
        .unwrap();
        assert!(proof.landed);
        assert_eq!(proof.covering_commit_sha, Some("c".repeat(40)));
    }

    #[test]
    fn unrelated_run_or_repo_markers_prove_nothing() {
        let mut other_run = marker("op", &"a".repeat(40), Some(true));
        other_run.run_id = "run-2".into();
        let mut other_repo = marker("op", &"a".repeat(40), Some(true));
        other_repo.cwd = "/other".into();
        let proof = assess_repository_landing_proof(&request(
            vec![other_run, other_repo],
            vec![],
        ))
        .unwrap();
        assert!(!proof.landed);
        assert_eq!(proof.detail, "no landing marker");
    }

    #[test]
    fn authenticated_executor_markers_cover_source_closes() {
        let mut marker = marker("op", &"a".repeat(40), Some(true));
        marker.run_id = "monitor-run".into();
        let mut request = request(vec![marker], vec![attempt(Some("op"))]);
        request.source_run_id = Some("run-1".into());
        request.executor_run_id = Some("monitor-run".into());
        let proof = assess_repository_landing_proof(&request).unwrap();
        assert!(proof.landed);
    }

    #[test]
    fn unauthenticated_cross_run_markers_are_ignored() {
        let mut marker = marker("op", &"a".repeat(40), Some(true));
        marker.run_id = "other-run".into();
        let mut request = request(vec![marker], vec![]);
        request.source_run_id = Some("run-1".into());
        request.executor_run_id = Some("monitor-run".into());
        let proof = assess_repository_landing_proof(&request).unwrap();
        assert!(!proof.landed);
        assert_eq!(proof.detail, "no landing marker");
    }

    #[test]
    fn settled_marker_is_not_a_landing() {
        let mut marker = marker("op", &"a".repeat(40), Some(true));
        marker.settled = true;
        let proof =
            assess_repository_landing_proof(&request(vec![marker], vec![]))
                .unwrap();
        assert!(!proof.landed);
    }
}

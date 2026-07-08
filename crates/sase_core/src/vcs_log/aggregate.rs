//! Frontend-agnostic interleaving of per-repo commit lists into one
//! chronological timeline.
//!
//! Mirrors `sase/src/sase/core/vcs_log_facade.py::_aggregate_commit_log_python`
//! byte-for-byte. Pure over data the host already collected (one commit
//! list per repo); no I/O.

use super::wire::{AggregatedCommitWire, VcsCommitWire};

/// Merge per-repo commit lists into a single timeline sorted newest-first.
///
/// Each input entry is `(repo_label, commits)`. The result flattens every
/// commit into an [`AggregatedCommitWire`] tagged with its repo label,
/// then sorts:
///
/// 1. by `timestamp` descending (newest first);
/// 2. tie-broken by `repo` ascending, then `full_id` ascending.
///
/// The deterministic tie-break keeps identical-timestamp commits (common
/// in scripted batches) in a stable order run-to-run, which matters for
/// snapshot tests. The merged list is truncated to `limit` entries.
pub fn aggregate_commit_log(
    repos: Vec<(String, Vec<VcsCommitWire>)>,
    limit: usize,
) -> Vec<AggregatedCommitWire> {
    let mut all: Vec<AggregatedCommitWire> = Vec::new();
    for (repo, commits) in repos {
        for commit in commits {
            all.push(AggregatedCommitWire {
                repo: repo.clone(),
                commit,
            });
        }
    }
    all.sort_by(|a, b| {
        b.commit
            .timestamp
            .cmp(&a.commit.timestamp)
            .then_with(|| a.repo.cmp(&b.repo))
            .then_with(|| a.commit.full_id.cmp(&b.commit.full_id))
    });
    all.truncate(limit);
    all
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(full: &str, ts: i64) -> VcsCommitWire {
        VcsCommitWire {
            full_id: full.to_string(),
            short_id: full.chars().take(7).collect(),
            author_name: "A".to_string(),
            author_email: "a@x".to_string(),
            timestamp: ts,
            subject: format!("subject {full}"),
            body: String::new(),
        }
    }

    #[test]
    fn empty_input_returns_empty() {
        assert!(aggregate_commit_log(Vec::new(), 20).is_empty());
    }

    #[test]
    fn interleaves_repos_by_timestamp_desc() {
        let repos = vec![
            ("sase".to_string(), vec![commit("a", 300), commit("b", 100)]),
            ("sase-core".to_string(), vec![commit("c", 200)]),
        ];
        let out = aggregate_commit_log(repos, 20);
        let order: Vec<(&str, &str)> = out
            .iter()
            .map(|r| (r.repo.as_str(), r.commit.full_id.as_str()))
            .collect();
        assert_eq!(
            order,
            vec![("sase", "a"), ("sase-core", "c"), ("sase", "b")]
        );
    }

    #[test]
    fn equal_timestamp_tie_break_is_repo_then_full_id() {
        let repos = vec![
            ("zebra".to_string(), vec![commit("x", 500)]),
            (
                "alpha".to_string(),
                vec![commit("m", 500), commit("a", 500)],
            ),
        ];
        let out = aggregate_commit_log(repos, 20);
        let order: Vec<(&str, &str)> = out
            .iter()
            .map(|r| (r.repo.as_str(), r.commit.full_id.as_str()))
            .collect();
        // Same timestamp -> repo asc ("alpha" before "zebra"), then
        // full_id asc within a repo ("a" before "m").
        assert_eq!(order, vec![("alpha", "a"), ("alpha", "m"), ("zebra", "x")]);
    }

    #[test]
    fn truncates_to_limit() {
        let repos = vec![(
            "sase".to_string(),
            vec![commit("a", 500), commit("b", 400), commit("c", 300)],
        )];
        let out = aggregate_commit_log(repos, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].commit.full_id, "a");
        assert_eq!(out[1].commit.full_id, "b");
    }

    #[test]
    fn limit_zero_returns_empty() {
        let repos = vec![("sase".to_string(), vec![commit("a", 500)])];
        assert!(aggregate_commit_log(repos, 0).is_empty());
    }

    #[test]
    fn aggregated_row_serializes_flat() {
        let repos = vec![("sase".to_string(), vec![commit("a1b2c3d", 500)])];
        let out = aggregate_commit_log(repos, 20);
        let value = serde_json::to_value(&out[0]).unwrap();
        let obj = value.as_object().unwrap();
        // Flattened: no nested "commit" key, repo sits beside the fields.
        assert!(obj.contains_key("repo"));
        assert!(obj.contains_key("full_id"));
        assert!(obj.contains_key("timestamp"));
        assert!(!obj.contains_key("commit"));
        assert_eq!(obj["repo"], "sase");
        assert_eq!(obj["timestamp"], 500);
    }
}

//! Local/remote presence classification for VCS log commits.
//!
//! Pure over commit records and precomputed SHA sets. The host remains
//! responsible for running `git rev-list`.

use std::collections::HashSet;

use super::wire::{CommitPresenceWire, VcsCommitWire};

/// Stamp each commit with local/remote presence.
///
/// `ahead_ids` are commits in local `HEAD` but not the compared remote ref.
/// `behind_ids` are commits in the compared remote ref but not local `HEAD`.
/// Any commit in neither set is present in both refs and is marked
/// [`CommitPresenceWire::Synced`].
pub fn classify_commit_presence(
    commits: Vec<VcsCommitWire>,
    ahead_ids: Vec<String>,
    behind_ids: Vec<String>,
) -> Vec<VcsCommitWire> {
    let ahead: HashSet<String> = ahead_ids.into_iter().collect();
    let behind: HashSet<String> = behind_ids.into_iter().collect();
    commits
        .into_iter()
        .map(|mut commit| {
            commit.presence = if ahead.contains(&commit.full_id) {
                CommitPresenceWire::LocalOnly
            } else if behind.contains(&commit.full_id) {
                CommitPresenceWire::RemoteOnly
            } else {
                CommitPresenceWire::Synced
            };
            commit
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn commit(full: &str) -> VcsCommitWire {
        VcsCommitWire {
            full_id: full.to_string(),
            short_id: full.chars().take(7).collect(),
            author_name: "A".to_string(),
            author_email: "a@x".to_string(),
            timestamp: 1,
            subject: format!("subject {full}"),
            body: String::new(),
            presence: CommitPresenceWire::Unknown,
        }
    }

    #[test]
    fn classifies_synced_ahead_and_behind() {
        let out = classify_commit_presence(
            vec![commit("synced"), commit("ahead"), commit("behind")],
            vec!["ahead".to_string()],
            vec!["behind".to_string()],
        );

        let states: Vec<CommitPresenceWire> =
            out.iter().map(|commit| commit.presence).collect();
        assert_eq!(
            states,
            vec![
                CommitPresenceWire::Synced,
                CommitPresenceWire::LocalOnly,
                CommitPresenceWire::RemoteOnly,
            ]
        );
    }

    #[test]
    fn ahead_wins_when_sets_overlap() {
        let out = classify_commit_presence(
            vec![commit("both")],
            vec!["both".to_string()],
            vec!["both".to_string()],
        );

        assert_eq!(out[0].presence, CommitPresenceWire::LocalOnly);
    }
}

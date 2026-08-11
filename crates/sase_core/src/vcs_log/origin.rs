//! Commit origin classification for VCS log commits.
//!
//! A commit is considered SASE-originated when it carries at least one
//! terminal structured `SASE_*` commit-footer tag. Commits without a SASE
//! footer are manual, which is also the wire default for legacy records.

use serde::{Deserialize, Serialize};

use crate::commit_footer::parse_commit_footer;

/// Where a commit originated from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum CommitOriginWire {
    /// Commit was authored outside the SASE tracked commit workflow.
    #[default]
    Manual,
    /// Commit was authored by the SASE tracked commit workflow.
    Sase,
}

/// Classify a full commit message as manual or SASE-originated.
pub fn classify_commit_origin(message: &str) -> CommitOriginWire {
    let footer = parse_commit_footer(message);
    if footer.tags.is_empty() {
        CommitOriginWire::Manual
    } else {
        CommitOriginWire::Sase
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_plain_commit_as_manual() {
        assert_eq!(
            classify_commit_origin("fix: handwritten\n\nDetails"),
            CommitOriginWire::Manual,
        );
    }

    #[test]
    fn classifies_sase_footer_as_sase() {
        assert_eq!(
            classify_commit_origin(
                "fix: tracked\n\nDetails\n\nSASE_STITCH=1\nSASE_BEAD=sase-1"
            ),
            CommitOriginWire::Sase,
        );
    }

    #[test]
    fn ignores_tag_shaped_body_text() {
        assert_eq!(
            classify_commit_origin(
                "fix: handwritten\n\nSASE_STITCH=not terminal\n\nMore"
            ),
            CommitOriginWire::Manual,
        );
    }
}

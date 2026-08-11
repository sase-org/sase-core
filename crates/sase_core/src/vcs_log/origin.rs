//! Commit origin classification for VCS log commits.
//!
//! The classifier reads the terminal structured commit-footer block. The
//! `TYPE` key is canonicalized by the footer parser, so both `TYPE=` and
//! `SASE_TYPE=` drive the same precedence rule.

use serde::{Deserialize, Serialize};

use crate::commit_footer::parse_commit_footer;

/// Where a commit originated from.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default,
)]
#[serde(rename_all = "snake_case")]
pub enum CommitOriginWire {
    /// Commit has no SASE provenance footer.
    #[default]
    Manual,
    /// Commit was created through `sase stitch create`.
    Stitch,
    /// Commit was created automatically by another SASE command.
    Auto,
}

/// Classify a full commit message as stitch, auto, or manual.
pub fn classify_commit_origin(message: &str) -> CommitOriginWire {
    let footer = parse_commit_footer(message);

    if let Some(tag) = footer.tags.iter().rev().find(|tag| tag.key == "TYPE") {
        if tag.label.trim().eq_ignore_ascii_case("stitch") {
            return CommitOriginWire::Stitch;
        }
        return CommitOriginWire::Auto;
    }

    if footer
        .tags
        .iter()
        .any(|tag| matches!(tag.key.as_str(), "AGENT" | "BEAD" | "PLAN"))
    {
        return CommitOriginWire::Stitch;
    }

    CommitOriginWire::Manual
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
    fn type_stitch_classifies_as_stitch() {
        assert_eq!(
            classify_commit_origin(
                "fix: tracked\n\nDetails\n\nSASE_TYPE=stitch\nSASE_AGENT=sase-1"
            ),
            CommitOriginWire::Stitch,
        );
    }

    #[test]
    fn legacy_type_spelling_classifies_as_stitch() {
        assert_eq!(
            classify_commit_origin("fix: tracked\n\nTYPE=stitch"),
            CommitOriginWire::Stitch,
        );
    }

    #[test]
    fn non_stitch_type_classifies_as_auto() {
        assert_eq!(
            classify_commit_origin("fix: automatic\n\nSASE_TYPE=sase init"),
            CommitOriginWire::Auto,
        );
    }

    #[test]
    fn legacy_agent_bead_or_plan_classifies_as_stitch() {
        assert_eq!(
            classify_commit_origin("fix: legacy\n\nSASE_AGENT=sase-1"),
            CommitOriginWire::Stitch,
        );
        assert_eq!(
            classify_commit_origin("fix: legacy\n\nSASE_BEAD=sase-1"),
            CommitOriginWire::Stitch,
        );
        assert_eq!(
            classify_commit_origin("fix: legacy\n\nSASE_PLAN=202608/p.md"),
            CommitOriginWire::Stitch,
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

//! Strict merge-commit subject summarization.
//!
//! The parser recognizes only well-known git/GitHub merge subjects. A
//! subject that does not fully match one of those shapes returns `None`
//! so callers can render the raw subject without risking a misleading
//! condensation.

use serde::{Deserialize, Serialize};

/// Recognized merge-subject family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeSummaryKindWire {
    PullRequest,
    Branch,
    RemoteBranch,
}

/// Structured summary for a recognized merge commit subject.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeSummaryWire {
    pub kind: MergeSummaryKindWire,
    pub reference: Option<String>,
    pub source: Option<String>,
    pub target: Option<String>,
    pub headline: Option<String>,
}

/// Parse a merge commit subject and body into a structured summary.
pub fn parse_merge_summary(
    subject: &str,
    body: &str,
) -> Option<MergeSummaryWire> {
    parse_pull_request_summary(subject, body)
        .or_else(|| parse_branch_summary(subject, body))
        .or_else(|| parse_remote_branch_summary(subject, body))
}

fn parse_pull_request_summary(
    subject: &str,
    body: &str,
) -> Option<MergeSummaryWire> {
    let rest = subject.strip_prefix("Merge pull request #")?;
    let (reference, source) = rest.split_once(" from ")?;
    if reference.is_empty()
        || !reference.bytes().all(|byte| byte.is_ascii_digit())
        || source.is_empty()
    {
        return None;
    }
    Some(MergeSummaryWire {
        kind: MergeSummaryKindWire::PullRequest,
        reference: Some(reference.to_string()),
        source: Some(source.to_string()),
        target: None,
        headline: first_headline(body),
    })
}

fn parse_branch_summary(subject: &str, body: &str) -> Option<MergeSummaryWire> {
    parse_quoted_branch_summary(
        subject.strip_prefix("Merge branch '")?,
        body,
        MergeSummaryKindWire::Branch,
    )
}

fn parse_remote_branch_summary(
    subject: &str,
    body: &str,
) -> Option<MergeSummaryWire> {
    parse_quoted_branch_summary(
        subject.strip_prefix("Merge remote-tracking branch '")?,
        body,
        MergeSummaryKindWire::RemoteBranch,
    )
}

fn parse_quoted_branch_summary(
    rest: &str,
    body: &str,
    kind: MergeSummaryKindWire,
) -> Option<MergeSummaryWire> {
    let (source, suffix) = rest.split_once('\'')?;
    if source.is_empty() {
        return None;
    }
    let target = if suffix.is_empty() {
        None
    } else {
        let target = suffix.strip_prefix(" into ")?;
        if target.is_empty() {
            return None;
        }
        Some(target.to_string())
    };
    Some(MergeSummaryWire {
        kind,
        reference: Some(source.to_string()),
        source: Some(source.to_string()),
        target,
        headline: first_headline(body),
    })
}

fn first_headline(body: &str) -> Option<String> {
    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_github_pull_request_summary() {
        let summary = parse_merge_summary(
            "Merge pull request #123 from org/feature-branch",
            "\nAdd useful feature\n\nBody text",
        )
        .unwrap();

        assert_eq!(summary.kind, MergeSummaryKindWire::PullRequest);
        assert_eq!(summary.reference.as_deref(), Some("123"));
        assert_eq!(summary.source.as_deref(), Some("org/feature-branch"));
        assert_eq!(summary.target, None);
        assert_eq!(summary.headline.as_deref(), Some("Add useful feature"));
    }

    #[test]
    fn pull_request_empty_body_has_no_headline() {
        let summary = parse_merge_summary(
            "Merge pull request #123 from org/feature-branch",
            "",
        )
        .unwrap();

        assert_eq!(summary.headline, None);
    }

    #[test]
    fn parses_branch_summary() {
        let summary =
            parse_merge_summary("Merge branch 'feature-branch'", "headline")
                .unwrap();

        assert_eq!(summary.kind, MergeSummaryKindWire::Branch);
        assert_eq!(summary.reference.as_deref(), Some("feature-branch"));
        assert_eq!(summary.source.as_deref(), Some("feature-branch"));
        assert_eq!(summary.target, None);
        assert_eq!(summary.headline.as_deref(), Some("headline"));
    }

    #[test]
    fn parses_branch_summary_with_target() {
        let summary = parse_merge_summary(
            "Merge branch 'feature-branch' into master",
            "headline",
        )
        .unwrap();

        assert_eq!(summary.kind, MergeSummaryKindWire::Branch);
        assert_eq!(summary.reference.as_deref(), Some("feature-branch"));
        assert_eq!(summary.source.as_deref(), Some("feature-branch"));
        assert_eq!(summary.target.as_deref(), Some("master"));
    }

    #[test]
    fn parses_remote_branch_summary() {
        let summary = parse_merge_summary(
            "Merge remote-tracking branch 'origin/feature'",
            "headline",
        )
        .unwrap();

        assert_eq!(summary.kind, MergeSummaryKindWire::RemoteBranch);
        assert_eq!(summary.reference.as_deref(), Some("origin/feature"));
        assert_eq!(summary.source.as_deref(), Some("origin/feature"));
        assert_eq!(summary.target, None);
    }

    #[test]
    fn parses_remote_branch_summary_with_target() {
        let summary = parse_merge_summary(
            "Merge remote-tracking branch 'origin/feature' into master",
            "headline",
        )
        .unwrap();

        assert_eq!(summary.kind, MergeSummaryKindWire::RemoteBranch);
        assert_eq!(summary.reference.as_deref(), Some("origin/feature"));
        assert_eq!(summary.source.as_deref(), Some("origin/feature"));
        assert_eq!(summary.target.as_deref(), Some("master"));
    }

    #[test]
    fn unrecognized_subject_returns_none() {
        assert_eq!(parse_merge_summary("feat: regular commit", "body"), None);
    }

    #[test]
    fn merge_prefix_without_known_shape_returns_none() {
        assert_eq!(
            parse_merge_summary("Merge whatever the maintainer wanted", "body"),
            None
        );
    }

    #[test]
    fn partial_pull_request_shape_returns_none() {
        assert_eq!(
            parse_merge_summary(
                "Merge pull request #abc from org/branch",
                "body"
            ),
            None
        );
    }
}

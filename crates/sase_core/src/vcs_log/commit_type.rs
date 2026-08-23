//! Derived type labels for VCS-log query rows.
//!
//! These labels are not persisted on [`VcsCommitWire`]. They are a
//! provider-neutral query facet derived from the existing commit wire fields
//! and terminal SASE footer grammar.

use crate::commit_footer::parse_commit_footer;

use super::origin::{classify_commit_origin_from_footer, CommitOriginWire};
use super::wire::VcsCommitWire;

/// Classify a full commit message plus merge structure into query type labels.
pub fn classify_commit_types(message: &str, is_merge: bool) -> Vec<String> {
    let footer = parse_commit_footer(message);
    let mut labels: Vec<String> = Vec::new();

    push_label(
        &mut labels,
        provenance_label(classify_commit_origin_from_footer(&footer)),
    );

    if let Some(type_tag) =
        footer.tags.iter().rev().find(|tag| tag.key == "TYPE")
    {
        if let Some(label) = normalize_type_label(&type_tag.label) {
            push_label(&mut labels, &label);
        }
    }

    if is_merge {
        push_label(&mut labels, "merge");
    }
    if footer
        .tags
        .iter()
        .any(|tag| tag.key == "PATCH" && !tag.label.trim().is_empty())
    {
        push_label(&mut labels, "patch");
    }

    labels
}

/// Classify an existing VCS-log commit without changing its wire shape.
pub fn classify_commit_types_for_commit(commit: &VcsCommitWire) -> Vec<String> {
    classify_commit_types(&commit_message(commit), commit.is_merge())
}

fn commit_message(commit: &VcsCommitWire) -> String {
    if commit.body.is_empty() {
        commit.subject.clone()
    } else {
        format!("{}\n\n{}", commit.subject, commit.body)
    }
}

fn provenance_label(origin: CommitOriginWire) -> &'static str {
    match origin {
        CommitOriginWire::Manual => "manual",
        CommitOriginWire::Stitch => "stitch",
        CommitOriginWire::Auto => "automatic",
    }
}

fn normalize_type_label(value: &str) -> Option<String> {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        None
    } else if collapsed.eq_ignore_ascii_case("auto") {
        Some("automatic".to_string())
    } else {
        Some(collapsed.to_ascii_lowercase())
    }
}

fn push_label(labels: &mut Vec<String>, label: &str) {
    if !labels.iter().any(|existing| existing == label) {
        labels.push(label.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::vcs_log::wire::CommitPresenceWire;

    fn commit(subject: &str, body: &str, parents: &[&str]) -> VcsCommitWire {
        VcsCommitWire {
            full_id: "abcdef".to_string(),
            short_id: "abcdef".to_string(),
            author_name: "Ada".to_string(),
            author_email: "ada@example.com".to_string(),
            timestamp: 1,
            parent_ids: parents.iter().map(|value| value.to_string()).collect(),
            subject: subject.to_string(),
            body: body.to_string(),
            presence: CommitPresenceWire::Unknown,
            origin: CommitOriginWire::Manual,
        }
    }

    #[test]
    fn manual_commit_has_only_manual_type() {
        assert_eq!(
            classify_commit_types("fix: handwritten\n\nDetails", false),
            vec!["manual".to_string()],
        );
    }

    #[test]
    fn automatic_commit_includes_concrete_terminal_type() {
        assert_eq!(
            classify_commit_types("fix: generated\n\nSASE_TYPE=SDD", false),
            vec!["automatic".to_string(), "sdd".to_string()],
        );
    }

    #[test]
    fn stitch_type_deduplicates_provenance_and_concrete_type() {
        assert_eq!(
            classify_commit_types("fix: tracked\n\nSASE_TYPE=stitch", false),
            vec!["stitch".to_string()],
        );
    }

    #[test]
    fn auto_type_alias_deduplicates_to_automatic() {
        assert_eq!(
            classify_commit_types("fix: generated\n\nSASE_TYPE=auto", false),
            vec!["automatic".to_string()],
        );
    }

    #[test]
    fn legacy_stitch_inference_still_uses_stitch_provenance() {
        assert_eq!(
            classify_commit_types("fix: tracked\n\nSASE_AGENT=sase-1", false),
            vec!["stitch".to_string()],
        );
    }

    #[test]
    fn ignores_non_terminal_tag_shaped_text() {
        assert_eq!(
            classify_commit_types(
                "fix: handwritten\n\nSASE_TYPE=sdd\n\nregular body after",
                false,
            ),
            vec!["manual".to_string()],
        );
    }

    #[test]
    fn includes_merge_and_patch_labels_after_concrete_type() {
        assert_eq!(
            classify_commit_types(
                "Merge pull request #1\n\nSASE_TYPE=bead_work\nSASE_PATCH=feat-x",
                true,
            ),
            vec![
                "automatic".to_string(),
                "bead_work".to_string(),
                "merge".to_string(),
                "patch".to_string(),
            ],
        );
    }

    #[test]
    fn legacy_patch_key_and_empty_patch_values_are_handled() {
        assert_eq!(
            classify_commit_types("fix: tracked\n\nPATCH=feat-x", false),
            vec!["manual".to_string(), "patch".to_string()],
        );
        assert_eq!(
            classify_commit_types("fix: tracked\n\nSASE_PATCH=", false),
            vec!["manual".to_string()],
        );
    }

    #[test]
    fn commit_wrapper_uses_subject_body_and_parent_count() {
        let row = commit("Merge branch", "Body\n\nSASE_TYPE=Init", &["a", "b"]);

        assert_eq!(
            classify_commit_types_for_commit(&row),
            vec![
                "automatic".to_string(),
                "init".to_string(),
                "merge".to_string(),
            ],
        );
    }
}

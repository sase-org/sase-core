//! Typed evidence shapes for the node view.
//!
//! This layer owns the evidence wire and the kind classifier. Headline
//! selection and op-record evidence land in `core-run-view-detail`; until
//! then every run reports empty evidence.

use serde::{Deserialize, Serialize};

/// One typed evidence record: `{kind, value, type, display}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunViewEvidenceWire {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_type: Option<String>,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display: Option<String>,
}

/// Classify an evidence kind suffix into its display type, following the
/// §3.4 conventions. The ambiguous commit `result` kind stays `text`.
pub fn classify_evidence_kind(kind: &str) -> &'static str {
    if kind == "result" {
        return "text";
    }
    if kind.ends_with("_sha") {
        return "sha";
    }
    if kind.ends_with("_url") {
        return "url";
    }
    if kind == "bead_id" || kind.ends_with("_bead_id") {
        return "bead";
    }
    if kind.ends_with("_path") {
        return "path";
    }
    if kind.ends_with("_seconds") {
        return "duration";
    }
    if kind == "exit_code" || kind.ends_with("_exit_code") {
        return "exit_code";
    }
    "text"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ambiguous_commit_result_stays_text() {
        assert_eq!(classify_evidence_kind("result"), "text");
        assert_eq!(classify_evidence_kind("commit_sha"), "sha");
        assert_eq!(classify_evidence_kind("pr_url"), "url");
        assert_eq!(classify_evidence_kind("bead_id"), "bead");
        assert_eq!(classify_evidence_kind("repo_path"), "path");
        assert_eq!(classify_evidence_kind("elapsed_seconds"), "duration");
        assert_eq!(classify_evidence_kind("exit_code"), "exit_code");
        assert_eq!(classify_evidence_kind("message"), "text");
    }
}

//! Deterministic policy for launch-time sidecar publication retries.
//!
//! The host owns Git subprocesses, repository locks, integration, recovery
//! refs, and notifications. This module only classifies one observed push
//! result and decides whether another attempt is allowed.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS: u32 = 3;

pub const SIDECAR_PUBLICATION_ACTION_SUCCESS: &str = "success";
pub const SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY: &str =
    "integrate_and_retry";
pub const SIDECAR_PUBLICATION_ACTION_STOP: &str = "stop";

pub const SIDECAR_PUBLICATION_CLASS_SUCCESS: &str = "success";
pub const SIDECAR_PUBLICATION_CLASS_SECRET_SCAN: &str = "rejected_secret_scan";
pub const SIDECAR_PUBLICATION_CLASS_REPOSITORY_RULE: &str =
    "rejected_repository_rule";
pub const SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD: &str =
    "rejected_non_fast_forward";
pub const SIDECAR_PUBLICATION_CLASS_FETCH_FIRST: &str = "rejected_fetch_first";
pub const SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE: &str = "unknown_failure";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SidecarPublicationClassificationWire {
    Success,
    #[serde(rename = "rejected_secret_scan")]
    SecretScanRejected,
    #[serde(rename = "rejected_repository_rule")]
    RepositoryRuleRejected,
    #[serde(rename = "rejected_non_fast_forward")]
    NonFastForward,
    #[serde(rename = "rejected_fetch_first")]
    FetchFirst,
    UnknownFailure,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarPublicationPushRejectionWire {
    pub schema_version: u32,
    pub classification: SidecarPublicationClassificationWire,
    #[serde(default)]
    pub commit_ids: Vec<String>,
    #[serde(default)]
    pub relative_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SidecarPublicationDecisionWire {
    pub schema_version: u32,
    pub action: String,
    pub classification: String,
    pub reason: String,
    pub attempt: u32,
    pub max_attempts: u32,
    pub retryable: bool,
}

pub fn decide_sidecar_publication_after_push(
    returncode: i32,
    stdout: &str,
    stderr: &str,
    attempt: u32,
) -> SidecarPublicationDecisionWire {
    if returncode == 0 {
        return SidecarPublicationDecisionWire {
            schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
            action: SIDECAR_PUBLICATION_ACTION_SUCCESS.to_string(),
            classification: SIDECAR_PUBLICATION_CLASS_SUCCESS.to_string(),
            reason: "git push succeeded".to_string(),
            attempt,
            max_attempts: SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
            retryable: false,
        };
    }

    let classification =
        classify_sidecar_publication_push_result(returncode, stdout, stderr)
            .classification;
    let retryable = matches!(
        classification,
        SidecarPublicationClassificationWire::NonFastForward
            | SidecarPublicationClassificationWire::FetchFirst
    );
    let classification_name = classification.as_str();
    if retryable && attempt < SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS {
        return SidecarPublicationDecisionWire {
            schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
            action: SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY.to_string(),
            classification: classification_name.to_string(),
            reason: "git push was rejected by remote divergence; integrate upstream and retry"
                .to_string(),
            attempt,
            max_attempts: SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
            retryable: true,
        };
    }

    let reason = match classification {
        SidecarPublicationClassificationWire::SecretScanRejected => {
            "git push was rejected by secret scanning; candidate content is blocked"
        }
        SidecarPublicationClassificationWire::RepositoryRuleRejected => {
            "git push was rejected by repository rules; no secret was confirmed"
        }
        _ if retryable => {
            "git push was rejected by remote divergence but the retry limit is exhausted"
        }
        _ => "git push failed without a recognized remote rejection",
    };
    SidecarPublicationDecisionWire {
        schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
        action: SIDECAR_PUBLICATION_ACTION_STOP.to_string(),
        classification: classification_name.to_string(),
        reason: reason.to_string(),
        attempt,
        max_attempts: SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
        retryable,
    }
}

impl SidecarPublicationClassificationWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => SIDECAR_PUBLICATION_CLASS_SUCCESS,
            Self::SecretScanRejected => SIDECAR_PUBLICATION_CLASS_SECRET_SCAN,
            Self::RepositoryRuleRejected => {
                SIDECAR_PUBLICATION_CLASS_REPOSITORY_RULE
            }
            Self::NonFastForward => SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD,
            Self::FetchFirst => SIDECAR_PUBLICATION_CLASS_FETCH_FIRST,
            Self::UnknownFailure => SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE,
        }
    }
}

/// Classify untrusted remote output into a sanitized rejection contract.
/// Secret-scanning and repository-rule failures take precedence over generic
/// divergence hints so callers never retry bytes a remote explicitly rejected.
pub fn classify_sidecar_publication_push_result(
    returncode: i32,
    stdout: &str,
    stderr: &str,
) -> SidecarPublicationPushRejectionWire {
    let combined = format!("{stderr}\n{stdout}");
    let lower = combined.to_ascii_lowercase();
    let classification = if returncode == 0 {
        SidecarPublicationClassificationWire::Success
    } else if lower.contains("gh013")
        || lower.contains("push cannot contain secrets")
        || (lower.contains("secret scanning")
            && lower.contains("push protection"))
    {
        SidecarPublicationClassificationWire::SecretScanRejected
    } else if lower.contains("repository rule violation")
        || lower.contains("ruleset violation")
        || lower.contains("protected branch update failed")
    {
        SidecarPublicationClassificationWire::RepositoryRuleRejected
    } else if lower.contains("non-fast-forward") {
        SidecarPublicationClassificationWire::NonFastForward
    } else if lower.contains("fetch first")
        || lower
            .contains("updates were rejected because the remote contains work")
    {
        SidecarPublicationClassificationWire::FetchFirst
    } else {
        SidecarPublicationClassificationWire::UnknownFailure
    };
    let commit_ids = extract_labeled_values(&combined, "commit:")
        .into_iter()
        .filter(|value| valid_commit_id(value))
        .map(|value| value.to_ascii_lowercase())
        .collect();
    let relative_paths = extract_labeled_values(&combined, "path:")
        .into_iter()
        .filter(|value| valid_relative_path(value))
        .collect();
    SidecarPublicationPushRejectionWire {
        schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
        classification,
        commit_ids,
        relative_paths,
    }
}

fn extract_labeled_values(output: &str, label: &str) -> Vec<String> {
    let mut values = BTreeSet::new();
    for line in output.lines() {
        let lower = line.to_ascii_lowercase();
        let Some(index) = lower.find(label) else {
            continue;
        };
        let rest = line[index + label.len()..].trim();
        let value = if let Some(quote) = rest
            .chars()
            .next()
            .filter(|c| matches!(c, '\'' | '"' | '`'))
        {
            rest[quote.len_utf8()..]
                .split_once(quote)
                .map_or("", |(value, _)| value)
        } else {
            rest.split_whitespace().next().unwrap_or("")
        };
        let value = value.trim_matches(|character: char| {
            matches!(character, ',' | ';' | ')' | ']' | '}')
        });
        if !value.is_empty() {
            values.insert(value.to_string());
        }
    }
    values.into_iter().collect()
}

fn valid_commit_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64)
        && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_relative_path(value: &str) -> bool {
    !value.is_empty()
        && !value.starts_with('/')
        && !value.contains('\\')
        && !value.contains('\0')
        && !value.contains(':')
        && value
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decide(stderr: &str, attempt: u32) -> SidecarPublicationDecisionWire {
        decide_sidecar_publication_after_push(1, "", stderr, attempt)
    }

    #[test]
    fn success_stops_the_policy_with_success_action() {
        let decision = decide_sidecar_publication_after_push(0, "ok", "", 1);
        assert_eq!(decision.action, SIDECAR_PUBLICATION_ACTION_SUCCESS);
        assert_eq!(decision.classification, SIDECAR_PUBLICATION_CLASS_SUCCESS);
        assert!(!decision.retryable);
    }

    #[test]
    fn explicit_non_fast_forward_retries_before_limit() {
        let decision =
            decide("! [rejected] main -> main (non-fast-forward)", 1);
        assert_eq!(
            decision.action,
            SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY
        );
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD
        );
        assert!(decision.retryable);
    }

    #[test]
    fn explicit_fetch_first_retries_before_limit() {
        let decision = decide("! [rejected] main -> main (fetch first)", 2);
        assert_eq!(
            decision.action,
            SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY
        );
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_FETCH_FIRST
        );
        assert!(decision.retryable);
    }

    #[test]
    fn generic_failed_refs_message_is_terminal() {
        let decision = decide("error: failed to push some refs to 'origin'", 1);
        assert_eq!(decision.action, SIDECAR_PUBLICATION_ACTION_STOP);
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE
        );
        assert!(!decision.retryable);
    }

    #[test]
    fn retryable_rejection_stops_at_attempt_limit() {
        let decision = decide(
            "! [rejected] main -> main (fetch first)",
            SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
        );
        assert_eq!(decision.action, SIDECAR_PUBLICATION_ACTION_STOP);
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_FETCH_FIRST
        );
        assert!(decision.retryable);
    }

    #[test]
    fn timeout_and_hook_failures_are_terminal_unknowns() {
        for stderr in [
            "git operation timed out after 120.0s",
            "remote: hook declined\nerror: failed to push some refs",
            "fatal: Authentication failed",
        ] {
            let decision = decide(stderr, 1);
            assert_eq!(decision.action, SIDECAR_PUBLICATION_ACTION_STOP);
            assert_eq!(
                decision.classification,
                SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE
            );
            assert!(!decision.retryable);
        }
    }

    #[test]
    fn secret_scan_rejection_precedes_generic_retry_hints() {
        let decision = decide(
            "remote: error: GH013: Repository rule violations found\nremote: fetch first",
            1,
        );
        assert_eq!(decision.action, SIDECAR_PUBLICATION_ACTION_STOP);
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_SECRET_SCAN
        );
        assert!(!decision.retryable);
        assert!(decision.reason.contains("candidate content is blocked"));
    }

    #[test]
    fn push_cannot_contain_secrets_is_a_secret_rejection() {
        let result = classify_sidecar_publication_push_result(
            1,
            "",
            "remote: Push cannot contain secrets. path: agents/a/chat.md",
        );
        assert_eq!(
            result.classification,
            SidecarPublicationClassificationWire::SecretScanRejected
        );
        assert_eq!(result.relative_paths, ["agents/a/chat.md"]);
    }

    #[test]
    fn generic_rule_rejection_is_not_a_secret_or_retryable() {
        let decision = decide(
            "remote: Repository rule violation: signed commits required",
            1,
        );
        assert_eq!(
            decision.classification,
            SIDECAR_PUBLICATION_CLASS_REPOSITORY_RULE
        );
        assert!(!decision.retryable);
        assert!(decision.reason.contains("no secret was confirmed"));
    }

    #[test]
    fn rejection_attribution_keeps_only_validated_relative_paths_and_commit_ids(
    ) {
        let secret = "SYNTHETIC_SECRET_SHOULD_NEVER_LEAK";
        let result = classify_sidecar_publication_push_result(
            1,
            "",
            &format!(
                "GH013: {secret} path: ../private.txt\ncommit: {}\npath: agents/a/chat.md\ncommit: deadbeef",
                "a".repeat(40)
            ),
        );
        assert!(result
            .relative_paths
            .contains(&"agents/a/chat.md".to_string()));
        assert_eq!(result.commit_ids, ["a".repeat(40)]);
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains(secret));
        assert!(!serialized.contains("../private.txt"));
    }
}

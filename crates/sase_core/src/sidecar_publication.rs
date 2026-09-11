//! Deterministic policy for launch-time sidecar publication retries.
//!
//! The host owns Git subprocesses, repository locks, integration, recovery
//! refs, and notifications. This module only classifies one observed push
//! result and decides whether another attempt is allowed.

use serde::{Deserialize, Serialize};

pub const SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS: u32 = 3;

pub const SIDECAR_PUBLICATION_ACTION_SUCCESS: &str = "success";
pub const SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY: &str =
    "integrate_and_retry";
pub const SIDECAR_PUBLICATION_ACTION_STOP: &str = "stop";

pub const SIDECAR_PUBLICATION_CLASS_SUCCESS: &str = "success";
pub const SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD: &str =
    "rejected_non_fast_forward";
pub const SIDECAR_PUBLICATION_CLASS_FETCH_FIRST: &str = "rejected_fetch_first";
pub const SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE: &str = "unknown_failure";

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

    let classification = classify_push_failure(stdout, stderr);
    let retryable = matches!(
        classification,
        SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD
            | SIDECAR_PUBLICATION_CLASS_FETCH_FIRST
    );
    if retryable && attempt < SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS {
        return SidecarPublicationDecisionWire {
            schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
            action: SIDECAR_PUBLICATION_ACTION_INTEGRATE_AND_RETRY.to_string(),
            classification: classification.to_string(),
            reason: "git push was rejected by remote divergence; integrate upstream and retry"
                .to_string(),
            attempt,
            max_attempts: SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
            retryable: true,
        };
    }

    let reason = if retryable {
        "git push was rejected by remote divergence but the retry limit is exhausted"
    } else {
        "git push failed without an explicit non-fast-forward or fetch-first rejection"
    };
    SidecarPublicationDecisionWire {
        schema_version: SIDECAR_PUBLICATION_WIRE_SCHEMA_VERSION,
        action: SIDECAR_PUBLICATION_ACTION_STOP.to_string(),
        classification: classification.to_string(),
        reason: reason.to_string(),
        attempt,
        max_attempts: SIDECAR_PUBLICATION_MAX_PUSH_ATTEMPTS,
        retryable,
    }
}

fn classify_push_failure(stdout: &str, stderr: &str) -> &'static str {
    let combined = format!("{stderr}\n{stdout}").to_ascii_lowercase();
    if combined.contains("non-fast-forward") {
        return SIDECAR_PUBLICATION_CLASS_NON_FAST_FORWARD;
    }
    if combined.contains("fetch first")
        || combined
            .contains("updates were rejected because the remote contains work")
    {
        return SIDECAR_PUBLICATION_CLASS_FETCH_FIRST;
    }
    SIDECAR_PUBLICATION_CLASS_UNKNOWN_FAILURE
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
}

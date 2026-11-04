use super::classify::{classify_failure_retryability, TRANSPORT_FAULT_MARKERS};
use super::wire::{
    FailureObservationWire, RETRYABILITY_VERDICT_AFTER_DELAY,
    RETRYABILITY_VERDICT_CREDENTIAL_UNCONFIRMED,
    RETRYABILITY_VERDICT_PERMANENT, RETRYABILITY_VERDICT_TRANSIENT,
    RETRY_OPERATION_GH, RETRY_OPERATION_GIT, RETRY_OPERATION_GIT_CLONE,
};

fn classify(stderr: &str) -> super::wire::RetryabilityVerdictWire {
    classify_failure_retryability(&FailureObservationWire {
        operation_kind: RETRY_OPERATION_GIT_CLONE.to_string(),
        exit_status: Some(128),
        stdout: String::new(),
        stderr: stderr.to_string(),
    })
}

fn classify_with(
    operation_kind: &str,
    stderr: &str,
) -> super::wire::RetryabilityVerdictWire {
    classify_failure_retryability(&FailureObservationWire {
        operation_kind: operation_kind.to_string(),
        exit_status: Some(1),
        stdout: String::new(),
        stderr: stderr.to_string(),
    })
}

fn reason_category(reason: &str) -> &str {
    reason.split(':').next().unwrap_or("").trim()
}

#[test]
fn classifies_existing_clone_markers_as_transient() {
    for (marker, _) in TRANSPORT_FAULT_MARKERS.iter().take(12) {
        let verdict = classify(&format!("fatal: {marker}"));
        assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_TRANSIENT);
        assert!(verdict.retryable);
    }
}

#[test]
fn classifies_http_5xx_as_transient() {
    let verdict = classify("gh: HTTP 503: Service Unavailable");
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_TRANSIENT);
    assert_eq!(verdict.reason, "http_5xx: server-side HTTP failure");
}

#[test]
fn classifies_tls_and_proxy_failures_as_transient() {
    assert!(classify("TLS handshake timeout").retryable);
    assert!(classify("proxyconnect tcp: dial tcp: i/o timeout").retryable);
}

#[test]
fn rate_limit_uses_retry_after_delay() {
    let verdict = classify_failure_retryability(&FailureObservationWire {
        operation_kind: RETRY_OPERATION_GH.to_string(),
        exit_status: Some(1),
        stdout: String::new(),
        stderr: "API rate limit exceeded\nRetry-After: 42".to_string(),
    });
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_AFTER_DELAY);
    assert!(verdict.retryable);
    assert_eq!(verdict.retry_after_seconds, Some(42));
}

#[test]
fn gh_auth_and_not_found_are_permanent() {
    for stderr in [
        "gh: Bad credentials (HTTP 401)",
        "gh: Not Found (HTTP 404)",
        "GraphQL: Could not resolve to a Repository",
    ] {
        let verdict = classify_failure_retryability(&FailureObservationWire {
            operation_kind: RETRY_OPERATION_GH.to_string(),
            exit_status: Some(1),
            stdout: String::new(),
            stderr: stderr.to_string(),
        });
        assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_PERMANENT);
        assert!(!verdict.retryable);
    }
}

#[test]
fn unclassified_failure_is_permanent() {
    let verdict = classify("fatal: unknown revision 'main'");
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_PERMANENT);
    assert!(!verdict.retryable);
}

#[test]
fn git_and_git_clone_publickey_are_credential_unconfirmed() {
    for operation_kind in [RETRY_OPERATION_GIT, RETRY_OPERATION_GIT_CLONE] {
        for stderr in [
            "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.",
            "Permission denied (publickey,keyboard-interactive).",
            "ERROR: Permission denied (publickey) for org/repo",
        ] {
            let verdict = classify_with(operation_kind, stderr);
            assert_eq!(
                verdict.verdict,
                RETRYABILITY_VERDICT_CREDENTIAL_UNCONFIRMED,
                "operation={operation_kind} stderr={stderr}"
            );
            assert!(!verdict.retryable);
            assert_eq!(verdict.retry_after_seconds, None);
            assert_eq!(reason_category(&verdict.reason), "authentication");
        }
    }
}

#[test]
fn gh_publickey_and_https_auth_stay_permanent() {
    let verdict = classify_with(
        RETRY_OPERATION_GH,
        "git@github.com: Permission denied (publickey).",
    );
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_PERMANENT);
    assert!(!verdict.retryable);
    assert_eq!(reason_category(&verdict.reason), "authentication");

    let verdict = classify_with(
        RETRY_OPERATION_GIT_CLONE,
        "Authentication failed for 'https://github.com/org/repo.git'",
    );
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_PERMANENT);
    assert!(!verdict.retryable);
    assert_eq!(reason_category(&verdict.reason), "authentication");
}

#[test]
fn deploy_key_refusal_stays_permanent_with_precedence() {
    for operation_kind in [
        RETRY_OPERATION_GIT,
        RETRY_OPERATION_GIT_CLONE,
        RETRY_OPERATION_GH,
    ] {
        let verdict = classify_with(
            operation_kind,
            "ERROR: This deploy key is denied to deploy key access",
        );
        assert_eq!(
            verdict.verdict, RETRYABILITY_VERDICT_PERMANENT,
            "operation={operation_kind}"
        );
        assert!(!verdict.retryable);
        assert_eq!(reason_category(&verdict.reason), "authentication");
    }

    let verdict = classify_with(
        RETRY_OPERATION_GIT_CLONE,
        "git@github.com: Permission denied (publickey).\nERROR: denied to deploy key for org/repo",
    );
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_PERMANENT);
    assert!(!verdict.retryable);
    assert_eq!(reason_category(&verdict.reason), "authentication");
}

#[test]
fn incident_publickey_stderr_is_credential_unconfirmed() {
    let verdict = classify(
        "git@github.com: Permission denied (publickey).\nfatal: Could not read from remote repository.",
    );
    assert_eq!(verdict.verdict, RETRYABILITY_VERDICT_CREDENTIAL_UNCONFIRMED);
    assert!(!verdict.retryable);
    assert_eq!(verdict.retry_after_seconds, None);
    assert_eq!(reason_category(&verdict.reason), "authentication");
}

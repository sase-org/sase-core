use sase_core::retryability::{
    classify_failure_retryability, retryability_wire_schema_version,
    FailureObservationWire, RETRYABILITY_VERDICT_AFTER_DELAY,
    RETRYABILITY_VERDICT_PERMANENT, RETRYABILITY_VERDICT_TRANSIENT,
    RETRYABILITY_WIRE_SCHEMA_VERSION, RETRY_OPERATION_GH,
    RETRY_OPERATION_GIT_CLONE,
};
use serde_json::json;

fn observation(operation_kind: &str, stderr: &str) -> FailureObservationWire {
    FailureObservationWire {
        operation_kind: operation_kind.to_string(),
        exit_status: Some(1),
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}

#[test]
fn retryability_wire_schema_version_is_one() {
    assert_eq!(
        retryability_wire_schema_version(),
        RETRYABILITY_WIRE_SCHEMA_VERSION
    );
}

#[test]
fn retryability_verdict_serializes_to_python_shape() {
    let verdict = classify_failure_retryability(&observation(
        RETRY_OPERATION_GIT_CLONE,
        "fatal: connection reset by peer",
    ));
    assert_eq!(
        serde_json::to_value(&verdict).unwrap(),
        json!({
            "schema_version": 1,
            "verdict": RETRYABILITY_VERDICT_TRANSIENT,
            "reason": "transport_connection_reset: transport failure",
            "retryable": true,
            "retry_after_seconds": null,
        })
    );
}

#[test]
fn parity_cases_cover_transient_delayed_and_permanent_verdicts() {
    let cases = [
        (
            RETRY_OPERATION_GIT_CLONE,
            "fatal: early EOF",
            RETRYABILITY_VERDICT_TRANSIENT,
            true,
        ),
        (
            RETRY_OPERATION_GIT_CLONE,
            "fatal: HTTP 502 from github.com",
            RETRYABILITY_VERDICT_TRANSIENT,
            true,
        ),
        (
            RETRY_OPERATION_GH,
            "API rate limit exceeded\nRetry-After: 5",
            RETRYABILITY_VERDICT_AFTER_DELAY,
            true,
        ),
        (
            RETRY_OPERATION_GH,
            "gh: Not Found (HTTP 404)",
            RETRYABILITY_VERDICT_PERMANENT,
            false,
        ),
    ];

    for (operation_kind, stderr, expected_verdict, retryable) in cases {
        let verdict =
            classify_failure_retryability(&observation(operation_kind, stderr));
        assert_eq!(verdict.verdict, expected_verdict);
        assert_eq!(verdict.retryable, retryable);
    }
}

//! Pure deterministic retryability classifier for GitHub-adjacent failures.
//!
//! The host owns subprocess execution, sleeping, deadlines, and telemetry.
//! This module only classifies observed `git`/`gh` stdout, stderr, exit
//! status, and operation kind into a stable retryability verdict.

pub mod wire;

pub use wire::{
    FailureObservationWire, RetryabilityVerdictWire,
    RETRYABILITY_VERDICT_AFTER_DELAY, RETRYABILITY_VERDICT_PERMANENT,
    RETRYABILITY_VERDICT_TRANSIENT, RETRYABILITY_WIRE_SCHEMA_VERSION,
    RETRY_OPERATION_GH, RETRY_OPERATION_GIT, RETRY_OPERATION_GIT_CLONE,
};

const TRANSPORT_FAULT_MARKERS: &[(&str, &str)] = &[
    ("broken pipe", "transport_broken_pipe"),
    ("closed by remote host", "transport_closed_by_remote_host"),
    ("connection refused", "transport_connection_refused"),
    ("connection reset", "transport_connection_reset"),
    ("connection timed out", "transport_connection_timed_out"),
    ("could not resolve hostname", "transport_dns_failure"),
    ("early eof", "transport_early_eof"),
    ("invalid index-pack output", "transport_invalid_index_pack"),
    ("network is unreachable", "transport_network_unreachable"),
    ("no route to host", "transport_no_route_to_host"),
    (
        "remote end hung up unexpectedly",
        "transport_remote_hung_up",
    ),
    ("unexpected disconnect", "transport_unexpected_disconnect"),
    ("operation timed out", "transport_operation_timed_out"),
    ("the operation timed out", "transport_operation_timed_out"),
    ("i/o timeout", "transport_io_timeout"),
    ("connection closed", "transport_connection_closed"),
    ("curl 28", "transport_curl_timeout"),
    ("curl 56", "transport_curl_receive_error"),
    (
        "ssh_exchange_identification",
        "transport_ssh_exchange_identification",
    ),
];

const TLS_MARKERS: &[&str] = &[
    "tls handshake",
    "ssl handshake",
    "ssl_connect",
    "ssl_error_syscall",
    "gnutls recv error",
    "schannel: failed to receive handshake",
    "tlsv1 alert",
];

const PROXY_MARKERS: &[&str] = &[
    "proxy error",
    "proxyconnect tcp",
    "could not resolve proxy",
    "tunnel connection failed",
    "received http code 502 from proxy",
    "proxy returned status",
];

const RATE_LIMIT_MARKERS: &[&str] = &[
    "api rate limit exceeded",
    "rate limit exceeded",
    "secondary rate limit",
    "abuse detection mechanism",
    "abuse rate limits",
    "x-ratelimit-remaining: 0",
    "retry-after:",
];

const AUTH_MARKERS: &[&str] = &[
    "authentication failed",
    "could not authenticate",
    "bad credentials",
    "requires authentication",
    "permission denied (publickey)",
    "resource not accessible by integration",
    "gh auth login",
    "gh_token",
    "http 401",
    "http status 401",
    "status code: 401",
    "401 unauthorized",
];

const NOT_FOUND_MARKERS: &[&str] = &[
    "repository not found",
    "not found (http 404)",
    "http 404",
    "http status 404",
    "status code: 404",
    "404 not found",
    "could not resolve to a repository",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Git,
    GitClone,
    Gh,
    Other,
}

impl OperationKind {
    fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            RETRY_OPERATION_GIT => Self::Git,
            RETRY_OPERATION_GIT_CLONE => Self::GitClone,
            RETRY_OPERATION_GH => Self::Gh,
            _ => Self::Other,
        }
    }
}

/// Classify one observed failure as retryable or permanent.
pub fn classify_failure_retryability(
    observation: &FailureObservationWire,
) -> RetryabilityVerdictWire {
    let operation = OperationKind::parse(&observation.operation_kind);
    let combined = normalize(&observation.stderr, &observation.stdout);

    if combined.trim().is_empty() && observation.exit_status == Some(0) {
        return permanent("success", "command succeeded");
    }

    if has_any(&combined, RATE_LIMIT_MARKERS) {
        return RetryabilityVerdictWire {
            schema_version: RETRYABILITY_WIRE_SCHEMA_VERSION,
            verdict: RETRYABILITY_VERDICT_AFTER_DELAY.to_string(),
            reason: "github_rate_limited".to_string(),
            retryable: true,
            retry_after_seconds: retry_after_seconds(&combined),
        };
    }

    if matches!(
        operation,
        OperationKind::Gh | OperationKind::Git | OperationKind::GitClone
    ) && has_any(&combined, AUTH_MARKERS)
    {
        return permanent(
            "authentication",
            "authentication or authorization failed",
        );
    }

    if matches!(
        operation,
        OperationKind::Gh | OperationKind::Git | OperationKind::GitClone
    ) && has_any(&combined, NOT_FOUND_MARKERS)
    {
        return permanent(
            "not_found",
            "requested GitHub resource was not found",
        );
    }

    if let Some(reason) = transport_reason(&combined) {
        return retryable_transient(reason, "transport failure");
    }

    if has_any(&combined, TLS_MARKERS) {
        return retryable_transient(
            "tls_or_ssl_handshake",
            "TLS/SSL handshake failure",
        );
    }

    if has_any(&combined, PROXY_MARKERS) {
        return retryable_transient("proxy_failure", "proxy transport failure");
    }

    if http_status_is_5xx(&combined) {
        return retryable_transient("http_5xx", "server-side HTTP failure");
    }

    if matches!(operation, OperationKind::Gh)
        && (contains_http_status(&combined, 403)
            || combined.contains("403 forbidden"))
    {
        return permanent("authorization", "GitHub rejected the request");
    }

    permanent(
        "unclassified",
        "failure did not match a retryable signature",
    )
}

pub fn retryability_wire_schema_version() -> u32 {
    RETRYABILITY_WIRE_SCHEMA_VERSION
}

fn retryable_transient(reason: &str, message: &str) -> RetryabilityVerdictWire {
    RetryabilityVerdictWire {
        schema_version: RETRYABILITY_WIRE_SCHEMA_VERSION,
        verdict: RETRYABILITY_VERDICT_TRANSIENT.to_string(),
        reason: format!("{reason}: {message}"),
        retryable: true,
        retry_after_seconds: None,
    }
}

fn permanent(reason: &str, message: &str) -> RetryabilityVerdictWire {
    RetryabilityVerdictWire {
        schema_version: RETRYABILITY_WIRE_SCHEMA_VERSION,
        verdict: RETRYABILITY_VERDICT_PERMANENT.to_string(),
        reason: format!("{reason}: {message}"),
        retryable: false,
        retry_after_seconds: None,
    }
}

fn normalize(stderr: &str, stdout: &str) -> String {
    format!("{stderr}\n{stdout}").to_ascii_lowercase()
}

fn has_any(haystack: &str, markers: &[&str]) -> bool {
    markers.iter().any(|marker| haystack.contains(marker))
}

fn transport_reason(haystack: &str) -> Option<&'static str> {
    TRANSPORT_FAULT_MARKERS.iter().find_map(|(marker, reason)| {
        haystack.contains(marker).then_some(*reason)
    })
}

fn retry_after_seconds(haystack: &str) -> Option<u64> {
    let marker = "retry-after:";
    let index = haystack.find(marker)?;
    let rest = &haystack[index + marker.len()..];
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();
    if digits.is_empty() {
        None
    } else {
        digits.parse().ok()
    }
}

fn http_status_is_5xx(haystack: &str) -> bool {
    (500..=599).any(|status| contains_http_status(haystack, status))
}

fn contains_http_status(haystack: &str, status: u16) -> bool {
    let status = status.to_string();
    let patterns = [
        format!("http {status}"),
        format!("http status {status}"),
        format!("http code {status}"),
        format!("status code: {status}"),
        format!("status {status}"),
        format!("({status})"),
        format!(" {status} "),
    ];
    patterns
        .iter()
        .any(|pattern| haystack.contains(pattern.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(stderr: &str) -> RetryabilityVerdictWire {
        classify_failure_retryability(&FailureObservationWire {
            operation_kind: RETRY_OPERATION_GIT_CLONE.to_string(),
            exit_status: Some(128),
            stdout: String::new(),
            stderr: stderr.to_string(),
        })
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
            let verdict =
                classify_failure_retryability(&FailureObservationWire {
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
}

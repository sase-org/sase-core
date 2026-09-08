use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION: u32 = 1;

pub const MANAGED_ORIGIN_ACTION_NONE: &str = "none";
pub const MANAGED_ORIGIN_ACTION_REWRITE: &str = "rewrite";
pub const MANAGED_ORIGIN_ACTION_FAIL: &str = "fail";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedOriginReconciliationRequestWire {
    #[serde(default)]
    pub managed: bool,
    #[serde(default)]
    pub identity_verified: bool,
    #[serde(default)]
    pub checkout_dir: String,
    #[serde(default)]
    pub primary_checkout_dir: String,
    #[serde(default)]
    pub canonical_remote_url: Option<String>,
    #[serde(default)]
    pub canonical_remote_error: Option<String>,
    #[serde(default)]
    pub canonical_remote_points_at_primary: bool,
    #[serde(default)]
    pub origin_url: Option<String>,
    #[serde(default)]
    pub origin_read_error: Option<String>,
    #[serde(default)]
    pub origin_points_at_primary: bool,
    #[serde(default)]
    pub origin_matches_canonical: bool,
    #[serde(default)]
    pub effective_push_urls: Vec<String>,
    #[serde(default)]
    pub effective_push_urls_pointing_at_primary: Vec<String>,
    #[serde(default)]
    pub explicit_push_urls: Vec<String>,
    #[serde(default)]
    pub explicit_push_urls_pointing_at_primary: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedOriginPushUrlRewriteWire {
    pub old_url: String,
    pub new_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedOriginReconciliationDecisionWire {
    pub schema_version: u32,
    pub action: String,
    pub reason: String,
    pub rewrite_origin_url: Option<String>,
    pub rewrite_push_urls: Vec<ManagedOriginPushUrlRewriteWire>,
    pub diagnostics: Vec<String>,
}

pub fn decide_managed_origin_reconciliation(
    request: &ManagedOriginReconciliationRequestWire,
) -> ManagedOriginReconciliationDecisionWire {
    if !request.managed {
        return no_change(
            "checkout is not managed; origin left unchanged",
            vec![],
        );
    }

    if !request.identity_verified {
        return fail(
            "managed checkout identity could not be verified; refusing to inspect or rewrite origin before provider selection",
            vec![format_checkout_diagnostic(request)],
        );
    }

    let origin = clean_option(&request.origin_url);
    if origin.is_none() {
        return fail(
            format!(
                "managed checkout origin for {} could not be read{}",
                format_checkout(&request.checkout_dir),
                format_error_suffix(&request.origin_read_error)
            ),
            vec![format_checkout_diagnostic(request)],
        );
    }

    let canonical = clean_option(&request.canonical_remote_url);
    let Some(canonical) = canonical else {
        return fail(
            format!(
                "managed checkout {} cannot reconcile origin before provider selection because the primary checkout's origin is unavailable{}",
                format_checkout(&request.checkout_dir),
                format_error_suffix(&request.canonical_remote_error)
            ),
            vec![format_checkout_diagnostic(request)],
        );
    };

    if request.canonical_remote_points_at_primary {
        return fail(
            format!(
                "primary checkout origin for {} resolves to the primary checkout path; fix the primary checkout's origin before using managed workspaces",
                format_checkout(&request.primary_checkout_dir)
            ),
            vec![format_checkout_diagnostic(request)],
        );
    }

    let stale_origin = request.origin_points_at_primary;
    let stale_explicit_push_urls =
        unique_nonempty(&request.explicit_push_urls_pointing_at_primary);
    let stale_effective_push_urls =
        unique_nonempty(&request.effective_push_urls_pointing_at_primary);

    if !stale_effective_push_urls.is_empty()
        && !stale_origin
        && stale_explicit_push_urls.is_empty()
    {
        return fail(
            "managed checkout has a push destination that resolves to the primary checkout, but the stale destination is not in origin or explicit pushurl configuration",
            vec![
                format_checkout_diagnostic(request),
                format!(
                    "stale effective push URLs: {}",
                    stale_effective_push_urls.join(", ")
                ),
            ],
        );
    }

    let rewrite_origin_url =
        if stale_origin && !request.origin_matches_canonical {
            Some(canonical.to_string())
        } else {
            None
        };
    let rewrite_push_urls = stale_explicit_push_urls
        .iter()
        .map(|old_url| ManagedOriginPushUrlRewriteWire {
            old_url: old_url.clone(),
            new_url: canonical.to_string(),
        })
        .collect::<Vec<_>>();

    if rewrite_origin_url.is_some() || !rewrite_push_urls.is_empty() {
        let mut diagnostics = vec![format_checkout_diagnostic(request)];
        if stale_origin {
            diagnostics.push(format!(
                "origin resolves to primary checkout: {}",
                origin.unwrap_or("<unreadable>")
            ));
        }
        if !stale_explicit_push_urls.is_empty() {
            diagnostics.push(format!(
                "explicit push URLs resolving to primary checkout: {}",
                stale_explicit_push_urls.join(", ")
            ));
        }
        return ManagedOriginReconciliationDecisionWire {
            schema_version: MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION,
            action: MANAGED_ORIGIN_ACTION_REWRITE.to_string(),
            reason: "managed checkout origin or push URL resolves to the primary checkout; rewrite to authoritative remote before provider selection".to_string(),
            rewrite_origin_url,
            rewrite_push_urls,
            diagnostics,
        };
    }

    no_change(
        "managed checkout origin does not resolve to the primary checkout; origin left unchanged",
        vec![format_checkout_diagnostic(request)],
    )
}

fn clean_option(value: &Option<String>) -> Option<&str> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn unique_nonempty(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if !trimmed.is_empty() && seen.insert(trimmed.to_string()) {
            result.push(trimmed.to_string());
        }
    }
    result
}

fn no_change(
    reason: impl Into<String>,
    diagnostics: Vec<String>,
) -> ManagedOriginReconciliationDecisionWire {
    ManagedOriginReconciliationDecisionWire {
        schema_version: MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION,
        action: MANAGED_ORIGIN_ACTION_NONE.to_string(),
        reason: reason.into(),
        rewrite_origin_url: None,
        rewrite_push_urls: vec![],
        diagnostics,
    }
}

fn fail(
    reason: impl Into<String>,
    diagnostics: Vec<String>,
) -> ManagedOriginReconciliationDecisionWire {
    ManagedOriginReconciliationDecisionWire {
        schema_version: MANAGED_ORIGIN_RECONCILIATION_WIRE_SCHEMA_VERSION,
        action: MANAGED_ORIGIN_ACTION_FAIL.to_string(),
        reason: reason.into(),
        rewrite_origin_url: None,
        rewrite_push_urls: vec![],
        diagnostics,
    }
}

fn format_error_suffix(error: &Option<String>) -> String {
    clean_option(error)
        .map(|value| format!(": {value}"))
        .unwrap_or_default()
}

fn format_checkout(value: &str) -> String {
    if value.trim().is_empty() {
        "<unknown>".to_string()
    } else {
        value.to_string()
    }
}

fn format_checkout_diagnostic(
    request: &ManagedOriginReconciliationRequestWire,
) -> String {
    format!(
        "checkout={}, primary={}",
        format_checkout(&request.checkout_dir),
        format_checkout(&request.primary_checkout_dir)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> ManagedOriginReconciliationRequestWire {
        ManagedOriginReconciliationRequestWire {
            managed: true,
            identity_verified: true,
            checkout_dir: "/work/repo_2".to_string(),
            primary_checkout_dir: "/work/repo".to_string(),
            canonical_remote_url: Some(
                "git@github.com:org/repo.git".to_string(),
            ),
            origin_url: Some("git@github.com:org/repo.git".to_string()),
            origin_matches_canonical: true,
            effective_push_urls: vec!["git@github.com:org/repo.git".to_string()],
            ..Default::default()
        }
    }

    #[test]
    fn stale_origin_is_rewritten_to_canonical_remote() {
        let mut req = request();
        req.origin_url = Some("/work/repo".to_string());
        req.origin_points_at_primary = true;
        req.origin_matches_canonical = false;
        req.effective_push_urls = vec!["/work/repo".to_string()];
        req.effective_push_urls_pointing_at_primary =
            vec!["/work/repo".to_string()];

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_REWRITE);
        assert_eq!(
            decision.rewrite_origin_url.as_deref(),
            Some("git@github.com:org/repo.git")
        );
        assert!(decision.rewrite_push_urls.is_empty());
    }

    #[test]
    fn unrelated_local_bare_remote_is_preserved() {
        let mut req = request();
        req.origin_url = Some("/tmp/repo.git".to_string());
        req.origin_matches_canonical = false;
        req.effective_push_urls = vec!["/tmp/repo.git".to_string()];

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_NONE);
        assert!(decision.rewrite_origin_url.is_none());
        assert!(decision.rewrite_push_urls.is_empty());
    }

    #[test]
    fn stale_explicit_push_url_is_rewritten() {
        let mut req = request();
        req.effective_push_urls = vec![
            "/work/repo".to_string(),
            "git@github.com:org/repo.git".to_string(),
        ];
        req.effective_push_urls_pointing_at_primary =
            vec!["/work/repo".to_string()];
        req.explicit_push_urls = vec![
            "/work/repo".to_string(),
            "git@github.com:org/repo.git".to_string(),
        ];
        req.explicit_push_urls_pointing_at_primary =
            vec!["/work/repo".to_string()];

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_REWRITE);
        assert!(decision.rewrite_origin_url.is_none());
        assert_eq!(
            decision.rewrite_push_urls,
            vec![ManagedOriginPushUrlRewriteWire {
                old_url: "/work/repo".to_string(),
                new_url: "git@github.com:org/repo.git".to_string(),
            }]
        );
    }

    #[test]
    fn missing_identity_fails() {
        let mut req = request();
        req.identity_verified = false;

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_FAIL);
        assert!(decision.reason.contains("identity"));
    }

    #[test]
    fn canonical_remote_unavailable_fails() {
        let mut req = request();
        req.canonical_remote_url = None;
        req.canonical_remote_error = Some("no origin".to_string());

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_FAIL);
        assert!(decision.reason.contains("primary checkout's origin"));
    }

    #[test]
    fn primary_origin_resolving_to_primary_fails() {
        let mut req = request();
        req.canonical_remote_points_at_primary = true;

        let decision = decide_managed_origin_reconciliation(&req);

        assert_eq!(decision.action, MANAGED_ORIGIN_ACTION_FAIL);
        assert!(decision
            .reason
            .contains("resolves to the primary checkout path"));
    }
}

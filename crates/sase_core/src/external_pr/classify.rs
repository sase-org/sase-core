//! Deterministic external-PR reconciliation classifier.

use std::sync::OnceLock;

use regex::Regex;

use crate::commit_footer::parse_commit_footer;
use crate::status::ARCHIVE_STATUSES;

use super::url::canonical_pull_request_url;
use super::wire::{
    ExternalPrImportPlanWire, ExternalPrImportRequestWire, LocalPatchWire,
    EXTERNAL_PR_WIRE_SCHEMA_VERSION,
};

const ACTION_ADOPT: &str = "adopt";
const ACTION_REFRESH: &str = "refresh";
const ACTION_REPAIR_ORIGIN: &str = "repair_origin";
const ACTION_SKIP: &str = "skip";

const REASON_ALREADY_OWNED: &str = "already_owned";
const REASON_AMBIGUOUS_MARKER: &str = "ambiguous_marker";
const REASON_MARKER_ORPHAN: &str = "marker_orphan";
const REASON_MARKER_REPAIR: &str = "marker_repair";
const REASON_UNMARKED: &str = "unmarked";

const DESTINATION_ACTIVE: &str = "active";
const DESTINATION_ARCHIVE: &str = "archive";

const PR_ORIGIN_EXTERNAL: &str = "external";

/// Pure classifier for one remote pull request.
pub fn plan_external_pr_import(
    request: &ExternalPrImportRequestWire,
) -> Result<ExternalPrImportPlanWire, String> {
    if request.schema_version != EXTERNAL_PR_WIRE_SCHEMA_VERSION {
        return Err(format!(
            "external PR wire schema mismatch: got {}, expected {}",
            request.schema_version, EXTERNAL_PR_WIRE_SCHEMA_VERSION
        ));
    }

    let canonical = canonical_pull_request_url(&request.remote.url);
    let canonical_key = canonical.as_ref().map(|parsed| parsed.key.clone());
    let canonical_pr_url = canonical
        .as_ref()
        .map(|_| request.remote.url.trim().to_string());
    let footer = parse_commit_footer(&request.remote.body);
    let marker = footer
        .tags
        .iter()
        .rev()
        .find(|tag| tag.key == "PATCH")
        .map(|tag| tag.label.trim().to_string());
    let stripped_description =
        description_without_footer(&request.remote.title, &footer.body);
    let (status, destination) = mapped_status_and_destination(
        &request.remote.state,
        request.remote.is_draft,
        &request.remote.merged_at,
    );

    if let Some(key) = canonical_key.as_deref() {
        if let Some(local) = request
            .local_patches
            .iter()
            .find(|patch| patch_pr_key(patch) == Some(key.to_string()))
        {
            let marker_names_local =
                marker.as_deref().is_some_and(|value| value == local.name);
            if !(marker_names_local
                && (local.reserved || local.pr_url.trim().is_empty()))
            {
                let destination_is_archive = destination == DESTINATION_ARCHIVE;
                let needs_refresh = local.pr_origin == PR_ORIGIN_EXTERNAL
                    && (local.status != status
                        || local.archived != destination_is_archive);
                let action = if needs_refresh {
                    ACTION_REFRESH
                } else {
                    ACTION_SKIP
                };
                return Ok(ExternalPrImportPlanWire {
                    action: action.into(),
                    reason: REASON_ALREADY_OWNED.into(),
                    patch_name: Some(local.name.clone()),
                    name_base: None,
                    pr_origin: local.pr_origin.clone(),
                    status,
                    destination,
                    description: stripped_description,
                    canonical_pr_url: canonical_pr_url.clone(),
                });
            }
        }
    }

    if let Some(raw_marker) = marker.as_deref() {
        if is_valid_patch_name(raw_marker) {
            if request
                .local_patches
                .iter()
                .any(|patch| patch.name == raw_marker)
            {
                return Ok(ExternalPrImportPlanWire {
                    action: ACTION_REPAIR_ORIGIN.into(),
                    reason: REASON_MARKER_REPAIR.into(),
                    patch_name: Some(raw_marker.to_string()),
                    name_base: None,
                    pr_origin: "sase".into(),
                    status,
                    destination,
                    description: stripped_description,
                    canonical_pr_url: canonical_pr_url.clone(),
                });
            }
            return Ok(ExternalPrImportPlanWire {
                action: ACTION_ADOPT.into(),
                reason: REASON_MARKER_ORPHAN.into(),
                patch_name: Some(raw_marker.to_string()),
                name_base: None,
                pr_origin: "sase".into(),
                status,
                destination,
                description: stripped_description,
                canonical_pr_url: canonical_pr_url.clone(),
            });
        }
        return Ok(ExternalPrImportPlanWire {
            action: ACTION_ADOPT.into(),
            reason: REASON_AMBIGUOUS_MARKER.into(),
            patch_name: None,
            name_base: Some(name_base_for_request(request)),
            pr_origin: "unknown".into(),
            status,
            destination,
            description: stripped_description,
            canonical_pr_url: canonical_pr_url.clone(),
        });
    }

    Ok(ExternalPrImportPlanWire {
        action: ACTION_ADOPT.into(),
        reason: REASON_UNMARKED.into(),
        patch_name: None,
        name_base: Some(name_base_for_request(request)),
        pr_origin: "external".into(),
        status,
        destination,
        description: stripped_description,
        canonical_pr_url,
    })
}

fn patch_pr_key(patch: &LocalPatchWire) -> Option<String> {
    canonical_pull_request_url(&patch.pr_url).map(|parsed| parsed.key)
}

fn mapped_status_and_destination(
    state: &str,
    is_draft: bool,
    merged_at: &str,
) -> (String, String) {
    let status = if !merged_at.trim().is_empty() {
        "Submitted"
    } else if state == "open" && is_draft {
        "Draft"
    } else if state == "open" {
        "Mailed"
    } else {
        "Archived"
    };
    let destination = if ARCHIVE_STATUSES.contains(&status) {
        DESTINATION_ARCHIVE
    } else {
        DESTINATION_ACTIVE
    };
    (status.into(), destination.into())
}

fn description_without_footer(
    title: &str,
    body_without_footer: &str,
) -> String {
    let title = title.trim();
    let body = body_without_footer.trim();
    match (title.is_empty(), body.is_empty()) {
        (true, true) => String::new(),
        (false, true) => title.to_string(),
        (true, false) => body.to_string(),
        (false, false) => format!("{title}\n\n{body}"),
    }
}

fn patch_name_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9_.~-]*$").unwrap())
}

fn is_valid_patch_name(value: &str) -> bool {
    let text = value.trim();
    !text.is_empty() && patch_name_re().is_match(text)
}

fn name_base_for_request(request: &ExternalPrImportRequestWire) -> String {
    for value in [
        request.remote.title.as_str(),
        request.remote.head_ref.as_str(),
        "",
    ] {
        let slug = slug_name_base(value);
        if !slug.is_empty() {
            return slug;
        }
    }
    format!("pr-{}", request.remote.number)
}

fn slug_name_base(value: &str) -> String {
    let mut result = String::new();
    let mut previous_underscore = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() {
            result.push(ch);
            previous_underscore = false;
        } else if !previous_underscore {
            result.push('_');
            previous_underscore = true;
        }
        if result.len() >= 48 {
            break;
        }
    }
    result.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_pr::wire::RemotePullRequestWire;

    fn remote(body: &str) -> RemotePullRequestWire {
        RemotePullRequestWire {
            number: 17,
            url: "https://github.com/sase-org/sase/pull/17".into(),
            provider_id: "PR_kw17".into(),
            title: "Feature title".into(),
            body: body.into(),
            state: "open".into(),
            is_draft: false,
            author: "alice".into(),
            head_ref: "feature/branch".into(),
            base_ref: "main".into(),
            created_at: "2026-08-01T00:00:00Z".into(),
            updated_at: "2026-08-02T00:00:00Z".into(),
            closed_at: String::new(),
            merged_at: String::new(),
        }
    }

    fn req(
        body: &str,
        locals: Vec<LocalPatchWire>,
    ) -> ExternalPrImportRequestWire {
        ExternalPrImportRequestWire {
            schema_version: EXTERNAL_PR_WIRE_SCHEMA_VERSION,
            remote: remote(body),
            local_patches: locals,
        }
    }

    fn local(
        name: &str,
        pr_url: &str,
        origin: &str,
        status: &str,
    ) -> LocalPatchWire {
        local_with_archived(name, pr_url, origin, status, false)
    }

    fn local_with_archived(
        name: &str,
        pr_url: &str,
        origin: &str,
        status: &str,
        archived: bool,
    ) -> LocalPatchWire {
        LocalPatchWire {
            name: name.into(),
            pr_url: pr_url.into(),
            pr_origin: origin.into(),
            status: status.into(),
            archived,
            reserved: status == "Reserved",
        }
    }

    const OWNED_PR_URL: &str = "https://github.com/sase-org/sase/pull/17";

    #[test]
    fn owned_url_skips_and_preserves_origin() {
        let plan = plan_external_pr_import(&req(
            "",
            vec![local(
                "sase_feature_1",
                "HTTP://www.github.com/SASE-ORG/SASE.git/pulls/17/",
                "unknown",
                "Mailed",
            )],
        ))
        .unwrap();
        assert_eq!(plan.action, ACTION_SKIP);
        assert_eq!(plan.reason, REASON_ALREADY_OWNED);
        assert_eq!(plan.patch_name.as_deref(), Some("sase_feature_1"));
        assert_eq!(plan.pr_origin, "unknown");
    }

    #[test]
    fn marker_repairs_reserved_stub() {
        let plan = plan_external_pr_import(&req(
            "Body\n\nSASE_PATCH=sase_feature_1",
            vec![local("sase_feature_1", "", "unknown", "Reserved")],
        ))
        .unwrap();
        assert_eq!(plan.action, ACTION_REPAIR_ORIGIN);
        assert_eq!(plan.reason, REASON_MARKER_REPAIR);
        assert_eq!(plan.patch_name.as_deref(), Some("sase_feature_1"));
        assert_eq!(plan.pr_origin, "sase");
        assert_eq!(plan.description, "Feature title\n\nBody");
    }

    #[test]
    fn marker_orphan_adopts_marker_name() {
        let plan = plan_external_pr_import(&req(
            "Body\n\nPATCH=sase_feature_2",
            vec![],
        ))
        .unwrap();
        assert_eq!(plan.action, ACTION_ADOPT);
        assert_eq!(plan.reason, REASON_MARKER_ORPHAN);
        assert_eq!(plan.patch_name.as_deref(), Some("sase_feature_2"));
        assert_eq!(plan.pr_origin, "sase");
    }

    #[test]
    fn unmarked_pr_adopts_external_with_slug() {
        let plan = plan_external_pr_import(&req("Body", vec![])).unwrap();
        assert_eq!(plan.action, ACTION_ADOPT);
        assert_eq!(plan.reason, REASON_UNMARKED);
        assert_eq!(plan.name_base.as_deref(), Some("feature_title"));
        assert_eq!(plan.pr_origin, "external");
    }

    #[test]
    fn blank_marker_is_ambiguous() {
        let plan = plan_external_pr_import(&req("Body\n\nSASE_PATCH=", vec![]))
            .unwrap();
        assert_eq!(plan.action, ACTION_ADOPT);
        assert_eq!(plan.reason, REASON_AMBIGUOUS_MARKER);
        assert_eq!(plan.pr_origin, "unknown");
    }

    #[test]
    fn maps_statuses_and_archive_destination() {
        let mut request = req("", vec![]);
        request.remote.is_draft = true;
        let draft = plan_external_pr_import(&request).unwrap();
        assert_eq!(
            (draft.status.as_str(), draft.destination.as_str()),
            ("Draft", "active")
        );

        request.remote.is_draft = false;
        let mailed = plan_external_pr_import(&request).unwrap();
        assert_eq!(
            (mailed.status.as_str(), mailed.destination.as_str()),
            ("Mailed", "active")
        );

        request.remote.state = "closed".into();
        request.remote.closed_at = "2026-08-03T00:00:00Z".into();
        let archived = plan_external_pr_import(&request).unwrap();
        assert_eq!(
            (archived.status.as_str(), archived.destination.as_str()),
            ("Archived", "archive")
        );

        request.remote.merged_at = "2026-08-03T00:00:00Z".into();
        let submitted = plan_external_pr_import(&request).unwrap();
        assert_eq!(
            (submitted.status.as_str(), submitted.destination.as_str()),
            ("Submitted", "archive")
        );
    }

    #[test]
    fn refresh_fires_when_open_external_patch_status_drifts() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "external",
                "Mailed",
                false,
            )],
        );
        request.remote.is_draft = true;
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_REFRESH);
        assert_eq!(plan.reason, REASON_ALREADY_OWNED);
        assert_eq!(plan.patch_name.as_deref(), Some("pr_feature_1"));
        assert_eq!(plan.pr_origin, "external");
        assert_eq!(
            (plan.status.as_str(), plan.destination.as_str()),
            ("Draft", "active")
        );
    }

    #[test]
    fn refresh_fires_for_merged_pr_and_moves_to_archive() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "external",
                "Mailed",
                false,
            )],
        );
        request.remote.merged_at = "2026-08-03T00:00:00Z".into();
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_REFRESH);
        assert_eq!(
            (plan.status.as_str(), plan.destination.as_str()),
            ("Submitted", "archive")
        );
    }

    #[test]
    fn refresh_fires_for_closed_unmerged_pr() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "external",
                "Mailed",
                false,
            )],
        );
        request.remote.state = "closed".into();
        request.remote.closed_at = "2026-08-03T00:00:00Z".into();
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_REFRESH);
        assert_eq!(
            (plan.status.as_str(), plan.destination.as_str()),
            ("Archived", "archive")
        );
    }

    #[test]
    fn unchanged_external_patch_skips() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "external",
                "Submitted",
                true,
            )],
        );
        request.remote.merged_at = "2026-08-03T00:00:00Z".into();
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_SKIP);
        assert_eq!(plan.reason, REASON_ALREADY_OWNED);
    }

    #[test]
    fn sase_owned_patch_never_refreshes_despite_status_drift() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "sase",
                "Mailed",
                false,
            )],
        );
        request.remote.merged_at = "2026-08-03T00:00:00Z".into();
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_SKIP);
        assert_eq!(plan.pr_origin, "sase");
    }

    #[test]
    fn unknown_origin_patch_never_refreshes_despite_status_drift() {
        let mut request = req(
            "",
            vec![local_with_archived(
                "pr_feature_1",
                OWNED_PR_URL,
                "unknown",
                "Mailed",
                false,
            )],
        );
        request.remote.merged_at = "2026-08-03T00:00:00Z".into();
        let plan = plan_external_pr_import(&request).unwrap();
        assert_eq!(plan.action, ACTION_SKIP);
        assert_eq!(plan.pr_origin, "unknown");
    }
}

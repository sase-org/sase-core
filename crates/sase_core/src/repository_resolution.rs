//! Canonical repository identity matching for `sase repo open`.
//!
//! The host owns inventory collection and checkout preparation. This module
//! owns the provider identity rules that decide whether a provider-style
//! reference names one of those configured repositories.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const REPOSITORY_RESOLUTION_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryResolutionRequestWire {
    pub requested: String,
    #[serde(default)]
    pub candidates: Vec<RepositoryResolutionCandidateWire>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepositoryResolutionCandidateWire {
    pub id: String,
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub aliases: Vec<String>,
    #[serde(default)]
    pub remote_urls: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryResolutionWire {
    pub schema_version: u32,
    pub status: RepositoryResolutionStatus,
    pub matched_id: Option<String>,
    pub match_reason: Option<String>,
    pub candidate_ids: Vec<String>,
    pub requested_identity: Option<RepositoryRemoteIdentityWire>,
    pub diagnostics: Vec<RepositoryResolutionDiagnosticWire>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryResolutionStatus {
    Matched,
    NoMatch,
    Ambiguous,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryResolutionDiagnosticWire {
    pub candidate_id: String,
    pub remote_url: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RepositoryRemoteIdentityWire {
    pub provider: String,
    pub host: String,
    pub owner: String,
    pub repo: String,
    pub canonical: String,
}

pub fn repository_resolution_wire_schema_version() -> u32 {
    REPOSITORY_RESOLUTION_WIRE_SCHEMA_VERSION
}

pub fn canonical_repository_identity(
    value: &str,
) -> Option<RepositoryRemoteIdentityWire> {
    github_identity(value)
}

pub fn resolve_repository_reference(
    request: &RepositoryResolutionRequestWire,
) -> RepositoryResolutionWire {
    let requested = request.requested.trim();
    let requested_identity = github_identity(requested);

    if requested.is_empty() {
        return no_match(requested_identity, vec![]);
    }

    if let Some(resolution) = resolve_exact_path(
        requested,
        &request.candidates,
        requested_identity.clone(),
    ) {
        return resolution;
    }
    if let Some(resolution) = resolve_exact_names(
        requested,
        &request.candidates,
        &["sidecar", "linked"],
        "exact_name",
        requested_identity.clone(),
    ) {
        return resolution;
    }
    if let Some(resolution) = resolve_exact_names(
        requested,
        &request.candidates,
        &["primary"],
        "exact_primary",
        requested_identity.clone(),
    ) {
        return resolution;
    }

    let Some(identity) = requested_identity.clone() else {
        return no_match(None, vec![]);
    };

    let mut matched_ids = BTreeSet::new();
    let mut diagnostics = Vec::new();
    for candidate in &request.candidates {
        for remote_url in &candidate.remote_urls {
            let trimmed = remote_url.trim();
            if trimmed.is_empty() {
                continue;
            }
            match github_identity(trimmed) {
                Some(candidate_identity)
                    if candidate_identity.canonical == identity.canonical =>
                {
                    matched_ids.insert(candidate.id.clone());
                }
                Some(_) => {}
                None => diagnostics.push(RepositoryResolutionDiagnosticWire {
                    candidate_id: candidate.id.clone(),
                    remote_url: trimmed.to_string(),
                    message:
                        "remote does not contain a supported GitHub identity"
                            .to_string(),
                }),
            }
        }
    }

    let candidate_ids: Vec<String> = matched_ids.into_iter().collect();
    match candidate_ids.len() {
        0 => no_match(Some(identity), diagnostics),
        1 => matched(
            candidate_ids[0].clone(),
            "remote_identity",
            Some(identity),
            diagnostics,
        ),
        _ => ambiguous(candidate_ids, Some(identity), diagnostics),
    }
}

fn resolve_exact_path(
    requested: &str,
    candidates: &[RepositoryResolutionCandidateWire],
    requested_identity: Option<RepositoryRemoteIdentityWire>,
) -> Option<RepositoryResolutionWire> {
    let ids: Vec<String> = candidates
        .iter()
        .filter(|candidate| candidate.path.as_deref() == Some(requested))
        .map(|candidate| candidate.id.clone())
        .collect();
    match ids.len() {
        0 => None,
        1 => Some(matched(
            ids[0].clone(),
            "exact_path",
            requested_identity,
            vec![],
        )),
        _ => Some(ambiguous(ids, requested_identity, vec![])),
    }
}

fn resolve_exact_names(
    requested: &str,
    candidates: &[RepositoryResolutionCandidateWire],
    kinds: &[&str],
    reason: &'static str,
    requested_identity: Option<RepositoryRemoteIdentityWire>,
) -> Option<RepositoryResolutionWire> {
    let ids: Vec<String> = candidates
        .iter()
        .filter(|candidate| {
            kinds.iter().any(|kind| candidate.kind == *kind)
                && candidate.exact_names().contains(&requested)
        })
        .map(|candidate| candidate.id.clone())
        .collect();
    match ids.len() {
        0 => None,
        1 => Some(matched(ids[0].clone(), reason, requested_identity, vec![])),
        _ => Some(ambiguous(ids, requested_identity, vec![])),
    }
}

impl RepositoryResolutionCandidateWire {
    fn exact_names(&self) -> Vec<&str> {
        let mut names = Vec::with_capacity(2 + self.aliases.len());
        names.push(self.name.as_str());
        for alias in &self.aliases {
            names.push(alias.as_str());
        }
        names
    }
}

fn matched(
    id: String,
    reason: &'static str,
    requested_identity: Option<RepositoryRemoteIdentityWire>,
    diagnostics: Vec<RepositoryResolutionDiagnosticWire>,
) -> RepositoryResolutionWire {
    RepositoryResolutionWire {
        schema_version: REPOSITORY_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: RepositoryResolutionStatus::Matched,
        matched_id: Some(id.clone()),
        match_reason: Some(reason.to_string()),
        candidate_ids: vec![id],
        requested_identity,
        diagnostics,
    }
}

fn no_match(
    requested_identity: Option<RepositoryRemoteIdentityWire>,
    diagnostics: Vec<RepositoryResolutionDiagnosticWire>,
) -> RepositoryResolutionWire {
    RepositoryResolutionWire {
        schema_version: REPOSITORY_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: RepositoryResolutionStatus::NoMatch,
        matched_id: None,
        match_reason: None,
        candidate_ids: vec![],
        requested_identity,
        diagnostics,
    }
}

fn ambiguous(
    candidate_ids: Vec<String>,
    requested_identity: Option<RepositoryRemoteIdentityWire>,
    diagnostics: Vec<RepositoryResolutionDiagnosticWire>,
) -> RepositoryResolutionWire {
    RepositoryResolutionWire {
        schema_version: REPOSITORY_RESOLUTION_WIRE_SCHEMA_VERSION,
        status: RepositoryResolutionStatus::Ambiguous,
        matched_id: None,
        match_reason: None,
        candidate_ids,
        requested_identity,
        diagnostics,
    }
}

fn github_identity(value: &str) -> Option<RepositoryRemoteIdentityWire> {
    let candidate = value.trim().trim_end_matches('/');
    if candidate.is_empty() {
        return None;
    }

    let (host, path) = if let Some(provider_ref) =
        strip_case_insensitive_prefix(candidate, "gh:")
    {
        ("github.com", provider_ref)
    } else if let Some(rest) = strip_http_like_prefix(candidate) {
        split_url_host_path(rest)?
    } else if let Some(rest) =
        strip_case_insensitive_prefix(candidate, "ssh://")
    {
        split_url_host_path(rest)?
    } else if let Some(rest) =
        strip_case_insensitive_prefix(candidate, "git://")
    {
        split_url_host_path(rest)?
    } else if let Some((host_part, path_part)) = split_scp_like(candidate) {
        (host_part, path_part)
    } else if let Some((owner, repo)) = split_owner_repo(candidate) {
        return build_github_identity("github.com", owner, repo);
    } else {
        return None;
    };

    let host = clean_host(host)?;
    if host != "github.com" {
        return None;
    }
    let (owner, repo) = split_owner_repo(path)?;
    build_github_identity(&host, owner, repo)
}

fn strip_http_like_prefix(value: &str) -> Option<&str> {
    strip_case_insensitive_prefix(value, "https://")
        .or_else(|| strip_case_insensitive_prefix(value, "http://"))
}

fn strip_case_insensitive_prefix<'a>(
    value: &'a str,
    prefix: &str,
) -> Option<&'a str> {
    if value.len() < prefix.len() {
        return None;
    }
    let (head, tail) = value.split_at(prefix.len());
    if head.eq_ignore_ascii_case(prefix) {
        Some(tail)
    } else {
        None
    }
}

fn split_url_host_path(value: &str) -> Option<(&str, &str)> {
    let (authority, path) = value.split_once('/')?;
    let authority = authority.rsplit('@').next().unwrap_or(authority);
    Some((authority, path))
}

fn split_scp_like(value: &str) -> Option<(&str, &str)> {
    let colon = value.find(':')?;
    let slash = value.find('/');
    if slash.is_some_and(|index| index < colon) {
        return None;
    }
    let (host_part, path_part) = value.split_at(colon);
    let path_part = path_part.strip_prefix(':')?;
    if path_part.is_empty() {
        return None;
    }
    let host = host_part.rsplit('@').next().unwrap_or(host_part);
    Some((host, path_part))
}

fn clean_host(value: &str) -> Option<String> {
    let host = value
        .trim()
        .trim_end_matches('/')
        .split_once(':')
        .map(|(host, _port)| host)
        .unwrap_or(value)
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_ascii_lowercase();
    if host.is_empty() {
        None
    } else {
        Some(host)
    }
}

fn split_owner_repo(value: &str) -> Option<(&str, &str)> {
    let trimmed = value.trim();
    let path = trimmed.trim_matches('/');
    if trimmed.starts_with('/')
        || path.starts_with('.')
        || path.contains(':')
        || path.contains('\\')
    {
        return None;
    }
    let parts: Vec<&str> =
        path.split('/').filter(|part| !part.is_empty()).collect();
    if parts.len() != 2 {
        return None;
    }
    Some((parts[0], strip_git_suffix(parts[1])))
}

fn strip_git_suffix(value: &str) -> &str {
    if let Some(prefix) = value.get(..value.len().saturating_sub(4)) {
        if value
            .get(value.len().saturating_sub(4)..)
            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(".git"))
        {
            return prefix;
        }
    }
    if value.eq_ignore_ascii_case(".git") {
        ""
    } else {
        value
    }
}

fn build_github_identity(
    host: &str,
    owner: &str,
    repo: &str,
) -> Option<RepositoryRemoteIdentityWire> {
    if !valid_github_component(owner) || !valid_github_component(repo) {
        return None;
    }
    let host = host.to_ascii_lowercase();
    let owner = owner.to_ascii_lowercase();
    let repo = repo.to_ascii_lowercase();
    let canonical = format!("gh:{owner}/{repo}");
    Some(RepositoryRemoteIdentityWire {
        provider: "gh".to_string(),
        host,
        owner,
        repo,
        canonical,
    })
}

fn valid_github_component(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.chars().any(|ch| {
            ch.is_ascii_whitespace()
                || matches!(ch, '/' | '\\' | ':' | '\0' | '?' | '#')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(
        id: &str,
        name: &str,
        kind: &str,
        remote_urls: Vec<&str>,
    ) -> RepositoryResolutionCandidateWire {
        RepositoryResolutionCandidateWire {
            id: id.to_string(),
            name: name.to_string(),
            kind: kind.to_string(),
            path: Some(format!("/work/{name}")),
            aliases: vec![],
            remote_urls: remote_urls.into_iter().map(str::to_string).collect(),
        }
    }

    fn request(
        requested: &str,
        candidates: Vec<RepositoryResolutionCandidateWire>,
    ) -> RepositoryResolutionRequestWire {
        RepositoryResolutionRequestWire {
            requested: requested.to_string(),
            candidates,
        }
    }

    #[test]
    fn normalizes_github_reference_spellings() {
        let expected = canonical_repository_identity("gh:SASE-Org/SASE-Core")
            .unwrap()
            .canonical;
        for value in [
            "sase-org/sase-core",
            "https://github.com/sase-org/sase-core.git",
            "git@github.com:sase-org/sase-core.git",
            "ssh://git@github.com/sase-org/sase-core.git/",
        ] {
            assert_eq!(
                canonical_repository_identity(value).unwrap().canonical,
                expected
            );
        }
    }

    #[test]
    fn rejects_malformed_and_other_provider_identities() {
        for value in [
            "gl:sase-org/sase-core",
            "https://gitlab.com/sase-org/sase-core",
            "https://github.com/sase-org",
            "gh:sase-org/sase-core/extra",
            "gh:sase-org/",
            "/tmp/sase-core",
        ] {
            assert_eq!(canonical_repository_identity(value), None, "{value}");
        }
    }

    #[test]
    fn exact_linked_name_precedes_remote_match() {
        let linked = candidate(
            "linked-core",
            "sase-core",
            "linked",
            vec!["git@github.com:sase-org/sase-core.git"],
        );
        let other = candidate(
            "other",
            "other",
            "linked",
            vec!["git@github.com:sase-org/sase-core.git"],
        );

        let decision = resolve_repository_reference(&request(
            "sase-core",
            vec![linked, other],
        ));

        assert_eq!(decision.status, RepositoryResolutionStatus::Matched);
        assert_eq!(decision.matched_id.as_deref(), Some("linked-core"));
        assert_eq!(decision.match_reason.as_deref(), Some("exact_name"));
    }

    #[test]
    fn remote_identity_resolves_provider_alias() {
        let decision = resolve_repository_reference(&request(
            "gh:sase-org/sase-core",
            vec![candidate(
                "linked-core",
                "sase-core",
                "linked",
                vec!["git@github.com:SASE-Org/SASE-Core.git"],
            )],
        ));

        assert_eq!(decision.status, RepositoryResolutionStatus::Matched);
        assert_eq!(decision.matched_id.as_deref(), Some("linked-core"));
        assert_eq!(decision.match_reason.as_deref(), Some("remote_identity"));
    }

    #[test]
    fn different_owner_or_host_does_not_match() {
        let decision = resolve_repository_reference(&request(
            "gh:sase-org/sase-core",
            vec![
                candidate(
                    "other-owner",
                    "sase-core",
                    "linked",
                    vec!["git@github.com:other/sase-core.git"],
                ),
                candidate(
                    "other-host",
                    "sase-core-two",
                    "linked",
                    vec!["git@gitlab.com:sase-org/sase-core.git"],
                ),
            ],
        ));

        assert_eq!(decision.status, RepositoryResolutionStatus::NoMatch);
        assert!(decision.matched_id.is_none());
    }

    #[test]
    fn duplicate_configured_remote_is_ambiguous() {
        let decision = resolve_repository_reference(&request(
            "sase-org/sase-core",
            vec![
                candidate(
                    "first",
                    "core-a",
                    "linked",
                    vec!["https://github.com/sase-org/sase-core"],
                ),
                candidate(
                    "second",
                    "core-b",
                    "linked",
                    vec!["git@github.com:sase-org/sase-core.git"],
                ),
            ],
        ));

        assert_eq!(decision.status, RepositoryResolutionStatus::Ambiguous);
        assert_eq!(decision.candidate_ids, vec!["first", "second"]);
    }
}

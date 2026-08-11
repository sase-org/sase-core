//! Canonical pull-request URL identity.

use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CanonicalPullRequestUrl {
    pub host: String,
    pub owner: String,
    pub repo: String,
    pub number: u64,
    pub key: String,
}

/// Canonicalize a pull-request URL into a stable comparison key.
///
/// Returns `None` for any unparseable input. No partial matches are exposed to
/// callers.
pub fn canonical_pull_request_url(
    raw: &str,
) -> Option<CanonicalPullRequestUrl> {
    let mut value = raw.trim();
    if value.is_empty() {
        return None;
    }
    value = value.split_once('#').map_or(value, |(head, _)| head);
    value = value.split_once('?').map_or(value, |(head, _)| head);
    value = value.trim_end_matches('/');

    let without_scheme = if let Some((scheme, rest)) = value.split_once("://") {
        if scheme.is_empty()
            || !scheme.chars().all(|c| {
                c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')
            })
        {
            return None;
        }
        rest
    } else {
        value
    };
    if without_scheme.is_empty() || without_scheme.contains("://") {
        return None;
    }

    let (authority, path) = without_scheme
        .split_once('/')
        .map_or((without_scheme, ""), |pair| pair);
    if authority.is_empty() || path.is_empty() {
        return None;
    }
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    let raw_host = host_port
        .split_once(':')
        .map_or(host_port, |(host, _)| host)
        .trim()
        .to_ascii_lowercase();
    let host = raw_host
        .strip_prefix("www.")
        .unwrap_or(&raw_host)
        .to_string();
    if host.is_empty() {
        return None;
    }

    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let (owner, repo, number) = match segments.as_slice() {
        [owner, repo, kind, number] if *kind == "pull" || *kind == "pulls" => {
            (*owner, *repo, parse_number(number)?)
        }
        [owner, repo, dash, kind, number]
            if *dash == "-" && *kind == "merge_requests" =>
        {
            (*owner, *repo, parse_number(number)?)
        }
        _ => return None,
    };

    let owner = owner.to_ascii_lowercase();
    let repo = repo.trim_end_matches(".git").to_ascii_lowercase();
    if owner.is_empty() || repo.is_empty() {
        return None;
    }
    let key = format!("{host}/{owner}/{repo}#{number}");
    Some(CanonicalPullRequestUrl {
        host,
        owner,
        repo,
        number,
        key,
    })
}

fn parse_number(value: &str) -> Option<u64> {
    if value.is_empty() || !value.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalizes_github_url_variants() {
        let cases = [
            (
                "HTTPS://WWW.GitHub.com/SASE-ORG/SASE.git/pull/17/",
                "github.com/sase-org/sase#17",
            ),
            (
                "github.com/SASE-ORG/SASE/pulls/17?foo=bar#frag",
                "github.com/sase-org/sase#17",
            ),
            (
                "https://user:token@github.com/SASE-ORG/SASE/pull/17",
                "github.com/sase-org/sase#17",
            ),
        ];
        for (raw, expected) in cases {
            assert_eq!(canonical_pull_request_url(raw).unwrap().key, expected);
        }
    }

    #[test]
    fn canonicalizes_gitlab_merge_request_urls() {
        let parsed = canonical_pull_request_url(
            "https://gitlab.example.com/Owner/Repo/-/merge_requests/4/",
        )
        .unwrap();
        assert_eq!(parsed.key, "gitlab.example.com/owner/repo#4");
    }

    #[test]
    fn rejects_unparseable_urls() {
        for raw in [
            "",
            "https://github.com/org/repo",
            "not a url",
            "x/y/pull/nope",
        ] {
            assert!(canonical_pull_request_url(raw).is_none(), "{raw}");
        }
    }
}

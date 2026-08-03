//! Bead ID prefix, alias, and exact-token identity helpers.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::wire::{BeadError, IssueWire};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadIdTokenRewriteOutcomeWire {
    pub text: String,
    pub replacement_counts: BTreeMap<String, u64>,
    pub total_replacements: u64,
}

#[derive(Debug, Clone)]
pub struct BeadIdentityResolver {
    canonical_ids: BTreeSet<String>,
    aliases: BTreeMap<String, String>,
}

impl BeadIdentityResolver {
    pub fn new(
        issues: &[IssueWire],
        aliases: &BTreeMap<String, String>,
    ) -> Result<Self, BeadError> {
        let canonical_ids = issues
            .iter()
            .map(|issue| issue.id.clone())
            .collect::<BTreeSet<_>>();
        validate_id_aliases(aliases, &canonical_ids)?;
        Ok(Self {
            canonical_ids,
            aliases: aliases.clone(),
        })
    }

    pub fn resolve(&self, issue_id: &str) -> Result<String, BeadError> {
        if self.canonical_ids.contains(issue_id) {
            return Ok(issue_id.to_string());
        }
        if let Some(canonical) = self.aliases.get(issue_id) {
            return Ok(canonical.clone());
        }
        if issue_id.is_empty() || issue_id.contains('-') {
            return Err(not_found(issue_id));
        }
        let mut candidates = self
            .canonical_ids
            .iter()
            .filter_map(|canonical_id| {
                canonical_id.rsplit_once('-').and_then(|(_, suffix)| {
                    (suffix == issue_id).then(|| canonical_id.clone())
                })
            })
            .collect::<Vec<_>>();
        candidates.sort();
        candidates.dedup();
        match candidates.as_slice() {
            [resolved] => Ok(resolved.clone()),
            [] => Err(not_found(issue_id)),
            _ => Err(BeadError {
                kind: "ambiguous".to_string(),
                message: format!(
                    "ambiguous bead ID shorthand {issue_id:?}: {}",
                    candidates.join(", ")
                ),
            }),
        }
    }

    pub fn resolve_if_known(&self, issue_id: &str) -> Option<String> {
        self.resolve(issue_id).ok()
    }
}

pub fn validate_issue_prefix(prefix: &str) -> Result<(), BeadError> {
    if prefix.is_empty() {
        return Err(BeadError::validation("issue prefix cannot be empty"));
    }
    if prefix.chars().any(char::is_whitespace) {
        return Err(BeadError::validation(
            "issue prefix cannot contain whitespace",
        ));
    }
    if prefix.contains('.')
        || prefix.contains('/')
        || prefix.contains('\\')
        || prefix.contains("--")
    {
        return Err(BeadError::validation(
            "issue prefix cannot contain dot, slash, backslash, or double hyphen",
        ));
    }
    if prefix.ends_with('-') {
        return Err(BeadError::validation(
            "issue prefix cannot end with a hyphen",
        ));
    }
    Ok(())
}

pub fn validate_issue_id_for_prefix(
    issue_id: &str,
    prefix: &str,
) -> Result<(), BeadError> {
    validate_issue_prefix(prefix)?;
    let expected = format!("{prefix}-");
    let suffix = issue_id.strip_prefix(&expected).ok_or_else(|| {
        BeadError::validation(format!(
            "bead ID {issue_id:?} does not use prefix {prefix:?}"
        ))
    })?;
    validate_issue_suffix(suffix).map_err(|message| {
        BeadError::validation(format!(
            "invalid bead ID {issue_id:?}: {message}"
        ))
    })
}

pub fn rewrite_issue_id_prefix(
    issue_id: &str,
    from_prefix: &str,
    to_prefix: &str,
) -> Result<Option<String>, BeadError> {
    validate_issue_prefix(from_prefix)?;
    validate_issue_prefix(to_prefix)?;
    let expected = format!("{from_prefix}-");
    let Some(suffix) = issue_id.strip_prefix(&expected) else {
        return Ok(None);
    };
    validate_issue_suffix(suffix).map_err(|message| {
        BeadError::validation(format!(
            "invalid bead ID {issue_id:?}: {message}"
        ))
    })?;
    Ok(Some(format!("{to_prefix}-{suffix}")))
}

pub fn validate_id_aliases(
    aliases: &BTreeMap<String, String>,
    canonical_ids: &BTreeSet<String>,
) -> Result<(), BeadError> {
    let mut targets = BTreeSet::new();
    for (alias, target) in aliases {
        if alias.trim().is_empty() {
            return Err(BeadError::validation("bead ID alias cannot be empty"));
        }
        if target.trim().is_empty() {
            return Err(BeadError::validation(format!(
                "bead ID alias {alias} has an empty target"
            )));
        }
        if alias == target {
            return Err(BeadError::validation(format!(
                "bead ID alias {alias} cannot target itself"
            )));
        }
        if canonical_ids.contains(alias) {
            return Err(BeadError::validation(format!(
                "bead ID alias {alias} shadows a canonical issue ID"
            )));
        }
        if aliases.contains_key(target) {
            return Err(BeadError::validation(format!(
                "bead ID alias {alias} targets another alias {target}"
            )));
        }
        if !canonical_ids.contains(target) {
            return Err(BeadError::validation(format!(
                "bead ID alias {alias} targets unknown issue ID {target}"
            )));
        }
        if !targets.insert(target.clone()) {
            return Err(BeadError::validation(format!(
                "multiple bead ID aliases target {target}"
            )));
        }
        validate_full_issue_id(alias).map_err(|message| {
            BeadError::validation(format!(
                "invalid bead ID alias {alias:?}: {message}"
            ))
        })?;
    }
    Ok(())
}

pub fn rewrite_id_tokens(
    text: &str,
    replacements: &BTreeMap<String, String>,
) -> BeadIdTokenRewriteOutcomeWire {
    if text.is_empty() || replacements.is_empty() {
        return BeadIdTokenRewriteOutcomeWire {
            text: text.to_string(),
            replacement_counts: BTreeMap::new(),
            total_replacements: 0,
        };
    }

    let mut keys = replacements.keys().cloned().collect::<Vec<_>>();
    keys.sort_by(|left, right| {
        right.len().cmp(&left.len()).then_with(|| left.cmp(right))
    });

    let mut rewritten = String::with_capacity(text.len());
    let mut counts = BTreeMap::new();
    let mut total = 0;
    let mut index = 0;
    while index < text.len() {
        let mut matched: Option<&str> = None;
        for key in &keys {
            if text[index..].starts_with(key)
                && is_token_boundary(text, index, index + key.len())
            {
                matched = Some(key.as_str());
                break;
            }
        }
        if let Some(key) = matched {
            let replacement = replacements
                .get(key)
                .expect("matched key came from replacement map");
            rewritten.push_str(replacement);
            *counts.entry(key.to_string()).or_insert(0) += 1;
            total += 1;
            index += key.len();
        } else {
            let ch =
                text[index..].chars().next().expect("index is within text");
            rewritten.push(ch);
            index += ch.len_utf8();
        }
    }

    BeadIdTokenRewriteOutcomeWire {
        text: rewritten,
        replacement_counts: counts,
        total_replacements: total,
    }
}

fn validate_full_issue_id(issue_id: &str) -> Result<(), &'static str> {
    let Some((prefix, suffix)) = issue_id.rsplit_once('-') else {
        return Err("missing prefix separator");
    };
    validate_issue_prefix(prefix).map_err(|_| "unsafe prefix")?;
    validate_issue_suffix(suffix)
}

fn validate_issue_suffix(suffix: &str) -> Result<(), &'static str> {
    let mut parts = suffix.split('.');
    let Some(counter) = parts.next() else {
        return Err("missing counter");
    };
    if counter.is_empty()
        || !counter
            .chars()
            .all(|ch| ch.is_ascii_digit() || ch.is_ascii_lowercase())
    {
        return Err("counter suffix must be lowercase base36");
    }
    for child in parts {
        if child.is_empty() || !child.chars().all(|ch| ch.is_ascii_digit()) {
            return Err("child suffixes must be decimal");
        }
    }
    Ok(())
}

fn is_token_boundary(text: &str, start: usize, end: usize) -> bool {
    !has_identifier_neighbor(text[..start].chars().next_back())
        && !has_identifier_neighbor(text[end..].chars().next())
}

fn has_identifier_neighbor(ch: Option<char>) -> bool {
    ch.is_some_and(|ch| {
        ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.')
    })
}

fn not_found(issue_id: &str) -> BeadError {
    BeadError {
        kind: "not_found".to_string(),
        message: format!("Issue not found: {issue_id}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(id: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: String::new(),
            status: super::super::wire::StatusWire::Open,
            issue_type: super::super::wire::IssueTypeWire::Task,
            tier: None,
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            updated_at: String::new(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs: Vec::new(),
            plus_one_evidence: Vec::new(),
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: Vec::new(),
        }
    }

    #[test]
    fn validates_safe_prefixes() {
        assert!(validate_issue_prefix("sase").is_ok());
        assert!(validate_issue_prefix("bob-cli").is_ok());
        assert!(validate_issue_prefix("").is_err());
        assert!(validate_issue_prefix("bad prefix").is_err());
        assert!(validate_issue_prefix("bad.prefix").is_err());
        assert!(validate_issue_prefix("bad/thing").is_err());
        assert!(validate_issue_prefix("bad--thing").is_err());
        assert!(validate_issue_prefix("bad-").is_err());
    }

    #[test]
    fn resolves_aliases_before_shorthand() {
        let mut aliases = BTreeMap::new();
        aliases.insert("old-1".to_string(), "sase-a1".to_string());
        let resolver = BeadIdentityResolver::new(
            &[issue("sase-a1"), issue("other-1")],
            &aliases,
        )
        .unwrap();
        assert_eq!(resolver.resolve("old-1").unwrap(), "sase-a1");
        assert_eq!(resolver.resolve("a1").unwrap(), "sase-a1");
        assert_eq!(resolver.resolve("sase-a1").unwrap(), "sase-a1");
    }

    #[test]
    fn rejects_alias_chains_and_shadowing() {
        let canonical = BTreeSet::from(["sase-1".to_string()]);
        let aliases =
            BTreeMap::from([("old-1".to_string(), "older-1".to_string())]);
        assert!(validate_id_aliases(&aliases, &canonical).is_err());
        let aliases =
            BTreeMap::from([("sase-1".to_string(), "sase-1.1".to_string())]);
        assert!(validate_id_aliases(&aliases, &canonical).is_err());
    }

    #[test]
    fn rewrites_complete_tokens_with_longest_match() {
        let replacements = BTreeMap::from([
            ("old-1".to_string(), "new-1".to_string()),
            ("old-1.12".to_string(), "new-1.12".to_string()),
        ]);
        let outcome = rewrite_id_tokens(
            "old-1 old-1.12 xold-1 old-1x /old-1 https://h/old-1",
            &replacements,
        );
        assert_eq!(
            outcome.text,
            "new-1 new-1.12 xold-1 old-1x /new-1 https://h/new-1"
        );
        assert_eq!(outcome.total_replacements, 4);
        assert_eq!(outcome.replacement_counts["old-1"], 3);
        assert_eq!(outcome.replacement_counts["old-1.12"], 1);
    }
}

//! Footer-first commit attribution, with directory lookup extension points.

use std::collections::BTreeMap;

use super::directory::{Directory, DirectoryMember};
use super::history::HistoryRow;
use super::wire::ProjectStatsModelKindWire;

pub(crate) const REASON_MALFORMED_MODEL_TAG: &str = "malformed_model_tag";
pub(crate) const REASON_PAGE_UNPUBLISHED: &str = "page_unpublished";
pub(crate) const REASON_PAGE_METADATA_EMPTY: &str = "page_metadata_empty";
pub(crate) const REASON_SESSION_AMBIGUOUS: &str = "session_ambiguous";

/// One commit's resolved or unresolved model bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attribution {
    pub kind: ProjectStatsModelKindWire,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub model_is_alias: bool,
    pub unknown_reason: Option<String>,
}

/// Attribute one cached history row.
///
/// An absent `AGENT` tag is always `no_agent`, even when `MODEL` is present.
/// A present agent tag uses the first `MODEL` value split at `/` when that
/// tag is well formed. Directory lookups are the extension points for the
/// sibling attribution phase; this phase's loader returns an empty map, so
/// unstamped agent commits become `unknown` / `page_unpublished`.
pub(crate) fn attribute_commit(
    row: &HistoryRow,
    directory: &Directory,
    aliases: &BTreeMap<String, String>,
) -> Attribution {
    let Some(agent) = row
        .agent_tag
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
    else {
        return Attribution {
            kind: ProjectStatsModelKindWire::NoAgent,
            provider: None,
            model: None,
            model_is_alias: false,
            unknown_reason: None,
        };
    };

    if let Some(model_tag) = row
        .model_tag
        .as_deref()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
    {
        return match parse_model_tag(model_tag) {
            Some((provider, model)) => resolved(provider, model, aliases),
            None => unknown(REASON_MALFORMED_MODEL_TAG),
        };
    }

    match directory_model(directory, row, agent) {
        Ok((provider, model)) => resolved(provider, model, aliases),
        Err(reason) => unknown(reason),
    }
}

fn resolved(
    provider: String,
    model: String,
    aliases: &BTreeMap<String, String>,
) -> Attribution {
    let model_is_alias = model_is_alias(&provider, &model, aliases);
    Attribution {
        kind: ProjectStatsModelKindWire::Resolved,
        provider: Some(provider),
        model: Some(model),
        model_is_alias,
        unknown_reason: None,
    }
}

fn unknown(reason: &str) -> Attribution {
    Attribution {
        kind: ProjectStatsModelKindWire::Unknown,
        provider: None,
        model: None,
        model_is_alias: false,
        unknown_reason: Some(reason.to_owned()),
    }
}

/// Split an authoritative `MODEL` tag at the first `/`.
pub(crate) fn parse_model_tag(tag: &str) -> Option<(String, String)> {
    let (provider, model) = tag.split_once('/')?;
    let provider = provider.trim();
    let model = model.trim();
    if provider.is_empty() || model.is_empty() {
        return None;
    }
    Some((provider.to_owned(), model.to_owned()))
}

fn model_is_alias(
    provider: &str,
    model: &str,
    aliases: &BTreeMap<String, String>,
) -> bool {
    aliases.contains_key(model)
        || aliases.contains_key(&format!("{provider}/{model}"))
}

/// Directory join used after footer attribution. Exact SHA, then
/// `<agent>--code`, then the exact agent name, then an unambiguous family
/// model. Reserved unknown reasons stay stable for the sibling phase.
fn directory_model(
    directory: &Directory,
    row: &HistoryRow,
    agent: &str,
) -> Result<(String, String), &'static str> {
    if let Some(name) = directory.by_sha.get(&row.sha) {
        return member_model(directory, name);
    }
    let code_page = format!("{agent}--code");
    if directory.by_name.contains_key(&code_page) {
        return member_model(directory, &code_page);
    }
    if directory.by_name.contains_key(agent) {
        return member_model(directory, agent);
    }
    family_shared_model(directory, agent)
}

fn member_model(
    directory: &Directory,
    name: &str,
) -> Result<(String, String), &'static str> {
    let Some(member) = directory.by_name.get(name) else {
        return Err(REASON_PAGE_UNPUBLISHED);
    };
    recorded_member_model(member)
}

fn recorded_member_model(
    member: &DirectoryMember,
) -> Result<(String, String), &'static str> {
    if !member.model_metadata_present {
        return Err(REASON_PAGE_METADATA_EMPTY);
    }
    let model = member
        .model
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(REASON_PAGE_METADATA_EMPTY)?;
    let provider = member
        .provider
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    match provider {
        Some(provider) => Ok((provider.to_owned(), model.to_owned())),
        None => parse_model_tag(model).ok_or(REASON_PAGE_METADATA_EMPTY),
    }
}

fn family_shared_model(
    directory: &Directory,
    agent: &str,
) -> Result<(String, String), &'static str> {
    let family = directory
        .by_name
        .get(agent)
        .and_then(|member| member.family.as_deref())
        .unwrap_or(agent);
    let Some(members) = directory.family_members.get(family) else {
        return Err(REASON_PAGE_UNPUBLISHED);
    };
    if members.is_empty() {
        return Err(REASON_PAGE_UNPUBLISHED);
    }
    let mut shared = None;
    for name in members {
        let model = member_model(directory, name)?;
        match &shared {
            None => shared = Some(model),
            Some(existing) if *existing != model => {
                return Err(REASON_SESSION_AMBIGUOUS);
            }
            Some(_) => {}
        }
    }
    shared.ok_or(REASON_PAGE_UNPUBLISHED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project_stats::directory::DirectoryMember;

    fn row(agent: Option<&str>, model: Option<&str>) -> HistoryRow {
        HistoryRow {
            sha: "a".repeat(40),
            author_ts: 10,
            committer_ts: 11,
            is_merge: false,
            agent_tag: agent.map(ToOwned::to_owned),
            model_tag: model.map(ToOwned::to_owned),
        }
    }

    #[test]
    fn absent_agent_is_no_agent_even_with_a_model_tag() {
        let attributed = attribute_commit(
            &row(None, Some("anthropic/claude")),
            &Directory::default(),
            &BTreeMap::new(),
        );
        assert_eq!(attributed.kind, ProjectStatsModelKindWire::NoAgent);
        assert_eq!(attributed.provider, None);
        assert_eq!(attributed.model, None);
    }

    #[test]
    fn authoritative_model_tag_splits_at_the_first_slash() {
        let attributed = attribute_commit(
            &row(Some("writer"), Some("anthropic/claude/sonnet")),
            &Directory::default(),
            &BTreeMap::new(),
        );
        assert_eq!(attributed.kind, ProjectStatsModelKindWire::Resolved);
        assert_eq!(attributed.provider.as_deref(), Some("anthropic"));
        assert_eq!(attributed.model.as_deref(), Some("claude/sonnet"));
        assert!(!attributed.model_is_alias);
    }

    #[test]
    fn malformed_model_tags_do_not_fall_back_to_the_directory() {
        let mut directory = Directory::default();
        directory.by_sha.insert("a".repeat(40), "writer".to_owned());
        directory.by_name.insert(
            "writer".to_owned(),
            DirectoryMember {
                name: "writer".to_owned(),
                family: None,
                provider: Some("anthropic".to_owned()),
                model: Some("claude".to_owned()),
                model_metadata_present: true,
            },
        );
        for tag in ["noshift", "anthropic/", "/claude", " / "] {
            let attributed = attribute_commit(
                &row(Some("writer"), Some(tag)),
                &directory,
                &BTreeMap::new(),
            );
            assert_eq!(attributed.kind, ProjectStatsModelKindWire::Unknown);
            assert_eq!(
                attributed.unknown_reason.as_deref(),
                Some(REASON_MALFORMED_MODEL_TAG)
            );
        }
    }

    #[test]
    fn empty_directory_yields_page_unpublished_for_unstamped_agent_commits() {
        let attributed = attribute_commit(
            &row(Some("writer"), None),
            &Directory::default(),
            &BTreeMap::new(),
        );
        assert_eq!(attributed.kind, ProjectStatsModelKindWire::Unknown);
        assert_eq!(
            attributed.unknown_reason.as_deref(),
            Some(REASON_PAGE_UNPUBLISHED)
        );
    }

    #[test]
    fn directory_lookups_follow_sha_then_code_page_then_name_then_family() {
        let mut directory = Directory::default();
        let sha_member = DirectoryMember {
            name: "from-sha".to_owned(),
            family: None,
            provider: Some("openai".to_owned()),
            model: Some("gpt".to_owned()),
            model_metadata_present: true,
        };
        directory
            .by_sha
            .insert("a".repeat(40), "from-sha".to_owned());
        directory.by_name.insert("from-sha".to_owned(), sha_member);
        let attributed = attribute_commit(
            &row(Some("writer"), None),
            &directory,
            &BTreeMap::new(),
        );
        assert_eq!(attributed.provider.as_deref(), Some("openai"));
        assert_eq!(attributed.model.as_deref(), Some("gpt"));

        let mut directory = Directory::default();
        directory.by_name.insert(
            "writer--code".to_owned(),
            DirectoryMember {
                name: "writer--code".to_owned(),
                family: None,
                provider: Some("anthropic".to_owned()),
                model: Some("opus".to_owned()),
                model_metadata_present: true,
            },
        );
        let attributed = attribute_commit(
            &row(Some("writer"), None),
            &directory,
            &BTreeMap::new(),
        );
        assert_eq!(attributed.model.as_deref(), Some("opus"));

        let mut directory = Directory::default();
        directory.family_members.insert(
            "clan".to_owned(),
            vec!["one".to_owned(), "two".to_owned()],
        );
        for name in ["one", "two"] {
            directory.by_name.insert(
                name.to_owned(),
                DirectoryMember {
                    name: name.to_owned(),
                    family: Some("clan".to_owned()),
                    provider: Some("xai".to_owned()),
                    model: Some("grok".to_owned()),
                    model_metadata_present: true,
                },
            );
        }
        let attributed = attribute_commit(
            &row(Some("clan"), None),
            &directory,
            &BTreeMap::new(),
        );
        assert_eq!(attributed.model.as_deref(), Some("grok"));

        directory.by_name.get_mut("two").unwrap().model =
            Some("other".to_owned());
        let attributed = attribute_commit(
            &row(Some("clan"), None),
            &directory,
            &BTreeMap::new(),
        );
        assert_eq!(
            attributed.unknown_reason.as_deref(),
            Some(REASON_SESSION_AMBIGUOUS)
        );
    }

    #[test]
    fn known_aliases_are_flagged_without_rewriting_the_recorded_model() {
        let aliases = BTreeMap::from([(
            "sonnet".to_owned(),
            "anthropic/claude-sonnet".to_owned(),
        )]);
        let attributed = attribute_commit(
            &row(Some("writer"), Some("anthropic/sonnet")),
            &Directory::default(),
            &aliases,
        );
        assert_eq!(attributed.model.as_deref(), Some("sonnet"));
        assert!(attributed.model_is_alias);
    }
}

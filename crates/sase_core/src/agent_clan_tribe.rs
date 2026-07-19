//! Deterministic clan-level attribute resolution.

use serde::{Deserialize, Serialize};

/// One launched clan member considered by the clan attribute resolvers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanTribeMemberWire {
    pub agent_clan: String,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
    #[serde(default)]
    pub clan_summary: Option<String>,
    pub launch_timestamp: String,
    pub identity: String,
}

/// Resolve one clan generation from a mixed collection of members.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanTribeResolutionRequestWire {
    pub clan_name: String,
    #[serde(default)]
    pub generation: Option<String>,
    #[serde(default)]
    pub members: Vec<ClanTribeMemberWire>,
}

/// The selected declaration and its stable source identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanTribeResolutionWire {
    pub clan_name: String,
    #[serde(default)]
    pub generation: Option<String>,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub source_launch_timestamp: Option<String>,
    #[serde(default)]
    pub source_identity: Option<String>,
}

/// The selected clan summary declaration and its stable source identity.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanSummaryResolutionWire {
    pub clan_name: String,
    #[serde(default)]
    pub generation: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub source_launch_timestamp: Option<String>,
    #[serde(default)]
    pub source_identity: Option<String>,
}

struct SelectedDeclaration {
    value: String,
    launch_timestamp: String,
    identity: String,
}

fn select_latest_explicit_declaration<'a>(
    request: &'a ClanTribeResolutionRequestWire,
    declaration: impl Fn(&'a ClanTribeMemberWire) -> Option<&'a str>,
) -> Option<SelectedDeclaration> {
    request
        .members
        .iter()
        .filter(|member| {
            member.agent_clan == request.clan_name
                && member.agent_clan_generation == request.generation
        })
        .filter_map(|member| {
            declaration(member)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| SelectedDeclaration {
                    value: value.to_string(),
                    launch_timestamp: member.launch_timestamp.clone(),
                    identity: member.identity.clone(),
                })
        })
        .max_by(|left, right| {
            left.launch_timestamp
                .cmp(&right.launch_timestamp)
                .then_with(|| left.identity.cmp(&right.identity))
        })
}

/// Select the latest explicit declaration for exactly one clan generation.
///
/// Members without `clan_tribe` never clear a prior declaration. Timestamp is
/// the primary launch-order key and the stable artifact identity breaks ties,
/// making the result independent of concurrent runner startup order.
pub fn resolve_clan_tribe(
    request: ClanTribeResolutionRequestWire,
) -> ClanTribeResolutionWire {
    let selected = select_latest_explicit_declaration(&request, |member| {
        member.clan_tribe.as_deref()
    });
    let (tribe, source_launch_timestamp, source_identity) =
        selected.map_or((None, None, None), |selected| {
            (
                Some(selected.value),
                Some(selected.launch_timestamp),
                Some(selected.identity),
            )
        });

    ClanTribeResolutionWire {
        clan_name: request.clan_name,
        generation: request.generation,
        tribe,
        source_launch_timestamp,
        source_identity,
    }
}

/// Select the latest explicit summary declaration for one clan generation.
///
/// Members without `clan_summary` never clear a prior declaration. Selection
/// uses the same deterministic timestamp and identity ordering as tribe
/// resolution.
pub fn resolve_clan_summary(
    request: ClanTribeResolutionRequestWire,
) -> ClanSummaryResolutionWire {
    let selected = select_latest_explicit_declaration(&request, |member| {
        member.clan_summary.as_deref()
    });
    let (summary, source_launch_timestamp, source_identity) =
        selected.map_or((None, None, None), |selected| {
            (
                Some(selected.value),
                Some(selected.launch_timestamp),
                Some(selected.identity),
            )
        });

    ClanSummaryResolutionWire {
        clan_name: request.clan_name,
        generation: request.generation,
        summary,
        source_launch_timestamp,
        source_identity,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(
        clan: &str,
        generation: Option<&str>,
        tribe: Option<&str>,
        summary: Option<&str>,
        timestamp: &str,
        identity: &str,
    ) -> ClanTribeMemberWire {
        ClanTribeMemberWire {
            agent_clan: clan.to_string(),
            agent_clan_generation: generation.map(str::to_string),
            clan_tribe: tribe.map(str::to_string),
            clan_summary: summary.map(str::to_string),
            launch_timestamp: timestamp.to_string(),
            identity: identity.to_string(),
        }
    }

    fn resolve_tribe(
        generation: Option<&str>,
        members: Vec<ClanTribeMemberWire>,
    ) -> ClanTribeResolutionWire {
        resolve_clan_tribe(ClanTribeResolutionRequestWire {
            clan_name: "research".to_string(),
            generation: generation.map(str::to_string),
            members,
        })
    }

    fn resolve_summary(
        generation: Option<&str>,
        members: Vec<ClanTribeMemberWire>,
    ) -> ClanSummaryResolutionWire {
        resolve_clan_summary(ClanTribeResolutionRequestWire {
            clan_name: "research".to_string(),
            generation: generation.map(str::to_string),
            members,
        })
    }

    #[test]
    fn absent_declarations_resolve_to_none() {
        let result = resolve_tribe(
            Some("g1"),
            vec![member("research", Some("g1"), None, None, "01", "a")],
        );
        assert_eq!(result.tribe, None);
    }

    #[test]
    fn latest_explicit_declaration_wins_and_omissions_do_not_clear() {
        let result = resolve_tribe(
            Some("g1"),
            vec![
                member("research", Some("g1"), Some("alpha"), None, "01", "a"),
                member("research", Some("g1"), Some("beta"), None, "02", "b"),
                member("research", Some("g1"), None, None, "03", "c"),
            ],
        );
        assert_eq!(result.tribe.as_deref(), Some("beta"));
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }

    #[test]
    fn repeated_declarations_and_generations_are_scoped() {
        let members = vec![
            member("research", Some("g1"), Some("same"), None, "01", "a"),
            member("research", Some("g1"), Some("same"), None, "02", "b"),
            member("research", Some("g2"), Some("other"), None, "03", "c"),
        ];
        assert_eq!(
            resolve_tribe(Some("g1"), members.clone()).tribe.as_deref(),
            Some("same")
        );
        assert_eq!(
            resolve_tribe(Some("g2"), members).tribe.as_deref(),
            Some("other")
        );
    }

    #[test]
    fn stable_identity_breaks_equal_timestamp_ties() {
        let result = resolve_tribe(
            Some("g1"),
            vec![
                member("research", Some("g1"), Some("alpha"), None, "01", "a"),
                member("research", Some("g1"), Some("beta"), None, "01", "b"),
            ],
        );
        assert_eq!(result.tribe.as_deref(), Some("beta"));
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }

    #[test]
    fn absent_summaries_resolve_to_none() {
        let result = resolve_summary(
            Some("g1"),
            vec![member("research", Some("g1"), None, None, "01", "a")],
        );
        assert_eq!(result.summary, None);
    }

    #[test]
    fn latest_explicit_summary_wins_and_omissions_do_not_clear() {
        let result = resolve_summary(
            Some("g1"),
            vec![
                member(
                    "research",
                    Some("g1"),
                    None,
                    Some("[bold]alpha[/bold]"),
                    "01",
                    "a",
                ),
                member(
                    "research",
                    Some("g1"),
                    None,
                    Some("  [bold]beta[/bold]\nsecond line  "),
                    "02",
                    "b",
                ),
                member("research", Some("g1"), None, None, "03", "c"),
            ],
        );
        assert_eq!(
            result.summary.as_deref(),
            Some("[bold]beta[/bold]\nsecond line")
        );
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }

    #[test]
    fn repeated_summaries_and_generations_are_scoped() {
        let members = vec![
            member("research", Some("g1"), None, Some("same"), "01", "a"),
            member("research", Some("g1"), None, Some("same"), "02", "b"),
            member("research", Some("g2"), None, Some("other"), "03", "c"),
        ];
        assert_eq!(
            resolve_summary(Some("g1"), members.clone())
                .summary
                .as_deref(),
            Some("same")
        );
        assert_eq!(
            resolve_summary(Some("g2"), members).summary.as_deref(),
            Some("other")
        );
    }

    #[test]
    fn stable_identity_breaks_equal_timestamp_summary_ties() {
        let result = resolve_summary(
            Some("g1"),
            vec![
                member("research", Some("g1"), None, Some("alpha"), "01", "a"),
                member("research", Some("g1"), None, Some("beta"), "01", "b"),
            ],
        );
        assert_eq!(result.summary.as_deref(), Some("beta"));
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }
}

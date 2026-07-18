//! Deterministic clan-level tribe resolution.

use serde::{Deserialize, Serialize};

/// One launched clan member considered by the tribe resolver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanTribeMemberWire {
    pub agent_clan: String,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub clan_tribe: Option<String>,
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

/// Select the latest explicit declaration for exactly one clan generation.
///
/// Members without `clan_tribe` never clear a prior declaration. Timestamp is
/// the primary launch-order key and the stable artifact identity breaks ties,
/// making the result independent of concurrent runner startup order.
pub fn resolve_clan_tribe(
    request: ClanTribeResolutionRequestWire,
) -> ClanTribeResolutionWire {
    let selected = request
        .members
        .iter()
        .filter(|member| {
            member.agent_clan == request.clan_name
                && member.agent_clan_generation == request.generation
        })
        .filter_map(|member| {
            member
                .clan_tribe
                .as_deref()
                .map(str::trim)
                .filter(|tribe| !tribe.is_empty())
                .map(|tribe| (member, tribe))
        })
        .max_by(|(left, _), (right, _)| {
            left.launch_timestamp
                .cmp(&right.launch_timestamp)
                .then_with(|| left.identity.cmp(&right.identity))
        });

    ClanTribeResolutionWire {
        clan_name: request.clan_name,
        generation: request.generation,
        tribe: selected.map(|(_, tribe)| tribe.to_string()),
        source_launch_timestamp: selected
            .map(|(member, _)| member.launch_timestamp.clone()),
        source_identity: selected.map(|(member, _)| member.identity.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn member(
        clan: &str,
        generation: Option<&str>,
        tribe: Option<&str>,
        timestamp: &str,
        identity: &str,
    ) -> ClanTribeMemberWire {
        ClanTribeMemberWire {
            agent_clan: clan.to_string(),
            agent_clan_generation: generation.map(str::to_string),
            clan_tribe: tribe.map(str::to_string),
            launch_timestamp: timestamp.to_string(),
            identity: identity.to_string(),
        }
    }

    fn resolve(
        generation: Option<&str>,
        members: Vec<ClanTribeMemberWire>,
    ) -> ClanTribeResolutionWire {
        resolve_clan_tribe(ClanTribeResolutionRequestWire {
            clan_name: "research".to_string(),
            generation: generation.map(str::to_string),
            members,
        })
    }

    #[test]
    fn absent_declarations_resolve_to_none() {
        let result = resolve(
            Some("g1"),
            vec![member("research", Some("g1"), None, "01", "a")],
        );
        assert_eq!(result.tribe, None);
    }

    #[test]
    fn latest_explicit_declaration_wins_and_omissions_do_not_clear() {
        let result = resolve(
            Some("g1"),
            vec![
                member("research", Some("g1"), Some("alpha"), "01", "a"),
                member("research", Some("g1"), Some("beta"), "02", "b"),
                member("research", Some("g1"), None, "03", "c"),
            ],
        );
        assert_eq!(result.tribe.as_deref(), Some("beta"));
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }

    #[test]
    fn repeated_declarations_and_generations_are_scoped() {
        let members = vec![
            member("research", Some("g1"), Some("same"), "01", "a"),
            member("research", Some("g1"), Some("same"), "02", "b"),
            member("research", Some("g2"), Some("other"), "03", "c"),
        ];
        assert_eq!(
            resolve(Some("g1"), members.clone()).tribe.as_deref(),
            Some("same")
        );
        assert_eq!(
            resolve(Some("g2"), members).tribe.as_deref(),
            Some("other")
        );
    }

    #[test]
    fn stable_identity_breaks_equal_timestamp_ties() {
        let result = resolve(
            Some("g1"),
            vec![
                member("research", Some("g1"), Some("alpha"), "01", "a"),
                member("research", Some("g1"), Some("beta"), "01", "b"),
            ],
        );
        assert_eq!(result.tribe.as_deref(), Some("beta"));
        assert_eq!(result.source_identity.as_deref(), Some("b"));
    }
}

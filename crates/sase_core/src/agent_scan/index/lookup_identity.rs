//! Effective lookup-session identity for indexed artifact queries.
//!
//! Mirrors `_agent_session_base_from_meta` / `_clan_identity_from_meta` in
//! sase's `_lookup_groups.py`. The existing `agent_session` column keeps its
//! documented meaning (`agent_meta.agent_session`); `lookup_session` is the
//! dedicated effective relation used by identity lookups.

use crate::agent_identity::parse_agent_session_name;
use crate::agent_scan::context::clan_key_from_meta;
use crate::agent_scan::wire::AgentMetaWire;

const PLAN_CHAIN_SUFFIXES: &[&str] =
    &["--plan", "--code", "--epic", "--commit", "--mon", "--gate"];
const LEGACY_DOTTED_SUFFIXES: &[&str] = &[".plan", ".code", ".epic", ".commit"];

/// Effective session relation used by targeted identity lookups.
pub(super) fn lookup_session_from_meta(meta: &AgentMetaWire) -> Option<String> {
    let session = nonempty(meta.agent_session.as_deref());
    if let Some(session) = session {
        return Some(session.to_string());
    }

    let workflow = nonempty(meta.workflow_name.as_deref())?;
    if is_plan_chain_artifact_meta(meta) {
        return Some(workflow.to_string());
    }
    let name = nonempty(meta.name.as_deref())?;
    if agent_session_base(name).as_deref() == Some(workflow) {
        return Some(workflow.to_string());
    }
    None
}

/// Clan identity name, including the legacy parallel-session form.
pub(super) fn lookup_clan_from_meta(meta: &AgentMetaWire) -> Option<String> {
    clan_key_from_meta(meta).map(|(clan, _)| clan)
}

/// True when metadata describes a plan-chain phase, matching
/// `is_plan_chain_artifact_meta`.
pub(super) fn is_plan_chain_artifact_meta(meta: &AgentMetaWire) -> bool {
    if meta
        .role_suffix
        .as_deref()
        .is_some_and(is_plan_chain_suffix)
    {
        return true;
    }
    let Some(name) = nonempty(meta.name.as_deref()) else {
        return false;
    };
    if let Some(workflow) = nonempty(meta.workflow_name.as_deref()) {
        if let Some(rest) = name.strip_prefix(workflow) {
            if is_plan_chain_suffix(rest) {
                return true;
            }
        }
    }
    agent_session_base(name).is_some()
}

fn agent_session_base(name: &str) -> Option<String> {
    if let Ok(parsed) = parse_agent_session_name(name) {
        if parsed.member_role.is_some() {
            return Some(parsed.agent_session_name);
        }
    }
    for suffix in LEGACY_DOTTED_SUFFIXES {
        if let Some(base) = name.strip_suffix(suffix) {
            if !base.is_empty() {
                return Some(base.to_string());
            }
        }
    }
    None
}

fn is_plan_chain_suffix(suffix: &str) -> bool {
    if suffix.is_empty() {
        return false;
    }
    if PLAN_CHAIN_SUFFIXES.contains(&suffix)
        || LEGACY_DOTTED_SUFFIXES.contains(&suffix)
    {
        return true;
    }
    if let Some(rest) = suffix.strip_prefix("--") {
        return !rest.is_empty()
            && rest.chars().all(|ch| {
                ch.is_ascii_alphanumeric() || ch == '_' || ch == '-'
            });
    }
    false
}

fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(
        name: Option<&str>,
        workflow: Option<&str>,
        session: Option<&str>,
        role_suffix: Option<&str>,
    ) -> AgentMetaWire {
        AgentMetaWire {
            name: name.map(str::to_string),
            workflow_name: workflow.map(str::to_string),
            agent_session: session.map(str::to_string),
            role_suffix: role_suffix.map(str::to_string),
            ..AgentMetaWire::default()
        }
    }

    #[test]
    fn explicit_session_wins() {
        let value =
            meta(Some("lane-a--plan"), Some("other"), Some("lane-a"), None);
        assert_eq!(lookup_session_from_meta(&value).as_deref(), Some("lane-a"));
    }

    #[test]
    fn plan_chain_workflow_is_session() {
        let value = meta(Some("lane-a--code"), Some("lane-a"), None, None);
        assert_eq!(lookup_session_from_meta(&value).as_deref(), Some("lane-a"));
    }

    #[test]
    fn role_suffix_marks_plan_chain() {
        let value =
            meta(Some("lane-a--plan"), Some("lane-a"), None, Some("--plan"));
        assert!(is_plan_chain_artifact_meta(&value));
        assert_eq!(lookup_session_from_meta(&value).as_deref(), Some("lane-a"));
    }

    #[test]
    fn member_name_matching_workflow_is_session() {
        let value = meta(Some("lane-a--mon"), Some("lane-a"), None, None);
        assert_eq!(lookup_session_from_meta(&value).as_deref(), Some("lane-a"));
    }

    #[test]
    fn unrelated_workflow_is_not_a_session_member() {
        let value = meta(Some("solo"), Some("lane-a"), None, None);
        assert_eq!(lookup_session_from_meta(&value), None);
    }

    #[test]
    fn legacy_dotted_suffix_is_session() {
        let value = meta(Some("lane-a.plan"), Some("lane-a"), None, None);
        assert_eq!(lookup_session_from_meta(&value).as_deref(), Some("lane-a"));
    }

    #[test]
    fn parallel_session_is_clan() {
        let value = AgentMetaWire {
            name: Some("wave".into()),
            agent_session: Some("wave".into()),
            agent_session_parallel: true,
            ..AgentMetaWire::default()
        };
        assert_eq!(lookup_clan_from_meta(&value).as_deref(), Some("wave"));
    }

    #[test]
    fn explicit_clan_wins_over_session() {
        let value = AgentMetaWire {
            name: Some("wave".into()),
            agent_clan: Some("clan-a".into()),
            agent_session: Some("wave".into()),
            agent_session_parallel: true,
            ..AgentMetaWire::default()
        };
        assert_eq!(lookup_clan_from_meta(&value).as_deref(), Some("clan-a"));
    }
}

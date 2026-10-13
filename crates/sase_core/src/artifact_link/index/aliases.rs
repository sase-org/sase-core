//! Pass-2, first-wins alias spellings for indexed refs.

use crate::agent_identity::{
    foreign_agent_owner_root, globalize_agent_name, normalize_owned_agent_name,
    AgentOwnerIdentity,
};

use super::target::parse_index_ref;
use super::wire::ArtifactLinkIndexIdentityWire;

#[derive(Debug, Clone)]
pub(crate) struct IndexIdentity {
    pub owner: Option<AgentOwnerIdentity>,
    pub known_owner_roots: Vec<String>,
}

impl IndexIdentity {
    pub(crate) fn from_wire(wire: &ArtifactLinkIndexIdentityWire) -> Self {
        let roots = super::wire::default_known_owner_roots(wire);
        let owner = match (
            wire.username.as_deref().filter(|value| !value.is_empty()),
            wire.machine_name
                .as_deref()
                .filter(|value| !value.is_empty()),
        ) {
            (Some(username), Some(machine_name)) => {
                AgentOwnerIdentity::new(username, machine_name).ok()
            }
            _ => None,
        };
        Self {
            owner,
            known_owner_roots: roots,
        }
    }
}

/// Extra spellings a canonical ref should also be reachable under.
///
/// Canonical entries take precedence: callers insert these with first-wins.
pub(crate) fn aliases_for_ref(
    reference: &str,
    identity: &IndexIdentity,
) -> Vec<String> {
    let Some(parsed) = parse_index_ref(reference) else {
        return Vec::new();
    };
    match parsed.kind.as_str() {
        "agent" => agent_aliases(&parsed.payload, identity)
            .into_iter()
            .map(|candidate| format!("agent:{candidate}"))
            .collect(),
        "stitch" => {
            let Some((repo, sha)) = parsed.payload.split_once('@') else {
                return Vec::new();
            };
            if repo.is_empty() || sha.chars().count() <= 7 {
                return Vec::new();
            }
            let short: String = sha.chars().take(7).collect();
            vec![format!("stitch:{repo}@{short}")]
        }
        "plan" => vec![format!("ref:plan:{}", parsed.payload)],
        "chop" => vec![format!("job:{}", parsed.payload)],
        _ => Vec::new(),
    }
}

/// Exact-first current-owner compatibility lookup spellings for an agent
/// payload, matching `current_owner_agent_name_lookup_candidates`.
pub(crate) fn agent_lookup_candidates(
    name: &str,
    identity: &IndexIdentity,
) -> Vec<String> {
    agent_aliases(name, identity)
}

fn agent_aliases(name: &str, identity: &IndexIdentity) -> Vec<String> {
    let Some(owner) = identity.owner.as_ref() else {
        return vec![name.to_string()];
    };
    match foreign_agent_owner_root(name, owner, &identity.known_owner_roots) {
        Ok(Some(_)) => return vec![name.to_string()],
        Ok(None) => {}
        Err(_) => return vec![name.to_string()],
    }
    let (prefix, core_name) = split_dismissed_prefix(name);
    let Ok(bare) = normalize_owned_agent_name(
        core_name,
        owner,
        &identity.known_owner_roots,
    ) else {
        return vec![name.to_string()];
    };
    let mut out = Vec::new();
    push_unique(&mut out, name.to_string());
    push_unique(&mut out, format!("{prefix}{bare}"));
    push_unique(&mut out, format!("{prefix}{}.{bare}", owner.machine_name));
    if let Ok(global) = globalize_agent_name(&bare, owner) {
        push_unique(&mut out, format!("{prefix}{global}"));
    }
    out
}

fn split_dismissed_prefix(name: &str) -> (&str, &str) {
    let bytes = name.as_bytes();
    if bytes.len() > 7
        && bytes[..6].iter().all(u8::is_ascii_digit)
        && bytes[6] == b'.'
    {
        name.split_at(7)
    } else {
        ("", name)
    }
}

fn push_unique(out: &mut Vec<String>, value: String) {
    if !out.iter().any(|existing| existing == &value) {
        out.push(value);
    }
}

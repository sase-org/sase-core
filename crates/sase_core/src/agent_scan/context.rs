//! Generation-scoped clan context for agent artifact snapshots.

use std::collections::BTreeSet;

use crate::agent_clan_tribe::{
    resolve_clan_summary, resolve_clan_tribe, ClanTribeMemberWire,
    ClanTribeResolutionRequestWire,
};

use super::wire::{
    AgentArtifactRecordWire, AgentClanContextWire, AgentMetaWire,
};

pub(crate) type ClanGenerationKey = (String, Option<String>);

pub(crate) fn clan_key_from_meta(
    meta: &AgentMetaWire,
) -> Option<ClanGenerationKey> {
    let clan = meta
        .agent_clan
        .as_deref()
        .or_else(|| {
            meta.agent_family_parallel
                .then_some(meta.agent_family.as_deref())
                .flatten()
        })?
        .trim();
    if clan.is_empty() {
        return None;
    }
    let generation = meta
        .agent_clan_generation
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    Some((clan.to_string(), generation))
}

pub(crate) fn represented_clan_keys(
    records: &[AgentArtifactRecordWire],
) -> BTreeSet<ClanGenerationKey> {
    records
        .iter()
        .filter_map(|record| {
            record.agent_meta.as_ref().and_then(clan_key_from_meta)
        })
        .collect()
}

pub(crate) fn clan_member_from_record(
    record: &AgentArtifactRecordWire,
) -> Option<ClanTribeMemberWire> {
    let meta = record.agent_meta.as_ref()?;
    let (agent_clan, agent_clan_generation) = clan_key_from_meta(meta)?;
    Some(ClanTribeMemberWire {
        agent_clan,
        agent_clan_generation,
        clan_tribe: meta.clan_tribe.clone(),
        clan_summary: meta.clan_summary.clone(),
        launch_timestamp: record.timestamp.clone(),
        identity: record.artifact_dir.clone(),
    })
}

pub(crate) fn resolve_clan_context(
    keys: BTreeSet<ClanGenerationKey>,
    members: Vec<ClanTribeMemberWire>,
) -> Vec<AgentClanContextWire> {
    keys.into_iter()
        .map(|(agent_clan, agent_clan_generation)| {
            let request = ClanTribeResolutionRequestWire {
                clan_name: agent_clan.clone(),
                generation: agent_clan_generation.clone(),
                members: members.clone(),
            };
            let tribe = resolve_clan_tribe(request.clone());
            let summary = resolve_clan_summary(request);
            AgentClanContextWire {
                agent_clan,
                agent_clan_generation,
                clan_tribe: tribe.tribe,
                clan_summary: summary.summary,
                clan_tribe_source_launch_timestamp: tribe
                    .source_launch_timestamp,
                clan_tribe_source_identity: tribe.source_identity,
                clan_summary_source_launch_timestamp: summary
                    .source_launch_timestamp,
                clan_summary_source_identity: summary.source_identity,
            }
        })
        .collect()
}

pub(crate) fn resolve_clan_context_from_records(
    records: &[AgentArtifactRecordWire],
) -> Vec<AgentClanContextWire> {
    let keys = represented_clan_keys(records);
    let members = records.iter().filter_map(clan_member_from_record).collect();
    resolve_clan_context(keys, members)
}

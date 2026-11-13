//! Compact classification and selection for runner-capacity scans.

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::path::Path;

use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use super::wire::{AgentArtifactRecordWire, AgentMetaWire};

/// Fields from `agent_meta.json` that affect capacity retention or clan
/// context. Other fields are skipped by the JSON visitor without building
/// their value trees.
const CAPACITY_META_FIELDS: &[&str] = &[
    "agent_clan",
    "agent_clan_generation",
    "agent_family",
    "agent_family_parallel",
    "clan_summary",
    "clan_tribe",
    "parent_timestamp",
    "pid",
    "runner_claim_owner_key",
    "stopped_at",
    "agent_session",
];

/// The part of a record needed for the capacity liveness and retention
/// closure. The parser keeps this projection for every candidate, including
/// candidates that will not be hydrated into a full wire record.
#[derive(Debug, Clone)]
pub(crate) struct CapacityProjection {
    pub project_name: String,
    pub timestamp: String,
    pub artifact_dir: String,
    pub meta: Option<AgentMetaWire>,
}

impl CapacityProjection {
    pub(crate) fn from_record(record: &AgentArtifactRecordWire) -> Self {
        Self {
            project_name: record.project_name.clone(),
            timestamp: record.timestamp.clone(),
            artifact_dir: record.artifact_dir.clone(),
            meta: record.agent_meta.clone(),
        }
    }
}

#[derive(Debug)]
struct RelevantMetaFields(Map<String, Value>);

impl<'de> Deserialize<'de> for RelevantMetaFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RelevantMetaVisitor;

        impl<'de> Visitor<'de> for RelevantMetaVisitor {
            type Value = RelevantMetaFields;

            fn expecting(
                &self,
                formatter: &mut std::fmt::Formatter<'_>,
            ) -> std::fmt::Result {
                formatter.write_str("an agent metadata object")
            }

            fn visit_map<A>(
                self,
                mut entries: A,
            ) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = Map::new();
                while let Some(key) = entries.next_key::<String>()? {
                    if CAPACITY_META_FIELDS.contains(&key.as_str()) {
                        fields.insert(key, entries.next_value::<Value>()?);
                    } else {
                        entries.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(RelevantMetaFields(fields))
            }
        }

        deserializer.deserialize_map(RelevantMetaVisitor)
    }
}

/// Decode only fields used by capacity selection while retaining the
/// scanner's established coercion and alias rules in `agent_meta_from_object`.
pub(crate) fn parse_capacity_meta(
    bytes: &[u8],
) -> Result<AgentMetaWire, serde_json::Error> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let RelevantMetaFields(fields) =
        RelevantMetaFields::deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(super::scanner::agent_meta_from_object(&fields))
}

/// Decide whether a conclusively markerless projection can never represent a
/// live runner. Callers use this only after all three live markers have been
/// proven absent with `symlink_metadata`.
pub(crate) fn markerless_projection_is_never_live_with<F>(
    projection: &CapacityProjection,
    process_is_missing: F,
) -> bool
where
    F: Fn(i32) -> bool + Copy,
{
    let Some(meta) = projection.meta.as_ref() else {
        return false;
    };
    if meta
        .stopped_at
        .as_deref()
        .is_some_and(|value| !value.is_empty())
    {
        return true;
    }
    process_id_is_provably_missing(meta.pid, process_is_missing)
}

/// Apply the collection-wide capacity closure to compact projections. The
/// order and key normalization intentionally match the legacy record pass.
///
/// Projections are borrowed so archive-scale scans can pass references into
/// their candidate table instead of cloning every compact meta a second time.
pub(crate) fn retain_capacity_projections<P>(
    projections: &[P],
    initially_live: &[bool],
) -> Vec<bool>
where
    P: Borrow<CapacityProjection>,
{
    debug_assert_eq!(projections.len(), initially_live.len());
    let mut keep = initially_live.to_vec();
    let mut by_project_timestamp: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();
    let mut by_owner_key: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();
    let mut clan_tribe_sources: BTreeMap<(String, String), Vec<usize>> =
        BTreeMap::new();

    for (index, projection) in projections.iter().enumerate() {
        let projection: &CapacityProjection = projection.borrow();
        by_project_timestamp
            .entry((
                projection.project_name.clone(),
                projection.timestamp.clone(),
            ))
            .or_default()
            .push(index);
        let Some(meta) = projection.meta.as_ref() else {
            continue;
        };
        if let Some(owner_key) =
            normalized_record_key(meta.runner_claim_owner_key.as_deref())
        {
            by_owner_key
                .entry((projection.project_name.clone(), owner_key))
                .or_default()
                .push(index);
        }
        if meta
            .clan_tribe
            .as_deref()
            .is_some_and(|value| !value.is_empty())
        {
            if let Some(clan_key) =
                hold_clan_generation_key(&projection.timestamp, meta)
            {
                clan_tribe_sources.entry(clan_key).or_default().push(index);
            }
        }
    }

    let mut pending = keep
        .iter()
        .enumerate()
        .filter_map(|(index, retained)| retained.then_some(index))
        .collect::<Vec<_>>();
    let mut cursor = 0;
    while cursor < pending.len() {
        let index = pending[cursor];
        cursor += 1;
        let projection: &CapacityProjection = projections[index].borrow();
        let Some(meta) = projection.meta.as_ref() else {
            continue;
        };

        if let Some(parent_timestamp) =
            normalized_record_key(meta.parent_timestamp.as_deref())
        {
            if let Some(ancestors) = by_project_timestamp
                .get(&(projection.project_name.clone(), parent_timestamp))
            {
                retain_indexed_records(ancestors, &mut keep, &mut pending);
            }
        }
        if let Some(owner_key) =
            normalized_record_key(meta.runner_claim_owner_key.as_deref())
        {
            if let Some(neighbours) =
                by_owner_key.get(&(projection.project_name.clone(), owner_key))
            {
                retain_indexed_records(neighbours, &mut keep, &mut pending);
            }
        }
        if let Some(clan_key) =
            hold_clan_generation_key(&projection.timestamp, meta)
        {
            if let Some(sources) = clan_tribe_sources.get(&clan_key) {
                retain_indexed_records(sources, &mut keep, &mut pending);
            }
        }
    }
    keep
}

fn retain_indexed_records(
    indexes: &[usize],
    keep: &mut [bool],
    pending: &mut Vec<usize>,
) {
    for &index in indexes {
        if !keep[index] {
            keep[index] = true;
            pending.push(index);
        }
    }
}

fn normalized_record_key(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn hold_clan_generation_key(
    timestamp: &str,
    meta: &AgentMetaWire,
) -> Option<(String, String)> {
    let clan = meta.agent_clan.as_deref().filter(|clan| !clan.is_empty())?;
    let generation = [
        meta.agent_clan_generation.as_deref(),
        meta.parent_timestamp.as_deref(),
        Some(timestamp),
    ]
    .into_iter()
    .flatten()
    .find(|value| !value.is_empty())?;
    Some((clan.to_string(), generation.to_string()))
}

pub(crate) fn process_id_is_provably_missing<F>(
    pid: Option<i64>,
    process_is_missing: F,
) -> bool
where
    F: Fn(i32) -> bool,
{
    pid.filter(|pid| *pid > 1)
        .and_then(|pid| i32::try_from(pid).ok())
        .is_some_and(process_is_missing)
}

/// Return the clan member represented by this compact projection without
/// hydrating its complete artifact record.
pub(crate) fn clan_member(
    projection: &CapacityProjection,
) -> Option<crate::agent_clan_tribe::ClanTribeMemberWire> {
    let meta = projection.meta.as_ref()?;
    let (agent_clan, agent_clan_generation) =
        super::context::clan_key_from_meta(meta)?;
    Some(crate::agent_clan_tribe::ClanTribeMemberWire {
        agent_clan,
        agent_clan_generation,
        clan_tribe: meta.clan_tribe.clone(),
        clan_summary: meta.clan_summary.clone(),
        launch_timestamp: projection.timestamp.clone(),
        identity: projection.artifact_dir.clone(),
    })
}

/// Whether a metadata path could contain marker data. The caller falls back
/// to the full parser whenever the answer is uncertain.
pub(crate) fn marker_may_exist(path: &Path) -> bool {
    match std::fs::symlink_metadata(path) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

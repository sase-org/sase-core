use super::placeholders;
use super::storage::open_index_read_only;
use crate::agent_clan_tribe::ClanTribeMemberWire;
use crate::agent_scan::context::{
    represented_clan_keys, resolve_clan_context, ClanGenerationKey,
};
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentOutputVariableHistoryQueryWire,
    AgentOutputVariableHistoryWire, AgentOutputVariableKeyGroupWire,
    AgentOutputVariableLimitWire, AgentOutputVariableOccurrenceWire,
    AgentOutputVariableValueGroupWire, OutputVariableValue,
    AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION,
};
use rusqlite::{params, params_from_iter, Connection};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// Query grouped output-variable history from the persistent artifact index.
pub fn query_agent_output_variable_history(
    index_path: &Path,
    query: AgentOutputVariableHistoryQueryWire,
) -> Result<AgentOutputVariableHistoryWire, String> {
    if !query.values.is_empty() && !query.value_json.is_empty() {
        return Err(
            "values and value_json filters are mutually exclusive".to_string()
        );
    }

    let conn = open_index_read_only(index_path)?;
    let exact_value_json = query
        .value_json
        .iter()
        .map(canonical_output_variable_json)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let rows =
        select_output_variable_occurrences(&conn, &query, &exact_value_json)?;
    let mut occurrences = Vec::new();
    for row in rows {
        let occurrence = row.into_occurrence()?;
        if !output_variable_occurrence_matches_filters(&occurrence, &query) {
            continue;
        }
        occurrences.push(occurrence);
    }
    occurrences.sort_by(compare_output_variable_occurrences_newest);

    let mut keys: BTreeMap<String, OutputVariableKeyAccumulator> =
        BTreeMap::new();
    for occurrence in occurrences {
        keys.entry(occurrence.key.clone())
            .or_insert_with(|| {
                OutputVariableKeyAccumulator::new(occurrence.key.clone())
            })
            .push(occurrence);
    }

    let mut key_groups: Vec<AgentOutputVariableKeyGroupWire> = keys
        .into_values()
        .map(|accumulator| {
            accumulator.into_wire(query.value_limit, query.reverse)
        })
        .collect();
    sort_output_variable_key_groups(&mut key_groups, query.reverse);

    let total_key_count = key_groups.len() as u64;
    let returned_key_count =
        truncate_to_limit(&mut key_groups, query.key_limit) as u64;
    let requested_key_limit = query.key_limit;

    Ok(AgentOutputVariableHistoryWire {
        schema_version: AGENT_OUTPUT_VARIABLE_HISTORY_WIRE_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        query,
        keys_limit: AgentOutputVariableLimitWire {
            limit: requested_key_limit,
            total_count: total_key_count,
            returned_count: returned_key_count,
            truncated: returned_key_count < total_key_count,
        },
        groups: key_groups,
    })
}

/// Load indexed output-variable occurrences for selector resolution.
pub(crate) fn load_output_variable_occurrences(
    index_path: &Path,
    projects: &[String],
    include_hidden: bool,
) -> Result<Vec<AgentOutputVariableOccurrenceWire>, String> {
    let query = AgentOutputVariableHistoryQueryWire {
        projects: projects.to_vec(),
        include_hidden,
        key_limit: 0,
        value_limit: 0,
        ..AgentOutputVariableHistoryQueryWire::default()
    };
    let conn = open_index_read_only(index_path)?;
    let rows =
        select_output_variable_occurrences(&conn, &query, &BTreeSet::new())?;
    let mut occurrences = Vec::new();
    for row in rows {
        occurrences.push(row.into_occurrence()?);
    }
    occurrences.sort_by(compare_output_variable_occurrences_newest);
    Ok(occurrences)
}

pub(super) fn select_output_variable_occurrences(
    conn: &Connection,
    query: &AgentOutputVariableHistoryQueryWire,
    exact_value_json: &BTreeSet<String>,
) -> Result<Vec<IndexedOutputVariableOccurrence>, String> {
    let mut clauses: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();
    if !query.include_hidden {
        clauses.push("hidden = 0".to_string());
    }
    if !query.projects.is_empty() {
        clauses.push(format!(
            "project_name IN ({})",
            placeholders(query.projects.len())
        ));
        values.extend(query.projects.iter().cloned());
    }
    if let Some(since) = query.since_timestamp.as_ref() {
        clauses.push("timestamp >= ?".to_string());
        values.push(since.clone());
    }
    if let Some(until) = query.until_timestamp.as_ref() {
        clauses.push("timestamp <= ?".to_string());
        values.push(until.clone());
    }
    if !exact_value_json.is_empty() {
        clauses.push(format!(
            "value_json IN ({})",
            placeholders(exact_value_json.len())
        ));
        values.extend(exact_value_json.iter().cloned());
    }

    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    let sql = format!(
        r#"
        SELECT artifact_dir, project_name, workflow_dir_name, timestamp,
               agent_name, cl_name, variable_key, value_json,
               hidden
        FROM agent_output_variables
        {where_sql}
        ORDER BY timestamp DESC, project_name ASC, artifact_dir ASC,
                 variable_key ASC
        "#
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(values.iter()))
        .map_err(|e| e.to_string())?;
    let mut result = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        result.push(IndexedOutputVariableOccurrence {
            artifact_dir: row.get(0).map_err(|e| e.to_string())?,
            project_name: row.get(1).map_err(|e| e.to_string())?,
            workflow_dir_name: row.get(2).map_err(|e| e.to_string())?,
            timestamp: row.get(3).map_err(|e| e.to_string())?,
            agent_name: row.get(4).map_err(|e| e.to_string())?,
            cl_name: row.get(5).map_err(|e| e.to_string())?,
            key: row.get(6).map_err(|e| e.to_string())?,
            value_json: row.get(7).map_err(|e| e.to_string())?,
            hidden: row.get::<_, i64>(8).map_err(|e| e.to_string())? != 0,
        });
    }
    Ok(result)
}

#[derive(Debug, Clone)]
pub(super) struct IndexedOutputVariableOccurrence {
    pub(super) artifact_dir: String,
    pub(super) project_name: String,
    pub(super) workflow_dir_name: String,
    pub(super) timestamp: String,
    pub(super) agent_name: Option<String>,
    pub(super) cl_name: Option<String>,
    pub(super) key: String,
    pub(super) value_json: String,
    pub(super) hidden: bool,
}

impl IndexedOutputVariableOccurrence {
    pub(super) fn into_occurrence(
        self,
    ) -> Result<AgentOutputVariableOccurrenceWire, String> {
        let value =
            serde_json::from_str::<OutputVariableValue>(&self.value_json)
                .map_err(|e| {
                    format!(
                        "invalid indexed output-variable JSON for {}:{}: {e}",
                        self.artifact_dir, self.key
                    )
                })?;
        Ok(AgentOutputVariableOccurrenceWire {
            artifact_dir: self.artifact_dir,
            project_name: self.project_name,
            workflow_dir_name: self.workflow_dir_name,
            timestamp: self.timestamp,
            agent_name: self.agent_name,
            cl_name: self.cl_name,
            key: self.key,
            value,
            value_json: self.value_json,
            hidden: self.hidden,
        })
    }
}

pub(super) fn output_variable_occurrence_matches_filters(
    occurrence: &AgentOutputVariableOccurrenceWire,
    query: &AgentOutputVariableHistoryQueryWire,
) -> bool {
    if !query.agents.is_empty()
        && !query.agents.iter().any(|pattern| {
            occurrence
                .agent_name
                .as_deref()
                .is_some_and(|agent| agent_pattern_matches(pattern, agent))
        })
    {
        return false;
    }
    if !query.keys.is_empty()
        && !query
            .keys
            .iter()
            .any(|pattern| glob_matches(pattern, &occurrence.key))
    {
        return false;
    }
    if !query.values.is_empty()
        && !query.values.iter().any(|needle| {
            output_variable_value_contains(&occurrence.value, needle)
        })
    {
        return false;
    }
    true
}

pub(super) fn output_variable_value_contains(
    value: &OutputVariableValue,
    needle: &str,
) -> bool {
    let needle = needle.to_lowercase();
    if needle.is_empty() {
        return true;
    }
    canonical_output_variable_json(value)
        .map(|json| json.to_lowercase().contains(&needle))
        .unwrap_or(false)
        || output_variable_scalar_text(value)
            .map(|text| text.to_lowercase().contains(&needle))
            .unwrap_or(false)
}

pub(super) fn agent_pattern_matches(pattern: &str, agent_name: &str) -> bool {
    if let Some(hood) = pattern.strip_suffix(".*") {
        if agent_name == hood
            || agent_name
                .strip_prefix(hood)
                .is_some_and(|suffix| suffix.starts_with('.'))
        {
            return true;
        }
    }
    glob_matches(pattern, agent_name)
}

pub(super) fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern = pattern.as_bytes();
    let value = value.as_bytes();
    let (mut p, mut v) = (0usize, 0usize);
    let mut star: Option<usize> = None;
    let mut star_value = 0usize;
    while v < value.len() {
        if p < pattern.len() && pattern[p] == value[v] {
            p += 1;
            v += 1;
        } else if p < pattern.len() && pattern[p] == b'*' {
            star = Some(p);
            p += 1;
            star_value = v;
        } else if let Some(star_index) = star {
            p = star_index + 1;
            star_value += 1;
            v = star_value;
        } else {
            return false;
        }
    }
    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }
    p == pattern.len()
}

#[derive(Debug)]
pub(super) struct OutputVariableKeyAccumulator {
    pub(super) key: String,
    pub(super) occurrences: Vec<AgentOutputVariableOccurrenceWire>,
    pub(super) values: BTreeMap<String, OutputVariableValueAccumulator>,
}

impl OutputVariableKeyAccumulator {
    pub(super) fn new(key: String) -> Self {
        Self {
            key,
            occurrences: Vec::new(),
            values: BTreeMap::new(),
        }
    }

    pub(super) fn push(
        &mut self,
        occurrence: AgentOutputVariableOccurrenceWire,
    ) {
        self.values
            .entry(occurrence.value_json.clone())
            .or_insert_with(|| {
                OutputVariableValueAccumulator::new(
                    occurrence.value.clone(),
                    occurrence.value_json.clone(),
                )
            })
            .push(occurrence.clone());
        self.occurrences.push(occurrence);
    }

    pub(super) fn into_wire(
        self,
        value_limit: u32,
        reverse: bool,
    ) -> AgentOutputVariableKeyGroupWire {
        let occurrence_count = self.occurrences.len() as u64;
        let mut values: Vec<AgentOutputVariableValueGroupWire> = self
            .values
            .into_values()
            .map(OutputVariableValueAccumulator::into_wire)
            .collect();
        sort_output_variable_value_groups(&mut values, reverse);
        let total_value_count = values.len() as u64;
        let returned_value_count =
            truncate_to_limit(&mut values, value_limit) as u64;
        AgentOutputVariableKeyGroupWire {
            key: self.key,
            occurrence_count,
            distinct_value_count: total_value_count,
            values_limit: AgentOutputVariableLimitWire {
                limit: value_limit,
                total_count: total_value_count,
                returned_count: returned_value_count,
                truncated: returned_value_count < total_value_count,
            },
            values,
        }
    }
}

#[derive(Debug)]
pub(super) struct OutputVariableValueAccumulator {
    pub(super) value: OutputVariableValue,
    pub(super) value_json: String,
    pub(super) occurrences: Vec<AgentOutputVariableOccurrenceWire>,
    pub(super) agent_latest: BTreeMap<String, String>,
    pub(super) projects: BTreeSet<String>,
}

impl OutputVariableValueAccumulator {
    pub(super) fn new(value: OutputVariableValue, value_json: String) -> Self {
        Self {
            value,
            value_json,
            occurrences: Vec::new(),
            agent_latest: BTreeMap::new(),
            projects: BTreeSet::new(),
        }
    }

    pub(super) fn push(
        &mut self,
        occurrence: AgentOutputVariableOccurrenceWire,
    ) {
        if let Some(agent_name) = occurrence.agent_name.as_ref() {
            let entry = self.agent_latest.entry(agent_name.clone());
            entry
                .and_modify(|timestamp| {
                    if occurrence.timestamp > *timestamp {
                        *timestamp = occurrence.timestamp.clone();
                    }
                })
                .or_insert_with(|| occurrence.timestamp.clone());
        }
        self.projects.insert(occurrence.project_name.clone());
        self.occurrences.push(occurrence);
    }

    pub(super) fn into_wire(mut self) -> AgentOutputVariableValueGroupWire {
        self.occurrences
            .sort_by(compare_output_variable_occurrences_newest);
        let newest = self.occurrences.first().cloned().unwrap_or_else(|| {
            AgentOutputVariableOccurrenceWire {
                artifact_dir: String::new(),
                project_name: String::new(),
                workflow_dir_name: String::new(),
                timestamp: String::new(),
                agent_name: None,
                cl_name: None,
                key: String::new(),
                value: self.value.clone(),
                value_json: self.value_json.clone(),
                hidden: false,
            }
        });
        let first_seen_timestamp = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.timestamp.as_str())
            .min()
            .unwrap_or("")
            .to_string();
        let last_seen_timestamp = self
            .occurrences
            .iter()
            .map(|occurrence| occurrence.timestamp.as_str())
            .max()
            .unwrap_or("")
            .to_string();
        let mut agents: Vec<(String, String)> =
            self.agent_latest.into_iter().collect();
        agents.sort_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let agents: Vec<String> =
            agents.into_iter().map(|(agent, _)| agent).collect();
        let agent_count = agents.len() as u64;

        AgentOutputVariableValueGroupWire {
            value: self.value,
            value_json: self.value_json,
            occurrence_count: self.occurrences.len() as u64,
            agent_count,
            agents,
            projects: self.projects.into_iter().collect(),
            first_seen_timestamp,
            last_seen_timestamp,
            newest,
        }
    }
}

pub(super) fn truncate_to_limit<T>(items: &mut Vec<T>, limit: u32) -> usize {
    if limit > 0 && items.len() > limit as usize {
        items.truncate(limit as usize);
    }
    items.len()
}

pub(super) fn sort_output_variable_key_groups(
    groups: &mut [AgentOutputVariableKeyGroupWire],
    reverse: bool,
) {
    groups.sort_by(|left, right| {
        let ordering = compare_output_variable_value_groups_by_seen(
            representative_value_group_for_key(left, reverse),
            representative_value_group_for_key(right, reverse),
            reverse,
        );
        ordering.then_with(|| left.key.cmp(&right.key))
    });
    for group in groups {
        sort_output_variable_value_groups(&mut group.values, reverse);
    }
}

pub(super) fn representative_value_group_for_key(
    group: &AgentOutputVariableKeyGroupWire,
    reverse: bool,
) -> Option<&AgentOutputVariableValueGroupWire> {
    group.values.iter().min_by(|left, right| {
        compare_output_variable_value_groups_by_seen(
            Some(*left),
            Some(*right),
            reverse,
        )
    })
}

pub(super) fn sort_output_variable_value_groups(
    groups: &mut [AgentOutputVariableValueGroupWire],
    reverse: bool,
) {
    groups.sort_by(|left, right| {
        compare_output_variable_value_groups_by_seen(
            Some(left),
            Some(right),
            reverse,
        )
        .then_with(|| left.value_json.cmp(&right.value_json))
    });
}

pub(super) fn compare_output_variable_value_groups_by_seen(
    left: Option<&AgentOutputVariableValueGroupWire>,
    right: Option<&AgentOutputVariableValueGroupWire>,
    reverse: bool,
) -> Ordering {
    let Some(left) = left else {
        return Ordering::Greater;
    };
    let Some(right) = right else {
        return Ordering::Less;
    };
    if reverse {
        left.first_seen_timestamp
            .cmp(&right.first_seen_timestamp)
            .then_with(|| {
                left.newest.project_name.cmp(&right.newest.project_name)
            })
            .then_with(|| {
                left.newest.artifact_dir.cmp(&right.newest.artifact_dir)
            })
    } else {
        right
            .last_seen_timestamp
            .cmp(&left.last_seen_timestamp)
            .then_with(|| {
                left.newest.project_name.cmp(&right.newest.project_name)
            })
            .then_with(|| {
                left.newest.artifact_dir.cmp(&right.newest.artifact_dir)
            })
    }
}

pub(crate) fn compare_output_variable_occurrences_newest(
    left: &AgentOutputVariableOccurrenceWire,
    right: &AgentOutputVariableOccurrenceWire,
) -> Ordering {
    right
        .timestamp
        .cmp(&left.timestamp)
        .then_with(|| left.project_name.cmp(&right.project_name))
        .then_with(|| left.artifact_dir.cmp(&right.artifact_dir))
        .then_with(|| left.key.cmp(&right.key))
}

pub(super) fn select_clan_context(
    conn: &Connection,
    records: &[AgentArtifactRecordWire],
) -> Result<Vec<crate::agent_scan::wire::AgentClanContextWire>, String> {
    select_clan_context_for_keys(conn, represented_clan_keys(records))
}

pub(super) fn select_clan_context_for_keys(
    conn: &Connection,
    keys: BTreeSet<ClanGenerationKey>,
) -> Result<Vec<crate::agent_scan::wire::AgentClanContextWire>, String> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut members = Vec::new();
    let mut stmt = conn
        .prepare(
            "SELECT agent_clan, agent_clan_generation, clan_tribe, \
                    clan_summary, timestamp, artifact_dir \
             FROM agent_artifacts \
             WHERE agent_clan = ?1 \
               AND (agent_clan_generation = ?2 \
                    OR (?2 IS NULL AND agent_clan_generation IS NULL)) \
               AND (NULLIF(TRIM(clan_tribe), '') IS NOT NULL \
                    OR NULLIF(TRIM(clan_summary), '') IS NOT NULL)",
        )
        .map_err(|error| error.to_string())?;
    for (agent_clan, agent_clan_generation) in &keys {
        let rows = stmt
            .query_map(params![agent_clan, agent_clan_generation], |row| {
                Ok(ClanTribeMemberWire {
                    agent_clan: row.get(0)?,
                    agent_clan_generation: row.get(1)?,
                    clan_tribe: row.get(2)?,
                    clan_summary: row.get(3)?,
                    launch_timestamp: row.get(4)?,
                    identity: row.get(5)?,
                })
            })
            .map_err(|error| error.to_string())?;
        for row in rows {
            members.push(row.map_err(|error| error.to_string())?);
        }
    }
    Ok(resolve_clan_context(keys, members))
}

pub(crate) fn canonical_output_variable_json(
    value: &OutputVariableValue,
) -> Result<String, String> {
    serde_json::to_string(value).map_err(|e| e.to_string())
}

pub(super) fn output_variable_scalar_text(
    value: &OutputVariableValue,
) -> Option<String> {
    match value {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

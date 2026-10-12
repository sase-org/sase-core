use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::agent_archive::corpus::compile::worse_outcome;
use crate::agent_archive::corpus::prepared::{
    outcome_key, PreparedAgentArchiveCorpus,
};
use crate::agent_archive::corpus::wire::{
    AgentArchiveCorpusRowWire, AgentArchiveCountRequestWire,
    AgentArchiveLightRowWire, AgentArchiveLookupRequestWire,
    AgentArchiveOutcomeWire, AgentArchiveQueryCountWire,
    AgentArchiveQueryError, AgentArchiveQueryGroupWire,
    AgentArchiveQueryLookupWire, AgentArchiveQueryRowsWire,
    AgentArchiveQuerySummaryWire, AgentArchiveRowsRequestWire,
    AgentArchiveSummaryRequestWire,
};
use crate::agent_identity::normalize_agent_archive_name;
use crate::query::evaluator::{
    compile_query_with_profile, try_evaluate_query_many_in_corpus,
};
use crate::query::profile::CompiledQueryProfile;

#[derive(Clone, Copy)]
enum GroupBy {
    Day,
    Project,
    Outcome,
    Model,
}

enum PresentationRoot {
    Member { index: usize },
    Container { key: String, members: Vec<usize> },
}

/// Number of matching presentation roots for `request.query`.
pub fn count_agent_archive_corpus(
    corpus: &PreparedAgentArchiveCorpus,
    request: AgentArchiveCountRequestWire,
) -> Result<AgentArchiveQueryCountWire, AgentArchiveQueryError> {
    let matching = matching_indexes(corpus, &request.query, &request.profile)?;
    Ok(AgentArchiveQueryCountWire {
        count: presentation_roots(corpus, &matching).len() as i64,
    })
}

/// Group matching presentation roots, omitting empty groups.
pub fn summarize_agent_archive_corpus(
    corpus: &PreparedAgentArchiveCorpus,
    request: AgentArchiveSummaryRequestWire,
) -> Result<AgentArchiveQuerySummaryWire, AgentArchiveQueryError> {
    let group_by = parse_group_by(&request.group_by)?;
    let matching = matching_indexes(corpus, &request.query, &request.profile)?;
    let roots = presentation_roots(corpus, &matching);
    let mut groups = Vec::new();
    let mut group_index = BTreeMap::new();
    for root in &roots {
        let key = group_key(corpus, root, group_by);
        let activity = root_activity(corpus, root);
        let slot = if let Some(&index) = group_index.get(&key) {
            &mut groups[index]
        } else {
            let index = groups.len();
            group_index.insert(key.clone(), index);
            groups.push(AgentArchiveQueryGroupWire {
                key,
                count: 0,
                last_activity_at: activity.cloned(),
            });
            &mut groups[index]
        };
        slot.count += 1;
        if slot.last_activity_at.is_none() {
            slot.last_activity_at = activity.cloned();
        }
    }
    Ok(AgentArchiveQuerySummaryWire {
        total: roots.len() as i64,
        groups,
        status: corpus.status.clone(),
    })
}

/// One offset/limit window of light rows inside `group_key`.
pub fn rows_agent_archive_corpus(
    corpus: &PreparedAgentArchiveCorpus,
    request: AgentArchiveRowsRequestWire,
) -> Result<AgentArchiveQueryRowsWire, AgentArchiveQueryError> {
    let group_by = parse_group_by(&request.group_by)?;
    let matching = matching_indexes(corpus, &request.query, &request.profile)?;
    let roots = presentation_roots(corpus, &matching);
    let expanded: HashSet<&str> = request
        .expanded_container_keys
        .iter()
        .map(String::as_str)
        .collect();
    let mut flattened = Vec::new();
    for root in &roots {
        if group_key(corpus, root, group_by) != request.group_key {
            continue;
        }
        match root {
            PresentationRoot::Member { index } => {
                flattened.push(member_light_row(&corpus.rows[*index]));
            }
            PresentationRoot::Container { key, members } => {
                flattened.push(container_light_row(corpus, key, members));
                if expanded.contains(key.as_str()) {
                    for &index in members {
                        flattened.push(member_light_row(&corpus.rows[index]));
                    }
                }
            }
        }
    }
    let total = flattened.len() as i64;
    let offset = request.offset.max(0);
    let limit = request.limit.max(0);
    let start = (offset as usize).min(flattened.len());
    let end = start.saturating_add(limit as usize).min(flattened.len());
    Ok(AgentArchiveQueryRowsWire {
        rows: flattened[start..end].to_vec(),
        offset,
        limit,
        total,
    })
}

/// Exact name lookup, including a workflow child that resolves to its owner.
pub fn lookup_agent_archive_corpus(
    corpus: &PreparedAgentArchiveCorpus,
    request: AgentArchiveLookupRequestWire,
) -> AgentArchiveQueryLookupWire {
    let name = request.name.trim();
    if name.is_empty() {
        return AgentArchiveQueryLookupWire { row: None };
    }
    let mut candidates = vec![name.to_string()];
    if let Ok(canonical) = normalize_agent_archive_name(name) {
        if !candidates.iter().any(|item| item == &canonical) {
            candidates.push(canonical);
        }
    }
    let mut best: Option<usize> = None;
    for candidate in &candidates {
        let Some(indexes) = corpus.name_indexes.get(candidate) else {
            continue;
        };
        for &index in indexes {
            best = Some(best.map_or(index, |current| current.min(index)));
        }
    }
    AgentArchiveQueryLookupWire {
        row: best.map(|index| member_light_row(&corpus.rows[index])),
    }
}

fn parse_group_by(value: &str) -> Result<GroupBy, AgentArchiveQueryError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "day" => Ok(GroupBy::Day),
        "project" => Ok(GroupBy::Project),
        "outcome" => Ok(GroupBy::Outcome),
        "model" => Ok(GroupBy::Model),
        _ => Err(AgentArchiveQueryError::UnknownGroupBy(value.to_string())),
    }
}

fn matching_indexes(
    corpus: &PreparedAgentArchiveCorpus,
    query: &str,
    profile: &Value,
) -> Result<Vec<usize>, AgentArchiveQueryError> {
    if query.trim().is_empty() {
        return Ok((0..corpus.rows.len()).collect());
    }
    let compiled_profile = CompiledQueryProfile::from_wire(profile)?;
    let query_corpus = corpus.query_corpus(&compiled_profile);
    let program = compile_query_with_profile(query, &compiled_profile)?;
    let mask =
        try_evaluate_query_many_in_corpus(&program, query_corpus.as_ref())?;
    Ok(mask
        .iter()
        .enumerate()
        .filter_map(|(index, matched)| matched.then_some(index))
        .collect())
}

fn presentation_roots(
    corpus: &PreparedAgentArchiveCorpus,
    matching: &[usize],
) -> Vec<PresentationRoot> {
    let mut members_by_container: BTreeMap<String, Vec<usize>> =
        BTreeMap::new();
    for &index in matching {
        members_by_container
            .entry(corpus.rows[index].container_key.clone())
            .or_default()
            .push(index);
    }
    let mut emitted = HashSet::new();
    let mut roots = Vec::new();
    for &index in matching {
        let key = &corpus.rows[index].container_key;
        let members = members_by_container
            .get(key)
            .expect("matching index grouped by container");
        if members.len() >= 2 {
            if emitted.insert(key.clone()) {
                roots.push(PresentationRoot::Container {
                    key: key.clone(),
                    members: members.clone(),
                });
            }
        } else {
            roots.push(PresentationRoot::Member { index });
        }
    }
    roots
}

fn group_key(
    corpus: &PreparedAgentArchiveCorpus,
    root: &PresentationRoot,
    group_by: GroupBy,
) -> String {
    match group_by {
        GroupBy::Day => {
            corpus.local_day_keys[representative_index(root)].clone()
        }
        GroupBy::Project => representative(corpus, root)
            .project
            .clone()
            .unwrap_or_default(),
        GroupBy::Outcome => outcome_key(root_outcome(corpus, root)).to_string(),
        GroupBy::Model => representative(corpus, root)
            .model
            .clone()
            .unwrap_or_default(),
    }
}

fn representative_index(root: &PresentationRoot) -> usize {
    match root {
        PresentationRoot::Member { index } => *index,
        PresentationRoot::Container { members, .. } => members[0],
    }
}

fn representative<'a>(
    corpus: &'a PreparedAgentArchiveCorpus,
    root: &PresentationRoot,
) -> &'a AgentArchiveCorpusRowWire {
    match root {
        PresentationRoot::Member { index } => &corpus.rows[*index],
        PresentationRoot::Container { members, .. } => &corpus.rows[members[0]],
    }
}

fn root_activity<'a>(
    corpus: &'a PreparedAgentArchiveCorpus,
    root: &PresentationRoot,
) -> Option<&'a String> {
    representative(corpus, root).last_activity_at.as_ref()
}

fn root_outcome(
    corpus: &PreparedAgentArchiveCorpus,
    root: &PresentationRoot,
) -> AgentArchiveOutcomeWire {
    match root {
        PresentationRoot::Member { index } => corpus.rows[*index].outcome,
        PresentationRoot::Container { members, .. } => members
            .iter()
            .map(|index| corpus.rows[*index].outcome)
            .reduce(worse_outcome)
            .unwrap_or(AgentArchiveOutcomeWire::Done),
    }
}

fn member_light_row(
    row: &AgentArchiveCorpusRowWire,
) -> AgentArchiveLightRowWire {
    AgentArchiveLightRowWire {
        archive_key: row.archive_key.clone(),
        container_key: row.container_key.clone(),
        container: false,
        member_count: 1,
        agent_name: row.agent_name.clone(),
        status: row.status.clone(),
        outcome: row.outcome,
        model: row.model.clone(),
        provider: row.provider.clone(),
        project: row.project.clone(),
        last_activity_at: row.last_activity_at.clone(),
        time_basis: row.time_basis,
        start_time: row.start_time.clone(),
        stop_time: row.stop_time.clone(),
        dismissed_at: row.dismissed_at.clone(),
        bundle_path: row.bundle_path.clone(),
        runtime_seconds: row.runtime_seconds,
        restorable: row.restorable,
    }
}

fn container_light_row(
    corpus: &PreparedAgentArchiveCorpus,
    key: &str,
    members: &[usize],
) -> AgentArchiveLightRowWire {
    let newest = &corpus.rows[members[0]];
    let outcome = members
        .iter()
        .map(|index| corpus.rows[*index].outcome)
        .reduce(worse_outcome)
        .unwrap_or(newest.outcome);
    let status = members
        .iter()
        .find(|index| corpus.rows[**index].outcome == outcome)
        .map(|index| corpus.rows[*index].status.clone())
        .unwrap_or_else(|| newest.status.clone());
    let restorable = members.iter().any(|index| corpus.rows[*index].restorable);
    AgentArchiveLightRowWire {
        archive_key: newest.archive_key.clone(),
        container_key: key.to_string(),
        container: true,
        member_count: members.len() as i64,
        agent_name: newest.container_label.clone(),
        status,
        outcome,
        model: newest.model.clone(),
        provider: newest.provider.clone(),
        project: newest.project.clone(),
        last_activity_at: newest.last_activity_at.clone(),
        time_basis: newest.time_basis,
        start_time: newest.start_time.clone(),
        stop_time: newest.stop_time.clone(),
        dismissed_at: newest.dismissed_at.clone(),
        bundle_path: newest.bundle_path.clone(),
        runtime_seconds: newest.runtime_seconds,
        restorable,
    }
}

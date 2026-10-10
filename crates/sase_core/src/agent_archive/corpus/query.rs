use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::agent_archive::corpus::compile::{
    parse_timezone, worse_outcome, ArchiveTimezone,
};
use crate::agent_archive::corpus::wire::{
    AgentArchiveCorpusRowWire, AgentArchiveCorpusWire,
    AgentArchiveCountRequestWire, AgentArchiveLightRowWire,
    AgentArchiveLookupRequestWire, AgentArchiveOutcomeWire,
    AgentArchiveQueryCountWire, AgentArchiveQueryError,
    AgentArchiveQueryGroupWire, AgentArchiveQueryLookupWire,
    AgentArchiveQueryRowsWire, AgentArchiveQuerySummaryWire,
    AgentArchiveRowsRequestWire, AgentArchiveSummaryRequestWire,
};
use crate::agent_identity::normalize_agent_archive_name;
use crate::query::evaluator::{
    compile_query_with_profile, try_evaluate_query_many_in_corpus, QueryCorpus,
};
use crate::query::profile::CompiledQueryProfile;
use crate::query::row::{QueryFieldValues, QueryPredicateFacts, QueryRow};

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
    corpus: &AgentArchiveCorpusWire,
    request: AgentArchiveCountRequestWire,
) -> Result<AgentArchiveQueryCountWire, AgentArchiveQueryError> {
    let matching = matching_indexes(corpus, &request.query, &request.profile)?;
    Ok(AgentArchiveQueryCountWire {
        count: presentation_roots(corpus, &matching).len() as i64,
    })
}

/// Group matching presentation roots, omitting empty groups.
pub fn summarize_agent_archive_corpus(
    corpus: &AgentArchiveCorpusWire,
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
    corpus: &AgentArchiveCorpusWire,
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
    corpus: &AgentArchiveCorpusWire,
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
    let mut owners = Vec::new();
    for candidate in &candidates {
        let Some(matches) = corpus.name_map.get(candidate) else {
            continue;
        };
        for item in matches {
            if !owners.iter().any(|key| key == &item.owner_key) {
                owners.push(item.owner_key.clone());
            }
        }
    }
    for row in &corpus.rows {
        if owners.iter().any(|key| key == &row.archive_key) {
            return AgentArchiveQueryLookupWire {
                row: Some(member_light_row(row)),
            };
        }
    }
    AgentArchiveQueryLookupWire { row: None }
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
    corpus: &AgentArchiveCorpusWire,
    query: &str,
    profile: &Value,
) -> Result<Vec<usize>, AgentArchiveQueryError> {
    if query.trim().is_empty() {
        return Ok((0..corpus.rows.len()).collect());
    }
    let compiled_profile = CompiledQueryProfile::from_wire(profile)?;
    let rows = corpus.rows.iter().map(query_row_from_corpus_row).collect();
    let query_corpus = QueryCorpus::from_rows(&compiled_profile, rows);
    let program = compile_query_with_profile(query, &compiled_profile)?;
    let mask = try_evaluate_query_many_in_corpus(&program, &query_corpus)?;
    Ok(mask
        .iter()
        .enumerate()
        .filter_map(|(index, matched)| matched.then_some(index))
        .collect())
}

fn presentation_roots(
    corpus: &AgentArchiveCorpusWire,
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
    corpus: &AgentArchiveCorpusWire,
    root: &PresentationRoot,
    group_by: GroupBy,
) -> String {
    match group_by {
        GroupBy::Day => {
            local_day_key(root_activity(corpus, root), &corpus.timezone)
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

fn representative<'a>(
    corpus: &'a AgentArchiveCorpusWire,
    root: &PresentationRoot,
) -> &'a AgentArchiveCorpusRowWire {
    match root {
        PresentationRoot::Member { index } => &corpus.rows[*index],
        PresentationRoot::Container { members, .. } => &corpus.rows[members[0]],
    }
}

fn root_activity<'a>(
    corpus: &'a AgentArchiveCorpusWire,
    root: &PresentationRoot,
) -> Option<&'a String> {
    representative(corpus, root).last_activity_at.as_ref()
}

fn root_outcome(
    corpus: &AgentArchiveCorpusWire,
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
        runtime_seconds: row.runtime_seconds,
        restorable: row.restorable,
    }
}

fn container_light_row(
    corpus: &AgentArchiveCorpusWire,
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
        runtime_seconds: newest.runtime_seconds,
        restorable,
    }
}

fn query_row_from_corpus_row(row: &AgentArchiveCorpusRowWire) -> QueryRow {
    let mut fields = BTreeMap::new();
    let mut names = Vec::new();
    push_unique(&mut names, row.agent_name.as_deref());
    push_unique(&mut names, row.canonical_name.as_deref());
    push_unique(&mut names, row.global_name.as_deref());
    fields.insert("name".into(), QueryFieldValues::from_strings(names.clone()));
    insert_opt(&mut fields, "session", row.session.as_deref());
    insert_opt(&mut fields, "clan", row.clan.as_deref());
    insert_opt(&mut fields, "project", row.project.as_deref());
    insert_opt(&mut fields, "role", row.role.as_deref());
    insert_opt(&mut fields, "workflow", row.workflow.as_deref());
    insert_opt(&mut fields, "model", row.model.as_deref());
    insert_opt(&mut fields, "provider", row.provider.as_deref());
    fields.insert(
        "kind".into(),
        QueryFieldValues::from_string(row.kind.clone()),
    );
    fields.insert(
        "status".into(),
        QueryFieldValues::from_string(row.status.clone()),
    );
    fields.insert(
        "outcome".into(),
        QueryFieldValues::from_string(outcome_key(row.outcome)),
    );
    insert_opt(&mut fields, "tab", row.tab.as_deref());
    insert_opt(&mut fields, "tribe", row.tribe.as_deref());
    insert_opt(&mut fields, "clan_tribe", row.clan_tribe.as_deref());
    fields.insert(
        "restorable".into(),
        QueryFieldValues::from_string(bool_text(row.restorable)),
    );
    fields.insert(
        "linked".into(),
        QueryFieldValues::from_string(bool_text(row.linked)),
    );
    fields.insert(
        "retry".into(),
        QueryFieldValues::from_string(bool_text(row.retry)),
    );
    fields.insert(
        "relation".into(),
        QueryFieldValues::from_strings(row.relations.clone()),
    );
    fields.insert(
        "artifact".into(),
        QueryFieldValues::from_strings(row.artifacts.clone()),
    );
    fields.insert(
        "attempt".into(),
        QueryFieldValues::from_string(row.attempt.to_string()),
    );
    if let Some(epoch) = epoch_seconds(row.start_time.as_deref()) {
        fields.insert(
            "since".into(),
            QueryFieldValues::from_string(epoch.clone()),
        );
        fields.insert("until".into(), QueryFieldValues::from_string(epoch));
    }
    if let Some(epoch) = epoch_seconds(row.last_activity_at.as_deref()) {
        fields.insert(
            "after".into(),
            QueryFieldValues::from_string(epoch.clone()),
        );
        fields.insert("before".into(), QueryFieldValues::from_string(epoch));
    }
    if let Some(runtime) = row.runtime_seconds {
        let text = runtime.to_string();
        fields
            .insert("min".into(), QueryFieldValues::from_string(text.clone()));
        fields.insert("max".into(), QueryFieldValues::from_string(text));
    }
    QueryRow {
        fields,
        searchable_text: names.join("\n"),
        predicates: QueryPredicateFacts::default(),
    }
}

fn insert_opt(
    fields: &mut BTreeMap<String, QueryFieldValues>,
    key: &str,
    value: Option<&str>,
) {
    if let Some(value) = value {
        fields.insert(key.to_string(), QueryFieldValues::from_string(value));
    }
}

fn push_unique(values: &mut Vec<String>, value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    if !values.iter().any(|item| item == value) {
        values.push(value.to_string());
    }
}

fn bool_text(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn outcome_key(outcome: AgentArchiveOutcomeWire) -> &'static str {
    match outcome {
        AgentArchiveOutcomeWire::Done => "done",
        AgentArchiveOutcomeWire::Failed => "failed",
        AgentArchiveOutcomeWire::Interrupted => "interrupted",
    }
}

fn epoch_seconds(value: Option<&str>) -> Option<String> {
    DateTime::parse_from_rfc3339(value?)
        .ok()
        .map(|timestamp| timestamp.timestamp().to_string())
}

fn local_day_key(last_activity_at: Option<&String>, timezone: &str) -> String {
    let Some(raw) = last_activity_at else {
        return String::new();
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(raw) else {
        return String::new();
    };
    let utc = parsed.with_timezone(&Utc);
    match parse_timezone(timezone) {
        Some(ArchiveTimezone::Iana(timezone)) => {
            utc.with_timezone(&timezone).format("%Y-%m-%d").to_string()
        }
        Some(ArchiveTimezone::Fixed(offset)) => {
            utc.with_timezone(&offset).format("%Y-%m-%d").to_string()
        }
        None => utc.format("%Y-%m-%d").to_string(),
    }
}

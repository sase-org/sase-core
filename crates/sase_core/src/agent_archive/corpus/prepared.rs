use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use crate::agent_archive::corpus::compile::{parse_timezone, ArchiveTimezone};
use crate::agent_archive::corpus::wire::{
    AgentArchiveContainerWire, AgentArchiveCorpusRowWire,
    AgentArchiveCorpusStatusWire, AgentArchiveNameMatchWire,
    AgentArchiveOutcomeWire,
};
use crate::agent_archive::AgentArchiveKeyWire;
use crate::query::evaluator::QueryCorpus;
use crate::query::profile::{CompiledQueryProfile, FieldValueKind};
use crate::query::row::{IndexedField, IndexedQueryRow, QueryPredicateFacts};

/// Query-ready archive corpus that stays in Rust after compile.
///
/// One handle is shared across threads, so the type is `Send + Sync`. Query
/// corpora are built lazily per profile digest on first use.
pub struct PreparedAgentArchiveCorpus {
    pub status: AgentArchiveCorpusStatusWire,
    pub timezone: String,
    pub rows: Vec<AgentArchiveCorpusRowWire>,
    pub containers: Vec<AgentArchiveContainerWire>,
    pub name_map: BTreeMap<String, Vec<AgentArchiveNameMatchWire>>,
    pub(super) indexed_rows: Arc<Vec<IndexedQueryRow>>,
    pub(super) local_day_keys: Vec<String>,
    pub(super) name_indexes: BTreeMap<String, Vec<usize>>,
    query_corpora: Mutex<HashMap<String, Arc<QueryCorpus>>>,
}

impl PreparedAgentArchiveCorpus {
    pub(super) fn new(
        status: AgentArchiveCorpusStatusWire,
        timezone: String,
        rows: Vec<AgentArchiveCorpusRowWire>,
        containers: Vec<AgentArchiveContainerWire>,
        name_map: BTreeMap<String, Vec<AgentArchiveNameMatchWire>>,
    ) -> Self {
        let timezone_parsed = parse_timezone(&timezone);
        let indexed_rows = Arc::new(index_corpus_rows(rows.as_slice()));
        let local_day_keys: Vec<String> = rows
            .iter()
            .map(|row| {
                local_day_key(row.last_activity_at.as_deref(), timezone_parsed)
            })
            .collect();
        let mut key_to_index = BTreeMap::new();
        for (index, row) in rows.iter().enumerate() {
            key_to_index
                .entry(archive_key_tuple(&row.archive_key))
                .or_insert(index);
        }
        let mut name_indexes = BTreeMap::new();
        for (name, matches) in &name_map {
            let mut indexes = Vec::new();
            for item in matches {
                let Some(&index) =
                    key_to_index.get(&archive_key_tuple(&item.owner_key))
                else {
                    continue;
                };
                if !indexes.contains(&index) {
                    indexes.push(index);
                }
            }
            if !indexes.is_empty() {
                name_indexes.insert(name.clone(), indexes);
            }
        }
        Self {
            status,
            timezone,
            rows,
            containers,
            name_map,
            indexed_rows,
            local_day_keys,
            name_indexes,
            query_corpora: Mutex::new(HashMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub(super) fn query_corpus(
        &self,
        profile: &CompiledQueryProfile,
    ) -> Arc<QueryCorpus> {
        {
            let cache = self
                .query_corpora
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(existing) = cache.get(&profile.digest) {
                return Arc::clone(existing);
            }
        }
        let built = Arc::new(QueryCorpus::from_indexed_rows(
            profile,
            Arc::clone(&self.indexed_rows),
        ));
        let mut cache = self
            .query_corpora
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        Arc::clone(
            cache
                .entry(profile.digest.clone())
                .or_insert_with(|| Arc::clone(&built)),
        )
    }
}

fn index_corpus_rows(
    rows: &[AgentArchiveCorpusRowWire],
) -> Vec<IndexedQueryRow> {
    const MIN_PARALLEL_ROWS: usize = 2_048;
    let threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 8);
    if rows.len() < MIN_PARALLEL_ROWS || threads == 1 {
        return rows.iter().map(indexed_row_from_corpus_row).collect();
    }
    let chunk_size = rows.len().div_ceil(threads);
    std::thread::scope(|scope| {
        let handles: Vec<_> = rows
            .chunks(chunk_size)
            .map(|chunk| {
                scope.spawn(move || {
                    chunk
                        .iter()
                        .map(indexed_row_from_corpus_row)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut indexed = Vec::with_capacity(rows.len());
        for handle in handles {
            indexed.extend(handle.join().expect("archive row index worker"));
        }
        indexed
    })
}

fn archive_key_tuple(key: &AgentArchiveKeyWire) -> (String, String, String) {
    (
        key.source_username.clone(),
        key.source_machine.clone(),
        key.source_run_id.clone(),
    )
}

fn indexed_row_from_corpus_row(
    row: &AgentArchiveCorpusRowWire,
) -> IndexedQueryRow {
    let mut fields = HashMap::with_capacity(24);
    let mut names = Vec::new();
    push_unique(&mut names, row.agent_name.as_deref());
    push_unique(&mut names, row.canonical_name.as_deref());
    push_unique(&mut names, row.global_name.as_deref());
    insert_indexed(
        &mut fields,
        "name",
        names.clone(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "session",
        row.session.as_deref(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "clan",
        row.clan.as_deref(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "project",
        row.project.as_deref(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "role",
        row.role.as_deref(),
        FieldValueKind::String,
        false,
    );
    insert_opt(
        &mut fields,
        "workflow",
        row.workflow.as_deref(),
        FieldValueKind::String,
        false,
    );
    insert_opt(
        &mut fields,
        "model",
        row.model.as_deref(),
        FieldValueKind::String,
        false,
    );
    insert_opt(
        &mut fields,
        "provider",
        row.provider.as_deref(),
        FieldValueKind::Enum,
        false,
    );
    insert_indexed(
        &mut fields,
        "kind",
        vec![row.kind.clone()],
        FieldValueKind::Enum,
        false,
    );
    insert_indexed(
        &mut fields,
        "status",
        vec![row.status.clone()],
        FieldValueKind::Enum,
        false,
    );
    insert_indexed(
        &mut fields,
        "outcome",
        vec![outcome_key(row.outcome).to_string()],
        FieldValueKind::Enum,
        false,
    );
    insert_opt(
        &mut fields,
        "tab",
        row.tab.as_deref(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "tribe",
        row.tribe.as_deref(),
        FieldValueKind::String,
        true,
    );
    insert_opt(
        &mut fields,
        "clan_tribe",
        row.clan_tribe.as_deref(),
        FieldValueKind::Enum,
        false,
    );
    insert_indexed(
        &mut fields,
        "restorable",
        vec![bool_text(row.restorable).to_string()],
        FieldValueKind::Bool,
        false,
    );
    insert_indexed(
        &mut fields,
        "linked",
        vec![bool_text(row.linked).to_string()],
        FieldValueKind::Bool,
        false,
    );
    insert_indexed(
        &mut fields,
        "retry",
        vec![bool_text(row.retry).to_string()],
        FieldValueKind::Bool,
        false,
    );
    insert_indexed(
        &mut fields,
        "relation",
        row.relations.clone(),
        FieldValueKind::Enum,
        false,
    );
    insert_indexed(
        &mut fields,
        "artifact",
        row.artifacts.clone(),
        FieldValueKind::String,
        true,
    );
    insert_indexed(
        &mut fields,
        "attempt",
        vec![row.attempt.to_string()],
        FieldValueKind::Int,
        false,
    );
    if let Some(epoch) = epoch_seconds(row.start_time.as_deref()) {
        insert_indexed(
            &mut fields,
            "since",
            vec![epoch.clone()],
            FieldValueKind::Date,
            false,
        );
        insert_indexed(
            &mut fields,
            "until",
            vec![epoch],
            FieldValueKind::Date,
            false,
        );
    }
    if let Some(epoch) = epoch_seconds(row.last_activity_at.as_deref()) {
        insert_indexed(
            &mut fields,
            "after",
            vec![epoch.clone()],
            FieldValueKind::Date,
            false,
        );
        insert_indexed(
            &mut fields,
            "before",
            vec![epoch],
            FieldValueKind::Date,
            false,
        );
    }
    if let Some(runtime) = row.runtime_seconds {
        let text = runtime.to_string();
        insert_indexed(
            &mut fields,
            "min",
            vec![text.clone()],
            FieldValueKind::Int,
            false,
        );
        insert_indexed(
            &mut fields,
            "max",
            vec![text],
            FieldValueKind::Int,
            false,
        );
    }
    let searchable_text = names.join("\n");
    let searchable_lower = searchable_text.to_lowercase();
    IndexedQueryRow {
        fields,
        searchable_text,
        searchable_lower,
        predicates: QueryPredicateFacts::default(),
    }
}

fn insert_indexed(
    fields: &mut HashMap<String, IndexedField>,
    key: &str,
    values: Vec<String>,
    kind: FieldValueKind,
    exact_match: bool,
) {
    let values: Vec<String> = values
        .into_iter()
        .filter(|value| !value.is_empty())
        .collect();
    let values_lower = match kind {
        FieldValueKind::String | FieldValueKind::Enum => values
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect(),
        FieldValueKind::Bool | FieldValueKind::Int | FieldValueKind::Date => {
            Vec::new()
        }
    };
    fields.insert(
        key.to_string(),
        IndexedField {
            kind,
            exact_match,
            values,
            values_lower,
        },
    );
}

fn insert_opt(
    fields: &mut HashMap<String, IndexedField>,
    key: &str,
    value: Option<&str>,
    kind: FieldValueKind,
    exact_match: bool,
) {
    if let Some(value) = value {
        insert_indexed(fields, key, vec![value.to_string()], kind, exact_match);
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

pub(super) fn outcome_key(outcome: AgentArchiveOutcomeWire) -> &'static str {
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

fn local_day_key(
    last_activity_at: Option<&str>,
    timezone: Option<ArchiveTimezone>,
) -> String {
    let Some(raw) = last_activity_at else {
        return String::new();
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(raw) else {
        return String::new();
    };
    let utc = parsed.with_timezone(&Utc);
    match timezone {
        Some(ArchiveTimezone::Iana(timezone)) => {
            utc.with_timezone(&timezone).format("%Y-%m-%d").to_string()
        }
        Some(ArchiveTimezone::Fixed(offset)) => {
            utc.with_timezone(&offset).format("%Y-%m-%d").to_string()
        }
        None => utc.format("%Y-%m-%d").to_string(),
    }
}

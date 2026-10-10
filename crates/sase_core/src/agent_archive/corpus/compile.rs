use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::{
    DateTime, Duration, FixedOffset, LocalResult, NaiveDateTime, Offset,
    SecondsFormat, TimeZone, Utc,
};
use chrono_tz::Tz;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row};
use thiserror::Error;

use crate::agent_archive::archive_outcome_for_status;
use crate::agent_archive::corpus::wire::{
    AgentArchiveCompileRequestWire, AgentArchiveContainerWire,
    AgentArchiveCorpusRowWire, AgentArchiveCorpusStatusWire,
    AgentArchiveCorpusWire, AgentArchiveIndexProbeStatusWire,
    AgentArchiveLinkFacetsWire, AgentArchiveNameMatchWire,
    AgentArchiveOutcomeWire, AgentArchiveTimeBasisWire,
    AgentArchiveUnsupportedIndexWire,
};
use crate::agent_archive::AgentArchiveKeyWire;
use crate::agent_identity::{
    globalize_agent_name, normalize_agent_archive_name, AgentOwnerIdentity,
};

const INDEX_FILENAME: &str = "index.sqlite";
const INDEX_SCHEMA_VERSION: u32 = 3;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentArchiveCompileError {
    #[error("could not open the archive index: {0}")]
    OpenIndex(String),
    #[error("could not read the archive index: {0}")]
    ReadIndex(String),
    #[error(
        "invalid archive timezone {0:?}; expected an IANA name or UTC offset"
    )]
    InvalidTimezone(String),
}

#[derive(Clone, Copy)]
enum ArchiveTimezone {
    Iana(Tz),
    Fixed(FixedOffset),
}

struct CompiledRow {
    row: AgentArchiveCorpusRowWire,
    bundle_path: String,
    activity: Option<DateTime<Utc>>,
    is_workflow_child: bool,
}

/// Compile the current v3 index into immutable rows and derived lookup data.
///
/// This function only opens an existing index in read-only mode. It never
/// creates or rebuilds the caller's index.
pub fn compile_agent_archive_corpus(
    request: AgentArchiveCompileRequestWire,
) -> Result<AgentArchiveCorpusWire, AgentArchiveCompileError> {
    if matches!(
        request.index_status,
        AgentArchiveIndexProbeStatusWire::Missing
    ) {
        return Ok(empty_corpus(AgentArchiveCorpusStatusWire::Missing));
    }

    let index_path = Path::new(&request.root).join(INDEX_FILENAME);
    if !index_path.is_file() {
        return Ok(empty_corpus(AgentArchiveCorpusStatusWire::Missing));
    }

    let conn = Connection::open_with_flags(
        &index_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| AgentArchiveCompileError::OpenIndex(error.to_string()))?;
    conn.pragma_update(None, "busy_timeout", 30_000)
        .map_err(|error| {
            AgentArchiveCompileError::ReadIndex(error.to_string())
        })?;

    let meta_table_exists = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            ["dismissed_bundle_index_meta"],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| {
            AgentArchiveCompileError::ReadIndex(error.to_string())
        })?
        .is_some();
    let actual_schema_version = if meta_table_exists {
        conn.query_row(
            "SELECT value FROM dismissed_bundle_index_meta WHERE key = 'schema_version'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| AgentArchiveCompileError::ReadIndex(error.to_string()))?
    } else {
        None
    };
    if actual_schema_version
        .as_deref()
        .and_then(|value| value.parse::<u32>().ok())
        != Some(INDEX_SCHEMA_VERSION)
    {
        return Ok(empty_corpus(AgentArchiveCorpusStatusWire::Unsupported {
            error: AgentArchiveUnsupportedIndexWire {
                expected_schema_version: INDEX_SCHEMA_VERSION,
                actual_schema_version,
            },
        }));
    }

    let timezone = parse_timezone(&request.timezone).ok_or_else(|| {
        AgentArchiveCompileError::InvalidTimezone(request.timezone.clone())
    })?;
    let status = match request.index_status {
        AgentArchiveIndexProbeStatusWire::Missing => {
            AgentArchiveCorpusStatusWire::Missing
        }
        AgentArchiveIndexProbeStatusWire::Rebuilding { indexed_rows } => {
            AgentArchiveCorpusStatusWire::Rebuilding {
                indexed_rows: indexed_rows.max(0),
            }
        }
        AgentArchiveIndexProbeStatusWire::Ok => {
            AgentArchiveCorpusStatusWire::Ok
        }
    };

    let mut stmt = conn
        .prepare(
            "SELECT s.bundle_path, s.source_username, s.source_machine, \
                    s.source_run_id, COALESCE(v.visibility, s.archive_visibility) \
                        AS effective_visibility, s.agent_name, s.agent_session, \
                    s.agent_clan, s.agent_clan_generation, s.project_name, \
                    s.agent_session_role, s.workflow, s.agent_type, s.status, \
                    s.start_time, s.stop_time, \
                    COALESCE(v.dismissed_at, s.dismissed_at) AS effective_dismissed_at, \
                    s.model, s.llm_provider, s.agent_tab, s.tribe, s.clan_tribe, \
                    s.runtime, s.retry_attempt, s.retry_of_timestamp, \
                    s.durably_revivable, s.is_workflow_child \
             FROM dismissed_bundle_summaries s \
             LEFT JOIN archive_visibility_projection v \
               ON v.source_username = s.source_username \
              AND v.source_machine = s.source_machine \
              AND v.source_run_id = s.source_run_id \
             WHERE COALESCE(v.visibility, s.archive_visibility) = 'hidden' \
             ORDER BY s.filename ASC, s.bundle_path ASC",
        )
        .map_err(|error| AgentArchiveCompileError::ReadIndex(error.to_string()))?;
    let mut rows = stmt.query([]).map_err(|error| {
        AgentArchiveCompileError::ReadIndex(error.to_string())
    })?;

    let mut compiled_rows = Vec::new();
    let mut name_map: BTreeMap<String, Vec<AgentArchiveNameMatchWire>> =
        BTreeMap::new();
    while let Some(row) = rows.next().map_err(|error| {
        AgentArchiveCompileError::ReadIndex(error.to_string())
    })? {
        let Some(compiled) =
            compile_row(row, timezone, &request.link_facets, &mut name_map)
                .map_err(|error| {
                    AgentArchiveCompileError::ReadIndex(error.to_string())
                })?
        else {
            continue;
        };
        if !compiled.is_workflow_child {
            compiled_rows.push(compiled);
        }
    }

    compiled_rows.sort_by(|left, right| {
        right
            .activity
            .cmp(&left.activity)
            .then_with(|| {
                archive_key_sort_key(&left.row.archive_key)
                    .cmp(&archive_key_sort_key(&right.row.archive_key))
            })
            .then_with(|| left.bundle_path.cmp(&right.bundle_path))
    });

    let rows: Vec<_> =
        compiled_rows.iter().map(|item| item.row.clone()).collect();
    let containers = compile_containers(&compiled_rows);
    Ok(AgentArchiveCorpusWire {
        status,
        rows,
        containers,
        name_map,
    })
}

fn empty_corpus(
    status: AgentArchiveCorpusStatusWire,
) -> AgentArchiveCorpusWire {
    AgentArchiveCorpusWire {
        status,
        rows: Vec::new(),
        containers: Vec::new(),
        name_map: BTreeMap::new(),
    }
}

fn compile_row(
    row: &Row<'_>,
    timezone: ArchiveTimezone,
    link_facets: &BTreeMap<String, AgentArchiveLinkFacetsWire>,
    name_map: &mut BTreeMap<String, Vec<AgentArchiveNameMatchWire>>,
) -> rusqlite::Result<Option<CompiledRow>> {
    let bundle_path: String = row.get("bundle_path")?;
    let source_username = optional_nonempty(row.get("source_username")?);
    let source_machine = optional_nonempty(row.get("source_machine")?);
    let source_run_id = optional_nonempty(row.get("source_run_id")?);
    let (Some(source_username), Some(source_machine), Some(source_run_id)) =
        (source_username, source_machine, source_run_id)
    else {
        return Ok(None);
    };
    let archive_key = AgentArchiveKeyWire {
        source_username,
        source_machine,
        source_run_id,
    };
    let Ok(archive_key) =
        crate::agent_archive::validate_agent_archive_key(archive_key)
    else {
        return Ok(None);
    };

    let agent_name = optional_nonempty(row.get("agent_name")?);
    let session = optional_nonempty(row.get("agent_session")?);
    let clan = optional_nonempty(row.get("agent_clan")?);
    let clan_generation = optional_nonempty(row.get("agent_clan_generation")?);
    let project = optional_nonempty(row.get("project_name")?);
    let role = optional_nonempty(row.get("agent_session_role")?);
    let workflow = optional_nonempty(row.get("workflow")?);
    let kind: String = row.get("agent_type")?;
    let status: String = row.get("status")?;
    let start_time_raw: Option<String> = row.get("start_time")?;
    let stop_time_raw: Option<String> = row.get("stop_time")?;
    let dismissed_at_raw: Option<String> = row.get("effective_dismissed_at")?;
    let model = optional_nonempty(row.get("model")?);
    let provider = optional_nonempty(row.get("llm_provider")?);
    let tab = optional_nonempty(row.get("agent_tab")?);
    let tribe = optional_nonempty(row.get("tribe")?);
    let clan_tribe = optional_nonempty(row.get("clan_tribe")?);
    let runtime: Option<String> = row.get("runtime")?;
    let attempt: i64 = row.get("retry_attempt")?;
    let retry_of_timestamp: Option<String> = row.get("retry_of_timestamp")?;
    let durably_revivable: i64 = row.get("durably_revivable")?;
    let is_workflow_child: i64 = row.get("is_workflow_child")?;

    let start_time = start_time_raw
        .as_deref()
        .and_then(|value| parse_archive_timestamp(value, timezone));
    let stop_time = stop_time_raw
        .as_deref()
        .and_then(|value| parse_archive_timestamp(value, timezone));
    let dismissed_at = dismissed_at_raw
        .as_deref()
        .and_then(|value| parse_archive_timestamp(value, timezone));
    let (activity, time_basis) = if let Some(value) = stop_time {
        (Some(value), Some(AgentArchiveTimeBasisWire::Ended))
    } else if let Some(value) = dismissed_at {
        (Some(value), Some(AgentArchiveTimeBasisWire::Dismissed))
    } else if let Some(value) = start_time {
        (Some(value), Some(AgentArchiveTimeBasisWire::Started))
    } else {
        (None, None)
    };
    let runtime_seconds = runtime
        .as_deref()
        .and_then(parse_runtime_seconds)
        .or_else(|| Some((stop_time? - start_time?).num_seconds().max(0)));

    let name_parts = derive_names(agent_name.as_deref(), &archive_key);
    add_name_aliases(
        name_map,
        name_parts.as_ref(),
        &archive_key,
        is_workflow_child != 0,
    );
    let facets = find_link_facets(
        link_facets,
        agent_name.as_deref(),
        name_parts.as_ref(),
    );
    let (container_key, container_label) = derive_container(
        &archive_key,
        session.as_deref(),
        clan.as_deref(),
        clan_generation.as_deref(),
    );

    let row = AgentArchiveCorpusRowWire {
        archive_key,
        container_key,
        container_label,
        agent_name,
        canonical_name: name_parts
            .as_ref()
            .and_then(|parts| parts.canonical_name.clone()),
        global_name: name_parts
            .as_ref()
            .and_then(|parts| parts.global_name.clone()),
        session,
        clan,
        clan_generation,
        project,
        role,
        workflow,
        kind,
        status: status.clone(),
        outcome: outcome_wire(archive_outcome_for_status(&status)),
        model,
        provider,
        tab,
        tribe,
        clan_tribe,
        last_activity_at: activity.map(format_timestamp),
        time_basis,
        start_time: start_time.map(format_timestamp),
        runtime_seconds,
        attempt,
        retry: attempt > 0 || retry_of_timestamp.is_some(),
        restorable: durably_revivable != 0
            && PathBuf::from(&bundle_path).exists(),
        linked: facets.count > 0,
        relations: facets.relations,
        artifacts: facets.artifacts,
    };
    Ok(Some(CompiledRow {
        row,
        bundle_path,
        activity,
        is_workflow_child: is_workflow_child != 0,
    }))
}

#[derive(Default)]
struct DerivedNames {
    original_name: Option<String>,
    canonical_name: Option<String>,
    global_name: Option<String>,
}

fn derive_names(
    agent_name: Option<&str>,
    key: &AgentArchiveKeyWire,
) -> Option<DerivedNames> {
    let agent_name = agent_name?.trim();
    if agent_name.is_empty() {
        return None;
    }
    let canonical_name = normalize_agent_archive_name(agent_name).ok()?;
    let owner =
        AgentOwnerIdentity::new(&key.source_username, &key.source_machine)
            .ok()?;
    let global_name = globalize_agent_name(&canonical_name, &owner).ok()?;
    Some(DerivedNames {
        original_name: Some(agent_name.to_string()),
        canonical_name: Some(canonical_name),
        global_name: Some(global_name),
    })
}

fn add_name_aliases(
    name_map: &mut BTreeMap<String, Vec<AgentArchiveNameMatchWire>>,
    names: Option<&DerivedNames>,
    owner_key: &AgentArchiveKeyWire,
    is_workflow_child: bool,
) {
    let Some(names) = names else {
        return;
    };
    let mut aliases = Vec::new();
    if let Some(name) = names.original_name.as_ref() {
        aliases.push(name.clone());
    }
    if let Some(name) = names.canonical_name.as_ref() {
        aliases.push(name.clone());
    }
    if let Some(name) = names.global_name.as_ref() {
        aliases.push(name.clone());
    }
    aliases.sort();
    aliases.dedup();
    for alias in aliases {
        let matches = name_map.entry(alias).or_default();
        let target = AgentArchiveNameMatchWire {
            owner_key: owner_key.clone(),
            is_workflow_child,
        };
        if !matches.contains(&target) {
            matches.push(target);
        }
    }
}

fn find_link_facets(
    link_facets: &BTreeMap<String, AgentArchiveLinkFacetsWire>,
    agent_name: Option<&str>,
    names: Option<&DerivedNames>,
) -> AgentArchiveLinkFacetsWire {
    agent_name
        .and_then(|name| link_facets.get(name.trim()))
        .or_else(|| {
            names
                .and_then(|names| names.canonical_name.as_ref())
                .and_then(|name| link_facets.get(name))
        })
        .or_else(|| {
            names
                .and_then(|names| names.global_name.as_ref())
                .and_then(|name| link_facets.get(name))
        })
        .cloned()
        .unwrap_or_default()
}

fn derive_container(
    key: &AgentArchiveKeyWire,
    session: Option<&str>,
    clan: Option<&str>,
    clan_generation: Option<&str>,
) -> (String, Option<String>) {
    if let (Some(clan), Some(generation)) = (clan, clan_generation) {
        return (format!("clan:{clan}:{generation}"), Some(clan.to_string()));
    }
    if let Some(session) = session {
        return (format!("session:{session}"), Some(session.to_string()));
    }
    (archive_container_key(key), None)
}

fn archive_container_key(key: &AgentArchiveKeyWire) -> String {
    format!(
        "archive:{}.{}@{}",
        key.source_username, key.source_machine, key.source_run_id
    )
}

fn compile_containers(rows: &[CompiledRow]) -> Vec<AgentArchiveContainerWire> {
    let mut members: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        members
            .entry(row.row.container_key.clone())
            .or_default()
            .push(index);
    }
    members
        .into_iter()
        .map(|(key, indexes)| {
            let member_rows: Vec<_> =
                indexes.iter().map(|index| &rows[*index]).collect();
            let last_activity = member_rows
                .iter()
                .filter_map(|item| item.activity.map(|time| (time, item)))
                .max_by_key(|(time, _)| *time)
                .map(|(_, item)| item.row.last_activity_at.clone());
            let outcome = member_rows
                .iter()
                .fold(AgentArchiveOutcomeWire::Done, |worst, item| {
                    worse_outcome(worst, item.row.outcome)
                });
            AgentArchiveContainerWire {
                key,
                label: member_rows
                    .iter()
                    .find_map(|item| item.row.container_label.clone()),
                member_keys: member_rows
                    .iter()
                    .map(|item| item.row.archive_key.clone())
                    .collect(),
                member_count: member_rows.len() as i64,
                last_activity_at: last_activity.flatten(),
                outcome,
            }
        })
        .collect()
}

fn worse_outcome(
    left: AgentArchiveOutcomeWire,
    right: AgentArchiveOutcomeWire,
) -> AgentArchiveOutcomeWire {
    fn priority(outcome: AgentArchiveOutcomeWire) -> u8 {
        match outcome {
            AgentArchiveOutcomeWire::Done => 1,
            AgentArchiveOutcomeWire::Interrupted => 2,
            AgentArchiveOutcomeWire::Failed => 3,
        }
    }
    if priority(left) >= priority(right) {
        left
    } else {
        right
    }
}

fn outcome_wire(outcome: &str) -> AgentArchiveOutcomeWire {
    match outcome {
        "done" => AgentArchiveOutcomeWire::Done,
        "failed" => AgentArchiveOutcomeWire::Failed,
        _ => AgentArchiveOutcomeWire::Interrupted,
    }
}

fn parse_timezone(value: &str) -> Option<ArchiveTimezone> {
    let value = value.trim();
    if let Ok(timezone) = value.parse::<Tz>() {
        return Some(ArchiveTimezone::Iana(timezone));
    }
    parse_fixed_timezone(value).map(ArchiveTimezone::Fixed)
}

fn parse_fixed_timezone(value: &str) -> Option<FixedOffset> {
    if matches!(value, "UTC" | "Z" | "GMT") {
        return FixedOffset::east_opt(0);
    }
    let offset = value.strip_prefix("UTC").unwrap_or(value);
    let (sign, digits) = match offset.as_bytes().first()? {
        b'+' => (1, &offset[1..]),
        b'-' => (-1, &offset[1..]),
        _ => return None,
    };
    let digits = digits.replace(':', "");
    if digits.len() != 4 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let hours = digits[0..2].parse::<i32>().ok()?;
    let minutes = digits[2..4].parse::<i32>().ok()?;
    if minutes > 59 {
        return None;
    }
    FixedOffset::east_opt(sign * (hours * 3_600 + minutes * 60))
}

fn parse_archive_timestamp(
    value: &str,
    timezone: ArchiveTimezone,
) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Some(timestamp.with_timezone(&Utc));
    }
    let naive = parse_naive_timestamp(value)?;
    match timezone {
        ArchiveTimezone::Iana(timezone) => parse_iana_local(naive, timezone),
        ArchiveTimezone::Fixed(offset) => offset
            .from_local_datetime(&naive)
            .single()
            .map(|timestamp| timestamp.with_timezone(&Utc)),
    }
}

fn parse_naive_timestamp(value: &str) -> Option<NaiveDateTime> {
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
    ] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Some(timestamp);
        }
    }
    NaiveDateTime::parse_from_str(
        &format!("{value}T00:00:00"),
        "%Y-%m-%dT%H:%M:%S",
    )
    .ok()
}

fn parse_iana_local(
    value: NaiveDateTime,
    timezone: Tz,
) -> Option<DateTime<Utc>> {
    match timezone.from_local_datetime(&value) {
        LocalResult::Single(timestamp) => Some(timestamp.with_timezone(&Utc)),
        LocalResult::Ambiguous(first, second) => {
            let timestamp = if first.timestamp() <= second.timestamp() {
                first
            } else {
                second
            };
            Some(timestamp.with_timezone(&Utc))
        }
        LocalResult::None => {
            // Match Python ZoneInfo's default fold=0 for a nonexistent local
            // time: use the offset immediately before the clock transition.
            for minutes in 1..=24 * 60 {
                let prior =
                    value.checked_sub_signed(Duration::minutes(minutes))?;
                let prior = match timezone.from_local_datetime(&prior) {
                    LocalResult::Single(timestamp) => timestamp,
                    LocalResult::Ambiguous(first, second) => {
                        if first.timestamp() <= second.timestamp() {
                            first
                        } else {
                            second
                        }
                    }
                    LocalResult::None => continue,
                };
                let offset = prior.offset().fix();
                return offset
                    .from_local_datetime(&value)
                    .single()
                    .map(|timestamp| timestamp.with_timezone(&Utc));
            }
            None
        }
    }
}

fn format_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::AutoSi, true)
}

fn parse_runtime_seconds(value: &str) -> Option<i64> {
    let seconds = value.trim().parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds < 0.0 {
        return None;
    }
    Some(seconds.floor() as i64)
}

fn optional_nonempty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn archive_key_sort_key(key: &AgentArchiveKeyWire) -> (&str, &str, &str) {
    (
        &key.source_username,
        &key.source_machine,
        &key.source_run_id,
    )
}

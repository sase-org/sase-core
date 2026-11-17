//! Read-only, target-selective artifact identity lookups.
//!
//! Healthy queries never migrate, rebuild, or reconcile. Unavailable
//! indexes take the shared bounded recovery path instead of becoming an
//! authoritative miss.

use super::index_wire::AGENT_ARTIFACT_INDEX_SCHEMA_VERSION;
use super::lookup_recovery::{
    bounded_lookup_recovery, revalidate_lookup_hits, LOOKUP_FALLBACK_BUDGET,
};
use super::readers::AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT;
use super::storage::{
    open_index_read_only_no_migrate, read_index_schema_version,
    resolve_index_artifact_dir,
};
use super::{placeholders, LOOKUP_SESSION_INDEX_COLUMN};
use crate::agent_scan::layout::ACE_RUN_WORKFLOW_DIR;
use rusqlite::{params_from_iter, Connection};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::Path;

pub const AGENT_ARTIFACT_LOOKUP_FALLBACK_BUDGET: u32 = LOOKUP_FALLBACK_BUDGET;

const LOOKUP_HIT_COLUMNS: &str =
    "artifact_dir, project_name, workflow_dir_name, \
     timestamp, agent_name, workflow_name, agent_session, lookup_session, \
     agent_clan, agent_clan_generation, parent_timestamp, has_done_marker, \
     has_running_marker, hidden";

/// Identity target for one lookup request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AgentArtifactLookupTargetWire {
    Names { names: Vec<String> },
    Session { names: Vec<String> },
    Clan { names: Vec<String> },
    Pointers { artifact_dirs: Vec<String> },
}

/// Query knobs for a targeted artifact lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactLookupQueryWire {
    pub target: AgentArtifactLookupTargetWire,
    #[serde(default = "default_include_hidden")]
    pub include_hidden: bool,
    /// When set, skip the index open and recover as this unavailability.
    #[serde(default)]
    pub forced_unavailability: Option<AgentArtifactLookupAvailabilityWire>,
}

fn default_include_hidden() -> bool {
    true
}

/// Why a lookup could not use a healthy index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentArtifactLookupAvailabilityWire {
    Ready,
    Missing,
    StaleSchema,
    NewerSchema,
    Corrupt,
    Busy,
}

/// Compact identity hit. Ranking and liveness stay with later callers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactLookupHitWire {
    pub artifact_dir: String,
    pub project_name: String,
    pub workflow_dir_name: String,
    pub timestamp: String,
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub agent_session: Option<String>,
    #[serde(default)]
    pub lookup_session: Option<String>,
    #[serde(default)]
    pub agent_clan: Option<String>,
    #[serde(default)]
    pub agent_clan_generation: Option<String>,
    #[serde(default)]
    pub parent_timestamp: Option<String>,
    pub has_done_marker: bool,
    pub has_running_marker: bool,
    pub hidden: bool,
    #[serde(default)]
    pub match_kind: Option<String>,
}

/// Result of one targeted lookup, including availability/completeness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentArtifactLookupResultWire {
    pub schema_version: u32,
    pub index_path: String,
    pub projects_root: String,
    pub query: AgentArtifactLookupQueryWire,
    pub availability: AgentArtifactLookupAvailabilityWire,
    pub complete: bool,
    pub source: String,
    #[serde(default)]
    pub diagnostic: Option<String>,
    pub hits: Vec<AgentArtifactLookupHitWire>,
    pub truncated: bool,
    pub dirs_visited: u64,
    pub records_decoded: u64,
}

/// Look up matching ace-run artifacts without maintenance on the read path.
pub fn lookup_agent_artifacts(
    index_path: &Path,
    projects_root: &Path,
    query: AgentArtifactLookupQueryWire,
) -> AgentArtifactLookupResultWire {
    let mut result = empty_result(index_path, projects_root, query.clone());
    if let Some(forced) = query.forced_unavailability {
        return recover(index_path, projects_root, query, forced, None);
    }

    match classify_index(index_path) {
        IndexClass::Ready(conn) => match query_index(&conn, &query) {
            Ok(hits) => {
                super::record_lookup_records_decoded(0);
                result.availability =
                    AgentArtifactLookupAvailabilityWire::Ready;
                result.complete = true;
                result.source = "index".to_string();
                result.hits =
                    revalidate_lookup_hits(projects_root, &query, hits);
                result
            }
            Err(error) if is_busy_error(&error) => recover(
                index_path,
                projects_root,
                query,
                AgentArtifactLookupAvailabilityWire::Busy,
                Some(error),
            ),
            Err(error) if is_corrupt_error(&error) => recover(
                index_path,
                projects_root,
                query,
                AgentArtifactLookupAvailabilityWire::Corrupt,
                Some(error),
            ),
            Err(error) => recover(
                index_path,
                projects_root,
                query,
                AgentArtifactLookupAvailabilityWire::Corrupt,
                Some(error),
            ),
        },
        IndexClass::Unavailable {
            availability,
            diagnostic,
        } => {
            recover(index_path, projects_root, query, availability, diagnostic)
        }
    }
}

enum IndexClass {
    Ready(Connection),
    Unavailable {
        availability: AgentArtifactLookupAvailabilityWire,
        diagnostic: Option<String>,
    },
}

fn classify_index(index_path: &Path) -> IndexClass {
    if !index_path.is_file() {
        return IndexClass::Unavailable {
            availability: AgentArtifactLookupAvailabilityWire::Missing,
            diagnostic: None,
        };
    }
    let conn = match open_index_read_only_no_migrate(
        index_path,
        AGENT_ARTIFACT_INDEX_PROJECTION_BUSY_TIMEOUT,
    ) {
        Ok(conn) => conn,
        Err(error) if is_busy_error(&error) => {
            return IndexClass::Unavailable {
                availability: AgentArtifactLookupAvailabilityWire::Busy,
                diagnostic: Some(error),
            };
        }
        Err(error) if is_corrupt_error(&error) => {
            return IndexClass::Unavailable {
                availability: AgentArtifactLookupAvailabilityWire::Corrupt,
                diagnostic: Some(error),
            };
        }
        Err(error) => {
            return IndexClass::Unavailable {
                availability: AgentArtifactLookupAvailabilityWire::Corrupt,
                diagnostic: Some(error),
            };
        }
    };
    let version = match read_index_schema_version(&conn) {
        Ok(version) => version,
        Err(error) if is_busy_error(&error) => {
            return IndexClass::Unavailable {
                availability: AgentArtifactLookupAvailabilityWire::Busy,
                diagnostic: Some(error),
            };
        }
        Err(error) => {
            let availability = if is_corrupt_error(&error) {
                AgentArtifactLookupAvailabilityWire::Corrupt
            } else {
                AgentArtifactLookupAvailabilityWire::StaleSchema
            };
            return IndexClass::Unavailable {
                availability,
                diagnostic: Some(error),
            };
        }
    };
    if version < AGENT_ARTIFACT_INDEX_SCHEMA_VERSION {
        return IndexClass::Unavailable {
            availability: AgentArtifactLookupAvailabilityWire::StaleSchema,
            diagnostic: Some(format!("schema_version={version}")),
        };
    }
    if version > AGENT_ARTIFACT_INDEX_SCHEMA_VERSION {
        return IndexClass::Unavailable {
            availability: AgentArtifactLookupAvailabilityWire::NewerSchema,
            diagnostic: Some(format!("schema_version={version}")),
        };
    }
    if !has_lookup_session_column(&conn) {
        return IndexClass::Unavailable {
            availability: AgentArtifactLookupAvailabilityWire::StaleSchema,
            diagnostic: Some("lookup_session column missing".to_string()),
        };
    }
    IndexClass::Ready(conn)
}

fn has_lookup_session_column(conn: &Connection) -> bool {
    let Ok(mut stmt) = conn.prepare("PRAGMA table_info(agent_artifacts)")
    else {
        return false;
    };
    let Ok(mut rows) = stmt.query([]) else {
        return false;
    };
    while let Ok(Some(row)) = rows.next() {
        let Ok(name) = row.get::<_, String>(1) else {
            continue;
        };
        if name == LOOKUP_SESSION_INDEX_COLUMN {
            return true;
        }
    }
    false
}

fn query_index(
    conn: &Connection,
    query: &AgentArtifactLookupQueryWire,
) -> Result<Vec<AgentArtifactLookupHitWire>, String> {
    match &query.target {
        AgentArtifactLookupTargetWire::Names { names } => {
            query_name_hits(conn, names, query.include_hidden)
        }
        AgentArtifactLookupTargetWire::Session { names } => query_column_hits(
            conn,
            LOOKUP_SESSION_INDEX_COLUMN,
            names,
            query.include_hidden,
            "session",
        ),
        AgentArtifactLookupTargetWire::Clan { names } => query_column_hits(
            conn,
            "agent_clan",
            names,
            query.include_hidden,
            "clan",
        ),
        AgentArtifactLookupTargetWire::Pointers { artifact_dirs } => {
            query_pointer_hits(conn, artifact_dirs, query.include_hidden)
        }
    }
}

fn query_name_hits(
    conn: &Connection,
    names: &[String],
    include_hidden: bool,
) -> Result<Vec<AgentArtifactLookupHitWire>, String> {
    let names = unique_nonempty(names);
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let name_placeholders = placeholders(names.len());
    let hidden_sql = hidden_predicate(include_hidden);
    let sql = format!(
        "SELECT {LOOKUP_HIT_COLUMNS} FROM agent_artifacts \
         WHERE workflow_dir_name = ? \
           AND agent_meta_sig IS NOT NULL \
           {hidden_sql} \
           AND (agent_name IN ({name_placeholders}) \
                OR workflow_name IN ({name_placeholders})) \
         ORDER BY timestamp DESC, artifact_dir DESC"
    );
    let mut params: Vec<String> = vec![ACE_RUN_WORKFLOW_DIR.to_string()];
    params.extend(names.iter().cloned());
    params.extend(names.iter().cloned());
    let mut hits = query_hits(conn, &sql, &params)?;
    let name_set: BTreeSet<&str> = names.iter().map(String::as_str).collect();
    for hit in &mut hits {
        hit.match_kind = Some(
            if hit
                .agent_name
                .as_deref()
                .is_some_and(|name| name_set.contains(name))
            {
                "exact".to_string()
            } else {
                "workflow".to_string()
            },
        );
    }
    Ok(hits)
}

fn query_column_hits(
    conn: &Connection,
    column: &str,
    names: &[String],
    include_hidden: bool,
    match_kind: &str,
) -> Result<Vec<AgentArtifactLookupHitWire>, String> {
    let names = unique_nonempty(names);
    if names.is_empty() {
        return Ok(Vec::new());
    }
    let name_placeholders = placeholders(names.len());
    let hidden_sql = hidden_predicate(include_hidden);
    let sql = format!(
        "SELECT {LOOKUP_HIT_COLUMNS} FROM agent_artifacts \
         WHERE workflow_dir_name = ? \
           AND agent_meta_sig IS NOT NULL \
           {hidden_sql} \
           AND {column} IN ({name_placeholders}) \
         ORDER BY timestamp DESC, artifact_dir DESC"
    );
    let mut params: Vec<String> = vec![ACE_RUN_WORKFLOW_DIR.to_string()];
    params.extend(names);
    let mut hits = query_hits(conn, &sql, &params)?;
    for hit in &mut hits {
        hit.match_kind = Some(match_kind.to_string());
    }
    Ok(hits)
}

fn query_pointer_hits(
    conn: &Connection,
    artifact_dirs: &[String],
    include_hidden: bool,
) -> Result<Vec<AgentArtifactLookupHitWire>, String> {
    let mut resolved = Vec::new();
    let mut seen = BTreeSet::new();
    for artifact_dir in artifact_dirs {
        if artifact_dir.is_empty() {
            continue;
        }
        let canonical = canonical_path(artifact_dir);
        for candidate in [artifact_dir.clone(), canonical] {
            let resolved_dir = resolve_index_artifact_dir(conn, &candidate)?;
            if seen.insert(resolved_dir.clone()) {
                resolved.push(resolved_dir);
            }
        }
    }
    if resolved.is_empty() {
        return Ok(Vec::new());
    }
    let dir_placeholders = placeholders(resolved.len());
    let hidden_sql = hidden_predicate(include_hidden);
    let sql = format!(
        "SELECT {LOOKUP_HIT_COLUMNS} FROM agent_artifacts \
         WHERE artifact_dir IN ({dir_placeholders}) \
           {hidden_sql} \
         ORDER BY timestamp DESC, artifact_dir DESC"
    );
    let mut hits = query_hits(conn, &sql, &resolved)?;
    for hit in &mut hits {
        hit.match_kind = Some("pointer".to_string());
    }
    Ok(hits)
}

fn query_hits(
    conn: &Connection,
    sql: &str,
    params: &[String],
) -> Result<Vec<AgentArtifactLookupHitWire>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| e.to_string())?;
    let mut rows = stmt
        .query(params_from_iter(params.iter()))
        .map_err(|e| e.to_string())?;
    let mut hits = Vec::new();
    while let Some(row) = rows.next().map_err(|e| e.to_string())? {
        hits.push(hit_from_row(row)?);
    }
    Ok(hits)
}

pub(super) fn hit_from_row(
    row: &rusqlite::Row<'_>,
) -> Result<AgentArtifactLookupHitWire, String> {
    Ok(AgentArtifactLookupHitWire {
        artifact_dir: row.get(0).map_err(|e| e.to_string())?,
        project_name: row.get(1).map_err(|e| e.to_string())?,
        workflow_dir_name: row.get(2).map_err(|e| e.to_string())?,
        timestamp: row.get(3).map_err(|e| e.to_string())?,
        agent_name: row.get(4).map_err(|e| e.to_string())?,
        workflow_name: row.get(5).map_err(|e| e.to_string())?,
        agent_session: row.get(6).map_err(|e| e.to_string())?,
        lookup_session: row.get(7).map_err(|e| e.to_string())?,
        agent_clan: row.get(8).map_err(|e| e.to_string())?,
        agent_clan_generation: row.get(9).map_err(|e| e.to_string())?,
        parent_timestamp: row.get(10).map_err(|e| e.to_string())?,
        has_done_marker: row.get::<_, i64>(11).map_err(|e| e.to_string())? != 0,
        has_running_marker: row.get::<_, i64>(12).map_err(|e| e.to_string())?
            != 0,
        hidden: row.get::<_, i64>(13).map_err(|e| e.to_string())? != 0,
        match_kind: None,
    })
}

fn hidden_predicate(include_hidden: bool) -> &'static str {
    if include_hidden {
        ""
    } else {
        "AND hidden = 0"
    }
}

fn unique_nonempty(values: &[String]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for value in values {
        if value.is_empty() {
            continue;
        }
        if seen.insert(value.clone()) {
            out.push(value.clone());
        }
    }
    out
}

pub(super) fn canonical_path(path: &str) -> String {
    Path::new(path)
        .canonicalize()
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

fn recover(
    index_path: &Path,
    projects_root: &Path,
    query: AgentArtifactLookupQueryWire,
    availability: AgentArtifactLookupAvailabilityWire,
    diagnostic: Option<String>,
) -> AgentArtifactLookupResultWire {
    let recovered = bounded_lookup_recovery(projects_root, &query);
    super::record_lookup_records_decoded(recovered.records_decoded);
    let mut result = empty_result(index_path, projects_root, query);
    result.availability = availability;
    // Any identity consumer can use bounded recovery when the source traversal
    // proves exhaustion without an IO error or budget truncation. A small
    // exhausted source is authoritative; a large or unreadable source remains
    // incomplete. An absent projects root is a complete empty result.
    result.complete = recovered.exhausted
        || projects_root.try_exists().is_ok_and(|exists| !exists);
    result.source = if recovered.hits.is_empty() && recovered.dirs_visited == 0
    {
        "none".to_string()
    } else {
        "fallback".to_string()
    };
    result.diagnostic = diagnostic;
    result.hits = recovered.hits;
    result.truncated = recovered.truncated;
    result.dirs_visited = recovered.dirs_visited;
    result.records_decoded = recovered.records_decoded;
    result
}

fn empty_result(
    index_path: &Path,
    projects_root: &Path,
    query: AgentArtifactLookupQueryWire,
) -> AgentArtifactLookupResultWire {
    AgentArtifactLookupResultWire {
        schema_version: AGENT_ARTIFACT_INDEX_SCHEMA_VERSION,
        index_path: index_path.to_string_lossy().into_owned(),
        projects_root: projects_root.to_string_lossy().into_owned(),
        query,
        availability: AgentArtifactLookupAvailabilityWire::Missing,
        complete: false,
        source: "none".to_string(),
        diagnostic: None,
        hits: Vec::new(),
        truncated: false,
        dirs_visited: 0,
        records_decoded: 0,
    }
}

pub(super) fn is_busy_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    lower.contains("database is locked")
        || lower.contains("database table is locked")
        || lower.contains("busy")
}

pub(super) fn is_corrupt_error(error: &str) -> bool {
    super::storage::is_sqlite_index_corruption_error(error)
        || error
            .to_ascii_lowercase()
            .contains("file is not a database")
}

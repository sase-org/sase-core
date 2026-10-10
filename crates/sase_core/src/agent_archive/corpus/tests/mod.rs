use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rusqlite::{params, Connection};

use crate::agent_archive::corpus::wire::{
    AgentArchiveCompileRequestWire, AgentArchiveCorpusRowWire,
    AgentArchiveIndexProbeStatusWire, AgentArchiveLinkFacetsWire,
};

mod compile;
mod query;

fn by_run_id(
    rows: &[AgentArchiveCorpusRowWire],
) -> BTreeMap<&str, &AgentArchiveCorpusRowWire> {
    rows.iter()
        .map(|row| (row.archive_key.source_run_id.as_str(), row))
        .collect()
}

fn request(
    root: &Path,
    index_status: AgentArchiveIndexProbeStatusWire,
    link_facets: BTreeMap<String, AgentArchiveLinkFacetsWire>,
) -> AgentArchiveCompileRequestWire {
    AgentArchiveCompileRequestWire {
        root: root.to_string_lossy().to_string(),
        index_status,
        timezone: "America/New_York".to_string(),
        link_facets,
    }
}

fn create_index(root: &Path, schema_version: u32) -> Connection {
    fs::create_dir_all(root).unwrap();
    let conn = Connection::open(root.join("index.sqlite")).unwrap();
    conn.execute_batch(
        "CREATE TABLE dismissed_bundle_index_meta (
             key TEXT PRIMARY KEY, value TEXT NOT NULL
         );
         CREATE TABLE dismissed_bundle_summaries (
             bundle_path TEXT PRIMARY KEY,
             filename TEXT NOT NULL,
             source_username TEXT,
             source_machine TEXT,
             source_run_id TEXT,
             archive_visibility TEXT NOT NULL,
             agent_name TEXT,
             agent_session TEXT,
             agent_clan TEXT,
             agent_clan_generation TEXT,
             project_name TEXT,
             agent_session_role TEXT,
             workflow TEXT,
             agent_type TEXT NOT NULL,
             status TEXT NOT NULL,
             start_time TEXT,
             stop_time TEXT,
             dismissed_at TEXT,
             model TEXT,
             llm_provider TEXT,
             agent_tab TEXT,
             tribe TEXT,
             clan_tribe TEXT,
             runtime TEXT,
             retry_attempt INTEGER NOT NULL,
             retry_of_timestamp TEXT,
             durably_revivable INTEGER NOT NULL,
             is_workflow_child INTEGER NOT NULL
         );
         CREATE TABLE archive_visibility_projection (
             source_username TEXT NOT NULL,
             source_machine TEXT NOT NULL,
             source_run_id TEXT NOT NULL,
             visibility TEXT NOT NULL,
             dismissed_at TEXT,
             PRIMARY KEY(source_username, source_machine, source_run_id)
         );",
    )
    .unwrap();
    conn.execute(
        "INSERT INTO dismissed_bundle_index_meta(key, value) VALUES ('schema_version', ?1)",
        [schema_version.to_string()],
    )
    .unwrap();
    conn
}

#[derive(Default)]
struct TestRow {
    id: String,
    filename: String,
    bundle_path: String,
    archive_visibility: String,
    projected_visibility: Option<String>,
    agent_name: Option<String>,
    agent_session: Option<String>,
    agent_clan: Option<String>,
    agent_clan_generation: Option<String>,
    project_name: Option<String>,
    agent_session_role: Option<String>,
    workflow: Option<String>,
    agent_type: String,
    status: String,
    start_time: Option<String>,
    stop_time: Option<String>,
    dismissed_at: Option<String>,
    model: Option<String>,
    llm_provider: Option<String>,
    agent_tab: Option<String>,
    tribe: Option<String>,
    clan_tribe: Option<String>,
    runtime: Option<String>,
    retry_attempt: i64,
    retry_of_timestamp: Option<String>,
    restorable: bool,
    is_workflow_child: bool,
}

impl TestRow {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            filename: format!("{id}.json"),
            archive_visibility: "hidden".to_string(),
            agent_type: "run".to_string(),
            status: "DONE".to_string(),
            ..Self::default()
        }
    }
}

fn insert_row(conn: &mut Connection, root: &Path, row: &TestRow) {
    let bundle_path = if row.bundle_path.is_empty() {
        root.join(&row.filename)
    } else {
        std::path::PathBuf::from(&row.bundle_path)
    };
    fs::write(&bundle_path, "{}").unwrap();
    conn.execute(
        "INSERT INTO dismissed_bundle_summaries (
             bundle_path, filename, source_username, source_machine, source_run_id,
             archive_visibility, agent_name, agent_session, agent_clan,
             agent_clan_generation, project_name, agent_session_role, workflow,
             agent_type, status, start_time, stop_time, dismissed_at, model,
             llm_provider, agent_tab, tribe, clan_tribe, runtime, retry_attempt,
             retry_of_timestamp, durably_revivable, is_workflow_child
         ) VALUES (
             ?1, ?2, 'bryan', 'athena', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22,
             ?23, ?24, ?25, ?26
         )",
        params![
            bundle_path.to_string_lossy(),
            row.filename,
            row.id,
            row.archive_visibility,
            row.agent_name,
            row.agent_session,
            row.agent_clan,
            row.agent_clan_generation,
            row.project_name,
            row.agent_session_role,
            row.workflow,
            row.agent_type,
            row.status,
            row.start_time,
            row.stop_time,
            row.dismissed_at,
            row.model,
            row.llm_provider,
            row.agent_tab,
            row.tribe,
            row.clan_tribe,
            row.runtime,
            row.retry_attempt,
            row.retry_of_timestamp,
            i64::from(row.restorable),
            i64::from(row.is_workflow_child),
        ],
    )
    .unwrap();
    if let Some(visibility) = row.projected_visibility.as_deref() {
        conn.execute(
            "INSERT INTO archive_visibility_projection (
                 source_username, source_machine, source_run_id, visibility, dismissed_at
             ) VALUES ('bryan', 'athena', ?1, ?2, NULL)",
            params![row.id, visibility],
        )
        .unwrap();
    }
}

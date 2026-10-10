use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use rusqlite::{params, Connection};
use tempfile::tempdir;

use crate::agent_archive::corpus::compile::compile_agent_archive_corpus;
use crate::agent_archive::corpus::wire::{
    AgentArchiveCompileRequestWire, AgentArchiveCorpusStatusWire,
    AgentArchiveIndexProbeStatusWire, AgentArchiveLinkFacetsWire,
    AgentArchiveOutcomeWire, AgentArchiveTimeBasisWire,
};

#[test]
fn compile_uses_hidden_top_level_rows_and_child_name_map() {
    let temp = tempdir().unwrap();
    let mut conn = create_index(temp.path(), 3);

    let mut hidden = TestRow::new("hidden");
    hidden.agent_name = Some("260101.root".to_string());
    hidden.restorable = true;
    insert_row(&mut conn, temp.path(), &hidden);

    let mut child = TestRow::new("hidden");
    child.filename = "260101__c1.json".to_string();
    child.bundle_path = temp
        .path()
        .join(&child.filename)
        .to_string_lossy()
        .to_string();
    child.agent_name = Some("260101.child".to_string());
    child.is_workflow_child = true;
    insert_row(&mut conn, temp.path(), &child);

    let mut projected_visible = TestRow::new("visible-projection");
    projected_visible.archive_visibility = "hidden".to_string();
    projected_visible.projected_visibility = Some("visible".to_string());
    insert_row(&mut conn, temp.path(), &projected_visible);

    let mut pinned = TestRow::new("pinned");
    pinned.archive_visibility = "pinned".to_string();
    insert_row(&mut conn, temp.path(), &pinned);

    let mut facets = BTreeMap::new();
    facets.insert(
        "root".to_string(),
        AgentArchiveLinkFacetsWire {
            relations: vec!["implements".to_string()],
            artifacts: vec!["plan:archive".to_string()],
            count: 1,
        },
    );
    let corpus = compile_agent_archive_corpus(request(
        temp.path(),
        AgentArchiveIndexProbeStatusWire::Ok,
        facets,
    ))
    .unwrap();

    assert_eq!(corpus.rows.len(), 1);
    let row = &corpus.rows[0];
    assert_eq!(row.archive_key.source_run_id, "hidden");
    assert_eq!(row.canonical_name.as_deref(), Some("root"));
    assert_eq!(row.global_name.as_deref(), Some("bryan.athena.root"));
    assert!(row.restorable);
    assert!(row.linked);
    assert_eq!(row.relations, vec!["implements"]);
    assert_eq!(row.artifacts, vec!["plan:archive"]);

    let child_matches = corpus.name_map.get("child").unwrap();
    assert_eq!(child_matches.len(), 1);
    assert!(child_matches[0].is_workflow_child);
    assert_eq!(child_matches[0].owner_key.source_run_id, "hidden");
    assert!(!corpus.name_map.contains_key("visible-projection"));
    assert!(!corpus.name_map.contains_key("pinned"));
}

#[test]
fn compile_derives_time_bases_runtime_and_utc_instants() {
    let temp = tempdir().unwrap();
    let mut conn = create_index(temp.path(), 3);

    let mut ended = TestRow::new("ended");
    ended.start_time = Some("2025-01-01T10:00:00".to_string());
    ended.stop_time = Some("2025-01-01T11:00:00".to_string());
    ended.runtime = Some("not-a-duration".to_string());
    insert_row(&mut conn, temp.path(), &ended);

    let mut dismissed = TestRow::new("dismissed");
    dismissed.start_time = Some("2025-07-01 08:00:00".to_string());
    dismissed.dismissed_at = Some("2025-07-01T16:00:00+02:00".to_string());
    dismissed.runtime = Some("125.8".to_string());
    insert_row(&mut conn, temp.path(), &dismissed);

    let mut started = TestRow::new("started");
    started.start_time = Some("2025-02-03T04:05:06Z".to_string());
    insert_row(&mut conn, temp.path(), &started);

    let no_times = TestRow::new("no-times");
    insert_row(&mut conn, temp.path(), &no_times);

    let corpus = compile_agent_archive_corpus(request(
        temp.path(),
        AgentArchiveIndexProbeStatusWire::Ok,
        BTreeMap::new(),
    ))
    .unwrap();
    let rows = by_run_id(&corpus.rows);

    assert_eq!(
        rows["ended"].time_basis,
        Some(AgentArchiveTimeBasisWire::Ended)
    );
    assert_eq!(
        rows["ended"].last_activity_at.as_deref(),
        Some("2025-01-01T16:00:00Z")
    );
    assert_eq!(
        rows["ended"].start_time.as_deref(),
        Some("2025-01-01T15:00:00Z")
    );
    assert_eq!(rows["ended"].runtime_seconds, Some(3_600));

    assert_eq!(
        rows["dismissed"].time_basis,
        Some(AgentArchiveTimeBasisWire::Dismissed)
    );
    assert_eq!(
        rows["dismissed"].last_activity_at.as_deref(),
        Some("2025-07-01T14:00:00Z")
    );
    assert_eq!(rows["dismissed"].runtime_seconds, Some(125));

    assert_eq!(
        rows["started"].time_basis,
        Some(AgentArchiveTimeBasisWire::Started)
    );
    assert_eq!(
        rows["started"].last_activity_at.as_deref(),
        Some("2025-02-03T04:05:06Z")
    );
    assert_eq!(rows["no-times"].last_activity_at, None);
    assert_eq!(rows["no-times"].time_basis, None);
    assert_eq!(rows["no-times"].runtime_seconds, None);
}

#[test]
fn compile_builds_clan_session_and_singleton_container_metadata() {
    let temp = tempdir().unwrap();
    let mut conn = create_index(temp.path(), 3);

    let mut clan_first = TestRow::new("clan-a");
    clan_first.agent_clan = Some("review".to_string());
    clan_first.agent_clan_generation = Some("run-1".to_string());
    clan_first.stop_time = Some("2025-03-01T00:00:00Z".to_string());
    clan_first.status = "DONE".to_string();
    insert_row(&mut conn, temp.path(), &clan_first);

    let mut clan_second = TestRow::new("clan-b");
    clan_second.agent_clan = Some("review".to_string());
    clan_second.agent_clan_generation = Some("run-1".to_string());
    clan_second.stop_time = Some("2025-03-02T00:00:00Z".to_string());
    clan_second.status = "FAILED".to_string();
    insert_row(&mut conn, temp.path(), &clan_second);

    let mut session_first = TestRow::new("session-a");
    session_first.agent_session = Some("build".to_string());
    session_first.status = "RUNNING".to_string();
    insert_row(&mut conn, temp.path(), &session_first);

    let mut session_second = TestRow::new("session-b");
    session_second.agent_session = Some("build".to_string());
    session_second.status = "DONE".to_string();
    insert_row(&mut conn, temp.path(), &session_second);

    insert_row(&mut conn, temp.path(), &TestRow::new("single"));

    let corpus = compile_agent_archive_corpus(request(
        temp.path(),
        AgentArchiveIndexProbeStatusWire::Ok,
        BTreeMap::new(),
    ))
    .unwrap();
    let containers: BTreeMap<_, _> = corpus
        .containers
        .iter()
        .map(|item| (item.key.as_str(), item))
        .collect();

    let clan = containers["clan:review:run-1"];
    assert_eq!(clan.label.as_deref(), Some("review"));
    assert_eq!(clan.member_count, 2);
    assert_eq!(clan.outcome, AgentArchiveOutcomeWire::Failed);
    assert_eq!(clan.member_keys[0].source_run_id, "clan-b");
    assert_eq!(
        clan.last_activity_at.as_deref(),
        Some("2025-03-02T00:00:00Z")
    );

    let session = containers["session:build"];
    assert_eq!(session.label.as_deref(), Some("build"));
    assert_eq!(session.member_count, 2);
    assert_eq!(session.outcome, AgentArchiveOutcomeWire::Interrupted);

    let singleton = containers["archive:bryan.athena@single"];
    assert_eq!(singleton.label, None);
    assert_eq!(singleton.member_count, 1);
    assert_eq!(singleton.outcome, AgentArchiveOutcomeWire::Done);
}

#[test]
fn compile_reports_missing_rebuilding_and_unsupported_index_status() {
    let missing = tempdir().unwrap();
    let corpus = compile_agent_archive_corpus(request(
        missing.path(),
        AgentArchiveIndexProbeStatusWire::Missing,
        BTreeMap::new(),
    ))
    .unwrap();
    assert_eq!(corpus.status, AgentArchiveCorpusStatusWire::Missing);
    assert!(corpus.rows.is_empty());

    let rebuilding = tempdir().unwrap();
    let mut conn = create_index(rebuilding.path(), 3);
    insert_row(&mut conn, rebuilding.path(), &TestRow::new("partial"));
    let corpus = compile_agent_archive_corpus(request(
        rebuilding.path(),
        AgentArchiveIndexProbeStatusWire::Rebuilding { indexed_rows: 17 },
        BTreeMap::new(),
    ))
    .unwrap();
    assert_eq!(
        corpus.status,
        AgentArchiveCorpusStatusWire::Rebuilding { indexed_rows: 17 }
    );
    assert_eq!(corpus.rows.len(), 1);

    let unsupported = tempdir().unwrap();
    let _conn = create_index(unsupported.path(), 2);
    let corpus = compile_agent_archive_corpus(request(
        unsupported.path(),
        AgentArchiveIndexProbeStatusWire::Ok,
        BTreeMap::new(),
    ))
    .unwrap();
    let AgentArchiveCorpusStatusWire::Unsupported { error } = corpus.status
    else {
        panic!("expected unsupported schema status");
    };
    assert_eq!(error.expected_schema_version, 3);
    assert_eq!(error.actual_schema_version.as_deref(), Some("2"));
    assert!(corpus.rows.is_empty());
    assert!(corpus.name_map.is_empty());
}

fn by_run_id(
    rows: &[crate::agent_archive::corpus::wire::AgentArchiveCorpusRowWire],
) -> BTreeMap<
    &str,
    &crate::agent_archive::corpus::wire::AgentArchiveCorpusRowWire,
> {
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

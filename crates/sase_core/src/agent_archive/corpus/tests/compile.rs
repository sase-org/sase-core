use std::collections::BTreeMap;

use tempfile::tempdir;

use super::{by_run_id, create_index, insert_row, request, TestRow};
use crate::agent_archive::corpus::compile::compile_agent_archive_corpus;
use crate::agent_archive::corpus::wire::{
    AgentArchiveCorpusStatusWire, AgentArchiveIndexProbeStatusWire,
    AgentArchiveLinkFacetsWire, AgentArchiveOutcomeWire,
    AgentArchiveTimeBasisWire,
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
    assert_eq!(corpus.timezone, "America/New_York");
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

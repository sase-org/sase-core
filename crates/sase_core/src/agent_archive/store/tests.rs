use super::*;
use serde_json::json;
use std::io::Write;

fn write_bundle(root: &Path, name: &str, bundle: &JsonValue) -> PathBuf {
    let path = root.join(name);
    let mut file = fs::File::create(&path).unwrap();
    file.write_all(serde_json::to_vec(bundle).unwrap().as_slice())
        .unwrap();
    path
}

fn sample_bundle(suffix: &str) -> JsonValue {
    json!({
        "raw_suffix": suffix,
        "status": "DONE",
        "cl_name": "cl_a",
        "agent_type": "run",
        "agent_name": "agent-1",
        "source_username": "alice",
        "source_machine": "athena",
        "source_run_id": "run-1",
        "start_time": "2026-05-12T12:00:00",
        "dismissed_at": "2026-05-12T12:30:00",
        "model": "m1",
        "llm_provider": "p1",
        "project_file": "/home/alice/work/proj/agent.json",
    })
}

#[test]
fn store_upsert_query_signature_and_delete_round_trip() {
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    let first = write_bundle(root, "a.json", &sample_bundle("suf-1"));
    let mut later = sample_bundle("suf-2");
    later["start_time"] = json!("2026-05-13T12:00:00");
    let second = write_bundle(root, "b.json", &later);
    upsert_bundle_summary(root, &first, &read_bundle(&first).unwrap()).unwrap();
    upsert_bundle_summary(root, &second, &read_bundle(&second).unwrap())
        .unwrap();

    let signature = index_signature(root).unwrap();
    assert_eq!(signature.schema_version, 3);
    assert_eq!(signature.row_count, 2);

    let all = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            visibility: Some("hidden".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(all.len(), 2);
    // Newest start_time first.
    assert_eq!(all[0].bundle_path, second.to_string_lossy());
    assert_eq!(all[0].agent_id, "alice.athena@run-1");
    assert_eq!(all[0].project_name.as_deref(), Some("proj"));
    assert_eq!(all[0].shard, "");
    assert!(all[0].archive_payload_sha256.is_some());
    assert!(all[0].historically_viewable);

    let filtered = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            suffixes: Some(vec!["suf-1".to_string()]),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].raw_suffix, "suf-1");

    let empty = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            suffixes: Some(Vec::new()),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert!(empty.is_empty());

    let by_cl = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            cl_name: Some("cl_a".to_string()),
            limit: Some(1),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(by_cl.len(), 1);

    let by_project = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            project_name: Some("proj".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(by_project.len(), 2);

    let page = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            limit: Some(1),
            offset: Some(1),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(page.len(), 1);

    let paths = query_bundle_paths_by_suffixes(root, vec!["suf-2".to_string()])
        .unwrap()
        .unwrap();
    assert_eq!(paths, vec![second.to_string_lossy().into_owned()]);

    let identities = query_summary_identities(root).unwrap().unwrap();
    assert_eq!(identities.len(), 2);

    delete_bundle_summaries_for_suffixes(root, &["suf-1".to_string()]).unwrap();
    let signature = index_signature(root).unwrap();
    assert_eq!(signature.row_count, 1);
    delete_bundle_summaries_for_suffixes(root, &[]).unwrap();
}

#[test]
fn store_set_visibility_merges_projection_and_summaries() {
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    let path = write_bundle(root, "a.json", &sample_bundle("suf-1"));
    upsert_bundle_summary(root, &path, &read_bundle(&path).unwrap()).unwrap();

    let changed = set_visibility_for_suffixes(
        root,
        &["suf-1".to_string()],
        "visible",
        Some("2026-05-12T13:00:00"),
        None,
        None,
    )
    .unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        set_visibility_for_suffixes(root, &[], "visible", None, None, None)
            .unwrap(),
        0
    );

    let hidden = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            visibility: Some("hidden".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert!(hidden.is_empty());
    let visible = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            visibility: Some("visible".to_string()),
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(
        visible[0].revived_at.as_deref(),
        Some("2026-05-12T13:00:00")
    );
    assert_eq!(visible[0].times_revived, 1);
    assert!(query_summary_identities(root).unwrap().unwrap().is_empty());
}

#[test]
fn store_opens_python_created_index_without_schema_drop() {
    // Fixture: the exact Python DDL plus one row written with raw SQL,
    // as if Python had created the index.
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    fs::create_dir_all(root).unwrap();
    let conn = Connection::open(index_path_for_root(root)).unwrap();
    conn.execute_batch(META_DDL).unwrap();
    conn.execute_batch(SUMMARIES_DDL).unwrap();
    conn.execute_batch(PROJECTION_DDL).unwrap();
    conn.execute_batch(FTS_DDL).unwrap();
    for ddl in INDEX_DDLS {
        conn.execute_batch(ddl).unwrap();
    }
    conn.execute(
        "INSERT OR REPLACE INTO dismissed_bundle_index_meta(key, value) \
         VALUES ('schema_version', '3')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO dismissed_bundle_summaries (
            bundle_path, agent_id, raw_suffix, shard, filename,
            agent_type, cl_name, status, is_workflow_child, retry_attempt,
            mtime_ns, size_bytes
         ) VALUES ('/tmp/x.json', 'legacy:suf-9', 'suf-9', '', 'x.json',
            'run', 'cl_x', 'DONE', 0, 0, 7, 8)",
        [],
    )
    .unwrap();
    drop(conn);

    let reopened = open_archive_store_writable(root).unwrap();
    let count: i64 = reopened
        .query_row(
            "SELECT COUNT(*) FROM dismissed_bundle_summaries",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let version: String = reopened
        .query_row(
            "SELECT value FROM dismissed_bundle_index_meta \
             WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, "3");

    let rows = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            visibility: None,
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].raw_suffix, "suf-9");
    assert_eq!(rows[0].cl_name, "cl_x");
}

#[test]
fn store_rebuilds_incompatible_schema() {
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    fs::create_dir_all(root).unwrap();
    let conn = Connection::open(index_path_for_root(root)).unwrap();
    conn.execute_batch(META_DDL).unwrap();
    // Legacy shape: missing the clan/tribe/session columns Python needs.
    conn.execute_batch(
        "CREATE TABLE dismissed_bundle_summaries (
            bundle_path TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            raw_suffix TEXT NOT NULL,
            mtime_ns INTEGER NOT NULL,
            size_bytes INTEGER NOT NULL
        )",
    )
    .unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO dismissed_bundle_index_meta(key, value) \
         VALUES ('schema_version', '3')",
        [],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO dismissed_bundle_summaries (
            bundle_path, agent_id, raw_suffix, mtime_ns, size_bytes
         ) VALUES ('/tmp/old.json', 'a', 'suf-old', 1, 2)",
        [],
    )
    .unwrap();
    drop(conn);

    open_archive_store_writable(root).unwrap();
    let reopened = open_archive_store_writable(root).unwrap();
    let count: i64 = reopened
        .query_row(
            "SELECT COUNT(*) FROM dismissed_bundle_summaries",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
    let columns =
        table_columns(&reopened, "dismissed_bundle_summaries").unwrap();
    assert!(columns.contains("clan_tribe"));
}

#[test]
fn store_payload_hash_matches_python() {
    let bundle = json!({
        "raw_suffix": "20260512120000",
        "status": "DONE",
        "cl_name": "cl_a",
        "agent_type": "run",
        "archive_schema_version": 3,
        "archive_search_text": null,
        "nested": {"z": 1, "a": [3, 2, 1]},
        "archive_payload_sha256": "stale",
        "uni": "héllo",
    });
    assert_eq!(
        archive_payload_hash_canonical(&bundle),
        "52593a468bf414d1bdea3d8310321e4073b4b81cce1fba598f841d00241fb3df"
    );
}

#[test]
fn store_creates_fts5_table() {
    let tmp = tempfile::TempDir::new().unwrap();
    let conn = open_archive_store_writable(tmp.path()).unwrap();
    let name: String = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type = 'table'
             AND name = 'dismissed_bundle_search_fts'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(name, "dismissed_bundle_search_fts");
    let count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM dismissed_bundle_search_fts",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn store_rebuild_progress_is_idle_without_rebuild() {
    let tmp = tempfile::TempDir::new().unwrap();
    let progress = rebuild_progress(tmp.path());
    assert!(!progress.rebuilding);
    assert_eq!(progress.indexed_rows, 0);
}

#[test]
fn store_quarantines_corrupt_file_and_rebuilds() {
    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path();
    fs::create_dir_all(root).unwrap();
    fs::write(index_path_for_root(root), b"this is not sqlite").unwrap();
    let kept = write_bundle(root, "a.json", &sample_bundle("suf-1"));
    fs::write(root.join("junk.json"), b"{oops").unwrap();

    let report = rebuild_index(root).unwrap();
    assert_eq!(report.indexed_rows, 1);
    assert_eq!(report.skipped_corrupt, 1);

    let mut quarantined = Vec::new();
    for entry in fs::read_dir(root).unwrap().flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with("index.sqlite.corrupt-") {
            quarantined.push(name);
        }
    }
    assert_eq!(quarantined.len(), 1);
    let stamp = quarantined[0]
        .strip_prefix("index.sqlite.corrupt-")
        .unwrap();
    assert_eq!(stamp.len(), 16, "expected UTC yyyymmddTHHMMSSZ");
    assert!(stamp.ends_with('Z'));

    let signature = index_signature(root).unwrap();
    assert_eq!(signature.row_count, 1);
    let rows = query_summaries(
        root,
        DismissedBundleIndexQueryWire {
            visibility: None,
            ..Default::default()
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].bundle_path, kept.to_string_lossy().into_owned());
}

#[test]
fn store_parses_bundle_filename_timestamps_like_python() {
    let ts = parse_filename_timestamp("agent-260512_120000.json").unwrap();
    assert_eq!(ts.format("%Y%m").to_string(), "202605");
    let ts = parse_filename_timestamp("20260512120000__c1.json").unwrap();
    assert_eq!(ts.format("%Y%m").to_string(), "202605");
    assert!(parse_filename_timestamp("plain.json").is_none());
    // A later valid stamp still matches when the earlier dash is not a
    // timestamp shape, mirroring `re.search`.
    let ts = parse_filename_timestamp("a-990199-b-260101_120000.json").unwrap();
    assert_eq!(ts.format("%Y%m").to_string(), "202601");
    // But when the first shape match is not a valid date, parsing stops.
    assert!(parse_filename_timestamp("a-990199_999999.json").is_none());
}

#[test]
fn store_concurrent_upsert_and_compile_verify_ok() {
    use crate::agent_archive::corpus::{
        compile_agent_archive_corpus, AgentArchiveCompileRequestWire,
        AgentArchiveIndexProbeStatusWire,
    };
    use crate::agent_archive::verify_agent_archive_index;

    let tmp = tempfile::TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    for i in 0..8 {
        let bundle = sample_bundle(&format!("suf-{i:02}"));
        write_bundle(&root, &format!("b{i:02}.json"), &bundle);
    }
    let root_key = root.to_string_lossy().into_owned();
    let mut handles = Vec::new();
    for worker in 0..4 {
        let root = root.clone();
        let root_key = root_key.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..8 {
                let name = format!("b{i:02}.json");
                let path = root.join(&name);
                let bundle = read_bundle(&path).unwrap();
                upsert_bundle_summary(&root, &path, &bundle).unwrap();
            }
            let request = AgentArchiveCompileRequestWire {
                root: root_key,
                index_status: AgentArchiveIndexProbeStatusWire::Ok,
                timezone: "UTC".to_string(),
                link_facets: BTreeMap::new(),
            };
            compile_agent_archive_corpus(request).unwrap();
            let _ = worker;
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let report = verify_agent_archive_index(&root);
    assert!(report.ok, "verify failed: {report:?}");
    assert_eq!(report.indexed_rows, 8);
}

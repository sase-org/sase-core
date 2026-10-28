use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;

use super::catalog::{
    catalog_rev, ingest_catalog, parse_catalog, validate_overlay_shape,
};
use super::derived::upsert_derived;
use super::fold::{fold, fold_file_in_tx};
use super::maintenance::{clear, prune};
use super::spool::{
    parse_spool_line, SpoolLine, SPOOL_DROP_SLACK_BYTES, SPOOL_FILE_MAX_BYTES,
    SPOOL_LINE_MAX_BYTES,
};
use super::status::status;
use super::store::{begin_immediate, open_write_store, FOOTPRINT_BUSY_MAX};
use super::wire::{
    DerivedCoverageRowWire, DerivedDailyRowWire, FOOTPRINT_WIRE_SCHEMA_VERSION,
};
use super::{catalog_status, footprint_wire_schema_version};

const TODAY: &str = "2026-10-27";
const BUSY: Duration = FOOTPRINT_BUSY_MAX;

fn write_catalog(dir: &Path, entries: &[&str]) -> std::path::PathBuf {
    let body = if entries.is_empty() {
        Vec::new()
    } else {
        let mut bytes = entries.join("\n").into_bytes();
        bytes.push(b'\n');
        bytes
    };
    let rev = catalog_rev(&body);
    let header = json!({
        "schema": "sase.footprint.catalog/1",
        "rev": rev,
        "count": entries.len() as u64,
        "sase_version": "0.18.1",
    });
    let mut text = serde_json::to_string(&header).unwrap();
    text.push('\n');
    if !body.is_empty() {
        text.push_str(std::str::from_utf8(&body).unwrap());
    }
    let path = dir.join("catalog.jsonl");
    fs::write(&path, text).unwrap();
    path
}

fn entry(id: &str, kind: &str, label: &str) -> String {
    let ns = id.split_once(':').unwrap().0;
    serde_json::to_string(&json!({
        "id": id,
        "ns": ns,
        "area": format!("{ns}.test"),
        "kind": kind,
        "label": label,
        "parent": null,
        "src": "sase/footprint/test.py:handle"
    }))
    .unwrap()
}

fn hit_line(feature: &str, extra: &str) -> String {
    format!(
        r#"{{"s":1,"d":"2026-10-26","f":"{feature}","a":"human","v":"cli","o":"ok","n":1{extra},"ver":"0.18.1"}}"#
    )
}

fn write_spool(dir: &Path, name: &str, lines: &[&str]) -> std::path::PathBuf {
    fs::create_dir_all(dir).unwrap();
    let path = dir.join(name);
    let mut file = fs::File::create(&path).unwrap();
    for line in lines {
        writeln!(file, "{line}").unwrap();
    }
    path
}

fn daily_n(conn: &rusqlite::Connection, feature_id: &str) -> i64 {
    conn.query_row(
        "SELECT COALESCE(SUM(n), 0) FROM daily WHERE feature_id = ?1",
        [feature_id],
        |row| row.get(0),
    )
    .unwrap()
}

#[test]
fn wire_schema_version_is_one() {
    assert_eq!(footprint_wire_schema_version(), 1);
    assert_eq!(FOOTPRINT_WIRE_SCHEMA_VERSION, 1);
}

#[test]
fn torn_and_partial_lines() {
    assert!(matches!(
        parse_spool_line("not-json"),
        SpoolLine::Torn { .. }
    ));
    let too_long = format!(
        r#"{{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"human","v":"cli","o":"ok","n":1,"ver":"{}","pad":"{}"}}"#,
        "0.18.1",
        "x".repeat(SPOOL_LINE_MAX_BYTES)
    );
    assert!(matches!(
        parse_spool_line(&too_long),
        SpoolLine::Torn { .. }
    ));
    let unknown_key = r#"{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"human","v":"cli","o":"ok","n":1,"ver":"0.18.1","cwd":"/tmp"}"#;
    assert!(matches!(
        parse_spool_line(unknown_key),
        SpoolLine::Torn { .. }
    ));
    let invalid_actor = r#"{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"root","v":"cli","o":"ok","n":1,"ver":"0.18.1"}"#;
    assert!(matches!(
        parse_spool_line(invalid_actor),
        SpoolLine::Torn { .. }
    ));

    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[
            &hit_line("cli:bead.list", ""),
            r#"{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"human","v":"cli","o":"ok","n":1,"ver":"0.18.1"}"#,
        ],
    );
    // Rewrite with a trailing partial line (no final newline).
    let path = spool.join("2026-10-26.jsonl");
    let mut data = fs::read(&path).unwrap();
    data.extend_from_slice(br#"{"s":1,"d":"2026-10-26","f":"cli:bead.list""#);
    fs::write(&path, data).unwrap();
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:~other"), 2);
    let offset: i64 = conn
        .query_row(
            "SELECT offset FROM fold_checkpoints WHERE file = '2026-10-26.jsonl'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(offset < fs::metadata(&path).unwrap().len() as i64);
}

#[test]
fn double_fold_equals_single_fold() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    let catalog = write_catalog(
        temp.path(),
        &[
            &entry("cli:bead.list", "command", "List beads"),
            &entry("cli:bead.list#json", "option", "JSON output"),
        ],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:bead.list", r#","ms":12,"opt":["json"]"#)],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:bead.list"), 1);
    assert_eq!(daily_n(&conn, "cli:bead.list#json"), 1);
}

#[test]
fn dropped_transaction_does_not_double_count() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("cli:bead.list", "command", "List beads")],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    let path = write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:bead.list", "")],
    );
    let mut conn = open_write_store(&store, BUSY).unwrap();
    let known = super::catalog::known_feature_ids(&conn).unwrap();
    let today = super::catalog::parse_day(TODAY).unwrap();
    {
        let tx = begin_immediate(&mut conn).unwrap();
        fold_file_in_tx(&tx, &path, today, &known).unwrap();
        drop(tx);
    }
    assert_eq!(daily_n(&conn, "cli:bead.list"), 0);
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:bead.list"), 1);
}

#[test]
fn rotated_file_resets_offset() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("cli:bead.list", "command", "List beads")],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:bead.list", "")],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let replacement = write_spool(
        &spool,
        "2026-10-26.jsonl.tmp",
        &[
            &hit_line("cli:bead.list", ""),
            &hit_line("cli:bead.list", ""),
        ],
    );
    fs::rename(replacement, spool.join("2026-10-26.jsonl")).unwrap();
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:bead.list"), 3);
}

#[test]
fn unknown_ids_become_other() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:never.heard", "")],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:~other"), 1);
    let unknown: i64 = conn
        .query_row(
            "SELECT unknown_ids FROM coverage WHERE day = '2026-10-26' AND surface = 'cli'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(unknown, 1);
}

#[test]
fn invalid_enums_are_torn() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[
            r#"{"s":1,"d":"2026-10-26","f":"cli:bead.list","a":"human","v":"telepathy","o":"ok","n":1,"ver":"0.18.1"}"#,
        ],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let snapshot = status(&store, &spool, TODAY, BUSY).unwrap();
    assert_eq!(snapshot.torn_total, 1);
    assert_eq!(snapshot.dropped_total, 0);
}

#[test]
fn drop_lines_past_32mb_within_slack_are_folded() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    fs::create_dir_all(&spool).unwrap();
    let path = spool.join("2026-10-26.jsonl");
    let mut file = fs::File::create(&path).unwrap();
    let line = br#"{"s":1,"d":"2026-10-26","drop":1,"ver":"0.18.1"}"#;
    let mut written = 0u64;
    while written + (line.len() as u64) < SPOOL_FILE_MAX_BYTES {
        file.write_all(line).unwrap();
        file.write_all(b"\n").unwrap();
        written += line.len() as u64 + 1;
    }
    writeln!(
        file,
        r#"{{"s":1,"d":"2026-10-26","drop":99,"ver":"0.18.1"}}"#
    )
    .unwrap();
    drop(file);
    assert!(fs::metadata(&path).unwrap().len() > SPOOL_FILE_MAX_BYTES);
    assert!(
        fs::metadata(&path).unwrap().len()
            < SPOOL_FILE_MAX_BYTES + SPOOL_DROP_SLACK_BYTES
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let snapshot = status(&store, &spool, TODAY, BUSY).unwrap();
    assert!(snapshot.dropped_total >= 99);
}

#[test]
fn derived_upsert_is_idempotent() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("macro:research", "macro", "Research")],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    let rows = vec![DerivedDailyRowWire {
        day: "2026-10-20".into(),
        feature_id: "macro:research".into(),
        actor: "human".into(),
        via: "prompt".into(),
        outcome: "ok".into(),
        n: 4,
        dur_ms_sum: 0,
        dur_ms_max: 0,
    }];
    let coverage = vec![DerivedCoverageRowWire {
        day: "2026-10-20".into(),
        surface: "macro".into(),
        measured: 1,
        human_active: 1,
        records: 4,
        torn: 0,
        dropped: 0,
        unknown_ids: 0,
        incomplete: 0,
    }];
    upsert_derived(
        &store,
        "derived.macros",
        &["2026-10-20".into()],
        &rows,
        &coverage,
        BUSY,
    )
    .unwrap();
    upsert_derived(
        &store,
        "derived.macros",
        &["2026-10-20".into()],
        &rows,
        &coverage,
        BUSY,
    )
    .unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "macro:research"), 4);
}

#[test]
fn derived_config_via_is_accepted() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("config:ace.page_size", "config", "Page size")],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    upsert_derived(
        &store,
        "derived.config",
        &["2026-10-27".into()],
        &[DerivedDailyRowWire {
            day: "2026-10-27".into(),
            feature_id: "config:ace.page_size".into(),
            actor: "human".into(),
            via: "config".into(),
            outcome: "ok".into(),
            n: 1,
            dur_ms_sum: 0,
            dur_ms_max: 0,
        }],
        &[DerivedCoverageRowWire {
            day: "2026-10-27".into(),
            surface: "config".into(),
            measured: 1,
            human_active: 1,
            records: 1,
            torn: 0,
            dropped: 0,
            unknown_ids: 0,
            incomplete: 0,
        }],
        BUSY,
    )
    .unwrap();
    let conn = open_write_store(&store, BUSY).unwrap();
    assert_eq!(daily_n(&conn, "config:ace.page_size"), 1);
}

#[test]
fn prune_and_clear() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("cli:bead.list", "command", "List beads")],
    );
    ingest_catalog(
        &store,
        &catalog,
        &BTreeMap::from([("cli:bead.list".into(), 12)]),
        TODAY,
        BUSY,
    )
    .unwrap();
    write_spool(
        &spool,
        "2026-01-01.jsonl",
        &[&hit_line("cli:bead.list", "").replace("2026-10-26", "2026-01-01")],
    );
    fold(&store, &spool, TODAY, 400, 400, BUSY).unwrap();
    let pruned = prune(&store, TODAY, 30, BUSY).unwrap();
    assert_eq!(pruned.daily_deleted, 1);
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:bead.list", "")],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    let cleared = clear(&store, &spool, BUSY).unwrap();
    assert_eq!(cleared.generation, 1);
    assert!(!spool.join("2026-10-26.jsonl").exists());
    let snapshot = status(&store, &spool, TODAY, BUSY).unwrap();
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.catalog_count, 0);
}

#[test]
fn concurrent_folds_do_not_double_count() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("cli:bead.list", "command", "List beads")],
    );
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    write_spool(
        &spool,
        "2026-10-26.jsonl",
        &[&hit_line("cli:bead.list", "")],
    );
    let store = Arc::new(store);
    let spool = Arc::new(spool);
    let mut joins = Vec::new();
    for _ in 0..2 {
        let store = Arc::clone(&store);
        let spool = Arc::clone(&spool);
        joins.push(thread::spawn(move || {
            fold(store.as_path(), spool.as_path(), TODAY, 7, 30, BUSY)
        }));
    }
    for join in joins {
        join.join().unwrap().unwrap();
    }
    let conn = open_write_store(store.as_path(), BUSY).unwrap();
    assert_eq!(daily_n(&conn, "cli:bead.list"), 1);
}

#[test]
fn catalog_status_and_overlay_shape() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let catalog = write_catalog(
        temp.path(),
        &[&entry("cli:bead.list", "command", "List beads")],
    );
    let parsed = parse_catalog(&catalog).unwrap();
    assert_eq!(parsed.header.count, 1);
    let status = catalog_status(&store, &catalog, BUSY).unwrap();
    assert!(!status.known);
    ingest_catalog(&store, &catalog, &BTreeMap::new(), TODAY, BUSY).unwrap();
    let status = catalog_status(&store, &catalog, BUSY).unwrap();
    assert!(status.known);

    validate_overlay_shape(&json!({
        "schema": "sase.footprint.overlay/1",
        "features": {
            "cli:doctor*": {
                "protected": "recovery",
                "why": "rare by design"
            },
            "cli:bead.list": {
                "keep": "agent_facing",
                "why": "skills parse this"
            }
        },
        "retire_with": [["cli:bead.list", "tui:app.bead_list"]]
    }))
    .unwrap();
    assert!(validate_overlay_shape(&json!({
        "schema": "sase.footprint.overlay/1",
        "features": {"cli:x": {"pid": 1}}
    }))
    .is_err());
}

#[test]
fn catalog_rev_is_first_twelve_hex_of_sha256() {
    let bytes = b"{\"id\":\"cli:x\"}\n";
    let digest = Sha256::digest(bytes);
    assert_eq!(catalog_rev(bytes), hex::encode(&digest[..6]));
}

#[test]
fn complete_old_spool_files_are_deleted() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    write_spool(
        &spool,
        "2026-10-01.jsonl",
        &[&hit_line("cli:bead.list", "").replace("2026-10-26", "2026-10-01")],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    assert!(!spool.join("2026-10-01.jsonl").exists());
}

#[test]
fn expired_spool_marks_coverage_incomplete() {
    let temp = tempdir().unwrap();
    let store = temp.path().join("footprint.sqlite");
    let spool = temp.path().join("spool");
    write_spool(
        &spool,
        "2026-01-01.jsonl",
        &[&hit_line("cli:bead.list", "").replace("2026-10-26", "2026-01-01")],
    );
    fold(&store, &spool, TODAY, 7, 30, BUSY).unwrap();
    assert!(!spool.join("2026-01-01.jsonl").exists());
    let conn = open_write_store(&store, BUSY).unwrap();
    let incomplete: i64 = conn
        .query_row(
            "SELECT incomplete FROM coverage WHERE day = '2026-01-01' AND surface = 'cli'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(incomplete, 1);
}

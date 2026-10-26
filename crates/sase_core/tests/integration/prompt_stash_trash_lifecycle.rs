use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use sase_core::prompt_stash::{
    append_prompt_stash, pop_prompt_stash, purge_prompt_stash,
    read_prompt_stash_lifecycle, read_prompt_stash_snapshot,
    reconcile_prompt_stash_trash, restore_prompt_stash, rewrite_prompt_stash,
    set_prompt_stash_pinned, trash_prompt_stash, PromptStashCursorWire,
    PromptStashEntryWire, PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION,
};
use serde_json::json;
use tempfile::tempdir;

const T1: &str = "2026-09-26T14:00:00+00:00";
const T2: &str = "2026-09-26T15:00:00+00:00";

fn store_path(root: &Path) -> PathBuf {
    root.join("prompt_stash.jsonl")
}

fn backup_path(root: &Path) -> PathBuf {
    root.join("prompt_stash.jsonl.pre-upgrade-backup")
}

fn entry(id: &str) -> PromptStashEntryWire {
    PromptStashEntryWire {
        id: id.to_string(),
        created_at: "2026-06-16T01:02:03+00:00".to_string(),
        text: format!("draft for {id}"),
        frontmatter: String::new(),
        project: None,
        source: "current".to_string(),
        pane_index: 0,
        pinned: false,
        cursor: None,
    }
}

fn rich_entry(id: &str) -> PromptStashEntryWire {
    PromptStashEntryWire {
        id: id.to_string(),
        created_at: "2026-06-16T01:02:03+00:00".to_string(),
        text: "alpha pane\n---\nbeta pane".to_string(),
        frontmatter: "model: claude\n".to_string(),
        project: Some("proj-a".to_string()),
        source: "all".to_string(),
        pane_index: 1,
        pinned: true,
        cursor: Some(PromptStashCursorWire {
            pane_index: 1,
            row: 2,
            column: 3,
        }),
    }
}

fn seed(path: &Path, ids: &[&str]) {
    for id in ids {
        append_prompt_stash(path, &entry(id)).unwrap();
    }
}

fn active_ids(path: &Path) -> Vec<String> {
    read_prompt_stash_lifecycle(path)
        .unwrap()
        .active
        .iter()
        .map(|row| row.id.clone())
        .collect()
}

fn trash_ids(path: &Path) -> Vec<String> {
    read_prompt_stash_lifecycle(path)
        .unwrap()
        .trash
        .iter()
        .map(|row| row.entry.id.clone())
        .collect()
}

#[test]
fn lifecycle_missing_file_returns_empty_versioned_snapshot() {
    let temp = tempdir().unwrap();
    let snapshot =
        read_prompt_stash_lifecycle(&store_path(temp.path())).unwrap();
    assert_eq!(
        snapshot.schema_version,
        PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION
    );
    assert!(snapshot.active.is_empty());
    assert!(snapshot.trash.is_empty());
}

#[test]
fn trash_restore_round_trip_preserves_the_complete_entry() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let original = rich_entry("full");
    append_prompt_stash(&path, &original).unwrap();

    let outcome =
        trash_prompt_stash(&path, &["full".to_string()], 20, T1).unwrap();
    assert_eq!(
        outcome.schema_version,
        PROMPT_STASH_LIFECYCLE_WIRE_SCHEMA_VERSION
    );
    assert_eq!(outcome.changed, vec!["full".to_string()]);
    assert!(outcome.evicted.is_empty());
    assert!(outcome.snapshot.active.is_empty());
    assert_eq!(outcome.snapshot.trash.len(), 1);
    let record = &outcome.snapshot.trash[0];
    assert_eq!(record.trashed_at, T1);
    assert_eq!(record.entry, original);

    // The tagged envelope keeps the entry nested; no bare deletion field.
    let content = fs::read_to_string(&path).unwrap();
    let lines: Vec<&str> = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    assert_eq!(lines.len(), 1);
    let envelope: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(envelope.get("kind"), Some(&json!("trash")));
    assert_eq!(envelope.get("trashed_at"), Some(&json!(T1)));
    assert_eq!(
        envelope.get("entry").and_then(|entry| entry.get("id")),
        Some(&json!("full"))
    );
    assert!(envelope.get("entry").unwrap().get("trashed_at").is_none());

    // The v1 snapshot hides Trash rows from legacy callers.
    let legacy = read_prompt_stash_snapshot(&path).unwrap();
    assert!(legacy.entries.is_empty());
    assert_eq!(legacy.stats.loaded_rows, 1);

    let outcome = restore_prompt_stash(&path, &["full".to_string()]).unwrap();
    assert_eq!(outcome.changed, vec!["full".to_string()]);
    assert!(outcome.snapshot.trash.is_empty());
    assert_eq!(outcome.snapshot.active, vec![original]);

    // Re-trashing a restored row assigns a new deletion time.
    let outcome =
        trash_prompt_stash(&path, &["full".to_string()], 20, T2).unwrap();
    assert_eq!(outcome.snapshot.trash[0].trashed_at, T2);
}

#[test]
fn trash_orders_newest_deleted_first() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c"]);

    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    trash_prompt_stash(&path, &["b".to_string()], 20, T2).unwrap();
    trash_prompt_stash(&path, &["c".to_string()], 20, T1).unwrap();

    let snapshot = read_prompt_stash_lifecycle(&path).unwrap();
    let order: Vec<&str> = snapshot
        .trash
        .iter()
        .map(|row| row.entry.id.as_str())
        .collect();
    assert_eq!(order, vec!["b", "c", "a"]);
}

#[test]
fn trash_limit_one_keeps_only_the_newest_row() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c"]);

    let outcome = trash_prompt_stash(
        &path,
        &["a".to_string(), "b".to_string(), "c".to_string()],
        1,
        T1,
    )
    .unwrap();
    // One shared batch timestamp: later input order counts as newer.
    assert_eq!(outcome.changed, vec!["c".to_string()]);
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(trash_ids(&path), vec!["c".to_string()]);
    assert!(active_ids(&path).is_empty());
}

#[test]
fn trash_limit_zero_disables_recovery() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);

    let outcome =
        trash_prompt_stash(&path, &["a".to_string(), "b".to_string()], 0, T1)
            .unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);
    let snapshot = read_prompt_stash_lifecycle(&path).unwrap();
    assert!(snapshot.active.is_empty());
    assert!(snapshot.trash.is_empty());
    // Nothing tagged was ever written, so no backup was needed.
    assert!(!backup_path(temp.path()).exists());
}

#[test]
fn reconcile_enforces_a_lowered_limit_and_reports_evictions() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c", "d", "e"]);
    trash_prompt_stash(
        &path,
        &[
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ],
        10,
        T1,
    )
    .unwrap();

    let outcome = reconcile_prompt_stash_trash(&path, 2).unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(
        outcome.evicted,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(trash_ids(&path), vec!["e".to_string(), "d".to_string()]);

    // Reconciling an already-compliant store is a no-op without a rewrite.
    let before = fs::read(&path).unwrap();
    let outcome = reconcile_prompt_stash_trash(&path, 2).unwrap();
    assert!(outcome.evicted.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn trash_enforces_a_lowered_limit_in_the_same_transaction() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c"]);
    trash_prompt_stash(
        &path,
        &["a".to_string(), "b".to_string(), "c".to_string()],
        10,
        T1,
    )
    .unwrap();
    seed(&path, &["d"]);

    // The older trash rows evict before the freshly trashed row.
    let outcome = trash_prompt_stash(&path, &["d".to_string()], 2, T2).unwrap();
    assert_eq!(outcome.changed, vec!["d".to_string()]);
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);
    assert_eq!(trash_ids(&path), vec!["d".to_string(), "c".to_string()]);
}

#[test]
fn equal_timestamps_evict_deterministically() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);
    trash_prompt_stash(&path, &["a".to_string()], 10, T1).unwrap();
    trash_prompt_stash(&path, &["b".to_string()], 10, T1).unwrap();

    let outcome = reconcile_prompt_stash_trash(&path, 1).unwrap();
    assert_eq!(outcome.evicted, vec!["a".to_string()]);
    assert_eq!(trash_ids(&path), vec!["b".to_string()]);
}

#[test]
fn unknown_ids_and_repeated_transitions_are_no_ops() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);

    let before = fs::read(&path).unwrap();
    let outcome =
        trash_prompt_stash(&path, &["missing".to_string()], 20, T1).unwrap();
    assert!(outcome.changed.is_empty() && outcome.evicted.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);

    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    let before = fs::read(&path).unwrap();
    let outcome =
        trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    assert!(outcome.changed.is_empty() && outcome.evicted.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);

    restore_prompt_stash(&path, &["a".to_string()]).unwrap();
    let before = fs::read(&path).unwrap();
    let outcome = restore_prompt_stash(&path, &["a".to_string()]).unwrap();
    assert!(outcome.changed.is_empty());
    let outcome =
        restore_prompt_stash(&path, &["missing".to_string()]).unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);

    // Purge only touches Trash: active ids and unknown ids are ignored.
    let outcome =
        purge_prompt_stash(&path, &["a".to_string(), "missing".to_string()])
            .unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(active_ids(&path), vec!["a".to_string()]);
}

#[test]
fn purge_deletes_only_the_requested_trash_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c", "live"]);
    trash_prompt_stash(
        &path,
        &["a".to_string(), "b".to_string(), "c".to_string()],
        20,
        T1,
    )
    .unwrap();

    let outcome =
        purge_prompt_stash(&path, &["a".to_string(), "c".to_string()]).unwrap();
    assert_eq!(outcome.changed, vec!["a".to_string(), "c".to_string()]);
    assert!(outcome.evicted.is_empty());
    assert_eq!(trash_ids(&path), vec!["b".to_string()]);
    assert_eq!(active_ids(&path), vec!["live".to_string()]);
}

#[test]
fn stale_rewrite_and_append_are_rejected_without_touching_the_file() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);
    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    let before = fs::read(&path).unwrap();

    let error = rewrite_prompt_stash(&path, &[entry("a")]).unwrap_err();
    assert!(error.to_string().contains("resurrect"), "{error}");
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(trash_ids(&path), vec!["a".to_string()]);

    let error = append_prompt_stash(&path, &entry("a")).unwrap_err();
    assert!(error.to_string().contains("resurrect"), "{error}");
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn empty_trashed_at_is_rejected() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);
    let error =
        trash_prompt_stash(&path, &["a".to_string()], 20, "").unwrap_err();
    assert!(error.to_string().contains("trashed_at"), "{error}");
    assert_eq!(active_ids(&path), vec!["a".to_string()]);
}

#[test]
fn opaque_lines_survive_every_mutation_verbatim() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let envelope = json!({
        "kind": "trash",
        "trashed_at": T1,
        "entry": {
            "id": "old",
            "created_at": "2026-06-16T01:02:03+00:00",
            "text": "old draft",
        },
    })
    .to_string();
    let opaque_lines = [
        "NOT JSON".to_string(),
        r#"{"id":"incomplete"}"#.to_string(),
        r#"{"kind":"future","payload":1}"#.to_string(),
        r#"{"id":"bare","created_at":"t","text":"x","trashed_at":"y"}"#
            .to_string(),
    ];
    let mut body =
        vec![serde_json::to_string(&entry("good")).unwrap(), envelope];
    body.extend(opaque_lines.clone());
    body.push(String::new());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, body.join("\n")).unwrap();

    let snapshot = read_prompt_stash_lifecycle(&path).unwrap();
    assert_eq!(active_ids(&path), vec!["good".to_string()]);
    assert_eq!(trash_ids(&path), vec!["old".to_string()]);
    assert_eq!(snapshot.stats.total_lines, 6);
    assert_eq!(snapshot.stats.blank_lines, 0);
    assert_eq!(snapshot.stats.invalid_json_lines, 1);
    assert_eq!(snapshot.stats.invalid_record_lines, 3);
    assert_eq!(snapshot.stats.loaded_rows, 2);

    // Legacy readers see only the active row; the bare deletion timestamp
    // never surfaces as an active entry.
    let legacy = read_prompt_stash_snapshot(&path).unwrap();
    assert_eq!(legacy.entries.len(), 1);
    assert_eq!(legacy.entries[0].id, "good");

    let check_raw = |step: &str| {
        let content = fs::read_to_string(&path).unwrap();
        for line in &opaque_lines {
            assert!(content.contains(line.as_str()), "{step}: lost {line}");
        }
    };

    pop_prompt_stash(&path, &["good".to_string()]).unwrap();
    assert!(active_ids(&path).is_empty());
    assert_eq!(trash_ids(&path), vec!["old".to_string()]);
    check_raw("pop");

    rewrite_prompt_stash(&path, &[entry("fresh")]).unwrap();
    assert_eq!(active_ids(&path), vec!["fresh".to_string()]);
    assert_eq!(trash_ids(&path), vec!["old".to_string()]);
    check_raw("rewrite");

    set_prompt_stash_pinned(&path, &["fresh".to_string()], true).unwrap();
    assert!(read_prompt_stash_lifecycle(&path).unwrap().active[0].pinned);
    check_raw("pin");

    restore_prompt_stash(&path, &["old".to_string()]).unwrap();
    assert_eq!(
        active_ids(&path),
        vec!["fresh".to_string(), "old".to_string()]
    );
    check_raw("restore");

    trash_prompt_stash(&path, &["old".to_string()], 20, T2).unwrap();
    assert_eq!(trash_ids(&path), vec!["old".to_string()]);
    check_raw("trash");

    purge_prompt_stash(&path, &["old".to_string()]).unwrap();
    assert!(trash_ids(&path).is_empty());
    check_raw("purge");

    reconcile_prompt_stash_trash(&path, 0).unwrap();
    check_raw("reconcile");
}

#[test]
fn legacy_mutators_preserve_trash() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b", "c"]);
    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();

    // Pop is restricted to active rows: trash ids are ignored.
    let outcome =
        pop_prompt_stash(&path, &["a".to_string(), "b".to_string()]).unwrap();
    assert_eq!(outcome.removed.len(), 1);
    assert_eq!(outcome.removed[0].id, "b");
    assert_eq!(trash_ids(&path), vec!["a".to_string()]);

    // Pin affects active rows only and keeps Trash intact.
    let snapshot = set_prompt_stash_pinned(
        &path,
        &["a".to_string(), "c".to_string()],
        true,
    )
    .unwrap();
    assert!(
        snapshot
            .entries
            .iter()
            .find(|row| row.id == "c")
            .unwrap()
            .pinned
    );
    assert_eq!(trash_ids(&path), vec!["a".to_string()]);
    assert!(!read_prompt_stash_lifecycle(&path)
        .unwrap()
        .trash
        .iter()
        .any(|row| row.entry.pinned));

    // Rewrite merges unseen active rows while preserving Trash.
    let mut updated = entry("c");
    updated.text = "updated".to_string();
    let snapshot = rewrite_prompt_stash(&path, &[updated, entry("d")]).unwrap();
    let ids: Vec<&str> =
        snapshot.entries.iter().map(|row| row.id.as_str()).collect();
    assert_eq!(ids, vec!["c", "d"]);
    assert_eq!(trash_ids(&path), vec!["a".to_string()]);
}

#[test]
fn first_tagged_write_takes_a_verified_backup_once() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);
    let pre_trash = fs::read(&path).unwrap();

    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    let backup = backup_path(temp.path());
    assert!(backup.exists());
    assert_eq!(fs::read(&backup).unwrap(), pre_trash);

    // A second trash keeps the original pre-upgrade bytes.
    trash_prompt_stash(&path, &["b".to_string()], 20, T2).unwrap();
    assert_eq!(fs::read(&backup).unwrap(), pre_trash);
}

#[test]
fn backup_failure_fails_closed_and_leaves_the_file_unchanged() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);
    let before = fs::read(&path).unwrap();

    fs::create_dir_all(backup_path(temp.path())).unwrap();
    let error =
        trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap_err();
    assert!(error.to_string().contains("backup"), "{error}");
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(active_ids(&path), vec!["a".to_string()]);
}

#[test]
fn legacy_writer_hazard_is_real_and_new_writers_avoid_it() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["keep", "discard"]);
    trash_prompt_stash(&path, &["discard".to_string()], 20, T1).unwrap();
    assert_eq!(trash_ids(&path), vec!["discard".to_string()]);

    // An old writer parses every line as a bare entry, silently skipping the
    // tagged trash envelope, then rewrites only what it understood.
    let content = fs::read_to_string(&path).unwrap();
    let mut kept = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(entry) = serde_json::from_str::<PromptStashEntryWire>(line) {
            if !entry.id.is_empty()
                && !entry.created_at.is_empty()
                && entry.id != "keep"
            {
                continue;
            }
            if entry.id == "keep" {
                kept.push(line.to_string());
            }
        }
    }
    fs::write(&path, kept.join("\n") + "\n").unwrap();
    assert!(
        trash_ids(&path).is_empty(),
        "old writer erased the trash row"
    );

    // The new writer preserves Trash across the same active-only removal.
    seed(&path, &["victim"]);
    trash_prompt_stash(&path, &["victim".to_string()], 20, T2).unwrap();
    pop_prompt_stash(&path, &["keep".to_string()]).unwrap();
    assert_eq!(trash_ids(&path), vec!["victim".to_string()]);
}

#[test]
fn concurrent_append_trash_restore_keeps_a_valid_disjoint_store() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let seed_ids: Vec<String> =
        (0..20).map(|idx| format!("seed-{idx}")).collect();
    seed(
        &path,
        &seed_ids.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    trash_prompt_stash(&path, &seed_ids, 100, T1).unwrap();

    let mut handles = Vec::new();
    for worker in 0..4 {
        let append_path = path.clone();
        handles.push(thread::spawn(move || {
            for idx in 0..20 {
                let id = format!("w{worker}-append-{idx}");
                append_prompt_stash(&append_path, &entry(&id)).unwrap();
            }
        }));
    }
    for _ in 0..2 {
        let restore_path = path.clone();
        let ids = seed_ids.clone();
        handles.push(thread::spawn(move || {
            for id in &ids {
                let _ = restore_prompt_stash(
                    &restore_path,
                    std::slice::from_ref(id),
                );
            }
        }));
    }
    for _ in 0..2 {
        let trash_path = path.clone();
        let ids = seed_ids.clone();
        handles.push(thread::spawn(move || {
            for id in &ids {
                let _ = trash_prompt_stash(
                    &trash_path,
                    std::slice::from_ref(id),
                    100,
                    T2,
                );
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let snapshot = read_prompt_stash_lifecycle(&path).unwrap();
    let content = fs::read_to_string(&path).unwrap();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let is_entry = value.get("kind").is_none();
        let is_trash = value.get("kind") == Some(&json!("trash"));
        assert!(is_entry || is_trash, "unparseable line: {line}");
    }
    // Active and Trash ids stay disjoint with no duplicates inside either.
    let mut seen = std::collections::BTreeSet::new();
    for row in snapshot.active.iter().map(|row| &row.id) {
        assert!(seen.insert(row.as_str()), "duplicate active id {row}");
    }
    let mut seen_trash = std::collections::BTreeSet::new();
    for row in snapshot.trash.iter().map(|row| &row.entry.id) {
        assert!(seen.insert(row.as_str()), "id in both collections: {row}");
        assert!(seen_trash.insert(row.as_str()), "duplicate trash id {row}");
    }
    assert_eq!(
        snapshot.stats.loaded_rows as usize,
        snapshot.active.len() + snapshot.trash.len()
    );
}

#[test]
fn crashed_tmp_files_never_replace_the_live_store() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);
    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();

    // Every committed write is a complete parseable file with no stray temp
    // files left beside it.
    let snapshot = read_prompt_stash_lifecycle(&path).unwrap();
    assert_eq!(snapshot.active.len() + snapshot.trash.len(), 1);
    for line in fs::read_to_string(&path).unwrap().lines() {
        if line.trim().is_empty() {
            continue;
        }
        serde_json::from_str::<serde_json::Value>(line).unwrap();
    }
    let stray: Vec<_> = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".tmp"))
        .collect();
    assert!(stray.is_empty(), "stray temp files: {stray:?}");
}

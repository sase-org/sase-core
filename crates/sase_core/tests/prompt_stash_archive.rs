use std::fs;
use std::path::{Path, PathBuf};

use sase_core::prompt_stash::{
    append_prompt_stash, archive_path_for_prompt_stash, pop_prompt_stash,
    purge_prompt_stash, read_prompt_stash_archive, read_prompt_stash_lifecycle,
    reconcile_prompt_stash_trash, recover_prompt_stash_archive,
    rewrite_prompt_stash, trash_prompt_stash, PromptStashArchiveReason,
    PromptStashEntryWire, PROMPT_STASH_ARCHIVE_WIRE_SCHEMA_VERSION,
};
use tempfile::tempdir;

const T1: &str = "2026-09-26T14:00:00+00:00";

fn store_path(root: &Path) -> PathBuf {
    root.join("prompt_stash.jsonl")
}

fn archive_path(root: &Path) -> PathBuf {
    root.join("prompt_stash_archive.jsonl")
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
        cursor: Some(sase_core::prompt_stash::PromptStashCursorWire {
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

#[test]
fn archive_path_derives_from_the_stash_path() {
    let temp = tempdir().unwrap();
    let stash = store_path(temp.path());
    assert_eq!(
        archive_path_for_prompt_stash(&stash),
        archive_path(temp.path())
    );
    let nested = temp.path().join("nested").join("prompt_stash.jsonl");
    assert_eq!(
        archive_path_for_prompt_stash(&nested),
        temp.path()
            .join("nested")
            .join("prompt_stash_archive.jsonl")
    );
}

#[test]
fn missing_archive_reads_as_empty() {
    let temp = tempdir().unwrap();
    let snapshot =
        read_prompt_stash_archive(&store_path(temp.path()), 20).unwrap();
    assert_eq!(
        snapshot.schema_version,
        PROMPT_STASH_ARCHIVE_WIRE_SCHEMA_VERSION
    );
    assert!(snapshot.records.is_empty());
    assert_eq!(snapshot.stats.total_lines, 0);
}

#[test]
fn pop_archives_every_removed_row_as_popped() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let original = rich_entry("gone");
    append_prompt_stash(&path, &original).unwrap();
    append_prompt_stash(&path, &entry("kept")).unwrap();

    let outcome = pop_prompt_stash(&path, &["gone".to_string()]).unwrap();
    assert_eq!(outcome.removed, vec![original.clone()]);

    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 1);
    let record = &snapshot.records[0];
    assert_eq!(record.kind, "archived");
    assert_eq!(record.reason, PromptStashArchiveReason::Popped);
    assert_eq!(record.entry, original);
    assert!(record.trashed_at.is_none());
    assert!(!record.archived_at.is_empty());
    assert!(chrono::DateTime::parse_from_rfc3339(&record.archived_at).is_ok());
    assert_eq!(snapshot.stats.loaded_rows, 1);

    // The archive is a sibling file; the stash no longer holds the row.
    assert!(archive_path(temp.path()).exists());
    let active: Vec<String> = read_prompt_stash_lifecycle(&path)
        .unwrap()
        .active
        .iter()
        .map(|row| row.id.clone())
        .collect();
    assert_eq!(active, vec!["kept".to_string()]);
}

#[test]
fn purge_archives_every_purged_row_with_its_deletion_time() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    let original = rich_entry("doomed");
    append_prompt_stash(&path, &original).unwrap();
    trash_prompt_stash(&path, &["doomed".to_string()], 20, T1).unwrap();

    let outcome = purge_prompt_stash(&path, &["doomed".to_string()]).unwrap();
    assert_eq!(outcome.changed, vec!["doomed".to_string()]);

    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 1);
    let record = &snapshot.records[0];
    assert_eq!(record.reason, PromptStashArchiveReason::Purged);
    assert_eq!(record.entry, original);
    assert_eq!(record.trashed_at.as_deref(), Some(T1));
}

#[test]
fn trash_limit_eviction_archives_as_evicted() {
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
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);

    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 2);
    for record in &snapshot.records {
        assert_eq!(record.reason, PromptStashArchiveReason::Evicted);
        assert_eq!(record.trashed_at.as_deref(), Some(T1));
    }
    let mut ids: Vec<&str> = snapshot
        .records
        .iter()
        .map(|record| record.entry.id.as_str())
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn trash_limit_zero_still_archives_evictions() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);

    let outcome =
        trash_prompt_stash(&path, &["a".to_string(), "b".to_string()], 0, T1)
            .unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);

    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 2);
    assert!(snapshot
        .records
        .iter()
        .all(|record| record.reason == PromptStashArchiveReason::Evicted));
}

#[test]
fn reconcile_eviction_archives_as_evicted() {
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
    assert_eq!(
        read_prompt_stash_archive(&path, 20).unwrap().records.len(),
        0
    );

    let outcome = reconcile_prompt_stash_trash(&path, 1).unwrap();
    assert_eq!(outcome.evicted, vec!["a".to_string(), "b".to_string()]);

    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 2);
    assert!(snapshot
        .records
        .iter()
        .all(|record| record.reason == PromptStashArchiveReason::Evicted));
}

#[test]
fn rewrite_archives_only_text_frontmatter_or_cursor_replacements() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "b"]);

    // Text change archives the previous version.
    let mut updated = entry("a");
    updated.text = "new text".to_string();
    rewrite_prompt_stash(&path, &[updated]).unwrap();
    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 1);
    assert_eq!(
        snapshot.records[0].reason,
        PromptStashArchiveReason::Overwritten
    );
    assert_eq!(snapshot.records[0].entry.id, "a");
    assert_eq!(snapshot.records[0].entry.text, "draft for a");
    assert!(snapshot.records[0].trashed_at.is_none());

    // A pin-only change archives nothing.
    let before = fs::read(archive_path(temp.path())).unwrap();
    let mut pinned_only = entry("b");
    // entry("b") matches disk except pinned; set pinned via rewrite input.
    pinned_only.pinned = true;
    // Disk row "b" is unpinned with default text/frontmatter/cursor, so only
    // pinned differs: no archive.
    rewrite_prompt_stash(&path, &[pinned_only]).unwrap();
    // "b" input also carries identical text/frontmatter/cursor, so the only
    // other candidate "a" is merge-preserved, not archived.
    assert_eq!(fs::read(archive_path(temp.path())).unwrap(), before);

    // Frontmatter and cursor changes archive.
    let mut fm = entry("b");
    fm.pinned = true;
    fm.frontmatter = "model: x\n".to_string();
    rewrite_prompt_stash(&path, &[fm]).unwrap();
    let snapshot = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(snapshot.records.len(), 2);
    assert_eq!(snapshot.records[0].entry.id, "b");
    assert_eq!(
        snapshot.records[0].reason,
        PromptStashArchiveReason::Overwritten
    );
}

#[test]
fn archive_append_failure_leaves_the_stash_untouched() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);
    let before = fs::read(&path).unwrap();

    // Make the archive path unwritable by replacing it with a directory.
    fs::create_dir_all(archive_path(temp.path())).unwrap();
    let error = pop_prompt_stash(&path, &["a".to_string()]).unwrap_err();
    assert!(error.to_string().contains("archive"), "{error}");
    assert_eq!(fs::read(&path).unwrap(), before);
    let active: Vec<String> = read_prompt_stash_lifecycle(&path)
        .unwrap()
        .active
        .iter()
        .map(|row| row.id.clone())
        .collect();
    assert_eq!(active, vec!["a".to_string()]);
}

#[test]
fn recovery_round_trips_and_skips_active_trashed_and_unknown_ids() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a", "live", "trashed"]);
    trash_prompt_stash(&path, &["trashed".to_string()], 20, T1).unwrap();
    pop_prompt_stash(&path, &["a".to_string()]).unwrap();

    // Unknown ids and ids already active or in Trash are skipped.
    let outcome = recover_prompt_stash_archive(
        &path,
        &[
            "missing".to_string(),
            "live".to_string(),
            "trashed".to_string(),
        ],
    )
    .unwrap();
    assert!(outcome.changed.is_empty());

    let outcome =
        recover_prompt_stash_archive(&path, &["a".to_string()]).unwrap();
    assert_eq!(outcome.changed, vec!["a".to_string()]);
    let active: Vec<String> = read_prompt_stash_lifecycle(&path)
        .unwrap()
        .active
        .iter()
        .map(|row| row.id.clone())
        .collect();
    assert!(active.contains(&"a".to_string()));

    // Recovery is append-only: the archive lines stay put.
    let archived = read_prompt_stash_archive(&path, 20).unwrap();
    assert!(archived.records.iter().any(|record| record.entry.id == "a"));

    // Recovering an already-active id is a no-op.
    let before = fs::read(&path).unwrap();
    let outcome =
        recover_prompt_stash_archive(&path, &["a".to_string()]).unwrap();
    assert!(outcome.changed.is_empty());
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn recovery_restores_the_newest_archived_version() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_prompt_stash(&path, &entry("a")).unwrap();

    let mut v2 = entry("a");
    v2.text = "v2 text".to_string();
    rewrite_prompt_stash(&path, &[v2.clone()]).unwrap();
    pop_prompt_stash(&path, &["a".to_string()]).unwrap();

    // Archive holds v1 (overwritten) then v2 (popped); newest wins.
    let archived = read_prompt_stash_archive(&path, 20).unwrap();
    assert_eq!(archived.records.len(), 2);

    let outcome =
        recover_prompt_stash_archive(&path, &["a".to_string()]).unwrap();
    assert_eq!(outcome.changed, vec!["a".to_string()]);
    assert_eq!(outcome.snapshot.active[0].text, "v2 text");
}

#[test]
fn archive_reader_tolerates_malformed_lines_and_limits_newest_first() {
    let temp = tempdir().unwrap();
    let stash = store_path(temp.path());
    let archive = archive_path(temp.path());
    seed(&stash, &["x"]);
    pop_prompt_stash(&stash, &["x".to_string()]).unwrap();

    let mut body = fs::read_to_string(&archive).unwrap();
    body.push('\n');
    body.push_str("NOT JSON\n");
    body.push_str("{\"kind\":\"archived\",\"bogus\":1}\n");
    body.push_str("{\"kind\":\"future\",\"archived_at\":\"t\"}\n");
    fs::write(&archive, body).unwrap();

    let snapshot = read_prompt_stash_archive(&stash, 20).unwrap();
    assert_eq!(snapshot.stats.total_lines, 5);
    assert_eq!(snapshot.stats.blank_lines, 1);
    assert_eq!(snapshot.stats.invalid_json_lines, 1);
    assert_eq!(snapshot.stats.invalid_record_lines, 2);
    assert_eq!(snapshot.stats.loaded_rows, 1);
    assert_eq!(snapshot.records.len(), 1);

    // Limit truncates newest-first but stats still describe the whole file.
    seed(&stash, &["y", "z"]);
    pop_prompt_stash(&stash, &["y".to_string(), "z".to_string()]).unwrap();
    let full = read_prompt_stash_archive(&stash, 100).unwrap();
    assert_eq!(full.stats.loaded_rows, 3);
    assert_eq!(full.records.len(), 3);
    let limited = read_prompt_stash_archive(&stash, 1).unwrap();
    assert_eq!(limited.records.len(), 1);
    assert_eq!(limited.records[0], full.records[0]);
    assert_eq!(limited.stats.loaded_rows, 3);
    // Newest-first: the last-popped row comes first.
    assert_eq!(limited.records[0].entry.id, "z");
}

#[test]
fn no_archive_file_is_created_without_a_permanent_removal() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    seed(&path, &["a"]);
    assert!(!archive_path(temp.path()).exists());

    trash_prompt_stash(&path, &["a".to_string()], 20, T1).unwrap();
    // Moving to Trash is recoverable without the archive.
    assert!(!archive_path(temp.path()).exists());

    // Pin changes never archive.
    append_prompt_stash(&path, &entry("b")).unwrap();
    sase_core::prompt_stash::set_prompt_stash_pinned(
        &path,
        &["b".to_string()],
        true,
    )
    .unwrap();
    assert!(!archive_path(temp.path()).exists());
}

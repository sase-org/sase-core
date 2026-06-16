use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use sase_core::prompt_stash::{
    append_prompt_stash, pop_prompt_stash, read_prompt_stash_snapshot,
    rewrite_prompt_stash, PromptStashEntryWire,
};
use serde_json::json;
use tempfile::tempdir;

fn store_path(root: &Path) -> PathBuf {
    root.join("nested").join("prompt_stash.jsonl")
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
    }
}

#[test]
fn prompt_stash_missing_file_returns_empty_snapshot() {
    let temp = tempdir().unwrap();
    let snapshot =
        read_prompt_stash_snapshot(&store_path(temp.path())).unwrap();
    assert!(snapshot.entries.is_empty());
    assert_eq!(snapshot.stats.loaded_rows, 0);
}

#[test]
fn prompt_stash_append_and_read_round_trip() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());

    let mut first = entry("one");
    first.project = Some("proj-a".to_string());
    first.frontmatter = "model: claude\n".to_string();
    first.pane_index = 0;
    let snapshot = append_prompt_stash(&path, &first).unwrap();
    assert_eq!(snapshot.entries.len(), 1);

    let mut second = entry("two");
    second.source = "all".to_string();
    second.pane_index = 1;
    let snapshot = append_prompt_stash(&path, &second).unwrap();

    assert_eq!(snapshot.entries, vec![first, second]);
    assert_eq!(snapshot.stats.loaded_rows, 2);
}

#[test]
fn prompt_stash_skips_blank_and_malformed_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        [
            "",
            "NOT JSON",
            r#"{"text":"missing id and created_at"}"#,
            r#"{"id":"only-id"}"#,
            r#"{"id":"good","created_at":"2026-06-16T01:02:03+00:00","text":"hi"}"#,
        ]
        .join("\n"),
    )
    .unwrap();

    let snapshot = read_prompt_stash_snapshot(&path).unwrap();
    assert_eq!(snapshot.entries.len(), 1);
    let loaded = &snapshot.entries[0];
    assert_eq!(loaded.id, "good");
    assert_eq!(loaded.text, "hi");
    assert_eq!(loaded.frontmatter, "");
    assert_eq!(loaded.project, None);
    assert_eq!(loaded.pane_index, 0);
    assert_eq!(snapshot.stats.blank_lines, 1);
    assert_eq!(snapshot.stats.invalid_json_lines, 1);
    assert_eq!(snapshot.stats.invalid_record_lines, 2);
}

#[test]
fn prompt_stash_pop_removes_only_requested_ids() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_prompt_stash(&path, &entry("a")).unwrap();
    append_prompt_stash(&path, &entry("b")).unwrap();
    append_prompt_stash(&path, &entry("c")).unwrap();

    let outcome =
        pop_prompt_stash(&path, &["a".to_string(), "c".to_string()]).unwrap();
    let removed_ids: Vec<&str> =
        outcome.removed.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(removed_ids, vec!["a", "c"]);
    let remaining_ids: Vec<&str> = outcome
        .snapshot
        .entries
        .iter()
        .map(|e| e.id.as_str())
        .collect();
    assert_eq!(remaining_ids, vec!["b"]);

    let snapshot = read_prompt_stash_snapshot(&path).unwrap();
    assert_eq!(snapshot.entries.len(), 1);
    assert_eq!(snapshot.entries[0].id, "b");
}

#[test]
fn prompt_stash_pop_unknown_ids_is_a_no_op() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_prompt_stash(&path, &entry("a")).unwrap();

    let outcome = pop_prompt_stash(&path, &["missing".to_string()]).unwrap();
    assert!(outcome.removed.is_empty());
    assert_eq!(outcome.snapshot.entries.len(), 1);
    assert_eq!(outcome.snapshot.entries[0].id, "a");
}

#[test]
fn prompt_stash_rewrite_merges_and_preserves_unseen_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    rewrite_prompt_stash(&path, &[entry("a"), entry("b"), entry("c")]).unwrap();

    let mut a_modified = entry("a");
    a_modified.text = "updated".to_string();
    let snapshot =
        rewrite_prompt_stash(&path, &[a_modified, entry("d")]).unwrap();

    let ids: Vec<&str> =
        snapshot.entries.iter().map(|e| e.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "d", "b", "c"]);
    let a = snapshot.entries.iter().find(|e| e.id == "a").unwrap();
    assert_eq!(a.text, "updated");
}

#[test]
fn prompt_stash_json_shape_uses_expected_wire_keys() {
    let mut e = entry("shape");
    e.project = Some("proj".to_string());
    e.frontmatter = "x: 1\n".to_string();
    e.source = "all".to_string();
    e.pane_index = 2;
    let value = serde_json::to_value(&e).unwrap();
    assert_eq!(
        value,
        json!({
            "id": "shape",
            "created_at": "2026-06-16T01:02:03+00:00",
            "text": "draft for shape",
            "frontmatter": "x: 1\n",
            "project": "proj",
            "source": "all",
            "pane_index": 2
        })
    );
}

#[test]
fn prompt_stash_append_plus_pop_concurrency_preserves_valid_rows() {
    let temp = tempdir().unwrap();
    let path = store_path(temp.path());
    append_prompt_stash(&path, &entry("seed")).unwrap();

    let append_path = path.clone();
    let append_thread = thread::spawn(move || {
        for idx in 0..80 {
            append_prompt_stash(&append_path, &entry(&format!("append-{idx}")))
                .unwrap();
        }
    });

    let pop_path = path.clone();
    let pop_thread = thread::spawn(move || {
        for idx in 0..30 {
            let _ = pop_prompt_stash(&pop_path, &[format!("append-{idx}")])
                .unwrap();
        }
    });

    append_thread.join().unwrap();
    pop_thread.join().unwrap();

    let content = fs::read_to_string(&path).unwrap();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(value.get("id").is_some());
    }
    let snapshot = read_prompt_stash_snapshot(&path).unwrap();
    assert!(snapshot.entries.iter().any(|e| e.id == "seed"));
    assert!(snapshot.entries.iter().any(|e| e.id == "append-79"));
}

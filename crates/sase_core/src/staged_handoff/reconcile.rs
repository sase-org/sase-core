//! Bounded discovery of staged origins, independent of their UI status.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::agent_scan::index::agent_artifact_recovery_page;
use crate::agent_scan::layout::{
    parse_agent_artifact_path, resolve_agent_artifact_path,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedReconcileDirectoryWire {
    pub directory: String,
    pub after: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagedReconcileCursorWire {
    #[serde(default)]
    pub index_after: String,
    #[serde(default)]
    pub backfill: Vec<StagedReconcileDirectoryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedReconcilePageWire {
    pub cursor: StagedReconcileCursorWire,
    pub candidates: Vec<String>,
    pub scanned: usize,
    pub errors: Vec<String>,
}

/// Read at most `limit` index rows and `limit` directory entries. The index
/// is a cache, so a separate resumable walk also finds pre-index stages and
/// evicted terminal rows. Neither path loads historical artifact contents.
/// Directory names are listed once per visited directory per page; symlinks
/// and non-layout subtrees are never followed. Cursors wrap to revisit live
/// origins after their runner dies, without depending on a new file event.
pub fn staged_reconcile_page(
    index_path: &Path,
    projects_root: &Path,
    cursor: StagedReconcileCursorWire,
    limit: usize,
) -> Result<StagedReconcilePageWire, String> {
    if !(1..=256).contains(&limit) {
        return Err("staged reconciliation page limit must be 1..256".into());
    }
    if cursor.backfill.len() > 6
        || cursor.backfill.iter().any(|frame| {
            Path::new(&frame.directory)
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
                || Path::new(&frame.directory).components().count() > 5
                || frame.after.contains('/')
                || frame.after == ".."
        })
    {
        return Err("invalid staged reconciliation directory cursor".into());
    }
    let mut page = StagedReconcilePageWire {
        cursor,
        candidates: Vec::new(),
        scanned: 0,
        errors: Vec::new(),
    };
    let mut candidates = BTreeSet::new();
    match agent_artifact_recovery_page(
        index_path,
        projects_root,
        &page.cursor.index_after,
        limit,
    ) {
        Ok(rows) => {
            page.scanned += rows.len();
            page.cursor.index_after = if rows.len() < limit {
                String::new()
            } else {
                rows.last().cloned().unwrap_or_default()
            };
            for row in rows {
                let path =
                    resolve_agent_artifact_path(projects_root, Path::new(&row));
                select_candidate(projects_root, &path, &mut candidates);
            }
        }
        Err(error) => page.errors.push(format!("artifact index: {error}")),
    }
    if page.cursor.backfill.is_empty() {
        page.cursor
            .backfill
            .push(StagedReconcileDirectoryWire::default());
    }
    let mut listings: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for _ in 0..limit {
        let Some(frame) = page.cursor.backfill.last_mut() else {
            break;
        };
        let directory = projects_root.join(&frame.directory);
        let entries =
            listings.entry(frame.directory.clone()).or_insert_with(|| {
                if !safe_directory(projects_root, &directory) {
                    return Vec::new();
                }
                match fs::read_dir(&directory) {
                    Ok(entries) => {
                        let mut names = Vec::new();
                        for entry in entries {
                            match entry {
                                Ok(entry) => names.push(
                                    entry
                                        .file_name()
                                        .to_string_lossy()
                                        .into_owned(),
                                ),
                                Err(error) => {
                                    page.errors.push(error.to_string())
                                }
                            }
                        }
                        names.sort();
                        names
                    }
                    Err(error) => {
                        if error.kind() != std::io::ErrorKind::NotFound {
                            page.errors.push(error.to_string());
                        }
                        Vec::new()
                    }
                }
            });
        let next = entries.iter().find(|name| *name > &frame.after).cloned();
        page.scanned += 1;
        let Some(name) = next else {
            page.cursor.backfill.pop();
            continue;
        };
        frame.after = name.clone();
        let child = directory.join(&name);
        if !safe_directory(projects_root, &child) {
            continue;
        }
        let relative = child
            .strip_prefix(projects_root)
            .map_err(|error| error.to_string())?;
        let parts: Vec<_> =
            relative.iter().map(|part| part.to_string_lossy()).collect();
        let descend = match parts.len() {
            1 => true,
            2 => name == "artifacts",
            3 => name == "ace-run",
            4 => {
                name.len() == 6
                    && name.bytes().all(|byte| byte.is_ascii_digit())
            }
            5 => {
                name.len() == 2
                    && name.bytes().all(|byte| byte.is_ascii_digit())
            }
            _ => false,
        };
        if descend {
            page.cursor.backfill.push(StagedReconcileDirectoryWire {
                directory: relative.to_string_lossy().into_owned(),
                after: String::new(),
            });
        } else {
            select_candidate(projects_root, &child, &mut candidates);
        }
    }
    page.candidates = candidates.into_iter().collect();
    Ok(page)
}

fn safe_directory(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let mut current = root.to_path_buf();
    for part in relative.components() {
        if !matches!(part, Component::Normal(_)) {
            return false;
        }
        current.push(part);
        if !fs::symlink_metadata(&current).is_ok_and(|meta| meta.is_dir()) {
            return false;
        }
    }
    true
}

fn select_candidate(
    root: &Path,
    path: &Path,
    candidates: &mut BTreeSet<String>,
) {
    if parse_agent_artifact_path(root, path)
        .is_some_and(|info| info.workflow_dir_name == "ace-run")
        && safe_directory(root, path)
        && path
            .join(".sase_handoff_staged.json")
            .try_exists()
            .unwrap_or(true)
    {
        candidates.insert(path.to_string_lossy().into_owned());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backfill_is_bounded_and_eventually_revisits_both_layouts() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let old = root.join("p/artifacts/ace-run/20261028120000");
        let sharded = root.join("p/artifacts/ace-run/202610/29/20261029120000");
        for path in [&old, &sharded] {
            fs::create_dir_all(path).unwrap();
            fs::write(
                path.join(".sase_handoff_staged.json"),
                "malformed but discoverable",
            )
            .unwrap();
        }
        let mut cursor = StagedReconcileCursorWire::default();
        let mut found = BTreeMap::new();
        for _ in 0..100 {
            let page = staged_reconcile_page(
                &temp.path().join("absent.sqlite"),
                &root,
                cursor,
                2,
            )
            .unwrap();
            assert!(page.scanned <= 4);
            for candidate in page.candidates {
                *found.entry(candidate).or_insert(0) += 1;
            }
            cursor = page.cursor;
        }
        assert_eq!(found.len(), 2);
        assert!(found.values().all(|count| *count > 1));
    }

    #[test]
    fn rejects_escaping_cursor() {
        let cursor = StagedReconcileCursorWire {
            backfill: vec![StagedReconcileDirectoryWire {
                directory: "../other".into(),
                after: String::new(),
            }],
            ..Default::default()
        };
        assert!(staged_reconcile_page(
            Path::new("missing"),
            Path::new("root"),
            cursor,
            2
        )
        .is_err());
    }

    #[test]
    fn index_pages_terminal_origins_without_loading_their_records() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        let index = temp.path().join("index.sqlite");
        let conn = rusqlite::Connection::open(&index).unwrap();
        conn.execute_batch("CREATE TABLE agent_artifacts (artifact_dir TEXT PRIMARY KEY, projects_root TEXT, workflow_dir_name TEXT, imported_owner_machine TEXT, hidden INTEGER, has_done_marker INTEGER);").unwrap();
        let mut expected = Vec::new();
        for stamp in ["20261028120000", "20261028120001", "20261028120002"] {
            let path = root.join("p/artifacts/ace-run").join(stamp);
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join(".sase_handoff_staged.json"), "{}").unwrap();
            let path = path.to_string_lossy().into_owned();
            conn.execute("INSERT INTO agent_artifacts VALUES (?1, ?2, 'ace-run', NULL, 1, 1)", rusqlite::params![path, root.to_string_lossy()]).unwrap();
            expected.push(path);
        }
        let first = staged_reconcile_page(
            &index,
            &root,
            StagedReconcileCursorWire::default(),
            1,
        )
        .unwrap();
        assert_eq!(first.candidates, expected[..1]);
        let second =
            staged_reconcile_page(&index, &root, first.cursor, 1).unwrap();
        assert_eq!(second.candidates, expected[1..2]);
        assert!(second.errors.is_empty());
    }
}

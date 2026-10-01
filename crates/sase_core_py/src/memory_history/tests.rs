use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::test_support::git;
use serde_json::json;
use std::fs;

fn tiny_repo() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--initial-branch=master"]);
    git(&repo, &["config", "user.name", "SASE Test"]);
    git(&repo, &["config", "user.email", "sase@example.com"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    fs::create_dir_all(repo.join("memory")).unwrap();
    fs::write(repo.join("memory").join("note.md"), "tiny words here\n")
        .unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-qm", "add tiny note"]);
    let cache = tmp.path().join("cache");
    (tmp, repo, cache)
}

fn tiny_scope(
    repo: &std::path::Path,
    cache: &std::path::Path,
) -> serde_json::Value {
    json!({
        "scope_key": "project:tiny",
        "scope_kind": "project",
        "repo_root": repo.to_string_lossy(),
        "memory_roots": ["memory"],
        "instruction_files": [],
        "generated_notes": [],
        "renderer_prefixes": [],
        "config_paths": [],
        "cache_dir": cache.to_string_lossy(),
    })
}

#[test]
fn memory_history_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_memory_history(&module).unwrap();
        for name in [
            "memory_history_wire_schema_version",
            "memory_history_sync",
            "memory_history_subjects",
            "memory_history_resolve",
            "memory_history_timeline",
            "memory_history_version",
            "memory_history_compare",
            "memory_history_feed",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }
        assert_eq!(py_memory_history_wire_schema_version(), 1);
    });
}

#[test]
fn memory_history_sync_and_subjects_round_trip() {
    pyo3::prepare_freethreaded_python();
    let (_tmp, repo, cache) = tiny_repo();
    let head = git(&repo, &["rev-parse", "HEAD"]);
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_memory_history(&module).unwrap();

        let request = json_value_to_py(
            py,
            &json!({ "scope": tiny_scope(&repo, &cache) }),
        )
        .unwrap();
        let request = request.bind(py).downcast::<PyDict>().unwrap();
        let result = py_memory_history_sync(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["status"], json!("rebuilt"));
        assert_eq!(value["tip"], json!(head));
        assert_eq!(value["subject_count"], json!(1));

        let listed = py_memory_history_subjects(py, request).unwrap();
        let listed = py_to_json_value(listed.bind(py)).unwrap();
        assert_eq!(listed["schema_version"], json!(1));
        assert_eq!(listed["subjects"].as_array().unwrap().len(), 1);
        assert_eq!(
            listed["subjects"][0]["id"],
            json!("note:project:tiny/note")
        );
        assert!(listed["subjects"][0]["versions"]
            .as_array()
            .unwrap()
            .is_empty());
    });
}

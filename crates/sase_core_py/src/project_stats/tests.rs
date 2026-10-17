use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::prelude::*;
use crate::test_support::git;
use serde_json::json;
use std::fs;

fn tiny_repo() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let repo = tmp.path().join("repo");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--initial-branch=main"]);
    git(&repo, &["config", "user.name", "SASE Test"]);
    git(&repo, &["config", "user.email", "sase@example.test"]);
    git(&repo, &["config", "commit.gpgsign", "false"]);
    fs::write(repo.join("file.txt"), "hello\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(
        &repo,
        &[
            "commit",
            "-m",
            "agent commit\n\nSASE_AGENT=writer\nSASE_MODEL=anthropic/opus\n",
        ],
    );
    let cache = tmp.path().join("cache");
    (tmp, repo, cache)
}

fn request_dict<'py>(
    py: Python<'py>,
    repo: &std::path::Path,
    cache: &std::path::Path,
) -> Bound<'py, PyDict> {
    let value = json!({
        "schema_version": 1,
        "start_ts": 0,
        "end_ts": 4_000_000_000i64,
        "now_ts": 4_000_000_000i64,
        "timezone": "America/New_York",
        "facets": ["output"],
        "cache_dir": cache.to_string_lossy(),
        "projects": [{
            "key": "tiny",
            "label": "Tiny",
            "state": "enabled",
            "selection": "always",
            "index_project_names": [],
            "code_repos": [{
                "id": "primary",
                "name": "primary",
                "kind": "primary",
                "path": repo.to_string_lossy(),
                "identity": "https://example.test/tiny.git",
                "ref_candidates": ["main", "HEAD"],
            }],
            "record_repos": [],
            "not_counted": [],
            "plan_store": {
                "storage": "in_tree",
                "plans_dir": "sdd/plans",
            },
        }],
    });
    let object = json_value_to_py(py, &value).unwrap();
    object
        .bind(py)
        .downcast::<pyo3::types::PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn project_stats_query_is_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_stats(&module).unwrap();
        assert!(module.getattr("project_stats_query").is_ok());
    });
}

#[test]
fn project_stats_query_round_trips_a_nontrivial_repo() {
    pyo3::prepare_freethreaded_python();
    let (_tmp, repo, cache) = tiny_repo();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_stats(&module).unwrap();
        let request = request_dict(py, &repo, &cache);
        let func = module.getattr("project_stats_query").unwrap();
        let result = func.call1((request,)).unwrap();
        let value = py_to_json_value(&result).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["projects"][0]["output"]["commits"], json!(1));
        assert_eq!(value["projects"][0]["output"]["agent_commits"], json!(1));
        assert!(value["projects"][0]["code"].is_null());
        assert!(value["projects"][0]["runs"].is_null());
        assert!(value["projects"][0]["output"]["commits"].is_number());
        assert!(value["fingerprint"].as_str().unwrap().len() > 16);
    });
}

#[test]
fn project_stats_query_serializes_null_counts_for_unavailable_sources() {
    pyo3::prepare_freethreaded_python();
    let tmp = tempfile::tempdir().unwrap();
    let cache = tmp.path().join("cache");
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_stats(&module).unwrap();
        let value = json!({
            "schema_version": 1,
            "start_ts": 0,
            "end_ts": 4_000_000_000i64,
            "now_ts": 4_000_000_000i64,
            "timezone": "America/New_York",
            "facets": ["output"],
            "cache_dir": cache.to_string_lossy(),
            "projects": [{
                "key": "missing",
                "label": "Missing",
                "state": "enabled",
                "selection": "always",
                "index_project_names": [],
                "code_repos": [{
                    "id": "primary",
                    "name": "primary",
                    "kind": "primary",
                    "path": "/no/such/project-stats-repo",
                    "identity": "https://example.test/missing.git",
                    "ref_candidates": ["main"],
                }],
                "record_repos": [],
                "not_counted": [],
                "plan_store": {
                    "storage": "in_tree",
                    "plans_dir": "sdd/plans",
                },
            }],
        });
        let request = json_value_to_py(py, &value).unwrap();
        let object = request
            .bind(py)
            .downcast::<pyo3::types::PyDict>()
            .unwrap()
            .clone();
        let func = module.getattr("project_stats_query").unwrap();
        let result = func.call1((object,)).unwrap();
        let value = py_to_json_value(&result).unwrap();
        assert!(value["projects"][0]["output"].is_object());
        assert!(value["projects"][0]["output"]["commits"].is_null());
        assert!(value["projects"][0]["lifetime"]["commits"].is_null());
        assert!(value["portfolio"]["output"]["commits"].is_null());
        assert!(value["projects"][0]["code"].is_null());
    });
}

#[test]
fn project_stats_query_rejects_an_invalid_request() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_project_stats(&module).unwrap();
        let request = json_value_to_py(
            py,
            &json!({
                "schema_version": 99,
                "start_ts": 0,
                "end_ts": 1,
                "now_ts": 1,
                "timezone": "America/New_York",
            }),
        )
        .unwrap();
        let func = module.getattr("project_stats_query").unwrap();
        let error = func.call1((request,)).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("unsupported project statistics schema"),
            "{message}"
        );
    });
}

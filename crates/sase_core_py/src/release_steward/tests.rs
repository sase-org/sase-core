use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use sase_core::release_steward::RELEASE_STEWARD_WIRE_SCHEMA_VERSION;
use serde_json::json;

fn releasable_request_json() -> serde_json::Value {
    json!({
        "schema_version": RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
        "observed_at": "2026-10-19T12:00:00Z",
        "policy": {
            "merge_authority_enabled": true,
            "observe_only": true,
            "tick_identity": "run-1/attempt-1",
            "cadence": {
                "kind": "daily",
                "merge_after_hour_utc": 9,
                "max_merges_per_utc_day": 1
            }
        },
        "snapshot": {
            "repository": {
                "status": "ready",
                "value": {
                    "owner": "sase-org",
                    "name": "sase",
                    "default_branch": "master"
                }
            },
            "master": {
                "status": "ready",
                "value": {
                    "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "gate_run": {
                        "workflow": "ci.yml",
                        "run_id": "gate-1",
                        "attempt": 1,
                        "ref_name": "master",
                        "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "status": "completed",
                        "conclusion": "success",
                        "jobs": [
                            {
                                "id": "job-test",
                                "name": "tests",
                                "conclusion": "success",
                                "is_test": true,
                                "is_lint": false,
                                "attempt": 1
                            },
                            {
                                "id": "job-lint",
                                "name": "lint",
                                "conclusion": "success",
                                "is_test": false,
                                "is_lint": true,
                                "attempt": 1
                            }
                        ],
                        "started_at": "2026-10-19T12:00:00Z",
                        "updated_at": "2026-10-19T12:00:00Z",
                        "event": "push",
                        "url": "https://example.test/gate-1"
                    },
                    "expected_required_checks": [
                        {"name": "tests", "source": "branch protection"},
                        {"name": "lint", "source": "branch protection"}
                    ],
                    "expected_checks_source": "branch protection",
                    "expected_checks_readable": true
                }
            },
            "release_pr": {
                "status": "ready",
                "value": {
                    "number": 42,
                    "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "source_pin": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "exact_public_core_pin": "0.50.0",
                    "labels": ["autorelease: pending"],
                    "checks": [
                        {
                            "name": "tests",
                            "conclusion": "success",
                            "is_test": true,
                            "is_lint": false,
                            "url": "https://example.test/check/tests"
                        },
                        {
                            "name": "lint",
                            "conclusion": "success",
                            "is_test": true,
                            "is_lint": false,
                            "url": "https://example.test/check/lint"
                        }
                    ],
                    "merge_history": [],
                    "stale_head": false,
                    "pin_changed": false,
                    "merged": false,
                    "url": "https://github.com/sase-org/sase/pull/42",
                    "core_smoke": {
                        "name": "release-core-floor-smoke",
                        "conclusion": "success",
                        "is_test": true,
                        "is_lint": false,
                        "url": "https://example.test/check/release-core-floor-smoke"
                    }
                }
            },
            "tags": {"status": "ready", "value": []},
            "releases": {"status": "ready", "value": []},
            "pypi": {
                "status": "ready",
                "value": {
                    "sase": {"package": "sase", "files": []},
                    "core": {"package": "sase-core-rs", "files": []}
                }
            },
            "trains": {"status": "ready", "value": []},
            "workflows": {
                "status": "ready",
                "value": {"release_yml": [], "publish_yml": []}
            },
            "core": {
                "status": "ready",
                "value": {
                    "master_ci": {
                        "workflow": "ci.yml",
                        "run_id": "gate-1",
                        "attempt": 1,
                        "ref_name": "master",
                        "sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "status": "completed",
                        "conclusion": "success",
                        "jobs": [
                            {
                                "id": "job-test",
                                "name": "tests",
                                "conclusion": "success",
                                "is_test": true,
                                "is_lint": false,
                                "attempt": 1
                            },
                            {
                                "id": "job-lint",
                                "name": "lint",
                                "conclusion": "success",
                                "is_test": false,
                                "is_lint": true,
                                "attempt": 1
                            }
                        ],
                        "started_at": "2026-10-19T12:00:00Z",
                        "updated_at": "2026-10-19T12:00:00Z",
                        "event": "push",
                        "url": "https://example.test/gate-1"
                    },
                    "cuts": []
                }
            },
            "canary": {
                "status": "ready",
                "value": {
                    "linux": {
                        "member": "linux",
                        "run_id": "canary-linux",
                        "attempt": 1,
                        "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "verdict": "pass",
                        "packages": ["sase"],
                        "scheduled": true
                    },
                    "macos": {
                        "member": "macos",
                        "run_id": "canary-macos",
                        "attempt": 1,
                        "sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "verdict": "pass",
                        "packages": ["sase"],
                        "scheduled": true
                    }
                }
            },
            "candidate": {"status": "ready", "value": null},
            "core_ensure": {
                "status": "ready",
                "value": {
                    "kind": "pinned",
                    "version": "0.50.0",
                    "files": [
                        "sase_core_rs-0.50.0.tar.gz",
                        "sase_core_rs-0.50.0-cp312-abi3-manylinux.whl"
                    ]
                }
            },
            "issues": {"status": "ready", "value": []},
            "core_actions_access": {
                "status": "ready",
                "value": {"readable": true, "detail": "read ok"}
            },
            "merge_history": {"status": "ready", "value": []},
            "blocker_lineage": {"status": "ready", "value": null}
        }
    })
}

#[test]
fn release_steward_binding_round_trips_releasable() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_release_steward_wire_schema_version(),
            RELEASE_STEWARD_WIRE_SCHEMA_VERSION
        );
        let request_obj =
            json_value_to_py(py, &releasable_request_json()).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_release_steward_decide(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["primary"]["kind"], json!("releasable"));
        assert_eq!(value["primary"]["pr_number"], json!(42));
        assert_eq!(value["process_exit"], json!(0));
        assert_eq!(
            value["train_action"]["action"]["kind"],
            json!("merge_release_pr")
        );
        assert_ne!(value["train_action"]["disposition"], json!("executed"));
        assert!(result.bind(py).downcast::<PyDict>().is_ok());
    });
}

#[test]
fn release_steward_binding_maps_unreadable_to_watcher_broken() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut request = releasable_request_json();
        request["snapshot"]["pypi"] = json!({
            "status": "unreadable",
            "source": "pypi.sase",
            "message": "503",
            "remedy": "retry PyPI"
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let result = py_release_steward_decide(py, request).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["primary"]["kind"], json!("watcher_broken"));
        assert_eq!(value["primary"]["source"], json!("pypi.sase"));
        assert_eq!(value["process_exit"], json!(1));
        assert!(value["train_action"].is_null());
    });
}

#[test]
fn release_steward_binding_rejects_invalid_request() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let request_obj = json_value_to_py(py, &json!({"nope": true})).unwrap();
        let request = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let error = py_release_steward_decide(py, request).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("invalid_release_steward_request"));
    });
}

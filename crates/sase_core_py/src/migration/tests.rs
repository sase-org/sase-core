use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;
use std::fs;

#[test]
fn migration_bindings_expose_contract_helpers() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::write(root.join("source.txt"), "before").unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let schema: u32 = module
            .getattr("migration_wire_schema_version")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(schema, 1);

        let manifest = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "manifest_id": "m1",
                "source_digests": {"root": "abc"},
                "operations": [
                    {
                        "operation": "state-residue",
                        "source_digests": {"state": "def"},
                        "x_phase": "kit-driver"
                    }
                ],
                "x_host_note": "keep"
            }),
        )
        .unwrap();
        let manifest = manifest.bind(py).downcast::<PyDict>().unwrap();
        let normalized = module
            .getattr("migration_manifest_normalize")
            .unwrap()
            .call1((manifest,))
            .unwrap();
        let normalized = py_to_json_value(&normalized).unwrap();
        assert_eq!(normalized["x_host_note"], "keep");
        assert_eq!(normalized["operations"][0]["x_phase"], "kit-driver");

        let records = PyList::empty_bound(py);
        append_json(
            py,
            &records,
            json!({"schema_version": 1, "state": "backed_up"}),
        );
        let observed =
            json_value_to_py(py, &json!({"root": "abc", "state": "def"}))
                .unwrap();
        let observed = observed.bind(py).downcast::<PyDict>().unwrap();
        let plan = module
            .getattr("migration_plan_next_step")
            .unwrap()
            .call1((manifest, &records, observed))
            .unwrap();
        let plan = py_to_json_value(&plan).unwrap();
        assert_eq!(plan["current_state"], "backed_up");
        assert_eq!(plan["next_step"], "apply");

        let digest = module
            .getattr("migration_tree_digest")
            .unwrap()
            .call1((root.to_str().unwrap(),))
            .unwrap();
        let digest = py_to_json_value(&digest).unwrap();
        assert_eq!(digest["schema_version"], 1);
        assert!(digest["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["relative_path"] == "source.txt"));

        let left = json_value_to_py(py, &json!([{"b": 2, "a": 1}])).unwrap();
        let right = json_value_to_py(py, &json!([{"a": 1, "b": 2}])).unwrap();
        let left_fp: String = module
            .getattr("migration_fingerprint")
            .unwrap()
            .call1((left.bind(py),))
            .unwrap()
            .extract()
            .unwrap();
        let right_fp: String = module
            .getattr("migration_fingerprint")
            .unwrap()
            .call1((right.bind(py),))
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(left_fp, right_fp);

        let entry = json_value_to_py(
            py,
            &json!({
                "entry_id": "agent-tags",
                "residue_path": "~/.sase/agent_tags.json",
                "canonical_counterpart": "~/.sase/agents"
            }),
        )
        .unwrap();
        let facts = json_value_to_py(
            py,
            &json!({
                "residue_exists": true,
                "counterpart_exists": true
            }),
        )
        .unwrap();
        let classification = module
            .getattr("migration_residue_classify")
            .unwrap()
            .call1((
                entry.bind(py).downcast::<PyDict>().unwrap(),
                facts.bind(py).downcast::<PyDict>().unwrap(),
            ))
            .unwrap();
        let classification = py_to_json_value(&classification).unwrap();
        assert_eq!(classification["decision"], "archive");

        let legacy = PyList::empty_bound(py);
        append_json(
            py,
            &legacy,
            json!({"task_id": "proc-1", "semantic_fingerprint": "same"}),
        );
        let canonical = PyList::empty_bound(py);
        canonical.append("proc-1").unwrap();
        let reconcile = module
            .getattr("migration_reconcile_procs")
            .unwrap()
            .call1((&legacy, &canonical))
            .unwrap();
        let reconcile = py_to_json_value(&reconcile).unwrap();
        assert_eq!(reconcile["matched"].as_array().unwrap().len(), 1);

        let legacy = "\
## ChangeSpec
NAME: alpha
STATUS: WIP
CL: https://example.test/1
COMMITS:
  (1) first
";
        let facts = json_value_to_py(py, &json!({})).unwrap();
        let facts = facts.bind(py).downcast::<PyDict>().unwrap();
        let bytes = PyBytes::new_bound(py, legacy.as_bytes());
        let patch_plan = module
            .getattr("migration_patch_records_plan")
            .unwrap()
            .call1(("proj.sase", bytes, facts))
            .unwrap();
        let patch_plan = py_to_json_value(&patch_plan).unwrap();
        assert_eq!(patch_plan["intended_action"], "convert");

        let envelope = json_value_to_py(
            py,
            &json!({
                "schema_version": 2,
                "kind": "plan",
                "request_id": "req-1",
                "branches": [["approve"], ["reject"]],
                "hashes": {"request": "dead", "resources": {}}
            }),
        )
        .unwrap();
        let envelope = envelope.bind(py).downcast::<PyDict>().unwrap();
        let settled = json_value_to_py(
            py,
            &json!({
                "has_response": true,
                "has_cancellation": false,
                "deadline_passed": false
            }),
        )
        .unwrap();
        let settled = settled.bind(py).downcast::<PyDict>().unwrap();
        let gate_plan = module
            .getattr("migration_gate_bundles_plan")
            .unwrap()
            .call1((envelope, settled))
            .unwrap();
        let gate_plan = py_to_json_value(&gate_plan).unwrap();
        assert_eq!(gate_plan["intended_action"], "convert");
    });
}

#[test]
fn migration_bounded_lock_binding_returns_releasable_handle() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let lock_path = temp.path().join("migration.lock");

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let lock = module
            .getattr("migration_acquire_bounded_lock")
            .unwrap()
            .call1((lock_path.to_str().unwrap(), 250_u64, "binding-test"))
            .unwrap();
        let waited_ms: u64 =
            lock.getattr("waited_ms").unwrap().extract().unwrap();
        assert!(waited_ms <= 250);
        lock.call_method0("release").unwrap();
        lock.call_method0("release").unwrap();
    });
}

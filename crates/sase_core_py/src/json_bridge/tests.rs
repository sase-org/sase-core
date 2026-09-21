use super::*;
use serde_json::json;
use std::fs;

fn py_dict_keys(dict: &Bound<'_, PyDict>) -> Vec<String> {
    dict.keys()
        .iter()
        .map(|key| key.extract::<String>().unwrap())
        .collect()
}

#[test]
fn direct_serializer_matches_json_bridge_for_json_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let value = json!({
            "schema_version": 7,
            "name": "agent",
            "done": null,
            "active": true,
            "ratio": 1.25,
            "records": [
                {"timestamp": "20260827120000", "tags": ["a", "b"]},
                {"timestamp": "20260827120100", "output": {"k": 1}},
            ],
        });
        let legacy = json_value_to_py(py, &value).unwrap();
        let direct = serialize_to_py(py, &value).unwrap();
        assert_eq!(
            py_to_json_value(legacy.bind(py)).unwrap(),
            py_to_json_value(direct.bind(py)).unwrap()
        );
    });
}

#[test]
fn direct_serializer_matches_json_bridge_for_agent_scan_snapshot() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("projects");
        fs::create_dir_all(&root).unwrap();
        let snapshot = core_scan_agent_artifacts(
            &root,
            AgentArtifactScanOptionsWire::default(),
        );
        let value = serde_json::to_value(&snapshot).unwrap();
        let legacy = json_value_to_py(py, &value).unwrap();
        let direct = serialize_to_py(py, &snapshot).unwrap();
        assert_eq!(
            py_to_json_value(legacy.bind(py)).unwrap(),
            py_to_json_value(direct.bind(py)).unwrap()
        );

        let legacy_dict = legacy.bind(py).downcast::<PyDict>().unwrap();
        let direct_dict = direct.bind(py).downcast::<PyDict>().unwrap();
        assert_eq!(py_dict_keys(direct_dict), py_dict_keys(legacy_dict));
    });
}

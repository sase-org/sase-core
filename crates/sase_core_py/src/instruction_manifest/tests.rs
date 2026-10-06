use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;

#[test]
fn instruction_manifest_bindings_are_registered() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        register_instruction_manifest(&module).unwrap();
        assert!(module.getattr("normalize_instruction_manifest").is_ok());
        assert!(module
            .getattr("instruction_manifest_wire_schema_version")
            .is_ok());
        assert_eq!(py_instruction_manifest_wire_schema_version(), 1);
    });
}

#[test]
fn normalize_instruction_manifest_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../../sase_core/tests/fixtures/instruction_manifest_v1.json"
        ))
        .expect("golden fixture must parse");
        let manifest = json_value_to_py(py, &raw).unwrap();
        let manifest = manifest.bind(py).downcast::<PyDict>().unwrap();
        let result = py_normalize_instruction_manifest(py, manifest).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value, raw);
    });
}

#[test]
fn normalize_binding_fills_null_common_digest() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../../sase_core/tests/fixtures/instruction_manifest_v1.json"
        ))
        .expect("golden fixture must parse");
        raw["bundle"]["common_digest"] = json!(null);
        let manifest = json_value_to_py(py, &raw).unwrap();
        let manifest = manifest.bind(py).downcast::<PyDict>().unwrap();
        let result = py_normalize_instruction_manifest(py, manifest).unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(
            value["bundle"]["common_digest"],
            json!(
                "35e3de246e801813311923851b16818b87e1c18588db05dc9dc04f4bcc0793c2"
            )
        );
    });
}

#[test]
fn normalize_binding_rejects_bad_actor() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let mut raw: serde_json::Value = serde_json::from_str(include_str!(
            "../../../sase_core/tests/fixtures/instruction_manifest_v1.json"
        ))
        .expect("golden fixture must parse");
        raw["facts"]["actor"] = json!("codx");
        let manifest = json_value_to_py(py, &raw).unwrap();
        let manifest = manifest.bind(py).downcast::<PyDict>().unwrap();
        let error =
            py_normalize_instruction_manifest(py, manifest).unwrap_err();
        assert!(error.to_string().contains("valid InstructionManifestWire"));
    });
}

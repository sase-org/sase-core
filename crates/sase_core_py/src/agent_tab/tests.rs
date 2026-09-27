use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn agent_tab_bindings_round_trip() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let canonical = module
            .getattr("canonicalize_agent_tab_name")
            .unwrap()
            .call1(("  Sase  ",))
            .unwrap();
        assert_eq!(
            py_to_json_value(&canonical).unwrap(),
            json!({"kind": "named", "name": "sase"})
        );
        let default = module
            .getattr("canonicalize_agent_tab_name")
            .unwrap()
            .call1(("main",))
            .unwrap();
        assert_eq!(
            py_to_json_value(&default).unwrap(),
            json!({"kind": "default"})
        );
        assert!(module
            .getattr("canonicalize_agent_tab_name")
            .unwrap()
            .call1(("local",))
            .is_err());

        let resolved = module
            .getattr("resolve_effective_agent_tab")
            .unwrap()
            .call1((
                json_value_to_py(
                    py,
                    &json!({
                        "agent_tab": null,
                        "owner": {
                            "kind": "remote",
                            "installation_id": "id-1",
                            "alias": "apollo",
                        },
                    }),
                )
                .unwrap(),
                true,
            ))
            .unwrap();
        assert_eq!(
            py_to_json_value(&resolved).unwrap(),
            json!({"kind": "machine", "installation_id": "id-1"})
        );

        let catalog = module
            .getattr("build_agent_tab_catalog")
            .unwrap()
            .call1((
                json_value_to_py(
                    py,
                    &json!([
                        {"agent_tab": "blog", "owner": {"kind": "local"}},
                        {"agent_tab": null, "owner": {"kind": "local"}},
                    ]),
                )
                .unwrap(),
                json_value_to_py(
                    py,
                    &json!({
                        "machine_mode": false,
                        "machine_order": [],
                        "named_order": {},
                    }),
                )
                .unwrap(),
            ))
            .unwrap();
        let catalog = py_to_json_value(&catalog).unwrap();
        assert_eq!(
            catalog["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|entry| entry["label"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["main", "blog"]
        );
        assert_eq!(catalog["keys"].as_array().unwrap().len(), 2);
    });
}

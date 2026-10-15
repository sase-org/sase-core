use super::*;
use crate::json_bridge::py_to_json_value;
use crate::sase_core_rs;
use serde_json::json;

#[test]
fn project_clan_waves_binding_round_trips_request_and_result() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let result = module
            .getattr("project_clan_waves")
            .unwrap()
            .call1((
                crate::json_bridge::json_value_to_py(
                    py,
                    &json!({
                        "clan_name": "research.4j",
                        "members": [
                            {"name": "research.4j.final", "launch_key": "02", "wait_for": ["research.4j.cdx"]},
                            {"name": "research.4j.cdx", "launch_key": "01"},
                        ],
                    }),
                )
                .unwrap(),
            ))
            .unwrap();

        assert_eq!(
            py_to_json_value(&result).unwrap(),
            json!({
                "waves": [
                    ["research.4j.cdx"],
                    ["research.4j.final"],
                ],
                "wave_of": {
                    "research.4j.cdx": 1,
                    "research.4j.final": 2,
                },
                "preds": {
                    "research.4j.cdx": [],
                    "research.4j.final": ["research.4j.cdx"],
                },
                "external_waits": {
                    "research.4j.cdx": 0,
                    "research.4j.final": 0,
                },
                "ghosts": [],
                "unresolved": [],
                "seal": [],
            })
        );
    });
}

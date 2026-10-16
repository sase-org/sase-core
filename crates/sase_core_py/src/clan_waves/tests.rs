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
                        "clan_names": ["research.4j"],
                        "members": [
                            {"key": "final", "name": "research.4j.final", "launch_key": "02", "wait_for": ["research.4j.cdx"]},
                            {"key": "cdx", "name": "research.4j.cdx", "launch_key": "01"},
                        ],
                    }),
                )
                .unwrap(),
            ))
            .unwrap();

        assert_eq!(
            py_to_json_value(&result).unwrap(),
            json!({
                "layered": true,
                "numbered_wave_count": 2,
                "has_seal": false,
                "waves": [
                    {"index": 1, "number": 1, "seal": false, "member_keys": ["cdx"], "ghost_names": []},
                    {"index": 2, "number": 2, "seal": false, "member_keys": ["final"], "ghost_names": []},
                ],
                "members": [
                    {"key": "cdx", "wave_index": 1, "rank": 0, "predecessor_keys": [], "ghost_predecessors": [], "external_wait_count": 0},
                    {"key": "final", "wave_index": 2, "rank": 1, "predecessor_keys": ["cdx"], "ghost_predecessors": [], "external_wait_count": 0},
                ],
                "unresolved": [],
            })
        );
    });
}

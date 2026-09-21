use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};

#[test]
fn fleet_attention_inventory_bindings_validate_envelopes() {
    use serde_json::json;

    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let origin_installation_id = format!(
            "{}{}",
            sase_core::FLEET_INSTALLATION_ID_PREFIX,
            "a".repeat(64)
        );
        let rows = json!([
            {
                "schema_version": 1,
                "notification": {
                    "id": "question-00000001",
                    "timestamp": "2026-09-07T00:00:00Z",
                    "sender": "uncataloged-agent",
                    "notes": ["Need a decision"],
                    "action": "UserQuestion",
                    "action_data": {
                        "question_count": "1"
                    }
                },
                "state": "available"
            }
        ]);
        let resolved = json!([]);
        let request = json!({
            "schema_version": 1,
            "limit": 10
        });
        let freshness = json!({
            "schema_version": 1,
            "freshness": "fresh",
            "partial": false,
            "refreshed_at_unix": 100.0,
            "error": null
        });

        let rows = json_value_to_py(py, &rows).unwrap().into_bound(py);
        let rows = rows.downcast::<PyList>().unwrap();
        let resolved = json_value_to_py(py, &resolved).unwrap().into_bound(py);
        let resolved = resolved.downcast::<PyList>().unwrap();
        let request = json_value_to_py(py, &request).unwrap().into_bound(py);
        let request = request.downcast::<PyDict>().unwrap();
        let freshness =
            json_value_to_py(py, &freshness).unwrap().into_bound(py);
        let freshness = freshness.downcast::<PyDict>().unwrap();

        let validated_request =
            py_fleet_validate_attention_inventory_request(py, request).unwrap();
        assert_eq!(
            py_to_json_value(validated_request.bind(py)).unwrap()["limit"],
            json!(10)
        );

        let projected = py_fleet_project_attention_inventory(
            py,
            &origin_installation_id,
            rows,
            resolved,
            request,
            100.0,
            freshness,
        )
        .unwrap();
        let projected_value = py_to_json_value(projected.bind(py)).unwrap();
        assert_eq!(projected_value["page"]["total_matching_entries"], json!(1));
        assert_eq!(
            projected_value["page"]["entries"][0]["logical_key"],
            json!(null)
        );
        assert_eq!(
            projected_value["page"]["entries"][0]["state"],
            json!("pending")
        );

        let projected_dict = projected.bind(py).downcast::<PyDict>().unwrap();
        let validated =
            py_fleet_validate_attention_inventory_response(py, projected_dict)
                .unwrap();
        assert_eq!(
            py_to_json_value(validated.bind(py)).unwrap(),
            projected_value
        );
    });
}

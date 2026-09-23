use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use tempfile::tempdir;

#[test]
fn effort_override_bindings_round_trip_and_resolve() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(
            py_effort_override_wire_schema_version(),
            sase_core::EFFORT_OVERRIDE_WIRE_SCHEMA_VERSION
        );
        let written = py_effort_override_set_relative(
            py,
            &home,
            "high",
            "binding-test",
            Some(900.0),
            Some(now),
        )
        .unwrap();
        let written_value = py_to_json_value(written.bind(py)).unwrap();
        assert_eq!(written_value["effort"], json!("high"));
        assert_eq!(written_value["expires_at"], json!(now + 900.0));

        let loaded = py_effort_override_get(py, &home, Some(now)).unwrap();
        assert_eq!(py_to_json_value(loaded.bind(py)).unwrap(), written_value);

        let resolved = py_resolve_effective_effort(
            py,
            None,
            None,
            Some("high"),
            Some("low"),
        )
        .unwrap();
        let resolved_value = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(resolved_value["level"], json!("high"));
        assert_eq!(resolved_value["source"], json!("temporary_override"));
        assert_eq!(resolved_value["explicit"], json!(false));

        assert!(py_effort_override_clear(&home).unwrap());
        assert!(!py_effort_override_clear(&home).unwrap());
    });
}

#[test]
fn effort_override_binding_rejects_invalid_values() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    Python::with_gil(|py| {
        let error = py_effort_override_set_until(
            py,
            &home,
            "turbo",
            2.0,
            "test",
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn size_model_route_binding_maps_public_aliases() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        for (size, alias) in [
            ("xsmall", "@xsmall"),
            ("small", "@small"),
            ("medium", "@medium"),
            ("large", "@large"),
            ("xlarge", "@xlarge"),
        ] {
            let routed = py_size_model_route(py, size).unwrap();
            assert_eq!(
                py_to_json_value(routed.bind(py)).unwrap(),
                json!({"size": size, "alias": alias})
            );
            let from_alias = py_size_model_route(py, alias).unwrap();
            assert_eq!(
                py_to_json_value(from_alias.bind(py)).unwrap(),
                json!({"size": size, "alias": alias})
            );
        }
    });
}

#[test]
fn size_model_route_binding_rejects_invalid_sizes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        for size in ["", "medium_worker", "@epic_lander", "MEDIUM"] {
            let error = py_size_model_route(py, size).unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py), "{size:?}");
            assert!(
                error.to_string().contains("size must be one of"),
                "{size:?} -> {error}"
            );
        }
    });
}

#[test]
fn epic_land_model_binding_selects_explicit_then_threshold() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let py_int = |value: i64| {
            json_value_to_py(py, &json!(value)).unwrap().into_bound(py)
        };
        let explicit = py_select_epic_land_model(
            py,
            Some("codex/gpt-5.5@xhigh"),
            py_int(9),
            py_int(5),
            "@large",
            "@xlarge",
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(explicit.bind(py)).unwrap(),
            json!({
                "model": "codex/gpt-5.5@xhigh",
                "source": "explicit",
                "explicit": true
            })
        );

        let normal = py_select_epic_land_model(
            py,
            None,
            py_int(4),
            py_int(5),
            "@large",
            "@xlarge",
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(normal.bind(py)).unwrap(),
            json!({
                "model": "@large",
                "source": "epic_lander_model",
                "explicit": false
            })
        );

        let big = py_select_epic_land_model(
            py,
            None,
            py_int(5),
            py_int(5),
            "@large",
            "@xlarge",
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(big.bind(py)).unwrap(),
            json!({
                "model": "@xlarge",
                "source": "big_epic_lander_model",
                "explicit": false
            })
        );
    });
}

#[test]
fn epic_land_model_binding_rejects_invalid_counts_and_thresholds() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let py_int = |value: i64| {
            json_value_to_py(py, &json!(value)).unwrap().into_bound(py)
        };
        let py_bool =
            json_value_to_py(py, &json!(true)).unwrap().into_bound(py);
        let bool_count = py_select_epic_land_model(
            py,
            None,
            py_bool,
            py_int(5),
            "@large",
            "@xlarge",
        )
        .unwrap_err();
        assert!(bool_count.is_instance_of::<PyValueError>(py));
        assert!(bool_count
            .to_string()
            .contains("phase_count must be an integer, not a boolean"));

        let negative = py_select_epic_land_model(
            py,
            None,
            py_int(-1),
            py_int(5),
            "@large",
            "@xlarge",
        )
        .unwrap_err();
        assert!(negative.is_instance_of::<PyValueError>(py));
        assert!(negative
            .to_string()
            .contains("phase_count must be a non-negative integer"));

        let zero_threshold = py_select_epic_land_model(
            py,
            None,
            py_int(2),
            py_int(0),
            "@large",
            "@xlarge",
        )
        .unwrap_err();
        assert!(zero_threshold.is_instance_of::<PyValueError>(py));
        assert!(zero_threshold
            .to_string()
            .contains("threshold must be a positive integer"));
    });
}

#[test]
fn runner_limit_override_bindings_round_trip_and_replace() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(
            py_runner_limit_override_wire_schema_version(),
            sase_core::RUNNER_LIMIT_OVERRIDE_WIRE_SCHEMA_VERSION
        );
        let first = py_runner_limit_override_set_relative(
            py,
            &home,
            1,
            "binding-test",
            Some(900.0),
            Some(now),
        )
        .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        assert_eq!(first_value["limit"], json!(1));
        assert_eq!(first_value["expires_at"], json!(now + 900.0));

        let replacement = py_runner_limit_override_set_until(
            py,
            &home,
            12,
            now + 60.0,
            "binding-test",
            Some(now),
        )
        .unwrap();
        let replacement_value = py_to_json_value(replacement.bind(py)).unwrap();
        assert_eq!(replacement_value["limit"], json!(12));

        let loaded =
            py_runner_limit_override_get(py, &home, Some(now)).unwrap();
        assert_eq!(
            py_to_json_value(loaded.bind(py)).unwrap(),
            replacement_value
        );
        assert!(py_runner_limit_override_clear(&home).unwrap());
        assert!(!py_runner_limit_override_clear(&home).unwrap());
    });
}

#[test]
fn runner_limit_override_binding_rejects_invalid_values() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    Python::with_gil(|py| {
        let error = py_runner_limit_override_set_relative(
            py,
            &home,
            0,
            "test",
            None,
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let error = py_runner_limit_override_set_until(
            py,
            &home,
            1,
            1.0,
            "test",
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_disable_bindings_round_trip_and_replace() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(
            py_provider_disable_wire_schema_version(),
            sase_core::PROVIDER_DISABLE_WIRE_SCHEMA_VERSION
        );
        let first = py_provider_disable_set_relative(
            py,
            &home,
            "claude",
            "binding-test",
            "hard",
            Some(900.0),
            Some(now),
        )
        .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        assert_eq!(
            first_value,
            json!({
                "version": 2,
                "provider": "claude",
                "created_at": now,
                "expires_at": now + 900.0,
                "source": "binding-test",
                "mode": "hard",
            })
        );

        let codex = py_provider_disable_set_relative(
            py,
            &home,
            "codex",
            "binding-test",
            "soft",
            None,
            Some(now),
        )
        .unwrap();
        let codex_value = py_to_json_value(codex.bind(py)).unwrap();
        assert_eq!(codex_value["mode"], json!("soft"));
        let replacement = py_provider_disable_set_until(
            py,
            &home,
            "claude",
            now + 60.0,
            "binding-test",
            "soft",
            Some(now),
        )
        .unwrap();
        let replacement_value = py_to_json_value(replacement.bind(py)).unwrap();
        assert_eq!(replacement_value["mode"], json!("soft"));

        let snapshot = py_provider_disable_get(py, &home, Some(now)).unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["version"], json!(2));
        assert_eq!(
            snapshot_value["disables"],
            json!([replacement_value, codex_value])
        );

        assert!(py_provider_disable_clear(&home, "codex").unwrap());
        assert!(!py_provider_disable_clear(&home, "codex").unwrap());
    });
}

#[test]
fn provider_disable_try_set_bindings_report_first_writer() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        let first = py_provider_disable_try_set_relative(
            py,
            &home,
            "claude",
            "usage_limit",
            "soft",
            Some(900.0),
            Some(now),
        )
        .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        assert_eq!(
            first_value,
            json!({
                "version": 2,
                "inserted": true,
                "record": {
                    "version": 2,
                    "provider": "claude",
                    "created_at": now,
                    "expires_at": now + 900.0,
                    "source": "usage_limit",
                    "mode": "soft",
                },
            })
        );

        let lost = py_provider_disable_try_set_until(
            py,
            &home,
            "claude",
            now + 3_600.0,
            "ace",
            "hard",
            Some(now),
        )
        .unwrap();
        let lost_value = py_to_json_value(lost.bind(py)).unwrap();
        assert_eq!(lost_value["inserted"], json!(false));
        assert_eq!(lost_value["record"], first_value["record"]);
    });
}

#[test]
fn provider_disable_binding_rejects_invalid_values() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    Python::with_gil(|py| {
        let error = py_provider_disable_set_relative(
            py,
            &home,
            "",
            "test",
            "hard",
            None,
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let error = py_provider_disable_set_until(
            py,
            &home,
            "claude",
            1.0,
            "test",
            "hard",
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let error = py_provider_disable_try_set_relative(
            py,
            &home,
            "",
            "test",
            "hard",
            None,
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let error = py_provider_disable_try_set_until(
            py,
            &home,
            "claude",
            1.0,
            "test",
            "hard",
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        for (call_name, error) in [
            (
                "set_relative",
                py_provider_disable_set_relative(
                    py,
                    &home,
                    "claude",
                    "test",
                    "medium",
                    Some(1.0),
                    Some(1.0),
                )
                .unwrap_err(),
            ),
            (
                "set_until",
                py_provider_disable_set_until(
                    py,
                    &home,
                    "claude",
                    2.0,
                    "test",
                    "MEDIUM",
                    Some(1.0),
                )
                .unwrap_err(),
            ),
            (
                "try_set_relative",
                py_provider_disable_try_set_relative(
                    py,
                    &home,
                    "claude",
                    "test",
                    "soft ",
                    Some(1.0),
                    Some(1.0),
                )
                .unwrap_err(),
            ),
            (
                "try_set_until",
                py_provider_disable_try_set_until(
                    py,
                    &home,
                    "claude",
                    2.0,
                    "test",
                    "",
                    Some(1.0),
                )
                .unwrap_err(),
            ),
        ] {
            assert!(
                error.is_instance_of::<PyValueError>(py),
                "{call_name} should reject an unknown mode"
            );
        }
        let snapshot = py_provider_disable_get(py, &home, Some(1.0)).unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["disables"], json!([]));
    });
}

#[test]
fn provider_priority_bindings_round_trip_conflict_and_clear() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        assert_eq!(
            py_provider_priority_wire_schema_version(),
            sase_core::PROVIDER_PRIORITY_WIRE_SCHEMA_VERSION
        );
        assert_eq!(
            py_provider_routing_context_wire_schema_version(),
            sase_core::PROVIDER_ROUTING_CONTEXT_WIRE_SCHEMA_VERSION
        );
        assert_eq!(
            py_provider_availability_wire_schema_version(),
            sase_core::PROVIDER_AVAILABILITY_WIRE_SCHEMA_VERSION
        );

        let facts = PyDict::new_bound(py);
        facts.set_item("provider", "codex").unwrap();
        facts.set_item("registered", true).unwrap();
        facts.set_item("user_facing", true).unwrap();
        facts.set_item("cli_available", true).unwrap();

        let first = py_provider_priority_set_relative(
            py,
            &home,
            "codex",
            "ace",
            &facts,
            None,
            Some(7_200.0),
            Some(now),
        )
        .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        let first_record =
            json_value_to_py(py, &first_value["record"]).unwrap();
        assert_eq!(first_value["status"], json!("changed"));
        assert_eq!(
            first_value["record"],
            json!({
                "version": 1,
                "provider": "codex",
                "created_at": now,
                "expires_at": now + 7_200.0,
                "source": "ace",
            })
        );

        let priority = py_provider_priority_get(py, &home, Some(now)).unwrap();
        let priority_value = py_to_json_value(priority.bind(py)).unwrap();
        assert_eq!(priority_value, first_value["record"]);

        let claude_facts = PyDict::new_bound(py);
        claude_facts.set_item("provider", "claude").unwrap();
        claude_facts.set_item("registered", true).unwrap();
        claude_facts.set_item("user_facing", true).unwrap();
        claude_facts.set_item("cli_available", true).unwrap();
        let conflict = py_provider_priority_set_until(
            py,
            &home,
            "claude",
            now + 60.0,
            "ace",
            &claude_facts,
            None,
            Some(now),
        )
        .unwrap();
        let conflict_value = py_to_json_value(conflict.bind(py)).unwrap();
        assert_eq!(conflict_value["status"], json!("conflict"));
        assert_eq!(conflict_value["current"], first_value["record"]);

        let replacement = py_provider_priority_set_until(
            py,
            &home,
            "claude",
            now + 60.0,
            "ace",
            &claude_facts,
            Some(first_record.bind(py)),
            Some(now),
        )
        .unwrap();
        let replacement_value = py_to_json_value(replacement.bind(py)).unwrap();
        let replacement_record =
            json_value_to_py(py, &replacement_value["record"]).unwrap();
        assert_eq!(replacement_value["status"], json!("changed"));
        assert_eq!(replacement_value["record"]["provider"], json!("claude"));

        let stale_clear = py_provider_priority_clear(
            py,
            &home,
            Some(first_record.bind(py)),
            Some(now),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(stale_clear.bind(py)).unwrap()["status"],
            json!("conflict")
        );
        let clear = py_provider_priority_clear(
            py,
            &home,
            Some(replacement_record.bind(py)),
            Some(now),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(clear.bind(py)).unwrap()["status"],
            json!("changed")
        );
        let second_clear =
            py_provider_priority_clear(py, &home, None, Some(now)).unwrap();
        assert_eq!(
            py_to_json_value(second_clear.bind(py)).unwrap()["status"],
            json!("unchanged")
        );
    });
}

#[test]
fn provider_priority_binding_context_and_policy_round_trip() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    let now = 1_800_000_000.0;
    Python::with_gil(|py| {
        let facts = PyDict::new_bound(py);
        facts.set_item("provider", "codex").unwrap();
        facts.set_item("registered", true).unwrap();
        facts.set_item("user_facing", true).unwrap();
        facts.set_item("cli_available", true).unwrap();
        let priority = py_provider_priority_set_relative(
            py,
            &home,
            "codex",
            "ace",
            &facts,
            None,
            None,
            Some(now),
        )
        .unwrap();
        let priority_value = py_to_json_value(priority.bind(py)).unwrap();
        let priority_record =
            json_value_to_py(py, &priority_value["record"]).unwrap();
        py_provider_disable_set_relative(
            py,
            &home,
            "grok",
            "usage_limit",
            "soft",
            None,
            Some(now),
        )
        .unwrap();

        let context =
            py_provider_routing_context_get(py, &home, Some(now)).unwrap();
        let context_value = py_to_json_value(context.bind(py)).unwrap();
        assert_eq!(context_value["priority"]["provider"], json!("codex"));
        assert_eq!(context_value["disables"][0]["provider"], json!("grok"));

        let claude = PyDict::new_bound(py);
        claude.set_item("provider", "claude").unwrap();
        claude.set_item("registered", true).unwrap();
        claude.set_item("user_facing", true).unwrap();
        claude.set_item("cli_available", true).unwrap();
        let classified = provider_availability_classify(
            py,
            context.bind(py).downcast::<PyDict>().unwrap(),
            &claude,
        )
        .unwrap();
        let classified_value = py_to_json_value(classified.bind(py)).unwrap();
        assert_eq!(classified_value["availability"], json!("sparing"));
        assert_eq!(classified_value["provenance"], json!(["priority_backup"]));

        let many = PyList::empty_bound(py);
        many.append(claude.as_any()).unwrap();
        let many_result = provider_availability_classify_many(
            py,
            context.bind(py).downcast::<PyDict>().unwrap(),
            &many,
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(many_result.bind(py)).unwrap()[0],
            classified_value
        );

        let bad_bytes = PyBytes::new_bound(py, b"not json");
        let decoded =
            py_provider_priority_decode(py, Some(&bad_bytes), Some(now))
                .unwrap();
        let decoded_value = py_to_json_value(decoded.bind(py)).unwrap();
        assert_eq!(decoded_value["priority"], json!(null));
        assert_eq!(decoded_value["diagnostics"].as_array().unwrap().len(), 1);

        let disables = PyList::empty_bound(py);
        for item in context_value["disables"].as_array().unwrap() {
            disables
                .append(json_value_to_py(py, item).unwrap())
                .unwrap();
        }
        let context_from_parts = provider_routing_context_from_parts(
            py,
            &disables,
            priority_record.bind(py),
            now,
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(context_from_parts.bind(py)).unwrap()["priority"],
            priority_value["record"]
        );
    });
}

#[test]
fn provider_priority_binding_rejects_invalid_values() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().to_string_lossy();
    Python::with_gil(|py| {
        let facts = PyDict::new_bound(py);
        facts.set_item("provider", "codex").unwrap();
        facts.set_item("registered", true).unwrap();
        facts.set_item("user_facing", true).unwrap();
        facts.set_item("cli_available", true).unwrap();
        let error = py_provider_priority_set_relative(
            py,
            &home,
            "",
            "ace",
            &facts,
            None,
            None,
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let error = py_provider_priority_set_until(
            py,
            &home,
            "codex",
            1.0,
            "ace",
            &facts,
            None,
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));

        let mut expected = serde_json::to_value(json!({
            "version": 1,
            "provider": "codex",
            "created_at": 1.0,
            "expires_at": null,
            "source": "ace",
        }))
        .unwrap();
        expected["source"] = json!("");
        let expected_py = json_value_to_py(py, &expected).unwrap();
        let error = py_provider_priority_clear(
            py,
            &home,
            Some(expected_py.bind(py)),
            Some(1.0),
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_pool_eligibility_bindings_cover_soft_vs_backup() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let disable = PyDict::new_bound(py);
        disable.set_item("version", 2).unwrap();
        disable.set_item("provider", "claude").unwrap();
        disable.set_item("created_at", 1_800_000_000.0).unwrap();
        disable.set_item("expires_at", py.None()).unwrap();
        disable.set_item("source", "ace").unwrap();
        disable.set_item("mode", "soft").unwrap();
        let disables = PyList::empty_bound(py);
        disables.append(disable.as_any()).unwrap();

        let priority = PyDict::new_bound(py);
        priority.set_item("version", 1).unwrap();
        priority.set_item("provider", "grok").unwrap();
        priority.set_item("created_at", 1_800_000_000.0).unwrap();
        priority.set_item("expires_at", py.None()).unwrap();
        priority.set_item("source", "ace").unwrap();

        let context = provider_routing_context_from_parts(
            py,
            &disables,
            priority.as_any(),
            1_800_000_000.0,
        )
        .unwrap();
        let context_dict = context.bind(py).downcast::<PyDict>().unwrap();

        let mut records = Vec::new();
        for provider in ["claude", "codex"] {
            let facts = PyDict::new_bound(py);
            facts.set_item("provider", provider).unwrap();
            facts.set_item("registered", true).unwrap();
            facts.set_item("user_facing", true).unwrap();
            facts.set_item("cli_available", true).unwrap();
            records.push(
                provider_availability_classify(py, context_dict, &facts)
                    .unwrap(),
            );
        }
        let record_list = PyList::empty_bound(py);
        for record in &records {
            record_list.append(record.bind(py).as_any()).unwrap();
        }

        let mask = provider_pool_eligibility_mask(py, &record_list).unwrap();
        assert_eq!(
            py_to_json_value(mask.bind(py)).unwrap(),
            json!([false, true])
        );
        assert!(!provider_pool_reservation_eligible(&record_list, 0).unwrap());
        assert!(provider_pool_reservation_eligible(&record_list, 1).unwrap());

        let empty = PyList::empty_bound(py);
        let error = provider_pool_eligibility_mask(py, &empty).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        let error =
            provider_pool_reservation_eligible(&record_list, -1).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        let error =
            provider_pool_reservation_eligible(&record_list, 2).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_usage_bindings_project_remaining_and_reject_invalid() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        assert_eq!(py_provider_usage_observation_schema_version(), 1);
        assert_eq!(py_provider_usage_public_schema_version(), 1);
        assert_eq!(py_provider_usage_indicator_schema_version(), 1);
        assert_eq!(py_provider_usage_collector_failing_threshold(), 3);
        assert_eq!(py_provider_usage_remaining_percent(12.5).unwrap(), 87.5);
        assert_eq!(
            py_provider_usage_format_remaining_text(0.4).unwrap(),
            "99% left"
        );
        assert_eq!(
            py_provider_usage_classify_freshness(now - 900.0, now, 300.0)
                .unwrap(),
            "stale"
        );

        let observation = json!({
            "schema_version": 1,
            "provider": "alpha",
            "context_id": "ctx-alpha",
            "account_generation": 1,
            "ordering_token": now - 10.0,
            "received_at": now - 5.0,
            "source": "probe",
            "outcome": "ok",
            "reason_code": null,
            "diagnostic": null,
            "completeness": "complete",
            "account_mode": "subscription",
            "plan": null,
            "windows": [{
                "key": "week",
                "label": "Weekly",
                "used_percent": 94.0,
                "resets_at": now + 3600.0,
                "duration_seconds": null,
                "period_start": null,
                "applicability": {"kind": "account"},
                "observed_at": now - 10.0,
                "source": "probe",
                "vendor_state": "allowed"
            }]
        });
        let observation_obj = json_value_to_py(py, &observation).unwrap();
        let observation_dict =
            observation_obj.bind(py).downcast::<PyDict>().unwrap();
        let validated =
            py_provider_usage_validate_observation(py, observation_dict, now)
                .unwrap();
        let validated_value = py_to_json_value(validated.bind(py)).unwrap();
        assert_eq!(validated_value["provider"], json!("alpha"));

        let observations = PyList::empty_bound(py);
        observations.append(observation_dict.as_any()).unwrap();
        let snapshot = py_provider_usage_project_snapshot(
            py,
            &observations,
            now,
            300.0,
            75.0,
            90.0,
        )
        .unwrap();
        let snapshot_value = py_to_json_value(snapshot.bind(py)).unwrap();
        assert_eq!(snapshot_value["schema_version"], json!(1));
        assert_eq!(
            snapshot_value["providers"][0]["windows"][0]["remaining_percent"],
            json!(6.0)
        );
        assert_eq!(
            snapshot_value["providers"][0]["summary"]["remaining_percent"],
            json!(6.0)
        );
        let fable_observation = json!({
            "schema_version": 1,
            "provider": "claude",
            "context_id": "ctx-claude",
            "account_generation": 1,
            "ordering_token": now - 10.0,
            "received_at": now - 5.0,
            "source": "stream_event",
            "outcome": "ok",
            "reason_code": null,
            "diagnostic": null,
            "completeness": "partial",
            "account_mode": "subscription",
            "plan": null,
            "windows": [{
                "key": "window:seven-day-overage-included",
                "label": "Claude seven_day_overage_included",
                "used_percent": 82.0,
                "resets_at": now + 604800.0,
                "duration_seconds": null,
                "period_start": null,
                "applicability": {
                    "kind": "unknown",
                    "vendor_label": "seven_day_overage_included",
                    "vendor_id": "seven-day-overage-included"
                },
                "observed_at": now - 10.0,
                "source": "stream_event",
                "vendor_state": "unknown"
            }]
        });
        let fable_obj = json_value_to_py(py, &fable_observation).unwrap();
        let fable_dict = fable_obj.bind(py).downcast::<PyDict>().unwrap();
        let fable_validated =
            py_provider_usage_validate_observation(py, fable_dict, now)
                .unwrap();
        let fable_value = py_to_json_value(fable_validated.bind(py)).unwrap();
        assert_eq!(
            fable_value["windows"][0]["key"],
            json!("weekly:claude-fable-5")
        );
        assert_eq!(
            fable_value["windows"][0]["label"],
            json!("Claude weekly Fable")
        );
        assert_eq!(
            fable_value["windows"][0]["applicability"],
            json!({
                "kind": "models",
                "model_ids": ["claude-fable-5"]
            })
        );
        let invalid_indicator = json_value_to_py(
            py,
            &json!({
                "default": true,
                "weekly_all": "always",
            }),
        )
        .unwrap();
        let validation = py_provider_usage_validate_indicator_config(
            py,
            Some(invalid_indicator.bind(py)),
        )
        .unwrap();
        let validation_value = py_to_json_value(validation.bind(py)).unwrap();
        assert_eq!(validation_value["schema_version"], json!(1));
        assert_eq!(
            validation_value["diagnostics"][0]["path"],
            json!("indicator.default")
        );

        let request = json!({
            "schema_version": 1,
            "snapshot": snapshot_value,
            "indicator": {
                "default": {"below_remaining_percent": 10},
                "weekly_all": "always"
            },
            "now": now,
            "cadence_seconds": 300.0,
            "warn_percent": 75.0,
            "critical_percent": 90.0
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request_dict = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let indicator_projection =
            py_provider_usage_project_indicator(py, request_dict).unwrap();
        let indicator_value =
            py_to_json_value(indicator_projection.bind(py)).unwrap();
        assert_eq!(indicator_value["schema_version"], json!(1));
        assert_eq!(indicator_value["entries"][0]["window_key"], json!("week"));
        assert_eq!(
            indicator_value["entries"][0]["remaining_percent"],
            json!(6.0)
        );

        let windows = snapshot_value["providers"][0]["windows"]
            .as_array()
            .unwrap();
        let window_list = PyList::empty_bound(py);
        for window in windows {
            window_list
                .append(json_value_to_py(py, window).unwrap())
                .unwrap();
        }
        let model_summary =
            py_provider_usage_summarize_for_model(py, &window_list, "x-1")
                .unwrap();
        let model_value = py_to_json_value(model_summary.bind(py)).unwrap();
        assert_eq!(model_value["remaining_percent"], json!(6.0));

        let account =
            json_value_to_py(py, &json!({"kind": "account"})).unwrap();
        let account_dict = account.bind(py).downcast::<PyDict>().unwrap();
        assert_eq!(
            py_provider_usage_window_applies(account_dict, Some("x-1"))
                .unwrap(),
            "applies"
        );

        let error = py_provider_usage_remaining_percent(-1.0).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        let error = py_provider_usage_project_snapshot(
            py,
            &observations,
            now,
            30.0,
            75.0,
            90.0,
        )
        .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_usage_normalize_grok_billing_round_trips_and_rejects_nonfinite() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let request = json!({
            "schema_version": 1,
            "payload": {
                "subscription_tier": "SuperGrok Heavy",
                "config": {
                    "currentPeriod": {
                        "type": "USAGE_PERIOD_TYPE_WEEKLY",
                        "start": "2027-01-15T00:00:00Z",
                        "end": "2027-01-22T00:00:00Z",
                    },
                    "isUnifiedBillingUser": true,
                }
            },
            "provider": "grok",
            "context_id": "probe",
            "account_generation": 1,
            "request_started_at": now,
            "now": now,
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request_dict = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let observation =
            py_provider_usage_normalize_grok_billing(py, request_dict).unwrap();
        let value = py_to_json_value(observation.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("ok"));
        assert_eq!(value["plan"], json!("SuperGrok Heavy"));
        assert_eq!(value["windows"][0]["used_percent"], json!(0.0));
        assert_eq!(value["windows"][0]["key"], json!("included_weekly"));

        for percent in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let invalid = json!({
                "schema_version": 1,
                "payload": {
                    "config": {
                        "creditUsagePercent": 0.0,
                        "isUnifiedBillingUser": true,
                        "monthlyLimit": {"val": 1000},
                        "used": {"val": 0},
                    }
                },
                "provider": "grok",
                "context_id": "probe",
                "account_generation": 1,
                "request_started_at": now,
                "now": now,
            });
            let invalid_obj = json_value_to_py(py, &invalid).unwrap();
            let invalid_dict =
                invalid_obj.bind(py).downcast::<PyDict>().unwrap();
            let payload = invalid_dict.get_item("payload").unwrap().unwrap();
            let payload_dict = payload.downcast::<PyDict>().unwrap();
            let config = payload_dict.get_item("config").unwrap().unwrap();
            let config_dict = config.downcast::<PyDict>().unwrap();
            config_dict.set_item("creditUsagePercent", percent).unwrap();
            let error =
                py_provider_usage_normalize_grok_billing(py, invalid_dict)
                    .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            assert!(error.to_string().to_lowercase().contains("non-finite"));
        }
    });
}

#[test]
fn provider_usage_normalize_muse_usage_round_trips_and_separates_errors() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_789_921_260.0;
        let request = |payload: serde_json::Value, schema_version: u32| {
            json!({
                "schema_version": schema_version,
                "payload": payload,
                "provider": "muse",
                "context_id": "probe",
                "account_generation": 1,
                "request_started_at": now - 3.0,
                "now": now,
            })
        };
        let call = |request: serde_json::Value| {
            let request_obj = json_value_to_py(py, &request).unwrap();
            let request_dict =
                request_obj.bind(py).downcast::<PyDict>().unwrap();
            py_provider_usage_normalize_muse_usage(py, request_dict)
        };

        let live = json!({
            "usage": {
                "observedAtMs": 1_789_921_255_705_u64,
                "tier": "27681631238169137",
                "weekly": {
                    "usedPercent": 0,
                    "resetsAtMs": 1_789_948_800_000_u64,
                },
                "window": {
                    "usedPercent": 0,
                    "windowDurationMins": 300,
                    "resetsAtMs": 1_789_935_797_000_u64,
                },
            }
        });
        let observation = call(request(live, 1)).unwrap();
        let value = py_to_json_value(observation.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("ok"));
        assert_eq!(value["plan"], json!(null));
        assert_eq!(value["windows"][0]["key"], json!("session"));
        assert_eq!(value["windows"][0]["duration_seconds"], json!(18_000.0));
        assert_eq!(value["windows"][1]["key"], json!("weekly"));
        assert_eq!(value["windows"][1]["duration_seconds"], json!(null));
        assert_eq!(value["windows"][1]["period_start"], json!(null));

        let absent = call(request(json!({}), 1)).unwrap();
        let value = py_to_json_value(absent.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("ok"));
        assert_eq!(value["authoritative_empty"], json!(true));
        assert_eq!(value["diagnostic"], json!("muse_usage_not_yet_observed"));
        assert_eq!(value["windows"], json!([]));

        // Malformed vendor data is a structured observation, not an
        // exception; a malformed request is an exception.
        let malformed = call(request(json!({"usage": 7}), 1)).unwrap();
        let value = py_to_json_value(malformed.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("error"));
        assert_eq!(value["reason_code"], json!("malformed_payload"));

        let error = call(request(json!({}), 2)).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_usage_normalize_agy_usage_round_trips_and_separates_errors() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let payload = || {
            json!({
                "conversation_id": "",
                "status": "SUCCESS",
                "response": "Gemini Models\tWeekly Limit Remaining\t96%\t2027-01-21T08:00:00Z\n",
                "duration_seconds": 0,
                "num_turns": 0,
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "thinking_tokens": 0,
                    "cache_read_tokens": 0,
                    "total_tokens": 0,
                },
                "command": {
                    "name": "usage",
                    "data": {
                        "description": "groups share limits",
                        "groups": [
                            {
                                "name": "Gemini Models",
                                "description": "Models within this group: Gemini Flash, Gemini Pro",
                                "buckets": [
                                    {
                                        "id": "gemini-weekly",
                                        "name": "Weekly Limit Remaining",
                                        "description": "weekly",
                                        "window": "weekly",
                                        "remaining_fraction": 0.96,
                                        "reset_time": "2027-01-21T08:00:00Z",
                                    },
                                    {
                                        "id": "gemini-5h",
                                        "name": "Five Hour Limit Remaining",
                                        "description": "5h",
                                        "window": "5h",
                                        "remaining_fraction": 1,
                                        "reset_time": "2027-01-15T12:00:00Z",
                                    },
                                ],
                            },
                            {
                                "name": "Claude and GPT models",
                                "buckets": [
                                    {
                                        "id": "3p-weekly",
                                        "name": "Weekly Limit Remaining",
                                        "window": "weekly",
                                        "remaining_fraction": 1,
                                        "reset_time": "2027-01-22T08:00:00Z",
                                    },
                                    {
                                        "id": "3p-5h",
                                        "name": "Five Hour Limit Remaining",
                                        "window": "5h",
                                        "remaining_fraction": 1,
                                        "reset_time": "2027-01-15T13:00:00Z",
                                    },
                                ],
                            },
                        ],
                    },
                },
            })
        };
        let request = |payload: serde_json::Value, schema_version: u32| {
            json!({
                "schema_version": schema_version,
                "payload": payload,
                "model_ids": ["gemini-3-flash", "claude-opus-4-6"],
                "provider": "agy",
                "context_id": "probe",
                "account_generation": 1,
                "request_started_at": now - 3.0,
                "now": now,
            })
        };
        let call = |request: serde_json::Value| {
            let request_obj = json_value_to_py(py, &request).unwrap();
            let request_dict =
                request_obj.bind(py).downcast::<PyDict>().unwrap();
            py_provider_usage_normalize_agy_usage(py, request_dict)
        };

        let observation = call(request(payload(), 1)).unwrap();
        let value = py_to_json_value(observation.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("ok"));
        assert_eq!(value["plan"], json!(null));
        assert_eq!(value["account_mode"], json!("subscription"));
        assert_eq!(value["windows"][0]["key"], json!("gemini-weekly"));
        assert_eq!(
            value["windows"][0]["applicability"]["kind"],
            json!("model_family")
        );
        assert_eq!(
            value["windows"][0]["applicability"]["family"],
            json!("gemini")
        );
        assert_eq!(
            value["windows"][0]["applicability"]["model_ids"],
            json!(["gemini-3-flash"])
        );
        assert_eq!(value["windows"][0]["duration_seconds"], json!(604800.0));
        assert_eq!(value["windows"][1]["key"], json!("gemini-5h"));
        assert_eq!(value["windows"][2]["applicability"]["family"], json!("3p"));

        // Malformed vendor data is a structured observation, not an
        // exception; a malformed request is an exception.
        let malformed = call(request(json!({"status": 7}), 1)).unwrap();
        let value = py_to_json_value(malformed.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("error"));
        assert_eq!(value["reason_code"], json!("malformed_payload"));

        let logged_out = call(request(
            json!({
                "status": "ERROR",
                "error": "authentication failed or timed out",
            }),
            1,
        ))
        .unwrap();
        let value = py_to_json_value(logged_out.bind(py)).unwrap();
        assert_eq!(value["outcome"], json!("unauthenticated"));
        assert_eq!(value["reason_code"], json!("logged_out"));

        let error = call(request(payload(), 2)).unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn provider_usage_store_bindings_record_load_context_and_reserve() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let temp = tempdir().unwrap();
        let home = temp.path().to_string_lossy().to_string();
        assert_eq!(py_provider_usage_store_schema_version(), 1);
        assert_eq!(py_provider_usage_collector_failing_threshold(), 3);
        assert!(py_provider_usage_state_path(&home)
            .ends_with("llm_provider_usage.json"));

        let observation = json!({
            "schema_version": 1,
            "provider": "alpha",
            "context_id": "ctx-alpha",
            "account_generation": 1,
            "ordering_token": now - 10.0,
            "received_at": now - 5.0,
            "source": "probe",
            "outcome": "ok",
            "reason_code": null,
            "diagnostic": null,
            "completeness": "complete",
            "authoritative_empty": false,
            "account_mode": "subscription",
            "plan": null,
            "windows": [{
                "key": "week",
                "label": "Weekly",
                "used_percent": 94.0,
                "resets_at": now + 3600.0,
                "duration_seconds": null,
                "period_start": null,
                "applicability": {"kind": "account"},
                "observed_at": now - 10.0,
                "source": "probe",
                "vendor_state": "allowed"
            }]
        });
        let observation_obj = json_value_to_py(py, &observation).unwrap();
        let observation_dict =
            observation_obj.bind(py).downcast::<PyDict>().unwrap();
        let write = py_provider_usage_record_observation(
            py,
            &home,
            observation_dict,
            now,
        )
        .unwrap();
        let write_value = py_to_json_value(write.bind(py)).unwrap();
        assert_eq!(write_value["status"], json!("recorded"));
        assert_eq!(write_value["accepted"], json!(true));

        let read =
            py_provider_usage_load(py, &home, now, 300.0, 75.0, 90.0, None)
                .unwrap();
        let read_value = py_to_json_value(read.bind(py)).unwrap();
        assert_eq!(read_value["version"], json!(1));
        assert_eq!(
            read_value["snapshot"]["providers"][0]["windows"][0]
                ["remaining_percent"],
            json!(6.0)
        );

        let context = py_provider_usage_prepare_account_context(
            py,
            &home,
            "alpha",
            "ctx-beta",
            now + 1.0,
        )
        .unwrap();
        let context_value = py_to_json_value(context.bind(py)).unwrap();
        assert_eq!(context_value["account_generation"], json!(2));
        assert_eq!(context_value["changed"], json!(true));

        let stale = py_provider_usage_record_observation(
            py,
            &home,
            observation_dict,
            now + 2.0,
        )
        .unwrap();
        let stale_value = py_to_json_value(stale.bind(py)).unwrap();
        assert_eq!(stale_value["status"], json!("stale_writer"));
        assert_eq!(stale_value["accepted"], json!(false));

        let request = json!({
            "provider": "alpha",
            "context_id": "ctx-beta",
            "account_generation": 2,
            "operation_id": "op-1",
            "ttl_seconds": 10.0
        });
        let request_obj = json_value_to_py(py, &request).unwrap();
        let request_dict = request_obj.bind(py).downcast::<PyDict>().unwrap();
        let first = py_provider_usage_reserve_refresh(
            py,
            &home,
            request_dict,
            now + 2.0,
        )
        .unwrap();
        let first_value = py_to_json_value(first.bind(py)).unwrap();
        assert_eq!(first_value["status"], json!("reserved"));
        let joined = py_provider_usage_reserve_refresh(
            py,
            &home,
            request_dict,
            now + 3.0,
        )
        .unwrap();
        let joined_value = py_to_json_value(joined.bind(py)).unwrap();
        assert_eq!(joined_value["status"], json!("joined"));
        let lease_id = first_value["reservation"]["lease_id"].as_str().unwrap();
        assert!(py_provider_usage_release_refresh(
            &home,
            "alpha",
            "ctx-beta",
            2,
            lease_id,
            now + 4.0,
        )
        .unwrap());
    });
}

#[test]
fn provider_usage_record_attempt_binding_covers_adaptive_fields() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let temp = tempdir().unwrap();
        let home = temp.path().to_string_lossy().to_string();

        // Legacy dicts without the new keys keep working.
        let legacy = json!({
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "outcome": "error",
            "retry_after_seconds": null,
            "cadence_seconds": 300.0,
        });
        let legacy_obj = json_value_to_py(py, &legacy).unwrap();
        let legacy_dict = legacy_obj.bind(py).downcast::<PyDict>().unwrap();
        let out = py_provider_usage_record_refresh_attempt(
            py,
            &home,
            legacy_dict,
            now,
        )
        .unwrap();
        let value = py_to_json_value(out.bind(py)).unwrap();
        assert_eq!(value["consecutive_failures"], json!(1));
        assert_eq!(value["cooldown_until"], json!(now + 5.0));

        // Adaptive rate-limit attempts clamp Retry-After and escalate.
        let adaptive = json!({
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "outcome": "error",
            "retry_after_seconds": 60.0,
            "cadence_seconds": 300.0,
            "reason_code": "rate_limited",
            "min_interval_seconds": 120.0,
            "cli_fingerprint": "fp-1",
            "adaptive": true,
        });
        let adaptive_obj = json_value_to_py(py, &adaptive).unwrap();
        let adaptive_dict = adaptive_obj.bind(py).downcast::<PyDict>().unwrap();
        let out = py_provider_usage_record_refresh_attempt(
            py,
            &home,
            adaptive_dict,
            now + 1.0,
        )
        .unwrap();
        let value = py_to_json_value(out.bind(py)).unwrap();
        assert_eq!(value["retry_after_until"], json!(now + 1.0 + 900.0));
        assert_eq!(value["consecutive_rate_limits"], json!(1));
        assert_eq!(value["cooldown_until"], json!(now + 1.0 + 60.0));
        assert_eq!(value["last_failure_reason"], json!("rate_limited"));

        // Unknown reason codes and bad floors are rejected.
        let unknown = json!({
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "outcome": "error",
            "retry_after_seconds": null,
            "cadence_seconds": 300.0,
            "reason_code": "vendor_changed",
            "adaptive": true,
        });
        let unknown_obj = json_value_to_py(py, &unknown).unwrap();
        let unknown_dict = unknown_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_provider_usage_record_refresh_attempt(
            py,
            &home,
            unknown_dict,
            now + 2.0,
        )
        .is_err());
        let bad_floor = json!({
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "outcome": "error",
            "retry_after_seconds": null,
            "cadence_seconds": 300.0,
            "reason_code": "timeout",
            "min_interval_seconds": 10.0,
            "adaptive": true,
        });
        let bad_floor_obj = json_value_to_py(py, &bad_floor).unwrap();
        let bad_floor_dict =
            bad_floor_obj.bind(py).downcast::<PyDict>().unwrap();
        assert!(py_provider_usage_record_refresh_attempt(
            py,
            &home,
            bad_floor_dict,
            now + 2.0,
        )
        .is_err());
    });
}

#[test]
fn provider_usage_observation_binding_clamps_retry_after() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let observation = json!({
            "schema_version": 1,
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "ordering_token": now - 10.0,
            "received_at": now - 5.0,
            "source": "probe",
            "outcome": "error",
            "reason_code": "rate_limited",
            "retry_after_seconds": 100_000.0,
            "diagnostic": null,
            "completeness": "partial",
            "authoritative_empty": false,
            "account_mode": null,
            "plan": null,
            "windows": []
        });
        let observation_obj = json_value_to_py(py, &observation).unwrap();
        let observation_dict =
            observation_obj.bind(py).downcast::<PyDict>().unwrap();
        let validated =
            py_provider_usage_validate_observation(py, observation_dict, now)
                .unwrap();
        let value = py_to_json_value(validated.bind(py)).unwrap();
        assert_eq!(value["reason_code"], json!("rate_limited"));
        assert_eq!(value["retry_after_seconds"], json!(86_400.0));
    });
}

fn binding_dict<'py>(
    py: Python<'py>,
    value: serde_json::Value,
) -> Bound<'py, PyDict> {
    json_value_to_py(py, &value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn provider_usage_adaptive_due_and_admit_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let temp = tempdir().unwrap();
        let home = temp.path().to_string_lossy().to_string();

        // Legacy admit dicts without the new keys still reserve.
        let admit = binding_dict(
            py,
            json!({
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "operation_id": "op-1",
                "ttl_seconds": 60.0,
                "cadence_seconds": 300.0,
                "explicit": false,
            }),
        );
        let reserved =
            py_provider_usage_admit_refresh(py, &home, &admit, now).unwrap();
        let reserved_value = py_to_json_value(reserved.bind(py)).unwrap();
        assert_eq!(reserved_value["status"], json!("reserved"));

        // An adaptive floor defers the next automatic request.
        let floored_due = binding_dict(
            py,
            json!({
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "cadence_seconds": 300.0,
                "explicit": false,
                "adaptive": true,
                "min_interval_seconds": 300.0,
            }),
        );
        let floored =
            py_provider_usage_refresh_due(py, &home, &floored_due, now + 1.0)
                .unwrap();
        let floored_value = py_to_json_value(floored.bind(py)).unwrap();
        assert_eq!(floored_value["due"], json!(false));
        assert_eq!(floored_value["reason"], json!("floor"));

        // A parked provider unparks on a CLI fingerprint change.
        let attempt = binding_dict(
            py,
            json!({
                "provider": "beta",
                "context_id": "ctx",
                "account_generation": 1,
                "outcome": "unsupported",
                "retry_after_seconds": null,
                "cadence_seconds": 300.0,
                "reason_code": "not_installed",
                "cli_fingerprint": "fp-1",
                "adaptive": true,
            }),
        );
        py_provider_usage_record_refresh_attempt(py, &home, &attempt, now)
            .unwrap();
        let parked_due = binding_dict(
            py,
            json!({
                "provider": "beta",
                "context_id": "ctx",
                "account_generation": 1,
                "cadence_seconds": 300.0,
                "explicit": false,
                "adaptive": true,
                "cli_fingerprint": "fp-1",
            }),
        );
        let parked =
            py_provider_usage_refresh_due(py, &home, &parked_due, now + 1.0)
                .unwrap();
        assert_eq!(
            py_to_json_value(parked.bind(py)).unwrap()["reason"],
            json!("parked")
        );
        let changed_due = binding_dict(
            py,
            json!({
                "provider": "beta",
                "context_id": "ctx",
                "account_generation": 1,
                "cadence_seconds": 300.0,
                "explicit": false,
                "adaptive": true,
                "cli_fingerprint": "fp-2",
            }),
        );
        let changed =
            py_provider_usage_refresh_due(py, &home, &changed_due, now + 1.0)
                .unwrap();
        let changed_value = py_to_json_value(changed.bind(py)).unwrap();
        assert_eq!(changed_value["due"], json!(true));
        assert_eq!(changed_value["reason"], json!("cli_changed"));

        // Bad adaptive fields are rejected.
        let bad_floor = binding_dict(
            py,
            json!({
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "cadence_seconds": 300.0,
                "explicit": false,
                "adaptive": true,
                "min_interval_seconds": 10.0,
            }),
        );
        assert!(
            py_provider_usage_refresh_due(py, &home, &bad_floor, now).is_err()
        );
    });
}

#[test]
fn provider_usage_hot_and_reservation_listing_bindings() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let temp = tempdir().unwrap();
        let home = temp.path().to_string_lossy().to_string();

        let hot = binding_dict(
            py,
            json!({
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "until": now + 900.0,
            }),
        );
        let marked = py_provider_usage_mark_hot(py, &home, &hot, now).unwrap();
        let marked_value = py_to_json_value(marked.bind(py)).unwrap();
        assert_eq!(marked_value["marked"], json!(true));
        assert_eq!(marked_value["hot_until"], json!(now + 900.0));
        let again =
            py_provider_usage_mark_hot(py, &home, &hot, now + 1.0).unwrap();
        assert_eq!(
            py_to_json_value(again.bind(py)).unwrap()["marked"],
            json!(false)
        );

        let reserve = binding_dict(
            py,
            json!({
                "provider": "alpha",
                "context_id": "ctx",
                "account_generation": 1,
                "operation_id": "op-1",
                "ttl_seconds": 10.0,
            }),
        );
        py_provider_usage_reserve_refresh(py, &home, &reserve, now).unwrap();
        let live =
            py_provider_usage_list_refresh_reservations(py, &home, now + 1.0)
                .unwrap();
        let live_value = py_to_json_value(live.bind(py)).unwrap();
        assert_eq!(live_value["reservations"].as_array().unwrap().len(), 1);
        assert_eq!(
            live_value["reservations"][0]["operation_id"],
            json!("op-1")
        );
        let expired =
            py_provider_usage_list_refresh_reservations(py, &home, now + 11.0)
                .unwrap();
        assert!(py_to_json_value(expired.bind(py)).unwrap()["reservations"]
            .as_array()
            .unwrap()
            .is_empty());
    });
}

#[test]
fn provider_usage_load_with_floors_binding() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let now = 1_800_000_000.0;
        let temp = tempdir().unwrap();
        let home = temp.path().to_string_lossy().to_string();

        let observation = json!({
            "schema_version": 1,
            "provider": "alpha",
            "context_id": "ctx",
            "account_generation": 1,
            "ordering_token": now - 700.0,
            "received_at": now - 699.0,
            "source": "probe",
            "outcome": "ok",
            "reason_code": null,
            "diagnostic": null,
            "completeness": "complete",
            "authoritative_empty": false,
            "account_mode": "subscription",
            "plan": null,
            "windows": [{
                "key": "week",
                "label": "Weekly",
                "used_percent": 10.0,
                "resets_at": now + 3600.0,
                "duration_seconds": null,
                "period_start": null,
                "applicability": {"kind": "account"},
                "observed_at": now - 700.0,
                "source": "probe",
                "vendor_state": "allowed"
            }]
        });
        let observation_dict = binding_dict(py, observation);
        py_provider_usage_record_observation(py, &home, &observation_dict, now)
            .unwrap();

        let base =
            py_provider_usage_load(py, &home, now, 300.0, 75.0, 90.0, None)
                .unwrap();
        assert_eq!(
            py_to_json_value(base.bind(py)).unwrap()["snapshot"]["providers"]
                [0]["windows"][0]["freshness"],
            json!("stale")
        );

        let floors = binding_dict(py, json!({"alpha": 600.0}));
        let floored = py_provider_usage_load(
            py,
            &home,
            now,
            300.0,
            75.0,
            90.0,
            Some(&floors),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(floored.bind(py)).unwrap()["snapshot"]
                ["providers"][0]["windows"][0]["freshness"],
            json!("fresh")
        );

        let bad_floors = binding_dict(py, json!({"alpha": 10.0}));
        assert!(py_provider_usage_load(
            py,
            &home,
            now,
            300.0,
            75.0,
            90.0,
            Some(&bad_floors)
        )
        .is_err());
    });
}

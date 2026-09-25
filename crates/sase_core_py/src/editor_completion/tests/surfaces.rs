use super::*;
use crate::agent_launch::{
    py_collect_queue_fields, py_format_queue_capacity_multiplier,
    py_format_queue_directive, py_normalize_persisted_queue_capacity,
    py_parse_queue_capacity, py_parse_queue_capacity_value,
    py_queue_directive_flag_key, py_resolve_queue_capacity_multiplier,
};
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::provider_policy::{
    py_runner_capacity_policy_schema_version, py_runner_capacity_snapshot,
};
use serde_json::json;

#[test]
fn placeholder_bindings_return_plain_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let text = "<Alpha> use <a>";
        let completion =
            py_placeholder_completion(py, text, 0, 14, None).unwrap();
        let value = py_to_json_value(completion.bind(py)).unwrap();
        assert_eq!(value["prefix"], json!("a"));
        assert_eq!(
            value["candidates"],
            json!([{"text": "Alpha", "source": "prompt"}])
        );
        assert_eq!(value["append_closing_bracket"], json!(false));
        assert_eq!(value["replacement_range"]["start"]["character"], json!(13));

        let with_common = py_placeholder_completion(
            py,
            text,
            0,
            14,
            Some(vec!["Alpha".to_string(), "anchor".to_string()]),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(with_common.bind(py)).unwrap()["candidates"],
            json!([
                {"text": "Alpha", "source": "prompt"},
                {"text": "anchor", "source": "common"},
            ])
        );

        let common_only = py_placeholder_completion(
            py,
            "<only>",
            0,
            5,
            Some(vec!["only tag".to_string()]),
        )
        .unwrap();
        assert_eq!(
            py_to_json_value(common_only.bind(py)).unwrap()["candidates"],
            json!([{"text": "only tag", "source": "common"}])
        );

        let empty =
            py_placeholder_completion(py, "<only>", 0, 5, None).unwrap();
        assert_eq!(py_to_json_value(empty.bind(py)).unwrap(), JsonValue::Null);

        let spans = py_placeholder_spans(py, "`<inline>` <live>").unwrap();
        let spans = py_to_json_value(spans.bind(py)).unwrap();
        assert_eq!(spans.as_array().unwrap().len(), 2);
        assert_eq!(spans[0]["text"], json!("inline"));
        assert_eq!(spans[0]["raw"], json!(false));
        assert_eq!(spans[0]["range"]["start"]["character"], json!(1));
        assert_eq!(spans[1]["text"], json!("live"));
        assert_eq!(spans[1]["raw"], json!(true));

        let fields =
            py_raw_placeholder_fields(py, "<live> and <live>", 60).unwrap();
        assert_eq!(
            py_to_json_value(fields.bind(py)).unwrap(),
            json!([{
                "text": "live",
                "occurrences": 2,
                "context": "<live> and <live>",
            }])
        );
        assert_eq!(
            py_substitute_raw_placeholders(
                "<live> and `<live>`",
                BTreeMap::from([("live".to_string(), "ready".to_string())]),
            ),
            "ready and `<live>`"
        );
        assert_eq!(
            py_placeholder_input_names(vec![
                "the plan".to_string(),
                "the-plan".to_string(),
            ]),
            vec!["the_plan", "the_plan_2"]
        );
    });
}

#[test]
fn at_reference_bindings_return_plain_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_at_reference_context, &module).unwrap(),
            )
            .unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_at_reference_menu, &module).unwrap(),
            )
            .unwrap();
        module.add_class::<PyAtReferenceInventory>().unwrap();
        module
            .add_function(wrap_pyfunction!(py_fuzzy_match, &module).unwrap())
            .unwrap();

        let context = module
            .getattr("at_reference_context")
            .unwrap()
            .call1(("open @fi", 0_u32, 8_u32))
            .unwrap();
        let context_value = py_to_json_value(&context).unwrap();
        assert_eq!(context_value["stage"], json!("kind"));
        assert_eq!(context_value["candidate_span"], json!([5, 8]));
        assert_eq!(context_value["replacement_span"], json!([6, 8]));
        assert_eq!(context_value["query_span"], json!([6, 8]));
        assert_eq!(context_value["query"], json!("fi"));
        assert_eq!(context_value["kind"], JsonValue::Null);
        assert_eq!(
            context_value["path_query"],
            json!({
                "directory": "",
                "partial": "fi",
                "show_hidden": false,
            })
        );

        let inventory = json_value_to_py(
            py,
            &json!({
                "kinds": [
                    {
                        "kind": "fixture",
                        "builtin": false,
                        "detail": "Custom references",
                    },
                    {
                        "kind": "file",
                        "builtin": true,
                        "detail": "Tracked files",
                    },
                ],
                "paths": [
                    {"name": "final.md", "is_dir": false},
                    {"name": "fixtures", "is_dir": true},
                    {"name": ".hidden", "is_dir": false},
                ],
                "payloads": [],
            }),
        )
        .unwrap();
        let menu = module
            .getattr("at_reference_menu")
            .unwrap()
            .call1((&context, inventory.bind(py)))
            .unwrap();
        let menu_value = py_to_json_value(&menu).unwrap();
        assert_eq!(menu_value["artifact_count"], json!(2));
        assert_eq!(menu_value["file_count"], json!(0));
        assert_eq!(menu_value["files_suppressed"], json!(true));
        assert_eq!(menu_value["shared_extension"], json!(""));
        assert_eq!(menu_value["rows"][0]["group"], json!("artifact"));
        assert_eq!(menu_value["rows"][0]["label"], json!("file"));
        assert_eq!(menu_value["rows"][0]["insertion"], json!("@file:"));
        assert_eq!(menu_value["rows"][1]["label"], json!("fixture"));

        let options =
            json_value_to_py(py, &json!({"include_files": true})).unwrap();
        let revealed_menu = module
            .getattr("at_reference_menu")
            .unwrap()
            .call1((&context, inventory.bind(py), py.None(), options.bind(py)))
            .unwrap();
        let revealed_value = py_to_json_value(&revealed_menu).unwrap();
        assert_eq!(revealed_value["file_count"], json!(2));
        assert_eq!(revealed_value["files_suppressed"], json!(false));
        assert_eq!(revealed_value["rows"][2]["group"], json!("file"));
        assert_eq!(revealed_value["rows"][2]["label"], json!("fixtures/"));
        assert_eq!(revealed_value["rows"][2]["insertion"], json!("@fixtures/"));
        assert_eq!(revealed_value["rows"][3]["label"], json!("final.md"));

        let payload_context = module
            .getattr("at_reference_context")
            .unwrap()
            .call1(("see @bug:sa", 0_u32, 11_u32))
            .unwrap();
        let payload_context_value = py_to_json_value(&payload_context).unwrap();
        assert_eq!(payload_context_value["stage"], json!("payload"));
        assert_eq!(payload_context_value["query"], json!("sa"));
        assert_eq!(payload_context_value["kind"], json!("bug"));

        let payloads = json_value_to_py(
            py,
            &json!([{
                "payload": "202607/sase_sites_hub_and_pages.md",
                "label": "SASE Sites Hub and Pages",
                "detail": "research",
                "age": "3d",
            }]),
        )
        .unwrap();
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("payloads", payloads).unwrap();
        let payload_index = module
            .getattr("AtReferenceInventory")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        assert_eq!(payload_index.len().unwrap(), 1);
        assert!(payload_index.setattr("payloads", py.None()).is_err());

        let indexed_context = module
            .getattr("at_reference_context")
            .unwrap()
            .call1(("see @research:site", 0_u32, 18_u32))
            .unwrap();
        let indexed_inventory = json_value_to_py(
            py,
            &json!({
                "kinds": [],
                "paths": [],
                "payloads": [{
                    "payload": "ignored.md",
                    "label": "Ignored",
                    "detail": "",
                    "age": "",
                }],
                "truncated_payloads": 4,
            }),
        )
        .unwrap();
        let indexed_menu = module
            .getattr("at_reference_menu")
            .unwrap()
            .call1((
                &indexed_context,
                indexed_inventory.bind(py),
                &payload_index,
            ))
            .unwrap();
        let indexed_menu = py_to_json_value(&indexed_menu).unwrap();
        assert_eq!(indexed_menu["payload_count"], json!(1));
        assert_eq!(indexed_menu["truncated_payloads"], json!(4));
        assert_eq!(
            indexed_menu["rows"][0]["label"],
            json!("202607/sase_sites_hub_and_pages.md")
        );
        assert_eq!(indexed_menu["rows"][0]["label_match"], json!([[12, 16]]));

        let fuzzy = module
            .getattr("fuzzy_match")
            .unwrap()
            .call1(("rés", "café/東京Résumé.md"))
            .unwrap();
        let fuzzy = py_to_json_value(&fuzzy).unwrap();
        assert_eq!(fuzzy["tier"], json!(2));
        assert_eq!(fuzzy["runs"], json!([[7, 10]]));
        let no_match = module
            .getattr("fuzzy_match")
            .unwrap()
            .call1(("missing", "text"))
            .unwrap();
        assert!(no_match.is_none());
    });
}

#[test]
#[cfg_attr(
    debug_assertions,
    ignore = "the 8 ms performance gate is calibrated for release builds"
)]
fn indexed_at_reference_binding_stays_below_eight_ms_for_5000_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module.add_class::<PyAtReferenceInventory>().unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_at_reference_menu, &module).unwrap(),
            )
            .unwrap();

        let payloads = (0..5_000)
            .map(|index| {
                json!({
                    "payload": format!(
                        "202607/bundle_{index:04}/artifact_{index:04}.md"
                    ),
                    "label": format!("Artifact title {index:04}"),
                    "detail": "plans",
                    "age": "now",
                })
            })
            .collect::<Vec<_>>();
        let payloads = json_value_to_py(py, &json!(payloads)).unwrap();
        let kwargs = PyDict::new_bound(py);
        kwargs.set_item("payloads", payloads).unwrap();
        let payload_index = module
            .getattr("AtReferenceInventory")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        let context = json_value_to_py(
            py,
            &json!({
                "stage": "payload",
                "candidate_span": [0, 12],
                "replacement_span": [1, 12],
                "query_span": [7, 12],
                "query": "artifact",
                "kind": "plans",
                "path_query": null,
            }),
        )
        .unwrap();
        let inventory = json_value_to_py(
            py,
            &json!({
                "kinds": [],
                "paths": [],
                "payloads": [],
                "truncated_payloads": 0,
            }),
        )
        .unwrap();
        let menu = module.getattr("at_reference_menu").unwrap();
        menu.call1((&context, &inventory, &payload_index)).unwrap();

        const SAMPLES: u32 = 40;
        let started = Instant::now();
        for _ in 0..SAMPLES {
            menu.call1((&context, &inventory, &payload_index)).unwrap();
        }
        let mean = started.elapsed() / SAMPLES;
        assert!(
            mean < Duration::from_millis(8),
            "indexed 5000-row binding mean {mean:?} exceeded 8 ms"
        );
    });
}

#[test]
fn directive_contract_and_completion_bindings_return_plain_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let contract = py_directive_contract(py, None).unwrap();
        let contract = py_to_json_value(contract.bind(py)).unwrap();
        let names: Vec<&str> = contract
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["name"].as_str().unwrap())
            .collect();
        assert_eq!(
            names,
            [
                "model",
                "effort",
                "final",
                "id",
                "clan",
                "wait",
                "queue",
                "hold",
                "dispatch",
                "if",
                "proc",
                "auto",
                "hide",
                "repeat",
                "alt",
                "xprompts_enabled",
            ]
        );
        let queue = contract
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == "queue")
            .unwrap();
        assert_eq!(queue["alias"], json!("q"));
        assert_eq!(queue.get("feature_flag"), None);
        assert!(queue["keywords"]
            .as_array()
            .unwrap()
            .iter()
            .any(|keyword| keyword["name"] == "weight"
                && keyword["value_role"] == "non_negative_float"));
        assert_eq!(py_queue_directive_flag_key(), "queue_directive");
        let occurrences = json_value_to_py(
            py,
            &json!([{
                "source": "%q(5, w=0.25)",
                "source_span": [0, 14],
                "args": [{"value": "5"}, {"name": "w", "value": "0.25"}],
                "has_plus_suffix": false
            }]),
        )
        .unwrap();
        let collected =
            py_collect_queue_fields(py, occurrences.bind(py), None).unwrap();
        let collected = py_to_json_value(collected.bind(py)).unwrap();
        assert_eq!(collected["fields"]["queue_capacity"], json!(5));
        assert_eq!(collected["fields"]["weight"], json!(0.25));
        assert!(collected["fields"]
            .get("queue_capacity_multiplier")
            .is_none());
        assert!(collected["errors"].as_array().unwrap().is_empty());
        let multiplier_occurrences = json_value_to_py(
            py,
            &json!([{
                "source": "%q(1.5x, w=0.25)",
                "source_span": [0, 16],
                "args": [{"value": "1.5x"}, {"name": "w", "value": "0.25"}],
                "has_plus_suffix": false
            }]),
        )
        .unwrap();
        let multiplier_collected =
            py_collect_queue_fields(py, multiplier_occurrences.bind(py), None)
                .unwrap();
        let multiplier_collected =
            py_to_json_value(multiplier_collected.bind(py)).unwrap();
        assert!(multiplier_collected["fields"]
            .get("queue_capacity")
            .is_none());
        assert_eq!(
            multiplier_collected["fields"]["queue_capacity_multiplier"],
            json!(1.5)
        );
        let multiplier_formatted = py_format_queue_directive(
            json_value_to_py(
                py,
                &json!({"queue_capacity_multiplier": 1.5, "weight": 0.25}),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(
            multiplier_formatted.as_deref(),
            Some("%queue(capacity=1.5x, weight=0.25)")
        );
        let parsed_value =
            py_parse_queue_capacity_value(py, "1.5x", None).unwrap();
        let parsed_value = py_to_json_value(parsed_value.bind(py)).unwrap();
        assert!(parsed_value.get("queue_capacity").is_none());
        assert_eq!(parsed_value["queue_capacity_multiplier"], json!(1.5));
        assert_eq!(
            py_format_queue_capacity_multiplier(1.5).as_deref(),
            Some("1.5x")
        );
        assert_eq!(py_resolve_queue_capacity_multiplier(1.5, 5.0), Some(7.5));
        let formatted = py_format_queue_directive(
            json_value_to_py(
                py,
                &json!({"capacity": 5, "priority": 20, "weight": 2.0}),
            )
            .unwrap()
            .bind(py),
        )
        .unwrap();
        assert_eq!(
            formatted.as_deref(),
            Some("%queue(capacity=5, priority=20, weight=2)")
        );
        let zero_formatted = py_format_queue_directive(
            json_value_to_py(py, &json!({"weight": 0.0}))
                .unwrap()
                .bind(py),
        )
        .unwrap();
        assert_eq!(zero_formatted.as_deref(), Some("%queue(weight=0)"));
        assert_eq!(py_parse_queue_capacity("0", None).unwrap(), 0);
        assert_eq!(py_parse_queue_capacity("3", None).unwrap(), 3);
        assert!(py_parse_queue_capacity(
            "0",
            Some(vec!["queue_capacity_budget".to_string()])
        )
        .is_err());
        assert!(py_parse_queue_capacity("true", None).is_err());
        assert_eq!(py_runner_capacity_policy_schema_version(), 6);
        let on_contract = py_directive_contract(
            py,
            Some(vec!["queue_capacity_budget".to_string()]),
        )
        .unwrap();
        let on_contract = py_to_json_value(on_contract.bind(py)).unwrap();
        let on_queue = on_contract
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == "queue")
            .unwrap();
        assert_eq!(on_queue["positional_role"], json!("positive_int"));
        let suggestions = on_queue["positional_suggestions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value["value"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(suggestions, ["1", "100", "1.5x"]);
        assert!(!suggestions.contains(&"0"));
        let capacity_kw = on_queue["keywords"]
            .as_array()
            .unwrap()
            .iter()
            .find(|keyword| keyword["name"] == "capacity")
            .unwrap();
        assert_eq!(capacity_kw["value_role"], json!("positive_int"));
        let normalized = py_normalize_persisted_queue_capacity(
            py,
            Some(0),
            true,
            0.25,
            8.0,
            true,
            None,
        )
        .unwrap();
        let normalized = py_to_json_value(normalized.bind(py)).unwrap();
        assert_eq!(normalized["admission_limit"], json!(0.25));
        assert_eq!(normalized["legacy_zero"], json!(true));
        assert!(normalized.get("reauthor_capacity").is_none());
        let multiplier_normalized = py_normalize_persisted_queue_capacity(
            py,
            None,
            false,
            0.25,
            5.0,
            true,
            Some(1.5),
        )
        .unwrap();
        let multiplier_normalized =
            py_to_json_value(multiplier_normalized.bind(py)).unwrap();
        assert_eq!(multiplier_normalized["admission_limit"], json!(7.5));
        assert_eq!(multiplier_normalized["reauthor_multiplier"], json!(1.5));
        let capacity_request = json_value_to_py(
            py,
            &json!({
                "effective_limit": 1.0,
                "records": [
                    {
                        "artifact_dir": "/tmp/running",
                        "project_name": "proj",
                        "timestamp": "20260910000000",
                        "run_started_at": "2026-09-10T00:00:00Z",
                        "queue_weight": 0.75
                    },
                    {
                        "artifact_dir": "/tmp/waiting",
                        "project_name": "proj",
                        "timestamp": "20260910000001",
                        "slot_requested_at": "2026-09-10T00:00:01Z",
                        "queue_weight": 0.25
                    }
                ]
            }),
        )
        .unwrap();
        let capacity =
            py_runner_capacity_snapshot(py, capacity_request.bind(py)).unwrap();
        let capacity = py_to_json_value(capacity.bind(py)).unwrap();
        assert_eq!(capacity["schema_version"], json!(6));
        assert_eq!(capacity["occupied_capacity"], json!(0.75));
        assert_eq!(
            capacity["first_eligible_artifact_dir"],
            json!("/tmp/waiting")
        );
        let capacity_request_with_candidate = json_value_to_py(
            py,
            &json!({
                "effective_limit": 1.0,
                "records": [
                    {
                        "artifact_dir": "/tmp/root",
                        "project_name": "proj",
                        "timestamp": "root",
                        "agent_session": "fam",
                        "run_started_at": "2026-09-10T00:00:00Z",
                        "queue_weight": 2.0
                    }
                ],
                "candidate": {
                    "artifact_dir": "/tmp/successor",
                    "project_name": "proj",
                    "timestamp": "successor",
                    "parent_timestamp": "root",
                    "agent_session": "fam",
                    "slot_requested_at": "2026-09-10T00:00:01Z"
                }
            }),
        )
        .unwrap();
        let capacity = py_runner_capacity_snapshot(
            py,
            capacity_request_with_candidate.bind(py),
        )
        .unwrap();
        let capacity = py_to_json_value(capacity.bind(py)).unwrap();
        assert_eq!(
            capacity["candidate_decision"]["decision"],
            json!("reuse_existing_claim")
        );
        assert_eq!(
            capacity["candidate_decision"]["effective_weight"],
            json!(2.0)
        );
        assert_eq!(
            capacity["candidate_decision"]["owner_key"],
            json!("proj:fam")
        );
        assert_eq!(capacity["candidate_decision"]["lineage_key"], json!("fam"));
        assert_eq!(
            capacity["candidate_decision"]["explicit_weight_compatibility"],
            json!("inherited-active-claim")
        );
        let held_capacity_request = json_value_to_py(
            py,
            &json!({
                "effective_limit": 1.0,
                "holds": [{
                    "schema_version": 2,
                    "armer": {
                        "kind": "agent",
                        "key": "agent:hold",
                        "display": "Hold Agent",
                        "project": "proj",
                        "agent_name": "holder.agent--code",
                        "agent_session": "holder.agent",
                        "pid": 123
                    },
                    "scope": {"kind": "project", "project": "proj"},
                    "selectors": {"names": ["target.agent--code"]},
                    "created_at": 1788998400.0,
                    "expires_at": 1788998580.0
                }],
                "records": [{
                    "artifact_dir": "/tmp/held",
                    "project_name": "proj",
                    "timestamp": "held",
                    "agent_name": "target.agent--code",
                    "created_at": 1788998430.0,
                    "slot_requested_at": "2026-09-10T00:00:30Z"
                }],
                "now": "2026-09-10T00:01:00Z"
            }),
        )
        .unwrap();
        let held_capacity =
            py_runner_capacity_snapshot(py, held_capacity_request.bind(py))
                .unwrap();
        let held_capacity = py_to_json_value(held_capacity.bind(py)).unwrap();
        assert_eq!(
            held_capacity["waiters"][0]["blockers"][0]["code"],
            json!("hold-barrier")
        );
        assert_eq!(
            held_capacity["waiters"][0]["blockers"][0]["held_by"],
            json!("agent:hold")
        );

        let wait = contract
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == "wait")
            .unwrap();
        assert_eq!(
            wait["keywords"]
                .as_array()
                .unwrap()
                .iter()
                .map(|keyword| keyword["name"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["agent", "bead", "hood", "proc", "time", "unit"]
        );

        let context =
            py_directive_completion_context(py, "%wait(bead=", 0, 11).unwrap();
        let context_value = py_to_json_value(context.bind(py)).unwrap();
        assert_eq!(context_value["kind"], json!("directive_argument_value"));
        assert_eq!(context_value["directive_name"], json!("wait"));
        assert_eq!(context_value["directive"]["active_keyword"], json!("bead"));
        assert_eq!(context_value["directive"]["value_role"], json!("bead"));

        let context_dict = context.bind(py).downcast::<PyDict>().unwrap();
        let inventories = json_value_to_py(
            py,
            &json!({
                "beads": [{
                    "id": "sase-a",
                    "title": "Active bug",
                    "status": "in_progress",
                    "updated_at": "2026-08-20T12:00:00Z"
                }],
                "agents": [{"name": "worker", "kind": "agent"}]
            }),
        )
        .unwrap();
        let inventories_dict =
            inventories.bind(py).downcast::<PyDict>().unwrap();
        let candidates = py_directive_completion_candidates(
            py,
            context_dict.clone(),
            Some(inventories_dict.clone()),
        )
        .unwrap();
        let candidates = py_to_json_value(candidates.bind(py)).unwrap();
        assert_eq!(candidates["candidates"][0]["insertion"], json!("sase-a"));

        let wait_context =
            py_directive_completion_context(py, "%wait(", 0, 6).unwrap();
        let wait_dict = wait_context.bind(py).downcast::<PyDict>().unwrap();
        let wait_candidates = py_directive_completion_candidates(
            py,
            wait_dict.clone(),
            Some(inventories_dict.clone()),
        )
        .unwrap();
        let wait_candidates =
            py_to_json_value(wait_candidates.bind(py)).unwrap();
        let insertions: Vec<&str> = wait_candidates["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .map(|candidate| candidate["insertion"].as_str().unwrap())
            .collect();
        assert_eq!(
            insertions,
            ["agent=", "bead=", "hood=", "proc=", "time=", "unit=", "worker"]
        );

        let colon =
            py_directive_completion_context(py, "%wait:t", 0, 7).unwrap();
        let colon_value = py_to_json_value(colon.bind(py)).unwrap();
        assert_eq!(colon_value["directive"]["syntax_form"], json!("colon"));
        let colon_dict = colon.bind(py).downcast::<PyDict>().unwrap();
        let colon_candidates = py_directive_completion_candidates(
            py,
            colon_dict.clone(),
            Some(inventories_dict.clone()),
        )
        .unwrap();
        let colon_candidates =
            py_to_json_value(colon_candidates.bind(py)).unwrap();
        assert!(colon_candidates["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .all(|candidate| {
                !candidate["insertion"].as_str().unwrap().ends_with('=')
            }));
    });
}

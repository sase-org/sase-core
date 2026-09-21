use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use serde_json::json;
use std::fs;

#[test]
fn compose_snippet_catalog_binding_returns_plain_dict_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_compose_snippet_catalog, &module).unwrap(),
            )
            .unwrap();
        let templates = PyDict::new_bound(py);
        templates.set_item("foo", "foo #[helper] $1$0").unwrap();
        templates.set_item("helper", "helper $1$0").unwrap();

        let result = module
            .getattr("compose_snippet_catalog")
            .unwrap()
            .call1((templates,))
            .unwrap();
        let value = py_to_json_value(&result).unwrap();

        assert_eq!(
            value["templates"],
            json!({
                "Foo": "Foo helper $1 $2$0",
                "Helper": "Helper $1$0",
                "foo": "foo helper $1 $2$0",
                "helper": "helper $1$0"
            })
        );
        assert_eq!(
            value["alias_provenance"],
            json!({
                "Foo": "foo",
                "Helper": "helper"
            })
        );
        assert_eq!(
            value["triggers"],
            json!({
                "foo": {
                    "trigger": "foo",
                    "valid": true,
                    "reason": null
                },
                "helper": {
                    "trigger": "helper",
                    "valid": true,
                    "reason": null
                }
            })
        );
        assert_eq!(
            value["calls"],
            json!({
                "foo": [{
                    "authored_target": "helper",
                    "canonical_target": "helper",
                    "positional_args": [],
                    "span": {"start": 4, "end": 13},
                    "status": "resolved"
                }],
                "helper": []
            })
        );
        assert_eq!(
            value["outbound"],
            json!({
                "foo": ["helper"],
                "helper": []
            })
        );
        assert_eq!(
            value["inbound"],
            json!({
                "foo": [],
                "helper": ["foo"]
            })
        );
        assert_eq!(value["diagnostics"], json!([]));
    });
}

#[test]
fn validate_snippet_trigger_binding_returns_plain_dict_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_validate_snippet_trigger, &module).unwrap(),
            )
            .unwrap();

        let valid = module
            .getattr("validate_snippet_trigger")
            .unwrap()
            .call1(("fix_it2",))
            .unwrap();
        assert_eq!(
            py_to_json_value(&valid).unwrap(),
            json!({
                "trigger": "fix_it2",
                "valid": true,
                "reason": null
            })
        );

        let invalid = module
            .getattr("validate_snippet_trigger")
            .unwrap()
            .call1(("bad-name!",))
            .unwrap();
        assert_eq!(
            py_to_json_value(&invalid).unwrap(),
            json!({
                "trigger": "bad-name!",
                "valid": false,
                "reason": "invalid_characters"
            })
        );
    });
}

#[test]
fn load_editor_snippet_catalog_binding_returns_plain_dict_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let xprompts = root.join("sase/xprompts");
        fs::create_dir_all(&xprompts).unwrap();
        fs::write(
            xprompts.join("fix.md"),
            "---\nsnippet: fixit\ndescription: Fix a bug\n---\nFix it",
        )
        .unwrap();
        fs::write(
            root.join("sase/sase.yml"),
            "ace:\n  snippets:\n    todo: TODO $1$0\n",
        )
        .unwrap();

        let result = py_load_editor_snippet_catalog(
            py,
            None,
            Some(root.to_string_lossy().to_string()),
        )
        .unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        let triggers = value["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| entry["trigger"].as_str().unwrap())
            .collect::<Vec<_>>();

        assert!(triggers.contains(&"fixit"));
        assert!(triggers.contains(&"Fixit"));
        assert!(triggers.contains(&"todo"));
        assert_eq!(value["result"]["status"], json!("success"));
    });
}

#[test]
fn filter_model_completion_entries_binding_returns_plain_dict_shape() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_filter_model_completion_entries, &module)
                    .unwrap(),
            )
            .unwrap();
        let entries = json_value_to_py(
            py,
            &json!([
                model_completion_entry_json(
                    "claude-fable-5",
                    "model",
                    "claude",
                    ["fable"],
                    0,
                ),
                model_completion_entry_json("opus", "model", "claude", [], 0,),
                model_completion_entry_json(
                    "@scout",
                    "user_alias",
                    "",
                    ["scout"],
                    0,
                ),
                model_completion_entry_json(
                    "claude/",
                    "provider",
                    "claude",
                    [],
                    2,
                ),
            ]),
        )
        .unwrap();

        let scoped = module
            .getattr("filter_model_completion_entries")
            .unwrap()
            .call1((entries, "claude/fa"))
            .unwrap();
        let value = py_to_json_value(&scoped).unwrap();

        assert_eq!(
            value,
            json!([model_completion_entry_json(
                "claude/claude-fable-5",
                "model",
                "claude",
                ["fable"],
                0,
            )])
        );
        assert_eq!(value[0]["display"], json!("claude/claude-fable-5"));
    });
}

#[test]
fn filter_model_completion_entries_binding_rejects_malformed_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let rows = json_value_to_py(py, &json!([{"value": "model"}]))
            .unwrap()
            .into_bound(py);
        let rows = rows.downcast::<PyList>().unwrap();
        let error = py_filter_model_completion_entries(py, rows, "")
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing field"), "unexpected error: {error}");
    });
}

#[test]
fn model_shortcut_bindings_return_plain_dict_list_and_none_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_model_shortcut_context, &module).unwrap(),
            )
            .unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_model_shortcut_edit, &module).unwrap(),
            )
            .unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_filter_explicit_model_shortcut_entries,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let entries = json_value_to_py(
            py,
            &json!([
                model_completion_entry_json(
                    "gpt-5.6-sol",
                    "model",
                    "codex",
                    ["gpt56sol"],
                    0,
                ),
                model_completion_entry_json(
                    "@large",
                    "user_alias",
                    "",
                    ["large"],
                    0,
                ),
                model_completion_entry_json(
                    "codex/",
                    "provider",
                    "codex",
                    [],
                    1,
                ),
            ]),
        )
        .unwrap();
        let position =
            json_value_to_py(py, &json!({"line": 0, "character": 7})).unwrap();

        let context = module
            .getattr("model_shortcut_context")
            .unwrap()
            .call1(("🙂 ==gp", position.clone_ref(py)))
            .unwrap();
        assert_eq!(
            py_to_json_value(&context).unwrap(),
            json!({
                "schema_version": 1,
                "kind": "model",
                "query": "gp",
                "token": "==gp",
                "caret": {"line": 0, "character": 7},
                "token_range": {
                    "start": {"line": 0, "character": 3},
                    "end": {"line": 0, "character": 7}
                },
                "replacement_range": {
                    "start": {"line": 0, "character": 3},
                    "end": {"line": 0, "character": 7}
                }
            })
        );

        let legacy_star_context = module
            .getattr("model_shortcut_context")
            .unwrap()
            .call1(("🙂 **gp", position.clone_ref(py)))
            .unwrap();
        assert!(legacy_star_context.is_none());

        let filtered = module
            .getattr("filter_explicit_model_shortcut_entries")
            .unwrap()
            .call1((entries.clone_ref(py), "codex/gp"))
            .unwrap();
        assert_eq!(
            py_to_json_value(&filtered).unwrap(),
            json!([model_completion_entry_json(
                "codex/gpt-5.6-sol",
                "model",
                "codex",
                ["gpt56sol"],
                0,
            )])
        );

        let scoped_position =
            json_value_to_py(py, &json!({"line": 0, "character": 13})).unwrap();
        let edit = module
            .getattr("model_shortcut_edit")
            .unwrap()
            .call1((
                "🙂 ==codex/gp",
                scoped_position.clone_ref(py),
                entries.clone_ref(py),
                "codex/gpt-5.6-sol",
            ))
            .unwrap();
        assert_eq!(
            py_to_json_value(&edit).unwrap(),
            json!({
                "schema_version": 1,
                "kind": "model",
                "value": "codex/gpt-5.6-sol",
                "replacement": "%m:codex/gpt-5.6-sol ",
                "edit": {
                    "range": {
                        "start": {"line": 0, "character": 3},
                        "end": {"line": 0, "character": 13}
                    },
                    "new_text": "%m:codex/gpt-5.6-sol "
                },
                "caret": {"line": 0, "character": 24}
            })
        );

        let stale = module
            .getattr("model_shortcut_edit")
            .unwrap()
            .call1(("🙂 ==codex/gp", scoped_position, entries, "gpt-5.6-sol"))
            .unwrap();
        assert!(stale.is_none());

        let unsafe_entries =
            json_value_to_py(
                py,
                &json!([
                    model_completion_entry_json(
                        "gpt\u{0000}bad",
                        "model",
                        "codex",
                        [],
                        0,
                    ),
                    model_completion_entry_json(
                        "gpt bad",
                        "model",
                        "codex",
                        [],
                        0,
                    ),
                ]),
            )
            .unwrap();
        let unsafe_position =
            json_value_to_py(py, &json!({"line": 0, "character": 9})).unwrap();
        for unsafe_value in ["gpt\u{0000}bad", "gpt bad"] {
            let unsafe_edit = module
                .getattr("model_shortcut_edit")
                .unwrap()
                .call1((
                    "Use ==gpt",
                    unsafe_position.clone_ref(py),
                    unsafe_entries.clone_ref(py),
                    unsafe_value,
                ))
                .unwrap();
            assert!(
                unsafe_edit.is_none(),
                "unsafe selected value {unsafe_value:?}"
            );
        }
    });
}

#[test]
fn model_shortcut_binding_rejects_malformed_inputs() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let position_error =
            py_model_shortcut_context(py, "==", PyDict::new_bound(py).as_any())
                .unwrap_err()
                .to_string();
        assert!(
            position_error.contains("UTF-16 line/character"),
            "unexpected error: {position_error}"
        );

        let rows = json_value_to_py(py, &json!([{"value": "gpt-5"}]))
            .unwrap()
            .into_bound(py);
        let rows = rows.downcast::<PyList>().unwrap();
        let catalog_error =
            py_filter_explicit_model_shortcut_entries(py, rows, "")
                .unwrap_err()
                .to_string();
        assert!(
            catalog_error.contains("missing field"),
            "unexpected error: {catalog_error}"
        );
    });
}

#[test]
fn argument_colon_to_parentheses_binding_returns_plain_edit_or_none() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_argument_colon_to_parentheses_edit,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let position =
            json_value_to_py(py, &json!({"line": 0, "character": 3})).unwrap();
        let edit = module
            .getattr("argument_colon_to_parentheses_edit")
            .unwrap()
            .call1(("%q:", position.clone_ref(py)))
            .unwrap();
        assert_eq!(
            py_to_json_value(&edit).unwrap(),
            json!({
                "range": {
                    "start": {"line": 0, "character": 2},
                    "end": {"line": 0, "character": 3}
                },
                "new_text": ""
            })
        );

        let ordinary = module
            .getattr("argument_colon_to_parentheses_edit")
            .unwrap()
            .call1(("Note:", position.clone_ref(py)))
            .unwrap();
        assert!(ordinary.is_none());

        let malformed_position =
            json_value_to_py(py, &json!({"line": "0", "character": 3}))
                .unwrap();
        let error = module
            .getattr("argument_colon_to_parentheses_edit")
            .unwrap()
            .call1(("%q:", malformed_position))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("position is not a valid EditorPosition"),
            "unexpected error: {error}"
        );
    });
}

#[test]
fn argument_double_colon_to_parentheses_binding_returns_plain_edit_or_none() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_argument_double_colon_to_parentheses_edit,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let position =
            json_value_to_py(py, &json!({"line": 0, "character": 8})).unwrap();
        let edit = module
            .getattr("argument_double_colon_to_parentheses_edit")
            .unwrap()
            .call1(("#foo::  ", position.clone_ref(py)))
            .unwrap();
        assert_eq!(
            py_to_json_value(&edit).unwrap(),
            json!({
                "range": {
                    "start": {"line": 0, "character": 4},
                    "end": {"line": 0, "character": 8}
                },
                "new_text": "()::  "
            })
        );

        let ordinary = module
            .getattr("argument_double_colon_to_parentheses_edit")
            .unwrap()
            .call1(("%q::  ", position.clone_ref(py)))
            .unwrap();
        assert!(ordinary.is_none());

        let utf16_position =
            json_value_to_py(py, &json!({"line": 1, "character": 12})).unwrap();
        let utf16_edit = module
            .getattr("argument_double_colon_to_parentheses_edit")
            .unwrap()
            .call1(("é🙂\nText #foo:: ", utf16_position))
            .unwrap();
        assert_eq!(
            py_to_json_value(&utf16_edit).unwrap()["range"],
            json!({
                "start": {"line": 1, "character": 9},
                "end": {"line": 1, "character": 12}
            })
        );

        let malformed_position =
            json_value_to_py(py, &json!({"line": "0", "character": 8}))
                .unwrap();
        let error = module
            .getattr("argument_double_colon_to_parentheses_edit")
            .unwrap()
            .call1(("#foo::  ", malformed_position))
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("position is not a valid EditorPosition"),
            "unexpected error: {error}"
        );
    });
}

#[test]
fn xprompt_argument_spans_binding_returns_open_structural_spans() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_xprompt_argument_spans, &module).unwrap(),
            )
            .unwrap();

        let source = "#foo(key=42, other=true";
        let result = module
            .getattr("xprompt_argument_spans")
            .unwrap()
            .call1((source,))
            .unwrap();
        let value = py_to_json_value(&result).unwrap();
        let spans = value.as_array().unwrap();
        let has = |role: &str, raw: &str| {
            spans.iter().any(|span| {
                let start = span["start"].as_u64().unwrap() as usize;
                let end = span["end"].as_u64().unwrap() as usize;
                span["role"] == json!(role) && &source[start..end] == raw
            })
        };

        assert!(has("arg_delimiter", "("), "{spans:?}");
        assert!(has("arg_delimiter", ","), "{spans:?}");
        assert!(has("arg_key", "key"), "{spans:?}");
        assert!(has("arg_value_number", "42"), "{spans:?}");
        assert!(has("arg_key", "other"), "{spans:?}");
        assert!(has("arg_value_bool", "true"), "{spans:?}");
    });
}

#[test]
fn model_alias_shortcut_bindings_return_plain_dict_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_model_alias_shortcut_context, &module)
                    .unwrap(),
            )
            .unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_model_alias_shortcut_edit, &module)
                    .unwrap(),
            )
            .unwrap();
        let position =
            json_value_to_py(py, &json!({"line": 0, "character": 6})).unwrap();
        let entries = json_value_to_py(
            py,
            &json!([
                model_completion_entry_json("@large", "user_alias", "", [], 0,),
                model_completion_entry_json(
                    "@small",
                    "implicit_alias",
                    "",
                    [],
                    0,
                ),
                model_completion_entry_json(
                    "large-model",
                    "model",
                    "openai",
                    [],
                    0,
                ),
            ]),
        )
        .unwrap();

        let context = module
            .getattr("model_alias_shortcut_context")
            .unwrap()
            .call1(("🙂 =la", position.clone_ref(py)))
            .unwrap();
        assert_eq!(
            py_to_json_value(&context).unwrap(),
            json!({
                "schema_version": 1,
                "query": "la",
                "token": "=la",
                "caret": {"line": 0, "character": 6},
                "token_range": {
                    "start": {"line": 0, "character": 3},
                    "end": {"line": 0, "character": 6}
                },
                "replacement_range": {
                    "start": {"line": 0, "character": 3},
                    "end": {"line": 0, "character": 6}
                }
            })
        );

        let legacy_star_context = module
            .getattr("model_alias_shortcut_context")
            .unwrap()
            .call1(("🙂 *la", position.clone_ref(py)))
            .unwrap();
        assert!(legacy_star_context.is_none());

        let edit = module
            .getattr("model_alias_shortcut_edit")
            .unwrap()
            .call1((
                "🙂 =la",
                position.clone_ref(py),
                entries.clone_ref(py),
                "@large",
            ))
            .unwrap();
        assert_eq!(
            py_to_json_value(&edit).unwrap(),
            json!({
                "schema_version": 1,
                "alias": "@large",
                "replacement": "%m:@large ",
                "edit": {
                    "range": {
                        "start": {"line": 0, "character": 3},
                        "end": {"line": 0, "character": 6}
                    },
                    "new_text": "%m:@large "
                },
                "caret": {"line": 0, "character": 13}
            })
        );

        let stale = module
            .getattr("model_alias_shortcut_edit")
            .unwrap()
            .call1(("🙂 =la", position, entries, "@small"))
            .unwrap();
        assert!(stale.is_none());
    });
}

#[test]
fn model_alias_shortcut_binding_rejects_malformed_position() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let error = py_model_alias_shortcut_context(
            py,
            "=",
            PyDict::new_bound(py).as_any(),
        )
        .unwrap_err()
        .to_string();
        assert!(
            error.contains("UTF-16 line/character"),
            "unexpected error: {error}"
        );
    });
}

#[test]
fn filter_model_alias_shortcut_entries_binding_restricts_to_alias_kinds() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(
                    py_filter_model_alias_shortcut_entries,
                    &module
                )
                .unwrap(),
            )
            .unwrap();
        let entries = json_value_to_py(
            py,
            &json!([
                model_completion_entry_json("@large", "user_alias", "", [], 0,),
                model_completion_entry_json(
                    "@launch",
                    "implicit_alias",
                    "",
                    [],
                    0,
                ),
                model_completion_entry_json(
                    "large-model",
                    "model",
                    "openai",
                    [],
                    0,
                ),
            ]),
        )
        .unwrap();

        let filtered = module
            .getattr("filter_model_alias_shortcut_entries")
            .unwrap()
            .call1((entries, "la"))
            .unwrap();

        assert_eq!(
            py_to_json_value(&filtered).unwrap(),
            json!([
                model_completion_entry_json("@large", "user_alias", "", [], 0,),
                model_completion_entry_json(
                    "@launch",
                    "implicit_alias",
                    "",
                    [],
                    0,
                ),
            ])
        );
    });
}

#[test]
fn filter_model_alias_shortcut_entries_binding_rejects_malformed_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let rows = json_value_to_py(py, &json!([{"value": "@large"}]))
            .unwrap()
            .into_bound(py);
        let rows = rows.downcast::<PyList>().unwrap();
        let error = py_filter_model_alias_shortcut_entries(py, rows, "")
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing field"), "unexpected error: {error}");
    });
}

#[test]
fn compose_snippet_catalog_binding_exposes_missing_and_cycle_graph() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_compose_snippet_catalog, &module).unwrap(),
            )
            .unwrap();
        let templates = PyDict::new_bound(py);
        templates.set_item("selfish", "#[selfish]$0").unwrap();
        templates.set_item("outer", "#[missing]$0").unwrap();
        templates.set_item("via_alias", "#[Selfish]$0").unwrap();

        let result = module
            .getattr("compose_snippet_catalog")
            .unwrap()
            .call1((templates,))
            .unwrap();
        let value = py_to_json_value(&result).unwrap();

        assert_eq!(value["calls"]["selfish"][0]["status"], json!("cycle"));
        assert_eq!(
            value["calls"]["selfish"][0]["canonical_target"],
            json!("selfish")
        );
        assert_eq!(value["calls"]["outer"][0]["status"], json!("missing"));
        assert_eq!(
            value["calls"]["via_alias"][0]["authored_target"],
            json!("Selfish")
        );
        assert_eq!(
            value["calls"]["via_alias"][0]["canonical_target"],
            json!("selfish")
        );
        assert_eq!(value["calls"]["via_alias"][0]["status"], json!("resolved"));
        assert_eq!(
            value["inbound"]["selfish"],
            json!(["selfish", "via_alias"])
        );
        let codes: Vec<&str> = value["diagnostics"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| item["code"].as_str().unwrap())
            .collect();
        assert_eq!(codes, vec!["missing_target", "direct_cycle"]);
    });
}

fn model_completion_entry_json<const N: usize>(
    value: &str,
    kind: &str,
    provider: &str,
    aliases: [&str; N],
    provider_model_count: u64,
) -> JsonValue {
    json!({
        "value": value,
        "display": value,
        "description": "",
        "kind": kind,
        "provider": provider,
        "provider_display": "",
        "aliases": aliases.to_vec(),
        "alias_kind": "",
        "target_provider": "",
        "target_model": "",
        "target_effort": "",
        "provenance": "",
        "reference": "",
        "reference_effort": "",
        "selector_mode": "",
        "pool_available": 0,
        "pool_total": 0,
        "config_source": "",
        "bucket": "",
        "advisory_label": "",
        "advisory_severity": "",
        "provider_model_count": provider_model_count
    })
}

#[test]
fn apply_snippet_session_event_binding_drives_nesting_through_dicts() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_apply_snippet_session_event, &module)
                    .unwrap(),
            )
            .unwrap();
        let apply_event = |state: &PyObject, event: JsonValue| {
            let state_dict = json_value_to_py(
                py,
                &py_to_json_value(state.bind(py)).unwrap(),
            )
            .unwrap();
            let event_dict = json_value_to_py(py, &event).unwrap();
            let result = module
                .getattr("apply_snippet_session_event")
                .unwrap()
                .call1((state_dict, event_dict))
                .unwrap();
            py_to_json_value(&result).unwrap()
        };

        let empty_state: PyObject = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "stops": [],
                "index": 0,
                "sessions": [],
                "next_session_id": 0
            }),
        )
        .unwrap();

        let planned = apply_event(
            &empty_state,
            json!({
                "kind": "plan",
                "template": "foo $1 bar $2 baz $3 buz",
                "line_indent": "",
                "indent_continuation_lines": true
            }),
        );
        assert_eq!(planned["text"], json!("foo  bar  baz  buz"));
        assert_eq!(planned["tabstop_offsets"], json!([4, 9, 14, 18]));
        assert_eq!(
            planned["state"],
            json!({
                "schema_version": 1,
                "stops": [],
                "index": 0,
                "sessions": [],
                "next_session_id": 0
            }),
            "planning is stateless and must echo the input state back"
        );
        assert_eq!(planned["cursor_offset"], JsonValue::Null);

        let expanded_state: PyObject =
            json_value_to_py(py, &planned["state"]).unwrap();
        let expanded = apply_event(
            &expanded_state,
            json!({
                "kind": "expand",
                "range_start": 0,
                "range_end": 100,
                "tabstop_offsets": planned["tabstop_offsets"]
            }),
        );
        let after_expand: PyObject =
            json_value_to_py(py, &expanded["state"]).unwrap();

        let advanced = apply_event(&after_expand, json!({"kind": "advance"}));
        assert_eq!(advanced["cursor_offset"], json!(9));
        let after_advance: PyObject =
            json_value_to_py(py, &advanced["state"]).unwrap();

        let inner_planned = apply_event(
            &after_advance,
            json!({
                "kind": "plan",
                "template": "inner $1 done",
                "line_indent": "",
                "indent_continuation_lines": true
            }),
        );

        let nested = apply_event(
            &after_advance,
            json!({
                "kind": "expand",
                "range_start": 8,
                "range_end": 19,
                "tabstop_offsets": inner_planned["tabstop_offsets"]
            }),
        );
        let after_nest: PyObject =
            json_value_to_py(py, &nested["state"]).unwrap();

        let advanced_inner =
            apply_event(&after_nest, json!({"kind": "advance"}));
        assert_eq!(
            advanced_inner["cursor_offset"],
            json!(19),
            "inner's own last stop"
        );
        let after_inner_advance: PyObject =
            json_value_to_py(py, &advanced_inner["state"]).unwrap();

        let resumed =
            apply_event(&after_inner_advance, json!({"kind": "advance"}));
        assert_eq!(
            resumed["cursor_offset"],
            json!(14),
            "resumes outer's $3 after inner exhausts"
        );
    });
}

#[test]
fn apply_snippet_session_event_binding_rejects_malformed_input() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        module
            .add_function(
                wrap_pyfunction!(py_apply_snippet_session_event, &module)
                    .unwrap(),
            )
            .unwrap();

        let valid_state = json_value_to_py(
            py,
            &json!({
                "schema_version": 1,
                "stops": [],
                "index": 0,
                "sessions": [],
                "next_session_id": 0
            }),
        )
        .unwrap();
        let malformed_state =
            json_value_to_py(py, &json!({"stops": []})).unwrap();
        let advance_event =
            json_value_to_py(py, &json!({"kind": "advance"})).unwrap();
        let unknown_kind_event =
            json_value_to_py(py, &json!({"kind": "teleport"})).unwrap();

        let call = |state: &PyObject, event: &PyObject| {
            module
                .getattr("apply_snippet_session_event")
                .unwrap()
                .call1((state, event))
        };

        assert!(
            call(&malformed_state, &advance_event).is_err(),
            "missing required state fields must be rejected"
        );
        assert!(
            call(&valid_state, &unknown_kind_event).is_err(),
            "an unrecognized event kind must be rejected"
        );
    });
}

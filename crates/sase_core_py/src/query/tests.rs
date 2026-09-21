use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use crate::test_support::append_json;
use serde_json::json;

fn spec_json(name: &str, status: &str, parent: Option<&str>) -> JsonValue {
    json!({
        "schema_version": 3,
        "name": name,
        "project_basename": "proj",
        "file_path": "proj.sase",
        "source_span": {
            "file_path": "proj.sase",
            "start_line": 1,
            "end_line": 10
        },
        "status": status,
        "parent": parent,
        "pr_url": null,
        "bug": null,
        "description": format!("description for {name}"),
        "commits": [],
        "hooks": [],
        "comments": [],
        "mentors": [],
        "timestamps": [],
        "deltas": []
    })
}

fn spec_list<'py>(py: Python<'py>, specs: &[JsonValue]) -> Bound<'py, PyList> {
    let list = PyList::empty_bound(py);
    for spec in specs {
        append_json(py, &list, spec.clone());
    }
    list
}

fn bools_from_py_list(list: &Bound<'_, PyList>) -> Vec<bool> {
    list.iter()
        .map(|item| item.extract::<bool>().unwrap())
        .collect()
}

#[test]
fn parse_patch_project_bytes_binding_emits_canonical_shape_and_query_accepts_it(
) {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();

        let src = "\
## Patch
NAME: alpha
STATUS: WIP
STITCHES:
  (2a) Proposed stitch
HOOKS:
  just test
      | (2a) [260101_120000] PASSED (3s)
MENTORS:
  (2a) profileA[1/1]
";
        let bytes = PyBytes::new_bound(py, src.as_bytes());
        let result = module
            .getattr("parse_patch_project_bytes")
            .unwrap()
            .call1(("proj.sase", bytes))
            .unwrap();
        let value = py_to_json_value(&result).unwrap();
        let patch = &value.as_array().unwrap()[0];

        assert!(patch.get("stitches").is_some());
        assert!(patch.get("commits").is_none());
        assert_eq!(patch["stitches"][0]["proposal_letter"], json!("a"));
        assert_eq!(
            patch["hooks"][0]["status_lines"][0]["stitch_id"],
            json!("2a")
        );
        assert!(patch["hooks"][0]["status_lines"][0]
            .get("commit_entry_num")
            .is_none());
        assert_eq!(patch["mentors"][0]["stitch_id"], json!("2a"));
        assert!(patch["mentors"][0].get("entry_id").is_none());

        let specs = PyList::empty_bound(py);
        append_json(py, &specs, patch.clone());
        let results = py_evaluate_query_many(py, "name:alpha", &specs).unwrap();
        assert_eq!(bools_from_py_list(&results), vec![true]);
    });
}

fn query_module<'py>(py: Python<'py>) -> Bound<'py, PyModule> {
    let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
    module.add_class::<PyQueryCorpusHandle>().unwrap();
    module.add_class::<PyQueryProgramHandle>().unwrap();
    module
        .add_function(wrap_pyfunction!(py_evaluate_many, &module).unwrap())
        .unwrap();
    module
}

#[test]
fn query_handles_evaluate_multiple_queries_against_one_corpus() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let specs = spec_list(
            py,
            &[
                spec_json("alpha", "WIP", None),
                spec_json("beta", "Submitted", Some("alpha")),
                spec_json("gamma", "WIP", Some("beta")),
            ],
        );
        let corpus = py_compile_corpus(py, &specs).unwrap();

        let alpha = py_compile_query("name:alpha").unwrap();
        let alpha_results = py_evaluate_many(py, &alpha, &corpus).unwrap();
        assert_eq!(
            bools_from_py_list(&alpha_results),
            vec![true, false, false]
        );

        let ancestor = py_compile_query("ancestor:alpha").unwrap();
        let ancestor_results =
            py_evaluate_many(py, &ancestor, &corpus).unwrap();
        assert_eq!(
            bools_from_py_list(&ancestor_results),
            vec![true, true, true]
        );
        assert_eq!(corpus.__len__(), 3);
    });
}

#[test]
fn query_handles_evaluate_one_query_against_multiple_corpora() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let program = py_compile_query("status:wip").unwrap();

        let first = spec_list(
            py,
            &[
                spec_json("alpha", "WIP", None),
                spec_json("beta", "Submitted", None),
            ],
        );
        let first_corpus = py_compile_corpus(py, &first).unwrap();
        let first_results =
            py_evaluate_many(py, &program, &first_corpus).unwrap();
        assert_eq!(bools_from_py_list(&first_results), vec![true, false]);

        let second = spec_list(
            py,
            &[
                spec_json("gamma", "Submitted", None),
                spec_json("delta", "WIP", None),
            ],
        );
        let second_corpus = py_compile_corpus(py, &second).unwrap();
        let second_results =
            py_evaluate_many(py, &program, &second_corpus).unwrap();
        assert_eq!(bools_from_py_list(&second_results), vec![false, true]);
    });
}

#[test]
fn query_handles_match_legacy_one_shot_results() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let specs = spec_list(
            py,
            &[
                spec_json("alpha", "WIP", None),
                spec_json("beta", "Submitted", Some("alpha")),
                spec_json("gamma", "WIP", Some("beta")),
            ],
        );
        let corpus = py_compile_corpus(py, &specs).unwrap();

        for query in ["alpha", "status:wip", "ancestor:alpha"] {
            let program = py_compile_query(query).unwrap();
            let handle_results =
                py_evaluate_many(py, &program, &corpus).unwrap();
            let legacy_results =
                py_evaluate_query_many(py, query, &specs).unwrap();
            assert_eq!(
                bools_from_py_list(&handle_results),
                bools_from_py_list(&legacy_results),
                "query {query}"
            );
        }
    });
}

#[test]
fn query_compile_errors_are_python_value_errors() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|_py| {
        let err = py_compile_query("").unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(_py));
        assert!(err.to_string().contains("Empty query"));
    });
}

#[test]
fn query_handle_bindings_reject_wrong_handle_types() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = query_module(py);
        let specs = spec_list(py, &[spec_json("alpha", "WIP", None)]);
        let corpus =
            Py::new(py, py_compile_corpus(py, &specs).unwrap()).unwrap();
        let program = Py::new(py, py_compile_query("alpha").unwrap()).unwrap();
        let bad = PyDict::new_bound(py);
        let evaluate_many = module.getattr("evaluate_many").unwrap();

        let err = evaluate_many
            .call1((bad.clone(), corpus.clone_ref(py)))
            .unwrap_err();
        assert!(err.to_string().contains("QueryProgramHandle"));

        let err = evaluate_many.call1((program, bad)).unwrap_err();
        assert!(err.to_string().contains("QueryCorpusHandle"));
    });
}

fn notes_profile_dict<'py>(py: Python<'py>) -> Bound<'py, PyDict> {
    let value = json!({
        "pane_id": "notes",
        "boolean": false,
        "fields": [
            {
                "key": "kind",
                "value_kind": "enum",
                "filterable": true,
                "searchable": false,
                "repeatable": true,
                "negatable": true,
                "static_values": ["note", "doc"],
                "hint": ""
            },
            {
                "key": "title",
                "value_kind": "string",
                "filterable": false,
                "searchable": true,
                "repeatable": false,
                "negatable": false,
                "static_values": [],
                "hint": ""
            }
        ],
        "sigils": [],
        "predicates": [],
        "any_special": false,
        "macros": [],
        "free_text_hint": "title"
    });
    json_value_to_py(py, &value)
        .unwrap()
        .bind(py)
        .downcast::<PyDict>()
        .unwrap()
        .clone()
}

#[test]
fn profile_binding_round_trips_compiled_profile_dict() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let profile = notes_profile_dict(py);
        let tokens = py_tokenize_query_with_profile(
            py,
            r#"kind:note "hello world""#,
            &profile,
        )
        .unwrap();
        assert_eq!(tokens.len(), 3);
        let canonical = py_canonicalize_query_with_profile(
            r#"kind:note "hello world""#,
            &profile,
        )
        .unwrap();
        assert_eq!(canonical, r#"kind:note "hello world""#);
    });
}

#[test]
fn profile_binding_evaluates_generic_rows() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let profile = notes_profile_dict(py);
        let rows = PyList::empty_bound(py);
        append_json(
            py,
            &rows,
            json!({
                "fields": {"kind": ["note"]},
                "searchable_text": "alpha hello",
                "predicates": {
                    "error_suffix": false,
                    "running_agent": false,
                    "running_process": false
                }
            }),
        );
        append_json(
            py,
            &rows,
            json!({
                "fields": {"kind": ["doc"]},
                "searchable_text": "beta world",
                "predicates": {}
            }),
        );
        let corpus =
            py_compile_corpus_with_profile(py, &profile, &rows).unwrap();
        let program =
            py_compile_query_with_profile("kind:note hello", &profile).unwrap();
        let results = py_evaluate_many(py, &program, &corpus).unwrap();
        assert_eq!(bools_from_py_list(&results), vec![true, false]);
    });
}

#[test]
fn profile_binding_rejects_invalid_profile_and_mismatch() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let bad = PyDict::new_bound(py);
        bad.set_item("pane_id", "").unwrap();
        bad.set_item("boolean", false).unwrap();
        let err = py_compile_query_with_profile("hello", &bad).unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        assert!(err.to_string().contains("pane_id"), "{err}");

        let profile = notes_profile_dict(py);
        let rows = PyList::empty_bound(py);
        append_json(py, &rows, json!({"fields": {}, "searchable_text": "x"}));
        let corpus =
            py_compile_corpus_with_profile(py, &profile, &rows).unwrap();
        let patch_program = py_compile_query("alpha").unwrap();
        let err = py_evaluate_many(py, &patch_program, &corpus).unwrap_err();
        assert!(err.to_string().contains("digest"), "{err}");
    });
}

#[test]
fn query_row_from_py_row_matches_json_wire_conversion_for_mixed_value_types() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let profile = profile_from_pydict(&notes_profile_dict(py)).unwrap();

        let row = PyDict::new_bound(py);
        let fields = PyDict::new_bound(py);
        fields.set_item("kind", "note").unwrap();
        fields.set_item("count", 3_i64).unwrap();
        fields.set_item("score", 1.5_f64).unwrap();
        fields.set_item("active", true).unwrap();
        fields.set_item("empty", "").unwrap();
        fields
            .set_item("tags", PyList::new_bound(py, ["alpha", "beta"]))
            .unwrap();
        row.set_item("fields", &fields).unwrap();
        row.set_item("searchable_text", "alpha hello").unwrap();
        let predicates = PyDict::new_bound(py);
        predicates.set_item("error_suffix", false).unwrap();
        predicates.set_item("running_agent", true).unwrap();
        predicates.set_item("running_process", false).unwrap();
        row.set_item("predicates", predicates).unwrap();

        let direct = query_row_from_py_row(row.as_any(), &profile).unwrap();

        let json_value = json!({
            "fields": {
                "kind": "note",
                "count": 3,
                "score": 1.5,
                "active": true,
                "empty": "",
                "tags": ["alpha", "beta"]
            },
            "searchable_text": "alpha hello",
            "predicates": {
                "error_suffix": false,
                "running_agent": true,
                "running_process": false
            }
        });
        let via_json = QueryRow::from_wire(&json_value, &profile).unwrap();

        assert_eq!(direct, via_json);
    });
}

#[test]
fn compile_corpus_with_profile_rejects_object_shaped_field_value_directly() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let profile = notes_profile_dict(py);
        let rows = PyList::empty_bound(py);
        let row = PyDict::new_bound(py);
        let fields = PyDict::new_bound(py);
        let nested = PyDict::new_bound(py);
        nested.set_item("bad", "shape").unwrap();
        fields.set_item("kind", nested).unwrap();
        row.set_item("fields", fields).unwrap();
        rows.append(row).unwrap();

        let err =
            py_compile_corpus_with_profile(py, &profile, &rows).unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));
        assert!(err.to_string().contains("rows[0]"), "{err}");
        assert!(err.to_string().contains("scalar or list"), "{err}");
    });
}

/// Manual perf check for the core-corpus phase of
/// `plan:202608/artifacts_query_performance.md`. Not run by `cargo
/// test`/`./scripts/check.sh test`; run explicitly with:
///
/// ```sh
/// cargo test -p sase_core_py --release -- --ignored --nocapture \
///     bench_compile_corpus_with_profile_over_agent_scale_corpus
/// ```
///
/// Baseline before this phase (measured on the Python side, 11,783
/// Agent-pane rows through the old `py_to_json_value` +
/// `QueryRow::from_wire` path): ~716ms. This synthetic corpus matches
/// that row/field order (~12k rows, ~20 fields) so the two numbers are
/// comparable.
#[test]
#[ignore = "manual perf check, not part of the default test run"]
fn bench_compile_corpus_with_profile_over_agent_scale_corpus() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        const ROW_COUNT: usize = 12_000;
        const FIELD_COUNT: usize = 20;

        let profile_fields = PyList::empty_bound(py);
        for i in 0..FIELD_COUNT {
            let field = PyDict::new_bound(py);
            field.set_item("key", format!("field_{i}")).unwrap();
            field.set_item("value_kind", "string").unwrap();
            field.set_item("filterable", true).unwrap();
            field.set_item("searchable", i % 4 == 0).unwrap();
            field.set_item("repeatable", false).unwrap();
            field.set_item("negatable", false).unwrap();
            field
                .set_item("static_values", PyList::empty_bound(py))
                .unwrap();
            field.set_item("hint", "").unwrap();
            profile_fields.append(field).unwrap();
        }
        let profile = PyDict::new_bound(py);
        profile.set_item("pane_id", "bench").unwrap();
        profile.set_item("boolean", false).unwrap();
        profile.set_item("fields", profile_fields).unwrap();
        profile.set_item("sigils", PyList::empty_bound(py)).unwrap();
        profile
            .set_item("predicates", PyList::empty_bound(py))
            .unwrap();
        profile.set_item("any_special", false).unwrap();
        profile.set_item("macros", PyList::empty_bound(py)).unwrap();
        profile.set_item("free_text_hint", "field_0").unwrap();

        let rows = PyList::empty_bound(py);
        for r in 0..ROW_COUNT {
            let row = PyDict::new_bound(py);
            let fields = PyDict::new_bound(py);
            for f in 0..FIELD_COUNT {
                fields
                    .set_item(format!("field_{f}"), format!("value_{r}_{f}"))
                    .unwrap();
            }
            row.set_item("fields", fields).unwrap();
            row.set_item(
                "searchable_text",
                format!("row {r} synthetic agent bench text"),
            )
            .unwrap();
            let predicates = PyDict::new_bound(py);
            predicates.set_item("error_suffix", false).unwrap();
            predicates.set_item("running_agent", r % 10 == 0).unwrap();
            predicates.set_item("running_process", false).unwrap();
            row.set_item("predicates", predicates).unwrap();
            rows.append(row).unwrap();
        }

        const RUNS: usize = 10;
        let mut samples_ms = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let start = Instant::now();
            let _ =
                py_compile_corpus_with_profile(py, &profile, &rows).unwrap();
            samples_ms.push(start.elapsed().as_secs_f64() * 1_000.0);
        }
        samples_ms.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let median = samples_ms[samples_ms.len() / 2];
        println!(
                "compile_corpus_with_profile: {ROW_COUNT} rows x {FIELD_COUNT} fields -> \
                 median {median:.2}ms over {RUNS} runs (min {:.2}ms, max {:.2}ms)",
                samples_ms.first().unwrap(),
                samples_ms.last().unwrap(),
            );
    });
}

#[test]
fn legacy_query_handles_still_use_patch_profile() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let specs = spec_list(py, &[spec_json("alpha", "WIP", None)]);
        let corpus = py_compile_corpus(py, &specs).unwrap();
        let program = py_compile_query("name:alpha").unwrap();
        let results = py_evaluate_many(py, &program, &corpus).unwrap();
        assert_eq!(bools_from_py_list(&results), vec![true]);
    });
}

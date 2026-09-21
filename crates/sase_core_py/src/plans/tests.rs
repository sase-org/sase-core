use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;
use serde_json::json;
use std::fs;

#[test]
fn plan_validation_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let content = "---\ntier: epic\ntitle: Binding parity\ngoal: The binding returns normalized data\nparent_bead: sase-7z.1\nbead: sase-88.1\nparent: sase/repos/plans/202607/parent.md\nphases:\n  - id: core\n    title: Core work\n    depends_on: []\n    description: Core work section exercises binding parity.\n    size: medium\n---\n# Plan\nImplement it.\n";
        let result =
            py_plan_validate(py, content, "epic", "authoring").unwrap();
        let value = py_to_json_value(result.bind(py)).unwrap();
        assert_eq!(value["schema_version"], json!(3));
        assert_eq!(value["ok"], json!(true));
        assert_eq!(
            value["diagnostics"][0]["code"],
            json!("parent-frontmatter-deprecated")
        );
        assert_eq!(value["plan"]["title"], json!("Binding parity"));
        assert_eq!(value["plan"]["parent_bead"], json!("sase-7z.1"));
        assert_eq!(value["plan"]["size"], json!(null));
        assert_eq!(value["plan"]["bead"], json!("sase-88.1"));
        assert_eq!(
            value["plan"]["parent"],
            json!("sase/repos/plans/202607/parent.md")
        );
        assert_eq!(value["plan"]["phases"][0]["depends_on"], json!([]));
        assert_eq!(value["plan"]["phases"][0]["size"], json!("medium"));

        let tale = "---\ntier: tale\ntitle: Tale binding parity\ngoal: The binding returns normalized data\nsize: medium\nbead: sase-88.1\nparent: sase/repos/plans/202607/parent.md\n---\n# Plan\nImplement it.\n";
        let tale_result =
            py_plan_validate(py, tale, "tale", "authoring").unwrap();
        let tale_value = py_to_json_value(tale_result.bind(py)).unwrap();
        assert_eq!(tale_value["ok"], json!(true));
        assert_eq!(
            tale_value["diagnostics"][0]["code"],
            json!("parent-frontmatter-deprecated")
        );
        assert_eq!(tale_value["plan"]["title"], json!("Tale binding parity"));
        assert_eq!(tale_value["plan"]["size"], json!("medium"));
        assert_eq!(tale_value["plan"]["bead"], json!("sase-88.1"));
        assert_eq!(
            tale_value["plan"]["parent"],
            json!("sase/repos/plans/202607/parent.md")
        );

        for (tier, extra) in [
                ("tale", "size: small\n"),
                (
                    "epic",
                    "phases:\n  - id: core\n    title: Core\n    depends_on: []\n    description: Core section exercises title validation.\n    size: small\n",
                ),
            ] {
                for title_line in ["", "title: ''\n", "title: 42\n"] {
                    let invalid = format!(
                        "---\ntier: {tier}\n{title_line}goal: outcome\n{extra}---\nbody\n"
                    );
                    let invalid_result =
                        py_plan_validate(py, &invalid, tier, "authoring")
                            .unwrap();
                    let invalid_value =
                        py_to_json_value(invalid_result.bind(py)).unwrap();
                    assert_eq!(invalid_value["ok"], json!(false));
                    assert!(invalid_value["diagnostics"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|diagnostic| diagnostic["field_path"] == "title"));
                }
            }

        let schema = py_plan_frontmatter_schema(py, "epic").unwrap();
        let schema_value = py_to_json_value(schema.bind(py)).unwrap();
        assert_eq!(schema_value[0]["name"], json!("tier"));
        assert_eq!(schema_value[0]["type"], json!("tale | epic"));
        assert!(schema_value
            .as_array()
            .unwrap()
            .iter()
            .any(|field| { field["name"] == json!("phases[].model") }));
        assert!(schema_value
            .as_array()
            .unwrap()
            .iter()
            .any(|field| { field["name"] == json!("phases[].size") }));
        assert!(schema_value
            .as_array()
            .unwrap()
            .iter()
            .any(|field| { field["name"] == json!("parent_bead") }));
        for field_name in ["bead", "parent"] {
            assert!(schema_value
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["name"] == json!(field_name)));
        }
        let tale_schema = py_plan_frontmatter_schema(py, "tale").unwrap();
        let tale_schema_value = py_to_json_value(tale_schema.bind(py)).unwrap();
        assert_eq!(tale_schema_value[1]["name"], json!("title"));
        assert_eq!(tale_schema_value[1]["required"], json!(true));
        assert!(tale_schema_value.as_array().unwrap().iter().any(|field| {
            field["name"] == json!("size") && field["required"] == json!(true)
        }));
        for field_name in ["bead", "parent"] {
            assert!(tale_schema_value
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field["name"] == json!(field_name)));
        }

        let legacy = content.replace("    size: medium\n", "");
        let legacy_result =
            py_plan_validate(py, &legacy, "epic", "launch").unwrap();
        let legacy_value = py_to_json_value(legacy_result.bind(py)).unwrap();
        assert_eq!(legacy_value["ok"], json!(true));
        assert!(legacy_value["diagnostics"].as_array().unwrap().iter().any(
            |diagnostic| { diagnostic["code"] == json!("phase-size-missing") }
        ));
        assert_eq!(legacy_value["plan"]["phases"][0]["size"], json!("small"));

        let error =
            py_plan_validate(py, content, "story", "authoring").unwrap_err();
        assert!(error.to_string().contains("unsupported plan tier"));
    });
}

#[test]
fn plan_reference_bindings_round_trip_json_shapes() {
    pyo3::prepare_freethreaded_python();
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("plans");
    let target = root.join("202608/plan.md");
    fs::create_dir_all(target.parent().unwrap()).unwrap();
    fs::write(&target, "# Plan\n").unwrap();

    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        for name in [
            "plan_reference_parse",
            "plan_reference_render",
            "plan_reference_canonicalize",
            "plan_reference_resolve",
            "plan_reference_resolution_wire_schema_version",
        ] {
            assert!(module.getattr(name).is_ok(), "missing {name}");
        }

        let parsed =
            py_plan_reference_parse(py, "plan:202607/plan.md").unwrap();
        let parsed = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(parsed["kind"], json!("plan"));
        assert_eq!(parsed["legacy"], json!(false));
        assert_eq!(parsed["rendered"], json!("plan:202607/plan.md"));

        let alias =
            py_plan_reference_parse(py, "plans:202607/plan.md").unwrap();
        let alias = py_to_json_value(alias.bind(py)).unwrap();
        assert_eq!(alias["kind"], json!("plan"));
        assert_eq!(alias["legacy"], json!(false));
        assert_eq!(alias["rendered"], json!("plan:202607/plan.md"));

        assert_eq!(
            py_plan_reference_render("plans", "202607/plan.md").unwrap(),
            "plan:202607/plan.md"
        );
        assert_eq!(
            py_plan_reference_canonicalize(
                target.to_str().unwrap(),
                vec![root.to_string_lossy().into_owned()],
            )
            .unwrap()
            .as_deref(),
            Some("plan:202608/plan.md")
        );

        let resolved = py_plan_reference_resolve(
            py,
            "plan:202607/plan.md",
            vec![root.to_string_lossy().into_owned()],
        )
        .unwrap();
        let resolved = py_to_json_value(resolved.bind(py)).unwrap();
        assert_eq!(resolved["schema_version"], json!(1));
        assert_eq!(resolved["status"], json!("drifted"));
        assert_eq!(resolved["resolved_path"], json!(target.to_string_lossy()));
        assert_eq!(py_plan_reference_resolution_wire_schema_version(), 1);
    });
}

#[test]
fn sdd_artifact_link_bindings_match_core_contract() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let rendered = py_sdd_artifact_link_render(
            "PROMPT",
            "202607/prompts/example.md",
            "prompts/example.md",
        )
        .unwrap();
        assert_eq!(
            rendered,
            "- **PROMPT:** [202607/prompts/example.md](prompts/example.md)"
        );

        let document = format!("{rendered}\n\n# Plan\n");
        let parsed = py_sdd_artifact_link_parse(py, &document).unwrap();
        let parsed_value = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(parsed_value["kind"], json!("canonical"));
        assert_eq!(parsed_value["label"], json!("202607/prompts/example.md"));
        assert_eq!(parsed_value["target"], json!("prompts/example.md"));

        assert_eq!(parsed_value["body"], json!("# Plan\n"));

        let legacy = py_sdd_artifact_link_parse(
            py,
            "---\nprompt: 202607/prompts/example.md\n---\n# Plan\n",
        )
        .unwrap();
        let legacy_value = py_to_json_value(legacy.bind(py)).unwrap();
        assert_eq!(legacy_value["kind"], json!("legacy"));
        assert_eq!(
            legacy_value["legacy"]["reference"],
            json!("202607/prompts/example.md")
        );

        let updated = py_sdd_artifact_link_upsert(
            "# Plan\n",
            "PROMPT",
            "202607/prompts/example.md",
            "prompts/example.md",
            true,
            false,
        )
        .unwrap();
        assert_eq!(updated, format!("{rendered}\n\n# Plan\n"));

        assert!(py_sdd_artifact_link_render(
            "PROMPT",
            "202607/prompts/example.md",
            "https://example.com/prompts/example.md",
        )
        .is_ok());
    });
}

#[test]
fn sdd_plan_header_block_bindings_match_core_contract() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        assert_eq!(
            py_sdd_plan_header_block_wire_schema_version(),
            PLAN_HEADER_BLOCK_WIRE_SCHEMA_VERSION
        );
        let sections_obj = json_value_to_py(
                py,
                &json!([
                    {
                        "kind": "PROMPT",
                        "label": "202607/prompts/example.md",
                        "target": "prompts/example.md"
                    },
                    {
                        "kind": "BEAD",
                        "label": "sase-ai.8",
                        "target": "https://github.com/sase-org/sase--beads/blob/main/pages/sase-ai/sase-ai.8.md"
                    },
                    {
                        "kind": "COMMITS",
                        "entries": [{
                            "label": "699456a",
                            "target": "https://github.com/sase-org/sase/commit/699456a",
                            "trailing_text": "fix(parser): wrap safely"
                        }]
                    }
                ]),
            )
            .unwrap();
        let sections = sections_obj.bind(py).downcast::<PyList>().unwrap();
        let rendered = py_sdd_plan_header_block_render(sections).unwrap();
        assert!(rendered.contains("- **PROMPT:**"));
        assert!(rendered.contains("- **COMMITS:**"));

        let document = format!("{rendered}\n\n# Plan\n");
        let parsed = py_sdd_plan_header_block_parse(py, &document).unwrap();
        let parsed = py_to_json_value(parsed.bind(py)).unwrap();
        assert_eq!(parsed["schema_version"], json!(3));
        assert_eq!(parsed["sections"][1]["kind"], json!("BEAD"));
        assert_eq!(parsed["sections"][2]["kind"], json!("COMMITS"));

        let section_obj = json_value_to_py(
                py,
                &json!({
                    "kind": "PARENT",
                    "label": "202607/epic.md",
                    "target": "https://github.com/sase-org/sase--plans/blob/main/202607/epic.md"
                }),
            )
            .unwrap();
        let section = section_obj.bind(py).downcast::<PyDict>().unwrap();
        let updated = py_sdd_plan_header_block_upsert_section(
            &document, section, false, false,
        )
        .unwrap();
        assert!(updated.contains("- **PARENT:**"));

        let removed = py_sdd_plan_header_block_remove_section(
            &updated, "PARENT", false, false,
        )
        .unwrap();
        assert_eq!(removed, document);
    });
}

#[test]
fn plan_search_binding_accepts_explicit_document_corpora() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let temp = tempfile::tempdir().unwrap();
        let designs = temp.path().join("designs");
        fs::create_dir_all(designs.join("202607")).unwrap();
        fs::write(
            designs.join("202607").join("entry.md"),
            "# Binding design\n",
        )
        .unwrap();
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        let kwargs = PyDict::new_bound(py);
        kwargs
            .set_item(
                "document_corpora",
                vec![(
                    designs.to_string_lossy().into_owned(),
                    "designs".to_string(),
                )],
            )
            .unwrap();

        let result = module
            .getattr("plan_search")
            .unwrap()
            .call((), Some(&kwargs))
            .unwrap();
        let value = py_to_json_value(&result).unwrap();

        assert_eq!(value[0]["plan"]["kind"], json!("designs"));
        assert_eq!(value[0]["plan"]["relpath"], json!("202607/entry.md"));
    });
}

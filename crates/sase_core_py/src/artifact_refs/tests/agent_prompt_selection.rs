use super::*;
use crate::json_bridge::json_value_to_py;
use crate::sase_core_rs;
use serde_json::json;

fn select(request: serde_json::Value) -> Option<String> {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("agent_prompt_select").is_ok());
        assert!(module
            .getattr("agent_prompt_selection_wire_schema_version")
            .is_ok());
        assert_eq!(py_agent_prompt_selection_wire_schema_version(), 1);
        let dict_object = json_value_to_py(py, &request).unwrap();
        let dict = dict_object.bind(py).downcast::<PyDict>().unwrap();
        py_agent_prompt_select(dict).unwrap()
    })
}

fn request(
    candidates: Vec<serde_json::Value>,
    is_workflow_child: bool,
    step_name: Option<&str>,
) -> serde_json::Value {
    json!({
        "candidates": candidates,
        "is_workflow_child": is_workflow_child,
        "step_name": step_name,
    })
}

fn candidate(path: &str, mtime_ns: i64) -> serde_json::Value {
    json!({"path": path, "mtime_ns": mtime_ns})
}

#[test]
fn agent_prompt_binding_excludes_raw_across_mtimes_and_orders() {
    for (raw_mtime, expanded_mtime) in [(200, 100), (100, 100), (100, 200)] {
        for order in [false, true] {
            let mut candidates = vec![
                candidate("/artifacts/raw_prompt.md", raw_mtime),
                candidate("/artifacts/01_prompt.md", expanded_mtime),
            ];
            if order {
                candidates.reverse();
            }
            let selected = select(request(candidates, false, None));
            assert_eq!(
                selected,
                Some("/artifacts/01_prompt.md".to_string()),
                "raw {raw_mtime} expanded {expanded_mtime} reversed={order}",
            );
        }
    }
    // Legacy raw spelling is excluded too.
    let selected = select(request(
        vec![
            candidate("/artifacts/raw_xprompt.md", 500),
            candidate("/artifacts/01_prompt.md", 100),
        ],
        false,
        None,
    ));
    assert_eq!(selected, Some("/artifacts/01_prompt.md".to_string()));
}

#[test]
fn agent_prompt_binding_tie_is_deterministic() {
    let first = request(
        vec![
            candidate("/artifacts/01_prompt.md", 100),
            candidate("/artifacts/02_prompt.md", 100),
        ],
        false,
        None,
    );
    let mut reversed_candidates = vec![
        candidate("/artifacts/02_prompt.md", 100),
        candidate("/artifacts/01_prompt.md", 100),
    ];
    reversed_candidates.reverse();
    let second = request(reversed_candidates, false, None);
    assert_eq!(
        select(first.clone()),
        Some("/artifacts/02_prompt.md".to_string())
    );
    assert_eq!(select(first), select(second));
}

#[test]
fn agent_prompt_binding_handles_empty_raw_only_and_finalizer() {
    assert_eq!(select(request(vec![], false, None)), None);
    assert_eq!(
        select(request(
            vec![candidate("/artifacts/raw_prompt.md", 500)],
            false,
            None
        )),
        None
    );
    assert_eq!(
        select(request(
            vec![
                candidate("/artifacts/sase_prompt.md", 100),
                candidate("/artifacts/commit_finalizer_pass_1_prompt.md", 500),
            ],
            false,
            None
        )),
        Some("/artifacts/sase_prompt.md".to_string())
    );
    assert_eq!(
        select(request(
            vec![candidate(
                "/artifacts/commit_finalizer_pass_12_prompt.md",
                500
            )],
            false,
            None
        )),
        None
    );
}

#[test]
fn agent_prompt_binding_prefers_workflow_step_with_fallback() {
    let stepped = request(
        vec![
            candidate("/artifacts/01-other_prompt.md", 500),
            candidate("/artifacts/02-coder_prompt.md", 100),
        ],
        true,
        Some("coder"),
    );
    assert_eq!(
        select(stepped),
        Some("/artifacts/02-coder_prompt.md".to_string())
    );
    let fallback = request(
        vec![
            candidate("/artifacts/01-other_prompt.md", 300),
            candidate("/artifacts/02-else_prompt.md", 100),
        ],
        true,
        Some("coder"),
    );
    assert_eq!(
        select(fallback),
        Some("/artifacts/01-other_prompt.md".to_string())
    );
    // Dict round trip keeps integer mtimes exact (no float conversion).
    let precise = request(
        vec![
            candidate("/artifacts/01_prompt.md", 1_793_575_361_917_208_150),
            candidate("/artifacts/02_prompt.md", 1_793_575_361_917_208_151),
        ],
        false,
        None,
    );
    let selected = select(precise.clone());
    assert_eq!(selected, Some("/artifacts/02_prompt.md".to_string()));
    assert!(precise["candidates"][0]["mtime_ns"].is_number());
}

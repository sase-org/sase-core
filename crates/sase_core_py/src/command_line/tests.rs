use super::*;
use crate::json_bridge::{json_value_to_py, py_to_json_value};
use crate::sase_core_rs;

const MINI_SPEC: &str = r#"{"prog":"sase","version":"0","root":{"name":"sase","path":[],"aliases":[],"hidden":false,"summary":"root","options":[],"positionals":[],"subcommands":[{"name":"bead","path":["bead"],"aliases":[],"hidden":false,"summary":"beads","options":[],"positionals":[],"subcommands":[{"name":"close","path":["bead","close"],"aliases":[],"hidden":false,"summary":"close beads","options":[{"strings":["-R","--resolution"],"dest":"resolution","summary":"resolution","takes_value":true,"repeatable":false,"choices":["done","canceled"],"kind":null,"hidden":false,"required":false,"metavar":null,"default":null,"value_hint":null}],"positionals":[{"metavar":"ID","dest":"ids","summary":"ids","nargs":"+","choices":null,"kind":null,"is_remainder":false,"required":true,"value_hint":null}],"subcommands":[],"default_child":null,"mutex_groups":[],"run_policy":[],"writes":true,"stdin":false}],"default_child":"close","mutex_groups":[],"run_policy":[],"writes":false,"stdin":false}],"default_child":null,"mutex_groups":[],"run_policy":[],"writes":false,"stdin":false}}"#;

#[test]
fn command_line_grammar_binding_round_trips() {
    pyo3::prepare_freethreaded_python();
    Python::with_gil(|py| {
        let module = PyModule::new_bound(py, "sase_core_rs").unwrap();
        sase_core_rs(py, &module).unwrap();
        assert!(module.getattr("CommandLineGrammar").is_ok());

        let cls = module.getattr("CommandLineGrammar").unwrap();
        let grammar = cls.call_method1("__new__", (cls.clone(), MINI_SPEC));
        let _ = grammar;
        let grammar = cls.call1((MINI_SPEC,)).unwrap();
        let schema: u32 = grammar
            .getattr("SCHEMA_VERSION")
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(
            schema,
            sase_core::command_line::COMMAND_LINE_WIRE_SCHEMA_VERSION
        );
        assert!(
            grammar
                .call_method0("__len__")
                .unwrap()
                .extract::<usize>()
                .unwrap()
                > 0
        );

        let resolved = grammar
            .call_method("resolve", ("bead close sase-1 ", 17), None)
            .unwrap();
        let value = py_to_json_value(resolved.as_any()).unwrap();
        assert_eq!(value["node_kind"], "leaf");
        assert_eq!(value["path"], serde_json::json!(["bead", "close"]));
        assert_eq!(value["slot"]["kind"], "positional");
        assert_eq!(value["writes"], true);
        assert_eq!(value["schema_version"], 1);

        let empty = pyo3::types::PyList::empty_bound(py);
        let completed = grammar
            .call_method(
                "complete",
                ("bead cl", 7, empty.clone(), empty.clone(), 100),
                None,
            )
            .unwrap();
        let value = py_to_json_value(completed.as_any()).unwrap();
        assert_eq!(value["kind"], "subcommand");
        assert!(value["total"].as_u64().unwrap_or(0) > 0);

        let dynamic =
            json_value_to_py(py, &serde_json::json!([{"value": "sase-9"}]))
                .unwrap();
        let completed = grammar
            .call_method(
                "complete",
                ("bead close sase-", 15, dynamic, empty.clone(), 100),
                None,
            )
            .unwrap();
        let value = py_to_json_value(completed.as_any()).unwrap();
        assert!(value["total"].as_u64().unwrap_or(0) >= 1);

        let help = grammar
            .call_method("command_help", (vec!["bead", "close"],), None)
            .unwrap();
        assert!(!help.is_none());
        let value = py_to_json_value(help.as_any()).unwrap();
        assert!(value["usage"].as_str().unwrap().starts_with("usage: "));

        let missing = grammar
            .call_method("command_help", (vec!["nosuch"],), None)
            .unwrap();
        assert!(missing.is_none());

        let bad = cls.call1(("not json",));
        assert!(bad.is_err());
        let err = bad.unwrap_err();
        assert!(err.is_instance_of::<PyValueError>(py));

        let bad_dynamic =
            json_value_to_py(py, &serde_json::json!([{"nope": 1}])).unwrap();
        let bad = grammar.call_method(
            "complete",
            ("bead close sase-", 15, bad_dynamic, empty, 100),
            None,
        );
        assert!(bad.is_err());
    });
}

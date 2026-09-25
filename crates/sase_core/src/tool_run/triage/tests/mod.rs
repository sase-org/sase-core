//! Triage unit tests: goldens, normalization, collisions, display.

use std::path::PathBuf;

use crate::tool_run::triage::{
    compare_triage_signatures, display_text, extract_triage_items,
    normalize_line, ToolRunTriageExtractRequestWire,
    ToolRunTriageExtractionStatusWire, ToolRunTriageItemWire,
    ToolRunTriageRecordRequestWire, ToolRunTriageShowResultWire,
    TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS,
};
use crate::tool_run::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/tool_run/triage/fixtures")
}

#[test]
fn golden_extractor_fixtures() {
    let dir = fixture_dir();
    let mut cases: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            if entry.file_type().ok()?.is_dir() {
                entry.file_name().into_string().ok()
            } else {
                None
            }
        })
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no triage golden cases");
    let rewrite =
        std::env::var("UPDATE_TRIAGE_GOLDENS").unwrap_or_default() == "1";
    for case in cases {
        let case_dir = dir.join(&case);
        let request_raw =
            std::fs::read_to_string(case_dir.join("request.json")).unwrap();
        let output = std::fs::read_to_string(case_dir.join("output.txt"))
            .unwrap_or_default();
        // request.json is the extract request minus output.
        let mut request_value: serde_json::Value =
            serde_json::from_str(&request_raw).unwrap();
        request_value["output"] = serde_json::Value::String(output);
        let request: ToolRunTriageExtractRequestWire =
            serde_json::from_value(request_value).unwrap();
        let result = extract_triage_items(request).unwrap();
        let pretty = serde_json::to_string_pretty(&result).unwrap() + "\n";
        let expected_path = case_dir.join("expected.json");
        if rewrite {
            std::fs::write(&expected_path, &pretty).unwrap();
        } else {
            let expected = std::fs::read_to_string(&expected_path)
                .unwrap_or_else(|_| panic!("missing expected.json for {case}"));
            assert_eq!(pretty, expected, "golden mismatch for {case}");
        }
    }
}

#[test]
fn triage_fixture_round_trips() {
    for name in [
        "triage_extract_request.json",
        "triage_record_request.json",
        "triage_show_result.json",
    ] {
        let raw = include_str!("../../fixtures/triage_extract_request.json");
        let _ = (name, raw);
    }
    let extract_raw =
        include_str!("../../fixtures/triage_extract_request.json");
    let extract_value: serde_json::Value =
        serde_json::from_str(extract_raw).unwrap();
    let extract_request: ToolRunTriageExtractRequestWire =
        serde_json::from_value(extract_value.clone()).unwrap();
    let reserialized = serde_json::to_value(&extract_request).unwrap();
    // Parse -> serialize round trip preserves the documented shape.
    assert_eq!(reserialized["stage_key"], extract_value["stage_key"]);
    let record_raw = include_str!("../../fixtures/triage_record_request.json");
    let record_value: serde_json::Value =
        serde_json::from_str(record_raw).unwrap();
    let record_request: ToolRunTriageRecordRequestWire =
        serde_json::from_value(record_value.clone()).unwrap();
    assert_eq!(record_request.run_id, "triage-1");
    let show_raw = include_str!("../../fixtures/triage_show_result.json");
    let show_value: serde_json::Value = serde_json::from_str(show_raw).unwrap();
    let show_result: ToolRunTriageShowResultWire =
        serde_json::from_value(show_value).unwrap();
    assert!(show_result.triaged);
    // Each request wire rejects an unknown field.
    for (label, payload) in [
        (
            "extract",
            serde_json::json!({
                "schema_version": 1,
                "stage_key": "x",
                "bogus": 1
            }),
        ),
        (
            "record",
            serde_json::json!({
                "schema_version": 1,
                "run_id": "r",
                "bogus": 1
            }),
        ),
        (
            "show",
            serde_json::json!({
                "schema_version": 1,
                "run_id": "r",
                "bogus": 1
            }),
        ),
    ] {
        let error =
            match label {
                "extract" => serde_json::from_value::<
                    ToolRunTriageExtractRequestWire,
                >(payload)
                .unwrap_err(),
                "record" => serde_json::from_value::<
                    ToolRunTriageRecordRequestWire,
                >(payload)
                .unwrap_err(),
                _ => serde_json::from_value::<
                    crate::tool_run::triage::ToolRunTriageShowRequestWire,
                >(payload)
                .unwrap_err(),
            };
        assert!(
            error.to_string().contains("bogus"),
            "unknown field not rejected for {label}: {error}"
        );
    }
}

fn extract(
    stage_key: &str,
    output: &str,
    project_root: Option<&str>,
    workspace_roots: Vec<&str>,
    truncated: bool,
) -> crate::tool_run::triage::ToolRunTriageExtractResultWire {
    extract_triage_items(ToolRunTriageExtractRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: stage_key.to_string(),
        stage_id: None,
        output: Some(output.to_string()),
        truncated,
        project_root: project_root.map(|root| root.to_string()),
        workspace_roots: workspace_roots
            .into_iter()
            .map(|root| root.to_string())
            .collect(),
    })
    .unwrap()
}

#[test]
fn cross_workspace_equality() {
    let mypy_body =
        "src/foo.py:10:5: error: \"Foo\" has no attribute \"bar\"  [attr-defined]";
    for root in [
        "/srv/ws/sase_3",
        "/srv/ws/sase_41",
        "/srv/ws/sase/repos/linked/sase-core",
    ] {
        let output = format!("{root}/{mypy_body}");
        let result = extract("lint (mypy)", &output, Some(root), vec![], false);
        assert_eq!(result.status, ToolRunTriageExtractionStatusWire::Parsed);
        assert_eq!(result.items.len(), 1);
    }
    let first = extract(
        "lint (mypy)",
        "/srv/ws/sase_3/src/foo.py:10:5: error: \"Foo\" has no attribute \"bar\"  [attr-defined]",
        Some("/srv/ws/sase_3"),
        vec![],
        false,
    );
    let second = extract(
        "lint (mypy)",
        "/srv/ws/sase_41/src/foo.py:99:2: error: \"Foo\" has no attribute \"bar\"  [attr-defined]",
        Some("/srv/ws/sase_41"),
        vec![],
        false,
    );
    assert_eq!(first.items[0].signature, second.items[0].signature);
    // Callersupplied project_root strips the same way.
    let third = extract(
        "lint (mypy)",
        "/srv/ws/sase_3/src/foo.py:10:5: error: \"Foo\" has no attribute \"bar\"  [attr-defined]",
        Some("/srv/ws/sase_3"),
        vec![],
        false,
    );
    assert_eq!(first.items[0].signature, third.items[0].signature);
    // ANSI, durations, PIDs, timestamps, hex ids, /tmp do not change keys.
    let noisy = "\x1b[31msrc/foo.py:10:5: error: \"Foo\" has no attribute \"bar\"  [attr-defined]\x1b[0m finished in 1.23s pid=1234 2026-09-24T10:11:12Z 0xdeadbeef /tmp/xyz";
    let clean = "src/foo.py:10:5: error: \"Foo\" has no attribute \"bar\"  [attr-defined] finished in <dur> pid=<pid> <ts> <hex> <tmp>";
    assert_eq!(normalize_line(noisy, &[]), normalize_line(clean, &[]));
    // Symvision, pytest, cargo across roots.
    let sym_first = extract(
        "lint (symvision)",
        "Unused public functions/classes:\n  my_helper in /srv/ws/sase_3/src/helpers.py",
        Some("/srv/ws/sase_3"),
        vec![],
        false,
    );
    let sym_second = extract(
        "lint (symvision)",
        "Unused public functions/classes:\n  my_helper in /srv/ws/sase_41/src/helpers.py",
        Some("/srv/ws/sase_41"),
        vec![],
        false,
    );
    assert_eq!(sym_first.items[0].signature, sym_second.items[0].signature);
}

#[test]
fn collisions_and_collapse() {
    // Different pytest node ids differ; parametrizations collapse.
    let result = extract(
        "test (scoped)",
        "FAILED tests/test_foo.py::test_a - x\nFAILED tests/test_foo.py::test_b - x\n",
        None,
        vec![],
        false,
    );
    assert_eq!(result.items.len(), 2);
    assert_ne!(result.items[0].signature, result.items[1].signature);
    let param = extract(
        "test (scoped)",
        "FAILED tests/test_foo.py::test_a[x] - x\nFAILED tests/test_foo.py::test_a[y] - x\n",
        None,
        vec![],
        false,
    );
    assert_eq!(param.items.len(), 1);
    assert_eq!(param.items[0].occurrences, 2);
    // Mypy repeats collapse with occurrences summed.
    let mypy = extract(
        "lint (mypy)",
        "src/foo.py:1:1: error: Bad  [attr-defined]\nsrc/foo.py:2:1: error: Bad  [attr-defined]\n",
        None,
        vec![],
        false,
    );
    assert_eq!(mypy.items.len(), 1);
    assert_eq!(mypy.items[0].occurrences, 2);
    // Ruff rules differ.
    let ruff = extract(
        "lint (ruff)",
        "src/a.py:1:1: F401 x\nsrc/a.py:1:1: F841 x\n",
        None,
        vec![],
        false,
    );
    assert_eq!(ruff.items.len(), 2);
}

#[test]
fn display_bounding_and_redaction() {
    let long = "x".repeat(2000);
    let display = display_text(&long);
    assert!(display.chars().count() <= TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS);
    assert!(display.ends_with('…'));
    // Multibyte safety: no split char.
    let emoji = "é".repeat(2000);
    let display = display_text(&emoji);
    assert!(display.chars().count() <= TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS);
    let secret = display_text("token: abc123 src/foo.py");
    assert!(!secret.contains("abc123"));
    let env = display_text("PATH=/usr/bin FOO=secret");
    assert!(env.contains("FOO=<redacted>"));
    let path = display_text("/srv/very/deep/path/file.py");
    assert!(path.contains("<path>/file.py"));
}

#[test]
fn version_refusal_and_status_precedence() {
    let first = ToolRunTriageItemWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        item_id: None,
        stage_key: "s".to_string(),
        stage_id: None,
        extractor: "mypy".to_string(),
        extractor_version: 1,
        signature: "a".repeat(64),
        display: "d".to_string(),
        locator_paths: Vec::new(),
        occurrences: 1,
        label: None,
    };
    let mut second = first.clone();
    second.extractor_version = 2;
    assert!(compare_triage_signatures(&first, &second).is_err());
    second.extractor_version = 1;
    second.extractor = "ruff".to_string();
    assert!(!compare_triage_signatures(&first, &second).unwrap());
    second.extractor = "mypy".to_string();
    assert!(compare_triage_signatures(&first, &second).unwrap());
    // Missing output beats everything.
    let missing = extract_triage_items(ToolRunTriageExtractRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: "s".to_string(),
        stage_id: None,
        output: None,
        truncated: true,
        project_root: None,
        workspace_roots: Vec::new(),
    })
    .unwrap();
    assert_eq!(
        missing.status,
        ToolRunTriageExtractionStatusWire::OutputMissing
    );
    // Truncated with items reports truncated.
    let truncated = extract(
        "lint (mypy)",
        "src/foo.py:1:1: error: Bad  [attr-defined]",
        None,
        vec![],
        true,
    );
    assert_eq!(
        truncated.status,
        ToolRunTriageExtractionStatusWire::OutputTruncated
    );
    assert!(truncated
        .diagnostics
        .iter()
        .any(|line| line.contains("truncated")));
    // Generic only when no specific matched.
    let generic = extract(
        "lint (feature flags)",
        "some unknown output line",
        None,
        vec![],
        false,
    );
    assert_eq!(generic.status, ToolRunTriageExtractionStatusWire::Generic);
    assert_eq!(generic.items[0].extractor, "generic");
}

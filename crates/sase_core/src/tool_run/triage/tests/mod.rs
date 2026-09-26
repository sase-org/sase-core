//! Triage unit tests: goldens, normalization, collisions, display.

use std::path::PathBuf;

use crate::tool_run::triage::{
    compare_triage_signatures, display_text, extract_triage_items,
    normalize_line, tool_run_triage_classify, tool_run_triage_verdict,
    ToolRunTriageClassifyRequestWire, ToolRunTriageEvidenceItemWire,
    ToolRunTriageEvidenceRunWire, ToolRunTriageExtractRequestWire,
    ToolRunTriageExtractionStatusWire, ToolRunTriageFlakeEntryWire,
    ToolRunTriageItemWire, ToolRunTriageKnobsWire,
    ToolRunTriageOwnerCandidateWire, ToolRunTriageRecordRequestWire,
    ToolRunTriageShowResultWire, ToolRunTriageSubjectItemWire,
    ToolRunTriageSubjectRunWire, ToolRunTriageVerdictItemWire,
    ToolRunTriageVerdictRequestWire, TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS,
    TOOL_RUN_TRIAGE_LOOKBACK_SECS,
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

fn sig(n: u64) -> String {
    format!("{n:064x}")
}

fn subject_run(
    dirty: Vec<&str>,
    fingerprint_digest: &str,
    base: &str,
) -> ToolRunTriageSubjectRunWire {
    ToolRunTriageSubjectRunWire {
        run_id: "subject-1".to_string(),
        project: "sase".to_string(),
        tool: "check".to_string(),
        extra_args_digest: "args-1".to_string(),
        workspace: Some("ws-a".to_string()),
        machine: Some("athena".to_string()),
        base_head: Some(base.to_string()),
        dirty_paths: dirty.into_iter().map(str::to_string).collect(),
        complete_fingerprint: true,
        fingerprint_digest: Some(fingerprint_digest.to_string()),
        ad_hoc: false,
    }
}

fn subject_item(
    stage: &str,
    extractor: &str,
    signature: String,
    locators: Vec<&str>,
) -> ToolRunTriageSubjectItemWire {
    ToolRunTriageSubjectItemWire {
        stage_key: stage.to_string(),
        extractor: extractor.to_string(),
        extractor_version: 1,
        signature,
        locator_paths: locators.into_iter().map(str::to_string).collect(),
    }
}

#[allow(clippy::too_many_arguments)]
fn evidence_run(
    run_id: &str,
    workspace: &str,
    base: &str,
    dirty: Vec<&str>,
    signatures: Vec<String>,
    stage_completions: Vec<&str>,
    settled_ts: i64,
    failed: bool,
    fingerprint_digest: &str,
) -> ToolRunTriageEvidenceRunWire {
    ToolRunTriageEvidenceRunWire {
        run_id: run_id.to_string(),
        project: "sase".to_string(),
        tool: "check".to_string(),
        extra_args_digest: "args-1".to_string(),
        workspace: Some(workspace.to_string()),
        agent: Some(format!("agent-{workspace}")),
        machine: Some("athena".to_string()),
        settled_ts,
        base_head: Some(base.to_string()),
        complete_fingerprint: true,
        fingerprint_digest: Some(fingerprint_digest.to_string()),
        dirty_paths: dirty.into_iter().map(str::to_string).collect(),
        dirty_unknown: false,
        clean_tree: false,
        ad_hoc: false,
        failed,
        stage_completions: stage_completions
            .into_iter()
            .map(str::to_string)
            .collect(),
        items: signatures
            .into_iter()
            .map(|signature| ToolRunTriageEvidenceItemWire {
                extractor: "mypy".to_string(),
                extractor_version: 1,
                signature,
                stage_key: Some("lint (mypy)".to_string()),
            })
            .collect(),
        selection_source: false,
    }
}

fn classify(
    subject_dirty: Vec<&str>,
    subjects: Vec<ToolRunTriageSubjectItemWire>,
    evidence: Vec<ToolRunTriageEvidenceRunWire>,
    ancestry: Vec<&str>,
    baseline: Vec<ToolRunTriageFlakeEntryWire>,
    knobs: ToolRunTriageKnobsWire,
) -> crate::tool_run::triage::ToolRunTriageClassifyResultWire {
    tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(subject_dirty, "fp-1", "head-3"),
        subjects,
        evidence_runs: evidence,
        selection_records: Vec::new(),
        ancestry: ancestry.into_iter().map(str::to_string).collect(),
        flake_baseline: baseline,
        owner_candidates: Vec::new(),
        knobs,
        now_ts: Some(2000),
    })
    .unwrap()
}

#[test]
fn ledger_known_witness_at_ancestor() {
    // (a) witnessed by another workspace at an ancestor HEAD -> KNOWN.
    let signature = sig(1);
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature.clone(),
            vec!["src/foo.py"],
        )],
        vec![evidence_run(
            "witness-1",
            "ws-b",
            "head-2",
            vec![],
            vec![signature],
            vec!["lint (mypy)"],
            1000,
            true,
            "fp-0",
        )],
        vec!["head-3", "head-2", "head-1"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "known");
    let evidence = &result.labels[0].evidence;
    assert_eq!(
        evidence["witness_run_ids"],
        serde_json::json!(["witness-1"])
    );
    assert_eq!(evidence["distinct_workspaces"], serde_json::json!(1));
}

#[test]
fn ledger_touched_without_witness_is_new() {
    // (b) touched with no witness -> NEW.
    let signature = sig(2);
    let result = classify(
        vec!["src/foo.py"],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![],
        vec!["head-3"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "new");
    assert!(result.labels[0].touched);
}

#[test]
fn ledger_baseline_is_flaky() {
    // (c) listed in the flake baseline -> FLAKY.
    let signature = sig(3);
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature.clone(),
            vec!["src/foo.py"],
        )],
        vec![],
        vec!["head-3"],
        vec![ToolRunTriageFlakeEntryWire {
            extractor: "mypy".to_string(),
            extractor_version: 1,
            signature,
            source: Some("tests/reproducible_flake_baseline.txt:7".to_string()),
        }],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "flaky");
}

#[test]
fn ledger_same_fingerprint_pytest_flip_is_flaky() {
    // (d) pytest fail->pass on identical complete fingerprint -> FLAKY.
    let signature = sig(4);
    let mut witness = evidence_run(
        "witness-1",
        "ws-b",
        "head-3",
        vec![],
        vec![],
        vec!["test (scoped)"],
        1000,
        true,
        "fp-1",
    );
    witness.items.clear();
    let subject = ToolRunTriageSubjectItemWire {
        stage_key: "test (scoped)".to_string(),
        extractor: "pytest".to_string(),
        extractor_version: 1,
        signature: signature.clone(),
        locator_paths: vec!["tests/test_foo.py".to_string()],
    };
    let result = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: vec![subject],
        evidence_runs: vec![witness],
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: Vec::new(),
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    assert_eq!(result.labels[0].class.as_str(), "flaky");
}

#[test]
fn ledger_same_fingerprint_symvision_flip_is_not_flaky() {
    // (e) the same flip in a symvision stage is not FLAKY.
    let signature = sig(5);
    let witness = evidence_run(
        "witness-1",
        "ws-b",
        "head-3",
        vec![],
        vec![],
        vec!["lint (symvision)"],
        1000,
        true,
        "fp-1",
    );
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (symvision)",
            "symvision",
            signature,
            vec!["src/helpers.py"],
        )],
        vec![witness],
        vec!["head-3"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_ne!(result.labels[0].class.as_str(), "flaky");
}

#[test]
fn ledger_untouched_without_evidence_is_unknown() {
    // (f) untouched with no evidence -> UNKNOWN with reason.
    let signature = sig(6);
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![],
        vec!["head-3"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "unknown");
    let reasons = result.labels[0].evidence["rejection_reasons"].clone();
    assert!(reasons
        .as_array()
        .unwrap()
        .contains(&serde_json::json!("untouched_no_pass_witness")));
}

#[test]
fn ledger_cleared_witness_is_not_known() {
    // (g) witness cleared by a newer ancestor run that completed the stage.
    let signature = sig(7);
    let witness = evidence_run(
        "witness-1",
        "ws-b",
        "head-1",
        vec![],
        vec![signature.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-0",
    );
    let clearing = evidence_run(
        "clearing-1",
        "ws-c",
        "head-2",
        vec![],
        vec![],
        vec!["lint (mypy)"],
        1500,
        true,
        "fp-2",
    );
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![witness, clearing],
        vec!["head-3", "head-2", "head-1"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_ne!(result.labels[0].class.as_str(), "known");
}

#[test]
fn ledger_adhoc_and_arg_mismatch_never_witness() {
    // (h) ad-hoc or different extra_args_digest is never a witness.
    let signature = sig(8);
    let mut adhoc = evidence_run(
        "adhoc-1",
        "ws-b",
        "head-2",
        vec![],
        vec![signature.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-0",
    );
    adhoc.ad_hoc = true;
    let mut mismatch = evidence_run(
        "other-args",
        "ws-c",
        "head-2",
        vec![],
        vec![signature.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-0",
    );
    mismatch.extra_args_digest = "args-2".to_string();
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![adhoc, mismatch],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "unknown");
}

#[test]
fn ledger_bead_match_never_known() {
    // (i) open bead title match with no witness -> UNKNOWN, not KNOWN.
    let signature = sig(9);
    let result = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        evidence_runs: Vec::new(),
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: vec![ToolRunTriageOwnerCandidateWire {
            node_id: "sase-999".to_string(),
            location: Some("src/foo.py".to_string()),
            title: Some("Fix foo".to_string()),
            status: "open".to_string(),
            closed_ts: None,
        }],
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    assert_eq!(result.labels[0].class.as_str(), "unknown");
}

#[test]
fn ledger_cross_project_never_witnesses() {
    // (j) cross-project evidence never witnesses.
    let signature = sig(10);
    let mut foreign = evidence_run(
        "foreign-1",
        "ws-b",
        "head-2",
        vec![],
        vec![signature.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-0",
    );
    foreign.project = "sase-core".to_string();
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![foreign],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "unknown");
}

#[test]
fn knobs_tighten_known() {
    let signature = sig(11);
    let witness = || {
        evidence_run(
            "witness-1",
            "ws-b",
            "head-2",
            vec![],
            vec![signature.clone()],
            vec!["lint (mypy)"],
            1000,
            true,
            "fp-0",
        )
    };
    // Default knob: one witness suffices.
    let loose = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature.clone(),
            vec!["src/foo.py"],
        )],
        vec![witness()],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(loose.labels[0].class.as_str(), "known");
    // Tightened: two distinct workspaces required.
    let tight = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature.clone(),
            vec!["src/foo.py"],
        )],
        vec![witness()],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire {
            min_witnesses: 2,
            touched_requires_clean_witness: false,
        },
    );
    assert_eq!(tight.labels[0].class.as_str(), "unknown");
    // Touched item needs a clean-tree witness when the knob is set.
    let touched = classify(
        vec!["src/foo.py"],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature.clone(),
            vec!["src/foo.py"],
        )],
        vec![witness()],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire {
            min_witnesses: 1,
            touched_requires_clean_witness: true,
        },
    );
    // The single witness is not clean-tree, so touched stays NEW via the
    // touched path (not KNOWN).
    assert_eq!(touched.labels[0].class.as_str(), "new");
}

#[test]
fn owner_matching_at_most_two_open_first() {
    let signature = sig(12);
    let candidates = vec![
        ToolRunTriageOwnerCandidateWire {
            node_id: "sase-3".to_string(),
            location: Some("src/foo.py".to_string()),
            title: None,
            status: "open".to_string(),
            closed_ts: None,
        },
        ToolRunTriageOwnerCandidateWire {
            node_id: "sase-1".to_string(),
            location: Some("src/foo.py".to_string()),
            title: None,
            status: "open".to_string(),
            closed_ts: None,
        },
        ToolRunTriageOwnerCandidateWire {
            node_id: "sase-2".to_string(),
            location: Some("src/foo.py".to_string()),
            title: None,
            status: "closed".to_string(),
            closed_ts: Some(1900),
        },
    ];
    let result = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec!["src/foo.py"], "fp-1", "head-3"),
        subjects: vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        evidence_runs: Vec::new(),
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: candidates,
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    let owners = result.labels[0].possible_owners.as_array().unwrap();
    assert_eq!(owners.len(), 2);
    assert_eq!(owners[0]["id"], serde_json::json!("sase-1"));
    assert_eq!(owners[0]["matched_on"], serde_json::json!("location"));
    assert_eq!(owners[1]["id"], serde_json::json!("sase-3"));
    assert_eq!(owners[1]["matched_on"], serde_json::json!("location"));
}

fn owner_candidate(
    node_id: &str,
    location: Option<&str>,
    title: Option<&str>,
    status: &str,
    closed_ts: Option<i64>,
) -> ToolRunTriageOwnerCandidateWire {
    ToolRunTriageOwnerCandidateWire {
        node_id: node_id.to_string(),
        location: location.map(str::to_string),
        title: title.map(str::to_string),
        status: status.to_string(),
        closed_ts,
    }
}

fn classify_owners(
    locators: Vec<&str>,
    candidates: Vec<ToolRunTriageOwnerCandidateWire>,
    now_ts: i64,
) -> crate::tool_run::triage::ToolRunTriageClassifyResultWire {
    tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: vec![subject_item("lint (mypy)", "mypy", sig(20), locators)],
        evidence_runs: Vec::new(),
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: candidates,
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(now_ts),
    })
    .unwrap()
}

fn owner_ids(
    result: &crate::tool_run::triage::ToolRunTriageClassifyResultWire,
) -> Vec<(String, String, String)> {
    result.labels[0]
        .possible_owners
        .as_array()
        .unwrap()
        .iter()
        .map(|owner| {
            (
                owner["id"].as_str().unwrap().to_string(),
                owner["reason"].as_str().unwrap().to_string(),
                owner["matched_on"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn owner_match_sase_191_3_probe_yields_no_owners() {
    // Locator src/sase/tool/executor.py used to match sase-106 / sase-10a
    // because every sase-* bead id contains the token "sase".
    let result = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![
            owner_candidate(
                "sase-106",
                Some(
                    "src/sase/gate_shell/handoff.py and __init__.py; agent/launch_request.py; main/gate_handler.py; xprompt/workflow_hitl_gate.py",
                ),
                Some(
                    "Restore gate creator handoff exports removed by coder-recovery refactor",
                ),
                "open",
                None,
            ),
            owner_candidate(
                "sase-10a",
                Some(
                    "crates/sase_gateway/src/routes.rs::routes::tests::fleet_mutate_refuses_terminal_missing_capability_and_bridge_failure",
                ),
                Some(
                    "sase_gateway fleet mutate/launch route tests fail at sase-core 17947a0",
                ),
                "open",
                None,
            ),
            owner_candidate("sase-999", None, None, "open", None),
        ],
        2000,
    );
    assert!(result.labels[0]
        .possible_owners
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn owner_match_location_and_title_cases() {
    let pytest_node = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-loc",
            Some("src/sase/tool/executor.py::test_timeout"),
            Some("unrelated title"),
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&pytest_node),
        vec![(
            "sase-loc".to_string(),
            "possible owner".to_string(),
            "location".to_string()
        )]
    );

    let prefix = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-dir",
            Some("src/sase/tool"),
            None,
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&prefix),
        vec![(
            "sase-dir".to_string(),
            "possible owner".to_string(),
            "location".to_string()
        )]
    );

    let listed = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-list",
            Some("src/other.py and src/sase/tool/executor.py:42 (lines 10-20)"),
            None,
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&listed),
        vec![(
            "sase-list".to_string(),
            "possible owner".to_string(),
            "location".to_string()
        )]
    );

    let title_path = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-title-path",
            None,
            Some("Fix src/sase/tool/executor.py timeout"),
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&title_path),
        vec![(
            "sase-title-path".to_string(),
            "possible owner".to_string(),
            "title".to_string()
        )]
    );

    let title_file = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-title-file",
            None,
            Some("Fix executor.py timeout"),
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&title_file),
        vec![(
            "sase-title-file".to_string(),
            "possible owner".to_string(),
            "title".to_string()
        )]
    );

    let both = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-both",
            Some("src/sase/tool/executor.py"),
            Some("Fix executor.py timeout"),
            "open",
            None,
        )],
        2000,
    );
    assert_eq!(
        owner_ids(&both),
        vec![(
            "sase-both".to_string(),
            "possible owner".to_string(),
            "location".to_string()
        )]
    );
}

#[test]
fn owner_match_generic_filename_stoplist() {
    for (locator, title) in [
        ("src/sase/__init__.py", "touch __init__.py"),
        ("tests/conftest.py", "rewrite conftest.py"),
        ("crates/sase_core/src/lib.rs", "export from lib.rs"),
        ("crates/sase_core/src/mod.rs", "split mod.rs"),
        ("src/main.py", "rewrite main.py"),
        ("src/main.rs", "rewrite main.rs"),
        ("Justfile", "update the Justfile"),
    ] {
        let result = classify_owners(
            vec![locator],
            vec![owner_candidate(
                "sase-generic",
                None,
                Some(title),
                "open",
                None,
            )],
            2000,
        );
        assert!(
            result.labels[0]
                .possible_owners
                .as_array()
                .unwrap()
                .is_empty(),
            "generic title match for {locator}"
        );
    }

    let subword = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-subword",
            None,
            Some("touch my_executor.py backup"),
            "open",
            None,
        )],
        2000,
    );
    assert!(subword.labels[0]
        .possible_owners
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn owner_match_closed_within_lookback_is_possibly_fixed() {
    let now = 10_000_000;
    let recent = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-closed",
            Some("src/sase/tool/executor.py"),
            None,
            "closed",
            Some(now - 60),
        )],
        now,
    );
    assert_eq!(
        owner_ids(&recent),
        vec![(
            "sase-closed".to_string(),
            "possibly fixed".to_string(),
            "location".to_string()
        )]
    );

    let stale = classify_owners(
        vec!["src/sase/tool/executor.py"],
        vec![owner_candidate(
            "sase-stale",
            Some("src/sase/tool/executor.py"),
            None,
            "closed",
            Some(now - TOOL_RUN_TRIAGE_LOOKBACK_SECS - 1),
        )],
        now,
    );
    assert!(stale.labels[0]
        .possible_owners
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn repeat_only_for_same_fingerprint_and_signatures() {
    let first = sig(13);
    let second = sig(14);
    let mut prior = evidence_run(
        "prior-1",
        "ws-b",
        "head-2",
        vec![],
        vec![first.clone(), second.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-1",
    );
    prior.items.push(ToolRunTriageEvidenceItemWire {
        extractor: "mypy".to_string(),
        extractor_version: 1,
        signature: second.clone(),
        stage_key: Some("lint (mypy)".to_string()),
    });
    // Deduplicate to the exact subject set below (two distinct sigs).
    prior.items.truncate(2);
    let result = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: vec![
            subject_item("lint (mypy)", "mypy", first, vec!["src/a.py"]),
            subject_item("lint (mypy)", "mypy", second, vec!["src/b.py"]),
        ],
        evidence_runs: vec![prior],
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string(), "head-2".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: Vec::new(),
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    assert_eq!(result.repeat_of, Some("prior-1".to_string()));
}

#[test]
fn cross_machine_evidence_never_witnesses() {
    let signature = sig(15);
    let mut foreign = evidence_run(
        "foreign-1",
        "ws-b",
        "head-2",
        vec![],
        vec![signature.clone()],
        vec!["lint (mypy)"],
        1000,
        true,
        "fp-0",
    );
    foreign.machine = Some("apollo".to_string());
    let result = classify(
        vec![],
        vec![subject_item(
            "lint (mypy)",
            "mypy",
            signature,
            vec!["src/foo.py"],
        )],
        vec![foreign],
        vec!["head-3", "head-2"],
        vec![],
        ToolRunTriageKnobsWire::default(),
    );
    assert_eq!(result.labels[0].class.as_str(), "unknown");
}

#[test]
fn classification_is_permutation_stable() {
    let first = sig(16);
    let second = sig(17);
    let subjects = vec![
        subject_item("lint (mypy)", "mypy", first.clone(), vec!["src/a.py"]),
        subject_item("lint (mypy)", "mypy", second.clone(), vec!["src/b.py"]),
    ];
    let evidence = vec![
        evidence_run(
            "witness-1",
            "ws-b",
            "head-2",
            vec![],
            vec![first.clone()],
            vec!["lint (mypy)"],
            1000,
            true,
            "fp-0",
        ),
        evidence_run(
            "witness-2",
            "ws-c",
            "head-2",
            vec![],
            vec![second.clone()],
            vec!["lint (mypy)"],
            1100,
            true,
            "fp-0",
        ),
    ];
    let forward = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: subjects.clone(),
        evidence_runs: evidence.clone(),
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string(), "head-2".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: Vec::new(),
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    let mut reversed_subjects = subjects;
    reversed_subjects.reverse();
    let mut reversed_evidence = evidence;
    reversed_evidence.reverse();
    let backward = tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        subject_run: subject_run(vec![], "fp-1", "head-3"),
        subjects: reversed_subjects,
        evidence_runs: reversed_evidence,
        selection_records: Vec::new(),
        ancestry: vec!["head-3".to_string(), "head-2".to_string()],
        flake_baseline: Vec::new(),
        owner_candidates: Vec::new(),
        knobs: ToolRunTriageKnobsWire::default(),
        now_ts: Some(2000),
    })
    .unwrap();
    assert_eq!(
        serde_json::to_string(&forward).unwrap(),
        serde_json::to_string(&backward).unwrap()
    );
}

#[test]
fn generic_and_environment_are_always_unknown() {
    for extractor in ["generic", "environment"] {
        let result = classify(
            vec!["src/foo.py"],
            vec![subject_item(
                "lint (x)",
                extractor,
                sig(18),
                vec!["src/foo.py"],
            )],
            vec![],
            vec!["head-3"],
            vec![],
            ToolRunTriageKnobsWire::default(),
        );
        assert_eq!(
            result.labels[0].class.as_str(),
            "unknown",
            "extractor {extractor}"
        );
    }
}

#[test]
fn verdict_terminal_causes_and_legacy_mapping() {
    for (cause, kind) in [
        ("stop_requested", "control"),
        ("interrupt", "control"),
        ("timeout", "control"),
        ("launch_failed", "infrastructure"),
        ("owner_lost", "infrastructure"),
        ("wrapper_lost", "infrastructure"),
        ("signal", "infrastructure"),
    ] {
        let result = tool_run_triage_verdict(ToolRunTriageVerdictRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            exit_code: Some(1),
            terminal_cause: Some(cause.to_string()),
            legacy_state: None,
            legacy_exit_code: None,
            legacy_signal: None,
            legacy_interruption_reason: None,
            legacy_lost_reason: None,
            has_completed_stage: true,
            has_setup_marker: false,
            has_failed_stage: true,
            all_stages_complete: true,
            recipe_finished: true,
            is_stageful_tool: true,
            triaged: true,
            has_unparsed_failed_stage: false,
            items: vec![ToolRunTriageVerdictItemWire {
                class: Some("known".to_string()),
            }],
        })
        .unwrap();
        assert_eq!(result.kind.as_str(), kind, "cause {cause}");
        assert_eq!(result.verdict.as_str(), "undetermined");
    }
    // Exit 0 means pass.
    let passed = tool_run_triage_verdict(ToolRunTriageVerdictRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        exit_code: Some(0),
        terminal_cause: Some("exited".to_string()),
        legacy_state: None,
        legacy_exit_code: None,
        legacy_signal: None,
        legacy_interruption_reason: None,
        legacy_lost_reason: None,
        has_completed_stage: true,
        has_setup_marker: false,
        has_failed_stage: false,
        all_stages_complete: true,
        recipe_finished: true,
        is_stageful_tool: true,
        triaged: true,
        has_unparsed_failed_stage: false,
        items: Vec::new(),
    })
    .unwrap();
    assert_eq!(passed.verdict.as_str(), "pass");
    // Legacy mapping: ambiguous rows are infrastructure, never verification.
    for (state, exit, kind) in [
        (Some("succeeded"), None, "none"),
        (Some("failed"), Some(1), "verification"),
        (Some("failed"), Some(126), "infrastructure"),
        (Some("failed"), None, "infrastructure"),
        (Some("interrupted"), None, "control"),
        (Some("signaled"), None, "infrastructure"),
        (Some("lost"), None, "infrastructure"),
    ] {
        let result = tool_run_triage_verdict(ToolRunTriageVerdictRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            exit_code: None,
            terminal_cause: None,
            legacy_state: state.map(str::to_string),
            legacy_exit_code: exit,
            legacy_signal: None,
            legacy_interruption_reason: None,
            legacy_lost_reason: None,
            has_completed_stage: true,
            has_setup_marker: false,
            has_failed_stage: true,
            all_stages_complete: true,
            recipe_finished: true,
            is_stageful_tool: true,
            triaged: true,
            has_unparsed_failed_stage: false,
            items: vec![ToolRunTriageVerdictItemWire {
                class: Some("known".to_string()),
            }],
        })
        .unwrap();
        assert_eq!(result.kind.as_str(), kind);
    }
    // Environment _setup marker with no completed stage.
    let environment =
        tool_run_triage_verdict(ToolRunTriageVerdictRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            exit_code: Some(1),
            terminal_cause: Some("exited".to_string()),
            legacy_state: None,
            legacy_exit_code: None,
            legacy_signal: None,
            legacy_interruption_reason: None,
            legacy_lost_reason: None,
            has_completed_stage: false,
            has_setup_marker: true,
            has_failed_stage: false,
            all_stages_complete: false,
            recipe_finished: false,
            is_stageful_tool: true,
            triaged: true,
            has_unparsed_failed_stage: false,
            items: vec![ToolRunTriageVerdictItemWire {
                class: Some("unknown".to_string()),
            }],
        })
        .unwrap();
    assert_eq!(environment.kind.as_str(), "environment");
    assert_eq!(environment.verdict.as_str(), "undetermined");
    assert!(environment.remedy.is_some());
}

#[test]
fn verdict_table_boundaries() {
    // no_new_failures requires all KNOWN/FLAKY, complete stages, finish,
    // stageful tool, and triaged rows.
    let base = ToolRunTriageVerdictRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        exit_code: Some(1),
        terminal_cause: Some("exited".to_string()),
        legacy_state: None,
        legacy_exit_code: None,
        legacy_signal: None,
        legacy_interruption_reason: None,
        legacy_lost_reason: None,
        has_completed_stage: true,
        has_setup_marker: false,
        has_failed_stage: true,
        all_stages_complete: true,
        recipe_finished: true,
        is_stageful_tool: true,
        triaged: true,
        has_unparsed_failed_stage: false,
        items: vec![
            ToolRunTriageVerdictItemWire {
                class: Some("known".to_string()),
            },
            ToolRunTriageVerdictItemWire {
                class: Some("flaky".to_string()),
            },
        ],
    };
    assert_eq!(
        tool_run_triage_verdict(base.clone())
            .unwrap()
            .verdict
            .as_str(),
        "no_new_failures"
    );
    for (label, mutate) in [
        ("without recipe finish", {
            let mut request = base.clone();
            request.recipe_finished = false;
            request
        }),
        ("with unknown", {
            let mut request = base.clone();
            request.items.push(ToolRunTriageVerdictItemWire {
                class: Some("unknown".to_string()),
            });
            request
        }),
        ("stages none", {
            let mut request = base.clone();
            request.is_stageful_tool = false;
            request
        }),
        ("untriaged", {
            let mut request = base.clone();
            request.triaged = false;
            request
        }),
        ("missing labels", {
            let mut request = base.clone();
            request
                .items
                .push(ToolRunTriageVerdictItemWire { class: None });
            request
        }),
    ] {
        assert_eq!(
            tool_run_triage_verdict(mutate).unwrap().verdict.as_str(),
            "undetermined",
            "{label}"
        );
    }
    // NEW items dominate.
    let mut new_failures = base.clone();
    new_failures.items.push(ToolRunTriageVerdictItemWire {
        class: Some("new".to_string()),
    });
    assert_eq!(
        tool_run_triage_verdict(new_failures)
            .unwrap()
            .verdict
            .as_str(),
        "new_failures"
    );
}

#[test]
fn classify_and_verdict_fixture_round_trips() {
    let classify_raw =
        include_str!("../../fixtures/triage_classify_request.json");
    let classify_value: serde_json::Value =
        serde_json::from_str(classify_raw).unwrap();
    let classify_request: ToolRunTriageClassifyRequestWire =
        serde_json::from_value(classify_value).unwrap();
    let classified = tool_run_triage_classify(classify_request).unwrap();
    assert_eq!(classified.labels[0].class.as_str(), "known");
    let expected_raw =
        include_str!("../../fixtures/triage_classify_result.json");
    let expected: serde_json::Value =
        serde_json::from_str(expected_raw).unwrap();
    assert_eq!(
        classified.labels[0].class.as_str(),
        expected["labels"][0]["class"].as_str().unwrap()
    );
    for name in [
        "triage_stage_request.json",
        "triage_settle_request.json",
        "triage_failures_request.json",
        "triage_failures_result.json",
    ] {
        let raw = std::fs::read_to_string(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("src/tool_run/fixtures")
                .join(name),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(value["schema_version"], serde_json::json!(1), "{name}");
    }
    // New request wires reject unknown fields; results stay lenient.
    let error = serde_json::from_value::<ToolRunTriageClassifyRequestWire>(
        serde_json::json!({"schema_version": 1, "bogus": 1}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("bogus"));
    let error = serde_json::from_value::<ToolRunTriageVerdictRequestWire>(
        serde_json::json!({"schema_version": 1, "bogus": 1}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("bogus"));
}

use serde_json::json;

use super::*;

const H: &str = "1111111111111111111111111111111111111111";
const U: &str = "2222222222222222222222222222222222222222";
const B: &str = "3333333333333333333333333333333333333333";
const NEXT: &str = "4444444444444444444444444444444444444444";
const TREE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NEXT_TREE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ROOT: &str = "/work/project";

fn change(
    path: &str,
    kind: RebaseGateChangeKindWire,
) -> RebaseGateChangedPathWire {
    RebaseGateChangedPathWire {
        path: path.into(),
        kind,
        old_path: None,
    }
}

fn facts() -> RebaseGateFactsWire {
    RebaseGateFactsWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        repository_root: ROOT.into(),
        old_head: H.into(),
        upstream_tip: U.into(),
        merge_base: B.into(),
        rebase_outcome: RebaseGateRebaseOutcomeWire::Applied,
        current_head: NEXT.into(),
        current_tree: TREE.into(),
        local_changes: vec![
            change("src/z.py", RebaseGateChangeKindWire::Modified),
            change("gone.py", RebaseGateChangeKindWire::Deleted),
        ],
        upstream_changes: vec![
            change("src/a.py", RebaseGateChangeKindWire::Added),
            RebaseGateChangedPathWire {
                path: "src/new.py".into(),
                kind: RebaseGateChangeKindWire::Renamed,
                old_path: Some("src/old.py".into()),
            },
            change("src/z.py", RebaseGateChangeKindWire::Modified),
        ],
    }
}

fn context() -> RebaseGateContextWire {
    build_rebase_gate_context(&facts()).unwrap().unwrap()
}

fn classify_request(
    exit_code: Option<i32>,
    timed_out: bool,
    report: Option<serde_json::Value>,
) -> RebaseGateClassifyRequestWire {
    RebaseGateClassifyRequestWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        context: context(),
        config: RebaseGateConfigWire {
            hook_command: "checker".into(),
            timeout_seconds: 10.0,
        },
        command: RebaseGateCommandFactsWire {
            exit_code,
            timed_out,
            ..Default::default()
        },
        report,
    }
}

fn clean_report() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "checked_head": NEXT,
        "checked_tree": TREE,
        "status": "clean",
        "findings": [],
        "diagnostics": null
    })
}

fn finding_report() -> serde_json::Value {
    json!({
        "schema_version": 1,
        "checked_head": NEXT,
        "checked_tree": TREE,
        "status": "findings",
        "findings": [{
            "path": "src/a.py",
            "line": 9,
            "module": "sase.removed",
            "imported_name": "Thing",
            "explanation": "name is absent from the first-party module"
        }],
        "diagnostics": null
    })
}

#[test]
fn eligibility_uses_divergence_and_actual_rebase_outcome() {
    let context = context();
    assert_eq!(context.inputs.len(), 1);
    assert_eq!(context.changed_paths.len(), 4);
    assert!(context
        .changed_paths
        .iter()
        .any(|change| change.path == "gone.py"
            && change.kind == RebaseGateChangeKindWire::Deleted));
    assert!(context.changed_paths.iter().any(|change| {
        change.path == "src/new.py"
            && change.old_path.as_deref() == Some("src/old.py")
    }));
    assert!(context
        .changed_paths
        .windows(2)
        .all(|pair| pair[0] <= pair[1]));

    let mut no_op = facts();
    no_op.rebase_outcome = RebaseGateRebaseOutcomeWire::NoOp;
    no_op.current_head = "5555555555555555555555555555555555555555".into();
    assert!(build_rebase_gate_context(&no_op).unwrap().is_none());

    let mut already_integrated = facts();
    already_integrated.merge_base = already_integrated.old_head.clone();
    assert!(build_rebase_gate_context(&already_integrated)
        .unwrap()
        .is_none());

    let mut fast_forward = facts();
    fast_forward.merge_base = fast_forward.upstream_tip.clone();
    assert!(build_rebase_gate_context(&fast_forward).unwrap().is_none());

    let mut conflict = facts();
    conflict.rebase_outcome = RebaseGateRebaseOutcomeWire::Conflict;
    assert!(build_rebase_gate_context(&conflict).unwrap().is_some());
}

#[test]
fn malformed_git_facts_and_non_normalized_paths_are_rejected() {
    let mut invalid_id = facts();
    invalid_id.old_head = "short".into();
    assert!(build_rebase_gate_context(&invalid_id).is_err());

    let mut invalid_path = facts();
    invalid_path.local_changes.push(change(
        "src/../outside.py",
        RebaseGateChangeKindWire::Modified,
    ));
    assert!(build_rebase_gate_context(&invalid_path).is_err());

    let mut invalid_rename = facts();
    invalid_rename.upstream_changes[1].old_path = None;
    assert!(build_rebase_gate_context(&invalid_rename).is_err());
}

#[test]
fn pending_contexts_accumulate_inputs_paths_and_latest_tree() {
    let first = context();
    let mut second_facts = facts();
    second_facts.old_head = NEXT.into();
    second_facts.upstream_tip =
        "5555555555555555555555555555555555555555".into();
    second_facts.merge_base = "6666666666666666666666666666666666666666".into();
    second_facts.current_head =
        "7777777777777777777777777777777777777777".into();
    second_facts.current_tree = NEXT_TREE.into();
    second_facts.local_changes =
        vec![change("repair.py", RebaseGateChangeKindWire::Modified)];
    second_facts.upstream_changes.clear();
    let second = build_rebase_gate_context(&second_facts).unwrap().unwrap();

    let merged = merge_rebase_gate_contexts(Some(&first), Some(&second))
        .unwrap()
        .unwrap();
    assert_eq!(merged.inputs.len(), 2);
    assert_eq!(merged.current_head, second.current_head);
    assert_eq!(merged.current_tree, second.current_tree);
    assert!(merged
        .changed_paths
        .iter()
        .any(|change| change.path == "repair.py"));
    assert!(merged
        .changed_paths
        .iter()
        .any(|change| change.path == "gone.py"));

    let update = RebaseGateContextUpdateWire {
        schema_version: REBASE_GATE_WIRE_SCHEMA_VERSION,
        repository_root: ROOT.into(),
        current_head: "8888888888888888888888888888888888888888".into(),
        current_tree: "cccccccccccccccccccccccccccccccccccccccc".into(),
        changed_paths: vec![change(
            "amend.py",
            RebaseGateChangeKindWire::Added,
        )],
    };
    let updated = update_rebase_gate_context(&merged, &update).unwrap();
    assert_eq!(updated.inputs, merged.inputs);
    assert_eq!(updated.current_head, update.current_head);
    assert!(updated
        .changed_paths
        .iter()
        .any(|change| change.path == "amend.py"));
}

#[test]
fn context_merge_rejects_different_repositories() {
    let first = context();
    let mut second = first.clone();
    second.repository_root = "/other/repository".into();
    assert!(merge_rebase_gate_contexts(Some(&first), Some(&second)).is_err());
}

#[test]
fn classification_passes_only_matching_clean_reports() {
    let decision = classify_rebase_gate_result(&classify_request(
        Some(0),
        false,
        Some(clean_report()),
    ));
    assert_eq!(decision.verdict, RebaseGateVerdictWire::Pass);

    let mut stale = clean_report();
    stale["checked_tree"] = json!(NEXT_TREE);
    let decision = classify_rebase_gate_result(&classify_request(
        Some(0),
        false,
        Some(stale),
    ));
    assert_eq!(decision.verdict, RebaseGateVerdictWire::Warn);
}

#[test]
fn classification_blocks_only_exit_one_with_concrete_matching_findings() {
    let decision = classify_rebase_gate_result(&classify_request(
        Some(1),
        false,
        Some(finding_report()),
    ));
    assert_eq!(decision.verdict, RebaseGateVerdictWire::Block);
    assert_eq!(decision.findings.len(), 1);

    let crash_without_report =
        classify_rebase_gate_result(&classify_request(Some(1), false, None));
    assert_eq!(crash_without_report.verdict, RebaseGateVerdictWire::Warn);

    let mut invalid_finding = finding_report();
    invalid_finding["findings"][0]["path"] = json!("unrelated.py");
    let decision = classify_rebase_gate_result(&classify_request(
        Some(1),
        false,
        Some(invalid_finding),
    ));
    assert_eq!(decision.verdict, RebaseGateVerdictWire::Warn);

    let mismatch = classify_rebase_gate_result(&classify_request(
        Some(0),
        false,
        Some(finding_report()),
    ));
    assert_eq!(mismatch.verdict, RebaseGateVerdictWire::Warn);
}

#[test]
fn classification_fails_open_for_protocol_and_execution_errors() {
    let mut disabled = classify_request(Some(0), false, Some(clean_report()));
    disabled.config.hook_command.clear();
    assert_eq!(
        classify_rebase_gate_result(&disabled).verdict,
        RebaseGateVerdictWire::Skip
    );

    let mut invalid_timeout =
        classify_request(Some(0), false, Some(clean_report()));
    invalid_timeout.config.timeout_seconds = 30.1;
    assert_eq!(
        classify_rebase_gate_result(&invalid_timeout).verdict,
        RebaseGateVerdictWire::Warn
    );

    let timed_out = classify_rebase_gate_result(&classify_request(
        None,
        true,
        Some(clean_report()),
    ));
    assert_eq!(timed_out.verdict, RebaseGateVerdictWire::Warn);

    let other_exit = classify_rebase_gate_result(&classify_request(
        Some(2),
        false,
        Some(clean_report()),
    ));
    assert_eq!(other_exit.verdict, RebaseGateVerdictWire::Warn);

    let mut error_report = clean_report();
    error_report["status"] = json!("error");
    error_report["diagnostics"] = json!("dependency import failed");
    let decision = classify_rebase_gate_result(&classify_request(
        Some(1),
        false,
        Some(error_report),
    ));
    assert_eq!(decision.verdict, RebaseGateVerdictWire::Warn);
}

#[test]
fn timeout_preflight_uses_finite_positive_maximum_policy() {
    assert_eq!(validate_rebase_gate_timeout_seconds(0.001).unwrap(), 0.001);
    assert_eq!(validate_rebase_gate_timeout_seconds(30.0).unwrap(), 30.0);
    for invalid in [
        0.0,
        -1.0,
        30.001,
        f64::INFINITY,
        f64::NEG_INFINITY,
        f64::NAN,
    ] {
        assert!(validate_rebase_gate_timeout_seconds(invalid).is_err());
    }
}

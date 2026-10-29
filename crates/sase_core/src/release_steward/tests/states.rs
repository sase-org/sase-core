use crate::release_steward::facts::{
    CheckConclusionWire, CheckFactsWire, CoreCutRunWire, CoreEnsureFactsWire,
    FactRead, RunStatusWire,
};
use crate::release_steward::release_steward_decide;
use crate::release_steward::wire::{
    ActionDispositionWire, PrimaryStateWire, TrainActionKindWire,
};

use super::fixtures::{
    active_train, green_gate_run, publishable_eval, releasable_request, HEAD,
    PIN,
};

fn decide_primary(
    request: crate::release_steward::wire::ReleaseStewardRequestWire,
) -> PrimaryStateWire {
    release_steward_decide(&request).unwrap().primary
}

fn core_cut(
    run_id: &str,
    status: RunStatusWire,
    conclusion: Option<CheckConclusionWire>,
    typed_failure: Option<&str>,
    diagnostic_available: bool,
) -> CoreCutRunWire {
    CoreCutRunWire {
        run_id: run_id.to_string(),
        attempt: 1,
        source_pin: PIN.to_string(),
        status,
        conclusion,
        typed_failure: typed_failure.map(str::to_string),
        diagnostic_available,
        event: None,
        started_at: None,
    }
}

/// A steward-dispatched cut at another pin: only the daily cap can
/// suppress a CORE_LAG dispatch when this cut is present.
fn dispatch_cut(
    run_id: &str,
    status: RunStatusWire,
    conclusion: Option<CheckConclusionWire>,
    started_at: &str,
) -> CoreCutRunWire {
    CoreCutRunWire {
        run_id: run_id.to_string(),
        attempt: 1,
        source_pin: "dddddddddddddddddddddddddddddddddddddddd".to_string(),
        status,
        conclusion,
        typed_failure: None,
        diagnostic_available: true,
        event: Some("workflow_dispatch".to_string()),
        started_at: Some(started_at.to_string()),
    }
}

fn lagging_request(
    cuts: Vec<CoreCutRunWire>,
    merge_authority: bool,
) -> crate::release_steward::wire::ReleaseStewardRequestWire {
    let mut request = releasable_request();
    request.policy.merge_authority_enabled = merge_authority;
    request.snapshot.core_ensure =
        FactRead::ready(CoreEnsureFactsWire::CoreLag {
            pin: PIN.to_string(),
            newest_complete: None,
        });
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        value.cuts = cuts;
    }
    request
}

fn with_cuts(
    cuts: Vec<CoreCutRunWire>,
) -> crate::release_steward::wire::ReleaseStewardRequestWire {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        value.cuts = cuts;
    }
    request
}

#[test]
fn releasable_proposes_merge() {
    let result = release_steward_decide(&releasable_request()).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::Releasable { pr_number: 42, .. }
    ));
    let action = result.train_action.expect("merge");
    assert!(matches!(
        action.action,
        TrainActionKindWire::MergeReleasePr { pr_number: 42, .. }
    ));
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    assert_eq!(action.argv[0], "gh");
    assert!(action.argv.contains(&"--match-head-commit".to_string()));
    assert_eq!(result.process_exit, 0);
}

#[test]
fn observe_only_never_marks_executed() {
    let result = release_steward_decide(&releasable_request()).unwrap();
    let action = result.train_action.unwrap();
    assert_ne!(action.disposition, ActionDispositionWire::Executed);
}

#[test]
fn merge_authority_off_keeps_releasable_and_suppresses() {
    let mut request = releasable_request();
    request.policy.merge_authority_enabled = false;
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::Releasable { .. }
    ));
    let action = result.train_action.unwrap();
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(action.reason.contains("merge authority"));
}

#[test]
fn skipped_required_check_blocks_merge() {
    let mut request = releasable_request();
    let pr = request
        .snapshot
        .release_pr
        .as_ready()
        .unwrap()
        .as_ref()
        .unwrap();
    let mut pr = pr.clone();
    pr.checks[0].conclusion = CheckConclusionWire::Skipped;
    request.snapshot.release_pr = FactRead::ready(Some(pr));
    match decide_primary(request) {
        PrimaryStateWire::Waiting { reason } => {
            assert!(reason.contains("tests"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn missing_required_check_blocks_merge() {
    let mut request = releasable_request();
    let mut pr = request
        .snapshot
        .release_pr
        .as_ready()
        .unwrap()
        .clone()
        .unwrap();
    pr.checks.clear();
    request.snapshot.release_pr = FactRead::ready(Some(pr));
    match decide_primary(request) {
        PrimaryStateWire::Waiting { reason } => {
            assert!(reason.contains("missing"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn unreadable_required_fact_is_watcher_broken() {
    let mut request = releasable_request();
    request.snapshot.pypi = FactRead::unreadable(
        "pypi.sase",
        "503 from pypi.org",
        "retry the PyPI JSON API",
    );
    let result = release_steward_decide(&request).unwrap();
    match result.primary {
        PrimaryStateWire::WatcherBroken { source, remedy } => {
            assert_eq!(source, "pypi.sase");
            assert!(remedy.contains("PyPI"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(result.process_exit, 1);
    assert!(result.train_action.is_none());
    assert!(result.escalation.is_none());
}

#[test]
fn rejected_eligible_cadence_is_invalid() {
    let mut request = releasable_request();
    request.policy.cadence.kind = "eligible".to_string();
    assert!(release_steward_decide(&request).is_err());
}

#[test]
fn publish_conflict_outranks_partial() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    train.py_digest_conflict = true;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    match decide_primary(request) {
        PrimaryStateWire::PublishConflict { candidate_id, .. } => {
            assert_eq!(candidate_id, train.candidate_id);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn dependency_unpublished_blocks_publication() {
    let mut request = releasable_request();
    let train = active_train();
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.clauses[1].ok = false;
    eval.clauses[1].detail = "core yanked".to_string();
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    match decide_primary(request) {
        PrimaryStateWire::DependencyUnpublished { package, .. } => {
            assert_eq!(package, "sase-core-rs");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn veto_from_hold_label() {
    let mut request = releasable_request();
    let mut pr = request
        .snapshot
        .release_pr
        .as_ready()
        .unwrap()
        .clone()
        .unwrap();
    pr.labels.push("release-hold".to_string());
    request.snapshot.release_pr = FactRead::ready(Some(pr));
    match decide_primary(request) {
        PrimaryStateWire::Vetoed { reason } => {
            assert!(reason.contains("release-hold"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn publish_partial_proposes_resume() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::PublishPartial { generation: 1, .. }
    ));
    match result.train_action.unwrap().action {
        TrainActionKindWire::DispatchRelease { mode, .. } => {
            assert_eq!(mode, "resume");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn in_flight_release_run_suppresses_resume() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    let mut run = green_gate_run(&train.sha);
    run.workflow = "release.yml".to_string();
    run.ref_name = train.tag.clone();
    run.status = RunStatusWire::InProgress;
    run.conclusion = None;
    request.snapshot.workflows.as_ready();
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value.release_yml.push(run);
    }
    let action = release_steward_decide(&request)
        .unwrap()
        .train_action
        .unwrap();
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
}

#[test]
fn confirmed_release_is_released() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.confirmed = true;
    train.confirmation_complete = true;
    train.py_files_present = 2;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.release_pr = FactRead::ready(None);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    match decide_primary(request) {
        PrimaryStateWire::Released { version } => {
            assert_eq!(version, "0.19.0");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn qualify_red_reruns_when_eligible() {
    let mut request = releasable_request();
    let train = active_train();
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.rerun_eligible = true;
    eval.qualify_red = Some("n1-inventory".to_string());
    eval.rerun_reason = "qualification red".to_string();
    let mut run = green_gate_run(&train.sha);
    run.workflow = "release.yml".to_string();
    run.ref_name = train.tag.clone();
    run.conclusion = Some(CheckConclusionWire::Failure);
    run.jobs[0].conclusion = Some(CheckConclusionWire::Failure);
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value.release_yml.push(run);
    }
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::QualifyRed {
            recovered: false,
            ..
        }
    ));
    assert!(matches!(
        result.train_action.unwrap().action,
        TrainActionKindWire::RerunFailedJobs { .. }
    ));
}

#[test]
fn spent_retry_abandons_unpublished_candidate() {
    let mut request = releasable_request();
    let train = active_train();
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.rerun_eligible = false;
    eval.qualify_red = Some("n1-inventory".to_string());
    eval.rerun_reason = "attempts exhausted".to_string();
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    let action = release_steward_decide(&request)
        .unwrap()
        .train_action
        .unwrap();
    assert!(matches!(
        action.action,
        TrainActionKindWire::AbandonCandidate { generation: 1, .. }
    ));
}

#[test]
fn public_files_refuse_abandonment() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.rerun_eligible = false;
    eval.qualify_red = Some("n1-inventory".to_string());
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::QualifyRed { .. }
    ));
    assert!(result.train_action.is_none());
}

#[test]
fn previous_generation_does_not_qualify_rebuild() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.generation = 2;
    let eval = publishable_eval(&train.candidate_id, 1);
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    match decide_primary(request) {
        PrimaryStateWire::WatcherBroken { source, .. } => {
            assert_eq!(source, "candidate");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn legacy_pre_train_tag_is_not_an_active_train() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.pre_train_legacy = true;
    train.confirmed = false;
    request.snapshot.trains = FactRead::ready(vec![train]);
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn ambiguous_trains_prevent_merge() {
    let mut request = releasable_request();
    let mut other = active_train();
    other.candidate_id = "sase-0.20.0-gen1".to_string();
    other.version = "0.20.0".to_string();
    other.tag = "v0.20.0".to_string();
    request.snapshot.trains = FactRead::ready(vec![active_train(), other]);
    match decide_primary(request) {
        PrimaryStateWire::AmbiguousTrains { detail } => {
            assert!(detail.contains("0.19.0"));
            assert!(detail.contains("0.20.0"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn newest_generation_wins_within_one_train() {
    let mut request = releasable_request();
    let gen1 = active_train();
    let mut gen2 = active_train();
    gen2.generation = 2;
    gen2.py_files_present = 1;
    request.snapshot.trains = FactRead::ready(vec![gen1, gen2.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&gen2.candidate_id, 2)));
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::PublishPartial { generation: 2, .. }
    ));
}

#[test]
fn tag_mismatch_fails_closed() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.manifest_mismatch = true;
    request.snapshot.trains = FactRead::ready(vec![train]);
    match decide_primary(request) {
        PrimaryStateWire::WatcherBroken { source, .. } => {
            assert_eq!(source, "trains");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn core_lag_proposes_one_cut() {
    let mut request = releasable_request();
    request.snapshot.core_ensure =
        FactRead::ready(CoreEnsureFactsWire::CoreLag {
            pin: PIN.to_string(),
            newest_complete: Some("0.49.0".to_string()),
        });
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::CoreLag { .. }));
    match result.train_action.unwrap().action {
        TrainActionKindWire::DispatchCoreCut { source_pin } => {
            assert_eq!(source_pin, PIN);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn in_flight_core_cut_suppresses_another() {
    let mut request = releasable_request();
    request.snapshot.core_ensure =
        FactRead::ready(CoreEnsureFactsWire::CoreLag {
            pin: PIN.to_string(),
            newest_complete: None,
        });
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        value.cuts.push(CoreCutRunWire {
            run_id: "cut-1".to_string(),
            attempt: 1,
            source_pin: PIN.to_string(),
            status: RunStatusWire::InProgress,
            conclusion: None,
            typed_failure: None,
            diagnostic_available: true,
            event: None,
            started_at: None,
        });
    }
    let action = release_steward_decide(&request)
        .unwrap()
        .train_action
        .unwrap();
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
}

#[test]
fn core_lag_without_merge_authority_suppresses_cut() {
    let result =
        release_steward_decide(&lagging_request(Vec::new(), false)).unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::CoreLag { .. }));
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(
        action.reason.contains("merge authority"),
        "{}",
        action.reason
    );
    assert!(
        action.reason.contains("cut_budget=daily"),
        "{}",
        action.reason
    );
}

#[test]
fn core_lag_without_merge_authority_suppresses_even_completed_cut() {
    let result = release_steward_decide(&lagging_request(
        vec![core_cut(
            "cut-done",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            None,
            true,
        )],
        false,
    ))
    .unwrap();
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(
        action.reason.contains("merge authority"),
        "{}",
        action.reason
    );
}

#[test]
fn core_lag_daily_cap_suppresses_second_dispatch() {
    // A completed steward dispatch earlier the same UTC day blocks another,
    // even at a pin no cut has covered yet.
    let result = release_steward_decide(&lagging_request(
        vec![dispatch_cut(
            "cut-dispatched",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            "2026-10-19T08:00:00Z",
        )],
        true,
    ))
    .unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::CoreLag { .. }));
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(action.reason.contains("UTC day"), "{}", action.reason);
}

#[test]
fn core_lag_in_flight_dispatch_for_new_pin_hits_daily_cap() {
    let result = release_steward_decide(&lagging_request(
        vec![dispatch_cut(
            "cut-live",
            RunStatusWire::InProgress,
            None,
            "2026-10-19T08:00:00Z",
        )],
        true,
    ))
    .unwrap();
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(action.reason.contains("UTC day"), "{}", action.reason);
}

#[test]
fn core_lag_yesterday_dispatch_does_not_block() {
    let result = release_steward_decide(&lagging_request(
        vec![dispatch_cut(
            "cut-yesterday",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            "2026-10-18T12:00:00Z",
        )],
        true,
    ))
    .unwrap();
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
}

#[test]
fn core_lag_schedule_cut_does_not_count_toward_daily_cap() {
    // The daily schedule cut is not a steward dispatch.
    let mut cut = dispatch_cut(
        "cut-schedule",
        RunStatusWire::Completed,
        Some(CheckConclusionWire::Success),
        "2026-10-19T08:00:00Z",
    );
    cut.event = Some("schedule".to_string());
    let result =
        release_steward_decide(&lagging_request(vec![cut], true)).unwrap();
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
}

#[test]
fn core_lag_dispatch_cap_uses_utc_day() {
    // 00:30 +02:00 is still yesterday in UTC, so no same-day dispatch.
    let result = release_steward_decide(&lagging_request(
        vec![dispatch_cut(
            "cut-tz",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            "2026-10-19T00:30:00+02:00",
        )],
        true,
    ))
    .unwrap();
    let action = result.train_action.expect("cut action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
}

#[test]
fn core_cut_red_does_not_rerun() {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        value.cuts.push(CoreCutRunWire {
            run_id: "cut-1".to_string(),
            attempt: 1,
            source_pin: PIN.to_string(),
            status: RunStatusWire::Completed,
            conclusion: Some(CheckConclusionWire::Failure),
            typed_failure: Some("CHECKS_NEVER_REGISTERED".to_string()),
            diagnostic_available: true,
            event: None,
            started_at: None,
        });
    }
    let result = release_steward_decide(&request).unwrap();
    match result.primary {
        PrimaryStateWire::CoreCutRed { check } => {
            assert_eq!(check, "CHECKS_NEVER_REGISTERED");
        }
        other => panic!("{other:?}"),
    }
    assert!(result.train_action.is_none());
}

#[test]
fn missing_cut_diagnostic_fails_closed() {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        value.cuts.push(CoreCutRunWire {
            run_id: "cut-1".to_string(),
            attempt: 1,
            source_pin: PIN.to_string(),
            status: RunStatusWire::Completed,
            conclusion: Some(CheckConclusionWire::Failure),
            typed_failure: Some("CORE_CUT_RED check=tests".to_string()),
            diagnostic_available: false,
            event: None,
            started_at: None,
        });
    }
    match decide_primary(request) {
        PrimaryStateWire::WatcherBroken { source, .. } => {
            assert_eq!(source, "core_cut");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn newer_successful_cut_supersedes_stale_typed_red() {
    // 2026-10-24 shape: typed-red cut 40131163210, then newer green cuts.
    let request = with_cuts(vec![
        core_cut(
            "40140000000",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            None,
            true,
        ),
        core_cut(
            "40131163210",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Failure),
            Some("CORE_CUT_RED check=tests"),
            true,
        ),
    ]);
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn newest_completed_typed_failure_is_core_cut_red() {
    let request = with_cuts(vec![
        core_cut(
            "cut-new",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Failure),
            Some("CHECKS_NEVER_REGISTERED"),
            true,
        ),
        core_cut(
            "cut-old-green",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            None,
            true,
        ),
    ]);
    match decide_primary(request) {
        PrimaryStateWire::CoreCutRed { check } => {
            assert_eq!(check, "CHECKS_NEVER_REGISTERED");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn cancelled_and_in_flight_cuts_do_not_clear_typed_red() {
    let request = with_cuts(vec![
        core_cut("cut-live", RunStatusWire::InProgress, None, None, true),
        core_cut(
            "cut-cancelled",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Cancelled),
            None,
            true,
        ),
        core_cut(
            "cut-skipped",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Skipped),
            None,
            true,
        ),
        core_cut(
            "cut-red",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Failure),
            Some("CORE_CUT_RED check=tests"),
            true,
        ),
    ]);
    match decide_primary(request) {
        PrimaryStateWire::CoreCutRed { check } => {
            assert_eq!(check, "CORE_CUT_RED check=tests");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn superseded_red_without_diagnostics_is_not_broken() {
    let request = with_cuts(vec![
        core_cut(
            "cut-green",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Success),
            None,
            true,
        ),
        core_cut(
            "cut-stale-red",
            RunStatusWire::Completed,
            Some(CheckConclusionWire::Failure),
            Some("CORE_CUT_RED check=tests"),
            false,
        ),
    ]);
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn core_ci_red_outranks_lag() {
    let mut request = releasable_request();
    request.snapshot.core_ensure =
        FactRead::ready(CoreEnsureFactsWire::CoreLag {
            pin: PIN.to_string(),
            newest_complete: None,
        });
    if let FactRead::Ready { value } = &mut request.snapshot.core {
        let mut ci = green_gate_run(PIN);
        ci.conclusion = Some(CheckConclusionWire::Failure);
        value.master_ci = Some(ci);
    }
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::CoreCiRed { .. }
    ));
}

#[test]
fn gate_red_reruns_failed_tests_not_lint() {
    let mut request = releasable_request();
    let mut run = green_gate_run(HEAD);
    run.conclusion = Some(CheckConclusionWire::Failure);
    run.jobs[0].conclusion = Some(CheckConclusionWire::Failure);
    run.jobs[1].conclusion = Some(CheckConclusionWire::Failure);
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.gate_run = Some(run);
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::GateRed { .. }));
    match result.train_action.unwrap().action {
        TrainActionKindWire::RerunFailedJobs { job_ids, .. } => {
            assert_eq!(job_ids, vec!["job-test".to_string()]);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn lint_only_gate_failure_does_not_rerun() {
    let mut request = releasable_request();
    let mut run = green_gate_run(HEAD);
    run.conclusion = Some(CheckConclusionWire::Failure);
    run.jobs[0].conclusion = Some(CheckConclusionWire::Success);
    run.jobs[1].conclusion = Some(CheckConclusionWire::Failure);
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.gate_run = Some(run);
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::GateRed { .. }));
    assert!(result.train_action.is_none());
}

#[test]
fn cancelled_gate_jobs_are_not_rerun() {
    let mut request = releasable_request();
    let mut run = green_gate_run(HEAD);
    run.conclusion = Some(CheckConclusionWire::Cancelled);
    run.jobs[0].conclusion = Some(CheckConclusionWire::Cancelled);
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.gate_run = Some(run);
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(result.train_action.is_none());
}

#[test]
fn install_red_does_not_block_publishable_resume() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    if let FactRead::Ready { value } = &mut request.snapshot.canary {
        value.linux.as_mut().unwrap().verdict = "fail".to_string();
        value.linux.as_mut().unwrap().reason = Some("pip check".to_string());
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::PublishPartial { .. }
    ));
    assert!(result.secondary.iter().any(|finding| {
        matches!(finding.state, PrimaryStateWire::InstallBroken { .. })
    }));
    assert!(matches!(
        result.train_action.unwrap().action,
        TrainActionKindWire::DispatchRelease { .. }
    ));
}

#[test]
fn install_red_is_primary_when_nothing_else_applies() {
    let mut request = releasable_request();
    request.snapshot.release_pr = FactRead::ready(None);
    if let FactRead::Ready { value } = &mut request.snapshot.canary {
        value.macos = None;
    }
    match decide_primary(request) {
        PrimaryStateWire::InstallBroken { member } => {
            assert_eq!(member, "macos");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn known_red_canary_is_not_watcher_broken() {
    let mut request = releasable_request();
    request.snapshot.release_pr = FactRead::ready(None);
    if let FactRead::Ready { value } = &mut request.snapshot.canary {
        value.linux.as_mut().unwrap().verdict = "missing_artifact".to_string();
    }
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::InstallBroken { .. }
    ));
}

#[test]
fn initial_tag_proposes_release_mode() {
    let mut request = releasable_request();
    let train = active_train();
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    match release_steward_decide(&request)
        .unwrap()
        .train_action
        .unwrap()
        .action
    {
        TrainActionKindWire::DispatchRelease { mode, .. } => {
            assert_eq!(mode, "release");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn nothing_to_release_without_pr() {
    let mut request = releasable_request();
    request.snapshot.release_pr = FactRead::ready(None);
    match decide_primary(request) {
        PrimaryStateWire::NothingToRelease { .. } => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn unreadable_expected_checks_cannot_merge() {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.expected_checks_readable = false;
    }
    match decide_primary(request) {
        PrimaryStateWire::Waiting { reason } => {
            assert!(reason.contains("unreadable"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn empty_collected_checks_are_not_green() {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.expected_required_checks.clear();
    }
    match decide_primary(request) {
        PrimaryStateWire::Waiting { reason } => {
            assert!(reason.contains("empty") || reason.contains("unknown"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn core_incompatible_proposes_one_metadata_refresh() {
    let mut request = releasable_request();
    let mut pr = request
        .snapshot
        .release_pr
        .as_ready()
        .unwrap()
        .clone()
        .unwrap();
    pr.core_smoke = Some(CheckFactsWire {
        name: "release-core-floor-smoke".to_string(),
        conclusion: CheckConclusionWire::Failure,
        is_test: true,
        is_lint: false,
        url: None,
    });
    request.snapshot.release_pr = FactRead::ready(Some(pr));
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::CoreIncompatible { .. }
    ));
    assert!(matches!(
        result.train_action.unwrap().action,
        TrainActionKindWire::DispatchPublishMetadata { .. }
    ));
}

#[test]
fn exercise_red_is_a_typed_state() {
    let mut request = releasable_request();
    let train = active_train();
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.exercise_red = Some("macos".to_string());
    eval.rerun_eligible = false;
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    assert!(matches!(
        decide_primary(request),
        PrimaryStateWire::ExerciseRed { platform, .. } if platform == "macos"
    ));
}

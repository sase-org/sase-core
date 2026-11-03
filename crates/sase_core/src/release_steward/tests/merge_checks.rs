use super::fixtures::{releasable_request, success_check};
use crate::release_steward::facts::{
    CheckConclusionWire, FactRead, RequiredCheckWire,
};
use crate::release_steward::{release_steward_decide, PrimaryStateWire};

#[test]
fn release_pr_inventory_is_independent_of_master_gate_jobs() {
    let mut request = releasable_request();
    let names = [
        "Conventional PR title",
        "prepare",
        "release-core-floor-smoke",
    ];
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.expected_required_checks = names
            .iter()
            .map(|name| RequiredCheckWire {
                name: name.to_string(),
                source: "release-qualification inventory".to_string(),
            })
            .collect();
    }
    if let FactRead::Ready { value: Some(pr) } =
        &mut request.snapshot.release_pr
    {
        pr.checks = names.iter().map(|name| success_check(name)).collect();
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(
        result.primary,
        PrimaryStateWire::Releasable { .. }
    ));
    assert!(result.train_action.is_some());
}

#[test]
fn every_master_gate_job_must_succeed_even_when_run_is_green() {
    for conclusion in [
        Some(CheckConclusionWire::Skipped),
        Some(CheckConclusionWire::Failure),
        Some(CheckConclusionWire::Cancelled),
        Some(CheckConclusionWire::Missing),
        None,
    ] {
        let mut request = releasable_request();
        if let FactRead::Ready { value } = &mut request.snapshot.master {
            let run = value.gate_run.as_mut().unwrap();
            // An extra job is also required; it need not be in the PR list.
            let mut extra = run.jobs[0].clone();
            extra.name = "core-wheel".to_string();
            extra.conclusion = conclusion;
            run.jobs.push(extra);
        }
        let result = release_steward_decide(&request).unwrap();
        assert!(matches!(result.primary, PrimaryStateWire::Waiting { .. }));
        assert!(result.train_action.is_none());
        assert!(result.details.contains("core-wheel"));
    }
}

#[test]
fn master_gate_without_jobs_cannot_merge() {
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.master {
        value.gate_run.as_mut().unwrap().jobs.clear();
    }
    let result = release_steward_decide(&request).unwrap();
    assert!(matches!(result.primary, PrimaryStateWire::Waiting { .. }));
    assert!(result.train_action.is_none());
    assert!(result.details.contains("jobs are missing"));
}

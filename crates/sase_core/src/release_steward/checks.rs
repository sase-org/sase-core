//! Strict required-check evaluation. Only `success` is green.

use super::facts::{
    CheckConclusionWire, CheckFactsWire, JobFactsWire, MasterFactsWire,
    ReleasePrFactsWire, RequiredCheckWire, RunStatusWire, WorkflowRunFactsWire,
};

pub fn conclusion_is_green(conclusion: Option<CheckConclusionWire>) -> bool {
    matches!(conclusion, Some(CheckConclusionWire::Success))
}

pub fn required_checks_green(
    expected: &[RequiredCheckWire],
    expected_readable: bool,
    actual: &[CheckFactsWire],
) -> Result<(), String> {
    if !expected_readable {
        return Err("expected required checks unreadable".to_string());
    }
    if expected.is_empty() {
        return Err("expected required checks unknown or empty".to_string());
    }
    for required in expected {
        let found = actual.iter().find(|check| check.name == required.name);
        match found {
            Some(check) if check.conclusion.is_success() => {}
            Some(check) => {
                return Err(format!(
                    "required check {} is {:?}",
                    required.name, check.conclusion
                ));
            }
            None => {
                return Err(format!(
                    "required check {} is missing",
                    required.name
                ));
            }
        }
    }
    Ok(())
}

pub fn master_gate_green(master: &MasterFactsWire) -> Result<(), String> {
    let Some(run) = master.gate_run.as_ref() else {
        return Err("master gate run is absent".to_string());
    };
    if run.sha != master.sha {
        return Err(format!(
            "master gate run sha {} does not match HEAD {}",
            run.sha, master.sha
        ));
    }
    if run.status != RunStatusWire::Completed
        || !conclusion_is_green(run.conclusion)
    {
        return Err("master gate is not successful".to_string());
    }
    let checks: Vec<CheckFactsWire> = run
        .jobs
        .iter()
        .map(|job| CheckFactsWire {
            name: job.name.clone(),
            conclusion: job.conclusion.unwrap_or(CheckConclusionWire::Missing),
            is_test: job.is_test,
            is_lint: job.is_lint,
            url: None,
        })
        .collect();
    required_checks_green(
        &master.expected_required_checks,
        master.expected_checks_readable,
        &checks,
    )
}

pub fn pr_checks_green(
    pr: &ReleasePrFactsWire,
    master: &MasterFactsWire,
) -> Result<(), String> {
    required_checks_green(
        &master.expected_required_checks,
        master.expected_checks_readable,
        &pr.checks,
    )
}

/// Failed Master Gate *test* jobs eligible for one rerun at this HEAD.
pub fn rerunnable_gate_jobs(
    run: &WorkflowRunFactsWire,
) -> Result<Vec<JobFactsWire>, String> {
    if run.attempt >= 2 {
        return Err("master gate rerun attempt is exhausted".to_string());
    }
    let mut jobs = Vec::new();
    for job in &run.jobs {
        let conclusion = job.conclusion.unwrap_or(CheckConclusionWire::Missing);
        if conclusion.is_success() {
            continue;
        }
        if job.is_lint {
            continue;
        }
        if matches!(
            conclusion,
            CheckConclusionWire::Cancelled
                | CheckConclusionWire::Missing
                | CheckConclusionWire::Skipped
                | CheckConclusionWire::Neutral
                | CheckConclusionWire::Stale
        ) {
            continue;
        }
        if job.is_test {
            jobs.push(job.clone());
        }
    }
    if jobs.is_empty() {
        return Err("no rerunnable failed master gate test jobs".to_string());
    }
    Ok(jobs)
}

pub fn matching_release_run<'a>(
    runs: &'a [WorkflowRunFactsWire],
    tag: &str,
    sha: &str,
) -> Option<&'a WorkflowRunFactsWire> {
    runs.iter().find(|run| {
        (run.ref_name == tag || run.ref_name == format!("refs/tags/{tag}"))
            && run.sha == sha
    })
}

pub fn matching_publish_run<'a>(
    runs: &'a [WorkflowRunFactsWire],
    sha: &str,
) -> Option<&'a WorkflowRunFactsWire> {
    runs.iter().find(|run| run.sha == sha)
}

pub fn matching_core_cut<'a>(
    cuts: &'a [super::facts::CoreCutRunWire],
    pin: &str,
) -> Option<&'a super::facts::CoreCutRunWire> {
    cuts.iter().find(|cut| cut.source_pin == pin)
}

//! Pure `release_steward_decide` state machine.

use serde_json::Value as JsonValue;

use super::cadence::{
    cadence_block_reason, parse_observed_at, validate_cadence,
};
use super::checks::{
    master_gate_green, matching_core_cut, matching_publish_run,
    matching_release_run, pr_checks_green, rerunnable_gate_jobs,
};
use super::escalation::{decide_escalation, EscalationInput};
use super::facts::{
    CanaryFactsWire, CandidateEvalWire, CheckConclusionWire, CoreCutRunWire,
    CoreEnsureFactsWire, EvidenceRefWire, FactRead, ReleasePrFactsWire,
    ReleaseStewardSnapshotWire, RunStatusWire, TrainFactsWire,
    WorkflowFactsWire,
};
use super::keys::{observation_asset, proposed_action};
use super::train::{select_trains, version_has_pypi_files};
use super::wire::{
    ActionDispositionWire, PrimaryStateWire, ProposedActionWire,
    ReleaseStewardError, ReleaseStewardRequestWire, ReleaseStewardResultWire,
    SecondaryFindingWire, TrainActionKindWire,
    RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
};

struct Draft {
    primary: PrimaryStateWire,
    details: String,
    evidence: Vec<EvidenceRefWire>,
    train_action: Option<ProposedActionWire>,
    secondary: Vec<SecondaryFindingWire>,
}

impl Draft {
    fn new(primary: PrimaryStateWire, details: impl Into<String>) -> Self {
        Self {
            primary,
            details: details.into(),
            evidence: Vec::new(),
            train_action: None,
            secondary: Vec::new(),
        }
    }

    fn action(mut self, action: ProposedActionWire) -> Self {
        self.train_action = Some(action);
        self
    }

    fn evidence(
        mut self,
        kind: &str,
        identity: &str,
        url: Option<String>,
    ) -> Self {
        self.evidence.push(EvidenceRefWire {
            kind: kind.to_string(),
            identity: identity.to_string(),
            url,
        });
        self
    }
}

fn broken(source: &str, message: &str, remedy: &str) -> Draft {
    Draft::new(
        PrimaryStateWire::WatcherBroken {
            source: source.to_string(),
            remedy: remedy.to_string(),
        },
        message,
    )
}

fn required_fact<T>(fact: &FactRead<T>, name: &str) -> Option<Draft> {
    match fact {
        FactRead::Unreadable {
            source,
            message,
            remedy,
        } => Some(broken(
            source,
            &format!("unreadable required fact {name}: {message}"),
            remedy,
        )),
        FactRead::Absent { reason } => Some(broken(
            name,
            &format!("required fact {name} is absent: {reason}"),
            "re-read the fact; absence of a required collection cannot authorize an action",
        )),
        FactRead::Ready { .. } => None,
    }
}

fn inspect_required(snapshot: &ReleaseStewardSnapshotWire) -> Option<Draft> {
    let required: [(&str, Option<Draft>); 12] = [
        (
            "repository",
            required_fact(&snapshot.repository, "repository"),
        ),
        ("master", required_fact(&snapshot.master, "master")),
        ("tags", required_fact(&snapshot.tags, "tags")),
        ("releases", required_fact(&snapshot.releases, "releases")),
        ("pypi", required_fact(&snapshot.pypi, "pypi")),
        ("trains", required_fact(&snapshot.trains, "trains")),
        ("workflows", required_fact(&snapshot.workflows, "workflows")),
        ("core", required_fact(&snapshot.core, "core")),
        (
            "core_ensure",
            required_fact(&snapshot.core_ensure, "core_ensure"),
        ),
        ("issues", required_fact(&snapshot.issues, "issues")),
        (
            "core_actions_access",
            required_fact(&snapshot.core_actions_access, "core_actions_access"),
        ),
        (
            "merge_history",
            required_fact(&snapshot.merge_history, "merge_history"),
        ),
    ];
    for (_name, draft) in required {
        if draft.is_some() {
            return draft;
        }
    }
    if snapshot.repository.as_ready().is_none() {
        return Some(broken(
            "repository",
            "repository identity is absent",
            "re-read GitHub repository metadata",
        ));
    }
    if snapshot.master.as_ready().is_none() {
        return Some(broken(
            "master",
            "master identity is absent",
            "re-read master HEAD and Master Gate",
        ));
    }
    if let Some(access) = snapshot.core_actions_access.as_ready() {
        if !access.readable {
            return Some(broken(
                "core_actions_access",
                &access.detail,
                "grant the release token read access to sase-core Actions",
            ));
        }
    } else {
        return Some(broken(
            "core_actions_access",
            "core Actions access probe is absent",
            "run the read-only core Actions probe",
        ));
    }
    if let Some(ensure) = snapshot.core_ensure.as_ready() {
        if let CoreEnsureFactsWire::Error { message } = ensure {
            return Some(broken(
                "core_ensure",
                message,
                "re-read PyPI metadata and the source pin",
            ));
        }
    } else {
        return Some(broken(
            "core_ensure",
            "core-ensure result is absent",
            "invoke ensure_core on freshly read facts",
        ));
    }
    if let Some((source, message, remedy)) = snapshot.canary.unreadable_break()
    {
        return Some(broken(
            source,
            &format!("canary transport failed: {message}"),
            remedy,
        ));
    }
    if snapshot.release_pr.unreadable_break().is_some() {
        let (source, message, remedy) =
            snapshot.release_pr.unreadable_break().unwrap();
        return Some(broken(source, message, remedy));
    }
    None
}

fn canary_findings(
    canary: &FactRead<CanaryFactsWire>,
) -> Vec<SecondaryFindingWire> {
    let mut findings = Vec::new();
    match canary {
        FactRead::Absent { reason } => findings.push(SecondaryFindingWire {
            state: PrimaryStateWire::InstallBroken {
                member: "missing".to_string(),
            },
            detail: reason.clone(),
        }),
        FactRead::Ready { value } => {
            for (member, result) in [
                ("linux", value.linux.as_ref()),
                ("macos", value.macos.as_ref()),
            ] {
                match result {
                    None => findings.push(SecondaryFindingWire {
                        state: PrimaryStateWire::InstallBroken {
                            member: member.to_string(),
                        },
                        detail: format!(
                            "contracted {member} canary result is missing"
                        ),
                    }),
                    Some(result) if result.verdict != "pass" => {
                        findings.push(SecondaryFindingWire {
                            state: PrimaryStateWire::InstallBroken {
                                member: result.member.clone(),
                            },
                            detail: result
                                .reason
                                .clone()
                                .unwrap_or_else(|| result.verdict.clone()),
                        });
                    }
                    Some(_) => {}
                }
            }
        }
        FactRead::Unreadable { .. } => {}
    }
    findings
}

fn hold_label(pr: &ReleasePrFactsWire) -> bool {
    pr.labels.iter().any(|label| label == "release-hold")
}

fn suppress_if_in_flight(
    action: ProposedActionWire,
    in_flight: bool,
    reason: &str,
) -> ProposedActionWire {
    if in_flight {
        ProposedActionWire {
            disposition: ActionDispositionWire::Suppressed,
            reason: reason.to_string(),
            ..action
        }
    } else {
        action
    }
}

fn abandon_action(eval: &CandidateEvalWire) -> ProposedActionWire {
    let asset = observation_asset(&eval.candidate_id, eval.generation);
    proposed_action(
        TrainActionKindWire::AbandonCandidate {
            candidate_id: eval.candidate_id.clone(),
            generation: eval.generation,
            observation_asset: asset,
        },
        ActionDispositionWire::Proposed,
        vec!["zero PyPI files".to_string()],
        "burn unpublished unrecoverable candidate",
    )
}

fn red_train_action(
    eval: &CandidateEvalWire,
    train: &TrainFactsWire,
    workflows: &WorkflowFactsWire,
    qualify: bool,
) -> Draft {
    let signature = if qualify {
        eval.qualify_red
            .clone()
            .unwrap_or_else(|| "qualification".to_string())
    } else {
        eval.exercise_red
            .clone()
            .unwrap_or_else(|| "exercise".to_string())
    };
    let primary = if qualify {
        PrimaryStateWire::QualifyRed {
            signature: signature.clone(),
            recovered: false,
        }
    } else {
        PrimaryStateWire::ExerciseRed {
            platform: signature.clone(),
        }
    };
    let mut draft = Draft::new(primary, eval.rerun_reason.clone());
    let matching =
        matching_release_run(&workflows.release_yml, &train.tag, &train.sha);
    let attempt_ok = matching.map(|run| run.attempt < 2).unwrap_or(true);
    if eval.rerun_eligible && attempt_ok {
        if let Some(run) = matching {
            if run.status.is_in_flight() {
                return draft.action(suppress_if_in_flight(
                    proposed_action(
                        TrainActionKindWire::RerunFailedJobs {
                            run_id: run.run_id.clone(),
                            attempt: run.attempt,
                            job_ids: Vec::new(),
                        },
                        ActionDispositionWire::Proposed,
                        vec!["rerun_eligible".to_string()],
                        "release run already in flight",
                    ),
                    true,
                    "release run already in flight",
                ));
            }
            let job_ids: Vec<String> = run
                .jobs
                .iter()
                .filter(|job| {
                    !job.conclusion.is_some_and(CheckConclusionWire::is_success)
                })
                .map(|job| job.id.clone())
                .collect();
            return draft.action(proposed_action(
                TrainActionKindWire::RerunFailedJobs {
                    run_id: run.run_id.clone(),
                    attempt: run.attempt,
                    job_ids,
                },
                ActionDispositionWire::Proposed,
                vec![
                    "rerun_eligible".to_string(),
                    "attempt permits rerun".to_string(),
                ],
                "one failed-job rerun",
            ));
        }
    }
    let exhausted = eval.retained_asset_missing
        || eval.workflow_broken_at_tag
        || !eval.rerun_eligible
        || !attempt_ok;
    if exhausted && !version_has_pypi_files(train) {
        return draft.action(abandon_action(eval));
    }
    if version_has_pypi_files(train) {
        draft.details = format!(
            "{}; public files exist so the candidate is preserved",
            draft.details
        );
    }
    draft
}

fn handle_active_train(
    train: &TrainFactsWire,
    eval: &CandidateEvalWire,
    snapshot: &ReleaseStewardSnapshotWire,
    pr: Option<&ReleasePrFactsWire>,
) -> Draft {
    if eval.candidate_id != train.candidate_id
        || eval.generation != train.generation
    {
        return broken(
            "candidate",
            "candidate evaluator identity does not match the selected train generation",
            "re-evaluate the newest generation; a previous pass never qualifies a rebuild",
        );
    }
    let workflows = snapshot.workflows.as_ready().expect("workflows required");
    if train.py_digest_conflict || train.py_extra_files {
        return Draft::new(
            PrimaryStateWire::PublishConflict {
                candidate_id: train.candidate_id.clone(),
                detail: "PyPI digest or extra-file conflict".to_string(),
            },
            "publication conflict forbids train publication actions",
        )
        .evidence("candidate", &train.candidate_id, None);
    }
    if let Some(clause) = eval.clauses.iter().find(|clause| {
        clause.name == "public_dependency_contract_satisfied" && !clause.ok
    }) {
        return Draft::new(
            PrimaryStateWire::DependencyUnpublished {
                package: "sase-core-rs".to_string(),
                detail: clause.detail.clone(),
            },
            clause.detail.clone(),
        );
    }
    if eval.active_veto || pr.is_some_and(hold_label) {
        let reason = if eval.active_veto {
            "active candidate veto".to_string()
        } else {
            "release-hold label".to_string()
        };
        return Draft::new(
            PrimaryStateWire::Vetoed {
                reason: reason.clone(),
            },
            reason,
        );
    }
    if train.confirmed && train.confirmation_complete {
        return Draft::new(
            PrimaryStateWire::Released {
                version: train.version.clone(),
            },
            format!("confirmed public release {}", train.version),
        )
        .evidence("release", &train.tag, None);
    }
    if eval.qualify_red.is_some() && !eval.publishable {
        return red_train_action(eval, train, workflows, true);
    }
    if eval.exercise_red.is_some() && !eval.publishable {
        return red_train_action(eval, train, workflows, false);
    }
    if eval.publishable
        && train.py_files_present > 0
        && train.py_files_present < train.py_files_expected
    {
        let matching = matching_release_run(
            &workflows.release_yml,
            &train.tag,
            &train.sha,
        );
        let in_flight = matching.is_some_and(|run| run.status.is_in_flight());
        let action = proposed_action(
            TrainActionKindWire::DispatchRelease {
                tag: train.tag.clone(),
                sha: train.sha.clone(),
                mode: "resume".to_string(),
                generation: train.generation,
            },
            ActionDispositionWire::Proposed,
            vec![
                "publishable retained candidate".to_string(),
                "missing PyPI files".to_string(),
            ],
            "resume release.yml for partial publication",
        );
        return Draft::new(
            PrimaryStateWire::PublishPartial {
                version: train.version.clone(),
                candidate_id: train.candidate_id.clone(),
                generation: train.generation,
            },
            "matching publishable candidate is missing PyPI files",
        )
        .action(suppress_if_in_flight(
            action,
            in_flight,
            "release.yml already in flight",
        ));
    }
    if train.py_files_present > 0 && !train.confirmation_complete {
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: "public files await release-workflow confirmation"
                    .to_string(),
            },
            "public files without confirmation remain release.yml work",
        );
    }
    if eval.publishable && train.py_files_present == 0 {
        let matching = matching_release_run(
            &workflows.release_yml,
            &train.tag,
            &train.sha,
        );
        let in_flight = matching.is_some_and(|run| run.status.is_in_flight());
        let action = proposed_action(
            TrainActionKindWire::DispatchRelease {
                tag: train.tag.clone(),
                sha: train.sha.clone(),
                mode: "release".to_string(),
                generation: train.generation,
            },
            ActionDispositionWire::Proposed,
            vec!["tag/draft ready".to_string(), "no PyPI files".to_string()],
            "initial release.yml run at the tag",
        );
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: "tag ready for initial release.yml run".to_string(),
            },
            "dispatch release.yml --ref <tag> -f mode=release",
        )
        .action(suppress_if_in_flight(
            action,
            in_flight,
            "release.yml already in flight",
        ));
    }
    Draft::new(
        PrimaryStateWire::Waiting {
            reason: "active train is not yet publishable".to_string(),
        },
        "selected train has no publication action this tick",
    )
}

/// UTC calendar day (`YYYY-MM-DD`) of an RFC 3339 timestamp, or `None`
/// when the timestamp does not parse. Readers validate the shape, so
/// `None` only fires on wires from older readers.
fn utc_day(timestamp: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(timestamp)
        .ok()
        .map(|moment| {
            moment
                .with_timezone(&chrono::Utc)
                .format("%Y-%m-%d")
                .to_string()
        })
}

/// A steward-dispatched cut (`workflow_dispatch`) already started this
/// UTC day counts against the daily cut budget, whatever its pin or
/// outcome. Schedule cuts do not: the daily `41 7 * * *` cut is not a
/// steward dispatch. Days derive from run timestamps, never from memory.
fn steward_dispatch_started_on(cuts: &[CoreCutRunWire], day: &str) -> bool {
    cuts.iter().any(|cut| {
        cut.event.as_deref() == Some("workflow_dispatch")
            && cut.started_at.as_deref().and_then(utc_day).as_deref()
                == Some(day)
    })
}

/// Walk `core.cuts` newest first. In-flight cuts never clear a red.
/// Cancelled, skipped, and other non-decisive completed cuts are skipped.
/// The first success means no `CoreCutRed`. The first typed failure is the
/// counted red.
fn counted_typed_cut_failure(
    cuts: &[CoreCutRunWire],
) -> Option<&CoreCutRunWire> {
    for cut in cuts {
        if cut.status != RunStatusWire::Completed {
            continue;
        }
        if cut.conclusion.is_some_and(CheckConclusionWire::is_success) {
            return None;
        }
        if cut.typed_failure.is_some() {
            return Some(cut);
        }
    }
    None
}

fn handle_no_train(
    request: &ReleaseStewardRequestWire,
    snapshot: &ReleaseStewardSnapshotWire,
    pr: Option<&ReleasePrFactsWire>,
) -> Draft {
    let core = snapshot.core.as_ready().expect("core required");
    let workflows = snapshot.workflows.as_ready().expect("workflows required");
    let master = snapshot.master.as_ready().expect("master required");
    let ensure = snapshot
        .core_ensure
        .as_ready()
        .expect("core_ensure required");

    if let Some(ci) = core.master_ci.as_ref() {
        if ci.status == RunStatusWire::Completed
            && !ci.conclusion.is_some_and(CheckConclusionWire::is_success)
        {
            return Draft::new(
                PrimaryStateWire::CoreCiRed {
                    detail: format!("sase-core master CI {}", ci.run_id),
                },
                "sase-core master CI is red",
            )
            .evidence("core-ci", &ci.run_id, Some(ci.url.clone()));
        }
    }
    if let Some(cut) = counted_typed_cut_failure(&core.cuts) {
        if !cut.diagnostic_available {
            return broken(
                "core_cut",
                "failed core-cut is missing required diagnostic evidence",
                "read the CORE_CUT_RED annotation or retained log line",
            );
        }
        let check = cut
            .typed_failure
            .clone()
            .unwrap_or_else(|| "unknown".to_string());
        return Draft::new(
            PrimaryStateWire::CoreCutRed { check: check.clone() },
            format!("core-cut failed ({check}); steward does not multiply its retry"),
        );
    }

    if let CoreEnsureFactsWire::CoreLag { pin, .. } = ensure {
        let matching = matching_core_cut(&core.cuts, pin);
        let in_flight = matching.is_some_and(|cut| cut.status.is_in_flight());
        let already = matching.is_some_and(|cut| {
            cut.status == RunStatusWire::Completed
                && cut.conclusion.is_some_and(CheckConclusionWire::is_success)
        });
        let action = proposed_action(
            TrainActionKindWire::DispatchCoreCut {
                source_pin: pin.clone(),
            },
            ActionDispositionWire::Proposed,
            vec!["no complete published core contains the pin".to_string()],
            "dispatch one core cut",
        );
        // cut_budget = daily: the steward cuts only with merge authority
        // on, at most one dispatch per UTC day.
        let observed_day = utc_day(&request.observed_at);
        let dispatched_today = observed_day
            .as_deref()
            .is_some_and(|day| steward_dispatch_started_on(&core.cuts, day));
        let (disposition, reason) = if !request.policy.merge_authority_enabled {
            (
                ActionDispositionWire::Suppressed,
                "merge authority disabled; steward cuts only with merge authority on (cut_budget=daily)",
            )
        } else if dispatched_today {
            (
                ActionDispositionWire::Suppressed,
                "steward core-cut dispatch already started this UTC day (cut_budget=daily: at most one dispatch per UTC day)",
            )
        } else if already {
            (
                ActionDispositionWire::AlreadySatisfied,
                "applicable core cut already completed",
            )
        } else if in_flight {
            (
                ActionDispositionWire::Suppressed,
                "core cut already in flight",
            )
        } else {
            (ActionDispositionWire::Proposed, "dispatch one core cut")
        };
        return Draft::new(
            PrimaryStateWire::CoreLag { pin: pin.clone() },
            "no complete published core contains the source pin",
        )
        .action(ProposedActionWire {
            disposition,
            reason: reason.to_string(),
            ..action
        });
    }

    if let Some(pr) = pr {
        if let Some(smoke) = pr.core_smoke.as_ref() {
            if !smoke.conclusion.is_success() {
                let matching =
                    matching_publish_run(&workflows.publish_yml, &pr.head_sha);
                let in_flight =
                    matching.is_some_and(|run| run.status.is_in_flight());
                let action = proposed_action(
                    TrainActionKindWire::DispatchPublishMetadata {
                        reason: "core incompatible smoke".to_string(),
                        pr_number: Some(pr.number),
                        head_sha: Some(pr.head_sha.clone()),
                        pin: Some(pr.source_pin.clone()),
                    },
                    ActionDispositionWire::Proposed,
                    vec!["one metadata refresh per PR identity".to_string()],
                    "refresh release-please metadata for the current PR",
                );
                let refreshed = matching.is_some_and(|run| {
                    run.status == RunStatusWire::Completed
                        && run
                            .conclusion
                            .is_some_and(CheckConclusionWire::is_success)
                });
                if refreshed {
                    return Draft::new(
                        PrimaryStateWire::CoreIncompatible {
                            detail: "published-core PR smoke still red after refresh"
                                .to_string(),
                        },
                        "metadata refresh already applied; escalate",
                    );
                }
                return Draft::new(
                    PrimaryStateWire::CoreIncompatible {
                        detail: format!(
                            "published-core PR smoke {:?}",
                            smoke.conclusion
                        ),
                    },
                    "red published-core PR smoke",
                )
                .action(suppress_if_in_flight(
                    action,
                    in_flight,
                    "publish.yml already in flight",
                ));
            }
        }
    }

    if let Some(run) = master.gate_run.as_ref() {
        if run.sha == master.sha
            && run.status == RunStatusWire::Completed
            && !run.conclusion.is_some_and(CheckConclusionWire::is_success)
        {
            match rerunnable_gate_jobs(run) {
                Ok(jobs) => {
                    let action = proposed_action(
                        TrainActionKindWire::RerunFailedJobs {
                            run_id: run.run_id.clone(),
                            attempt: run.attempt,
                            job_ids: jobs
                                .iter()
                                .map(|job| job.id.clone())
                                .collect(),
                        },
                        ActionDispositionWire::Proposed,
                        vec![
                            "failed Master Gate test jobs".to_string(),
                            "attempt permits one rerun".to_string(),
                        ],
                        "rerun failed Master Gate test jobs at HEAD",
                    );
                    return Draft::new(
                        PrimaryStateWire::GateRed {
                            sha: master.sha.clone(),
                        },
                        "Master Gate is red at HEAD",
                    )
                    .action(action)
                    .evidence(
                        "gate",
                        &run.run_id,
                        Some(run.url.clone()),
                    );
                }
                Err(reason) => {
                    return Draft::new(
                        PrimaryStateWire::GateRed {
                            sha: master.sha.clone(),
                        },
                        reason,
                    )
                    .evidence(
                        "gate",
                        &run.run_id,
                        Some(run.url.clone()),
                    );
                }
            }
        }
    }

    if let Some(pr) = pr {
        if pr.stale_head || pr.pin_changed {
            let matching =
                matching_publish_run(&workflows.publish_yml, &pr.head_sha);
            let in_flight =
                matching.is_some_and(|run| run.status.is_in_flight());
            let action = proposed_action(
                TrainActionKindWire::DispatchPublishMetadata {
                    reason: "stale head or changed pin".to_string(),
                    pr_number: Some(pr.number),
                    head_sha: Some(pr.head_sha.clone()),
                    pin: Some(pr.source_pin.clone()),
                },
                ActionDispositionWire::Proposed,
                vec!["metadata mode; never publish_existing".to_string()],
                "refresh release-please for stale head or changed pin",
            );
            return Draft::new(
                PrimaryStateWire::Waiting {
                    reason: "release PR head or pin needs metadata refresh"
                        .to_string(),
                },
                "stale or retargeted release PR",
            )
            .action(suppress_if_in_flight(
                action,
                in_flight,
                "publish.yml already in flight",
            ));
        }
        if pr.merged {
            let tags = snapshot.tags.as_ready().into_iter().flatten();
            let has_tag = tags.clone().any(|tag| tag.peeled_sha == pr.head_sha);
            let mut releases =
                snapshot.releases.as_ready().into_iter().flatten();
            let has_draft = releases.any(|rel| rel.draft);
            if !has_tag || !has_draft {
                let matching =
                    matching_publish_run(&workflows.publish_yml, &pr.head_sha);
                let in_flight =
                    matching.is_some_and(|run| run.status.is_in_flight());
                let action = proposed_action(
                    TrainActionKindWire::DispatchPublishMetadata {
                        reason: "merged PR missing tag/draft".to_string(),
                        pr_number: Some(pr.number),
                        head_sha: Some(pr.head_sha.clone()),
                        pin: Some(pr.source_pin.clone()),
                    },
                    ActionDispositionWire::Proposed,
                    vec!["metadata mode; never publish_existing".to_string()],
                    "create tag and draft after merge",
                );
                return Draft::new(
                    PrimaryStateWire::Waiting {
                        reason: "merged PR is missing its tag or draft"
                            .to_string(),
                    },
                    "dispatch publish.yml after merge",
                )
                .action(suppress_if_in_flight(
                    action,
                    in_flight,
                    "publish.yml already in flight",
                ));
            }
        }
    }

    let Some(pr) = pr else {
        return Draft::new(
            PrimaryStateWire::NothingToRelease {
                reason: "no open release PR and no active train".to_string(),
            },
            "nothing to release",
        );
    };
    if pr.merged {
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: "merged PR awaiting train facts".to_string(),
            },
            "merge already landed",
        );
    }
    if hold_label(pr) {
        return Draft::new(
            PrimaryStateWire::Vetoed {
                reason: "release-hold label".to_string(),
            },
            "release PR carries release-hold",
        );
    }
    if let Err(reason) = master_gate_green(master) {
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: reason.clone(),
            },
            reason,
        );
    }
    if let Err(reason) = pr_checks_green(pr, master) {
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: reason.clone(),
            },
            reason,
        );
    }
    let CoreEnsureFactsWire::Pinned { version, .. } = ensure else {
        return Draft::new(
            PrimaryStateWire::Waiting {
                reason: "core-ensure has not pinned a complete core"
                    .to_string(),
            },
            "merge requires core-ensure's complete pick",
        );
    };
    match pr.exact_public_core_pin.as_deref() {
        Some(pin) if pin == version.as_str() => {}
        Some(pin) => {
            return Draft::new(
                PrimaryStateWire::Waiting {
                    reason: format!(
                        "PR pin {pin} does not equal core-ensure pick {version}"
                    ),
                },
                "exact public core pin mismatch",
            );
        }
        None => {
            return Draft::new(
                PrimaryStateWire::Waiting {
                    reason: "release PR has no exact public core pin"
                        .to_string(),
                },
                "missing exact public core pin",
            );
        }
    }
    let merges = snapshot
        .merge_history
        .as_ready()
        .cloned()
        .unwrap_or_default();
    match cadence_block_reason(
        parse_observed_at(&request.observed_at).expect("validated"),
        &request.policy.cadence,
        &merges,
    ) {
        Ok(Some(reason)) => {
            return Draft::new(
                PrimaryStateWire::Waiting {
                    reason: reason.clone(),
                },
                reason,
            );
        }
        Ok(None) => {}
        Err(error) => {
            return broken(
                "cadence",
                &error.to_string(),
                "supply RFC 3339 merge timestamps",
            );
        }
    }

    let merge = proposed_action(
        TrainActionKindWire::MergeReleasePr {
            pr_number: pr.number,
            head_sha: pr.head_sha.clone(),
        },
        ActionDispositionWire::Proposed,
        vec![
            "no train in flight".to_string(),
            "Master Gate green at HEAD".to_string(),
            "required PR checks exactly successful".to_string(),
            "PR pin equals core-ensure pick".to_string(),
            "no veto".to_string(),
            "daily cadence permits".to_string(),
        ],
        "merge the release PR",
    );
    let mut draft = Draft::new(
        PrimaryStateWire::Releasable {
            pr_number: pr.number,
            head_sha: pr.head_sha.clone(),
        },
        "release PR is releasable",
    )
    .evidence("release-pr", &pr.number.to_string(), Some(pr.url.clone()));
    if !request.policy.merge_authority_enabled {
        draft = draft.action(ProposedActionWire {
            disposition: ActionDispositionWire::Suppressed,
            reason: "merge authority disabled".to_string(),
            ..merge
        });
    } else {
        draft = draft.action(merge);
    }
    draft
}

fn attach_install(
    mut draft: Draft,
    findings: Vec<SecondaryFindingWire>,
) -> Draft {
    if findings.is_empty() {
        return draft;
    }
    if matches!(draft.primary, PrimaryStateWire::WatcherBroken { .. }) {
        return draft;
    }
    let recovery = draft.train_action.as_ref().is_some_and(|action| {
        matches!(action.disposition, ActionDispositionWire::Proposed)
            && matches!(
                action.action,
                TrainActionKindWire::DispatchRelease { .. }
                    | TrainActionKindWire::MergeReleasePr { .. }
            )
    });
    if recovery {
        draft.secondary.extend(findings);
        return draft;
    }
    if matches!(
        draft.primary,
        PrimaryStateWire::NothingToRelease { .. }
            | PrimaryStateWire::Waiting { .. }
    ) && draft.train_action.is_none()
    {
        let mut iter = findings.into_iter();
        let first = iter.next().expect("non-empty");
        draft.secondary.extend(iter);
        draft.primary = first.state;
        draft.details = first.detail;
        return draft;
    }
    draft.secondary.extend(findings);
    draft
}

fn finish(
    request: &ReleaseStewardRequestWire,
    observed_at: chrono::DateTime<chrono::Utc>,
    draft: Draft,
) -> ReleaseStewardResultWire {
    let mut draft = draft;
    if request.policy.observe_only {
        if let Some(action) = draft.train_action.as_mut() {
            if action.disposition == ActionDispositionWire::Executed {
                action.disposition = ActionDispositionWire::Proposed;
                action.reason = format!(
                    "{}; observe output never says an action occurred",
                    action.reason
                );
            }
        }
    }
    let (escalation, started, uncertain) = decide_escalation(EscalationInput {
        observed_at,
        policy: &request.policy,
        snapshot: &request.snapshot,
        primary: &draft.primary,
        secondary: &draft.secondary,
    });
    let process_exit = u32::from(matches!(
        draft.primary,
        PrimaryStateWire::WatcherBroken { .. }
    ));
    ReleaseStewardResultWire {
        schema_version: RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
        primary: draft.primary,
        details: draft.details,
        evidence: draft.evidence,
        blocker_started_at: started,
        blocker_duration_uncertain: uncertain,
        train_action: draft.train_action,
        escalation,
        secondary: draft.secondary,
        process_exit,
    }
}

/// Decide one release-steward tick from a complete snapshot.
pub fn release_steward_decide(
    request: &ReleaseStewardRequestWire,
) -> Result<ReleaseStewardResultWire, ReleaseStewardError> {
    if request.schema_version != RELEASE_STEWARD_WIRE_SCHEMA_VERSION {
        return Err(ReleaseStewardError::UnsupportedSchema {
            actual: request.schema_version,
            expected: RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
        });
    }
    validate_cadence(&request.policy.cadence)?;
    let observed_at = parse_observed_at(&request.observed_at)?;
    let snapshot = &request.snapshot;
    if let Some(draft) = inspect_required(snapshot) {
        return Ok(finish(request, observed_at, draft));
    }
    let findings = canary_findings(&snapshot.canary);
    let trains = snapshot.trains.as_ready().cloned().unwrap_or_default();
    let selection = select_trains(&trains);
    if let Some(mismatch) = selection.mismatch {
        return Ok(finish(
            request,
            observed_at,
            broken(
                "trains",
                &mismatch,
                "fail closed on tag/manifest/evidence mismatch",
            ),
        ));
    }
    if selection.ambiguous {
        let draft = Draft::new(
            PrimaryStateWire::AmbiguousTrains {
                detail: selection.active_versions.join(", "),
            },
            "ambiguous set of active trains prevents another merge",
        );
        return Ok(finish(
            request,
            observed_at,
            attach_install(draft, findings),
        ));
    }
    let pr = match snapshot.release_pr.as_ready() {
        Some(value) => value.as_ref(),
        None => None,
    };
    if let Some(train) = selection.selected.as_ref() {
        if let Some((source, message, remedy)) =
            snapshot.candidate.unreadable_break()
        {
            return Ok(finish(
                request,
                observed_at,
                broken(source, message, remedy),
            ));
        }
        let Some(Some(eval)) = snapshot.candidate.as_ready() else {
            return Ok(finish(
                request,
                observed_at,
                broken(
                    "candidate",
                    "in-flight train is missing a candidate evaluator result",
                    "run release_candidate evaluate for the selected generation",
                ),
            ));
        };
        let draft = handle_active_train(train, eval, snapshot, pr);
        return Ok(finish(
            request,
            observed_at,
            attach_install(draft, findings),
        ));
    }
    let mut draft = handle_no_train(request, snapshot, pr);
    if matches!(draft.primary, PrimaryStateWire::NothingToRelease { .. }) {
        if let Some(train) = selection.confirmed {
            if train.confirmation_complete {
                draft = Draft::new(
                    PrimaryStateWire::Released {
                        version: train.version.clone(),
                    },
                    format!("confirmed public release {}", train.version),
                )
                .evidence("release", &train.tag, None);
            }
        }
    }
    Ok(finish(
        request,
        observed_at,
        attach_install(draft, findings),
    ))
}

pub fn release_steward_decide_from_json(
    value: &JsonValue,
) -> Result<ReleaseStewardResultWire, ReleaseStewardError> {
    let request: ReleaseStewardRequestWire =
        serde_json::from_value(value.clone()).map_err(|error| {
            ReleaseStewardError::invalid(format!(
                "request is not a valid ReleaseStewardRequestWire: {error}"
            ))
        })?;
    release_steward_decide(&request)
}

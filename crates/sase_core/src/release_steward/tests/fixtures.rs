use crate::release_steward::facts::{
    CanaryFactsWire, CanaryResultWire, CandidateEvalWire, CheckConclusionWire,
    CheckFactsWire, ClauseWire, CoreActionsAccessWire, CoreEnsureFactsWire,
    CoreFactsWire, FactRead, JobFactsWire, MasterFactsWire, MergeEventWire,
    PackageInventoryWire, PypiFactsWire, ReleasePrFactsWire,
    ReleaseStewardSnapshotWire, RepositoryFactsWire, RequiredCheckWire,
    RunStatusWire, TrainFactsWire, WorkflowFactsWire, WorkflowRunFactsWire,
};
use crate::release_steward::wire::{
    CadencePolicyWire, ReleaseStewardPolicyWire, ReleaseStewardRequestWire,
    RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
};

pub const HEAD: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const PIN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
pub const CORE_VERSION: &str = "0.50.0";
pub const TICK: &str = "run-1/attempt-1";
pub const NOON: &str = "2026-10-19T12:00:00Z";
pub const BEFORE_NINE: &str = "2026-10-19T08:59:00Z";
pub const AT_NINE: &str = "2026-10-19T09:00:00Z";
pub const YESTERDAY_NOON: &str = "2026-10-18T12:00:00Z";
pub const AGE_START: &str = "2026-10-18T11:00:00Z";

pub fn daily_cadence() -> CadencePolicyWire {
    CadencePolicyWire {
        kind: "daily".to_string(),
        merge_after_hour_utc: 9,
        max_merges_per_utc_day: 1,
    }
}

pub fn success_check(name: &str) -> CheckFactsWire {
    CheckFactsWire {
        name: name.to_string(),
        conclusion: CheckConclusionWire::Success,
        is_test: true,
        is_lint: false,
        url: Some(format!("https://example.test/check/{name}")),
    }
}

pub fn green_gate_run(sha: &str) -> WorkflowRunFactsWire {
    WorkflowRunFactsWire {
        workflow: "ci.yml".to_string(),
        run_id: "gate-1".to_string(),
        attempt: 1,
        ref_name: "master".to_string(),
        sha: sha.to_string(),
        status: RunStatusWire::Completed,
        conclusion: Some(CheckConclusionWire::Success),
        jobs: vec![
            JobFactsWire {
                id: "job-test".to_string(),
                name: "tests".to_string(),
                conclusion: Some(CheckConclusionWire::Success),
                is_test: true,
                is_lint: false,
                attempt: 1,
            },
            JobFactsWire {
                id: "job-lint".to_string(),
                name: "lint".to_string(),
                conclusion: Some(CheckConclusionWire::Success),
                is_test: false,
                is_lint: true,
                attempt: 1,
            },
        ],
        started_at: Some(NOON.to_string()),
        updated_at: Some(NOON.to_string()),
        event: Some("push".to_string()),
        url: "https://example.test/gate-1".to_string(),
    }
}

pub fn passing_canary(member: &str) -> CanaryResultWire {
    CanaryResultWire {
        member: member.to_string(),
        run_id: format!("canary-{member}"),
        attempt: 1,
        sha: HEAD.to_string(),
        verdict: "pass".to_string(),
        packages: vec!["sase".to_string()],
        failing_step: None,
        reason: None,
        scheduled: true,
    }
}

pub fn publishable_eval(
    candidate_id: &str,
    generation: u64,
) -> CandidateEvalWire {
    CandidateEvalWire {
        candidate_id: candidate_id.to_string(),
        generation,
        version: "0.19.0".to_string(),
        publishable: true,
        rerun_eligible: false,
        rerun_reason: String::new(),
        clauses: vec![
            ClauseWire {
                name: "source_and_version_identity_match".to_string(),
                ok: true,
                detail: "ok".to_string(),
            },
            ClauseWire {
                name: "public_dependency_contract_satisfied".to_string(),
                ok: true,
                detail: "core complete".to_string(),
            },
        ],
        active_veto: false,
        qualify_red: None,
        exercise_red: None,
        retained_asset_missing: false,
        workflow_broken_at_tag: false,
    }
}

pub fn active_train() -> TrainFactsWire {
    TrainFactsWire {
        candidate_id: "sase-0.19.0-gen1".to_string(),
        version: "0.19.0".to_string(),
        tag: "v0.19.0".to_string(),
        sha: HEAD.to_string(),
        generation: 1,
        confirmed: false,
        abandoned: false,
        pre_train_legacy: false,
        manifest_mismatch: false,
        evidence_mismatch: false,
        py_files_present: 0,
        py_files_expected: 2,
        py_digest_conflict: false,
        py_extra_files: false,
        confirmation_complete: false,
    }
}

pub fn releasable_pr() -> ReleasePrFactsWire {
    ReleasePrFactsWire {
        number: 42,
        head_sha: HEAD.to_string(),
        source_pin: PIN.to_string(),
        exact_public_core_pin: Some(CORE_VERSION.to_string()),
        labels: vec!["autorelease: pending".to_string()],
        checks: vec![success_check("tests"), success_check("lint")],
        merge_history: Vec::new(),
        stale_head: false,
        pin_changed: false,
        merged: false,
        merged_at: None,
        url: "https://github.com/sase-org/sase/pull/42".to_string(),
        core_smoke: Some(success_check("release-core-floor-smoke")),
    }
}

pub fn snapshot() -> ReleaseStewardSnapshotWire {
    ReleaseStewardSnapshotWire {
        repository: FactRead::ready(RepositoryFactsWire {
            owner: "sase-org".to_string(),
            name: "sase".to_string(),
            default_branch: "master".to_string(),
        }),
        master: FactRead::ready(MasterFactsWire {
            sha: HEAD.to_string(),
            gate_run: Some(green_gate_run(HEAD)),
            expected_required_checks: vec![
                RequiredCheckWire {
                    name: "tests".to_string(),
                    source: "branch protection".to_string(),
                },
                RequiredCheckWire {
                    name: "lint".to_string(),
                    source: "branch protection".to_string(),
                },
            ],
            expected_checks_source: "branch protection".to_string(),
            expected_checks_readable: true,
        }),
        release_pr: FactRead::ready(Some(releasable_pr())),
        tags: FactRead::ready(Vec::new()),
        releases: FactRead::ready(Vec::new()),
        pypi: FactRead::ready(PypiFactsWire {
            sase: PackageInventoryWire {
                package: "sase".to_string(),
                files: Vec::new(),
            },
            core: PackageInventoryWire {
                package: "sase-core-rs".to_string(),
                files: Vec::new(),
            },
        }),
        trains: FactRead::ready(Vec::new()),
        workflows: FactRead::ready(WorkflowFactsWire {
            release_yml: Vec::new(),
            publish_yml: Vec::new(),
        }),
        core: FactRead::ready(CoreFactsWire {
            master_ci: Some(green_gate_run(PIN)),
            open_release_pr: None,
            cuts: Vec::new(),
        }),
        canary: FactRead::ready(CanaryFactsWire {
            linux: Some(passing_canary("linux")),
            macos: Some(passing_canary("macos")),
        }),
        candidate: FactRead::ready(None),
        core_ensure: FactRead::ready(CoreEnsureFactsWire::Pinned {
            version: CORE_VERSION.to_string(),
            files: vec![
                "sase_core_rs-0.50.0.tar.gz".to_string(),
                "sase_core_rs-0.50.0-cp312-abi3-manylinux.whl".to_string(),
            ],
        }),
        issues: FactRead::ready(Vec::new()),
        core_actions_access: FactRead::ready(CoreActionsAccessWire {
            readable: true,
            detail: "read ok".to_string(),
        }),
        merge_history: FactRead::ready(Vec::new()),
        blocker_lineage: FactRead::ready(None),
    }
}

pub fn releasable_request() -> ReleaseStewardRequestWire {
    ReleaseStewardRequestWire {
        schema_version: RELEASE_STEWARD_WIRE_SCHEMA_VERSION,
        observed_at: NOON.to_string(),
        policy: ReleaseStewardPolicyWire {
            merge_authority_enabled: true,
            observe_only: true,
            tick_identity: TICK.to_string(),
            cadence: daily_cadence(),
        },
        snapshot: snapshot(),
    }
}

pub fn merge_event(at: &str, abandoned_after: bool) -> MergeEventWire {
    MergeEventWire {
        pr_number: 41,
        head_sha: "cccccccccccccccccccccccccccccccccccccccc".to_string(),
        merged_at: at.to_string(),
        candidate_id: Some("sase-0.18.0-gen1".to_string()),
        abandoned_after,
    }
}

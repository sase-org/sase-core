use std::collections::BTreeMap;

use serde_json::{json, Value};

use super::*;

fn repo(
    id: &str,
    name: &str,
    kind: ProjectStatsRepoKindWire,
) -> ProjectStatsRepoSpecWire {
    ProjectStatsRepoSpecWire {
        id: id.to_owned(),
        name: name.to_owned(),
        kind,
        path: format!("/repos/{name}"),
        identity: "https://example.test/team/repo.git".to_owned(),
        ref_candidates: vec!["main".to_owned(), "HEAD".to_owned()],
    }
}

fn project(
    key: &str,
    state: ProjectStatsProjectStateWire,
) -> ProjectStatsProjectSpecWire {
    ProjectStatsProjectSpecWire {
        key: key.to_owned(),
        label: format!("Project {key}"),
        state,
        selection: ProjectStatsSelectionWire::Always,
        index_project_names: vec!["sample".to_owned()],
        code_repos: vec![repo(
            "code",
            "code",
            ProjectStatsRepoKindWire::Primary,
        )],
        record_repos: vec![repo(
            "records",
            "records",
            ProjectStatsRepoKindWire::Sidecar,
        )],
        not_counted: vec![ProjectStatsNotCountedWire {
            name: "upstream-docs".to_owned(),
            reason: ProjectStatsNotCountedReasonWire::External,
        }],
        plan_store: ProjectStatsPlanStoreWire {
            storage: ProjectStatsPlanStorageWire::SeparateRepo,
            repo_path: Some("/repos/plans".to_owned()),
            plans_dir: "plans".to_owned(),
            project_filter: Some("sample".to_owned()),
        },
    }
}

fn request() -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: 1_700_000_000,
        end_ts: 1_700_086_400,
        now_ts: 1_700_172_800,
        timezone: "America/New_York".to_owned(),
        facets: vec![
            ProjectStatsFacetWire::Output,
            ProjectStatsFacetWire::Code,
            ProjectStatsFacetWire::Runs,
        ],
        cache_dir: "/tmp/project-stats-cache".to_owned(),
        run_index_path: Some("/tmp/run-index.json".to_owned()),
        agents_sidecars: vec![ProjectStatsAgentSidecarSpecWire {
            project_key: "alpha".to_owned(),
            path: "/tmp/agents".to_owned(),
        }],
        local_agent_name_prefix: "local-".to_owned(),
        known_model_aliases: BTreeMap::from([(
            "sonnet".to_owned(),
            "anthropic/claude-sonnet".to_owned(),
        )]),
        record_repo_names: vec!["plans".to_owned()],
        include_run_only_projects: true,
        budget: ProjectStatsBudgetWire::default(),
        projects: vec![project("alpha", ProjectStatsProjectStateWire::Enabled)],
    }
}

fn coverage() -> ProjectStatsCoverageWire {
    ProjectStatsCoverageWire {
        configured_repos: 2,
        measured_repos: 1,
        partial_repos: 1,
        unavailable_repos: 0,
        agent_tagged_commits: 2,
        resolved_agent_commits: 1,
        unknown_agent_commits: 1,
        unknown_reasons: vec![ProjectStatsUnknownReasonCountWire {
            reason: "page_unpublished".to_owned(),
            count: 1,
        }],
    }
}

fn response_fixture() -> ProjectStatsResponseWire {
    let models = vec![
        ProjectStatsModelWire {
            kind: ProjectStatsModelKindWire::Resolved,
            provider: Some("anthropic".to_owned()),
            model: Some("claude-sonnet".to_owned()),
            model_is_alias: true,
            commits: 1,
            plans: Some(2),
            plans_epic: Some(1),
            plans_tale: Some(1),
            unknown_reasons: Vec::new(),
        },
        ProjectStatsModelWire {
            kind: ProjectStatsModelKindWire::Unknown,
            provider: None,
            model: None,
            model_is_alias: false,
            commits: 1,
            plans: Some(0),
            plans_epic: Some(0),
            plans_tale: Some(0),
            unknown_reasons: vec!["page_unpublished".to_owned()],
        },
        ProjectStatsModelWire {
            kind: ProjectStatsModelKindWire::NoAgent,
            provider: None,
            model: None,
            model_is_alias: false,
            commits: 2,
            plans: Some(0),
            plans_epic: Some(0),
            plans_tale: Some(0),
            unknown_reasons: Vec::new(),
        },
    ];
    let project_output = ProjectStatsOutputWire {
        commits: 4,
        agent_commits: 2,
        agent_share: Some(0.5),
        plans_committed: Some(2),
        plans_epic: Some(1),
        plans_tale: Some(1),
        models: models.clone(),
        commit_coverage: coverage(),
        plan_coverage: ProjectStatsCoverageWire {
            configured_repos: 1,
            measured_repos: 1,
            ..ProjectStatsCoverageWire::default()
        },
    };
    let lifetime = ProjectStatsLifetimeWire {
        first_commit_ts: Some(1_600_000_000),
        last_commit_ts: Some(1_700_000_000),
        age_days: Some(1157),
        active_days: Some(3),
        commits: 11,
        agent_commits: 7,
    };
    let code = ProjectStatsCodeWire {
        counter: ProjectStatsCodeCounterWire::Sloc,
        code_files: 8,
        code_lines: 120,
        test_lines: 35,
        test_share: Some(0.2258),
        doc_files: 2,
        doc_lines: 18,
        agent_context_files: 1,
        agent_context_lines: 12,
        languages: vec![ProjectStatsLanguageWire {
            name: "Rust".to_owned(),
            files: 6,
            lines: 90,
        }],
        repos_configured: 2,
        repos_measured: 1,
    };
    let runs = ProjectStatsRunsWire {
        runs: 5,
        completed: 3,
        failed: 1,
        other_terminal: 1,
        in_progress: 0,
        waiting: 0,
        success_rate: Some(0.6),
        total_runtime_seconds: 321.5,
        distinct_patches: 2,
        unattributed_runs: 1,
        last_run_ts: Some(1_700_000_000),
    };
    let repo_output = ProjectStatsRepoWire {
        id: "code".to_owned(),
        name: "code".to_owned(),
        kind: ProjectStatsRepoKindWire::Primary,
        tier: ProjectStatsRepoTierWire::Code,
        status: ProjectStatsRepoStatusWire::Ok,
        issue: None,
        reference: Some(ProjectStatsRefWire {
            name: "main".to_owned(),
            commit: "0123456789abcdef".to_owned(),
            shallow: false,
            fetched_at: Some(1_700_000_000),
        }),
        output: Some(project_output.clone()),
        lifetime: Some(lifetime.clone()),
        snapshot: None,
    };
    let record_repo = ProjectStatsRepoWire {
        id: "records".to_owned(),
        name: "records".to_owned(),
        kind: ProjectStatsRepoKindWire::Sidecar,
        tier: ProjectStatsRepoTierWire::Records,
        status: ProjectStatsRepoStatusWire::Partial,
        issue: Some(ProjectStatsIssueWire {
            project: Some("alpha".to_owned()),
            repo: Some("records".to_owned()),
            code: "snapshot_partial".to_owned(),
            message: "Some tracked files could not be classified".to_owned(),
            hint: Some("Check the repository index".to_owned()),
        }),
        reference: None,
        output: None,
        lifetime: None,
        snapshot: Some(ProjectStatsSnapshotWire {
            tracked_files: 10,
            tracked_lines: 200,
            test_files: 2,
            test_lines: 30,
            doc_files: 3,
            doc_lines: 50,
            agent_context_files: 1,
            agent_context_lines: 15,
            lifetime_commits: 22,
            lifetime_agent_commits: 9,
            languages: vec![ProjectStatsLanguageWire {
                name: "Markdown".to_owned(),
                files: 3,
                lines: 50,
            }],
            as_of_ts: Some(1_700_000_000),
        }),
    };
    let project_wire = ProjectStatsProjectWire {
        key: "alpha".to_owned(),
        label: "Alpha".to_owned(),
        state: ProjectStatsProjectStateWire::Enabled,
        output: Some(project_output.clone()),
        lifetime: Some(lifetime.clone()),
        code: Some(code.clone()),
        runs: Some(runs.clone()),
        repos: vec![repo_output, record_repo],
        not_counted: vec![ProjectStatsNotCountedWire {
            name: "vendor".to_owned(),
            reason: ProjectStatsNotCountedReasonWire::UserLinked,
        }],
        models: models.clone(),
        patches: vec![ProjectStatsPatchWire {
            name: "feature.patch".to_owned(),
            status: "merged".to_owned(),
            has_pr: true,
            runs: 2,
            completed: 2,
            failed: 0,
            run_commits: 4,
            total_runtime_seconds: 120,
            first_run_ts: Some(1_699_000_000),
            last_run_ts: Some(1_700_000_000),
        }],
        truncated_patch_rows: 1,
        coverage: coverage(),
    };
    ProjectStatsResponseWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        metric_policy_version: PROJECT_STATS_METRIC_POLICY_VERSION,
        classifier_version: PROJECT_STATS_CLASSIFIER_VERSION,
        engine_version: PROJECT_STATS_ENGINE_VERSION,
        range: ProjectStatsRangeWire {
            start_ts: 1_700_000_000,
            end_ts: 1_700_086_400,
        },
        fingerprint: "abc123".to_owned(),
        projects: vec![project_wire],
        portfolio: ProjectStatsPortfolioWire {
            output: Some(project_output),
            lifetime: Some(lifetime),
            code: Some(code),
            runs: Some(runs),
            models,
            coverage: coverage(),
        },
        issues: Vec::new(),
    }
}

#[test]
fn full_request_and_response_contracts_round_trip() {
    let request = request();
    validate_project_stats_request(&request).expect("request is valid");
    let request_json =
        serde_json::to_value(&request).expect("serialize request");
    let decoded_request: ProjectStatsRequestWire =
        serde_json::from_value(request_json).expect("deserialize request");
    assert_eq!(decoded_request, request);

    let response = response_fixture();
    let response_json =
        serde_json::to_value(&response).expect("serialize response");
    let decoded_response: ProjectStatsResponseWire =
        serde_json::from_value(response_json.clone())
            .expect("deserialize response");
    assert_eq!(decoded_response, response);
    assert_eq!(
        response_json["projects"][0]["repos"][0]["ref"],
        json!({
            "name": "main",
            "commit": "0123456789abcdef",
            "shallow": false,
            "fetched_at": 1_700_000_000
        })
    );
}

#[test]
fn nullable_facets_are_serialized_as_explicit_nulls() {
    let empty_project = ProjectStatsProjectWire {
        key: "empty".to_owned(),
        label: "Empty".to_owned(),
        state: ProjectStatsProjectStateWire::Disabled,
        output: None,
        lifetime: None,
        code: None,
        runs: None,
        repos: Vec::new(),
        not_counted: Vec::new(),
        models: Vec::new(),
        patches: Vec::new(),
        truncated_patch_rows: 0,
        coverage: ProjectStatsCoverageWire::default(),
    };
    let response = ProjectStatsResponseWire {
        schema_version: 1,
        metric_policy_version: 1,
        classifier_version: 1,
        engine_version: 1,
        range: ProjectStatsRangeWire::default(),
        fingerprint: "empty".to_owned(),
        projects: vec![empty_project],
        portfolio: ProjectStatsPortfolioWire::default(),
        issues: Vec::new(),
    };
    let json = serde_json::to_value(response).expect("serialize response");
    assert_eq!(json["projects"][0]["output"], Value::Null);
    assert_eq!(json["projects"][0]["lifetime"], Value::Null);
    assert_eq!(json["projects"][0]["code"], Value::Null);
    assert_eq!(json["projects"][0]["runs"], Value::Null);
    assert_eq!(json["portfolio"]["output"], Value::Null);
    assert_eq!(json["portfolio"]["lifetime"], Value::Null);
    assert_eq!(json["portfolio"]["code"], Value::Null);
    assert_eq!(json["portfolio"]["runs"], Value::Null);
}

#[test]
fn validation_checks_versions_ranges_timezone_bounds_and_identities() {
    let mut candidate = request();
    assert!(validate_project_stats_request(&candidate).is_ok());
    candidate.start_ts = candidate.end_ts;
    assert!(validate_project_stats_request(&candidate).is_ok());

    candidate = request();
    candidate.schema_version += 1;
    assert!(matches!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::UnsupportedSchema { .. })
    ));
    candidate = request();
    candidate.timezone = "Not/A_Zone".to_owned();
    assert!(matches!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::InvalidTimezone(_))
    ));
    candidate = request();
    candidate.end_ts = candidate.start_ts - 1;
    assert_eq!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::ReversedRange)
    );
    candidate = request();
    candidate.budget.timeout_ms = 0;
    assert_eq!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::InvalidBudget)
    );
    candidate = request();
    candidate.projects[0].code_repos[0].path.clear();
    assert!(matches!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::InvalidIdentity(_))
    ));
    candidate = request();
    candidate.facets.push(ProjectStatsFacetWire::Output);
    assert!(matches!(
        validate_project_stats_request(&candidate),
        Err(ProjectStatsError::DuplicateFacet(_))
    ));
    assert!(ProjectStatsBudgetWire::default().timeout_ms > 0);
    assert!(ProjectStatsBudgetWire::default().max_output_bytes > 0);
}

#[test]
fn enums_use_snake_case_and_cover_contract_variants() {
    let value = serde_json::to_value(vec![
        ProjectStatsFacetWire::Output,
        ProjectStatsFacetWire::Code,
        ProjectStatsFacetWire::Runs,
    ])
    .expect("serialize facets");
    assert_eq!(value, json!(["output", "code", "runs"]));
    assert_eq!(
        serde_json::to_value(ProjectStatsSelectionWire::IfRuns).unwrap(),
        json!("if_runs")
    );
    assert_eq!(
        serde_json::to_value(ProjectStatsRepoKindWire::Sidecar).unwrap(),
        json!("sidecar")
    );
    assert_eq!(
        serde_json::to_value(ProjectStatsPlanStorageWire::SeparateRepo)
            .unwrap(),
        json!("separate_repo")
    );
    assert_eq!(
        serde_json::to_value(ProjectStatsCodeCounterWire::Nonblank).unwrap(),
        json!("nonblank")
    );
    assert_eq!(
        serde_json::to_value(ProjectStatsRepoTierWire::Records).unwrap(),
        json!("records")
    );
    assert_eq!(
        serde_json::to_value(ProjectStatsRepoStatusWire::Partial).unwrap(),
        json!("partial")
    );

    let models = vec![
        ProjectStatsModelKindWire::Resolved,
        ProjectStatsModelKindWire::Unknown,
        ProjectStatsModelKindWire::NoAgent,
    ];
    assert_eq!(
        serde_json::to_value(models).unwrap(),
        json!(["resolved", "unknown", "no_agent"])
    );
}

#[test]
fn unimplemented_facets_stay_null_and_fingerprint_ignores_cache_dir() {
    let mut first_request = request();
    let first = query_project_stats(&first_request).expect("query");
    assert_eq!(first.projects.len(), 1);
    let first_project = &first.projects[0];
    let output = first_project.output.as_ref().expect("output facet");
    assert!(first_project.lifetime.is_some());
    assert!(first_project.code.is_none());
    assert!(first_project.runs.is_none());
    assert_eq!(output.commits, 0);
    assert_eq!(first_project.coverage.configured_repos, 1);
    assert_eq!(first_project.coverage.unavailable_repos, 1);
    assert_eq!(first.portfolio.output.as_ref().unwrap().commits, 0);
    assert_eq!(first.portfolio.code, None);
    assert_eq!(first.portfolio.runs, None);

    first_request.cache_dir = "/other/cache".to_owned();
    let second = query_project_stats(&first_request).expect("second query");
    assert_eq!(first.fingerprint, second.fingerprint);
}

#[test]
fn request_and_response_support_every_project_state() {
    let mut candidate = request();
    candidate.projects = vec![
        project("enabled", ProjectStatsProjectStateWire::Enabled),
        project("disabled", ProjectStatsProjectStateWire::Disabled),
        project("unregistered", ProjectStatsProjectStateWire::Unregistered),
    ];
    let response =
        query_project_stats(&candidate).expect("all states validate");
    assert_eq!(
        response
            .projects
            .iter()
            .map(|item| item.state)
            .collect::<Vec<_>>(),
        vec![
            ProjectStatsProjectStateWire::Enabled,
            ProjectStatsProjectStateWire::Disabled,
            ProjectStatsProjectStateWire::Unregistered,
        ]
    );
}

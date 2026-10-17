use std::collections::BTreeMap;
use std::path::Path;

use crate::project_stats::aggregate::check_output_invariants;
use crate::project_stats::query_project_stats;
use crate::project_stats::test_support::{
    commit, commit_all, commit_at, git, init_repo, plan_markdown, spec,
    write_file,
};
use crate::project_stats::wire::{
    ProjectStatsAgentSidecarSpecWire, ProjectStatsBudgetWire,
    ProjectStatsFacetWire, ProjectStatsModelKindWire,
    ProjectStatsPlanStorageWire, ProjectStatsPlanStoreWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectStateWire,
    ProjectStatsRequestWire, ProjectStatsSelectionWire,
    PROJECT_STATS_WIRE_SCHEMA_VERSION,
};

fn project(
    key: &str,
    repo: &Path,
    store: ProjectStatsPlanStoreWire,
) -> ProjectStatsProjectSpecWire {
    let mut spec = spec(repo);
    spec.id = format!("{key}-code");
    spec.name = key.to_owned();
    ProjectStatsProjectSpecWire {
        key: key.to_owned(),
        label: format!("Project {key}"),
        state: ProjectStatsProjectStateWire::Enabled,
        selection: ProjectStatsSelectionWire::Always,
        index_project_names: Vec::new(),
        code_repos: vec![spec],
        record_repos: Vec::new(),
        not_counted: Vec::new(),
        plan_store: store,
    }
}

fn request(
    projects: Vec<ProjectStatsProjectSpecWire>,
    cache_dir: &str,
    sidecar: Option<&Path>,
) -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: 0,
        end_ts: 4_000_000_000,
        now_ts: 4_000_000_000,
        timezone: "America/New_York".to_owned(),
        facets: vec![ProjectStatsFacetWire::Output],
        cache_dir: cache_dir.to_owned(),
        run_index_path: None,
        agents_sidecars: sidecar
            .map(|path| ProjectStatsAgentSidecarSpecWire {
                project_key: "alpha".to_owned(),
                path: path.to_string_lossy().into_owned(),
            })
            .into_iter()
            .collect(),
        local_agent_name_prefix: String::new(),
        known_model_aliases: BTreeMap::new(),
        record_repo_names: Vec::new(),
        include_run_only_projects: false,
        budget: ProjectStatsBudgetWire::default(),
        projects,
    }
}

fn in_tree_store() -> ProjectStatsPlanStoreWire {
    ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::InTree,
        repo_path: None,
        plans_dir: "sdd/plans".to_owned(),
        project_filter: None,
    }
}

#[test]
fn sidecar_root_counts_plans_without_a_directory_prefix() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "202610/root-plan.md",
        &plan_markdown("epic", Some("openai/gpt"), None, None),
        "add a plan at the sidecar root",
    );
    commit(&fixture.repo, "README.md", "Not a plan", "add readme");
    let store = ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::SidecarRepos,
        repo_path: Some(fixture.repo.to_string_lossy().into_owned()),
        plans_dir: ".".to_owned(),
        project_filter: None,
    };
    let response = query_project_stats(&request(
        vec![project("alpha", &fixture.repo, store)],
        "",
        None,
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.plans_committed, Some(1));
    assert_eq!(output.plans_epic, Some(1));
}

#[test]
fn unresolved_sidecar_does_not_count_primary_repository_plans() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "sdd/plans/primary.md",
        &plan_markdown("epic", Some("openai/gpt"), None, None),
        "add a primary plan",
    );
    let store = ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::SeparateRepo,
        repo_path: None,
        plans_dir: ".".to_owned(),
        project_filter: None,
    };
    let response = query_project_stats(&request(
        vec![project("alpha", &fixture.repo, store)],
        "",
        None,
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.commits, Some(1));
    assert_eq!(output.plans_committed, None);
    assert!(response
        .issues
        .iter()
        .any(|issue| issue.code == "plan_store_unresolved"));
}

#[test]
fn first_add_survives_rename_and_later_edits_are_not_double_counted() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("tale", Some("anthropic/opus"), None, None),
        "add plan",
    );
    std::fs::create_dir_all(fixture.repo.join("sdd/plans/202611")).unwrap();
    git(
        &fixture.repo,
        &[
            "mv",
            "sdd/plans/202610/alpha.md",
            "sdd/plans/202611/renamed.md",
        ],
    );
    commit_all(&fixture.repo, "rename plan");
    write_file(
        &fixture.repo,
        "sdd/plans/202611/renamed.md",
        &format!(
            "{} \nEdited body.\n",
            plan_markdown("tale", Some("anthropic/opus"), None, None)
        ),
    );
    commit_all(&fixture.repo, "edit plan");
    commit(
        &fixture.repo,
        "sdd/plans/202610/skip.plan-decisions.json",
        "{\"rows\":[]}\n",
        "decisions sidecar",
    );
    let cache = tempfile::tempdir().unwrap();
    let response = query_project_stats(&request(
        vec![project("alpha", &fixture.repo, in_tree_store())],
        &cache.path().to_string_lossy(),
        None,
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    check_output_invariants(output).unwrap();
    assert_eq!(output.plans_committed, Some(1));
    assert_eq!(output.plans_tale, Some(1));
    assert_eq!(output.plans_epic, Some(0));
    let resolved = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.plans, Some(1));
    assert_eq!(resolved.model.as_deref(), Some("opus"));
}

#[test]
fn re_proposal_credits_standing_frontmatter_and_range_uses_first_add() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("epic", Some("anthropic/opus"), None, None),
        "first add",
        1_700_000_000,
        1_700_000_000,
    );
    write_file(
        &fixture.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("epic", Some("openai/gpt"), None, None),
    );
    commit_at(
        &fixture.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("epic", Some("openai/gpt"), None, None),
        "re-propose",
        1_800_000_000,
        1_800_000_000,
    );
    let cache = tempfile::tempdir().unwrap();
    let mut req = request(
        vec![project("alpha", &fixture.repo, in_tree_store())],
        &cache.path().to_string_lossy(),
        None,
    );
    req.start_ts = 1_699_000_000;
    req.end_ts = 1_701_000_000;
    let response = query_project_stats(&req).unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.plans_committed, Some(1));
    assert_eq!(output.plans_epic, Some(1));
    let resolved = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.model.as_deref(), Some("gpt"));
}

#[test]
fn local_storage_is_null_and_shared_store_filters_by_project() {
    let local = init_repo();
    commit(
        &local.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("tale", Some("anthropic/opus"), None, Some("alpha")),
        "local-looking plan",
    );
    let cache = tempfile::tempdir().unwrap();
    let local_store = ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::Local,
        repo_path: None,
        plans_dir: "sdd/plans".to_owned(),
        project_filter: None,
    };
    let response = query_project_stats(&request(
        vec![project("alpha", &local.repo, local_store)],
        &cache.path().to_string_lossy(),
        None,
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    assert!(output.plans_committed.is_none());

    let shared = init_repo();
    commit(
        &shared.repo,
        "plans/202610/alpha.md",
        &plan_markdown("tale", Some("anthropic/opus"), None, Some("alpha")),
        "alpha plan",
    );
    commit(
        &shared.repo,
        "plans/202610/beta.md",
        &plan_markdown("epic", Some("openai/gpt"), None, Some("beta")),
        "beta plan",
    );
    let cache = tempfile::tempdir().unwrap();
    let filtered = ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::SeparateRepo,
        repo_path: Some(shared.repo.to_string_lossy().into_owned()),
        plans_dir: "plans".to_owned(),
        project_filter: Some("alpha".to_owned()),
    };
    let code = init_repo();
    commit(&code.repo, "code.txt", "c\n", "code");
    let response = query_project_stats(&request(
        vec![project("alpha", &code.repo, filtered)],
        &cache.path().to_string_lossy(),
        None,
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    check_output_invariants(output).unwrap();
    assert_eq!(output.plans_committed, Some(1));
    assert_eq!(output.plans_tale, Some(1));
    assert_eq!(output.plans_epic, Some(0));
}

#[test]
fn plan_member_lookup_uses_plan_role_page() {
    let code = init_repo();
    commit(&code.repo, "code.txt", "c\n", "code");
    commit(
        &code.repo,
        "sdd/plans/202610/alpha.md",
        &plan_markdown("tale", None, Some("writer"), None),
        "proposed plan",
    );
    let sidecar = init_repo();
    crate::project_stats::test_support::commit_files(
        &sidecar.repo,
        &[(
            "agents/writer--plan/meta.json",
            r#"{
                "schema_version": 2,
                "owner": {"username": "bbugyi200", "machine_name": "apollo"},
                "project": {"key": "sase", "name": "sase"},
                "source_run_id": "run-1",
                "local_name": "writer--plan",
                "global_name": "writer--plan",
                "metadata": {
                    "llm_provider": "anthropic",
                    "model": "sonnet",
                    "agent_family": "writer",
                    "agent_family_role": "plan"
                }
            }"#,
        )],
        "publish planner",
    );
    let cache = tempfile::tempdir().unwrap();
    let response = query_project_stats(&request(
        vec![project("alpha", &code.repo, in_tree_store())],
        &cache.path().to_string_lossy(),
        Some(&sidecar.repo),
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    check_output_invariants(output).unwrap();
    assert_eq!(output.plans_committed, Some(1));
    let resolved = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.model.as_deref(), Some("sonnet"));
    assert_eq!(resolved.plans, Some(1));
}

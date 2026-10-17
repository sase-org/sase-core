use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::TimeZone;
use chrono_tz::America::New_York;

use super::aggregate::check_output_invariants;
use super::test_support::{
    agent_message, clone_repo, commit, commit_at, git, init_repo, spec,
};
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsFacetWire, ProjectStatsModelKindWire,
    ProjectStatsPlanStorageWire, ProjectStatsPlanStoreWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectStateWire,
    ProjectStatsRepoSpecWire, ProjectStatsRepoStatusWire,
    ProjectStatsRepoTierWire, ProjectStatsRequestWire,
    ProjectStatsSelectionWire, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use super::*;

fn plan_store() -> ProjectStatsPlanStoreWire {
    ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::InTree,
        repo_path: None,
        plans_dir: "sdd/plans".to_owned(),
        project_filter: None,
    }
}

fn repo_spec(id: &str, path: &Path) -> ProjectStatsRepoSpecWire {
    let mut spec = spec(path);
    spec.id = id.to_owned();
    spec.name = id.to_owned();
    spec
}

fn project(
    key: &str,
    code: Vec<ProjectStatsRepoSpecWire>,
    record: Vec<ProjectStatsRepoSpecWire>,
) -> ProjectStatsProjectSpecWire {
    ProjectStatsProjectSpecWire {
        key: key.to_owned(),
        label: format!("Project {key}"),
        state: ProjectStatsProjectStateWire::Enabled,
        selection: ProjectStatsSelectionWire::Always,
        index_project_names: Vec::new(),
        code_repos: code,
        record_repos: record,
        not_counted: Vec::new(),
        plan_store: plan_store(),
    }
}

fn request(
    projects: Vec<ProjectStatsProjectSpecWire>,
    start_ts: i64,
    end_ts: i64,
    now_ts: i64,
    cache_dir: &str,
) -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts,
        end_ts,
        now_ts,
        timezone: "America/New_York".to_owned(),
        facets: vec![ProjectStatsFacetWire::Output],
        cache_dir: cache_dir.to_owned(),
        run_index_path: None,
        agents_sidecars: Vec::new(),
        local_agent_name_prefix: String::new(),
        known_model_aliases: BTreeMap::new(),
        record_repo_names: Vec::new(),
        include_run_only_projects: false,
        budget: ProjectStatsBudgetWire::default(),
        projects,
    }
}

fn cache_dir() -> tempfile::TempDir {
    tempfile::tempdir().expect("cache directory")
}

fn wide_range() -> (i64, i64, i64) {
    (0, 4_000_000_000, 4_000_000_000)
}

fn query_repo(path: &Path, cache: &Path) -> ProjectStatsResponseWire {
    let (start, end, now) = wide_range();
    query_project_stats(&request(
        vec![project("alpha", vec![repo_spec("code", path)], Vec::new())],
        start,
        end,
        now,
        &cache.to_string_lossy(),
    ))
    .expect("query")
}

#[test]
fn both_footer_spellings_and_authoritative_model_tags() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "one.txt",
        "one\n",
        &agent_message("sase spelling", "writer", "anthropic/opus"),
    );
    commit(
        &fixture.repo,
        "two.txt",
        "two\n",
        "bare spelling\n\nAGENT=writer\nMODEL=openai/gpt\n",
    );
    let cache = cache_dir();
    let response = query_repo(&fixture.repo, cache.path());
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.commits, Some(2));
    assert_eq!(output.agent_commits, Some(2));
    check_output_invariants(output).unwrap();
    let resolved: Vec<_> = output
        .models
        .iter()
        .filter(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .collect();
    assert_eq!(resolved.len(), 2);
}

#[test]
fn malformed_repeated_prose_and_absent_agent_model_tags() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "malformed.txt",
        "m\n",
        "malformed\n\nSASE_AGENT=writer\nSASE_MODEL=noshift\n",
    );
    commit(
        &fixture.repo,
        "repeat.txt",
        "r\n",
        "repeat\n\nSASE_AGENT=writer\nSASE_MODEL=anthropic/first\nSASE_MODEL=anthropic/second\n",
    );
    commit(
        &fixture.repo,
        "prose.txt",
        "p\n",
        "body mentions AGENT=fake and MODEL=x/y in prose only\n",
    );
    commit(
        &fixture.repo,
        "human.txt",
        "h\n",
        "human with model\n\nSASE_MODEL=anthropic/opus\n",
    );
    let cache = cache_dir();
    let response = query_repo(&fixture.repo, cache.path());
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.commits, Some(4));
    assert_eq!(output.agent_commits, Some(2));
    check_output_invariants(output).unwrap();
    let unknown = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Unknown)
        .unwrap();
    assert_eq!(unknown.commits, 1);
    assert_eq!(unknown.unknown_reasons, ["malformed_model_tag"]);
    let resolved = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.model.as_deref(), Some("first"));
    let no_agent = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::NoAgent)
        .unwrap();
    assert_eq!(no_agent.commits, 2);
}

#[test]
fn unknown_and_no_agent_zero_rows_and_alias_flag() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "alias.txt",
        "a\n",
        &agent_message("alias", "writer", "anthropic/sonnet"),
    );
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let mut req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    );
    req.known_model_aliases
        .insert("sonnet".to_owned(), "anthropic/claude-sonnet".to_owned());
    let response = query_project_stats(&req).unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.models.len(), 3);
    let resolved = &output.models[0];
    assert_eq!(resolved.model.as_deref(), Some("sonnet"));
    assert!(resolved.model_is_alias);
    assert_eq!(output.models[1].kind, ProjectStatsModelKindWire::Unknown);
    assert_eq!(output.models[1].commits, 0);
    assert_eq!(output.models[2].kind, ProjectStatsModelKindWire::NoAgent);
    assert_eq!(output.models[2].commits, 0);
}

#[test]
fn merges_are_included_in_output_counts() {
    let fixture = init_repo();
    commit(&fixture.repo, "base.txt", "base\n", "base");
    git(&fixture.repo, &["checkout", "-b", "side"]);
    commit(&fixture.repo, "side.txt", "side\n", "side");
    git(&fixture.repo, &["checkout", "main"]);
    commit(&fixture.repo, "main.txt", "main\n", "main");
    git(
        &fixture.repo,
        &["merge", "--no-ff", "side", "-m", "merge side"],
    );
    let cache = cache_dir();
    let response = query_repo(&fixture.repo, cache.path());
    let output = response.projects[0].output.as_ref().unwrap();
    assert_eq!(output.commits, Some(4));
}

#[test]
fn shared_history_is_deduplicated_within_and_across_projects() {
    let source = init_repo();
    commit(&source.repo, "shared.txt", "shared\n", "shared commit");
    let clone = clone_repo(&source.repo);
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let response = query_project_stats(&request(
        vec![
            project(
                "alpha",
                vec![
                    repo_spec("primary", &source.repo),
                    repo_spec("clone", &clone.repo),
                ],
                Vec::new(),
            ),
            project("beta", vec![repo_spec("copy", &clone.repo)], Vec::new()),
        ],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_eq!(
        response.projects[0].output.as_ref().unwrap().commits,
        Some(1)
    );
    assert_eq!(
        response.projects[1].output.as_ref().unwrap().commits,
        Some(1)
    );
    assert_eq!(response.portfolio.output.as_ref().unwrap().commits, Some(1));
    assert_eq!(response.projects[0].coverage.configured_repos, 1);
    let primary = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "primary")
        .unwrap();
    let cloned = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "clone")
        .unwrap();
    assert_eq!(primary.output.as_ref().unwrap().commits, Some(1));
    assert_eq!(cloned.output.as_ref().unwrap().commits, Some(1));
}

#[test]
fn record_repos_are_visible_and_excluded_from_code_totals() {
    let code = init_repo();
    commit(&code.repo, "code.txt", "code\n", "code commit");
    let records = init_repo();
    commit(&records.repo, "rec.txt", "rec\n", "record commit");
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &code.repo)],
            vec![repo_spec("records", &records.repo)],
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let project = &response.projects[0];
    assert_eq!(project.output.as_ref().unwrap().commits, Some(1));
    let record = project
        .repos
        .iter()
        .find(|repo| repo.tier == ProjectStatsRepoTierWire::Records)
        .unwrap();
    assert_eq!(record.output.as_ref().unwrap().commits, Some(1));
    assert_eq!(project.coverage.configured_repos, 1);
}

#[test]
fn half_open_range_and_non_monotonic_clocks() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "start.txt",
        "s\n",
        "at start",
        1_600_000_000,
        1_700_000_000,
    );
    commit_at(
        &fixture.repo,
        "end.txt",
        "e\n",
        "at end",
        1_700_000_050,
        1_700_000_100,
    );
    commit_at(
        &fixture.repo,
        "outside.txt",
        "o\n",
        "committer outside",
        1_700_000_040,
        1_699_000_000,
    );
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        1_700_000_000,
        1_700_000_100,
        1_700_000_200,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let project = &response.projects[0];
    let output = project.output.as_ref().unwrap();
    let lifetime = project.lifetime.as_ref().unwrap();
    assert_eq!(output.commits, Some(1));
    assert_eq!(lifetime.commits, Some(3));
    assert_eq!(lifetime.first_commit_ts, Some(1_600_000_000));
}

#[test]
fn new_york_dst_author_days_are_bucketed_locally() {
    let fixture = init_repo();
    let before = New_York
        .with_ymd_and_hms(2026, 3, 7, 23, 30, 0)
        .unwrap()
        .timestamp();
    let after = New_York
        .with_ymd_and_hms(2026, 3, 8, 3, 30, 0)
        .unwrap()
        .timestamp();
    commit_at(
        &fixture.repo,
        "before.txt",
        "b\n",
        "before spring forward",
        before,
        before,
    );
    commit_at(
        &fixture.repo,
        "after.txt",
        "a\n",
        "after spring forward",
        after,
        after,
    );
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        0,
        after + 10,
        after + 86_400,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let lifetime = response.projects[0].lifetime.as_ref().unwrap();
    assert_eq!(lifetime.active_days, Some(2));
}

#[test]
fn mixed_available_unavailable_and_partial_sources() {
    let available = init_repo();
    commit(&available.repo, "ok.txt", "ok\n", "ok");
    let heavy = init_repo();
    for index in 0..16 {
        commit(
            &heavy.repo,
            &format!("f-{index}.txt"),
            &format!("{index}\n"),
            &format!("commit {index}"),
        );
    }
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let mut req = request(
        vec![project(
            "alpha",
            vec![
                repo_spec("ok", &available.repo),
                repo_spec("missing", Path::new("/no/such/repo")),
                repo_spec("partial", &heavy.repo),
            ],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    );
    req.budget.max_output_bytes = 256;
    let response = query_project_stats(&req).unwrap();
    let project = &response.projects[0];
    assert!(project.output.as_ref().unwrap().commits.unwrap_or(0) >= 1);
    assert!(project.coverage.unavailable_repos >= 1);
    assert!(project.coverage.partial_repos >= 1);
    assert!(project.coverage.measured_repos >= 1);
    assert!(project.repos.iter().any(|repo| {
        repo.status == ProjectStatsRepoStatusWire::Unavailable
    }));
    assert!(project
        .repos
        .iter()
        .any(|repo| repo.status == ProjectStatsRepoStatusWire::Partial));
    assert!(response
        .issues
        .iter()
        .any(|issue| issue.code == "repo_unavailable"
            || issue.code == "history_truncated"));
}

#[test]
fn all_facets_serialize_with_explicit_nulls_for_unimplemented_seams() {
    let fixture = init_repo();
    commit(&fixture.repo, "file.txt", "one\n", "one");
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let mut req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    );
    req.facets = vec![
        ProjectStatsFacetWire::Output,
        ProjectStatsFacetWire::Code,
        ProjectStatsFacetWire::Runs,
    ];
    let response = query_project_stats(&req).unwrap();
    let json = serde_json::to_value(&response).unwrap();
    assert!(json["projects"][0]["output"].is_object());
    assert!(json["projects"][0]["lifetime"].is_object());
    assert_eq!(json["projects"][0]["code"], serde_json::Value::Null);
    assert_eq!(json["projects"][0]["runs"], serde_json::Value::Null);
    assert_eq!(json["portfolio"]["code"], serde_json::Value::Null);
    assert_eq!(json["portfolio"]["runs"], serde_json::Value::Null);
    assert!(json["projects"][0]["output"]["plans_committed"].is_null());
}

#[test]
fn cold_warm_and_cache_disabled_results_share_fingerprint_and_data() {
    let fixture = init_repo();
    commit(
        &fixture.repo,
        "file.txt",
        "one\n",
        &agent_message("stamped", "writer", "anthropic/opus"),
    );
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let cached = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    );
    let cold = query_project_stats(&cached).unwrap();
    let warm = query_project_stats(&cached).unwrap();
    let mut disabled = cached.clone();
    disabled.cache_dir.clear();
    let uncached = query_project_stats(&disabled).unwrap();
    assert_eq!(cold.fingerprint, warm.fingerprint);
    assert_eq!(cold.fingerprint, uncached.fingerprint);
    assert_eq!(cold.projects[0].output, warm.projects[0].output);
    assert_eq!(cold.projects[0].output, uncached.projects[0].output);
    assert_eq!(cold.projects[0].lifetime, warm.projects[0].lifetime);
}

#[test]
fn empty_repository_is_measured_zero() {
    let fixture = init_repo();
    let cache = cache_dir();
    let response = query_repo(&fixture.repo, cache.path());
    let output = response.projects[0].output.as_ref().unwrap();
    let lifetime = response.projects[0].lifetime.as_ref().unwrap();
    assert_eq!(output.commits, Some(0));
    assert_eq!(output.agent_share, None);
    assert_eq!(lifetime.commits, Some(0));
    assert_eq!(lifetime.first_commit_ts, None);
    assert_eq!(
        response.projects[0].repos[0].status,
        ProjectStatsRepoStatusWire::Ok
    );
}

fn ref_spec(id: &str, path: &Path, refs: &[&str]) -> ProjectStatsRepoSpecWire {
    let mut spec = repo_spec(id, path);
    spec.ref_candidates = refs.iter().map(|item| (*item).to_owned()).collect();
    spec
}

fn named_spec(
    id: &str,
    name: &str,
    path: &Path,
    refs: &[&str],
) -> ProjectStatsRepoSpecWire {
    let mut spec = ref_spec(id, path, refs);
    spec.name = name.to_owned();
    spec
}

fn divergent_fixture() -> super::test_support::Fixture {
    let fixture = init_repo();
    commit(&fixture.repo, "root.txt", "root\n", "root");
    git(&fixture.repo, &["checkout", "-b", "side"]);
    commit(&fixture.repo, "side.txt", "side\n", "side");
    git(&fixture.repo, &["checkout", "main"]);
    commit(&fixture.repo, "main.txt", "main\n", "main");
    fixture
}

fn assert_output_invariants(response: &ProjectStatsResponseWire) {
    for project in &response.projects {
        if let Some(output) = &project.output {
            check_output_invariants(output).unwrap();
        }
    }
    if let Some(output) = &response.portfolio.output {
        check_output_invariants(output).unwrap();
    }
}

#[test]
fn divergent_refs_union_history_without_inflating_coverage() {
    let fixture = divergent_fixture();
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let main_side = vec![
        ref_spec("main", &fixture.repo, &["main"]),
        ref_spec("side", &fixture.repo, &["side"]),
    ];
    let side_main = vec![
        ref_spec("side", &fixture.repo, &["side"]),
        ref_spec("main", &fixture.repo, &["main"]),
    ];
    let first = query_project_stats(&request(
        vec![project("alpha", main_side, Vec::new())],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let second = query_project_stats(&request(
        vec![project("alpha", side_main, Vec::new())],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_output_invariants(&first);
    assert_eq!(first.projects[0].output.as_ref().unwrap().commits, Some(3));
    assert_eq!(first.portfolio.output.as_ref().unwrap().commits, Some(3));
    assert_eq!(
        first.projects[0].lifetime.as_ref().unwrap().commits,
        Some(3)
    );
    assert_eq!(first.projects[0].coverage.configured_repos, 1);
    assert_eq!(first.projects[0].coverage.measured_repos, 1);
    assert_output_invariants(&second);
    assert_eq!(
        first.projects[0].output.as_ref().unwrap().commits,
        second.projects[0].output.as_ref().unwrap().commits
    );
    assert_eq!(first.projects[0].coverage, second.projects[0].coverage);
    assert_eq!(
        first.portfolio.output.as_ref().unwrap().commits,
        second.portfolio.output.as_ref().unwrap().commits
    );
}

#[test]
fn divergent_refs_across_projects_union_in_the_portfolio() {
    let fixture = divergent_fixture();
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let response = query_project_stats(&request(
        vec![
            project(
                "alpha",
                vec![ref_spec("main", &fixture.repo, &["main"])],
                Vec::new(),
            ),
            project(
                "beta",
                vec![ref_spec("side", &fixture.repo, &["side"])],
                Vec::new(),
            ),
        ],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_output_invariants(&response);
    assert_eq!(
        response.projects[0].output.as_ref().unwrap().commits,
        Some(2)
    );
    assert_eq!(
        response.projects[1].output.as_ref().unwrap().commits,
        Some(2)
    );
    assert_eq!(response.portfolio.output.as_ref().unwrap().commits, Some(3));
    assert_eq!(response.portfolio.coverage.configured_repos, 1);
}

#[test]
fn same_tip_aliases_keep_their_selected_ref_names() {
    let fixture = init_repo();
    commit(&fixture.repo, "file.txt", "one\n", "one");
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_spec("main", "main-alias", &fixture.repo, &["main"]),
                named_spec("head", "head-alias", &fixture.repo, &["HEAD"]),
            ],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let main = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "main")
        .unwrap();
    let head = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "head")
        .unwrap();
    assert_eq!(main.reference.as_ref().unwrap().name, "main");
    assert_eq!(head.reference.as_ref().unwrap().name, "HEAD");
    assert_eq!(
        main.reference.as_ref().unwrap().commit,
        head.reference.as_ref().unwrap().commit
    );
    assert_eq!(
        response.projects[0].output.as_ref().unwrap().commits,
        Some(1)
    );
}

#[test]
fn repeated_missing_paths_keep_their_issue_names() {
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let missing = Path::new("/no/such/project-stats-repo");
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_spec("one", "display-one", missing, &["main"]),
                named_spec("two", "display-two", missing, &["main"]),
            ],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let one = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "one")
        .unwrap();
    let two = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "two")
        .unwrap();
    assert_eq!(
        one.issue.as_ref().and_then(|issue| issue.repo.as_deref()),
        Some("display-one")
    );
    assert_eq!(
        two.issue.as_ref().and_then(|issue| issue.repo.as_deref()),
        Some("display-two")
    );
    assert_eq!(response.projects[0].output.as_ref().unwrap().commits, None);
    assert!(response.projects[0]
        .output
        .as_ref()
        .unwrap()
        .models
        .is_empty());
    assert_eq!(
        response.projects[0].lifetime.as_ref().unwrap().commits,
        None
    );
}

#[test]
fn two_clones_share_coverage_identity_and_keep_isolated_history() {
    let source = init_repo();
    commit(&source.repo, "shared.txt", "shared\n", "shared");
    let clone = clone_repo(&source.repo);
    commit(&clone.repo, "only-clone.txt", "clone\n", "clone only");
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let first_specs = vec![
        repo_spec("source", &source.repo),
        repo_spec("clone", &clone.repo),
    ];
    let mut reversed = first_specs.clone();
    reversed.reverse();
    let first = query_project_stats(&request(
        vec![project("alpha", first_specs, Vec::new())],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let second = query_project_stats(&request(
        vec![project("alpha", reversed, Vec::new())],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_eq!(first.projects[0].output.as_ref().unwrap().commits, Some(2));
    assert_eq!(first.portfolio.output.as_ref().unwrap().commits, Some(2));
    assert_eq!(first.projects[0].coverage.configured_repos, 1);
    assert_eq!(first.projects[0].coverage.measured_repos, 1);
    assert_eq!(first.projects[0].coverage, second.projects[0].coverage);
    assert_eq!(
        first.projects[0].output.as_ref().unwrap().commits,
        second.projects[0].output.as_ref().unwrap().commits
    );
}

#[test]
fn coverage_uses_worst_health_for_a_shared_identity() {
    let available = init_repo();
    commit(&available.repo, "ok.txt", "ok\n", "ok");
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let mut ok = repo_spec("ok", &available.repo);
    ok.identity = "https://example.test/shared.git".to_owned();
    let holder = tempfile::tempdir().unwrap();
    let shallow_path = holder.path().join("shallow");
    let source_url = format!("file://{}", available.repo.display());
    let clone_output = std::process::Command::new("git")
        .args(["clone", "--depth=1", &source_url])
        .arg(&shallow_path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env_remove("GIT_OBJECT_DIRECTORY")
        .env_remove("GIT_ALTERNATE_OBJECT_DIRECTORIES")
        .env_remove("GIT_COMMON_DIR")
        .env_remove("GIT_NAMESPACE")
        .output()
        .expect("clone shallow");
    assert!(clone_output.status.success());
    let mut shallow_spec = repo_spec("shallow", &shallow_path);
    shallow_spec.identity = ok.identity.clone();
    let forward = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![ok.clone(), shallow_spec.clone()],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let reverse = query_project_stats(&request(
        vec![project("alpha", vec![shallow_spec, ok], Vec::new())],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_eq!(forward.projects[0].coverage.configured_repos, 1);
    assert_eq!(forward.projects[0].coverage.measured_repos, 1);
    assert_eq!(forward.projects[0].coverage.partial_repos, 1);
    assert_eq!(
        forward.projects[0].coverage.partial_repos,
        reverse.projects[0].coverage.partial_repos
    );
}

#[test]
fn all_missing_sources_serialize_null_counts() {
    let cache = cache_dir();
    let (start, end, now) = wide_range();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("missing", Path::new("/no/such/repo"))],
            Vec::new(),
        )],
        start,
        end,
        now,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let json = serde_json::to_value(&response).unwrap();
    assert!(json["projects"][0]["output"].is_object());
    assert_eq!(
        json["projects"][0]["output"]["commits"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["projects"][0]["output"]["agent_commits"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["projects"][0]["lifetime"]["commits"],
        serde_json::Value::Null
    );
    assert_eq!(
        json["portfolio"]["output"]["commits"],
        serde_json::Value::Null
    );
    assert_eq!(json["projects"][0]["code"], serde_json::Value::Null);
    assert!(json["projects"][0]["output"]["models"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn empty_range_on_nonempty_history_is_measured_zero() {
    let fixture = init_repo();
    commit_at(
        &fixture.repo,
        "file.txt",
        "one\n",
        "one",
        1_700_000_000,
        1_700_000_000,
    );
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        1_700_000_100,
        1_700_000_100,
        1_700_000_200,
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert_eq!(
        response.projects[0].output.as_ref().unwrap().commits,
        Some(0)
    );
    assert_eq!(
        response.projects[0].lifetime.as_ref().unwrap().commits,
        Some(1)
    );
}

#[test]
#[ignore = "manual cold/warm measurement; not a CI timing gate"]
fn measured_history_acceptance_on_sase_checkout() {
    let repo = sase_checkout().expect("authorized sase checkout");
    let cache = cache_dir();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let req = request(
        vec![project(
            "sase",
            vec![repo_spec("primary", &repo)],
            Vec::new(),
        )],
        now - 86_400 * 30,
        now + 1,
        now,
        &cache.path().to_string_lossy(),
    );
    let start = std::time::Instant::now();
    let cold = query_project_stats(&req).expect("cold query");
    let cold_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut warm_samples = Vec::new();
    for _ in 0..5 {
        let start = std::time::Instant::now();
        let warm = query_project_stats(&req).expect("warm query");
        warm_samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(cold.fingerprint, warm.fingerprint);
        assert_eq!(cold.projects[0].output, warm.projects[0].output);
        assert_eq!(cold.projects[0].lifetime, warm.projects[0].lifetime);
    }
    warm_samples.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let median = warm_samples[warm_samples.len() / 2];
    let output = cold.projects[0].output.as_ref().unwrap();
    let lifetime = cold.projects[0].lifetime.as_ref().unwrap();
    let tip = cold.projects[0].repos[0]
        .reference
        .as_ref()
        .map(|reference| reference.commit.clone())
        .unwrap_or_default();
    let identity = cold.projects[0].repos[0]
        .reference
        .as_ref()
        .map(|reference| reference.name.clone())
        .unwrap_or_default();
    eprintln!(
        "project_stats measure repo={} identity_ref={} history_commits={} range_commits={} tip={} budget_ms={} max_bytes={} cold_ms={cold_ms:.1} warm_median_ms={median:.1} cache={} includes_extension_startup=false",
        repo.display(),
        identity,
        lifetime.commits.unwrap_or(0),
        output.commits.unwrap_or(0),
        tip,
        req.budget.timeout_ms,
        req.budget.max_output_bytes,
        cache.path().display()
    );
    assert!(
        median < 150.0,
        "warm one-project median {median:.1} ms exceeds 150 ms budget"
    );
}

#[test]
fn acceptance_harness_rejects_core_and_missing_paths() {
    let err = validate_sase_checkout(Path::new("/no/such/sase")).unwrap_err();
    assert!(err.contains("not a git checkout"), "{err}");
    let core = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let err = validate_sase_checkout(&core).unwrap_err();
    assert!(err.contains("sase-core"), "{err}");
}

fn sase_checkout() -> Result<std::path::PathBuf, String> {
    let raw = std::env::var("PROJECT_STATS_SASE_REPO").map_err(|_| {
        "PROJECT_STATS_SASE_REPO must name the primary sase checkout; \
the ignored acceptance test does not fall back to sase-core"
            .to_owned()
    })?;
    let path =
        std::path::PathBuf::from(&raw)
            .canonicalize()
            .map_err(|error| {
                format!(
                "PROJECT_STATS_SASE_REPO={raw} is not a usable path: {error}"
            )
            })?;
    validate_sase_checkout(&path)?;
    Ok(path)
}

fn validate_sase_checkout(path: &Path) -> Result<(), String> {
    let git_dir = path.join(".git");
    if !git_dir.exists() {
        return Err(format!(
            "PROJECT_STATS_SASE_REPO={} is not a git checkout",
            path.display()
        ));
    }
    let looks_like_core = path.join("crates/sase_core").is_dir()
        && !path.join("src/sase").is_dir();
    if looks_like_core {
        return Err(format!(
            "PROJECT_STATS_SASE_REPO={} points at sase-core, not the sase primary checkout",
            path.display()
        ));
    }
    if !path.join("src/sase").is_dir()
        || !path.join("sase-core-revision.txt").is_file()
    {
        return Err(format!(
            "PROJECT_STATS_SASE_REPO={} is not the sase primary repository",
            path.display()
        ));
    }
    Ok(())
}

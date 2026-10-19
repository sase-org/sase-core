use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::aggregate::check_output_invariants;
use super::code::check_code_invariants;
use super::git;
use super::test_support::{clone_repo, commit, commit_files, init_repo, spec};
use super::wire::{
    ProjectStatsBudgetWire, ProjectStatsFacetWire, ProjectStatsFileRoleWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectStateWire,
    ProjectStatsRepoSpecWire, ProjectStatsRepoStatusWire,
    ProjectStatsRepoTierWire, ProjectStatsRequestWire,
    ProjectStatsSelectionWire, PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use super::*;

fn plan_store() -> super::wire::ProjectStatsPlanStoreWire {
    super::wire::ProjectStatsPlanStoreWire {
        storage: super::wire::ProjectStatsPlanStorageWire::InTree,
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

fn named_identity(
    id: &str,
    path: &Path,
    identity: &str,
) -> ProjectStatsRepoSpecWire {
    let mut spec = repo_spec(id, path);
    spec.identity = identity.to_owned();
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
    facets: Vec<ProjectStatsFacetWire>,
    cache_dir: &str,
) -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: 0,
        end_ts: 4_000_000_000,
        now_ts: 4_000_000_000,
        timezone: "America/New_York".to_owned(),
        facets,
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

fn sample_repo() -> super::test_support::Fixture {
    let fixture = init_repo();
    commit_files(
        &fixture.repo,
        &[
            ("src/lib.rs", "pub fn value() -> u8 {\n    1\n}\n"),
            ("tests/lib.rs", "#[test]\nfn it_works() {}\n"),
            ("README.md", "docs\n\n"),
        ],
        "sample",
    );
    fixture
}

fn git_invoked(commands: &[Vec<String>], needle: &[&str]) -> bool {
    commands.iter().any(|args| {
        args.windows(needle.len()).any(|window| {
            window.iter().zip(needle).all(|(got, want)| got == want)
        })
    })
}

#[test]
fn code_only_populates_snapshots_without_output() {
    let fixture = sample_repo();
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let project = &response.projects[0];
    assert!(project.output.is_none());
    assert!(project.lifetime.is_none());
    assert!(project.models.is_empty());
    let code = project.code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.code_files, Some(2));
    assert_eq!(code.test_lines, Some(2));
    assert!(code.test_share.unwrap() > 0.0);
    assert_eq!(code.doc_files, Some(1));
    assert_eq!(code.repos_configured, 1);
    assert_eq!(code.repos_measured, 1);
    let repo = &project.repos[0];
    assert_eq!(repo.status, ProjectStatsRepoStatusWire::Ok);
    let snapshot = repo.snapshot.as_ref().unwrap();
    assert_eq!(snapshot.tracked_files, 3);
    let roles = snapshot.role_counts.as_ref().unwrap();
    let included: u64 = roles.iter().map(|row| row.files).sum();
    let excluded: u64 = snapshot
        .exclusion_counts
        .as_ref()
        .unwrap()
        .iter()
        .map(|row| row.files)
        .sum();
    assert_eq!(
        included + excluded + snapshot.skipped_symlinks.unwrap_or(0),
        snapshot.tracked_files
    );
    assert_eq!(
        response.portfolio.code.as_ref().unwrap().code_files,
        Some(2)
    );
}

#[test]
fn code_only_does_not_walk_history() {
    let fixture = sample_repo();
    let cache = cache_dir();
    let req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    );
    let (response, commands) =
        git::capture_git_commands(|| query_project_stats(&req).unwrap());
    assert!(response.projects[0].code.is_some());
    assert!(!git_invoked(&commands, &["log", "-z"]));
}

#[test]
fn output_only_does_not_read_blobs_or_attributes() {
    let fixture = sample_repo();
    let cache = cache_dir();
    let req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Output],
        &cache.path().to_string_lossy(),
    );
    let (response, commands) =
        git::capture_git_commands(|| query_project_stats(&req).unwrap());
    check_output_invariants(response.projects[0].output.as_ref().unwrap())
        .unwrap();
    assert!(response.projects[0].code.is_none());
    assert!(response.projects[0].repos[0].snapshot.is_none());
    assert!(!git_invoked(&commands, &["cat-file", "--batch"]));
    assert!(!git_invoked(&commands, &["check-attr"]));
}

#[test]
fn records_snapshot_is_metadata_only() {
    let records = init_repo();
    commit(&records.repo, "note.md", "secret body\n", "record");
    let cache = cache_dir();
    let req = request(
        vec![project(
            "alpha",
            Vec::new(),
            vec![repo_spec("records", &records.repo)],
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    );
    let (response, commands) =
        git::capture_git_commands(|| query_project_stats(&req).unwrap());
    let record = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.tier == ProjectStatsRepoTierWire::Records)
        .unwrap();
    let snapshot = record.snapshot.as_ref().unwrap();
    assert_eq!(snapshot.tracked_files, 1);
    assert_eq!(snapshot.lifetime_commits, Some(1));
    assert!(snapshot.tracked_lines.is_none());
    assert!(snapshot.languages.is_none());
    let code = response.projects[0].code.as_ref().unwrap();
    assert_eq!(code.repos_configured, 0);
    assert_eq!(code.code_files, Some(0));
    assert!(!git_invoked(&commands, &["cat-file", "--batch"]));
    assert!(!git_invoked(&commands, &["check-attr"]));
}

#[test]
fn clones_do_not_double_count_and_are_request_order_stable() {
    let source = sample_repo();
    let clone = clone_repo(&source.repo);
    commit(&clone.repo, "src/extra.rs", "pub fn extra() {}\n", "extra");
    let cache = cache_dir();
    let identity = "https://example.test/code.git";
    let forward = vec![
        named_identity("source", &source.repo, identity),
        named_identity("clone", &clone.repo, identity),
    ];
    let mut reverse = forward.clone();
    reverse.reverse();
    let first = query_project_stats(&request(
        vec![project("alpha", forward, Vec::new())],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let second = query_project_stats(&request(
        vec![project("alpha", reverse, Vec::new())],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = first.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.repos_configured, 1);
    assert_eq!(code.repos_measured, 1);
    assert_eq!(first.projects[0].code, second.projects[0].code);
    let disagreement = first
        .issues
        .iter()
        .find(|issue| issue.code == "code_tip_disagreement")
        .expect("successful clone disagreement");
    assert!(first.projects[0].repos.iter().all(|repo| {
        repo.issue
            .as_ref()
            .is_some_and(|issue| issue.code == "code_tip_disagreement")
    }));
    let chosen_path = disagreement
        .message
        .split(" using ")
        .nth(1)
        .expect("chosen clone path")
        .split('@')
        .next()
        .expect("chosen clone path segment");
    let chosen_id = if Path::new(chosen_path)
        == source.repo.canonicalize().expect("source path")
    {
        "source"
    } else {
        assert_eq!(
            Path::new(chosen_path),
            clone.repo.canonicalize().expect("clone path")
        );
        "clone"
    };
    assert_eq!(disagreement.repo.as_deref(), Some(chosen_id));
    assert!(first.projects[0]
        .repos
        .iter()
        .any(|repo| { repo.id == chosen_id && repo.snapshot.is_some() }));
}

#[test]
fn agreeing_clones_do_not_emit_a_tip_disagreement() {
    let source = sample_repo();
    let clone = clone_repo(&source.repo);
    let cache = cache_dir();
    let identity = "https://example.test/agreed.git";
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_identity("source", &source.repo, identity),
                named_identity("clone", &clone.repo, identity),
            ],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert!(!response
        .issues
        .iter()
        .any(|issue| issue.code == "code_tip_disagreement"));
}

#[test]
fn failed_clone_at_another_tip_does_not_trigger_disagreement() {
    let available = sample_repo();
    let broken = init_repo();
    commit(&broken.repo, "src/broken.rs", "fn broken() {}\n", "broken");
    let tree_oid =
        super::test_support::git(&broken.repo, &["rev-parse", "HEAD^{tree}"]);
    let tree_object = broken
        .repo
        .join(".git/objects")
        .join(&tree_oid[..2])
        .join(&tree_oid[2..]);
    fs::remove_file(tree_object).expect("remove broken clone tree object");

    let cache = cache_dir();
    let identity = "https://example.test/failed-tip.git";
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_identity("available", &available.repo, identity),
                named_identity("broken", &broken.repo, identity),
            ],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    assert!(response.projects[0]
        .code
        .as_ref()
        .and_then(|code| code.code_files)
        .is_some());
    assert!(!response
        .issues
        .iter()
        .any(|issue| issue.code == "code_tip_disagreement"));
}

#[test]
fn mixed_availability_keeps_measured_sources() {
    let available = sample_repo();
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_identity(
                    "ok",
                    &available.repo,
                    "https://example.test/ok.git",
                ),
                named_identity(
                    "missing",
                    Path::new("/no/such/code-repo"),
                    "https://example.test/missing.git",
                ),
            ],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.repos_configured, 2);
    assert_eq!(code.repos_measured, 1);
    assert!(code.code_files.unwrap() >= 1);
    assert!(response.projects[0].repos.iter().any(|repo| {
        repo.status == ProjectStatsRepoStatusWire::Unavailable
    }));
    assert!(response.projects[0]
        .repos
        .iter()
        .any(|repo| repo.status == ProjectStatsRepoStatusWire::Ok
            && repo.snapshot.is_some()));
}

#[test]
fn empty_repo_is_measured_zero_and_missing_repo_is_null() {
    let empty = init_repo();
    let cache = cache_dir();
    let empty_response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("empty", &empty.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = empty_response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.code_files, Some(0));
    assert_eq!(code.code_lines, Some(0));
    assert_eq!(code.test_share, None);
    assert_eq!(code.languages.as_ref().map(Vec::len), Some(0));
    assert_eq!(code.repos_measured, 1);
    assert_eq!(
        empty_response.projects[0].repos[0].status,
        ProjectStatsRepoStatusWire::Ok
    );

    let missing = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("missing", Path::new("/no/such/code-repo"))],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = missing.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.code_files, None);
    assert_eq!(code.repos_configured, 1);
    assert_eq!(code.repos_measured, 0);
    assert_eq!(
        missing.projects[0].repos[0].status,
        ProjectStatsRepoStatusWire::Unavailable
    );
}

#[test]
fn role_and_language_sums_and_zero_denominator() {
    let fixture = init_repo();
    commit(&fixture.repo, "README.md", "docs only\n", "docs");
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("docs", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.code_files, Some(0));
    assert_eq!(code.code_lines, Some(0));
    assert_eq!(code.test_share, None);
    assert_eq!(code.doc_files, Some(1));
    let snapshot = response.projects[0].repos[0].snapshot.as_ref().unwrap();
    let docs = snapshot
        .role_counts
        .as_ref()
        .unwrap()
        .iter()
        .find(|row| row.role == ProjectStatsFileRoleWire::Docs)
        .unwrap();
    assert_eq!(docs.files, 1);
}

#[test]
fn fingerprint_ignores_cache_warmth_and_dirty_files() {
    let fixture = sample_repo();
    let cache = cache_dir();
    let mut req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    );
    let cold = query_project_stats(&req).unwrap();
    let warm = query_project_stats(&req).unwrap();
    fs::write(fixture.repo.join("src/lib.rs"), "pub fn dirty() {}\n").unwrap();
    let dirty = query_project_stats(&req).unwrap();
    assert_eq!(cold.fingerprint, warm.fingerprint);
    assert_eq!(cold.fingerprint, dirty.fingerprint);
    assert_eq!(cold.projects[0].code, dirty.projects[0].code);
    req.cache_dir.clear();
    let uncached = query_project_stats(&req).unwrap();
    assert_eq!(cold.fingerprint, uncached.fingerprint);
}

#[test]
fn fingerprint_changes_when_the_selected_tip_moves() {
    let fixture = sample_repo();
    let cache = cache_dir();
    let req = request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &fixture.repo)],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    );
    let before = query_project_stats(&req).unwrap();
    commit(&fixture.repo, "src/more.rs", "pub fn more() {}\n", "more");
    let after = query_project_stats(&req).unwrap();
    assert_ne!(before.fingerprint, after.fingerprint);
    assert_ne!(
        before.projects[0].code.as_ref().unwrap().code_files,
        after.projects[0].code.as_ref().unwrap().code_files
    );
}

#[test]
fn records_do_not_enter_code_totals_or_models() {
    let code = sample_repo();
    let records = init_repo();
    commit(
        &records.repo,
        "record.md",
        "body\n",
        "record commit\n\nSASE_AGENT=writer\nSASE_MODEL=anthropic/opus\n",
    );
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![repo_spec("code", &code.repo)],
            vec![repo_spec("records", &records.repo)],
        )],
        vec![ProjectStatsFacetWire::Output, ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let output = response.projects[0].output.as_ref().unwrap();
    check_output_invariants(output).unwrap();
    assert_eq!(output.commits, Some(1));
    let code = response.projects[0].code.as_ref().unwrap();
    assert_eq!(code.repos_configured, 1);
    assert_eq!(
        response.projects[0]
            .repos
            .iter()
            .filter(|repo| repo.tier == ProjectStatsRepoTierWire::Records)
            .count(),
        1
    );
}

#[test]
fn combined_request_merges_status_without_promoting_failures() {
    let available = sample_repo();
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_identity(
                    "ok",
                    &available.repo,
                    "https://example.test/ok.git",
                ),
                named_identity(
                    "missing",
                    Path::new("/no/such/code-repo"),
                    "https://example.test/missing.git",
                ),
            ],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Output, ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let ok = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "ok")
        .unwrap();
    let missing = response.projects[0]
        .repos
        .iter()
        .find(|repo| repo.id == "missing")
        .unwrap();
    assert_eq!(ok.status, ProjectStatsRepoStatusWire::Ok);
    assert!(ok.output.is_some());
    assert!(ok.snapshot.is_some());
    assert_eq!(missing.status, ProjectStatsRepoStatusWire::Unavailable);
    assert!(missing.snapshot.is_none());
}

#[test]
fn no_configured_code_repos_is_an_empty_population() {
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project("alpha", Vec::new(), Vec::new())],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.code_files, Some(0));
    assert_eq!(code.repos_configured, 0);
    assert_eq!(code.repos_measured, 0);
}

#[test]
fn successful_clone_is_preferred_over_a_failed_clone() {
    let available = sample_repo();
    let cache = cache_dir();
    let identity = "https://example.test/prefer.git";
    let mut missing = repo_spec("missing", Path::new("/no/such/prefer-repo"));
    missing.identity = identity.to_owned();
    let ok = named_identity("ok", &available.repo, identity);
    let response = query_project_stats(&request(
        vec![project("alpha", vec![missing, ok], Vec::new())],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    assert_eq!(code.repos_configured, 1);
    assert_eq!(code.repos_measured, 1);
    assert!(code.code_files.unwrap() >= 1);
}

#[test]
fn languages_merge_from_totals_not_shares() {
    let first = sample_repo();
    let second = init_repo();
    commit_files(
        &second.repo,
        &[
            ("src/main.rs", "fn main() {}\n"),
            ("src/other.rs", "fn other() {}\n"),
        ],
        "second",
    );
    let cache = cache_dir();
    let response = query_project_stats(&request(
        vec![project(
            "alpha",
            vec![
                named_identity(
                    "one",
                    &first.repo,
                    "https://example.test/one.git",
                ),
                named_identity(
                    "two",
                    &second.repo,
                    "https://example.test/two.git",
                ),
            ],
            Vec::new(),
        )],
        vec![ProjectStatsFacetWire::Code],
        &cache.path().to_string_lossy(),
    ))
    .unwrap();
    let code = response.projects[0].code.as_ref().unwrap();
    check_code_invariants(code).unwrap();
    let rust = code
        .languages
        .as_ref()
        .unwrap()
        .iter()
        .find(|row| row.name == "rust")
        .unwrap();
    let file_sum: u64 = response.projects[0]
        .repos
        .iter()
        .filter_map(|repo| repo.snapshot.as_ref())
        .filter_map(|snapshot| snapshot.languages.as_ref())
        .flat_map(|rows| rows.iter())
        .filter(|row| row.name == "rust")
        .map(|row| row.files)
        .sum();
    assert_eq!(rust.files, file_sum);
    assert_eq!(code.repos_configured, 2);
}

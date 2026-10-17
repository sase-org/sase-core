use std::collections::BTreeMap;
use std::path::Path;

use serde_json::{json, Value};
use tempfile::TempDir;

use crate::agent_scan::{
    rebuild_agent_artifact_index, AgentArtifactScanOptionsWire,
};
use crate::project_stats::attribution::REASON_SESSION_AMBIGUOUS;
use crate::project_stats::test_support::{
    commit, commit_files, git, init_repo, spec,
};
use crate::project_stats::wire::{
    ProjectStatsAgentSidecarSpecWire, ProjectStatsBudgetWire,
    ProjectStatsFacetWire, ProjectStatsModelKindWire,
    ProjectStatsPlanStorageWire, ProjectStatsPlanStoreWire,
    ProjectStatsProjectSpecWire, ProjectStatsProjectStateWire,
    ProjectStatsRequestWire, ProjectStatsSelectionWire,
    PROJECT_STATS_WIRE_SCHEMA_VERSION,
};
use crate::project_stats::{query_project_stats, ProjectStatsResponseWire};

fn plan_store() -> ProjectStatsPlanStoreWire {
    ProjectStatsPlanStoreWire {
        storage: ProjectStatsPlanStorageWire::Local,
        repo_path: None,
        plans_dir: "plans".to_owned(),
        project_filter: None,
    }
}

fn project(key: &str, repo: &Path) -> ProjectStatsProjectSpecWire {
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
        plan_store: plan_store(),
    }
}

fn request(
    projects: Vec<ProjectStatsProjectSpecWire>,
    cache_dir: &str,
    sidecar: Option<(&str, &Path)>,
    index: Option<&str>,
    prefix: &str,
) -> ProjectStatsRequestWire {
    ProjectStatsRequestWire {
        schema_version: PROJECT_STATS_WIRE_SCHEMA_VERSION,
        start_ts: 0,
        end_ts: 4_000_000_000,
        now_ts: 4_000_000_000,
        timezone: "America/New_York".to_owned(),
        facets: vec![ProjectStatsFacetWire::Output],
        cache_dir: cache_dir.to_owned(),
        run_index_path: index.map(ToOwned::to_owned),
        agents_sidecars: sidecar
            .map(|(key, path)| ProjectStatsAgentSidecarSpecWire {
                project_key: key.to_owned(),
                path: path.to_string_lossy().into_owned(),
            })
            .into_iter()
            .collect(),
        local_agent_name_prefix: prefix.to_owned(),
        known_model_aliases: BTreeMap::new(),
        record_repo_names: Vec::new(),
        include_run_only_projects: false,
        budget: ProjectStatsBudgetWire::default(),
        projects,
    }
}

fn cache_dir() -> TempDir {
    tempfile::tempdir().expect("cache directory")
}

fn meta_json(
    global_name: &str,
    provider: Option<&str>,
    model: Option<&str>,
    family: Option<&str>,
    role: Option<&str>,
) -> String {
    let mut metadata = serde_json::Map::new();
    if let Some(provider) = provider {
        metadata.insert("llm_provider".into(), json!(provider));
    }
    if let Some(model) = model {
        metadata.insert("model".into(), json!(model));
    }
    if let Some(family) = family {
        metadata.insert("agent_family".into(), json!(family));
    }
    if let Some(role) = role {
        metadata.insert("agent_family_role".into(), json!(role));
    }
    serde_json::to_string(&json!({
        "schema_version": 2,
        "owner": {"username": "bbugyi200", "machine_name": "apollo"},
        "project": {"key": "sase", "name": "sase"},
        "source_run_id": "run-1",
        "local_name": global_name.rsplit('.').next().unwrap_or(global_name),
        "global_name": global_name,
        "metadata": Value::Object(metadata),
    }))
    .unwrap()
}

fn commits_json(sha: &str) -> String {
    serde_json::to_string(&json!({
        "schema_version": 2,
        "source_run_id": "run-1",
        "commits": [{
            "sha": sha,
            "subject": "work",
            "committed_at": 1
        }]
    }))
    .unwrap()
}

fn query(
    code: &Path,
    sidecar: Option<&Path>,
    index: Option<&str>,
    prefix: &str,
    cache: &Path,
) -> ProjectStatsResponseWire {
    query_project_stats(&request(
        vec![project("alpha", code)],
        &cache.to_string_lossy(),
        sidecar.map(|path| ("alpha", path)),
        index,
        prefix,
    ))
    .expect("query")
}

fn add_page(
    repo: &Path,
    name: &str,
    provider: Option<&str>,
    model: Option<&str>,
    family: Option<&str>,
    role: Option<&str>,
    sha: Option<&str>,
) {
    let mut files = vec![(
        format!("agents/{name}/meta.json"),
        meta_json(name, provider, model, family, role),
    )];
    if let Some(sha) = sha {
        files.push((format!("agents/{name}/commits.json"), commits_json(sha)));
    }
    let owned: Vec<(String, String)> = files;
    let refs: Vec<(&str, &str)> = owned
        .iter()
        .map(|(path, text)| (path.as_str(), text.as_str()))
        .collect();
    commit_files(repo, &refs, &format!("publish {name}"));
}

#[test]
fn sha_beats_name_and_code_member_is_preferred() {
    let code = init_repo();
    commit(
        &code.repo,
        "file.txt",
        "one\n",
        "unstamped\n\nSASE_AGENT=writer\n",
    );
    let sha = git(&code.repo, &["rev-parse", "HEAD"]);
    let sidecar = init_repo();
    add_page(
        &sidecar.repo,
        "from-sha",
        Some("openai"),
        Some("gpt"),
        None,
        None,
        Some(&sha),
    );
    add_page(
        &sidecar.repo,
        "writer--code",
        Some("anthropic"),
        Some("opus"),
        Some("writer"),
        Some("code"),
        None,
    );
    add_page(
        &sidecar.repo,
        "writer",
        Some("xai"),
        Some("grok"),
        Some("writer"),
        None,
        None,
    );
    let cache = cache_dir();
    let response =
        query(&code.repo, Some(&sidecar.repo), None, "", cache.path());
    let output = response.projects[0].output.as_ref().unwrap();
    let resolved = output
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.provider.as_deref(), Some("openai"));
    assert_eq!(resolved.model.as_deref(), Some("gpt"));

    let code = init_repo();
    commit(
        &code.repo,
        "file.txt",
        "one\n",
        "unstamped\n\nSASE_AGENT=writer\n",
    );
    let sidecar = init_repo();
    add_page(
        &sidecar.repo,
        "writer--code",
        Some("anthropic"),
        Some("opus"),
        Some("writer"),
        Some("code"),
        None,
    );
    add_page(
        &sidecar.repo,
        "writer",
        Some("xai"),
        Some("grok"),
        Some("writer"),
        None,
        None,
    );
    let cache = cache_dir();
    let response =
        query(&code.repo, Some(&sidecar.repo), None, "", cache.path());
    let resolved = response.projects[0]
        .output
        .as_ref()
        .unwrap()
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.model.as_deref(), Some("opus"));
}

#[test]
fn empty_page_metadata_and_mixed_family_models() {
    let code = init_repo();
    commit(
        &code.repo,
        "empty.txt",
        "e\n",
        "empty\n\nSASE_AGENT=hollow\n",
    );
    commit(&code.repo, "mixed.txt", "m\n", "mixed\n\nSASE_AGENT=clan\n");
    let sidecar = init_repo();
    add_page(&sidecar.repo, "hollow", None, None, None, None, None);
    add_page(
        &sidecar.repo,
        "one",
        Some("anthropic"),
        Some("opus"),
        Some("clan"),
        None,
        None,
    );
    add_page(
        &sidecar.repo,
        "two",
        Some("anthropic"),
        Some("sonnet"),
        Some("clan"),
        None,
        None,
    );
    let cache = cache_dir();
    let response =
        query(&code.repo, Some(&sidecar.repo), None, "", cache.path());
    let unknown = response.projects[0]
        .output
        .as_ref()
        .unwrap()
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Unknown)
        .unwrap();
    assert_eq!(unknown.commits, 2);
    assert!(unknown
        .unknown_reasons
        .iter()
        .any(|reason| reason == "page_metadata_empty"));
    assert!(unknown
        .unknown_reasons
        .iter()
        .any(|reason| reason == REASON_SESSION_AMBIGUOUS));
}

fn write_json(path: &Path, payload: Value) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

#[test]
fn index_only_agent_resolves_and_published_page_wins() {
    let tmp = tempfile::tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_json(
        &projects
            .join("alpha")
            .join("artifacts")
            .join("ace-run")
            .join("20260710010000")
            .join("agent_meta.json"),
        json!({
            "name": "writer",
            "run_started_at": "2026-07-10T01:00:00Z",
            "llm_provider": "openai",
            "model": "gpt"
        }),
    );
    write_json(
        &projects
            .join("alpha")
            .join("artifacts")
            .join("ace-run")
            .join("20260710010000")
            .join("done.json"),
        json!({"outcome": "completed", "finished_at": 1_783_644_660.0}),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let code = init_repo();
    commit(
        &code.repo,
        "file.txt",
        "one\n",
        "unstamped\n\nSASE_AGENT=bbugyi200.apollo.writer\n",
    );
    let cache = cache_dir();
    let response = query(
        &code.repo,
        None,
        Some(&index.to_string_lossy()),
        "bbugyi200.apollo.",
        cache.path(),
    );
    let resolved = response.projects[0]
        .output
        .as_ref()
        .unwrap()
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.provider.as_deref(), Some("openai"));
    assert_eq!(resolved.model.as_deref(), Some("gpt"));

    let sidecar = init_repo();
    add_page(
        &sidecar.repo,
        "bbugyi200.apollo.writer",
        Some("anthropic"),
        Some("opus"),
        None,
        None,
        None,
    );
    let cache = cache_dir();
    let response = query(
        &code.repo,
        Some(&sidecar.repo),
        Some(&index.to_string_lossy()),
        "bbugyi200.apollo.",
        cache.path(),
    );
    let resolved = response.projects[0]
        .output
        .as_ref()
        .unwrap()
        .models
        .iter()
        .find(|row| row.kind == ProjectStatsModelKindWire::Resolved)
        .unwrap();
    assert_eq!(resolved.model.as_deref(), Some("opus"));
}

#[test]
#[ignore = "manual cold/warm directory measurement; not a CI timing gate"]
fn measured_directory_on_sase_agents_sidecar() {
    let sidecar = std::env::var("PROJECT_STATS_AGENTS_SIDECAR").expect(
        "PROJECT_STATS_AGENTS_SIDECAR must name the sase agents sidecar",
    );
    let sidecar = Path::new(&sidecar);
    assert!(
        sidecar.is_dir(),
        "agents sidecar is missing: {}",
        sidecar.display()
    );
    let cache = cache_dir();
    let req = request(
        Vec::new(),
        &cache.path().to_string_lossy(),
        Some(("sase", sidecar)),
        None,
        "",
    );
    let start = std::time::Instant::now();
    let cold = crate::project_stats::directory::load(&req);
    let cold_ms = start.elapsed().as_secs_f64() * 1000.0;
    let mut warm_samples = Vec::new();
    let mut warm = cold.clone();
    for _ in 0..5 {
        let start = std::time::Instant::now();
        warm = crate::project_stats::directory::load(&req);
        warm_samples.push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(cold.directory.by_name.len(), warm.directory.by_name.len());
        assert_eq!(cold.directory.by_sha.len(), warm.directory.by_sha.len());
    }
    warm_samples.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let median = warm_samples[warm_samples.len() / 2];
    eprintln!(
        "project_stats directory sidecar={} pages={} shas={} issues={} cold_ms={cold_ms:.1} warm_median_ms={median:.1} cache={}",
        sidecar.display(),
        warm.directory.by_name.len(),
        warm.directory.by_sha.len(),
        warm.issues.len(),
        cache.path().display()
    );
}

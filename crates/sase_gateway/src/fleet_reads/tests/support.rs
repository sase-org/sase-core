//! Shared fixtures for fleet-read tests.

use std::{fs, path::Path};

use chrono::Utc;
use sase_core::{
    agent_scan::AgentArtifactScanOptionsWire,
    fleet_contract::{
        FleetCatalogQueryWire, FleetCatalogScopeWire,
        FLEET_CONTRACT_SCHEMA_VERSION,
    },
};
use serde_json::json;
use tempfile::{tempdir, TempDir};

use super::super::service::FleetReadService;

pub(super) fn write_json(path: &Path, payload: serde_json::Value) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, serde_json::to_string(&payload).unwrap()).unwrap();
}

pub(super) fn seed_home() -> (TempDir, FleetReadService) {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    // Recent-relative-to-now timestamps: the fleet presentation policy
    // windows terminal presentation to the last seven days, so a fixed
    // historical date would eventually fall outside that window and
    // make these seeded rows silently vanish.
    let now = Utc::now();
    let alpha_ts = (now - chrono::Duration::minutes(2))
        .format("%Y%m%d%H%M%S")
        .to_string();
    let beta_ts = (now - chrono::Duration::minutes(1))
        .format("%Y%m%d%H%M%S")
        .to_string();
    seed_agent(&projects, &alpha_ts, "alpha", "alpha output");
    seed_agent(&projects, &beta_ts, "beta", "beta output");
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    (temp, FleetReadService::new(home))
}

pub(super) fn seed_project(projects: &Path, name: &str) {
    let project = projects.join(name);
    fs::create_dir_all(&project).unwrap();
    write_json(
        &project.join("placeholder.json"),
        json!({"created_for": "fleet_read_test"}),
    );
    fs::write(
        project.join(format!("{name}.sase")),
        format!(
            "NAME: {name}\nWORKSPACE_DIR: {}\nPROJECT_STATE: enabled\n",
            project.display()
        ),
    )
    .unwrap();
}

pub(super) fn seed_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    output: &str,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    fs::write(artifact.join("output.txt"), output).unwrap();
    write_json(
        &artifact.join("agent_meta.json"),
        json!({
            "name": name,
            "model": "gpt-5",
            "llm_provider": "codex",
            "output_path": "output.txt"
        }),
    );
    // Use this test process's own PID so the host liveness observer
    // can resolve `Alive` for ordinary current agents. Records without
    // a workspace-claim shape still pass the process classifier; a
    // `NotProcess` or identity-mismatch active-tier record is no
    // longer served as current.
    write_json(
        &artifact.join("running.json"),
        json!({"pid": std::process::id()}),
    );
}

pub(super) fn seed_done_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    output: &str,
    finished_at: f64,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    fs::write(artifact.join("output.txt"), output).unwrap();
    write_json(
        &artifact.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": finished_at,
            "name": name,
            "model": "gpt-5",
            "llm_provider": "codex",
            "output_path": "output.txt"
        }),
    );
}

/// Seed an active-tier record whose PID (`0`) resolves deterministically
/// to `NotProcess` liveness, optionally as a tracked child of `parent`.
pub(super) fn seed_dead_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    parent: Option<&str>,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    let mut meta = json!({"name": name});
    if let Some(parent) = parent {
        meta["parent_timestamp"] = json!(parent);
    }
    write_json(&artifact.join("agent_meta.json"), meta);
    write_json(&artifact.join("running.json"), json!({"pid": 0}));
}

pub(super) fn seed_done_family_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    family: &str,
    parent: Option<&str>,
    finished_at: f64,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    fs::write(artifact.join("output.txt"), "done output").unwrap();
    write_json(
        &artifact.join("agent_meta.json"),
        family_meta(name, family, parent),
    );
    write_json(
        &artifact.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": finished_at,
            "name": name,
            "model": "gpt-5",
            "llm_provider": "codex",
            "output_path": "output.txt"
        }),
    );
}

pub(super) fn seed_dead_family_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    family: &str,
    parent: Option<&str>,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    write_json(
        &artifact.join("agent_meta.json"),
        family_meta(name, family, parent),
    );
    write_json(&artifact.join("running.json"), json!({"pid": 0}));
}

pub(super) fn seed_alive_family_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    family: &str,
    parent: Option<&str>,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    write_json(
        &artifact.join("agent_meta.json"),
        family_meta(name, family, parent),
    );
    write_json(
        &artifact.join("running.json"),
        json!({"pid": std::process::id()}),
    );
}

pub(super) fn seed_waiting_agent(projects: &Path, timestamp: &str, name: &str) {
    seed_dead_agent(projects, timestamp, name, None);
    write_json(
        &projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(timestamp)
            .join("waiting.json"),
        json!({}),
    );
}

pub(super) fn seed_protected_family_agent(
    projects: &Path,
    timestamp: &str,
    name: &str,
    family: &str,
    parent: Option<&str>,
    marker: &str,
) {
    let artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(timestamp);
    fs::create_dir_all(&artifact).unwrap();
    write_json(
        &artifact.join("agent_meta.json"),
        family_meta(name, family, parent),
    );
    write_json(&artifact.join("running.json"), json!({"pid": 0}));
    write_json(&artifact.join(marker), json!({}));
}

pub(super) fn family_meta(
    name: &str,
    family: &str,
    parent: Option<&str>,
) -> serde_json::Value {
    let mut meta = json!({
        "name": name,
        "agent_family": family
    });
    if let Some(parent) = parent {
        meta["parent_timestamp"] = json!(parent);
    }
    meta
}

/// Seed an alive agent whose owner-written prompt file spans several
/// lines, the ordinary shape produced by every real agent launch.
pub(super) fn seed_agent_with_raw_prompt(
    projects: &Path,
    timestamp: &str,
    name: &str,
    prompt: &str,
) {
    seed_agent(projects, timestamp, name, "output");
    fs::write(
        projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(timestamp)
            .join("raw_xprompt.md"),
        prompt,
    )
    .unwrap();
}

pub(super) fn build_service(home: &Path, projects: &Path) -> FleetReadService {
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    FleetReadService::new(home.to_path_buf())
}

pub(super) fn recent_timestamp(minutes: i64) -> String {
    (Utc::now() - chrono::Duration::minutes(minutes))
        .format("%Y%m%d%H%M%S")
        .to_string()
}

pub(super) fn catalog_query() -> FleetCatalogQueryWire {
    FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::Presentation,
        snapshot_id: None,
        cursor: None,
        limit: Some(10),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: true,
    }
}

pub(super) fn history_catalog_query(
    limit: u32,
    cursor: Option<String>,
) -> FleetCatalogQueryWire {
    FleetCatalogQueryWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        scope: FleetCatalogScopeWire::History,
        snapshot_id: None,
        cursor,
        limit: Some(limit),
        project_ids: Vec::new(),
        query: None,
        status_buckets: Vec::new(),
        include_terminal: true,
    }
}

pub(super) fn assert_no_paths_or_pids(value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, value) in map {
                let lower = key.to_ascii_lowercase();
                assert!(!lower.contains("path"), "forbidden key {key}");
                assert!(!lower.contains("pid"), "forbidden key {key}");
                assert_no_paths_or_pids(value);
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                assert_no_paths_or_pids(value);
            }
        }
        serde_json::Value::String(text) => {
            assert!(
                !text.contains(temp_root_hint()),
                "forbidden path text {text}"
            );
        }
        _ => {}
    }
}

pub(super) fn temp_root_hint() -> &'static str {
    "/tmp/"
}

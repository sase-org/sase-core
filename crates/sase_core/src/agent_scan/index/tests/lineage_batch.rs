use super::super::last_lineage_pair_load_queries;
use super::super::lineage::{
    resolve_agent_session_dismissal_lineage_per_candidate,
    MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS,
    MAX_RELATED_ARTIFACT_QUERY_ITERATIONS, PAIR_LINEAGE_COLUMNS_SQL,
};
use super::super::*;
use super::support::write_json;
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

fn artifact_at(
    projects: &Path,
    project: &str,
    workflow: &str,
    timestamp: &str,
) -> PathBuf {
    projects
        .join(project)
        .join("artifacts")
        .join(workflow)
        .join(timestamp)
}

fn write_agent(
    projects: &Path,
    project: &str,
    workflow: &str,
    timestamp: &str,
    meta: Value,
) -> PathBuf {
    let dir = artifact_at(projects, project, workflow, timestamp);
    write_json(&dir.join("agent_meta.json"), meta);
    dir
}

fn candidate(
    identity: &str,
    project: &str,
    workflow: &str,
    timestamp: &str,
    seed_definitively_dead: bool,
) -> AgentSessionDismissalLineageCandidateWire {
    AgentSessionDismissalLineageCandidateWire {
        identity: identity.to_string(),
        project_name: project.to_string(),
        workflow_dir_name: workflow.to_string(),
        timestamp: timestamp.to_string(),
        seed_definitively_dead,
    }
}

fn rebuild(index: &Path, projects: &Path) {
    rebuild_agent_artifact_index(
        index,
        projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
}

fn dismiss_suffixes(index: &Path, suffixes: &[&str]) {
    let identities: Vec<AgentCleanupIdentityWire> = suffixes
        .iter()
        .map(|suffix| AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "unknown".to_string(),
            raw_suffix: Some((*suffix).to_string()),
        })
        .collect();
    replace_agent_artifact_index_dismissed_agents(index, &identities).unwrap();
}

fn assert_batched_matches_oracle(
    index: &Path,
    candidates: &[AgentSessionDismissalLineageCandidateWire],
) -> Vec<AgentSessionDismissalLineageResultWire> {
    let batched =
        resolve_agent_session_dismissal_lineage(index, candidates).unwrap();
    let oracle = resolve_agent_session_dismissal_lineage_per_candidate(
        index, candidates,
    )
    .unwrap();
    assert_eq!(
        batched, oracle,
        "batched resolver must match the per-candidate SQL oracle"
    );
    batched
}

fn mixed_fixture(
    projects: &Path,
) -> Vec<AgentSessionDismissalLineageCandidateWire> {
    // Parent chain in alpha/ace-run: root <- member.
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508120000",
        json!({"name": "alpha-root"}),
    );
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508120500",
        json!({
            "name": "alpha-member",
            "parent_timestamp": "20260508120000",
        }),
    );

    // Retry chain in alpha/ace-run.
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508121000",
        json!({
            "name": "retry-root",
            "retry_chain_root_timestamp": "20260508121000",
            "retried_as_timestamp": "20260508121100",
        }),
    );
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508121100",
        json!({
            "name": "retry",
            "retry_of_timestamp": "20260508121000",
            "retry_chain_root_timestamp": "20260508121000",
            "retried_as_timestamp": "20260508121200",
        }),
    );
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508121200",
        json!({
            "name": "retry2",
            "retry_of_timestamp": "20260508121100",
            "retry_chain_root_timestamp": "20260508121000",
        }),
    );

    // Multi-member session whose walk never reaches a parentless row; the
    // agent_session fallback finds the real root in the same pair.
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508122000",
        json!({
            "name": "fallback-root",
            "agent_session": "sess-fallback",
        }),
    );
    write_agent(
        projects,
        "alpha",
        "ace-run",
        "20260508122100",
        json!({
            "name": "fallback-member",
            "agent_session": "sess-fallback",
            "parent_timestamp": "missing-parent",
        }),
    );

    // Dead seed with its own dismissed suffix in alpha/run.
    write_agent(
        projects,
        "alpha",
        "run",
        "20260508123000",
        json!({"name": "killed"}),
    );
    write_json(
        &artifact_at(projects, "alpha", "run", "20260508123000")
            .join("workflow_state.json"),
        json!({
            "workflow_name": "gh",
            "cl_name": "alpha",
            "status": "running",
            "appears_as_agent": true
        }),
    );

    // Second project/workflow pair so the load is O(pairs).
    write_agent(
        projects,
        "beta",
        "ace-run",
        "20260508124000",
        json!({"name": "beta-root"}),
    );
    write_agent(
        projects,
        "beta",
        "ace-run",
        "20260508124100",
        json!({
            "name": "beta-member",
            "parent_timestamp": "20260508124000",
        }),
    );
    write_agent(
        projects,
        "beta",
        "run",
        "20260508125000",
        json!({"name": "beta-run-root"}),
    );

    vec![
        candidate("alpha-root", "alpha", "ace-run", "20260508120000", false),
        candidate("alpha-member", "alpha", "ace-run", "20260508120500", false),
        candidate("retry", "alpha", "ace-run", "20260508121100", false),
        candidate("retry2", "alpha", "ace-run", "20260508121200", false),
        candidate(
            "fallback-member",
            "alpha",
            "ace-run",
            "20260508122100",
            false,
        ),
        candidate("killed-live", "alpha", "run", "20260508123000", false),
        candidate("killed-dead", "alpha", "run", "20260508123000", true),
        candidate("missing", "alpha", "ace-run", "99999999999999", false),
        candidate("beta-member", "beta", "ace-run", "20260508124100", false),
        candidate("beta-run-root", "beta", "run", "20260508125000", false),
    ]
}

#[test]
fn pair_lineage_load_sql_never_selects_record_json() {
    assert!(
        !PAIR_LINEAGE_COLUMNS_SQL.contains("record_json"),
        "the batched pair load must not fetch record_json: {PAIR_LINEAGE_COLUMNS_SQL}"
    );
}

#[test]
fn batched_lineage_matches_per_candidate_oracle_on_mixed_fixture() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let candidates = mixed_fixture(&projects);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild(&index, &projects);
    dismiss_suffixes(
        &index,
        &[
            "20260508120000",
            "20260508121000",
            "20260508122000",
            "20260508123000",
            "20260508124000",
        ],
    );

    let results = assert_batched_matches_oracle(&index, &candidates);
    let dismissed: std::collections::BTreeMap<&str, bool> = results
        .iter()
        .map(|result| {
            (
                result.identity.as_str(),
                result.agent_session_root_dismissed,
            )
        })
        .collect();
    assert!(dismissed["alpha-root"]);
    assert!(dismissed["alpha-member"]);
    assert!(dismissed["retry"]);
    assert!(dismissed["retry2"]);
    assert!(dismissed["fallback-member"]);
    assert!(!dismissed["killed-live"]);
    assert!(dismissed["killed-dead"]);
    assert!(!dismissed["missing"]);
    assert!(dismissed["beta-member"]);
    assert!(!dismissed["beta-run-root"]);
}

#[test]
fn batched_lineage_pair_load_count_equals_distinct_pairs() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let candidates = mixed_fixture(&projects);
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild(&index, &projects);

    let _ =
        resolve_agent_session_dismissal_lineage(&index, &candidates).unwrap();
    let pairs: std::collections::BTreeSet<(&str, &str)> = candidates
        .iter()
        .map(|candidate| {
            (
                candidate.project_name.as_str(),
                candidate.workflow_dir_name.as_str(),
            )
        })
        .collect();
    assert_eq!(
        last_lineage_pair_load_queries(),
        pairs.len() as u64,
        "lineage-column loads must equal distinct (project, workflow) pairs, \
         not the candidate count {}",
        candidates.len()
    );
    assert!(
        pairs.len() < candidates.len(),
        "the fixture must have more candidates than pairs so the count \
         distinguishes O(pairs) from O(candidates)"
    );
}

#[test]
fn batched_lineage_matches_oracle_when_iteration_bound_stops_walk() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let chain_len = MAX_RELATED_ARTIFACT_QUERY_ITERATIONS + 8;
    let mut timestamps = Vec::new();
    for i in 1..=chain_len {
        let timestamp = format!("20260509{:06}", i);
        let meta = if i == 1 {
            json!({"name": "bound-root"})
        } else {
            json!({
                "name": format!("bound-{i}"),
                "parent_timestamp": format!("20260509{:06}", i - 1),
            })
        };
        write_agent(projects.as_path(), "alpha", "ace-run", &timestamp, meta);
        timestamps.push(timestamp);
    }
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild(&index, &projects);
    dismiss_suffixes(&index, &[timestamps[0].as_str()]);

    let candidates = vec![candidate(
        "tail",
        "alpha",
        "ace-run",
        timestamps.last().unwrap(),
        false,
    )];
    let results = assert_batched_matches_oracle(&index, &candidates);
    assert!(
        !results[0].agent_session_root_dismissed,
        "a walk that stops at the iteration bound must not invent a \
         dismissed root: {results:?}"
    );
}

#[test]
fn batched_lineage_matches_oracle_when_timestamp_bound_caps_expansion() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root = "20260510120000";
    write_agent(
        &projects,
        "alpha",
        "ace-run",
        root,
        json!({"name": "star-root"}),
    );
    let child_count = MAX_RELATED_ARTIFACT_LINEAGE_TIMESTAMPS + 20;
    let mut children = Vec::new();
    for i in 1..=child_count {
        let timestamp = format!("20260510{:06}", i);
        write_agent(
            &projects,
            "alpha",
            "ace-run",
            &timestamp,
            json!({
                "name": format!("star-{i}"),
                "parent_timestamp": root,
            }),
        );
        children.push(timestamp);
    }
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild(&index, &projects);
    dismiss_suffixes(&index, &[root]);

    let mut candidates =
        vec![candidate("root", "alpha", "ace-run", root, false)];
    candidates.push(candidate(
        "first-child",
        "alpha",
        "ace-run",
        &children[0],
        false,
    ));
    candidates.push(candidate(
        "last-child",
        "alpha",
        "ace-run",
        children.last().unwrap(),
        false,
    ));
    let results = assert_batched_matches_oracle(&index, &candidates);
    assert!(
        results
            .iter()
            .all(|result| result.agent_session_root_dismissed),
        "the timestamp cap must not change the dismissed-root verdict \
         when the parentless root is already in the seed set: {results:?}"
    );
}

use super::super::*;
use super::support::{
    artifact, artifact_for_project, timestamps_from_artifact_dirs,
    write_gate_shell_artifact, write_json,
};
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::wire::AgentArtifactScanOptionsWire;
use rusqlite::Connection;
use serde_json::json;
use std::collections::BTreeMap;
use tempfile::tempdir;

#[test]
fn related_artifact_dirs_follow_retry_and_parent_lineage() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root = artifact(&projects, "20260504120000");
    let followup = artifact(&projects, "20260504120500");
    let retry = artifact(&projects, "20260504121000");
    let retry2 = artifact(&projects, "20260504121500");
    let unrelated = artifact(&projects, "20260504122000");
    write_json(
        &root.join("agent_meta.json"),
        json!({
            "name": "root",
            "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
            "retried_as_timestamp": retry.file_name().unwrap().to_string_lossy(),
        }),
    );
    write_json(
        &followup.join("agent_meta.json"),
        json!({
            "name": "followup",
            "parent_timestamp": root.file_name().unwrap().to_string_lossy(),
        }),
    );
    write_json(
        &retry.join("agent_meta.json"),
        json!({
            "name": "retry",
            "retry_of_timestamp": root.file_name().unwrap().to_string_lossy(),
            "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
            "retried_as_timestamp": retry2.file_name().unwrap().to_string_lossy(),
        }),
    );
    write_json(
        &retry2.join("agent_meta.json"),
        json!({
            "name": "retry2",
            "retry_of_timestamp": retry.file_name().unwrap().to_string_lossy(),
            "retry_chain_root_timestamp": root.file_name().unwrap().to_string_lossy(),
        }),
    );
    write_json(
        &unrelated.join("agent_meta.json"),
        json!({"name": "unrelated", "parent_timestamp": "other-root"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let root_related =
        query_related_agent_artifact_dirs(&index, &root, &[]).unwrap();
    assert_eq!(
        timestamps_from_artifact_dirs(&root_related),
        vec![
            "20260504120000",
            "20260504120500",
            "20260504121000",
            "20260504121500",
        ]
    );

    let retry_related =
        query_related_agent_artifact_dirs(&index, &retry, &[]).unwrap();
    assert_eq!(
        timestamps_from_artifact_dirs(&retry_related),
        vec![
            "20260504121000",
            "20260504120000",
            "20260504120500",
            "20260504121500",
        ]
    );
}

#[test]
fn resolve_agent_session_dismissal_lineage_follows_parent_to_dismissed_root() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root = artifact(&projects, "20260505120000");
    let member = artifact(&projects, "20260505120500");
    let unrelated = artifact(&projects, "20260505121000");
    write_json(&root.join("agent_meta.json"), json!({"name": "root"}));
    write_json(
        &member.join("agent_meta.json"),
        json!({
            "name": "member",
            "parent_timestamp": root.file_name().unwrap().to_string_lossy(),
        }),
    );
    write_json(
        &unrelated.join("agent_meta.json"),
        json!({"name": "unrelated", "parent_timestamp": "other-root"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let candidates = vec![
        AgentSessionDismissalLineageCandidateWire {
            identity: "root".to_string(),
            project_name: "proj".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: "20260505120000".to_string(),
            seed_definitively_dead: false,
        },
        AgentSessionDismissalLineageCandidateWire {
            identity: "member".to_string(),
            project_name: "proj".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: "20260505120500".to_string(),
            seed_definitively_dead: false,
        },
        AgentSessionDismissalLineageCandidateWire {
            identity: "unrelated".to_string(),
            project_name: "proj".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: "20260505121000".to_string(),
            seed_definitively_dead: false,
        },
    ];

    let before =
        resolve_agent_session_dismissal_lineage(&index, &candidates).unwrap();
    assert!(
        before
            .iter()
            .all(|result| !result.agent_session_root_dismissed),
        "nothing is dismissed yet: {before:?}"
    );

    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "unknown".to_string(),
            raw_suffix: Some("20260505120000".to_string()),
        }],
    )
    .unwrap();

    let after =
        resolve_agent_session_dismissal_lineage(&index, &candidates).unwrap();
    let dismissed_by_identity: BTreeMap<&str, bool> = after
        .iter()
        .map(|result| {
            (
                result.identity.as_str(),
                result.agent_session_root_dismissed,
            )
        })
        .collect();
    assert!(dismissed_by_identity["root"]);
    assert!(dismissed_by_identity["member"]);
    assert!(
        !dismissed_by_identity["unrelated"],
        "a parent pointer to a record outside the index must never be \
         treated as a dismissed root"
    );
}

#[test]
fn resolve_agent_session_dismissal_lineage_honors_dead_seed_own_dismissal() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let killed = artifact(&projects, "20260505130000");
    write_json(&killed.join("agent_meta.json"), json!({"name": "killed"}));
    // A force-killed workflow run never finalizes its workflow state, so
    // the index still classifies an active `workflow` record while the
    // kill path recorded a `run` dismissal for it.
    write_json(
        &killed.join("workflow_state.json"),
        json!({
            "workflow_name": "gh",
            "cl_name": "proj",
            "status": "running",
            "appears_as_agent": true
        }),
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "proj".to_string(),
            raw_suffix: Some("20260505130000".to_string()),
        }],
    )
    .unwrap();
    let candidate =
        |seed_definitively_dead| AgentSessionDismissalLineageCandidateWire {
            identity: "killed".to_string(),
            project_name: "proj".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            timestamp: "20260505130000".to_string(),
            seed_definitively_dead,
        };

    let unproven =
        resolve_agent_session_dismissal_lineage(&index, &[candidate(false)])
            .unwrap();
    assert!(
        !unproven[0].agent_session_root_dismissed,
        "without liveness evidence an active record keeps the strict \
         identity match: {unproven:?}"
    );
    let dead =
        resolve_agent_session_dismissal_lineage(&index, &[candidate(true)])
            .unwrap();
    assert!(
        dead[0].agent_session_root_dismissed,
        "a definitively dead record honors its own dismissal: {dead:?}"
    );
}

#[test]
fn wait_completed_records_are_indexed_as_running() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260513120000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({
            "name": "active",
            "pid": 123,
            "wait_completed_at": "2026-05-13T16:00:00Z",
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let conn = Connection::open(&index).unwrap();
    let status: String = conn
        .query_row(
            "SELECT status FROM agent_artifacts WHERE artifact_dir = ?1",
            [artifact_dir.to_string_lossy().as_ref()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "running");
}

#[test]
fn find_gate_shell_by_gate_id_uses_indexed_lookup_not_full_decode() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for n in 0..40 {
        write_gate_shell_artifact(
            &projects,
            "proj",
            &format!("2026081210{n:04}"),
            &format!("unrelated-{n}"),
        );
    }
    let target = write_gate_shell_artifact(
        &projects,
        "proj",
        "20260812999999",
        "gate-target",
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let found = find_gate_shell_by_gate_id(&index, Some("proj"), "gate-target")
        .unwrap()
        .expect("gate-target must resolve");
    assert_eq!(found.artifact_dir, target.to_string_lossy());
    assert_eq!(
        last_gate_shell_lookup_records_decoded(),
        1,
        "an indexed exact lookup must decode only the matched row, \
         regardless of how many unrelated gate shells are indexed"
    );
}

#[test]
fn find_gate_shell_by_gate_id_returns_none_for_unknown_id() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_gate_shell_artifact(&projects, "proj", "20260812100000", "gate-1");
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let found =
        find_gate_shell_by_gate_id(&index, Some("proj"), "no-such-gate")
            .unwrap();
    assert!(found.is_none());
    assert_eq!(last_gate_shell_lookup_records_decoded(), 0);
}

#[test]
fn find_gate_shell_by_gate_id_ignores_inherited_id_on_descendant() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let owner = write_gate_shell_artifact(
        &projects,
        "proj",
        "20260812100000",
        "gate-1",
    );
    // A follow-up agent launched after the gate settles inherits the
    // same on-disk `gate_id` but is not itself a gate-shell member: its
    // `agent_session_role` is not "gate".
    write_json(
        &artifact_for_project(&projects, "proj", "20260812100100")
            .join("agent_meta.json"),
        json!({
            "name": "follow-up",
            "agent_family": "approvals",
            "agent_family_role": "code",
            "gate_id": "gate-1",
            "gate_kind": "approval",
            "gate_state": "answered"
        }),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let found = find_gate_shell_by_gate_id(&index, Some("proj"), "gate-1")
        .unwrap()
        .expect("gate-1 must resolve to its owning shell");
    assert_eq!(found.artifact_dir, owner.to_string_lossy());
}

#[test]
fn find_gate_shell_by_gate_id_respects_project_scoping() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let alpha = write_gate_shell_artifact(
        &projects,
        "alpha",
        "20260812100000",
        "gate-shared",
    );
    write_gate_shell_artifact(
        &projects,
        "beta",
        "20260812200000",
        "gate-shared",
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let scoped =
        find_gate_shell_by_gate_id(&index, Some("alpha"), "gate-shared")
            .unwrap()
            .expect("alpha's gate must resolve even though beta's is newer");
    assert_eq!(scoped.artifact_dir, alpha.to_string_lossy());
    assert_eq!(scoped.project_name, "alpha");

    let unscoped = find_gate_shell_by_gate_id(&index, None, "gate-shared")
        .unwrap()
        .expect("an unscoped search must still resolve one match");
    assert_eq!(
        unscoped.project_name, "beta",
        "newest-first across projects"
    );
}

#[test]
fn find_gate_shell_by_gate_id_prefers_newest_real_shell() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    write_gate_shell_artifact(&projects, "proj", "20260812100000", "gate-dup");
    let newest = write_gate_shell_artifact(
        &projects,
        "proj",
        "20260812200000",
        "gate-dup",
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let found = find_gate_shell_by_gate_id(&index, Some("proj"), "gate-dup")
        .unwrap()
        .expect("gate-dup must resolve");
    assert_eq!(found.artifact_dir, newest.to_string_lossy());
}

#[test]
fn schema_v30_upgrade_adds_and_backfills_gate_shell_id_projection() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let owner = write_gate_shell_artifact(
        &projects,
        "proj",
        "20260812100000",
        "gate-legacy",
    );
    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    {
        let conn = Connection::open(&index).unwrap();
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_agent_artifacts_gate_shell_id;
             ALTER TABLE agent_artifacts DROP COLUMN gate_shell_id;
             INSERT OR REPLACE INTO meta(key, value)
             VALUES ('schema_version', '30');",
        )
        .unwrap();
    }

    let found = find_gate_shell_by_gate_id(&index, Some("proj"), "gate-legacy")
        .unwrap()
        .expect(
            "an index predating the gate_shell_id column must \
                 self-migrate and still resolve the gate",
        );
    assert_eq!(found.artifact_dir, owner.to_string_lossy());

    let conn = Connection::open(&index).unwrap();
    let version: String = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(version, AGENT_ARTIFACT_INDEX_SCHEMA_VERSION.to_string());
}

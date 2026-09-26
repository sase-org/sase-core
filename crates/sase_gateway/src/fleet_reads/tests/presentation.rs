//! Record-resolution and presentation tests: agent session selection,
//! owner facts, and served-set membership.

use std::{fs, sync::Arc};

use chrono::Utc;
use sase_core::{
    agent_scan::{AgentArtifactRecordWire, AgentArtifactScanOptionsWire},
    fleet_contract::{
        FleetCatalogQueryWire, FleetCatalogScopeWire, FleetStatusBucketWire,
        OwnerLivenessWire, FLEET_CONTRACT_SCHEMA_VERSION,
    },
    host_liveness::OwnerProcessObservation,
};
use serde_json::json;
use tempfile::tempdir;

use super::super::resolution::{parse_record_timestamp, parse_rfc3339_unix};
use super::super::service::SNAPSHOT_REFRESH_TIMEOUT;
use super::super::*;
use super::support::*;

#[tokio::test]
async fn dead_active_leftovers_are_demoted_and_window_bounded() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let recent_ts = (now - chrono::Duration::minutes(5))
        .format("%Y%m%d%H%M%S")
        .to_string();
    let old_ts = (now - chrono::Duration::days(8))
        .format("%Y%m%d%H%M%S")
        .to_string();
    seed_dead_agent(&projects, &recent_ts, "recent-dead", None);
    seed_dead_agent(&projects, &old_ts, "old-dead", None);
    sase_core::rebuild_agent_artifact_index(
        &home.join("agent_artifact_index.sqlite"),
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let service = FleetReadService::new(home);

    let all = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();

    let recent_row = all
        .page
        .rows
        .iter()
        .find(|row| row.labels.agent_label.as_deref() == Some("recent-dead"))
        .expect("recent dead-active leftover should still be served");
    assert_eq!(recent_row.status_bucket, FleetStatusBucketWire::Stopped);
    assert_eq!(recent_row.liveness, OwnerLivenessWire::NotProcess);
    assert!(
        !all.page
            .rows
            .iter()
            .any(|row| row.labels.agent_label.as_deref() == Some("old-dead")),
        "a leftover outside the seven-day window must be excluded: {:?}",
        all.page.rows
    );
}

#[tokio::test]
async fn orphan_terminal_agent_session_members_are_hidden_from_presentation() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let missing_root_ts = (now - chrono::Duration::minutes(30))
        .format("%Y%m%d%H%M%S")
        .to_string();
    for index in 0..7 {
        let timestamp = (now - chrono::Duration::minutes(index + 1))
            .format("%Y%m%d%H%M%S")
            .to_string();
        seed_dead_agent_session_agent(
            &projects,
            &timestamp,
            &format!("lane--gate-{index}"),
            "lane",
            Some(&missing_root_ts),
        );
    }
    let service = build_service(&home, &projects);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    assert!(
        presentation.page.rows.is_empty(),
        "orphan terminal members must not synthesize an agent session row: {:?}",
        presentation.page.rows
    );

    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    assert_eq!(
        history.page.total_matching_rows, 7,
        "explicit history keeps terminal member records reachable"
    );
    assert!(history.page.rows.iter().all(|row| {
        row.labels.agent_session_label.as_deref() == Some("lane")
    }));
}

#[tokio::test]
async fn terminal_agent_session_root_represents_completed_members() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let root_finished = now - chrono::Duration::minutes(10);
    let member_finished = now - chrono::Duration::minutes(5);
    let root_ts = root_finished.format("%Y%m%d%H%M%S").to_string();
    let member_ts = member_finished.format("%Y%m%d%H%M%S").to_string();
    seed_done_agent_session_agent(
        &projects,
        &root_ts,
        "lane",
        "lane",
        None,
        root_finished.timestamp() as f64,
    );
    seed_done_agent_session_agent(
        &projects,
        &member_ts,
        "lane--gate",
        "lane",
        Some(&root_ts),
        member_finished.timestamp() as f64,
    );
    let service = build_service(&home, &projects);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    let labels = presentation
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref())
        .collect::<Vec<_>>();
    assert!(labels.contains(&Some("lane")), "{labels:?}");
    assert!(labels.contains(&Some("lane--gate")), "{labels:?}");
    let member = presentation
        .page
        .rows
        .iter()
        .find(|row| row.labels.agent_label.as_deref() == Some("lane--gate"))
        .unwrap();
    assert_ne!(
        member.agent_session_role,
        sase_core::fleet_contract::FleetAgentSessionRoleWire::Root
    );
    assert_eq!(member.parent_timestamp.as_deref(), Some(root_ts.as_str()));

    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    let labels = history
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref())
        .collect::<Vec<_>>();
    assert!(labels.contains(&Some("lane")));
    assert!(labels.contains(&Some("lane--gate")));
}

#[tokio::test]
async fn root_less_completed_plan_chain_agent_session_is_presented_through_its_plan_shell(
) {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let plan_finished = now - chrono::Duration::minutes(30);
    let gate_finished = now - chrono::Duration::minutes(20);
    let code_finished = now - chrono::Duration::minutes(10);
    let plan_ts = plan_finished.format("%Y%m%d%H%M%S").to_string();
    let gate_ts = gate_finished.format("%Y%m%d%H%M%S").to_string();
    let code_ts = code_finished.format("%Y%m%d%H%M%S").to_string();
    // Production shape: no separate root record. The plan shell is the
    // agent session's first record (no parent) and later shells point at it.
    seed_done_agent_session_agent(
        &projects,
        &plan_ts,
        "chain--plan",
        "chain",
        None,
        plan_finished.timestamp() as f64,
    );
    seed_done_agent_session_agent(
        &projects,
        &gate_ts,
        "chain--gate",
        "chain",
        Some(&plan_ts),
        gate_finished.timestamp() as f64,
    );
    seed_done_agent_session_agent(
        &projects,
        &code_ts,
        "chain--1",
        "chain",
        Some(&plan_ts),
        code_finished.timestamp() as f64,
    );
    let service = build_service(&home, &projects);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    let mut labels = presentation
        .page
        .rows
        .iter()
        .filter_map(|row| row.labels.agent_label.as_deref())
        .collect::<Vec<_>>();
    labels.sort_unstable();
    assert_eq!(labels, vec!["chain--1", "chain--gate", "chain--plan"]);
}

#[tokio::test]
async fn plan_shell_without_parent_timestamp_is_nested_not_a_root() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let root_ts = (now - chrono::Duration::minutes(4))
        .format("%Y%m%d%H%M%S")
        .to_string();
    let plan_ts = (now - chrono::Duration::minutes(3))
        .format("%Y%m%d%H%M%S")
        .to_string();
    seed_alive_agent_session_agent(&projects, &root_ts, "lane", "lane", None);
    let plan_artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(&plan_ts);
    fs::create_dir_all(&plan_artifact).unwrap();
    write_json(
        &plan_artifact.join("agent_meta.json"),
        json!({
            "name": "lane--plan",
            "agent_family": "lane",
            "agent_agent_session_role": "gate",
            "gate_id": "gate-1",
            "gate_state": "pending"
        }),
    );
    write_json(&plan_artifact.join("running.json"), json!({"pid": 0}));
    let service = build_service(&home, &projects);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    let plan = presentation
        .page
        .rows
        .iter()
        .find(|row| row.labels.agent_label.as_deref() == Some("lane--plan"))
        .expect("plan shell served for nesting");
    assert_ne!(
        plan.agent_session_role,
        sase_core::fleet_contract::FleetAgentSessionRoleWire::Root
    );
    assert_eq!(plan.parent_timestamp.as_deref(), Some(root_ts.as_str()));
    assert_eq!(
        plan.logical_locator.agent_session_id.as_deref(),
        Some("lane")
    );
}

#[tokio::test]
async fn history_rows_inherit_owner_presentation_facts_from_agent_session_root()
{
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let root_finished = now - chrono::Duration::minutes(10);
    let child_finished = now - chrono::Duration::minutes(4);
    let child_started = now - chrono::Duration::minutes(6);
    let root_ts = root_finished.format("%Y%m%d%H%M%S").to_string();
    let child_ts = child_finished.format("%Y%m%d%H%M%S").to_string();
    let root_artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(&root_ts);
    fs::create_dir_all(&root_artifact).unwrap();
    fs::write(root_artifact.join("output.txt"), "root output").unwrap();
    write_json(
        &root_artifact.join("agent_meta.json"),
        json!({
            "name": "lane",
            "agent_family": "lane",
            "tribe": "review",
            "clan_tribe": "parity",
            "agent_clan": "fleet",
            "agent_clan_generation": "202609"
        }),
    );
    write_json(
        &root_artifact.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": root_finished.timestamp() as f64,
            "name": "lane",
            "status_label": "ROOT-DONE",
            "output_path": "output.txt"
        }),
    );
    let child_artifact = projects
        .join("proj")
        .join("artifacts")
        .join("ace-run")
        .join(&child_ts);
    fs::create_dir_all(&child_artifact).unwrap();
    fs::write(child_artifact.join("output.txt"), "child output").unwrap();
    write_json(
        &child_artifact.join("agent_meta.json"),
        json!({
            "name": "lane--code",
            "parent_timestamp": root_ts.clone(),
            "run_started_at": child_started.to_rfc3339()
        }),
    );
    write_json(
        &child_artifact.join("done.json"),
        json!({
            "outcome": "completed",
            "finished_at": child_finished.timestamp() as f64,
            "name": "lane--code",
            "status_label": "CHILD-DONE",
            "output_path": "output.txt"
        }),
    );
    let service = build_service(&home, &projects);

    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    let child = history
        .page
        .rows
        .iter()
        .find(|row| row.labels.agent_label.as_deref() == Some("lane--code"))
        .expect("history child row");

    assert_eq!(child.status, "CHILD-DONE");
    assert_eq!(
        child.agent_session_role,
        sase_core::fleet_contract::FleetAgentSessionRoleWire::HistoricalTurn
    );
    assert_eq!(
        child.logical_locator.agent_session_id.as_deref(),
        Some("lane")
    );
    assert_eq!(child.parent_timestamp.as_deref(), Some(root_ts.as_str()));
    assert_eq!(child.tribe.as_deref(), Some("review"));
    assert_eq!(child.clan_tribe.as_deref(), Some("parity"));
    assert_eq!(child.started_at_unix, parse_record_timestamp(&root_ts));
    assert_eq!(
        child.run_started_at_unix,
        parse_rfc3339_unix(&child_started.to_rfc3339())
    );
}

#[tokio::test]
async fn active_and_protected_members_remain_visible_without_root() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let missing_root_ts = (now - chrono::Duration::minutes(30))
        .format("%Y%m%d%H%M%S")
        .to_string();
    seed_alive_agent_session_agent(
        &projects,
        &(now - chrono::Duration::minutes(3))
            .format("%Y%m%d%H%M%S")
            .to_string(),
        "lane--worker",
        "lane",
        Some(&missing_root_ts),
    );
    seed_protected_agent_session_agent(
        &projects,
        &(now - chrono::Duration::minutes(2))
            .format("%Y%m%d%H%M%S")
            .to_string(),
        "lane--waiting",
        "lane",
        Some(&missing_root_ts),
        "waiting.json",
    );
    seed_protected_agent_session_agent(
        &projects,
        &(now - chrono::Duration::minutes(1))
            .format("%Y%m%d%H%M%S")
            .to_string(),
        "lane--question",
        "lane",
        Some(&missing_root_ts),
        "pending_question.json",
    );
    let service = build_service(&home, &projects);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    let labels = presentation
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref())
        .collect::<Vec<_>>();
    assert!(labels.contains(&Some("lane--worker")), "{labels:?}");
    assert!(labels.contains(&Some("lane--waiting")), "{labels:?}");
    assert!(labels.contains(&Some("lane--question")), "{labels:?}");

    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    let history_labels = history
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref())
        .collect::<Vec<_>>();
    assert!(history_labels.contains(&Some("lane--worker")));
    assert!(history_labels.contains(&Some("lane--waiting")));
    assert!(history_labels.contains(&Some("lane--question")));
}

#[tokio::test]
async fn owner_served_set_matches_visible_identity_set() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let live_ts = recent_timestamp(4);
    let waiting_ts = recent_timestamp(3);
    let dismissed_ts = recent_timestamp(2);
    let recycled_ts = recent_timestamp(1);
    seed_agent(&projects, &live_ts, "live", "live output");
    seed_waiting_agent(&projects, &waiting_ts, "waiting-dead");
    seed_dead_agent(&projects, &dismissed_ts, "dismissed", None);
    seed_agent(&projects, &recycled_ts, "recycled", "recycled output");
    let index = home.join("agent_artifact_index.sqlite");
    sase_core::rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    sase_core::replace_agent_artifact_index_dismissed_agents(
        &index,
        &[sase_core::AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "unknown".to_string(),
            raw_suffix: Some(dismissed_ts.clone()),
        }],
    )
    .unwrap();

    let observations = std::collections::BTreeMap::from([
        ("live".to_string(), OwnerProcessObservation::Alive),
        ("waiting-dead".to_string(), OwnerProcessObservation::Dead),
        ("dismissed".to_string(), OwnerProcessObservation::Alive),
        (
            "recycled".to_string(),
            OwnerProcessObservation::IdentityMismatch,
        ),
    ]);
    let service = FleetReadService::new_with_liveness(
        home,
        SNAPSHOT_REFRESH_TIMEOUT,
        Arc::new(move |record: &AgentArtifactRecordWire| {
            let name = record
                .agent_meta
                .as_ref()
                .and_then(|meta| meta.name.clone())
                .unwrap_or_default();
            observations
                .get(&name)
                .copied()
                .unwrap_or(OwnerProcessObservation::Unknown)
        }),
    );

    let presentation = service.catalog(catalog_query()).await.unwrap();
    let labels = presentation
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref().unwrap_or(""))
        .collect::<Vec<_>>();
    assert!(
        labels.contains(&"live"),
        "catalog must serve the ordinary live identity: {labels:?}"
    );
    assert!(
            labels.contains(&"waiting-dead"),
            "catalog must keep the recent dead protected row as terminal: {labels:?}"
        );
    assert!(
        !labels.contains(&"dismissed"),
        "catalog must not serve the dismissed identity: {labels:?}"
    );
    assert!(
        !labels.contains(&"recycled"),
        "catalog must not serve the recycled-PID identity: {labels:?}"
    );
    assert_eq!(labels.len(), 2, "unexpected extra identities: {labels:?}");
    let waiting = presentation
        .page
        .rows
        .iter()
        .find(|row| row.labels.agent_label.as_deref() == Some("waiting-dead"))
        .unwrap();
    assert_eq!(waiting.liveness, OwnerLivenessWire::Dead);

    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    let history_labels = history
        .page
        .rows
        .iter()
        .map(|row| row.labels.agent_label.as_deref().unwrap_or(""))
        .collect::<Vec<_>>();
    assert!(
        history_labels.contains(&"live"),
        "history keeps the live identity: {history_labels:?}"
    );
    assert!(
        history_labels.contains(&"waiting-dead"),
        "history keeps the recent dead leftover: {history_labels:?}"
    );
    assert!(
        !history_labels.contains(&"dismissed"),
        "history must not resurrect the dismissed identity: {history_labels:?}"
    );
    assert!(
            !history_labels.contains(&"recycled"),
            "history must not resurrect the recycled-PID identity: {history_labels:?}"
        );
}

#[tokio::test]
async fn dead_orphan_of_dismissed_agent_session_is_excluded_from_catalog() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let now = Utc::now();
    let root_ts = (now - chrono::Duration::minutes(10))
        .format("%Y%m%d%H%M%S")
        .to_string();
    let member_ts = (now - chrono::Duration::minutes(5))
        .format("%Y%m%d%H%M%S")
        .to_string();
    seed_dead_agent(&projects, &root_ts, "root", None);
    seed_dead_agent(&projects, &member_ts, "member", Some(&root_ts));
    let index = home.join("agent_artifact_index.sqlite");
    sase_core::rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    sase_core::replace_agent_artifact_index_dismissed_agents(
        &index,
        &[sase_core::AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "unknown".to_string(),
            raw_suffix: Some(root_ts.clone()),
        }],
    )
    .unwrap();
    let service = FleetReadService::new(home);

    let all = service
        .catalog(FleetCatalogQueryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            scope: FleetCatalogScopeWire::Presentation,
            snapshot_id: None,
            cursor: None,
            limit: Some(10),
            project_ids: Vec::new(),
            query: None,
            status_buckets: Vec::new(),
            include_terminal: true,
        })
        .await
        .unwrap();

    assert!(
        all.page.rows.is_empty(),
        "a dead root and its dead member must both be excluded once the \
             agent session root is dismissed: {:?}",
        all.page.rows
    );
}

#[tokio::test]
async fn dead_dismissed_workflow_run_is_excluded_from_catalog() {
    let temp = tempdir().unwrap();
    let home = temp.path().to_path_buf();
    let projects = home.join("projects");
    seed_project(&projects, "proj");
    let timestamp = recent_timestamp(10);
    seed_dead_agent(&projects, &timestamp, "killed", None);
    // A force-killed `#gh:` workflow run never finalizes its workflow
    // state, so the owner index still sees an active `workflow` record
    // while the kill path recorded a `run` dismissal for it.
    write_json(
        &projects
            .join("proj")
            .join("artifacts")
            .join("ace-run")
            .join(&timestamp)
            .join("workflow_state.json"),
        json!({
            "workflow_name": "gh",
            "cl_name": "proj",
            "status": "running",
            "appears_as_agent": true
        }),
    );
    let index = home.join("agent_artifact_index.sqlite");
    sase_core::rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let before = FleetReadService::new(home.clone())
        .catalog(catalog_query())
        .await
        .unwrap();
    assert_eq!(
        before.page.rows.len(),
        1,
        "an undismissed dead run stays presentable as recent terminal"
    );

    sase_core::replace_agent_artifact_index_dismissed_agents(
        &index,
        &[sase_core::AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "proj".to_string(),
            raw_suffix: Some(timestamp.clone()),
        }],
    )
    .unwrap();
    let service = FleetReadService::new(home);

    let presentation = service.catalog(catalog_query()).await.unwrap();
    assert!(
        presentation.page.rows.is_empty(),
        "a definitively dead run must honor its own dismissal even while \
             its workflow state still claims running: {:?}",
        presentation.page.rows
    );
    let history = service
        .catalog(history_catalog_query(10, None))
        .await
        .unwrap();
    assert!(
        history.page.rows.is_empty(),
        "history applies the same dismissal filter: {:?}",
        history.page.rows
    );
}

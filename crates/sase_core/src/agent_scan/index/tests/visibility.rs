use super::super::dismissal::insert_dismissed_identities;
use super::super::maintenance::upsert_record;
use super::super::storage::open_index;
use super::super::*;
use super::support::{
    artifact, fixture_dead_agent_session_record, reconcile_n_plus_one,
    write_json,
};
use crate::agent_cleanup::AgentCleanupIdentityWire;
use crate::agent_scan::wire::{
    AgentArtifactRecordShapeWire, AgentArtifactScanOptionsWire,
};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::Path;
use tempfile::tempdir;

#[test]
fn tier1_active_query_is_bounded_to_newest_incomplete_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..5 {
        let artifact_dir =
            artifact(&projects, &format!("2026051312000{index}"));
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("stale-{index}")}),
        );
    }

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: Some(2),
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let timestamps: Vec<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(timestamps, vec!["20260513120003", "20260513120004"]);
}

#[test]
fn recent_completed_limit_does_not_bound_active_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..3 {
        let artifact_dir =
            artifact(&projects, &format!("2026051313000{index}"));
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("active-{index}")}),
        );
    }

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(1),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    assert_eq!(snapshot.records.len(), 3);
}

#[test]
fn active_limit_prioritizes_waiting_rows_over_newer_stale_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    for index in 0..5 {
        let artifact_dir =
            artifact(&projects, &format!("2026051315000{index}"));
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("stale-{index}")}),
        );
    }
    for timestamp in ["20260513140000", "20260513140001"] {
        let artifact_dir = artifact(&projects, timestamp);
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({"name": format!("waiting-{timestamp}")}),
        );
        write_json(
            &artifact_dir.join("waiting.json"),
            json!({"waiting_for": ["review"]}),
        );
    }

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let snapshot = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: Some(2),
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let timestamps: Vec<&str> = snapshot
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(timestamps, vec!["20260513140000", "20260513140001"]);
}

#[test]
fn index_query_wire_round_trips_active_limit() {
    let query: AgentArtifactIndexQueryWire = serde_json::from_value(json!({
        "include_active": true,
        "include_recent_completed": false,
        "include_full_history": false,
        "active_limit": 7,
        "recent_completed_limit": 11,
        "include_hidden": true,
        "freshness": "cached",
    }))
    .unwrap();

    assert_eq!(query.active_limit, Some(7));
    assert_eq!(query.freshness, AgentArtifactIndexFreshnessWire::Cached);
    let payload = serde_json::to_value(&query).unwrap();
    assert_eq!(payload["active_limit"], json!(7));
    assert_eq!(payload["freshness"], json!("cached"));

    let legacy: AgentArtifactIndexQueryWire = serde_json::from_value(json!({
        "include_active": true,
        "include_recent_completed": true,
        "include_full_history": false,
        "recent_completed_limit": 5,
        "include_hidden": false,
    }))
    .unwrap();
    assert_eq!(legacy.active_limit, None);
    assert_eq!(
        legacy.freshness,
        AgentArtifactIndexFreshnessWire::Revalidate
    );
    assert!(!legacy.agents_list_projection);
}

#[test]
fn active_query_excludes_dismissed_identity_after_rebuild() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260514120000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "dismissed-active", "pid": 123}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "unknown".to_string(),
            raw_suffix: Some("20260514120000".to_string()),
        }],
    )
    .unwrap();
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let visible = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(visible.records.is_empty());
}

#[test]
fn dismissal_reconcile_backfills_dead_agent_session_members_only() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root = artifact(&projects, "20260515120000");
    let member = artifact(&projects, "20260515120500");
    let agent_session_fallback_member = artifact(&projects, "20260515121000");
    let dead_active_member = artifact(&projects, "20260515121500");
    let live_member = artifact(&projects, "20260515122000");
    let unknown_member = artifact(&projects, "20260515122500");
    let running_unknown_member = artifact(&projects, "20260515123000");
    let waiting_done_member = artifact(&projects, "20260515123500");
    let question_dead_member = artifact(&projects, "20260515124000");

    write_json(
        &root.join("agent_meta.json"),
        json!({"name": "fam", "cl_name": "fam", "agent_family": "fam"}),
    );
    write_json(
        &root.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam"}),
    );
    write_json(
        &member.join("agent_meta.json"),
        json!({
            "name": "fam--0",
            "cl_name": "fam--0",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000"
        }),
    );
    write_json(
        &member.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam--0"}),
    );
    write_json(
        &agent_session_fallback_member.join("agent_meta.json"),
        json!({
            "name": "fam--code",
            "cl_name": "fam--code",
            "agent_family": "fam",
            "parent_timestamp": "missing-parent"
        }),
    );
    write_json(
        &agent_session_fallback_member.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam--code"}),
    );
    write_json(
        &live_member.join("agent_meta.json"),
        json!({
            "name": "fam--live",
            "cl_name": "fam--live",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000",
            "pid": std::process::id(),
            "run_started_at": "2026-05-15T12:15:00Z"
        }),
    );
    write_json(
        &unknown_member.join("agent_meta.json"),
        json!({
            "name": "fam--unknown",
            "cl_name": "fam--unknown",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000"
        }),
    );
    write_json(
        &dead_active_member.join("agent_meta.json"),
        json!({
            "name": "fam--dead-active",
            "cl_name": "fam--dead-active",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000",
            "pid": 99999999,
            "run_started_at": "2026-05-15T12:30:00Z"
        }),
    );
    write_json(
        &running_unknown_member.join("agent_meta.json"),
        json!({
            "name": "fam--running-unknown",
            "cl_name": "fam--running-unknown",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000"
        }),
    );
    write_json(&running_unknown_member.join("running.json"), json!({}));
    write_json(
        &waiting_done_member.join("agent_meta.json"),
        json!({
            "name": "fam--waiting",
            "cl_name": "fam--waiting",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000"
        }),
    );
    write_json(
        &waiting_done_member.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam--waiting"}),
    );
    write_json(
        &waiting_done_member.join("waiting.json"),
        json!({"waiting_for": ["dependency"]}),
    );
    write_json(
        &question_dead_member.join("agent_meta.json"),
        json!({
            "name": "fam--question",
            "cl_name": "fam--question",
            "agent_family": "fam",
            "parent_timestamp": "20260515120000",
            "pid": 99999999,
            "run_started_at": "2026-05-15T12:40:00Z"
        }),
    );
    write_json(
        &question_dead_member.join("pending_question.json"),
        json!({"session_id": "q1"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "fam".to_string(),
            raw_suffix: Some("20260515120000".to_string()),
        }],
    )
    .unwrap();
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let dry_run =
        reconcile_agent_artifact_index_dismissed_agent_session_members(
            &index, true,
        )
        .unwrap();
    assert_eq!(dry_run.rows_backfilled, 3);
    assert_eq!(dry_run.rows_skipped_live_or_unknown, 5);

    let before = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(before.records.len(), 8, "dry run must not hide rows");

    let applied =
        reconcile_agent_artifact_index_dismissed_agent_session_members(
            &index, false,
        )
        .unwrap();
    assert_eq!(applied.rows_backfilled, 3);

    let after = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let visible_timestamps: Vec<&str> = after
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(
        visible_timestamps.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([
            "20260515122000",
            "20260515122500",
            "20260515123000",
            "20260515123500",
            "20260515124000",
        ]),
    );
}

#[test]
fn dismissal_reconcile_uses_dismissed_parent_suffix_when_root_row_deleted() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let root = artifact(&projects, "20260516120000");
    let member = artifact(&projects, "20260516120500");

    write_json(
        &root.join("agent_meta.json"),
        json!({"name": "fam", "cl_name": "fam", "agent_family": "fam"}),
    );
    write_json(
        &root.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam"}),
    );
    write_json(
        &member.join("agent_meta.json"),
        json!({
            "name": "fam--code",
            "cl_name": "fam--code",
            "agent_family": "fam",
            "parent_timestamp": "20260516120000"
        }),
    );
    write_json(
        &member.join("done.json"),
        json!({"outcome": "completed", "cl_name": "fam--code"}),
    );

    let index = tmp.path().join("agent_artifact_index.sqlite");
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    delete_agent_artifact_index_row(&index, &root).unwrap();
    replace_agent_artifact_index_dismissed_agents(
        &index,
        &[AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "fam".to_string(),
            raw_suffix: Some("20260516120000".to_string()),
        }],
    )
    .unwrap();

    let applied =
        reconcile_agent_artifact_index_dismissed_agent_session_members(
            &index, false,
        )
        .unwrap();
    assert_eq!(applied.rows_backfilled, 1);

    let after = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: true,
            include_full_history: false,
            active_limit: None,
            recent_completed_limit: Some(10),
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(after.records.is_empty());
}

#[test]
fn dismissed_replace_diffs_instead_of_rewriting_the_table() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    let mut identities: Vec<AgentCleanupIdentityWire> = (0..1_000)
        .map(|i| AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "bulk".to_string(),
            raw_suffix: Some(format!("20260516{i:06}")),
        })
        .collect();
    let first =
        replace_agent_artifact_index_dismissed_agents(&index, &identities)
            .unwrap();
    assert_eq!(first.rows_indexed, 1_000);
    assert_eq!(first.rows_deleted, 0);

    let unchanged =
        replace_agent_artifact_index_dismissed_agents(&index, &identities)
            .unwrap();
    assert_eq!(unchanged.rows_indexed, 1_000);
    assert_eq!(unchanged.rows_deleted, 0);
    let unchanged_statements = last_index_sql_statements();
    assert!(
        unchanged_statements <= 4,
        "identical projection must not rewrite the table, got {unchanged_statements} statements"
    );

    identities.push(AgentCleanupIdentityWire {
        agent_type: "run".to_string(),
        cl_name: "bulk".to_string(),
        raw_suffix: Some("20260516999999".to_string()),
    });
    identities.remove(0);
    let delta =
        replace_agent_artifact_index_dismissed_agents(&index, &identities)
            .unwrap();
    assert_eq!(delta.rows_indexed, 1_000);
    assert_eq!(delta.rows_deleted, 1);

    let forced = replace_agent_artifact_index_dismissed_agents_with_force(
        &index,
        &identities,
        true,
    )
    .unwrap();
    assert_eq!(forced.rows_indexed, 1_000);
    assert_eq!(forced.rows_deleted, 1_000);
}

#[test]
fn dismissal_reconcile_is_set_based_on_large_fixture() {
    let tmp = tempdir().unwrap();
    let index = tmp.path().join("agent_artifact_index.sqlite");
    let mut conn = open_index(&index).unwrap();
    let tx = conn.transaction().unwrap();
    let root_ts = "20260515120000";
    upsert_record(
        &tx,
        Path::new("/proj"),
        &fixture_dead_agent_session_record(root_ts, "fam", None, Some("fam")),
    )
    .unwrap();
    let member_count = 10_000usize;
    for i in 0..member_count {
        let timestamp = format!("20260516{i:06}");
        upsert_record(
            &tx,
            Path::new("/proj"),
            &fixture_dead_agent_session_record(
                &timestamp,
                &format!("fam--{i}"),
                Some(root_ts),
                Some("fam"),
            ),
        )
        .unwrap();
    }
    let noise: Vec<AgentCleanupIdentityWire> = (0..40_000)
        .map(|i| AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "noise".to_string(),
            raw_suffix: Some(format!("20260401{i:06}")),
        })
        .collect();
    insert_dismissed_identities(&tx, noise.iter()).unwrap();
    insert_dismissed_identities(
        &tx,
        [AgentCleanupIdentityWire {
            agent_type: "run".to_string(),
            cl_name: "fam".to_string(),
            raw_suffix: Some(root_ts.to_string()),
        }]
        .iter(),
    )
    .unwrap();
    tx.commit().unwrap();
    drop(conn);

    let expected = reconcile_n_plus_one(&index, true);
    let applied =
        reconcile_agent_artifact_index_dismissed_agent_session_members(
            &index, true,
        )
        .unwrap();
    assert_eq!(applied.candidate_rows, member_count as u64);
    assert_eq!(applied.rows_backfilled, member_count as u64);
    assert_eq!(applied.candidate_rows, expected.candidate_rows);
    assert_eq!(applied.rows_backfilled, expected.rows_backfilled);
    assert_eq!(
        applied.rows_already_dismissed,
        expected.rows_already_dismissed
    );
    assert_eq!(
        applied.rows_skipped_live_or_unknown,
        expected.rows_skipped_live_or_unknown
    );
    assert_eq!(
        applied.rows_skipped_no_dismissed_root,
        expected.rows_skipped_no_dismissed_root
    );
    assert_eq!(
        applied.rows_skipped_decode_errors,
        expected.rows_skipped_decode_errors
    );
    let statements = last_index_sql_statements();
    assert!(
        statements < 16,
        "reconcile SQL must not scale with candidate count, got {statements}"
    );
}

#[test]
fn stale_dismissed_suffixes_do_not_consume_active_limit() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let mut dismissed = Vec::new();
    for index in 0..1_000 {
        let timestamp = format!("20260515{index:06}");
        let artifact_dir = artifact(&projects, &timestamp);
        write_json(
            &artifact_dir.join("agent_meta.json"),
            json!({
                "name": format!("stale-dismissed-{index}"),
                "cl_name": "current_shape",
            }),
        );
        dismissed.push(AgentCleanupIdentityWire {
            agent_type: "workflow".to_string(),
            cl_name: "historical_shape".to_string(),
            raw_suffix: Some(timestamp),
        });
    }
    for timestamp in ["20260514000001", "20260514000002"] {
        write_json(
            &artifact(&projects, timestamp).join("agent_meta.json"),
            json!({"name": format!("visible-{timestamp}")}),
        );
    }

    let index = tmp.path().join("agent_artifact_index.sqlite");
    replace_agent_artifact_index_dismissed_agents(&index, &dismissed).unwrap();
    rebuild_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();

    let visible = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: true,
            include_recent_completed: false,
            include_full_history: false,
            active_limit: Some(5),
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    let timestamps: Vec<&str> = visible
        .records
        .iter()
        .map(|record| record.timestamp.as_str())
        .collect();
    assert_eq!(timestamps, vec!["20260514000001", "20260514000002"]);
}

#[test]
fn hidden_inclusive_full_history_can_inspect_dismissed_rows() {
    let tmp = tempdir().unwrap();
    let projects = tmp.path().join("projects");
    let artifact_dir = artifact(&projects, "20260514123000");
    write_json(
        &artifact_dir.join("agent_meta.json"),
        json!({"name": "dismissed-active", "pid": 123}),
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
            cl_name: "unknown".to_string(),
            raw_suffix: Some("20260514123000".to_string()),
        }],
    )
    .unwrap();

    let visible = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: false,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert!(visible.records.is_empty());

    let all = query_agent_artifact_index(
        &index,
        &projects,
        AgentArtifactIndexQueryWire {
            include_active: false,
            include_recent_completed: false,
            include_full_history: true,
            active_limit: None,
            recent_completed_limit: None,
            include_hidden: true,
            freshness: AgentArtifactIndexFreshnessWire::Revalidate,
            only_monitors: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
            window_limit: None,
            candidate_filter: None,
            agents_list_projection: false,
        },
        AgentArtifactScanOptionsWire::default(),
    )
    .unwrap();
    assert_eq!(all.records.len(), 1);
    assert_eq!(all.records[0].timestamp, "20260514123000");
}

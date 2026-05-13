use rusqlite::OptionalExtension;
use sase_core::agent_archive::AgentArchiveSummaryWire;
use sase_core::agent_cleanup::AgentCleanupIdentityWire;
use sase_core::agent_scan::wire::PendingQuestionMarkerWire;
use sase_core::agent_scan::{
    AgentArtifactRecordWire, AgentMetaWire, DoneMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
};
use sase_core::projection::{
    agent_artifact_records, agent_projection_snapshot, apply_agent_event,
    AgentArchiveBundleUpsertedPayload, AgentDismissedIdentityUpsertedPayload,
    AgentEventKind, AgentPurgedPayload, AgentRevivedPayload,
    AgentTombstonePayload, EventSource, EventSourceKind, HostIdentity,
    NewProjectionEvent, ProjectIdentity, ProjectionEvent, ProjectionEventType,
    ProjectionStore,
};
use tempfile::tempdir;

fn source() -> EventSource {
    EventSource::new(EventSourceKind::Test, "projection-phase1c")
}

fn project() -> ProjectIdentity {
    ProjectIdentity {
        id: "sase-core".to_string(),
        root: Some("/tmp/sase-core".to_string()),
    }
}

fn host() -> HostIdentity {
    HostIdentity {
        id: "host-1".to_string(),
        hostname: Some("devhost".to_string()),
    }
}

fn event(
    kind: AgentEventKind,
    payload: serde_json::Value,
    key: &str,
) -> NewProjectionEvent {
    NewProjectionEvent {
        timestamp: "2026-05-13T20:00:00.000Z".to_string(),
        source: source(),
        project: project(),
        host: host(),
        event_type: ProjectionEventType::Agent(kind),
        payload,
        idempotency_key: key.to_string(),
        causality: Default::default(),
    }
}

fn append_agent_event(
    store: &mut ProjectionStore,
    event: NewProjectionEvent,
) -> ProjectionEvent {
    store
        .append_event_with_projection(event, |tx, event| {
            apply_agent_event(tx, event)
        })
        .unwrap()
        .event
}

fn replay_agent_events(events: &[ProjectionEvent]) -> ProjectionStore {
    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    for event in events {
        append_agent_event(
            &mut store,
            NewProjectionEvent {
                timestamp: event.timestamp.clone(),
                source: event.source.clone(),
                project: event.project.clone(),
                host: event.host.clone(),
                event_type: event.event_type.clone(),
                payload: event.payload.clone(),
                idempotency_key: event.idempotency_key.clone(),
                causality: event.causality.clone(),
            },
        );
    }
    store
}

#[test]
fn projection_migrates_agent_artifact_archive_tables() {
    let temp = tempdir().unwrap();
    let store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();

    for table in [
        "agents",
        "agent_attempts",
        "agent_edges",
        "agent_artifacts",
        "agent_archive",
        "dismissed_identities",
        "agent_search_fts",
    ] {
        assert!(table_exists(&store, table), "missing table {table}");
    }
}

#[test]
fn artifact_events_preserve_scanner_wire_and_replay_lifecycle() {
    let running = artifact_record("20260513T200000", "worker", |record| {
        record.running = Some(RunningMarkerWire {
            pid: Some(4242),
            cl_name: Some("sase-3e".to_string()),
            model: Some("gpt-5.5".to_string()),
            llm_provider: Some("codex".to_string()),
            vcs_provider: None,
            workspace_dir: Some("/tmp/sase_100".to_string()),
        });
        record.agent_meta.as_mut().unwrap().run_started_at =
            Some("2026-05-13T20:00:00Z".to_string());
    });
    let done = artifact_record("20260513T200000", "worker", |record| {
        record.has_done_marker = true;
        record.done = Some(DoneMarkerWire {
            outcome: Some("success".to_string()),
            finished_at: Some(1_778_704_800.0),
            cl_name: Some("sase-3e".to_string()),
            model: Some("gpt-5.5".to_string()),
            llm_provider: Some("codex".to_string()),
            name: Some("worker".to_string()),
            response_path: Some("/tmp/response.md".to_string()),
            ..Default::default()
        });
    });

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    let events = vec![
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::ArtifactUpserted,
                serde_json::json!({ "record": running }),
                "artifact-running",
            ),
        ),
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::ArtifactUpserted,
                serde_json::json!({ "record": done.clone() }),
                "artifact-done",
            ),
        ),
    ];

    let snapshot = agent_projection_snapshot(store.connection()).unwrap();
    assert_eq!(
        agent_artifact_records(store.connection()).unwrap(),
        vec![done]
    );
    assert_eq!(snapshot.agents.len(), 1);
    assert_eq!(snapshot.agents[0].status, "done");
    assert_eq!(snapshot.attempts[0].status, "done");
    assert!(snapshot.search[0].summary.contains("worker"));

    let replayed = replay_agent_events(&events);
    assert_eq!(
        agent_projection_snapshot(store.connection()).unwrap(),
        agent_projection_snapshot(replayed.connection()).unwrap()
    );
}

#[test]
fn waiting_question_parent_edges_and_retry_chains_project_from_artifacts() {
    let waiting =
        artifact_record("20260513T201000", "worker.child", |record| {
            record.waiting = Some(WaitingMarkerWire {
                waiting_for: vec!["question".to_string()],
                wait_duration: Some(30.0),
                wait_until: None,
            });
            record.pending_question = Some(PendingQuestionMarkerWire {
                session_id: Some("session-1".to_string()),
                request_path: Some("/tmp/request.json".to_string()),
                submitted_at: Some("2026-05-13T20:10:00Z".to_string()),
            });
            let meta = record.agent_meta.as_mut().unwrap();
            meta.parent_timestamp = Some("20260513T200000".to_string());
            meta.retry_of_timestamp = Some("20260513T200500".to_string());
            meta.retry_chain_root_timestamp =
                Some("20260513T200000".to_string());
            meta.retry_attempt = Some(2);
            record.prompt_steps.push(PromptStepMarkerWire {
                file_name: "prompt_step_1.json".to_string(),
                workflow_name: "workflow".to_string(),
                step_name: "ask".to_string(),
                step_type: "agent".to_string(),
                step_source: None,
                step_index: Some(1),
                total_steps: Some(3),
                parent_step_index: None,
                parent_total_steps: None,
                status: "waiting".to_string(),
                hidden: false,
                is_pre_prompt_step: false,
                embedded_workflow_name: None,
                artifacts_dir: Some(record.artifact_dir.clone()),
                diff_path: None,
                response_path: None,
                error: None,
                traceback: None,
                model: Some("gpt-5.5".to_string()),
                llm_provider: Some("codex".to_string()),
                output: None,
                output_types: None,
            });
        });

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    append_agent_event(
        &mut store,
        event(
            AgentEventKind::ArtifactUpserted,
            serde_json::json!({ "record": waiting }),
            "artifact-waiting",
        ),
    );

    let snapshot = agent_projection_snapshot(store.connection()).unwrap();
    assert_eq!(snapshot.agents[0].status, "waiting");
    assert!(snapshot.agents[0].has_waiting_marker);
    assert!(snapshot.agents[0].has_pending_question);
    assert_eq!(snapshot.attempts[0].attempt_number, 2);
    assert_eq!(
        snapshot.attempts[0].retry_of_timestamp.as_deref(),
        Some("20260513T200500")
    );
    assert!(snapshot.edges.iter().any(|edge| {
        edge.parent_agent_id == "workflow:20260513T200000"
            && edge.child_agent_id == "workflow:worker.child"
            && edge.edge_kind == "parent_child"
            && edge.step_index == Some(1)
    }));
}

#[test]
fn archive_dismiss_revive_purge_and_tombstone_cleanup_replay() {
    let summary = archive_summary();
    let identity = AgentCleanupIdentityWire {
        agent_type: "agent".to_string(),
        cl_name: "sase-3e".to_string(),
        raw_suffix: Some("20260513T203000".to_string()),
    };

    let temp = tempdir().unwrap();
    let mut store =
        ProjectionStore::open(temp.path().join("projection.db")).unwrap();
    let events = vec![
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::ArchiveBundleUpserted,
                serde_json::to_value(AgentArchiveBundleUpsertedPayload {
                    summary: summary.clone(),
                    search_text: Some("dismissed archived worker".to_string()),
                })
                .unwrap(),
                "archive-upsert",
            ),
        ),
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::DismissedIdentityUpserted,
                serde_json::to_value(AgentDismissedIdentityUpsertedPayload {
                    identity: identity.clone(),
                    agent_id: Some(summary.agent_id.clone()),
                    dismissed_at: summary.dismissed_at.clone(),
                    metadata: None,
                })
                .unwrap(),
                "dismissed-identity",
            ),
        ),
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::Revived,
                serde_json::to_value(AgentRevivedPayload {
                    agent_ids: vec![summary.agent_id.clone()],
                    bundle_paths: vec![summary.bundle_path.clone()],
                    revived_at: "2026-05-13T21:00:00Z".to_string(),
                })
                .unwrap(),
                "archive-revive",
            ),
        ),
        append_agent_event(
            &mut store,
            event(
                AgentEventKind::Purged,
                serde_json::to_value(AgentPurgedPayload {
                    agent_ids: Vec::new(),
                    bundle_paths: vec![summary.bundle_path.clone()],
                })
                .unwrap(),
                "archive-purge",
            ),
        ),
    ];

    let snapshot = agent_projection_snapshot(store.connection()).unwrap();
    assert_eq!(snapshot.archive.len(), 1);
    assert_eq!(
        snapshot.archive[0].revived_at.as_deref(),
        Some("2026-05-13T21:00:00Z")
    );
    assert!(snapshot.archive[0].purged);
    assert_eq!(snapshot.dismissed_identities[0].identity, identity);

    let replayed = replay_agent_events(&events);
    assert_eq!(
        agent_projection_snapshot(store.connection()).unwrap(),
        agent_projection_snapshot(replayed.connection()).unwrap()
    );

    append_agent_event(
        &mut store,
        event(
            AgentEventKind::Tombstone,
            serde_json::to_value(AgentTombstonePayload {
                agent_id: Some(summary.agent_id),
                artifact_dir: None,
                bundle_path: Some(summary.bundle_path),
                identity: Some(identity),
            })
            .unwrap(),
            "archive-tombstone",
        ),
    );
    let after_tombstone =
        agent_projection_snapshot(store.connection()).unwrap();
    assert!(after_tombstone.archive.is_empty());
    assert!(after_tombstone.dismissed_identities.is_empty());
}

fn artifact_record(
    timestamp: &str,
    name: &str,
    update: impl FnOnce(&mut AgentArtifactRecordWire),
) -> AgentArtifactRecordWire {
    let mut record = AgentArtifactRecordWire {
        project_name: "sase-core".to_string(),
        project_dir: "/tmp/projects/sase-core".to_string(),
        project_file: "/tmp/projects/sase-core/project.sase".to_string(),
        workflow_dir_name: "workflow".to_string(),
        artifact_dir: format!("/tmp/artifacts/workflow/{timestamp}"),
        timestamp: timestamp.to_string(),
        agent_meta: Some(AgentMetaWire {
            name: Some(name.to_string()),
            artifact_agent_id: Some(name.to_string()),
            changespec_name: Some("sase-3e".to_string()),
            cl_name: Some("sase-3e".to_string()),
            model: Some("gpt-5.5".to_string()),
            llm_provider: Some("codex".to_string()),
            ..Default::default()
        }),
        done: None,
        running: None,
        waiting: None,
        pending_question: None,
        workflow_state: None,
        plan_path: None,
        prompt_steps: Vec::new(),
        raw_prompt_snippet: Some("implement projection".to_string()),
        has_done_marker: false,
    };
    update(&mut record);
    record
}

fn archive_summary() -> AgentArchiveSummaryWire {
    AgentArchiveSummaryWire {
        agent_id: "workflow:archived-worker".to_string(),
        raw_suffix: "20260513T203000".to_string(),
        bundle_path: "/tmp/archive/20260513T203000.json".to_string(),
        cl_name: "sase-3e".to_string(),
        agent_name: Some("archived-worker".to_string()),
        status: "done".to_string(),
        start_time: Some("2026-05-13T20:30:00Z".to_string()),
        dismissed_at: Some("2026-05-13T20:45:00Z".to_string()),
        revived_at: None,
        project_name: Some("sase-core".to_string()),
        model: Some("gpt-5.5".to_string()),
        runtime: Some("codex".to_string()),
        llm_provider: Some("codex".to_string()),
        step_index: Some(0),
        step_name: Some("implement".to_string()),
        step_type: Some("agent".to_string()),
        retry_attempt: 0,
        is_workflow_child: false,
    }
}

fn table_exists(store: &ProjectionStore, table_name: &str) -> bool {
    store
        .connection()
        .query_row(
            "SELECT name FROM sqlite_master WHERE name = ?1",
            [table_name],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .unwrap()
        .is_some()
}

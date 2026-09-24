use super::super::*;
use crate::agent_scan::{
    AgentArtifactRecordShapeWire, AgentArtifactRecordWire, AgentMetaWire,
    DoneMarkerWire, RunningMarkerWire,
};
use serde_json::Value;

pub(super) fn id(hex: char) -> String {
    format!(
        "{FLEET_INSTALLATION_ID_PREFIX}{}",
        hex.to_string().repeat(64)
    )
}

pub(super) fn origin(hex: char) -> OriginLocatorWire {
    OriginLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        installation_id: id(hex),
    }
}

pub(super) fn logical(hex: char, agent: &str) -> LogicalAgentLocatorWire {
    LogicalAgentLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project: ProjectLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: origin(hex),
            project_id: "project-1".to_string(),
        },
        agent_id: agent.to_string(),
        agent_session_id: Some("family-1".to_string()),
    }
}

pub(super) fn exact(
    hex: char,
    agent: &str,
    run: &str,
) -> AgentInstanceLocatorWire {
    AgentInstanceLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical: logical(hex, agent),
        shell_id: "shell-1".to_string(),
        run_id: run.to_string(),
        attempt_id: "attempt-1".to_string(),
    }
}

pub(super) fn revision(
    locator: &LogicalAgentLocatorWire,
    revision: u64,
) -> ResourceRevisionWire {
    ResourceRevisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_key: logical_key_unchecked(locator),
        revision,
    }
}

pub(super) fn caps(resource: &[&str]) -> CapabilitySetWire {
    CapabilitySetWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        resource: resource.iter().map(|value| (*value).to_string()).collect(),
        host: Vec::new(),
        protocol: Vec::new(),
    }
}

pub(super) fn handle(locator: &LogicalAgentLocatorWire) -> ContentHandleWire {
    ContentHandleWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        id: "transcript-1".to_string(),
        kind: ContentHandleKindWire::Transcript,
        revision: Some(revision(locator, 1)),
        digest: Some("a".repeat(64)),
        byte_len: Some(42),
        supports_range: true,
        supports_growth: true,
    }
}

pub(super) fn authoritative_counts(
    running: u64,
    total: u64,
    observed_at_unix_max: Option<f64>,
) -> FleetLogicalAgentCountsWire {
    FleetLogicalAgentCountsWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        basis: FleetCountBasisWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            input_rows: total,
            selected_rows: total,
            max_revision: Some(total),
            observed_at_unix_max,
        },
        logical_agent_total: total,
        running,
        waiting: 0,
        attention: 0,
        occupied_runner_slots: running,
    }
}

pub(super) fn record_running() -> AgentArtifactRecordWire {
    AgentArtifactRecordWire {
        project_name: "SASE".to_string(),
        project_dir: "/tmp/project".to_string(),
        project_file: "/tmp/project.sase".to_string(),
        workflow_dir_name: "ace-run".to_string(),
        artifact_dir: "/tmp/artifacts/20260906120000".to_string(),
        timestamp: "20260906120000".to_string(),
        agent_meta: Some(AgentMetaWire {
            name: Some("athena.worker".to_string()),
            model: Some("gpt-5".to_string()),
            llm_provider: Some("codex".to_string()),
            agent_session: Some("family-1".to_string()),
            ..AgentMetaWire::default()
        }),
        done: None,
        running: Some(RunningMarkerWire {
            pid: Some(1234),
            model: Some("gpt-5".to_string()),
            llm_provider: Some("codex".to_string()),
            workspace_dir: Some("/tmp/workspace".to_string()),
            ..RunningMarkerWire::default()
        }),
        waiting: None,
        pending_question: None,
        workflow_state: None,
        plan_path: None,
        prompt_steps: Vec::new(),
        raw_prompt_snippet: Some("Implement the approved plan".to_string()),
        used_xprompts: Vec::new(),
        has_done_marker: false,
        record_shape: AgentArtifactRecordShapeWire::Full,
    }
}

pub(super) fn summary_done(
    hex: char,
    agent: &str,
    revision_num: u64,
    observed_at_unix: f64,
) -> ResolvedAgentSummaryWire {
    let locator = logical(hex, agent);
    let mut record = record_running();
    record.running = None;
    record.done = Some(DoneMarkerWire {
        outcome: Some("completed".to_string()),
        status_label: Some("DONE".to_string()),
        ..DoneMarkerWire::default()
    });
    record.has_done_marker = true;
    let mut request = projection_request(
        locator,
        Some(exact(hex, agent, "run-1")),
        revision_num,
        record,
    );
    request.owner_facts.liveness = OwnerLivenessWire::Dead;
    request.owner_facts.occupied_runner_slot = false;
    request.owner_facts.capabilities = caps(&[]);
    request.owner_facts.observed_at_unix = observed_at_unix;
    project_resolved_agent_summary(&request).unwrap()
}

pub(super) fn catalog_snapshot_id(
    scope: FleetCatalogScopeWire,
    summaries: &[ResolvedAgentSummaryWire],
) -> String {
    fleet_catalog_snapshot_id(scope, summaries).unwrap()
}

pub(super) fn catalog_cursor(
    scope: FleetCatalogScopeWire,
    snapshot_id: &str,
    offset: usize,
) -> String {
    let scope = match scope {
        FleetCatalogScopeWire::Presentation => "p",
        FleetCatalogScopeWire::History => "h",
    };
    format!("catcur_v1:{scope}:{snapshot_id}:{offset}")
}

pub(super) fn catalog_response(
    page: FleetCatalogPageSelectionWire,
    count_rows: &[ResolvedAgentSummaryWire],
) -> FleetCatalogPageWire {
    let counts = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: count_rows.to_vec(),
    })
    .unwrap();
    FleetCatalogPageWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        cursor: StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: "gen-test".to_string(),
            sequence: 1,
        },
        counts: counts.clone(),
        count_revision: fleet_count_revision(&counts),
        freshness: FleetSnapshotFreshnessWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            freshness: ObservationFreshnessWire::Fresh,
            partial: false,
            refreshed_at_unix: Some(1000.0),
            error: None,
        },
        page,
    }
}

pub(super) fn projection_request(
    locator: LogicalAgentLocatorWire,
    exact_locator: Option<AgentInstanceLocatorWire>,
    revision_num: u64,
    record: AgentArtifactRecordWire,
) -> ResolvedAgentProjectionRequestWire {
    ResolvedAgentProjectionRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        record,
        logical_locator: locator.clone(),
        owner_facts: OwnerResolutionFactsWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            exact_locator,
            row_revision: revision(&locator, revision_num),
            liveness: OwnerLivenessWire::Alive,
            connection_health: ConnectionHealthWire::Online,
            freshness: ObservationFreshnessWire::Fresh,
            observed_at_unix: 1000.0,
            display_status: None,
            started_at_unix: None,
            run_started_at_unix: None,
            stopped_at_unix: None,
            agent_session_id: None,
            parent_timestamp: None,
            workspace_num: None,
            project_label: None,
            agent_clan: None,
            agent_clan_generation: None,
            clan_tribe: None,
            tribe: None,
            presentation: Default::default(),
            row_kind: FleetRowKindWire::AgentShell,
            current_instance: true,
            dismissable: false,
            needs_attention: false,
            occupied_runner_slot: true,
            container_projected_concrete_agent: false,
            capabilities: caps(&["stop"]),
            content_handles: Vec::new(),
        },
    }
}

pub(super) fn singleton(hex: char, agent: &str) -> LogicalAgentLocatorWire {
    LogicalAgentLocatorWire {
        agent_session_id: None,
        ..logical(hex, agent)
    }
}

pub(super) fn operation_key(id: &str) -> ScopedOperationKeyWire {
    ScopedOperationKeyWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        controller_id: "controller-1".to_string(),
        operation_id: id.to_string(),
    }
}

pub(super) fn follow_record(
    locator: LogicalAgentLocatorWire,
    created_by: FollowCreatedByWire,
    state: FollowStateWire,
    timestamp: f64,
) -> FollowRecordWire {
    let logical_key = logical_key_unchecked(&locator);
    FollowRecordWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: locator,
        logical_key,
        created_by,
        state,
        created_at_unix: timestamp,
        updated_at_unix: timestamp,
        activated_at_unix: match state {
            FollowStateWire::Active => Some(timestamp),
            FollowStateWire::Pending => None,
        },
        operation_key: match created_by {
            FollowCreatedByWire::Explicit => None,
            FollowCreatedByWire::Dispatch => Some(operation_key("op-1")),
        },
    }
}

pub(super) fn tombstone(
    locator: LogicalAgentLocatorWire,
    timestamp: f64,
) -> FollowTombstoneWire {
    let logical_key = logical_key_unchecked(&locator);
    FollowTombstoneWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_locator: locator,
        logical_key,
        unfollowed_at_unix: timestamp,
    }
}

pub(super) fn assert_no_forbidden_local_fields(value: &Value) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let key_lower = key.to_ascii_lowercase();
                assert!(!key_lower.contains("path"), "forbidden key {key}");
                assert!(!key_lower.contains("dir"), "forbidden key {key}");
                assert!(!key_lower.contains("pid"), "forbidden key {key}");
                assert!(!key_lower.contains("token"), "forbidden key {key}");
                assert!(
                    !key_lower.contains("authorization"),
                    "forbidden key {key}"
                );
                assert_no_forbidden_local_fields(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_no_forbidden_local_fields(value);
            }
        }
        Value::String(text) => {
            assert!(!text.contains("/tmp/"), "forbidden local path {text}");
            assert!(!text.contains("Bearer "), "forbidden credential {text}");
        }
        _ => {}
    }
}

pub(super) trait SummaryTestExt {
    fn owner_liveness_for_test(&mut self, liveness: OwnerLivenessWire);
}

impl SummaryTestExt for ResolvedAgentSummaryWire {
    fn owner_liveness_for_test(&mut self, liveness: OwnerLivenessWire) {
        self.liveness = liveness;
    }
}

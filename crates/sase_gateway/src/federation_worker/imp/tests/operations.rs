use super::super::*;
use serde_json::json;

fn sample_mutation_request(installation_id: &str) -> FleetMutationRequestWire {
    let target = sase_core::AgentInstanceLocatorWire {
        schema_version: 1,
        logical: sase_core::LogicalAgentLocatorWire {
            schema_version: 1,
            project: sase_core::ProjectLocatorWire {
                schema_version: 1,
                origin: sase_core::OriginLocatorWire {
                    schema_version: 1,
                    installation_id: installation_id.to_string(),
                },
                project_id: "proj".to_string(),
            },
            agent_id: "alpha".to_string(),
            agent_session_id: None,
        },
        turn_id: "ace-run".to_string(),
        run_id: "20260906120000".to_string(),
        attempt_id: "attempt-0".to_string(),
    };
    let logical_key = sase_core::logical_locator_key(&target.logical).unwrap();
    FleetMutationRequestWire {
        schema_version: 1,
        key: sase_core::ScopedOperationKeyWire {
            schema_version: 1,
            controller_id: "controller-1".to_string(),
            operation_id: "op-1".to_string(),
        },
        target_installation_id: installation_id.to_string(),
        intent: sase_core::FleetMutationIntentWire {
            schema_version: 1,
            kind: sase_core::FleetMutationKindWire::Stop,
            row_revision: sase_core::ResourceRevisionWire {
                schema_version: 1,
                logical_key,
                revision: 1,
            },
            target,
            reason: Some("stop".to_string()),
            fork_prompt: None,
            kill_source_first: None,
            follow: false,
        },
        payload_fingerprint: sase_core::PayloadFingerprintWire {
            schema_version: 1,
            sha256: "a".repeat(64),
        },
        acceptance_window_seconds: 30.0,
    }
}

#[tokio::test]
async fn mutate_rejects_unknown_alias_pin_mismatch_and_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let installation = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "a".repeat(64)
    );
    let request = sample_mutation_request(&installation);
    let missing = state
        .mutate_one(
            "apollo".to_string(),
            request.clone(),
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code, "not_found");

    let other = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "b".repeat(64)
    );
    state
        .replace_config(vec![FederationHostConfigWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: Some("apollo".to_string()),
            plan: ConnectionPlanWire {
                schema_version: 1,
                provider_ref: "builtin:https".to_string(),
                endpoint: "https://apollo.example".to_string(),
                credential_ref: "cred-1".to_string(),
                pinned_installation_id: other,
                connection_kind: sase_core::FleetConnectionKindWire::Gateway,
                tls: sase_core::TlsTrustSettingsWire {
                    schema_version: 1,
                    mode: sase_core::TlsTrustModeWire::SystemRoots,
                    ca_ref: None,
                    server_name_ref: None,
                },
            },
            bearer_token: "token".to_string(),
        }])
        .await
        .unwrap();
    let pin = state
        .mutate_one(
            "apollo".to_string(),
            request,
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap_err();
    assert_eq!(pin.code, "quarantined");

    let envelope = FederationIpcRequestEnvelopeWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        request_id: "req-1".to_string(),
        deadline_unix_ms: Some(1),
        operation: FederationIpcRequestWire::Mutate {
            target: "apollo".to_string(),
            request: Box::new(sample_mutation_request(&installation)),
        },
    };
    let response = handle_request(state, envelope).await;
    assert!(!response.ok);
    assert_eq!(response.error.unwrap().code, "deadline");
}

fn sample_attention_request(
    installation_id: &str,
) -> FleetAttentionRequestWire {
    let intent = sase_core::FleetAttentionIntentWire {
        schema_version: 1,
        kind: sase_core::FleetAttentionKindWire::Gate,
        request_key: sase_core::FleetAttentionRequestKeyWire {
            schema_version: 1,
            origin_installation_id: installation_id.to_string(),
            request_id: "gate-00000001".to_string(),
            pending_action_prefix: "gate-000".to_string(),
        },
        observed_revision: 1,
        selected_option_ids: vec!["approve".to_string()],
        feedback: None,
        question_choice: None,
        question_index: None,
        selected_option_id: None,
        selected_option_label: None,
        selected_option_index: None,
        custom_answer: None,
        global_note: None,
    };
    let fingerprint =
        sase_core::fleet_attention_payload_fingerprint(&intent).unwrap();
    FleetAttentionRequestWire {
        schema_version: 1,
        key: sase_core::ScopedOperationKeyWire {
            schema_version: 1,
            controller_id: "controller-1".to_string(),
            operation_id: "op-1".to_string(),
        },
        target_installation_id: installation_id.to_string(),
        intent,
        payload_fingerprint: fingerprint,
        acceptance_window_seconds: 30.0,
    }
}

#[tokio::test]
async fn resolve_attention_rejects_unknown_alias_pin_mismatch_and_deadline() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let installation = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "a".repeat(64)
    );
    let request = sample_attention_request(&installation);
    let missing = state
        .resolve_attention_one(
            "apollo".to_string(),
            request.clone(),
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code, "not_found");

    let other = format!(
        "{}{}",
        sase_core::FLEET_INSTALLATION_ID_PREFIX,
        "b".repeat(64)
    );
    state
        .replace_config(vec![FederationHostConfigWire {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            alias: Some("apollo".to_string()),
            plan: ConnectionPlanWire {
                schema_version: 1,
                provider_ref: "builtin:https".to_string(),
                endpoint: "https://apollo.example".to_string(),
                credential_ref: "cred-1".to_string(),
                pinned_installation_id: other,
                connection_kind: sase_core::FleetConnectionKindWire::Gateway,
                tls: sase_core::TlsTrustSettingsWire {
                    schema_version: 1,
                    mode: sase_core::TlsTrustModeWire::SystemRoots,
                    ca_ref: None,
                    server_name_ref: None,
                },
            },
            bearer_token: "token".to_string(),
        }])
        .await
        .unwrap();
    let pin = state
        .resolve_attention_one(
            "apollo".to_string(),
            request,
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap_err();
    assert_eq!(pin.code, "quarantined");

    let envelope = FederationIpcRequestEnvelopeWire {
        schema_version: FEDERATION_IPC_SCHEMA_VERSION,
        request_id: "req-1".to_string(),
        deadline_unix_ms: Some(1),
        operation: FederationIpcRequestWire::ResolveAttention {
            target: "apollo".to_string(),
            request: Box::new(sample_attention_request(&installation)),
        },
    };
    let response = handle_request(state, envelope).await;
    assert!(!response.ok);
    assert_eq!(response.error.unwrap().code, "deadline");
}

#[tokio::test]
async fn attention_read_returns_empty_hosts_when_unconfigured() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let result = state
        .read_all(
            ReadOperation::Attention(FleetLogicalBatchRequestWire {
                schema_version: 1,
                logical_keys: vec!["logical:alpha".to_string()],
            }),
            false,
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap();
    assert_eq!(result["operation"], "attention");
    assert_eq!(result["hosts"], json!([]));
}

#[tokio::test]
async fn attention_inventory_read_returns_empty_hosts_when_unconfigured() {
    let tmp = tempfile::tempdir().unwrap();
    let state = Arc::new(FederationWorkerState::new(
        FederationWorkerConfig::new(tmp.path()),
    ));
    let result = state
        .read_all(
            ReadOperation::AttentionInventory(
                FleetAttentionInventoryRequestWire {
                    schema_version: 1,
                    cursor: None,
                    limit: None,
                },
            ),
            false,
            RequestDeadline { unix_ms: None },
        )
        .await
        .unwrap();
    assert_eq!(result["operation"], "attention_inventory");
    assert_eq!(result["hosts"], json!([]));
}

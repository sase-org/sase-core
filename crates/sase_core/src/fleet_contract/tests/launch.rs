use super::super::*;
use super::support::*;

#[test]
fn fleet_launch_intent_and_replay_are_portable_and_target_pinned() {
    let intent = FleetLaunchIntentWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        prompt: "do the remote work".to_string(),
        request_id: Some("request-1".to_string()),
        display_name: Some("Remote work".to_string()),
        name: Some("worker".to_string()),
        model: Some("gpt-5".to_string()),
        provider: Some("openai".to_string()),
        runtime: None,
        project: FleetLaunchProjectContextWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            provider_ref: Some("provider-a".to_string()),
            project_id: "project-1".to_string(),
            revision: Some("a".repeat(40)),
            patch_ref: None,
        },
        dry_run: Some(false),
        follow: true,
        references: vec![FleetLaunchReferenceWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetLaunchReferenceKindWire::Artifact,
            reference: "artifact:abc123".to_string(),
            sha256: Some("a".repeat(64)),
        }],
    };
    let fingerprint = fleet_launch_payload_fingerprint(&intent).unwrap();
    let request = FleetLaunchRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: operation_key("dispatch-1"),
        target_installation_id: id('a'),
        intent: intent.clone(),
        payload_fingerprint: fingerprint.clone(),
        acceptance_window_seconds: 30.0,
    };
    assert_eq!(validate_fleet_launch_request(&request).unwrap(), request);

    let decision_request = FleetLaunchDecisionRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: operation_key("dispatch-1"),
        payload_fingerprint: fingerprint.clone(),
        target_installation_id: id('a'),
        now_unix: 10.0,
        acceptance_window_seconds: 30.0,
        existing_record: None,
    };
    let accepted = decide_fleet_launch_replay(&decision_request).unwrap();
    assert_eq!(accepted.decision, OperationDecisionKindWire::AcceptNew);
    let receipt = accepted.receipt.unwrap();
    assert_eq!(receipt.target_installation_id, id('a'));

    let replay = decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
        existing_record: Some(DurableFleetLaunchRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            receipt: receipt.clone(),
            tombstoned_at_unix_ms: None,
        }),
        now_unix: 11.0,
        ..decision_request.clone()
    })
    .unwrap();
    assert_eq!(
        replay.decision,
        OperationDecisionKindWire::ReturnOriginalReceipt
    );

    let conflict =
        decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
            payload_fingerprint: PayloadFingerprintWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                sha256: "b".repeat(64),
            },
            existing_record: Some(DurableFleetLaunchRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            }),
            now_unix: 11.0,
            ..decision_request.clone()
        })
        .unwrap();
    assert_eq!(conflict.decision, OperationDecisionKindWire::Conflict);

    let mismatch =
        decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
            target_installation_id: id('b'),
            existing_record: Some(DurableFleetLaunchRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            }),
            now_unix: 11.0,
            ..decision_request
        })
        .unwrap();
    assert_eq!(
        mismatch.decision,
        OperationDecisionKindWire::PreconditionMismatch
    );

    let mut path_context = intent;
    path_context.project.revision = Some("/tmp/source-checkout".to_string());
    assert!(validate_fleet_launch_intent(&path_context).is_err());

    let bad_receipt = FleetLaunchReceiptWire {
        logical_locator: Some(logical('b', "worker")),
        ..receipt
    };
    assert!(bad_receipt.validate().is_err());
}

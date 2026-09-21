use super::super::*;
use super::support::*;
use serde_json::json;

#[test]
fn operation_fingerprint_and_replay_decisions_are_scoped() {
    let left = operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: json!({"b": 2, "a": [1, true]}),
    })
    .unwrap();
    let right = operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: json!({"a": [1, true], "b": 2}),
    })
    .unwrap();
    assert_eq!(left, right);

    let target = exact('a', "worker", "run-1");
    let req = OperationDecisionRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: ScopedOperationKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            controller_id: "controller-a".to_string(),
            operation_id: "op-1".to_string(),
        },
        payload_fingerprint: left.clone(),
        resource_revision: revision(&target.logical, 1),
        target: target.clone(),
        now_unix: 10.0,
        acceptance_window_seconds: 5.0,
        existing_record: None,
    };
    let accepted = decide_operation_replay(&req).unwrap();
    assert_eq!(accepted.decision, OperationDecisionKindWire::AcceptNew);
    let receipt = accepted.receipt.unwrap();

    let replay = decide_operation_replay(&OperationDecisionRequestWire {
        existing_record: Some(DurableOperationRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            receipt: receipt.clone(),
            tombstoned_at_unix_ms: None,
        }),
        now_unix: 12.0,
        ..req.clone()
    })
    .unwrap();
    assert_eq!(
        replay.decision,
        OperationDecisionKindWire::ReturnOriginalReceipt
    );

    let conflict = decide_operation_replay(&OperationDecisionRequestWire {
        payload_fingerprint: PayloadFingerprintWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            sha256: "b".repeat(64),
        },
        existing_record: Some(DurableOperationRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            receipt: receipt.clone(),
            tombstoned_at_unix_ms: None,
        }),
        now_unix: 12.0,
        ..req.clone()
    })
    .unwrap();
    assert_eq!(conflict.decision, OperationDecisionKindWire::Conflict);

    let expired = decide_operation_replay(&OperationDecisionRequestWire {
        existing_record: Some(DurableOperationRecordWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            receipt,
            tombstoned_at_unix_ms: None,
        }),
        now_unix: 16.0,
        ..req
    })
    .unwrap();
    assert_eq!(expired.decision, OperationDecisionKindWire::Expired);
}

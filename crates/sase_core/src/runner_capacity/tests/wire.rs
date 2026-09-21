//! Wire alias and deserialization tests.

use super::super::*;
use super::support::*;

#[test]
fn legacy_wait_runners_aliases_deserialize_to_queue_capacity() {
    let mut record: RunnerCapacityRecordWire =
        serde_json::from_value(serde_json::json!({
            "artifact_dir": "/tmp/waiter",
            "project_name": "proj",
            "timestamp": "waiter",
            "wait_runners": 3,
            "wait_runners_explicit": true
        }))
        .unwrap();
    record.normalize_queue_capacity_aliases();
    assert_eq!(record.queue_capacity, Some(3));
    assert!(record.queue_capacity_explicit);
    let encoded = serde_json::to_value(&record).unwrap();
    assert_eq!(encoded["queue_capacity"], serde_json::json!(3));
    assert!(encoded.get("wait_runners").is_none());

    let waiter: RunnerCapacityWaiterWire =
        serde_json::from_value(serde_json::json!({
            "artifact_dir": "/tmp/waiter",
            "queue_position": 1,
            "priority": 10,
            "slot_requested_at": "2026-09-10T00:00:00Z",
            "timestamp": "waiter",
            "requested_weight": 1.0,
            "wait_runners": 3,
            "admission_limit": 3.0,
            "eligible": false
        }))
        .unwrap();
    assert_eq!(waiter.queue_capacity, Some(3));
}

#[test]
fn dual_written_capacity_fields_prefer_canonical_without_duplicate_error() {
    let record: RunnerCapacityRecordWire =
        serde_json::from_value(serde_json::json!({
            "artifact_dir": "/tmp/waiter",
            "project_name": "proj",
            "timestamp": "waiter",
            "queue_capacity": 100,
            "wait_runners": 0,
            "queue_capacity_explicit": true,
            "wait_runners_explicit": true
        }))
        .unwrap();
    let mut record = record;
    record.normalize_queue_capacity_aliases();
    assert_eq!(record.queue_capacity, Some(100));
    assert!(record.queue_capacity_explicit);
    assert!(record.wait_runners.is_none());

    let mut waiting_agent =
        waiting("waiter", "2026-09-10T00:00:00Z", Some(0.25));
    waiting_agent.queue_capacity = Some(100);
    waiting_agent.wait_runners = Some(0);
    waiting_agent.queue_capacity_explicit = true;
    waiting_agent.wait_runners_explicit = true;
    let result = snapshot_with_flags(
        1.0,
        vec![running("busy", Some(1.0)), waiting_agent],
        &capacity_budget_flags(),
    );
    assert_eq!(
        result.first_eligible_artifact_dir.as_deref(),
        Some("/tmp/waiter")
    );
    assert_eq!(result.waiters[0].queue_capacity, Some(100));
    assert_eq!(result.waiters[0].admission_limit, 100.0);
}

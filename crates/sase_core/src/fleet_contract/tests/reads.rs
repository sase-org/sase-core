use super::super::*;
use super::support::*;
use crate::agent_scan::wire::PendingQuestionMarkerWire;
use crate::agent_scan::WaitingMarkerWire;

#[test]
fn count_contract_is_order_independent_and_deduplicates_current_instances() {
    let locator = logical('a', "worker");
    let exact_one = exact('a', "worker", "run-1");
    let exact_two = exact('a', "worker", "run-2");
    let mut older = project_resolved_agent_summary(&projection_request(
        locator.clone(),
        Some(exact_one),
        1,
        record_running(),
    ))
    .unwrap();
    let newer = project_resolved_agent_summary(&projection_request(
        locator.clone(),
        Some(exact_two),
        2,
        record_running(),
    ))
    .unwrap();
    older.occupied_runner_slot = false;

    let waiter_locator = logical('a', "waiter");
    let mut waiting_record = record_running();
    waiting_record.running = None;
    waiting_record.waiting = Some(WaitingMarkerWire::default());
    let mut waiting = project_resolved_agent_summary(&projection_request(
        waiter_locator,
        Some(exact('a', "waiter", "run-1")),
        1,
        waiting_record,
    ))
    .unwrap();
    waiting.owner_liveness_for_test(OwnerLivenessWire::Unknown);
    waiting.occupied_runner_slot = false;

    let question_locator = logical('a', "question");
    let mut question_record = record_running();
    question_record.running = None;
    question_record.pending_question =
        Some(PendingQuestionMarkerWire::default());
    let mut attention = project_resolved_agent_summary(&projection_request(
        question_locator,
        Some(exact('a', "question", "run-1")),
        1,
        question_record,
    ))
    .unwrap();
    attention.occupied_runner_slot = false;

    let mut monitor = newer.clone();
    monitor.row_kind = FleetRowKindWire::Monitor;
    monitor.family_role = FleetFamilyRoleWire::Monitor;
    let request = FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: vec![
            waiting.clone(),
            monitor.clone(),
            newer.clone(),
            attention.clone(),
            older.clone(),
        ],
    };
    let counts = count_logical_agents(&request).unwrap();
    assert_eq!(counts.logical_agent_total, 3);
    assert_eq!(counts.running, 1);
    assert_eq!(counts.waiting, 1);
    assert_eq!(counts.attention, 1);
    assert_eq!(counts.occupied_runner_slots, 1);

    let reversed = FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: vec![older, attention, newer, monitor, waiting],
    };
    assert_eq!(count_logical_agents(&reversed).unwrap(), counts);
}
#[test]
fn count_rejects_equal_revision_competing_current_instances() {
    let locator = logical('a', "worker");
    let one = project_resolved_agent_summary(&projection_request(
        locator.clone(),
        Some(exact('a', "worker", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let two = project_resolved_agent_summary(&projection_request(
        locator,
        Some(exact('a', "worker", "run-2")),
        1,
        record_running(),
    ))
    .unwrap();
    let err = count_logical_agents(&FleetLogicalAgentCountsRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        summaries: vec![one, two],
    })
    .unwrap_err();
    assert!(err.to_string().contains("ambiguous current instances"));
}
#[test]
fn batch_lookup_preserves_requested_order_and_bounds_ids() {
    let alpha = project_resolved_agent_summary(&projection_request(
        logical('a', "alpha"),
        Some(exact('a', "alpha", "run-1")),
        1,
        record_running(),
    ))
    .unwrap();
    let beta = project_resolved_agent_summary(&projection_request(
        logical('a', "beta"),
        Some(exact('a', "beta", "run-1")),
        2,
        record_running(),
    ))
    .unwrap();
    let request = FleetLogicalBatchRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        logical_keys: vec![
            beta.logical_key.clone(),
            "missing-logical-key".to_string(),
            alpha.logical_key.clone(),
        ],
    };
    let entries =
        select_fleet_logical_batch(&request, &[alpha.clone(), beta.clone()])
            .unwrap();
    assert_eq!(
        entries[0].summary.as_ref().unwrap().logical_key,
        beta.logical_key
    );
    assert!(entries[1].summary.is_none());
    assert_eq!(
        entries[2].summary.as_ref().unwrap().logical_key,
        alpha.logical_key
    );

    let mut too_many = request;
    too_many.logical_keys = vec!["k".to_string(); FLEET_READ_MAX_BATCH_IDS + 1];
    assert!(validate_fleet_logical_batch_request(&too_many).is_err());
}
#[test]
fn content_and_project_read_requests_are_bounded() {
    let request = FleetContentReadRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        handle_id: "handle-1".to_string(),
        row_revision: revision(&logical('a', "alpha"), 1),
        offset: 0,
        limit: Some(FLEET_READ_DEFAULT_CONTENT_BYTES),
    };
    assert_eq!(
        fleet_content_read_limit(&request).unwrap(),
        FLEET_READ_DEFAULT_CONTENT_BYTES
    );
    assert!(validate_fleet_content_read_request(
        &FleetContentReadRequestWire {
            handle_id: "../secret".to_string(),
            ..request.clone()
        }
    )
    .is_err());
    assert!(validate_fleet_content_read_request(
        &FleetContentReadRequestWire {
            limit: Some(FLEET_READ_MAX_CONTENT_BYTES + 1),
            ..request
        }
    )
    .is_err());

    assert!(validate_fleet_project_eligibility_request(
        &FleetProjectEligibilityRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project_ids: vec![
                "project".to_string();
                FLEET_READ_MAX_PROJECT_IDS + 1
            ],
            limit: None,
        }
    )
    .is_err());
}

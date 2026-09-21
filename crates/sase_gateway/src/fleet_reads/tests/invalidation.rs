//! Invalidation-hub tests: publish, replay, and resync faults.

use sase_core::fleet_contract::{
    FleetEventStreamItemWire, FleetInvalidationKindWire, FleetResyncReasonWire,
};

use super::support::*;

#[tokio::test]
async fn fleet_events_publish_replay_and_resync_faults() {
    let (_temp, service) = seed_home();
    let cursor = service.current_event_cursor();
    let mut first = service
        .subscribe_events(Some(cursor.clone()))
        .await
        .unwrap();
    let mut second = service
        .subscribe_events(Some(cursor.clone()))
        .await
        .unwrap();
    assert!(first.initial.is_empty());
    service
        .publish_invalidation(
            FleetInvalidationKindWire::RevisionChanged,
            None,
            None,
            "revision_changed",
        )
        .unwrap();
    assert!(first.receiver.recv().await.is_ok());
    assert!(second.receiver.recv().await.is_ok());

    let replay = service
        .subscribe_events(Some(cursor.clone()))
        .await
        .unwrap();
    assert!(matches!(
        replay.initial.first(),
        Some(FleetEventStreamItemWire::Invalidation(_))
    ));

    for _ in 0..(sase_core::FLEET_READ_DEFAULT_REPLAY_EVENTS + 2) {
        service
            .publish_invalidation(
                FleetInvalidationKindWire::RevisionChanged,
                None,
                None,
                "revision_changed",
            )
            .unwrap();
    }
    let rolled = service
        .subscribe_events(Some(cursor.clone()))
        .await
        .unwrap();
    assert!(matches!(
        rolled.initial.first(),
        Some(FleetEventStreamItemWire::ResyncRequired(resync))
            if resync.reason == FleetResyncReasonWire::ReplayGap
    ));

    let old_generation = service.current_event_cursor();
    service.replace_event_generation_for_test();
    let replaced = service
        .subscribe_events(Some(old_generation))
        .await
        .unwrap();
    assert!(matches!(
        replaced.initial.first(),
        Some(FleetEventStreamItemWire::ResyncRequired(resync))
            if resync.reason == FleetResyncReasonWire::GenerationMismatch
    ));

    let current = service.current_event_cursor();
    service.mark_deletion_history_incomplete_for_test();
    let incomplete = service.subscribe_events(Some(current)).await.unwrap();
    assert!(matches!(
        incomplete.initial.first(),
        Some(FleetEventStreamItemWire::ResyncRequired(resync))
            if resync.reason == FleetResyncReasonWire::IncompleteDeletionHistory
    ));
}

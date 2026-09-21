use super::super::*;
use super::support::*;

#[test]
fn cursor_replay_classifies_resync_boundaries() {
    let req = CursorReplayRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        cursor: StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: "gen-1".to_string(),
            sequence: 5,
        },
        current_generation: "gen-1".to_string(),
        newest_sequence: 8,
        oldest_replayable_sequence: 6,
        deletion_history_complete: true,
    };
    let replay = classify_cursor_replay(&req).unwrap();
    assert_eq!(
        replay.classification,
        CursorReplayClassificationWire::Replayable
    );
    assert_eq!(replay.replay_from_sequence, Some(6));

    let mut current = req.clone();
    current.cursor.sequence = 8;
    assert_eq!(
        classify_cursor_replay(&current).unwrap().classification,
        CursorReplayClassificationWire::Current
    );
    let mut ahead = req.clone();
    ahead.cursor.sequence = 9;
    assert_eq!(
        classify_cursor_replay(&ahead).unwrap().reason,
        CursorReplayReasonWire::SequenceAheadOfAuthority
    );
    let mut gap = req.clone();
    gap.cursor.sequence = 4;
    assert_eq!(
        classify_cursor_replay(&gap).unwrap().reason,
        CursorReplayReasonWire::ReplayGap
    );
    let mut generation = req.clone();
    generation.cursor.store_generation = "gen-0".to_string();
    assert_eq!(
        classify_cursor_replay(&generation).unwrap().reason,
        CursorReplayReasonWire::GenerationMismatch
    );
    let mut tombstones = req;
    tombstones.deletion_history_complete = false;
    assert_eq!(
        classify_cursor_replay(&tombstones).unwrap().reason,
        CursorReplayReasonWire::IncompleteDeletionHistory
    );
    let mut initial = tombstones;
    initial.deletion_history_complete = true;
    initial.cursor = StoreCursorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        store_generation: FLEET_INITIAL_CURSOR_GENERATION.to_string(),
        sequence: 0,
    };
    assert_eq!(
        classify_cursor_replay(&initial).unwrap().reason,
        CursorReplayReasonWire::InitialCursor
    );
}
#[test]
fn invalidations_validate_cursor_and_revision_identity() {
    let locator = logical('a', "alpha");
    let logical_key = logical_key_unchecked(&locator);
    let event = FleetInvalidationEventWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        cursor: StoreCursorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            store_generation: "gen-1".to_string(),
            sequence: 1,
        },
        kind: FleetInvalidationKindWire::RevisionChanged,
        logical_key: Some(logical_key.clone()),
        row_revision: Some(ResourceRevisionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            logical_key,
            revision: 1,
        }),
        reason: "revision_changed".to_string(),
    };
    assert!(validate_fleet_invalidation_event(&event).is_ok());
    assert!(
        validate_fleet_invalidation_event(&FleetInvalidationEventWire {
            reason: "../path".to_string(),
            ..event
        })
        .is_err()
    );
}

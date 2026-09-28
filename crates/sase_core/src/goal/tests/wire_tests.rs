//! Wire parsing and forward-compatibility tests.

use serde_json::json;

use crate::goal::wire::{
    parse_payload_for_kind, GoalEventKindWire, GoalEventPayloadWire,
    GoalEventWire, GoalStatusWire, GOAL_WIRE_SCHEMA_VERSION,
};

use super::support::{created, eid, human, GOAL, T0};

#[test]
fn kind_words_round_trip() {
    for word in GoalEventKindWire::KNOWN {
        let kind = GoalEventKindWire::parse(word);
        assert!(kind.is_supported());
        assert_eq!(kind.as_str(), word);
        let json = serde_json::to_string(&kind).unwrap();
        let back: GoalEventKindWire = serde_json::from_str(&json).unwrap();
        assert_eq!(back, kind);
    }
}

#[test]
fn unknown_kind_stays_typed() {
    let kind = GoalEventKindWire::parse("time_traveled");
    assert!(!kind.is_supported());
    assert_eq!(kind.as_str(), "time_traveled");
    let json = serde_json::to_string(&kind).unwrap();
    assert_eq!(json, "\"time_traveled\"");
    let back: GoalEventKindWire = serde_json::from_str(&json).unwrap();
    assert_eq!(back, kind);
}

#[test]
fn unknown_fields_are_ignored() {
    let event = created(
        GOAL,
        &eid(1000, 1),
        T0,
        human(),
        "k:1",
        "Title",
        "Outcome",
        "sase",
    );
    let mut json = serde_json::to_value(&event).unwrap();
    json["envelope_future"] = json!("ignore me");
    json["payload"]["title_future"] = json!("ignore me too");
    let back: GoalEventWire = serde_json::from_value(json).unwrap();
    assert_eq!(back, event);
}

#[test]
fn payload_dispatch_matches_envelope_kind() {
    let event = created(
        GOAL,
        &eid(1000, 1),
        T0,
        human(),
        "k:1",
        "Title",
        "Outcome",
        "sase",
    );
    let payload = event.parse_payload().unwrap();
    assert!(matches!(payload, GoalEventPayloadWire::Created(_)));
}

#[test]
fn payload_dispatch_refuses_untagged_guessing() {
    let settled = json!({"flavor": "verified"});
    let parsed =
        parse_payload_for_kind(&GoalEventKindWire::Edited, &settled).unwrap();
    assert!(matches!(parsed, GoalEventPayloadWire::Edited(_)));
}

#[test]
fn unsupported_kind_parses_as_typed_value() {
    let mut event = created(
        GOAL,
        &eid(1000, 1),
        T0,
        human(),
        "k:1",
        "Title",
        "Outcome",
        "sase",
    );
    event.kind = GoalEventKindWire::parse("time_traveled");
    let error = event.parse_payload().unwrap_err();
    assert!(error.to_string().contains("time_traveled"));
}

#[test]
fn newer_schema_version_parses_as_typed_value() {
    let mut event = created(
        GOAL,
        &eid(1000, 1),
        T0,
        human(),
        "k:1",
        "Title",
        "Outcome",
        "sase",
    );
    event.schema_version = GOAL_WIRE_SCHEMA_VERSION + 1;
    let error = event.parse_payload().unwrap_err();
    assert!(error.to_string().contains("schema_version"));
}

#[test]
fn status_words_and_heat() {
    assert_eq!(GoalStatusWire::Draft.as_str(), "draft");
    assert_eq!(GoalStatusWire::Active.as_str(), "active");
    assert_eq!(GoalStatusWire::Review.as_str(), "review");
    assert_eq!(GoalStatusWire::Done.as_str(), "done");
    assert_eq!(GoalStatusWire::Dropped.as_str(), "dropped");
    assert!(GoalStatusWire::Draft.is_unsettled());
    assert!(GoalStatusWire::Active.is_unsettled());
    assert!(GoalStatusWire::Review.is_unsettled());
    assert!(!GoalStatusWire::Done.is_unsettled());
    assert!(!GoalStatusWire::Dropped.is_unsettled());
    assert!(GoalStatusWire::Done.is_settled());
}

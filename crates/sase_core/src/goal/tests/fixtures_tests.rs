//! JSON round-trip tests against committed fixtures.
//!
//! The fixtures freeze the event vocabulary: later epics add
//! fixture files and never edit these.

use crate::goal::wire::{GoalEventKindWire, GoalEventWire};

const FIXTURES: [(&str, &str); 12] = [
    ("created", include_str!("fixtures/created.json")),
    ("named", include_str!("fixtures/named.json")),
    ("edited", include_str!("fixtures/edited.json")),
    ("adopted", include_str!("fixtures/adopted.json")),
    (
        "agent_attached",
        include_str!("fixtures/agent_attached.json"),
    ),
    ("progress", include_str!("fixtures/progress.json")),
    ("plan_attached", include_str!("fixtures/plan_attached.json")),
    ("claimed", include_str!("fixtures/claimed.json")),
    (
        "claim_retracted",
        include_str!("fixtures/claim_retracted.json"),
    ),
    ("settled", include_str!("fixtures/settled.json")),
    ("reopened", include_str!("fixtures/reopened.json")),
    ("merged", include_str!("fixtures/merged.json")),
];

#[test]
fn every_fixture_parses_and_matches_its_kind() {
    for (word, body) in FIXTURES {
        let event: GoalEventWire = serde_json::from_str(body)
            .unwrap_or_else(|error| panic!("fixture {word} parses: {error}"));
        assert_eq!(
            event.kind,
            GoalEventKindWire::parse(word),
            "fixture {word} carries its kind"
        );
        event.parse_payload().unwrap_or_else(|error| {
            panic!("fixture {word} payload parses: {error}")
        });
    }
}

#[test]
fn every_fixture_round_trips() {
    for (word, body) in FIXTURES {
        let event: GoalEventWire = serde_json::from_str(body)
            .unwrap_or_else(|error| panic!("fixture {word} parses: {error}"));
        let value = serde_json::to_value(&event).unwrap();
        let back: GoalEventWire = serde_json::from_value(value).unwrap();
        assert_eq!(back, event, "fixture {word} round-trips");
    }
}

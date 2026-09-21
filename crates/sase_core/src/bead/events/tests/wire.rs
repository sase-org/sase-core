//! Wire tests, covering `super::super::wire`: update-fields
//! encoding round trips.

use std::collections::BTreeMap;

use serde_json;

use crate::bead::wire::{
    BeadResolutionWire, BeadTierWire, PhaseSizeWire, StatusWire,
};

use super::super::*;

#[test]
fn issue_update_event_fields_resolution_round_trips_all_three_encodings() {
    let base = BeadIssueUpdateEventFieldsWire::default();

    let omitted = serde_json::to_value(&base).unwrap();
    assert!(omitted.get("resolution").is_none());
    let decoded: BeadIssueUpdateEventFieldsWire =
        serde_json::from_value(omitted.clone()).unwrap();
    assert_eq!(decoded, base);
    assert_eq!(serde_json::to_value(&decoded).unwrap(), omitted);

    let cleared = BeadIssueUpdateEventFieldsWire {
        resolution: Some(None),
        ..base.clone()
    };
    let cleared_json = serde_json::to_value(&cleared).unwrap();
    assert_eq!(cleared_json["resolution"], serde_json::Value::Null);
    let decoded: BeadIssueUpdateEventFieldsWire =
        serde_json::from_value(cleared_json.clone()).unwrap();
    assert_eq!(decoded, cleared);
    assert_eq!(serde_json::to_value(&decoded).unwrap(), cleared_json);

    let set = BeadIssueUpdateEventFieldsWire {
        resolution: Some(Some(BeadResolutionWire::Done)),
        ..base
    };
    let set_json = serde_json::to_value(&set).unwrap();
    assert_eq!(set_json["resolution"], serde_json::json!("done"));
    let decoded: BeadIssueUpdateEventFieldsWire =
        serde_json::from_value(set_json.clone()).unwrap();
    assert_eq!(decoded, set);
    assert_eq!(serde_json::to_value(&decoded).unwrap(), set_json);
}

#[test]
fn issue_update_event_fields_round_trip_every_field() {
    let fixtures: Vec<BeadIssueUpdateEventFieldsWire> = vec![
        BeadIssueUpdateEventFieldsWire::default(),
        BeadIssueUpdateEventFieldsWire {
            title: Some("New title".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            status: Some(StatusWire::Closed),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            assignee: Some("agent@example.com".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            description: Some("desc".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            notes: Some("notes".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            design: Some("design".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            model: Some("model".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            size: Some(PhaseSizeWire::Small),
            ..Default::default()
        },
        // closed_at and close_reason have no skip_serializing_if, so
        // Some(None) and None both serialize to an explicit `null` and
        // are indistinguishable on decode; that pre-existing collapse
        // is what keeps their *bytes* stable and is out of scope for
        // this fix (see the tale this test guards). Only their
        // Some(Some(_)) state is exercised here.
        BeadIssueUpdateEventFieldsWire {
            closed_at: Some(Some("2026-01-01T00:00:00Z".to_string())),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            close_reason: Some(Some("done early".to_string())),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            resolution: Some(None),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            resolution: Some(Some(BeadResolutionWire::Canceled)),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            changespec_name: Some("cs".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            changespec_bug_id: Some("bug-1".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            external_ref: Some("ext".to_string()),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            tier: Some(BeadTierWire::Plan),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            is_ready_to_work: Some(true),
            ..Default::default()
        },
        BeadIssueUpdateEventFieldsWire {
            task_type_fields: Some(BTreeMap::from([(
                "remove_by_date".to_string(),
                "2026-12-15".to_string(),
            )])),
            ..Default::default()
        },
    ];

    for fixture in fixtures {
        let encoded = serde_json::to_value(&fixture).unwrap();
        let decoded: BeadIssueUpdateEventFieldsWire =
            serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(
            decoded, fixture,
            "decode(encode(x)) == x failed for {encoded}"
        );
        let re_encoded = serde_json::to_value(&decoded).unwrap();
        assert_eq!(
            re_encoded, encoded,
            "encode(decode(json)) == json failed for {encoded}"
        );
    }
}

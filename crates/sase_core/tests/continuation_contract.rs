use sase_core::continuation::{
    plan_continuation_replay, ContinuationReplayPlanRequestWire,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct SerialReplayFixture {
    request: ContinuationReplayPlanRequestWire,
    expected_order: Vec<String>,
    expected_rendered_bytes: u64,
}

#[test]
fn serial_replay_fixture_preserves_exact_parent_order() {
    let fixture: SerialReplayFixture = serde_json::from_str(include_str!(
        "fixtures/continuation/serial_replay.json"
    ))
    .unwrap();

    let manifest = plan_continuation_replay(fixture.request).unwrap();

    assert_eq!(manifest.ordered_node_ids, fixture.expected_order);
    assert_eq!(
        manifest.rendered_component_sizes.total_utf8_bytes,
        fixture.expected_rendered_bytes
    );
    assert_eq!(
        manifest.selected_evidence_refs,
        vec!["file:explicit:diagnostics"]
    );
    assert_eq!(manifest.omissions, vec![]);
}

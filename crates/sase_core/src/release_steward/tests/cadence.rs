use crate::release_steward::facts::FactRead;
use crate::release_steward::release_steward_decide;
use crate::release_steward::wire::PrimaryStateWire;

use super::fixtures::{
    merge_event, releasable_request, AT_NINE, BEFORE_NINE, NOON, YESTERDAY_NOON,
};

fn waiting_reason(
    request: crate::release_steward::wire::ReleaseStewardRequestWire,
) -> String {
    match release_steward_decide(&request).unwrap().primary {
        PrimaryStateWire::Waiting { reason } => reason,
        other => panic!("expected waiting, got {other:?}"),
    }
}

#[test]
fn before_nine_utc_blocks_merge() {
    let mut request = releasable_request();
    request.observed_at = BEFORE_NINE.to_string();
    let reason = waiting_reason(request);
    assert!(reason.contains("09:00"));
}

#[test]
fn at_nine_utc_allows_merge() {
    let mut request = releasable_request();
    request.observed_at = AT_NINE.to_string();
    assert!(matches!(
        release_steward_decide(&request).unwrap().primary,
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn same_day_merge_blocks() {
    let mut request = releasable_request();
    request.observed_at = NOON.to_string();
    request.snapshot.merge_history =
        FactRead::ready(vec![merge_event(AT_NINE, false)]);
    let reason = waiting_reason(request);
    assert!(reason.contains("daily merge already used"));
}

#[test]
fn abandoned_same_day_candidate_frees_the_slot() {
    let mut request = releasable_request();
    request.snapshot.merge_history =
        FactRead::ready(vec![merge_event(AT_NINE, true)]);
    assert!(matches!(
        release_steward_decide(&request).unwrap().primary,
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn prior_day_merge_does_not_block() {
    let mut request = releasable_request();
    request.snapshot.merge_history =
        FactRead::ready(vec![merge_event(YESTERDAY_NOON, false)]);
    assert!(matches!(
        release_steward_decide(&request).unwrap().primary,
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn utc_midnight_starts_a_new_day() {
    let mut request = releasable_request();
    request.observed_at = "2026-10-20T09:00:00Z".to_string();
    request.snapshot.merge_history =
        FactRead::ready(vec![merge_event(NOON, false)]);
    assert!(matches!(
        release_steward_decide(&request).unwrap().primary,
        PrimaryStateWire::Releasable { .. }
    ));
}

#[test]
fn active_train_prevents_another_merge() {
    let mut request = releasable_request();
    request.snapshot.trains =
        FactRead::ready(vec![super::fixtures::active_train()]);
    request.snapshot.candidate = FactRead::ready(Some(
        super::fixtures::publishable_eval("sase-0.19.0-gen1", 1),
    ));
    let primary = release_steward_decide(&request).unwrap().primary;
    assert!(!matches!(primary, PrimaryStateWire::Releasable { .. }));
}

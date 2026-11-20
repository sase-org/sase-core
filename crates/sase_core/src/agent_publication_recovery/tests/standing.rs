//! Standing publication-failure alert decider tests.

use super::super::*;

fn standing_observation(
    project: &str,
    signature: &str,
    kind: &str,
) -> PublicationStandingObservationWire {
    PublicationStandingObservationWire {
        project: project.to_string(),
        signature: signature.to_string(),
        failure_kind: kind.to_string(),
        error_class: PUBLICATION_ERROR_OTHER.to_string(),
        detail: format!("{kind} detail"),
    }
}

fn standing_request(
    previous: Option<PublicationStandingAlertWire>,
    observation: Option<PublicationStandingObservationWire>,
    now: f64,
) -> PublicationStandingDecideRequestWire {
    PublicationStandingDecideRequestWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        previous,
        observation,
        now,
    }
}

#[test]
fn standing_first_failure_opens_with_initial_backoff() {
    let response = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_DRAIN_ERROR,
        )),
        1_000.0,
    ))
    .expect("first failure must open");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_OPENED);
    assert!(response.project_scoped);
    let alert = response.alert.expect("opened must carry the alert");
    assert_eq!(alert.attempts, 1);
    assert_eq!(alert.first_seen, 1_000.0);
    assert_eq!(alert.last_seen, 1_000.0);
    assert_eq!(alert.last_reported, 1_000.0);
    assert!(!alert.escalated);
    assert_eq!(
        alert.next_attempt_at,
        1_000.0 + PUBLICATION_STANDING_BACKOFF_INITIAL_SECONDS
    );
}

#[test]
fn standing_row_scoped_kind_takes_no_backoff() {
    let response = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_CHARGED_ITEM,
        )),
        1_000.0,
    ))
    .expect("row-scoped failure must open");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_OPENED);
    assert!(!response.project_scoped);
    let alert = response.alert.expect("opened must carry the alert");
    assert!(!alert.project_scoped);
    assert_eq!(alert.next_attempt_at, 0.0);
}

#[test]
fn standing_repeat_stays_silent_and_counts_the_attempt() {
    let opened = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_LOCK_BUSY,
        )),
        1_000.0,
    ))
    .expect("open")
    .alert
    .expect("alert");
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_LOCK_BUSY,
        )),
        1_060.0,
    ))
    .expect("repeat must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_NONE);
    let alert = response.alert.expect("silent tick keeps the alert");
    assert_eq!(alert.attempts, 2);
    assert_eq!(alert.first_seen, 1_000.0);
    assert_eq!(alert.last_seen, 1_060.0);
    assert_eq!(alert.last_reported, 1_000.0);
    assert_eq!(alert.next_attempt_at, 1_060.0 + 120.0);
}

#[test]
fn standing_backoff_doubles_to_the_cap() {
    let mut previous = None;
    let mut now = 0.0;
    let mut delays = Vec::new();
    for _ in 0..8 {
        let response = decide_publication_standing(&standing_request(
            previous,
            Some(standing_observation(
                "proj",
                "sig",
                PUBLICATION_STANDING_KIND_DRAIN_TIMEOUT,
            )),
            now,
        ))
        .expect("repeat must decide");
        let alert = response.alert.expect("alert");
        delays.push(alert.next_attempt_at - now);
        now += 1.0;
        previous = Some(alert);
    }
    assert_eq!(
        delays,
        vec![60.0, 120.0, 240.0, 480.0, 960.0, 1800.0, 1800.0, 1800.0]
    );
}

fn open_drain_error_alert() -> PublicationStandingAlertWire {
    let mut observation = standing_observation(
        "proj",
        "sig",
        PUBLICATION_STANDING_KIND_DRAIN_ERROR,
    );
    observation.detail =
        "lock held by pid 4242 for 13s (alice.athena.foo@0123abcd)".to_string();
    decide_publication_standing(&standing_request(
        None,
        Some(observation),
        1_000.0,
    ))
    .expect("open")
    .alert
    .expect("alert")
}

#[test]
fn standing_volatile_detail_change_stays_silent_and_keeps_newest_detail() {
    let opened = open_drain_error_alert();
    let mut repeat = standing_observation(
        "proj",
        "sig",
        PUBLICATION_STANDING_KIND_DRAIN_ERROR,
    );
    repeat.detail =
        "lock held by pid 9001 for 74s (bob.apollo.bar@fedcba98)".to_string();
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        Some(repeat),
        1_060.0,
    ))
    .expect("volatile repeat must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_NONE);
    let alert = response.alert.expect("silent tick keeps the alert");
    assert_eq!(alert.attempts, 2);
    assert_eq!(alert.last_reported, 1_000.0);
    assert_eq!(alert.last_seen, 1_060.0);
    assert_eq!(
        alert.detail,
        "lock held by pid 9001 for 74s (bob.apollo.bar@fedcba98)"
    );
}

#[test]
fn standing_failure_kind_change_reports_once_more() {
    let opened = open_drain_error_alert();
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_DRAIN_TIMEOUT,
        )),
        1_060.0,
    ))
    .expect("kind change must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_CHANGED);
    let alert = response.alert.expect("changed keeps the alert");
    assert_eq!(alert.failure_kind, PUBLICATION_STANDING_KIND_DRAIN_TIMEOUT);
    assert_eq!(alert.attempts, 2);
    assert_eq!(alert.last_reported, 1_060.0);
}

#[test]
fn standing_error_class_change_reports_once_more() {
    let opened = open_drain_error_alert();
    let mut reclassed = standing_observation(
        "proj",
        "sig",
        PUBLICATION_STANDING_KIND_DRAIN_ERROR,
    );
    reclassed.error_class = PUBLICATION_ERROR_LOCK_BUSY.to_string();
    reclassed.detail = opened.detail.clone();
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        Some(reclassed),
        1_060.0,
    ))
    .expect("class change must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_CHANGED);
    let alert = response.alert.expect("changed keeps the alert");
    assert_eq!(alert.error_class, PUBLICATION_ERROR_LOCK_BUSY);
    assert_eq!(alert.last_reported, 1_060.0);
}

#[test]
fn standing_key_change_reports_once_more() {
    let opened = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig-a",
            PUBLICATION_STANDING_KIND_DRAIN_ERROR,
        )),
        1_000.0,
    ))
    .expect("open")
    .alert
    .expect("alert");
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        Some(standing_observation(
            "proj",
            "sig-b",
            PUBLICATION_STANDING_KIND_DRAIN_ERROR,
        )),
        1_060.0,
    ))
    .expect("key change must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_CHANGED);
    let alert = response.alert.expect("changed adopts the new key");
    assert_eq!(alert.signature, "sig-b");
    assert_eq!(alert.attempts, 2);
}

#[test]
fn standing_escalates_once_after_24h_then_stays_silent() {
    let opened = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_TARGET_UNAVAILABLE,
        )),
        1_000.0,
    ))
    .expect("open")
    .alert
    .expect("alert");
    let escalated = decide_publication_standing(&standing_request(
        Some(opened),
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_TARGET_UNAVAILABLE,
        )),
        1_000.0 + PUBLICATION_STANDING_ESCALATE_AFTER_SECONDS,
    ))
    .expect("escalation must decide")
    .alert
    .expect("escalated keeps the alert");
    // The second decision in this chain is the escalation itself.
    assert!(escalated.escalated);
    assert_eq!(escalated.last_reported, 1_000.0 + 86_400.0);
    let quiet = decide_publication_standing(&standing_request(
        Some(escalated),
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_TARGET_UNAVAILABLE,
        )),
        1_000.0 + 2.0 * PUBLICATION_STANDING_ESCALATE_AFTER_SECONDS,
    ))
    .expect("post-escalation must decide");
    assert_eq!(quiet.transition, PUBLICATION_STANDING_TRANSITION_NONE);
    assert!(quiet.alert.expect("alert").escalated);
}

#[test]
fn standing_clean_pass_clears_without_an_alert() {
    let opened = decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "sig",
            PUBLICATION_STANDING_KIND_REFERENCED_BY,
        )),
        1_000.0,
    ))
    .expect("open")
    .alert
    .expect("alert");
    let response = decide_publication_standing(&standing_request(
        Some(opened),
        None,
        1_060.0,
    ))
    .expect("clean pass must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_CLEARED);
    assert!(response.alert.is_none());
}

#[test]
fn standing_idle_tick_reports_nothing() {
    let response =
        decide_publication_standing(&standing_request(None, None, 1.0))
            .expect("idle tick must decide");
    assert_eq!(response.transition, PUBLICATION_STANDING_TRANSITION_NONE);
    assert!(response.alert.is_none());
}

#[test]
fn standing_rejects_bad_requests() {
    assert!(decide_publication_standing(
        &PublicationStandingDecideRequestWire {
            schema_version: 999,
            previous: None,
            observation: None,
            now: 1.0,
        }
    )
    .is_err());
    assert!(decide_publication_standing(&standing_request(
        None,
        Some(standing_observation("proj", "sig", "nope")),
        1.0,
    ))
    .is_err());
    assert!(decide_publication_standing(&standing_request(
        None,
        Some(standing_observation(
            "proj",
            "",
            PUBLICATION_STANDING_KIND_LOCK_BUSY
        )),
        1.0,
    ))
    .is_err());
    assert!(decide_publication_standing(&standing_request(
        None,
        None,
        f64::NAN
    ))
    .is_err());
    assert!(
        decide_publication_standing(&standing_request(None, None, -1.0))
            .is_err()
    );
}

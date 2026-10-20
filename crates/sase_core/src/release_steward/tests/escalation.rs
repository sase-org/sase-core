use crate::release_steward::facts::{
    BlockerLineageWire, FactRead, IssueCommentWire, IssueFactsWire,
};
use crate::release_steward::release_steward_decide;
use crate::release_steward::wire::{
    ActionDispositionWire, EscalationOpWire, EscalationSeverityWire,
};

use super::fixtures::{
    active_train, publishable_eval, releasable_request, AGE_START, NOON, TICK,
};

fn issue(
    family: &str,
    identity: &str,
    state: &str,
    comments: Vec<IssueCommentWire>,
) -> IssueFactsWire {
    IssueFactsWire {
        number: 9,
        title: format!("release-steward: {family} ({identity})"),
        state: state.to_string(),
        labels: vec!["release-steward".to_string()],
        identity_marker: format!("release-steward:{family}:{identity}"),
        family: family.to_string(),
        candidate_id: Some(identity.to_string()),
        pin: None,
        sha: None,
        signature: None,
        comments,
        opened_at: AGE_START.to_string(),
        updated_at: NOON.to_string(),
        closed_at: None,
    }
}

#[test]
fn install_broken_escalates_immediately() {
    let mut request = releasable_request();
    request.snapshot.release_pr = FactRead::ready(None);
    if let FactRead::Ready { value } = &mut request.snapshot.canary {
        value.linux.as_mut().unwrap().verdict = "fail".to_string();
        value.linux.as_mut().unwrap().reason = Some("boom".to_string());
    }
    let result = release_steward_decide(&request).unwrap();
    let escalation = result.escalation.expect("immediate");
    assert_eq!(escalation.severity, EscalationSeverityWire::Immediate);
    assert!(matches!(
        escalation.operation,
        EscalationOpWire::Open { .. }
    ));
}

#[test]
fn publish_conflict_escalates_immediately() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_digest_conflict = true;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    assert_eq!(escalation.severity, EscalationSeverityWire::Immediate);
}

#[test]
fn aged_blocking_opens_once_at_24h() {
    let mut request = releasable_request();
    request.snapshot.core_ensure = FactRead::ready(
        crate::release_steward::facts::CoreEnsureFactsWire::CoreLag {
            pin: super::fixtures::PIN.to_string(),
            newest_complete: None,
        },
    );
    request.snapshot.blocker_lineage =
        FactRead::ready(Some(BlockerLineageWire {
            family: "core_lag".to_string(),
            identity: super::fixtures::PIN.to_string(),
            started_at: Some(AGE_START.to_string()),
            continuous: true,
            identity_changed: false,
        }));
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    assert_eq!(escalation.severity, EscalationSeverityWire::Aged);
    assert!(matches!(
        escalation.operation,
        EscalationOpWire::Open { .. }
    ));
}

#[test]
fn later_tick_comments_plus_one() {
    let mut request = releasable_request();
    request.snapshot.core_ensure = FactRead::ready(
        crate::release_steward::facts::CoreEnsureFactsWire::CoreLag {
            pin: super::fixtures::PIN.to_string(),
            newest_complete: None,
        },
    );
    request.snapshot.blocker_lineage =
        FactRead::ready(Some(BlockerLineageWire {
            family: "core_lag".to_string(),
            identity: super::fixtures::PIN.to_string(),
            started_at: Some(AGE_START.to_string()),
            continuous: true,
            identity_changed: false,
        }));
    request.snapshot.issues = FactRead::ready(vec![issue(
        "core_lag",
        super::fixtures::PIN,
        "open",
        Vec::new(),
    )]);
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    match escalation.operation {
        EscalationOpWire::Comment { body, .. } => {
            assert!(body.starts_with("+1"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn same_tick_identity_does_not_duplicate_comment() {
    let mut request = releasable_request();
    request.snapshot.core_ensure = FactRead::ready(
        crate::release_steward::facts::CoreEnsureFactsWire::CoreLag {
            pin: super::fixtures::PIN.to_string(),
            newest_complete: None,
        },
    );
    request.snapshot.blocker_lineage =
        FactRead::ready(Some(BlockerLineageWire {
            family: "core_lag".to_string(),
            identity: super::fixtures::PIN.to_string(),
            started_at: Some(AGE_START.to_string()),
            continuous: true,
            identity_changed: false,
        }));
    request.snapshot.issues = FactRead::ready(vec![issue(
        "core_lag",
        super::fixtures::PIN,
        "open",
        vec![IssueCommentWire {
            tick_identity: TICK.to_string(),
            created_at: NOON.to_string(),
        }],
    )]);
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    assert_eq!(
        escalation.disposition,
        ActionDispositionWire::AlreadySatisfied
    );
}

#[test]
fn recovery_closes_the_open_issue() {
    let mut request = releasable_request();
    request.snapshot.issues = FactRead::ready(vec![issue(
        "core_lag",
        super::fixtures::PIN,
        "open",
        Vec::new(),
    )]);
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    assert_eq!(escalation.severity, EscalationSeverityWire::Recovery);
    assert!(matches!(
        escalation.operation,
        EscalationOpWire::Close { .. }
    ));
}

#[test]
fn uncertain_duration_does_not_invent_24h() {
    let mut request = releasable_request();
    request.snapshot.core_ensure = FactRead::ready(
        crate::release_steward::facts::CoreEnsureFactsWire::CoreLag {
            pin: super::fixtures::PIN.to_string(),
            newest_complete: None,
        },
    );
    request.snapshot.blocker_lineage =
        FactRead::ready(Some(BlockerLineageWire {
            family: "core_lag".to_string(),
            identity: super::fixtures::PIN.to_string(),
            started_at: None,
            continuous: true,
            identity_changed: false,
        }));
    let result = release_steward_decide(&request).unwrap();
    assert!(result.blocker_duration_uncertain);
    assert!(
        result.escalation.is_none() || {
            result
                .escalation
                .as_ref()
                .is_some_and(|esc| esc.severity != EscalationSeverityWire::Aged)
        }
    );
}

#[test]
fn identity_change_resets_duration() {
    let mut request = releasable_request();
    request.snapshot.core_ensure = FactRead::ready(
        crate::release_steward::facts::CoreEnsureFactsWire::CoreLag {
            pin: super::fixtures::PIN.to_string(),
            newest_complete: None,
        },
    );
    request.snapshot.blocker_lineage =
        FactRead::ready(Some(BlockerLineageWire {
            family: "core_lag".to_string(),
            identity: super::fixtures::PIN.to_string(),
            started_at: Some(AGE_START.to_string()),
            continuous: true,
            identity_changed: true,
        }));
    let result = release_steward_decide(&request).unwrap();
    assert!(
        result.escalation.is_none()
            || result
                .escalation
                .as_ref()
                .is_some_and(|esc| esc.severity != EscalationSeverityWire::Aged)
    );
}

#[test]
fn advisory_qualification_uses_ci_label() {
    let mut request = releasable_request();
    let train = active_train();
    let mut eval = publishable_eval(&train.candidate_id, 1);
    eval.publishable = false;
    eval.qualify_red = Some("n1".to_string());
    eval.rerun_eligible = true;
    request.snapshot.trains = FactRead::ready(vec![train]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    match escalation.operation {
        EscalationOpWire::Open { labels, .. } => {
            assert!(labels.iter().any(|label| label == "ci"));
            assert!(labels.iter().any(|label| label == "release-steward"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(escalation.severity, EscalationSeverityWire::Advisory);
}

#[test]
fn immediate_install_outranks_advisory() {
    let mut request = releasable_request();
    let mut train = active_train();
    train.py_files_present = 1;
    let eval = publishable_eval(&train.candidate_id, 1);
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate = FactRead::ready(Some(eval));
    if let FactRead::Ready { value } = &mut request.snapshot.canary {
        value.linux.as_mut().unwrap().verdict = "fail".to_string();
    }
    let escalation = release_steward_decide(&request)
        .unwrap()
        .escalation
        .unwrap();
    assert_eq!(escalation.severity, EscalationSeverityWire::Immediate);
    match escalation.operation {
        EscalationOpWire::Open { family, .. } => {
            assert_eq!(family, "install_broken");
        }
        other => panic!("{other:?}"),
    }
}

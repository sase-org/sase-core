//! Train start at a fresh tag: the steward dispatches `release.yml`
//! `mode=release` once instead of reading `WATCHER_BROKEN`, burns a tag
//! whose run completed without stashing a manifest, and resumes (never
//! re-releases) a sealed publishable generation with no PyPI files.

use crate::release_steward::facts::{FactRead, RunStatusWire};
use crate::release_steward::release_steward_decide;
use crate::release_steward::wire::{
    ActionDispositionWire, PrimaryStateWire, TrainActionKindWire,
};

use super::fixtures::{
    active_train, fresh_train, publishable_eval, releasable_request,
    release_run,
};

fn decide(
    request: crate::release_steward::wire::ReleaseStewardRequestWire,
) -> crate::release_steward::wire::ReleaseStewardResultWire {
    release_steward_decide(&request).unwrap()
}

fn with_fresh_train(
    mut request: crate::release_steward::wire::ReleaseStewardRequestWire,
) -> crate::release_steward::wire::ReleaseStewardRequestWire {
    request.snapshot.trains = FactRead::ready(vec![fresh_train()]);
    // A fresh train carries no candidate evaluator result.
    request.snapshot.candidate = FactRead::ready(None);
    request
}

#[test]
fn fresh_train_proposes_one_release_dispatch() {
    let request = with_fresh_train(releasable_request());
    let result = decide(request);
    assert!(matches!(result.primary, PrimaryStateWire::Waiting { .. }));
    let action = result.train_action.expect("one train action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    match action.action {
        TrainActionKindWire::DispatchRelease {
            tag,
            sha,
            mode,
            generation,
        } => {
            assert_eq!(tag, "v0.19.0");
            assert_eq!(mode, "release");
            assert_eq!(generation, 0);
            assert_eq!(sha, super::fixtures::HEAD);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(result.process_exit, 0);
}

#[test]
fn fresh_train_with_in_flight_run_takes_no_action() {
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value
            .release_yml
            .push(release_run(RunStatusWire::InProgress));
    }
    let result = decide(request);
    let action = result.train_action.expect("one train action");
    assert_eq!(action.disposition, ActionDispositionWire::Suppressed);
    assert!(action.reason.contains("already in flight"));
}

#[test]
fn fresh_train_with_completed_run_burns_the_version() {
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value
            .release_yml
            .push(release_run(RunStatusWire::Completed));
    }
    let result = decide(request);
    let action = result.train_action.expect("one train action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    match action.action {
        TrainActionKindWire::AbandonCandidate {
            candidate_id,
            generation,
            observation_asset,
        } => {
            assert_eq!(candidate_id, fresh_train().candidate_id);
            assert_eq!(generation, 0);
            assert_eq!(
                observation_asset,
                format!("{candidate_id}.gen0.abandoned.json")
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(action
        .preconditions
        .iter()
        .any(|item| item.contains("zero PyPI files")));
    assert_eq!(result.process_exit, 0);
}

#[test]
fn fresh_train_with_public_files_never_burns() {
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.trains {
        value[0].py_files_present = 1;
    }
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value
            .release_yml
            .push(release_run(RunStatusWire::Completed));
    }
    let result = decide(request);
    assert!(result.train_action.is_none());
    match result.primary {
        PrimaryStateWire::Waiting { reason } => {
            assert!(reason.contains("already has public PyPI files"));
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn sealed_publishable_train_without_pypi_resumes() {
    let mut request = releasable_request();
    let train = active_train();
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    let result = decide(request);
    let action = result.train_action.expect("one train action");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    match action.action {
        TrainActionKindWire::DispatchRelease {
            mode, generation, ..
        } => {
            assert_eq!(mode, "resume");
            assert_eq!(generation, 1);
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn fresh_train_with_unreadable_candidate_is_watcher_broken() {
    let mut request = with_fresh_train(releasable_request());
    request.snapshot.candidate = FactRead::unreadable(
        "candidate",
        "evaluator crashed",
        "repair the candidate evaluator",
    );
    let result = decide(request);
    assert!(matches!(
        result.primary,
        PrimaryStateWire::WatcherBroken { .. }
    ));
    assert!(result.train_action.is_none());
    assert_eq!(result.process_exit, 1);
}

#[test]
fn sealed_mismatch_still_fails_closed() {
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.trains {
        value[0].manifest_mismatch = true;
    }
    let result = decide(request);
    match result.primary {
        PrimaryStateWire::WatcherBroken { .. } => {}
        other => panic!("{other:?}"),
    }
    assert_eq!(result.process_exit, 1);
}

/// One train from an open releasable PR with merge authority on, through
/// merge, tag plus draft, the initial release, the sealed manifest,
/// publication and confirmation, to `Released`. Each tick takes at most one
/// train action, and a repeated tick over unchanged facts takes none.
#[test]
fn fixture_walk_drives_one_train_from_releasable_to_released() {
    // Tick 1: an open releasable PR with merge authority on merges.
    let open_pr = releasable_request();
    let first = decide(open_pr);
    let action = first.train_action.expect("merge");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    assert!(matches!(
        action.action,
        TrainActionKindWire::MergeReleasePr { .. }
    ));

    // Tick 2: the merged PR has no tag yet, so publish.yml mints one.
    let mut request = releasable_request();
    if let FactRead::Ready { value } = &mut request.snapshot.release_pr {
        let pr = value.as_mut().unwrap();
        pr.merged = true;
    }
    let second = decide(request);
    let action = second.train_action.expect("metadata dispatch");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    assert!(matches!(
        action.action,
        TrainActionKindWire::DispatchPublishMetadata { .. }
    ));

    // Tick 3: the fresh tag with its draft dispatches one initial release.
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.release_pr {
        value.as_mut().unwrap().merged = true;
    }
    let third = decide(request);
    let action = third.train_action.expect("initial release");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    match &action.action {
        TrainActionKindWire::DispatchRelease {
            mode, generation, ..
        } => {
            assert_eq!(mode, "release");
            assert_eq!(*generation, 0);
        }
        other => panic!("{other:?}"),
    }

    // Tick 4: the identical facts plus the in-flight run take no action.
    let mut request = with_fresh_train(releasable_request());
    if let FactRead::Ready { value } = &mut request.snapshot.release_pr {
        value.as_mut().unwrap().merged = true;
    }
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value
            .release_yml
            .push(release_run(RunStatusWire::InProgress));
    }
    let fourth = decide(request);
    let action = fourth.train_action.expect("suppressed repeat");
    assert_ne!(action.disposition, ActionDispositionWire::Proposed);

    // Tick 5: the sealed publishable generation with no PyPI files resumes.
    let mut request = releasable_request();
    let train = active_train();
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    if let FactRead::Ready { value } = &mut request.snapshot.workflows {
        value
            .release_yml
            .push(release_run(RunStatusWire::Completed));
    }
    let fifth = decide(request);
    let action = fifth.train_action.expect("resume");
    assert_eq!(action.disposition, ActionDispositionWire::Proposed);
    match &action.action {
        TrainActionKindWire::DispatchRelease {
            mode, generation, ..
        } => {
            assert_eq!(mode, "resume");
            assert_eq!(*generation, 1);
        }
        other => panic!("{other:?}"),
    }

    // Tick 6: confirmation completes the train and the version is released.
    // The release PR is long merged, so no open PR remains.
    let mut request = releasable_request();
    request.snapshot.release_pr = FactRead::ready(None);
    let mut train = active_train();
    train.confirmed = true;
    train.confirmation_complete = true;
    request.snapshot.trains = FactRead::ready(vec![train.clone()]);
    request.snapshot.candidate =
        FactRead::ready(Some(publishable_eval(&train.candidate_id, 1)));
    let sixth = decide(request);
    assert!(matches!(sixth.primary, PrimaryStateWire::Released { .. }));
    assert!(sixth.train_action.is_none());
}

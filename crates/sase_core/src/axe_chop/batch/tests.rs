//! Unit coverage for the chop launch batch contract.

use super::*;

fn member(agent_name: &str, source_index: usize) -> ChopBatchMemberWire {
    ChopBatchMemberWire {
        source_index,
        proposal_id: Some(format!("proposal-{source_index}")),
        agent_name: agent_name.to_string(),
        clan: Some("toobig-68".to_string()),
        clan_generation: Some("gen-1".to_string()),
        launch_role: ChopBatchLaunchRoleWire::Join,
        wait_on: None,
        prompt: "Fix the finding.\n#pr".to_string(),
        workspace: "git:sase".to_string(),
        env: BTreeMap::new(),
        model: None,
        effort: None,
        dedupe_key: None,
        tribe: None,
        status: ChopBatchMemberStatusWire::Pending,
        status_reason: None,
    }
}

fn manifest(names: &[&str]) -> ChopBatchManifestWire {
    ChopBatchManifestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        batch_id: "batch-1".to_string(),
        run: ChopBatchRunContextWire {
            routine: "guard".to_string(),
            job: "split".to_string(),
            run_id: "20261117T120000_000001".to_string(),
            run_started_at: None,
            preflight_digest: Some("preflight-abc".to_string()),
            checkpoint_scope: None,
        },
        typed_bundle: Some(ChopBatchTypedBundleRefWire {
            bundle_id: "bundle-7".to_string(),
            bundle_digest: "digest-7".to_string(),
        }),
        clan: Some("toobig-68".to_string()),
        clan_generation: Some("gen-1".to_string()),
        status: ChopBatchStatusWire::Open,
        status_reason: None,
        created_at: None,
        members: names
            .iter()
            .enumerate()
            .map(|(index, name)| member(name, index))
            .collect(),
    }
}

fn transition(
    manifest: &ChopBatchManifestWire,
    event: ChopBatchEventKindWire,
    member_index: Option<usize>,
    reason: Option<&str>,
) -> Result<ChopBatchManifestWire, ChopEngineError> {
    apply_chop_batch_transition(&ChopBatchTransitionRequestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        manifest: manifest.clone(),
        event,
        member_index,
        reason: reason.map(str::to_string),
    })
}

fn dispatch(
    manifest: &ChopBatchManifestWire,
    index: usize,
) -> ChopBatchManifestWire {
    transition(
        manifest,
        ChopBatchEventKindWire::BeginDispatch,
        Some(index),
        None,
    )
    .expect("begin_dispatch must succeed")
}

fn launch(
    manifest: &ChopBatchManifestWire,
    index: usize,
) -> ChopBatchManifestWire {
    let next = dispatch(manifest, index);
    transition(
        &next,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(index),
        None,
    )
    .expect("acknowledge_launch must succeed")
}

fn observation(
    agent_name: &str,
    observation: ChopBatchSpawnObservationKindWire,
) -> ChopBatchSpawnObservationWire {
    ChopBatchSpawnObservationWire {
        agent_name: agent_name.to_string(),
        observation,
        detail: None,
    }
}

fn reconcile(
    manifest: &ChopBatchManifestWire,
    observations: Vec<ChopBatchSpawnObservationWire>,
) -> ChopBatchResumeWire {
    reconcile_chop_batch(&ChopBatchReconcileRequestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        manifest: manifest.clone(),
        observations,
    })
    .expect("reconcile must succeed")
}

#[test]
fn manifest_round_trip_preserves_contract_fields() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let validated =
        validate_chop_batch_manifest(&shown).expect("valid manifest");
    assert_eq!(validated, shown);
    assert_eq!(
        validated.run.preflight_digest.as_deref(),
        Some("preflight-abc")
    );
    assert_eq!(
        validated
            .typed_bundle
            .as_ref()
            .map(|bundle| bundle.bundle_id.as_str()),
        Some("bundle-7")
    );
    let value = serde_json::to_value(&validated).unwrap();
    let revived: ChopBatchManifestWire = serde_json::from_value(value).unwrap();
    assert_eq!(revived, shown);
}

#[test]
fn unknown_schema_and_corrupt_manifests_refuse_dispatch() {
    let mut shown = manifest(&["toobig-68.split.a1"]);
    shown.schema_version = CHOP_BATCH_SCHEMA_VERSION + 1;
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "schema_version_mismatch");

    let mut shown = manifest(&["toobig-68.split.a1"]);
    shown.batch_id = "   ".to_string();
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "blank_value");

    let mut shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a1"]);
    shown.members[1].source_index = 1;
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "duplicate_agent_name");

    let mut shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    shown.members[1].source_index = 0;
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "duplicate_source_index");

    let shown = manifest(&["toobig-68.split.a1"]);
    let request = ChopBatchReconcileRequestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION + 1,
        manifest: shown,
        observations: Vec::new(),
    };
    let error = reconcile_chop_batch(&request).unwrap_err();
    assert_eq!(error.code, "schema_version_mismatch");

    let unknown = serde_json::json!({
        "schema_version": CHOP_BATCH_SCHEMA_VERSION,
        "batch_id": "batch-1",
        "run": {
            "routine": "guard",
            "job": "split",
            "run_id": "run-1",
        },
        "members": [{
            "source_index": 0,
            "agent_name": "toobig-68.split.a1",
            "prompt": "Fix it.\n#pr",
            "workspace": "git:sase",
            "surprise": true,
        }],
    });
    let error = serde_json::from_value::<ChopBatchManifestWire>(unknown)
        .unwrap_err()
        .to_string();
    assert!(error.contains("surprise"), "{error}");
}

#[test]
fn dispatch_acknowledge_flow_marks_launched_before_next_member() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let next = dispatch(&shown, 0);
    assert_eq!(
        next.members[0].status,
        ChopBatchMemberStatusWire::Dispatching
    );
    // Duplicate dispatch intent is idempotent while the spawn is in flight.
    let again = dispatch(&next, 0);
    assert_eq!(
        again.members[0].status,
        ChopBatchMemberStatusWire::Dispatching
    );
    let next = transition(
        &again,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(0),
        None,
    )
    .expect("ack must succeed");
    assert_eq!(next.members[0].status, ChopBatchMemberStatusWire::Launched);
    // Duplicate acknowledgement after a crash-retry is idempotent.
    let again = transition(
        &next,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(0),
        None,
    )
    .expect("duplicate ack must succeed");
    assert_eq!(again.members[0].status, ChopBatchMemberStatusWire::Launched);
    assert_eq!(again.members[1].status, ChopBatchMemberStatusWire::Pending);
}

#[test]
fn acknowledge_without_dispatch_is_rejected() {
    let shown = manifest(&["toobig-68.split.a1"]);
    let error = transition(
        &shown,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(0),
        None,
    )
    .unwrap_err();
    assert_eq!(error.code, "launch_without_dispatch");
}

#[test]
fn terminal_skips_release_members_but_never_reopen_launched_ones() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let next = transition(
        &shown,
        ChopBatchEventKindWire::RecordSkip,
        Some(1),
        Some("wait target dismissed"),
    )
    .expect("skip must succeed");
    assert_eq!(next.members[1].status, ChopBatchMemberStatusWire::Skipped);
    assert_eq!(
        next.members[1].status_reason.as_deref(),
        Some("wait target dismissed")
    );

    let missing = transition(
        &shown,
        ChopBatchEventKindWire::RecordSkip,
        Some(0),
        Some("   "),
    )
    .unwrap_err();
    assert_eq!(missing.code, "missing_reason");

    let launched = launch(&shown, 0);
    let error = transition(
        &launched,
        ChopBatchEventKindWire::RecordSkip,
        Some(0),
        Some("too late"),
    )
    .unwrap_err();
    assert_eq!(error.code, "terminal_state_conflict");

    let error = transition(
        &launched,
        ChopBatchEventKindWire::MarkBlocked,
        Some(0),
        Some("too late"),
    )
    .unwrap_err();
    assert_eq!(error.code, "terminal_state_conflict");
}

#[test]
fn cancellation_wins_over_inflight_dispatch() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let in_flight = dispatch(&shown, 0);
    let next = transition(
        &in_flight,
        ChopBatchEventKindWire::CancelClanGeneration,
        None,
        Some("user killed clan toobig-68"),
    )
    .expect("cancel must succeed");
    assert_eq!(next.status, ChopBatchStatusWire::Cancelled);
    // A launch already crossing the spawn boundary remains dispatching until
    // its real process is acknowledged. Pending work is cancelled immediately.
    assert_eq!(
        next.members[0].status,
        ChopBatchMemberStatusWire::Dispatching
    );
    assert_eq!(next.members[1].status, ChopBatchMemberStatusWire::Cancelled);

    let settled = transition(
        &next,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(0),
        None,
    )
    .expect("the crossing launch must settle after clan cancellation");
    assert_eq!(settled.status, ChopBatchStatusWire::Cancelled);
    assert_eq!(
        settled.members[0].status,
        ChopBatchMemberStatusWire::Launched
    );
    assert_eq!(
        settled.members[1].status,
        ChopBatchMemberStatusWire::Cancelled
    );

    let error =
        transition(&next, ChopBatchEventKindWire::BeginDispatch, Some(0), None)
            .unwrap_err();
    assert_eq!(error.code, "batch_not_open");

    let error = transition(&next, ChopBatchEventKindWire::Resume, None, None)
        .unwrap_err();
    assert_eq!(error.code, "terminal_batch_conflict");

    // Cancellation is idempotent so a retried kill notice still lands.
    let again = transition(
        &next,
        ChopBatchEventKindWire::CancelClanGeneration,
        None,
        Some("user killed clan toobig-68"),
    )
    .expect("duplicate cancel must succeed");
    assert_eq!(again.status, ChopBatchStatusWire::Cancelled);
}

#[test]
fn pause_holds_dispatch_but_lets_the_crossing_launch_settle() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let in_flight = dispatch(&shown, 0);
    let paused = transition(
        &in_flight,
        ChopBatchEventKindWire::RequestPause,
        None,
        Some("sigterm"),
    )
    .expect("pause must succeed");
    assert_eq!(paused.status, ChopBatchStatusWire::Paused);

    let error = transition(
        &paused,
        ChopBatchEventKindWire::BeginDispatch,
        Some(1),
        None,
    )
    .unwrap_err();
    assert_eq!(error.code, "batch_not_open");

    // The launch that already crossed the spawn boundary still settles.
    let settled = transition(
        &paused,
        ChopBatchEventKindWire::AcknowledgeLaunch,
        Some(0),
        None,
    )
    .expect("in-flight ack under pause must succeed");
    assert_eq!(
        settled.members[0].status,
        ChopBatchMemberStatusWire::Launched
    );

    let resumed =
        transition(&settled, ChopBatchEventKindWire::Resume, None, None)
            .expect("resume must succeed");
    assert_eq!(resumed.status, ChopBatchStatusWire::Open);
    let next = dispatch(&resumed, 1);
    assert_eq!(
        next.members[1].status,
        ChopBatchMemberStatusWire::Dispatching
    );
}

#[test]
fn completing_every_member_completes_the_batch() {
    let shown = manifest(&["toobig-68.split.a1", "toobig-68.split.a2"]);
    let first = launch(&shown, 0);
    assert_eq!(first.status, ChopBatchStatusWire::Open);
    let second = launch(&first, 1);
    assert_eq!(second.status, ChopBatchStatusWire::Complete);

    let error = transition(&second, ChopBatchEventKindWire::Resume, None, None)
        .unwrap_err();
    assert_eq!(error.code, "terminal_batch_conflict");
}

#[test]
fn reconcile_selects_only_provable_missing_members() {
    let shown = manifest(&["a1", "a2", "a3", "a4", "a5"]);
    let resumed = reconcile(
        &shown,
        vec![
            observation("a1", ChopBatchSpawnObservationKindWire::MatchedSpawn),
            observation(
                "a2",
                ChopBatchSpawnObservationKindWire::ReservationOnly,
            ),
            observation(
                "a3",
                ChopBatchSpawnObservationKindWire::ForeignConflict,
            ),
            observation("a4", ChopBatchSpawnObservationKindWire::LiveOwner),
        ],
    );
    assert_eq!(resumed.batch_id, "batch-1");
    assert_eq!(resumed.batch_status, ChopBatchStatusWire::Open);
    // a2 has no spawn evidence, a5 was never observed: both need a launch.
    assert_eq!(resumed.to_launch, vec![1, 4]);
    assert_eq!(resumed.already_launched, vec![0]);
    assert_eq!(
        resumed
            .blocked
            .iter()
            .map(|entry| (entry.member_index, entry.reason.as_str()))
            .collect::<Vec<_>>(),
        vec![(2, "foreign_name_conflict"), (3, "live_owner"),]
    );
    assert_eq!(resumed.counts.total, 5);
    assert_eq!(resumed.counts.pending, 5);

    let duplicate = ChopBatchReconcileRequestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        manifest: shown.clone(),
        observations: vec![
            observation("a1", ChopBatchSpawnObservationKindWire::MatchedSpawn),
            observation(
                "a1",
                ChopBatchSpawnObservationKindWire::ReservationOnly,
            ),
        ],
    };
    let error = reconcile_chop_batch(&duplicate).unwrap_err();
    assert_eq!(error.code, "duplicate_observation");
}

#[test]
fn reconcile_never_relaunches_settled_or_cancelled_batches() {
    let shown = manifest(&["a1", "a2", "a3"]);
    let first = launch(&shown, 0);
    let skipped = transition(
        &first,
        ChopBatchEventKindWire::RecordSkip,
        Some(1),
        Some("dismissed"),
    )
    .expect("skip must succeed");
    let resumed = reconcile(&skipped, vec![]);
    assert_eq!(resumed.to_launch, vec![2]);
    assert_eq!(resumed.already_launched, vec![0]);
    assert!(resumed.blocked.is_empty());

    let paused = transition(
        &skipped,
        ChopBatchEventKindWire::RequestPause,
        None,
        Some("sigterm"),
    )
    .expect("pause must succeed");
    let held = reconcile(&paused, vec![]);
    assert!(held.to_launch.is_empty());
    assert_eq!(held.already_launched, vec![0]);
    assert_eq!(
        held.blocked
            .iter()
            .map(|entry| entry.reason.as_str())
            .collect::<Vec<_>>(),
        vec!["batch_paused"]
    );

    let cancelled = transition(
        &skipped,
        ChopBatchEventKindWire::CancelClanGeneration,
        None,
        Some("user kill"),
    )
    .expect("cancel must succeed");
    // The pending member became terminal-cancelled, so nothing is left to
    // hold: the settled launch is preserved and nothing relaunches.
    let discarded = reconcile(&cancelled, vec![]);
    assert!(discarded.to_launch.is_empty());
    assert_eq!(discarded.already_launched, vec![0]);
    assert!(discarded.blocked.is_empty());
    assert_eq!(discarded.counts.cancelled, 1);
    assert_eq!(discarded.counts.skipped, 1);
}

#[test]
fn summarize_counts_every_member_state() {
    let shown = manifest(&["a1", "a2", "a3", "a4"]);
    let first = launch(&shown, 0);
    let skipped = transition(
        &first,
        ChopBatchEventKindWire::RecordSkip,
        Some(1),
        Some("dismissed"),
    )
    .expect("skip must succeed");
    let blocked = transition(
        &skipped,
        ChopBatchEventKindWire::MarkBlocked,
        Some(2),
        Some("ambiguous spawn"),
    )
    .expect("block must succeed");
    let summary = summarize_chop_batch(&blocked).expect("summarize");
    assert_eq!(summary.schema_version, CHOP_BATCH_SCHEMA_VERSION);
    assert_eq!(summary.batch_id, "batch-1");
    assert_eq!(summary.batch_status, ChopBatchStatusWire::Open);
    assert_eq!(
        summary.counts,
        ChopBatchCountsWire {
            total: 4,
            pending: 1,
            dispatching: 0,
            launched: 1,
            skipped: 1,
            blocked: 1,
            cancelled: 0,
        }
    );
}

#[test]
fn wait_references_must_name_batch_members() {
    let mut shown = manifest(&["a1", "a2"]);
    shown.members[1].wait_on = Some(ProposalWaitOnWire::Index(7));
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "unknown_wait_target");

    let mut shown = manifest(&["a1", "a2"]);
    shown.members[1].wait_on =
        Some(ProposalWaitOnWire::Id("ghost".to_string()));
    let error = validate_chop_batch_manifest(&shown).unwrap_err();
    assert_eq!(error.code, "unknown_wait_target");

    let mut shown = manifest(&["a1", "a2"]);
    shown.members[1].proposal_id = Some("second".to_string());
    shown.members[1].wait_on =
        Some(ProposalWaitOnWire::Id("second".to_string()));
    // A member may wait on itself structurally; execution ordering is the
    // launcher's job, so validation accepts the resolvable reference.
    validate_chop_batch_manifest(&shown).expect("resolvable wait ref");
}

fn mixed_clan_manifest() -> ChopBatchManifestWire {
    let mut shown = manifest(&["a.one", "b.one"]);
    shown.members[0].clan = Some("a".to_string());
    shown.members[0].clan_generation = Some("20261118_120000".to_string());
    shown.members[1].clan = Some("b".to_string());
    shown.members[1].clan_generation = Some("20261118_130000".to_string());
    shown
}

fn generation_match(
    manifest: &ChopBatchManifestWire,
    clan: &str,
    generation: &str,
) -> bool {
    chop_batch_generation_matches(&ChopBatchGenerationMatchRequestWire {
        schema_version: CHOP_BATCH_SCHEMA_VERSION,
        manifest: manifest.clone(),
        clan: clan.to_string(),
        generation: generation.to_string(),
    })
    .expect("generation match must succeed")
}

#[test]
fn generation_match_is_scoped_to_the_requested_clan() {
    let shown = mixed_clan_manifest();
    // Wrong generation that belongs to the other clan must not match.
    assert!(!generation_match(&shown, "a", "20261118_130000"));
    assert!(!generation_match(&shown, "b", "20261118_120000"));
    // Each clan's own generation matches and cancels the whole batch.
    assert!(generation_match(&shown, "a", "20261118_120000"));
    assert!(generation_match(&shown, "b", "20261118_130000"));
}

#[test]
fn cancelled_dispatch_settles_without_relaunch() {
    let shown = manifest(&["a1", "a2"]);
    let in_flight = dispatch(&shown, 0);
    let cancelled = transition(
        &in_flight,
        ChopBatchEventKindWire::CancelClanGeneration,
        None,
        Some("user killed clan"),
    )
    .expect("cancel must succeed");
    assert_eq!(
        cancelled.members[0].status,
        ChopBatchMemberStatusWire::Dispatching
    );
    // Dead-executor reconciliation with proof of no spawn settles the
    // in-flight member to cancelled without launching.
    let settled = transition(
        &cancelled,
        ChopBatchEventKindWire::SettleCancelledDispatch,
        Some(0),
        Some("dead executor; no spawn evidence"),
    )
    .expect("settle must succeed");
    assert_eq!(
        settled.members[0].status,
        ChopBatchMemberStatusWire::Cancelled
    );
    assert_eq!(settled.status, ChopBatchStatusWire::Cancelled);
    // Settling a non-dispatching member or a non-cancelled batch refuses.
    let error = transition(
        &settled,
        ChopBatchEventKindWire::SettleCancelledDispatch,
        Some(0),
        Some("again"),
    )
    .unwrap_err();
    assert_eq!(error.code, "terminal_state_conflict");
    let error = transition(
        &shown,
        ChopBatchEventKindWire::SettleCancelledDispatch,
        Some(0),
        Some("not cancelled"),
    )
    .unwrap_err();
    assert_eq!(error.code, "batch_not_cancelled");
}

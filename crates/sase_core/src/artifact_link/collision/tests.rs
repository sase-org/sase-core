use super::super::events::{
    artifact_link_event_digest, reduce_link_events, ArtifactLinkEventEdgeWire,
    ArtifactLinkEventKindWire, ArtifactLinkEventWire,
};
use super::super::wire::ArtifactLinkOriginWire;
use super::*;

fn op(ch: char) -> String {
    ch.to_string().repeat(32)
}

fn commit(ch: char) -> String {
    ch.to_string().repeat(40)
}

fn edge(target: &str) -> ArtifactLinkEventEdgeWire {
    ArtifactLinkEventEdgeWire::Directed {
        source_ref: "agent:reader".to_string(),
        relation: "read".to_string(),
        target_ref: target.to_string(),
    }
}

fn observation(
    operation_id: &str,
    target: &str,
    occurrences: u64,
) -> ArtifactLinkEventWire {
    observation_at(operation_id, target, occurrences, "2026-09-09T12:00:00Z")
}

fn observation_at(
    operation_id: &str,
    target: &str,
    occurrences: u64,
    created_at: &str,
) -> ArtifactLinkEventWire {
    ArtifactLinkEventWire {
        schema_version: 1,
        project_key: "gh_acme__widget".to_string(),
        operation_id: operation_id.to_string(),
        created_by: "agent:reader".to_string(),
        origin: ArtifactLinkOriginWire::Read,
        created_at: created_at.to_string(),
        kind: ArtifactLinkEventKindWire::Observation {
            edge: edge(target),
            description: "read the artifact".to_string(),
            occurrences,
        },
    }
}

fn put(
    operation_id: &str,
    target: &str,
    description: &str,
) -> ArtifactLinkEventWire {
    ArtifactLinkEventWire {
        schema_version: 1,
        project_key: "gh_acme__widget".to_string(),
        operation_id: operation_id.to_string(),
        created_by: "agent:writer".to_string(),
        origin: ArtifactLinkOriginWire::Manual,
        created_at: "2026-09-09T12:01:00Z".to_string(),
        kind: ArtifactLinkEventKindWire::EdgePut {
            edge: edge(target),
            description: description.to_string(),
            observed_operation_ids: Vec::new(),
        },
    }
}

fn remove(operation_id: &str, target: &str) -> ArtifactLinkEventWire {
    ArtifactLinkEventWire {
        schema_version: 1,
        project_key: "gh_acme__widget".to_string(),
        operation_id: operation_id.to_string(),
        created_by: "agent:writer".to_string(),
        origin: ArtifactLinkOriginWire::Manual,
        created_at: "2026-09-09T12:02:00Z".to_string(),
        kind: ArtifactLinkEventKindWire::EdgeRemove {
            edge: edge(target),
            observed_operation_ids: vec![op('e')],
        },
    }
}

fn alias_event(
    operation_id: &str,
    old_ref: &str,
    new_ref: &str,
) -> ArtifactLinkEventWire {
    ArtifactLinkEventWire {
        schema_version: 1,
        project_key: "gh_acme__widget".to_string(),
        operation_id: operation_id.to_string(),
        created_by: "agent:writer".to_string(),
        origin: ArtifactLinkOriginWire::Derived,
        created_at: "2026-09-09T12:03:00Z".to_string(),
        kind: ArtifactLinkEventKindWire::Alias {
            old_ref: old_ref.to_string(),
            new_ref: new_ref.to_string(),
        },
    }
}

fn candidate(
    event: ArtifactLinkEventWire,
    tier: ArtifactLinkCollisionTierWire,
    evidence: ArtifactLinkCollisionEvidenceWire,
    source_locator: Option<&str>,
) -> ArtifactLinkCollisionCandidateWire {
    let canonical_json = artifact_link_event_canonical_json(&event).unwrap();
    let digest = artifact_link_event_digest(&event).unwrap();
    ArtifactLinkCollisionCandidateWire {
        event,
        digest,
        canonical_json,
        tier,
        evidence,
        source_locator: source_locator.map(str::to_string),
    }
}

fn empty_evidence() -> ArtifactLinkCollisionEvidenceWire {
    ArtifactLinkCollisionEvidenceWire::default()
}

fn history_evidence(
    commit_id: &str,
    commit_at: &str,
    ancestors: &[&str],
) -> ArtifactLinkCollisionEvidenceWire {
    ArtifactLinkCollisionEvidenceWire {
        history_complete: true,
        first_commit_id: Some(commit_id.to_string()),
        first_commit_at: Some(commit_at.to_string()),
        ancestor_commit_ids: ancestors
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
        admission_sequence: None,
    }
}

fn sequence_evidence(sequence: u64) -> ArtifactLinkCollisionEvidenceWire {
    ArtifactLinkCollisionEvidenceWire {
        history_complete: false,
        first_commit_id: None,
        first_commit_at: None,
        ancestor_commit_ids: Vec::new(),
        admission_sequence: Some(sequence),
    }
}

fn request(
    candidates: Vec<ArtifactLinkCollisionCandidateWire>,
) -> ArtifactLinkCollisionRequestWire {
    ArtifactLinkCollisionRequestWire {
        schema_version: ARTIFACT_LINK_COLLISION_WIRE_SCHEMA_VERSION,
        candidates,
    }
}

#[test]
fn exact_retries_collapse_without_quarantine() {
    let event = observation(&op('a'), "plan:202609/a.md", 2);
    let first = candidate(
        event.clone(),
        ArtifactLinkCollisionTierWire::Durable,
        empty_evidence(),
        Some("durable:root-a"),
    );
    let second = candidate(
        event.clone(),
        ArtifactLinkCollisionTierWire::Pending,
        empty_evidence(),
        Some("pending:root-b"),
    );
    let partition =
        partition_artifact_link_collision_events(&request(vec![first, second]))
            .unwrap();
    assert_eq!(partition.groups.len(), 1);
    assert_eq!(partition.accepted.len(), 1);
    let group = &partition.groups[0];
    assert!(group.rejected.is_empty());
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::ExactRetryCollapsed
    );
    assert!(group.ambiguity.is_empty());
    // Strongest tier copy represents the collapsed digest.
    assert_eq!(
        partition.accepted[0].tier,
        ArtifactLinkCollisionTierWire::Durable
    );
}

#[test]
fn distinct_operations_are_both_accepted() {
    let first = candidate(
        observation(&op('a'), "plan:202609/a.md", 1),
        ArtifactLinkCollisionTierWire::Durable,
        empty_evidence(),
        None,
    );
    let second = candidate(
        observation(&op('b'), "plan:202609/b.md", 1),
        ArtifactLinkCollisionTierWire::Durable,
        empty_evidence(),
        None,
    );
    let partition =
        partition_artifact_link_collision_events(&request(vec![first, second]))
            .unwrap();
    assert_eq!(partition.groups.len(), 2);
    assert_eq!(partition.accepted.len(), 2);
    for group in &partition.groups {
        assert!(group.rejected.is_empty());
    }
}

#[test]
fn durable_beats_incoming_regardless_of_created_at() {
    // Replay claims an earlier created_at, but durability wins.
    let durable =
        observation_at(&op('a'), "plan:202609/a.md", 1, "2026-09-10T12:00:00Z");
    let replay =
        observation_at(&op('a'), "plan:202609/b.md", 1, "2026-09-01T12:00:00Z");
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            durable.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            Some("durable:store"),
        ),
        candidate(
            replay.clone(),
            ArtifactLinkCollisionTierWire::Incoming,
            empty_evidence(),
            Some("incoming:outbox"),
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(group.rejected.len(), 1);
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::DurableBeatsIncoming
    );
    assert_eq!(group.accepted.event, durable);
    assert_eq!(group.rejected[0].event, replay);
}

#[test]
fn durable_beats_pending_replay() {
    let durable = observation(&op('a'), "plan:202609/a.md", 1);
    let replay = observation(&op('a'), "plan:202609/b.md", 1);
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            durable,
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
        candidate(
            replay,
            ArtifactLinkCollisionTierWire::Pending,
            empty_evidence(),
            None,
        ),
    ]))
    .unwrap();
    assert_eq!(
        partition.groups[0].basis,
        ArtifactLinkCollisionBasisWire::DurableBeatsPending
    );
}

#[test]
fn historical_original_wins_with_identical_timestamps() {
    // Same event created_at and reversed digest/path order must not
    // decide: first-add ancestry picks the original.
    let created_at = "2026-09-09T12:00:00Z";
    let first = observation_at(&op('a'), "plan:202609/a.md", 1, created_at);
    let second = observation_at(&op('a'), "plan:202609/z.md", 1, created_at);
    let first_digest = artifact_link_event_digest(&first).unwrap();
    let second_digest = artifact_link_event_digest(&second).unwrap();
    assert_ne!(first_digest, second_digest);
    // Assign ancestry so the larger digest wins, proving digest order
    // is not the selector.
    let (original, replay) = if first_digest > second_digest {
        (first, second)
    } else {
        (second, first)
    };
    assert!(
        artifact_link_event_digest(&original).unwrap()
            > artifact_link_event_digest(&replay).unwrap()
    );
    let original_commit = commit('a');
    let replay_commit = commit('b');
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            replay.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            history_evidence(
                &replay_commit,
                "2026-09-10T12:00:00Z",
                &[&original_commit],
            ),
            Some("durable:replay"),
        ),
        candidate(
            original.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            history_evidence(&original_commit, "2026-09-09T12:00:00Z", &[]),
            Some("durable:original"),
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::HistoricalAncestorWins
    );
    assert_eq!(group.accepted.event, original);
    assert_eq!(group.rejected[0].event, replay);
}

#[test]
fn historical_commit_time_orders_independent_histories() {
    let first = observation(&op('a'), "plan:202609/a.md", 1);
    let second = observation(&op('a'), "plan:202609/b.md", 1);
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            first.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            history_evidence(&commit('b'), "2026-09-10T12:00:00Z", &[]),
            None,
        ),
        candidate(
            second.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            history_evidence(&commit('a'), "2026-09-09T12:00:00Z", &[]),
            None,
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::HistoricalCommitTimeWins
    );
    assert_eq!(group.accepted.event, second);
}

#[test]
fn missing_evidence_falls_back_to_digest_tie_break() {
    let first = observation(&op('a'), "plan:202609/a.md", 1);
    let second = observation(&op('a'), "plan:202609/b.md", 1);
    let first_digest = artifact_link_event_digest(&first).unwrap();
    let second_digest = artifact_link_event_digest(&second).unwrap();
    let expected = if first_digest < second_digest {
        first.clone()
    } else {
        second.clone()
    };
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            first,
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
        candidate(
            second,
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::DigestTieBreakUnordered
    );
    assert!(!group.ambiguity.is_empty());
    assert_eq!(group.accepted.event, expected);
    assert!(!group.rejected[0].ambiguity.is_empty());
}

#[test]
fn established_evidence_beats_missing_evidence() {
    let with_history = observation(&op('a'), "plan:202609/a.md", 1);
    let without = observation(&op('a'), "plan:202609/b.md", 1);
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            without,
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
        candidate(
            with_history.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            history_evidence(&commit('a'), "2026-09-09T12:00:00Z", &[]),
            None,
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::EstablishedEvidenceWins
    );
    assert_eq!(group.accepted.event, with_history);
}

#[test]
fn admission_sequence_orders_local_only_objects() {
    let first = observation(&op('a'), "plan:202609/a.md", 1);
    let second = observation(&op('a'), "plan:202609/b.md", 1);
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            first,
            ArtifactLinkCollisionTierWire::Durable,
            sequence_evidence(7),
            None,
        ),
        candidate(
            second.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            sequence_evidence(3),
            None,
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(
        group.basis,
        ArtifactLinkCollisionBasisWire::AdmissionSequenceWins
    );
    assert_eq!(group.accepted.event, second);
}

#[test]
fn permutations_and_duplicate_roots_are_stable() {
    let durable = observation(&op('a'), "plan:202609/a.md", 1);
    let replay = observation(&op('a'), "plan:202609/b.md", 1);
    let durable_candidate = || {
        candidate(
            durable.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            Some("durable:root-a"),
        )
    };
    let replay_candidate = || {
        candidate(
            replay.clone(),
            ArtifactLinkCollisionTierWire::Incoming,
            empty_evidence(),
            Some("incoming:root-b"),
        )
    };
    // Duplicate roots: same evidence repeated across roots.
    let base = partition_artifact_link_collision_events(&request(vec![
        durable_candidate(),
        replay_candidate(),
        durable_candidate(),
        replay_candidate(),
    ]))
    .unwrap();
    // Permuted input order gives the same result.
    let permuted = partition_artifact_link_collision_events(&request(vec![
        replay_candidate(),
        durable_candidate(),
        replay_candidate(),
        durable_candidate(),
    ]))
    .unwrap();
    assert_eq!(base, permuted);
    assert_eq!(base.groups[0].accepted.event, durable);
    assert_eq!(base.groups[0].rejected[0].event, replay);
}

#[test]
fn three_payloads_produce_one_accepted_and_two_findings() {
    let durable = observation(&op('a'), "plan:202609/a.md", 1);
    let pending = observation(&op('a'), "plan:202609/b.md", 1);
    let incoming = observation(&op('a'), "plan:202609/c.md", 1);
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            incoming.clone(),
            ArtifactLinkCollisionTierWire::Incoming,
            empty_evidence(),
            None,
        ),
        candidate(
            pending.clone(),
            ArtifactLinkCollisionTierWire::Pending,
            empty_evidence(),
            None,
        ),
        candidate(
            durable.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
    ]))
    .unwrap();
    let group = &partition.groups[0];
    assert_eq!(partition.accepted.len(), 1);
    assert_eq!(group.rejected.len(), 2);
    assert_eq!(group.accepted.event, durable);
    let mut bases: Vec<ArtifactLinkCollisionBasisWire> =
        group.rejected.iter().map(|finding| finding.basis).collect();
    bases.sort_by_key(|basis| rank_basis(*basis));
    assert_eq!(
        bases,
        vec![
            ArtifactLinkCollisionBasisWire::DurableBeatsPending,
            ArtifactLinkCollisionBasisWire::DurableBeatsIncoming,
        ]
    );
    // Every outcome carries its digest, even sharing one operation ID.
    assert_ne!(group.accepted.digest, group.rejected[0].digest);
    assert_ne!(group.accepted.digest, group.rejected[1].digest);
    assert_ne!(group.rejected[0].digest, group.rejected[1].digest);
}

#[test]
fn rejected_kinds_cannot_affect_accepted_reduction() {
    let accepted = put(&op('a'), "plan:202609/a.md", "kept");
    let rejected_observation = observation(&op('a'), "plan:202609/b.md", 5);
    let rejected_remove = remove(&op('a'), "plan:202609/a.md");
    let rejected_alias =
        alias_event(&op('a'), "plan:202609/a.md", "plan:202609/z.md");
    for rejected in [rejected_observation, rejected_remove, rejected_alias] {
        let rejected_digest = artifact_link_event_digest(&rejected).unwrap();
        let partition =
            partition_artifact_link_collision_events(&request(vec![
                candidate(
                    rejected.clone(),
                    ArtifactLinkCollisionTierWire::Incoming,
                    empty_evidence(),
                    None,
                ),
                candidate(
                    accepted.clone(),
                    ArtifactLinkCollisionTierWire::Durable,
                    empty_evidence(),
                    None,
                ),
            ]))
            .unwrap();
        let group = &partition.groups[0];
        assert_eq!(group.accepted.event, accepted);
        assert_eq!(group.rejected.len(), 1);
        assert_eq!(group.rejected[0].digest, rejected_digest);
        // Reducing only the accepted payload succeeds and keeps the
        // accepted description; the rejected payload never enters
        // reduction. The strict reducer still rejects the pair together.
        let accepted_only =
            reduce_link_events(std::slice::from_ref(&accepted), &[]).unwrap();
        assert_eq!(accepted_only.rows[0].description, "kept".to_string());
        assert!(reduce_link_events(&[accepted.clone(), rejected], &[]).is_err());
    }
}

#[test]
fn rejected_put_cannot_supersede_accepted_observation() {
    let accepted = observation(&op('a'), "plan:202609/a.md", 4);
    let rejected = put(&op('a'), "plan:202609/a.md", "discarded");
    let partition = partition_artifact_link_collision_events(&request(vec![
        candidate(
            rejected.clone(),
            ArtifactLinkCollisionTierWire::Incoming,
            empty_evidence(),
            None,
        ),
        candidate(
            accepted.clone(),
            ArtifactLinkCollisionTierWire::Durable,
            empty_evidence(),
            None,
        ),
    ]))
    .unwrap();
    assert_eq!(partition.groups[0].accepted.event, accepted);
    let accepted_only =
        reduce_link_events(std::slice::from_ref(&accepted), &[]).unwrap();
    assert_eq!(accepted_only.rows[0].uses, 4);
    assert!(reduce_link_events(&[accepted, rejected], &[]).is_err());
}

#[test]
fn invalid_individual_events_still_error() {
    let mut bad = observation(&op('a'), "plan:202609/a.md", 1);
    bad.operation_id = "not-an-operation-id".to_string();
    let candidate = ArtifactLinkCollisionCandidateWire {
        event: bad,
        digest: "0".repeat(64),
        canonical_json: "{}".to_string(),
        tier: ArtifactLinkCollisionTierWire::Durable,
        evidence: empty_evidence(),
        source_locator: None,
    };
    assert!(partition_artifact_link_collision_events(&request(vec![
        candidate
    ]))
    .unwrap_err()
    .message
    .contains("candidates[0].event is invalid"));
    let zero = observation(&op('b'), "plan:202609/a.md", 0);
    let canonical_json =
        artifact_link_event_canonical_json(&zero).unwrap_or_default();
    let digest =
        artifact_link_event_digest(&zero).unwrap_or_else(|_| "0".repeat(64));
    let candidate = ArtifactLinkCollisionCandidateWire {
        event: zero,
        digest,
        canonical_json,
        tier: ArtifactLinkCollisionTierWire::Durable,
        evidence: empty_evidence(),
        source_locator: None,
    };
    assert!(partition_artifact_link_collision_events(&request(vec![
        candidate
    ]))
    .is_err());
}

#[test]
fn quarantine_record_path_validate_and_canonicalize() {
    let accepted = observation(&op('a'), "plan:202609/a.md", 1);
    let rejected = observation(&op('a'), "plan:202609/b.md", 1);
    let accepted_digest = artifact_link_event_digest(&accepted).unwrap();
    let rejected_digest = artifact_link_event_digest(&rejected).unwrap();
    let rejected_canonical_json =
        artifact_link_event_canonical_json(&rejected).unwrap();
    let path = artifact_link_quarantine_record_path(&op('a'), &rejected_digest)
        .unwrap();
    assert_eq!(
        path,
        format!(
            "link-events/quarantine/v1/{}/{}.json",
            op('a'),
            rejected_digest
        )
    );
    let record = ArtifactLinkQuarantineRecordWire {
        schema_version: ARTIFACT_LINK_QUARANTINE_WIRE_SCHEMA_VERSION,
        project_key: "gh_acme__widget".to_string(),
        operation_id: op('a'),
        accepted_digest: accepted_digest.clone(),
        rejected_digest: rejected_digest.clone(),
        rejected_event: rejected.clone(),
        rejected_canonical_json: rejected_canonical_json.clone(),
        selection_basis: ArtifactLinkCollisionBasisWire::DurableBeatsIncoming,
        ambiguity: vec!["missing durability ordering".to_string()],
        source_locator: Some("incoming:outbox".to_string()),
        detail: "durable beats incoming".to_string(),
    };
    let validated = validate_artifact_link_quarantine_record(&record).unwrap();
    assert_eq!(validated.accepted_digest, accepted_digest);
    assert_eq!(validated.rejected_digest, rejected_digest);
    let canonical =
        artifact_link_quarantine_record_canonical_json(&record).unwrap();
    assert!(canonical.ends_with('\n'));
    assert!(canonical.contains(&rejected_digest));
    // Single-payload bases are not quarantine evidence.
    let mut bad_basis = record.clone();
    bad_basis.selection_basis = ArtifactLinkCollisionBasisWire::Single;
    assert!(validate_artifact_link_quarantine_record(&bad_basis)
        .unwrap_err()
        .message
        .contains("must describe a collision"));
    // Mismatched canonical bytes are corruption, not an overwrite.
    let mut bad_bytes = record.clone();
    bad_bytes.rejected_canonical_json = "{}".to_string();
    assert!(validate_artifact_link_quarantine_record(&bad_bytes)
        .unwrap_err()
        .message
        .contains("canonical bytes"));
}

#[test]
fn strict_reducer_still_rejects_operation_id_collisions() {
    let first = observation(&op('a'), "plan:202609/a.md", 1);
    let second = observation(&op('a'), "plan:202609/b.md", 1);
    assert!(reduce_link_events(&[first, second], &[])
        .unwrap_err()
        .message
        .contains("reused"));
}

//! Deterministic collision partition and quarantine contracts for artifact-link events.
//!
//! Python gathers filesystem/Git evidence, performs side effects, and renders
//! results. It must not implement winner selection: this module owns the
//! canonical policy. Filesystem reads, Git subprocesses, and commits stay in
//! sase's adapter.
//!
//! Canonical identity is canonical event bytes/digest, not just operation ID.
//! Exact retries (same digest) collapse without quarantine. Different payloads
//! sharing one project/operation ID produce one accepted payload plus typed
//! quarantine findings for the others.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::events::{
    artifact_link_event_canonical_json, canonical_json_for_serializable,
    canonicalize_artifact_link_event, sha256_hex, validate_operation_id,
    validate_project_key, validate_sha256_digest, validate_single_line,
    ArtifactLinkEventWire,
};
use super::wire::ArtifactLinkError;

/// Schema version for collision partition request/response wires.
pub const ARTIFACT_LINK_COLLISION_WIRE_SCHEMA_VERSION: u64 = 1;

/// Schema version for versioned quarantine record wires.
pub const ARTIFACT_LINK_QUARANTINE_WIRE_SCHEMA_VERSION: u64 = 1;

/// Versioned namespace for persisted quarantine evidence, outside the active
/// `link-events/v1` scan.
pub const ARTIFACT_LINK_QUARANTINE_PATH_PREFIX: &str =
    "link-events/quarantine/v1";

/// Durability tier of one collision candidate.
///
/// Already durable payloads beat pending and incoming replays, even when a
/// replay claims an earlier `created_at`. Pending beats incoming.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkCollisionTierWire {
    Incoming,
    Pending,
    Durable,
}

impl ArtifactLinkCollisionTierWire {
    fn rank(self) -> u8 {
        match self {
            Self::Incoming => 0,
            Self::Pending => 1,
            Self::Durable => 2,
        }
    }
}

/// Explicit durability provenance for one candidate.
///
/// For historical durable collisions Python supplies first-add Git history
/// only for the collided digests: `first_commit_id`, `first_commit_at`, and
/// the ancestor set of the first-add commit. Ancestry distinguishes an
/// original from a descendant replay; `first_commit_at` orders independent
/// histories. Neither directory enumeration, digest order, checkout mtime,
/// nor event `created_at` proves which payload was durable first.
///
/// For future local-only objects Python records durable admission evidence
/// under the existing publication lock as `admission_sequence` so restart
/// retains the first accepted payload. Older local-only objects,
/// shallow/missing history, and simultaneous independent histories may not
/// provide a provable ordering: set `history_complete` false, leave commit
/// fields empty, and the policy falls back to a stable digest tie-break while
/// reporting the ambiguity. Input order and repeated copies across roots
/// never alter the result when the same evidence is supplied.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionEvidenceWire {
    #[serde(default)]
    pub history_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_commit_at: Option<String>,
    #[serde(default)]
    pub ancestor_commit_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_sequence: Option<u64>,
}

/// One candidate payload for partition input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionCandidateWire {
    pub event: ArtifactLinkEventWire,
    pub digest: String,
    pub canonical_json: String,
    pub tier: ArtifactLinkCollisionTierWire,
    #[serde(default)]
    pub evidence: ArtifactLinkCollisionEvidenceWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_locator: Option<String>,
}

/// Partition request: candidates across durability tiers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionRequestWire {
    pub schema_version: u64,
    pub candidates: Vec<ArtifactLinkCollisionCandidateWire>,
}

/// Why one payload won over another in the same project/operation group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactLinkCollisionBasisWire {
    Single,
    ExactRetryCollapsed,
    DurableBeatsPending,
    DurableBeatsIncoming,
    PendingBeatsIncoming,
    HistoricalAncestorWins,
    HistoricalCommitTimeWins,
    EstablishedEvidenceWins,
    AdmissionSequenceWins,
    DigestTieBreakUnordered,
}

impl ArtifactLinkCollisionBasisWire {
    fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::ExactRetryCollapsed => "exact_retry_collapsed",
            Self::DurableBeatsPending => "durable_beats_pending",
            Self::DurableBeatsIncoming => "durable_beats_incoming",
            Self::PendingBeatsIncoming => "pending_beats_incoming",
            Self::HistoricalAncestorWins => "historical_ancestor_wins",
            Self::HistoricalCommitTimeWins => "historical_commit_time_wins",
            Self::EstablishedEvidenceWins => "established_evidence_wins",
            Self::AdmissionSequenceWins => "admission_sequence_wins",
            Self::DigestTieBreakUnordered => "digest_tie_break_unordered",
        }
    }
}

/// One accepted payload, associated with its digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionOutcomeWire {
    pub digest: String,
    pub event: ArtifactLinkEventWire,
    pub tier: ArtifactLinkCollisionTierWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_locator: Option<String>,
}

/// One rejected payload with its selection basis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionFindingWire {
    pub digest: String,
    pub event: ArtifactLinkEventWire,
    pub tier: ArtifactLinkCollisionTierWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_locator: Option<String>,
    pub basis: ArtifactLinkCollisionBasisWire,
    pub detail: String,
    #[serde(default)]
    pub ambiguity: Vec<String>,
}

/// One project/operation collision group and its decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionGroupWire {
    pub project_key: String,
    pub operation_id: String,
    pub accepted: ArtifactLinkCollisionOutcomeWire,
    #[serde(default)]
    pub rejected: Vec<ArtifactLinkCollisionFindingWire>,
    pub basis: ArtifactLinkCollisionBasisWire,
    #[serde(default)]
    pub ambiguity: Vec<String>,
    pub detail: String,
}

/// Partition result: accepted payloads plus per-group decisions.
///
/// Every outcome (accepted or rejected) carries its digest, so a consumer can
/// associate both sides of a collision even when they share one operation ID.
/// Callers reduce only `accepted` events with the strict reducer; rejected
/// payloads never enter reduction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkCollisionPartitionWire {
    pub schema_version: u64,
    pub accepted: Vec<ArtifactLinkCollisionOutcomeWire>,
    #[serde(default)]
    pub groups: Vec<ArtifactLinkCollisionGroupWire>,
}

/// Versioned quarantine record for one rejected payload.
///
/// Provenance lives here, separate from the immutable event schema. The file
/// persists at
/// `link-events/quarantine/v1/<operation_id>/<rejected_digest>.json` and
/// survives restart; repeat replays must not grow duplicate records. An
/// existing record with different bytes is corruption, not permission to
/// overwrite it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkQuarantineRecordWire {
    pub schema_version: u64,
    pub project_key: String,
    pub operation_id: String,
    pub accepted_digest: String,
    pub rejected_digest: String,
    pub rejected_event: ArtifactLinkEventWire,
    pub rejected_canonical_json: String,
    pub selection_basis: ArtifactLinkCollisionBasisWire,
    #[serde(default)]
    pub ambiguity: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_locator: Option<String>,
    pub detail: String,
}

/// Return the persisted path for one quarantine record.
pub fn artifact_link_quarantine_record_path(
    operation_id: &str,
    rejected_digest: &str,
) -> Result<String, ArtifactLinkError> {
    let operation_id = validate_operation_id("operation_id", operation_id)?;
    let rejected_digest = validate_sha256_digest(rejected_digest)?;
    Ok(format!(
        "{}/{}/{}.json",
        ARTIFACT_LINK_QUARANTINE_PATH_PREFIX, operation_id, rejected_digest
    ))
}

/// Return sorted compact JSON plus one trailing newline for one record.
pub fn artifact_link_quarantine_record_canonical_json(
    record: &ArtifactLinkQuarantineRecordWire,
) -> Result<String, ArtifactLinkError> {
    let record = validate_artifact_link_quarantine_record(record)?;
    canonical_json_for_serializable(&record)
}

/// Validate one quarantine record and return its canonical form.
pub fn validate_artifact_link_quarantine_record(
    record: &ArtifactLinkQuarantineRecordWire,
) -> Result<ArtifactLinkQuarantineRecordWire, ArtifactLinkError> {
    if record.schema_version != ARTIFACT_LINK_QUARANTINE_WIRE_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "unsupported artifact link quarantine schema_version {}; expected {}",
            record.schema_version,
            ARTIFACT_LINK_QUARANTINE_WIRE_SCHEMA_VERSION
        )));
    }
    let project_key = validate_project_key(&record.project_key)?;
    let operation_id =
        validate_operation_id("operation_id", &record.operation_id)?;
    let accepted_digest = validate_sha256_digest(&record.accepted_digest)?;
    let rejected_digest = validate_sha256_digest(&record.rejected_digest)?;
    if accepted_digest == rejected_digest {
        return Err(ArtifactLinkError::validation(
            "artifact link quarantine accepted_digest and rejected_digest must differ",
        ));
    }
    let rejected_event =
        canonicalize_artifact_link_event(&record.rejected_event)?;
    if rejected_event.project_key != project_key {
        return Err(ArtifactLinkError::validation(format!(
            "artifact link quarantine project_key `{}` does not match rejected event project `{}`",
            project_key, rejected_event.project_key
        )));
    }
    if rejected_event.operation_id != operation_id {
        return Err(ArtifactLinkError::validation(format!(
            "artifact link quarantine operation_id `{}` does not match rejected event operation `{}`",
            operation_id, rejected_event.operation_id
        )));
    }
    let canonical_json = artifact_link_event_canonical_json(&rejected_event)?;
    if record.rejected_canonical_json != canonical_json {
        return Err(ArtifactLinkError::validation(
            "artifact link quarantine rejected_canonical_json does not match rejected event canonical bytes",
        ));
    }
    let computed_digest = sha256_hex(canonical_json.as_bytes());
    if rejected_digest != computed_digest {
        return Err(ArtifactLinkError::validation(
            "artifact link quarantine rejected_digest does not match rejected event digest",
        ));
    }
    match record.selection_basis {
        ArtifactLinkCollisionBasisWire::Single
        | ArtifactLinkCollisionBasisWire::ExactRetryCollapsed => {
            return Err(ArtifactLinkError::validation(
                "artifact link quarantine selection_basis must describe a collision, not a single payload",
            ));
        }
        _ => {}
    }
    let mut ambiguity = Vec::new();
    for entry in &record.ambiguity {
        ambiguity.push(validate_single_line("ambiguity", entry)?);
    }
    let source_locator = record
        .source_locator
        .as_ref()
        .map(|locator| validate_single_line("source_locator", locator))
        .transpose()?;
    let detail = validate_single_line("detail", &record.detail)?;
    let mut ancestors_note = ambiguity;
    ancestors_note.sort();
    ancestors_note.dedup();
    Ok(ArtifactLinkQuarantineRecordWire {
        schema_version: ARTIFACT_LINK_QUARANTINE_WIRE_SCHEMA_VERSION,
        project_key,
        operation_id,
        accepted_digest,
        rejected_digest,
        rejected_event,
        rejected_canonical_json: canonical_json,
        selection_basis: record.selection_basis,
        ambiguity: ancestors_note,
        source_locator,
        detail,
    })
}

/// Partition collision candidates into accepted payloads and quarantine findings.
///
/// Rules, in order:
/// - exact retries (same digest) collapse without quarantine;
/// - an already durable payload beats pending/incoming replays regardless of
///   `created_at`;
/// - among same-tier durables, ancestry beats commit time, established
///   evidence beats missing evidence, admission sequence orders local-only
///   objects, and a stable digest tie-break covers only genuinely unordered
///   candidates while reporting the ambiguity.
///
/// Invalid individual events still error. Input order and repeated copies
/// across roots do not alter the result when the same evidence is supplied.
pub fn partition_artifact_link_collision_events(
    request: &ArtifactLinkCollisionRequestWire,
) -> Result<ArtifactLinkCollisionPartitionWire, ArtifactLinkError> {
    if request.schema_version != ARTIFACT_LINK_COLLISION_WIRE_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "unsupported artifact link collision schema_version {}; expected {}",
            request.schema_version,
            ARTIFACT_LINK_COLLISION_WIRE_SCHEMA_VERSION
        )));
    }
    let mut validated: Vec<ValidatedCandidate> =
        Vec::with_capacity(request.candidates.len());
    for (index, candidate) in request.candidates.iter().enumerate() {
        validated.push(validate_candidate(index, candidate)?);
    }
    let mut groups: BTreeMap<(String, String), Vec<ValidatedCandidate>> =
        BTreeMap::new();
    for candidate in validated {
        groups
            .entry((
                candidate.project_key.clone(),
                candidate.operation_id.clone(),
            ))
            .or_default()
            .push(candidate);
    }
    let mut out_groups = Vec::new();
    let mut accepted_all = Vec::new();
    for ((project_key, operation_id), members) in groups {
        let group = decide_group(&project_key, &operation_id, members)?;
        accepted_all.push(group.accepted.clone());
        out_groups.push(group);
    }
    out_groups.sort_by(|left, right| {
        (&left.project_key, &left.operation_id)
            .cmp(&(&right.project_key, &right.operation_id))
    });
    accepted_all.sort_by(|left, right| {
        (
            &left.event.project_key,
            &left.event.operation_id,
            &left.digest,
        )
            .cmp(&(
                &right.event.project_key,
                &right.event.operation_id,
                &right.digest,
            ))
    });
    Ok(ArtifactLinkCollisionPartitionWire {
        schema_version: ARTIFACT_LINK_COLLISION_WIRE_SCHEMA_VERSION,
        accepted: accepted_all,
        groups: out_groups,
    })
}

#[derive(Debug, Clone)]
struct ValidatedCandidate {
    project_key: String,
    operation_id: String,
    digest: String,
    event: ArtifactLinkEventWire,
    tier: ArtifactLinkCollisionTierWire,
    evidence: ArtifactLinkCollisionEvidenceWire,
    source_locator: Option<String>,
}

fn validate_candidate(
    index: usize,
    candidate: &ArtifactLinkCollisionCandidateWire,
) -> Result<ValidatedCandidate, ArtifactLinkError> {
    let label = format!("candidates[{index}]");
    let event = canonicalize_artifact_link_event(&candidate.event).map_err(
        |error| {
            ArtifactLinkError::validation(format!(
                "{label}.event is invalid: {}",
                error.message
            ))
        },
    )?;
    let canonical_json = artifact_link_event_canonical_json(&event)?;
    if candidate.canonical_json != canonical_json {
        return Err(ArtifactLinkError::validation(format!(
            "{label}.canonical_json does not match event canonical bytes"
        )));
    }
    let digest =
        validate_sha256_digest(&candidate.digest).map_err(|error| {
            ArtifactLinkError::validation(format!(
                "{label}.digest is invalid: {}",
                error.message
            ))
        })?;
    let computed = sha256_hex(canonical_json.as_bytes());
    if digest != computed {
        return Err(ArtifactLinkError::validation(format!(
            "{label}.digest does not match event digest"
        )));
    }
    let evidence = validate_evidence(&label, &candidate.evidence)?;
    let source_locator = candidate
        .source_locator
        .as_ref()
        .map(|locator| {
            validate_single_line(&format!("{label}.source_locator"), locator)
        })
        .transpose()?;
    Ok(ValidatedCandidate {
        project_key: event.project_key.clone(),
        operation_id: event.operation_id.clone(),
        digest,
        event,
        tier: candidate.tier,
        evidence,
        source_locator,
    })
}

fn validate_evidence(
    label: &str,
    evidence: &ArtifactLinkCollisionEvidenceWire,
) -> Result<ArtifactLinkCollisionEvidenceWire, ArtifactLinkError> {
    let first_commit_id = evidence
        .first_commit_id
        .as_ref()
        .map(|value| validate_commit_id(&format!("{label}.evidence"), value))
        .transpose()?;
    let first_commit_at = evidence
        .first_commit_at
        .as_ref()
        .map(|value| {
            validate_single_line(
                &format!("{label}.evidence.first_commit_at"),
                value,
            )
        })
        .transpose()?;
    if evidence.history_complete
        && (first_commit_id.is_none() || first_commit_at.is_none())
    {
        return Err(ArtifactLinkError::validation(format!(
            "{label}.evidence.history_complete requires first_commit_id and first_commit_at"
        )));
    }
    let mut ancestors = Vec::new();
    for value in &evidence.ancestor_commit_ids {
        ancestors
            .push(validate_commit_id(&format!("{label}.evidence"), value)?);
    }
    ancestors.sort();
    ancestors.dedup();
    Ok(ArtifactLinkCollisionEvidenceWire {
        history_complete: evidence.history_complete,
        first_commit_id,
        first_commit_at,
        ancestor_commit_ids: ancestors,
        admission_sequence: evidence.admission_sequence,
    })
}

fn validate_commit_id(
    label: &str,
    value: &str,
) -> Result<String, ArtifactLinkError> {
    let value =
        validate_single_line(&format!("{label}.first_commit_id"), value)
            .or_else(|_| {
                validate_single_line(
                    &format!("{label}.ancestor_commit_id"),
                    value,
                )
            })?;
    if value.len() < 7 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ArtifactLinkError::validation(format!(
            "{label} commit id must be at least 7 hexadecimal characters"
        )));
    }
    Ok(value)
}

#[derive(Debug, Clone)]
struct DigestEntry {
    digest: String,
    event: ArtifactLinkEventWire,
    tier: ArtifactLinkCollisionTierWire,
    evidence: ArtifactLinkCollisionEvidenceWire,
    source_locator: Option<String>,
}

fn decide_group(
    project_key: &str,
    operation_id: &str,
    members: Vec<ValidatedCandidate>,
) -> Result<ArtifactLinkCollisionGroupWire, ArtifactLinkError> {
    let mut by_digest: BTreeMap<String, Vec<ValidatedCandidate>> =
        BTreeMap::new();
    for member in members {
        by_digest
            .entry(member.digest.clone())
            .or_default()
            .push(member);
    }
    let mut entries: Vec<(DigestEntry, usize)> = Vec::new();
    for (digest, copies) in by_digest {
        let count = copies.len();
        entries.push((representative_entry(digest, copies), count));
    }
    entries.sort_by(|left, right| left.0.digest.cmp(&right.0.digest));
    if entries.len() == 1 {
        let (only, count) = entries.into_iter().next().expect("one entry");
        let basis = ArtifactLinkCollisionBasisWire::Single;
        let (basis, detail) = if count > 1 {
            (
                ArtifactLinkCollisionBasisWire::ExactRetryCollapsed,
                format!(
                    "exact retries collapse for {}:{} digest {}",
                    project_key, operation_id, only.digest
                ),
            )
        } else {
            (
                basis,
                format!(
                    "single payload for {}:{} digest {}",
                    project_key, operation_id, only.digest
                ),
            )
        };
        let accepted = ArtifactLinkCollisionOutcomeWire {
            digest: only.digest.clone(),
            event: only.event.clone(),
            tier: only.tier,
            source_locator: only.source_locator.clone(),
        };
        return Ok(ArtifactLinkCollisionGroupWire {
            project_key: project_key.to_string(),
            operation_id: operation_id.to_string(),
            accepted,
            rejected: Vec::new(),
            basis,
            ambiguity: Vec::new(),
            detail,
        });
    }
    let plain: Vec<DigestEntry> =
        entries.into_iter().map(|(entry, _)| entry).collect();
    let winner = select_winner(&plain);
    let winner_entry = plain
        .iter()
        .find(|entry| entry.digest == winner)
        .expect("winner digest present")
        .clone();
    let mut rejected = Vec::new();
    let mut group_ambiguity = BTreeSet::new();
    for entry in &plain {
        if entry.digest == winner_entry.digest {
            continue;
        }
        let (basis, detail, ambiguity) = pairwise_basis(&winner_entry, entry);
        for note in &ambiguity {
            group_ambiguity.insert(note.clone());
        }
        rejected.push(ArtifactLinkCollisionFindingWire {
            digest: entry.digest.clone(),
            event: entry.event.clone(),
            tier: entry.tier,
            source_locator: entry.source_locator.clone(),
            basis,
            detail,
            ambiguity,
        });
    }
    rejected.sort_by(|left, right| left.digest.cmp(&right.digest));
    let runner_up = rejected
        .iter()
        .min_by(|left, right| {
            rank_basis(left.basis).cmp(&rank_basis(right.basis))
        })
        .expect("at least one rejection");
    let group_basis = runner_up.basis;
    for note in &runner_up.ambiguity {
        group_ambiguity.insert(note.clone());
    }
    let mut ambiguity: Vec<String> = group_ambiguity.into_iter().collect();
    ambiguity.sort();
    let detail = format!(
        "collision for {}:{} selects {} over {} rejected payload(s) via {}",
        project_key,
        operation_id,
        winner_entry.digest,
        rejected.len(),
        group_basis.as_str(),
    );
    let accepted = ArtifactLinkCollisionOutcomeWire {
        digest: winner_entry.digest.clone(),
        event: winner_entry.event.clone(),
        tier: winner_entry.tier,
        source_locator: winner_entry.source_locator.clone(),
    };
    Ok(ArtifactLinkCollisionGroupWire {
        project_key: project_key.to_string(),
        operation_id: operation_id.to_string(),
        accepted,
        rejected,
        basis: group_basis,
        ambiguity,
        detail,
    })
}

fn representative_entry(
    digest: String,
    mut copies: Vec<ValidatedCandidate>,
) -> DigestEntry {
    copies.sort_by(|left, right| {
        right
            .tier
            .rank()
            .cmp(&left.tier.rank())
            .then_with(|| {
                right
                    .evidence
                    .history_complete
                    .cmp(&left.evidence.history_complete)
            })
            .then_with(|| {
                rank_sequence(&left.evidence)
                    .cmp(&rank_sequence(&right.evidence))
            })
            .then_with(|| left.source_locator.cmp(&right.source_locator))
    });
    let best = copies.into_iter().next().expect("at least one copy");
    DigestEntry {
        digest,
        event: best.event,
        tier: best.tier,
        evidence: best.evidence,
        source_locator: best.source_locator,
    }
}

fn rank_sequence(evidence: &ArtifactLinkCollisionEvidenceWire) -> (u8, u64) {
    match evidence.admission_sequence {
        Some(sequence) => (0, sequence),
        None => (1, u64::MAX),
    }
}

fn evidence_strength(entry: &DigestEntry) -> u8 {
    if entry.evidence.history_complete
        && entry.evidence.first_commit_id.is_some()
        && entry.evidence.first_commit_at.is_some()
    {
        2
    } else if entry.evidence.admission_sequence.is_some() {
        1
    } else {
        0
    }
}

fn select_winner(entries: &[DigestEntry]) -> String {
    let mut best = &entries[0];
    for candidate in &entries[1..] {
        if compares_greater(candidate, best) {
            best = candidate;
        }
    }
    best.digest.clone()
}

fn compares_greater(left: &DigestEntry, right: &DigestEntry) -> bool {
    if left.tier.rank() != right.tier.rank() {
        return left.tier.rank() > right.tier.rank();
    }
    let left_strength = evidence_strength(left);
    let right_strength = evidence_strength(right);
    if left_strength != right_strength {
        return left_strength > right_strength;
    }
    if left_strength == 2 {
        let left_commit =
            left.evidence.first_commit_id.as_ref().expect("checked");
        let right_commit =
            right.evidence.first_commit_id.as_ref().expect("checked");
        let left_ancestors: BTreeSet<&String> =
            left.evidence.ancestor_commit_ids.iter().collect();
        let right_ancestors: BTreeSet<&String> =
            right.evidence.ancestor_commit_ids.iter().collect();
        let left_is_ancestor = right_ancestors.contains(left_commit);
        let right_is_ancestor = left_ancestors.contains(right_commit);
        if left_is_ancestor != right_is_ancestor {
            return left_is_ancestor;
        }
        let left_at = left.evidence.first_commit_at.as_ref().expect("checked");
        let right_at =
            right.evidence.first_commit_at.as_ref().expect("checked");
        if left_at != right_at {
            return left_at < right_at;
        }
        return left.digest < right.digest;
    }
    if left_strength == 1 {
        let left_sequence = left.evidence.admission_sequence.expect("checked");
        let right_sequence =
            right.evidence.admission_sequence.expect("checked");
        if left_sequence != right_sequence {
            return left_sequence < right_sequence;
        }
        return left.digest < right.digest;
    }
    left.digest < right.digest
}

fn pairwise_basis(
    winner: &DigestEntry,
    loser: &DigestEntry,
) -> (ArtifactLinkCollisionBasisWire, String, Vec<String>) {
    if winner.tier.rank() != loser.tier.rank() {
        let basis = match (winner.tier, loser.tier) {
            (
                ArtifactLinkCollisionTierWire::Durable,
                ArtifactLinkCollisionTierWire::Pending,
            ) => ArtifactLinkCollisionBasisWire::DurableBeatsPending,
            (
                ArtifactLinkCollisionTierWire::Durable,
                ArtifactLinkCollisionTierWire::Incoming,
            ) => ArtifactLinkCollisionBasisWire::DurableBeatsIncoming,
            (
                ArtifactLinkCollisionTierWire::Pending,
                ArtifactLinkCollisionTierWire::Incoming,
            ) => ArtifactLinkCollisionBasisWire::PendingBeatsIncoming,
            _ => ArtifactLinkCollisionBasisWire::EstablishedEvidenceWins,
        };
        let detail = format!(
            "{} beats {} for digest {} over {}",
            tier_name(winner.tier),
            tier_name(loser.tier),
            winner.digest,
            loser.digest,
        );
        return (basis, detail, Vec::new());
    }
    let winner_strength = evidence_strength(winner);
    let loser_strength = evidence_strength(loser);
    if winner_strength != loser_strength {
        let detail = format!(
            "established evidence wins for digest {} over {}",
            winner.digest, loser.digest,
        );
        let mut ambiguity = Vec::new();
        if loser_strength == 0 {
            ambiguity.push(format!(
                "rejected digest {} lacks provable durability ordering",
                loser.digest,
            ));
        }
        return (
            ArtifactLinkCollisionBasisWire::EstablishedEvidenceWins,
            detail,
            ambiguity,
        );
    }
    if winner_strength == 2 {
        let winner_commit =
            winner.evidence.first_commit_id.as_ref().expect("checked");
        let loser_commit =
            loser.evidence.first_commit_id.as_ref().expect("checked");
        let winner_ancestors: BTreeSet<&String> =
            winner.evidence.ancestor_commit_ids.iter().collect();
        let loser_ancestors: BTreeSet<&String> =
            loser.evidence.ancestor_commit_ids.iter().collect();
        if loser_ancestors.contains(winner_commit)
            && !winner_ancestors.contains(loser_commit)
        {
            return (
                ArtifactLinkCollisionBasisWire::HistoricalAncestorWins,
                format!(
                    "first-add ancestor {} wins over descendant {}",
                    winner.digest, loser.digest,
                ),
                Vec::new(),
            );
        }
        let winner_at =
            winner.evidence.first_commit_at.as_ref().expect("checked");
        let loser_at =
            loser.evidence.first_commit_at.as_ref().expect("checked");
        if winner_at != loser_at {
            return (
                ArtifactLinkCollisionBasisWire::HistoricalCommitTimeWins,
                format!(
                    "earlier first-add {} wins over {}",
                    winner.digest, loser.digest,
                ),
                Vec::new(),
            );
        }
        return (
            ArtifactLinkCollisionBasisWire::DigestTieBreakUnordered,
            format!(
                "identical first-add times; digest tie-break selects {} over {}",
                winner.digest, loser.digest,
            ),
            vec![format!(
                "identical first-add times for {} and {}",
                winner.digest, loser.digest,
            )],
        );
    }
    if winner_strength == 1 {
        let winner_sequence =
            winner.evidence.admission_sequence.expect("checked");
        let loser_sequence =
            loser.evidence.admission_sequence.expect("checked");
        if winner_sequence != loser_sequence {
            return (
                ArtifactLinkCollisionBasisWire::AdmissionSequenceWins,
                format!(
                    "earlier admission sequence {} wins over {}",
                    winner.digest, loser.digest,
                ),
                Vec::new(),
            );
        }
        return (
            ArtifactLinkCollisionBasisWire::DigestTieBreakUnordered,
            format!(
                "identical admission sequences; digest tie-break selects {} over {}",
                winner.digest, loser.digest,
            ),
            vec![format!(
                "identical admission sequences for {} and {}",
                winner.digest, loser.digest,
            )],
        );
    }
    (
        ArtifactLinkCollisionBasisWire::DigestTieBreakUnordered,
        format!(
            "no provable ordering; digest tie-break selects {} over {}",
            winner.digest, loser.digest,
        ),
        vec![format!(
            "missing durability ordering for {} and {}",
            winner.digest, loser.digest,
        )],
    )
}

fn tier_name(tier: ArtifactLinkCollisionTierWire) -> &'static str {
    match tier {
        ArtifactLinkCollisionTierWire::Durable => "durable",
        ArtifactLinkCollisionTierWire::Pending => "pending",
        ArtifactLinkCollisionTierWire::Incoming => "incoming",
    }
}

fn rank_basis(basis: ArtifactLinkCollisionBasisWire) -> u8 {
    match basis {
        ArtifactLinkCollisionBasisWire::Single => 0,
        ArtifactLinkCollisionBasisWire::ExactRetryCollapsed => 1,
        ArtifactLinkCollisionBasisWire::DurableBeatsPending => 2,
        ArtifactLinkCollisionBasisWire::DurableBeatsIncoming => 3,
        ArtifactLinkCollisionBasisWire::PendingBeatsIncoming => 4,
        ArtifactLinkCollisionBasisWire::HistoricalAncestorWins => 5,
        ArtifactLinkCollisionBasisWire::HistoricalCommitTimeWins => 6,
        ArtifactLinkCollisionBasisWire::EstablishedEvidenceWins => 7,
        ArtifactLinkCollisionBasisWire::AdmissionSequenceWins => 8,
        ArtifactLinkCollisionBasisWire::DigestTieBreakUnordered => 9,
    }
}

#[cfg(test)]
mod tests;

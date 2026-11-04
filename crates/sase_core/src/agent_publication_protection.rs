//! Content-addressed protection policy for agent-sidecar publication.
//!
//! The host owns file staging and private persistence. This module owns the
//! scan disposition, the durable block schema, idempotent transitions, and
//! the rule that an exact blocked digest cannot be retried automatically.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::note_attachment::{
    AttachmentScanHitKindWire, AttachmentScanWire,
    ATTACHMENT_SCANNER_RULES_VERSION,
};
use crate::sidecar_publication::SidecarPublicationClassificationWire;

pub const AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AgentPublicationProtectionError {
    #[error("unsupported agent publication protection schema_version {0}")]
    UnsupportedSchema(u32),
    #[error("invalid agent publication protection record: {0}")]
    InvalidRecord(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationScanStatusWire {
    Clean,
    Blocked,
    Unverified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationScanReasonWire {
    Clean,
    ScannerHit,
    ScanSkipped,
    ScannerUnavailable,
    ScanError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationProtectionStatusWire {
    Blocked,
    Unverified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationProtectionReasonWire {
    ScannerHit,
    ScanSkipped,
    ScannerUnavailable,
    ScanError,
    RemoteSecretRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationScanFailureWire {
    ScannerUnavailable,
    ScanError,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationScanDispositionRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub scan: Option<AttachmentScanWire>,
    #[serde(default)]
    pub failure: Option<PublicationScanFailureWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationScanDispositionWire {
    pub schema_version: u32,
    pub status: PublicationScanStatusWire,
    pub reason: PublicationScanReasonWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hit: Option<PublicationScanEvidenceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rules_version: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationScanEvidenceWire {
    pub kind: AttachmentScanHitKindWire,
    pub rule_id: String,
    pub line: u64,
    pub rules_version: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionLedgerWire {
    pub schema_version: u32,
    #[serde(default)]
    pub records: Vec<PublicationProtectionRecordWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub superseded: Vec<PublicationProtectionSupersededWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionSupersededWire {
    pub record: PublicationProtectionRecordWire,
    pub superseded_at: f64,
    pub superseded_by_rules_version: u32,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionRecordWire {
    pub repository_identity: String,
    pub relative_destination: String,
    pub sha256: String,
    pub status: PublicationProtectionStatusWire,
    pub reason: PublicationProtectionReasonWire,
    #[serde(default)]
    pub affected_requests: Vec<String>,
    #[serde(default)]
    pub evidence: Vec<PublicationScanEvidenceWire>,
    #[serde(default)]
    pub rules_versions: Vec<u32>,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationProtectionIdentityWire {
    pub repository_identity: String,
    pub relative_destination: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionRecordFromScanRequestWire {
    pub schema_version: u32,
    pub identity: PublicationProtectionIdentityWire,
    #[serde(default)]
    pub affected_requests: Vec<String>,
    pub disposition: PublicationScanDispositionWire,
    pub timestamp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionRecordFromRejectionRequestWire {
    pub schema_version: u32,
    pub identity: PublicationProtectionIdentityWire,
    #[serde(default)]
    pub affected_requests: Vec<String>,
    pub classification: SidecarPublicationClassificationWire,
    pub timestamp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionUpsertRequestWire {
    pub schema_version: u32,
    pub ledger: PublicationProtectionLedgerWire,
    pub record: PublicationProtectionRecordWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionLookupRequestWire {
    pub schema_version: u32,
    pub ledger: PublicationProtectionLedgerWire,
    pub identity: PublicationProtectionIdentityWire,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicationProtectionResolveVerifiedCleanRequestWire {
    pub schema_version: u32,
    pub ledger: PublicationProtectionLedgerWire,
    pub identity: PublicationProtectionIdentityWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clean_rules_version: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<f64>,
}

/// Map the attachment scanner result into the fail-closed publication policy.
pub fn publication_scan_disposition(
    request: &PublicationScanDispositionRequestWire,
) -> Result<PublicationScanDispositionWire, AgentPublicationProtectionError> {
    require_schema(request.schema_version)?;
    if request.scan.is_some() == request.failure.is_some() {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "provide exactly one scan result or scanner failure",
        ));
    }
    let (status, reason, hit, rules_version) = if let Some(failure) =
        request.failure
    {
        let reason = match failure {
            PublicationScanFailureWire::ScannerUnavailable => {
                PublicationScanReasonWire::ScannerUnavailable
            }
            PublicationScanFailureWire::ScanError => {
                PublicationScanReasonWire::ScanError
            }
        };
        (PublicationScanStatusWire::Unverified, reason, None, None)
    } else {
        let scan = request.scan.as_ref().expect("validated scan result");
        match scan.outcome {
            crate::note_attachment::AttachmentScanOutcomeWire::Clean => {
                if scan.hit.is_some() {
                    return Err(
                        AgentPublicationProtectionError::InvalidRecord(
                            "clean scanner result cannot contain hit evidence",
                        ),
                    );
                }
                (
                    PublicationScanStatusWire::Clean,
                    PublicationScanReasonWire::Clean,
                    None,
                    Some(scan.rules_version),
                )
            }
            crate::note_attachment::AttachmentScanOutcomeWire::Hit => {
                let Some(hit) = scan.hit.as_ref() else {
                    return Err(
                        AgentPublicationProtectionError::InvalidRecord(
                            "hit scanner result must contain rule evidence",
                        ),
                    );
                };
                (
                    PublicationScanStatusWire::Blocked,
                    PublicationScanReasonWire::ScannerHit,
                    Some(PublicationScanEvidenceWire {
                        kind: hit.kind,
                        rule_id: hit.rule_id.clone(),
                        line: hit.line,
                        rules_version: scan.rules_version,
                    }),
                    Some(scan.rules_version),
                )
            }
            crate::note_attachment::AttachmentScanOutcomeWire::Skipped => {
                if scan.hit.is_some() {
                    return Err(AgentPublicationProtectionError::InvalidRecord(
                        "skipped scanner result cannot contain hit evidence",
                    ));
                }
                (
                    PublicationScanStatusWire::Unverified,
                    PublicationScanReasonWire::ScanSkipped,
                    None,
                    Some(scan.rules_version),
                )
            }
        }
    };
    Ok(PublicationScanDispositionWire {
        schema_version: AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION,
        status,
        reason,
        hit,
        rules_version,
    })
}

pub fn publication_protection_ledger_empty() -> PublicationProtectionLedgerWire
{
    PublicationProtectionLedgerWire {
        schema_version: AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION,
        records: Vec::new(),
        superseded: Vec::new(),
    }
}

/// Validate a ledger read from private disk and return its canonical ordering.
pub fn publication_protection_ledger_validate(
    ledger: &PublicationProtectionLedgerWire,
) -> Result<PublicationProtectionLedgerWire, AgentPublicationProtectionError> {
    require_schema(ledger.schema_version)?;
    let mut records = ledger.records.clone();
    let mut keys = BTreeSet::new();
    for record in &mut records {
        validate_record(record)?;
        if !keys.insert(record_key(record)) {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "ledger contains duplicate content identities",
            ));
        }
        record.affected_requests.sort();
        record.affected_requests.dedup();
        sort_evidence(&mut record.evidence);
        record.evidence.dedup();
        record.rules_versions.sort_unstable();
        record.rules_versions.dedup();
    }
    records.sort_by_key(record_key);
    let mut superseded = ledger.superseded.clone();
    for entry in &mut superseded {
        validate_record(&mut entry.record)?;
        validate_timestamp(entry.superseded_at)?;
        if entry.superseded_by_rules_version == 0 {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "superseded entry must name a rules version",
            ));
        }
        if entry.reason.trim().is_empty() {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "superseded entry must name a reason",
            ));
        }
    }
    superseded.sort_by(|left, right| {
        record_key(&left.record)
            .cmp(&record_key(&right.record))
            .then_with(|| left.superseded_at.total_cmp(&right.superseded_at))
            .then_with(|| {
                left.superseded_by_rules_version
                    .cmp(&right.superseded_by_rules_version)
            })
            .then_with(|| left.reason.cmp(&right.reason))
    });
    superseded.dedup();
    Ok(PublicationProtectionLedgerWire {
        schema_version: AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION,
        records,
        superseded,
    })
}

fn max_evidence_rules_version(
    record: &PublicationProtectionRecordWire,
) -> Option<u32> {
    record
        .evidence
        .iter()
        .map(|entry| entry.rules_version)
        .max()
}

fn is_stale_scanner_hit(record: &PublicationProtectionRecordWire) -> bool {
    if record.status != PublicationProtectionStatusWire::Blocked
        || record.reason != PublicationProtectionReasonWire::ScannerHit
    {
        return false;
    }
    if record.evidence.is_empty() {
        return false;
    }
    if record
        .evidence
        .iter()
        .any(|entry| entry.kind == AttachmentScanHitKindWire::KnownValue)
    {
        return false;
    }
    match max_evidence_rules_version(record) {
        Some(max) => max < ATTACHMENT_SCANNER_RULES_VERSION,
        None => false,
    }
}

/// A scanner-hit block is enforced only while its evidence is current.
/// Remote rejections, KnownValue verdicts, records without evidence, and
/// blocks whose evidence already meets the current rules stay enforced.
/// Staleness is computed from evidence, never from `rules_versions`.
pub fn publication_protection_record_enforced(
    record: &PublicationProtectionRecordWire,
) -> bool {
    !is_stale_scanner_hit(record)
}

/// Convert a scan result into a durable row. Clean scans intentionally have no row.
pub fn publication_protection_record_from_scan(
    request: &PublicationProtectionRecordFromScanRequestWire,
) -> Result<
    Option<PublicationProtectionRecordWire>,
    AgentPublicationProtectionError,
> {
    require_schema(request.schema_version)?;
    validate_identity(&request.identity)?;
    validate_timestamp(request.timestamp)?;
    if request.disposition.schema_version
        != AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION
    {
        return Err(AgentPublicationProtectionError::UnsupportedSchema(
            request.disposition.schema_version,
        ));
    }
    match request.disposition.status {
        PublicationScanStatusWire::Clean
            if request.disposition.reason
                != PublicationScanReasonWire::Clean
                || request.disposition.hit.is_some() =>
        {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "clean scan disposition must not contain hit evidence",
            ));
        }
        PublicationScanStatusWire::Blocked
            if request.disposition.reason
                != PublicationScanReasonWire::ScannerHit
                || request.disposition.hit.is_none() =>
        {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "blocked scan disposition must contain scanner evidence",
            ));
        }
        PublicationScanStatusWire::Unverified
            if request.disposition.hit.is_some()
                || request.disposition.reason
                    == PublicationScanReasonWire::Clean
                || request.disposition.reason
                    == PublicationScanReasonWire::ScannerHit =>
        {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "unverified scan disposition must not contain hit evidence",
            ));
        }
        _ => {}
    }
    let (status, reason) = match request.disposition.status {
        PublicationScanStatusWire::Clean => return Ok(None),
        PublicationScanStatusWire::Blocked => (
            PublicationProtectionStatusWire::Blocked,
            PublicationProtectionReasonWire::ScannerHit,
        ),
        PublicationScanStatusWire::Unverified => (
            PublicationProtectionStatusWire::Unverified,
            match request.disposition.reason {
                PublicationScanReasonWire::Clean => {
                    return Err(
                        AgentPublicationProtectionError::InvalidRecord(
                            "unverified scan result has an invalid reason",
                        ),
                    );
                }
                PublicationScanReasonWire::ScanSkipped => {
                    PublicationProtectionReasonWire::ScanSkipped
                }
                PublicationScanReasonWire::ScannerUnavailable => {
                    PublicationProtectionReasonWire::ScannerUnavailable
                }
                PublicationScanReasonWire::ScanError => {
                    PublicationProtectionReasonWire::ScanError
                }
                PublicationScanReasonWire::ScannerHit => {
                    return Err(
                        AgentPublicationProtectionError::InvalidRecord(
                            "unverified scan result has an invalid reason",
                        ),
                    );
                }
            },
        ),
    };
    let evidence = request.disposition.hit.iter().cloned().collect::<Vec<_>>();
    let rules_versions = request
        .disposition
        .rules_version
        .into_iter()
        .chain(evidence.iter().map(|row| row.rules_version))
        .collect();
    let mut record = PublicationProtectionRecordWire {
        repository_identity: request.identity.repository_identity.clone(),
        relative_destination: request.identity.relative_destination.clone(),
        sha256: request.identity.sha256.clone(),
        status,
        reason,
        affected_requests: request.affected_requests.clone(),
        evidence,
        rules_versions,
        created_at: request.timestamp,
        updated_at: request.timestamp,
    };
    validate_record(&mut record)?;
    Ok(Some(record))
}

/// A confirmed remote secret rejection blocks the exact candidate digest without
/// inventing a scanner rule. Generic repository-rule failures create no secret block.
pub fn publication_protection_record_from_rejection(
    request: &PublicationProtectionRecordFromRejectionRequestWire,
) -> Result<
    Option<PublicationProtectionRecordWire>,
    AgentPublicationProtectionError,
> {
    require_schema(request.schema_version)?;
    validate_identity(&request.identity)?;
    validate_timestamp(request.timestamp)?;
    if request.classification
        != SidecarPublicationClassificationWire::SecretScanRejected
    {
        return Ok(None);
    }
    let mut record = PublicationProtectionRecordWire {
        repository_identity: request.identity.repository_identity.clone(),
        relative_destination: request.identity.relative_destination.clone(),
        sha256: request.identity.sha256.clone(),
        status: PublicationProtectionStatusWire::Blocked,
        reason: PublicationProtectionReasonWire::RemoteSecretRejection,
        affected_requests: request.affected_requests.clone(),
        evidence: Vec::new(),
        rules_versions: Vec::new(),
        created_at: request.timestamp,
        updated_at: request.timestamp,
    };
    validate_record(&mut record)?;
    Ok(Some(record))
}

/// Idempotently merge one record, preserving blocks and their evidence forever.
pub fn publication_protection_upsert(
    request: &PublicationProtectionUpsertRequestWire,
) -> Result<PublicationProtectionLedgerWire, AgentPublicationProtectionError> {
    require_schema(request.schema_version)?;
    let mut ledger = publication_protection_ledger_validate(&request.ledger)?;
    let mut incoming = request.record.clone();
    validate_record(&mut incoming)?;
    if let Some(existing) = ledger.records.iter_mut().find(|row| {
        row.repository_identity == incoming.repository_identity
            && row.relative_destination == incoming.relative_destination
            && row.sha256 == incoming.sha256
    }) {
        existing
            .affected_requests
            .extend(incoming.affected_requests);
        existing.evidence.extend(incoming.evidence);
        existing.rules_versions.extend(incoming.rules_versions);
        if incoming.status == PublicationProtectionStatusWire::Blocked {
            existing.status = PublicationProtectionStatusWire::Blocked;
            existing.reason = incoming.reason;
        }
        existing.created_at = existing.created_at.min(incoming.created_at);
        existing.updated_at = existing.updated_at.max(incoming.updated_at);
        let mut candidate = existing.clone();
        validate_record(&mut candidate)?;
        *existing = candidate;
    } else {
        ledger.records.push(incoming);
    }
    publication_protection_ledger_validate(&ledger)
}

/// Look up the exact repository, destination, and digest tuple.
pub fn publication_protection_lookup(
    request: &PublicationProtectionLookupRequestWire,
) -> Result<
    Option<PublicationProtectionRecordWire>,
    AgentPublicationProtectionError,
> {
    require_schema(request.schema_version)?;
    validate_identity(&request.identity)?;
    let ledger = publication_protection_ledger_validate(&request.ledger)?;
    Ok(ledger.records.into_iter().find(|record| {
        record.repository_identity == request.identity.repository_identity
            && record.relative_destination
                == request.identity.relative_destination
            && record.sha256 == request.identity.sha256
    }))
}

/// Look up the enforced protection record. Stale scanner-hit blocks are
/// hidden so the caller rescans under the current rules. Raw diagnostics
/// should use `publication_protection_lookup`.
pub fn publication_protection_lookup_enforced(
    request: &PublicationProtectionLookupRequestWire,
) -> Result<
    Option<PublicationProtectionRecordWire>,
    AgentPublicationProtectionError,
> {
    Ok(publication_protection_lookup(request)?
        .filter(publication_protection_record_enforced))
}

/// Enforced blocks never become retryable; stale scanner-hit blocks do, so
/// the next publish attempt rescans under the current rules. A changed
/// digest is a new scan.
pub fn publication_protection_retry_allowed(
    request: &PublicationProtectionLookupRequestWire,
) -> Result<bool, AgentPublicationProtectionError> {
    Ok(match publication_protection_lookup(request)? {
        None => true,
        Some(record) => {
            record.status != PublicationProtectionStatusWire::Blocked
                || !publication_protection_record_enforced(&record)
        }
    })
}

/// Remove a transient unverified row after a later complete clean scan.
/// A stale scanner-hit block whose evidence predates `clean_rules_version`
/// moves to `superseded` with provenance instead of staying enforced.
/// Enforced blocks behave exactly as before. Without both new fields,
/// resolve behaves exactly as before (fail-closed for old callers).
pub fn publication_protection_resolve_verified_clean(
    request: &PublicationProtectionResolveVerifiedCleanRequestWire,
) -> Result<PublicationProtectionLedgerWire, AgentPublicationProtectionError> {
    require_schema(request.schema_version)?;
    validate_identity(&request.identity)?;
    if let Some(timestamp) = request.timestamp {
        validate_timestamp(timestamp)?;
    }
    let mut ledger = publication_protection_ledger_validate(&request.ledger)?;
    if let (Some(clean_version), Some(timestamp)) =
        (request.clean_rules_version, request.timestamp)
    {
        if let Some(index) = ledger.records.iter().position(|record| {
            record.repository_identity == request.identity.repository_identity
                && record.relative_destination
                    == request.identity.relative_destination
                && record.sha256 == request.identity.sha256
        }) {
            let stale = {
                let record = &ledger.records[index];
                record.status == PublicationProtectionStatusWire::Blocked
                    && record.reason
                        == PublicationProtectionReasonWire::ScannerHit
                    && !record.evidence.is_empty()
                    && !record.evidence.iter().any(|entry| {
                        entry.kind == AttachmentScanHitKindWire::KnownValue
                    })
                    && match max_evidence_rules_version(record) {
                        Some(max) => max < clean_version,
                        None => false,
                    }
            };
            if stale {
                let record = ledger.records.remove(index);
                ledger.superseded.push(PublicationProtectionSupersededWire {
                    record,
                    superseded_at: timestamp,
                    superseded_by_rules_version: clean_version,
                    reason: "rescanned_clean".to_string(),
                });
                return publication_protection_ledger_validate(&ledger);
            }
        }
    }
    ledger.records.retain(|record| {
        record.repository_identity != request.identity.repository_identity
            || record.relative_destination
                != request.identity.relative_destination
            || record.sha256 != request.identity.sha256
            || record.status == PublicationProtectionStatusWire::Blocked
    });
    Ok(ledger)
}

fn require_schema(
    schema_version: u32,
) -> Result<(), AgentPublicationProtectionError> {
    if schema_version != AGENT_PUBLICATION_PROTECTION_WIRE_SCHEMA_VERSION {
        return Err(AgentPublicationProtectionError::UnsupportedSchema(
            schema_version,
        ));
    }
    Ok(())
}

fn validate_identity(
    identity: &PublicationProtectionIdentityWire,
) -> Result<(), AgentPublicationProtectionError> {
    if identity.repository_identity.trim().is_empty() {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "repository identity must be non-empty",
        ));
    }
    if !valid_relative_path(&identity.relative_destination) {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "destination must be a safe relative path",
        ));
    }
    if identity.sha256.len() != 64
        || !identity
            .sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "sha256 must be 64 hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_record(
    record: &mut PublicationProtectionRecordWire,
) -> Result<(), AgentPublicationProtectionError> {
    validate_identity(&PublicationProtectionIdentityWire {
        repository_identity: record.repository_identity.clone(),
        relative_destination: record.relative_destination.clone(),
        sha256: record.sha256.clone(),
    })?;
    validate_timestamp(record.created_at)?;
    validate_timestamp(record.updated_at)?;
    if record.updated_at < record.created_at {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "updated_at precedes created_at",
        ));
    }
    if record
        .affected_requests
        .iter()
        .any(|request| request.trim().is_empty())
    {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "affected request identities must be non-empty",
        ));
    }
    for evidence in &record.evidence {
        if evidence.rule_id.is_empty()
            || !evidence.rule_id.bytes().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || b"_-".contains(&byte)
            })
        {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "scanner rule ids must use lowercase letters, digits, underscore, or hyphen",
            ));
        }
        if !record.rules_versions.contains(&evidence.rules_version) {
            return Err(AgentPublicationProtectionError::InvalidRecord(
                "evidence rules version must be present on its record",
            ));
        }
    }
    let evidence_required = record.reason
        == PublicationProtectionReasonWire::ScannerHit
        && record.status == PublicationProtectionStatusWire::Blocked;
    if evidence_required && record.evidence.is_empty() {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "scanner-hit blocks must retain rule evidence",
        ));
    }
    if record.status == PublicationProtectionStatusWire::Unverified
        && !matches!(
            record.reason,
            PublicationProtectionReasonWire::ScanSkipped
                | PublicationProtectionReasonWire::ScannerUnavailable
                | PublicationProtectionReasonWire::ScanError
        )
    {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "unverified records must describe a scan failure",
        ));
    }
    if record.status == PublicationProtectionStatusWire::Unverified
        && !record.evidence.is_empty()
    {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "unverified records cannot contain scanner-hit evidence",
        ));
    }
    if record.status == PublicationProtectionStatusWire::Blocked
        && !matches!(
            record.reason,
            PublicationProtectionReasonWire::ScannerHit
                | PublicationProtectionReasonWire::RemoteSecretRejection
        )
    {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "blocked records must describe a confirmed secret decision",
        ));
    }
    record.affected_requests.sort();
    record.affected_requests.dedup();
    sort_evidence(&mut record.evidence);
    record.evidence.dedup();
    record.rules_versions.sort_unstable();
    record.rules_versions.dedup();
    Ok(())
}

fn validate_timestamp(
    timestamp: f64,
) -> Result<(), AgentPublicationProtectionError> {
    if !timestamp.is_finite() || timestamp < 0.0 {
        return Err(AgentPublicationProtectionError::InvalidRecord(
            "timestamps must be finite and non-negative",
        ));
    }
    Ok(())
}

fn valid_relative_path(path: &str) -> bool {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path.contains('\0')
        || path.contains(':')
    {
        return false;
    }
    path.split('/')
        .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn record_key(
    record: &PublicationProtectionRecordWire,
) -> (String, String, String) {
    (
        record.repository_identity.clone(),
        record.relative_destination.clone(),
        record.sha256.clone(),
    )
}

fn sort_evidence(evidence: &mut Vec<PublicationScanEvidenceWire>) {
    evidence.sort_by(|left, right| {
        (
            hit_kind_rank(left.kind),
            left.rule_id.as_str(),
            left.line,
            left.rules_version,
        )
            .cmp(&(
                hit_kind_rank(right.kind),
                right.rule_id.as_str(),
                right.line,
                right.rules_version,
            ))
    });
    evidence.dedup();
}

fn hit_kind_rank(kind: AttachmentScanHitKindWire) -> u8 {
    match kind {
        AttachmentScanHitKindWire::KnownValue => 0,
        AttachmentScanHitKindWire::CredentialPattern => 1,
        AttachmentScanHitKindWire::EnvDump => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::note_attachment::{
        attachment_scan_file, AttachmentScanHitWire, AttachmentScanOutcomeWire,
    };
    use crate::sidecar_publication::classify_sidecar_publication_push_result;
    use std::collections::HashMap;

    fn identity(
        destination: &str,
        digest: char,
    ) -> PublicationProtectionIdentityWire {
        PublicationProtectionIdentityWire {
            repository_identity: "sase-org/sase--agents".to_string(),
            relative_destination: destination.to_string(),
            sha256: digest.to_string().repeat(64),
        }
    }

    fn scan(outcome: AttachmentScanOutcomeWire) -> AttachmentScanWire {
        AttachmentScanWire {
            outcome,
            hit: (outcome == AttachmentScanOutcomeWire::Hit).then(|| {
                AttachmentScanHitWire {
                    kind: AttachmentScanHitKindWire::KnownValue,
                    rule_id: "known-value".to_string(),
                    line: 2,
                }
            }),
            bytes_scanned: 20,
            rules_version: 1,
        }
    }

    fn disposition(
        scan: Option<AttachmentScanWire>,
        failure: Option<PublicationScanFailureWire>,
    ) -> PublicationScanDispositionWire {
        publication_scan_disposition(&PublicationScanDispositionRequestWire {
            schema_version: 1,
            scan,
            failure,
        })
        .unwrap()
    }

    #[test]
    fn maps_clean_hit_skipped_and_scanner_failures_without_values() {
        let clean =
            disposition(Some(scan(AttachmentScanOutcomeWire::Clean)), None);
        assert_eq!(clean.status, PublicationScanStatusWire::Clean);
        assert!(clean.hit.is_none());

        let hit = disposition(Some(scan(AttachmentScanOutcomeWire::Hit)), None);
        assert_eq!(hit.status, PublicationScanStatusWire::Blocked);
        assert_eq!(hit.hit.as_ref().unwrap().rule_id, "known-value");

        let skipped =
            disposition(Some(scan(AttachmentScanOutcomeWire::Skipped)), None);
        assert_eq!(skipped.status, PublicationScanStatusWire::Unverified);
        assert_eq!(skipped.reason, PublicationScanReasonWire::ScanSkipped);

        let unavailable = disposition(
            None,
            Some(PublicationScanFailureWire::ScannerUnavailable),
        );
        assert_eq!(unavailable.status, PublicationScanStatusWire::Unverified);
        assert_eq!(
            unavailable.reason,
            PublicationScanReasonWire::ScannerUnavailable
        );

        let error =
            disposition(None, Some(PublicationScanFailureWire::ScanError));
        assert_eq!(error.reason, PublicationScanReasonWire::ScanError);
        let json = serde_json::to_string(&hit).unwrap();
        assert!(!json.contains("matched-value"));
    }

    #[test]
    fn all_scanner_detector_families_keep_only_rule_metadata() {
        for (kind, rule_id) in [
            (AttachmentScanHitKindWire::KnownValue, "known-value"),
            (
                AttachmentScanHitKindWire::CredentialPattern,
                "private-key-header",
            ),
            (AttachmentScanHitKindWire::EnvDump, "environment-dump"),
        ] {
            let request = PublicationScanDispositionRequestWire {
                schema_version: 1,
                scan: Some(AttachmentScanWire {
                    outcome: AttachmentScanOutcomeWire::Hit,
                    hit: Some(AttachmentScanHitWire {
                        kind,
                        rule_id: rule_id.to_string(),
                        line: 3,
                    }),
                    bytes_scanned: 50,
                    rules_version: 1,
                }),
                failure: None,
            };
            let result = publication_scan_disposition(&request).unwrap();
            assert_eq!(result.hit.as_ref().unwrap().kind, kind);
            assert_eq!(result.hit.as_ref().unwrap().rule_id, rule_id);
        }
    }

    #[test]
    fn actual_scanner_hits_each_detector_family_without_returning_values() {
        let known = format!("{}{}", "publication-test-", "0123456789abcdef");
        let credential = format!("{}{}", ["gh", "p_"].concat(), "A".repeat(30));
        let env_dump = (0..10)
            .map(|index| format!("VAR{index}=value{index}"))
            .collect::<Vec<_>>()
            .join("\n");
        let cases = [
            (
                format!("known value {known}\n"),
                HashMap::from([(
                    "PUBLICATION_TEST_TOKEN".to_string(),
                    known.clone(),
                )]),
                AttachmentScanHitKindWire::KnownValue,
                known,
            ),
            (
                format!("credential {credential}\n"),
                HashMap::new(),
                AttachmentScanHitKindWire::CredentialPattern,
                credential,
            ),
            (
                format!("{env_dump}\n"),
                HashMap::new(),
                AttachmentScanHitKindWire::EnvDump,
                env_dump,
            ),
        ];
        for (contents, env, expected_kind, synthetic_value) in cases {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("candidate.txt");
            std::fs::write(&path, contents.as_bytes()).unwrap();
            let scan = attachment_scan_file(
                path.to_str().unwrap(),
                1024 * 1024,
                &env,
                "/home/test",
                "/home/test/.sase",
            );
            assert_eq!(scan.outcome, AttachmentScanOutcomeWire::Hit);
            let disposition = publication_scan_disposition(
                &PublicationScanDispositionRequestWire {
                    schema_version: 1,
                    scan: Some(scan),
                    failure: None,
                },
            )
            .unwrap();
            assert_eq!(disposition.status, PublicationScanStatusWire::Blocked);
            assert_eq!(disposition.hit.as_ref().unwrap().kind, expected_kind);
            assert!(!serde_json::to_string(&disposition)
                .unwrap()
                .contains(&synthetic_value));
        }
    }

    #[test]
    fn ledger_deduplicates_requests_preserves_blocks_and_tracks_digest_changes()
    {
        let hit = disposition(Some(scan(AttachmentScanOutcomeWire::Hit)), None);
        let request = PublicationProtectionRecordFromScanRequestWire {
            schema_version: 1,
            identity: identity("agents/alice/run/chat.md", 'a'),
            affected_requests: vec!["request-1".to_string()],
            disposition: hit,
            timestamp: 10.0,
        };
        let first = publication_protection_record_from_scan(&request)
            .unwrap()
            .unwrap();
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger: publication_protection_ledger_empty(),
                record: first.clone(),
            },
        )
        .unwrap();
        let again = PublicationProtectionRecordWire {
            affected_requests: vec![
                "request-1".to_string(),
                "request-2".to_string(),
            ],
            created_at: 9.0,
            updated_at: 11.0,
            ..first
        };
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger,
                record: again,
            },
        )
        .unwrap();
        assert_eq!(ledger.records.len(), 1);
        assert_eq!(
            ledger.records[0].affected_requests,
            ["request-1", "request-2"]
        );
        assert_eq!(ledger.records[0].created_at, 9.0);
        assert_eq!(ledger.records[0].updated_at, 11.0);
        let blocked = PublicationProtectionLookupRequestWire {
            schema_version: 1,
            ledger: ledger.clone(),
            identity: identity("agents/alice/run/chat.md", 'a'),
        };
        assert!(!publication_protection_retry_allowed(&blocked).unwrap());
        let changed = PublicationProtectionLookupRequestWire {
            identity: identity("agents/alice/run/chat.md", 'b'),
            ..blocked
        };
        assert!(publication_protection_retry_allowed(&changed).unwrap());
        assert_eq!(
            publication_protection_ledger_validate(&ledger).unwrap(),
            ledger
        );
    }

    #[test]
    fn unverified_is_durable_but_retryable_and_later_hit_promotes_it() {
        let id = identity("agents/alice/run/chat.md", 'a');
        let record = publication_protection_record_from_scan(
            &PublicationProtectionRecordFromScanRequestWire {
                schema_version: 1,
                identity: id.clone(),
                affected_requests: vec![],
                disposition: disposition(
                    Some(scan(AttachmentScanOutcomeWire::Skipped)),
                    None,
                ),
                timestamp: 5.0,
            },
        )
        .unwrap()
        .unwrap();
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger: publication_protection_ledger_empty(),
                record,
            },
        )
        .unwrap();
        assert!(publication_protection_retry_allowed(
            &PublicationProtectionLookupRequestWire {
                schema_version: 1,
                ledger: ledger.clone(),
                identity: id.clone(),
            }
        )
        .unwrap());
        let blocked = publication_protection_record_from_scan(
            &PublicationProtectionRecordFromScanRequestWire {
                schema_version: 1,
                identity: id.clone(),
                affected_requests: vec![],
                disposition: disposition(
                    Some(scan(AttachmentScanOutcomeWire::Hit)),
                    None,
                ),
                timestamp: 6.0,
            },
        )
        .unwrap()
        .unwrap();
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger,
                record: blocked,
            },
        )
        .unwrap();
        assert!(!publication_protection_retry_allowed(
            &PublicationProtectionLookupRequestWire {
                schema_version: 1,
                ledger,
                identity: id,
            }
        )
        .unwrap());
    }

    #[test]
    fn later_clean_scan_resolves_only_unverified_records() {
        let unverified_identity = identity("agents/a/run/chat.md", 'a');
        let unverified = publication_protection_record_from_scan(
            &PublicationProtectionRecordFromScanRequestWire {
                schema_version: 1,
                identity: unverified_identity.clone(),
                affected_requests: vec![],
                disposition: disposition(
                    Some(scan(AttachmentScanOutcomeWire::Skipped)),
                    None,
                ),
                timestamp: 5.0,
            },
        )
        .unwrap()
        .unwrap();
        let blocked_identity = identity("agents/a/run/prompt.md", 'b');
        let blocked = publication_protection_record_from_scan(
            &PublicationProtectionRecordFromScanRequestWire {
                schema_version: 1,
                identity: blocked_identity.clone(),
                affected_requests: vec![],
                disposition: disposition(
                    Some(scan(AttachmentScanOutcomeWire::Hit)),
                    None,
                ),
                timestamp: 5.0,
            },
        )
        .unwrap()
        .unwrap();
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger: publication_protection_ledger_empty(),
                record: unverified,
            },
        )
        .unwrap();
        let ledger = publication_protection_upsert(
            &PublicationProtectionUpsertRequestWire {
                schema_version: 1,
                ledger,
                record: blocked,
            },
        )
        .unwrap();
        let ledger = publication_protection_resolve_verified_clean(
            &PublicationProtectionResolveVerifiedCleanRequestWire {
                schema_version: 1,
                ledger,
                identity: unverified_identity,
                clean_rules_version: None,
                timestamp: None,
            },
        )
        .unwrap();
        assert_eq!(ledger.records.len(), 1);
        assert_eq!(
            ledger.records[0].status,
            PublicationProtectionStatusWire::Blocked
        );
        assert_eq!(
            ledger.records[0].relative_destination,
            "agents/a/run/prompt.md"
        );
    }

    #[test]
    fn secret_rejection_blocks_without_fabricating_rule_evidence() {
        let rejection = classify_sidecar_publication_push_result(
            1,
            "",
            "remote: error: GH013: Repository rule violations found; fetch first",
        );
        assert_eq!(
            rejection.classification,
            SidecarPublicationClassificationWire::SecretScanRejected
        );
        let record = publication_protection_record_from_rejection(
            &PublicationProtectionRecordFromRejectionRequestWire {
                schema_version: 1,
                identity: identity("agents/alice/run/chat.md", 'a'),
                affected_requests: vec!["request-1".to_string()],
                classification: rejection.classification,
                timestamp: 3.0,
            },
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            record.reason,
            PublicationProtectionReasonWire::RemoteSecretRejection
        );
        assert!(record.evidence.is_empty());
    }

    #[test]
    fn old_schema_paths_and_bad_digests_are_rejected() {
        let mut ledger = publication_protection_ledger_empty();
        ledger.schema_version = 0;
        assert!(publication_protection_ledger_validate(&ledger).is_err());
        for (path, digest) in
            [("../chat.md", 'a'), ("/chat.md", 'a'), ("chat.md", 'x')]
        {
            let mut record = PublicationProtectionRecordWire {
                repository_identity: "repo".to_string(),
                relative_destination: path.to_string(),
                sha256: digest.to_string().repeat(64),
                status: PublicationProtectionStatusWire::Unverified,
                reason: PublicationProtectionReasonWire::ScanSkipped,
                affected_requests: vec![],
                evidence: vec![],
                rules_versions: vec![1],
                created_at: 1.0,
                updated_at: 1.0,
            };
            assert!(validate_record(&mut record).is_err());
        }
    }

    fn credential_block(
        destination: &str,
        digest: char,
        rules_version: u32,
        kind: AttachmentScanHitKindWire,
        rule_id: &str,
    ) -> PublicationProtectionRecordWire {
        let scan = AttachmentScanWire {
            outcome: AttachmentScanOutcomeWire::Hit,
            hit: Some(AttachmentScanHitWire {
                kind,
                rule_id: rule_id.to_string(),
                line: 1,
            }),
            bytes_scanned: 64,
            rules_version,
        };
        let result = publication_scan_disposition(
            &PublicationScanDispositionRequestWire {
                schema_version: 1,
                scan: Some(scan),
                failure: None,
            },
        )
        .unwrap();
        publication_protection_record_from_scan(
            &PublicationProtectionRecordFromScanRequestWire {
                schema_version: 1,
                identity: identity(destination, digest),
                affected_requests: vec!["request-1".to_string()],
                disposition: result,
                timestamp: 5.0,
            },
        )
        .unwrap()
        .unwrap()
    }

    fn ledger_with(
        record: PublicationProtectionRecordWire,
    ) -> PublicationProtectionLedgerWire {
        publication_protection_upsert(&PublicationProtectionUpsertRequestWire {
            schema_version: 1,
            ledger: publication_protection_ledger_empty(),
            record,
        })
        .unwrap()
    }

    fn lookup_request(
        ledger: PublicationProtectionLedgerWire,
        destination: &str,
        digest: char,
    ) -> PublicationProtectionLookupRequestWire {
        PublicationProtectionLookupRequestWire {
            schema_version: 1,
            ledger,
            identity: identity(destination, digest),
        }
    }

    #[test]
    fn stale_scanner_hit_is_not_enforced_and_retries() {
        let record = credential_block(
            "agents/a/run/chat.md",
            'a',
            1,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        assert!(!publication_protection_record_enforced(&record));
        let ledger = ledger_with(record);
        let request =
            lookup_request(ledger.clone(), "agents/a/run/chat.md", 'a');
        assert!(publication_protection_lookup(&request).unwrap().is_some());
        assert!(publication_protection_lookup_enforced(&request)
            .unwrap()
            .is_none());
        assert!(publication_protection_retry_allowed(&request).unwrap());
    }

    #[test]
    fn current_scanner_hit_stays_enforced() {
        let record = credential_block(
            "agents/a/run/chat.md",
            'a',
            ATTACHMENT_SCANNER_RULES_VERSION,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        assert!(publication_protection_record_enforced(&record));
        let ledger = ledger_with(record);
        let request = lookup_request(ledger, "agents/a/run/chat.md", 'a');
        assert!(publication_protection_lookup_enforced(&request)
            .unwrap()
            .is_some());
        assert!(!publication_protection_retry_allowed(&request).unwrap());
    }

    #[test]
    fn known_value_and_remote_blocks_stay_enforced() {
        let known = credential_block(
            "agents/a/run/known.md",
            'b',
            1,
            AttachmentScanHitKindWire::KnownValue,
            "known-value",
        );
        assert!(publication_protection_record_enforced(&known));
        let ledger = ledger_with(known);
        let request = lookup_request(ledger, "agents/a/run/known.md", 'b');
        assert!(publication_protection_lookup_enforced(&request)
            .unwrap()
            .is_some());
        assert!(!publication_protection_retry_allowed(&request).unwrap());

        let rejection = publication_protection_record_from_rejection(
            &PublicationProtectionRecordFromRejectionRequestWire {
                schema_version: 1,
                identity: identity("agents/a/run/remote.md", 'c'),
                affected_requests: vec![],
                classification: classify_sidecar_publication_push_result(
                    1,
                    "",
                    "remote: error: GH013: Repository rule violations found",
                )
                .classification,
                timestamp: 3.0,
            },
        )
        .unwrap()
        .unwrap();
        assert!(publication_protection_record_enforced(&rejection));
    }

    #[test]
    fn staleness_ignores_rules_versions() {
        let mut record = credential_block(
            "agents/a/run/chat.md",
            'a',
            1,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        record.rules_versions.push(ATTACHMENT_SCANNER_RULES_VERSION);
        assert!(!publication_protection_record_enforced(&record));
    }

    #[test]
    fn resolve_supersedes_stale_with_provenance() {
        let record = credential_block(
            "agents/a/run/chat.md",
            'a',
            1,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        let ledger = ledger_with(record.clone());
        let resolved = publication_protection_resolve_verified_clean(
            &PublicationProtectionResolveVerifiedCleanRequestWire {
                schema_version: 1,
                ledger,
                identity: identity("agents/a/run/chat.md", 'a'),
                clean_rules_version: Some(ATTACHMENT_SCANNER_RULES_VERSION),
                timestamp: Some(9.0),
            },
        )
        .unwrap();
        assert!(resolved.records.is_empty());
        assert_eq!(resolved.superseded.len(), 1);
        let entry = &resolved.superseded[0];
        assert_eq!(entry.record, record);
        assert_eq!(entry.superseded_at, 9.0);
        assert_eq!(
            entry.superseded_by_rules_version,
            ATTACHMENT_SCANNER_RULES_VERSION
        );
        assert_eq!(entry.reason, "rescanned_clean");
        assert_eq!(
            publication_protection_ledger_validate(&resolved).unwrap(),
            resolved
        );
    }

    #[test]
    fn resolve_without_fields_keeps_stale_blocked() {
        let record = credential_block(
            "agents/a/run/chat.md",
            'a',
            1,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        let ledger = ledger_with(record);
        let resolved = publication_protection_resolve_verified_clean(
            &PublicationProtectionResolveVerifiedCleanRequestWire {
                schema_version: 1,
                ledger,
                identity: identity("agents/a/run/chat.md", 'a'),
                clean_rules_version: None,
                timestamp: None,
            },
        )
        .unwrap();
        assert_eq!(resolved.records.len(), 1);
        assert!(resolved.superseded.is_empty());
    }

    #[test]
    fn resolve_keeps_enforced_block() {
        let record = credential_block(
            "agents/a/run/chat.md",
            'a',
            ATTACHMENT_SCANNER_RULES_VERSION,
            AttachmentScanHitKindWire::CredentialPattern,
            "openai-key",
        );
        let ledger = ledger_with(record);
        let resolved = publication_protection_resolve_verified_clean(
            &PublicationProtectionResolveVerifiedCleanRequestWire {
                schema_version: 1,
                ledger,
                identity: identity("agents/a/run/chat.md", 'a'),
                clean_rules_version: Some(ATTACHMENT_SCANNER_RULES_VERSION),
                timestamp: Some(9.0),
            },
        )
        .unwrap();
        assert_eq!(resolved.records.len(), 1);
        assert!(resolved.superseded.is_empty());
    }
}

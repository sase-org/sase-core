//! Shared ownership and receipt policy for artifact-link event publication.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::artifact_ref::canonical_artifact_ref_kind;

use super::events::{
    canonicalize_artifact_link_event, ArtifactLinkEventEdgeWire,
    ArtifactLinkEventKindWire, ArtifactLinkEventWire,
};
use super::row_resolution::parse_artifact_link_ref_parts;
use super::wire::{canonicalize_artifact_link_ref, ArtifactLinkError};

pub const ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION: u64 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkOwnerRefWire {
    pub reference: String,
    pub kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkOwnerRequirementWire {
    pub schema_version: u64,
    pub operation_id: String,
    pub document_refs: Vec<ArtifactLinkOwnerRefWire>,
    pub bead_refs: Vec<String>,
    pub unowned_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationEvidenceWire {
    pub schema_version: u64,
    pub operation_id: String,
    pub resolved_roots: BTreeMap<String, String>,
    pub forced_roots: Vec<String>,
    pub durable_roots: Vec<String>,
    pub bead_owner: bool,
    pub bead_receipt: bool,
    pub local_receipt: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactLinkPublicationReceiptWire {
    pub schema_version: u64,
    pub operation_id: String,
    pub acknowledged: bool,
    pub pending_reasons: Vec<String>,
}

pub fn artifact_link_event_owner_requirements(
    event: &ArtifactLinkEventWire,
    document_kinds: &[String],
) -> Result<ArtifactLinkOwnerRequirementWire, ArtifactLinkError> {
    let event = canonicalize_artifact_link_event(event)?;
    let document_kinds = canonical_document_kinds(document_kinds)?;
    let mut document_refs = BTreeMap::<String, ArtifactLinkOwnerRefWire>::new();
    let mut bead_refs = BTreeSet::<String>::new();
    let mut unowned_refs = BTreeSet::<String>::new();

    for reference in event_refs(&event) {
        let reference = canonicalize_artifact_link_ref(&reference)?;
        let parts =
            parse_artifact_link_ref_parts(&reference).ok_or_else(|| {
                ArtifactLinkError::validation(format!(
                    "artifact link ref `{reference}` is not parseable"
                ))
            })?;
        if parts.kind == "bead" {
            bead_refs.insert(reference);
        } else if document_kinds.contains(&parts.kind) {
            document_refs.insert(
                reference.clone(),
                ArtifactLinkOwnerRefWire {
                    reference,
                    kind: parts.kind,
                },
            );
        } else {
            unowned_refs.insert(reference);
        }
    }

    Ok(ArtifactLinkOwnerRequirementWire {
        schema_version: ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION,
        operation_id: event.operation_id,
        document_refs: document_refs.into_values().collect(),
        bead_refs: bead_refs.into_iter().collect(),
        unowned_refs: unowned_refs.into_iter().collect(),
    })
}

pub fn artifact_link_publication_receipt(
    requirements: &ArtifactLinkOwnerRequirementWire,
    evidence: &ArtifactLinkPublicationEvidenceWire,
) -> Result<ArtifactLinkPublicationReceiptWire, ArtifactLinkError> {
    validate_schema(
        requirements.schema_version,
        "requirements.schema_version",
    )?;
    validate_schema(evidence.schema_version, "evidence.schema_version")?;
    if requirements.operation_id != evidence.operation_id {
        return Err(ArtifactLinkError::validation(format!(
            "artifact link publication receipt operation_id mismatch: requirements `{}` != evidence `{}`",
            requirements.operation_id, evidence.operation_id
        )));
    }

    let mut pending = Vec::new();
    for owner in &requirements.document_refs {
        if !evidence.resolved_roots.contains_key(&owner.kind) {
            pending.push(format!(
                "artifact-link event owner {} of kind {} has no resolved sidecar root",
                owner.reference, owner.kind
            ));
        }
    }

    let required_roots: BTreeSet<String> = evidence
        .forced_roots
        .iter()
        .chain(evidence.resolved_roots.values())
        .map(|root| root.to_string())
        .collect();
    let durable_roots: BTreeSet<String> = evidence
        .durable_roots
        .iter()
        .map(|root| root.to_string())
        .collect();
    for root in &required_roots {
        if !durable_roots.contains(root) {
            pending.push(format!(
                "artifact-link event required root {root} is not durable"
            ));
        }
    }

    if evidence.bead_owner && !evidence.bead_receipt {
        pending
            .push("artifact-link bead projection is not committed".to_string());
    }
    if required_roots.is_empty() && !evidence.local_receipt {
        pending.push(
            "artifact-link event has no durable owner receipt".to_string(),
        );
    }

    Ok(ArtifactLinkPublicationReceiptWire {
        schema_version: ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION,
        operation_id: requirements.operation_id.clone(),
        acknowledged: pending.is_empty(),
        pending_reasons: pending,
    })
}

fn canonical_document_kinds(
    document_kinds: &[String],
) -> Result<BTreeSet<String>, ArtifactLinkError> {
    let mut kinds = BTreeSet::new();
    for raw in document_kinds {
        let kind = raw.trim();
        if kind.is_empty() {
            return Err(ArtifactLinkError::validation(
                "document kind must not be empty",
            ));
        }
        kinds.insert(canonical_artifact_ref_kind(kind).canonical);
    }
    Ok(kinds)
}

fn event_refs(event: &ArtifactLinkEventWire) -> Vec<String> {
    match &event.kind {
        ArtifactLinkEventKindWire::Observation { edge, .. }
        | ArtifactLinkEventKindWire::EdgePut { edge, .. }
        | ArtifactLinkEventKindWire::EdgeRemove { edge, .. } => edge_refs(edge),
        ArtifactLinkEventKindWire::Alias { old_ref, new_ref } => {
            vec![old_ref.clone(), new_ref.clone()]
        }
        ArtifactLinkEventKindWire::BaselineImport { rows, .. } => rows
            .iter()
            .flat_map(|row| [row.source_ref.clone(), row.target_ref.clone()])
            .collect(),
    }
}

fn edge_refs(edge: &ArtifactLinkEventEdgeWire) -> Vec<String> {
    match edge {
        ArtifactLinkEventEdgeWire::Directed {
            source_ref,
            target_ref,
            ..
        } => vec![source_ref.clone(), target_ref.clone()],
        ArtifactLinkEventEdgeWire::Undirected {
            left_ref,
            right_ref,
            ..
        } => vec![left_ref.clone(), right_ref.clone()],
    }
}

fn validate_schema(value: u64, field: &str) -> Result<(), ArtifactLinkError> {
    if value != ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION {
        return Err(ArtifactLinkError::validation(format!(
            "{field} must be {}, got {value}",
            ARTIFACT_LINK_PUBLICATION_OWNERSHIP_WIRE_SCHEMA_VERSION
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact_link::{
        ArtifactLinkEventKindWire, ArtifactLinkOriginWire, ArtifactLinkRowWire,
        ARTIFACT_LINK_ROW_SCHEMA_VERSION,
    };

    fn op(ch: char) -> String {
        ch.to_string().repeat(32)
    }

    fn event(
        source: &str,
        relation: &str,
        target: &str,
    ) -> ArtifactLinkEventWire {
        ArtifactLinkEventWire {
            schema_version: 1,
            project_key: "gh_acme__widget".to_string(),
            operation_id: op('a'),
            created_by: "agent:reader".to_string(),
            origin: ArtifactLinkOriginWire::Read,
            created_at: "2026-09-09T12:00:00Z".to_string(),
            kind: ArtifactLinkEventKindWire::Observation {
                edge: ArtifactLinkEventEdgeWire::Directed {
                    source_ref: source.to_string(),
                    relation: relation.to_string(),
                    target_ref: target.to_string(),
                },
                description: "read the artifact".to_string(),
                occurrences: 1,
            },
        }
    }

    fn evidence_for(
        requirements: &ArtifactLinkOwnerRequirementWire,
    ) -> ArtifactLinkPublicationEvidenceWire {
        ArtifactLinkPublicationEvidenceWire {
            schema_version: 1,
            operation_id: requirements.operation_id.clone(),
            resolved_roots: BTreeMap::new(),
            forced_roots: Vec::new(),
            durable_roots: Vec::new(),
            bead_owner: false,
            bead_receipt: false,
            local_receipt: false,
        }
    }

    #[test]
    fn owner_requirements_partition_and_dedupe_event_refs() {
        let requirements = artifact_link_event_owner_requirements(
            &event("agent:reader", "read", "@plans:202609/a.md"),
            &["plan".to_string()],
        )
        .unwrap();

        assert_eq!(requirements.operation_id, op('a'));
        assert_eq!(
            requirements.document_refs,
            [ArtifactLinkOwnerRefWire {
                reference: "plan:202609/a.md".to_string(),
                kind: "plan".to_string(),
            }]
        );
        assert_eq!(requirements.unowned_refs, vec!["agent:reader".to_string()]);
        assert!(requirements.bead_refs.is_empty());

        let bead_requirements = artifact_link_event_owner_requirements(
            &event("bead:sase-yy.8", "implements", "plan:202609/a.md"),
            &["plans".to_string()],
        )
        .unwrap();
        assert_eq!(
            bead_requirements.bead_refs,
            vec!["bead:sase-yy.8".to_string()]
        );
        assert_eq!(
            bead_requirements.document_refs[0].reference,
            "plan:202609/a.md"
        );
    }

    #[test]
    fn baseline_import_rows_contribute_document_owners() {
        let mut event = event("agent:reader", "read", "plan:202609/a.md");
        event.origin = ArtifactLinkOriginWire::Migrated;
        event.kind = ArtifactLinkEventKindWire::BaselineImport {
            import_id: "import-1".to_string(),
            source_head: "abc123".to_string(),
            rows: vec![ArtifactLinkRowWire {
                schema_version: ARTIFACT_LINK_ROW_SCHEMA_VERSION,
                source_ref: "research:202609/report.md".to_string(),
                relation: "related".to_string(),
                target_ref: "plan:202609/a.md".to_string(),
                description: "legacy edge".to_string(),
                origin: ArtifactLinkOriginWire::Migrated,
                created_by: "importer".to_string(),
                created_at: "2026-09-09T12:00:00Z".to_string(),
                uses: 2,
            }],
        };

        let requirements = artifact_link_event_owner_requirements(
            &event,
            &["plan".to_string(), "research".to_string()],
        )
        .unwrap();

        assert_eq!(
            requirements
                .document_refs
                .iter()
                .map(|owner| owner.reference.as_str())
                .collect::<Vec<_>>(),
            ["plan:202609/a.md", "research:202609/report.md"]
        );
    }

    #[test]
    fn receipt_requires_resolved_roots_and_durable_receipts() {
        let requirements = artifact_link_event_owner_requirements(
            &event("agent:reader", "read", "plan:202609/a.md"),
            &["plan".to_string()],
        )
        .unwrap();
        let mut evidence = evidence_for(&requirements);
        let receipt =
            artifact_link_publication_receipt(&requirements, &evidence)
                .unwrap();
        assert!(!receipt.acknowledged);
        assert!(receipt.pending_reasons[0].contains("plan:202609/a.md"));
        assert!(receipt.pending_reasons[0].contains("kind plan"));

        evidence
            .resolved_roots
            .insert("plan".to_string(), "/tmp/plans".to_string());
        let receipt =
            artifact_link_publication_receipt(&requirements, &evidence)
                .unwrap();
        assert!(!receipt.acknowledged);
        assert!(receipt.pending_reasons[0].contains("/tmp/plans"));

        evidence.durable_roots.push("/tmp/plans".to_string());
        let receipt =
            artifact_link_publication_receipt(&requirements, &evidence)
                .unwrap();
        assert!(receipt.acknowledged);
        assert!(receipt.pending_reasons.is_empty());
    }

    #[test]
    fn receipt_accepts_local_or_bead_durable_owners() {
        let requirements = artifact_link_event_owner_requirements(
            &event("agent:reader", "read", "agent:other"),
            &["plan".to_string()],
        )
        .unwrap();
        let mut evidence = evidence_for(&requirements);
        let receipt =
            artifact_link_publication_receipt(&requirements, &evidence)
                .unwrap();
        assert!(!receipt.acknowledged);
        assert!(receipt.pending_reasons[0].contains("no durable owner receipt"));

        evidence.local_receipt = true;
        assert!(
            artifact_link_publication_receipt(&requirements, &evidence)
                .unwrap()
                .acknowledged
        );

        let bead_requirements = artifact_link_event_owner_requirements(
            &event("agent:reader", "read", "bead:sase-yy.8"),
            &[],
        )
        .unwrap();
        let mut bead_evidence = evidence_for(&bead_requirements);
        bead_evidence.bead_owner = true;
        let receipt = artifact_link_publication_receipt(
            &bead_requirements,
            &bead_evidence,
        )
        .unwrap();
        assert!(!receipt.acknowledged);
        assert!(receipt.pending_reasons[0].contains("bead projection"));
        bead_evidence.bead_receipt = true;
        let receipt = artifact_link_publication_receipt(
            &bead_requirements,
            &bead_evidence,
        )
        .unwrap();
        assert!(!receipt.acknowledged);
        assert!(receipt.pending_reasons[0].contains("durable owner receipt"));
        bead_evidence.local_receipt = true;
        assert!(
            artifact_link_publication_receipt(
                &bead_requirements,
                &bead_evidence
            )
            .unwrap()
            .acknowledged
        );
    }

    #[test]
    fn receipt_rejects_schema_or_operation_mismatches() {
        let requirements = artifact_link_event_owner_requirements(
            &event("agent:reader", "read", "agent:other"),
            &[],
        )
        .unwrap();
        let mut evidence = evidence_for(&requirements);
        evidence.operation_id = op('b');
        assert!(artifact_link_publication_receipt(&requirements, &evidence)
            .unwrap_err()
            .message
            .contains("operation_id mismatch"));

        let mut stale = requirements.clone();
        stale.schema_version = 2;
        assert!(artifact_link_publication_receipt(&stale, &evidence)
            .unwrap_err()
            .message
            .contains("schema_version"));
    }
}

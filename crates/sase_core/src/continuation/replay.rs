//! Deterministic continuation replay planning.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::budget::ContinuationBudgetDecisionWire;
use super::schema::{
    validate_checkpoint_coverage, validate_continuation_node,
    validate_identifier, validate_rendered_component, validate_schema,
    validate_text, ContinuationCheckpointCoverageWire, ContinuationError,
    ContinuationNodeWire, ContinuationOmissionWire,
    ContinuationRenderedComponentWire, CONTINUATION_WIRE_SCHEMA_VERSION,
    MAX_NODES, MAX_REF_BYTES,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationReplayPlanRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub records: Vec<ContinuationNodeWire>,
    #[serde(default)]
    pub root_ids: Vec<String>,
    #[serde(default)]
    pub selected_evidence_refs: Vec<String>,
    #[serde(default)]
    pub checkpoint_coverage: Vec<ContinuationCheckpointCoverageWire>,
    #[serde(default)]
    pub rendered_components: Vec<ContinuationRenderedComponentWire>,
    #[serde(default)]
    pub budget: Option<ContinuationBudgetDecisionWire>,
    #[serde(default)]
    pub prefix_reset_reason: Option<String>,
    #[serde(default)]
    pub max_depth: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationParentEdgeWire {
    pub child_id: String,
    pub parent_id: String,
    pub parent_index: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationBranchAttributionWire {
    pub branch_index: u32,
    pub root_id: String,
    pub node_id: String,
    pub reused: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationRenderedComponentSizesWire {
    #[serde(default)]
    pub components: Vec<ContinuationRenderedComponentWire>,
    pub total_utf8_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationReplayManifestWire {
    pub schema_version: u32,
    pub projection_version: u32,
    #[serde(default)]
    pub ordered_node_ids: Vec<String>,
    #[serde(default)]
    pub parent_edges: Vec<ContinuationParentEdgeWire>,
    #[serde(default)]
    pub branch_attribution: Vec<ContinuationBranchAttributionWire>,
    #[serde(default)]
    pub selected_evidence_refs: Vec<String>,
    #[serde(default)]
    pub checkpoint_coverage: Vec<ContinuationCheckpointCoverageWire>,
    #[serde(default)]
    pub omissions: Vec<ContinuationOmissionWire>,
    pub rendered_component_sizes: ContinuationRenderedComponentSizesWire,
    #[serde(default)]
    pub budget: Option<ContinuationBudgetDecisionWire>,
    #[serde(default)]
    pub prefix_reset_reason: Option<String>,
}

pub fn plan_continuation_replay(
    request: ContinuationReplayPlanRequestWire,
) -> Result<ContinuationReplayManifestWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationReplayPlanRequestWire",
    )?;
    if request.records.len() > MAX_NODES {
        return Err(ContinuationError::validation(format!(
            "records has {} entries; maximum is {MAX_NODES}",
            request.records.len()
        )));
    }
    if request.root_ids.is_empty() {
        return Err(ContinuationError::validation(
            "root_ids must contain at least one exact node id",
        ));
    }
    for (index, root_id) in request.root_ids.iter().enumerate() {
        validate_identifier(root_id, &format!("root_ids[{index}]"))?;
    }
    for (index, reference) in request.selected_evidence_refs.iter().enumerate()
    {
        validate_identifier(
            reference,
            &format!("selected_evidence_refs[{index}]"),
        )?;
    }
    for (index, coverage) in request.checkpoint_coverage.iter().enumerate() {
        validate_checkpoint_coverage(coverage, index)?;
    }
    for (index, component) in request.rendered_components.iter().enumerate() {
        validate_rendered_component(component, index)?;
    }
    if let Some(reason) = &request.prefix_reset_reason {
        validate_text(reason, "prefix_reset_reason", MAX_REF_BYTES)?;
    }
    let max_depth = request.max_depth.unwrap_or(MAX_NODES as u32) as usize;
    if max_depth == 0 {
        return Err(ContinuationError::validation(
            "max_depth must be greater than zero",
        ));
    }

    let mut records = BTreeMap::<String, ContinuationNodeWire>::new();
    for record in request.records {
        let record = validate_continuation_node(record)?;
        if let Some(existing) = records.get(&record.node_id) {
            if existing != &record {
                return Err(ContinuationError::conflict(format!(
                    "conflicting duplicate continuation node {}",
                    record.node_id
                )));
            }
            continue;
        }
        records.insert(record.node_id.clone(), record);
    }

    let mut ordered_node_ids = Vec::new();
    let mut parent_edges = Vec::new();
    let mut branch_attribution = Vec::new();
    let mut omissions = Vec::new();
    let mut seen = BTreeSet::new();

    for (branch_index, root_id) in request.root_ids.iter().enumerate() {
        let mut visiting = Vec::new();
        visit_node(
            root_id,
            branch_index as u32,
            root_id,
            &records,
            &mut seen,
            &mut visiting,
            &mut ordered_node_ids,
            &mut parent_edges,
            &mut branch_attribution,
            &mut omissions,
            max_depth,
            0,
        )?;
    }

    let total_utf8_bytes = request
        .rendered_components
        .iter()
        .map(|component| component.utf8_bytes)
        .sum();

    Ok(ContinuationReplayManifestWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        projection_version: 1,
        ordered_node_ids,
        parent_edges,
        branch_attribution,
        selected_evidence_refs: request.selected_evidence_refs,
        checkpoint_coverage: request.checkpoint_coverage,
        omissions,
        rendered_component_sizes: ContinuationRenderedComponentSizesWire {
            components: request.rendered_components,
            total_utf8_bytes,
        },
        budget: request.budget,
        prefix_reset_reason: request.prefix_reset_reason,
    })
}

#[allow(clippy::too_many_arguments)]
fn visit_node(
    node_id: &str,
    branch_index: u32,
    root_id: &str,
    records: &BTreeMap<String, ContinuationNodeWire>,
    seen: &mut BTreeSet<String>,
    visiting: &mut Vec<String>,
    ordered_node_ids: &mut Vec<String>,
    parent_edges: &mut Vec<ContinuationParentEdgeWire>,
    branch_attribution: &mut Vec<ContinuationBranchAttributionWire>,
    omissions: &mut Vec<ContinuationOmissionWire>,
    max_depth: usize,
    depth: usize,
) -> Result<(), ContinuationError> {
    if seen.contains(node_id) {
        branch_attribution.push(ContinuationBranchAttributionWire {
            branch_index,
            root_id: root_id.to_string(),
            node_id: node_id.to_string(),
            reused: true,
        });
        return Ok(());
    }
    if depth >= max_depth {
        return Err(ContinuationError::validation(format!(
            "continuation ancestry for {node_id} exceeds max_depth {max_depth}"
        )));
    }
    if let Some(position) =
        visiting.iter().position(|candidate| candidate == node_id)
    {
        let mut path = visiting[position..].to_vec();
        path.push(node_id.to_string());
        return Err(ContinuationError::cycle(format!(
            "continuation graph contains a cycle: {}",
            path.join(" -> ")
        )));
    }

    let Some(record) = records.get(node_id) else {
        omissions.push(ContinuationOmissionWire {
            kind: "missing_root".to_string(),
            node_id: Some(node_id.to_string()),
            parent_id: None,
            reason: "root id has no continuation record".to_string(),
        });
        return Ok(());
    };

    visiting.push(node_id.to_string());
    for (parent_index, parent_id) in record.parent_ids.iter().enumerate() {
        parent_edges.push(ContinuationParentEdgeWire {
            child_id: node_id.to_string(),
            parent_id: parent_id.clone(),
            parent_index: parent_index as u32,
        });
        if records.contains_key(parent_id) {
            visit_node(
                parent_id,
                branch_index,
                root_id,
                records,
                seen,
                visiting,
                ordered_node_ids,
                parent_edges,
                branch_attribution,
                omissions,
                max_depth,
                depth + 1,
            )?;
        } else {
            omissions.push(ContinuationOmissionWire {
                kind: "missing_parent".to_string(),
                node_id: Some(node_id.to_string()),
                parent_id: Some(parent_id.clone()),
                reason: "parent id has no continuation record".to_string(),
            });
        }
    }
    visiting.pop();

    seen.insert(node_id.to_string());
    ordered_node_ids.push(node_id.to_string());
    branch_attribution.push(ContinuationBranchAttributionWire {
        branch_index,
        root_id: root_id.to_string(),
        node_id: node_id.to_string(),
        reused: false,
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuation::schema::{
        ContinuationExecutionIdentityWire, ContinuationNodeKindWire,
    };

    fn owner() -> ContinuationExecutionIdentityWire {
        ContinuationExecutionIdentityWire {
            project: "sase".to_string(),
            run_id: "run-1".to_string(),
            agent_name: "agent-1".to_string(),
            machine_name: Some("athena".to_string()),
            workspace_id: None,
        }
    }

    fn node(id: &str, parents: Vec<&str>) -> ContinuationNodeWire {
        ContinuationNodeWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            node_id: id.to_string(),
            kind: ContinuationNodeKindWire::AgentDelta,
            parent_ids: parents.into_iter().map(str::to_string).collect(),
            owner: owner(),
            content_ref: format!("file:explicit:{id}"),
            content_sha256:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            checkpoint_ref: None,
            intent_ref: None,
            workspace_ref: None,
            attribution: None,
        }
    }

    fn request(
        records: Vec<ContinuationNodeWire>,
        roots: Vec<&str>,
    ) -> ContinuationReplayPlanRequestWire {
        ContinuationReplayPlanRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            records,
            root_ids: roots.into_iter().map(str::to_string).collect(),
            selected_evidence_refs: vec![],
            checkpoint_coverage: vec![],
            rendered_components: vec![],
            budget: None,
            prefix_reset_reason: None,
            max_depth: None,
        }
    }

    #[test]
    fn detects_cycles() {
        let err = plan_continuation_replay(request(
            vec![node("a", vec!["b"]), node("b", vec!["a"])],
            vec!["a"],
        ))
        .unwrap_err();

        assert_eq!(err.kind, "cycle");
        assert!(err.message.contains("a -> b -> a"));
    }

    #[test]
    fn rejects_conflicting_duplicate_ids() {
        let mut duplicate = node("a", vec![]);
        duplicate.content_sha256 =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                .to_string();

        let err = plan_continuation_replay(request(
            vec![node("a", vec![]), duplicate],
            vec!["a"],
        ))
        .unwrap_err();

        assert_eq!(err.kind, "conflict");
    }

    #[test]
    fn preserves_parent_order_for_diamond_graphs() {
        let manifest = plan_continuation_replay(request(
            vec![
                node("base", vec![]),
                node("left", vec!["base"]),
                node("right", vec!["base"]),
                node("merge", vec!["left", "right"]),
            ],
            vec!["merge"],
        ))
        .unwrap();

        assert_eq!(
            manifest.ordered_node_ids,
            vec!["base", "left", "right", "merge"]
        );
        assert_eq!(
            manifest
                .parent_edges
                .iter()
                .filter(|edge| edge.child_id == "merge")
                .map(|edge| edge.parent_id.as_str())
                .collect::<Vec<_>>(),
            vec!["left", "right"]
        );
        assert_eq!(
            manifest
                .branch_attribution
                .iter()
                .filter(|attribution| attribution.node_id == "base")
                .count(),
            2
        );
    }
}

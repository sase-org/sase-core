//! Provider-context budget decisions for continuation replay.

use serde::{Deserialize, Serialize};

use super::schema::{
    validate_reference, validate_schema, ContinuationError,
    CONTINUATION_WIRE_SCHEMA_VERSION,
};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationBudgetReductionKindWire {
    IdentityDeduplication,
    OldRawExcerpts,
    NewestDiagnostics,
    Checkpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationBudgetDecisionKindWire {
    Fits,
    Compact,
    Refuse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ContinuationBudgetReserveWire {
    #[serde(default)]
    pub context_limit_bytes: Option<u64>,
    #[serde(default)]
    pub transport_limit_bytes: Option<u64>,
    #[serde(default)]
    pub instruction_reserve_bytes: u64,
    #[serde(default)]
    pub tool_reserve_bytes: u64,
    #[serde(default)]
    pub output_reserve_bytes: u64,
    #[serde(default)]
    pub reasoning_reserve_bytes: u64,
    #[serde(default)]
    pub estimate_uncertain: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationBudgetReductionCandidateWire {
    pub kind: ContinuationBudgetReductionKindWire,
    pub bytes: u64,
    #[serde(default)]
    pub checkpoint_ref: Option<String>,
    #[serde(default)]
    pub covered_node_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationBudgetRequestWire {
    pub schema_version: u32,
    pub rendered_prompt_bytes: u64,
    pub essential_bytes: u64,
    #[serde(default)]
    pub selected_evidence_bytes: u64,
    #[serde(default)]
    pub provider_budget: ContinuationBudgetReserveWire,
    #[serde(default)]
    pub reduction_candidates: Vec<ContinuationBudgetReductionCandidateWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationBudgetDecisionWire {
    pub schema_version: u32,
    pub kind: ContinuationBudgetDecisionKindWire,
    pub prompt_budget_bytes: u64,
    pub estimated_prompt_bytes: u64,
    pub remaining_prompt_bytes: i64,
    #[serde(default)]
    pub reductions: Vec<ContinuationBudgetReductionCandidateWire>,
    #[serde(default)]
    pub reasons: Vec<String>,
}

pub fn plan_continuation_budget(
    request: ContinuationBudgetRequestWire,
) -> Result<ContinuationBudgetDecisionWire, ContinuationError> {
    validate_schema(request.schema_version, "ContinuationBudgetRequestWire")?;
    validate_reduction_candidates(&request.reduction_candidates)?;

    let prompt_budget_bytes =
        effective_prompt_budget(&request.provider_budget)?;
    let mut reasons = Vec::new();
    if request.provider_budget.estimate_uncertain {
        reasons.push("provider_budget_estimate_uncertain".to_string());
    }
    if request.provider_budget.context_limit_bytes.is_none()
        && request.provider_budget.transport_limit_bytes.is_none()
    {
        reasons.push("provider_limits_unknown".to_string());
    }

    if request.essential_bytes > prompt_budget_bytes {
        return Ok(decision(
            ContinuationBudgetDecisionKindWire::Refuse,
            prompt_budget_bytes,
            request.rendered_prompt_bytes,
            vec![],
            with_reason(reasons, "essential_content_exceeds_budget"),
        ));
    }

    if request.rendered_prompt_bytes <= prompt_budget_bytes {
        return Ok(decision(
            ContinuationBudgetDecisionKindWire::Fits,
            prompt_budget_bytes,
            request.rendered_prompt_bytes,
            vec![],
            reasons,
        ));
    }

    let mut candidates = request.reduction_candidates;
    candidates.sort_by_key(|candidate| candidate.kind);
    let mut applied = Vec::new();
    let mut estimated = request.rendered_prompt_bytes;
    for candidate in candidates {
        if candidate.bytes == 0 {
            continue;
        }
        estimated = estimated.saturating_sub(candidate.bytes);
        applied.push(candidate);
        if estimated <= prompt_budget_bytes {
            return Ok(decision(
                ContinuationBudgetDecisionKindWire::Compact,
                prompt_budget_bytes,
                estimated,
                applied,
                with_reason(reasons, "compaction_required"),
            ));
        }
    }

    Ok(decision(
        ContinuationBudgetDecisionKindWire::Refuse,
        prompt_budget_bytes,
        estimated,
        applied,
        with_reason(reasons, "context_budget_exceeded"),
    ))
}

fn effective_prompt_budget(
    provider_budget: &ContinuationBudgetReserveWire,
) -> Result<u64, ContinuationError> {
    let limit = match (
        provider_budget.context_limit_bytes,
        provider_budget.transport_limit_bytes,
    ) {
        (Some(context), Some(transport)) => context.min(transport),
        (Some(context), None) => context,
        (None, Some(transport)) => transport,
        (None, None) => u64::MAX,
    };
    let reserves = provider_budget
        .instruction_reserve_bytes
        .saturating_add(provider_budget.tool_reserve_bytes)
        .saturating_add(provider_budget.output_reserve_bytes)
        .saturating_add(provider_budget.reasoning_reserve_bytes);
    if reserves > limit {
        return Err(ContinuationError::budget(
            "provider reserves exceed the effective context/transport limit",
        ));
    }
    Ok(limit - reserves)
}

fn validate_reduction_candidates(
    candidates: &[ContinuationBudgetReductionCandidateWire],
) -> Result<(), ContinuationError> {
    for (index, candidate) in candidates.iter().enumerate() {
        if let Some(reference) = &candidate.checkpoint_ref {
            validate_reference(
                reference,
                &format!("reduction_candidates[{index}].checkpoint_ref"),
            )?;
        }
        for (node_index, node_id) in
            candidate.covered_node_ids.iter().enumerate()
        {
            validate_reference(
                node_id,
                &format!("reduction_candidates[{index}].covered_node_ids[{node_index}]"),
            )?;
        }
    }
    Ok(())
}

fn decision(
    kind: ContinuationBudgetDecisionKindWire,
    prompt_budget_bytes: u64,
    estimated_prompt_bytes: u64,
    reductions: Vec<ContinuationBudgetReductionCandidateWire>,
    reasons: Vec<String>,
) -> ContinuationBudgetDecisionWire {
    let remaining_prompt_bytes = if prompt_budget_bytes == u64::MAX {
        i64::MAX
    } else {
        prompt_budget_bytes as i64 - estimated_prompt_bytes as i64
    };
    ContinuationBudgetDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        kind,
        prompt_budget_bytes,
        estimated_prompt_bytes,
        remaining_prompt_bytes,
        reductions,
        reasons,
    }
}

fn with_reason(mut reasons: Vec<String>, reason: &str) -> Vec<String> {
    reasons.push(reason.to_string());
    reasons
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(rendered: u64, essential: u64) -> ContinuationBudgetRequestWire {
        ContinuationBudgetRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            rendered_prompt_bytes: rendered,
            essential_bytes: essential,
            selected_evidence_bytes: 0,
            provider_budget: ContinuationBudgetReserveWire {
                context_limit_bytes: Some(10_000),
                transport_limit_bytes: Some(9_000),
                instruction_reserve_bytes: 1_000,
                tool_reserve_bytes: 0,
                output_reserve_bytes: 0,
                reasoning_reserve_bytes: 0,
                estimate_uncertain: false,
            },
            reduction_candidates: vec![],
        }
    }

    #[test]
    fn fits_inside_budget() {
        let decision = plan_continuation_budget(request(4_000, 3_000)).unwrap();

        assert_eq!(decision.kind, ContinuationBudgetDecisionKindWire::Fits);
        assert_eq!(decision.prompt_budget_bytes, 8_000);
    }

    #[test]
    fn compacts_in_design_order() {
        let mut req = request(12_000, 6_000);
        req.reduction_candidates = vec![
            ContinuationBudgetReductionCandidateWire {
                kind: ContinuationBudgetReductionKindWire::Checkpoint,
                bytes: 5_000,
                checkpoint_ref: Some("file:explicit:checkpoint".to_string()),
                covered_node_ids: vec!["old-node".to_string()],
            },
            ContinuationBudgetReductionCandidateWire {
                kind:
                    ContinuationBudgetReductionKindWire::IdentityDeduplication,
                bytes: 2_000,
                checkpoint_ref: None,
                covered_node_ids: vec![],
            },
        ];

        let decision = plan_continuation_budget(req).unwrap();

        assert_eq!(decision.kind, ContinuationBudgetDecisionKindWire::Compact);
        assert_eq!(
            decision.reductions[0].kind,
            ContinuationBudgetReductionKindWire::IdentityDeduplication
        );
        assert_eq!(
            decision.reductions[1].kind,
            ContinuationBudgetReductionKindWire::Checkpoint
        );
    }

    #[test]
    fn refuses_when_essential_content_cannot_fit() {
        let decision =
            plan_continuation_budget(request(12_000, 9_001)).unwrap();

        assert_eq!(decision.kind, ContinuationBudgetDecisionKindWire::Refuse);
        assert!(decision
            .reasons
            .contains(&"essential_content_exceeds_budget".to_string()));
    }
}

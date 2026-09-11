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
    pub checkpoint_threshold_bytes: Option<u64>,
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
    pub target_prompt_bytes: u64,
    pub estimated_prompt_bytes: u64,
    pub remaining_prompt_bytes: i64,
    #[serde(default)]
    pub reductions: Vec<ContinuationBudgetReductionCandidateWire>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub recovery_guidance: Vec<String>,
    #[serde(default)]
    pub disposition: Option<String>,
}

pub fn plan_continuation_budget(
    request: ContinuationBudgetRequestWire,
) -> Result<ContinuationBudgetDecisionWire, ContinuationError> {
    validate_schema(request.schema_version, "ContinuationBudgetRequestWire")?;
    validate_reduction_candidates(&request.reduction_candidates)?;

    let prompt_budget_bytes =
        effective_prompt_budget(&request.provider_budget)?;
    let target_prompt_bytes = effective_target_prompt_budget(
        prompt_budget_bytes,
        request.checkpoint_threshold_bytes,
    )?;
    let mut reasons = Vec::new();
    if request.provider_budget.estimate_uncertain {
        reasons.push("provider_budget_estimate_uncertain".to_string());
    }
    if request.provider_budget.context_limit_bytes.is_none()
        && request.provider_budget.transport_limit_bytes.is_none()
    {
        reasons.push("provider_limits_unknown".to_string());
    }
    if request.rendered_prompt_bytes > target_prompt_bytes
        && target_prompt_bytes < prompt_budget_bytes
    {
        reasons.push("checkpoint_threshold_exceeded".to_string());
    }

    if request.essential_bytes > target_prompt_bytes {
        return Ok(decision(
            ContinuationBudgetDecisionKindWire::Refuse,
            prompt_budget_bytes,
            target_prompt_bytes,
            request.rendered_prompt_bytes,
            vec![],
            with_reason(reasons, "essential_content_exceeds_budget"),
        ));
    }

    if request.rendered_prompt_bytes <= target_prompt_bytes {
        return Ok(decision(
            ContinuationBudgetDecisionKindWire::Fits,
            prompt_budget_bytes,
            target_prompt_bytes,
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
        if estimated <= target_prompt_bytes {
            return Ok(decision(
                ContinuationBudgetDecisionKindWire::Compact,
                prompt_budget_bytes,
                target_prompt_bytes,
                estimated,
                applied,
                with_reason(reasons, "compaction_required"),
            ));
        }
    }

    Ok(decision(
        ContinuationBudgetDecisionKindWire::Refuse,
        prompt_budget_bytes,
        target_prompt_bytes,
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

fn effective_target_prompt_budget(
    prompt_budget_bytes: u64,
    checkpoint_threshold_bytes: Option<u64>,
) -> Result<u64, ContinuationError> {
    match checkpoint_threshold_bytes {
        Some(0) => Err(ContinuationError::budget(
            "checkpoint threshold must be greater than zero",
        )),
        Some(threshold) => Ok(threshold.min(prompt_budget_bytes)),
        None => Ok(prompt_budget_bytes),
    }
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
    target_prompt_bytes: u64,
    estimated_prompt_bytes: u64,
    reductions: Vec<ContinuationBudgetReductionCandidateWire>,
    reasons: Vec<String>,
) -> ContinuationBudgetDecisionWire {
    let remaining_prompt_bytes = if target_prompt_bytes == u64::MAX {
        i64::MAX
    } else {
        target_prompt_bytes as i64 - estimated_prompt_bytes as i64
    };
    let disposition = if kind == ContinuationBudgetDecisionKindWire::Refuse {
        Some("context_budget_exceeded".to_string())
    } else {
        None
    };
    let recovery_guidance = recovery_guidance(&kind, &reasons);
    ContinuationBudgetDecisionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        kind,
        prompt_budget_bytes,
        target_prompt_bytes,
        estimated_prompt_bytes,
        remaining_prompt_bytes,
        reductions,
        reasons,
        recovery_guidance,
        disposition,
    }
}

fn with_reason(mut reasons: Vec<String>, reason: &str) -> Vec<String> {
    reasons.push(reason.to_string());
    reasons
}

fn recovery_guidance(
    kind: &ContinuationBudgetDecisionKindWire,
    reasons: &[String],
) -> Vec<String> {
    if *kind != ContinuationBudgetDecisionKindWire::Refuse {
        return vec![];
    }
    if reasons
        .iter()
        .any(|reason| reason == "essential_content_exceeds_budget")
    {
        return vec![
            "Preserve the active objective, user constraints, human decisions, and current next action before retrying.".to_string(),
            "Resume with an adequate explicit checkpoint or choose a route with a larger context budget.".to_string(),
        ];
    }
    vec![
        "Resume with an adequate explicit checkpoint or choose a route with a larger context budget.".to_string(),
        "Do not rerun the monitored command solely to rebuild context.".to_string(),
    ]
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
            checkpoint_threshold_bytes: None,
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
        assert_eq!(decision.target_prompt_bytes, 8_000);
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
        assert_eq!(
            decision.disposition.as_deref(),
            Some("context_budget_exceeded")
        );
        assert!(!decision.recovery_guidance.is_empty());
    }

    #[test]
    fn checkpoint_threshold_uses_reductions_before_hard_limit() {
        let mut req = request(7_000, 2_000);
        req.checkpoint_threshold_bytes = Some(5_000);
        req.reduction_candidates =
            vec![ContinuationBudgetReductionCandidateWire {
                kind: ContinuationBudgetReductionKindWire::Checkpoint,
                bytes: 2_500,
                checkpoint_ref: Some("file:explicit:checkpoint".to_string()),
                covered_node_ids: vec!["old-node".to_string()],
            }];

        let decision = plan_continuation_budget(req).unwrap();

        assert_eq!(decision.kind, ContinuationBudgetDecisionKindWire::Compact);
        assert_eq!(decision.prompt_budget_bytes, 8_000);
        assert_eq!(decision.target_prompt_bytes, 5_000);
        assert_eq!(decision.estimated_prompt_bytes, 4_500);
        assert!(decision
            .reasons
            .contains(&"checkpoint_threshold_exceeded".to_string()));
        assert_eq!(
            decision.reductions[0].kind,
            ContinuationBudgetReductionKindWire::Checkpoint
        );
    }

    #[test]
    fn refuses_when_threshold_crosses_without_adequate_checkpoint() {
        let mut req = request(7_000, 2_000);
        req.checkpoint_threshold_bytes = Some(5_000);

        let decision = plan_continuation_budget(req).unwrap();

        assert_eq!(decision.kind, ContinuationBudgetDecisionKindWire::Refuse);
        assert_eq!(decision.prompt_budget_bytes, 8_000);
        assert_eq!(decision.target_prompt_bytes, 5_000);
        assert!(decision
            .reasons
            .contains(&"context_budget_exceeded".to_string()));
        assert!(decision
            .reasons
            .contains(&"checkpoint_threshold_exceeded".to_string()));
    }
}

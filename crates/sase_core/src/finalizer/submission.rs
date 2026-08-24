use std::collections::BTreeSet;

use super::digest::{canonical_json_bytes, finalizer_digest_serializable};
use super::selection::{
    finalizer_plan_digest, validate_digest, validate_instance_id,
    validate_list_len, validate_required_text, validate_schema,
};
use super::wire::{
    FinalizerContextWire, FinalizerDeferralWire, FinalizerPlanWire,
    FinalizerSubmissionEnvelopeWire, FinalizerSubmissionValidationWire,
    FinalizerTriggerKindWire, FINALIZER_PAYLOAD_MAX_BYTES,
    FINALIZER_WIRE_SCHEMA_VERSION,
};
use super::FinalizerError;

pub fn validate_finalizer_context(
    plan: &FinalizerPlanWire,
    context: &FinalizerContextWire,
) -> Result<String, FinalizerError> {
    validate_schema(plan.schema_version, "plan")?;
    validate_schema(context.schema_version, "context")?;
    validate_required_text(&context.run_id, "run_id")?;
    validate_required_text(&context.agent_id, "agent_id")?;
    validate_required_text(&context.turn_nonce, "turn_nonce")?;
    let actual_plan_digest = finalizer_plan_digest(plan)?;
    if context.plan_digest != plan.plan_digest
        || context.plan_digest != actual_plan_digest
    {
        return Err(FinalizerError::validation(
            "context plan_digest does not match the resolved plan",
        ));
    }
    validate_list_len(context.requirements.len(), "requirements")?;
    validate_list_len(context.obligations.len(), "obligations")?;

    let selected = plan
        .entries
        .iter()
        .map(|entry| entry.instance_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_requirements = BTreeSet::new();
    for requirement in &context.requirements {
        validate_instance_id(&requirement.instance_id)?;
        if !selected.contains(requirement.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "context requirement references unselected instance '{}'",
                requirement.instance_id
            )));
        }
        if !seen_requirements.insert(requirement.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "duplicate requirement for instance '{}'",
                requirement.instance_id
            )));
        }
        if requirement.trigger == FinalizerTriggerKindWire::NotTriggered
            && requirement.submission_required
        {
            return Err(FinalizerError::validation(format!(
                "instance '{}' cannot require submission when not triggered",
                requirement.instance_id
            )));
        }
        if let Some(digest) = &requirement.requirement_digest {
            validate_digest(digest, "requirement_digest")?;
        }
    }
    for entry in &plan.entries {
        if !seen_requirements.contains(entry.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "context is missing requirement coverage for selected instance '{}'",
                entry.instance_id
            )));
        }
    }

    let mut seen_obligations = BTreeSet::new();
    for obligation in &context.obligations {
        validate_required_text(&obligation.obligation_id, "obligation_id")?;
        validate_required_text(&obligation.kind, "obligation.kind")?;
        validate_list_len(obligation.paths.len(), "obligation.paths")?;
        if !seen_obligations.insert(obligation.obligation_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "duplicate obligation_id '{}'",
                obligation.obligation_id
            )));
        }
        if let Some(display) = &obligation.display_name {
            validate_required_text(display, "obligation.display_name")?;
        }
        if let Some(digest) = &obligation.digest {
            validate_digest(digest, "obligation.digest")?;
        }
    }

    let digest = finalizer_context_digest(context)?;
    if let Some(expected) = &context.context_digest {
        if expected != &digest {
            return Err(FinalizerError::validation(
                "context_digest does not match context content",
            ));
        }
    }
    Ok(digest)
}

pub fn finalizer_context_digest(
    context: &FinalizerContextWire,
) -> Result<String, FinalizerError> {
    let mut normalized = context.clone();
    normalized.context_digest = None;
    finalizer_digest_serializable(&normalized)
}

pub fn validate_finalizer_submission(
    plan: &FinalizerPlanWire,
    context: &FinalizerContextWire,
    submission: &FinalizerSubmissionEnvelopeWire,
) -> Result<FinalizerSubmissionValidationWire, FinalizerError> {
    validate_schema(submission.schema_version, "submission")?;
    let context_digest = validate_finalizer_context(plan, context)?;
    if submission.run_id != context.run_id {
        return Err(FinalizerError::validation("submission run_id is stale"));
    }
    if submission.agent_id != context.agent_id {
        return Err(FinalizerError::validation("submission agent_id is stale"));
    }
    if submission.turn_nonce != context.turn_nonce {
        return Err(FinalizerError::validation(
            "submission turn_nonce is stale",
        ));
    }
    if submission.plan_digest != context.plan_digest {
        return Err(FinalizerError::validation(
            "submission plan_digest is stale",
        ));
    }
    if submission.context_digest != context_digest {
        return Err(FinalizerError::validation(
            "submission context_digest is stale",
        ));
    }
    validate_list_len(submission.payloads.len(), "payloads")?;

    let required = context
        .requirements
        .iter()
        .filter(|requirement| requirement.submission_required)
        .map(|requirement| requirement.instance_id.as_str())
        .collect::<BTreeSet<_>>();
    let selected = plan
        .entries
        .iter()
        .map(|entry| entry.instance_id.as_str())
        .collect::<BTreeSet<_>>();

    let mut seen = BTreeSet::new();
    let mut accepted = Vec::new();
    for payload in &submission.payloads {
        validate_instance_id(&payload.instance_id)?;
        if !selected.contains(payload.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "submission payload references unselected instance '{}'",
                payload.instance_id
            )));
        }
        if !required.contains(payload.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "submission payload for instance '{}' was not required by context",
                payload.instance_id
            )));
        }
        if !seen.insert(payload.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "duplicate submission payload for instance '{}'",
                payload.instance_id
            )));
        }
        let encoded = canonical_json_bytes(&payload.payload)?;
        if encoded.len() > FINALIZER_PAYLOAD_MAX_BYTES {
            return Err(FinalizerError::validation(format!(
                "submission payload for instance '{}' exceeds {FINALIZER_PAYLOAD_MAX_BYTES} bytes",
                payload.instance_id
            )));
        }
        if let Some(expected) = &payload.payload_digest {
            let actual =
                super::digest::canonical_json_sha256(&payload.payload)?;
            if expected != &actual {
                return Err(FinalizerError::validation(format!(
                    "payload_digest for instance '{}' does not match payload",
                    payload.instance_id
                )));
            }
        }
        accepted.push(payload.instance_id.clone());
    }

    for instance_id in required {
        if !seen.contains(instance_id) {
            return Err(FinalizerError::validation(format!(
                "submission is missing required payload for instance '{instance_id}'"
            )));
        }
    }

    Ok(FinalizerSubmissionValidationWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        submission_digest: finalizer_digest_serializable(submission)?,
        accepted_instances: accepted,
    })
}

/// Validate the shape of one typed deferral. `reason` is already a closed
/// enum, so serde rejects an unknown value before this ever runs; this
/// checks what serde cannot: that the deferral names at least one path,
/// bounded by the same list-length limit as every other wire list.
pub fn validate_finalizer_deferral(
    deferral: &FinalizerDeferralWire,
) -> Result<(), FinalizerError> {
    validate_list_len(deferral.paths.len(), "deferral.paths")?;
    if deferral.paths.is_empty() {
        return Err(FinalizerError::validation(
            "deferral.paths must name at least one path",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::finalizer::selection::resolve_finalizer_plan;
    use crate::finalizer::wire::{
        FinalizerDeferralReasonWire, FinalizerInstancePolicyWire,
        FinalizerInstanceSpecWire, FinalizerPayloadRequirementWire,
        FinalizerPlanInputWire, FinalizerRefusalPolicyWire,
        FinalizerSelectorOpWire, FinalizerSubmissionPayloadWire,
        FINALIZER_LIST_MAX_LEN,
    };

    fn plan() -> FinalizerPlanWire {
        resolve_finalizer_plan(&FinalizerPlanInputWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            instances: vec![FinalizerInstanceSpecWire {
                schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
                instance_id: "commit".to_string(),
                provider_ref: "builtin@commit".to_string(),
                after: Vec::new(),
                policy: FinalizerInstancePolicyWire {
                    max_attempts: 2,
                    refusal: FinalizerRefusalPolicyWire::Fail,
                },
                config_digest: None,
                provenance_id: None,
            }],
            defaults: vec!["commit".to_string()],
            required: Vec::new(),
            selectors: vec![FinalizerSelectorOpWire::Add {
                instance_id: "commit".to_string(),
            }],
        })
        .unwrap()
    }

    fn context(plan: &FinalizerPlanWire) -> FinalizerContextWire {
        let mut context = FinalizerContextWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            run_id: "run-1".to_string(),
            agent_id: "agent-1".to_string(),
            turn_nonce: "nonce-1".to_string(),
            plan_digest: plan.plan_digest.clone(),
            requirements: vec![FinalizerPayloadRequirementWire {
                instance_id: "commit".to_string(),
                trigger: FinalizerTriggerKindWire::DirtyRepository,
                submission_required: true,
                requirement_digest: None,
            }],
            obligations: Vec::new(),
            context_digest: None,
        };
        context.context_digest =
            Some(finalizer_context_digest(&context).unwrap());
        context
    }

    fn submission(
        context: &FinalizerContextWire,
    ) -> FinalizerSubmissionEnvelopeWire {
        FinalizerSubmissionEnvelopeWire {
            schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
            run_id: context.run_id.clone(),
            agent_id: context.agent_id.clone(),
            turn_nonce: context.turn_nonce.clone(),
            plan_digest: context.plan_digest.clone(),
            context_digest: context.context_digest.clone().unwrap(),
            payloads: vec![FinalizerSubmissionPayloadWire {
                instance_id: "commit".to_string(),
                payload: json!({"repositories": []}),
                payload_digest: None,
            }],
        }
    }

    #[test]
    fn validates_complete_submission_and_digest_identity() {
        let plan = plan();
        let context = context(&plan);
        let submission = submission(&context);
        let result =
            validate_finalizer_submission(&plan, &context, &submission)
                .unwrap();
        assert_eq!(result.accepted_instances, vec!["commit"]);
        assert_eq!(result.submission_digest.len(), 64);
    }

    #[test]
    fn rejects_stale_identity_fields() {
        let plan = plan();
        let context = context(&plan);
        for (field, mut submission) in [
            ("run_id", submission(&context)),
            ("agent_id", submission(&context)),
            ("turn_nonce", submission(&context)),
            ("plan_digest", submission(&context)),
            ("context_digest", submission(&context)),
        ] {
            match field {
                "run_id" => submission.run_id = "other".to_string(),
                "agent_id" => submission.agent_id = "other".to_string(),
                "turn_nonce" => submission.turn_nonce = "other".to_string(),
                "plan_digest" => submission.plan_digest = "0".repeat(64),
                "context_digest" => submission.context_digest = "0".repeat(64),
                _ => unreachable!(),
            }
            assert!(validate_finalizer_submission(
                &plan,
                &context,
                &submission
            )
            .unwrap_err()
            .to_string()
            .contains(field));
        }
    }

    #[test]
    fn rejects_missing_duplicate_and_unexpected_payloads() {
        let plan = plan();
        let context = context(&plan);
        let mut missing = submission(&context);
        missing.payloads.clear();
        assert!(validate_finalizer_submission(&plan, &context, &missing)
            .unwrap_err()
            .to_string()
            .contains("missing required"));

        let mut duplicate = submission(&context);
        duplicate.payloads.push(duplicate.payloads[0].clone());
        assert!(validate_finalizer_submission(&plan, &context, &duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));

        let mut unexpected_context = context.clone();
        unexpected_context.requirements[0].submission_required = false;
        unexpected_context.context_digest =
            Some(finalizer_context_digest(&unexpected_context).unwrap());
        let unexpected = submission(&unexpected_context);
        assert!(validate_finalizer_submission(
            &plan,
            &unexpected_context,
            &unexpected
        )
        .unwrap_err()
        .to_string()
        .contains("not required"));
    }

    #[test]
    fn deferral_paths_must_be_nonempty_and_bounded() {
        let empty = FinalizerDeferralWire {
            reason: FinalizerDeferralReasonWire::ProtectedPaths,
            paths: Vec::new(),
        };
        assert!(validate_finalizer_deferral(&empty)
            .unwrap_err()
            .to_string()
            .contains("at least one path"));

        let too_many = FinalizerDeferralWire {
            reason: FinalizerDeferralReasonWire::ForeignWork,
            paths: (0..=FINALIZER_LIST_MAX_LEN)
                .map(|index| format!("path-{index}"))
                .collect(),
        };
        assert!(validate_finalizer_deferral(&too_many).is_err());

        let ok = FinalizerDeferralWire {
            reason: FinalizerDeferralReasonWire::UnsafeContent,
            paths: vec!["notes/secret.md".to_string()],
        };
        assert!(validate_finalizer_deferral(&ok).is_ok());
    }
}

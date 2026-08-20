use std::collections::BTreeSet;

use super::selection::{
    validate_instance_id, validate_list_len, validate_required_text,
};
use super::wire::{
    FinalizerAggregateResultWire, FinalizerAggregateStatusWire,
    FinalizerDiagnosticSeverityWire, FinalizerDiagnosticWire,
    FinalizerInstanceResultWire, FinalizerInstanceStatusWire,
    FINALIZER_ATTEMPT_MAX_LEN, FINALIZER_DIAGNOSTIC_MAX_LEN,
    FINALIZER_LIST_MAX_LEN, FINALIZER_WIRE_SCHEMA_VERSION,
};
use super::FinalizerError;

pub fn validate_finalizer_instance_results(
    results: &[FinalizerInstanceResultWire],
) -> Result<(), FinalizerError> {
    validate_list_len(results.len(), "instance results")?;
    let mut seen = BTreeSet::new();
    for result in results {
        validate_instance_id(&result.instance_id)?;
        if !seen.insert(result.instance_id.as_str()) {
            return Err(FinalizerError::validation(format!(
                "duplicate instance result '{}'",
                result.instance_id
            )));
        }
        if result.attempts.len() > FINALIZER_ATTEMPT_MAX_LEN {
            return Err(FinalizerError::validation(format!(
                "instance '{}' has too many attempts",
                result.instance_id
            )));
        }
        if result.diagnostics.len() > FINALIZER_DIAGNOSTIC_MAX_LEN {
            return Err(FinalizerError::validation(format!(
                "instance '{}' has too many diagnostics",
                result.instance_id
            )));
        }
        if result.evidence.len() > FINALIZER_LIST_MAX_LEN {
            return Err(FinalizerError::validation(format!(
                "instance '{}' has too many evidence records",
                result.instance_id
            )));
        }
        if result.status == FinalizerInstanceStatusWire::Refused {
            validate_required_text(
                result.refusal_reason.as_deref().unwrap_or_default(),
                "refusal_reason",
            )?;
        } else if result.refusal_reason.is_some() {
            return Err(FinalizerError::validation(format!(
                "instance '{}' has refusal_reason without refused status",
                result.instance_id
            )));
        }
        for attempt in &result.attempts {
            if attempt.attempt == 0 {
                return Err(FinalizerError::validation(format!(
                    "instance '{}' attempt numbers are 1-based",
                    result.instance_id
                )));
            }
        }
        validate_diagnostics(&result.diagnostics)?;
        for evidence in &result.evidence {
            validate_required_text(&evidence.kind, "evidence.kind")?;
            validate_required_text(&evidence.value, "evidence.value")?;
        }
    }
    Ok(())
}

pub fn aggregate_finalizer_outcomes(
    results: Vec<FinalizerInstanceResultWire>,
) -> Result<FinalizerAggregateResultWire, FinalizerError> {
    validate_finalizer_instance_results(&results)?;
    let status = if results
        .iter()
        .any(|result| result.status == FinalizerInstanceStatusWire::Failed)
    {
        FinalizerAggregateStatusWire::Failed
    } else if results
        .iter()
        .any(|result| result.status == FinalizerInstanceStatusWire::Refused)
    {
        FinalizerAggregateStatusWire::Refused
    } else if results
        .iter()
        .any(|result| result.status == FinalizerInstanceStatusWire::Pending)
    {
        FinalizerAggregateStatusWire::Pending
    } else {
        FinalizerAggregateStatusWire::Success
    };
    let diagnostics = aggregate_diagnostics(status, &results);
    Ok(FinalizerAggregateResultWire {
        schema_version: FINALIZER_WIRE_SCHEMA_VERSION,
        status,
        instances: results,
        diagnostics,
    })
}

fn aggregate_diagnostics(
    status: FinalizerAggregateStatusWire,
    results: &[FinalizerInstanceResultWire],
) -> Vec<FinalizerDiagnosticWire> {
    let code = match status {
        FinalizerAggregateStatusWire::Success => return Vec::new(),
        FinalizerAggregateStatusWire::Pending => "finalizer_pending",
        FinalizerAggregateStatusWire::Refused => "finalizer_refused",
        FinalizerAggregateStatusWire::Failed => "finalizer_failed",
    };
    let instance_id = results
        .iter()
        .find(|result| match status {
            FinalizerAggregateStatusWire::Pending => {
                result.status == FinalizerInstanceStatusWire::Pending
            }
            FinalizerAggregateStatusWire::Refused => {
                result.status == FinalizerInstanceStatusWire::Refused
            }
            FinalizerAggregateStatusWire::Failed => {
                result.status == FinalizerInstanceStatusWire::Failed
            }
            FinalizerAggregateStatusWire::Success => false,
        })
        .map(|result| result.instance_id.clone());
    Vec::from([FinalizerDiagnosticWire {
        code: code.to_string(),
        message: format!("aggregate finalizer status is {status:?}"),
        severity: FinalizerDiagnosticSeverityWire::Error,
        instance_id,
    }])
}

fn validate_diagnostics(
    diagnostics: &[FinalizerDiagnosticWire],
) -> Result<(), FinalizerError> {
    for diagnostic in diagnostics {
        validate_required_text(&diagnostic.code, "diagnostic.code")?;
        validate_required_text(&diagnostic.message, "diagnostic.message")?;
        if let Some(instance_id) = &diagnostic.instance_id {
            validate_instance_id(instance_id)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn result(
        instance_id: &str,
        status: FinalizerInstanceStatusWire,
    ) -> FinalizerInstanceResultWire {
        FinalizerInstanceResultWire {
            instance_id: instance_id.to_string(),
            status,
            attempts: Vec::new(),
            refusal_reason: (status == FinalizerInstanceStatusWire::Refused)
                .then(|| "No attributable commit should be made".to_string()),
            evidence: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn aggregate_failure_precedence_is_stable() {
        let aggregate = aggregate_finalizer_outcomes(vec![
            result("audit", FinalizerInstanceStatusWire::Refused),
            result("commit", FinalizerInstanceStatusWire::Failed),
            result("lint", FinalizerInstanceStatusWire::Pending),
        ])
        .unwrap();
        assert_eq!(aggregate.status, FinalizerAggregateStatusWire::Failed);
        assert_eq!(
            aggregate.diagnostics[0].instance_id.as_deref(),
            Some("commit")
        );

        let aggregate = aggregate_finalizer_outcomes(vec![
            result("audit", FinalizerInstanceStatusWire::Refused),
            result("lint", FinalizerInstanceStatusWire::Pending),
        ])
        .unwrap();
        assert_eq!(aggregate.status, FinalizerAggregateStatusWire::Refused);
    }

    #[test]
    fn serde_rejects_unknown_statuses() {
        let value = json!({
            "instance_id": "commit",
            "status": "mystery",
            "attempts": [],
            "diagnostics": []
        });
        let error =
            serde_json::from_value::<FinalizerInstanceResultWire>(value)
                .unwrap_err();
        assert!(error.to_string().contains("unknown variant"));
    }

    #[test]
    fn refused_status_requires_reason() {
        let mut refused =
            result("commit", FinalizerInstanceStatusWire::Refused);
        refused.refusal_reason = None;
        assert!(validate_finalizer_instance_results(&[refused])
            .unwrap_err()
            .to_string()
            .contains("refusal_reason"));
    }
}

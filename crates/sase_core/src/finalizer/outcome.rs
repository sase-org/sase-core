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
        validate_attempt_ledger(result)?;
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
        attempt: None,
    }])
}

fn validate_attempt_ledger(
    result: &FinalizerInstanceResultWire,
) -> Result<(), FinalizerError> {
    let mut previous_attempt = 0_u32;
    for attempt in &result.attempts {
        if attempt.attempt == 0 {
            return Err(FinalizerError::validation(format!(
                "instance '{}' attempt numbers are 1-based",
                result.instance_id
            )));
        }
        if attempt.attempt <= previous_attempt {
            return Err(FinalizerError::validation(format!(
                "instance '{}' attempt numbers must be unique and increasing",
                result.instance_id
            )));
        }
        previous_attempt = attempt.attempt;
    }
    match result.status {
        FinalizerInstanceStatusWire::Skipped => {
            if !result.attempts.is_empty() {
                return Err(FinalizerError::validation(format!(
                    "instance '{}' skipped status cannot record attempts",
                    result.instance_id
                )));
            }
        }
        FinalizerInstanceStatusWire::Failed
        | FinalizerInstanceStatusWire::Refused => {
            if result.attempts.is_empty() {
                return Err(FinalizerError::validation(format!(
                    "instance '{}' {} status requires a terminal attempt",
                    result.instance_id,
                    instance_status_name(result.status)
                )));
            }
        }
        FinalizerInstanceStatusWire::Success
        | FinalizerInstanceStatusWire::Pending => {}
    }
    if let Some(last) = result.attempts.last() {
        if last.status != result.status {
            return Err(FinalizerError::validation(format!(
                "instance '{}' terminal status does not match last attempt",
                result.instance_id
            )));
        }
    }
    Ok(())
}

fn instance_status_name(status: FinalizerInstanceStatusWire) -> &'static str {
    match status {
        FinalizerInstanceStatusWire::Pending => "pending",
        FinalizerInstanceStatusWire::Skipped => "skipped",
        FinalizerInstanceStatusWire::Success => "success",
        FinalizerInstanceStatusWire::Refused => "refused",
        FinalizerInstanceStatusWire::Failed => "failed",
    }
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
        if diagnostic.attempt == Some(0) {
            return Err(FinalizerError::validation(
                "diagnostic attempt numbers are 1-based".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::wire::FinalizerAttemptWire;
    use super::*;

    fn result(
        instance_id: &str,
        status: FinalizerInstanceStatusWire,
    ) -> FinalizerInstanceResultWire {
        let attempts = match status {
            FinalizerInstanceStatusWire::Failed
            | FinalizerInstanceStatusWire::Refused => {
                vec![FinalizerAttemptWire {
                    attempt: 1,
                    status,
                    diagnostic_code: None,
                }]
            }
            _ => Vec::new(),
        };
        FinalizerInstanceResultWire {
            instance_id: instance_id.to_string(),
            status,
            attempts,
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

    #[test]
    fn all_skipped_aggregates_success_and_all_failed_aggregates_failed() {
        let skipped = aggregate_finalizer_outcomes(vec![
            result("lint", FinalizerInstanceStatusWire::Skipped),
            result("audit", FinalizerInstanceStatusWire::Skipped),
        ])
        .unwrap();
        assert_eq!(skipped.status, FinalizerAggregateStatusWire::Success);
        assert!(skipped.diagnostics.is_empty());

        let failed = aggregate_finalizer_outcomes(vec![
            result("lint", FinalizerInstanceStatusWire::Failed),
            result("audit", FinalizerInstanceStatusWire::Failed),
        ])
        .unwrap();
        assert_eq!(failed.status, FinalizerAggregateStatusWire::Failed);
        assert_eq!(failed.diagnostics[0].instance_id.as_deref(), Some("lint"));
    }

    #[test]
    fn attempt_numbers_must_be_unique_increasing_and_terminal() {
        let mut duplicate = result("lint", FinalizerInstanceStatusWire::Failed);
        duplicate.attempts = vec![
            FinalizerAttemptWire {
                attempt: 1,
                status: FinalizerInstanceStatusWire::Failed,
                diagnostic_code: None,
            },
            FinalizerAttemptWire {
                attempt: 1,
                status: FinalizerInstanceStatusWire::Failed,
                diagnostic_code: None,
            },
        ];
        assert!(validate_finalizer_instance_results(&[duplicate])
            .unwrap_err()
            .to_string()
            .contains("unique and increasing"));

        let mut mismatched =
            result("lint", FinalizerInstanceStatusWire::Failed);
        mismatched.attempts[0].status = FinalizerInstanceStatusWire::Success;
        assert!(validate_finalizer_instance_results(&[mismatched])
            .unwrap_err()
            .to_string()
            .contains("terminal status"));

        let mut skipped_with_attempts =
            result("lint", FinalizerInstanceStatusWire::Skipped);
        skipped_with_attempts.attempts = vec![FinalizerAttemptWire {
            attempt: 1,
            status: FinalizerInstanceStatusWire::Skipped,
            diagnostic_code: None,
        }];
        assert!(
            validate_finalizer_instance_results(&[skipped_with_attempts])
                .unwrap_err()
                .to_string()
                .contains("skipped status cannot record attempts")
        );
    }
}

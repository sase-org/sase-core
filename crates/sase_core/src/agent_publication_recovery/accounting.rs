use std::collections::BTreeMap;

use super::classify::classify_publication_error_class;
use super::support::{logical_key_tuple, require_schema};
use super::wire::{
    PublicationFailureAccountingRequestWire,
    PublicationFailureAccountingResponseWire, PublicationFailureChargeWire,
    PublicationFailureRowWire, PublicationRecoveryError,
    PUBLICATION_FAILURE_SCOPE_HOOD, PUBLICATION_FAILURE_SCOPE_REPOSITORY,
    PUBLICATION_FAILURE_SCOPE_UNKNOWN,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
};

pub fn decide_publication_failure_accounting(
    request: &PublicationFailureAccountingRequestWire,
) -> Result<PublicationFailureAccountingResponseWire, PublicationRecoveryError>
{
    require_schema(request.schema_version)?;
    if let Some(threshold) = request.quarantine_threshold {
        if threshold == 0 {
            return Err(PublicationRecoveryError::InvalidRequest(
                "quarantine_threshold must be a positive integer".to_string(),
            ));
        }
    }
    let error_class =
        classify_publication_error_class(Some(request.error.as_str()));
    let scope = match request
        .scope
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(PUBLICATION_FAILURE_SCOPE_HOOD) => PUBLICATION_FAILURE_SCOPE_HOOD,
        Some(PUBLICATION_FAILURE_SCOPE_REPOSITORY) => {
            PUBLICATION_FAILURE_SCOPE_REPOSITORY
        }
        Some(_) | None => PUBLICATION_FAILURE_SCOPE_UNKNOWN,
    };
    if scope != PUBLICATION_FAILURE_SCOPE_HOOD {
        return Ok(PublicationFailureAccountingResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            scope: scope.to_string(),
            charge: false,
            error_class: error_class.to_string(),
            charges: Vec::new(),
        });
    }

    let mut rows_by_key = BTreeMap::new();
    for row in &request.rows {
        let key = (
            super::support::require_nonempty(
                "global_agent",
                &row.global_agent,
            )?,
            super::support::require_nonempty(
                "primary_revision",
                &row.primary_revision,
            )?,
        );
        rows_by_key.entry(key).or_insert(row);
    }

    let mut seen = std::collections::BTreeSet::new();
    let mut charges = Vec::new();
    for key in &request.attempted_keys {
        let (global_agent, primary_revision) = logical_key_tuple(key)?;
        if !seen.insert((global_agent.clone(), primary_revision.clone())) {
            continue;
        }
        let row = rows_by_key
            .get(&(global_agent.clone(), primary_revision.clone()))
            .map(|value| (*value).clone())
            .unwrap_or_else(|| PublicationFailureRowWire {
                global_agent: global_agent.clone(),
                primary_revision: primary_revision.clone(),
                attempts: 0,
                last_error: None,
                terminal: false,
                quarantined: false,
                terminal_reason: None,
            });
        charges.push(charge_hood_row(
            &row,
            &request.error,
            request.format_error,
            request.quarantine_threshold,
        ));
    }

    Ok(PublicationFailureAccountingResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        scope: PUBLICATION_FAILURE_SCOPE_HOOD.to_string(),
        charge: !charges.is_empty(),
        error_class: error_class.to_string(),
        charges,
    })
}

fn charge_hood_row(
    row: &PublicationFailureRowWire,
    error: &str,
    format_error: bool,
    quarantine_threshold: Option<u32>,
) -> PublicationFailureChargeWire {
    let attempts = row.attempts.saturating_add(1);
    let retire = row.terminal
        || (format_error
            && row.attempts > 0
            && row.last_error.as_deref() == Some(error));
    let quarantine = !retire
        && (row.quarantined
            || quarantine_threshold
                .is_some_and(|threshold| attempts >= threshold));
    PublicationFailureChargeWire {
        global_agent: row.global_agent.clone(),
        primary_revision: row.primary_revision.clone(),
        attempts,
        increment_attempts: true,
        quarantine,
        retire,
        terminal_reason: if retire {
            row.terminal_reason
                .clone()
                .or_else(|| format_error.then(|| error.to_string()))
        } else {
            None
        },
    }
}

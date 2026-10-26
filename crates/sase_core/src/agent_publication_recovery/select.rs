use std::collections::BTreeSet;

use super::classify::classify_publication_error_class;
use super::support::{
    active_sort_key, is_retry_eligible, logical_key_tuple, require_nonempty,
    require_positive_limit, require_schema, row_sort_key, selected_from_row,
    validate_error_classes,
};
use super::wire::{
    PublicationDrainSelectionRequestWire,
    PublicationDrainSelectionResponseWire, PublicationRecoveryError,
    PublicationRetryRowWire, PublicationRetrySelectedWire,
    PublicationRetrySelectionRequestWire,
    PublicationRetrySelectionResponseWire, PUBLICATION_LIFECYCLE_ACTIVE,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
};

struct IndexedRow<'a> {
    global_agent: String,
    primary_revision: String,
    row: &'a PublicationRetryRowWire,
    selected: PublicationRetrySelectedWire,
}

pub fn select_publication_retries(
    request: &PublicationRetrySelectionRequestWire,
) -> Result<PublicationRetrySelectionResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    if !request.retry_retired && !request.retry_quarantined {
        return Err(PublicationRecoveryError::InvalidRequest(
            "at least one of retry_retired or retry_quarantined must be true"
                .to_string(),
        ));
    }
    if let Some(limit) = request.limit {
        require_positive_limit(limit)?;
    }
    let error_classes = validate_error_classes(&request.error_classes)?;
    let mut selected = collect_eligible_retries(
        &request.rows,
        request.retry_retired,
        request.retry_quarantined,
        &error_classes,
        request.limit.is_some(),
    )?;
    if let Some(limit) = request.limit {
        selected.truncate(limit as usize);
    }
    Ok(PublicationRetrySelectionResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        selected,
    })
}

pub fn select_publication_drain(
    request: &PublicationDrainSelectionRequestWire,
) -> Result<PublicationDrainSelectionResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let limit = require_positive_limit(request.limit)? as usize;
    let error_classes = validate_error_classes(&request.error_classes)?;
    if request.exact_key.is_some() && request.preferred_key.is_some() {
        return Err(PublicationRecoveryError::InvalidRequest(
            "exact_key and preferred_key are mutually exclusive".to_string(),
        ));
    }
    let indexed = index_rows(&request.rows)?;
    let mut seen = BTreeSet::new();
    let mut active = Vec::new();
    let mut revive = Vec::new();

    if let Some(exact) = request.exact_key.as_ref() {
        let exact_key = logical_key_tuple(exact)?;
        if let Some(row) = indexed.iter().find(|row| {
            row.global_agent == exact_key.0
                && row.primary_revision == exact_key.1
        }) {
            if row_is_drain_candidate(row, request, &error_classes) {
                push_selected(row, &mut seen, limit, &mut active, &mut revive);
            }
        }
        return Ok(PublicationDrainSelectionResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            active,
            revive,
        });
    }

    if let Some(preferred) = request.preferred_key.as_ref() {
        let preferred_key = logical_key_tuple(preferred)?;
        if let Some(row) = indexed.iter().find(|row| {
            row.global_agent == preferred_key.0
                && row.primary_revision == preferred_key.1
        }) {
            if row_is_drain_candidate(row, request, &error_classes) {
                push_selected(row, &mut seen, limit, &mut active, &mut revive);
            }
        }
    }

    let mut actives: Vec<&IndexedRow<'_>> = indexed
        .iter()
        .filter(|row| row.selected.prior_class == PUBLICATION_LIFECYCLE_ACTIVE)
        .collect();
    actives.sort_by(|left, right| {
        active_sort_key(left.row, &left.global_agent, &left.primary_revision)
            .cmp(&active_sort_key(
                right.row,
                &right.global_agent,
                &right.primary_revision,
            ))
    });
    for row in actives {
        push_selected(row, &mut seen, limit, &mut active, &mut revive);
    }

    if seen.len() < limit
        && (request.retry_retired || request.retry_quarantined)
    {
        let stopped = collect_eligible_retries(
            &request.rows,
            request.retry_retired,
            request.retry_quarantined,
            &error_classes,
            true,
        )?;
        for selected in stopped {
            if seen.len() >= limit {
                break;
            }
            let key = (
                selected.global_agent.clone(),
                selected.primary_revision.clone(),
            );
            if !seen.insert(key) {
                continue;
            }
            revive.push(selected);
        }
    }

    Ok(PublicationDrainSelectionResponseWire {
        schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
        active,
        revive,
    })
}

fn collect_eligible_retries(
    rows: &[PublicationRetryRowWire],
    retry_retired: bool,
    retry_quarantined: bool,
    error_classes: &[String],
    order_for_limit: bool,
) -> Result<Vec<PublicationRetrySelectedWire>, PublicationRecoveryError> {
    let mut eligible = Vec::new();
    for row in index_rows(rows)? {
        if !is_retry_eligible(row.row, retry_retired, retry_quarantined) {
            continue;
        }
        if !error_class_allowed(&row.selected.error_class, error_classes) {
            continue;
        }
        eligible.push(row);
    }
    if order_for_limit {
        eligible.sort_by(|left, right| {
            row_sort_key(left.row, &left.global_agent, &left.primary_revision)
                .cmp(&row_sort_key(
                    right.row,
                    &right.global_agent,
                    &right.primary_revision,
                ))
        });
    }
    Ok(eligible.into_iter().map(|row| row.selected).collect())
}

fn index_rows(
    rows: &[PublicationRetryRowWire],
) -> Result<Vec<IndexedRow<'_>>, PublicationRecoveryError> {
    let mut indexed = Vec::new();
    let mut seen = BTreeSet::new();
    for row in rows {
        let global_agent = require_nonempty("global_agent", &row.global_agent)?;
        let primary_revision =
            require_nonempty("primary_revision", &row.primary_revision)?;
        let key = (global_agent.clone(), primary_revision.clone());
        if !seen.insert(key) {
            continue;
        }
        let error_class = classify_publication_error_class(
            row.terminal_reason.as_deref().or(row.last_error.as_deref()),
        );
        indexed.push(IndexedRow {
            global_agent: global_agent.clone(),
            primary_revision: primary_revision.clone(),
            row,
            selected: selected_from_row(
                global_agent,
                primary_revision,
                row,
                error_class,
            ),
        });
    }
    Ok(indexed)
}

fn error_class_allowed(error_class: &str, accepted: &[String]) -> bool {
    accepted.is_empty() || accepted.iter().any(|class| class == error_class)
}

fn row_is_drain_candidate(
    row: &IndexedRow<'_>,
    request: &PublicationDrainSelectionRequestWire,
    error_classes: &[String],
) -> bool {
    if row.selected.prior_class == PUBLICATION_LIFECYCLE_ACTIVE {
        return true;
    }
    is_retry_eligible(row.row, request.retry_retired, request.retry_quarantined)
        && error_class_allowed(&row.selected.error_class, error_classes)
}

fn push_selected(
    row: &IndexedRow<'_>,
    seen: &mut BTreeSet<(String, String)>,
    limit: usize,
    active: &mut Vec<PublicationRetrySelectedWire>,
    revive: &mut Vec<PublicationRetrySelectedWire>,
) {
    if seen.len() >= limit {
        return;
    }
    let key = (row.global_agent.clone(), row.primary_revision.clone());
    if !seen.insert(key) {
        return;
    }
    if row.selected.prior_class == PUBLICATION_LIFECYCLE_ACTIVE {
        active.push(row.selected.clone());
    } else {
        revive.push(row.selected.clone());
    }
}

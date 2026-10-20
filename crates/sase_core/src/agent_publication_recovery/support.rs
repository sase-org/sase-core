use super::wire::{
    PublicationLogicalKeyWire, PublicationRecoveryError,
    PublicationRetryRowWire, PublicationRetrySelectedWire,
    PUBLICATION_ERROR_CLASSES, PUBLICATION_LIFECYCLE_ACTIVE,
    PUBLICATION_LIFECYCLE_QUARANTINED, PUBLICATION_LIFECYCLE_RETIRED,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
};

pub(super) fn require_schema(
    schema_version: u32,
) -> Result<(), PublicationRecoveryError> {
    if schema_version == PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION {
        Ok(())
    } else {
        Err(PublicationRecoveryError::UnsupportedSchema(schema_version))
    }
}

pub(super) fn require_nonempty(
    label: &str,
    value: &str,
) -> Result<String, PublicationRecoveryError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(PublicationRecoveryError::InvalidRequest(format!(
            "{label} must not be empty"
        )))
    } else {
        Ok(trimmed.to_string())
    }
}

pub(super) fn require_positive_limit(
    limit: u32,
) -> Result<u32, PublicationRecoveryError> {
    if limit == 0 {
        Err(PublicationRecoveryError::InvalidRequest(
            "limit must be a positive integer".to_string(),
        ))
    } else {
        Ok(limit)
    }
}

pub(super) fn validate_error_classes(
    error_classes: &[String],
) -> Result<Vec<String>, PublicationRecoveryError> {
    let mut accepted = Vec::new();
    for class in error_classes {
        let normalized = class.trim();
        if !PUBLICATION_ERROR_CLASSES.contains(&normalized) {
            return Err(PublicationRecoveryError::InvalidRequest(format!(
                "unknown publication error class {class:?}"
            )));
        }
        if !accepted.iter().any(|existing| existing == normalized) {
            accepted.push(normalized.to_string());
        }
    }
    Ok(accepted)
}

pub(super) fn row_lifecycle(row: &PublicationRetryRowWire) -> &'static str {
    if row.terminal {
        PUBLICATION_LIFECYCLE_RETIRED
    } else if row.quarantined {
        PUBLICATION_LIFECYCLE_QUARANTINED
    } else {
        PUBLICATION_LIFECYCLE_ACTIVE
    }
}

pub(super) fn is_retry_eligible(
    row: &PublicationRetryRowWire,
    retry_retired: bool,
    retry_quarantined: bool,
) -> bool {
    (retry_retired && row.terminal)
        || (retry_quarantined && row.quarantined && !row.terminal)
}

pub(super) fn prior_failure(row: &PublicationRetryRowWire) -> String {
    let prior_failure = row
        .terminal_reason
        .as_deref()
        .or(row.last_error.as_deref())
        .unwrap_or("unknown reason")
        .trim();
    if prior_failure.is_empty() {
        "unknown reason".to_string()
    } else {
        prior_failure.to_string()
    }
}

pub(super) fn selected_from_row(
    global_agent: String,
    primary_revision: String,
    row: &PublicationRetryRowWire,
    error_class: &str,
) -> PublicationRetrySelectedWire {
    PublicationRetrySelectedWire {
        global_agent,
        primary_revision,
        prior_class: row_lifecycle(row).to_string(),
        prior_failure: prior_failure(row),
        error_class: error_class.to_string(),
    }
}

pub(super) fn logical_key_tuple(
    key: &PublicationLogicalKeyWire,
) -> Result<(String, String), PublicationRecoveryError> {
    Ok((
        require_nonempty("global_agent", &key.global_agent)?,
        require_nonempty("primary_revision", &key.primary_revision)?,
    ))
}

pub(super) fn lifecycle_rank(lifecycle: &str) -> u8 {
    match lifecycle {
        PUBLICATION_LIFECYCLE_RETIRED => 0,
        PUBLICATION_LIFECYCLE_QUARANTINED => 1,
        _ => 2,
    }
}

pub(super) fn row_sort_key(
    row: &PublicationRetryRowWire,
    global_agent: &str,
    primary_revision: &str,
) -> (u8, OrderedCreatedAt, String, String) {
    (
        lifecycle_rank(row_lifecycle(row)),
        OrderedCreatedAt(row.created_at),
        global_agent.to_string(),
        primary_revision.to_string(),
    )
}

pub(super) fn active_sort_key(
    row: &PublicationRetryRowWire,
    global_agent: &str,
    primary_revision: &str,
) -> (OrderedCreatedAt, String, String) {
    (
        OrderedCreatedAt(row.created_at),
        global_agent.to_string(),
        primary_revision.to_string(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct OrderedCreatedAt(pub f64);

impl Eq for OrderedCreatedAt {}

impl PartialOrd for OrderedCreatedAt {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedCreatedAt {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

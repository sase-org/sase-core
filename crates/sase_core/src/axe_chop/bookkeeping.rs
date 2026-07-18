use std::collections::BTreeSet;

use super::wire::{
    ChopCheckpointEntryWire, ChopCheckpointEventWire, ChopCheckpointPolicyWire,
    ChopCheckpointUpdateRequestWire, ChopEngineError, ChopOncePerDecisionWire,
    ChopOncePerRequestWire, ChopSeenEntryWire, CHOP_ENGINE_SCHEMA_VERSION,
    CHOP_STATE_SCHEMA_VERSION,
};

/// Apply one lifecycle observation to a runner-owned checkpoint document.
pub fn apply_checkpoint_update(
    request: &ChopCheckpointUpdateRequestWire,
) -> Result<super::wire::ChopCheckpointDocumentWire, ChopEngineError> {
    validate_checkpoint_request(request)?;
    let mut document = request.document.clone();

    match request.policy {
        ChopCheckpointPolicyWire::OnObservation => {
            if request.event == ChopCheckpointEventWire::Observed {
                commit_cursor(&mut document.entries, request);
            }
        }
        ChopCheckpointPolicyWire::OnActionAccepted => {
            if request.event == ChopCheckpointEventWire::ActionAccepted {
                commit_cursor(&mut document.entries, request);
            }
        }
        ChopCheckpointPolicyWire::OnActionSuccess => {
            apply_success_policy(&mut document.entries, request);
        }
    }
    Ok(document)
}

fn commit_cursor(
    entries: &mut std::collections::BTreeMap<String, ChopCheckpointEntryWire>,
    request: &ChopCheckpointUpdateRequestWire,
) {
    entries.insert(
        request.key.clone(),
        ChopCheckpointEntryWire {
            cursor: request.cursor.clone(),
            updated_at: request.now.clone(),
            pending_cursor: None,
            pending_at: None,
        },
    );
}

fn apply_success_policy(
    entries: &mut std::collections::BTreeMap<String, ChopCheckpointEntryWire>,
    request: &ChopCheckpointUpdateRequestWire,
) {
    match request.event {
        ChopCheckpointEventWire::Observed
        | ChopCheckpointEventWire::ActionAccepted => {
            let entry =
                entries.entry(request.key.clone()).or_insert_with(|| {
                    ChopCheckpointEntryWire {
                        cursor: String::new(),
                        updated_at: String::new(),
                        pending_cursor: None,
                        pending_at: None,
                    }
                });
            entry.pending_cursor = Some(request.cursor.clone());
            entry.pending_at = Some(request.now.clone());
        }
        ChopCheckpointEventWire::ActionSucceeded => {
            let cursor = entries
                .get_mut(&request.key)
                .and_then(|entry| entry.pending_cursor.take())
                .unwrap_or_else(|| request.cursor.clone());
            entries.insert(
                request.key.clone(),
                ChopCheckpointEntryWire {
                    cursor,
                    updated_at: request.now.clone(),
                    pending_cursor: None,
                    pending_at: None,
                },
            );
        }
        ChopCheckpointEventWire::ActionFailed => {
            let should_remove =
                entries.get_mut(&request.key).is_some_and(|entry| {
                    entry.pending_cursor = None;
                    entry.pending_at = None;
                    entry.cursor.is_empty()
                });
            if should_remove {
                entries.remove(&request.key);
            }
        }
    }
}

fn validate_checkpoint_request(
    request: &ChopCheckpointUpdateRequestWire,
) -> Result<(), ChopEngineError> {
    validate_engine_schema(request.schema_version)?;
    validate_state_schema(request.document.schema_version, "$.document")?;
    for (path, value, label) in [
        ("$.key", request.key.as_str(), "checkpoint key"),
        ("$.cursor", request.cursor.as_str(), "checkpoint cursor"),
        ("$.now", request.now.as_str(), "checkpoint timestamp"),
    ] {
        if value.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                path,
                format!("{label} must not be blank"),
            ));
        }
    }
    Ok(())
}

/// Check one event key and atomically return the bounded transformed store.
pub fn check_and_record_once_per(
    request: &ChopOncePerRequestWire,
) -> Result<ChopOncePerDecisionWire, ChopEngineError> {
    validate_engine_schema(request.schema_version)?;
    validate_state_schema(request.document.schema_version, "$.document")?;
    if request.key.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            "$.key",
            "once-per key must not be blank",
        ));
    }
    if request.now.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            "$.now",
            "once-per timestamp must not be blank",
        ));
    }
    if request.capacity == 0 {
        return Err(ChopEngineError::new(
            "non_positive_capacity",
            "$.capacity",
            "once-per store capacity must be positive",
        ));
    }
    let mut known = BTreeSet::new();
    for (index, entry) in request.document.entries.iter().enumerate() {
        if entry.key.trim().is_empty() {
            return Err(ChopEngineError::new(
                "blank_value",
                format!("$.document.entries[{index}].key"),
                "stored once-per key must not be blank",
            ));
        }
        if !known.insert(entry.key.as_str()) {
            return Err(ChopEngineError::new(
                "duplicate_seen_key",
                format!("$.document.entries[{index}].key"),
                format!("once-per key `{}` is duplicated", entry.key),
            ));
        }
    }

    if known.contains(request.key.as_str()) {
        return Ok(ChopOncePerDecisionWire {
            schema_version: CHOP_ENGINE_SCHEMA_VERSION,
            outcome: "duplicate".to_string(),
            reason: format!("once-per key `{}` was already seen", request.key),
            document: request.document.clone(),
        });
    }

    let mut document = request.document.clone();
    document.entries.push(ChopSeenEntryWire {
        key: request.key.clone(),
        seen_at: request.now.clone(),
    });
    let excess = document.entries.len().saturating_sub(request.capacity);
    if excess > 0 {
        document.entries.drain(..excess);
    }
    Ok(ChopOncePerDecisionWire {
        schema_version: CHOP_ENGINE_SCHEMA_VERSION,
        outcome: "accept".to_string(),
        reason: format!("recorded once-per key `{}`", request.key),
        document,
    })
}

fn validate_engine_schema(version: u32) -> Result<(), ChopEngineError> {
    if version != CHOP_ENGINE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!("got {version}, expected {CHOP_ENGINE_SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

fn validate_state_schema(
    version: u32,
    path: &str,
) -> Result<(), ChopEngineError> {
    if version != CHOP_STATE_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "state_schema_version_mismatch",
            format!("{path}.schema_version"),
            format!("got {version}, expected {CHOP_STATE_SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

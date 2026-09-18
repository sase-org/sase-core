//! Review-state policy for suppressing repeated machine-init prompts.

use super::wire::{
    DiscoveryCandidateWire, MachineInitReviewAssessmentRequestWire,
    MachineInitReviewAssessmentResultWire, MachineInitReviewEntryWire,
    MachineInitReviewMergeRequestWire, MachineInitReviewStateWire,
    MACHINE_SETUP_WIRE_SCHEMA_VERSION,
};
use super::MachineSetupError;

/// Decide whether onboarding should offer an explicit machine-init review.
pub fn assess_machine_init_review(
    request: &MachineInitReviewAssessmentRequestWire,
) -> Result<MachineInitReviewAssessmentResultWire, MachineSetupError> {
    MachineSetupError::check_schema(request.schema_version)?;

    let normalized_state = match &request.state {
        Some(state) => Some(normalize_state(state)?),
        None => None,
    };
    match normalized_state.as_ref() {
        Some(state) if state.initial_review_completed => {}
        _ => {
            return Ok(MachineInitReviewAssessmentResultWire {
                schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
                offer_enrollment: true,
                initial_review_required: true,
                unreviewed_candidates: Vec::new(),
                normalized_state,
            });
        }
    }

    Ok(MachineInitReviewAssessmentResultWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        offer_enrollment: false,
        initial_review_required: false,
        unreviewed_candidates: Vec::new(),
        normalized_state,
    })
}

/// Merge the candidates shown by one successful explicit review.
pub fn merge_machine_init_review(
    request: &MachineInitReviewMergeRequestWire,
) -> Result<MachineInitReviewStateWire, MachineSetupError> {
    MachineSetupError::check_schema(request.schema_version)?;

    let mut state = match &request.existing_state {
        Some(existing) => normalize_state(existing)?,
        None => empty_state(),
    };
    for candidate in &request.presented_candidates {
        merge_candidate(&mut state.reviewed, candidate);
    }
    state.initial_review_completed = true;
    normalize_state(&state)
}

fn empty_state() -> MachineInitReviewStateWire {
    MachineInitReviewStateWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        initial_review_completed: false,
        reviewed: Vec::new(),
    }
}

fn normalize_state(
    state: &MachineInitReviewStateWire,
) -> Result<MachineInitReviewStateWire, MachineSetupError> {
    if state.schema_version != MACHINE_SETUP_WIRE_SCHEMA_VERSION {
        return Err(MachineSetupError::Validation(format!(
            "unsupported machine init review schema_version {}; expected {}",
            state.schema_version, MACHINE_SETUP_WIRE_SCHEMA_VERSION
        )));
    }
    let mut normalized = MachineInitReviewStateWire {
        schema_version: MACHINE_SETUP_WIRE_SCHEMA_VERSION,
        initial_review_completed: state.initial_review_completed,
        reviewed: Vec::new(),
    };
    for entry in &state.reviewed {
        merge_entry(&mut normalized.reviewed, normalize_entry(entry));
    }
    Ok(normalized)
}

fn normalize_entry(
    entry: &MachineInitReviewEntryWire,
) -> MachineInitReviewEntryWire {
    MachineInitReviewEntryWire {
        provider_ref: entry.provider_ref.trim().to_string(),
        endpoint: entry.endpoint.trim().to_string(),
        installation_pin: entry.installation_pin.trim().to_string(),
    }
}

fn entry_is_empty(entry: &MachineInitReviewEntryWire) -> bool {
    entry.provider_ref.is_empty()
        && entry.endpoint.is_empty()
        && entry.installation_pin.is_empty()
}

fn merge_candidate(
    entries: &mut Vec<MachineInitReviewEntryWire>,
    candidate: &DiscoveryCandidateWire,
) {
    merge_entry(
        entries,
        MachineInitReviewEntryWire {
            provider_ref: candidate.provider_ref.trim().to_string(),
            endpoint: candidate.endpoint.trim().to_string(),
            installation_pin: candidate.installation_pin.trim().to_string(),
        },
    );
}

fn merge_entry(
    entries: &mut Vec<MachineInitReviewEntryWire>,
    incoming: MachineInitReviewEntryWire,
) {
    if entry_is_empty(&incoming) {
        return;
    }
    if !incoming.installation_pin.is_empty() {
        if let Some(existing) = entries
            .iter_mut()
            .find(|entry| entry.installation_pin == incoming.installation_pin)
        {
            if existing.provider_ref.is_empty() {
                existing.provider_ref = incoming.provider_ref;
            }
            if existing.endpoint.is_empty() {
                existing.endpoint = incoming.endpoint;
            }
            return;
        }
    }
    if !incoming.provider_ref.is_empty() && !incoming.endpoint.is_empty() {
        if let Some(existing) = entries.iter_mut().find(|entry| {
            entry.provider_ref == incoming.provider_ref
                && entry.endpoint == incoming.endpoint
                && (entry.installation_pin.is_empty()
                    || incoming.installation_pin.is_empty()
                    || entry.installation_pin == incoming.installation_pin)
        }) {
            if existing.installation_pin.is_empty()
                && !incoming.installation_pin.is_empty()
            {
                existing.installation_pin = incoming.installation_pin;
            }
            return;
        }
    }
    entries.push(incoming);
}

//! Standing publication-failure alerts with backoff.
//!
//! A publication failure that repeats every scheduler tick must not page
//! every tick. This module keys failures by `(project, signature)` and
//! decides, from the previous alert and this tick's observation, whether to
//! report an `opened`, `changed`, `escalated` (once after 24 h), or
//! `cleared` transition, or to stay silent (`none`).
//!
//! Only the alert key (project and signature), the failure kind, and the
//! error class decide whether a repeat is a `changed` transition. The raw
//! `detail` text can carry volatile tokens (a pid, an age, `agent@sha`, a
//! path), so a detail-only difference is not a transition: the alert keeps
//! the newest detail as its current sample without reporting.
//!
//! Project-scoped kinds back off with a capped exponential schedule (1 min
//! doubling to a 30 min cap) so the scheduler can skip the drain and the
//! doorbell until the alert is due. Row-scoped kinds take no new backoff:
//! attempt counts, quarantine, and parking already govern them.
//!
//! This module performs no filesystem, clock, or process I/O.

use super::support::require_schema;
use super::wire::{
    PublicationRecoveryError, PublicationStandingAlertWire,
    PublicationStandingDecideRequestWire,
    PublicationStandingDecideResponseWire, PublicationStandingObservationWire,
    PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
    PUBLICATION_STANDING_BACKOFF_INITIAL_SECONDS,
    PUBLICATION_STANDING_BACKOFF_MAX_SECONDS,
    PUBLICATION_STANDING_ESCALATE_AFTER_SECONDS, PUBLICATION_STANDING_KINDS,
    PUBLICATION_STANDING_ROW_SCOPED_KINDS,
    PUBLICATION_STANDING_TRANSITION_CHANGED,
    PUBLICATION_STANDING_TRANSITION_CLEARED,
    PUBLICATION_STANDING_TRANSITION_ESCALATED,
    PUBLICATION_STANDING_TRANSITION_NONE,
    PUBLICATION_STANDING_TRANSITION_OPENED,
};

fn require_finite_now(now: f64) -> Result<f64, PublicationRecoveryError> {
    if !now.is_finite() || now < 0.0 {
        return Err(PublicationRecoveryError::InvalidRequest(format!(
            "now must be a finite non-negative timestamp, got {now}"
        )));
    }
    Ok(now)
}

fn require_observation(
    observation: &PublicationStandingObservationWire,
) -> Result<(), PublicationRecoveryError> {
    if observation.project.trim().is_empty() {
        return Err(PublicationRecoveryError::InvalidRequest(
            "observation project must not be empty".to_string(),
        ));
    }
    if observation.signature.trim().is_empty() {
        return Err(PublicationRecoveryError::InvalidRequest(
            "observation signature must not be empty".to_string(),
        ));
    }
    require_kind(&observation.failure_kind)?;
    Ok(())
}

fn require_kind(kind: &str) -> Result<(), PublicationRecoveryError> {
    if !PUBLICATION_STANDING_KINDS.contains(&kind) {
        return Err(PublicationRecoveryError::InvalidRequest(format!(
            "unknown publication standing failure kind {kind:?}"
        )));
    }
    Ok(())
}

fn is_project_scoped(kind: &str) -> bool {
    !PUBLICATION_STANDING_ROW_SCOPED_KINDS.contains(&kind)
}

fn backoff_delay_seconds(attempts: u32) -> f64 {
    // attempts is already the post-increment count (>= 1): first failure
    // waits the initial interval, then doubles per repeat up to the cap.
    let shift = attempts.saturating_sub(1).min(10);
    let delay =
        PUBLICATION_STANDING_BACKOFF_INITIAL_SECONDS * 2f64.powi(shift as i32);
    delay.min(PUBLICATION_STANDING_BACKOFF_MAX_SECONDS)
}

fn next_attempt_at(now: f64, attempts: u32, project_scoped: bool) -> f64 {
    if project_scoped {
        now + backoff_delay_seconds(attempts)
    } else {
        0.0
    }
}

pub fn decide_publication_standing(
    request: &PublicationStandingDecideRequestWire,
) -> Result<PublicationStandingDecideResponseWire, PublicationRecoveryError> {
    require_schema(request.schema_version)?;
    let now = require_finite_now(request.now)?;
    if let Some(observation) = &request.observation {
        require_observation(observation)?;
    }
    if let Some(previous) = &request.previous {
        require_kind(&previous.failure_kind)?;
    }

    match (&request.previous, &request.observation) {
        (None, None) => Ok(PublicationStandingDecideResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            transition: PUBLICATION_STANDING_TRANSITION_NONE.to_string(),
            alert: None,
            project_scoped: false,
        }),
        (None, Some(observation)) => {
            let project_scoped = is_project_scoped(&observation.failure_kind);
            Ok(PublicationStandingDecideResponseWire {
                schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                transition: PUBLICATION_STANDING_TRANSITION_OPENED.to_string(),
                alert: Some(PublicationStandingAlertWire {
                    project: observation.project.clone(),
                    signature: observation.signature.clone(),
                    failure_kind: observation.failure_kind.clone(),
                    error_class: observation.error_class.clone(),
                    detail: observation.detail.clone(),
                    attempts: 1,
                    first_seen: now,
                    last_seen: now,
                    last_reported: now,
                    escalated: false,
                    next_attempt_at: next_attempt_at(now, 1, project_scoped),
                    project_scoped,
                }),
                project_scoped,
            })
        }
        (Some(previous), None) => Ok(PublicationStandingDecideResponseWire {
            schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
            transition: PUBLICATION_STANDING_TRANSITION_CLEARED.to_string(),
            alert: None,
            project_scoped: previous.project_scoped,
        }),
        (Some(previous), Some(observation)) => {
            let key_changed = observation.project != previous.project
                || observation.signature != previous.signature;
            // Raw detail is deliberately excluded: it may differ only in
            // volatile tokens while the cause is unchanged.
            let cause_changed = observation.failure_kind
                != previous.failure_kind
                || observation.error_class != previous.error_class;
            let attempts = previous.attempts.saturating_add(1);
            let project_scoped = is_project_scoped(&observation.failure_kind);
            if key_changed || cause_changed {
                Ok(PublicationStandingDecideResponseWire {
                    schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                    transition: PUBLICATION_STANDING_TRANSITION_CHANGED
                        .to_string(),
                    alert: Some(PublicationStandingAlertWire {
                        project: observation.project.clone(),
                        signature: observation.signature.clone(),
                        failure_kind: observation.failure_kind.clone(),
                        error_class: observation.error_class.clone(),
                        detail: observation.detail.clone(),
                        attempts,
                        first_seen: previous.first_seen,
                        last_seen: now,
                        last_reported: now,
                        escalated: previous.escalated,
                        next_attempt_at: next_attempt_at(
                            now,
                            attempts,
                            project_scoped,
                        ),
                        project_scoped,
                    }),
                    project_scoped,
                })
            } else if !previous.escalated
                && now - previous.first_seen
                    >= PUBLICATION_STANDING_ESCALATE_AFTER_SECONDS
            {
                Ok(PublicationStandingDecideResponseWire {
                    schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                    transition: PUBLICATION_STANDING_TRANSITION_ESCALATED
                        .to_string(),
                    alert: Some(PublicationStandingAlertWire {
                        project: previous.project.clone(),
                        signature: previous.signature.clone(),
                        failure_kind: previous.failure_kind.clone(),
                        error_class: previous.error_class.clone(),
                        detail: observation.detail.clone(),
                        attempts,
                        first_seen: previous.first_seen,
                        last_seen: now,
                        last_reported: now,
                        escalated: true,
                        next_attempt_at: next_attempt_at(
                            now,
                            attempts,
                            project_scoped,
                        ),
                        project_scoped,
                    }),
                    project_scoped,
                })
            } else {
                Ok(PublicationStandingDecideResponseWire {
                    schema_version: PUBLICATION_RECOVERY_WIRE_SCHEMA_VERSION,
                    transition: PUBLICATION_STANDING_TRANSITION_NONE
                        .to_string(),
                    alert: Some(PublicationStandingAlertWire {
                        project: previous.project.clone(),
                        signature: previous.signature.clone(),
                        failure_kind: previous.failure_kind.clone(),
                        error_class: previous.error_class.clone(),
                        detail: observation.detail.clone(),
                        attempts,
                        first_seen: previous.first_seen,
                        last_seen: now,
                        last_reported: previous.last_reported,
                        escalated: previous.escalated,
                        next_attempt_at: next_attempt_at(
                            now,
                            attempts,
                            project_scoped,
                        ),
                        project_scoped,
                    }),
                    project_scoped,
                })
            }
        }
    }
}

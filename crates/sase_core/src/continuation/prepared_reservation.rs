//! Prepared-intent reservation ownership: which stage may bind, hold,
//! or release one sealed intent.
//!
//! The sealed intent stays in `prepared` lifecycle state while a
//! host-owned reservation associates it with one staged monitor. Python
//! owns the file/lock operations and persists ownership separately from
//! the sealed intent under the existing store lock, keyed by intent and
//! stage identity; Rust owns identity and transition legality here.
//!
//! The rules are absolute: matching adoption may bind, another stage or an
//! immediate start cannot steal the intent, and matching cleanup may
//! release it. Release is idempotent so discarded stages clean up safely.

use serde::{Deserialize, Serialize};

use super::prepared_coverage::{validate_uuid, PreparedReservationWire};
use super::schema::{
    validate_command_part, validate_identifier, validate_non_empty_text,
    validate_schema, ContinuationError, CONTINUATION_WIRE_SCHEMA_VERSION,
    MAX_COMMAND_PARTS, MAX_REF_BYTES,
};

/// Reservation access actions the decision supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparedReservationActionWire {
    Reserve,
    Bind,
    Release,
}

/// One durable prepared-intent ownership record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredPreparedReservationWire {
    pub intent_ref: String,
    pub reservation: PreparedReservationWire,
}

/// Request to check reservation access for one intent and stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedReservationAccessRequestWire {
    pub schema_version: u32,
    pub intent_ref: String,
    pub staged_id: String,
    pub monitor_id: String,
    pub action: PreparedReservationActionWire,
    #[serde(default)]
    pub existing: Vec<StoredPreparedReservationWire>,
}

/// Whether the requested reservation access is allowed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedReservationAccessDecisionWire {
    pub schema_version: u32,
    pub allowed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Decide whether a stage may reserve, bind, or release a prepared intent.
pub fn evaluate_prepared_reservation_access(
    request: PreparedReservationAccessRequestWire,
) -> Result<PreparedReservationAccessDecisionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "PreparedReservationAccessRequestWire",
    )?;
    validate_non_empty_text(&request.intent_ref, "intent_ref", MAX_REF_BYTES)?;
    validate_uuid(&request.staged_id, "staged_id")?;
    validate_identifier(&request.monitor_id, "monitor_id")?;
    for stored in &request.existing {
        validate_non_empty_text(
            &stored.intent_ref,
            "existing.intent_ref",
            MAX_REF_BYTES,
        )?;
        validate_reservation(&stored.reservation, "existing.reservation")?;
    }
    let incumbent = request
        .existing
        .iter()
        .find(|stored| stored.intent_ref == request.intent_ref);
    let decision = |allowed: bool, reason: Option<&str>| {
        PreparedReservationAccessDecisionWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            allowed,
            reason: reason.map(str::to_string),
        }
    };
    match request.action {
        PreparedReservationActionWire::Reserve => match incumbent {
            // Matching replay: the same stage re-reserving its intent.
            Some(stored)
                if stored.reservation.staged_id == request.staged_id
                    && stored.reservation.monitor_id == request.monitor_id =>
            {
                Ok(decision(true, Some("already_reserved_by_stage")))
            }
            // Another stage holds this intent: it cannot be stolen.
            Some(_) => Ok(decision(false, Some("reserved_by_another_stage"))),
            // No incumbent: reserving this intent is always allowed.
            // Reservations are keyed by intent, so records for other
            // intents never block or collide.
            None => Ok(decision(true, None)),
        },
        PreparedReservationActionWire::Bind => match incumbent {
            Some(stored)
                if stored.reservation.staged_id == request.staged_id =>
            {
                if stored.reservation.monitor_id != request.monitor_id {
                    Ok(decision(false, Some("reservation_monitor_mismatch")))
                } else {
                    Ok(decision(true, None))
                }
            }
            // No reservation, or another stage's: an immediate start or a
            // different stage cannot steal the intent.
            _ => Ok(decision(false, Some("no_reservation_for_intent"))),
        },
        PreparedReservationActionWire::Release => match incumbent {
            Some(stored)
                if stored.reservation.staged_id == request.staged_id =>
            {
                Ok(decision(true, None))
            }
            Some(_) => Ok(decision(false, Some("reserved_by_another_stage"))),
            // Idempotent cleanup: releasing what is not reserved succeeds.
            None => Ok(decision(true, Some("nothing_to_release"))),
        },
    }
}

fn validate_reservation(
    reservation: &PreparedReservationWire,
    field: &str,
) -> Result<(), ContinuationError> {
    validate_uuid(&reservation.staged_id, &format!("{field}.staged_id"))?;
    validate_identifier(
        &reservation.monitor_id,
        &format!("{field}.monitor_id"),
    )?;
    validate_identifier(
        &reservation.request_fingerprint,
        &format!("{field}.request_fingerprint"),
    )?;
    validate_command(&reservation.command, &format!("{field}.command"))?;
    Ok(())
}

fn validate_command(
    command: &[String],
    field: &str,
) -> Result<(), ContinuationError> {
    if command.is_empty() {
        return Err(ContinuationError::validation(format!(
            "{field} must contain at least one argv part"
        )));
    }
    if command.len() > MAX_COMMAND_PARTS {
        return Err(ContinuationError::validation(format!(
            "{field} has {} parts; maximum is {MAX_COMMAND_PARTS}",
            command.len()
        )));
    }
    for (index, part) in command.iter().enumerate() {
        validate_command_part(part, &format!("{field}[{index}]"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const STAGED_A: &str = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
    const STAGED_B: &str = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff";

    fn reservation(
        staged_id: &str,
        monitor_id: &str,
    ) -> PreparedReservationWire {
        PreparedReservationWire {
            staged_id: staged_id.to_string(),
            monitor_id: monitor_id.to_string(),
            request_fingerprint: "fp-1".to_string(),
            command: vec!["just".to_string(), "check".to_string()],
        }
    }

    fn stored(
        intent_ref: &str,
        staged_id: &str,
        monitor_id: &str,
    ) -> StoredPreparedReservationWire {
        StoredPreparedReservationWire {
            intent_ref: intent_ref.to_string(),
            reservation: reservation(staged_id, monitor_id),
        }
    }

    fn request(
        intent_ref: &str,
        staged_id: &str,
        monitor_id: &str,
        action: PreparedReservationActionWire,
        existing: Vec<StoredPreparedReservationWire>,
    ) -> PreparedReservationAccessRequestWire {
        PreparedReservationAccessRequestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            intent_ref: intent_ref.to_string(),
            staged_id: staged_id.to_string(),
            monitor_id: monitor_id.to_string(),
            action,
            existing,
        }
    }

    #[test]
    fn reserve_bind_release_round_trip_for_matching_stage() {
        let existing = Vec::new();
        let reserve = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Reserve,
            existing,
        ))
        .expect("reserve");
        assert!(reserve.allowed);

        let existing = vec![stored("intent-1", STAGED_A, "monitor-1")];
        let replay = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Reserve,
            existing.clone(),
        ))
        .expect("reserve replay");
        assert!(replay.allowed);
        assert_eq!(replay.reason.as_deref(), Some("already_reserved_by_stage"));

        let bind = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Bind,
            existing.clone(),
        ))
        .expect("bind");
        assert!(bind.allowed);

        let release = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Release,
            existing,
        ))
        .expect("release");
        assert!(release.allowed);
    }

    #[test]
    fn another_stage_cannot_steal_or_release() {
        let existing = vec![stored("intent-1", STAGED_A, "monitor-1")];
        let steal = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_B,
            "monitor-9",
            PreparedReservationActionWire::Reserve,
            existing.clone(),
        ))
        .expect("steal reserve");
        assert!(!steal.allowed);
        assert_eq!(steal.reason.as_deref(), Some("reserved_by_another_stage"));

        let bind = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_B,
            "monitor-9",
            PreparedReservationActionWire::Bind,
            existing.clone(),
        ))
        .expect("steal bind");
        assert!(!bind.allowed);

        let release = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_B,
            "monitor-9",
            PreparedReservationActionWire::Release,
            existing,
        ))
        .expect("steal release");
        assert!(!release.allowed);
        assert_eq!(
            release.reason.as_deref(),
            Some("reserved_by_another_stage")
        );
    }

    #[test]
    fn immediate_start_cannot_bind_without_reservation() {
        let bind = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Bind,
            Vec::new(),
        ))
        .expect("bind");
        assert!(!bind.allowed);
        assert_eq!(bind.reason.as_deref(), Some("no_reservation_for_intent"));
    }

    #[test]
    fn bind_refuses_monitor_mismatch() {
        let existing = vec![stored("intent-1", STAGED_A, "monitor-1")];
        let bind = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-2",
            PreparedReservationActionWire::Bind,
            existing,
        ))
        .expect("bind");
        assert!(!bind.allowed);
        assert_eq!(
            bind.reason.as_deref(),
            Some("reservation_monitor_mismatch")
        );
    }

    #[test]
    fn release_without_reservation_is_idempotent() {
        let release = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Release,
            Vec::new(),
        ))
        .expect("release");
        assert!(release.allowed);
        assert_eq!(release.reason.as_deref(), Some("nothing_to_release"));
    }

    #[test]
    fn other_intents_never_block_or_collide() {
        let existing = vec![stored("intent-9", STAGED_B, "monitor-9")];
        let reserve = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Reserve,
            existing.clone(),
        ))
        .expect("reserve");
        assert!(reserve.allowed);
        assert!(reserve.reason.is_none());

        let bind = evaluate_prepared_reservation_access(request(
            "intent-1",
            STAGED_A,
            "monitor-1",
            PreparedReservationActionWire::Bind,
            existing,
        ))
        .expect("bind");
        assert!(!bind.allowed);
    }
}

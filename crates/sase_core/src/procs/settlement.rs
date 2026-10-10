//! Freeze and select a named-proc command observation during settlement.
//!
//! The store lock that moves a proc into `settling` also persists the first
//! authoritative command result. Recovery may finish bookkeeping, but it
//! cannot replace that observation with a supervisor-loss or reboot fallback.

use chrono::{DateTime, FixedOffset};
use serde_json::Value;

use super::store::ProcStoreError;
use super::wire::{
    ProcObservedSettlementOutcomeWire, ProcSettlementWire, ProcWire,
};

const FALLBACK_REASONS: [&str; 2] = ["supervisor-loss", "reboot"];
const TERMINAL_STATUSES: [&str; 3] = ["success", "error", "killed"];

type ProcStoreResult<T> = Result<T, ProcStoreError>;

/// Apply begin-settlement mutation after ownership and lifecycle checks.
pub(super) fn apply_begin_settlement(
    proc: &mut ProcWire,
    settlement: &ProcSettlementWire,
) -> ProcStoreResult<()> {
    let selected = select_settlement_outcome(proc, settlement)?;
    let already_settling = proc.status == "settling";
    if !already_settling {
        proc.status = "settling".to_string();
        proc.settling_started_at = Some(settlement.settling_at.clone());
    }
    if let Some(outcome) = selected {
        apply_frozen_outcome(proc, &outcome);
    } else if !already_settling {
        if settlement.exit_code.is_some() {
            proc.exit_code = settlement.exit_code;
        }
        if settlement.message.is_some() {
            proc.message.clone_from(&settlement.message);
        }
    }
    Ok(())
}

/// Choose the frozen observation for this begin call.
///
/// Precedence: an existing store field, a valid sidecar adoption, then the
/// request's outcome. Supervisor-loss/reboot fallbacks never replace a
/// frozen known result. A second, different authoritative observation is
/// rejected.
pub fn select_settlement_outcome(
    proc: &ProcWire,
    settlement: &ProcSettlementWire,
) -> ProcStoreResult<Option<ProcObservedSettlementOutcomeWire>> {
    let requested = match settlement.outcome.as_ref() {
        Some(outcome) => Some(
            normalize_outcome(outcome, fallback_ended_at(outcome, settlement))
                .map_err(|error| attach_proc_id(error, &proc.proc_id))?,
        ),
        None => None,
    };
    let sidecar = adopt_sidecar_outcome(proc, settlement)?;

    if let Some(frozen) = proc.settlement_outcome.as_ref() {
        let frozen =
            normalize_outcome(frozen, frozen.command_ended_at.clone())?;
        reject_conflicting_observation(proc, &frozen, requested.as_ref())?;
        reject_conflicting_observation(proc, &frozen, sidecar.as_ref())?;
        return Ok(Some(frozen));
    }

    match (sidecar, requested) {
        (Some(sidecar), Some(requested)) => {
            if is_fallback_reason(&requested.termination_reason)
                || observations_match(&sidecar, &requested)
            {
                Ok(Some(sidecar))
            } else if is_fallback_reason(&sidecar.termination_reason) {
                Ok(Some(requested))
            } else {
                Err(conflict(proc, &sidecar, &requested))
            }
        }
        (Some(sidecar), None) => Ok(Some(sidecar)),
        (None, Some(requested)) => Ok(Some(requested)),
        (None, None) => Ok(None),
    }
}

fn apply_frozen_outcome(
    proc: &mut ProcWire,
    outcome: &ProcObservedSettlementOutcomeWire,
) {
    proc.settlement_outcome = Some(outcome.clone());
    proc.exit_code = outcome.exit_code;
    if outcome.message.is_some() {
        proc.message.clone_from(&outcome.message);
    }
}

fn adopt_sidecar_outcome(
    proc: &ProcWire,
    settlement: &ProcSettlementWire,
) -> ProcStoreResult<Option<ProcObservedSettlementOutcomeWire>> {
    let Some(sidecar) = settlement.sidecar.as_ref() else {
        return Ok(None);
    };
    Ok(parse_sidecar_outcome(
        proc,
        sidecar,
        &settlement.settling_at,
    ))
}

fn parse_sidecar_outcome(
    proc: &ProcWire,
    sidecar: &Value,
    settling_at: &str,
) -> Option<ProcObservedSettlementOutcomeWire> {
    let object = sidecar.as_object()?;
    let proc_id = object.get("proc_id")?.as_str()?;
    if proc_id != proc.proc_id {
        return None;
    }
    let supervisor_id = object.get("supervisor_id")?.as_str()?;
    let expected = proc.supervisor_id.as_deref()?;
    if supervisor_id != expected {
        return None;
    }
    let status = object.get("status")?.as_str()?.trim();
    let termination_reason = object.get("termination_reason")?.as_str()?.trim();
    if status.is_empty() || termination_reason.is_empty() {
        return None;
    }
    if is_fallback_reason(termination_reason) {
        return None;
    }
    let exit_code = match object.get("exit_code") {
        None | Some(Value::Null) => None,
        Some(value) => Some(i32::try_from(value.as_i64()?).ok()?),
    };
    let message = match object.get("message") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_str()?.to_string()),
    };
    let command_ended_at = object
        .get("command_ended_at")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or(proc.settling_started_at.as_deref())
        .unwrap_or(settling_at)
        .to_string();
    normalize_outcome(
        &ProcObservedSettlementOutcomeWire {
            status: status.to_string(),
            termination_reason: termination_reason.to_string(),
            exit_code,
            message,
            command_ended_at,
        },
        settling_at.to_string(),
    )
    .ok()
}

fn normalize_outcome(
    outcome: &ProcObservedSettlementOutcomeWire,
    fallback_ended_at: String,
) -> ProcStoreResult<ProcObservedSettlementOutcomeWire> {
    let status = outcome.status.trim();
    if !TERMINAL_STATUSES.contains(&status) {
        return Err(ProcStoreError::InvalidProc {
            proc_id: String::new(),
            reason: format!(
                "settlement outcome status must be terminal, got {status:?}"
            ),
        });
    }
    let termination_reason = outcome.termination_reason.trim();
    if termination_reason.is_empty() {
        return Err(ProcStoreError::InvalidProc {
            proc_id: String::new(),
            reason: "settlement outcome termination_reason must not be empty"
                .to_string(),
        });
    }
    let command_ended_at = if outcome.command_ended_at.trim().is_empty() {
        fallback_ended_at
    } else {
        outcome.command_ended_at.trim().to_string()
    };
    validate_command_ended_at(&command_ended_at).map_err(|reason| {
        ProcStoreError::InvalidProc {
            proc_id: String::new(),
            reason,
        }
    })?;
    let message = outcome.message.as_ref().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    Ok(ProcObservedSettlementOutcomeWire {
        status: status.to_string(),
        termination_reason: termination_reason.to_string(),
        exit_code: outcome.exit_code,
        message,
        command_ended_at,
    })
}

fn fallback_ended_at(
    outcome: &ProcObservedSettlementOutcomeWire,
    settlement: &ProcSettlementWire,
) -> String {
    if outcome.command_ended_at.trim().is_empty() {
        settlement.settling_at.clone()
    } else {
        outcome.command_ended_at.clone()
    }
}

fn reject_conflicting_observation(
    proc: &ProcWire,
    frozen: &ProcObservedSettlementOutcomeWire,
    incoming: Option<&ProcObservedSettlementOutcomeWire>,
) -> ProcStoreResult<()> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if is_fallback_reason(&incoming.termination_reason)
        || observations_match(frozen, incoming)
    {
        return Ok(());
    }
    Err(conflict(proc, frozen, incoming))
}

fn observations_match(
    left: &ProcObservedSettlementOutcomeWire,
    right: &ProcObservedSettlementOutcomeWire,
) -> bool {
    left.status == right.status
        && left.termination_reason == right.termination_reason
        && left.exit_code == right.exit_code
}

fn is_fallback_reason(reason: &str) -> bool {
    FALLBACK_REASONS.contains(&reason)
}

fn conflict(
    proc: &ProcWire,
    frozen: &ProcObservedSettlementOutcomeWire,
    incoming: &ProcObservedSettlementOutcomeWire,
) -> ProcStoreError {
    ProcStoreError::InvalidProc {
        proc_id: proc.proc_id.clone(),
        reason: format!(
            "inconsistent settlement observation: frozen {}/{}/{:?} cannot be replaced by {}/{}/{:?}",
            frozen.status,
            frozen.termination_reason,
            frozen.exit_code,
            incoming.status,
            incoming.termination_reason,
            incoming.exit_code
        ),
    }
}

fn validate_command_ended_at(value: &str) -> Result<(), String> {
    let timestamp = DateTime::parse_from_rfc3339(value).map_err(|error| {
        format!("command_ended_at must be an RFC3339 timestamp: {error}")
    })?;
    if timestamp.offset().local_minus_utc() != 0 {
        return Err("command_ended_at must use a UTC offset".to_string());
    }
    let _unused: DateTime<FixedOffset> = timestamp;
    Ok(())
}

fn attach_proc_id(error: ProcStoreError, proc_id: &str) -> ProcStoreError {
    match error {
        ProcStoreError::InvalidProc {
            proc_id: existing,
            reason,
        } if existing.is_empty() => ProcStoreError::InvalidProc {
            proc_id: proc_id.to_string(),
            reason,
        },
        other => other,
    }
}

/// Validate a standalone outcome and rewrite empty-proc-id errors.
pub(super) fn validate_persisted_outcome(
    proc: &ProcWire,
    outcome: &ProcObservedSettlementOutcomeWire,
) -> ProcStoreResult<()> {
    normalize_outcome(outcome, outcome.command_ended_at.clone())
        .map(|_| ())
        .map_err(|error| attach_proc_id(error, &proc.proc_id))
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::*;
    use crate::procs::store::{
        begin_proc_settlement, claim_proc_supervisor, finish_proc, reserve_proc,
    };
    use crate::procs::wire::{
        ProcFinishWire, ProcReserveWire, ProcSupervisorClaimWire,
        PROC_WIRE_SCHEMA_VERSION,
    };

    fn reserve_request(proc_id: &str) -> ProcReserveWire {
        ProcReserveWire {
            schema_version: PROC_WIRE_SCHEMA_VERSION,
            proc_id: proc_id.to_string(),
            label: format!("Proc {proc_id}"),
            kind: "detached".to_string(),
            argv: vec!["true".to_string()],
            cwd: "/tmp".to_string(),
            project: Some("sase".to_string()),
            workspace_num: Some(1),
            session_id: None,
            session_label: None,
            origin: "named-proc".to_string(),
            cl_name: None,
            tags: Vec::new(),
            created_at: "2026-10-10T12:00:00Z".to_string(),
            log_path: format!("/tmp/{proc_id}.log"),
            log_owner: "proc-store".to_string(),
            proc_name: Some(proc_id.to_string()),
            proc_role: Some("proc".to_string()),
            concurrency_keys: Vec::new(),
            request_fingerprint: format!("fp-{proc_id}"),
            reserved_by: "agent-one".to_string(),
            timeout_seconds: None,
            idle_timeout_seconds: None,
            prompt_proc: None,
            service: None,
        }
    }

    fn claimed_store(proc_id: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let temp = tempdir().unwrap();
        let path = temp.path().join("procs.jsonl");
        reserve_proc(&path, &reserve_request(proc_id), 10).unwrap();
        claim_proc_supervisor(
            &path,
            &ProcSupervisorClaimWire {
                proc_id: proc_id.to_string(),
                supervisor_id: "supervisor-a".to_string(),
                claimed_at: "2026-10-10T12:00:01Z".to_string(),
                pid: Some(11),
                pgid: Some(11),
            },
        )
        .unwrap();
        (temp, path)
    }

    fn success_outcome() -> ProcObservedSettlementOutcomeWire {
        ProcObservedSettlementOutcomeWire {
            status: "success".to_string(),
            termination_reason: "success".to_string(),
            exit_code: Some(0),
            message: Some("completed successfully".to_string()),
            command_ended_at: "2026-10-10T12:12:19Z".to_string(),
        }
    }

    fn loss_outcome() -> ProcObservedSettlementOutcomeWire {
        ProcObservedSettlementOutcomeWire {
            status: "error".to_string(),
            termination_reason: "supervisor-loss".to_string(),
            exit_code: Some(0),
            message: Some("supervisor exited without reporting".to_string()),
            command_ended_at: "2026-10-10T12:12:30Z".to_string(),
        }
    }

    fn begin(
        path: &std::path::Path,
        proc_id: &str,
        outcome: Option<ProcObservedSettlementOutcomeWire>,
        sidecar: Option<Value>,
        settling_at: &str,
    ) -> ProcWire {
        begin_proc_settlement(
            path,
            &ProcSettlementWire {
                proc_id: proc_id.to_string(),
                supervisor_id: "supervisor-a".to_string(),
                settling_at: settling_at.to_string(),
                exit_code: outcome.as_ref().and_then(|item| item.exit_code),
                message: outcome.as_ref().and_then(|item| item.message.clone()),
                outcome,
                sidecar,
            },
        )
        .unwrap()
        .proc
        .unwrap()
    }

    #[test]
    fn first_observation_freezes_atomically_with_settling_transition() {
        let (_temp, path) = claimed_store("obs-one");
        let settling = begin(
            &path,
            "obs-one",
            Some(success_outcome()),
            None,
            "2026-10-10T12:12:19Z",
        );
        assert_eq!(settling.status, "settling");
        let frozen = settling.settlement_outcome.expect("frozen");
        assert_eq!(frozen.status, "success");
        assert_eq!(frozen.termination_reason, "success");
        assert_eq!(frozen.exit_code, Some(0));
        assert_eq!(frozen.command_ended_at, "2026-10-10T12:12:19Z");
        assert_eq!(
            settling.settling_started_at.as_deref(),
            Some("2026-10-10T12:12:19Z")
        );
        assert_eq!(settling.exit_code, Some(0));
    }

    #[test]
    fn identical_replay_keeps_original_timestamp() {
        let (_temp, path) = claimed_store("obs-replay");
        begin(
            &path,
            "obs-replay",
            Some(success_outcome()),
            None,
            "2026-10-10T12:12:19Z",
        );
        let mut replay_outcome = success_outcome();
        replay_outcome.command_ended_at = "2026-10-10T12:12:30Z".to_string();
        let replay = begin(
            &path,
            "obs-replay",
            Some(replay_outcome),
            None,
            "2026-10-10T12:12:30Z",
        );
        let frozen = replay.settlement_outcome.expect("frozen");
        assert_eq!(frozen.command_ended_at, "2026-10-10T12:12:19Z");
        assert_eq!(
            replay.settling_started_at.as_deref(),
            Some("2026-10-10T12:12:19Z")
        );
    }

    #[test]
    fn supervisor_loss_fallback_cannot_replace_frozen_success() {
        let (_temp, path) = claimed_store("obs-loss");
        begin(
            &path,
            "obs-loss",
            Some(success_outcome()),
            None,
            "2026-10-10T12:12:19Z",
        );
        let replay = begin(
            &path,
            "obs-loss",
            Some(loss_outcome()),
            None,
            "2026-10-10T12:12:30Z",
        );
        let frozen = replay.settlement_outcome.expect("frozen");
        assert_eq!(frozen.status, "success");
        assert_eq!(frozen.termination_reason, "success");
        assert_eq!(frozen.command_ended_at, "2026-10-10T12:12:19Z");
        assert_eq!(frozen.exit_code, Some(0));
    }

    #[test]
    fn conflicting_authoritative_observation_is_rejected() {
        let (_temp, path) = claimed_store("obs-conflict");
        begin(
            &path,
            "obs-conflict",
            Some(success_outcome()),
            None,
            "2026-10-10T12:12:19Z",
        );
        let error = begin_proc_settlement(
            &path,
            &ProcSettlementWire {
                proc_id: "obs-conflict".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                settling_at: "2026-10-10T12:12:30Z".to_string(),
                exit_code: Some(1),
                message: Some("exited with code 1".to_string()),
                outcome: Some(ProcObservedSettlementOutcomeWire {
                    status: "error".to_string(),
                    termination_reason: "error".to_string(),
                    exit_code: Some(1),
                    message: Some("exited with code 1".to_string()),
                    command_ended_at: "2026-10-10T12:12:30Z".to_string(),
                }),
                sidecar: None,
            },
        )
        .unwrap_err();
        let ProcStoreError::InvalidProc { reason, .. } = error else {
            panic!("expected invalid proc, got {error:?}");
        };
        assert!(reason.contains("inconsistent settlement observation"));
        assert!(reason.contains("success/success"));
        assert!(reason.contains("error/error"));
    }

    #[test]
    fn legacy_sidecar_is_adopted_once() {
        let (_temp, path) = claimed_store("obs-sidecar");
        begin(&path, "obs-sidecar", None, None, "2026-10-10T12:12:19Z");
        let sidecar = json!({
            "proc_id": "obs-sidecar",
            "supervisor_id": "supervisor-a",
            "status": "success",
            "termination_reason": "success",
            "exit_code": 0,
            "message": "completed successfully",
        });
        let adopted = begin(
            &path,
            "obs-sidecar",
            Some(loss_outcome()),
            Some(sidecar),
            "2026-10-10T12:12:30Z",
        );
        let frozen = adopted.settlement_outcome.expect("adopted");
        assert_eq!(frozen.status, "success");
        assert_eq!(frozen.termination_reason, "success");
        assert_eq!(frozen.exit_code, Some(0));
        assert_eq!(frozen.command_ended_at, "2026-10-10T12:12:19Z");
    }

    #[test]
    fn mismatched_or_corrupt_sidecar_keeps_loss_fallback() {
        for (proc_id, sidecar) in [
            (
                "obs-mismatch",
                json!({
                    "proc_id": "other-proc",
                    "supervisor_id": "supervisor-a",
                    "status": "success",
                    "termination_reason": "success",
                    "exit_code": 0,
                }),
            ),
            ("obs-corrupt", json!("not-an-object")),
        ] {
            let (_temp, path) = claimed_store(proc_id);
            let settling = begin(
                &path,
                proc_id,
                Some(loss_outcome()),
                Some(sidecar),
                "2026-10-10T12:12:30Z",
            );
            let frozen = settling.settlement_outcome.expect("loss frozen");
            assert_eq!(frozen.termination_reason, "supervisor-loss");
            assert_eq!(settling.status, "settling");
        }
    }

    #[test]
    fn bare_store_exit_code_is_not_an_observation() {
        let (_temp, path) = claimed_store("obs-bare");
        let settling = begin_proc_settlement(
            &path,
            &ProcSettlementWire {
                proc_id: "obs-bare".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                settling_at: "2026-10-10T12:12:19Z".to_string(),
                exit_code: Some(0),
                message: Some("done".to_string()),
                outcome: None,
                sidecar: None,
            },
        )
        .unwrap()
        .proc
        .unwrap();
        assert_eq!(settling.status, "settling");
        assert_eq!(settling.exit_code, Some(0));
        assert!(settling.settlement_outcome.is_none());
    }

    #[test]
    fn wrong_supervisor_cannot_begin_settlement() {
        let (_temp, path) = claimed_store("obs-owner");
        let error = begin_proc_settlement(
            &path,
            &ProcSettlementWire {
                proc_id: "obs-owner".to_string(),
                supervisor_id: "supervisor-b".to_string(),
                settling_at: "2026-10-10T12:12:19Z".to_string(),
                exit_code: Some(0),
                message: None,
                outcome: Some(success_outcome()),
                sidecar: None,
            },
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProcStoreError::Conflict { ref field, .. } if field == "supervisor_id"
        ));
    }

    #[test]
    fn terminal_rows_are_not_converted_by_sidecar_adoption() {
        let (_temp, path) = claimed_store("obs-terminal");
        begin(
            &path,
            "obs-terminal",
            Some(success_outcome()),
            None,
            "2026-10-10T12:12:19Z",
        );
        finish_proc(
            &path,
            &ProcFinishWire {
                proc_id: "obs-terminal".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                status: "success".to_string(),
                finished_at: "2026-10-10T12:12:40Z".to_string(),
                exit_code: Some(0),
                message: Some("completed successfully".to_string()),
                result: None,
            },
        )
        .unwrap();
        let error = begin_proc_settlement(
            &path,
            &ProcSettlementWire {
                proc_id: "obs-terminal".to_string(),
                supervisor_id: "supervisor-a".to_string(),
                settling_at: "2026-10-10T12:12:50Z".to_string(),
                exit_code: Some(0),
                message: None,
                outcome: Some(success_outcome()),
                sidecar: Some(json!({
                    "proc_id": "obs-terminal",
                    "supervisor_id": "supervisor-a",
                    "status": "success",
                    "termination_reason": "success",
                    "exit_code": 0,
                })),
            },
        )
        .unwrap_err();
        assert!(matches!(error, ProcStoreError::InvalidProc { .. }));
    }

    #[test]
    fn legacy_request_without_outcome_still_deserializes() {
        let parsed: ProcSettlementWire = serde_json::from_value(json!({
            "proc_id": "legacy",
            "supervisor_id": "supervisor-a",
            "settling_at": "2026-10-10T12:12:19Z",
            "exit_code": 0,
            "message": "done"
        }))
        .unwrap();
        assert!(parsed.outcome.is_none());
        assert!(parsed.sidecar.is_none());
        assert_eq!(parsed.exit_code, Some(0));
    }
}

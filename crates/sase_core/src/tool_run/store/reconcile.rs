//! Owner-aware reconciliation: foreground keeps its lost path, hand-off
//! follows the epic's reconcile table through a pure decision function.

use super::super::handoff_wire::{
    ToolRunOwnerStateWire, ToolRunReconcileSettlementWire,
    ToolRunSettledByWire, ToolRunTerminalCauseWire,
};
use super::super::wire::{
    ToolLivenessObservationWire, ToolRunEventKindWire, ToolRunEventWire,
    ToolRunLivenessFactWire, ToolRunReapCandidateWire,
    ToolRunReconcileRequestWire, ToolRunReconcileResultWire, ToolRunStateWire,
    ToolRunWire, TOOL_RUN_LOST_REASON_RUNNER_EXITED,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    touch_write_meta, unix_now, validate_schema, with_write_store,
};
use super::lifecycle::{ingest_event, new_hex_id, record_settlement};
use super::query::load_run;
use rusqlite::TransactionBehavior;
use std::path::Path;
use std::time::Duration;

enum Decision {
    NoTransition { diagnostic: Option<String> },
    Settle(Settlement),
}

struct Settlement {
    state: ToolRunStateWire,
    terminal_cause: &'static str,
    settled_by: &'static str,
    exit_code: Option<i32>,
    signal: Option<i32>,
    reason: Option<String>,
    diagnostic: Option<String>,
}

pub fn reconcile(
    store_path: &Path,
    request: ToolRunReconcileRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunReconcileResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    if !store_path.exists() {
        return Ok(ToolRunReconcileResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            marked_lost: Vec::new(),
            persisted: false,
            reap_candidates: Vec::new(),
            settled: Vec::new(),
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    match with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut marked = Vec::new();
        let mut reap_candidates = Vec::new();
        let mut settled = Vec::new();
        let mut diagnostics = Vec::new();
        for fact in &request.facts {
            let Some(run) = load_run(&tx, &fact.run_id)? else {
                continue;
            };
            if !run.state.is_unsettled() {
                continue;
            }
            let is_handoff = run.launch_mode.as_deref() == Some("handoff");
            if !is_handoff {
                if let Some(outcome) =
                    reconcile_foreground(&tx, &run, fact, now)?
                {
                    marked.push(outcome.run_id.clone());
                    if let Some(candidate) = outcome.reap {
                        reap_candidates.push(candidate);
                    }
                    settled.push(outcome.settlement);
                } else if matches!(
                    fact.observation,
                    ToolLivenessObservationWire::Unknown
                ) {
                    diagnostics.push(format!(
                        "liveness for {} is unknown; not proof of death",
                        fact.run_id
                    ));
                }
                continue;
            }
            match decide_handoff(&run, fact) {
                Decision::NoTransition { diagnostic } => {
                    if let Some(message) = diagnostic {
                        diagnostics.push(message);
                    }
                }
                Decision::Settle(settlement) => {
                    let event_kind = match settlement.state {
                        ToolRunStateWire::Succeeded => {
                            ToolRunEventKindWire::Succeeded
                        }
                        ToolRunStateWire::Failed => {
                            ToolRunEventKindWire::Failed
                        }
                        ToolRunStateWire::Signaled => {
                            ToolRunEventKindWire::Signaled
                        }
                        ToolRunStateWire::Interrupted => {
                            ToolRunEventKindWire::Interrupted
                        }
                        ToolRunStateWire::Lost => ToolRunEventKindWire::Lost,
                        _ => {
                            return Err(ToolRunError::invalid(
                                "reconcile settled an unsettled state",
                            ));
                        }
                    };
                    let cause = match settlement.terminal_cause {
                        "exited" => Some(ToolRunTerminalCauseWire::Exited),
                        "signal" => Some(ToolRunTerminalCauseWire::Signal),
                        "interrupt" => {
                            Some(ToolRunTerminalCauseWire::Interrupt)
                        }
                        "stop_requested" => {
                            Some(ToolRunTerminalCauseWire::StopRequested)
                        }
                        "timeout" => Some(ToolRunTerminalCauseWire::Timeout),
                        "launch_failed" => {
                            Some(ToolRunTerminalCauseWire::LaunchFailed)
                        }
                        "owner_lost" => {
                            Some(ToolRunTerminalCauseWire::OwnerLost)
                        }
                        "wrapper_lost" => {
                            Some(ToolRunTerminalCauseWire::WrapperLost)
                        }
                        _ => None,
                    };
                    let event = ToolRunEventWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        event_id: new_hex_id(),
                        run_id: run.run_id.clone(),
                        attempt: 1,
                        kind: event_kind,
                        created_ts: now,
                        stage: None,
                        sample: None,
                        exit_code: settlement.exit_code,
                        signal: settlement.signal,
                        reason: settlement.reason.clone(),
                        diagnostics: Vec::new(),
                    };
                    ingest_event(&tx, &event, cause)?;
                    let diag_vec: Vec<String> =
                        settlement.diagnostic.clone().into_iter().collect();
                    record_settlement(
                        &tx,
                        &run.run_id,
                        settlement.terminal_cause,
                        settlement.settled_by,
                        &diag_vec,
                    )?;
                    if settlement.state == ToolRunStateWire::Lost {
                        marked.push(run.run_id.clone());
                    }
                    settled.push(ToolRunReconcileSettlementWire {
                        run_id: run.run_id.clone(),
                        state: settlement.state,
                        terminal_cause: settlement.terminal_cause.to_string(),
                        settled_by: settlement.settled_by.to_string(),
                    });
                }
            }
        }
        touch_write_meta(&tx, now)?;
        tx.commit()?;
        Ok(ToolRunReconcileResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            marked_lost: marked,
            persisted: true,
            reap_candidates,
            settled,
            diagnostics,
        })
    }) {
        Ok(result) => Ok(result),
        Err(ToolRunError::Busy { message })
        | Err(ToolRunError::ReadOnly { message }) => {
            Ok(ToolRunReconcileResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                marked_lost: Vec::new(),
                persisted: false,
                reap_candidates: Vec::new(),
                settled: Vec::new(),
                diagnostics: vec![format!(
                    "reconciliation could not persist: {message}"
                )],
            })
        }
        Err(error) => Err(error),
    }
}

struct ForegroundOutcome {
    run_id: String,
    reap: Option<ToolRunReapCandidateWire>,
    settlement: ToolRunReconcileSettlementWire,
}

fn reconcile_foreground(
    tx: &rusqlite::Transaction<'_>,
    run: &ToolRunWire,
    fact: &ToolRunLivenessFactWire,
    now: i64,
) -> Result<Option<ForegroundOutcome>, ToolRunError> {
    match fact.observation {
        ToolLivenessObservationWire::Unknown
        | ToolLivenessObservationWire::Alive => Ok(None),
        ToolLivenessObservationWire::Dead => {
            let event = ToolRunEventWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                event_id: new_hex_id(),
                run_id: run.run_id.clone(),
                attempt: 1,
                kind: ToolRunEventKindWire::Lost,
                created_ts: now,
                stage: None,
                sample: None,
                exit_code: None,
                signal: None,
                reason: Some(fact.reason.clone().unwrap_or_else(|| {
                    TOOL_RUN_LOST_REASON_RUNNER_EXITED.to_string()
                })),
                diagnostics: Vec::new(),
            };
            ingest_event(
                tx,
                &event,
                Some(ToolRunTerminalCauseWire::WrapperLost),
            )?;
            record_settlement(
                tx,
                &run.run_id,
                ToolRunTerminalCauseWire::WrapperLost.as_str(),
                ToolRunSettledByWire::Reconcile.as_str(),
                &[],
            )?;
            let reap = if run.owner_kind.is_none() {
                match (&run.child_pgid, &run.child_process_start_identity) {
                    (Some(pgid), Some(identity)) => {
                        Some(ToolRunReapCandidateWire {
                            run_id: run.run_id.clone(),
                            pgid: *pgid,
                            child_process_start_identity: Some(
                                identity.clone(),
                            ),
                        })
                    }
                    _ => None,
                }
            } else {
                None
            };
            Ok(Some(ForegroundOutcome {
                run_id: run.run_id.clone(),
                reap,
                settlement: ToolRunReconcileSettlementWire {
                    run_id: run.run_id.clone(),
                    state: ToolRunStateWire::Lost,
                    terminal_cause: "wrapper_lost".to_string(),
                    settled_by: "reconcile".to_string(),
                },
            }))
        }
    }
}

fn owner_state_for(
    run: &ToolRunWire,
    fact: &ToolRunLivenessFactWire,
) -> (ToolRunOwnerStateWire, Option<String>) {
    let Some(owner) = fact.owner.as_ref() else {
        return (
            ToolRunOwnerStateWire::Unknown,
            Some(format!(
                "owner fact for {} is missing; treating as unknown",
                fact.run_id
            )),
        );
    };
    if owner.kind != run.owner_kind.as_deref().unwrap_or_default()
        || owner.id != run.owner_id.as_deref().unwrap_or_default()
    {
        return (
            ToolRunOwnerStateWire::Unknown,
            Some(format!(
                "owner fact for {} does not match the run owner; treating as unknown",
                fact.run_id
            )),
        );
    }
    (owner.state, None)
}

fn stop_requested(run: &ToolRunWire, fact: &ToolRunLivenessFactWire) -> bool {
    if run.stop_request.is_some() {
        return true;
    }
    if let Some(owner) = fact.owner.as_ref() {
        if owner.stop_requested == Some(true) {
            return true;
        }
        if owner.termination_reason.as_deref() == Some("stop") {
            return true;
        }
    }
    false
}

fn decide_handoff(
    run: &ToolRunWire,
    fact: &ToolRunLivenessFactWire,
) -> Decision {
    let (owner_state, owner_diagnostic) = owner_state_for(run, fact);
    let owner = fact.owner.clone();
    let stop = stop_requested(run, fact);
    match run.state {
        ToolRunStateWire::Created => {
            if owner_state == ToolRunOwnerStateWire::Active
                || fact.observation == ToolLivenessObservationWire::Alive
            {
                return Decision::NoTransition { diagnostic: None };
            }
            if owner_state == ToolRunOwnerStateWire::Terminal && stop {
                return Decision::Settle(Settlement {
                    state: ToolRunStateWire::Signaled,
                    terminal_cause: "stop_requested",
                    settled_by: "reconcile",
                    exit_code: None,
                    signal: None,
                    reason: None,
                    diagnostic: Some("command was not run".to_string()),
                });
            }
            if owner_state == ToolRunOwnerStateWire::Terminal {
                return Decision::Settle(Settlement {
                    state: ToolRunStateWire::Failed,
                    terminal_cause: "launch_failed",
                    settled_by: "reconcile",
                    exit_code: None,
                    signal: None,
                    reason: None,
                    diagnostic: Some("command was not run".to_string()),
                });
            }
            if owner_state == ToolRunOwnerStateWire::Missing
                && fact.observation == ToolLivenessObservationWire::Dead
            {
                return Decision::Settle(Settlement {
                    state: ToolRunStateWire::Failed,
                    terminal_cause: "launch_failed",
                    settled_by: "reconcile",
                    exit_code: None,
                    signal: None,
                    reason: None,
                    diagnostic: Some("command was not run".to_string()),
                });
            }
            Decision::NoTransition {
                diagnostic: Some(owner_diagnostic.unwrap_or_else(|| {
                    format!(
                        "created hand-off {} has no proof of launch failure",
                        run.run_id
                    )
                })),
            }
        }
        ToolRunStateWire::Running => {
            if matches!(
                fact.observation,
                ToolLivenessObservationWire::Alive
                    | ToolLivenessObservationWire::Unknown
            ) {
                return Decision::NoTransition { diagnostic: None };
            }
            if owner_state == ToolRunOwnerStateWire::Active {
                return Decision::NoTransition { diagnostic: None };
            }
            if owner_state == ToolRunOwnerStateWire::Terminal {
                let reason = owner
                    .as_ref()
                    .and_then(|item| item.termination_reason.clone())
                    .unwrap_or_default();
                let exit_code = owner.as_ref().and_then(|item| item.exit_code);
                if matches!(reason.as_str(), "success" | "error")
                    && matches!(exit_code, Some(code) if code >= 0)
                {
                    let code = exit_code.unwrap_or(0);
                    let state = if code == 0 {
                        ToolRunStateWire::Succeeded
                    } else {
                        ToolRunStateWire::Failed
                    };
                    return Decision::Settle(Settlement {
                        state,
                        terminal_cause: "exited",
                        settled_by: "owner",
                        exit_code: Some(code),
                        signal: None,
                        reason: None,
                        diagnostic: Some(
                            "recovered from owner result; fingerprints and stages may be missing"
                                .to_string(),
                        ),
                    });
                }
                if matches!(reason.as_str(), "total-timeout" | "idle-timeout") {
                    return Decision::Settle(Settlement {
                        state: ToolRunStateWire::Signaled,
                        terminal_cause: "timeout",
                        settled_by: "reconcile",
                        exit_code: None,
                        signal: None,
                        reason: None,
                        diagnostic: None,
                    });
                }
                if stop {
                    return Decision::Settle(Settlement {
                        state: ToolRunStateWire::Signaled,
                        terminal_cause: "stop_requested",
                        settled_by: "reconcile",
                        exit_code: None,
                        signal: None,
                        reason: None,
                        diagnostic: None,
                    });
                }
                if matches!(
                    reason.as_str(),
                    "supervisor-loss" | "reboot" | "launch-failure"
                ) || (reason == "error"
                    && !matches!(exit_code, Some(code) if code >= 0))
                {
                    return Decision::Settle(Settlement {
                        state: ToolRunStateWire::Lost,
                        terminal_cause: "owner_lost",
                        settled_by: "reconcile",
                        exit_code: None,
                        signal: None,
                        reason: None,
                        diagnostic: None,
                    });
                }
            }
            if owner_state == ToolRunOwnerStateWire::Missing {
                return Decision::Settle(Settlement {
                    state: ToolRunStateWire::Lost,
                    terminal_cause: "owner_lost",
                    settled_by: "reconcile",
                    exit_code: None,
                    signal: None,
                    reason: None,
                    diagnostic: None,
                });
            }
            Decision::NoTransition {
                diagnostic: Some(owner_diagnostic.unwrap_or_else(|| {
                    format!(
                        "running hand-off {} has no recoverable owner outcome",
                        run.run_id
                    )
                })),
            }
        }
        _ => Decision::NoTransition { diagnostic: None },
    }
}

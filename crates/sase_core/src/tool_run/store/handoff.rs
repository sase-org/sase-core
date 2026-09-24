//! Hand-off claim and durable stop requests.
//!
//! Claim atomically moves a `created` hand-off run to `running` when the
//! owner matches, or settles it `signaled`/`stop_requested` when a stop was
//! requested first. Stop requests are durable first-writer-wins facts that
//! never transition the run themselves.

use super::super::handoff_wire::{
    ToolRunClaimOutcomeWire, ToolRunClaimRefusalWire, ToolRunClaimRequestWire,
    ToolRunClaimResultWire, ToolRunStopOutcomeWire, ToolRunStopRequestWire,
    ToolRunStopResultWire,
};
use super::super::wire::{
    ToolRunEventKindWire, ToolRunEventWire, ToolRunStateWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    touch_write_meta, unix_now, validate_schema, with_write_store,
};
use super::lifecycle::{
    ingest_event, nonempty_or_generated, record_settlement,
};
use super::query::{load_launch_envelope, load_run};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

pub fn claim(
    store_path: &Path,
    request: ToolRunClaimRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunClaimResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    if request.owner_kind.trim().is_empty()
        || request.owner_id.trim().is_empty()
    {
        return Err(ToolRunError::invalid(
            "claim requires non-empty owner_kind and owner_id",
        ));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(run) = load_run(&tx, &request.run_id)? else {
            return Err(ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            });
        };
        let now = request.now_ts.unwrap_or_else(unix_now);
        let is_handoff = run.launch_mode.as_deref() == Some("handoff");
        match run.state {
            ToolRunStateWire::Created => {
                if !is_handoff {
                    let run =
                        load_run(&tx, &request.run_id)?.expect("claimed run");
                    tx.commit()?;
                    return Ok(refused(
                        run,
                        ToolRunClaimRefusalWire::NotHandoff,
                    ));
                }
                if run.owner_kind.as_deref()
                    != Some(request.owner_kind.as_str())
                    || run.owner_id.as_deref()
                        != Some(request.owner_id.as_str())
                {
                    let run =
                        load_run(&tx, &request.run_id)?.expect("claimed run");
                    tx.commit()?;
                    return Ok(refused(
                        run,
                        ToolRunClaimRefusalWire::OwnerMismatch,
                    ));
                }
                if has_stored_stop(&tx, &request.run_id)? {
                    let event_id = nonempty_or_generated(
                        request.running_event_id.as_deref(),
                    );
                    let event = ToolRunEventWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        event_id,
                        run_id: request.run_id.clone(),
                        attempt: 1,
                        kind: ToolRunEventKindWire::Signaled,
                        created_ts: now,
                        stage: None,
                        sample: None,
                        exit_code: None,
                        signal: None,
                        reason: None,
                        diagnostics: Vec::new(),
                    };
                    let replayed = ingest_event(
                        &tx,
                        &event,
                        Some(
                            super::super::handoff_wire::ToolRunTerminalCauseWire::StopRequested,
                        ),
                    )?;
                    if !replayed {
                        record_settlement(
                            &tx,
                            &request.run_id,
                            "stop_requested",
                            "wrapper",
                            &[
                                "command was not run: stop requested before the claim"
                                    .to_string(),
                            ],
                        )?;
                    }
                    touch_write_meta(&tx, now)?;
                    let run =
                        load_run(&tx, &request.run_id)?.expect("stopped run");
                    tx.commit()?;
                    return Ok(ToolRunClaimResultWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        outcome: ToolRunClaimOutcomeWire::Stopped,
                        refusal: None,
                        replayed,
                        run,
                        launch: None,
                        diagnostics: Vec::new(),
                    });
                }
                let event_id =
                    nonempty_or_generated(request.running_event_id.as_deref());
                let event = ToolRunEventWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    event_id,
                    run_id: request.run_id.clone(),
                    attempt: 1,
                    kind: ToolRunEventKindWire::Running,
                    created_ts: now,
                    stage: None,
                    sample: None,
                    exit_code: None,
                    signal: None,
                    reason: None,
                    diagnostics: Vec::new(),
                };
                ingest_event(&tx, &event, None)?;
                tx.execute(
                    "UPDATE runs SET wrapper_pid = ?2, boot_id = ?3,
                        process_start_identity = ?4 WHERE run_id = ?1",
                    params![
                        request.run_id,
                        request.wrapper_pid,
                        request.boot_id,
                        request.process_start_identity,
                    ],
                )?;
                if let Some(owner_log_path) = request.owner_log_path.as_deref()
                {
                    tx.execute(
                        "UPDATE runs SET owner_log_path = COALESCE(?2, owner_log_path)
                         WHERE run_id = ?1",
                        params![request.run_id, owner_log_path],
                    )?;
                }
                let envelope = load_launch_envelope(&tx, &request.run_id)?
                    .ok_or_else(|| {
                        ToolRunError::invalid(
                            "stored launch envelope was unreadable",
                        )
                    })?;
                touch_write_meta(&tx, now)?;
                let run = load_run(&tx, &request.run_id)?.expect("claimed run");
                tx.commit()?;
                Ok(ToolRunClaimResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunClaimOutcomeWire::Claimed,
                    refusal: None,
                    replayed: false,
                    run,
                    launch: Some(envelope),
                    diagnostics: Vec::new(),
                })
            }
            ToolRunStateWire::Running => {
                let owner_matches = run.owner_kind.as_deref()
                    == Some(request.owner_kind.as_str())
                    && run.owner_id.as_deref()
                        == Some(request.owner_id.as_str());
                let identity_matches = run.wrapper_pid == request.wrapper_pid
                    && run.boot_id == request.boot_id
                    && run.process_start_identity
                        == request.process_start_identity;
                if owner_matches && identity_matches {
                    let envelope = load_launch_envelope(&tx, &request.run_id)?
                        .ok_or_else(|| {
                            ToolRunError::invalid(
                                "stored launch envelope was unreadable",
                            )
                        })?;
                    let run =
                        load_run(&tx, &request.run_id)?.expect("claimed run");
                    tx.commit()?;
                    return Ok(ToolRunClaimResultWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        outcome: ToolRunClaimOutcomeWire::Claimed,
                        refusal: None,
                        replayed: true,
                        run,
                        launch: Some(envelope),
                        diagnostics: Vec::new(),
                    });
                }
                let run = load_run(&tx, &request.run_id)?.expect("claimed run");
                tx.commit()?;
                Ok(refused(run, ToolRunClaimRefusalWire::AlreadyClaimed))
            }
            _ => {
                if run.state == ToolRunStateWire::Signaled
                    && run.terminal_cause.as_deref() == Some("stop_requested")
                {
                    let run =
                        load_run(&tx, &request.run_id)?.expect("stopped run");
                    tx.commit()?;
                    return Ok(ToolRunClaimResultWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        outcome: ToolRunClaimOutcomeWire::Stopped,
                        refusal: None,
                        replayed: true,
                        run,
                        launch: None,
                        diagnostics: Vec::new(),
                    });
                }
                let run = load_run(&tx, &request.run_id)?.expect("refused run");
                tx.commit()?;
                Ok(refused(run, ToolRunClaimRefusalWire::NotCreated))
            }
        }
    })
}

fn refused(
    run: super::super::wire::ToolRunWire,
    refusal: ToolRunClaimRefusalWire,
) -> ToolRunClaimResultWire {
    ToolRunClaimResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        outcome: ToolRunClaimOutcomeWire::Refused,
        refusal: Some(refusal),
        replayed: false,
        run,
        launch: None,
        diagnostics: Vec::new(),
    }
}

fn has_stored_stop(
    tx: &rusqlite::Transaction<'_>,
    run_id: &str,
) -> Result<bool, ToolRunError> {
    let raw: Option<Option<String>> = tx
        .query_row(
            "SELECT stop_request_json FROM runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(None);
    Ok(matches!(raw, Some(Some(_))))
}

pub fn request_stop(
    store_path: &Path,
    request: ToolRunStopRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunStopResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(run) = load_run(&tx, &request.run_id)? else {
            return Err(ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            });
        };
        if run.state.is_terminal() {
            let stop_request = run.stop_request.clone();
            tx.commit()?;
            return Ok(ToolRunStopResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunStopOutcomeWire::AlreadySettled,
                run,
                stop_request,
                diagnostics: Vec::new(),
            });
        }
        if let Some(existing) = load_existing_stop(&tx, &request.run_id)? {
            tx.commit()?;
            return Ok(ToolRunStopResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunStopOutcomeWire::AlreadyRequested,
                run,
                stop_request: Some(existing),
                diagnostics: Vec::new(),
            });
        }
        let now = request.now_ts.unwrap_or_else(unix_now);
        let record = super::super::handoff_wire::ToolRunStopRecordWire {
            requested_ts: now,
            requested_by: request.requested_by.clone(),
            reason: request.reason.clone(),
        };
        let json = serde_json::to_string(&record)
            .map_err(|error| ToolRunError::store(error.to_string()))?;
        tx.execute(
            "UPDATE runs SET stop_request_json = ?2 WHERE run_id = ?1",
            params![request.run_id, json],
        )?;
        touch_write_meta(&tx, now)?;
        let run = load_run(&tx, &request.run_id)?.expect("stopped run");
        let stop_request = run.stop_request.clone();
        tx.commit()?;
        Ok(ToolRunStopResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            outcome: ToolRunStopOutcomeWire::Recorded,
            run,
            stop_request,
            diagnostics: Vec::new(),
        })
    })
}

fn load_existing_stop(
    tx: &rusqlite::Transaction<'_>,
    run_id: &str,
) -> Result<
    Option<super::super::handoff_wire::ToolRunStopRecordWire>,
    ToolRunError,
> {
    let raw: Option<Option<String>> = tx
        .query_row(
            "SELECT stop_request_json FROM runs WHERE run_id = ?1",
            [run_id],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(None);
    let Some(raw) = raw.flatten() else {
        return Ok(None);
    };
    let record = serde_json::from_str(&raw).map_err(|_| {
        ToolRunError::invalid("stored stop request was unreadable")
    })?;
    Ok(Some(record))
}

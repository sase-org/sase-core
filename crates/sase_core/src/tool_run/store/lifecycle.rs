//! Run lifecycle writes: begin, event append, finish, and reconcile.
//!
//! Owns the `runs`/`attempts` insert path, event ingestion with state
//! projection, transition validation, and run-scoped fingerprint persistence.

use super::super::catalog::{extra_args_digest, normalize_tool_definition};
use super::super::fingerprint::{
    canonicalize_tool_fingerprint, fingerprint_digest,
};
use super::super::wire::{
    ToolEvidenceCompletenessWire, ToolFingerprintWire,
    ToolLivenessObservationWire, ToolRunAppendRequestWire,
    ToolRunAppendResultWire, ToolRunBeginRequestWire, ToolRunBeginResultWire,
    ToolRunEventKindWire, ToolRunEventWire, ToolRunFinishRequestWire,
    ToolRunFinishResultWire, ToolRunObserveRequestWire,
    ToolRunObserveResultWire, ToolRunReapCandidateWire,
    ToolRunReconcileRequestWire, ToolRunReconcileResultWire, ToolRunStateWire,
    TOOL_RUN_LOST_REASON_RUNNER_EXITED, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    touch_write_meta, unix_now, validate_schema, with_write_store,
};
use super::query::load_run;
use rand::{rngs::OsRng, RngCore};
use rusqlite::{params, OptionalExtension, Transaction, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

pub fn begin(
    store_path: &Path,
    request: ToolRunBeginRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunBeginResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    let run_id = nonempty_or_generated(request.run_id.as_deref());
    let created_event_id =
        nonempty_or_generated(request.created_event_id.as_deref());
    let running_event_id = if request.commit_running {
        Some(nonempty_or_generated(request.running_event_id.as_deref()))
    } else {
        None
    };
    let (tool_name, definition_digest, extra_args_digest) =
        identity_for_begin(&request)?;
    if let Some(parent) = request.parent_run_id.as_deref() {
        if parent == run_id {
            return Err(ToolRunError::invalid(
                "parent_run_id must not equal run_id",
            ));
        }
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(parent) = request.parent_run_id.as_deref() {
            let exists: Option<String> = tx
                .query_row(
                    "SELECT run_id FROM runs WHERE run_id = ?1",
                    [parent],
                    |row| row.get(0),
                )
                .optional()?;
            if exists.is_none() {
                return Err(ToolRunError::invalid(format!(
                    "parent run {parent} was not found"
                )));
            }
        }
        let state = if request.commit_running {
            ToolRunStateWire::Running
        } else {
            ToolRunStateWire::Created
        };
        insert_run(
            &tx,
            &run_id,
            state,
            &tool_name,
            &definition_digest,
            &extra_args_digest,
            &request,
            now,
        )?;
        tx.execute(
            "INSERT INTO attempts(
                run_id, attempt, state, started_ts, settled_ts, exit_code,
                signal, diagnostics_json
             ) VALUES (?1, 1, ?2, ?3, NULL, NULL, NULL, '[]')",
            params![
                run_id,
                state.as_str(),
                if request.commit_running {
                    Some(now)
                } else {
                    None
                }
            ],
        )?;
        insert_lifecycle_event(
            &tx,
            &created_event_id,
            &run_id,
            ToolRunEventKindWire::Created,
            now,
            None,
            None,
            None,
        )?;
        if let Some(running_id) = running_event_id.as_deref() {
            insert_lifecycle_event(
                &tx,
                running_id,
                &run_id,
                ToolRunEventKindWire::Running,
                now,
                None,
                None,
                None,
            )?;
        }
        touch_write_meta(&tx, now)?;
        let run = load_run(&tx, &run_id)?.expect("inserted run");
        tx.commit()?;
        Ok(ToolRunBeginResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run,
            created_event_id: created_event_id.clone(),
            running_event_id: running_event_id.clone(),
            diagnostics: Vec::new(),
        })
    })
}

pub fn append_event(
    store_path: &Path,
    request: ToolRunAppendRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunAppendResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    validate_schema(request.event.schema_version)?;
    if request.event.event_id.trim().is_empty() {
        return Err(ToolRunError::invalid("event_id must not be empty"));
    }
    if request.event.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let replayed = ingest_event(&tx, &request.event)?;
        touch_write_meta(&tx, request.event.created_ts)?;
        let run = load_run(&tx, &request.event.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.event.run_id.clone(),
            }
        })?;
        tx.commit()?;
        Ok(ToolRunAppendResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            event_id: request.event.event_id.clone(),
            replayed,
            run,
            diagnostics: Vec::new(),
        })
    })
}

/// Persist a running run's child process facts at spawn time.
///
/// Begin happens before the spawn, so the child pid, pgid, and process-start
/// identity arrive through this call rather than through begin fields. A
/// repeated observation with identical facts is a replay, not a conflict; a
/// later `finish` that repeats the same facts stays idempotent through
/// `COALESCE`. Observing a settled or missing run is an error.
pub fn observe(
    store_path: &Path,
    request: ToolRunObserveRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunObserveResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let Some(stored) = load_run(&tx, &request.run_id)? else {
            return Err(ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            });
        };
        if !stored.state.is_unsettled() {
            return Err(ToolRunError::InvalidTransition {
                from: stored.state.as_str().to_string(),
                to: "observe".to_string(),
            });
        }
        let replayed = stored.child_pid == request.child_pid
            && stored.child_pgid == request.child_pgid
            && stored.child_process_start_identity
                == request.child_process_start_identity;
        if !replayed {
            tx.execute(
                "UPDATE runs SET child_pid = COALESCE(?2, child_pid),
                    child_pgid = COALESCE(?3, child_pgid),
                    child_process_start_identity =
                        COALESCE(?4, child_process_start_identity)
                 WHERE run_id = ?1",
                params![
                    request.run_id,
                    request.child_pid,
                    request.child_pgid,
                    request.child_process_start_identity,
                ],
            )?;
            touch_write_meta(&tx, unix_now())?;
        }
        let run = load_run(&tx, &request.run_id)?.expect("observed run");
        tx.commit()?;
        Ok(ToolRunObserveResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run,
            replayed,
            diagnostics: Vec::new(),
        })
    })
}

pub fn finish(
    store_path: &Path,
    request: ToolRunFinishRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunFinishResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if !request.state.is_terminal() {
        return Err(ToolRunError::invalid(format!(
            "finish state {} is not terminal",
            request.state.as_str()
        )));
    }
    let event_id = nonempty_or_generated(request.event_id.as_deref());
    let now = request.now_ts.unwrap_or_else(unix_now);
    let event = ToolRunEventWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        event_id: event_id.clone(),
        run_id: request.run_id.clone(),
        attempt: 1,
        kind: match request.state {
            ToolRunStateWire::Succeeded => ToolRunEventKindWire::Succeeded,
            ToolRunStateWire::Failed => ToolRunEventKindWire::Failed,
            ToolRunStateWire::Signaled => ToolRunEventKindWire::Signaled,
            ToolRunStateWire::Interrupted => ToolRunEventKindWire::Interrupted,
            ToolRunStateWire::Lost => ToolRunEventKindWire::Lost,
            other => {
                return Err(ToolRunError::invalid(format!(
                    "cannot finish in state {}",
                    other.as_str()
                )));
            }
        },
        created_ts: now,
        stage: None,
        sample: None,
        exit_code: request.exit_code,
        signal: request.signal,
        reason: request
            .lost_reason
            .clone()
            .or(request.interruption_reason.clone()),
        diagnostics: request.diagnostics.clone(),
    };
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ingest_event(&tx, &event)?;
        if request.child_pid.is_some()
            || request.child_pgid.is_some()
            || request.child_process_start_identity.is_some()
        {
            tx.execute(
                "UPDATE runs SET child_pid = COALESCE(?2, child_pid),
                    child_pgid = COALESCE(?3, child_pgid),
                    child_process_start_identity =
                        COALESCE(?4, child_process_start_identity)
                 WHERE run_id = ?1",
                params![
                    request.run_id,
                    request.child_pid,
                    request.child_pgid,
                    request.child_process_start_identity,
                ],
            )?;
        }
        if let Some(duration_ms) = request.duration_ms {
            tx.execute(
                "UPDATE runs SET duration_ms = ?2, duration_missing = NULL
                 WHERE run_id = ?1",
                params![request.run_id, duration_ms],
            )?;
        }
        let fingerprint_before = persist_optional_fingerprint(
            &tx,
            &request.run_id,
            "fingerprint_before_json",
            request.fingerprint_before.as_ref(),
        )?;
        let fingerprint_after = persist_optional_fingerprint(
            &tx,
            &request.run_id,
            "fingerprint_after_json",
            request.fingerprint_after.as_ref(),
        )?;
        let stored = load_run(&tx, &request.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            }
        })?;
        let before = fingerprint_before.or(stored.fingerprint_before);
        let after = fingerprint_after.or(stored.fingerprint_after);
        let mutated = request.mutated_input.or_else(|| {
            mutated_input_from_fingerprints(before.as_ref(), after.as_ref())
        });
        if let Some(mutated) = mutated {
            tx.execute(
                "UPDATE runs SET mutated_input = ?2 WHERE run_id = ?1",
                params![request.run_id, i64::from(mutated)],
            )?;
        }
        let evidence =
            evidence_from_fingerprints(before.as_ref(), after.as_ref());
        tx.execute(
            "UPDATE runs SET evidence_json = ?2 WHERE run_id = ?1",
            params![
                request.run_id,
                serde_json::to_string(&evidence)
                    .map_err(|error| ToolRunError::store(error.to_string()))?
            ],
        )?;
        touch_write_meta(&tx, now)?;
        let run = load_run(&tx, &request.run_id)?.ok_or_else(|| {
            ToolRunError::NotFound {
                run_id: request.run_id.clone(),
            }
        })?;
        tx.commit()?;
        Ok(ToolRunFinishResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run,
            event_id: event_id.clone(),
            diagnostics: Vec::new(),
        })
    })
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
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    match with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut marked = Vec::new();
        let mut reap_candidates = Vec::new();
        let mut diagnostics = Vec::new();
        for fact in &request.facts {
            match fact.observation {
                ToolLivenessObservationWire::Unknown => {
                    diagnostics.push(format!(
                        "liveness for {} is unknown; not proof of death",
                        fact.run_id
                    ));
                }
                ToolLivenessObservationWire::Alive => {}
                ToolLivenessObservationWire::Dead => {
                    if let Some(run) = load_run(&tx, &fact.run_id)? {
                        if run.state.is_unsettled() {
                            let event_id = new_hex_id();
                            let event = ToolRunEventWire {
                                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                                event_id,
                                run_id: fact.run_id.clone(),
                                attempt: 1,
                                kind: ToolRunEventKindWire::Lost,
                                created_ts: now,
                                stage: None,
                                sample: None,
                                exit_code: None,
                                signal: None,
                                reason: Some(
                                    fact.reason.clone().unwrap_or_else(|| {
                                        TOOL_RUN_LOST_REASON_RUNNER_EXITED
                                            .to_string()
                                    }),
                                ),
                                diagnostics: Vec::new(),
                            };
                            ingest_event(&tx, &event)?;
                            marked.push(fact.run_id.clone());
                            // Authorize reaping only when the wrapper is
                            // definitively dead (this arm) and the child
                            // facts were recorded at spawn. Rust never
                            // signals; the caller verifies the live
                            // process-group leader still matches the
                            // recorded identity before signaling. A missing
                            // pgid or identity authorizes nothing.
                            if let (
                                Some(pgid),
                                Some(child_process_start_identity),
                            ) = (
                                run.child_pgid,
                                run.child_process_start_identity.clone(),
                            ) {
                                reap_candidates.push(
                                    ToolRunReapCandidateWire {
                                        run_id: fact.run_id.clone(),
                                        pgid,
                                        child_process_start_identity: Some(
                                            child_process_start_identity,
                                        ),
                                    },
                                );
                            }
                        }
                    }
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
                diagnostics: vec![format!(
                    "reconciliation could not persist: {message}"
                )],
            })
        }
        Err(error) => Err(error),
    }
}

fn identity_for_begin(
    request: &ToolRunBeginRequestWire,
) -> Result<(Option<String>, String, String), ToolRunError> {
    let extra = extra_args_digest(&request.extra_args)?;
    if let Some(name) = &request.tool_name {
        let mut definition = request.definition.clone();
        if definition.name.is_empty() {
            definition.name = name.clone();
        }
        let normalized = normalize_tool_definition(definition)?;
        Ok((Some(normalized.definition.name), normalized.digest, extra))
    } else {
        let argv = request
            .private_argv
            .as_ref()
            .unwrap_or(&request.definition.argv);
        Ok((None, extra_args_digest(argv)?, extra))
    }
}

#[allow(clippy::too_many_arguments)]
fn insert_run(
    tx: &Transaction<'_>,
    run_id: &str,
    state: ToolRunStateWire,
    tool_name: &Option<String>,
    definition_digest: &str,
    extra_args_digest: &str,
    request: &ToolRunBeginRequestWire,
    now: i64,
) -> Result<(), ToolRunError> {
    let display_argv = serde_json::to_string(&request.display_argv)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let private_argv = request
        .private_argv
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let evidence =
        serde_json::to_string(&ToolEvidenceCompletenessWire::default())
            .map_err(|error| ToolRunError::store(error.to_string()))?;
    tx.execute(
        "INSERT INTO runs(
            run_id, state, source, executor, attempt, tool_name,
            definition_digest, extra_args_digest, display_argv_json,
            private_argv_json, project, agent, workspace, bead, owner_kind,
            owner_id, parent_run_id, created_ts, running_ts, wrapper_pid,
            boot_id, process_start_identity, log_stdout_path, log_stderr_path,
            events_path, evidence_json, diagnostics_json
         ) VALUES (
            ?1, ?2, 'native', 'inline', 1, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
            '[]'
         )",
        params![
            run_id,
            state.as_str(),
            tool_name,
            definition_digest,
            extra_args_digest,
            display_argv,
            private_argv,
            request.project,
            request.agent,
            request.workspace,
            request.bead,
            request.owner_kind,
            request.owner_id,
            request.parent_run_id,
            now,
            if request.commit_running {
                Some(now)
            } else {
                None
            },
            request.wrapper_pid,
            request.boot_id,
            request.process_start_identity,
            request.log_stdout_path,
            request.log_stderr_path,
            request.events_path,
            evidence,
        ],
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn insert_lifecycle_event(
    tx: &Transaction<'_>,
    event_id: &str,
    run_id: &str,
    kind: ToolRunEventKindWire,
    created_ts: i64,
    exit_code: Option<i32>,
    signal: Option<i32>,
    reason: Option<String>,
) -> Result<(), ToolRunError> {
    let event = ToolRunEventWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        event_id: event_id.to_string(),
        run_id: run_id.to_string(),
        attempt: 1,
        kind,
        created_ts,
        stage: None,
        sample: None,
        exit_code,
        signal,
        reason,
        diagnostics: Vec::new(),
    };
    insert_event_row(tx, &event)?;
    Ok(())
}

fn insert_event_row(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<(), ToolRunError> {
    let payload = serde_json::to_string(&canonical_event(event))
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    tx.execute(
        "INSERT INTO events(
            event_id, run_id, attempt, kind, payload_json, created_ts
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event.event_id,
            event.run_id,
            event.attempt as i64,
            event.kind.as_str(),
            payload,
            event.created_ts
        ],
    )?;
    Ok(())
}

fn ingest_event(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<bool, ToolRunError> {
    if event.attempt != 1 {
        return Err(ToolRunError::invalid(
            "native tool runs only support attempt 1",
        ));
    }
    let payload = serde_json::to_string(&canonical_event(event))
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let existing: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM events WHERE event_id = ?1",
            [&event.event_id],
            |row| row.get(0),
        )
        .optional()?;
    if let Some(existing) = existing {
        if existing == payload {
            return Ok(true);
        }
        return Err(ToolRunError::ConflictingEvent {
            event_id: event.event_id.clone(),
            reason: "payload does not match the stored event".to_string(),
        });
    }
    let run =
        load_run(tx, &event.run_id)?.ok_or_else(|| ToolRunError::NotFound {
            run_id: event.run_id.clone(),
        })?;
    if let Some(target) = event.kind.target_state() {
        if !can_transition(run.state, target) {
            return Err(ToolRunError::InvalidTransition {
                from: run.state.as_str().to_string(),
                to: target.as_str().to_string(),
            });
        }
    }
    insert_event_row(tx, event)?;
    apply_event_projection(tx, event)?;
    Ok(false)
}

fn apply_event_projection(
    tx: &Transaction<'_>,
    event: &ToolRunEventWire,
) -> Result<(), ToolRunError> {
    if let Some(target) = event.kind.target_state() {
        let settled = target.is_terminal().then_some(event.created_ts);
        let running_ts =
            (target == ToolRunStateWire::Running).then_some(event.created_ts);
        tx.execute(
            "UPDATE runs SET
                state = ?2,
                running_ts = COALESCE(?3, running_ts),
                settled_ts = COALESCE(?4, settled_ts),
                exit_code = COALESCE(?5, exit_code),
                signal = COALESCE(?6, signal),
                lost_reason = CASE WHEN ?2 = 'lost'
                    THEN COALESCE(?7, lost_reason) ELSE lost_reason END,
                interruption_reason = CASE WHEN ?2 = 'interrupted'
                    THEN COALESCE(?7, interruption_reason)
                    ELSE interruption_reason END,
                duration_missing = CASE WHEN ?2 IN (
                    'lost', 'interrupted', 'signaled'
                ) AND duration_ms IS NULL
                    THEN 'exit time was not observed'
                    ELSE duration_missing END
             WHERE run_id = ?1",
            params![
                event.run_id,
                target.as_str(),
                running_ts,
                settled,
                event.exit_code,
                event.signal,
                event.reason,
            ],
        )?;
        tx.execute(
            "UPDATE attempts SET
                state = ?2,
                started_ts = COALESCE(?3, started_ts),
                settled_ts = COALESCE(?4, settled_ts),
                exit_code = COALESCE(?5, exit_code),
                signal = COALESCE(?6, signal)
             WHERE run_id = ?1 AND attempt = ?7",
            params![
                event.run_id,
                target.as_str(),
                running_ts,
                settled,
                event.exit_code,
                event.signal,
                event.attempt as i64
            ],
        )?;
        return Ok(());
    }
    match event.kind {
        ToolRunEventKindWire::StageStarted
        | ToolRunEventKindWire::StageFinished => {
            let Some(stage) = &event.stage else {
                return Err(ToolRunError::invalid(
                    "stage events require a stage payload",
                ));
            };
            if stage.run_id != event.run_id {
                return Err(ToolRunError::invalid(
                    "stage.run_id must match event.run_id",
                ));
            }
            let diagnostics = serde_json::to_string(&stage.diagnostics)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            tx.execute(
                "INSERT INTO stages(
                    stage_id, run_id, attempt, event_id, description,
                    started_ts, finished_ts, elapsed_ms, exit_code,
                    output_bytes, incomplete, diagnostics_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
                 ON CONFLICT(stage_id) DO UPDATE SET
                    event_id = excluded.event_id,
                    finished_ts = COALESCE(excluded.finished_ts, stages.finished_ts),
                    elapsed_ms = COALESCE(excluded.elapsed_ms, stages.elapsed_ms),
                    exit_code = COALESCE(excluded.exit_code, stages.exit_code),
                    output_bytes = COALESCE(excluded.output_bytes, stages.output_bytes),
                    incomplete = excluded.incomplete,
                    diagnostics_json = excluded.diagnostics_json",
                params![
                    stage.stage_id,
                    stage.run_id,
                    stage.attempt as i64,
                    event.event_id,
                    stage.description,
                    stage.started_ts,
                    stage.finished_ts,
                    stage.elapsed_ms,
                    stage.exit_code,
                    stage.output_bytes,
                    i64::from(stage.incomplete
                        || event.kind == ToolRunEventKindWire::StageStarted),
                    diagnostics,
                ],
            )?;
        }
        ToolRunEventKindWire::Sample => {
            let Some(sample) = &event.sample else {
                return Err(ToolRunError::invalid(
                    "sample events require a sample payload",
                ));
            };
            if sample.run_id != event.run_id {
                return Err(ToolRunError::invalid(
                    "sample.run_id must match event.run_id",
                ));
            }
            let payload = serde_json::to_string(sample)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            tx.execute(
                "INSERT OR IGNORE INTO samples(
                    sample_id, run_id, attempt, event_id, observed_ts,
                    payload_json
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    sample.sample_id,
                    sample.run_id,
                    sample.attempt as i64,
                    event.event_id,
                    sample.observed_ts,
                    payload
                ],
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn can_transition(from: ToolRunStateWire, to: ToolRunStateWire) -> bool {
    matches!(
        (from, to),
        (ToolRunStateWire::Created, ToolRunStateWire::Running)
            | (ToolRunStateWire::Created, ToolRunStateWire::Lost)
            | (ToolRunStateWire::Running, ToolRunStateWire::Succeeded)
            | (ToolRunStateWire::Running, ToolRunStateWire::Failed)
            | (ToolRunStateWire::Running, ToolRunStateWire::Signaled)
            | (ToolRunStateWire::Running, ToolRunStateWire::Interrupted)
            | (ToolRunStateWire::Running, ToolRunStateWire::Lost)
    )
}

fn canonical_event(event: &ToolRunEventWire) -> ToolRunEventWire {
    let mut cloned = event.clone();
    cloned.diagnostics = Vec::new();
    cloned
}

fn persist_optional_fingerprint(
    tx: &Transaction<'_>,
    run_id: &str,
    column: &str,
    fingerprint: Option<&ToolFingerprintWire>,
) -> Result<Option<ToolFingerprintWire>, ToolRunError> {
    let Some(fingerprint) = fingerprint else {
        return Ok(None);
    };
    let canonical = canonicalize_tool_fingerprint(fingerprint.clone())?;
    let json = serde_json::to_string(&canonical.fingerprint)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    match column {
        "fingerprint_before_json" => {
            tx.execute(
                "UPDATE runs SET fingerprint_before_json = ?2 WHERE run_id = ?1",
                params![run_id, json],
            )?;
        }
        "fingerprint_after_json" => {
            tx.execute(
                "UPDATE runs SET fingerprint_after_json = ?2 WHERE run_id = ?1",
                params![run_id, json],
            )?;
        }
        other => {
            return Err(ToolRunError::store(format!(
                "unknown fingerprint column {other}"
            )));
        }
    }
    Ok(Some(canonical.fingerprint))
}

fn mutated_input_from_fingerprints(
    before: Option<&ToolFingerprintWire>,
    after: Option<&ToolFingerprintWire>,
) -> Option<bool> {
    let before = before?;
    let after = after?;
    if !before.completeness.complete || !after.completeness.complete {
        return None;
    }
    let left = fingerprint_digest(before).ok()?;
    let right = fingerprint_digest(after).ok()?;
    Some(left != right)
}

fn evidence_from_fingerprints(
    before: Option<&ToolFingerprintWire>,
    after: Option<&ToolFingerprintWire>,
) -> ToolEvidenceCompletenessWire {
    let mut missing = Vec::new();
    match before {
        None => missing.push("fingerprint_before not observed".to_string()),
        Some(fingerprint) if !fingerprint.completeness.complete => {
            missing.extend(fingerprint.completeness.missing.iter().cloned());
        }
        Some(_) => {}
    }
    match after {
        None => missing.push("fingerprint_after not observed".to_string()),
        Some(fingerprint) if !fingerprint.completeness.complete => {
            missing.extend(fingerprint.completeness.missing.iter().cloned());
        }
        Some(_) => {}
    }
    if missing.is_empty() {
        ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        }
    } else {
        ToolEvidenceCompletenessWire {
            complete: false,
            missing,
        }
    }
}

fn nonempty_or_generated(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.trim().is_empty() => value.to_string(),
        _ => new_hex_id(),
    }
}

fn new_hex_id() -> String {
    let mut bytes = [0u8; 16];
    OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

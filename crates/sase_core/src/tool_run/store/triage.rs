//! Store-backed triage record/show plus table probe.
//!
//! Additive and cascade-safe: record never updates stored rows, labels are
//! first-writer-wins, and retention deletes triage rows explicitly.

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

use super::super::canonical::canonical_digest;
use super::super::triage::{
    tool_run_triage_verdict, ToolRunTriageClassWire, ToolRunTriageItemWire,
    ToolRunTriageLabelWire, ToolRunTriageRecordRequestWire,
    ToolRunTriageRecordResultWire, ToolRunTriageRefusalWire,
    ToolRunTriageRunFactsWire, ToolRunTriageShowRequestWire,
    ToolRunTriageShowResultWire, ToolRunTriageStageFactsWire,
    ToolRunTriageVerdictItemWire, ToolRunTriageVerdictRequestWire,
    TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS, TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
use super::super::triage::{
    ToolRunTriageContinuationModeWire, ToolRunTriageDecisionKindWire,
    ToolRunTriageDecisionWire, ToolRunTriageExtractionStatusWire,
};
use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::connection::{
    runs_column_set, touch_write_meta, unix_now, validate_schema,
    with_read_store, with_write_store,
};

pub fn triage_tables_present(conn: &Connection) -> Result<bool, ToolRunError> {
    for table in [
        "tool_triage_items",
        "tool_triage_stages",
        "tool_triage_runs",
    ] {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )?;
        if count == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

fn verdict_for_unstored(
    state: &str,
    exit_code: Option<i64>,
    terminal_cause: Option<String>,
    signal: Option<i64>,
    interruption: Option<String>,
    lost: Option<String>,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    let has_terminal = terminal_cause.is_some();
    let request = ToolRunTriageVerdictRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        exit_code: exit_code.map(|code| code as i32),
        terminal_cause,
        legacy_state: if has_terminal {
            None
        } else {
            Some(state.to_string())
        },
        legacy_exit_code: if has_terminal {
            None
        } else {
            exit_code.map(|code| code as i32)
        },
        legacy_signal: if has_terminal {
            None
        } else {
            signal.map(|code| code as i32)
        },
        legacy_interruption_reason: if has_terminal {
            None
        } else {
            interruption
        },
        legacy_lost_reason: if has_terminal { None } else { lost },
        has_completed_stage: false,
        has_setup_marker: false,
        has_failed_stage: state == "failed",
        all_stages_complete: false,
        recipe_finished: false,
        is_stageful_tool: true,
        triaged: false,
        has_unparsed_failed_stage: false,
        items: Vec::new(),
    };
    match tool_run_triage_verdict(request) {
        Ok(result) => (
            Some(result.kind.as_str().to_string()),
            Some(result.verdict.as_str().to_string()),
            Some(result.reason),
            result.remedy,
        ),
        Err(_) => (None, None, None, None),
    }
}

type StoredRunRow = (
    String,
    Option<i64>,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<String>,
);

fn is_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn bound_display(raw: &str) -> String {
    let count = raw.chars().count();
    if count <= TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS {
        return raw.to_string();
    }
    let mut out: String = raw
        .chars()
        .take(TOOL_RUN_TRIAGE_DISPLAY_MAX_CHARS - 1)
        .collect();
    out.push('…');
    out
}

fn triage_item_id(
    run_id: &str,
    stage_key: &str,
    extractor: &str,
    extractor_version: u32,
    signature: &str,
) -> Result<String, ToolRunError> {
    canonical_digest(&(
        run_id,
        stage_key,
        extractor,
        extractor_version,
        signature,
    ))
    .map_err(ToolRunError::invalid)
}

#[allow(clippy::too_many_lines)]
pub fn triage_record(
    store_path: &Path,
    request: ToolRunTriageRecordRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunTriageRecordResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    // Stage keys non-empty and unique within the request.
    let mut seen_stages = std::collections::HashSet::new();
    for stage in &request.stages {
        if stage.stage_key.trim().is_empty() {
            return Err(ToolRunError::invalid("stage_key must not be empty"));
        }
        if !seen_stages.insert(stage.stage_key.clone()) {
            return Err(ToolRunError::invalid(format!(
                "duplicate stage_key {:?} in triage record",
                stage.stage_key
            )));
        }
        for item in &stage.items {
            if item.stage_key != stage.stage_key {
                return Err(ToolRunError::invalid(
                    "triage item stage_key must equal its stage",
                ));
            }
            if item.extractor.trim().is_empty() {
                return Err(ToolRunError::invalid(
                    "triage extractor must not be empty",
                ));
            }
            if item.extractor_version < 1 {
                return Err(ToolRunError::invalid(
                    "triage extractor_version must be >= 1",
                ));
            }
            if item.occurrences < 1 {
                return Err(ToolRunError::invalid(
                    "triage occurrences must be >= 1",
                ));
            }
            if !is_hex64(&item.signature) {
                return Err(ToolRunError::invalid(
                    "triage signature must be 64 lowercase hex",
                ));
            }
            for path in &item.locator_paths {
                if path.starts_with('/') {
                    return Err(ToolRunError::invalid(
                        "triage locator path must not be absolute",
                    ));
                }
            }
            let expected = triage_item_id(
                &request.run_id,
                &item.stage_key,
                &item.extractor,
                item.extractor_version,
                &item.signature,
            )?;
            if let Some(supplied) = item.item_id.as_deref() {
                if supplied != expected {
                    return Err(ToolRunError::invalid(
                        "triage item_id mismatch",
                    ));
                }
            }
        }
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        // Refusals: nothing written.
        let stored_run: Option<Option<String>> = tx
            .query_row(
                "SELECT tool_name FROM runs WHERE run_id = ?1",
                [&request.run_id],
                |row| row.get(0),
            )
            .optional()?;
        let Some(tool_name) = stored_run else {
            return Ok(ToolRunTriageRecordResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                refused: Some(ToolRunTriageRefusalWire::RunNotFound),
                items_inserted: 0,
                items_existing: 0,
                labels_written: 0,
                labels_kept: 0,
                stages_inserted: 0,
                decisions_written: 0,
                diagnostics: Vec::new(),
            });
        };
        if tool_name.is_none() {
            return Ok(ToolRunTriageRecordResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                refused: Some(ToolRunTriageRefusalWire::AdHocRun),
                items_inserted: 0,
                items_existing: 0,
                labels_written: 0,
                labels_kept: 0,
                stages_inserted: 0,
                decisions_written: 0,
                diagnostics: Vec::new(),
            });
        }
        let mut items_inserted = 0u64;
        let mut items_existing = 0u64;
        let mut labels_written = 0u64;
        let mut labels_kept = 0u64;
        let mut stages_inserted = 0u64;
        let mut decisions_written = 0u64;
        for stage in &request.stages {
            let changed = tx.execute(
                "INSERT INTO tool_triage_stages(
                    run_id, stage_key, stage_id, extraction_status,
                    output_path, created_ts, mode, decision, reason,
                    elapsed_ms, decided_ts
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT(run_id, stage_key) DO NOTHING",
                params![
                    request.run_id,
                    stage.stage_key,
                    stage.stage_id,
                    stage.extraction_status.as_str(),
                    stage.output_path,
                    now,
                    stage
                        .decision
                        .as_ref()
                        .map(|decision| decision.mode.as_str()),
                    stage
                        .decision
                        .as_ref()
                        .map(|decision| decision.decision.as_str()),
                    stage
                        .decision
                        .as_ref()
                        .map(|decision| decision.reason.clone()),
                    stage
                        .decision
                        .as_ref()
                        .and_then(|decision| decision.elapsed_ms),
                    stage.decision.as_ref().map(|decision| decision.decided_ts),
                ],
            )?;
            if changed > 0 {
                stages_inserted += 1;
            } else {
                // Fill stage_id/output_path only when NULL; decision only
                // when decision IS NULL (first decision wins).
                if stage.stage_id.is_some() {
                    tx.execute(
                        "UPDATE tool_triage_stages SET stage_id = COALESCE(stage_id, ?3)
                         WHERE run_id = ?1 AND stage_key = ?2",
                        params![
                            request.run_id,
                            stage.stage_key,
                            stage.stage_id,
                        ],
                    )?;
                }
                if stage.output_path.is_some() {
                    tx.execute(
                        "UPDATE tool_triage_stages SET output_path = COALESCE(output_path, ?3)
                         WHERE run_id = ?1 AND stage_key = ?2",
                        params![
                            request.run_id,
                            stage.stage_key,
                            stage.output_path,
                        ],
                    )?;
                }
                if let Some(decision) = stage.decision.as_ref() {
                    let wrote = tx.execute(
                        "UPDATE tool_triage_stages
                         SET mode = ?3, decision = ?4, reason = ?5,
                             elapsed_ms = ?6, decided_ts = ?7
                         WHERE run_id = ?1 AND stage_key = ?2
                           AND decision IS NULL",
                        params![
                            request.run_id,
                            stage.stage_key,
                            decision.mode.as_str(),
                            decision.decision.as_str(),
                            decision.reason,
                            decision.elapsed_ms,
                            decision.decided_ts,
                        ],
                    )?;
                    decisions_written += u64::from(wrote > 0);
                }
            }
            if stage.decision.is_some() && changed > 0 {
                decisions_written += 1;
            }
            for item in &stage.items {
                let item_id = triage_item_id(
                    &request.run_id,
                    &item.stage_key,
                    &item.extractor,
                    item.extractor_version,
                    &item.signature,
                )?;
                let display = bound_display(&item.display);
                let locators = serde_json::to_string(&item.locator_paths)
                    .map_err(|error| ToolRunError::store(error.to_string()))?;
                let changed = tx.execute(
                    "INSERT INTO tool_triage_items(
                        item_id, run_id, stage_id, stage_key, extractor,
                        extractor_version, signature, display,
                        locator_paths_json, occurrences, created_ts,
                        class, touched, rule_version, knobs_json,
                        evidence_json, possible_owners_json, classified_ts
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                               ?12, ?13, ?14, ?15, ?16, ?17, ?18)
                     ON CONFLICT(item_id) DO NOTHING",
                    params![
                        item_id,
                        request.run_id,
                        item.stage_id,
                        item.stage_key,
                        item.extractor,
                        item.extractor_version as i64,
                        item.signature,
                        display,
                        locators,
                        item.occurrences as i64,
                        now,
                        item.label.as_ref().map(|label| label.class.as_str()),
                        item.label
                            .as_ref()
                            .and_then(|label| label.touched.map(i64::from)),
                        item.label
                            .as_ref()
                            .map(|label| label.rule_version as i64),
                        item.label
                            .as_ref()
                            .map(|label| serde_json::to_string(&label.knobs)
                                .unwrap_or_else(|_| "null".to_string())),
                        item.label.as_ref().map(|label| serde_json::to_string(
                            &label.evidence
                        )
                        .unwrap_or_else(|_| "null".to_string())),
                        item.label.as_ref().map(|label| serde_json::to_string(
                            &label.possible_owners
                        )
                        .unwrap_or_else(|_| "[]".to_string())),
                        item.label.as_ref().map(|label| label.classified_ts),
                    ],
                )?;
                if changed > 0 {
                    items_inserted += 1;
                    if item.label.is_some() {
                        labels_written += 1;
                    }
                } else {
                    items_existing += 1;
                    if let Some(label) = item.label.as_ref() {
                        let knobs = serde_json::to_string(&label.knobs)
                            .map_err(|error| {
                                ToolRunError::store(error.to_string())
                            })?;
                        let evidence = serde_json::to_string(&label.evidence)
                            .map_err(|error| {
                            ToolRunError::store(error.to_string())
                        })?;
                        let owners =
                            serde_json::to_string(&label.possible_owners)
                                .map_err(|error| {
                                    ToolRunError::store(error.to_string())
                                })?;
                        let wrote = tx.execute(
                            "UPDATE tool_triage_items
                             SET class = ?2, touched = ?3, rule_version = ?4,
                                 knobs_json = ?5, evidence_json = ?6,
                                 possible_owners_json = ?7, classified_ts = ?8
                             WHERE item_id = ?1 AND class IS NULL",
                            params![
                                item_id,
                                label.class.as_str(),
                                label.touched.map(i64::from),
                                label.rule_version as i64,
                                knobs,
                                evidence,
                                owners,
                                label.classified_ts,
                            ],
                        )?;
                        if wrote > 0 {
                            labels_written += 1;
                        } else {
                            labels_kept += 1;
                        }
                    }
                }
            }
        }
        // Run facts: insert when missing; nullable columns COALESCE.
        let facts =
            request
                .run_facts
                .clone()
                .unwrap_or(ToolRunTriageRunFactsWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    continuation_mode: None,
                    recipe_finished_ts: None,
                    first_continued_exit_code: None,
                    continuation_extra_ms: None,
                    repeat_of_run_id: None,
                    triaged_ts: None,
                    diagnostics: Vec::new(),
                });
        let triaged_ts = facts.triaged_ts.unwrap_or(now);
        let diagnostics_json = serde_json::to_string(&Vec::<String>::new())
            .map_err(|error| ToolRunError::store(error.to_string()))?;
        tx.execute(
            "INSERT INTO tool_triage_runs(
                run_id, continuation_mode, recipe_finished_ts,
                first_continued_exit_code, continuation_extra_ms,
                repeat_of_run_id, triaged_ts, created_ts, diagnostics_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(run_id) DO NOTHING",
            params![
                request.run_id,
                facts
                    .continuation_mode
                    .map(|mode| mode.as_str().to_string()),
                facts.recipe_finished_ts,
                facts.first_continued_exit_code,
                facts.continuation_extra_ms,
                facts.repeat_of_run_id,
                triaged_ts,
                now,
                diagnostics_json,
            ],
        )?;
        // COALESCE nullable columns with the new values.
        tx.execute(
            "UPDATE tool_triage_runs
             SET continuation_mode = COALESCE(continuation_mode, ?2),
                 recipe_finished_ts = COALESCE(recipe_finished_ts, ?3),
                 first_continued_exit_code = COALESCE(first_continued_exit_code, ?4),
                 continuation_extra_ms = COALESCE(continuation_extra_ms, ?5),
                 repeat_of_run_id = COALESCE(repeat_of_run_id, ?6),
                 triaged_ts = COALESCE(triaged_ts, ?7)
             WHERE run_id = ?1",
            params![
                request.run_id,
                facts.continuation_mode.map(|mode| mode.as_str().to_string()),
                facts.recipe_finished_ts,
                facts.first_continued_exit_code,
                facts.continuation_extra_ms,
                facts.repeat_of_run_id,
                triaged_ts,
            ],
        )?;
        // Diagnostics: order-preserving de-duplicated union.
        let existing_raw: Option<String> = tx
            .query_row(
                "SELECT diagnostics_json FROM tool_triage_runs WHERE run_id = ?1",
                [&request.run_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        let mut merged: Vec<String> = existing_raw
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
            .unwrap_or_default();
        for item in facts.diagnostics {
            if !merged.contains(&item) {
                merged.push(item);
            }
        }
        let merged_json = serde_json::to_string(&merged)
            .map_err(|error| ToolRunError::store(error.to_string()))?;
        tx.execute(
            "UPDATE tool_triage_runs SET diagnostics_json = ?2 WHERE run_id = ?1",
            params![request.run_id, merged_json],
        )?;
        touch_write_meta(&tx, now)?;
        tx.commit()?;
        Ok(ToolRunTriageRecordResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            refused: None,
            items_inserted,
            items_existing,
            labels_written,
            labels_kept,
            stages_inserted,
            decisions_written,
            diagnostics: Vec::new(),
        })
    })
}

pub fn triage_show(
    store_path: &Path,
    request: ToolRunTriageShowRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunTriageShowResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if !store_path.exists() {
        return Ok(ToolRunTriageShowResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            run_found: false,
            triaged: false,
            run_facts: None,
            stages: Vec::new(),
            items: Vec::new(),
            failure_kind: None,
            verdict: None,
            verdict_reason: None,
            remedy: None,
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        // Newest-run lookup by (owner_kind, owner_id) when run_id is empty.
        let resolved_run_id = if request.run_id.trim().is_empty() {
            match (request.owner_kind.as_deref(), request.owner_id.as_deref()) {
                (Some(kind), Some(id))
                    if !kind.trim().is_empty() && !id.trim().is_empty() =>
                {
                    let found: Option<String> = conn
                        .query_row(
                            "SELECT run_id FROM runs
                                 WHERE owner_kind = ?1 AND owner_id = ?2
                                 ORDER BY created_ts DESC, run_id DESC LIMIT 1",
                            [kind, id],
                            |row| row.get(0),
                        )
                        .optional()?;
                    found.unwrap_or_default()
                }
                _ => String::new(),
            }
        } else {
            request.run_id.clone()
        };
        let columns = runs_column_set(conn)?;
        let terminal_projection = if columns.contains("terminal_cause") {
            "terminal_cause".to_string()
        } else {
            "NULL".to_string()
        };
        let sql = format!(
            "SELECT state, exit_code, {terminal_projection}, signal,
                    interruption_reason, lost_reason
             FROM runs WHERE run_id = ?1"
        );
        let stored_run: Option<StoredRunRow> = conn
            .query_row(&sql, [&resolved_run_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })
            .optional()?;
        let run_found = stored_run.is_some();
        if !triage_tables_present(conn)? {
            let (failure_kind, verdict, verdict_reason, remedy) = stored_run
                .as_ref()
                .map(
                    |(
                        state,
                        exit_code,
                        terminal_cause,
                        signal,
                        interruption,
                        lost,
                    )| {
                        verdict_for_unstored(
                            state,
                            *exit_code,
                            terminal_cause.clone(),
                            *signal,
                            interruption.clone(),
                            lost.clone(),
                        )
                    },
                )
                .unwrap_or((None, None, None, None));
            return Ok(ToolRunTriageShowResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: resolved_run_id,
                run_found,
                triaged: false,
                run_facts: None,
                stages: Vec::new(),
                items: Vec::new(),
                failure_kind,
                verdict,
                verdict_reason,
                remedy,
                diagnostics: Vec::new(),
            });
        }
        let mut diagnostics = Vec::new();
        // Stages ordered by created_ts, stage_key.
        let mut stages = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT stage_key, stage_id, extraction_status, output_path,
                        mode, decision, reason, elapsed_ms, decided_ts,
                        created_ts
                 FROM tool_triage_stages WHERE run_id = ?1
                 ORDER BY created_ts, stage_key",
            )?;
            let rows = stmt.query_map([&resolved_run_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<i64>>(7)?,
                    row.get::<_, Option<i64>>(8)?,
                    row.get::<_, i64>(9)?,
                ))
            })?;
            for row in rows {
                let (
                    stage_key,
                    stage_id,
                    status_raw,
                    output_path,
                    mode_raw,
                    decision_raw,
                    reason,
                    elapsed_ms,
                    decided_ts,
                    created_ts,
                ) = row?;
                let status =
                    ToolRunTriageExtractionStatusWire::from_db(&status_raw)
                        .map_err(ToolRunError::store)?;
                let decision =
                    match (mode_raw, decision_raw, reason, decided_ts) {
                        (
                            Some(mode_raw),
                            Some(decision_raw),
                            Some(reason),
                            Some(decided_ts),
                        ) => {
                            let mode =
                                ToolRunTriageContinuationModeWire::from_db(
                                    &mode_raw,
                                )
                                .map_err(ToolRunError::store)?;
                            let decision =
                                ToolRunTriageDecisionKindWire::from_db(
                                    &decision_raw,
                                )
                                .map_err(ToolRunError::store)?;
                            Some(ToolRunTriageDecisionWire {
                                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                                mode,
                                decision,
                                reason,
                                elapsed_ms,
                                decided_ts,
                            })
                        }
                        _ => None,
                    };
                stages.push(ToolRunTriageStageFactsWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_key,
                    stage_id,
                    extraction_status: status,
                    output_path,
                    decision,
                    created_ts: Some(created_ts),
                });
            }
        }
        // Items ordered by stage_key, created_ts, extractor, signature.
        let mut items = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT item_id, stage_key, stage_id, extractor,
                        extractor_version, signature, display,
                        locator_paths_json, occurrences,
                        class, touched, rule_version, knobs_json,
                        evidence_json, possible_owners_json, classified_ts
                 FROM tool_triage_items WHERE run_id = ?1
                 ORDER BY stage_key, created_ts, extractor, signature",
            )?;
            let rows = stmt.query_map([&resolved_run_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                    row.get::<_, Option<String>>(9)?,
                    row.get::<_, Option<i64>>(10)?,
                    row.get::<_, Option<i64>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, Option<String>>(13)?,
                    row.get::<_, Option<String>>(14)?,
                    row.get::<_, Option<i64>>(15)?,
                ))
            })?;
            for row in rows {
                let (
                    item_id,
                    stage_key,
                    stage_id,
                    extractor,
                    extractor_version,
                    signature,
                    display,
                    locators_raw,
                    occurrences,
                    class_raw,
                    touched_raw,
                    rule_version,
                    knobs_raw,
                    evidence_raw,
                    owners_raw,
                    classified_ts,
                ) = row?;
                let locator_paths: Vec<String> =
                    serde_json::from_str(&locators_raw).unwrap_or_default();
                let label = match class_raw {
                    None => None,
                    Some(class_raw) => {
                        match ToolRunTriageClassWire::from_db(&class_raw) {
                            Ok(class) => Some(ToolRunTriageLabelWire {
                                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                                class,
                                touched: touched_raw.map(|value| value != 0),
                                rule_version: rule_version.unwrap_or(0) as u32,
                                knobs: knobs_raw
                                    .as_deref()
                                    .and_then(|raw| {
                                        serde_json::from_str(raw).ok()
                                    })
                                    .unwrap_or(serde_json::Value::Null),
                                evidence: evidence_raw
                                    .as_deref()
                                    .and_then(|raw| {
                                        serde_json::from_str(raw).ok()
                                    })
                                    .unwrap_or(serde_json::Value::Null),
                                possible_owners: owners_raw
                                    .as_deref()
                                    .and_then(|raw| {
                                        serde_json::from_str(raw).ok()
                                    })
                                    .unwrap_or(serde_json::Value::Array(
                                        Vec::new(),
                                    )),
                                classified_ts: classified_ts.unwrap_or(0),
                            }),
                            Err(_) => {
                                diagnostics.push(format!(
                                    "unreadable triage label class {class_raw:?} for item {item_id}"
                                ));
                                None
                            }
                        }
                    }
                };
                items.push(ToolRunTriageItemWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    item_id: Some(item_id),
                    stage_key,
                    stage_id,
                    extractor,
                    extractor_version: extractor_version as u32,
                    signature,
                    display,
                    locator_paths,
                    occurrences: occurrences as u32,
                    label,
                });
            }
        }
        // Run facts.
        type TriageRunRow = (
            Option<String>,
            Option<i64>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<i64>,
            String,
        );
        let run_row: Option<TriageRunRow> = conn
            .query_row(
                "SELECT continuation_mode, recipe_finished_ts,
                        first_continued_exit_code, continuation_extra_ms,
                        repeat_of_run_id, triaged_ts, diagnostics_json
                 FROM tool_triage_runs WHERE run_id = ?1",
                [&resolved_run_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()?;
        let (run_facts, triaged) = match run_row {
            None => (None, false),
            Some((
                mode_raw,
                recipe_finished_ts,
                first_continued_exit_code,
                continuation_extra_ms,
                repeat_of_run_id,
                triaged_ts,
                diagnostics_raw,
            )) => {
                let continuation_mode = mode_raw
                    .as_deref()
                    .map(ToolRunTriageContinuationModeWire::from_db)
                    .transpose()
                    .map_err(ToolRunError::store)?;
                let diagnostics: Vec<String> =
                    serde_json::from_str(&diagnostics_raw).unwrap_or_default();
                let triaged = triaged_ts.is_some();
                (
                    Some(ToolRunTriageRunFactsWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        continuation_mode,
                        recipe_finished_ts,
                        first_continued_exit_code: first_continued_exit_code
                            .map(|value| value as i32),
                        continuation_extra_ms,
                        repeat_of_run_id,
                        triaged_ts,
                        diagnostics,
                    }),
                    triaged,
                )
            }
        };
        let (failure_kind, verdict, verdict_reason, remedy) = match stored_run
            .as_ref()
        {
            None => (None, None, None, None),
            Some((
                state,
                exit_code,
                terminal_cause,
                signal,
                interruption,
                lost,
            )) => {
                let has_terminal = terminal_cause.is_some();
                let has_completed_stage = stages.iter().any(|stage| {
                    stage.stage_key != TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT
                });
                let has_setup_marker =
                    items.iter().any(|item| item.extractor == "environment");
                let has_failed_stage = state == "failed"
                    && (has_completed_stage || !items.is_empty());
                let has_unparsed = stages.iter().any(|stage| {
                        !matches!(
                            stage.extraction_status,
                            super::super::triage::ToolRunTriageExtractionStatusWire::Parsed
                        )
                    });
                let is_stageful = !(stages.iter().all(|stage| {
                    stage.stage_key == TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT
                }) && !stages.is_empty());
                let request = ToolRunTriageVerdictRequestWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    exit_code: exit_code.map(|code| code as i32),
                    terminal_cause: terminal_cause.clone(),
                    legacy_state: if has_terminal {
                        None
                    } else {
                        Some(state.clone())
                    },
                    legacy_exit_code: if has_terminal {
                        None
                    } else {
                        exit_code.map(|code| code as i32)
                    },
                    legacy_signal: if has_terminal {
                        None
                    } else {
                        signal.map(|code| code as i32)
                    },
                    legacy_interruption_reason: if has_terminal {
                        None
                    } else {
                        interruption.clone()
                    },
                    legacy_lost_reason: if has_terminal {
                        None
                    } else {
                        lost.clone()
                    },
                    has_completed_stage,
                    has_setup_marker,
                    has_failed_stage,
                    all_stages_complete: true,
                    recipe_finished: run_facts.as_ref().is_some_and(|facts| {
                        facts.recipe_finished_ts.is_some()
                    }),
                    is_stageful_tool: is_stageful,
                    triaged,
                    has_unparsed_failed_stage: has_unparsed
                        && state == "failed",
                    items: items
                        .iter()
                        .map(|item| ToolRunTriageVerdictItemWire {
                            class: item
                                .label
                                .as_ref()
                                .map(|label| label.class.as_str().to_string()),
                        })
                        .collect(),
                };
                match tool_run_triage_verdict(request) {
                    Ok(result) => (
                        Some(result.kind.as_str().to_string()),
                        Some(result.verdict.as_str().to_string()),
                        Some(result.reason),
                        result.remedy,
                    ),
                    Err(_) => (None, None, None, None),
                }
            }
        };
        Ok(ToolRunTriageShowResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: resolved_run_id.clone(),
            run_found,
            triaged,
            run_facts,
            stages,
            items,
            failure_kind,
            verdict,
            verdict_reason,
            remedy,
            diagnostics,
        })
    })
}

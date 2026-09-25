//! Store-backed stage and settle: extract, gather bounded evidence,
//! classify unlabeled items, persist labels once.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use rusqlite::{params, OptionalExtension};

use super::super::triage::{
    extract_triage_items, tool_run_triage_classify,
    ToolRunTriageClassifyRequestWire, ToolRunTriageEvidenceItemWire,
    ToolRunTriageEvidenceRunWire, ToolRunTriageItemWire,
    ToolRunTriageLabelWire, ToolRunTriageSubjectItemWire,
    ToolRunTriageSubjectRunWire, TOOL_RUN_TRIAGE_LOOKBACK_SECS,
    TOOL_RUN_TRIAGE_RULE_VERSION,
};
use super::super::triage::{
    ToolRunTriageExtractRequestWire, ToolRunTriageRecordRequestWire,
    ToolRunTriageRefusalWire, ToolRunTriageRunFactsWire,
    ToolRunTriageStageRecordWire, TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT,
};
use super::super::triage::{
    ToolRunTriageSettleRequestWire, ToolRunTriageSettleResultWire,
    ToolRunTriageStageRequestWire, ToolRunTriageStageResultWire,
};
use super::super::wire::{ToolFingerprintWire, TOOL_RUN_WIRE_SCHEMA_VERSION};
use super::super::ToolRunError;
use super::connection::{
    runs_column_set, unix_now, validate_schema, with_read_store,
    with_write_store,
};
use super::triage::{triage_record, triage_show, triage_tables_present};

type FingerprintParts = (Option<String>, Vec<String>, bool, Option<String>);

type SubjectRowTuple = (
    String,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

fn subject_fingerprint_parts(
    raw: Option<String>,
    project: &str,
) -> Result<FingerprintParts, ToolRunError> {
    let Some(raw) = raw else {
        return Ok((None, Vec::new(), false, None));
    };
    let fingerprint: ToolFingerprintWire = serde_json::from_str(&raw)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    let complete = fingerprint.completeness.complete;
    let repo = fingerprint
        .repos
        .iter()
        .find(|repo| repo.identity == project);
    let base = repo.and_then(|repo| repo.head.clone());
    let dirty = repo
        .map(|repo| {
            repo.dirty_paths
                .iter()
                .map(|entry| entry.path.clone())
                .collect()
        })
        .unwrap_or_default();
    let digest = crate::tool_run::fingerprint::canonicalize_tool_fingerprint(
        fingerprint,
    )
    .map(|result| result.digest)
    .ok();
    Ok((base, dirty, complete, digest))
}

struct SubjectRunRow {
    run_id: String,
    project: String,
    tool: String,
    extra_args_digest: String,
    workspace: Option<String>,
    terminal_cause: Option<String>,
    fingerprint_before_json: Option<String>,
}

fn load_subject_row(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<Option<SubjectRunRow>, ToolRunError> {
    let columns = runs_column_set(conn)?;
    let terminal_projection = if columns.contains("terminal_cause") {
        "terminal_cause".to_string()
    } else {
        "NULL".to_string()
    };
    let sql = format!(
        "SELECT run_id, project, tool_name, extra_args_digest, workspace,
                {terminal_projection}, fingerprint_before_json
         FROM runs WHERE run_id = ?1"
    );
    let row: Option<SubjectRowTuple> = conn
        .query_row(&sql, [run_id], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ))
        })
        .optional()?;
    Ok(row.map(
        |(
            run_id,
            project,
            tool,
            extra_args_digest,
            workspace,
            terminal_cause,
            fingerprint_before_json,
        )| {
            SubjectRunRow {
                run_id,
                project: project.unwrap_or_default(),
                tool: tool.unwrap_or_default(),
                extra_args_digest,
                workspace,
                terminal_cause,
                fingerprint_before_json,
            }
        },
    ))
}

fn subject_is_suppressed(subject: &SubjectRunRow) -> bool {
    // Control/infrastructure runs carry no item labels: the verdict is
    // undetermined without judgments. Environment and verification run
    // through classification; legacy rows without a cause are not
    // suppressed here (the verdict maps ambiguity conservatively).
    matches!(
        subject.terminal_cause.as_deref(),
        Some(
            "stop_requested"
                | "interrupt"
                | "timeout"
                | "launch_failed"
                | "owner_lost"
                | "wrapper_lost"
                | "signal"
        )
    )
}

fn load_items_for_run(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<Vec<ToolRunTriageEvidenceItemWire>, ToolRunError> {
    if !triage_tables_present(conn)? {
        return Ok(Vec::new());
    }
    let mut stmt = conn.prepare(
        "SELECT extractor, extractor_version, signature, stage_key
         FROM tool_triage_items WHERE run_id = ?1",
    )?;
    let rows = stmt.query_map([run_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (extractor, version, signature, stage_key) = row?;
        out.push(ToolRunTriageEvidenceItemWire {
            extractor,
            extractor_version: version as u32,
            signature,
            stage_key: Some(stage_key),
        });
    }
    Ok(out)
}

fn load_stage_completions(
    conn: &rusqlite::Connection,
    run_id: &str,
) -> Result<Vec<String>, ToolRunError> {
    let mut out: BTreeSet<String> = BTreeSet::new();
    if triage_tables_present(conn)? {
        let mut stmt = conn.prepare(
            "SELECT stage_key FROM tool_triage_stages WHERE run_id = ?1",
        )?;
        let rows = stmt.query_map([run_id], |row| row.get::<_, String>(0))?;
        for row in rows {
            out.insert(row?);
        }
    }
    // Execution stages that finished also count as completed.
    let mut stmt = conn.prepare(
        "SELECT description FROM stages WHERE run_id = ?1
         AND finished_ts IS NOT NULL",
    )?;
    let rows = stmt.query_map([run_id], |row| row.get::<_, String>(0))?;
    for row in rows {
        out.insert(row?);
    }
    Ok(out.into_iter().collect())
}

fn gather_evidence(
    conn: &rusqlite::Connection,
    subject: &SubjectRunRow,
    now_ts: i64,
) -> Result<Vec<ToolRunTriageEvidenceRunWire>, ToolRunError> {
    if !triage_tables_present(conn)? {
        return Ok(Vec::new());
    }
    let cutoff = now_ts.saturating_sub(TOOL_RUN_TRIAGE_LOOKBACK_SECS);
    let mut stmt = conn.prepare(
        "SELECT run_id, project, tool_name, extra_args_digest, workspace,
                agent, settled_ts, fingerprint_before_json, state
         FROM runs
         WHERE project = ?1 AND tool_name = ?2 AND extra_args_digest = ?3
           AND run_id != ?4 AND settled_ts IS NOT NULL AND settled_ts >= ?5
         ORDER BY settled_ts DESC, run_id DESC LIMIT 500",
    )?;
    let rows = stmt.query_map(
        params![
            subject.project,
            subject.tool,
            subject.extra_args_digest,
            subject.run_id,
            cutoff,
        ],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<i64>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, String>(8)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (
            run_id,
            _project,
            _tool,
            extra_args_digest,
            workspace,
            agent,
            settled_ts,
            fingerprint_json,
            _state,
        ) = row?;
        let Some(settled_ts) = settled_ts else {
            continue;
        };
        let (base, dirty, complete, digest) =
            subject_fingerprint_parts(fingerprint_json, &subject.project)?;
        if !complete {
            continue;
        }
        let clean_tree = dirty.is_empty();
        let items = load_items_for_run(conn, &run_id)?;
        let stage_completions = load_stage_completions(conn, &run_id)?;
        out.push(ToolRunTriageEvidenceRunWire {
            run_id,
            project: subject.project.clone(),
            tool: subject.tool.clone(),
            extra_args_digest,
            workspace,
            agent,
            machine: None,
            settled_ts,
            base_head: base,
            complete_fingerprint: complete,
            fingerprint_digest: digest,
            dirty_paths: dirty,
            dirty_unknown: false,
            clean_tree,
            ad_hoc: false,
            failed: _state == "failed",
            stage_completions,
            items,
            selection_source: false,
        });
    }
    Ok(out)
}

fn label_to_stored(
    label: &super::super::triage::ToolRunTriageClassifyLabelWire,
    now_ts: i64,
) -> ToolRunTriageLabelWire {
    ToolRunTriageLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        class: label.class,
        touched: Some(label.touched),
        rule_version: TOOL_RUN_TRIAGE_RULE_VERSION,
        knobs: label.knobs.clone(),
        evidence: label.evidence.clone(),
        possible_owners: label.possible_owners.clone(),
        classified_ts: now_ts,
    }
}

pub fn triage_stage(
    store_path: &Path,
    request: ToolRunTriageStageRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunTriageStageResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    if request.stage.stage_key.trim().is_empty() {
        return Err(ToolRunError::invalid("stage_key must not be empty"));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    // Read subject + evidence without holding a write transaction.
    let read = with_read_store(store_path, busy_timeout, |conn| {
        let subject = load_subject_row(conn, &request.run_id)?;
        let Some(subject) = subject else {
            return Ok(None);
        };
        if subject.tool.is_empty() {
            return Ok(None);
        };
        let evidence = gather_evidence(conn, &subject, now)?;
        Ok(Some((subject, evidence)))
    })?;
    let Some((subject, mut evidence)) = read else {
        // Distinguish missing vs ad-hoc with a second lookup.
        let adhoc = with_read_store(store_path, busy_timeout, |conn| {
            let row: Option<Option<String>> = conn
                .query_row(
                    "SELECT tool_name FROM runs WHERE run_id = ?1",
                    [&request.run_id],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(row)
        })?;
        let refused = match adhoc {
            None => ToolRunTriageRefusalWire::RunNotFound,
            Some(None) => ToolRunTriageRefusalWire::AdHocRun,
            Some(Some(_)) => ToolRunTriageRefusalWire::RunNotFound,
        };
        // Ad-hoc check: tool_name NULL means ad-hoc.
        if matches!(refused, ToolRunTriageRefusalWire::AdHocRun) {
            return Ok(ToolRunTriageStageResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                stage_key: request.stage.stage_key.clone(),
                refused: Some(refused),
                items: Vec::new(),
                repeat_of: None,
                diagnostics: Vec::new(),
            });
        }
        // Missing run.
        return Ok(ToolRunTriageStageResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            stage_key: request.stage.stage_key.clone(),
            refused: Some(ToolRunTriageRefusalWire::RunNotFound),
            items: Vec::new(),
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    };
    if subject.tool.is_empty() {
        return Ok(ToolRunTriageStageResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            stage_key: request.stage.stage_key.clone(),
            refused: Some(ToolRunTriageRefusalWire::AdHocRun),
            items: Vec::new(),
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    }
    // Extract the supplied stage output.
    let extracted = extract_triage_items(ToolRunTriageExtractRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: request.stage.stage_key.clone(),
        stage_id: request.stage.stage_id.clone(),
        output: request.stage.output.clone(),
        truncated: request.stage.truncated,
        project_root: request.project_root.clone(),
        workspace_roots: request.workspace_roots.clone(),
    })?;
    let (base, dirty, complete, digest) = subject_fingerprint_parts(
        subject.fingerprint_before_json.clone(),
        &subject.project,
    )?;
    if !complete {
        return Err(ToolRunError::invalid("subject fingerprint is incomplete"));
    }
    let subjects: Vec<ToolRunTriageSubjectItemWire> = extracted
        .items
        .iter()
        .map(|item| ToolRunTriageSubjectItemWire {
            stage_key: item.stage_key.clone(),
            extractor: item.extractor.clone(),
            extractor_version: item.extractor_version,
            signature: item.signature.clone(),
            locator_paths: item.locator_paths.clone(),
        })
        .collect();
    if subjects.is_empty() {
        // Still record the stage row so show/aggregate see it.
        let record = triage_record(
            store_path,
            ToolRunTriageRecordRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                stages: vec![ToolRunTriageStageRecordWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_key: request.stage.stage_key.clone(),
                    stage_id: request.stage.stage_id.clone(),
                    extraction_status: extracted.status,
                    output_path: request.stage.output_path.clone(),
                    decision: None,
                    items: Vec::new(),
                }],
                run_facts: None,
                now_ts: Some(now),
            },
            busy_timeout,
        )?;
        let _ = record;
        return Ok(ToolRunTriageStageResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            stage_key: request.stage.stage_key.clone(),
            refused: None,
            items: Vec::new(),
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    }
    if subject_is_suppressed(&subject) {
        // Control/infrastructure runs carry no labels.
        triage_record(
            store_path,
            ToolRunTriageRecordRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                stages: vec![ToolRunTriageStageRecordWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    stage_key: request.stage.stage_key.clone(),
                    stage_id: request.stage.stage_id.clone(),
                    extraction_status: extracted.status,
                    output_path: request.stage.output_path.clone(),
                    decision: None,
                    items: extracted.items.clone(),
                }],
                run_facts: Some(ToolRunTriageRunFactsWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    continuation_mode: None,
                    recipe_finished_ts: None,
                    first_continued_exit_code: None,
                    continuation_extra_ms: None,
                    repeat_of_run_id: None,
                    triaged_ts: Some(now),
                    diagnostics: Vec::new(),
                }),
                now_ts: Some(now),
            },
            busy_timeout,
        )?;
        let shown = triage_show(
            store_path,
            super::super::triage::ToolRunTriageShowRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                owner_kind: None,
                owner_id: None,
            },
            busy_timeout,
        )?;
        let stage_items: Vec<ToolRunTriageItemWire> = shown
            .items
            .into_iter()
            .filter(|item| item.stage_key == request.stage.stage_key)
            .collect();
        return Ok(ToolRunTriageStageResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            stage_key: request.stage.stage_key.clone(),
            refused: None,
            items: stage_items,
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    }
    evidence.extend(request.selection_records.clone());
    let classified =
        tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            subject_run: ToolRunTriageSubjectRunWire {
                run_id: subject.run_id.clone(),
                project: subject.project.clone(),
                tool: subject.tool.clone(),
                extra_args_digest: subject.extra_args_digest.clone(),
                workspace: subject.workspace.clone(),
                machine: None,
                base_head: base,
                dirty_paths: dirty,
                complete_fingerprint: complete,
                fingerprint_digest: digest,
                ad_hoc: false,
            },
            subjects,
            evidence_runs: evidence,
            selection_records: Vec::new(),
            ancestry: request.ancestry.clone(),
            flake_baseline: request.flake_baseline.clone(),
            owner_candidates: request.owner_candidates.clone(),
            knobs: request.knobs.clone(),
            now_ts: Some(now),
        })?;
    // Persist items with labels once.
    let mut stored_items = Vec::new();
    for (extracted_item, label) in
        extracted.items.iter().zip(classified.labels.iter())
    {
        let mut item = extracted_item.clone();
        item.label = Some(label_to_stored(label, now));
        stored_items.push(item);
    }
    triage_record(
        store_path,
        ToolRunTriageRecordRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            stages: vec![ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: request.stage.stage_key.clone(),
                stage_id: request.stage.stage_id.clone(),
                extraction_status: extracted.status,
                output_path: request.stage.output_path.clone(),
                decision: None,
                items: stored_items.clone(),
            }],
            run_facts: Some(ToolRunTriageRunFactsWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                continuation_mode: None,
                recipe_finished_ts: None,
                first_continued_exit_code: None,
                continuation_extra_ms: None,
                repeat_of_run_id: classified.repeat_of.clone(),
                triaged_ts: Some(now),
                diagnostics: Vec::new(),
            }),
            now_ts: Some(now),
        },
        busy_timeout,
    )?;
    // Read back stored rows so callers see item ids.
    let shown = triage_show(
        store_path,
        super::super::triage::ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            owner_kind: None,
            owner_id: None,
        },
        busy_timeout,
    )?;
    let stage_items: Vec<ToolRunTriageItemWire> = shown
        .items
        .into_iter()
        .filter(|item| item.stage_key == request.stage.stage_key)
        .collect();
    Ok(ToolRunTriageStageResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        stage_key: request.stage.stage_key.clone(),
        refused: None,
        items: stage_items,
        repeat_of: classified.repeat_of,
        diagnostics: Vec::new(),
    })
}

pub fn triage_settle(
    store_path: &Path,
    request: ToolRunTriageSettleRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunTriageSettleResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let subject_opt = with_read_store(store_path, busy_timeout, |conn| {
        load_subject_row(conn, &request.run_id)
    })?;
    let Some(subject) = subject_opt else {
        return Ok(ToolRunTriageSettleResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            refused: Some(ToolRunTriageRefusalWire::RunNotFound),
            triaged: false,
            items: Vec::new(),
            failure_kind: None,
            verdict: None,
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    };
    if subject.tool.is_empty() {
        return Ok(ToolRunTriageSettleResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            refused: Some(ToolRunTriageRefusalWire::AdHocRun),
            triaged: false,
            items: Vec::new(),
            failure_kind: None,
            verdict: None,
            repeat_of: None,
            diagnostics: Vec::new(),
        });
    }
    // Extract each supplied missing stage; already-recorded stages are
    // classified from their stored unlabeled rows instead.
    let mut fresh: Vec<(
        ToolRunTriageStageRecordWire,
        crate::tool_run::triage::ToolRunTriageExtractionStatusWire,
    )> = Vec::new();
    for stage in &request.stages {
        if stage.stage_key.trim().is_empty() {
            return Err(ToolRunError::invalid("stage_key must not be empty"));
        }
        let extracted =
            extract_triage_items(ToolRunTriageExtractRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: stage.stage_key.clone(),
                stage_id: stage.stage_id.clone(),
                output: stage.output.clone(),
                truncated: stage.truncated,
                project_root: request.project_root.clone(),
                workspace_roots: request.workspace_roots.clone(),
            })?;
        fresh.push((
            ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: stage.stage_key.clone(),
                stage_id: stage.stage_id.clone(),
                extraction_status: extracted.status,
                output_path: stage.output_path.clone(),
                decision: None,
                items: extracted.items,
            },
            extracted.status,
        ));
    }
    // Run-output fallback under `*` for stages:none tools.
    if let Some(output) = request.run_output.clone() {
        let extracted =
            extract_triage_items(ToolRunTriageExtractRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT.to_string(),
                stage_id: None,
                output: Some(output),
                truncated: request.run_output_truncated,
                project_root: request.project_root.clone(),
                workspace_roots: request.workspace_roots.clone(),
            })?;
        fresh.push((
            ToolRunTriageStageRecordWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                stage_key: TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT.to_string(),
                stage_id: None,
                extraction_status: extracted.status,
                output_path: None,
                decision: None,
                items: extracted.items,
            },
            extracted.status,
        ));
    }
    let (base, dirty, complete, digest) = subject_fingerprint_parts(
        subject.fingerprint_before_json.clone(),
        &subject.project,
    )?;
    if !complete {
        return Err(ToolRunError::invalid("subject fingerprint is incomplete"));
    }
    let evidence = with_read_store(store_path, busy_timeout, |conn| {
        gather_evidence(conn, &subject, now)
    })?;
    let mut combined_evidence = evidence;
    combined_evidence.extend(request.selection_records.clone());
    // Subjects are the fresh items; stored unlabeled rows are merged after
    // the first record below. Classify fresh rows now.
    let subjects: Vec<ToolRunTriageSubjectItemWire> = fresh
        .iter()
        .flat_map(|(stage, _)| stage.items.clone())
        .map(|item| ToolRunTriageSubjectItemWire {
            stage_key: item.stage_key.clone(),
            extractor: item.extractor.clone(),
            extractor_version: item.extractor_version,
            signature: item.signature.clone(),
            locator_paths: item.locator_paths.clone(),
        })
        .collect();
    let suppressed = subject_is_suppressed(&subject);
    let classified = if subjects.is_empty() || suppressed {
        None
    } else {
        Some(tool_run_triage_classify(
            ToolRunTriageClassifyRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                subject_run: ToolRunTriageSubjectRunWire {
                    run_id: subject.run_id.clone(),
                    project: subject.project.clone(),
                    tool: subject.tool.clone(),
                    extra_args_digest: subject.extra_args_digest.clone(),
                    workspace: subject.workspace.clone(),
                    machine: None,
                    base_head: base.clone(),
                    dirty_paths: dirty.clone(),
                    complete_fingerprint: complete,
                    fingerprint_digest: digest.clone(),
                    ad_hoc: false,
                },
                subjects,
                evidence_runs: combined_evidence.clone(),
                selection_records: Vec::new(),
                ancestry: request.ancestry.clone(),
                flake_baseline: request.flake_baseline.clone(),
                owner_candidates: request.owner_candidates.clone(),
                knobs: request.knobs.clone(),
                now_ts: Some(now),
            },
        )?)
    };
    // Attach labels to fresh rows by (extractor, version, signature).
    let mut labeled_stages = Vec::new();
    for (mut stage, _) in fresh {
        let mut labeled_items = Vec::new();
        for mut item in stage.items.drain(..) {
            if let Some(classified) = classified.as_ref() {
                if let Some(label) = classified.labels.iter().find(|label| {
                    label.extractor == item.extractor
                        && label.extractor_version == item.extractor_version
                        && label.signature == item.signature
                }) {
                    item.label = Some(label_to_stored(label, now));
                }
            }
            labeled_items.push(item);
        }
        stage.items = labeled_items;
        labeled_stages.push(stage);
    }
    // Persist fresh rows plus run facts; stored rows win on replay and
    // stored labels win over recomputation.
    with_write_store(store_path, busy_timeout, |conn| {
        // Ensure triage tables exist before recording.
        if !triage_tables_present(conn)? {
            return Err(ToolRunError::store("triage tables are absent"));
        }
        Ok(())
    })?;
    let mut record_request = ToolRunTriageRecordRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        stages: labeled_stages,
        run_facts: Some(ToolRunTriageRunFactsWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            continuation_mode: request.continuation_mode,
            recipe_finished_ts: request.recipe_finished_ts,
            first_continued_exit_code: None,
            continuation_extra_ms: None,
            repeat_of_run_id: classified
                .as_ref()
                .and_then(|result| result.repeat_of.clone()),
            triaged_ts: Some(now),
            diagnostics: Vec::new(),
        }),
        now_ts: Some(now),
    };
    // Drop empty stages so a settle with no new output still classifies
    // stored unlabeled rows below.
    record_request.stages.retain(|stage| {
        !stage.items.is_empty()
            || !matches!(
                stage.extraction_status,
                crate::tool_run::triage::ToolRunTriageExtractionStatusWire::Parsed
            )
    });
    if !record_request.stages.is_empty() || record_request.run_facts.is_some() {
        triage_record(store_path, record_request, busy_timeout)?;
    }
    // Classify any stored unlabeled rows the fresh pass did not cover.
    let shown_before = triage_show(
        store_path,
        super::super::triage::ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            owner_kind: None,
            owner_id: None,
        },
        busy_timeout,
    )?;
    let unlabeled: Vec<ToolRunTriageSubjectItemWire> = shown_before
        .items
        .iter()
        .filter(|item| item.label.is_none())
        .map(|item| ToolRunTriageSubjectItemWire {
            stage_key: item.stage_key.clone(),
            extractor: item.extractor.clone(),
            extractor_version: item.extractor_version,
            signature: item.signature.clone(),
            locator_paths: item.locator_paths.clone(),
        })
        .collect();
    if !unlabeled.is_empty() && !suppressed {
        let classified =
            tool_run_triage_classify(ToolRunTriageClassifyRequestWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                subject_run: ToolRunTriageSubjectRunWire {
                    run_id: subject.run_id.clone(),
                    project: subject.project.clone(),
                    tool: subject.tool.clone(),
                    extra_args_digest: subject.extra_args_digest.clone(),
                    workspace: subject.workspace.clone(),
                    machine: None,
                    base_head: base,
                    dirty_paths: dirty,
                    complete_fingerprint: complete,
                    fingerprint_digest: digest,
                    ad_hoc: false,
                },
                subjects: unlabeled,
                evidence_runs: combined_evidence,
                selection_records: Vec::new(),
                ancestry: request.ancestry.clone(),
                flake_baseline: request.flake_baseline.clone(),
                owner_candidates: request.owner_candidates.clone(),
                knobs: request.knobs.clone(),
                now_ts: Some(now),
            })?;
        // Persist unlabeled rows grouped by stage.
        let mut by_stage: std::collections::BTreeMap<
            String,
            Vec<ToolRunTriageItemWire>,
        > = std::collections::BTreeMap::new();
        for item in shown_before.items {
            if item.label.is_some() {
                continue;
            }
            if let Some(label) = classified.labels.iter().find(|label| {
                label.signature == item.signature
                    && label.extractor == item.extractor
            }) {
                let mut stored = item.clone();
                stored.label = Some(label_to_stored(label, now));
                by_stage
                    .entry(stored.stage_key.clone())
                    .or_default()
                    .push(stored);
            }
        }
        for (stage_key, items) in by_stage {
            let stage_id =
                items.iter().filter_map(|item| item.stage_id.clone()).next();
            triage_record(
                store_path,
                ToolRunTriageRecordRequestWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    run_id: request.run_id.clone(),
                    stages: vec![ToolRunTriageStageRecordWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        stage_key,
                        stage_id,
                        extraction_status:
                            crate::tool_run::triage::ToolRunTriageExtractionStatusWire::Parsed,
                        output_path: None,
                        decision: None,
                        items,
                    }],
                    run_facts: None,
                    now_ts: Some(now),
                },
                busy_timeout,
            )?;
        }
    }
    let shown = triage_show(
        store_path,
        super::super::triage::ToolRunTriageShowRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            owner_kind: None,
            owner_id: None,
        },
        busy_timeout,
    )?;
    Ok(ToolRunTriageSettleResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        run_id: request.run_id.clone(),
        refused: None,
        triaged: shown.triaged,
        items: shown.items,
        failure_kind: shown.failure_kind,
        verdict: shown.verdict,
        repeat_of: shown
            .run_facts
            .as_ref()
            .and_then(|facts| facts.repeat_of_run_id.clone()),
        diagnostics: Vec::new(),
    })
}

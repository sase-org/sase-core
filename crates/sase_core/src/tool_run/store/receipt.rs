//! Receipt settle/lookup SQL plus retention helpers.
//!
//! Storage for the schema-1 ToolRun receipt contract. Pure policy, proof,
//! and diff helpers live in `super::receipt`; this module owns SQLite.

use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use std::path::Path;
use std::time::Duration;

use super::super::catalog::{normalize_receipt_policy, receipt_ttl_seconds};
use super::super::fingerprint::{
    canonicalize_tool_fingerprint, fingerprint_digest,
};
use super::super::receipt::{
    build_receipt_proof, diff_proof_against_fingerprint, proof_from_json,
    proof_to_json, receipt_id_for_run, RECEIPT_MAX_CHANGED_PATHS,
    RECEIPT_POLICY_VERSION,
};
use super::super::triage::tool_run_triage_verdict;
use super::super::triage::ToolRunTriageVerdictItemWire;
use super::super::triage::ToolRunTriageVerdictRequestWire;
use super::super::wire::{
    ToolRunReceiptLookupRequestWire, ToolRunReceiptLookupResultWire,
    ToolRunReceiptOutcomeWire, ToolRunReceiptRefusalWire,
    ToolRunReceiptSettleRequestWire, ToolRunReceiptSettleResultWire,
    ToolRunReceiptSignatureRefWire, ToolRunReceiptWire,
    TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::super::ToolRunError;
use super::connection::{
    touch_write_meta, unix_now, validate_schema, with_read_store,
    with_write_store,
};
use super::query::load_run;
use super::triage::triage_tables_present;

fn receipt_tables_present_inner(
    conn: &Connection,
) -> Result<bool, ToolRunError> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'tool_receipts'",
        [],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

#[allow(dead_code)]
pub(crate) fn receipt_tables_present(
    conn: &Connection,
) -> Result<bool, ToolRunError> {
    receipt_tables_present_inner(conn)
}

pub fn receipt_count_inner(conn: &Connection) -> Result<u64, ToolRunError> {
    if !receipt_tables_present_inner(conn)? {
        return Ok(0);
    }
    let count: i64 =
        conn.query_row("SELECT COUNT(*) FROM tool_receipts", [], |row| {
            row.get(0)
        })?;
    Ok(count as u64)
}

type ReceiptRowTuple = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    String,
    String,
);

type ReceiptFullRowTuple = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    i64,
    i64,
    i64,
    i64,
    i64,
    String,
    String,
    Option<String>,
);

struct TriageSubject {
    state: String,
    exit_code: Option<i64>,
    terminal_cause: Option<String>,
    signal: Option<i64>,
    interruption: Option<String>,
    lost: Option<String>,
}

fn row_to_receipt(
    row: ReceiptRowTuple,
) -> Result<ToolRunReceiptWire, ToolRunError> {
    let (
        receipt_id,
        source_run_id,
        project,
        tool_name,
        definition_digest,
        extra_args_digest,
        fingerprint_digest,
        verdict,
        signature_refs_json,
        issue_ts,
        mint_ts,
        expiry_ts,
        policy_version,
        ttl_seconds,
        accept_json,
        status,
    ) = row;
    let signature_refs: Vec<ToolRunReceiptSignatureRefWire> =
        serde_json::from_str(&signature_refs_json)
            .map_err(|error| ToolRunError::store(error.to_string()))?;
    let accept: Vec<String> = serde_json::from_str(&accept_json)
        .map_err(|error| ToolRunError::store(error.to_string()))?;
    Ok(ToolRunReceiptWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        receipt_id,
        source_run_id,
        project,
        tool_name,
        definition_digest,
        extra_args_digest,
        fingerprint_digest,
        verdict,
        signature_refs,
        issue_ts,
        mint_ts,
        expiry_ts,
        policy_version: policy_version as u32,
        ttl_seconds,
        accept,
        status,
    })
}

#[allow(clippy::too_many_lines)]
fn load_receipt_by_source(
    conn: &Connection,
    source_run_id: &str,
) -> Result<Option<ToolRunReceiptWire>, ToolRunError> {
    if !receipt_tables_present_inner(conn)? {
        return Ok(None);
    }
    let row: Option<ReceiptRowTuple> = conn
        .query_row(
            "SELECT receipt_id, source_run_id, project, tool_name,
                    definition_digest, extra_args_digest, fingerprint_digest,
                    verdict, signature_refs_json, issue_ts, mint_ts,
                    expiry_ts, policy_version, ttl_seconds, accept_json,
                    status
             FROM tool_receipts WHERE source_run_id = ?1",
            [source_run_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                ))
            },
        )
        .optional()?;
    match row {
        None => Ok(None),
        Some(tuple) => Ok(Some(row_to_receipt(tuple)?)),
    }
}

fn load_active_receipt(
    conn: &Connection,
    project: &str,
    tool_name: &str,
    definition_digest: &str,
    extra_args_digest: &str,
    fingerprint_digest: &str,
) -> Result<Option<ToolRunReceiptWire>, ToolRunError> {
    if !receipt_tables_present_inner(conn)? {
        return Ok(None);
    }
    let row: Option<ReceiptRowTuple> = conn
        .query_row(
            "SELECT receipt_id, source_run_id, project, tool_name,
                    definition_digest, extra_args_digest, fingerprint_digest,
                    verdict, signature_refs_json, issue_ts, mint_ts,
                    expiry_ts, policy_version, ttl_seconds, accept_json,
                    status
             FROM tool_receipts
             WHERE project = ?1 AND tool_name = ?2
               AND definition_digest = ?3 AND extra_args_digest = ?4
               AND fingerprint_digest = ?5 AND status = 'active'
             LIMIT 1",
            params![
                project,
                tool_name,
                definition_digest,
                extra_args_digest,
                fingerprint_digest
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                    row.get(14)?,
                    row.get(15)?,
                ))
            },
        )
        .optional()?;
    match row {
        None => Ok(None),
        Some(tuple) => Ok(Some(row_to_receipt(tuple)?)),
    }
}

struct TriageFacts {
    triaged: bool,
    has_run_row: bool,
    items: Vec<(String, u32, String, Option<String>)>,
}

fn load_triage_facts(
    conn: &Connection,
    run_id: &str,
    subject: &TriageSubject,
) -> Result<(TriageFacts, Option<String>, Option<String>), ToolRunError> {
    if !triage_tables_present(conn)? {
        let request = ToolRunTriageVerdictRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            exit_code: subject.exit_code.map(|code| code as i32),
            terminal_cause: subject.terminal_cause.clone(),
            legacy_state: if subject.terminal_cause.is_some() {
                None
            } else {
                Some(subject.state.clone())
            },
            legacy_exit_code: if subject.terminal_cause.is_some() {
                None
            } else {
                subject.exit_code.map(|code| code as i32)
            },
            legacy_signal: if subject.terminal_cause.is_some() {
                None
            } else {
                subject.signal.map(|code| code as i32)
            },
            legacy_interruption_reason: if subject.terminal_cause.is_some() {
                None
            } else {
                subject.interruption.clone()
            },
            legacy_lost_reason: if subject.terminal_cause.is_some() {
                None
            } else {
                subject.lost.clone()
            },
            has_completed_stage: false,
            has_setup_marker: false,
            has_failed_stage: subject.state == "failed",
            all_stages_complete: false,
            recipe_finished: false,
            is_stageful_tool: true,
            triaged: false,
            has_unparsed_failed_stage: false,
            items: Vec::new(),
        };
        match tool_run_triage_verdict(request) {
            Ok(result) => {
                return Ok((
                    TriageFacts {
                        triaged: false,
                        has_run_row: false,
                        items: Vec::new(),
                    },
                    Some(result.kind.as_str().to_string()),
                    Some(result.verdict.as_str().to_string()),
                ));
            }
            Err(_) => {
                return Ok((
                    TriageFacts {
                        triaged: false,
                        has_run_row: false,
                        items: Vec::new(),
                    },
                    None,
                    None,
                ));
            }
        }
    }
    let mut stages: Vec<(String, String)> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT stage_key, extraction_status FROM tool_triage_stages
             WHERE run_id = ?1",
        )?;
        let rows = stmt.query_map([run_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            stages.push(row?);
        }
    }
    let mut items: Vec<(String, u32, String, Option<String>)> = Vec::new();
    {
        let mut stmt = conn.prepare(
            "SELECT extractor, extractor_version, signature, class
             FROM tool_triage_items WHERE run_id = ?1",
        )?;
        let rows = stmt.query_map([run_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u32,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
            ))
        })?;
        for row in rows {
            items.push(row?);
        }
    }
    let run_row: Option<(Option<String>, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT continuation_mode, recipe_finished_ts, triaged_ts
             FROM tool_triage_runs WHERE run_id = ?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let (recipe_finished, triaged, has_run_row) = match run_row {
        None => (false, false, false),
        Some((_, recipe_ts, triaged_ts)) => {
            (recipe_ts.is_some(), triaged_ts.is_some(), true)
        }
    };
    let has_completed_stage = stages.iter().any(|(key, _)| {
        key != super::super::triage::TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT
    });
    let has_setup_marker = {
        let mut found = false;
        if triage_tables_present(conn)? {
            let mut stmt = conn.prepare(
                "SELECT COUNT(*) FROM tool_triage_items
                 WHERE run_id = ?1 AND extractor = 'environment'",
            )?;
            let count: i64 = stmt.query_row([run_id], |row| row.get(0))?;
            found = count > 0;
        }
        found
    };
    let has_failed_stage =
        subject.state == "failed" && (has_completed_stage || !items.is_empty());
    let has_unparsed = stages.iter().any(|(_, status)| status != "parsed")
        && subject.state == "failed";
    let is_stageful = !(stages.iter().all(|(key, _)| {
        key == super::super::triage::TOOL_RUN_TRIAGE_STAGE_KEY_RUN_OUTPUT
    }) && !stages.is_empty());
    let verdict_items: Vec<ToolRunTriageVerdictItemWire> = items
        .iter()
        .map(|(_, _, _, class)| ToolRunTriageVerdictItemWire {
            class: class.clone(),
        })
        .collect();
    let request = ToolRunTriageVerdictRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        exit_code: subject.exit_code.map(|code| code as i32),
        terminal_cause: subject.terminal_cause.clone(),
        legacy_state: if subject.terminal_cause.is_some() {
            None
        } else {
            Some(subject.state.clone())
        },
        legacy_exit_code: if subject.terminal_cause.is_some() {
            None
        } else {
            subject.exit_code.map(|code| code as i32)
        },
        legacy_signal: if subject.terminal_cause.is_some() {
            None
        } else {
            subject.signal.map(|code| code as i32)
        },
        legacy_interruption_reason: if subject.terminal_cause.is_some() {
            None
        } else {
            subject.interruption.clone()
        },
        legacy_lost_reason: if subject.terminal_cause.is_some() {
            None
        } else {
            subject.lost.clone()
        },
        has_completed_stage,
        has_setup_marker,
        has_failed_stage,
        all_stages_complete: true,
        recipe_finished,
        is_stageful_tool: is_stageful,
        triaged,
        has_unparsed_failed_stage: has_unparsed,
        items: verdict_items,
    };
    match tool_run_triage_verdict(request) {
        Ok(result) => {
            let kind = result.kind.as_str().to_string();
            let verdict = result.verdict.as_str().to_string();
            Ok((
                TriageFacts {
                    triaged,
                    has_run_row,
                    items,
                },
                Some(kind),
                Some(verdict),
            ))
        }
        Err(_) => Ok((
            TriageFacts {
                triaged,
                has_run_row,
                items,
            },
            None,
            None,
        )),
    }
}

fn normalize_lookup_accept(raw: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    for token in raw {
        let normalized = token.trim().to_lowercase();
        if normalized != "pass" && normalized != "no_new_failures" {
            continue;
        }
        if !out.iter().any(|existing| existing == &normalized) {
            out.push(normalized);
        }
    }
    if !out.iter().any(|token| token == "pass") {
        out.push("pass".to_string());
    }
    out.sort();
    out.dedup();
    out
}

#[allow(clippy::too_many_lines)]
pub fn receipt_settle(
    store_path: &Path,
    request: ToolRunReceiptSettleRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunReceiptSettleResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    if request.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("run_id must not be empty"));
    }
    let now = request.now_ts.unwrap_or_else(unix_now);
    let normalized_policy = match request.policy.clone() {
        None => None,
        Some(policy) => Some(normalize_receipt_policy(policy)?),
    };
    let ttl_seconds = match normalized_policy.as_ref() {
        None => None,
        Some(policy) => Some(receipt_ttl_seconds(policy)?),
    };
    let accept_list = normalized_policy
        .as_ref()
        .map(|policy| policy.accept.clone())
        .unwrap_or_default();
    with_write_store(store_path, busy_timeout, |conn| {
        let tx =
            conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !receipt_tables_present_inner(&tx)? {
            return Err(ToolRunError::store("tool_receipts table is absent"));
        }
        if let Some(existing) = load_receipt_by_source(&tx, &request.run_id)? {
            return Ok(ToolRunReceiptSettleResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                minted: false,
                superseded: existing.status == "superseded",
                receipt: Some(existing),
                reason: None,
                diagnostics: Vec::new(),
            });
        }
        let already: Option<String> = tx
            .query_row(
                "SELECT receipt_id FROM tool_receipts
                 WHERE superseded_by_run_id = ?1 LIMIT 1",
                [&request.run_id],
                |row| row.get(0),
            )
            .optional()?;
        if already.is_some() {
            return Ok(ToolRunReceiptSettleResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                minted: false,
                superseded: true,
                receipt: None,
                reason: Some(
                    "run already superseded an active receipt".to_string(),
                ),
                diagnostics: Vec::new(),
            });
        }
        let Some(run) = load_run(&tx, &request.run_id)? else {
            return Ok(ToolRunReceiptSettleResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                minted: false,
                superseded: false,
                receipt: None,
                reason: Some("run_not_found".to_string()),
                diagnostics: Vec::new(),
            });
        };
        let subject = TriageSubject {
            state: run.state.as_str().to_string(),
            exit_code: run.exit_code.map(|code| code as i64),
            terminal_cause: run.terminal_cause.clone(),
            signal: run.signal.map(|code| code as i64),
            interruption: run.interruption_reason.clone(),
            lost: run.lost_reason.clone(),
        };
        let (facts, kind_opt, verdict_opt) =
            load_triage_facts(&tx, &request.run_id, &subject)?;
        let mut ineligible: Option<String> = None;
        if normalized_policy.is_none() {
            ineligible = Some("no policy".to_string());
        } else if request.bypassed {
            ineligible = Some("bypassed".to_string());
        } else if run.tool_name.as_deref().unwrap_or_default().is_empty()
            || run.project.as_deref().unwrap_or_default().is_empty()
        {
            ineligible = Some("ad-hoc run".to_string());
        } else if run.definition_digest.is_empty()
            || run.extra_args_digest.is_empty()
        {
            ineligible = Some("missing definition digest".to_string());
        } else if run.settled_by.as_deref() != Some("wrapper") {
            ineligible = Some("not wrapper-settled".to_string());
        } else if !run.state.is_terminal() {
            ineligible = Some("run not terminal".to_string());
        } else if run.mutated_input != Some(false) {
            ineligible = Some("mutated input".to_string());
        }
        let (_before_digest, after_digest, fingerprints_ok) =
            match (&run.fingerprint_before, &run.fingerprint_after) {
                (Some(before), Some(after)) => {
                    if !before.completeness.complete
                        || !after.completeness.complete
                    {
                        (None, None, false)
                    } else {
                        let before_result =
                            canonicalize_tool_fingerprint(before.clone());
                        let after_result =
                            canonicalize_tool_fingerprint(after.clone());
                        match (before_result, after_result) {
                            (Ok(before_canon), Ok(after_canon)) => {
                                if before_canon.digest == after_canon.digest {
                                    (
                                        Some(before_canon.digest.clone()),
                                        Some(after_canon.digest),
                                        true,
                                    )
                                } else {
                                    (None, None, false)
                                }
                            }
                            _ => (None, None, false),
                        }
                    }
                }
                _ => (None, None, false),
            };
        if ineligible.is_none() && !fingerprints_ok {
            ineligible =
                Some("incomplete or mismatched fingerprints".to_string());
        }
        if ineligible.is_none() && !facts.has_run_row {
            ineligible = Some("missing triage".to_string());
        }
        if ineligible.is_none() {
            match (kind_opt.as_deref(), verdict_opt.as_deref()) {
                (Some("none"), Some("pass")) => {}
                (Some("verification"), Some("no_new_failures")) => {}
                _ => {
                    ineligible =
                        Some("kind or verdict not mintable".to_string());
                }
            }
        }
        if ineligible.is_none() {
            let verdict = verdict_opt.clone().unwrap_or_default();
            if !accept_list.iter().any(|token| token == &verdict) {
                ineligible = Some("verdict not in accept set".to_string());
            }
        }
        if ineligible.is_none()
            && verdict_opt.as_deref() == Some("no_new_failures")
            && !facts.triaged
        {
            ineligible = Some("no_new_failures without triage".to_string());
        }
        let project = run.project.clone().unwrap_or_default();
        let tool_name = run.tool_name.clone().unwrap_or_default();
        let fp_digest = after_digest.clone().unwrap_or_default();
        if ineligible.is_none() {
            let policy = normalized_policy.clone().unwrap();
            let ttl = ttl_seconds.unwrap();
            let verdict = verdict_opt.clone().unwrap();
            let after_fp = run.fingerprint_after.clone().unwrap();
            let canonical_after =
                canonicalize_tool_fingerprint(after_fp.clone())?;
            let proof = build_receipt_proof(&canonical_after.fingerprint)?;
            let proof_json = proof_to_json(&proof)?;
            let signature_refs = if verdict == "pass" {
                Vec::new()
            } else {
                let mut refs = Vec::new();
                for (extractor, version, signature, class) in &facts.items {
                    if class.as_deref() == Some("known")
                        || class.as_deref() == Some("flaky")
                    {
                        refs.push(ToolRunReceiptSignatureRefWire {
                            extractor: extractor.clone(),
                            extractor_version: *version,
                            signature: signature.clone(),
                        });
                    }
                }
                refs.sort_by(|left, right| {
                    left.extractor
                        .cmp(&right.extractor)
                        .then(
                            left.extractor_version
                                .cmp(&right.extractor_version),
                        )
                        .then(left.signature.cmp(&right.signature))
                });
                refs
            };
            let signature_refs_json = serde_json::to_string(&signature_refs)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            let accept_json = serde_json::to_string(&accept_list)
                .map_err(|error| ToolRunError::store(error.to_string()))?;
            let receipt_id = receipt_id_for_run(&request.run_id)?;
            let issue_ts = run.settled_ts.unwrap_or(now);
            let mint_ts = now;
            let expiry_ts = mint_ts.saturating_add(ttl);
            let previous = load_active_receipt(
                &tx,
                &project,
                &tool_name,
                &run.definition_digest,
                &run.extra_args_digest,
                &fp_digest,
            )?;
            let superseded_previous = previous.is_some();
            if let Some(previous) = previous {
                if previous.source_run_id == request.run_id {
                    let existing =
                        load_receipt_by_source(&tx, &request.run_id)?.unwrap();
                    return Ok(ToolRunReceiptSettleResultWire {
                        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                        run_id: request.run_id.clone(),
                        minted: false,
                        superseded: false,
                        receipt: Some(existing),
                        reason: None,
                        diagnostics: Vec::new(),
                    });
                }
                tx.execute(
                    "UPDATE tool_receipts SET status = 'superseded',
                        superseded_by_run_id = ?2, superseded_ts = ?3
                     WHERE receipt_id = ?1",
                    params![previous.receipt_id, request.run_id, now],
                )?;
            }
            let insert = tx.execute(
                "INSERT INTO tool_receipts(
                    receipt_id, source_run_id, project, tool_name,
                    definition_digest, extra_args_digest, fingerprint_digest,
                    verdict, signature_refs_json, proof_json, issue_ts,
                    mint_ts, expiry_ts, policy_version, ttl_seconds,
                    accept_json, status, superseded_by_run_id,
                    superseded_ts, explanation
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                           ?13, ?14, ?15, ?16, 'active', NULL, NULL, NULL)",
                params![
                    receipt_id,
                    request.run_id,
                    project,
                    tool_name,
                    run.definition_digest,
                    run.extra_args_digest,
                    fp_digest,
                    verdict,
                    signature_refs_json,
                    proof_json,
                    issue_ts,
                    mint_ts,
                    expiry_ts,
                    RECEIPT_POLICY_VERSION as i64,
                    ttl,
                    accept_json,
                ],
            );
            match insert {
                Ok(_) => {}
                Err(error) => {
                    let message = error.to_string();
                    if message.contains("UNIQUE constraint failed") {
                        let existing =
                            load_receipt_by_source(&tx, &request.run_id)?;
                        if let Some(existing) = existing {
                            return Ok(ToolRunReceiptSettleResultWire {
                                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                                run_id: request.run_id.clone(),
                                minted: false,
                                superseded: existing.status == "superseded",
                                receipt: Some(existing),
                                reason: None,
                                diagnostics: Vec::new(),
                            });
                        }
                    }
                    return Err(error.into());
                }
            }
            let receipt = ToolRunReceiptWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                receipt_id: receipt_id.clone(),
                source_run_id: request.run_id.clone(),
                project: project.clone(),
                tool_name: tool_name.clone(),
                definition_digest: run.definition_digest.clone(),
                extra_args_digest: run.extra_args_digest.clone(),
                fingerprint_digest: fp_digest.clone(),
                verdict: verdict.clone(),
                signature_refs: signature_refs.clone(),
                issue_ts,
                mint_ts,
                expiry_ts,
                policy_version: RECEIPT_POLICY_VERSION,
                ttl_seconds: ttl,
                accept: accept_list.clone(),
                status: "active".to_string(),
            };
            touch_write_meta(&tx, now)?;
            tx.commit()?;
            let _ = policy;
            return Ok(ToolRunReceiptSettleResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                run_id: request.run_id.clone(),
                minted: true,
                superseded: superseded_previous,
                receipt: Some(receipt),
                reason: None,
                diagnostics: Vec::new(),
            });
        }
        let reason = ineligible.unwrap_or_else(|| "ineligible".to_string());
        if run.state.is_terminal() {
            let candidate_digest = {
                let mut digest: Option<String> = None;
                if let Some(after) = run.fingerprint_after.as_ref() {
                    if after.completeness.complete {
                        digest = fingerprint_digest(after).ok();
                    }
                }
                if digest.is_none() {
                    if let Some(before) = run.fingerprint_before.as_ref() {
                        if before.completeness.complete {
                            digest = fingerprint_digest(before).ok();
                        }
                    }
                }
                digest
            };
            if let Some(candidate_digest) = candidate_digest {
                if let Some(active) = load_active_receipt(
                    &tx,
                    &project,
                    &tool_name,
                    &run.definition_digest,
                    &run.extra_args_digest,
                    &candidate_digest,
                )? {
                    let later = match run.settled_ts {
                        Some(settled) => {
                            if settled > active.issue_ts {
                                true
                            } else if settled == active.issue_ts {
                                request.run_id > active.source_run_id
                            } else {
                                false
                            }
                        }
                        None => false,
                    };
                    if later {
                        tx.execute(
                            "UPDATE tool_receipts SET status = 'superseded',
                                superseded_by_run_id = ?2, superseded_ts = ?3
                             WHERE receipt_id = ?1",
                            params![active.receipt_id, request.run_id, now],
                        )?;
                        touch_write_meta(&tx, now)?;
                        tx.commit()?;
                        return Ok(ToolRunReceiptSettleResultWire {
                            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                            run_id: request.run_id.clone(),
                            minted: false,
                            superseded: true,
                            receipt: None,
                            reason: Some(reason),
                            diagnostics: Vec::new(),
                        });
                    }
                }
            }
        }
        Ok(ToolRunReceiptSettleResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: request.run_id.clone(),
            minted: false,
            superseded: false,
            receipt: None,
            reason: Some(reason),
            diagnostics: Vec::new(),
        })
    })
}

#[allow(clippy::too_many_lines)]
pub fn receipt_lookup(
    store_path: &Path,
    request: ToolRunReceiptLookupRequestWire,
    busy_timeout: Duration,
) -> Result<ToolRunReceiptLookupResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let now = request.now_ts.unwrap_or_else(unix_now);
    let canonical = canonicalize_tool_fingerprint(request.fingerprint.clone())?;
    if !canonical.fingerprint.completeness.complete {
        return Ok(ToolRunReceiptLookupResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            outcome: ToolRunReceiptOutcomeWire::Refused,
            refusal: Some(ToolRunReceiptRefusalWire::IncompleteFingerprint),
            changed_paths: Vec::new(),
            paths_truncated: false,
            receipt: None,
            age_seconds: None,
            reason: Some("incomplete fingerprint".to_string()),
            diagnostics: Vec::new(),
        });
    }
    let current_digest = canonical.digest.clone();
    let accept = normalize_lookup_accept(&request.accept);
    if !store_path.exists() {
        return Ok(ToolRunReceiptLookupResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            outcome: ToolRunReceiptOutcomeWire::Refused,
            refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
            changed_paths: Vec::new(),
            paths_truncated: false,
            receipt: None,
            age_seconds: None,
            reason: Some("no receipt".to_string()),
            diagnostics: vec!["tool run store does not exist".to_string()],
        });
    }
    with_read_store(store_path, busy_timeout, |conn| {
        if !receipt_tables_present_inner(conn)? {
            return Ok(ToolRunReceiptLookupResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunReceiptOutcomeWire::Refused,
                refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
                changed_paths: Vec::new(),
                paths_truncated: false,
                receipt: None,
                age_seconds: None,
                reason: Some("no receipt".to_string()),
                diagnostics: Vec::new(),
            });
        }
        let scoped: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tool_receipts
             WHERE project = ?1 AND tool_name = ?2 AND extra_args_digest = ?3",
            params![
                request.project,
                request.tool_name,
                request.extra_args_digest
            ],
            |row| row.get(0),
        )?;
        if scoped > 0 {
            let matching: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tool_receipts
                 WHERE project = ?1 AND tool_name = ?2
                   AND extra_args_digest = ?3 AND definition_digest = ?4",
                params![
                    request.project,
                    request.tool_name,
                    request.extra_args_digest,
                    request.definition_digest
                ],
                |row| row.get(0),
            )?;
            if matching == 0 {
                return Ok(ToolRunReceiptLookupResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunReceiptOutcomeWire::Refused,
                    refusal: Some(ToolRunReceiptRefusalWire::DefinitionChanged),
                    changed_paths: Vec::new(),
                    paths_truncated: false,
                    receipt: None,
                    age_seconds: None,
                    reason: Some("definition changed".to_string()),
                    diagnostics: Vec::new(),
                });
            }
        }
        let definition_rows: i64 = conn.query_row(
            "SELECT COUNT(*) FROM tool_receipts
             WHERE project = ?1 AND tool_name = ?2
               AND definition_digest = ?3 AND extra_args_digest = ?4",
            params![
                request.project,
                request.tool_name,
                request.definition_digest,
                request.extra_args_digest
            ],
            |row| row.get(0),
        )?;
        if definition_rows > 0 {
            let fingerprint_rows: i64 = conn.query_row(
                "SELECT COUNT(*) FROM tool_receipts
                 WHERE project = ?1 AND tool_name = ?2
                   AND definition_digest = ?3 AND extra_args_digest = ?4
                   AND fingerprint_digest = ?5",
                params![
                    request.project,
                    request.tool_name,
                    request.definition_digest,
                    request.extra_args_digest,
                    current_digest
                ],
                |row| row.get(0),
            )?;
            if fingerprint_rows == 0 {
                let newest_proof: Option<String> = conn
                    .query_row(
                        "SELECT proof_json FROM tool_receipts
                         WHERE project = ?1 AND tool_name = ?2
                           AND definition_digest = ?3
                           AND extra_args_digest = ?4
                         ORDER BY mint_ts DESC, receipt_id DESC LIMIT 1",
                        params![
                            request.project,
                            request.tool_name,
                            request.definition_digest,
                            request.extra_args_digest
                        ],
                        |row| row.get(0),
                    )
                    .optional()?;
                let mut changed = Vec::new();
                if let Some(raw) = newest_proof {
                    if let Ok(proof) = proof_from_json(&raw) {
                        changed = diff_proof_against_fingerprint(
                            &proof,
                            &canonical.fingerprint,
                        );
                    }
                }
                changed.sort();
                changed.dedup();
                let truncated = changed.len() > RECEIPT_MAX_CHANGED_PATHS;
                if truncated {
                    changed.truncate(RECEIPT_MAX_CHANGED_PATHS);
                }
                return Ok(ToolRunReceiptLookupResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunReceiptOutcomeWire::Refused,
                    refusal: Some(
                        ToolRunReceiptRefusalWire::FingerprintChanged,
                    ),
                    changed_paths: changed,
                    paths_truncated: truncated,
                    receipt: None,
                    age_seconds: None,
                    reason: Some("fingerprint changed".to_string()),
                    diagnostics: Vec::new(),
                });
            }
        }
        let full_rows: Vec<ReceiptFullRowTuple> = {
            let mut stmt = conn.prepare(
                "SELECT receipt_id, source_run_id, project, tool_name,
                        definition_digest, extra_args_digest,
                        fingerprint_digest, verdict, signature_refs_json,
                        issue_ts, mint_ts, expiry_ts, policy_version,
                        ttl_seconds, accept_json, status, explanation
                 FROM tool_receipts
                 WHERE project = ?1 AND tool_name = ?2
                   AND definition_digest = ?3 AND extra_args_digest = ?4
                   AND fingerprint_digest = ?5",
            )?;
            let rows = stmt.query_map(
                params![
                    request.project,
                    request.tool_name,
                    request.definition_digest,
                    request.extra_args_digest,
                    current_digest
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                        row.get(12)?,
                        row.get(13)?,
                        row.get(14)?,
                        row.get(15)?,
                        row.get(16)?,
                    ))
                },
            )?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            out
        };
        if full_rows.is_empty() {
            return Ok(ToolRunReceiptLookupResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunReceiptOutcomeWire::Refused,
                refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
                changed_paths: Vec::new(),
                paths_truncated: false,
                receipt: None,
                age_seconds: None,
                reason: Some("no receipt".to_string()),
                diagnostics: Vec::new(),
            });
        }
        let tombstone_explanation = full_rows
            .iter()
            .filter(|row| row.15 == "tombstone")
            .filter_map(|row| row.16.clone())
            .next();
        let has_active = full_rows.iter().any(|row| row.15 == "active");
        let has_superseded = full_rows.iter().any(|row| row.15 == "superseded");
        if !has_active {
            if has_superseded {
                return Ok(ToolRunReceiptLookupResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunReceiptOutcomeWire::Refused,
                    refusal: Some(
                        ToolRunReceiptRefusalWire::InvalidatedByLaterRun,
                    ),
                    changed_paths: Vec::new(),
                    paths_truncated: false,
                    receipt: None,
                    age_seconds: None,
                    reason: Some("invalidated by later run".to_string()),
                    diagnostics: Vec::new(),
                });
            }
            return Ok(ToolRunReceiptLookupResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunReceiptOutcomeWire::Refused,
                refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
                changed_paths: Vec::new(),
                paths_truncated: false,
                receipt: None,
                age_seconds: None,
                reason: tombstone_explanation
                    .or_else(|| Some("no receipt".to_string())),
                diagnostics: Vec::new(),
            });
        }
        let active = full_rows.iter().find(|row| row.15 == "active").unwrap();
        let receipt_tuple: ReceiptRowTuple = (
            active.0.clone(),
            active.1.clone(),
            active.2.clone(),
            active.3.clone(),
            active.4.clone(),
            active.5.clone(),
            active.6.clone(),
            active.7.clone(),
            active.8.clone(),
            active.9,
            active.10,
            active.11,
            active.12,
            active.13,
            active.14.clone(),
            "active".to_string(),
        );
        let receipt = match row_to_receipt(receipt_tuple) {
            Ok(receipt) => receipt,
            Err(_) => {
                return Ok(ToolRunReceiptLookupResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunReceiptOutcomeWire::Refused,
                    refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
                    changed_paths: Vec::new(),
                    paths_truncated: false,
                    receipt: None,
                    age_seconds: None,
                    reason: Some("receipt proof unreadable".to_string()),
                    diagnostics: Vec::new(),
                });
            }
        };
        let proof_raw: Option<String> = conn
            .query_row(
                "SELECT proof_json FROM tool_receipts WHERE receipt_id = ?1",
                [&receipt.receipt_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        if let Some(raw) = proof_raw {
            if proof_from_json(&raw).is_err() {
                return Ok(ToolRunReceiptLookupResultWire {
                    schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                    outcome: ToolRunReceiptOutcomeWire::Refused,
                    refusal: Some(ToolRunReceiptRefusalWire::NoReceipt),
                    changed_paths: Vec::new(),
                    paths_truncated: false,
                    receipt: None,
                    age_seconds: None,
                    reason: Some("receipt proof unreadable".to_string()),
                    diagnostics: Vec::new(),
                });
            }
        }
        if receipt.expiry_ts <= now {
            return Ok(ToolRunReceiptLookupResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunReceiptOutcomeWire::Refused,
                refusal: Some(ToolRunReceiptRefusalWire::Expired),
                changed_paths: Vec::new(),
                paths_truncated: false,
                receipt: None,
                age_seconds: None,
                reason: Some("expired".to_string()),
                diagnostics: Vec::new(),
            });
        }
        if receipt.policy_version != RECEIPT_POLICY_VERSION
            || !accept.iter().any(|token| token == &receipt.verdict)
        {
            return Ok(ToolRunReceiptLookupResultWire {
                schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
                outcome: ToolRunReceiptOutcomeWire::Refused,
                refusal: Some(ToolRunReceiptRefusalWire::VerdictInsufficient),
                changed_paths: Vec::new(),
                paths_truncated: false,
                receipt: None,
                age_seconds: None,
                reason: Some("verdict insufficient".to_string()),
                diagnostics: Vec::new(),
            });
        }
        let age = now.saturating_sub(receipt.mint_ts);
        Ok(ToolRunReceiptLookupResultWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            outcome: ToolRunReceiptOutcomeWire::Covered,
            refusal: None,
            changed_paths: Vec::new(),
            paths_truncated: false,
            receipt: Some(receipt),
            age_seconds: Some(age),
            reason: None,
            diagnostics: Vec::new(),
        })
    })
}

pub fn count_deletable_receipts(
    conn: &Connection,
    summary_cut: i64,
    now: i64,
) -> Result<u64, ToolRunError> {
    if !receipt_tables_present_inner(conn)? {
        return Ok(0);
    }
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM tool_receipts
         WHERE (status IN ('superseded', 'tombstone') OR expiry_ts <= ?1)
           AND ((status = 'superseded'
                 AND superseded_ts IS NOT NULL AND superseded_ts < ?2)
                OR (status != 'superseded' AND expiry_ts < ?2))",
        params![now, summary_cut],
        |row| row.get(0),
    )?;
    Ok(count as u64)
}

pub fn delete_deletable_receipts(
    conn: &Connection,
    summary_cut: i64,
    now: i64,
) -> Result<u64, ToolRunError> {
    if !receipt_tables_present_inner(conn)? {
        return Ok(0);
    }
    let changed = conn.execute(
        "DELETE FROM tool_receipts
         WHERE (status IN ('superseded', 'tombstone') OR expiry_ts <= ?1)
           AND ((status = 'superseded'
                 AND superseded_ts IS NOT NULL AND superseded_ts < ?2)
                OR (status != 'superseded' AND expiry_ts < ?2))",
        params![now, summary_cut],
    )?;
    Ok(changed as u64)
}

pub fn tombstone_unreadable_for_runs(
    conn: &Connection,
    run_ids: &[String],
) -> Result<(), ToolRunError> {
    if run_ids.is_empty() || !receipt_tables_present_inner(conn)? {
        return Ok(());
    }
    for run_id in run_ids {
        let rows: Vec<(String, String)> = {
            let mut stmt = conn.prepare(
                "SELECT receipt_id, proof_json FROM tool_receipts
                 WHERE source_run_id = ?1 AND status = 'active'",
            )?;
            let mapped = stmt.query_map([run_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut out = Vec::new();
            for row in mapped {
                out.push(row?);
            }
            out
        };
        for (receipt_id, proof_json) in rows {
            if proof_from_json(&proof_json).is_err() {
                conn.execute(
                    "UPDATE tool_receipts SET status = 'tombstone',
                        explanation = 'source run pruned; receipt proof unreadable'
                     WHERE receipt_id = ?1",
                    [receipt_id],
                )?;
            }
        }
    }
    Ok(())
}

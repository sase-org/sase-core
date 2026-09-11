//! Outcome-aware evidence selection for continuation context.

use serde::{Deserialize, Serialize};

use super::schema::{
    validate_diagnostic_manifest, validate_monitor_result, validate_schema,
    DiagnosticManifestWire, DiagnosticStageStatusWire, MonitorOutcomeWire,
    MonitorResultWire, CONTINUATION_WIRE_SCHEMA_VERSION,
};
use super::ContinuationError;

pub const DEFAULT_SELECTED_DIAGNOSTICS_BYTES: u64 = 8 * 1024;
pub const DEFAULT_FALLBACK_TAIL_BYTES: u64 = 4 * 1024;
pub const DEFAULT_TOTAL_RAW_EXCERPT_BYTES: u64 = 12 * 1024;
pub const DEFAULT_RAW_TAIL_LINES: u32 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationEvidencePolicyWire {
    Auto,
    Tail,
    File,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContinuationEvidenceContextKindWire {
    FactsOnly,
    FailedDiagnostics,
    TimeoutTail,
    RecoveryRefs,
    Tail,
    FileRefs,
    RetrievalOnly,
    HistoricalFacts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationEvidenceLimitsWire {
    pub selected_diagnostics_bytes: u64,
    pub fallback_tail_bytes: u64,
    pub total_raw_excerpt_bytes: u64,
    pub raw_tail_lines: u32,
}

impl Default for ContinuationEvidenceLimitsWire {
    fn default() -> Self {
        Self {
            selected_diagnostics_bytes: DEFAULT_SELECTED_DIAGNOSTICS_BYTES,
            fallback_tail_bytes: DEFAULT_FALLBACK_TAIL_BYTES,
            total_raw_excerpt_bytes: DEFAULT_TOTAL_RAW_EXCERPT_BYTES,
            raw_tail_lines: DEFAULT_RAW_TAIL_LINES,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationEvidenceSelectionRequestWire {
    pub schema_version: u32,
    pub result: MonitorResultWire,
    pub policy: ContinuationEvidencePolicyWire,
    #[serde(default)]
    pub historical_result: bool,
    #[serde(default)]
    pub diagnostic_manifest: Option<DiagnosticManifestWire>,
    #[serde(default)]
    pub limits: ContinuationEvidenceLimitsWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContinuationEvidenceSelectionWire {
    pub schema_version: u32,
    pub policy: ContinuationEvidencePolicyWire,
    pub outcome: MonitorOutcomeWire,
    pub context_kind: ContinuationEvidenceContextKindWire,
    pub include_raw_excerpt: bool,
    pub max_embedded_bytes: u64,
    pub max_tail_lines: u32,
    #[serde(default)]
    pub selected_refs: Vec<String>,
    #[serde(default)]
    pub diagnostic_stage_ids: Vec<String>,
    #[serde(default)]
    pub log_locators: Vec<String>,
    #[serde(default)]
    pub reasons: Vec<String>,
    #[serde(default)]
    pub omissions: Vec<String>,
}

pub fn select_continuation_evidence(
    request: ContinuationEvidenceSelectionRequestWire,
) -> Result<ContinuationEvidenceSelectionWire, ContinuationError> {
    validate_schema(
        request.schema_version,
        "ContinuationEvidenceSelectionRequestWire",
    )?;
    let result = validate_monitor_result(request.result)?;
    let diagnostic_manifest = request
        .diagnostic_manifest
        .map(validate_diagnostic_manifest)
        .transpose()?;
    validate_limits(&request.limits)?;

    if request.historical_result {
        let mut selected_refs = compact_result_refs(&result);
        selected_refs.sort();
        selected_refs.dedup();
        return Ok(selection(
            request.policy,
            result.outcome,
            ContinuationEvidenceContextKindWire::HistoricalFacts,
            false,
            0,
            0,
            selected_refs,
            vec![],
            vec![],
            vec!["historical_result_suppresses_raw_excerpt".to_string()],
            vec![],
        ));
    }

    match request.policy {
        ContinuationEvidencePolicyWire::Auto => {
            auto_selection(result, diagnostic_manifest, request.limits)
        }
        ContinuationEvidencePolicyWire::Tail => Ok(selection(
            ContinuationEvidencePolicyWire::Tail,
            result.outcome,
            ContinuationEvidenceContextKindWire::Tail,
            true,
            request.limits.total_raw_excerpt_bytes,
            request.limits.raw_tail_lines,
            compact_result_refs(&result),
            vec![],
            log_locators(&result),
            vec!["tail_policy_embeds_bounded_retained_output".to_string()],
            vec![],
        )),
        ContinuationEvidencePolicyWire::File => Ok(selection(
            ContinuationEvidencePolicyWire::File,
            result.outcome,
            ContinuationEvidenceContextKindWire::FileRefs,
            false,
            0,
            0,
            compact_result_refs(&result),
            vec![],
            log_locators(&result),
            vec!["file_policy_exposes_refs_without_raw_output".to_string()],
            vec![],
        )),
        ContinuationEvidencePolicyWire::None => Ok(selection(
            ContinuationEvidencePolicyWire::None,
            result.outcome,
            ContinuationEvidenceContextKindWire::RetrievalOnly,
            false,
            0,
            0,
            compact_result_refs(&result),
            vec![],
            vec![],
            vec!["none_policy_omits_raw_output".to_string()],
            vec![],
        )),
    }
}

fn auto_selection(
    result: MonitorResultWire,
    diagnostic_manifest: Option<DiagnosticManifestWire>,
    limits: ContinuationEvidenceLimitsWire,
) -> Result<ContinuationEvidenceSelectionWire, ContinuationError> {
    match result.outcome {
        MonitorOutcomeWire::Completed => Ok(selection(
            ContinuationEvidencePolicyWire::Auto,
            result.outcome,
            ContinuationEvidenceContextKindWire::FactsOnly,
            false,
            0,
            0,
            compact_result_refs(&result),
            vec![],
            vec![],
            vec!["completed_auto_uses_host_facts_and_refs".to_string()],
            vec![],
        )),
        MonitorOutcomeWire::Failed => {
            let (stage_ids, diagnostic_refs, omissions) =
                failed_diagnostics(diagnostic_manifest.as_ref());
            let include_fallback = diagnostic_refs.is_empty();
            let mut selected_refs = compact_result_refs(&result);
            selected_refs.extend(diagnostic_refs);
            selected_refs.sort();
            selected_refs.dedup();
            Ok(selection(
                ContinuationEvidencePolicyWire::Auto,
                result.outcome,
                ContinuationEvidenceContextKindWire::FailedDiagnostics,
                include_fallback,
                if include_fallback {
                    limits.fallback_tail_bytes
                } else {
                    limits.selected_diagnostics_bytes
                },
                if include_fallback {
                    limits.raw_tail_lines
                } else {
                    0
                },
                selected_refs,
                stage_ids,
                if include_fallback {
                    log_locators(&result)
                } else {
                    vec![]
                },
                vec!["failed_auto_prefers_stage_diagnostics".to_string()],
                omissions,
            ))
        }
        MonitorOutcomeWire::Timeout => Ok(selection(
            ContinuationEvidencePolicyWire::Auto,
            result.outcome,
            ContinuationEvidenceContextKindWire::TimeoutTail,
            true,
            limits.fallback_tail_bytes,
            limits.raw_tail_lines,
            compact_result_refs(&result),
            vec![],
            log_locators(&result),
            vec!["timeout_auto_includes_bounded_tail".to_string()],
            vec![],
        )),
        MonitorOutcomeWire::Stopped | MonitorOutcomeWire::Lost => {
            Ok(selection(
                ContinuationEvidencePolicyWire::Auto,
                result.outcome,
                ContinuationEvidenceContextKindWire::RecoveryRefs,
                false,
                0,
                0,
                compact_result_refs(&result),
                vec![],
                log_locators(&result),
                vec!["cancelled_or_lost_auto_does_not_launch_successor"
                    .to_string()],
                vec![],
            ))
        }
        MonitorOutcomeWire::Unknown => Ok(selection(
            ContinuationEvidencePolicyWire::Auto,
            result.outcome,
            ContinuationEvidenceContextKindWire::FactsOnly,
            false,
            0,
            0,
            compact_result_refs(&result),
            vec![],
            log_locators(&result),
            vec!["unknown_outcome_uses_facts_only".to_string()],
            vec![],
        )),
    }
}

fn failed_diagnostics(
    manifest: Option<&DiagnosticManifestWire>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let Some(manifest) = manifest else {
        return (
            vec![],
            vec![],
            vec!["missing_diagnostic_manifest".to_string()],
        );
    };

    let mut stage_ids = Vec::new();
    let mut refs = Vec::new();
    let mut omissions = Vec::new();
    for stage in &manifest.stages {
        if matches!(
            stage.status,
            DiagnosticStageStatusWire::Failed
                | DiagnosticStageStatusWire::Error
                | DiagnosticStageStatusWire::Unknown
        ) {
            stage_ids.push(stage.stage_id.clone());
            refs.extend(stage.diagnostic_refs.clone());
            if stage.diagnostic_refs.is_empty() {
                omissions.push(format!(
                    "stage {} has no diagnostic refs",
                    stage.stage_id
                ));
            }
            omissions.extend(stage.capture_errors.clone());
        }
    }
    if stage_ids.is_empty() {
        omissions
            .push("failed result had no failed diagnostic stage".to_string());
    }
    (stage_ids, refs, omissions)
}

fn compact_result_refs(result: &MonitorResultWire) -> Vec<String> {
    let mut refs = Vec::new();
    if let Some(reference) = &result.diagnostic_manifest_ref {
        refs.push(reference.clone());
    }
    if let Some(reference) = &result.retained_log.log_ref {
        refs.push(reference.clone());
    }
    refs
}

fn log_locators(result: &MonitorResultWire) -> Vec<String> {
    let mut locators = Vec::new();
    if let Some(locator) = &result.retained_log.local_locator {
        locators.push(locator.clone());
    }
    if let Some(reference) = &result.retained_log.log_ref {
        locators.push(reference.clone());
    }
    locators
}

#[allow(clippy::too_many_arguments)]
fn selection(
    policy: ContinuationEvidencePolicyWire,
    outcome: MonitorOutcomeWire,
    context_kind: ContinuationEvidenceContextKindWire,
    include_raw_excerpt: bool,
    max_embedded_bytes: u64,
    max_tail_lines: u32,
    selected_refs: Vec<String>,
    diagnostic_stage_ids: Vec<String>,
    log_locators: Vec<String>,
    reasons: Vec<String>,
    omissions: Vec<String>,
) -> ContinuationEvidenceSelectionWire {
    ContinuationEvidenceSelectionWire {
        schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
        policy,
        outcome,
        context_kind,
        include_raw_excerpt,
        max_embedded_bytes,
        max_tail_lines,
        selected_refs,
        diagnostic_stage_ids,
        log_locators,
        reasons,
        omissions,
    }
}

fn validate_limits(
    limits: &ContinuationEvidenceLimitsWire,
) -> Result<(), ContinuationError> {
    if limits.selected_diagnostics_bytes > limits.total_raw_excerpt_bytes {
        return Err(ContinuationError::validation(
            "selected_diagnostics_bytes must not exceed total_raw_excerpt_bytes",
        ));
    }
    if limits.fallback_tail_bytes > limits.total_raw_excerpt_bytes {
        return Err(ContinuationError::validation(
            "fallback_tail_bytes must not exceed total_raw_excerpt_bytes",
        ));
    }
    if limits.raw_tail_lines == 0 {
        return Err(ContinuationError::validation(
            "raw_tail_lines must be greater than zero",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuation::schema::{
        ContinuationByteRangeWire, DiagnosticStageWire, RetainedLogMetadataWire,
    };

    fn result(
        outcome: MonitorOutcomeWire,
        exit_code: Option<i32>,
    ) -> MonitorResultWire {
        MonitorResultWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            result_id: "result-1".to_string(),
            monitor_id: "monitor-1".to_string(),
            starter_execution_id: "run-1".to_string(),
            outcome,
            exit_code,
            command: vec!["just".to_string(), "check".to_string()],
            cwd: "/repo".to_string(),
            started_at: "2026-09-11T10:00:00Z".to_string(),
            ended_at: Some("2026-09-11T10:01:00Z".to_string()),
            elapsed_ms: Some(60_000),
            timeout_kind: None,
            timeout_budget_ms: None,
            workspace_identity: "workspace-1".to_string(),
            diagnostic_manifest_ref: Some("file:explicit:manifest".to_string()),
            retained_log: RetainedLogMetadataWire {
                log_ref: Some("file:explicit:log".to_string()),
                local_locator: Some("monitor://monitor-1/log".to_string()),
                total_observed_bytes: Some(100),
                retained_ranges: vec![ContinuationByteRangeWire {
                    start: 0,
                    end: 100,
                }],
                complete: true,
                drain_confirmed: true,
            },
        }
    }

    #[test]
    fn auto_completed_uses_facts_without_raw_log() {
        let selection = select_continuation_evidence(
            ContinuationEvidenceSelectionRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                result: result(MonitorOutcomeWire::Completed, Some(0)),
                policy: ContinuationEvidencePolicyWire::Auto,
                historical_result: false,
                diagnostic_manifest: None,
                limits: ContinuationEvidenceLimitsWire::default(),
            },
        )
        .unwrap();

        assert_eq!(
            selection.context_kind,
            ContinuationEvidenceContextKindWire::FactsOnly
        );
        assert!(!selection.include_raw_excerpt);
    }

    #[test]
    fn auto_failed_uses_stage_diagnostics() {
        let manifest = DiagnosticManifestWire {
            schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
            producer: "run-silent".to_string(),
            stages: vec![DiagnosticStageWire {
                stage_id: "mypy".to_string(),
                name: "Type checking".to_string(),
                status: DiagnosticStageStatusWire::Failed,
                exit_code: Some(1),
                diagnostic_refs: vec!["file:explicit:mypy".to_string()],
                counts: Default::default(),
                retained_ranges: vec![],
                capture_errors: vec![],
            }],
            complete: true,
            manifest_ref: Some("file:explicit:manifest".to_string()),
        };

        let selection = select_continuation_evidence(
            ContinuationEvidenceSelectionRequestWire {
                schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                result: result(MonitorOutcomeWire::Failed, Some(1)),
                policy: ContinuationEvidencePolicyWire::Auto,
                historical_result: false,
                diagnostic_manifest: Some(manifest),
                limits: ContinuationEvidenceLimitsWire::default(),
            },
        )
        .unwrap();

        assert_eq!(
            selection.context_kind,
            ContinuationEvidenceContextKindWire::FailedDiagnostics
        );
        assert!(!selection.include_raw_excerpt);
        assert_eq!(selection.diagnostic_stage_ids, vec!["mypy"]);
        assert!(selection
            .selected_refs
            .contains(&"file:explicit:mypy".to_string()));
    }

    #[test]
    fn strict_file_and_none_policies_never_embed_raw_output() {
        for policy in [
            ContinuationEvidencePolicyWire::File,
            ContinuationEvidencePolicyWire::None,
        ] {
            let selection = select_continuation_evidence(
                ContinuationEvidenceSelectionRequestWire {
                    schema_version: CONTINUATION_WIRE_SCHEMA_VERSION,
                    result: result(MonitorOutcomeWire::Failed, Some(1)),
                    policy,
                    historical_result: false,
                    diagnostic_manifest: None,
                    limits: ContinuationEvidenceLimitsWire::default(),
                },
            )
            .unwrap();
            assert!(!selection.include_raw_excerpt);
        }
    }
}

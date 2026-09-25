//! Pure witness-based NEW/KNOWN/FLAKY/UNKNOWN classification.
//!
//! Deterministic under any reordering of set inputs: subjects, evidence,
//! baselines, and owner candidates are canonicalized before evaluation.
//! Ancestry order is meaningful (newest first) and is preserved.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::super::wire::TOOL_RUN_WIRE_SCHEMA_VERSION;
use super::super::ToolRunError;
use super::wire::ToolRunTriageClassWire;

fn schema_version() -> u32 {
    TOOL_RUN_WIRE_SCHEMA_VERSION
}

fn default_min_witnesses() -> u32 {
    1
}

/// Rule version stamped on every label.
pub const TOOL_RUN_TRIAGE_RULE_VERSION: u32 = 1;
/// Seven-day evidence lookback in seconds.
pub const TOOL_RUN_TRIAGE_LOOKBACK_SECS: i64 = 7 * 24 * 3600;
/// Extractors whose same-fingerprint disagreement implies flake.
pub const TOOL_RUN_TRIAGE_TEST_EXTRACTORS: [&str; 2] = ["pytest", "cargo_test"];

fn is_test_extractor(name: &str) -> bool {
    TOOL_RUN_TRIAGE_TEST_EXTRACTORS.contains(&name)
}

fn is_hex64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunTriageKnobsWire {
    #[serde(default = "default_min_witnesses")]
    pub min_witnesses: u32,
    #[serde(default)]
    pub touched_requires_clean_witness: bool,
}

impl Default for ToolRunTriageKnobsWire {
    fn default() -> Self {
        Self {
            min_witnesses: 1,
            touched_requires_clean_witness: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageSubjectItemWire {
    pub stage_key: String,
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    #[serde(default)]
    pub locator_paths: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageSubjectRunWire {
    pub run_id: String,
    pub project: String,
    pub tool: String,
    pub extra_args_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_head: Option<String>,
    #[serde(default)]
    pub dirty_paths: Vec<String>,
    pub complete_fingerprint: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_digest: Option<String>,
    #[serde(default)]
    pub ad_hoc: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolRunTriageEvidenceItemWire {
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageEvidenceRunWire {
    pub run_id: String,
    pub project: String,
    pub tool: String,
    pub extra_args_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine: Option<String>,
    pub settled_ts: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_head: Option<String>,
    #[serde(default)]
    pub complete_fingerprint: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fingerprint_digest: Option<String>,
    #[serde(default)]
    pub dirty_paths: Vec<String>,
    #[serde(default)]
    pub dirty_unknown: bool,
    #[serde(default)]
    pub clean_tree: bool,
    #[serde(default)]
    pub ad_hoc: bool,
    #[serde(default)]
    pub failed: bool,
    #[serde(default)]
    pub stage_completions: Vec<String>,
    #[serde(default)]
    pub items: Vec<ToolRunTriageEvidenceItemWire>,
    /// True for selection-health full-run records (changed_files semantics).
    #[serde(default)]
    pub selection_source: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageFlakeEntryWire {
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageOwnerCandidateWire {
    pub node_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// "open" or "closed".
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolRunTriageClassifyRequestWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub subject_run: ToolRunTriageSubjectRunWire,
    #[serde(default)]
    pub subjects: Vec<ToolRunTriageSubjectItemWire>,
    #[serde(default)]
    pub evidence_runs: Vec<ToolRunTriageEvidenceRunWire>,
    #[serde(default)]
    pub selection_records: Vec<ToolRunTriageEvidenceRunWire>,
    /// First-parent ancestry newest first, including base(R) at index 0.
    #[serde(default)]
    pub ancestry: Vec<String>,
    #[serde(default)]
    pub flake_baseline: Vec<ToolRunTriageFlakeEntryWire>,
    #[serde(default)]
    pub owner_candidates: Vec<ToolRunTriageOwnerCandidateWire>,
    #[serde(default)]
    pub knobs: ToolRunTriageKnobsWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub now_ts: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageClassifyLabelWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    pub stage_key: String,
    pub extractor: String,
    pub extractor_version: u32,
    pub signature: String,
    pub class: ToolRunTriageClassWire,
    pub touched: bool,
    pub rule_version: u32,
    pub knobs: serde_json::Value,
    pub evidence: serde_json::Value,
    #[serde(default)]
    pub possible_owners: serde_json::Value,
    pub classified_ts: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolRunTriageClassifyResultWire {
    #[serde(default = "schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub labels: Vec<ToolRunTriageClassifyLabelWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repeat_of: Option<String>,
    #[serde(default)]
    pub diagnostics: Vec<String>,
}

fn validate_schema(version: u32) -> Result<(), ToolRunError> {
    if version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: version,
        });
    }
    Ok(())
}

fn locator_tokens(paths: &[String]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for path in paths {
        for token in path
            .split(|c: char| !c.is_ascii_alphanumeric())
            .map(str::to_lowercase)
            .filter(|token| token.len() >= 3)
        {
            out.insert(token);
        }
        // File stem without extension is a strong signal.
        if let Some(stem) = path.rsplit('/').next() {
            let stem = stem.split('.').next().unwrap_or(stem).to_lowercase();
            if stem.len() >= 3 {
                out.insert(stem);
            }
        }
    }
    out
}

fn candidate_text(candidate: &ToolRunTriageOwnerCandidateWire) -> String {
    let mut parts = vec![candidate.node_id.clone()];
    if let Some(location) = candidate.location.as_deref() {
        parts.push(location.to_string());
    }
    if let Some(title) = candidate.title.as_deref() {
        parts.push(title.to_string());
    }
    parts.join(" ").to_lowercase()
}

fn match_owners(
    locator_paths: &[String],
    candidates: &[ToolRunTriageOwnerCandidateWire],
    now_ts: i64,
) -> Vec<serde_json::Value> {
    let tokens = locator_tokens(locator_paths);
    if tokens.is_empty() || candidates.is_empty() {
        return Vec::new();
    }
    let mut scored: Vec<(u8, &ToolRunTriageOwnerCandidateWire)> = Vec::new();
    for candidate in candidates {
        // Never match by bead/title alone without a locator token hit:
        // require a token from the item's locators to appear in the
        // candidate's id/location/title text.
        let text = candidate_text(candidate);
        let hit = tokens.iter().any(|token| text.contains(token.as_str()));
        if !hit {
            continue;
        }
        let status = candidate.status.to_lowercase();
        if status == "open" {
            scored.push((0, candidate));
        } else if status == "closed" {
            // Only recently closed fixes are suggested.
            let recent = candidate.closed_ts.is_none_or(|ts| {
                now_ts.saturating_sub(ts) <= TOOL_RUN_TRIAGE_LOOKBACK_SECS
            });
            if recent {
                scored.push((1, candidate));
            }
        }
    }
    scored.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then(left.1.node_id.cmp(&right.1.node_id))
    });
    scored
        .into_iter()
        .take(2)
        .map(|(_, candidate)| {
            let status = candidate.status.to_lowercase();
            let reason = if status == "open" {
                "possible owner"
            } else {
                "possibly fixed"
            };
            serde_json::json!({
                "id": candidate.node_id,
                "status": status,
                "reason": reason,
            })
        })
        .collect()
}

fn paths_touch(locators: &[String], dirty: &[String]) -> bool {
    for locator in locators {
        for dirt in dirty {
            if locator == dirt
                || locator.starts_with(&format!("{dirt}/"))
                || dirt.starts_with(&format!("{locator}/"))
            {
                return true;
            }
        }
    }
    false
}

fn evidence_key(
    extractor: &str,
    version: u32,
    signature: &str,
) -> (String, u32, String) {
    (extractor.to_string(), version, signature.to_string())
}

fn contains_triple(
    items: &[ToolRunTriageEvidenceItemWire],
    extractor: &str,
    version: u32,
    signature: &str,
) -> bool {
    items.iter().any(|item| {
        item.extractor == extractor
            && item.extractor_version == version
            && item.signature == signature
    })
}

/// Pure witness-based classification. See the E3 contract for rule order.
pub fn tool_run_triage_classify(
    request: ToolRunTriageClassifyRequestWire,
) -> Result<ToolRunTriageClassifyResultWire, ToolRunError> {
    validate_schema(request.schema_version)?;
    let subject = &request.subject_run;
    if subject.run_id.trim().is_empty() {
        return Err(ToolRunError::invalid("subject run_id must not be empty"));
    }
    if subject.project.trim().is_empty() || subject.tool.trim().is_empty() {
        return Err(ToolRunError::invalid(
            "subject project and tool must not be empty",
        ));
    }
    if !subject.complete_fingerprint {
        return Err(ToolRunError::invalid("subject fingerprint is incomplete"));
    }
    if subject
        .fingerprint_digest
        .as_deref()
        .is_none_or(str::is_empty)
    {
        return Err(ToolRunError::invalid(
            "subject fingerprint digest is missing",
        ));
    }
    if request.knobs.min_witnesses < 1 {
        return Err(ToolRunError::invalid("min_witnesses must be >= 1"));
    }
    // Canonicalize set inputs; ancestry order is preserved.
    let mut subjects = request.subjects.clone();
    for item in &subjects {
        if item.stage_key.trim().is_empty() {
            return Err(ToolRunError::invalid(
                "subject stage_key must not be empty",
            ));
        }
        if item.extractor.trim().is_empty() {
            return Err(ToolRunError::invalid(
                "subject extractor must not be empty",
            ));
        }
        if item.extractor_version < 1 {
            return Err(ToolRunError::invalid(
                "subject extractor_version must be >= 1",
            ));
        }
        if !is_hex64(&item.signature) {
            return Err(ToolRunError::invalid(
                "subject signature must be 64 lowercase hex",
            ));
        }
        for path in &item.locator_paths {
            if path.starts_with('/') {
                return Err(ToolRunError::invalid(
                    "subject locator path must not be absolute",
                ));
            }
        }
    }
    subjects.sort_by(|left, right| {
        left.stage_key
            .cmp(&right.stage_key)
            .then(left.extractor.cmp(&right.extractor))
            .then(left.extractor_version.cmp(&right.extractor_version))
            .then(left.signature.cmp(&right.signature))
    });
    let mut evidence = request.evidence_runs.clone();
    evidence.extend(request.selection_records.clone());
    // Refuse ad-hoc, incomplete, cross-identity, or self evidence by
    // excluding it from witness consideration (never KNOWN from it).
    // Cross-version signature collisions remain hard errors, mirroring
    // compare_triage_signatures.
    evidence.retain(|run| {
        !run.ad_hoc
            && run.complete_fingerprint
            && run.project == subject.project
            && run.tool == subject.tool
            && run.extra_args_digest == subject.extra_args_digest
            && run.run_id != subject.run_id
    });
    // Cross-version signature collision: same 64-hex digest claimed under a
    // different (extractor, version) than the subject's triple is refused,
    // mirroring compare_triage_signatures.
    for item in &subjects {
        for run in &evidence {
            for evidence_item in &run.items {
                if evidence_item.signature == item.signature
                    && (evidence_item.extractor != item.extractor
                        || evidence_item.extractor_version
                            != item.extractor_version)
                {
                    return Err(ToolRunError::invalid(
                        "triage cross-version signatures are not comparable",
                    ));
                }
            }
        }
        for entry in &request.flake_baseline {
            if entry.signature == item.signature
                && (entry.extractor != item.extractor
                    || entry.extractor_version != item.extractor_version)
            {
                return Err(ToolRunError::invalid(
                    "triage cross-version signatures are not comparable",
                ));
            }
        }
    }
    evidence.sort_by(|left, right| left.run_id.cmp(&right.run_id));
    let mut baseline = request.flake_baseline.clone();
    baseline.sort_by(|left, right| {
        left.extractor
            .cmp(&right.extractor)
            .then(left.extractor_version.cmp(&right.extractor_version))
            .then(left.signature.cmp(&right.signature))
    });
    let mut candidates = request.owner_candidates.clone();
    candidates.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    // Ancestry index: newest (base R) at 0.
    let mut ancestry_index: BTreeMap<String, usize> = BTreeMap::new();
    for (index, head) in request.ancestry.iter().enumerate() {
        ancestry_index.entry(head.clone()).or_insert(index);
    }
    let now_ts = request.now_ts.unwrap_or_else(|| {
        evidence.iter().map(|run| run.settled_ts).max().unwrap_or(0)
    });
    let lookback_cutoff = now_ts.saturating_sub(TOOL_RUN_TRIAGE_LOOKBACK_SECS);
    let knobs_value = serde_json::json!({
        "min_witnesses": request.knobs.min_witnesses,
        "touched_requires_clean_witness": request.knobs.touched_requires_clean_witness,
    });
    let mut labels = Vec::new();
    for item in &subjects {
        let touched = paths_touch(&item.locator_paths, &subject.dirty_paths);
        // Generic and environment items are always UNKNOWN.
        if item.extractor == "generic" {
            labels.push(label_wire(
                item,
                ToolRunTriageClassWire::Unknown,
                touched,
                &knobs_value,
                serde_json::json!({
                    "witness_run_ids": [],
                    "selection_record_ids": [],
                    "distinct_agents": 0,
                    "distinct_workspaces": 0,
                    "first_seen_ts": null,
                    "clearing_run_id": null,
                    "baseline_source": null,
                    "touched": touched,
                    "rejection_reasons": ["extractor_generic"],
                }),
                match_owners(&item.locator_paths, &candidates, now_ts),
                now_ts,
            ));
            continue;
        }
        if item.extractor == "environment" {
            labels.push(label_wire(
                item,
                ToolRunTriageClassWire::Unknown,
                touched,
                &knobs_value,
                serde_json::json!({
                    "witness_run_ids": [],
                    "selection_record_ids": [],
                    "distinct_agents": 0,
                    "distinct_workspaces": 0,
                    "first_seen_ts": null,
                    "clearing_run_id": null,
                    "baseline_source": null,
                    "touched": touched,
                    "rejection_reasons": ["environment"],
                }),
                match_owners(&item.locator_paths, &candidates, now_ts),
                now_ts,
            ));
            continue;
        }
        // FLAKY via active baseline entry.
        if let Some(entry) = baseline.iter().find(|entry| {
            entry.extractor == item.extractor
                && entry.extractor_version == item.extractor_version
                && entry.signature == item.signature
        }) {
            labels.push(label_wire(
                item,
                ToolRunTriageClassWire::Flaky,
                touched,
                &knobs_value,
                serde_json::json!({
                    "witness_run_ids": [],
                    "selection_record_ids": [],
                    "distinct_agents": 0,
                    "distinct_workspaces": 0,
                    "first_seen_ts": null,
                    "clearing_run_id": null,
                    "baseline_source": entry.source,
                    "touched": touched,
                    "rejection_reasons": [],
                }),
                match_owners(&item.locator_paths, &candidates, now_ts),
                now_ts,
            ));
            continue;
        }
        // FLAKY via same-complete-fingerprint disagreement (tests only).
        if is_test_extractor(&item.extractor) {
            let disagreement = evidence.iter().find(|run| {
                run.complete_fingerprint
                    && run.fingerprint_digest == subject.fingerprint_digest
                    && run.settled_ts >= lookback_cutoff
                    && run.stage_completions.contains(&item.stage_key)
                    && !contains_triple(
                        &run.items,
                        &item.extractor,
                        item.extractor_version,
                        &item.signature,
                    )
            });
            if disagreement.is_some() {
                labels.push(label_wire(
                    item,
                    ToolRunTriageClassWire::Flaky,
                    touched,
                    &knobs_value,
                    serde_json::json!({
                        "witness_run_ids": [],
                        "selection_record_ids": [],
                        "distinct_agents": 0,
                        "distinct_workspaces": 0,
                        "first_seen_ts": null,
                        "clearing_run_id": null,
                        "baseline_source": "same-fingerprint-disagreement",
                        "touched": touched,
                        "rejection_reasons": [],
                    }),
                    match_owners(&item.locator_paths, &candidates, now_ts),
                    now_ts,
                ));
                continue;
            }
        }
        // Gather valid witnesses (all conditions).
        let mut witnesses: Vec<&ToolRunTriageEvidenceRunWire> = Vec::new();
        for run in &evidence {
            if run.settled_ts < lookback_cutoff {
                continue;
            }
            if run.base_head.is_none() {
                continue;
            }
            let base = run.base_head.as_deref().unwrap_or_default();
            let in_ancestry = subject
                .base_head
                .as_deref()
                .is_some_and(|subject_base| base == subject_base)
                || ancestry_index.contains_key(base);
            if !in_ancestry {
                continue;
            }
            // Same machine only; differing machines never witness.
            if subject.machine.is_some()
                && run.machine.is_some()
                && subject.machine != run.machine
            {
                continue;
            }
            let different_workspace_or_clean =
                run.workspace != subject.workspace || run.clean_tree;
            if !different_workspace_or_clean {
                continue;
            }
            if !contains_triple(
                &run.items,
                &item.extractor,
                item.extractor_version,
                &item.signature,
            ) {
                continue;
            }
            if run.dirty_unknown {
                continue;
            }
            if paths_touch(&item.locator_paths, &run.dirty_paths) {
                continue;
            }
            witnesses.push(run);
        }
        witnesses.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        let distinct_workspaces: BTreeSet<String> = witnesses
            .iter()
            .map(|run| run.workspace.clone().unwrap_or_default())
            .collect();
        let distinct_agents: BTreeSet<String> = witnesses
            .iter()
            .filter_map(|run| run.agent.clone())
            .collect();
        let first_seen = witnesses
            .iter()
            .map(|run| run.settled_ts)
            .min()
            .or_else(|| {
                evidence
                    .iter()
                    .filter(|run| {
                        contains_triple(
                            &run.items,
                            &item.extractor,
                            item.extractor_version,
                            &item.signature,
                        )
                    })
                    .map(|run| run.settled_ts)
                    .min()
            });
        // Newest witness by ancestry (smallest index), tie by settled_ts.
        let newest_witness_index = witnesses
            .iter()
            .filter_map(|run| {
                let base = run.base_head.as_deref().unwrap_or_default();
                if subject
                    .base_head
                    .as_deref()
                    .is_some_and(|subject_base| base == subject_base)
                {
                    Some(0usize)
                } else {
                    ancestry_index.get(base).copied()
                }
            })
            .min();
        // Clearing run: base strictly between newest witness and base(R),
        // stage completed, s absent, not touched.
        let mut clearing: Option<&ToolRunTriageEvidenceRunWire> = None;
        if let Some(witness_index) = newest_witness_index {
            let mut candidates_clearing: Vec<&ToolRunTriageEvidenceRunWire> =
                Vec::new();
            for run in &evidence {
                if run.settled_ts < lookback_cutoff {
                    continue;
                }
                let Some(base) = run.base_head.as_deref() else {
                    continue;
                };
                let run_index = if subject
                    .base_head
                    .as_deref()
                    .is_some_and(|subject_base| base == subject_base)
                {
                    0usize
                } else if let Some(index) = ancestry_index.get(base) {
                    *index
                } else {
                    continue;
                };
                if run_index >= witness_index {
                    continue;
                }
                if !run.stage_completions.contains(&item.stage_key) {
                    continue;
                }
                if contains_triple(
                    &run.items,
                    &item.extractor,
                    item.extractor_version,
                    &item.signature,
                ) {
                    continue;
                }
                if run.dirty_unknown {
                    continue;
                }
                if paths_touch(&item.locator_paths, &run.dirty_paths) {
                    continue;
                }
                if subject.machine.is_some()
                    && run.machine.is_some()
                    && subject.machine != run.machine
                {
                    continue;
                }
                candidates_clearing.push(run);
            }
            candidates_clearing
                .sort_by(|left, right| left.run_id.cmp(&right.run_id));
            clearing = candidates_clearing.into_iter().next();
        }
        let witness_count = distinct_workspaces.len() as u32;
        let has_clean_witness = witnesses.iter().any(|run| run.clean_tree);
        if clearing.is_none()
            && witness_count >= request.knobs.min_witnesses
            && (!request.knobs.touched_requires_clean_witness
                || !touched
                || has_clean_witness)
        {
            let mut witness_ids: Vec<String> = witnesses
                .iter()
                .filter(|run| !run.selection_source)
                .map(|run| run.run_id.clone())
                .collect();
            witness_ids.sort();
            let mut selection_ids: Vec<String> = witnesses
                .iter()
                .filter(|run| run.selection_source)
                .map(|run| run.run_id.clone())
                .collect();
            selection_ids.sort();
            labels.push(label_wire(
                item,
                ToolRunTriageClassWire::Known,
                touched,
                &knobs_value,
                serde_json::json!({
                    "witness_run_ids": witness_ids,
                    "selection_record_ids": selection_ids,
                    "distinct_agents": distinct_agents.len(),
                    "distinct_workspaces": distinct_workspaces.len(),
                    "first_seen_ts": first_seen,
                    "clearing_run_id": null,
                    "baseline_source": null,
                    "touched": touched,
                    "rejection_reasons": [],
                }),
                match_owners(&item.locator_paths, &candidates, now_ts),
                now_ts,
            ));
            continue;
        }
        // NEW via touched or pass witness.
        let pass_witness = evidence.iter().find(|run| {
            if run.settled_ts < lookback_cutoff {
                return false;
            }
            let Some(base) = run.base_head.as_deref() else {
                return false;
            };
            let in_ancestry_or_equal = subject
                .base_head
                .as_deref()
                .is_some_and(|subject_base| base == subject_base)
                || ancestry_index.contains_key(base);
            if !in_ancestry_or_equal {
                return false;
            }
            if !run.stage_completions.contains(&item.stage_key) {
                return false;
            }
            if contains_triple(
                &run.items,
                &item.extractor,
                item.extractor_version,
                &item.signature,
            ) {
                return false;
            }
            if run.dirty_unknown {
                return false;
            }
            if paths_touch(&item.locator_paths, &run.dirty_paths) {
                return false;
            }
            if subject.machine.is_some()
                && run.machine.is_some()
                && subject.machine != run.machine
            {
                return false;
            }
            true
        });
        if touched || pass_witness.is_some() {
            labels.push(label_wire(
                item,
                ToolRunTriageClassWire::New,
                touched,
                &knobs_value,
                serde_json::json!({
                    "witness_run_ids": [],
                    "selection_record_ids": [],
                    "distinct_agents": distinct_agents.len(),
                    "distinct_workspaces": distinct_workspaces.len(),
                    "first_seen_ts": first_seen,
                    "clearing_run_id": clearing.map(|run| run.run_id.clone()),
                    "baseline_source": null,
                    "touched": touched,
                    "rejection_reasons": [],
                }),
                match_owners(&item.locator_paths, &candidates, now_ts),
                now_ts,
            ));
            continue;
        }
        // UNKNOWN with ordered rejection reasons.
        let mut reasons: Vec<&str> = Vec::new();
        if witnesses.is_empty() {
            reasons.push("no_witness");
        } else if clearing.is_some() {
            reasons.push("witness_cleared");
        } else if witness_count < request.knobs.min_witnesses {
            reasons.push("insufficient_witnesses");
        }
        if request.knobs.touched_requires_clean_witness
            && touched
            && !has_clean_witness
            && !witnesses.is_empty()
            && clearing.is_none()
        {
            reasons.push("touched_needs_clean_witness");
        }
        reasons.push("untouched_no_pass_witness");
        // Emit in the contract's canonical order.
        let order = [
            "no_witness",
            "witness_cleared",
            "untouched_no_pass_witness",
            "extractor_generic",
            "environment",
            "insufficient_witnesses",
            "touched_needs_clean_witness",
        ];
        reasons.sort_by_key(|reason| {
            order.iter().position(|item| item == reason).unwrap_or(99)
        });
        labels.push(label_wire(
            item,
            ToolRunTriageClassWire::Unknown,
            touched,
            &knobs_value,
            serde_json::json!({
                "witness_run_ids": [],
                "selection_record_ids": [],
                "distinct_agents": distinct_agents.len(),
                "distinct_workspaces": distinct_workspaces.len(),
                "first_seen_ts": first_seen,
                "clearing_run_id": clearing.map(|run| run.run_id.clone()),
                "baseline_source": null,
                "touched": touched,
                "rejection_reasons": reasons,
            }),
            match_owners(&item.locator_paths, &candidates, now_ts),
            now_ts,
        ));
    }
    // REPEAT: same complete fingerprint digest and same signature set as a
    // prior failed evidence run.
    let mut subject_set: Vec<(String, u32, String)> = subjects
        .iter()
        .map(|item| {
            evidence_key(
                &item.extractor,
                item.extractor_version,
                &item.signature,
            )
        })
        .collect();
    subject_set.sort();
    let repeat_of = evidence
        .iter()
        .filter(|run| {
            !run.selection_source
                && run.failed
                && run.complete_fingerprint
                && run.fingerprint_digest == subject.fingerprint_digest
        })
        .filter(|run| {
            let mut run_set: Vec<(String, u32, String)> = run
                .items
                .iter()
                .map(|item| {
                    evidence_key(
                        &item.extractor,
                        item.extractor_version,
                        &item.signature,
                    )
                })
                .collect();
            run_set.sort();
            run_set == subject_set
        })
        .max_by(|left, right| {
            left.settled_ts
                .cmp(&right.settled_ts)
                .then(left.run_id.cmp(&right.run_id))
        })
        .map(|run| run.run_id.clone());
    Ok(ToolRunTriageClassifyResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        labels,
        repeat_of,
        diagnostics: Vec::new(),
    })
}

#[allow(clippy::too_many_arguments)]
fn label_wire(
    item: &ToolRunTriageSubjectItemWire,
    class: ToolRunTriageClassWire,
    touched: bool,
    knobs: &serde_json::Value,
    evidence: serde_json::Value,
    owners: Vec<serde_json::Value>,
    now_ts: i64,
) -> ToolRunTriageClassifyLabelWire {
    ToolRunTriageClassifyLabelWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        stage_key: item.stage_key.clone(),
        extractor: item.extractor.clone(),
        extractor_version: item.extractor_version,
        signature: item.signature.clone(),
        class,
        touched,
        rule_version: TOOL_RUN_TRIAGE_RULE_VERSION,
        knobs: knobs.clone(),
        evidence,
        possible_owners: serde_json::Value::Array(owners),
        classified_ts: now_ts,
    }
}

use std::collections::BTreeSet;
use std::path::{Component, Path};

use regex::Regex;
use serde_json::Value;

use super::wire::{
    ChopEngineError, ChopLaunchProposalWire, ChopResultDocumentWire,
    ChopResultStatusWire, ProposalWaitOnWire, CHOP_RESULT_SCHEMA_VERSION,
};

/// Parse and validate one versioned chop result JSON document.
pub fn parse_chop_result(
    document: &str,
) -> Result<ChopResultDocumentWire, ChopEngineError> {
    let value: Value = serde_json::from_str(document).map_err(|error| {
        ChopEngineError::new(
            "invalid_json",
            "$",
            format!(
                "chop result is not valid JSON at line {}, column {}: {error}",
                error.line(),
                error.column()
            ),
        )
    })?;
    validate_result_value(&value)?;
    let result: ChopResultDocumentWire = serde_json::from_value(value)
        .map_err(|error| {
            ChopEngineError::new(
                "invalid_result",
                "$",
                format!("chop result has an invalid field: {error}"),
            )
        })?;
    validate_chop_result(&result)?;
    Ok(result)
}

fn validate_result_value(value: &Value) -> Result<(), ChopEngineError> {
    let object = value.as_object().ok_or_else(|| {
        ChopEngineError::new(
            "invalid_result",
            "$",
            "chop result must be a JSON object",
        )
    })?;
    let version = object
        .get("schema_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| {
            ChopEngineError::new(
                "missing_schema_version",
                "$.schema_version",
                "schema_version must be an integer",
            )
        })?;
    if version != CHOP_RESULT_SCHEMA_VERSION as u64 {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!("got {version}, expected {CHOP_RESULT_SCHEMA_VERSION}"),
        ));
    }
    Ok(())
}

/// Validate an already-decoded chop result document.
pub fn validate_chop_result(
    result: &ChopResultDocumentWire,
) -> Result<(), ChopEngineError> {
    if result.schema_version != CHOP_RESULT_SCHEMA_VERSION {
        return Err(ChopEngineError::new(
            "schema_version_mismatch",
            "$.schema_version",
            format!(
                "got {}, expected {CHOP_RESULT_SCHEMA_VERSION}",
                result.schema_version
            ),
        ));
    }
    validate_optional_text(result.summary.as_deref(), "$.summary")?;
    validate_optional_text(result.reason.as_deref(), "$.reason")?;
    for key in result.counters.keys() {
        if key.trim().is_empty() {
            return Err(ChopEngineError::new(
                "invalid_counter",
                "$.counters",
                "counter names must not be blank",
            ));
        }
    }
    for (index, evidence) in result.evidence.iter().enumerate() {
        validate_evidence_path(evidence, index)?;
    }
    if result.status != ChopResultStatusWire::Ok
        && !result.proposed_launches.is_empty()
    {
        return Err(ChopEngineError::new(
            "proposals_for_non_actionable_status",
            "$.proposed_launches",
            "only an `ok` chop result may propose agent launches",
        ));
    }

    let mut prior_ids = Vec::new();
    let mut all_ids = BTreeSet::new();
    for (index, proposal) in result.proposed_launches.iter().enumerate() {
        validate_chop_proposal(proposal, index, &prior_ids)?;
        if let Some(id) = proposal.id.as_ref() {
            if !all_ids.insert(id.clone()) {
                return Err(ChopEngineError::new(
                    "duplicate_proposal_id",
                    format!("$.proposed_launches[{index}].id"),
                    format!("proposal id `{id}` is duplicated"),
                ));
            }
            prior_ids.push(id.clone());
        }
    }
    Ok(())
}

fn validate_optional_text(
    value: Option<&str>,
    path: &str,
) -> Result<(), ChopEngineError> {
    if value.is_some_and(|item| item.trim().is_empty()) {
        return Err(ChopEngineError::new(
            "blank_value",
            path,
            "value must not be blank when present",
        ));
    }
    Ok(())
}

fn validate_evidence_path(
    evidence: &str,
    index: usize,
) -> Result<(), ChopEngineError> {
    let path_label = format!("$.evidence[{index}]");
    if evidence.trim().is_empty() {
        return Err(ChopEngineError::new(
            "invalid_evidence",
            path_label,
            "evidence path must not be blank",
        ));
    }
    let path = Path::new(evidence);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(ChopEngineError::new(
            "invalid_evidence",
            path_label,
            "evidence must be a relative path inside the chop run directory",
        ));
    }
    Ok(())
}

/// Validate one proposal and any `wait_on` reference to earlier proposals.
pub fn validate_chop_proposal(
    proposal: &ChopLaunchProposalWire,
    index: usize,
    prior_ids: &[String],
) -> Result<(), ChopEngineError> {
    let base = format!("$.proposed_launches[{index}]");
    if let Some(id) = proposal.id.as_deref() {
        validate_token(id, &format!("{base}.id"), "proposal id")?;
    }
    if proposal.prompt.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_prompt",
            format!("{base}.prompt"),
            "proposal prompt must not be blank",
        ));
    }
    let workflow_reference = Regex::new(r"(?m)^\s*#!\S+")
        .expect("standalone workflow regex is valid");
    if workflow_reference.is_match(&proposal.prompt) {
        return Err(ChopEngineError::new(
            "workflow_reference_forbidden",
            format!("{base}.prompt"),
            "standalone `#!workflow` references are not allowed in chop proposals; put the work directly in the proposal prompt",
        ));
    }
    validate_token(
        &proposal.workspace,
        &format!("{base}.workspace"),
        "workspace reference",
    )?;
    validate_optional_token(
        proposal.agent_name.as_deref(),
        &format!("{base}.agent_name"),
        "agent name template",
    )?;
    validate_optional_token(
        proposal.tribe.as_deref(),
        &format!("{base}.tribe"),
        "tribe",
    )?;
    validate_optional_token(
        proposal.model.as_deref(),
        &format!("{base}.model"),
        "model",
    )?;
    validate_optional_token(
        proposal.effort.as_deref(),
        &format!("{base}.effort"),
        "effort",
    )?;
    validate_optional_text(
        proposal.dedupe_key.as_deref(),
        &format!("{base}.dedupe_key"),
    )?;

    let env_name = Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$")
        .expect("environment name regex is valid");
    for (name, value) in &proposal.env {
        if !env_name.is_match(name) {
            return Err(ChopEngineError::new(
                "invalid_env_name",
                format!("{base}.env.{name}"),
                format!("`{name}` is not a valid environment variable name"),
            ));
        }
        if value.contains('\0') {
            return Err(ChopEngineError::new(
                "invalid_env_value",
                format!("{base}.env.{name}"),
                "environment values must not contain NUL bytes",
            ));
        }
    }

    if let Some(wait_on) = proposal.wait_on.as_ref() {
        match wait_on {
            ProposalWaitOnWire::Index(wait_index) if *wait_index >= index => {
                return Err(ChopEngineError::new(
                    "invalid_wait_on",
                    format!("{base}.wait_on"),
                    format!(
                        "wait_on index {wait_index} must refer to an earlier proposal"
                    ),
                ));
            }
            ProposalWaitOnWire::Id(wait_id)
                if !prior_ids.iter().any(|prior| prior == wait_id) =>
            {
                return Err(ChopEngineError::new(
                    "invalid_wait_on",
                    format!("{base}.wait_on"),
                    format!(
                        "wait_on id `{wait_id}` does not name an earlier proposal"
                    ),
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_optional_token(
    value: Option<&str>,
    path: &str,
    label: &str,
) -> Result<(), ChopEngineError> {
    if let Some(value) = value {
        validate_token(value, path, label)?;
    }
    Ok(())
}

fn validate_token(
    value: &str,
    path: &str,
    label: &str,
) -> Result<(), ChopEngineError> {
    if value.trim().is_empty() {
        return Err(ChopEngineError::new(
            "blank_value",
            path,
            format!("{label} must not be blank"),
        ));
    }
    if value.chars().any(char::is_whitespace) || value.contains('\0') {
        return Err(ChopEngineError::new(
            "invalid_token",
            path,
            format!("{label} must be a single non-NUL token"),
        ));
    }
    Ok(())
}

/// Derive the default `%name` value for a proposal.
pub fn derive_chop_agent_name(
    chop_name: &str,
    target_key: Option<&str>,
    proposal_index: usize,
    run_token: Option<&str>,
) -> Result<String, ChopEngineError> {
    let chop = sanitized_component(chop_name);
    if chop.is_empty() {
        return Err(ChopEngineError::new(
            "invalid_chop_name",
            "$.chop_name",
            "chop name must contain at least one letter or digit",
        ));
    }
    let mut parts = vec!["chop".to_string(), chop];
    if let Some(target) = target_key {
        let target = sanitized_component(target);
        if target.is_empty() {
            return Err(ChopEngineError::new(
                "invalid_target_key",
                "$.target_key",
                "target key must contain at least one letter or digit",
            ));
        }
        parts.push(target);
    }
    if let Some(run_token) = run_token {
        let mut run_token = sanitized_component(run_token);
        // Chop run ids begin with a sortable date shared by every run that
        // day; keep the entropy-bearing seconds/microseconds at the tail.
        if run_token.len() > 8 {
            run_token = run_token.split_off(run_token.len() - 8);
        }
        while run_token.starts_with('-') || run_token.starts_with('_') {
            run_token.remove(0);
        }
        while run_token.ends_with('-') || run_token.ends_with('_') {
            run_token.pop();
        }
        if run_token.is_empty() {
            return Err(ChopEngineError::new(
                "invalid_run_token",
                "$.run_token",
                "run token must contain at least one letter or digit",
            ));
        }
        parts.push(run_token);
    }
    parts.push((proposal_index + 1).to_string());
    let mut value = parts.join(".");
    if value.len() > 120 {
        value.truncate(120);
        while value.ends_with('.')
            || value.ends_with('-')
            || value.ends_with('_')
        {
            value.pop();
        }
    }
    Ok(value)
}

fn sanitized_component(value: &str) -> String {
    let mut output = String::new();
    let mut separator = false;
    for character in value.trim().chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            output.push(character.to_ascii_lowercase());
            separator = false;
        } else if !output.is_empty() && !separator {
            output.push('-');
            separator = true;
        }
    }
    output.trim_matches('-').to_string()
}

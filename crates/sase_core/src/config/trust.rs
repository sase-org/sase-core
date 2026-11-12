//! Digest-pinned trust for project-local executable commands.
//!
//! Pure-Rust command-bearing projection, canonical digest, filtering and
//! trust-decision contracts for project-local config (plan
//! `202611/local_config_digest_trust.md`, phase `trust-policy`).
//!
//! Python supplies already-decoded local data (plus lower-layer executable
//! definitions when override analysis needs them). Rust normalizes legacy
//! macro spellings, returns the normalized command-bearing projection, its
//! canonical digest, the effective local contribution for a trust verdict,
//! blocked paths, and a structured status/reason. No enforcement happens
//! here: callers decide when to filter.
//!
//! Digest scope: the canonical command-bearing projection only. YAML
//! whitespace, comments, and mapping order never affect it. The digest does
//! not cover transitive script/binary contents invoked by a command, nor
//! standalone macro files, service/tool catalogs, or prompt substitution.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::macro_syntax::normalize_macro_config_layer;
use super::macro_syntax::MacroLayerNormalizeRequestWire;
use super::wire::{ConfigDiagnosticWire, ConfigError};

/// Wire schema version for the local-command trust contract.
pub const LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION: u32 = 1;

/// Policy version stamped into projections and decisions.
pub const LOCAL_COMMAND_TRUST_POLICY_VERSION: u32 = 1;

/// Domain/version prefix prepended to the canonical JSON before SHA-256.
pub const LOCAL_COMMAND_DIGEST_DOMAIN: &str = "sase.local-commands/v1:";

/// Projection request: one decoded local layer plus lower-layer executable
/// definitions for sparse-override analysis.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCommandProjectionRequestWire {
    pub layer: Value,
    #[serde(default)]
    pub lower_macros: Value,
    #[serde(default)]
    pub lower_workflows: Value,
    #[serde(default)]
    pub source: String,
    /// Logical selected config location, included in the digest when supplied.
    #[serde(default)]
    pub config_path: Option<String>,
}

/// Normalized command-bearing projection plus digest and blocked paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCommandProjectionWire {
    pub schema_version: u32,
    pub policy_version: u32,
    pub projection: Value,
    pub blocked_paths: Vec<String>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
    pub digest: String,
    pub canonical_json: String,
    #[serde(default)]
    pub config_path: Option<String>,
}

/// Digest request: hash one already-projected value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCommandDigestRequestWire {
    pub projection: Value,
}

/// Digest output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalCommandDigestWire {
    pub schema_version: u32,
    pub digest: String,
    pub canonical_json: String,
}

/// Filter request: apply one trust verdict to one decoded local layer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalTrustFilterRequestWire {
    pub layer: Value,
    pub trusted: bool,
    #[serde(default)]
    pub lower_macros: Value,
    #[serde(default)]
    pub lower_workflows: Value,
    #[serde(default)]
    pub source: String,
    /// Logical selected config location, used by the trust digest.
    #[serde(default)]
    pub config_path: Option<String>,
}

/// Filter output: the effective local contribution survivors keep.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalTrustFilterWire {
    pub schema_version: u32,
    pub effective_layer: Value,
    pub removed_paths: Vec<String>,
    pub status: String,
    pub reason: String,
}

/// Trust-decision request: compare digests and policy versions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalTrustDecisionRequestWire {
    pub current_digest: String,
    pub acknowledged_digest: Option<String>,
    #[serde(default = "default_policy_version")]
    pub current_policy_version: u32,
    #[serde(default)]
    pub acknowledged_policy_version: Option<u32>,
}

/// Trust-decision output.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LocalTrustDecisionWire {
    pub schema_version: u32,
    pub status: String,
    pub reason: String,
}

fn default_policy_version() -> u32 {
    LOCAL_COMMAND_TRUST_POLICY_VERSION
}

fn diagnostic(
    path: Option<String>,
    code: &str,
    message: impl Into<String>,
) -> ConfigDiagnosticWire {
    ConfigDiagnosticWire {
        severity: "info".to_string(),
        code: code.to_string(),
        message: message.into(),
        path,
        layer: None,
    }
}

/// Recursively sort mapping keys, preserving list order and scalar types.
pub fn canonicalize_command_value(value: &Value) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(canonicalize_command_value).collect())
        }
        Value::Object(map) => {
            let sorted: BTreeMap<&String, &Value> = map.iter().collect();
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, val)| {
                        (key.clone(), canonicalize_command_value(val))
                    })
                    .collect(),
            )
        }
        scalar => scalar.clone(),
    }
}

/// Canonical JSON for one projected value (sorted keys, compact form).
pub fn canonical_json_for_projection(projection: &Value) -> String {
    serde_json::to_string(&canonicalize_command_value(projection))
        .unwrap_or_else(|_| "null".to_string())
}

/// SHA-256 hex of `domain + canonical_json`.
pub fn digest_canonical_json(canonical_json: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(LOCAL_COMMAND_DIGEST_DOMAIN.as_bytes());
    hasher.update(canonical_json.as_bytes());
    hex::encode(hasher.finalize())
}

/// Hash one already-projected value.
pub fn digest_local_command_projection(
    request: &LocalCommandDigestRequestWire,
) -> LocalCommandDigestWire {
    let canonical_json = canonical_json_for_projection(&request.projection);
    let digest = digest_canonical_json(&canonical_json);
    LocalCommandDigestWire {
        schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
        digest,
        canonical_json,
    }
}

/// Project the command-bearing subset of one decoded local layer.
pub fn project_local_commands(
    request: &LocalCommandProjectionRequestWire,
) -> Result<LocalCommandProjectionWire, ConfigError> {
    let normalized =
        normalize_macro_config_layer(&MacroLayerNormalizeRequestWire {
            layer: request.layer.clone(),
            accept_legacy_xprompt_names: true,
            source: request.source.clone(),
        })?
        .canonical;
    let Some(layer_map) = normalized.as_object() else {
        return Err(ConfigError::validation(
            "local command layer must be a mapping",
        ));
    };
    let lower_macros = request.lower_macros.as_object();
    let lower_workflows = request.lower_workflows.as_object();

    let mut projection = Map::new();
    let mut blocked_paths: Vec<String> = Vec::new();
    let mut diagnostics: Vec<ConfigDiagnosticWire> = Vec::new();

    if layer_map.contains_key("commit_hooks") {
        let value = layer_map["commit_hooks"].clone();
        projection.insert(
            "commit_hooks".to_string(),
            canonicalize_command_value(&value),
        );
        blocked_paths.push("commit_hooks".to_string());
        diagnostics.push(diagnostic(
            Some("commit_hooks".to_string()),
            "local-command-commit-hooks",
            "local commit_hooks contribution is command-bearing",
        ));
    }

    if let Some(raw_hooks) = layer_map.get("file_hooks") {
        match raw_hooks.as_array() {
            Some(entries) => {
                let mut kept = Vec::with_capacity(entries.len());
                for (index, entry) in entries.iter().enumerate() {
                    // Every file-hook entry is command-bearing: literal
                    // `command:` entries execute, and `use:` template
                    // entries resolve to executable providers without a
                    // literal command field.
                    kept.push(canonicalize_command_value(entry));
                    blocked_paths.push(format!("file_hooks[{index}]"));
                }
                projection.insert("file_hooks".to_string(), Value::Array(kept));
                if !entries.is_empty() {
                    blocked_paths.push("file_hooks".to_string());
                }
                diagnostics.push(diagnostic(
                    Some("file_hooks".to_string()),
                    "local-command-file-hooks",
                    format!(
                        "local file_hooks carries {} command-bearing entries",
                        entries.len()
                    ),
                ));
            }
            None => {
                projection.insert(
                    "file_hooks".to_string(),
                    canonicalize_command_value(raw_hooks),
                );
                blocked_paths.push("file_hooks".to_string());
                diagnostics.push(diagnostic(
                    Some("file_hooks".to_string()),
                    "local-command-file-hooks-invalid",
                    "local file_hooks is not a list; blocked conservatively",
                ));
            }
        }
    }

    if let Some(raw_macros) = layer_map.get("macros") {
        match raw_macros.as_object() {
            Some(entries) => {
                let mut kept = Map::new();
                for (name, def) in entries {
                    let lower = lower_macros.and_then(|lower| lower.get(name));
                    let executable =
                        is_executable_macro_override(name, def, lower);
                    if executable {
                        kept.insert(
                            name.clone(),
                            canonicalize_command_value(def),
                        );
                        blocked_paths.push(format!("macros.{name}"));
                    }
                }
                if !kept.is_empty() {
                    projection
                        .insert("macros".to_string(), Value::Object(kept));
                    diagnostics.push(diagnostic(
                        Some("macros".to_string()),
                        "local-command-macros",
                        "local macros carry command-bearing definitions",
                    ));
                }
            }
            None => {
                projection.insert(
                    "macros".to_string(),
                    canonicalize_command_value(raw_macros),
                );
                blocked_paths.push("macros".to_string());
                diagnostics.push(diagnostic(
                    Some("macros".to_string()),
                    "local-command-macros-invalid",
                    "local macros is not a mapping; blocked conservatively",
                ));
            }
        }
    }

    if let Some(raw_workflows) = layer_map.get("workflows") {
        match raw_workflows.as_object() {
            Some(entries) => {
                let mut kept = Map::new();
                for (name, def) in entries {
                    let lower =
                        lower_workflows.and_then(|lower| lower.get(name));
                    if is_executable_workflow_override(def, lower) {
                        kept.insert(
                            name.clone(),
                            canonicalize_command_value(def),
                        );
                        blocked_paths.push(format!("workflows.{name}"));
                    }
                }
                if !kept.is_empty() {
                    projection
                        .insert("workflows".to_string(), Value::Object(kept));
                    diagnostics.push(diagnostic(
                        Some("workflows".to_string()),
                        "local-command-workflows",
                        "local workflows carry command-bearing definitions",
                    ));
                }
            }
            None => {
                projection.insert(
                    "workflows".to_string(),
                    canonicalize_command_value(raw_workflows),
                );
                blocked_paths.push("workflows".to_string());
                diagnostics.push(diagnostic(
                    Some("workflows".to_string()),
                    "local-command-workflows-invalid",
                    "local workflows is not a mapping; blocked conservatively",
                ));
            }
        }
    }

    blocked_paths.sort();
    blocked_paths.dedup();
    diagnostics.sort_by(|left, right| {
        left.path.cmp(&right.path).then(left.code.cmp(&right.code))
    });

    let projection_value = Value::Object(projection);
    let canonical_json = match &request.config_path {
        Some(config_path) => {
            let mut digest_input = Map::new();
            digest_input.insert(
                "config_path".to_string(),
                Value::String(config_path.clone()),
            );
            digest_input
                .insert("commands".to_string(), projection_value.clone());
            canonical_json_for_projection(&Value::Object(digest_input))
        }
        None => canonical_json_for_projection(&projection_value),
    };
    let digest = digest_canonical_json(&canonical_json);
    Ok(LocalCommandProjectionWire {
        schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
        policy_version: LOCAL_COMMAND_TRUST_POLICY_VERSION,
        projection: projection_value,
        blocked_paths,
        diagnostics,
        digest,
        canonical_json,
        config_path: request.config_path.clone(),
    })
}

/// Filter one decoded local layer by one trust verdict.
pub fn filter_trusted_local_layer(
    request: &LocalTrustFilterRequestWire,
) -> Result<LocalTrustFilterWire, ConfigError> {
    let projection =
        project_local_commands(&LocalCommandProjectionRequestWire {
            layer: request.layer.clone(),
            lower_macros: request.lower_macros.clone(),
            lower_workflows: request.lower_workflows.clone(),
            source: request.source.clone(),
            config_path: request.config_path.clone(),
        })?;
    let normalized =
        normalize_macro_config_layer(&MacroLayerNormalizeRequestWire {
            layer: request.layer.clone(),
            accept_legacy_xprompt_names: true,
            source: request.source.clone(),
        })?
        .canonical;
    if request.trusted {
        return Ok(LocalTrustFilterWire {
            schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
            effective_layer: normalized,
            removed_paths: Vec::new(),
            status: "trusted".to_string(),
            reason: "acknowledged-digest-current".to_string(),
        });
    }

    let Some(layer_map) = normalized.as_object().cloned() else {
        return Err(ConfigError::validation(
            "local command layer must be a mapping",
        ));
    };
    let projection_map = projection
        .projection
        .as_object()
        .cloned()
        .unwrap_or_default();
    let mut effective = layer_map;
    let mut removed_paths: Vec<String> = Vec::new();

    if projection_map.contains_key("commit_hooks") {
        effective.remove("commit_hooks");
        removed_paths.push("commit_hooks".to_string());
    }
    if let Some(blocked) = projection_map.get("file_hooks") {
        // All local file-hook entries are command-bearing, so an
        // untrusted verdict omits every local entry. Keep the key only
        // when a future non-command entry survives (none today).
        effective.remove("file_hooks");
        removed_paths.push("file_hooks".to_string());
        if let Some(blocked_hooks) = blocked.as_array() {
            for (index, _) in blocked_hooks.iter().enumerate() {
                removed_paths.push(format!("file_hooks[{index}]"));
            }
        }
    }
    remove_blocked_mapping_entries(
        &mut effective,
        projection_map.get("macros"),
        "macros",
        &mut removed_paths,
    );
    remove_blocked_mapping_entries(
        &mut effective,
        projection_map.get("workflows"),
        "workflows",
        &mut removed_paths,
    );

    removed_paths.sort();
    removed_paths.dedup();
    Ok(LocalTrustFilterWire {
        schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
        effective_layer: Value::Object(effective),
        removed_paths,
        status: "untrusted-filtered".to_string(),
        reason: "digest-not-acknowledged".to_string(),
    })
}

fn remove_blocked_mapping_entries(
    effective: &mut Map<String, Value>,
    blocked: Option<&Value>,
    key: &str,
    removed_paths: &mut Vec<String>,
) {
    let Some(blocked_value) = blocked else {
        return;
    };
    let Some(blocked_map) = blocked_value.as_object() else {
        // Invalid non-mapping shapes are blocked conservatively as a whole.
        if effective.remove(key).is_some() {
            removed_paths.push(key.to_string());
        }
        return;
    };
    if blocked_map.is_empty() {
        return;
    }
    let Some(local_map) = effective.get_mut(key).and_then(Value::as_object_mut)
    else {
        return;
    };
    for name in blocked_map.keys() {
        if local_map.remove(name).is_some() {
            removed_paths.push(format!("{key}.{name}"));
        }
    }
    if local_map.is_empty() {
        effective.remove(key);
        removed_paths.push(key.to_string());
    }
}

/// Decide trust from the current and acknowledged digests.
pub fn decide_local_command_trust(
    request: &LocalTrustDecisionRequestWire,
) -> LocalTrustDecisionWire {
    if request.current_policy_version != LOCAL_COMMAND_TRUST_POLICY_VERSION {
        return LocalTrustDecisionWire {
            schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
            status: "untrusted".to_string(),
            reason: "policy-version-mismatch".to_string(),
        };
    }
    match (
        &request.acknowledged_digest,
        &request.acknowledged_policy_version,
    ) {
        (None, _) => LocalTrustDecisionWire {
            schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
            status: "untrusted".to_string(),
            reason: "no-acknowledgment".to_string(),
        },
        (Some(_), Some(version))
            if *version != request.current_policy_version =>
        {
            LocalTrustDecisionWire {
                schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
                status: "untrusted".to_string(),
                reason: "policy-version-mismatch".to_string(),
            }
        }
        (Some(acknowledged), _) if acknowledged == &request.current_digest => {
            LocalTrustDecisionWire {
                schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
                status: "trusted".to_string(),
                reason: "acknowledged-digest-current".to_string(),
            }
        }
        (Some(_), _) => LocalTrustDecisionWire {
            schema_version: LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION,
            status: "untrusted".to_string(),
            reason: "digest-mismatch".to_string(),
        },
    }
}

/// True when one local macro entry is command-bearing.
///
/// Prompt-only entries are strings or mappings with only prompt keys
/// (`content`, `input`, `tags`, `snippet`, `description`, `skill`,
/// `log_skill_use`, `memory_type`, nested `macros` helpers). Anything else —
/// including `steps`, `bash`, `python`, `use`, `command`, or unknown keys —
/// is conservative executable. A prompt-only local entry that overrides a
/// lower-layer executable definition is still command-bearing.
fn is_executable_macro_override(
    _name: &str,
    def: &Value,
    lower: Option<&Value>,
) -> bool {
    if let Some(lower_def) = lower {
        if is_executable_macro_definition(lower_def) {
            return true;
        }
    }
    is_executable_macro_definition(def)
}

fn is_executable_macro_definition(def: &Value) -> bool {
    match def {
        Value::String(_) => false,
        Value::Object(map) => {
            for (key, value) in map {
                match key.as_str() {
                    "content" | "input" | "inputs" | "tags" | "snippet"
                    | "description" | "skill" | "log_skill_use"
                    | "memory_type" => continue,
                    "macros" | "xprompts" => {
                        if let Some(helpers) = value.as_object() {
                            for helper in helpers.values() {
                                if is_executable_macro_definition(helper) {
                                    return true;
                                }
                            }
                            continue;
                        }
                        return true;
                    }
                    "steps" | "bash" | "python" | "use" | "command"
                    | "parallel" | "agent" | "prompt" | "prompt_part"
                    | "workflow" | "workflows" | "jobs" | "chops"
                    | "routine" | "routines" | "run" | "exec" | "script"
                    | "shell" => return true,
                    _ => {
                        // Unknown macro keys fail closed: a future
                        // executable field must not slip through as
                        // prompt-only.
                        if (value.is_object() || value.is_array())
                            && contains_executable_key(value)
                        {
                            return true;
                        }
                        return true;
                    }
                }
            }
            false
        }
        // Nulls, booleans, numbers, and lists are not valid prompt-only
        // macro shapes; block them conservatively.
        _ => true,
    }
}

fn contains_executable_key(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, val)| {
            matches!(
                key.as_str(),
                "bash"
                    | "python"
                    | "use"
                    | "command"
                    | "steps"
                    | "parallel"
                    | "run"
                    | "exec"
                    | "script"
                    | "shell"
            ) || contains_executable_key(val)
        }),
        Value::Array(items) => items.iter().any(contains_executable_key),
        _ => false,
    }
}

/// True when one local workflow definition is command-bearing or overrides
/// a lower-layer executable workflow.
fn is_executable_workflow_override(def: &Value, lower: Option<&Value>) -> bool {
    if let Some(lower_def) = lower {
        if is_executable_workflow_definition(lower_def) {
            return true;
        }
    }
    is_executable_workflow_definition(def)
}

fn is_executable_workflow_definition(def: &Value) -> bool {
    let Value::Object(map) = def else {
        // Workflow definitions are mappings; strings carry no steps, while
        // other scalars are ambiguous shapes blocked conservatively.
        return !def.is_string();
    };
    if let Some(steps) = map.get("steps") {
        if let Some(items) = steps.as_array() {
            return items.iter().any(is_executable_step_value);
        }
        return true;
    }
    for key in ["bash", "python", "use", "command", "parallel", "steps"] {
        if map.contains_key(key) {
            return true;
        }
    }
    // Inputs, environment, tags, and descriptions alone never execute.
    // Unknown workflow-level keys fail closed.
    for (key, value) in map {
        match key.as_str() {
            "name" | "inputs" | "input" | "environment" | "tags"
            | "description" | "hidden" | "wraps_all" | "skill_name"
            | "memory_type" | "macros" | "xprompts" => continue,
            _ => {
                if (value.is_object() || value.is_array())
                    && contains_executable_key(value)
                {
                    return true;
                }
                return true;
            }
        }
    }
    false
}

fn is_executable_step_value(step: &Value) -> bool {
    let Some(map) = step.as_object() else {
        return true;
    };
    if map.contains_key("bash") || map.contains_key("python") {
        return true;
    }
    if map.contains_key("use") {
        // A structured `use:` reference cannot be proven safe without
        // resolving it, so it is command-bearing by construction.
        return true;
    }
    if let Some(parallel) = map.get("parallel") {
        if let Some(nested) = parallel.as_array() {
            return nested.iter().any(is_executable_step_value);
        }
        return true;
    }
    if map.contains_key("agent")
        || map.contains_key("prompt_part")
        || map.contains_key("prompt")
    {
        // Agent/prompt steps render text; loops and conditions around them
        // do not execute shell code. Embedded executable helpers still
        // count, as do unknown fields.
        if let Some(helpers) = map
            .get("macros")
            .or_else(|| map.get("xprompts"))
            .and_then(Value::as_object)
        {
            if helpers.values().any(is_executable_macro_definition) {
                return true;
            }
        }
        for (key, value) in map {
            match key.as_str() {
                "name" | "agent" | "prompt" | "prompt_part" | "output"
                | "inputs" | "input" | "hitl" | "hidden" | "condition"
                | "if" | "for" | "repeat" | "while" | "join" | "finally"
                | "artifact" | "on_error" | "tags" | "description"
                | "environment" | "macros" | "xprompts" => continue,
                _ => {
                    if (value.is_object() || value.is_array())
                        && contains_executable_key(value)
                    {
                        return true;
                    }
                    return true;
                }
            }
        }
        return false;
    }
    // A step with none of the recognized worker keys is ambiguous;
    // block it conservatively (except an empty mapping, which parses to
    // no valid step and carries no command).
    !map.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn projection_request(layer: Value) -> LocalCommandProjectionRequestWire {
        LocalCommandProjectionRequestWire {
            layer,
            lower_macros: Value::Object(Map::new()),
            lower_workflows: Value::Object(Map::new()),
            source: "test".to_string(),
            config_path: None,
        }
    }

    #[test]
    fn empty_layer_projects_to_empty_digest() {
        let first =
            project_local_commands(&projection_request(json!({}))).unwrap();
        let second =
            project_local_commands(&projection_request(json!({"alias": "x"})))
                .unwrap();
        assert_eq!(first.projection, json!({}));
        assert_eq!(first.digest, second.digest);
        assert!(first.blocked_paths.is_empty());
        assert_eq!(
            first.schema_version,
            LOCAL_COMMAND_TRUST_WIRE_SCHEMA_VERSION
        );
        assert_eq!(first.policy_version, LOCAL_COMMAND_TRUST_POLICY_VERSION);
    }

    #[test]
    fn non_command_keys_do_not_change_digest() {
        let base =
            project_local_commands(&projection_request(json!({}))).unwrap();
        let with_display = project_local_commands(&projection_request(json!({
            "timezone": "UTC",
            "alias": "x",
            "ace": {"prompt_completion": {"auto_macro_menu": false}},
        })))
        .unwrap();
        assert_eq!(base.digest, with_display.digest);
    }

    #[test]
    fn legacy_spellings_normalize_before_projection() {
        let legacy = project_local_commands(&projection_request(json!({
            "xprompts": {"hello": "world"},
        })))
        .unwrap();
        assert_eq!(legacy.projection, json!({}));
        let legacy_exec = project_local_commands(&projection_request(json!({
            "xprompts": {"tool": {"content": "hi", "steps": [{"bash": "echo hi"}]}},
        })))
        .unwrap();
        assert_eq!(
            legacy_exec.projection,
            json!({"macros": {"tool": {"content": "hi", "steps": [{"bash": "echo hi"}]}}})
        );
        assert!(legacy_exec
            .blocked_paths
            .contains(&"macros.tool".to_string()));
    }

    #[test]
    fn commit_hooks_whole_contribution_is_command_bearing() {
        let out = project_local_commands(&projection_request(json!({
            "commit_hooks": {"before": "", "after_rebase_timeout_seconds": 10},
        })))
        .unwrap();
        assert_eq!(
            out.projection,
            json!({"commit_hooks": {"after_rebase_timeout_seconds": 10, "before": ""}})
        );
        assert!(out.blocked_paths.contains(&"commit_hooks".to_string()));
    }

    #[test]
    fn file_hook_template_reference_is_command_bearing() {
        let out = project_local_commands(&projection_request(json!({
            "file_hooks": [{"use": "builtin@fmt", "name": "fmt"}],
        })))
        .unwrap();
        assert_eq!(
            out.projection,
            json!({"file_hooks": [{"name": "fmt", "use": "builtin@fmt"}]})
        );
        assert!(out.blocked_paths.contains(&"file_hooks[0]".to_string()));
    }

    #[test]
    fn nested_parallel_bash_step_is_command_bearing() {
        let out = project_local_commands(&projection_request(json!({
            "workflows": {"deploy": {"steps": [
                {"name": "fan", "parallel": [
                    {"name": "a", "agent": "review"},
                    {"name": "b", "bash": "echo hi"},
                ]},
            ]}},
        })))
        .unwrap();
        assert!(out.blocked_paths.contains(&"workflows.deploy".to_string()));
    }

    #[test]
    fn prompt_only_workflow_stays_available() {
        let out = project_local_commands(&projection_request(json!({
            "workflows": {"notes": {"steps": [
                {"name": "main", "prompt_part": "hello"},
            ]}},
        })))
        .unwrap();
        assert_eq!(out.projection, json!({}));
    }

    #[test]
    fn sparse_override_of_executable_lower_is_command_bearing() {
        let request = LocalCommandProjectionRequestWire {
            layer: json!({"macros": {"tool": {"input": {"q": "word"}}}}),
            lower_macros: json!({"tool": {"content": "hi", "bash": "echo hi"}}),
            lower_workflows: json!({}),
            source: "test".to_string(),
            config_path: None,
        };
        let out = project_local_commands(&request).unwrap();
        assert!(out.blocked_paths.contains(&"macros.tool".to_string()));
    }

    #[test]
    fn mapping_order_and_whitespace_do_not_change_digest() {
        let first = project_local_commands(&projection_request(json!({
            "commit_hooks": {"before": "just fix", "after": "echo done"},
        })))
        .unwrap();
        let second = project_local_commands(&projection_request(json!({
            "commit_hooks": {"after": "echo done", "before": "just fix"},
        })))
        .unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(first.canonical_json, second.canonical_json);
        assert!(first.digest.len() == 64);
    }

    #[test]
    fn selected_config_path_is_part_of_the_digest() {
        let mut first_request = projection_request(json!({
            "commit_hooks": {"before": "just fix"},
        }));
        first_request.config_path = Some("sase/sase.yml".to_string());
        let mut second_request = first_request.clone();
        second_request.config_path = Some("sase.yml".to_string());

        let first = project_local_commands(&first_request).unwrap();
        let second = project_local_commands(&second_request).unwrap();
        assert_ne!(first.digest, second.digest);
        assert_eq!(first.projection, second.projection);
        assert!(first.canonical_json.contains("config_path"));
    }

    #[test]
    fn filter_keeps_non_command_keys_and_removes_blocked() {
        let out = filter_trusted_local_layer(&LocalTrustFilterRequestWire {
            layer: json!({
                "timezone": "UTC",
                "commit_hooks": {"before": "just fix"},
                "macros": {"hello": "world"},
            }),
            trusted: false,
            lower_macros: json!({}),
            lower_workflows: json!({}),
            source: "test".to_string(),
            config_path: None,
        })
        .unwrap();
        assert_eq!(
            out.effective_layer,
            json!({"macros": {"hello": "world"}, "timezone": "UTC"})
        );
        assert!(out.removed_paths.contains(&"commit_hooks".to_string()));
        assert_eq!(out.status, "untrusted-filtered");
    }

    #[test]
    fn decision_matches_current_digest_only() {
        let trust =
            decide_local_command_trust(&LocalTrustDecisionRequestWire {
                current_digest: "abc".to_string(),
                acknowledged_digest: None,
                current_policy_version: LOCAL_COMMAND_TRUST_POLICY_VERSION,
                acknowledged_policy_version: None,
            });
        assert_eq!(trust.status, "untrusted");
        assert_eq!(trust.reason, "no-acknowledgment");
        let trust =
            decide_local_command_trust(&LocalTrustDecisionRequestWire {
                current_digest: "abc".to_string(),
                acknowledged_digest: Some("abc".to_string()),
                current_policy_version: LOCAL_COMMAND_TRUST_POLICY_VERSION,
                acknowledged_policy_version: Some(
                    LOCAL_COMMAND_TRUST_POLICY_VERSION,
                ),
            });
        assert_eq!(trust.status, "trusted");
        let stale =
            decide_local_command_trust(&LocalTrustDecisionRequestWire {
                current_digest: "def".to_string(),
                acknowledged_digest: Some("abc".to_string()),
                current_policy_version: LOCAL_COMMAND_TRUST_POLICY_VERSION,
                acknowledged_policy_version: Some(
                    LOCAL_COMMAND_TRUST_POLICY_VERSION,
                ),
            });
        assert_eq!(stale.reason, "digest-mismatch");
    }
}

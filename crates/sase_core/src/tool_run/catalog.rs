//! Catalog normalization and definition-identity hashing.

use serde::Serialize;

use super::canonical::canonical_digest;
use super::wire::{
    ToolDefinitionNormalizeResultWire, ToolDefinitionWire,
    ToolFingerprintSpecWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};
use super::ToolRunError;

#[derive(Serialize)]
struct ToolDefinitionIdentity<'a> {
    name: &'a str,
    argv: &'a [String],
    stages: &'a str,
    inputs: &'a [String],
    env: &'a [String],
    args: &'a str,
    fingerprint: &'a ToolFingerprintSpecWire,
}

pub fn normalize_tool_definition(
    mut definition: ToolDefinitionWire,
) -> Result<ToolDefinitionNormalizeResultWire, ToolRunError> {
    if definition.schema_version != TOOL_RUN_WIRE_SCHEMA_VERSION {
        return Err(ToolRunError::SchemaVersion {
            expected: TOOL_RUN_WIRE_SCHEMA_VERSION,
            actual: definition.schema_version,
        });
    }
    let mut diagnostics = definition.diagnostics.clone();
    definition.name = definition.name.trim().to_string();
    if definition.name.is_empty() {
        return Err(ToolRunError::invalid(
            "tool definition name must not be empty",
        ));
    }
    if definition.argv.is_empty()
        || definition.argv.iter().any(|part| part.is_empty())
    {
        return Err(ToolRunError::invalid(
            "tool definition argv must be a nonempty array of nonempty strings",
        ));
    }
    definition.inputs = normalize_string_list(&definition.inputs, "inputs")?;
    for input in &definition.inputs {
        validate_repo_relative_path(input)?;
    }
    definition.env = normalize_string_list(&definition.env, "env")?;
    for name in &definition.env {
        validate_env_name(name)?;
    }
    definition.fingerprint.repos = normalize_string_list(
        &definition.fingerprint.repos,
        "fingerprint.repos",
    )?;
    for identity in &definition.fingerprint.repos {
        if identity.is_empty() {
            return Err(ToolRunError::invalid(
                "fingerprint.repos identities must not be empty",
            ));
        }
    }
    let mut toolchain = definition.fingerprint.toolchain.clone();
    for (name, argv) in toolchain.iter_mut() {
        if name.trim().is_empty() {
            return Err(ToolRunError::invalid(
                "fingerprint.toolchain probe names must not be empty",
            ));
        }
        if argv.is_empty() || argv.iter().any(|part| part.is_empty()) {
            return Err(ToolRunError::invalid(format!(
                "fingerprint.toolchain probe {name:?} argv must be a nonempty array of nonempty strings"
            )));
        }
    }
    definition.fingerprint.toolchain = toolchain;
    let digest = definition_digest(&definition)?;
    definition.diagnostics = diagnostics.clone();
    diagnostics.push(format!("normalized definition digest {digest}"));
    Ok(ToolDefinitionNormalizeResultWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        definition,
        digest,
        diagnostics,
    })
}

pub fn definition_digest(
    definition: &ToolDefinitionWire,
) -> Result<String, ToolRunError> {
    let identity = ToolDefinitionIdentity {
        name: &definition.name,
        argv: &definition.argv,
        stages: definition.stages.as_str(),
        inputs: &definition.inputs,
        env: &definition.env,
        args: definition.args.as_str(),
        fingerprint: &definition.fingerprint,
    };
    canonical_digest(&identity).map_err(ToolRunError::invalid)
}

pub fn extra_args_digest(
    extra_args: &[String],
) -> Result<String, ToolRunError> {
    canonical_digest(&extra_args).map_err(ToolRunError::invalid)
}

fn normalize_string_list(
    values: &[String],
    field: &str,
) -> Result<Vec<String>, ToolRunError> {
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Err(ToolRunError::invalid(format!(
                "{field} entries must not be empty"
            )));
        }
        if !out.iter().any(|existing| existing == trimmed) {
            out.push(trimmed.to_string());
        }
    }
    out.sort();
    Ok(out)
}

fn validate_repo_relative_path(path: &str) -> Result<(), ToolRunError> {
    if path.starts_with('/') || path.starts_with('\\') {
        return Err(ToolRunError::invalid(format!(
            "input path {path:?} must be repository-relative"
        )));
    }
    if path.contains('\0') || path.contains('\\') {
        return Err(ToolRunError::invalid(format!(
            "input path {path:?} contains invalid characters"
        )));
    }
    for component in path.split('/') {
        if component.is_empty() || component == "." || component == ".." {
            return Err(ToolRunError::invalid(format!(
                "input path {path:?} is not a valid repository-relative glob"
            )));
        }
    }
    Ok(())
}

fn validate_env_name(name: &str) -> Result<(), ToolRunError> {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return Err(ToolRunError::invalid("env name must not be empty"));
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(ToolRunError::invalid(format!(
            "env name {name:?} must start with A-Z or _"
        )));
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(ToolRunError::invalid(format!(
            "env name {name:?} must match [A-Za-z_][A-Za-z0-9_]*"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool_run::wire::{ToolArgsPolicyWire, ToolStagesWire};

    fn sample_definition() -> ToolDefinitionWire {
        ToolDefinitionWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            name: "check".to_string(),
            argv: vec!["just".into(), "check".into()],
            description: "Repository check".to_string(),
            stages: ToolStagesWire::RunSilent,
            inputs: vec!["Justfile".into(), "src/**/*.py".into()],
            env: vec!["SASE_PYTEST_WORKERS".into()],
            args: ToolArgsPolicyWire::Deny,
            fingerprint: ToolFingerprintSpecWire {
                repos: vec!["sase".into()],
                toolchain: [(
                    "python".to_string(),
                    vec!["python".into(), "--version".into()],
                )]
                .into_iter()
                .collect(),
            },
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn rejects_empty_argv_and_unknown_schema() {
        let mut empty = sample_definition();
        empty.argv.clear();
        assert!(normalize_tool_definition(empty).is_err());

        let mut schema = sample_definition();
        schema.schema_version = 2;
        match normalize_tool_definition(schema) {
            Err(ToolRunError::SchemaVersion { actual: 2, .. }) => {}
            other => panic!("expected schema error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_absolute_and_parent_input_paths() {
        let mut absolute = sample_definition();
        absolute.inputs = vec!["/etc/passwd".into()];
        assert!(normalize_tool_definition(absolute).is_err());

        let mut parent = sample_definition();
        parent.inputs = vec!["../secret".into()];
        assert!(normalize_tool_definition(parent).is_err());
    }

    #[test]
    fn digest_is_stable_for_reordered_optional_lists() {
        let mut left = sample_definition();
        left.inputs = vec!["b".into(), "a".into()];
        left.env = vec!["FOO".into(), "BAR".into()];
        let mut right = sample_definition();
        right.inputs = vec!["a".into(), "b".into()];
        right.env = vec!["BAR".into(), "FOO".into()];
        let left = normalize_tool_definition(left).unwrap();
        let right = normalize_tool_definition(right).unwrap();
        assert_eq!(left.digest, right.digest);
        assert_eq!(left.definition.inputs, ["a", "b"]);
        assert_eq!(right.definition.inputs, ["a", "b"]);
        assert_eq!(left.definition.env, ["BAR", "FOO"]);
    }

    #[test]
    fn default_args_policy_is_deny() {
        let definition = sample_definition();
        assert_eq!(definition.args, ToolArgsPolicyWire::Deny);
    }

    #[test]
    fn golden_definition_fixture_normalizes() {
        let fixture: ToolDefinitionWire = serde_json::from_str(include_str!(
            "fixtures/definition_check.json"
        ))
        .unwrap();
        let normalized = normalize_tool_definition(fixture).unwrap();
        assert_eq!(normalized.definition.name, "check");
        assert_eq!(normalized.definition.stages, ToolStagesWire::RunSilent);
        assert_eq!(normalized.definition.args, ToolArgsPolicyWire::Deny);
        assert_eq!(normalized.digest.len(), 64);
    }
}

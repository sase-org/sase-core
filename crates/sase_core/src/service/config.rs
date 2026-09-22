//! `service.procs` config composition.
//!
//! Modeled on `config::axe`, but much simpler: service proc entries have no
//! aliases and no legacy list representation, so every layer merges
//! field-by-field with whole-value replacement (the one exception is `env`,
//! which merges key by key). Composition never fails outright; a malformed
//! `service` section is reported as a `fatal` diagnostic and a malformed
//! entry is reported as `available: false`, so a caller can still render the
//! rest of the config.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{validate_service_proc_name, RESERVED_BUILTIN_SERVICE_PROCS};
pub use crate::config::wire::ConfigDiagnosticWire;
use crate::config::wire::ConfigLayerInputWire;

/// Wire schema version for the `service.procs` composition contract.
pub const SERVICE_CONFIG_WIRE_SCHEMA_VERSION: u32 = 1;

const ALLOWED_RESTART_POLICIES: [&str; 3] = ["always", "on-failure", "never"];
const ALLOWED_STOP_SIGNALS: [&str; 7] =
    ["HUP", "INT", "QUIT", "KILL", "USR1", "USR2", "TERM"];

const DEFAULT_MODE: &str = "daemon";
const DEFAULT_RESTART: &str = "on-failure";
const DEFAULT_STOP_SIGNAL: &str = "SIGTERM";
const DEFAULT_STOP_TIMEOUT_SECONDS: f64 = 10.0;
const DEFAULT_LOG_MAX_BYTES: u64 = 2_097_152;

/// Request over the standard ordered config layer input.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceConfigComposeRequestWire {
    #[serde(default)]
    pub layers: Vec<ConfigLayerInputWire>,
}

/// One layer's contribution to a single field, for the provenance rail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceFieldProvenanceWire {
    pub field: String,
    pub layer: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// The layer (if any) that supplied the effective `enabled` value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceEnablementSourceWire {
    pub explicit: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A resolved launch target: either a shell/argv command or a builtin.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ServiceLauncherWire {
    Command { command: Value, argv: Vec<String> },
    Builtin { builtin: String },
}

/// One effective `service.procs.<name>` entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceProcConfigWire {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub available: bool,
    #[serde(default)]
    pub unavailable_reasons: Vec<String>,
    pub source: String,
    pub declared_by: String,
    pub enabled: bool,
    pub enablement: ServiceEnablementSourceWire,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launcher: Option<ServiceLauncherWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    pub env: BTreeMap<String, String>,
    pub restart: String,
    pub success_exit_codes: Vec<i32>,
    pub stop_signal: String,
    pub stop_timeout_seconds: f64,
    pub after: Vec<String>,
    pub log_max_bytes: u64,
    pub field_provenance: Vec<ServiceFieldProvenanceWire>,
}

/// The full `service.procs` composition: effective entries, diagnostics, and
/// the labels of layers whose `service` section was ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceConfigCompositionWire {
    pub schema_version: u32,
    pub fatal: bool,
    pub procs: Vec<ServiceProcConfigWire>,
    pub diagnostics: Vec<ConfigDiagnosticWire>,
    pub ignored_layers: Vec<String>,
}

fn layer_label(layer: &ConfigLayerInputWire) -> String {
    match layer.path.as_deref() {
        Some(path) => format!("{}:{path}", layer.name),
        None => layer.name.clone(),
    }
}

const ALLOWED_LAYER_KINDS: [&str; 5] =
    ["builtin", "plugin", "user", "overlay", "local"];

fn classify_source(layer_kind: &str) -> &'static str {
    match layer_kind {
        "builtin" => "builtin",
        "plugin" => "plugin",
        "user" | "overlay" | "local" => "user",
        // Unknown kinds are rejected before composition reaches this point,
        // so this arm is unreachable in practice.
        _ => "user",
    }
}

#[derive(Debug, Clone, Default)]
struct EntryBuilder {
    source: String,
    declared_by: String,
    description: Option<String>,
    enabled: Option<bool>,
    enablement: ServiceEnablementSourceWire,
    mode: Option<String>,
    command_raw: Option<Value>,
    argv: Option<Vec<String>>,
    builtin: Option<String>,
    cwd: Option<String>,
    env: BTreeMap<String, String>,
    restart: Option<String>,
    success_exit_codes: Option<Vec<i32>>,
    stop_signal: Option<String>,
    stop_timeout_seconds: Option<f64>,
    after: Option<Vec<String>>,
    log_max_bytes: Option<u64>,
    field_provenance: BTreeMap<String, ServiceFieldProvenanceWire>,
    unavailable_reasons: Vec<String>,
}

impl EntryBuilder {
    fn record_field(&mut self, field: &str, label: &str, path: Option<&str>) {
        self.field_provenance.insert(
            field.to_string(),
            ServiceFieldProvenanceWire {
                field: field.to_string(),
                layer: label.to_string(),
                path: path.map(str::to_string),
            },
        );
    }
}

/// Compose the `service.procs` layer stack into effective entries.
pub fn compose_service_config(
    request: &ServiceConfigComposeRequestWire,
) -> ServiceConfigCompositionWire {
    let mut entries: BTreeMap<String, EntryBuilder> = BTreeMap::new();
    let mut diagnostics: Vec<ConfigDiagnosticWire> = Vec::new();
    let mut ignored_layers: Vec<String> = Vec::new();
    let mut fatal = false;

    for layer in &request.layers {
        let label = layer_label(layer);

        if let Some(error) = layer.error.as_deref() {
            if layer.kind == "local" {
                diagnostics.push(ConfigDiagnosticWire {
                    severity: "warning".to_string(),
                    code: "service_config_layer_error".to_string(),
                    message: format!(
                        "layer `{label}`{} has a config error: {error}; \
                         ignoring its `service` section",
                        layer
                            .path
                            .as_deref()
                            .map_or(String::new(), |path| format!(
                                " at `{path}`"
                            )),
                    ),
                    path: Some("service".to_string()),
                    layer: Some(label.clone()),
                });
            } else {
                fatal = true;
                diagnostics.push(ConfigDiagnosticWire {
                    severity: "error".to_string(),
                    code: "service_config_layer_error".to_string(),
                    message: format!(
                        "layer `{label}`{} has a config error: {error}; \
                         ignoring its `service` section",
                        layer
                            .path
                            .as_deref()
                            .map_or(String::new(), |path| format!(
                                " at `{path}`"
                            )),
                    ),
                    path: Some("service".to_string()),
                    layer: Some(label.clone()),
                });
            }
            continue;
        }

        if !ALLOWED_LAYER_KINDS.contains(&layer.kind.as_str()) {
            fatal = true;
            diagnostics.push(ConfigDiagnosticWire {
                severity: "error".to_string(),
                code: "service_config_unknown_layer_kind".to_string(),
                message: format!(
                    "layer `{label}` has unknown kind `{}`; \
                     ignoring its `service` section",
                    layer.kind,
                ),
                path: Some("service".to_string()),
                layer: Some(label.clone()),
            });
            continue;
        }

        let Some(raw_service) = layer.value.get("service") else {
            continue;
        };

        if layer.kind == "local" {
            diagnostics.push(ConfigDiagnosticWire {
                severity: "warning".to_string(),
                code: "service_config_ignored_in_project_layer".to_string(),
                message: "project-local `service` config is ignored; the \
                          service host resolves machine-level layers only"
                    .to_string(),
                path: Some("service".to_string()),
                layer: Some(label.clone()),
            });
            ignored_layers.push(label);
            continue;
        }

        let Some(service_obj) = raw_service.as_object() else {
            fatal = true;
            diagnostics.push(section_error(
                &label,
                "service",
                "service must be a mapping",
            ));
            continue;
        };
        let extra_keys: Vec<&String> = service_obj
            .keys()
            .filter(|key| key.as_str() != "procs")
            .collect();
        if !extra_keys.is_empty() {
            fatal = true;
            diagnostics.push(section_error(
                &label,
                "service",
                "service must contain only a `procs` key",
            ));
            continue;
        }
        let Some(procs_value) = service_obj.get("procs") else {
            continue;
        };
        let Some(procs_map) = procs_value.as_object() else {
            fatal = true;
            diagnostics.push(section_error(
                &label,
                "service.procs",
                "service.procs must be a mapping",
            ));
            continue;
        };

        let source = classify_source(&layer.kind);
        for (name, entry_value) in procs_map {
            let builder =
                entries.entry(name.clone()).or_insert_with(|| EntryBuilder {
                    source: source.to_string(),
                    declared_by: label.clone(),
                    ..Default::default()
                });

            let Some(fields) = entry_value.as_object() else {
                record_error(
                    &mut diagnostics,
                    &mut builder.unavailable_reasons,
                    &label,
                    format!("service.procs.{name}"),
                    "service_config_entry_not_a_mapping",
                    "entry must be a mapping".to_string(),
                );
                continue;
            };

            for (field, value) in fields {
                apply_field(
                    name,
                    field,
                    value,
                    layer,
                    &label,
                    builder,
                    &mut diagnostics,
                );
            }
        }
    }

    let procs = finish_entries(entries, &mut diagnostics);

    ServiceConfigCompositionWire {
        schema_version: SERVICE_CONFIG_WIRE_SCHEMA_VERSION,
        fatal,
        procs,
        diagnostics,
        ignored_layers,
    }
}

fn section_error(
    label: &str,
    path: &str,
    message: &str,
) -> ConfigDiagnosticWire {
    ConfigDiagnosticWire {
        severity: "error".to_string(),
        code: "service_config_invalid_section".to_string(),
        message: message.to_string(),
        path: Some(path.to_string()),
        layer: Some(label.to_string()),
    }
}

fn record_error(
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
    unavailable_reasons: &mut Vec<String>,
    label: &str,
    path: String,
    code: &str,
    message: String,
) {
    diagnostics.push(ConfigDiagnosticWire {
        severity: "error".to_string(),
        code: code.to_string(),
        message: message.clone(),
        path: Some(path),
        layer: Some(label.to_string()),
    });
    unavailable_reasons.push(message);
}

fn record_warning(
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
    label: &str,
    path: String,
    code: &str,
    message: String,
) {
    diagnostics.push(ConfigDiagnosticWire {
        severity: "warning".to_string(),
        code: code.to_string(),
        message,
        path: Some(path),
        layer: Some(label.to_string()),
    });
}

#[allow(clippy::too_many_arguments)]
fn apply_field(
    name: &str,
    field: &str,
    value: &Value,
    layer: &ConfigLayerInputWire,
    label: &str,
    builder: &mut EntryBuilder,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) {
    let path = format!("service.procs.{name}.{field}");
    let mut invalid = |message: String, code: &str| {
        record_error(
            diagnostics,
            &mut builder.unavailable_reasons,
            label,
            path.clone(),
            code,
            message,
        );
    };

    match field {
        "description" => match value.as_str() {
            Some(s) => {
                builder.description = Some(s.to_string());
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid("description must be a string".to_string(), "service_proc_invalid_field"),
        },
        "enabled" => match value.as_bool() {
            Some(b) => {
                builder.enabled = Some(b);
                builder.enablement = ServiceEnablementSourceWire {
                    explicit: true,
                    layer: Some(label.to_string()),
                    layer_kind: Some(layer.kind.clone()),
                    path: layer.path.clone(),
                };
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid("enabled must be a boolean".to_string(), "service_proc_invalid_field"),
        },
        "mode" => match value.as_str() {
            Some("daemon") => {
                builder.mode = Some("daemon".to_string());
                builder.record_field(field, label, layer.path.as_deref());
            }
            Some("oneshot") => invalid(
                "oneshot service procs are transient-only; use `sase service proc run`"
                    .to_string(),
                "service_oneshot_not_configurable",
            ),
            _ => invalid(
                "mode must be \"daemon\"".to_string(),
                "service_proc_invalid_field",
            ),
        },
        "command" => match parse_command(value) {
            Some(argv) => {
                builder.command_raw = Some(value.clone());
                builder.argv = Some(argv);
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid(
                "command must be a non-empty string or a non-empty list of \
                 non-empty strings"
                    .to_string(),
                "service_proc_invalid_field",
            ),
        },
        "builtin" => match value.as_str() {
            Some(s) if RESERVED_BUILTIN_SERVICE_PROCS.contains(&s) => {
                builder.builtin = Some(s.to_string());
                builder.record_field(field, label, layer.path.as_deref());
            }
            _ => invalid(
                format!(
                    "builtin must be one of: {}",
                    RESERVED_BUILTIN_SERVICE_PROCS.join(", ")
                ),
                "service_proc_invalid_field",
            ),
        },
        "cwd" => match value.as_str() {
            Some(s) if !s.is_empty() => {
                builder.cwd = Some(s.to_string());
                builder.record_field(field, label, layer.path.as_deref());
            }
            _ => invalid("cwd must be a non-empty string".to_string(), "service_proc_invalid_field"),
        },
        "env" => match parse_env(value) {
            Ok(entries) => {
                for (key, val) in entries {
                    builder.env.insert(key, val);
                }
                builder.record_field(field, label, layer.path.as_deref());
            }
            Err(message) => invalid(message, "service_proc_invalid_field"),
        },
        "restart" => match value.as_str() {
            Some(s) if ALLOWED_RESTART_POLICIES.contains(&s) => {
                builder.restart = Some(s.to_string());
                builder.record_field(field, label, layer.path.as_deref());
            }
            _ => invalid(
                format!(
                    "restart must be one of: {}",
                    ALLOWED_RESTART_POLICIES.join(", ")
                ),
                "service_proc_invalid_field",
            ),
        },
        "success_exit_codes" => match parse_exit_codes(value) {
            Some(codes) => {
                builder.success_exit_codes = Some(codes);
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid(
                "success_exit_codes must be a list of integers in 0..=255".to_string(),
                "service_proc_invalid_field",
            ),
        },
        "stop_signal" => match value.as_str().and_then(normalize_stop_signal) {
            Some(normalized) => {
                builder.stop_signal = Some(normalized);
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid(
                format!(
                    "stop_signal must be one of (with or without a SIG prefix): {}",
                    ALLOWED_STOP_SIGNALS.join(", ")
                ),
                "service_proc_invalid_field",
            ),
        },
        "stop_timeout" => match value.as_f64() {
            Some(n) if n > 0.0 && n <= 3600.0 => {
                builder.stop_timeout_seconds = Some(n);
                builder.record_field(field, label, layer.path.as_deref());
            }
            _ => invalid(
                "stop_timeout must be a number greater than 0 and at most 3600".to_string(),
                "service_proc_invalid_field",
            ),
        },
        "after" => match parse_string_list(value) {
            Some(items) => {
                builder.after = Some(items);
                builder.record_field(field, label, layer.path.as_deref());
            }
            None => invalid("after must be a list of strings".to_string(), "service_proc_invalid_field"),
        },
        "log_max_bytes" => match value.as_u64() {
            Some(n) if n >= 4096 => {
                builder.log_max_bytes = Some(n);
                builder.record_field(field, label, layer.path.as_deref());
            }
            _ => invalid(
                "log_max_bytes must be an integer of at least 4096".to_string(),
                "service_proc_invalid_field",
            ),
        },
        other => invalid(
            format!("unknown service proc field `{other}`"),
            "unknown_service_proc_field",
        ),
    }
}

fn parse_command(value: &Value) -> Option<Vec<String>> {
    if let Some(s) = value.as_str() {
        if s.is_empty() {
            return None;
        }
        return Some(vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            s.to_string(),
        ]);
    }
    if let Some(items) = value.as_array() {
        if items.is_empty() {
            return None;
        }
        let mut argv = Vec::with_capacity(items.len());
        for item in items {
            let s = item.as_str()?;
            if s.is_empty() {
                return None;
            }
            argv.push(s.to_string());
        }
        return Some(argv);
    }
    None
}

fn parse_env(value: &Value) -> Result<Vec<(String, String)>, String> {
    let Some(obj) = value.as_object() else {
        return Err("env must be a mapping".to_string());
    };
    let mut out = Vec::with_capacity(obj.len());
    for (key, val) in obj {
        if !is_valid_env_name(key) {
            return Err(format!(
                "env key `{key}` must match `[A-Za-z_][A-Za-z0-9_]*`"
            ));
        }
        let Some(s) = val.as_str() else {
            return Err(format!("env value for `{key}` must be a string"));
        };
        validate_env_reference_value(s).map_err(|reason| {
            format!("env value for `{key}` is invalid: {reason}")
        })?;
        out.push((key.clone(), s.to_string()));
    }
    Ok(out)
}

fn is_valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn is_valid_env_ref_name(name: &str) -> bool {
    !name.is_empty() && is_valid_env_name(name)
}

/// Scan an env value for well-formed `${NAME}` references. `$$` is a literal
/// `$`; any other bare `$` is malformed.
fn validate_env_reference_value(value: &str) -> Result<(), String> {
    let chars: Vec<char> = value.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] != '$' {
            i += 1;
            continue;
        }
        if chars.get(i + 1) == Some(&'$') {
            i += 2;
            continue;
        }
        if chars.get(i + 1) == Some(&'{') {
            let close = chars[i + 2..].iter().position(|c| *c == '}');
            match close {
                Some(offset) => {
                    let name: String =
                        chars[i + 2..i + 2 + offset].iter().collect();
                    if !is_valid_env_ref_name(&name) {
                        return Err(format!(
                            "malformed reference `${{{name}}}`"
                        ));
                    }
                    i = i + 2 + offset + 1;
                    continue;
                }
                None => return Err("unterminated `${` reference".to_string()),
            }
        }
        return Err(
            "bare `$` must be `$$` or start a `${NAME}` reference".to_string()
        );
    }
    Ok(())
}

fn parse_exit_codes(value: &Value) -> Option<Vec<i32>> {
    let items = value.as_array()?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let n = item.as_i64()?;
        if !(0..=255).contains(&n) {
            return None;
        }
        out.push(n as i32);
    }
    Some(out)
}

fn parse_string_list(value: &Value) -> Option<Vec<String>> {
    let items = value.as_array()?;
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(item.as_str()?.to_string());
    }
    Some(out)
}

fn normalize_stop_signal(value: &str) -> Option<String> {
    let token = value.strip_prefix("SIG").unwrap_or(value);
    if ALLOWED_STOP_SIGNALS.contains(&token) {
        Some(format!("SIG{token}"))
    } else {
        None
    }
}

fn finish_entries(
    entries: BTreeMap<String, EntryBuilder>,
    diagnostics: &mut Vec<ConfigDiagnosticWire>,
) -> Vec<ServiceProcConfigWire> {
    let names: BTreeMap<String, ()> =
        entries.keys().map(|name| (name.clone(), ())).collect();

    let mut resolved: BTreeMap<String, (ServiceProcConfigWire, Vec<String>)> =
        BTreeMap::new();

    for (name, builder) in entries {
        let EntryBuilder {
            source,
            declared_by,
            description,
            enabled,
            enablement,
            mode,
            command_raw,
            argv,
            builtin,
            cwd,
            env,
            restart,
            success_exit_codes,
            stop_signal,
            stop_timeout_seconds,
            after,
            log_max_bytes,
            field_provenance,
            mut unavailable_reasons,
        } = builder;

        if let Err(reason) = validate_service_proc_name(&name) {
            record_error(
                diagnostics,
                &mut unavailable_reasons,
                &declared_by,
                format!("service.procs.{name}"),
                "service_proc_invalid_name",
                reason,
            );
        }

        let enabled_default = source != "plugin";
        let enabled = enabled.unwrap_or(enabled_default);

        let mut launcher = None;
        match (command_raw, argv, builtin.clone()) {
            (Some(command), Some(argv), None) => {
                launcher = Some(ServiceLauncherWire::Command { command, argv });
            }
            (None, None, Some(builtin)) => {
                launcher = Some(ServiceLauncherWire::Builtin { builtin });
            }
            (None, None, None) => {
                let mut message =
                    "no launcher: set `command` or `builtin`".to_string();
                if source == "user" {
                    message.push_str(
                        " (is the plugin that declares it installed?)",
                    );
                }
                record_error(
                    diagnostics,
                    &mut unavailable_reasons,
                    &declared_by,
                    format!("service.procs.{name}"),
                    "service_proc_no_launcher",
                    message,
                );
            }
            _ => {
                record_error(
                    diagnostics,
                    &mut unavailable_reasons,
                    &declared_by,
                    format!("service.procs.{name}"),
                    "service_proc_launcher_conflict",
                    "entry declares both `command` and `builtin`".to_string(),
                );
            }
        }

        if let Some(builtin_value) = builtin.as_deref() {
            if builtin_value != name {
                record_error(
                    diagnostics,
                    &mut unavailable_reasons,
                    &declared_by,
                    format!("service.procs.{name}.builtin"),
                    "service_proc_builtin_name_mismatch",
                    format!("builtin `{builtin_value}` must match entry name `{name}`"),
                );
            }
        } else if RESERVED_BUILTIN_SERVICE_PROCS.contains(&name.as_str())
            && launcher.is_some()
        {
            record_error(
                diagnostics,
                &mut unavailable_reasons,
                &declared_by,
                format!("service.procs.{name}"),
                "service_proc_reserved_name_misuse",
                format!("reserved service proc `{name}` must use its own builtin launcher"),
            );
        }

        let raw_after = after.unwrap_or_default();
        let mut effective_after = Vec::with_capacity(raw_after.len());
        for target in raw_after {
            if target == name {
                record_error(
                    diagnostics,
                    &mut unavailable_reasons,
                    &declared_by,
                    format!("service.procs.{name}.after"),
                    "service_proc_after_self_reference",
                    format!(
                        "`{name}` cannot declare `after: [{name}]` on itself"
                    ),
                );
                continue;
            }
            if !names.contains_key(&target) {
                record_warning(
                    diagnostics,
                    &declared_by,
                    format!("service.procs.{name}.after"),
                    "unknown_service_proc_after",
                    format!("after target `{target}` is not a configured service proc"),
                );
                continue;
            }
            effective_after.push(target);
        }

        let field_provenance: Vec<ServiceFieldProvenanceWire> =
            field_provenance.into_values().collect();

        let wire = ServiceProcConfigWire {
            name: name.clone(),
            description,
            available: unavailable_reasons.is_empty(),
            unavailable_reasons: unavailable_reasons.clone(),
            source,
            declared_by,
            enabled,
            enablement,
            mode: mode.unwrap_or_else(|| DEFAULT_MODE.to_string()),
            launcher,
            cwd,
            env,
            restart: restart.unwrap_or_else(|| DEFAULT_RESTART.to_string()),
            success_exit_codes: success_exit_codes.unwrap_or_default(),
            stop_signal: stop_signal
                .unwrap_or_else(|| DEFAULT_STOP_SIGNAL.to_string()),
            stop_timeout_seconds: stop_timeout_seconds
                .unwrap_or(DEFAULT_STOP_TIMEOUT_SECONDS),
            after: effective_after.clone(),
            log_max_bytes: log_max_bytes.unwrap_or(DEFAULT_LOG_MAX_BYTES),
            field_provenance,
        };
        resolved.insert(name, (wire, effective_after));
    }

    let cycle_members = detect_after_cycles(&resolved);
    for name in &cycle_members {
        if let Some((wire, _)) = resolved.get_mut(name) {
            let reason =
                format!("service proc `{name}` is part of an `after` cycle");
            diagnostics.push(ConfigDiagnosticWire {
                severity: "error".to_string(),
                code: "service_proc_after_cycle".to_string(),
                message: reason.clone(),
                path: Some(format!("service.procs.{name}.after")),
                layer: Some(wire.declared_by.clone()),
            });
            wire.unavailable_reasons.push(reason);
            wire.available = false;
        }
    }

    resolved.into_values().map(|(wire, _)| wire).collect()
}

/// Return every entry name that participates in an `after` cycle.
fn detect_after_cycles(
    resolved: &BTreeMap<String, (ServiceProcConfigWire, Vec<String>)>,
) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum Color {
        White,
        Gray,
        Black,
    }

    let mut colors: BTreeMap<String, Color> = resolved
        .keys()
        .map(|name| (name.clone(), Color::White))
        .collect();
    let mut in_cycle: Vec<String> = Vec::new();
    let mut stack: Vec<String> = Vec::new();

    fn visit(
        node: &str,
        resolved: &BTreeMap<String, (ServiceProcConfigWire, Vec<String>)>,
        colors: &mut BTreeMap<String, Color>,
        stack: &mut Vec<String>,
        in_cycle: &mut Vec<String>,
    ) {
        colors.insert(node.to_string(), Color::Gray);
        stack.push(node.to_string());
        if let Some((_, edges)) = resolved.get(node) {
            for next in edges {
                match colors.get(next).copied() {
                    Some(Color::White) => {
                        visit(next, resolved, colors, stack, in_cycle);
                    }
                    Some(Color::Gray) => {
                        if let Some(pos) = stack.iter().position(|n| n == next)
                        {
                            for member in &stack[pos..] {
                                if !in_cycle.contains(member) {
                                    in_cycle.push(member.clone());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        stack.pop();
        colors.insert(node.to_string(), Color::Black);
    }

    let names: Vec<String> = resolved.keys().cloned().collect();
    for name in names {
        if colors.get(&name).copied() == Some(Color::White) {
            visit(&name, resolved, &mut colors, &mut stack, &mut in_cycle);
        }
    }
    in_cycle
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layer(
        name: &str,
        kind: &str,
        path: Option<&str>,
        value: Value,
    ) -> ConfigLayerInputWire {
        layer_with_error(name, kind, path, value, None)
    }

    fn layer_with_error(
        name: &str,
        kind: &str,
        path: Option<&str>,
        value: Value,
        error: Option<&str>,
    ) -> ConfigLayerInputWire {
        ConfigLayerInputWire {
            name: name.to_string(),
            kind: kind.to_string(),
            path: path.map(str::to_string),
            value,
            list_strategy: "concatenate".to_string(),
            writable: path.is_some(),
            exists: Some(true),
            error: error.map(str::to_string),
        }
    }

    fn compose(
        layers: Vec<ConfigLayerInputWire>,
    ) -> ServiceConfigCompositionWire {
        compose_service_config(&ServiceConfigComposeRequestWire { layers })
    }

    #[test]
    fn a_one_line_string_command_becomes_sh_c_argv() {
        let composition = compose(vec![layer(
            "user",
            "user",
            Some("/home/u/sase.yml"),
            serde_json::json!({
                "service": {"procs": {"tunnel": {"command": "autossh -M 0 -N box"}}}
            }),
        )]);
        assert!(composition.diagnostics.is_empty());
        let entry = &composition.procs[0];
        assert!(entry.available, "{:?}", entry.unavailable_reasons);
        match entry.launcher.as_ref().unwrap() {
            ServiceLauncherWire::Command { argv, command } => {
                assert_eq!(
                    argv,
                    &vec![
                        "/bin/sh".to_string(),
                        "-c".to_string(),
                        "autossh -M 0 -N box".to_string()
                    ]
                );
                assert_eq!(
                    command,
                    &Value::String("autossh -M 0 -N box".to_string())
                );
            }
            other => panic!("expected command launcher, got {other:?}"),
        }
    }

    #[test]
    fn an_overlay_replaces_a_plugin_list_whole() {
        let plugin = layer(
            "plugin:telegram",
            "plugin",
            None,
            serde_json::json!({
                "service": {"procs": {"telegram_receiver": {
                    "command": ["run-receiver"],
                    "success_exit_codes": [0, 1],
                }}}
            }),
        );
        let overlay = layer(
            "overlay:sase_athena.yml",
            "overlay",
            Some("/home/u/sase_athena.yml"),
            serde_json::json!({
                "service": {"procs": {"telegram_receiver": {
                    "command": ["run-receiver", "--flag"],
                    "success_exit_codes": [0],
                }}}
            }),
        );
        let composition = compose(vec![plugin, overlay]);
        let entry = &composition.procs[0];
        assert_eq!(entry.success_exit_codes, vec![0]);
        match entry.launcher.as_ref().unwrap() {
            ServiceLauncherWire::Command { argv, .. } => {
                assert_eq!(
                    argv,
                    &vec!["run-receiver".to_string(), "--flag".to_string()]
                );
            }
            other => panic!("expected command launcher, got {other:?}"),
        }
    }

    #[test]
    fn overlay_enabled_false_beats_default_and_true_reenables_plugin_entry() {
        let plugin = layer(
            "plugin:telegram",
            "plugin",
            None,
            serde_json::json!({
                "service": {"procs": {"telegram_receiver": {"command": "run"}}}
            }),
        );
        let disabled = compose(vec![plugin.clone()]);
        assert!(!disabled.procs[0].enabled);
        assert!(!disabled.procs[0].enablement.explicit);

        let overlay_enable = layer(
            "overlay:sase_athena.yml",
            "overlay",
            Some("/home/u/sase_athena.yml"),
            serde_json::json!({
                "service": {"procs": {"telegram_receiver": {"enabled": true}}}
            }),
        );
        let enabled = compose(vec![plugin.clone(), overlay_enable]);
        assert!(enabled.procs[0].enabled);
        assert!(enabled.procs[0].enablement.explicit);
        assert_eq!(
            enabled.procs[0].enablement.layer.as_deref(),
            Some("overlay:sase_athena.yml:/home/u/sase_athena.yml")
        );

        let overlay_disable = layer(
            "overlay:sase_apollo.yml",
            "overlay",
            Some("/home/u/sase_apollo.yml"),
            serde_json::json!({
                "service": {"procs": {"telegram_receiver": {"enabled": false}}}
            }),
        );
        let still_disabled = compose(vec![plugin, overlay_disable]);
        assert!(!still_disabled.procs[0].enabled);
        assert!(still_disabled.procs[0].enablement.explicit);
    }

    #[test]
    fn the_default_layer_gateway_entry_is_disabled_explicitly() {
        let default_layer = layer(
            "default",
            "builtin",
            None,
            serde_json::json!({
                "service": {"procs": {"gateway": {"builtin": "gateway", "enabled": false}}}
            }),
        );
        let composition = compose(vec![default_layer]);
        let entry = &composition.procs[0];
        assert_eq!(entry.source, "builtin");
        assert!(!entry.enabled);
        assert!(entry.enablement.explicit);
    }

    #[test]
    fn a_local_layer_is_ignored_with_a_warning() {
        let composition = compose(vec![layer(
            "local",
            "local",
            Some("/proj/sase.yml"),
            serde_json::json!({"service": {"procs": {"tunnel": {"command": "x"}}}}),
        )]);
        assert!(composition.procs.is_empty());
        assert_eq!(composition.ignored_layers, vec!["local:/proj/sase.yml"]);
        assert_eq!(
            composition.diagnostics[0].code,
            "service_config_ignored_in_project_layer"
        );
        assert_eq!(composition.diagnostics[0].severity, "warning");
        assert!(!composition.fatal);
    }

    #[test]
    fn rejections_mode_oneshot_reserved_name_builtin_mismatch_unknown_field() {
        let oneshot = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {"mode": "oneshot", "command": "x"}}}}),
        )]);
        assert!(!oneshot.procs[0].available);
        assert!(oneshot
            .diagnostics
            .iter()
            .any(|d| d.code == "service_oneshot_not_configurable"));

        let reserved_misuse = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"scheduler": {"command": "x"}}}}),
        )]);
        assert!(!reserved_misuse.procs[0].available);
        assert!(reserved_misuse
            .diagnostics
            .iter()
            .any(|d| d.code == "service_proc_reserved_name_misuse"));

        let mismatch = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"scheduler": {"builtin": "gateway"}}}}),
        )]);
        assert!(!mismatch.procs[0].available);
        assert!(mismatch
            .diagnostics
            .iter()
            .any(|d| d.code == "service_proc_builtin_name_mismatch"));

        let unknown_field = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {"command": "x", "bogus": 1}}}}),
        )]);
        assert!(!unknown_field.procs[0].available);
        assert!(unknown_field
            .diagnostics
            .iter()
            .any(|d| d.code == "unknown_service_proc_field"));
    }

    #[test]
    fn env_merges_key_by_key_and_bad_dollar_brace_syntax_is_rejected() {
        let base = layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {
                "command": "x",
                "env": {"A": "1", "B": "${A}"},
            }}}}),
        );
        let overlay = layer(
            "overlay:o.yml",
            "overlay",
            Some("/o.yml"),
            serde_json::json!({"service": {"procs": {"x": {"env": {"B": "2"}}}}}),
        );
        let composition = compose(vec![base, overlay]);
        let entry = &composition.procs[0];
        assert_eq!(entry.env.get("A").map(String::as_str), Some("1"));
        assert_eq!(entry.env.get("B").map(String::as_str), Some("2"));
        assert!(entry.available);

        let bad = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {
                "command": "x",
                "env": {"A": "$bad"},
            }}}}),
        )]);
        assert!(!bad.procs[0].available);
    }

    #[test]
    fn stop_signal_is_normalized() {
        let composition = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {
                "command": "x",
                "stop_signal": "INT",
            }}}}),
        )]);
        assert_eq!(composition.procs[0].stop_signal, "SIGINT");
    }

    #[test]
    fn an_unknown_after_target_warns_and_an_after_cycle_marks_entries_unavailable(
    ) {
        let unknown = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {
                "command": "x",
                "after": ["ghost"],
            }}}}),
        )]);
        assert!(unknown.procs[0].available);
        assert!(unknown.procs[0].after.is_empty());
        assert!(unknown
            .diagnostics
            .iter()
            .any(|d| d.code == "unknown_service_proc_after"));

        let cycle = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {
                "a": {"command": "a", "after": ["b"]},
                "b": {"command": "b", "after": ["a"]},
            }}}),
        )]);
        for entry in &cycle.procs {
            assert!(!entry.available, "{} should be unavailable", entry.name);
        }
        assert!(cycle
            .diagnostics
            .iter()
            .any(|d| d.code == "service_proc_after_cycle"));
    }

    #[test]
    fn field_provenance_shows_the_overriding_layer_and_its_file_path() {
        let base = layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": {"procs": {"x": {"command": "one"}}}}),
        );
        let overlay = layer(
            "overlay:o.yml",
            "overlay",
            Some("/o.yml"),
            serde_json::json!({"service": {"procs": {"x": {"command": "two"}}}}),
        );
        let composition = compose(vec![base, overlay]);
        let provenance = &composition.procs[0].field_provenance;
        let command_prov =
            provenance.iter().find(|p| p.field == "command").unwrap();
        assert_eq!(command_prov.layer, "overlay:o.yml:/o.yml");
        assert_eq!(command_prov.path.as_deref(), Some("/o.yml"));
    }

    #[test]
    fn an_overlay_only_partial_entry_is_unavailable_with_a_plugin_hint() {
        let composition = compose(vec![layer(
            "overlay:o.yml",
            "overlay",
            Some("/o.yml"),
            serde_json::json!({"service": {"procs": {"telegram_receiver": {"enabled": true}}}}),
        )]);
        let entry = &composition.procs[0];
        assert!(!entry.available);
        assert!(entry.unavailable_reasons.iter().any(|reason| reason
            .contains("is the plugin that declares it installed?")));
    }

    #[test]
    fn a_non_mapping_service_value_is_fatal() {
        let composition = compose(vec![layer(
            "user",
            "user",
            Some("/u/sase.yml"),
            serde_json::json!({"service": "nope"}),
        )]);
        assert!(composition.fatal);
        assert!(composition.procs.is_empty());
        assert_eq!(
            composition.diagnostics[0].code,
            "service_config_invalid_section"
        );
    }

    #[test]
    fn an_errored_overlay_is_fatal_and_names_the_file() {
        let composition = compose(vec![layer_with_error(
            "overlay:o.yml",
            "overlay",
            Some("/o.yml"),
            serde_json::json!({"service": {"procs": {
                "tunnel": {"command": "x"}
            }}}),
            Some("mapping values are not allowed here (line 2)"),
        )]);
        assert!(composition.fatal);
        assert!(composition.procs.is_empty());
        let diagnostic = composition
            .diagnostics
            .iter()
            .find(|d| d.code == "service_config_layer_error")
            .expect("errored overlay emits service_config_layer_error");
        assert_eq!(diagnostic.severity, "error");
        assert!(
            diagnostic.message.contains("/o.yml"),
            "names the file, got: {}",
            diagnostic.message
        );
        assert!(
            diagnostic
                .message
                .contains("mapping values are not allowed"),
            "names the parse error, got: {}",
            diagnostic.message
        );
    }

    #[test]
    fn an_errored_local_layer_warns_and_is_not_fatal() {
        let composition = compose(vec![layer_with_error(
            "local",
            "local",
            Some("/proj/sase.yml"),
            serde_json::json!({"service": {"procs": {
                "tunnel": {"command": "x"}
            }}}),
            Some("mapping values are not allowed here (line 2)"),
        )]);
        assert!(!composition.fatal);
        assert!(composition.procs.is_empty());
        let diagnostic = composition
            .diagnostics
            .iter()
            .find(|d| d.code == "service_config_layer_error")
            .expect("errored local emits service_config_layer_error");
        assert_eq!(diagnostic.severity, "warning");
        assert!(
            diagnostic.message.contains("/proj/sase.yml"),
            "names the path, got: {}",
            diagnostic.message
        );
    }

    #[test]
    fn an_unknown_layer_kind_is_fatal() {
        let composition = compose(vec![layer(
            "machine",
            "machine",
            Some("/m/sase.yml"),
            serde_json::json!({"service": {"procs": {
                "tunnel": {"command": "x"}
            }}}),
        )]);
        assert!(composition.fatal);
        assert!(composition.procs.is_empty());
        assert!(composition
            .diagnostics
            .iter()
            .any(|d| d.code == "service_config_unknown_layer_kind"));
    }
}

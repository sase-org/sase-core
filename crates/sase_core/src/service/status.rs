//! Versioned service-host and service-proc status snapshot wire.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;

use super::config::{
    ServiceConfigCompositionWire, ServiceEnablementSourceWire,
    ServiceLauncherWire, ServiceProcConfigWire,
};
use super::restart::ServiceRestartDecisionWire;
use super::state::{
    ServiceEnablementOverrideWire, ServiceHostRecordWire, ServiceStateWire,
    ServiceStopWire,
};

pub const SERVICE_STATUS_WIRE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_HOST_STALE_AFTER_SECONDS: f64 = 15.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceEnablementWire {
    pub enabled: bool,
    pub provenance: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<f64>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusHostRequestWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<ServiceHostRecordWire>,
    #[serde(default)]
    pub lock_held: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid_alive: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_unit: Option<String>,
    #[serde(default = "default_host_stale_after_seconds")]
    pub stale_after_seconds: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceProcLastExitWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spawn_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceProcReportedStatusWire {
    pub summary: String,
    pub state: String,
    pub updated_at: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceProcObservationWire {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default)]
    pub alive: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<ServiceProcLastExitWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<ServiceRestartDecisionWire>,
    #[serde(default)]
    pub restarts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<ServiceProcReportedStatusWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusRequestWire {
    pub generated_at: f64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    pub host: ServiceStatusHostRequestWire,
    pub config: ServiceConfigCompositionWire,
    pub state: ServiceStateWire,
    #[serde(default)]
    pub procs: Vec<ServiceProcObservationWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusHostWire {
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat_age_seconds: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sase_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusProcWire {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub source: String,
    pub declared_by: String,
    pub mode: String,
    pub available: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proc_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<ServiceProcLastExitWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<ServiceRestartDecisionWire>,
    pub restarts: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<ServiceProcReportedStatusWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub launcher_summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<String>,
    pub enablement: ServiceEnablementWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop: Option<ServiceStopWire>,
    pub desired: String,
    pub state: String,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStatusSnapshotWire {
    pub schema_version: u32,
    pub generated_at: f64,
    pub change_token: String,
    pub host: ServiceStatusHostWire,
    pub procs: Vec<ServiceStatusProcWire>,
    pub orphans: Vec<ServiceStatusProcWire>,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceStatusError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error(
        "service status has newer schema_version {found}; supported {supported}"
    )]
    NewerSchema { found: u32, supported: u32 },
    #[error("corrupt service status: {0}")]
    Corrupt(String),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn resolve_service_enablement(
    config_enabled: bool,
    source: &ServiceEnablementSourceWire,
    override_value: Option<&ServiceEnablementOverrideWire>,
) -> ServiceEnablementWire {
    resolve_service_enablement_inner(
        config_enabled,
        source,
        None,
        override_value,
    )
}

pub fn build_service_status(
    request: &ServiceStatusRequestWire,
) -> Result<ServiceStatusSnapshotWire, ServiceStatusError> {
    validate_request(request)?;
    let observations = observations_by_name(&request.procs);
    let configured_names: BTreeSet<String> = request
        .config
        .procs
        .iter()
        .map(|entry| entry.name.clone())
        .collect();
    let mut diagnostics = Vec::new();
    diagnostics.extend(request.state_diagnostics());
    diagnostics.extend(duplicate_observation_diagnostics(&request.procs));

    let host = derive_host(&request.host, request.generated_at);
    let procs = request
        .config
        .procs
        .iter()
        .map(|entry| {
            let observation = observations.get(&entry.name);
            derive_configured_proc(
                entry,
                request.state.enablement.get(&entry.name),
                request.state.stops.get(&entry.name),
                observation.copied(),
            )
        })
        .collect();
    let orphans = request
        .procs
        .iter()
        .filter(|observation| !configured_names.contains(&observation.name))
        .map(derive_orphan_proc)
        .collect();

    let mut snapshot = ServiceStatusSnapshotWire {
        schema_version: SERVICE_STATUS_WIRE_SCHEMA_VERSION,
        generated_at: request.generated_at,
        change_token: String::new(),
        host,
        procs,
        orphans,
        diagnostics,
    };
    snapshot.change_token = change_token(&snapshot)?;
    Ok(snapshot)
}

pub fn write_service_status_snapshot(
    path: impl AsRef<Path>,
    snapshot: &ServiceStatusSnapshotWire,
) -> Result<(), ServiceStatusError> {
    validate_snapshot(snapshot)?;
    let path = path.as_ref();
    let parent = path.parent().ok_or_else(|| {
        ServiceStatusError::Validation(
            "service status path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, snapshot)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

pub fn read_service_status_snapshot(
    path: impl AsRef<Path>,
) -> Result<Option<ServiceStatusSnapshotWire>, ServiceStatusError> {
    let bytes = match fs::read(path.as_ref()) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error.into()),
    };
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| ServiceStatusError::Corrupt(error.to_string()))?;
    let schema_version = schema_version(&value).unwrap_or(0);
    if schema_version > SERVICE_STATUS_WIRE_SCHEMA_VERSION {
        return Err(ServiceStatusError::NewerSchema {
            found: schema_version,
            supported: SERVICE_STATUS_WIRE_SCHEMA_VERSION,
        });
    }
    let snapshot: ServiceStatusSnapshotWire = serde_json::from_value(value)
        .map_err(|error| ServiceStatusError::Corrupt(error.to_string()))?;
    validate_snapshot(&snapshot)?;
    Ok(Some(snapshot))
}

trait ServiceStatusRequestExt {
    fn state_diagnostics(&self) -> Vec<String>;
}

impl ServiceStatusRequestExt for ServiceStatusRequestWire {
    fn state_diagnostics(&self) -> Vec<String> {
        Vec::new()
    }
}

fn default_host_stale_after_seconds() -> f64 {
    DEFAULT_HOST_STALE_AFTER_SECONDS
}

fn validate_request(
    request: &ServiceStatusRequestWire,
) -> Result<(), ServiceStatusError> {
    validate_finite("generated_at", request.generated_at)?;
    validate_positive_finite(
        "host.stale_after_seconds",
        request.host.stale_after_seconds,
    )?;
    if let Some(record) = &request.host.record {
        validate_finite("host.record.started_at", record.started_at)?;
        validate_finite("host.record.heartbeat_at", record.heartbeat_at)?;
    }
    for (index, observation) in request.procs.iter().enumerate() {
        validate_observation(index, observation)?;
    }
    Ok(())
}

fn validate_observation(
    index: usize,
    observation: &ServiceProcObservationWire,
) -> Result<(), ServiceStatusError> {
    if observation.name.trim().is_empty() {
        return Err(ServiceStatusError::Validation(format!(
            "procs[{index}].name must not be empty"
        )));
    }
    if let Some(started_at) = observation.started_at {
        validate_finite(&format!("procs[{index}].started_at"), started_at)?;
    }
    if let Some(last_exit) = &observation.last_exit {
        if let Some(finished_at) = last_exit.finished_at {
            validate_finite(
                &format!("procs[{index}].last_exit.finished_at"),
                finished_at,
            )?;
        }
    }
    if let Some(reported) = &observation.reported {
        validate_finite(
            &format!("procs[{index}].reported.updated_at"),
            reported.updated_at,
        )?;
    }
    Ok(())
}

fn validate_snapshot(
    snapshot: &ServiceStatusSnapshotWire,
) -> Result<(), ServiceStatusError> {
    if snapshot.schema_version != SERVICE_STATUS_WIRE_SCHEMA_VERSION {
        return Err(ServiceStatusError::Validation(format!(
            "service status schema_version must be {}",
            SERVICE_STATUS_WIRE_SCHEMA_VERSION
        )));
    }
    validate_finite("generated_at", snapshot.generated_at)?;
    Ok(())
}

fn validate_finite(field: &str, value: f64) -> Result<(), ServiceStatusError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ServiceStatusError::Validation(format!(
            "{field} must be finite"
        )))
    }
}

fn validate_positive_finite(
    field: &str,
    value: f64,
) -> Result<(), ServiceStatusError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ServiceStatusError::Validation(format!(
            "{field} must be finite and positive"
        )))
    }
}

fn observations_by_name(
    observations: &[ServiceProcObservationWire],
) -> BTreeMap<String, &ServiceProcObservationWire> {
    observations
        .iter()
        .map(|observation| (observation.name.clone(), observation))
        .collect()
}

fn duplicate_observation_diagnostics(
    observations: &[ServiceProcObservationWire],
) -> Vec<String> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for observation in observations {
        *counts.entry(&observation.name).or_default() += 1;
    }
    counts
        .into_iter()
        .filter(|&(_, count)| count > 1)
        .map(|(name, _)| {
            format!("multiple observations for service proc `{name}`; using the last")
        })
        .collect()
}

fn derive_host(
    host: &ServiceStatusHostRequestWire,
    generated_at: f64,
) -> ServiceStatusHostWire {
    let Some(record) = host.record.as_ref() else {
        let state = if host.lock_held {
            "starting"
        } else {
            "stopped"
        };
        return ServiceStatusHostWire {
            state: state.to_string(),
            pid: None,
            mode: None,
            platform_unit: host.platform_unit.clone(),
            started_at: None,
            heartbeat_at: None,
            heartbeat_age_seconds: None,
            sase_version: None,
            error: None,
            summary: state.to_string(),
        };
    };

    let heartbeat_age = (generated_at - record.heartbeat_at).max(0.0);
    let heartbeat_fresh = heartbeat_age <= host.stale_after_seconds;
    let pid_alive = host.pid_alive.unwrap_or(true);
    let state = if pid_alive && heartbeat_fresh {
        "running"
    } else {
        "stale"
    };
    ServiceStatusHostWire {
        state: state.to_string(),
        pid: Some(record.pid),
        mode: Some(record.mode.clone()),
        platform_unit: host
            .platform_unit
            .clone()
            .or_else(|| record.unit.clone()),
        started_at: Some(record.started_at),
        heartbeat_at: Some(record.heartbeat_at),
        heartbeat_age_seconds: Some(heartbeat_age),
        sase_version: record.sase_version.clone(),
        error: record.error.clone(),
        summary: format!("{state} · pid {}", record.pid),
    }
}

fn derive_configured_proc(
    entry: &ServiceProcConfigWire,
    override_value: Option<&ServiceEnablementOverrideWire>,
    stop: Option<&ServiceStopWire>,
    observation: Option<&ServiceProcObservationWire>,
) -> ServiceStatusProcWire {
    let enablement =
        resolve_service_enablement_for_entry(entry, override_value);
    let desired = if entry.available && enablement.enabled && stop.is_none() {
        "running"
    } else {
        "stopped"
    };
    let state = derive_proc_state(entry, &enablement, stop, observation);
    let summary = proc_summary(&state, entry, &enablement, stop, observation);
    ServiceStatusProcWire {
        name: entry.name.clone(),
        description: entry.description.clone(),
        source: entry.source.clone(),
        declared_by: entry.declared_by.clone(),
        mode: entry.mode.clone(),
        available: entry.available,
        pid: observation.and_then(|item| item.pid),
        proc_id: observation.and_then(|item| item.proc_id.clone()),
        started_at: observation.and_then(|item| item.started_at),
        last_exit: observation.and_then(|item| item.last_exit.clone()),
        restart: observation.and_then(|item| item.restart.clone()),
        restarts: observation.map_or(0, |item| item.restarts),
        reported: observation.and_then(|item| item.reported.clone()),
        log_path: observation.and_then(|item| item.log_path.clone()),
        launcher_summary: entry.launcher.as_ref().map(launcher_summary),
        unavailable_reason: entry.unavailable_reasons.first().cloned(),
        enablement,
        stop: stop.cloned(),
        desired: desired.to_string(),
        state,
        summary,
    }
}

fn derive_orphan_proc(
    observation: &ServiceProcObservationWire,
) -> ServiceStatusProcWire {
    let enablement = ServiceEnablementWire {
        enabled: true,
        provenance: "default".to_string(),
        layer: None,
        path: None,
        updated_at: None,
        summary: "enabled by default".to_string(),
    };
    let state = if observation.alive {
        "running"
    } else {
        "stopped"
    };
    ServiceStatusProcWire {
        name: observation.name.clone(),
        description: None,
        source: "observed".to_string(),
        declared_by: "runtime".to_string(),
        mode: "daemon".to_string(),
        available: true,
        pid: observation.pid,
        proc_id: observation.proc_id.clone(),
        started_at: observation.started_at,
        last_exit: observation.last_exit.clone(),
        restart: observation.restart.clone(),
        restarts: observation.restarts,
        reported: observation.reported.clone(),
        log_path: observation.log_path.clone(),
        launcher_summary: None,
        unavailable_reason: None,
        enablement,
        stop: None,
        desired: "stopped".to_string(),
        state: state.to_string(),
        summary: observation_summary(state, observation),
    }
}

fn derive_proc_state(
    entry: &ServiceProcConfigWire,
    enablement: &ServiceEnablementWire,
    stop: Option<&ServiceStopWire>,
    observation: Option<&ServiceProcObservationWire>,
) -> String {
    if observation.is_some_and(|item| item.alive) {
        return "running".to_string();
    }
    if !entry.available {
        return "unavailable".to_string();
    }
    if !enablement.enabled {
        return "disabled".to_string();
    }
    if stop.is_some() {
        return "stopped".to_string();
    }
    if let Some(restart) = observation.and_then(|item| item.restart.as_ref()) {
        if restart.action == "restart" {
            if restart.crash_loop {
                return "crash_loop".to_string();
            }
            return "backoff".to_string();
        }
    }
    if observation.is_some_and(|item| item.last_exit.is_some()) {
        return "exited".to_string();
    }
    "stopped".to_string()
}

fn proc_summary(
    state: &str,
    entry: &ServiceProcConfigWire,
    enablement: &ServiceEnablementWire,
    stop: Option<&ServiceStopWire>,
    observation: Option<&ServiceProcObservationWire>,
) -> String {
    match state {
        "running" => observation.map_or_else(
            || "running".to_string(),
            |item| observation_summary("running", item),
        ),
        "backoff" | "crash_loop" => observation
            .and_then(|item| item.restart.as_ref())
            .map(|restart| restart.reason.clone())
            .unwrap_or_else(|| state.to_string()),
        "disabled" => enablement.summary.clone(),
        "unavailable" => entry
            .unavailable_reasons
            .first()
            .map(|reason| format!("unavailable: {reason}"))
            .unwrap_or_else(|| "unavailable".to_string()),
        "stopped" if stop.is_some() => "stopped until next boot".to_string(),
        "exited" => observation
            .and_then(|item| item.last_exit.as_ref())
            .map(last_exit_summary)
            .unwrap_or_else(|| "exited".to_string()),
        _ => state.to_string(),
    }
}

fn observation_summary(
    state: &str,
    observation: &ServiceProcObservationWire,
) -> String {
    if state == "running" {
        observation
            .pid
            .map(|pid| format!("running · pid {pid}"))
            .unwrap_or_else(|| "running".to_string())
    } else {
        state.to_string()
    }
}

fn last_exit_summary(last_exit: &ServiceProcLastExitWire) -> String {
    if let Some(error) = &last_exit.spawn_error {
        return format!("failed to start: {error}");
    }
    if let Some(code) = last_exit.exit_code {
        return format!("exited with code {code}");
    }
    if let Some(signal) = last_exit.signal {
        return format!("killed by signal {signal}");
    }
    "exited".to_string()
}

pub fn resolve_service_enablement_for_entry(
    entry: &ServiceProcConfigWire,
    override_value: Option<&ServiceEnablementOverrideWire>,
) -> ServiceEnablementWire {
    resolve_service_enablement_inner(
        entry.enabled,
        &entry.enablement,
        Some(entry.source.as_str()),
        override_value,
    )
}

fn resolve_service_enablement_inner(
    config_enabled: bool,
    source: &ServiceEnablementSourceWire,
    entry_source: Option<&str>,
    override_value: Option<&ServiceEnablementOverrideWire>,
) -> ServiceEnablementWire {
    if let Some(override_value) = override_value {
        let summary = if override_value.enabled {
            "enabled here"
        } else {
            "disabled here"
        };
        return ServiceEnablementWire {
            enabled: override_value.enabled,
            provenance: "override".to_string(),
            layer: None,
            path: None,
            updated_at: Some(override_value.updated_at),
            summary: summary.to_string(),
        };
    }

    if source.explicit {
        return ServiceEnablementWire {
            enabled: config_enabled,
            provenance: "config".to_string(),
            layer: source.layer.clone(),
            path: source.path.clone(),
            updated_at: None,
            summary: config_enablement_summary(config_enabled, source),
        };
    }

    let summary = match (config_enabled, entry_source) {
        (false, Some("plugin")) => "disabled by default (plugin-declared)",
        (true, _) => "enabled by default",
        (false, _) => "disabled by default",
    };
    ServiceEnablementWire {
        enabled: config_enabled,
        provenance: "default".to_string(),
        layer: None,
        path: None,
        updated_at: None,
        summary: summary.to_string(),
    }
}

fn config_enablement_summary(
    enabled: bool,
    source: &ServiceEnablementSourceWire,
) -> String {
    let verb = if enabled { "enabled" } else { "disabled" };
    if let Some(path) = &source.path {
        return format!("{verb} by {}", file_name(path));
    }
    if source.layer_kind.as_deref() == Some("plugin") {
        return format!(
            "{verb} by plugin {}",
            plugin_label(source.layer.as_deref())
        );
    }
    if source.layer_kind.as_deref() == Some("builtin")
        || source.layer.as_deref().is_none()
    {
        return format!("{verb} by default config");
    }
    format!("{verb} by config")
}

fn file_name(path: &str) -> String {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
        .to_string()
}

fn plugin_label(layer: Option<&str>) -> String {
    let raw = layer.unwrap_or("unknown");
    let trimmed = raw.strip_prefix("plugin:").unwrap_or(raw);
    trimmed.split(':').next().unwrap_or(trimmed).to_string()
}

fn launcher_summary(launcher: &ServiceLauncherWire) -> String {
    match launcher {
        ServiceLauncherWire::Builtin { builtin } => {
            format!("builtin: {builtin}")
        }
        ServiceLauncherWire::Command { argv, .. } => shell_join(argv),
    }
}

fn shell_join(argv: &[String]) -> String {
    argv.iter()
        .map(|arg| {
            if arg.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || b"@%_+=:,./-".contains(&byte)
            }) {
                arg.clone()
            } else {
                format!("'{}'", arg.replace('\'', "'\\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn change_token(
    snapshot: &ServiceStatusSnapshotWire,
) -> Result<String, ServiceStatusError> {
    let mut value = serde_json::to_value(snapshot)?;
    if let Some(object) = value.as_object_mut() {
        object.insert("generated_at".to_string(), Value::Null);
        object.insert("change_token".to_string(), Value::String(String::new()));
        if let Some(host) =
            object.get_mut("host").and_then(Value::as_object_mut)
        {
            host.insert("heartbeat_at".to_string(), Value::Null);
            host.insert("heartbeat_age_seconds".to_string(), Value::Null);
        }
    }
    let bytes = serde_json::to_vec(&value)?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn schema_version(value: &Value) -> Option<u32> {
    value
        .get("schema_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::service::config::{
        ServiceConfigCompositionWire, ServiceEnablementSourceWire,
        ServiceLauncherWire, ServiceProcConfigWire,
        SERVICE_CONFIG_WIRE_SCHEMA_VERSION,
    };
    use crate::service::restart::{
        ServiceRestartDecisionWire, ServiceRestartHistoryWire,
    };

    fn entry(name: &str) -> ServiceProcConfigWire {
        ServiceProcConfigWire {
            name: name.to_string(),
            description: Some(format!("{name} description")),
            available: true,
            unavailable_reasons: Vec::new(),
            source: "builtin".to_string(),
            declared_by: "default".to_string(),
            enabled: true,
            enablement: ServiceEnablementSourceWire::default(),
            mode: "daemon".to_string(),
            launcher: Some(ServiceLauncherWire::Builtin {
                builtin: name.to_string(),
            }),
            cwd: None,
            env: BTreeMap::new(),
            restart: "on-failure".to_string(),
            success_exit_codes: Vec::new(),
            stop_signal: "SIGTERM".to_string(),
            stop_timeout_seconds: 10.0,
            after: Vec::new(),
            log_max_bytes: 4096,
            field_provenance: Vec::new(),
        }
    }

    fn request_with_entry(
        entry: ServiceProcConfigWire,
        state: ServiceStateWire,
        observation: Option<ServiceProcObservationWire>,
    ) -> ServiceStatusRequestWire {
        ServiceStatusRequestWire {
            generated_at: 20.0,
            boot_id: Some("boot-a".to_string()),
            host: ServiceStatusHostRequestWire {
                record: Some(ServiceHostRecordWire {
                    pid: 42,
                    boot_id: Some("boot-a".to_string()),
                    started_at: 1.0,
                    heartbeat_at: 19.0,
                    mode: "foreground".to_string(),
                    unit: None,
                    sase_version: Some("0.test".to_string()),
                    error: None,
                }),
                lock_held: false,
                pid_alive: Some(true),
                platform_unit: Some("sase.service".to_string()),
                stale_after_seconds: 15.0,
            },
            config: ServiceConfigCompositionWire {
                schema_version: SERVICE_CONFIG_WIRE_SCHEMA_VERSION,
                fatal: false,
                procs: vec![entry],
                diagnostics: Vec::new(),
                ignored_layers: Vec::new(),
            },
            state,
            procs: observation.into_iter().collect(),
        }
    }

    fn restart(crash_loop: bool) -> ServiceRestartDecisionWire {
        ServiceRestartDecisionWire {
            schema_version: 1,
            action: "restart".to_string(),
            clean_exit: false,
            delay_seconds: 4.0,
            restart_at: Some(14.0),
            reason: "exited with code 1; retrying in 4s".to_string(),
            crash_loop,
            notify: crash_loop,
            history: ServiceRestartHistoryWire::default(),
        }
    }

    #[test]
    fn enablement_resolution_summaries_match_sources() {
        let explicit_file = ServiceEnablementSourceWire {
            explicit: true,
            layer: Some("machine:/home/u/sase_apollo.yml".to_string()),
            layer_kind: Some("machine".to_string()),
            path: Some("/home/u/sase_apollo.yml".to_string()),
        };
        assert_eq!(
            resolve_service_enablement(false, &explicit_file, None).summary,
            "disabled by sase_apollo.yml"
        );

        let plugin = ServiceEnablementSourceWire {
            explicit: true,
            layer: Some("plugin:telegram".to_string()),
            layer_kind: Some("plugin".to_string()),
            path: None,
        };
        assert_eq!(
            resolve_service_enablement(false, &plugin, None).summary,
            "disabled by plugin telegram"
        );

        let default = ServiceEnablementSourceWire {
            explicit: true,
            layer: Some("default".to_string()),
            layer_kind: Some("builtin".to_string()),
            path: None,
        };
        assert_eq!(
            resolve_service_enablement(false, &default, None).summary,
            "disabled by default config"
        );

        let override_value = ServiceEnablementOverrideWire {
            enabled: true,
            updated_at: 12.0,
            updated_by: "pytest".to_string(),
        };
        let resolved =
            resolve_service_enablement(false, &default, Some(&override_value));
        assert_eq!(resolved.provenance, "override");
        assert_eq!(resolved.summary, "enabled here");
    }

    #[test]
    fn proc_state_derivation_uses_first_matching_rule() {
        let base_state = ServiceStateWire::default();

        let running = build_service_status(&request_with_entry(
            entry("scheduler"),
            base_state.clone(),
            Some(ServiceProcObservationWire {
                name: "scheduler".to_string(),
                pid: Some(123),
                alive: true,
                proc_id: Some("proc-1".to_string()),
                started_at: Some(10.0),
                last_exit: None,
                restart: None,
                restarts: 0,
                reported: None,
                log_path: None,
            }),
        ))
        .unwrap();
        assert_eq!(running.procs[0].state, "running");
        assert_eq!(running.procs[0].summary, "running · pid 123");

        let mut unavailable_entry = entry("scheduler");
        unavailable_entry.available = false;
        unavailable_entry
            .unavailable_reasons
            .push("no launcher".to_string());
        let unavailable = build_service_status(&request_with_entry(
            unavailable_entry,
            base_state.clone(),
            None,
        ))
        .unwrap();
        assert_eq!(unavailable.procs[0].state, "unavailable");
        assert_eq!(unavailable.procs[0].summary, "unavailable: no launcher");

        let mut disabled_state = ServiceStateWire::default();
        disabled_state.enablement.insert(
            "scheduler".to_string(),
            ServiceEnablementOverrideWire {
                enabled: false,
                updated_at: 1.0,
                updated_by: "pytest".to_string(),
            },
        );
        let disabled = build_service_status(&request_with_entry(
            entry("scheduler"),
            disabled_state,
            None,
        ))
        .unwrap();
        assert_eq!(disabled.procs[0].state, "disabled");
        assert_eq!(disabled.procs[0].summary, "disabled here");

        let mut stopped_state = ServiceStateWire::default();
        stopped_state.stops.insert(
            "scheduler".to_string(),
            ServiceStopWire {
                boot_id: Some("boot-a".to_string()),
                stopped_at: 1.0,
                stopped_by: "pytest".to_string(),
                reason: None,
            },
        );
        let stopped = build_service_status(&request_with_entry(
            entry("scheduler"),
            stopped_state,
            None,
        ))
        .unwrap();
        assert_eq!(stopped.procs[0].state, "stopped");
        assert_eq!(stopped.procs[0].summary, "stopped until next boot");

        let backoff = build_service_status(&request_with_entry(
            entry("scheduler"),
            base_state.clone(),
            Some(ServiceProcObservationWire {
                name: "scheduler".to_string(),
                pid: None,
                alive: false,
                proc_id: None,
                started_at: None,
                last_exit: None,
                restart: Some(restart(false)),
                restarts: 1,
                reported: None,
                log_path: None,
            }),
        ))
        .unwrap();
        assert_eq!(backoff.procs[0].state, "backoff");
        assert_eq!(
            backoff.procs[0].summary,
            "exited with code 1; retrying in 4s"
        );

        let crash_loop = build_service_status(&request_with_entry(
            entry("scheduler"),
            base_state.clone(),
            Some(ServiceProcObservationWire {
                name: "scheduler".to_string(),
                pid: None,
                alive: false,
                proc_id: None,
                started_at: None,
                last_exit: None,
                restart: Some(restart(true)),
                restarts: 3,
                reported: None,
                log_path: None,
            }),
        ))
        .unwrap();
        assert_eq!(crash_loop.procs[0].state, "crash_loop");

        let exited = build_service_status(&request_with_entry(
            entry("scheduler"),
            base_state,
            Some(ServiceProcObservationWire {
                name: "scheduler".to_string(),
                pid: None,
                alive: false,
                proc_id: None,
                started_at: None,
                last_exit: Some(ServiceProcLastExitWire {
                    exit_code: Some(1),
                    signal: None,
                    spawn_error: None,
                    finished_at: Some(12.0),
                }),
                restart: None,
                restarts: 0,
                reported: None,
                log_path: None,
            }),
        ))
        .unwrap();
        assert_eq!(exited.procs[0].state, "exited");
        assert_eq!(exited.procs[0].summary, "exited with code 1");
    }

    #[test]
    fn change_token_ignores_generation_and_heartbeat_churn() {
        let mut request = request_with_entry(
            entry("scheduler"),
            ServiceStateWire::default(),
            None,
        );
        let first = build_service_status(&request).unwrap();

        request.generated_at = 21.0;
        request.host.record.as_mut().unwrap().heartbeat_at = 20.0;
        let heartbeat_only = build_service_status(&request).unwrap();
        assert_eq!(first.change_token, heartbeat_only.change_token);

        request.procs.push(ServiceProcObservationWire {
            name: "scheduler".to_string(),
            pid: Some(123),
            alive: true,
            proc_id: None,
            started_at: Some(20.0),
            last_exit: None,
            restart: None,
            restarts: 0,
            reported: None,
            log_path: None,
        });
        let changed = build_service_status(&request).unwrap();
        assert_ne!(first.change_token, changed.change_token);
    }

    #[test]
    fn status_snapshot_writes_reads_and_rejects_bad_files() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("service").join("status.json");
        let snapshot = build_service_status(&request_with_entry(
            entry("scheduler"),
            ServiceStateWire::default(),
            None,
        ))
        .unwrap();

        write_service_status_snapshot(&path, &snapshot).unwrap();
        assert_eq!(
            read_service_status_snapshot(&path).unwrap(),
            Some(snapshot)
        );
        assert_eq!(
            read_service_status_snapshot(temp.path().join("missing.json"))
                .unwrap(),
            None
        );

        fs::write(&path, "{not-json").unwrap();
        assert!(matches!(
            read_service_status_snapshot(&path),
            Err(ServiceStatusError::Corrupt(_))
        ));

        fs::write(
            &path,
            serde_json::to_vec(&json!({"schema_version": 2})).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            read_service_status_snapshot(&path),
            Err(ServiceStatusError::NewerSchema { .. })
        ));
    }

    #[test]
    fn observations_without_config_entries_become_orphans() {
        let mut request = request_with_entry(
            entry("scheduler"),
            ServiceStateWire::default(),
            None,
        );
        request.procs.push(ServiceProcObservationWire {
            name: "legacy".to_string(),
            pid: Some(999),
            alive: true,
            proc_id: Some("proc-legacy".to_string()),
            started_at: Some(2.0),
            last_exit: None,
            restart: None,
            restarts: 0,
            reported: None,
            log_path: Some("/tmp/legacy.log".to_string()),
        });

        let snapshot = build_service_status(&request).unwrap();
        assert_eq!(snapshot.procs.len(), 1);
        assert_eq!(snapshot.orphans.len(), 1);
        assert_eq!(snapshot.orphans[0].name, "legacy");
        assert_eq!(snapshot.orphans[0].state, "running");
        assert_eq!(snapshot.orphans[0].desired, "stopped");
    }
}

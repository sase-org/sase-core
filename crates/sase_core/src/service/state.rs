//! Locked machine-local service state store.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::validate_service_proc_name;
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
    StoreLockError,
};

pub const SERVICE_STATE_WIRE_SCHEMA_VERSION: u32 = 1;
pub const SERVICE_STATE_LOCK_TIMEOUT_ENV: &str =
    "SASE_SERVICE_STATE_LOCK_TIMEOUT";
const SERVICE_STATE_LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceEnablementOverrideWire {
    pub enabled: bool,
    pub updated_at: f64,
    pub updated_by: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStopWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    pub stopped_at: f64,
    pub stopped_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceMarkerWire {
    pub recorded_at: f64,
    pub recorded_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceHostRecordWire {
    pub pid: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot_id: Option<String>,
    pub started_at: f64,
    pub heartbeat_at: f64,
    pub mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sase_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStateWire {
    pub schema_version: u32,
    #[serde(default)]
    pub enablement: BTreeMap<String, ServiceEnablementOverrideWire>,
    #[serde(default)]
    pub stops: BTreeMap<String, ServiceStopWire>,
    #[serde(default)]
    pub markers: BTreeMap<String, ServiceMarkerWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<ServiceHostRecordWire>,
}

impl Default for ServiceStateWire {
    fn default() -> Self {
        Self {
            schema_version: SERVICE_STATE_WIRE_SCHEMA_VERSION,
            enablement: BTreeMap::new(),
            stops: BTreeMap::new(),
            markers: BTreeMap::new(),
            host: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStateSnapshotWire {
    pub schema_version: u32,
    pub state: ServiceStateWire,
    pub expired_stops: Vec<String>,
    pub read_only: bool,
    pub diagnostics: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum ServiceStateMutationWire {
    SetEnablement {
        name: String,
        enabled: bool,
        actor: String,
    },
    ClearEnablement {
        name: String,
    },
    Stop {
        name: String,
        actor: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    ClearStop {
        name: String,
    },
    SetMarker {
        key: String,
        actor: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    ClearMarker {
        key: String,
    },
    RecordHost {
        host: ServiceHostRecordWire,
    },
    ClearHost {
        pid: u32,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceStateMutationOutcomeWire {
    pub snapshot: ServiceStateSnapshotWire,
    pub changed: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceStateError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("{0}")]
    LockTimeout(String),
    #[error(
        "service state has newer schema_version {found}; supported {supported}"
    )]
    NewerSchema { found: u32, supported: u32 },
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

pub fn service_state_path(sase_home: impl AsRef<Path>) -> PathBuf {
    sase_home.as_ref().join("service").join("state.json")
}

pub fn read_service_state(
    sase_home: impl AsRef<Path>,
    boot_id: Option<&str>,
) -> Result<ServiceStateSnapshotWire, ServiceStateError> {
    let path = service_state_path(sase_home);
    let _lock = lock_state(&path, LockMode::Shared, "read_service_state")?;
    match read_state_value(&path) {
        Err(ServiceStateError::Json(error)) => Ok(ServiceStateSnapshotWire {
            schema_version: SERVICE_STATE_WIRE_SCHEMA_VERSION,
            state: ServiceStateWire::default(),
            expired_stops: Vec::new(),
            read_only: false,
            diagnostics: vec![format!("corrupt service state: {error}")],
        }),
        Err(error) => Err(error),
        Ok(None) => Ok(snapshot(
            ServiceStateWire::default(),
            boot_id,
            false,
            Vec::new(),
        )),
        Ok(Some(value)) => read_snapshot_from_value(value, boot_id),
    }
}

pub fn mutate_service_state(
    sase_home: impl AsRef<Path>,
    mutation: ServiceStateMutationWire,
    boot_id: Option<&str>,
    now: Option<f64>,
) -> Result<ServiceStateMutationOutcomeWire, ServiceStateError> {
    let now = now.unwrap_or_else(epoch_seconds);
    validate_now(now)?;
    validate_mutation(&mutation)?;

    let path = service_state_path(sase_home);
    let _lock = lock_state(&path, LockMode::Exclusive, "mutate_service_state")?;
    let mut diagnostics = Vec::new();
    let mut changed = false;

    let mut state = match read_state_value(&path) {
        Ok(None) => ServiceStateWire::default(),
        Ok(Some(value)) => state_for_mutation(value)?,
        Err(ServiceStateError::Json(error)) => {
            quarantine_corrupt_state(&path, now)?;
            diagnostics
                .push(format!("corrupt service state quarantined: {error}"));
            changed = true;
            ServiceStateWire::default()
        }
        Err(error) => return Err(error),
    };

    let expired_stops = prune_expired_stops(&mut state, boot_id);
    if !expired_stops.is_empty() {
        changed = true;
    }
    if apply_mutation(&mut state, mutation, boot_id, now) {
        changed = true;
    }
    if changed {
        write_state_atomic(&path, &state)?;
    }
    Ok(ServiceStateMutationOutcomeWire {
        snapshot: ServiceStateSnapshotWire {
            schema_version: SERVICE_STATE_WIRE_SCHEMA_VERSION,
            state,
            expired_stops,
            read_only: false,
            diagnostics,
        },
        changed,
    })
}

fn read_snapshot_from_value(
    value: serde_json::Value,
    boot_id: Option<&str>,
) -> Result<ServiceStateSnapshotWire, ServiceStateError> {
    let schema_version = schema_version(&value).unwrap_or(0);
    let read_only = schema_version > SERVICE_STATE_WIRE_SCHEMA_VERSION;
    let mut diagnostics = Vec::new();
    if read_only {
        diagnostics.push(format!(
            "service state schema_version {schema_version} is newer than supported {}",
            SERVICE_STATE_WIRE_SCHEMA_VERSION
        ));
    }
    let state = serde_json::from_value::<ServiceStateWire>(value)?;
    Ok(snapshot(state, boot_id, read_only, diagnostics))
}

fn state_for_mutation(
    value: serde_json::Value,
) -> Result<ServiceStateWire, ServiceStateError> {
    let schema_version = schema_version(&value).unwrap_or(0);
    if schema_version > SERVICE_STATE_WIRE_SCHEMA_VERSION {
        return Err(ServiceStateError::NewerSchema {
            found: schema_version,
            supported: SERVICE_STATE_WIRE_SCHEMA_VERSION,
        });
    }
    Ok(serde_json::from_value(value)?)
}

fn snapshot(
    mut state: ServiceStateWire,
    boot_id: Option<&str>,
    read_only: bool,
    diagnostics: Vec<String>,
) -> ServiceStateSnapshotWire {
    let expired_stops = prune_expired_stops(&mut state, boot_id);
    ServiceStateSnapshotWire {
        schema_version: SERVICE_STATE_WIRE_SCHEMA_VERSION,
        state,
        expired_stops,
        read_only,
        diagnostics,
    }
}

fn read_state_value(
    path: &Path,
) -> Result<Option<serde_json::Value>, ServiceStateError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn schema_version(value: &serde_json::Value) -> Option<u32> {
    value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn apply_mutation(
    state: &mut ServiceStateWire,
    mutation: ServiceStateMutationWire,
    boot_id: Option<&str>,
    now: f64,
) -> bool {
    let before = state.clone();
    match mutation {
        ServiceStateMutationWire::SetEnablement {
            name,
            enabled,
            actor,
        } => {
            state.enablement.insert(
                name,
                ServiceEnablementOverrideWire {
                    enabled,
                    updated_at: now,
                    updated_by: actor,
                },
            );
        }
        ServiceStateMutationWire::ClearEnablement { name } => {
            state.enablement.remove(&name);
        }
        ServiceStateMutationWire::Stop {
            name,
            actor,
            reason,
        } => {
            state.stops.insert(
                name,
                ServiceStopWire {
                    boot_id: boot_id.map(str::to_string),
                    stopped_at: now,
                    stopped_by: actor,
                    reason,
                },
            );
        }
        ServiceStateMutationWire::ClearStop { name } => {
            state.stops.remove(&name);
        }
        ServiceStateMutationWire::SetMarker { key, actor, detail } => {
            state.markers.insert(
                key,
                ServiceMarkerWire {
                    recorded_at: now,
                    recorded_by: actor,
                    detail,
                },
            );
        }
        ServiceStateMutationWire::ClearMarker { key } => {
            state.markers.remove(&key);
        }
        ServiceStateMutationWire::RecordHost { host } => {
            state.host = Some(host);
        }
        ServiceStateMutationWire::ClearHost { pid } => {
            if state.host.as_ref().is_some_and(|host| host.pid == pid) {
                state.host = None;
            }
        }
    }
    *state != before
}

fn prune_expired_stops(
    state: &mut ServiceStateWire,
    boot_id: Option<&str>,
) -> Vec<String> {
    let expired: Vec<String> = state
        .stops
        .iter()
        .filter_map(|(name, stop)| {
            if stop.boot_id.as_deref() == boot_id {
                None
            } else {
                Some(name.clone())
            }
        })
        .collect();
    for name in &expired {
        state.stops.remove(name);
    }
    expired
}

fn validate_mutation(
    mutation: &ServiceStateMutationWire,
) -> Result<(), ServiceStateError> {
    match mutation {
        ServiceStateMutationWire::SetEnablement { name, actor, .. }
        | ServiceStateMutationWire::Stop { name, actor, .. } => {
            validate_name(name)?;
            validate_actor(actor)?;
        }
        ServiceStateMutationWire::ClearEnablement { name }
        | ServiceStateMutationWire::ClearStop { name } => {
            validate_name(name)?;
        }
        ServiceStateMutationWire::SetMarker { key, actor, .. } => {
            validate_marker_key(key)?;
            validate_actor(actor)?;
        }
        ServiceStateMutationWire::ClearMarker { key } => {
            validate_marker_key(key)?;
        }
        ServiceStateMutationWire::RecordHost { host } => {
            validate_host(host)?;
        }
        ServiceStateMutationWire::ClearHost { .. } => {}
    }
    Ok(())
}

fn validate_name(name: &str) -> Result<(), ServiceStateError> {
    validate_service_proc_name(name).map_err(ServiceStateError::Validation)
}

fn validate_actor(actor: &str) -> Result<(), ServiceStateError> {
    if actor.trim().is_empty() {
        Err(ServiceStateError::Validation(
            "actor must not be empty".to_string(),
        ))
    } else {
        Ok(())
    }
}

fn validate_now(now: f64) -> Result<(), ServiceStateError> {
    if now.is_finite() {
        Ok(())
    } else {
        Err(ServiceStateError::Validation(
            "now must be finite".to_string(),
        ))
    }
}

fn validate_marker_key(key: &str) -> Result<(), ServiceStateError> {
    let mut bytes = key.bytes();
    let Some(first) = bytes.next() else {
        return Err(ServiceStateError::Validation(
            "marker key must not be empty".to_string(),
        ));
    };
    if key.len() > 64 {
        return Err(ServiceStateError::Validation(
            "marker key must be no more than 64 characters".to_string(),
        ));
    }
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(ServiceStateError::Validation(
            "marker key must start with a lowercase letter or digit"
                .to_string(),
        ));
    }
    if !bytes.all(|byte| {
        byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'_' | b'.' | b'-')
    }) {
        return Err(ServiceStateError::Validation(
            "marker key must contain only lowercase letters, digits, underscores, dots, and hyphens"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_host(
    host: &ServiceHostRecordWire,
) -> Result<(), ServiceStateError> {
    validate_now(host.started_at)?;
    validate_now(host.heartbeat_at)?;
    if !matches!(
        host.mode.as_str(),
        "platform_unit" | "detached" | "foreground"
    ) {
        return Err(ServiceStateError::Validation(
            "host mode must be platform_unit, detached, or foreground"
                .to_string(),
        ));
    }
    Ok(())
}

fn lock_state(
    path: &Path,
    mode: LockMode,
    operation: &str,
) -> Result<crate::store_lock::HeldStoreLock, ServiceStateError> {
    let parent = path.parent().ok_or_else(|| {
        ServiceStateError::Validation(
            "service state path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let lock_path = path.with_file_name("state.json.lock");
    acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        mode,
        timeout_from_env(
            SERVICE_STATE_LOCK_TIMEOUT_ENV,
            SERVICE_STATE_LOCK_TIMEOUT_DEFAULT,
        ),
        operation,
    )
    .map_err(lock_error)
}

fn lock_error(error: StoreLockError) -> ServiceStateError {
    match error {
        StoreLockError::Timeout {
            mode,
            lock_path,
            waited_ms,
            holder,
        } => ServiceStateError::LockTimeout(format!(
            "timed out after {waited_ms}ms waiting for {mode} lock {}; holder: {}",
            lock_path.display(),
            holder
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown".to_string())
        )),
        StoreLockError::Open { source, .. }
        | StoreLockError::Acquire { source, .. } => ServiceStateError::Io(source),
    }
}

fn write_state_atomic(
    path: &Path,
    state: &ServiceStateWire,
) -> Result<(), ServiceStateError> {
    let parent = path.parent().ok_or_else(|| {
        ServiceStateError::Validation(
            "service state path has no parent directory".to_string(),
        )
    })?;
    fs::create_dir_all(parent)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut temporary, state)?;
    temporary.write_all(b"\n")?;
    temporary.flush()?;
    temporary.as_file().sync_all()?;
    temporary.persist(path).map_err(|error| error.error)?;
    if let Ok(directory) = File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

fn quarantine_corrupt_state(
    path: &Path,
    now: f64,
) -> Result<(), ServiceStateError> {
    if !path.exists() {
        return Ok(());
    }
    let suffix = (now * 1000.0).trunc().max(0.0) as u64;
    let quarantine =
        path.with_file_name(format!("state.json.corrupt-{suffix}"));
    fs::rename(path, quarantine)?;
    Ok(())
}

fn epoch_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::*;

    fn set_enablement(name: &str, enabled: bool) -> ServiceStateMutationWire {
        ServiceStateMutationWire::SetEnablement {
            name: name.to_string(),
            enabled,
            actor: "tester".to_string(),
        }
    }

    #[test]
    fn setting_and_clearing_enablement_round_trips() {
        let temp = tempfile::tempdir().unwrap();
        let outcome = mutate_service_state(
            temp.path(),
            set_enablement("scheduler", true),
            Some("boot-a"),
            Some(10.0),
        )
        .unwrap();
        assert!(outcome.changed);
        assert!(outcome.snapshot.state.enablement["scheduler"].enabled);

        let snapshot = read_service_state(temp.path(), Some("boot-a")).unwrap();
        assert_eq!(snapshot.state.enablement["scheduler"].updated_by, "tester");

        let outcome = mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::ClearEnablement {
                name: "scheduler".to_string(),
            },
            Some("boot-a"),
            Some(11.0),
        )
        .unwrap();
        assert!(outcome.changed);
        assert!(outcome.snapshot.state.enablement.is_empty());
    }

    #[test]
    fn stop_expires_under_different_boot_and_is_pruned_on_write() {
        let temp = tempfile::tempdir().unwrap();
        mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::Stop {
                name: "gateway".to_string(),
                actor: "tester".to_string(),
                reason: Some("manual".to_string()),
            },
            Some("boot-a"),
            Some(10.0),
        )
        .unwrap();

        let snapshot = read_service_state(temp.path(), Some("boot-b")).unwrap();
        assert_eq!(snapshot.expired_stops, vec!["gateway"]);
        assert!(snapshot.state.stops.is_empty());

        let outcome = mutate_service_state(
            temp.path(),
            set_enablement("scheduler", false),
            Some("boot-b"),
            Some(20.0),
        )
        .unwrap();
        assert_eq!(outcome.snapshot.expired_stops, vec!["gateway"]);
        let persisted =
            fs::read_to_string(service_state_path(temp.path())).unwrap();
        assert!(!persisted.contains("gateway"));
    }

    #[test]
    fn none_boot_id_semantics_match_only_none() {
        let temp = tempfile::tempdir().unwrap();
        mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::Stop {
                name: "scheduler".to_string(),
                actor: "tester".to_string(),
                reason: None,
            },
            None,
            Some(1.0),
        )
        .unwrap();
        assert!(read_service_state(temp.path(), None)
            .unwrap()
            .state
            .stops
            .contains_key("scheduler"));
        assert!(read_service_state(temp.path(), Some("boot-a"))
            .unwrap()
            .state
            .stops
            .is_empty());
    }

    #[test]
    fn markers_and_host_pid_guard_work() {
        let temp = tempfile::tempdir().unwrap();
        mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::SetMarker {
                key: "handover.ready".to_string(),
                actor: "tester".to_string(),
                detail: Some("ok".to_string()),
            },
            Some("boot-a"),
            Some(1.0),
        )
        .unwrap();
        mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::RecordHost {
                host: ServiceHostRecordWire {
                    pid: 123,
                    boot_id: Some("boot-a".to_string()),
                    started_at: 1.0,
                    heartbeat_at: 2.0,
                    mode: "foreground".to_string(),
                    unit: None,
                    sase_version: None,
                    error: None,
                },
            },
            Some("boot-a"),
            Some(2.0),
        )
        .unwrap();
        let unchanged = mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::ClearHost { pid: 456 },
            Some("boot-a"),
            Some(3.0),
        )
        .unwrap();
        assert!(!unchanged.changed);
        assert!(unchanged.snapshot.state.host.is_some());
        let cleared = mutate_service_state(
            temp.path(),
            ServiceStateMutationWire::ClearHost { pid: 123 },
            Some("boot-a"),
            Some(4.0),
        )
        .unwrap();
        assert!(cleared.changed);
        assert!(cleared.snapshot.state.host.is_none());
    }

    #[test]
    fn corrupt_file_is_reported_on_read_and_quarantined_on_mutate() {
        let temp = tempfile::tempdir().unwrap();
        let path = service_state_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"{not-json").unwrap();

        let snapshot = read_service_state(temp.path(), Some("boot-a")).unwrap();
        assert!(snapshot.diagnostics[0].contains("corrupt service state"));
        assert!(path.exists());

        let outcome = mutate_service_state(
            temp.path(),
            set_enablement("scheduler", true),
            Some("boot-a"),
            Some(12.345),
        )
        .unwrap();
        assert!(outcome.diagnostics()[0].contains("quarantined"));
        assert!(service_state_path(temp.path()).exists());
        assert!(path
            .parent()
            .unwrap()
            .join("state.json.corrupt-12345")
            .exists());
    }

    #[test]
    fn newer_schema_is_read_only_and_refuses_mutation() {
        let temp = tempfile::tempdir().unwrap();
        let path = service_state_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r#"{"schema_version":2,"enablement":{},"stops":{},"markers":{},"host":null}"#,
        )
        .unwrap();
        let snapshot = read_service_state(temp.path(), Some("boot-a")).unwrap();
        assert!(snapshot.read_only);
        assert!(matches!(
            mutate_service_state(
                temp.path(),
                set_enablement("scheduler", true),
                Some("boot-a"),
                Some(1.0),
            ),
            Err(ServiceStateError::NewerSchema { found: 2, .. })
        ));
    }

    #[test]
    fn concurrent_mutators_lose_no_updates() {
        let temp = tempfile::tempdir().unwrap();
        let home = Arc::new(temp.path().to_path_buf());
        let barrier = Arc::new(Barrier::new(6));
        let mut handles = Vec::new();
        for index in 0..5 {
            let home = Arc::clone(&home);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                barrier.wait();
                mutate_service_state(
                    home.as_path(),
                    set_enablement(&format!("svc{index}"), true),
                    Some("boot-a"),
                    Some(index as f64 + 1.0),
                )
                .unwrap();
            }));
        }
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
        let snapshot =
            read_service_state(home.as_path(), Some("boot-a")).unwrap();
        assert_eq!(snapshot.state.enablement.len(), 5);
    }

    #[test]
    fn validation_rejects_bad_names_markers_actors_and_now() {
        let temp = tempfile::tempdir().unwrap();
        assert!(matches!(
            mutate_service_state(
                temp.path(),
                set_enablement("Bad", true),
                Some("boot-a"),
                Some(1.0),
            ),
            Err(ServiceStateError::Validation(_))
        ));
        assert!(matches!(
            mutate_service_state(
                temp.path(),
                ServiceStateMutationWire::SetMarker {
                    key: ".bad".to_string(),
                    actor: "tester".to_string(),
                    detail: None,
                },
                Some("boot-a"),
                Some(1.0),
            ),
            Err(ServiceStateError::Validation(_))
        ));
        assert!(matches!(
            mutate_service_state(
                temp.path(),
                ServiceStateMutationWire::Stop {
                    name: "scheduler".to_string(),
                    actor: " ".to_string(),
                    reason: None,
                },
                Some("boot-a"),
                Some(1.0),
            ),
            Err(ServiceStateError::Validation(_))
        ));
        assert!(matches!(
            mutate_service_state(
                temp.path(),
                set_enablement("scheduler", true),
                Some("boot-a"),
                Some(f64::INFINITY),
            ),
            Err(ServiceStateError::Validation(_))
        ));
    }

    impl ServiceStateMutationOutcomeWire {
        fn diagnostics(&self) -> &[String] {
            &self.snapshot.diagnostics
        }
    }
}

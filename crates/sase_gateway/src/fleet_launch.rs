use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use sase_core::fleet_contract::{
    decide_fleet_launch_replay, validate_fleet_launch_request,
    DurableFleetLaunchRecordWire, FleetContractError,
    FleetLaunchDecisionRequestWire, FleetLaunchReceiptWire,
    FleetLaunchRequestWire, OperationDecisionKindWire,
    OperationDecisionReasonWire, OperationReceiptStateWire,
    FLEET_CONTRACT_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::wire::MobileAgentLaunchResultWire;

const FLEET_LAUNCH_DIR: &str = "mobile_gateway";
const FLEET_LAUNCH_FILE: &str = "fleet_launches.json";
const FLEET_LAUNCH_LOCK_FILE: &str = "fleet_launches.lock";

#[derive(Clone, Debug)]
pub struct FleetLaunchStore {
    state_dir: Arc<PathBuf>,
    path: Arc<PathBuf>,
    lock_path: Arc<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetLaunchAdmission {
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetLaunchReceiptWire,
    pub should_launch: bool,
}

#[derive(Debug, Error)]
pub enum FleetLaunchStoreError {
    #[error("fleet launch validation failed: {0}")]
    Validation(String),
    #[error("fleet launch conflict: {0}")]
    Conflict(String),
    #[error("fleet launch expired: {0}")]
    Expired(String),
    #[error("fleet launch store I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet launch store JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FleetLaunchStoreFile {
    schema_version: u32,
    records: Vec<DurableFleetLaunchRecordWire>,
}

impl FleetLaunchStore {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        let state_dir = sase_home.into().join(FLEET_LAUNCH_DIR);
        Self {
            path: Arc::new(state_dir.join(FLEET_LAUNCH_FILE)),
            lock_path: Arc::new(state_dir.join(FLEET_LAUNCH_LOCK_FILE)),
            state_dir: Arc::new(state_dir),
        }
    }

    pub fn reserve(
        &self,
        request: &FleetLaunchRequestWire,
        target_installation_id: &str,
        now_unix: f64,
    ) -> Result<FleetLaunchAdmission, FleetLaunchStoreError> {
        let request = validate_fleet_launch_request(request)
            .map_err(contract_error_to_launch_store)?;
        if request.target_installation_id != target_installation_id {
            return Err(FleetLaunchStoreError::Validation(
                "fleet launch target_installation_id does not match this gateway"
                    .to_string(),
            ));
        }

        let _lock = self.lock_file()?;
        let mut file = self.read_unlocked()?;
        let key = operation_key(&request.key);
        let existing = file
            .records
            .iter()
            .find(|record| operation_key(&record.receipt.key) == key);
        let decision =
            decide_fleet_launch_replay(&FleetLaunchDecisionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                key: request.key.clone(),
                payload_fingerprint: request.payload_fingerprint.clone(),
                target_installation_id: request.target_installation_id.clone(),
                now_unix,
                acceptance_window_seconds: request.acceptance_window_seconds,
                existing_record: existing.cloned(),
            })
            .map_err(contract_error_to_launch_store)?;

        let Some(receipt) = decision.receipt.clone() else {
            return Err(match decision.decision {
                OperationDecisionKindWire::Expired => {
                    FleetLaunchStoreError::Expired(format!(
                        "{:?}",
                        decision.reason
                    ))
                }
                _ => FleetLaunchStoreError::Conflict(format!(
                    "{:?}",
                    decision.reason
                )),
            });
        };
        let should_launch =
            decision.decision == OperationDecisionKindWire::AcceptNew;
        if should_launch {
            file.records.push(DurableFleetLaunchRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            });
            self.write_unlocked(&file)?;
        }
        Ok(FleetLaunchAdmission {
            decision: decision.decision,
            reason: decision.reason,
            receipt,
            should_launch,
        })
    }

    pub fn settle(
        &self,
        receipt: &FleetLaunchReceiptWire,
        launch: &MobileAgentLaunchResultWire,
        project_id: &str,
        message: Option<String>,
    ) -> Result<FleetLaunchReceiptWire, FleetLaunchStoreError> {
        let _lock = self.lock_file()?;
        let mut file = self.read_unlocked()?;
        let key = operation_key(&receipt.key);
        let Some(record) = file
            .records
            .iter_mut()
            .find(|record| operation_key(&record.receipt.key) == key)
        else {
            return Err(FleetLaunchStoreError::Validation(
                "fleet launch reservation record is missing".to_string(),
            ));
        };
        record.receipt.state = OperationReceiptStateWire::Settled;
        record.receipt.logical_locator =
            launch_primary_logical_locator(launch, receipt, project_id);
        record.receipt.message = message.filter(|value| {
            !value.trim().is_empty()
                && !value.contains('/')
                && !value.to_ascii_lowercase().contains("bearer ")
        });
        let settled = record.receipt.clone();
        self.write_unlocked(&file)?;
        Ok(settled)
    }

    fn lock_file(&self) -> Result<File, FleetLaunchStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetLaunchStoreError::Io {
                path: self.state_dir.as_ref().clone(),
                source,
            }
        })?;
        restrict_dir(self.state_dir.as_ref())?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.lock_path.as_ref())
            .map_err(|source| FleetLaunchStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            })?;
        file.lock_exclusive()
            .map_err(|source| FleetLaunchStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            })?;
        Ok(file)
    }

    fn read_unlocked(
        &self,
    ) -> Result<FleetLaunchStoreFile, FleetLaunchStoreError> {
        let bytes = match fs::read(self.path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(FleetLaunchStoreFile {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    records: Vec::new(),
                })
            }
            Err(source) => {
                return Err(FleetLaunchStoreError::Io {
                    path: self.path.as_ref().clone(),
                    source,
                })
            }
        };
        let file: FleetLaunchStoreFile = serde_json::from_slice(&bytes)
            .map_err(|source| FleetLaunchStoreError::Json {
                path: self.path.as_ref().clone(),
                source,
            })?;
        if file.schema_version != FLEET_CONTRACT_SCHEMA_VERSION {
            return Err(FleetLaunchStoreError::Validation(
                "unsupported fleet launch store schema_version".to_string(),
            ));
        }
        Ok(file)
    }

    fn write_unlocked(
        &self,
        file: &FleetLaunchStoreFile,
    ) -> Result<(), FleetLaunchStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetLaunchStoreError::Io {
                path: self.state_dir.as_ref().clone(),
                source,
            }
        })?;
        restrict_dir(self.state_dir.as_ref())?;
        let tmp = self.path.with_file_name(format!(
            ".{FLEET_LAUNCH_FILE}.{}.{}.tmp",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut bytes = serde_json::to_vec_pretty(file).map_err(|source| {
            FleetLaunchStoreError::Json {
                path: self.path.as_ref().clone(),
                source,
            }
        })?;
        bytes.push(b'\n');
        {
            let mut handle = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&tmp)
                .map_err(|source| FleetLaunchStoreError::Io {
                    path: tmp.clone(),
                    source,
                })?;
            restrict_file(&tmp)?;
            handle.write_all(&bytes).map_err(|source| {
                FleetLaunchStoreError::Io {
                    path: tmp.clone(),
                    source,
                }
            })?;
            handle
                .sync_all()
                .map_err(|source| FleetLaunchStoreError::Io {
                    path: tmp.clone(),
                    source,
                })?;
        }
        fs::rename(&tmp, self.path.as_ref()).map_err(|source| {
            let _ = fs::remove_file(&tmp);
            FleetLaunchStoreError::Io {
                path: self.path.as_ref().clone(),
                source,
            }
        })?;
        restrict_file(self.path.as_ref())?;
        Ok(())
    }
}

fn operation_key(key: &sase_core::ScopedOperationKeyWire) -> String {
    format!("{}\0{}", key.controller_id, key.operation_id)
}

fn launch_primary_logical_locator(
    launch: &MobileAgentLaunchResultWire,
    receipt: &FleetLaunchReceiptWire,
    project_id: &str,
) -> Option<sase_core::LogicalAgentLocatorWire> {
    let agent_id = launch
        .primary
        .as_ref()
        .and_then(|slot| slot.name.as_ref())
        .filter(|name| is_locator_component(name))
        .cloned()
        .unwrap_or_else(|| receipt.key.operation_id.clone());
    Some(sase_core::LogicalAgentLocatorWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        project: sase_core::ProjectLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin: sase_core::OriginLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                installation_id: receipt.target_installation_id.clone(),
            },
            project_id: project_id.to_string(),
        },
        agent_id,
        family_id: None,
    })
}

fn is_locator_component(value: &str) -> bool {
    let value = value.trim();
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'-' | b'.' | b':')
        })
}

fn contract_error_to_launch_store(
    error: FleetContractError,
) -> FleetLaunchStoreError {
    FleetLaunchStoreError::Validation(error.to_string())
}

fn restrict_dir(path: &Path) -> Result<(), FleetLaunchStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
            |source| FleetLaunchStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), FleetLaunchStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(
            |source| FleetLaunchStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

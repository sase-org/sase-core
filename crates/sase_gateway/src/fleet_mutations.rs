use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use sase_core::{
    decide_fleet_mutation_replay, validate_fleet_mutation_request,
    DurableFleetMutationRecordWire, FleetContractError,
    FleetMutationDecisionRequestWire, FleetMutationKindWire,
    FleetMutationOutcomeWire, FleetMutationReceiptWire,
    FleetMutationRequestWire, LogicalAgentLocatorWire,
    OperationDecisionKindWire, OperationDecisionReasonWire,
    OperationReceiptStateWire, FLEET_CONTRACT_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

const FLEET_MUTATION_DIR: &str = "mobile_gateway";
const FLEET_MUTATION_FILE: &str = "fleet_mutations.json";
const FLEET_MUTATION_LOCK_FILE: &str = "fleet_mutations.lock";

#[derive(Clone, Debug)]
pub struct FleetMutationStore {
    state_dir: Arc<PathBuf>,
    path: Arc<PathBuf>,
    lock_path: Arc<PathBuf>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMutationAdmission {
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetMutationReceiptWire,
    pub should_execute: bool,
}

#[derive(Debug, Error)]
pub enum FleetMutationStoreError {
    #[error("fleet mutation validation failed: {0}")]
    Validation(String),
    #[error("fleet mutation conflict: {0}")]
    Conflict(String),
    #[error("fleet mutation expired: {0}")]
    Expired(String),
    #[error("fleet mutation store I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet mutation store JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FleetMutationStoreFile {
    schema_version: u32,
    records: Vec<DurableFleetMutationRecordWire>,
}

impl FleetMutationStore {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        let state_dir = sase_home.into().join(FLEET_MUTATION_DIR);
        Self {
            path: Arc::new(state_dir.join(FLEET_MUTATION_FILE)),
            lock_path: Arc::new(state_dir.join(FLEET_MUTATION_LOCK_FILE)),
            state_dir: Arc::new(state_dir),
        }
    }

    pub fn reserve(
        &self,
        request: &FleetMutationRequestWire,
        target_installation_id: &str,
        now_unix: f64,
    ) -> Result<FleetMutationAdmission, FleetMutationStoreError> {
        let request = validate_fleet_mutation_request(request)
            .map_err(contract_error_to_mutation_store)?;
        if request.target_installation_id != target_installation_id {
            return Err(FleetMutationStoreError::Validation(
                "fleet mutation target_installation_id does not match this gateway"
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
            decide_fleet_mutation_replay(&FleetMutationDecisionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                key: request.key.clone(),
                payload_fingerprint: request.payload_fingerprint.clone(),
                target: request.intent.target.clone(),
                resource_revision: request.intent.row_revision.clone(),
                target_installation_id: request.target_installation_id.clone(),
                now_unix,
                acceptance_window_seconds: request.acceptance_window_seconds,
                existing_record: existing.cloned(),
            })
            .map_err(contract_error_to_mutation_store)?;

        let Some(receipt) = decision.receipt.clone() else {
            return Err(match decision.decision {
                OperationDecisionKindWire::Expired => {
                    FleetMutationStoreError::Expired(format!(
                        "{:?}",
                        decision.reason
                    ))
                }
                _ => FleetMutationStoreError::Conflict(format!(
                    "{:?}",
                    decision.reason
                )),
            });
        };
        let should_execute =
            decision.decision == OperationDecisionKindWire::AcceptNew;
        if should_execute {
            file.records.push(DurableFleetMutationRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            });
            self.write_unlocked(&file)?;
        }
        Ok(FleetMutationAdmission {
            decision: decision.decision,
            reason: decision.reason,
            receipt,
            should_execute,
        })
    }

    pub fn settle(
        &self,
        receipt: &FleetMutationReceiptWire,
        kind: FleetMutationKindWire,
        result_locator: Option<LogicalAgentLocatorWire>,
        instance_locator: Option<sase_core::AgentInstanceLocatorWire>,
        message: Option<String>,
    ) -> Result<FleetMutationReceiptWire, FleetMutationStoreError> {
        let _lock = self.lock_file()?;
        let mut file = self.read_unlocked()?;
        let key = operation_key(&receipt.key);
        let Some(record) = file
            .records
            .iter_mut()
            .find(|record| operation_key(&record.receipt.key) == key)
        else {
            return Err(FleetMutationStoreError::Validation(
                "fleet mutation reservation record is missing".to_string(),
            ));
        };
        record.receipt.state = OperationReceiptStateWire::Settled;
        record.receipt.outcome = Some(FleetMutationOutcomeWire::Applied);
        record.receipt.logical_locator = result_locator.or_else(|| {
            if matches!(kind, FleetMutationKindWire::Stop) {
                Some(record.receipt.target.logical.clone())
            } else {
                None
            }
        });
        record.receipt.instance_locator = instance_locator;
        record.receipt.message = message.filter(|value| {
            !value.trim().is_empty()
                && !value.contains('/')
                && !value.to_ascii_lowercase().contains("bearer ")
        });
        let settled = record.receipt.clone();
        self.write_unlocked(&file)?;
        Ok(settled)
    }

    fn lock_file(&self) -> Result<File, FleetMutationStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetMutationStoreError::Io {
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
            .map_err(|source| FleetMutationStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| {
            FleetMutationStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            }
        })?;
        Ok(file)
    }

    fn read_unlocked(
        &self,
    ) -> Result<FleetMutationStoreFile, FleetMutationStoreError> {
        let bytes = match fs::read(self.path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(FleetMutationStoreFile {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    records: Vec::new(),
                })
            }
            Err(source) => {
                return Err(FleetMutationStoreError::Io {
                    path: self.path.as_ref().clone(),
                    source,
                })
            }
        };
        let file: FleetMutationStoreFile = serde_json::from_slice(&bytes)
            .map_err(|source| FleetMutationStoreError::Json {
                path: self.path.as_ref().clone(),
                source,
            })?;
        if file.schema_version != FLEET_CONTRACT_SCHEMA_VERSION {
            return Err(FleetMutationStoreError::Validation(
                "unsupported fleet mutation store schema_version".to_string(),
            ));
        }
        Ok(file)
    }

    fn write_unlocked(
        &self,
        file: &FleetMutationStoreFile,
    ) -> Result<(), FleetMutationStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetMutationStoreError::Io {
                path: self.state_dir.as_ref().clone(),
                source,
            }
        })?;
        restrict_dir(self.state_dir.as_ref())?;
        let tmp = self.path.with_file_name(format!(
            ".{FLEET_MUTATION_FILE}.{}.{}.tmp",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut bytes = serde_json::to_vec_pretty(file).map_err(|source| {
            FleetMutationStoreError::Json {
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
                .map_err(|source| FleetMutationStoreError::Io {
                    path: tmp.clone(),
                    source,
                })?;
            restrict_file(&tmp)?;
            handle.write_all(&bytes).map_err(|source| {
                FleetMutationStoreError::Io {
                    path: tmp.clone(),
                    source,
                }
            })?;
            handle.sync_all().map_err(|source| {
                FleetMutationStoreError::Io {
                    path: tmp.clone(),
                    source,
                }
            })?;
        }
        fs::rename(&tmp, self.path.as_ref()).map_err(|source| {
            let _ = fs::remove_file(&tmp);
            FleetMutationStoreError::Io {
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

fn contract_error_to_mutation_store(
    error: FleetContractError,
) -> FleetMutationStoreError {
    FleetMutationStoreError::Validation(error.to_string())
}

fn restrict_dir(path: &Path) -> Result<(), FleetMutationStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
            |source| FleetMutationStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), FleetMutationStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(
            |source| FleetMutationStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

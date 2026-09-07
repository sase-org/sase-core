use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use fs2::FileExt;
use sase_core::{
    decide_fleet_attention_replay, validate_fleet_attention_request,
    DurableFleetAttentionRecordWire, FleetAttentionDecisionRequestWire,
    FleetAttentionOutcomeWire, FleetAttentionReceiptWire,
    FleetAttentionRequestKeyWire, FleetAttentionRequestWire,
    FleetContractError, OperationDecisionKindWire, OperationDecisionReasonWire,
    OperationReceiptStateWire, FLEET_CONTRACT_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

const FLEET_ATTENTION_DIR: &str = "mobile_gateway";
const FLEET_ATTENTION_FILE: &str = "fleet_attention.json";
const FLEET_ATTENTION_LOCK_FILE: &str = "fleet_attention.lock";

#[derive(Clone, Debug)]
pub struct FleetAttentionStore {
    state_dir: Arc<PathBuf>,
    path: Arc<PathBuf>,
    lock_path: Arc<PathBuf>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FleetAttentionAdmission {
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetAttentionReceiptWire,
    pub should_execute: bool,
}

#[derive(Debug, Error)]
pub enum FleetAttentionStoreError {
    #[error("fleet attention validation failed: {0}")]
    Validation(String),
    #[error("fleet attention conflict: {0}")]
    Conflict(String),
    #[error("fleet attention expired: {0}")]
    Expired(String),
    #[error("fleet attention store I/O failed at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("fleet attention store JSON failed at {}: {source}", path.display())]
    Json {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FleetAttentionStoreFile {
    schema_version: u32,
    records: Vec<DurableFleetAttentionRecordWire>,
}

impl FleetAttentionStore {
    pub fn new(sase_home: impl Into<PathBuf>) -> Self {
        let state_dir = sase_home.into().join(FLEET_ATTENTION_DIR);
        Self {
            path: Arc::new(state_dir.join(FLEET_ATTENTION_FILE)),
            lock_path: Arc::new(state_dir.join(FLEET_ATTENTION_LOCK_FILE)),
            state_dir: Arc::new(state_dir),
        }
    }

    pub fn reserve(
        &self,
        request: &FleetAttentionRequestWire,
        target_installation_id: &str,
        now_unix: f64,
    ) -> Result<FleetAttentionAdmission, FleetAttentionStoreError> {
        let request = validate_fleet_attention_request(request)
            .map_err(contract_error_to_attention_store)?;
        if request.target_installation_id != target_installation_id {
            return Err(FleetAttentionStoreError::Validation(
                "fleet attention target_installation_id does not match this gateway"
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
            decide_fleet_attention_replay(&FleetAttentionDecisionRequestWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                key: request.key.clone(),
                payload_fingerprint: request.payload_fingerprint.clone(),
                request_key: request.intent.request_key.clone(),
                observed_revision: request.intent.observed_revision,
                target_installation_id: request.target_installation_id.clone(),
                now_unix,
                acceptance_window_seconds: request.acceptance_window_seconds,
                existing_record: existing.cloned(),
            })
            .map_err(contract_error_to_attention_store)?;

        let Some(receipt) = decision.receipt.clone() else {
            return Err(match decision.decision {
                OperationDecisionKindWire::Expired => {
                    FleetAttentionStoreError::Expired(format!(
                        "{:?}",
                        decision.reason
                    ))
                }
                _ => FleetAttentionStoreError::Conflict(format!(
                    "{:?}",
                    decision.reason
                )),
            });
        };
        let should_execute =
            decision.decision == OperationDecisionKindWire::AcceptNew;
        if should_execute {
            file.records.push(DurableFleetAttentionRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: receipt.clone(),
                tombstoned_at_unix_ms: None,
            });
            self.write_unlocked(&file)?;
        }
        Ok(FleetAttentionAdmission {
            decision: decision.decision,
            reason: decision.reason,
            receipt,
            should_execute,
        })
    }

    /// Bind the settled outcome onto a previously-reserved receipt. `outcome`
    /// is `Applied` when this host's own execution answered/approved the
    /// request, or `AlreadySettled` when the host bridge reports the request
    /// was already handled by a different controller (a race this host
    /// lost) — either way the receipt records exactly one settlement.
    pub fn settle(
        &self,
        receipt: &FleetAttentionReceiptWire,
        outcome: FleetAttentionOutcomeWire,
        settled_by_host_label: Option<String>,
        settled_response: Option<JsonValue>,
        message: Option<String>,
    ) -> Result<FleetAttentionReceiptWire, FleetAttentionStoreError> {
        let _lock = self.lock_file()?;
        let mut file = self.read_unlocked()?;
        let key = operation_key(&receipt.key);
        let Some(record) = file
            .records
            .iter_mut()
            .find(|record| operation_key(&record.receipt.key) == key)
        else {
            return Err(FleetAttentionStoreError::Validation(
                "fleet attention reservation record is missing".to_string(),
            ));
        };
        record.receipt.state = OperationReceiptStateWire::Settled;
        record.receipt.outcome = Some(outcome);
        record.receipt.settled_by_host_label =
            settled_by_host_label.filter(|value| is_safe_label(value));
        record.receipt.settled_response = settled_response;
        record.receipt.message = message.filter(|value| is_safe_label(value));
        let settled = record.receipt.clone();
        self.write_unlocked(&file)?;
        Ok(settled)
    }

    /// Return the settled receipt for `request_key`, if any operation key
    /// has already settled it. Used to tell a losing controller the real
    /// settled result rather than a generic message, regardless of which
    /// operation key answered first.
    pub fn find_settled_by_request_key(
        &self,
        request_key: &FleetAttentionRequestKeyWire,
    ) -> Result<Option<FleetAttentionReceiptWire>, FleetAttentionStoreError>
    {
        let _lock = self.lock_file()?;
        let file = self.read_unlocked()?;
        Ok(file.records.into_iter().map(|record| record.receipt).find(
            |receipt| {
                receipt.request_key == *request_key
                    && receipt.state == OperationReceiptStateWire::Settled
            },
        ))
    }

    fn lock_file(&self) -> Result<File, FleetAttentionStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetAttentionStoreError::Io {
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
            .map_err(|source| FleetAttentionStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            })?;
        file.lock_exclusive().map_err(|source| {
            FleetAttentionStoreError::Io {
                path: self.lock_path.as_ref().clone(),
                source,
            }
        })?;
        Ok(file)
    }

    fn read_unlocked(
        &self,
    ) -> Result<FleetAttentionStoreFile, FleetAttentionStoreError> {
        let bytes = match fs::read(self.path.as_ref()) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(FleetAttentionStoreFile {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    records: Vec::new(),
                })
            }
            Err(source) => {
                return Err(FleetAttentionStoreError::Io {
                    path: self.path.as_ref().clone(),
                    source,
                })
            }
        };
        let file: FleetAttentionStoreFile = serde_json::from_slice(&bytes)
            .map_err(|source| FleetAttentionStoreError::Json {
                path: self.path.as_ref().clone(),
                source,
            })?;
        if file.schema_version != FLEET_CONTRACT_SCHEMA_VERSION {
            return Err(FleetAttentionStoreError::Validation(
                "unsupported fleet attention store schema_version".to_string(),
            ));
        }
        Ok(file)
    }

    fn write_unlocked(
        &self,
        file: &FleetAttentionStoreFile,
    ) -> Result<(), FleetAttentionStoreError> {
        fs::create_dir_all(self.state_dir.as_ref()).map_err(|source| {
            FleetAttentionStoreError::Io {
                path: self.state_dir.as_ref().clone(),
                source,
            }
        })?;
        restrict_dir(self.state_dir.as_ref())?;
        let tmp = self.path.with_file_name(format!(
            ".{FLEET_ATTENTION_FILE}.{}.{}.tmp",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut bytes = serde_json::to_vec_pretty(file).map_err(|source| {
            FleetAttentionStoreError::Json {
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
                .map_err(|source| FleetAttentionStoreError::Io {
                    path: tmp.clone(),
                    source,
                })?;
            restrict_file(&tmp)?;
            handle.write_all(&bytes).map_err(|source| {
                FleetAttentionStoreError::Io {
                    path: tmp.clone(),
                    source,
                }
            })?;
            handle.sync_all().map_err(|source| {
                FleetAttentionStoreError::Io {
                    path: tmp.clone(),
                    source,
                }
            })?;
        }
        fs::rename(&tmp, self.path.as_ref()).map_err(|source| {
            let _ = fs::remove_file(&tmp);
            FleetAttentionStoreError::Io {
                path: self.path.as_ref().clone(),
                source,
            }
        })?;
        restrict_file(self.path.as_ref())?;
        Ok(())
    }
}

fn is_safe_label(value: &str) -> bool {
    !value.trim().is_empty()
        && !value.contains('/')
        && !value.to_ascii_lowercase().contains("bearer ")
}

fn operation_key(key: &sase_core::ScopedOperationKeyWire) -> String {
    format!("{}\0{}", key.controller_id, key.operation_id)
}

fn contract_error_to_attention_store(
    error: FleetContractError,
) -> FleetAttentionStoreError {
    FleetAttentionStoreError::Validation(error.to_string())
}

fn restrict_dir(path: &Path) -> Result<(), FleetAttentionStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(
            |source| FleetAttentionStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn restrict_file(path: &Path) -> Result<(), FleetAttentionStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(
            |source| FleetAttentionStoreError::Io {
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sase_core::{
        FleetAttentionIntentWire, FleetAttentionKindWire,
        FleetAttentionRequestKeyWire, ScopedOperationKeyWire,
    };

    fn installation() -> String {
        format!(
            "{}{}",
            sase_core::fleet_contract::FLEET_INSTALLATION_ID_PREFIX,
            "a".repeat(64)
        )
    }

    fn intent() -> FleetAttentionIntentWire {
        FleetAttentionIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetAttentionKindWire::Gate,
            request_key: FleetAttentionRequestKeyWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin_installation_id: installation(),
                request_id: "notif-1".to_string(),
                pending_action_prefix: "notif-1"[..7].to_string(),
            },
            observed_revision: 1,
            selected_option_ids: vec!["approve".to_string()],
            feedback: None,
            question_choice: None,
            question_index: None,
            selected_option_id: None,
            selected_option_label: None,
            selected_option_index: None,
            custom_answer: None,
            global_note: None,
        }
    }

    fn request() -> FleetAttentionRequestWire {
        let intent = intent();
        let fingerprint =
            sase_core::fleet_attention_payload_fingerprint(&intent).unwrap();
        FleetAttentionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: ScopedOperationKeyWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                controller_id: "controller-1".to_string(),
                operation_id: "op-1".to_string(),
            },
            target_installation_id: installation(),
            intent,
            payload_fingerprint: fingerprint,
            acceptance_window_seconds: 30.0,
        }
    }

    #[test]
    fn reserve_then_settle_round_trips_through_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = FleetAttentionStore::new(dir.path());
        let admission =
            store.reserve(&request(), &installation(), 10.0).unwrap();
        assert!(admission.should_execute);
        let settled = store
            .settle(
                &admission.receipt,
                FleetAttentionOutcomeWire::Applied,
                None,
                Some(serde_json::json!({"selected": "approve"})),
                Some("Approved".to_string()),
            )
            .unwrap();
        assert_eq!(settled.state, OperationReceiptStateWire::Settled);
        assert_eq!(settled.outcome, Some(FleetAttentionOutcomeWire::Applied));

        // A second reservation with the identical key+payload replays the
        // settled receipt rather than re-executing.
        let replay = store.reserve(&request(), &installation(), 11.0).unwrap();
        assert!(!replay.should_execute);
        assert_eq!(
            replay.decision,
            OperationDecisionKindWire::ReturnOriginalReceipt
        );
        assert_eq!(replay.receipt.state, OperationReceiptStateWire::Settled);
    }

    #[test]
    fn settle_rejects_unsafe_message_and_host_label() {
        let dir = tempfile::tempdir().unwrap();
        let store = FleetAttentionStore::new(dir.path());
        let admission =
            store.reserve(&request(), &installation(), 10.0).unwrap();
        let settled = store
            .settle(
                &admission.receipt,
                FleetAttentionOutcomeWire::AlreadySettled,
                Some("/tmp/leaky".to_string()),
                None,
                Some("bearer abc123".to_string()),
            )
            .unwrap();
        assert!(settled.settled_by_host_label.is_none());
        assert!(settled.message.is_none());
    }

    #[test]
    fn reserve_rejects_installation_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let store = FleetAttentionStore::new(dir.path());
        let other = format!(
            "{}{}",
            sase_core::fleet_contract::FLEET_INSTALLATION_ID_PREFIX,
            "b".repeat(64)
        );
        let error = store.reserve(&request(), &other, 10.0).unwrap_err();
        assert!(matches!(error, FleetAttentionStoreError::Validation(_)));
    }
}

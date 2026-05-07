use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use chrono::{DateTime, SecondsFormat, Utc};
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::wire::{
    DeviceRecordWire, PairingDeviceMetadataWire, PushHintCategoryWire,
    PushProviderWire, PushSubscriptionRecordWire, PushSubscriptionRequestWire,
    GATEWAY_WIRE_SCHEMA_VERSION,
};

const DEVICES_FILE: &str = "devices.json";
const PUSH_SUBSCRIPTIONS_FILE: &str = "push_subscriptions.json";
const AUDIT_FILE: &str = "audit.jsonl";

#[derive(Clone, Debug)]
pub struct DeviceTokenStore {
    state_dir: Arc<PathBuf>,
    lock: Arc<Mutex<()>>,
}

impl DeviceTokenStore {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: Arc::new(state_dir.into()),
            lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    pub fn pair_device(
        &self,
        device: PairingDeviceMetadataWire,
        now: DateTime<Utc>,
    ) -> Result<(DeviceRecordWire, String), StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut file = self.read_devices_unlocked()?;
        let token = generate_token();
        let record = StoredDeviceRecord {
            device_id: generate_prefixed_id("dev"),
            display_name: device.display_name,
            platform: device.platform,
            app_version: device.app_version,
            token_hash: hash_token(&token),
            paired_at: format_time(now),
            last_seen_at: None,
            revoked_at: None,
        };
        let wire = record.to_wire();
        file.devices.push(record);
        self.write_devices_unlocked(&file)?;
        Ok((wire, token))
    }

    pub fn authenticate_token(
        &self,
        token: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<DeviceRecordWire>, StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut file = self.read_devices_unlocked()?;
        let token_hash = hash_token(token);
        let Some(record) = file
            .devices
            .iter_mut()
            .find(|record| record.token_hash == token_hash)
        else {
            return Ok(None);
        };
        if record.revoked_at.is_some() {
            return Ok(None);
        }
        record.last_seen_at = Some(format_time(now));
        let wire = record.to_wire();
        self.write_devices_unlocked(&file)?;
        Ok(Some(wire))
    }

    pub fn revoke_device(
        &self,
        device_id: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut file = self.read_devices_unlocked()?;
        let Some(record) = file
            .devices
            .iter_mut()
            .find(|record| record.device_id == device_id)
        else {
            return Ok(false);
        };
        record.revoked_at = Some(format_time(now));
        self.write_devices_unlocked(&file)?;
        Ok(true)
    }

    pub fn list_push_subscriptions(
        &self,
        device_id: &str,
    ) -> Result<Vec<PushSubscriptionRecordWire>, StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        Ok(self
            .read_push_subscriptions_unlocked()?
            .subscriptions
            .into_iter()
            .filter(|record| {
                record.device_id == device_id && record.disabled_at.is_none()
            })
            .map(|record| record.to_wire())
            .collect())
    }

    pub fn register_push_subscription(
        &self,
        device_id: &str,
        request: PushSubscriptionRequestWire,
        now: DateTime<Utc>,
    ) -> Result<(PushSubscriptionRecordWire, bool), StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut file = self.read_push_subscriptions_unlocked()?;
        let now = format_time(now);
        if let Some(record) = file.subscriptions.iter_mut().find(|record| {
            record.device_id == device_id
                && record.provider == request.provider
                && record.provider_token == request.provider_token
                && record.app_instance_id == request.app_instance_id
        }) {
            record.device_display_name = request.device_display_name;
            record.platform = request.platform;
            record.app_version = request.app_version;
            record.hint_categories = request.hint_categories;
            record.disabled_at = None;
            record.last_seen_at = Some(now);
            let wire = record.to_wire();
            self.write_push_subscriptions_unlocked(&file)?;
            return Ok((wire, false));
        }

        let record = StoredPushSubscriptionRecord {
            id: generate_prefixed_id("push"),
            device_id: device_id.to_string(),
            provider: request.provider,
            provider_token: request.provider_token,
            app_instance_id: request.app_instance_id,
            device_display_name: request.device_display_name,
            platform: request.platform,
            app_version: request.app_version,
            hint_categories: request.hint_categories,
            enabled_at: now,
            disabled_at: None,
            last_seen_at: None,
        };
        let wire = record.to_wire();
        file.subscriptions.push(record);
        self.write_push_subscriptions_unlocked(&file)?;
        Ok((wire, true))
    }

    pub fn revoke_push_subscription(
        &self,
        device_id: &str,
        subscription_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<(PushSubscriptionRecordWire, bool)>, StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        let mut file = self.read_push_subscriptions_unlocked()?;
        let Some(record) = file.subscriptions.iter_mut().find(|record| {
            record.device_id == device_id && record.id == subscription_id
        }) else {
            return Ok(None);
        };
        let revoked = record.disabled_at.is_none();
        if revoked {
            record.disabled_at = Some(format_time(now));
        }
        let wire = record.to_wire();
        self.write_push_subscriptions_unlocked(&file)?;
        Ok(Some((wire, revoked)))
    }

    pub fn append_audit(
        &self,
        entry: AuditLogEntryWire,
    ) -> Result<(), StoreError> {
        let _guard = self.lock.lock().map_err(|_| StoreError::LockPoisoned)?;
        fs::create_dir_all(self.state_dir.as_path()).map_err(StoreError::Io)?;
        let audit_path = self.state_dir.join(AUDIT_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(audit_path)
            .map_err(StoreError::Io)?;
        serde_json::to_writer(&mut file, &entry).map_err(StoreError::Json)?;
        file.write_all(b"\n").map_err(StoreError::Io)?;
        file.sync_all().map_err(StoreError::Io)?;
        Ok(())
    }

    fn read_devices_unlocked(&self) -> Result<StoredDevicesFile, StoreError> {
        let path = self.state_dir.join(DEVICES_FILE);
        if !path.exists() {
            return Ok(StoredDevicesFile::default());
        }
        let bytes = fs::read(path).map_err(StoreError::Io)?;
        serde_json::from_slice(&bytes).map_err(StoreError::Json)
    }

    fn write_devices_unlocked(
        &self,
        file: &StoredDevicesFile,
    ) -> Result<(), StoreError> {
        fs::create_dir_all(self.state_dir.as_path()).map_err(StoreError::Io)?;
        let path = self.state_dir.join(DEVICES_FILE);
        let temp_path = self.state_dir.join(format!(
            "{DEVICES_FILE}.{}.tmp",
            generate_prefixed_id("write")
        ));
        let bytes =
            serde_json::to_vec_pretty(file).map_err(StoreError::Json)?;
        fs::write(&temp_path, bytes).map_err(StoreError::Io)?;
        fs::rename(&temp_path, path).map_err(StoreError::Io)?;
        Ok(())
    }

    fn read_push_subscriptions_unlocked(
        &self,
    ) -> Result<StoredPushSubscriptionsFile, StoreError> {
        let path = self.state_dir.join(PUSH_SUBSCRIPTIONS_FILE);
        if !path.exists() {
            return Ok(StoredPushSubscriptionsFile::default());
        }
        let bytes = fs::read(path).map_err(StoreError::Io)?;
        serde_json::from_slice(&bytes).map_err(StoreError::Json)
    }

    fn write_push_subscriptions_unlocked(
        &self,
        file: &StoredPushSubscriptionsFile,
    ) -> Result<(), StoreError> {
        fs::create_dir_all(self.state_dir.as_path()).map_err(StoreError::Io)?;
        let path = self.state_dir.join(PUSH_SUBSCRIPTIONS_FILE);
        let temp_path = self.state_dir.join(format!(
            "{PUSH_SUBSCRIPTIONS_FILE}.{}.tmp",
            generate_prefixed_id("write")
        ));
        let bytes =
            serde_json::to_vec_pretty(file).map_err(StoreError::Json)?;
        fs::write(&temp_path, bytes).map_err(StoreError::Io)?;
        fs::rename(&temp_path, path).map_err(StoreError::Io)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("gateway device store lock was poisoned")]
    LockPoisoned,
    #[error("gateway device store I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("gateway device store JSON failed: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditLogEntryWire {
    pub schema_version: u32,
    pub timestamp: String,
    pub device_id: Option<String>,
    pub endpoint: String,
    pub target_id: Option<String>,
    pub outcome: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredDevicesFile {
    schema_version: u32,
    devices: Vec<StoredDeviceRecord>,
}

impl Default for StoredDevicesFile {
    fn default() -> Self {
        Self {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            devices: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredDeviceRecord {
    device_id: String,
    display_name: String,
    platform: String,
    app_version: Option<String>,
    token_hash: String,
    paired_at: String,
    last_seen_at: Option<String>,
    revoked_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPushSubscriptionsFile {
    schema_version: u32,
    subscriptions: Vec<StoredPushSubscriptionRecord>,
}

impl Default for StoredPushSubscriptionsFile {
    fn default() -> Self {
        Self {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            subscriptions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct StoredPushSubscriptionRecord {
    id: String,
    device_id: String,
    provider: PushProviderWire,
    provider_token: String,
    app_instance_id: Option<String>,
    device_display_name: Option<String>,
    platform: Option<String>,
    app_version: Option<String>,
    hint_categories: Vec<PushHintCategoryWire>,
    enabled_at: String,
    disabled_at: Option<String>,
    last_seen_at: Option<String>,
}

impl StoredPushSubscriptionRecord {
    fn to_wire(&self) -> PushSubscriptionRecordWire {
        PushSubscriptionRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            id: self.id.clone(),
            device_id: self.device_id.clone(),
            provider: self.provider.clone(),
            provider_token: self.provider_token.clone(),
            app_instance_id: self.app_instance_id.clone(),
            device_display_name: self.device_display_name.clone(),
            platform: self.platform.clone(),
            app_version: self.app_version.clone(),
            hint_categories: self.hint_categories.clone(),
            enabled_at: self.enabled_at.clone(),
            disabled_at: self.disabled_at.clone(),
            last_seen_at: self.last_seen_at.clone(),
        }
    }
}

impl StoredDeviceRecord {
    fn to_wire(&self) -> DeviceRecordWire {
        DeviceRecordWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            device_id: self.device_id.clone(),
            display_name: self.display_name.clone(),
            platform: self.platform.clone(),
            app_version: self.app_version.clone(),
            paired_at: Some(self.paired_at.clone()),
            last_seen_at: self.last_seen_at.clone(),
            revoked_at: self.revoked_at.clone(),
        }
    }
}

pub fn format_time(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub fn generate_pairing_code() -> String {
    let mut rng = rand::thread_rng();
    format!("{:06}", rng.gen_range(0..1_000_000))
}

pub fn generate_prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", random_alphanumeric(20))
}

fn generate_token() -> String {
    format!("sase_mobile_{}", random_alphanumeric(48))
}

fn random_alphanumeric(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_pairing_persists_only_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DeviceTokenStore::new(tmp.path());
        let (_device, token) = store
            .pair_device(
                PairingDeviceMetadataWire {
                    display_name: "Pixel".to_string(),
                    platform: "android".to_string(),
                    app_version: None,
                },
                Utc::now(),
            )
            .unwrap();

        let devices =
            fs::read_to_string(tmp.path().join(DEVICES_FILE)).unwrap();
        assert!(!devices.contains(&token));
        assert!(devices.contains("token_hash"));
    }

    #[test]
    fn push_subscription_register_update_and_revoke_is_atomic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = DeviceTokenStore::new(tmp.path());
        let now = Utc::now();
        let (device, _token) = store
            .pair_device(
                PairingDeviceMetadataWire {
                    display_name: "Pixel".to_string(),
                    platform: "android".to_string(),
                    app_version: Some("0.1.0".to_string()),
                },
                now,
            )
            .unwrap();

        let request = PushSubscriptionRequestWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            provider: PushProviderWire::Fcm,
            provider_token: "opaque-token".to_string(),
            app_instance_id: Some("app".to_string()),
            device_display_name: Some("Pixel".to_string()),
            platform: Some("android".to_string()),
            app_version: Some("0.1.0".to_string()),
            hint_categories: vec![PushHintCategoryWire::Notifications],
        };
        let (created, created_flag) = store
            .register_push_subscription(&device.device_id, request, now)
            .unwrap();
        assert!(created_flag);

        let updated_request = PushSubscriptionRequestWire {
            schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
            provider: PushProviderWire::Fcm,
            provider_token: "opaque-token".to_string(),
            app_instance_id: Some("app".to_string()),
            device_display_name: Some("Pixel".to_string()),
            platform: Some("android".to_string()),
            app_version: Some("0.2.0".to_string()),
            hint_categories: vec![PushHintCategoryWire::Agents],
        };
        let (updated, updated_flag) = store
            .register_push_subscription(&device.device_id, updated_request, now)
            .unwrap();
        assert!(!updated_flag);
        assert_eq!(updated.id, created.id);
        assert_eq!(updated.app_version.as_deref(), Some("0.2.0"));
        assert_eq!(updated.hint_categories, vec![PushHintCategoryWire::Agents]);

        let listed = store.list_push_subscriptions(&device.device_id).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);

        let revoked = store
            .revoke_push_subscription(&device.device_id, &created.id, now)
            .unwrap()
            .unwrap();
        assert!(revoked.1);
        assert!(revoked.0.disabled_at.is_some());
        assert!(store
            .list_push_subscriptions(&device.device_id)
            .unwrap()
            .is_empty());

        let persisted =
            fs::read_to_string(tmp.path().join(PUSH_SUBSCRIPTIONS_FILE))
                .unwrap();
        assert!(persisted.contains("opaque-token"));
        assert!(!tmp.path().read_dir().unwrap().any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".tmp")));
    }
}

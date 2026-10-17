use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use super::crypto::{
    constant_time_eq, generate_pairing_code, generate_pairing_id, id_hash,
    secret_hash,
};
use super::error::MobilePairingError as Error;
use super::fs::resolve_state_root;
#[cfg(unix)]
use super::fs::{open_pairing_dir, ProtectedDir};
use super::wire::{
    MobilePairingConsumeOutcome as Outcome, MobilePairingIssueResponseWire,
    MOBILE_PAIRING_TTL, MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
};

pub(crate) struct PairingStore {
    #[cfg(unix)]
    dir: ProtectedDir,
}

#[derive(Serialize, Deserialize)]
struct StoreFile {
    schema_version: u32,
    challenges: Vec<StoredChallenge>,
}

#[derive(Serialize, Deserialize)]
struct StoredChallenge {
    id_hash: String,
    secret_hash: String,
    expires_at: String,
    host_label: String,
}

impl std::fmt::Debug for StoredChallenge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StoredChallenge")
            .field("id_hash", &"<redacted>")
            .field("secret_hash", &"<redacted>")
            .field("expires_at", &self.expires_at)
            .field("host_label", &self.host_label)
            .finish()
    }
}

impl PairingStore {
    pub(crate) fn open(
        explicit_root: Option<&str>,
        create: bool,
    ) -> Result<Self, Error> {
        #[cfg(not(unix))]
        {
            let _ = (explicit_root, create);
            Err(Error::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            let state_root = resolve_state_root(explicit_root)?;
            let dir = open_pairing_dir(&state_root, create)?;
            Ok(Self { dir })
        }
    }

    pub(crate) fn issue(
        &self,
        host_label: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<MobilePairingIssueResponseWire, Error> {
        #[cfg(not(unix))]
        {
            let _ = (host_label, now);
            Err(Error::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            let host_label = normalize_host_label(host_label);
            let pairing_id = generate_pairing_id();
            let code = generate_pairing_code();
            let expires_at = now + MOBILE_PAIRING_TTL;
            let record = StoredChallenge {
                id_hash: id_hash(&pairing_id),
                secret_hash: secret_hash(&pairing_id, &code),
                expires_at: format_time(expires_at),
                host_label: host_label.clone(),
            };
            let _lock = self.dir.lock()?;
            let mut store = self.load_unlocked()?;
            prune_expired(&mut store, now);
            store.challenges.push(record);
            self.save_unlocked(&store)?;
            Ok(MobilePairingIssueResponseWire {
                schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
                pairing_id,
                code,
                expires_at: format_time(expires_at),
                host_label,
                host_fingerprint: None,
            })
        }
    }

    pub(crate) fn consume(
        &self,
        pairing_id: &str,
        code: &str,
        now: DateTime<Utc>,
    ) -> Result<Outcome, Error> {
        #[cfg(not(unix))]
        {
            let _ = (pairing_id, code, now);
            Err(Error::UnsupportedPlatform)
        }
        #[cfg(unix)]
        {
            let presented_id_hash = id_hash(pairing_id);
            let presented_secret = secret_hash(pairing_id, code);
            let _lock = self.dir.lock()?;
            let mut store = self.load_unlocked()?;
            let index = store
                .challenges
                .iter()
                .position(|challenge| challenge.id_hash == presented_id_hash);
            let Some(index) = index else {
                prune_expired(&mut store, now);
                if !store.challenges.is_empty() {
                    self.save_unlocked(&store)?;
                }
                return Ok(Outcome::Rejected);
            };
            let challenge = store.challenges.remove(index);
            prune_expired(&mut store, now);
            self.save_unlocked(&store)?;
            let expired = parse_time(&challenge.expires_at)
                .map(|expires_at| expires_at <= now)
                .unwrap_or(true);
            if expired {
                return Ok(Outcome::Expired);
            }
            if constant_time_eq(&challenge.secret_hash, &presented_secret) {
                Ok(Outcome::Accepted)
            } else {
                Ok(Outcome::Rejected)
            }
        }
    }

    #[cfg(unix)]
    fn load_unlocked(&self) -> Result<StoreFile, Error> {
        match self.dir.read_store()? {
            None => Ok(StoreFile {
                schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
                challenges: Vec::new(),
            }),
            Some(bytes) => {
                let store: StoreFile = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Corrupt)?;
                if store.schema_version != MOBILE_PAIRING_WIRE_SCHEMA_VERSION {
                    return Err(Error::Corrupt);
                }
                Ok(store)
            }
        }
    }

    #[cfg(unix)]
    fn save_unlocked(&self, store: &StoreFile) -> Result<(), Error> {
        let mut bytes =
            serde_json::to_vec_pretty(store).map_err(|_| Error::Unavailable)?;
        if !bytes.ends_with(b"\n") {
            bytes.push(b'\n');
        }
        self.dir.write_store(&bytes)
    }
}

fn prune_expired(store: &mut StoreFile, now: DateTime<Utc>) {
    store.challenges.retain(|challenge| {
        parse_time(&challenge.expires_at)
            .map(|expires_at| expires_at > now)
            .unwrap_or(false)
    });
}

fn normalize_host_label(host_label: Option<&str>) -> String {
    host_label
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
        .unwrap_or_else(|| "sase-host".to_string())
}

fn format_time(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn parse_time(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

use super::*;
use std::os::unix::fs::PermissionsExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct FederationCacheFile {
    schema_version: u32,
    entries: Vec<FederationCacheEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct FederationCacheEntry {
    host_id: String,
    key: String,
    saved_at_unix_ms: u64,
    last_used_unix_ms: u64,
    byte_size: usize,
    pub(super) payload: JsonValue,
}

#[derive(Debug)]
pub(super) struct FederationCache {
    entries: BTreeMap<String, FederationCacheEntry>,
    entry_limit: usize,
    byte_limit: usize,
    total_bytes: usize,
}

impl FederationCache {
    pub(super) fn load(
        path: PathBuf,
        entry_limit: usize,
        byte_limit: usize,
    ) -> Self {
        let empty = Self {
            entries: BTreeMap::new(),
            entry_limit,
            byte_limit,
            total_bytes: 0,
        };
        let Ok(bytes) = fs::read(path) else {
            return empty;
        };
        let Ok(file) = serde_json::from_slice::<FederationCacheFile>(&bytes)
        else {
            return empty;
        };
        if file.schema_version > FEDERATION_IPC_SCHEMA_VERSION {
            return empty;
        }
        let mut cache = empty;
        for entry in file.entries {
            if entry.byte_size <= byte_limit {
                cache.total_bytes =
                    cache.total_bytes.saturating_add(entry.byte_size);
                cache.entries.insert(
                    compound_cache_key(&entry.host_id, &entry.key),
                    entry,
                );
            }
        }
        cache.enforce_budget();
        cache
    }

    pub(super) fn store(
        &mut self,
        host_id: &str,
        key: &str,
        payload: JsonValue,
    ) -> Option<FederationCacheEntry> {
        let byte_size = serde_json::to_vec(&payload).ok()?.len();
        if byte_size > self.byte_limit {
            return None;
        }
        let now = unix_now_ms();
        let compound = compound_cache_key(host_id, key);
        if let Some(previous) = self.entries.remove(&compound) {
            self.total_bytes =
                self.total_bytes.saturating_sub(previous.byte_size);
        }
        let entry = FederationCacheEntry {
            host_id: host_id.to_string(),
            key: key.to_string(),
            saved_at_unix_ms: now,
            last_used_unix_ms: now,
            byte_size,
            payload,
        };
        self.total_bytes = self.total_bytes.saturating_add(byte_size);
        self.entries.insert(compound, entry.clone());
        self.enforce_budget();
        Some(entry)
    }

    pub(super) fn get(
        &mut self,
        host_id: &str,
        key: &str,
    ) -> Option<FederationCacheEntry> {
        let compound = compound_cache_key(host_id, key);
        let entry = self.entries.get_mut(&compound)?;
        entry.last_used_unix_ms = unix_now_ms();
        Some(entry.clone())
    }

    pub(super) fn persist(&self, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec(&FederationCacheFile {
            schema_version: FEDERATION_IPC_SCHEMA_VERSION,
            entries: self.entries.values().cloned().collect(),
        })?;
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp)?;
            file.set_permissions(fs::Permissions::from_mode(0o600))?;
            use std::io::Write;
            file.write_all(&bytes)?;
            file.sync_all()?;
        }
        fs::rename(tmp, path)
    }

    pub(super) fn enforce_budget(&mut self) {
        while self.entries.len() > self.entry_limit
            || self.total_bytes > self.byte_limit
        {
            let Some(key) = self
                .entries
                .iter()
                .min_by_key(|(key, entry)| {
                    (entry.last_used_unix_ms, (*key).clone())
                })
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            if let Some(entry) = self.entries.remove(&key) {
                self.total_bytes =
                    self.total_bytes.saturating_sub(entry.byte_size);
            }
        }
    }
}

pub(super) fn cache_path(sase_home: &Path) -> PathBuf {
    sase_home.join("fleet").join("worker_cache.json")
}

pub(super) fn compound_cache_key(host_id: &str, key: &str) -> String {
    format!("{host_id}\0{key}")
}

pub(super) fn entry_age(entry: Option<&FederationCacheEntry>) -> Option<f64> {
    entry.map(|entry| {
        unix_now_ms().saturating_sub(entry.saved_at_unix_ms) as f64 / 1000.0
    })
}

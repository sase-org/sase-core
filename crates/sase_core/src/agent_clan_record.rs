//! Durable per-clan attribute records (clan tribes and summaries).
//!
//! One JSON file per clan lives under `<sase_home>/agent_clans/`. Each
//! file maps artifact-directory generation names to the clan's recorded
//! `tribe`, `summary`, and `summary_script` attributes. Records are
//! written on declaration, generation, and edit, and captured just
//! before an artifact directory is deleted, so clan attributes survive
//! member kills, dismissals, relaunches, and full reloads. The scan and
//! index paths apply these records over member-derived clan context.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use crate::agent_scan::wire::{AgentClanContextWire, AgentMetaWire};
use crate::store_lock::{
    acquire_store_lock, holder_path_for, timeout_from_env, LockMode,
};

/// Wire schema version for per-clan record files.
pub const AGENT_CLAN_RECORD_SCHEMA_VERSION: u32 = 1;

/// Directory under `sase_home` holding per-clan record files.
pub const AGENT_CLAN_RECORDS_DIR_NAME: &str = "agent_clans";

/// Newest generations retained per clan. Generation keys are
/// artifact-directory timestamp names, so lexicographic order is
/// chronological.
pub const MAX_CLAN_GENERATIONS_PER_RECORD: usize = 8;

/// Launch-cap truncation for recorded summaries, in bytes.
pub const MAX_CLAN_SUMMARY_BYTES: usize = 32 * 1024;

/// Lock timeout env override; the default is about 2 s.
const LOCK_TIMEOUT_ENV: &str = "SASE_CLAN_RECORD_LOCK_TIMEOUT";
const LOCK_TIMEOUT_DEFAULT: Duration = Duration::from_secs(2);

/// In-process read cache bound; entries are validated by `(mtime, len)`.
const RECORD_CACHE_LIMIT: usize = 512;

/// How one recorded clan attribute value was produced.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ClanAttributeSourceWire {
    Declared,
    Script,
    Edited,
    Inherited,
    Propagated,
    #[default]
    Captured,
}

impl ClanAttributeSourceWire {
    /// Overwriting sources replace any existing attribute value.
    const fn overwrites(self) -> bool {
        match self {
            Self::Declared | Self::Script | Self::Edited | Self::Inherited => {
                true
            }
            Self::Propagated | Self::Captured => false,
        }
    }
}

/// One recorded value for a single clan attribute.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanAttributeRecordWire {
    /// `None` together with `source = edited` is an explicit unset
    /// tombstone.
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub source: ClanAttributeSourceWire,
    #[serde(default)]
    pub recorded_at: String,
    #[serde(default)]
    pub source_identity: Option<String>,
}

/// Recorded attributes for one clan generation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanGenerationRecordWire {
    #[serde(default)]
    pub first_recorded_at: String,
    #[serde(default)]
    pub tribe: Option<ClanAttributeRecordWire>,
    #[serde(default)]
    pub summary: Option<ClanAttributeRecordWire>,
    #[serde(default)]
    pub summary_script: Option<ClanAttributeRecordWire>,
}

/// Durable per-clan record file.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentClanRecordWire {
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub clan: String,
    #[serde(default)]
    pub latest_generation: Option<String>,
    #[serde(default)]
    pub generations: BTreeMap<String, ClanGenerationRecordWire>,
}

/// One attribute value supplied by a record update.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanAttributeUpdateWire {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(default)]
    pub source: ClanAttributeSourceWire,
    #[serde(default)]
    pub source_identity: Option<String>,
}

/// Attribute values recorded for one `(clan, generation)` key.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanRecordUpdateWire {
    #[serde(default)]
    pub clan: String,
    #[serde(default)]
    pub generation: String,
    #[serde(default)]
    pub tribe: Option<ClanAttributeUpdateWire>,
    #[serde(default)]
    pub summary: Option<ClanAttributeUpdateWire>,
    #[serde(default)]
    pub summary_script: Option<ClanAttributeUpdateWire>,
}

/// Outcome of merging one record update.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanRecordUpdateOutcomeWire {
    #[serde(default)]
    pub changed: bool,
    #[serde(default)]
    pub record: AgentClanRecordWire,
}

/// Remembered attributes seeding a new generation of a known clan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClanLaunchDefaultsWire {
    #[serde(default)]
    pub clan: String,
    #[serde(default)]
    pub tribe: Option<String>,
    #[serde(default)]
    pub tribe_generation: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub summary_generation: Option<String>,
    #[serde(default)]
    pub summary_script: Option<String>,
    #[serde(default)]
    pub summary_script_generation: Option<String>,
}

/// Errors from clan record I/O, validation, and locking.
#[derive(Debug, thiserror::Error)]
pub enum ClanRecordError {
    #[error("invalid clan name: {0}")]
    InvalidClan(String),
    #[error("invalid clan generation: {0}")]
    InvalidGeneration(String),
    #[error("clan record lock error: {0}")]
    Lock(String),
    #[error("clan record io error: {0}")]
    Io(String),
    #[error("clan record json error: {0}")]
    Json(String),
}

impl From<io::Error> for ClanRecordError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for ClanRecordError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

/// Directory holding per-clan record files for one `sase_home`.
pub fn clan_records_dir(sase_home: &Path) -> PathBuf {
    sase_home.join(AGENT_CLAN_RECORDS_DIR_NAME)
}

/// File holding one clan's record.
///
/// Every byte outside `[A-Za-z0-9_.-]` is percent-encoded, so the
/// result is always a single file name inside `records_dir`.
pub fn clan_record_path(
    records_dir: &Path,
    clan: &str,
) -> Result<PathBuf, ClanRecordError> {
    let clan = clan.trim();
    if clan.is_empty() {
        return Err(ClanRecordError::InvalidClan(
            "clan name is empty".to_string(),
        ));
    }
    if clan == "." || clan == ".." {
        return Err(ClanRecordError::InvalidClan(format!(
            "clan name is reserved: {clan:?}"
        )));
    }
    let mut encoded = String::with_capacity(clan.len());
    for byte in clan.as_bytes() {
        if byte.is_ascii_alphanumeric()
            || *byte == b'_'
            || *byte == b'.'
            || *byte == b'-'
        {
            encoded.push(char::from(*byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    let filename = format!("{encoded}.json");
    let path = records_dir.join(&filename);
    let escapes = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_none_or(|name| name != filename);
    if escapes {
        return Err(ClanRecordError::InvalidClan(format!(
            "clan name escapes the records directory: {clan:?}"
        )));
    }
    Ok(path)
}

#[derive(Debug, Clone)]
struct CachedRecord {
    mtime: Option<SystemTime>,
    len: u64,
    record: AgentClanRecordWire,
}

fn record_cache() -> &'static Mutex<HashMap<PathBuf, CachedRecord>> {
    static CACHE: OnceLock<Mutex<HashMap<PathBuf, CachedRecord>>> =
        OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_cache() -> std::sync::MutexGuard<'static, HashMap<PathBuf, CachedRecord>>
{
    match record_cache().lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn cache_lookup(
    path: &Path,
    mtime: Option<SystemTime>,
    len: u64,
) -> Option<AgentClanRecordWire> {
    lock_cache().get(path).and_then(|cached| {
        if cached.mtime == mtime && cached.len == len {
            Some(cached.record.clone())
        } else {
            None
        }
    })
}

fn cache_store(path: &Path, record: &AgentClanRecordWire) {
    let Ok(metadata) = fs::metadata(path) else {
        return;
    };
    let entry = CachedRecord {
        mtime: metadata.modified().ok(),
        len: metadata.len(),
        record: record.clone(),
    };
    let mut cache = lock_cache();
    if cache.len() >= RECORD_CACHE_LIMIT && !cache.contains_key(path) {
        if let Some(evict) = cache.keys().next().cloned() {
            cache.remove(&evict);
        }
    }
    cache.insert(path.to_path_buf(), entry);
}

fn cache_evict(path: &Path) {
    lock_cache().remove(path);
}

/// Load one clan's record without taking any lock.
///
/// A missing file returns `None`. A corrupt file returns an error;
/// scan/index callers treat that as absent and keep member-derived
/// values.
pub fn load_clan_record(
    records_dir: &Path,
    clan: &str,
) -> Result<Option<AgentClanRecordWire>, ClanRecordError> {
    let path = clan_record_path(records_dir, clan)?;
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(ClanRecordError::Io(error.to_string())),
    };
    let metadata = fs::metadata(&path).ok();
    let mtime = metadata.as_ref().and_then(|m| m.modified().ok());
    let len = bytes.len() as u64;
    if let Some(cached) = cache_lookup(&path, mtime, len) {
        return Ok(Some(cached));
    }
    let record: AgentClanRecordWire = match serde_json::from_slice(&bytes) {
        Ok(record) => record,
        Err(error) => {
            cache_evict(&path);
            return Err(ClanRecordError::Json(error.to_string()));
        }
    };
    cache_store(&path, &record);
    Ok(Some(record))
}

fn fresh_record(clan: &str) -> AgentClanRecordWire {
    AgentClanRecordWire {
        schema_version: AGENT_CLAN_RECORD_SCHEMA_VERSION,
        clan: clan.to_string(),
        latest_generation: None,
        generations: BTreeMap::new(),
    }
}

fn truncate_to_byte_limit(value: &mut String, limit: usize) {
    if value.len() <= limit {
        return;
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}

/// Merge one attribute update into its record slot.
///
/// Returns true when the slot changed. Empty values are no-ops and
/// never erase a recorded value.
fn merge_attribute(
    slot: &mut Option<ClanAttributeRecordWire>,
    update: Option<&ClanAttributeUpdateWire>,
    is_summary: bool,
    now: &str,
) -> bool {
    let Some(update) = update else {
        return false;
    };
    match &update.value {
        None => {
            // Only an edit may write an explicit unset tombstone.
            if update.source != ClanAttributeSourceWire::Edited {
                return false;
            }
            let identical = slot.as_ref().is_some_and(|existing| {
                existing.value.is_none()
                    && existing.source == update.source
                    && existing.source_identity == update.source_identity
            });
            if identical {
                return false;
            }
            *slot = Some(ClanAttributeRecordWire {
                value: None,
                source: update.source,
                recorded_at: now.to_string(),
                source_identity: update.source_identity.clone(),
            });
            true
        }
        Some(raw) => {
            let mut value = raw.trim().to_string();
            if value.is_empty() {
                return false;
            }
            if is_summary {
                truncate_to_byte_limit(&mut value, MAX_CLAN_SUMMARY_BYTES);
            }
            if !update.source.overwrites() {
                // Fill-only sources write only when the attribute is
                // absent: no value and no tombstone.
                if slot.is_some() {
                    return false;
                }
            } else if let Some(existing) = slot.as_ref() {
                let identical = existing.value.as_deref()
                    == Some(value.as_str())
                    && existing.source == update.source
                    && existing.source_identity == update.source_identity;
                if identical {
                    return false;
                }
            }
            *slot = Some(ClanAttributeRecordWire {
                value: Some(value),
                source: update.source,
                recorded_at: now.to_string(),
                source_identity: update.source_identity.clone(),
            });
            true
        }
    }
}

/// Merge one update into a clan's record file.
///
/// Normalization trims values; an empty string is a no-op. Fill-only
/// sources (`propagated`, `captured`) write only absent attributes,
/// and `captured` never writes `summary_script`. A merge that changes
/// nothing returns `changed: false` and does not touch the file.
pub fn record_clan_attributes(
    records_dir: &Path,
    update: ClanRecordUpdateWire,
) -> Result<ClanRecordUpdateOutcomeWire, ClanRecordError> {
    let clan = update.clan.trim().to_string();
    if clan.is_empty() {
        return Err(ClanRecordError::InvalidClan(
            "clan name is empty".to_string(),
        ));
    }
    let generation = update.generation.trim().to_string();
    if generation.is_empty() {
        return Err(ClanRecordError::InvalidGeneration(
            "generation is empty".to_string(),
        ));
    }
    fs::create_dir_all(records_dir)
        .map_err(|error| ClanRecordError::Io(error.to_string()))?;
    let path = clan_record_path(records_dir, &clan)?;
    let mut lock_name = path.as_os_str().to_owned();
    lock_name.push(".lock");
    let lock_path = PathBuf::from(lock_name);
    let lock = acquire_store_lock(
        &lock_path,
        &holder_path_for(&lock_path),
        LockMode::Exclusive,
        timeout_from_env(LOCK_TIMEOUT_ENV, LOCK_TIMEOUT_DEFAULT),
        "record_clan_attributes",
    )
    .map_err(|error| ClanRecordError::Lock(error.to_string()))?;
    let outcome = record_locked(&path, &clan, &generation, &update);
    let unlock = lock.release();
    match (outcome, unlock) {
        (Ok(outcome), Ok(())) => Ok(outcome),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(ClanRecordError::Io(error.to_string())),
    }
}

fn record_locked(
    path: &Path,
    clan: &str,
    generation: &str,
    update: &ClanRecordUpdateWire,
) -> Result<ClanRecordUpdateOutcomeWire, ClanRecordError> {
    let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let mut record = match fs::read(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fresh_record(clan)
        }
        Err(error) => return Err(ClanRecordError::Io(error.to_string())),
        // A corrupt file heals on the next recording write.
        Ok(bytes) => serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| fresh_record(clan)),
    };
    let entry_exists = record.generations.contains_key(generation);
    let mut staged = record
        .generations
        .get(generation)
        .cloned()
        .unwrap_or_default();
    let mut attrs_changed =
        merge_attribute(&mut staged.tribe, update.tribe.as_ref(), false, &now);
    attrs_changed |= merge_attribute(
        &mut staged.summary,
        update.summary.as_ref(),
        true,
        &now,
    );
    // Captured artifact copies never carry the launch script.
    let script_update = update
        .summary_script
        .as_ref()
        .filter(|attr| attr.source != ClanAttributeSourceWire::Captured);
    attrs_changed |=
        merge_attribute(&mut staged.summary_script, script_update, false, &now);

    let mut changed = attrs_changed;
    if attrs_changed {
        if !entry_exists || staged.first_recorded_at.trim().is_empty() {
            staged.first_recorded_at = now.clone();
        }
        record.generations.insert(generation.to_string(), staged);
        let latest_wins = record
            .latest_generation
            .as_deref()
            .is_none_or(|latest| generation >= latest);
        if latest_wins
            && record.latest_generation.as_deref() != Some(generation)
        {
            record.latest_generation = Some(generation.to_string());
            changed = true;
        }
        while record.generations.len() > MAX_CLAN_GENERATIONS_PER_RECORD {
            record.generations.pop_first();
            changed = true;
        }
    } else if entry_exists {
        // Heal a stale latest pointer without touching attributes.
        let latest_wins = record
            .latest_generation
            .as_deref()
            .is_none_or(|latest| generation > latest);
        if latest_wins
            && record.latest_generation.as_deref() != Some(generation)
        {
            record.latest_generation = Some(generation.to_string());
            changed = true;
        }
    }
    if !changed {
        return Ok(ClanRecordUpdateOutcomeWire {
            changed: false,
            record,
        });
    }
    record.schema_version = AGENT_CLAN_RECORD_SCHEMA_VERSION;
    record.clan = clan.to_string();
    write_record_atomic(path, &record)?;
    cache_store(path, &record);
    Ok(ClanRecordUpdateOutcomeWire {
        changed: true,
        record,
    })
}

fn write_record_atomic(
    path: &Path,
    record: &AgentClanRecordWire,
) -> Result<(), ClanRecordError> {
    let parent = path.parent().ok_or_else(|| {
        ClanRecordError::Io("clan record path has no parent".to_string())
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| ClanRecordError::Io(error.to_string()))?;
    let mut staged = NamedTempFile::new_in(parent)?;
    serde_json::to_writer_pretty(&mut staged, record)?;
    staged.write_all(b"\n")?;
    staged.flush()?;
    staged.as_file().sync_all()?;
    staged
        .persist(path)
        .map_err(|error| ClanRecordError::Io(error.error.to_string()))?;
    Ok(())
}

fn attribute_update(
    value: Option<&str>,
    source: ClanAttributeSourceWire,
    source_identity: &str,
) -> Option<ClanAttributeUpdateWire> {
    value.map(|value| ClanAttributeUpdateWire {
        value: Some(value.to_string()),
        source,
        source_identity: Some(source_identity.to_string()),
    })
}

/// Copy a dying artifact directory's clan attributes into the record.
///
/// Reads `agent_meta.json` under `artifacts_dir` and fills `tribe`
/// and `summary` as `captured`. Does nothing when the meta file is
/// missing, corrupt, or carries no clan key, and returns `None` when
/// nothing was captured.
pub fn capture_clan_record_from_artifacts(
    records_dir: &Path,
    artifacts_dir: &Path,
) -> Result<Option<AgentClanRecordWire>, ClanRecordError> {
    let bytes = match fs::read(artifacts_dir.join("agent_meta.json")) {
        Ok(bytes) => bytes,
        Err(_) => return Ok(None),
    };
    let meta: AgentMetaWire = match serde_json::from_slice(&bytes) {
        Ok(meta) => meta,
        Err(_) => return Ok(None),
    };
    let Some((clan, generation)) =
        crate::agent_scan::context::clan_key_from_meta(&meta)
    else {
        return Ok(None);
    };
    let generation = match generation {
        Some(generation) if !generation.trim().is_empty() => generation,
        _ => return Ok(None),
    };
    let tribe = meta
        .clan_tribe
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let summary = meta
        .clan_summary
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if tribe.is_none() && summary.is_none() {
        return Ok(None);
    }
    let identity = artifacts_dir.to_string_lossy().into_owned();
    let update = ClanRecordUpdateWire {
        clan,
        generation,
        tribe: attribute_update(
            tribe,
            ClanAttributeSourceWire::Captured,
            &identity,
        ),
        summary: attribute_update(
            summary,
            ClanAttributeSourceWire::Captured,
            &identity,
        ),
        summary_script: None,
    };
    let outcome = record_clan_attributes(records_dir, update)?;
    Ok(Some(outcome.record))
}

/// Remembered attributes for a new generation of a known clan.
///
/// Each attribute resolves from the newest generation other than
/// `exclude_generation` that carries it. A tombstone yields `None`
/// and stops the search for that attribute. Missing or corrupt
/// records resolve to empty defaults.
pub fn resolve_clan_launch_defaults(
    records_dir: &Path,
    clan: &str,
    exclude_generation: Option<&str>,
) -> Result<ClanLaunchDefaultsWire, ClanRecordError> {
    let clan = clan.trim();
    if clan.is_empty() {
        return Err(ClanRecordError::InvalidClan(
            "clan name is empty".to_string(),
        ));
    }
    let excluded = exclude_generation.map(str::trim).filter(|g| !g.is_empty());
    let mut defaults = ClanLaunchDefaultsWire {
        clan: clan.to_string(),
        ..Default::default()
    };
    let record = load_clan_record(records_dir, clan).unwrap_or_default();
    let Some(record) = record else {
        return Ok(defaults);
    };
    let mut generations: Vec<&String> = record
        .generations
        .keys()
        .filter(|generation| {
            excluded.is_none_or(|excluded| generation.as_str() != excluded)
        })
        .collect();
    generations.sort();
    generations.reverse();
    let pick = |select: fn(
        &ClanGenerationRecordWire,
    ) -> &Option<ClanAttributeRecordWire>| {
        for generation in &generations {
            if let Some(attribute) =
                select(&record.generations[generation.as_str()])
            {
                match &attribute.value {
                    Some(value) => {
                        return (
                            Some(value.clone()),
                            Some((*generation).clone()),
                        );
                    }
                    // A tombstone blocks older generations.
                    None => return (None, None),
                }
            }
        }
        (None, None)
    };
    let (tribe, tribe_generation) = pick(|entry| &entry.tribe);
    defaults.tribe = tribe;
    defaults.tribe_generation = tribe_generation;
    let (summary, summary_generation) = pick(|entry| &entry.summary);
    defaults.summary = summary;
    defaults.summary_generation = summary_generation;
    let (script, script_generation) = pick(|entry| &entry.summary_script);
    defaults.summary_script = script;
    defaults.summary_script_generation = script_generation;
    Ok(defaults)
}

/// Apply durable records over member-derived clan context.
///
/// For each context with a non-empty generation whose record holds
/// that generation, recorded `tribe`/`summary` values (including
/// tombstones) win over member-derived ones. Failures are soft: a
/// missing or corrupt record keeps the member-derived values.
pub fn apply_clan_records_to_context(
    records_dir: &Path,
    contexts: &mut [AgentClanContextWire],
) {
    let mut loaded: HashMap<String, Option<AgentClanRecordWire>> =
        HashMap::new();
    for context in contexts.iter_mut() {
        let generation = context
            .agent_clan_generation
            .as_deref()
            .map(str::trim)
            .filter(|generation| !generation.is_empty());
        let Some(generation) = generation else {
            continue;
        };
        let clan = context.agent_clan.trim();
        if clan.is_empty() {
            continue;
        }
        let record = loaded.entry(clan.to_string()).or_insert_with(|| {
            load_clan_record(records_dir, clan).unwrap_or(None)
        });
        let Some(record) = record else {
            continue;
        };
        let Some(entry) = record.generations.get(generation) else {
            continue;
        };
        if let Some(tribe) = &entry.tribe {
            context.clan_tribe = tribe.value.clone();
            context.clan_tribe_source_identity = tribe.source_identity.clone();
            context.clan_tribe_source_launch_timestamp = None;
        }
        if let Some(summary) = &entry.summary {
            context.clan_summary = summary.value.clone();
            context.clan_summary_source_identity =
                summary.source_identity.clone();
            context.clan_summary_source_launch_timestamp = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn update(
        clan: &str,
        generation: &str,
        tribe: Option<(&str, ClanAttributeSourceWire)>,
        summary: Option<(&str, ClanAttributeSourceWire)>,
    ) -> ClanRecordUpdateWire {
        let attr =
            |v: (&str, ClanAttributeSourceWire)| ClanAttributeUpdateWire {
                value: Some(v.0.to_string()),
                source: v.1,
                source_identity: Some("test".to_string()),
            };
        ClanRecordUpdateWire {
            clan: clan.to_string(),
            generation: generation.to_string(),
            tribe: tribe.map(attr),
            summary: summary.map(attr),
            summary_script: None,
        }
    }

    fn recorded_tribe(records_dir: &Path, clan: &str) -> Option<String> {
        load_clan_record(records_dir, clan)
            .unwrap()
            .unwrap()
            .generations["g1"]
            .tribe
            .clone()
            .and_then(|attr| attr.value)
    }

    #[test]
    fn overwrite_sources_replace_and_fill_only_sources_do_not() {
        use ClanAttributeSourceWire as Source;
        // Each (first, second, expected) tribe sequence.
        let cases = [
            (Source::Declared, Source::Declared, "second"),
            (Source::Declared, Source::Script, "second"),
            (Source::Declared, Source::Edited, "second"),
            (Source::Declared, Source::Inherited, "second"),
            (Source::Declared, Source::Propagated, "first"),
            (Source::Declared, Source::Captured, "first"),
            (Source::Propagated, Source::Declared, "second"),
            (Source::Captured, Source::Edited, "second"),
            (Source::Propagated, Source::Propagated, "first"),
        ];
        for (index, (first, second, expected)) in cases.into_iter().enumerate()
        {
            let tmp = tempdir().unwrap();
            let clan = format!("clan-{index}");
            record_clan_attributes(
                tmp.path(),
                update(&clan, "g1", Some(("first", first)), None),
            )
            .unwrap();
            record_clan_attributes(
                tmp.path(),
                update(&clan, "g1", Some(("second", second)), None),
            )
            .unwrap();
            assert_eq!(
                recorded_tribe(tmp.path(), &clan).as_deref(),
                Some(expected),
                "case {index}: {first:?} then {second:?}"
            );
        }
    }

    #[test]
    fn only_edited_writes_a_tombstone() {
        let tmp = tempdir().unwrap();
        // Non-edited unset attempts are no-ops.
        let outcome = record_clan_attributes(
            tmp.path(),
            ClanRecordUpdateWire {
                clan: "ghost".to_string(),
                generation: "g1".to_string(),
                tribe: Some(ClanAttributeUpdateWire {
                    value: None,
                    source: ClanAttributeSourceWire::Propagated,
                    source_identity: None,
                }),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!outcome.changed);
        assert!(load_clan_record(tmp.path(), "ghost").unwrap().is_none());

        // An edit writes the tombstone and clears the value.
        record_clan_attributes(
            tmp.path(),
            update(
                "tribed",
                "g1",
                Some(("alpha", ClanAttributeSourceWire::Declared)),
                None,
            ),
        )
        .unwrap();
        let outcome = record_clan_attributes(
            tmp.path(),
            ClanRecordUpdateWire {
                clan: "tribed".to_string(),
                generation: "g1".to_string(),
                tribe: Some(ClanAttributeUpdateWire {
                    value: None,
                    source: ClanAttributeSourceWire::Edited,
                    source_identity: Some("tui".to_string()),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(outcome.changed);
        let entry = &outcome.record.generations["g1"].tribe.as_ref().unwrap();
        assert_eq!(entry.value, None);
        assert_eq!(entry.source, ClanAttributeSourceWire::Edited);
    }

    #[test]
    fn empty_summary_is_a_no_op_and_long_summaries_truncate() {
        let tmp = tempdir().unwrap();
        let outcome = record_clan_attributes(
            tmp.path(),
            update(
                "blank",
                "g1",
                None,
                Some(("   ", ClanAttributeSourceWire::Declared)),
            ),
        )
        .unwrap();
        assert!(!outcome.changed);
        assert!(load_clan_record(tmp.path(), "blank").unwrap().is_none());

        let long = "s".repeat(MAX_CLAN_SUMMARY_BYTES + 100);
        let outcome = record_clan_attributes(
            tmp.path(),
            update(
                "long",
                "g1",
                None,
                Some((&long, ClanAttributeSourceWire::Script)),
            ),
        )
        .unwrap();
        assert!(outcome.changed);
        let stored = outcome.record.generations["g1"].summary.as_ref().unwrap();
        let value = stored.value.as_deref().unwrap();
        assert_eq!(value.len(), MAX_CLAN_SUMMARY_BYTES);
        assert!(long.starts_with(value));
    }

    #[test]
    fn generations_keep_only_the_newest_eight() {
        let tmp = tempdir().unwrap();
        for n in 1..=9 {
            let generation = format!("2026090100000{n}");
            let outcome = record_clan_attributes(
                tmp.path(),
                update(
                    "rolling",
                    &generation,
                    Some(("t", ClanAttributeSourceWire::Declared)),
                    None,
                ),
            )
            .unwrap();
            assert!(outcome.changed);
        }
        let record = load_clan_record(tmp.path(), "rolling").unwrap().unwrap();
        assert_eq!(record.generations.len(), 8);
        assert!(!record.generations.contains_key("20260901000001"));
        assert!(record.generations.contains_key("20260901000009"));
        assert_eq!(record.latest_generation.as_deref(), Some("20260901000009"));
    }

    #[test]
    fn identical_rewrites_are_no_ops_and_do_not_touch_the_file() {
        let tmp = tempdir().unwrap();
        let first = record_clan_attributes(
            tmp.path(),
            update(
                "steady",
                "g1",
                Some(("t", ClanAttributeSourceWire::Declared)),
                None,
            ),
        )
        .unwrap();
        assert!(first.changed);
        let mtime =
            fs::metadata(clan_record_path(tmp.path(), "steady").unwrap())
                .unwrap()
                .modified()
                .unwrap();
        let second = record_clan_attributes(
            tmp.path(),
            update(
                "steady",
                "g1",
                Some(("t", ClanAttributeSourceWire::Declared)),
                None,
            ),
        )
        .unwrap();
        assert!(!second.changed);
        let again =
            fs::metadata(clan_record_path(tmp.path(), "steady").unwrap())
                .unwrap()
                .modified()
                .unwrap();
        assert_eq!(mtime, again);
    }

    #[test]
    fn record_paths_encode_and_reject() {
        let root = PathBuf::from("/records");
        assert_eq!(
            clan_record_path(&root, "plain-clan_1.2").unwrap(),
            root.join("plain-clan_1.2.json")
        );
        assert_eq!(
            clan_record_path(&root, "a/b c").unwrap(),
            root.join("a%2Fb%20c.json")
        );
        for bad in ["", "   ", ".", ".."] {
            assert!(
                clan_record_path(&root, bad).is_err(),
                "{bad:?} must be rejected"
            );
        }
        assert!(clan_record_path(&root, "../escape")
            .unwrap()
            .starts_with(&root));
    }

    #[test]
    fn concurrent_writers_keep_both_attributes() {
        let tmp = tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..10 {
                    record_clan_attributes(
                        &dir,
                        update(
                            "raced",
                            "g1",
                            Some(("tribe", ClanAttributeSourceWire::Declared)),
                            None,
                        ),
                    )
                    .unwrap();
                }
            });
            scope.spawn(|| {
                for _ in 0..10 {
                    record_clan_attributes(
                        &dir,
                        update(
                            "raced",
                            "g1",
                            None,
                            Some(("summary", ClanAttributeSourceWire::Script)),
                        ),
                    )
                    .unwrap();
                }
            });
        });
        let record = load_clan_record(tmp.path(), "raced").unwrap().unwrap();
        let entry = &record.generations["g1"];
        assert_eq!(
            entry.tribe.as_ref().and_then(|a| a.value.as_deref()),
            Some("tribe")
        );
        assert_eq!(
            entry.summary.as_ref().and_then(|a| a.value.as_deref()),
            Some("summary")
        );
    }

    #[test]
    fn corrupt_files_error_and_external_rewrites_invalidate_cache() {
        let tmp = tempdir().unwrap();
        record_clan_attributes(
            tmp.path(),
            update(
                "cached",
                "g1",
                Some(("t", ClanAttributeSourceWire::Declared)),
                None,
            ),
        )
        .unwrap();
        assert!(load_clan_record(tmp.path(), "cached").unwrap().is_some());
        // An external rewrite with a different length invalidates the
        // (mtime, len) cache entry.
        let path = clan_record_path(tmp.path(), "cached").unwrap();
        record_clan_attributes(
            tmp.path(),
            update(
                "cached",
                "g1",
                Some((
                    "a-much-longer-tribe-value",
                    ClanAttributeSourceWire::Edited,
                )),
                None,
            ),
        )
        .unwrap();
        assert_eq!(
            recorded_tribe(tmp.path(), "cached").as_deref(),
            Some("a-much-longer-tribe-value")
        );
        fs::write(&path, b"{ not json").unwrap();
        assert!(load_clan_record(tmp.path(), "cached").is_err());
    }

    #[test]
    fn capture_fills_but_never_overrides_an_edit() {
        let tmp = tempdir().unwrap();
        let artifacts = tmp
            .path()
            .join("projects/proj/artifacts/ace-run/20260901000000");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("agent_meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": "declarer",
                "agent_clan": "epic-clan",
                "agent_clan_generation": "20260901000000",
                "clan_tribe": "epic",
                "clan_summary": "Epic work"
            }))
            .unwrap(),
        )
        .unwrap();
        let captured =
            capture_clan_record_from_artifacts(tmp.path(), &artifacts)
                .unwrap()
                .unwrap();
        let entry = &captured.generations["20260901000000"];
        assert_eq!(
            entry.tribe.as_ref().unwrap().source,
            ClanAttributeSourceWire::Captured
        );
        assert_eq!(
            entry.summary.as_ref().and_then(|a| a.value.as_deref()),
            Some("Epic work")
        );
        // Captured copies never carry the launch script.
        assert!(entry.summary_script.is_none());

        // An edit wins; a later capture does not override it.
        record_clan_attributes(
            tmp.path(),
            ClanRecordUpdateWire {
                clan: "epic-clan".to_string(),
                generation: "20260901000000".to_string(),
                tribe: Some(ClanAttributeUpdateWire {
                    value: Some("custom".to_string()),
                    source: ClanAttributeSourceWire::Edited,
                    source_identity: Some("tui".to_string()),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        capture_clan_record_from_artifacts(tmp.path(), &artifacts).unwrap();
        let record =
            load_clan_record(tmp.path(), "epic-clan").unwrap().unwrap();
        assert_eq!(
            record.generations["20260901000000"]
                .tribe
                .as_ref()
                .and_then(|a| a.value.as_deref()),
            Some("custom")
        );
    }

    #[test]
    fn capture_honors_the_parallel_family_fallback() {
        let tmp = tempdir().unwrap();
        let artifacts = tmp
            .path()
            .join("projects/proj/artifacts/ace-run/20260902000000");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(
            artifacts.join("agent_meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": "worker",
                "agent_family": "fam",
                "agent_family_parallel": true,
                "agent_clan_generation": "20260902000000",
                "clan_summary": "Family work"
            }))
            .unwrap(),
        )
        .unwrap();
        let captured =
            capture_clan_record_from_artifacts(tmp.path(), &artifacts)
                .unwrap()
                .unwrap();
        assert_eq!(captured.clan, "fam");
        assert!(captured.generations.contains_key("20260902000000"));
    }

    #[test]
    fn capture_ignores_non_clan_directories() {
        let tmp = tempdir().unwrap();
        let artifacts = tmp.path().join("lonely");
        fs::create_dir_all(&artifacts).unwrap();
        fs::write(artifacts.join("agent_meta.json"), br#"{"name": "solo"}"#)
            .unwrap();
        assert!(capture_clan_record_from_artifacts(tmp.path(), &artifacts)
            .unwrap()
            .is_none());
        assert!(capture_clan_record_from_artifacts(
            tmp.path(),
            &tmp.path().join("missing")
        )
        .unwrap()
        .is_none());
    }

    #[test]
    fn launch_defaults_resolve_per_attribute_and_honor_tombstones() {
        let tmp = tempdir().unwrap();
        record_clan_attributes(
            tmp.path(),
            update(
                "remembered",
                "g1",
                Some(("old-tribe", ClanAttributeSourceWire::Declared)),
                Some(("old-summary", ClanAttributeSourceWire::Script)),
            ),
        )
        .unwrap();
        record_clan_attributes(
            tmp.path(),
            ClanRecordUpdateWire {
                clan: "remembered".to_string(),
                generation: "g2".to_string(),
                tribe: Some(ClanAttributeUpdateWire {
                    value: Some("new-tribe".to_string()),
                    source: ClanAttributeSourceWire::Edited,
                    source_identity: Some("tui".to_string()),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let defaults =
            resolve_clan_launch_defaults(tmp.path(), "remembered", Some("g3"))
                .unwrap();
        assert_eq!(defaults.tribe.as_deref(), Some("new-tribe"));
        assert_eq!(defaults.tribe_generation.as_deref(), Some("g2"));
        assert_eq!(defaults.summary.as_deref(), Some("old-summary"));
        assert_eq!(defaults.summary_generation.as_deref(), Some("g1"));

        // The excluded generation is skipped.
        let defaults =
            resolve_clan_launch_defaults(tmp.path(), "remembered", Some("g2"))
                .unwrap();
        assert_eq!(defaults.tribe.as_deref(), Some("old-tribe"));
        assert_eq!(defaults.tribe_generation.as_deref(), Some("g1"));

        // A tombstone stops the search: no inherited tribe.
        record_clan_attributes(
            tmp.path(),
            ClanRecordUpdateWire {
                clan: "remembered".to_string(),
                generation: "g2".to_string(),
                tribe: Some(ClanAttributeUpdateWire {
                    value: None,
                    source: ClanAttributeSourceWire::Edited,
                    source_identity: Some("tui".to_string()),
                }),
                ..Default::default()
            },
        )
        .unwrap();
        let defaults =
            resolve_clan_launch_defaults(tmp.path(), "remembered", Some("g9"))
                .unwrap();
        assert_eq!(defaults.tribe, None);
        assert_eq!(defaults.tribe_generation, None);
        assert_eq!(defaults.summary.as_deref(), Some("old-summary"));
    }

    #[test]
    fn overlay_applies_matching_generations_only() {
        let tmp = tempdir().unwrap();
        record_clan_attributes(
            tmp.path(),
            update(
                "overlaid",
                "g1",
                Some(("recorded", ClanAttributeSourceWire::Declared)),
                Some(("recorded summary", ClanAttributeSourceWire::Script)),
            ),
        )
        .unwrap();
        let mut contexts = vec![
            AgentClanContextWire {
                agent_clan: "overlaid".to_string(),
                agent_clan_generation: Some("g1".to_string()),
                clan_tribe: Some("epic".to_string()),
                clan_summary: Some("member summary".to_string()),
                clan_tribe_source_launch_timestamp: Some(
                    "20260901000000".to_string(),
                ),
                clan_tribe_source_identity: Some("member-dir".to_string()),
                clan_summary_source_launch_timestamp: Some(
                    "20260901000000".to_string(),
                ),
                clan_summary_source_identity: Some("member-dir".to_string()),
            },
            AgentClanContextWire {
                agent_clan: "overlaid".to_string(),
                agent_clan_generation: Some("g2".to_string()),
                clan_tribe: Some("epic".to_string()),
                ..Default::default()
            },
            AgentClanContextWire {
                agent_clan: "overlaid".to_string(),
                agent_clan_generation: None,
                clan_tribe: Some("epic".to_string()),
                ..Default::default()
            },
        ];
        apply_clan_records_to_context(tmp.path(), &mut contexts);
        assert_eq!(contexts[0].clan_tribe.as_deref(), Some("recorded"));
        assert_eq!(
            contexts[0].clan_summary.as_deref(),
            Some("recorded summary")
        );
        assert_eq!(contexts[0].clan_tribe_source_launch_timestamp, None);
        assert_eq!(
            contexts[0].clan_tribe_source_identity.as_deref(),
            Some("test")
        );
        // Another generation and legacy `None` generations are untouched.
        assert_eq!(contexts[1].clan_tribe.as_deref(), Some("epic"));
        assert_eq!(contexts[2].clan_tribe.as_deref(), Some("epic"));
    }

    #[test]
    fn scan_overlay_keeps_recorded_values_after_declarer_deleted() {
        use crate::agent_scan::{
            scan_agent_artifact_dirs, scan_agent_artifacts,
            AgentArtifactScanOptionsWire,
        };
        let tmp = tempdir().unwrap();
        let projects = tmp.path().join("projects");
        let declarer = projects.join("proj/artifacts/ace-run/20260901000000");
        let joiner = projects.join("proj/artifacts/ace-run/20260901000001");
        for dir in [&declarer, &joiner] {
            fs::create_dir_all(dir).unwrap();
        }
        fs::write(
            declarer.join("agent_meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": "declarer",
                "agent_clan": "overlay-clan",
                "agent_clan_generation": "20260901000000",
                "clan_tribe": "epic",
                "clan_summary": "Member summary"
            }))
            .unwrap(),
        )
        .unwrap();
        fs::write(
            joiner.join("agent_meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "name": "joiner",
                "agent_clan": "overlay-clan",
                "agent_clan_generation": "20260901000000"
            }))
            .unwrap(),
        )
        .unwrap();
        let records_dir = tmp.path().join("agent_clans");
        record_clan_attributes(
            &records_dir,
            update(
                "overlay-clan",
                "20260901000000",
                Some(("epic", ClanAttributeSourceWire::Declared)),
                Some(("Recorded summary", ClanAttributeSourceWire::Script)),
            ),
        )
        .unwrap();
        // The declarer's artifact directory is gone; the surviving
        // joiner still keys the generation.
        fs::remove_dir_all(&declarer).unwrap();
        let options = AgentArtifactScanOptionsWire {
            clan_records_dir: Some(records_dir.to_string_lossy().into_owned()),
            ..Default::default()
        };
        for snapshot in [
            scan_agent_artifacts(&projects, options.clone()),
            scan_agent_artifact_dirs(&projects, &[joiner], options.clone()),
        ] {
            let context = snapshot
                .clan_context
                .iter()
                .find(|context| context.agent_clan == "overlay-clan")
                .unwrap();
            assert_eq!(
                context.clan_tribe.as_deref(),
                Some("epic"),
                "recorded tribe survives the deleted declarer"
            );
            assert_eq!(
                context.clan_summary.as_deref(),
                Some("Recorded summary")
            );
            assert_eq!(context.clan_tribe_source_launch_timestamp, None);
        }
        // Without the records dir the surviving joiner resolves blank.
        let bare = scan_agent_artifacts(
            &projects,
            AgentArtifactScanOptionsWire::default(),
        );
        let context = bare
            .clan_context
            .iter()
            .find(|context| context.agent_clan == "overlay-clan")
            .unwrap();
        assert_eq!(context.clan_summary, None);
    }

    #[test]
    fn corrupt_records_leave_member_values_in_place() {
        let tmp = tempdir().unwrap();
        let path = clan_record_path(tmp.path(), "flaky").unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"{ corrupt").unwrap();
        let mut contexts = vec![AgentClanContextWire {
            agent_clan: "flaky".to_string(),
            agent_clan_generation: Some("g1".to_string()),
            clan_tribe: Some("member".to_string()),
            ..Default::default()
        }];
        apply_clan_records_to_context(tmp.path(), &mut contexts);
        assert_eq!(contexts[0].clan_tribe.as_deref(), Some("member"));
    }
}

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use super::mobile::{
    pending_action_identity, MobileActionKindWire, MobileActionStateWire,
    PendingActionIdentityWire,
};
use super::wire::NotificationWire;

pub const PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION: u32 = 3;
pub const DEFAULT_PENDING_ACTION_PREFIX_LEN: usize = 8;
pub const DEFAULT_PENDING_ACTION_STALE_SECONDS: f64 = 24.0 * 60.0 * 60.0;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionStoreWire {
    pub schema_version: u32,
    pub actions: BTreeMap<String, PendingActionWire>,
}

impl Default for PendingActionStoreWire {
    fn default() -> Self {
        Self {
            schema_version: PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
            actions: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionWire {
    pub schema_version: u32,
    pub prefix: String,
    pub notification_id: String,
    pub action_kind: MobileActionKindWire,
    pub action: String,
    #[serde(default)]
    pub action_data: BTreeMap<String, String>,
    #[serde(default)]
    pub files: Vec<String>,
    pub created_at_unix: f64,
    pub updated_at_unix: f64,
    pub stale_deadline_unix: f64,
    #[serde(default)]
    pub transports: Vec<PendingActionTransportWire>,
    pub state: MobileActionStateWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handled_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handled_at_unix: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handled_action: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionTransportWire {
    pub transport: String,
    #[serde(default)]
    pub record: BTreeMap<String, JsonValue>,
}

pub fn pending_action_store_path(sase_home: &Path) -> PathBuf {
    sase_home.join("pending_actions").join("actions.json")
}

pub fn legacy_telegram_pending_actions_path(sase_home: &Path) -> PathBuf {
    sase_home.join("telegram").join("pending_actions.json")
}

pub fn pending_action_from_notification(
    notification: &NotificationWire,
    now_unix: f64,
) -> Option<PendingActionWire> {
    let action = notification.action.as_deref()?;
    let action_kind =
        MobileActionKindWire::from_notification_action(Some(action));
    if !action_kind.is_gate() {
        return None;
    }
    let identity = pending_action_identity(
        &notification.id,
        DEFAULT_PENDING_ACTION_PREFIX_LEN,
    );
    Some(PendingActionWire {
        schema_version: PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
        prefix: identity.prefix,
        notification_id: notification.id.clone(),
        action_kind,
        action: action.to_string(),
        action_data: notification.action_data.clone().into_iter().collect(),
        files: notification.files.clone(),
        created_at_unix: now_unix,
        updated_at_unix: now_unix,
        stale_deadline_unix: now_unix + DEFAULT_PENDING_ACTION_STALE_SECONDS,
        transports: vec![PendingActionTransportWire {
            transport: "notification_store".to_string(),
            record: BTreeMap::new(),
        }],
        state: MobileActionStateWire::Available,
        handled_source: None,
        handled_at_unix: None,
        handled_action: None,
    })
}

pub fn register_pending_action(
    path: &Path,
    action: &PendingActionWire,
) -> Result<PendingActionStoreWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = (|| {
        let mut store = read_pending_action_store_unlocked(path)?;
        let mut next = action.clone();
        if let Some(existing) = store.actions.get(&action.prefix) {
            next.created_at_unix = existing.created_at_unix;
            next.stale_deadline_unix = existing.stale_deadline_unix;
            next.state = existing.state;
            next.handled_source = existing.handled_source.clone();
            next.handled_at_unix = existing.handled_at_unix;
            next.handled_action = existing.handled_action.clone();
            for transport in &existing.transports {
                if !next
                    .transports
                    .iter()
                    .any(|item| item.transport == transport.transport)
                {
                    next.transports.push(transport.clone());
                }
            }
        }
        store.actions.insert(action.prefix.clone(), next);
        write_pending_action_store_unlocked(path, &store)?;
        Ok(store)
    })();
    unlock(lock)?;
    result
}

#[allow(clippy::incompatible_msrv)]
pub fn read_pending_action_store(
    path: &Path,
    legacy_telegram_path: Option<&Path>,
) -> Result<PendingActionStoreWire, String> {
    let lock = open_lock_file(path)?;
    lock.lock_shared().map_err(|e| e.to_string())?;
    let result = read_pending_action_store_unlocked(path);
    unlock(lock)?;
    let mut store = result?;
    if let Some(legacy_path) = legacy_telegram_path {
        merge_legacy_telegram_pending_actions(&mut store, legacy_path)?;
    }
    Ok(store)
}

pub fn cleanup_stale_pending_actions(
    path: &Path,
    now_unix: f64,
) -> Result<Vec<String>, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = (|| {
        let mut store = read_pending_action_store_unlocked(path)?;
        let stale: Vec<String> = store
            .actions
            .iter()
            .filter(|(_, entry)| entry.stale_deadline_unix <= now_unix)
            .map(|(prefix, _)| prefix.clone())
            .collect();
        for prefix in &stale {
            store.actions.remove(prefix);
        }
        if !stale.is_empty() {
            write_pending_action_store_unlocked(path, &store)?;
        }
        Ok(stale)
    })();
    unlock(lock)?;
    result
}

pub fn merge_pending_action_transport(
    path: &Path,
    identifier: &str,
    transport: &str,
    record: &BTreeMap<String, JsonValue>,
    now_unix: f64,
) -> Result<bool, String> {
    mutate_pending_action_store(path, |store| {
        let Some(key) = find_pending_action_key(store, identifier) else {
            return Ok((false, false));
        };
        let entry = store
            .actions
            .get_mut(&key)
            .expect("pending action key came from this store");
        merge_transport_record(entry, transport, record);
        entry.updated_at_unix = now_unix;
        Ok((true, true))
    })
}

pub fn mark_pending_action_handled(
    path: &Path,
    identifier: &str,
    source: &str,
    action: Option<&str>,
    now_unix: f64,
) -> Result<bool, String> {
    mutate_pending_action_store(path, |store| {
        let Some(key) = find_pending_action_key(store, identifier) else {
            return Ok((false, false));
        };
        let entry = store
            .actions
            .get_mut(&key)
            .expect("pending action key came from this store");
        apply_handled(entry, source, action, now_unix);
        Ok((true, true))
    })
}

pub fn remove_pending_action(
    path: &Path,
    identifier: &str,
) -> Result<bool, String> {
    mutate_pending_action_store(path, |store| {
        let Some(key) = find_pending_action_key(store, identifier) else {
            return Ok((false, false));
        };
        store.actions.remove(&key);
        Ok((true, true))
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingActionTransportRequestWire {
    pub operation: String,
    pub path: PathBuf,
    #[serde(default)]
    pub legacy_path: Option<PathBuf>,
    pub now_unix: f64,
    #[serde(default)]
    pub identifier: Option<String>,
    #[serde(default)]
    pub transport: Option<String>,
    #[serde(default)]
    pub record: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub plan_file: Option<String>,
    #[serde(default)]
    pub identity: BTreeMap<String, String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
}

pub fn pending_action_transport(
    request: &PendingActionTransportRequestWire,
) -> Result<JsonValue, String> {
    match request.operation.as_str() {
        "upsert" => {
            let identifier =
                required_field(request.identifier.as_deref(), "identifier")?;
            let transport =
                required_field(request.transport.as_deref(), "transport")?;
            upsert_pending_action_transport(
                &request.path,
                request.legacy_path.as_deref(),
                identifier,
                transport,
                &request.record,
                request.now_unix,
            )?;
            Ok(JsonValue::Null)
        }
        "list" => {
            let transport =
                required_field(request.transport.as_deref(), "transport")?;
            serde_json::to_value(list_pending_action_transport(
                &request.path,
                request.legacy_path.as_deref(),
                transport,
            )?)
            .map_err(|error| {
                format!("failed to serialize pending action transport list: {error}")
            })
        }
        "remove" => {
            let identifier =
                required_field(request.identifier.as_deref(), "identifier")?;
            let transport =
                required_field(request.transport.as_deref(), "transport")?;
            Ok(JsonValue::Bool(remove_pending_action_transport(
                &request.path,
                request.legacy_path.as_deref(),
                identifier,
                transport,
            )?))
        }
        "cleanup" => {
            let transport =
                required_field(request.transport.as_deref(), "transport")?;
            Ok(json!(cleanup_pending_action_transport(
                &request.path,
                request.legacy_path.as_deref(),
                transport,
                request.now_unix,
            )?))
        }
        "mark_plan_handled" => {
            let plan_file =
                required_field(request.plan_file.as_deref(), "plan_file")?;
            let source = required_field(request.source.as_deref(), "source")?;
            Ok(json!(mark_plan_pending_actions_handled(
                &request.path,
                request.legacy_path.as_deref(),
                plan_file,
                &request.identity,
                source,
                request.action.as_deref(),
                request.now_unix,
            )?))
        }
        other => Err(format!(
            "unknown pending action transport operation: {other}"
        )),
    }
}

pub fn resolve_pending_action_prefix(
    store: &PendingActionStoreWire,
    prefix: &str,
) -> PendingActionIdentityWire {
    let ids = store
        .actions
        .values()
        .map(|entry| entry.notification_id.as_str());
    super::mobile::resolve_notification_prefix(prefix, ids)
}

pub fn pending_action_state_for_notification(
    notification: &NotificationWire,
    pending: Option<&PendingActionWire>,
    now_unix: f64,
) -> MobileActionStateWire {
    let Some(action) = notification.action.as_deref() else {
        return MobileActionStateWire::Unsupported;
    };
    let action_kind =
        MobileActionKindWire::from_notification_action(Some(action));
    if !action_kind.is_gate() {
        return MobileActionStateWire::Unsupported;
    }
    if externally_handled_state(notification) {
        return MobileActionStateWire::AlreadyHandled;
    }
    if required_target_missing(notification) {
        return MobileActionStateWire::MissingTarget;
    }
    if let Some(pending) = pending {
        if pending.state == MobileActionStateWire::Stale
            || pending.stale_deadline_unix <= now_unix
        {
            return MobileActionStateWire::Stale;
        }
        if pending.state == MobileActionStateWire::AlreadyHandled {
            return MobileActionStateWire::AlreadyHandled;
        }
    }
    MobileActionStateWire::Available
}

pub fn pending_action_state_from_store(
    store: &PendingActionStoreWire,
    notification: &NotificationWire,
    now_unix: f64,
) -> MobileActionStateWire {
    let pending = store
        .actions
        .values()
        .find(|entry| entry.notification_id == notification.id);
    pending_action_state_for_notification(notification, pending, now_unix)
}

pub fn current_unix_time() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0)
}

fn required_field<'a>(
    value: Option<&'a str>,
    field: &str,
) -> Result<&'a str, String> {
    value.filter(|value| !value.is_empty()).ok_or_else(|| {
        format!("pending action transport request missing {field}")
    })
}

fn mutate_pending_action_store<T>(
    path: &Path,
    mutator: impl FnOnce(&mut PendingActionStoreWire) -> Result<(T, bool), String>,
) -> Result<T, String> {
    let lock = open_lock_file(path)?;
    lock.lock_exclusive().map_err(|e| e.to_string())?;
    let result = (|| {
        let mut store = read_pending_action_store_unlocked(path)?;
        let (value, changed) = mutator(&mut store)?;
        if changed {
            write_pending_action_store_unlocked(path, &store)?;
        }
        Ok(value)
    })();
    unlock(lock)?;
    result
}

fn find_pending_action_key(
    store: &PendingActionStoreWire,
    identifier: &str,
) -> Option<String> {
    if store.actions.contains_key(identifier) {
        return Some(identifier.to_string());
    }
    if let Some((key, _entry)) = store
        .actions
        .iter()
        .find(|(_key, entry)| entry.notification_id == identifier)
    {
        return Some(key.clone());
    }
    let mut matches = store
        .actions
        .iter()
        .filter(|(_key, entry)| entry.notification_id.starts_with(identifier));
    let (key, _entry) = matches.next()?;
    if matches.next().is_some() {
        return None;
    }
    Some(key.clone())
}

fn apply_handled(
    entry: &mut PendingActionWire,
    source: &str,
    action: Option<&str>,
    now_unix: f64,
) {
    entry.state = MobileActionStateWire::AlreadyHandled;
    entry.handled_source = Some(source.to_string());
    entry.handled_at_unix = Some(now_unix);
    entry.updated_at_unix = now_unix;
    if let Some(action) = action {
        entry.handled_action = Some(action.to_string());
    }
}

fn merge_transport_record(
    entry: &mut PendingActionWire,
    transport: &str,
    record: &BTreeMap<String, JsonValue>,
) {
    if let Some(existing) = entry
        .transports
        .iter_mut()
        .find(|item| item.transport == transport)
    {
        existing.record.extend(record.clone());
        return;
    }
    entry.transports.push(PendingActionTransportWire {
        transport: transport.to_string(),
        record: record.clone(),
    });
}

fn upsert_pending_action_transport(
    path: &Path,
    legacy_path: Option<&Path>,
    identifier: &str,
    transport: &str,
    record: &BTreeMap<String, JsonValue>,
    now_unix: f64,
) -> Result<(), String> {
    mutate_pending_action_store(path, |store| {
        if let Some(legacy_path) = legacy_path {
            merge_legacy_telegram_pending_actions(store, legacy_path)?;
        }
        if let Some(key) = find_pending_action_key(store, identifier) {
            let entry = store
                .actions
                .get_mut(&key)
                .expect("pending action key came from this store");
            merge_transport_record(entry, transport, record);
            entry.updated_at_unix = now_unix;
            return Ok(((), true));
        }
        store.actions.insert(
            identifier.to_string(),
            pending_action_from_transport_record(
                identifier, transport, record, now_unix,
            ),
        );
        Ok(((), true))
    })
}

fn pending_action_from_transport_record(
    identifier: &str,
    transport: &str,
    record: &BTreeMap<String, JsonValue>,
    now_unix: f64,
) -> PendingActionWire {
    let action = record
        .get("action")
        .and_then(JsonValue::as_str)
        .unwrap_or("")
        .to_string();
    PendingActionWire {
        schema_version: PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
        prefix: identifier.to_string(),
        notification_id: record
            .get("notification_id")
            .and_then(JsonValue::as_str)
            .unwrap_or(identifier)
            .to_string(),
        action_kind: MobileActionKindWire::from_notification_action(Some(
            &action,
        )),
        action,
        action_data: record
            .get("action_data")
            .and_then(JsonValue::as_object)
            .map(string_object)
            .unwrap_or_default(),
        files: record
            .get("files")
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        created_at_unix: now_unix,
        updated_at_unix: now_unix,
        stale_deadline_unix: now_unix + DEFAULT_PENDING_ACTION_STALE_SECONDS,
        transports: vec![PendingActionTransportWire {
            transport: transport.to_string(),
            record: record.clone(),
        }],
        state: MobileActionStateWire::Available,
        handled_source: None,
        handled_at_unix: None,
        handled_action: None,
    }
}

fn string_object(
    values: &serde_json::Map<String, JsonValue>,
) -> BTreeMap<String, String> {
    values
        .iter()
        .filter_map(|(key, value)| {
            value.as_str().map(|value| (key.clone(), value.to_string()))
        })
        .collect()
}

fn list_pending_action_transport(
    path: &Path,
    legacy_path: Option<&Path>,
    transport: &str,
) -> Result<BTreeMap<String, JsonValue>, String> {
    let store = read_pending_action_store(path, legacy_path)?;
    Ok(store
        .actions
        .iter()
        .filter_map(|(key, entry)| {
            transport_action_view(entry, transport, legacy_path.is_some())
                .map(|record| (key.clone(), record))
        })
        .collect())
}

fn transport_action_view(
    entry: &PendingActionWire,
    transport: &str,
    include_legacy: bool,
) -> Option<JsonValue> {
    let transport_record = entry.transports.iter().find(|item| {
        transport_matches(&item.transport, transport, include_legacy)
    })?;
    let mut record: serde_json::Map<String, JsonValue> =
        transport_record.record.clone().into_iter().collect();
    record
        .entry("notification_id".to_string())
        .or_insert_with(|| json!(entry.notification_id));
    record
        .entry("action".to_string())
        .or_insert_with(|| json!(entry.action));
    record
        .entry("action_kind".to_string())
        .or_insert_with(|| json!(entry.action_kind));
    record
        .entry("action_data".to_string())
        .or_insert_with(|| json!(entry.action_data));
    record
        .entry("files".to_string())
        .or_insert_with(|| json!(entry.files));
    record
        .entry("created_at".to_string())
        .or_insert_with(|| json!(entry.created_at_unix));
    record
        .entry("created_at_unix".to_string())
        .or_insert_with(|| json!(entry.created_at_unix));
    record
        .entry("updated_at_unix".to_string())
        .or_insert_with(|| json!(entry.updated_at_unix));
    record
        .entry("stale_deadline_unix".to_string())
        .or_insert_with(|| json!(entry.stale_deadline_unix));
    Some(JsonValue::Object(record))
}

fn remove_pending_action_transport(
    path: &Path,
    legacy_path: Option<&Path>,
    identifier: &str,
    transport: &str,
) -> Result<bool, String> {
    mutate_pending_action_store(path, |store| {
        if let Some(legacy_path) = legacy_path {
            merge_legacy_telegram_pending_actions(store, legacy_path)?;
        }
        let Some(key) = find_pending_action_key(store, identifier) else {
            return Ok((false, false));
        };
        let entry = store
            .actions
            .get_mut(&key)
            .expect("pending action key came from this store");
        let before = entry.transports.len();
        entry.transports.retain(|item| {
            !transport_matches(&item.transport, transport, true)
        });
        let removed = entry.transports.len() != before;
        let remove_entry = removed
            && entry.transports.is_empty()
            && !entry.action_kind.is_gate();
        if remove_entry {
            store.actions.remove(&key);
        }
        Ok((removed, removed))
    })
}

fn cleanup_pending_action_transport(
    path: &Path,
    legacy_path: Option<&Path>,
    transport: &str,
    now_unix: f64,
) -> Result<Vec<String>, String> {
    mutate_pending_action_store(path, |store| {
        if let Some(legacy_path) = legacy_path {
            merge_legacy_telegram_pending_actions(store, legacy_path)?;
        }
        let mut removed = Vec::new();
        let mut remove_entries = Vec::new();
        for (key, entry) in &mut store.actions {
            if entry.stale_deadline_unix > now_unix {
                continue;
            }
            let before = entry.transports.len();
            entry.transports.retain(|item| {
                !transport_matches(&item.transport, transport, true)
            });
            if entry.transports.len() == before {
                continue;
            }
            removed.push(key.clone());
            if entry.transports.is_empty() && !entry.action_kind.is_gate() {
                remove_entries.push(key.clone());
            }
        }
        for key in remove_entries {
            store.actions.remove(&key);
        }
        let changed = !removed.is_empty();
        Ok((removed, changed))
    })
}

fn mark_plan_pending_actions_handled(
    path: &Path,
    legacy_path: Option<&Path>,
    plan_file: &str,
    identity: &BTreeMap<String, String>,
    source: &str,
    action: Option<&str>,
    now_unix: f64,
) -> Result<Vec<String>, String> {
    if identity.is_empty() {
        return Ok(Vec::new());
    }
    mutate_pending_action_store(path, |store| {
        if let Some(legacy_path) = legacy_path {
            merge_legacy_telegram_pending_actions(store, legacy_path)?;
        }
        let mut marked = Vec::new();
        for entry in store.actions.values_mut() {
            if !matches!(entry.action.as_str(), "PlanApproval" | "EpicApproval")
                || !entry_plan_file_matches(entry, plan_file)
                || !entry_identity_matches(entry, identity)
            {
                continue;
            }
            apply_handled(entry, source, action, now_unix);
            marked.push(entry.notification_id.clone());
        }
        let changed = !marked.is_empty();
        Ok((marked, changed))
    })
}

fn entry_plan_file_matches(entry: &PendingActionWire, plan_file: &str) -> bool {
    entry.files.iter().any(|file| file == plan_file)
        || entry
            .action_data
            .get("plan_file")
            .is_some_and(|value| value == plan_file)
}

fn entry_identity_matches(
    entry: &PendingActionWire,
    identity: &BTreeMap<String, String>,
) -> bool {
    identity
        .iter()
        .any(|(key, value)| entry.action_data.get(key) == Some(value))
}

fn transport_matches(
    candidate: &str,
    transport: &str,
    include_legacy: bool,
) -> bool {
    candidate == transport
        || (include_legacy
            && transport == "telegram"
            && candidate == "telegram_legacy")
}

fn read_pending_action_store_unlocked(
    path: &Path,
) -> Result<PendingActionStoreWire, String> {
    if !path.exists() {
        return Ok(PendingActionStoreWire::default());
    }
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Ok(PendingActionStoreWire::default());
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| format!("failed to parse pending action store: {e}"))
}

fn write_pending_action_store_unlocked(
    path: &Path,
    store: &PendingActionStoreWire,
) -> Result<(), String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    reap_stale_pending_action_temps(path);
    let tmp_path = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("actions.json"),
        std::process::id()
    ));
    {
        let mut file = File::create(&tmp_path).map_err(|e| e.to_string())?;
        serde_json::to_writer_pretty(&mut file, store).map_err(|e| {
            format!("failed to serialize pending action store: {e}")
        })?;
        file.write_all(b"\n").map_err(|e| e.to_string())?;
        file.flush().map_err(|e| e.to_string())?;
    }
    fs::rename(&tmp_path, path).map_err(|e| e.to_string())
}

fn reap_stale_pending_action_temps(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
        return;
    };
    let prefix = format!(".{name}.");
    let Ok(entries) = fs::read_dir(parent) else {
        return;
    };
    let now = SystemTime::now();
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        if !file_name.starts_with(&prefix)
            || !file_name.ends_with(".tmp")
            || file_name.len() <= prefix.len() + ".tmp".len()
        {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age.as_secs_f64() > DEFAULT_PENDING_ACTION_STALE_SECONDS {
            let _ = fs::remove_file(entry.path());
        }
    }
}

fn merge_legacy_telegram_pending_actions(
    store: &mut PendingActionStoreWire,
    path: &Path,
) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let bytes = fs::read(path).map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Ok(());
    }
    let legacy: BTreeMap<String, JsonValue> = serde_json::from_slice(&bytes)
        .map_err(|e| {
            format!("failed to parse legacy telegram pending actions: {e}")
        })?;
    for (prefix, value) in legacy {
        if store.actions.contains_key(&prefix) {
            continue;
        }
        let Some(object) = value.as_object() else {
            continue;
        };
        let Some(notification_id) =
            object.get("notification_id").and_then(JsonValue::as_str)
        else {
            continue;
        };
        let Some(action) = object.get("action").and_then(JsonValue::as_str)
        else {
            continue;
        };
        let action_kind =
            MobileActionKindWire::from_notification_action(Some(action));
        if !action_kind.is_gate() {
            continue;
        }
        let action_data = object
            .get("action_data")
            .and_then(JsonValue::as_object)
            .map(|items| {
                items
                    .iter()
                    .filter_map(|(key, value)| {
                        value
                            .as_str()
                            .map(|value| (key.clone(), value.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let created_at_unix = object
            .get("created_at")
            .and_then(JsonValue::as_f64)
            .unwrap_or_else(current_unix_time);
        let mut record = BTreeMap::new();
        for key in ["chat_id", "message_id"] {
            if let Some(value) = object.get(key) {
                record.insert(key.to_string(), value.clone());
            }
        }
        store.actions.insert(
            prefix.clone(),
            PendingActionWire {
                schema_version: PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION,
                prefix,
                notification_id: notification_id.to_string(),
                action_kind,
                action: action.to_string(),
                action_data,
                files: object
                    .get("plan_file")
                    .and_then(JsonValue::as_str)
                    .map(|path| vec![path.to_string()])
                    .unwrap_or_default(),
                created_at_unix,
                updated_at_unix: created_at_unix,
                stale_deadline_unix: created_at_unix
                    + DEFAULT_PENDING_ACTION_STALE_SECONDS,
                transports: vec![PendingActionTransportWire {
                    transport: "telegram_legacy".to_string(),
                    record,
                }],
                state: MobileActionStateWire::Available,
                handled_source: None,
                handled_at_unix: None,
                handled_action: None,
            },
        );
    }
    Ok(())
}

fn externally_handled_state(notification: &NotificationWire) -> bool {
    match MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    ) {
        MobileActionKindWire::PlanApproval
        | MobileActionKindWire::EpicApproval => {
            let Some(response_dir) = action_path(notification, "response_dir")
            else {
                return false;
            };
            (response_dir.join("plan_response.json")).exists()
                || (response_dir.join("plan_approved.marker")).exists()
                || (response_dir.is_dir()
                    && !(response_dir.join("plan_request.json")).exists())
        }
        MobileActionKindWire::Hitl => {
            let Some(artifacts_dir) =
                action_path(notification, "artifacts_dir")
            else {
                return false;
            };
            (artifacts_dir.join("hitl_response.json")).exists()
                || (artifacts_dir.is_dir()
                    && !(artifacts_dir.join("hitl_request.json")).exists())
        }
        MobileActionKindWire::UserQuestion => {
            let Some(response_dir) = action_path(notification, "response_dir")
            else {
                return false;
            };
            (response_dir.join("question_response.json")).exists()
                || (response_dir.is_dir()
                    && !(response_dir.join("question_request.json")).exists())
        }
        MobileActionKindWire::LaunchApproval => {
            let Some(response_dir) = action_path(notification, "response_dir")
            else {
                return false;
            };
            (response_dir.join("launch_response.json")).exists()
                || (response_dir.is_dir()
                    && !(response_dir.join("launch_request.json")).exists())
        }
        MobileActionKindWire::TaskTriage
        | MobileActionKindWire::BeadSnooze
        | MobileActionKindWire::FlagTriage
        | MobileActionKindWire::BeadStaleCleanup
        | MobileActionKindWire::PluginsRequired
        | MobileActionKindWire::CustomGate => {
            let Some(bundle_path) = action_path(notification, "bundle_path")
            else {
                return false;
            };
            (bundle_path.join("response.json")).exists()
                || (bundle_path.join("cancellation.json")).exists()
        }
        MobileActionKindWire::NonAction | MobileActionKindWire::Unsupported => {
            false
        }
    }
}

fn required_target_missing(notification: &NotificationWire) -> bool {
    match MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    ) {
        MobileActionKindWire::PlanApproval
        | MobileActionKindWire::EpicApproval
        | MobileActionKindWire::UserQuestion
        | MobileActionKindWire::LaunchApproval => {
            action_path(notification, "response_dir").is_none()
        }
        MobileActionKindWire::Hitl => {
            action_path(notification, "artifacts_dir").is_none()
        }
        MobileActionKindWire::TaskTriage
        | MobileActionKindWire::BeadSnooze
        | MobileActionKindWire::FlagTriage
        | MobileActionKindWire::BeadStaleCleanup
        | MobileActionKindWire::PluginsRequired
        | MobileActionKindWire::CustomGate => {
            action_path(notification, "bundle_path").is_none()
        }
        MobileActionKindWire::NonAction | MobileActionKindWire::Unsupported => {
            false
        }
    }
}

fn action_path(notification: &NotificationWire, key: &str) -> Option<PathBuf> {
    let raw = notification.action_data.get(key)?.trim();
    if raw.is_empty() {
        return None;
    }
    Some(expand_home_path(raw))
}

fn expand_home_path(path: &str) -> PathBuf {
    if path == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(path));
    }
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(path)
}

fn ensure_parent(path: &Path) -> Result<&Path, String> {
    path.parent().ok_or_else(|| {
        format!("pending action path has no parent: {}", path.display())
    })
}

fn open_lock_file(path: &Path) -> Result<File, String> {
    let parent = ensure_parent(path)?;
    fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(|e| e.to_string())
}

#[allow(clippy::incompatible_msrv)]
fn unlock(lock: File) -> Result<(), String> {
    lock.unlock().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::super::mobile::PendingActionPrefixResolutionWire;
    use super::*;

    fn notification(
        id: &str,
        action: &str,
        dir_key: &str,
        dir: &Path,
    ) -> NotificationWire {
        NotificationWire {
            id: id.to_string(),
            timestamp: "2026-05-06T12:00:00+00:00".to_string(),
            sender: "test".to_string(),
            action: Some(action.to_string()),
            action_data: BTreeMap::from([(
                dir_key.to_string(),
                dir.display().to_string(),
            )]),
            ..NotificationWire::default()
        }
    }

    #[test]
    fn pending_store_registers_and_resolves_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let path = pending_action_store_path(tmp.path());
        let n1 = notification(
            "abcdef01-one",
            "PlanApproval",
            "response_dir",
            tmp.path(),
        );
        let n2 = notification(
            "abcdef02-two",
            "PlanApproval",
            "response_dir",
            tmp.path(),
        );
        let a1 = pending_action_from_notification(&n1, 10.0).unwrap();
        let a2 = pending_action_from_notification(&n2, 10.0).unwrap();

        register_pending_action(&path, &a1).unwrap();
        register_pending_action(&path, &a2).unwrap();
        let store = read_pending_action_store(&path, None).unwrap();

        assert_eq!(
            resolve_pending_action_prefix(&store, "abcdef01").resolution,
            PendingActionPrefixResolutionWire::UniquePrefix
        );
        assert_eq!(
            resolve_pending_action_prefix(&store, "abcdef").resolution,
            PendingActionPrefixResolutionWire::AmbiguousPrefix
        );
    }

    #[test]
    fn pending_state_detects_stale_and_external_plan_response() {
        let tmp = tempfile::tempdir().unwrap();
        let response_dir = tmp.path().join("plan");
        fs::create_dir_all(&response_dir).unwrap();
        fs::write(response_dir.join("plan_request.json"), "{}").unwrap();
        let n = notification(
            "plan-row",
            "PlanApproval",
            "response_dir",
            &response_dir,
        );
        let mut pending = pending_action_from_notification(&n, 10.0).unwrap();
        pending.stale_deadline_unix = 20.0;

        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 21.0),
            MobileActionStateWire::Stale
        );

        fs::write(response_dir.join("plan_response.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 21.0),
            MobileActionStateWire::AlreadyHandled
        );
    }

    #[test]
    fn launch_approval_is_a_pending_action_kind() {
        let tmp = tempfile::tempdir().unwrap();
        let response_dir = tmp.path().join("launch");
        fs::create_dir_all(&response_dir).unwrap();
        fs::write(response_dir.join("launch_request.json"), "{}").unwrap();
        let n = notification(
            "launch-1234",
            "LaunchApproval",
            "response_dir",
            &response_dir,
        );
        let pending = pending_action_from_notification(&n, 10.0).unwrap();

        assert_eq!(pending.action_kind, MobileActionKindWire::LaunchApproval);
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::Available
        );

        fs::write(response_dir.join("launch_response.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 12.0),
            MobileActionStateWire::AlreadyHandled
        );
    }

    #[test]
    fn epic_approval_is_typed_and_resolves_through_pending_store() {
        let tmp = tempfile::tempdir().unwrap();
        let response_dir = tmp.path().join("epic");
        fs::create_dir_all(&response_dir).unwrap();
        fs::write(response_dir.join("plan_request.json"), "{}").unwrap();
        let n = notification(
            "epic1234-full",
            "EpicApproval",
            "response_dir",
            &response_dir,
        );
        let pending = pending_action_from_notification(&n, 10.0).unwrap();

        assert_eq!(pending.schema_version, 3);
        assert_eq!(pending.action_kind, MobileActionKindWire::EpicApproval);
        assert_eq!(
            serde_json::to_value(&pending).unwrap()["action_kind"],
            "epic_approval"
        );
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::Available
        );

        let path = pending_action_store_path(tmp.path());
        register_pending_action(&path, &pending).unwrap();
        let store = read_pending_action_store(&path, None).unwrap();
        assert_eq!(
            resolve_pending_action_prefix(&store, "epic1234").resolution,
            PendingActionPrefixResolutionWire::UniquePrefix
        );

        fs::write(response_dir.join("plan_response.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 12.0),
            MobileActionStateWire::AlreadyHandled
        );
    }

    #[test]
    fn epic_approval_without_response_dir_has_missing_target_state() {
        let mut n = notification(
            "epic-missing",
            "EpicApproval",
            "response_dir",
            Path::new("/tmp/unused"),
        );
        n.action_data.clear();
        let pending = pending_action_from_notification(&n, 10.0).unwrap();

        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::MissingTarget
        );
    }

    #[test]
    fn custom_gate_uses_only_neutral_terminal_files() {
        let tmp = tempfile::tempdir().unwrap();
        let bundle = tmp.path().join("custom");
        fs::create_dir_all(&bundle).unwrap();
        fs::write(bundle.join("request.json"), "{}").unwrap();
        let n = notification(
            "custom1234-full",
            "CustomGate",
            "bundle_path",
            &bundle,
        );
        let pending = pending_action_from_notification(&n, 10.0).unwrap();
        let store_path = pending_action_store_path(tmp.path());
        register_pending_action(&store_path, &pending).unwrap();
        let store = read_pending_action_store(&store_path, None).unwrap();

        assert_eq!(pending.action_kind, MobileActionKindWire::CustomGate);
        assert_eq!(
            resolve_pending_action_prefix(&store, "custom12").resolution,
            PendingActionPrefixResolutionWire::UniquePrefix
        );
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::Available
        );

        fs::write(bundle.join("hitl_response.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::Available
        );

        fs::write(bundle.join("cancellation.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::AlreadyHandled
        );
        fs::remove_file(bundle.join("cancellation.json")).unwrap();
        fs::write(bundle.join("response.json"), "{}").unwrap();
        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::AlreadyHandled
        );
    }

    #[test]
    fn custom_gate_without_bundle_path_has_missing_target_state() {
        let mut n = notification(
            "custom-missing",
            "CustomGate",
            "bundle_path",
            Path::new("/tmp/unused"),
        );
        n.action_data.clear();
        let pending = pending_action_from_notification(&n, 10.0).unwrap();

        assert_eq!(
            pending_action_state_for_notification(&n, Some(&pending), 11.0),
            MobileActionStateWire::MissingTarget
        );
    }

    #[test]
    fn pending_store_merges_legacy_telegram_shape() {
        let tmp = tempfile::tempdir().unwrap();
        let path = pending_action_store_path(tmp.path());
        let legacy = legacy_telegram_pending_actions_path(tmp.path());
        fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        fs::write(
            &legacy,
            r#"{"abcd1234":{"notification_id":"abcd1234-full","action":"PlanApproval","action_data":{"response_dir":"/tmp/plan"},"message_id":42,"chat_id":"chat","created_at":10.0}}"#,
        )
        .unwrap();

        let store = read_pending_action_store(&path, Some(&legacy)).unwrap();

        assert_eq!(store.actions["abcd1234"].notification_id, "abcd1234-full");
        assert_eq!(
            store.actions["abcd1234"].transports[0].transport,
            "telegram_legacy"
        );
    }

    #[test]
    fn cleanup_stale_pending_actions_removes_only_expired_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let path = pending_action_store_path(tmp.path());
        let n = notification(
            "abcdef01-one",
            "PlanApproval",
            "response_dir",
            tmp.path(),
        );
        let mut old = pending_action_from_notification(&n, 10.0).unwrap();
        old.stale_deadline_unix = 20.0;
        register_pending_action(&path, &old).unwrap();

        let removed = cleanup_stale_pending_actions(&path, 21.0).unwrap();
        let store = read_pending_action_store(&path, None).unwrap();

        assert_eq!(removed, vec!["abcdef01".to_string()]);
        assert!(store.actions.is_empty());
    }
}

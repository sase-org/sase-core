use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use super::mobile::{
    pending_action_identity, MobileActionKindWire, MobileActionStateWire,
    PendingActionIdentityWire,
};
use super::wire::NotificationWire;

pub const PENDING_ACTION_STORE_WIRE_SCHEMA_VERSION: u32 = 1;
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
    let action_kind = action_kind_from_str(action);
    if !is_pending_action_kind(action_kind) {
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
    let action_kind = action_kind_from_str(action);
    if !is_pending_action_kind(action_kind) {
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
        let action_kind = action_kind_from_str(action);
        if !is_pending_action_kind(action_kind) {
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
            },
        );
    }
    Ok(())
}

fn externally_handled_state(notification: &NotificationWire) -> bool {
    match notification.action.as_deref() {
        Some("PlanApproval") => {
            let Some(response_dir) = action_path(notification, "response_dir")
            else {
                return false;
            };
            (response_dir.join("plan_response.json")).exists()
                || (response_dir.join("plan_approved.marker")).exists()
                || (response_dir.is_dir()
                    && !(response_dir.join("plan_request.json")).exists())
        }
        Some("HITL") => {
            let Some(artifacts_dir) =
                action_path(notification, "artifacts_dir")
            else {
                return false;
            };
            (artifacts_dir.join("hitl_response.json")).exists()
                || (artifacts_dir.is_dir()
                    && !(artifacts_dir.join("hitl_request.json")).exists())
        }
        Some("UserQuestion") => {
            let Some(response_dir) = action_path(notification, "response_dir")
            else {
                return false;
            };
            (response_dir.join("question_response.json")).exists()
                || (response_dir.is_dir()
                    && !(response_dir.join("question_request.json")).exists())
        }
        _ => false,
    }
}

fn required_target_missing(notification: &NotificationWire) -> bool {
    match notification.action.as_deref() {
        Some("PlanApproval") | Some("UserQuestion") => {
            action_path(notification, "response_dir").is_none()
        }
        Some("HITL") => action_path(notification, "artifacts_dir").is_none(),
        _ => false,
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

fn action_kind_from_str(action: &str) -> MobileActionKindWire {
    match action {
        "PlanApproval" => MobileActionKindWire::PlanApproval,
        "HITL" => MobileActionKindWire::Hitl,
        "UserQuestion" => MobileActionKindWire::UserQuestion,
        _ => MobileActionKindWire::Unsupported,
    }
}

fn is_pending_action_kind(kind: MobileActionKindWire) -> bool {
    matches!(
        kind,
        MobileActionKindWire::PlanApproval
            | MobileActionKindWire::Hitl
            | MobileActionKindWire::UserQuestion
    )
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
            notes: Vec::new(),
            files: Vec::new(),
            action: Some(action.to_string()),
            action_data: BTreeMap::from([(
                dir_key.to_string(),
                dir.display().to_string(),
            )]),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
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

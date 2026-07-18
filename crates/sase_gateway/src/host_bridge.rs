use std::{
    fmt,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

pub use sase_core::host_bridge::{
    split_command_words, CommandHelperHostBridge, DynHelperHostBridge,
    HelperHostBridge, HostBridgeError, StaticHelperHostBridge,
    UnavailableHelperHostBridge,
};
use sase_core::notifications::{
    apply_notification_state_update, current_unix_time,
    legacy_telegram_pending_actions_path, pending_action_state_from_store,
    pending_action_store_path, read_notifications_snapshot_with_options,
    ActionResultWire, GateActionRequestWire, MobileActionStateWire,
    NotificationStateUpdateWire, NotificationStoreSnapshotWire,
    NotificationWire, QuestionActionRequestWire,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value as JsonValue;

use crate::wire::{
    MobileAgentImageLaunchRequestWire, MobileAgentKillRequestWire,
    MobileAgentKillResultWire, MobileAgentLaunchResultWire,
    MobileAgentListRequestWire, MobileAgentListResponseWire,
    MobileAgentResumeOptionsResponseWire, MobileAgentRetryRequestWire,
    MobileAgentRetryResultWire, MobileAgentTextLaunchRequestWire,
    NotificationStateMutationResponseWire, GATEWAY_WIRE_SCHEMA_VERSION,
};

#[derive(Clone)]
pub struct DynAgentHostBridge(Arc<dyn AgentHostBridge>);

impl DynAgentHostBridge {
    pub fn new(bridge: Arc<dyn AgentHostBridge>) -> Self {
        Self(bridge)
    }

    pub fn list_agents(
        &self,
        request: &MobileAgentListRequestWire,
    ) -> Result<MobileAgentListResponseWire, HostBridgeError> {
        self.0.list_agents(request)
    }

    pub fn resume_options(
        &self,
    ) -> Result<MobileAgentResumeOptionsResponseWire, HostBridgeError> {
        self.0.resume_options()
    }

    pub fn launch_text(
        &self,
        request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.0.launch_text(request)
    }

    pub fn launch_image(
        &self,
        request: &MobileAgentImageLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.0.launch_image(request)
    }

    pub fn kill_agent(
        &self,
        name: &str,
        request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        self.0.kill_agent(name, request)
    }

    pub fn retry_agent(
        &self,
        name: &str,
        request: &MobileAgentRetryRequestWire,
    ) -> Result<MobileAgentRetryResultWire, HostBridgeError> {
        self.0.retry_agent(name, request)
    }
}

impl fmt::Debug for DynAgentHostBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynAgentHostBridge").finish_non_exhaustive()
    }
}

pub trait AgentHostBridge: Send + Sync {
    fn list_agents(
        &self,
        _request: &MobileAgentListRequestWire,
    ) -> Result<MobileAgentListResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }

    fn resume_options(
        &self,
    ) -> Result<MobileAgentResumeOptionsResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }

    fn launch_text(
        &self,
        _request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }

    fn launch_image(
        &self,
        _request: &MobileAgentImageLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }

    fn kill_agent(
        &self,
        _name: &str,
        _request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }

    fn retry_agent(
        &self,
        _name: &str,
        _request: &MobileAgentRetryRequestWire,
    ) -> Result<MobileAgentRetryResultWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "agent_bridge".to_string(),
        ))
    }
}

#[derive(Debug, Default)]
pub struct UnavailableAgentHostBridge;

impl AgentHostBridge for UnavailableAgentHostBridge {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandAgentHostBridge {
    command: Vec<String>,
    sase_home: Option<PathBuf>,
}

impl CommandAgentHostBridge {
    pub fn new(command: Vec<String>) -> Self {
        Self {
            command,
            sase_home: None,
        }
    }

    pub fn new_with_sase_home(
        command: Vec<String>,
        sase_home: impl Into<PathBuf>,
    ) -> Self {
        Self {
            command,
            sase_home: Some(sase_home.into()),
        }
    }

    pub fn default_command() -> Vec<String> {
        std::env::var("SASE_MOBILE_AGENT_BRIDGE_COMMAND")
            .ok()
            .and_then(|raw| split_command_words(&raw).ok())
            .filter(|parts| !parts.is_empty())
            .unwrap_or_else(|| vec!["sase".to_string()])
    }

    fn invoke<Request, Response>(
        &self,
        operation: &str,
        request: &Request,
    ) -> Result<Response, HostBridgeError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let (program, fixed_args) =
            self.command.split_first().ok_or_else(|| {
                HostBridgeError::BridgeUnavailable("agent_bridge".to_string())
            })?;
        let mut command = Command::new(program);
        command
            .args(fixed_args)
            .args(["mobile", "agent-bridge", operation])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(sase_home) = &self.sase_home {
            command.env("SASE_HOME", sase_home);
        }
        let mut child = command.spawn().map_err(|_| {
            HostBridgeError::BridgeUnavailable("agent_bridge".to_string())
        })?;

        {
            let Some(mut stdin) = child.stdin.take() else {
                return Err(HostBridgeError::BridgeUnavailable(format!(
                    "agent_bridge:{operation}:stdin"
                )));
            };
            serde_json::to_writer(&mut stdin, request).map_err(|_| {
                HostBridgeError::BridgeUnavailable(format!(
                    "agent_bridge:{operation}:encode"
                ))
            })?;
            stdin.write_all(b"\n").map_err(|_| {
                HostBridgeError::BridgeUnavailable(format!(
                    "agent_bridge:{operation}:stdin"
                ))
            })?;
        }

        let output = child.wait_with_output().map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "agent_bridge:{operation}:exit"
            ))
        })?;
        if !output.status.success() {
            if operation == "launch-image" && output.status.code() == Some(3) {
                return Err(HostBridgeError::InvalidUpload(format!(
                    "agent_bridge:{operation}"
                )));
            }
            if operation == "kill-agent" || operation == "retry-agent" {
                let target = format!("agent_bridge:{operation}");
                return Err(match output.status.code() {
                    Some(4) => HostBridgeError::AgentNotFound(target),
                    Some(5) => HostBridgeError::AgentNotRunning(target),
                    Some(6) => HostBridgeError::PermissionDenied(target),
                    _ => HostBridgeError::BridgeUnavailable(target),
                });
            }
            if operation.starts_with("launch-") {
                return Err(HostBridgeError::LaunchFailed(format!(
                    "agent_bridge:{operation}"
                )));
            }
            return Err(HostBridgeError::BridgeUnavailable(format!(
                "agent_bridge:{operation}"
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "agent_bridge:{operation}:invalid_json"
            ))
        })
    }
}

impl AgentHostBridge for CommandAgentHostBridge {
    fn list_agents(
        &self,
        request: &MobileAgentListRequestWire,
    ) -> Result<MobileAgentListResponseWire, HostBridgeError> {
        self.invoke("list-agents", request)
    }

    fn resume_options(
        &self,
    ) -> Result<MobileAgentResumeOptionsResponseWire, HostBridgeError> {
        self.invoke(
            "resume-options",
            &serde_json::json!({"schema_version": GATEWAY_WIRE_SCHEMA_VERSION}),
        )
    }

    fn launch_text(
        &self,
        request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.invoke("launch-text", request)
    }

    fn launch_image(
        &self,
        request: &MobileAgentImageLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        self.invoke("launch-image", request)
    }

    fn kill_agent(
        &self,
        name: &str,
        request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        self.invoke(
            "kill-agent",
            &serde_json::json!({
                "schema_version": request.schema_version,
                "name": name,
                "reason": request.reason.clone(),
                "device_id": request.device_id.clone(),
            }),
        )
    }

    fn retry_agent(
        &self,
        name: &str,
        request: &MobileAgentRetryRequestWire,
    ) -> Result<MobileAgentRetryResultWire, HostBridgeError> {
        self.invoke(
            "retry-agent",
            &serde_json::json!({
                "schema_version": request.schema_version,
                "name": name,
                "request_id": request.request_id.clone(),
                "prompt_override": request.prompt_override.clone(),
                "dry_run": request.dry_run,
                "kill_source_first": request.kill_source_first,
                "device_id": request.device_id.clone(),
            }),
        )
    }
}

#[derive(Clone)]
pub struct DynNotificationHostBridge(Arc<dyn NotificationHostBridge>);

impl DynNotificationHostBridge {
    pub fn new(bridge: Arc<dyn NotificationHostBridge>) -> Self {
        Self(bridge)
    }

    pub fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        self.0.list_notifications(include_dismissed)
    }

    pub fn notification_file_metadata(
        &self,
        path: &str,
    ) -> HostFileMetadataWire {
        self.0.notification_file_metadata(path)
    }

    pub fn action_state(
        &self,
        notification: &NotificationWire,
    ) -> MobileActionStateWire {
        self.0.action_state(notification)
    }

    pub fn mark_notification_read(
        &self,
        id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        self.0.mark_notification_read(id)
    }

    pub fn dismiss_notification(
        &self,
        id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        self.0.dismiss_notification(id)
    }

    pub fn execute_gate_action(
        &self,
        request: &GateActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.0.execute_gate_action(request)
    }

    pub fn execute_question_action(
        &self,
        request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.0.execute_question_action(request)
    }
}

impl fmt::Debug for DynNotificationHostBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynNotificationHostBridge")
            .finish_non_exhaustive()
    }
}

pub trait NotificationHostBridge: Send + Sync {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError>;

    fn notification_file_metadata(&self, path: &str) -> HostFileMetadataWire {
        let path = expand_home_path(path);
        match std::fs::metadata(path) {
            Ok(metadata) => HostFileMetadataWire {
                byte_size: if metadata.is_file() {
                    Some(metadata.len())
                } else {
                    None
                },
                path_available: true,
            },
            Err(_) => HostFileMetadataWire {
                byte_size: None,
                path_available: false,
            },
        }
    }

    fn action_state(
        &self,
        notification: &NotificationWire,
    ) -> MobileActionStateWire {
        sase_core::notifications::pending_action_state_for_notification(
            notification,
            None,
            current_unix_time(),
        )
    }

    fn mark_notification_read(
        &self,
        _id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "notification state mutations are not supported by this bridge"
                .to_string(),
        ))
    }

    fn dismiss_notification(
        &self,
        _id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "notification state mutations are not supported by this bridge"
                .to_string(),
        ))
    }

    fn execute_gate_action(
        &self,
        _request: &GateActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "gate action mutations are not supported by this bridge"
                .to_string(),
        ))
    }

    fn execute_question_action(
        &self,
        _request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "question action mutations are not supported by this bridge"
                .to_string(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostFileMetadataWire {
    pub byte_size: Option<u64>,
    pub path_available: bool,
}

#[derive(Debug)]
pub struct LocalJsonlNotificationBridge {
    sase_home: PathBuf,
    notifications_path: PathBuf,
    pending_actions_path: PathBuf,
    legacy_telegram_pending_actions_path: PathBuf,
    gate_bridge_command: Vec<String>,
}

impl LocalJsonlNotificationBridge {
    pub fn new(sase_home: impl AsRef<Path>) -> Self {
        let sase_home = sase_home.as_ref().to_path_buf();
        Self {
            notifications_path: sase_home
                .join("notifications")
                .join("notifications.jsonl"),
            pending_actions_path: pending_action_store_path(&sase_home),
            legacy_telegram_pending_actions_path:
                legacy_telegram_pending_actions_path(&sase_home),
            sase_home,
            gate_bridge_command: std::env::var(
                "SASE_MOBILE_NOTIFICATION_BRIDGE_COMMAND",
            )
            .ok()
            .and_then(|raw| split_command_words(&raw).ok())
            .filter(|parts| !parts.is_empty())
            .unwrap_or_else(|| vec!["sase".to_string()]),
        }
    }

    pub fn new_with_gate_bridge_command(
        sase_home: impl AsRef<Path>,
        command: Vec<String>,
    ) -> Self {
        let mut bridge = Self::new(sase_home);
        bridge.gate_bridge_command = command;
        bridge
    }

    fn invoke_notification_bridge<Request>(
        &self,
        operation: &str,
        request: &Request,
        prefix: &str,
    ) -> Result<ActionResultWire, HostBridgeError>
    where
        Request: Serialize,
    {
        let (program, fixed_args) =
            self.gate_bridge_command.split_first().ok_or_else(|| {
                HostBridgeError::BridgeUnavailable(
                    "notification_bridge".to_string(),
                )
            })?;
        let mut command = Command::new(program);
        command
            .args(fixed_args)
            .args(["mobile", "notification-bridge", operation])
            .env("SASE_HOME", &self.sase_home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "notification_bridge:{operation}"
            ))
        })?;
        {
            let Some(mut stdin) = child.stdin.take() else {
                return Err(HostBridgeError::BridgeUnavailable(format!(
                    "notification_bridge:{operation}:stdin"
                )));
            };
            serde_json::to_writer(&mut stdin, request).map_err(|_| {
                HostBridgeError::InvalidActionRequest(
                    "failed to encode notification action request".to_string(),
                )
            })?;
            stdin.write_all(b"\n").map_err(|_| {
                HostBridgeError::BridgeUnavailable(format!(
                    "notification_bridge:{operation}:stdin"
                ))
            })?;
        }
        let output = child.wait_with_output().map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "notification_bridge:{operation}:exit"
            ))
        })?;
        if !output.status.success() {
            let detail = serde_json::from_slice::<JsonValue>(&output.stderr)
                .ok()
                .and_then(|value| {
                    value
                        .get("message")
                        .and_then(JsonValue::as_str)
                        .map(str::to_string)
                })
                .unwrap_or_else(|| {
                    "notification action bridge failed".to_string()
                });
            return Err(match output.status.code() {
                Some(4) => HostBridgeError::ActionMissing(prefix.to_string()),
                Some(5) => {
                    HostBridgeError::ActionAlreadyHandled(prefix.to_string())
                }
                Some(6) => HostBridgeError::ActionStale(prefix.to_string()),
                Some(7) => HostBridgeError::UnsupportedAction(detail),
                Some(2) => HostBridgeError::InvalidActionRequest(detail),
                _ => HostBridgeError::BridgeUnavailable(format!(
                    "notification_bridge:{operation}"
                )),
            });
        }
        serde_json::from_slice(&output.stdout).map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "notification_bridge:{operation}:invalid_json"
            ))
        })
    }
}

impl NotificationHostBridge for LocalJsonlNotificationBridge {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        read_notifications_snapshot_with_options(
            &self.notifications_path,
            include_dismissed,
            true,
        )
        .map_err(HostBridgeError::ReadNotifications)
    }

    fn action_state(
        &self,
        notification: &NotificationWire,
    ) -> MobileActionStateWire {
        match sase_core::notifications::read_pending_action_store(
            &self.pending_actions_path,
            Some(&self.legacy_telegram_pending_actions_path),
        ) {
            Ok(store) => pending_action_state_from_store(
                &store,
                notification,
                current_unix_time(),
            ),
            Err(_) => {
                sase_core::notifications::pending_action_state_for_notification(
                    notification,
                    None,
                    current_unix_time(),
                )
            }
        }
    }

    fn mark_notification_read(
        &self,
        id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        apply_notification_state_mutation(
            &self.notifications_path,
            id,
            NotificationStateUpdateWire::MarkRead { id: id.to_string() },
        )
    }

    fn dismiss_notification(
        &self,
        id: &str,
    ) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
        apply_notification_state_mutation(
            &self.notifications_path,
            id,
            NotificationStateUpdateWire::MarkDismissed { id: id.to_string() },
        )
    }

    fn execute_gate_action(
        &self,
        request: &GateActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.invoke_notification_bridge("gate-action", request, &request.prefix)
    }

    fn execute_question_action(
        &self,
        request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.invoke_notification_bridge(
            "question-action",
            request,
            &request.prefix,
        )
    }
}

#[derive(Debug)]
pub struct StaticNotificationHostBridge {
    snapshot: NotificationStoreSnapshotWire,
    action_states: std::collections::HashMap<String, MobileActionStateWire>,
}

impl StaticNotificationHostBridge {
    pub fn new(notifications: Vec<NotificationWire>) -> Self {
        Self {
            snapshot: NotificationStoreSnapshotWire {
                schema_version: sase_core::notifications::NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                notifications,
                counts: Default::default(),
                expired_ids: Vec::new(),
                stats: Default::default(),
            },
            action_states: Default::default(),
        }
    }

    pub fn new_with_action_states(
        notifications: Vec<NotificationWire>,
        action_states: std::collections::HashMap<String, MobileActionStateWire>,
    ) -> Self {
        Self {
            snapshot: NotificationStoreSnapshotWire {
                schema_version: sase_core::notifications::NOTIFICATION_STORE_WIRE_SCHEMA_VERSION,
                notifications,
                counts: Default::default(),
                expired_ids: Vec::new(),
                stats: Default::default(),
            },
            action_states,
        }
    }
}

impl NotificationHostBridge for StaticNotificationHostBridge {
    fn list_notifications(
        &self,
        include_dismissed: bool,
    ) -> Result<NotificationStoreSnapshotWire, HostBridgeError> {
        let mut snapshot = self.snapshot.clone();
        if !include_dismissed {
            snapshot
                .notifications
                .retain(|notification| !notification.dismissed);
        }
        Ok(snapshot)
    }

    fn action_state(
        &self,
        notification: &NotificationWire,
    ) -> MobileActionStateWire {
        self.action_states
            .get(&notification.id)
            .copied()
            .unwrap_or_else(|| {
                sase_core::notifications::pending_action_state_for_notification(
                    notification,
                    None,
                    current_unix_time(),
                )
            })
    }
}

#[derive(Debug, Clone)]
pub struct StaticAgentHostBridge {
    pub list_response: MobileAgentListResponseWire,
    pub resume_options_response: MobileAgentResumeOptionsResponseWire,
    pub text_launch_response: MobileAgentLaunchResultWire,
    pub image_launch_response: MobileAgentLaunchResultWire,
    pub kill_response: MobileAgentKillResultWire,
    pub retry_response: MobileAgentRetryResultWire,
}

impl AgentHostBridge for StaticAgentHostBridge {
    fn list_agents(
        &self,
        _request: &MobileAgentListRequestWire,
    ) -> Result<MobileAgentListResponseWire, HostBridgeError> {
        Ok(self.list_response.clone())
    }

    fn resume_options(
        &self,
    ) -> Result<MobileAgentResumeOptionsResponseWire, HostBridgeError> {
        Ok(self.resume_options_response.clone())
    }

    fn launch_text(
        &self,
        _request: &MobileAgentTextLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        Ok(self.text_launch_response.clone())
    }

    fn launch_image(
        &self,
        _request: &MobileAgentImageLaunchRequestWire,
    ) -> Result<MobileAgentLaunchResultWire, HostBridgeError> {
        Ok(self.image_launch_response.clone())
    }

    fn kill_agent(
        &self,
        _name: &str,
        _request: &MobileAgentKillRequestWire,
    ) -> Result<MobileAgentKillResultWire, HostBridgeError> {
        Ok(self.kill_response.clone())
    }

    fn retry_agent(
        &self,
        _name: &str,
        _request: &MobileAgentRetryRequestWire,
    ) -> Result<MobileAgentRetryResultWire, HostBridgeError> {
        Ok(self.retry_response.clone())
    }
}

fn apply_notification_state_mutation(
    notifications_path: &Path,
    id: &str,
    update: NotificationStateUpdateWire,
) -> Result<NotificationStateMutationResponseWire, HostBridgeError> {
    let outcome = apply_notification_state_update(notifications_path, &update)
        .map_err(HostBridgeError::ReadNotifications)?;
    if outcome.matched_count == 0 {
        return Err(HostBridgeError::NotificationMissing(id.to_string()));
    }
    let notification = outcome
        .notifications
        .into_iter()
        .find(|row| row.id == id)
        .ok_or_else(|| HostBridgeError::NotificationMissing(id.to_string()))?;
    Ok(NotificationStateMutationResponseWire {
        schema_version: GATEWAY_WIRE_SCHEMA_VERSION,
        notification_id: notification.id,
        read: notification.read,
        dismissed: notification.dismissed,
        changed: outcome.changed_count > 0,
    })
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

use std::{
    fmt,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
};

use sase_core::notifications::{
    apply_notification_state_update, current_unix_time,
    legacy_telegram_pending_actions_path, pending_action_state_from_store,
    pending_action_store_path, plan_hitl_action_response,
    plan_plan_action_response, plan_question_action_response_from_bytes,
    read_notifications_snapshot_with_options, read_pending_action_store,
    resolve_pending_action_prefix, ActionResultWire, HitlActionRequestWire,
    MobileActionStateWire, NotificationStateUpdateWire,
    NotificationStoreSnapshotWire, NotificationWire,
    PendingActionPrefixResolutionWire, PendingActionStoreWire,
    PlanActionRequestWire, QuestionActionRequestWire,
};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

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

    pub fn execute_plan_action(
        &self,
        request: &PlanActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.0.execute_plan_action(request)
    }

    pub fn execute_hitl_action(
        &self,
        request: &HitlActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        self.0.execute_hitl_action(request)
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

    fn execute_plan_action(
        &self,
        _request: &PlanActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "plan action mutations are not supported by this bridge"
                .to_string(),
        ))
    }

    fn execute_hitl_action(
        &self,
        _request: &HitlActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        Err(HostBridgeError::UnsupportedAction(
            "HITL action mutations are not supported by this bridge"
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
    notifications_path: PathBuf,
    pending_actions_path: PathBuf,
    legacy_telegram_pending_actions_path: PathBuf,
}

impl LocalJsonlNotificationBridge {
    pub fn new(sase_home: impl AsRef<Path>) -> Self {
        Self {
            notifications_path: sase_home
                .as_ref()
                .join("notifications")
                .join("notifications.jsonl"),
            pending_actions_path: pending_action_store_path(sase_home.as_ref()),
            legacy_telegram_pending_actions_path:
                legacy_telegram_pending_actions_path(sase_home.as_ref()),
        }
    }

    fn resolve_action_notification(
        &self,
        prefix: &str,
        expected_action: &str,
    ) -> Result<(PendingActionStoreWire, NotificationWire), HostBridgeError>
    {
        let store = read_pending_action_store(
            &self.pending_actions_path,
            Some(&self.legacy_telegram_pending_actions_path),
        )
        .map_err(HostBridgeError::ReadPendingActions)?;
        let identity = resolve_pending_action_prefix(&store, prefix);
        match identity.resolution {
            PendingActionPrefixResolutionWire::Exact
            | PendingActionPrefixResolutionWire::UniquePrefix => {}
            PendingActionPrefixResolutionWire::Missing => {
                return Err(HostBridgeError::ActionMissing(prefix.to_string()));
            }
            PendingActionPrefixResolutionWire::AmbiguousPrefix
            | PendingActionPrefixResolutionWire::DuplicateFullId => {
                return Err(HostBridgeError::AmbiguousPrefix(
                    prefix.to_string(),
                ));
            }
        }

        let snapshot = self.list_notifications(true)?;
        let notification = snapshot
            .notifications
            .into_iter()
            .find(|row| row.id == identity.notification_id)
            .ok_or_else(|| {
                HostBridgeError::ActionMissing(identity.notification_id.clone())
            })?;
        if notification.action.as_deref() != Some(expected_action) {
            return Err(HostBridgeError::UnsupportedAction(
                notification
                    .action
                    .clone()
                    .unwrap_or_else(|| "non_action".to_string()),
            ));
        }
        ensure_action_available(&store, &notification)?;
        Ok((store, notification))
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

    fn execute_plan_action(
        &self,
        request: &PlanActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        let (_store, notification) =
            self.resolve_action_notification(&request.prefix, "PlanApproval")?;

        let response_dir = action_path(&notification, "response_dir")
            .filter(|path| path.is_dir())
            .ok_or_else(|| {
                HostBridgeError::MissingTarget("response_dir".into())
            })?;
        if !response_dir.join("plan_request.json").is_file() {
            return Err(HostBridgeError::ActionAlreadyHandled(
                notification.id.clone(),
            ));
        }
        if notification.files.is_empty() {
            return Err(HostBridgeError::MissingTarget("plan_file".into()));
        }

        let mut result = plan_plan_action_response(request).map_err(|err| {
            HostBridgeError::InvalidActionRequest(err.message)
        })?;
        result.notification_id = Some(notification.id.clone());
        result.state = MobileActionStateWire::Available;

        let response_path = response_dir.join(&result.response_file);
        write_response_file_once(&response_path, &result.response_json)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    HostBridgeError::ActionAlreadyHandled(
                        notification.id.clone(),
                    )
                } else {
                    HostBridgeError::WriteResponse(err.to_string())
                }
            })?;

        let _ = apply_notification_state_update(
            &self.notifications_path,
            &NotificationStateUpdateWire::MarkDismissed {
                id: notification.id.clone(),
            },
        );
        persist_plan_approval_metadata(&notification, request);
        Ok(result)
    }

    fn execute_hitl_action(
        &self,
        request: &HitlActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        let (_store, notification) =
            self.resolve_action_notification(&request.prefix, "HITL")?;
        let artifacts_dir = action_path(&notification, "artifacts_dir")
            .filter(|path| path.is_dir())
            .ok_or_else(|| {
                HostBridgeError::MissingTarget("artifacts_dir".into())
            })?;
        if !artifacts_dir.join("hitl_request.json").is_file() {
            return Err(HostBridgeError::ActionAlreadyHandled(
                notification.id.clone(),
            ));
        }

        let mut result = plan_hitl_action_response(request).map_err(|err| {
            HostBridgeError::InvalidActionRequest(err.message)
        })?;
        result.notification_id = Some(notification.id.clone());
        result.state = MobileActionStateWire::Available;

        let response_path = artifacts_dir.join(&result.response_file);
        write_response_file_once(&response_path, &result.response_json)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    HostBridgeError::ActionAlreadyHandled(
                        notification.id.clone(),
                    )
                } else {
                    HostBridgeError::WriteResponse(err.to_string())
                }
            })?;
        dismiss_notification_best_effort(
            &self.notifications_path,
            &notification,
        );
        Ok(result)
    }

    fn execute_question_action(
        &self,
        request: &QuestionActionRequestWire,
    ) -> Result<ActionResultWire, HostBridgeError> {
        let (_store, notification) =
            self.resolve_action_notification(&request.prefix, "UserQuestion")?;
        let response_dir = action_path(&notification, "response_dir")
            .filter(|path| path.is_dir())
            .ok_or_else(|| {
                HostBridgeError::MissingTarget("response_dir".into())
            })?;
        let request_path = response_dir.join("question_request.json");
        let request_bytes = std::fs::read(&request_path).map_err(|err| {
            if err.kind() == std::io::ErrorKind::NotFound {
                HostBridgeError::ActionAlreadyHandled(notification.id.clone())
            } else {
                HostBridgeError::InvalidActionRequest(format!(
                    "failed to read question_request.json: {err}"
                ))
            }
        })?;

        let mut result =
            plan_question_action_response_from_bytes(request, &request_bytes)
                .map_err(|err| {
                HostBridgeError::InvalidActionRequest(err.message)
            })?;
        result.notification_id = Some(notification.id.clone());
        result.state = MobileActionStateWire::Available;

        let response_path = response_dir.join(&result.response_file);
        write_response_file_once(&response_path, &result.response_json)
            .map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    HostBridgeError::ActionAlreadyHandled(
                        notification.id.clone(),
                    )
                } else {
                    HostBridgeError::WriteResponse(err.to_string())
                }
            })?;
        dismiss_notification_best_effort(
            &self.notifications_path,
            &notification,
        );
        Ok(result)
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

fn ensure_action_available(
    store: &PendingActionStoreWire,
    notification: &NotificationWire,
) -> Result<(), HostBridgeError> {
    let state = pending_action_state_from_store(
        store,
        notification,
        current_unix_time(),
    );
    match state {
        MobileActionStateWire::Available => Ok(()),
        MobileActionStateWire::AlreadyHandled => Err(
            HostBridgeError::ActionAlreadyHandled(notification.id.clone()),
        ),
        MobileActionStateWire::Stale => {
            Err(HostBridgeError::ActionStale(notification.id.clone()))
        }
        MobileActionStateWire::MissingRequest
        | MobileActionStateWire::MissingTarget => {
            Err(HostBridgeError::MissingTarget(notification.id.clone()))
        }
        MobileActionStateWire::Unsupported => {
            Err(HostBridgeError::UnsupportedAction(
                notification
                    .action
                    .clone()
                    .unwrap_or_else(|| "non_action".to_string()),
            ))
        }
    }
}

fn dismiss_notification_best_effort(
    notifications_path: &Path,
    notification: &NotificationWire,
) {
    let _ = apply_notification_state_update(
        notifications_path,
        &NotificationStateUpdateWire::MarkDismissed {
            id: notification.id.clone(),
        },
    );
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

#[derive(Debug, Error)]
pub enum HostBridgeError {
    #[error("agent bridge is unavailable: {0}")]
    BridgeUnavailable(String),
    #[error("agent not found: {0}")]
    AgentNotFound(String),
    #[error("agent is not running: {0}")]
    AgentNotRunning(String),
    #[error("agent launch failed: {0}")]
    LaunchFailed(String),
    #[error("invalid image upload: {0}")]
    InvalidUpload(String),
    #[error("permission denied: {0}")]
    PermissionDenied(String),
    #[error("failed to read notifications: {0}")]
    ReadNotifications(String),
    #[error("notification not found: {0}")]
    NotificationMissing(String),
    #[error("failed to read pending actions: {0}")]
    ReadPendingActions(String),
    #[error("action prefix not found: {0}")]
    ActionMissing(String),
    #[error("action prefix is ambiguous: {0}")]
    AmbiguousPrefix(String),
    #[error("unsupported action: {0}")]
    UnsupportedAction(String),
    #[error("action already handled: {0}")]
    ActionAlreadyHandled(String),
    #[error("action is stale: {0}")]
    ActionStale(String),
    #[error("missing action target: {0}")]
    MissingTarget(String),
    #[error("invalid action request: {0}")]
    InvalidActionRequest(String),
    #[error("failed to write response: {0}")]
    WriteResponse(String),
}

pub fn split_command_words(raw: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut chars = raw.chars().peekable();
    let mut quote: Option<char> = None;

    while let Some(ch) = chars.next() {
        match (quote, ch) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), '\\') => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            (Some(_), c) => current.push(c),
            (None, '\'' | '"') => quote = Some(ch),
            (None, '\\') => {
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            (None, c) if c.is_whitespace() => {
                if !current.is_empty() {
                    words.push(std::mem::take(&mut current));
                }
            }
            (None, c) => current.push(c),
        }
    }

    if let Some(q) = quote {
        return Err(format!("unterminated quote {q}"));
    }
    if !current.is_empty() {
        words.push(current);
    }
    Ok(words)
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

fn action_path(notification: &NotificationWire, key: &str) -> Option<PathBuf> {
    let raw = notification.action_data.get(key)?.trim();
    if raw.is_empty() {
        return None;
    }
    Some(expand_home_path(raw))
}

fn write_response_file_once(
    path: &Path,
    value: &JsonValue,
) -> std::io::Result<()> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "response path has no parent",
        )
    })?;
    std::fs::create_dir_all(parent)?;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    serde_json::to_writer_pretty(&mut file, value)
        .map_err(std::io::Error::other)?;
    use std::io::Write;
    file.write_all(b"\n")?;
    file.flush()
}

fn persist_plan_approval_metadata(
    notification: &NotificationWire,
    request: &PlanActionRequestWire,
) {
    let Some(response_dir) = action_path(notification, "response_dir") else {
        return;
    };
    let Some(parent) = response_dir.parent() else {
        return;
    };
    let meta_path = parent.join("agent_meta.json");
    let mut meta = match std::fs::read_to_string(&meta_path)
        .ok()
        .and_then(|content| serde_json::from_str::<JsonValue>(&content).ok())
    {
        Some(JsonValue::Object(object)) => object,
        _ => serde_json::Map::new(),
    };
    let action = match request.choice {
        sase_core::notifications::PlanActionChoiceWire::Approve => {
            if request.commit_plan.unwrap_or(true)
                && !request.run_coder.unwrap_or(true)
            {
                "commit"
            } else {
                "approve"
            }
        }
        sase_core::notifications::PlanActionChoiceWire::Epic => "epic",
        sase_core::notifications::PlanActionChoiceWire::Legend => "legend",
        _ => return,
    };
    meta.insert("plan_approved".to_string(), JsonValue::Bool(true));
    meta.insert(
        "plan_action".to_string(),
        JsonValue::String(action.to_string()),
    );
    if let Ok(mut file) = std::fs::File::create(&meta_path) {
        let _ =
            serde_json::to_writer_pretty(&mut file, &JsonValue::Object(meta));
        use std::io::Write;
        let _ = file.write_all(b"\n");
    }
}

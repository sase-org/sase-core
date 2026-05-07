//! Shared host helper bridge plumbing for non-Python frontends.
//!
//! The command-backed bridge performs blocking subprocess I/O. Async callers,
//! including the xprompt LSP, should run calls on a blocking worker and apply
//! their own timeout policy around that task.

use std::{
    fmt,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
    sync::Arc,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone)]
pub struct DynHelperHostBridge(Arc<dyn HelperHostBridge>);

impl DynHelperHostBridge {
    pub fn new(bridge: Arc<dyn HelperHostBridge>) -> Self {
        Self(bridge)
    }

    pub fn list_changespec_tags(
        &self,
        request: &MobileChangeSpecTagListRequestWire,
    ) -> Result<MobileChangeSpecTagListResponseWire, HostBridgeError> {
        self.0.list_changespec_tags(request)
    }

    pub fn xprompt_catalog(
        &self,
        request: &MobileXpromptCatalogRequestWire,
    ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
        self.0.xprompt_catalog(request)
    }

    pub fn list_beads(
        &self,
        request: &MobileBeadListRequestWire,
    ) -> Result<MobileBeadListResponseWire, HostBridgeError> {
        self.0.list_beads(request)
    }

    pub fn show_bead(
        &self,
        request: &MobileBeadShowRequestWire,
    ) -> Result<MobileBeadShowResponseWire, HostBridgeError> {
        self.0.show_bead(request)
    }

    pub fn update_start(
        &self,
        request: &MobileUpdateStartRequestWire,
    ) -> Result<MobileUpdateStartResponseWire, HostBridgeError> {
        self.0.update_start(request)
    }

    pub fn update_status(
        &self,
        request: &MobileUpdateStatusRequestWire,
    ) -> Result<MobileUpdateStatusResponseWire, HostBridgeError> {
        self.0.update_status(request)
    }
}

impl fmt::Debug for DynHelperHostBridge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynHelperHostBridge")
            .finish_non_exhaustive()
    }
}

pub trait HelperHostBridge: Send + Sync {
    fn list_changespec_tags(
        &self,
        _request: &MobileChangeSpecTagListRequestWire,
    ) -> Result<MobileChangeSpecTagListResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }

    fn xprompt_catalog(
        &self,
        _request: &MobileXpromptCatalogRequestWire,
    ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }

    fn list_beads(
        &self,
        _request: &MobileBeadListRequestWire,
    ) -> Result<MobileBeadListResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }

    fn show_bead(
        &self,
        _request: &MobileBeadShowRequestWire,
    ) -> Result<MobileBeadShowResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }

    fn update_start(
        &self,
        _request: &MobileUpdateStartRequestWire,
    ) -> Result<MobileUpdateStartResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }

    fn update_status(
        &self,
        _request: &MobileUpdateStatusRequestWire,
    ) -> Result<MobileUpdateStatusResponseWire, HostBridgeError> {
        Err(HostBridgeError::BridgeUnavailable(
            "helper_bridge".to_string(),
        ))
    }
}

#[derive(Debug, Default)]
pub struct UnavailableHelperHostBridge;

impl HelperHostBridge for UnavailableHelperHostBridge {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandHelperHostBridge {
    command: Vec<String>,
    sase_home: Option<PathBuf>,
}

impl CommandHelperHostBridge {
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
        std::env::var("SASE_MOBILE_HELPER_BRIDGE_COMMAND")
            .ok()
            .and_then(|raw| split_command_words(&raw).ok())
            .filter(|parts| !parts.is_empty())
            .or_else(|| {
                std::env::var("SASE_MOBILE_AGENT_BRIDGE_COMMAND")
                    .ok()
                    .and_then(|raw| split_command_words(&raw).ok())
                    .filter(|parts| !parts.is_empty())
            })
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
                HostBridgeError::BridgeUnavailable("helper_bridge".to_string())
            })?;
        let mut command = Command::new(program);
        command
            .args(fixed_args)
            .args(["mobile", "helper-bridge", operation])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(sase_home) = &self.sase_home {
            command.env("SASE_HOME", sase_home);
        }
        let mut child = command.spawn().map_err(|_| {
            HostBridgeError::BridgeUnavailable("helper_bridge".to_string())
        })?;

        {
            let Some(mut stdin) = child.stdin.take() else {
                return Err(HostBridgeError::BridgeUnavailable(format!(
                    "helper_bridge:{operation}:stdin"
                )));
            };
            serde_json::to_writer(&mut stdin, request).map_err(|_| {
                HostBridgeError::BridgeUnavailable(format!(
                    "helper_bridge:{operation}:encode"
                ))
            })?;
            stdin.write_all(b"\n").map_err(|_| {
                HostBridgeError::BridgeUnavailable(format!(
                    "helper_bridge:{operation}:stdin"
                ))
            })?;
        }

        let output = child.wait_with_output().map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "helper_bridge:{operation}:exit"
            ))
        })?;
        if !output.status.success() {
            if output.status.code() == Some(4) {
                if operation == "update-start" {
                    return Err(HostBridgeError::UpdateAlreadyRunning(
                        "update".to_string(),
                    ));
                }
                if operation == "update-status" {
                    return Err(HostBridgeError::UpdateJobNotFound(
                        "update".to_string(),
                    ));
                }
                return Err(HostBridgeError::HelperNotFound(format!(
                    "helper_bridge:{operation}"
                )));
            }
            return Err(HostBridgeError::BridgeUnavailable(format!(
                "helper_bridge:{operation}"
            )));
        }
        serde_json::from_slice(&output.stdout).map_err(|_| {
            HostBridgeError::BridgeUnavailable(format!(
                "helper_bridge:{operation}:invalid_json"
            ))
        })
    }
}

impl HelperHostBridge for CommandHelperHostBridge {
    fn list_changespec_tags(
        &self,
        request: &MobileChangeSpecTagListRequestWire,
    ) -> Result<MobileChangeSpecTagListResponseWire, HostBridgeError> {
        self.invoke("changespec-tags", request)
    }

    fn xprompt_catalog(
        &self,
        request: &MobileXpromptCatalogRequestWire,
    ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
        self.invoke("xprompt-catalog", request)
    }

    fn list_beads(
        &self,
        request: &MobileBeadListRequestWire,
    ) -> Result<MobileBeadListResponseWire, HostBridgeError> {
        self.invoke("beads-list", request)
    }

    fn show_bead(
        &self,
        request: &MobileBeadShowRequestWire,
    ) -> Result<MobileBeadShowResponseWire, HostBridgeError> {
        self.invoke("beads-show", request)
    }

    fn update_start(
        &self,
        request: &MobileUpdateStartRequestWire,
    ) -> Result<MobileUpdateStartResponseWire, HostBridgeError> {
        self.invoke("update-start", request)
    }

    fn update_status(
        &self,
        request: &MobileUpdateStatusRequestWire,
    ) -> Result<MobileUpdateStatusResponseWire, HostBridgeError> {
        self.invoke("update-status", request)
    }
}

#[derive(Debug, Clone)]
pub struct StaticHelperHostBridge {
    pub changespec_tags_response: MobileChangeSpecTagListResponseWire,
    pub xprompt_catalog_response: MobileXpromptCatalogResponseWire,
    pub bead_list_response: MobileBeadListResponseWire,
    pub bead_show_response: MobileBeadShowResponseWire,
    pub update_start_response: MobileUpdateStartResponseWire,
    pub update_status_response: MobileUpdateStatusResponseWire,
}

impl HelperHostBridge for StaticHelperHostBridge {
    fn list_changespec_tags(
        &self,
        _request: &MobileChangeSpecTagListRequestWire,
    ) -> Result<MobileChangeSpecTagListResponseWire, HostBridgeError> {
        Ok(self.changespec_tags_response.clone())
    }

    fn xprompt_catalog(
        &self,
        _request: &MobileXpromptCatalogRequestWire,
    ) -> Result<MobileXpromptCatalogResponseWire, HostBridgeError> {
        Ok(self.xprompt_catalog_response.clone())
    }

    fn list_beads(
        &self,
        _request: &MobileBeadListRequestWire,
    ) -> Result<MobileBeadListResponseWire, HostBridgeError> {
        Ok(self.bead_list_response.clone())
    }

    fn show_bead(
        &self,
        _request: &MobileBeadShowRequestWire,
    ) -> Result<MobileBeadShowResponseWire, HostBridgeError> {
        Ok(self.bead_show_response.clone())
    }

    fn update_start(
        &self,
        _request: &MobileUpdateStartRequestWire,
    ) -> Result<MobileUpdateStartResponseWire, HostBridgeError> {
        Ok(self.update_start_response.clone())
    }

    fn update_status(
        &self,
        _request: &MobileUpdateStatusRequestWire,
    ) -> Result<MobileUpdateStatusResponseWire, HostBridgeError> {
        Ok(self.update_status_response.clone())
    }
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
    #[error("helper result not found: {0}")]
    HelperNotFound(String),
    #[error("update is already running: {0}")]
    UpdateAlreadyRunning(String),
    #[error("update job not found: {0}")]
    UpdateJobNotFound(String),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileHelperResultWire {
    pub status: MobileHelperStatusWire,
    pub message: Option<String>,
    pub warnings: Vec<String>,
    pub skipped: Vec<MobileHelperSkippedWire>,
    pub partial_failure_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileHelperStatusWire {
    Success,
    PartialSuccess,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileHelperSkippedWire {
    pub target: Option<String>,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileHelperProjectContextWire {
    pub project: Option<String>,
    pub scope: MobileHelperProjectScopeWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileHelperProjectScopeWire {
    Explicit,
    DeviceDefault,
    AllKnown,
    Unspecified,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileChangeSpecTagListRequestWire {
    pub schema_version: u32,
    pub project: Option<String>,
    pub limit: Option<u32>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileChangeSpecTagListResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub context: MobileHelperProjectContextWire,
    pub tags: Vec<MobileChangeSpecTagEntryWire>,
    pub total_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileChangeSpecTagEntryWire {
    pub tag: String,
    pub project: Option<String>,
    pub changespec: String,
    pub title: Option<String>,
    pub status: String,
    pub workflow: Option<String>,
    pub source_path_display: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptCatalogRequestWire {
    pub schema_version: u32,
    pub project: Option<String>,
    pub source: Option<String>,
    pub tag: Option<String>,
    pub query: Option<String>,
    pub include_pdf: bool,
    pub limit: Option<u32>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptCatalogResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub context: MobileHelperProjectContextWire,
    pub entries: Vec<MobileXpromptCatalogEntryWire>,
    pub stats: MobileXpromptCatalogStatsWire,
    pub catalog_attachment: Option<MobileXpromptCatalogAttachmentWire>,
}

pub type EditorXpromptCatalogRequestWire = MobileXpromptCatalogRequestWire;
pub type EditorXpromptCatalogResponseWire = MobileXpromptCatalogResponseWire;
pub type EditorXpromptCatalogEntryWire = MobileXpromptCatalogEntryWire;
pub type EditorXpromptInputWire = MobileXpromptInputWire;
pub type EditorXpromptCatalogStatsWire = MobileXpromptCatalogStatsWire;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptCatalogEntryWire {
    pub name: String,
    pub display_label: String,
    pub insertion: Option<String>,
    pub reference_prefix: Option<String>,
    pub kind: Option<String>,
    pub description: Option<String>,
    pub source_bucket: String,
    pub project: Option<String>,
    pub tags: Vec<String>,
    pub input_signature: Option<String>,
    #[serde(default)]
    pub inputs: Vec<MobileXpromptInputWire>,
    pub is_skill: bool,
    pub content_preview: Option<String>,
    pub source_path_display: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptInputWire {
    pub name: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub required: bool,
    pub default_display: Option<String>,
    pub position: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptCatalogStatsWire {
    pub total_count: u64,
    pub project_count: u64,
    pub skill_count: u64,
    pub pdf_requested: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileXpromptCatalogAttachmentWire {
    pub display_name: String,
    pub content_type: Option<String>,
    pub byte_size: Option<u64>,
    pub path_display: Option<String>,
    pub generated: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadListRequestWire {
    pub schema_version: u32,
    pub project: Option<String>,
    pub all_projects: bool,
    pub status: Option<String>,
    pub bead_type: Option<String>,
    pub tier: Option<String>,
    pub include_closed: bool,
    pub limit: Option<u32>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadListResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub context: MobileHelperProjectContextWire,
    pub beads: Vec<MobileBeadSummaryWire>,
    pub total_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadShowRequestWire {
    pub schema_version: u32,
    pub bead_id: String,
    pub project: Option<String>,
    pub all_projects: bool,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadShowResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub context: MobileHelperProjectContextWire,
    pub bead: MobileBeadDetailWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadSummaryWire {
    pub id: String,
    pub title: String,
    pub status: String,
    pub bead_type: String,
    pub tier: Option<String>,
    pub project: Option<String>,
    pub parent_id: Option<String>,
    pub assignee: Option<String>,
    pub updated_at: Option<String>,
    pub dependency_count: u64,
    pub block_count: u64,
    pub child_count: u64,
    pub plan_path_display: Option<String>,
    pub changespec_name: Option<String>,
    pub changespec_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileBeadDetailWire {
    pub summary: MobileBeadSummaryWire,
    pub description: Option<String>,
    pub notes: Option<String>,
    pub design_path_display: Option<String>,
    pub dependencies: Vec<String>,
    pub blocks: Vec<String>,
    pub children: Vec<String>,
    pub workspace_display: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileUpdateStartRequestWire {
    pub schema_version: u32,
    pub request_id: Option<String>,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileUpdateStartResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub job: MobileUpdateJobWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileUpdateStatusRequestWire {
    pub schema_version: u32,
    pub job_id: String,
    pub device_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileUpdateStatusResponseWire {
    pub schema_version: u32,
    pub result: MobileHelperResultWire,
    pub job: MobileUpdateJobWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileUpdateJobWire {
    pub job_id: String,
    pub status: MobileUpdateJobStatusWire,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub message: Option<String>,
    pub log_path_display: Option<String>,
    pub completion_path_display: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileUpdateJobStatusWire {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;

    fn helper_result() -> MobileHelperResultWire {
        MobileHelperResultWire {
            status: MobileHelperStatusWire::Success,
            message: None,
            warnings: Vec::new(),
            skipped: Vec::new(),
            partial_failure_count: None,
        }
    }

    fn helper_context() -> MobileHelperProjectContextWire {
        MobileHelperProjectContextWire {
            project: Some("sase".to_string()),
            scope: MobileHelperProjectScopeWire::Explicit,
        }
    }

    fn empty_bead_summary() -> MobileBeadSummaryWire {
        MobileBeadSummaryWire {
            id: "sase-1".to_string(),
            title: "Example".to_string(),
            status: "open".to_string(),
            bead_type: "phase".to_string(),
            tier: None,
            project: Some("sase".to_string()),
            parent_id: None,
            assignee: None,
            updated_at: None,
            dependency_count: 0,
            block_count: 0,
            child_count: 0,
            plan_path_display: None,
            changespec_name: None,
            changespec_status: None,
        }
    }

    fn static_bridge() -> StaticHelperHostBridge {
        StaticHelperHostBridge {
            changespec_tags_response: MobileChangeSpecTagListResponseWire {
                schema_version: 1,
                result: helper_result(),
                context: helper_context(),
                tags: Vec::new(),
                total_count: 0,
            },
            xprompt_catalog_response: MobileXpromptCatalogResponseWire {
                schema_version: 1,
                result: helper_result(),
                context: helper_context(),
                entries: vec![MobileXpromptCatalogEntryWire {
                    name: "bd/work_phase_bead".to_string(),
                    display_label: "Work phase bead".to_string(),
                    insertion: Some("#bd/work_phase_bead".to_string()),
                    reference_prefix: Some("#".to_string()),
                    kind: Some("prompt".to_string()),
                    description: Some(
                        "Work an assigned phase bead".to_string(),
                    ),
                    source_bucket: "builtin".to_string(),
                    project: None,
                    tags: vec!["beads".to_string()],
                    input_signature: Some("bead_id".to_string()),
                    inputs: vec![MobileXpromptInputWire {
                        name: "bead_id".to_string(),
                        r#type: "word".to_string(),
                        required: true,
                        default_display: None,
                        position: 0,
                    }],
                    is_skill: false,
                    content_preview: Some("Complete the bead".to_string()),
                    source_path_display: Some(
                        "xprompts/bd/work_phase_bead.md".to_string(),
                    ),
                }],
                stats: MobileXpromptCatalogStatsWire {
                    total_count: 1,
                    project_count: 0,
                    skill_count: 0,
                    pdf_requested: false,
                },
                catalog_attachment: None,
            },
            bead_list_response: MobileBeadListResponseWire {
                schema_version: 1,
                result: helper_result(),
                context: helper_context(),
                beads: Vec::new(),
                total_count: 0,
            },
            bead_show_response: MobileBeadShowResponseWire {
                schema_version: 1,
                result: helper_result(),
                context: helper_context(),
                bead: MobileBeadDetailWire {
                    summary: empty_bead_summary(),
                    description: None,
                    notes: None,
                    design_path_display: None,
                    dependencies: Vec::new(),
                    blocks: Vec::new(),
                    children: Vec::new(),
                    workspace_display: None,
                },
            },
            update_start_response: MobileUpdateStartResponseWire {
                schema_version: 1,
                result: helper_result(),
                job: MobileUpdateJobWire {
                    job_id: "job_1".to_string(),
                    status: MobileUpdateJobStatusWire::Running,
                    started_at: None,
                    finished_at: None,
                    message: None,
                    log_path_display: None,
                    completion_path_display: None,
                },
            },
            update_status_response: MobileUpdateStatusResponseWire {
                schema_version: 1,
                result: helper_result(),
                job: MobileUpdateJobWire {
                    job_id: "job_1".to_string(),
                    status: MobileUpdateJobStatusWire::Succeeded,
                    started_at: None,
                    finished_at: None,
                    message: None,
                    log_path_display: None,
                    completion_path_display: None,
                },
            },
        }
    }

    #[test]
    fn static_helper_bridge_returns_structured_catalog_response() {
        let bridge = DynHelperHostBridge::new(Arc::new(static_bridge()));
        let response = bridge
            .xprompt_catalog(&MobileXpromptCatalogRequestWire {
                schema_version: 1,
                project: Some("sase".to_string()),
                source: None,
                tag: None,
                query: Some("phase".to_string()),
                include_pdf: false,
                limit: Some(20),
                device_id: None,
            })
            .unwrap();

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "schema_version": 1,
                "result": {
                    "status": "success",
                    "message": null,
                    "warnings": [],
                    "skipped": [],
                    "partial_failure_count": null
                },
                "context": {
                    "project": "sase",
                    "scope": "explicit"
                },
                "entries": [{
                    "name": "bd/work_phase_bead",
                    "display_label": "Work phase bead",
                    "insertion": "#bd/work_phase_bead",
                    "reference_prefix": "#",
                    "kind": "prompt",
                    "description": "Work an assigned phase bead",
                    "source_bucket": "builtin",
                    "project": null,
                    "tags": ["beads"],
                    "input_signature": "bead_id",
                    "inputs": [{
                        "name": "bead_id",
                        "type": "word",
                        "required": true,
                        "default_display": null,
                        "position": 0
                    }],
                    "is_skill": false,
                    "content_preview": "Complete the bead",
                    "source_path_display": "xprompts/bd/work_phase_bead.md"
                }],
                "stats": {
                    "total_count": 1,
                    "project_count": 0,
                    "skill_count": 0,
                    "pdf_requested": false
                },
                "catalog_attachment": null
            })
        );
    }
}

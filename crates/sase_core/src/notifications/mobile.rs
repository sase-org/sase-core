use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use super::wire::NotificationWire;

pub const MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION: u32 = 4;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileNotificationListRequestWire {
    pub schema_version: u32,
    #[serde(default)]
    pub unread_only: bool,
    #[serde(default)]
    pub include_dismissed: bool,
    #[serde(default)]
    pub include_silent: bool,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub newer_than: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileNotificationListResponseWire {
    pub schema_version: u32,
    pub notifications: Vec<MobileNotificationCardWire>,
    pub total_count: u64,
    pub next_high_water: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileNotificationDetailResponseWire {
    pub schema_version: u32,
    pub notification: MobileNotificationCardWire,
    pub notes: Vec<String>,
    pub attachments: Vec<MobileAttachmentManifestWire>,
    pub action: MobileActionDetailWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileNotificationCardWire {
    pub id: String,
    pub timestamp: String,
    pub sender: String,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub priority: bool,
    pub actionable: bool,
    pub read: bool,
    pub dismissed: bool,
    pub silent: bool,
    pub muted: bool,
    pub notes_summary: String,
    pub file_count: u64,
    pub action_summary: Option<MobileActionSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileActionSummaryWire {
    pub kind: MobileActionKindWire,
    pub state: MobileActionStateWire,
    pub label: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingActionIdentityWire {
    pub notification_id: String,
    pub prefix: String,
    pub prefix_len: u32,
    pub resolution: PendingActionPrefixResolutionWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingActionPrefixResolutionWire {
    Exact,
    UniquePrefix,
    AmbiguousPrefix,
    DuplicateFullId,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileActionKindWire {
    PlanApproval,
    EpicApproval,
    Hitl,
    UserQuestion,
    LaunchApproval,
    CustomGate,
    NonAction,
    Unsupported,
}

impl MobileActionKindWire {
    pub fn from_notification_action(action: Option<&str>) -> Self {
        match action {
            Some("PlanApproval") => Self::PlanApproval,
            Some("EpicApproval") => Self::EpicApproval,
            Some("HITL") => Self::Hitl,
            Some("UserQuestion") => Self::UserQuestion,
            Some("LaunchApproval") => Self::LaunchApproval,
            Some("CustomGate") => Self::CustomGate,
            None => Self::NonAction,
            Some(_) => Self::Unsupported,
        }
    }

    pub const fn notification_action(self) -> Option<&'static str> {
        match self {
            Self::PlanApproval => Some("PlanApproval"),
            Self::EpicApproval => Some("EpicApproval"),
            Self::Hitl => Some("HITL"),
            Self::UserQuestion => Some("UserQuestion"),
            Self::LaunchApproval => Some("LaunchApproval"),
            Self::CustomGate => Some("CustomGate"),
            Self::NonAction | Self::Unsupported => None,
        }
    }

    pub const fn is_gate(self) -> bool {
        matches!(
            self,
            Self::PlanApproval
                | Self::EpicApproval
                | Self::Hitl
                | Self::UserQuestion
                | Self::LaunchApproval
                | Self::CustomGate
        )
    }

    pub const fn is_priority_gate(self) -> bool {
        matches!(
            self,
            Self::PlanApproval
                | Self::EpicApproval
                | Self::UserQuestion
                | Self::LaunchApproval
        )
    }

    pub const fn is_agent_dismissable_gate(self) -> bool {
        matches!(
            self,
            Self::PlanApproval
                | Self::EpicApproval
                | Self::UserQuestion
                | Self::LaunchApproval
                | Self::CustomGate
        )
    }

    pub const fn label(self) -> &'static str {
        match self {
            Self::PlanApproval => "Plan approval",
            Self::EpicApproval => "Epic approval",
            Self::Hitl => "HITL",
            Self::UserQuestion => "Question",
            Self::LaunchApproval => "Launch approval",
            Self::CustomGate => "Custom gate",
            Self::NonAction => "Notification",
            Self::Unsupported => "Unsupported action",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileActionStateWire {
    Available,
    AlreadyHandled,
    Stale,
    MissingRequest,
    MissingTarget,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MobileActionDetailWire {
    PlanApproval {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        response_dir: Option<String>,
        plan_file: Option<String>,
        branches: Vec<GateBranchWire>,
    },
    EpicApproval {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        response_dir: Option<String>,
        plan_file: Option<String>,
        branches: Vec<GateBranchWire>,
    },
    Hitl {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        artifacts_dir: Option<String>,
        workflow_name: Option<String>,
        branches: Vec<GateBranchWire>,
    },
    UserQuestion {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        response_dir: Option<String>,
        question_count: u32,
        choices: Vec<QuestionActionChoiceWire>,
    },
    LaunchApproval {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        response_dir: Option<String>,
        request_id: Option<String>,
        source_surface: Option<String>,
        slot_count: u32,
        branches: Vec<GateBranchWire>,
    },
    CustomGate {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        request_id: Option<String>,
        branches: Vec<GateBranchWire>,
    },
    NonAction {
        state: MobileActionStateWire,
    },
    Unsupported {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        action: Option<String>,
    },
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum GateFeedbackModeWire {
    Disabled,
    #[default]
    Optional,
    Required,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateOptionWire {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub default_selected: bool,
    #[serde(default)]
    pub feedback: GateFeedbackModeWire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateSubmitWire {
    pub label: String,
    #[serde(default)]
    pub icon: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateBranchWire {
    pub options: Vec<GateOptionWire>,
    #[serde(default)]
    pub submit: Option<GateSubmitWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
struct GateGroupEnvelopeWire {
    options: Vec<String>,
    #[serde(default)]
    label: Option<String>,
    #[serde(default)]
    icon: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GateEnvelopeWire {
    schema_version: u32,
    #[serde(default)]
    options: Vec<GateOptionWire>,
    #[serde(default)]
    groups: Vec<GateGroupEnvelopeWire>,
    #[serde(default)]
    branches: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileAttachmentManifestWire {
    pub id: String,
    pub token: Option<String>,
    pub display_name: String,
    pub kind: MobileAttachmentKindWire,
    pub content_type: Option<String>,
    pub byte_size: Option<u64>,
    pub source_notification_id: String,
    pub downloadable: bool,
    pub download_requires_auth: bool,
    pub can_inline: bool,
    pub path_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileAttachmentKindWire {
    Markdown,
    Pdf,
    Diff,
    Image,
    Text,
    Json,
    Directory,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateActionRequestWire {
    pub schema_version: u32,
    pub prefix: String,
    pub selected_option_ids: Vec<String>,
    #[serde(default)]
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuestionActionChoiceWire {
    Answer,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuestionActionRequestWire {
    pub schema_version: u32,
    pub prefix: String,
    pub choice: QuestionActionChoiceWire,
    #[serde(default)]
    pub question_index: Option<u32>,
    #[serde(default)]
    pub selected_option_id: Option<String>,
    #[serde(default)]
    pub selected_option_label: Option<String>,
    #[serde(default)]
    pub selected_option_index: Option<u32>,
    #[serde(default)]
    pub custom_answer: Option<String>,
    #[serde(default)]
    pub global_note: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionResultWire {
    pub schema_version: u32,
    pub action_kind: MobileActionKindWire,
    pub prefix: String,
    pub notification_id: Option<String>,
    pub state: MobileActionStateWire,
    pub response_file: String,
    pub response_json: JsonValue,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MobileActionPlanErrorWire {
    pub code: MobileActionPlanErrorCodeWire,
    pub message: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MobileActionPlanErrorCodeWire {
    InvalidRequest,
    UnsupportedAction,
    MalformedQuestionRequest,
    InvalidQuestionIndex,
    InvalidOption,
}

pub fn pending_action_identity(
    notification_id: &str,
    prefix_len: usize,
) -> PendingActionIdentityWire {
    let safe_len = prefix_len.min(notification_id.len());
    PendingActionIdentityWire {
        notification_id: notification_id.to_string(),
        prefix: notification_id[..safe_len].to_string(),
        prefix_len: safe_len as u32,
        resolution: PendingActionPrefixResolutionWire::UniquePrefix,
    }
}

pub fn resolve_notification_prefix<'a>(
    prefix: &str,
    notification_ids: impl IntoIterator<Item = &'a str>,
) -> PendingActionIdentityWire {
    let ids: Vec<&str> = notification_ids.into_iter().collect();
    let exact_count = ids.iter().filter(|id| **id == prefix).count();
    if exact_count == 1 {
        return PendingActionIdentityWire {
            notification_id: prefix.to_string(),
            prefix: prefix.to_string(),
            prefix_len: prefix.len() as u32,
            resolution: PendingActionPrefixResolutionWire::Exact,
        };
    }
    if exact_count > 1 {
        return PendingActionIdentityWire {
            notification_id: prefix.to_string(),
            prefix: prefix.to_string(),
            prefix_len: prefix.len() as u32,
            resolution: PendingActionPrefixResolutionWire::DuplicateFullId,
        };
    }

    let matches: Vec<&str> = ids
        .into_iter()
        .filter(|id| id.starts_with(prefix))
        .collect();
    match matches.as_slice() {
        [id] => PendingActionIdentityWire {
            notification_id: (*id).to_string(),
            prefix: prefix.to_string(),
            prefix_len: prefix.len() as u32,
            resolution: PendingActionPrefixResolutionWire::UniquePrefix,
        },
        [] => PendingActionIdentityWire {
            notification_id: String::new(),
            prefix: prefix.to_string(),
            prefix_len: prefix.len() as u32,
            resolution: PendingActionPrefixResolutionWire::Missing,
        },
        _ => PendingActionIdentityWire {
            notification_id: String::new(),
            prefix: prefix.to_string(),
            prefix_len: prefix.len() as u32,
            resolution: PendingActionPrefixResolutionWire::AmbiguousPrefix,
        },
    }
}

pub fn mobile_notification_card_from_wire(
    notification: &NotificationWire,
    state: MobileActionStateWire,
    priority: bool,
) -> MobileNotificationCardWire {
    let action_kind = MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    );
    let actionable =
        action_kind.is_gate() && state == MobileActionStateWire::Available;
    MobileNotificationCardWire {
        id: notification.id.clone(),
        timestamp: notification.timestamp.clone(),
        sender: notification.sender.clone(),
        icon: notification.icon.clone(),
        tags: notification.tags.clone(),
        priority,
        actionable,
        read: notification.read,
        dismissed: notification.dismissed,
        silent: notification.silent,
        muted: notification.muted,
        notes_summary: notification.notes.first().cloned().unwrap_or_default(),
        file_count: notification.files.len() as u64,
        action_summary: match action_kind {
            MobileActionKindWire::NonAction => None,
            _ => Some(MobileActionSummaryWire {
                kind: action_kind,
                state,
                label: action_kind.label().to_string(),
            }),
        },
    }
}

pub fn mobile_notification_error_from_wire(
    notification: &NotificationWire,
) -> bool {
    notification.action.as_deref() == Some("ViewErrorReport")
        && matches!(notification.sender.as_str(), "axe" | "user-agent")
}

pub fn mobile_notification_priority_from_wire(
    notification: &NotificationWire,
) -> bool {
    if mobile_notification_error_from_wire(notification) {
        return false;
    }
    MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    )
    .is_priority_gate()
        || notification.action.as_deref() == Some("JumpToMentorReview")
        || matches!(notification.sender.as_str(), "axe" | "crs")
}

pub fn mobile_action_detail_from_notification(
    notification: &NotificationWire,
    state: MobileActionStateWire,
) -> MobileActionDetailWire {
    let identity = pending_action_identity(&notification.id, 8);
    match MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    ) {
        MobileActionKindWire::PlanApproval => {
            MobileActionDetailWire::PlanApproval {
                identity,
                state,
                response_dir: notification
                    .action_data
                    .get("response_dir")
                    .cloned(),
                plan_file: notification.files.first().cloned(),
                branches: gate_branches_from_notification(notification),
            }
        }
        MobileActionKindWire::EpicApproval => {
            MobileActionDetailWire::EpicApproval {
                identity,
                state,
                response_dir: notification
                    .action_data
                    .get("response_dir")
                    .cloned(),
                plan_file: notification.files.first().cloned(),
                branches: gate_branches_from_notification(notification),
            }
        }
        MobileActionKindWire::Hitl => MobileActionDetailWire::Hitl {
            identity,
            state,
            artifacts_dir: notification
                .action_data
                .get("artifacts_dir")
                .cloned(),
            workflow_name: notification
                .action_data
                .get("workflow_name")
                .cloned(),
            branches: gate_branches_from_notification(notification),
        },
        MobileActionKindWire::UserQuestion => {
            MobileActionDetailWire::UserQuestion {
                identity,
                state,
                response_dir: notification
                    .action_data
                    .get("response_dir")
                    .cloned(),
                question_count: notification
                    .action_data
                    .get("question_count")
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or(0),
                choices: vec![
                    QuestionActionChoiceWire::Answer,
                    QuestionActionChoiceWire::Custom,
                ],
            }
        }
        MobileActionKindWire::LaunchApproval => {
            MobileActionDetailWire::LaunchApproval {
                identity,
                state,
                response_dir: notification
                    .action_data
                    .get("response_dir")
                    .cloned(),
                request_id: notification.action_data.get("request_id").cloned(),
                source_surface: notification
                    .action_data
                    .get("source_surface")
                    .cloned(),
                slot_count: notification
                    .action_data
                    .get("slot_count")
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or(0),
                branches: gate_branches_from_notification(notification),
            }
        }
        MobileActionKindWire::CustomGate => {
            MobileActionDetailWire::CustomGate {
                identity,
                state,
                request_id: notification.action_data.get("request_id").cloned(),
                branches: gate_branches_from_notification(notification),
            }
        }
        MobileActionKindWire::NonAction => MobileActionDetailWire::NonAction {
            state: MobileActionStateWire::Unsupported,
        },
        MobileActionKindWire::Unsupported => {
            MobileActionDetailWire::Unsupported {
                identity,
                state: MobileActionStateWire::Unsupported,
                action: notification.action.clone(),
            }
        }
    }
}

fn gate_branches_from_notification(
    notification: &NotificationWire,
) -> Vec<GateBranchWire> {
    let Some(request_path) = notification.action_data.get("request_path")
    else {
        return Vec::new();
    };
    let Ok(bytes) = std::fs::read(request_path) else {
        return Vec::new();
    };
    let Ok(envelope) = serde_json::from_slice::<GateEnvelopeWire>(&bytes)
    else {
        return Vec::new();
    };
    if envelope.schema_version != 2 {
        return Vec::new();
    }
    let options_by_id: BTreeMap<String, GateOptionWire> = envelope
        .options
        .into_iter()
        .map(|option| (option.id.clone(), option))
        .collect();
    let groups: BTreeMap<Vec<String>, GateGroupEnvelopeWire> = envelope
        .groups
        .into_iter()
        .map(|group| (group.options.clone(), group))
        .collect();
    envelope
        .branches
        .into_iter()
        .filter_map(|option_ids| {
            let options = option_ids
                .iter()
                .map(|option_id| options_by_id.get(option_id).cloned())
                .collect::<Option<Vec<_>>>()?;
            if options.is_empty() {
                return None;
            }
            let submit = if options.len() > 1 {
                let group = groups.get(&option_ids);
                let first = &options[0];
                Some(GateSubmitWire {
                    label: group
                        .and_then(|value| value.label.clone())
                        .unwrap_or_else(|| first.label.clone()),
                    icon: group
                        .and_then(|value| value.icon.clone())
                        .or_else(|| first.icon.clone()),
                })
            } else {
                None
            };
            Some(GateBranchWire { options, submit })
        })
        .collect()
}

pub fn mobile_attachment_manifest_from_path(
    source_notification_id: &str,
    index: usize,
    display_name: impl Into<String>,
    byte_size: Option<u64>,
    path_available: bool,
) -> MobileAttachmentManifestWire {
    let display_name = display_name.into();
    let (kind, content_type, can_inline) =
        classify_attachment_display_name(&display_name);
    MobileAttachmentManifestWire {
        id: format!("att_{index:03}"),
        token: None,
        display_name,
        kind,
        content_type,
        byte_size,
        source_notification_id: source_notification_id.to_string(),
        downloadable: path_available,
        download_requires_auth: true,
        can_inline,
        path_available,
    }
}

pub fn plan_question_action_response_from_bytes(
    request: &QuestionActionRequestWire,
    question_request_bytes: &[u8],
) -> Result<ActionResultWire, MobileActionPlanErrorWire> {
    let value: JsonValue = serde_json::from_slice(question_request_bytes)
        .map_err(|err| {
            plan_error(
                MobileActionPlanErrorCodeWire::MalformedQuestionRequest,
                format!("malformed question_request.json: {err}"),
                Some("question_request"),
            )
        })?;
    plan_question_action_response(request, &value)
}

pub fn plan_question_action_response(
    request: &QuestionActionRequestWire,
    question_request: &JsonValue,
) -> Result<ActionResultWire, MobileActionPlanErrorWire> {
    validate_schema(request.schema_version)?;
    let question_index = request.question_index.unwrap_or(0) as usize;
    let question = question_by_index(question_request, question_index)?;
    let question_text = question
        .get("question")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_string();
    let (selected, custom_feedback) = match request.choice {
        QuestionActionChoiceWire::Answer => (
            vec![resolve_question_option_label(request, question)?],
            None,
        ),
        QuestionActionChoiceWire::Custom => (
            Vec::new(),
            Some(request.custom_answer.clone().ok_or_else(|| {
                plan_error(
                    MobileActionPlanErrorCodeWire::InvalidRequest,
                    "custom question response requires custom_answer",
                    Some("custom_answer"),
                )
            })?),
        ),
    };

    Ok(ActionResultWire {
        schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
        action_kind: MobileActionKindWire::UserQuestion,
        prefix: request.prefix.clone(),
        notification_id: None,
        state: MobileActionStateWire::Available,
        response_file: "question_response.json".to_string(),
        response_json: json!({
            "answers": [{
                "question": question_text,
                "selected": selected,
                "custom_feedback": custom_feedback,
            }],
            "global_note": request.global_note.clone().unwrap_or_default(),
        }),
        message: Some("Question answered".to_string()),
    })
}

fn classify_attachment_display_name(
    display_name: &str,
) -> (MobileAttachmentKindWire, Option<String>, bool) {
    let lower = display_name.to_ascii_lowercase();
    if lower.ends_with(".md") || lower.ends_with(".markdown") {
        return (
            MobileAttachmentKindWire::Markdown,
            Some("text/markdown".to_string()),
            true,
        );
    }
    if lower.ends_with(".pdf") {
        return (
            MobileAttachmentKindWire::Pdf,
            Some("application/pdf".to_string()),
            true,
        );
    }
    if lower.ends_with(".diff") || lower.ends_with(".patch") {
        return (
            MobileAttachmentKindWire::Diff,
            Some("text/x-diff".to_string()),
            true,
        );
    }
    if lower.ends_with(".png") {
        return (
            MobileAttachmentKindWire::Image,
            Some("image/png".to_string()),
            true,
        );
    }
    if lower.ends_with(".jpg") || lower.ends_with(".jpeg") {
        return (
            MobileAttachmentKindWire::Image,
            Some("image/jpeg".to_string()),
            true,
        );
    }
    if lower.ends_with(".json") {
        return (
            MobileAttachmentKindWire::Json,
            Some("application/json".to_string()),
            true,
        );
    }
    if lower.ends_with(".txt") || lower.ends_with(".log") {
        return (
            MobileAttachmentKindWire::Text,
            Some("text/plain".to_string()),
            true,
        );
    }
    (MobileAttachmentKindWire::Unknown, None, false)
}

fn question_by_index(
    question_request: &JsonValue,
    index: usize,
) -> Result<&serde_json::Map<String, JsonValue>, MobileActionPlanErrorWire> {
    let questions = question_request
        .get("questions")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            plan_error(
                MobileActionPlanErrorCodeWire::MalformedQuestionRequest,
                "question_request.json missing questions array",
                Some("questions"),
            )
        })?;
    questions
        .get(index)
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            plan_error(
                MobileActionPlanErrorCodeWire::InvalidQuestionIndex,
                format!("question index {index} is not available"),
                Some("question_index"),
            )
        })
}

fn resolve_question_option_label(
    request: &QuestionActionRequestWire,
    question: &serde_json::Map<String, JsonValue>,
) -> Result<String, MobileActionPlanErrorWire> {
    if let Some(label) = &request.selected_option_label {
        return Ok(label.clone());
    }
    let options = question
        .get("options")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| {
            plan_error(
                MobileActionPlanErrorCodeWire::MalformedQuestionRequest,
                "question is missing options array",
                Some("options"),
            )
        })?;

    if let Some(id) = &request.selected_option_id {
        return options
            .iter()
            .filter_map(JsonValue::as_object)
            .find(|option| {
                option
                    .get("id")
                    .and_then(JsonValue::as_str)
                    .is_some_and(|candidate| candidate == id)
            })
            .and_then(option_label)
            .ok_or_else(|| {
                plan_error(
                    MobileActionPlanErrorCodeWire::InvalidOption,
                    format!("question option id {id:?} is not available"),
                    Some("selected_option_id"),
                )
            });
    }

    let index = request.selected_option_index.ok_or_else(|| {
        plan_error(
            MobileActionPlanErrorCodeWire::InvalidRequest,
            "question answer requires selected_option_label, selected_option_id, or selected_option_index",
            Some("selected_option"),
        )
    })? as usize;
    let option = options
        .get(index)
        .and_then(JsonValue::as_object)
        .ok_or_else(|| {
            plan_error(
                MobileActionPlanErrorCodeWire::InvalidOption,
                format!("question option index {index} is not available"),
                Some("selected_option_index"),
            )
        })?;
    option_label(option).ok_or_else(|| {
        plan_error(
            MobileActionPlanErrorCodeWire::MalformedQuestionRequest,
            format!("question option index {index} is missing a label"),
            Some("label"),
        )
    })
}

fn option_label(option: &serde_json::Map<String, JsonValue>) -> Option<String> {
    option
        .get("label")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
}

fn validate_schema(
    schema_version: u32,
) -> Result<(), MobileActionPlanErrorWire> {
    if (1..=MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION).contains(&schema_version) {
        return Ok(());
    }
    Err(plan_error(
        MobileActionPlanErrorCodeWire::InvalidRequest,
        format!(
            "mobile action wire schema mismatch: got {schema_version}, expected {MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION}"
        ),
        Some("schema_version"),
    ))
}

fn plan_error(
    code: MobileActionPlanErrorCodeWire,
    message: impl Into<String>,
    target: Option<&str>,
) -> MobileActionPlanErrorWire {
    MobileActionPlanErrorWire {
        code,
        message: message.into(),
        target: target.map(str::to_string),
    }
}

#[allow(dead_code)]
fn _action_data(values: &[(&str, &str)]) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_gate_request(path: &std::path::Path, value: JsonValue) {
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
    }

    fn sample_plan_notification() -> NotificationWire {
        NotificationWire {
            id: "abcdef1234567890".to_string(),
            timestamp: "2026-05-06T15:30:00Z".to_string(),
            sender: "planner".to_string(),
            icon: None,
            notes: vec!["Plan ready for review".to_string()],
            files: vec!["/tmp/plan.md".to_string()],
            tags: vec!["plan".to_string(), "review".to_string()],
            action: Some("PlanApproval".to_string()),
            action_data: _action_data(&[
                ("response_dir", "/tmp/agent"),
                ("llm_provider", "codex"),
            ]),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        }
    }

    fn sample_epic_notification() -> NotificationWire {
        NotificationWire {
            id: "epic1234567890".to_string(),
            timestamp: "2026-07-16T15:30:00Z".to_string(),
            sender: "planner".to_string(),
            icon: None,
            notes: vec!["Epic ready for review".to_string()],
            files: vec!["/tmp/epic.md".to_string()],
            tags: vec!["epic".to_string(), "review".to_string()],
            action: Some("EpicApproval".to_string()),
            action_data: _action_data(&[("response_dir", "/tmp/epic")]),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        }
    }

    #[test]
    fn mobile_notification_contract_snapshot_is_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let request_path = tmp.path().join("request.json");
        write_gate_request(
            &request_path,
            json!({
                "schema_version": 2,
                "branches": [["approve", "commit"], ["reject"], ["feedback"]],
                "options": [
                    {"id": "approve", "label": "Approve", "icon": "✅", "default_selected": true, "feedback": "disabled"},
                    {"id": "commit", "label": "Commit plan file to the plans sidecar", "icon": "💾", "default_selected": true, "feedback": "disabled"},
                    {"id": "reject", "label": "Reject", "icon": "❌", "default_selected": true, "feedback": "disabled"},
                    {"id": "feedback", "label": "Send Feedback", "icon": "💬", "default_selected": true, "feedback": "required"}
                ],
                "groups": [{"options": ["approve", "commit"], "label": "Approve", "icon": "✅"}]
            }),
        );
        let mut notification = sample_plan_notification();
        notification.action_data.insert(
            "request_path".to_string(),
            request_path.to_string_lossy().into_owned(),
        );
        let card = mobile_notification_card_from_wire(
            &notification,
            MobileActionStateWire::Available,
            true,
        );
        let detail = MobileNotificationDetailResponseWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            notification: card,
            notes: notification.notes.clone(),
            attachments: vec![mobile_attachment_manifest_from_path(
                &notification.id,
                0,
                "plan.md",
                Some(42),
                true,
            )],
            action: mobile_action_detail_from_notification(
                &notification,
                MobileActionStateWire::Available,
            ),
        };
        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/mobile/mobile_notification_contract.json"
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(detail).unwrap(), expected);
    }

    #[test]
    fn epic_notification_contract_snapshot_is_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let request_path = tmp.path().join("request.json");
        write_gate_request(
            &request_path,
            json!({
                "schema_version": 2,
                "branches": [["approve"], ["reject"], ["feedback"]],
                "options": [
                    {"id": "approve", "label": "Approve", "icon": "✅", "default_selected": true, "feedback": "disabled"},
                    {"id": "reject", "label": "Reject", "icon": "❌", "default_selected": true, "feedback": "disabled"},
                    {"id": "feedback", "label": "Send Feedback", "icon": "💬", "default_selected": true, "feedback": "required"}
                ],
                "groups": []
            }),
        );
        let mut notification = sample_epic_notification();
        notification.action_data.insert(
            "request_path".to_string(),
            request_path.to_string_lossy().into_owned(),
        );
        let detail = MobileNotificationDetailResponseWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            notification: mobile_notification_card_from_wire(
                &notification,
                MobileActionStateWire::Available,
                mobile_notification_priority_from_wire(&notification),
            ),
            notes: notification.notes.clone(),
            attachments: vec![mobile_attachment_manifest_from_path(
                &notification.id,
                0,
                "epic.md",
                Some(84),
                true,
            )],
            action: mobile_action_detail_from_notification(
                &notification,
                MobileActionStateWire::Available,
            ),
        };
        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/mobile/mobile_epic_notification_contract.json"
        ))
        .unwrap();
        assert_eq!(serde_json::to_value(detail).unwrap(), expected);
    }

    #[test]
    fn custom_gate_notification_contract_snapshot_is_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let request_path = tmp.path().join("request.json");
        std::fs::write(
            &request_path,
            serde_json::to_vec(&json!({
                "schema_version": 2,
                "request_id": "custom-demo",
                "kind": "custom",
                "branches": [["proceed", "open_report"], ["cancel"]],
                "options": [
                    {
                        "id": "proceed",
                        "label": "Proceed safely",
                        "icon": "✅",
                        "feedback": "required",
                        "default_selected": true
                    },
                    {
                        "id": "open_report",
                        "label": "Open the report",
                        "icon": "📄",
                        "default_selected": true,
                        "feedback": "optional"
                    },
                    {
                        "id": "cancel",
                        "label": "Cancel",
                        "default_selected": true,
                        "feedback": "optional"
                    }
                ],
                "groups": [{"options": ["proceed", "open_report"], "label": "Proceed safely", "icon": "✅"}]
            }))
            .unwrap(),
        )
        .unwrap();
        let notification = NotificationWire {
            id: "custom1234567890".to_string(),
            timestamp: "2026-07-16T23:00:00Z".to_string(),
            sender: "security-agent".to_string(),
            icon: Some("🛡️".to_string()),
            notes: vec!["Confirm the guarded operation".to_string()],
            tags: vec!["custom".to_string(), "gate".to_string()],
            action: Some("CustomGate".to_string()),
            action_data: _action_data(&[
                ("request_id", "custom-demo"),
                ("bundle_path", tmp.path().to_str().unwrap()),
                ("request_path", request_path.to_str().unwrap()),
            ]),
            ..Default::default()
        };
        let priority = mobile_notification_priority_from_wire(&notification);
        let card = mobile_notification_card_from_wire(
            &notification,
            MobileActionStateWire::Available,
            priority,
        );
        let detail = MobileNotificationDetailResponseWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            notification: card,
            notes: notification.notes.clone(),
            attachments: Vec::new(),
            action: mobile_action_detail_from_notification(
                &notification,
                MobileActionStateWire::Available,
            ),
        };
        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/mobile/mobile_custom_gate_notification_contract.json"
        ))
        .unwrap();

        assert!(!priority);
        assert_eq!(serde_json::to_value(detail).unwrap(), expected);
    }

    #[test]
    fn launch_approval_detail_exposes_preview_identity() {
        let tmp = tempfile::tempdir().unwrap();
        let request_path = tmp.path().join("request.json");
        write_gate_request(
            &request_path,
            json!({
                "schema_version": 2,
                "branches": [["approve"], ["reject"], ["feedback"]],
                "options": [
                    {"id": "approve", "label": "Approve", "icon": "✅", "default_selected": true, "feedback": "disabled"},
                    {"id": "reject", "label": "Reject", "icon": "❌", "default_selected": true, "feedback": "disabled"},
                    {"id": "feedback", "label": "Send Feedback", "icon": "💬", "default_selected": true, "feedback": "required"}
                ],
                "groups": []
            }),
        );
        let notification = NotificationWire {
            id: "launch1234567890".to_string(),
            timestamp: "2026-05-06T15:30:00Z".to_string(),
            sender: "launch".to_string(),
            icon: None,
            notes: vec!["Launch request ready".to_string()],
            files: vec![
                "/tmp/launch_preview.md".to_string(),
                "/tmp/launch_request.json".to_string(),
            ],
            tags: vec!["launch".to_string()],
            action: Some("LaunchApproval".to_string()),
            action_data: _action_data(&[
                ("response_dir", "/tmp/launch"),
                ("request_id", "req-123"),
                ("request_path", request_path.to_str().unwrap()),
                ("source_surface", "agent"),
                ("slot_count", "3"),
            ]),
            read: false,
            dismissed: false,
            silent: false,
            muted: false,
            snooze_until: None,
        };

        let detail = mobile_action_detail_from_notification(
            &notification,
            MobileActionStateWire::Available,
        );

        assert_eq!(
            serde_json::to_value(detail).unwrap(),
            json!({
                "kind": "launch_approval",
                "identity": {
                    "notification_id": "launch1234567890",
                    "prefix": "launch12",
                    "prefix_len": 8,
                    "resolution": "unique_prefix"
                },
                "state": "available",
                "response_dir": "/tmp/launch",
                "request_id": "req-123",
                "source_surface": "agent",
                "slot_count": 3,
                "branches": [
                    {"options": [{"id": "approve", "label": "Approve", "icon": "✅", "default_selected": true, "feedback": "disabled"}], "submit": null},
                    {"options": [{"id": "reject", "label": "Reject", "icon": "❌", "default_selected": true, "feedback": "disabled"}], "submit": null},
                    {"options": [{"id": "feedback", "label": "Send Feedback", "icon": "💬", "default_selected": true, "feedback": "required"}], "submit": null}
                ],
            })
        );
    }

    #[test]
    fn action_result_contract_snapshot_is_stable() {
        let gate = ActionResultWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            prefix: "abcdef12".to_string(),
            action_kind: MobileActionKindWire::PlanApproval,
            notification_id: Some("abcdef1234567890".to_string()),
            state: MobileActionStateWire::Available,
            response_file: "response.json".to_string(),
            response_json: json!({
                "schema_version": 2,
                "selected_option_ids": ["approve", "commit"],
                "feedback": null,
            }),
            message: Some("Gate resolved with approve, commit".to_string()),
        };
        let question = plan_question_action_response(
            &QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "ques0001".to_string(),
                choice: QuestionActionChoiceWire::Answer,
                question_index: Some(0),
                selected_option_id: Some("pg".to_string()),
                selected_option_label: None,
                selected_option_index: None,
                custom_answer: None,
                global_note: Some("Answered via mobile".to_string()),
            },
            &json!({
                "questions": [{
                    "question": "Which DB?",
                    "options": [
                        {"id": "pg", "label": "PostgreSQL"},
                        {"id": "sqlite", "label": "SQLite"}
                    ]
                }]
            }),
        )
        .unwrap();

        let expected: JsonValue = serde_json::from_str(include_str!(
            "../../tests/fixtures/mobile/mobile_action_result_contract.json"
        ))
        .unwrap();
        assert_eq!(
            json!({
                "gate": gate,
                "question": question,
            }),
            expected
        );
    }

    #[test]
    fn question_planner_supports_index_label_id_and_custom_answers() {
        let request = json!({
            "questions": [{
                "question": "Which approach?",
                "options": [
                    {"id": "a", "label": "Option A"},
                    {"id": "b", "label": "Option B"}
                ]
            }]
        });

        let by_index = plan_question_action_response(
            &QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "ques0001".to_string(),
                choice: QuestionActionChoiceWire::Answer,
                question_index: None,
                selected_option_id: None,
                selected_option_label: None,
                selected_option_index: Some(1),
                custom_answer: None,
                global_note: None,
            },
            &request,
        )
        .unwrap();
        assert_eq!(
            by_index.response_json["answers"][0]["selected"],
            json!(["Option B"])
        );

        let by_label = plan_question_action_response(
            &QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "ques0001".to_string(),
                choice: QuestionActionChoiceWire::Answer,
                question_index: None,
                selected_option_id: None,
                selected_option_label: Some("Manual Label".to_string()),
                selected_option_index: None,
                custom_answer: None,
                global_note: None,
            },
            &request,
        )
        .unwrap();
        assert_eq!(
            by_label.response_json["answers"][0]["selected"],
            json!(["Manual Label"])
        );

        let custom = plan_question_action_response(
            &QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "ques0001".to_string(),
                choice: QuestionActionChoiceWire::Custom,
                question_index: None,
                selected_option_id: None,
                selected_option_label: None,
                selected_option_index: None,
                custom_answer: Some("Use both".to_string()),
                global_note: Some("Answered via mobile".to_string()),
            },
            &request,
        )
        .unwrap();
        assert_eq!(
            custom.response_json,
            json!({
                "answers": [{
                    "question": "Which approach?",
                    "selected": [],
                    "custom_feedback": "Use both",
                }],
                "global_note": "Answered via mobile",
            })
        );
    }

    #[test]
    fn prefix_resolution_makes_collisions_explicit() {
        let ids = ["abcdef1234", "abcd999999", "unique0001"];
        assert_eq!(
            resolve_notification_prefix("unique", ids).resolution,
            PendingActionPrefixResolutionWire::UniquePrefix
        );
        assert_eq!(
            resolve_notification_prefix("abcd", ids).resolution,
            PendingActionPrefixResolutionWire::AmbiguousPrefix
        );
        assert_eq!(
            resolve_notification_prefix("missing", ids).resolution,
            PendingActionPrefixResolutionWire::Missing
        );
        assert_eq!(
            resolve_notification_prefix("abcdef1234", ids).resolution,
            PendingActionPrefixResolutionWire::Exact
        );
    }

    #[test]
    fn planner_errors_are_deterministic() {
        let err = plan_question_action_response(
            &QuestionActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "ques0001".to_string(),
                choice: QuestionActionChoiceWire::Answer,
                question_index: None,
                selected_option_id: None,
                selected_option_label: None,
                selected_option_index: Some(99),
                custom_answer: None,
                global_note: None,
            },
            &json!({"questions": [{"question": "Q?", "options": []}]}),
        )
        .unwrap_err();
        assert_eq!(err.code, MobileActionPlanErrorCodeWire::InvalidOption);
        assert_eq!(err.target.as_deref(), Some("selected_option_index"));
    }

    #[test]
    fn priority_and_error_classifiers_are_disjoint() {
        let axe_error = NotificationWire {
            sender: "axe".to_string(),
            action: Some("ViewErrorReport".to_string()),
            ..Default::default()
        };
        assert!(mobile_notification_error_from_wire(&axe_error));
        assert!(!mobile_notification_priority_from_wire(&axe_error));

        let user_agent_error = NotificationWire {
            sender: "user-agent".to_string(),
            action: Some("ViewErrorReport".to_string()),
            ..Default::default()
        };
        assert!(mobile_notification_error_from_wire(&user_agent_error));
        assert!(!mobile_notification_priority_from_wire(&user_agent_error));

        let axe_other = NotificationWire {
            sender: "axe".to_string(),
            action: Some("JumpToChangeSpec".to_string()),
            ..Default::default()
        };
        assert!(!mobile_notification_error_from_wire(&axe_other));
        assert!(mobile_notification_priority_from_wire(&axe_other));

        let plan_approval = NotificationWire {
            sender: "plan".to_string(),
            action: Some("PlanApproval".to_string()),
            ..Default::default()
        };
        assert!(!mobile_notification_error_from_wire(&plan_approval));
        assert!(mobile_notification_priority_from_wire(&plan_approval));

        let epic_approval = sample_epic_notification();
        assert!(!mobile_notification_error_from_wire(&epic_approval));
        assert!(mobile_notification_priority_from_wire(&epic_approval));
    }

    #[test]
    fn gate_action_string_mapping_is_complete() {
        let expected = [
            ("PlanApproval", MobileActionKindWire::PlanApproval),
            ("EpicApproval", MobileActionKindWire::EpicApproval),
            ("HITL", MobileActionKindWire::Hitl),
            ("UserQuestion", MobileActionKindWire::UserQuestion),
            ("LaunchApproval", MobileActionKindWire::LaunchApproval),
            ("CustomGate", MobileActionKindWire::CustomGate),
        ];
        for (action, kind) in expected {
            assert_eq!(
                MobileActionKindWire::from_notification_action(Some(action)),
                kind
            );
            assert_eq!(kind.notification_action(), Some(action));
            assert!(kind.is_gate());
        }
    }

    #[test]
    fn every_non_question_gate_projects_the_same_branch_wire() {
        let tmp = tempfile::tempdir().unwrap();
        let request_path = tmp.path().join("request.json");
        write_gate_request(
            &request_path,
            json!({
                "schema_version": 2,
                "branches": [["approve"]],
                "options": [{
                    "id": "approve",
                    "label": "Approve",
                    "icon": "✅",
                    "default_selected": true,
                    "feedback": "disabled"
                }],
                "groups": []
            }),
        );
        for action in [
            "PlanApproval",
            "EpicApproval",
            "HITL",
            "LaunchApproval",
            "CustomGate",
        ] {
            let notification = NotificationWire {
                id: format!("{action}-notification"),
                action: Some(action.to_string()),
                action_data: _action_data(&[(
                    "request_path",
                    request_path.to_str().unwrap(),
                )]),
                ..Default::default()
            };
            let detail =
                serde_json::to_value(mobile_action_detail_from_notification(
                    &notification,
                    MobileActionStateWire::Available,
                ))
                .unwrap();
            assert_eq!(
                detail["branches"],
                json!([{
                    "options": [{
                        "id": "approve",
                        "label": "Approve",
                        "icon": "✅",
                        "default_selected": true,
                        "feedback": "disabled"
                    }],
                    "submit": null
                }]),
                "{action} did not use the generic branch projection"
            );
        }
    }
}

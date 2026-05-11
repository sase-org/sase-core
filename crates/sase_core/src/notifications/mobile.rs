use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use super::wire::NotificationWire;

pub const MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION: u32 = 1;

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
    Hitl,
    UserQuestion,
    NonAction,
    Unsupported,
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
        choices: Vec<PlanActionChoiceWire>,
    },
    Hitl {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        artifacts_dir: Option<String>,
        workflow_name: Option<String>,
        choices: Vec<HitlActionChoiceWire>,
    },
    UserQuestion {
        identity: PendingActionIdentityWire,
        state: MobileActionStateWire,
        response_dir: Option<String>,
        question_count: u32,
        choices: Vec<QuestionActionChoiceWire>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanActionChoiceWire {
    Approve,
    Run,
    Reject,
    Epic,
    Legend,
    Feedback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanActionRequestWire {
    pub schema_version: u32,
    pub prefix: String,
    pub choice: PlanActionChoiceWire,
    #[serde(default)]
    pub feedback: Option<String>,
    #[serde(default)]
    pub commit_plan: Option<bool>,
    #[serde(default)]
    pub run_coder: Option<bool>,
    #[serde(default)]
    pub coder_prompt: Option<String>,
    #[serde(default)]
    pub coder_model: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HitlActionChoiceWire {
    Accept,
    Reject,
    Feedback,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HitlActionRequestWire {
    pub schema_version: u32,
    pub prefix: String,
    pub choice: HitlActionChoiceWire,
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
    let action_kind = mobile_action_kind(notification.action.as_deref());
    let actionable = matches!(
        action_kind,
        MobileActionKindWire::PlanApproval
            | MobileActionKindWire::Hitl
            | MobileActionKindWire::UserQuestion
    ) && state == MobileActionStateWire::Available;
    MobileNotificationCardWire {
        id: notification.id.clone(),
        timestamp: notification.timestamp.clone(),
        sender: notification.sender.clone(),
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
                label: action_label(action_kind).to_string(),
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
    matches!(
        notification.action.as_deref(),
        Some("PlanApproval" | "UserQuestion" | "JumpToMentorReview")
    ) || matches!(notification.sender.as_str(), "axe" | "crs")
}

pub fn mobile_action_detail_from_notification(
    notification: &NotificationWire,
    state: MobileActionStateWire,
) -> MobileActionDetailWire {
    let identity = pending_action_identity(&notification.id, 8);
    match notification.action.as_deref() {
        Some("PlanApproval") => MobileActionDetailWire::PlanApproval {
            identity,
            state,
            response_dir: notification.action_data.get("response_dir").cloned(),
            plan_file: notification.files.first().cloned(),
            choices: vec![
                PlanActionChoiceWire::Approve,
                PlanActionChoiceWire::Run,
                PlanActionChoiceWire::Reject,
                PlanActionChoiceWire::Epic,
                PlanActionChoiceWire::Legend,
                PlanActionChoiceWire::Feedback,
            ],
        },
        Some("HITL") => MobileActionDetailWire::Hitl {
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
            choices: vec![
                HitlActionChoiceWire::Accept,
                HitlActionChoiceWire::Reject,
                HitlActionChoiceWire::Feedback,
            ],
        },
        Some("UserQuestion") => MobileActionDetailWire::UserQuestion {
            identity,
            state,
            response_dir: notification.action_data.get("response_dir").cloned(),
            question_count: notification
                .action_data
                .get("question_count")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or(0),
            choices: vec![
                QuestionActionChoiceWire::Answer,
                QuestionActionChoiceWire::Custom,
            ],
        },
        None => MobileActionDetailWire::NonAction {
            state: MobileActionStateWire::Unsupported,
        },
        other => MobileActionDetailWire::Unsupported {
            identity,
            state: MobileActionStateWire::Unsupported,
            action: other.map(str::to_string),
        },
    }
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

pub fn plan_plan_action_response(
    request: &PlanActionRequestWire,
) -> Result<ActionResultWire, MobileActionPlanErrorWire> {
    validate_schema(request.schema_version)?;
    let mut response = serde_json::Map::new();
    let message = match request.choice {
        PlanActionChoiceWire::Approve => {
            response.insert("action".to_string(), json!("approve"));
            insert_plan_options(&mut response, request);
            "Plan approved"
        }
        PlanActionChoiceWire::Run => {
            response.insert("action".to_string(), json!("approve"));
            response.insert("commit_plan".to_string(), json!(false));
            response.insert("run_coder".to_string(), json!(true));
            if let Some(value) = &request.coder_prompt {
                response.insert("coder_prompt".to_string(), json!(value));
            }
            if let Some(value) = &request.coder_model {
                response.insert("coder_model".to_string(), json!(value));
            }
            "Running coder"
        }
        PlanActionChoiceWire::Reject => {
            response.insert("action".to_string(), json!("reject"));
            if let Some(value) = &request.feedback {
                response.insert("feedback".to_string(), json!(value));
            }
            "Plan rejected"
        }
        PlanActionChoiceWire::Epic => {
            response.insert("action".to_string(), json!("epic"));
            "Epic created"
        }
        PlanActionChoiceWire::Legend => {
            response.insert("action".to_string(), json!("legend"));
            "Legend created"
        }
        PlanActionChoiceWire::Feedback => {
            let feedback = request.feedback.as_deref().ok_or_else(|| {
                plan_error(
                    MobileActionPlanErrorCodeWire::InvalidRequest,
                    "plan feedback requires feedback text",
                    Some("feedback"),
                )
            })?;
            response.insert("action".to_string(), json!("reject"));
            response.insert("feedback".to_string(), json!(feedback));
            "Feedback received"
        }
    };

    Ok(ActionResultWire {
        schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
        action_kind: MobileActionKindWire::PlanApproval,
        prefix: request.prefix.clone(),
        notification_id: None,
        state: MobileActionStateWire::Available,
        response_file: "plan_response.json".to_string(),
        response_json: JsonValue::Object(response),
        message: Some(message.to_string()),
    })
}

pub fn plan_hitl_action_response(
    request: &HitlActionRequestWire,
) -> Result<ActionResultWire, MobileActionPlanErrorWire> {
    validate_schema(request.schema_version)?;
    let response_json = match request.choice {
        HitlActionChoiceWire::Accept => {
            json!({"action": "accept", "approved": true})
        }
        HitlActionChoiceWire::Reject => {
            json!({"action": "reject", "approved": false})
        }
        HitlActionChoiceWire::Feedback => {
            let feedback = request.feedback.as_deref().ok_or_else(|| {
                plan_error(
                    MobileActionPlanErrorCodeWire::InvalidRequest,
                    "HITL feedback requires feedback text",
                    Some("feedback"),
                )
            })?;
            json!({
                "action": "feedback",
                "approved": false,
                "feedback": feedback,
            })
        }
    };

    Ok(ActionResultWire {
        schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
        action_kind: MobileActionKindWire::Hitl,
        prefix: request.prefix.clone(),
        notification_id: None,
        state: MobileActionStateWire::Available,
        response_file: "hitl_response.json".to_string(),
        response_json,
        message: Some(hitl_message(request.choice).to_string()),
    })
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

fn mobile_action_kind(action: Option<&str>) -> MobileActionKindWire {
    match action {
        Some("PlanApproval") => MobileActionKindWire::PlanApproval,
        Some("HITL") => MobileActionKindWire::Hitl,
        Some("UserQuestion") => MobileActionKindWire::UserQuestion,
        None => MobileActionKindWire::NonAction,
        Some(_) => MobileActionKindWire::Unsupported,
    }
}

fn action_label(kind: MobileActionKindWire) -> &'static str {
    match kind {
        MobileActionKindWire::PlanApproval => "Plan approval",
        MobileActionKindWire::Hitl => "HITL",
        MobileActionKindWire::UserQuestion => "Question",
        MobileActionKindWire::NonAction => "Notification",
        MobileActionKindWire::Unsupported => "Unsupported action",
    }
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

fn insert_plan_options(
    response: &mut serde_json::Map<String, JsonValue>,
    request: &PlanActionRequestWire,
) {
    if let Some(value) = request.commit_plan {
        response.insert("commit_plan".to_string(), json!(value));
    }
    if let Some(value) = request.run_coder {
        response.insert("run_coder".to_string(), json!(value));
    }
    if let Some(value) = &request.coder_prompt {
        response.insert("coder_prompt".to_string(), json!(value));
    }
    if let Some(value) = &request.coder_model {
        response.insert("coder_model".to_string(), json!(value));
    }
}

fn hitl_message(choice: HitlActionChoiceWire) -> &'static str {
    match choice {
        HitlActionChoiceWire::Accept => "Accepted",
        HitlActionChoiceWire::Reject => "Rejected",
        HitlActionChoiceWire::Feedback => "Feedback received",
    }
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
    if schema_version == MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION {
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

    fn sample_plan_notification() -> NotificationWire {
        NotificationWire {
            id: "abcdef1234567890".to_string(),
            timestamp: "2026-05-06T15:30:00Z".to_string(),
            sender: "planner".to_string(),
            notes: vec!["Plan ready for review".to_string()],
            files: vec!["/tmp/plan.md".to_string()],
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

    #[test]
    fn mobile_notification_contract_snapshot_is_stable() {
        let notification = sample_plan_notification();
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
    fn action_result_contract_snapshot_is_stable() {
        let plan = plan_plan_action_response(&PlanActionRequestWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            prefix: "abcdef12".to_string(),
            choice: PlanActionChoiceWire::Run,
            feedback: None,
            commit_plan: None,
            run_coder: None,
            coder_prompt: Some("Focus tests".to_string()),
            coder_model: Some("gpt-5.2".to_string()),
        })
        .unwrap();
        let hitl = plan_hitl_action_response(&HitlActionRequestWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            prefix: "hitl0001".to_string(),
            choice: HitlActionChoiceWire::Feedback,
            feedback: Some("Please revise".to_string()),
        })
        .unwrap();
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
                "plan": plan,
                "hitl": hitl,
                "question": question,
            }),
            expected
        );
    }

    #[test]
    fn plan_response_planner_covers_all_choices() {
        let expected = [
            (PlanActionChoiceWire::Approve, json!({"action": "approve"})),
            (
                PlanActionChoiceWire::Run,
                json!({
                    "action": "approve",
                    "commit_plan": false,
                    "run_coder": true,
                }),
            ),
            (PlanActionChoiceWire::Reject, json!({"action": "reject"})),
            (PlanActionChoiceWire::Epic, json!({"action": "epic"})),
            (PlanActionChoiceWire::Legend, json!({"action": "legend"})),
            (
                PlanActionChoiceWire::Feedback,
                json!({"action": "reject", "feedback": "try again"}),
            ),
        ];
        for (choice, response_json) in expected {
            let feedback = if choice == PlanActionChoiceWire::Feedback {
                Some("try again".to_string())
            } else {
                None
            };
            let result = plan_plan_action_response(&PlanActionRequestWire {
                schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
                prefix: "abcd1234".to_string(),
                choice,
                feedback,
                commit_plan: None,
                run_coder: None,
                coder_prompt: None,
                coder_model: None,
            })
            .unwrap();
            assert_eq!(result.response_json, response_json);
        }
    }

    #[test]
    fn hitl_response_planner_matches_existing_shapes() {
        let accept = plan_hitl_action_response(&HitlActionRequestWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            prefix: "hitl0001".to_string(),
            choice: HitlActionChoiceWire::Accept,
            feedback: None,
        })
        .unwrap();
        assert_eq!(
            accept.response_json,
            json!({"action": "accept", "approved": true})
        );

        let reject = plan_hitl_action_response(&HitlActionRequestWire {
            schema_version: MOBILE_NOTIFICATION_WIRE_SCHEMA_VERSION,
            prefix: "hitl0001".to_string(),
            choice: HitlActionChoiceWire::Reject,
            feedback: None,
        })
        .unwrap();
        assert_eq!(
            reject.response_json,
            json!({"action": "reject", "approved": false})
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
    }
}

//! Journaled fleet attention contract: remote questions and gates.
//!
//! A viewer never reads a gate bundle, a question response directory, a
//! notification file, or any local path for a remote row; it holds an opaque
//! attention request key, a bounded projection, and opaque content handles.
//! Answering is always a request to the owner, never a local write. Owner
//! resolution stays on the owning host: this module only projects and
//! validates; it never executes a gate or question action itself.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};

use crate::fleet_contract::{
    duration_ms, operation_payload_fingerprint, reject_path_like,
    reject_secretish, timestamp_ms, validate_content_handle,
    validate_installation_id, validate_label, validate_non_negative_seconds,
    validate_schema, validate_timestamp, CapabilitySetWire, ContentHandleWire,
    FleetContractError, LogicalAgentLocatorWire, OperationDecisionKindWire,
    OperationDecisionReasonWire, OperationReceiptStateWire,
    PayloadFingerprintRequestWire, PayloadFingerprintWire,
    ScopedOperationKeyWire, FLEET_CONTRACT_SCHEMA_VERSION, MAX_LABEL_BYTES,
};
use crate::notifications::mobile::{
    mobile_action_detail_from_notification, pending_action_identity,
    GateFeedbackModeWire, MobileActionDetailWire, MobileActionKindWire,
    MobileActionStateWire, QuestionActionChoiceWire,
};
use crate::notifications::wire::NotificationWire;

/// Capability advertised for owner-side answering of a pending question.
pub const FLEET_ATTENTION_CAPABILITY_ANSWER_QUESTION: &str =
    "attention.answer_question";
/// Capability advertised for owner-side approval of a pending gate.
pub const FLEET_ATTENTION_CAPABILITY_APPROVE_GATE: &str =
    "attention.approve_gate";

/// `action_data` key a gate producer sets to attribute itself to an agent.
///
/// Must match Python's
/// `sase.notification_gates.presentation.GATE_ORIGIN_AGENT_ACTION_DATA_KEY`.
const ORIGIN_AGENT_ACTION_DATA_KEY: &str = "origin_agent";

const MAX_ATTENTION_ROWS: usize = 200;
const MAX_ATTENTION_IDENTITIES: usize = 200;
const MAX_ATTENTION_OPTIONS: usize = 32;
const MAX_TITLE_BYTES: usize = 200;
const MAX_SUMMARY_BYTES: usize = 2048;

/// Closed attention kinds the fleet attention journal accepts.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetAttentionKindWire {
    Question,
    Gate,
}

/// Owner-observed lifecycle of one attention request.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetAttentionStateWire {
    Pending,
    Settled,
    Expired,
    Unknown,
}

/// Stable identity for one attention request: origin plus the owner's
/// opaque pending-action identity. This is the same identity `sase gate
/// answer` and the mobile bridge already consume, so the same request is one
/// request no matter which surface answers it.
#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionRequestKeyWire {
    pub schema_version: u32,
    pub origin_installation_id: String,
    pub request_id: String,
    pub pending_action_prefix: String,
}

impl FleetAttentionRequestKeyWire {
    /// Canonical string form used as a dedupe/ledger map key.
    pub fn canonical(&self) -> String {
        format!(
            "{}\u{1}{}\u{1}{}",
            self.origin_installation_id,
            self.request_id,
            self.pending_action_prefix
        )
    }

    pub fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet attention request key", self.schema_version)?;
        validate_installation_id(&self.origin_installation_id)?;
        validate_label(
            "fleet attention request key request_id",
            &self.request_id,
            MAX_LABEL_BYTES,
        )?;
        reject_path_like(
            "fleet attention request key request_id",
            &self.request_id,
        )?;
        reject_secretish(
            "fleet attention request key request_id",
            &self.request_id,
        )?;
        validate_label(
            "fleet attention request key pending_action_prefix",
            &self.pending_action_prefix,
            MAX_LABEL_BYTES,
        )?;
        reject_path_like(
            "fleet attention request key pending_action_prefix",
            &self.pending_action_prefix,
        )?;
        reject_secretish(
            "fleet attention request key pending_action_prefix",
            &self.pending_action_prefix,
        )?;
        Ok(())
    }
}

/// One selectable gate option surfaced to the reviewer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionOptionWire {
    pub schema_version: u32,
    pub id: String,
    pub label: String,
}

impl FleetAttentionOptionWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet attention option", self.schema_version)?;
        validate_label("fleet attention option id", &self.id, MAX_LABEL_BYTES)?;
        reject_path_like("fleet attention option id", &self.id)?;
        reject_secretish("fleet attention option id", &self.id)?;
        validate_label(
            "fleet attention option label",
            &self.label,
            MAX_LABEL_BYTES,
        )?;
        reject_secretish("fleet attention option label", &self.label)?;
        Ok(())
    }
}

/// Question-shaped answer form, mirroring the local question action modes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionQuestionFormWire {
    pub schema_version: u32,
    pub question_index: u32,
    pub question_count: u32,
    pub choices: Vec<QuestionActionChoiceWire>,
}

/// Bounded, safe attention entry. Never carries a local path, response
/// directory, PID, or credential.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionEntryWire {
    pub schema_version: u32,
    pub kind: FleetAttentionKindWire,
    pub state: FleetAttentionStateWire,
    pub request_key: FleetAttentionRequestKeyWire,
    pub revision: u64,
    pub logical_key: Option<String>,
    pub logical_locator: Option<LogicalAgentLocatorWire>,
    pub title: String,
    pub summary: String,
    pub options: Vec<FleetAttentionOptionWire>,
    pub feedback_required: bool,
    pub question_form: Option<FleetAttentionQuestionFormWire>,
    pub preview: Option<ContentHandleWire>,
    pub settled_by_host_label: Option<String>,
    pub settled_response: Option<JsonValue>,
}

impl FleetAttentionEntryWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet attention entry", self.schema_version)?;
        self.request_key.validate()?;
        if let Some(locator) = &self.logical_locator {
            locator.validate()?;
        }
        if let Some(key) = &self.logical_key {
            validate_label(
                "fleet attention entry logical_key",
                key,
                MAX_LABEL_BYTES,
            )?;
        }
        validate_label(
            "fleet attention entry title",
            &self.title,
            MAX_TITLE_BYTES,
        )?;
        reject_secretish("fleet attention entry title", &self.title)?;
        if !self.summary.is_empty() {
            validate_label(
                "fleet attention entry summary",
                &self.summary,
                MAX_SUMMARY_BYTES,
            )?;
            reject_secretish("fleet attention entry summary", &self.summary)?;
        }
        if self.options.len() > MAX_ATTENTION_OPTIONS {
            return Err(FleetContractError::Validation(format!(
                "fleet attention entry options exceeds {MAX_ATTENTION_OPTIONS} entries"
            )));
        }
        for option in &self.options {
            option.validate()?;
        }
        if let Some(preview) = &self.preview {
            validate_content_handle(preview)?;
        }
        if let Some(label) = &self.settled_by_host_label {
            validate_label(
                "fleet attention entry settled_by_host_label",
                label,
                MAX_LABEL_BYTES,
            )?;
            reject_path_like(
                "fleet attention entry settled_by_host_label",
                label,
            )?;
            reject_secretish(
                "fleet attention entry settled_by_host_label",
                label,
            )?;
        }
        Ok(())
    }
}

/// One host's attention snapshot for a bounded set of followed rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionSnapshotWire {
    pub schema_version: u32,
    pub entries: Vec<FleetAttentionEntryWire>,
    pub observed_at_unix: f64,
}

/// One notification row plus its already-resolved mobile action state, as
/// read through the existing notification bridge (`list_notifications` plus
/// `action_state` per row).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionNotificationRowWire {
    pub schema_version: u32,
    pub notification: NotificationWire,
    pub state: MobileActionStateWire,
}

/// One resolved row's logical identity, used to correlate a notification to
/// the fleet row that raised it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionLogicalIdentityWire {
    pub schema_version: u32,
    pub agent_label: String,
    pub logical_key: String,
    pub logical_locator: LogicalAgentLocatorWire,
}

/// Deterministically project owner notification rows into a bounded,
/// deduplicated attention snapshot.
///
/// Correlates a request to a row through the gate notification's
/// `origin_agent` action-data key and, for questions, through the asking
/// agent's `sender` identity. An uncorrelated request is still returned,
/// keyed to no row, so it can never be silently dropped by this pure
/// projection; callers that must scope attention to a bounded set of
/// followed rows do so by bounding `rows`/`resolved` before calling this
/// function.
pub fn project_fleet_attention(
    origin_installation_id: &str,
    rows: &[FleetAttentionNotificationRowWire],
    resolved: &[FleetAttentionLogicalIdentityWire],
    observed_at_unix: f64,
) -> Result<FleetAttentionSnapshotWire, FleetContractError> {
    validate_installation_id(origin_installation_id)?;
    validate_timestamp("observed_at_unix", observed_at_unix)?;
    if rows.len() > MAX_ATTENTION_ROWS {
        return Err(FleetContractError::Validation(format!(
            "fleet attention notification rows exceeds {MAX_ATTENTION_ROWS} entries"
        )));
    }
    if resolved.len() > MAX_ATTENTION_IDENTITIES {
        return Err(FleetContractError::Validation(format!(
            "fleet attention logical identities exceeds {MAX_ATTENTION_IDENTITIES} entries"
        )));
    }
    for identity in resolved {
        validate_schema(
            "fleet attention logical identity",
            identity.schema_version,
        )?;
        identity.logical_locator.validate()?;
        validate_label(
            "fleet attention logical identity agent_label",
            &identity.agent_label,
            MAX_LABEL_BYTES,
        )?;
    }
    let mut by_agent_label: BTreeMap<&str, &FleetAttentionLogicalIdentityWire> =
        BTreeMap::new();
    for identity in resolved {
        by_agent_label
            .entry(identity.agent_label.as_str())
            .or_insert(identity);
    }

    let mut entries = Vec::new();
    let mut seen_keys = BTreeSet::new();
    for row in rows {
        validate_schema(
            "fleet attention notification row",
            row.schema_version,
        )?;
        let notification = &row.notification;
        let action_kind = MobileActionKindWire::from_notification_action(
            notification.action.as_deref(),
        );
        let Some(kind) = fleet_attention_kind_for(action_kind) else {
            continue;
        };

        let identity = pending_action_identity(&notification.id, 8);
        let request_key = FleetAttentionRequestKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin_installation_id: origin_installation_id.to_string(),
            request_id: notification.id.clone(),
            pending_action_prefix: identity.prefix,
        };
        request_key.validate()?;
        if !seen_keys.insert(request_key.canonical()) {
            continue;
        }

        let correlated = correlate_row(kind, notification, &by_agent_label);
        let (logical_key, logical_locator) = match correlated {
            Some(found) => (
                Some(found.logical_key.clone()),
                Some(found.logical_locator.clone()),
            ),
            None => (None, None),
        };

        let state = fleet_attention_state_for(row.state);
        let title = attention_title(notification);
        let summary = attention_summary(notification);
        let (options, feedback_required) =
            attention_options(kind, notification, row.state);
        let question_form = if kind == FleetAttentionKindWire::Question {
            Some(FleetAttentionQuestionFormWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                question_index: 0,
                question_count: notification
                    .action_data
                    .get("question_count")
                    .and_then(|value| value.parse::<u32>().ok())
                    .unwrap_or(0),
                choices: vec![
                    QuestionActionChoiceWire::Answer,
                    QuestionActionChoiceWire::Custom,
                ],
            })
        } else {
            None
        };
        let revision = attention_revision_from_content(
            kind,
            &title,
            &summary,
            &options,
            feedback_required,
            question_form.as_ref(),
        )?;

        let entry = FleetAttentionEntryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind,
            state,
            request_key,
            revision,
            logical_key,
            logical_locator,
            title,
            summary,
            options,
            feedback_required,
            question_form,
            preview: None,
            settled_by_host_label: None,
            settled_response: None,
        };
        entry.validate()?;
        entries.push(entry);
    }
    entries.sort_by(|left, right| {
        left.request_key
            .canonical()
            .cmp(&right.request_key.canonical())
    });
    Ok(FleetAttentionSnapshotWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        entries,
        observed_at_unix,
    })
}

fn fleet_attention_kind_for(
    action_kind: MobileActionKindWire,
) -> Option<FleetAttentionKindWire> {
    match action_kind {
        MobileActionKindWire::UserQuestion => {
            Some(FleetAttentionKindWire::Question)
        }
        MobileActionKindWire::PlanApproval
        | MobileActionKindWire::EpicApproval
        | MobileActionKindWire::Hitl
        | MobileActionKindWire::LaunchApproval
        | MobileActionKindWire::TaskTriage
        | MobileActionKindWire::BeadSnooze
        | MobileActionKindWire::FlagTriage
        | MobileActionKindWire::BeadStaleCleanup
        | MobileActionKindWire::PluginsRequired
        | MobileActionKindWire::CustomGate => {
            Some(FleetAttentionKindWire::Gate)
        }
        MobileActionKindWire::NonAction | MobileActionKindWire::Unsupported => {
            None
        }
    }
}

fn fleet_attention_state_for(
    state: MobileActionStateWire,
) -> FleetAttentionStateWire {
    match state {
        MobileActionStateWire::Available => FleetAttentionStateWire::Pending,
        MobileActionStateWire::AlreadyHandled => {
            FleetAttentionStateWire::Settled
        }
        MobileActionStateWire::Stale => FleetAttentionStateWire::Expired,
        MobileActionStateWire::MissingRequest
        | MobileActionStateWire::MissingTarget
        | MobileActionStateWire::Unsupported => {
            FleetAttentionStateWire::Unknown
        }
    }
}

fn correlate_row<'a>(
    kind: FleetAttentionKindWire,
    notification: &NotificationWire,
    by_agent_label: &BTreeMap<&str, &'a FleetAttentionLogicalIdentityWire>,
) -> Option<&'a FleetAttentionLogicalIdentityWire> {
    let label = match kind {
        FleetAttentionKindWire::Gate => notification
            .action_data
            .get(ORIGIN_AGENT_ACTION_DATA_KEY)?
            .as_str(),
        FleetAttentionKindWire::Question => notification.sender.as_str(),
    };
    by_agent_label.get(label).copied()
}

fn attention_title(notification: &NotificationWire) -> String {
    if let Some(title) = notification.action_data.get("gate_title") {
        let trimmed = title.trim();
        if !trimmed.is_empty() {
            return bounded(trimmed, MAX_TITLE_BYTES);
        }
    }
    let action_kind = MobileActionKindWire::from_notification_action(
        notification.action.as_deref(),
    );
    bounded(action_kind.label(), MAX_TITLE_BYTES)
}

fn attention_summary(notification: &NotificationWire) -> String {
    let joined = notification.notes.join(" ");
    bounded(joined.trim(), MAX_SUMMARY_BYTES)
}

fn attention_options(
    kind: FleetAttentionKindWire,
    notification: &NotificationWire,
    state: MobileActionStateWire,
) -> (Vec<FleetAttentionOptionWire>, bool) {
    if kind != FleetAttentionKindWire::Gate {
        return (Vec::new(), false);
    }
    let detail = mobile_action_detail_from_notification(notification, state);
    let branches = match &detail {
        MobileActionDetailWire::PlanApproval { branches, .. }
        | MobileActionDetailWire::EpicApproval { branches, .. }
        | MobileActionDetailWire::Hitl { branches, .. }
        | MobileActionDetailWire::LaunchApproval { branches, .. }
        | MobileActionDetailWire::CustomGate { branches, .. } => branches,
        _ => return (Vec::new(), false),
    };
    let mut seen = BTreeSet::new();
    let mut options = Vec::new();
    let mut feedback_required = false;
    for branch in branches {
        for option in &branch.options {
            if option.feedback == GateFeedbackModeWire::Required {
                feedback_required = true;
            }
            if seen.insert(option.id.clone()) {
                options.push(FleetAttentionOptionWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    id: option.id.clone(),
                    label: option.label.clone(),
                });
            }
        }
    }
    (options, feedback_required)
}

fn attention_revision_from_content(
    kind: FleetAttentionKindWire,
    title: &str,
    summary: &str,
    options: &[FleetAttentionOptionWire],
    feedback_required: bool,
    question_form: Option<&FleetAttentionQuestionFormWire>,
) -> Result<u64, FleetContractError> {
    let payload = json!({
        "kind": kind,
        "title": title,
        "summary": summary,
        "options": options,
        "feedback_required": feedback_required,
        "question_form": question_form,
    });
    let fingerprint =
        operation_payload_fingerprint(&PayloadFingerprintRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            payload,
        })?;
    u64::from_str_radix(&fingerprint.sha256[..16], 16).map_err(|_| {
        FleetContractError::Validation(
            "fleet attention revision fingerprint was not valid hex"
                .to_string(),
        )
    })
}

fn bounded(value: &str, max_bytes: usize) -> String {
    let value = value.trim();
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

/// Validated attention answer intent. Paths and secrets are rejected at
/// validate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionIntentWire {
    pub schema_version: u32,
    pub kind: FleetAttentionKindWire,
    pub request_key: FleetAttentionRequestKeyWire,
    pub observed_revision: u64,
    // Gate-only fields.
    pub selected_option_ids: Vec<String>,
    pub feedback: Option<String>,
    // Question-only fields, mirroring the existing question action shape.
    pub question_choice: Option<QuestionActionChoiceWire>,
    pub question_index: Option<u32>,
    pub selected_option_id: Option<String>,
    pub selected_option_label: Option<String>,
    pub selected_option_index: Option<u32>,
    pub custom_answer: Option<String>,
    pub global_note: Option<String>,
}

impl FleetAttentionIntentWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet attention intent", self.schema_version)?;
        self.request_key.validate()?;
        match self.kind {
            FleetAttentionKindWire::Gate => {
                if self.selected_option_ids.is_empty() {
                    return Err(FleetContractError::Validation(
                        "fleet attention gate intent requires at least one selected option"
                            .to_string(),
                    ));
                }
                if self.selected_option_ids.len() > MAX_ATTENTION_OPTIONS {
                    return Err(FleetContractError::Validation(format!(
                        "fleet attention gate intent selected_option_ids exceeds {MAX_ATTENTION_OPTIONS} entries"
                    )));
                }
                for id in &self.selected_option_ids {
                    validate_label(
                        "fleet attention selected option id",
                        id,
                        MAX_LABEL_BYTES,
                    )?;
                    reject_path_like("fleet attention selected option id", id)?;
                    reject_secretish("fleet attention selected option id", id)?;
                }
                if self.question_choice.is_some()
                    || self.question_index.is_some()
                    || self.selected_option_id.is_some()
                    || self.selected_option_label.is_some()
                    || self.selected_option_index.is_some()
                    || self.custom_answer.is_some()
                {
                    return Err(FleetContractError::Validation(
                        "fleet attention gate intent must not include question fields"
                            .to_string(),
                    ));
                }
            }
            FleetAttentionKindWire::Question => {
                if !self.selected_option_ids.is_empty() {
                    return Err(FleetContractError::Validation(
                        "fleet attention question intent must not include selected_option_ids"
                            .to_string(),
                    ));
                }
                match self.question_choice {
                    Some(QuestionActionChoiceWire::Answer) => {
                        if self.selected_option_id.is_none()
                            && self.selected_option_index.is_none()
                        {
                            return Err(FleetContractError::Validation(
                                "fleet attention question answer requires a selected option"
                                    .to_string(),
                            ));
                        }
                        if self.custom_answer.is_some() {
                            return Err(FleetContractError::Validation(
                                "fleet attention question answer must not include a custom answer"
                                    .to_string(),
                            ));
                        }
                    }
                    Some(QuestionActionChoiceWire::Custom) => {
                        let Some(answer) = &self.custom_answer else {
                            return Err(FleetContractError::Validation(
                                "fleet attention question custom choice requires a custom answer"
                                    .to_string(),
                            ));
                        };
                        validate_label(
                            "fleet attention custom answer",
                            answer,
                            MAX_SUMMARY_BYTES,
                        )?;
                        reject_secretish(
                            "fleet attention custom answer",
                            answer,
                        )?;
                    }
                    None => {
                        return Err(FleetContractError::Validation(
                            "fleet attention question intent requires a question_choice"
                                .to_string(),
                        ));
                    }
                }
            }
        }
        if let Some(feedback) = &self.feedback {
            validate_label(
                "fleet attention feedback",
                feedback,
                MAX_SUMMARY_BYTES,
            )?;
            reject_secretish("fleet attention feedback", feedback)?;
        }
        if let Some(note) = &self.global_note {
            validate_label(
                "fleet attention global_note",
                note,
                MAX_SUMMARY_BYTES,
            )?;
            reject_secretish("fleet attention global_note", note)?;
        }
        if let Some(label) = &self.selected_option_label {
            validate_label(
                "fleet attention selected_option_label",
                label,
                MAX_LABEL_BYTES,
            )?;
            reject_secretish("fleet attention selected_option_label", label)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub target_installation_id: String,
    pub intent: FleetAttentionIntentWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub acceptance_window_seconds: f64,
}

/// Settled attention outcome recorded on the receipt.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetAttentionOutcomeWire {
    Applied,
    AlreadySettled,
    StaleRevision,
    UnknownRequest,
    CapabilityMissing,
    PreconditionFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionReceiptWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub target_installation_id: String,
    pub request_key: FleetAttentionRequestKeyWire,
    pub observed_revision: u64,
    pub accepted_at_unix_ms: u64,
    pub expires_at_unix_ms: u64,
    pub state: OperationReceiptStateWire,
    pub outcome: Option<FleetAttentionOutcomeWire>,
    pub settled_by_host_label: Option<String>,
    pub settled_response: Option<JsonValue>,
    pub message: Option<String>,
}

impl FleetAttentionReceiptWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("fleet attention receipt", self.schema_version)?;
        self.key.validate()?;
        self.payload_fingerprint.validate()?;
        validate_installation_id(&self.target_installation_id)?;
        self.request_key.validate()?;
        if self.expires_at_unix_ms < self.accepted_at_unix_ms {
            return Err(FleetContractError::Validation(
                "fleet attention receipt expires before it was accepted"
                    .to_string(),
            ));
        }
        if let Some(label) = &self.settled_by_host_label {
            validate_label(
                "fleet attention receipt settled_by_host_label",
                label,
                MAX_LABEL_BYTES,
            )?;
            reject_path_like(
                "fleet attention receipt settled_by_host_label",
                label,
            )?;
            reject_secretish(
                "fleet attention receipt settled_by_host_label",
                label,
            )?;
        }
        if let Some(message) = &self.message {
            validate_label(
                "fleet attention receipt message",
                message,
                MAX_LABEL_BYTES,
            )?;
            reject_path_like("fleet attention receipt message", message)?;
            reject_secretish("fleet attention receipt message", message)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableFleetAttentionRecordWire {
    pub schema_version: u32,
    pub receipt: FleetAttentionReceiptWire,
    pub tombstoned_at_unix_ms: Option<u64>,
}

impl DurableFleetAttentionRecordWire {
    fn validate(&self) -> Result<(), FleetContractError> {
        validate_schema("durable fleet attention record", self.schema_version)?;
        self.receipt.validate()?;
        if self
            .tombstoned_at_unix_ms
            .is_some_and(|value| value < self.receipt.accepted_at_unix_ms)
        {
            return Err(FleetContractError::Validation(
                "fleet attention tombstone predates acceptance".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionDecisionRequestWire {
    pub schema_version: u32,
    pub key: ScopedOperationKeyWire,
    pub payload_fingerprint: PayloadFingerprintWire,
    pub request_key: FleetAttentionRequestKeyWire,
    pub observed_revision: u64,
    pub target_installation_id: String,
    pub now_unix: f64,
    pub acceptance_window_seconds: f64,
    pub existing_record: Option<DurableFleetAttentionRecordWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionDecisionWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: Option<FleetAttentionReceiptWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionResponseWire {
    pub schema_version: u32,
    pub decision: OperationDecisionKindWire,
    pub reason: OperationDecisionReasonWire,
    pub receipt: FleetAttentionReceiptWire,
}

/// Typed refusal from re-projecting the current attention entry.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum FleetAttentionPreconditionReasonWire {
    Ok,
    UnknownRequest,
    StaleRevision,
    AlreadySettled,
    CapabilityMissing,
    InvalidOption,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionPreconditionDecisionWire {
    pub schema_version: u32,
    pub allowed: bool,
    pub reason: FleetAttentionPreconditionReasonWire,
    pub required_capability: String,
    pub settled_by_host_label: Option<String>,
    pub settled_response: Option<JsonValue>,
}

pub fn validate_fleet_attention_intent(
    intent: &FleetAttentionIntentWire,
) -> Result<FleetAttentionIntentWire, FleetContractError> {
    intent.validate()?;
    Ok(intent.clone())
}

pub fn fleet_attention_payload_fingerprint(
    intent: &FleetAttentionIntentWire,
) -> Result<PayloadFingerprintWire, FleetContractError> {
    intent.validate()?;
    operation_payload_fingerprint(&PayloadFingerprintRequestWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        payload: serde_json::to_value(intent).map_err(|source| {
            FleetContractError::Json {
                path: PathBuf::from("<fleet_attention_intent>"),
                source,
            }
        })?,
    })
}

pub fn validate_fleet_attention_request(
    request: &FleetAttentionRequestWire,
) -> Result<FleetAttentionRequestWire, FleetContractError> {
    validate_schema("fleet attention request", request.schema_version)?;
    request.key.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    request.intent.validate()?;
    request.payload_fingerprint.validate()?;
    let expected = fleet_attention_payload_fingerprint(&request.intent)?;
    if request.payload_fingerprint != expected {
        return Err(FleetContractError::Validation(
            "fleet attention payload_fingerprint does not match intent"
                .to_string(),
        ));
    }
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    Ok(request.clone())
}

pub fn decide_fleet_attention_replay(
    request: &FleetAttentionDecisionRequestWire,
) -> Result<FleetAttentionDecisionWire, FleetContractError> {
    validate_schema(
        "fleet attention decision request",
        request.schema_version,
    )?;
    request.key.validate()?;
    request.payload_fingerprint.validate()?;
    request.request_key.validate()?;
    validate_installation_id(&request.target_installation_id)?;
    validate_timestamp("now_unix", request.now_unix)?;
    validate_non_negative_seconds(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?;
    let now_ms = timestamp_ms("now_unix", request.now_unix)?;
    let expires_at = now_ms.saturating_add(duration_ms(
        "acceptance_window_seconds",
        request.acceptance_window_seconds,
    )?);
    if let Some(record) = &request.existing_record {
        record.validate()?;
        if record.receipt.key != request.key {
            return Err(FleetContractError::Validation(
                "existing fleet attention record key does not match request key"
                    .to_string(),
            ));
        }
        if record.tombstoned_at_unix_ms.is_some()
            || now_ms > record.receipt.expires_at_unix_ms
        {
            return Ok(fleet_attention_decision(
                OperationDecisionKindWire::Expired,
                OperationDecisionReasonWire::ExpiredOrTombstonedKey,
                None,
            ));
        }
        if record.receipt.payload_fingerprint != request.payload_fingerprint {
            return Ok(fleet_attention_decision(
                OperationDecisionKindWire::Conflict,
                OperationDecisionReasonWire::SameScopedKeyDifferentPayload,
                Some(record.receipt.clone()),
            ));
        }
        if record.receipt.request_key != request.request_key
            || record.receipt.observed_revision != request.observed_revision
            || record.receipt.target_installation_id
                != request.target_installation_id
        {
            return Ok(fleet_attention_decision(
                OperationDecisionKindWire::PreconditionMismatch,
                OperationDecisionReasonWire::TargetOrRevisionMismatch,
                Some(record.receipt.clone()),
            ));
        }
        return Ok(fleet_attention_decision(
            OperationDecisionKindWire::ReturnOriginalReceipt,
            OperationDecisionReasonWire::SameScopedKeyAndPayload,
            Some(record.receipt.clone()),
        ));
    }
    let receipt = FleetAttentionReceiptWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        key: request.key.clone(),
        payload_fingerprint: request.payload_fingerprint.clone(),
        target_installation_id: request.target_installation_id.clone(),
        request_key: request.request_key.clone(),
        observed_revision: request.observed_revision,
        accepted_at_unix_ms: now_ms,
        expires_at_unix_ms: expires_at,
        state: OperationReceiptStateWire::Accepted,
        outcome: None,
        settled_by_host_label: None,
        settled_response: None,
        message: None,
    };
    Ok(fleet_attention_decision(
        OperationDecisionKindWire::AcceptNew,
        OperationDecisionReasonWire::UnseenInWindow,
        Some(receipt),
    ))
}

/// Compare an intent against the current re-projected attention entry (or
/// its absence) and the capability set advertised for the correlated row.
pub fn evaluate_attention_precondition(
    intent: &FleetAttentionIntentWire,
    observed: Option<&FleetAttentionEntryWire>,
    capabilities: &CapabilitySetWire,
) -> Result<FleetAttentionPreconditionDecisionWire, FleetContractError> {
    intent.validate()?;
    let required_capability = intent.kind.required_capability().to_string();
    let Some(entry) = observed else {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::UnknownRequest,
            required_capability,
            None,
            None,
        ));
    };
    entry.validate()?;
    if entry.request_key != intent.request_key || entry.kind != intent.kind {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::UnknownRequest,
            required_capability,
            None,
            None,
        ));
    }
    if entry.state == FleetAttentionStateWire::Settled {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::AlreadySettled,
            required_capability,
            entry.settled_by_host_label.clone(),
            entry.settled_response.clone(),
        ));
    }
    if entry.state != FleetAttentionStateWire::Pending {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::UnknownRequest,
            required_capability,
            None,
            None,
        ));
    }
    if entry.revision != intent.observed_revision {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::StaleRevision,
            required_capability,
            None,
            None,
        ));
    }
    if !capability_present(capabilities, &required_capability) {
        return Ok(precondition(
            FleetAttentionPreconditionReasonWire::CapabilityMissing,
            required_capability,
            None,
            None,
        ));
    }
    match intent.kind {
        FleetAttentionKindWire::Gate => {
            let known: BTreeSet<&str> = entry
                .options
                .iter()
                .map(|option| option.id.as_str())
                .collect();
            if intent
                .selected_option_ids
                .iter()
                .any(|id| !known.contains(id.as_str()))
            {
                return Ok(precondition(
                    FleetAttentionPreconditionReasonWire::InvalidOption,
                    required_capability,
                    None,
                    None,
                ));
            }
        }
        FleetAttentionKindWire::Question => {
            if let Some(id) = &intent.selected_option_id {
                let known: BTreeSet<&str> = entry
                    .options
                    .iter()
                    .map(|option| option.id.as_str())
                    .collect();
                if !known.is_empty() && !known.contains(id.as_str()) {
                    return Ok(precondition(
                        FleetAttentionPreconditionReasonWire::InvalidOption,
                        required_capability,
                        None,
                        None,
                    ));
                }
            }
        }
    }
    Ok(precondition(
        FleetAttentionPreconditionReasonWire::Ok,
        required_capability,
        None,
        None,
    ))
}

impl FleetAttentionKindWire {
    pub fn required_capability(self) -> &'static str {
        match self {
            Self::Question => FLEET_ATTENTION_CAPABILITY_ANSWER_QUESTION,
            Self::Gate => FLEET_ATTENTION_CAPABILITY_APPROVE_GATE,
        }
    }
}

fn fleet_attention_decision(
    decision: OperationDecisionKindWire,
    reason: OperationDecisionReasonWire,
    receipt: Option<FleetAttentionReceiptWire>,
) -> FleetAttentionDecisionWire {
    FleetAttentionDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        decision,
        reason,
        receipt,
    }
}

fn precondition(
    reason: FleetAttentionPreconditionReasonWire,
    required_capability: String,
    settled_by_host_label: Option<String>,
    settled_response: Option<JsonValue>,
) -> FleetAttentionPreconditionDecisionWire {
    FleetAttentionPreconditionDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        allowed: reason == FleetAttentionPreconditionReasonWire::Ok,
        reason,
        required_capability,
        settled_by_host_label,
        settled_response,
    }
}

fn capability_present(capabilities: &CapabilitySetWire, wanted: &str) -> bool {
    capabilities.resource.iter().any(|value| value == wanted)
}

/// One durable notice-ledger entry: the last revision of a request this
/// viewer was shown a toast for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionNoticeLedgerEntryWire {
    pub schema_version: u32,
    pub request_key: FleetAttentionRequestKeyWire,
    pub announced_revision: u64,
    pub announced_at_unix_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetAttentionNoticeDecisionWire {
    pub schema_version: u32,
    pub to_announce: Vec<FleetAttentionEntryWire>,
    pub suppressed: Vec<FleetAttentionEntryWire>,
    pub ledger: Vec<FleetAttentionNoticeLedgerEntryWire>,
}

/// Decide which pending entries deserve a fresh toast, keyed by request key
/// plus revision, so a reconnect that re-delivers the same request announces
/// nothing while a new revision (a superseded and re-asked request)
/// announces exactly once. Returns the pruned/updated ledger to persist.
pub fn decide_attention_notices(
    current: &[FleetAttentionEntryWire],
    ledger: &[FleetAttentionNoticeLedgerEntryWire],
    retention_window_seconds: f64,
    now_unix: f64,
) -> Result<FleetAttentionNoticeDecisionWire, FleetContractError> {
    validate_timestamp("now_unix", now_unix)?;
    validate_non_negative_seconds(
        "retention_window_seconds",
        retention_window_seconds,
    )?;
    for entry in current {
        entry.validate()?;
    }
    for record in ledger {
        validate_schema(
            "fleet attention notice ledger entry",
            record.schema_version,
        )?;
        record.request_key.validate()?;
    }
    let now_ms = timestamp_ms("now_unix", now_unix)?;
    let retention_ms =
        duration_ms("retention_window_seconds", retention_window_seconds)?;
    let cutoff_ms = now_ms.saturating_sub(retention_ms);

    let mut previous: BTreeMap<String, &FleetAttentionNoticeLedgerEntryWire> =
        BTreeMap::new();
    for record in ledger {
        previous.insert(record.request_key.canonical(), record);
    }

    let mut to_announce = Vec::new();
    let mut suppressed = Vec::new();
    let mut next_ledger: BTreeMap<String, FleetAttentionNoticeLedgerEntryWire> =
        BTreeMap::new();

    for entry in current {
        if entry.state != FleetAttentionStateWire::Pending {
            continue;
        }
        let canonical = entry.request_key.canonical();
        match previous.get(&canonical) {
            Some(record) if record.announced_revision == entry.revision => {
                suppressed.push(entry.clone());
                next_ledger.insert(
                    canonical,
                    FleetAttentionNoticeLedgerEntryWire {
                        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                        request_key: entry.request_key.clone(),
                        announced_revision: record.announced_revision,
                        announced_at_unix_ms: record.announced_at_unix_ms,
                    },
                );
            }
            _ => {
                to_announce.push(entry.clone());
                next_ledger.insert(
                    canonical,
                    FleetAttentionNoticeLedgerEntryWire {
                        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                        request_key: entry.request_key.clone(),
                        announced_revision: entry.revision,
                        announced_at_unix_ms: now_ms,
                    },
                );
            }
        }
    }
    // Prune entries for requests no longer projected as pending once they
    // age past the retention window, so the ledger does not grow forever.
    for (canonical, record) in &previous {
        if next_ledger.contains_key(canonical) {
            continue;
        }
        if record.announced_at_unix_ms >= cutoff_ms {
            next_ledger.insert(canonical.clone(), (*record).clone());
        }
    }

    Ok(FleetAttentionNoticeDecisionWire {
        schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
        to_announce,
        suppressed,
        ledger: next_ledger.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fleet_contract::ProjectLocatorWire;

    fn installation(hex: char) -> String {
        format!(
            "{}{}",
            crate::fleet_contract::FLEET_INSTALLATION_ID_PREFIX,
            hex.to_string().repeat(64)
        )
    }

    fn logical(agent: &str) -> LogicalAgentLocatorWire {
        LogicalAgentLocatorWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            project: ProjectLocatorWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                origin: crate::fleet_contract::OriginLocatorWire {
                    schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                    installation_id: installation('a'),
                },
                project_id: "project-1".to_string(),
            },
            agent_id: agent.to_string(),
            family_id: None,
        }
    }

    fn identity(agent: &str) -> FleetAttentionLogicalIdentityWire {
        FleetAttentionLogicalIdentityWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            agent_label: agent.to_string(),
            logical_key: format!("logical:{agent}"),
            logical_locator: logical(agent),
        }
    }

    fn gate_row(
        id: &str,
        origin_agent: Option<&str>,
    ) -> FleetAttentionNotificationRowWire {
        let mut action_data = BTreeMap::new();
        if let Some(agent) = origin_agent {
            action_data.insert("origin_agent".to_string(), agent.to_string());
        }
        FleetAttentionNotificationRowWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            notification: NotificationWire {
                id: id.to_string(),
                timestamp: "2026-09-07T00:00:00Z".to_string(),
                sender: "axe".to_string(),
                action: Some("CustomGate".to_string()),
                action_data,
                ..Default::default()
            },
            state: MobileActionStateWire::Available,
        }
    }

    fn question_row(
        id: &str,
        sender: &str,
    ) -> FleetAttentionNotificationRowWire {
        let mut action_data = BTreeMap::new();
        action_data.insert("question_count".to_string(), "2".to_string());
        FleetAttentionNotificationRowWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            notification: NotificationWire {
                id: id.to_string(),
                timestamp: "2026-09-07T00:00:00Z".to_string(),
                sender: sender.to_string(),
                action: Some("UserQuestion".to_string()),
                action_data,
                ..Default::default()
            },
            state: MobileActionStateWire::Available,
        }
    }

    #[test]
    fn projection_correlates_gate_by_origin_agent() {
        let rows = vec![gate_row("notif-1", Some("athena.worker"))];
        let resolved = vec![identity("athena.worker")];
        let snapshot = project_fleet_attention(
            &installation('a'),
            &rows,
            &resolved,
            100.0,
        )
        .unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.kind, FleetAttentionKindWire::Gate);
        assert_eq!(entry.logical_key.as_deref(), Some("logical:athena.worker"));
        assert!(entry.logical_locator.is_some());
    }

    #[test]
    fn projection_correlates_question_by_sender() {
        let rows = vec![question_row("notif-2", "athena.worker")];
        let resolved = vec![identity("athena.worker")];
        let snapshot = project_fleet_attention(
            &installation('a'),
            &rows,
            &resolved,
            100.0,
        )
        .unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        let entry = &snapshot.entries[0];
        assert_eq!(entry.kind, FleetAttentionKindWire::Question);
        assert!(entry.question_form.is_some());
        assert_eq!(entry.question_form.as_ref().unwrap().question_count, 2);
    }

    #[test]
    fn uncorrelated_request_is_still_returned() {
        let rows = vec![gate_row("notif-3", Some("someone.else"))];
        let resolved = vec![identity("athena.worker")];
        let snapshot = project_fleet_attention(
            &installation('a'),
            &rows,
            &resolved,
            100.0,
        )
        .unwrap();
        assert_eq!(snapshot.entries.len(), 1);
        assert!(snapshot.entries[0].logical_key.is_none());
        assert!(snapshot.entries[0].logical_locator.is_none());
    }

    #[test]
    fn non_actionable_notifications_are_skipped() {
        let mut row = gate_row("notif-4", Some("athena.worker"));
        row.notification.action = None;
        let snapshot = project_fleet_attention(
            &installation('a'),
            &[row],
            &[identity("athena.worker")],
            100.0,
        )
        .unwrap();
        assert!(snapshot.entries.is_empty());
    }

    #[test]
    fn projection_is_deterministic_and_sorted() {
        let rows = vec![
            gate_row("notif-b", Some("athena.worker")),
            gate_row("notif-a", Some("athena.worker")),
        ];
        let resolved = vec![identity("athena.worker")];
        let first =
            project_fleet_attention(&installation('a'), &rows, &resolved, 1.0)
                .unwrap();
        let second =
            project_fleet_attention(&installation('a'), &rows, &resolved, 1.0)
                .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.entries[0].request_key.request_id, "notif-a");
        assert_eq!(first.entries[1].request_key.request_id, "notif-b");
    }

    #[test]
    fn secretish_title_is_rejected() {
        let mut row = gate_row("notif-5", Some("athena.worker"));
        row.notification
            .action_data
            .insert("gate_title".to_string(), "token=abc123".to_string());
        let error = project_fleet_attention(
            &installation('a'),
            &[row],
            &[identity("athena.worker")],
            100.0,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("inline credentials"), "{error}");
    }

    #[test]
    fn path_like_option_id_is_rejected() {
        let option = FleetAttentionOptionWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            id: "/tmp/evil".to_string(),
            label: "Evil".to_string(),
        };
        let error = option.validate().unwrap_err().to_string();
        assert!(error.contains("must not look like a path"), "{error}");
    }

    fn request_key(id: &str) -> FleetAttentionRequestKeyWire {
        FleetAttentionRequestKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            origin_installation_id: installation('a'),
            request_id: id.to_string(),
            pending_action_prefix: id[..id.len().min(8)].to_string(),
        }
    }

    fn gate_intent(id: &str, revision: u64) -> FleetAttentionIntentWire {
        FleetAttentionIntentWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: FleetAttentionKindWire::Gate,
            request_key: request_key(id),
            observed_revision: revision,
            selected_option_ids: vec!["approve".to_string()],
            feedback: None,
            question_choice: None,
            question_index: None,
            selected_option_id: None,
            selected_option_label: None,
            selected_option_index: None,
            custom_answer: None,
            global_note: None,
        }
    }

    fn operation_key(id: &str) -> ScopedOperationKeyWire {
        ScopedOperationKeyWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            controller_id: "controller-1".to_string(),
            operation_id: id.to_string(),
        }
    }

    #[test]
    fn fingerprint_is_stable_and_sensitive_to_changes() {
        let intent = gate_intent("notif-1", 42);
        let first = fleet_attention_payload_fingerprint(&intent).unwrap();
        let second = fleet_attention_payload_fingerprint(&intent).unwrap();
        assert_eq!(first, second);
        let mut other = intent.clone();
        other.feedback = Some("looks good".to_string());
        assert_ne!(first, fleet_attention_payload_fingerprint(&other).unwrap());
    }

    fn decision_request(
        fingerprint: PayloadFingerprintWire,
        existing: Option<DurableFleetAttentionRecordWire>,
        now: f64,
    ) -> FleetAttentionDecisionRequestWire {
        FleetAttentionDecisionRequestWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            key: operation_key("op-1"),
            payload_fingerprint: fingerprint,
            request_key: request_key("notif-1"),
            observed_revision: 42,
            target_installation_id: installation('a'),
            now_unix: now,
            acceptance_window_seconds: 30.0,
            existing_record: existing,
        }
    }

    #[test]
    fn replay_accepts_unseen_key() {
        let intent = gate_intent("notif-1", 42);
        let fingerprint = fleet_attention_payload_fingerprint(&intent).unwrap();
        let decision = decide_fleet_attention_replay(&decision_request(
            fingerprint,
            None,
            10.0,
        ))
        .unwrap();
        assert_eq!(decision.decision, OperationDecisionKindWire::AcceptNew);
        assert_eq!(
            decision.receipt.unwrap().state,
            OperationReceiptStateWire::Accepted
        );
    }

    #[test]
    fn replay_returns_original_receipt_for_same_payload() {
        let intent = gate_intent("notif-1", 42);
        let fingerprint = fleet_attention_payload_fingerprint(&intent).unwrap();
        let accepted = decide_fleet_attention_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        let decision = decide_fleet_attention_replay(&decision_request(
            fingerprint,
            Some(DurableFleetAttentionRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted.clone(),
                tombstoned_at_unix_ms: None,
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(
            decision.decision,
            OperationDecisionKindWire::ReturnOriginalReceipt
        );
        assert_eq!(decision.receipt.unwrap(), accepted);
    }

    #[test]
    fn replay_conflicts_on_changed_payload() {
        let intent = gate_intent("notif-1", 42);
        let fingerprint = fleet_attention_payload_fingerprint(&intent).unwrap();
        let accepted = decide_fleet_attention_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        let mut other = fingerprint;
        other.sha256 = "c".repeat(64);
        let decision = decide_fleet_attention_replay(&decision_request(
            other,
            Some(DurableFleetAttentionRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted,
                tombstoned_at_unix_ms: None,
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(decision.decision, OperationDecisionKindWire::Conflict);
    }

    #[test]
    fn replay_rejects_expired_or_tombstoned_key() {
        let intent = gate_intent("notif-1", 42);
        let fingerprint = fleet_attention_payload_fingerprint(&intent).unwrap();
        let mut accepted = decide_fleet_attention_replay(&decision_request(
            fingerprint.clone(),
            None,
            10.0,
        ))
        .unwrap()
        .receipt
        .unwrap();
        accepted.expires_at_unix_ms = 10_000;
        let expired = decide_fleet_attention_replay(&decision_request(
            fingerprint.clone(),
            Some(DurableFleetAttentionRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted.clone(),
                tombstoned_at_unix_ms: None,
            }),
            20.0,
        ))
        .unwrap();
        assert_eq!(expired.decision, OperationDecisionKindWire::Expired);
        let tombstoned = decide_fleet_attention_replay(&decision_request(
            fingerprint,
            Some(DurableFleetAttentionRecordWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                receipt: accepted,
                tombstoned_at_unix_ms: Some(10_001),
            }),
            11.0,
        ))
        .unwrap();
        assert_eq!(tombstoned.decision, OperationDecisionKindWire::Expired);
    }

    fn entry_for(intent: &FleetAttentionIntentWire) -> FleetAttentionEntryWire {
        FleetAttentionEntryWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            kind: intent.kind,
            state: FleetAttentionStateWire::Pending,
            request_key: intent.request_key.clone(),
            revision: intent.observed_revision,
            logical_key: Some("logical:athena.worker".to_string()),
            logical_locator: Some(logical("athena.worker")),
            title: "Approve plan".to_string(),
            summary: "Summary".to_string(),
            options: vec![FleetAttentionOptionWire {
                schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
                id: "approve".to_string(),
                label: "Approve".to_string(),
            }],
            feedback_required: false,
            question_form: None,
            preview: None,
            settled_by_host_label: None,
            settled_response: None,
        }
    }

    fn full_capabilities() -> CapabilitySetWire {
        CapabilitySetWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            resource: vec![FLEET_ATTENTION_CAPABILITY_APPROVE_GATE.to_string()],
            host: Vec::new(),
            protocol: vec!["fleet.v1".to_string()],
        }
    }

    #[test]
    fn precondition_allows_matching_pending_entry() {
        let intent = gate_intent("notif-1", 42);
        let entry = entry_for(&intent);
        let decision = evaluate_attention_precondition(
            &intent,
            Some(&entry),
            &full_capabilities(),
        )
        .unwrap();
        assert!(decision.allowed);
        assert_eq!(decision.reason, FleetAttentionPreconditionReasonWire::Ok);
    }

    #[test]
    fn precondition_unknown_request_when_missing() {
        let intent = gate_intent("notif-1", 42);
        let decision = evaluate_attention_precondition(
            &intent,
            None,
            &full_capabilities(),
        )
        .unwrap();
        assert_eq!(
            decision.reason,
            FleetAttentionPreconditionReasonWire::UnknownRequest
        );
    }

    #[test]
    fn precondition_stale_revision() {
        let intent = gate_intent("notif-1", 42);
        let mut entry = entry_for(&intent);
        entry.revision = 43;
        let decision = evaluate_attention_precondition(
            &intent,
            Some(&entry),
            &full_capabilities(),
        )
        .unwrap();
        assert_eq!(
            decision.reason,
            FleetAttentionPreconditionReasonWire::StaleRevision
        );
    }

    #[test]
    fn precondition_already_settled_reports_settling_host() {
        let intent = gate_intent("notif-1", 42);
        let mut entry = entry_for(&intent);
        entry.state = FleetAttentionStateWire::Settled;
        entry.settled_by_host_label = Some("apollo".to_string());
        entry.settled_response = Some(json!({"selected": "approve"}));
        let decision = evaluate_attention_precondition(
            &intent,
            Some(&entry),
            &full_capabilities(),
        )
        .unwrap();
        assert_eq!(
            decision.reason,
            FleetAttentionPreconditionReasonWire::AlreadySettled
        );
        assert_eq!(decision.settled_by_host_label.as_deref(), Some("apollo"));
        assert!(decision.settled_response.is_some());
    }

    #[test]
    fn precondition_capability_missing() {
        let intent = gate_intent("notif-1", 42);
        let entry = entry_for(&intent);
        let empty = CapabilitySetWire {
            schema_version: FLEET_CONTRACT_SCHEMA_VERSION,
            resource: Vec::new(),
            host: Vec::new(),
            protocol: vec!["fleet.v1".to_string()],
        };
        let decision =
            evaluate_attention_precondition(&intent, Some(&entry), &empty)
                .unwrap();
        assert_eq!(
            decision.reason,
            FleetAttentionPreconditionReasonWire::CapabilityMissing
        );
    }

    #[test]
    fn precondition_invalid_option() {
        let mut intent = gate_intent("notif-1", 42);
        intent.selected_option_ids = vec!["not-an-option".to_string()];
        let entry = entry_for(&gate_intent("notif-1", 42));
        let decision = evaluate_attention_precondition(
            &intent,
            Some(&entry),
            &full_capabilities(),
        )
        .unwrap();
        assert_eq!(
            decision.reason,
            FleetAttentionPreconditionReasonWire::InvalidOption
        );
    }

    #[test]
    fn notice_dedupe_suppresses_reconnect_and_announces_new_revision() {
        let intent = gate_intent("notif-1", 1);
        let entry = entry_for(&intent);
        let first = decide_attention_notices(
            std::slice::from_ref(&entry),
            &[],
            3600.0,
            100.0,
        )
        .unwrap();
        assert_eq!(first.to_announce.len(), 1);
        assert!(first.suppressed.is_empty());

        // A reconnect with the identical revision announces nothing.
        let second = decide_attention_notices(
            std::slice::from_ref(&entry),
            &first.ledger,
            3600.0,
            101.0,
        )
        .unwrap();
        assert!(second.to_announce.is_empty());
        assert_eq!(second.suppressed.len(), 1);

        // A superseded and re-asked request (new revision) announces once.
        let mut superseded = entry.clone();
        superseded.revision = 2;
        let third = decide_attention_notices(
            &[superseded],
            &second.ledger,
            3600.0,
            102.0,
        )
        .unwrap();
        assert_eq!(third.to_announce.len(), 1);
        assert!(third.suppressed.is_empty());
    }

    #[test]
    fn notice_ledger_prunes_stale_entries_outside_retention_window() {
        let intent = gate_intent("notif-1", 1);
        let entry = entry_for(&intent);
        let announced =
            decide_attention_notices(&[entry], &[], 3600.0, 100.0).unwrap();
        assert_eq!(announced.ledger.len(), 1);
        // The request is no longer projected (e.g. settled/expired) and the
        // ledger entry has aged past retention: it is pruned.
        let pruned =
            decide_attention_notices(&[], &announced.ledger, 10.0, 10_200.0)
                .unwrap();
        assert!(pruned.ledger.is_empty());
        // Within the retention window, the entry is kept for later.
        let kept =
            decide_attention_notices(&[], &announced.ledger, 10.0, 105.0)
                .unwrap();
        assert_eq!(kept.ledger.len(), 1);
    }
}

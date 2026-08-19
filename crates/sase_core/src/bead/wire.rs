//! Bead wire records matching `sase_100/src/sase/bead/model.py`.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::{de, Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::artifact_ref::normalize_artifact_ref_list;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusWire {
    #[default]
    Open,
    Claimed,
    Ready,
    Snoozed,
    InProgress,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueTypeWire {
    Plan,
    #[default]
    Phase,
    Task,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadTierWire {
    Plan,
    #[default]
    Epic,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadResolutionWire {
    Done,
    Canceled,
    Superseded,
}

impl BeadResolutionWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Canceled => "canceled",
            Self::Superseded => "superseded",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PhaseSizeWire {
    Xsmall,
    Small,
    Medium,
    Large,
    Xlarge,
}

impl PhaseSizeWire {
    pub const NAMES: [&'static str; 5] =
        ["xsmall", "small", "medium", "large", "xlarge"];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Xsmall => "xsmall",
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::Xlarge => "xlarge",
        }
    }

    pub fn from_name(value: &str) -> Option<Self> {
        match value {
            "xsmall" => Some(Self::Xsmall),
            "small" => Some(Self::Small),
            "medium" => Some(Self::Medium),
            "large" => Some(Self::Large),
            "xlarge" => Some(Self::Xlarge),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {message}")]
pub struct BeadError {
    pub kind: String,
    pub message: String,
}

impl BeadError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: "validation".to_string(),
            message: message.into(),
        }
    }

    pub fn io(message: impl Into<String>) -> Self {
        Self {
            kind: "io".to_string(),
            message: message.into(),
        }
    }

    pub fn json(message: impl Into<String>) -> Self {
        Self {
            kind: "json".to_string(),
            message: message.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self {
            kind: "conflict".to_string(),
            message: message.into(),
        }
    }
}

impl From<std::io::Error> for BeadError {
    fn from(error: std::io::Error) -> Self {
        Self::io(error.to_string())
    }
}

impl From<serde_json::Error> for BeadError {
    fn from(error: serde_json::Error) -> Self {
        Self::json(error.to_string())
    }
}

fn empty_string() -> String {
    String::new()
}

fn false_value() -> bool {
    false
}

fn deserialize_string_default_empty<'de, D>(
    deserializer: D,
) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

pub fn validate_model_value(model: &str) -> Result<(), BeadError> {
    if model.chars().any(char::is_control) {
        return Err(BeadError::validation(
            "model cannot contain control characters",
        ));
    }
    Ok(())
}

pub(crate) fn deserialize_option_non_empty_string<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    Ok(value.filter(|s| !s.is_empty()))
}

pub(crate) fn deserialize_option_phase_size<'de, D>(
    deserializer: D,
) -> Result<Option<PhaseSizeWire>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match value.as_deref() {
        None | Some("") => Ok(None),
        Some(value) => {
            PhaseSizeWire::from_name(value).map(Some).ok_or_else(|| {
                de::Error::unknown_variant(value, &PhaseSizeWire::NAMES)
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyWire {
    pub issue_id: String,
    pub depends_on_id: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub created_at: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub created_by: String,
}

/// One independently attributed corroboration of an existing task bead.
///
/// The collection on [`IssueWire`] is the source of truth for the visible
/// `+N` count.  Keeping the evidence structured and append-only prevents the
/// count from drifting away from the reports that justify it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskPlusOneEvidenceWire {
    pub timestamp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_since: Option<String>,
    pub reporter: String,
    pub note: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
}

impl TaskPlusOneEvidenceWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.timestamp.trim().is_empty() {
            return Err(BeadError::validation(
                "task +1 evidence timestamp cannot be empty or blank",
            ));
        }
        if let Some(observed_since) = &self.observed_since {
            parse_task_plus_one_observed_since(observed_since)?;
        }
        if self.reporter.trim().is_empty() {
            return Err(BeadError::validation(
                "task +1 reporter cannot be empty or blank",
            ));
        }
        if self.note.trim().is_empty() {
            return Err(BeadError::validation(
                "task +1 note cannot be empty or blank",
            ));
        }
        let normalized =
            normalize_artifact_ref_list(&self.refs).map_err(|error| {
                BeadError {
                    kind: error.kind,
                    message: error.message,
                }
            })?;
        if normalized != self.refs {
            return Err(BeadError::validation(
                "task +1 evidence refs must be normalized and deduplicated",
            ));
        }
        Ok(())
    }
}

pub(crate) fn parse_task_plus_one_observed_since(
    value: &str,
) -> Result<DateTime<FixedOffset>, BeadError> {
    DateTime::parse_from_rfc3339(value.trim()).map_err(|error| {
        BeadError::validation(format!(
            "task +1 evidence observed_since must be an RFC-3339 timestamp with an offset: {value:?} ({error})"
        ))
    })
}

/// Why a close was undone.
///
/// One variant per reducer branch that reopens a bead, so an archived record
/// can say what brought the bead back without the reader having to correlate
/// timestamps against the event log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadReopenCauseWire {
    PlusOne,
    Open,
    Update,
    EpicPreclaim,
}

impl BeadReopenCauseWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PlusOne => "plus_one",
            Self::Open => "open",
            Self::Update => "update",
            Self::EpicPreclaim => "epic_preclaim",
        }
    }
}

/// One close episode that has since been undone.
///
/// The flat `closed_at` / `close_reason` / `resolution` fields on
/// [`IssueWire`] always describe the *current* close; this record is strictly
/// the past.  Every record carries a `reopened_at`, because a record only
/// comes into existence once the close is undone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadCloseRecordWire {
    pub closed_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub close_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<BeadResolutionWire>,
    pub reopened_at: String,
    pub reopened_via: BeadReopenCauseWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reopened_by: Option<String>,
}

impl BeadCloseRecordWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.closed_at.trim().is_empty() {
            return Err(BeadError::validation(
                "close history closed_at cannot be empty or blank",
            ));
        }
        if self.reopened_at.trim().is_empty() {
            return Err(BeadError::validation(
                "close history reopened_at cannot be empty or blank",
            ));
        }
        if self
            .reopened_by
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err(BeadError::validation(
                "close history reopened_by cannot be empty or blank",
            ));
        }
        Ok(())
    }
}

/// The wake conditions a snoozed task bead is currently waiting on.
///
/// One embedded record, present exactly while the status is `snoozed` and
/// cleared on wake, is deliberate: no flat field can drift out of sync with
/// the status, and `sase bead history` replays the event stream when past
/// snoozes matter, so no separate history list is needed here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadSnoozeWire {
    /// Wake time, ISO-8601 with an offset.
    pub until: String,
    pub snoozed_at: String,
    pub snoozed_by: String,
    /// Absolute total +1 count that wakes the bead early.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plus_one_target: Option<u32>,
    /// +1 count when this snooze started, so surfaces can render progress.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plus_one_baseline: Option<u32>,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub reason: String,
}

impl BeadSnoozeWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.snoozed_at.trim().is_empty() {
            return Err(BeadError::validation(
                "bead snooze snoozed_at cannot be empty or blank",
            ));
        }
        if self.snoozed_by.trim().is_empty() {
            return Err(BeadError::validation(
                "bead snooze snoozed_by cannot be empty or blank",
            ));
        }
        parse_snooze_timestamp(&self.until, "until")?;
        if let Some(target) = self.plus_one_target {
            let baseline = self.plus_one_baseline.unwrap_or(0);
            if target <= baseline {
                return Err(BeadError::validation(format!(
                    "bead snooze plus_one_target must exceed plus_one_baseline: {target} <= {baseline}"
                )));
            }
        }
        Ok(())
    }

    /// Return the wake time as an absolute instant.
    ///
    /// Validation already rejects an unparsable `until`, so every stored
    /// record answers this; the `Result` keeps decoded-from-disk callers
    /// honest about a record written by something that skipped validation.
    pub fn until_datetime(&self) -> Result<DateTime<FixedOffset>, BeadError> {
        parse_snooze_timestamp(&self.until, "until")
    }
}

/// Parse one snooze timestamp, requiring an explicit UTC offset.
///
/// A snooze is compared against wall-clock time on several surfaces, so an
/// offset-less timestamp is rejected at write time rather than silently
/// meaning different instants to different readers.
pub fn parse_snooze_timestamp(
    value: &str,
    field: &str,
) -> Result<DateTime<FixedOffset>, BeadError> {
    DateTime::parse_from_rfc3339(value.trim()).map_err(|error| {
        BeadError::validation(format!(
            "bead snooze {field} must be an RFC-3339 timestamp with an offset: {value:?} ({error})"
        ))
    })
}

/// Return whether a flag task bead's calendar and release thresholds are both
/// reached.
///
/// Thresholds now live on `task_type_fields` of a `task` bead whose
/// `task_type` is `flag`. Unparseable values are not due.
pub fn flag_thresholds_due(
    remove_by_date: &str,
    remove_by_release: &str,
    today: NaiveDate,
    release: &str,
) -> bool {
    let Ok(remove_by_date) =
        NaiveDate::parse_from_str(remove_by_date.trim(), "%Y-%m-%d")
    else {
        return false;
    };
    let Some(current_release) = release_tuple(release) else {
        return false;
    };
    let Some(remove_by_release) = release_tuple(remove_by_release) else {
        return false;
    };
    today >= remove_by_date && current_release >= remove_by_release
}

fn release_tuple(value: &str) -> Option<(u64, u64, u64)> {
    let core = value.trim().split(['-', '+']).next()?;
    let mut parts = core.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor, patch))
}

const TASK_TYPE_SLUG_MAX_LEN: usize = 32;
const TASK_TYPE_FIELD_NAME_MAX_LEN: usize = 64;

fn validate_task_type_metadata(
    issue_type: &IssueTypeWire,
    task_type: Option<&str>,
    task_type_fields: &BTreeMap<String, String>,
) -> Result<(), BeadError> {
    if *issue_type != IssueTypeWire::Task {
        if task_type.is_some() || !task_type_fields.is_empty() {
            return Err(BeadError::validation(
                "Only task issues can carry task_type metadata",
            ));
        }
        return Ok(());
    }
    if task_type.is_none() && !task_type_fields.is_empty() {
        return Err(BeadError::validation(
            "task_type_fields requires task_type",
        ));
    }
    if let Some(task_type) = task_type {
        validate_snake_case_slug(
            task_type,
            "task_type",
            TASK_TYPE_SLUG_MAX_LEN,
        )?;
    }
    for key in task_type_fields.keys() {
        validate_snake_case_slug(
            key,
            "task_type_fields key",
            TASK_TYPE_FIELD_NAME_MAX_LEN,
        )?;
    }
    Ok(())
}

fn validate_snake_case_slug(
    value: &str,
    what: &str,
    max_len: usize,
) -> Result<(), BeadError> {
    if value.len() <= max_len && is_snake_case_key(value) {
        Ok(())
    } else {
        Err(BeadError::validation(format!(
            "{what} must be a non-empty snake_case slug at most {max_len} characters: {value:?}"
        )))
    }
}

fn is_snake_case_key(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() {
        return false;
    }
    let mut prev_underscore = false;
    for character in chars {
        if character == '_' {
            if prev_underscore {
                return false;
            }
            prev_underscore = true;
        } else if character.is_ascii_lowercase() || character.is_ascii_digit() {
            prev_underscore = false;
        } else {
            return false;
        }
    }
    !prev_underscore
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IssueWire {
    pub id: String,
    pub title: String,
    pub status: StatusWire,
    pub issue_type: IssueTypeWire,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tier: Option<BeadTierWire>,
    #[serde(default, deserialize_with = "deserialize_option_non_empty_string")]
    pub parent_id: Option<String>,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub owner: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub assignee: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub created_at: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub created_by: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub updated_at: String,
    #[serde(default, deserialize_with = "deserialize_option_non_empty_string")]
    pub closed_at: Option<String>,
    #[serde(default, deserialize_with = "deserialize_option_non_empty_string")]
    pub close_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolution: Option<BeadResolutionWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub close_history: Vec<BeadCloseRecordWire>,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub description: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub notes: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub design: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<crate::artifact_link::BeadLinkWire>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub plus_one_evidence: Vec<TaskPlusOneEvidenceWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snooze: Option<BeadSnoozeWire>,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub model: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_phase_size"
    )]
    pub size: Option<PhaseSizeWire>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_option_non_empty_string"
    )]
    pub task_type: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub task_type_fields: BTreeMap<String, String>,
    #[serde(default = "false_value")]
    pub is_ready_to_work: bool,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub changespec_name: String,
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub changespec_bug_id: String,
    #[serde(
        default = "empty_string",
        skip_serializing_if = "String::is_empty",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub external_ref: String,
    #[serde(default)]
    pub dependencies: Vec<DependencyWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadSearchMatchWire {
    pub issue: IssueWire,
    pub matched_fields: Vec<String>,
}

impl IssueWire {
    pub fn plus_one_count(&self) -> usize {
        self.plus_one_evidence.len()
    }

    pub fn is_flag_task(&self) -> bool {
        self.issue_type == IssueTypeWire::Task
            && self.task_type.as_deref() == Some("flag")
    }

    pub fn flag_is_due(&self, today: NaiveDate, release: &str) -> bool {
        if !self.is_flag_task() {
            return false;
        }
        let Some(remove_by_date) = self.task_type_fields.get("remove_by_date")
        else {
            return false;
        };
        let Some(remove_by_release) =
            self.task_type_fields.get("remove_by_release")
        else {
            return false;
        };
        flag_thresholds_due(remove_by_date, remove_by_release, today, release)
    }

    pub fn validate(&self) -> Result<(), BeadError> {
        if self.issue_type == IssueTypeWire::Phase && self.parent_id.is_none() {
            return Err(BeadError::validation(
                "Phase issues must have a parent_id",
            ));
        }
        if self.issue_type == IssueTypeWire::Phase && self.tier.is_some() {
            return Err(BeadError::validation(
                "Phase issues cannot carry plan tier metadata",
            ));
        }
        if self.issue_type == IssueTypeWire::Task && self.parent_id.is_some() {
            return Err(BeadError::validation(
                "Task issues cannot have a parent_id",
            ));
        }
        if self.issue_type == IssueTypeWire::Task && self.tier.is_some() {
            return Err(BeadError::validation(
                "Task issues cannot carry plan tier metadata",
            ));
        }
        if self.issue_type != IssueTypeWire::Task
            && !self.plus_one_evidence.is_empty()
        {
            return Err(BeadError::validation(
                "Only task issues can carry +1 evidence",
            ));
        }
        let mut reporters = BTreeSet::new();
        for evidence in &self.plus_one_evidence {
            evidence.validate()?;
            if !reporters.insert(evidence.reporter.as_str()) {
                return Err(BeadError::validation(format!(
                    "duplicate task +1 reporter: {}",
                    evidence.reporter
                )));
            }
        }
        if self.issue_type != IssueTypeWire::Plan && self.is_ready_to_work {
            return Err(BeadError::validation(
                "Only plan issues can be marked is_ready_to_work",
            ));
        }
        if self.issue_type == IssueTypeWire::Plan && self.size.is_some() {
            return Err(BeadError::validation(
                "Only phase and task issues can carry size metadata",
            ));
        }
        validate_task_type_metadata(
            &self.issue_type,
            self.task_type.as_deref(),
            &self.task_type_fields,
        )?;
        if self.issue_type != IssueTypeWire::Plan
            && (!self.changespec_name.is_empty()
                || !self.changespec_bug_id.is_empty())
        {
            return Err(BeadError::validation(
                "Only plan issues can carry Patch metadata",
            ));
        }
        if self.status == StatusWire::Ready
            && self.issue_type != IssueTypeWire::Task
        {
            return Err(BeadError::validation(
                "Only task issues can have ready status",
            ));
        }
        if self.status == StatusWire::Snoozed
            && self.issue_type != IssueTypeWire::Task
        {
            return Err(BeadError::validation(
                "Only task issues can have snoozed status",
            ));
        }
        match (&self.status, &self.snooze) {
            (StatusWire::Snoozed, None) => {
                return Err(BeadError::validation(
                    "snoozed issues must carry snooze metadata",
                ));
            }
            (status, Some(_)) if *status != StatusWire::Snoozed => {
                return Err(BeadError::validation(
                    "Only snoozed issues can carry snooze metadata",
                ));
            }
            _ => {}
        }
        if let Some(snooze) = &self.snooze {
            snooze.validate()?;
        }
        if !self.changespec_bug_id.is_empty() && self.changespec_name.is_empty()
        {
            return Err(BeadError::validation(
                "changespec_bug_id requires changespec_name",
            ));
        }
        if self.status != StatusWire::Closed && self.resolution.is_some() {
            return Err(BeadError::validation(
                "Only closed issues can carry resolution metadata",
            ));
        }
        for record in &self.close_history {
            record.validate()?;
        }
        validate_model_value(&self.model)?;
        Ok(())
    }
}

pub(crate) fn deserialize_valid_issue(
    value: serde_json::Value,
) -> Result<IssueWire, BeadError> {
    let issue: IssueWire = serde_json::from_value(value)?;
    issue.validate()?;
    Ok(issue)
}

pub(crate) fn validate_unique_external_refs(
    issues: &[IssueWire],
) -> Result<(), BeadError> {
    let mut owner_by_ref: BTreeSet<(&str, &str)> = BTreeSet::new();
    for issue in issues {
        let external_ref = issue.external_ref.trim();
        if external_ref.is_empty() {
            continue;
        }
        if let Some((existing_ref, existing_id)) = owner_by_ref
            .iter()
            .find(|(candidate_ref, _)| *candidate_ref == external_ref)
            .copied()
        {
            return Err(BeadError::conflict(format!(
                "external_ref {existing_ref} already belongs to {existing_id}; cannot also assign it to {}",
                issue.id
            )));
        }
        owner_by_ref.insert((external_ref, issue.id.as_str()));
    }
    Ok(())
}

pub(crate) fn invalid_record_error<E: std::fmt::Display>(
    error: E,
) -> de::value::Error {
    de::Error::custom(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn phase(parent_id: Option<&str>) -> IssueWire {
        IssueWire {
            id: "test-1".to_string(),
            title: "A phase".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Phase,
            tier: None,
            parent_id: parent_id.map(str::to_string),
            owner: String::new(),
            assignee: String::new(),
            created_at: String::new(),
            created_by: String::new(),
            updated_at: String::new(),
            closed_at: None,
            close_reason: None,
            resolution: None,
            close_history: Vec::new(),
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs: Vec::new(),
            links: Vec::new(),
            plus_one_evidence: Vec::new(),
            snooze: None,
            model: String::new(),
            size: None,
            task_type: None,
            task_type_fields: BTreeMap::new(),
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            external_ref: String::new(),
            dependencies: vec![],
        }
    }

    fn close_record() -> BeadCloseRecordWire {
        BeadCloseRecordWire {
            closed_at: "2026-07-30T09:12:04Z".to_string(),
            close_reason: Some("Not reproducible on main.".to_string()),
            resolution: Some(BeadResolutionWire::Canceled),
            reopened_at: "2026-08-05T17:04:11Z".to_string(),
            reopened_via: BeadReopenCauseWire::PlusOne,
            reopened_by: Some("claude.probe".to_string()),
        }
    }

    #[test]
    fn close_history_round_trips_and_stays_absent_when_empty() {
        let mut issue = phase(Some("test-0"));
        assert!(issue.close_history.is_empty());
        let without_history = serde_json::to_value(&issue).unwrap();
        assert!(without_history.get("close_history").is_none());

        issue.close_history.push(close_record());
        issue.validate().unwrap();
        let encoded = serde_json::to_value(&issue).unwrap();
        assert_eq!(
            encoded["close_history"],
            json!([{
                "closed_at": "2026-07-30T09:12:04Z",
                "close_reason": "Not reproducible on main.",
                "resolution": "canceled",
                "reopened_at": "2026-08-05T17:04:11Z",
                "reopened_via": "plus_one",
                "reopened_by": "claude.probe",
            }])
        );
        let decoded: IssueWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, issue);

        // A row written before this change carries no key at all.
        let legacy =
            serde_json::from_value::<IssueWire>(without_history).unwrap();
        assert!(legacy.close_history.is_empty());
    }

    #[test]
    fn close_history_omits_absent_reason_and_resolution() {
        let record = BeadCloseRecordWire {
            close_reason: None,
            resolution: None,
            reopened_via: BeadReopenCauseWire::Open,
            reopened_by: None,
            ..close_record()
        };
        assert_eq!(
            serde_json::to_value(&record).unwrap(),
            json!({
                "closed_at": "2026-07-30T09:12:04Z",
                "reopened_at": "2026-08-05T17:04:11Z",
                "reopened_via": "open",
            })
        );
    }

    #[test]
    fn close_history_validation_rejects_blank_fields() {
        let mut issue = phase(Some("test-0"));

        issue.close_history = vec![BeadCloseRecordWire {
            closed_at: "  ".to_string(),
            ..close_record()
        }];
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("closed_at cannot be empty"));

        issue.close_history = vec![BeadCloseRecordWire {
            reopened_at: String::new(),
            ..close_record()
        }];
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("reopened_at cannot be empty"));

        issue.close_history = vec![BeadCloseRecordWire {
            reopened_by: Some(" ".to_string()),
            ..close_record()
        }];
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("reopened_by cannot be empty"));

        issue.close_history = vec![close_record()];
        issue.validate().unwrap();
    }

    #[test]
    fn close_history_is_allowed_on_every_issue_type() {
        for (issue_type, parent_id, tier) in [
            (IssueTypeWire::Phase, Some("test-0"), None),
            (IssueTypeWire::Plan, None, Some(BeadTierWire::Epic)),
            (IssueTypeWire::Task, None, None),
        ] {
            let mut issue = phase(parent_id);
            issue.issue_type = issue_type.clone();
            issue.parent_id = parent_id.map(str::to_string);
            issue.tier = tier;
            issue.close_history = vec![close_record()];
            issue.validate().unwrap();
        }
    }

    #[test]
    fn phase_requires_parent() {
        let issue = phase(None);
        let error = issue.validate().unwrap_err();
        assert_eq!(error.message, "Phase issues must have a parent_id");
    }

    #[test]
    fn phase_rejects_plan_only_fields() {
        let mut issue = phase(Some("test-0"));
        issue.is_ready_to_work = true;
        assert!(issue.validate().unwrap_err().message.contains("Only plan"));

        issue.is_ready_to_work = false;
        issue.changespec_name = "feature_epic".to_string();
        assert!(issue.validate().unwrap_err().message.contains("Only plan"));
    }

    #[test]
    fn bug_id_requires_changespec_name() {
        let mut issue = phase(Some("test-0"));
        issue.issue_type = IssueTypeWire::Plan;
        issue.parent_id = None;
        issue.changespec_bug_id = "BUG-1".to_string();
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("requires changespec_name"));
    }

    #[test]
    fn legacy_defaults_match_python_jsonl_loader() {
        let issue: IssueWire = serde_json::from_value(json!({
            "id": "legacy-1",
            "title": "Legacy",
            "status": "open",
            "issue_type": "plan",
            "parent_id": null,
            "created_at": null,
            "updated_at": "2026-01-01T00:00:00Z",
            "dependencies": [{
                "issue_id": "legacy-1",
                "depends_on_id": "legacy-0"
            }]
        }))
        .unwrap();

        assert_eq!(issue.created_at, "");
        assert!(!issue.is_ready_to_work);
        assert_eq!(issue.model, "");
        assert_eq!(issue.size, None);
        assert!(issue.plus_one_evidence.is_empty());
        assert_eq!(issue.changespec_name, "");
        assert_eq!(issue.external_ref, "");
        assert_eq!(issue.dependencies[0].created_at, "");
    }

    #[test]
    fn external_ref_round_trips_when_non_empty() {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.external_ref = "bug:sase#42".to_string();

        let encoded = serde_json::to_value(&issue).unwrap();
        assert_eq!(encoded["external_ref"], "bug:sase#42");

        let decoded: IssueWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.external_ref, "bug:sase#42");
        decoded.validate().unwrap();
    }

    #[test]
    fn task_plus_one_evidence_validates_structure_and_reporter_uniqueness() {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.size = Some(PhaseSizeWire::Small);
        issue.plus_one_evidence = vec![TaskPlusOneEvidenceWire {
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            observed_since: None,
            reporter: "agent-a".to_string(),
            note: "reproduced".to_string(),
            refs: vec!["research:202608/repro.md".to_string()],
        }];
        issue.validate().unwrap();
        assert_eq!(issue.plus_one_count(), 1);

        issue
            .plus_one_evidence
            .push(issue.plus_one_evidence[0].clone());
        assert_eq!(
            issue.validate().unwrap_err().message,
            "duplicate task +1 reporter: agent-a"
        );

        issue.plus_one_evidence.truncate(1);
        issue.plus_one_evidence[0].note = "   ".to_string();
        assert!(issue.validate().unwrap_err().message.contains("note"));

        issue.plus_one_evidence[0].note = "reproduced".to_string();
        issue.plus_one_evidence[0].observed_since =
            Some("not-a-timestamp".to_string());
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("observed_since"));

        issue.plus_one_evidence[0].observed_since = None;
        issue.issue_type = IssueTypeWire::Plan;
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only task issues can carry +1 evidence"
        );
    }

    #[test]
    fn model_rejects_control_characters() {
        let mut issue = phase(Some("test-0"));
        issue.model = "codex/gpt-5.5\n%tag:bad".to_string();

        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("model cannot contain"));
    }

    #[test]
    fn phase_size_round_trips_all_enum_values_and_rejects_plan_usage() {
        for (size, expected) in [
            (PhaseSizeWire::Xsmall, "xsmall"),
            (PhaseSizeWire::Small, "small"),
            (PhaseSizeWire::Medium, "medium"),
            (PhaseSizeWire::Large, "large"),
            (PhaseSizeWire::Xlarge, "xlarge"),
        ] {
            let mut issue = phase(Some("test-0"));
            issue.size = Some(size.clone());
            issue.validate().unwrap();

            let value = serde_json::to_value(&issue).unwrap();
            assert_eq!(value["size"], expected);
            let round_tripped: IssueWire =
                serde_json::from_value(value).unwrap();
            assert_eq!(round_tripped.size, Some(size));
        }

        let mut issue = phase(Some("test-0"));
        issue.size = Some(PhaseSizeWire::Xlarge);
        issue.issue_type = IssueTypeWire::Plan;
        issue.parent_id = None;
        let error = issue.validate().unwrap_err();
        assert_eq!(
            error.message,
            "Only phase and task issues can carry size metadata"
        );
    }

    #[test]
    fn task_validation_allows_size_and_ready_but_rejects_plan_fields() {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.status = StatusWire::Ready;
        issue.size = Some(PhaseSizeWire::Medium);
        issue.validate().unwrap();

        issue.parent_id = Some("test-0".to_string());
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Task issues cannot have a parent_id"
        );
        issue.parent_id = None;
        issue.tier = Some(BeadTierWire::Plan);
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Task issues cannot carry plan tier metadata"
        );
        issue.tier = None;
        issue.is_ready_to_work = true;
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only plan issues can be marked is_ready_to_work"
        );
        issue.is_ready_to_work = false;
        issue.changespec_name = "not_allowed".to_string();
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only plan issues can carry Patch metadata"
        );
    }

    #[test]
    fn ready_status_requires_task_type() {
        let mut issue = phase(Some("test-0"));
        issue.status = StatusWire::Ready;

        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only task issues can have ready status"
        );
    }

    fn snoozed_task(snooze: BeadSnoozeWire) -> IssueWire {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.status = StatusWire::Snoozed;
        issue.snooze = Some(snooze);
        issue
    }

    fn snooze_record() -> BeadSnoozeWire {
        BeadSnoozeWire {
            until: "2026-08-09T09:00:00-04:00".to_string(),
            snoozed_at: "2026-08-06T09:00:00-04:00".to_string(),
            snoozed_by: "bryanbugyi34@gmail.com".to_string(),
            plus_one_target: None,
            plus_one_baseline: None,
            reason: String::new(),
        }
    }

    #[test]
    fn snoozed_status_round_trips_with_its_record() {
        let issue = snoozed_task(BeadSnoozeWire {
            plus_one_target: Some(3),
            plus_one_baseline: Some(1),
            reason: "waiting on upstream".to_string(),
            ..snooze_record()
        });
        issue.validate().unwrap();

        let value = serde_json::to_value(&issue).unwrap();
        assert_eq!(value["status"], "snoozed");
        assert_eq!(
            value["snooze"],
            json!({
                "until": "2026-08-09T09:00:00-04:00",
                "snoozed_at": "2026-08-06T09:00:00-04:00",
                "snoozed_by": "bryanbugyi34@gmail.com",
                "plus_one_target": 3,
                "plus_one_baseline": 1,
                "reason": "waiting on upstream",
            })
        );
        let round_tripped: IssueWire = serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped, issue);
    }

    #[test]
    fn snooze_key_is_absent_when_the_bead_is_not_snoozed() {
        let issue = phase(Some("test-0"));
        let value = serde_json::to_value(&issue).unwrap();
        assert!(value.get("snooze").is_none());
        let decoded: IssueWire = serde_json::from_value(value).unwrap();
        assert_eq!(decoded.snooze, None);
    }

    #[test]
    fn snoozed_status_requires_a_task_and_a_record() {
        let mut issue = snoozed_task(snooze_record());
        issue.issue_type = IssueTypeWire::Phase;
        issue.parent_id = Some("test-0".to_string());
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only task issues can have snoozed status"
        );

        let mut issue = snoozed_task(snooze_record());
        issue.snooze = None;
        assert_eq!(
            issue.validate().unwrap_err().message,
            "snoozed issues must carry snooze metadata"
        );
    }

    #[test]
    fn snooze_metadata_requires_snoozed_status() {
        let mut issue = snoozed_task(snooze_record());
        issue.status = StatusWire::Ready;
        assert_eq!(
            issue.validate().unwrap_err().message,
            "Only snoozed issues can carry snooze metadata"
        );
    }

    #[test]
    fn snooze_record_validates_its_own_fields() {
        let issue = snoozed_task(BeadSnoozeWire {
            snoozed_at: "  ".to_string(),
            ..snooze_record()
        });
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("snoozed_at cannot be empty"));

        let issue = snoozed_task(BeadSnoozeWire {
            snoozed_by: String::new(),
            ..snooze_record()
        });
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("snoozed_by cannot be empty"));

        // Offset-less and non-timestamp wake times are both rejected.
        for until in ["2026-08-09T09:00:00", "soon", ""] {
            let issue = snoozed_task(BeadSnoozeWire {
                until: until.to_string(),
                ..snooze_record()
            });
            assert!(issue
                .validate()
                .unwrap_err()
                .message
                .contains("until must be an RFC-3339 timestamp"));
        }

        let issue = snoozed_task(BeadSnoozeWire {
            plus_one_target: Some(2),
            plus_one_baseline: Some(2),
            ..snooze_record()
        });
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("plus_one_target must exceed plus_one_baseline"));

        let issue = snoozed_task(BeadSnoozeWire {
            plus_one_target: Some(1),
            plus_one_baseline: None,
            ..snooze_record()
        });
        issue.validate().unwrap();
        assert_eq!(
            issue.snooze.unwrap().until_datetime().unwrap().to_rfc3339(),
            "2026-08-09T09:00:00-04:00"
        );
    }

    #[test]
    fn claimed_status_round_trips_through_serde() {
        let mut issue = phase(Some("test-0"));
        issue.status = StatusWire::Claimed;

        let value = serde_json::to_value(&issue).unwrap();
        assert_eq!(value["status"], "claimed");
        let round_tripped: IssueWire = serde_json::from_value(value).unwrap();
        assert_eq!(round_tripped.status, StatusWire::Claimed);
    }

    #[test]
    fn empty_phase_size_normalizes_to_none_and_invalid_values_fail() {
        let mut value = serde_json::to_value(phase(Some("test-0"))).unwrap();
        value["size"] = json!("");
        let issue: IssueWire = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(issue.size, None);

        value["size"] = json!("huge");
        let error = serde_json::from_value::<IssueWire>(value).unwrap_err();
        assert!(error.to_string().contains("unknown variant `huge`"));
    }

    #[test]
    fn empty_optional_strings_normalize_to_none() {
        let issue: IssueWire = serde_json::from_value(json!({
            "id": "legacy-1",
            "title": "Legacy",
            "status": "open",
            "issue_type": "plan",
            "parent_id": "",
            "closed_at": "",
            "close_reason": "",
            "created_at": "",
            "updated_at": "",
            "dependencies": []
        }))
        .unwrap();

        assert_eq!(issue.parent_id, None);
        assert_eq!(issue.closed_at, None);
        assert_eq!(issue.close_reason, None);
        assert_eq!(issue.resolution, None);
    }

    #[test]
    fn resolution_round_trips_and_requires_closed_status() {
        for (resolution, expected) in [
            (BeadResolutionWire::Done, "done"),
            (BeadResolutionWire::Canceled, "canceled"),
            (BeadResolutionWire::Superseded, "superseded"),
        ] {
            let mut issue = phase(Some("test-0"));
            issue.status = StatusWire::Closed;
            issue.resolution = Some(resolution.clone());
            issue.validate().unwrap();

            let value = serde_json::to_value(&issue).unwrap();
            assert_eq!(value["resolution"], expected);
            let round_tripped: IssueWire =
                serde_json::from_value(value).unwrap();
            assert_eq!(round_tripped.resolution, Some(resolution));
        }

        let mut issue = phase(Some("test-0"));
        issue.resolution = Some(BeadResolutionWire::Done);
        let error = issue.validate().unwrap_err();
        assert_eq!(
            error.message,
            "Only closed issues can carry resolution metadata"
        );
    }

    fn typed_task(
        task_type: Option<&str>,
        fields: BTreeMap<String, String>,
    ) -> IssueWire {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.task_type = task_type.map(str::to_string);
        issue.task_type_fields = fields;
        issue
    }

    #[test]
    fn task_type_round_trips_and_stays_absent_when_empty() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "node_id".to_string(),
            "tests/foo.py::test_bar".to_string(),
        );
        fields.insert("evidence".to_string(), "failed then passed".to_string());
        let issue = typed_task(Some("flake"), fields);
        issue.validate().unwrap();

        let encoded = serde_json::to_value(&issue).unwrap();
        assert_eq!(encoded["task_type"], "flake");
        assert_eq!(
            encoded["task_type_fields"],
            json!({
                "evidence": "failed then passed",
                "node_id": "tests/foo.py::test_bar",
            })
        );
        let decoded: IssueWire = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, issue);

        let untyped = typed_task(None, BTreeMap::new());
        untyped.validate().unwrap();
        let without_type = serde_json::to_value(&untyped).unwrap();
        assert!(without_type.get("task_type").is_none());
        assert!(without_type.get("task_type_fields").is_none());
        let legacy = serde_json::from_value::<IssueWire>(without_type).unwrap();
        assert_eq!(legacy.task_type, None);
        assert!(legacy.task_type_fields.is_empty());
    }

    #[test]
    fn empty_task_type_normalizes_to_none() {
        let mut value =
            serde_json::to_value(typed_task(None, BTreeMap::new())).unwrap();
        value["task_type"] = json!("");
        let issue: IssueWire = serde_json::from_value(value).unwrap();
        assert_eq!(issue.task_type, None);
        issue.validate().unwrap();
    }

    #[test]
    fn task_type_is_rejected_on_non_task_issues() {
        for (issue_type, parent_id, tier) in [
            (IssueTypeWire::Phase, Some("test-0"), None),
            (IssueTypeWire::Plan, None, Some(BeadTierWire::Epic)),
        ] {
            let mut issue = phase(parent_id);
            issue.issue_type = issue_type;
            issue.parent_id = parent_id.map(str::to_string);
            issue.tier = tier;
            issue.task_type = Some("flake".to_string());
            assert_eq!(
                issue.validate().unwrap_err().message,
                "Only task issues can carry task_type metadata"
            );

            issue.task_type = None;
            issue
                .task_type_fields
                .insert("node_id".to_string(), "x".to_string());
            assert_eq!(
                issue.validate().unwrap_err().message,
                "Only task issues can carry task_type metadata"
            );
        }
    }

    #[test]
    fn task_type_fields_require_task_type() {
        let mut fields = BTreeMap::new();
        fields.insert(
            "node_id".to_string(),
            "tests/foo.py::test_bar".to_string(),
        );
        let issue = typed_task(None, fields);
        assert_eq!(
            issue.validate().unwrap_err().message,
            "task_type_fields requires task_type"
        );
    }

    #[test]
    fn task_type_and_field_keys_must_be_bounded_snake_case() {
        let issue = typed_task(Some("Not_Snake"), BTreeMap::new());
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("task_type must be a non-empty snake_case slug"));

        let issue = typed_task(Some(&"a".repeat(33)), BTreeMap::new());
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("at most 32 characters"));

        let issue = typed_task(Some("flake"), BTreeMap::new());
        issue.validate().unwrap();

        let mut fields = BTreeMap::new();
        fields.insert("Node-Id".to_string(), "x".to_string());
        let issue = typed_task(Some("flake"), fields);
        assert!(issue.validate().unwrap_err().message.contains(
            "task_type_fields key must be a non-empty snake_case slug"
        ));

        let mut fields = BTreeMap::new();
        fields.insert("a".repeat(65), "x".to_string());
        let issue = typed_task(Some("flake"), fields);
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("at most 64 characters"));

        let mut fields = BTreeMap::new();
        fields.insert("node_id".to_string(), String::new());
        typed_task(Some("github"), fields).validate().unwrap();
    }
}

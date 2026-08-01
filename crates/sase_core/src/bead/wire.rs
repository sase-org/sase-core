//! Bead wire records matching `sase_100/src/sase/bead/model.py`.

use std::collections::BTreeSet;

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
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Xsmall => "xsmall",
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
            Self::Xlarge => "xlarge",
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

fn deserialize_option_non_empty_string<'de, D>(
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
        Some("xsmall") => Ok(Some(PhaseSizeWire::Xsmall)),
        Some("small") => Ok(Some(PhaseSizeWire::Small)),
        Some("medium") => Ok(Some(PhaseSizeWire::Medium)),
        Some("large") => Ok(Some(PhaseSizeWire::Large)),
        Some("xlarge") => Ok(Some(PhaseSizeWire::Xlarge)),
        Some(value) => Err(de::Error::unknown_variant(
            value,
            &["xsmall", "small", "medium", "large", "xlarge"],
        )),
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
    pub plus_one_evidence: Vec<TaskPlusOneEvidenceWire>,
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
        if self.issue_type != IssueTypeWire::Plan
            && (!self.changespec_name.is_empty()
                || !self.changespec_bug_id.is_empty())
        {
            return Err(BeadError::validation(
                "Only plan issues can carry ChangeSpec metadata",
            ));
        }
        if self.status == StatusWire::Ready
            && self.issue_type != IssueTypeWire::Task
        {
            return Err(BeadError::validation(
                "Only task issues can have ready status",
            ));
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
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            refs: Vec::new(),
            plus_one_evidence: Vec::new(),
            model: String::new(),
            size: None,
            is_ready_to_work: false,
            changespec_name: String::new(),
            changespec_bug_id: String::new(),
            dependencies: vec![],
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
        assert_eq!(issue.dependencies[0].created_at, "");
    }

    #[test]
    fn task_plus_one_evidence_validates_structure_and_reporter_uniqueness() {
        let mut issue = phase(None);
        issue.issue_type = IssueTypeWire::Task;
        issue.size = Some(PhaseSizeWire::Small);
        issue.plus_one_evidence = vec![TaskPlusOneEvidenceWire {
            timestamp: "2026-01-01T00:00:00Z".to_string(),
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
            "Only plan issues can carry ChangeSpec metadata"
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
}

//! Bead wire records matching `sase_100/src/sase/bead/model.py`.

use serde::{de, Deserialize, Deserializer, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusWire {
    #[default]
    Open,
    InProgress,
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IssueTypeWire {
    Plan,
    #[default]
    Phase,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadTierWire {
    Plan,
    #[default]
    Epic,
    Legend,
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
    Ok(value.and_then(|s| if s.is_empty() { None } else { Some(s) }))
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
    #[serde(
        default = "empty_string",
        deserialize_with = "deserialize_string_default_empty"
    )]
    pub model: String,
    #[serde(default = "false_value")]
    pub is_ready_to_work: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epic_count: Option<i64>,
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

impl IssueWire {
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
        if self.issue_type == IssueTypeWire::Phase && self.is_ready_to_work {
            return Err(BeadError::validation(
                "Only plan issues can be marked is_ready_to_work",
            ));
        }
        if self.issue_type != IssueTypeWire::Plan && self.epic_count.is_some() {
            return Err(BeadError::validation(
                "Only plan issues can carry epic_count",
            ));
        }
        if let Some(epic_count) = self.epic_count {
            if self.tier != Some(BeadTierWire::Legend) {
                return Err(BeadError::validation(
                    "Only legend plan beads can carry epic_count",
                ));
            }
            if epic_count <= 0 {
                return Err(BeadError::validation(
                    "epic_count must be a positive integer",
                ));
            }
        }
        if self.issue_type == IssueTypeWire::Phase
            && (!self.changespec_name.is_empty()
                || !self.changespec_bug_id.is_empty())
        {
            return Err(BeadError::validation(
                "Phase issues cannot carry ChangeSpec metadata",
            ));
        }
        if !self.changespec_bug_id.is_empty() && self.changespec_name.is_empty()
        {
            return Err(BeadError::validation(
                "changespec_bug_id requires changespec_name",
            ));
        }
        validate_model_value(&self.model)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationOutcomeWire {
    pub operation: String,
    pub changed: bool,
    #[serde(default)]
    pub issue_ids: Vec<String>,
    #[serde(default)]
    pub message: String,
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
            description: String::new(),
            notes: String::new(),
            design: String::new(),
            model: String::new(),
            is_ready_to_work: false,
            epic_count: None,
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
        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("cannot carry"));
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
        assert_eq!(issue.epic_count, None);
        assert_eq!(issue.model, "");
        assert_eq!(issue.changespec_name, "");
        assert_eq!(issue.dependencies[0].created_at, "");
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
    }

    #[test]
    fn legend_plan_allows_positive_epic_count() {
        let mut issue = phase(Some("test-0"));
        issue.issue_type = IssueTypeWire::Plan;
        issue.parent_id = None;
        issue.tier = Some(BeadTierWire::Legend);
        issue.epic_count = Some(3);

        issue.validate().unwrap();
    }

    #[test]
    fn non_legend_epic_count_is_invalid() {
        let mut issue = phase(Some("test-0"));
        issue.issue_type = IssueTypeWire::Plan;
        issue.parent_id = None;
        issue.tier = Some(BeadTierWire::Epic);
        issue.epic_count = Some(3);

        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("Only legend plan beads"));
    }

    #[test]
    fn non_positive_epic_count_is_invalid() {
        let mut issue = phase(Some("test-0"));
        issue.issue_type = IssueTypeWire::Plan;
        issue.parent_id = None;
        issue.tier = Some(BeadTierWire::Legend);
        issue.epic_count = Some(0);

        assert!(issue
            .validate()
            .unwrap_err()
            .message
            .contains("positive integer"));
    }
}

//! Bead event wire records and payload validation.
//!
//! Owns every `BeadEvent*Wire` type plus the link-payload validators
//! and small link helpers the other event submodules build on.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::artifact_link::{
    canonicalize_artifact_link_ref, lookup_artifact_relation,
    validate_artifact_link_description,
};
use crate::serde_option::deserialize_present_option;

use super::super::wire::{
    BeadError, BeadResolutionWire, BeadSnoozeWire, BeadTierWire,
    DependencyWire, IssueWire, PhaseSizeWire, StatusWire,
    TaskPlusOneEvidenceWire,
};
use super::merge::remapped_id;

pub const BEAD_EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStoreManifestWire {
    pub schema_version: u32,
    pub stream_count: usize,
    pub generated_from: String,
    pub migration_tool: String,
}

impl BeadEventStoreManifestWire {
    pub fn from_streams(streams: &[BeadEventStreamWire]) -> Self {
        Self {
            schema_version: BEAD_EVENT_SCHEMA_VERSION,
            stream_count: streams.len(),
            generated_from: "issues.jsonl".to_string(),
            migration_tool: "sase-core bead events".to_string(),
        }
    }

    pub fn validate(&self) -> Result<(), BeadError> {
        if self.schema_version != BEAD_EVENT_SCHEMA_VERSION {
            return Err(BeadError::validation(format!(
                "unsupported bead event manifest schema_version: {}",
                self.schema_version
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventStreamWire {
    pub stream_id: String,
    pub root_issue_id: String,
    #[serde(default)]
    pub events: Vec<BeadEventRecordWire>,
}

impl BeadEventStreamWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.stream_id.is_empty() {
            return Err(BeadError::validation("bead event stream_id is empty"));
        }
        if self.root_issue_id.is_empty() {
            return Err(BeadError::validation(
                "bead event root_issue_id is empty",
            ));
        }
        if self.stream_id != self.root_issue_id {
            return Err(BeadError::validation(format!(
                "bead event stream_id must match root_issue_id: {} != {}",
                self.stream_id, self.root_issue_id
            )));
        }
        for event in &self.events {
            event.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeadEventRecordWire {
    pub schema_version: u32,
    pub event_id: String,
    pub timestamp: String,
    pub actor: String,
    pub operation: BeadEventOperationWire,
    pub issue_id: String,
    pub payload: BeadEventPayloadWire,
}

impl BeadEventRecordWire {
    pub fn validate(&self) -> Result<(), BeadError> {
        if self.schema_version != BEAD_EVENT_SCHEMA_VERSION {
            return Err(BeadError::validation(format!(
                "unsupported bead event schema_version: {}",
                self.schema_version
            )));
        }
        if self.event_id.is_empty() {
            return Err(BeadError::validation("bead event_id is empty"));
        }
        if self.issue_id.is_empty() {
            return Err(BeadError::validation("bead event issue_id is empty"));
        }
        if self.timestamp.is_empty() {
            return Err(BeadError::validation("bead event timestamp is empty"));
        }
        self.payload.validate_for(self.operation, &self.issue_id)
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum BeadEventOperationWire {
    IssueCreated,
    IssueUpdated,
    NoteAppended,
    NoteEdited,
    NoteRemoved,
    IssueOpened,
    IssueClosed,
    IssueRemoved,
    DependencyAdded,
    DependencyRemoved,
    ReferenceAdded,
    ReferenceRemoved,
    LinkAdded,
    LinkRemoved,
    ReadyMarked,
    ReadyUnmarked,
    EpicWorkPreclaimed,
    TaskPlusOneRecorded,
    TaskSnoozed,
    TaskSnoozeCanceled,
    TaskSnoozeWoken,
}

/// Which wake condition ended a snooze.
///
/// The wake *time* never changes a bead's status on its own — it raises a
/// gate the human answers — so every event recorded here comes from a
/// condition the store itself observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeadSnoozeWakeCauseWire {
    PlusOne,
}

impl BeadSnoozeWakeCauseWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PlusOne => "plus_one",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
// Keep the public wire shape direct and serde-compatible with existing event logs.
#[allow(clippy::large_enum_variant)]
pub enum BeadEventPayloadWire {
    IssueCreated {
        issue: IssueWire,
    },
    IssueUpdated {
        fields: BeadIssueUpdateEventFieldsWire,
    },
    NoteAppended {
        entry: String,
    },
    NoteEdited {
        note_id: String,
        text: String,
    },
    NoteRemoved {
        note_id: String,
    },
    IssueOpened,
    IssueClosed {
        close_reason: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resolution: Option<BeadResolutionWire>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        forced_descendant_ids: Vec<String>,
    },
    IssueRemoved {
        #[serde(default)]
        cascade_removed_issue_ids: Vec<String>,
    },
    DependencyAdded {
        dependency: DependencyWire,
    },
    DependencyRemoved {
        dependency: DependencyWire,
    },
    ReferenceAdded {
        reference: String,
    },
    ReferenceRemoved {
        reference: String,
    },
    LinkAdded {
        target_ref: String,
        relation: String,
        description: String,
        origin: crate::artifact_link::ArtifactLinkOriginWire,
        #[serde(default)]
        direction: crate::artifact_link::BeadLinkDirectionWire,
        #[serde(default = "default_bead_link_uses")]
        uses: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
    },
    LinkRemoved {
        target_ref: String,
        relation: String,
        #[serde(default)]
        direction: crate::artifact_link::BeadLinkDirectionWire,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        operation_id: Option<String>,
    },
    ReadyMarked,
    ReadyUnmarked,
    EpicWorkPreclaimed {
        agent_name: String,
    },
    TaskPlusOneRecorded {
        evidence: TaskPlusOneEvidenceWire,
    },
    TaskSnoozed {
        snooze: BeadSnoozeWire,
    },
    TaskSnoozeCanceled,
    TaskSnoozeWoken {
        cause: BeadSnoozeWakeCauseWire,
    },
}

impl BeadEventPayloadWire {
    fn validate_for(
        &self,
        operation: BeadEventOperationWire,
        issue_id: &str,
    ) -> Result<(), BeadError> {
        match (operation, self) {
            (
                BeadEventOperationWire::IssueCreated,
                BeadEventPayloadWire::IssueCreated { issue },
            ) => {
                if issue.id != issue_id {
                    return Err(BeadError::validation(format!(
                        "issue_created payload id mismatch: {} != {}",
                        issue.id, issue_id
                    )));
                }
                issue.validate()
            }
            (
                BeadEventOperationWire::IssueUpdated,
                BeadEventPayloadWire::IssueUpdated { fields },
            ) => fields.validate(),
            (
                BeadEventOperationWire::NoteAppended,
                BeadEventPayloadWire::NoteAppended { entry },
            ) => {
                if entry.trim().is_empty() {
                    return Err(BeadError::validation(
                        "note_appended entry cannot be empty or blank",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::NoteEdited,
                BeadEventPayloadWire::NoteEdited { note_id, text },
            ) => {
                if note_id.trim().is_empty() {
                    return Err(BeadError::validation(
                        "note_edited note_id cannot be empty or blank",
                    ));
                }
                if text.trim().is_empty() {
                    return Err(BeadError::validation(
                        "note_edited text cannot be empty or blank",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::NoteRemoved,
                BeadEventPayloadWire::NoteRemoved { note_id },
            ) => {
                if note_id.trim().is_empty() {
                    return Err(BeadError::validation(
                        "note_removed note_id cannot be empty or blank",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::IssueOpened,
                BeadEventPayloadWire::IssueOpened,
            )
            | (
                BeadEventOperationWire::IssueClosed,
                BeadEventPayloadWire::IssueClosed { .. },
            )
            | (
                BeadEventOperationWire::IssueRemoved,
                BeadEventPayloadWire::IssueRemoved { .. },
            )
            | (
                BeadEventOperationWire::ReadyMarked,
                BeadEventPayloadWire::ReadyMarked,
            )
            | (
                BeadEventOperationWire::ReadyUnmarked,
                BeadEventPayloadWire::ReadyUnmarked,
            )
            | (
                BeadEventOperationWire::TaskSnoozeCanceled,
                BeadEventPayloadWire::TaskSnoozeCanceled,
            )
            | (
                BeadEventOperationWire::TaskSnoozeWoken,
                BeadEventPayloadWire::TaskSnoozeWoken { .. },
            ) => Ok(()),
            (
                BeadEventOperationWire::TaskSnoozed,
                BeadEventPayloadWire::TaskSnoozed { snooze },
            ) => snooze.validate(),
            (
                BeadEventOperationWire::DependencyAdded,
                BeadEventPayloadWire::DependencyAdded { dependency },
            ) => {
                if dependency.issue_id != issue_id {
                    return Err(BeadError::validation(format!(
                        "dependency_added payload issue_id mismatch: {} != {}",
                        dependency.issue_id, issue_id
                    )));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::DependencyRemoved,
                BeadEventPayloadWire::DependencyRemoved { dependency },
            ) => {
                if dependency.issue_id != issue_id {
                    return Err(BeadError::validation(format!(
                        "dependency_removed payload issue_id mismatch: {} != {}",
                        dependency.issue_id, issue_id
                    )));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::ReferenceAdded,
                BeadEventPayloadWire::ReferenceAdded { .. },
            )
            | (
                BeadEventOperationWire::ReferenceRemoved,
                BeadEventPayloadWire::ReferenceRemoved { .. },
            ) => Ok(()),
            (
                BeadEventOperationWire::LinkAdded,
                BeadEventPayloadWire::LinkAdded {
                    target_ref,
                    relation,
                    description,
                    ..
                },
            ) => validate_link_added_payload(target_ref, relation, description),
            (
                BeadEventOperationWire::LinkRemoved,
                BeadEventPayloadWire::LinkRemoved {
                    target_ref,
                    relation,
                    ..
                },
            ) => validate_link_removed_payload(target_ref, relation),
            (
                BeadEventOperationWire::EpicWorkPreclaimed,
                BeadEventPayloadWire::EpicWorkPreclaimed { agent_name },
            ) => {
                if agent_name.is_empty() {
                    return Err(BeadError::validation(
                        "epic_work_preclaimed agent_name is empty",
                    ));
                }
                Ok(())
            }
            (
                BeadEventOperationWire::TaskPlusOneRecorded,
                BeadEventPayloadWire::TaskPlusOneRecorded { evidence },
            ) => evidence.validate(),
            _ => Err(BeadError::validation(format!(
                "event operation/payload mismatch for {issue_id}"
            ))),
        }
    }
}

fn validate_link_added_payload(
    target_ref: &str,
    relation: &str,
    description: &str,
) -> Result<(), BeadError> {
    canonicalize_artifact_link_ref(target_ref).map_err(link_error)?;
    lookup_artifact_relation(relation).map_err(link_error)?;
    validate_artifact_link_description(description).map_err(link_error)?;
    Ok(())
}

fn validate_link_removed_payload(
    target_ref: &str,
    relation: &str,
) -> Result<(), BeadError> {
    canonicalize_artifact_link_ref(target_ref).map_err(link_error)?;
    lookup_artifact_relation(relation).map_err(link_error)?;
    Ok(())
}

pub(super) fn link_error(
    error: crate::artifact_link::ArtifactLinkError,
) -> BeadError {
    BeadError::validation(error.to_string())
}

pub(super) fn canonical_bead_source_ref(issue_id: &str) -> String {
    format!("bead:{issue_id}")
}

fn default_bead_link_uses() -> u64 {
    1
}

pub(super) fn remap_link_target_ref(
    target_ref: &str,
    old_id: &str,
    new_id: &str,
) -> String {
    let Ok(canonical) = canonicalize_artifact_link_ref(target_ref) else {
        return target_ref.to_string();
    };
    let Some(bead_id) = canonical.strip_prefix("bead:") else {
        return canonical;
    };
    format!("bead:{}", remapped_id(bead_id, old_id, new_id))
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct BeadIssueUpdateEventFieldsWire {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub status: Option<StatusWire>,
    #[serde(default)]
    pub assignee: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub design: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub size: Option<PhaseSizeWire>,
    #[serde(default)]
    pub closed_at: Option<Option<String>>,
    #[serde(default)]
    pub close_reason: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "deserialize_present_option",
        skip_serializing_if = "Option::is_none"
    )]
    pub resolution: Option<Option<BeadResolutionWire>>,
    #[serde(default)]
    pub changespec_name: Option<String>,
    #[serde(default)]
    pub changespec_bug_id: Option<String>,
    #[serde(default)]
    pub external_ref: Option<String>,
    #[serde(default)]
    pub tier: Option<BeadTierWire>,
    #[serde(default)]
    pub is_ready_to_work: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_type_fields: Option<BTreeMap<String, String>>,
}

impl BeadIssueUpdateEventFieldsWire {
    fn validate(&self) -> Result<(), BeadError> {
        if self.status.is_none()
            && self.title.is_none()
            && self.assignee.is_none()
            && self.description.is_none()
            && self.notes.is_none()
            && self.design.is_none()
            && self.model.is_none()
            && self.size.is_none()
            && self.closed_at.is_none()
            && self.close_reason.is_none()
            && self.resolution.is_none()
            && self.changespec_name.is_none()
            && self.changespec_bug_id.is_none()
            && self.external_ref.is_none()
            && self.tier.is_none()
            && self.is_ready_to_work.is_none()
            && self.task_type_fields.is_none()
        {
            return Err(BeadError::validation(
                "issue_updated event has no fields",
            ));
        }
        Ok(())
    }
}

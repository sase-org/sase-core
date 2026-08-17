//! Deterministic committed catalog snapshot for task-type specs.

use serde::{Deserialize, Serialize};

use super::error::TaskTypeError;
use super::spec::{
    task_type_spec_digest, validate_task_type_spec, TaskTypeFieldSpecWire,
    TaskTypeSpecWire, TaskTypeTriageWire, TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION,
};

/// Provenance of one catalog member. Deliberately has no package version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskTypeSourceWire {
    Builtin,
    Plugin,
    Project,
}

impl TaskTypeSourceWire {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Plugin => "plugin",
            Self::Project => "project",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeSnapshotWire {
    pub types: Vec<TaskTypeSnapshotEntryWire>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskTypeSnapshotEntryWire {
    pub task_type: String,
    pub label: String,
    pub summary: String,
    pub when_to_use: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub glyph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accent_color: Option<String>,
    pub agent_creatable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_size: Option<String>,
    pub fields: Vec<TaskTypeFieldSpecWire>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_template: Option<String>,
    pub triage: TaskTypeTriageWire,
    pub source: TaskTypeSourceWire,
    pub package: String,
    pub digest: String,
}

impl TaskTypeSnapshotEntryWire {
    pub fn from_spec(
        spec: &TaskTypeSpecWire,
        source: TaskTypeSourceWire,
        package: impl Into<String>,
    ) -> Result<Self, TaskTypeError> {
        validate_task_type_spec(spec)?;
        Ok(Self {
            task_type: spec.task_type.clone(),
            label: spec.label.clone(),
            summary: spec.summary.clone(),
            when_to_use: spec.when_to_use.clone(),
            glyph: spec.glyph.clone(),
            accent_color: spec.accent_color.clone(),
            agent_creatable: spec.agent_creatable,
            default_size: spec.default_size.clone(),
            fields: spec.fields.clone(),
            body_template: spec.body_template.clone(),
            triage: spec.triage.clone(),
            source,
            package: package.into(),
            digest: task_type_spec_digest(spec)?,
        })
    }

    pub fn to_spec(&self) -> TaskTypeSpecWire {
        TaskTypeSpecWire {
            schema_version: TASK_TYPE_SPEC_WIRE_SCHEMA_VERSION,
            task_type: self.task_type.clone(),
            label: self.label.clone(),
            summary: self.summary.clone(),
            when_to_use: self.when_to_use.clone(),
            glyph: self.glyph.clone(),
            accent_color: self.accent_color.clone(),
            agent_creatable: self.agent_creatable,
            default_size: self.default_size.clone(),
            fields: self.fields.clone(),
            body_template: self.body_template.clone(),
            triage: self.triage.clone(),
        }
    }
}

/// Parse a committed `task_types.json` document.
pub fn parse_task_type_snapshot(
    data: &str,
) -> Result<TaskTypeSnapshotWire, TaskTypeError> {
    let mut snapshot: TaskTypeSnapshotWire = serde_json::from_str(data)
        .map_err(|error| {
            TaskTypeError::validation(format!(
                "task type snapshot is not valid JSON: {error}"
            ))
        })?;
    normalize_and_validate_snapshot(&mut snapshot)?;
    Ok(snapshot)
}

/// Serialize a snapshot deterministically: types sorted by slug, keys in
/// struct order, no package-version field, trailing newline.
pub fn serialize_task_type_snapshot(
    snapshot: &TaskTypeSnapshotWire,
) -> Result<String, TaskTypeError> {
    let mut snapshot = snapshot.clone();
    normalize_and_validate_snapshot(&mut snapshot)?;
    let mut encoded =
        serde_json::to_string_pretty(&snapshot).map_err(|error| {
            TaskTypeError::validation(format!(
                "unable to serialize task type snapshot: {error}"
            ))
        })?;
    if !encoded.ends_with('\n') {
        encoded.push('\n');
    }
    Ok(encoded)
}

fn normalize_and_validate_snapshot(
    snapshot: &mut TaskTypeSnapshotWire,
) -> Result<(), TaskTypeError> {
    snapshot
        .types
        .sort_by(|left, right| left.task_type.cmp(&right.task_type));
    let mut seen = std::collections::BTreeSet::new();
    for entry in &snapshot.types {
        if !seen.insert(entry.task_type.as_str()) {
            return Err(TaskTypeError::validation(format!(
                "task type snapshot has duplicate slug '{}'",
                entry.task_type
            )));
        }
        if entry.package.trim().is_empty() {
            return Err(TaskTypeError::validation(format!(
                "task type '{}' snapshot package must not be empty",
                entry.task_type
            )));
        }
        if !is_sha256_hex(&entry.digest) {
            return Err(TaskTypeError::validation(format!(
                "task type '{}' snapshot digest must be 64 lowercase hex characters",
                entry.task_type
            )));
        }
        let spec = entry.to_spec();
        validate_task_type_spec(&spec)?;
        let digest = task_type_spec_digest(&spec)?;
        if digest != entry.digest {
            return Err(TaskTypeError::validation(format!(
                "task type '{}' snapshot digest does not match the spec",
                entry.task_type
            )));
        }
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::super::spec::valid_spec;
    use super::*;

    fn snapshot_with(spec: &TaskTypeSpecWire) -> TaskTypeSnapshotWire {
        TaskTypeSnapshotWire {
            types: vec![TaskTypeSnapshotEntryWire::from_spec(
                spec,
                TaskTypeSourceWire::Builtin,
                "sase",
            )
            .unwrap()],
        }
    }

    #[test]
    fn round_trips_and_sorts_by_slug() {
        let flake = valid_spec();
        let mut bug = valid_spec();
        bug.task_type = "bug".to_string();
        bug.label = "Bug".to_string();
        bug.glyph = Some("⨯".to_string());
        bug.accent_color = Some("#FF5F5F".to_string());
        bug.body_template = None;
        bug.fields.clear();

        let snapshot = TaskTypeSnapshotWire {
            types: vec![
                TaskTypeSnapshotEntryWire::from_spec(
                    &flake,
                    TaskTypeSourceWire::Builtin,
                    "sase",
                )
                .unwrap(),
                TaskTypeSnapshotEntryWire::from_spec(
                    &bug,
                    TaskTypeSourceWire::Plugin,
                    "sase-github",
                )
                .unwrap(),
            ],
        };
        let encoded = serialize_task_type_snapshot(&snapshot).unwrap();
        assert!(encoded.ends_with('\n'));
        assert!(!encoded.contains("schema_version"));
        assert!(!encoded.contains("package_version"));
        assert!(
            encoded.find("\"bug\"").unwrap()
                < encoded.find("\"flake\"").unwrap()
        );

        let parsed = parse_task_type_snapshot(&encoded).unwrap();
        assert_eq!(parsed.types[0].task_type, "bug");
        assert_eq!(parsed.types[1].task_type, "flake");
        assert_eq!(parsed.types[0].source, TaskTypeSourceWire::Plugin);
        assert_eq!(serialize_task_type_snapshot(&parsed).unwrap(), encoded);
    }

    #[test]
    fn rejects_digest_mismatch_and_duplicate_slugs() {
        let mut snapshot = snapshot_with(&valid_spec());
        snapshot.types[0].digest = "0".repeat(64);
        assert!(serialize_task_type_snapshot(&snapshot)
            .unwrap_err()
            .message
            .contains("digest does not match"));

        let mut snapshot = snapshot_with(&valid_spec());
        snapshot.types.push(snapshot.types[0].clone());
        assert!(serialize_task_type_snapshot(&snapshot)
            .unwrap_err()
            .message
            .contains("duplicate slug"));
    }

    #[test]
    fn parse_rejects_invalid_json() {
        assert!(parse_task_type_snapshot("{").is_err());
    }
}

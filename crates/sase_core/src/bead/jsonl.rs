//! JSONL import/export for git-portable bead storage.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use std::collections::BTreeSet;

use super::events::{
    BeadEventRecordWire, BeadEventStoreManifestWire, BeadEventStreamWire,
};
use super::wire::{
    deserialize_valid_issue, invalid_record_error, BeadError, BeadTierWire,
    IssueTypeWire, IssueWire,
};

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct JsonlLoadOutcome {
    pub issues: Vec<IssueWire>,
    pub loaded_rows: usize,
    pub blank_lines: usize,
    pub invalid_json_lines: usize,
    pub invalid_record_lines: usize,
}

pub fn parse_issues_jsonl(input: &str) -> JsonlLoadOutcome {
    let mut outcome = JsonlLoadOutcome::default();
    if input.trim().is_empty() {
        return outcome;
    }

    for line in input.lines() {
        let line = line.trim();
        if line.is_empty() {
            outcome.blank_lines += 1;
            continue;
        }
        let value = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(_) => {
                outcome.invalid_json_lines += 1;
                continue;
            }
        };
        match deserialize_valid_issue(value) {
            Ok(issue) => {
                outcome.loaded_rows += 1;
                outcome.issues.push(issue);
            }
            Err(error) => {
                let _ = invalid_record_error(error);
                outcome.invalid_record_lines += 1;
            }
        }
    }

    apply_missing_tiers(&mut outcome.issues);
    outcome.issues.retain(|issue| issue.validate().is_ok());
    outcome
        .issues
        .sort_by(|a, b| issue_import_key(a).cmp(&issue_import_key(b)));
    outcome
}

pub fn import_issues_from_jsonl(
    path: &Path,
) -> Result<JsonlLoadOutcome, BeadError> {
    if !path.exists() {
        return Ok(JsonlLoadOutcome::default());
    }
    let contents = fs::read_to_string(path)?;
    Ok(parse_issues_jsonl(&contents))
}

pub fn export_issues_to_jsonl(
    issues: &[IssueWire],
) -> Result<String, BeadError> {
    let mut sorted = issues.to_vec();
    sorted.sort_by(|a, b| a.id.cmp(&b.id));

    let mut output = String::new();
    for issue in sorted {
        issue.validate()?;
        output.push_str(&serde_json::to_string(&issue)?);
        output.push('\n');
    }
    Ok(output)
}

pub fn write_issues_jsonl(
    beads_dir: &Path,
    issues: &[IssueWire],
) -> Result<(), BeadError> {
    let jsonl = export_issues_to_jsonl(issues)?;
    write_file_atomic(&beads_dir.join("issues.jsonl"), jsonl.as_bytes())
}

pub fn event_store_present(beads_dir: &Path) -> bool {
    event_manifest_path(beads_dir).exists()
        || event_streams_dir(beads_dir).exists()
}

pub fn event_manifest_path(beads_dir: &Path) -> PathBuf {
    beads_dir.join("events").join("manifest.json")
}

pub fn event_streams_dir(beads_dir: &Path) -> PathBuf {
    beads_dir.join("events").join("streams")
}

pub fn read_event_store(
    beads_dir: &Path,
) -> Result<(BeadEventStoreManifestWire, Vec<BeadEventStreamWire>), BeadError> {
    let manifest_path = event_manifest_path(beads_dir);
    let manifest_text = fs::read_to_string(&manifest_path).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead events manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: BeadEventStoreManifestWire =
        serde_json::from_str(&manifest_text)?;
    manifest.validate()?;

    let streams_dir = event_streams_dir(beads_dir);
    let mut stream_paths = Vec::new();
    for entry in fs::read_dir(&streams_dir).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead event streams directory {}: {err}",
            streams_dir.display()
        ))
    })? {
        let path = entry
            .map_err(|err| {
                BeadError::io(format!(
                    "failed to read bead event stream entry: {err}"
                ))
            })?
            .path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            stream_paths.push(path);
        }
    }
    stream_paths.sort();

    if manifest.stream_count != stream_paths.len() {
        return Err(BeadError::validation(format!(
            "bead event manifest stream_count mismatch: {} != {}",
            manifest.stream_count,
            stream_paths.len()
        )));
    }

    let streams = stream_paths
        .into_iter()
        .map(|path| read_event_stream_file(&path))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((manifest, streams))
}

pub fn write_event_store(
    beads_dir: &Path,
    streams: &[BeadEventStreamWire],
) -> Result<(), BeadError> {
    let events_dir = beads_dir.join("events");
    let streams_dir = event_streams_dir(beads_dir);
    fs::create_dir_all(&streams_dir)?;

    let mut streams = streams.to_vec();
    streams.sort_by(|a, b| a.stream_id.cmp(&b.stream_id));
    for stream in &streams {
        stream.validate()?;
        let mut output = String::new();
        for event in &stream.events {
            output.push_str(&serde_json::to_string(event)?);
            output.push('\n');
        }
        write_file_atomic(
            &streams_dir.join(format!("{}.jsonl", stream.stream_id)),
            output.as_bytes(),
        )?;
    }

    let manifest = BeadEventStoreManifestWire::from_streams(&streams);
    let manifest_json = serde_json::to_vec_pretty(&manifest)?;
    write_file_atomic(&events_dir.join("manifest.json"), &manifest_json)?;
    Ok(())
}

fn issue_import_key(issue: &IssueWire) -> (u8, &str) {
    let kind_order = match issue.issue_type {
        IssueTypeWire::Plan => 0,
        IssueTypeWire::Phase => 1,
    };
    (kind_order, issue.id.as_str())
}

fn read_event_stream_file(
    path: &Path,
) -> Result<BeadEventStreamWire, BeadError> {
    let stream_id = path
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            BeadError::validation(format!(
                "invalid bead event stream filename: {}",
                path.display()
            ))
        })?
        .to_string();
    let contents = fs::read_to_string(path).map_err(|err| {
        BeadError::io(format!(
            "failed to read bead event stream {}: {err}",
            path.display()
        ))
    })?;
    let mut events = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: BeadEventRecordWire =
            serde_json::from_str(line).map_err(|err| {
                BeadError::validation(format!(
                    "invalid bead event stream {} line {}: {err}",
                    path.display(),
                    index + 1
                ))
            })?;
        events.push(event);
    }
    let stream = BeadEventStreamWire {
        stream_id: stream_id.clone(),
        root_issue_id: stream_id,
        events,
    };
    stream.validate()?;
    Ok(stream)
}

fn write_file_atomic(path: &Path, bytes: &[u8]) -> Result<(), BeadError> {
    let parent = path.parent().ok_or_else(|| {
        BeadError::io(format!(
            "cannot determine parent directory for {}",
            path.display()
        ))
    })?;
    fs::create_dir_all(parent)?;
    let tmp_path = path.with_extension("tmp");
    {
        let mut file = fs::File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    fs::rename(&tmp_path, path)?;
    if let Ok(dir) = fs::File::open(parent) {
        let _ = dir.sync_all();
    }
    Ok(())
}

pub(crate) fn apply_missing_tiers(issues: &mut [IssueWire]) {
    let phase_parent_ids: BTreeSet<String> = issues
        .iter()
        .filter(|issue| issue.issue_type == IssueTypeWire::Phase)
        .filter_map(|issue| issue.parent_id.clone())
        .collect();
    for issue in issues {
        if issue.issue_type == IssueTypeWire::Plan && issue.tier.is_none() {
            issue.tier = Some(if phase_parent_ids.contains(&issue.id) {
                BeadTierWire::Epic
            } else {
                BeadTierWire::Plan
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bead::wire::{DependencyWire, StatusWire};

    fn plan(id: &str) -> IssueWire {
        IssueWire {
            id: id.to_string(),
            title: "Plan".to_string(),
            status: StatusWire::Open,
            issue_type: IssueTypeWire::Plan,
            tier: Some(BeadTierWire::Epic),
            parent_id: None,
            owner: String::new(),
            assignee: String::new(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            created_by: String::new(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
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
    fn corrupt_lines_are_skipped() {
        let outcome = parse_issues_jsonl(
            r#"not json
{"id":"ok","title":"OK","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":
"#,
        );
        assert_eq!(outcome.issues.len(), 1);
        assert_eq!(outcome.invalid_json_lines, 2);
        assert_eq!(outcome.invalid_record_lines, 0);
    }

    #[test]
    fn export_sorts_by_id_and_uses_compact_json() {
        let mut child = plan("gold-1.1");
        child.issue_type = IssueTypeWire::Phase;
        child.tier = None;
        child.parent_id = Some("gold-1".to_string());
        child.dependencies = vec![DependencyWire {
            issue_id: "gold-1.1".to_string(),
            depends_on_id: "gold-1".to_string(),
            created_at: "2026-01-01T00:02:00Z".to_string(),
            created_by: String::new(),
        }];
        let output = export_issues_to_jsonl(&[child, plan("gold-1")]).unwrap();
        assert!(output.starts_with(r#"{"id":"gold-1","#));
        assert!(output.contains(r#""dependencies":[{"issue_id":"gold-1.1""#));
        assert!(!output.contains(": "));
    }

    #[test]
    fn import_defaults_missing_plan_tiers_from_phase_children() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"solo","title":"Solo","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}
{"id":"epic.1","title":"Phase","status":"open","issue_type":"phase","parent_id":"epic","created_at":"","updated_at":"","dependencies":[]}
"#,
        );

        let solo = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "solo")
            .unwrap();
        let epic = outcome
            .issues
            .iter()
            .find(|issue| issue.id == "epic")
            .unwrap();

        assert_eq!(solo.tier, Some(BeadTierWire::Plan));
        assert_eq!(epic.tier, Some(BeadTierWire::Epic));
    }

    #[test]
    fn import_defaults_missing_epic_count_to_none() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"legend","title":"Legend","status":"open","issue_type":"plan","tier":"legend","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].epic_count, None);
    }

    #[test]
    fn import_defaults_missing_model_to_empty() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].model, "");
    }

    #[test]
    fn import_preserves_model() {
        let outcome = parse_issues_jsonl(
            r##"{"id":"epic","title":"Epic","status":"open","issue_type":"plan","parent_id":null,"created_at":"","updated_at":"","model":"#pro","dependencies":[]}"##,
        );

        assert_eq!(outcome.issues[0].model, "#pro");
    }

    #[test]
    fn import_rejects_model_control_characters() {
        let outcome = parse_issues_jsonl(
            "{\"id\":\"epic\",\"title\":\"Epic\",\"status\":\"open\",\"issue_type\":\"plan\",\"parent_id\":null,\"created_at\":\"\",\"updated_at\":\"\",\"model\":\"bad\\n%tag:x\",\"dependencies\":[]}",
        );

        assert_eq!(outcome.issues.len(), 0);
        assert_eq!(outcome.invalid_record_lines, 1);
    }

    #[test]
    fn import_preserves_legend_epic_count() {
        let outcome = parse_issues_jsonl(
            r#"{"id":"legend","title":"Legend","status":"open","issue_type":"plan","tier":"legend","parent_id":null,"created_at":"","updated_at":"","epic_count":5,"dependencies":[]}"#,
        );

        assert_eq!(outcome.issues[0].epic_count, Some(5));
    }
}

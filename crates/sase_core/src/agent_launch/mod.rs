//! Wire records and deterministic helpers for agent launch.

use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Write;
use std::path::Path;

pub const AGENT_LAUNCH_WIRE_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimWire {
    pub workspace_num: u32,
    pub workflow: String,
    #[serde(default)]
    pub cl_name: Option<String>,
    pub pid: u32,
    #[serde(default)]
    pub artifacts_timestamp: Option<String>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimRequestWire {
    pub project_file: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub pid: u32,
    #[serde(default)]
    pub cl_name: String,
    #[serde(default)]
    pub artifacts_timestamp: String,
    #[serde(default)]
    pub transfer_from_pid: Option<u32>,
    #[serde(default)]
    pub pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimOutcomeWire {
    pub success: bool,
    pub workspace_num: u32,
    pub project_file: String,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceClaimPlanWire {
    pub content: String,
    pub outcome: WorkspaceClaimOutcomeWire,
    pub changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchRequestWire {
    pub schema_version: u32,
    pub cl_name: String,
    pub project_file: String,
    pub workspace_dir: String,
    pub workspace_num: u32,
    pub workflow_name: String,
    pub prompt: String,
    pub timestamp: String,
    #[serde(default)]
    pub update_target: String,
    #[serde(default)]
    pub project_name: String,
    #[serde(default)]
    pub history_sort_key: String,
    #[serde(default)]
    pub is_home_mode: bool,
    #[serde(default)]
    pub vcs_workflow_type: Option<String>,
    #[serde(default)]
    pub vcs_ref: Option<String>,
    #[serde(default)]
    pub deferred_workspace: bool,
    #[serde(default)]
    pub local_xprompts_file: Option<String>,
    #[serde(default)]
    pub extra_env: BTreeMap<String, String>,
    #[serde(default)]
    pub retry_transfer_from_pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentLaunchPreparedWire {
    pub schema_version: u32,
    pub prompt_file: String,
    pub output_path: String,
    pub safe_name: String,
    #[serde(default)]
    pub argv: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env_delta: BTreeMap<String, String>,
    #[serde(default)]
    pub claim_request: Option<WorkspaceClaimRequestWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutSlotWire {
    pub prompt: String,
    pub launch_kind: String,
    pub slot_index: u32,
    #[serde(default)]
    pub timestamp: Option<String>,
    #[serde(default)]
    pub workflow_name: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub repeat_name: Option<String>,
    #[serde(default)]
    pub wait_for_previous: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LaunchFanoutPlanWire {
    pub schema_version: u32,
    pub launch_kind: String,
    #[serde(default)]
    pub slots: Vec<LaunchFanoutSlotWire>,
    #[serde(default)]
    pub requires_sequential_naming_wait: bool,
    #[serde(default)]
    pub fanout_sleep_seconds: f64,
}

#[derive(Debug)]
pub enum AgentLaunchPreparationError {
    SchemaVersion { expected: u32, actual: u32 },
    CreateTempFile(std::io::Error),
    WritePrompt(std::io::Error),
    KeepTempFile(std::io::Error),
    CreateOutputRoot(std::io::Error),
}

impl fmt::Display for AgentLaunchPreparationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SchemaVersion { expected, actual } => write!(
                f,
                "unsupported AgentLaunchRequestWire schema_version {actual}; expected {expected}"
            ),
            Self::CreateTempFile(err) => {
                write!(f, "failed to create prompt temp file: {err}")
            }
            Self::WritePrompt(err) => {
                write!(f, "failed to write prompt temp file: {err}")
            }
            Self::KeepTempFile(err) => {
                write!(f, "failed to keep prompt temp file: {err}")
            }
            Self::CreateOutputRoot(err) => {
                write!(f, "failed to create launch output root: {err}")
            }
        }
    }
}

impl std::error::Error for AgentLaunchPreparationError {}

#[derive(Debug)]
pub enum TimestampBatchAllocationError {
    InvalidTimestamp {
        field: &'static str,
        value: String,
        error: chrono::ParseError,
    },
}

impl fmt::Display for TimestampBatchAllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimestamp {
                field,
                value,
                error,
            } => write!(
                f,
                "invalid {field} launch timestamp {value:?}; expected YYmmdd_HHMMSS: {error}"
            ),
        }
    }
}

impl std::error::Error for TimestampBatchAllocationError {}

pub fn allocate_launch_timestamp_batch(
    count: usize,
    base_timestamp: &str,
    after_timestamp: Option<&str>,
) -> Result<Vec<String>, TimestampBatchAllocationError> {
    if count == 0 {
        return Ok(Vec::new());
    }

    let base = parse_launch_timestamp("base_timestamp", base_timestamp)?;
    let start = match after_timestamp {
        Some(after) if !after.is_empty() => {
            let after = parse_launch_timestamp("after_timestamp", after)?;
            std::cmp::max(base, after + Duration::seconds(1))
        }
        _ => base,
    };

    Ok((0..count)
        .map(|offset| {
            (start + Duration::seconds(offset as i64))
                .format("%y%m%d_%H%M%S")
                .to_string()
        })
        .collect())
}

fn parse_launch_timestamp(
    field: &'static str,
    value: &str,
) -> Result<NaiveDateTime, TimestampBatchAllocationError> {
    NaiveDateTime::parse_from_str(value, "%y%m%d_%H%M%S").map_err(|error| {
        TimestampBatchAllocationError::InvalidTimestamp {
            field,
            value: value.to_string(),
            error,
        }
    })
}

pub fn prepare_agent_launch(
    request: &AgentLaunchRequestWire,
    python_executable: &str,
    runner_script: &str,
    sase_tmpdir: Option<&str>,
    output_root: &str,
    preallocated_env: &BTreeMap<String, String>,
) -> Result<AgentLaunchPreparedWire, AgentLaunchPreparationError> {
    if request.schema_version != AGENT_LAUNCH_WIRE_SCHEMA_VERSION {
        return Err(AgentLaunchPreparationError::SchemaVersion {
            expected: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            actual: request.schema_version,
        });
    }

    let prompt_file =
        write_prompt_temp_file(sase_tmpdir, request.prompt.as_bytes())?;
    let safe_name = safe_launch_name(&request.cl_name);
    let output_root_path = Path::new(output_root);
    std::fs::create_dir_all(output_root_path)
        .map_err(AgentLaunchPreparationError::CreateOutputRoot)?;
    let output_path = output_root_path
        .join(format!("{safe_name}_ace-run-{}.txt", request.timestamp))
        .to_string_lossy()
        .into_owned();

    let mut env_delta = request.extra_env.clone();
    env_delta.insert("SASE_AGENT".to_string(), "1".to_string());
    env_delta.insert("SASE_AGENT_CL_NAME".to_string(), request.cl_name.clone());
    env_delta.insert(
        "SASE_AGENT_PROJECT_FILE".to_string(),
        request.project_file.clone(),
    );
    env_delta.insert(
        "SASE_AGENT_TIMESTAMP".to_string(),
        request.timestamp.clone(),
    );

    if request.deferred_workspace {
        env_delta.insert(
            "SASE_AGENT_DEFERRED_WORKSPACE".to_string(),
            "1".to_string(),
        );
        if let Some(workflow_type) = request.vcs_workflow_type.as_ref() {
            env_delta.insert(
                "SASE_AGENT_VCS_WORKFLOW_TYPE".to_string(),
                workflow_type.clone(),
            );
        }
    }

    for (key, value) in preallocated_env {
        env_delta.insert(key.clone(), value.clone());
    }

    if let Some(local_xprompts_file) = request.local_xprompts_file.as_ref() {
        env_delta.insert(
            "SASE_AGENT_LOCAL_XPROMPTS".to_string(),
            local_xprompts_file.clone(),
        );
    }

    let prompt_file_str = prompt_file.to_string_lossy().into_owned();
    let argv = vec![
        python_executable.to_string(),
        runner_script.to_string(),
        request.cl_name.clone(),
        request.project_file.clone(),
        request.workspace_dir.clone(),
        output_path.clone(),
        request.workspace_num.to_string(),
        request.workflow_name.clone(),
        prompt_file_str.clone(),
        request.timestamp.clone(),
        request.update_target.clone(),
        request.project_name.clone(),
        request.history_sort_key.clone(),
        if request.is_home_mode {
            "1".to_string()
        } else {
            String::new()
        },
    ];

    let claim_request = if request.is_home_mode {
        None
    } else {
        Some(WorkspaceClaimRequestWire {
            project_file: request.project_file.clone(),
            workspace_num: if request.deferred_workspace {
                0
            } else {
                request.workspace_num
            },
            workflow_name: request.workflow_name.clone(),
            pid: 0,
            cl_name: request.cl_name.clone(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: request.retry_transfer_from_pid,
            pinned: false,
        })
    };

    Ok(AgentLaunchPreparedWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        prompt_file: prompt_file_str,
        output_path,
        safe_name,
        argv,
        cwd: request.workspace_dir.clone(),
        env_delta,
        claim_request,
    })
}

pub fn safe_launch_name(cl_name: &str) -> String {
    cl_name
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn write_prompt_temp_file(
    sase_tmpdir: Option<&str>,
    prompt: &[u8],
) -> Result<std::path::PathBuf, AgentLaunchPreparationError> {
    let mut builder = tempfile::Builder::new();
    builder.prefix("sase_ace_prompt_").suffix(".md");
    let mut file = match sase_tmpdir {
        Some(dir) if !dir.is_empty() => builder
            .tempfile_in(dir)
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
        _ => builder
            .tempfile()
            .map_err(AgentLaunchPreparationError::CreateTempFile)?,
    };
    file.write_all(prompt)
        .map_err(AgentLaunchPreparationError::WritePrompt)?;
    let (_file, path) = file
        .keep()
        .map_err(|err| AgentLaunchPreparationError::KeepTempFile(err.error))?;
    Ok(path)
}

pub fn list_workspace_claims_from_content(
    content: &str,
) -> Vec<WorkspaceClaimWire> {
    let mut claims = Vec::new();
    let mut in_running_field = false;

    for line in content.split('\n') {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if !in_running_field {
            continue;
        }
        if !is_running_continuation_line(line) {
            break;
        }
        if let Some(claim) = WorkspaceClaimLine::parse(line) {
            claims.push(claim.into_wire());
        }
    }

    claims
}

pub fn plan_claim_workspace_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let (_running_idx, running_end_idx) = find_running_field_bounds(&lines);

    if request.workspace_num != 0 {
        for line in running_claim_lines(&lines) {
            if let Some(existing) = WorkspaceClaimLine::parse(line) {
                if existing.workspace_num == request.workspace_num {
                    return claim_plan(
                        content.to_string(),
                        false,
                        request,
                        Some(format!(
                            "workspace #{} is already claimed",
                            request.workspace_num
                        )),
                        false,
                    );
                }
            }
        }
    }

    let new_claim = WorkspaceClaimLine::from_request(request);
    if let Some(end) = running_end_idx {
        lines.insert(end + 1, new_claim.to_line());
    } else {
        lines.insert(0, String::new());
        lines.insert(0, new_claim.to_line());
        lines.insert(0, "RUNNING:".to_string());
    }

    claim_plan(
        normalize_running_field_spacing(&lines.join("\n")),
        true,
        request,
        None,
        true,
    )
}

pub fn plan_transfer_workspace_claim_from_content(
    content: &str,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let Some(from_pid) = request.transfer_from_pid else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some("transfer_from_pid is required".to_string()),
            false,
        );
    };

    let mut lines: Vec<String> =
        content.split('\n').map(ToString::to_string).collect();
    let mut in_running_field = false;

    for line in &mut lines {
        if line.starts_with("RUNNING:") {
            in_running_field = true;
            continue;
        }
        if in_running_field && is_running_continuation_line(line) {
            if let Some(claim) = WorkspaceClaimLine::parse(line) {
                let cl_matches = request.cl_name.is_empty()
                    || claim.cl_name.as_deref()
                        == Some(request.cl_name.as_str());
                if claim.workspace_num == request.workspace_num
                    && claim.pid == from_pid
                    && cl_matches
                {
                    let replacement = WorkspaceClaimLine {
                        workspace_num: claim.workspace_num,
                        pid: request.pid,
                        workflow: request.workflow_name.clone(),
                        cl_name: claim.cl_name,
                        artifacts_timestamp: if request
                            .artifacts_timestamp
                            .is_empty()
                        {
                            claim.artifacts_timestamp
                        } else {
                            Some(request.artifacts_timestamp.clone())
                        },
                        pinned: claim.pinned,
                    };
                    *line = replacement.to_line();
                    return claim_plan(
                        lines.join("\n"),
                        true,
                        request,
                        None,
                        true,
                    );
                }
            }
        } else {
            in_running_field = false;
        }
    }

    claim_plan(
        content.to_string(),
        false,
        request,
        Some(format!(
            "workspace #{} with pid {from_pid} was not found",
            request.workspace_num
        )),
        false,
    )
}

pub fn allocate_and_claim_workspace_from_content(
    content: &str,
    min_workspace: u32,
    max_workspace: u32,
    request: &WorkspaceClaimRequestWire,
) -> WorkspaceClaimPlanWire {
    let claimed: BTreeSet<u32> = list_workspace_claims_from_content(content)
        .into_iter()
        .map(|claim| claim.workspace_num)
        .collect();
    let Some(workspace_num) =
        (min_workspace..=max_workspace).find(|n| !claimed.contains(n))
    else {
        return claim_plan(
            content.to_string(),
            false,
            request,
            Some(format!(
                "all workspaces ({min_workspace}-{max_workspace}) are claimed"
            )),
            false,
        );
    };

    let mut allocated_request = request.clone();
    allocated_request.workspace_num = workspace_num;
    plan_claim_workspace_from_content(content, &allocated_request)
}

fn claim_plan(
    content: String,
    success: bool,
    request: &WorkspaceClaimRequestWire,
    error: Option<String>,
    changed: bool,
) -> WorkspaceClaimPlanWire {
    WorkspaceClaimPlanWire {
        content,
        outcome: WorkspaceClaimOutcomeWire {
            success,
            workspace_num: request.workspace_num,
            project_file: request.project_file.clone(),
            pid: Some(request.pid),
            error,
        },
        changed,
    }
}

fn running_claim_lines(lines: &[String]) -> impl Iterator<Item = &str> {
    let (start, end) = find_running_field_bounds(lines);
    let start = start.unwrap_or(0);
    let end = end.unwrap_or(0);
    lines
        .iter()
        .enumerate()
        .filter(move |(idx, _)| *idx > start && *idx <= end)
        .map(|(_, line)| line.as_str())
}

fn find_running_field_bounds(
    lines: &[String],
) -> (Option<usize>, Option<usize>) {
    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("RUNNING:") {
            let mut running_end_idx = i;
            for (j, candidate) in lines.iter().enumerate().skip(i + 1) {
                if is_running_continuation_line(candidate) {
                    running_end_idx = j;
                } else {
                    break;
                }
            }
            return (Some(i), Some(running_end_idx));
        }
    }
    (None, None)
}

fn is_running_continuation_line(line: &str) -> bool {
    line.starts_with("  ")
        && (line.trim().starts_with('#') || line.trim().starts_with('|'))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspaceClaimLine {
    workspace_num: u32,
    pid: u32,
    workflow: String,
    cl_name: Option<String>,
    artifacts_timestamp: Option<String>,
    pinned: bool,
}

impl WorkspaceClaimLine {
    fn parse(line: &str) -> Option<Self> {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') {
            return None;
        }
        let parts: Vec<&str> = trimmed.split('|').map(str::trim).collect();
        if parts.len() < 4 {
            return None;
        }

        let workspace_num = parts[0].strip_prefix('#')?.parse::<u32>().ok()?;
        let pid = parts[1].parse::<u32>().ok()?;
        let workflow = parts[2];
        if workflow.is_empty() {
            return None;
        }

        let mut artifacts_timestamp = None;
        let mut pinned = false;
        for part in parts.iter().skip(4) {
            if *part == "PINNED" {
                pinned = true;
            } else if is_timestamp_part(part) {
                artifacts_timestamp = Some((*part).to_string());
            } else {
                return None;
            }
        }

        Some(Self {
            workspace_num,
            pid,
            workflow: workflow.to_string(),
            cl_name: if parts[3].is_empty() {
                None
            } else {
                Some(parts[3].to_string())
            },
            artifacts_timestamp,
            pinned,
        })
    }

    fn from_request(request: &WorkspaceClaimRequestWire) -> Self {
        Self {
            workspace_num: request.workspace_num,
            pid: request.pid,
            workflow: request.workflow_name.clone(),
            cl_name: if request.cl_name.is_empty() {
                None
            } else {
                Some(request.cl_name.clone())
            },
            artifacts_timestamp: if request.artifacts_timestamp.is_empty() {
                None
            } else {
                Some(request.artifacts_timestamp.clone())
            },
            pinned: request.pinned,
        }
    }

    fn into_wire(self) -> WorkspaceClaimWire {
        WorkspaceClaimWire {
            workspace_num: self.workspace_num,
            workflow: self.workflow,
            cl_name: self.cl_name,
            pid: self.pid,
            artifacts_timestamp: self.artifacts_timestamp,
            pinned: self.pinned,
        }
    }

    fn to_line(&self) -> String {
        let cl_part = self.cl_name.as_deref().unwrap_or("");
        let ts_part = self
            .artifacts_timestamp
            .as_ref()
            .map(|ts| format!(" | {ts}"))
            .unwrap_or_default();
        let pin_part = if self.pinned { " | PINNED" } else { "" };
        format!(
            "  #{} | {} | {} | {}{}{}",
            self.workspace_num,
            self.pid,
            self.workflow,
            cl_part,
            ts_part,
            pin_part
        )
    }
}

fn is_timestamp_part(value: &str) -> bool {
    (value.len() == 14 && value.as_bytes().iter().all(u8::is_ascii_digit))
        || (value.len() == 13
            && value.as_bytes()[0..6].iter().all(u8::is_ascii_digit)
            && value.as_bytes()[6] == b'_'
            && value.as_bytes()[7..13].iter().all(u8::is_ascii_digit))
}

fn normalize_running_field_spacing(content: &str) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let mut result_lines = Vec::with_capacity(lines.len());
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i];
        if line.starts_with("RUNNING:") {
            result_lines.push(line.to_string());
            i += 1;
            while i < lines.len() {
                let entry_line = lines[i];
                if entry_line.starts_with("  ")
                    && entry_line.trim().starts_with('#')
                {
                    result_lines.push(entry_line.to_string());
                    i += 1;
                } else {
                    break;
                }
            }
            while i < lines.len() && lines[i].trim().is_empty() {
                i += 1;
            }
            if i < lines.len() {
                result_lines.push(String::new());
                result_lines.push(String::new());
            }
        } else {
            result_lines.push(line.to_string());
            i += 1;
        }
    }

    result_lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn request(workspace_num: u32) -> WorkspaceClaimRequestWire {
        WorkspaceClaimRequestWire {
            project_file: "/tmp/project.gp".to_string(),
            workspace_num,
            workflow_name: "run".to_string(),
            pid: 222,
            cl_name: "demo".to_string(),
            artifacts_timestamp: String::new(),
            transfer_from_pid: None,
            pinned: false,
        }
    }

    #[test]
    fn launch_request_round_trips_json_shape() {
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.gp".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 2,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: Some("/tmp/xp.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(10),
        };

        let value = serde_json::to_value(&request).unwrap();
        assert_eq!(value["schema_version"], json!(1));
        assert_eq!(value["extra_env"]["SASE_REPEAT_NAME"], json!("task.1"));
        let back: AgentLaunchRequestWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, request);
    }

    #[test]
    fn prepared_wire_preserves_null_claim_request() {
        let prepared = AgentLaunchPreparedWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            prompt_file: "/tmp/prompt.md".to_string(),
            output_path: "/tmp/out.txt".to_string(),
            safe_name: "home".to_string(),
            argv: vec!["python".to_string()],
            cwd: "/home/user".to_string(),
            env_delta: BTreeMap::new(),
            claim_request: None,
        };
        let value = serde_json::to_value(&prepared).unwrap();
        assert_eq!(value["claim_request"], json!(null));
        let back: AgentLaunchPreparedWire =
            serde_json::from_value(value).unwrap();
        assert_eq!(back, prepared);
    }

    #[test]
    fn fanout_plan_round_trips_slots() {
        let plan = LaunchFanoutPlanWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            launch_kind: "repeat".to_string(),
            slots: vec![LaunchFanoutSlotWire {
                prompt: "%n:task.1\nfix it".to_string(),
                launch_kind: "repeat".to_string(),
                slot_index: 0,
                timestamp: None,
                workflow_name: None,
                model: None,
                repeat_name: Some("task.1".to_string()),
                wait_for_previous: false,
            }],
            requires_sequential_naming_wait: false,
            fanout_sleep_seconds: 1.0,
        };
        let value = serde_json::to_value(&plan).unwrap();
        assert_eq!(value["slots"][0]["repeat_name"], json!("task.1"));
        let back: LaunchFanoutPlanWire = serde_json::from_value(value).unwrap();
        assert_eq!(back, plan);
    }

    #[test]
    fn timestamp_batch_allocates_unique_visible_timestamps() {
        let timestamps =
            allocate_launch_timestamp_batch(3, "260501_120000", None).unwrap();

        assert_eq!(
            timestamps,
            vec!["260501_120000", "260501_120001", "260501_120002"]
        );
    }

    #[test]
    fn timestamp_batch_starts_after_previous_allocation() {
        let timestamps = allocate_launch_timestamp_batch(
            2,
            "260501_120000",
            Some("260501_120005"),
        )
        .unwrap();

        assert_eq!(timestamps, vec!["260501_120006", "260501_120007"]);
    }

    #[test]
    fn timestamp_batch_rejects_invalid_format() {
        let err = allocate_launch_timestamp_batch(1, "not-a-timestamp", None)
            .unwrap_err();

        assert!(err.to_string().contains("expected YYmmdd_HHMMSS"));
    }

    #[test]
    fn prepare_agent_launch_writes_prompt_and_shapes_process_data() {
        let tmp = tempfile::tempdir().unwrap();
        let prompt_dir = tmp.path().join("prompts");
        std::fs::create_dir(&prompt_dir).unwrap();
        let output_root = tmp.path().join("workflows").join("202605");
        let mut extra_env = BTreeMap::new();
        extra_env.insert("SASE_AGENT".to_string(), "caller".to_string());
        extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
        let request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "feature/test".to_string(),
            project_file: "/tmp/project.gp".to_string(),
            workspace_dir: "/tmp/ws".to_string(),
            workspace_num: 4,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: "p4head".to_string(),
            project_name: "proj".to_string(),
            history_sort_key: "feature/test".to_string(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: false,
            local_xprompts_file: Some("/tmp/xprompts.json".to_string()),
            extra_env,
            retry_transfer_from_pid: Some(99),
        };
        let mut preallocated = BTreeMap::new();
        preallocated.insert("GH_PRE_ALLOCATED".to_string(), "1".to_string());
        preallocated.insert("GH_WORKSPACE_NUM".to_string(), "4".to_string());

        let prepared = prepare_agent_launch(
            &request,
            "/venv/bin/python",
            "/repo/run_agent_runner.py",
            Some(prompt_dir.to_str().unwrap()),
            output_root.to_str().unwrap(),
            &preallocated,
        )
        .unwrap();

        assert_eq!(prepared.safe_name, "feature_test");
        assert_eq!(
            std::fs::read_to_string(&prepared.prompt_file).unwrap(),
            "fix it"
        );
        assert!(prepared
            .prompt_file
            .starts_with(prompt_dir.to_str().unwrap()));
        assert_eq!(
            prepared.output_path,
            output_root
                .join("feature_test_ace-run-260501_120000.txt")
                .to_string_lossy()
        );
        assert_eq!(prepared.argv[0], "/venv/bin/python");
        assert_eq!(prepared.argv[2], "feature/test");
        assert_eq!(prepared.argv[5], prepared.output_path);
        assert_eq!(prepared.argv[8], prepared.prompt_file);
        assert_eq!(prepared.env_delta["SASE_AGENT"], "1");
        assert_eq!(prepared.env_delta["SASE_REPEAT_NAME"], "task.1");
        assert_eq!(prepared.env_delta["GH_PRE_ALLOCATED"], "1");
        assert_eq!(
            prepared.env_delta["SASE_AGENT_LOCAL_XPROMPTS"],
            "/tmp/xprompts.json"
        );
        assert!(!prepared
            .env_delta
            .contains_key("SASE_AGENT_VCS_WORKFLOW_TYPE"));
        assert_eq!(prepared.claim_request.unwrap().transfer_from_pid, Some(99));
    }

    #[test]
    fn prepare_agent_launch_deferred_and_home_claim_shapes() {
        let tmp = tempfile::tempdir().unwrap();
        let mut request = AgentLaunchRequestWire {
            schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
            cl_name: "home".to_string(),
            project_file: "/tmp/home.gp".to_string(),
            workspace_dir: "/home/me".to_string(),
            workspace_num: 9,
            workflow_name: "ace(run)-260501_120000".to_string(),
            prompt: "fix it".to_string(),
            timestamp: "260501_120000".to_string(),
            update_target: String::new(),
            project_name: String::new(),
            history_sort_key: String::new(),
            is_home_mode: false,
            vcs_workflow_type: Some("gh".to_string()),
            vcs_ref: Some("feature/test".to_string()),
            deferred_workspace: true,
            local_xprompts_file: None,
            extra_env: BTreeMap::new(),
            retry_transfer_from_pid: None,
        };

        let deferred = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert_eq!(deferred.claim_request.unwrap().workspace_num, 0);
        assert_eq!(deferred.env_delta["SASE_AGENT_DEFERRED_WORKSPACE"], "1");
        assert_eq!(deferred.env_delta["SASE_AGENT_VCS_WORKFLOW_TYPE"], "gh");

        request.is_home_mode = true;
        let home = prepare_agent_launch(
            &request,
            "python",
            "runner.py",
            None,
            tmp.path().to_str().unwrap(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(home.claim_request.is_none());
        assert_eq!(home.argv[13], "1");
    }

    #[test]
    fn workspace_claims_parse_valid_rows_and_ignore_malformed() {
        let content = "RUNNING:\n  #0 | 111 | wait | deferred | 20260501120000 | PINNED\n  #bad | nope\n  #2 | 222 | run | demo\n\n\nNAME: demo\n";

        let claims = list_workspace_claims_from_content(content);

        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].workspace_num, 0);
        assert_eq!(
            claims[0].artifacts_timestamp.as_deref(),
            Some("20260501120000")
        );
        assert!(claims[0].pinned);
        assert_eq!(claims[1].workspace_num, 2);
    }

    #[test]
    fn claim_workspace_rejects_duplicate_nonzero_but_allows_zero() {
        let content = "RUNNING:\n  #2 | 111 | run | demo\n\n\nNAME: demo\n";

        let duplicate = plan_claim_workspace_from_content(content, &request(2));
        assert!(!duplicate.outcome.success);
        assert!(!duplicate.changed);

        let zero = plan_claim_workspace_from_content(content, &request(0));
        assert!(zero.outcome.success);
        assert!(zero.content.contains("#0 | 222 | run | demo"));
    }

    #[test]
    fn allocate_and_claim_picks_first_available_workspace() {
        let content = "RUNNING:\n  #100 | 111 | run | a\n  #102 | 333 | run | c\n\n\nNAME: demo\n";
        let mut req = request(0);
        req.cl_name = "b".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.pinned = true;

        let plan =
            allocate_and_claim_workspace_from_content(content, 100, 102, &req);

        assert!(plan.outcome.success);
        assert_eq!(plan.outcome.workspace_num, 101);
        assert!(plan
            .content
            .contains("#101 | 222 | run | b | 20260501120000 | PINNED"));
    }

    #[test]
    fn transfer_workspace_claim_matches_pid_and_preserves_claim_name() {
        let content = "RUNNING:\n  #101 | 111 | run | demo | 20260501115959\n\n\nNAME: demo\n";
        let mut req = request(101);
        req.workflow_name = "run-retry".to_string();
        req.artifacts_timestamp = "20260501120000".to_string();
        req.transfer_from_pid = Some(111);

        let plan = plan_transfer_workspace_claim_from_content(content, &req);

        assert!(plan.outcome.success);
        assert!(plan
            .content
            .contains("#101 | 222 | run-retry | demo | 20260501120000"));
    }
}

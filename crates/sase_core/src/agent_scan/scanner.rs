//! Pure-Rust filesystem snapshot scanner for agent artifact trees.
//!
//! Mirrors `sase.core.agent_scan_facade.scan_agent_artifacts_python`. The
//! scanner walks `projects_root/<project>/artifacts/<workflow>/<timestamp>/`
//! and parses a small fixed set of marker JSON files into the wire shape
//! defined in [`super::wire`].
//!
//! Soft errors (unreadable directories, malformed marker JSON, marker JSON
//! whose top level is not an object) are absorbed silently and counted on
//! [`AgentArtifactScanStatsWire`]. A single bad artifact never breaks a
//! scan.
//!
//! Records are sorted deterministically by
//! `(project_name, workflow_dir_name, timestamp)` before returning so a
//! Python parity test can compare snapshots without extra agreement.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use super::wire::{
    is_supported_workflow_dir, AgentArtifactRecordWire,
    AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
    AgentArtifactScanWire, AgentMetaWire, DoneMarkerWire, PlanPathMarkerWire,
    PromptStepMarkerWire, RunningMarkerWire, WaitingMarkerWire,
    WorkflowStateWire, WorkflowStepStateWire, AGENT_SCAN_WIRE_SCHEMA_VERSION,
};

const RAW_PROMPT_FILE: &str = "raw_xprompt.md";

/// Scan `projects_root` and return a deterministic snapshot of every
/// artifact directory's parsed marker files.
///
/// Returns an empty snapshot (with zero stats) if `projects_root` does
/// not exist. Soft errors do not propagate; check
/// [`AgentArtifactScanWire::stats`] for diagnostics.
pub fn scan_agent_artifacts(
    projects_root: &Path,
    options: AgentArtifactScanOptionsWire,
) -> AgentArtifactScanWire {
    let mut stats = AgentArtifactScanStatsWire::default();
    let mut records: Vec<AgentArtifactRecordWire> = Vec::new();

    let only: Option<Vec<String>> = if options.only_workflow_dirs.is_empty() {
        None
    } else {
        Some(options.only_workflow_dirs.clone())
    };

    if projects_root.exists() {
        let project_dirs = sorted_dir_entries(projects_root, &mut stats);
        for project_dir in project_dirs {
            if !project_dir.is_dir() {
                continue;
            }
            stats.projects_visited += 1;

            let artifacts_base = project_dir.join("artifacts");
            if !artifacts_base.exists() {
                continue;
            }

            let workflow_dirs = sorted_dir_entries(&artifacts_base, &mut stats);
            for workflow_dir in workflow_dirs {
                if !workflow_dir.is_dir() {
                    continue;
                }
                let workflow_dir_name = match workflow_dir.file_name() {
                    Some(n) => n.to_string_lossy().into_owned(),
                    None => continue,
                };
                if let Some(only) = &only {
                    if !only.iter().any(|s| s == &workflow_dir_name) {
                        continue;
                    }
                } else if !is_supported_workflow_dir(&workflow_dir_name) {
                    continue;
                }

                let artifact_dirs =
                    sorted_dir_entries(&workflow_dir, &mut stats);
                for artifact_dir in artifact_dirs {
                    if !artifact_dir.is_dir() {
                        continue;
                    }
                    stats.artifact_dirs_visited += 1;
                    let project_name = project_dir
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let record = scan_artifact_dir(
                        &project_name,
                        &project_dir,
                        &workflow_dir_name,
                        &artifact_dir,
                        &options,
                        &mut stats,
                    );
                    records.push(record);
                }
            }
        }
    }

    records.sort_by(|a, b| {
        (
            a.project_name.as_str(),
            a.workflow_dir_name.as_str(),
            a.timestamp.as_str(),
        )
            .cmp(&(
                b.project_name.as_str(),
                b.workflow_dir_name.as_str(),
                b.timestamp.as_str(),
            ))
    });

    AgentArtifactScanWire {
        schema_version: AGENT_SCAN_WIRE_SCHEMA_VERSION,
        projects_root: projects_root.to_string_lossy().into_owned(),
        options,
        stats,
        records,
    }
}

/// List `path` entries sorted by file name. Soft-errors increment the
/// `os_errors` counter and yield an empty list.
fn sorted_dir_entries(
    path: &Path,
    stats: &mut AgentArtifactScanStatsWire,
) -> Vec<PathBuf> {
    let read_dir = match fs::read_dir(path) {
        Ok(rd) => rd,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                stats.os_errors += 1;
            }
            return Vec::new();
        }
    };
    let mut entries: Vec<PathBuf> = Vec::new();
    for entry in read_dir {
        match entry {
            Ok(e) => entries.push(e.path()),
            Err(_) => stats.os_errors += 1,
        }
    }
    entries.sort_by(|a, b| {
        a.file_name()
            .unwrap_or_default()
            .cmp(b.file_name().unwrap_or_default())
    });
    entries
}

fn scan_artifact_dir(
    project_name: &str,
    project_dir: &Path,
    workflow_dir_name: &str,
    artifact_dir: &Path,
    options: &AgentArtifactScanOptionsWire,
    stats: &mut AgentArtifactScanStatsWire,
) -> AgentArtifactRecordWire {
    let project_file = project_dir.join(format!("{project_name}.gp"));

    let agent_meta =
        load_marker_object(&artifact_dir.join("agent_meta.json"), stats)
            .map(|m| agent_meta_from_object(&m));

    let done_path = artifact_dir.join("done.json");
    let has_done_marker = done_path.exists();
    let done = if has_done_marker {
        match load_marker_object(&done_path, stats) {
            Some(m) => Some(done_marker_from_object(&m)),
            // Mirror Python: "done.json exists but unreadable" still counts
            // as a completed-marker presence; expose a default DoneMarkerWire
            // so `record.done.is_some()` agrees with `has_done_marker`.
            None => Some(DoneMarkerWire::default()),
        }
    } else {
        None
    };

    let running = load_marker_object(&artifact_dir.join("running.json"), stats)
        .map(|m| running_marker_from_object(&m));

    let waiting = load_marker_object(&artifact_dir.join("waiting.json"), stats)
        .map(|m| waiting_marker_from_object(&m));

    let workflow_state =
        load_marker_object(&artifact_dir.join("workflow_state.json"), stats)
            .map(|m| workflow_state_from_object(&m));

    let plan_path =
        load_marker_object(&artifact_dir.join("plan_path.json"), stats)
            .map(|m| plan_path_from_object(&m));

    let mut prompt_steps: Vec<PromptStepMarkerWire> = Vec::new();
    if options.include_prompt_step_markers {
        let mut step_files: Vec<PathBuf> =
            sorted_dir_entries(artifact_dir, stats)
                .into_iter()
                .filter(|p| {
                    if !p.is_file() {
                        return false;
                    }
                    let name =
                        p.file_name().and_then(|n| n.to_str()).unwrap_or("");
                    name.starts_with("prompt_step_") && name.ends_with(".json")
                })
                .collect();
        step_files.sort_by(|a, b| {
            a.file_name()
                .unwrap_or_default()
                .cmp(b.file_name().unwrap_or_default())
        });
        for step_file in step_files {
            if let Some(obj) = load_marker_object(&step_file, stats) {
                stats.prompt_step_markers_parsed += 1;
                let file_name = step_file
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default();
                prompt_steps.push(prompt_step_from_object(file_name, &obj));
            }
        }
    }

    let raw_prompt_snippet = if options.include_raw_prompt_snippets {
        read_raw_prompt_snippet(
            artifact_dir,
            options.max_prompt_snippet_bytes as usize,
            stats,
        )
    } else {
        None
    };

    AgentArtifactRecordWire {
        project_name: project_name.to_string(),
        project_dir: project_dir.to_string_lossy().into_owned(),
        project_file: project_file.to_string_lossy().into_owned(),
        workflow_dir_name: workflow_dir_name.to_string(),
        artifact_dir: artifact_dir.to_string_lossy().into_owned(),
        timestamp: artifact_dir
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
        agent_meta,
        done,
        running,
        waiting,
        workflow_state,
        plan_path,
        prompt_steps,
        raw_prompt_snippet,
        has_done_marker,
    }
}

/// Read `path`, parse as JSON, and return only when the top-level value
/// is a JSON object. All soft-error variants (missing, unreadable,
/// malformed, non-object) are reported by side effect on `stats`.
fn load_marker_object(
    path: &Path,
    stats: &mut AgentArtifactScanStatsWire,
) -> Option<Map<String, Value>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                stats.os_errors += 1;
            }
            return None;
        }
    };
    let value: Value = match serde_json::from_slice(&bytes) {
        Ok(v) => v,
        Err(_) => {
            stats.json_decode_errors += 1;
            return None;
        }
    };
    match value {
        Value::Object(map) => {
            stats.marker_files_parsed += 1;
            Some(map)
        }
        _ => {
            stats.json_decode_errors += 1;
            None
        }
    }
}

fn read_raw_prompt_snippet(
    artifact_dir: &Path,
    max_bytes: usize,
    stats: &mut AgentArtifactScanStatsWire,
) -> Option<String> {
    let raw_path = artifact_dir.join(RAW_PROMPT_FILE);
    let mut file = match fs::File::open(&raw_path) {
        Ok(f) => f,
        Err(err) => {
            if err.kind() != io::ErrorKind::NotFound {
                stats.os_errors += 1;
            }
            return None;
        }
    };
    let mut buf = String::new();
    if let Err(_err) = file.read_to_string(&mut buf) {
        stats.os_errors += 1;
        return None;
    }
    let truncated: String = buf
        .chars()
        .scan(0usize, |bytes_so_far, c| {
            let next = *bytes_so_far + c.len_utf8();
            if next > max_bytes {
                None
            } else {
                *bytes_so_far = next;
                Some(c)
            }
        })
        .collect();
    Some(truncated.trim().to_string())
}

// ---------------------------------------------------------------------------
// Coercion helpers
// ---------------------------------------------------------------------------

fn coerce_str(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn coerce_int(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Bool(_)) => None,
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else {
                n.as_f64().map(|f| f as i64)
            }
        }
        Some(Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn coerce_float(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Bool(_)) => None,
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn coerce_str_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::String(s)) => vec![s.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| match v {
                Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn coerce_object(value: Option<&Value>) -> Option<Map<String, Value>> {
    match value {
        Some(Value::Object(m)) => Some(m.clone()),
        _ => None,
    }
}

fn coerce_str_str_map(
    value: Option<&Value>,
) -> Option<BTreeMap<String, String>> {
    let map = match value {
        Some(Value::Object(m)) => m,
        _ => return None,
    };
    let mut out: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in map.iter() {
        if let Value::String(s) = v {
            out.insert(k.clone(), s.clone());
        }
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

/// Python `bool(x)` semantics: anything truthy → true.
fn coerce_bool_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(u) = n.as_u64() {
                u != 0
            } else {
                n.as_f64().map(|f| f != 0.0).unwrap_or(false)
            }
        }
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
    }
}

/// Strict-int variant for `retry_attempt` (the Python facade only accepts
/// real `int` values here, mirroring the source dataclass field type).
fn coerce_strict_int(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Bool(_)) => None,
        Some(Value::Number(n)) if n.is_i64() => n.as_i64(),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Marker -> wire converters
// ---------------------------------------------------------------------------

fn agent_meta_from_object(data: &Map<String, Value>) -> AgentMetaWire {
    AgentMetaWire {
        name: coerce_str(data.get("name")),
        workflow_name: coerce_str(data.get("workflow_name")),
        pid: coerce_int(data.get("pid")),
        model: coerce_str(data.get("model")),
        llm_provider: coerce_str(data.get("llm_provider")),
        vcs_provider: coerce_str(data.get("vcs_provider")),
        role_suffix: coerce_str(data.get("role_suffix")),
        parent_timestamp: coerce_str(data.get("parent_timestamp")),
        workspace_num: coerce_int(data.get("workspace_num")),
        approve: coerce_bool_truthy(data.get("approve")),
        hidden: coerce_bool_truthy(data.get("hidden")),
        plan: coerce_bool_truthy(data.get("plan")),
        plan_approved: coerce_bool_truthy(data.get("plan_approved")),
        plan_action: coerce_str(data.get("plan_action")),
        wait_for: coerce_str_list(data.get("wait_for")),
        wait_duration: coerce_float(data.get("wait_duration")),
        wait_until: coerce_str(data.get("wait_until")),
        plan_submitted_at: coerce_str_list(data.get("plan_submitted_at")),
        feedback_submitted_at: coerce_str_list(
            data.get("feedback_submitted_at"),
        ),
        questions_submitted_at: coerce_str_list(
            data.get("questions_submitted_at"),
        ),
        retry_started_at: coerce_str_list(data.get("retry_started_at")),
        run_started_at: coerce_str(data.get("run_started_at")),
        stopped_at: coerce_str(data.get("stopped_at")),
        retry_of_timestamp: coerce_str(data.get("retry_of_timestamp")),
        retry_attempt: coerce_strict_int(data.get("retry_attempt")),
        retry_chain_root_timestamp: coerce_str(
            data.get("retry_chain_root_timestamp"),
        ),
        retried_as_timestamp: coerce_str(data.get("retried_as_timestamp")),
        retry_terminal: coerce_bool_truthy(data.get("retry_terminal")),
        retry_error_category: coerce_str(data.get("retry_error_category")),
    }
}

fn done_marker_from_object(data: &Map<String, Value>) -> DoneMarkerWire {
    DoneMarkerWire {
        outcome: coerce_str(data.get("outcome")),
        finished_at: coerce_float(data.get("finished_at")),
        cl_name: coerce_str(data.get("cl_name")),
        project_file: coerce_str(data.get("project_file")),
        workspace_num: coerce_int(data.get("workspace_num")),
        pid: coerce_int(data.get("pid")),
        model: coerce_str(data.get("model")),
        llm_provider: coerce_str(data.get("llm_provider")),
        vcs_provider: coerce_str(data.get("vcs_provider")),
        name: coerce_str(data.get("name")),
        plan_path: coerce_str(data.get("plan_path")),
        diff_path: coerce_str(data.get("diff_path")),
        markdown_pdf_paths: coerce_str_list(data.get("markdown_pdf_paths")),
        response_path: coerce_str(data.get("response_path")),
        output_path: coerce_str(data.get("output_path")),
        step_output: coerce_object(data.get("step_output")),
        error: coerce_str(data.get("error")),
        traceback: coerce_str(data.get("traceback")),
        retried_as_timestamp: coerce_str(data.get("retried_as_timestamp")),
        retry_chain_root_timestamp: coerce_str(
            data.get("retry_chain_root_timestamp"),
        ),
        retry_error_category: coerce_str(data.get("retry_error_category")),
        approve: coerce_bool_truthy(data.get("approve")),
        hidden: coerce_bool_truthy(data.get("hidden")),
    }
}

fn running_marker_from_object(data: &Map<String, Value>) -> RunningMarkerWire {
    RunningMarkerWire {
        pid: coerce_int(data.get("pid")),
        cl_name: coerce_str(data.get("cl_name")),
        model: coerce_str(data.get("model")),
        llm_provider: coerce_str(data.get("llm_provider")),
        vcs_provider: coerce_str(data.get("vcs_provider")),
    }
}

fn waiting_marker_from_object(data: &Map<String, Value>) -> WaitingMarkerWire {
    WaitingMarkerWire {
        waiting_for: coerce_str_list(data.get("waiting_for")),
        wait_duration: coerce_float(data.get("wait_duration")),
        wait_until: coerce_str(data.get("wait_until")),
    }
}

fn workflow_step_from_object(
    data: &Map<String, Value>,
) -> WorkflowStepStateWire {
    WorkflowStepStateWire {
        name: coerce_str(data.get("name")).unwrap_or_default(),
        status: coerce_str(data.get("status"))
            .unwrap_or_else(|| "pending".to_string()),
        output: coerce_object(data.get("output")),
        output_types: coerce_str_str_map(data.get("output_types")),
        error: coerce_str(data.get("error")),
        traceback: coerce_str(data.get("traceback")),
    }
}

fn workflow_state_from_object(data: &Map<String, Value>) -> WorkflowStateWire {
    let mut steps: Vec<WorkflowStepStateWire> = Vec::new();
    if let Some(Value::Array(raw_steps)) = data.get("steps") {
        for step in raw_steps {
            if let Value::Object(map) = step {
                steps.push(workflow_step_from_object(map));
            }
        }
    }
    let cl_name = match data.get("context") {
        Some(Value::Object(ctx)) => coerce_str(ctx.get("cl_name")),
        _ => None,
    };
    WorkflowStateWire {
        workflow_name: coerce_str(data.get("workflow_name"))
            .unwrap_or_else(|| "unknown".to_string()),
        cl_name,
        status: coerce_str(data.get("status"))
            .unwrap_or_else(|| "running".to_string()),
        pid: coerce_int(data.get("pid")),
        appears_as_agent: coerce_bool_truthy(data.get("appears_as_agent")),
        is_anonymous: coerce_bool_truthy(data.get("is_anonymous")),
        current_step_index: coerce_int(data.get("current_step_index"))
            .unwrap_or(0),
        start_time: coerce_str(data.get("start_time")),
        error: coerce_str(data.get("error")),
        traceback: coerce_str(data.get("traceback")),
        steps,
    }
}

fn prompt_step_from_object(
    file_name: String,
    data: &Map<String, Value>,
) -> PromptStepMarkerWire {
    PromptStepMarkerWire {
        file_name,
        workflow_name: coerce_str(data.get("workflow_name"))
            .unwrap_or_else(|| "unknown".to_string()),
        step_name: coerce_str(data.get("step_name"))
            .unwrap_or_else(|| "unknown".to_string()),
        step_type: coerce_str(data.get("step_type"))
            .unwrap_or_else(|| "agent".to_string()),
        step_source: coerce_str(data.get("step_source")),
        step_index: coerce_int(data.get("step_index")),
        total_steps: coerce_int(data.get("total_steps")),
        parent_step_index: coerce_int(data.get("parent_step_index")),
        parent_total_steps: coerce_int(data.get("parent_total_steps")),
        status: coerce_str(data.get("status"))
            .unwrap_or_else(|| "completed".to_string()),
        hidden: coerce_bool_truthy(data.get("hidden")),
        is_pre_prompt_step: coerce_bool_truthy(data.get("is_pre_prompt_step")),
        embedded_workflow_name: coerce_str(data.get("embedded_workflow_name")),
        artifacts_dir: coerce_str(data.get("artifacts_dir")),
        diff_path: coerce_str(data.get("diff_path")),
        response_path: coerce_str(data.get("response_path")),
        error: coerce_str(data.get("error")),
        traceback: coerce_str(data.get("traceback")),
        model: coerce_str(data.get("model")),
        llm_provider: coerce_str(data.get("llm_provider")),
        output: coerce_object(data.get("output")),
        output_types: coerce_str_str_map(data.get("output_types")),
    }
}

fn plan_path_from_object(data: &Map<String, Value>) -> PlanPathMarkerWire {
    PlanPathMarkerWire {
        plan_path: coerce_str(data.get("plan_path")),
    }
}

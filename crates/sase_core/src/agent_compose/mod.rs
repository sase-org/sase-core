//! Pure Rust deterministic agent-list composition.

use std::collections::{HashMap, HashSet};

use chrono::NaiveDateTime;
use serde_json::{Map, Value};

use crate::agent_scan::{
    AgentArtifactRecordWire, AgentArtifactScanWire, PromptStepMarkerWire,
    WorkflowStepStateWire, DONE_WORKFLOW_DIR_NAMES, DONE_WORKFLOW_DIR_PREFIXES,
    WORKFLOW_STATE_DIR_NAMES, WORKFLOW_STATE_DIR_PREFIXES,
};
use crate::wire::ChangeSpecWire;

pub mod wire;

pub use wire::{
    AgentComposeInputWire, AgentComposeOptionsWire, AgentIdentityWire,
    AgentWire, ComposedAgentListWire, DropReasonWire, MergeReasonWire,
    RunningClaimWire, AGENT_COMPOSE_WIRE_SCHEMA_VERSION,
};

const COMPLETED_STATUSES: &[&str] = &["DONE", "FAILED"];
const ACTIVE_WORKFLOW_STATUSES: &[&str] = &[
    "RUNNING",
    "WAITING INPUT",
    "PLANNING",
    "PLAN APPROVED",
    "QUESTION",
];
const AXE_WORKFLOW_PREFIXES: &[&str] =
    &["axe(mentor)", "axe(fix_hook)", "axe(crs)", "mentor("];
const DONE_AXE_WORKFLOWS: &[&str] = &["fix_hook", "crs", "summarize_hook"];
const DONE_AXE_PREFIXES: &[&str] = &["mentor_"];
const PLAIN_AXE_WORKFLOWS: &[&str] =
    &["fix_hook", "crs", "mentor", "summarize_hook"];
const PLAIN_AXE_PREFIXES: &[&str] = &["mentor_"];
const VCS_WORKFLOW_PREFIXES: &[&str] = &["hg-", "gh-", "git-"];

pub fn compose_agent_list(
    input: &AgentComposeInputWire,
) -> ComposedAgentListWire {
    if input.schema_version != AGENT_COMPOSE_WIRE_SCHEMA_VERSION {
        return ComposedAgentListWire {
            schema_version: AGENT_COMPOSE_WIRE_SCHEMA_VERSION,
            agents: Vec::new(),
            workflow_agent_steps: Vec::new(),
            dismissed_from_loader: Vec::new(),
            dropped: vec![DropReasonWire {
                stage: "schema".to_string(),
                identity: AgentIdentityWire(String::new(), String::new(), None),
                reason: "unsupported_schema_version".to_string(),
                detail: Some(input.schema_version.to_string()),
            }],
            merge_log: Vec::new(),
        };
    }

    let mut diagnostics = Diagnostics::default();
    let mut agents = Vec::new();

    let (bug_by_cl, cl_by_cl) = build_changespec_lookups(&input.changespecs);
    agents.extend(build_running_claim_agents(input, &bug_by_cl, &cl_by_cl));

    let mut workflow_agent_steps = Vec::new();
    if let Some(snapshot) = &input.artifact_scan {
        agents.extend(build_done_agents(snapshot, &bug_by_cl, &cl_by_cl));
        agents.extend(build_home_running_agents(
            snapshot,
            input,
            &mut diagnostics,
        ));
        let (steps, step_meta_by_parent) = build_workflow_agent_steps(snapshot);
        workflow_agent_steps = steps;
        agents.extend(build_workflow_agents(
            snapshot,
            &step_meta_by_parent,
            input,
        ));
    }
    agents.extend(build_changespec_agents(
        &input.changespecs,
        &bug_by_cl,
        &cl_by_cl,
    ));

    agents = filter_dead_pids(agents, input, &mut diagnostics);
    agents = dedup_axe_spawned_agents(agents, &mut diagnostics);
    agents = remove_vcs_workspace_claims(agents, &mut diagnostics);
    agents = dedup_workflow_entries(agents, &mut diagnostics);
    agents = dedup_running_vs_workflow(agents, &mut diagnostics);
    agents = dedup_by_pid(agents, &mut diagnostics);

    apply_status_overrides(&mut agents);

    let mut dismissed_from_loader = Vec::new();
    agents = split_dismissed(agents, input, &mut dismissed_from_loader);

    let sorted = sort_and_reorder(agents, &mut workflow_agent_steps);

    ComposedAgentListWire {
        schema_version: AGENT_COMPOSE_WIRE_SCHEMA_VERSION,
        agents: sorted,
        workflow_agent_steps: if input.options.include_workflow_steps {
            workflow_agent_steps
        } else {
            Vec::new()
        },
        dismissed_from_loader,
        dropped: if input.options.include_diagnostics {
            diagnostics.dropped
        } else {
            Vec::new()
        },
        merge_log: if input.options.include_diagnostics {
            diagnostics.merge_log
        } else {
            Vec::new()
        },
    }
}

#[derive(Default)]
struct Diagnostics {
    dropped: Vec<DropReasonWire>,
    merge_log: Vec<MergeReasonWire>,
}

fn build_changespec_lookups(
    changespecs: &[ChangeSpecWire],
) -> (
    HashMap<String, Option<String>>,
    HashMap<String, Option<String>>,
) {
    let mut bug_by_cl = HashMap::new();
    let mut cl_by_cl = HashMap::new();
    for cs in changespecs {
        if let Some(bug) = &cs.bug {
            let stripped = bug.strip_prefix("http://b/").unwrap_or(bug);
            bug_by_cl
                .insert(cs.name.clone(), Some(format!("http://b/{stripped}")));
        }
        if cs.cl_or_pr.is_some() {
            cl_by_cl.insert(cs.name.clone(), cs.cl_or_pr.clone());
        }
    }
    (bug_by_cl, cl_by_cl)
}

fn build_running_claim_agents(
    input: &AgentComposeInputWire,
    bug_by_cl: &HashMap<String, Option<String>>,
    cl_by_cl: &HashMap<String, Option<String>>,
) -> Vec<AgentWire> {
    let mut agents = Vec::new();
    for claim in &input.running_claims {
        let Some(raw_workflow) = &claim.workflow else {
            continue;
        };
        if raw_workflow.starts_with("axe(hooks)") {
            continue;
        }
        let (agent_type, workflow) = if raw_workflow.starts_with("workflow(")
            && raw_workflow.ends_with(')')
        {
            (
                "workflow".to_string(),
                Some(raw_workflow[9..raw_workflow.len() - 1].to_string()),
            )
        } else {
            ("run".to_string(), claim.workflow.clone())
        };
        let normalized_ts = normalize_to_14_digit(claim.raw_suffix.as_deref())
            .or_else(|| claim.raw_suffix.clone());
        let start_time = normalized_ts
            .as_deref()
            .and_then(parse_timestamp_14_digit)
            .or_else(|| {
                extract_timestamp_from_workflow(claim.workflow.as_deref())
                    .as_deref()
                    .and_then(parse_timestamp_13_char)
            });
        let mut agent = agent_required(
            &agent_type,
            if claim.cl_name.is_empty() {
                "unknown"
            } else {
                claim.cl_name.as_str()
            },
            &claim.project_file,
            "RUNNING",
        );
        agent.start_time = start_time;
        agent.workspace_num = claim.workspace_num;
        agent.workspace_dir = claim.workspace_dir.clone();
        agent.workflow = workflow;
        agent.pid = claim.pid;
        agent.raw_suffix = normalized_ts;
        agent.model = claim.model.clone();
        agent.llm_provider = claim.llm_provider.clone();
        agent.vcs_provider = claim.vcs_provider.clone();
        agent.agent_name = claim.agent_name.clone();
        agent.approve = claim.approve;
        agent.hidden = claim.hidden;
        agent.bug = claim
            .bug
            .clone()
            .or_else(|| bug_by_cl.get(&agent.cl_name).cloned().flatten());
        agent.cl_num = claim
            .cl_num
            .clone()
            .or_else(|| cl_by_cl.get(&agent.cl_name).cloned().flatten());
        if raw_workflow
            .replace('-', "_")
            .starts_with_any(AXE_WORKFLOW_PREFIXES)
        {
            agent.hidden = true;
        }
        agents.push(agent);
    }
    agents
}

fn build_done_agents(
    snapshot: &AgentArtifactScanWire,
    bug_by_cl: &HashMap<String, Option<String>>,
    cl_by_cl: &HashMap<String, Option<String>>,
) -> Vec<AgentWire> {
    let mut agents = Vec::new();
    for record in &snapshot.records {
        if !is_done_record(record) || !record.has_done_marker {
            continue;
        }
        let Some(done) = &record.done else {
            continue;
        };
        let outcome = done.outcome.as_deref().unwrap_or("completed");
        if outcome == "noop" {
            continue;
        }
        let (status, error_message, error_traceback) = match outcome {
            "failed" if done.retried_as_timestamp.is_some() => (
                "FAILED (RETRIED)",
                done.error.clone(),
                done.traceback.clone(),
            ),
            "failed" => ("FAILED", done.error.clone(), done.traceback.clone()),
            "plan_rejected" => ("PLAN REJECTED", None, None),
            _ => ("DONE", None, None),
        };
        let cl_name = done.cl_name.as_deref().unwrap_or("unknown");
        let mut agent = agent_required(
            "run",
            cl_name,
            done.project_file.as_deref().unwrap_or(""),
            status,
        );
        agent.start_time = parse_timestamp_14_digit(&record.timestamp);
        agent.workflow = Some(record.workflow_dir_name.clone());
        agent.raw_suffix = Some(record.timestamp.clone());
        agent.response_path = done.response_path.clone();
        agent.diff_path = done.diff_path.clone();
        agent.extra_files = done_extra_files(
            done.plan_path.as_deref(),
            &done.markdown_pdf_paths,
            &Vec::new(),
        );
        agent.step_output = done.step_output.clone();
        agent.workspace_num = done.workspace_num;
        agent.workspace_dir = done.workspace_dir.clone();
        agent.bug = bug_by_cl.get(cl_name).cloned().flatten();
        agent.cl_num = cl_by_cl.get(cl_name).cloned().flatten();
        agent.error_message = error_message;
        agent.error_traceback = error_traceback;
        agent.output_path = done.output_path.clone();
        agent.model = done.model.clone();
        agent.llm_provider = done.llm_provider.clone();
        agent.vcs_provider = done.vcs_provider.clone();
        agent.agent_name = done.name.clone();
        agent.hidden = done.hidden;
        agent.approve = done.approve;
        agent.retried_as_timestamp = done.retried_as_timestamp.clone();
        agent.retry_chain_root_timestamp =
            done.retry_chain_root_timestamp.clone();
        agent.retry_error_category = done.retry_error_category.clone();
        enrich_from_meta(&mut agent, &record.agent_meta, &record.waiting);
        enrich_from_prompt_markers(&mut agent, &record.prompt_steps);
        agents.push(agent);
    }
    agents
}

fn build_home_running_agents(
    snapshot: &AgentArtifactScanWire,
    input: &AgentComposeInputWire,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let mut agents = Vec::new();
    for record in &snapshot.records {
        if record.project_name != "home"
            || record.workflow_dir_name != "ace-run"
        {
            continue;
        }
        let Some(running) = &record.running else {
            continue;
        };
        let Some(pid) = running.pid else {
            continue;
        };
        if !is_pid_alive(pid, input) {
            diagnostics.dropped.push(DropReasonWire {
                stage: "home_running_liveness".to_string(),
                identity: AgentIdentityWire(
                    "run".to_string(),
                    running.cl_name.clone().unwrap_or_else(|| "~".to_string()),
                    Some(record.timestamp.clone()),
                ),
                reason: "dead_pid".to_string(),
                detail: Some(pid.to_string()),
            });
            continue;
        }
        let mut agent = agent_required(
            "run",
            running.cl_name.as_deref().unwrap_or("~"),
            &format!("{}/home.gp", parent_project_dir(&record.project_file)),
            "RUNNING",
        );
        agent.start_time = parse_timestamp_14_digit(&record.timestamp);
        agent.workflow = Some("ace(run)".to_string());
        agent.pid = Some(pid);
        agent.raw_suffix = Some(record.timestamp.clone());
        agent.model = running.model.clone();
        agent.llm_provider = running.llm_provider.clone();
        agent.vcs_provider = running.vcs_provider.clone();
        agent.workspace_dir = running.workspace_dir.clone();
        enrich_from_meta(&mut agent, &record.agent_meta, &record.waiting);
        enrich_from_prompt_markers(&mut agent, &record.prompt_steps);
        agents.push(agent);
    }
    agents
}

fn build_workflow_agent_steps(
    snapshot: &AgentArtifactScanWire,
) -> (Vec<AgentWire>, HashMap<String, Map<String, Value>>) {
    let mut agents = Vec::new();
    let mut meta_by_parent = HashMap::new();
    for record in &snapshot.records {
        if !is_workflow_state_record(record) || record.prompt_steps.is_empty() {
            continue;
        }
        let parent_state = record.workflow_state.as_ref();
        let parent_failed = parent_state
            .map(|s| s.status.as_str() == "failed")
            .unwrap_or(false);
        let parent_completed = parent_state
            .map(|s| s.status.as_str() == "completed")
            .unwrap_or(false);
        let mut dir_meta = Map::new();
        for step in &record.prompt_steps {
            let mut agent = build_workflow_step_agent(record, step);
            if let Some(output) = &step.output {
                for (key, value) in output {
                    if key.starts_with("meta_") && !value.is_null() {
                        dir_meta.insert(key.clone(), value.clone());
                    }
                }
            }
            if parent_failed
                && agent.status == "RUNNING"
                && agent.error_message.is_none()
            {
                agent.status = "FAILED".to_string();
                agent.error_message =
                    parent_state.and_then(|s| s.error.clone());
                agent.error_traceback =
                    parent_state.and_then(|s| s.traceback.clone());
            }
            if parent_completed && agent.status == "RUNNING" {
                agent.status = "DONE".to_string();
            }
            agents.push(agent);
        }
        if !dir_meta.is_empty() {
            meta_by_parent.insert(record.timestamp.clone(), dir_meta);
        }
    }
    (agents, meta_by_parent)
}

fn build_workflow_step_agent(
    record: &AgentArtifactRecordWire,
    step: &PromptStepMarkerWire,
) -> AgentWire {
    let mut agent = agent_required(
        "workflow",
        &step.step_name,
        &record.project_file,
        &display_step_status(&step.status),
    );
    agent.start_time = parse_timestamp_14_digit(&record.timestamp);
    agent.workflow = Some(step.workflow_name.clone());
    agent.raw_suffix = Some(record.timestamp.clone());
    agent.parent_workflow = Some(step.workflow_name.clone());
    agent.parent_timestamp = Some(record.timestamp.clone());
    agent.step_name = Some(step.step_name.clone());
    agent.step_type = Some(step.step_type.clone());
    agent.step_source = step.step_source.clone();
    agent.step_output = step.output.clone();
    agent.step_index = step.step_index;
    agent.total_steps = step.total_steps;
    agent.parent_step_index = step.parent_step_index;
    agent.parent_total_steps = step.parent_total_steps;
    agent.is_hidden_step = step.hidden || step.is_pre_prompt_step;
    agent.artifacts_dir = step.artifacts_dir.clone();
    agent.diff_path = step
        .diff_path
        .clone()
        .or_else(|| path_output(&step.output, &step.output_types));
    agent.error_message = step.error.clone();
    agent.error_traceback = step.traceback.clone();
    agent.response_path = step.response_path.clone();
    agent.embedded_workflow_name = step.embedded_workflow_name.clone();
    agent.is_pre_prompt_step = step.is_pre_prompt_step;
    agent.model = step.model.clone();
    agent.llm_provider = step.llm_provider.clone();
    agent
}

fn build_workflow_agents(
    snapshot: &AgentArtifactScanWire,
    step_meta_by_parent: &HashMap<String, Map<String, Value>>,
    input: &AgentComposeInputWire,
) -> Vec<AgentWire> {
    let mut agents = Vec::new();
    for record in &snapshot.records {
        if !is_workflow_state_record(record) {
            continue;
        }
        let Some(state) = &record.workflow_state else {
            continue;
        };
        let mut status = display_workflow_status(&state.status);
        if ACTIVE_WORKFLOW_STATUSES.contains(&status.as_str())
            && state.pid.is_some_and(|pid| !is_pid_alive(pid, input))
            && !state.steps.iter().any(|s| s.status == "in_progress")
        {
            status = "FAILED".to_string();
        }
        let mut agent = agent_required(
            "workflow",
            state.cl_name.as_deref().unwrap_or("unknown"),
            &record.project_file,
            &status,
        );
        agent.start_time = parse_timestamp_14_digit(&record.timestamp)
            .or_else(|| state.start_time.clone());
        agent.workflow = Some(state.workflow_name.clone());
        agent.raw_suffix = Some(record.timestamp.clone());
        agent.pid = state.pid;
        agent.appears_as_agent = state.appears_as_agent;
        agent.is_anonymous = state.is_anonymous;
        agent.artifacts_dir = Some(record.artifact_dir.clone());
        agent.diff_path = workflow_diff_path(&state.steps);
        if let Some(plan_path) =
            record.plan_path.as_ref().and_then(|p| p.plan_path.clone())
        {
            agent.extra_files = vec![plan_path];
        }
        agent.error_message = state.error.clone();
        agent.error_traceback = state.traceback.clone();
        if agent.error_message.is_none() && status == "FAILED" {
            for step in &state.steps {
                if step.status == "failed" {
                    if let Some(error) = &step.error {
                        agent.error_message = Some(format!(
                            "Step '{}' failed: {error}",
                            step.name
                        ));
                        agent.error_traceback = step.traceback.clone();
                        break;
                    }
                }
            }
        }
        agent.step_output = last_step_output(&state.steps);
        if let Some(meta) = step_meta_by_parent.get(&record.timestamp) {
            let mut output = agent.step_output.take().unwrap_or_default();
            for (key, value) in meta {
                output.insert(key.clone(), value.clone());
            }
            agent.step_output = Some(output);
        }
        agent.workspace_num = meta_workspace(&agent.step_output);
        enrich_from_meta(&mut agent, &record.agent_meta, &record.waiting);
        agents.push(agent);
    }
    agents
}

fn build_changespec_agents(
    changespecs: &[ChangeSpecWire],
    bug_by_cl: &HashMap<String, Option<String>>,
    cl_by_cl: &HashMap<String, Option<String>>,
) -> Vec<AgentWire> {
    let mut agents = Vec::new();
    for cs in changespecs {
        let bug = bug_by_cl.get(&cs.name).cloned().flatten();
        let cl_num = cl_by_cl.get(&cs.name).cloned().flatten();
        for hook in &cs.hooks {
            for sl in &hook.status_lines {
                let suffix_type = sl.suffix_type.as_deref();
                if suffix_type != Some("running_agent")
                    && suffix_type != Some("error")
                {
                    continue;
                }
                let suffix = sl.suffix.clone();
                let workflow = if suffix
                    .as_deref()
                    .map(|s| s.to_lowercase().contains("summarize"))
                    .unwrap_or(false)
                {
                    "summarize-hook"
                } else {
                    "fix-hook"
                };
                let status = if suffix_type == Some("running_agent") {
                    "RUNNING"
                } else {
                    &sl.status
                };
                let mut agent =
                    agent_required("run", &cs.name, &cs.file_path, status);
                agent.start_time =
                    parse_timestamp_from_suffix(suffix.as_deref());
                agent.workflow = Some(workflow.to_string());
                agent.hook_command = Some(hook.command.clone());
                agent.commit_entry_id = Some(sl.commit_entry_num.clone());
                agent.pid = extract_pid_from_agent_suffix(suffix.as_deref());
                agent.raw_suffix = suffix.clone();
                agent.bug = bug.clone();
                agent.cl_num = cl_num.clone();
                agent.hidden = true;
                agent.from_changespec = true;
                if suffix_type == Some("error") {
                    agent.error_message = match (&sl.suffix, &sl.summary) {
                        (Some(suffix), Some(summary)) => {
                            Some(format!("{suffix}\n\nOutput: {summary}"))
                        }
                        (Some(suffix), None) => Some(suffix.clone()),
                        _ => None,
                    };
                }
                agents.push(agent);
            }
        }
        for mentor in &cs.mentors {
            for msl in &mentor.status_lines {
                if msl.suffix_type.as_deref() != Some("running_agent") {
                    continue;
                }
                let mut agent =
                    agent_required("run", &cs.name, &cs.file_path, &msl.status);
                agent.start_time =
                    parse_timestamp_from_suffix(msl.suffix.as_deref());
                agent.workflow = Some("mentor".to_string());
                agent.mentor_profile = Some(msl.profile_name.clone());
                agent.mentor_name = Some(msl.mentor_name.clone());
                agent.commit_entry_id = Some(mentor.entry_id.clone());
                agent.pid =
                    extract_pid_from_agent_suffix(msl.suffix.as_deref());
                agent.raw_suffix = msl.suffix.clone();
                agent.bug = bug.clone();
                agent.cl_num = cl_num.clone();
                agent.hidden = true;
                agent.from_changespec = true;
                agents.push(agent);
            }
        }
        for comment in &cs.comments {
            if comment.suffix_type.as_deref() != Some("running_agent") {
                continue;
            }
            let mut agent =
                agent_required("run", &cs.name, &cs.file_path, "RUNNING");
            agent.start_time =
                parse_timestamp_from_suffix(comment.suffix.as_deref());
            agent.workflow = Some("crs".to_string());
            agent.reviewer = Some(comment.reviewer.clone());
            agent.pid =
                extract_pid_from_agent_suffix(comment.suffix.as_deref());
            agent.raw_suffix = comment.suffix.clone();
            agent.bug = bug.clone();
            agent.cl_num = cl_num.clone();
            agent.hidden = true;
            agent.from_changespec = true;
            agents.push(agent);
        }
    }
    agents
}

fn filter_dead_pids(
    agents: Vec<AgentWire>,
    input: &AgentComposeInputWire,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    agents
        .into_iter()
        .filter(|agent| {
            if COMPLETED_STATUSES.contains(&agent.status.as_str())
                || agent.pid.is_none()
            {
                return true;
            }
            let pid = agent.pid.unwrap();
            if is_pid_alive(pid, input) {
                return true;
            }
            diagnostics.dropped.push(DropReasonWire {
                stage: "dead_pid_filter".to_string(),
                identity: agent.identity(),
                reason: "dead_pid".to_string(),
                detail: Some(pid.to_string()),
            });
            false
        })
        .collect()
}

fn dedup_axe_spawned_agents(
    mut agents: Vec<AgentWire>,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let mut running_by_key: HashMap<(String, String), usize> = HashMap::new();
    for (idx, agent) in agents.iter().enumerate() {
        if agent.agent_type != "run" {
            continue;
        }
        let workflow =
            agent.workflow.as_deref().unwrap_or("").replace('-', "_");
        if AXE_WORKFLOW_PREFIXES
            .iter()
            .any(|prefix| workflow.starts_with(prefix))
        {
            let ts = extract_timestamp_from_workflow(Some(&workflow)).or_else(
                || agent.raw_suffix.as_deref().and_then(timestamp_14_to_13),
            );
            if let Some(ts) = ts {
                running_by_key.insert((agent.cl_name.clone(), ts), idx);
            }
        }
        if COMPLETED_STATUSES.contains(&agent.status.as_str())
            && agent.workflow.is_some()
        {
            let norm = agent.workflow.as_deref().unwrap().replace('-', "_");
            let is_done_axe = DONE_AXE_WORKFLOWS.contains(&norm.as_str())
                || DONE_AXE_PREFIXES
                    .iter()
                    .any(|prefix| norm.starts_with(prefix));
            if is_done_axe {
                if let Some(ts) =
                    agent.raw_suffix.as_deref().and_then(timestamp_14_to_13)
                {
                    running_by_key.insert((agent.cl_name.clone(), ts), idx);
                }
            }
        }
    }
    let mut remove = HashSet::new();
    let changespec_indices: Vec<usize> = agents
        .iter()
        .enumerate()
        .filter_map(|(idx, agent)| agent.from_changespec.then_some(idx))
        .collect();
    for idx in changespec_indices {
        let Some(ts) = extract_timestamp_str_from_suffix(
            agents[idx].raw_suffix.as_deref(),
        ) else {
            continue;
        };
        let key = (agents[idx].cl_name.clone(), ts);
        let Some(&target_idx) = running_by_key.get(&key) else {
            continue;
        };
        if target_idx == idx {
            continue;
        }
        let source = agents[idx].clone();
        let fields =
            merge_changespec_specific_fields(&mut agents[target_idx], &source);
        diagnostics.merge_log.push(MergeReasonWire {
            stage: "dedup_axe_spawned_agents".to_string(),
            source_identity: source.identity(),
            target_identity: agents[target_idx].identity(),
            reason: "prefer_running_claim".to_string(),
            fields,
        });
        diagnostics.dropped.push(DropReasonWire {
            stage: "dedup_axe_spawned_agents".to_string(),
            identity: source.identity(),
            reason: "matched_running_claim_timestamp".to_string(),
            detail: Some(key.1),
        });
        remove.insert(idx);
    }
    agents
        .into_iter()
        .enumerate()
        .filter_map(|(idx, agent)| (!remove.contains(&idx)).then_some(agent))
        .collect()
}

fn remove_vcs_workspace_claims(
    mut agents: Vec<AgentWire>,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let mut axe_by_pid = HashMap::new();
    for (idx, agent) in agents.iter().enumerate() {
        if let (Some(pid), Some(workflow)) =
            (agent.pid, agent.workflow.as_deref())
        {
            let norm = workflow.replace('-', "_");
            let is_axe = AXE_WORKFLOW_PREFIXES
                .iter()
                .any(|prefix| norm.starts_with(prefix))
                || PLAIN_AXE_WORKFLOWS.contains(&norm.as_str())
                || PLAIN_AXE_PREFIXES
                    .iter()
                    .any(|prefix| norm.starts_with(prefix));
            if is_axe {
                axe_by_pid.insert(pid, idx);
            }
        }
    }
    let mut remove = HashSet::new();
    for idx in 0..agents.len() {
        let agent = &agents[idx];
        let Some(pid) = agent.pid else {
            continue;
        };
        if agent.agent_type != "run"
            || !agent.workflow.as_deref().is_some_and(|workflow| {
                workflow.starts_with_any(VCS_WORKFLOW_PREFIXES)
            })
        {
            continue;
        }
        let Some(&target_idx) = axe_by_pid.get(&pid) else {
            continue;
        };
        if target_idx == idx {
            continue;
        }
        let source = agents[idx].clone();
        let fields = merge_agent_fields(&mut agents[target_idx], &source);
        diagnostics.merge_log.push(MergeReasonWire {
            stage: "remove_vcs_workspace_claims".to_string(),
            source_identity: source.identity(),
            target_identity: agents[target_idx].identity(),
            reason: "merge_vcs_workspace_claim".to_string(),
            fields,
        });
        diagnostics.dropped.push(DropReasonWire {
            stage: "remove_vcs_workspace_claims".to_string(),
            identity: source.identity(),
            reason: "vcs_workspace_claim_duplicate_pid".to_string(),
            detail: Some(pid.to_string()),
        });
        remove.insert(idx);
    }
    agents
        .into_iter()
        .enumerate()
        .filter_map(|(idx, agent)| (!remove.contains(&idx)).then_some(agent))
        .collect()
}

fn dedup_workflow_entries(
    mut agents: Vec<AgentWire>,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut remove = HashSet::new();
    for idx in 0..agents.len() {
        if agents[idx].agent_type != "workflow" {
            continue;
        }
        let Some(suffix) = agents[idx].raw_suffix.clone() else {
            continue;
        };
        if let Some(&target_idx) = seen.get(&suffix) {
            let source = agents[idx].clone();
            let fields = merge_workflow_duplicate_fields(
                &mut agents[target_idx],
                &source,
            );
            diagnostics.merge_log.push(MergeReasonWire {
                stage: "dedup_workflow_entries".to_string(),
                source_identity: source.identity(),
                target_identity: agents[target_idx].identity(),
                reason: "same_workflow_timestamp".to_string(),
                fields,
            });
            diagnostics.dropped.push(DropReasonWire {
                stage: "dedup_workflow_entries".to_string(),
                identity: source.identity(),
                reason: "duplicate_workflow_timestamp".to_string(),
                detail: Some(suffix),
            });
            remove.insert(idx);
        } else {
            seen.insert(suffix, idx);
        }
    }
    agents
        .into_iter()
        .enumerate()
        .filter_map(|(idx, agent)| (!remove.contains(&idx)).then_some(agent))
        .collect()
}

fn dedup_running_vs_workflow(
    mut agents: Vec<AgentWire>,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let workflow_by_suffix: HashMap<String, usize> = agents
        .iter()
        .enumerate()
        .filter_map(|(idx, a)| {
            (a.agent_type == "workflow")
                .then(|| a.raw_suffix.as_ref().map(|s| (s.clone(), idx)))
                .flatten()
        })
        .collect();
    let mut remove = HashSet::new();
    for idx in 0..agents.len() {
        let agent = &agents[idx];
        let Some(suffix) = agent.raw_suffix.clone() else {
            continue;
        };
        let matches_workflow_claim = agent.agent_type == "run"
            && agent.workflow.as_deref().is_some_and(|workflow| {
                workflow.starts_with("ace(run)")
                    || workflow == "ace-run"
                    || workflow == "run"
            });
        if !matches_workflow_claim {
            continue;
        }
        let Some(&target_idx) = workflow_by_suffix.get(&suffix) else {
            continue;
        };
        let source = agents[idx].clone();
        let fields =
            merge_running_into_workflow(&mut agents[target_idx], &source);
        diagnostics.merge_log.push(MergeReasonWire {
            stage: "dedup_running_vs_workflow".to_string(),
            source_identity: source.identity(),
            target_identity: agents[target_idx].identity(),
            reason: "prefer_workflow_artifact".to_string(),
            fields,
        });
        diagnostics.dropped.push(DropReasonWire {
            stage: "dedup_running_vs_workflow".to_string(),
            identity: source.identity(),
            reason: "running_workflow_duplicate".to_string(),
            detail: Some(suffix),
        });
        remove.insert(idx);
    }
    agents
        .into_iter()
        .enumerate()
        .filter_map(|(idx, agent)| (!remove.contains(&idx)).then_some(agent))
        .collect()
}

fn dedup_by_pid(
    mut agents: Vec<AgentWire>,
    diagnostics: &mut Diagnostics,
) -> Vec<AgentWire> {
    let mut seen: HashMap<i64, usize> = HashMap::new();
    let mut remove = HashSet::new();
    for idx in 0..agents.len() {
        let Some(pid) = agents[idx].pid else {
            continue;
        };
        if let Some(&existing_idx) = seen.get(&pid) {
            let existing_is_vcs = is_vcs_workflow(&agents[existing_idx]);
            let agent_is_vcs = is_vcs_workflow(&agents[idx]);
            let keep_new = if agent_is_vcs && !existing_is_vcs {
                false
            } else if existing_is_vcs && !agent_is_vcs {
                true
            } else if agents[idx].agent_type == "run"
                && agents[existing_idx].agent_type == "workflow"
            {
                false
            } else if agents[existing_idx].agent_type == "run"
                && agents[idx].agent_type == "workflow"
            {
                true
            } else if agents[idx].agent_type == agents[existing_idx].agent_type
                && agents[idx].raw_suffix.is_some()
                && agents[existing_idx].raw_suffix.is_some()
                && agents[idx].raw_suffix != agents[existing_idx].raw_suffix
            {
                continue;
            } else {
                false
            };
            if keep_new {
                let source = agents[existing_idx].clone();
                let fields = merge_agent_fields(&mut agents[idx], &source);
                diagnostics.merge_log.push(MergeReasonWire {
                    stage: "dedup_by_pid".to_string(),
                    source_identity: source.identity(),
                    target_identity: agents[idx].identity(),
                    reason: "prefer_more_specific_pid_owner".to_string(),
                    fields,
                });
                diagnostics.dropped.push(DropReasonWire {
                    stage: "dedup_by_pid".to_string(),
                    identity: source.identity(),
                    reason: "duplicate_pid".to_string(),
                    detail: Some(pid.to_string()),
                });
                remove.insert(existing_idx);
                seen.insert(pid, idx);
            } else {
                let source = agents[idx].clone();
                let target_idx = existing_idx;
                let fields =
                    merge_agent_fields(&mut agents[target_idx], &source);
                diagnostics.merge_log.push(MergeReasonWire {
                    stage: "dedup_by_pid".to_string(),
                    source_identity: source.identity(),
                    target_identity: agents[target_idx].identity(),
                    reason: "prefer_first_pid_owner".to_string(),
                    fields,
                });
                diagnostics.dropped.push(DropReasonWire {
                    stage: "dedup_by_pid".to_string(),
                    identity: source.identity(),
                    reason: "duplicate_pid".to_string(),
                    detail: Some(pid.to_string()),
                });
                remove.insert(idx);
            }
        } else {
            seen.insert(pid, idx);
        }
    }
    agents
        .into_iter()
        .enumerate()
        .filter_map(|(idx, agent)| (!remove.contains(&idx)).then_some(agent))
        .collect()
}

fn apply_status_overrides(agents: &mut [AgentWire]) {
    let parent_by_suffix = index_parent_agents(agents);
    let mut parents_with_active_feedback = HashSet::new();
    for agent in agents.iter() {
        if agent.parent_timestamp.is_some()
            && agent.parent_workflow.is_none()
            && is_feedback_suffix(agent.role_suffix.as_deref())
            && !COMPLETED_STATUSES.contains(&agent.status.as_str())
        {
            parents_with_active_feedback
                .insert(agent.parent_timestamp.clone().unwrap());
        }
    }
    for idx in 0..agents.len() {
        if agents[idx].parent_timestamp.is_some()
            && agents[idx].parent_workflow.is_none()
            && is_feedback_suffix(agents[idx].role_suffix.as_deref())
        {
            let Some(parent_idx) = agents[idx]
                .parent_timestamp
                .as_ref()
                .and_then(|ts| parent_by_suffix.get(ts).copied())
            else {
                continue;
            };
            let child = agents[idx].clone();
            agents[parent_idx].plan_times.extend(child.plan_times);
            agents[parent_idx]
                .feedback_times
                .extend(child.feedback_times);
            agents[parent_idx]
                .questions_times
                .extend(child.questions_times);
            if !COMPLETED_STATUSES.contains(&child.status.as_str())
                && agents[parent_idx].status == "PLANNING"
            {
                agents[parent_idx].status = "RUNNING".to_string();
            }
        }
    }
    for idx in 0..agents.len() {
        if agents[idx].parent_workflow.is_some()
            && agents[idx].parent_timestamp.is_some()
            && !COMPLETED_STATUSES.contains(&agents[idx].status.as_str())
        {
            if let Some(parent_idx) = agents[idx]
                .parent_timestamp
                .as_ref()
                .and_then(|ts| parent_by_suffix.get(ts).copied())
            {
                if agents[parent_idx].status == "PLANNING" {
                    agents[parent_idx].status = "RUNNING".to_string();
                }
            }
        }
    }

    let mut parents_with_followup = HashSet::new();
    let mut followup_override: HashMap<String, String> = HashMap::new();
    let mut completed_followup_override: HashMap<String, Option<String>> =
        HashMap::new();
    let mut ordered_indices: Vec<_> = (0..agents.len()).collect();
    ordered_indices.sort_by_key(|idx| sort_time_key(&agents[*idx]));
    for idx in ordered_indices {
        let agent = agents[idx].clone();
        if agent.parent_timestamp.is_none()
            || agent.parent_workflow.is_some()
            || is_feedback_suffix(agent.role_suffix.as_deref())
        {
            continue;
        }
        let parent_ts = agent.parent_timestamp.clone().unwrap();
        parents_with_followup.insert(parent_ts.clone());
        let Some(parent_idx) = parent_by_suffix.get(&parent_ts).copied() else {
            continue;
        };
        if !COMPLETED_STATUSES.contains(&agent.status.as_str()) {
            let status = match agent.role_suffix.as_deref() {
                Some(".epic") => "EPIC APPROVED",
                Some(".commit") => "PLAN COMMITTED",
                _ => "PLAN APPROVED",
            };
            followup_override.insert(parent_ts.clone(), status.to_string());
        } else if agent.status == "DONE"
            && agent.role_suffix.as_deref() == Some(".epic")
        {
            completed_followup_override
                .insert(parent_ts.clone(), Some("EPIC CREATED".to_string()));
        } else {
            completed_followup_override.insert(parent_ts.clone(), None);
        }
        if let Some(output) = agent.step_output {
            let meta_fields: Vec<_> = output
                .iter()
                .filter(|(key, value)| {
                    key.starts_with("meta_") && !value.is_null()
                })
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            if !meta_fields.is_empty() {
                let target =
                    agents[parent_idx].step_output.get_or_insert_with(Map::new);
                for (key, value) in meta_fields {
                    target.insert(key, value);
                }
            }
        }
        if agent.role_suffix.as_deref() == Some(".code") {
            agents[parent_idx].code_time =
                agent.run_start_time.or(agent.start_time);
        }
        if agent.diff_path.is_some() {
            agents[parent_idx].diff_path = agent.diff_path;
        }
    }
    for (parent_ts, status) in followup_override {
        if let Some(parent_idx) = parent_by_suffix.get(&parent_ts).copied() {
            if agents[parent_idx].status == "DONE" {
                agents[parent_idx].status = status;
            }
        }
    }
    for agent in agents.iter_mut() {
        if is_root_plan_workflow(agent)
            && agent.status == "DONE"
            && agent
                .raw_suffix
                .as_ref()
                .is_some_and(|suffix| parents_with_followup.contains(suffix))
        {
            let suffix = agent.raw_suffix.as_ref().unwrap();
            agent.status = match completed_followup_override
                .get(suffix)
                .and_then(Clone::clone)
            {
                Some(status) => status,
                None => "PLAN DONE".to_string(),
            };
        }
    }
    for agent in agents.iter_mut() {
        if is_root_plan_workflow(agent)
            && agent.status == "DONE"
            && !agent
                .raw_suffix
                .as_ref()
                .is_some_and(|suffix| parents_with_followup.contains(suffix))
        {
            agent.status = if agent.raw_suffix.as_ref().is_some_and(|suffix| {
                parents_with_active_feedback.contains(suffix)
            }) {
                "RUNNING".to_string()
            } else {
                "PLANNING".to_string()
            };
        }
    }
    let parent_by_suffix = index_parent_agents(agents);
    for idx in 0..agents.len() {
        if agents[idx].status == "DONE"
            && !agents[idx].questions_times.is_empty()
            && agents[idx]
                .raw_suffix
                .as_ref()
                .is_some_and(|suffix| !parents_with_followup.contains(suffix))
        {
            agents[idx].status = "QUESTION".to_string();
        }
        if agents[idx].parent_timestamp.is_some()
            && agents[idx].parent_workflow.is_none()
        {
            if let Some(parent_idx) = agents[idx]
                .parent_timestamp
                .as_ref()
                .and_then(|ts| parent_by_suffix.get(ts).copied())
            {
                let child_identity = agents[idx].identity();
                if !agents[parent_idx]
                    .followup_identities
                    .contains(&child_identity)
                {
                    agents[parent_idx].followup_identities.push(child_identity);
                }
            }
        }
    }
    let by_suffix = index_by_suffix(agents);
    for idx in 0..agents.len() {
        if let Some(parent_idx) = agents[idx]
            .retry_of_timestamp
            .as_ref()
            .and_then(|ts| by_suffix.get(ts).copied())
        {
            let child_identity = agents[idx].identity();
            if !agents[parent_idx]
                .retry_chain_sibling_identities
                .contains(&child_identity)
            {
                agents[parent_idx]
                    .retry_chain_sibling_identities
                    .push(child_identity);
            }
        }
    }
}

fn split_dismissed(
    agents: Vec<AgentWire>,
    input: &AgentComposeInputWire,
    dismissed_from_loader: &mut Vec<AgentWire>,
) -> Vec<AgentWire> {
    if input.dismissed_identities.is_empty()
        && input.dismissed_suffixes.is_empty()
    {
        return agents;
    }
    let identities: HashSet<_> =
        input.dismissed_identities.iter().cloned().collect();
    let suffixes: HashSet<_> =
        input.dismissed_suffixes.iter().cloned().collect();
    let mut visible = Vec::new();
    for agent in agents {
        let dismissed = identities.contains(&agent.identity())
            || agent
                .raw_suffix
                .as_ref()
                .is_some_and(|suffix| suffixes.contains(suffix));
        if dismissed {
            dismissed_from_loader.push(agent);
        } else {
            visible.push(agent);
        }
    }
    visible
}

fn sort_and_reorder(
    agents: Vec<AgentWire>,
    workflow_agent_steps: &mut [AgentWire],
) -> Vec<AgentWire> {
    let mut sorted = agents;
    sorted.sort_by(compare_start_desc);

    let mut followups_by_parent: HashMap<String, Vec<AgentWire>> =
        HashMap::new();
    let mut non_followup = Vec::new();
    for agent in sorted {
        if let Some(parent_ts) = &agent.parent_timestamp {
            if agent.parent_workflow.is_none() {
                followups_by_parent
                    .entry(parent_ts.clone())
                    .or_default()
                    .push(agent);
                continue;
            }
        }
        non_followup.push(agent);
    }
    for followups in followups_by_parent.values_mut() {
        followups.sort_by_key(sort_time_key);
    }

    workflow_agent_steps.sort_by(|a, b| {
        let a_parent = a.parent_timestamp.clone().unwrap_or_default();
        let b_parent = b.parent_timestamp.clone().unwrap_or_default();
        a_parent
            .cmp(&b_parent)
            .then_with(|| step_sort_key(a).cmp(&step_sort_key(b)))
    });
    let mut steps_by_parent: HashMap<String, Vec<AgentWire>> = HashMap::new();
    for step in workflow_agent_steps.iter().cloned() {
        if let Some(parent_ts) = &step.parent_timestamp {
            steps_by_parent
                .entry(parent_ts.clone())
                .or_default()
                .push(step);
        }
    }
    for steps in steps_by_parent.values_mut() {
        steps.sort_by_key(step_sort_key);
    }
    let prompt_step_by_parent: HashMap<_, _> = steps_by_parent
        .iter()
        .filter_map(|(parent_ts, steps)| {
            steps
                .iter()
                .find(|step| {
                    step.step_type.as_deref() == Some("agent")
                        && !step.is_hidden_step
                        && step.parent_step_index.is_none()
                })
                .map(|step| {
                    (
                        parent_ts.clone(),
                        (
                            step.step_index.unwrap_or(0),
                            step.total_steps.unwrap_or(1),
                        ),
                    )
                })
        })
        .collect();
    for (parent_ts, followups) in &mut followups_by_parent {
        if let Some((step_idx, total_steps)) =
            prompt_step_by_parent.get(parent_ts)
        {
            for agent in followups {
                if agent.role_suffix.is_some() && agent.step_index.is_none() {
                    agent.step_index = Some(*step_idx);
                    agent.total_steps = Some(*total_steps);
                }
            }
        }
    }

    let mut result = Vec::new();
    for agent in non_followup {
        let suffix = agent.raw_suffix.clone();
        let inserts_after = agent.agent_type == "workflow"
            || agent.workflow.as_deref().is_some_and(|workflow| {
                workflow.starts_with("workflow-")
                    || workflow.starts_with("ace(run)")
            });
        result.push(agent);
        let Some(suffix) = suffix else {
            continue;
        };
        if inserts_after {
            let steps = steps_by_parent.remove(&suffix).unwrap_or_default();
            let followups =
                followups_by_parent.remove(&suffix).unwrap_or_default();
            let mut main_agent_steps = Vec::new();
            let mut other_steps = Vec::new();
            for step in steps {
                if step.step_type.as_deref() == Some("agent")
                    && step.parent_step_index.is_none()
                {
                    main_agent_steps.push(step);
                } else {
                    other_steps.push(step);
                }
            }
            result.extend(main_agent_steps);
            result.extend(followups);
            result.extend(other_steps);
        } else if let Some(followups) = followups_by_parent.remove(&suffix) {
            result.extend(followups);
        }
    }
    for followups in followups_by_parent.into_values() {
        result.extend(followups);
    }
    result
}

fn agent_required(
    agent_type: &str,
    cl_name: &str,
    project_file: &str,
    status: &str,
) -> AgentWire {
    AgentWire {
        agent_type: agent_type.to_string(),
        cl_name: cl_name.to_string(),
        project_file: project_file.to_string(),
        status: status.to_string(),
        start_time: None,
        run_start_time: None,
        stop_time: None,
        workspace_num: None,
        workflow: None,
        hook_command: None,
        commit_entry_id: None,
        mentor_profile: None,
        mentor_name: None,
        reviewer: None,
        pid: None,
        raw_suffix: None,
        response_path: None,
        diff_path: None,
        extra_files: Vec::new(),
        bug: None,
        cl_num: None,
        parent_workflow: None,
        parent_timestamp: None,
        step_name: None,
        step_type: None,
        step_source: None,
        step_output: None,
        step_index: None,
        total_steps: None,
        parent_step_index: None,
        parent_total_steps: None,
        is_hidden_step: false,
        appears_as_agent: false,
        is_anonymous: false,
        error_message: None,
        error_traceback: None,
        output_path: None,
        model: None,
        llm_provider: None,
        vcs_provider: None,
        workspace_dir: None,
        agent_name: None,
        waiting_for: Vec::new(),
        wait_duration: None,
        wait_until: None,
        artifacts_dir: None,
        embedded_workflow_name: None,
        is_pre_prompt_step: false,
        hidden: false,
        retry_count: 0,
        max_retries: 0,
        retry_next_at_epoch: None,
        retry_wait_seconds: 0,
        using_fallback: false,
        fallback_model: None,
        retry_status: None,
        from_changespec: false,
        approve: false,
        role_suffix: None,
        tag: None,
        retry_of_timestamp: None,
        retry_attempt: 0,
        retry_chain_root_timestamp: None,
        retried_as_timestamp: None,
        retry_terminal: false,
        retry_error_category: None,
        plan_times: Vec::new(),
        code_time: None,
        feedback_times: Vec::new(),
        questions_times: Vec::new(),
        retry_times: Vec::new(),
        followup_identities: Vec::new(),
        retry_chain_sibling_identities: Vec::new(),
    }
}

fn is_done_record(record: &AgentArtifactRecordWire) -> bool {
    DONE_WORKFLOW_DIR_NAMES.contains(&record.workflow_dir_name.as_str())
        || DONE_WORKFLOW_DIR_PREFIXES
            .iter()
            .any(|prefix| record.workflow_dir_name.starts_with(prefix))
}

fn is_workflow_state_record(record: &AgentArtifactRecordWire) -> bool {
    WORKFLOW_STATE_DIR_NAMES.contains(&record.workflow_dir_name.as_str())
        || WORKFLOW_STATE_DIR_PREFIXES
            .iter()
            .any(|prefix| record.workflow_dir_name.starts_with(prefix))
}

fn display_step_status(status: &str) -> String {
    match status {
        "waiting_hitl" => "WAITING INPUT",
        "completed" => "DONE",
        "in_progress" => "RUNNING",
        "failed" => "FAILED",
        other => return other.to_uppercase(),
    }
    .to_string()
}

fn display_workflow_status(status: &str) -> String {
    match status {
        "waiting_hitl" => "WAITING INPUT",
        "completed" => "DONE",
        "failed" => "FAILED",
        _ => "RUNNING",
    }
    .to_string()
}

fn enrich_from_meta(
    agent: &mut AgentWire,
    meta: &Option<crate::agent_scan::AgentMetaWire>,
    waiting: &Option<crate::agent_scan::WaitingMarkerWire>,
) {
    if let Some(meta) = meta {
        set_opt_if_none(&mut agent.agent_name, &meta.name);
        set_opt_if_none(&mut agent.workflow, &meta.workflow_name);
        set_opt_if_none(&mut agent.pid, &meta.pid);
        set_opt_if_none(&mut agent.model, &meta.model);
        set_opt_if_none(&mut agent.llm_provider, &meta.llm_provider);
        set_opt_if_none(&mut agent.vcs_provider, &meta.vcs_provider);
        set_opt_if_none(&mut agent.role_suffix, &meta.role_suffix);
        set_opt_if_none(&mut agent.parent_timestamp, &meta.parent_timestamp);
        set_opt_if_none(&mut agent.workspace_num, &meta.workspace_num);
        set_opt_if_none(&mut agent.workspace_dir, &meta.workspace_dir);
        if meta.approve {
            agent.approve = true;
        }
        if meta.hidden {
            agent.hidden = true;
        }
        if !meta.wait_for.is_empty() && agent.waiting_for.is_empty() {
            agent.waiting_for = meta.wait_for.clone();
        }
        set_opt_if_none(&mut agent.wait_duration, &meta.wait_duration);
        set_opt_if_none(&mut agent.wait_until, &meta.wait_until);
        if !meta.plan_submitted_at.is_empty() {
            agent.plan_times.extend(meta.plan_submitted_at.clone());
        }
        if !meta.feedback_submitted_at.is_empty() {
            agent
                .feedback_times
                .extend(meta.feedback_submitted_at.clone());
        }
        if !meta.questions_submitted_at.is_empty() {
            agent
                .questions_times
                .extend(meta.questions_submitted_at.clone());
        }
        if !meta.retry_started_at.is_empty() {
            agent.retry_times.extend(meta.retry_started_at.clone());
        }
        set_opt_if_none(&mut agent.run_start_time, &meta.run_started_at);
        set_opt_if_none(&mut agent.stop_time, &meta.stopped_at);
        set_opt_if_none(
            &mut agent.retry_of_timestamp,
            &meta.retry_of_timestamp,
        );
        if let Some(attempt) = meta.retry_attempt {
            agent.retry_attempt = attempt;
        }
        set_opt_if_none(
            &mut agent.retry_chain_root_timestamp,
            &meta.retry_chain_root_timestamp,
        );
        set_opt_if_none(
            &mut agent.retried_as_timestamp,
            &meta.retried_as_timestamp,
        );
        if meta.retry_terminal {
            agent.retry_terminal = true;
        }
        set_opt_if_none(
            &mut agent.retry_error_category,
            &meta.retry_error_category,
        );
    }
    if let Some(waiting) = waiting {
        if !waiting.waiting_for.is_empty() {
            agent.waiting_for = waiting.waiting_for.clone();
        }
        set_opt_if_none(&mut agent.wait_duration, &waiting.wait_duration);
        set_opt_if_none(&mut agent.wait_until, &waiting.wait_until);
    }
}

fn enrich_from_prompt_markers(
    agent: &mut AgentWire,
    prompt_steps: &[PromptStepMarkerWire],
) {
    for step in prompt_steps {
        set_opt_if_none(&mut agent.diff_path, &step.diff_path);
        set_opt_if_none(&mut agent.response_path, &step.response_path);
        if agent.step_output.is_none() {
            agent.step_output = step.output.clone();
        }
    }
}

fn set_opt_if_none<T: Clone>(target: &mut Option<T>, source: &Option<T>) {
    if target.is_none() {
        *target = source.clone();
    }
}

fn is_pid_alive(pid: i64, input: &AgentComposeInputWire) -> bool {
    if input.dead_pids.contains(&pid) {
        return false;
    }
    if !input.alive_pids.is_empty() {
        return input.alive_pids.contains(&pid);
    }
    true
}

fn parse_timestamp_14_digit(value: &str) -> Option<String> {
    if value.len() != 14 || !value.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    NaiveDateTime::parse_from_str(value, "%Y%m%d%H%M%S")
        .ok()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
}

fn parse_timestamp_13_char(value: &str) -> Option<String> {
    if value.len() != 13 || value.as_bytes().get(6) != Some(&b'_') {
        return None;
    }
    NaiveDateTime::parse_from_str(value, "%y%m%d_%H%M%S")
        .ok()
        .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S").to_string())
}

fn parse_timestamp_from_suffix(suffix: Option<&str>) -> Option<String> {
    extract_timestamp_str_from_suffix(suffix)
        .as_deref()
        .and_then(parse_timestamp_13_char)
}

fn extract_timestamp_str_from_suffix(suffix: Option<&str>) -> Option<String> {
    let suffix = suffix?;
    let last = suffix.rsplit('-').next()?;
    (last.len() == 13 && last.as_bytes().get(6) == Some(&b'_'))
        .then(|| last.to_string())
}

fn extract_timestamp_from_workflow(workflow: Option<&str>) -> Option<String> {
    let workflow = workflow?;
    let last = workflow.rsplit('-').next()?;
    (last.len() == 13 && last.as_bytes().get(6) == Some(&b'_'))
        .then(|| last.to_string())
}

fn normalize_to_14_digit(value: Option<&str>) -> Option<String> {
    let value = value?;
    if value.len() == 14 && value.chars().all(|c| c.is_ascii_digit()) {
        return Some(value.to_string());
    }
    if value.len() == 13 && value.as_bytes().get(6) == Some(&b'_') {
        return Some(format!("20{}{}", &value[..6], &value[7..]));
    }
    None
}

fn timestamp_14_to_13(value: &str) -> Option<String> {
    (value.len() == 14 && value.chars().all(|c| c.is_ascii_digit()))
        .then(|| format!("{}_{}", &value[2..8], &value[8..]))
}

fn extract_pid_from_agent_suffix(suffix: Option<&str>) -> Option<i64> {
    let suffix = suffix?;
    let mut parts = suffix.rsplitn(3, '-');
    let _ts = parts.next()?;
    let pid = parts.next()?;
    pid.parse().ok()
}

fn done_extra_files(
    plan_path: Option<&str>,
    markdown_pdf_paths: &[String],
    image_paths: &[String],
) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut files = Vec::new();
    for path in plan_path
        .into_iter()
        .chain(markdown_pdf_paths.iter().map(String::as_str))
        .chain(image_paths.iter().map(String::as_str))
    {
        if path.is_empty() || !seen.insert(path.to_string()) {
            continue;
        }
        files.push(path.to_string());
    }
    files
}

fn path_output(
    output: &Option<Map<String, Value>>,
    output_types: &Option<std::collections::BTreeMap<String, String>>,
) -> Option<String> {
    let output = output.as_ref()?;
    let output_types = output_types.as_ref()?;
    for (field_name, field_type) in output_types {
        if field_type == "path" {
            if let Some(value) = output.get(field_name).and_then(Value::as_str)
            {
                return Some(value.to_string());
            }
        }
    }
    None
}

fn workflow_diff_path(steps: &[WorkflowStepStateWire]) -> Option<String> {
    for step in steps.iter().rev() {
        if let Some(value) = step
            .output
            .as_ref()
            .and_then(|output| output.get("diff_path"))
            .and_then(Value::as_str)
        {
            return Some(value.to_string());
        }
    }
    steps
        .last()
        .and_then(|step| path_output(&step.output, &step.output_types))
}

fn last_step_output(
    steps: &[WorkflowStepStateWire],
) -> Option<Map<String, Value>> {
    steps.iter().rev().find_map(|step| step.output.clone())
}

fn meta_workspace(output: &Option<Map<String, Value>>) -> Option<i64> {
    let value = output.as_ref()?.get("meta_workspace")?;
    if let Some(i) = value.as_i64() {
        return Some(i);
    }
    value.as_str()?.parse().ok()
}

fn merge_agent_fields(
    target: &mut AgentWire,
    source: &AgentWire,
) -> Vec<String> {
    let mut fields = Vec::new();
    merge_opt(
        &mut target.workspace_num,
        &source.workspace_num,
        "workspace_num",
        &mut fields,
    );
    merge_opt(&mut target.model, &source.model, "model", &mut fields);
    merge_opt(
        &mut target.llm_provider,
        &source.llm_provider,
        "llm_provider",
        &mut fields,
    );
    merge_opt(
        &mut target.vcs_provider,
        &source.vcs_provider,
        "vcs_provider",
        &mut fields,
    );
    merge_opt(
        &mut target.agent_name,
        &source.agent_name,
        "agent_name",
        &mut fields,
    );
    merge_opt(&mut target.bug, &source.bug, "bug", &mut fields);
    merge_opt(&mut target.cl_num, &source.cl_num, "cl_num", &mut fields);
    merge_opt(
        &mut target.diff_path,
        &source.diff_path,
        "diff_path",
        &mut fields,
    );
    if target.extra_files.is_empty() && !source.extra_files.is_empty() {
        target.extra_files = source.extra_files.clone();
        fields.push("extra_files".to_string());
    }
    merge_opt(
        &mut target.artifacts_dir,
        &source.artifacts_dir,
        "artifacts_dir",
        &mut fields,
    );
    if target.waiting_for.is_empty() && !source.waiting_for.is_empty() {
        target.waiting_for = source.waiting_for.clone();
        fields.push("waiting_for".to_string());
    }
    if !target.approve && source.approve {
        target.approve = true;
        fields.push("approve".to_string());
    }
    merge_opt(
        &mut target.start_time,
        &source.start_time,
        "start_time",
        &mut fields,
    );
    merge_opt(
        &mut target.raw_suffix,
        &source.raw_suffix,
        "raw_suffix",
        &mut fields,
    );
    merge_opt(
        &mut target.mentor_profile,
        &source.mentor_profile,
        "mentor_profile",
        &mut fields,
    );
    merge_opt(
        &mut target.mentor_name,
        &source.mentor_name,
        "mentor_name",
        &mut fields,
    );
    merge_opt(
        &mut target.hook_command,
        &source.hook_command,
        "hook_command",
        &mut fields,
    );
    merge_opt(
        &mut target.reviewer,
        &source.reviewer,
        "reviewer",
        &mut fields,
    );
    merge_opt(
        &mut target.commit_entry_id,
        &source.commit_entry_id,
        "commit_entry_id",
        &mut fields,
    );
    fields
}

fn merge_changespec_specific_fields(
    target: &mut AgentWire,
    source: &AgentWire,
) -> Vec<String> {
    let mut fields = Vec::new();
    merge_opt(
        &mut target.hook_command,
        &source.hook_command,
        "hook_command",
        &mut fields,
    );
    merge_opt(
        &mut target.mentor_profile,
        &source.mentor_profile,
        "mentor_profile",
        &mut fields,
    );
    merge_opt(
        &mut target.mentor_name,
        &source.mentor_name,
        "mentor_name",
        &mut fields,
    );
    merge_opt(
        &mut target.reviewer,
        &source.reviewer,
        "reviewer",
        &mut fields,
    );
    merge_opt(
        &mut target.commit_entry_id,
        &source.commit_entry_id,
        "commit_entry_id",
        &mut fields,
    );
    merge_opt(
        &mut target.start_time,
        &source.start_time,
        "start_time",
        &mut fields,
    );
    if target.raw_suffix.is_none() {
        if let Some(ts) =
            extract_timestamp_str_from_suffix(source.raw_suffix.as_deref())
                .and_then(|ts| normalize_to_14_digit(Some(&ts)))
        {
            target.raw_suffix = Some(ts);
            fields.push("raw_suffix".to_string());
        }
    }
    fields
}

fn merge_workflow_duplicate_fields(
    target: &mut AgentWire,
    source: &AgentWire,
) -> Vec<String> {
    let mut fields = Vec::new();
    merge_opt(
        &mut target.workspace_num,
        &source.workspace_num,
        "workspace_num",
        &mut fields,
    );
    if target.cl_name == "unknown" && source.cl_name != "unknown" {
        target.cl_name = source.cl_name.clone();
        fields.push("cl_name".to_string());
    }
    if target.status == "RUNNING" && source.status != "RUNNING" {
        target.status = source.status.clone();
        fields.push("status".to_string());
    }
    merge_opt(&mut target.pid, &source.pid, "pid", &mut fields);
    merge_opt(
        &mut target.error_traceback,
        &source.error_traceback,
        "error_traceback",
        &mut fields,
    );
    merge_opt(
        &mut target.agent_name,
        &source.agent_name,
        "agent_name",
        &mut fields,
    );
    fields
}

fn merge_running_into_workflow(
    target: &mut AgentWire,
    source: &AgentWire,
) -> Vec<String> {
    let mut fields = merge_agent_fields(target, source);
    if target.cl_name == "unknown" && source.cl_name != "unknown" {
        target.cl_name = source.cl_name.clone();
        fields.push("cl_name".to_string());
    }
    merge_opt(
        &mut target.response_path,
        &source.response_path,
        "response_path",
        &mut fields,
    );
    merge_opt(
        &mut target.error_message,
        &source.error_message,
        "error_message",
        &mut fields,
    );
    merge_opt(
        &mut target.error_traceback,
        &source.error_traceback,
        "error_traceback",
        &mut fields,
    );
    if target.step_output.is_none() && source.step_output.is_some() {
        target.step_output = source.step_output.clone();
        fields.push("step_output".to_string());
    } else if let (Some(target_output), Some(source_output)) =
        (&mut target.step_output, &source.step_output)
    {
        for (key, value) in source_output {
            if !target_output.contains_key(key) {
                target_output.insert(key.clone(), value.clone());
            }
        }
    }
    if target.status == "RUNNING" && source.status != "RUNNING" {
        target.status = source.status.clone();
        fields.push("status".to_string());
    }
    fields
}

fn merge_opt<T: Clone>(
    target: &mut Option<T>,
    source: &Option<T>,
    field: &str,
    fields: &mut Vec<String>,
) {
    if target.is_none() && source.is_some() {
        *target = source.clone();
        fields.push(field.to_string());
    }
}

fn is_vcs_workflow(agent: &AgentWire) -> bool {
    agent
        .workflow
        .as_deref()
        .is_some_and(|workflow| workflow.starts_with_any(VCS_WORKFLOW_PREFIXES))
}

fn is_feedback_suffix(suffix: Option<&str>) -> bool {
    let Some(suffix) = suffix else {
        return false;
    };
    suffix.strip_prefix('.').is_some_and(|tail| {
        !tail.is_empty() && tail.chars().all(|c| c.is_ascii_digit())
    })
}

fn is_root_plan_workflow(agent: &AgentWire) -> bool {
    agent.agent_type == "workflow"
        && agent.role_suffix.as_deref() == Some(".plan")
        && agent.parent_workflow.is_none()
}

fn index_parent_agents(agents: &[AgentWire]) -> HashMap<String, usize> {
    agents
        .iter()
        .enumerate()
        .filter_map(|(idx, agent)| {
            (agent.parent_workflow.is_none())
                .then(|| {
                    agent
                        .raw_suffix
                        .as_ref()
                        .map(|suffix| (suffix.clone(), idx))
                })
                .flatten()
        })
        .collect()
}

fn index_by_suffix(agents: &[AgentWire]) -> HashMap<String, usize> {
    agents
        .iter()
        .enumerate()
        .filter_map(|(idx, agent)| {
            agent
                .raw_suffix
                .as_ref()
                .map(|suffix| (suffix.clone(), idx))
        })
        .collect()
}

fn compare_start_desc(a: &AgentWire, b: &AgentWire) -> std::cmp::Ordering {
    match (&a.start_time, &b.start_time) {
        (Some(a), Some(b)) => b.cmp(a),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn sort_time_key(agent: &AgentWire) -> String {
    agent
        .run_start_time
        .as_ref()
        .or(agent.start_time.as_ref())
        .cloned()
        .unwrap_or_default()
}

fn step_sort_key(agent: &AgentWire) -> (i32, i64, i32, i64) {
    let status_priority = if COMPLETED_STATUSES.contains(&agent.status.as_str())
    {
        0
    } else {
        1
    };
    (
        status_priority,
        agent
            .parent_step_index
            .unwrap_or_else(|| agent.step_index.unwrap_or(0)),
        if agent.parent_step_index.is_some() {
            1
        } else {
            0
        },
        agent.step_index.unwrap_or(0),
    )
}

fn parent_project_dir(project_file: &str) -> String {
    project_file
        .rsplit_once('/')
        .map(|(parent, _)| parent.to_string())
        .unwrap_or_default()
}

trait StartsWithAny {
    fn starts_with_any(&self, prefixes: &[&str]) -> bool;
}

impl StartsWithAny for str {
    fn starts_with_any(&self, prefixes: &[&str]) -> bool {
        prefixes.iter().any(|prefix| self.starts_with(prefix))
    }
}

impl StartsWithAny for String {
    fn starts_with_any(&self, prefixes: &[&str]) -> bool {
        self.as_str().starts_with_any(prefixes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_scan::{
        AgentArtifactScanOptionsWire, AgentArtifactScanStatsWire,
        AgentMetaWire, PlanPathMarkerWire, PromptStepMarkerWire,
        WorkflowStateWire, WorkflowStepStateWire,
    };
    use crate::wire::{HookStatusLineWire, HookWire, SourceSpanWire};
    use serde_json::json;

    fn scan(records: Vec<AgentArtifactRecordWire>) -> AgentArtifactScanWire {
        AgentArtifactScanWire {
            schema_version: 1,
            projects_root: "/tmp/sase/projects".into(),
            options: AgentArtifactScanOptionsWire::default(),
            stats: AgentArtifactScanStatsWire::default(),
            records,
        }
    }

    fn record(
        workflow_dir_name: &str,
        timestamp: &str,
    ) -> AgentArtifactRecordWire {
        AgentArtifactRecordWire {
            project_name: "demo".into(),
            project_dir: "/tmp/sase/projects/demo".into(),
            project_file: "/tmp/sase/projects/demo/demo.gp".into(),
            workflow_dir_name: workflow_dir_name.into(),
            artifact_dir: format!("/tmp/sase/projects/demo/artifacts/{workflow_dir_name}/{timestamp}"),
            timestamp: timestamp.into(),
            agent_meta: None,
            done: None,
            running: None,
            waiting: None,
            workflow_state: None,
            plan_path: None,
            prompt_steps: Vec::new(),
            raw_prompt_snippet: None,
            has_done_marker: false,
        }
    }

    #[test]
    fn running_claim_and_workflow_artifact_are_deduped_and_ordered() {
        let mut wf = record("workflow-three_phase", "20260501090000");
        wf.workflow_state = Some(WorkflowStateWire {
            workflow_name: "three_phase".into(),
            cl_name: Some("demo_plan".into()),
            status: "completed".into(),
            pid: Some(77),
            appears_as_agent: true,
            is_anonymous: false,
            current_step_index: 0,
            start_time: None,
            error: None,
            traceback: None,
            steps: vec![WorkflowStepStateWire {
                name: "plan".into(),
                status: "completed".into(),
                output: Some(Map::from_iter([(
                    "meta_workspace".into(),
                    json!("17"),
                )])),
                output_types: None,
                error: None,
                traceback: None,
            }],
        });
        wf.agent_meta = Some(AgentMetaWire {
            role_suffix: Some(".plan".into()),
            plan_submitted_at: vec!["2026-05-01T09:04:00".into()],
            ..AgentMetaWire::default()
        });
        wf.plan_path = Some(PlanPathMarkerWire {
            plan_path: Some("/tmp/plans/demo.md".into()),
        });
        wf.prompt_steps = vec![PromptStepMarkerWire {
            file_name: "prompt_step_0.json".into(),
            workflow_name: "three_phase".into(),
            step_name: "plan".into(),
            step_type: "agent".into(),
            step_source: None,
            step_index: Some(0),
            total_steps: Some(1),
            parent_step_index: None,
            parent_total_steps: None,
            status: "completed".into(),
            hidden: false,
            is_pre_prompt_step: false,
            embedded_workflow_name: None,
            artifacts_dir: Some("/tmp/artifacts/plan".into()),
            diff_path: None,
            response_path: None,
            error: None,
            traceback: None,
            model: None,
            llm_provider: None,
            output: Some(Map::from_iter([(
                "meta_commit_message".into(),
                json!("ship it"),
            )])),
            output_types: None,
        }];
        let input = AgentComposeInputWire {
            artifact_scan: Some(scan(vec![wf])),
            alive_pids: vec![77, 4242],
            running_claims: vec![RunningClaimWire {
                project_file: "/tmp/sase/projects/demo/demo.gp".into(),
                project_name: "demo".into(),
                cl_name: "demo_plan".into(),
                workspace_num: Some(17),
                workflow: Some("ace(run)".into()),
                raw_suffix: Some("20260501090000".into()),
                pid: Some(77),
                model: Some("gpt-5.5".into()),
                ..running_claim("demo_plan")
            }],
            ..AgentComposeInputWire::default()
        };

        let result = compose_agent_list(&input);

        assert_eq!(result.agents.len(), 2);
        assert_eq!(result.agents[0].agent_type, "workflow");
        assert_eq!(result.agents[0].status, "PLANNING");
        assert_eq!(result.agents[0].workspace_num, Some(17));
        assert_eq!(result.agents[0].model.as_deref(), Some("gpt-5.5"));
        assert_eq!(result.agents[1].step_name.as_deref(), Some("plan"));
        assert!(result
            .merge_log
            .iter()
            .any(|entry| entry.stage == "dedup_running_vs_workflow"));
    }

    #[test]
    fn active_followup_promotes_plan_to_approved_and_links_identity() {
        let mut plan_done = record("workflow-three_phase", "20260501090000");
        plan_done.workflow_state = Some(WorkflowStateWire {
            workflow_name: "three_phase".into(),
            cl_name: Some("demo_plan".into()),
            status: "completed".into(),
            appears_as_agent: true,
            ..WorkflowStateWire::default()
        });
        plan_done.agent_meta = Some(AgentMetaWire {
            role_suffix: Some(".plan".into()),
            ..AgentMetaWire::default()
        });
        let mut code_child = record("ace-run", "20260501090800");
        code_child.project_name = "home".into();
        code_child.project_file = "/tmp/sase/projects/home/home.gp".into();
        code_child.running = Some(crate::agent_scan::RunningMarkerWire {
            pid: Some(4242),
            cl_name: Some("demo_plan".into()),
            model: Some("gpt-5.5".into()),
            llm_provider: Some("codex".into()),
            vcs_provider: Some("GitHub".into()),
            workspace_dir: None,
        });
        code_child.agent_meta = Some(AgentMetaWire {
            role_suffix: Some(".code".into()),
            parent_timestamp: Some("20260501090000".into()),
            name: Some("demo-code".into()),
            ..AgentMetaWire::default()
        });
        let input = AgentComposeInputWire {
            artifact_scan: Some(scan(vec![plan_done, code_child])),
            alive_pids: vec![4242],
            ..AgentComposeInputWire::default()
        };

        let result = compose_agent_list(&input);

        assert_eq!(result.agents[0].status, "PLAN APPROVED");
        assert_eq!(
            result.agents[0].followup_identities,
            vec![AgentIdentityWire(
                "run".into(),
                "demo_plan".into(),
                Some("20260501090800".into())
            )]
        );
        assert_eq!(result.agents[1].step_index, None);
    }

    #[test]
    fn changespec_axe_agent_merges_into_running_claim() {
        let input = AgentComposeInputWire {
            running_claims: vec![RunningClaimWire {
                workflow: Some("axe(fix-hook)-260501_120000".into()),
                raw_suffix: Some("20260501120000".into()),
                pid: Some(100),
                ..running_claim("demo_cl")
            }],
            changespecs: vec![ChangeSpecWire {
                schema_version: 1,
                name: "demo_cl".into(),
                project_basename: "demo".into(),
                file_path: "/tmp/sase/projects/demo/demo.gp".into(),
                source_span: SourceSpanWire {
                    file_path: "/tmp/sase/projects/demo/demo.gp".into(),
                    start_line: 1,
                    end_line: 10,
                },
                status: "WIP".into(),
                parent: None,
                cl_or_pr: Some("123".into()),
                bug: Some("http://b/456".into()),
                description: String::new(),
                test_targets: Vec::new(),
                kickstart: None,
                commits: Vec::new(),
                hooks: vec![HookWire {
                    command: "just test".into(),
                    status_lines: vec![HookStatusLineWire {
                        commit_entry_num: "1".into(),
                        timestamp: "20260501_120000".into(),
                        status: "RUNNING".into(),
                        duration: None,
                        suffix: Some("fix_hook-100-260501_120000".into()),
                        suffix_type: Some("running_agent".into()),
                        summary: None,
                    }],
                }],
                comments: Vec::new(),
                mentors: Vec::new(),
                timestamps: Vec::new(),
                deltas: Vec::new(),
            }],
            alive_pids: vec![100],
            ..AgentComposeInputWire::default()
        };

        let result = compose_agent_list(&input);

        assert_eq!(result.agents.len(), 1);
        assert_eq!(result.agents[0].hook_command.as_deref(), Some("just test"));
        assert_eq!(result.agents[0].cl_num.as_deref(), Some("123"));
        assert!(result.dropped.iter().any(|drop| {
            drop.stage == "dedup_axe_spawned_agents"
                && drop.reason == "matched_running_claim_timestamp"
        }));
    }

    #[test]
    fn dead_pids_are_host_owned_input() {
        let input = AgentComposeInputWire {
            running_claims: vec![RunningClaimWire {
                pid: Some(999),
                raw_suffix: Some("20260501121212".into()),
                ..running_claim("dead")
            }],
            dead_pids: vec![999],
            ..AgentComposeInputWire::default()
        };

        let result = compose_agent_list(&input);

        assert!(result.agents.is_empty());
        assert_eq!(result.dropped[0].stage, "dead_pid_filter");
    }

    fn running_claim(cl_name: &str) -> RunningClaimWire {
        RunningClaimWire {
            project_file: "/tmp/sase/projects/demo/demo.gp".into(),
            project_name: "demo".into(),
            cl_name: cl_name.into(),
            workspace_num: None,
            workspace_dir: None,
            workflow: Some("ace(run)".into()),
            raw_suffix: Some("20260501090800".into()),
            pid: None,
            model: None,
            llm_provider: None,
            vcs_provider: None,
            agent_name: None,
            approve: false,
            hidden: false,
            bug: None,
            cl_num: None,
        }
    }
}

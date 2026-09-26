use crate::agent_runtime::{
    is_real_gate_member_record, parse_runtime_timestamp,
};
use crate::agent_scan::context::clan_key_from_meta;
use crate::agent_scan::wire::{
    AgentArtifactRecordWire, AgentMetaWire, DoneMarkerWire,
};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

pub(super) const MARKER_FILES: &[&str] = &[
    "agent_meta.json",
    "done.json",
    "running.json",
    "waiting.json",
    "pending_question.json",
    "workflow_state.json",
    "plan_path.json",
    "xprompts.json",
];

#[derive(Default)]
pub(super) struct RecordSummary {
    pub(super) status: String,
    pub(super) agent_type: String,
    pub(super) cl_name: Option<String>,
    pub(super) agent_name: Option<String>,
    pub(super) workflow_name: Option<String>,
    pub(super) agent_clan: Option<String>,
    pub(super) agent_clan_generation: Option<String>,
    pub(super) clan_tribe: Option<String>,
    pub(super) clan_summary: Option<String>,
    pub(super) agent_session: Option<String>,
    pub(super) model: Option<String>,
    pub(super) llm_provider: Option<String>,
    pub(super) started_at: Option<String>,
    pub(super) finished_at: Option<f64>,
    pub(super) workflow_status: Option<String>,
    pub(super) hidden: bool,
    pub(super) parent_timestamp: Option<String>,
    pub(super) step_index: Option<i64>,
    pub(super) step_name: Option<String>,
    pub(super) retry_of_timestamp: Option<String>,
    pub(super) retried_as_timestamp: Option<String>,
    pub(super) retry_chain_root_timestamp: Option<String>,
    pub(super) retry_attempt: Option<i64>,
    pub(super) model_alias_origin: Option<String>,
    pub(super) source_machine: Option<String>,
    pub(super) imported_owner_machine: Option<String>,
    pub(super) gate_turn_id: Option<String>,
}

impl RecordSummary {
    pub(super) fn from_record(record: &AgentArtifactRecordWire) -> Self {
        let meta = record.agent_meta.as_ref();
        let done = record.done.as_ref();
        let running = record.running.as_ref();
        let waiting = record.waiting.as_ref();
        let workflow_state = record.workflow_state.as_ref();
        let first_step = record.prompt_steps.first();

        let workflow_status = workflow_state.map(|w| w.status.clone());
        let status = if waiting.is_some() {
            "waiting"
        } else if let Some(workflow_status) = workflow_status.as_deref() {
            workflow_status
        } else if record.has_done_marker {
            "done"
        } else if meta
            .and_then(|m| {
                m.run_started_at.as_ref().or(m.wait_completed_at.as_ref())
            })
            .is_some()
        {
            "running"
        } else {
            "starting"
        }
        .to_string();

        let clan_key = meta.and_then(clan_key_from_meta);
        let machines = machine_projection_from_record(record);
        Self {
            status,
            agent_type: if workflow_state.is_some() {
                "workflow".to_string()
            } else {
                "agent".to_string()
            },
            cl_name: done
                .and_then(|d| d.cl_name.clone())
                .or_else(|| running.and_then(|r| r.cl_name.clone()))
                .or_else(|| workflow_state.and_then(|w| w.cl_name.clone()))
                .or_else(|| meta.and_then(|m| m.cl_name.clone())),
            agent_name: meta
                .and_then(|m| m.name.clone())
                .or_else(|| done.and_then(|d| d.name.clone()))
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            workflow_name: meta
                .and_then(|m| m.workflow_name.clone())
                .or_else(|| workflow_state.map(|w| w.workflow_name.clone())),
            agent_clan: clan_key.as_ref().map(|(clan, _)| clan.clone()),
            agent_clan_generation: clan_key
                .as_ref()
                .and_then(|(_, generation)| generation.clone()),
            clan_tribe: meta.and_then(|m| m.clan_tribe.clone()),
            clan_summary: meta.and_then(|m| m.clan_summary.clone()),
            agent_session: meta.and_then(|m| m.agent_session.clone()),
            model: meta
                .and_then(|m| m.model.clone())
                .or_else(|| done.and_then(|d| d.model.clone()))
                .or_else(|| running.and_then(|r| r.model.clone()))
                .or_else(|| first_step.and_then(|s| s.model.clone())),
            llm_provider: meta
                .and_then(|m| m.llm_provider.clone())
                .or_else(|| done.and_then(|d| d.llm_provider.clone()))
                .or_else(|| running.and_then(|r| r.llm_provider.clone()))
                .or_else(|| first_step.and_then(|s| s.llm_provider.clone())),
            started_at: meta
                .and_then(|m| m.run_started_at.clone())
                .or_else(|| workflow_state.and_then(|w| w.start_time.clone())),
            // A done marker that omits finished_at sorts behind every
            // stamped row in the recent-completed window. Fall back to
            // agent_meta.stopped_at so settled monitors stay visible.
            finished_at: summary_finished_at(done, meta),
            workflow_status,
            hidden: meta.map(|m| m.hidden).unwrap_or(false)
                || done.map(|d| d.hidden).unwrap_or(false)
                || workflow_state.map(|w| w.hidden).unwrap_or(false),
            parent_timestamp: meta.and_then(|m| m.parent_timestamp.clone()),
            step_index: first_step.and_then(|s| s.step_index),
            step_name: first_step.map(|s| s.step_name.clone()),
            retry_of_timestamp: meta.and_then(|m| m.retry_of_timestamp.clone()),
            retried_as_timestamp: meta
                .and_then(|m| m.retried_as_timestamp.clone())
                .or_else(|| done.and_then(|d| d.retried_as_timestamp.clone())),
            retry_chain_root_timestamp: meta
                .and_then(|m| m.retry_chain_root_timestamp.clone())
                .or_else(|| {
                    done.and_then(|d| d.retry_chain_root_timestamp.clone())
                }),
            retry_attempt: meta.and_then(|m| m.retry_attempt),
            model_alias_origin: meta.and_then(|m| m.model_alias_origin.clone()),
            source_machine: machines.source_machine,
            imported_owner_machine: machines.imported_owner_machine,
            gate_turn_id: gate_turn_id_from_record(record),
        }
    }
}

/// Return the durable gate id iff *record* is a real gate-turn member.
///
/// `gate_id` alone is inherited by later gate-associated follow-ups, so
/// indexing it unconditionally would let a successor shadow the shell that
/// actually owns the gate. Only [`is_real_gate_member_record`] rows project
/// a value here, which is what makes an exact `gate_turn_id` match resolve
/// the owning shell instead of an inheritor.
pub(super) fn gate_turn_id_from_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    if !is_real_gate_member_record(record) {
        return None;
    }
    record
        .agent_meta
        .as_ref()
        .and_then(|meta| meta.agent_session_turn.as_ref())
        .and_then(|shell| shell.id.clone())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct MachineProjection {
    pub(super) source_machine: Option<String>,
    pub(super) imported_owner_machine: Option<String>,
}

pub(super) fn machine_projection_from_record(
    record: &AgentArtifactRecordWire,
) -> MachineProjection {
    let meta = record.agent_meta.as_ref();
    let done = record.done.as_ref();
    machine_projection_from_parts(
        meta.and_then(|marker| marker.source_machine.as_deref()),
        meta.and_then(|marker| {
            marker
                .imported_source_owner
                .as_ref()
                .map(|owner| owner.machine_name.as_str())
        }),
        done.and_then(|marker| marker.source_machine.as_deref()),
        done.and_then(|marker| {
            marker
                .imported_source_owner
                .as_ref()
                .map(|owner| owner.machine_name.as_str())
        }),
    )
}

pub(super) fn source_machine_from_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    machine_projection_from_record(record).source_machine
}

pub(super) fn source_machine_from_marker_files(
    artifact_dir: &Path,
) -> Option<String> {
    machine_projection_from_marker_files(artifact_dir).source_machine
}

pub(super) fn machine_projection_from_marker_files(
    artifact_dir: &Path,
) -> MachineProjection {
    let meta = read_marker_json(artifact_dir, "agent_meta.json");
    let done = read_marker_json(artifact_dir, "done.json");
    machine_projection_from_parts(
        json_machine_field(meta.as_ref(), "source_machine"),
        json_owner_machine(meta.as_ref()),
        json_machine_field(done.as_ref(), "source_machine"),
        json_owner_machine(done.as_ref()),
    )
}

pub(super) fn read_marker_json(
    artifact_dir: &Path,
    name: &str,
) -> Option<serde_json::Value> {
    fs::read_to_string(artifact_dir.join(name))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
}

pub(super) fn json_machine_field<'a>(
    value: Option<&'a serde_json::Value>,
    key: &str,
) -> Option<&'a str> {
    value
        .and_then(|payload| payload.get(key))
        .and_then(|value| value.as_str())
}

pub(super) fn json_owner_machine(
    value: Option<&serde_json::Value>,
) -> Option<&str> {
    value
        .and_then(|payload| payload.get("imported_source_owner"))
        .and_then(|owner| owner.get("machine_name"))
        .and_then(|value| value.as_str())
}

pub(super) fn machine_projection_from_parts(
    meta_source: Option<&str>,
    meta_owner: Option<&str>,
    done_source: Option<&str>,
    done_owner: Option<&str>,
) -> MachineProjection {
    let meta_owner = trim_machine(meta_owner);
    let done_owner = trim_machine(done_owner);
    MachineProjection {
        // Match Python loader precedence: meta source, then the already-applied
        // meta owner fallback, then done source, then done owner.
        source_machine: trim_machine(meta_source)
            .or_else(|| meta_owner.clone())
            .or_else(|| trim_machine(done_source))
            .or_else(|| done_owner.clone()),
        imported_owner_machine: meta_owner.or(done_owner),
    }
}

pub(super) fn trim_machine(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn summary_finished_at(
    done: Option<&DoneMarkerWire>,
    meta: Option<&AgentMetaWire>,
) -> Option<f64> {
    let marker = done?;
    if let Some(finished_at) = marker.finished_at {
        return Some(finished_at);
    }
    meta.and_then(|value| value.stopped_at.as_deref())
        .and_then(parse_runtime_timestamp)
}

#[derive(Default, PartialEq, Eq)]
pub(super) struct MarkerSignatures {
    pub(super) agent_meta: Option<String>,
    pub(super) done: Option<String>,
    pub(super) running: Option<String>,
    pub(super) waiting: Option<String>,
    pub(super) pending_question: Option<String>,
    pub(super) workflow_state: Option<String>,
    pub(super) plan_path: Option<String>,
    pub(super) prompt_steps: Option<String>,
    pub(super) xprompts: Option<String>,
}

impl MarkerSignatures {
    pub(super) fn from_artifact_dir(artifact_dir: &str) -> Self {
        let dir = PathBuf::from(artifact_dir);
        let mut sigs = Self {
            agent_meta: marker_signature(&dir.join("agent_meta.json")),
            done: marker_signature(&dir.join("done.json")),
            running: marker_signature(&dir.join("running.json")),
            waiting: marker_signature(&dir.join("waiting.json")),
            pending_question: marker_signature(
                &dir.join("pending_question.json"),
            ),
            workflow_state: marker_signature(&dir.join("workflow_state.json")),
            plan_path: marker_signature(&dir.join("plan_path.json")),
            prompt_steps: None,
            xprompts: marker_signature(&dir.join("xprompts.json")),
        };

        let mut step_sigs: Vec<String> = Vec::new();
        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str())
                else {
                    continue;
                };
                if name.starts_with("prompt_step_")
                    && name.ends_with(".json")
                    && path.is_file()
                {
                    if let Some(sig) = marker_signature(&path) {
                        step_sigs.push(format!("{name}:{sig}"));
                    }
                }
            }
        }
        step_sigs.sort();
        if !step_sigs.is_empty() {
            sigs.prompt_steps = Some(step_sigs.join("|"));
        }
        sigs
    }
}

pub(super) fn marker_signature(path: &Path) -> Option<String> {
    if !MARKER_FILES
        .iter()
        .any(|name| path.file_name().and_then(|n| n.to_str()) == Some(*name))
        && !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with("prompt_step_"))
    {
        return None;
    }
    let meta = fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    let duration = modified.duration_since(UNIX_EPOCH).ok()?;
    Some(format!(
        "{}:{}:{}",
        meta.len(),
        duration.as_secs(),
        duration.subsec_nanos()
    ))
}

//! Shared agent session-root versus concrete-shell classification.
//!
//! Modern owner records often carry `agent_session_id` / `agent_session_shell` / a
//! plan-chain name suffix without `parent_timestamp`. A concrete `--plan`
//! gate with `agent_session_id` and a null parent is a nested shell, never an
//! agent session root. Gateway presentation, catalog projection, and owner listing
//! must share this classifier.

use crate::agent_scan::{
    AgentArtifactRecordWire, AgentMetaWire, AgentSessionShellWire,
    DoneMarkerWire,
};

/// Kind of a concrete agent session shell, independent of whether a parent
/// timestamp is recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcreteAgentSessionShellKind {
    Plan,
    Code,
    Monitor,
    Gate,
    Proc,
    Member,
}

impl ConcreteAgentSessionShellKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plan => "plan",
            Self::Code => "code",
            Self::Monitor => "monitor",
            Self::Gate => "gate",
            Self::Proc => "proc",
            Self::Member => "member",
        }
    }
}

/// Return the tracked parent timestamp, if any.
pub fn tracked_parent_timestamp(
    record: &AgentArtifactRecordWire,
) -> Option<&str> {
    record.agent_meta.as_ref().and_then(|value| {
        first_non_empty([
            value.parent_timestamp.as_deref(),
            value.parent_agent_timestamp.as_deref(),
        ])
    })
}

/// Agent session id used to group a root with its shells.
pub fn agent_session_id_for_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    let meta = record.agent_meta.as_ref();
    first_non_empty([
        meta.and_then(|value| value.agent_session.as_deref()),
        agent_session_shell(meta, record.done.as_ref())
            .and_then(|value| value.label.as_deref()),
    ])
    .map(str::to_string)
}

/// Stable grouping key for presentation "currently presented agent session".
pub fn agent_session_key_for_record(
    record: &AgentArtifactRecordWire,
) -> Option<String> {
    if let Some(agent_session_id) = agent_session_id_for_record(record) {
        return Some(agent_session_id);
    }
    if let Some(parent) = tracked_parent_timestamp(record) {
        return Some(parent.to_string());
    }
    let name = record_name(record)?;
    if let Some(base) = agent_session_base_from_name(name) {
        return Some(base.to_string());
    }
    None
}

/// Whether this record is a concrete plan/code/monitor/gate/proc/member
/// shell rather than an agent session root or standalone agent.
pub fn record_is_concrete_agent_session_shell(
    record: &AgentArtifactRecordWire,
) -> bool {
    concrete_agent_session_shell_kind(record).is_some()
}

/// Classify a record as a concrete agent session shell from modern facts, in
/// this order: `agent_session_shell.kind`, `agent_session_role` / `role_suffix`,
/// plan-chain name suffix, then `parent_timestamp`. `agent_session_id` alone
/// does not make a root into a shell.
pub fn concrete_agent_session_shell_kind(
    record: &AgentArtifactRecordWire,
) -> Option<ConcreteAgentSessionShellKind> {
    let meta = record.agent_meta.as_ref();
    if meta.and_then(|value| value.proc_id.as_deref()).is_some() {
        return Some(ConcreteAgentSessionShellKind::Proc);
    }
    if let Some(kind) = agent_session_shell(meta, record.done.as_ref())
        .and_then(kind_from_agent_session_shell)
    {
        return Some(kind);
    }
    if let Some(kind) = kind_from_role(meta.and_then(|value| {
        first_non_empty([
            value.agent_session_role.as_deref(),
            value.role_suffix.as_deref(),
        ])
    })) {
        return Some(kind);
    }
    if let Some(kind) = record_name(record).and_then(kind_from_name) {
        return Some(kind);
    }
    if tracked_parent_timestamp(record).is_some() {
        return Some(ConcreteAgentSessionShellKind::Member);
    }
    None
}

pub fn agent_session_shell<'a>(
    meta: Option<&'a AgentMetaWire>,
    done: Option<&'a DoneMarkerWire>,
) -> Option<&'a AgentSessionShellWire> {
    meta.and_then(|value| value.agent_session_shell.as_ref())
        .or_else(|| done.and_then(|value| value.agent_session_shell.as_ref()))
}

fn kind_from_agent_session_shell(
    shell: &AgentSessionShellWire,
) -> Option<ConcreteAgentSessionShellKind> {
    match shell.kind.trim().to_ascii_lowercase().as_str() {
        "monitor" | "mon" => Some(ConcreteAgentSessionShellKind::Monitor),
        "gate" => Some(ConcreteAgentSessionShellKind::Gate),
        "proc" => Some(ConcreteAgentSessionShellKind::Proc),
        "plan" => Some(ConcreteAgentSessionShellKind::Plan),
        "code" => Some(ConcreteAgentSessionShellKind::Code),
        _ => None,
    }
}

fn kind_from_role(raw: Option<&str>) -> Option<ConcreteAgentSessionShellKind> {
    let value = raw?.trim().trim_start_matches('-').to_ascii_lowercase();
    match value.as_str() {
        "plan" => Some(ConcreteAgentSessionShellKind::Plan),
        "code" => Some(ConcreteAgentSessionShellKind::Code),
        "monitor" | "mon" => Some(ConcreteAgentSessionShellKind::Monitor),
        "gate" => Some(ConcreteAgentSessionShellKind::Gate),
        "proc" => Some(ConcreteAgentSessionShellKind::Proc),
        "member" => Some(ConcreteAgentSessionShellKind::Member),
        "root" | "epic" | "commit" | "feedback" => None,
        _ => None,
    }
}

fn kind_from_name(name: &str) -> Option<ConcreteAgentSessionShellKind> {
    let lower = name.to_ascii_lowercase();
    for (suffix, kind) in [
        ("--plan", ConcreteAgentSessionShellKind::Plan),
        ("--code", ConcreteAgentSessionShellKind::Code),
        ("--gate", ConcreteAgentSessionShellKind::Gate),
        ("--mon", ConcreteAgentSessionShellKind::Monitor),
        ("--proc", ConcreteAgentSessionShellKind::Proc),
    ] {
        if name_has_plan_chain_suffix(&lower, suffix) {
            return Some(kind);
        }
    }
    None
}

fn name_has_plan_chain_suffix(lower_name: &str, suffix: &str) -> bool {
    let Some(index) = lower_name.rfind(suffix) else {
        return false;
    };
    let rest = &lower_name[index + suffix.len()..];
    rest.is_empty()
        || rest
            .chars()
            .all(|character| character == '-' || character.is_ascii_digit())
}

fn agent_session_base_from_name(name: &str) -> Option<&str> {
    let lower = name.to_ascii_lowercase();
    for suffix in ["--plan", "--code", "--gate", "--mon", "--proc"] {
        if let Some(index) = lower.rfind(suffix) {
            if name_has_plan_chain_suffix(&lower, suffix) && index > 0 {
                return Some(&name[..index]);
            }
        }
    }
    None
}

fn record_name(record: &AgentArtifactRecordWire) -> Option<&str> {
    first_non_empty([
        record
            .agent_meta
            .as_ref()
            .and_then(|value| value.name.as_deref()),
        record.done.as_ref().and_then(|value| value.name.as_deref()),
    ])
}

fn first_non_empty<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_scan::{
        AgentArtifactRecordShapeWire, AgentArtifactRecordWire, AgentMetaWire,
        AgentSessionShellWire,
    };

    fn record_named(name: &str) -> AgentArtifactRecordWire {
        AgentArtifactRecordWire {
            project_name: "proj".to_string(),
            project_dir: "/tmp/proj".to_string(),
            project_file: "/tmp/proj.sase".to_string(),
            workflow_dir_name: "ace-run".to_string(),
            artifact_dir: format!("/tmp/artifacts/{name}"),
            timestamp: "20260919120000".to_string(),
            agent_meta: Some(AgentMetaWire {
                name: Some(name.to_string()),
                ..AgentMetaWire::default()
            }),
            done: None,
            running: None,
            waiting: None,
            pending_question: None,
            workflow_state: None,
            plan_path: None,
            prompt_steps: Vec::new(),
            raw_prompt_snippet: None,
            used_xprompts: Vec::new(),
            has_done_marker: false,
            record_shape: AgentArtifactRecordShapeWire::Full,
        }
    }

    #[test]
    fn plan_without_parent_timestamp_is_a_shell_not_a_root() {
        let mut record = record_named("0n--plan");
        record.agent_meta.as_mut().unwrap().agent_session =
            Some("lane".to_string());
        record.agent_meta.as_mut().unwrap().agent_session_shell =
            Some(AgentSessionShellWire {
                kind: "gate".to_string(),
                ..AgentSessionShellWire::default()
            });
        assert_eq!(
            concrete_agent_session_shell_kind(&record),
            Some(ConcreteAgentSessionShellKind::Gate)
        );
        assert!(tracked_parent_timestamp(&record).is_none());
        assert_eq!(
            agent_session_key_for_record(&record).as_deref(),
            Some("lane")
        );
    }

    #[test]
    fn agent_session_id_alone_does_not_make_a_root_a_shell() {
        let mut record = record_named("lane");
        record.agent_meta.as_mut().unwrap().agent_session =
            Some("lane".to_string());
        record.agent_meta.as_mut().unwrap().agent_session_role =
            Some("root".to_string());
        assert_eq!(concrete_agent_session_shell_kind(&record), None);
        assert_eq!(
            agent_session_key_for_record(&record).as_deref(),
            Some("lane")
        );
    }

    #[test]
    fn name_suffix_classifies_code_and_monitor_shells() {
        assert_eq!(
            concrete_agent_session_shell_kind(&record_named("0k--code")),
            Some(ConcreteAgentSessionShellKind::Code)
        );
        assert_eq!(
            concrete_agent_session_shell_kind(&record_named("lane--mon")),
            Some(ConcreteAgentSessionShellKind::Monitor)
        );
        assert_eq!(
            concrete_agent_session_shell_kind(&record_named("lane--gate-0")),
            Some(ConcreteAgentSessionShellKind::Gate)
        );
        assert_eq!(
            concrete_agent_session_shell_kind(&record_named("lane--proc")),
            Some(ConcreteAgentSessionShellKind::Proc)
        );
    }

    #[test]
    fn parent_timestamp_classifies_an_otherwise_plain_member() {
        let mut record = record_named("worker");
        record.agent_meta.as_mut().unwrap().parent_timestamp =
            Some("20260919100000".to_string());
        assert_eq!(
            concrete_agent_session_shell_kind(&record),
            Some(ConcreteAgentSessionShellKind::Member)
        );
    }
}

use std::path::Path;

use crate::agent_hold::{
    validate_and_normalize_armer, AgentHoldArmerKindWire, AgentHoldArmerWire,
    AgentHoldError,
};

use super::wires::{
    AgentUnitWire, LaunchUnitPayloadWire, LaunchUnitWire, ProcUnitWire,
};

pub fn launch_unit_hold_key(
    request_id: &str,
    logical_id: &str,
) -> Result<String, AgentHoldError> {
    validate_key_component("request_id", request_id)?;
    validate_key_component("logical_id", logical_id)?;
    Ok(format!("launch:{request_id}/{logical_id}"))
}

pub fn launch_unit_hold_armer(
    unit: &LaunchUnitWire,
    request_id: &str,
    project: &str,
    pid: u32,
    done_marker_path: &Path,
) -> Result<AgentHoldArmerWire, AgentHoldError> {
    let key = launch_unit_hold_key(request_id, &unit.logical_id)?;
    let request_prefix: String = request_id.chars().take(8).collect();
    let (label, agent_name, agent_session, clan) = match &unit.payload {
        LaunchUnitPayloadWire::Agent(agent) => {
            let identity = agent.effective_identity();
            let armer_identity = agent_armer_identity(agent, identity.clone());
            (
                identity.unwrap_or_else(|| unit.logical_id.clone()),
                armer_identity,
                agent.agent_session_attach_parent.clone(),
                agent.clan.clone(),
            )
        }
        LaunchUnitPayloadWire::Proc(proc_unit) => {
            (proc_label(proc_unit, &unit.logical_id), None, None, None)
        }
    };
    let mut armer = AgentHoldArmerWire {
        kind: AgentHoldArmerKindWire::Launch,
        key,
        display: format!("{label} (launch {request_prefix})"),
        project: project.to_string(),
        agent_name,
        agent_session,
        clan,
        proc_id: None,
        pid: Some(pid),
        done_marker_path: Some(done_marker_path.to_string_lossy().to_string()),
    };
    validate_and_normalize_armer(&mut armer)?;
    Ok(armer)
}

fn validate_key_component(
    label: &str,
    value: &str,
) -> Result<(), AgentHoldError> {
    if value.is_empty() {
        return Err(AgentHoldError::Validation(format!(
            "{label} must be non-empty"
        )));
    }
    if value
        .chars()
        .any(|ch| ch.is_whitespace() || ch.is_control())
    {
        return Err(AgentHoldError::Validation(format!(
            "{label} must not contain whitespace or control characters"
        )));
    }
    Ok(())
}

fn agent_armer_identity(
    agent: &AgentUnitWire,
    identity: Option<String>,
) -> Option<String> {
    if agent.identity_explicit {
        return identity;
    }
    match agent.agent_session_attach_suffix.as_deref() {
        Some("@") | None => None,
        Some(_) => identity,
    }
}

fn proc_label(proc_unit: &ProcUnitWire, logical_id: &str) -> String {
    proc_unit
        .shell_name
        .clone()
        .unwrap_or_else(|| logical_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_launch::{
        AgentUnitWire, LaunchUnitPayloadWire, ProcUnitWire,
    };
    use crate::fenced_code::CodeValueWire;

    fn agent_unit(agent: AgentUnitWire) -> LaunchUnitWire {
        LaunchUnitWire {
            logical_id: "unit-1".to_string(),
            source_order: 1,
            waits: Vec::new(),
            condition: None,
            payload: LaunchUnitPayloadWire::Agent(agent),
        }
    }

    fn proc_unit(proc_unit: ProcUnitWire) -> LaunchUnitWire {
        LaunchUnitWire {
            logical_id: "proc-1".to_string(),
            source_order: 1,
            waits: Vec::new(),
            condition: None,
            payload: LaunchUnitPayloadWire::Proc(proc_unit),
        }
    }

    fn proc_payload(
        shell_name: Option<&str>,
        label: Option<&str>,
    ) -> ProcUnitWire {
        ProcUnitWire {
            code: CodeValueWire {
                schema_version:
                    crate::fenced_code::CODE_VALUE_WIRE_SCHEMA_VERSION,
                source: "just check".to_string(),
                language: "bash".to_string(),
                info_string: None,
                digest: "digest".to_string(),
                preview: "just check".to_string(),
            },
            shell_name: shell_name.map(str::to_string),
            label: label.map(str::to_string),
            timeout: None,
            idle_timeout: None,
            cwd: None,
            workspace: true,
            workspace_explicit: false,
            selected_project: None,
            queue_capacity: None,
            queue_capacity_multiplier: None,
            wait_priority: None,
            queue_weight: None,
            queue_weight_explicit: false,
            hold: None,
        }
    }

    #[test]
    fn launch_unit_hold_key_validates_components() {
        assert_eq!(
            launch_unit_hold_key("req-123456789", "unit-1").unwrap(),
            "launch:req-123456789/unit-1"
        );
        assert!(launch_unit_hold_key("", "unit-1")
            .unwrap_err()
            .to_string()
            .contains("request_id"));
        assert!(launch_unit_hold_key("req 1", "unit-1").is_err());
        assert!(launch_unit_hold_key("req", "unit\n1").is_err());
    }

    #[test]
    fn launch_unit_hold_armer_builds_agent_identity_shapes() {
        let explicit = launch_unit_hold_armer(
            &agent_unit(AgentUnitWire {
                prompt: "Review".to_string(),
                identity: Some("reviewer".to_string()),
                identity_explicit: true,
                clan: Some("guild".to_string()),
                ..Default::default()
            }),
            "request123",
            "sase",
            42,
            Path::new("/tmp/receipt.json"),
        )
        .unwrap();
        assert_eq!(explicit.kind, AgentHoldArmerKindWire::Launch);
        assert_eq!(explicit.key, "launch:request123/unit-1");
        assert_eq!(explicit.display, "guild.reviewer (launch request1)");
        assert_eq!(explicit.agent_name.as_deref(), Some("guild.reviewer"));
        assert_eq!(explicit.agent_session, None);
        assert_eq!(explicit.clan.as_deref(), Some("guild"));

        let agent_session = launch_unit_hold_armer(
            &agent_unit(AgentUnitWire {
                prompt: "Review".to_string(),
                agent_session_attach_parent: Some("parent".to_string()),
                agent_session_attach_suffix: Some("child".to_string()),
                ..Default::default()
            }),
            "request123",
            "sase",
            42,
            Path::new("/tmp/receipt.json"),
        )
        .unwrap();
        assert_eq!(agent_session.display, "parent--child (launch request1)");
        assert_eq!(agent_session.agent_name.as_deref(), Some("parent--child"));
        assert_eq!(agent_session.agent_session.as_deref(), Some("parent"));

        let auto = launch_unit_hold_armer(
            &agent_unit(AgentUnitWire {
                prompt: "Review".to_string(),
                tribe: Some("review".to_string()),
                ..Default::default()
            }),
            "request123",
            "sase",
            42,
            Path::new("/tmp/receipt.json"),
        )
        .unwrap();
        assert_eq!(auto.display, "unit-1 (launch request1)");
        assert_eq!(auto.agent_name, None);
        assert_eq!(auto.agent_session, None);
    }

    #[test]
    fn launch_unit_hold_armer_builds_proc_without_identity_fields() {
        let armer = launch_unit_hold_armer(
            &proc_unit(proc_payload(Some("build.check"), Some("Build"))),
            "request123",
            "sase",
            42,
            Path::new("/tmp/receipt.json"),
        )
        .unwrap();
        assert_eq!(armer.display, "build.check (launch request1)");
        assert_eq!(armer.agent_name, None);
        assert_eq!(armer.agent_session, None);
        assert_eq!(armer.clan, None);
        assert_eq!(armer.proc_id, None);
        assert_eq!(armer.pid, Some(42));
        assert_eq!(
            armer.done_marker_path.as_deref(),
            Some("/tmp/receipt.json")
        );

        let fallback = launch_unit_hold_armer(
            &proc_unit(proc_payload(None, Some("Build"))),
            "request123",
            "sase",
            42,
            Path::new("/tmp/receipt.json"),
        )
        .unwrap();
        assert_eq!(fallback.display, "proc-1 (launch request1)");
    }
}

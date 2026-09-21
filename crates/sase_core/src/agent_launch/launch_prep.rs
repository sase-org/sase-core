//! Launch materialization: shape a launch request into process data and
//! spill the prompt to a private temp file.
use super::wires::{
    AgentLaunchPreparationError, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, WorkspaceClaimRequestWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

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

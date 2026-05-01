//! Skeleton wire records for the future Rust-backed agent launch planner.
//!
//! Phase 1 only pins serde-compatible shapes. Production launch behavior still
//! lives in Python until later phases port deterministic pieces behind these
//! records.

use serde::{Deserialize, Serialize};

pub const AGENT_LAUNCH_WIRE_SCHEMA_VERSION: u32 = 1;

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
    pub extra_env: std::collections::BTreeMap<String, String>,
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
    pub env_delta: std::collections::BTreeMap<String, String>,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;

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
}

//! Wire round-trips and timestamp-batch allocation (`wires.rs`).
use std::collections::BTreeMap;

use serde_json::json;

use crate::agent_launch::{
    allocate_launch_timestamp_batch, AgentLaunchPreparedWire,
    AgentLaunchRequestWire, LaunchFanoutPlanWire, LaunchFanoutSlotWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
};

#[test]
fn launch_request_round_trips_json_shape() {
    let mut extra_env = BTreeMap::new();
    extra_env.insert("SASE_REPEAT_NAME".to_string(), "task.1".to_string());
    let request = AgentLaunchRequestWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        cl_name: "feature/test".to_string(),
        project_file: "/tmp/project.sase".to_string(),
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
    let back: AgentLaunchRequestWire = serde_json::from_value(value).unwrap();
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
    let back: AgentLaunchPreparedWire = serde_json::from_value(value).unwrap();
    assert_eq!(back, prepared);
}

#[test]
fn fanout_plan_round_trips_slots() {
    let plan = LaunchFanoutPlanWire {
        schema_version: AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
        launch_kind: "repeat".to_string(),
        slots: vec![LaunchFanoutSlotWire {
            prompt: "%i:task.1\nfix it".to_string(),
            launch_kind: "repeat".to_string(),
            slot_index: 0,
            alt_id: None,
            timestamp: None,
            workflow_name: None,
            model: None,
            repeat_name: Some("task.1".to_string()),
            bead_id: Some("sase-8f.2".to_string()),
            wait_for_previous: false,
        }],
        requires_sequential_naming_wait: false,
        fanout_sleep_seconds: 1.0,
    };
    let value = serde_json::to_value(&plan).unwrap();
    assert_eq!(value["slots"][0]["repeat_name"], json!("task.1"));
    assert_eq!(value["slots"][0]["bead_id"], json!("sase-8f.2"));
    assert_eq!(value["slots"][0]["alt_id"], json!(null));
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

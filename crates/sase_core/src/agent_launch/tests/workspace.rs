//! Launch preparation and workspace-claim content (`launch_prep.rs`,
//! `workspace_claims.rs`).
use std::collections::BTreeMap;

use crate::agent_launch::{
    allocate_and_claim_workspace_from_content,
    decide_workspace_occupant_conflict, list_workspace_claims_from_content,
    plan_claim_workspace_from_content,
    plan_transfer_workspace_claim_from_content, prepare_agent_launch,
    AgentLaunchRequestWire, OccupancyCallerWire, OccupantRecordWire,
    WorkspaceClaimRequestWire, WorkspaceClaimWire,
    AGENT_LAUNCH_WIRE_SCHEMA_VERSION,
};

fn request(workspace_num: u32) -> WorkspaceClaimRequestWire {
    WorkspaceClaimRequestWire {
        project_file: "/tmp/project.sase".to_string(),
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
        project_file: "/tmp/project.sase".to_string(),
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
        project_file: "/tmp/home.sase".to_string(),
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
fn workspace_claims_keep_suffix_corrupt_rows_occupied() {
    let content = "RUNNING:\n  #10 | 111 | run | demo | 20260820_121314 | LEGACY=bad | PINNED\n\n\nNAME: demo\n";

    let claims = list_workspace_claims_from_content(content);
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].workspace_num, 10);
    assert_eq!(
        claims[0].artifacts_timestamp.as_deref(),
        Some("20260820_121314")
    );
    assert!(claims[0].pinned);

    let duplicate = plan_claim_workspace_from_content(content, &request(10));
    assert!(!duplicate.outcome.success);
    assert!(!duplicate.changed);

    let allocated =
        allocate_and_claim_workspace_from_content(content, 10, 11, &request(0));
    assert!(allocated.outcome.success);
    assert_eq!(allocated.outcome.workspace_num, 11);
    assert!(allocated.content.contains("#11 | 222 | run | demo"));
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

#[test]
fn transfer_workspace_claim_updates_claim_name() {
    let content = "RUNNING:\n  #101 | 111 | git-main | old | 20260501115959\n\n\nNAME: demo\n";
    let mut req = request(101);
    req.workflow_name = "ace-runner".to_string();
    req.cl_name = "feature".to_string();
    req.artifacts_timestamp = "20260501120000".to_string();
    req.transfer_from_pid = Some(111);

    let plan = plan_transfer_workspace_claim_from_content(content, &req);

    assert!(plan.outcome.success);
    assert!(plan
        .content
        .contains("#101 | 222 | ace-runner | feature | 20260501120000"));
}

#[test]
fn transfer_numbered_workspace_matches_pid_when_claim_name_changes() {
    let content = "RUNNING:\n  #101 | 111 | git-main |  | 20260501115959\n\n\nNAME: demo\n";
    let mut req = request(101);
    req.workflow_name = "ace-runner".to_string();
    req.cl_name = "feature".to_string();
    req.artifacts_timestamp = "20260501120000".to_string();
    req.transfer_from_pid = Some(111);

    let plan = plan_transfer_workspace_claim_from_content(content, &req);

    assert!(plan.outcome.success);
    assert!(plan
        .content
        .contains("#101 | 222 | ace-runner | feature | 20260501120000"));
}

#[test]
fn transfer_placeholder_workspace_still_matches_claim_name() {
    let content = "RUNNING:\n  #0 | 111 | ace-runner | other | 20260501115959\n  #0 | 112 | ace-runner | feature | 20260501115959\n\nNAME: demo\n";
    let mut req = request(0);
    req.workflow_name = "ace-runner-retry".to_string();
    req.cl_name = "feature".to_string();
    req.artifacts_timestamp = "20260501120000".to_string();
    req.transfer_from_pid = Some(111);

    let plan = plan_transfer_workspace_claim_from_content(content, &req);

    assert!(!plan.outcome.success);
    assert!(plan.content.contains("#0 | 111 | ace-runner | other"));
    assert!(plan.content.contains("#0 | 112 | ace-runner | feature"));
}

#[test]
fn transfer_workspace_claim_preserves_unknown_suffix_fields() {
    let content = "RUNNING:\n  #101 | 111 | run | demo | 20260820_121314 | LEGACY=bad | PINNED | extra\n\n\nNAME: demo\n";
    let mut req = request(101);
    req.workflow_name = "run-retry".to_string();
    req.artifacts_timestamp = "20260820121516".to_string();
    req.transfer_from_pid = Some(111);

    let plan = plan_transfer_workspace_claim_from_content(content, &req);

    assert!(plan.outcome.success);
    assert!(plan.content.contains(
            "#101 | 222 | run-retry | demo | 20260820121516 | LEGACY=bad | PINNED | extra"
        ));
}

fn occupancy_caller(pid: u32) -> OccupancyCallerWire {
    OccupancyCallerWire {
        pid,
        workspace_num: 17,
        project: "sase".to_string(),
        workflow: "ace(run)-260818_120000".to_string(),
        artifacts_timestamp: Some("20260818T120000".to_string()),
    }
}

fn occupant(pid: u32) -> OccupantRecordWire {
    OccupantRecordWire {
        pid,
        artifacts_timestamp: Some("20260818T115900".to_string()),
        agent_name: Some("06e--plan".to_string()),
        workflow: "ace(run)-260818_115900".to_string(),
        project: "sase".to_string(),
        workspace_num: 17,
        cl_name: Some("demo".to_string()),
        claimed_at: 1_755_000_000.0,
    }
}

fn claim(pid: u32) -> WorkspaceClaimWire {
    WorkspaceClaimWire {
        workspace_num: 17,
        workflow: "ace(run)-260818_115900".to_string(),
        cl_name: Some("demo".to_string()),
        pid,
        artifacts_timestamp: Some("20260818T115900".to_string()),
        pinned: false,
    }
}

#[test]
fn occupancy_proceeds_when_no_occupant_record() {
    let decision = decide_workspace_occupant_conflict(
        None,
        &occupancy_caller(500),
        false,
        Some(&claim(999)),
        true,
    );
    assert!(decision.may_proceed);
    assert!(!decision.conflict);
}

#[test]
fn occupancy_proceeds_when_occupant_is_caller() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(500)),
        &occupancy_caller(500),
        true,
        Some(&claim(500)),
        true,
    );
    assert!(decision.may_proceed);
    assert!(!decision.conflict);
}

#[test]
fn occupancy_proceeds_when_occupant_pid_is_dead() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(111)),
        &occupancy_caller(500),
        false,
        None,
        false,
    );
    assert!(decision.may_proceed);
    assert!(!decision.conflict);
}

#[test]
fn occupancy_refuses_when_occupant_is_live_other_pid() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(111)),
        &occupancy_caller(500),
        true,
        Some(&claim(111)),
        true,
    );
    assert!(!decision.may_proceed);
    assert!(decision.conflict);
    assert!(decision.reason.contains("06e--plan"));
    assert!(decision.reason.contains("111"));
}

#[test]
fn occupancy_refuses_when_running_field_disagrees_with_dead_occupant() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(111)),
        &occupancy_caller(500),
        false,
        Some(&claim(222)),
        true,
    );
    assert!(!decision.may_proceed);
    assert!(decision.conflict);
    assert!(decision.reason.contains("222"));
}

#[test]
fn occupancy_refuses_and_flags_disagreement_when_running_field_missing() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(111)),
        &occupancy_caller(500),
        true,
        None,
        false,
    );
    assert!(!decision.may_proceed);
    assert!(decision.conflict);
    assert!(decision.reason.contains("disagree"));
}

#[test]
fn occupancy_refuses_and_flags_disagreement_when_running_pid_differs() {
    let decision = decide_workspace_occupant_conflict(
        Some(&occupant(111)),
        &occupancy_caller(500),
        true,
        Some(&claim(333)),
        true,
    );
    assert!(!decision.may_proceed);
    assert!(decision.conflict);
    assert!(decision.reason.contains("disagree"));
}

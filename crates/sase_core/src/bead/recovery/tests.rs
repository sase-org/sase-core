use std::fs;
use std::sync::Arc;
use std::thread;

use tempfile::tempdir;

use super::*;

fn beads_dir() -> (tempfile::TempDir, std::path::PathBuf) {
    let temp = tempdir().unwrap();
    let beads_dir = temp.path().join("beads");
    fs::create_dir_all(&beads_dir).unwrap();
    (temp, beads_dir)
}

fn arm_request(project_id: &str, armed_at: &str) -> RecoveryArmRequestWire {
    RecoveryArmRequestWire {
        project_id: project_id.to_string(),
        armed_at: Some(armed_at.to_string()),
        generation: None,
    }
}

fn claim_request(
    project_id: &str,
    epic_id: &str,
    close_token: &str,
    owner: &str,
    proc_id: &str,
) -> RecoveryClaimCreateRequestWire {
    RecoveryClaimCreateRequestWire {
        project_id: project_id.to_string(),
        epic_id: epic_id.to_string(),
        close_token: close_token.to_string(),
        owner_machine: owner.to_string(),
        proc_id: proc_id.to_string(),
        request_fingerprint: format!("fp-{proc_id}"),
        activation_generation: None,
        created_at: Some("2026-11-28T12:00:00Z".to_string()),
    }
}

fn lookup(
    project_id: &str,
    epic_id: &str,
    close_token: &str,
) -> RecoveryClaimLookupWire {
    RecoveryClaimLookupWire {
        project_id: project_id.to_string(),
        epic_id: epic_id.to_string(),
        close_token: close_token.to_string(),
    }
}

#[test]
fn claim_keys_include_project_identity_for_shared_bead_suffixes() {
    let left = recovery_claim_key("alpha", "demo-1", "demo-1.1:evt-1");
    let right = recovery_claim_key("beta", "demo-1", "demo-1.1:evt-1");
    assert_ne!(left, right);
    assert_eq!(left.len(), 64);
    assert_eq!(
        left,
        recovery_claim_key("alpha", "demo-1", "demo-1.1:evt-1")
    );
}

#[test]
fn inspect_absent_activation_is_disabled_not_armed() {
    let (_temp, beads_dir) = beads_dir();
    let inspect = inspect_recovery_activation(&beads_dir, "alpha").unwrap();
    assert_eq!(inspect.state, "absent");
    assert!(!inspect.recovery_enabled);
    assert!(inspect.activation.is_none());
}

#[test]
fn arm_is_create_if_absent_and_preserves_cutoff_across_restarts() {
    let (_temp, beads_dir) = beads_dir();
    let first = arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    assert_eq!(first.state, "created");
    assert!(first.created);
    assert!(first.recovery_enabled);
    assert_eq!(
        first.activation.as_ref().unwrap().armed_at,
        "2026-11-28T00:00:00Z"
    );
    assert_eq!(first.activation.as_ref().unwrap().generation, 1);

    let restart = arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-12-01T00:00:00Z"),
    )
    .unwrap();
    assert_eq!(restart.state, "existing");
    assert!(!restart.created);
    assert_eq!(
        restart.activation.as_ref().unwrap().armed_at,
        "2026-11-28T00:00:00Z"
    );
    assert_eq!(restart.activation.as_ref().unwrap().generation, 1);

    let upgrade = inspect_recovery_activation(&beads_dir, "alpha").unwrap();
    assert_eq!(upgrade.state, "armed");
    assert_eq!(
        upgrade.activation.as_ref().unwrap().armed_at,
        "2026-11-28T00:00:00Z"
    );
}

#[test]
fn malformed_activation_disables_recovery_and_does_not_rearm() {
    let (_temp, beads_dir) = beads_dir();
    let path = activation_path(&beads_dir);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "{not-json\n").unwrap();

    let inspect = inspect_recovery_activation(&beads_dir, "alpha").unwrap();
    assert_eq!(inspect.state, "malformed");
    assert!(!inspect.recovery_enabled);
    assert!(inspect.diagnostic.as_ref().unwrap().contains("unreadable"));

    let arm = arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    assert_eq!(arm.state, "malformed");
    assert!(!arm.created);
    assert!(!arm.recovery_enabled);
    assert_eq!(fs::read_to_string(&path).unwrap(), "{not-json\n");
}

#[test]
fn unsupported_schema_and_project_mismatch_are_malformed() {
    let (_temp, beads_dir) = beads_dir();
    let path = activation_path(&beads_dir);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        r#"{
  "schema_version": 99,
  "project_id": "alpha",
  "armed_at": "2026-11-28T00:00:00Z",
  "generation": 1
}
"#,
    )
    .unwrap();
    let inspect = inspect_recovery_activation(&beads_dir, "alpha").unwrap();
    assert_eq!(inspect.state, "malformed");
    assert!(inspect
        .diagnostic
        .as_ref()
        .unwrap()
        .contains("schema_version"));

    fs::write(
        &path,
        r#"{
  "schema_version": 1,
  "project_id": "beta",
  "armed_at": "2026-11-28T00:00:00Z",
  "generation": 1
}
"#,
    )
    .unwrap();
    let mismatch = inspect_recovery_activation(&beads_dir, "alpha").unwrap();
    assert_eq!(mismatch.state, "malformed");
    assert!(mismatch
        .diagnostic
        .as_ref()
        .unwrap()
        .contains("does not match"));
}

#[test]
fn missing_close_token_refuses_a_claim_without_writing() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let mut request = claim_request("alpha", "demo-1", "", "athena", "proc-a");
    request.close_token.clear();
    let created = create_recovery_claim(&beads_dir, &request).unwrap();
    assert_eq!(created.state, "refused");
    assert!(!created.created);
    assert!(!created.local_winner);
    assert!(created.diagnostic.as_ref().unwrap().contains("close_token"));
    assert!(!claims_dir(&beads_dir).exists());
}

#[test]
fn claim_create_is_create_if_absent_and_repeated_reads_match() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let first = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    assert_eq!(first.state, "created");
    assert!(first.created);
    assert!(first.local_winner);
    assert_eq!(first.claim.as_ref().unwrap().activation_generation, 1);

    let loser = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "apollo", "proc-b"),
    )
    .unwrap();
    assert_eq!(loser.state, "existing");
    assert!(!loser.created);
    assert!(!loser.local_winner);
    assert_eq!(loser.claim.as_ref().unwrap().owner_machine, "athena");
    assert_eq!(loser.claim.as_ref().unwrap().proc_id, "proc-a");

    let replay = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    assert_eq!(replay.state, "existing");
    assert!(replay.local_winner);

    let first_inspect = inspect_recovery_claim(
        &beads_dir,
        &lookup("alpha", "demo-1", "demo-1.1:evt-1"),
    )
    .unwrap();
    let second_inspect = inspect_recovery_claim(
        &beads_dir,
        &lookup("alpha", "demo-1", "demo-1.1:evt-1"),
    )
    .unwrap();
    assert_eq!(first_inspect, second_inspect);
    assert_eq!(first_inspect.state, "present");
}

#[test]
fn claims_for_two_projects_with_the_same_suffix_do_not_collide() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let other = beads_dir.parent().unwrap().join("other-beads");
    fs::create_dir_all(&other).unwrap();
    arm_recovery_activation(
        &other,
        &arm_request("beta", "2026-11-28T00:00:00Z"),
    )
    .unwrap();

    let alpha = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    let beta = create_recovery_claim(
        &other,
        &claim_request("beta", "demo-1", "demo-1.1:evt-1", "apollo", "proc-b"),
    )
    .unwrap();
    assert_ne!(alpha.claim_key, beta.claim_key);
    assert_eq!(alpha.claim.as_ref().unwrap().owner_machine, "athena");
    assert_eq!(beta.claim.as_ref().unwrap().owner_machine, "apollo");
}

#[test]
fn claim_without_activation_is_refused() {
    let (_temp, beads_dir) = beads_dir();
    let created = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    assert_eq!(created.state, "refused");
    assert!(created
        .diagnostic
        .as_ref()
        .unwrap()
        .contains("activation is absent"));
}

#[test]
fn malformed_claim_is_not_replaced() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let key = recovery_claim_key("alpha", "demo-1", "demo-1.1:evt-1");
    let path = claim_path(&beads_dir, &key);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, "[]\n").unwrap();

    let created = create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    assert_eq!(created.state, "malformed");
    assert!(!created.created);
    assert!(!created.local_winner);
    assert_eq!(fs::read_to_string(&path).unwrap(), "[]\n");
}

#[test]
fn recovery_writes_do_not_change_issue_status_or_assignee() {
    let (_temp, beads_dir) = beads_dir();
    let jsonl = concat!(
        r#"{"id":"demo-1","title":"Epic","status":"in_progress","issue_type":"plan","assignee":"demo-1.land","created_at":"2026-11-01T00:00:00Z","updated_at":"2026-11-01T00:00:00Z"}"#,
        "\n",
    );
    let issues_path = beads_dir.join("issues.jsonl");
    fs::write(&issues_path, jsonl).unwrap();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    create_recovery_claim(
        &beads_dir,
        &claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a"),
    )
    .unwrap();
    assert_eq!(fs::read_to_string(&issues_path).unwrap(), jsonl);
}

#[test]
fn concurrent_local_callers_keep_a_single_winner() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let beads_dir = Arc::new(beads_dir);
    let mut joins = Vec::new();
    for index in 0..8 {
        let beads_dir = Arc::clone(&beads_dir);
        joins.push(thread::spawn(move || {
            create_recovery_claim(
                &beads_dir,
                &claim_request(
                    "alpha",
                    "demo-1",
                    "demo-1.1:evt-1",
                    &format!("machine-{index}"),
                    &format!("proc-{index}"),
                ),
            )
            .unwrap()
        }));
    }
    let results: Vec<_> =
        joins.into_iter().map(|join| join.join().unwrap()).collect();
    let created: Vec<_> =
        results.iter().filter(|result| result.created).collect();
    assert_eq!(created.len(), 1);
    let winner = created[0].claim.as_ref().unwrap();
    for result in &results {
        assert_eq!(result.claim.as_ref().unwrap(), winner);
        assert_eq!(result.claim_key, created[0].claim_key);
    }
    let store = inspect_recovery_store(&beads_dir, "alpha").unwrap();
    assert_eq!(store.claims.len(), 1);
    assert_eq!(store.claims[0].state, "present");
}

#[test]
fn generation_mismatch_refuses_without_writing() {
    let (_temp, beads_dir) = beads_dir();
    arm_recovery_activation(
        &beads_dir,
        &arm_request("alpha", "2026-11-28T00:00:00Z"),
    )
    .unwrap();
    let mut request =
        claim_request("alpha", "demo-1", "demo-1.1:evt-1", "athena", "proc-a");
    request.activation_generation = Some(9);
    let created = create_recovery_claim(&beads_dir, &request).unwrap();
    assert_eq!(created.state, "refused");
    assert!(created
        .diagnostic
        .as_ref()
        .unwrap()
        .contains("does not match activation generation"));
    assert!(!claims_dir(&beads_dir).exists());
}

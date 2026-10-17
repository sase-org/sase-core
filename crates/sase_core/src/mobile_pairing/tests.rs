use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::{Arc, Barrier};
use std::thread;

use chrono::{TimeZone, Utc};

use super::*;

fn chmod(path: &std::path::Path, mode: u32) {
    let mut perms = fs::metadata(path).unwrap().permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms).unwrap();
}

fn temp_state() -> tempfile::TempDir {
    let tmp = tempfile::Builder::new()
        .prefix("sase-pairing-")
        .tempdir_in("/tmp")
        .expect("create pairing test dir under /tmp");
    chmod(tmp.path(), 0o700);
    tmp
}

fn issue_at(
    state_root: &std::path::Path,
    now: chrono::DateTime<Utc>,
) -> MobilePairingIssueResponseWire {
    issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: Some("workstation".to_string()),
            state_root: Some(state_root.to_string_lossy().into_owned()),
        },
        now,
    )
    .expect("issue pairing challenge")
}

fn consume_at(
    state_root: &std::path::Path,
    pairing_id: &str,
    code: &str,
    now: chrono::DateTime<Utc>,
) -> MobilePairingConsumeOutcome {
    consume_mobile_pairing_challenge(
        &MobilePairingConsumeRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            pairing_id: pairing_id.to_string(),
            code: code.to_string(),
            state_root: Some(state_root.to_string_lossy().into_owned()),
        },
        now,
    )
    .expect("consume pairing challenge")
}

fn pairing_paths(
    state_root: &std::path::Path,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let pairing = state_root.join("mobile_gateway").join("pairing");
    let store = pairing.join("challenges.json");
    (pairing, store)
}

fn fixed_now() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 5, 6, 15, 0, 0).unwrap()
}

#[test]
fn issue_then_consume_accepts_once() {
    let tmp = temp_state();
    let now = fixed_now();
    let issued = issue_at(tmp.path(), now);
    assert!(issued.pairing_id.starts_with("pair_"));
    assert_eq!(issued.code.len(), 6);
    assert_eq!(issued.host_label, "workstation");
    assert_eq!(issued.expires_at, "2026-05-06T15:05:00Z");

    let (pairing_dir, store_path) = pairing_paths(tmp.path());
    let pairing_mode = fs::metadata(&pairing_dir).unwrap().mode() & 0o777;
    let gateway_mode = fs::metadata(tmp.path().join("mobile_gateway"))
        .unwrap()
        .mode()
        & 0o777;
    assert_eq!(pairing_mode, 0o700);
    assert_eq!(gateway_mode, 0o755);
    let stored = fs::read_to_string(&store_path).unwrap();
    assert!(!stored.contains(&issued.pairing_id));
    assert!(!stored.contains(&issued.code));
    assert!(stored.contains("id_hash"));
    assert!(stored.contains("secret_hash"));

    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, now),
        MobilePairingConsumeOutcome::Accepted
    );
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, now),
        MobilePairingConsumeOutcome::Rejected
    );
}

#[test]
fn consume_missing_store_is_rejected_and_does_not_create() {
    let tmp = temp_state();
    assert_eq!(
        consume_at(tmp.path(), "pair_missing", "123456", fixed_now()),
        MobilePairingConsumeOutcome::Rejected
    );
    assert!(!tmp.path().join("mobile_gateway").exists());
}

#[test]
fn wrong_code_burns_matching_id() {
    let tmp = temp_state();
    let now = fixed_now();
    let issued = issue_at(tmp.path(), now);
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, "000000", now),
        MobilePairingConsumeOutcome::Rejected
    );
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, now),
        MobilePairingConsumeOutcome::Rejected
    );
}

#[test]
fn expired_challenge_burns_matching_id() {
    let tmp = temp_state();
    let issued_at = fixed_now();
    let issued = issue_at(tmp.path(), issued_at);
    let later = issued_at + chrono::TimeDelta::minutes(6);
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, later),
        MobilePairingConsumeOutcome::Expired
    );
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, later),
        MobilePairingConsumeOutcome::Rejected
    );
}

#[test]
fn concurrent_valid_finish_mints_at_most_one_accept() {
    let tmp = temp_state();
    let now = fixed_now();
    let issued = issue_at(tmp.path(), now);
    let root = tmp.path().to_path_buf();
    let pairing_id = issued.pairing_id.clone();
    let code = issued.code.clone();
    let barrier = Arc::new(Barrier::new(2));
    let mut joins = Vec::new();
    for _ in 0..2 {
        let root = root.clone();
        let pairing_id = pairing_id.clone();
        let code = code.clone();
        let barrier = Arc::clone(&barrier);
        joins.push(thread::spawn(move || {
            barrier.wait();
            consume_at(&root, &pairing_id, &code, now)
        }));
    }
    let outcomes: Vec<_> = joins
        .into_iter()
        .map(|join| join.join().expect("consume thread"))
        .collect();
    let accepted = outcomes
        .iter()
        .filter(|outcome| **outcome == MobilePairingConsumeOutcome::Accepted)
        .count();
    let rejected = outcomes
        .iter()
        .filter(|outcome| **outcome == MobilePairingConsumeOutcome::Rejected)
        .count();
    assert_eq!(accepted, 1);
    assert_eq!(rejected, 1);
}

#[test]
fn pairing_dir_symlink_is_rejected() {
    let tmp = temp_state();
    let gateway = tmp.path().join("mobile_gateway");
    fs::create_dir(&gateway).unwrap();
    chmod(&gateway, 0o755);
    let target = tmp.path().join("elsewhere");
    fs::create_dir(&target).unwrap();
    std::os::unix::fs::symlink(&target, gateway.join("pairing")).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("symlink pairing dir");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn insecure_preexisting_store_file_is_rejected() {
    let tmp = temp_state();
    let (pairing_dir, store_path) = pairing_paths(tmp.path());
    fs::create_dir_all(&pairing_dir).unwrap();
    chmod(pairing_dir.parent().unwrap(), 0o755);
    chmod(&pairing_dir, 0o700);
    fs::write(
        &store_path,
        b"{\n  \"schema_version\": 1,\n  \"challenges\": []\n}\n",
    )
    .unwrap();
    let mut file_perms = fs::metadata(&store_path).unwrap().permissions();
    file_perms.set_mode(0o644);
    fs::set_permissions(&store_path, file_perms).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("insecure store file");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn world_writable_containing_dir_is_rejected() {
    let tmp = temp_state();
    let open = tmp.path().join("open");
    fs::create_dir(&open).unwrap();
    let mut perms = fs::metadata(&open).unwrap().permissions();
    perms.set_mode(0o777);
    fs::set_permissions(&open, perms).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(open.join("sase").to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("world-writable containing dir");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn issue_and_consume_debug_redacts_secrets() {
    let tmp = temp_state();
    let issued = issue_at(tmp.path(), fixed_now());
    let issue_debug = format!("{issued:?}");
    assert!(issue_debug.contains("<redacted>"));
    assert!(!issue_debug.contains(&issued.pairing_id));
    assert!(!issue_debug.contains(&issued.code));

    let request = MobilePairingConsumeRequestWire {
        schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
        pairing_id: issued.pairing_id.clone(),
        code: issued.code.clone(),
        state_root: Some(tmp.path().to_string_lossy().into_owned()),
    };
    let consume_debug = format!("{request:?}");
    assert!(consume_debug.contains("<redacted>"));
    assert!(!consume_debug.contains(&issued.pairing_id));
    assert!(!consume_debug.contains(&issued.code));
}

#[test]
fn schema_mismatch_is_rejected() {
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: 2,
            host_label: None,
            state_root: None,
        },
        fixed_now(),
    )
    .expect_err("schema mismatch");
    assert!(matches!(
        error,
        MobilePairingError::SchemaVersion {
            found: 2,
            expected: 1
        }
    ));
}

#[test]
fn empty_consume_fields_are_rejected_without_creating_store() {
    let tmp = temp_state();
    let outcome = consume_mobile_pairing_challenge(
        &MobilePairingConsumeRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            pairing_id: String::new(),
            code: String::new(),
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .unwrap();
    assert_eq!(outcome, MobilePairingConsumeOutcome::Rejected);
    assert!(!tmp.path().join("mobile_gateway").exists());
}

#[test]
fn corrupt_store_fails_closed() {
    let tmp = temp_state();
    let issued = issue_at(tmp.path(), fixed_now());
    let (_, store_path) = pairing_paths(tmp.path());
    fs::write(&store_path, b"{not-json").unwrap();
    let mut file_perms = fs::metadata(&store_path).unwrap().permissions();
    file_perms.set_mode(0o600);
    fs::set_permissions(&store_path, file_perms).unwrap();
    let error = consume_mobile_pairing_challenge(
        &MobilePairingConsumeRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            pairing_id: issued.pairing_id,
            code: issued.code,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("corrupt store");
    assert!(matches!(error, MobilePairingError::Corrupt));
}

#[test]
fn store_file_symlink_is_rejected() {
    let tmp = temp_state();
    let (pairing_dir, store_path) = pairing_paths(tmp.path());
    fs::create_dir_all(&pairing_dir).unwrap();
    chmod(pairing_dir.parent().unwrap(), 0o755);
    chmod(&pairing_dir, 0o700);
    let target = tmp.path().join("elsewhere.json");
    fs::write(&target, b"{}\n").unwrap();
    std::os::unix::fs::symlink(&target, &store_path).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("symlink store file");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn store_file_hard_link_is_rejected() {
    let tmp = temp_state();
    let issued = issue_at(tmp.path(), fixed_now());
    let (_, store_path) = pairing_paths(tmp.path());
    let alias = tmp
        .path()
        .join("mobile_gateway")
        .join("pairing")
        .join("alias.json");
    fs::hard_link(&store_path, &alias).unwrap();
    let error = consume_mobile_pairing_challenge(
        &MobilePairingConsumeRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            pairing_id: issued.pairing_id,
            code: issued.code,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("hard-linked store file");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn insecure_lock_file_is_rejected() {
    let tmp = temp_state();
    let (pairing_dir, _) = pairing_paths(tmp.path());
    fs::create_dir_all(&pairing_dir).unwrap();
    chmod(pairing_dir.parent().unwrap(), 0o755);
    chmod(&pairing_dir, 0o700);
    let lock_path = pairing_dir.join("challenges.lock");
    fs::write(&lock_path, b"").unwrap();
    let mut lock_perms = fs::metadata(&lock_path).unwrap().permissions();
    lock_perms.set_mode(0o644);
    fs::set_permissions(&lock_path, lock_perms).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        fixed_now(),
    )
    .expect_err("insecure lock file");
    assert!(matches!(error, MobilePairingError::UnsafePath { .. }));
}

#[test]
fn failed_second_write_leaves_first_challenge_intact() {
    let tmp = temp_state();
    let now = fixed_now();
    let issued = issue_at(tmp.path(), now);
    let (pairing_dir, _) = pairing_paths(tmp.path());
    let mut pairing_perms = fs::metadata(&pairing_dir).unwrap().permissions();
    pairing_perms.set_mode(0o500);
    fs::set_permissions(&pairing_dir, pairing_perms.clone()).unwrap();
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: Some(tmp.path().to_string_lossy().into_owned()),
        },
        now,
    )
    .expect_err("read-only pairing dir");
    assert!(matches!(
        error,
        MobilePairingError::Unavailable | MobilePairingError::UnsafePath { .. }
    ));
    pairing_perms.set_mode(0o700);
    fs::set_permissions(&pairing_dir, pairing_perms).unwrap();
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, now),
        MobilePairingConsumeOutcome::Accepted
    );
}

#[test]
fn symlink_state_root_alias_issues_and_consumes() {
    let tmp = temp_state();
    let alias = tmp.path().parent().unwrap().join(format!(
        "{}-alias",
        tmp.path().file_name().unwrap().to_string_lossy()
    ));
    std::os::unix::fs::symlink(tmp.path(), &alias).unwrap();
    let now = fixed_now();
    let issued = issue_at(&alias, now);
    assert_eq!(
        consume_at(tmp.path(), &issued.pairing_id, &issued.code, now),
        MobilePairingConsumeOutcome::Accepted
    );
    let _ = fs::remove_file(&alias);
}

#[test]
fn issue_under_permissive_umask_creates_restrictive_modes() {
    if let Ok(root) = std::env::var("PAIRING_UMASK_ROOT") {
        unsafe {
            libc::umask(0);
        }
        issue_at(std::path::Path::new(&root), fixed_now());
        return;
    }
    let tmp = temp_state();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "mobile_pairing::tests::issue_under_permissive_umask_creates_restrictive_modes",
            "--exact",
        ])
        .env("PAIRING_UMASK_ROOT", tmp.path())
        .status()
        .expect("spawn umask child");
    assert!(status.success(), "umask child failed: {status}");
    let (pairing_dir, store_path) = pairing_paths(tmp.path());
    let pairing_mode = fs::metadata(&pairing_dir).unwrap().mode() & 0o777;
    let store_mode = fs::metadata(&store_path).unwrap().mode() & 0o777;
    let lock_mode = fs::metadata(pairing_dir.join("challenges.lock"))
        .unwrap()
        .mode()
        & 0o777;
    assert_eq!(pairing_mode, 0o700);
    assert_eq!(store_mode, 0o600);
    assert_eq!(lock_mode, 0o600);
}

#[cfg(not(unix))]
#[test]
fn non_unix_issue_fails_closed() {
    let error = issue_mobile_pairing_challenge(
        &MobilePairingIssueRequestWire {
            schema_version: MOBILE_PAIRING_WIRE_SCHEMA_VERSION,
            host_label: None,
            state_root: None,
        },
        fixed_now(),
    )
    .expect_err("non-unix pairing");
    assert!(matches!(error, MobilePairingError::UnsupportedPlatform));
}

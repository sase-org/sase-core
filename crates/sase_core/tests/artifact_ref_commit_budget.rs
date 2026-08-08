//! End-to-end coverage for the artifact-ref commit-log wall-clock budget.
//!
//! This file deliberately holds exactly one test. The budget override is read
//! from the process environment, and Rust runs the tests inside one binary on
//! parallel threads, so a second test here would race this one's `set_var`.

#![cfg(unix)]

use std::ffi::CString;
use std::path::Path;
use std::process::Command;

use sase_core::{
    editor_build_artifact_ref_payload_inventory, ArtifactRefContextWire,
    ArtifactRefRepositoryWire,
};

const BUDGET_ENV: &str = "SASE_ARTIFACT_REF_COMMIT_TIMEOUT";

#[test]
fn commit_inventory_budget_override_controls_whether_rows_survive() {
    let temp = tempfile::tempdir().unwrap();
    let healthy = temp.path().join("healthy");
    let wedged = temp.path().join("wedged");
    init_git_repo(&healthy);
    init_git_repo(&wedged);
    commit_at(&healthy, 1_700_000_000, "only commit");
    commit_at(&wedged, 1_700_000_000, "only commit");
    wedge_git_forever(&wedged);
    let healthy_context = context_for("sase", &healthy);
    let wedged_context = context_for("sase", &wedged);

    // The regression this override exists to rescue: a `git log` that outruns
    // the budget is dropped, and the caller cannot tell that from a repository
    // with no commits at all.
    std::env::set_var(BUDGET_ENV, "0.2");
    let starved =
        editor_build_artifact_ref_payload_inventory("commit", &wedged_context)
            .unwrap();
    assert!(
        starved.payloads.is_empty(),
        "expected an exhausted budget to drop every row, got {:?}",
        starved.payloads
    );
    // The same budget leaves a responsive repository alone.
    assert_eq!(
        editor_build_artifact_ref_payload_inventory("commit", &healthy_context)
            .unwrap()
            .payloads
            .len(),
        1
    );

    std::env::set_var(BUDGET_ENV, "120");
    let generous =
        editor_build_artifact_ref_payload_inventory("commit", &healthy_context)
            .unwrap();
    assert_eq!(generous.payloads.len(), 1);
    assert_eq!(generous.payloads[0].label, "only commit");

    // A malformed override is ignored rather than treated as zero, and so is
    // an absent one.
    std::env::set_var(BUDGET_ENV, "not-a-number");
    let malformed =
        editor_build_artifact_ref_payload_inventory("commit", &healthy_context)
            .unwrap();
    assert_eq!(malformed.payloads, generous.payloads);

    std::env::remove_var(BUDGET_ENV);
    let defaulted =
        editor_build_artifact_ref_payload_inventory("commit", &healthy_context)
            .unwrap();
    assert_eq!(defaulted.payloads, generous.payloads);
}

fn context_for(name: &str, checkout: &Path) -> ArtifactRefContextWire {
    ArtifactRefContextWire {
        repositories: vec![ArtifactRefRepositoryWire {
            name: name.to_string(),
            checkout_paths: vec![checkout.to_string_lossy().into_owned()],
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// Make every `git` invocation in `repo` block forever.
///
/// A configured `include.path` pointing at a FIFO with no writer stalls git
/// while it parses config at start-up. That is deterministic in a way a merely
/// tiny budget is not: with a small budget a healthy `git log` can still win
/// the race and exit before the first poll observes it.
fn wedge_git_forever(repo: &Path) {
    let blocker = repo.join("blocker.fifo");
    let path = CString::new(blocker.as_os_str().as_encoded_bytes())
        .expect("fifo path should not contain a NUL byte");
    // SAFETY: `path` is a valid NUL-terminated string that outlives the call,
    // and the mode is a plain permission bitmask.
    let created = unsafe { libc::mkfifo(path.as_ptr(), 0o600) };
    assert_eq!(
        created,
        0,
        "mkfifo failed: {}",
        std::io::Error::last_os_error()
    );
    git(
        repo,
        &["config", "include.path", &blocker.to_string_lossy()],
    );
}

fn init_git_repo(repo: &Path) {
    std::fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "--quiet"]);
    git(repo, &["config", "user.name", "Commit Budget"]);
    git(repo, &["config", "user.email", "budget@example.com"]);
}

fn commit_at(repo: &Path, timestamp: i64, subject: &str) {
    let date = format!("{timestamp} +0000");
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["commit", "--quiet", "--allow-empty", "-m", subject])
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git commit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

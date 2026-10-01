//! Canary for the hermetic cargo-test environment installed by
//! `scripts/check.sh test` (see `run_hermetic_tests` there): no ambient git
//! identity or config, no ambient `SASE_*` input, and git identity guessing
//! disabled so a commit without a configured identity fails everywhere.

use std::process::Command;

/// Where the environment comes from and how to get back under it.
const GUIDANCE: &str = "cargo tests must run through `scripts/check.sh test` (`just test` / `just check`, which is also what CI runs). That script installs the hermetic test environment; see the sase-core AGENTS.md \"Build and verify\" note.";

#[test]
fn hermetic_test_env_is_in_force() {
    for var in [
        "EMAIL",
        "GIT_AUTHOR_NAME",
        "GIT_AUTHOR_EMAIL",
        "GIT_COMMITTER_NAME",
        "GIT_COMMITTER_EMAIL",
        "GIT_DIR",
    ] {
        assert!(
            std::env::var_os(var).is_none(),
            "`{var}` must not be set under the hermetic test environment. {GUIDANCE}"
        );
    }
    let leaked: Vec<String> = std::env::vars_os()
        .filter_map(|(key, _)| key.into_string().ok())
        .filter(|key| key.starts_with("SASE_"))
        .collect();
    assert!(
        leaked.is_empty(),
        "no SASE_* variable may be set under the hermetic test environment, found {leaked:?}. {GUIDANCE}"
    );

    // A commit with no identity configured anywhere must fail: with
    // `user.useConfigOnly` in force git cannot invent one from the hostname.
    // Every git call uses `-C <tmp>` so the sase-core checkout's own
    // `.git/config` can never supply an identity.
    let tmp = tempfile::tempdir().unwrap();
    let init = Command::new("git")
        .arg("-C")
        .arg(tmp.path())
        .args(["init", "--quiet"])
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "git init failed: {}. {GUIDANCE}",
        String::from_utf8_lossy(&init.stderr)
    );
    let commit = Command::new("git")
        .arg("-C")
        .arg(tmp.path())
        .args(["commit", "--allow-empty", "-m", "canary"])
        .output()
        .unwrap();
    assert!(
        !commit.status.success(),
        "git commit without an identity unexpectedly succeeded; the hermetic test environment (user.useConfigOnly) is not in force. {GUIDANCE}"
    );
}

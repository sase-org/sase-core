use super::decision::identities_equal;
use super::*;

fn snapshot(path: &str, inode: u64, mtime_ns: i64) -> GitLockSnapshotWire {
    GitLockSnapshotWire {
        path: path.to_string(),
        file_type: GIT_LOCK_FILE_REGULAR.to_string(),
        device: 1,
        inode,
        size: 8,
        mtime_ns,
        ctime_ns: mtime_ns,
    }
}

fn owner(status: &str, reason: &str) -> GitLockOwnerObservationWire {
    GitLockOwnerObservationWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        status: status.to_string(),
        reason: reason.to_string(),
    }
}

fn matched_path(path: &str) -> GitLockPathMatchWire {
    GitLockPathMatchWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        matched_path: Some(path.to_string()),
        eligible_for_removal: true,
        reason: "matched".to_string(),
    }
}

fn index_failure() -> GitLockFailureClassWire {
    GitLockFailureClassWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        kind: GIT_LOCK_FAILURE_INDEX_LOCK.to_string(),
        retryable: true,
        parsed_lock_path: Some("/repo/.git/index.lock".to_string()),
        reason: "index lock contention".to_string(),
    }
}

fn recovery_request(
    attempts: u32,
    now_unix_ns: i64,
    current: Option<GitLockSnapshotWire>,
    owner_status: &str,
) -> GitLockRecoveryRequestWire {
    let first = current.clone();
    GitLockRecoveryRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        failure: index_failure(),
        delays: vec![0.1, 0.2],
        allow_lock_removal: true,
        attempts,
        now_unix_ns,
        remaining_command_deadline_ns: None,
        remaining_recovery_budget_ns: DEFAULT_GIT_LOCK_RECOVERY_WAIT_CAP_NS,
        cancelled: false,
        removal_already_attempted: false,
        identity_disqualified: false,
        first_snapshot: first,
        current_snapshot: current,
        path_match: matched_path("/repo/.git/index.lock"),
        owner: owner(owner_status, "test"),
    }
}

#[test]
fn classifies_exit_one_and_128_index_lock_errors() {
    for exit in [1, 128] {
        let class = classify_git_lock_failure(&GitLockFailureObservationWire {
            exit_status: Some(exit),
            stdout: String::new(),
            stderr:
                "error: Unable to create '/repo/.git/index.lock': File exists."
                    .to_string(),
            timed_out: false,
            signal_terminated: false,
            os_error: None,
            resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
        });
        assert_eq!(class.kind, GIT_LOCK_FAILURE_INDEX_LOCK);
        assert!(class.retryable);
        assert_eq!(
            class.parsed_lock_path.as_deref(),
            Some("/repo/.git/index.lock")
        );
    }
}

#[test]
fn classifies_combined_stdout_bytes_style_text_and_alternate_index() {
    let class = classify_git_lock_failure(&GitLockFailureObservationWire {
        exit_status: Some(128),
        stdout: "fatal: Unable to create '/tmp/custom.lock': File exists."
            .to_string(),
        stderr: String::new(),
        timed_out: false,
        signal_terminated: false,
        os_error: None,
        resolved_index_lock_path: Some("/tmp/custom.lock".to_string()),
    });
    assert_eq!(class.kind, GIT_LOCK_FAILURE_INDEX_LOCK);
    assert!(class.retryable);
}

#[test]
fn rejects_success_timeout_signal_permission_and_incidental_mentions() {
    let success = classify_git_lock_failure(&GitLockFailureObservationWire {
        exit_status: Some(0),
        stdout: "index.lock mentioned".to_string(),
        stderr: String::new(),
        timed_out: false,
        signal_terminated: false,
        os_error: None,
        resolved_index_lock_path: None,
    });
    assert_eq!(success.kind, GIT_LOCK_FAILURE_SUCCESS);
    assert!(!success.retryable);

    let timeout = classify_git_lock_failure(&GitLockFailureObservationWire {
        exit_status: Some(124),
        stdout: String::new(),
        stderr: "Unable to create '/repo/.git/index.lock'".to_string(),
        timed_out: true,
        signal_terminated: false,
        os_error: None,
        resolved_index_lock_path: None,
    });
    assert_eq!(timeout.kind, GIT_LOCK_FAILURE_NOT_LOCK);

    let signal = classify_git_lock_failure(&GitLockFailureObservationWire {
        exit_status: Some(-15),
        stdout: String::new(),
        stderr: "Unable to create '/repo/.git/index.lock'".to_string(),
        timed_out: false,
        signal_terminated: true,
        os_error: None,
        resolved_index_lock_path: None,
    });
    assert_eq!(signal.kind, GIT_LOCK_FAILURE_NOT_LOCK);

    let permission =
        classify_git_lock_failure(&GitLockFailureObservationWire {
            exit_status: None,
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            signal_terminated: false,
            os_error: Some("permission_denied".to_string()),
            resolved_index_lock_path: None,
        });
    assert_eq!(permission.kind, GIT_LOCK_FAILURE_NOT_LOCK);

    let incidental =
        classify_git_lock_failure(&GitLockFailureObservationWire {
            exit_status: Some(1),
            stdout: String::new(),
            stderr: "hint: index.lock was mentioned only as context"
                .to_string(),
            timed_out: false,
            signal_terminated: false,
            os_error: None,
            resolved_index_lock_path: None,
        });
    assert_eq!(incidental.kind, GIT_LOCK_FAILURE_NOT_LOCK);
}

#[test]
fn other_lock_is_retryable_but_never_deleted() {
    let class = classify_git_lock_failure(&GitLockFailureObservationWire {
        exit_status: Some(128),
        stdout: String::new(),
        stderr:
            "fatal: Unable to create '/repo/.git/config.lock': File exists."
                .to_string(),
        timed_out: false,
        signal_terminated: false,
        os_error: None,
        resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
    });
    assert_eq!(class.kind, GIT_LOCK_FAILURE_OTHER_LOCK);
    let mut request =
        recovery_request(1, 20_000_000_000, None, GIT_LOCK_OWNER_ABSENT);
    request.failure = class;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_RETRY);
    request.attempts = 3;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
    assert!(!decision.claim_lock_removed);
}

#[test]
fn delay_parsing_rejects_nonfinite_and_negative_values() {
    assert_eq!(
        parse_git_lock_retry_delays(None),
        DEFAULT_GIT_LOCK_RETRY_DELAYS.to_vec()
    );
    assert_eq!(
        parse_git_lock_retry_delays(Some("0, 0.025,1.5")),
        vec![0.0, 0.025, 1.5]
    );
    for raw in ["", "wat", "0.1,,0.2", "-0.1,0.2", "nan", "inf", "0.1,inf"] {
        assert_eq!(
            parse_git_lock_retry_delays(Some(raw)),
            DEFAULT_GIT_LOCK_RETRY_DELAYS.to_vec(),
            "{raw}"
        );
    }
    assert_eq!(
        validate_git_lock_retry_delays(&[]).unwrap(),
        Vec::<f64>::new()
    );
    assert_eq!(
        validate_git_lock_retry_delays(&[f64::NAN]).unwrap_err(),
        GitLockError::InvalidDelays
    );
    assert_eq!(
        validate_git_lock_retry_delays(&[f64::INFINITY]).unwrap_err(),
        GitLockError::InvalidDelays
    );
    assert_eq!(
        validate_git_lock_retry_delays(&[-0.1]).unwrap_err(),
        GitLockError::InvalidDelays
    );
}

#[test]
fn path_match_requires_independent_resolution() {
    let matched = match_git_lock_path(&GitLockPathMatchRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        parsed_error_path: Some("/repo/.git/index.lock".to_string()),
        resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
        git_context_supported: true,
        git_context_contradictory: false,
    })
    .unwrap();
    assert!(matched.eligible_for_removal);

    let mismatched = match_git_lock_path(&GitLockPathMatchRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        parsed_error_path: Some("/other/.git/index.lock".to_string()),
        resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
        git_context_supported: true,
        git_context_contradictory: false,
    })
    .unwrap();
    assert!(!mismatched.eligible_for_removal);

    let unsupported = match_git_lock_path(&GitLockPathMatchRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        parsed_error_path: None,
        resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
        git_context_supported: false,
        git_context_contradictory: false,
    })
    .unwrap();
    assert!(!unsupported.eligible_for_removal);

    let contradictory = match_git_lock_path(&GitLockPathMatchRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        parsed_error_path: None,
        resolved_index_lock_path: Some("/repo/.git/index.lock".to_string()),
        git_context_supported: true,
        git_context_contradictory: true,
    })
    .unwrap();
    assert!(!contradictory.eligible_for_removal);
}

#[test]
fn owner_classification_uses_descriptor_git_cwd_and_ambiguity() {
    let lock = "/repo/.git/index.lock";
    let worktree = "/repo";
    let git_dir = "/repo/.git";
    let facts = |processes: Vec<GitLockProcessFactWire>, complete: bool| {
        GitLockOwnerFactsWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            probe_status: if complete {
                GIT_LOCK_PROBE_COMPLETE.to_string()
            } else {
                GIT_LOCK_PROBE_INCOMPLETE.to_string()
            },
            probe_reason: "test".to_string(),
            processes,
            resolved_git_dir: Some(git_dir.to_string()),
            resolved_index_lock_path: Some(lock.to_string()),
            resolved_worktree: Some(worktree.to_string()),
        }
    };
    let git = |pid, cwd: Option<&str>, holds, minus_c: Option<&str>| {
        let mut argv = vec!["git".to_string(), "merge".to_string()];
        if let Some(path) = minus_c {
            argv = vec![
                "git".to_string(),
                "-C".to_string(),
                path.to_string(),
                "merge".to_string(),
            ];
        }
        GitLockProcessFactWire {
            pid,
            ppid: None,
            executable: Some("/usr/bin/git".to_string()),
            argv,
            cwd: cwd.map(str::to_string),
            git_dir_env: None,
            git_index_file_env: None,
            git_work_tree_env: None,
            holds_lock_descriptor: holds,
            association_ambiguous: false,
        }
    };

    let active = classify_git_lock_owner(&facts(
        vec![git(9, Some(worktree), false, None)],
        true,
    ))
    .unwrap();
    assert_eq!(active.status, GIT_LOCK_OWNER_ACTIVE);

    let holder = classify_git_lock_owner(&facts(
        vec![GitLockProcessFactWire {
            pid: 11,
            ppid: Some(9),
            executable: Some("/usr/bin/vim".to_string()),
            argv: vec!["vim".to_string(), "COMMIT_EDITMSG".to_string()],
            cwd: Some(worktree.to_string()),
            git_dir_env: None,
            git_index_file_env: None,
            git_work_tree_env: None,
            holds_lock_descriptor: true,
            association_ambiguous: false,
        }],
        true,
    ))
    .unwrap();
    assert_eq!(holder.status, GIT_LOCK_OWNER_ACTIVE);

    let unrelated = classify_git_lock_owner(&facts(
        vec![git(12, Some("/other"), false, Some("/other"))],
        true,
    ))
    .unwrap();
    assert_eq!(unrelated.status, GIT_LOCK_OWNER_ABSENT);

    let unknown_tools = classify_git_lock_owner(&facts(vec![], false)).unwrap();
    assert_eq!(unknown_tools.status, GIT_LOCK_OWNER_UNKNOWN);

    let ambiguous = classify_git_lock_owner(&facts(
        vec![GitLockProcessFactWire {
            pid: 13,
            ppid: None,
            executable: Some("/usr/bin/git".to_string()),
            argv: vec!["git".to_string(), "status".to_string()],
            cwd: None,
            git_dir_env: None,
            git_index_file_env: None,
            git_work_tree_env: None,
            holds_lock_descriptor: false,
            association_ambiguous: false,
        }],
        true,
    ))
    .unwrap();
    assert_eq!(ambiguous.status, GIT_LOCK_OWNER_UNKNOWN);
}

#[test]
fn retries_before_considering_removal() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    let request =
        recovery_request(1, 20_000_000_000, Some(snap), GIT_LOCK_OWNER_ABSENT);
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_RETRY);
    assert_eq!(decision.delay_ns, Some(100_000_000));
    assert!(!decision.claim_lock_removed);
}

#[test]
fn removes_aged_owner_free_lock_once() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    let mut request =
        recovery_request(3, 20_000_000_000, Some(snap), GIT_LOCK_OWNER_ABSENT);
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_REMOVE_THEN_RETRY);
    assert!(decision.claim_lock_removed);

    request.removal_already_attempted = true;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
}

#[test]
fn waits_for_young_owner_free_lock_within_budget() {
    let snap = snapshot("/repo/.git/index.lock", 4, 10_000_000_000);
    let request =
        recovery_request(3, 12_000_000_000, Some(snap), GIT_LOCK_OWNER_ABSENT);
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_WAIT);
    assert_eq!(decision.delay_ns, Some(13_000_000_000));
}

#[test]
fn zero_delay_override_does_not_remove_a_young_lock() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    let mut request =
        recovery_request(1, 1_000_000_000, Some(snap), GIT_LOCK_OWNER_ABSENT);
    request.delays = vec![];
    request.remaining_recovery_budget_ns = 0;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
    assert!(!decision.claim_lock_removed);
}

#[test]
fn preserves_live_unknown_symlink_and_changed_locks() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    for status in [GIT_LOCK_OWNER_ACTIVE, GIT_LOCK_OWNER_UNKNOWN] {
        let request =
            recovery_request(3, 20_000_000_000, Some(snap.clone()), status);
        let decision = decide_git_lock_recovery(&request).unwrap();
        assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
        assert!(!decision.claim_lock_removed);
    }

    let mut symlink = snap.clone();
    symlink.file_type = GIT_LOCK_FILE_SYMLINK.to_string();
    let request = recovery_request(
        3,
        20_000_000_000,
        Some(symlink),
        GIT_LOCK_OWNER_ABSENT,
    );
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);

    let mut request = recovery_request(
        3,
        20_000_000_000,
        Some(snapshot("/repo/.git/index.lock", 9, 0)),
        GIT_LOCK_OWNER_ABSENT,
    );
    request.first_snapshot = Some(snapshot("/repo/.git/index.lock", 4, 0));
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
    assert!(decision.identity_disqualified);
}

#[test]
fn disappeared_lock_retries_without_claiming_removal() {
    let mut request =
        recovery_request(3, 20_000_000_000, None, GIT_LOCK_OWNER_ABSENT);
    request.first_snapshot = Some(snapshot("/repo/.git/index.lock", 4, 0));
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_RETRY_WITHOUT_REMOVAL);
    assert!(!decision.claim_lock_removed);
}

#[test]
fn cancelled_and_exhausted_budget_never_remove() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    let mut request = recovery_request(
        3,
        20_000_000_000,
        Some(snap.clone()),
        GIT_LOCK_OWNER_ABSENT,
    );
    request.cancelled = true;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);

    request.cancelled = false;
    request.remaining_recovery_budget_ns = 0;
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
    assert!(!decision.claim_lock_removed);
}

#[test]
fn replacement_with_old_mtime_is_a_new_identity() {
    let first = snapshot("/repo/.git/index.lock", 4, 0);
    let mut replacement = first.clone();
    replacement.inode = 99;
    replacement.mtime_ns = 1;
    assert!(!identities_equal(&first, &replacement));
}

#[test]
fn eager_cleanup_requires_age_and_absent_owner() {
    let snap = snapshot("/repo/.git/index.lock", 4, 0);
    let allowed =
        decide_eager_git_lock_cleanup(&GitLockEagerCleanupRequestWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            now_unix_ns: 20_000_000_000,
            min_age_ns: STALE_GIT_INDEX_LOCK_MIN_AGE_NS,
            snapshot: Some(snap.clone()),
            owner: owner(GIT_LOCK_OWNER_ABSENT, "absent"),
        })
        .unwrap();
    assert_eq!(allowed.action, GIT_LOCK_ACTION_REMOVE_THEN_RETRY);

    let live = decide_eager_git_lock_cleanup(&GitLockEagerCleanupRequestWire {
        schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
        now_unix_ns: 20_000_000_000,
        min_age_ns: STALE_GIT_INDEX_LOCK_MIN_AGE_NS,
        snapshot: Some(snap.clone()),
        owner: owner(GIT_LOCK_OWNER_ACTIVE, "live"),
    })
    .unwrap();
    assert_eq!(live.action, GIT_LOCK_ACTION_STOP);

    let young =
        decide_eager_git_lock_cleanup(&GitLockEagerCleanupRequestWire {
            schema_version: GIT_LOCK_WIRE_SCHEMA_VERSION,
            now_unix_ns: 1_000_000_000,
            min_age_ns: STALE_GIT_INDEX_LOCK_MIN_AGE_NS,
            snapshot: Some(snap),
            owner: owner(GIT_LOCK_OWNER_ABSENT, "absent"),
        })
        .unwrap();
    assert_eq!(young.action, GIT_LOCK_ACTION_STOP);
}

#[test]
fn honors_shorter_caller_deadline_for_age_wait() {
    let snap = snapshot("/repo/.git/index.lock", 4, 10_000_000_000);
    let mut request =
        recovery_request(3, 12_000_000_000, Some(snap), GIT_LOCK_OWNER_ABSENT);
    request.remaining_command_deadline_ns = Some(1_000_000_000);
    let decision = decide_git_lock_recovery(&request).unwrap();
    assert_eq!(decision.action, GIT_LOCK_ACTION_STOP);
}

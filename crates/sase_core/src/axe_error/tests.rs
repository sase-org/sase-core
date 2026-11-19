//! Golden tests for the error signature normalizer and grouping.
//!
//! Audit strings come from the apollo/athena notification and log audit:
//! deferred-prompt archives for different agents must share one signature,
//! while a missing-target error and a lock timeout must not.

use super::*;

fn normalize(text: &str) -> String {
    normalize_error_signature(text)
}

fn group_request(
    entries: Vec<(&str, &str, &str, &str, &str)>,
) -> AxeErrorGroupResponseWire {
    let wire_entries = entries
        .into_iter()
        .map(
            |(routine, job, signature, timestamp, sample)| AxeErrorEntryWire {
                routine: routine.to_string(),
                job: job.to_string(),
                signature: signature.to_string(),
                timestamp: timestamp.to_string(),
                sample: sample.to_string(),
            },
        )
        .collect();
    group_axe_errors(&AxeErrorGroupRequestWire {
        schema_version: AXE_ERROR_WIRE_SCHEMA_VERSION,
        entries: wire_entries,
    })
    .expect("golden grouping requests are valid")
}

#[test]
fn deferred_prompt_archives_for_different_agents_share_one_signature() {
    let first = normalize(
        "deferred prompt archive bbugyi200.athena.1e6@50c90e3f2f54 is unrestorable: missing chat.md",
    );
    let second = normalize(
        "deferred prompt archive bbugyi200.athena.sase-1t3.4@ac19f907ae53 is unrestorable: missing chat.md",
    );
    assert_eq!(first, second);
}

#[test]
fn missing_target_and_lock_timeout_have_distinct_signatures() {
    let missing = normalize(
        "agent_publication: projects=10 polled=3 errors=1: missing target for project gh_bbugyi200__nut",
    );
    let timeout = normalize(
        "workspace SDD clone integration timed out acquiring git lock for /home/bryan/.sase/projects/gh_sase-org__sase/repos/agents after 900s",
    );
    assert_ne!(missing, timeout);
}

#[test]
fn volatile_tokens_mask_to_stable_signature() {
    let first = normalize(
        "Error in housekeeping/artifact_retention: [Errno 28] No space left on device: '/home/bryan/.sase/axe/recent_errors.json' (pid 12345) at 2026-11-19T10:18:00-05:00 after 900s, run_id abc12345",
    );
    let second = normalize(
        "Error in   housekeeping/artifact_retention: [Errno 13] No space left on device: '/home/bryan/.sase/axe/metrics.json' (PID 6789) at 2026-11-18T09:00:00-05:00 after 30s, run_id def67890",
    );
    assert_eq!(first, second);
    assert!(first.contains("<path>"), "paths mask: {first}");
    assert!(first.contains("<pid>"), "pids mask: {first}");
    assert!(first.contains("<time>"), "timestamps mask: {first}");
    assert!(first.contains("<dur>"), "durations mask: {first}");
    assert!(!first.contains("12345"), "raw pid leaks: {first}");
    // Bare counts collapse so "312 errors" and "7 errors" group together.
    assert_eq!(
        normalize("agent_publication: 312 errors from 10 projects"),
        normalize("agent_publication: 7 errors from 3 projects"),
    );
}

#[test]
fn hex_shas_and_uuids_mask_but_plain_words_survive() {
    let masked =
        normalize("commit 50c90e3f2f54 failed for request 123e4567-e89b-12d3-a456-426614174000");
    assert!(masked.contains("<sha>"), "sha masks: {masked}");
    assert!(masked.contains("<id>"), "uuid masks: {masked}");
    // "defaced" is hex-valid but has no digit: it must survive untouched.
    assert!(normalize("the defaced page failed").contains("defaced"));
    // A missing target names a project without volatile tokens: it stays.
    assert!(normalize("missing target for project gh_bbugyi200__nut")
        .contains("gh_bbugyi200__nut"));
}

#[test]
fn grouping_folds_entries_by_routine_job_and_signature() {
    let response = group_request(vec![
        (
            "axe",
            "agent_publication",
            "sig-a",
            "2026-11-19T10:00:00-05:00",
            "first sample",
        ),
        (
            "axe",
            "agent_publication",
            "sig-a",
            "2026-11-19T10:05:00-05:00",
            "second sample",
        ),
        (
            "axe",
            "other_job",
            "sig-a",
            "2026-11-19T10:06:00-05:00",
            "other job",
        ),
        (
            "axe",
            "agent_publication",
            "sig-b",
            "2026-11-19T10:07:00-05:00",
            "rare cause",
        ),
    ]);
    assert_eq!(response.schema_version, AXE_ERROR_WIRE_SCHEMA_VERSION);
    assert_eq!(response.groups.len(), 3);
    let loud = &response.groups[0];
    assert_eq!(loud.count, 2);
    assert_eq!(loud.first_ts, "2026-11-19T10:00:00-05:00");
    assert_eq!(loud.last_ts, "2026-11-19T10:05:00-05:00");
    assert_eq!(loud.sample, "first sample");
    assert_eq!(response.groups[1].job, "other_job");
    assert_eq!(response.groups[2].signature, "sig-b");
}

#[test]
fn grouping_rejects_a_stale_schema_version() {
    let result = group_axe_errors(&AxeErrorGroupRequestWire {
        schema_version: AXE_ERROR_WIRE_SCHEMA_VERSION + 1,
        entries: Vec::new(),
    });
    assert!(result.is_err());
}

#[test]
fn long_signatures_and_samples_truncate() {
    let long = "x".repeat(400);
    let signature = normalize(&long);
    assert_eq!(signature.chars().count(), AXE_ERROR_MAX_SIGNATURE_CHARS);
    let response = group_request(vec![(
        "axe",
        "job",
        "sig",
        "2026-11-19T10:00:00-05:00",
        &"y".repeat(3000),
    )]);
    assert_eq!(
        response.groups[0].sample.chars().count(),
        AXE_ERROR_MAX_SAMPLE_CHARS
    );
}

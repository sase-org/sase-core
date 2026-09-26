//! Receipt opportunity report tests mirroring Python fixtures.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use rusqlite::Connection;

use crate::tool_run::catalog::normalize_tool_definition;
use crate::tool_run::store::{
    begin, finish, observe, tool_run_receipts_report,
};
use crate::tool_run::wire::{
    ToolArgsPolicyWire, ToolDefinitionWire, ToolDirtyPathWire,
    ToolEvidenceCompletenessWire, ToolFingerprintSpecWire, ToolFingerprintWire,
    ToolRepoFingerprintWire, ToolRunBeginRequestWire, ToolRunFinishRequestWire,
    ToolRunObserveRequestWire, ToolRunReceiptsReportRequestWire,
    ToolRunStateWire, ToolStagesWire, TOOL_RUN_WIRE_SCHEMA_VERSION,
};

fn timeout() -> Duration {
    Duration::from_secs(1)
}

fn definition() -> ToolDefinitionWire {
    ToolDefinitionWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        name: "tiny".into(),
        argv: vec!["true".into()],
        description: String::new(),
        stages: ToolStagesWire::None,
        inputs: Vec::new(),
        env: Vec::new(),
        args: ToolArgsPolicyWire::Deny,
        fingerprint: ToolFingerprintSpecWire::default(),
        receipt: None,
        diagnostics: Vec::new(),
    }
}

fn git(repo: &Path, args: &[&str]) -> String {
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
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn init_repo() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    fs::create_dir(&repo).unwrap();
    git(&repo, &["init", "-q"]);
    git(&repo, &["config", "user.email", "t@t.example"]);
    git(&repo, &["config", "user.name", "t"]);
    fs::write(repo.join("tracked.txt"), b"v1\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-qm", "init"]);
    (temp, repo)
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn complete_fingerprint(
    identity: &str,
    head: Option<String>,
    dirty: Vec<(String, String, String)>,
) -> ToolFingerprintWire {
    ToolFingerprintWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project_identity: None,
        definition_digest: None,
        extra_args_digest: None,
        repos: vec![ToolRepoFingerprintWire {
            identity: identity.to_string(),
            head,
            index_tree: None,
            dirty_paths: dirty
                .into_iter()
                .map(|(path, status, hash)| ToolDirtyPathWire {
                    path,
                    status,
                    kind: "file".to_string(),
                    mode: None,
                    content_hash: Some(hash),
                    incomplete: None,
                })
                .collect(),
            incomplete: None,
        }],
        inputs: Vec::new(),
        env: BTreeMap::new(),
        toolchain: BTreeMap::new(),
        completeness: ToolEvidenceCompletenessWire {
            complete: true,
            missing: Vec::new(),
        },
        diagnostics: Vec::new(),
    }
}

fn begin_run(
    store: &Path,
    run_id: &str,
    project: &str,
    tool: &str,
    now: i64,
) -> (String, String) {
    let mut named = definition();
    named.name = tool.to_string();
    let normalized = normalize_tool_definition(named).unwrap();
    let result = begin(
        store,
        ToolRunBeginRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: Some(run_id.to_string()),
            created_event_id: None,
            running_event_id: None,
            tool_name: Some(tool.to_string()),
            definition: normalized.definition,
            extra_args: Vec::new(),
            display_argv: vec!["true".to_string()],
            private_argv: None,
            project: Some(project.to_string()),
            agent: None,
            workspace: None,
            bead: None,
            owner_kind: None,
            owner_id: None,
            parent_run_id: None,
            wrapper_pid: None,
            boot_id: None,
            process_start_identity: None,
            events_path: None,
            log_stdout_path: None,
            log_stderr_path: None,
            now_ts: Some(now),
            commit_running: true,
            launch_mode: None,
            launch: None,
            owner_log_path: None,
        },
        timeout(),
    )
    .unwrap();
    (result.run.run_id, result.run.definition_digest.clone())
}

fn finish_with_fingerprint(
    store: &Path,
    run_id: &str,
    fingerprint: &ToolFingerprintWire,
    now: i64,
    duration_ms: i64,
) {
    observe(
        store,
        ToolRunObserveRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            child_pid: Some(1),
            child_pgid: Some(1),
            child_process_start_identity: None,
            fingerprint_before: Some(fingerprint.clone()),
        },
        timeout(),
    )
    .unwrap();
    finish(
        store,
        ToolRunFinishRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            run_id: run_id.to_string(),
            event_id: None,
            state: ToolRunStateWire::Succeeded,
            exit_code: Some(0),
            signal: None,
            interruption_reason: None,
            lost_reason: None,
            child_pid: None,
            child_pgid: None,
            child_process_start_identity: None,
            duration_ms: Some(duration_ms),
            fingerprint_before: None,
            fingerprint_after: Some(fingerprint.clone()),
            mutated_input: Some(false),
            now_ts: Some(now),
            terminal_cause: None,
            diagnostics: Vec::new(),
        },
        timeout(),
    )
    .unwrap();
}

fn report_request(
    project: &str,
    root: &Path,
    now: i64,
) -> ToolRunReceiptsReportRequestWire {
    ToolRunReceiptsReportRequestWire {
        schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
        project: project.to_string(),
        days: 7,
        now_ts: Some(now),
        project_root: root.to_string_lossy().into_owned(),
    }
}

#[test]
fn empty_store_reports_zero() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let out = tool_run_receipts_report(
        &store,
        report_request("testproj", &repo, 1_700_000_000),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.schema_version, 1);
    assert_eq!(out.receipts.count, 0);
    assert_eq!(out.opportunities.group_count, 0);
    assert_eq!(out.uncomparable.count, 0);
    assert_eq!(out.runs_scanned, 0);
    assert!(!out.runs_truncated);
    assert!(out.note.contains("measurement only"));
    assert!(out.note.contains("not a covering receipt"));
    assert_eq!(
        out.diagnostics,
        vec!["tool run store does not exist".to_string()]
    );
}

#[test]
fn rejects_negative_days_and_bad_schema() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let bad_days = tool_run_receipts_report(
        &store,
        ToolRunReceiptsReportRequestWire {
            schema_version: TOOL_RUN_WIRE_SCHEMA_VERSION,
            project: "testproj".to_string(),
            days: -1,
            now_ts: Some(10),
            project_root: repo.to_string_lossy().into_owned(),
        },
        timeout(),
    );
    assert!(bad_days.is_err());
    assert!(bad_days.unwrap_err().to_string().contains("-d/--days"));

    let bad_schema = tool_run_receipts_report(
        &store,
        ToolRunReceiptsReportRequestWire {
            schema_version: 999,
            project: "testproj".to_string(),
            days: 7,
            now_ts: Some(10),
            project_root: repo.to_string_lossy().into_owned(),
        },
        timeout(),
    );
    assert!(bad_schema.is_err());
}

#[test]
fn identical_repeats_group_together() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let project = "testproj";
    let now = 1_700_000_000;
    for (id, ts) in [("run-1", now - 100), ("run-2", now - 50)] {
        let (run_id, _) = begin_run(&store, id, project, "tiny", ts);
        let fingerprint =
            complete_fingerprint(project, Some(head.clone()), Vec::new());
        finish_with_fingerprint(&store, &run_id, &fingerprint, ts + 5, 12);
    }
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.schema_version, 1);
    assert_eq!(out.opportunities.group_count, 1);
    assert_eq!(out.opportunities.repeat_runs, 1);
    assert!(out.opportunities.repeat_duration_ms >= 0);
    assert_eq!(out.opportunities.groups.len(), 1);
    let group = &out.opportunities.groups[0];
    assert_eq!(group.tool, "tiny");
    assert_eq!(group.runs, 2);
    assert!(!group.spans_commits);
    assert_eq!(group.repeat_runs, 1);
    // Repeat duration sums all but the first run.
    assert_eq!(group.repeat_duration_ms, 12);
    assert_eq!(out.opportunities.top_tools[0].tool, "tiny");
    assert_eq!(out.uncomparable.count, 0);
    assert!(out.note.contains("measurement only"));
    assert_eq!(out.window.days, 7);
    assert_eq!(out.project, project);
    // Python envelope field parity.
    let json = serde_json::to_value(&out).unwrap();
    for key in [
        "schema_version",
        "project",
        "window",
        "receipts",
        "opportunities",
        "uncomparable",
        "runs_scanned",
        "runs_truncated",
        "note",
        "diagnostics",
    ] {
        assert!(json.get(key).is_some(), "missing key {key}");
    }
}

#[test]
fn dirty_tree_committed_counts_as_opportunity() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head1 = git(&repo, &["rev-parse", "HEAD"]);
    // Dirty run: working tree has v2 but HEAD still v1.
    let dirty_hash = sha256_hex(b"v2\n");
    let (run1, _) = begin_run(&store, "run-1", project, "tiny", now - 100);
    let dirty_fp = complete_fingerprint(
        project,
        Some(head1.clone()),
        vec![(
            "tracked.txt".to_string(),
            "modified".to_string(),
            dirty_hash,
        )],
    );
    finish_with_fingerprint(&store, &run1, &dirty_fp, now - 90, 5);
    // Commit v2 and run clean at the new HEAD.
    fs::write(repo.join("tracked.txt"), b"v2\n").unwrap();
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-qm", "v2"]);
    let head2 = git(&repo, &["rev-parse", "HEAD"]);
    assert_ne!(head1, head2);
    let (run2, _) = begin_run(&store, "run-2", project, "tiny", now - 50);
    let clean_fp = complete_fingerprint(project, Some(head2), Vec::new());
    finish_with_fingerprint(&store, &run2, &clean_fp, now - 40, 7);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.opportunities.group_count, 1);
    assert_eq!(out.opportunities.repeat_runs, 1);
    assert!(out.opportunities.groups[0].spans_commits);
}

#[test]
fn incomplete_fingerprint_is_uncomparable() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let (run_id, _) = begin_run(&store, "run-1", project, "brokentc", now - 10);
    let mut fingerprint = complete_fingerprint(project, Some(head), Vec::new());
    fingerprint.completeness = ToolEvidenceCompletenessWire {
        complete: false,
        missing: vec!["absent executable".to_string()],
    };
    finish_with_fingerprint(&store, &run_id, &fingerprint, now - 5, 5);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.uncomparable.count, 1);
    assert_eq!(out.uncomparable.runs[0].tool, "brokentc");
    assert!(out.uncomparable.runs[0]
        .reason
        .contains("incomplete fingerprint"));
    assert_eq!(out.opportunities.group_count, 0);
}

#[test]
fn missing_git_history_is_uncomparable() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head = git(&repo, &["rev-parse", "HEAD"]);
    // Two runs with same non-tree key but different bogus HEADs.
    for (id, ts) in [("run-1", now - 100), ("run-2", now - 50)] {
        let (run_id, _) = begin_run(&store, id, project, "tiny", ts);
        let fingerprint = complete_fingerprint(
            project,
            Some(format!("deadbeef{id}")),
            Vec::new(),
        );
        finish_with_fingerprint(&store, &run_id, &fingerprint, ts + 5, 5);
    }
    // Sanity: real head exists, bogus does not.
    assert!(Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["cat-file", "-e", &head])
        .output()
        .unwrap()
        .status
        .success());
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    // Different bogus HEADs cannot be compared without objects.
    assert!(out.uncomparable.count >= 1);
    assert!(out
        .uncomparable
        .runs
        .iter()
        .any(|run| run.reason.contains("missing Git object")));
    assert_eq!(out.opportunities.group_count, 0);
}

#[test]
fn missing_receipt_table_stays_empty_without_diagnostic() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let (run_id, _) = begin_run(&store, "run-1", project, "tiny", now - 10);
    let fingerprint = complete_fingerprint(project, Some(head), Vec::new());
    finish_with_fingerprint(&store, &run_id, &fingerprint, now - 5, 5);
    // Drop the receipts table; runs remain.
    let conn = Connection::open(&store).unwrap();
    conn.execute("DROP TABLE tool_receipts", []).unwrap();
    drop(conn);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.receipts.count, 0);
    assert!(out.diagnostics.is_empty());
    assert_eq!(out.runs_scanned, 1);
}

#[test]
fn missing_runs_table_reports_ledger_unavailable() {
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    // Valid SQLite file with no ledger tables.
    let conn = Connection::open(&store).unwrap();
    conn.execute(
        "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
        [],
    )
    .unwrap();
    drop(conn);
    let (_tmp, repo) = init_repo();
    let out = tool_run_receipts_report(
        &store,
        report_request("testproj", &repo, 1_700_000_000),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.runs_scanned, 0);
    assert_eq!(out.diagnostics.len(), 1);
    assert!(out.diagnostics[0].contains("receipt ledger unavailable"));
}

#[test]
fn window_bounds_exclude_old_runs() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head = git(&repo, &["rev-parse", "HEAD"]);
    // Old run outside a 7-day window.
    let old_ts = now - 30 * 86_400;
    let (old_id, _) = begin_run(&store, "old", project, "tiny", old_ts);
    let fingerprint =
        complete_fingerprint(project, Some(head.clone()), Vec::new());
    finish_with_fingerprint(&store, &old_id, &fingerprint, old_ts + 5, 5);
    // Recent run inside the window.
    let (recent_id, _) =
        begin_run(&store, "recent", project, "tiny", now - 100);
    finish_with_fingerprint(&store, &recent_id, &fingerprint, now - 90, 5);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.runs_scanned, 1);
    assert_eq!(out.window.since_ts, now - 7 * 86_400);
}

#[test]
fn report_is_read_only() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let (run_id, _) = begin_run(&store, "run-1", project, "tiny", now - 10);
    let fingerprint = complete_fingerprint(project, Some(head), Vec::new());
    finish_with_fingerprint(&store, &run_id, &fingerprint, now - 5, 5);
    let before = fs::read(&store).unwrap();
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.runs_scanned, 1);
    let after = fs::read(&store).unwrap();
    assert_eq!(before, after, "report must not write the ledger");
}

#[test]
fn uncomparable_output_is_bounded() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    // 60 incomplete runs exceed the 50-row display bound.
    for index in 0..60 {
        let id = format!("run-{index:03}");
        let ts = now - 1000 + index as i64;
        let (run_id, _) = begin_run(&store, &id, project, "tiny", ts);
        let mut fingerprint =
            complete_fingerprint(project, Some("head".to_string()), Vec::new());
        fingerprint.completeness = ToolEvidenceCompletenessWire {
            complete: false,
            missing: vec!["incomplete".to_string()],
        };
        finish_with_fingerprint(&store, &run_id, &fingerprint, ts + 1, 1);
    }
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.uncomparable.count, 60);
    assert!(out.uncomparable.truncated);
    assert_eq!(out.uncomparable.runs.len(), 50);
}

#[test]
fn scan_window_is_bounded_to_newest_500() {
    let (_tmp, repo) = init_repo();
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    // Seed one run through the write path so the schema exists.
    let head = git(&repo, &["rev-parse", "HEAD"]);
    let (seed_id, _) = begin_run(&store, "seed", project, "tiny", now - 10);
    let fingerprint = complete_fingerprint(project, Some(head), Vec::new());
    finish_with_fingerprint(&store, &seed_id, &fingerprint, now - 5, 1);
    // Bulk-insert 600 more runs directly; the report must scan only 500.
    let conn = Connection::open(&store).unwrap();
    for index in 0..600 {
        let id = format!("bulk-{index:04}");
        conn.execute(
            "INSERT OR IGNORE INTO runs(
                run_id, state, source, executor, attempt, tool_name,
                definition_digest, extra_args_digest, display_argv_json,
                project, created_ts, evidence_json, diagnostics_json
             ) VALUES (?1, 'succeeded', 'native', 'inline', 1, 'tiny',
                'ddd', 'eee', '[\"true\"]', ?2, ?3, '{\"complete\":false,\"missing\":[]}', '[]')",
            rusqlite::params![id, project, now - 1000 + index as i64],
        )
        .unwrap();
    }
    drop(conn);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    assert_eq!(out.runs_scanned, 500);
    assert!(out.runs_truncated);
}

#[test]
fn git_budget_exhaustion_stays_uncomparable() {
    let (_tmp, repo) = init_repo();
    // Build enough distinct commits that pairwise diffs exceed 400 git calls.
    let mut heads = Vec::new();
    for index in 0..30 {
        fs::write(repo.join("tracked.txt"), format!("v{index}\n")).unwrap();
        fs::write(
            repo.join(format!("extra-{index}.txt")),
            format!("x{index}\n"),
        )
        .unwrap();
        git(&repo, &["add", "-A"]);
        git(&repo, &["commit", "-qm", &format!("c{index}")]);
        heads.push(git(&repo, &["rev-parse", "HEAD"]));
    }
    let store_dir = tempfile::tempdir().unwrap();
    let store = store_dir.path().join("runs.sqlite");
    let project = "testproj";
    let now = 1_700_000_000;
    let (seed_id, _) = begin_run(&store, "seed", project, "tiny", now - 5000);
    let seed_fp =
        complete_fingerprint(project, Some(heads[0].clone()), Vec::new());
    finish_with_fingerprint(&store, &seed_id, &seed_fp, now - 4990, 1);
    let conn = Connection::open(&store).unwrap();
    for (index, head) in heads.iter().enumerate() {
        let id = format!("budget-{index:03}");
        let fingerprint =
            complete_fingerprint(project, Some(head.clone()), Vec::new());
        let raw = serde_json::to_string(&fingerprint).unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO runs(
                run_id, state, source, executor, attempt, tool_name,
                definition_digest, extra_args_digest, display_argv_json,
                project, created_ts, duration_ms, fingerprint_after_json,
                evidence_json, diagnostics_json
             ) VALUES (?1, 'succeeded', 'native', 'inline', 1, 'tiny',
                'ddd', 'eee', '[\"true\"]', ?2, ?3, 5, ?4,
                '{\"complete\":true,\"missing\":[]}', '[]')",
            rusqlite::params![id, project, now - 4000 + index as i64, raw],
        )
        .unwrap();
    }
    drop(conn);
    let out = tool_run_receipts_report(
        &store,
        report_request(project, &repo, now),
        timeout(),
    )
    .unwrap();
    // Budget exhaustion must degrade to uncomparable, never an error.
    assert!(out.uncomparable.count >= 1);
    assert!(out.uncomparable.runs.iter().any(|run| run
        .reason
        .contains("budget")
        || run.reason.contains("cannot diff")
        || run.reason.contains("missing Git")));
}

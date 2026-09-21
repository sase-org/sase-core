//! `@`-reference inventory and commit-log tests, covering the same seam
//! as `super::super::artifact_ref`.

use super::super::*;
use super::support::*;
use crate::editor::at_reference::AtReferenceInventoryWire;
use crate::editor::token::DocumentSnapshot;
use crate::editor::wire::{
    CompletionContext, CompletionContextKind, EditorPosition, EditorRange,
};
use crate::{
    ArtifactRefAgentRootWire, ArtifactRefBeadStoreWire, ArtifactRefContextWire,
    ArtifactRefDocumentRootWire, ArtifactRefPayloadWire,
    ArtifactRefRepositoryWire,
};
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn artifact_context(root: &Path) -> ArtifactRefContextWire {
    ArtifactRefContextWire {
        document_roots: vec![ArtifactRefDocumentRootWire {
            kind: "designs".to_string(),
            root: root.join("designs").to_string_lossy().into_owned(),
            path_globs: None,
        }],
        chats_root: Some(root.join("chats").to_string_lossy().into_owned()),
        artifact_index_path: Some(
            root.join("artifact-index.jsonl")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
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
fn init_git_repo(repo: &Path) {
    fs::create_dir_all(repo).unwrap();
    git(repo, &["init", "--quiet"]);
    git(repo, &["config", "user.name", "Commit Test"]);
    git(repo, &["config", "user.email", "commit@example.com"]);
    git(repo, &["config", "core.abbrev", "7"]);
    // Keep background git maintenance from racing fixture construction (a
    // known class of interference, not a confirmed cause of the commit_at
    // flake seen in CI).
    git(repo, &["config", "gc.auto", "0"]);
    git(repo, &["config", "maintenance.auto", "false"]);
}
fn commit_at(repo: &Path, timestamp: i64, subject: &str, body: &str) -> String {
    let date = format!("{timestamp} +0000");
    let mut command = Command::new("git");
    command.arg("-C").arg(repo).args([
        "commit",
        "--quiet",
        "--allow-empty",
        "-m",
        subject,
    ]);
    if !body.is_empty() {
        command.args(["-m", body]);
    }
    let output = command
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_DATE", &date)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git commit in {} failed: {}",
        repo.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    git(repo, &["rev-parse", "HEAD"])
}
/// Build one repository's entire commit history with a single
/// `git fast-import` invocation instead of one `git commit` subprocess
/// per commit. `fast-import` defaults the author identity to the
/// committer identity, so `%at` and `%an` come out correct without
/// separate author fields, and omitting the `from` command on every
/// `commit` block still chains each commit onto the branch's current
/// tip within the same stream.
fn commit_batch(repo: &Path, commits: &[(i64, &str, &str)]) {
    let branch_ref = git(repo, &["symbolic-ref", "HEAD"]);
    let mut stream = Vec::new();
    for (timestamp, subject, body) in commits {
        let message = if body.is_empty() {
            format!("{subject}\n")
        } else {
            format!("{subject}\n\n{body}\n")
        };
        stream.extend_from_slice(
                format!(
                    "commit {branch_ref}\n\
                     committer Commit Test <commit@example.com> {timestamp} +0000\n\
                     data {}\n",
                    message.len()
                )
                .as_bytes(),
            );
        stream.extend_from_slice(message.as_bytes());
        stream.push(b'\n');
    }
    let mut child = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(["fast-import", "--quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&stream).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "git fast-import into {} ({} commits) failed: {}",
        repo.display(),
        commits.len(),
        String::from_utf8_lossy(&output.stderr)
    );
}
fn repository(name: &str, checkout: &Path) -> ArtifactRefRepositoryWire {
    ArtifactRefRepositoryWire {
        name: name.to_string(),
        checkout_paths: vec![checkout.to_string_lossy().into_owned()],
        ..Default::default()
    }
}
fn repository_with_kind(
    name: &str,
    kind: &str,
    checkout: &Path,
) -> ArtifactRefRepositoryWire {
    ArtifactRefRepositoryWire {
        kind: kind.to_string(),
        ..repository(name, checkout)
    }
}
fn artifact_completion_context(
    text: &str,
    cursor: usize,
    context: &ArtifactRefContextWire,
) -> CompletionContext {
    let document = DocumentSnapshot::new(text);
    let position = document.byte_offset_to_position(cursor).unwrap();
    classify_completion_context_with_artifacts_and_workflows(
        &document,
        position,
        &entries(),
        &[],
        Some(context),
    )
    .unwrap_or_else(|| {
        panic!("expected artifact completion context for {text:?} at {cursor}")
    })
}
#[test]
fn classifies_artifact_kind_and_payload_at_every_cursor_position() {
    let context = artifact_context(Path::new("/tmp/artifacts"));
    let incomplete = "@des";
    for cursor in 1..=incomplete.len() {
        let completion =
            artifact_completion_context(incomplete, cursor, &context);
        assert_eq!(
            completion.kind,
            CompletionContextKind::ArtifactRefKind,
            "cursor {cursor}"
        );
    }

    let reference = "@designs:202607/guide.md";
    let separator = reference.find(':').unwrap();
    for cursor in 1..=reference.len() {
        let completion =
            artifact_completion_context(reference, cursor, &context);
        let expected = if cursor <= separator {
            CompletionContextKind::ArtifactRefKind
        } else {
            CompletionContextKind::ArtifactRefPayload
        };
        assert_eq!(completion.kind, expected, "cursor {cursor}");
        let trigger = completion.artifact_ref.unwrap();
        assert_eq!(trigger.candidate_span, (0, reference.len()));
        assert_eq!(
            trigger.kind.as_deref(),
            (cursor > separator).then_some("designs")
        );
    }
}
#[test]
fn artifact_kind_candidates_list_builtins_in_documented_order() {
    let context = ArtifactRefContextWire::default();
    let completion = artifact_completion_context("@", 1, &context);
    let list = build_artifact_ref_kind_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        Some(completion.replacement_range),
        &context,
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["commit:", "chat:", "bug:", "file:", "bead:", "agent:"]
    );
    assert!(list.candidates.iter().all(|candidate| {
        candidate.detail.as_deref() == Some("builtin artifact kind")
    }));
}
#[test]
fn builds_dynamic_kind_and_payload_candidates() {
    let temp = tempfile::tempdir().unwrap();
    let designs = temp.path().join("designs");
    fs::create_dir_all(designs.join("202607")).unwrap();
    fs::write(
        designs.join("202607/Guide.md"),
        "---\ntitle: Product Guide\n---\nguide",
    )
    .unwrap();
    fs::write(designs.join("202607/other.md"), "other").unwrap();
    let context = artifact_context(temp.path());

    let kind_context = artifact_completion_context("@DES", 4, &context);
    let kind_list = build_artifact_ref_kind_completion_candidates(
        kind_context.artifact_ref.as_ref().unwrap(),
        Some(kind_context.replacement_range),
        &context,
    );
    assert_eq!(kind_list.candidates.len(), 1);
    assert_eq!(kind_list.candidates[0].insertion, "designs:");

    let payload_text = "@designs:product";
    let payload_context =
        artifact_completion_context(payload_text, payload_text.len(), &context);
    let payload_list = build_artifact_ref_payload_completion_candidates(
        payload_context.artifact_ref.as_ref().unwrap(),
        Some(payload_context.replacement_range),
        &context,
    );
    assert_eq!(payload_list.candidates.len(), 1);
    assert_eq!(payload_list.candidates[0].insertion, "202607/Guide.md");
    assert_eq!(payload_list.candidates[0].name, "Product Guide");
    assert!(payload_list.candidates[0]
        .detail
        .as_deref()
        .unwrap()
        .contains("designs"));

    let fallback_text = "@designs:other";
    let fallback_context = artifact_completion_context(
        fallback_text,
        fallback_text.len(),
        &context,
    );
    let fallback_list = build_artifact_ref_payload_completion_candidates(
        fallback_context.artifact_ref.as_ref().unwrap(),
        Some(fallback_context.replacement_range),
        &context,
    );
    assert_eq!(fallback_list.candidates.len(), 1);
    assert_eq!(fallback_list.candidates[0].insertion, "202607/other.md");
    assert_eq!(fallback_list.candidates[0].name, "other.md");
}
#[test]
fn commit_inventory_merges_repositories_by_recency_and_assigns_rank() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    init_git_repo(&first);
    init_git_repo(&second);
    let old_sha = commit_at(&first, 1_700_000_000, "oldest", "");
    let middle_sha = commit_at(&second, 1_700_000_100, "middle", "");
    let new_sha = commit_at(&first, 1_700_000_200, "newest", "");
    let context = ArtifactRefContextWire {
        repositories: vec![
            repository("alpha", &first),
            repository("beta", &second),
            repository("alpha", &first),
        ],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert_eq!(inventory.truncated_payloads, 0);
    assert_eq!(inventory.payloads.len(), 3);
    assert_eq!(
        inventory
            .payloads
            .iter()
            .map(|row| row.label.as_str())
            .collect::<Vec<_>>(),
        vec!["newest", "middle", "oldest"]
    );
    assert_eq!(
        inventory
            .payloads
            .iter()
            .map(|row| row.rank)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2)]
    );
    assert_eq!(
        inventory
            .payloads
            .iter()
            .map(|row| row.payload.as_str())
            .collect::<Vec<_>>(),
        vec![
            format!("alpha@{}", &new_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            format!("beta@{}", &middle_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            format!("alpha@{}", &old_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
        ]
    );
    assert!(inventory.payloads.iter().all(|row| {
        row.detail.is_empty()
            && row.scope == row.payload.split_once('@').unwrap().0
    }));

    for row in &inventory.payloads {
        let parsed =
            crate::parse_artifact_ref(&format!("commit:{}", row.payload))
                .unwrap();
        let ArtifactRefPayloadWire::Commit { repo, sha } = parsed.payload
        else {
            panic!("expected commit payload");
        };
        assert!(context.repositories.iter().any(|entry| entry.name == repo));
        assert_eq!(sha.len(), ARTIFACT_REF_COMMIT_ABBREV);
    }

    let completion = artifact_completion_context("@commit:", 8, &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        None,
        &context,
    );
    assert_eq!(list.candidates.len(), 3);
}
#[test]
fn commit_inventory_keeps_non_sidecar_repository_kinds() {
    let temp = tempfile::tempdir().unwrap();
    let unclassified = temp.path().join("unclassified");
    let primary = temp.path().join("primary");
    let linked = temp.path().join("linked");
    let external = temp.path().join("external");
    let sidecar = temp.path().join("sidecar");
    init_git_repo(&unclassified);
    init_git_repo(&primary);
    init_git_repo(&linked);
    init_git_repo(&external);
    init_git_repo(&sidecar);
    let unclassified_sha =
        commit_at(&unclassified, 1_700_000_000, "unclassified", "");
    let primary_sha = commit_at(&primary, 1_700_000_100, "primary", "");
    let linked_sha = commit_at(&linked, 1_700_000_200, "linked", "");
    let external_sha = commit_at(&external, 1_700_000_300, "external", "");
    commit_at(&sidecar, 1_700_000_400, "sidecar", "");
    let context = ArtifactRefContextWire {
        repositories: vec![
            repository("unclassified", &unclassified),
            repository_with_kind("primary", "primary", &primary),
            repository_with_kind("linked", "linked", &linked),
            repository_with_kind("external", "external", &external),
            repository_with_kind(
                "plans",
                ARTIFACT_REF_REPOSITORY_KIND_SIDECAR,
                &sidecar,
            ),
        ],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert_eq!(inventory.truncated_payloads, 0);
    assert_eq!(
        inventory
            .payloads
            .iter()
            .map(|row| row.payload.as_str())
            .collect::<Vec<_>>(),
        vec![
            format!("external@{}", &external_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            format!("linked@{}", &linked_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            format!("primary@{}", &primary_sha[..ARTIFACT_REF_COMMIT_ABBREV]),
            format!(
                "unclassified@{}",
                &unclassified_sha[..ARTIFACT_REF_COMMIT_ABBREV]
            ),
        ]
    );
    assert!(!inventory.payloads.iter().any(|row| row.scope == "plans"));
}
#[test]
fn commit_inventory_is_empty_for_sidecar_only_context() {
    let temp = tempfile::tempdir().unwrap();
    let sidecar = temp.path().join("sidecar");
    init_git_repo(&sidecar);
    commit_at(&sidecar, 1_700_000_000, "sidecar", "");
    let context = ArtifactRefContextWire {
        repositories: vec![repository_with_kind("plans", "sidecar", &sidecar)],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert!(inventory.payloads.is_empty());
    assert_eq!(inventory.truncated_payloads, 0);
}
#[test]
fn commit_inventory_skips_sidecars_before_reporting_the_row_cap() {
    let temp = tempfile::tempdir().unwrap();
    let mut repositories = Vec::new();
    for repo_index in
        0..(ARTIFACT_REF_COMMIT_MAX_ROWS / ARTIFACT_REF_COMMIT_SCAN_LIMIT)
    {
        let repo = temp.path().join(format!("code-{repo_index}"));
        init_git_repo(&repo);
        let subjects = (0..ARTIFACT_REF_COMMIT_SCAN_LIMIT)
            .map(|commit_index| format!("code {repo_index} {commit_index}"))
            .collect::<Vec<_>>();
        let commits = subjects
            .iter()
            .enumerate()
            .map(|(commit_index, subject)| {
                let timestamp = 1_700_000_000
                    + (repo_index * ARTIFACT_REF_COMMIT_SCAN_LIMIT
                        + commit_index) as i64;
                (timestamp, subject.as_str(), "")
            })
            .collect::<Vec<_>>();
        commit_batch(&repo, &commits);
        repositories.push(repository_with_kind(
            &format!("code-{repo_index}"),
            "human-code",
            &repo,
        ));
    }
    let sidecar = temp.path().join("sidecar");
    init_git_repo(&sidecar);
    commit_at(&sidecar, 1_800_000_000, "newer sidecar", "");
    repositories.push(repository_with_kind("plans", "sidecar", &sidecar));
    let context = ArtifactRefContextWire {
        repositories,
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert_eq!(inventory.payloads.len(), ARTIFACT_REF_COMMIT_MAX_ROWS);
    assert_eq!(inventory.truncated_payloads, 0);
    assert!(!inventory.payloads.iter().any(|row| row.scope == "plans"));
    assert!(!inventory
        .payloads
        .iter()
        .any(|row| row.label == "newer sidecar"));
}
#[test]
fn commit_inventory_preserves_subject_and_multiline_body() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    init_git_repo(&repo);
    let subject = "fix \"quoted\"\t日本語";
    let body = "first body line\nsecond\tline";
    commit_at(&repo, 1_700_000_000, subject, body);
    let context = ArtifactRefContextWire {
        repositories: vec![repository("sase-core", &repo)],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert_eq!(inventory.payloads.len(), 1);
    assert_eq!(inventory.payloads[0].label, subject);
    assert_eq!(inventory.payloads[0].body, body);
}
#[test]
fn commit_inventory_enforces_the_per_repository_scan_limit() {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    init_git_repo(&repo);
    let subjects = (0..=ARTIFACT_REF_COMMIT_SCAN_LIMIT)
        .map(|index| format!("commit {index}"))
        .collect::<Vec<_>>();
    let commits = subjects
        .iter()
        .enumerate()
        .map(|(index, subject)| {
            (1_700_000_000 + index as i64, subject.as_str(), "")
        })
        .collect::<Vec<_>>();
    commit_batch(&repo, &commits);
    let context = ArtifactRefContextWire {
        repositories: vec![repository("sase", &repo)],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("commit", &context).unwrap();

    assert_eq!(inventory.payloads.len(), ARTIFACT_REF_COMMIT_SCAN_LIMIT);
    assert_eq!(inventory.payloads[0].label, "commit 200");
    assert_eq!(inventory.payloads.last().unwrap().label, "commit 1");
    assert!(!inventory.payloads.iter().any(|row| row.label == "commit 0"));
}
#[test]
fn commit_inventory_skips_unusable_checkouts_and_bug_stays_empty() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing");
    let not_git = temp.path().join("not-git");
    let empty_git = temp.path().join("empty-git");
    let populated_git = temp.path().join("populated-git");
    fs::create_dir_all(&not_git).unwrap();
    init_git_repo(&empty_git);
    init_git_repo(&populated_git);
    commit_at(&populated_git, 1_700_000_000, "hidden second path", "");
    let context = ArtifactRefContextWire {
        repositories: vec![
            repository("missing", &missing),
            repository("not-git", &not_git),
            repository("empty-git", &empty_git),
            repository("bad@repo", &populated_git),
            ArtifactRefRepositoryWire {
                name: "first-only".to_string(),
                checkout_paths: vec![
                    missing.to_string_lossy().into_owned(),
                    populated_git.to_string_lossy().into_owned(),
                ],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    assert!(build_artifact_ref_payload_inventory("commit", &context)
        .unwrap()
        .payloads
        .is_empty());
    assert_eq!(
        build_artifact_ref_payload_inventory("bug", &context).unwrap(),
        AtReferenceInventoryWire::default()
    );
}
#[test]
fn commit_inventory_reports_the_merged_row_cap() {
    let commits = (0..ARTIFACT_REF_COMMIT_MAX_ROWS + 3)
        .map(|index| CommitCandidate {
            repository: "sase".to_string(),
            abbreviated_sha: format!("{index:012x}"),
            timestamp: 10_000 - index as i64,
            subject: format!("commit {index}"),
            body: String::new(),
        })
        .collect();
    let mut payloads = Vec::new();
    let mut seen = BTreeSet::new();

    let truncated = append_ranked_commit_candidates(
        &mut payloads,
        &mut seen,
        commits,
        10_000,
        0,
    );

    assert_eq!(payloads.len(), ARTIFACT_REF_COMMIT_MAX_ROWS);
    assert_eq!(truncated, 3);
    assert_eq!(payloads.last().unwrap().rank, Some(999));
}
#[test]
fn commit_merge_ties_break_by_repository_then_sha() {
    let mut commits = vec![
        CommitCandidate {
            repository: "zeta".to_string(),
            abbreviated_sha: "000000000001".to_string(),
            timestamp: 100,
            subject: String::new(),
            body: String::new(),
        },
        CommitCandidate {
            repository: "alpha".to_string(),
            abbreviated_sha: "000000000002".to_string(),
            timestamp: 100,
            subject: String::new(),
            body: String::new(),
        },
        CommitCandidate {
            repository: "alpha".to_string(),
            abbreviated_sha: "000000000001".to_string(),
            timestamp: 100,
            subject: String::new(),
            body: String::new(),
        },
        CommitCandidate {
            repository: "zeta".to_string(),
            abbreviated_sha: "000000000002".to_string(),
            timestamp: 200,
            subject: String::new(),
            body: String::new(),
        },
    ];

    sort_commit_candidates(&mut commits);

    assert_eq!(
        commits
            .iter()
            .map(|commit| {
                format!("{}@{}", commit.repository, commit.abbreviated_sha)
            })
            .collect::<Vec<_>>(),
        vec![
            "zeta@000000000002",
            "alpha@000000000001",
            "alpha@000000000002",
            "zeta@000000000001",
        ]
    );
}
#[test]
fn commit_age_labels_match_prompt_bar_thresholds() {
    let now = 1_700_000_000;
    assert_eq!(commit_age_label(0, now, 0), "");
    assert_eq!(commit_age_label(now + 1, now, 0), "now");
    assert_eq!(commit_age_label(now - 59, now, 0), "now");
    assert_eq!(commit_age_label(now - 60, now, 0), "1m");
    assert_eq!(commit_age_label(now - 3_600, now, 0), "1h");
    assert_eq!(commit_age_label(now - 86_400, now, 0), "1d");
    assert_eq!(commit_age_label(now - 7 * 86_400, now, 0), "2023-11-07");
}
#[test]
fn commit_age_label_applies_the_utc_offset_before_the_date_falls_back() {
    // 2023-11-01T01:00:00Z: just after UTC midnight, so a negative
    // (western) offset pins the *previous* calendar day.
    let timestamp = 1_698_800_400;
    let now = timestamp + 8 * 86_400;

    assert_eq!(commit_age_label(timestamp, now, 0), "2023-11-01");
    assert_eq!(commit_age_label(timestamp, now, -4 * 3_600), "2023-10-31");
}
/// Make `git` block forever inside this repository.
///
/// A configured `include.path` pointing at a FIFO with no writer stalls
/// git during start-up config parsing, which is deterministic in a way
/// that a merely tiny budget is not: with a small budget the child can
/// still win the race and exit before the first poll observes it.
#[cfg(unix)]
fn wedge_git_forever(repo: &Path) {
    use std::ffi::CString;

    let blocker = repo.join("blocker.fifo");
    let path = CString::new(blocker.as_os_str().as_encoded_bytes())
        .expect("fifo path should not contain a NUL byte");
    // SAFETY: `path` is a valid NUL-terminated string that outlives the
    // call, and the mode is a plain permission bitmask.
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
#[cfg(unix)]
#[test]
fn commit_log_reports_an_expired_budget_instead_of_empty_output() {
    let temp = tempfile::tempdir().unwrap();
    let healthy = temp.path().join("healthy");
    let wedged = temp.path().join("wedged");
    init_git_repo(&healthy);
    init_git_repo(&wedged);
    commit_at(&healthy, 1_700_000_000, "only", "");
    commit_at(&wedged, 1_700_000_000, "only", "");
    wedge_git_forever(&wedged);

    // This is the R6 mechanism in isolation: a `git log` that outlives the
    // budget is killed and the repository silently contributes zero rows.
    let budget = Duration::from_millis(250);
    let started = Instant::now();
    assert_eq!(
        commit_log_output(&wedged, budget),
        Err(CommitLogFailure::Budget)
    );
    assert!(started.elapsed() >= budget, "the budget was not honoured");

    // The row-producing path is unaffected by the new plumbing.
    assert!(
        !commit_log_output(&healthy, ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT)
            .expect("default budget should complete")
            .is_empty()
    );
}
#[test]
fn commit_log_distinguishes_every_unusable_repository_outcome() {
    let temp = tempfile::tempdir().unwrap();
    let broken_repo = temp.path().join("broken");
    fs::create_dir_all(&broken_repo).unwrap();
    // A malformed `.git` file makes git fail in place instead of walking
    // up to whatever repository happens to contain TMPDIR.
    fs::write(broken_repo.join(".git"), "not a gitfile\n").unwrap();

    assert_eq!(
        commit_log_output(&broken_repo, ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT),
        Err(CommitLogFailure::ExitStatus)
    );

    let budget = Duration::from_secs(30);
    let cause =
        CommitLogIoCause::new(&io::Error::from(io::ErrorKind::NotFound));
    let descriptions = [
        CommitLogFailure::Scratch(ScratchStep::Create, cause),
        CommitLogFailure::Scratch(ScratchStep::Clone, cause),
        CommitLogFailure::Spawn(cause),
        CommitLogFailure::Budget,
        CommitLogFailure::Wait(cause),
        CommitLogFailure::ExitStatus,
        CommitLogFailure::Read(cause),
    ]
    .map(|failure| failure.describe(budget));
    assert!(descriptions.iter().all(|text| !text.is_empty()));
    assert_eq!(
        descriptions.iter().collect::<BTreeSet<_>>().len(),
        descriptions.len()
    );

    let budget_text = CommitLogFailure::Budget.describe(budget);
    assert!(budget_text.contains("30s"));
    assert!(budget_text.contains(ARTIFACT_REF_COMMIT_TIMEOUT_ENV));
}
#[cfg(unix)]
#[test]
fn commit_log_failures_report_the_underlying_os_error() {
    // The investigation behind this plumbing stalled because
    // `CommitLogFailure::Scratch` guessed at "check that TMPDIR exists and
    // is writable" while discarding the errno: EMFILE (descriptor
    // exhaustion) and ENOSPC (no space or inodes) both reach the same two
    // syscalls and were indistinguishable in the message.
    let budget = Duration::from_secs(30);
    let emfile =
        CommitLogIoCause::new(&io::Error::from_raw_os_error(libc::EMFILE));
    let enospc =
        CommitLogIoCause::new(&io::Error::from_raw_os_error(libc::ENOSPC));

    let create =
        CommitLogFailure::Scratch(ScratchStep::Create, emfile).describe(budget);
    assert!(create.contains("os error 24"), "{create}");
    assert!(
        !create.contains("TMPDIR exists and is writable"),
        "the disproved TMPDIR guess should not have come back: {create}"
    );

    let full =
        CommitLogFailure::Scratch(ScratchStep::Create, enospc).describe(budget);
    assert!(full.contains("os error 28"), "{full}");
    assert_ne!(create, full, "distinct errnos must read differently");

    // The `dup` call site is named separately from the `open` one.
    let clone =
        CommitLogFailure::Scratch(ScratchStep::Clone, emfile).describe(budget);
    assert!(clone.contains("os error 24"), "{clone}");
    assert_ne!(create, clone);

    // A cause with no errno still renders something usable.
    let kind_only =
        CommitLogIoCause::new(&io::Error::from(io::ErrorKind::BrokenPipe));
    assert_eq!(kind_only.raw_os_error, None);
    assert!(!kind_only.describe().is_empty());
}
#[test]
fn commit_timeout_override_accepts_only_positive_finite_seconds() {
    assert_eq!(
        parse_commit_timeout(Some("0.25")),
        Some(Duration::from_millis(250))
    );
    assert_eq!(
        parse_commit_timeout(Some("  120  ")),
        Some(Duration::from_secs(120))
    );
    for rejected in [
        None,
        Some(""),
        Some("0"),
        Some("-5"),
        Some("nan"),
        Some("inf"),
    ] {
        assert_eq!(parse_commit_timeout(rejected), None);
    }
    assert_eq!(ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT, Duration::from_secs(30));
}
#[test]
fn commit_timeout_reads_the_documented_environment_override() {
    // Deliberately not mutating the process environment: these tests share
    // it with every other test in the binary. Assert the wiring instead.
    assert_eq!(
        ARTIFACT_REF_COMMIT_TIMEOUT_ENV,
        "SASE_ARTIFACT_REF_COMMIT_TIMEOUT"
    );
    let observed = artifact_ref_commit_timeout();
    let expected = parse_commit_timeout(
        std::env::var(ARTIFACT_REF_COMMIT_TIMEOUT_ENV)
            .ok()
            .as_deref(),
    )
    .unwrap_or(ARTIFACT_REF_COMMIT_TIMEOUT_DEFAULT);
    assert_eq!(observed, expected);
}
#[test]
fn builds_bead_payload_candidates_from_published_pages() {
    let temp = tempfile::tempdir().unwrap();
    let bead_root = temp.path().join("beads");
    fs::create_dir_all(bead_root.join("pages/sase-9z")).unwrap();
    fs::write(
        bead_root.join("pages/sase-9z/README.md"),
        "# Bead: sase-9z \u{2014} Root bead\n",
    )
    .unwrap();
    fs::write(
        bead_root.join("pages/sase-9z/sase-9z.1.md"),
        "# Bead: sase-9z.1 \u{2014} Phase bead\n",
    )
    .unwrap();
    fs::write(bead_root.join("pages/sase-9z/notes.txt"), "ignore").unwrap();
    let context = ArtifactRefContextWire {
        bead_stores: vec![ArtifactRefBeadStoreWire {
            project: "sase".to_string(),
            prefix: "sase".to_string(),
            root: bead_root.to_string_lossy().into_owned(),
        }],
        ..Default::default()
    };

    let completion = artifact_completion_context("@bead:sase-9z", 13, &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        Some(completion.replacement_range),
        &context,
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["sase-9z", "sase-9z.1"]
    );
    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["Root bead", "Phase bead"]
    );
    assert!(list
        .candidates
        .iter()
        .all(|candidate| candidate.detail.as_deref() == Some("bead · sase")));
}
#[test]
fn builds_agent_payload_candidates_from_published_pages() {
    let temp = tempfile::tempdir().unwrap();
    let agent_root = temp.path().join("agents-sidecar");
    fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.9w--code"))
        .unwrap();
    fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.9w")).unwrap();
    fs::create_dir_all(agent_root.join("agents/bbugyi200.athena.skip"))
        .unwrap();
    fs::write(
        agent_root.join("agents/bbugyi200.athena.9w--code/README.md"),
        "member",
    )
    .unwrap();
    fs::write(
        agent_root.join("agents/bbugyi200.athena.9w/README.md"),
        "agent",
    )
    .unwrap();
    let context = ArtifactRefContextWire {
        agent_roots: vec![ArtifactRefAgentRootWire {
            project: "sase".to_string(),
            root: agent_root.to_string_lossy().into_owned(),
        }],
        ..Default::default()
    };

    let text = "@agent:bbugyi200.athena.9w";
    let completion = artifact_completion_context(text, text.len(), &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        Some(completion.replacement_range),
        &context,
    );

    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.insertion.as_str())
            .collect::<Vec<_>>(),
        vec!["bbugyi200.athena.9w", "bbugyi200.athena.9w--code"]
    );
    assert_eq!(
        list.candidates
            .iter()
            .map(|candidate| candidate.name.as_str())
            .collect::<Vec<_>>(),
        vec!["9w", "9w--code"]
    );
    assert!(list
        .candidates
        .iter()
        .all(|candidate| candidate.detail.as_deref() == Some("agent · sase")));
}
#[test]
fn agent_and_indexed_file_payloads_match_mid_name_fragments() {
    let temp = tempfile::tempdir().unwrap();
    let agent_root = temp.path().join("agents-sidecar");
    for name in [
        "bbugyi200.athena.sase-b3.5",
        "bbugyi200.athena.9w--code",
        "bbugyi200.athena.other",
    ] {
        fs::create_dir_all(agent_root.join("agents").join(name)).unwrap();
        fs::write(
            agent_root.join("agents").join(name).join("README.md"),
            "agent",
        )
        .unwrap();
    }
    let context = ArtifactRefContextWire {
        agent_roots: vec![ArtifactRefAgentRootWire {
            project: "sase".to_string(),
            root: agent_root.to_string_lossy().into_owned(),
        }],
        artifact_index_path: Some(
            temp.path()
                .join("artifact-index.jsonl")
                .to_string_lossy()
                .into_owned(),
        ),
        ..Default::default()
    };
    fs::write(
            temp.path().join("artifact-index.jsonl"),
            "{\"schema_version\":1,\"artifact\":{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/tmp/panel.png\"}}\n",
        )
        .unwrap();

    for (text, expected) in [
        ("@agent:sase-b3", "bbugyi200.athena.sase-b3.5"),
        ("@file:931185", "default:52895d68931185056fd0e49f"),
    ] {
        let completion =
            artifact_completion_context(text, text.len(), &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );
        assert_eq!(
            list.candidates
                .iter()
                .map(|candidate| candidate.insertion.as_str())
                .collect::<Vec<_>>(),
            vec![expected],
            "{text}"
        );
    }
}
#[test]
fn agent_prefix_query_survives_a_corpus_of_fuzzy_matches() {
    let temp = tempfile::tempdir().unwrap();
    let agent_root = temp.path().join("agents-sidecar");
    // Every name below fuzzy-matches "zq", but only the last one — sorted
    // last in walk order — matches it as a prefix.
    for index in 0..crate::editor::at_reference::AT_REFERENCE_MAX_GROUP_ROWS + 5
    {
        let name = format!("aaz{index:04}q");
        fs::create_dir_all(agent_root.join("agents").join(&name)).unwrap();
        fs::write(
            agent_root.join("agents").join(&name).join("README.md"),
            "agent",
        )
        .unwrap();
    }
    fs::create_dir_all(agent_root.join("agents/zq-target")).unwrap();
    fs::write(agent_root.join("agents/zq-target/README.md"), "agent").unwrap();
    let context = ArtifactRefContextWire {
        agent_roots: vec![ArtifactRefAgentRootWire {
            project: "sase".to_string(),
            root: agent_root.to_string_lossy().into_owned(),
        }],
        ..Default::default()
    };

    let text = "@agent:zq";
    let completion = artifact_completion_context(text, text.len(), &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        None,
        &context,
    );

    assert_eq!(list.candidates[0].insertion, "zq-target");
}
#[test]
fn payload_enumeration_is_bounded_and_deduplicated() {
    let temp = tempfile::tempdir().unwrap();
    let first = temp.path().join("first");
    let second = temp.path().join("second");
    fs::create_dir_all(&first).unwrap();
    fs::create_dir_all(&second).unwrap();
    for index in 0..205 {
        fs::write(first.join(format!("{index:03}.md")), "x").unwrap();
    }
    fs::write(second.join("000.md"), "duplicate").unwrap();
    fs::write(second.join("unique.md"), "unique").unwrap();
    let context = ArtifactRefContextWire {
        document_roots: vec![
            ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: first.to_string_lossy().into_owned(),
                path_globs: None,
            },
            ArtifactRefDocumentRootWire {
                kind: "designs".to_string(),
                root: second.to_string_lossy().into_owned(),
                path_globs: None,
            },
        ],
        ..Default::default()
    };
    let completion = artifact_completion_context("@designs:", 9, &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        None,
        &context,
    );

    assert_eq!(
        list.candidates.len(),
        crate::editor::at_reference::AT_REFERENCE_MAX_GROUP_ROWS
    );
    assert_eq!(
        list.candidates
            .iter()
            .filter(|candidate| candidate.insertion == "000.md")
            .count(),
        1
    );
}
#[test]
fn payload_inventory_reaches_past_the_editor_display_cap() {
    let temp = tempfile::tempdir().unwrap();
    let designs = temp.path().join("designs");
    fs::create_dir_all(&designs).unwrap();
    for index in 0..205 {
        fs::write(designs.join(format!("{index:03}.md")), "x").unwrap();
    }
    fs::write(designs.join("zzz-needle.md"), "x").unwrap();
    let context = ArtifactRefContextWire {
        document_roots: vec![ArtifactRefDocumentRootWire {
            kind: "designs".to_string(),
            root: designs.to_string_lossy().into_owned(),
            path_globs: None,
        }],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("designs", &context).unwrap();
    assert_eq!(inventory.payloads.len(), 206);
    assert_eq!(inventory.truncated_payloads, 0);

    let completion =
        artifact_completion_context("@designs:needle", 15, &context);
    let list = build_artifact_ref_payload_completion_candidates(
        completion.artifact_ref.as_ref().unwrap(),
        None,
        &context,
    );
    assert_eq!(list.candidates.len(), 1);
    assert_eq!(list.candidates[0].insertion, "zzz-needle.md");
}
#[test]
fn payload_inventory_applies_document_root_path_globs() {
    let temp = tempfile::tempdir().unwrap();
    let designs = temp.path().join("designs");
    fs::create_dir_all(designs.join("allowed/private")).unwrap();
    fs::write(designs.join("allowed/keep.md"), "keep").unwrap();
    fs::write(designs.join("allowed/private/skip.md"), "skip").unwrap();
    fs::write(designs.join("other.md"), "other").unwrap();
    let context = ArtifactRefContextWire {
        document_roots: vec![ArtifactRefDocumentRootWire {
            kind: "designs".to_string(),
            root: designs.to_string_lossy().into_owned(),
            path_globs: Some(vec![
                "allowed/**".to_string(),
                "!allowed/private/**".to_string(),
            ]),
        }],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("designs", &context).unwrap();

    assert_eq!(
        inventory
            .payloads
            .iter()
            .map(|row| row.payload.as_str())
            .collect::<Vec<_>>(),
        vec!["allowed/keep.md"]
    );
}
#[test]
fn payload_inventory_discloses_the_scan_bound() {
    let temp = tempfile::tempdir().unwrap();
    let designs = temp.path().join("designs");
    fs::create_dir_all(&designs).unwrap();
    for index in 0..ARTIFACT_REF_MAX_SCAN_RESULTS + 1 {
        fs::write(designs.join(format!("{index:05}.md")), "").unwrap();
    }
    let context = ArtifactRefContextWire {
        document_roots: vec![ArtifactRefDocumentRootWire {
            kind: "designs".to_string(),
            root: designs.to_string_lossy().into_owned(),
            path_globs: None,
        }],
        ..Default::default()
    };

    let inventory =
        build_artifact_ref_payload_inventory("designs", &context).unwrap();

    assert_eq!(inventory.payloads.len(), ARTIFACT_REF_MAX_SCAN_RESULTS);
    assert_eq!(inventory.truncated_payloads, 1);
}
#[test]
fn builds_chat_and_indexed_file_payloads_but_not_remote_kinds() {
    let temp = tempfile::tempdir().unwrap();
    let context = artifact_context(temp.path());
    fs::create_dir_all(temp.path().join("chats/202607")).unwrap();
    fs::write(temp.path().join("chats/202607/agent.md"), "chat").unwrap();
    fs::write(
            temp.path().join("artifact-index.jsonl"),
            "{\"schema_version\":1,\"artifact\":{\"id\":\"default:52895d68931185056fd0e49f\",\"path\":\"/tmp/panel-screenshot.png\"}}\n",
        )
        .unwrap();

    for (text, expected, expected_title) in [
        ("@chat:202607/a", "202607/agent.md", "agent.md"),
        (
            "@file:default:",
            "default:52895d68931185056fd0e49f",
            "panel-screenshot.png",
        ),
        (
            "@file:panel",
            "default:52895d68931185056fd0e49f",
            "panel-screenshot.png",
        ),
    ] {
        let completion =
            artifact_completion_context(text, text.len(), &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );
        assert_eq!(list.candidates.len(), 1, "{text}");
        assert_eq!(list.candidates[0].insertion, expected, "{text}");
        assert_eq!(list.candidates[0].name, expected_title, "{text}");
    }

    for text in ["@commit:sase@0123456", "@bug:sase#1", "@unknown:value"] {
        let completion =
            artifact_completion_context(text, text.len(), &context);
        let list = build_artifact_ref_payload_completion_candidates(
            completion.artifact_ref.as_ref().unwrap(),
            None,
            &context,
        );
        assert!(list.candidates.is_empty(), "{text}: {list:?}");
    }
}
#[test]
fn artifact_replacement_ranges_are_utf16_safe_and_at_paths_stay_references() {
    let temp = tempfile::tempdir().unwrap();
    let context = artifact_context(temp.path());
    let text = "é @designs:guidé.md";
    let completion = artifact_completion_context(text, text.len(), &context);
    assert_eq!(
        completion.replacement_range,
        EditorRange {
            start: EditorPosition {
                line: 0,
                character: 11,
            },
            end: EditorPosition {
                line: 0,
                character: 19,
            },
        }
    );

    let document = DocumentSnapshot::new("@src/foo");
    let context = classify_completion_context_with_artifacts_and_workflows(
        &document,
        pos(8),
        &entries(),
        &[],
        Some(&context),
    )
    .unwrap();
    assert_eq!(context.kind, CompletionContextKind::ArtifactRefKind);
    let trigger = context.artifact_ref.unwrap();
    assert_eq!(trigger.query, "src/foo");
    assert_eq!(trigger.candidate_span, (0, 8));
}
